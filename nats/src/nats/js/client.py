# Copyright 2021-2022 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

import asyncio
import dataclasses
import json
import time
from email.parser import BytesParser
from secrets import token_hex
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
)

import nats.errors
import nats.js.errors
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js import api
from nats.js.errors import (
    BadBucketError,
    BucketNotFoundError,
    FetchTimeoutError,
    InvalidBucketNameError,
    NotFoundError,
)
from nats.js.kv import KeyValue
from nats.js.manager import JetStreamManager
from nats.js.object_store import (
    OBJ_ALL_CHUNKS_PRE_TEMPLATE,
    OBJ_ALL_META_PRE_TEMPLATE,
    OBJ_STREAM_TEMPLATE,
    VALID_BUCKET_RE,
    ObjectStore,
)

if TYPE_CHECKING:
    from nats import NATS

NO_RESPONDERS_STATUS = "503"

NATS_HDR_LINE = bytearray(b"NATS/1.0")
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
_CRLF_ = b"\r\n"
_CRLF_LEN_ = len(_CRLF_)
KV_STREAM_TEMPLATE = "KV_{bucket}"
KV_STREAM_PREFIX = "KV_"
KV_PRE_TEMPLATE = "$KV.{bucket}."
KV_ALL_SUBJECTS = "$KV.*.>"
Callback = Callable[["Msg"], Awaitable[None]]

# For JetStream the default pending limits are larger.
DEFAULT_JS_SUB_PENDING_MSGS_LIMIT = 512 * 1024
DEFAULT_JS_SUB_PENDING_BYTES_LIMIT = 256 * 1024 * 1024

# Max history limit for key value.
KV_MAX_HISTORY = 64

# JetStream error codes the KV management calls map to bucket errors.
KV_STREAM_NAME_IN_USE = 10058
KV_STREAM_NOT_FOUND = 10059

# 409 status descriptions of pull requests that the server would reject
# again on retry, so they are reported instead of treated as a timeout.
_TERMINAL_CONFLICTS = (
    "Consumer Deleted",
    "Consumer is push based",
    "Exceeded MaxRequestBatch",
    "Exceeded MaxRequestExpires",
    "Exceeded MaxRequestMaxBytes",
)


class PubAckFuture(asyncio.Future):
    """
    PubAckFuture is the future returned by
    :meth:`JetStreamContext.publish_async`. It is an :class:`asyncio.Future`
    that resolves to the :class:`api.PubAck` of the publish, or fails with
    its error, and keeps the published message.
    """

    def __init__(self, msg: Msg, max_retries: int = 0, retry_wait: float = 0) -> None:
        super().__init__()
        self._pub_msg = msg
        self._retries = 0
        self._max_retries = max_retries
        self._retry_wait = retry_wait
        self._timer: Optional[asyncio.TimerHandle] = None

    @property
    def msg(self) -> Msg:
        """The message that was published."""
        return self._pub_msg

    async def ok(self) -> api.PubAck:
        """Wait for the acknowledgement, raising the publish error if it failed."""
        return await self

    async def err(self) -> Optional[BaseException]:
        """Wait for the publish to complete and return its error, if any."""
        try:
            await self
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return e
        return None


# Async publish handlers: called with the context, the published message and
# the acknowledgement (or the error) once the publish completes.
PublishAsyncAckHandler = Callable[["JetStreamContext", Msg, api.PubAck], Any]
PublishAsyncErrHandler = Callable[["JetStreamContext", Msg, Exception], Any]


class JetStreamContext(JetStreamManager):
    """
    Fully featured context for interacting with JetStream.

    :param conn: NATS Connection
    :param prefix: Default JetStream API Prefix.
    :param domain: Optional domain used by the JetStream API.
    :param timeout: Timeout for all JS API actions.
    :param publish_async_max_pending: Maximum outstanding async publishes that can be inflight at one time.
    :param client_trace: Hooks called around each JetStream API request.
    :param publish_async_timeout: Seconds an async publish waits for its acknowledgement
        before failing with AsyncPublishTimeoutError (``None`` waits forever).
    :param publish_async_ack_handler: Called as ``handler(js, msg, ack)`` for each
        acknowledged async publish; may be a coroutine function.
    :param publish_async_err_handler: Called as ``handler(js, msg, err)`` for each
        failed async publish; may be a coroutine function.

    ::

        import asyncio
        import nats

        async def main():
            nc = await nats.connect()
            js = nc.jetstream()

            await js.add_stream(name='hello', subjects=['hello'])
            ack = await js.publish('hello', b'Hello JS!')
            print(f'Ack: stream={ack.stream}, sequence={ack.seq}')
            # Ack: stream=hello, sequence=1
            await nc.close()

        if __name__ == '__main__':
            asyncio.run(main())

    """

    def __init__(
        self,
        conn: NATS,
        prefix: str = api.DEFAULT_PREFIX,
        domain: Optional[str] = None,
        timeout: float = 5,
        publish_async_max_pending: int = 4000,
        client_trace: Optional[api.ClientTrace] = None,
        publish_async_timeout: Optional[float] = None,
        publish_async_ack_handler: Optional[PublishAsyncAckHandler] = None,
        publish_async_err_handler: Optional[PublishAsyncErrHandler] = None,
    ) -> None:
        if publish_async_max_pending < 1:
            raise ValueError("nats: publish_async_max_pending must be at least 1")
        if publish_async_timeout is not None and publish_async_timeout < 0:
            raise ValueError("nats: publish_async_timeout must not be negative")
        self._prefix = prefix
        if domain is not None:
            self._prefix = f"$JS.{domain}.API"
        self._nc = conn
        self._timeout = timeout
        self._hdr_parser = BytesParser()
        self._client_trace = client_trace
        self._options = api.JetStreamOptions(
            api_prefix=prefix,
            domain=domain,
            default_timeout=timeout,
            client_trace=client_trace,
            publish_async_max_pending=publish_async_max_pending,
            publish_async_timeout=publish_async_timeout,
        )

        self._async_reply_prefix: Optional[bytearray] = None
        self._publish_async_futures: Dict[str, asyncio.Future] = {}

        self._publish_async_completed_event = asyncio.Event()
        self._publish_async_completed_event.set()

        self._publish_async_pending_semaphore = asyncio.Semaphore(publish_async_max_pending)
        self._publish_async_timeout = publish_async_timeout
        self._publish_async_ack_handler = publish_async_ack_handler
        self._publish_async_err_handler = publish_async_err_handler
        self._async_reply_sub: Optional[Subscription] = None

    @property
    def _jsm(self) -> JetStreamManager:
        return JetStreamManager(
            conn=self._nc,
            prefix=self._prefix,
            timeout=self._timeout,
            client_trace=self._client_trace,
        )

    @property
    def options(self) -> api.JetStreamOptions:
        """
        The options the context was created with.
        """
        return dataclasses.replace(self._options)

    async def _init_async_reply(self) -> None:
        self._publish_async_futures = {}

        self._async_reply_prefix = self._nc._inbox_prefix[:]
        self._async_reply_prefix.extend(b".")
        self._async_reply_prefix.extend(self._nc._nuid.next())
        self._async_reply_prefix.extend(b".")

        async_reply_subject = self._async_reply_prefix[:]
        async_reply_subject.extend(b"*")

        self._async_reply_sub = await self._nc.subscribe(async_reply_subject.decode(), cb=self._handle_async_reply)

    async def _handle_async_reply(self, msg: Msg) -> None:
        token = msg.subject[len(self._nc._inbox_prefix) + 22 + 2 :]
        future = self._publish_async_futures.get(token)

        if not future:
            return

        if future.done():
            return

        # Handle no responders
        if msg.headers and msg.headers.get(api.Header.STATUS) == NO_RESPONDERS_STATUS:
            if isinstance(future, PubAckFuture) and future._retries < future._max_retries:
                # Resend after the retry wait, as nats.go does.
                future._retries += 1
                asyncio.get_running_loop().call_later(
                    future._retry_wait, lambda: asyncio.ensure_future(self._resend_async(msg.subject, future))
                )
                return
            future.set_exception(nats.js.errors.NoStreamResponseError)
            return

        # Handle response errors
        try:
            ack = self._parse_pub_ack(msg.data)
        except nats.js.errors.Error as err:
            future.set_exception(err)
            return
        try:
            future.set_result(ack)
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass

    async def _resend_async(self, reply: str, future: PubAckFuture) -> None:
        if future.done():
            return
        msg = future.msg
        try:
            await self._nc.publish(msg.subject, msg.data, reply=reply, headers=msg.headers)
        except Exception as e:
            if not future.done():
                future.set_exception(e)

    def _publish_async_done(self, future: PubAckFuture) -> None:
        if future._timer is not None:
            future._timer.cancel()
            future._timer = None
        if future.cancelled():
            return
        err = future.exception()
        if err is None:
            if self._publish_async_ack_handler is not None:
                asyncio.ensure_future(
                    self._call_publish_async_handler(self._publish_async_ack_handler, future.msg, future.result())
                )
        elif self._publish_async_err_handler is not None:
            asyncio.ensure_future(self._call_publish_async_handler(self._publish_async_err_handler, future.msg, err))

    async def _call_publish_async_handler(self, handler: Callable, msg: Msg, result: Any) -> None:
        try:
            ret = handler(self, msg, result)
            if asyncio.iscoroutine(ret) or isinstance(ret, asyncio.Future):
                await ret
        except Exception as e:
            await self._nc._error_cb(e)

    async def publish(
        self,
        subject: str,
        payload: bytes = b"",
        timeout: Optional[float] = None,
        stream: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        msg_ttl: Optional[float] = None,
        *,
        msg_id: Optional[str] = None,
        expected_last_msg_id: Optional[str] = None,
        expected_last_sequence: Optional[int] = None,
        expected_last_subject_sequence: Optional[int] = None,
        expected_last_subject_sequence_subject: Optional[str] = None,
        schedule: Optional[api.MsgSchedule] = None,
        retry_attempts: int = 0,
        retry_wait: float = api.DEFAULT_PUB_RETRY_WAIT,
    ) -> api.PubAck:
        """
        publish emits a new message to JetStream and waits for acknowledgement.

        :param subject: Subject to publish to.
        :param payload: Message payload.
        :param timeout: Request timeout in seconds, covering any retries.
        :param stream: Expected stream name.
        :param headers: Message headers.
        :param msg_ttl: Per-message TTL in seconds (requires NATS Server 2.11+).
        :param msg_id: Message ID used by the stream for deduplication.
        :param expected_last_msg_id: Expected ID of the last message in the stream.
        :param expected_last_sequence: Expected sequence of the last message in the stream.
        :param expected_last_subject_sequence: Expected sequence of the last message
            on the subject (or on ``expected_last_subject_sequence_subject``).
        :param expected_last_subject_sequence_subject: Subject (may contain wildcards)
            that ``expected_last_subject_sequence`` refers to.
        :param schedule: Publish the message as a message schedule.
        :param retry_attempts: Times to retry when no stream responded (negative
            retries until the timeout).
        :param retry_wait: Seconds to wait before each retry.
        """
        hdr = self._publish_headers(
            headers,
            stream=stream,
            msg_ttl=msg_ttl,
            msg_id=msg_id,
            expected_last_msg_id=expected_last_msg_id,
            expected_last_sequence=expected_last_sequence,
            expected_last_subject_sequence=expected_last_subject_sequence,
            expected_last_subject_sequence_subject=expected_last_subject_sequence_subject,
            schedule=schedule,
        )
        if timeout is None:
            timeout = self._timeout

        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            try:
                msg = await self._nc.request(
                    subject,
                    payload,
                    timeout=timeout,
                    headers=hdr,
                )
                break
            except nats.errors.NoRespondersError:
                if 0 <= retry_attempts <= attempt:
                    raise nats.js.errors.NoStreamResponseError
            # Retry to ride out small blips such as leadership changes,
            # all within the original timeout as nats.go does.
            attempt += 1
            await asyncio.sleep(min(retry_wait, max(deadline - time.monotonic(), 0)))
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                raise nats.errors.TimeoutError

        return self._parse_pub_ack(msg.data)

    @staticmethod
    def _parse_pub_ack(data: bytes) -> api.PubAck:
        try:
            resp = json.loads(data)
        except ValueError:
            raise nats.js.errors.InvalidJSAckError
        if not isinstance(resp, dict):
            raise nats.js.errors.InvalidJSAckError
        if "error" in resp:
            raise nats.js.errors.APIError.from_error(resp["error"])
        if not resp.get("stream"):
            raise nats.js.errors.InvalidJSAckError
        return api.PubAck.from_response(resp)

    @staticmethod
    def _publish_headers(
        headers: Optional[Dict[str, Any]],
        stream: Optional[str] = None,
        msg_ttl: Optional[float] = None,
        msg_id: Optional[str] = None,
        expected_last_msg_id: Optional[str] = None,
        expected_last_sequence: Optional[int] = None,
        expected_last_subject_sequence: Optional[int] = None,
        expected_last_subject_sequence_subject: Optional[str] = None,
        schedule: Optional[api.MsgSchedule] = None,
    ) -> Optional[Dict[str, Any]]:
        hdr = headers
        if msg_id:
            hdr = hdr or {}
            hdr[api.Header.MSG_ID] = msg_id
        if expected_last_msg_id:
            hdr = hdr or {}
            hdr[api.Header.EXPECTED_LAST_MSG_ID] = expected_last_msg_id
        if stream is not None:
            hdr = hdr or {}
            hdr[api.Header.EXPECTED_STREAM] = stream
        if expected_last_sequence is not None:
            hdr = hdr or {}
            hdr[api.Header.EXPECTED_LAST_SEQUENCE] = str(expected_last_sequence)
        if expected_last_subject_sequence_subject and expected_last_subject_sequence is None:
            raise ValueError("nats: expected_last_subject_sequence is required with its subject")
        if expected_last_subject_sequence is not None:
            hdr = hdr or {}
            hdr[api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(expected_last_subject_sequence)
            if expected_last_subject_sequence_subject:
                hdr[api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT] = expected_last_subject_sequence_subject
        if msg_ttl is not None:
            hdr = hdr or {}
            # TTL header accepts seconds as integer or duration string
            hdr[api.Header.MSG_TTL] = str(int(msg_ttl))
        if schedule is not None:
            hdr = hdr or {}
            hdr.update(schedule.headers())
        return hdr

    async def publish_async(
        self,
        subject: str,
        payload: bytes = b"",
        wait_stall: Optional[float] = None,
        stream: Optional[str] = None,
        headers: Optional[Dict] = None,
        msg_ttl: Optional[float] = None,
        *,
        msg_id: Optional[str] = None,
        expected_last_msg_id: Optional[str] = None,
        expected_last_sequence: Optional[int] = None,
        expected_last_subject_sequence: Optional[int] = None,
        expected_last_subject_sequence_subject: Optional[str] = None,
        schedule: Optional[api.MsgSchedule] = None,
        retry_attempts: int = 0,
        retry_wait: float = api.DEFAULT_PUB_RETRY_WAIT,
    ) -> PubAckFuture:
        """
        emits a new message to JetStream and returns a future that can be awaited for acknowledgement.

        :param subject: Subject to publish to.
        :param payload: Message payload.
        :param wait_stall: Maximum time to wait for semaphore in seconds.
        :param stream: Expected stream name.
        :param headers: Message headers.
        :param msg_ttl: Per-message TTL in seconds (requires NATS Server 2.11+).
        :param retry_attempts: Times to resend the message when no stream responded.
        :param retry_wait: Seconds to wait before each resend.

        The other keyword arguments set the same headers as in :meth:`publish`.

        The returned :class:`PubAckFuture` fails with AsyncPublishTimeoutError
        when the context's ``publish_async_timeout`` passes without an
        acknowledgement, and with JetStreamPublisherClosedError when
        :meth:`cleanup_publisher` is called first.
        """

        if not self._async_reply_prefix:
            await self._init_async_reply()
        assert self._async_reply_prefix

        hdr = self._publish_headers(
            headers,
            stream=stream,
            msg_ttl=msg_ttl,
            msg_id=msg_id,
            expected_last_msg_id=expected_last_msg_id,
            expected_last_sequence=expected_last_sequence,
            expected_last_subject_sequence=expected_last_subject_sequence,
            expected_last_subject_sequence_subject=expected_last_subject_sequence_subject,
            schedule=schedule,
        )

        try:
            await asyncio.wait_for(self._publish_async_pending_semaphore.acquire(), timeout=wait_stall)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            raise nats.js.errors.TooManyStalledMsgsError

        # Use a new NUID + couple of unique token bytes to identify the request,
        # then use the future to get the response.
        token = self._nc._nuid.next()
        token.extend(token_hex(2).encode())
        inbox = self._async_reply_prefix[:]
        inbox.extend(token)

        future = PubAckFuture(
            Msg(_client=self._nc, subject=subject, data=payload, headers=hdr),
            max_retries=retry_attempts,
            retry_wait=retry_wait,
        )
        futures = self._publish_async_futures

        def handle_done(future):
            # Only for the futures of this publisher, which cleanup_publisher replaces.
            if futures.pop(token.decode(), None) is not None and len(futures) == 0:
                self._publish_async_completed_event.set()

            self._publish_async_pending_semaphore.release()
            self._publish_async_done(future)

        future.add_done_callback(handle_done)

        futures[token.decode()] = future

        if self._publish_async_completed_event.is_set():
            self._publish_async_completed_event.clear()

        if self._publish_async_timeout:
            future._timer = asyncio.get_running_loop().call_later(
                self._publish_async_timeout, self._expire_async_publish, future
            )

        try:
            await self._nc.publish(subject, payload, reply=inbox.decode(), headers=hdr)
        except BaseException:
            future.cancel()
            raise

        return future

    @staticmethod
    def _expire_async_publish(future: PubAckFuture) -> None:
        future._timer = None
        if not future.done():
            future.set_exception(nats.js.errors.AsyncPublishTimeoutError())

    async def publish_msg_async(self, msg: Msg, **kwargs: Any) -> PubAckFuture:
        """
        publish_msg_async is like :meth:`publish_async` for a message, taking
        its subject, data and headers. The message must not have a reply
        subject, which is used for the acknowledgement.
        """
        if msg.reply:
            raise nats.js.errors.AsyncPublishReplySubjectSetError
        return await self.publish_async(msg.subject, msg.data, headers=msg.headers, **kwargs)

    def publish_async_pending(self) -> int:
        """
        returns the number of pending async publishes.
        """
        return len(self._publish_async_futures)

    async def publish_async_completed(self) -> None:
        """
        waits for all pending async publishes to be completed.
        """
        await self._publish_async_completed_event.wait()

    async def publish_async_complete(self, timeout: Optional[float] = None) -> None:
        """
        waits up to ``timeout`` seconds for all pending async publishes to be
        completed, raising nats.errors.TimeoutError if some are still pending.
        """
        try:
            await asyncio.wait_for(self._publish_async_completed_event.wait(), timeout)
        except asyncio.TimeoutError:
            raise nats.errors.TimeoutError

    async def cleanup_publisher(self) -> None:
        """
        cleanup_publisher fails all pending async publishes with
        JetStreamPublisherClosedError and removes the subscription for their
        acknowledgements. Later async publishes start a new one.
        """
        futures = self._publish_async_futures
        sub = self._async_reply_sub
        self._publish_async_futures = {}
        self._async_reply_prefix = None
        self._async_reply_sub = None
        for future in list(futures.values()):
            if not future.done():
                future.set_exception(nats.js.errors.JetStreamPublisherClosedError())
        futures.clear()
        self._publish_async_completed_event.set()
        if sub is not None:
            try:
                await sub.unsubscribe()
            except nats.errors.Error:
                pass

    async def subscribe(
        self,
        subject: str,
        queue: Optional[str] = None,
        cb: Optional[Callback] = None,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[api.ConsumerConfig] = None,
        manual_ack: bool = False,
        ordered_consumer: bool = False,
        idle_heartbeat: Optional[float] = None,
        flow_control: bool = False,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        deliver_policy: Optional[api.DeliverPolicy] = None,
        headers_only: Optional[bool] = None,
        inactive_threshold: Optional[float] = None,
    ) -> PushSubscription:
        """Create consumer if needed and push-subscribe to it.

        1. Check if consumer exists.
        2. Creates consumer if needed.
        3. Calls `subscribe_bind`.

        :param subject: Subject from a stream from JetStream.
        :param queue: Deliver group name from a set a of queue subscribers.
        :param durable: Name of the durable consumer to which the the subscription should be bound.
        :param stream: Name of the stream to which the subscription should be bound. If not set,
          then the client will automatically look it up based on the subject.
        :param manual_ack: Disables auto acking for async subscriptions.
        :param ordered_consumer: Enable ordered consumer mode.
        :param idle_heartbeat: Enable Heartbeats for a consumer to detect failures.
        :param flow_control: Enable Flow Control for a consumer.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect()
                js = nc.jetstream()

                await js.add_stream(name='hello', subjects=['hello'])
                await js.publish('hello', b'Hello JS!')

                async def cb(msg):
                  print('Received:', msg)

                # Ephemeral Async Subscribe
                await js.subscribe('hello', cb=cb)

                # Durable Async Subscribe
                # NOTE: Only one subscription can be bound to a durable name. It also auto acks by default.
                await js.subscribe('hello', cb=cb, durable='foo')

                # Durable Sync Subscribe
                # NOTE: Sync subscribers do not auto ack.
                await js.subscribe('hello', durable='bar')

                # Queue Async Subscribe
                # NOTE: Here 'workers' becomes deliver_group, durable name and queue name.
                await js.subscribe('hello', 'workers', cb=cb)

            if __name__ == '__main__':
                asyncio.run(main())

        """
        if stream is None:
            stream = await self._jsm.find_stream_name_by_subject(subject)

        deliver = None
        consumer = None

        # If using a queue, that will be the consumer/durable name.
        if queue:
            if durable and durable != queue:
                raise nats.js.errors.Error(f"cannot create queue subscription '{queue}' to consumer '{durable}'")
            else:
                durable = queue

        consumer_info = None
        # Ephemeral subscribe always has to be auto created.
        should_create = not durable
        if durable:
            try:
                # TODO: Detect configuration drift with any present durable consumer.
                consumer_info = await self._jsm.consumer_info(stream, durable)
                consumer = durable
            except nats.js.errors.NotFoundError:
                should_create = True

        if consumer_info is not None:
            config = consumer_info.config
            # At this point, we know the user wants push mode, and the JS consumer is
            # really push mode.
            deliver_group = consumer_info.config.deliver_group
            if not deliver_group:
                # Prevent an user from attempting to create a queue subscription on
                # a JS consumer that was not created with a deliver group.
                if queue:
                    # TODO: Currently, this would not happen in client
                    # since the queue name is used as durable name.
                    raise nats.js.errors.Error(
                        "cannot create a queue subscription for a consumer without a deliver group"
                    )
                elif consumer_info.push_bound:
                    # Need to reject a non queue subscription to a non queue consumer
                    # if the consumer is already bound.
                    raise nats.js.errors.ConsumerHasActiveSubscriptionError(
                        "consumer is already bound to a subscription"
                    )
            else:
                if not queue:
                    raise nats.js.errors.Error(
                        f"cannot create a subscription for a consumer with a deliver group {deliver_group}"
                    )
                elif queue != deliver_group:
                    raise nats.js.errors.Error(
                        f"cannot create a queue subscription {queue} for a consumer "
                        f"with a deliver group {deliver_group}"
                    )
        elif should_create:
            # Auto-create consumer if none found.
            if config is None:
                config = api.ConsumerConfig()
            if not config.durable_name:
                config.durable_name = durable
            if not config.deliver_group:
                config.deliver_group = queue
            if not config.headers_only:
                config.headers_only = headers_only
            if deliver_policy:
                # NOTE: deliver_policy is defaulting to ALL so check is different for this one.
                config.deliver_policy = deliver_policy
            if inactive_threshold:
                config.inactive_threshold = inactive_threshold

            # Create inbox for push consumer, if deliver_subject is not assigned already.
            if config.deliver_subject is None:
                deliver = self._nc.new_inbox()
                config.deliver_subject = deliver

            # Auto created consumers use the filter subject, unless filter_subjects is set.
            if not config.filter_subjects:
                config.filter_subject = subject

            # Heartbeats / FlowControl
            config.flow_control = flow_control
            if idle_heartbeat:
                config.idle_heartbeat = idle_heartbeat
            else:
                idle_heartbeat = config.idle_heartbeat or 5

            # Enable ordered consumer mode where at most there is
            # one message being delivered at a time.
            if ordered_consumer:
                config.flow_control = True
                config.ack_policy = api.AckPolicy.NONE
                config.max_deliver = 1
                config.ack_wait = 22 * 3600  # 22 hours
                config.idle_heartbeat = idle_heartbeat
                config.num_replicas = 1
                config.mem_storage = True

            consumer_info = await self._jsm.add_consumer(stream, config=config)
            consumer = consumer_info.name

        if consumer is None:
            raise TypeError("cannot detect consumer")
        if config is None:
            raise TypeError("config is required for existing durable consumer")
        return await self.subscribe_bind(
            cb=cb,
            stream=stream,
            config=config,
            manual_ack=manual_ack,
            ordered_consumer=ordered_consumer,
            consumer=consumer,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )

    async def subscribe_bind(
        self,
        stream: str,
        config: api.ConsumerConfig,
        consumer: str,
        cb: Optional[Callback] = None,
        manual_ack: bool = False,
        ordered_consumer: bool = False,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
    ) -> PushSubscription:
        """Push-subscribe to an existing consumer."""
        # By default, async subscribers wrap the original callback and
        # auto ack the messages as they are delivered.
        #
        # In case ack policy is none then we also do not require to ack.
        # Sourcing consumers (flow_control) are acknowledged by the sourcing
        # server through flow control messages, not by an ack reply per
        # message, so they must not be wrapped either.
        #
        # Compared by value rather than identity so a policy that arrived as a
        # plain string from the server is matched too.
        #
        if cb and (not manual_ack) and config.ack_policy not in (api.AckPolicy.NONE, api.AckPolicy.FLOW_CONTROL):
            cb = self._auto_ack_callback(cb)
        if config.deliver_subject is None:
            raise TypeError("config.deliver_subject is required")
        sub = await self._nc.subscribe(
            subject=config.deliver_subject,
            queue=config.deliver_group or "",
            cb=cb,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        psub = JetStreamContext.PushSubscription(self, sub, stream, consumer)

        # Keep state to support ordered consumers.
        sub._jsi = JetStreamContext._JSI(
            js=self,
            conn=self._nc,
            stream=stream,
            ordered=ordered_consumer,
            psub=psub,
            sub=sub,
            ccreq=config,
        )

        if config.idle_heartbeat:
            sub._jsi._hbtask = asyncio.create_task(sub._jsi.activity_check())

        if ordered_consumer:
            sub._jsi._fctask = asyncio.create_task(sub._jsi.check_flow_control_response())

        return psub

    @staticmethod
    def _auto_ack_callback(callback: Callback) -> Callback:
        async def new_callback(msg: Msg) -> None:
            await callback(msg)
            try:
                await msg.ack()
            except nats.errors.MsgAlreadyAckdError:
                pass

        return new_callback

    async def pull_subscribe(
        self,
        subject: str,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[api.ConsumerConfig] = None,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        inbox_prefix: Optional[bytes] = None,
        priority_group: Optional[str] = None,
    ) -> JetStreamContext.PullSubscription:
        """Create consumer and pull subscription.

        1. Find stream name by subject if `stream` is not passed.
        2. Create consumer with the given `config` if not created.
        3. Call `pull_subscribe_bind`.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect()
                js = nc.jetstream()

                await js.add_stream(name='mystream', subjects=['foo'])
                await js.publish('foo', b'Hello World!')

                sub = await js.pull_subscribe('foo', stream='mystream')

                msgs = await sub.fetch()
                msg = msgs[0]
                await msg.ack()

                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())

        """
        if stream is None:
            stream = await self._jsm.find_stream_name_by_subject(subject)

        if config and config.priority_groups and priority_group is None:
            raise ValueError("nats: priority_group is required when consumer has priority_groups configured")

        should_create = True
        try:
            if durable:
                await self._jsm.consumer_info(stream, durable)
                should_create = False
        except nats.js.errors.NotFoundError:
            pass

        consumer_name = durable
        if should_create:
            # If not found then attempt to create with the defaults.
            if config is None:
                config = api.ConsumerConfig()

            # Auto created consumers use the filter subject, unless filter_subjects is set.
            if not config.filter_subjects:
                config.filter_subject = subject

            if durable:
                config.name = durable
                config.durable_name = durable
            else:
                consumer_name = self._nc._nuid.next().decode()
                config.name = consumer_name

            # Auto created consumers use the priority group, unless priority_groups is set.
            if not config.priority_groups and priority_group:
                config.priority_groups = [priority_group]

            await self._jsm.add_consumer(stream, config=config)

        return await self.pull_subscribe_bind(
            durable=durable,
            stream=stream,
            inbox_prefix=inbox_prefix,
            pending_bytes_limit=pending_bytes_limit,
            pending_msgs_limit=pending_msgs_limit,
            name=consumer_name,
            priority_group=priority_group,
        )

    async def pull_subscribe_bind(
        self,
        consumer: Optional[str] = None,
        stream: Optional[str] = None,
        inbox_prefix: Optional[bytes] = None,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        name: Optional[str] = None,
        durable: Optional[str] = None,
        priority_group: Optional[str] = None,
    ) -> JetStreamContext.PullSubscription:
        """
        pull_subscribe returns a `PullSubscription` that can be delivered messages
        from a JetStream pull based consumer by calling `sub.fetch`.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect()
                js = nc.jetstream()

                await js.add_stream(name='mystream', subjects=['foo'])
                await js.publish('foo', b'Hello World!')

                msgs = await sub.fetch()
                msg = msgs[0]
                await msg.ack()

                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())

        """
        if not stream:
            raise nats.js.errors.StreamNameRequiredError()

        if inbox_prefix is None:
            inbox_prefix = bytes(self._nc._inbox_prefix[:]) + b"."

        deliver = inbox_prefix + self._nc._nuid.next()
        sub = await self._nc.subscribe(
            deliver.decode(),
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        consumer_name = None
        # In nats.py v2.7.0 changing the first arg to be 'consumer' instead of 'durable',
        # but continue to support for backwards compatibility.
        if durable:
            consumer_name = durable
        elif name:
            # This should not be common and 'consumer' arg preferred instead but support anyway.
            consumer_name = name
        else:
            consumer_name = consumer
        return JetStreamContext.PullSubscription(
            js=self,
            sub=sub,
            stream=stream,
            consumer=consumer_name,
            deliver=deliver,
            group=priority_group,
        )

    @classmethod
    def is_status_msg(cls, msg: Optional[Msg]) -> Optional[str]:
        if msg is None or msg.headers is None:
            return None
        return msg.headers.get(api.Header.STATUS)

    @classmethod
    def _is_processable_msg(cls, status: Optional[str], msg: Msg) -> bool:
        if not status:
            return True
        # Skip most 4XX errors and do not raise exception.
        if JetStreamContext._is_temporary_error(status, msg):
            return False
        raise nats.js.errors.APIError.from_msg(msg)

    @classmethod
    def _is_temporary_error(cls, status: Optional[str], msg: Optional[Msg] = None) -> bool:
        if status == api.StatusCode.CONFLICT:
            # Some conflicts would repeat on every retry of the same request.
            return msg is None or not JetStreamContext._is_terminal_conflict(msg)
        if (
            status == api.StatusCode.NO_MESSAGES
            or status == api.StatusCode.REQUEST_TIMEOUT
            or status == api.StatusCode.PIN_ID_MISMATCH
        ):
            return True
        else:
            return False

    @classmethod
    def _is_terminal_conflict(cls, msg: Msg) -> bool:
        desc = msg.headers.get(api.Header.DESCRIPTION, "") if msg.headers else ""
        return desc.startswith(_TERMINAL_CONFLICTS)

    @classmethod
    def _is_pin_id_mismatch_error(cls, status: Optional[str]) -> bool:
        return status == api.StatusCode.PIN_ID_MISMATCH

    @classmethod
    def _is_heartbeat(cls, status: Optional[str]) -> bool:
        if status == api.StatusCode.CONTROL_MESSAGE:
            return True
        else:
            return False

    @classmethod
    def _time_until(cls, timeout: Optional[float], start_time: float) -> Optional[float]:
        if timeout is None:
            return None
        return timeout - (time.monotonic() - start_time)

    class _JSI:
        def __init__(
            self,
            js: JetStreamContext,
            conn: NATS,
            stream: str,
            ordered: Optional[bool],
            psub: JetStreamContext.PushSubscription,
            sub: Subscription,
            ccreq: api.ConsumerConfig,
        ) -> None:
            self._conn = conn
            self._js = js
            self._stream = stream
            self._ordered = ordered
            self._psub = psub
            self._sub = sub
            self._ccreq = ccreq

            # Heartbeat
            self._hbtask = None
            self._hbi = None
            if ccreq and ccreq.idle_heartbeat:
                self._hbi = ccreq.idle_heartbeat

            # Ordered Consumer implementation.
            self._dseq = 1
            self._sseq = 0
            self._cmeta: Optional[str] = None
            self._fcr: Optional[str] = None
            self._fcd = 0
            self._fciseq = 0
            self._active: Optional[bool] = True
            self._fctask = None

        def track_sequences(self, reply: str) -> None:
            self._fciseq += 1
            self._cmeta = reply

        def schedule_flow_control_response(self, reply: str) -> None:
            self._active = True
            self._fcr = reply
            self._fcd = self._fciseq

        def get_js_delivered(self):
            if self._sub._cb:
                return self._sub.delivered
            return self._fciseq - self._sub._pending_queue.qsize()

        async def activity_check(self):
            # Can at most miss two heartbeats.
            hbc_threshold = 2
            while True:
                try:
                    if self._conn.is_closed:
                        break

                    # Wait for all idle heartbeats to be received,
                    # one of them would have toggled the state of the
                    # consumer back to being active.
                    await asyncio.sleep(self._hbi * hbc_threshold)
                    active = self._active
                    self._active = False
                    if not active:
                        if self._ordered:
                            await self.reset_ordered_consumer(self._sseq + 1)
                except asyncio.CancelledError:
                    break

        async def check_flow_control_response(self):
            while True:
                try:
                    if self._conn.is_closed:
                        break

                    if (self._fciseq - self._psub._pending_queue.qsize()) >= self._fcd:
                        fc_reply = self._fcr
                        try:
                            if fc_reply:
                                await self._conn.publish(fc_reply)
                        except Exception:
                            pass
                        self._fcr = None
                        self._fcd = 0
                    await asyncio.sleep(0.25)
                except asyncio.CancelledError:
                    break

        async def check_for_sequence_mismatch(self, msg: Msg) -> Optional[bool]:
            self._active = True
            if not self._cmeta:
                return None

            tokens = msg._get_metadata_fields(self._cmeta)
            dseq = int(tokens[6])  # consumer sequence
            ldseq = None
            if msg.headers:
                ldseq_str = msg.headers.get(api.Header.LAST_CONSUMER)
                if ldseq_str:
                    ldseq = int(ldseq_str)
            did_reset = None

            if ldseq != dseq:
                sseq = int(tokens[5])  # stream sequence

                if self._ordered:
                    did_reset = await self.reset_ordered_consumer(self._sseq + 1)
                else:
                    ecs = nats.js.errors.ConsumerSequenceMismatchError(
                        stream_resume_sequence=sseq,
                        consumer_sequence=dseq,
                        last_consumer_sequence=ldseq,
                    )
                    await self._conn._error_cb(ecs)
            return did_reset

        async def reset_ordered_consumer(self, sseq: Optional[int]) -> bool:
            # FIXME: Handle AUTO_UNSUB called previously to this.

            # Replace current subscription.
            osid = self._sub._id
            self._conn._remove_sub(osid)
            new_deliver = self._conn.new_inbox()

            # Place new one.
            self._conn._sid += 1
            nsid = self._conn._sid
            self._conn._subs[nsid] = self._sub
            self._sub._id = nsid
            self._psub._id = nsid

            # unsub
            await self._conn._send_unsubscribe(osid)

            # resub
            self._sub._subject = new_deliver
            await self._conn._send_subscribe(self._sub)

            # relinquish cpu to let proto commands make it to the server.
            await asyncio.sleep(0)

            # Reset some items in jsi.
            self._cmeta = None
            self._dseq = 1

            # Reset consumer request for starting policy.
            config = self._ccreq
            config.deliver_subject = new_deliver
            config.deliver_policy = api.DeliverPolicy.BY_START_SEQUENCE
            config.opt_start_seq = sseq
            self._ccreq = config

            # Handle the creation of new consumer in a background task
            # to avoid blocking the process_msg coroutine further
            # when making the request.
            asyncio.create_task(self.recreate_consumer())

            return True

        async def recreate_consumer(self) -> None:
            try:
                cinfo = await self._js._jsm.add_consumer(self._stream, config=self._ccreq, timeout=self._js._timeout)
                self._psub._consumer = cinfo.name
            except Exception as err:
                await self._conn._error_cb(err)

    class PushSubscription(Subscription):
        """
        PushSubscription is a subscription that is delivered messages.
        """

        def __init__(
            self,
            js: JetStreamContext,
            sub: Subscription,
            stream: str,
            consumer: str,
        ) -> None:
            self._js = js
            self._stream = stream
            self._consumer = consumer

            self._sub = sub
            self._conn = sub._conn
            self._id = sub._id
            self._subject = sub._subject
            self._queue = sub._queue
            self._max_msgs = sub._max_msgs
            self._received = sub._received
            self._cb = sub._cb
            self._future = sub._future
            self._closed = sub._closed

            # Per subscription message processor.
            self._pending_msgs_limit = sub._pending_msgs_limit
            self._pending_bytes_limit = sub._pending_bytes_limit
            self._pending_queue = sub._pending_queue
            self._pending_size = sub._pending_size
            self._wait_for_msgs_task = sub._wait_for_msgs_task
            self._message_iterator = sub._message_iterator
            self._pending_next_msgs_calls = sub._pending_next_msgs_calls

        async def consumer_info(self) -> api.ConsumerInfo:
            """
            consumer_info gets the current info of the consumer from this subscription.
            """
            info = await self._js._jsm.consumer_info(
                self._stream,
                self._consumer,
            )
            return info

        @property
        def delivered(self) -> int:
            """
            Number of delivered messages to this subscription so far.
            """
            return self._sub._received

        @delivered.setter
        def delivered(self, value):
            self._sub._received = value

        @property
        def _pending_size(self):
            return self._sub._pending_size

        @_pending_size.setter
        def _pending_size(self, value):
            self._sub._pending_size = value

        async def next_msg(self, timeout: Optional[float] = 1.0) -> Msg:
            """
            :params timeout: Time in seconds to wait for next message before timing out.
            :raises nats.errors.TimeoutError:

            next_msg can be used to retrieve the next message from a stream of messages using
            await syntax, this only works when not passing a callback on `subscribe`::
            """
            msg = await self._sub.next_msg(timeout)

            # In case there is a flow control reply present need to handle here.
            if self._sub and self._sub._jsi:
                self._sub._jsi._active = True
                if self._sub._jsi.get_js_delivered() >= self._sub._jsi._fciseq:
                    fc_reply = self._sub._jsi._fcr
                    if fc_reply:
                        await self._conn.publish(fc_reply)
                        self._sub._jsi._fcr = None
            return msg

        async def unsubscribe(self, limit: int = 0):
            """
            Unsubscribes from a subscription, canceling any heartbeat and flow control tasks,
            and optionally limits the number of messages to process before unsubscribing.
            """
            await super().unsubscribe(limit)

            if self._sub._jsi._hbtask:
                self._sub._jsi._hbtask.cancel()

            if self._sub._jsi._fctask:
                self._sub._jsi._fctask.cancel()

    class PullSubscription:
        """
        PullSubscription is a subscription that can fetch messages.
        """

        def __init__(
            self,
            js: JetStreamContext,
            sub: Subscription,
            stream: str,
            consumer: str,
            deliver: bytes,
            group: Optional[str] = None,
        ) -> None:
            # JS/JSM context
            self._js = js
            self._nc = js._nc

            # NATS Subscription
            self._sub = sub
            self._stream = stream
            self._consumer = consumer
            prefix = self._js._prefix
            self._nms = f"{prefix}.CONSUMER.MSG.NEXT.{stream}.{consumer}"
            self._deliver = deliver.decode()
            self._pin_id: Optional[str] = None
            self._group = group

        @property
        def pending_msgs(self) -> int:
            """
            Number of delivered messages by the NATS Server that are being buffered
            in the pending queue.
            """
            return self._sub._pending_queue.qsize()

        @property
        def pending_bytes(self) -> int:
            """
            Size of data sent by the NATS Server that is being buffered
            in the pending queue.
            """
            return self._sub._pending_size

        @property
        def delivered(self) -> int:
            """
            Number of delivered messages to this subscription so far.
            """
            return self._sub._received

        @property
        def pin_id(self) -> Optional[str]:
            """
            Pin id assigned by the server when the consumer uses
            ``PriorityPolicy.PINNED``, or ``None`` if not pinned.
            """
            return self._pin_id

        def _build_next_req(
            self,
            batch: int,
            expires: Optional[int] = None,
            heartbeat: Optional[float] = None,
            no_wait: bool = False,
            min_pending: Optional[int] = None,
            min_ack_pending: Optional[int] = None,
            priority: Optional[int] = None,
        ) -> Dict[str, Any]:
            next_req: Dict[str, Any] = {"batch": batch}
            if expires:
                next_req["expires"] = int(expires)
            if heartbeat:
                next_req["idle_heartbeat"] = int(heartbeat * 1_000_000_000)  # to nanoseconds
            if no_wait:
                next_req["no_wait"] = True
            if self._group:
                next_req["group"] = self._group
            if self._pin_id:
                next_req["id"] = self._pin_id
            if min_pending is not None:
                next_req["min_pending"] = min_pending
            if min_ack_pending is not None:
                next_req["min_ack_pending"] = min_ack_pending
            if priority is not None:
                next_req["priority"] = priority
            return next_req

        async def unsubscribe(self) -> None:
            """
            unsubscribe destroys the inboxes of the pull subscription making it
            unable to continue to receive messages.
            """
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            await self._sub.unsubscribe()

        async def consumer_info(self) -> api.ConsumerInfo:
            """
            consumer_info gets the current info of the consumer from this subscription.
            """
            info = await self._js._jsm.consumer_info(self._stream, self._consumer)
            return info

        async def fetch(
            self,
            batch: int = 1,
            timeout: Optional[float] = 5,
            heartbeat: Optional[float] = None,
            min_pending: Optional[int] = None,
            min_ack_pending: Optional[int] = None,
            priority: Optional[int] = None,
        ) -> List[Msg]:
            """
            fetch makes a request to JetStream to be delivered a set of messages.

            :param batch: Number of messages to fetch from server.
            :param timeout: Max duration of the fetch request before it expires.
            :param heartbeat: Idle Heartbeat interval in seconds for the fetch request.
            :param min_pending: Only deliver when the consumer has at least this many
                pending messages. Requires ``PriorityPolicy.OVERFLOW``.
            :param min_ack_pending: Only deliver when the consumer has at least this
                many unacknowledged messages. Requires ``PriorityPolicy.OVERFLOW``.
            :param priority: Priority of this request from 0 (highest) to 9.
                Requires ``PriorityPolicy.PRIORITIZED``.

            ::

                import asyncio
                import nats

                async def main():
                    nc = await nats.connect()
                    js = nc.jetstream()

                    await js.add_stream(name='mystream', subjects=['foo'])
                    await js.publish('foo', b'Hello World!')

                    msgs = await sub.fetch(5)
                    for msg in msgs:
                      await msg.ack()

                    await nc.close()

                if __name__ == '__main__':
                    asyncio.run(main())
            """
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            # FIXME: Check connection is not closed, etc...
            if batch < 1:
                raise ValueError("nats: invalid batch size")
            if timeout is not None and timeout <= 0:
                raise ValueError("nats: invalid fetch timeout")
            if min_pending is not None and min_pending <= 0:
                raise ValueError("nats: min_pending must be more than 0")
            if min_ack_pending is not None and min_ack_pending <= 0:
                raise ValueError("nats: min_ack_pending must be more than 0")
            if priority is not None and not (0 <= priority <= 9):
                raise ValueError("nats: priority must be 0-9")

            expires = int(timeout * 1_000_000_000) - 100_000 if timeout else None
            if batch == 1:
                msg = await self._fetch_one(expires, timeout, heartbeat, min_pending, min_ack_pending, priority)
                return [msg]
            msgs = await self._fetch_n(batch, expires, timeout, heartbeat, min_pending, min_ack_pending, priority)
            return msgs

        async def _fetch_one(
            self,
            expires: Optional[int],
            timeout: Optional[float],
            heartbeat: Optional[float] = None,
            min_pending: Optional[int] = None,
            min_ack_pending: Optional[int] = None,
            priority: Optional[int] = None,
        ) -> Msg:
            queue = self._sub._pending_queue

            # Check the next message in case there are any.
            while not queue.empty():
                try:
                    msg = queue.get_nowait()
                    self._sub._pending_size -= len(msg.data)
                    status = JetStreamContext.is_status_msg(msg)
                    if status:
                        # Discard status messages at this point since were meant
                        # for other fetch requests.
                        continue
                    return msg
                except Exception:
                    # Fallthrough to make request in case this failed.
                    pass

            # Make lingering request with expiration and wait for response.
            async def send_next_request() -> None:
                next_req = self._build_next_req(
                    1,
                    expires=expires,
                    heartbeat=heartbeat,
                    min_pending=min_pending,
                    min_ack_pending=min_ack_pending,
                    priority=priority,
                )
                await self._nc.publish(
                    self._nms,
                    json.dumps(next_req).encode(),
                    self._deliver,
                )

            await send_next_request()

            start_time = time.monotonic()
            got_any_response = False
            resent_without_pin_id = False
            while True:
                try:
                    deadline = JetStreamContext._time_until(timeout, start_time)
                    # Wait for the response or raise timeout.
                    msg = await self._sub.next_msg(timeout=deadline)

                    # Should have received at least a processable message at this point,
                    status = JetStreamContext.is_status_msg(msg)
                    if status:
                        if JetStreamContext._is_heartbeat(status):
                            got_any_response = True
                            continue

                        if JetStreamContext._is_pin_id_mismatch_error(status):
                            # The pin this request carried is no longer valid and
                            # the server has discarded the request. Drop the stale
                            # id and re-issue once without it so the fetch can
                            # recover within its own deadline instead of waiting
                            # on a request that will never be served.
                            self._pin_id = None
                            got_any_response = True
                            if not resent_without_pin_id:
                                resent_without_pin_id = True
                                await send_next_request()
                                continue

                        # In case of a temporary error, treat it as a timeout to retry.
                        if JetStreamContext._is_temporary_error(status, msg):
                            raise nats.errors.TimeoutError
                        else:
                            # Any other type of status message is an error.
                            raise nats.js.errors.APIError.from_msg(msg)
                    else:
                        pin_id = msg.headers.get(api.Header.PIN_ID) if msg.headers else None
                        if pin_id:
                            self._pin_id = pin_id
                        return msg
                except asyncio.TimeoutError:
                    deadline = JetStreamContext._time_until(timeout, start_time)
                    if deadline is not None and deadline < 0:
                        # No response from the consumer could have been
                        # due to a reconnect while the fetch request,
                        # the JS API not responding on time, or maybe
                        # there were no messages yet.
                        if got_any_response:
                            raise FetchTimeoutError
                        raise

        async def _fetch_n(
            self,
            batch: int,
            expires: Optional[int],
            timeout: Optional[float],
            heartbeat: Optional[float] = None,
            min_pending: Optional[int] = None,
            min_ack_pending: Optional[int] = None,
            priority: Optional[int] = None,
        ) -> List[Msg]:
            msgs = []
            queue = self._sub._pending_queue
            start_time = time.monotonic()
            got_any_response = False
            needed = batch

            # Fetch as many as needed from the internal pending queue.
            msg: Optional[Msg]

            while not queue.empty():
                try:
                    msg = queue.get_nowait()
                    self._sub._pending_size -= len(msg.data)
                    status = JetStreamContext.is_status_msg(msg)
                    if status:
                        # Discard status messages at this point since were meant
                        # for other fetch requests.
                        continue
                    needed -= 1
                    msgs.append(msg)
                except Exception:
                    pass

            # First request: Use no_wait to synchronously get as many available
            # based on the batch size until server sends 'No Messages' status msg.
            # Omit `expires` when the drain step already found messages: NATS
            # server ignores no_wait when expires is present, treating the probe
            # as a lingering pull and blocking for the full expires duration.
            # Without expires the server honors no_wait immediately, so Phase 3
            # returns quickly with any additional server-side messages or a 404,
            # and the existing `if len(msgs) > 0` guard returns the collected
            # messages without delay. When the drain step found nothing expires
            # is still included to preserve the intended behaviour.
            next_req = self._build_next_req(
                needed,
                expires=expires if not msgs else None,
                heartbeat=heartbeat,
                no_wait=True,
                min_pending=min_pending,
                min_ack_pending=min_ack_pending,
                priority=priority,
            )
            await self._nc.publish(
                self._nms,
                json.dumps(next_req).encode(),
                self._deliver,
            )
            await asyncio.sleep(0)

            try:
                msg = await self._sub.next_msg(timeout)
            except asyncio.TimeoutError:
                # Return any message that was already available in the internal queue.
                if msgs:
                    return msgs
                raise

            got_any_response = False

            status = JetStreamContext.is_status_msg(msg)
            if JetStreamContext._is_heartbeat(status):
                # Mark that we got any response from the server so this is not
                # a possible i/o timeout error or due to a disconnection.
                got_any_response = True
                pass
            elif JetStreamContext._is_pin_id_mismatch_error(status):
                self._pin_id = None
                got_any_response = True
            elif JetStreamContext._is_processable_msg(status, msg):
                # First processable message received, do not raise error from now.
                pin_id = msg.headers.get(api.Header.PIN_ID) if msg.headers else None
                if pin_id:
                    self._pin_id = pin_id
                msgs.append(msg)
                needed -= 1

                try:
                    for i in range(0, needed):
                        deadline = JetStreamContext._time_until(timeout, start_time)
                        msg = await self._sub.next_msg(timeout=deadline)
                        status = JetStreamContext.is_status_msg(msg)
                        if status == api.StatusCode.NO_MESSAGES or status == api.StatusCode.REQUEST_TIMEOUT:
                            # No more messages after this so fallthrough
                            # after receiving the rest.
                            break
                        elif JetStreamContext._is_heartbeat(status):
                            # Skip heartbeats.
                            got_any_response = True
                            continue
                        elif JetStreamContext._is_pin_id_mismatch_error(status):
                            self._pin_id = None
                            got_any_response = True
                        elif JetStreamContext._is_processable_msg(status, msg):
                            pin_id = msg.headers.get(api.Header.PIN_ID) if msg.headers else None
                            if pin_id:
                                self._pin_id = pin_id
                            needed -= 1
                            msgs.append(msg)
                except asyncio.TimeoutError:
                    # Ignore any timeout errors at this point since
                    # at least one message has already arrived.
                    pass

            # Stop if have some messages.
            if len(msgs) > 0:
                return msgs

            # Second request: lingering request that will block until new messages
            # are made available and delivered to the client.
            #
            # Use the *remaining* deadline as the request's expires rather than
            # the original full timeout.  The original expires was computed at
            # the very start of fetch() and may be nearly exhausted by the time
            # we reach this point (e.g. when the server's 408 for the no-wait
            # probe arrives just before the asyncio timer fires).  Sending a
            # lingering request with the full original expires in that situation
            # creates an orphaned pull request that survives on the server long
            # after the client has timed out, capturing the next published
            # message and causing the subsequent fetch() to stall for the full
            # timeout window.
            async def send_lingering_request() -> None:
                deadline = JetStreamContext._time_until(timeout, start_time)
                if deadline is not None and deadline <= 0:
                    raise asyncio.TimeoutError

                if deadline is not None:
                    remaining_expires = int(deadline * 1_000_000_000) - 100_000
                    if remaining_expires <= 0:
                        raise asyncio.TimeoutError
                else:
                    remaining_expires = expires
                next_req = self._build_next_req(
                    needed,
                    expires=remaining_expires,
                    heartbeat=heartbeat,
                    min_pending=min_pending,
                    min_ack_pending=min_ack_pending,
                    priority=priority,
                )
                await self._nc.publish(
                    self._nms,
                    json.dumps(next_req).encode(),
                    self._deliver,
                )
                await asyncio.sleep(0)

            await send_lingering_request()

            # Get the immediate next message which could be a status message
            # or a processable message.
            msg = None
            resent_without_pin_id = False

            while True:
                # Check if already got enough at this point.
                if needed == 0:
                    return msgs

                deadline = JetStreamContext._time_until(timeout, start_time)
                if len(msgs) == 0:
                    # Not a single processable message has been received so far,
                    # if this timed out then let the error be raised.
                    try:
                        msg = await self._sub.next_msg(timeout=deadline)
                    except asyncio.TimeoutError:
                        if got_any_response:
                            raise FetchTimeoutError
                        raise
                else:
                    try:
                        msg = await self._sub.next_msg(timeout=deadline)
                    except asyncio.TimeoutError:
                        # Ignore any timeout since already got at least a message.
                        break

                if msg:
                    status = JetStreamContext.is_status_msg(msg)
                    if JetStreamContext._is_heartbeat(status):
                        got_any_response = True
                        continue
                    if JetStreamContext._is_pin_id_mismatch_error(status):
                        # The pin this request carried is no longer valid and
                        # the server has discarded the request. Drop the stale
                        # id and re-issue once without it so the fetch can
                        # recover within its own deadline instead of waiting
                        # on a request that will never be served.
                        self._pin_id = None
                        got_any_response = True
                        if not resent_without_pin_id:
                            resent_without_pin_id = True
                            await send_lingering_request()
                            continue

                    if not status:
                        pin_id = msg.headers.get(api.Header.PIN_ID) if msg.headers else None
                        if pin_id:
                            self._pin_id = pin_id
                        needed -= 1
                        msgs.append(msg)
                        break
                    elif not msgs and not JetStreamContext._is_temporary_error(status, msg):
                        raise nats.js.errors.APIError.from_msg(msg)
                    elif status == api.StatusCode.NO_MESSAGES or status:
                        # If there is still time, try again to get the next message
                        # or timeout.  This could be due to concurrent uses of fetch
                        # with the same inbox.
                        break
                    elif len(msgs) == 0:
                        raise nats.js.errors.APIError.from_msg(msg)

            # Wait for the rest of the messages to be delivered to the internal pending queue.
            try:
                for _ in range(needed):
                    deadline = JetStreamContext._time_until(timeout, start_time)
                    if deadline is not None and deadline < 0:
                        return msgs

                    msg = await self._sub.next_msg(timeout=deadline)
                    status = JetStreamContext.is_status_msg(msg)
                    if JetStreamContext._is_heartbeat(status):
                        got_any_response = True
                        continue
                    if JetStreamContext._is_pin_id_mismatch_error(status):
                        self._pin_id = None
                        got_any_response = True
                    if status in (
                        api.StatusCode.NO_MESSAGES,
                        api.StatusCode.REQUEST_TIMEOUT,
                    ):
                        # No more messages will be delivered on this pull
                        # request; return what we have.
                        break
                    if JetStreamContext._is_processable_msg(status, msg):
                        pin_id = msg.headers.get(api.Header.PIN_ID) if msg.headers else None
                        if pin_id:
                            self._pin_id = pin_id
                        needed -= 1
                        msgs.append(msg)
            except asyncio.TimeoutError:
                # Ignore any timeout errors at this point since
                # at least one message has already arrived.
                pass

            if len(msgs) == 0 and got_any_response:
                raise FetchTimeoutError

            return msgs

    ######################
    #                    #
    # KeyValue Context   #
    #                    #
    ######################

    async def key_value(self, bucket: str) -> KeyValue:
        if VALID_BUCKET_RE.match(bucket) is None:
            raise InvalidBucketNameError

        stream = KV_STREAM_TEMPLATE.format(bucket=bucket)
        try:
            si = await self.stream_info(stream)
        except NotFoundError:
            raise BucketNotFoundError
        if si.config.max_msgs_per_subject < 1:
            raise BadBucketError

        return self._map_stream_to_kv(si)

    async def create_key_value(
        self,
        config: Optional[api.KeyValueConfig] = None,
        **params,
    ) -> KeyValue:
        """
        create_key_value takes an api.KeyValueConfig and creates a KV in JetStream.

        Raises BucketExistsError when a bucket with that name already
        exists with a different configuration.
        """
        config = self._key_value_config(config, params)
        stream = await self._prepare_key_value_config(config)

        try:
            si = await self.add_stream(stream)
        except nats.js.errors.APIError as err:
            if err.err_code != KV_STREAM_NAME_IN_USE:
                raise
            exists = nats.js.errors.BucketExistsError(
                bucket=config.bucket,
                code=err.code,
                description=err.description,
                err_code=err.err_code,
                stream=err.stream,
                seq=err.seq,
            )
            # As nats.go, a bucket whose stream differs only in its discard
            # policy or direct gets (e.g. one created by an older client) is
            # updated. Re-adding the stream with the existing values of those
            # fields lets the server tell whether anything else differs.
            try:
                current = await self.stream_info(stream.name)
                probe = stream.evolve(
                    discard=current.config.discard,
                    allow_direct=current.config.allow_direct,
                )
                await self.add_stream(probe)
            except nats.js.errors.APIError:
                raise exists from err
            si = await self.update_stream(stream)

        return self._map_stream_to_kv(si)

    async def update_key_value(
        self,
        config: Optional[api.KeyValueConfig] = None,
        **params,
    ) -> KeyValue:
        """
        update_key_value updates the configuration of an existing KV,
        raising BucketNotFoundError when it does not exist.
        """
        config = self._key_value_config(config, params)
        stream = await self._prepare_key_value_config(config)

        try:
            si = await self.update_stream(stream)
        except NotFoundError as err:
            if err.err_code != KV_STREAM_NOT_FOUND:
                raise
            raise BucketNotFoundError(
                code=err.code,
                description=f"bucket not found: {config.bucket}",
                err_code=err.err_code,
            ) from err
        return self._map_stream_to_kv(si)

    async def create_or_update_key_value(
        self,
        config: Optional[api.KeyValueConfig] = None,
        **params,
    ) -> KeyValue:
        """
        create_or_update_key_value updates a KV, creating it when it does
        not exist yet.
        """
        config = self._key_value_config(config, params)
        stream = await self._prepare_key_value_config(config)

        try:
            si = await self.update_stream(stream)
        except NotFoundError as err:
            if err.err_code != KV_STREAM_NOT_FOUND:
                raise
            si = await self.add_stream(stream)
        return self._map_stream_to_kv(si)

    @staticmethod
    def _key_value_config(config: Optional[api.KeyValueConfig], params: Dict[str, Any]) -> api.KeyValueConfig:
        if config is None:
            if "bucket" not in params:
                raise nats.js.errors.KeyValueConfigRequiredError
            config = api.KeyValueConfig(bucket=params["bucket"])
        return config.evolve(**params)

    async def _prepare_key_value_config(self, config: api.KeyValueConfig) -> api.StreamConfig:
        """
        _prepare_key_value_config derives the stream configuration of a KV
        (nats.go prepareKeyValueConfig).
        """
        if VALID_BUCKET_RE.match(config.bucket) is None:
            raise InvalidBucketNameError

        duplicate_window: float = 2 * 60  # 2 minutes

        if config.ttl and config.ttl < duplicate_window:
            duplicate_window = config.ttl

        if config.history > 64:
            raise nats.js.errors.KeyHistoryTooLargeError

        subject_delete_marker_ttl = None
        if config.limit_marker_ttl is not None and config.limit_marker_ttl > 0:
            info = await self.account_info()
            if not info.api.level or info.api.level < 1:
                raise nats.js.errors.KeyValueLimitMarkerTTLNotSupportedError()
            subject_delete_marker_ttl = config.limit_marker_ttl

        stream = api.StreamConfig(
            name=KV_STREAM_TEMPLATE.format(bucket=config.bucket),
            description=config.description,
            subjects=[f"$KV.{config.bucket}.>"],
            allow_direct=config.direct,
            allow_rollup_hdrs=True,
            allow_msg_ttl=True,
            deny_delete=True,
            discard=api.DiscardPolicy.NEW,
            duplicate_window=duplicate_window,
            max_age=config.ttl,
            max_bytes=config.max_bytes,
            max_consumers=-1,
            max_msg_size=config.max_value_size,
            max_msgs=-1,
            max_msgs_per_subject=config.history,
            num_replicas=config.replicas,
            storage=config.storage,
            republish=config.republish,
            placement=config.placement,
            subject_delete_marker_ttl=subject_delete_marker_ttl,
        )
        if config.compression:
            stream.compression = api.StoreCompression.S2
        if config.metadata:
            stream.metadata = config.metadata

        if config.mirror is not None:
            # A mirror has no subjects of its own; it is read through direct
            # gets of the mirror (nats.go sets MirrorDirect).
            mirror = config.mirror.evolve()
            if not mirror.name.startswith(KV_STREAM_PREFIX):
                mirror.name = KV_STREAM_TEMPLATE.format(bucket=mirror.name)
            stream.mirror = mirror
            stream.mirror_direct = True
            stream.subjects = None
        elif config.sources:
            sources = []
            for source in config.sources:
                source = source.evolve()
                # A source with its own subject transforms is kept as given.
                if not source.subject_transforms:
                    if source.name.startswith(KV_STREAM_PREFIX):
                        source_bucket = source.name[len(KV_STREAM_PREFIX) :]
                    else:
                        source_bucket = source.name
                        source.name = KV_STREAM_TEMPLATE.format(bucket=source_bucket)
                    # Keys of another bucket are mapped into this bucket's
                    # subjects (not needed for the same bucket in another domain).
                    if source.external is None or source_bucket != config.bucket:
                        source.subject_transforms = [
                            api.SubjectTransform(
                                src=f"$KV.{source_bucket}.>",
                                dest=f"$KV.{config.bucket}.>",
                            )
                        ]
                sources.append(source)
            stream.sources = sources
        return stream

    def _map_stream_to_kv(self, si: api.StreamInfo) -> KeyValue:
        """
        _map_stream_to_kv returns the KeyValue handle of a KV stream
        (nats.go mapStreamToKVS).
        """
        stream = si.config.name
        assert stream is not None
        bucket = stream[len(KV_STREAM_PREFIX) :] if stream.startswith(KV_STREAM_PREFIX) else stream
        pre = KV_PRE_TEMPLATE.format(bucket=bucket)
        put_pre = None
        use_js_prefix = True

        # A mirror writes to the bucket it mirrors. When that bucket is in
        # another domain, keys are also read under its subjects and writes
        # go through that domain's API prefix.
        mirror = si.config.mirror
        if mirror is not None:
            name = mirror.name
            origin = name[len(KV_STREAM_PREFIX) :] if name.startswith(KV_STREAM_PREFIX) else name
            if mirror.external is not None and mirror.external.api:
                use_js_prefix = False
                pre = KV_PRE_TEMPLATE.format(bucket=origin)
                put_pre = f"{mirror.external.api}.{KV_PRE_TEMPLATE.format(bucket=origin)}"
            else:
                put_pre = KV_PRE_TEMPLATE.format(bucket=origin)

        return KeyValue(
            name=bucket,
            stream=stream,
            pre=pre,
            js=self,
            direct=bool(si.config.allow_direct),
            put_pre=put_pre,
            use_js_prefix=use_js_prefix,
        )

    async def key_value_store_names(self) -> AsyncIterator[str]:
        """
        key_value_store_names yields the names of the KeyValue stores
        (nats.go KeyValueStoreNames). Errors are raised while iterating.

        ::

            async for name in js.key_value_store_names():
                print(name)
        """
        offset = 0
        while True:
            resp = await self._api_request(
                f"{self._prefix}.STREAM.NAMES",
                json.dumps({"offset": offset, "subject": KV_ALL_SUBJECTS}).encode(),
                timeout=self._timeout,
            )
            names = resp.get("streams") or []
            for name in names:
                if name.startswith(KV_STREAM_PREFIX):
                    yield name[len(KV_STREAM_PREFIX) :]
            offset += len(names)
            if not names or offset >= resp.get("total", 0):
                return

    async def key_value_stores(self) -> AsyncIterator[KeyValue.BucketStatus]:
        """
        key_value_stores yields the status of each KeyValue store
        (nats.go KeyValueStores). Errors are raised while iterating.

        ::

            async for status in js.key_value_stores():
                print(status.bucket, status.values)
        """
        offset = 0
        while True:
            resp = await self._api_request(
                f"{self._prefix}.STREAM.LIST",
                json.dumps({"offset": offset, "subject": KV_ALL_SUBJECTS}).encode(),
                timeout=self._timeout,
            )
            infos = resp.get("streams") or []
            for info in infos:
                si = api.StreamInfo.from_response(info)
                name = si.config.name or ""
                if name.startswith(KV_STREAM_PREFIX):
                    yield KeyValue.BucketStatus(stream_info=si, bucket=name[len(KV_STREAM_PREFIX) :])
            offset += len(infos)
            if not infos or offset >= resp.get("total", 0):
                return

    async def delete_key_value(self, bucket: str) -> bool:
        """
        delete_key_value deletes a JetStream KeyValue store by destroying
        the associated stream.
        """
        if VALID_BUCKET_RE.match(bucket) is None:
            raise InvalidBucketNameError

        stream = KV_STREAM_TEMPLATE.format(bucket=bucket)
        return await self.delete_stream(stream)

    #######################
    #                     #
    # ObjectStore Context #
    #                     #
    #######################

    async def object_store(self, bucket: str) -> ObjectStore:
        if VALID_BUCKET_RE.match(bucket) is None:
            raise nats.js.errors.InvalidStoreNameError

        stream = OBJ_STREAM_TEMPLATE.format(bucket=bucket)
        try:
            await self.stream_info(stream)
        except NotFoundError:
            raise BucketNotFoundError

        return ObjectStore(
            name=bucket,
            stream=stream,
            js=self,
        )

    async def create_object_store(
        self,
        bucket: str = None,
        config: Optional[api.ObjectStoreConfig] = None,
        **params,
    ) -> ObjectStore:
        """
        create_object_store takes an api.ObjectStoreConfig and creates a OBJ in JetStream.
        """
        config = self._object_store_config(bucket, config, params)
        stream = self._object_store_stream_config(config)
        try:
            await self.add_stream(stream)
        except nats.js.errors.APIError as err:
            # As nats.go, an existing bucket with another config is ErrBucketExists.
            if err.err_code != KV_STREAM_NAME_IN_USE:
                raise
            raise nats.js.errors.BucketExistsError(
                bucket=config.bucket,
                code=err.code,
                description=err.description,
                err_code=err.err_code,
                stream=err.stream,
                seq=err.seq,
            ) from err

        assert stream.name is not None
        return ObjectStore(
            name=config.bucket,
            stream=stream.name,
            js=self,
        )

    async def update_object_store(
        self,
        bucket: str = None,
        config: Optional[api.ObjectStoreConfig] = None,
        **params,
    ) -> ObjectStore:
        """
        update_object_store takes an api.ObjectStoreConfig and updates the
        stream of an existing OBJ in JetStream.

        Raises BucketNotFoundError when the bucket does not exist.
        """
        config = self._object_store_config(bucket, config, params)
        stream = self._object_store_stream_config(config)
        try:
            await self.update_stream(stream)
        except NotFoundError as e:
            raise BucketNotFoundError(code=e.code, err_code=e.err_code, description=e.description) from e

        assert stream.name is not None
        return ObjectStore(
            name=config.bucket,
            stream=stream.name,
            js=self,
        )

    async def create_or_update_object_store(
        self,
        bucket: str = None,
        config: Optional[api.ObjectStoreConfig] = None,
        **params,
    ) -> ObjectStore:
        """
        create_or_update_object_store takes an api.ObjectStoreConfig and
        updates the OBJ in JetStream, creating it when it does not exist.
        """
        config = self._object_store_config(bucket, config, params)
        stream = self._object_store_stream_config(config)
        try:
            await self.update_stream(stream)
        except NotFoundError:
            await self.add_stream(stream)

        assert stream.name is not None
        return ObjectStore(
            name=config.bucket,
            stream=stream.name,
            js=self,
        )

    @staticmethod
    def _object_store_config(
        bucket: Optional[str],
        config: Optional[api.ObjectStoreConfig],
        params: Dict[str, Any],
    ) -> api.ObjectStoreConfig:
        if config is None:
            if bucket is None and not params:
                raise nats.js.errors.ObjectConfigRequiredError
            config = api.ObjectStoreConfig(bucket=bucket)
        elif bucket is not None:
            config.bucket = bucket
        config = config.evolve(**params)

        if config.bucket is None or VALID_BUCKET_RE.match(config.bucket) is None:
            raise nats.js.errors.InvalidStoreNameError
        return config

    @staticmethod
    def _object_store_stream_config(config: api.ObjectStoreConfig) -> api.StreamConfig:
        name = config.bucket
        chunks = OBJ_ALL_CHUNKS_PRE_TEMPLATE.format(bucket=name)
        meta = OBJ_ALL_META_PRE_TEMPLATE.format(bucket=name)

        max_bytes = config.max_bytes
        if max_bytes == 0:
            max_bytes = -1

        return api.StreamConfig(
            name=OBJ_STREAM_TEMPLATE.format(bucket=config.bucket),
            description=config.description,
            subjects=[chunks, meta],
            max_age=config.ttl,
            max_bytes=max_bytes,
            max_consumers=0,
            storage=config.storage,
            num_replicas=config.replicas,
            placement=config.placement,
            discard=api.DiscardPolicy.NEW,
            allow_rollup_hdrs=True,
            allow_direct=True,
            compression=api.StoreCompression.S2 if config.compression else None,
            metadata=config.metadata,
        )

    async def object_store_names(self) -> AsyncIterator[str]:
        """
        object_store_names yields the bucket names of the object stores in JetStream.

        ::

            async for name in js.object_store_names():
                print(name)
        """
        async for name in self._object_store_streams("NAMES"):
            if name.startswith("OBJ_"):
                yield name[len("OBJ_") :]

    async def object_stores(self) -> AsyncIterator[ObjectStore.ObjectStoreStatus]:
        """
        object_stores yields the status of each object store in JetStream.

        ::

            async for status in js.object_stores():
                print(status.bucket, status.size)
        """
        async for resp in self._object_store_streams("LIST"):
            info = api.StreamInfo.from_response(resp)
            name = info.config.name
            if name is not None and name.startswith("OBJ_"):
                yield ObjectStore.ObjectStoreStatus(stream_info=info, bucket=name[len("OBJ_") :])

    async def _object_store_streams(self, kind: str) -> AsyncIterator[Any]:
        # Pages through the streams listed (STREAM.NAMES or STREAM.LIST)
        # under the chunk subjects of every object store, as nats.go does.
        offset = 0
        while True:
            resp = await self._api_request(
                f"{self._prefix}.STREAM.{kind}",
                json.dumps({"offset": offset, "subject": "$O.*.C.>"}).encode(),
                timeout=self._timeout,
            )
            streams = resp.get("streams") or []
            for stream in streams:
                yield stream
            offset += len(streams)
            if not streams or offset >= resp.get("total", 0):
                return

    async def delete_object_store(self, bucket: str) -> bool:
        """
        delete_object_store will delete the underlying stream for the named object.
        """
        if VALID_BUCKET_RE.match(bucket) is None:
            raise nats.js.errors.InvalidStoreNameError

        stream = OBJ_STREAM_TEMPLATE.format(bucket=bucket)
        return await self.delete_stream(stream)
