# Copyright 2016-2026 The NATS Authors
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
"""
Consumer handles modelled on nats.go's jetstream package: a pull consumer
that fetches batches of messages or pulls continuously
(:class:`PullConsumer`, :class:`ConsumeContext`, :class:`MessagesContext`),
and an ordered consumer built on it (:class:`OrderedConsumer`).
"""

from __future__ import annotations

import asyncio
import datetime
import inspect
import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import nats.errors
import nats.js.errors
from nats.aio.msg import Msg
from nats.js import api

if TYPE_CHECKING:
    from nats.aio.subscription import Subscription
    from nats.js.client import JetStreamContext

# Expiry of a pull request when none is given (nats.go DefaultExpires).
DEFAULT_EXPIRES = api.DEFAULT_EXPIRES

# Messages asked for by a pull limited by bytes only.
_BYTES_BATCH = 1_000_000

# How much longer than a pull request's expiry its client waits.
_EXPIRES_GRACE = 1.0

# Messages asked for by each pull of consume() and messages() (nats.go
# DefaultMaxMessages).
DEFAULT_MAX_MESSAGES = api.DEFAULT_MAX_MESSAGES

# Headers of a pull status message that tell what the pull left undelivered.
_PENDING_MESSAGES_HEADER = "Nats-Pending-Messages"
_PENDING_BYTES_HEADER = "Nats-Pending-Bytes"

_NANOSECOND = 1_000_000_000


def _status(msg: Msg) -> Optional[str]:
    """The status of a pulled message, or None for a message from the stream."""
    if msg.data or not msg.headers:
        return None
    return msg.headers.get(api.Header.STATUS) or None


def _msg_size(msg: Msg) -> int:
    """The size of a message as nats.go's Msg.Size counts it."""
    size = len(msg.subject) + len(msg.reply) + len(msg.data)
    if msg.headers:
        size += len(b"NATS/1.0\r\n\r\n")
        for k, v in msg.headers.items():
            size += len(k) + len(v) + 4
    return size


def _status_error(msg: Msg, status: str) -> Exception:
    """The error of a pull status message (nats.go checkMsg)."""
    desc = msg.headers.get(api.Header.DESCRIPTION, "") if msg.headers else ""
    if status == api.StatusCode.SERVICE_UNAVAILABLE:
        return nats.errors.NoRespondersError()
    if status == "400":
        return nats.js.errors.BadRequestError(code=400, description=desc)
    if status == api.StatusCode.REQUEST_TIMEOUT:
        return nats.errors.TimeoutError()
    if status == api.StatusCode.NO_MESSAGES:
        return nats.js.errors.NoMessagesError()
    cls = nats.js.errors.APIError._status_error_class(status, desc)
    return cls(code=int(status), description=desc)


async def _call(handler: Callable, *args: Any) -> None:
    ret = handler(*args)
    if inspect.isawaitable(ret):
        await ret


def _check_positive(name: str, value: Optional[float]) -> None:
    if value is not None and value <= 0:
        raise ValueError(f"nats: invalid option: {name} must be greater than 0")


def _check_at_least_one(name: str, value: Optional[int]) -> None:
    if value is not None and value < 1:
        raise ValueError(f"nats: invalid option: {name} must be at least 1")


class _PullRequest:
    """A pull request sent to $JS.API.CONSUMER.MSG.NEXT (nats.go pullRequest)."""

    def __init__(
        self,
        batch: int,
        expires: float = 0,
        max_bytes: int = 0,
        no_wait: bool = False,
        heartbeat: float = 0,
        min_pending: Optional[int] = None,
        min_ack_pending: Optional[int] = None,
        priority: Optional[int] = None,
        group: Optional[str] = None,
    ) -> None:
        self.batch = batch
        self.expires = expires
        self.max_bytes = max_bytes
        self.no_wait = no_wait
        self.heartbeat = heartbeat
        self.min_pending = min_pending
        self.min_ack_pending = min_ack_pending
        self.priority = priority
        self.group = group

    def encode(self, pin_id: Optional[str]) -> bytes:
        req: Dict[str, Any] = {}
        if self.expires:
            req["expires"] = int(self.expires * _NANOSECOND)
        req["batch"] = self.batch
        if self.max_bytes:
            req["max_bytes"] = self.max_bytes
        if self.no_wait:
            req["no_wait"] = True
        if self.heartbeat:
            req["idle_heartbeat"] = int(self.heartbeat * _NANOSECOND)
        if self.min_pending:
            req["min_pending"] = self.min_pending
        if self.min_ack_pending:
            req["min_ack_pending"] = self.min_ack_pending
        if pin_id:
            req["id"] = pin_id
        if self.priority:
            req["priority"] = self.priority
        if self.group:
            req["group"] = self.group
        return json.dumps(req).encode()


class MessageBatch:
    """
    MessageBatch holds the messages of one pull request made by
    :meth:`PullConsumer.fetch` and friends (nats.go MessageBatch).

    Messages are delivered as they arrive: iterate over the batch with
    ``async for`` (or :meth:`messages`). Once the iteration ends,
    :attr:`error` holds the error that ended the pull early, if any; a pull
    that expired or ran out of messages has none.

    ::

        batch = await consumer.fetch(10, max_wait=5)
        async for msg in batch:
            await msg.ack()
        if batch.error:
            print("fetch failed:", batch.error)
    """

    def __init__(self, consumer: PullConsumer, sub: Subscription, request: _PullRequest) -> None:
        self._consumer = consumer
        self._sub = sub
        self._request = request
        self._queue: asyncio.Queue = asyncio.Queue()
        self._err: Optional[Exception] = None
        self._done = False
        self._received = 0
        self._received_bytes = 0
        # Stream sequence of the last message received.
        self._sseq = 0
        self._task = asyncio.ensure_future(self._run())

    @property
    def error(self) -> Optional[Exception]:
        """The error that ended the pull, once the batch is done."""
        return self._err

    @property
    def done(self) -> bool:
        """Whether the pull ended; messages may still be left to iterate."""
        return self._done

    def __aiter__(self) -> AsyncIterator[Msg]:
        return self.messages()

    async def messages(self) -> AsyncIterator[Msg]:
        """Iterate over the messages of the batch as they arrive."""
        while True:
            msg = await self._queue.get()
            if msg is None:
                # Leave the end mark for any other iteration.
                self._queue.put_nowait(None)
                return
            yield msg

    async def _run(self) -> None:
        try:
            await self._read()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._err = e
        finally:
            self._done = True
            self._queue.put_nowait(None)
            try:
                await self._sub.unsubscribe()
            except Exception:
                pass

    async def _read(self) -> None:
        req = self._request
        hb = req.heartbeat
        deadline = time.monotonic() + req.expires + _EXPIRES_GRACE
        hb_deadline = time.monotonic() + 2 * hb if hb else None
        while True:
            now = time.monotonic()
            wait = deadline - now
            if hb_deadline is not None and hb_deadline - now < wait:
                wait = hb_deadline - now
                if wait <= 0:
                    raise nats.js.errors.NoHeartbeatError
            if wait <= 0:
                return
            try:
                msg = await self._sub.next_msg(timeout=wait)
            except nats.errors.TimeoutError:
                continue
            if hb_deadline is not None:
                hb_deadline = time.monotonic() + 2 * hb

            status = _status(msg)
            if status is None:
                self._consumer._update_pin(msg)
                self._queue.put_nowait(msg)
                self._received += 1
                try:
                    self._sseq = msg.metadata.sequence.stream
                except nats.errors.Error:
                    pass
                if req.max_bytes:
                    self._received_bytes += _msg_size(msg)
                if self._received == req.batch or (req.max_bytes and self._received_bytes >= req.max_bytes):
                    return
                continue
            if status == api.StatusCode.CONTROL_MESSAGE:
                continue
            if status == api.StatusCode.PIN_ID_MISMATCH:
                self._consumer._pin_id = None
            err = _status_error(msg, status)
            if isinstance(err, (nats.errors.TimeoutError, nats.js.errors.MaxBytesExceededError)):
                return
            if status == api.StatusCode.NO_MESSAGES:
                return
            raise err


class PullConsumer:
    """
    PullConsumer is a handle on a pull consumer (nats.go jetstream.Consumer),
    obtained from :meth:`JetStreamContext.pull_consumer`.

    ::

        consumer = await js.pull_consumer("mystream", "dur")
        batch = await consumer.fetch(10)
        async for msg in batch:
            await msg.ack()
    """

    def __init__(
        self,
        js: JetStreamContext,
        stream: str,
        name: str,
        info: Optional[api.ConsumerInfo] = None,
    ) -> None:
        self._js = js
        self._nc = js._nc
        self._stream = stream
        self._name = name
        self._info = info
        self._pin_id: Optional[str] = None

    @property
    def stream(self) -> str:
        return self._stream

    @property
    def name(self) -> str:
        return self._name

    @property
    def pin_id(self) -> Optional[str]:
        """Pin ID the server gave to this consumer handle in a pinned priority group."""
        return self._pin_id

    async def info(self) -> api.ConsumerInfo:
        """Fetch the consumer info from the server, updating :meth:`cached_info`."""
        self._info = await self._js.consumer_info(self._stream, self._name)
        return self._info

    def cached_info(self) -> Optional[api.ConsumerInfo]:
        """The consumer info last fetched, without asking the server."""
        return self._info

    @property
    def _next_subject(self) -> str:
        return f"{self._js._prefix}.CONSUMER.MSG.NEXT.{self._stream}.{self._name}"

    def _update_pin(self, msg: Msg) -> None:
        pin_id = msg.headers.get(api.Header.PIN_ID) if msg.headers else None
        if pin_id:
            self._pin_id = pin_id

    async def _send_pull(self, request: _PullRequest, reply: str) -> None:
        await self._nc.publish(self._next_subject, request.encode(self._pin_id), reply=reply)

    async def fetch(
        self,
        batch: int,
        max_wait: Optional[float] = None,
        heartbeat: Optional[float] = None,
        min_pending: Optional[int] = None,
        min_ack_pending: Optional[int] = None,
        priority: Optional[int] = None,
        group: Optional[str] = None,
    ) -> MessageBatch:
        """
        fetch makes a single pull request for up to ``batch`` messages and
        returns the :class:`MessageBatch` they are delivered to. The pull
        ends once ``batch`` messages arrived or ``max_wait`` seconds passed
        (30 by default).

        :param batch: Maximum number of messages.
        :param max_wait: Seconds the server keeps the request open.
        :param heartbeat: Idle heartbeat interval in seconds; when two are
            missed the batch ends with NoHeartbeatError. Defaults to 5 for a
            ``max_wait`` of 10 seconds or more.
        :param min_pending: Only deliver when the consumer has at least this
            many pending messages (``PriorityPolicy.OVERFLOW``).
        :param min_ack_pending: Only deliver when the consumer has at least
            this many unacknowledged messages (``PriorityPolicy.OVERFLOW``).
        :param priority: Priority from 0 (highest) to 9 (``PriorityPolicy.PRIORITIZED``).
        :param group: Priority group of the request.
        """
        _check_at_least_one("batch", batch)
        return await self._fetch(
            self._fetch_request(batch, 0, max_wait, heartbeat, min_pending, min_ack_pending, priority, group)
        )

    async def fetch_bytes(
        self,
        max_bytes: int,
        max_wait: Optional[float] = None,
        heartbeat: Optional[float] = None,
        min_pending: Optional[int] = None,
        min_ack_pending: Optional[int] = None,
        priority: Optional[int] = None,
        group: Optional[str] = None,
    ) -> MessageBatch:
        """
        fetch_bytes is like :meth:`fetch` but asks for messages up to
        ``max_bytes`` in total instead of a number of messages. A message
        larger than what is left of ``max_bytes`` ends the batch.
        """
        _check_at_least_one("max_bytes", max_bytes)
        return await self._fetch(
            self._fetch_request(
                _BYTES_BATCH, max_bytes, max_wait, heartbeat, min_pending, min_ack_pending, priority, group
            )
        )

    async def fetch_no_wait(self, batch: int) -> MessageBatch:
        """
        fetch_no_wait asks for up to ``batch`` messages that are available
        now, the batch ends as soon as the server has no more.
        """
        _check_at_least_one("batch", batch)
        return await self._fetch(_PullRequest(batch, no_wait=True))

    async def next(self, max_wait: Optional[float] = None, heartbeat: Optional[float] = None) -> Msg:
        """
        next fetches a single message, raising nats.errors.TimeoutError when
        none arrived within ``max_wait`` seconds, or the error that ended the
        pull.
        """
        batch = await self.fetch(1, max_wait=max_wait, heartbeat=heartbeat)
        async for msg in batch:
            return msg
        if batch.error is not None:
            raise batch.error
        raise nats.errors.TimeoutError

    async def consume(
        self,
        cb: Callable[[Msg], Awaitable[None]],
        *,
        max_messages: Optional[int] = None,
        max_bytes: Optional[int] = None,
        bytes_limit: Optional[int] = None,
        expires: Optional[float] = None,
        threshold_messages: Optional[int] = None,
        threshold_bytes: Optional[int] = None,
        min_pending: Optional[int] = None,
        min_ack_pending: Optional[int] = None,
        priority: Optional[int] = None,
        group: Optional[str] = None,
        heartbeat: Optional[float] = None,
        stop_after: Optional[int] = None,
        error_cb: Optional[Callable[[ConsumeContext, Exception], Any]] = None,
    ) -> ConsumeContext:
        """
        consume calls ``cb`` with each message of the consumer, pulling more
        messages as the buffered ones are handled (nats.go Consumer.Consume),
        until the returned :class:`ConsumeContext` is stopped or drained.

        :param cb: Coroutine function called with each message, one at a time.
        :param max_messages: Messages buffered by the pulls (default 500).
        :param max_bytes: Bytes buffered by the pulls instead of a number of
            messages; cannot be used with ``max_messages``.
        :param bytes_limit: Also cap each pull of ``max_messages`` messages
            at this many bytes (nats.go PullMaxMessagesWithBytesLimit).
        :param expires: Seconds each pull request lasts, at least 1 (default 30).
        :param threshold_messages: Pull again once fewer messages are pending
            (default half of ``max_messages``).
        :param threshold_bytes: Pull again once fewer bytes are pending
            (default half of ``max_bytes``).
        :param min_pending: Pull with min_pending (``PriorityPolicy.OVERFLOW``).
        :param min_ack_pending: Pull with min_ack_pending (``PriorityPolicy.OVERFLOW``).
        :param priority: Pull priority 0-9 (``PriorityPolicy.PRIORITIZED``).
        :param group: Priority group, required by consumers with priority groups.
        :param heartbeat: Idle heartbeat in seconds, 0.5 to 30 and at most
            half of ``expires`` (default half of ``expires``, at most 30).
            When two are missed the pull is made again.
        :param stop_after: Stop after this many messages were handled.
        :param error_cb: Called as ``error_cb(ctx, err)`` with the errors met
            while consuming (missed heartbeats, status errors); may be a
            coroutine function.
        """
        if cb is None:
            raise nats.js.errors.HandlerRequiredError
        opts = _PullOptions(
            False,
            max_messages,
            max_bytes,
            bytes_limit,
            expires,
            threshold_messages,
            threshold_bytes,
            min_pending,
            min_ack_pending,
            priority,
            group,
            heartbeat,
            stop_after,
        )
        self._check_group(group)
        ctx = ConsumeContext(self, opts, cb, error_cb)
        await ctx._start()
        return ctx

    async def messages(
        self,
        *,
        max_messages: Optional[int] = None,
        max_bytes: Optional[int] = None,
        bytes_limit: Optional[int] = None,
        expires: Optional[float] = None,
        threshold_messages: Optional[int] = None,
        threshold_bytes: Optional[int] = None,
        min_pending: Optional[int] = None,
        min_ack_pending: Optional[int] = None,
        priority: Optional[int] = None,
        group: Optional[str] = None,
        heartbeat: Optional[float] = None,
        stop_after: Optional[int] = None,
        err_on_missing_heartbeat: bool = True,
    ) -> MessagesContext:
        """
        messages returns a :class:`MessagesContext` to iterate over the
        messages of the consumer, pulling more as they are taken (nats.go
        Consumer.Messages). The options are those of :meth:`consume`.

        :param err_on_missing_heartbeat: Raise NoHeartbeatError from ``next``
            when two heartbeats were missed; otherwise pull again silently.

        ::

            msgs = await consumer.messages()
            async for msg in msgs:
                await msg.ack()
        """
        opts = _PullOptions(
            False,
            max_messages,
            max_bytes,
            bytes_limit,
            expires,
            threshold_messages,
            threshold_bytes,
            min_pending,
            min_ack_pending,
            priority,
            group,
            heartbeat,
            stop_after,
        )
        self._check_group(group)
        ctx = MessagesContext(self, opts, err_on_missing_heartbeat)
        await ctx._start()
        return ctx

    def _check_group(self, group: Optional[str]) -> None:
        groups = self._info.config.priority_groups if self._info else None
        if groups:
            if not group:
                raise ValueError("nats: invalid option: priority group is required for priority consumer")
            if group not in groups:
                raise ValueError("nats: invalid option: invalid priority group")
        elif group:
            raise ValueError("nats: invalid option: priority groups not supported by consumer")

    @staticmethod
    def _fetch_request(
        batch: int,
        max_bytes: int,
        max_wait: Optional[float],
        heartbeat: Optional[float],
        min_pending: Optional[int],
        min_ack_pending: Optional[int],
        priority: Optional[int],
        group: Optional[str],
    ) -> _PullRequest:
        _check_positive("max_wait", max_wait)
        _check_positive("heartbeat", heartbeat)
        _check_at_least_one("min_pending", min_pending)
        _check_at_least_one("min_ack_pending", min_ack_pending)
        if priority is not None and not 0 <= priority <= 9:
            raise ValueError("nats: invalid option: priority must be 0-9")
        expires = DEFAULT_EXPIRES if max_wait is None else max_wait
        if heartbeat is None:
            heartbeat = 5.0 if expires >= 10 else 0
        if 2 * heartbeat > expires:
            raise ValueError("nats: invalid option: the value of heartbeat must be less than 50% of expiry")
        return _PullRequest(
            batch,
            expires=expires,
            max_bytes=max_bytes,
            heartbeat=heartbeat,
            min_pending=min_pending,
            min_ack_pending=min_ack_pending,
            priority=priority,
            group=group,
        )

    async def _fetch(self, request: _PullRequest) -> MessageBatch:
        sub = await self._nc.subscribe(self._nc.new_inbox())
        try:
            await self._send_pull(request, sub.subject)
        except BaseException:
            await sub.unsubscribe()
            raise
        return MessageBatch(self, sub, request)


class _PullOptions:
    """The options of consume() and messages() (nats.go consumeOpts) with their defaults."""

    def __init__(
        self,
        ordered: bool,
        max_messages: Optional[int],
        max_bytes: Optional[int],
        bytes_limit: Optional[int],
        expires: Optional[float],
        threshold_messages: Optional[int],
        threshold_bytes: Optional[int],
        min_pending: Optional[int],
        min_ack_pending: Optional[int],
        priority: Optional[int],
        group: Optional[str],
        heartbeat: Optional[float],
        stop_after: Optional[int],
    ) -> None:
        _check_at_least_one("max_messages", max_messages)
        _check_at_least_one("bytes_limit", bytes_limit)
        _check_positive("max_bytes", max_bytes)
        if expires is not None and expires < 1:
            raise ValueError("nats: invalid option: expires value must be at least 1s")
        _check_at_least_one("min_pending", min_pending)
        _check_at_least_one("min_ack_pending", min_ack_pending)
        if priority is not None and not 0 <= priority <= 9:
            raise ValueError("nats: invalid option: priority must be 0-9")
        if heartbeat is not None and not 0.5 <= heartbeat <= 30:
            raise ValueError("nats: invalid option: idle_heartbeat value must be within 500ms-30s range")
        _check_at_least_one("stop_after", stop_after)

        self.limit_size = bytes_limit is not None
        if self.limit_size and max_bytes is not None:
            raise ValueError("nats: invalid option: only one of bytes_limit and max_bytes can be specified")
        if max_messages is not None and max_bytes is not None:
            raise ValueError("nats: invalid option: only one of MaxMessages and MaxBytes can be specified")
        if max_bytes is not None:
            # Pull by bytes, with no limit on the number of messages.
            self.max_messages = _BYTES_BATCH
            self.max_bytes = int(max_bytes)
        else:
            self.max_messages = max_messages or DEFAULT_MAX_MESSAGES
            self.max_bytes = bytes_limit or 0
        self.threshold_messages = threshold_messages or -(-self.max_messages // 2)
        self.threshold_bytes = threshold_bytes or -(-self.max_bytes // 2)

        self.expires = DEFAULT_EXPIRES if expires is None else expires
        if heartbeat is None:
            if ordered:
                heartbeat = self.expires / 2 if self.expires < 10 else 5.0
            else:
                heartbeat = min(self.expires / 2, 30.0)
        if heartbeat > self.expires / 2:
            raise ValueError("nats: invalid option: the value of heartbeat must be less than 50% of expiry")
        self.heartbeat = heartbeat
        self.min_pending = min_pending
        self.min_ack_pending = min_ack_pending
        self.priority = priority
        self.group = group
        self.stop_after = stop_after or 0

    @property
    def tracks_bytes(self) -> bool:
        """Whether the pending bytes decide when to pull."""
        return bool(self.max_bytes) and not self.limit_size


class _PullSubscription:
    """
    State shared by consume() and messages() (nats.go pullSubscription):
    the messages and bytes still pending from the pulls made, the messages
    delivered, and the decision of when to pull again.
    """

    def __init__(self, consumer: PullConsumer, opts: _PullOptions) -> None:
        self._consumer = consumer
        self._nc = consumer._nc
        self._opts = opts
        self._sub: Optional[Subscription] = None
        self._lock = asyncio.Lock()
        self._msg_count = 0
        self._byte_count = 0
        self._delivered = 0
        self._closed = False
        self._draining = False

    def _request(self, batch: int, max_bytes: int) -> _PullRequest:
        o = self._opts
        return _PullRequest(
            batch,
            expires=o.expires,
            max_bytes=max_bytes,
            heartbeat=o.heartbeat,
            min_pending=o.min_pending,
            min_ack_pending=o.min_ack_pending,
            priority=o.priority,
            group=o.group,
        )

    def _restart(self, reconnect: bool = False) -> _PullRequest:
        """A pull made afresh: at first and after missed heartbeats."""
        o = self._opts
        batch = o.max_messages
        if o.stop_after and not reconnect:
            batch = min(batch, o.stop_after - self._delivered)
        self._msg_count = o.max_messages
        self._byte_count = o.max_bytes
        return self._request(batch, o.max_bytes)

    def _check_pending(self) -> Optional[_PullRequest]:
        """The pull to make once too few messages or bytes are pending (nats.go checkPending)."""
        o = self._opts
        low = self._msg_count < o.threshold_messages or (o.tracks_bytes and self._byte_count < o.threshold_bytes)
        if not low or self._closed:
            return None
        batch = o.max_messages if o.tracks_bytes else o.max_messages - self._msg_count
        max_bytes = 0
        if o.max_bytes:
            max_bytes = o.max_bytes if o.limit_size else o.max_bytes - self._byte_count
        if o.stop_after:
            batch = min(batch, o.stop_after - self._delivered - self._msg_count)
        if batch <= 0:
            return None
        self._msg_count = o.max_messages
        self._byte_count = o.max_bytes
        return self._request(batch, max_bytes)

    def _delivered_msg(self, msg: Msg) -> None:
        self._msg_count -= 1
        if self._opts.tracks_bytes:
            self._byte_count -= _msg_size(msg)
        self._delivered += 1

    def _handle_status(self, msg: Msg, status: str) -> Tuple[Optional[Exception], Optional[Exception]]:
        """
        What a status message does (nats.go handleStatusMsg): the error that
        ends consuming, and the one only reported.
        """
        err = _status_error(msg, status)
        if isinstance(
            err, (nats.errors.TimeoutError, nats.js.errors.MaxBytesExceededError, nats.js.errors.BatchCompletedError)
        ):
            # The pull ended: what it left undelivered is no longer pending.
            hdrs = msg.headers or {}
            try:
                pending_msgs = int(hdrs.get(_PENDING_MESSAGES_HEADER) or 0)
                pending_bytes = int(hdrs.get(_PENDING_BYTES_HEADER) or 0)
            except ValueError as e:
                return nats.js.errors.Error(f"invalid pending headers: {e}"), None
            self._msg_count = max(self._msg_count - pending_msgs, 0)
            if self._opts.tracks_bytes:
                self._byte_count = max(self._byte_count - pending_bytes, 0)
            return None, None
        if isinstance(err, (nats.js.errors.ConsumerDeletedError, nats.js.errors.BadRequestError)):
            return err, None
        if isinstance(err, nats.js.errors.PinIdMismatchError):
            self._consumer._pin_id = None
        if isinstance(err, (nats.js.errors.PinIdMismatchError, nats.js.errors.ConsumerLeadershipChangedError)):
            self._msg_count = 0
            self._byte_count = 0
        return None, err

    async def _pull(self, request: Optional[_PullRequest]) -> Optional[Exception]:
        if request is None or self._sub is None:
            return None
        try:
            await self._consumer._send_pull(request, self._sub.subject)
        except Exception as e:
            return e
        return None

    async def _subscribe(self, cb: Callable[[Msg], Awaitable[None]]) -> None:
        self._sub = await self._nc.subscribe(self._nc.new_inbox(), cb=cb)

    async def _end_subscription(self, drain: bool) -> None:
        sub = self._sub
        if sub is None:
            return
        try:
            if drain:
                await sub.drain()
            else:
                await sub.unsubscribe()
        except Exception:
            pass


class ConsumeContext(_PullSubscription):
    """
    ConsumeContext controls a running :meth:`PullConsumer.consume` (nats.go
    ConsumeContext).
    """

    def __init__(
        self,
        consumer: PullConsumer,
        opts: _PullOptions,
        cb: Callable[[Msg], Awaitable[None]],
        error_cb: Optional[Callable[[ConsumeContext, Exception], Any]],
    ) -> None:
        super().__init__(consumer, opts)
        self._cb = cb
        self._error_cb = error_cb
        self._closed_event = asyncio.Event()
        self._last_activity = time.monotonic()
        self._in_handler = False
        self._hb_task: Optional[asyncio.Future] = None
        self._end_task: Optional[asyncio.Future] = None

    async def _start(self) -> None:
        await self._subscribe(self._handle)
        async with self._lock:
            err = await self._pull(self._restart())
        if err is not None:
            await self._report(err)
        self._last_activity = time.monotonic()
        if self._opts.heartbeat:
            self._hb_task = asyncio.ensure_future(self._monitor_heartbeats())

    @property
    def is_closed(self) -> bool:
        """Whether consuming has fully stopped."""
        return self._closed_event.is_set()

    async def closed(self) -> None:
        """Wait until consuming has fully stopped."""
        await self._closed_event.wait()

    def stop(self) -> None:
        """Stop consuming at once; messages buffered but not handled are discarded."""
        self._end(False)

    def drain(self) -> None:
        """Stop pulling, and stop consuming once the buffered messages are handled."""
        self._end(True)

    def _end(self, drain: bool) -> None:
        if self._closed:
            return
        self._closed = True
        self._draining = drain
        self._end_task = asyncio.ensure_future(self._finish(drain))

    async def _finish(self, drain: bool) -> None:
        if self._hb_task is not None:
            self._hb_task.cancel()
        await self._end_subscription(drain)
        self._closed_event.set()

    async def _report(self, err: Exception) -> None:
        if self._error_cb is None:
            return
        try:
            await _call(self._error_cb, self, err)
        except Exception as e:
            await self._nc._error_cb(e)

    async def _handle(self, msg: Msg) -> None:
        self._in_handler = True
        try:
            await self._process(msg)
        finally:
            self._in_handler = False
            self._last_activity = time.monotonic()

    async def _process(self, msg: Msg) -> None:
        status = _status(msg)
        if status is None:
            self._consumer._update_pin(msg)
            try:
                await self._cb(msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self._nc._error_cb(e)
            async with self._lock:
                self._delivered_msg(msg)
                err = await self._pull(self._check_pending())
            if err is not None:
                await self._report(err)
            if self._opts.stop_after and self._delivered == self._opts.stop_after:
                self.stop()
            return
        if status == api.StatusCode.CONTROL_MESSAGE:
            return
        async with self._lock:
            term, notify = self._handle_status(msg, status)
            err = None
            if term is None:
                err = await self._pull(self._check_pending())
        for e in (notify, err, term):
            if e is not None:
                await self._report(e)
        if term is not None:
            self.stop()

    async def _monitor_heartbeats(self) -> None:
        # Two heartbeats missed: report it and pull afresh, which also
        # recovers the pulls lost by a reconnect.
        hb = self._opts.heartbeat
        while not self._closed:
            if self._nc.is_closed:
                await self._report(nats.errors.ConnectionClosedError())
                self.stop()
                return
            wait = self._last_activity + 2 * hb - time.monotonic()
            if self._in_handler or wait > 0:
                await asyncio.sleep(wait if wait > 0 else hb)
                continue
            self._last_activity = time.monotonic()
            async with self._lock:
                err = await self._pull(self._restart())
            await self._report(nats.js.errors.NoHeartbeatError())
            if err is not None:
                await self._report(err)


_CLOSED = object()


class MessagesContext(_PullSubscription):
    """
    MessagesContext iterates over the messages of a pull consumer, made by
    :meth:`PullConsumer.messages` (nats.go MessagesContext). Use
    ``async for`` or :meth:`next`; the iteration ends once the context is
    stopped, or drained and its buffered messages taken.
    """

    def __init__(self, consumer: PullConsumer, opts: _PullOptions, err_on_missing_heartbeat: bool) -> None:
        super().__init__(consumer, opts)
        self._err_on_missing_heartbeat = err_on_missing_heartbeat
        self._queue: asyncio.Queue = asyncio.Queue()
        self._end_task: Optional[asyncio.Future] = None

    async def _start(self) -> None:
        await self._subscribe(self._deliver)

    async def _deliver(self, msg: Msg) -> None:
        self._queue.put_nowait(msg)

    def __aiter__(self) -> MessagesContext:
        return self

    async def __anext__(self) -> Msg:
        try:
            return await self.next()
        except nats.js.errors.MsgIteratorClosedError:
            raise StopAsyncIteration

    async def next(self, timeout: Optional[float] = None) -> Msg:
        """
        next returns the next message, pulling more when needed.

        :param timeout: Seconds to wait, raising nats.errors.TimeoutError
            (``None`` waits until a message arrives).
        :raises MsgIteratorClosedError: once stopped, or drained and empty.
        :raises NoHeartbeatError: when two heartbeats were missed.
        """
        if self._closed and not self._draining:
            raise nats.js.errors.MsgIteratorClosedError
        if self._opts.stop_after and self._delivered >= self._opts.stop_after:
            self.stop()
            raise nats.js.errors.MsgIteratorClosedError
        hb = self._opts.heartbeat
        start = time.monotonic()
        deadline = start + timeout if timeout is not None else None
        hb_deadline = start + 2 * hb if hb else None
        while True:
            async with self._lock:
                err = await self._pull(self._check_pending())
            if err is not None:
                raise err
            now = time.monotonic()
            waits = [d - now for d in (deadline, hb_deadline) if d is not None]
            wait = max(min(waits), 0) if waits else None
            try:
                item = await asyncio.wait_for(self._queue.get(), wait)
            except asyncio.TimeoutError:
                now = time.monotonic()
                if hb_deadline is not None and now >= hb_deadline and (deadline is None or hb_deadline < deadline):
                    # Missed heartbeats: the next round pulls afresh.
                    self._msg_count = 0
                    self._byte_count = 0
                    hb_deadline = now + 2 * hb
                    if self._err_on_missing_heartbeat:
                        raise nats.js.errors.NoHeartbeatError
                    continue
                raise nats.errors.TimeoutError
            if item is _CLOSED:
                self._queue.put_nowait(_CLOSED)
                raise nats.js.errors.MsgIteratorClosedError
            msg: Msg = item
            if hb_deadline is not None:
                hb_deadline = time.monotonic() + 2 * hb
            status = _status(msg)
            if status is None:
                self._consumer._update_pin(msg)
                self._delivered_msg(msg)
                return msg
            if status == api.StatusCode.CONTROL_MESSAGE:
                continue
            term, _ = self._handle_status(msg, status)
            if term is not None:
                self.stop()
                raise term

    def stop(self) -> None:
        """Stop iterating at once; buffered messages are discarded."""
        self._end(False)

    def drain(self) -> None:
        """Stop pulling; the buffered messages can still be taken."""
        self._end(True)

    def _end(self, drain: bool) -> None:
        if self._closed:
            return
        self._closed = True
        self._draining = drain
        if not drain:
            while not self._queue.empty():
                self._queue.get_nowait()
            self._queue.put_nowait(_CLOSED)
        self._end_task = asyncio.ensure_future(self._finish(drain))

    async def _finish(self, drain: bool) -> None:
        await self._end_subscription(drain)
        if drain:
            self._queue.put_nowait(_CLOSED)


@dataclass
class OrderedConsumerConfig:
    """
    OrderedConsumerConfig configures an :class:`OrderedConsumer` (nats.go
    OrderedConsumerConfig).

    - ``filter_subjects``: subjects to deliver, all of the stream by default.
    - ``deliver_policy``, ``opt_start_seq``, ``opt_start_time``: where the
      first consumer starts (``DeliverPolicy.ALL`` by default).
    - ``replay_policy``, ``headers_only``, ``metadata``: as in ConsumerConfig.
    - ``inactive_threshold``: seconds before an unused consumer is removed
      by the server (5 minutes by default).
    - ``max_reset_attempts``: attempts to recreate the consumer before giving
      up; ``None`` or 0 retries without limit.
    - ``name_prefix``: prefix of the consumer names, ``<prefix>_<n>``; a NUID
      by default.
    """

    filter_subjects: Optional[List[str]] = None
    deliver_policy: Optional[api.DeliverPolicy] = None
    opt_start_seq: Optional[int] = None
    opt_start_time: Optional[datetime.datetime] = None
    replay_policy: Optional[api.ReplayPolicy] = None
    inactive_threshold: Optional[float] = None
    headers_only: Optional[bool] = None
    max_reset_attempts: Optional[int] = None
    metadata: Optional[Dict[str, str]] = None
    name_prefix: Optional[str] = None


# Seconds a recreated ordered consumer may stay unused (nats.go default).
_ORDERED_INACTIVE_THRESHOLD = 300.0

# Backoff between attempts to recreate an ordered consumer.
_ORDERED_BACKOFF_START = 1.0
_ORDERED_BACKOFF_MAX = 10.0

# How an ordered consumer is used: the first use decides.
_KIND_NOT_SET = 0
_KIND_CONSUME = 1
_KIND_FETCH = 2


class _OrderedConsumerClosed(Exception):
    """The ordered consume or iteration stopped while the consumer was recreated."""


def _serial_from_name(name: str) -> int:
    _, sep, tail = name.rpartition("_")
    if not sep or not tail.isdigit():
        return 0
    return int(tail)


class OrderedConsumer:
    """
    OrderedConsumer delivers the messages of a stream in order, without
    acks, by recreating an ephemeral pull consumer (named
    ``<prefix>_<n>``) after the last delivered message whenever a message
    is missed, heartbeats stop or the consumer is gone (nats.go ordered
    consumer). Obtained from :meth:`JetStreamContext.ordered_consumer`.

    An ordered consumer is used either with :meth:`consume` or
    :meth:`messages`, or with :meth:`fetch`, :meth:`fetch_bytes`,
    :meth:`fetch_no_wait` and :meth:`next`, one call at a time.
    """

    def __init__(self, js: JetStreamContext, stream: str, config: OrderedConsumerConfig, prefix: str) -> None:
        self._js = js
        self._nc = js._nc
        self._stream = stream
        self._cfg = config
        self._prefix = prefix
        self._kind = _KIND_NOT_SET
        self._serial = 0
        self._stream_seq = 0
        self._deliver_seq = 0
        self._current: Optional[PullConsumer] = None
        self._current_ctx: Optional[Union[ConsumeContext, MessagesContext]] = None
        self._running_fetch: Optional[MessageBatch] = None
        self._active: Optional[Union[_OrderedConsumeContext, _OrderedMessagesContext]] = None
        self._reset_lock = asyncio.Lock()

    def _next_config(self) -> api.ConsumerConfig:
        """The configuration of the next consumer (nats.go getConsumerConfig)."""
        cfg = self._cfg
        self._serial += 1
        fresh = self._stream_seq == 0
        policy = cfg.deliver_policy or api.DeliverPolicy.ALL
        config = api.ConsumerConfig(
            name=f"{self._prefix}_{self._serial}",
            ack_policy=api.AckPolicy.NONE,
            inactive_threshold=cfg.inactive_threshold or _ORDERED_INACTIVE_THRESHOLD,
            num_replicas=1,
            mem_storage=True,
            headers_only=cfg.headers_only,
            metadata=cfg.metadata,
            replay_policy=cfg.replay_policy,
        )
        filters = cfg.filter_subjects or []
        if len(filters) == 1:
            config.filter_subject = filters[0]
        elif filters:
            config.filter_subjects = list(filters)
        if fresh:
            config.deliver_policy = policy
            if policy == api.DeliverPolicy.BY_START_SEQUENCE:
                config.opt_start_seq = cfg.opt_start_seq or 1
            elif policy == api.DeliverPolicy.BY_START_TIME:
                config.opt_start_time = cfg.opt_start_time
            elif policy == api.DeliverPolicy.LAST_PER_SUBJECT and not filters:
                config.filter_subjects = [">"]
        else:
            config.deliver_policy = api.DeliverPolicy.BY_START_SEQUENCE
            config.opt_start_seq = self._stream_seq + 1
        self._deliver_seq = 0
        return config

    def _check_delivered(self, msg: Msg) -> Tuple[bool, bool]:
        """Whether a message is stale (of a replaced consumer) or out of order."""
        meta = msg.metadata
        if _serial_from_name(meta.consumer) != self._serial:
            return True, False
        if meta.sequence.consumer != self._deliver_seq + 1:
            return False, True
        self._deliver_seq = meta.sequence.consumer
        self._stream_seq = meta.sequence.stream
        return False, False

    def _begin(self, consume: bool, fetch_running: bool = False) -> bool:
        """Check the call may proceed; whether the consumer must first be recreated."""
        if consume:
            if self._kind == _KIND_FETCH:
                raise nats.js.errors.OrderConsumerUsedAsFetchError
            needs_reset = self._current is None
            if not needs_reset and self._kind == _KIND_CONSUME:
                raise nats.js.errors.OrderedConsumerConcurrentRequestsError
            self._kind = _KIND_CONSUME
            return needs_reset
        if self._kind == _KIND_CONSUME:
            raise nats.js.errors.OrderConsumerUsedAsConsumeError
        if fetch_running:
            raise nats.js.errors.OrderedConsumerConcurrentRequestsError
        self._kind = _KIND_FETCH
        return True

    async def _reset(self) -> None:
        """
        Replace the current consumer with one starting after the last
        delivered message, retrying with backoff (nats.go reset).
        """
        async with self._reset_lock:
            if self._current is not None:
                if self._current_ctx is not None:
                    self._current_ctx.stop()
                    self._current_ctx = None
                asyncio.ensure_future(self._delete_quietly(self._current.name))
                self._current = None
            config = self._next_config()
            attempts = self._cfg.max_reset_attempts or -1
            attempt = 0
            interval = _ORDERED_BACKOFF_START
            while True:
                if self._active is not None and self._active._closed:
                    raise _OrderedConsumerClosed
                try:
                    info = await self._js.add_consumer(self._stream, config=config)
                    self._current = PullConsumer(self._js, self._stream, info.name, info)
                    return
                except Exception:
                    if 0 < attempts <= attempt + 1:
                        raise
                attempt += 1
                await asyncio.sleep(interval)
                interval = min(2 * interval, _ORDERED_BACKOFF_MAX)

    async def _delete_quietly(self, name: str) -> None:
        try:
            await self._js.delete_consumer(self._stream, name)
        except Exception:
            pass

    async def info(self) -> api.ConsumerInfo:
        """Fetch the info of the current consumer, updating :meth:`cached_info`."""
        if self._current is None:
            raise nats.js.errors.OrderedConsumerNotCreatedError
        return await self._current.info()

    def cached_info(self) -> Optional[api.ConsumerInfo]:
        """The info of the current consumer last fetched, without asking the server."""
        if self._current is None:
            return None
        return self._current.cached_info()

    async def _start_fetch(self) -> PullConsumer:
        batch = self._running_fetch
        self._begin(False, batch is not None and not batch.done)
        if batch is not None and batch._sseq:
            self._stream_seq = batch._sseq
        await self._reset()
        assert self._current is not None
        return self._current

    async def fetch(self, batch: int, **kwargs: Any) -> MessageBatch:
        """Fetch up to ``batch`` messages after the last fetched; see :meth:`PullConsumer.fetch`."""
        current = await self._start_fetch()
        self._running_fetch = await current.fetch(batch, **kwargs)
        return self._running_fetch

    async def fetch_bytes(self, max_bytes: int, **kwargs: Any) -> MessageBatch:
        """Fetch messages up to ``max_bytes``; see :meth:`PullConsumer.fetch_bytes`."""
        current = await self._start_fetch()
        self._running_fetch = await current.fetch_bytes(max_bytes, **kwargs)
        return self._running_fetch

    async def fetch_no_wait(self, batch: int) -> MessageBatch:
        """Fetch up to ``batch`` messages available now; see :meth:`PullConsumer.fetch_no_wait`."""
        current = await self._start_fetch()
        self._running_fetch = await current.fetch_no_wait(batch)
        return self._running_fetch

    async def next(self, max_wait: Optional[float] = None, heartbeat: Optional[float] = None) -> Msg:
        """Fetch the next message; see :meth:`PullConsumer.next`."""
        batch = await self.fetch(1, max_wait=max_wait, heartbeat=heartbeat)
        async for msg in batch:
            return msg
        if batch.error is not None:
            raise batch.error
        raise nats.errors.TimeoutError

    async def consume(
        self,
        cb: Callable[[Msg], Awaitable[None]],
        *,
        error_cb: Optional[Callable[[Any, Exception], Any]] = None,
        stop_after: Optional[int] = None,
        **kwargs: Any,
    ) -> _OrderedConsumeContext:
        """
        consume calls ``cb`` with each message in order, recreating the
        consumer as needed. The options are those of
        :meth:`PullConsumer.consume`; the heartbeat defaults to 5 seconds
        (half the expiry under 10 seconds). Missed heartbeats, a deleted
        consumer and no responders recreate the consumer and are reported
        to ``error_cb``.
        """
        if cb is None:
            raise nats.js.errors.HandlerRequiredError
        needs_reset = self._begin(True)
        # Validate the options before creating anything.
        _PullOptions(True, **_pull_kwargs(kwargs, stop_after))
        ctx = _OrderedConsumeContext(self, cb, error_cb, stop_after, kwargs)
        self._active = ctx
        if needs_reset:
            await self._reset()
        await ctx._consume()
        return ctx

    async def messages(self, *, stop_after: Optional[int] = None, **kwargs: Any) -> _OrderedMessagesContext:
        """
        messages returns an iterator over the messages in order,
        recreating the consumer as needed. The options are those of
        :meth:`PullConsumer.messages`.
        """
        needs_reset = self._begin(True)
        kwargs.pop("err_on_missing_heartbeat", None)
        _PullOptions(True, **_pull_kwargs(kwargs, stop_after))
        ctx = _OrderedMessagesContext(self, stop_after, kwargs)
        self._active = ctx
        if needs_reset:
            await self._reset()
        await ctx._renew_iterator()
        return ctx


def _pull_kwargs(kwargs: Dict[str, Any], stop_after: Optional[int]) -> Dict[str, Any]:
    """The keyword arguments of consume()/messages() as _PullOptions takes them."""
    names = (
        "max_messages",
        "max_bytes",
        "bytes_limit",
        "expires",
        "threshold_messages",
        "threshold_bytes",
        "min_pending",
        "min_ack_pending",
        "priority",
        "group",
        "heartbeat",
    )
    unknown = set(kwargs) - set(names)
    if unknown:
        raise TypeError(f"unexpected options: {', '.join(sorted(unknown))}")
    opts = {name: kwargs.get(name) for name in names}
    opts["stop_after"] = stop_after
    return opts


def _ordered_heartbeat(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """The ordered consumer's default heartbeat, as the inner consumers must use it."""
    if kwargs.get("heartbeat") is not None:
        return kwargs
    expires = kwargs.get("expires") or DEFAULT_EXPIRES
    return dict(kwargs, heartbeat=expires / 2 if expires < 10 else 5.0)


class _OrderedConsumeContext:
    """Controls :meth:`OrderedConsumer.consume`, with the methods of :class:`ConsumeContext`."""

    def __init__(
        self,
        oc: OrderedConsumer,
        cb: Callable[[Msg], Awaitable[None]],
        error_cb: Optional[Callable[[Any, Exception], Any]],
        stop_after: Optional[int],
        kwargs: Dict[str, Any],
    ) -> None:
        self._oc = oc
        self._cb = cb
        self._error_cb = error_cb
        self._stop_after = stop_after
        self._kwargs = _ordered_heartbeat(kwargs)
        self._delivered = 0
        self._closed = False
        self._closed_event = asyncio.Event()
        self._resetting = False
        self._reset_task: Optional[asyncio.Future] = None

    @property
    def is_closed(self) -> bool:
        """Whether consuming has fully stopped."""
        return self._closed_event.is_set()

    async def closed(self) -> None:
        """Wait until consuming has fully stopped."""
        await self._closed_event.wait()

    def stop(self) -> None:
        """Stop consuming at once."""
        self._end(False)

    def drain(self) -> None:
        """Stop consuming once the buffered messages are handled."""
        self._end(True)

    def _end(self, drain: bool) -> None:
        if self._closed:
            return
        self._closed = True
        ctx = self._oc._current_ctx
        asyncio.ensure_future(self._finish(ctx, drain))

    async def _finish(self, ctx: Optional[Union[ConsumeContext, MessagesContext]], drain: bool) -> None:
        if isinstance(ctx, ConsumeContext):
            if drain:
                ctx.drain()
            else:
                ctx.stop()
            await ctx.closed()
        self._closed_event.set()

    async def _consume(self) -> None:
        oc = self._oc
        assert oc._current is not None
        stop_after = None
        if self._stop_after:
            stop_after = self._stop_after - self._delivered
        serial = oc._serial
        ctx = await oc._current.consume(
            self._handler(serial),
            error_cb=self._internal_error_cb(serial),
            stop_after=stop_after,
            **self._kwargs,
        )
        if self._closed:
            ctx.stop()
        oc._current_ctx = ctx

    async def _report(self, err: Exception) -> None:
        if self._error_cb is None:
            return
        try:
            await _call(self._error_cb, self, err)
        except Exception as e:
            await self._oc._nc._error_cb(e)

    def _handler(self, serial: int) -> Callable[[Msg], Awaitable[None]]:
        async def handler(msg: Msg) -> None:
            oc = self._oc
            if self._closed or serial != oc._serial:
                return
            try:
                stale, mismatch = oc._check_delivered(msg)
            except nats.errors.Error as e:
                await self._report(e)
                return
            if stale:
                return
            if mismatch:
                self._request_reset(serial)
                return
            self._delivered += 1
            await self._cb(msg)
            if self._stop_after and self._delivered >= self._stop_after:
                self.stop()

        return handler

    def _internal_error_cb(self, serial: int) -> Callable[[Any, Exception], Awaitable[None]]:
        async def error_cb(ctx: Any, err: Exception) -> None:
            if isinstance(err, nats.errors.ConnectionClosedError):
                await self._report(err)
                self.stop()
                return
            await self._report(err)
            if isinstance(
                err,
                (nats.js.errors.NoHeartbeatError, nats.js.errors.ConsumerDeletedError, nats.errors.NoRespondersError),
            ):
                self._request_reset(serial)

        return error_cb

    def _request_reset(self, serial: int) -> None:
        if self._closed or self._resetting or serial != self._oc._serial:
            return
        self._resetting = True
        self._reset_task = asyncio.ensure_future(self._recreate())

    async def _recreate(self) -> None:
        try:
            await self._oc._reset()
            if not self._closed:
                await self._consume()
        except _OrderedConsumerClosed:
            pass
        except Exception as e:
            # The consumer could not be recreated: consuming stops.
            await self._report(e)
            self.stop()
        finally:
            self._resetting = False


class _OrderedMessagesContext:
    """Iterates over :meth:`OrderedConsumer.messages`, with the methods of :class:`MessagesContext`."""

    def __init__(self, oc: OrderedConsumer, stop_after: Optional[int], kwargs: Dict[str, Any]) -> None:
        self._oc = oc
        self._stop_after = stop_after
        self._kwargs = _ordered_heartbeat(kwargs)
        self._delivered = 0
        self._closed = False
        self._draining = False

    async def _renew_iterator(self) -> None:
        oc = self._oc
        assert oc._current is not None
        stop_after = None
        if self._stop_after:
            stop_after = self._stop_after - self._delivered
        oc._current_ctx = await oc._current.messages(
            stop_after=stop_after, err_on_missing_heartbeat=True, **self._kwargs
        )

    async def _renew(self) -> None:
        try:
            await self._oc._reset()
        except _OrderedConsumerClosed:
            raise nats.js.errors.MsgIteratorClosedError
        await self._renew_iterator()

    def __aiter__(self) -> _OrderedMessagesContext:
        return self

    async def __anext__(self) -> Msg:
        try:
            return await self.next()
        except nats.js.errors.MsgIteratorClosedError:
            raise StopAsyncIteration

    async def next(self, timeout: Optional[float] = None) -> Msg:
        """
        next returns the next message in order, recreating the consumer when
        a message was missed, heartbeats stopped or the consumer is gone.
        """
        while True:
            if self._closed:
                raise nats.js.errors.MsgIteratorClosedError
            if self._stop_after and self._delivered >= self._stop_after:
                self.stop()
                raise nats.js.errors.MsgIteratorClosedError
            ctx = self._oc._current_ctx
            if not isinstance(ctx, MessagesContext):
                await self._renew()
                continue
            try:
                msg = await ctx.next(timeout=timeout)
            except (nats.errors.TimeoutError, nats.errors.ConnectionClosedError):
                raise
            except nats.js.errors.MsgIteratorClosedError:
                # Stopped, drained, or all of stop_after delivered.
                self._closed = True
                raise
            except Exception:
                if self._draining:
                    self._closed = True
                    raise nats.js.errors.MsgIteratorClosedError
                await self._renew()
                continue
            stale, mismatch = self._oc._check_delivered(msg)
            if stale:
                continue
            if mismatch:
                if self._draining:
                    self._closed = True
                    raise nats.js.errors.MsgIteratorClosedError
                await self._renew()
                continue
            self._delivered += 1
            return msg

    def stop(self) -> None:
        """Stop iterating at once."""
        self._end(False)

    def drain(self) -> None:
        """Stop pulling; the buffered messages can still be taken."""
        self._end(True)

    def _end(self, drain: bool) -> None:
        if self._closed or self._draining:
            return
        ctx = self._oc._current_ctx
        if drain and isinstance(ctx, MessagesContext):
            self._draining = True
            ctx.drain()
            return
        self._closed = True
        if isinstance(ctx, MessagesContext):
            ctx.stop()
