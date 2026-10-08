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
that fetches batches of messages (:class:`PullConsumer`).
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, Optional

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
