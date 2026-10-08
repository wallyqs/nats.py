# Copyright 2026 The NATS Authors
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
JetStream extensions modelled on orbit.go's ``jetstreamext`` package:
atomic batch publishing, fast-ingest batch publishing and direct batch get.

Error codes follow nats-server's ``server/errors.json``, which is what the
server actually sends; orbit.go's codes for the fast-ingest errors
(10203-10206) and the gap mode (10202) disagree with it.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set

import nats.errors
from nats.aio.msg import Msg
from nats.js import api
from nats.js.errors import APIError, BadRequestError, Error, NoStreamResponseError, NotFoundError

if TYPE_CHECKING:
    from nats.aio.client import Client as NATS
    from nats.aio.subscription import Subscription
    from nats.js.client import JetStreamContext

# Headers of an atomic batch message (ADR-50).
BATCH_ID_HEADER = "Nats-Batch-Id"
BATCH_SEQ_HEADER = "Nats-Batch-Sequence"
BATCH_COMMIT_HEADER = "Nats-Batch-Commit"

# Value of BATCH_COMMIT_HEADER for the final, stored message of a batch.
BATCH_COMMIT_FINAL = "1"
# Value of BATCH_COMMIT_HEADER for an end-of-batch marker: the batch is
# committed and the marker itself is not stored (nats-server v2.14.0+).
BATCH_COMMIT_EOB = "eob"

# JetStream API error codes of batch publishing, from nats-server's
# server/errors.json.
JS_ERR_CODE_BATCH_PUBLISH_NOT_ENABLED = 10174  # JSAtomicPublishDisabledErr
JS_ERR_CODE_BATCH_PUBLISH_MISSING_SEQ = 10175  # JSAtomicPublishMissingSeqErr
JS_ERR_CODE_BATCH_PUBLISH_INCOMPLETE = 10176  # JSAtomicPublishIncompleteBatchErr
JS_ERR_CODE_BATCH_PUBLISH_UNSUPPORTED_HEADER = 10177  # JSAtomicPublishUnsupportedHeaderBatchErr
JS_ERR_CODE_BATCH_PUBLISH_INVALID_ID = 10179  # JSAtomicPublishInvalidBatchIDErr
JS_ERR_CODE_BATCH_PUBLISH_EXCEEDS_LIMIT = 10199  # JSAtomicPublishTooLargeBatchErrF
JS_ERR_CODE_BATCH_PUBLISH_INVALID_COMMIT = 10200  # JSAtomicPublishInvalidBatchCommitErr
JS_ERR_CODE_BATCH_PUBLISH_DUPLICATE_MSG_ID = 10201  # JSAtomicPublishContainsDuplicateMessageErr
JS_ERR_CODE_FAST_BATCH_NOT_ENABLED = 10205  # JSBatchPublishDisabledErr
JS_ERR_CODE_FAST_BATCH_INVALID_PATTERN = 10206  # JSBatchPublishInvalidPatternErr
JS_ERR_CODE_FAST_BATCH_INVALID_ID = 10207  # JSBatchPublishInvalidBatchIDErr
JS_ERR_CODE_FAST_BATCH_UNKNOWN_ID = 10208  # JSBatchPublishUnknownBatchIDErr
JS_ERR_CODE_ATOMIC_PUBLISH_TOO_MANY_INFLIGHT = 10210  # JSAtomicPublishTooManyInflight
JS_ERR_CODE_BATCH_PUBLISH_TOO_MANY_INFLIGHT = 10211  # JSBatchPublishTooManyInflight

# nats-server has no code of its own for an invalid gap mode in a fast batch
# reply subject: it answers with JSBatchPublishInvalidPatternErr.
JS_ERR_CODE_BATCH_PUBLISH_INVALID_GAP_MODE = JS_ERR_CODE_FAST_BATCH_INVALID_PATTERN


class BatchPublishNotEnabledError(BadRequestError):
    """Atomic batch publishing is not enabled on the stream (``allow_atomic``)."""


class BatchPublishMissingSeqError(BadRequestError):
    """An atomic batch message has no batch sequence."""


class BatchPublishIncompleteError(BadRequestError):
    """The atomic batch is incomplete and was abandoned by the server."""


class BatchPublishUnsupportedHeaderError(BadRequestError):
    """An atomic batch message uses a header that batches do not support."""


class BatchPublishInvalidIDError(BadRequestError):
    """The atomic batch ID is invalid (longer than 64 characters)."""


class BatchPublishExceedsLimitError(BadRequestError):
    """The atomic batch exceeds the server's batch size limit (default 1000)."""


class BatchPublishInvalidCommitError(BadRequestError):
    """The atomic batch commit header value is not recognized by the server."""


class BatchPublishDuplicateMsgIDError(BadRequestError):
    """The atomic batch contains a duplicate message ID (Nats-Msg-Id)."""


class FastBatchNotEnabledError(BadRequestError):
    """Fast-ingest batch publishing is not enabled on the stream (``allow_batched``)."""


class FastBatchInvalidPatternError(BadRequestError):
    """The fast batch reply subject does not follow the fast-ingest pattern."""


# An invalid gap mode is reported by nats-server as an invalid pattern.
BatchPublishInvalidGapModeError = FastBatchInvalidPatternError


class FastBatchInvalidIDError(BadRequestError):
    """The fast batch ID is invalid (longer than 64 characters)."""


class FastBatchUnknownIDError(BadRequestError):
    """The fast batch ID is unknown to the server, e.g. the batch already ended."""


class AtomicPublishTooManyInflightError(APIError):
    """Too many atomic batches are inflight on the stream (code 429)."""


class BatchPublishTooManyInflightError(APIError):
    """Too many fast batches are inflight on the stream (code 429)."""


_API_ERRORS = {
    JS_ERR_CODE_BATCH_PUBLISH_NOT_ENABLED: BatchPublishNotEnabledError,
    JS_ERR_CODE_BATCH_PUBLISH_MISSING_SEQ: BatchPublishMissingSeqError,
    JS_ERR_CODE_BATCH_PUBLISH_INCOMPLETE: BatchPublishIncompleteError,
    JS_ERR_CODE_BATCH_PUBLISH_UNSUPPORTED_HEADER: BatchPublishUnsupportedHeaderError,
    JS_ERR_CODE_BATCH_PUBLISH_INVALID_ID: BatchPublishInvalidIDError,
    JS_ERR_CODE_BATCH_PUBLISH_EXCEEDS_LIMIT: BatchPublishExceedsLimitError,
    JS_ERR_CODE_BATCH_PUBLISH_INVALID_COMMIT: BatchPublishInvalidCommitError,
    JS_ERR_CODE_BATCH_PUBLISH_DUPLICATE_MSG_ID: BatchPublishDuplicateMsgIDError,
    JS_ERR_CODE_FAST_BATCH_NOT_ENABLED: FastBatchNotEnabledError,
    JS_ERR_CODE_FAST_BATCH_INVALID_PATTERN: FastBatchInvalidPatternError,
    JS_ERR_CODE_FAST_BATCH_INVALID_ID: FastBatchInvalidIDError,
    JS_ERR_CODE_FAST_BATCH_UNKNOWN_ID: FastBatchUnknownIDError,
    JS_ERR_CODE_ATOMIC_PUBLISH_TOO_MANY_INFLIGHT: AtomicPublishTooManyInflightError,
    JS_ERR_CODE_BATCH_PUBLISH_TOO_MANY_INFLIGHT: BatchPublishTooManyInflightError,
}


def api_error_from(err: Dict[str, Any]) -> APIError:
    """
    Returns the error for a JetStream API error response (the ``error``
    object of a reply): the batch error class for a batch publishing
    ``err_code``, otherwise what ``APIError.from_error`` raises.
    """
    params = {k: err.get(k) for k in ("code", "description", "err_code", "stream", "seq") if k in err}
    cls = _API_ERRORS.get(err.get("err_code"))  # type: ignore[arg-type]
    if cls is not None:
        return cls(**params)
    if "code" in params:
        try:
            APIError.from_error(params)
        except APIError as e:
            return e
    return APIError(**params)


class BatchClosedError(Error):
    """The batch publisher was already committed, closed or discarded."""

    def __str__(self) -> str:
        return "nats: batch publisher closed"


class EmptyBatchError(Error):
    """There are no messages in the batch to close or publish."""

    def __str__(self) -> str:
        return "nats: no messages in batch"


class InvalidBatchAckError(Error):
    """The reply that commits a batch is not a valid batch acknowledgement."""

    def __str__(self) -> str:
        return "nats: invalid jetstream batch publish response"


class FastBatchGapDetectedError(Error):
    """
    The server detected a gap in a fast batch: the messages from
    ``expected_last_sequence`` up to, but not including,
    ``current_sequence`` were lost.
    """

    def __init__(
        self,
        expected_last_sequence: Optional[int] = None,
        current_sequence: Optional[int] = None,
    ) -> None:
        self.description = None
        self.expected_last_sequence = expected_last_sequence
        self.current_sequence = current_sequence

    def __str__(self) -> str:
        s = "nats: fast batch gap detected"
        if self.expected_last_sequence is not None:
            s += f": expected last sequence {self.expected_last_sequence}; current sequence {self.current_sequence}"
        return s


class FastBatchMsgError(Error):
    """
    The server failed to store the fast batch message at ``sequence``;
    ``error`` is the server's API error.
    """

    def __init__(self, sequence: int, error: APIError) -> None:
        self.description = None
        self.sequence = sequence
        self.error = error

    def __str__(self) -> str:
        return f"nats: error processing batch at sequence {self.sequence}: {self.error}"


class BatchAckTimeoutError(nats.errors.TimeoutError):
    """
    A fast batch timed out waiting for the server's flow acknowledgement
    of message ``sequence``; ``ack_sequence`` is the highest sequence
    acknowledged.
    """

    def __init__(self, sequence: int = 0, ack_sequence: Optional[int] = None) -> None:
        self.sequence = sequence
        self.ack_sequence = ack_sequence

    def __str__(self) -> str:
        s = f"nats: batch message {self.sequence} ack timeout"
        if self.ack_sequence is not None:
            s += f"; current ack sequence: {self.ack_sequence}"
        return s


class InvalidOptionError(Error):
    """An option has an invalid value."""

    def __str__(self) -> str:
        if self.description:
            return f"nats: invalid option: {self.description}"
        return "nats: invalid option"


class BatchUnsupportedError(Error):
    """The server does not support batch direct get (needs nats-server v2.11.0+)."""

    def __str__(self) -> str:
        return "nats: batch get not supported by server"


class InvalidResponseError(Error):
    """A direct get response is not a valid stream message."""

    def __str__(self) -> str:
        if self.description:
            return f"nats: invalid stream response: {self.description}"
        return "nats: invalid stream response"


class NoMessagesError(NotFoundError):
    """There are no messages to get for the given options."""

    def __init__(self, description: Optional[str] = None) -> None:
        super().__init__(code=404, description=description)

    def __str__(self) -> str:
        return "nats: no messages"


class SubjectRequiredError(Error):
    """At least one subject is required."""

    def __str__(self) -> str:
        return "nats: at least one subject is required"


EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT_HEADER = "Nats-Expected-Last-Subject-Sequence-Subject"


@dataclass
class BatchAck(api.PubAck):
    """
    BatchAck is the acknowledgement of a committed batch: the stream and
    sequence of its last stored message, its ``batch_id`` and the number of
    messages it stored (``batch_size``).
    """


@dataclass
class BatchFlowControl:
    """
    Flow control of an atomic batch publisher.

    :param ack_first: Wait for the server's ack of the first message.
    :param ack_every: Wait for an ack every ``ack_every`` messages (0 disables).
    :param ack_timeout: Seconds to wait for each of those acks; ``None`` is
        the JetStream context's timeout.
    """

    ack_first: bool = True
    ack_every: int = 0
    ack_timeout: Optional[float] = None


def _format_duration(seconds: float) -> str:
    """Formats seconds as Go's time.Duration.String does (e.g. "1m30s")."""
    ns = int(round(seconds * 1e9))
    if ns == 0:
        return "0s"
    sign = "-" if ns < 0 else ""
    ns = abs(ns)

    def frac(value: int, unit: int, digits: int) -> str:
        whole, rest = divmod(value, unit)
        if rest == 0:
            return str(whole)
        return f"{whole}.{str(rest).zfill(digits).rstrip('0')}"

    if ns < 1000:
        return f"{sign}{ns}ns"
    if ns < 1000_000:
        return f"{sign}{frac(ns, 1000, 3)}µs"
    if ns < 1000_000_000:
        return f"{sign}{frac(ns, 1000_000, 6)}ms"
    hours, rest = divmod(ns, 3600 * 1000_000_000)
    minutes, rest = divmod(rest, 60 * 1000_000_000)
    out = sign
    if hours:
        out += f"{hours}h"
    if hours or minutes:
        out += f"{minutes}m"
    return out + frac(rest, 1000_000_000, 9) + "s"


def _apply_msg_opts(
    headers: Optional[Dict[str, str]],
    msg_ttl: Optional[float],
    expect_stream: Optional[str],
    expect_last_sequence: Optional[int],
    expect_last_subject_sequence: Optional[int],
    expect_last_subject_sequence_subject: Optional[str],
) -> Optional[Dict[str, str]]:
    """
    Sets the headers of a batch message's options, in orbit.go's order, and
    returns the headers (a new dict only when an option needs one).
    """
    if expect_last_subject_sequence_subject is not None:
        if expect_last_subject_sequence_subject == "":
            raise InvalidOptionError("subject cannot be empty")
        if expect_last_subject_sequence is None:
            raise InvalidOptionError("expect_last_subject_sequence is required with a subject")
    if not msg_ttl and not expect_stream and expect_last_sequence is None and expect_last_subject_sequence is None:
        return headers
    if headers is None:
        headers = {}
    if msg_ttl is not None and msg_ttl > 0:
        headers[api.Header.MSG_TTL.value] = _format_duration(msg_ttl)
    if expect_stream:
        headers[api.Header.EXPECTED_STREAM.value] = expect_stream
    if expect_last_subject_sequence is not None:
        if expect_last_subject_sequence_subject:
            headers[EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT_HEADER] = expect_last_subject_sequence_subject
        headers[api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE.value] = str(expect_last_subject_sequence)
    if expect_last_sequence is not None:
        headers[api.Header.EXPECTED_LAST_SEQUENCE.value] = str(expect_last_sequence)
    return headers


def _new_msg(nc: NATS, subject: str, payload: bytes, headers: Optional[Dict[str, str]] = None) -> Msg:
    return Msg(_client=nc, subject=subject, data=payload, headers=headers)


def _needs_ack(flow_control: BatchFlowControl, sequence: int) -> bool:
    if flow_control.ack_first and sequence == 1:
        return True
    return flow_control.ack_every > 0 and sequence % flow_control.ack_every == 0


async def _request_msg(nc: NATS, msg: Msg, timeout: float) -> Msg:
    try:
        return await nc.request(msg.subject, msg.data, timeout=timeout, headers=msg.headers)
    except nats.errors.NoRespondersError:
        raise NoStreamResponseError


async def _flow_request(nc: NATS, msg: Msg, timeout: float, strict: bool) -> None:
    """
    Publishes a batch message that waits for the server's flow control
    reply: no data is the ack, an error reply raises its API error.
    """
    resp = await _request_msg(nc, msg, timeout)
    if not resp.data:
        return
    try:
        reply = json.loads(resp.data)
    except ValueError:
        if strict:
            raise InvalidBatchAckError
        return
    if isinstance(reply, dict) and reply.get("error"):
        raise api_error_from(reply["error"])


def _parse_batch_ack(data: bytes, batch_id: str, expected_size: int) -> BatchAck:
    """Decides the reply that commits an atomic batch (orbit.go parseBatchAck)."""
    try:
        reply = json.loads(data)
    except ValueError:
        raise InvalidBatchAckError
    if not isinstance(reply, dict):
        raise InvalidBatchAckError
    if reply.get("error"):
        raise api_error_from(reply["error"])
    if not reply.get("stream") or reply.get("batch") != batch_id or reply.get("count") != expected_size:
        raise InvalidBatchAckError
    return BatchAck.from_response(reply)


class BatchPublisher:
    """
    BatchPublisher publishes messages to a stream as one atomic batch
    (ADR-50, needs a stream with ``allow_atomic``). Each message is
    published as it is added and stored only when the batch commits:
    ``commit``/``commit_msg`` commit it with a final stored message,
    ``close`` with an end-of-batch marker that is not stored (nats-server
    v2.14.0+), and ``discard`` abandons it.

    Messages take these options, which set the matching headers:

    :param msg_ttl: Per-message TTL in seconds (stream needs ``allow_msg_ttl``).
    :param expect_stream: Expected stream name.
    :param expect_last_sequence: Expected last stream sequence.
    :param expect_last_subject_sequence: Expected last sequence of the
        message's subject, or of ``expect_last_subject_sequence_subject``.
    :param expect_last_subject_sequence_subject: Subject (may be a wildcard)
        whose last sequence ``expect_last_subject_sequence`` is.

    ::

        batch = new_batch_publisher(js)
        await batch.add("orders.new", b"1")
        await batch.add("orders.new", b"2", expect_stream="ORDERS")
        ack = await batch.commit("orders.new", b"3")
        print(ack.stream, ack.seq, ack.batch_size)  # ORDERS 3 3
    """

    def __init__(self, js: JetStreamContext, flow_control: Optional[BatchFlowControl] = None) -> None:
        self._js = js
        self._nc = js._nc
        fc = flow_control or BatchFlowControl()
        if fc.ack_timeout is None:
            fc = BatchFlowControl(fc.ack_first, fc.ack_every, js._timeout)
        self._flow_control = fc
        self._batch_id = js._nc._nuid.next().decode()
        self._sequence = 0
        self._closed = False
        # Subject of the first message, used by the end-of-batch marker.
        self._batch_subject: Optional[str] = None
        self._lock = asyncio.Lock()

    @property
    def batch_id(self) -> str:
        return self._batch_id

    async def add(
        self,
        subject: str,
        payload: bytes = b"",
        msg_ttl: Optional[float] = None,
        expect_stream: Optional[str] = None,
        expect_last_sequence: Optional[int] = None,
        expect_last_subject_sequence: Optional[int] = None,
        expect_last_subject_sequence_subject: Optional[str] = None,
    ) -> None:
        """
        add publishes a message to the batch; it is stored when the batch
        commits. The first message (and every ``ack_every``-th) waits for
        the server's ack.
        """
        await self.add_msg(
            _new_msg(self._nc, subject, payload),
            msg_ttl=msg_ttl,
            expect_stream=expect_stream,
            expect_last_sequence=expect_last_sequence,
            expect_last_subject_sequence=expect_last_subject_sequence,
            expect_last_subject_sequence_subject=expect_last_subject_sequence_subject,
        )

    async def add_msg(
        self,
        msg: Msg,
        msg_ttl: Optional[float] = None,
        expect_stream: Optional[str] = None,
        expect_last_sequence: Optional[int] = None,
        expect_last_subject_sequence: Optional[int] = None,
        expect_last_subject_sequence_subject: Optional[str] = None,
    ) -> None:
        """
        add_msg publishes ``msg`` to the batch. Its headers hold the
        options' and the batch's headers afterwards.
        """
        async with self._lock:
            if self._closed:
                raise BatchClosedError
            headers = msg.headers if msg.headers is not None else {}
            msg.headers = _apply_msg_opts(
                headers,
                msg_ttl,
                expect_stream,
                expect_last_sequence,
                expect_last_subject_sequence,
                expect_last_subject_sequence_subject,
            )
            self._sequence += 1
            if self._batch_subject is None:
                self._batch_subject = msg.subject
            headers[BATCH_ID_HEADER] = self._batch_id
            headers[BATCH_SEQ_HEADER] = str(self._sequence)

            if not _needs_ack(self._flow_control, self._sequence):
                await self._nc.publish(msg.subject, msg.data, headers=headers)
                return
            await _flow_request(self._nc, msg, self._flow_control.ack_timeout or self._js._timeout, True)

    async def commit(
        self,
        subject: str,
        payload: bytes = b"",
        timeout: Optional[float] = None,
        msg_ttl: Optional[float] = None,
        expect_stream: Optional[str] = None,
        expect_last_sequence: Optional[int] = None,
        expect_last_subject_sequence: Optional[int] = None,
        expect_last_subject_sequence_subject: Optional[str] = None,
    ) -> BatchAck:
        """
        commit publishes the final message and commits the batch, returning
        the server's BatchAck. ``timeout`` defaults to the JetStream
        context's timeout.
        """
        return await self.commit_msg(
            _new_msg(self._nc, subject, payload),
            timeout=timeout,
            msg_ttl=msg_ttl,
            expect_stream=expect_stream,
            expect_last_sequence=expect_last_sequence,
            expect_last_subject_sequence=expect_last_subject_sequence,
            expect_last_subject_sequence_subject=expect_last_subject_sequence_subject,
        )

    async def commit_msg(
        self,
        msg: Msg,
        timeout: Optional[float] = None,
        msg_ttl: Optional[float] = None,
        expect_stream: Optional[str] = None,
        expect_last_sequence: Optional[int] = None,
        expect_last_subject_sequence: Optional[int] = None,
        expect_last_subject_sequence_subject: Optional[str] = None,
    ) -> BatchAck:
        """
        commit_msg publishes ``msg`` as the final message, with the commit
        header, and returns the server's BatchAck.
        """
        if timeout is None:
            timeout = self._js._timeout
        async with self._lock:
            if self._closed:
                raise BatchClosedError
            headers = msg.headers if msg.headers is not None else {}
            msg.headers = _apply_msg_opts(
                headers,
                msg_ttl,
                expect_stream,
                expect_last_sequence,
                expect_last_subject_sequence,
                expect_last_subject_sequence_subject,
            )
            self._sequence += 1
            headers[BATCH_ID_HEADER] = self._batch_id
            headers[BATCH_SEQ_HEADER] = str(self._sequence)
            headers[BATCH_COMMIT_HEADER] = BATCH_COMMIT_FINAL
            resp = await _request_msg(self._nc, msg, timeout)
            self._closed = True
            return _parse_batch_ack(resp.data, self._batch_id, self._sequence)

    async def close(self, timeout: Optional[float] = None) -> BatchAck:
        """
        close commits the batch with an end-of-batch marker, without
        storing a final message: the marker is sent on the first message's
        subject and is neither stored nor counted in the ack's
        ``batch_size``. Requires nats-server v2.14.0 or later.

        To abandon a batch without committing it, use ``discard``.
        """
        if timeout is None:
            timeout = self._js._timeout
        async with self._lock:
            if self._closed:
                raise BatchClosedError
            if self._sequence == 0 or not self._batch_subject:
                raise EmptyBatchError
            headers = {
                BATCH_ID_HEADER: self._batch_id,
                BATCH_SEQ_HEADER: str(self._sequence + 1),
                BATCH_COMMIT_HEADER: BATCH_COMMIT_EOB,
            }
            resp = await _request_msg(self._nc, _new_msg(self._nc, self._batch_subject, b"", headers), timeout)
            self._closed = True
            return _parse_batch_ack(resp.data, self._batch_id, self._sequence)

    def discard(self) -> None:
        """
        discard abandons the batch without committing it; the server drops
        it after a timeout.
        """
        if self._closed:
            raise BatchClosedError
        self._closed = True

    def size(self) -> int:
        """size returns the number of messages added to the batch so far."""
        return self._sequence

    def is_closed(self) -> bool:
        """is_closed tells whether the batch was committed, closed or discarded."""
        return self._closed


def new_batch_publisher(js: JetStreamContext, flow_control: Optional[BatchFlowControl] = None) -> BatchPublisher:
    """
    new_batch_publisher returns a BatchPublisher of one atomic batch on
    ``js``. By default the first message waits for its ack, for the
    context's timeout.
    """
    return BatchPublisher(js, flow_control)


async def publish_msg_batch(
    js: JetStreamContext,
    messages: List[Msg],
    flow_control: Optional[BatchFlowControl] = None,
    timeout: Optional[float] = None,
) -> BatchAck:
    """
    publish_msg_batch publishes ``messages`` as one atomic batch and waits
    for the ack of the last one, which commits it. Each message gets the
    batch's headers (and loses a commit header it brought).
    """
    if not messages:
        raise EmptyBatchError
    if timeout is None:
        timeout = js._timeout
    nc = js._nc
    fc = flow_control or BatchFlowControl()
    ack_timeout = fc.ack_timeout if fc.ack_timeout is not None else js._timeout
    batch_id = nc._nuid.next().decode()
    count = len(messages)
    for i, msg in enumerate(messages):
        sequence = i + 1
        if msg.headers is None:
            msg.headers = {}
        msg.headers.pop(BATCH_COMMIT_HEADER, None)
        msg.headers[BATCH_ID_HEADER] = batch_id
        msg.headers[BATCH_SEQ_HEADER] = str(sequence)
        if sequence < count:
            if not _needs_ack(fc, sequence):
                await nc.publish(msg.subject, msg.data, headers=msg.headers)
            else:
                await _flow_request(nc, msg, ack_timeout, False)
            continue
        msg.headers[BATCH_COMMIT_HEADER] = BATCH_COMMIT_FINAL
        resp = await _request_msg(nc, msg, timeout)
        return _parse_batch_ack(resp.data, batch_id, count)
    raise EmptyBatchError  # unreachable: messages is not empty


# Operations of a fast-ingest batch message, the <op> token of its reply
# subject <inbox>.<flow>.<gap>.<seq>.<op>.$FI.
_FAST_BATCH_START = 0
_FAST_BATCH_ADD = 1
_FAST_BATCH_COMMIT = 2
_FAST_BATCH_COMMIT_EOB = 3
_FAST_BATCH_PING = 4

_DEFAULT_FAST_FLOW = 100
_DEFAULT_MAX_OUTSTANDING_ACKS = 2


@dataclass
class FastPublishFlowControl:
    """
    Flow control of a fast-ingest batch publisher.

    :param flow: Initial number of messages per server flow ack (1-65535,
        0 keeps the default of 100); the server may change it.
    :param max_outstanding_acks: How many flow acks may be outstanding
        before ``add`` stalls (1-65535, 0 keeps the default of 2).
    :param ack_timeout: Seconds to wait for acks; ``None`` or 0 is the
        JetStream context's timeout.
    """

    flow: int = _DEFAULT_FAST_FLOW
    max_outstanding_acks: int = _DEFAULT_MAX_OUTSTANDING_ACKS
    ack_timeout: Optional[float] = None


@dataclass
class FastPubAck:
    """
    FastPubAck is the result of adding a message to a fast batch.

    :param batch_sequence: Sequence of the message within the batch.
    :param ack_sequence: Highest batch sequence the server acknowledged.
        With the default gap mode every message up to it was persisted;
        with ``continue_on_gap`` some of them may have been lost.
    """

    batch_sequence: int
    ack_sequence: int


class FastPublisher:
    """
    FastPublisher publishes a fast-ingest batch (needs a stream with
    ``allow_batched``, nats-server v2.14.0+). Messages are stored as they
    arrive; the server sends a flow ack every ``flow`` messages and ``add``
    stalls while ``max_outstanding_acks`` acks are outstanding, pinging the
    server to recover lost acks until the ack timeout. ``commit`` and
    ``commit_msg`` end the batch with a final message, ``close`` with an
    end-of-batch commit.

    A gap the server detects abandons the batch unless the publisher was
    made with ``continue_on_gap``; either way it is reported to the error
    handler as FastBatchGapDetectedError, as are the server's errors for
    single messages (FastBatchMsgError) and replies that cannot be decoded.

    A FastPublisher is not safe for concurrent use: await each call before
    making the next one. Messages take the same options as
    BatchPublisher's.

    ::

        fp = new_fast_publisher(js, FastPublishFlowControl(flow=200))
        for i in range(1000):
            await fp.add("events.raw", str(i).encode())
        ack = await fp.close()
    """

    def __init__(
        self,
        js: JetStreamContext,
        flow_control: Optional[FastPublishFlowControl] = None,
        continue_on_gap: bool = False,
        error_handler: Optional[Callable[[Exception], Any]] = None,
    ) -> None:
        fc = flow_control or FastPublishFlowControl()
        if fc.ack_timeout is not None and fc.ack_timeout < 0:
            raise InvalidOptionError("ack timeout must be non-negative")
        for name in ("flow", "max_outstanding_acks"):
            value = getattr(fc, name)
            if value < 0 or value > 65535:
                raise InvalidOptionError(f"{name} must be between 0 and 65535")
        self._js = js
        self._nc = js._nc
        self._flow = fc.flow or _DEFAULT_FAST_FLOW
        self._max_outstanding_acks = fc.max_outstanding_acks or _DEFAULT_MAX_OUTSTANDING_ACKS
        self._ack_timeout = fc.ack_timeout or js._timeout
        self._continue_on_gap = continue_on_gap
        self._error_handler = error_handler
        self._inbox = self._nc.new_inbox()
        gap = "ok" if continue_on_gap else "fail"
        # Fixed when the publisher is made, not rebuilt when the flow changes.
        self._reply_prefix = f"{self._inbox}.{self._flow}.{gap}."
        self._ack_sub: Optional[Subscription] = None
        self._sequence = 0
        self._ack_sequence = 0
        self._closed = False
        # Subject of the first message, used by pings and close.
        self._batch_subject: Optional[str] = None
        self._first_ack: Optional[asyncio.Future] = None
        self._commit_ack: Optional[asyncio.Future] = None
        self._stall: Optional[asyncio.Event] = None
        # The error reported last, raised by a call the batch's end interrupts.
        self._last_error: Optional[Exception] = None
        self._tasks: Set[asyncio.Task] = set()

    def _reply(self, sequence: int, operation: int) -> str:
        return f"{self._reply_prefix}{sequence}.{operation}.$FI"

    async def _subscribe(self) -> None:
        if self._stall is None:
            self._stall = asyncio.Event()
        if self._ack_sub is None:
            self._ack_sub = await self._nc.subscribe(f"{self._inbox}.>", cb=self._handle_ack)

    async def _unsubscribe(self) -> None:
        sub, self._ack_sub = self._ack_sub, None
        if sub is not None:
            try:
                await sub.unsubscribe()
            except nats.errors.Error:
                pass

    def _must_wait(self) -> bool:
        return self._ack_sequence + self._flow * self._max_outstanding_acks <= self._sequence

    async def _send_ping(self) -> None:
        """Asks the server for its latest flow ack, reusing the highest sequence sent."""
        await self._nc.publish(
            self._batch_subject or "",
            b"",
            reply=self._reply(self._sequence, _FAST_BATCH_PING),
        )

    def _ended_error(self) -> Exception:
        return self._last_error or BatchClosedError()

    async def _wait_for_acks(self) -> None:
        """
        Waits until fewer than max_outstanding_acks acks are outstanding,
        pinging every third of the ack timeout. The ack timeout bounds the
        whole wait.
        """
        assert self._stall is not None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._ack_timeout
        interval = self._ack_timeout / 3
        next_ping = loop.time() + interval
        while True:
            if self._closed:
                raise self._ended_error()
            if not self._must_wait():
                return
            now = loop.time()
            if now >= deadline:
                raise BatchAckTimeoutError(self._sequence, self._ack_sequence)
            if now >= next_ping:
                await self._send_ping()
                next_ping = now + interval
                continue
            self._stall.clear()
            try:
                await asyncio.wait_for(self._stall.wait(), min(deadline, next_ping) - now)
            except asyncio.TimeoutError:
                pass

    async def add(
        self,
        subject: str,
        payload: bytes = b"",
        msg_ttl: Optional[float] = None,
        expect_stream: Optional[str] = None,
        expect_last_sequence: Optional[int] = None,
        expect_last_subject_sequence: Optional[int] = None,
        expect_last_subject_sequence_subject: Optional[str] = None,
    ) -> FastPubAck:
        """
        add publishes a message to the batch. The first message waits for
        the server's ack; later ones wait only while too many acks are
        outstanding.
        """
        return await self.add_msg(
            _new_msg(self._nc, subject, payload),
            msg_ttl=msg_ttl,
            expect_stream=expect_stream,
            expect_last_sequence=expect_last_sequence,
            expect_last_subject_sequence=expect_last_subject_sequence,
            expect_last_subject_sequence_subject=expect_last_subject_sequence_subject,
        )

    async def add_msg(
        self,
        msg: Msg,
        msg_ttl: Optional[float] = None,
        expect_stream: Optional[str] = None,
        expect_last_sequence: Optional[int] = None,
        expect_last_subject_sequence: Optional[int] = None,
        expect_last_subject_sequence_subject: Optional[str] = None,
    ) -> FastPubAck:
        """
        add_msg publishes ``msg`` to the batch and returns its FastPubAck.
        Its reply is set to the batch's reply subject.
        """
        if self._closed:
            raise BatchClosedError
        msg.headers = _apply_msg_opts(
            msg.headers,
            msg_ttl,
            expect_stream,
            expect_last_sequence,
            expect_last_subject_sequence,
            expect_last_subject_sequence_subject,
        )
        self._sequence += 1
        sequence = self._sequence
        if sequence == 1:
            return await self._start(msg)
        msg.reply = self._reply(sequence, _FAST_BATCH_ADD)
        await self._nc.publish(msg.subject, msg.data, reply=msg.reply, headers=msg.headers)
        if self._must_wait():
            try:
                await self._wait_for_acks()
            except Exception:
                self._closed = True
                await self._unsubscribe()
                raise
        return FastPubAck(batch_sequence=sequence, ack_sequence=self._ack_sequence)

    async def _start(self, msg: Msg) -> FastPubAck:
        """Publishes the first message and waits for its ack."""
        await self._subscribe()
        self._first_ack = asyncio.get_running_loop().create_future()
        msg.reply = self._reply(1, _FAST_BATCH_START)
        try:
            await self._nc.publish(msg.subject, msg.data, reply=msg.reply, headers=msg.headers)
            ack_sequence = await asyncio.wait_for(self._first_ack, self._ack_timeout)
        except asyncio.TimeoutError:
            self._closed = True
            await self._unsubscribe()
            raise BatchAckTimeoutError(1)
        except Exception:
            self._closed = True
            await self._unsubscribe()
            raise
        finally:
            self._first_ack = None
        self._batch_subject = msg.subject
        return FastPubAck(batch_sequence=1, ack_sequence=ack_sequence)

    async def commit(
        self,
        subject: str,
        payload: bytes = b"",
        timeout: Optional[float] = None,
        msg_ttl: Optional[float] = None,
        expect_stream: Optional[str] = None,
        expect_last_sequence: Optional[int] = None,
        expect_last_subject_sequence: Optional[int] = None,
        expect_last_subject_sequence_subject: Optional[str] = None,
    ) -> BatchAck:
        """
        commit publishes the final message and commits the batch; see
        commit_msg.
        """
        return await self.commit_msg(
            _new_msg(self._nc, subject, payload),
            timeout=timeout,
            msg_ttl=msg_ttl,
            expect_stream=expect_stream,
            expect_last_sequence=expect_last_sequence,
            expect_last_subject_sequence=expect_last_subject_sequence,
            expect_last_subject_sequence_subject=expect_last_subject_sequence_subject,
        )

    async def commit_msg(
        self,
        msg: Msg,
        timeout: Optional[float] = None,
        msg_ttl: Optional[float] = None,
        expect_stream: Optional[str] = None,
        expect_last_sequence: Optional[int] = None,
        expect_last_subject_sequence: Optional[int] = None,
        expect_last_subject_sequence_subject: Optional[str] = None,
    ) -> BatchAck:
        """
        commit_msg publishes ``msg`` as the final message of the batch and
        returns the server's BatchAck, pinging for a lost ack until the ack
        timeout. ``timeout`` (seconds) bounds the call as well.
        """
        if self._closed:
            raise BatchClosedError
        msg.headers = _apply_msg_opts(
            msg.headers,
            msg_ttl,
            expect_stream,
            expect_last_sequence,
            expect_last_subject_sequence,
            expect_last_subject_sequence_subject,
        )
        return await self._commit(msg, _FAST_BATCH_COMMIT, timeout)

    async def close(self, timeout: Optional[float] = None) -> BatchAck:
        """
        close ends the batch with an end-of-batch commit on the first
        message's subject, without adding a message, and returns the
        server's BatchAck.
        """
        if self._sequence == 0:
            raise EmptyBatchError
        if self._closed:
            raise BatchClosedError
        return await self._commit(_new_msg(self._nc, self._batch_subject or "", b""), _FAST_BATCH_COMMIT_EOB, timeout)

    async def _commit(self, msg: Msg, operation: int, timeout: Optional[float]) -> BatchAck:
        loop = asyncio.get_running_loop()
        self._sequence += 1
        if self._batch_subject is None:
            self._batch_subject = msg.subject
        msg.reply = self._reply(self._sequence, operation)
        commit_ack = self._commit_ack = loop.create_future()
        try:
            await self._subscribe()
            await self._nc.publish(msg.subject, msg.data, reply=msg.reply, headers=msg.headers)
            now = loop.time()
            deadline = now + self._ack_timeout
            call_deadline = now + timeout if timeout is not None else None
            interval = self._ack_timeout / 3
            next_ping = now + interval
            while not commit_ack.done():
                now = loop.time()
                if call_deadline is not None and now >= call_deadline:
                    raise nats.errors.TimeoutError
                if now >= deadline:
                    raise BatchAckTimeoutError(self._sequence)
                if now >= next_ping:
                    await self._send_ping()
                    next_ping = now + interval
                    continue
                wake = min(deadline, next_ping)
                if call_deadline is not None:
                    wake = min(wake, call_deadline)
                try:
                    await asyncio.wait_for(asyncio.shield(commit_ack), wake - now)
                except asyncio.TimeoutError:
                    pass
            reply = commit_ack.result()
        finally:
            self._closed = True
            self._commit_ack = None
            await self._unsubscribe()
        if reply.get("error"):
            raise api_error_from(reply["error"])
        if not reply.get("stream"):
            raise InvalidBatchAckError
        return BatchAck.from_response(reply)

    def is_closed(self) -> bool:
        """is_closed tells whether the batch was committed or abandoned."""
        return self._closed

    async def _report(self, err: Exception) -> None:
        if self._error_handler is None:
            return
        result = self._error_handler(err)
        if inspect.isawaitable(result):
            await result

    async def _handle_ack(self, msg: Msg) -> None:
        """
        Handles the batch's replies: flow acks move the acked sequence and
        the flow, gaps and message errors go to the error handler, and a
        reply without a type is the commit ack that ends the batch.
        """
        try:
            reply = json.loads(msg.data)
        except ValueError as e:
            await self._report(e)
            return
        if not isinstance(reply, dict):
            await self._report(InvalidBatchAckError())
            return
        kind = reply.get("type")
        if kind == "ack":
            messages = reply.get("msgs")
            if isinstance(messages, int) and messages > 0:
                self._flow = messages
            self._ack_sequence = reply.get("seq") or 0
            if self._first_ack is not None and not self._first_ack.done():
                self._first_ack.set_result(self._ack_sequence)
            elif self._stall is not None:
                self._stall.set()
            return
        if kind == "gap":
            self._last_error = FastBatchGapDetectedError(reply.get("last_seq"), reply.get("seq"))
            await self._report(self._last_error)
            return
        if kind == "err":
            self._last_error = FastBatchMsgError(reply.get("seq") or 0, api_error_from(reply.get("error") or {}))
            await self._report(self._last_error)
            return

        # The commit ack, which always ends the batch.
        self._closed = True
        error = api_error_from(reply["error"]) if reply.get("error") else None
        if self._commit_ack is not None and not self._commit_ack.done():
            self._commit_ack.set_result(reply)
        elif self._first_ack is not None and not self._first_ack.done() and error is not None:
            self._first_ack.set_exception(error)
        else:
            if error is not None:
                self._last_error = error
                await self._report(error)
            task = asyncio.ensure_future(self._unsubscribe())
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        if self._stall is not None:
            self._stall.set()


def new_fast_publisher(
    js: JetStreamContext,
    flow_control: Optional[FastPublishFlowControl] = None,
    continue_on_gap: bool = False,
    error_handler: Optional[Callable[[Exception], Any]] = None,
) -> FastPublisher:
    """
    new_fast_publisher returns a FastPublisher on ``js``. By default the
    server acks every 100 messages, ``add`` stalls once two acks are
    outstanding, waits are bounded by the context's timeout, and a gap
    abandons the batch.

    :param continue_on_gap: Keep the batch going when the server detects
        a gap, only reporting it to the error handler.
    :param error_handler: Callable (or coroutine function) that receives
        gaps, per-message server errors and undecodable replies.
    """
    return FastPublisher(js, flow_control, continue_on_gap, error_handler)
