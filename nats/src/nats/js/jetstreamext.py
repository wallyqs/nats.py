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

from typing import Any, Dict, Optional

import nats.errors
from nats.js.errors import APIError, BadRequestError, Error, NotFoundError

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
