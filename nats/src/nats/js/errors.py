# Copyright 2016-2024 The NATS Authors
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

from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Dict, NoReturn, Optional, Type

import nats.errors
from nats.js import api

if TYPE_CHECKING:
    from nats.aio.msg import Msg


class ErrorCode(IntEnum):
    """
    JetStream API error codes, as sent by the server in the ``err_code``
    field of an API error (nats.go ``jetstream.ErrorCode`` and its
    ``JSErrCode*`` constants).
    """

    BAD_REQUEST = 10003
    CONSUMER_CREATE = 10012
    CONSUMER_NAME_EXISTS = 10013
    CONSUMER_NOT_FOUND = 10014
    MAXIMUM_CONSUMERS_LIMIT = 10026
    MESSAGE_NOT_FOUND = 10037
    JETSTREAM_NOT_ENABLED_FOR_ACCOUNT = 10039
    STREAM_NAME_IN_USE = 10058
    STREAM_NOT_FOUND = 10059
    STREAM_WRONG_LAST_SEQUENCE = 10071
    JETSTREAM_NOT_ENABLED = 10076
    CONSUMER_ALREADY_EXISTS = 10105
    DUPLICATE_FILTER_SUBJECTS = 10136
    OVERLAPPING_FILTER_SUBJECTS = 10138
    CONSUMER_EMPTY_FILTER = 10139
    CONSUMER_EXISTS = 10148
    CONSUMER_DOES_NOT_EXIST = 10149
    STREAM_WRONG_LAST_SEQUENCE_CONSTANT = 10164
    MIRROR_WITH_MSG_SCHEDULES = 10186
    SOURCE_WITH_MSG_SCHEDULES = 10187
    MESSAGE_SCHEDULES_DISABLED = 10188
    SCHEDULE_PATTERN_INVALID = 10189
    SCHEDULE_TARGET_INVALID = 10190
    SCHEDULE_TTL_INVALID = 10191
    SCHEDULE_ROLLUP_INVALID = 10192
    SCHEDULE_SOURCE_INVALID = 10203
    CONSUMER_INVALID_RESET = 10204


class Error(nats.errors.Error):
    """
    An Error raised by the NATS client when using JetStream.
    """

    def __init__(self, description: Optional[str] = None) -> None:
        self.description = description

    def __str__(self) -> str:
        desc = ""
        if self.description:
            desc = self.description
        return f"nats: JetStream.{self.__class__.__name__} {desc}"

    @property
    def api_error(self) -> Optional[APIError]:
        """
        The API error returned by the server that caused this error, if any
        (nats.go ``JetStreamError.APIError()``).
        """
        return None


# JetStreamError is the nats.go name of the base JetStream error.
JetStreamError = Error


@dataclass(repr=False, init=False)
class APIError(Error):
    """
    An Error that is the result of interacting with NATS JetStream.
    """

    code: Optional[int]
    err_code: Optional[int]
    description: Optional[str]
    stream: Optional[str]
    seq: Optional[int]

    def __init__(
        self,
        code: Optional[int] = None,
        description: Optional[str] = None,
        err_code: Optional[int] = None,
        stream: Optional[str] = None,
        seq: Optional[int] = None,
    ) -> None:
        self.code = code
        self.err_code = err_code
        self.description = description
        self.stream = stream
        self.seq = seq

    @classmethod
    def from_msg(cls, msg: Msg) -> NoReturn:
        if msg.header is None:
            raise APIError
        code = msg.header[api.Header.STATUS]
        if code == api.StatusCode.SERVICE_UNAVAILABLE:
            raise ServiceUnavailableError
        else:
            desc = msg.header[api.Header.DESCRIPTION]
            raise cls._status_error_class(code, desc)(code=int(code), description=desc)

    @staticmethod
    def _status_error_class(code: str, description: Optional[str]) -> Type[APIError]:
        """The error class of a pull status message, as nats.go's checkMsg tells them apart."""
        if code == api.StatusCode.PIN_ID_MISMATCH:
            return PinIdMismatchError
        if code == api.StatusCode.CONFLICT and description:
            desc = description.lower()
            for reason, err in _CONFLICT_ERRORS:
                if reason in desc:
                    return err
        return APIError

    @classmethod
    def from_error(cls, err: Dict[str, Any]):
        code = err["code"]
        base: Type[APIError]
        if code == 503:
            base = ServiceUnavailableError
        elif code == 500:
            base = ServerError
        elif code == 423:
            base = PinIdMismatchError
        elif code == 404:
            base = NotFoundError
        elif code == 400:
            base = BadRequestError
        else:
            base = APIError
        # Raise the error matching the JetStream error code when there is one
        # (nats.go maps err_code to its sentinel errors), as long as it is a
        # subclass of the error raised for the status code.
        typed = _API_ERRORS.get(err.get("err_code"))
        if typed is not None and issubclass(typed, base):
            raise typed(**err)
        raise base(**err)

    @property
    def api_error(self) -> Optional[APIError]:
        return self

    def __str__(self) -> str:
        return (
            f"nats: {type(self).__name__}: code={self.code} err_code={self.err_code} description='{self.description}'"
        )


class ServiceUnavailableError(APIError):
    """
    A 503 error
    """

    pass


class ServerError(APIError):
    """
    A 500 error
    """

    pass


class PinIdMismatchError(APIError):
    """
    A 423 error

    PinIdMismatchError is returned when Pin ID sent in the request does not match
    the currently pinned consumer subscriber ID on the server.
    """

    pass


class NotFoundError(APIError):
    """
    A 404 error
    """

    pass


class BadRequestError(APIError):
    """
    A 400 error.
    """

    pass


class ConsumerInvalidResetError(BadRequestError):
    """
    Raised when a consumer reset request violates the consumer's
    DeliverPolicy constraints (JetStream error code 10204).

    For example a non-zero ``seq`` below ``opt_start_seq`` on a
    ``by_start_sequence`` consumer.
    """

    pass


class StreamNotFoundError(NotFoundError):
    """
    Raised when a stream does not exist (err_code 10059).
    """

    pass


class ConsumerNotFoundError(NotFoundError):
    """
    Raised when a consumer does not exist (err_code 10014).
    """

    pass


class MsgNotFoundError(NotFoundError):
    """
    Raised when a stream message does not exist (err_code 10037).
    """

    pass


class MsgDeleteUnsuccessfulError(APIError):
    """
    Raised by a Stream handle's delete_msg / secure_delete_msg when the
    server did not delete the message (nats.go ErrMsgDeleteUnsuccessful).
    It carries the code, err_code and description of the server's API
    error, which is also its ``__cause__``.
    """

    @classmethod
    def from_api_error(cls, err: Optional[APIError]) -> MsgDeleteUnsuccessfulError:
        if err is None:
            return cls()
        return cls(
            code=err.code,
            description=err.description,
            err_code=err.err_code,
            stream=err.stream,
            seq=err.seq,
        )

    def __str__(self) -> str:
        if self.description:
            return f"nats: message deletion unsuccessful: {self.description}"
        return "nats: message deletion unsuccessful"


class StreamNameAlreadyInUseError(BadRequestError):
    """
    Raised when creating a stream whose name is already in use with a
    different configuration (err_code 10058).
    """

    pass


class StreamWrongLastSequenceError(BadRequestError):
    """
    Raised when an expected last sequence does not match the stream
    (err_code 10071 or 10164).
    """

    pass


class JetStreamBadRequestError(BadRequestError):
    """
    Raised when the server rejects a request as a bad request (err_code
    10003, nats.go ``ErrBadRequest``).
    """

    pass


class ConsumerCreateError(ServerError):
    """
    Raised when the server could not create a consumer (err_code 10012).
    """

    pass


class ConsumerNameAlreadyInUseError(BadRequestError):
    """
    Raised when a consumer name is already in use (err_code 10013).
    """

    pass


class ConsumerExistsError(BadRequestError):
    """
    Raised when creating a consumer that already exists with a different
    configuration (err_code 10148).
    """

    pass


class ConsumerDoesNotExistError(BadRequestError):
    """
    Raised when updating a consumer that does not exist (err_code 10149).
    """

    pass


class MaximumConsumersLimitError(BadRequestError):
    """
    Raised when the stream's or account's consumer limit is reached
    (err_code 10026).
    """

    pass


class DuplicateFilterSubjectsError(BadRequestError):
    """
    Raised when a consumer has both filter_subject and filter_subjects
    (err_code 10136).
    """

    pass


class OverlappingFilterSubjectsError(BadRequestError):
    """
    Raised when a consumer's filter subjects overlap (err_code 10138).
    """

    pass


class EmptyFilterError(BadRequestError):
    """
    Raised when one of a consumer's filter subjects is empty
    (err_code 10139).
    """

    pass


class JetStreamNotEnabledError(ServiceUnavailableError):
    """
    Raised when JetStream is not enabled on the server (err_code 10076),
    and when a JetStream API request has no responders.
    """

    pass


class JetStreamNotEnabledForAccountError(ServiceUnavailableError):
    """
    Raised when JetStream is not enabled for the account (err_code 10039).
    """

    pass


class MirrorWithMsgSchedulesError(BadRequestError):
    """
    Raised when a mirror stream enables message schedules (err_code 10186).
    """

    pass


class SourceWithMsgSchedulesError(BadRequestError):
    """
    Raised when a sourcing stream enables message schedules
    (err_code 10187).
    """

    pass


class MessageSchedulesDisabledError(BadRequestError):
    """
    Raised when a scheduled message is published to a stream without
    message schedules enabled (err_code 10188).
    """

    pass


class SchedulePatternInvalidError(BadRequestError):
    """
    Raised when a message schedule pattern is invalid (err_code 10189).
    """

    pass


class ScheduleTargetInvalidError(BadRequestError):
    """
    Raised when a message schedule target is invalid (err_code 10190).
    """

    pass


class ScheduleTTLInvalidError(BadRequestError):
    """
    Raised when a message schedule TTL is invalid (err_code 10191).
    """

    pass


class ScheduleRollupInvalidError(BadRequestError):
    """
    Raised when a message schedule rollup is invalid (err_code 10192).
    """

    pass


class ScheduleSourceInvalidError(BadRequestError):
    """
    Raised when a message schedule source is invalid (err_code 10203).
    """

    pass


class _InvalidValueError(Error, ValueError):
    """
    A client-side validation error. It is also a ValueError, which the
    client raised for these checks before it had dedicated errors.
    """

    _default = ""

    def __init__(self, description: Optional[str] = None) -> None:
        self.description = description

    def __str__(self) -> str:
        return self.description or self._default


class StreamNameRequiredError(_InvalidValueError):
    """
    Raised when a stream name is required but missing.
    """

    _default = "nats: stream name is required"


class InvalidStreamNameError(_InvalidValueError):
    """
    Raised when a stream name contains characters it cannot have.
    """

    _default = "nats: invalid stream name"


class InvalidConsumerNameError(_InvalidValueError):
    """
    Raised when a consumer name is missing or contains characters it cannot
    have.
    """

    _default = "nats: invalid consumer name"


class InvalidSubjectError(_InvalidValueError):
    """
    Raised when a subject is empty or invalid.
    """

    _default = "nats: invalid subject name"


class InvalidOptionError(_InvalidValueError):
    """
    Raised when JetStream options are invalid or conflict.
    """

    _default = "nats: invalid jetstream option"


class ConsumerCreationResponseEmptyError(Error):
    """
    Raised when the server replies to a consumer create request without the
    consumer's info.
    """

    def __str__(self) -> str:
        return "nats: consumer creation response is empty"


class ConsumerResetResponseEmptyError(Error):
    """
    Raised when the server replies to a consumer reset request without the
    consumer's info.
    """

    def __str__(self) -> str:
        return "nats: consumer reset response is empty"


class StreamSubjectTransformNotSupportedError(Error):
    """
    Raised when the server dropped a requested stream subject transform.
    """

    def __str__(self) -> str:
        return "nats: stream subject transformation not supported by nats-server"


class StreamSourceNotSupportedError(Error):
    """
    Raised when the server dropped the requested stream sources.
    """

    def __str__(self) -> str:
        return "nats: stream sourcing is not supported by nats-server"


class StreamSourceSubjectTransformNotSupportedError(StreamSubjectTransformNotSupportedError):
    """
    Raised when the server dropped a stream source's subject transforms.
    """

    pass


class StreamSourceMultipleFilterSubjectsNotSupportedError(Error):
    """
    Raised when the server does not support stream sources with multiple
    subject filters.
    """

    def __str__(self) -> str:
        return "nats: stream sourcing with multiple subject filters not supported by nats-server"


class ConsumerMultipleFilterSubjectsNotSupportedError(Error):
    """
    Raised when the server dropped a consumer's filter_subjects.
    """

    def __str__(self) -> str:
        return "nats: multiple consumer filter subjects not supported by nats-server"


class ConsumerHasActiveSubscriptionError(Error):
    """
    Raised when binding to a push consumer that already has an active
    subscription.
    """

    pass


class ConsumerAlreadyConsumingError(Error):
    """
    Raised when a consumer is already being consumed.
    """

    def __str__(self) -> str:
        return "nats: consumer is already consuming"


class ConsumerDeletedError(APIError):
    """
    Raised when the consumer was deleted while it was being consumed
    (a 409 "Consumer Deleted" status).
    """

    pass


class NotPullConsumerError(APIError):
    """
    Raised when a pull operation is used with a push consumer (a 409
    "Consumer is push based" status, or a push consumer's info).
    """

    pass


class NotPushConsumerError(Error):
    """
    Raised when a push operation is used with a pull consumer.
    """

    def __str__(self) -> str:
        return "nats: consumer is not a push consumer"


class HandlerRequiredError(Error):
    """
    Raised when a message handler is required but missing.
    """

    def __str__(self) -> str:
        return "nats: handler cannot be empty"


class EndOfDataError(Error):
    """
    Raised when a listing reached its end.
    """

    def __str__(self) -> str:
        return "nats: end of data reached"


class OrderConsumerUsedAsFetchError(Error):
    """
    Raised when an ordered consumer being consumed is used to fetch.
    """

    def __str__(self) -> str:
        return "nats: ordered consumer initialized as fetch"


class OrderConsumerUsedAsConsumeError(Error):
    """
    Raised when an ordered consumer being fetched from is used to consume.
    """

    def __str__(self) -> str:
        return "nats: ordered consumer initialized as consume"


class MaxBytesExceededError(APIError):
    """
    A 409 status: a message was larger than the max_bytes of the pull request.
    """

    pass


class BatchCompletedError(APIError):
    """
    A 409 status: the pull request was completed by the server.
    """

    pass


class ConsumerLeadershipChangedError(APIError):
    """
    A 409 status: the consumer leader changed while pulling from it.
    """

    pass


class ServerShutdownError(APIError):
    """
    A 409 status: the server is shutting down.
    """

    pass


# 409 descriptions (lower case) and their errors, checked in nats.go's order.
_CONFLICT_ERRORS = (
    ("message size exceeds maxbytes", MaxBytesExceededError),
    ("batch completed", BatchCompletedError),
    ("consumer deleted", ConsumerDeletedError),
    ("leadership change", ConsumerLeadershipChangedError),
    ("server shutdown", ServerShutdownError),
    ("consumer is push based", NotPullConsumerError),
)


class NoHeartbeatError(Error):
    """
    Raised when the idle heartbeats of a consumer stopped arriving.
    """

    def __str__(self) -> str:
        return "nats: no heartbeat received"


class MsgIteratorClosedError(Error):
    """
    Raised when getting the next message of a stopped messages iterator.
    """

    def __str__(self) -> str:
        return "nats: messages iterator closed"


class OrderedConsumerConcurrentRequestsError(Error):
    """
    Raised when an ordered consumer is consumed or fetched from concurrently.
    """

    def __str__(self) -> str:
        return "nats: cannot run concurrent processing using ordered consumer"


class OrderedConsumerNotCreatedError(Error):
    """
    Raised when asking the info of an ordered consumer without a current consumer.
    """

    def __str__(self) -> str:
        return "nats: consumer instance not yet created"


class OrderedConsumerResetError(Error):
    """
    Raised when an ordered consumer could not be recreated within its
    ``max_reset_attempts`` (nats.go ErrOrderedConsumerReset). The error of
    the last attempt is its ``__cause__`` and its description.
    """

    def __str__(self) -> str:
        if self.description:
            return f"nats: recreating ordered consumer: {self.description}"
        return "nats: recreating ordered consumer"

    @property
    def api_error(self) -> Optional[APIError]:
        cause = self.__cause__
        if isinstance(cause, Error):
            return cause.api_error
        return None


class NoStreamResponseError(Error):
    """
    Raised if the client gets a 503 when publishing a message.
    """

    def __str__(self) -> str:
        return "nats: no response from stream"


class InvalidJSAckError(Error, ValueError):
    """
    Raised when the response to a JetStream publish is not a valid acknowledgement.
    """

    def __str__(self) -> str:
        return "nats: invalid jetstream publish response"


class InvalidJetStreamResponseError(Error, ValueError):
    """
    Raised when the response to a JetStream API request cannot be decoded.
    """

    def __str__(self) -> str:
        return "nats: invalid jetstream api response"


class TooManyStalledMsgsError(Error):
    """
    Raised when too many outstanding async published messages are waiting for ack.
    """

    def __str__(self) -> str:
        return "nats: stalled with too many outstanding async published messages"


class AsyncPublishTimeoutError(Error):
    """
    Raised by an async publish future when its acknowledgement did not arrive in time.
    """

    def __str__(self) -> str:
        return "nats: timeout waiting for ack"


class JetStreamPublisherClosedError(Error):
    """
    Raised by the pending async publish futures when the publisher is cleaned up.
    """

    def __str__(self) -> str:
        return "nats: jetstream context closed"


class AsyncPublishReplySubjectSetError(Error):
    """
    Raised when an async published message has a reply subject.
    """

    def __str__(self) -> str:
        return "nats: reply subject should be empty"


class FetchTimeoutError(nats.errors.TimeoutError):
    """
    Raised if the consumer timed out waiting for messages.
    """

    def __str__(self) -> str:
        return "nats: fetch timeout"


class NoMessagesError(FetchTimeoutError):
    """
    Raised when no messages were received before the request expired.
    """

    def __str__(self) -> str:
        return "nats: no messages"


class ConsumerSequenceMismatchError(Error):
    """
    Async error raised by the client with idle_heartbeat mode enabled
    when one of the message sequences is not the expected one.
    """

    def __init__(
        self,
        stream_resume_sequence=None,
        consumer_sequence=None,
        last_consumer_sequence=None,
    ) -> None:
        self.stream_resume_sequence = stream_resume_sequence
        self.consumer_sequence = consumer_sequence
        self.last_consumer_sequence = last_consumer_sequence

    def __str__(self) -> str:
        gap = self.last_consumer_sequence - self.consumer_sequence
        return (
            f"nats: sequence mismatch for consumer at sequence {self.consumer_sequence} "
            f"({gap} sequences behind), should restart consumer from stream sequence {self.stream_resume_sequence}"
        )


class BucketNotFoundError(NotFoundError):
    """
    When attempted to bind to a JetStream KeyValue that does not exist.
    """

    pass


class BadBucketError(APIError):
    pass


class BucketExistsError(BadRequestError):
    """
    Raised when creating a KeyValue store whose bucket already exists
    with a different configuration (nats.go ErrBucketExists).

    It keeps the server's stream-name-in-use API error details, so it is
    still the BadRequestError that was raised before.
    """

    def __init__(
        self,
        bucket: Optional[str] = None,
        code: Optional[int] = None,
        description: Optional[str] = None,
        err_code: Optional[int] = None,
        stream: Optional[str] = None,
        seq: Optional[int] = None,
    ) -> None:
        super().__init__(
            code=code,
            description=description,
            err_code=err_code,
            stream=stream,
            seq=seq,
        )
        self.bucket = bucket

    def __str__(self) -> str:
        s = "nats: bucket name already in use"
        if self.bucket:
            s += f": {self.bucket}"
        if self.description:
            s += f": {self.description}"
        return s


class KeyValueError(APIError):
    """
    Raised when there is an issue interacting with the KeyValue store.
    """

    pass


class KeyDeletedError(KeyValueError, NotFoundError):
    """
    Raised when trying to get a key that was deleted from a JetStream KeyValue store.
    """

    def __init__(self, entry=None, op=None) -> None:
        self.entry = entry
        self.op = op

    def __str__(self) -> str:
        return "nats: key was deleted"


class KeyNotFoundError(KeyValueError, NotFoundError):
    """
    Raised when trying to get a key that does not exists from a JetStream KeyValue store.
    """

    def __init__(self, entry=None, op=None, message=None) -> None:
        self.entry = entry
        self.op = op
        self.message = message

    def __str__(self) -> str:
        s = "nats: key not found"
        if self.message:
            s += f": {self.message}"
        return s


class KeyWrongLastSequenceError(KeyValueError, BadRequestError):
    """
    Raised when trying to update a key with the wrong last sequence.
    """

    def __init__(self, description=None) -> None:
        self.description = description

    def __str__(self) -> str:
        return f"nats: {self.description}"


class NoKeysError(KeyValueError):
    def __str__(self) -> str:
        return "nats: no keys found"


class KeyHistoryTooLargeError(KeyValueError):
    def __str__(self) -> str:
        return "nats: history limited to a max of 64"


class KeyHistoryNotFoundError(NoKeysError, KeyNotFoundError):
    """
    Raised by KeyValue.history() when the key has no revisions.

    nats.go returns ErrKeyNotFound for it, so it is a KeyNotFoundError;
    it is also the NoKeysError that history() raised before.
    """

    def __str__(self) -> str:
        return "nats: key not found"


class KeyRevisionMismatchError(KeyWrongLastSequenceError):
    """
    Raised when an update, delete or purge expected a revision that is not
    the latest revision of the key (nats.go ErrKeyRevisionMismatch).

    It is a KeyWrongLastSequenceError, so existing handlers keep working,
    and keeps the server's API error details when it has them.
    """

    def __init__(
        self,
        description: Optional[str] = None,
        code: Optional[int] = None,
        err_code: Optional[int] = None,
        stream: Optional[str] = None,
        seq: Optional[int] = None,
    ) -> None:
        APIError.__init__(
            self,
            code=code,
            description=description,
            err_code=err_code,
            stream=stream,
            seq=seq,
        )

    def __str__(self) -> str:
        s = "nats: key revision mismatch"
        if self.description:
            s += f": {self.description}"
        return s


class KeyValueConfigRequiredError(Error):
    """
    Raised when creating a KeyValue store without a configuration
    (nats.go ErrKeyValueConfigRequired).
    """

    def __str__(self) -> str:
        return "nats: config required"


class InvalidKeyError(Error):
    """
    Raised when trying to put an object in Key Value with an invalid key.
    """

    pass


class InvalidBucketNameError(Error):
    """
    Raised when trying to create a KV or OBJ bucket with invalid name.
    """

    pass


class InvalidObjectNameError(Error):
    """
    Raised when trying to put an object in Object Store with invalid key.
    """

    pass


class BadObjectMetaError(Error):
    """
    Raised when trying to read corrupted metadata from Object Store.
    """

    pass


class LinkIsABucketError(Error):
    """
    Raised when trying to get object from Object Store that is a bucket.
    """

    pass


class DigestMismatchError(Error):
    """
    Raised when getting an object from Object Store that has a different digest than expected.
    """

    pass


class ObjectNotFoundError(NotFoundError):
    """
    When attempted to lookup an Object that does not exist.
    """

    pass


class ObjectDeletedError(NotFoundError):
    """
    When attempted to do an operation to an Object that does not exist.
    """

    pass


class ObjectAlreadyExists(Error):
    """
    When attempted to do an operation to an Object that already exist.
    """

    pass


class InvalidDigestFormatError(Error):
    """
    Raised when an object digest is not of the form ``SHA-256=<base64url>``.
    """

    def __str__(self) -> str:
        return "nats: object digest hash has invalid format"


class InvalidStoreNameError(InvalidBucketNameError):
    """
    Raised when an Object Store bucket name is not valid.
    """

    def __str__(self) -> str:
        return "nats: invalid object-store name"


class ObjectNameRequiredError(InvalidObjectNameError, ObjectNotFoundError):
    """
    Raised when an Object Store operation is given an empty object name.

    It is an ObjectNotFoundError too, which is what looking up an empty
    name used to raise.
    """

    def __str__(self) -> str:
        return "nats: name is required"


class NoObjectsFoundError(NotFoundError):
    """
    Raised when listing an Object Store that has no objects.
    """

    def __str__(self) -> str:
        return "nats: no objects found"


class UpdateMetaDeletedError(ObjectDeletedError):
    """
    Raised when updating the meta of an object that is missing or deleted.
    """

    def __str__(self) -> str:
        return "nats: cannot update meta for a deleted object"


class LinkNotAllowedError(Error):
    """
    Raised when putting an object whose meta options carry a link.
    """

    def __str__(self) -> str:
        return "nats: link cannot be set when putting the object in bucket"


class ObjectRequiredError(Error):
    """
    Raised when adding a link without the info of the object to link to.
    """

    def __str__(self) -> str:
        return "nats: object required"


class NoLinkToDeletedError(Error):
    """
    Raised when adding a link to a deleted object.
    """

    def __str__(self) -> str:
        return "nats: not allowed to link to a deleted object"


class NoLinkToLinkError(Error):
    """
    Raised when adding a link to an object that is itself a link.
    """

    def __str__(self) -> str:
        return "nats: not allowed to link to another link"


class BucketRequiredError(Error):
    """
    Raised when adding a bucket link without the bucket to link to.
    """

    def __str__(self) -> str:
        return "nats: bucket required"


class BucketMalformedError(Error):
    """
    Raised when adding a bucket link to something that is not an ObjectStore.
    """

    def __str__(self) -> str:
        return "nats: bucket malformed"


class ObjectConfigRequiredError(Error):
    """
    Raised when creating or updating an Object Store without a config.
    """

    def __str__(self) -> str:
        return "nats: object-store config required"


class KeyValueLimitMarkerTTLNotSupportedError(Error):
    """
    Raised when limit_marker_ttl is used but the connected server does not support it (pre-2.11).
    """

    def __str__(self):
        return "nats: limit marker TTLs not supported by server"


# The JetStream API errors raised for an err_code (see APIError.from_error).
_API_ERRORS: Dict[Optional[int], Type[APIError]] = {
    ErrorCode.BAD_REQUEST: JetStreamBadRequestError,
    ErrorCode.CONSUMER_CREATE: ConsumerCreateError,
    ErrorCode.CONSUMER_NAME_EXISTS: ConsumerNameAlreadyInUseError,
    ErrorCode.CONSUMER_NOT_FOUND: ConsumerNotFoundError,
    ErrorCode.MAXIMUM_CONSUMERS_LIMIT: MaximumConsumersLimitError,
    ErrorCode.MESSAGE_NOT_FOUND: MsgNotFoundError,
    ErrorCode.JETSTREAM_NOT_ENABLED_FOR_ACCOUNT: JetStreamNotEnabledForAccountError,
    ErrorCode.STREAM_NAME_IN_USE: StreamNameAlreadyInUseError,
    ErrorCode.STREAM_NOT_FOUND: StreamNotFoundError,
    ErrorCode.STREAM_WRONG_LAST_SEQUENCE: StreamWrongLastSequenceError,
    ErrorCode.JETSTREAM_NOT_ENABLED: JetStreamNotEnabledError,
    ErrorCode.DUPLICATE_FILTER_SUBJECTS: DuplicateFilterSubjectsError,
    ErrorCode.OVERLAPPING_FILTER_SUBJECTS: OverlappingFilterSubjectsError,
    ErrorCode.CONSUMER_EMPTY_FILTER: EmptyFilterError,
    ErrorCode.CONSUMER_EXISTS: ConsumerExistsError,
    ErrorCode.CONSUMER_DOES_NOT_EXIST: ConsumerDoesNotExistError,
    ErrorCode.STREAM_WRONG_LAST_SEQUENCE_CONSTANT: StreamWrongLastSequenceError,
    ErrorCode.MIRROR_WITH_MSG_SCHEDULES: MirrorWithMsgSchedulesError,
    ErrorCode.SOURCE_WITH_MSG_SCHEDULES: SourceWithMsgSchedulesError,
    ErrorCode.MESSAGE_SCHEDULES_DISABLED: MessageSchedulesDisabledError,
    ErrorCode.SCHEDULE_PATTERN_INVALID: SchedulePatternInvalidError,
    ErrorCode.SCHEDULE_TARGET_INVALID: ScheduleTargetInvalidError,
    ErrorCode.SCHEDULE_TTL_INVALID: ScheduleTTLInvalidError,
    ErrorCode.SCHEDULE_ROLLUP_INVALID: ScheduleRollupInvalidError,
    ErrorCode.SCHEDULE_SOURCE_INVALID: ScheduleSourceInvalidError,
    ErrorCode.CONSUMER_INVALID_RESET: ConsumerInvalidResetError,
}
