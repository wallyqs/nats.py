# Copyright 2021 The NATS Authors
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

import base64
import copy
import json
from email.parser import BytesParser
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, Iterable, List, Optional

from nats.errors import NoRespondersError
from nats.js import api
from nats.js.errors import (
    APIError,
    ConsumerCreationResponseEmptyError,
    ConsumerInvalidResetError,
    ConsumerMultipleFilterSubjectsNotSupportedError,
    ConsumerResetResponseEmptyError,
    Error,
    ErrorCode,
    InvalidConsumerNameError,
    InvalidJetStreamResponseError,
    InvalidOptionError,
    InvalidStreamNameError,
    InvalidSubjectError,
    JetStreamNotEnabledError,
    NotFoundError,
    NotPullConsumerError,
    NotPushConsumerError,
    StreamNameRequiredError,
    StreamNotFoundError,
    StreamSourceNotSupportedError,
    StreamSourceSubjectTransformNotSupportedError,
    StreamSubjectTransformNotSupportedError,
)

if TYPE_CHECKING:
    from nats import NATS
    from nats.aio.msg import Msg

NATS_HDR_LINE = bytearray(b"NATS/1.0")
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
_CRLF_ = b"\r\n"
_CRLF_LEN_ = len(_CRLF_)

# Characters the server cannot use as part of a filesystem path for stream or
# consumer storage. Matches nats.go's validateStreamName / validateConsumerName.
_INVALID_NAME_CHARS = ">*. /\\\t\r\n"


def _validate_stream_name(name: Optional[str]) -> None:
    if not name:
        raise StreamNameRequiredError()
    if any(c in _INVALID_NAME_CHARS for c in name):
        raise InvalidStreamNameError(f"nats: invalid stream name: {name!r}")


def _validate_consumer_name(name: Optional[str]) -> None:
    if not name:
        raise InvalidConsumerNameError("nats: consumer name is required")
    if any(c in _INVALID_NAME_CHARS for c in name):
        raise InvalidConsumerNameError(f"nats: invalid consumer name: {name!r}")


def _validate_subject(subject: Optional[str]) -> None:
    """
    Validates a subject as nats.go's validateSubject does.
    """
    if not subject:
        raise InvalidSubjectError("nats: invalid subject name: subject cannot be empty")
    if subject.startswith(".") or subject.endswith(".") or " " in subject or ">" in subject[:-1]:
        raise InvalidSubjectError(f"nats: invalid subject name: {subject}")


def _stream_not_found() -> StreamNotFoundError:
    return StreamNotFoundError(code=404, err_code=ErrorCode.STREAM_NOT_FOUND, description="stream not found")


def _check_stream_support(config: api.StreamConfig, info: api.StreamInfo) -> None:
    """
    Checks that the server kept the subject transform and sources of a
    created or updated stream, as nats.go does: servers that do not support
    them drop them from the stream's config.
    """
    if config.subject_transform is not None and info.config.subject_transform is None:
        raise StreamSubjectTransformNotSupportedError
    if not config.sources:
        return
    returned = info.config.sources or []
    if len(config.sources) != len(returned):
        raise StreamSourceNotSupportedError

    # The server may return the sources in another order.
    def counts(sources: List[Any]) -> List[int]:
        def transforms(src: Any) -> Any:
            if isinstance(src, dict):
                return src.get("subject_transforms")
            return getattr(src, "subject_transforms", None)

        return sorted(len(transforms(src) or []) for src in sources)

    if counts(config.sources) != counts(returned):
        raise StreamSourceSubjectTransformNotSupportedError


def _check_consumer_support(config: api.ConsumerConfig, info: api.ConsumerInfo) -> None:
    """
    Checks that the server kept a consumer's filter_subjects, which servers
    before v2.10 drop.
    """
    if config.filter_subjects and not info.config.filter_subjects:
        raise ConsumerMultipleFilterSubjectsNotSupportedError


def _has_consumer_info(resp: Dict[str, Any]) -> bool:
    return "name" in resp or "config" in resp


class JetStreamManager:
    """
    JetStreamManager exposes management APIs for JetStream.
    """

    def __init__(
        self,
        conn: NATS,
        prefix: str = api.DEFAULT_PREFIX,
        timeout: float = 5,
        client_trace: Optional[api.ClientTrace] = None,
    ) -> None:
        self._prefix = prefix
        self._nc = conn
        self._timeout = timeout
        self._hdr_parser = BytesParser()
        self._client_trace = client_trace

    @property
    def conn(self) -> NATS:
        """
        The NATS connection used by the context.
        """
        return self._nc

    @property
    def options(self) -> api.JetStreamOptions:
        """
        The options the context was created with.
        """
        return api.JetStreamOptions(
            api_prefix=self._prefix,
            default_timeout=self._timeout,
            client_trace=self._client_trace,
        )

    async def account_info(self) -> api.AccountInfo:
        resp = await self._api_request(f"{self._prefix}.INFO", b"", timeout=self._timeout)
        return api.AccountInfo.from_response(resp)

    async def find_stream_name_by_subject(self, subject: str) -> str:
        """
        Find the stream to which a subject belongs in an account.
        """

        req_sub = f"{self._prefix}.STREAM.NAMES"
        req_data = json.dumps({"subject": subject})
        info = await self._api_request(req_sub, req_data.encode(), timeout=self._timeout)
        if not info["streams"]:
            raise _stream_not_found()
        return info["streams"][0]

    async def stream_name_by_subject(self, subject: str) -> str:
        """
        Returns the name of the stream that holds the subject.

        :raises InvalidSubjectError: if the subject is not a valid subject.
        :raises StreamNotFoundError: if no stream holds the subject.
        """
        _validate_subject(subject)
        return await self.find_stream_name_by_subject(subject)

    async def stream(self, name: str) -> Stream:
        """
        Returns a handle on a stream, with its info cached
        (nats.go StreamManager.Stream).

        :raises StreamNotFoundError: if the stream does not exist.
        """
        info = await self.stream_info(name)
        return Stream(self, name, info)

    async def stream_info(
        self,
        name: str,
        subjects_filter: Optional[str] = None,
        deleted_details: Optional[bool] = None,
    ) -> api.StreamInfo:
        """
        Get the latest StreamInfo by stream name.

        :param name: The name of the stream.
        :param subjects_filter: Report the message count of the subjects
            matching this filter in ``state.subjects``. The subjects are
            fetched page by page when the server cannot send them all at once.
        :param deleted_details: Report the deleted sequences in
            ``state.deleted``.
        """
        _validate_stream_name(name)
        req: Dict[str, Any] = {}
        if subjects_filter:
            req["subjects_filter"] = subjects_filter
        if deleted_details:
            req["deleted_details"] = True

        subjects: Optional[Dict[str, int]] = None
        while True:
            resp = await self._api_request(
                f"{self._prefix}.STREAM.INFO.{name}",
                json.dumps(req).encode() if req else b"",
                timeout=self._timeout,
            )
            total = resp.get("total") or 0
            page = (resp.get("state") or {}).get("subjects") or {}
            if not subjects_filter or (subjects is None and len(page) >= total):
                break
            # The server limits the subjects it sends in one response.
            subjects = subjects or {}
            subjects.update(page)
            if not page or len(subjects) >= total:
                resp["state"]["subjects"] = subjects
                break
            req["offset"] = len(subjects)
        return api.StreamInfo.from_response(resp)

    async def add_stream(self, config: Optional[api.StreamConfig] = None, **params) -> api.StreamInfo:
        """
        add_stream creates a stream.
        """
        if config is None:
            config = api.StreamConfig()
        config = config.evolve(**params)

        stream_name = config.name
        _validate_stream_name(stream_name)

        data = json.dumps(config.as_dict())
        resp = await self._api_request(
            f"{self._prefix}.STREAM.CREATE.{stream_name}",
            data.encode(),
            timeout=self._timeout,
        )
        info = api.StreamInfo.from_response(resp)
        _check_stream_support(config, info)
        return info

    async def update_stream(self, config: Optional[api.StreamConfig] = None, **params) -> api.StreamInfo:
        """
        update_stream updates a stream.
        """
        if config is None:
            config = api.StreamConfig()
        config = config.evolve(**params)
        _validate_stream_name(config.name)

        data = json.dumps(config.as_dict())
        resp = await self._api_request(
            f"{self._prefix}.STREAM.UPDATE.{config.name}",
            data.encode(),
            timeout=self._timeout,
        )
        info = api.StreamInfo.from_response(resp)
        _check_stream_support(config, info)
        return info

    async def delete_stream(self, name: str) -> bool:
        """
        Delete a stream by name.

        :param name: The name of the stream to delete.
        """
        _validate_stream_name(name)
        resp = await self._api_request(f"{self._prefix}.STREAM.DELETE.{name}", timeout=self._timeout)
        return resp["success"]

    async def purge_stream(
        self,
        name: str,
        seq: Optional[int] = None,
        subject: Optional[str] = None,
        keep: Optional[int] = None,
    ) -> bool:
        """
        Purge a stream by name.
        """
        _validate_stream_name(name)
        stream_req: Dict[str, Any] = {}
        if seq:
            stream_req["seq"] = seq
        if subject:
            stream_req["filter"] = subject
        if keep:
            stream_req["keep"] = keep

        req = json.dumps(stream_req)
        resp = await self._api_request(f"{self._prefix}.STREAM.PURGE.{name}", req.encode(), timeout=self._timeout)
        return resp["success"]

    async def consumer_info(self, stream: str, consumer: str, timeout: Optional[float] = None):
        _validate_stream_name(stream)
        _validate_consumer_name(consumer)
        if timeout is None:
            timeout = self._timeout
        resp = await self._api_request(f"{self._prefix}.CONSUMER.INFO.{stream}.{consumer}", b"", timeout=timeout)
        return api.ConsumerInfo.from_response(resp)

    async def streams_info(self, offset=0) -> List[api.StreamInfo]:
        """
        streams_info retrieves a list of streams with an optional offset.
        """
        resp = await self._api_request(
            f"{self._prefix}.STREAM.LIST",
            json.dumps({"offset": offset}).encode(),
            timeout=self._timeout,
        )
        streams = []
        for stream in resp["streams"]:
            stream_info = api.StreamInfo.from_response(stream)
            streams.append(stream_info)
        return streams

    async def list_streams(self, subject: Optional[str] = None) -> AsyncIterator[api.StreamInfo]:
        """
        Iterates over the infos of all the streams, fetched page by page.

        :param subject: Only list the streams that hold this subject.

        ::

            async for info in js.list_streams():
                print(info.config.name)
        """
        req: Dict[str, Any] = {}
        if subject:
            req["subject"] = subject
        offset = 0
        while True:
            req["offset"] = offset
            resp = await self._api_request(
                f"{self._prefix}.STREAM.LIST",
                json.dumps(req).encode(),
                timeout=self._timeout,
            )
            page = resp.get("streams") or []
            for stream in page:
                yield api.StreamInfo.from_response(stream)
            offset += len(page)
            if not page or offset >= resp.get("total", 0):
                return

    async def stream_names(self, subject: Optional[str] = None) -> AsyncIterator[str]:
        """
        Iterates over the names of all the streams, fetched page by page.

        :param subject: Only list the streams that hold this subject.
        """
        req: Dict[str, Any] = {}
        if subject:
            req["subject"] = subject
        offset = 0
        while True:
            req["offset"] = offset
            resp = await self._api_request(
                f"{self._prefix}.STREAM.NAMES",
                json.dumps(req).encode(),
                timeout=self._timeout,
            )
            page = resp.get("streams") or []
            for name in page:
                yield name
            offset += len(page)
            if not page or offset >= resp.get("total", 0):
                return

    async def create_or_update_stream(self, config: Optional[api.StreamConfig] = None, **params) -> api.StreamInfo:
        """
        Updates a stream, or creates it when it does not exist.
        """
        if config is None:
            config = api.StreamConfig()
        config = config.evolve(**params)
        try:
            return await self.update_stream(config)
        except StreamNotFoundError:
            return await self.add_stream(config)

    async def streams_info_iterator(self, offset=0) -> Iterable[api.StreamInfo]:
        """
        streams_info retrieves a list of streams Iterator.
        """
        resp = await self._api_request(
            f"{self._prefix}.STREAM.LIST",
            json.dumps({"offset": offset}).encode(),
            timeout=self._timeout,
        )

        return api.StreamsListIterator(resp["offset"], resp["total"], resp["streams"])

    async def add_consumer(
        self,
        stream: str,
        config: Optional[api.ConsumerConfig] = None,
        timeout: Optional[float] = None,
        **params,
    ) -> api.ConsumerInfo:
        _validate_stream_name(stream)
        if not timeout:
            timeout = self._timeout
        if config is None:
            config = api.ConsumerConfig()
        config = config.evolve(**params)
        durable_name = config.durable_name
        if config.name:
            _validate_consumer_name(config.name)
        if durable_name:
            _validate_consumer_name(durable_name)
        req = {"stream_name": stream, "config": config.as_dict()}
        req_data = json.dumps(req).encode()

        resp = None
        subject = ""
        version = self._nc.connected_server_version
        consumer_name_supported = version.major >= 2 and version.minor >= 9
        if consumer_name_supported and config.name:
            # NOTE: Only supported after nats-server v2.9.0
            if config.filter_subject and config.filter_subject != ">":
                subject = f"{self._prefix}.CONSUMER.CREATE.{stream}.{config.name}.{config.filter_subject}"
            else:
                subject = f"{self._prefix}.CONSUMER.CREATE.{stream}.{config.name}"
        elif durable_name:
            # NOTE: Legacy approach to create consumers. After nats-server v2.9
            # name option can be used instead.
            subject = f"{self._prefix}.CONSUMER.DURABLE.CREATE.{stream}.{durable_name}"
        else:
            subject = f"{self._prefix}.CONSUMER.CREATE.{stream}"

        resp = await self._api_request(subject, req_data, timeout=timeout)
        if not _has_consumer_info(resp):
            raise ConsumerCreationResponseEmptyError
        info = api.ConsumerInfo.from_response(resp)
        _check_consumer_support(config, info)
        return info

    async def create_consumer(
        self,
        stream: str,
        config: Optional[api.ConsumerConfig] = None,
        timeout: Optional[float] = None,
        **params,
    ) -> api.ConsumerInfo:
        """
        Creates a consumer. Creating a consumer that exists with the same
        configuration returns its info; with another configuration it raises
        ConsumerExistsError. A consumer without a name or durable name gets a
        generated name. Requires nats-server 2.10.0 or later.
        """
        return await self._upsert_consumer(stream, config, timeout, params, "create")

    async def update_consumer(
        self,
        stream: str,
        config: Optional[api.ConsumerConfig] = None,
        timeout: Optional[float] = None,
        **params,
    ) -> api.ConsumerInfo:
        """
        Updates an existing consumer; ConsumerDoesNotExistError if there is
        none. Requires nats-server 2.10.0 or later.
        """
        return await self._upsert_consumer(stream, config, timeout, params, "update")

    async def create_or_update_consumer(
        self,
        stream: str,
        config: Optional[api.ConsumerConfig] = None,
        timeout: Optional[float] = None,
        **params,
    ) -> api.ConsumerInfo:
        """
        Creates a consumer, or updates it if it exists. A consumer without a
        name or durable name gets a generated name.
        """
        return await self._upsert_consumer(stream, config, timeout, params, "")

    async def _upsert_consumer(
        self,
        stream: str,
        config: Optional[api.ConsumerConfig],
        timeout: Optional[float],
        params: Dict[str, Any],
        action: str,
    ) -> api.ConsumerInfo:
        """
        Sends a CONSUMER.CREATE request with an action, as nats.go's
        upsertConsumer does: "create" fails if the consumer exists with
        another configuration, "update" if it does not exist, and "" creates
        or updates it.
        """
        _validate_stream_name(stream)
        if not timeout:
            timeout = self._timeout
        if config is None:
            config = api.ConsumerConfig()
        config = config.evolve(**params)
        if config.name:
            _validate_consumer_name(config.name)
        if config.durable_name:
            _validate_consumer_name(config.durable_name)
        name = config.name or config.durable_name
        if not name:
            name = self._nc._nuid.next().decode()

        req: Dict[str, Any] = {"stream_name": stream, "config": config.as_dict()}
        if action:
            req["action"] = action
        if config.filter_subject and config.filter_subject != ">" and not config.filter_subjects:
            subject = f"{self._prefix}.CONSUMER.CREATE.{stream}.{name}.{config.filter_subject}"
        else:
            subject = f"{self._prefix}.CONSUMER.CREATE.{stream}.{name}"

        resp = await self._api_request(subject, json.dumps(req).encode(), timeout=timeout)
        if not _has_consumer_info(resp):
            raise ConsumerCreationResponseEmptyError
        info = api.ConsumerInfo.from_response(resp)
        _check_consumer_support(config, info)
        return info

    async def consumer_names(self, stream: str) -> AsyncIterator[str]:
        """
        Iterates over the names of the stream's consumers, fetched page by
        page.
        """
        _validate_stream_name(stream)
        offset = 0
        while True:
            resp = await self._api_request(
                f"{self._prefix}.CONSUMER.NAMES.{stream}",
                json.dumps({"offset": offset}).encode(),
                timeout=self._timeout,
            )
            page = resp.get("consumers") or []
            for name in page:
                yield name
            offset += len(page)
            if not page or offset >= resp.get("total", 0):
                return

    async def list_consumers(self, stream: str) -> AsyncIterator[api.ConsumerInfo]:
        """
        Iterates over the infos of the stream's consumers, fetched page by
        page.
        """
        _validate_stream_name(stream)
        offset = 0
        while True:
            resp = await self._api_request(
                f"{self._prefix}.CONSUMER.LIST.{stream}",
                json.dumps({"offset": offset}).encode(),
                timeout=self._timeout,
            )
            page = resp.get("consumers") or []
            for consumer in page:
                yield api.ConsumerInfo.from_response(consumer)
            offset += len(page)
            if not page or offset >= resp.get("total", 0):
                return

    async def delete_consumer(self, stream: str, consumer: str) -> bool:
        """
        Delete a consumer from a given stream.

        :param stream: The name of the stream from which the consumer should be deleted.
        :param consumer: The name of the consumer to be deleted.
        """
        _validate_stream_name(stream)
        _validate_consumer_name(consumer)
        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.DELETE.{stream}.{consumer}",
            b"",
            timeout=self._timeout,
        )
        return resp["success"]

    async def pause_consumer(
        self,
        stream: str,
        consumer: str,
        pause_until: str,
        timeout: Optional[float] = None,
    ) -> api.ConsumerPause:
        """
        Pause a consumer until the specified time.

        Args:
            stream: The stream name
            consumer: The consumer name
            pause_until: RFC 3339 timestamp string (e.g., "2025-10-22T12:00:00Z")
                        until which the consumer should be paused
            timeout: Request timeout in seconds

        Returns:
            ConsumerPause with paused status

        Note:
            Requires nats-server 2.11.0 or later
        """
        _validate_stream_name(stream)
        _validate_consumer_name(consumer)
        if timeout is None:
            timeout = self._timeout

        req = {"pause_until": pause_until}
        req_data = json.dumps(req).encode()

        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.PAUSE.{stream}.{consumer}",
            req_data,
            timeout=timeout,
        )
        return api.ConsumerPause.from_response(resp)

    async def resume_consumer(
        self,
        stream: str,
        consumer: str,
        timeout: Optional[float] = None,
    ) -> api.ConsumerPause:
        """
        Resume a paused consumer immediately.

        This is equivalent to calling pause_consumer with a timestamp in the past.

        Args:
            stream: The stream name
            consumer: The consumer name
            timeout: Request timeout in seconds

        Returns:
            ConsumerPause with paused=False

        Note:
            Requires nats-server 2.11.0 or later
        """
        # Resume by pausing until a time in the past (epoch)
        return await self.pause_consumer(stream, consumer, "1970-01-01T00:00:00Z", timeout)

    async def reset_consumer(
        self,
        stream: str,
        consumer: str,
        seq: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> api.ConsumerReset:
        """
        Reset a consumer's delivery state (ADR-60).

        Pending and redelivered counts are cleared and the consumer's delivery
        sequence restarts at 1. If ``seq`` is provided and non-zero, the ack
        floor stream sequence is set to one below it so the next delivered
        message has a stream sequence ``>= seq``; otherwise the ack floor
        stream sequence is left where it was and redelivery resumes from one
        above it.

        Resetting to a specific sequence is only allowed on consumers with
        DeliverPolicy of ``all``, ``by_start_sequence``, or ``by_start_time``;
        for the bounded policies the server rejects resets below the
        configured starting sequence/time.

        Args:
            stream: The stream name.
            consumer: The consumer name.
            seq: Optional stream sequence the consumer should be reset to.
                ``None`` and ``0`` are equivalent: both resume from one above
                the consumer's ack floor.
            timeout: Request timeout in seconds.

        Returns:
            ConsumerReset carrying the refreshed ConsumerInfo and the stream
            sequence the next delivered message will be at or above.

        Raises:
            ConsumerInvalidResetError: If the requested reset violates the
                consumer's DeliverPolicy (for example, ``seq`` is below
                ``opt_start_seq`` on a bounded policy).

        Note:
            Requires nats-server 2.14.0 or later.
        """
        _validate_stream_name(stream)
        _validate_consumer_name(consumer)
        if timeout is None:
            timeout = self._timeout

        req: bytes = b""
        if seq:
            req = json.dumps({"seq": seq}).encode()

        try:
            resp = await self._api_request(
                f"{self._prefix}.CONSUMER.RESET.{stream}.{consumer}",
                req,
                timeout=timeout,
            )
        except APIError as err:
            # 10204: JSConsumerInvalidResetErr
            if err.err_code == 10204:
                raise ConsumerInvalidResetError(
                    code=err.code,
                    description=err.description,
                    err_code=err.err_code,
                ) from err
            raise

        if not _has_consumer_info(resp):
            raise ConsumerResetResponseEmptyError
        return api.ConsumerReset.from_response(resp)

    async def consumers_info(self, stream: str, offset: Optional[int] = None) -> List[api.ConsumerInfo]:
        """
        consumers_info retrieves a list of consumers. Consumers list limit is 256 for more
        consider to use offset

        :param stream: stream to get consumers
        :param offset: consumers list offset
        """
        _validate_stream_name(stream)
        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.LIST.{stream}",
            b"" if offset is None else json.dumps({"offset": offset}).encode(),
            timeout=self._timeout,
        )
        consumers = []
        for consumer in resp["consumers"]:
            consumer_info = api.ConsumerInfo.from_response(consumer)
            consumers.append(consumer_info)
        return consumers

    async def get_msg(
        self,
        stream_name: str,
        seq: Optional[int] = None,
        subject: Optional[str] = None,
        direct: Optional[bool] = False,
        next: Optional[bool] = False,
    ) -> api.RawStreamMsg:
        """
        get_msg retrieves a message from a stream.
        """
        _validate_stream_name(stream_name)
        req_subject = None
        req: Dict[str, Any] = {}
        if seq:
            req["seq"] = seq
        if subject:
            req["seq"] = None
            req.pop("seq", None)
            req["last_by_subj"] = subject
        if next:
            req["seq"] = seq
            req["last_by_subj"] = None
            req.pop("last_by_subj", None)
            req["next_by_subj"] = subject
        data = json.dumps(req)

        if direct:
            # $JS.API.DIRECT.GET.KV_{stream_name}.$KV.TEST.{key}
            if subject and (seq is None):
                # last_by_subject type request requires no payload.
                data = ""
                req_subject = f"{self._prefix}.DIRECT.GET.{stream_name}.{subject}"
            else:
                req_subject = f"{self._prefix}.DIRECT.GET.{stream_name}"

            resp = await self._request(req_subject, data.encode(), timeout=self._timeout)
            raw_msg = JetStreamManager._lift_msg_to_raw_msg(resp)
            return raw_msg

        # Non Direct form
        req_subject = f"{self._prefix}.STREAM.MSG.GET.{stream_name}"
        resp_data = await self._api_request(req_subject, data.encode(), timeout=self._timeout)

        raw_msg = api.RawStreamMsg.from_response(resp_data["message"])
        if raw_msg.hdrs:
            hdrs = base64.b64decode(raw_msg.hdrs)
            raw_headers = hdrs[NATS_HDR_LINE_SIZE + _CRLF_LEN_ :]
            parsed_headers = self._hdr_parser.parsebytes(raw_headers)
            headers = None
            if len(parsed_headers.items()) > 0:
                headers = {}
                for k, v in parsed_headers.items():
                    headers[k] = v
            raw_msg.headers = headers

        msg_data: Optional[bytes] = None
        if raw_msg.data:
            msg_data = base64.b64decode(raw_msg.data)
        raw_msg.data = msg_data

        return raw_msg

    @classmethod
    def _lift_msg_to_raw_msg(self, msg) -> api.RawStreamMsg:
        if not msg.data:
            msg.data = None
            status = msg.headers.get("Status")
            if status:
                if status == "404":
                    raise NotFoundError
                else:
                    raise APIError.from_msg(msg)

        raw_msg = api.RawStreamMsg()
        subject = msg.headers["Nats-Subject"]
        raw_msg.subject = subject

        seq = msg.headers.get("Nats-Sequence")
        if seq:
            raw_msg.seq = int(seq)
        raw_msg.data = msg.data
        raw_msg.headers = msg.headers
        if time_string := msg.headers.get(api.Header.TIME_STAMP):
            raw_msg.time = api.Base._parse_utc_iso(time_string)

        return raw_msg

    async def delete_msg(self, stream_name: str, seq: int, no_erase: bool = False) -> bool:
        """
        Delete a message from a stream based on the sequence ID.

        By default the server overwrites the message's data in storage, as
        secure_delete_msg does. With ``no_erase=True`` it only marks the
        message as deleted, which is faster; this is what nats.go's
        Stream.DeleteMsg does.

        :param stream_name: The name of the stream from which the message should be deleted.
        :param seq: The sequence id to delete
        :param no_erase: Mark the message as deleted without erasing it.
        """
        _validate_stream_name(stream_name)
        req_subject = f"{self._prefix}.STREAM.MSG.DELETE.{stream_name}"
        req: Dict[str, Any] = {"seq": seq}
        if no_erase:
            req["no_erase"] = True
        data = json.dumps(req)
        resp = await self._api_request(req_subject, data.encode())
        return resp["success"]

    async def secure_delete_msg(self, stream_name: str, seq: int) -> bool:
        """
        Delete a message from a stream, overwriting its data in storage
        (nats.go Stream.SecureDeleteMsg).

        :param stream_name: The name of the stream from which the message should be deleted.
        :param seq: The sequence id to delete
        """
        return await self.delete_msg(stream_name, seq)

    async def get_last_msg(
        self,
        stream_name: str,
        subject: str,
        direct: Optional[bool] = False,
    ) -> api.RawStreamMsg:
        """
        get_last_msg retrieves the last message from a stream.
        """
        return await self.get_msg(stream_name, subject=subject, direct=direct)

    async def unpin_consumer(self, stream_name: str, consumer_name: str, group: str) -> None:
        """
        unpin_consumer releases the client currently pinned to a priority group
        of a consumer using ``PriorityPolicy.PINNED``, so that the next pull
        request from any client in that group is pinned instead.

        :param stream_name: Name of the stream the consumer belongs to.
        :param consumer_name: Name of the consumer.
        :param group: Priority group to unpin.
        """
        _validate_stream_name(stream_name)
        _validate_consumer_name(consumer_name)
        req_subject = f"{self._prefix}.CONSUMER.UNPIN.{stream_name}.{consumer_name}"
        req = {"group": group}
        data = json.dumps(req)
        await self._api_request(req_subject, data.encode())

    async def _request(self, req_subject: str, req: bytes, timeout: float) -> Msg:
        """
        Sends a JetStream API request, calling the client trace hooks.
        """
        trace = getattr(self, "_client_trace", None)
        if trace is not None and trace.request_sent is not None:
            trace.request_sent(req_subject, req)
        msg = await self._nc.request(req_subject, req, timeout=timeout)
        if trace is not None and trace.response_received is not None:
            trace.response_received(req_subject, msg.data, msg.headers)
        return msg

    async def _api_request(
        self,
        req_subject: str,
        req: bytes = b"",
        timeout: float = 5,
    ) -> Dict[str, Any]:
        try:
            msg = await self._request(req_subject, req, timeout=timeout)
            resp = json.loads(msg.data)
        except NoRespondersError:
            # nats.go reports a JetStream API request without responders as
            # ErrJetStreamNotEnabled.
            raise JetStreamNotEnabledError(
                code=503,
                err_code=ErrorCode.JETSTREAM_NOT_ENABLED,
                description="jetstream not enabled",
            )
        except ValueError:
            raise InvalidJetStreamResponseError

        # Check for API errors.
        if "error" in resp:
            raise APIError.from_error(resp["error"])

        return resp


class Stream:
    """
    A handle on one stream (nats.go ``jetstream.Stream``), returned by
    ``JetStreamManager.stream()``. It manages the stream's messages and
    consumers, and caches the stream's info.

    ::

        stream = await js.stream("ORDERS")
        print(stream.cached_info().state.messages)
        await stream.create_consumer(durable_name="processor")
        psub = await stream.consumer("processor")
        msgs = await psub.fetch(10)
    """

    def __init__(self, jsm: JetStreamManager, name: str, info: Optional[api.StreamInfo] = None) -> None:
        self._jsm = jsm
        self._name = name
        self._info = info

    @property
    def name(self) -> str:
        return self._name

    def cached_info(self) -> Optional[api.StreamInfo]:
        """
        The stream's info as last fetched, without a request.
        """
        return self._info

    async def info(
        self,
        subjects_filter: Optional[str] = None,
        deleted_details: Optional[bool] = None,
    ) -> api.StreamInfo:
        """
        Fetches the stream's info and caches it (without the subjects of
        ``subjects_filter``).
        """
        info = await self._jsm.stream_info(
            self._name,
            subjects_filter=subjects_filter,
            deleted_details=deleted_details,
        )
        cached = copy.copy(info)
        cached.state = copy.copy(info.state)
        cached.state.subjects = None
        self._info = cached
        return info

    async def purge(
        self,
        subject: Optional[str] = None,
        seq: Optional[int] = None,
        keep: Optional[int] = None,
    ) -> bool:
        """
        Purges the stream's messages: those of the subject, those before
        the sequence, or all but the last ``keep``. ``seq`` and ``keep``
        cannot be combined.
        """
        if seq and keep:
            raise InvalidOptionError(
                "nats: invalid jetstream option: both 'keep' and 'sequence' cannot be provided in purge request"
            )
        return await self._jsm.purge_stream(self._name, seq=seq, subject=subject, keep=keep)

    def _direct(self) -> bool:
        return bool(self._info is not None and self._info.config.allow_direct)

    async def get_msg(self, seq: int, subject: Optional[str] = None) -> api.RawStreamMsg:
        """
        Gets the message stored at the sequence or, with a subject, the
        first message on that subject at or after it. Uses direct get when
        the cached info allows it.
        """
        if subject:
            return await self._jsm.get_msg(self._name, seq=seq, subject=subject, next=True, direct=self._direct())
        return await self._jsm.get_msg(self._name, seq=seq, direct=self._direct())

    async def get_last_msg_for_subject(self, subject: str) -> api.RawStreamMsg:
        """
        Gets the last message stored on the subject.
        """
        return await self._jsm.get_last_msg(self._name, subject, direct=self._direct())

    async def delete_msg(self, seq: int) -> bool:
        """
        Marks the message at the sequence as deleted, without erasing it.
        """
        return await self._jsm.delete_msg(self._name, seq, no_erase=True)

    async def secure_delete_msg(self, seq: int) -> bool:
        """
        Deletes the message at the sequence, overwriting its data.
        """
        return await self._jsm.delete_msg(self._name, seq)

    async def create_consumer(self, config: Optional[api.ConsumerConfig] = None, **params) -> api.ConsumerInfo:
        """
        Creates a consumer of the stream; see JetStreamManager.create_consumer.
        """
        return await self._jsm.create_consumer(self._name, config, **params)

    async def update_consumer(self, config: Optional[api.ConsumerConfig] = None, **params) -> api.ConsumerInfo:
        """
        Updates a consumer of the stream; see JetStreamManager.update_consumer.
        """
        return await self._jsm.update_consumer(self._name, config, **params)

    async def create_or_update_consumer(
        self, config: Optional[api.ConsumerConfig] = None, **params
    ) -> api.ConsumerInfo:
        """
        Creates a consumer of the stream, or updates it.
        """
        return await self._jsm.create_or_update_consumer(self._name, config, **params)

    @staticmethod
    def _push_config(config: Optional[api.ConsumerConfig], params: Dict[str, Any]) -> api.ConsumerConfig:
        config = (config or api.ConsumerConfig()).evolve(**params)
        if not config.deliver_subject:
            raise NotPushConsumerError(description="consumer is not a push consumer")
        return config

    async def create_push_consumer(self, config: Optional[api.ConsumerConfig] = None, **params) -> api.ConsumerInfo:
        """
        Creates a push consumer of the stream; the config needs a
        deliver_subject (NotPushConsumerError otherwise).
        """
        return await self._jsm.create_consumer(self._name, self._push_config(config, params))

    async def update_push_consumer(self, config: Optional[api.ConsumerConfig] = None, **params) -> api.ConsumerInfo:
        """
        Updates a push consumer of the stream.
        """
        return await self._jsm.update_consumer(self._name, self._push_config(config, params))

    async def create_or_update_push_consumer(
        self, config: Optional[api.ConsumerConfig] = None, **params
    ) -> api.ConsumerInfo:
        """
        Creates a push consumer of the stream, or updates it.
        """
        return await self._jsm.create_or_update_consumer(self._name, self._push_config(config, params))

    async def consumer_info(self, name: str) -> api.ConsumerInfo:
        return await self._jsm.consumer_info(self._name, name)

    async def delete_consumer(self, name: str) -> bool:
        return await self._jsm.delete_consumer(self._name, name)

    async def pause_consumer(self, name: str, pause_until: str) -> api.ConsumerPause:
        return await self._jsm.pause_consumer(self._name, name, pause_until)

    async def resume_consumer(self, name: str) -> api.ConsumerPause:
        return await self._jsm.resume_consumer(self._name, name)

    async def reset_consumer(self, name: str, seq: Optional[int] = None) -> api.ConsumerReset:
        return await self._jsm.reset_consumer(self._name, name, seq)

    async def unpin_consumer(self, name: str, group: str) -> None:
        await self._jsm.unpin_consumer(self._name, name, group)

    def list_consumers(self) -> AsyncIterator[api.ConsumerInfo]:
        """
        Iterates over the infos of the stream's consumers.
        """
        return self._jsm.list_consumers(self._name)

    def consumer_names(self) -> AsyncIterator[str]:
        """
        Iterates over the names of the stream's consumers.
        """
        return self._jsm.consumer_names(self._name)

    def _context(self) -> Any:
        if not hasattr(self._jsm, "pull_subscribe_bind"):
            raise Error("consuming requires a JetStreamContext (nc.jetstream())")
        return self._jsm

    async def consumer(self, name: str, **params) -> Any:
        """
        Returns a pull subscription bound to the stream's pull consumer
        (nats.go Stream.Consumer). ``params`` are passed to
        JetStreamContext.pull_subscribe_bind.

        :raises NotPullConsumerError: if the consumer is a push consumer.
        """
        js = self._context()
        info = await self._jsm.consumer_info(self._name, name)
        if info.config.deliver_subject:
            raise NotPullConsumerError(description="consumer is not a pull consumer")
        return await js.pull_subscribe_bind(consumer=name, stream=self._name, **params)

    async def push_consumer(self, name: str, cb: Optional[Any] = None, **params) -> Any:
        """
        Returns a push subscription bound to the stream's push consumer
        (nats.go Stream.PushConsumer), delivering to ``cb`` if given.
        ``params`` are passed to JetStreamContext.subscribe_bind.

        :raises NotPushConsumerError: if the consumer is a pull consumer.
        """
        js = self._context()
        info = await self._jsm.consumer_info(self._name, name)
        if not info.config.deliver_subject:
            raise NotPushConsumerError(description="consumer is not a push consumer")
        return await js.subscribe_bind(stream=self._name, config=info.config, consumer=name, cb=cb, **params)

    async def ordered_consumer(self, subject: str = ">", cb: Optional[Any] = None, **params) -> Any:
        """
        Returns an ordered consumer of the stream's messages on the subject
        (nats.go Stream.OrderedConsumer): a push subscription whose
        ephemeral consumer is recreated in order whenever a delivery is
        missed. ``params`` are passed to JetStreamContext.subscribe.
        """
        js = self._context()
        return await js.subscribe(subject, cb=cb, stream=self._name, ordered_consumer=True, **params)
