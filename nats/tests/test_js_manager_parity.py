import asyncio
import json
import unittest

import nats.js.api
import pytest
from nats.aio.msg import Msg
from nats.js import api
from nats.js.errors import *

import nats
from tests.utils import *


class ErrorCodeTest(unittest.TestCase):
    def test_error_code_values(self):
        assert ErrorCode.BAD_REQUEST == 10003
        assert ErrorCode.CONSUMER_CREATE == 10012
        assert ErrorCode.CONSUMER_NAME_EXISTS == 10013
        assert ErrorCode.CONSUMER_NOT_FOUND == 10014
        assert ErrorCode.MAXIMUM_CONSUMERS_LIMIT == 10026
        assert ErrorCode.MESSAGE_NOT_FOUND == 10037
        assert ErrorCode.JETSTREAM_NOT_ENABLED_FOR_ACCOUNT == 10039
        assert ErrorCode.STREAM_NAME_IN_USE == 10058
        assert ErrorCode.STREAM_NOT_FOUND == 10059
        assert ErrorCode.STREAM_WRONG_LAST_SEQUENCE == 10071
        assert ErrorCode.JETSTREAM_NOT_ENABLED == 10076
        assert ErrorCode.CONSUMER_ALREADY_EXISTS == 10105
        assert ErrorCode.DUPLICATE_FILTER_SUBJECTS == 10136
        assert ErrorCode.OVERLAPPING_FILTER_SUBJECTS == 10138
        assert ErrorCode.CONSUMER_EMPTY_FILTER == 10139
        assert ErrorCode.CONSUMER_EXISTS == 10148
        assert ErrorCode.CONSUMER_DOES_NOT_EXIST == 10149
        assert ErrorCode.STREAM_WRONG_LAST_SEQUENCE_CONSTANT == 10164
        assert ErrorCode.MIRROR_WITH_MSG_SCHEDULES == 10186
        assert ErrorCode.SOURCE_WITH_MSG_SCHEDULES == 10187
        assert ErrorCode.MESSAGE_SCHEDULES_DISABLED == 10188
        assert ErrorCode.SCHEDULE_PATTERN_INVALID == 10189
        assert ErrorCode.SCHEDULE_TARGET_INVALID == 10190
        assert ErrorCode.SCHEDULE_TTL_INVALID == 10191
        assert ErrorCode.SCHEDULE_ROLLUP_INVALID == 10192
        assert ErrorCode.SCHEDULE_SOURCE_INVALID == 10203
        assert ErrorCode.CONSUMER_INVALID_RESET == 10204

    def test_from_error_maps_err_code(self):
        cases = [
            (404, 10059, StreamNotFoundError, NotFoundError),
            (404, 10014, ConsumerNotFoundError, NotFoundError),
            (404, 10037, MsgNotFoundError, NotFoundError),
            (400, 10058, StreamNameAlreadyInUseError, BadRequestError),
            (400, 10071, StreamWrongLastSequenceError, BadRequestError),
            (400, 10164, StreamWrongLastSequenceError, BadRequestError),
            (400, 10003, JetStreamBadRequestError, BadRequestError),
            (500, 10012, ConsumerCreateError, ServerError),
            (400, 10013, ConsumerNameAlreadyInUseError, BadRequestError),
            (400, 10148, ConsumerExistsError, BadRequestError),
            (400, 10149, ConsumerDoesNotExistError, BadRequestError),
            (400, 10026, MaximumConsumersLimitError, BadRequestError),
            (400, 10136, DuplicateFilterSubjectsError, BadRequestError),
            (400, 10138, OverlappingFilterSubjectsError, BadRequestError),
            (400, 10139, EmptyFilterError, BadRequestError),
            (503, 10076, JetStreamNotEnabledError, ServiceUnavailableError),
            (503, 10039, JetStreamNotEnabledForAccountError, ServiceUnavailableError),
            (400, 10186, MirrorWithMsgSchedulesError, BadRequestError),
            (400, 10187, SourceWithMsgSchedulesError, BadRequestError),
            (400, 10188, MessageSchedulesDisabledError, BadRequestError),
            (400, 10189, SchedulePatternInvalidError, BadRequestError),
            (400, 10190, ScheduleTargetInvalidError, BadRequestError),
            (400, 10191, ScheduleTTLInvalidError, BadRequestError),
            (400, 10192, ScheduleRollupInvalidError, BadRequestError),
            (400, 10203, ScheduleSourceInvalidError, BadRequestError),
            (400, 10204, ConsumerInvalidResetError, BadRequestError),
        ]
        for code, err_code, typed, base in cases:
            with pytest.raises(typed) as err:
                APIError.from_error({"code": code, "err_code": err_code, "description": "x"})
            assert isinstance(err.value, base)
            assert err.value.code == code
            assert err.value.err_code == err_code
            assert err.value.api_error is err.value

    def test_from_error_keeps_status_class_when_codes_disagree(self):
        # A typed error is only raised when it is a subclass of the error
        # the status code maps to, so existing except-clauses keep working.
        with pytest.raises(BadRequestError) as err:
            APIError.from_error({"code": 400, "err_code": 10059, "description": "x"})
        assert type(err.value) is BadRequestError
        with pytest.raises(NotFoundError) as err:
            APIError.from_error({"code": 404, "err_code": 99999, "description": "x"})
        assert type(err.value) is NotFoundError

    def test_from_msg_conflicts(self):
        def status_msg(desc):
            return Msg(None, subject="x", headers={"Status": "409", "Description": desc})

        with pytest.raises(ConsumerDeletedError) as err:
            APIError.from_msg(status_msg("Consumer Deleted"))
        assert err.value.code == 409
        with pytest.raises(NotPullConsumerError):
            APIError.from_msg(status_msg("Consumer is push based"))
        with pytest.raises(APIError) as err:
            APIError.from_msg(status_msg("Exceeded MaxWaiting"))
        assert type(err.value) is APIError

    def test_client_side_errors(self):
        assert issubclass(StreamNameRequiredError, ValueError)
        assert issubclass(InvalidStreamNameError, ValueError)
        assert issubclass(InvalidConsumerNameError, ValueError)
        assert issubclass(InvalidSubjectError, ValueError)
        assert issubclass(InvalidOptionError, ValueError)
        assert issubclass(NoMessagesError, nats.errors.TimeoutError)
        assert JetStreamError is Error
        assert Error("x").api_error is None
        assert str(StreamNameRequiredError()) == "nats: stream name is required"
        assert str(ConsumerCreationResponseEmptyError()) == "nats: consumer creation response is empty"
        assert str(ConsumerResetResponseEmptyError()) == "nats: consumer reset response is empty"
        assert issubclass(StreamSourceSubjectTransformNotSupportedError, StreamSubjectTransformNotSupportedError)


class JetStreamErrorsTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_api_errors_are_typed(self):
        nc = await nats.connect()
        js = nc.jetstream()

        with pytest.raises(StreamNotFoundError) as err:
            await js.stream_info("missing")
        assert isinstance(err.value, NotFoundError)
        assert err.value.err_code == ErrorCode.STREAM_NOT_FOUND

        await js.add_stream(name="ERRS", subjects=["errs.>"], max_consumers=1)

        with pytest.raises(ConsumerNotFoundError) as err:
            await js.consumer_info("ERRS", "missing")
        assert isinstance(err.value, NotFoundError)
        assert err.value.err_code == ErrorCode.CONSUMER_NOT_FOUND

        with pytest.raises(StreamNameAlreadyInUseError) as err:
            await js.add_stream(name="ERRS", subjects=["other.>"])
        assert isinstance(err.value, BadRequestError)

        await js.publish("errs.a", b"one")
        with pytest.raises(StreamWrongLastSequenceError) as err:
            await js.publish("errs.a", b"two", headers={api.Header.EXPECTED_LAST_SEQUENCE: "5"})
        assert isinstance(err.value, BadRequestError)
        assert err.value.err_code == ErrorCode.STREAM_WRONG_LAST_SEQUENCE

        with pytest.raises(OverlappingFilterSubjectsError):
            await js.add_consumer("ERRS", durable_name="overlap", filter_subjects=["errs.*", "errs.a"])

        await js.add_consumer("ERRS", durable_name="first")
        with pytest.raises(MaximumConsumersLimitError) as err:
            await js.add_consumer("ERRS", durable_name="second")
        assert isinstance(err.value, BadRequestError)

        with pytest.raises(InvalidStreamNameError):
            await js.stream_info("bad.name")
        with pytest.raises(StreamNameRequiredError):
            await js.stream_info("")
        with pytest.raises(InvalidConsumerNameError):
            await js.consumer_info("ERRS", "bad.name")
        with pytest.raises(StreamNameRequiredError):
            await js.pull_subscribe_bind("first", stream="")

        await nc.close()

    @async_test
    async def test_pull_conflicts_are_typed(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="CONFLICTS", subjects=["conflicts"])

        # A pull request on a push consumer.
        await js.add_consumer("CONFLICTS", durable_name="push", deliver_subject="push.deliver")
        psub = await js.pull_subscribe_bind("push", stream="CONFLICTS")
        with pytest.raises(NotPullConsumerError) as err:
            await psub.fetch(1, timeout=1)
        assert isinstance(err.value, APIError)
        assert err.value.code == 409

        # A pending pull request whose consumer gets deleted.
        psub = await js.pull_subscribe("conflicts", durable="pull", stream="CONFLICTS")
        fetch = asyncio.ensure_future(psub.fetch(1, timeout=3))
        await asyncio.sleep(0.2)
        await js.delete_consumer("CONFLICTS", "pull")
        with pytest.raises(ConsumerDeletedError) as err:
            await fetch
        assert isinstance(err.value, APIError)

        await nc.close()

    @async_test
    async def test_consumer_has_active_subscription(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ACTIVE", subjects=["active"])
        await js.subscribe("active", durable="dur", stream="ACTIVE")
        await nc.flush()
        # The server marks the consumer as push bound once it sees interest.
        for _ in range(20):
            info = await js.consumer_info("ACTIVE", "dur")
            if info.push_bound:
                break
            await asyncio.sleep(0.05)
        with pytest.raises(ConsumerHasActiveSubscriptionError) as err:
            await js.subscribe("active", durable="dur", stream="ACTIVE")
        assert isinstance(err.value, Error)
        await nc.close()


class JetStreamNotEnabledTest(SingleServerTestCase):
    @async_test
    async def test_jetstream_not_enabled(self):
        nc = await nats.connect()
        js = nc.jetstream()
        with pytest.raises(JetStreamNotEnabledError) as err:
            await js.account_info()
        assert isinstance(err.value, ServiceUnavailableError)
        assert err.value.err_code == ErrorCode.JETSTREAM_NOT_ENABLED
        await nc.close()


LIMITS = {
    "max_memory": -1,
    "max_storage": -1,
    "max_streams": -1,
    "max_consumers": -1,
    "max_ack_pending": -1,
    "memory_max_stream_bytes": -1,
    "storage_max_stream_bytes": -1,
    "max_bytes_required": False,
}


class APITypesTest(unittest.TestCase):
    def test_cluster_info_fields(self):
        info = api.ClusterInfo.from_response(
            {
                "name": "C1",
                "leader": "n1",
                "system_account": True,
                "traffic_account": "SYS",
                "replicas": [{"name": "n2", "current": True, "active": 1, "peer": "abc", "pending": True}],
                "desired": {
                    "created": "2026-01-02T03:04:05.123456789Z",
                    "name": "C2",
                    "replicas": [{"name": "n3", "offline": True, "peer": "def"}],
                    "origin": {"replicas": 3, "placement": {"cluster": "C1"}, "retention": "limits"},
                    "status": {"description": "waiting for quorum", "type": "quorum", "err": "boom"},
                },
            }
        )
        assert info.system_acc is True
        assert info.traffic_acc == "SYS"
        assert info.replicas[0].peer == "abc"
        assert info.replicas[0].pending is True
        desired = info.desired
        assert isinstance(desired, api.DesiredClusterInfo)
        assert desired.created.year == 2026
        assert desired.name == "C2"
        assert desired.replicas == [api.DesiredPeerInfo(name="n3", offline=True, peer="def")]
        assert desired.origin.replicas == 3
        assert desired.origin.placement == api.Placement(cluster="C1")
        assert desired.origin.retention == api.RetentionPolicy.LIMITS
        assert desired.status.description == "waiting for quorum"
        assert desired.status.type == api.MigrationStatusType.QUORUM
        assert desired.status.err == "boom"

    def test_migration_status_values(self):
        assert [m.value for m in api.MigrationStatusType] == [
            "meta",
            "membership",
            "snapshot",
            "catchup",
            "quorum",
            "blocked",
            "unavailable",
        ]

    def test_stream_source_domain(self):
        src = api.StreamSource(name="ORIGIN", domain="hub")
        assert src.as_dict() == {"name": "ORIGIN", "external": {"api": "$JS.hub.API"}}
        with pytest.raises(ValueError, match="domain and external are both set"):
            api.StreamSource(name="ORIGIN", domain="hub", external=api.ExternalStream(api="$JS.x.API")).as_dict()

    def test_defaults(self):
        assert api.DEFAULT_EXPIRES == 30.0
        assert api.DEFAULT_MAX_MESSAGES == 500
        assert api.DEFAULT_PUB_RETRY_ATTEMPTS == 2
        assert api.DEFAULT_PUB_RETRY_WAIT == 0.25
        assert api.Header.TIME_STAMP == "Nats-Time-Stamp"

    def test_api_stats_and_tiers(self):
        info = api.AccountInfo.from_response(
            {
                "memory": 1,
                "storage": 2,
                "reserved_memory": 3,
                "reserved_storage": 4,
                "streams": 1,
                "consumers": 0,
                "limits": LIMITS,
                "api": {"level": 1, "total": 5, "errors": 0, "inflight": 2},
                "tiers": {
                    "R1": {
                        "memory": 1,
                        "storage": 2,
                        "reserved_memory": 5,
                        "reserved_storage": 6,
                        "streams": 1,
                        "consumers": 0,
                        "limits": LIMITS,
                    }
                },
            }
        )
        assert info.reserved_memory == 3
        assert info.reserved_storage == 4
        assert info.api.inflight == 2
        assert info.tiers["R1"].reserved_memory == 5
        assert info.tiers["R1"].reserved_storage == 6


class APITypesServerTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_info_fields(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.add_stream(name="ORIGIN1", subjects=["one.>"])
        await js.add_stream(name="ORIGIN2", subjects=["two.>"])
        for subject in ("one.a", "one.b", "two.a"):
            await js.publish(subject, b"x")
        await js.add_stream(
            name="SOURCED",
            sources=[
                api.StreamSource(name="ORIGIN1", filter_subject="one.a"),
                api.StreamSource(
                    name="ORIGIN2",
                    subject_transforms=[api.SubjectTransform(src="two.>", dest="moved.>")],
                ),
            ],
        )
        await js.add_stream(name="REMOTE", sources=[api.StreamSource(name="ORIGIN1", domain="hub")])

        si = await js.stream_info("ORIGIN1")
        assert si.ts is not None
        assert si.state.first_ts is not None
        assert si.state.last_ts >= si.state.first_ts
        assert si.state.num_subjects == 2

        for _ in range(50):
            si = await js.stream_info("SOURCED")
            if si.state.messages == 2:
                break
            await asyncio.sleep(0.05)
        sources = {src.name: src for src in si.sources}
        assert sources["ORIGIN1"].filter_subject == "one.a"
        assert sources["ORIGIN2"].subject_transforms == [api.SubjectTransform(src="two.>", dest="moved.>")]

        si = await js.stream_info("REMOTE")
        assert si.config.sources[0].external.api == "$JS.hub.API"

        await js.add_consumer(
            "ORIGIN1",
            durable_name="limits",
            max_batch=10,
            max_expires=2.5,
            max_bytes=1024,
        )
        ci = await js.consumer_info("ORIGIN1", "limits")
        assert ci.ts is not None
        assert ci.config.max_batch == 10
        assert ci.config.max_expires == 2.5
        assert ci.config.max_bytes == 1024

        info = await js.account_info()
        assert info.reserved_memory is not None
        assert info.reserved_storage is not None

        await nc.close()


class StreamManagementTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_create_or_update_stream(self):
        nc = await nats.connect()
        js = nc.jetstream()

        info = await js.create_or_update_stream(name="UPSERT", subjects=["upsert.a"])
        assert info.config.subjects == ["upsert.a"]
        assert info.did_create is True

        info = await js.create_or_update_stream(api.StreamConfig(name="UPSERT"), subjects=["upsert.a", "upsert.b"])
        assert info.config.subjects == ["upsert.a", "upsert.b"]

        # Errors other than a missing stream are raised.
        with pytest.raises(ServerError):
            await js.create_or_update_stream(name="UPSERT", subjects=["upsert.a"], storage=api.StorageType.MEMORY)
        await nc.close()

    @async_test
    async def test_stream_names_and_list_streams(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.add_stream(name="LA", subjects=["la.>"])
        await js.add_stream(name="LB", subjects=["lb.>"])
        await js.add_stream(name="LC", subjects=["lc.x"])

        names = [name async for name in js.stream_names()]
        assert sorted(names) == ["LA", "LB", "LC"]
        names = [name async for name in js.stream_names(subject="lb.foo")]
        assert names == ["LB"]
        assert [name async for name in js.stream_names(subject="nothing")] == []

        infos = [info async for info in js.list_streams()]
        assert sorted(info.config.name for info in infos) == ["LA", "LB", "LC"]
        infos = [info async for info in js.list_streams(subject="lc.x")]
        assert [info.config.name for info in infos] == ["LC"]

        assert await js.stream_name_by_subject("la.foo") == "LA"
        with pytest.raises(StreamNotFoundError):
            await js.stream_name_by_subject("nothing")
        with pytest.raises(StreamNotFoundError):
            await js.find_stream_name_by_subject("nothing")
        for subject in ("", ".a", "a.", "a b", "a.>.b"):
            with pytest.raises(InvalidSubjectError):
                await js.stream_name_by_subject(subject)
        await nc.close()

    @async_long_test
    async def test_list_streams_pages(self):
        nc = await nats.connect()
        js = nc.jetstream()

        # The server lists at most 256 stream infos per page.
        total = 300
        for i in range(total):
            await js.add_stream(name=f"P{i}", subjects=[f"p.{i}"], storage=api.StorageType.MEMORY)

        infos = [info async for info in js.list_streams()]
        assert len(infos) == total
        assert len({info.config.name for info in infos}) == total
        names = [name async for name in js.stream_names()]
        assert len(set(names)) == total
        # The single page API is unchanged.
        assert len(await js.streams_info()) == 256
        await nc.close()

    @async_test
    async def test_stream_info_deleted_details(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="DELETED", subjects=["deleted.>"])
        for i in range(5):
            await js.publish(f"deleted.{i}", b"x")
        await js.delete_msg("DELETED", 2)
        await js.delete_msg("DELETED", 4)

        info = await js.stream_info("DELETED")
        assert info.state.deleted is None
        assert info.state.num_deleted == 2
        info = await js.stream_info("DELETED", deleted_details=True)
        assert info.state.deleted == [2, 4]
        info = await js.stream_info("DELETED", subjects_filter=">", deleted_details=True)
        assert info.state.deleted == [2, 4]
        assert info.state.subjects == {"deleted.0": 1, "deleted.2": 1, "deleted.4": 1}
        await nc.close()

    @async_test
    async def test_stream_info_subjects_pages(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="SUBJECTS", subjects=["subjects.>"])
        for i in range(3):
            await js.publish(f"subjects.{i}", b"x")

        # Make the server's reply look like one page of a larger listing, as
        # it is for more than 100k subjects.
        requests = []
        real_request = js._api_request

        async def paged_request(subject, req=b"", timeout=5):
            requests.append(json.loads(req) if req else None)
            offset = requests[-1].pop("offset", 0)
            resp = await real_request(subject, json.dumps(requests[-1]).encode(), timeout=timeout)
            if offset:
                requests[-1]["offset"] = offset
            subjects = sorted(resp["state"]["subjects"].items())
            resp["state"]["subjects"] = dict(subjects[offset : offset + 1])
            resp["total"] = len(subjects)
            resp["offset"] = offset
            return resp

        js._api_request = paged_request
        info = await js.stream_info("SUBJECTS", subjects_filter=">")
        assert info.state.subjects == {"subjects.0": 1, "subjects.1": 1, "subjects.2": 1}
        assert requests == [
            {"subjects_filter": ">"},
            {"subjects_filter": ">", "offset": 1},
            {"subjects_filter": ">", "offset": 2},
        ]
        await nc.close()


class ConsumerManagementTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_create_and_update_consumer(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="CONS", subjects=["cons.>"])

        info = await js.create_consumer("CONS", durable_name="dur", description="first")
        assert info.name == "dur"
        assert info.config.description == "first"
        # The same config is idempotent.
        info = await js.create_consumer("CONS", durable_name="dur", description="first")
        assert info.name == "dur"
        with pytest.raises(ConsumerExistsError) as err:
            await js.create_consumer("CONS", durable_name="dur", description="second")
        assert isinstance(err.value, BadRequestError)
        assert err.value.err_code == ErrorCode.CONSUMER_EXISTS

        info = await js.update_consumer("CONS", api.ConsumerConfig(durable_name="dur"), description="second")
        assert info.config.description == "second"
        with pytest.raises(ConsumerDoesNotExistError) as err:
            await js.update_consumer("CONS", name="missing", description="x")
        assert isinstance(err.value, BadRequestError)

        info = await js.create_or_update_consumer("CONS", name="named", filter_subject="cons.a")
        assert info.config.filter_subject == "cons.a"
        info = await js.create_or_update_consumer("CONS", name="named", filter_subject="cons.b")
        assert info.config.filter_subject == "cons.b"

        # A consumer without a name gets a generated one.
        info = await js.create_consumer("CONS", api.ConsumerConfig(inactive_threshold=10))
        assert info.name
        info = await js.consumer_info("CONS", info.name)
        assert info.config.durable_name is None

        with pytest.raises(InvalidConsumerNameError):
            await js.create_consumer("CONS", name="bad.name")
        await nc.close()

    @async_long_test
    async def test_consumer_names_and_list_consumers(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="MANY", subjects=["many"], storage=api.StorageType.MEMORY)
        assert [name async for name in js.consumer_names("MANY")] == []

        # The server lists at most 256 consumer infos per page.
        total = 300
        for i in range(total):
            await js.add_consumer("MANY", durable_name=f"c{i}", mem_storage=True)

        names = [name async for name in js.consumer_names("MANY")]
        assert sorted(names) == sorted(f"c{i}" for i in range(total))
        infos = [info async for info in js.list_consumers("MANY")]
        assert sorted(info.name for info in infos) == sorted(names)
        assert len(await js.consumers_info("MANY")) == 256
        await nc.close()

    @async_test
    async def test_delete_msg_no_erase(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="DEL", subjects=["del"])
        for _ in range(3):
            await js.publish("del", b"x")

        requests = []

        async def spy(msg):
            requests.append(json.loads(msg.data))

        await nc.subscribe("$JS.API.STREAM.MSG.DELETE.DEL", cb=spy)
        assert await js.delete_msg("DEL", 1)
        assert await js.delete_msg("DEL", 2, no_erase=True)
        assert await js.secure_delete_msg("DEL", 3)
        await nc.flush()
        assert requests == [{"seq": 1}, {"seq": 2, "no_erase": True}, {"seq": 3}]
        info = await js.stream_info("DEL")
        assert info.state.messages == 0
        await nc.close()


class JetStreamOptionsTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_client_trace_and_options(self):
        nc = await nats.connect()
        sent = []
        received = []
        trace = api.ClientTrace(
            request_sent=lambda subject, payload: sent.append((subject, payload)),
            response_received=lambda subject, payload, headers: received.append((subject, payload, headers)),
        )
        js = nc.jetstream(timeout=3, client_trace=trace)
        assert js.conn is nc
        opts = js.options
        assert opts == api.JetStreamOptions(
            api_prefix="$JS.API",
            domain=None,
            default_timeout=3,
            client_trace=trace,
            publish_async_max_pending=4000,
        )

        await js.add_stream(name="TRACED", subjects=["traced"], allow_direct=True)
        assert sent[0][0] == "$JS.API.STREAM.CREATE.TRACED"
        assert json.loads(sent[0][1])["name"] == "TRACED"
        assert received[0][0] == "$JS.API.STREAM.CREATE.TRACED"
        assert json.loads(received[0][1])["config"]["name"] == "TRACED"

        await js.publish("traced", b"hello")
        # Publishing is not an API request.
        assert len(sent) == 1

        with pytest.raises(StreamNotFoundError):
            await js.stream_info("MISSING")
        assert sent[1] == ("$JS.API.STREAM.INFO.MISSING", b"")
        assert json.loads(received[1][1])["error"]["err_code"] == 10059

        msg = await js.get_msg("TRACED", 1, direct=True)
        assert msg.data == b"hello"
        assert sent[2][0] == "$JS.API.DIRECT.GET.TRACED"
        assert received[2][1] == b"hello"
        assert received[2][2]["Nats-Sequence"] == "1"

        # Consumers created by subscriptions are traced as well.
        await js.pull_subscribe("traced", durable="dur")
        assert any(subject.startswith("$JS.API.CONSUMER.") for subject, _ in sent[3:])

        jsm = nc.jsm(prefix="$JS.API", timeout=2, client_trace=trace)
        assert jsm.conn is nc
        assert jsm.options == api.JetStreamOptions(api_prefix="$JS.API", default_timeout=2, client_trace=trace)
        count = len(sent)
        await jsm.account_info()
        assert sent[count] == ("$JS.API.INFO", b"")

        domain_js = nc.jetstream(domain="hub")
        assert domain_js.options.domain == "hub"
        assert domain_js.options.client_trace is None
        await nc.close()


class StreamHandleTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_stream_handle(self):
        nc = await nats.connect()
        js = nc.jetstream()

        with pytest.raises(StreamNotFoundError):
            await js.stream("MISSING")

        await js.add_stream(name="HANDLE", subjects=["handle.>"])
        stream = await js.stream("HANDLE")
        assert stream.name == "HANDLE"
        assert stream.cached_info().state.messages == 0

        for subject in ("handle.a", "handle.b", "handle.a", "handle.b"):
            await js.publish(subject, b"x")
        assert stream.cached_info().state.messages == 0
        info = await stream.info(subjects_filter=">")
        assert info.state.subjects == {"handle.a": 2, "handle.b": 2}
        assert stream.cached_info().state.messages == 4
        assert stream.cached_info().state.subjects is None

        msg = await stream.get_msg(1)
        assert msg.subject == "handle.a"
        msg = await stream.get_msg(2, subject="handle.a")
        assert msg.seq == 3
        msg = await stream.get_last_msg_for_subject("handle.b")
        assert msg.seq == 4

        assert await stream.delete_msg(2)
        assert await stream.secure_delete_msg(3)
        info = await stream.info(deleted_details=True)
        assert info.state.deleted == [2, 3]

        with pytest.raises(InvalidOptionError):
            await stream.purge(seq=4, keep=1)
        assert await stream.purge(subject="handle.a")
        assert (await stream.info()).state.messages == 1

        # Consumers.
        info = await stream.create_consumer(durable_name="pull")
        assert info.name == "pull"
        info = await stream.update_consumer(durable_name="pull", description="updated")
        assert info.config.description == "updated"
        info = await stream.create_or_update_consumer(durable_name="other")
        assert (await stream.consumer_info("other")).name == "other"
        with pytest.raises(NotPushConsumerError):
            await stream.create_push_consumer(durable_name="push")
        info = await stream.create_push_consumer(durable_name="push", deliver_subject="deliver.one")
        assert info.config.deliver_subject == "deliver.one"
        info = await stream.update_push_consumer(
            durable_name="push", deliver_subject="deliver.one", description="updated"
        )
        assert info.config.description == "updated"
        info = await stream.create_or_update_push_consumer(durable_name="push2", deliver_subject="deliver.two")
        assert info.name == "push2"

        assert sorted([name async for name in stream.consumer_names()]) == ["other", "pull", "push", "push2"]
        infos = [info async for info in stream.list_consumers()]
        assert sorted(info.name for info in infos) == ["other", "pull", "push", "push2"]
        assert await stream.delete_consumer("other")

        # The remaining message, handle.b at sequence 4.
        psub = await stream.consumer("pull")
        msgs = await psub.fetch(1, timeout=1)
        assert msgs[0].subject == "handle.b"
        with pytest.raises(NotPullConsumerError):
            await stream.consumer("push")

        received = asyncio.Queue()

        async def cb(msg):
            await received.put(msg)

        await stream.push_consumer("push", cb=cb)
        msg = await asyncio.wait_for(received.get(), 1)
        assert msg.subject == "handle.b"
        with pytest.raises(NotPushConsumerError):
            await stream.push_consumer("pull")

        osub = await stream.ordered_consumer()
        msg = await osub.next_msg(timeout=1)
        assert msg.subject == "handle.b"

        jsm = nc.jsm()
        handle = await jsm.stream("HANDLE")
        with pytest.raises(Error):
            await handle.consumer("pull")
        await nc.close()
