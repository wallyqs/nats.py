import asyncio
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
