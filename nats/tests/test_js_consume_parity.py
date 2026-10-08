import asyncio
import json
import datetime
import time
import unittest

import nats
import nats.js.api
import nats.js.consume
import pytest
from nats.aio.msg import Msg
from nats.js import api
from nats.js.errors import *

from tests.utils import *


class MsgScheduleHeadersTest(unittest.TestCase):
    def test_every_formats_go_duration(self):
        cases = {
            1: "@every 1s",
            1.5: "@every 1.5s",
            90: "@every 1m30s",
            3600: "@every 1h0m0s",
            0.25: "@every 250ms",
        }
        for every, value in cases.items():
            with self.subTest(every=every):
                assert api.MsgSchedule(every=every).headers() == {api.Header.SCHEDULE: value}

    def test_at_is_rfc3339_utc(self):
        at = datetime.datetime(2030, 1, 2, 3, 4, 5, 600000, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
        assert api.MsgSchedule(at=at).headers() == {api.Header.SCHEDULE: "@at 2030-01-02T01:04:05Z"}
        naive = datetime.datetime(2030, 1, 2, 3, 4, 5)
        assert api.MsgSchedule(at=naive).headers() == {api.Header.SCHEDULE: "@at 2030-01-02T03:04:05Z"}

    def test_all_fields(self):
        sched = api.MsgSchedule(
            cron=api.SCHEDULE_HOURLY,
            target="out",
            source="src",
            ttl=30,
            time_zone="Europe/Amsterdam",
            rollup=True,
        )
        assert sched.headers() == {
            "Nats-Schedule": "@hourly",
            "Nats-Schedule-Target": "out",
            "Nats-Schedule-Source": "src",
            "Nats-Schedule-TTL": "30s",
            "Nats-Schedule-Time-Zone": "Europe/Amsterdam",
            "Nats-Schedule-Rollup": "sub",
        }
        assert api.MsgSchedule(cron="* * * * * *", ttl_never=True).headers()["Nats-Schedule-TTL"] == "never"

    def test_conflicting_fields(self):
        with pytest.raises(ValueError):
            api.MsgSchedule(cron="@daily", every=1).headers()
        with pytest.raises(ValueError):
            api.MsgSchedule(cron="@daily", ttl=1, ttl_never=True).headers()

    def test_constants(self):
        assert api.MSG_ROLLUP_ALL == "all"
        assert api.MSG_ROLLUP_SUBJECT == "sub"
        assert api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT == "Nats-Expected-Last-Subject-Sequence-Subject"
        assert api.Header.LAST_SEQUENCE == "Nats-Last-Sequence"
        assert api.Header.SEQUENCE == "Nats-Sequence"
        assert api.Header.STREAM == "Nats-Stream"
        assert api.Header.SUBJECT == "Nats-Subject"


class PublishOptionsTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_msg_id_and_expectations(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="OPTS", subjects=["opts.>"])

        ack = await js.publish("opts.a", b"1", msg_id="m1")
        assert ack.seq == 1 and not ack.duplicate
        ack = await js.publish("opts.a", b"1", msg_id="m1")
        assert ack.seq == 1 and ack.duplicate

        ack = await js.publish("opts.b", b"2", expected_last_msg_id="m1", expected_last_sequence=1)
        assert ack.seq == 2
        with pytest.raises(APIError) as err:
            await js.publish("opts.b", b"3", expected_last_msg_id="nope")
        assert err.value.err_code == 10070
        with pytest.raises(APIError) as err:
            await js.publish("opts.b", b"3", expected_last_sequence=1)
        assert err.value.err_code == 10071

        # Last sequence of the published subject.
        ack = await js.publish("opts.a", b"4", expected_last_subject_sequence=1)
        assert ack.seq == 3
        with pytest.raises(APIError) as err:
            await js.publish("opts.a", b"5", expected_last_subject_sequence=1)
        assert err.value.err_code == 10071

        # Last sequence of another subject.
        ack = await js.publish(
            "opts.c",
            b"6",
            expected_last_subject_sequence=2,
            expected_last_subject_sequence_subject="opts.b",
        )
        assert ack.seq == 4
        with pytest.raises(APIError) as err:
            await js.publish(
                "opts.c",
                b"7",
                expected_last_subject_sequence=3,
                expected_last_subject_sequence_subject="opts.b",
            )
        assert err.value.err_code == 10071
        with pytest.raises(ValueError):
            await js.publish("opts.c", b"7", expected_last_subject_sequence_subject="opts.b")

        # The same options apply to async publishes.
        future = await js.publish_async("opts.d", b"8", msg_id="m1")
        ack = await future
        assert ack.duplicate
        future = await js.publish_async(
            "opts.d", b"9", expected_last_subject_sequence=4, expected_last_subject_sequence_subject="opts.c"
        )
        assert (await future).seq == 5

        await nc.close()

    @async_test
    async def test_rollup_all(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ROLL", subjects=["roll.>"], allow_rollup_hdrs=True)
        for subject in ("roll.a", "roll.b", "roll.c"):
            await js.publish(subject, b"x")
        await js.publish("roll.d", b"y", headers={api.Header.ROLLUP: api.MSG_ROLLUP_ALL})
        info = await js.stream_info("ROLL")
        assert info.state.messages == 1
        await nc.close()

    @async_test
    async def test_schedule(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="SCHED", subjects=["sched.>"], allow_msg_schedules=True)
        sub = await js.subscribe("sched.target", stream="SCHED")

        at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=1)
        await js.publish(
            "sched.schedule",
            b"hello",
            schedule=api.MsgSchedule(at=at, target="sched.target"),
        )
        msg = await sub.next_msg(timeout=4)
        assert msg.data == b"hello"
        assert msg.headers[api.Header.SCHEDULER] == "sched.schedule"
        await nc.close()

    @async_test
    async def test_retry_on_no_responders(self):
        nc = await nats.connect()
        js = nc.jetstream()

        # Default: nats.go's DefaultPubRetryAttempts retries, each after
        # DefaultPubRetryWait.
        assert api.DEFAULT_PUB_RETRY_ATTEMPTS == 2
        assert api.DEFAULT_PUB_RETRY_WAIT == 0.25
        start = time.monotonic()
        with pytest.raises(NoStreamResponseError):
            await js.publish("retry.a", b"x")
        assert time.monotonic() - start >= 0.5

        # retry_attempts=0 disables the retries.
        start = time.monotonic()
        with pytest.raises(NoStreamResponseError):
            await js.publish("retry.a", b"x", retry_attempts=0)
        assert time.monotonic() - start < 0.2

        start = time.monotonic()
        with pytest.raises(NoStreamResponseError):
            await js.publish("retry.a", b"x", retry_attempts=2, retry_wait=0.2)
        assert time.monotonic() - start >= 0.4

        # The stream appears while the publish is retrying.
        async def add_stream():
            await asyncio.sleep(0.3)
            await js.add_stream(name="RETRY", subjects=["retry.>"])

        task = asyncio.create_task(add_stream())
        ack = await js.publish("retry.a", b"x", retry_attempts=-1, retry_wait=0.1, timeout=3)
        assert ack.stream == "RETRY"
        await task

        # Retrying never outlasts the timeout.
        with pytest.raises(nats.errors.TimeoutError):
            await js.publish("nostream", b"x", retry_attempts=-1, retry_wait=0.1, timeout=0.5)

        await nc.close()


class MsgAckTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_term_with_reason(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="TERM", subjects=["term"])
        await js.publish("term", b"x")

        advisories = await nc.subscribe("$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.TERM.dur")
        psub = await js.pull_subscribe("term", "dur", config=api.ConsumerConfig(ack_wait=0.5))
        msg = (await psub.fetch(1))[0]
        await msg.term_with_reason("cannot process")
        assert msg.is_acked
        with pytest.raises(nats.errors.MsgAlreadyAckdError):
            await msg.term_with_reason("again")

        advisory = json.loads((await advisories.next_msg(timeout=2)).data)
        assert advisory["reason"] == "cannot process"
        assert advisory["stream_seq"] == 1

        # Terminated messages are not redelivered.
        with pytest.raises(nats.errors.TimeoutError):
            await psub.fetch(1, timeout=1)
        await nc.close()

    @async_test
    async def test_ack_errors(self):
        nc = await nats.connect()

        no_reply = Msg(_client=nc, subject="foo")
        for method in (no_reply.ack, no_reply.nak, no_reply.term, no_reply.in_progress):
            with pytest.raises(nats.errors.MsgNoReplyError):
                await method()
        with pytest.raises(nats.errors.MsgNoReplyError):
            await no_reply.term_with_reason("x")
        # Still reported as not being a JetStream message.
        with pytest.raises(nats.errors.NotJSMessageError):
            no_reply.metadata
        assert str(nats.errors.MsgNoReplyError()) == "nats: message does not have a reply"

        unbound = Msg(_client=None, subject="foo", reply="$JS.ACK.S.C.1.1.1.1.0")
        for method in (unbound.ack, unbound.ack_sync, unbound.term, unbound.in_progress):
            with pytest.raises(nats.errors.MsgNotBoundError):
                await method()
        await nc.close()


class InvalidResponseTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_invalid_pub_ack(self):
        nc = await nats.connect()
        js = nc.jetstream()

        replies = {"bad.json": b"not json", "bad.empty": b"{}", "bad.list": b"[1]"}

        async def responder(msg):
            await msg.respond(replies[msg.subject])

        await nc.subscribe("bad.*", cb=responder)
        for subject in replies:
            with self.subTest(subject=subject):
                with pytest.raises(InvalidJSAckError) as err:
                    await js.publish(subject, b"x")
                assert str(err.value) == "nats: invalid jetstream publish response"
                future = await js.publish_async(subject, b"x")
                with pytest.raises(InvalidJSAckError):
                    await asyncio.wait_for(future, 1)
        await nc.close()

    @async_test
    async def test_publish_async_api_error(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ASYNCERR", subjects=["asyncerr"])

        future = await js.publish_async("asyncerr", b"x", stream="OTHER")
        with pytest.raises(BadRequestError) as err:
            await asyncio.wait_for(future, 1)
        assert err.value.err_code == 10060
        assert js.publish_async_pending() == 0
        await nc.close()

    @async_test
    async def test_invalid_api_response(self):
        nc = await nats.connect()

        async def responder(msg):
            await msg.respond(b"<html>")

        await nc.subscribe("fake.api.>", cb=responder)
        jsm = nc.jsm(prefix="fake.api")
        with pytest.raises(InvalidJetStreamResponseError) as err:
            await jsm.stream_info("FOO")
        assert str(err.value) == "nats: invalid jetstream api response"
        await nc.close()


class PublishAsyncTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_pub_ack_future(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="PAF", subjects=["paf"])

        future = await js.publish_async("paf", b"hello", headers={"X": "1"}, msg_id="id-1")
        assert isinstance(future, asyncio.Future)
        assert isinstance(future, nats.js.client.PubAckFuture)
        assert future.msg.subject == "paf"
        assert future.msg.data == b"hello"
        assert future.msg.headers == {"X": "1", api.Header.MSG_ID: "id-1"}
        ack = await future.ok()
        assert ack.seq == 1
        assert await future.err() is None
        assert future.result() is ack

        future = await js.publish_async("paf", b"x", stream="OTHER")
        err = await future.err()
        assert isinstance(err, APIError)
        with pytest.raises(APIError):
            await future.ok()
        await nc.close()

    @async_test
    async def test_ack_and_err_handlers(self):
        nc = await nats.connect()
        acks = []
        errs = []

        async def ack_handler(js, msg, ack):
            acks.append((js, msg, ack))

        def err_handler(js, msg, err):
            errs.append((js, msg, err))

        js = nc.jetstream(publish_async_ack_handler=ack_handler, publish_async_err_handler=err_handler)
        await js.add_stream(name="HANDLERS", subjects=["handlers"])
        ok = await js.publish_async("handlers", b"1")
        bad = await js.publish_async("handlers", b"2", stream="OTHER")
        await js.publish_async_complete(timeout=1)
        await asyncio.sleep(0.05)

        assert len(acks) == 1 and len(errs) == 1
        assert acks[0][0] is js
        assert acks[0][1] is ok.msg
        assert acks[0][2].seq == 1
        assert errs[0][1] is bad.msg
        assert isinstance(errs[0][2], APIError)
        await nc.close()

    @async_test
    async def test_timeout(self):
        nc = await nats.connect()
        errs = []

        async def err_handler(js, msg, err):
            errs.append(err)

        # A responder that never answers.
        await nc.subscribe("silent")
        js = nc.jetstream(publish_async_timeout=0.3, publish_async_err_handler=err_handler)
        assert js.options.publish_async_timeout == 0.3
        future = await js.publish_async("silent", b"x")
        assert js.publish_async_pending() == 1
        with pytest.raises(nats.errors.TimeoutError):
            await js.publish_async_complete(timeout=0.1)
        with pytest.raises(AsyncPublishTimeoutError) as err:
            await asyncio.wait_for(future, 1)
        assert str(err.value) == "nats: timeout waiting for ack"
        assert js.publish_async_pending() == 0
        await js.publish_async_complete(timeout=0.1)
        await asyncio.sleep(0.05)
        assert len(errs) == 1 and isinstance(errs[0], AsyncPublishTimeoutError)

        with pytest.raises(ValueError):
            nc.jetstream(publish_async_max_pending=0)
        await nc.close()

    @async_test
    async def test_cleanup_publisher(self):
        nc = await nats.connect()
        js = nc.jetstream(publish_async_max_pending=3)
        await js.add_stream(name="CLEANUP", subjects=["cleanup"])
        await nc.subscribe("silent")

        futures = [await js.publish_async("silent", b"x") for _ in range(3)]
        assert js.publish_async_pending() == 3
        with pytest.raises(TooManyStalledMsgsError):
            await js.publish_async("silent", b"x", wait_stall=0.1)

        await js.cleanup_publisher()
        assert js.publish_async_pending() == 0
        for future in futures:
            with pytest.raises(JetStreamPublisherClosedError):
                await future
        await js.publish_async_complete(timeout=0.1)

        # The publisher is set up again on the next publish, with capacity freed.
        future = await js.publish_async("cleanup", b"y")
        assert (await asyncio.wait_for(future, 1)).seq == 1
        await nc.close()

    @async_test
    async def test_publish_msg_async(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="PMA", subjects=["pma"])

        with pytest.raises(AsyncPublishReplySubjectSetError):
            await js.publish_msg_async(Msg(_client=nc, subject="pma", reply="inbox", data=b"x"))
        future = await js.publish_msg_async(Msg(_client=nc, subject="pma", data=b"x", headers={"A": "b"}), msg_id="1")
        ack = await asyncio.wait_for(future, 1)
        assert ack.seq == 1
        msg = await js.get_msg("PMA", 1)
        assert msg.headers["A"] == "b"
        await nc.close()

    @async_test
    async def test_retry_on_no_responders(self):
        nc = await nats.connect()
        js = nc.jetstream()

        # Resent DEFAULT_PUB_RETRY_ATTEMPTS times by default, as nats.go does.
        start = time.monotonic()
        future = await js.publish_async("aretry.a", b"x")
        with pytest.raises(NoStreamResponseError):
            await asyncio.wait_for(future, 2)
        assert future._retries == api.DEFAULT_PUB_RETRY_ATTEMPTS
        assert time.monotonic() - start >= 0.5

        # retry_attempts=0 fails on the first no responders.
        start = time.monotonic()
        future = await js.publish_async("aretry.a", b"x", retry_attempts=0)
        with pytest.raises(NoStreamResponseError):
            await asyncio.wait_for(future, 1)
        assert future._retries == 0
        assert time.monotonic() - start < 0.2

        # The stream appears while the default retries are pending.
        async def add_stream_soon():
            await asyncio.sleep(0.1)
            await js.add_stream(name="DRETRY", subjects=["dretry.>"])

        task = asyncio.create_task(add_stream_soon())
        future = await js.publish_async("dretry.a", b"x")
        ack = await asyncio.wait_for(future, 2)
        assert ack.stream == "DRETRY"
        await task

        async def add_stream():
            await asyncio.sleep(0.3)
            await js.add_stream(name="ARETRY", subjects=["aretry.>"])

        task = asyncio.create_task(add_stream())
        future = await js.publish_async("aretry.a", b"x", retry_attempts=20, retry_wait=0.1)
        ack = await asyncio.wait_for(future, 3)
        assert ack.stream == "ARETRY"
        await task
        await nc.close()


class StatusErrorsTest(SingleJetStreamServerTestCase):
    def test_status_error_classes(self):
        cases = [
            ("409", "Message Size Exceeds MaxBytes", MaxBytesExceededError),
            ("409", "Batch Completed", BatchCompletedError),
            ("409", "Consumer Deleted", ConsumerDeletedError),
            ("409", "Leadership Change", ConsumerLeadershipChangedError),
            ("409", "Server Shutdown", ServerShutdownError),
            ("409", "Consumer is push based", NotPullConsumerError),
            ("409", "Exceeded MaxWaiting", APIError),
            ("423", "Nats-Pin-Id mismatch", PinIdMismatchError),
            ("400", "Bad Request", APIError),
        ]
        for code, desc, cls in cases:
            with self.subTest(desc=desc):
                msg = Msg(_client=None, headers={api.Header.STATUS: code, api.Header.DESCRIPTION: desc})
                with pytest.raises(APIError) as err:
                    APIError.from_msg(msg)
                assert type(err.value) is cls
                assert err.value.code == int(code)
                assert err.value.description == desc
        assert str(NoHeartbeatError()) == "nats: no heartbeat received"
        assert str(MsgIteratorClosedError()) == "nats: messages iterator closed"

    def test_invalid_option_error(self):
        import nats.js.jetstreamext as jetstreamext

        # One ErrInvalidOption: jetstreamext's is the JetStream client's,
        # keeping its own wording, and both are ValueErrors.
        assert issubclass(InvalidOptionError, ValueError)
        assert issubclass(jetstreamext.InvalidOptionError, InvalidOptionError)
        assert str(InvalidOptionError()) == "nats: invalid jetstream option"
        assert str(jetstreamext.InvalidOptionError("x")) == "nats: invalid option: x"
        with pytest.raises(InvalidOptionError):
            jetstreamext.new_fast_publisher(None, jetstreamext.FastPublishFlowControl(ack_timeout=-1))

    @async_test
    async def test_fetch_consumer_deleted_error(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="CDEL", subjects=["cdel"])
        await js.add_consumer("CDEL", durable_name="dur", ack_policy="explicit")
        sub = await js.pull_subscribe_bind("dur", "CDEL")

        fetch = asyncio.create_task(sub.fetch(1, timeout=5))
        await asyncio.sleep(0.5)
        await js.delete_consumer("CDEL", "dur")
        with pytest.raises(ConsumerDeletedError) as err:
            await asyncio.wait_for(fetch, timeout=2)
        assert err.value.code == 409
        await nc.close()


class PullConsumerFetchTest(SingleJetStreamServerTestCase):
    async def _setup(self, n=5, payload=b"x", **config):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="PULL", subjects=["pull.>"])
        for i in range(n):
            await js.publish(f"pull.{i}", payload)
        await js.add_consumer("PULL", durable_name="dur", ack_policy="explicit", **config)
        consumer = await js.pull_consumer("PULL", "dur")
        return nc, js, consumer

    @async_test
    async def test_fetch(self):
        nc, js, consumer = await self._setup()
        assert consumer.cached_info().name == "dur"
        assert consumer.cached_info().num_pending == 5

        batch = await consumer.fetch(3)
        msgs = [msg async for msg in batch]
        assert [m.subject for m in msgs] == ["pull.0", "pull.1", "pull.2"]
        assert batch.done and batch.error is None
        for msg in msgs:
            await msg.ack_sync()

        # Fewer messages than asked for: the batch ends when the pull expires.
        start = time.monotonic()
        batch = await consumer.fetch(10, max_wait=0.5)
        msgs = [msg async for msg in batch]
        assert len(msgs) == 2
        assert batch.error is None
        assert 0.4 < time.monotonic() - start < 2
        for msg in msgs:
            await msg.ack_sync()

        # The cached info is only updated by info().
        assert consumer.cached_info().num_pending == 5
        info = await consumer.info()
        assert info.num_pending == 0 and info.num_ack_pending == 0
        assert consumer.cached_info() is info
        await nc.close()

    @async_test
    async def test_fetch_no_wait(self):
        nc, js, consumer = await self._setup(n=2)
        start = time.monotonic()
        batch = await consumer.fetch_no_wait(10)
        msgs = [msg async for msg in batch]
        assert len(msgs) == 2 and batch.error is None
        batch = await consumer.fetch_no_wait(10)
        assert [msg async for msg in batch] == []
        assert batch.error is None
        assert time.monotonic() - start < 0.5
        await nc.close()

    @async_test
    async def test_fetch_bytes(self):
        nc, js, consumer = await self._setup(n=5, payload=b"a" * 1000)
        batch = await consumer.fetch_bytes(2500, max_wait=1)
        msgs = [msg async for msg in batch]
        # The third message would exceed the bytes left: the batch ends quietly.
        assert len(msgs) == 2
        assert batch.error is None
        await nc.close()

    @async_test
    async def test_next(self):
        nc, js, consumer = await self._setup(n=1)
        msg = await consumer.next()
        assert msg.subject == "pull.0"
        await msg.ack()
        with pytest.raises(nats.errors.TimeoutError):
            await consumer.next(max_wait=0.3)
        await nc.close()

    @async_test
    async def test_consumer_deleted(self):
        nc, js, consumer = await self._setup(n=0)
        batch = await consumer.fetch(1, max_wait=3)
        await asyncio.sleep(0.3)
        await js.delete_consumer("PULL", "dur")
        msgs = [msg async for msg in batch]
        assert msgs == []
        assert isinstance(batch.error, ConsumerDeletedError)
        with pytest.raises(ConsumerDeletedError):
            raise batch.error
        await nc.close()

    @async_test
    async def test_invalid_options(self):
        nc, js, consumer = await self._setup(n=0)
        for kwargs in ({"batch": 0}, {"batch": 1, "max_wait": 0}, {"batch": 1, "priority": 10}):
            with pytest.raises(InvalidOptionError):
                await consumer.fetch(**kwargs)
        with pytest.raises(InvalidOptionError) as err:
            await consumer.fetch(1, max_wait=1, heartbeat=0.6)
        # Worded as nats.go wraps ErrInvalidOption, and still a ValueError.
        assert str(err.value) == (
            "nats: invalid jetstream option: the value of heartbeat must be less than 50% of expiry"
        )
        assert isinstance(err.value, ValueError)
        with pytest.raises(InvalidOptionError):
            await consumer.fetch_bytes(0)

        await js.add_consumer("PULL", durable_name="push", deliver_subject="deliver")
        with pytest.raises(NotPullConsumerError):
            await js.pull_consumer("PULL", "push")
        with pytest.raises(NotFoundError):
            await js.pull_consumer("PULL", "missing")
        await nc.close()

    @async_test
    async def test_fetch_pinned(self):
        nc, js, consumer = await self._setup(
            n=2, priority_policy=api.PriorityPolicy.PINNED, priority_groups=["A"], priority_timeout=5
        )
        other = await js.pull_consumer("PULL", "dur")

        batch = await consumer.fetch(1, max_wait=1, group="A")
        msgs = [m async for m in batch]
        assert len(msgs) == 1
        assert consumer.pin_id
        assert msgs[0].headers[api.Header.PIN_ID] == consumer.pin_id

        # Another client is not pinned and gets nothing.
        batch = await other.fetch(1, max_wait=0.5, group="A")
        assert [m async for m in batch] == []
        assert other.pin_id is None

        batch = await consumer.fetch(1, max_wait=1, group="A")
        assert len([m async for m in batch]) == 1
        await nc.close()


class PullConsumeTest(SingleJetStreamServerTestCase):
    async def _setup(self, n=10, payload=b"x", **config):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="CONS", subjects=["cons.>"])
        for i in range(n):
            await js.publish(f"cons.{i}", payload)
        await js.add_consumer("CONS", durable_name="dur", ack_policy="explicit", **config)
        consumer = await js.pull_consumer("CONS", "dur")
        return nc, js, consumer

    async def _silent_consumer(self, nc):
        # A consumer handle whose pulls reach a subscriber that never
        # answers: no messages, no heartbeats.
        await nc.subscribe("silent.api.>")
        js = nc.jetstream(prefix="silent.api")
        return nats.js.consume.PullConsumer(js, "S", "C")

    @async_test
    async def test_consume(self):
        nc, js, consumer = await self._setup()
        received = []
        done = asyncio.Event()

        async def cb(msg):
            received.append(msg.subject)
            await msg.ack()
            if len(received) == 10:
                done.set()

        ctx = await consumer.consume(cb, max_messages=3)
        await asyncio.wait_for(done.wait(), 3)
        assert received == [f"cons.{i}" for i in range(10)]
        assert not ctx.is_closed
        ctx.stop()
        await asyncio.wait_for(ctx.closed(), 1)
        assert ctx.is_closed
        await nc.close()

    @async_test
    async def test_consume_by_bytes(self):
        for kwargs in ({"max_bytes": 3000}, {"max_messages": 4, "bytes_limit": 2500}):
            with self.subTest(**kwargs):
                nc, js, consumer = await self._setup(payload=b"a" * 1000)
                received = []
                done = asyncio.Event()

                async def cb(msg):
                    received.append(msg)
                    await msg.ack()
                    if len(received) == 10:
                        done.set()

                ctx = await consumer.consume(cb, **kwargs)
                await asyncio.wait_for(done.wait(), 4)
                ctx.stop()
                await ctx.closed()
                await js.delete_stream("CONS")
                await nc.close()

    @async_test
    async def test_consume_stop_after(self):
        nc, js, consumer = await self._setup()
        received = []

        async def cb(msg):
            received.append(msg)
            await msg.ack()

        ctx = await consumer.consume(cb, max_messages=3, stop_after=4)
        await asyncio.wait_for(ctx.closed(), 3)
        assert len(received) == 4
        info = await consumer.info()
        assert info.num_pending == 6
        await nc.close()

    @async_test
    async def test_consume_drain(self):
        nc, js, consumer = await self._setup()
        received = []

        async def cb(msg):
            received.append(msg)
            if len(received) == 1:
                ctx.drain()
            await asyncio.sleep(0.01)

        ctx = await consumer.consume(cb, max_messages=5)
        await asyncio.wait_for(ctx.closed(), 3)
        # The messages of the first pull were all handled, and no more pulled.
        assert len(received) == 5
        await nc.close()

    @async_test
    async def test_consume_errors(self):
        nc, js, consumer = await self._setup(n=0)
        errors = []

        def error_cb(ctx, err):
            errors.append((ctx, err))

        async def cb(msg):
            pass

        ctx = await consumer.consume(cb, error_cb=error_cb)
        await asyncio.sleep(0.2)
        await js.delete_consumer("CONS", "dur")
        await asyncio.wait_for(ctx.closed(), 3)
        assert len(errors) == 1
        assert errors[0][0] is ctx
        assert isinstance(errors[0][1], ConsumerDeletedError)
        await nc.close()

    @async_test
    async def test_consume_missing_heartbeat(self):
        nc = await nats.connect()
        consumer = await self._silent_consumer(nc)
        errors = []

        async def error_cb(ctx, err):
            errors.append(err)

        async def cb(msg):
            pass

        ctx = await consumer.consume(cb, expires=1, heartbeat=0.5, error_cb=error_cb)
        await asyncio.sleep(1.4)
        assert len(errors) == 1
        assert isinstance(errors[0], NoHeartbeatError)
        # Consuming goes on with a new pull.
        assert not ctx.is_closed
        ctx.stop()
        await ctx.closed()
        await nc.close()

    @async_test
    async def test_messages(self):
        nc, js, consumer = await self._setup()
        msgs = await consumer.messages(max_messages=4)
        received = []
        async for msg in msgs:
            received.append(msg.subject)
            await msg.ack()
            if len(received) == 10:
                msgs.stop()
        assert received == [f"cons.{i}" for i in range(10)]
        with pytest.raises(MsgIteratorClosedError):
            await msgs.next()

        msgs = await consumer.messages()
        with pytest.raises(nats.errors.TimeoutError):
            await msgs.next(timeout=0.3)
        msgs.stop()
        await nc.close()

    @async_test
    async def test_messages_stop_after_and_drain(self):
        nc, js, consumer = await self._setup()
        msgs = await consumer.messages(max_messages=4, stop_after=3)
        received = [msg async for msg in msgs]
        assert len(received) == 3
        for msg in received:
            await msg.ack()

        msgs = await consumer.messages(max_messages=4)
        first = await msgs.next(timeout=1)
        await asyncio.sleep(0.2)
        msgs.drain()
        # The rest of the first pull is still delivered after draining.
        rest = [msg async for msg in msgs]
        assert [m.metadata.sequence.stream for m in [first] + rest] == [4, 5, 6, 7]
        await nc.close()

    @async_test
    async def test_fetch_missing_heartbeat(self):
        nc = await nats.connect()
        consumer = await self._silent_consumer(nc)
        start = time.monotonic()
        batch = await consumer.fetch(1, max_wait=3, heartbeat=0.5)
        assert [m async for m in batch] == []
        assert isinstance(batch.error, NoHeartbeatError)
        assert 0.9 < time.monotonic() - start < 1.5
        with pytest.raises(NoHeartbeatError):
            await consumer.next(max_wait=3, heartbeat=0.5)
        await nc.close()

    @async_test
    async def test_messages_missing_heartbeat(self):
        nc = await nats.connect()
        consumer = await self._silent_consumer(nc)
        msgs = await consumer.messages(expires=1, heartbeat=0.5)
        start = time.monotonic()
        with pytest.raises(NoHeartbeatError):
            await msgs.next()
        assert 0.9 < time.monotonic() - start < 1.5
        msgs.stop()

        msgs = await consumer.messages(expires=1, heartbeat=0.5, err_on_missing_heartbeat=False)
        with pytest.raises(nats.errors.TimeoutError):
            await msgs.next(timeout=1.5)
        msgs.stop()
        await nc.close()

    @async_test
    async def test_invalid_options(self):
        nc, js, consumer = await self._setup(n=0)

        async def cb(msg):
            pass

        with pytest.raises(HandlerRequiredError):
            await consumer.consume(None)
        for kwargs in (
            {"max_messages": 10, "max_bytes": 100},
            {"max_bytes": 100, "bytes_limit": 100},
            {"max_messages": 0},
            {"expires": 0.5},
            {"heartbeat": 0.1},
            {"expires": 2, "heartbeat": 1.5},
            {"stop_after": 0},
            {"group": "A"},
        ):
            with self.subTest(**kwargs):
                with pytest.raises(InvalidOptionError):
                    await consumer.consume(cb, **kwargs)
                with pytest.raises(InvalidOptionError):
                    await consumer.messages(**kwargs)

        await js.add_consumer(
            "CONS", durable_name="grouped", priority_policy=api.PriorityPolicy.OVERFLOW, priority_groups=["A"]
        )
        grouped = await js.pull_consumer("CONS", "grouped")
        for group in (None, "B"):
            with pytest.raises(InvalidOptionError):
                await grouped.messages(group=group)
        msgs = await grouped.messages(group="A")
        msgs.stop()
        await nc.close()


class OrderedConsumerTest(SingleJetStreamServerTestCase):
    async def _setup(self, n=10):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ORD", subjects=["ord.>"])
        for i in range(n):
            await js.publish(f"ord.{i % 2}", str(i).encode())
        return nc, js

    @async_test
    async def test_messages(self):
        nc, js = await self._setup()
        oc = await js.ordered_consumer("ORD", nats.js.consume.OrderedConsumerConfig(name_prefix="pfx"))
        assert oc.cached_info().name == "pfx_1"
        assert oc.cached_info().config.ack_policy == api.AckPolicy.NONE
        msgs = await oc.messages()
        got = [(await msgs.next(timeout=2)).data for _ in range(10)]
        assert got == [str(i).encode() for i in range(10)]

        # The consumer is deleted: it is recreated after the last message.
        await js.delete_consumer("ORD", oc.cached_info().name)
        for i in range(10, 15):
            await js.publish("ord.0", str(i).encode())
        got = [(await msgs.next(timeout=4)).data for _ in range(5)]
        assert got == [str(i).encode() for i in range(10, 15)]
        assert oc.cached_info().name.startswith("pfx_")
        assert oc.cached_info().name != "pfx_1"
        msgs.stop()
        with pytest.raises(MsgIteratorClosedError):
            await msgs.next()
        await nc.close()

    @async_long_test
    async def test_messages_stop_after_and_config(self):
        nc, js = await self._setup()
        config = nats.js.consume.OrderedConsumerConfig(
            filter_subjects=["ord.1"],
            deliver_policy=api.DeliverPolicy.BY_START_SEQUENCE,
            opt_start_seq=5,
        )
        oc = await js.ordered_consumer("ORD", config)
        msgs = await oc.messages(stop_after=2)
        got = [msg.data async for msg in msgs]
        assert got == [b"5", b"7"]
        await nc.close()

    @async_test
    async def test_config_metadata_start_time_and_replay(self):
        # nats.go OrderedConsumerConfig.Metadata, OptStartTime (with
        # DeliverByStartTimePolicy) and ReplayPolicy reach the consumer.
        nc, js = await self._setup(n=3)
        await asyncio.sleep(0.2)
        start = datetime.datetime.now(datetime.timezone.utc)
        await asyncio.sleep(0.2)
        for i in range(3, 6):
            await js.publish("ord.0", str(i).encode())

        config = nats.js.consume.OrderedConsumerConfig(
            deliver_policy=api.DeliverPolicy.BY_START_TIME,
            opt_start_time=start,
            replay_policy=api.ReplayPolicy.ORIGINAL,
            metadata={"owner": "parity", "kind": "ordered"},
        )
        oc = await js.ordered_consumer("ORD", config)
        info = await js.consumer_info("ORD", oc.cached_info().name)
        assert info.config.deliver_policy == api.DeliverPolicy.BY_START_TIME
        assert info.config.opt_start_time == start
        assert info.config.opt_start_seq is None
        assert info.config.replay_policy == api.ReplayPolicy.ORIGINAL
        # The server adds its own _nats.* entries.
        assert {k: v for k, v in info.config.metadata.items() if not k.startswith("_nats.")} == config.metadata

        # Only the messages published after the start time are delivered.
        batch = await oc.fetch(10, max_wait=1)
        assert [int(m.data) async for m in batch] == [3, 4, 5]

        # A recreated consumer resumes by sequence, keeping metadata and
        # the replay policy.
        await js.delete_consumer("ORD", oc.cached_info().name)
        await js.publish("ord.0", b"6")
        batch = await oc.fetch(1, max_wait=2)
        assert [int(m.data) async for m in batch] == [6]
        info = await js.consumer_info("ORD", oc.cached_info().name)
        assert info.config.deliver_policy == api.DeliverPolicy.BY_START_SEQUENCE
        assert info.config.opt_start_seq == 7
        assert info.config.opt_start_time is None
        assert info.config.replay_policy == api.ReplayPolicy.ORIGINAL
        assert info.config.metadata["owner"] == "parity"
        await nc.close()

    @async_long_test
    async def test_consume(self):
        nc, js = await self._setup()
        oc = await js.ordered_consumer("ORD")
        received = []
        errors = []
        first = asyncio.Event()
        done = asyncio.Event()

        async def cb(msg):
            received.append(int(msg.data))
            if len(received) == 10:
                first.set()
            if len(received) == 15:
                done.set()

        async def error_cb(ctx, err):
            errors.append(err)

        ctx = await oc.consume(cb, error_cb=error_cb)
        await asyncio.wait_for(first.wait(), 3)
        await js.delete_consumer("ORD", oc.cached_info().name)
        for i in range(10, 15):
            await js.publish("ord.0", str(i).encode())
        await asyncio.wait_for(done.wait(), 5)
        assert received == list(range(15))
        assert any(isinstance(err, ConsumerDeletedError) for err in errors)

        with pytest.raises(OrderedConsumerConcurrentRequestsError):
            await oc.consume(cb)
        with pytest.raises(OrderConsumerUsedAsConsumeError):
            await oc.fetch(1)
        ctx.stop()
        await asyncio.wait_for(ctx.closed(), 2)
        assert ctx.is_closed
        await nc.close()

    @async_test
    async def test_consume_stop_after(self):
        nc, js = await self._setup()
        oc = await js.ordered_consumer("ORD")
        received = []

        async def cb(msg):
            received.append(int(msg.data))

        ctx = await oc.consume(cb, stop_after=4)
        await asyncio.wait_for(ctx.closed(), 3)
        assert received == [0, 1, 2, 3]
        await nc.close()

    @async_test
    async def test_fetch(self):
        nc, js = await self._setup()
        oc = await js.ordered_consumer("ORD")
        batch = await oc.fetch(4, max_wait=1)
        assert [int(m.data) async for m in batch] == [0, 1, 2, 3]
        batch = await oc.fetch(4, max_wait=1)
        assert [int(m.data) async for m in batch] == [4, 5, 6, 7]
        msg = await oc.next(max_wait=1)
        assert msg.data == b"8"
        batch = await oc.fetch_no_wait(5)
        assert [int(m.data) async for m in batch] == [9]
        batch = await oc.fetch_bytes(1000, max_wait=1)
        assert [m async for m in batch] == []

        running = await oc.fetch(1, max_wait=2)
        with pytest.raises(OrderedConsumerConcurrentRequestsError):
            await oc.fetch(1)
        assert [m async for m in running] == []

        with pytest.raises(OrderConsumerUsedAsFetchError):
            await oc.messages()
        await nc.close()

    @async_test
    async def test_recreate_fails(self):
        nc, js = await self._setup()
        oc = await js.ordered_consumer("ORD", nats.js.consume.OrderedConsumerConfig(max_reset_attempts=1))
        assert (await oc.info()).name == oc.cached_info().name
        await js.delete_stream("ORD")
        # nats.go ErrOrderedConsumerReset, chained from the last attempt's error.
        with pytest.raises(OrderedConsumerResetError) as err:
            await oc.fetch(1)
        assert isinstance(err.value.__cause__, StreamNotFoundError)
        assert err.value.api_error is err.value.__cause__
        assert err.value.api_error.err_code == 10059
        assert str(err.value).startswith("nats: recreating ordered consumer: StreamNotFoundError")
        assert oc.cached_info() is None
        with pytest.raises(OrderedConsumerNotCreatedError):
            await oc.info()
        assert str(OrderedConsumerResetError()) == "nats: recreating ordered consumer"
        await nc.close()

    @async_test
    async def test_consume_recreate_fails(self):
        nc, js = await self._setup()
        oc = await js.ordered_consumer("ORD", nats.js.consume.OrderedConsumerConfig(max_reset_attempts=2))
        received = []
        errors = []

        async def cb(msg):
            received.append(msg)

        async def error_cb(ctx, err):
            errors.append(err)

        cc = await oc.consume(cb, error_cb=error_cb, expires=2)
        for _ in range(50):
            if len(received) == 10:
                break
            await asyncio.sleep(0.1)
        await js.delete_stream("ORD")
        # One failed attempt and a 1s backoff before the last one.
        await asyncio.wait_for(cc.closed(), 10)
        resets = [e for e in errors if isinstance(e, OrderedConsumerResetError)]
        assert len(resets) == 1
        assert isinstance(resets[0].__cause__, StreamNotFoundError)
        await nc.close()


class PushConsumerTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_consume(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="PUSH", subjects=["push.>"])
        for i in range(5):
            await js.publish("push.a", str(i).encode())
        await js.add_consumer(
            "PUSH", durable_name="dur", deliver_subject="deliver.push", ack_policy="explicit", idle_heartbeat=0.5
        )
        consumer = await js.push_consumer("PUSH", "dur")
        assert consumer.cached_info().name == "dur"

        received = []
        done = asyncio.Event()

        async def cb(msg):
            received.append(int(msg.data))
            await msg.ack()
            if len(received) == 5:
                done.set()

        ctx = await consumer.consume(cb)
        await asyncio.wait_for(done.wait(), 2)
        assert received == list(range(5))
        with pytest.raises(ConsumerAlreadyConsumingError):
            await consumer.consume(cb)
        info = await consumer.info()
        assert info.num_ack_pending == 0
        assert consumer.cached_info() is info

        ctx.stop()
        await asyncio.wait_for(ctx.closed(), 1)
        # Once stopped, it can be consumed again.
        ctx = await consumer.consume(cb)
        ctx.drain()
        await asyncio.wait_for(ctx.closed(), 1)

        await js.add_consumer("PUSH", durable_name="pull")
        with pytest.raises(NotPushConsumerError):
            await js.push_consumer("PUSH", "pull")
        await nc.close()

    @async_test
    async def test_flow_control(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="FC", subjects=["fc"])
        payload = b"x" * 64 * 1024
        for _ in range(100):
            await js.publish("fc", payload)
        await js.add_consumer(
            "FC",
            durable_name="dur",
            deliver_subject="deliver.fc",
            ack_policy="none",
            flow_control=True,
            idle_heartbeat=1,
        )
        consumer = await js.push_consumer("FC", "dur")
        received = []
        done = asyncio.Event()

        async def cb(msg):
            received.append(msg)
            if len(received) == 100:
                done.set()

        ctx = await consumer.consume(cb)
        await asyncio.wait_for(done.wait(), 4)
        ctx.stop()
        await nc.close()

    @async_test
    async def test_missing_heartbeat(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="HB", subjects=["hb"])
        info = await js.add_consumer("HB", durable_name="dur", deliver_subject="deliver.hb", idle_heartbeat=0.5)
        # Listen where the heartbeats do not go.
        info.config.deliver_subject = "elsewhere"
        consumer = nats.js.consume.PushConsumer(js, "HB", "dur", info)
        errors = []

        async def cb(msg):
            pass

        ctx = await consumer.consume(cb, error_cb=lambda ctx, err: errors.append(err))
        await asyncio.sleep(1.3)
        assert len(errors) == 1 and isinstance(errors[0], NoHeartbeatError)
        ctx.stop()
        await nc.close()
