import asyncio
import json
import datetime
import time
import unittest

import nats
import nats.js.api
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

        # Default: no retries.
        start = time.monotonic()
        with pytest.raises(NoStreamResponseError):
            await js.publish("retry.a", b"x")
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

        future = await js.publish_async("aretry.a", b"x")
        with pytest.raises(NoStreamResponseError):
            await asyncio.wait_for(future, 1)

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
