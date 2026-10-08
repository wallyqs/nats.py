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
