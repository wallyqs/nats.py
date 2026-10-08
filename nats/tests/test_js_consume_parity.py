import asyncio
import datetime
import time
import unittest

import nats
import nats.js.api
import pytest
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
