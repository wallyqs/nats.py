import asyncio
import datetime
import json

import pytest
from nats.errors import *
from nats.js import jetstreamext
from nats.js.errors import *
from nats.js.jetstreamext import *

import nats
from tests.utils import *


async def _raw_error(nc, subject, headers=None, reply_suffix=None):
    """Publishes to subject and returns the error object of the server's reply."""
    if reply_suffix is None:
        msg = await nc.request(subject, b"x", timeout=2, headers=headers)
        return json.loads(msg.data)["error"]
    inbox = nc.new_inbox()
    sub = await nc.subscribe(inbox + ".>")
    await nc.publish(subject, b"x", reply=inbox + reply_suffix, headers=headers)
    msg = await sub.next_msg(timeout=2)
    await sub.unsubscribe()
    return json.loads(msg.data)["error"]


class BatchErrorIdentityTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_server_batch_errors_map_to_classes(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="PLAIN", subjects=["plain.>"])
        await js.add_stream(name="ATOM", subjects=["atom.>"], allow_atomic=True)
        await js.add_stream(name="FAST", subjects=["fast.>"], allow_batched=True)

        cases = [
            (
                ("plain.a", {BATCH_ID_HEADER: "b1", BATCH_SEQ_HEADER: "1"}),
                BatchPublishNotEnabledError,
                JS_ERR_CODE_BATCH_PUBLISH_NOT_ENABLED,
            ),
            (("atom.a", {BATCH_ID_HEADER: "b2"}), BatchPublishMissingSeqError, JS_ERR_CODE_BATCH_PUBLISH_MISSING_SEQ),
            (
                ("atom.a", {BATCH_ID_HEADER: "b" * 65, BATCH_SEQ_HEADER: "1"}),
                BatchPublishInvalidIDError,
                JS_ERR_CODE_BATCH_PUBLISH_INVALID_ID,
            ),
            (
                ("atom.a", {BATCH_ID_HEADER: "b3", BATCH_SEQ_HEADER: "1", BATCH_COMMIT_HEADER: "nope"}),
                BatchPublishInvalidCommitError,
                JS_ERR_CODE_BATCH_PUBLISH_INVALID_COMMIT,
            ),
            (
                ("atom.a", {BATCH_ID_HEADER: "b4", BATCH_SEQ_HEADER: "1001"}),
                BatchPublishExceedsLimitError,
                JS_ERR_CODE_BATCH_PUBLISH_EXCEEDS_LIMIT,
            ),
        ]
        for (subject, headers), cls, err_code in cases:
            err = api_error_from(await _raw_error(nc, subject, headers))
            assert type(err) is cls, (subject, headers, err)
            assert isinstance(err, BadRequestError)
            assert err.code == 400
            assert err.err_code == err_code

        # A batch that skips a sequence is incomplete.
        msg = await nc.request("atom.a", b"1", timeout=2, headers={BATCH_ID_HEADER: "b6", BATCH_SEQ_HEADER: "1"})
        assert msg.data == b""
        err = api_error_from(
            await _raw_error(
                nc, "atom.a", {BATCH_ID_HEADER: "b6", BATCH_SEQ_HEADER: "3", BATCH_COMMIT_HEADER: BATCH_COMMIT_FINAL}
            )
        )
        assert isinstance(err, BatchPublishIncompleteError)
        assert err.err_code == JS_ERR_CODE_BATCH_PUBLISH_INCOMPLETE

        # A message ID used twice in a batch.
        msg = await nc.request(
            "atom.a", b"1", timeout=2, headers={BATCH_ID_HEADER: "b7", BATCH_SEQ_HEADER: "1", "Nats-Msg-Id": "m"}
        )
        assert msg.data == b""
        err = api_error_from(
            await _raw_error(
                nc,
                "atom.a",
                {BATCH_ID_HEADER: "b7", BATCH_SEQ_HEADER: "2", BATCH_COMMIT_HEADER: "1", "Nats-Msg-Id": "m"},
            )
        )
        assert isinstance(err, BatchPublishDuplicateMsgIDError)
        assert err.err_code == JS_ERR_CODE_BATCH_PUBLISH_DUPLICATE_MSG_ID

        # Headers a batch does not support are refused when it commits.
        err = api_error_from(
            await _raw_error(
                nc,
                "atom.a",
                {
                    BATCH_ID_HEADER: "b8",
                    BATCH_SEQ_HEADER: "1",
                    BATCH_COMMIT_HEADER: "1",
                    "Nats-Expected-Last-Msg-Id": "x",
                },
            )
        )
        assert isinstance(err, BatchPublishUnsupportedHeaderError)
        assert err.err_code == JS_ERR_CODE_BATCH_PUBLISH_UNSUPPORTED_HEADER

        # Fast-ingest batches: <inbox>.<flow>.<gap>.<seq>.<op>.$FI
        fast_cases = [
            ("plain.a", ".10.fail.1.0.$FI", FastBatchNotEnabledError, JS_ERR_CODE_FAST_BATCH_NOT_ENABLED),
            ("fast.a", ".10.fail.1.9.$FI", FastBatchInvalidPatternError, JS_ERR_CODE_FAST_BATCH_INVALID_PATTERN),
            (
                "fast.a",
                ".10.maybe.1.0.$FI",
                BatchPublishInvalidGapModeError,
                JS_ERR_CODE_BATCH_PUBLISH_INVALID_GAP_MODE,
            ),
            ("fast.a", ".10.fail.2.1.$FI", FastBatchUnknownIDError, JS_ERR_CODE_FAST_BATCH_UNKNOWN_ID),
        ]
        for subject, suffix, cls, err_code in fast_cases:
            err = api_error_from(await _raw_error(nc, subject, reply_suffix=suffix))
            assert type(err) is cls, (subject, suffix, err)
            assert err.err_code == err_code

        # The batch ID of a fast batch is its reply inbox: a long one is invalid.
        inbox = "_INBOX." + "x" * 70
        sub = await nc.subscribe(inbox + ".>")
        await nc.publish("fast.a", b"x", reply=inbox + ".10.fail.1.0.$FI")
        err = api_error_from(json.loads((await sub.next_msg(timeout=2)).data)["error"])
        assert isinstance(err, FastBatchInvalidIDError)
        assert err.err_code == JS_ERR_CODE_FAST_BATCH_INVALID_ID

        # Other errors keep their usual classes.
        err = api_error_from({"code": 404, "err_code": 10059, "description": "stream not found"})
        assert isinstance(err, NotFoundError)
        assert type(err).__module__ == "nats.js.errors"
        err = api_error_from({"code": 429, "err_code": 10210, "description": "atomic publish too many inflight"})
        assert isinstance(err, AtomicPublishTooManyInflightError)
        err = api_error_from({"code": 429, "err_code": 10211, "description": "batch publish too many inflight"})
        assert isinstance(err, BatchPublishTooManyInflightError)
        await nc.close()

    def test_error_codes_follow_nats_server(self):
        assert JS_ERR_CODE_BATCH_PUBLISH_NOT_ENABLED == 10174
        assert JS_ERR_CODE_BATCH_PUBLISH_MISSING_SEQ == 10175
        assert JS_ERR_CODE_BATCH_PUBLISH_INCOMPLETE == 10176
        assert JS_ERR_CODE_BATCH_PUBLISH_UNSUPPORTED_HEADER == 10177
        assert JS_ERR_CODE_BATCH_PUBLISH_INVALID_ID == 10179
        assert JS_ERR_CODE_BATCH_PUBLISH_EXCEEDS_LIMIT == 10199
        assert JS_ERR_CODE_BATCH_PUBLISH_INVALID_COMMIT == 10200
        assert JS_ERR_CODE_BATCH_PUBLISH_DUPLICATE_MSG_ID == 10201
        assert JS_ERR_CODE_FAST_BATCH_NOT_ENABLED == 10205
        assert JS_ERR_CODE_FAST_BATCH_INVALID_PATTERN == 10206
        assert JS_ERR_CODE_FAST_BATCH_INVALID_ID == 10207
        assert JS_ERR_CODE_FAST_BATCH_UNKNOWN_ID == 10208
        assert JS_ERR_CODE_ATOMIC_PUBLISH_TOO_MANY_INFLIGHT == 10210
        assert JS_ERR_CODE_BATCH_PUBLISH_TOO_MANY_INFLIGHT == 10211
        assert BATCH_COMMIT_EOB == "eob"

    def test_client_error_texts(self):
        assert str(BatchClosedError()) == "nats: batch publisher closed"
        assert str(EmptyBatchError()) == "nats: no messages in batch"
        assert str(InvalidBatchAckError()) == "nats: invalid jetstream batch publish response"
        assert str(FastBatchGapDetectedError()) == "nats: fast batch gap detected"
        assert (
            str(FastBatchGapDetectedError(4, 6))
            == "nats: fast batch gap detected: expected last sequence 4; current sequence 6"
        )
        assert str(InvalidOptionError("max bytes has to be greater than 0")) == (
            "nats: invalid option: max bytes has to be greater than 0"
        )
        assert str(InvalidOptionError()) == "nats: invalid option"
        assert str(BatchUnsupportedError()) == "nats: batch get not supported by server"
        assert str(InvalidResponseError("missing stream header")) == (
            "nats: invalid stream response: missing stream header"
        )
        assert str(NoMessagesError()) == "nats: no messages"
        assert isinstance(NoMessagesError(), NotFoundError)
        assert str(SubjectRequiredError()) == "nats: at least one subject is required"
        assert isinstance(BatchAckTimeoutError(3, 1), TimeoutError)
        assert str(BatchAckTimeoutError(3, 1)) == "nats: batch message 3 ack timeout; current ack sequence: 1"


class BatchPublisherTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_add_and_commit(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ORDERS", subjects=["orders.>"], allow_atomic=True)

        batch = new_batch_publisher(js)
        assert batch.size() == 0
        assert not batch.is_closed()
        await batch.add("orders.new", b"1")
        await batch.add_msg(nats.aio.msg.Msg(nc, subject="orders.new", data=b"2", headers={"X-Custom": "a"}))
        assert batch.size() == 2
        # Nothing is stored before the commit.
        info = await js.stream_info("ORDERS")
        assert info.state.messages == 0

        ack = await batch.commit("orders.new", b"3")
        assert isinstance(ack, BatchAck)
        assert ack.stream == "ORDERS"
        assert ack.seq == 3
        assert ack.batch_id == batch.batch_id
        assert ack.batch_size == 3
        assert batch.size() == 3
        assert batch.is_closed()

        info = await js.stream_info("ORDERS")
        assert info.state.messages == 3
        msg = await js.get_msg("ORDERS", 2)
        assert msg.data == b"2"
        assert msg.headers["X-Custom"] == "a"
        assert msg.headers[BATCH_ID_HEADER] == batch.batch_id
        assert msg.headers[BATCH_SEQ_HEADER] == "2"
        msg = await js.get_msg("ORDERS", 3)
        assert msg.headers[BATCH_COMMIT_HEADER] == "1"

        with pytest.raises(BatchClosedError):
            await batch.add("orders.new", b"4")
        with pytest.raises(BatchClosedError):
            await batch.commit("orders.new", b"4")
        with pytest.raises(BatchClosedError):
            await batch.close()
        with pytest.raises(BatchClosedError):
            batch.discard()
        await nc.close()

    @async_test
    async def test_commit_msg_and_first_message_commit(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ORDERS", subjects=["orders.>"], allow_atomic=True)

        batch = new_batch_publisher(js)
        ack = await batch.commit_msg(nats.aio.msg.Msg(nc, subject="orders.one", data=b"only"))
        assert ack.batch_size == 1
        assert ack.seq == 1
        assert batch.is_closed()
        await nc.close()

    @async_test
    async def test_close_commits_with_end_of_batch(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ORDERS", subjects=["orders.>"], allow_atomic=True)

        batch = new_batch_publisher(js)
        with pytest.raises(EmptyBatchError):
            await batch.close()
        assert not batch.is_closed()

        await batch.add("orders.a", b"1")
        await batch.add("orders.b", b"2")
        ack = await batch.close()
        assert ack.stream == "ORDERS"
        assert ack.seq == 2
        assert ack.batch_size == 2
        assert batch.is_closed()
        # The end-of-batch marker is not stored and the last message carries
        # the regular commit header.
        info = await js.stream_info("ORDERS")
        assert info.state.messages == 2
        msg = await js.get_msg("ORDERS", 2)
        assert msg.data == b"2"
        assert msg.headers[BATCH_COMMIT_HEADER] == "1"
        await nc.close()

    @async_test
    async def test_discard(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ORDERS", subjects=["orders.>"], allow_atomic=True)

        batch = new_batch_publisher(js)
        await batch.add("orders.a", b"1")
        batch.discard()
        assert batch.is_closed()
        assert batch.size() == 1
        with pytest.raises(BatchClosedError):
            await batch.add("orders.a", b"2")
        with pytest.raises(BatchClosedError):
            batch.discard()
        info = await js.stream_info("ORDERS")
        assert info.state.messages == 0
        await nc.close()

    @async_test
    async def test_flow_control_and_errors(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="PLAIN", subjects=["plain.>"])

        # The first message waits for its ack, which carries the error.
        batch = new_batch_publisher(js)
        with pytest.raises(BatchPublishNotEnabledError) as e:
            await batch.add("plain.a", b"1")
        assert e.value.err_code == JS_ERR_CODE_BATCH_PUBLISH_NOT_ENABLED

        # Without acks the error only comes with the commit.
        batch = new_batch_publisher(js, BatchFlowControl(ack_first=False))
        await batch.add("plain.a", b"1")
        await batch.add("plain.a", b"2")
        with pytest.raises(BatchPublishNotEnabledError):
            await batch.commit("plain.a", b"3")

        # Every second message waits for its ack.
        batch = new_batch_publisher(js, BatchFlowControl(ack_first=False, ack_every=2, ack_timeout=1.0))
        await batch.add("plain.a", b"1")
        with pytest.raises(BatchPublishNotEnabledError):
            await batch.add("plain.a", b"2")

        # No stream listens on the subject.
        batch = new_batch_publisher(js)
        with pytest.raises(NoStreamResponseError):
            await batch.add("nothing.here", b"1")
        await nc.close()

    @async_test
    async def test_message_options(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ORDERS", subjects=["orders.>"], allow_atomic=True, allow_msg_ttl=True)
        await js.publish("orders.a", b"0")

        batch = new_batch_publisher(js)
        await batch.add(
            "orders.a",
            b"1",
            expect_stream="ORDERS",
            expect_last_sequence=1,
            expect_last_subject_sequence=1,
            expect_last_subject_sequence_subject="orders.*",
        )
        await batch.add("orders.b", b"2", msg_ttl=90)
        ack = await batch.commit("orders.c", b"3")
        assert ack.batch_size == 3
        msg = await js.get_msg("ORDERS", 2)
        assert msg.headers["Nats-Expected-Stream"] == "ORDERS"
        assert msg.headers["Nats-Expected-Last-Sequence"] == "1"
        assert msg.headers["Nats-Expected-Last-Subject-Sequence"] == "1"
        assert msg.headers[EXPECTED_LAST_SUBJECT_SEQUENCE_SUBJECT_HEADER] == "orders.*"
        msg = await js.get_msg("ORDERS", 3)
        assert msg.headers["Nats-TTL"] == "1m30s"

        # A failed expectation fails the batch.
        batch = new_batch_publisher(js)
        await batch.add("orders.a", b"1", expect_stream="OTHER")
        with pytest.raises(BadRequestError) as e:
            await batch.commit("orders.a", b"2")
        assert e.value.err_code == 10060

        batch = new_batch_publisher(js)
        await batch.add("orders.a", b"1", expect_last_subject_sequence=0)
        with pytest.raises(BadRequestError) as e:
            await batch.commit("orders.a", b"2")
        assert e.value.err_code == 10071

        batch = new_batch_publisher(js)
        with pytest.raises(InvalidOptionError):
            await batch.add("orders.a", b"1", expect_last_subject_sequence=1, expect_last_subject_sequence_subject="")
        with pytest.raises(InvalidOptionError):
            await batch.add("orders.a", b"1", expect_last_subject_sequence_subject="orders.a")
        assert batch.size() == 0
        await nc.close()

    @async_test
    async def test_publish_msg_batch(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="ORDERS", subjects=["orders.>"], allow_atomic=True)

        with pytest.raises(EmptyBatchError):
            await publish_msg_batch(js, [])

        msgs = [
            nats.aio.msg.Msg(nc, subject="orders.new", data=str(i).encode(), headers={BATCH_COMMIT_HEADER: "1"})
            for i in range(5)
        ]
        msgs.append(nats.aio.msg.Msg(nc, subject="orders.new", data=b"5"))
        ack = await publish_msg_batch(js, msgs, BatchFlowControl(ack_first=True, ack_every=2))
        assert ack.stream == "ORDERS"
        assert ack.seq == 6
        assert ack.batch_size == 6
        info = await js.stream_info("ORDERS")
        assert info.state.messages == 6
        msg = await js.get_msg("ORDERS", 3)
        assert BATCH_COMMIT_HEADER not in msg.headers
        assert msg.headers[BATCH_SEQ_HEADER] == "3"
        await nc.close()

    def test_format_duration(self):
        fmt = jetstreamext._format_duration
        assert fmt(0) == "0s"
        assert fmt(1) == "1s"
        assert fmt(1.5) == "1.5s"
        assert fmt(90) == "1m30s"
        assert fmt(3600) == "1h0m0s"
        assert fmt(0.5) == "500ms"
        assert fmt(0.0015) == "1.5ms"
        assert fmt(2e-6) == "2µs"
        assert fmt(3e-9) == "3ns"


class FastPublisherTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_add_and_close(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="EVENTS", subjects=["events.>"], allow_batched=True)

        fp = new_fast_publisher(js, FastPublishFlowControl(flow=10, max_outstanding_acks=2))
        with pytest.raises(EmptyBatchError):
            await fp.close()

        msg = nats.aio.msg.Msg(nc, subject="events.raw", data=b"0")
        ack = await fp.add_msg(msg)
        assert ack == FastPubAck(batch_sequence=1, ack_sequence=0)
        assert msg.reply.endswith(".10.fail.1.0.$FI")
        assert msg.headers is None
        last = ack
        for i in range(1, 100):
            last = await fp.add("events.raw", str(i).encode())
            assert last.batch_sequence == i + 1
            # Never more than two acks (of ten messages) outstanding.
            assert last.batch_sequence - last.ack_sequence < 20
        assert last.ack_sequence > 0
        assert not fp.is_closed()

        ack = await fp.close()
        assert isinstance(ack, BatchAck)
        assert ack.stream == "EVENTS"
        assert ack.seq == 100
        assert ack.batch_size == 100
        assert fp.is_closed()
        info = await js.stream_info("EVENTS")
        assert info.state.messages == 100

        with pytest.raises(BatchClosedError):
            await fp.add("events.raw", b"x")
        with pytest.raises(BatchClosedError):
            await fp.commit("events.raw", b"x")
        with pytest.raises(BatchClosedError):
            await fp.close()
        await nc.close()

    @async_test
    async def test_commit(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="EVENTS", subjects=["events.>"], allow_batched=True, allow_msg_ttl=True)

        fp = new_fast_publisher(js)
        await fp.add("events.a", b"1", expect_stream="EVENTS")
        await fp.add("events.b", b"2", msg_ttl=60)
        ack = await fp.commit_msg(nats.aio.msg.Msg(nc, subject="events.c", data=b"3", headers={"X": "y"}))
        assert ack.stream == "EVENTS"
        assert ack.seq == 3
        assert ack.batch_size == 3
        assert fp.is_closed()
        msg = await js.get_msg("EVENTS", 1)
        assert msg.headers["Nats-Expected-Stream"] == "EVENTS"
        msg = await js.get_msg("EVENTS", 2)
        assert msg.headers["Nats-TTL"] == "1m0s"
        msg = await js.get_msg("EVENTS", 3)
        assert msg.data == b"3"
        assert msg.headers["X"] == "y"
        await nc.close()

    @async_test
    async def test_not_enabled(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="PLAIN", subjects=["plain.>"])

        errors = []
        fp = new_fast_publisher(js, error_handler=errors.append)
        with pytest.raises(FastBatchNotEnabledError) as e:
            await fp.add("plain.a", b"1")
        assert e.value.err_code == JS_ERR_CODE_FAST_BATCH_NOT_ENABLED
        assert fp.is_closed()
        with pytest.raises(BatchClosedError):
            await fp.add("plain.a", b"2")
        assert errors == []
        await nc.close()

    @async_test
    async def test_gap_abandons_batch(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="EVENTS", subjects=["events.>"], allow_batched=True)

        errors = []
        fp = new_fast_publisher(js, error_handler=errors.append)
        await fp.add("events.a", b"1")
        await fp.add("events.a", b"2")
        # Lose message 3.
        fp._sequence += 1
        await fp.add("events.a", b"4")
        for _ in range(50):
            if fp.is_closed():
                break
            await asyncio.sleep(0.02)
        assert fp.is_closed()
        assert len(errors) == 1
        assert isinstance(errors[0], FastBatchGapDetectedError)
        assert errors[0].expected_last_sequence == 3
        assert errors[0].current_sequence == 4
        with pytest.raises(BatchClosedError):
            await fp.add("events.a", b"5")
        info = await js.stream_info("EVENTS")
        assert info.state.messages == 2
        await nc.close()

    @async_test
    async def test_continue_on_gap(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="EVENTS", subjects=["events.>"], allow_batched=True)

        errors = []

        async def handler(err):
            errors.append(err)

        fp = new_fast_publisher(js, continue_on_gap=True, error_handler=handler)
        msg = nats.aio.msg.Msg(nc, subject="events.a", data=b"1")
        await fp.add_msg(msg)
        assert msg.reply.endswith(".100.ok.1.0.$FI")
        fp._sequence += 1
        await fp.add("events.a", b"3")
        ack = await fp.commit("events.a", b"4")
        assert ack.stream == "EVENTS"
        assert ack.seq == 3
        assert len(errors) == 1
        assert isinstance(errors[0], FastBatchGapDetectedError)
        assert str(errors[0]) == "nats: fast batch gap detected: expected last sequence 2; current sequence 3"
        await nc.close()

    @async_test
    async def test_message_error(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="EVENTS", subjects=["events.>"], allow_batched=True)

        errors = []
        fp = new_fast_publisher(js, error_handler=errors.append)
        await fp.add("events.a", b"1")
        await fp.add("events.a", b"2", expect_stream="OTHER")
        for _ in range(50):
            if fp.is_closed():
                break
            await asyncio.sleep(0.02)
        assert fp.is_closed()
        assert isinstance(errors[0], FastBatchMsgError)
        assert errors[0].sequence == 2
        assert errors[0].error.err_code == 10060
        with pytest.raises(BatchClosedError):
            await fp.commit("events.a", b"3")
        await nc.close()

    @async_test
    async def test_ping_recovers_lost_ack(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="EVENTS", subjects=["events.>"], allow_batched=True)

        fp = new_fast_publisher(js, FastPublishFlowControl(flow=2, max_outstanding_acks=1, ack_timeout=1.5))
        handle = fp._handle_ack
        seen = []

        async def drop_first_flow_ack(msg):
            seen.append(msg.subject)
            if len(seen) == 2:
                return
            await handle(msg)

        fp._handle_ack = drop_first_flow_ack
        await fp.add("events.a", b"1")
        ack = await fp.add("events.a", b"2")
        # The ack of message 2 was lost and recovered by a ping.
        assert ack == FastPubAck(batch_sequence=2, ack_sequence=2)
        assert any(s.endswith(".2.4.$FI") for s in seen)
        ack = await fp.close()
        assert ack.batch_size == 2
        await nc.close()

    @async_test
    async def test_ack_timeout(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="EVENTS", subjects=["events.>"], allow_batched=True)

        fp = new_fast_publisher(js, FastPublishFlowControl(flow=2, max_outstanding_acks=1, ack_timeout=0.6))
        handle = fp._handle_ack

        async def first_ack_only(msg):
            if msg.subject.endswith(".1.0.$FI"):
                await handle(msg)

        fp._handle_ack = first_ack_only
        await fp.add("events.a", b"1")
        with pytest.raises(BatchAckTimeoutError) as e:
            await fp.add("events.a", b"2")
        assert e.value.sequence == 2
        assert e.value.ack_sequence == 0
        assert isinstance(e.value, TimeoutError)
        assert fp.is_closed()
        await nc.close()

    @async_test
    async def test_invalid_options(self):
        nc = await nats.connect()
        js = nc.jetstream()
        with pytest.raises(InvalidOptionError):
            new_fast_publisher(js, FastPublishFlowControl(ack_timeout=-1))
        with pytest.raises(InvalidOptionError):
            new_fast_publisher(js, FastPublishFlowControl(flow=70000))
        with pytest.raises(InvalidOptionError):
            new_fast_publisher(js, FastPublishFlowControl(max_outstanding_acks=-1))
        fp = new_fast_publisher(js, FastPublishFlowControl(flow=0, max_outstanding_acks=0))
        assert fp._flow == 100
        assert fp._max_outstanding_acks == 2
        assert fp._ack_timeout == js._timeout
        await nc.close()


async def _collect(it):
    return [msg async for msg in it]


class GetBatchTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_get_batch(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="S", subjects=["s.>"], allow_direct=True)
        for i in range(5):
            await js.publish(f"s.{i % 2}", str(i).encode())

        msgs = await _collect(await get_batch(js, "S", 3))
        assert [m.seq for m in msgs] == [1, 2, 3]
        assert [m.data for m in msgs] == [b"0", b"1", b"2"]
        assert msgs[1].subject == "s.1"
        assert msgs[1].stream == "S"
        assert isinstance(msgs[1].time, datetime.datetime)
        assert msgs[1].headers["Nats-Num-Pending"] == "3"

        msgs = await _collect(await get_batch(js, "S", 10, seq=4))
        assert [m.seq for m in msgs] == [4, 5]

        msgs = await _collect(await get_batch(js, "S", 10, subject="s.0"))
        assert [m.seq for m in msgs] == [1, 3, 5]
        msgs = await _collect(await get_batch(js, "S", 2, seq=2, subject="s.*"))
        assert [m.seq for m in msgs] == [2, 3]

        with pytest.raises(NoMessagesError):
            await _collect(await get_batch(js, "S", 3, seq=10))
        with pytest.raises(NotFoundError):
            await _collect(await get_batch(js, "S", 3, subject="s.9"))

        # A stream without direct gets has no responders.
        await js.add_stream(name="NODIRECT", subjects=["nodirect.>"])
        with pytest.raises(NoRespondersError):
            await _collect(await get_batch(js, "NODIRECT", 3))
        await nc.close()

    @async_test
    async def test_get_batch_max_bytes_and_start_time(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="S", subjects=["s.>"], allow_direct=True)
        for i in range(3):
            await js.publish("s.a", b"x" * 100)
        await asyncio.sleep(0.05)
        start = datetime.datetime.now(datetime.timezone.utc)
        await asyncio.sleep(0.05)
        for i in range(2):
            await js.publish("s.b", b"y" * 100)

        msgs = await _collect(await get_batch(js, "S", 10, max_bytes=250))
        assert 1 <= len(msgs) < 5

        msgs = await _collect(await get_batch(js, "S", 10, start_time=start))
        assert [m.seq for m in msgs] == [4, 5]
        # Naive datetimes are UTC.
        naive = start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        msgs = await _collect(await get_batch(js, "S", 1, start_time=naive))
        assert [m.seq for m in msgs] == [4]
        await nc.close()

    @async_test
    async def test_get_batch_invalid_options(self):
        nc = await nats.connect()
        js = nc.jetstream()
        now = datetime.datetime.now(datetime.timezone.utc)
        with pytest.raises(InvalidOptionError):
            await get_batch(js, "S", 1, seq=0)
        with pytest.raises(InvalidOptionError):
            await get_batch(js, "S", 1, seq=1, start_time=now)
        with pytest.raises(InvalidOptionError):
            await get_batch(js, "S", 1, max_bytes=0)
        with pytest.raises(SubjectRequiredError):
            await get_last_msgs_for(js, "S", [])
        with pytest.raises(InvalidOptionError):
            await get_last_msgs_for(js, "S", ["s.a"], up_to_seq=1, up_to_time=now)
        with pytest.raises(InvalidOptionError):
            await get_last_msgs_for(js, "S", ["s.a"], batch=0)
        await nc.close()

    @async_test
    async def test_get_last_msgs_for(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="S", subjects=["s.>"], allow_direct=True)
        for i in range(4):
            await js.publish(f"s.{i % 2}", str(i).encode())
        await asyncio.sleep(0.05)
        middle = datetime.datetime.now(datetime.timezone.utc)
        await asyncio.sleep(0.05)
        await js.publish("s.0", b"4")

        msgs = await _collect(await get_last_msgs_for(js, "S", ["s.0", "s.1"]))
        assert [(m.subject, m.seq) for m in msgs] == [("s.1", 4), ("s.0", 5)]
        msgs = await _collect(await get_last_msgs_for(js, "S", ["s.*"], up_to_seq=2))
        assert [m.seq for m in msgs] == [1, 2]
        msgs = await _collect(await get_last_msgs_for(js, "S", ["s.0", "s.1"], up_to_time=middle))
        assert [m.seq for m in msgs] == [3, 4]
        msgs = await _collect(await get_last_msgs_for(js, "S", ["s.>"], batch=1))
        assert len(msgs) == 1
        with pytest.raises(NoMessagesError):
            await _collect(await get_last_msgs_for(js, "S", ["s.9"]))
        await nc.close()

    def test_invalid_responses(self):
        def msg(headers, data=b"x"):
            return nats.aio.msg.Msg(None, subject="_INBOX.x", data=data, headers=headers)

        good = {
            "Nats-Stream": "S",
            "Nats-Subject": "s.a",
            "Nats-Sequence": "1",
            "Nats-Time-Stamp": "2026-10-08T05:09:50.822302496Z",
            "Nats-Num-Pending": "0",
        }
        raw = jetstreamext._direct_msg(msg(dict(good)))
        assert raw.seq == 1
        assert raw.subject == "s.a"

        no_pending = dict(good)
        del no_pending["Nats-Num-Pending"]
        with pytest.raises(BatchUnsupportedError):
            jetstreamext._direct_msg(msg(no_pending))
        with pytest.raises(InvalidResponseError):
            jetstreamext._direct_msg(msg(None))
        for name in ("Nats-Stream", "Nats-Sequence", "Nats-Time-Stamp", "Nats-Subject"):
            headers = dict(good)
            del headers[name]
            with pytest.raises(InvalidResponseError):
                jetstreamext._direct_msg(msg(headers))
        with pytest.raises(InvalidResponseError):
            jetstreamext._direct_msg(msg(dict(good, **{"Nats-Sequence": "x"})))
        with pytest.raises(NoMessagesError):
            jetstreamext._direct_msg(msg({"Status": "404", "Description": "No Results"}, b""))
        with pytest.raises(APIError) as e:
            jetstreamext._direct_msg(msg({"Status": "408", "Description": "Request Timeout"}, b""))
        assert e.value.code == 408
