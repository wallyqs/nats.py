import asyncio
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
        assert type(err) is NotFoundError
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
