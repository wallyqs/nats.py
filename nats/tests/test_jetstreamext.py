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
