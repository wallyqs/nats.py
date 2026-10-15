"""Property-based tests for PullSubscription.fetch using Hypothesis.

Three layers, from fastest to most realistic:

* ``test_fetch_matches_model`` drives ``fetch`` with generated server replies
  (data, heartbeats, 404/408/409/423/503 statuses, payloads carrying status
  headers, pin ids) through an in-memory subscription and compares every
  observable effect against a small reference model of the nats.go Fetch
  contract: returned messages, raised error, pull requests sent, leftover
  buffered messages and their byte accounting, pin id, and cleanup.
* ``test_fetch_validation`` checks that argument validation is total and
  happens before any buffered message is consumed or request is sent.
* ``test_fetch_against_server`` runs generated publish/fetch programs against
  a real JetStream consumer and checks that delivery is ordered, bounded by
  the batch size, and loses or duplicates nothing.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections import deque
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

import nats
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js import api, errors
from tests.test_js_pull_subscribe import server as server

# (timeout, heartbeat) pairs. Frames are delivered synchronously with each
# request, so deadlines only matter once fetch goes idle. Examples that expire
# on purpose use short deadlines that still tolerate scheduler stalls; all other
# examples use deadlines long enough that they can never fire.
EXPIRING = (0.25, 0.05)
LONG = (5.0, 1.0)


class Frame(NamedTuple):
    uid: int
    data: bytes
    headers: Optional[Dict[str, str]]

    @property
    def status(self) -> Optional[str]:
        # Only empty payloads are control frames, even with a Status header.
        if self.data or not self.headers:
            return None
        return self.headers.get("Status")


class Outcome(NamedTuple):
    msgs: List[int]
    error: Optional[type]
    leftover: List[int]
    requests: List[Dict[str, Any]]
    pin_id: Optional[str]
    # (code, description) of the status frame behind an APIError.
    terminal: Optional[Tuple[int, str]] = None


STATUS_FRAMES = [
    ("100", "Idle Heartbeat"),
    ("404", "No Messages"),
    ("408", "Request Timeout"),
    ("409", "Consumer Deleted"),
    ("409", "Leadership Change"),
    ("409", "Exceeded MaxWaiting"),
    ("409", "Message Size Exceeds MaxBytes"),
    ("423", "Nats-Pin-Id mismatch"),
    ("503", "No Responders"),
    ("400", "Bad Request"),
]


# Statuses that keep the fetch going (heartbeat, retry, pin reset) need runs
# of their own to reach the deeper request/retry paths, so weight them up.
CONTINUING = [frame for frame in STATUS_FRAMES if frame[0] in ("100", "404", "408", "423")]


def status(code: str, description: str = "") -> Tuple[bytes, Dict[str, str]]:
    return b"", {"Status": code, "Description": description}


status_specs = st.one_of(st.sampled_from(CONTINUING), st.sampled_from(STATUS_FRAMES)).map(lambda f: status(*f))


@st.composite
def frame_specs(draw) -> Tuple[bytes, Optional[Dict[str, str]]]:
    kind = draw(st.sampled_from(["data", "data", "data", "status", "status", "empty"]))
    if kind == "status":
        return draw(status_specs)
    if kind == "empty":
        # Empty application payloads are still messages.
        return b"", draw(st.sampled_from([None, {"App": "empty"}]))
    headers: Dict[str, str] = {}
    if draw(st.booleans()):
        # A non-empty payload is delivered even when it carries a status.
        headers["Status"] = draw(st.sampled_from(["100", "404", "408", "409", "503"]))
    if draw(st.booleans()):
        headers["Nats-Pin-Id"] = draw(st.sampled_from(["pin-a", "pin-b"]))
    return draw(st.binary(min_size=1, max_size=8)), headers or None


def make_scenario(
    batch: int,
    buffered: List[Tuple[bytes, Optional[Dict[str, str]]]],
    replies: List[List[Tuple[bytes, Optional[Dict[str, str]]]]],
    idle: str,
    heartbeat: Optional[float] = None,
    timeout: Optional[float] = None,
    pin_id: Optional[str] = None,
    min_pending: Optional[int] = None,
    priority: Optional[int] = None,
) -> Dict[str, Any]:
    # Give every frame a unique id so results can be matched by identity.
    uid = 0

    def number(specs):
        nonlocal uid
        frames = [Frame(uid + i, data, headers) for i, (data, headers) in enumerate(specs)]
        uid += len(frames)
        return frames

    return {
        "batch": batch,
        "buffered": number(buffered),
        "replies": [number(specs) for specs in replies],
        "heartbeat": heartbeat,
        "timeout": timeout,
        "idle": idle,
        "pin_id": pin_id,
        "min_pending": min_pending,
        "priority": priority,
    }


@st.composite
def scenarios(draw) -> Dict[str, Any]:
    reply = st.one_of(st.lists(frame_specs(), max_size=8), st.lists(status_specs, min_size=1, max_size=3))
    # What happens once fetch has consumed every frame and is waiting for more.
    idle = draw(st.sampled_from(["wait", "disconnect", "close", "unsubscribe", "cancel"]))
    timeout, interval = EXPIRING if idle == "wait" else LONG
    heartbeat = draw(st.sampled_from([None, 0, interval]))
    # Without a deadline, an idle wait needs the heartbeat monitor to end it.
    if (heartbeat or idle != "wait") and draw(st.booleans()):
        timeout = None
    return make_scenario(
        batch=draw(st.integers(1, 6)),
        buffered=draw(st.lists(frame_specs(), max_size=6)),
        replies=draw(st.lists(reply, max_size=4)),
        idle=idle,
        heartbeat=heartbeat,
        timeout=timeout,
        pin_id=draw(st.sampled_from([None, "pin-z"])),
        min_pending=draw(st.sampled_from([None, 1, 5])),
        priority=draw(st.sampled_from([None, 0, 9])),
    )


def model(scenario: Dict[str, Any]) -> Outcome:
    """Sequential reference of the documented Fetch contract."""
    batch = scenario["batch"]
    heartbeat = scenario["heartbeat"]
    replies = scenario["replies"]
    queue = deque(scenario["buffered"])
    msgs: List[Frame] = []
    requests: List[Dict[str, Any]] = []
    pin_id = scenario["pin_id"]

    def append(frame: Frame) -> None:
        nonlocal pin_id
        if frame.headers and frame.headers.get("Nats-Pin-Id"):
            pin_id = frame.headers["Nats-Pin-Id"]
        msgs.append(frame)

    def result(error: Optional[type] = None, terminal: Optional[Frame] = None) -> Outcome:
        status = None
        if terminal is not None and terminal.headers:
            status = (int(terminal.headers["Status"]), terminal.headers.get("Description", ""))
        return Outcome([f.uid for f in msgs], error, [f.uid for f in queue], requests, pin_id, status)

    # Buffered messages count toward the batch; stale control frames are dropped.
    while len(msgs) < batch and queue:
        frame = queue.popleft()
        if not frame.status:
            append(frame)
    if len(msgs) == batch:
        return result()

    no_wait = batch - len(msgs) > 1
    got_heartbeat = False
    retried_pin = False

    def request() -> None:
        # A no_wait probe with messages in hand neither expires nor heartbeats.
        probe = no_wait and bool(msgs)
        requests.append(
            {
                "batch": batch - len(msgs),
                "no_wait": no_wait,
                "expires": scenario["timeout"] is not None and not probe,
                "idle_heartbeat": bool(heartbeat) and not probe,
                "id": pin_id,
            }
        )
        if len(requests) <= len(replies):
            queue.extend(replies[len(requests) - 1])

    request()
    error: Optional[type] = None
    terminal: Optional[Frame] = None
    while len(msgs) < batch:
        if not queue:
            error = {
                "disconnect": errors.FetchDisconnectedError,
                "close": nats.errors.ConnectionClosedError,
                "unsubscribe": nats.errors.BadSubscriptionError,
                "cancel": asyncio.CancelledError,
            }.get(scenario["idle"])
            if error is None:
                if heartbeat:
                    error = errors.NoHeartbeatError
                elif got_heartbeat:
                    error = errors.FetchTimeoutError
                else:
                    error = nats.errors.TimeoutError
            break
        frame = queue.popleft()
        status = frame.status
        if not status:
            append(frame)
            continue
        if status == "100":
            got_heartbeat = True
            continue
        if status == "423":
            pin_id = None
            if not retried_pin and not msgs:
                retried_pin = True
                request()
                continue
        if status in ("404", "408"):
            if msgs:
                break
            if no_wait:
                no_wait = False
                request()
                continue
            if status == "408":
                continue
        error = errors.ServiceUnavailableError if status == "503" else errors.APIError
        terminal = frame
        break

    # Partial batches succeed on terminal errors, but cancellation propagates
    # and the messages collected so far are consumed without being returned.
    if error is asyncio.CancelledError:
        msgs.clear()
    elif msgs:
        error = None
    return result(error, terminal if error is errors.APIError else None)


async def run_fetch(scenario: Dict[str, Any]) -> Tuple[Outcome, Optional[BaseException]]:
    nc = Client()
    native = Subscription(nc, subject="inbox")
    native._start(nc._error_cb)
    js = nc.jetstream()
    sub = js.PullSubscription(js, native, "STREAM", "worker", b"inbox")
    sub._pin_id = scenario["pin_id"]
    queue = native._pending_queue
    by_uid: Dict[int, Msg] = {}
    requests: List[Dict[str, Any]] = []

    def deliver(frame: Frame) -> None:
        msg = Msg(nc, subject="inbox", data=frame.data, headers=frame.headers)
        by_uid[frame.uid] = msg
        queue.put_nowait(msg)
        native._pending_size += len(frame.data)

    async def publish(subject: str, payload: bytes, reply: str) -> None:
        assert subject == sub._nms
        assert reply == "inbox"
        requests.append(json.loads(payload))
        if len(requests) <= len(scenario["replies"]):
            for frame in scenario["replies"][len(requests) - 1]:
                deliver(frame)

    for frame in scenario["buffered"]:
        deliver(frame)
    nc.publish = publish  # type: ignore[method-assign]

    fetch = asyncio.ensure_future(
        sub.fetch(
            scenario["batch"],
            timeout=scenario["timeout"],
            heartbeat=scenario["heartbeat"],
            min_pending=scenario["min_pending"],
            priority=scenario["priority"],
        )
    )

    async def when_idle() -> None:
        # Fetch only registers a receive waiter once the queue is drained.
        while not (native._pending_next_msgs_calls and queue.empty()):
            await asyncio.sleep(0)
        idle = scenario["idle"]
        if idle == "disconnect":
            nc._notify_disconnect()
        elif idle == "close":
            nc._status = Client.CLOSED
            nc._notify_disconnect()
        elif idle == "unsubscribe":
            native._closed = True
            native._stop_processing()
        else:
            fetch.cancel()

    trigger = asyncio.ensure_future(when_idle()) if scenario["idle"] != "wait" else None
    raised: Optional[BaseException] = None
    msgs: List[Msg] = []
    try:
        msgs = await asyncio.wait_for(asyncio.shield(fetch), 5)
    except asyncio.CancelledError as exc:
        if not fetch.cancelled():
            raise
        raised = exc
    except (nats.errors.Error, ValueError) as exc:
        raised = exc
    finally:
        if trigger is not None:
            trigger.cancel()
            await asyncio.gather(trigger, return_exceptions=True)

    uid_of = {id(msg): uid for uid, msg in by_uid.items()}
    assert sub.pending_msgs == queue.qsize()
    assert sub.pending_bytes == sum(len(msg.data) for msg in queue._queue)  # type: ignore[attr-defined]
    # Every consumed frame was marked done, so join() only waits for leftovers.
    assert queue._unfinished_tasks == queue.qsize()  # type: ignore[attr-defined]
    assert not nc._disconnect_waiters
    assert not native._pending_next_msgs_calls
    assert not queue._getters  # type: ignore[attr-defined]
    leftover = [uid_of[id(msg)] for msg in queue._queue]  # type: ignore[attr-defined]

    terminal = None
    if type(raised) is errors.APIError:
        terminal = (raised.code, raised.description)
    observed = Outcome(
        [uid_of[id(m)] for m in msgs], type(raised) if raised else None, leftover, requests, sub.pin_id, terminal
    )
    return observed, raised


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(scenario=scenarios())
# Paths that need several specific frames in a row, pinned so every run covers them.
# A pin id mismatch is retried once, then reported.
@example(scenario=make_scenario(1, [], [[status("423")], [status("423")], [(b"one", None)]], "disconnect"))
# Heartbeats without messages turn an expiry into FetchTimeoutError.
@example(scenario=make_scenario(1, [], [[status("100")]], "wait", timeout=EXPIRING[0]))
# An empty no_wait probe is retried as a waiting request, whose 408 is ignored.
@example(scenario=make_scenario(3, [], [[status("404")], [status("408")]], "wait", timeout=EXPIRING[0]))
# Buffered messages count toward the batch, and the probe for the rest neither
# expires nor requests heartbeats.
@example(scenario=make_scenario(3, [(b"one", None)], [[(b"two", None), status("404")]], "close", 1.0, 5.0))
def test_fetch_matches_model(scenario):
    expected = model(scenario)
    observed, raised = asyncio.run(run_fetch(scenario))

    assert len(observed.msgs) <= scenario["batch"]
    assert observed.msgs == expected.msgs
    assert observed.error is expected.error, raised
    assert observed.leftover == expected.leftover
    assert observed.pin_id == expected.pin_id
    assert observed.terminal == expected.terminal

    assert len(observed.requests) == len(expected.requests)
    expires = []
    for sent, want in zip(observed.requests, expected.requests):
        assert sent["batch"] == want["batch"]
        assert sent.get("no_wait", False) == want["no_wait"]
        assert ("expires" in sent) == want["expires"]
        if want["expires"]:
            assert 0 < sent["expires"] <= scenario["timeout"] * 0.91 * 1e9
            expires.append(sent["expires"])
        assert sent.get("idle_heartbeat") == (int(scenario["heartbeat"] * 1e9) if want["idle_heartbeat"] else None)
        assert sent.get("id") == want["id"]
        assert sent.get("min_pending") == scenario["min_pending"]
        assert sent.get("priority") == scenario["priority"]
    # One deadline spans every request, so server expirations only shrink.
    assert expires == sorted(expires, reverse=True)


fetch_args = {
    "batch": st.integers(-2, 5),
    "timeout": st.one_of(st.none(), st.floats(-1, 3, allow_nan=False)),
    "heartbeat": st.one_of(st.none(), st.floats(-1, 2, allow_nan=False)),
    "min_pending": st.one_of(st.none(), st.integers(-2, 3)),
    "min_ack_pending": st.one_of(st.none(), st.integers(-2, 3)),
    "priority": st.one_of(st.none(), st.integers(-2, 11)),
}


def valid(batch, timeout, heartbeat, min_pending, min_ack_pending, priority) -> bool:
    return (
        batch >= 1
        and (timeout is None or timeout > 0)
        and (heartbeat is None or heartbeat >= 0)
        and not (heartbeat and timeout is not None and 2 * heartbeat >= timeout)
        and (min_pending is None or min_pending > 0)
        and (min_ack_pending is None or min_ack_pending > 0)
        and (priority is None or 0 <= priority <= 9)
    )


@settings(max_examples=300, deadline=None)
@given(**fetch_args)
def test_fetch_validation(batch, timeout, heartbeat, min_pending, min_ack_pending, priority):
    buffered = [b"one", b"two", b"three", b"four", b"five"]

    async def scenario():
        nc = Client()
        native = Subscription(nc, subject="inbox")
        native._start(nc._error_cb)
        js = nc.jetstream()
        sub = js.PullSubscription(js, native, "STREAM", "worker", b"inbox")
        for data in buffered:
            native._pending_queue.put_nowait(Msg(nc, data=data))
            native._pending_size += len(data)

        async def publish(subject, payload, reply):
            raise AssertionError("buffered batches must not send a pull request")

        nc.publish = publish
        kwargs = dict(
            timeout=timeout,
            heartbeat=heartbeat,
            min_pending=min_pending,
            min_ack_pending=min_ack_pending,
            priority=priority,
        )
        if valid(batch, timeout, heartbeat, min_pending, min_ack_pending, priority):
            msgs = await sub.fetch(batch, **kwargs)
            assert [msg.data for msg in msgs] == buffered[:batch]
            remaining = buffered[batch:]
        else:
            with pytest.raises(ValueError):
                await sub.fetch(batch, **kwargs)
            remaining = buffered
        assert sub.pending_msgs == len(remaining)
        assert sub.pending_bytes == sum(len(data) for data in remaining)
        assert not nc._disconnect_waiters

    asyncio.run(scenario())


publish_op = st.tuples(st.just("publish"), st.integers(1, 6))
# (fetch, batch, messages published while the fetch is waiting, ack)
fetch_op = st.tuples(st.just("fetch"), st.integers(1, 5), st.sampled_from([0, 0, 1, 3]), st.booleans())


@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(program=st.lists(st.one_of(publish_op, fetch_op), min_size=1, max_size=6))
def test_fetch_against_server(server, program):
    asyncio.run(run_program(server.port, program))


async def run_program(port: int, program: List[Tuple[Any, ...]]) -> None:
    nc = await nats.connect(f"nats://127.0.0.1:{port}")
    js = nc.jetstream()
    stream = "PROP" + uuid.uuid4().hex
    subject = stream + ".events"
    await js.add_stream(name=stream, subjects=[subject])
    try:
        sub = await js.pull_subscribe(subject, "worker", stream=stream, config=api.ConsumerConfig(ack_wait=30))
        published: List[bytes] = []
        delivered: List[bytes] = []
        acked = 0

        async def publish(count: int, delay: float = 0) -> None:
            await asyncio.sleep(delay)
            for _ in range(count):
                payload = str(len(published)).encode()
                published.append(payload)
                await js.publish(subject, payload)

        for op in program:
            if op[0] == "publish":
                await publish(op[1])
                continue
            _, batch, concurrent, ack = op
            available = len(published) - len(delivered)
            publisher = asyncio.ensure_future(publish(concurrent, delay=0.02))
            try:
                msgs = await sub.fetch(batch, timeout=0.3 if not available else 2)
            except nats.errors.TimeoutError:
                msgs = []
            finally:
                await publisher

            got = [msg.data for msg in msgs]
            # Delivery is ordered and never skips, duplicates, or exceeds the batch.
            assert got == published[len(delivered) : len(delivered) + len(got)]
            assert min(batch, available) <= len(got) <= min(batch, available + concurrent)
            if available + concurrent:
                assert got, "messages were available but fetch returned none"
            for offset, msg in enumerate(msgs):
                assert msg.metadata.sequence.stream == len(delivered) + offset + 1
                assert msg.metadata.num_delivered == 1
                if ack:
                    await msg.ack_sync()
                    acked += 1
            delivered.extend(got)
            assert not nc._disconnect_waiters
            assert not sub._sub._pending_next_msgs_calls

        remaining = len(published) - len(delivered)
        if remaining:
            msgs = await sub.fetch(remaining, timeout=2)
            delivered.extend(msg.data for msg in msgs)
            for msg in msgs:
                await msg.ack_sync()
                acked += 1
        assert delivered == published
        # A waiting request's 408 can arrive just after the local deadline, since
        # the server expires it only slightly earlier. The next fetch discards
        # such stale control frames, but no application message may be left.
        leftover = list(sub._sub._pending_queue._queue)
        assert all(not msg.data and msg.headers and msg.headers.get("Status") for msg in leftover), [
            (msg.data, msg.headers) for msg in leftover
        ]

        info = await sub.consumer_info()
        assert info.num_pending == 0
        assert info.delivered.consumer_seq == len(published)
        assert info.num_ack_pending == len(published) - acked
    finally:
        await js.delete_stream(stream)
        await nc.close()
