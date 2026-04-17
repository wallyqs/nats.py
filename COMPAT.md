# nats-py → nats-core Migration Guide

This document covers all breaking changes and usage differences between the legacy `nats-py` package (`nats.aio`) and the modern `nats-core` package (`nats.client`).

`nats-core` requires **Python 3.13+** and is not a drop-in replacement. Most changes are intentional API improvements.

---

## Table of Contents

1. [Package & Import Changes](#package--import-changes)
2. [connect() Parameter Changes](#connect-parameter-changes)
3. [Client Methods](#client-methods)
4. [Subscribing](#subscribing)
5. [Messages](#messages)
6. [Headers](#headers)
7. [Callbacks & Event Handlers](#callbacks--event-handlers)
8. [Authentication](#authentication)
9. [Error Classes](#error-classes)
10. [Side-by-Side Examples](#side-by-side-examples)
11. [Quick Migration Checklist](#quick-migration-checklist)

---

## Package & Import Changes

| | nats-py (legacy) | nats-core (modern) |
|---|---|---|
| PyPI package | `nats-py` | `nats-core` |
| Import | `import nats` / `from nats.aio.client import Client` | `from nats.client import connect, Client` |
| Entry point | `nats.connect()` | `nats.client.connect()` |
| Python requirement | >=3.7 | >=3.13 |

```python
# nats-py
import nats
nc = await nats.connect("nats://localhost:4222")

# nats-core
from nats.client import connect
nc = await connect("nats://localhost:4222")
```

---

## connect() Parameter Changes

### Renamed Parameters

| nats-py | nats-core | Notes |
|---|---|---|
| `servers` (list or str) | `url` (str only) | Single URL string; cluster URLs not yet supported in connect() |
| `connect_timeout` | `timeout` | |
| `reconnect_time_wait` | `reconnect_time_wait` | Same name, now `float` instead of `int` |
| `max_reconnect_attempts` | `reconnect_max_attempts` | Renamed; default changed: 60 → 10 |
| `dont_randomize` | `no_randomize` | Renamed |
| `pending_size` | _(removed)_ | Per-subscription limits instead |
| `flush_timeout` | _(removed)_ | Pass timeout directly to `flush()` |
| `ws_connection_headers` | _(removed)_ | WebSocket not yet supported |
| `reconnect_to_server_handler` | _(removed)_ | |
| `lame_duck_mode_cb` | _(removed)_ | |
| `discovered_server_cb` | _(removed)_ | |
| `flusher_queue_size` | _(removed)_ | |
| `tls_handshake_first` | `tls_handshake_first` | Same |

### New Parameters in nats-core

| Parameter | Type | Description |
|---|---|---|
| `reconnect_time_wait_max` | `float` | Upper bound for reconnect backoff (default: 10.0s) |
| `reconnect_jitter` | `float` | Jitter factor added to reconnect delays (default: 0.1) |
| `reconnect_timeout` | `float \| None` | Total timeout for entire reconnect attempt |
| `nkey` | `NkeySeed \| NkeyHandlers \| None` | NKey authentication (replaces `nkeys_seed`) |
| `jwt` | `JWTCredentials \| JWTHandlers \| None` | JWT authentication (replaces `user_credentials`) |

### Parameter Defaults That Changed

| Parameter | nats-py default | nats-core default |
|---|---|---|
| `max_reconnect_attempts` / `reconnect_max_attempts` | 60 | 10 |
| `request` timeout | 0.5s | 2.0s |
| `inbox_prefix` | `b"_INBOX"` (bytes) | `"_INBOX"` (str) |

### Signature Comparison

```python
# nats-py
await nats.connect(
    servers=["nats://localhost:4222"],
    error_cb=error_cb,
    disconnected_cb=disconnected_cb,
    closed_cb=closed_cb,
    reconnected_cb=reconnected_cb,
    name="my-client",
    connect_timeout=2,
    max_reconnect_attempts=60,
    dont_randomize=False,
    tls=ssl_ctx,
    user="alice",
    password="secret",
    token="mytoken",
    nkeys_seed="SUAGIEYF...",
    user_credentials="path/to/file.creds",
    inbox_prefix=b"_INBOX",
)

# nats-core
from nats.client import connect
await connect(
    "nats://localhost:4222",    # positional, single URL only
    timeout=2.0,                # was connect_timeout
    reconnect_max_attempts=10,  # was max_reconnect_attempts
    no_randomize=False,         # was dont_randomize
    tls=ssl_ctx,
    user="alice",
    password="secret",
    token="mytoken",
    nkey="SUAGIEYF...",         # was nkeys_seed
    jwt=Path("file.creds"),     # was user_credentials
    inbox_prefix="_INBOX",      # str, not bytes
)
```

---

## Client Methods

### Publish

```python
# nats-py
await nc.publish("subject", b"data", reply="reply_subject", headers={"X-Key": "val"})

# nats-core — keyword-only after payload
await nc.publish("subject", b"data", reply="reply_subject", headers={"X-Key": "val"})
```

Both have the same shape, but nats-core's `reply` and `headers` are keyword-only arguments.

### Subscribe

See the [Subscribing](#subscribing) section — this is the largest change.

### Request

```python
# nats-py — default timeout 0.5s
msg = await nc.request("subject", b"data", timeout=0.5)

# nats-core — default timeout 2.0s; keyword-only args; new return_on_error flag
msg = await nc.request("subject", b"data", timeout=2.0, headers=None, return_on_error=False)
```

`return_on_error=True` returns the status error message instead of raising, useful for inspecting 503 no-responders responses.

### Flush

```python
# nats-py — timeout is positional int (seconds)
await nc.flush(timeout=10)

# nats-core — timeout is keyword float or None
await nc.flush(timeout=5.0)
```

### Status Properties

| nats-py | nats-core |
|---|---|
| `nc.is_connected` (bool property) | `nc.status == ClientStatus.CONNECTED` |
| `nc.is_closed` | `nc.status == ClientStatus.CLOSED` |
| `nc.is_connecting` | `nc.status == ClientStatus.CONNECTING` |
| `nc.is_reconnecting` | `nc.status == ClientStatus.RECONNECTING` |
| `nc.is_draining` | `nc.status == ClientStatus.DRAINING` |
| `nc.last_error` (Exception) | `nc.last_error` (str \| None) |
| `nc.connected_url` (ParseResult) | `nc.server_info.host` / `.port` |
| `nc.max_payload` | `nc.server_info.max_payload` |
| `nc.client_id` | `nc.server_info.client_id` |
| `nc.pending_data_size` | `nc.stats().out_bytes` (cumulative) |
| `nc.set_server_pool(...)` | _(removed)_ |

### Statistics

```python
# nats-py — no built-in stats

# nats-core
stats = nc.stats()
# ClientStatistics(in_messages=..., out_messages=..., in_bytes=..., out_bytes=..., reconnects=...)
```

### RTT

```python
# nats-py — no RTT method

# nats-core
rtt_seconds = await nc.rtt(timeout=5.0)
```

### Context Manager

```python
# nats-py — no context manager support

# nats-core
async with await connect("nats://localhost:4222") as nc:
    await nc.publish("foo", b"bar")
# automatically closed on exit
```

---

## Subscribing

This is the most significant API change between the two packages.

### Push (Callback) Subscriptions

```python
# nats-py — callback passed to subscribe()
async def handler(msg):
    print(msg.data)

sub = await nc.subscribe("subject", cb=handler)

# nats-core — callback registered separately; MUST be a sync function
sub = await nc.subscribe("subject")
sub.add_callback(lambda msg: print(msg.data))  # sync, not async
```

**Breaking:** nats-core callbacks are **synchronous** (`Callable[[Message], None]`). In nats-py they were `async`. Avoid heavy work in nats-core callbacks as they block the I/O loop.

### Pull (Iterator) Subscriptions

```python
# nats-py
sub = await nc.subscribe("subject")
msg = await sub.next_msg(timeout=1.0)         # raises nats.errors.TimeoutError

# nats-core
sub = await nc.subscribe("subject")
msg = await sub.next(timeout=1.0)             # raises asyncio.TimeoutError
```

Key differences:
- Method renamed: `next_msg()` → `next()`
- Timeout exception changed: `nats.errors.TimeoutError` → `asyncio.TimeoutError`

### Async Iteration

```python
# nats-py
async for msg in sub.messages:               # property
    print(msg.data)

# nats-core — both forms work
async for msg in sub:                        # direct iteration (preferred)
    print(msg.data)

async for msg in sub.messages:              # property alias (for compatibility)
    print(msg.data)
```

### Queue Groups

```python
# nats-py — positional second argument
sub = await nc.subscribe("subject", "queue-group", handler)

# nats-core — keyword-only
sub = await nc.subscribe("subject", queue="queue-group")
```

### Pending Limits

```python
# nats-py
sub = await nc.subscribe(
    "subject",
    pending_msgs_limit=512,         # messages (default ~512MB worth)
    pending_bytes_limit=128*1024*1024,
)

# nats-core
sub = await nc.subscribe(
    "subject",
    max_pending_messages=65536,     # default 65536 messages (was named differently)
    max_pending_bytes=64*1024*1024, # default 64MB
)
# Pass None for unlimited
```

### Unsubscribe / Drain

```python
# nats-py
await sub.unsubscribe(limit=0)   # optional auto-unsub after N messages

# nats-core
await sub.unsubscribe()          # no limit parameter
await sub.drain()                # stops new messages but drains queue
```

### Subscription Context Manager

```python
# nats-py — no context manager

# nats-core
async with await nc.subscribe("subject") as sub:
    msg = await sub.next(timeout=1.0)
# automatically unsubscribed on exit
```

### Subscription Properties

| nats-py | nats-core |
|---|---|
| `sub.subject` | `sub.subject` |
| `sub.queue` | `sub.queue` |
| `sub.pending_msgs` (int) | `sub.pending` → `(msgs, bytes)` tuple |
| `sub.pending_bytes` (int) | `sub.pending` → `(msgs, bytes)` tuple |
| `sub.delivered` (int) | _(removed)_ |
| _(none)_ | `sub.dropped` → `(msgs, bytes)` dropped counts |
| `sub.messages` (AsyncIterator) | `sub.messages` (compat alias) + direct `async for sub:` |
| `sub.closed` (bool) | `sub.closed` |

---

## Messages

```python
# nats-py — dataclass with _client back-reference
@dataclass
class Msg:
    subject: str = ""
    reply: str = ""
    data: bytes = b""
    headers: Optional[Dict[str, str]] = None
    _client: NATS = ...

msg.header   # alias for msg.headers

# nats-core — slots dataclass, no client back-reference
@dataclass(slots=True)
class Message:
    subject: str
    data: bytes
    reply: str | None = None       # None instead of ""
    headers: Headers | None = None
    status: Status | None = None   # new: carries server status codes
```

Key changes:
- Class renamed: `Msg` → `Message`
- `reply` default: `""` → `None`
- `header` alias removed (use `headers`)
- `status` field added for server-side status responses
- No JetStream ack methods (`ack`, `nak`, `in_progress`, `term`) — use `nats-jetstream` package
- `msg.respond()` removed — use `nc.publish(msg.reply, data)` directly
- `msg.metadata` (JetStream) removed — use `nats-jetstream` package

---

## Headers

```python
# nats-py — plain dict[str, str]
headers = {"X-Trace-ID": "abc123"}
await nc.publish("subject", b"data", headers=headers)

msg.headers["X-Trace-ID"]    # returns str

# nats-core — custom Headers class with multi-value support
from nats.client import Headers

headers = Headers({"X-Trace-ID": "abc123"})
await nc.publish("subject", b"data", headers=headers)

# Can also pass a plain dict (auto-converted)
await nc.publish("subject", b"data", headers={"X-Trace-ID": "abc123"})

msg.headers.get("X-Trace-ID")           # returns first value as str
msg.headers.get_all("X-Trace-ID")       # returns list[str]
msg.headers.set("key", "value")
msg.headers.append("key", "value2")     # multi-value
msg.headers.delete("key")
msg.headers.asdict()                    # dict[str, list[str]]
```

nats-core headers store values as `list[str]` internally, supporting multi-value headers per the NATS spec. nats-py truncated multi-value headers to a single string.

---

## Callbacks & Event Handlers

### Registration

```python
# nats-py — registered at connect() time, must be async
await nats.connect(
    error_cb=error_cb,            # async def error_cb(e: Exception)
    disconnected_cb=disconnected_cb,  # async def disconnected_cb()
    closed_cb=closed_cb,          # async def closed_cb()
    reconnected_cb=reconnected_cb,    # async def reconnected_cb()
    discovered_server_cb=discovered_server_cb,
    lame_duck_mode_cb=lame_duck_mode_cb,
)

# nats-core — registered after connect(), must be sync
nc = await connect("nats://localhost:4222")
nc.add_error_callback(lambda e: print(f"Error: {e}"))      # sync
nc.add_disconnected_callback(lambda: print("Disconnected")) # sync
nc.add_reconnected_callback(lambda: print("Reconnected"))   # sync
```

**Breaking:**
- Callbacks are now registered **after** connect, not during
- Callbacks are **synchronous** (`Callable[[], None]`) — not async coroutines
- No `closed_cb`, `discovered_server_cb`, or `lame_duck_mode_cb` equivalents yet
- Multiple callbacks can be registered (additive, not replacing)

---

## Authentication

### Token

```python
# nats-py
await nats.connect(servers="nats://localhost:4222", token="mytoken")

# nats-core — also accepts callable for dynamic tokens
await connect("nats://localhost:4222", token="mytoken")
await connect("nats://localhost:4222", token=lambda: get_token())
```

### User + Password

```python
# nats-py
await nats.connect(servers="nats://localhost:4222", user="alice", password="secret")

# nats-core — also accepts callables
await connect("nats://localhost:4222", user="alice", password="secret")
await connect("nats://localhost:4222", user=lambda: get_user(), password=lambda: get_pass())
```

### NKey

```python
# nats-py
await nats.connect(servers="nats://localhost:4222", nkeys_seed="SUAGIEYF...")
# or from file
await nats.connect(servers="nats://localhost:4222", nkeys_seed_str="SUAGIEYF...")

# nats-core
from pathlib import Path
await connect("nats://localhost:4222", nkey="SUAGIEYF...")           # seed string
await connect("nats://localhost:4222", nkey=Path("seed.nk"))         # seed file
await connect("nats://localhost:4222", nkey=(pub_key_fn, sign_fn))   # custom handlers
```

### JWT / Credentials File

```python
# nats-py
await nats.connect(servers="nats://localhost:4222", user_credentials="path/to/file.creds")

# nats-core
from pathlib import Path
await connect("nats://localhost:4222", jwt=Path("file.creds"))           # .creds file
await connect("nats://localhost:4222", jwt=("jwt_str", "seed_str"))      # inline strings
await connect("nats://localhost:4222", jwt=(jwt_fn, sign_fn))            # custom handlers
```

---

## Error Classes

### nats-py errors (`nats.errors`)

All inherit from `nats.errors.Error(Exception)` and use a `"nats: ..."` prefix in `__str__`.

| Class | Meaning |
|---|---|
| `TimeoutError` | Request/flush timed out |
| `NoRespondersError` | No subscribers for request subject |
| `SlowConsumerError` | Subscription buffer full |
| `ConnectionClosedError` | Operation on closed connection |
| `ConnectionDrainingError` | Operation during drain |
| `ConnectionReconnectingError` | Operation during reconnect |
| `StaleConnectionError` | Ping timeout |
| `OutboundBufferLimitError` | Write buffer exceeded |
| `FlushTimeoutError` | Flush timed out |
| `DrainTimeoutError` | Drain timed out |
| `BadSubscriptionError` | Invalid subscription |
| `BadSubjectError` | Invalid subject |
| `AuthorizationError` | Auth failed |
| `NoServersError` | No server could be reached |
| `MaxPayloadError` | Message exceeds server max |
| `NotJSMessageError` | JetStream ack on non-JS message |
| `MsgAlreadyAckdError` | Double-ack attempt |
| + 10 more | |

### nats-core errors (`nats.client.errors`)

Minimal, focused set:

| Class | Meaning |
|---|---|
| `StatusError` | Server returned a status code (has `.status`, `.description`, `.subject`) |
| `NoRespondersError` | Subclass of `StatusError` for 503 no-responders |
| `SlowConsumerError` | Subscription buffer full (has `.subject`, `.sid`, `.pending_messages`, `.pending_bytes`) |

All other error conditions raise standard Python exceptions (`asyncio.TimeoutError`, `RuntimeError`, `OSError`, etc.) rather than custom types.

**Breaking:** `nats.errors.TimeoutError` → `asyncio.TimeoutError`

---

## Side-by-Side Examples

### Basic Publish / Subscribe

```python
# nats-py
import nats

nc = await nats.connect("nats://localhost:4222")

async def handler(msg):
    print(f"Received: {msg.data.decode()}")
    await msg.respond(b"pong")

sub = await nc.subscribe("ping", cb=handler)
await nc.publish("ping", b"hello")
await nc.drain()
```

```python
# nats-core
from nats.client import connect

nc = await connect("nats://localhost:4222")

sub = await nc.subscribe("ping")
await nc.publish("ping", b"hello")

msg = await sub.next(timeout=1.0)
print(f"Received: {msg.data.decode()}")
if msg.reply:
    await nc.publish(msg.reply, b"pong")   # respond manually

await nc.drain()
```

### Request / Reply

```python
# nats-py
msg = await nc.request("service", b"query", timeout=0.5)
print(msg.data)

# nats-core
msg = await nc.request("service", b"query", timeout=2.0)
print(msg.data)
```

### Queue Group

```python
# nats-py
sub = await nc.subscribe("work", "workers", handler)

# nats-core
sub = await nc.subscribe("work", queue="workers")
async for msg in sub:
    process(msg)
```

### Reconnect Callbacks

```python
# nats-py
async def on_reconnect():
    print("Reconnected!")

nc = await nats.connect("nats://localhost:4222", reconnected_cb=on_reconnect)

# nats-core
nc = await connect("nats://localhost:4222")
nc.add_reconnected_callback(lambda: print("Reconnected!"))
```

### Error Handling on Timeout

```python
# nats-py
import nats

try:
    msg = await sub.next_msg(timeout=1.0)
except nats.errors.TimeoutError:
    print("No message")

# nats-core
import asyncio

try:
    msg = await sub.next(timeout=1.0)
except asyncio.TimeoutError:
    print("No message")
```

---

## Quick Migration Checklist

- [ ] Change `import nats` → `from nats.client import connect`
- [ ] Change `nats.connect(servers=...)` → `connect(url)` (single URL string, positional)
- [ ] Rename `connect_timeout` → `timeout`
- [ ] Rename `max_reconnect_attempts` → `reconnect_max_attempts`
- [ ] Rename `dont_randomize` → `no_randomize`
- [ ] Rename `nkeys_seed` → `nkey`; `user_credentials` → `jwt`
- [ ] Move event callbacks from `connect()` kwargs to `nc.add_*_callback()` calls
- [ ] Make all callbacks **synchronous** (remove `async def` from callbacks)
- [ ] Change `nc.is_connected` / `nc.is_closed` → `nc.status == ClientStatus.*`
- [ ] Change subscribe callback: `await nc.subscribe("s", cb=fn)` → `sub = await nc.subscribe("s"); sub.add_callback(fn)`
- [ ] Change `sub.next_msg(timeout=...)` → `sub.next(timeout=...)`
- [ ] Catch `asyncio.TimeoutError` instead of `nats.errors.TimeoutError`
- [ ] Change `msg.respond(data)` → `await nc.publish(msg.reply, data)`
- [ ] Change `msg.header` → `msg.headers`
- [ ] Update header access from `msg.headers["key"]` → `msg.headers.get("key")`
- [ ] Change `sub.pending_msgs` / `sub.pending_bytes` → `sub.pending` (returns tuple)
- [ ] JetStream features (`msg.ack()`, `msg.nak()`, etc.) → use `nats-jetstream` package
