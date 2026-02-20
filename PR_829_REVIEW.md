# Review: PR #829 — Add server pool management and reconnect handler

**PR:** https://github.com/nats-io/nats.py/pull/829
**Author:** caspervonb
**Branch:** `add-custom-server-pool` → `main`
**Reviewed at:** commit `f98db4d`
**Diff base:** upstream/main (`1e6c44d`)

---

## Summary

This PR adds two features to the NATS client:

1. **Dynamic server pool management** — `server_pool` property and `set_server_pool()` method to inspect and replace the server pool at runtime
2. **Custom reconnect handler** — `reconnect_to_server_handler` callback that lets applications control which server to reconnect to and with what delay

**Files changed:** 3 (`client.py` +211/-51, `errors.py` +5, `test_client.py` +323)

### What's Good

- Clean public API surface: `Server` dataclass exposes only `uri` + `reconnects`, keeping internal `Srv` state private
- `server_pool` returns a copy, preventing accidental mutation of internal state
- `set_server_pool()` validates all URLs atomically before modifying state
- State preservation: reconnect counts, `did_connect`, `last_attempt` are carried over when servers match by `netloc`
- `_current_server` is updated to point into the new pool, preventing stale references
- Connection logic extracted into `_connect_to_server()` — good refactoring that reduces duplication between `_select_next_server` and the new handler path
- URL parsing extracted into `_parse_server_uri()` static method — reusable and testable
- Test coverage is thorough: 9 test cases covering pool property, pool replacement, state preservation, closed-state error, reconnect with updated pool, handler selection, delay, invalid server fallback, and server_info passthrough
- Handler receives server snapshots (not internal objects), so user code can't corrupt client state

---

## Issues

### 1. BUG: Handler exceptions crash the reconnection task (High)

**File:** `nats/src/nats/aio/client.py`, reconnect loop in `_attempt_reconnect`

The handler is called inside the reconnect `while True` loop:

```python
selected, callback_delay = self._reconnect_to_server_handler(server_snapshot, self._server_info)
```

The exception handlers only catch `errors.NoServersError`, `(OSError, errors.Error, asyncio.TimeoutError)`, and `asyncio.CancelledError`. If the user's handler raises any other exception (e.g., `TypeError`, `ValueError`, `KeyError`, `RuntimeError`), it propagates out of the loop uncaught, killing the reconnection task. The client then sits in `RECONNECTING` status forever with no way to recover.

**Suggestion:** Wrap the handler call in a `try/except Exception` that reports the error via `_error_cb` and falls back to `eligible[0]`:

```python
try:
    selected, callback_delay = self._reconnect_to_server_handler(server_snapshot, self._server_info)
except Exception as e:
    await self._error_cb(e)
    selected = None
    callback_delay = 0
```

---

### 2. BUG: Handler is synchronous but `_server_info` dict is mutable (Medium)

**File:** `nats/src/nats/aio/client.py`, line with `self._reconnect_to_server_handler(server_snapshot, self._server_info)`

The handler receives `self._server_info` directly — the same mutable dict used internally by the client. While the `Server` objects in the snapshot are copies, the `server_info` dict is not. A handler that modifies it (e.g., `server_info.pop("nonce")`) would corrupt client state.

**Suggestion:** Pass a shallow copy: `dict(self._server_info)` or `self._server_info.copy()`.

---

### 3. BUG: `_connect_to_server` doesn't increment `reconnects` on failure (Medium)

**File:** `nats/src/nats/aio/client.py`, `_attempt_reconnect` — handler path

In the default `_select_next_server` path, when a connection attempt fails, both `s.reconnects` and `s.last_attempt` are incremented/updated in the `except` block:

```python
except Exception as e:
    s.last_attempt = time.monotonic()
    s.reconnects += 1
```

In the handler path, when `_connect_to_server` raises, the outer `except` block does update `self._current_server.reconnects += 1`. However, the eligible-server filtering happens *before* the connection attempt:

```python
eligible = [s for s in self._server_pool if s.reconnects <= max_reconnect]
```

This means the reconnect count is correctly tracked. But there's a subtle issue: the handler path doesn't cycle through servers on failure — it calls the handler again on the next iteration, which may return the same server repeatedly. The `_select_next_server` path pops servers from the pool and rotates them, giving each server a turn. With the handler, a poorly-written handler that always returns the same down server will cause a tight retry loop (mitigated only by `reconnect_time_wait` in the default path... which is skipped in the handler path).

**Suggestion:** Consider adding a minimum backoff between handler-path reconnect attempts, or at least document that the handler is responsible for implementing its own backoff via the `delay` return value. Without any delay, a failing server causes a tight loop of connect attempts.

---

### 4. `_setup_server_pool` list path not updated to use `_parse_server_uri` (Low)

**File:** `nats/src/nats/aio/client.py`, `_setup_server_pool`

The single-string path was refactored to call `_parse_server_uri()`, but the list path still uses raw `urlparse()`:

```python
elif isinstance(connect_url, list):
    try:
        for server in connect_url:
            uri = urlparse(server)  # <-- raw urlparse, no scheme/port defaults
            self._server_pool.append(Srv(uri))
```

This means `set_server_pool(["localhost"])` correctly becomes `nats://localhost:4222` (via `_parse_server_uri`), but `connect(servers=["localhost"])` would create a `Srv` with scheme `""` and port `None`.

**Note:** This is a pre-existing issue, not introduced by this PR. But since `_parse_server_uri` was extracted specifically to centralize this logic, the list path in `_setup_server_pool` should also use it for consistency.

---

### 5. `set_server_pool` doesn't validate against mixed protocols (Low)

**File:** `nats/src/nats/aio/client.py`, `set_server_pool`

`_setup_server_pool` validates that WebSocket and non-WebSocket URLs aren't mixed:

```python
if not (
    all(server.uri.scheme in ("nats", "tls") for server in self._server_pool)
    or all(server.uri.scheme in ("ws", "wss") for server in self._server_pool)
):
    raise errors.Error("nats: mixing of websocket and non websocket URLs is not allowed")
```

`set_server_pool` skips this check. A user could replace the pool with `["nats://a:4222", "ws://b:80"]`, which would cause unpredictable transport behavior on reconnect.

**Suggestion:** Add the same protocol-mixing validation to `set_server_pool`.

---

### 6. `Server` and `Srv` duplication could be confusing (Nit)

**File:** `nats/src/nats/aio/client.py`

Having both `Server` (public) and `Srv` (internal) is reasonable for encapsulation, but the names are very similar. Consider renaming `Server` to `ServerInfo` or `ServerEntry` to better distinguish it, or adding a brief note in the `Server` docstring that it's the public-facing subset of the internal `Srv` type.

---

### 7. `ReconnectToServerHandler` is synchronous-only (Nit)

**File:** `nats/src/nats/aio/client.py`, type alias

```python
ReconnectToServerHandler = Callable[[List[Server], Dict[str, Any]], Tuple[Optional[Server], float]]
```

The handler is defined as a synchronous callable. This is fine and arguably correct (simpler, no event loop concerns), but it means handlers cannot do async work like DNS lookups or health checks. This is worth documenting explicitly, since other NATS callbacks (`reconnected_cb`, `disconnected_cb`, etc.) are all async.

---

### 8. Type annotation: `_setup_server_pool` parameter (Nit)

**File:** `nats/src/nats/aio/client.py`

```python
def _setup_server_pool(self, connect_url: Union[List[str]]) -> None:
```

`Union[List[str]]` is equivalent to `List[str]` — `Union` with a single type is a no-op. Based on the body, this should be `Union[str, List[str]]`.

**Note:** Pre-existing issue, not introduced by this PR.

---

## Summary Table

| # | Issue | Severity | New/Pre-existing |
|---|-------|----------|------------------|
| 1 | Handler exceptions crash reconnect task | **High** | New |
| 2 | `_server_info` passed by reference to handler | **Medium** | New |
| 3 | No backoff in handler path on connection failure | **Medium** | New |
| 4 | `_setup_server_pool` list path doesn't use `_parse_server_uri` | Low | Pre-existing |
| 5 | `set_server_pool` missing protocol-mixing validation | Low | New |
| 6 | `Server` vs `Srv` naming | Nit | New |
| 7 | Handler is sync-only (should document) | Nit | New |
| 8 | `Union[List[str]]` type annotation | Nit | Pre-existing |

### Recommendation

**Request changes** on issues #1 and #2 — the handler exception crash and mutable `_server_info` exposure are correctness bugs that should be fixed before merge. Issue #3 (no backoff) is worth discussing. The rest are low-severity or nits.
