# PR #829 Review: Add server pool management, reconnect handler, and new JetStream package

**Author:** Casper Beyer (@caspervonb)
**Branch:** `add-custom-server-pool` -> `main`
**Size:** ~14,400 additions across 87 files

---

## Overview

This PR bundles three major areas of work:

1. **Server pool management & reconnect handler** for the core NATS client
2. **Datetime handling fixes** in the existing JetStream API (`nats/src/nats/js/api.py`)
3. **A brand-new `nats-jetstream` package** — a complete rewrite/redesign of the JetStream client

The scope is very large. It would benefit from being split into at least 2-3 separate PRs for easier review and safer integration (server pool changes, datetime fixes, new JetStream package).

---

## 1. Server Pool Management & Reconnect Handler

**Files:** `nats/src/nats/aio/client.py`, `nats/src/nats/errors.py`, `nats/tests/test_client.py`

### What it does

- Adds a new `Server` dataclass (public) alongside the existing internal `Srv`
- Adds `server_pool` property (returns a defensive copy) and `set_server_pool()` method
- Adds `reconnect_to_server_handler` callback parameter to `connect()`
- Extracts `_parse_server_uri()` and `_connect_to_server()` as reusable helpers
- Introduces `ServerNotInPoolError`

### Positive

- Clean extraction of `_parse_server_uri()` — eliminates duplication between `_setup_server_pool` and `set_server_pool`
- `server_pool` returns a defensive copy, which is correct
- `set_server_pool` preserves reconnect state from existing pool entries (good)
- Handler receives snapshots, not live internal objects — prevents mutation bugs
- Test coverage is solid: 10 tests covering pool access, immutability, state preservation, reconnection, handler invocation, delay, invalid selection, and server info

### Issues

#### High

1. **`Server` vs `Srv` duplication (`client.py:122-130` vs `client.py:133-143`)**: Two dataclasses represent the same concept. `Server` is the public API; `Srv` is internal. This is understandable for encapsulation, but adds cognitive overhead. Consider whether `Srv` could be refactored to extend `Server`, or at least add a clear comment explaining the relationship.

2. **Handler type signature is synchronous but called in async context (`client.py:147`)**: `ReconnectToServerHandler = Callable[[List[Server], Dict[str, Any]], Tuple[Optional[Server], float]]` is a synchronous callable, but if a user needs to do async work (e.g., consult a service discovery endpoint), they can't. Consider also accepting an `Awaitable` return or making the type `Union[Callable[..., Tuple], Callable[..., Awaitable[Tuple]]]`.

3. **Missing `reconnects` increment on handler path failure (`client.py:1535-1567`)**: When `_connect_to_server` fails in the handler path, the exception falls through to the `except (OSError, errors.Error, asyncio.TimeoutError)` block at line ~1515, which increments `self._current_server.reconnects`. This works, but the handler re-selects on every loop iteration, potentially selecting the same failing server repeatedly — the round-robin `_select_next_server` naturally cycles, while the handler path does not, so it could get stuck in an infinite retry loop on the same server if the handler always picks the same one.

4. **No `_transport` cleanup before `_connect_to_server` in handler path**: The default path goes through `_select_next_server`, which starts fresh. But the handler path at line 1567 calls `_connect_to_server(self._current_server)` directly. If a previous connection attempt partially initialized `self._transport`, the new `_connect_to_server` checks `if not self._transport` (line ~1388) and will reuse the old (possibly broken) transport object rather than creating a new one.

#### Medium

5. **`set_server_pool` is not thread-safe**: `_server_pool` is reassigned atomically (Python GIL guarantees reference assignment), but if a reconnect attempt is in progress reading `_server_pool`, the list could change underneath it. Consider documenting this caveat or adding a note about thread safety.

6. **`set_server_pool` missing `_current_server` update when pool is empty (`client.py:1224-1232`)**: If `new_pool` is empty, `_current_server` retains its old value, which now points to a server not in the pool. Consider raising `ValueError` if an empty server list is provided, or explicitly setting `_current_server = None`.

7. **`_parse_server_uri` is now a `@staticmethod` (`client.py:1340`)**: This is fine, but it was inlined before with comments explaining behavior. The comments were lost in the extraction. Consider keeping the "Closer to how the Go client handles this" and similar inline notes for maintainability.

#### Low

8. **`ReconnectToServerHandler` type hint imports**: The type alias at line 147 uses `Callable`, `List`, `Dict`, `Tuple`, `Optional` from `typing`. With `from __future__ import annotations` at the top, these could use native Python syntax (`list[Server]`, `dict[str, Any]`, etc.) for consistency with the rest of the file.

---

## 2. Datetime Handling Fixes (`nats/src/nats/js/api.py`)

### Positive

- Resolves multiple long-standing `# FIXME` comments
- Consolidates duplicated ISO 8601 parsing into shared `_parse_utc_iso` and `_to_utc_iso` helpers
- Good test coverage with 14 unit tests plus an integration test

### Issues

9. **`_parse_utc_iso` fails on negative UTC offsets**: The line `frac, tz = frac_tz.split("+")` will raise `ValueError` for timestamps like `2024-01-01T00:00:00.000-05:00`. NATS server always emits `Z`, so this is low risk in practice, but the method name implies general ISO 8601 support.

10. **`ConsumerInfo.created` is now a required field**: Previously omitted (commented out), now `created: datetime.datetime` with no default. This is a breaking change for anyone constructing `ConsumerInfo` directly (e.g., in tests/mocks). Consider adding a default value or documenting the breaking change.

11. **`_to_utc_iso` string passthrough**: Accepting `str` and passing through without validation could silently allow malformed timestamps.

---

## 3. New `nats-jetstream` Package

**Files:** `nats-jetstream/` (entire new package)

### Architecture

The new package is well-structured:
- `__init__.py` — `JetStream` entry point, publish, stream/consumer CRUD, pagination
- `api/client.py` — Low-level API request/response handling with error mapping
- `api/types.py` — TypedDict definitions generated from JSON schemas
- `consumer/` — `Consumer` ABC, `ConsumerInfo`, `MessageBatch`/`MessageStream` protocols
- `consumer/pull.py` — `PullConsumer`, `PullMessageBatch`, `PullMessageStream`
- `stream.py` — `Stream`, `StreamConfig`, `StreamInfo`, `StreamMessage`, etc.
- `message.py` — `Message` with ack/nak/term, `Metadata` parsed from reply subjects
- `errors.py` — Error hierarchy with specific error types mapped from server error codes

### Positive

- Uses `match/case` for reply subject parsing — clean and idiomatic Python 3.10+
- Well-documented with docstrings, ADR-37 references, and clear type annotations
- `from_response` pattern with `strict` mode for forward compatibility with new server fields
- `from_response` uses `dict.pop()` to consume fields, detecting unconsumed fields in strict mode — clever approach
- Proper heartbeat pause/resume on disconnect/reconnect (ADR-37)
- Priority group and priority support in pull consumer
- Clean separation between `fetch` (one-shot batch) and `messages` (continuous stream)
- Generated TypedDict types from JSON schemas ensure API coverage
- Good test coverage (5 test files, ~3300 lines)

### Issues

#### Critical

12. **PEP 695 generic syntax breaks Python 3.11 (`api/client.py:92,360,370,379`)**: The code uses `def check_response[ResponseT](...)` syntax which requires Python 3.12+. However, `pyproject.toml` claims `requires-python = ">=3.11"`. This will cause a `SyntaxError` on Python 3.11. Either bump the minimum to 3.12 or rewrite using `TypeVar`.

#### High

13. **Duplicate code in `get_message` and `get_last_message_for_subject` (`__init__.py:727-813`)**: These two methods are nearly identical (~40 lines each), differing only in the API call parameters. The same decoding logic also appears a third time in `stream.py:_get_message()`. Extract a shared helper.

14. **`PullMessageBatch` disconnect/reconnect callbacks are never unregistered (`consumer/pull.py:79-83`)**: `add_disconnected_callback` and `add_reconnected_callback` are called in `__init__`, but there's no corresponding removal in cleanup. If many batches are created, this leaks callbacks and keeps batch objects alive. The same issue exists for `PullMessageStream` (line 277-279).

15. **`_request_loop` uses polling (`consumer/pull.py:430`)**: `await asyncio.sleep(0.1)` in a tight loop is wasteful. The `_heartbeat_monitor` (line 451) has the same issue. Consider using an `asyncio.Event` or `asyncio.Condition` to wake only when thresholds are crossed.

16. **`PullConsumer.get_info()` doesn't actually refresh (`consumer/pull.py:518-520`)**: The comment says "Refresh info from server" but the method just returns `self._info` without making any API call. This is a bug.

17. **`publish()` doesn't check for JetStream API errors in response (`__init__.py:334`)**: `json.loads(response.data)` is called and passed directly to `PublishAck.from_response`. If the server returns a JetStream error response (e.g., `{"error": {...}}`), the code will get a `KeyError` on `data.pop("stream")` instead of a meaningful error.

18. **Direct message get hardcodes `$JS.API` prefix (`stream.py:1086`)**: `_get_message` uses `f"$JS.API.DIRECT.GET.{self._name}"` ignoring the configurable `_prefix`. If a custom domain is used, direct gets will fail.

#### Medium

19. **`publish` method uses `asyncio.get_event_loop()` (`__init__.py:303,310,344`)**: Should be `asyncio.get_running_loop()` — `get_event_loop()` is deprecated since Python 3.10 and may create a new loop if none is running.

20. **`PullConsumer` does not satisfy the `Consumer` Protocol**: The Protocol's `fetch` defines positional `max_messages`, while `PullConsumer.fetch` uses keyword-only `max_messages`. A static type checker would flag this as non-conforming.

21. **`ConsumerConfig.opt_start_time` typed as `int | None` (`consumer/__init__.py:111`)**: The API sends/receives this as an ISO 8601 string. Should be `str | None` or `datetime | None`. Same issue with `StreamSource.opt_start_time` (`stream.py:135`).

22. **`ConsumerConfig.to_request()` skips default-valued fields (`consumer/__init__.py:229-233`)**: If `ack_policy` is explicitly set to `"none"`, it won't be included in the request due to `if self.ack_policy != "none"`. This means you can't explicitly set default values in an update request.

23. **`from_response` pattern mutates input dicts**: Every `from_response` call uses `data.pop()`, which mutates the input dictionary. If a caller needs to inspect the original response after calling `from_response`, the data will be gone. Consider working on a copy, or document this behavior clearly.

24. **`Message.__init__` silently swallows `ValueError` from reply parsing (`message.py:168-172`)**: If the reply subject is malformed, the message gets default metadata with zeroed sequences and `datetime.now()`. This could mask bugs. At minimum, log a warning.

25. **`PullMessageStream.__anext__` swallows all exceptions (`consumer/pull.py:319`)**: `except Exception: await self._cleanup(); raise StopAsyncIteration` converts any exception (including `ConnectionClosedError`) into a silent stream termination. The user will never know why the stream stopped.

26. **Deep private attribute chains**: `self._consumer._stream._jetstream._client` (4 levels of private access, `consumer/pull.py:279,387`). This violates Law of Demeter and is fragile. Consider passing the client or a facade directly.

27. **`stream_names` and `list_streams` return `AsyncIterator` but are `async def` with `yield` (`__init__.py:352-415`)**: These are actually `AsyncGenerator`s. While `AsyncGenerator` is a subtype of `AsyncIterator`, the return type annotation could be more precise for tooling.

28. **`config` and `state` typed as `Any` in API types (`api/types.py:547,558`)**: `StreamInfo`, `StreamInfoResponse`, etc. type these as `Any` instead of `StreamConfig`/`StreamState`. This defeats the purpose of TypedDict type safety.

29. **`request_json` has hardcoded 5s timeout (`api/client.py:386`)**: No caller overrides it and there's no user-facing configuration. Long operations (e.g., stream purge on large streams) could time out unexpectedly.

30. **`NoRespondersError` handling is inconsistent across API methods**: Only `account_info()` wraps `NoRespondersError` into `JetStreamNotEnabledError`. All other methods propagate the raw error.

31. **No `__repr__` on key types**: `Message`, `Consumer`, `Stream` lack `__repr__`, making debugging harder. Dataclasses get this for free but these non-dataclass types don't.

#### Low

32. **`StreamConfig.from_kwargs` / `to_request` round-trip**: Many fields (~50+) are manually mapped. This is a common source of subtle bugs when new fields are added. Consider generating these mappings.

33. **Package uses `setuptools` (`pyproject.toml:1-3`)**: The root project appears to use `uv`. Consider aligning build backends.

34. **`nats-jetstream` depends on `nats-core` but this internal dependency isn't documented**: The relationship between workspace packages needs clarification for users.

35. **`StreamNamesResponse` has a `consumers` field (`api/types.py:1473`)**: A stream names response should not have a `consumers` field. Likely a schema generation bug.

36. **Duplicate `DeliverPolicy` types (`api/types.py:270-333`)**: Two sets of identical deliver policy TypedDicts exist; the first set appears unused.

37. **Empty `README.md` and missing `py.typed` marker**: The README is 0 bytes. For PEP 561 type-checking support, a `py.typed` marker file should be included.

38. **Redundant `new()` factory function (`__init__.py:816-828`)**: Adds no value over calling the `JetStream()` constructor directly.

---

## 4. Test Reliability (`nats-core/tests/test_examples.py`)

39. **`run_example_with_retry` can return `None`**: If the overall timeout expires before any attempt completes, the function returns `None`, but callers do `assert req_result.returncode == 0` without a None guard. This would produce a confusing `AttributeError` instead of a clear test failure.

---

## 5. Build/Config Changes

40. **Ruff `target-version` bumped from `py311` to `py312` (`pyproject.toml`)**: This should be coordinated with the actual minimum supported Python version. If the project still supports 3.11, this could mask lint warnings about 3.12-only syntax. (This is directly related to issue #12 — the new JetStream package uses 3.12+ syntax.)

---

## Summary

| Severity | Count |
|----------|-------|
| Critical | 1     |
| High     | 10    |
| Medium   | 13    |
| Low      | 7     |

### Top Recommendations

1. **Fix Python 3.11 compatibility** (#12) or explicitly bump minimum version — this is a ship-blocker
2. **Split the PR**: Server pool changes, datetime fixes, and the new JetStream package should be separate PRs
3. **Fix the transport reuse bug** (#4) in the reconnect handler path
4. **Fix callback leaks** (#14) — add unregistration in cleanup paths
5. **Replace polling loops** (#15) with event-driven signaling
6. **Implement `PullConsumer.get_info()`** (#16) — currently a no-op despite its docstring
7. **Handle JetStream errors in `publish()`** (#17) — currently raises `KeyError`
8. **Fix hardcoded prefix** (#18) in direct message get
9. **Fix `get_event_loop()` deprecation** (#19)
10. **Add async handler support** (#2) or document the limitation

The overall code quality is good — well-documented, well-tested, and follows consistent patterns. The new JetStream package is a significant step forward in API design compared to the existing `nats.js` module. The main concerns are the Python 3.11 compatibility break, several bugs in the pull consumer, and the sheer scope of the PR making it hard to review holistically.
