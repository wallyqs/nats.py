# Re-Review: nats.py main branch (post-merge of JetStream, datetime, and test fixes)

**Reviewed at:** `a9bfe71` (origin/main)
**Original PR:** #829 (`add-custom-server-pool`)
**Date:** 2026-02-20

---

## Context

The original PR #829 was split into separate commits now merged to main:

| Commit | PR | Description |
|--------|----|-------------|
| `f7a3c3d` | #734 | Add nats-jetstream package |
| `1e6c44d` | #823 | Fix opt_start_time and other datetime fields |
| `1506159` | #824 | Fix flaky example tests |
| `3f13f5d` | #822 | Release nats-jetstream v0.1.0 |
| `a9bfe71` | #644 | Fix watching past history replay |

**Not merged:** The server pool management and reconnect handler changes (issues #1-8 from the original review). The `add-custom-server-pool` branch has been deleted.

---

## Re-Review Status

### Issues from original review that were addressed

| # | Issue | Status | Notes |
|---|-------|--------|-------|
| 1-8 | Server pool management issues | **OUT OF SCOPE** | Not merged; will need re-review when/if resubmitted |
| 39 | `run_example_with_retry` can return `None` | **MOSTLY FIXED** | Now returns `result` (last attempt) instead of `None` in most cases. Still returns `None` if deadline expires before any attempt runs, but this is a theoretical edge case |

### Issues from original review that are STILL PRESENT on main

All JetStream package issues remain unchanged. The merged code is identical to what was in the PR branch.

---

## 1. Datetime Handling Fixes (merged as #823)

**Files:** `nats/src/nats/js/api.py`

### Positive

- Resolves multiple long-standing `# FIXME` comments
- Consolidates duplicated ISO 8601 parsing into shared `_parse_utc_iso` and `_to_utc_iso` helpers
- Good test coverage with 14 unit tests plus an integration test

### Still Present

1. **`_parse_utc_iso` fails on negative UTC offsets** (Low): The line `frac, tz = frac_tz.split("+")` will raise `ValueError` for timestamps like `2024-01-01T00:00:00.000-05:00`. NATS server always emits `Z`, so this is low risk, but the method name implies general ISO 8601 support.

2. **`ConsumerInfo.created` is now a required field** (Medium): Previously omitted, now `created: datetime.datetime` with no default. Breaking change for anyone constructing `ConsumerInfo` directly (e.g., in tests/mocks).

3. **`_to_utc_iso` string passthrough** (Low): Accepting `str` and passing through without validation could silently allow malformed timestamps.

---

## 2. New `nats-jetstream` Package (merged as #734, released as #822)

**Files:** `nats-jetstream/` (entire package, now at v0.1.0)

### Architecture

Well-structured package:
- `__init__.py` — `JetStream` entry point, publish, stream/consumer CRUD, pagination
- `api/client.py` — Low-level API request/response handling with error mapping
- `api/types.py` — TypedDict definitions generated from JSON schemas
- `consumer/` — `Consumer` Protocol, `ConsumerInfo`, `MessageBatch`/`MessageStream`
- `consumer/pull.py` — `PullConsumer`, `PullMessageBatch`, `PullMessageStream`
- `stream.py` — `Stream`, `StreamConfig`, `StreamInfo`, `StreamMessage`, etc.
- `message.py` — `Message` with ack/nak/term, `Metadata` parsed from reply subjects
- `errors.py` — Error hierarchy with specific error types mapped from server error codes

### Positive

- Uses `match/case` for reply subject parsing — clean and idiomatic
- Well-documented with docstrings, ADR-37 references, and clear type annotations
- `from_response` pattern with `strict` mode for forward compatibility
- Proper heartbeat pause/resume on disconnect/reconnect (ADR-37)
- Priority group and priority support in pull consumer
- Clean separation between `fetch` (one-shot batch) and `messages` (continuous stream)
- Generated TypedDict types from JSON schemas ensure API coverage
- Good test coverage (5 test files, ~3300 lines)

### Issues — Still Present

#### Critical

4. **PEP 695 generic syntax breaks Python 3.11 (`api/client.py:92,360,370,379`)**: Uses `def check_response[ResponseT](...)` syntax (Python 3.12+). `pyproject.toml` claims `requires-python = ">=3.11"` and lists `Programming Language :: Python :: 3.11` in classifiers. This causes `SyntaxError` on Python 3.11. **This is a ship-blocker for any 3.11 user — the package is broken on import.**

#### High — Bugs

5. **`PullConsumer.get_info()` doesn't refresh from server (`consumer/pull.py:518-520`)**: Comment says "Refresh info from server" but implementation just returns cached `self._info`. No API call is made.

6. **`publish()` doesn't handle JetStream error responses (`__init__.py:334`)**: `json.loads(response.data)` is passed directly to `PublishAck.from_response`. If the server returns `{"error": {...}}`, the code raises `KeyError` on `data.pop("stream")` instead of a meaningful `JetStreamError`.

7. **Direct message get hardcodes `$JS.API` prefix (`stream.py:1086,1091`)**: `_get_message` uses `f"$JS.API.DIRECT.GET.{self._name}"` ignoring the configurable `_prefix` on the API client. Custom domain configurations will break.

#### High — Resource Leaks

8. **Disconnect/reconnect callbacks never unregistered (`consumer/pull.py:82-83,279-280`)**: `PullMessageBatch.__init__` and `PullMessageStream.__init__` call `add_disconnected_callback` / `add_reconnected_callback` but never remove them. In long-running applications that create many batches/streams, this leaks callbacks and prevents garbage collection of completed batch/stream objects.

#### High — Performance

9. **Polling loops with `asyncio.sleep(0.1)` (`consumer/pull.py:430,451`)**: Both `_request_loop` and `_heartbeat_monitor` poll every 100ms. Should use `asyncio.Event` or `asyncio.Condition` for event-driven wakeup.

#### High — Code Duplication

10. **Triplicated message decoding (`__init__.py:727-813`, `stream.py:_get_message`)**: `get_message()`, `get_last_message_for_subject()`, and `Stream._get_message()` all contain nearly identical ~40-line blocks for decoding base64 data, parsing headers, and constructing `StreamMessage`.

#### Medium

11. **`asyncio.get_event_loop()` deprecated (`__init__.py:303,310,344`)**: Should use `asyncio.get_running_loop()`. `get_event_loop()` is deprecated since Python 3.10, emits `DeprecationWarning` in 3.12+, and may create a new event loop if none is running.

12. **`PullConsumer` doesn't satisfy `Consumer` Protocol**: Protocol's `fetch` has positional `max_messages`; `PullConsumer.fetch` uses keyword-only. Static type checkers flag this as non-conforming.

13. **`ConsumerConfig.opt_start_time` typed as `int | None` (`consumer/__init__.py:111`)**: API sends/receives as ISO 8601 string. Should be `str | None` or `datetime | None`. Same for `StreamSource.opt_start_time` (`stream.py:135`).

14. **`ConsumerConfig.to_request()` skips default-valued fields (`consumer/__init__.py:229-233`)**: `if self.ack_policy != "none"` means explicitly setting `ack_policy="none"` won't include it in the request. Same pattern for other fields.

15. **`PullMessageStream.__anext__` swallows all exceptions (`consumer/pull.py:319`)**: `except Exception: ... raise StopAsyncIteration` silently converts `ConnectionClosedError`, `RuntimeError`, etc. into stream termination with no diagnostics.

16. **`from_response` mutates input dicts**: Every `from_response` uses `data.pop()`, destructively consuming the input. If a caller logs or debugs the response after parsing, it's empty.

17. **`Message.__init__` silently swallows reply parse errors (`message.py:168-172`)**: Malformed reply subjects get default metadata with zeroed sequences and `datetime.now()`, masking bugs.

18. **Deep private attribute chains (`consumer/pull.py:279,387`)**: `self._consumer._stream._jetstream._client` — 4 levels of private access. Fragile and hard to refactor.

19. **`config`/`state` typed as `Any` in API types (`api/types.py:547,558`)**: `StreamInfo`, response types, etc. lose type safety.

20. **`request_json` hardcoded 5s timeout (`api/client.py:386`)**: No caller overrides it; no user-facing configuration for long-running operations.

21. **`NoRespondersError` handling inconsistent**: Only `account_info()` wraps it into `JetStreamNotEnabledError`. Other methods propagate the raw NATS error.

22. **No `__repr__` on `Message`, `Consumer`, `Stream`**: Makes debugging harder — these non-dataclass types show unhelpful default repr.

23. **`stream_names`/`list_streams` annotated as `AsyncIterator` but are `AsyncGenerator`s**: Technically works but imprecise for tooling.

#### Low

24. **~50+ manually mapped fields in `StreamConfig.from_kwargs`/`to_request`**: Error-prone when new fields are added. Consider generating.

25. **`setuptools` backend vs `uv` workspace**: Package uses `setuptools` while root uses `uv`.

26. **`nats-jetstream` -> `nats-core` dependency undocumented**: Users need guidance on installing.

27. **`StreamNamesResponse` has `consumers` field (`api/types.py:1473`)**: Schema generation bug — stream names response shouldn't have this.

28. **Duplicate `DeliverPolicy` TypedDicts (`api/types.py:270-333`)**: Two identical sets; first appears unused.

29. **Empty `README.md`, no `py.typed` marker**: Package released as v0.1.0 without documentation or PEP 561 support.

30. **Redundant `new()` factory (`__init__.py:816-828`)**: Identical to `JetStream()` constructor.

---

## 3. New: Fix Watching Past History Replay (commit `a9bfe71`, #644)

**Files:** `nats/src/nats/js/kv.py`, `nats/src/nats/js/object_store.py`

### What it does

Fixes a bug where `None` entries in the watcher queue prematurely stopped async iteration. Previously, `__anext__` checked `if not entry: raise StopAsyncIteration`, which stopped on `None` values (used for "caught up" signaling) as well as the actual end-of-iteration. The fix introduces a `StopIterSentinel` class so only explicit stop signals terminate iteration.

### Positive

- Clean sentinel pattern avoids the `None` ambiguity
- Applied consistently to both `KeyValue.KeyWatcher` and `ObjectStore.ObjectWatcher`
- The `keys()` method now correctly breaks on `None` (caught-up signal) rather than relying on the iterator

### Issues

31. **`asyncio.get_event_loop()` in `run_example_with_retry` (`nats-core/tests/test_examples.py:44,46`)**: Same deprecation as issue #11, now in test code. Should use `asyncio.get_running_loop()`.

32. **`StopIterSentinel` is module-level in `kv.py` and imported by `object_store.py`**: A minor coupling concern — both watchers share the same sentinel class. This is fine for now but could be moved to a shared `_utils` module if more watchers are added.

---

## 4. Build/Config

33. **Ruff `target-version` = `py312` (`pyproject.toml`)**: Coordinated with actual usage of 3.12+ syntax in the JetStream package. However, `nats-jetstream/pyproject.toml` still claims `>=3.11`. These should agree.

---

## Summary

| Severity | Count | Fixed | Still Present |
|----------|-------|-------|---------------|
| Critical | 1     | 0     | 1             |
| High     | 6     | 0     | 6             |
| Medium   | 13    | 0     | 13            |
| Low      | 10    | 0     | 10            |
| Out of scope | 8 | — | — |
| **Total** | **33** (+ 8 deferred) | **0** | **33** |

### Top Priorities

1. **Fix Python 3.11 compatibility** (#4) — package is broken on import for 3.11 users. Either bump `requires-python` to `>=3.12` or rewrite PEP 695 generics using `TypeVar`
2. **Fix `PullConsumer.get_info()`** (#5) — documented as refreshing but returns stale data
3. **Fix `publish()` error handling** (#6) — `KeyError` instead of proper `JetStreamError`
4. **Fix direct message hardcoded prefix** (#7) — breaks custom domain configs
5. **Fix callback leaks** (#8) — memory leak in long-running applications
6. **Replace polling loops** (#9) — 100ms polling is wasteful; use event-driven signaling
7. **Fix `get_event_loop()` deprecation** (#11) — emits warnings on Python 3.12+

### What's Good

The overall code quality remains strong. The package is well-architected, well-tested, and the ADR-37 implementation (heartbeats, fetch semantics, message batching) is thorough. The datetime fixes are clean and well-tested. The #644 sentinel fix is a correct and clean solution. The main concerns are runtime correctness issues that should be addressed before users rely on the v0.1.0 release.
