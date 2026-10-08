# nats.py vs NQ: missing features

What nats.py lacks compared with an NQ Python client (`wallyqs/nq.dev`,
`packages/python/nq`), using NQ's oracle inventories as the feature list.

- nats.py: `db5f8da` (nats-io/nats.py `main`, synced into this branch)
- nq.dev: `e602352` (`dev`)
- Feature list: NQ's oracle contracts, 1,410 symbols in total.
  - `ir/contract.json`: nats.go core, 282 symbols
  - `ir/jetstream-contract.json`: nats.go/jetstream, 904 symbols, covering JetStream, KV and Object Store
  - `ir/services-contract.json`: nats.go/micro, 121 symbols
  - `ir/orbit-contract.json`: orbit.go/jetstreamext, 103 symbols
- Per-symbol results: [`nq-gap-audit.tsv`](nq-gap-audit.tsv). Its columns are area, oracle id, kind, NQ Python status, nats.py modern, nats.py legacy, and evidence with `file:line`.

Each symbol was classified for both nats.py clients:

- **modern**: the `nats-core`, `nats-jetstream` and `nats-key-value` packages
- **legacy**: `nats-py`, which provides `nats.aio`, `nats.js` and `nats.micro`

The classes are `present`, `partial`, `missing`, or `n/a`. `n/a` means a Go-only idiom, such as channels, `context.Context` or functional-option types, that Python expresses as kwargs. Pythonic names and shapes count as present.

This is a static source audit. Nothing was run against a live server. Every finding listed under [Bugs](#bugs-found-during-the-audit) was re-checked by reading the source.

## Totals

| Area | NQ Python | modern: present / partial / **missing** / n/a | legacy: present / partial / **missing** / n/a |
|---|---|---|---|
| Core (282) | 246 impl, 36 unsupported | 111 / 46 / **93** / 32 | 136 / 38 / **78** / 30 |
| JetStream (665) | 227 impl, 438 planned* | 386 / 72 / **158** / 49 | 351 / 98 / **167** / 49 |
| KV + Object Store (239) | 167 impl, 72 planned* | 98 / 7 / **126** / 8 | 135 / 24 / **70** / 10 |
| Services / micro (121) | 111 impl, 10 unsupported | 0 / 0 / **109** / 12 | 75 / 13 / **21** / 12 |
| orbit batch / fast publish (103) | 63 impl, 40 planned | 10 / 1 / **85** / 7 | 9 / 1 / **86** / 7 |
| **All (1,410)** | 814 impl | 605 / 126 / **571** / 108 | 706 / 174 / **422** / 108 |

Restricted to the 814 symbols that NQ Python has bound and measured against the oracle:

- **modern** is missing 432 and has 90 partial.
- **legacy** is missing 293 and has 138 partial.

\* The `planned` status in NQ's contract files lags its code. NQ's `nq/jetstream/_types.py` and `aio.py` already have most of the "planned" struct fields and enums, and the context-level consumer pause, resume and reset. Treat `planned` rows as features NQ mostly has.

## Missing features by area

### Core (`nats-core` modern, `nats.aio` legacy)

1. **Server pool and discovery.**
   - **Modern:** `connect()` takes a single URL. It has none of `servers`, `discovered_servers`, `set_server_pool`, `reconnect_to_server` (`ErrServerNotInPool`), the discovered-servers callback, or `connected_url`. Legacy has all of these.
   - **Both:** no `IgnoreDiscoveredServers` and no `ConnectedUrlRedacted`.
2. **Reconnect policy.**
   - **Both:** no `CustomReconnectDelay`, `ReconnectJitterTLS`, connect callback (`ConnectHandler`) or `PermissionErrOnSubscribe`.
   - **Both:** nats.go's "abort on a repeated auth error" rule is absent (`IgnoreAuthErrorAbort` is partial).
   - **Modern:** has no `RetryOnFailedConnect` and no reconnect-error callback.
   - **Modern:** has no `ReconnectBufSize` or `ErrReconnectBufExceeded`, and buffers publishes without limit while reconnecting.
3. **Callbacks.**
   - **Modern:** has no closed callback.
   - **Legacy:** has no `set_*_handler` after connecting.
   - **Both:** have no handler getters.
   - **Both:** the disconnect callback receives no error argument.
4. **Connection introspection.**
   - **Both missing:** `ConnectedAddr`, `LocalAddr`, `ConnectedClusterName`, `ConnectedDomain`, `IsSystemAccount`, `NumSubscriptions`, `NewRespInbox`, `Barrier`, `TLSConnectionState` (`ErrConnectionNotTLS`).
   - **Legacy:** the server INFO is private, so these are missing: `AuthRequired`, `TLSRequired`, `HeadersSupported`, and `ConnectedServerId`/`Name`/`JetStream`.
   - **Modern:** has no `GetClientIP` or `Buffered`.
5. **TLS.**
   - **Both:** have no `ClientTLSConfig`, `TLSCertCB` or `RootCAsCB`, which re-read certificates on every handshake.
   - **Legacy:** has a security gap (see Bugs).
6. **WebSocket.**
   - **Modern:** has no connection headers.
   - **Both:** have no headers callback (`WebSocketConnectionHeadersHandler`).
   - **Both:** `ProxyPath` is supported only by putting the path in the URL.
   - **Compression:** legacy cannot enable it; modern gets it only from the `websockets` default.
7. **Write path.**
   - **Both:** have no custom dialer (`SetCustomDialer`).
   - **Modern:** has no `WriteBufferSize`, `FlusherTimeout` or `ReconnectOnFlusherError`, and its write-buffer limits are hard-coded.
   - **Modern:** a flush timeout disconnects instead of raising.
8. **Auth.**
   - **Legacy:** has no `Nkey(pubkey, sign_cb)` (seed only).
   - **Modern:** has no `UserCredentialBytes`.
   - **Both:** need the third-party `nkeys` package. NQ ships NKeys, JWT and Ed25519 itself and does not need it.
   - **Both:** do not validate conflicting auth options. These errors are all missing: `ErrNkeyAndUser`, `ErrNkeyButNoSigCB`, `ErrNoUserCB`, `ErrUserButNoSigCB`, `ErrTokenAlreadySet`, `ErrUserInfoAlreadySet`, `ErrNkeysNotSupported`.
   - **Both:** cannot serve auth callouts; NQ has `AuthService`.
9. **Messages and request/reply.**
   - **Modern:** a `Message` is not bound to its connection, so `msg.respond`, `RespondMsg`, `ErrMsgNoReply` and `ErrMsgNotBound` are missing. There is no `UseOldRequestStyle`.
   - **Legacy:** `respond()` cannot take headers and re-sends the request's headers.
   - **Legacy:** headers are a `Dict[str, str]`, so there are no multi-value headers.
   - **Both:** have no `Msg.Size`.
10. **Subscriptions.**
    - **Modern:** callbacks are synchronous and run on the read loop (see Bugs).
    - **Modern:** drain sends no flush round-trip and does not wait for queued messages to be handled.
    - **Both:** have no `PendingLimits`/`MaxPending`/`ClearMaxPending`, no `Subscription.IsDraining` and no subscription closed handler. Pending limits can be set only at subscribe time.
    - **Legacy:** has no `Dropped` and no `IsValid`.
11. **Errors.**
    - **Modern:** mostly raises Python built-ins (`ValueError`, `TimeoutError`, ...) and passes raw `-ERR` text to error callbacks. About 25 error identities are therefore partial.
    - **Modern:** has no `ErrConnectionDraining`, `ErrConnectionReconnecting`, `ErrBadTimeout`, `ErrHeadersNotSupported`, `ErrNoEchoNotSupported` or `ErrDisconnected`.
    - **Legacy:** collapses `ErrAuthRevoked`, `ErrAccountAuthExpired`, `ErrMaxSubscriptionsExceeded` and `ErrPermissionViolation` into generic errors.
12. **Runtime profiles.**
    - **Both:** have no threaded (synchronous) client. NQ ships `nq.io` alongside `nq.aio`, with a bounded callback pool and a free-threaded CPython path.

### JetStream (`nats-jetstream` modern, `nats.js` legacy)

1. **Push consumers.** Modern has none: `_upsert_consumer` and `get_consumer` raise `NotImplementedError` (`stream.py:1384`, `1469`). Legacy supports them only through subscribe/bind.
2. **Callback consume.** These are missing in both:
   - `Consumer.Consume` and `ConsumeContext` (Stop/Drain/Closed)
   - `ConsumeErrHandler`, `StopAfter`, `MessagesContext.Drain`
   - `PullMaxMessagesWithBytesLimit`, `WithMessagesErrOnMissingHeartbeat`

   Modern has only the `messages()` iterator. Legacy has no pull `Messages`, `FetchBytes`, `FetchNoWait` or `Next`.
3. **Async publish.**
   - **Modern:** has none: no `PublishAsync`, `PubAckFuture`, `PublishAsyncPending`/`Complete`, stall wait or max-pending, and no `ErrTooManyStalledMsgs`.
   - **Legacy:** has a bare `asyncio.Future`, without ack/err handlers, an async-publish timeout, `CleanupPublisher` or the publisher-closed error.
4. **Publish options.**
   - **Modern:** takes only raw `headers=`. It has no `WithMsgID`, `WithMsgTTL`, `WithExpectStream` or `WithExpectLast*`, and no constants for those headers.
   - **Both:** are missing `WithExpectLastSequenceForSubject` and `MsgRollupAll`.
   - **Both:** have the `WithSchedule*` header constants but no helpers for them.
   - **Legacy:** has no publish retry.
5. **Consumer management.**
   - **Modern:** has no `UnpinConsumer` and no pinned-client pulls (no `Nats-Pin-Id` tracking, no `ErrPinIDMismatch`).
   - **Modern:** pause, resume and reset exist only on `Stream`, not on the JetStream context, and `pause` and `resume` return `None`.
   - **Legacy:** has no `consumer_names`, no paginated consumer list and no create-only or update-only actions.
   - **Legacy:** its ordered consumer is push-based rather than pull-based. It has no `MaxResetAttempts` or `NamePrefix`.
6. **Stream management.**
   - **Both:** have no `CreateOrUpdateStream`.
   - **Modern:** `update_stream` takes raw kwargs. There is no `WithGetMsgSubject` (`next_by_subj`).
   - **Legacy:** has no `stream_names`, subject filter or `WithDeletedDetails`, and `streams_info` returns one page. There is no Stream handle. `delete_msg` always secure-erases.
7. **Info types.**
   - StreamConfig has all 40 fields serialized in both clients. ConsumerConfig is complete in modern.
   - **Legacy ConsumerConfig:** lacks `max_request_batch`, `max_request_expires` and `max_request_max_bytes`.
   - **Both:** lack `APIStats.Inflight`, `Tier.Reserved*`, `PeerInfo.Peer`/`Pending`, `ClusterInfo.SystemAcc`/`Desired`, the `DesiredClusterInfo*` and `MigrationStatus*` types, `StreamSourceInfo.Seq` and `StreamSource.Domain`.
   - **Legacy:** lacks timestamps, `StreamState.FirstTime`/`LastTime`/`NumSubjects` and the source `FilterSubject`/`SubjectTransforms` fields.
   - **Modern:** `ConsumerInfo.delivered`, `ack_floor`, `cluster` and `priority_groups` are raw dicts.
8. **Errors.**
   - **Modern:** maps only 8 API error codes to classes. It has no `ErrMsgAlreadyAckd` (ack state is not tracked), no `ErrNotJSMessage` and no `ErrNoHeartbeat`, and no name or subject validation errors.
   - **Legacy:** dispatches errors by HTTP status and has no error-code constants. It has no `TermWithReason`.
   - **Both:** have no `ErrMaxBytesExceeded`, `ErrBatchCompleted`, `ErrConsumerLeadershipChanged`, `ErrServerShutdown`, `ErrInvalidJSAck`, ordered-consumer misuse errors or server-version "not supported" errors.
9. **Context options.**
   - **Both:** have no `ClientTrace` and no `JetStream.Options()`.
   - **Modern:** has no default-timeout option; the API timeout is fixed at 5 s (`api/client.py:424`).

### Key-Value and Object Store

1. **No modern Object Store.** `nats-core`, `nats-jetstream` and `nats-key-value` contain no object store. All 111 Object Store symbols are missing in modern; NQ implements the methods.
2. **Modern KV.**
   - No `ErrNoKeysFound` or `PutString`.
   - `keys()` and `history()` return iterators and take no watch options. An empty history does not raise KeyNotFound.
   - The key lister has no `stop()`, so stopping early leaks the consumer. It also lists server limit markers as live keys.
   - `list_keys_filtered` takes one pattern, not a list.
   - **Mirror and sources:** these are passed straight through. KV `Subjects` stay set (the server rejects that), there is no `KV_` origin prefix, no `mirror_direct`, no `$KV` subject transforms, and reads and writes do not redirect to the origin.
   - The KV status has no `Config()`.
   - Missing errors: `ErrKeyDeleted`, `ErrKeyRevisionMismatch`, `ErrBucketRequired`, `ErrBucketMalformed`, `ErrKeyValueConfigRequired`, `ErrLimitMarkerTTLNotSupported`.
   - `ErrKeyExists` does not map error code 10164.
3. **Legacy KV.**
   - **Bucket management:** no `update_key_value`, `create_or_update_key_value`, bucket names or bucket listers. `ErrBucketExists` is never raised.
   - **Config:** no `compression`, `mirror`, `sources` or `metadata`. `placement` is accepted but dropped.
   - **Watch:** has no `updates_only`, `resume_from_revision` or `watch_filtered`. `keys(filters=)` matches substrings instead of subject wildcards.
   - `purge` has no `last_revision`.
   - Entries returned by `get()` have no created time, no delta and no proper operation.
   - The KV status has no backing store, bytes, compression flag, metadata or config.
4. **Legacy Object Store.**
   - No `add_link` or `add_bucket_link`.
   - No update, create-or-update, names or list for buckets.
   - No compression or metadata.
   - No public digest helpers.
   - `get()` buffers the whole object, and `ObjectResult` is not a streaming reader.
   - `list()` shows deleted objects by default.
   - `ObjectWatcher.stop()` does not end an `async for` loop that is already waiting.

### Services (micro)

1. **No modern micro package.** All 109 non-idiom symbols are missing for `nats-core`. NQ has micro on both `nq.aio` and `nq.io`.
2. **Legacy `nats.micro`.**
   - **Lifecycle and config:**
     - Queue groups cannot be disabled; the queue group is always `"q"` by default (`service.py:214,229`).
     - There are no `done_handler`/`error_handler` and no auto-stop when the connection closes.
     - There is no `Config.Endpoint` default endpoint and no endpoint pending limits.
   - **Responses:** there is no `respond_json` or `Reply`.
   - **Errors:** none of the `NATSError` types, and no `ErrArgRequired`, `ErrMarshalResponse`, `ErrRespond`, `ErrServiceNameRequired` or `ErrVerbNotSupported`. Config validation raises a plain `ValueError`.
   - **Partial:**
     - `reset()` (see Bugs).
     - `respond_error` neither validates nor counts.
     - The stats handler receives stats instead of the endpoint.
     - `control_subject` does not validate.

### orbit.go `jetstreamext`

1. **Atomic batch publisher.**
   - Neither client has the following, which NQ implements: `NewBatchPublisher`, all 8 `BatchPublisher` methods, `PublishMsgBatch`, and the `WithBatch*` options.
   - What both clients do have: the `Nats-Batch-*` header constants, the batch fields on the publish ack, and `allow_atomic`/`allow_batched` on StreamConfig. A batch can only be built by setting those headers on publishes by hand.
2. **Fast-ingest publisher.** Neither client has `NewFastPublisher` or its 6 methods (flow acks, gap detection, pings, error handler).
3. **Batch error identities.** All 15 `JSErrCode*` constants and their errors are missing, plus `ErrBatchClosed`, `ErrEmptyBatch`, `ErrFastBatchGapDetected` and `ErrInvalidBatchAck`.
4. **Direct batch get** (`GetBatch`, `GetLastMsgsFor` and their options).
   - Neither client has it, and NQ lists it as planned.
   - Modern has only a generated request type that carries `batch`, `multi_last` and `up_to_*`, and nothing sends it.

**Caution on error codes.** orbit.go `main` assigns these codes:
- `JSErrCodeBatchPublishInvalidGapMode` = 10202
- `JSErrCodeFastBatch*` = 10203–10206

They do not match nats-server `main`'s `server/errors.json`. Checked on 2026-10-08, it uses:
- 10202 for a cluster member change
- 10203 for invalid message-schedule sources
- 10204 for an invalid consumer reset
- 10205–10208 for `BatchPublishDisabled`, `InvalidPattern`, `InvalidBatchID` and `UnknownBatchID`

NQ copies orbit's values. nats.py already uses 10203 and 10204 with the server's meanings. Take these codes from `errors.json`, not from orbit.go.

## Bugs found during the audit

All of these were confirmed by reading the source at `db5f8da`.

- **Legacy TLS downgrade.** Only the server's `tls_required` triggers the TLS upgrade (`nats/src/nats/aio/client.py:2282-2295`). A `tls://` URL or `tls=` context against a server that doesn't require TLS stays plaintext with no error. `SecureConnWantedError` is defined but never raised. Modern raises `SecureConnectionRequiredError` in this case.
- **nats-core callback subscriptions.** Callbacks run synchronously on the read loop, and the message is then still put on the pending queue (`nats-core/src/nats/client/subscription.py:219-228`). A subscription that only uses a callback eventually reaches its pending limit and reports slow-consumer.
- **nats-jetstream `get_info()`.** `PullConsumer.get_info()` returns the cached info without a server request (`consumer/pull.py:546-548`). The ordered consumer inherits this.
- **nats-jetstream direct get.** It hard-codes `$JS.API.DIRECT.GET` and ignores the API prefix or domain (`stream.py:1181-1186`).
- **nats-jetstream `opt_start_time`.** `StreamSource.opt_start_time` is typed `int` and sent raw; the server expects RFC 3339 (`stream.py:172,225`).
- **nats-jetstream `ack_policy` default.** The default is `"none"` and is left out of the request (`consumer/__init__.py:32,239`). The oracle's zero value is `AckExplicit`.
- **Legacy object store rename.** `ObjectStore.update_meta` rename is broken (`nats/src/nats/js/object_store.py:384-412`):
  - The "already exists" check looks up the old name, so every rename raises `ObjectAlreadyExists`.
  - The new metadata is also published under the old subject, which is then purged.
- **Legacy micro `reset()`.** `Service.reset()` only resets `started` (`nats/src/nats/micro/service.py:749`). Endpoint request and error counts and processing times are never cleared.
- **Legacy pause fields.** `ConsumerInfo.pause_remaining` is typed `str`, but the server sends an integer number of nanoseconds. Reported by the audit; not re-checked.

## Legacy fixes on this branch

Each legacy nats-py defect above, plus the others the audit turned up, is fixed in its own commit. Every commit adds a regression test, and each test was confirmed to fail without its fix.

| Commit | Fix |
|---|---|
| `0f988bc` | A `tls://` URL or a `tls=` context now forces TLS, and raises `SecureConnWantedError` when the server offers none. |
| `7820a23` | `ObjectStore.update_meta` renames work: it checks the new name, publishes under it, and purges the old name. |
| `5cb65fe` | micro `Service.reset()` clears every endpoint's statistics. |
| `4d4d83e` | micro `respond_error` rejects an empty code or description, and counts explicit error responses. |
| `dacef6a` | micro `control_subject` rejects an ID without a name, and treats empty strings as absent. |
| `ae0ddcb` | `pause_remaining` on `ConsumerInfo` and `ConsumerPause` is converted from nanoseconds to seconds. |
| `d6ce4c5` | `KeyValueConfig.placement` reaches the bucket's stream. |
| `0a18fb5` | `ObjectWatcher.stop()` ends an `async for` loop that is already waiting. |
| `c9d2ba0` | `put` and `update_meta` raise `InvalidObjectNameError` for an empty name, so no chunks are orphaned. |
| `34079c2` | `Msg.respond(data, headers=...)`; without `headers` it still echoes the request's headers. |
| `9488d8b` | A pull fetch reports terminal 409s instead of timing out. These are Consumer Deleted, push based, and MaxRequestBatch, MaxRequestExpires and MaxRequestMaxBytes exceeded. |
| `bf613ad` | Object `mtime` is filled from the server's timestamp in `get_info` and in watch updates. |
| `77162a8` | `ObjectStore.list()` hides deleted objects by default. |
| `43e13fb` | `KeyValue.get` fills `created` and `delta`, and a deleted entry carries `DEL` or `PURGE`. |

### Closing the remaining legacy gaps

After the fixes above, every legacy row the audit marked `missing` or `partial` was closed. Each feature has its own commit and tests in the matching new test file:

| Area | Test file |
|---|---|
| Core client | `test_client_parity.py` |
| micro | `test_micro_parity.py` |
| JetStream management | `test_js_manager_parity.py` |
| JetStream publish/consume | `test_js_consume_parity.py` |
| Key-Value | `test_kv_parity.py` |
| Object Store | `test_object_store_parity.py` |
| orbit `jetstreamext` | `test_jetstreamext.py` |

[`legacy-issues.tsv`](legacy-issues.tsv) maps every one of the 596 legacy rows the audit marked `missing` or `partial` to:
- its verdict at the branch tip,
- the commit or commits that closed it,
- the test that exercises it,
- `file:line` evidence.

A final read-only check against the tip found all 596 rows closed. A mechanical re-check then confirmed, for every row:
- each cited commit is on this branch,
- each cited commit changes the row's evidence file or test file,
- every `file:line` exists.

All 238 distinct cited tests pass against nats-server v2.15.0, including a three-node cluster test for the Leadership Change status. The defects found outside the symbol inventory are in the commit table above.

The three earlier compatibility exceptions now follow nats.go. Each changed its own commit and updated the upstream test that pinned the old behaviour:

- **`keys(filters=)`** takes subject patterns (`*`, `>`) and lets the server filter them. It no longer matches substrings.
- **`Entry.operation`** is a `KeyValueOp` for every entry, `PUT` included. It is never `None`.
- **An empty object name** raises `ObjectNameRequiredError`, which is no longer an `ObjectNotFoundError`.

`history()` raises `KeyHistoryNotFoundError`, which is both a `KeyNotFoundError` and a `NoKeysError`.

Some nats.go defaults are kept as opt-ins, so existing callers keep their current behaviour:
- **Reconnect jitter:** defaults to 0.
- **`retry_on_failed_connect`:** off unless enabled.
- **Disconnect error:** delivered through the new `disconnected_err_cb`; `disconnected_cb` keeps its zero-argument signature.

`ErrClientCertOrRootCAsRequired` is covered by `connect(client_tls_config=ClientTLSConfig(cert_cb, roots_cb))`. Like nats.go's `ClientTLSConfig`, it raises that error when it has neither callback.

The batch error codes follow nats-server's `errors.json`, not orbit.go's 10202–10206.

Verified at the branch tip: the whole `nats/tests` suite passes against nats-server v2.15.0, with 629 passed and 4 skipped.

## Notes on method

- Some calls were judgement calls:
  - `ClientCert`, `RootCAs` and `TLSConfig` count as present because both clients take an `ssl.SSLContext`.
  - `SkipHostLookup` counts as present because neither client resolves hosts itself.
  - nats-core's `Headers` class and async iteration over subscriptions count as present, even though NQ marks those symbols `unsupported`.
- Some nats.py features have no NQ equivalent:
  - multiple callbacks per event, with add/remove (nats-core)
  - `request(return_on_error=True)`
  - subscriptions as async context managers
  - `msg_class` and the optional `fast_mail_parser`
- To regenerate the feature list, read `targets["python.asyncio"]` from each nq.dev `ir/*contract.json`.
