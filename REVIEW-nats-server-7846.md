# Review: nats-io/nats-server#7846

**Title:** [IMPROVED] Eventually force snapshots if blocked due to catchups
**Author:** MauriceVanVeen
**Base:** main <- maurice/force-snapshot
**Scope:** 12 files, +153 / -75

## Overview

Adds a mechanism to track failed snapshot attempts in the stream and consumer monitor
goroutines. When snapshots repeatedly fail, the timer interval is reduced from 2 minutes
to 15 seconds, and after enough failures (>4), the Raft-layer catchup check is bypassed
via a new `force` parameter on `InstallSnapshot`. Also threads `force` through to
`createSnapshotCheckpointLocked`, and the `runCatchup` path now always forces its snapshot.

## Architecture

The two-level forcing design is solid:

1. **Application level** (`doSnapshot`): Suppresses event-driven snapshot attempts when
   prior failures exist (`!force && failedSnapshots > 0`), funneling retries through the
   timer path only.
2. **Raft level** (`InstallSnapshot`): After 5+ failures, passes `force=true` to
   `createSnapshotCheckpointLocked`, which bypasses the `len(n.progress) > 0` catchup
   check.

The timer speed-up on first failure (15s) and reset on success avoids burning cycles on
retries while ensuring the system responds reasonably quickly.

## Potential Issue: `errCatchupsRunning` excluded from failure counter

In `jetstream_cluster.go`, both the stream and consumer monitor `doSnapshot` closures:

```go
} else if err != errNoSnapAvailable && err != errNodeClosed && err != errCatchupsRunning {
    s.RateLimitWarnf(...)
    if failedSnapshots == 0 {
        t.Reset(compactMinInterval)
    }
    failedSnapshots++
}
```

`errCatchupsRunning` is explicitly excluded from incrementing `failedSnapshots`. This
means that if catchups are the sole reason snapshots are blocked, the counter never
increments, the timer is never reduced, and `forceSnapshot` (which requires
`failedSnapshots > 4`) is never set to `true`.

The code comment says:

```go
// If we had a significant number of failed snapshots, start relaxing Raft-layer checks
// to force it through. We might have been catching up a peer for a long period, and this
// protects our log size from growing indefinitely.
```

This describes exactly the catchup-blocked scenario, but the implementation will not
trigger for it. The `runCatchup` path does pass `force=true`, but that only fires when
the catchup code itself needs to re-snapshot after a store reset -- it does not address
the monitor goroutine being blocked by a long-running catchup.

If the exclusion is intentional (e.g., to avoid interfering with active catchups making
progress), the comment should be adjusted to reflect actual scope. If it is an oversight,
`errCatchupsRunning` should either be included in the counter or tracked separately with
a higher threshold before forcing.

## Minor Observations

- **Consistent patterns**: Stream and consumer monitor changes mirror each other well.
  The consumer already had `force` on `doSnapshot`; the stream now matches.
- **Test coverage**: `TestNRGInstallSnapshotForce` validates the Raft-layer force
  mechanism. No integration-level test covers the monitor goroutine's failure counting,
  timer adjustment, or the progression from `failedSnapshots=0` to eventual Raft-level
  forcing. An integration test exercising that full path would add confidence.
- **`runCatchup` always forces**: Passing `true` from `runCatchup` is correct -- the
  catchup process knows it is safe to snapshot at that point.
- **All external/test callers pass `false`**: Reviewed all ~40 updated call sites; they
  correctly pass `false`.

## Summary

The design is clean and the implementation is well-structured. The main question is
whether the `errCatchupsRunning` exclusion from the failure counter is intended or an
oversight, since it prevents the force mechanism from activating in the exact scenario
the comments describe.
