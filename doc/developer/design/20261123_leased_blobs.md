# Design: Leased Blobs for Content that Skips Consensus

## 1. Summary

Persist GC finds the blobs to delete by reading shard state, so it never deletes a blob no state
names. The peek stash is
written through persist but intentionally never joins any shard's state, so its only cleanup is
the reader's own `Batch::delete`. If that delete doesn't run (the `persist_batch_delete_enabled`
flag is off, or a process dies), nothing else can remove it.

Another [design doc](20261123_blob_reaper.md) covers a generic reaper for leaked blobs, but for the stash that approach
is indirect and riskier.

We propose a separate namespace, `leased/<lease>/...`, whose lifetime is tied to a long-lived lease.
For the current use case, each envd (environmentd) that reads stashes holds a lease and keeps
renewing it. Leases are recorded in a persist shard, the lease shard, with a row per envd.

Once a lease stops being renewed, its holder halts as soon as it next runs. An envd records that
by marking the lease expired, and the blob reaper deletes its prefix.

## 2. Context

### The peek stash

When a peek result is too large to send inline, clusterd writes it as a persist batch under a
fresh shard id `s<peek_uuid>` using `PersistClient::batch_builder`, which never touches consensus.

Its `ProtoBatch` goes back to envd in a compute response. Envd rebuilds the batches, streams the
rows to the client, and calls `Batch::delete`.

Two observations:

- **Only the issuing envd reads a stash.** The `ProtoBatch` lives only in envd's memory and
  in compute protocol messages.
- **A read can last hours.** Envd streams at the client's pace (`mpsc::channel(1)`), so a
  portal read across many FETCHes, or a slow TCP client, can hold a stash open indefinitely.

### Where stash blobs leak

`Batch::delete` is the only thing that removes a stash, and many paths never reach it:

| Source | When |
|---|---|
| Multi-replica peek | controller keeps the first stashed response, drops the rest |
| Late response | after cancel, replica failure, or replay to a restarted replica |
| Cancel or error merge | `merge_peek_responses` drops a stash on `Canceled`/`Error` |
| Builder run merge | merged-from parts are never deleted, even on success |
| Process death | envd loses its in-memory batches, or clusterd dies mid-upload |

Each can be fixed at the source except process death, so some leak always remains.

### Why not leave it to a generic reaper

A generic reaper can delete leaked stash blobs, but only after the next upgrade. A stash key looks like a
shard key, so the reaper has to work out what a blob is from what's missing (no consensus entry,
an old writer version). Then it has to fall back on a policy that is also safe for a shard whose
creation crashed: wait for an upgrade, and trust that consensus is complete.

But the writer already knows what it's writing and how long it lives. If it records that intent in
where it writes, each kind of content gets its own lifecycle policy: a stash lives exactly as long
as its reader. The generic reaper stays as a backstop, including for leaks already in the bucket.

### More than one envd

Today an environment runs one writable envd. The
[zero-downtime upgrades, physical isolation, and high availability](20251219_zero_downtime_upgrades_physical_isolation_high_availability.md)
design moves us to several envds sharing the bucket and consensus.

Any of them can crash, be partitioned, or be reaped by the orchestrator independently. A design
based on envd version isn't enough, since several envds can run the same version.

## 3. Requirements

1. **Recognizable by key.** A reaper can tell these blobs apart by key alone.
2. **Deletion based on lifespan.** A blob is deleted only once no running process can read it.
3. **Correct with many envds.**
4. **No risk to shard data.** No change to shard keys, and the leased rules mark nothing outside the new namespace.
5. **Not peek-specific.** Any persist user that writes a batch, hands it to a reader, and never links it into consensus gets this too.

## 4. Non-goals

- **Existing `s<id>/` stash blobs and real-shard leaks.** The blob reaper handles both
  ([design](20261123_blob_reaper.md)).
- **Leaks under a live lease.** Reclaimed when the envd stops.
- **Deleting.** The blob reaper lists and deletes. This doc only says, safely, when a set of blobs may be deleted.

## 5. Proposal (high level)

The stash leaks because the only reference to it lives in memory. Recording each stash reference
durably costs too much, which is why peek stashes don't do it today. A lease tied to the lifespan
of the envd issuing the peek amortizes that cost.

1. **A new key root.** Blobs that by design never join a shard go under `leased/<lease>/<object>/<part>`
   (requirement 1).
2. **One lease per envd.** Each envd creates a lease at boot and renews it until it stops. It
   passes the lease id on every peek, and clusterd writes the stash under it.
3. **An expired lease means the holder is gone.** An envd that can't renew its lease halts.
   After an interval, any other process may mark the lease expired.
4. **The blob reaper deletes the prefix.** Everything under an expired lease is a candidate
   for deletion.

Leases and the key root live in persist as a generic mechanism.

## 6. Proposed Design

### 6.1 Key format

```
leased/<lease>/<object>/<part>
```

- `<lease>` is the lease uuid. Everything under it goes together, including the parts of hollow runs (runs whose part list is itself a blob) and builder run-merge leftovers, which today need special handling.
- `<object>` is one logical thing under the lease. For the stash, the peek uuid.
- `<part>` is `p<uuid>`.

### 6.2 The lease shard

**The rule: while envd's lease is alive, envd may read any blob under its prefix.** Once the
lease is dead, nothing may. Clusterd doesn't hold a lease. It writes under the lease envd
holds.[^writer-race]

[^writer-race]: This leaves a race. Envd holds a lease, clusterd starts writing under it, the
lease expires mid-write, and the reaper deletes the blobs. Nothing is lost, since the only
consumer (that envd) has halted. The writer's side is worse than a wasted upload: the batch
builder's run merge reads parts it already uploaded and treats a missing one as a panic
(`.expect("successful compaction")`, `persist-client/src/batch.rs:1003`), so the clusterd
aborts. The builder should return that error to its caller, so the stash upload fails instead.

The lease shard is a dedicated persist shard.[^lease-shard-id]

[^lease-shard-id]: Its id is a catalog setting, like `EXPRESSION_CACHE_SHARD_KEY`.

Each row is `lease -> (debug, fence_timeout, renewal, state)`:

- `debug` names the purpose and creating build, and holds the holder's wall-clock time of its last renewal.
- `fence_timeout` is the holder's `persist_lease_fence_timeout` (Section 6.3), fixed at create.
- `renewal` is a counter each renewal bumps.
- `state` moves one way, `live` then `expired`.

A change reads the current rows (a snapshot at the upper), then appends a retraction of the old row
and an insert of the new one with `compare_and_append` at that upper. If another write that
expired the lease landed first, the envd must halt.

### 6.3 Holding a lease

Three dyncfgs set the timing:

| Dyncfg | Default | Meaning |
|---|---|---|
| `persist_lease_renew_interval` | 15 min | how often a holder renews |
| `persist_lease_fence_timeout` | 30 min | a holder halts after this long without a successful renewal |
| `persist_lease_expiry_slack` | 5 min | an observer waits `fence_timeout` plus this before expiring |

The renew interval must be well under the fence timeout. A holder copies its fence timeout into
its row at create, so changing the dyncfg never lets an observer expire a lease sooner than its
holder fences.

- **Create.** At boot each envd creates a lease with a random uuid and holds it until it stops,
  never sharing or rotating it. A restart or new version gets a new lease.
- **Renew.** Every `persist_lease_renew_interval`, a conditional write bumps `renewal` if `state = live`, retrying
  with backoff if the lease shard is unreachable.
- **Halt on expiry.** If a renewal is rejected, or the holder reads its row and it isn't `live`,
  the holder halts, as a fenced deployment does, since its prefix may be deleted.
- **Self-fence.** A holder halts once `fence_timeout` has passed, by its own monotonic clock,
  since the *start* of its last successful renewal. Counting from when the renewal returned is
  unsafe: a slow acknowledgement would push the holder's deadline past the observer's expiry.

### 6.4 Expiry

On each renewal tick an envd also checks the other rows in the lease shard. It treats a `live`
lease as expired once it has seen `renewal` unchanged for the row's `fence_timeout` plus
`persist_lease_expiry_slack` (35 minutes by default) on its own monotonic clock,
so no wall clocks are compared across hosts.

The row only says that a renewal happened, not when, so the observer has to measure the change
time itself. Seeing a change late only delays expiry. The wall-clock time in `debug` is never used
for this. The observer then durably writes `state = expired`, conditional on `renewal` still
matching, so a racing renewal and expiry can't both succeed.

### 6.5 Deletion

The blob reaper gets a set of rules for `leased/`. The reaper never
judges liveness itself. It acts only on the `state = expired` an envd wrote to the lease shard in
Section 6.4. A key under `leased/<L>/` is a candidate if `L`'s row says `expired`.

An expired lease doesn't prove the holder has stopped: a paused holder can't run its self-fence
timer, so it may resume after the delete. On resume it either has its renewal rejected and
halts, or reads a deleted part and aborts.  Either way it stops without returning a wrong result.

## 7. Discussion

**Self-fencing couples envd to storage.** Every envd holds a lease on the same shard, so an outage of
consensus or blob longer than `fence_timeout` halts all of them at once. The alternative is to
stop using the stash instead of halting: fail in-flight stashed reads, refuse new stashed peeks,
and take a new lease once the shard is reachable.

But if envd can't reach storage for 30 minutes it is already broken, so halting seems acceptable.
