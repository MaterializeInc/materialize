# Design: Blob Reaper

## 1. Summary

Persist GC finds the blobs to delete by reading shard state, so it never deletes a blob no state
names. While we can close the gaps in GC so it doesn't leak there are some
leaks that we can't prevent. E.g., A writer uploads a batch, fails to link
it into consensus, and crashes before deleting it.[^examples] Since those blobs are not named, nothing
will ever delete them.

[^examples]: Compaction does this in production: a halted generation at 0dt cutover that
is in the middle of writing a batch will leak the blobs.

A *blob reaper* is a backstop that finds blobs nothing will ever use and deletes them.

To do so, we walk the bucket and apply **deletion rules** to each key, and delete
once the rule has held for a minimum time.[^policy]

[^policy]: What happens to a key the rules match is a policy choice. The reaper need not delete it
outright: it could move it aside (a quarantine prefix, or a cheaper storage class) and alert,
deleting only after a further wait or once someone has looked. An unexpected match then shows up
as an alert, not as lost data.

For now, we could design the reaper to run as an admin task (persist cli tool). In a future Materialize
it could be one of persist's own components or services that run in the background like a compactor
(or repairer).

## 2. Context

### What GC can't see

Three kinds of blob fall outside GC:

- **Never named.** A writer uploads parts, then its `compare_and_append` fails, or the process
  dies before calling it.
- **Named, then lost.** GC bugs that drop references without deleting the blobs.
- **Never meant to be named.** Some content is written through persist but by design never joins
  a shard's state. The peek stash is the one case today.

Note that Consensus has no delete, only `truncate`, so a dropped shard keeps a tombstone.
Therefore, every shard id in the bucket is one of:

- **A. In consensus.** A real shard, leaking blobs its state no longer, or never, named.
- **B. Not in consensus.** Peek stash (see corresponding [doc](20261123_leased_blobs.md)
about moving these to a new namespace) or shard creations that crashed before their first consensus write.

Note: `persistcli inspect blob-usage` already measures both cases (`persist-client/src/usage.rs:83-89`
calls them `leaked_bytes`, "eligible for reclamation by a (future) leaked blob cleanup task").


### When is a Blob Safe to Delete?

A blob is safe to delete once no running process can read it or link it into a shard's state.

However, just because a blob is not referenced doesn't mean that it can't be:
a writer could upload a batch, stall, then install it in consensus.
txn-wal holds data-shard batches in the txns shard until they are applied.

Thus, age of a blob is not evidence of its liveness.

However, the writer's version is. Every part key carries this information (`n<version>`).
A shard's state records the version of the code that last wrote it (`applier_version`),
and older code halts on reading it. Once a newer version has written a shard's
state, a writer at an older version can never commit to it again.[^applier-version].
A tombstoned shard accepts no commits at all.

[^applier-version]: "Written" here means upgraded. `applier_version` only moves in
`Machine::upgrade_version` so an ordinary write by a newer build
tags its diff with the state's recorded version, so older code can still commit.

## 3. Requirements

1. **Proof it is safe to delete.** Never delete a blob a running process could still read or link.
2. **Incremental.** Bounded memory and progress that survives restarts, as we can have many keys.
3. **Crash-safe and idempotent.** A crash at any point, or two copies running at once, can delay
   a delete but never cause an early one.
4. **Outside envd.** This should operate outside envd, needing only blob and consensus access, not any catalog state.
5. **Report before delete.** Support a mode that does not delete state but reports what it *would* delete.
6. **Rules for deletion are separate from the mechanism to delete.** We should be able to add new policies for reaping.


## 4. Non-goals

- **Replacing GC.** GC stays the primary path for blobs a shard's state names.

## 5. Proposal (high level)

The reaper walks every key in the bucket and asks of each: can any running process still read it
or link it into a shard? To answer, a set of rules reads the
shard's state in consensus, or the lease shard for keys under `leased/`.
A key no process can use is a *candidate* for deletion.

The reaper then notes when it first saw each candidate and deletes it only once it has stayed a
candidate for a minimum time. That information, and how far the walk has got, are kept
durably in a *journal*, so the reaper can stop and resume.

This splits the reaper into a few parts.

1. **Walker.** Splits the key space into fixed units and visits them in order.
2. **Rules.** Judge the blob as whether to keep or mark as a candidate for deletion.
3. **Journal.** A persist shard holding the cursor and when each candidate was first seen.
4. **Deleter.** Rate-limited physical deletion.


## 6. Proposed Design

### 6.1 Rules and machinery

The rules take one unit's listing (Section 6.2), grouped by shard (or by lease, under `leased/`), plus
blob and consensus access, and decide for each blob whether to keep it or make it a candidate.
Candidates are deleted once it has stayed a candidate for `--min-age`.

The rules themselves are defined in Section 6.4.

### 6.2 The walk

The walker splits `s` into units by shard id prefix[^split], and `leased/` the same way by lease
uuid, and visits them in order, wrapping after the last. One full pass over every unit is a *lap*.

[^split]: The first `--unit-hex-chars` hex characters of the shard id (default 3: 4096 units,
`s000` through `sfff`).

1. List every key under `u`.
2. Apply the rules to the unit.
3. For each candidate, look up when it was first seen.
    a. Not seen before: record it as first seen now.
    b. First seen at least `--min-age` ago: delete the blob.
4. Append the row changes and the cursor move to the journal.

Reading state and deleting aren't atomic. That's safe because once a blob is a candidate it stays
one: a writer that can't commit now never can, and an expired lease stays expired.

### 6.3 The journal

A persist shard whose id is derived from the organization id and a reserved seed, so any run
finds it with no stored state or catalog access.[^journal-id]

[^journal-id]: The same way the catalog finds its own shard: `shard_id(organization_id, seed)`
with `CATALOG_SEED = 1` and seeds 2-4 reserved. The journal takes seed 5,
and `shard_id` moves into persist so `persistcli`, which can't depend on
the catalog, can call it.

The journal holds two kinds of row:

| Row | Live count | Meaning |
|---|---|---|
| `Cursor { unit }` | exactly 1 | next unit to visit |
| `FirstSeen { group, at }` | one per group with candidates | when this group first became a candidate[^group] |

[^group]: Kept per group of candidates, not per blob, to keep the journal small: per shard and
writer version for parts, per shard for rollups and for shards not in consensus, and per lease. A
blob that joins a group later inherits the group's first-seen time.

The journal never stores verdicts, so every delete rests on state read in that visit. A stale or
lost journal can delay a delete but never justify one.[^wall-clock]

[^wall-clock]: `at` is wall-clock time, since runs on different hosts must agree on it.
Clock skew shifts a delete by minutes, but doesn't make it unsafe.

Each visit commits with one `compare_and_append`, so two reapers running at once can't both
commit a visit; the loser exits. Concurrent reaping is safe because each reaper deletes only what
its own fresh verdicts prove, given the same inputs.

Crash safety:

- **Missing row** (journal reset, or a candidate forgotten and seen again): its wait
  restarts. A delete comes later.
- **Crash between deletes and the append:** the next visit tries the delete again,
and deleting a missing key succeeds.
- **Crash mid-unit:** the cursor didn't move, so the unit is redone.

### 6.4 Rules

#### 6.4.1 Shards, case A: shard in consensus

**Parts.** A part `s<X>/n<v>/p<id>` is a candidate if:

1. **Unreachable:** no live state of `X` names it. A shard's state is a rollup plus the diffs after
   it, so this means every seqno from the earliest live diff to head,
   with hollow runs (runs whose part list is itself a blob) expanded recursively.
2. **Not pending in txn-wal:** if `X` is a txn-wal data shard, no unapplied txn in the txns shard
   names it.[^read-order]
3. **Its writer can't link it:** `X` is tombstoned, or `v` is older than `X`'s
   `applier_version`.[^rule-3-scope]

[^read-order]: Read the txns shard before `X`'s state. Otherwise a txn applied between the two
reads is in neither, and its parts look like candidates.

[^rule-3-scope]: For a txn-wal data shard, use the older of its and the txns shard's
`applier_version`. Leaks at a live shard's current version wait for its next upgrade.

`w<uuid>` keys are kept. If `X`'s state names a hollow run that is missing from the blob, `X`'s
unreferenced keys are kept, since its reachable set is incomplete.

**Why it's safe.** A process reads a part only through a state whose seqno it holds, and GC never
truncates past a held seqno, so rule 1 covers every read. Linking is a commit, which reads current
state, and older code halts on it (rule 3). A newer process could link the part only if handed the
pointer, through txn-wal (rule 2). `--min-age` is a backstop against a missed kind of reference.

**Rollups.** A rollup `s<X>/v<seqno>/r<id>` is a candidate if no live state of `X` names it and
its seqno is below the oldest rollup in `X`'s current state, or `X` already records a different
rollup at that seqno.[^add-rollup]

[^add-rollup]: `add_rollup` refuses exactly those (`internal/state.rs:1433-1463`), and the oldest
rollup only moves up, so once a rollup can't be added it never can, tombstoned or not.

#### 6.4.2 Shards, case B: shard not in consensus

Every key under `s<X>/` is a candidate if every writer is gone: each part's `v`, and each rollup's
writer version, is older than the oldest build still running. Rollup keys have no version, so the
rules fetch it from the file.

If `X` appears in consensus before its wait passes, it is judged under case A instead, and its
wait starts over.

**Why it's safe.** No state names these blobs, so the only pointers are in the memory of the
processes that wrote them: the stash's clusterd and envd, or the shard's creator. A writer build
older than the oldest running one means an upgrade has finished and those processes are gone.[^complete-consensus]

[^complete-consensus]: This assumes consensus is complete. A wrong or restored-from-backup
consensus would make real shards look absent, but they would not be discoverable.

#### 6.4.3 Leased

The [leased blobs design](20261123_leased_blobs.md) decides when a lease's prefix may go; these rules apply it. The rules read the
lease shard: a key under an `expired` lease is a candidate, and a key under a
lease with no row is kept. They never write the lease shard.

### 6.5 Interface: `persistcli admin reap-orphans`

The reaper runs as a `persistcli` admin command, by hand or as a k8s Job. Like the other admin
commands, it changes nothing without `--commit`.

| Flag | Purpose |
|---|---|
| `--blob-url`, `--consensus-url` | the environment's storage |
| `--organization-id` | derives the journal shard id |
| `--txns-shard` | for case A rule 2 |
| `--oldest-live-version` | the floor for case B |
| `--lease-shard` | for the `leased/` rules |
| `--commit` | delete. Without it: journal and report only. |
| `--shard` (repeatable) | only these shards[^shard] |
| `--laps`, `--min-age`, `--delete-rate`, `--unit-hex-chars` | tuning |

A rule whose input is missing marks nothing as a candidate.

[^shard]: The cursor doesn't move, but first-seen times are shared with full laps. Useful for
cleaning a known shard or testing a rule.

### 6.6 Later: a reaper service

In a Materialize where persist's background work (compaction, GC,
reaping) is split out of envd and clusterd into its own components, the reaper is one of them: the
same module walking forever. It already needs only blob, consensus and three inputs, the txns shard
id, the lease shard id and the oldest running build, so it can move there unchanged.
