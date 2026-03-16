# Persist Shared Log: Protocol Specification

This document specifies the protocol precisely enough to derive a formal
model (e.g. Stateright) or a complete implementation.

## Data Model

### Proposals

A _proposal_ is a CAS operation submitted by a client. Proposals are
appended to the log by the acceptor and evaluated later by learners.

Each proposal is stored in the log shard as a persist update where the key
carries the serialized proposal and the value is unused:

```
Log shard schema:
  Key (K):   bytes    -- serialized ProtoLogProposal (opaque to persist)
  Value (V): ()       -- unused
  Time (T):  u64      -- batch number
  Diff (D):  i64      -- always +1
```

Each key is a serialized `ProtoLogProposal` containing either a CAS or
truncate operation. The proposal is opaque to persist and to the acceptor;
the learner deserializes it during evaluation. A CAS proposal contains
`(shard_id, expected, new_seqno, data)`. A truncate proposal contains
`(shard_id, seqno)`.

### Batches

A _batch_ is a set of proposals appended atomically to the log shard at a
single timestamp. Each batch has a `batch_number` (the persist timestamp)
and contains zero or more proposals ordered by position (0-indexed).

### Receipts

After a proposal is durably appended, the acceptor returns a _receipt_:

```
Receipt:
  batch_number: u64         -- the log timestamp this proposal was written at
  position:     u32         -- 0-indexed position within the batch
```

The receipt uniquely identifies a proposal in the log. Clients use it to
retrieve results from a learner.

### Client Shard State

Each learner maintains a materialized view of all client shards:

```
StateMachine:
  shards: Map<String, ShardState>

ShardState:
  entries: Vec<VersionedEntry>

VersionedEntry:
  seqno: u64
  data:  bytes
```

This state is derived deterministically from the log. All learners
processing the same log prefix arrive at identical state.

### Result Cache

Each learner maintains a bounded cache of proposal outcomes:

```
ResultCache:
  results: Map<batch_number, Vec<ProposalResult>>

ProposalResult = CasResult { committed: bool }
               | TruncateResult { deleted: u64 | error }
```

Results are retained for a configurable number of batches (default 10,000)
and pruned as the log advances.

## Write Path

### Step 1: Client submits proposal

The client serializes a proposal and sends it to the acceptor via the
`Append` RPC.

### Step 2: Acceptor buffers proposal

The acceptor adds the proposal to its pending buffer. The client blocks,
waiting for the flush.

### Step 3: Acceptor flushes batch

When flushed, the acceptor:

1. Takes all pending proposals from the buffer.
2. Reads the current `upper` from its `WriteHandle`; this is the next
   batch number.
3. Creates persist updates: one `((serialized_proposal, ()), batch_number, +1)`
   per proposal.
4. Calls `compare_and_append(updates, expected_upper, new_upper)` where
   `new_upper = batch_number + 1`.

If `compare_and_append` succeeds, each proposal receives its receipt
`(batch_number, position)`.

If `compare_and_append` returns `UpperMismatch`, another writer advanced
the upper. The acceptor reads the new upper and retries with the updated
batch number.

### Step 4: Learner evaluates batch

The learner's subscription receives the batch as a `ListenEvent::Updates`.
For each proposal in batch order:

**CAS evaluation:**
```
apply_cas(key=(shard_id, expected, seqno), value=data):
  let shard = state.shards.entry(shard_id)
  let current_seqno = shard.entries.last().map(|e| e.seqno)

  if current_seqno == expected:
    shard.entries.push(VersionedEntry { seqno, data })
    return CasResult { committed: true }
  else:
    return CasResult { committed: false }
```

Results are stored in the result cache at `(batch_number, position)`.

### Step 5: Client retrieves result

The client calls `AwaitCasResult(batch_number, position)` on a learner.
The learner blocks until it has evaluated the given batch, then returns the
cached result.

## Read Path

Reads are served from the learner's materialized `StateMachine`. Three
read operations are supported:

- **`head(key)`**: Returns the latest `VersionedEntry` for the given client
  shard, or `None` if the shard has no data.
- **`scan(key, from_seqno, limit)`**: Returns up to `limit` entries with
  `seqno >= from_seqno` for the given client shard.
- **`list_keys()`**: Returns all client shard keys that have at least one
  entry.

### Read Linearization

Reads must reflect all proposals committed at the time the read was issued.
The learner achieves this using a "bus stop" pattern:

1. A read arrives and is enqueued.
2. The learner fetches the acceptor's current upper via
   `fetch_recent_upper()`. This is the next batch number the acceptor will
   write at, meaning all batches before it are committed.
3. All reads arriving while the upper fetch is in-flight share the same
   linearization target. This amortizes the cost of the fetch across
   concurrent reads.
4. The learner blocks each read until its listen frontier reaches the target
   upper.
5. The read is served from the materialized state.

## Combined Path

The RPC service provides a combined interface that maps 1:1 to persist's
`Consensus` trait:

```
compare_and_set(key, expected, new_data):
  1. Construct CAS proposal
  2. Append via acceptor; receive receipt (batch_number, position)
  3. AwaitCasResult(batch_number, position) from learner
  4. Return CaSResult::Committed or CaSResult::ExpectationMismatch

head(key):
  1. Read from learner (linearized)

scan(key, from, limit):
  1. Read from learner (linearized)

truncate(key, seqno):
  1. Construct Truncate proposal
  2. Append via acceptor; receive receipt (batch_number, position)
  3. AwaitTruncateResult(batch_number, position) from learner
  4. Return TruncateResult

list_keys():
  1. Read from learner (linearized)
```

This is the primary interface for persist clients. They interact with the
shared log as they would any other `Consensus` implementation.

## Ordering Guarantees

1. **Total order on batches.** Persist's `compare_and_append` ensures
   batches are totally ordered by batch number.

2. **Deterministic order within batches.** Proposals within a batch are
   ordered by position (insertion order in the acceptor's pending buffer).

3. **Deterministic CAS evaluation.** Given the same log prefix, all
   learners evaluate proposals in the same order and arrive at the same
   client shard state. Batches are processed in batch number order;
   proposals within a batch are processed in position order; CAS evaluation
   depends only on the current client shard state and the proposal.

4. **Client shard independence.** A proposal for client shard A never
   affects the evaluation of a proposal for client shard B. Client shards
   are independent namespaces within the log.

## Ambiguous Append Handling

If `compare_and_append` to the log shard returns an ambiguous result
(timeout, network error), the acceptor cannot know whether the batch landed:

- If the batch did land, the upper has advanced. The acceptor's next
  `compare_and_append` attempt sees the new upper and adjusts.
- If the batch did not land, retrying with the same upper succeeds.

Persist's frontier advancement ensures at most one batch per timestamp.
Clients waiting on proposals from an ambiguous batch eventually receive
their results, from either the original append or the retry.

## Rehydration and Recovery

### Learner rehydration

A learner opens a `Listen` handle with `as_of = since` and rebuilds its
materialized state by processing events from there forward. Rollups reduce
the number of diffs the learner must scan to reach the current state.
Compaction reduces the number of blobs persist reads to serve those diffs.
The learner applies each batch through its state machine exactly as it
would during normal operation.

### Acceptor restart

The acceptor opens a new `WriteHandle` at the log shard's current upper.
Proposals in the pending buffer at crash time are lost; clients observe a
timeout and retry. The acceptor carries no durable state of its own.

## Log Shard Backend

The log shard is a persist shard and requires its own `Consensus`
implementation. This is where the recursion bottoms out: the log shard must
use an external system (object storage with conditional writes, an OLTP
database, etc.) for its root-level CAS operation. The choice of backend is
not yet settled. See [00_overview.md](00_overview.md) for context.
