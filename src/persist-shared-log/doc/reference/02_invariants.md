# Persist Shared Log: Invariants & Properties

This document enumerates the properties the system must maintain. These
properties are the source of truth for formal verification (Stateright model
checking), deterministic simulation testing (DST) assertions, and stress
test acceptance criteria.

## Property Classification

Properties are organized into three tiers:

- _Safety_: Must always hold. A violation is a correctness bug.
- _Liveness_: Must eventually hold under fair scheduling. A violation
  indicates the system is stuck.
- _Performance_: Must hold within bounds under specified load. A violation
  is a scalability concern.

## Safety Properties

### Log Properties

**L1. Batch total order.**
Batches in the log are totally ordered by batch number. If batch A has
`batch_number < batch B`, then A precedes B.

*Follows from:* Persist's `compare_and_append` with frontier advancement.

**L2. Batch atomicity.**
All proposals in a batch are durably logged atomically. Either all proposals
in a batch are present in the log, or none are.

*Follows from:* Persist's `compare_and_append` atomicity.

**L3. Within-batch order.**
Proposals within a batch are ordered by position (0-indexed). This order is
deterministic and stable; it is the insertion order from the acceptor's
pending buffer at flush time.

**L4. Append-only log.**
Proposals are never modified or removed from the log by the acceptor. The
log is append-only from the perspective of proposal content. Persist may
compact the physical representation via rollups and compaction, but the
logical content is preserved.

**L5. No duplicate batches.**
Each batch number appears exactly once in the log. Persist's frontier
advancement prevents two `compare_and_append` calls from both succeeding at
the same timestamp.

### CAS Evaluation Properties

**C1. Deterministic evaluation.**
Given the same log prefix (batches 0..N), all learners produce identical
client shard state. CAS evaluation is a pure function of the log.

This is the fundamental correctness property. It allows multiple learners
to independently evaluate proposals and agree on results.

**C2. Linearizable CAS.**
If a CAS proposal P commits (its precondition matched), then P's effect is
visible to all subsequent proposals for the same client shard. "Subsequent"
means later in batch order, or at a later position within the same batch.

**C3. CAS precondition check.**
A CAS proposal with `expected = Some(s)` commits if and only if the current
head seqno for the client shard equals `s`. A CAS proposal with
`expected = None` commits if and only if the client shard has no entries.

**C4. Client shard independence.**
A proposal for client shard A never affects the evaluation outcome of a
proposal for client shard B. Formally: removing all proposals for client
shard A from the log does not change any evaluation result for client
shard B.

**C5. CAS seqno advancement.**
If a CAS proposal with `new_seqno = s` commits, then the client shard's
head seqno becomes `s` and remains `s` until the next committed CAS for
that shard.

### Truncate Properties

Truncate is a log operation: truncate proposals are appended to the log by
the acceptor and evaluated by the learner during playback, following the
same write path as CAS proposals. This ensures all learners apply the same
truncates in the same order, preserving deterministic evaluation (C1).

**T1. Truncate removes old entries.**
After a truncate with `seqno = s`, the client shard contains no entries
with `seqno < s`.

**T2. Truncate preserves recent entries.**
After a truncate with `seqno = s`, all entries with `seqno >= s` remain
in the client shard.

**T3. Truncate is idempotent.**
Truncating to a seqno at or below the current minimum seqno results in no
entries removed.

### Read Properties

**R1. Linearizable reads.**
A read issued at time T returns state that includes the effects of all
proposals committed before T. If a CAS commits and the client subsequently
issues a `head` for the same client shard, the result reflects the CAS.

*Implemented by:* The bus stop linearization protocol; reads wait for the
learner's listen frontier to reach the acceptor's upper at read issue time.

**R2. Snapshot consistency.**
A read returns a consistent snapshot of client shard state. `head` and
`scan` for the same client shard reflect the same set of committed
proposals.

**R3. Read availability under learner loss.**
If at least one serving learner is caught up to the linearization target,
reads are available.

### Acceptor Properties

**A1. Receipt uniqueness.**
Each `(batch_number, position)` receipt corresponds to exactly one proposal.

**A2. Receipt validity.**
If the acceptor returns receipt `(b, p)`, then batch `b` exists in the log
and contains a proposal at position `p`.

## Liveness Properties

**Lv1. CAS progress.**
If a CAS proposal is submitted to a non-failing acceptor and at least one
learner is running, the client eventually receives a result
(`committed = true` or `committed = false`).

**Lv2. Read progress.**
If a read is submitted to a non-failing learner and the acceptor is
running (so the learner can fetch the upper for linearization), the read
eventually returns.

**Lv3. Learner convergence.**
If a learner is subscribed to the log and the log is advancing, the learner
eventually processes all batches and its materialized state converges to
reflect the full log.

**Lv4. Upper advancement.**
If proposals are being submitted, the acceptor's upper (next batch number)
eventually advances.

## Performance Properties (Targets)

These are targets for stress testing. Violations indicate design or
implementation issues, not correctness bugs.

**P1. Writer throughput.**
The system sustains 10,000 concurrent writers at 10Hz with 4KiB payloads
(100,000 proposals/s aggregate).

**P2. Batch efficiency.**
At 10Hz writer tick rate with 20ms flush interval, each batch contains
roughly 2,000 proposals (100,000 proposals/s * 0.02s). Batching collapses
O(writers * tick_rate) into O(1/flush_interval) log writes.

**P3. CAS latency.**
End-to-end CAS latency (client submit to result received) is bounded by
flush_interval + learner_lag + network_RTT. At 20ms flush interval with
co-located learner: p50 < 25ms, p99 < 50ms.

**P4. Read latency.**
Read latency is bounded by linearization_check_RTT + learner_lag. For a
caught-up learner: p50 < 5ms, p99 < 15ms.

**P5. Rehydration time.**
A new learner rehydrating from the latest rollup reaches the current upper
within seconds. Rollup size and compaction determine this.

## Verification Matrix

Each property is verified by one or more approaches:

| Property | Stateright | DST | Stress Test | Code Review |
|----------|------------|-----|-------------|-------------|
| L1-L5    | Yes        | Yes |             | Yes         |
| C1       | Yes        | Yes |             | Yes         |
| C2       | Yes        | Yes |             |             |
| C3       | Yes        | Yes |             |             |
| C4       | Yes        | Yes |             |             |
| C5       | Yes        | Yes |             |             |
| T1-T3    | Yes        | Yes |             |             |
| R1       |            | Yes | Yes         | Yes         |
| R2       |            | Yes |             | Yes         |
| R3       |            |     | Yes         |             |
| A1-A2    |            | Yes |             | Yes         |
| Lv1-Lv4  | Yes*       | Yes | Yes         |             |
| P1-P5    |            |     | Yes         |             |

\* Stateright checks liveness via `eventually` properties, bounded by the
model's state space.

## Relationship to Persist's Own Invariants

The shared log builds on persist, which maintains its own invariants
(verified by the `persist-stateright` model):

- _Upper monotonicity_: The shard upper frontier never decreases.
- _Since monotonicity_: The shard since frontier never decreases.
- _Since <= upper_: The since frontier never exceeds the upper.
- _Write contiguity_: Appended batches are contiguous (no gaps in the
  frontier).

The shared log depends on these invariants. They are verified by persist's
own Stateright model and exercised by persist's own test suite. The shared
log's DST exercises the real persist code, so persist-level bugs would
surface as shared log test failures.
