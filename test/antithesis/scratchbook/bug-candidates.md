# Bug Candidates for Antithesis Reproduction

Bugs found by mining the Materialize git history for timing/concurrency fixes
that Antithesis's deterministic scheduling would reliably find.

## 1. Persist Lease Race (Best Candidate)

**Commit**: `43f024da36` — "persist: Make sure to obtain a lease before selecting a batch"
**PR**: #35554
**Severity**: Production incident — read-time halt
**Category**: TOCTOU race

### The Bug

Persist uses "seqno leases" to prevent GC from deleting batches a reader is
still processing. Before the fix, readers selected a batch *then* obtained a
lease. GC could delete the batch in between:

```
Reader                              GC
──────                              ──
1. snapshot() at SeqNo 5
   → picks BatchA (blob: part-0001)
                                    2. Compaction merges BatchA away → SeqNo 6
                                    3. seqno_since advances (no lease on 5)
                                    4. Deletes part-0001 from blob storage
5. lease_seqno() → SeqNo 7 (too late)
6. fetch(BatchA) → 404 → HALT
```

The fix reorders to: lease first, then select batch. The lease prevents GC
from advancing past the leased SeqNo.

### Code Paths Affected

- `Listen::next` (read.rs:287) — continuous feed that hydrates MVs. Runs in
  the background for every materialized view with an active source. This is the
  most natural trigger — always active, exercises the lease path on every new
  batch.
- `snapshot_cursor` (read.rs:1176) — used by "persist peeks" (SELECT on
  unindexed tables). Less common than the listen path.
- `snapshot_and_fetch` (read.rs:889) — used by catalog ops and txn-WAL reads.

All three now go through `snapshot_batches()` (read.rs:846), which does
lease-then-snapshot.

### Workload to Trigger

Simple mixed read/write traffic exercises the listen path:
- Continuous INSERTs into a table (creates new batches → SeqNo churn → GC pressure)
- A materialized view over that table (its listen is always running)
- Concurrent SELECTs on the MV (served from in-memory arrangements, but
  the listen feeding the MV is the actual race target)

Compaction and GC run automatically in the background. Antithesis's scheduler
can interleave GC between batch selection and lease acquisition.

### Properties

- `persist-cas-monotonicity` — batch data should never disappear
- `critical-reader-fence-linearization` — leases should protect batches
- Workload-side: reads never hang or error unexpectedly
- SUT-side: the panic at read.rs:864 fires if a batch is missing after the
  upper advanced (added by the fix — would need to be preserved in a
  revert-and-detect test)

### Testing Notes

A pure `git revert` of `43f024da36` removes both the fix AND the panic that
detects the impossible state. To validate, surgically revert only the ordering
(put lease back after snapshot) while keeping the panic or replacing it with
`assert_unreachable!`.

---

## 2. Compute Dependency Frontier Race

**Commit**: `42a22b7ff5` — "compute: fix a race condition in collecting dependency frontiers"
**Severity**: Compute controller panic
**Category**: TOCTOU — check-then-act across async boundary

### The Bug

The compute controller checked whether storage collections existed, then
collected their frontiers in a second step. Collections could be dropped
between the two steps:

```
Step 1: check_exists(collection_id) → true
                                          storage drops collection_id
Step 2: collections_frontiers([collection_id]) → panic! missing key
```

Fix: replaced the two-step check-then-read with a single
`collection_frontiers(id).ok()` that handles missing collections atomically.

**File**: `src/compute-client/src/controller/instance.rs`

### Workload to Trigger

Rapid concurrent DDL — CREATE/DROP of sources and MVs while the compute
controller is resolving dependency frontiers.

### Properties

- `compute-replica-epoch-isolation`
- System should never panic from DDL operations

---

## 3. Reclock Upper Race with as_of

**Commit**: `e3805ad790` — "Fetch latest upper in reclock to avoid races with as_of"
**Severity**: Panic (fixes database-issues#8698)
**Category**: Stale cached value in timing-sensitive decision

A cached `upper` became stale between caching and `as_of` calculation, causing
panic when `as_of > upper`.

### Properties

- `strict-serializable-reads`

---

## 4. MV-Sink Discarding Valid Batch Descriptions

**Commit**: `0886c94dc2` — "mv-sink: stop discarding valid batch descriptions"
**Severity**: Silent data loss
**Category**: Stale frontier view

Incorrect persist frontier view caused valid batch descriptions to be rejected
as "outdated." No crash, no error — just silently dropped data.

### Properties

- `mv-reflects-source-updates`

---

## 5. Introspection Collection Frontier Regression

**Commit**: `ec4f8996bb` — "compute: avoid frontier regressions for introspection collections"
**Severity**: Frontier monotonicity violation
**Category**: Initialization ordering mismatch

### Properties

- `persist-cas-monotonicity`

---

## 6. as_of Selection Upper Constraint Bugs

**Commit**: `e6ca4801fa` — "as_of_selection: fix two bugs around upper constraints"
**Severity**: 0dt upgrade availability blocked
**Category**: Incorrect boundary calculation

### Properties

- `deployment-promotion-safety`
