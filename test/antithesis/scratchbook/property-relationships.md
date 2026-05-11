# Property Relationships

## Cluster 1: Persist Layer Safety

**Properties**: `persist-cas-monotonicity`, `tombstone-sealing-finality`, `idempotent-write-under-indeterminate`, `critical-reader-fence-linearization`

These properties share the persist state machine code in `src/persist-client/src/internal/`. They all exercise the compare-and-swap loop in `Machine` and the `State` transitions. A bug in the CaS loop or state validation could violate multiple properties simultaneously.

**Suspected dominance**: `persist-cas-monotonicity` is foundational — if SeqNo monotonicity breaks, all other persist properties likely break too. It dominates `tombstone-sealing-finality` and `critical-reader-fence-linearization`.

## Cluster 2: Fencing and Split-Brain Prevention

**Properties**: `epoch-fencing-prevents-split-brain`, `compute-replica-epoch-isolation`, `deployment-promotion-safety`

These properties all use epoch-based fencing to prevent stale actors from mutating state. They share the pattern of "increment epoch on new incarnation, reject operations from old epoch." The catalog fencing (`epoch-fencing-prevents-split-brain`) and deployment fencing (`deployment-promotion-safety`) share code paths in `src/catalog/src/durable/persist.rs`.

**Suspected dominance**: `epoch-fencing-prevents-split-brain` is the most fundamental — it protects the catalog. `deployment-promotion-safety` builds on it by also requiring caught-up checks before promotion. `compute-replica-epoch-isolation` is independent (different epoch mechanism for compute).

## Cluster 3: Crash Recovery Pipeline

**Properties**: `catalog-recovery-consistency`, `storage-command-replay-idempotent`, `fault-recovery-exercised`

These properties test the recovery path after process crashes. `fault-recovery-exercised` is the end-to-end liveness check; `catalog-recovery-consistency` and `storage-command-replay-idempotent` test specific subsystems within recovery.

**Suspected dominance**: `fault-recovery-exercised` is the weakest check (just "system comes back"). `catalog-recovery-consistency` is strictly stronger (catalog state is correct after recovery). If catalog recovery fails, the end-to-end recovery also fails.

## Cluster 4: Consistency Model

**Properties**: `strict-serializable-reads`, `mv-reflects-source-updates`, `source-ingestion-progress`

These properties form a chain: source ingestion feeds materialized views, which serve reads. `strict-serializable-reads` depends on correct timestamp oracle behavior and frontier management. If `source-ingestion-progress` fails (data doesn't flow), `mv-reflects-source-updates` also fails, but `strict-serializable-reads` could still pass on stale but consistent data.

**Suspected dominance**: `strict-serializable-reads` is independent of the liveness properties. `mv-reflects-source-updates` implies `source-ingestion-progress` (if MVs update, sources must have made progress).

## Cluster 5: Coordinator Concurrency

**Properties**: `group-commit-toctou-safety`, `peek-lifecycle-exactly-once`, `command-channel-ordering`

These properties target different concurrency mechanisms within the coordinator and compute engine. They share the coordinator's event loop as the execution context but test independent subsystems (write path, read path, command dispatch).

**No dominance**: These properties are independent of each other. A bug in peek handling doesn't imply a bug in group_commit or command channels.

## Cluster 6: Deployment Lifecycle

**Properties**: `deployment-promotion-safety`, `deployment-lag-detection`

Both test the 0DT deployment pipeline. `deployment-lag-detection` is a prerequisite for `deployment-promotion-safety` — if lag detection fails, promotion may proceed unsafely.

**Suspected dominance**: `deployment-promotion-safety` is stronger — it requires both lag detection and correct fencing. `deployment-lag-detection` is a liveness check on a subsystem of the promotion pipeline.

## Cluster 7: Kafka Source — User-Visible Ingestion Correctness

**Properties**: `kafka-source-no-data-loss`, `kafka-source-no-data-duplication`, `kafka-source-frontier-monotonic`, `kafka-source-survives-broker-fault`, `kafka-source-survives-clusterd-restart`

End-to-end Kafka source ingestion contract observable from the workload side. `kafka-source-no-data-loss` and `kafka-source-no-data-duplication` are the inverse-pair safety/liveness checks: every produced message must show up *exactly once*. The two recovery properties (`survives-broker-fault`, `survives-clusterd-restart`) exercise the same contract under different fault classes. `kafka-source-frontier-monotonic` is the lower-level safety property that both no-loss and no-duplication depend on.

**Suspected dominance**: `kafka-source-frontier-monotonic` underpins both `no-data-loss` and `no-data-duplication` — if the persist shard upper goes backwards, both higher-level properties fail. `survives-clusterd-restart` strictly implies `survives-broker-fault` for the recovery code path (clusterd restart triggers all the same rehydration logic plus more), but the two stress different fault classes.

## Cluster 8: UPSERT Envelope — Per-Key Semantics

**Properties**: `upsert-key-reflects-latest-value`, `upsert-tombstone-removes-key`, `upsert-state-rehydrates-correctly`, `upsert-decode-error-retractable`

The user-visible UPSERT contract. `upsert-key-reflects-latest-value` is the headline: latest produced value per key wins. `upsert-tombstone-removes-key` is the special-case for `None` values. `upsert-state-rehydrates-correctly` is the post-crash version of `latest-value`. `upsert-decode-error-retractable` is the error-recovery half of the contract — bad messages can be retracted.

**Suspected dominance**: `upsert-state-rehydrates-correctly` implies `upsert-key-reflects-latest-value` in steady state (rehydration produces the right state, and that state is what subsequent operations operate on). `upsert-tombstone-removes-key` is a special case of `upsert-key-reflects-latest-value` (the "last produced was null" case). `upsert-decode-error-retractable` is independent.

## Cluster 9: UPSERT Operator Internals — SUT-Side Asserts

**Properties**: `upsert-no-internal-panic`, `upsert-state-consolidation-wellformed`, `upsert-ensure-decoded-called-before-access`

Operator-internal correctness backbone for the UPSERT envelope. All three properties are about converting existing `panic!`/`assert!` sites in the upsert code into Antithesis-reportable assertions. `upsert-state-consolidation-wellformed` is the math-correctness check (XOR/checksum invariants in `ensure_decoded`); `upsert-ensure-decoded-called-before-access` is the type-state protocol check on `StateValue` accessors; `upsert-no-internal-panic` is the broader umbrella covering the diff-positive / commands-state / snapshot-completion guards.

**Suspected dominance**: `upsert-state-consolidation-wellformed` is the deepest signal — a trip there indicates upstream code already failed to preserve some invariant. `upsert-no-internal-panic`'s `assert!(diff.is_positive())` family catches a similar class of upstream-bug-evidence higher up the stack.

## Cluster 10: Kafka Source Internals — SUT-Side Asserts

**Properties**: `kafka-source-no-internal-panic`, `remap-shard-antichain-wellformed`, `reclock-mint-eventually-succeeds`, `offset-known-not-below-committed`

Reclock and source-reader operator-internal correctness. `remap-shard-antichain-wellformed` is the load-bearing invariant for the entire reclocking subsystem; `reclock-mint-eventually-succeeds` is its liveness companion. `kafka-source-no-internal-panic` is the umbrella for the explicit reader asserts. `offset-known-not-below-committed` is a much narrower statistics-causality check.

**Suspected dominance**: `remap-shard-antichain-wellformed` underpins everything in Cluster 7 — a malformed remap antichain corrupts the resume frontier, which breaks both data-loss and data-duplication properties at the next restart.

## Cross-Cluster Connections

- `epoch-fencing-prevents-split-brain` (Cluster 2) protects `catalog-recovery-consistency` (Cluster 3) — fencing ensures only one writer during recovery
- `persist-cas-monotonicity` (Cluster 1) underpins `catalog-recovery-consistency` (Cluster 3) — catalog is stored in persist, so CaS correctness is a prerequisite
- `strict-serializable-reads` (Cluster 4) depends on `epoch-fencing-prevents-split-brain` (Cluster 2) — split-brain would allow inconsistent timestamp assignments
- `idempotent-write-under-indeterminate` (Cluster 1) protects `storage-command-replay-idempotent` (Cluster 3) — storage ingestion uses persist writes, so idempotency matters for both
- `persist-cas-monotonicity` (Cluster 1) underpins `kafka-source-frontier-monotonic` (Cluster 7) — frontier monotonicity at the source level is a direct consequence of CaS monotonicity at the persist level
- `storage-command-replay-idempotent` (Cluster 3) supports `kafka-source-survives-clusterd-restart` (Cluster 7) — correct command replay is required for source recovery to be idempotent
- `idempotent-write-under-indeterminate` (Cluster 1) supports `kafka-source-no-data-duplication` (Cluster 7) — the no-duplicate-write guarantee at the persist level is what makes no-data-duplication observable at the source level
- `remap-shard-antichain-wellformed` (Cluster 10) underpins `kafka-source-no-data-loss` and `kafka-source-no-data-duplication` (Cluster 7) — a malformed remap antichain breaks the resume frontier across restart
- `upsert-state-consolidation-wellformed` (Cluster 9) underpins `upsert-state-rehydrates-correctly` (Cluster 8) — if the consolidating math is wrong, rehydration is wrong
- `source-ingestion-progress` (Cluster 4, pre-existing) is now subsumed by `kafka-source-no-data-loss` (Cluster 7) for Kafka specifically; `source-ingestion-progress` remains relevant for non-Kafka sources (Postgres CDC, MySQL CDC, generators)
- `mv-reflects-source-updates` (Cluster 4) depends on every Cluster 7 and Cluster 8 property — MVs over Kafka sources inherit those sources' correctness
