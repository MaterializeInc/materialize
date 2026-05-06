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

## Cross-Cluster Connections

- `epoch-fencing-prevents-split-brain` (Cluster 2) protects `catalog-recovery-consistency` (Cluster 3) — fencing ensures only one writer during recovery
- `persist-cas-monotonicity` (Cluster 1) underpins `catalog-recovery-consistency` (Cluster 3) — catalog is stored in persist, so CaS correctness is a prerequisite
- `strict-serializable-reads` (Cluster 4) depends on `epoch-fencing-prevents-split-brain` (Cluster 2) — split-brain would allow inconsistent timestamp assignments
- `idempotent-write-under-indeterminate` (Cluster 1) protects `storage-command-replay-idempotent` (Cluster 3) — storage ingestion uses persist writes, so idempotency matters for both
