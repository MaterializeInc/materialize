---
commit: ca6deb6758e651876582ae7d4dec24ce32d87567
updated: 2026-05-06
---

# Property Catalog: Materialize

## Category 1: Data Integrity Under Faults

Properties that verify data correctness when crashes, network partitions, and concurrent access interact with the persist layer and catalog.

### epoch-fencing-prevents-split-brain — Epoch-Based Fencing Prevents Split-Brain Writes

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P0 — fundamental split-brain prevention; failure here corrupts all state |
| **Property** | After a coordinator restart with a higher epoch, the old coordinator (lower epoch) cannot successfully write to the catalog persist shard. |
| **Invariant** | `Always`: once a higher epoch is written to consensus, any compare_and_append from a lower epoch must fail with FenceError. This is a strict safety invariant — every check must hold. |
| **Antithesis Angle** | Network partition separates old coordinator from consensus while new coordinator starts with higher epoch. When partition heals, old coordinator's in-flight writes must be rejected. Antithesis explores the timing window between old coordinator's last successful write and new coordinator's first write. |
| **Why It Matters** | Split-brain writes corrupt the catalog, potentially causing data loss or inconsistent schema state. This is the fundamental distributed safety mechanism. Surfaced by: Distributed Coordination, Failure Recovery. |

### persist-cas-monotonicity — Persist SeqNo Never Decreases

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P0 — backbone of persist consistency; all other persist properties depend on this |
| **Property** | Persist shard state versions (SeqNo) form a strictly increasing sequence. No writer can observe or apply a lower SeqNo after observing a higher one. |
| **Invariant** | `Always`: for any shard, if SeqNo N is observed, no subsequent observation returns SeqNo < N. Rollups maintain seqno <= seqno_since. This must hold on every check — a single violation means state corruption. |
| **Antithesis Angle** | Partition storage from persist backend mid-write. One writer races to increment SeqNo while another caches an old value and retries. Crash during GC/rollup operations. Antithesis explores interleaving of concurrent CaS loops. |
| **Why It Matters** | SeqNo monotonicity is the backbone of persist's consistency model. Violations cause state reconstruction failures and data loss. Surfaced by: Data Integrity, Distributed Coordination. |

### tombstone-sealing-finality — Tombstoned Shards Are Immutable

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — prevents zombie writes to dropped collections |
| **Property** | Once a shard's upper and since both advance to the empty antichain (tombstone), no new writes, reader registrations, or writer registrations can succeed. The transition is irreversible. |
| **Invariant** | `Always`: after `is_tombstone()` returns true, any append, downgrade_since, or registration attempt must fail. The state machine must never revert from tombstone. |
| **Antithesis Angle** | Crash and restart after tombstone. Fire concurrent write/read attempts while state is being replayed from consensus. Antithesis explores whether recovery code can accidentally un-tombstone a shard. |
| **Why It Matters** | Tombstone finality prevents zombie writes to dropped collections. Violation could resurface deleted data. Surfaced by: Data Integrity. |

### idempotent-write-under-indeterminate — Compare-and-Append Idempotency on Retry

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — indeterminate errors are the hardest distributed systems edge case |
| **Property** | When compare_and_append receives an Indeterminate error from consensus and retries with the same idempotency token, the shard contains exactly one copy of the write — never zero, never two. |
| **Invariant** | `Always`: after retry with identical IdempotencyToken, the shard's upper reflects exactly one successful write. Duplicate data must never appear in the shard trace. |
| **Antithesis Angle** | Inject network failures on consensus calls mid-flight. Kill writer after batch is queued but before state is committed. Antithesis explores the window between consensus write and acknowledgment. |
| **Why It Matters** | Indeterminate errors are the hardest to handle correctly in distributed systems. Duplication or loss here silently corrupts downstream materialized views. Surfaced by: Data Integrity. |

## Category 2: Consistency Model Enforcement

Properties that verify Materialize's strict serializability guarantee and timestamp oracle correctness.

### strict-serializable-reads — Reads Respect Timestamp Oracle Linearization

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P0 — Materialize's core advertised guarantee; user-visible |
| **Property** | Two reads on the same collection at timestamps t1 < t2 (assigned by the oracle) must observe consistent ordering: if t1 sees state S, t2 cannot observe a state prior to S. |
| **Invariant** | `Always`: for any two reads where oracle assigns t1 < t2, the result at t2 must include all changes visible at t1. The oracle read timestamp must advance monotonically. |
| **Antithesis Angle** | Run parallel transactions in StrictSerializable mode. One writes, another reads concurrently. Inject delays in oracle timestamp advancement. Antithesis explores whether reads can bypass the linearization point. |
| **Why It Matters** | Strict serializability is Materialize's core advertised guarantee. Users explicitly choose it over eventual consistency. Violation is a correctness bug visible to end users. Surfaced by: Protocol Contracts. |

### catalog-recovery-consistency — Catalog State Consistent After Crash Recovery

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — catalog corruption on recovery prevents system from starting |
| **Property** | After coordinator crash and restart, the recovered catalog state is equivalent to the pre-crash state: upper never decreases, snapshot is consolidated, and all committed transactions are visible. |
| **Invariant** | `Always`: upper(post_restart) >= upper(pre_crash). After sync_to_current_upper(), the snapshot contains no unconsolidated entries (all diffs resolved). |
| **Antithesis Angle** | Crash coordinator during catalog_transact (after some updates persist but before upper advances). Crash during consolidation. Antithesis explores the timing of crashes within the catalog write path. |
| **Why It Matters** | Catalog inconsistency after recovery can cause schema corruption, lost DDL, or inability to restart. Surfaced by: Failure Recovery. |

## Category 3: Compute and Storage Recovery

Properties that verify correct behavior during and after process crashes in the compute and storage layers.

### compute-replica-epoch-isolation — Stale Replica Commands Rejected After Rehydration

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — stale commands cause compute divergence and wrong query results |
| **Property** | Each compute replica incarnation has a unique epoch (nonce + u64). After rehydration with epoch N+1, no commands from epoch N can execute or affect dataflow state. |
| **Invariant** | `Always`: once a command with epoch N+1 is processed, all epoch N commands are dropped. The epoch forms a strict ordering on replica incarnations. |
| **Antithesis Angle** | Kill compute replica mid-dataflow. Controller rehydrates with new epoch. In-flight commands from the old epoch leak back due to network buffering. Antithesis explores whether stale commands can sneak past the epoch check. |
| **Why It Matters** | Stale command execution causes compute replicas to diverge from the coordinator's expected state, potentially returning wrong query results. Surfaced by: Distributed Coordination. |

### storage-command-replay-idempotent — Storage Command History Replay Is Idempotent

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — non-idempotent replay causes data duplication in all downstream MVs |
| **Property** | When a storage replica reconnects, the controller replays command history from the last frontier. Replaying the same commands twice yields identical state — no duplicated ingestion or state divergence. |
| **Invariant** | `Always`: apply(history[0:i]) + apply(history[0:i]) == apply(history[0:i]). Source ingestion positions must resume from persisted offsets, not restart from zero. |
| **Antithesis Angle** | Crash storage controller mid-send of RunIngestionCommand. Restart and replay history. Antithesis explores whether partial command delivery causes duplicate ingestion. |
| **Why It Matters** | Non-idempotent replay causes duplicate data in sources, which propagates to all downstream materialized views. Surfaced by: Failure Recovery. |

## Category 4: Concurrency and Race Conditions

Properties that verify correctness under concurrent access patterns within the coordinator.

### group-commit-toctou-safety — No Phantom Writes to Deleted Tables

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — phantom writes corrupt catalog; TOCTOU explicitly acknowledged in code |
| **Property** | If a table is deleted between when a write is deferred and when group_commit executes, the write is silently dropped — not persisted. No phantom writes to non-existent tables. |
| **Invariant** | `Always`: if catalog.try_get_entry(table_id) returns None at group_commit time, the write's updates are not included in the committed batch. |
| **Antithesis Angle** | Concurrent table deletion + write operations. Antithesis delays between deferred write queuing and group_commit catalog check, exposing the TOCTOU window where the table ceases to exist between validation and execution. |
| **Why It Matters** | Phantom writes to deleted tables corrupt the catalog or cause panics during downstream processing. The explicit TOCTOU check in appends.rs:479-486 acknowledges this risk. Surfaced by: Concurrency. |

### peek-lifecycle-exactly-once — Each Peek Gets Exactly One Response

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — leaked peeks cause OOM; explicit 1:1 contract documented |
| **Property** | For each peek command sent to compute, exactly one PeekResponse is delivered to the client — no duplicates, no missing responses, no orphaned pending_peeks entries. |
| **Invariant** | `Always`: count(peek_commands) == count(peek_responses) with bijective UUID mapping. When CancelPendingPeeks races with PeekNotification, exactly one of (canceled, completed) occurs — never both, never neither. |
| **Antithesis Angle** | Trigger replica failures mid-peek. Race cancel requests with response delivery. Antithesis explores the two-map removal sequence (client_pending_peeks + pending_peeks) that is not atomic. |
| **Why It Matters** | Leaked peeks cause memory growth and eventually OOM. Duplicate responses confuse clients. The 1:1 contract is explicitly documented in peek.rs:80-95. Surfaced by: Protocol Contracts, Concurrency. |

### command-channel-ordering — Timely Workers See Commands in Identical Order

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P2 — code explicitly acknowledges ordering is unguaranteed; hard to trigger |
| **Property** | CreateDataflow commands broadcast through the command channel execute in identical order across all Timely workers — no reordering. |
| **Invariant** | `Always`: for any two workers W1 and W2, if W1 sees command A before B, W2 also sees A before B. Code comment at command_channel.rs:88-90 explicitly notes this relies on "Timely channels preserving order of inputs, which is not something they guarantee." |
| **Antithesis Angle** | Inject timing delays in the source operator between command channel invocations. Stress the sync_activator bridge between sync and async contexts. Antithesis explores whether worker scheduling variations cause reordering. |
| **Why It Matters** | Command reordering causes workers to diverge, producing inconsistent dataflow results. The code explicitly acknowledges this is unguaranteed. Surfaced by: Concurrency. |

## Category 5: Lifecycle Transitions

Properties about 0DT deployment, startup, and shutdown correctness.

### deployment-promotion-safety — 0DT Promotion Only After Full Catchup

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P2 — relevant for cloud deployments; requires multi-coordinator setup |
| **Property** | During 0DT deployment, the new coordinator transitions to ReadyToPromote only after catalog is loaded, caught-up checks pass, and all replica frontiers have advanced past the required threshold. Promotion with stale replicas is prevented. |
| **Invariant** | `Always`: at the moment set_ready_to_promote() is called, all collections tracked by caught_up checks have frontiers >= the cutoff threshold. The deployment generation fence prevents the old coordinator from writing after promotion. |
| **Antithesis Angle** | Trigger preflight concurrently with replica startup. Crash replicas during catchup. Antithesis explores whether the caught_up check can pass while a replica is still lagging or crash-looping. |
| **Why It Matters** | Premature promotion causes the new coordinator to serve stale data or fail to serve at all. This is the primary risk in zero-downtime deployments. Surfaced by: Lifecycle, Distributed Coordination. |

### deployment-lag-detection — Caught-Up Check Detects Stuck Replicas

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | P2 — companion to deployment-promotion-safety; requires 0DT setup |
| **Property** | During 0DT catchup, maybe_check_caught_up() eventually detects replicas that are lagging beyond configured thresholds or crash-looping, and prevents promotion until resolved. |
| **Invariant** | `Sometimes(lagging_replica_detected)`: Antithesis should observe at least one scenario where a lagging/crashing replica is detected and promotion is blocked. This is a liveness property — the detection must eventually happen. |
| **Antithesis Angle** | Inject replica crashes during catchup phase. Verify the analyze_replica_looping() function identifies the problem via mz_cluster_replica_status_history. |
| **Why It Matters** | Undetected stuck replicas during 0DT deployment lead to silent data staleness in production. Surfaced by: Lifecycle. |

## Category 6: Reachability and Coverage

Properties that verify the system reaches interesting states under fault injection.

### fault-recovery-exercised — System Recovers from Coordinator Crash

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | P0 — most fundamental operational property; prerequisite for all others |
| **Property** | After the coordinator (environmentd) crashes and restarts, the system eventually becomes healthy (readiness endpoint returns 200) and can serve SQL queries. |
| **Invariant** | `Sometimes(healthy_after_crash)`: the system must reach a state where it can serve queries after a crash. This confirms recovery works end-to-end, not just in unit tests. |
| **Antithesis Angle** | Kill environmentd at various points during operation. Verify it restarts, reconnects to persist, recovers catalog, and serves queries. Antithesis explores crash timing — during DDL, during peek, during group_commit. |
| **Why It Matters** | Recovery is the most critical operational property. If it doesn't work, nothing else matters. Surfaced by: Failure Recovery. |

### source-ingestion-progress — Kafka Source Eventually Catches Up

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | P2 — important but requires Kafka/Redpanda in topology |
| **Property** | After creating a Kafka source, Materialize eventually ingests all available data and the source's write frontier advances past the data's timestamps. |
| **Invariant** | `Sometimes(source_frontier_advances)`: the source's upper antichain must advance at least once during the test run, confirming data is flowing through the ingestion pipeline. |
| **Antithesis Angle** | Create a Kafka source, produce messages, then inject network faults between Materialize and Redpanda. Verify the source eventually catches up when connectivity is restored. |
| **Why It Matters** | Source ingestion is the primary data path. If it stalls, all downstream materialized views stop updating. Surfaced by: Product Context. |

### mv-reflects-source-updates — Materialized Views Eventually Reflect Source Changes

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | P1 — end-to-end user-visible correctness; Materialize's core value |
| **Property** | After data is written to a source, materialized views that depend on that source eventually reflect the new data. |
| **Invariant** | `Sometimes(mv_contains_new_data)`: after inserting data into a table or producing to a Kafka source, a SELECT on a dependent materialized view must eventually return the new data. |
| **Antithesis Angle** | Insert data, inject faults (compute replica crash, storage reconnection), then verify the MV eventually shows the data. Antithesis explores whether faults during the incremental update pipeline cause permanent stalls. |
| **Why It Matters** | This is the end-to-end user-visible correctness property. Materialize's value proposition is that MVs are always up-to-date. Surfaced by: Product Context. |

### critical-reader-fence-linearization — Critical Reader Opaque Token Linearizes

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — incorrect fencing allows premature GC causing data loss |
| **Property** | When two concurrent critical readers attempt compare_and_downgrade_since with mismatched opaque tokens, exactly one succeeds in updating the shard's since. No reader can re-observe an old opaque value after a SeqNo increment. |
| **Invariant** | `Always`: concurrent compare_and_downgrade_since operations with different opaques result in exactly one mutation. The winner's opaque is durably recorded; the loser gets a mismatch. |
| **Antithesis Angle** | Inject network delays between state check and state commit. Fail CaS operations after token comparison but before state write. Antithesis explores concurrent reader contention. |
| **Why It Matters** | Critical readers control garbage collection boundaries. Incorrect fencing allows premature GC, which deletes data needed by active readers. Surfaced by: Data Integrity. |
