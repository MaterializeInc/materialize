---
commit: 007c7af9d9970fb2030c7212368b232e0fbc363e
updated: 2026-05-14
---
<!-- Category 8 (MySQL CDC) added 2026-05-12: mysql-source-no-data-loss -->
<!-- 2026-05-14: mysql-source-gtid-monotonicity-violation added to Category 9 -->

# Property Catalog: Materialize

## Category 1: Data Integrity Under Faults

Properties that verify data correctness when crashes, network partitions, and concurrent access interact with the persist layer and catalog.

### epoch-fencing-prevents-split-brain — Epoch-Based Fencing Prevents Split-Brain Writes

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P0 — fundamental split-brain prevention; failure here corrupts all state |
| **Status** | **Partially implemented (SUT-side, single-coordinator scope)** — `src/catalog/src/durable/persist.rs`: an `assert_always_greater_than!(new_epoch, prior_durable_epoch, "catalog fencing: new durable epoch did not strictly increase after fence-token CaS", …)` fires after each successful fence-token CaS in `open_inner`. Every environmentd restart in the Antithesis topology exercises this path. **The cross-coordinator half of the property (a `Fenced` writer being correctly rejected at validate time) is NOT exercised today and is not planned.** Materialize does not run multiple concurrent environmentd processes against the same catalog shard in any supported topology, so the `FenceableToken::Fenced` state is unreachable here. The two `assert!` panics in `FenceableToken::validate` would be the natural Antithesis anchor for that half; they are intentionally left as bare panics with an in-source comment pointing back to this entry, to be promoted to `assert_always!` if a 0DT-preflight-style multi-environmentd topology is ever added. |
| **Property** | After a coordinator restart with a higher epoch, the old coordinator (lower epoch) cannot successfully write to the catalog persist shard. |
| **Invariant** | `Always`: once a higher epoch is written to consensus, any compare_and_append from a lower epoch must fail with FenceError. This is a strict safety invariant — every check must hold. |
| **Antithesis Angle** | Network partition separates old coordinator from consensus while new coordinator starts with higher epoch. When partition heals, old coordinator's in-flight writes must be rejected. Antithesis explores the timing window between old coordinator's last successful write and new coordinator's first write. |
| **Why It Matters** | Split-brain writes corrupt the catalog, potentially causing data loss or inconsistent schema state. This is the fundamental distributed safety mechanism. Surfaced by: Distributed Coordination, Failure Recovery. |

### persist-cas-monotonicity — Persist SeqNo Never Decreases

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P0 — backbone of persist consistency; all other persist properties depend on this |
| **Status** | **Implemented (SUT-side)** — `src/persist-client/src/internal/apply.rs`: alongside the existing `assert_eq!(expected.next(), new_state.seqno(), …)` strict-increment check in `compute_next_state_locked`, an `assert_always_greater_than!(new_state.seqno().0, expected.0, "persist: state seqno did not strictly increase across CaS apply", …)` makes the broader monotonicity invariant a reportable Antithesis property rather than only a process panic. The strict-equality `assert_eq!` is retained so the narrower invariant (next == seqno) still surfaces. The companion rollup-seqno invariant (`state.rs:1324` doc comment) is deferred. |
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

### critical-reader-fence-linearization — Critical Reader Opaque Token Linearizes

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — incorrect fencing allows premature GC causing data loss |
| **Property** | When two concurrent critical readers attempt compare_and_downgrade_since with mismatched opaque tokens, exactly one succeeds in updating the shard's since. No reader can re-observe an old opaque value after a SeqNo increment. |
| **Invariant** | `Always`: concurrent compare_and_downgrade_since operations with different opaques result in exactly one mutation. The winner's opaque is durably recorded; the loser gets a mismatch. |
| **Antithesis Angle** | Inject network delays between state check and state commit. Fail CaS operations after token comparison but before state write. Antithesis explores concurrent reader contention. |
| **Why It Matters** | Critical readers control garbage collection boundaries. Incorrect fencing allows premature GC, which deletes data needed by active readers. Surfaced by: Data Integrity. |

## Category 2: Consistency Model Enforcement

Properties that verify Materialize's strict serializability guarantee and timestamp oracle correctness.

### strict-serializable-reads — Reads Respect Timestamp Oracle Linearization

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P0 — Materialize's core advertised guarantee; user-visible |
| **Status** | **Implemented (workload-side)** — `test/antithesis/workload/test/parallel_driver_strict_serializable_reads.py`. Inserts one row per step into `mv_input_table` and, between steps, opens a *fresh* psycopg connection (explicit `SET transaction_isolation TO 'strict serializable'`) to SELECT the rolling-count MV's row for the invocation's prefix. After a quiet-period closing observation, asserts (a) `always("…fresh-connection read regressed across adjacent observations", …)` for every adjacent pair, and (b) `always("…closing fresh-connection read regressed below earlier maximum", …)` for the closing read versus the historical max. One `sometimes("…final fresh-connection read reached inserted count", …)` liveness anchor. The SUT-side oracle-timestamp-non-decreasing mirror in `src/adapter/src/coord/in_memory_oracle.rs` is deferred. |
| **Property** | Two reads on the same collection at timestamps t1 < t2 (assigned by the oracle) must observe consistent ordering: if t1 sees state S, t2 cannot observe a state prior to S. |
| **Invariant** | `Always`: for any two reads where oracle assigns t1 < t2, the result at t2 must include all changes visible at t1. The oracle read timestamp must advance monotonically. |
| **Antithesis Angle** | Run parallel transactions in StrictSerializable mode. One writes, another reads concurrently. Inject delays in oracle timestamp advancement. Antithesis explores whether reads can bypass the linearization point. |
| **Why It Matters** | Strict serializability is Materialize's core advertised guarantee. Users explicitly choose it over eventual consistency. Violation is a correctness bug visible to end users. Surfaced by: Protocol Contracts. |

### catalog-recovery-consistency — Catalog State Consistent After Crash Recovery

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — catalog corruption on recovery prevents system from starting |
| **Status** | **Implemented (workload-side)** — `test/antithesis/workload/test/singleton_driver_catalog_recovery_consistency.py`. Long-running singleton driver holds an in-process `expected_tables` set across cycles. Each cycle runs one CREATE TABLE or DROP TABLE under `execute_retry`, then opens a *fresh* psycopg connection and SELECTs `mz_tables` filtered to the driver's namespace, asserting `always("catalog recovery: live catalog table set matches in-process expected model", …)`. Cross-cycle stability is exactly the recovery check: if an environmentd restart lands between cycles, the next cycle's read is the post-recovery snapshot. Two `sometimes(...)` anchors record (a) "2+ assertion cycles ran" so the post-restart half is exercised, and (b) "observed environmentd connect failure during run" as a corroborating signal that a fault actually landed. The SUT-side upper-non-regression mirror in `sync_to_current_upper` and the consolidation `assert_always!` are deferred. |
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
| **Status** | **Implemented (workload-side)** — `test/antithesis/workload/test/anytime_fault_recovery_exercised.py`. Anytime driver probes `SELECT 1` with a short connect timeout (bypassing helper_pg's retry budget so the fault-active window is observable) and records `sometimes("...succeeded after a previously-observed connect failure", …)` for the recovery transition, plus corroborating `sometimes` anchors for "observed replica non-online" and "at least one probe succeeded this invocation". |
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
| **Status** | **Implemented (workload-side, table-backed)** — `test/antithesis/workload/test/parallel_driver_mv_reflects_table_updates.py` + `helper_table_mv.py`. Each invocation inserts N rows tagged with a per-invocation prefix into `mv_input_table`, polls the rolling-count MV `mv_input_count` after a quiet period, and pairs `sometimes("mv: row_count caught up …", …)` (liveness anchor) with `always("mv: row_count equals inserted count …", …)` (safety on the settled count). Kafka-source-backed MV is covered indirectly by the Kafka-source drivers — direct MV-on-Kafka-source coverage is deferred. |
| **Property** | After data is written to a source, materialized views that depend on that source eventually reflect the new data. |
| **Invariant** | `Sometimes(mv_contains_new_data)`: after inserting data into a table or producing to a Kafka source, a SELECT on a dependent materialized view must eventually return the new data. |
| **Antithesis Angle** | Insert data, inject faults (compute replica crash, storage reconnection), then verify the MV eventually shows the data. Antithesis explores whether faults during the incremental update pipeline cause permanent stalls. |
| **Why It Matters** | This is the end-to-end user-visible correctness property. Materialize's value proposition is that MVs are always up-to-date. Surfaced by: Product Context. |

## Category 7: Kafka Source Ingestion (Append-Only + UPSERT)

Properties specific to the Kafka source ingestion pipeline: `KafkaSourceReader` → `ReclockOperator` → optional decode/UPSERT → `persist_sink`. Both envelopes are covered, with shared properties for reclocking and source-frontier behavior. Workload-level checks compare produced Kafka records against what a SQL `SELECT` over the source returns; SUT-side checks live in the source/upsert/reclock operators.

### kafka-source-no-data-loss — Every Produced Record Is Eventually Visible

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | P0 — primary user-visible contract; "data is in Kafka but not in Materialize" is the worst possible streaming bug |
| **Status** | **Implemented (workload-side, NONE envelope)** — `test/antithesis/workload/test/parallel_driver_kafka_none_envelope.py`. Per-payload `always("kafka source: every produced payload is visible exactly once", …)` joined to a quiet-period catchup wait. UPSERT-envelope version is covered by `upsert-key-reflects-latest-value`. The SUT-side `assert_sometimes!(persist_sink_appended_batch)` anchor in `append_batches` is deferred. |
| **Property** | After producing a message to a Kafka topic, the Materialize source over that topic eventually contains a row corresponding to that message (NONE envelope) or a row reflecting the latest value for that key (UPSERT envelope). |
| **Invariant** | `Sometimes(all_produced_records_visible)`: at least once during a run, after a quiet period, the workload observes `COUNT(*) FROM source` >= number of produced records (NONE) or every produced (key, value) pair is reflected in the source state (UPSERT). Liveness, so `Sometimes` on the catch-up event. |
| **Antithesis Angle** | Network partitions between Materialize and Kafka, clusterd kills mid-ingestion, persist write retries, and rebalances. The interesting timing is the *crash mid-batch* window: some offsets are in persist, some are not, and the resume frontier determines what we re-read. Antithesis explores whether the re-read covers exactly the missing offsets. |
| **Why It Matters** | This is the headline guarantee of a streaming database. A bug here is silent data loss visible to every user of the source. Supersedes the more generic `source-ingestion-progress` for Kafka specifically. |

### kafka-source-no-data-duplication — No Record Appears Twice After Settling

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P0 — silent duplication corrupts every aggregate downstream MV |
| **Status** | **Implemented (workload-side, NONE envelope)** — `test/antithesis/workload/test/parallel_driver_kafka_none_envelope.py`. `always("kafka source: no duplicate (partition, offset)", …)` over a `GROUP BY partition, "offset" HAVING COUNT(*) > 1` query scoped to the invocation's prefix; carries up to five offending rows in `details`. UPSERT-envelope version is covered indirectly by `upsert-key-reflects-latest-value` (per-key uniqueness assertion) and directly by the SUT-side `assert_always!(diff.is_positive(), …)` of `upsert-no-internal-panic`. |
| **Property** | After settling, the NONE-envelope source contains at most one row per `(partition, offset)` tuple; the UPSERT-envelope source contains at most one row per key. |
| **Invariant** | `Always`: `SELECT partition, "offset", COUNT(*) FROM source GROUP BY 1,2 HAVING COUNT(*) > 1` returns no rows for NONE; `SELECT key, COUNT(*) FROM source GROUP BY 1 HAVING COUNT(*) > 1` returns no rows for UPSERT. Checked on every assertion firing — must hold on every observation. |
| **Antithesis Angle** | Reader crashes between persist-sink batch write and `compare_and_append`; rehydration re-reads offsets we already wrote. The protection lives in `last_offsets` filtering (kafka.rs:1158) but only for the *current* incarnation — across restart, idempotency depends on the persist sink and (for UPSERT) the feedback-driven snapshot. Antithesis explores crash/restart timing across batch boundaries. Direct regression target for upsert double-retraction bug (commit 1accbe28b3, database-issues#9160). |
| **Why It Matters** | Duplicate rows in the source flow into every downstream materialized view's aggregates and joins. Silent and devastating. |

### kafka-source-frontier-monotonic — Source Persist Shard Upper Never Regresses

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — frontier regression panics downstream operators and breaks `AS OF` queries |
| **Status** | **Implemented (workload-side)** — `test/antithesis/workload/test/anytime_kafka_frontier_monotonic.py`. Continuous `anytime_` driver polls `mz_internal.mz_source_statistics.offset_committed` for every known Kafka source every 500ms and asserts `always("kafka: source offset_committed non-monotonic", details)` whenever a new sample is less than the previous one. Faults are active throughout. SUT-side `assert_always!(new_upper >= prev_upper, ...)` in `append_batches` is deferred. |
| **Property** | The `upper` frontier of the source's data persist shard never regresses across the lifetime of the source, including across clusterd restarts and `compare_and_append` retries. |
| **Invariant** | `Always`: observed `upper(t2) >= upper(t1)` for any observation order `t1 < t2`. Checked on every observation in a workload polling loop, and ideally also as a SUT-side `assert_always!` next to the persist sink's `compare_and_append`. |
| **Antithesis Angle** | Kill clusterd mid-`compare_and_append`; resume the source with a stale cached upper; concurrent reclock and persist-sink writers. Direct regression target for the `as_of`/reclock-upper race (commit e3805ad790, database-issues#8698) and the persist-sink cached upper bug (commit 505dc96aaa). |
| **Why It Matters** | Frontier regression manifests as panics (`as_of > upper`) or as observably incorrect AS OF queries. Documented invariant for persist. |

### kafka-source-survives-broker-fault — Source Resumes After Broker Connectivity Restored

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | P1 — operational expectation; broker faults are a routine condition |
| **Status** | **Implemented (workload-side, shared driver)** — `test/antithesis/workload/test/anytime_kafka_source_resumes_after_fault.py`. Continuous polling state machine per Kafka source: `OBSERVING` -> `STALLED` after N consecutive identical `offset_committed` samples, then `Reachable("...resumed advancing after a sustained stall", …)` on the first strictly-greater sample. The driver tags each recovery with `saw_kafka_metadata_failure` (broker-fault signal) and `saw_replica_non_online` (clusterd-restart signal) so triage can distinguish the two fault classes. |
| **Property** | After a transient network partition or Kafka broker outage that prevents the source from making progress, once connectivity is restored, the source eventually ingests all messages that were produced during the outage. |
| **Invariant** | `Sometimes(source_resumes_after_broker_fault)`: at least once per run, after injecting a network fault between materialized and Kafka and then calling `ANTITHESIS_STOP_FAULTS`, the workload observes the source's `COUNT(*)` advance past its pre-fault value. |
| **Antithesis Angle** | Network partition between the `materialized` container and the Kafka container; persist+metadata stay reachable. Tests rdkafka reconnect, snapshot statistics restoration (commit 0a34b6c79d), and that no permanent stall mode is entered. |
| **Why It Matters** | Cloud streaming setups routinely see transient Kafka unavailability. A source that gets stuck and never recovers is an outage. |

### kafka-source-survives-clusterd-restart — Source Resumes After clusterd Crash

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | P1 — recovery from clusterd kill is the most common operational fault path |
| **Status** | **Implemented (workload-side, shared driver)** — same `test/antithesis/workload/test/anytime_kafka_source_resumes_after_fault.py` as `kafka-source-survives-broker-fault`. The stall-then-advance transition is fault-kind-agnostic; `saw_replica_non_online` corroborates that the source recovered specifically from a clusterd kill. Combines with the existing `kafka-source-no-data-duplication` and `kafka-source-no-data-loss` assertions to also rule out double-counting and gaps on the rehydrated path. Requires node-termination faults to be enabled in the Antithesis tenant. |
| **Property** | After clusterd (storage worker) is killed and restarted, the Kafka source recovers, replays the right resume offsets, and ingests messages produced before, during, and after the restart. |
| **Invariant** | `Sometimes(source_recovered_after_clusterd_restart)`: after a kill+restart, eventually `COUNT(*) FROM source >= produced_count`. Combined with `kafka-source-no-data-duplication` to also rule out double-counting. |
| **Antithesis Angle** | Direct test of the `storage-command-replay-idempotent` mechanism end-to-end through Kafka. Antithesis explores crash timing across the reclock mint, persist-sink append, and upsert snapshot-completion windows. Requires node-termination faults to be enabled. |
| **Why It Matters** | This is the recovery contract the storage controller is built around. Failure here makes every higher-level property meaningless. |

### upsert-key-reflects-latest-value — UPSERT Source Reflects Latest Value Per Key

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P0 — the entire user-visible promise of the UPSERT envelope |
| **Status** | **Implemented** (workload-side) — `test/antithesis/workload/test/parallel_driver_upsert_latest_value.py`. Two `always()` assertions ("upsert: SELECT for key matches latest produced value", "upsert: tombstoned key has no row in source") plus one `sometimes()` liveness anchor ("upsert: source caught up to produced offsets after quiet period"). |
| **Property** | At a settled timestamp, for each key produced by the workload, the UPSERT source contains exactly the value from the last `(key, value)` message produced — or no row if the last message for that key was a tombstone. |
| **Invariant** | `Always`: for every workload-tracked key, `SELECT value FROM source WHERE key = ?` returns the expected value (or empty for tombstoned keys), as determined by the workload's local model of what it produced. Checked after `ANTITHESIS_STOP_FAULTS` quiet periods. |
| **Antithesis Angle** | Reorder produce timing, kill clusterd between the prior-value lookup (`multi_get`) and the new-value write (`multi_put`), inject delays in the feedback-driven snapshot phase. Tests order-key monotonicity (commit f177db8286), state-backend consistency, and snapshot-completion correctness. |
| **Why It Matters** | UPSERT semantics — "the source mirrors the upstream key/value store" — is the reason customers pick this envelope. Wrong value per key is silent corruption that flows into all downstream MVs. |

### upsert-tombstone-removes-key — Tombstone Eventually Removes the Key

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — delete semantics are routinely relied on for GDPR/correctness |
| **Status** | **Implemented (workload-side)** — `test/antithesis/workload/test/parallel_driver_upsert_latest_value.py`. The existing `always("upsert: tombstoned key has no row in source", ...)` covers the safety half; a new `sometimes("upsert: tombstone overwrote a live value at least once this invocation", ...)` confirms the *interesting* tombstone path (tombstone replacing a live value) is exercised rather than the trivial "tombstone a never-written key" case. |
| **Property** | After producing a `(key, null)` tombstone message to the Kafka topic, the UPSERT source eventually contains no row for that key, and the row stays absent until a new non-null value is produced. |
| **Invariant** | `Always`: at any settled observation after the tombstone has been ingested (resume_upper > tombstone offset), `SELECT * FROM source WHERE key = ?` returns 0 rows. The "no resurrection" half is also `Always`: a key that has been tombstoned and not re-inserted must not reappear after a clusterd restart or rehydration cycle. |
| **Antithesis Angle** | Race the tombstone against a state-store snapshot completion. Crash clusterd between persist sink writing the retraction and the upsert state recording the tombstone. The `StateValue::Value` -> tombstone path in `upsert/types.rs` is the relevant code; bugs here look like resurrected rows. |
| **Why It Matters** | A "deleted" row reappearing is both a correctness bug and a compliance hazard. |

### upsert-state-rehydrates-correctly — UPSERT State Reconstructs Exactly After Restart

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — incorrect rehydration produces wrong-but-plausible-looking output |
| **Status** | **Implemented (workload-side)** — `test/antithesis/workload/test/singleton_driver_upsert_state_rehydration.py`. Long-running `singleton_driver_` runs N produce→settle→assert cycles holding `expected_state` in process memory. Cross-cycle stability is the rehydration check: if a clusterd restart lands between cycles, the next cycle's `always("upsert: rehydrated state matches local model (live key|tombstoned key)", ...)` verifies the rebuilt source matches the pre-restart model. Requires node-termination faults enabled. |
| **Property** | After a clusterd restart, the rehydrated upsert state, as observed via `SELECT * FROM source`, equals the state at the most recent durable timestamp before the restart, for every key produced so far. |
| **Invariant** | `Always`: after a kill+restart quiet period, the workload's local key/value model matches the source's contents for every key whose latest message has `offset <= resume_upper`. Combines with `kafka-source-no-data-duplication` (no double inserts on rehydration) and `upsert-key-reflects-latest-value` (correct value per key). |
| **Antithesis Angle** | The interesting window is between `compare_and_append` of the persist sink and the upsert operator's feedback-driven snapshot completion. If the feedback replay deduplication is wrong, rehydrated state diverges from durable state. Direct regression target for the upsert snapshot-completion logic in `upsert/types.rs` and `upsert_continual_feedback*`. |
| **Why It Matters** | Wrong rehydration is silent — the source comes up "healthy" and serves bad data. Hardest class of bug to detect in production. |

### upsert-decode-error-retractable — Bad Value Errors Are Retracted By Subsequent Good Value

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P2 — documented contract; supports operational "fix the bad message and continue" recovery |
| **Property** | When a Kafka message decoding produces an `UpsertError::Value` (or `UpsertError::KeyDecode` or `UpsertError::NullKey`) for a key, and a subsequent message produces a valid `(key, value)` pair for the same key, the source state for that key transitions from "row containing error" to "row containing the new value" — i.e. the error is retracted. |
| **Invariant** | `Always`: at a settled timestamp after the corrective message has been ingested, `SELECT * FROM source WHERE key = ?` returns the corrected value with no remaining error row. Note this is the *upsert*-specific retractability (`EnvelopeError::Upsert(..)`); `EnvelopeError::Flat(..)` is explicitly non-retractable. |
| **Antithesis Angle** | Produce an undecodable value, then a good value for the same key, while injecting delays between the two. Race against snapshot completion (errored value during snapshot vs. corrected value post-snapshot). |
| **Why It Matters** | Encoded as the operational contract by which users recover from upstream schema mistakes without dropping the source. Code in `upsert_commands` (render/sources.rs) and `upsert.rs` is the relevant path. |

### upsert-no-internal-panic — Upsert Operator's Internal Asserts Never Fire

| | |
|---|---|
| **Type** | Reachability (Unreachable) |
| **Priority** | P1 — these panics are explicit "should-never-happen" guards that bug history has hit |
| **Status** | **Implemented (SUT-side, reachable sites only)** — every targeted *reachable* site has a uniquely-messaged `assert_always!`/`assert_unreachable!` paired with the original `panic!`/`assert!`: `upsert_continual_feedback.rs` (stash diff-positive, `commands_state` missing key), `upsert_continual_feedback_v2.rs` (input diff-positive, `(None, None)` join), and `upsert/types.rs` (`merge_update_state` non-Consolidating, double snapshot completion). The mirror sites in `src/storage/src/upsert.rs` (classic) were dropped: `upsert_operator` hard-codes `use_continual_feedback_upsert = true` (commit a63d1763e5, Feb 2025), so the classic-upsert code is provably unreachable in supported configurations and Antithesis-instrumenting it added dead-weight assertions. Panics still terminate the process; Antithesis receives a reportable property failure with rich details for every reachable site. |
| **Property** | The explicit panics and `assert!`s in the upsert operator never fire under any Antithesis-injected fault sequence. Specifically (reachable sites): `assert!(diff.is_positive(), "invalid upsert input")` (upsert_continual_feedback.rs:626, v2:315); `panic!("key missing from commands_state")` (upsert_continual_feedback.rs:800); `unreachable!()` for `(None, None)` in continual-feedback v2 (v2:483); the order-key panic that used to live in `drain_staged_input` (now a skip; commit f177db8286). |
| **Invariant** | `Unreachable`: each of these sites is converted to an Antithesis `assert_unreachable!("…")` (or `assert_always!(false, …)`) so that any firing produces an explicit Antithesis property failure rather than a process crash. Distinct, unique message per site. |
| **Antithesis Angle** | These are the high-signal SUT-side anchors. They catch the same family of bugs that historically reached production: order-key regression, missing dedup entry, retraction-on-input. Adding them costs almost nothing in the SUT and gives Antithesis precise replay anchors. |
| **Why It Matters** | These panics indicate the operator entered an internal state its author thought was impossible. Past bugs (commits f177db8286, 1accbe28b3) reached production exactly through these paths. The asserts already exist; we just need to wrap them with the Antithesis SDK so the failures become reportable properties rather than process kills. |

### upsert-state-consolidation-wellformed — `ensure_decoded` Resolves To `diff_sum ∈ {0, 1}` With Matching Checksums

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P0 — directly guards upsert state-store data integrity; catches XOR/checksum corruption |
| **Status** | **Implemented (SUT-side)** — `src/storage/src/upsert/types.rs`. Five `assert_always!` calls inside `ensure_decoded` covering the `diff_sum == 1` checksum match, the three `diff_sum == 0` zero-residue checks, and the `diff_sum ∉ {0,1}` impossible-state path. Each carries the consolidating state's diagnostic in `details`. |
| **Property** | When the upsert state backend's `StateValue::ensure_decoded` finalizes a `Consolidating` cell into either a live `Value` or a `tombstone`, the consolidating accumulator is well-formed: `diff_sum ∈ {0, 1}`; if `diff_sum == 1` the recovered bytes match the recorded `len_sum` and `checksum_sum` (seahash of `value_xor[..len_sum]`); if `diff_sum == 0` then `len_sum == 0`, `checksum_sum == 0`, and every byte of `value_xor` is zero. |
| **Invariant** | `Always`: the `panic!("invalid upsert state: non 0/1 diff_sum: …")` at `upsert/types.rs:672` becomes an `assert_always!(false, "upsert: non 0/1 diff_sum")` with a unique message. The intermediate `assert_eq!`s at :621, :632, :637 and the `assert!` at :642 are likewise upgraded to `assert_always!` so they report rather than crash. Each site gets a distinct, specific message. |
| **Antithesis Angle** | The consolidating state collapses many `(diff, bytes)` updates per key into running `diff_sum`, `len_sum`, `checksum_sum`, and an XOR-merged `value_xor` blob. The invariant relies on (a) every retraction being paired with an identical insertion in the snapshot stream, and (b) the snapshot completion contract delivering exactly the durable state at the resume frontier. Antithesis explores: crash mid-snapshot-replay, RocksDB merge operator interleaved with multi_put, partial feedback delivery across restart, and (most subtly) duplicated retractions from multi-replica drain (commit 1accbe28b3). Any of these can break the XOR cancellation and trip a non-{0,1} diff_sum. |
| **Why It Matters** | This is the deepest "the math broke" guard in the upsert pipeline. A trip here means either the feedback stream replayed wrong contents or a duplicate retraction snuck through. The existing panic already dumps a rich diagnostic — wrapping it as an Antithesis assertion turns it into a reportable, replayable property failure rather than a process abort. |

### upsert-ensure-decoded-called-before-access — Consolidating State Is Always Decoded Before Use

| | |
|---|---|
| **Type** | Reachability (Unreachable) |
| **Priority** | P2 — type-state protocol invariant; high-signal as a replay anchor |
| **Status** | **Implemented (SUT-side)** — `src/storage/src/upsert/types.rs`. Six `assert_unreachable!` calls, one per accessor (`into_decoded`, `into_provisional_value`, `into_provisional_tombstone`, `provisional_order`, `provisional_value_ref`, `into_finalized_value`), each with a distinct message naming the accessor. Original `panic!` preserved after the assertion. |
| **Property** | Every accessor on `StateValue` that requires the cell to be in `Value` form is preceded by a call to `ensure_decoded` for that cell. The six accessor panics — `into_decoded` (297), `into_provisional_value` (369), `into_provisional_tombstone` (403), `provisional_order` (416), `provisional_value_ref` (430), `into_finalized_value` (440) — never fire. |
| **Invariant** | `Unreachable`: each `panic!("called \`...\` without calling \`ensure_decoded\`")` site is converted to a distinct `assert_unreachable!("upsert: <accessor> on Consolidating")`. Six unique assertion messages, one per accessor, so an Antithesis report distinguishes which contract was violated. These are pure protocol-misuse guards — they cannot fire in valid execution. |
| **Antithesis Angle** | These panics are most likely to fire after a code change to the upsert operator (e.g. a new code path that forgets `ensure_decoded` before reading `provisional_value`). Antithesis exercises every operator branch under fault injection; turning these into reachability assertions gives a cheap regression-detection net for future refactors of `upsert.rs` / `upsert_continual_feedback*.rs`. They are also useful replay anchors — if Antithesis ever does reach them, the bug is reproducible. |
| **Why It Matters** | These guard a type-state contract that is currently enforced only at runtime. The cost of instrumenting them is essentially zero (rename `panic!` to `assert_unreachable!`), and the upside is that any future violation surfaces as a property failure that can be replayed deterministically. |

### kafka-source-no-internal-panic — Kafka Source Reader's Explicit Panics Never Fire

| | |
|---|---|
| **Type** | Reachability (Unreachable) |
| **Priority** | P1 — direct regression target for topic-recreation and offset-handling bugs |
| **Status** | **Implemented (SUT-side, production sites)** — `src/storage/src/source/kafka.rs` covers the four production panic/assert sites (`unexpected source export details`, `partition_consumers not drained at shutdown`, `partition missing from last_offsets`, `negative offset from non-error message`); `src/storage/src/source/reclock/compat.rs` covers `compare_and_append InvalidUsage`. The remaining `expect()` sites on resume-upper / statistics / offset arithmetic are deferred to a follow-up; they would be a wide mechanical conversion to soft assertions rather than reportable properties. |
| **Property** | The explicit panics in `kafka.rs` never fire: `panic!("got negative offset (...)")` (kafka.rs:1193); `panic!("unexpected source export details: ...")` (kafka.rs:276); the `assert!(self.last_offsets[output][partition])` (kafka.rs:1142); plus the `expect()` sites on resume-upper / statistics / offset arithmetic. |
| **Invariant** | `Unreachable`: each site converted to a unique `assert_unreachable!("kafka: <site>")`. The "negative offset" panic in particular is a known structural-invariant violation that has fired before. |
| **Antithesis Angle** | Topic deletion + recreation, partition rebalancing, manual offset reset on the Kafka broker, clock jumps that interact with Kafka's internal offset arithmetic. Direct regression target for commit 99ad668af5 (capability downgrade on topic recreation). |
| **Why It Matters** | A panic in the source reader takes down the storage worker. Replacing the panic with an Antithesis assertion gives a *reportable* failure rather than a crash that masks itself as "clusterd was restarted." |

### remap-shard-antichain-wellformed — Remap Shard Accumulates To Well-Formed Antichain

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P1 — load-bearing invariant for reclock correctness; explicitly stated in source doc comment |
| **Property** | At every Materialize timestamp `t`, the remap shard's contents accumulated to `t` form a well-formed `Antichain<KafkaTimestamp>`: each source-time element has frequency exactly 1, the antichain is not empty if any source data has been bound, and (under multi-partition source) there is one element per partition range with no overlaps. |
| **Invariant** | `Always`: enforced as an `assert_always!` inside `ReclockOperator::mint`/`sync` after every state update — that's where the doc comment promises the invariant (reclock.rs:31-34). Workload-level approximation: a periodic SQL query that joins source/remap progress with computed offsets and verifies one-to-one. |
| **Antithesis Angle** | Concurrent reclock writers (across restart), partition adds/removes between mints, `compare_and_append` retries that interleave with metadata refresh. The remap shard is the only place where source-time → into-time is durably recorded; a malformed antichain corrupts every subsequent restart's resume frontier. |
| **Why It Matters** | This is the foundational reclock invariant. Violation here breaks recovery (resume_upper computed wrong), `AS OF` semantics, and the upsert operator's snapshot phase. |

### reclock-mint-eventually-succeeds — Reclock Mint Completes Despite CaS Retries

| | |
|---|---|
| **Type** | Liveness |
| **Priority** | P2 — pre-existing concern under persist instability |
| **Status** | **Implemented (SUT-side anchor)** — `src/storage/src/source/reclock.rs`: `ReclockOperator::mint` carries a local `cas_retry_count` and fires `assert_reachable!("reclock: mint completed after at least one compare_and_append UpperMismatch", …)` after the while-loop terminates when at least one `UpperMismatch` was observed. The reachability anchor covers the "retry path was exercised AND mint terminated" half of the property. The workload-side "source frontier advanced past the contention point" liveness check is approximated by the existing `anytime_kafka_frontier_monotonic.py` + `anytime_kafka_source_resumes_after_fault.py` drivers and is not duplicated here. |
| **Property** | Under transient persist outages or competing writers, the reclock mint loop (`compare_and_append` with `UpperMismatch` retry, reclock.rs:160-166) eventually completes for every source-frontier advance that has data to bind. |
| **Invariant** | `Sometimes(mint_completed_after_cas_retry)`: at least once per run, Antithesis observes a reclock mint that took >1 CaS attempt and then completed (i.e. a successful retry path was exercised). Critically, the workload should also observe that the source frontier eventually advances past the value of `source_upper` captured at the time of the contention — i.e. the loop is not livelocked. |
| **Antithesis Angle** | Inject persist consensus latency, kill+restart concurrently to create a competing writer, race the metadata fetcher's partition-add against a mint that is already in flight. The retry loop in `mint()` has no upper bound; this property confirms it is not livelocked even under adversarial schedules. |
| **Why It Matters** | A livelocked mint loop manifests as a source that never advances its frontier — externally indistinguishable from a stalled Kafka consumer, but caused inside Materialize. |

## Category 8: Randomized Concurrency Stress

Properties that use intentionally adversarial concurrent SQL workloads to flush
out catalog, planning, and recovery bugs that are hard to encode as a single
deterministic correctness scenario.

### parallel-workload-no-unexpected-errors — Randomized Concurrent SQL Only Hits Expected Race Errors

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P2 — broad regression net rather than one product contract, but good at finding real crashes and catalog races |
| **Status** | **Implemented (workload-side)** — `test/antithesis/workload/test/parallel_driver_parallel_workload.py`. A shared schema plus four tables/four materialized views are stressed by multiple worker threads racing `CREATE`, `DROP`, `INSERT`, `UPDATE`, `DELETE`, and `SELECT`. The driver records `sometimes("parallel workload: randomized concurrent SQL executed successfully", …)` for liveness, `sometimes("parallel workload: DDL actions were exercised", …)` for coverage, `sometimes("parallel workload: expected concurrent-catalog races were observed", …)` to confirm the workload is hitting the intended contention paths, and one `always("parallel workload: no unexpected SQL errors escaped the randomized stress driver", …)` safety assertion for the failure signal itself. This is intentionally a subset port of `test/parallel-workload/mzcompose.py`, scoped to the existing Antithesis topology rather than the full mzcompose service matrix. |
| **Property** | Under fault injection and concurrent randomized SQL, Materialize may return expected dropped-object / concurrent-catalog errors, but it must not surface *unexpected* query failures. |
| **Invariant** | `Always`: every SQL exception raised by the randomized workload matches the driver's expected-concurrency ignore list; any uncategorized error is a property failure. |
| **Antithesis Angle** | Antithesis can pause or restart environmentd/clusterd while several client threads concurrently create/drop objects and query them. The interesting windows are plan invalidation, catalog transaction races, and recovery of half-finished DDL. |
| **Why It Matters** | This is a broad bug-finder for timing-sensitive failures that do not map cleanly to one narrow user contract but still produce visible query failures or crashes. It complements the more specific properties by covering the "something went wrong under concurrent SQL churn" space. |

## Category 9: MySQL CDC Source

Properties specific to Materialize's MySQL CDC source pipeline, which reads
from a multithreaded MySQL replica. The topology adds a MySQL primary (GTID +
WRITESET dependency tracking) and a MySQL replica (4 parallel workers,
commit-order preservation) to the Antithesis environment.

### mysql-source-no-data-loss — Every Row Written to MySQL Primary Is Eventually Visible

| | |
|---|---|
| **Type** | Liveness + Safety |
| **Priority** | P1 — end-to-end correctness of the MySQL CDC pipeline; tests a distinct code path from Kafka |
| **Status** | **Implemented (workload-side)** — `test/antithesis/workload/test/parallel_driver_mysql_cdc.py` + `first_mysql_replica_setup.py`. Each `parallel_driver_` invocation inserts 20 rows to MySQL primary, waits for a quiet period, then polls `antithesis_cdc` until all rows appear (or 90 s budget expires). `always("mysql: CDC source row has correct value after catchup", …)` and `always("mysql: CDC source row count matches inserted count after catchup", …)` fire per-row and per-batch after confirmed catchup; `sometimes("mysql: CDC source caught up to all primary inserts after quiet period", …)` is the liveness anchor. The `first_mysql_replica_setup.py` creates the MySQL schema, configures multithreaded replication (4 workers, `replica_preserve_commit_order=ON`), and creates the Materialize connection/source/table, firing `reachable("mysql: first-run setup complete …")` as a coverage anchor. |
| **Property** | After inserting a row to the MySQL primary (via the binlog + GTID-based multithreaded replica), the Materialize CDC source eventually contains that row with the correct value. |
| **Invariant** | `Always`: after catchup, for every row inserted to `antithesis.cdc_test` on the primary, `SELECT value FROM antithesis_cdc WHERE id = ?` returns the expected value. `Sometimes`: catchup completes within the quiet-period budget at least once per run. |
| **Antithesis Angle** | Kills to the MySQL replica container (replica restarts from persisted GTID position); kills to the MySQL primary (replica and Materialize source must handle upstream silence gracefully); clusterd restarts (MySQL CDC resume exercises the same `storage-command-replay-idempotent` path as Kafka); parallel worker scheduling jitter that stresses the `replica_preserve_commit_order` protocol. |
| **Why It Matters** | MySQL CDC is a distinct ingestion code path from Kafka. Wrong behavior here — dropped rows, wrong values after restart, duplicate rows after resume — is not caught by the Kafka-source drivers. |

### mysql-source-gtid-monotonicity-violation — MySQL Source Must Not Error Due to Out-of-Order GTIDs

| | |
|---|---|
| **Type** | Safety (Unreachable) |
| **Priority** | P1 — permanent source error with no self-recovery path; directly testable by Antithesis fault injection against the multithreaded replica's commit-order protocol |
| **Status** | **Implemented (SUT-side + workload-side)** — `src/storage/src/source/mysql/replication/partitions.rs`: `assert_unreachable!("mysql: BinlogGtidMonotonicityViolation — received out-of-order GTID from multithreaded replica", …)` fires immediately before `DefiniteError::BinlogGtidMonotonicityViolation` is returned in `advance_frontier`, giving Antithesis a precise replay anchor at the exact causal site. Workload-side: `test/antithesis/workload/test/anytime_mysql_source_no_gtid_errors.py` polls `mz_internal.mz_source_statuses` every 2 s and fires `always(not is_gtid_error, "mysql: source must not enter errored state due to out-of-order GTIDs", …)` at the user-visible error surface. |
| **Property** | The Materialize MySQL CDC source must never receive a GTID with a lower transaction-id than one already observed for the same UUID. With `replica_preserve_commit_order=ON` and 4 parallel replica workers, the commit-order protocol must hold even under Antithesis fault injection. |
| **Invariant** | `Unreachable`: the `BinlogGtidMonotonicityViolation` error site in `advance_frontier` must never be reached. `Always`: `mz_internal.mz_source_statuses` for the MySQL CDC source must never show `status = 'errored'` with `error` containing `"out of order gtids"`. |
| **Antithesis Angle** | Scheduling jitter under 4 parallel replica workers; container kills of the replica at arbitrary replication progress points; network delays between primary and replica that could desynchronize the commit-order queue. The property tests whether `replica_preserve_commit_order=ON` holds its guarantee when Antithesis controls the scheduler. |
| **Why It Matters** | `BinlogGtidMonotonicityViolation` is a `DefiniteError` — the source is permanently stuck with no self-recovery path. It also silently neutralizes the `mysql-source-no-data-loss` liveness assertions (catchup never completes once the source is errored). Surfaced by: MySQL CDC source configuration, multithreaded replication correctness. |

### offset-known-not-below-committed — Source Statistics Causality

| | |
|---|---|
| **Type** | Safety |
| **Priority** | P2 — observable statistics correctness; regression target for commit 3e32df1f69 |
| **Status** | **Implemented (workload-side)** — `test/antithesis/workload/test/anytime_kafka_offset_known_not_below_committed.py`. Continuous polling driver queries every Kafka source's `mz_source_statistics_per_worker` row and fires `always("kafka: source offset_known < offset_committed", …)` whenever a single per-worker row has `offset_known < offset_committed`. Both fields are read from the same row of the same query so the comparison cannot cross a metric-update boundary. The SUT-side mirror in `src/storage/src/statistics.rs` is deferred. |
| **Property** | For every Kafka source, the source-statistics view always reports `offset_known >= offset_committed`. The metric `offset_known` reflects what the broker has told us is available; `offset_committed` reflects what Materialize has durably ingested. Causally, `offset_known` cannot lag `offset_committed`. |
| **Invariant** | `Always`: a polling assertion in the workload — `SELECT offset_known, offset_committed FROM mz_internal.mz_source_statistics_per_worker WHERE id = ?` — invariant `offset_known >= offset_committed`. Mirror as an `assert_always!` inside the statistics update path in `src/storage/src/statistics.rs`. |
| **Antithesis Angle** | Clusterd restart resets `offset_known` to broker-reported watermark while `offset_committed` is restored from persist. If the restoration order is wrong, the invariant flips. Direct regression target for commit 3e32df1f69. |
| **Why It Matters** | The statistics view is consumed by users and by operational tooling to compute lag. A regression in causality makes lag metrics meaningless and is the kind of bug that survives unit tests but fails under adversarial timing. |
