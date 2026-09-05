# Improved hydration visibility

- Associated: SQL-565
- PRD: [Improved hydration visibility](https://app.notion.com/p/3a513f48d37b806cb3a5e7f5b5a9c1e6)

## The Problem

Hydration is a black box.
A user starting or restarting a cluster cannot tell how long hydration will take, whether it is making progress, or how much memory and disk it will need.
The three questions that matter are: how long will this take, is it making progress, and what resources does it need.

The catalog surfaces that exist today answer none of these durably.
`mz_internal.mz_hydration_statuses` is a point-in-time boolean.
`mz_internal.mz_compute_hydration_statuses` adds a `hydration_time` interval, but it is derived from a differential logging collection that resets to empty on every `environmentd` or replica restart.
`mz_internal.mz_cluster_replica_metrics_history` retains sampled CPU, memory, and disk for 30 days, but it samples every ~60s and records no peaks, so the sharp memory spike that drives an OOM during hydration is routinely missed.
Nobody records what memory hydration peaked at, and hydration times do not survive the one event that most often triggers rehydration: a restart or upgrade.

This has cost real POCs.
Zepz was stuck for two days during their POC because they could not tell what hydration was doing.
General Mills went down mid-hydration and could not tell whether it had OOMed.
The same gap hurt the Chainalysis POC.
The core assumption behind any fix: it is impossible to predict the *very first* hydration of a workload, because join order, data skew, and data volume are all data-dependent, so the only trustworthy signal is the last observed hydration.

## Success Criteria

A solution is successful when:

1. **History survives restart.** A user can query how long an object or a replica took to hydrate the last time, and that answer survives `environmentd` restarts, replica restarts, and failed upgrades.
2. **Peaks are real, not sampled.** The recorded peak memory and disk for a hydration episode reflect the true high-water mark of the episode, not the maximum of ~60s samples that can miss a spike.
3. **Failures are rows, not gaps.** An OOM-killed or canceled hydration produces a durable row that records the terminal status and the peak that killed it, joinable against `mz_cluster_replica_status_history`. (Reachable only via the measured-at-source per-object path; the coordinator-only MVP does not produce terminal-status rows. See Open Question 1. If V1 resolves to coordinator-only, this criterion moves to a later milestone.)
4. **Progress is honest and monotone.** A user can see "N of M dataflow nodes hydrated" for an in-flight object, with no false ETA precision.
5. **Per-replica granularity.** With replication factor > 1, every replica gets its own history rows, and slowest-replica rollups are a `GROUP BY` away.
6. **Stable schema.** The user-facing relations live in `mz_catalog` so customers can build views and dashboards on top without breakage.
7. **Available to agents.** The data is queryable through the developer MCP server, and a docs/skill guide explains how to read it and how to act on it.

Quantitatively: the PRD's motivating query (last five hydration episodes on a named cluster, with duration and peaks) returns correct rows after a full replica restart.

## Out of Scope

* **Prometheus metrics, Grafana dashboard, and 0dt read-only-generation visibility.** These are the PRD's Future Requirements and belong to the Cloud / self-managed team. V1 catalog history is written only by the read-write leader (see "Read-only mode" below), so it does not cover the read-only generation during a 0dt upgrade. Longer-horizon retention and alerting ride on the Prometheus work.
* **Sources and sinks.** V1 objects are materialized views and indexes. Storage-object hydration is tracked separately today (`mz_source_statistics.rehydration_latency`) and is an open question below.
* **Predictive ETAs.** Node-fraction progress is not time-linear. We expose progress and last-observed duration, never a computed time-remaining. Revisit only if a provably-linear workload class is found.
* **Speeding up hydration.** That is the cluster autoscaling PRD (`HYDRATION_SIZE`) and compute-team optimization work. This project makes the wait visible, not shorter. The two compose: autoscaling consumes hydration status, and users need this data to pick a good `HYDRATION_SIZE`.
* **Console changes.** The improved-clusters-page PRD reserves a hydration pill that will consume this data later.
* **Auto-tuning `HYDRATION TIME ESTIMATE` on REFRESH clusters** from history. Good follow-up, but V1 only exposes the data.

## Solution Proposal

### Summary

Persist hydration history in durable, append-only introspection collections, and expose it through stable `mz_catalog` relations.
Two new histories: one per object hydration on a replica, one per replica hydration episode carrying peak resources.
A third relation, a progress view, rolls up the existing per-LIR-node hydration booleans.
The design reuses the machinery that already backs `mz_cluster_replica_status_history` and `mz_source_status_history`: append-only introspection collections written into persist shards, pruned by a time-window retention policy driven by a dyncfg.
The one genuinely new mechanism is true peak-resource capture, which the existing 60s metrics sampling cannot provide.

The two hard problems, and how we solve them:

1. **Durability.** The existing hydration signal (`mz_compute_hydration_times`) is a differential collection that resets on restart. We instead write hydration episodes into append-only introspection collections, which are persist-backed and survive restart exactly like the status histories do.
2. **True peaks.** Metrics are sampled every ~60s (`src/controller/src/clusters.rs:936`) and no running max is kept anywhere. A sampled peak that misses the spike produces exactly the OOM-surprise this project exists to kill (PRD, and the July 16 meeting flagged this). We capture the kernel-tracked high-water mark from the cgroup via clusterd's existing usage endpoint, and fall back to a controller-side running max over samples where the kernel value is unavailable. **This mechanism only yields a per-episode true peak if the cgroup counter is reset at episode start.** A cgroup high-water mark is since-container-start (or since-reset), so without a reset it over-reports every episode after the first on a replica that has not restarted, which is the routine `CREATE INDEX`-on-running-cluster case. Whether clusterd can reset the counter (write `memory.peak` on cgroup v2 / `memory.max_usage_in_bytes` on v1) is unverified and gates Success Criterion 2. See Open Question 3; until reset is proven feasible, peak values are trustworthy only for a replica's first episode, and Success Criterion 2 is met only for that case.

### Why append-only introspection collections, not `BuiltinTable`s

The existing `*_history` relations are **not** coordinator-written `BuiltinTable`s.
`mz_cluster_replica_status_history`, `mz_source_status_history`, and `mz_sink_status_history` are `BuiltinSource`s whose `data_source` is an `IntrospectionType` (`src/catalog/src/builtin/mz_internal.rs:1061`, `:1179`, `:2229`).
They are backed by storage-managed persist shards registered as append-only introspection collections (`register_append_only_collection`, `src/storage-controller/src/lib.rs:3251`), written via `append_introspection_updates` → `CollectionManager::blind_write` (`src/storage-controller/src/lib.rs:2485`).

This mechanism gives us, for free, exactly the properties the PRD asks for:

* **Durability across restart.** Rows live in a persist shard, not an in-memory differential collection. This is the property `mz_compute_hydration_times` lacks.
* **Retention.** A background task prunes at collection `prepare()` time (`src/storage-controller/src/collection_mgmt.rs:1134`), applying a `StatusHistoryRetentionPolicy::TimeWindow(30 days)` (`src/storage-controller/src/lib.rs:4009`). Two distinct config mechanisms feed such a window, and the spec must not conflate them:
  * The replica *status* history reads its window from a SQL system parameter (`VarDefinition` `replica_status_history_retention_window`, `src/sql/src/session/vars/definitions.rs:1468`), threaded through `StorageParameters` into `replica_status_history_desc(&parameters)` (`src/storage-controller/src/lib.rs:4090`) and pruned by `partially_truncate_status_history` (`:1568`). Adding a field here also requires the coordinator-side system-parameter push into `StorageParameters`, which is the legacy `update_storage_config` path the adapter guide flags (`guide-adapter.md:58`).
  * The replica *metrics* history reads its window from a true dyncfg (`Config<Duration>` `REPLICA_METRICS_HISTORY_RETENTION_INTERVAL`, `src/storage-types/src/dyncfgs.rs:61`) via `config_set`, pruned by `partially_truncate_metrics_history` (`src/storage-controller/src/collection_mgmt.rs:1145`).
  * **Decision:** follow the metrics-history dyncfg path, not the status-history `VarDefinition` path. Our data is resource/time-series introspection (the closer analog), and the dyncfg avoids touching `StorageParameters` and the legacy `update_storage_config` plumbing entirely. We add one `Config<Duration>` dyncfg per new collection (default 30 days) and a matching `TimeWindow` retention descriptor read via `config_set`.
* **Append-only semantics.** `blind_write` on an append-only collection is exactly "add a row", which is the write shape for a history.

We therefore do not touch the catalog transaction / implications framework (`src/adapter/src/coord/catalog_implications.rs`) for the row writes.
That framework governs durable catalog *DDL* Ops. History rows are introspection data, not catalog items. The adapter-guide invariants that do apply here are distributed correctness and "no local-only assumptions" (see Correctness below), not the implications pipeline.

### Catalog surface: raw in `mz_internal`, stable views in `mz_catalog`

Follow the established idiom: the raw introspection collection lives in `mz_internal`, and the stable, documented relation in `mz_catalog` is a `BuiltinView` over it.
This mirrors `mz_cluster_replica_metrics` (a `mz_catalog`-adjacent curated view) over `mz_cluster_replica_metrics_history`.
It satisfies the PRD's requirement (stable `mz_catalog` schema, users build views on top) while keeping the introspection collections where every other introspection collection lives, and it lets us reshape the raw feed later without breaking the public relation.

New raw introspection collections (`BuiltinSource`, `schema: MZ_INTERNAL_SCHEMA`):

* `mz_internal.mz_object_hydration_history_raw` — `IntrospectionType::ObjectHydrationHistory`
* `mz_internal.mz_replica_hydration_history_raw` — `IntrospectionType::ReplicaHydrationHistory`

New stable relations (`schema: MZ_CATALOG_SCHEMA`):

* `mz_catalog.mz_object_hydration_history` — `BuiltinView` over the raw object collection. The `cluster_id` is written into the raw row at append time (not derived by joining `mz_cluster_replicas` at read time), so history survives replica drops and resizes. A resize replaces the `replica_id`, and an INNER join to `mz_cluster_replicas` would erase every prior row for the gone replica while the raw collection still retains it for the full window, directly breaking Success Criterion 1. If any lookup against `mz_clusters` / `mz_cluster_replicas` is used for a human-readable name, it must be a LEFT join so a dropped replica yields NULL name, not a dropped row.
* `mz_catalog.mz_replica_hydration_history` — `BuiltinView` over the raw replica collection.
* `mz_catalog.mz_hydration_progress` — `BuiltinView`, no new raw collection (rolls up existing data).

The column shapes match the PRD:

```
mz_catalog.mz_object_hydration_history
------------------------------------------------------------------
object_id     text          -- index or materialized view
cluster_id    text
replica_id    text
started_at    timestamptz   -- dataflow installed on the replica
finished_at   timestamptz   -- hydrated; NULL if it never finished
status        text          -- 'hydrated' | 'failed' | 'canceled'

mz_catalog.mz_replica_hydration_history
------------------------------------------------------------------
replica_id            text
cluster_id            text
started_at            timestamptz
finished_at           timestamptz   -- all objects hydrated; NULL if never
object_count          uint8         -- objects installed at episode start
peak_memory_bytes     uint8
peak_disk_bytes       uint8
peak_cpu_nano_cores   uint8
status                text          -- 'hydrated' | 'oom-killed' | 'canceled'

mz_catalog.mz_hydration_progress   -- per object per replica
------------------------------------------------------------------
object_id             text
replica_id            text
nodes_hydrated        uint8
nodes_total           uint8
hydrated              boolean
last_hydration_time   interval      -- from history; the honest "ETA"
```

Cluster-level answers come from `GROUP BY cluster_id`. Slowest-replica rollups from `GROUP BY object_id` over replicas.

### Where the data comes from

There are two distinct producers, corresponding to the two histories.

#### Per-object history (`ObjectHydrationHistory`)

An object hydration episode on a replica has three edges: installed, hydrated, and terminated-before-hydrated.
All three are observable in the compute controller, which already tracks per-replica per-collection state and detects the hydration transition.

* **started_at** — when the dataflow is installed on the replica. The compute controller knows this moment (`Instance` collection state, `src/compute-client/src/controller/instance.rs`), and clusterd records the install instant as `ExportState.created_at` (`src/compute/src/logging/compute.rs:733`).
* **finished_at, status='hydrated'** — the not-hydrated → hydrated transition is detected in clusterd (`src/compute/src/compute_state.rs:1964`, `set_reported_output_frontier` → `set_hydrated`), surfaced to the coordinator through the existing `ComputeHydrationTimes` introspection subscribe (`src/adapter/src/coord/introspection.rs:599`), and observed at `handle_introspection_subscribe_batch` (`src/adapter/src/coord/introspection.rs:437`), where the coordinator has wall-clock access.
* **status='canceled'** — the collection is dropped on the replica before it hydrated (object dropped, or `DROP CLUSTER REPLICA`).
* **status='failed'** — the replica disconnects or crashes before the object hydrated (OOM, node loss).

Today only a *duration* is captured (`time_ns = created_at.elapsed()`, `src/compute/src/logging/compute.rs:978`); no absolute timestamp is persisted, and there is no "started" or "canceled/failed" edge in the feed.
Two ways to get the absolute-timestamp, terminal-status episode:

**Recommended (measured at source):** extend the compute hydration logging to emit an episode record, not just a duration.
Emit `started_at` (absolute, using the replica's wall clock at `created_at`) and a terminal edge (`hydrated` / `canceled` / `failed`) into a new per-worker log variant, rolled up by a new introspection subscribe into `ObjectHydrationHistory`.
This keeps the source of truth in clusterd, where the only real edges exist, and models cancel/fail natively. It touches `src/compute-client/src/logging.rs`, `src/compute/src/logging/compute.rs`, `src/compute/src/compute_state.rs`, `src/compute/src/render.rs`, the introspection subscribe specs (`src/adapter/src/coord/introspection.rs:569`), and the per-worker builtin logs (`src/catalog/src/builtin/mz_introspection.rs`).

**Alternative (reconstructed at coordinator):** on observing the existing `ComputeHydrationTimes` completion edge, the coordinator appends a row with `finished_at = now()` and `started_at = finished_at - duration`. Cheaper (no compute-layer changes), but `finished_at` is skewed late by subscribe latency, `started_at` is derived rather than measured, and cancel/fail require new coordinator-side in-flight tracking. Weaker on all three; recommended only if the compute-layer change proves too costly for V1.

The write itself, in either case, goes through `StorageController::append_introspection_updates(IntrospectionType::ObjectHydrationHistory, ...)`, identical to how replica status history is written from the coordinator (`src/adapter/src/coord/message_handler.rs:1045`).

#### Per-replica episode + peaks (`ReplicaHydrationHistory`)

A replica hydration episode runs from replica start/reconnect (or a fully-hydrated → not-hydrated transition when new objects are installed) until every installed object on the replica is hydrated.
The combined `Controller` (`src/controller/src/lib.rs`) is the right owner because it sees both signals: replica resource metrics arrive at its choke point, and it can read compute hydration state.

Introduce a `ReplicaHydrationTracker` in `src/controller`:

* **Episode boundaries** from compute hydration state (`collections_hydrated_on_replicas`, `src/compute-client/src/controller/instance.rs:744`; `collection_hydrated`, `:719`). Start = replica has ≥1 unhydrated installed object and no active episode. Finish = all installed objects hydrated. **Boundary detection must not be evaluated only on the 60s metrics tick.** Doing so would give `started_at`/`finished_at` ~60s jitter and would couple episode correctness to `fetch_service_metrics`, whose failures are swallowed with a `warn!` (`src/controller/src/clusters.rs:952`) and skip the tick entirely. Instead evaluate boundaries where hydration state actually changes: the compute controller already recomputes hydration on frontier progress, so the tracker subscribes to that transition (an explicit hook alongside where `FrontierUpper` responses are handled) rather than polling. The metrics tick is used only for peak accumulation, never for boundary timing. On a metrics-fetch failure, the peak accumulator simply misses that sample; episode boundaries are unaffected because they do not depend on the metrics path.
* **object_count** = number of objects installed on the replica at episode start. Objects installed *after* the episode start (e.g. a `CREATE INDEX` mid-episode) are counted into the current episode and extend its finish condition, so `object_count` is the count at finish, not at start. This keeps `object_count` consistent with the finish predicate "all installed objects hydrated". Recorded once, at flush.
* **Peak accumulation** on every metrics sample. `process_replica_metrics` (`src/controller/src/lib.rs:570`) is the single point through which every 60s sample for every replica passes. While a replica has an active episode, fold the sample into a per-replica running max.
* **Flush** on episode finish or failure: append one row to `ReplicaHydrationHistory` with the peaks, `object_count`, `started_at`, `finished_at`, and `status`, then reset the accumulator.
* **status** — `hydrated` on normal completion; `oom-killed` if the replica's terminal status (from the cluster event / `mz_cluster_replica_status_history` reason) indicates OOM while the episode was active; `canceled` if the replica was dropped by the user mid-episode. `finished_at` is NULL for a row that never completed.

##### Capturing true peaks

The 60s sampled running-max still misses a sub-interval spike, which is the exact failure mode the PRD calls out.
Use the kernel's high-water mark instead of sampling for memory and disk:

* clusterd already serves a usage endpoint (`ClusterdUsage`: `memory_bytes`, `swap_bytes`, `disk_bytes`, `heap_limit`) consumed by the Kubernetes orchestrator (`src/orchestrator-kubernetes/src/lib.rs:1508`). Extend it to expose the cgroup high-water mark (`memory.peak` on cgroup v2, `memory.max_usage_in_bytes` on v1). This is a kernel-tracked maximum since the container (or since reset), so it captures the true peak regardless of sampling cadence, and it is still valid at the moment of an OOM.
* `peak_memory_bytes` and `peak_disk_bytes` then read from this high-water value at episode finish, not from a running max of samples.
* `peak_cpu_nano_cores` has no kernel high-water analog (CPU is a rate), so it stays a running max over samples. Documented as coarser than the memory/disk peaks.
* Where the endpoint cannot supply a high-water mark (the local process orchestrator, `src/orchestrator-process/src/lib.rs:558`, returns `None` for disk/heap), fall back to the controller-side running max over samples. As a defense-in-depth mitigation, make the metrics interval a dyncfg (currently `const METRICS_INTERVAL = 60s`, `src/controller/src/clusters.rs:936`); the metrics loop is one task per replica (`clusters.rs:931`), so shorten that individual replica's interval (e.g. to 5s) while it has an active episode, and restore 60s on finish. This is per-replica, not a global poll change, so idle replicas keep the cheap cadence.

This is the PRD's "option 2 (running max)" strengthened with the kernel high-water mark so the recommendation actually delivers a true peak rather than a tighter-but-still-sampled one.

#### Progress view (`mz_hydration_progress`)

Pure `BuiltinView`, no new collection.
Roll up `mz_internal.mz_compute_operator_hydration_statuses` (per-LIR-node boolean, `src/catalog/src/builtin/mz_internal.rs:4830`) into `nodes_hydrated` / `nodes_total` per `(object_id, replica_id)`, join `hydrated` from `mz_hydration_statuses`, and `last_hydration_time` from `mz_catalog.mz_object_hydration_history`. `last_hydration_time` is the duration (`finished_at - started_at`) of the row with the greatest `finished_at` for that object, restricted to rows where `finished_at IS NOT NULL` (i.e. the last *completed* hydration, not the maximum duration ever seen). Use `DISTINCT ON (object_id) ... ORDER BY finished_at DESC`.
Node-fraction is not time-linear; the docs must say so, the way RisingWave documents its percentage as "just an estimate". We expose the fraction, never a time-remaining.

### Builtin-relation mechanics

Adding these relations is mechanically routine and needs no catalog-version bump or migration entry.
A net-new builtin has no stored fingerprint, so the startup fingerprint-diff (`add_new_remove_old_builtin_items_migration`, `src/adapter/src/catalog/open.rs:747`) adopts it as a new builtin collection automatically, which is safe for existing and 0dt deployments.
The `MIGRATIONS` list (`src/adapter/src/catalog/open/builtin_schema_migration.rs:84`) is only for changing the schema of an *existing* durable builtin.

Files touched per relation:

1. `src/pgrepr-consts/src/oid.rs` — append the next monotonic OID (currently ~17107) for each relation.
2. `src/storage-client/src/healthcheck.rs` — add the physical `RelationDesc` for each new raw introspection collection, alongside `REPLICA_STATUS_HISTORY_DESC` (`:148`) and `REPLICA_METRICS_HISTORY_DESC` (`:164`).
3. `IntrospectionType` enum (`src/storage-client/src/controller.rs:76`) — add `ObjectHydrationHistory` and `ReplicaHydrationHistory`.
4. `src/storage-controller/src/lib.rs` — register the two as append-only (`impl From<&IntrospectionType> for CollectionManagerKind`, `:3811`); add `TimeWindow` retention descriptors that read the new dyncfgs via `config_set` (the `partially_truncate_metrics_history` style, not the `&parameters`/`replica_status_history_desc` style).
5. `src/storage-controller/src/collection_mgmt.rs` — add the two `IntrospectionType` arms to the `prepare()` truncation dispatch (`:1134`), routing to the metrics-history-style truncation (`partially_truncate_metrics_history`, `:1145`).
6. `src/storage-types/src/dyncfgs.rs` — add `Config<Duration>` dyncfgs `object_hydration_history_retention_interval` and `replica_hydration_history_retention_interval`, default 30 days (mirror `REPLICA_METRICS_HISTORY_RETENTION_INTERVAL`, `:61`). No `StorageParameters` field and no `update_storage_config` change.
7. `src/catalog/src/builtin/mz_internal.rs` — define the two raw `BuiltinSource`s.
8. `src/catalog/src/builtin/mz_catalog.rs` — define the three `mz_catalog` `BuiltinView`s.
9. `src/catalog/src/builtin.rs` — register all in `BUILTINS_STATIC` / `builtin_items` (respect dependency ordering: views after the sources they read), and add indexes if needed.

Column types (`RelationDesc::builder().with_column(...)`): `text` → `SqlScalarType::String`, `uint8` → `SqlScalarType::UInt64`, `timestamptz` → `SqlScalarType::TimestampTz { precision: None }`, `interval` → `SqlScalarType::Interval`, `boolean` → `SqlScalarType::Bool`, each `.nullable(...)` per the schema (`finished_at` nullable).

### Expose to agents and docs

Once the data is in the catalog, the developer MCP server surfaces it for free.
Add a docs guide, exposed as an agent skill, covering: what hydration is, how to read the new tables and metrics, the ~2x rehydration rule (rehydration takes ~2x the time and ~2x the peak memory of steady state), how to use autoscaling, and how to handle slow-hydrating workloads (swap-heavy, wide rows, `EXCLUDE COLUMNS`).

## Minimal Viable Prototype

Ship the read path end-to-end on the existing signal, deferring peaks and terminal-status modeling:

1. Define the two raw introspection collections, the three `mz_catalog` relations, and the retention dyncfgs.
2. Wire the per-object history using the **reconstructed-at-coordinator** path (append on the existing `ComputeHydrationTimes` completion edge with `finished_at = now()`, `started_at = finished_at - duration`, `status = 'hydrated'` only).
3. Populate `mz_hydration_progress` from the existing per-LIR-node collection.
4. Leave `mz_replica_hydration_history` peaks unpopulated (rows written with NULL peaks) and terminal statuses (`failed`, `canceled`, `oom-killed`) unmodeled.

This validates the PRD's motivating query shape and the durability-across-restart property with the smallest change, and de-risks the catalog surface before investing in the compute-layer episode change and the cgroup high-water-mark endpoint.
Demo: create an index on a cluster, restart the replica, and confirm the prior hydration row still returns.

## Decomposition (stacked PRs)

Landable in order; each builds on the prior, matching the squash-merge stacked-PR convention.

1. **Catalog surface + retention.** The 9-file builtin checklist above plus the two dyncfgs. Relations exist, return zero rows, pruning wired. Pure SQL/catalog, no producer yet. Independently landable and testable.
2. **Per-object history, coordinator-only (MVP producer).** Append `status='hydrated'` rows on the existing `ComputeHydrationTimes` completion edge (`src/adapter/src/coord/introspection.rs:437`). Delivers the PRD motivating query minus terminal statuses and peaks. SQL-owned.
3. **`ReplicaHydrationTracker` + sampled peaks.** New component in `src/controller`, boundary hook on hydration-state change, running-max accumulator at `process_replica_metrics`, flush on finish. Peaks from the sampled fallback only. SQL/adapter-owned, no clusterd change.
4. **cgroup high-water endpoint.** Extend `ClusterdUsage` + the Kubernetes orchestrator; swap `peak_memory_bytes`/`peak_disk_bytes` to the kernel value; resolve the reset question (Open Question 3). **Compute/Cloud-owned; blocked on that team's sign-off.** Unit 3 ships peaks NULL-or-sampled until this lands.
5. **Measured-at-source per-object episodes** (only if Open Question 1 resolves this way). Compute-layer log variant + absolute timestamps + `canceled`/`failed` edges; replaces unit 2's reconstruction and unlocks Success Criterion 3. **Compute-owned.**
6. **Docs guide + agent skill.**

Two collision hazards for parallel implementers: units 2 and 3 both derive from the `ComputeHydrationTimes` signal through different paths (subscribe-batch vs hydration-state hook) and must agree on timing semantics; if they are built in isolation, pin the shared hook first. Unit 5 supersedes unit 2, so do not hand both out concurrently.

## Testing

Each stacked unit lands with its own acceptance check, not one demo at the end:

* **Catalog surface** (unit 1). Regenerate `test/sqllogictest/autogenerated/mz_catalog.slt`, `mz_internal.slt`, `test/sqllogictest/oid.slt`, `test/sqllogictest/information_schema_tables.slt`, and `test/workload-replay/system_catalog_identifiers.txt`. A testdrive file asserts the three relations exist with the expected columns and types, and that `SELECT` returns zero rows on a fresh cluster.
* **Per-object history**. A testdrive that creates an index, waits for hydration, and asserts one `status='hydrated'` row with a plausible `started_at < finished_at`. For the measured-at-source path, add drop-before-hydrate and replica-kill-before-hydrate cases asserting `canceled` / `failed` rows.
* **Durability across restart** (the flagship property). A platform-check (`misc/python/materialize/checks/`) is the right harness: hydrate, restart `environmentd` and the replica, and assert the prior rows still return. Restart-survival is exactly what platform-checks exist for.
* **Peaks**. Unit tests for the running-max accumulator and boundary logic in `src/controller`. End-to-end peak values are only assertable where the orchestrator supplies a high-water mark, so a k8s-backed test (not the local process orchestrator, which returns `None`) is required for the cgroup path; otherwise assert the sampled-fallback path.
* **Retention**. A test that sets the retention dyncfg low and asserts old rows are pruned at the next `prepare()`.

## Alternatives

**Coordinator-written `BuiltinTable`s via the implications framework.**
Rejected. History rows are introspection data, not catalog items, and the append-only introspection collection machinery already provides durability, append semantics, and time-window retention. Routing high-frequency history writes through catalog transactions would be a large amount of new machinery for no benefit and would abuse a framework built for DDL.

**Keep everything in `mz_internal`.**
Rejected against the PRD requirement for a stable surface. Customers need to build dashboards and views on top without breakage; `mz_internal` carries no stability commitment.

**Put the raw introspection collections directly in `mz_catalog`.**
Rejected in favor of `mz_internal` raw + `mz_catalog` view. No introspection collection lives in `mz_catalog` today, and a curated view lets us reshape the raw feed later without breaking the public relation. It also matches the existing `mz_cluster_replica_metrics` / `_history` split.

**Sampled peaks (raise sampling frequency only).**
Rejected as the primary mechanism. Even at a higher fixed rate, a sampled max can miss a sharp spike between samples, which reproduces the OOM-surprise. Higher sampling is kept only as a fallback tightener where the kernel high-water mark is unavailable.

**Predictive ETA.**
Rejected (see Out of Scope). Only Redis exposes an ETA, and only because its load is provably linear in bytes. Our hydration is not linear; a wrong ETA destroys trust. We follow Lambda/Cloud Run: report the last observed duration and let the user reason.

## Open questions

1. **Per-object edge: source vs coordinator (gating, needs an author/product decision before per-object work is scoped).** The recommended measured-at-source approach requires a compute-layer change (new log variant + absolute timestamps + terminal edges) owned by the Compute team, not SQL. Is that in scope for V1, or does V1 ship the reconstructed-at-coordinator MVP and defer measured episodes? The choice determines whether Success Criterion 3 (failed/canceled rows) is reachable in V1: the coordinator-only path does not produce terminal-status rows without new in-flight tracking. It also determines whether the per-object unit is a SQL-only change or a cross-crate Compute change. Cross-team pieces (compute-layer episode logging, the clusterd cgroup high-water endpoint, orchestrator changes) need a named owning-team handoff (a tracking issue per crate, Compute sign-off) before the corresponding stacked PR is handed to an implementing agent.
2. **Episode state loss on `environmentd` restart.** The `ReplicaHydrationTracker` accumulator is in-memory in the controller. If `environmentd` restarts mid-episode, the in-flight episode's peak accumulation is lost and no row is emitted for that episode (or a fresh episode starts on the new leader). Is "NULL finished_at, no peak" acceptable for an episode interrupted this way, or must the accumulator be checkpointed? This is the "no local-only assumptions" tension from the adapter guide: the tracker is local-only state, tolerable only because a single read-write leader owns it at a time.
3. **cgroup high-water reset semantics.** `memory.peak` (v2) is since-container-start unless reset. If a replica is not restarted between two hydration episodes (e.g. `CREATE INDEX` on a running cluster), the high-water mark spans both episodes and over-reports the second. Do we reset the cgroup counter at episode start (write to `memory.peak` on v2 / `memory.max_usage_in_bytes` on v1), or accept that the peak is a since-replica-start high-water and document it? Resetting is cleaner but requires a writable cgroup path from clusterd.
4. **Sources and sinks.** The PRD scopes objects to indexes and MVs. Source rehydration latency already exists (`mz_source_statistics.rehydration_latency`). Do we unify storage-object hydration into the same history relations later, and does that change the column set now?
5. **`CREATE INDEX` on a running, fully-hydrated cluster.** Confirmed model: a new per-object row starts; a new replica episode starts only if the replica transitions from fully-hydrated to not-hydrated. Verify this matches the `ReplicaHydrationTracker` boundary definition under concurrent object creation.
6. **Read-only mode.** Resolved by inspection: `record_replica_metrics` already skips read-only mode (`src/controller/src/lib.rs:580`), and the replica-status-history write already gates on `read_only()` (`src/adapter/src/coord/message_handler.rs:1032`). The new object-history and replica-history write paths must apply the same guard, so the read-only generation during a 0dt upgrade writes no rows. This is the intended V1 behavior (Prometheus covers the read-only generation per Future Requirements), not an open item; listed here only as an implementation must-do.
7. **Retention: time-window vs count.** Replica *status* history uses `TimeWindow`, but source/sink status history uses `LastN`. 30-day `TimeWindow` matches the PRD; confirm a busy cluster with many short episodes does not blow the shard size within the window, else a hybrid cap is needed.
