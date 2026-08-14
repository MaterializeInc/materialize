# Cluster Branches

- Associated:
  - [PRD](https://app.notion.com/p/materialize/Help-Claude-Optimize-my-dataflows-Part-2-Branches-3b913f48d37b80a0bfccd8af3fe83c42) "Help Claude Optimize my dataflows: Part 2, Branches" (Pranshu)

## The Problem

Agents needs mechanisms to propose changes to Materialize e.g., add an index, rewrite a view, resize a cluster. Whether such a change is beneficial must be verified against an environment. Presently, the only options are to mutate production, which is unsafe, or to hand-build a parallel cluster and copy its inputs, which is slow and manual. As a result an agent proposes changes it has not tested.

The changes an agent proposes are compute changes: will this index fit in memory, will this rewrite keep up with ingest, does this cluster still fit at a smaller size. Answering that means running the change on real data and load, on compute isolated from production and sized the way you're testing. In Materialize the cluster is the unit of compute isolation, so the sandbox we want is a forked cluster: isolated, right-sized compute to run and measure the change against production's live data, without touching production's own compute.

The agent forks a cluster, changes it, measures against production, and hands the diff back as a PR.

## Success Criteria

- **Create latency.** Branching a cluster completes in under a second, independent of the data volume on that cluster.
- **Isolation.** A change made in a branch (a new, altered, or dropped index/MV/view, or table DML and schema change) leaves production and every other branch unchanged in both query results and catalog.
- **Liveness.** A re-rendered object in a branch advances against the live source frontier, so its measured lag reflects real ingest rather than a frozen snapshot.
- **Observability.** An agent can measure a branch object's memory, CPU utilization and its freshness. We can observe whether it is hydrated, through existing introspection. Those figures are representative of what the same object would cost in production.
- **No production impact.** Live branches do not measurably change production read latency or compaction over the shards they share.
- **Change retrieval.** A branch's changes can be read back as runnable DDL, so a human can apply them to production themselves.
- **Privacy.** A role cannot observe or use a branch it did not create.

## Out of Scope

- **Promotion / merge back into main.** exit is a PR with a human in the loop, not an in-database `PROMOTE`.
- **Branch off a branch.** branches only off `main`.
- **Cross-branch comparison in a single query.** No inline `object@branch` qualifier.
- **Mid-branch rebase.** No moving a branch's point-in-time forward after create.
- **Branch-local source re-ingestion.** A source is read-only in a branch. The intended way to add columns is `CREATE TABLE FROM SOURCE`, standing up a branch-local re-read of the external upstream, isolated from production.

Promotion and branch-off-a-branch are deferred but must not be precluded. How the design leaves room for each is called out in the Solution below.

## Usage

The feature sits behind one flag, `enable_branching`.

**1. Preflight.** `EXPLAIN CREATE BRANCH` reports the branch's scope and what it reads, creating nothing.

```
EXPLAIN CREATE BRANCH FROM CLUSTER fraud_features;

Branched (cluster fraud_features): 5 objects
  1 materialized view, 4 indexes
Forkable inputs (forked at branch point): 1
  materialize.fraud.labels           (table, writable)
Shared live inputs (read-only): 12
  materialize.fraud.transactions     (source)
  materialize.fraud.enriched_events  (table, read-only, fed by source)
  materialize.fraud.user_profiles    (materialized view, other cluster)
  9 views
Warnings: 1
  Table materialize.fraud.labels is forked, but materialize.fraud.user_profiles
  (materialized view on another cluster, not branched) also reads it and will not
  see the branch's writes. Branch that cluster too for a consistent view.
```

The warning covers a table that is forked here but also read through a cluster that is not in this branch. That path keeps reading production, so it never sees the branch's writes. Branching that cluster too forks the shared table once, and every branched cluster reads the one fork consistently.

**2. Create.** You create and size the branch cluster, then map each production cluster to one. Multi-cluster branches list one `prod -> branch` map per line.

```sql
CREATE CLUSTER exp_fraud (SIZE = '6400cc');

CREATE BRANCH exp_delta_joins
    FROM CLUSTER fraud_features IN CLUSTER exp_fraud
    WITH (EXPIRES IN = '48h');
```

`SHOW BRANCHES` lists your branches, owner-scoped.

```
SHOW BRANCHES;

 name            | created          | expires          | clusters
-----------------+------------------+------------------+-----------------------------
 exp_delta_joins | 2026-08-11 09:14 | 2026-08-13 09:14 | fraud_features -> exp_fraud
```

**3. Enter and iterate.** `SET branch` reroutes this session's name resolution into the branch. `RESET branch` returns to production.

```sql
SET branch = exp_delta_joins;

CREATE MATERIALIZED VIEW materialize.fraud.txn_features_v2 AS ...;
CREATE INDEX object_type_by_id ON materialize.fraud.object_type (id);
```

**4. Measure.** Existing introspection resolves within the branch: `mz_cluster_replica_utilization` for memory and CPU fit, `mz_materialization_lag` for freshness against live ingest. A branch gives a warm, live, isolated place to answer "does the rewrite fit on this size" and "does it keep up".

**5. Extract.** `SHOW BRANCH CHANGES` diffs the branch against its branch point. `AS SQL` re-serializes each change into runnable DDL for a PR.

```
SHOW BRANCH CHANGES exp_delta_joins;

 object                              | change  | detail
-------------------------------------+---------+--------------------------
 materialize.fraud.txn_features_v2   | added   | materialized view
 materialize.fraud.object_type_by_id | added   | index
 materialize.fraud.feature_rollup    | altered | redefined, added risk_tier
```

**6. Tear down.** `DROP BRANCH exp_delta_joins`, or let `EXPIRES` lapse. Creation is gated by `GRANT CREATE ON BRANCH TO <role>`, and a role sees and uses only its own branches.

A branch covers the following categories of object. Only the first is *in* the branched cluster; how others are treated is based on what those objects depend on.

| Object kind | Category | In the branch |
|---|---|---|
| **Indexes, MVs** | Cluster-resident | Branch-owned. Add, drop, or redefine freely; each gets its own branch identity. |
| **Sinks** | Cluster-resident | Inert by default, so a branch never writes to production's destination. `ALTER` a sink to a new destination (a different one is required) and it starts writing there. |
| **Tables** | Forkable input | Not on any cluster. A writable table reachable from the branched cluster's dataflows is forked at the branch point, so DML and schema changes are real and isolated to the branch. A read-only table (one fed by a source) is not forked; it is a shared live input, read live like a source. |
| **Sources** | Shared live input | Live and read-only. Data keeps flowing, but you cannot alter them. |
| **Views** | Shared live input | Read-only. A view is just SQL folded into a dependent dataflow; to change one, create a new view inside the branch. |

## Solution Proposal

A branch forks one or more clusters. The design principle: a branch **runs the branched cluster's own objects, reads everything outside the cluster live, and forks only the writable tables it reaches.** It reads on production's timeline, starting at the branch point and tracking forward with the live frontier, so its objects stay as fresh as production's rather than frozen at a snapshot. The branch cluster re-renders the branched cluster's indexes and materialized views, so their memory and CPU are real and measurable at the branch's chosen size. Inputs from outside the branched cluster, sources and objects on other clusters, are read from the same live shards that serve production, never recomputed. The only storage a branch adds is the new data it writes to a forked table, and nothing unchanged is duplicated. `CREATE BRANCH` records catalog metadata and nothing else, so it scales with object count, not data size. The cost a branch does incur is re-rendering the cluster's in-memory compute state on the branch replicas.

### Forking a written table

A table the branch writes needs its data isolated as production must not see the branch's writes, and the branch must not see production's later writes. The branch gets this by **forking the table's shard at the branch point**. The fork is a new shard that references production's history as of the branch point by pointer: the blobs holding data at or below `branch_ts` stay in production's shard, shared and immutable, and the fork points at them. The branch's writes append above `branch_ts` as new, self-owned data, while production's later writes append only to production's shard. From the branch point the two histories diverge, and neither sees the other's.

No data is copied, at create or on write. Because the shard is an append-only log of diffs an update or delete of an inherited row is a new compensating diff appended above `branch_ts`, leaving the shared blobs untouched. The fork is a metadata operation, so it is independent of data size. Forking a persist shard, results in **its own compaction frontier** (`since`) so production's table compacts independently. The fork holds a retained reference to the shared blobs it inherited, so production's garbage collection keeps exactly those instead of reclaiming them while a branch still points at them.

The detailed persist design (blob addressing, the cutoff, compaction boundaries, retention through the existing GC path, and txns registration) is in [Forking a table shard in persist](./20260814_cluster_branches_persist_fork.md).

### Reads without taxing production

A branch touches production's storage only to read its out-of-cluster inputs: the sources and objects on other clusters its dataflows depend on. A reader of a persist shard holds a read hold on it, pinning the shard's `since` no further back than that reader still needs.

A branch reads in two phases. While an object hydrates, it reads its inputs as of `branch_ts`, so it holds those input shards' `since` at `branch_ts` until it catches up, a hold proportional to hydration time. Once caught up it follows the live frontier, and its read holds advance forward with it, sitting near live like production's own consumers.  So a branch never pins `since` back long term.

Note: an alternative design (which is in the alternative section) is freezing the inputs at the branch point, which holds every input at `branch_ts` for the branch's whole life. However, because a branch tracks forward instead, production's compaction and read latency are unaffected.

### Hydration

Every object on the branched cluster, index or materialized view, re-renders on the branch replicas, so its arrangements must be warmed from persist. The cost is not paid at create. It is deferred and is proportional to data size.

At a high level two rejected options are:
- **Lazy hydrate from persist.** The branch object installs on first use and warms via the normal path. Correct, zero new mechanism, but the first query pays full hydration.
- **Shared-cluster shortcut.** Home the branch's dataflows on the production cluster itself. An unchanged object then shares the arrangements already resident on each replica through an ordinary trace import, rather than rebuilding them, so there is no hydration, at the cost of sharing production's compute and memory. This is the documented cheap path when you accept shared compute.

Instead, this design proposes **arrangement checkpointing**: capture every arrangement in a dataflow as of the branch point and restore them onto the branch replicas, so the branch resumes a dataflow instead of rebuilding one. It is scoped as a **general primitive**, since the same mechanism gives fast replica bring-up, fast restart, and fast blue/green cutover, with branches as its first consumer. Checkpointing is applied only where it pays: a dataflow with joins or aggregates, whose in-memory state is far smaller than its inputs, not a plain index on a table, where the fork already shares production's blobs and a checkpoint would only make the branch ready later. `SHOW BRANCH` reports each object's hydration state, so an agent knows when the branch is warm enough to measure.

The detailed compute design (capturing the whole dataflow graph, non-blocking capture, restoring traces with no upstream differential change, and the cost model for when it pays) is in [Hydration](./20260814_cluster_branches_hydration.md).

### What a branch can answer, and when

The ability for a branch to give feedback to an agent is hierarchical, and each level need increasing hydration. Therefore, an agent gets the cheap answers first and pays data-proportional time only for the last one.

- **Plan and validity (no hydration).** Does it plan, and does the optimizer use the new index or accept the rewrite. Available at create.
- **Result equivalence (hydrate to serve).** Does the rewrite return the same answers, checked as of a bounded time. Needs the arrangement warm enough to serve, not caught up to live.
- **Keep-up and fit (hydrate to live).** Does it keep up with live ingest, and its steady-state memory and CPU. The only level proportional to data size, and what checkpointing accelerates.

### Sinks

A branch re-renders the branched cluster's sinks like its other objects, but a sink is **inert** by default: the branch runs the sink's dataflow, so its compute is still measurable, but starts no external writer, so nothing reaches production's destination. This is the hard invariant, a branch never writes to a production sink's destination.

To exercise a sink for real, `ALTER` it to a new destination, which must differ from production's, and only then does the branch start a writer against the new destination. An `ALTER` that targets the production sink's own destination is rejected, so the safe default cannot be bypassed by accident.

### Catalog identity and resolution

Each branch object has its own catalog identity, so a branch MV, index, or table is a real catalog item. `CREATE BRANCH` writes the branch header, the per-object identities, and a snapshot of the branch-point catalog, all in one atomic catalog transaction. That snapshot is what `SHOW BRANCH CHANGES` later diffs against. These two properties, decoupled per-object identity and a single atomic transaction over many objects, are what a future promote would need to swap a branch's objects into production in one step, so keeping them here leaves promotion addable without a redesign.

Entering a branch is a session overlay, modeled on the session-scoped temporary-item overlay that already shadows the global catalog at the catalog resolver. An object in the branch's scope, a cluster-resident object on the branched cluster or one the branch itself created, resolves to its branch item. A source resolves to the live production source, and everything else falls through to production.

All DDL on branch-owned objects is unrestricted: create, alter, or drop them freely. A newly created object lands on the branch cluster in its production schema and shadows any production object of the same name within the branch session. Shared live inputs stay read-only, as the object table above notes, so to change one you add a new object rather than altering it.

### Tracking changes

`SHOW BRANCH CHANGES` is a catalog diff, not a data scan. It compares the branch's current catalog against the branch-point snapshot: an object present in the branch but not the snapshot is *added*, one in the snapshot but not the branch is *dropped*, and one in both with a changed definition is *altered*. Because every catalog item stores its canonical `create_sql`, `AS SQL` reconstructs runnable DDL by re-serializing each changed object's definition. A schema change to a branched table, an `ALTER TABLE`, is a catalog change too, so it appears here.

Row-level writes to a branched table are data, not catalog, so they are not in this diff. They are exactly what the table's fork appended above the branch point, so they can be read back as a consolidated diff over that range if we later choose to emit them as DML.

### Timestamps

**The branch point.** `branch_ts` is the single logical time that the whole branch is anchored to: table fork cuts off there, checkpoints are captured as of it, and live inputs begin there before tracking forward. It must be a time every input can serve.

The timestamp oracle selects one `branch_ts` in the valid read interval over all the branch's inputs at once, `max(since) <= branch_ts < min(upper)` across every forkable and shared-live input.

Note that a healthy input put `branch_ts` near live, while lagging input pin `branch_ts` back to its `upper`, so the branch starts as fresh as its slowest input. This matches the semantics of a multi-input query. If the interval is empty, meaning an input has compacted past another's frontier, create fails like an unservable read.

Furthermore becuase `branch_ts >= since` at selection and the branch takes its read holds atomically at create, no input's `since` can have passed `branch_ts`, so the fork can always pin the inherited blobs there. In the future, a multi-cluster branch selects one `branch_ts` over the union of every mapped cluster's inputs.

Consequently, a branch reads on production's timeline. Live inputs read at the live frontier. A branched table advances on the branch's writes and on normal table progress, so it never stalls downstream reads. Branch-cluster objects advance at their dataflow frontier fed by those inputs. Every read uses the normal timestamp oracle.

### Lifecycle

`WITH (EXPIRES IN = '48h')` computes an absolute expiry at create. A background sweep drops expired branches through the **same teardown as `DROP BRANCH`**: release the branch's read holds so production compaction can advance again, release any table-fork storage, tombstone the catalog rows, and tear down the branch's dataflows. The user's branch cluster is never auto-dropped, since they own its cost.

Expiry is the default so abandoned agent branches self-clean, while `DROP` is the deterministic reclaim.

Across an `environmentd` restart or generation cutover a branch behaves like any other catalog object: its durable state is the catalog rows and the persist fork's shard and retained reference, and its read holds and dataflows are reconciled from the catalog on bootstrap like every other object's. That retained reference, not the runtime read hold, keeps the inherited blobs alive in the meantime, so no branch-specific recovery path is needed.

### RBAC

Branches are owner-private. `GRANT CREATE ON BRANCH TO <role>` gates creation. A role sees and uses only branches it created, and cannot detect that another role's branch exists, so `SHOW BRANCHES` and every branch reference are owner-scoped with no information leak.

Superusers can see and drop any branch for operational cleanup.

## Minimal Viable Prototype

Single-cluster branch, behind `enable_branching`, hydrating lazily (no checkpointing yet)

1. `EXPLAIN CREATE BRANCH FROM CLUSTER fraud_features` reports scope and upstream inputs.
2. `CREATE CLUSTER exp_fraud` then `CREATE BRANCH ... IN CLUSTER exp_fraud`, sub-second.
3. `SET branch`, then `CREATE INDEX` / `CREATE MATERIALIZED VIEW`, and an `INSERT` into a branched table, inside it.
4. Introspection over the branch cluster's replica shows utilization and lag against live ingest.
5. `SHOW BRANCH CHANGES ... AS SQL` re-serializes the change to runnable DDL.
6. `DROP BRANCH` tears it all down.

This validates the UX, live-forward reads against a source, the table fork (the `INSERT` diverges from production), the resolution overlay, and the live-source freshness that makes the measurement meaningful. It does not require the checkpointing workstream.

## Alternatives

### Scope unit: cluster vs environment vs schema

- **Environment (branch everything).** Simplest model (Neon, Supabase), but it re-renders every cluster to test a change touching one, and needs a parallel control plane (an environment is one `environmentd`, so would require multi-envd support). Cluster scope reproduces it on demand by branching every cluster, without either cost.
- **Schema.** A schema's objects cut across clusters, so blast radius and compute isolation is lost (PlanetScale is schema-scoped).
- **Cluster (chosen).** Already Materialize's unit of isolation, sizing, and billing, so blast radius, fit, and cost stay explicit. Environment branching falls out by branching every cluster.

### Reading inputs: live vs frozen at the branch point

- **Freeze at the branch point.** Read a frozen fork of each input. Deterministic, but it pins cold storage per branch, stops the branch tracking live ingest, and defeats the freshness measurement.
- **Read live, track forward (chosen).** The branch is an ordinary extra reader whose read holds advance forward, so production's compaction is untaxed. Forking is reserved for the one thing needing isolated writable storage, a written table.

### Hydration: checkpoint vs peer-to-peer vs lazy-only

- **Peer-to-peer remote read.** The branch replica pulls an arrangement from a live production replica's memory. It couples bring-up to a running production replica and its worker layout, and needs a new cross-cluster transport.
- **Lazy-only.** Correct, but every unchanged index pays full hydration on first use, which undercuts the cheap-sandbox promise for large indexes.
- **Checkpoint to persist (chosen target).** Decoupled from production being up, reuses persist's blob storage, generalizes to replica bring-up and restart. Costs a persist round-trip and a re-exchange on differing worker counts.

### Exit: PR vs in-database promotion

In-database promotion needs merge queues, conflict handling, catch-up, and a rollback window. The scope for this work is `SHOW BRANCH CHANGES AS SQL` into a PR with a human in the loop. The design keeps promotion *addable* (decoupled per-object identity, atomic multi-object catalog transaction) without shaping around it.

## Open questions

1. **Freezing inputs for reproducibility.** Reads are live by default and track forward. Do we also want an opt-in mode to freeze a branch's inputs at the branch point for a deterministic experiment, or is live-only fine for v1? Live-only proposed.
2. **Promote of a source-bearing cluster.** A source's ingestion runs on its origin cluster, not the branch cluster, so a future promote of a cluster that owns a source would need to hand ingestion over. Out of scope now, recorded so the promote design accounts for it.
3. **Expiry and churn defaults.** Default `EXPIRES IN`, the per-owner branch quota, and the create/drop rate limit.
4. **Alterable sinks scope.** Sinks are inert by default and become writable only via `ALTER` to a new destination.
5. **Exporting table DML.** `SHOW BRANCH CHANGES` exports DDL. A branched table's row writes are recoverable from its fork, so should we also emit them as DML for the PR, or leave experimental data in the branch? DDL-only proposed for v1.

---

Detailed designs for the two core mechanisms: [Forking a table shard in persist](./20260814_cluster_branches_persist_fork.md) and [Hydration](./20260814_cluster_branches_hydration.md).
