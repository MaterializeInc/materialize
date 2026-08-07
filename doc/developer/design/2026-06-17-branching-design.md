# Schema branching

## The Problem

Materialize has no first-class way to fork the state of a schema. Agents and developers iterating on real data are stuck with three workarounds, all of them bad:

1. **Spin up a separate environment.** Slow, lacks the real data, and coordinating state across environments is the same problem we're trying to dodge.
2. **Copy tables by hand into a sandbox schema.** No consistency across tables, no MVs or indexes carried along, dependencies break.
3. **Edit in place against the live schema.** The current default, and the source of most "I broke prod" incidents.

The recurring pattern (fork the state, mess with it, throw it away) has no clean primitive. And workflows that depend on it (agent loops, full-app dev environments, DR rehearsal, cross-object refactors) need *catalog-wide* forking.

Schema-level branching is the smallest useful slice, while the larger user story is whole-database branching, which has the same shape applied N times with a coordinator.

What we're missing is a `git branch`-style primitive for database state: fork at a point in time, get a real namespace you can read, write, and ALTER, throw it away when done.

## Prior art

Neon's database branching ([neon.com/docs/introduction/branching](https://neon.com/docs/introduction/branching)) is the closest existing implementation of this pattern in a database product. Branches are first-class, created instantly from any LSN, share unchanged storage with the parent via copy-on-write, support sub-branches by the same mechanism, and there's no merge. We adopt the same product shape (instant branches, no merge, ephemeral, compose-by-reference) but build the implementation around our own primitives (persist batches over a shared blob store, not Postgres pages).

## Success Criteria

A solution succeeds if all of the following hold:

- A user can fork every object in a schema (or in a whole database) at a single, coordinated `branch_ts` without blocking writes on the source.
- Reads on a branched object return `source@branch_ts + branch-local writes`. Writes affect only the branch. Source reads are untouched.
- Minimal storage overhead. A branch with no writes adds no copy of the source's bytes; only the new shard's manifest (which references the source's existing blobs), the per-blob reference rows, and the branch's catalog entries cost anything. Cost grows with writes on the branch, not with the size of the source.
- Each branched object has its own catalog identity (`GlobalId`, `CatalogItemId`, `RelationDesc`). The branch is a real namespace, not a view.
- Source schemas keep working while branches exist. `ALTER TABLE`, `DROP TABLE`, `RENAME` on the source don't corrupt branch reads, because each branch holds its own snapshot of the schema and its own shard manifest pointing at the source's bytes.
- A branched schema can contain sources and sinks. By default they're inert in the branch (no ingestion, no emission). `ALTER` opts in to activating them.
- Sub-branches (`CREATE BRANCH … FROM BRANCH <branch>`) compose without new primitives. A parent branch cannot be dropped while it has live children; the user must drop the children first.
- Compute for branched MVs and indexes runs on branch-local clusters, isolated from source compute. `DROP BRANCH` tears the clusters down.
- Whole-database branching uses the same machinery: one coordinated `branch_ts`, all-or-nothing shard fork, full catalog fork with cross-object reference remap.
- Branch DDL is unrestricted. The branch is the user's, let them edit it.
- Drop is clean. `DROP BRANCH` decrements blob reference counts, drops branch-local catalog entries, and lets normal GC reclaim now-unreferenced source blobs.
- Lands behind a flag (`enable_branching`), off by default, so we can dogfood before recommending it.

## Out of Scope

True non-goals

- **`MERGE BRANCH`.** Permanent. The production path is agent → dbt / mz-deploy → CI/CD, not merge.
- **Selective cloning, cluster targeting** (`INCLUDE TABLES`, `IN CLUSTER`, `EXCLUDE MATERIALIZED VIEWS`). Orthogonal optimizations. May get added later as ergonomic sugar over the core primitives.

Items that *are* in the design but deferred from the first delivery slice are called out in the MVP section, not here.

## Solution Proposal

A branch is a catalog-decoupled fork of a schema's objects. Each branched object gets a brand-new persist shard whose initial manifest points at the source's existing blobs via absolute keys; subsequent writes on the branch land in new blobs under the new shard's own namespace. The source's bytes are protected from GC by ref-count rows in shared metadata. Reads on the new shard apply a per-batch timestamp cutoff so that source data committed after `branch_ts` (in straddling batches) and parent-branch data committed after a sub-branch's `branch_ts` are correctly hidden. The design has seven pieces:

### 1. Catalog: independent identity per branched object

A new catalog collection, `BranchDescriptor`, records per-schema branch state. Each entry carries per-object:

```
(fork_shard_id,
 branch_ts,
 source_catalog_id,
 branch_catalog_id, branch_global_id, relation_desc)
```

`fork_shard_id` is the new shard allocated for this branched object (piece (2)). `branch_ts` is the coordinated fork timestamp; the read path uses it as the cutoff for source-derived batches. `source_catalog_id` records the source object the branch was forked from, for `SHOW BRANCH STATUS` and bookkeeping only. The branch's read path never re-resolves through it.

The branch's catalog item is built from the descriptor's own `branch_catalog_id`, `branch_global_id`, and snapshotted `relation_desc`. It never re-resolves through the source's current catalog entry. That decoupling lets the source schema evolve without corrupting the branch. The same shape extends to whole-DB scope without re-doing the catalog work.

### 2. Storage: fork shard with absolute blob refs and per-batch cutoff

Persist stores data as immutable blobs in object storage. Each blob's full key includes the owning shard's UUID: `s<shard_uuid>/<writer_key>/<part_id>`. Once written, a blob never changes; only the shard's manifest (the list of `HollowBatch`es and the blob keys their parts live in) is mutated. The branching design exploits this: a new shard whose manifest points at an existing shard's blobs is a zero-byte copy of that shard's state.

For each branched object, at `CREATE BRANCH` time:

1. **Allocate a new shard id** `fork_shard_id`.
2. **Snapshot the source's `Trace`** at `branch_ts`: collect every `HollowBatch` from the source shard whose `lower <= branch_ts` (i.e. every batch that could contain data at or before `branch_ts`, including straddling batches whose `upper > branch_ts`).
3. **Rewrite each batch part's key to absolute form.** A persist `PartialBatchKey` normally stores a relative path (`<writer_key>/<part_id>`) that is resolved against the shard's own UUID at access time. We extend it with an `Absolute` variant that stores a full path (`s<source_shard>/<writer_key>/<part_id>`); `complete()` returns the full path as-is, ignoring the calling shard's UUID. This lets the fork shard's manifest reference the source shard's blobs directly.
4. **Stamp each inherited batch with `cutoff_ts = branch_ts`.** This is a new optional field on `HollowBatch`; absent on batches that originate as ordinary writes, present (and set to the fork's `branch_ts`) on every batch inherited at fork time.
5. **Write the new shard's initial state** to consensus: the rewritten hollow batches, `since = Antichain::from_elem(T::minimum())`, `upper = branch_ts + 1`. Use a low-level `Machine::initialize_from_snapshot` path that mirrors the existing fresh-shard bootstrap but takes pre-formed hollow batches as input.

After step (5) the fork shard is a normal shard from the controller's perspective. New writes via `WriteHandle` at `ts > branch_ts` append fresh blobs under the fork shard's own UUID prefix (`s<fork_shard>/...`), with `cutoff_ts` absent (these are not inherited; they are the branch's own data).

**Reads** on the fork shard go through standard `persist_source`, with one addition: when streaming a `HollowBatch` whose `cutoff_ts = T_c` is set, the read path drops every update with `time > T_c`. Batches without a `cutoff_ts` are read as today. This single filter is what handles both:

- **Straddling source batches.** A source batch with `lower = t_3` and `upper = t_8`, inherited into a fork at `branch_ts = t_5`, has `cutoff_ts = t_5`; the read path emits only the updates at `time ≤ t_5`.
- **Sub-branch writes inherited at sub-branch time.** A sub-branch forking a parent that has accumulated writes carries those writes forward with the sub-branch's `branch_ts` as `cutoff_ts` (see piece (7)).

**Compaction on the fork shard** proceeds normally. When the fork shard compacts a span that includes source-referenced batches, it reads through those batches (honoring `cutoff_ts`) and writes the compacted output as a normal batch under its own UUID, with `cutoff_ts` absent. The cutoff has been *applied* and baked into the output. The source-blob references those input batches held are released (see piece (3)). A fork shard's dependence on source blobs falls off as the fork is used; long-lived forks with active compaction trend toward zero source-blob dependence over time.

### 3. GC story: `fork_blob_refs`

Pinning source blobs against GC is bookkeeping in shared metadata, not a primitive that lives inside the source shard's state machine. A new table in the shared metadata store (CockroachDB):

```sql
CREATE TABLE fork_blob_refs (
    blob_key        TEXT    NOT NULL,        -- full key, e.g. s<source>/n.../p...
    fork_shard_id   TEXT    NOT NULL,
    branch_id       UUID    NOT NULL,
    PRIMARY KEY (blob_key, fork_shard_id)
);
CREATE INDEX ON fork_blob_refs (branch_id);
CREATE INDEX ON fork_blob_refs (blob_key);
```

One row per `(blob_key, fork_shard_id)`. At `CREATE BRANCH`, the coordinator bulk-inserts one row for every distinct absolute blob key in every fork shard's initial manifest. The `branch_id` lets `DROP BRANCH` clean up in one bulk delete.

Persist's blob deletion path (`GarbageCollector::delete_all`) gains a single gate: before calling `blob.delete(full_key)`, check whether any row in `fork_blob_refs` references that key. If so, skip. The check is a point lookup (`SELECT 1 FROM fork_blob_refs WHERE blob_key = $1 LIMIT 1`). For environments with no live branches the table is empty and the lookup is cheap; for environments with branches GC is already a batched, infrequent background process and an extra lookup per superseded blob is negligible.

**Lifecycle:**

| Event | Action |
|---|---|
| `CREATE BRANCH` | Bulk insert one row per (absolute blob key, fork_shard_id) across every forked object |
| Source GC runs | Each superseded blob's key checked against `fork_blob_refs`; matches are skipped |
| Fork compacts a source-referenced batch | New compacted batch lives in the fork's own blob namespace, with no `cutoff_ts`; the old source-blob refs for the input batches are deleted from `fork_blob_refs` (the fork no longer depends on them) |
| `DROP BRANCH` | `DELETE FROM fork_blob_refs WHERE branch_id = $1` (one round-trip); tombstones the fork's catalog entries; next source GC cycle reclaims now-unreferenced blobs |

The source shard's `since` is unaffected. The source can compact past `branch_ts` freely; compaction creates new blobs and marks old ones for deletion, and the ref-count gate keeps the old ones alive only as long as some fork's manifest still points at them.

### 4. Source schema evolution with branches alive

`ALTER TABLE`, `DROP TABLE`, `RENAME` on a source object that has live branches are all allowed. The mechanisms:

- The branch's catalog item holds its own `RelationDesc` snapshotted at `branch_ts`, so the branch's *catalog shape* is fixed at branch creation and doesn't follow source ALTERs.
- The fork shard's hollow batches were captured with the source's batch schemas in effect at `branch_ts`. Persist's existing schema registry already tracks per-batch schemas, so the fork shard's read path decodes inherited batches at their captured schema regardless of how the source's live schema later diverges.
- `DROP TABLE` on the source is allowed because the fork shard's manifest holds its own references to the source blobs (via `fork_blob_refs`); the source's catalog tombstone and shard removal cannot delete blobs the fork still references.

When the last branch referencing a source blob releases (via `DROP BRANCH` or compaction-induced ref release in piece (3)), the blob becomes GC-eligible on the source's next cycle.

The symmetric branch-side story: `ALTER TABLE` on a branched object mutates only the branch's snapshotted `RelationDesc` and the fork shard's schema. The source's catalog entry, `RelationDesc`, and shard are untouched. Branch-side DDL is unrestricted (the branch is the user's), and a branch and its source can have divergent schemas indefinitely.

### 5. Sources and sinks in branched schemas

A branched schema can contain sources and sinks. Default semantics in the branch:

- **Branched source: inert.** The fork shard's data is whatever was committed up to `branch_ts`; no ingestion process is attached in the branch. The inert state is exposed as a standard source option (working name: `INGESTION = OFF`), set by the branch-creation path. Activating uses the existing `ALTER SOURCE … SET (...)` syntax (no new keyword) to flip the option, at which point the storage controller starts a fresh ingestion against the same external system, writing into the fork shard instead of the source's.
- **Branched sink: inert.** No emission. Same shape via existing `ALTER SINK … SET (...)` (working name: `EMISSION = OFF` by default). Activating requires the user to also supply a redirected destination via the same `SET` (you can't toggle on without redirecting; the planner errors otherwise, to prevent accidental double-emit).

The source database's sources and sinks are untouched. Inside the branch, `SHOW SOURCES` / `SHOW SINKS` lists branched objects normally; their inert state surfaces through `mz_internal` status views and through the source-option columns in `SHOW CREATE`. Reads against an inert branched source return the data up to `branch_ts` (via the cutoff filter on inherited batches) and nothing later, since no ingestion is writing to the fork shard. This is the safety story: branching a schema containing a production sink can never accidentally double-emit.

### 6. Compute: isolated clusters for branched dataflows

Branched MVs and indexes need somewhere to run their dataflows. Rendering them on the source's cluster would couple branch compute back to production, which defeats the isolation point. The branch gets its own.

At branch creation:

- The coordinator walks the source schema's MVs and indexes and collects the set of clusters hosting them. For each distinct source cluster, it allocates one branch-local cluster, sized to match.
- Each branched MV and index renders on the branch cluster mapped from its source cluster, reading from the branch's fork shards (which already encode the snapshot view via piece (2)) and writing to the branch's own MV output shards (themselves fork shards of the source MV's persist shard).
- The source's clusters are untouched. Production compute and branch compute share no resources.
- Cluster lifetime is tied to the branch. `DROP BRANCH` tears down every branch-local cluster along with the catalog entries and ref rows.

This one-to-one mapping preserves the source's compute topology in the branch: an MV that was isolated to its own cluster in the source is still isolated in the branch, and one that shared a cluster with N siblings still shares with those same N siblings.

For whole-database branching in piece (7), the same rule scales: one branch cluster per distinct cluster used across the source DB.

Allocating a cluster is cheap; hydrating the dataflows that run on it is not. That cost is treated separately in piece (8).

### 7. Sub-branches and whole-database branching

**Sub-branches.** `CREATE BRANCH <name> FROM BRANCH <parent>` is the same primitive applied to a fork shard: allocate a new fork shard, copy the parent fork shard's hollow batches (preserving each batch's blob keys, which may be absolute pointers further up the chain), stamp each inherited batch with `cutoff_ts = sub_branch_ts` (taking the minimum with any existing `cutoff_ts` so that source-derived batches keep their earlier, tighter cutoff), and bulk-insert ref rows into `fork_blob_refs` for every blob key the sub-branch shard now references.

Worked example:

- Source `S` accepts writes up to `t_10`.
- `CREATE BRANCH B1 FROM SCHEMA …` at `branch_ts = t_5`. `B1`'s fork shard inherits `S`'s batches with absolute keys; each gets `cutoff_ts = t_5`. Reads on `B1` see source data ≤ `t_5`.
- User writes to `B1` at `t_6`. New batch lives in `B1`'s own blob namespace, with `cutoff_ts` absent. Reads on `B1` see source data ≤ `t_5` ∪ `B1`-local writes.
- `CREATE BRANCH B2 FROM BRANCH B1` at `sub_branch_ts = t_8`.
  - For each of `B1`'s inherited (absolute-keyed) batches: copy into `B2`'s manifest. Its existing `cutoff_ts = t_5` is already tighter than `t_8`, so `cutoff_ts` stays `t_5`.
  - For each of `B1`'s local batches (the `t_6` write): copy into `B2`'s manifest with the same blob key (which is absolute, pointing into `B1`'s namespace) and stamp `cutoff_ts = t_8`.
  - Bulk-insert ref rows into `fork_blob_refs` for every blob `B2` now references: both the source blobs `B1` was already protecting and the `B1`-local blobs `B2` newly protects.
- Reads on `B2`:
  - Source batches: filtered at `t_5` (cutoff baked in earlier). ✓
  - `B1`-local batches: filtered at `t_8`. If `B1` writes again at `t_9`, that's invisible to `B2` because `t_9 > t_8`. The batch is also brand-new in `B1` after `B2`'s creation and is not present in `B2`'s manifest at all. ✓
  - `B2`-local writes (none yet): seen unfiltered.

Persist has no parent/child concept; the forest lives in the catalog. `DROP BRANCH` on a parent with live children is rejected at planning with a clear error listing the children; the user must drop children first. This keeps catalog state acyclic and avoids zombie parents kept alive only to satisfy descendant ref-counts. Within a single tree, drop order is therefore leaf-to-root.

**Whole-database branching.** `CREATE BRANCH <name> FROM DATABASE <db>` snapshots every object in the source DB at a single coordinated `branch_ts` (`t ≤ min(upper(s))` across every shard in the DB), forks every shard via the piece (2) primitive, forks the entire catalog into a new namespace, and rewrites every cross-object reference to its branch-local `GlobalId` via a dependency-graph walk. All shard forks and ref-row inserts happen in a single CRDB transaction (all-or-nothing). MVs and indexes are copied with remapped refs; their output shards are forked the same way as table shards. Sources and sinks land inert by default per (5). No new persist or metadata primitives; this is composition of the schema-branching machinery at DB scope.

### 8. Compute hydration: lazy render with snapshot serving

The storage fork (piece 2) and cluster allocation (piece 6) are O(metadata) and finish in milliseconds. Hydrating the in-memory arrangements that back branched MVs and indexes is the slow part. For a schema with non-trivial indexed state, the time between `CREATE BRANCH` returning and the branch being queryable on the index path is bounded by the slowest arrangement to rebuild. That's minutes to hours for large workloads. "Instant" refers to the fork. Branch readiness is a separate clock, and the design has to be explicit about both.

The cost is asymmetric across object kinds:

| Kind | Hydration cost |
|------|----------------|
| Table | None. The forked shard is the data; reads open it. |
| Materialized view | Output shard is forked, so a point-in-time read at `branch_ts` returns immediately. The maintenance dataflow needs full hydration before any branch-local write to an upstream can propagate into a fresh read. |
| Index | Worst case. No persistent output to serve from; the arrangement *is* the index. The branch cluster reads the full snapshot of the indexed collection and builds the arrangement before the index is usable. |

The design ships one optimization for MVs and names two others as future work.

**Snapshot-served MVs (in scope).** A branched MV's maintenance dataflow is not installed at `CREATE BRANCH`. Until a query against the MV needs up-to-date results (meaning at least one branch-local write has landed in one of its upstream shards), reads of the MV go directly to the forked output shard, on the same code path as a table read. This is correct because the forked output shard's contents *are* the MV's value at `branch_ts`, and no upstream has diverged.

When the coordinator detects the first read against a branched MV whose forked inputs have advanced past `branch_ts`, it installs the dataflow on the branch-local cluster (piece 6). The dataflow's `persist_source` operators read the full forked input shards (source data ≤ `branch_ts` plus branch-local writes), recompute the MV, and serve subsequent reads incrementally. The dataflow stays installed for the life of the branch.

The trigger state is per-MV runtime in the coordinator, recoverable from catalog + persist on restart (the catalog says which MVs exist in the branch; persist tells us which input shards have `upper > branch_ts`). No new catalog field.

For workflows of the shape "branch a 50-MV schema, write to 2 tables, query 3 MVs," at most three MV dataflows hydrate; the other 47 stay snapshot-served and cost nothing. This is the dominant case for agent iteration and dev-environment use, so the optimization is worth shipping.

Indexes do not benefit. There is no output shard to serve from, so a branched index must hydrate eagerly on `CREATE BRANCH`. `SHOW BRANCH STATUS` exposes an aggregate `indexes_ready_at` (NULL while any branched index is still hydrating, the max completion timestamp once all are ready); per-index detail is available through the existing `mz_internal` compute status views.

**Render-time warm-start (future work).** When a snapshot-served MV finally has to render, today's pipeline reads inputs from `T::minimum()` and reconstructs intermediate state from scratch. A warm-start path would inject the forked output shard as the operator's initial state and consume input deltas from `branch_ts + 1` forward. The win is operator-dependent:

- **Reductions** (`GROUP BY … SUM/COUNT/…`): the operator's per-group state *is* the output. Loading the output shard into an arrangement gives a ready-to-go operator. Largest win.
- **Joins**: the output shard does not contain the input-side arrangements keyed by join column that incremental maintenance requires. Warm-start saves the join's compute cost (potentially massive) but you still pay to scan and arrange the forked input shards. Partial win.
- **Stateless transforms** (map, filter, project): nothing to warm-start; deltas pass through.

This needs new compute machinery. Today's render pipeline begins with `persist_source`, not with pre-computed state injected mid-plan. Deferred from this design but named so future work has a place to attach.

**Cross-cluster arrangement ship (future work).** The only known answer for the index-on-base-table case, where neither snapshot serving nor warm-start helps. The source cluster's replica already has the exact arrangement at `branch_ts` in memory. Serializing it (or streaming it over the replica protocol) and deserializing on the branch cluster's replica would drop hydration from "full input scan + arrangement build" to "network transfer + deserialization." This is a large independent effort. It touches arrangement representation, the replica protocol, and the consistency model. Not in scope here. Worth naming so the design has a known endpoint: once cross-cluster ship lands, branching's hydration costs converge to network bandwidth, not recomputation.

### User-visible DDL

```sql
CREATE BRANCH <name> FROM SCHEMA   <schema>;
CREATE BRANCH <name> FROM BRANCH   <branch>;     -- sub-branch
CREATE BRANCH <name> FROM DATABASE <db>;         -- whole-DB

DROP BRANCH [IF EXISTS] <name>;
SHOW BRANCHES;
SHOW BRANCH STATUS <name>;
```

The three forms are one parser rule; the qualifier (`SCHEMA` / `BRANCH` / `DATABASE`) selects the referent kind and is required (no inference from the name). All gated by `enable_branching` (default off). The flag is monolithic in v1 — schema, sub-branch, and DB branching are gated together. If phased rollout becomes necessary we'll add a sub-flag (e.g. `enable_branching_database`) without renaming the main one. Privileges are regular-role, not superuser: `CREATE BRANCH … FROM SCHEMA` requires `CREATE` on the database + `USAGE` on the source schema; `FROM DATABASE` requires the existing `CREATEDB` system privilege + `USAGE` on the source DB; `FROM BRANCH` requires ownership of the parent branch. `DROP BRANCH` requires ownership. `SHOW BRANCHES` / `SHOW BRANCH STATUS` are visibility-filtered like other catalog views.

`SHOW BRANCH STATUS <name>` returns one row with the branch's runtime state. Approximate shape:

```
name               text         -- branch name
kind               text         -- schema | sub-branch | database
source             text         -- e.g. "schema=prod.sales" or "branch=prod.sales/exp1"
branch_ts          mz_timestamp -- the coordinated fork timestamp
created_at         timestamp
owner              text
object_count       int          -- branched tables + MVs + indexes
blob_ref_count     int          -- live rows in fork_blob_refs for this branch
clusters           text[]       -- branch-local clusters allocated for this branch
indexes_ready_at   timestamp    -- max hydration completion across branched indexes; NULL while any index is still hydrating (see piece 8)
child_count        int          -- 0 for leaves; nonzero blocks DROP
```


## Minimal Viable Prototype

The MVP delivers schema-level branching for tables, materialized views, and indexes, behind a flag. Every restriction is a single removable check, so the lift to the full design is incremental and never requires a catalog migration.

What the MVP delivers from the design:

- Piece (1), catalog decoupling, in full. `BranchDescriptor` ships with all the fields the long-term design needs.
- Piece (2), fork-shard storage, in full for the single-level case: a `fork_shard` primitive in persist (absolute blob keys, `cutoff_ts` on hollow batches, `initialize_from_snapshot` on `Machine`) and the read-time cutoff filter in `persist_source`.
- Piece (3), `fork_blob_refs` ref-counting and GC integration, in full.
- User-visible DDL for the schema-level case: `CREATE BRANCH … FROM SCHEMA`, `DROP BRANCH`, `SHOW BRANCHES`, `SHOW BRANCH STATUS`. (`FROM BRANCH` and `FROM DATABASE` parse but are rejected at planning; see below.)

What the MVP restricts, and how the restriction lifts:

| Piece | MVP behavior | How it lifts |
|-------|--------------|---------------|
| (4) Source schema evolution | Narrow source-side freeze: `ALTER TABLE` / `DROP TABLE` / `RENAME` blocked on a source object that has a live branch. `CREATE` and DML on source are untouched. One helper (`coord::branch::source_ddl_blocked_by_branch`) behind one flag. | Per-batch schema decoding from persist's existing schema registry already works for inherited batches; the freeze is conservative belt-and-suspenders for MVP. Helper is deleted in a single revert once we've validated decoding across source `ALTER` end-to-end. |
| (5) Sources/sinks in branches | Branching a schema that contains a source or sink is rejected at planning time with a clear error. | Inert-by-default behavior at planning and storage-controller layers. Catalog shape doesn't change. |
| (6) Compute isolation | Branched MVs and indexes render on the user's session cluster (or the source schema's default cluster). No branch-local cluster spin-up. Risk: shared compute pressure during dogfooding. | Coordinator allocates one branch-local cluster per distinct source cluster at `CREATE BRANCH`, renders branched MVs and indexes on the mapped branch cluster, tears all of them down on `DROP`. |
| (7) Sub-branches | `CREATE BRANCH … FROM BRANCH` rejected at planning in MVP (parser accepts the qualifier, planner errors). | The fork-shard primitive composes natively; the sub-branch path just calls it on the parent's fork shard and carries `cutoff_ts` forward as described. |
| (7) Whole-DB | `CREATE BRANCH … FROM DATABASE` rejected at planning in MVP (parser accepts the qualifier, planner errors). | Lands after sub-branches and inert-source/sink behavior; coordinator orchestrates N parallel fork-shard calls in one CRDB transaction. |
| (8) Compute hydration | Snapshot-served MVs ship in MVP; trigger logic is coordinator-side. Branched indexes hydrate eagerly. Render-time warm-start and cross-cluster arrangement ship are not in MVP. | No catalog or persist change to lift; warm-start and arrangement ship are independent future efforts. |

## Alternatives

**Empty delta shard + N-way read merge over pinned source snapshots.** Each branched object gets a fresh empty shard for branch-local writes; reads consolidate over a chain `Shard@T0 + Delta_1 + ... + Delta_N`; a new persist "pinned snapshot" primitive protects source blobs from GC without blocking the source's `since`. Rejected because read-time consolidation cost grows linearly with sub-branch depth, with no clean compaction story that preserves the pin semantics; (c) the read path becomes a per-object special case rather than a normal persist shard, complicating every downstream consumer (`persist_source`, MV rehydration, indexes). The fork-shard model in this design keeps reads on a regular shard and pushes the only special behavior (the `cutoff_ts` filter) into a single well-scoped place in `persist_source`.

**Pinned snapshots with no delta shard (read-only branches).** Use only the pin primitive and surface branched objects as read-only views over `source@branch_ts`. Rejected: violates the success criterion that branches accept writes and DDL, doesn't support branch-local MVs/indexes that produce new data, and the catalog cost we'd pay later to upgrade to writable branches is identical to doing it right now.

**Source-side `CriticalSinceHandle` to pin `since` past `branch_ts`.** Hold the source shard's `since` at `branch_ts` for the life of the branch. Source bytes stay alive trivially because nothing past `branch_ts` is ever compacted. Rejected: blocks source-shard compaction for the entire lifetime of every branch, which is exactly the kind of production-blast-radius coupling branching is supposed to avoid. Also doesn't generalize to sub-branches without nesting holds, and the `since` ratchet is irreversible if anything goes wrong.

**Branching as a view, not a namespace.** Surface branched tables as read-only views over `source AS OF branch_ts` with a side-table for branch writes. Rejected: it violates the success criterion that branches be addressable real catalog items, doesn't support branch-local DDL, and the catalog cost we'd pay later to upgrade is identical to doing it right now.

**Status quo (no branching).** Keep the three workarounds. Rejected. The agent-iteration pattern is showing up in more and more workflows, and the workarounds actively hurt.

## Open questions

1. **`PartialBatchKey` proto compatibility.** Persist's hollow-batch state proto encodes batch part keys as a single string. Adding the `Absolute` variant needs a backwards-compatible discriminator: old readers must not silently mis-parse an absolute key as a relative key (which would resolve to the wrong full path). Likely solution: a new proto field that defaults to `Relative` on older state, with the discriminator gated behind a feature version bump.
2. **`fork_blob_refs` check scaling under bulk GC.** A point lookup per superseded blob is fine for typical GC (dozens of blobs per cycle). For a hot shard compacting thousands of blobs in one cycle, the per-blob lookups serialize into the GC critical path. Plausible mitigation: batch the lookup (`SELECT blob_key FROM fork_blob_refs WHERE blob_key = ANY($keys)`) or maintain an in-memory bloom filter of referenced keys per environment. Need a load model before we pick.
3. **`Machine::initialize_from_snapshot` interaction with epoch/fence.** Initializing a brand-new shard's state from pre-formed hollow batches sidesteps the normal `compare_and_append` write path.
4. **Sub-branch depth and `cutoff_ts` propagation cost.** Each sub-branch level copies the parent's hollow-batch manifest into a new shard and updates `cutoff_ts`. Manifest size is proportional to the number of distinct batches the parent had, which can be large for long-lived shards. Need to confirm the manifest copy is fast enough at realistic batch counts to keep `CREATE BRANCH` instant.
5. **Whole-DB cross-DB query semantics.** Today's catalog allows cross-DB references; a join between source DB and branch DB sees different `as_of`s. Block at planning, warn-and-allow, or define a consistent `as_of` rule for mixed queries?
