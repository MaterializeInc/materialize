# POC: sorted persist shards as an object-store-backed lookup tier

Spike notes. Not a design doc. Nothing here is reviewed.

## Goal

Serve ad hoc queries (point lookups, and joins keyed on a large collection)
from an isolated serving cluster without hydrating indexes. The data lives in
a persist shard that is sorted by a declared key, so readers prune parts by
stats instead of arranging the whole collection.

Motivation: the stage `mz_catalog_server` has 166 indexes and took about 8
minutes to hydrate on `M.1-xsmall`. Every new isolated serving cluster would
pay that again.

## Pitch

`CREATE MATERIALIZED INDEX` is an index that lives in object storage instead
of in a cluster's memory. One cluster maintains it, writing a persist shard
sorted by the index key. Any cluster can then answer key lookups from it,
reading about one part per sorted run instead of hydrating an arrangement.

Today, isolating or scaling out reads means copying indexes: each new cluster
or replica rebuilds every index it needs in its own memory, and pays the
hydration time again. A materialized index separates maintaining the data
from serving it:

- **Read scale-out.** Maintenance happens once. Serving clusters are small
  and stateless: they start answering immediately and hold no index memory,
  so adding read capacity means adding clusters, not copying state. Because
  the shard is sorted by key, different keys hit different parts. That makes
  routing serving clusters by key range, for cache locality, a natural next
  step. This scales reads over shared storage. It does not shard the
  maintenance work itself.
- **Catalog server isolation.** Every catalog query (Console, agents and MCP,
  `SHOW`, user introspection) shares `mz_catalog_server`, so a heavy ad hoc
  query slows down the critical ones. Rebuilding its 166 indexes on another
  cluster took about 8 minutes on stage. With materialized indexes on the
  catalog relations, each workload could get its own serving cluster that
  reads the same sorted shards, with no hydration and no shared CPU.

Tradeoffs: lookups take tens of milliseconds instead of single milliseconds,
plus object store round trips. Freshness follows the maintaining dataflow's
lag, like an MV. It serves lookups with literals on the leading key column
and two-input ad hoc joins on it. Joins whose probe keys spread over the
whole key range, and full scans, are no faster than reading an MV.

## SQL surface

```sql
CREATE MATERIALIZED INDEX [IF NOT EXISTS] [name] [IN CLUSTER c] ON obj (col, ...);
```

- Maintained by the `IN CLUSTER` cluster like an MV. Stored in persist with
  the key columns first, sorted on write. Takes no arrangement memory.
- Used automatically, like an index: a query on `obj` with literals on the
  leading key columns reads the materialized index's shard from whatever
  cluster runs the query. You never query it by name (you can, though).
- Keys must be distinct plain columns whose type preserves order in persist.
  No expressions, no `DEFAULT`.

Considered and rejected: giving MV `WITH (PARTITION BY ...)` this meaning.
That option already parses and validates (behind
`enable_collection_partition_by`, default on) but is ignored downstream, so
honoring it would change existing MVs that set it. The POC leaves user
`PARTITION BY` ignored and uses an internal `partition_by` field instead.

## How the POC implements it

- Parser: `CreateIndexStatement` gains `materialized: bool` and an internal
  `AS OF` (like MVs, the sequencer writes the storage `as_of` into
  `create_sql` so bootstrap can use it).
- Planner (`plan_materialized_index` in `ddl.rs`): plans the statement as
  `CREATE MATERIALIZED VIEW <name> AS SELECT <key cols>, <rest> FROM obj`
  through the normal MV planner, then sets `create_sql` back to the
  `CREATE MATERIALIZED INDEX` text, `partition_by = key.len()` and
  `materialized_index = { on, key }`. Restarts re-plan through the same path
  (verified: restart without `--reset` keeps serving lookups).
- Catalog: a real item type, `CatalogItemType::MaterializedIndex`, reported
  by `CatalogItem::typ()` for MVs with `materialized_index` set and by the
  durable catalog's `item_type(create_sql)`. Users see an index: its
  `ObjectType` is `Index`, so `DROP INDEX` works and `DROP MATERIALIZED VIEW`
  refuses it; `CREATE` returns `CREATE INDEX`; `mz_objects` says `index`;
  `SHOW INDEXES` lists it and `SHOW MATERIALIZED VIEWS` does not. Internally
  it is still a `CatalogItem::MaterializedView` (storage collection plus
  compute sink), which is the machinery it needs. Remaining shortcuts: the
  durable proto `CatalogItemType` has no variant for it (only system object
  mappings serialize item types, so it maps to `MaterializedView`), `SHOW
  INDEXES` shows an empty key list, and selecting from it by name works.
- Sink: `partition_by` flows plan -> catalog -> MV optimizer ->
  `MaterializedViewSinkConnection::partition_by` (serde, no proto change).
  The sink (v1 and v2) passes `max_runs = Some(4)` to persist's batch builders
  through `PersistApi`, including the shared cross-worker builder.
  `PersistClient::batch_builder` already had this per-builder override.
- Routing (`src/adapter/src/optimize/materialized_index.rs`): chosen
  before the timestamp. For a `SELECT` whose dependencies have a materialized
  index, peek sequencing locally optimizes the query first (like `EXPLAIN
  TIMESTAMP` already does), and `choose_routes` picks a materialized index for
  each relation whose leading key column is pinned by a literal filter or a
  join equivalence, unless the cluster has an in-memory index on it. The
  timestamp and read holds come from the routed plan's dependencies, and the
  peek optimizer applies the same routes (`Get t` becomes a permuted
  `Get mi`). The fast path then sees `Get mi` and uses the persist lookup on
  any literal key prefix. Verified: with the materialized index's cluster at
  zero replicas, a non-key query on `t` still returns immediately, and a key
  lookup waits for the index and then sees the latest write.
- Lookup join (`plan_lookup_join` in `materialized_index.rs`, `PendingLookup`
  in `src/compute/src/render.rs`): when a peek dataflow joins a routed
  materialized index at its key with one other input, the optimizer adds a
  keys object (`Distinct` of the probe side's join key) ahead of the joining
  object and marks the index's source import with
  `SourceImport::lookup = { keys_from, key_column }`. Rendering withholds the
  import's start signal until a collector operator has seen every key at the
  `as_of` (keys are broadcast to all workers), then a part filter on
  `persist_source` drops parts whose key-column stats hold none of the keys.
  The join operator is the normal one: the lookup is a gated persist source
  with a dynamic part filter, so the join stays exact on a superset of rows.
  Dropped parts use a new `FilterResult::Skip`, which persist does not audit:
  the pushdown audit re-checks only the MFP and panics (`panic_on_audit_failure`)
  on parts dropped for any other reason. Limits: two-input joins, first key
  column, probe side computed twice, applies only when the index is read once
  in the dataflow, no cutover to a scan for wide key sets, recursive dataflows
  fall back to an unfiltered read.
- Durable catalog: `item_type(create_sql)` asserted `MATERIALIZED` is
  followed by `VIEW` and crashed the coordinator on the first
  `CREATE MATERIALIZED INDEX`. Mapped to `MaterializedView`.
- Builtin catalog views derive from `create_sql`, so `mz_indexes` and
  `mz_objects` list the materialized index as an `index` while the item is an
  MV internally. Cosmetic for the POC, arguably the right label.
- Not done: smaller parts. `BLOB_TARGET_SIZE` is shard-wide config with no
  per-writer override.

## Findings (possible MZ bugs or follow-ups, surface later)

1. **Filter pushdown lost by fallible projections.** `ColumnSpecs::mfp_filter`
   and `mfp_plan_filter` marked a part fallible if any map expression could
   error, even one only evaluated after predicates that rule out every row.
   Projecting `length(v)` fetched 576 of 582 parts vs 6 for `upper(v)`. Fixed
   in local commit `5c613ca419` on branch `pushdown-fallible-maps` (proven red
   without the fix, proptest against `evaluate_inner`). PR candidate.
2. **Large writes land as hundreds of unsorted runs that never compact.**
   `persist_batch_max_runs` defaults to 1, which disables write-time
   compaction. A 2M-row MV snapshot became 575 single-part `Unordered` runs,
   each spanning the whole key range. Spine compaction only merges batches
   with each other, so a lone big batch stayed that way through 80 later
   writes. Fast path lookups on it took up to 2.4 s. Worth raising with
   persist even outside this POC (re-sort a batch with many unordered runs).
3. **MV `PARTITION BY` is accepted and silently ignored** (see above). Either
   document it as a no-op or wire it to sort-on-write behind its own flag.
4. **`statement_timeout` does not fire while a peek waits on a stalled
   collection.** With `statement_timeout = '5s'` and the maintaining cluster
   at zero replicas, `SELECT count(*) FROM t_plain` (a plain MV, no POC code
   on its path) waited 128 s until the replica came back. Verify on a `main`
   build before filing.
5. **Durable catalog infers item types from `create_sql` words.** Any new
   `CREATE <words>` statement form needs `item_type()` updated, or the
   coordinator panics on the first create.
6. **A peek that panics a replica crash-loops the cluster.** While building
   the lookup join, a render panic in a one-shot SELECT made the serving
   replica abort, and after each relaunch the controller replayed the same
   peek, so the replica crashed every ~6 s until the query was cancelled. A
   bad ad hoc query can take a cluster down indefinitely. Worth checking how
   the controller treats peeks across replica restarts.
7. **Bare `psql` timing with `echo '\timing on'` in zsh** turns `\t` into a
   tab. Not an MZ bug, just a trap for benchmark scripts.

## Measurements so far (Docker v26.50, local-file blob, no S3 latency)

| Lookup | Latency |
|---|---|
| `ReadStorage Lookup`, 575 unsorted runs | 18 ms to 2.4 s, grows with key position |
| `ReadStorage Lookup`, 6 sorted runs (`max_runs = 4`) | 6 to 25 ms |
| In-memory index lookup | 3 to 12 ms |

## Measurements: POC build (local `bin/environmentd`, local-file blob)

Setup: table `t (k int, v text)`, 2M rows in 4 inserts, `persist_blob_target_size`
= 256 KiB. `CREATE MATERIALIZED INDEX t_k_mi IN CLUSTER producer ON t (k)` and a
control `CREATE MATERIALIZED VIEW t_plain AS SELECT * FROM t`, both on
`producer` (2 workers). Queries run on a separate `serving` cluster (1 worker).

Shard layout right after creation:

| Shard | Runs | Parts | Order |
|---|---|---|---|
| `t_k_mi` | 4 | 642 | all `Structured` |
| `t_plain` | 636 | 636 | all `Unordered` |

`EXPLAIN SELECT k, length(v) FROM t WHERE k = 5 AND v > 'a'` on `serving`:
`ReadStorage Lookup on t_k_mi, Lookup value: (5)` with the filter and map
applied on top. Non-key filters (`WHERE v = 'x'`) still read `t`.

Point lookups from `serving`, 10 keys spread over the range:

| Path | Latency |
|---|---|
| `SELECT * FROM t WHERE k = ?` via materialized index | 5 to 8 ms |
| Plain MV, no index (dataflow per query) | 8 ms to 1 s |
| In-memory index on `serving` (after 2.0 s hydration) | 1 to 2 ms |

After 40 more inserts of 10k rows: both shards compacted to 5 runs (3 sorted,
2 small unsorted recent batches) and materialized index lookups took 8 to 12
ms. So the plain MV does become sorted once enough writes arrive to merge its
big batch. The materialized index is sorted from the start and does not
depend on that. Earlier, with only single-row writes, a 2M-row batch stayed
unsorted through 80 writes.

Caveat: no S3. Each lookup touches about one part per run, so on S3 expect
roughly one round trip per run (fetched in parallel), plus 256 KiB parts here
vs 128 MiB by default.

## Lookup join measurements (same setup, `serving` cluster)

`SELECT ... FROM probe p JOIN t ON t.k = p.pk` with `t_k_mi` on `t`, against the
same join on the plain MV `t_plain` (2.4M rows, 721 parts). Results identical
in every case. Five runs each, first run cold.

| Probe | Path | Parts fetched / skipped | Latency |
|---|---|---|---|
| 5 keys | lookup join | 4 / 717 | 15 to 37 ms (1.07 s cold) |
| 5 keys | plain MV join | 721 / 0 | about 1.1 s |
| 2000 keys, contiguous | lookup join | 2 / 720 | 31 ms |
| 2000 keys, contiguous | plain MV join | 722 / 0 | about 1.1 s |
| 2000 keys, spread over the range | lookup join | 640 / 82 | about 0.9 s |
| 2000 keys, spread over the range | plain MV join | 722 / 0 | about 1.1 s |

Spread keys touch most parts, so the lookup degrades to a scan. A real
version needs a cost cutoff that falls back to the plain read, and smaller
parts (or ranged reads within parts) to keep spread lookups selective.
