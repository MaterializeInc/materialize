# Ordered reads from indexes

- Associated: TBD

## The Problem

Operational applications page through the newest, or otherwise first, rows of
a large collection. Take an issue tracker that lists a project's open tickets
carrying any of a set of labels, most recently updated first:

```sql
SELECT DISTINCT ON (updated_at, ticket_id) ticket_id, title, assignee, updated_at
FROM ticket_labels
WHERE project_id = 'proj-a'
  AND label IN ('bug', 'regression')
  AND status = 'open'
ORDER BY updated_at DESC, ticket_id DESC
LIMIT 21;
```

`ticket_labels` is a materialized view that joins tickets with their labels, one
row per ticket and label, so that a filter on labels is a plain predicate and
the query reads a single index. A ticket carrying both requested labels appears
twice, and `DISTINCT ON` collapses it back to one row per ticket. A semi-join
against the labels instead would make the query a join, which the fast path does
not serve. Aggregating each ticket's labels into a list costs more to maintain
and turns the filter into a containment test.

`LIMIT 21` is a page of 20 plus a probe for whether a next page exists. The
filters are runtime parameters. The queries back interactive pages, so they are
latency-sensitive. Many queries share the shape: they bind a key such as a
project, order by recency or rank, filter on attributes at a finer grain than
the result, and return a page.

Materialize serves these poorly regardless of which indexes the user builds.

**Indexes cannot be read in SQL order.** An index is an arrangement, a
multiversioned collection of sorted batches whose cursors seek and step through
keys in order. That order is `Row`'s, which compares the byte length of the
encoding first and the bytes second (`RowRef::cmp` in `src/repr/src/row.rs`,
mirrored by `DatumSeq` in `src/row-spine/src/lib.rs`). Integers are packed with
variable-width tags, so even an index on one integer column is not in numeric
order. Values within a key are in the same order. No access path can stop after
the first `k` rows in SQL order.

**Fast-path peeks scan everything they could return.** For a peek whose
finishing has an `ORDER BY` and a `LIMIT`, each worker walks its entire share of
the index, or the entire result of a literal-constraint lookup, applies the MFP,
and partitions a buffer down to `limit + offset` rows whenever the buffer
doubles (`PeekScan::thin` in `src/compute/src/compute_state/peek_scan.rs`). The
walk cannot end early: `PeekScan::finishing_satisfied` is never true for an
ordered finishing, because no prefix of the walk ranks rows against the whole
trace. Network and memory are bounded by `k`, but CPU is proportional to the
collection or lookup size on every query. A lookup key's rows all hash to one
worker, so a large project concentrates that cost on a single worker. A walk
that outruns `compute_index_peek_inline_budget` cursor positions finishes on a
blocking pool under a permit (`enable_compute_index_peek_offload`, see
`src/compute/src/compute_state/peek_offload.rs`), so it no longer stalls the
timely worker. It still costs its full CPU, and offloaded walks queue for a
bounded number of permits (`mz_index_peek_permit_queue_depth`), so a burst of
large ordered reads delays every other expensive peek on the replica. The gap
between `mz_index_peek_row_iteration_rows` and the rows a peek returns measures
the waste.

**`DISTINCT ON` leaves the fast path entirely.** `DISTINCT ON` plans as a
`TopK` whose group key holds the distinct columns and whose limit is 1
(`src/sql/src/plan/query.rs`). `create_fast_path_plan` in
`src/adapter/src/coord/peek.rs` only removes a `TopK` with an empty group key
that the finishing subsumes. Every execution of a query like the one above
therefore builds a one-off dataflow, arranges every row of the lookup in the
`TopK`, and tears the dataflow down again.

The existing remedies each cover a slice of the problem:

- An indexed view with `ORDER BY ... LIMIT`, or a `ROW_NUMBER() ... <= k`
  filter, is maintained incrementally and served from the fast path. It fixes
  `k`, the ordering, and every filter when the view is created, so runtime
  parameters such as the label list above cannot be expressed.
- The persist fast path can read the first rows of a shard in order
  (`persist_fast_path_order`). It applies only without filters, with a small
  limit, in ascending order, on a prefix of the relation's columns, and for
  types whose persist encoding preserves order (`preserves_order` in
  `src/repr/src/row/encode.rs`).

## Success Criteria

1. A query that binds an index's key by equality, orders by other columns, and
   has a `LIMIT k`, is served from the fast path. Each worker visits `O(k)` rows
   plus the rows the residual filters reject before `k` rows pass, instead of
   all rows of the lookup.
2. A query whose `DISTINCT ON`, or `TopK`, has a group key that is a prefix of
   its `ORDER BY` is served from the fast path, with or without an ordered
   read, and no longer builds a dataflow.
3. Results are the results the query's plan defines. They are identical to the
   existing paths' wherever the `ORDER BY` determines the order, and within a
   `DISTINCT ON` group the row chosen is one the plan's `TopK` may choose, which
   SQL leaves unspecified.
4. No query gets slower or changes plan unless it uses an ordered read, and
   creating any new structure never changes the plan of a query that does not
   order by it.
5. The memory cost of serving several shapes over one wide collection is
   measured and stated, and it decides between the candidate representations
   together with tail latency.
6. In the feature benchmark, ordered-read latency is flat in the collection size
   for a fixed `k` and unselective filters, where the full scan behind
   `FastPathOrderByLimit` is linear in it. Tail latency under an update-heavy
   workload is reported alongside.
7. The feature is behind a feature flag.

## Out of Scope

- Choosing an ordered structure in dataflows for its order, for example for
  range joins.
- Ordering on expressions. Ordered columns are column references.
- Cost-based index selection. Indexed collections have no statistics, so
  selection is rule-based.
- Making `OFFSET` pagination cheaper. Skipping `n` rows still visits them.
  Keyset pagination is served through range bounds instead.
- Ordered reads from persist beyond what `persist_fast_path_order` provides.
- `SUBSCRIBE`.

## Solution Proposal

### Overview

The proposal has two layers.

1. **A read path that does not depend on how order is stored.** The fast path
   learns to serve `TopK` with group-aware thinning and a `TopK` step in the
   adapter, which removes the dataflow from `DISTINCT ON` queries over existing
   indexes. On top of that, an ordered-access peek mode walks rows in SQL order
   and stops early, with stop rules whose correctness is stated once for any
   representation.
2. **A representation of the order.** Two candidates serve the same read path.
   *Per-batch orderings* keep a permutation of each batch's values in SQL order
   beside an existing index. *Order-key indexes* are new indexes whose key is an
   order-preserving encoding of the ordered columns. The minimal viable
   prototype builds both, and the choice is made on measured memory and tail
   latency (see [Choosing a representation](#choosing-a-representation)).

The delivery order follows the layers. The `TopK` fast path lands first and is
useful on its own. The ordered-access mode and both prototypes follow, then the
chosen representation.

### The `TopK` fast path

`create_fast_path_plan` accepts `[projection] -> TopK -> MFP -> Get`, and the
`IndexedFilter` join form of a lookup, when the `TopK`'s group key is a prefix,
as a set, of the finishing's `ORDER BY`, its offset is zero, and its limit is a
literal. `DISTINCT ON` produces this shape whenever its `ORDER BY` covers the
distinct columns. The plan carries the `TopK` as a post-step.

**Group-aware thinning.** Compute must not thin by rows in this mode, because
the finishing's `limit + offset` counts output rows after the `TopK`, and a
group has many input rows. `PeekScan` takes its bound from
`peek.finishing.num_rows_needed()` today. The `Peek` command carries the group
key, and `PeekScan::thin` thins by groups instead. It orders the buffer by the
finishing's order and keeps every row of the first `limit + offset` groups,
thinning again whenever the buffer has doubled since the last thinning. A group
dropped at some point already had `limit + offset` better groups whose rows are
all kept, so it cannot be among the final first `limit + offset`, and later rows
of it are dropped again. Any group among the global first `limit + offset` that
has rows on a worker is among that worker's first `limit + offset`, so every
worker keeps all of its rows for those groups.

**The `TopK` step.** `create_peek_response_stream` in
`src/adapter/src/coord/peek.rs` applies the `TopK` to the merged rows before
`RowSetFinishing::finish`. It uses the `TopK` operator's comparator
(`compare_columns(order_key, ..., || left.cmp(right))` in
`src/compute/src/render/top_k.rs`).

This removes the dataflow but still scans the whole lookup. The ordered-access
mode removes the scan.

### Ordered access

An ordered access delivers the rows of each *range* in non-decreasing order of
a *tie key* `t(r)`. The tie key is monotone in the query's order: if `r` sorts
at or before `s` under the `ORDER BY`, then `t(r) <= t(s)`. Each representation
defines its ranges and its tie key. A range is conservative. It has to contain
every qualifying row, and the MFP keeps every predicate, including those that
produced the range.

Ranges come from the bound columns. Each combination of literal values for them
is one range, so an `IN` list on a bound column produces one range per value. A
range predicate on the first ordered column narrows a range with a lower and
upper bound. Keyset pagination (`updated_at < $cursor`) and temporal filters,
once `mz_now()` has been resolved and folded to a literal, both produce such
bounds.

The worker accumulates a row's multiplicity at the peek timestamp before
evaluating the MFP, and skips rows whose multiplicity is zero. Ordered access
walks the front of the order, where churn concentrates. Evaluating the MFP on
versions that are not live at the peek timestamp would waste work there and can
fail the peek on an error from such a version.

The stop rules below are what `PeekScan::finishing_satisfied` checks for an
ordered access, so an ordered finishing can end the walk early. The scan stays
budgeted and suspendable, and an ordered read of a small `k` finishes within
the inline budget, without offload.

**Stop rule for rows.** A worker walks each range, applies the MFP, and stops
once at least `limit + offset` rows have passed and the next row's tie key is
larger than the last passing row's. Every unread row then has a tie key larger
than that of every collected row. By monotonicity it sorts strictly after every
collected row, so the collected rows include the first `limit + offset`. The
finishing sorts them with its own comparator and tiebreak and returns exactly
what a full scan returns.

**Stop rule for groups.** With a `TopK` post-step, the worker counts distinct
group keys among passing rows. Once it has seen `limit + offset + 1` groups, it
stops at the next increase of the tie key. As above, every unread row sorts
strictly after every collected row. The `ORDER BY` starts with the group key, so
an unread row belongs to a group no earlier than the latest group seen, and the
earlier groups, at least `limit + offset` of them, are complete. Each nonempty
group yields at least one output row, so they contain the first
`limit + offset` output rows.

**Across workers and ranges.** Each pair of worker and range holds a subset of
the rows. Whichever of the global first `k` rows or groups fall into a subset
are among that subset's first `k`, so the stop rules apply to each subset
independently, and the adapter merges the subsets' results as it does today.

### Representation 1: per-batch orderings over existing indexes

An index's batches are immutable and shared through `ArcBatch`
(`src/row-spine/src/arc_batch.rs`). For a batch, a key, and an ordering, the
worker keeps a permutation of that key's values sorted by the ordering under
`compare_columns`, with the value's `Row` order as the final tiebreak. An
ordered read binds the index's full key by literals, as a lookup does. It seeks
the key in each batch of the trace, walks each batch's permutation, and merges
the batches with a heap under the same order. Equal updates from different
batches compare equal on every column and on the tiebreak, so they arrive
together and their multiplicities accumulate as they do in a cursor. The tie key
is the ordered columns themselves, so tie groups are exact.

- **Building.** Permutations are built on first use and cached per worker. A
  cache entry refers to its batch through a weak handle, so it disappears with
  the batch when a merge replaces it. A cache miss sorts that key's values in
  that batch, and the sort is charged to the scan's fuel, so an expensive miss
  offloads like an expensive walk. Small recent batches miss often and cheaply.
  The largest batches change only when a merge reaches the top of the spine and
  when a replica rehydrates, which is when a miss costs most, up to a sort of a
  key's full history. Hot keys can be prewarmed outside the query path.
- **Memory.** Four bytes per value and ordering, for the keys that queries
  touch, under a budget with eviction and introspection.
- **Coverage.** Any column of any type, including variable-length ones, with
  exact ties. A permutation read backwards serves the reverse order with
  opposite null placement. One index serves any number of orderings, and no
  new SQL is needed.
- **Limits.** The full key of an existing index must be bound, so a global
  `ORDER BY` without a bound key is not served. Serving it needs whole-batch
  orderings (see [Alternatives](#alternatives)). A key's work stays on the
  worker that owns it. At `O(k)` per warm read that is cheap, but misses
  concentrate there.
- **Code.** Compute reads batch storage directly. `OrdValStorage` exposes its
  `keys`, `vals` and `upds` in differential 0.25.1, and the peek path already
  holds the trace's batches (`TraceStorage` in
  `src/compute/src/compute_state/peek_result_iterator.rs`). That storage is
  slated to change. The buffer-managed state design
  (`20260610_buffer_managed_state.md`) moves sealed batches to pageable chunks,
  with compute's row spines ported later. Per-batch orderings would follow that
  port, and a permutation walk visits values out of storage order, so on a
  paged batch it can touch more cold chunks than an in-order walk.

### Representation 2: order-key indexes

An order-key index is an ordinary index whose key is a single fixed-width
`Bytes` datum computed from the declared columns by an encoder that maps SQL
order to byte order.

```sql
CREATE INDEX ticket_labels_project_recent IN CLUSTER serving
ON ticket_labels (project_id, updated_at DESC, ticket_id DESC)
WITH (ORDERED);
```

**Layout.** The key is `[order_key(project_id, updated_at, ticket_id)]`, a call
to an internal variadic function whose parameters carry each column's field
kind, width, direction, and null placement. The encoder is total over the
supported types, so the index cannot produce errors. The key is not a column
reference, so `permutation_for_arrangement` thins nothing and the value is the
full row. `Row` encodes a `Bytes` datum as a length-class tag, a length, and the
payload. Keys of equal width share tag and length, so comparing keys compares
payloads, and dictionary-compressed key containers compare as if decoded (see
`DatumSeq`). Seek targets are padded to the full width, because they compare
length-first too. Arrangements are exchanged by the hash of the key
(`columnar_exchange`), so one project's rows spread across all workers.

**Fields.** The order key concatenates one fixed-width field per column, in
index column order. Order and equality are `Datum`'s, as used by
`compare_columns`.

| Kind   | Contract                        | Applies to                                                                                                   |
|--------|---------------------------------|--------------------------------------------------------------------------------------------------------------|
| exact  | `a < b` iff `e(a) < e(b)`       | `bool`, integer types, `date`, `time`, `timestamp`, `timestamptz`, `interval`, `uuid`, `mz_timestamp`, floats |
| prefix | `a <= b` implies `e(a) <= e(b)` | the last column, when it is `text`, `varchar`, `bytea`, or `numeric`                                         |
| hash   | `a = b` implies `e(a) = e(b)`   | any other column of those four types                                                                         |

- Exact fields reuse the fixed-size encodings persist relies on for ordering.
  `PackedNaiveTime` (8 bytes), `PackedNaiveDateTime`, and `PackedInterval`
  (16 bytes each) are documented to sort like the values they encode. Integers
  are big-endian, with the sign bit flipped for signed types. Floats use the
  usual sign-dependent bit flip after mapping -0 to +0 and every NaN to one
  pattern, because `Datum` treats -0 and +0 as equal and NaN as larger than
  every other value.
- A prefix field holds the first `N` bytes of an exact variable-length
  encoding. Strings and bytes use their bytes, which is how `Datum` compares
  them, padded with zeros. `numeric` uses the sign, then the exponent and digits
  of the reduced value, both inverted for negative values, which pad with a
  byte above every digit so that a shorter digit string sorts after its
  extensions. Truncating an order-preserving encoding is monotone.
- A hash field is 8 bytes computed over a canonical representative of the
  value's equality class. For `numeric` that is the canonicalization
  `OrderedDecimal`'s `Hash` implementation performs, which reduces the value
  and also maps every zero and every NaN to one representative.
- A nullable column's field starts with a flag byte placed according to its
  `NULLS FIRST` or `NULLS LAST`. A `DESC` field inverts its payload bytes. Both
  follow `compare_columns`, where null placement does not depend on the
  direction.

The concatenation is monotone in the lexicographic order of the columns as long
as every ordered field except the last is exact. A later field only orders rows
that agree on every earlier column, so an earlier field must not map distinct
values to equal bytes. This is why only the last column can be a prefix field,
and why hash fields must be bound, never ordered. Other types, such as `jsonb`,
`char`, lists, arrays, and records, are rejected.

**Ranges and tie key.** A range holds the keys whose leading bytes equal the
encoded bound values. Hash collisions put other projects' rows into a range, and
the residual filter rejects them. The tie key is the first `P` bytes of the key,
where `P` covers the bound fields and the fields the `ORDER BY` uses. Within a
range the bound fields are equal, so the tie key is monotone in the order.

**Durability.** The encoded data has no durability obligation. Arrangements are
rebuilt from persist whenever a replica starts, and environmentd and clusterd
refuse to connect across versions (`src/service/src/transport.rs`), so the
binary that builds an arrangement also encodes the seek bounds for it. The
`order_key` function itself is part of LIR, which plan pinning makes durable
(`20260826_pinned_lir.md`). Adding it extends the LIR schema snapshot and the
function registry, which `LIR_VERSION_POLICY` in
`src/compute-types/src/plan.rs` permits without a version bump. Once LIR has
shipped, a change to its parameters or declared properties needs a new
`LIR_VERSION`. A change to the bytes it produces for the same parameters
changes no query result, and the registry's source snapshot reports it for
review.

**Narrow variant.** Storing the full row makes each order-key index a full copy
of the collection. A narrow order-key index instead stores the order key, the
relation's unique key, and the columns the residual filters read, and fetches
the remaining columns from a regular index on the unique key. A peek cannot move
data between workers, so the narrow index is exchanged by the hash of its
unique-key value rather than of its key, which places each row on the worker
that holds it in the regular index. That is sound because nothing joins or
reduces on an `order_key` call, and the index records that it is not
partitioned by its key so that this stays true. The cost is one seek per
returned row, and a choice of stored columns that must anticipate the filters.

### Choosing a representation

|                                    | Per-batch orderings                                   | Order-key index                         | Narrow order-key index                       |
|------------------------------------|-------------------------------------------------------|-----------------------------------------|----------------------------------------------|
| Memory per served ordering         | 4 bytes per value of touched keys, within a budget    | full row plus key, per row              | key, unique key, and filter columns, per row |
| Maintenance                        | none outside reads                                    | one encoder evaluation per update       | as order-key index                           |
| Warm read                          | one seek per batch, `O(k)` heap steps, one worker     | one seek per batch per worker, `O(k)`   | plus one seek per returned row               |
| Cold read                          | sort of the key's values in each uncached batch       | as warm                                 | as warm                                      |
| Orderings                          | any columns and types, exact ties, both directions    | one per index, restricted types         | as order-key index                           |
| Bound columns                      | the full key of an existing index                     | any prefix of the declared columns      | as order-key index                           |
| Global `ORDER BY` without a key    | not served                                            | served                                  | served                                       |
| New SQL                            | none                                                  | index syntax                            | index syntax                                 |
| Stable LIR surface                 | none                                                  | a new scalar function                   | as order-key index                           |
| Main risk                          | tail latency on cold batches, coupling to batch storage through its port to chunks | memory per shape | non-key partitioning invariant |

The prototype decides on four measurements over a wide collection served in
three shapes: memory per shape, warm p50 and p99 latency, p99.9 latency under an
update-heavy workload, and latency of the first reads after a rehydration and
after a merge into the largest batch. Implementation complexity and the
strategic questions below weigh in as well.

### Planner

Under per-batch orderings, an ordered read is an index lookup with an ordering
attached. `LiteralConstraints` already chooses the index, and
`create_fast_path_plan` attaches the ordering from the finishing when the plan
is an `IndexedFilter` lookup, the finishing has a `LIMIT`, and the `ORDER BY`
columns map back through the MFP to columns of the index's collection. No other
plan changes.

Under order-key indexes, the planner has to choose an index that no
predicate refers to. `DataflowBuilder::import_into_dataflow` imports every index
on a collection, and `prune_and_annotate_dataflow_index_imports` later drops
those the optimized plan does not use. A transform therefore runs before
`LiteralConstraints`, in both the fast-path and the physical optimizer, after
view inlining. It sees the finishing through the `TransformCtx`, finds order-key
indexes through the `IndexOracle`, and annotates the `Get` with the ordered
access, which keeps the index through pruning and records its usage for EXPLAIN.
`create_fast_path_plan` turns the annotation into a plan. The transform chooses
an order-key index when the query's `ORDER BY` or `TopK` is served by it, and no
regular index binds a superset of its bound columns or a unique key of the
relation. Equality-bound reads without an `ORDER BY` use it only when no regular
index binds the same columns. Join planning and `LiteralConstraints` never
select an order-key index for a lookup, because no predicate or join key equals
an `order_key` call.

In both cases, EXPLAIN shows the access, roughly:

```
Explained Query (fast path):
  Finish order_by=[#3 desc nulls_first, #0 desc nulls_first] limit=21 output=[#0..=#3]
    →TopK group_by=[#3, #0] limit=1
      →Ordered Index Lookup on materialize.public.ticket_labels (using ticket_labels_project_id_idx)
        Lookup values: ("proj-a")
        Order: #3 desc nulls_first, #0 desc nulls_first
        Filter: ...
```

`IndexUsageType` in `src/repr/src/explain.rs` gains a variant for ordered
access.

### Compute and protocol

`Peek` in `src/compute-client/src/protocol/command.rs` gains a group key for the
`TopK` fast path and an optional ordered access, exclusive with a plain
`literal_constraints` lookup:

```rust
pub enum OrderedAccess {
    /// Per-batch orderings: look up `keys` and walk their values in `order`.
    KeyOrdering { keys: Vec<Row>, order: Vec<ColumnOrder> },
    /// Order-key index: walk `ranges`, with ties decided by the first
    /// `tie_prefix_len` key bytes.
    OrderKeyRanges { ranges: Vec<OrderKeyRange>, tie_prefix_len: usize },
}
```

The stop rule follows from the finishing and the group key. The adapter encodes
order-key bounds by evaluating the index's encoder on the literals, and the
worker treats them as opaque bytes. `PeekResultIterator` gains the access modes
and accumulates multiplicities before evaluating the MFP. `PeekScan` gains the
stop rules in `finishing_satisfied` and group-aware `thin`. The inline and
offload drivers, the stash (which the same walk feeds), the result size limit,
and the error walk are unchanged.

### Costs

- **Memory.** Per-batch orderings cost their cache budget. An order-key index
  costs the full row plus the key per row, plus per-key offsets, because every
  key is unique. A regular index keyed by `project_id` thins `project_id` out of
  its values, so an order-key index is larger than it by more than the key
  width. Each served shape, a combination of bound columns and ordering, needs
  its own order-key index. For wide collections with fine grain and many shapes
  this is the dominant cost, which the narrow variant reduces.
- **Results.** Each worker returns up to twice `limit + offset` rows, or groups,
  plus ties, the same bound today's thinning applies.
- **Seeks.** A seek performs a search in each batch of the spine, and each step
  merges across batches, which is negligible at small `k`.
- **Churn.** Ordering by update time puts the most frequently updated rows at
  the front of the order, where reads walk. Versions that are not live at the
  peek timestamp are skipped before the MFP, but they are still visited until
  merging consolidates them.

### Implementation touch points

Shared:

- `src/adapter/src/coord/peek.rs`: `FastPathPlan`, the `TopK` matcher and
  post-step, `create_peek_response_stream`, EXPLAIN rendering, `used_indexes`.
- `src/adapter/src/frontend_peek.rs`, `src/adapter/src/peek_client.rs`: the
  frontend peek sequencing, which also matches on `FastPathPlan` and forwards
  the finishing to compute.
- `src/compute-client/src/protocol/command.rs`: the `Peek` fields.
- `src/compute/src/compute_state/peek_scan.rs`: group-aware thinning and the
  stop rules.
- `src/compute/src/compute_state/peek_result_iterator.rs`: the access modes,
  and multiplicity before MFP.
- `src/repr/src/explain.rs`: `IndexUsageType`.
- `src/sql/src/session/vars/definitions.rs`: the feature flag.

Per-batch orderings:

- `src/compute/src/compute_state/`: the permutation cache, its budget, and its
  introspection.

Order-key indexes:

- `src/expr/src/scalar/func/variadic.rs`: the `order_key` function, which also
  extends the stable LIR schema and the function registry
  (`src/compute-types/tests/snapshots/lir_v1.json` and `func_registry.json`).
  The field encodings live next to the `Packed*` types in `src/repr/src/adt/`.
- `src/sql-parser/src/ast/defs/statement.rs`, `src/sql-parser/src/parser.rs`,
  `src/sql/src/plan/statement/ddl.rs`: syntax, validation, and the key
  expression.
- `src/transform/`: the access transform, and the finishing in `TransformCtx`.
- `src/compute/src/render/context.rs`: exchange by unique key, for the narrow
  variant only.

### Testing

- SLT under the feature flag. EXPLAIN shows the `TopK` fast path and ordered
  access when the matcher's conditions hold, and falls back when one does not.
  Each query is compared against the same query without the fast path, covering
  ties at the limit, NULLs, both directions, `IN` lists, `DISTINCT ON` over
  fan-out, residual filters that reject most rows, and offsets.
- A regression test for an MFP error on a version that is not live at the peek
  timestamp, which must not fail the peek.
- Testdrive: ordered reads under inserts, updates, and retractions, at several
  timestamps.
- Per-batch orderings: reads that span merges and rehydration, eviction under a
  small budget, and agreement with a cursor walk over randomized traces.
- Order-key indexes: property tests per supported type over
  `arb_datum_for_scalar`, checking each field's contract against
  `compare_columns` for both directions and null placements, equal hashes for
  equal values, and monotonicity of truncation. Hash collisions forced by a
  test-only hash width.
- Feature benchmark: ordered variants of `FastPathOrderByLimit` as described in
  [Choosing a representation](#choosing-a-representation).

### Observability

The existing `mz_index_peek_*` histograms cover ordered reads.
`mz_index_peek_row_iteration_rows` is the acceptance signal: for ordered reads
it should stay close to the number of rows each worker returns. Ordered reads of
a small `k` should end on the `inline` substrate of `mz_index_peek_walks_total`,
and offloading `TopK` reads should stop adding to
`mz_index_peek_permit_queue_depth`. A counter of peeks by access path separates
the populations. Per-batch orderings add cache hits, misses, build time, and
resident bytes.

## Strategic considerations

The representation choice is also a choice of direction on five questions that
reach beyond this design.

**Physical design in SQL.** Order-key indexes add syntax that declares a
physical layout: an `ORDERED` option, per-column directions, and possibly field
widths. Per-batch orderings add none, because orderings are derived from the
reads the system serves, within a budget. Materialize asks users to choose
arrangement keys with `CREATE INDEX`, advises on them
(`mz_internal.mz_index_advice`), and accepts hints such as
`DISTINCT ON INPUT GROUP SIZE`. Should serving performance keep being expressed
through declared structures, or should new read paths derive their structures
from the workload? A declared structure has predictable latency and memory. A
derived one serves shapes nobody declared, at the cost of tail latency while its
state is cold.

**Growing the fast path.** The fast path is a set of hand-matched shapes:
constants, index scans and lookups, and persist reads with a limit or an order.
This design adds a `TopK` step, group-aware thinning, and ordered access. Each
shape touches the matcher, EXPLAIN, index usage accounting, both peek sequencing
paths, the compute protocol, the scan, and the stash path, as the touch points
above show. Continuing this way multiplies shapes. The alternative is a small
plan language for one-shot reads that the worker executes without a dataflow:
an access (scan, lookup, ordered), an MFP, an optional `TopK`, and a limit. This
design's protocol additions are the first operators such a plan would have.
Should they land as the start of a peek plan, or should that generalization wait
until the set of operators is known?

**Arrangements versus purpose-built structures.** Order-key indexes reuse
arrangements unchanged, and pay with a copy of the data per shape. Per-batch
orderings attach a structure to arrangement batches, with its own lifecycle and
memory accounting, and pay with complexity and coupling to batch storage. A
separate serving store would reimplement times, compaction, frontiers, and
sharing (see [Alternatives](#alternatives)). Once structures attached to
immutable batches exist, they are the natural home for further read
accelerators, such as data-skipping filters for selective residual predicates.
Is attaching read-side structures to batches a direction we want, or should
arrangements stay the only structure compute maintains?

**Read isolation.** An index peek's walk is one budgeted, suspendable scan that
leaves the timely worker once it is measured expensive
(MaterializeInc/materialize#38508, MaterializeInc/materialize#38509,
MaterializeInc/materialize#39123), building on `Arc`-backed batches
(MaterializeInc/materialize#38396). Offload addresses the stall a long walk
caused. It does not address the walk's cost, which this design removes, and it
bounds concurrent expensive walks with permits, which ordered reads stop
consuming. Both representations plug into the scan rather than beside it, and
the open design for one execution path for index peeks
(MaterializeInc/materialize#38449) is where the access modes would be
described.

**Larger-than-memory state.** The buffer-managed state design
(`20260610_buffer_managed_state.md`) moves sealed batches toward pageable chunks
under one memory budget. Under that direction, an order-key index's per-shape
copy costs pool budget and disk rather than resident memory, and an ordered walk
over it reads its chunks in order. Per-batch orderings stay small, but read
values out of storage order and depend on batch storage that the port to chunks
will change. Does the move to buffer-managed spines shift the memory argument
enough to favor copies, or does the coupling argument then favor them as well?

## Minimal Viable Prototype

The `TopK` fast path, with group-aware thinning and multiplicity before MFP, is
a self-contained change and lands first. It serves `DISTINCT ON` queries over
existing indexes without a dataflow.

The prototype of ordered access then implements the shared mode with the row
and group stop rules, a single equality range, and both representations in
their simplest form: per-batch orderings without a budget or prewarming, and
order-key indexes with exact and hash fields only. Both serve the example query
over a wide collection with fan-out, at `10^6` and `10^7` rows, with a project
of `10^5` rows. The measurements are those in
[Choosing a representation](#choosing-a-representation), each with a static and
an update-heavy variant, compared against today's lookup with thinning. Prefix
fields, the narrow variant, `IN` lists, and range bounds follow for the chosen
representation.

## Alternatives

### Other representations of the order

**Whole-batch orderings, built with each batch.** Build a permutation of a
batch's entire contents, in the order of the bound and ordered columns, whenever
a batch is built or merged (`ArcBuilder::done` and `ArcMerger::done` in
`src/row-spine/src/arc_batch.rs`). A merged batch's permutation can be produced
by merging its inputs' permutations. Because the ordering covers the whole
batch, it serves a global `ORDER BY` without a bound key, and the index can be
keyed by a unique key so that a project spreads across workers. The cost is a
sort or merge on every batch build on the maintenance path, a fallback scan for
a batch whose permutation is not ready, and a declared ordering, since building
eagerly requires knowing it in advance. It is the eager form of per-batch
orderings and the natural next step if lazy building misses too often.

**An exact variable-length order key in a lexicographic arrangement.** Encode
every column exactly, with escaped and terminated strings, and store the key in
a container that compares bytes lexicographically rather than length-first. This
lifts the order-key restrictions on variable-length columns and makes ties
exact. It needs a new arrangement flavor through
`src/compute/src/render/context.rs`, the trace bundles in `compute_state`, the
row spine, and arrangement logging. It keeps the memory cost of order-key
indexes and is an upgrade path for them rather than an alternative to the
choice above.

**A separate multiversioned ordered structure.** Maintain a B-tree or skip list
per worker beside, or instead of, an arrangement. To serve reads at a timestamp
it has to hold times and diffs, compact with the arrangement's frontier, and be
shared with readers, which is the arrangement reimplemented. Stripped of those,
it becomes one of the representations above.

**Comparing datums in arrangements.** Give arrangements a key type whose `Ord`
decodes datums, or make `Row` compare in datum order. Either makes every
comparison in batch merging decode datums. Changing `Row` changes the order of
every arrangement to benefit the few whose order matters, and a per-index key
type needs a new arrangement flavor as well as a way to pass column orders to
`Ord`.

### Pruning without a sorted structure

**Per-batch statistics on time-correlated columns.** Newer batches hold newer
updates, and `updated_at` tracks update time. A maximum per key and batch would
let a read visit recent batches first and skip older ones. The correlation does
not survive the events that matter for latency. Rehydration builds the
collection into a few large batches, and each merge into the largest batch mixes
old and new updates, after which a read scans the key's full share again. The
gain also applies only to orderings that track update time.

**Faster scans.** Thinning already partitions instead of sorting. Decoding only
the columns the filters read would further help every fast-path read, including
ordered access whose filters reject most of a range, and is worth doing
regardless. It leaves the cost of reading a large key proportional to its size
on every query.

### Moving work out of the read

**Maintained prefixes.** A maintained `TopK` of the first `M` rows per project,
with a fallback to the full query, works in SQL today. Pagination soon runs past
the maintained prefix, and every filter combination needs its own prefix.

**Standing dataflows over a request collection.** Write request parameters into
a collection and maintain a lateral `TopK` per request. Each request pays a
write's latency, still joins every row of the project, and keeps being
maintained after it has been read.

**Partial, demand-driven state.** Keep state only for the keys and parameters
that reads touch. That needs eviction and recomputation on demand, which
differential dataflow's frontier model lacks, and runtime filter values
multiply the keys.

### Partitioning and storage

**Partitioning by the bound columns.** Exchange an order-key index by the hash
of its bound columns, so each project lives on one worker. A large project then
concentrates its reads and its maintenance on that worker. Hashing the full key
spreads it, and each worker stops after at most `k` rows.

**Range partitioning.** Assign workers ranges of the order. New rows all sort to
the front of an update-time order, so every insert lands on one worker.

**Ordered reads from persist.** Persist parts carry statistics and can be read
in order for some types. Blob reads add latency an interactive read cannot
afford, and the ordering is ascending on a prefix of the relation's columns.

**Reverse cursors.** Serve both directions from one order-key index by walking
batches backwards. Differential's `Cursor` has no backward steps, so this needs
a reverse cursor over the batch storage, merged across batches. Per-batch
orderings get both directions without it.

## Open questions

- Which representation, decided by the prototype's measurements.
- Syntax, if order-key indexes or declared orderings are chosen.
  `WITH (ORDERED)` keeps Postgres's key-part syntax. `USING btree` would be
  familiar, but Postgres users would expect it to serve joins and both
  directions. The form
  `ON ticket_labels (project_id) WITH (ORDER BY (updated_at DESC, ticket_id DESC))`
  separates the lookup key from the orderings and can back either
  representation.
- For per-batch orderings: the budget policy, eviction, prewarming, and how the
  cache appears in introspection next to arrangement sizes.
- For order-key indexes: the default width `N` of prefix fields, an exact
  fixed-width `numeric` encoding (precision is bounded at 39 digits), and how
  `mz_index_columns` presents the declared columns rather than the `order_key`
  expression.
- How the finishing reaches the access transform, which today only sees MIR.
