# Ordered reads from indexes

- Associated: TBD

## The Problem

Operational applications page through the newest, or otherwise first, rows of
a large collection. Take an issue tracker that lists a project's open tickets
carrying any of a set of labels, most recently updated first:

```sql
SELECT DISTINCT ticket_id, title, assignee, updated_at
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
twice, and `DISTINCT` collapses it back to one row per ticket. A semi-join
against the labels instead would make the query a join, which the fast path does
not serve. Aggregating each ticket's labels into a list avoids the fan-out but
costs more to maintain.

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
order. Values within a key are sorted the same way. No access path can stop
after the first `k` rows in SQL order.

**Fast-path peeks scan everything they could return.** A fast-path peek answers
a `SELECT` from an existing index without building a dataflow. Each worker walks
its share of the index and applies the query's map-filter-project (MFP), and the
adapter applies the *finishing*: the `ORDER BY`, `LIMIT`, `OFFSET`, and final
projection. For a finishing with an `ORDER BY` and a `LIMIT`, each worker walks
its entire share of the index, or the entire result of a literal-constraint
lookup, and partitions a buffer down to `limit + offset` rows whenever the
buffer doubles (`PeekScan::thin` in
`src/compute/src/compute_state/peek_scan.rs`). The walk cannot end early:
`PeekScan::finishing_satisfied` is never true for an ordered finishing, because
no prefix of the walk ranks rows against the whole trace. Network and memory are
bounded by `k`, but CPU is proportional to the collection or lookup size on
every query. A lookup key's rows all hash to one worker, so a large project
concentrates that cost on a single worker. A walk that outruns
`compute_index_peek_inline_budget` cursor positions finishes on a blocking pool
under a permit (`enable_compute_index_peek_offload`, see
`src/compute/src/compute_state/peek_offload.rs`), so it does not stall the
timely worker. It still costs its full CPU, and offloaded walks queue for a
bounded number of permits (`mz_index_peek_permit_queue_depth`), so a burst of
large ordered reads delays every other expensive peek on the replica. The gap
between `mz_index_peek_row_iteration_rows` and the rows a peek returns measures
the waste.

**Deduplication leaves the fast path entirely.** `SELECT DISTINCT` plans as a
`Reduce` without aggregates, and `DISTINCT ON` as a `TopK` whose group key holds
the distinct columns and whose limit is 1 (`src/sql/src/plan/query.rs`).
`create_fast_path_plan` in `src/adapter/src/coord/peek.rs` serves neither,
apart from a `TopK` with an empty group key that the finishing subsumes. Every
execution of a query like the one above therefore builds a one-off dataflow,
arranges every row of the lookup, and tears the dataflow down again.

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

1. For a query that binds an index's leading columns by equality, orders by
   other columns, and has a `LIMIT k`, each worker visits `O(k)` rows plus the
   rows the residual filters reject before `k` rows pass, instead of all rows
   of the lookup.
2. A `SELECT DISTINCT`, a `DISTINCT ON`, or another `TopK` whose groups line up
   with the query's `ORDER BY` is served from the fast path without building a
   dataflow, with or without an ordered read.
3. Results are identical to the existing paths', up to the row a `DISTINCT ON`
   group keeps where its order leaves a choice, which SQL leaves unspecified.
   Errors in rows an early stop never visits are not raised, as for a
   fast-path `LIMIT` without an `ORDER BY` today.
4. No query visits more rows than it does today. Plans change only for queries
   that an ordered read or the `TopK` improvement serves, and for reads of a
   collection that no regular index serves as well as an order-key index.
5. The memory cost of serving several shapes over one wide collection is
   measured and stated.
6. In the feature benchmark, ordered-read latency is flat in the lookup size for
   a fixed `k` and unselective filters, where today's walk is linear in it. Tail
   latency under an update-heavy workload is reported alongside.
7. Each part is behind a feature flag.

## Out of Scope

- Choosing an ordered structure in dataflows for its order, for example for
  range joins.
- Ordering on expressions. Ordered columns are column references.
- Cost-based index selection. Indexed collections have no statistics, so
  selection follows a fixed precedence (see [Planner](#planner)).
- Making `OFFSET` pagination cheaper. Skipping `n` rows still visits them.
  Keyset pagination is served through range bounds instead.
- `SUBSCRIBE`.

## Solution Proposal

### Overview

An *ordered read* walks an index in SQL order, within the range its bound
columns select, and stops once the result is determined. It is a mode of the
existing index peek scan, and its correctness rests on a stop rule.

The order lives in *order-key indexes*: indexes whose key is an order-preserving
encoding of the declared columns, so that each range of bound values is sorted
in SQL order.

*Fast-path improvements* widen the shapes an ordered read serves: `TopK` for
deduplication, `IN` lists, range bounds, reads without a `LIMIT`, reverse
cursors, and exact variable-length keys. All but `TopK` build on ordered reads.
[Query shapes](#query-shapes) lists the improvements each shape needs.

### Ordered reads

An ordered read binds zero or more of an index's leading columns to one
combination of literal values, which selects a *range* of the index, and
delivers the range's rows in non-decreasing order of a *tie key* `t(r)`. The tie
key is monotone in the query's order: if `r` sorts at or before `s` under the
`ORDER BY`, then `t(r) <= t(s)`. The index's layout defines the ranges and the
tie key (see [Order-key indexes](#order-key-indexes)). A range is conservative.
It has to contain every qualifying row, and the MFP keeps every predicate,
including those that produced the range.

The worker accumulates a row's multiplicity at the peek timestamp before
evaluating the MFP, and skips rows whose multiplicity is zero. Ordered reads
walk the front of the order, where churn concentrates. Evaluating the MFP on
versions that are not live at the peek timestamp would waste work there and can
fail the peek on an error from such a version.

The stop rule below is what `PeekScan::finishing_satisfied` checks for an
ordered read, so an ordered finishing can end the walk early. The scan stays
budgeted and suspendable, and an ordered read of a small `k` usually finishes
within the inline budget, without offload. Rows past the stop are never visited,
so errors in them are not raised.

**Stop rule.** A worker walks its range, applies the MFP, and stops once at
least `limit + offset` rows have passed and the next row's tie key is larger
than the last passing row's. Every unread row then has a tie key larger than
that of every collected row. By monotonicity it sorts strictly after every
collected row, so the collected rows include the first `limit + offset`. Sorted
with the finishing's comparator and tiebreak, they give exactly what a full scan
gives.

**Across workers.** Each worker holds a subset of the range's rows. Whichever of
the global first `k` rows fall on a worker are among that worker's first `k`, so
the stop rule applies to each worker independently, and the adapter merges the
workers' results as it does today.

### Order-key indexes

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
supported types, so the index cannot produce errors. `Row` encodes a `Bytes`
datum as a length-class tag, a length, and the payload. Keys of equal width
share tag and length, so comparing keys compares payloads, and
dictionary-compressed key containers compare as if decoded (see `DatumSeq`).
Seek targets are padded to the full width, because they compare length-first
too. Arrangements are exchanged by the hash of the key (`columnar_exchange`),
so one project's rows spread across all workers. Hashing only the bound columns
would put a large project on one worker, and assigning workers ranges of the
order would put every new row on one worker.

The key is not a column reference, so `permutation_for_arrangement` removes no
column from the value, which holds the full row. An order-key index therefore
serves its collection like any index: full scans, index exports, and sinks can
read it, and it can replace a regular index on its bound columns instead of
sitting beside one.

**Fields.** The order key concatenates one fixed-width field per column, in
index column order. Order and equality are `Datum`'s, as used by
`compare_columns`.

| Kind   | Contract                        | Applies to                                                                                                   |
|--------|---------------------------------|--------------------------------------------------------------------------------------------------------------|
| exact  | `a < b` iff `e(a) < e(b)`       | `bool`, integer types, `date`, `time`, `timestamp`, `timestamptz`, `interval`, `uuid`, `mz_timestamp`, floats |
| prefix | `a <= b` implies `e(a) <= e(b)` | the last column, when it is `text`, `varchar`, `bytea`, or `numeric`                                         |
| hash   | `a = b` implies `e(a) = e(b)`   | any other column of those four types                                                                         |

- Exact fields reuse the fixed-size encodings persist relies on for ordering.
  `PackedNaiveTime` (8 bytes), `PackedNaiveDateTime`, and `PackedInterval` (16
  bytes each) are documented to sort like the values they encode. Integers are
  big-endian, with the sign bit flipped for signed types. Floats use the usual
  sign-dependent bit flip after mapping -0 to +0 and every NaN to one pattern,
  because `Datum` treats -0 and +0 as equal and NaN as larger than every other
  value.
- A prefix field holds the first `N` bytes of an exact variable-length encoding.
  Strings and bytes use their bytes, which is how `Datum` compares them, padded
  with zeros. `numeric` starts with a class byte that orders negative values,
  zero, positive values, and NaN. A nonzero value continues with its adjusted
  exponent, the position of its most significant digit, and then the digits of
  the reduced value. Both are inverted for negative values, which pad with a
  byte above every digit so that a digit string sorts after every longer string
  it begins. Truncating an order-preserving encoding is monotone.
- A hash field is 8 bytes computed over a canonical representative of the
  value's equality class. For `numeric` that is the canonicalization
  `OrderedDecimal`'s `Hash` implementation performs, which reduces the value and
  also maps every zero and every NaN to one representative.
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

**Narrow variant.** Storing the full row makes each order-key index a copy of
the collection. A narrow order-key index instead stores the order key, the
relation's unique key, and the columns the residual filters read, and fetches
the remaining columns from a regular index on the unique key. A peek cannot move
data between workers, so the narrow index is exchanged by the hash of its
unique-key value rather than of its key, which places each row on the worker
that holds it in the regular index. That is sound because nothing joins or
reduces on an `order_key` call, and the index records that it is not
partitioned by its key so that this stays true. A narrow index lacks columns,
so it never serves its collection for full scans, index exports, or sinks. A
read through it touches two indexes, so the `Peek` command names both and the
peek holds both. The costs are one seek per returned row, and a choice of
stored columns that must anticipate the filters.

### Fast-path improvements

#### `TopK`

`create_fast_path_plan` accepts `[projection] -> TopK -> MFP -> Get`, and the
`IndexedFilter` join form of a lookup, which is how `LiteralConstraints`
expresses a lookup: a join of the `Get` with the literal keys. The `TopK` must
have an offset of zero and a literal limit, and the finishing must have an
`ORDER BY` that the `TopK`'s groups line up with: the group key is a prefix of
the `ORDER BY`, as a set, or contains every one of its columns. `DISTINCT ON`
produces the first form whenever its `ORDER BY` covers the distinct columns. A
`SELECT DISTINCT` is a `Reduce` without aggregates, which the matcher treats as
a `TopK` that keeps one row per group of all its columns. Its `ORDER BY` columns
must appear in the select list, so it produces the second form. The plan carries
the `TopK` as a post-step.

**Group-aware thinning.** Compute must not thin by rows in this mode, because
the finishing's `limit + offset` counts output rows after the `TopK`, and a
group has many input rows. `PeekScan` takes its bound from
`peek.finishing.num_rows_needed()` today. The `Peek` command carries the group
key, and `PeekScan::thin` thins by groups instead. It orders the buffer by the
finishing's order and keeps every row of the first `limit + offset` groups,
thinning again whenever the buffer has doubled since the last thinning. Groups
are identified as the `TopK` operator identifies them, by the bytes of their
key, and thinning keeps or drops whole classes of groups whose keys compare
equal, since such groups interleave in the finishing's order. A group dropped
at some point already had `limit + offset` better groups whose rows are all
kept, so it cannot be among the final first `limit + offset`, and later rows of
it are dropped again. Any group among the global first `limit + offset` that
has rows on a worker is among that worker's first `limit + offset`, so every
worker keeps all of its rows for those groups.

**The `TopK` on the worker.** Within each kept group, the worker keeps only the
`TopK`'s first `limit` rows, under the `TopK` operator's comparator
(`compare_columns(order_key, ..., || left.cmp(right))` in
`src/compute/src/render/top_k.rs`). A group's global first rows are among each
worker's first rows of that group, so this is sound. Each worker then returns
at most the `TopK`'s limit rows per group, for at most `limit + offset` groups,
or for every group without a finishing `LIMIT`. Without it, a deduplicating
query over heavy fan-out would ship every fan-out row and could exceed
`max_result_size` where today's deduplicated dataflow result fits.

**Stop rule for groups.** In an ordered read, the worker counts distinct group
keys among passing rows. Once it has seen `limit + offset + 1` groups, it stops
at the next increase of the tie key. As for rows, every unread row sorts
strictly after every collected row. If the `ORDER BY` starts with the group key,
an unread row belongs to a group no earlier than the latest group seen. If the
group key contains every `ORDER BY` column, a group's rows share their tie key,
so an unread row belongs to a group not yet seen. Either way the earlier groups,
at least `limit + offset` of them, are complete. Each nonempty group yields at
least one output row, so they contain the first `limit + offset` output rows.

**The `TopK` step.** `create_peek_response_stream` in
`src/adapter/src/coord/peek.rs` applies the `TopK` to the merged rows before
`RowSetFinishing::finish`, with the same comparator.

This improvement also applies to a lookup on a regular index without an ordered
read. That removes the dataflow from deduplicating queries but still walks the
whole lookup, so it can land before ordered reads.

#### `IN` lists

An `IN` list on a bound column produces one range per value, and a worker walks
each range separately. Whichever of the global first `k` rows or groups fall
into a range are among that range's first `k`, so the stop rules apply to each
range independently, and the worker combines the ranges' rows into its answer.

#### Range bounds

A range predicate on the first ordered column narrows a range with a lower and
upper bound, so the walk starts at the bound rather than at the front of the
order. The column must stand alone on one side of the comparison, with a literal
on the other. Keyset pagination (`updated_at < $cursor`) produces such a bound,
and so do temporal filters of the form `mz_now() <= updated_at` once `mz_now()`
has been resolved and folded to a literal. A predicate such as
`mz_now() <= updated_at + INTERVAL '1 day'` stays a residual filter, so a read
whose window holds fewer than `k` rows walks past the window to the end of the
range.

Keyset pagination with a tiebreaker compares rows,
`(updated_at, ticket_id) < ($u, $t)`, or spells the comparison out as
`updated_at < $u OR (updated_at = $u AND ticket_id < $t)`. The planner derives
`updated_at <= $u` from either form, since each implies it, bounds the range by
that, and leaves the rest to the residual filter.

#### Reads without a `LIMIT`

This improvement lets the planner choose an ordered read for a finishing without
a `LIMIT`. The stop rule then never fires, and the read visits every row of its
range. It still saves work. A worker builds its answer with
`RowCollection::new` (`src/expr/src/row/collection.rs`), which sorts every row
it collected with `RowComparator::compare_rows`, partially decoding both rows on
each comparison. The adapter only merges the workers' sorted runs
(`RowCollection::merge_sorted`), and `RowSetFinishing::finish` does not sort
again. An ordered read delivers rows sorted up to ties of its tie key, so the
worker sorts only each run of equal tie keys, with the finishing's comparator
and tiebreak, and builds its answer without the full sort. `PeekResponse::Rows`
already carries a list of sorted runs, so each range of an `IN` list can be its
own run.

The walk visits the rows a lookup would, so skipping the sort is a pure saving.
Together with range bounds, the improvement also serves range-only reads
without an `ORDER BY`, which then walk only the range.

#### Reverse cursors

An order-key index serves the direction and null placement it declares. Walking
it backwards serves the reverse of both, so one index covers `ORDER BY a` and
`ORDER BY a DESC` alike. Differential's `Cursor` has no backward steps, so this
needs a reverse cursor over the batch storage, merged across batches.

#### Exact variable-length keys

Encode every column exactly, with escaped and terminated strings, and store the
key in a container that compares bytes lexicographically rather than
length-first. This lifts the restriction that only the last column can be
variable-length and ordered, and makes ties exact. It needs a new arrangement
flavor through `src/compute/src/render/context.rs`, the trace bundles in
`compute_state`, the row spine, and arrangement logging.

### Planner

Indexed collections have no statistics, so the planner picks the best index for
a read by what each candidate guarantees. Between two candidates, the first rule
that separates them decides:

1. Binding a unique key of the relation wins, since it returns at most one row
   per value.
2. Binding a strict superset of the other's bound columns wins, since it visits
   a subset of the other's rows.
3. With equal bound columns, an ordered read that serves the `ORDER BY` or a
   range bound wins, since it visits a prefix of the rows the other visits. It
   needs a `LIMIT` or the [reads without a `LIMIT`](#reads-without-a-limit)
   improvement.
4. Otherwise a regular index wins, so a query that no ordered read improves
   keeps its plan.

An order-key index is also a candidate for plain lookups and full scans, and
wins them when no regular index binds as much. No query therefore visits more
rows than it does today.

A transform makes this choice before `LiteralConstraints`, so that it decides
before `LiteralConstraints` turns equality predicates into a lookup on a regular
index. It runs in both the fast-path and the physical optimizer, after view
inlining. `DataflowBuilder::import_into_dataflow` imports every index on a
collection, and `prune_and_annotate_dataflow_index_imports` later drops those
the optimized plan does not use. The transform annotates the `Get` with the
chosen ordered read, `LiteralConstraints` leaves an annotated `Get` alone, and
`CollectIndexRequests` in `src/transform/src/dataflow.rs` keeps the
annotation, so the index survives pruning and EXPLAIN reports its use. The
transform needs the finishing, which the peek optimizer passes in through the
`TransformCtx`. `create_fast_path_plan` turns the annotation into a plan. Join
planning and `LiteralConstraints` never select an order-key index for a join or
a regular lookup, because no predicate or join key equals an `order_key` call.

An order-key index serves a query whose plan has the shape
`[MFP] -> [TopK] -> MFP -> Get` when:

1. The finishing's columns and the `TopK`'s columns map back through the MFPs to
   columns of the `Get`. Columns computed by an MFP do not match.
2. Zero or more leading declared columns are each bound by an equality against
   literals in the inner MFP's filters, or, with the [`IN` lists](#in-lists)
   improvement, by an `IN` list, and every hash field is among them.
3. With bound columns removed from the `ORDER BY`, the remaining `ORDER BY`
   agrees with the index's next columns in column, direction, and null
   placement, up to the shorter of the two. Null placement is ignored for a
   column that is not nullable. With the [reverse cursors](#reverse-cursors)
   improvement, the `ORDER BY` may instead agree with all of them reversed.
4. A `TopK`, if any, meets the conditions of the [`TopK`](#topk) improvement.
5. The outer MFP, if any, is a projection, which folds into the finishing's
   projection.

EXPLAIN shows the ordered read, roughly:

```
Explained Query (fast path):
  Finish order_by=[#3 desc nulls_first, #0 desc nulls_first] limit=21 output=[#0..=#3]
    →Distinct project=[#0..=#3]
      →Ordered Index Scan on materialize.public.ticket_labels (using ticket_labels_project_recent)
        Ranges: (project_id = "proj-a")
        Filter: ...
```

`IndexUsageType` in `src/repr/src/explain.rs` gains a variant for ordered
reads.

### SQL and catalog

Index key parts accept `ASC` or `DESC` and `NULLS FIRST` or `NULLS LAST`, which
are valid only with the new index option `ORDERED` (`IndexOptionName` in
`src/sql-parser/src/ast/defs/statement.rs`). `plan_create_index` in
`src/sql/src/plan/statement/ddl.rs` chooses each column's field kind from its
type and position, builds the `order_key` key expression, and rejects
unsupported types and key parts that are not column references. The catalog
stores `create_sql` and plans it again on startup, so order-key indexes need no
durable catalog change, and `SHOW CREATE INDEX` returns the user's syntax.

Two feature flags gate the work, off in production and on in CI:
`enable_fast_path_topk` for the `TopK` improvement, which can land alone, and
`enable_ordered_indexes` for order-key indexes and ordered reads.

### Compute and protocol

`Peek` in `src/compute-client/src/protocol/command.rs` gains a group key for the
`TopK` improvement and an optional `OrderedAccess` for an ordered read,
exclusive with a plain `literal_constraints` lookup:

```rust
pub struct OrderedAccess {
    /// Ranges of the order-key index to walk, as encoded key bytes.
    pub ranges: Vec<OrderKeyRange>,
    /// The number of leading key bytes that decide ties for the stop rule.
    pub tie_prefix_len: usize,
    /// Whether to walk the ranges backwards, with reverse cursors.
    pub reverse: bool,
}
```

The stop rule follows from the finishing and the group key. The adapter encodes
range bounds by evaluating the index's encoder on the literals, and the worker
treats them as opaque bytes. `PeekResultIterator` gains the range walk and
accumulates multiplicities before evaluating the MFP. `PeekScan` gains the stop
rules in `finishing_satisfied`, and group-aware thinning with the per-group
`TopK` in `thin`. For reads without a `LIMIT`, `rows_response` in
`peek_scan.rs` builds its `RowCollection` from rows that are already sorted,
sorting only runs of equal tie keys. The inline and offload drivers, the stash
(which the same walk feeds), the result size limit, and the error walk are
unchanged.

### Costs

- **Memory.** An order-key index costs the full row plus the key per row, plus
  per-key offsets, because nearly every key is distinct. A regular index keyed
  by `project_id` omits `project_id` from its values, so an order-key index is
  larger than it by more than the key width. Each served shape, a combination
  of bound columns and ordering, needs its own order-key index unless one index
  serves several. For wide collections with fine grain and many shapes this is
  the dominant cost, which the narrow variant reduces.
- **Maintenance.** Each order-key index is another arrangement: it hydrates and
  merges like any index, and evaluates the encoder once per update.
- **Results.** With an ordered read, each worker returns about `limit + offset`
  rows or groups, ties included. Without one, thinning keeps up to twice that,
  as today. The per-group `TopK` bounds each group to the `TopK`'s limit.
- **Seeks.** A seek performs a search in each batch of the spine, and each step
  merges across batches, which is negligible at small `k`.
- **Churn.** Ordering by update time puts the most frequently updated rows at
  the front of the order, where reads walk. Versions that are not live at the
  peek timestamp are skipped before the MFP, but they are still visited until
  merging consolidates them.
- **Multiplicity before the MFP.** Every index peek, not only ordered reads,
  then accumulates times and diffs for every row it visits, where today it does
  so only for rows the MFP keeps.

### Implementation touch points

- `src/adapter/src/coord/peek.rs`: `FastPathPlan`, the `TopK` matcher and
  post-step, `create_peek_response_stream`, EXPLAIN rendering, `used_indexes`.
- `src/adapter/src/frontend_peek.rs`, `src/adapter/src/peek_client.rs`: the
  frontend peek sequencing, which also matches on `FastPathPlan` and forwards
  the finishing to compute.
- `src/adapter/src/optimize/peek.rs`: passing the finishing into the
  `TransformCtx`.
- `src/transform/`: the ordered-read transform, the finishing in `TransformCtx`,
  and `CollectIndexRequests` and `choose_index` in `dataflow.rs`, which keep the
  annotation and exclude narrow indexes from full scans, index exports, and
  sinks.
- `src/compute-client/src/protocol/command.rs`: the `Peek` fields.
- `src/compute/src/compute_state/peek_scan.rs`: group-aware thinning, the
  per-group `TopK`, the stop rules, and answers built from sorted rows.
- `src/compute/src/compute_state/peek_result_iterator.rs`: the range walk, in
  reverse with reverse cursors, and multiplicity before MFP.
- `src/expr/src/row/collection.rs`: a `RowCollection` constructor for rows that
  arrive sorted.
- `src/expr/src/scalar/func/variadic.rs`: the `order_key` function, which also
  extends the stable LIR schema and the function registry
  (`src/compute-types/tests/snapshots/lir_v1.json`, `func_registry.json`,
  `func_registry_source.json`, and `func_registry_digests.json`). The field
  encodings live next to the `Packed*` types in `src/repr/src/adt/`.
- `src/sql-parser/src/ast/defs/statement.rs`, `src/sql-parser/src/parser.rs`,
  `src/sql/src/plan/statement/ddl.rs`: syntax, validation, and the key
  expression.
- `src/repr/src/explain.rs`: `IndexUsageType`.
- `src/compute/src/metrics.rs`: a counter of peeks by access path.
- `src/sql/src/session/vars/definitions.rs`: the feature flags.
- `src/compute/src/render/context.rs`: exchange by unique key, for the narrow
  variant only.

### Testing

- SLT under the feature flags. EXPLAIN shows the `TopK` improvement and ordered
  reads when the planner's rules choose them, and falls back when they do not.
  Each query is compared against the same query without the fast path, covering
  ties at the limit, NULLs, both directions, `IN` lists, `SELECT DISTINCT` and
  `DISTINCT ON` over fan-out, residual filters that reject most rows, offsets,
  range bounds and keyset pagination, and reads without a `LIMIT`.
- A regression test for an MFP error on a version that is not live at the peek
  timestamp, which must not fail the peek.
- Testdrive: ordered reads under inserts, updates, and retractions, at several
  timestamps.
- Property tests per supported type over `arb_datum_for_scalar`, checking each
  field's contract against `compare_columns` for both directions and null
  placements, equal hashes for equal values, and monotonicity of truncation.
  Hash collisions forced by a test-only hash width.
- Feature benchmark: scenarios for the example's shape, read through an
  order-key index and through a regular index with the `TopK` improvement, at
  several project sizes, next to `FastPathOrderByLimit`.

### Observability

The existing `mz_index_peek_*` histograms cover ordered reads.
`mz_index_peek_row_iteration_rows` is the acceptance signal: for ordered reads
it should stay close to the number of rows each worker returns. Ordered reads of
a small `k` should end on the `inline` substrate of `mz_index_peek_walks_total`,
and ordered reads that replace walks which offload today should stop adding to
`mz_index_peek_permit_queue_depth`. A counter of peeks by access path separates
the populations.

## Query shapes

The shapes below read one relation `t`. `p` is bound by a literal, `a`, `g`,
and `id` are fixed-width orderable columns, `id` is a unique key, `title` is
text, `f` is a column the index does not cover, and `$n` and `$m` are literals.
Today's plan uses a regular index keyed by the columns the shape binds by
equality, such as `t (p)`. The *Order-key index* column names the declared index
that serves the shape, and *Served* says how.

A ✓ means an ordered read in its basic form serves the shape: one equality
range, a `LIMIT`, and the stop rule. Each worker then visits `O(k)` rows, where
`k` is the limit plus the offset, plus those the residual filters reject. Rows
that a predicate on the first ordered column rejects do not count, since the
walk may visit all of them before the first row that passes. Each `+` names a
fast-path improvement the shape needs, from
[Fast-path improvements](#fast-path-improvements). A ✗ means the shape keeps
today's plan. A note in parentheses below a mark explains it and adds no
requirement. The [Minimal Viable Prototype](#minimal-viable-prototype) covers ✓
and ✓ + `TopK`. The `TopK` improvement alone, over a regular index without an
ordered read, already takes the deduplicating shapes off the dataflow path,
though they still walk the whole lookup.

### Ordered pages

| Shape | Today | Order-key index | Served |
|---|---|---|---|
| `WHERE p = $1 ORDER BY a LIMIT $n` | walks the lookup | `t (p, a)` | ✓<br><br>(with reverse cursors, `t (p, a DESC, id DESC)` serves it too) |
| `WHERE p = $1 ORDER BY a DESC, id DESC LIMIT $n` | walks the lookup | `t (p, a DESC, id DESC)` | ✓ |
| `WHERE p = $1 ORDER BY a DESC LIMIT $n` | walks the lookup | `t (p, a DESC, id DESC)` | ✓<br><br>(the index of the previous row, whose order starts with this one) |
| `WHERE p = $1 AND f IN (...) ORDER BY a LIMIT $n` | walks the lookup | `t (p, a)` | ✓<br><br>(walks past rows the filter rejects) |
| `WHERE p = $1 AND g = $2 ORDER BY a LIMIT $n` | walks the lookup | `t (p, g, a)` | ✓ |
| `WHERE p = $1 ORDER BY g, a LIMIT $n` | walks the lookup | `t (p, g, a)` | ✓<br><br>(the index of the previous row) |
| `ORDER BY a LIMIT $n` | full scan | `t (a)` | ✓ |
| `WHERE p IN ($1, $2) ORDER BY a LIMIT $n` | walks both lookups | `t (p, a)` | ✓ + `IN` lists |
| `WHERE p = $1 ORDER BY title LIMIT $n` | walks the lookup | `t (p, title)` | ✓<br><br>(the finishing orders rows that share `title`'s first `N` bytes) |
| `WHERE p = $1 ORDER BY title, a LIMIT $n` | walks the lookup | `t (p, title)` | ✓<br><br>(the finishing orders `a` among rows that share `title`'s first `N` bytes) |
| `WHERE p = $1 ORDER BY lower(title) LIMIT $n` | walks the lookup | none | ✗<br><br>(ordering on expressions is out of scope) |
| `WHERE p = $1 ORDER BY a LIMIT $n OFFSET $m` | walks the lookup | `t (p, a)` | ✓<br><br>(walks `$m + $n` rows) |
| `WHERE p = $1 ORDER BY a` | walks and sorts the lookup | `t (p, a)` | ✓ + reads without a `LIMIT`<br><br>(visits every row, and skips the sort) |

These shapes need five order-key indexes, `t (p, a)`, `t (p, a DESC, id DESC)`,
`t (p, g, a)`, `t (a)`, and `t (p, title)`, each a copy of `t`. With reverse
cursors, `t (p, a DESC, id DESC)` also serves the ascending shapes and replaces
`t (p, a)`.

### Deduplication

| Shape | Today | Order-key index | Served |
|---|---|---|---|
| `SELECT DISTINCT a, id, title ... WHERE p = $1 ORDER BY a DESC, id DESC LIMIT $n` | one-off dataflow | `t (p, a DESC, id DESC)` | ✓ + `TopK` |
| `SELECT DISTINCT ON (a, id) ... WHERE p = $1 ORDER BY a DESC, id DESC LIMIT $n` | one-off dataflow | `t (p, a DESC, id DESC)` | ✓ + `TopK` |
| `SELECT DISTINCT ON (g) ... WHERE p = $1 ORDER BY g, a LIMIT $n` | one-off dataflow | `t (p, g, a)` | ✓ + `TopK` |
| `SELECT DISTINCT ON (g) ... WHERE p = $1 ORDER BY g, a` | one-off dataflow | `t (p, g, a)` | ✓ + `TopK` + reads without a `LIMIT` |
| `SELECT DISTINCT ON (g) ... WHERE p = $1 LIMIT $n` | one-off dataflow | none | ✗<br><br>(the `TopK` improvement needs an `ORDER BY`) |

### Ranges and pagination

| Shape | Today | Order-key index | Served |
|---|---|---|---|
| `WHERE p = $1 AND a < $c ORDER BY a DESC LIMIT $n` | walks the lookup | `t (p, a DESC, id DESC)` | ✓ + range bounds |
| `WHERE p = $1 AND (a, id) < ($c, $d) ORDER BY a DESC, id DESC LIMIT $n`<br><br>(or its `OR` expansion) | walks the lookup | `t (p, a DESC, id DESC)` | ✓ + range bounds<br><br>(bounded by `a <= $c`) |
| `WHERE p = $1 AND a BETWEEN $x AND $y ORDER BY a LIMIT $n` | walks the lookup | `t (p, a)` | ✓ + range bounds |
| `WHERE p = $1 AND mz_now() <= a ORDER BY a DESC LIMIT $n` | walks the lookup | `t (p, a DESC, id DESC)` | ✓ + range bounds |
| `WHERE p = $1 AND f < $c ORDER BY a LIMIT $n` | walks the lookup | `t (p, a)` | ✓<br><br>(`f < $c` is a residual filter) |
| `WHERE p = $1 AND mz_now() <= a + INTERVAL '1 day' ORDER BY a DESC LIMIT $n` | walks the lookup | `t (p, a DESC, id DESC)` | ✓<br><br>(the predicate is a residual filter, so a window with fewer than `$n` rows walks past it) |
| `WHERE p = $1 AND a > $c`<br><br>(without an `ORDER BY` or a `LIMIT`) | walks the lookup | `t (p, a)` | ✓ + range bounds + reads without a `LIMIT`<br><br>(walks only the range) |

Without range bounds, an ordered read still answers the first four shapes,
treating the bound as a residual filter, so it also walks rows the bound
excludes, such as every row before a later page.

### Shapes this design does not change

| Shape | Today | Order-key index | Served |
|---|---|---|---|
| `WHERE p = $1 LIMIT $n` | lookup that stops at the limit | none | unchanged<br><br>(a regular index wins the tie) |
| `WHERE p = $1 AND id = $2 LIMIT 1`<br><br>(with a regular index on `id`) | point lookup | none | unchanged<br><br>(a unique-key lookup outranks an ordered read) |
| `... FROM t JOIN u ... ORDER BY a LIMIT $n`, or `SELECT count(*) ... WHERE p = $1` | one-off dataflow | none | unchanged<br><br>(still a dataflow) |
| Any of the above through a view over `t` | as for `t`<br><br>(after view inlining) | as for `t` | as for `t` |

## Strategic considerations

Recommending order-key indexes takes a position on five questions that reach
beyond this design.

**Physical design in SQL.** Order-key indexes add syntax that declares a
physical layout: an `ORDERED` option, per-column directions, and possibly field
widths. Materialize asks users to choose arrangement keys with `CREATE INDEX`,
advises on them (`mz_internal.mz_index_advice`), and accepts hints such as
`DISTINCT ON INPUT GROUP SIZE`. Should serving performance keep being expressed
through declared structures, or should new read paths derive their structures
from the workload? This design takes the first position.

**Growing the fast path.** The fast path is a set of hand-matched shapes:
constants, index scans and lookups, and persist reads with a limit or an order.
This design adds ordered reads and the fast-path improvements on top of them.
Each shape touches the matcher, EXPLAIN, index usage accounting, both peek
sequencing paths, the compute protocol, and the scan, as the touch points above
show. Continuing this way multiplies shapes. The alternative is a small plan
language for one-shot reads that the worker executes without a dataflow: an
access (scan, lookup, ordered), an MFP, an optional `TopK`, and a limit. This
design's protocol additions are the first operators such a plan would have.
Should they land as the start of a peek plan, or should that generalization wait
until the set of operators is known?

**Arrangements versus purpose-built structures.** Order-key indexes reuse
arrangements unchanged, and pay with a copy of the data per shape. A separate
serving store would reimplement times, compaction, frontiers, and sharing (see
[Alternatives](#alternatives)). This design keeps arrangements the only
structure compute maintains. Read-side structures attached to immutable batches
would be the natural home for further read accelerators, such as data-skipping
filters for selective residual predicates, at the cost of their own lifecycle
and memory accounting. Is keeping arrangements the only structure the direction
we want?

**Peek execution.** An index peek's walk is one budgeted, suspendable scan that
leaves the timely worker once it is measured expensive
(MaterializeInc/materialize#38508, MaterializeInc/materialize#38509,
MaterializeInc/materialize#39123), building on `Arc`-backed batches
(MaterializeInc/materialize#38396). Offload keeps a long walk from stalling the
worker, but not from costing its CPU, which this design removes, and it bounds
concurrent expensive walks with permits, which ordered reads stop consuming.
Ordered reads plug into the scan rather than beside it. Should the ordered walk
be specified as part of the open design for one execution path for index peeks
(MaterializeInc/materialize#38449), so that access modes are a property of the
scan rather than of the fast-path matcher?

**Larger-than-memory state.** The buffer-managed state design
(`20260610_buffer_managed_state.md`) moves sealed batches toward pageable chunks
under one memory budget. Under that direction, an order-key index's per-shape
copy costs pool budget and disk rather than resident memory, and an ordered walk
over it reads its chunks in order. Is paged memory cheap enough that a copy per
shape stays acceptable for wide collections?

## Minimal Viable Prototype

The prototype implements ordered reads over order-key indexes, with all three
field kinds, and the [`TopK`](#topk) improvement, including `SELECT DISTINCT`
and the `TopK` on the worker. The `TopK` improvement also serves deduplication
over regular indexes, so it can land first, together with accumulating
multiplicities before the MFP.

The prototype serves the example query over a wide collection with fan-out, for
projects of `10^4` to `10^6` rows. For each it compares three plans: today's
one-off dataflow, the `TopK` improvement over a regular index on `project_id`,
and the ordered read. It measures p50 and p99 latency, p99.9 latency under an
update-heavy workload, and rows visited per worker. It also measures the memory,
hydration time, and maintenance CPU of serving the collection in three shapes,
against the regular indexes the same queries use today, and the cost of
accumulating multiplicities before the MFP on `FastPathFilterIndex`. The
remaining fast-path improvements and the narrow variant follow.

## Alternatives

### Other representations of the order

**Per-batch orderings over a regular index.** Keep a regular index on the bound
columns and derive orderings from reads instead of declaring them. A batch never
changes once built, so the first read for a key and an ordering sorts the key's
values in each batch and caches the sorted order as a permutation, and later
reads merge the cached permutations across batches and stop by the same stop
rule. That costs four bytes per value and ordering rather than a copy, and
serves any ordering without new SQL. Its latency depends on cache state, though.
The first read after a rehydration, or after a merge into the largest batch,
sorts the key's full history, so tail latency follows restarts and merges rather
than the query. It also reads batch storage directly, which the port of
compute's spines to buffer-managed chunks (`20260610_buffer_managed_state.md`)
will change. Building whole-batch permutations eagerly with each batch avoids
the cold reads and serves an `ORDER BY` without a bound key, at the cost of a
sort on every batch build and a declared ordering.

**Comparing datums in arrangements.** Give arrangements a key type whose `Ord`
decodes datums, or make `Row` compare in datum order. Either makes every
comparison in batch merging decode datums. Changing `Row` changes the order of
every arrangement to benefit the few whose order matters, and a per-index key
type needs a new arrangement flavor as well as a way to pass column orders to
`Ord`.

**A separate multiversioned ordered structure.** Maintain a B-tree or skip list
per worker beside, or instead of, an arrangement. To serve reads at a timestamp
it has to hold times and diffs, compact with the arrangement's frontier, and be
shared with readers, which is the arrangement reimplemented. Stripped of those,
it becomes one of the representations above.

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
ordered reads whose filters reject most of a range, and is worth doing
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

### Reading from persist

**Ordered reads from persist.** Persist parts carry statistics and can be read
in order for some types. Blob reads add latency an interactive read cannot
afford, and the ordering is ascending on a prefix of the relation's columns.

## Open questions

- Syntax. `WITH (ORDERED)` keeps Postgres's key-part syntax. `USING btree` would
  be familiar, but Postgres users would expect it to serve joins. The form
  `ON ticket_labels (project_id) WITH (ORDER BY (updated_at DESC, ticket_id DESC))`
  separates the bound columns from the ordering.
- Whether the narrow variant should be the default for wide collections, given
  that each served shape needs its own index.
- The default width `N` of prefix fields, an exact fixed-width `numeric`
  encoding (precision is bounded at 39 digits), and how `mz_index_columns`
  presents the declared columns rather than the `order_key` expression.
- Whether rows that arrive in order could make ordered results streamable
  through the peek stash, which today only takes results without an `ORDER BY`
  (`RowSetFinishing::is_streamable`).
