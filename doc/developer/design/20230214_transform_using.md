# Overview

*Allow users to transform a source before persisting it using SQL*

# Goals

- Make append-only sources viable by retaining a bounded number of updates
- Thin-down sources by filtering before consuming storage

This design doc proposes supporting arbitrary data transformations during ingestion made available
to users by extending the syntax of the `CREATE SOURCE` statement. The goal is to allow expressing
any transformation that can be written as a SQL query with one candidate syntax being a `TRANSFORM
USING <query>` fragment that can be optionally specified at the end of a `CREATE SOURCE` statement.

**Example**

```sql
CREATE SOURCE foobar FROM POSTGRES ...
TRANSFORM USING SELECT colA, colB FROM foobar WHERE colC > 4;
```

**Example with temporal filter**

```sql
CREATE SOURCE foobar FROM POSTGRES ...
TRANSFORM USING SELECT colA, colB FROM foobar WHERE created_at > mz_now()
```

### Non-goals

- Express existing envelopes and decoders in terms of a transformation query instead of custom code.
  It seems possible but it’s out of scope for the initial implementation of this feature

## Product Details

While the proposed syntax allows arbitrary queries to be written there is a wide spectrum of product
behavior that affects the kind of queries that the system is willing to accept and the promises to
the user we are willing to make as to the effect of the queries will have to their cost model.

Engineering wise, it is possible to support arbitrary queries (described below) where the system
makes a best effort to push as much of that query as possible before we start persisting data. This
is not always possible and in the worst case the provided query can result in zero gains in the
amount of persisted data. For this reason we might want to constraint kind of queries we accept to
ones that have a predictable effect to storage usage.

I postulate that a user typing a `TRANSFORM USING` query in their `CREATE SOURCE` statement would
have the expectation that the only state stored by materialize would be the result of applying that
query on top of their source. If that is the case, we might want to outright reject queries that
cannot be fully applied on the raw source stream. In practice this could mean that we accept any
query in the form of: `SELECT expr1, expr2, .. exprN WHERE pred1 && pred2 ..` which should be enough
for users to apply temporal filters and thin down their data.

The counterargument to this is that users already accept that the resource usage of their COMPUTE
queries in terms of RAM usage is at the mercy of the optimizer and surprising behaviors might arise
so it is possible to imagine them being fine with `STORAGE` also being less predictable in its usage
of S3 storage. In that world we could be accepting any query and the user would inspect the effect
their transform had in the ingestion after the fact, or perhaps with a suitable `EXPLAIN` query.

I will expand this section once I get the chance to talk with more people about it. Keep in mind
that there are engineering reasons to prefer the first approach where the set of accepted queries is
constrained.

## Engineering Details

### Storage execution model

Running dataflows in storage has some unique characteristics which are important to keep in mind
when evaluating the various options laid out in the rest of the document so I will describe it here
briefly.

First, some definitions. We will represent the data in the external system that we want to ingest as
a remote differential collection `RAW`. Storage is tasked with producing a durable storage
collection `C = PERSIST(TRANSFORM(RAW))` by scheduling executions of ingestion dataflows. Currently
`TRANSFORM` includes things like format decoding and envelope processing. For each remote collection
storage maintains a read cursor in it at some frontier `F` that allows it to read the updates to
`RAW` that happened at times `t: F <= t`. Ingestion dataflows are fault-tolerant and can be
re-executed when one fails. Executions share no state between them except for any data durably
stored in `persist` shards. Each execution is tasked with resuming the production of updates to `C`
by only observing updates to `RAW` that happened beyond `F`.

The unique characteristic is that `RAW` collections do not offer the ability to read a snapshot at
an arbitrary time `t`, even if that time is beyond our read cursor. `RAW` collections are only able
to provide the updates that happened at `t` and it is up to the reader to remember past values if
that is important. The reason for that is that we want to be giving permission to upstream systems
to *delete* (not just compact) data that we have already seen, if desired.

As a result, the `TRANSFORM` function cannot rely on any in-memory state since its current execution
can be interrupted at any moment and restarted in a new machine with a blank in-memory state. This
limits the kinds of dataflow operators that we can use. Stateless operators like `map` and `filter`
are safe and so are operators that can re-construct their in-memory state by reading a snapshot of
`C` (not `RAW`). The upsert envelope is an instance of the latter case.

### Adding arbitrary SQL to `TRANSFORM`

Given the constraints of the transformation function that can be ran during ingestion we can explore
a few different avenues to incorporate an arbitrary query `Q` as a transformation step. Every query
in materialize, after planning and optimizing, can be decomposed into a `MapFilterProject` (MFP from
now on) part (`Q_mfp`) and a relation expression part (`Q_expr`). Depending on the query any of
these can be trivial (i.e the identity).

This decomposition is relevant to what we’re trying to accomplish because `Q_mfp` has the property
that it’s stateless (state for temporal filters discussed below) and can be evaluated without having
access to input snapshots whereas `Q_expr` might include arrangements and therefore will need an
input snapshot before it can resume.

With this decomposition we can instantiate two dataflows:

1. `C_intermediate = PERSIST(Q_mfp(RAW))`
2. `C = PERSIST(Q_expr(C_intermediate))`

Where the first is scheduled in the storage timely cluster and the second in the compute timely
cluster. In a future where the two timely clusters get unified this could be a single big dataflow.

### Handling MFPs with temporal filters

Evaluation of `MFP`s is parameterized by the `Row` that we want to map, filter and project and the
time `t` that this evaluation is happening at. When the `MFP` contains no temporal filters the
output is a single transformed `Row` at the same time `t`. However, when there is a temporal filter
involved there is a possibility that we get `Row`s that happen in the future. This is because for
each `Row` the temporal filter will calculate a `lower_ts` and an `upper_ts` which define the
validity period for the row. Then these differential updates will be produced: `(row, lower_ts, +1)`
and `(row, upper_ts, -1)`. The `lower_ts` and `upper_ts` timestamps are entirely dependent on the
expression the user typed and can be arbitrarily in the future.. This is best depicted in this
diagram:

![Temporal filter application](https://github.com/MaterializeInc/materialize/raw/9c79faf5d58b3a97b11f982fa4f6e7736c864922/doc/developer/design/static/temporal_filters.png)

This is a problem because when the `RAW` colletion produces the differential update `(row, t, +1)`
we might have to emit it at some future time `t_temporal >> t` but if we crash and restart we might
have to resume from some time `t_resume > t && t_resume < t_temporal`. In this case we will entirely
miss the original update from `RAW` and we will fail to produce the differential update that we
would have produced had we not crashed.

In order to solve this we will need to decompose `Q_mfp` into three parts: `Q_safemfp`,
`Q_expiration`, and `Q_final`

> 💡 To understand the following decomposition it is important to keep in mind that all temporal
> predicates are internally converted into expressions that produce a candidate timestamp for either
> the lower or upper bound of the validity period. For example, the predicate `expr <= mz_now()`
> will get converted into just the expression `expr` and will be stored in a `lower_bounds` list. To
> calculate the lower and upper bounds of the validity period of a row during a temporal MFP
> evaluation all the lower bound expressions are evaluated and the maximum is selected and all the
> upper bound expressions are evaluated and minimum is selected.

The goal of the decomposition is to come up with a `Q_safemfp` that contains no temporal filters,
performs the maximum amount of mapping and filtering, and retains enough information to later
recover the original validity period. We can define the three parts like so:

1. `Q_safemfp` will contain the non-temporal part of `Q_mfp` combined with two additional `Map`
   expressions. These two additional map expressions will correspond to the lower and upper bounds
   and will be calculating the final timestamp for them. We can do so by fabricating two
   `MirScalarExpr`s. One that calculates the maximum of all the lower bound exprs and one that
   calculates the minimum of all the upper bound exprs. Effectively, we will precompute the validity
   period and store it as two additional timestamp columns at the end of the original row.
2. `Q_expiration` will contain the temporal predicate `mz_now() <= upper_ts` which ensures each
   record is retracted at the upper bound of its validity period.
3. `Q_final` will contain the temporal predicate `lower_ts <= mz_now()` and a projection that hides
   the last two columns, which contain the two timestamps of the validity period. This will ensure
   that records are only inserted at the lower bound of their validity period and that `Q_expr` will
   see rows with the correct schema.

Given this decomposition we can now define the ingestion as:

1. `C_intermediate = PERSIST(Q_expiration(Q_safemfp(RAW)))`
2. `C = PERSIST(Q_expr(Q_final(C_intermediate)))`

With these equations we have accomplished the goal of doing as much work as possible before the
first persist operator while requiring no past state from `RAW` to resume. To see why that is the
case, let's describe the state needed by the `Q_expiration` operator. The state of `Q_expiration` at
time `t` is the set of records `(row, t_record, diff)` for which `t_record <= t && t <
upper_ts(Q_mfp(row))`. In other words, for every timestamp it needs to remember all past records
that expire after that timestamp.

If we now look at the output of `Q_safemfp` we see that it converts a `(row, t_record, diff)` update
to `((row, lower_ts, upper_ts), t_record, diff)` update. Additionally, `Q_expiration` will transform
each `((row, lower_ts, upper_ts), t_record, diff)` update into two:

1. `((row, lower_ts, upper_ts), t_record, diff)`
2. `((row, lower_ts, upper_ts), upper_ts, -diff)`

Given the above we can see that at the beginning of an ingestion execution `Q_expiration` can
recover its in-memory state at time `t` by taking a snapshot of `C_intermediate` (not `RAW`) at time
`t`, which is available because it is stored in persist! This is because any record that may expire
in the future is maintained until the end of its validity period and `Q_mfp` has preserved the
previously computed timestamps. This technique is similar to how the `UPSERT` envelope recovers its
state from its output.

### Planner/Optimizer/Data structure stabilization

The inability to have `RAW` snapshots readily available has some deep implications on how much of
our internal choices we are willing to stabilize, which in turn have deep implications on the kind
of queries we want to allow users to use in the `TRANSFORM USING` position.

In the equations above we had to define `C_intermediate` to effectively be a collection whose schema
is very much dependent on planner and optimizer choices. Since the transformation from `RAW` to
`C_intermediate` is not described by high level `SQL` that can be stored in the catalog we must
explore what else we must stabilize in order to be able to continue ingesting a source across
versions.

There are various flavors of how this could go. I will lay out some of them below.

**Option 1: Only accept transforms `Q` whose `Q_expr` is the identity, stored as SQL**

We make the guarantee that if a version of Materialize plans and optimizes a piece of SQL to just an
MFP then all future versions will continue to do so and will be producing an identical MFP. No data
structure stabilization needed.

**Option 2: Only accept transforms `Q` whose `Q_expr` is the identity, stored as raw MFP**

We make the guarantee that an MFP produced by some version of Materialize will be able to be read
and interpreted in the intended way by all future versions. In practice this means stabilizing
`MirScalarExpr`, `UnaryFunc`, `BinaryFunc`, and `VariadicFunc`.

**Option 3: Accept any transform `Q`, stored as SQL**

We make the guarantee that if a version of Materialize plans and optimizes a piece of SQL to a pair
of `Q_mfp` and `Q_expr` then all future versions will continue to produce an identical `Q_mfp` but a
potentially different `Q_expr`. No data structure stabilization needed.

**Option 4: Accept any transform `Q`, stored as `MirRelationExpr`**

We make no guarantees to the properties of the planner/optimizer over time but we guarantee that a
`MirRelationExpr` produced by a version of Materialize will be readable in the intended way by all
future versions. This requires stabilizing everything that was needed for option 2 and additionally
`MirRelationExpr`, `JoinImplementation`, `AggregateExpr` and perhaps more data strucutres that are
transitively depended on by `MirRelationExpr`.

> Petros’ take: Stabilizing data structures seems easier to think about than stabilizing code
> behavior so I would shy away from options 1 and 3. Stabilizing `MirScalarExpr` seems tractable
> given its simplicity so I would go for option 2 unless an amazing option 5 appears. Input from
> COMPUTE folks would be helpful here.

### Transformation re-optimization

In the previous section we discussed how a query `Q` can be broken down into a `Q_mfp` and `Q_expr`
where `Q_mfp` must stay fixed as Materialize evolves. This penalizes early adopters with potentially
worse optimizations and potentially worse cost savings. This section discusses how we could take
advantage of future improvements to the planner and optimizer in the context of source
transformations.

#### Optimizer improvements

First, let's consider improvements to the optimizer which operates on the `Mir` level. Suppose that
some version `v1` of Materialize processes a source transformation query into a `Q_mfp_v1` and
`Q_expr_v1` pair. As described in the previous section this will result in persisting the durable
collection `Q_mfp_v1(RAW)`, whose schema will entirely depend on the optimizations that the `v1`
optimizer will perform. Now let's say that version `v2` gets released that has more optimizations
that would have resulted a better (`Q_mfp_v2`, `Q_expr_v2`) pair. Unfortunately we cannot use these
query fragments as-is, because to do that we'd have to calculate `Q_mfp_v2(RAW)` on a snapshot, but
the snapshot isn't available.

What we can do instead is to leave `Q_mfp_v1` fixed and compute `(Q_mfp_v2, Q_expr_v2) =
optimizer_v2(Q_expr_v1)`. In other words, we can re-optimize the part of the query that *has* access
to a snapshot. If such re-optimization yields a non-trivial MFP then it means that the new version
of Materialize managed to push down more parts of the query and we can now create a
`C_intermediate_v2` by stitching together `Q_mfp_v2(C_intermediate)` and `Q_mfp_v2(Q_mfp_v1(RAW))`.

In order to be able to re-optimize it is crucial that `MirRelationExpr` and the data structures it
transitively depends on get stabilized so that future versions of Materialize can understand the
semantics of `Q_expr_v1`.

#### Planner improvements

Materialize currently performs a limited set of optimizations at the `Hir` level but more may be
written in the future (e.g the QGM effort). My understanding of what we have today is that the only
way that we could reap this kind of improvements after the fact would be to not lower the provided
query `Q` to `Mir` for storage, but instead perform predicate push-down at the SQL level. For
example, if given a SQL query `Q` we could come up with a `Q_hir_mfp` and a `Q_hir_expr` where
`Q_hir_mfp` is guaranteed to be optimized to an `MFP` then every version of materialize could be
re-running its optimizations in the respective parts and progressively moving computation from
`Q_hir_expr` into `Q_hir_mfp`. However, this seems like a huge lift and potentially not worth
blocking this feature on.

> Input from COMPUTE folks on other approaches for re-optimization would be helpful here.
