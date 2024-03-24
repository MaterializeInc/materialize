# Overview

People want to use Materialize to maintain SQL queries over recent histories of their append-only
data. This is because persisting append-only data grows without bound, making rehydration both
inefficient and expensive. Giving users more control into how and when to expire their data makes
Materialize applicable to broader set of use cases and customers.

# Goals

This design doc proposes supporting a limited set of data transformations during ingestion made
available to users by extending the syntax of the `CREATE SOURCE` statement. The extension has the
form of a new `TRANSFORM USING` keyword which is paramterized by a SQL query describing the
transformation.

The goal are to allow users to:

* Thin down their data through expiration, by using a temporal filter in their transformation.
* More generally filter and transform their data before peristing them through SQL expressions.

### Non-goals

- Express existing envelopes and decoders in terms of a transformation query instead of custom code.
  It seems possible but it’s out of scope for the initial implementation of this feature.
- Allow arbitrary computation in the transformation queries.
- Define syntax for both transforming and breaking up a source into subsources.

## Feature description

### Syntax

The current ingestion syntax can be viewed as a series of transformations that are applied one after
the other. For example users can add a `FORMAT` transformation to do format decoding on top of the
raw output and an `ENVELOPE` transformation to interpret a certain types of envelopes after format
decoding.

This proposal introduces an additional transformation whose general form is `TRANSFORM USING
(<query>)`. Semantically this transformation could be applied at before or after any of the existing
transformations (i.e pre/post format and pre/post envelope) we will limit the allowed positions for
the initial release of this feature to effectively post envelope. In other words, the schema that
this query should expect to operate on will be the same schema that the source would have produced
without it.

**Example: Decode JSON and keep only certain rows**

```sql
CREATE SOURCE sales_staff
FROM KAFKA CONNECTION ...
FORMAT BYTES
TRANSFORM USING (
    SELECT
        (data->>'id')::int AS id,
        (data->>'name')::text AS name,
    FROM
        (SELECT CONVERT_FROM(data, 'utf8')::jsonb as data FROM sales_staff)
    WHERE
        (data->>'department')::text == 'sales'
);
```

**Example: Apply expiration to keep the orders of the last 30 days**

```sql
CREATE SOURCE orders_30d
FROM KAFKA ...
FORMAT AVRO USING ...
TRANSFORM USING (
    SELECT * FROM orders_30d
    WHERE mz_now() <= created_at + '30 days'
);
```

**Example: Apply expiration based on ingestion time**

```sql
CREATE SOURCE orders_30d
FROM KAFKA ...
FORMAT AVRO USING ...
INCLUDE INGESTION TIMESTAMP as ingest_ts
TRANSFORM USING (
    SELECT * FROM orders_30d
    WHERE mz_now() <= ingest_ts + '30 days'
);
```

**Example: Project specific columns of a multi-output source**

```sql
CREATE SOURCE pg_source
FROM POSTGRES ...
FOR TABLES (
   orders TRANSFORM USING (SELECT id, item FROM orders),
   customers TRANSFORM USING (SELECT id, name FROM customers) AS customer_data,
);
```

### Allowed transformations

#### Storage execution model

Running dataflows in storage has some unique characteristics that affect the kind of transformations
that we are able to execute at that stage. At a high level, storage is tasked with reading a remote
differential colletion `RAW`, applying some transformations, and writing down the result as a
durable collection in `persist`. Currently the transformations include things like Avro decoding and
upsert envelope processing. Crucially, storage never requests a full snapshot of `RAW` from the
external system, except for the very beginning of the ingestion. Even beyond that, it actively gives
permission to the upstream system to *delete* (not compact) updates that happen at times that we
have already ingested. In other words, when a storage dataflow resumes, it can immediately pick up
where it left of.

#### Limitations due to absense of snapshots

The inability to access a snapshot of the data on resumption limits the kind of queries that can
successfully resume in this setting. An obvious limitation is that any query that needs arrangements
cannot be executed at this stage as we wouldn't be able to rehydrate the arrangement from a
snapshot. However, dataflow state is not limited to arrangements. Operators can hold onto private
in-memory state and rely on rehydrating from a snapshot to rebuild it. Therefore, absense of
arrangements is not enough to characterize the set of queries that should be allowed.

Instead of looking for arrangements the criterion will instead be whether the transformation can be
fully optimized down to a `MapFilterProject` (MFP from now on). An MFP is a structure that can
express any combination of simple maps, filters, and projections, including temporal filters. One of
its appealing properties is that it can be fully evaluated "in-place" with no additional state,
which makes it ideal for the storage execution model. A detailed discussion on the handling of
temporal filters is defined below.

It is hard to give a precise description of the queries that will optimize to an MFP, which poses a
documentation challenge. A practical option can be presenting some guidelines (e.g no joins or
agreggations) and a comprehensive set of examples. As shown in the examples above this set of
queries are powerful enough to cover:

* Thinning down collections by filtering
* Thinning down collections by projecting specific columns
* Thinning down collections by applying a temporal filter

#### Limitations due to backward incompatibility

Another implication of storage's execution model is that it is not as robust as materialized views
in the presence of changes in how a particular SQL query evaluates. This is mostly a concern across
version upgrades where a query might get optimized differently and be producing a different output
for the same input. Some examples of how this can happen are:

1. Re-ordering of floating point operations
1. Re-ordering of logical operators where one of the atoms produces an error
1. Purpusefully changing the behavior of a SQL function (e.g to fix a bug)

This can be an especially bad problem if the transformation is applied over a non append-only
source. If the input to the transformation contains a pair of updates `(data, t1, +1), (data, t2,
-1)` that perfectly cancel out but we process them with two different versions of Materialize
(perhaps they happen weeks apart), then we can end up with `(transform_v1(data), t1, +1),
(transform_v2(data), t2, -1)` where the pair no longer matches up and we produce a negative
accumulation (or other anomalies).

To limit the risk of this happening we will initially limit the `TRANSFORM USING` syntax to only be
available on the output of append-only envelopes, which practically means just `ENVELOPE NONE`.

As we make progress on making more of our query evaluation deterministic and gain more confidence in
the stability of our implementation we can lift this restriction and allow transformations on any
kind of source.

### Expected savings in storage and rehydration time

The limitations introduced above have a positive flip-side which is great predictability of their
effect in cost and rehydration times. When the Materialize accepts a `TRANSFORM USING` invocation
the user can be certain that the entirety of their computation will be perform and applied before
any data is persisted in Materialize.

## Technical description

In this section we explore some of the technial aspects of how the transformation described above
can be implemented. We will represent as `Q_mfp` the MFP representation of the query after it has
been planned and optimized.

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

```
C = Q_final(PERSIST(Q_expiration(Q_safemfp(RAW))))
```

Note that in the equation above `Q_final` is applied on the *output* of durably persisting
persisting the rest of the ingestion. This MFP fragment will be deferred and applied on the read
side when a dataflow uses the collection `C`.

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

### Planner/Optimizer stabilization

When accepting a `CREATE SOURCE` statement with a `TRANSFORM USING` clause we are at that moment
making a promise that that transformation will always be compatible with the storage execution
model.

In practice, since we have limited the accepted queries to only those that fully compile to an MFP,
this means that the planner/optimizer must maintain the following invariant:

> If a query `Q` fully optimizes to an MFP at some version `n` of Materialize, then it will continue
> to plan and optimize to an MFP for all versions `n' >= n`. The MFPs are allowed to vary across
> versions to allow for new optimizations to be taken advantage of.

## Future improvements

The feature as described includes a lot of limitations that are necessary with our current appetite
for stability promises. Here we explore how we might lift some of them in the future.

### Accept `TRANSFORM USING` for non-append only collections

The fisrt limitation that we can lift is allowing transforming non-append only streams. This would
allow users to transform the output of envelopes other than NONE, and also to transform the output
of postgres sources.

In order to do that we'll have to address the sources of non-determinism in the evaluation semantics
of a SQL query. Here are a few thoughts on how each could be addressed but more design work is
needed to fully spec these out.

### Accept `TRANSFORM USING` in more ingestion positions

An improvement closely related to the one above would be to allow `TRANSFORM USING` to be used in
positions other than post-envelope. Allowing this opens up usecases such as customizing the behavior
of `ENVELOPE UPSERT` by transforming its input or even the behavior of `FORMAT` decoding by
manipulating the bytes for instance before passing them to the `FORMAT` decoders.

Discussion on how the ingestion description can become more pipeliny and exposed to SQL is tracked
[in this issue](https://github.com/MaterializeInc/materialize/issues/11957).

#### Floating point operations

There is an active discussion about fixing [floating point
correctness](https://github.com/MaterializeInc/materialize/issues/15186) for aggregations but that
work will have to extend to normal scalar expression as well to fix the MFP issues.

Another alternative would be to forbid floating point expressions on transformations on
non-append-only collections. This comes with a documentation burden as we'll have to be listing
these exceptions to explain to users how to use the feature.

#### Non-deterministic logical operators

The execution order of logical operators is currently unspecified and at the same time we are
allowed to short-circuit evaluation. These together introduce non-determinism when faced with
expressions such as `FALSE && ERROR` where we skip evaluating the error expression altogether.

SQL has a construct that guarantees execution order, the `IF THEN ELSE` expression so one possible
path would be that for the specific queries used in `TRANSFORM USING` we replace all occurences of
logical operators with their equivalent `IF THEN ELSE` expression that will serve as an optimization
blocker.

Another option would be to define the behavior of logical operators to be commutative, for example
with the following "truth" table.

| A  | B  | A && B      | B && A
|----|----|-------------|--------
| T  | T  | T           | T
| T  | F  | F           | F
| T  | E  | E           | E
| F  | T  | F           | F
| F  | F  | F           | F
| F  | E  | F           | F
| E  | T  | E           | E
| E  | F  | F           | F
| E1 | E2 | min(E1, E2) | min(E1, E2)

#### Buggy SQL functions

Potential options to address this risk is to have a list of allowed SQL functions for `TRANSFORM
USING` that we are reasoably certain are bug free. Having this mechanism allows us to evolve and
introduce new SQL functions without worry and only making them available to `TRANSFORM USING`
queries once they stabilize.

### Accept arbitrary queries

Given the constraints of the transformation function that can be ran during ingestion we can explore
a few different avenues to incorporate an arbitrary query `Q` as a transformation step. Every query
in materialize, after planning and optimizing, can be decomposed into a `MapFilterProject` (MFP from
now on) part (`Q_mfp`) and a relation expression part (`Q_expr`). Depending on the query any of
these can be trivial (i.e the identity).

With this decomposition we can instantiate two dataflows:

1. `C_intermediate = PERSIST(Q_mfp(RAW))`
2. `C = PERSIST(Q_expr(C_intermediate))`

Where the first is scheduled in the storage timely cluster and the second in the compute timely
cluster. In a future where the two timely clusters get unified this could be a single big dataflow.

Going down that path has serious stability requirements for the behavior and/or the data structures
that will have to be explored in detail in a future design document.

### Allow decomposition of a source into multiple subsources

The final future improvement is offering a way to not only transform the source collection into
another collection but to allow definition of subsources, in a manner similar to how postgres
sources directly produce their subsources.

This is another direction where the design space is large and more design work is needed to pinpoint
the shape we want.
