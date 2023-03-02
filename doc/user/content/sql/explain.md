---
title: "EXPLAIN"
description: "`EXPLAIN` displays the plan used for a `SELECT` statement or a view."
menu:
  main:
    parent: commands
---

`EXPLAIN` displays the plan used for a `SELECT` statement, a view, or a materialized view.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Conceptual framework

To execute `SELECT` statements, Materialize generates a plan consisting of
operators that interface with our underlying Differential dataflow engine.
`EXPLAIN` lets you see the plan for a given query, which can provide insight
into Materialize's behavior for specific queries, e.g. performance.

## Syntax

{{< diagram "explain.svg" >}}

### Explained object

The following three objects can be explained.

Explained object | Description
------|-----
**select_stmt** | Display the plan for an ad hoc `SELECT` statement.
**VIEW view_name** | Display the plan for an existing view.
**MATERIALIZED VIEW view_name** | Display the plan for an existing materialized view.

### Output format

You can select between `JSON` and `TEXT` for the output format of `EXPLAIN`. Non-text
output is more machine-readable and can be parsed by common graph visualization libraries,
while formatted text is more human-readable.

Output type | Description
------|-----
**TEXT** | Format the explanation output as UTF-8 text.
**JSON** | Format the explanation output as a JSON object.

### Explained stage

This stage determines the query optimization stage at which the plan snapshot will be taken.

Plan Stage | Description
------|-----
**RAW** | Display the raw plan.
**DECORRELATED** | Display the decorrelated plan.
**OPTIMIZED** | _(Default)_ Display the optimized plan.
**PHYSICAL** | Display the physical plan.

### Output modifiers

Output modifiers act as boolean toggles and can be combined in order to slightly tweak
the information and rendering style of the generated explanation output.

Modifier | Description
------|-----
**arity** | Annotate each subplan with its number of produced columns. This is useful due to the use of offset-based column names.
**join_impls** | Render details about the implementation strategy of optimized MIR `Join` nodes.
**keys** | Annotate each subplan with its unique keys.
**timing** | Annotate each plan with the time spent in optimization (including decorrelation).
**types** | Annotate each subplan with its inferred type.

## Query compilation pipeline

The job of the Materialize planner is to turn SQL code into a differential
dataflow program. We get there via a series of progressively lower-level plans:

```text
SQL ⇒ raw plan ⇒ decorrelated plan ⇒ optimized plan ⇒ physical plan ⇒ dataflow
```

#### From SQL to raw plan

In this stage, the planner:

- Replaces SQL variable names with column numbers.
- Infers the type of each expression.
- Choose the correct overload for each function.

#### From raw plan to decorrelated plan

In this stage, the planner:

- Replaces subqueries and lateral joins with non-nested operations.
- Replaces `OUTER` joins with lower-level operations.
- Replaces aggregate default values with lower-level operations.

#### From decorrelated plan to optimized plan

In this stage, the planner performs various optimizing rewrites:

- Coalesces joins.
- Chooses join order and implementation.
- Fuses adjacent operations.
- Removes redundant operations.
- Evaluates any operations on constants.

#### From optimized plan to physical plan

In this stage, the planner:

- Maps plan operators to differential dataflow operators.
- Locates existing arrangements which can be reused.

#### From physical plan to dataflow

In the final stage, the planner:

- Renders an actual dataflow from the physical plan, and
- Installs the new dataflow into the running system.

No smart logic runs as part of the rendering step, as the physical plan is meant to
be a definitive and complete description of the rendered dataflow.

### Reading decorrelated/optimized plans

Materialize plans are directed acyclic graphs of operators. Each operator in the graph
receives inputs from zero or more other operators and produces a single output.
Sub-graphs where each output is consumed only once are rendered as tree-shaped fragments.
Sub-graphs consumed more than once are represented as common table expressions (CTEs).
In the example below, the CTE `l0` represents a linear sub-plan (a chain of `Get`,
`Filter`, and `Project` operators) which is used in both inputs of a self-join.

```text
Return
  Join on=(#1 = #2)
    Get l0
    Get l0
With
  cte l0 =
    Project (#0, #1)
      Filter (#0 > #2)
        Get materialize.public.t
```

Many operators need to refer to columns in their input. These are displayed like
`#3` for column number 3. (Columns are numbered starting from column 0). To get a better sense of
columns assigned to `Map` operators, it might be useful to request [the `arity` output modifier](#output-modifiers).

Each operator can also be annotated with additional metadata. Details are shown by default in
the `EXPLAIN PHYSICAL PLAN` output, but are hidden elsewhere. In `EXPLAIN OPTIMIZED PLAN`, details
about the implementation in the `Join` operator can be requested with [the `join_impls` output modifier](#output-modifiers):

```text
Join on=(#1 = #2 AND #3 = #4) type=delta
  implementation
    %0:t » %1:u[#0]KA » %2:v[#0]KA
    %1:u » %0:t[#1]KA » %2:v[#0]KA
    %2:v » %1:u[#1]KA » %0:t[#1]KA
  ArrangeBy keys=[[#1]]
    Get materialize.public.t
  ArrangeBy keys=[[#0], [#1]]
    Get materialize.public.u
  ArrangeBy keys=[[#0]]
    Get materialize.public.v
```
The `%0`, `%1`, etc. refer to each of the join inputs.
A *differential* join shows one join path, which is simply a sequence of binary
joins (each of whose results need to be maintained as state).
A [*delta* join](https://materialize.com/blog/maintaining-joins-using-few-resources)
shows a join path for each of the inputs.
The expressions in
a bracket show the key for joining with that input. The letters after the brackets
indicate the input characteristics used for join ordering. `U` means unique, the
number of `K`s means the key length, `A` means already arranged (e.g., an index
exists). The small letters refer to filter characteristics:
**e**quality to a literal,
**l**ike,
is **n**ull,
**i**nequality to a literal,
any **f**ilter.

A plan can optionally end with a finishing action which can sort, limit and
project the result data. This operator is special, as it can only occur at the
top of the plan. Finishing actions are executed outside the parallel dataflow
that implements the rest of the plan.

```
Finish order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5 output=[#0, #1]
  CrossJoin
    Get materialize.public.r
    Get materialize.public.s
```

Finally, simple queries are sometimes implemented using a so-called fast path.
In this mode, the program that implements the query will just hit an existing index,
transform the results, and optionally apply a finishing action. For fast path queries,
all of these actions happen outside of the regular dataflow engine. The fast path is
indicated by an "Explained Query (fast path):" heading before the explained query in the
`EXPLAIN OPTIMIZED PLAN` and `EXPLAIN PHYSICAL PLAN` result.

```text
Explained Query (fast path):
  Finish order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5 output=[#0, #1]
    ReadExistingIndex materialize.public.t_a_idx
```

#### Operators in decorrelated and optimized plans

Operator | Meaning | Example
---------|---------|---------
**Constant** | Always produces the same collection of rows. | `Constant`<br />`- ((1, 2) x 2)`<br />`- (3, 4)`
**Get** | Produces rows from either an existing source/view or from a previous operator in the same plan. | `Get materialize.public.ordered`
**Project** | Produces a subset of the columns in the input rows. | `Project (#2, #3)`
**Map** | Appends the results of some scalar expressions to each row in the input. | `Map (((#1 * 10000000dec) / #2) * 1000dec)`
**FlatMap** | Appends the result of some table function to each row in the input. | `FlatMap jsonb_foreach(#3)`
**Filter** | Removes rows of the input for which some scalar predicates return `false`. | `Filter (#20 < #21)`
**Join** | Returns combinations of rows from each input whenever some equality predicates are `true`. | `Join on=(#1 = #2)`
**CrossJoin** | An alias for a `Join` with an empty predicate (emits all combinations). | `CrossJoin`
**Reduce** | Groups the input rows by some scalar expressions, reduces each groups using some aggregate functions and produce rows containing the group key and aggregate outputs. | `Reduce group_by=[#0] aggregates=[max((#0 * #1))]`
**Distinct** | Alias for a `Reduce` with an empty aggregate list. | `Distinct`
**TopK** | Groups the inputs rows by some scalar expressions, sorts each group using the group key, removes the top `offset` rows in each group and returns the next `limit` rows.| `TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5 monotonic=false`
**Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input. | `Negate`
**Threshold** | Removes any rows with negative counts. | `Threshold`
**Union** | Sums the counts of each row of all inputs. | `Union`
**ArrangeBy** | Indicates a point that will become an arrangement in the dataflow engine (each `keys` element will be a different arrangement). | `ArrangeBy keys=[[#0]]`
**Return ... With ...**  | Binds sub-plans consumed multiple times by downstream operators. | [See above](#reading-decorrelatedoptimized-plans)

#### Operators in raw plans

Operator | Meaning | Example
---------|---------|---------
**Constant** | Always produces the same collection of rows. |`Constant`<br />`- ((1, 2) x 2)`<br />`- (3, 4)`
**Get** | Produces rows from either an existing source/view or from a previous operator in the same plan. | `Get materialize.public.ordered`
**Project** | Produces a subset of the columns in the input rows. | `Project (#2, #3)`
**Map** | Appends the results of some scalar expressions to each row in the input. | `Map (((#1 * 10000000dec) / #2) * 1000dec)`
**CallTable** | Appends the result of some table function to each row in the input. | `CallTable generate_series(1, 7, 1)`
**Filter** | Removes rows of the input for which some scalar predicates return false. | `Filter (#20 < #21)`
**~Join** | Performs one of `INNER` / `LEFT` / `RIGHT` / `FULL OUTER` / `CROSS` join on the two inputs, using the given predicate. | `InnerJoin (#3 = #5)`.
**Reduce** | Groups the input rows by some scalar expressions, reduces each group using some aggregate functions and produce rows containing the group key and aggregate outputs. In the case where the group key is empty and the input is empty, returns a single row with the aggregate functions applied to the empty input collection. | `Reduce group_by=[#2] aggregates=[min(#0), max(#0)]`
**Distinct** | Removes duplicate copies of input rows. | `Distinct`
**TopK** | Groups the inputs rows by some scalar expressions, sorts each group using the group key, removes the top `offset` rows in each group and returns the next `limit` rows. | `TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5`
**Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input. | `Negate`
**Threshold** | Removes any rows with negative counts. | `Threshold`
**Union** | Sums the counts of each row of all inputs. | `Union`
**Return ... With ...**  | Binds sub-plans consumed multiple times by downstream operators. | [See above](#reading-decorrelatedoptimized-plans)

### Timestamps

`EXPLAIN TIMESTAMP` displays the timestamps used for a `SELECT` statement, view, or materialized view -- valuable information to acknowledge query delays.

The explanation divides in two parts:
1. Determinations for a timestamp
2. Sources frontiers

#### Determinations for a timestamp

Queries in Materialize have a logical timestamp, known as _query timestamp_. It plays a critical role to return a correct result. Returning a correct result implies retrieving data with the same logical time from each source present in a query.

In this case, sources are objects providing data: materialized views, views, indexes, tables, or sources. Each will have a pair of logical timestamps frontiers, denoted as _sources frontiers_.

#### Sources frontiers

Every source has a beginning _read frontier_ and an ending _write frontier_. They stand for a source’s limits to return a correct result immediately:

* Read frontier: Indicates the minimum logical timestamp to return a correct result (known as _compaction_)
* Write frontier: Indicates the maximum timestamp to build a correct result without waiting unprocessed data.

Having a _query timestamp_ outside the values of the frontiers can explain the presence of delays. While in the middle, the space of processed but not yet compacted data, allows building and returning a correct result immediately.

#### Example

```sql
EXPLAIN TIMESTAMP FOR MATERIALIZED VIEW users;
```
```
                                 Timestamp
---------------------------------------------------------------------------
                 query timestamp: 1673618185152 (2023-01-13 13:56:25.152) +
           oracle read timestamp: 1673618185152 (2023-01-13 13:56:25.152) +
 largest not in advance of upper: 1673618185152 (2023-01-13 13:56:25.152) +
                           upper:[1673618185153 (2023-01-13 13:56:25.153)]+
                           since:[1673618184000 (2023-01-13 13:56:24.000)]+
         can respond immediately: true                                    +
                        timeline: Some(EpochMilliseconds)                 +
                                                                          +
 source materialize.public.raw_users (u2014, storage):                    +
                   read frontier:[1673618184000 (2023-01-13 13:56:24.000)]+
                  write frontier:[1673618185153 (2023-01-13 13:56:25.153)]+
```

#### Definitions

Field | Meaning | Example
---------|---------|---------
**query timestamp** | The query timestamp value |`1673612424151 (2023-01-13 12:20:24.151)`
**oracle read** | The value of the timeline's oracle timestamp, if used. | `1673612424151 (2023-01-13 12:20:24.151)`
**largest not in advance of upper** | The largest timestamp not in advance of upper. | `1673612424151 (2023-01-13 12:20:24.151)`
**since** | The maximum read frontier of all involved sources. | `[1673612423000 (2023-01-13 12:20:23.000)]`
**upper** | The minimum write frontier of all involved sources | `[1673612424152 (2023-01-13 12:20:24.152)]`
**can respond immediately** | Returns true when the **query timestamp** is greater or equal to **since** and lower than **upper** | `true`
**timeline** | The type of timeline the query's timestamp belongs | `Some(EpochMilliseconds)`


##### Timeline values
Field | Meaning | Example
---------|---------|---------
**EpochMilliseconds** | Means the timestamp is the number of milliseconds since the Unix epoch. | `Some(EpochMilliseconds)`
**External** | Means the timestamp comes from an external data source and we don't know what the number means. The attached String is the source's name, which will result in different sources being incomparable. | `Some(External)`
**User** | Means the user has manually specified a timeline. The attached String is specified by the user, allowing them to decide sources that are joinable. | `Some(User)`

##### Sources frontiers

Field | Meaning | Example
---------|---------|---------
**source** | Source’s identifiers | `source materialize.public.raw_users (u2014, storage)`
**read frontier** | Minimum logical timestamp. |`[1673612423000 (2023-01-13 12:20:23.000)]`
**write frontier** | Maximum logical timestamp. | `[1673612424152 (2023-01-13 12:20:24.152)]`

<!-- We think of `since` as the "read frontier": times not later than or equal to
`since` cannot be correctly read. We think of `upper` as the "write frontier":
times later than or equal to `upper` may still be written to the TVC. -->
<!-- Who is the oracle? -->
<!--
We maintain a timestamp oracle that returns strictly increasing timestamps
Mentions that this is inspired/similar to Percolator.

Timestamp oracle is periodically bumped up to the current system clock
We never revert oracle if system clock goes backwards.

https://tikv.org/deep-dive/distributed-transaction/timestamp-oracle/
-->
<!-- Materialize's objects request timestamp to the oracle, a timestamp provider. The oracle's timestamp bumps up periodically to match the current system clock, and never goes backwards.

It relies on an oracle, a timestamp provider, to handle them correctly.

The oracle it is a timestamp provider. It bumps up periodically internal value to the current system clock, never going backwards.

Issuing a select statement in Materialize
When a select statement runs, Materialize will pick a timestamp between all the sources:

`max(max(read_frontiers), min(write_frontiers) - 1)` -->

<!-- /// Information used when determining the timestamp for a query.
#[derive(Serialize, Deserialize)]
pub struct TimestampDetermination<T> {
    /// The chosen timestamp context from `determine_timestamp`.
    pub timestamp_context: TimestampContext<T>,
    /// The largest timestamp not in advance of upper.
    pub largest_not_in_advance_of_upper: T,
}


*Query timestamp: The timestamp in a timeline at which the query makes the read
oracle read: The value of the timeline's oracle timestamp, if used.
largest not in advance of upper: The largest timestamp not in advance of upper.
upper: The write frontier of all involved sources.
since: The read frontier of all involved sources.
can respond immediately: True when the write frontier is greater than the query timestamp.
timeline: The type of timeline the query's timestamp belongs:
      /// EpochMilliseconds means the timestamp is the number of milliseconds since
      /// the Unix epoch.
      EpochMilliseconds,
      /// External means the timestamp comes from an external data source and we
      /// don't know what the number means. The attached String is the source's name,
      /// which will result in different sources being incomparable.
      External(String),
      /// User means the user has manually specified a timeline. The attached
      /// String is specified by the user, allowing them to decide sources that are
      /// joinable.
      User(String),

Each source contains two frontiers:
  Read: At which time
  Write:

                 query timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
           oracle read timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
 largest not in advance of upper: 1673612424151 (2023-01-13 12:20:24.151) +
                           upper:[1673612424152 (2023-01-13 12:20:24.152)]+
                           since:[1673612423000 (2023-01-13 12:20:23.000)]+


                                 Timestamp
---------------------------------------------------------------------------
                 query timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
           oracle read timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
 largest not in advance of upper: 1673612424151 (2023-01-13 12:20:24.151) +
                           upper:[1673612424152 (2023-01-13 12:20:24.152)]+
                           since:[1673612423000 (2023-01-13 12:20:23.000)]+
         can respond immediately: true                                    +
                        timeline: Some(EpochMilliseconds)                 +
                                                                          +
 source materialize.public.a (u2014, storage):                            +
                   read frontier:[1673612423000 (2023-01-13 12:20:23.000)]+
                  write frontier:[1673612424152 (2023-01-13 12:20:24.152)]+ -->
