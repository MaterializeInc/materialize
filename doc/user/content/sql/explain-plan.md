---
title: "EXPLAIN PLAN"
description: "`EXPLAIN PLAN` is used to inspect the plans of `SELECT` statements, indexes, and materialized views."
aliases:
  - /sql/explain/
menu:
  main:
    parent: commands
---

`EXPLAIN PLAN` displays the plans used for `SELECT` statements, indexes, and materialized views.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

{{< diagram "explain-plan.svg" >}}

Note that the `FOR` keyword is required if the `PLAN` keyword is present. In other words, the following three statements are equivalent:

```sql
EXPLAIN <explainee>;
EXPLAIN PLAN FOR <explainee>;
EXPLAIN OPTIMIZED PLAN FOR <explainee>;
```

### Explained object

The following three objects can be explained.

Explained object | Description
------|-----
**select_stmt** | Display a plan for an ad-hoc [`SELECT` statement](../select).
**create_index** | Display a plan for a [`CREATE INDEX` statement](../create-index).
**create_materialized_view** | Display a plan for a [`CREATE MATERIALIZED VIEW` statement](../create-materialized-view).
**INDEX index_name** | Display the `OPTIMIZED` or `PHYSICAL` plan for an existing index.
**MATERIALIZED VIEW view_name** | Display the `OPTIMIZED` or `PHYSICAL` plan for an existing materialized view.

### Output format

You can select between `JSON` and `TEXT` for the output format of `EXPLAIN PLAN`. Non-text
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
**RAW PLAN** | Display the raw plan.
**DECORRELATED PLAN** | Display the decorrelated plan.
**OPTIMIZED PLAN** | _(Default)_ Display the optimized plan.
**PHYSICAL PLAN** | Display the physical plan.

### Output modifiers

Output modifiers act as boolean toggles and can be combined in order to slightly tweak
the information and rendering style of the generated explanation output.

Modifier | Description
------|-----
**arity** | Annotate each subplan with its number of produced columns. This is useful due to the use of offset-based column names.
**cardinality** | Annotate each subplan with a symbolic estimate of its cardinality.
**join_impls** | Render details about the implementation strategy of optimized MIR `Join` nodes.
**keys** | Annotate each subplan with its unique keys.
**node_ids** | Annotate each subplan in a `PHYSICAL PLAN` with its node ID.
**types** | Annotate each subplan with its inferred type.
**humanized_exprs** | Render `EXPLAIN AS TEXT` output with human-readable column references in operator expressions. **Warning**: SQL-level aliasing is not considered when inferring column names, so the plan output might become ambiguous if you use this modifier.
**filter_pushdown** | **Private preview** For each source, include a `pushdown` field that explains which filters [can be pushed down](../../transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

## Details

To execute `SELECT` statements, Materialize generates a plan consisting of
operators that interface with our underlying Differential dataflow engine.
`EXPLAIN PLAN` lets you see the plan for a given query, which can provide insight
into Materialize's behavior for specific queries, e.g. performance.

### Query compilation pipeline

The job of the Materialize planner is to turn SQL code into a differential
dataflow program. We get there via a series of progressively lower-level plans:

```text
SQL ⇒ raw plan ⇒ decorrelated plan ⇒ optimized plan ⇒ physical plan ⇒ dataflow
```

#### From SQL to raw plan

In this stage, the planner:

- Replaces SQL variable names with column numbers.
- Infers the type of each expression.
- Chooses the correct overload for each function.

#### From raw plan to decorrelated plan

In this stage, the planner:

- Replaces subqueries and lateral joins with non-nested operations.
- Replaces `OUTER` joins with lower-level operations.
- Replaces global aggregate default values with lower-level operations.

#### From decorrelated plan to optimized plan

In this stage, the planner performs various optimizing rewrites:

- Coalesces joins.
- Chooses join order and implementation.
- Fuses adjacent operations.
- Removes redundant operations.
- Pushes down predicates.
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

### Fast path queries

Queries are sometimes implemented using a _fast path_.
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


### Reading decorrelated and optimized plans

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
        ReadStorage materialize.public.t
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
    %0:t » %1:u[#0]K » %2:v[#0]K
    %1:u » %0:t[#1]K » %2:v[#0]K
    %2:v » %1:u[#1]K » %0:t[#1]K
  ArrangeBy keys=[[#1]]
    ReadStorage materialize.public.t
  ArrangeBy keys=[[#0], [#1]]
    ReadStorage materialize.public.u
  ArrangeBy keys=[[#0]]
    ReadStorage materialize.public.v
```
The `%0`, `%1`, etc. refer to each of the join inputs.
A *differential* join shows one join path, which is simply a sequence of binary
joins (each of whose results need to be maintained as state).
A [*delta* join](/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins)
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
    ReadStorage materialize.public.r
    ReadStorage materialize.public.s
```

Below the plan, a "Used indexes" section indicates which indexes will be used by the query, [and in what way](/transform-data/optimization/#use-explain-to-verify-index-usage).

### Reference: Operators in raw plans

Operator | Meaning | Example
---------|---------|---------
**Constant** | Always produces the same collection of rows. |`Constant`<br />`- ((1, 2) x 2)`<br />`- (3, 4)`
**Get** | Produces rows from either an existing source/view or from a previous operator in the same plan. | `Get materialize.public.ordered`
**Project** | Produces a subset of the columns in the input rows. | `Project (#2, #3)`
**Map** | Appends the results of some scalar expressions to each row in the input. | `Map (((#1 * 10000000dec) / #2) * 1000dec)`
**CallTable** | Appends the result of some table function to each row in the input. | `CallTable generate_series(1, 7, 1)`
**Filter** | Removes rows of the input for which some scalar predicates return false. | `Filter (#20 < #21)`
**~Join** | Performs one of `INNER` / `LEFT` / `RIGHT` / `FULL OUTER` / `CROSS` join on the two inputs, using the given predicate. | `InnerJoin (#3 = #5)`.
**Reduce** | Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs. In the case where the group key is empty and the input is empty, returns a single row with the aggregate functions applied to the empty input collection. | `Reduce group_by=[#2] aggregates=[min(#0), max(#0)]`
**Distinct** | Removes duplicate copies of input rows. | `Distinct`
**TopK** | Groups the inputs rows by some scalar expressions, sorts each group using the group key, removes the top `offset` rows in each group, and returns the next `limit` rows. | `TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5`
**Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input. | `Negate`
**Threshold** | Removes any rows with negative counts. | `Threshold`
**Union** | Sums the counts of each row of all inputs. | `Union`
**Return ... With ...**  | Binds sub-plans consumed multiple times by downstream operators. | [See above](#reading-decorrelated-and-optimized-plans)

### Reference: Operators in decorrelated and optimized plans

Operator | Meaning                                                                                                                                                                  | Example
---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------
**Constant** | Always produces the same collection of rows.                                                                                                                             | `Constant`<br />`- ((1, 2) x 2)`<br />`- (3, 4)`
**Get** | Produces rows from either an existing source/view or from a previous operator in the same plan.                                                                          | `Get materialize.public.ordered`
**Project** | Produces a subset of the columns in the input rows.                                                                                                                      | `Project (#2, #3)`
**Map** | Appends the results of some scalar expressions to each row in the input.                                                                                                 | `Map (((#1 * 10000000dec) / #2) * 1000dec)`
**FlatMap** | Appends the result of some table function to each row in the input.                                                                                                      | `FlatMap jsonb_foreach(#3)`
**Filter** | Removes rows of the input for which some scalar predicates return `false`.                                                                                               | `Filter (#20 < #21)`
**Join** | Returns combinations of rows from each input whenever some equality predicates are `true`.                                                                               | `Join on=(#1 = #2)`
**CrossJoin** | An alias for a `Join` with an empty predicate (emits all combinations).                                                                                                  | `CrossJoin`
**Reduce** | Groups the input rows by some scalar expressions, reduces each groups using some aggregate functions, and produce rows containing the group key and aggregate outputs.   | `Reduce group_by=[#0] aggregates=[max((#0 * #1))]`
**Distinct** | Alias for a `Reduce` with an empty aggregate list.                                                                                                                       | `Distinct`
**TopK** | Groups the inputs rows by some scalar expressions, sorts each group using the group key, removes the top `offset` rows in each group, and returns the next `limit` rows. | `TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5`
**Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.                                           | `Negate`
**Threshold** | Removes any rows with negative counts.                                                                                                                                   | `Threshold`
**Union** | Sums the counts of each row of all inputs.                                                                                                                               | `Union`
**ArrangeBy** | Indicates a point that will become an arrangement in the dataflow engine (each `keys` element will be a different arrangement). Note that if the output of the previous operator is already arranged with a key that is also requested here, then this operator will just pass on that existing arrangement instead of creating a new one.                                         | `ArrangeBy keys=[[#0]]`
**Return ... With ...**  | Binds sub-plans consumed multiple times by downstream operators.                                                                                                         | [See above](#reading-decorrelated-and-optimized-plans)

## Examples

For the following examples, let's assume that you have [the auction house load generator](/sql/create-source/load-generator/#creating-an-auction-load-generator) created in your current environment.

### Explaining a `SELECT` query

Let's start with a simple join query that lists the total amounts bid per buyer.

Explain the optimized plan as text:

```sql
EXPLAIN
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Same as above, but a bit more verbose:

```sql
EXPLAIN PLAN
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Same as above, but even more verbose:

```sql
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Same as above, but every sub-plan is annotated with its schema types:

```sql
EXPLAIN WITH(types) FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Explain the physical plan as text:

```sql
EXPLAIN PHYSICAL PLAN FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

### Explaining an index on a view

Let's create a view with an index for the above query.

```sql
-- create the view
CREATE VIEW my_view AS
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
-- create an index on the view
CREATE INDEX my_view_idx ON my_view(id);
```

You can inspect the plan of the dataflow that will maintain your index with the following statements.

Explain the optimized plan as text:

```sql
EXPLAIN
INDEX my_view_idx;
```

Same as above, but a bit more verbose:

```sql
EXPLAIN PLAN FOR
INDEX my_view_idx;
```

Same as above, but even more verbose:

```sql
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
INDEX my_view_idx;
```

Same as above, but every sub-plan is annotated with its schema types:

```sql
EXPLAIN WITH(types) FOR
INDEX my_view_idx;
```

Explain the physical plan as text:

```sql
EXPLAIN PHYSICAL PLAN FOR
INDEX my_view_idx;
```

### Explaining a materialized view

Let's create a materialized view for the above `SELECT` query.

```sql
CREATE MATERIALIZED VIEW my_mat_view AS
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

You can inspect the plan of the dataflow that will maintain your view with the following statements.

Explain the optimized plan as text:

```sql
EXPLAIN
MATERIALIZED VIEW my_mat_view;
```

Same as above, but a bit more verbose:

```sql
EXPLAIN PLAN FOR
MATERIALIZED VIEW my_mat_view;
```

Same as above, but even more verbose:

```sql
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
MATERIALIZED VIEW my_mat_view;
```

Same as above, but every sub-plan is annotated with its schema types:

```sql
EXPLAIN WITH(types)
MATERIALIZED VIEW my_mat_view;
```

Explain the physical plan as text:

```sql
EXPLAIN PHYSICAL PLAN FOR
MATERIALIZED VIEW my_mat_view;
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the explainee are contained in.
