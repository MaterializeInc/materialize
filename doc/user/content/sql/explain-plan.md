---
title: "EXPLAIN PLAN"
description: "Reference page for `EXPLAIN PLAN`. `EXPLAIN PLAN` is used to inspect the plans of `SELECT` statements, indexes, and materialized views."
aliases:
  - /sql/explain/
menu:
  main:
    parent: commands
---

`EXPLAIN PLAN` displays the plans used for:

|                             |                       |
|-----------------------------|-----------------------|
| <ul><li>`SELECT` statements </li><li>`CREATE VIEW` statements</li><li>`CREATE INDEX` statements</li><li>`CREATE MATERIALIZED VIEW` statements</li></ul>|<ul><li>Existing views</li><li>Existing indexes</li><li>Existing materialized views</li></ul> |

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

{{< tabs >}}
{{< tab "FOR SELECT">}}
```mzsql
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]       -- The FOR keyword is required if the PLAN keyword is specified
    <SELECT ...>
;
```
{{</tab>}}
{{< tab "FOR CREATE VIEW">}}

```mzsql
EXPLAIN <RAW | DECORRELATED | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR
    <CREATE VIEW ...>
;
```
{{</tab>}}
{{< tab "FOR CREATE INDEX">}}
```mzsql
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE INDEX ...>
;
```
{{</tab>}}
{{< tab "FOR CREATE MATERIALIZED VIEW">}}
```mzsql
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]          -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE MATERIALIZED VIEW ...>
;
```
{{</tab>}}
{{< tab "FOR VIEW">}}
```mzsql
EXPLAIN <RAW | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR
  VIEW <name>
;
```
{{</tab>}}
{{< tab "FOR INDEX">}}
```mzsql
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
      [ WITH (<output_modifier> [, <output_modifier> ...]) ]
      [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
  INDEX <name>
;
```
{{</tab>}}
{{< tab "FOR MATERIALIZED VIEW">}}
```mzsql
EXPLAIN [[ RAW | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ] -- The FOR keyword is required if the PLAN keyword is specified
  MATERIALIZED VIEW <name>
;
```
{{</tab>}}
{{</tabs>}}

Note that the `FOR` keyword is required if the `PLAN` keyword is present. The following three statements are equivalent:

```mzsql
EXPLAIN <explainee>;
EXPLAIN PLAN FOR <explainee>;
EXPLAIN PHYSICAL PLAN AS TEXT FOR <explainee>;
```

If `PHSYICAL PLAN` is specified without an `AS`-format, we will provide output similar to the above, but more verbose. The following two statements are equivalent (and produce the more verbose output):

```mzsql
EXPLAIN PHYSICAL PLAN FOR <explainee>;
EXPLAIN PHYSICAL PLAN AS VERBOSE TEXT FOR <explainee>;
```

### Explained object

The following object types can be explained.

Explained object | Description
------|-----
**select_stmt** | Display a plan for an ad-hoc [`SELECT` statement](../select).
**create_view** | Display a plan for a [`CREATE VIEW` statement](../create-view).
**create_index** | Display a plan for a [`CREATE INDEX` statement](../create-index).
**create_materialized_view** | Display a plan for a [`CREATE MATERIALIZED VIEW` statement](../create-materialized-view).
**VIEW name** | Display the `RAW` or `LOCALLY OPTIMIZED` plan for an existing view.
**INDEX name** | Display the `OPTIMIZED` or `PHYSICAL` plan for an existing index.
**MATERIALIZED VIEW name** | Display the `OPTIMIZED` or `PHYSICAL` plan for an existing materialized view.

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
**RAW PLAN** | Display the raw plan; this is closest to the original SQL.
**DECORRELATED PLAN** | Display the decorrelated but not-yet-optimized plan.
**LOCALLY OPTIMIZED** | Display the locally optimized plan (before view inlining and access path selection). This is the final stage for regular `CREATE VIEW` optimization.
**OPTIMIZED PLAN** | Display the optimized plan.
**PHYSICAL PLAN** | _(Default)_ Display the physical plan; this corresponds to the operators shown in [`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping).

### Output modifiers

Output modifiers act as boolean toggles and can be combined in order to slightly tweak
the information and rendering style of the generated explanation output.

Modifier | Description
------|-----
**arity** | _(on by default)_ Annotate each subplan with its number of produced columns. This is useful due to the use of offset-based column names.
**cardinality** | Annotate each subplan with a symbolic estimate of its cardinality.
**join implementations** | Render details about the [implementation strategy of optimized MIR `Join` nodes](#explain-with-join-implementations).
**keys** | Annotates each subplan with a parenthesized list of unique keys. Each unique key is presented as a bracketed list of column identifiers. A list of column identifiers is reported as a unique key when for each setting of those columns to values there is at most one record in the collection. For example, `([0], [1,2])` is a list of two unique keys: column zero is a unique key, and columns 1 and 2 also form a unique key. Materialize only reports the most succinct form of keys, so for example while `[0]` and `[0, 1]` might both be unique keys, the latter is implied by the former and omitted. `()` indicates that the collection does not have any unique keys, while `([])` indicates that the empty projection is a unique key, meaning that the collection consists of 0 or 1 rows.
**node identifiers** | Annotate each subplan in a `PHYSICAL PLAN` with its node ID.
**redacted** | Anonymize literals in the output.
**timing** | Annotate the output with the optimization time.
**types** | Annotate each subplan with its inferred type.
**humanized expressions** | _(on by default)_ Add human-readable column names to column references. For example, `#0{id}` refers to column 0, whose name is `id`. Note that SQL-level aliasing is not considered when inferring column names, which means that the displayed column names can be ambiguous.
**filter pushdown** | _(on by default)_ For each source, include a `pushdown` field that explains which filters [can be pushed down to the storage layer](../../transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

Note that most modifiers are currently only supported for the `AS TEXT` output.

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

- Decides on the exact execution details of each operator, and maps plan operators to differential dataflow operators.
- Makes the final choices about creating or reusing [arrangements](/get-started/arrangements/#arrangements).

#### From physical plan to dataflow

In the final stage, the planner:

- Renders an actual dataflow from the physical plan, and
- Installs the new dataflow into the running system.

The rendering step does not make any further optimization choices, as the physical plan is meant to
be a definitive and complete description of the rendered dataflow.

### Fast path queries

Queries are sometimes implemented using a _fast path_.
In this mode, the program that implements the query will just hit an existing index,
transform the results, and optionally apply a finishing action. For fast path queries,
all of these actions happen outside of the regular dataflow engine. The fast path is
indicated by an "Explained Query (fast path):" heading before the explained query in the `EXPLAIN`,
`EXPLAIN OPTIMIZED PLAN` and `EXPLAIN PHYSICAL PLAN` result.

```text
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=materialize.public.t1 t1_x_idx=[lookup value=(5)]

Used Indexes:
  - materialize.public.t1_x_idx (lookup)
```


### Reading plans

Materialize plans are directed, potentially cyclic, graphs of operators. Each operator in the graph
receives inputs from zero or more other operators and produces a single output.
Sub-graphs where each output is consumed only once are rendered as tree-shaped fragments.
Sub-graphs consumed more than once are represented as common table expressions (CTEs).
In the example below, the CTE `l0` represents a linear sub-plan (a chain of `Read` from the table `t`)
which is used in both inputs of a self-join (`Differential Join`).

```text
> CREATE TABLE t(x INT NOT NULL, y INT NOT NULL);
CREATE TABLE
> EXPLAIN SELECT t1.x, t1.y
          FROM (SELECT * FROM t WHERE x > y) AS t1,
               (SELECT * FROM t where x > y) AS t2
          WHERE t1.y = t2.y;
                     Physical Plan
--------------------------------------------------------
 Explained Query:                                      +
   →With                                               +
     cte l0 =                                          +
       →Read materialize.public.t                      +
   →Return                                             +
     →Differential Join %0 » %1                        +
       Join stage %0: Lookup key #0{y} in %1           +
       →Arrange                                        +
         Keys: 1 arrangement available, plus raw stream+
           Arrangement 0: #1{y}                        +
         →Stream l0                                    +
       →Arrange                                        +
         Keys: 1 arrangement available, plus raw stream+
           Arrangement 0: #0{y}                        +
         →Read l0                                      +
                                                       +
 Source materialize.public.t                           +
   filter=((#0{x} > #1{y}))                            +
                                                       +
 Target cluster: quickstart                            +
```

Note that CTEs in optimized plans do not directly correspond to CTEs in your original SQL query: For example, CTEs might disappear due to inlining (i.e., when a CTE is used only once, its definition is copied to that usage site); new CTEs can appear due to the optimizer recognizing that a part of the query appears more than once (aka common subexpression elimination). Also, certain SQL-level concepts, such as outer joins or subqueries, do not have an explicit representation in optimized plans, and are instead expressed as a pattern of operators involving CTEs. CTE names are always `l0`, `l1`, `l2`, ..., and do not correspond to SQL-level CTE names.

<a name="explain-plan-columns"></a>

Many operators need to refer to columns in their input. These are displayed like
`#3` for column number 3. (Columns are numbered starting from column 0). To get a better sense of
columns assigned to `Map` operators, it might be useful to request [the `arity` output modifier](#output-modifiers).

Each operator can also be annotated with additional metadata. Some details are shown in the default `EXPLAIN` output (`EXPLAIN PHYSICAL PLAN AS TEXT`), but are hidden elsewhere. <a
name="explain-with-join-implementations"></a>In `EXPLAIN OPTIMIZED
PLAN`, details about the implementation in the `Join` operator can be requested
with [the `join implementations` output modifier](#output-modifiers) (that is,
`EXPLAIN OPTIMIZED PLAN WITH (join implementations) FOR ...`).

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

A plan can optionally end with a finishing action, which can sort, limit and
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

### Reference: Plan operators

Materialize offers several output formats for `EXPLAIN` and debugging.
LIR plans as rendered in
[`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping)
are deliberately succinct, while the plans in other formats give more
detail.

The decorrelated and optimized plans from `EXPLAIN DECORRELATED PLAN
FOR ...`, `EXPLAIN LOCALLY OPTIMIZED PLAN FOR ...`, and `EXPLAIN
OPTIMIZED PLAN FOR ...` are in a mid-level representation that is
closer to LIR than SQL. The raw plans from `EXPLAIN RAW PLAN FOR ...`
are closer to SQL (and therefore less indicative of how the query will
actually run).

{{< tabs >}}

{{< tab "In fully optimized physical (LIR) plans" >}}
{{< explain-plans/operator-table data="explain_plan_operators" planType="LIR" >}}
{{< /tab >}}

{{< tab "In decorrelated and optimized plans (default EXPLAIN)" >}}
{{< explain-plans/operator-table data="explain_plan_operators" planType="optimized" >}}
{{< /tab >}}

{{< tab "In raw plans" >}}
{{< explain-plans/operator-table data="explain_plan_operators" planType="raw" >}}
{{< /tab >}}

{{< /tabs >}}

Operators are sometimes marked as `Fused ...`. We write this to mean that the operator is fused with its input, i.e., the operator below it. That is, if you see a `Fused X` operator above a `Y` operator:

```
→Fused X
  →Y
```

Then the `X` and `Y` operators will be combined into a single, more efficient operator.

## Examples

For the following examples, let's assume that you have [the auction house load generator](/sql/create-source-v1/load-generator/#creating-an-auction-load-generator) created in your current environment.

### Explaining a `SELECT` query

Let's start with a simple join query that lists the total amounts bid per buyer.

Explain the optimized plan as text:

```mzsql
EXPLAIN
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Same explanation as above, but with the `EXPLAIN` expressed a bit more verbosely:

```mzsql
EXPLAIN PLAN FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Same explanation as above, but expressed even more verbosely:

```mzsql
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Same as above, but every sub-plan is annotated with its schema types:

```mzsql
EXPLAIN WITH(types) FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Explain the physical plan as verbose text (i.e., in complete detail):

```mzsql
EXPLAIN PHYSICAL PLAN FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

### Explaining an index on a view

Let's create a view with an index for the above query.

```mzsql
-- create the view
CREATE VIEW my_view AS
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
-- create an index on the view
CREATE INDEX my_view_idx ON my_view(id);
```

You can inspect the plan of the dataflow that will maintain your index with the following statements.

Explain the optimized plan as text:

```mzsql
EXPLAIN
INDEX my_view_idx;
```

Same as above, but a bit more verbose:

```mzsql
EXPLAIN PLAN FOR
INDEX my_view_idx;
```

Same as above, but even more verbose:

```mzsql
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
INDEX my_view_idx;
```

Same as above, but every sub-plan is annotated with its schema types:

```mzsql
EXPLAIN WITH(types) FOR
INDEX my_view_idx;
```

Explain the physical plan as verbose text:

```mzsql
EXPLAIN PHYSICAL PLAN FOR
INDEX my_view_idx;
```

### Explaining a materialized view

Let's create a materialized view for the above `SELECT` query.

```mzsql
CREATE MATERIALIZED VIEW my_mat_view AS
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

You can inspect the plan of the dataflow that will maintain your view with the following statements.

Explain the optimized plan as text:

```mzsql
EXPLAIN
MATERIALIZED VIEW my_mat_view;
```

Same as above, but a bit more verbose:

```mzsql
EXPLAIN PLAN FOR
MATERIALIZED VIEW my_mat_view;
```

Same as above, but even more verbose:

```mzsql
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
MATERIALIZED VIEW my_mat_view;
```

Same as above, but every sub-plan is annotated with its schema types:

```mzsql
EXPLAIN WITH(types)
MATERIALIZED VIEW my_mat_view;
```

Explain the physical plan as verbose text:

```mzsql
EXPLAIN PHYSICAL PLAN FOR
MATERIALIZED VIEW my_mat_view;
```

## Debugging running dataflows

The [`EXPLAIN ANALYZE`](/sql/explain-analyze/) statement will let you debug memory and cpu usage (optionally with information about worker skew) for existing indexes and materialized views in terms of their physical plan operators. It can also attribute [TopK hints](/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1) to individual operators.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/explain-plan.md" >}}
