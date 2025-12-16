<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# EXPLAIN PLAN

`EXPLAIN PLAN` displays the plans used for:

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td><ul>
<li><code>SELECT</code> statements</li>
<li><code>CREATE VIEW</code> statements</li>
<li><code>CREATE INDEX</code> statements</li>
<li><code>CREATE MATERIALIZED VIEW</code> statements</li>
</ul></td>
<td><ul>
<li>Existing views</li>
<li>Existing indexes</li>
<li>Existing materialized views</li>
</ul></td>
</tr>
</tbody>
</table>

<div class="warning">

**WARNING!** `EXPLAIN` is not part of Materialize’s stable interface and
is not subject to our backwards compatibility guarantee. The syntax and
output of `EXPLAIN` may change arbitrarily in future versions of
Materialize.

</div>

## Syntax

<div class="code-tabs">

<div class="tab-content">

<div id="tab-for-select" class="tab-pane" title="FOR SELECT">

<div class="highlight">

``` chroma
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]       -- The FOR keyword is required if the PLAN keyword is specified
    <SELECT ...>
;
```

</div>

</div>

<div id="tab-for-create-view" class="tab-pane" title="FOR CREATE VIEW">

<div class="highlight">

``` chroma
EXPLAIN <RAW | DECORRELATED | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR
    <CREATE VIEW ...>
;
```

</div>

</div>

<div id="tab-for-create-index" class="tab-pane"
title="FOR CREATE INDEX">

<div class="highlight">

``` chroma
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE INDEX ...>
;
```

</div>

</div>

<div id="tab-for-create-materialized-view" class="tab-pane"
title="FOR CREATE MATERIALIZED VIEW">

<div class="highlight">

``` chroma
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]          -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE MATERIALIZED VIEW ...>
;
```

</div>

</div>

<div id="tab-for-view" class="tab-pane" title="FOR VIEW">

<div class="highlight">

``` chroma
EXPLAIN <RAW | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR
  VIEW <name>
;
```

</div>

</div>

<div id="tab-for-index" class="tab-pane" title="FOR INDEX">

<div class="highlight">

``` chroma
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
      [ WITH (<output_modifier> [, <output_modifier> ...]) ]
      [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
  INDEX <name>
;
```

</div>

</div>

<div id="tab-for-materialized-view" class="tab-pane"
title="FOR MATERIALIZED VIEW">

<div class="highlight">

``` chroma
EXPLAIN [[ RAW | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ] -- The FOR keyword is required if the PLAN keyword is specified
  MATERIALIZED VIEW <name>
;
```

</div>

</div>

</div>

</div>

Note that the `FOR` keyword is required if the `PLAN` keyword is
present. The following four statements are equivalent:

<div class="highlight">

``` chroma
EXPLAIN <explainee>;
EXPLAIN PLAN FOR <explainee>;
EXPLAIN PHYSICAL PLAN FOR <explainee>;
EXPLAIN PHYSICAL PLAN AS TEXT FOR <explainee>;
```

</div>

### Explained object

The following object types can be explained.

| Explained object | Description |
|----|----|
| **select_stmt** | Display a plan for an ad-hoc [`SELECT` statement](../select). |
| **create_view** | Display a plan for a [`CREATE VIEW` statement](../create-view). |
| **create_index** | Display a plan for a [`CREATE INDEX` statement](../create-index). |
| **create_materialized_view** | Display a plan for a [`CREATE MATERIALIZED VIEW` statement](../create-materialized-view). |
| **VIEW name** | Display the `RAW` or `LOCALLY OPTIMIZED` plan for an existing view. |
| **INDEX name** | Display the `OPTIMIZED` or `PHYSICAL` plan for an existing index. |
| **MATERIALIZED VIEW name** | Display the `OPTIMIZED` or `PHYSICAL` plan for an existing materialized view. |

### Output format

You can select between `JSON` and `TEXT` for the output format of
`EXPLAIN PLAN`. Non-text output is more machine-readable and can be
parsed by common graph visualization libraries, while formatted text is
more human-readable.

| Output type | Description                                     |
|-------------|-------------------------------------------------|
| **TEXT**    | Format the explanation output as UTF-8 text.    |
| **JSON**    | Format the explanation output as a JSON object. |

### Explained stage

This stage determines the query optimization stage at which the plan
snapshot will be taken.

| Plan Stage | Description |
|----|----|
| **RAW PLAN** | Display the raw plan; this is closest to the original SQL. |
| **DECORRELATED PLAN** | Display the decorrelated but not-yet-optimized plan. |
| **LOCALLY OPTIMIZED** | Display the locally optimized plan (before view inlining and access path selection). This is the final stage for regular `CREATE VIEW` optimization. |
| **OPTIMIZED PLAN** | Display the optimized plan. |
| **PHYSICAL PLAN** | *(Default)* Display the physical plan; this corresponds to the operators shown in [`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping). |

### Output modifiers

Output modifiers act as boolean toggles and can be combined in order to
slightly tweak the information and rendering style of the generated
explanation output.

| Modifier | Description |
|----|----|
| **arity** | *(on by default)* Annotate each subplan with its number of produced columns. This is useful due to the use of offset-based column names. |
| **cardinality** | Annotate each subplan with a symbolic estimate of its cardinality. |
| **join implementations** | Render details about the [implementation strategy of optimized MIR `Join` nodes](#explain-with-join-implementations). |
| **keys** | Annotates each subplan with a parenthesized list of unique keys. Each unique key is presented as a bracketed list of column identifiers. A list of column identifiers is reported as a unique key when for each setting of those columns to values there is at most one record in the collection. For example, `([0], [1,2])` is a list of two unique keys: column zero is a unique key, and columns 1 and 2 also form a unique key. Materialize only reports the most succinct form of keys, so for example while `[0]` and `[0, 1]` might both be unique keys, the latter is implied by the former and omitted. `()` indicates that the collection does not have any unique keys, while `([])` indicates that the empty projection is a unique key, meaning that the collection consists of 0 or 1 rows. |
| **node identifiers** | Annotate each subplan in a `PHYSICAL PLAN` with its node ID. |
| **redacted** | Anonymize literals in the output. |
| **timing** | Annotate the output with the optimization time. |
| **types** | Annotate each subplan with its inferred type. |
| **humanized expressions** | *(on by default)* Add human-readable column names to column references. For example, `#0{id}` refers to column 0, whose name is `id`. Note that SQL-level aliasing is not considered when inferring column names, which means that the displayed column names can be ambiguous. |
| **filter pushdown** | *(on by default)* For each source, include a `pushdown` field that explains which filters [can be pushed down to the storage layer](../../transform-data/patterns/temporal-filters/#temporal-filter-pushdown). |

Note that most modifiers are currently only supported for the `AS TEXT`
output.

## Details

To execute `SELECT` statements, Materialize generates a plan consisting
of operators that interface with our underlying Differential dataflow
engine. `EXPLAIN PLAN` lets you see the plan for a given query, which
can provide insight into Materialize’s behavior for specific queries,
e.g. performance.

### Query compilation pipeline

The job of the Materialize planner is to turn SQL code into a
differential dataflow program. We get there via a series of
progressively lower-level plans:

<div class="highlight">

``` chroma
SQL ⇒ raw plan ⇒ decorrelated plan ⇒ optimized plan ⇒ physical plan ⇒ dataflow
```

</div>

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

- Decides on the exact execution details of each operator, and maps plan
  operators to differential dataflow operators.
- Makes the final choices about creating or reusing
  [arrangements](/docs/self-managed/v25.2/get-started/arrangements/#arrangements).

#### From physical plan to dataflow

In the final stage, the planner:

- Renders an actual dataflow from the physical plan, and
- Installs the new dataflow into the running system.

The rendering step does not make any further optimization choices, as
the physical plan is meant to be a definitive and complete description
of the rendered dataflow.

### Fast path queries

Queries are sometimes implemented using a *fast path*. In this mode, the
program that implements the query will just hit an existing index,
transform the results, and optionally apply a finishing action. For fast
path queries, all of these actions happen outside of the regular
dataflow engine. The fast path is indicated by an “Explained Query (fast
path):” heading before the explained query in the
`EXPLAIN OPTIMIZED PLAN` and `EXPLAIN PHYSICAL PLAN` result.

<div class="highlight">

``` chroma
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=materialize.public.t1 t1_x_idx=[lookup value=(5)]

Used Indexes:
  - materialize.public.t1_x_idx (lookup)
```

</div>

### Reading decorrelated and optimized plans

Materialize plans are directed, potentially cyclic, graphs of operators.
Each operator in the graph receives inputs from zero or more other
operators and produces a single output. Sub-graphs where each output is
consumed only once are rendered as tree-shaped fragments. Sub-graphs
consumed more than once are represented as common table expressions
(CTEs). In the example below, the CTE `l0` represents a linear sub-plan
(a chain of `Read` from the table `t`) which is used in both inputs of a
self-join (`Differential Join`).

<div class="highlight">

``` chroma
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

</div>

Note that CTEs in optimized plans do not directly correspond to CTEs in
your original SQL query: For example, CTEs might disappear due to
inlining (i.e., when a CTE is used only once, its definition is copied
to that usage site); new CTEs can appear due to the optimizer
recognizing that a part of the query appears more than once (aka common
subexpression elimination). Also, certain SQL-level concepts, such as
outer joins or subqueries, do not have an explicit representation in
optimized plans, and are instead expressed as a pattern of operators
involving CTEs. CTE names are always `l0`, `l1`, `l2`, …, and do not
correspond to SQL-level CTE names.

<span id="explain-plan-columns"></span>

Many operators need to refer to columns in their input. These are
displayed like `#3` for column number 3. (Columns are numbered starting
from column 0). To get a better sense of columns assigned to `Map`
operators, it might be useful to request [the `arity` output
modifier](#output-modifiers).

Each operator can also be annotated with additional metadata. Details
are shown by default in the `EXPLAIN PHYSICAL PLAN` output (the
default), but are hidden elsewhere.
<span id="explain-with-join-implementations"></span>In
`EXPLAIN OPTIMIZED PLAN`, details about the implementation in the `Join`
operator can be requested with [the `join implementations` output
modifier](#output-modifiers) (that is,
`EXPLAIN OPTIMIZED PLAN WITH (join implementations) FOR ...`).

<div class="highlight">

``` chroma
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

</div>

The `%0`, `%1`, etc. refer to each of the join inputs. A *differential*
join shows one join path, which is simply a sequence of binary joins
(each of whose results need to be maintained as state). A [*delta*
join](/docs/self-managed/v25.2/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins)
shows a join path for each of the inputs. The expressions in a bracket
show the key for joining with that input. The letters after the brackets
indicate the input characteristics used for join ordering. `U` means
unique, the number of `K`s means the key length, `A` means already
arranged (e.g., an index exists). The small letters refer to filter
characteristics: **e**quality to a literal, **l**ike, is **n**ull,
**i**nequality to a literal, any **f**ilter.

A plan can optionally end with a finishing action, which can sort, limit
and project the result data. This operator is special, as it can only
occur at the top of the plan. Finishing actions are executed outside the
parallel dataflow that implements the rest of the plan.

```
Finish order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5 output=[#0, #1]
  CrossJoin
    ReadStorage materialize.public.r
    ReadStorage materialize.public.s
```

Below the plan, a “Used indexes” section indicates which indexes will be
used by the query, [and in what
way](/docs/self-managed/v25.2/transform-data/optimization/#use-explain-to-verify-index-usage).

### Reference: Plan operators

Materialize offers several output formats for `EXPLAIN` and debugging.
LIR plans as rendered in
[`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping)
are deliberately succinct, while the plans in other formats give more
detail.

The decorrelated and optimized plans from
`EXPLAIN DECORRELATED PLAN FOR ...`,
`EXPLAIN LOCALLY OPTIMIZED PLAN FOR ...`, and
`EXPLAIN OPTIMIZED PLAN FOR ...` are in a mid-level representation that
is closer to LIR than SQL. The raw plans from `EXPLAIN RAW PLAN FOR ...`
are closer to SQL (and therefore less indicative of how the query will
actually run).

<div class="code-tabs">

<div class="tab-content">

<div id="tab-in-fully-optimized-physical-lir-plans" class="tab-pane"
title="In fully optimized physical (LIR) plans">

The following table lists the operators that are available in the LIR
plan.

- For those operators that require memory to maintain intermediate
  state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or
  columns), **Can increase data size** is marked with **Yes**.

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Operator</th>
<th>Description</th>
<th>Example</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Constant</strong></td>
<td>Always produces the same collection of rows.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>→Constant (2 rows)</code></td>
</tr>
<tr>
<td><strong>Stream, Arranged, Index Lookup, Read</strong></td>
<td><p>Produces rows from either an existing relation
(source/view/materialized view/table) or from a previous CTE in the same
plan. A parent <code>Fused Map/Filter/Project</code> operator can
combine with this operator.</p>
<p>There are four types of <code>Get</code>.</p>
<ol>
<li><p><code>Stream</code> indicates that the results are not <a
href="/docs/self-managed/v25.2/get-started/arrangements/#arrangements">arranged</a>
in memory and will be streamed directly.</p></li>
<li><p><code>Arranged</code> indicates that the results are <a
href="/docs/self-managed/v25.2/get-started/arrangements/#arrangements">arranged</a>
in memory.</p></li>
<li><p><code>Index Lookup</code> indicates the results will be
<em>looked up</em> in an existing
[arrangement]((/get-started/arrangements/#arrangements).</p></li>
<li><p><code>Read</code> indicates that the results are unarranged, and
will be processed as they arrive.</p></li>
</ol>
<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>Arranged materialize.public.t</code></td>
</tr>
<tr>
<td><strong>Map/Filter/Project</strong></td>
<td><p>Computes new columns (maps), filters columns, and projects away
columns. Works row-by-row. Maps and filters will be printed, but
projects will not.</p>
<p>These may be marked as <strong><code>Fused</code></strong>
<code>Map/Filter/Project</code>, which means they will combine with the
operator beneath them to run more efficiently.</p>
<br />
<br />
<strong>Can increase data size:</strong> Each row may have more data,
from the <code>Map</code>. Each row may also have less data, from the
<code>Project</code>. There may be fewer rows, from the
<code>Filter</code>.<br />
<strong>Uses memory:</strong> No</td>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>→Map/Filter/Project
  Filter: (#0{a} &lt; 7)
  Map: (#0{a} + #1{b})</code></pre>
</div></td>
</tr>
<tr>
<td><strong>Table Function</strong></td>
<td><p>Appends the result of some (one-to-many) <a
href="/docs/self-managed/v25.2/sql/functions/#table-functions">table
function</a> to each row in the input.</p>
<p>A parent <code>Fused Table Function unnest_list</code> operator will
fuse with its child <code>GroupAggregate</code> operator. Fusing these
operator is part of how we efficiently compile window functions from SQL
to dataflows.</p>
<p>A parent <code>Fused Map/Filter/Project</code> can combine with this
operator.</p>
<br />
<br />
<strong>Can increase data size:</strong> Depends on the <a
href="/docs/self-managed/v25.2/sql/functions/#table-functions">table
function</a> used.<br />
<strong>Uses memory:</strong> No</td>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>→Table Function generate_series(#0{a}, #1{b}, 1)
  Input key: (#0{a})</code></pre>
</div></td>
</tr>
<tr>
<td><strong>Differential Join, Delta Join</strong></td>
<td><p>Both join operators indicate the join ordering selected.</p>
<p>Returns combinations of rows from each input whenever some equality
predicates are <code>true</code>.</p>
<p>Joins will indicate the join order of their children, starting from
0. For example, <code>Differential Join %1 » %0</code> will join its
second child into its first.</p>
<p>The <a
href="/docs/self-managed/v25.2/transform-data/optimization/#join">two
joins differ in performance characteristics</a>.</p>
<br />
<br />
<strong>Can increase data size:</strong> Depends on the join order and
facts about the joined collections.<br />
<strong>Uses memory:</strong> ✅ Uses memory for 3-way or more
differential joins.</td>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>→Differential Join %1 » %0
  Join stage %0: Lookup key #0{a} in %0</code></pre>
</div></td>
</tr>
<tr>
<td><strong>GroupAggregate</strong></td>
<td><p>Groups the input rows by some scalar expressions, reduces each
group using some aggregate functions, and produces rows containing the
group key and aggregate outputs.</p>
<p>There are five types of <code>GroupAggregate</code>, ordered by
increasing complexity:</p>
<ol>
<li><p><code>Distinct GroupAggregate</code> corresponds to the SQL
<code>DISTINCT</code> operator.</p></li>
<li><p><code>Accumulable GroupAggregate</code> (e.g., <code>SUM</code>,
<code>COUNT</code>) corresponds to several easy to implement
aggregations that can be executed simultaneously and
efficiently.</p></li>
<li><p><code>Hierarchical GroupAggregate</code> (e.g., <code>MIN</code>,
<code>MAX</code>) corresponds to an aggregation requiring a tower of
arrangements. These can be either monotonic (more efficient) or
bucketed. These may benefit from a hint; <a
href="/docs/self-managed/v25.2/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice">see
<code>mz_introspection.mz_expected_group_size_advice</code></a>. These
may either be bucketed or monotonic (more efficient). These may
consolidate their output, which will increase memory usage.</p></li>
<li><p><code>Collated Multi-GroupAggregate</code> corresponds to an
arbitrary mix of reductions of different types, which will be performed
separately and then joined together.</p></li>
<li><p><code>Non-incremental GroupAggregate</code> (e.g., window
functions, <code>list_agg</code>) corresponds to a single
non-incremental aggregation. These are the most computationally
intensive reductions.</p></li>
</ol>
<p>A parent <code>Fused Map/Filter/Project</code> can combine with this
operator.</p>
<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ <code>Distinct</code> and
<code>Accumulable</code> aggregates use a moderate amount of memory
(proportional to twice the output size). <code>MIN</code> and
<code>MAX</code> aggregates can use significantly more memory. This can
be improved by including group size hints in the query, see <a
href="/docs/self-managed/v25.2/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice"><code>mz_introspection.mz_expected_group_size_advice</code></a>.
<code>Non-incremental</code> aggregates use memory proportional to the
input + output size. <code>Collated</code> aggregates use memory that is
the sum of their constituents, plus some memory for the join at the
end.</td>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>→Accumulable GroupAggregate
  Simple aggregates: count(*)
  Post-process Map/Filter/Project
    Filter: (#0 &gt; 1)</code></pre>
</div></td>
</tr>
<tr>
<td><strong>TopK</strong></td>
<td><p>Groups the input rows, sorts them according to some ordering, and
returns at most <code>K</code> rows at some offset from the top of the
list, where <code>K</code> is some (possibly computed) limit.</p>
<p>There are three types of <code>TopK</code>. Two are special cased for
monotonic inputs (i.e., inputs which never retract data).</p>
<ol>
<li><code>Monotonic Top1</code>.</li>
<li><code>Monotonic TopK</code>, which may give an expression indicating
the limit.</li>
<li><code>Non-monotonic TopK</code>, a generic <code>TopK</code>
plan.</li>
</ol>
<p>Each version of the <code>TopK</code> operator may include grouping,
ordering, and limit directives.</p>
<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ <code>Monotonic Top1</code> and
<code>Monotonic TopK</code> use a moderate amount of memory.
<code>Non-monotonic TopK</code> uses significantly more memory as the
operator can significantly overestimate the group sizes. Consult <a
href="/docs/self-managed/v25.2/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice"><code>mz_introspection.mz_expected_group_size_advice</code></a>.</td>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>→Consolidating Monotonic TopK
  Order By #1 asc nulls_last, #0 desc nulls_first
  Limit 5</code></pre>
</div></td>
</tr>
<tr>
<td><strong>Negate Diffs</strong></td>
<td>Negates the row counts of the input. This is usually used in
combination with union to remove rows from the other union input.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>→Negate Diffs</code></td>
</tr>
<tr>
<td><strong>Threshold Diffs</strong></td>
<td>Removes any rows with negative counts.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Uses memory proportional to the input
and output size, twice.</td>
<td><code>→Threshold Diffs</code></td>
</tr>
<tr>
<td><strong>Union</strong></td>
<td>Combines its inputs into a unified output, emitting one row for each
row on any input. (Corresponds to <code>UNION ALL</code> rather than
<code>UNION</code>/<code>UNION DISTINCT</code>.)<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ A <code>Consolidating Union</code> will
make moderate use of memory, particularly at hydration time. A
<code>Union</code> that is not <code>Consolidating</code> will not
consume memory.</td>
<td><code>→Consolidating Union</code></td>
</tr>
<tr>
<td><strong>Arrange</strong></td>
<td>Indicates a point that will become an <a
href="/docs/self-managed/v25.2/get-started/arrangements/#arrangements">arrangement</a>
in the dataflow engine, i.e., it will consume memory to cache
results.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Uses memory proportional to the input
size. Note that in the LIR / physical plan,
<code>Arrange</code>/<code>ArrangeBy</code> almost always means that an
arrangement will actually be created. (This is in contrast to the
“optimized” plan, where an <code>ArrangeBy</code> being present in the
plan often does not mean that an arrangement will actually be
created.)</td>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>→Arrange
    Keys: 1 arrangement available, plus raw stream
      Arrangement 0: #0</code></pre>
</div></td>
</tr>
<tr>
<td><strong>Unarranged Raw Stream</strong></td>
<td>Indicates a point where data will be streamed (even if it is somehow
already arranged).<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>→Unarranged Raw Stream</code></td>
</tr>
<tr>
<td><strong>With ... Return ...</strong></td>
<td>Introduces CTEs, i.e., makes it possible for sub-plans to be
consumed multiple times by downstream operators.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><a href="#reading-decorrelated-and-optimized-plans">See
above</a></td>
</tr>
</tbody>
</table>

<span class="caption"> </span>

- **Can increase data size:** Specifies whether the operator can
  increase the data size (can be the number of rows or the number of
  columns).
- **Uses memory:** Specifies whether the operator use memory to maintain
  state for its inputs.

</div>

<div id="tab-in-decorrelated-and-optimized-plans-default-explain"
class="tab-pane"
title="In decorrelated and optimized plans (default EXPLAIN)">

The following table lists the operators that are available in the
optimized plan.

- For those operators that require memory to maintain intermediate
  state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or
  columns), **Can increase data size** is marked with **Yes**.

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Operator</th>
<th>Description</th>
<th>Example</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Constant</strong></td>
<td>Always produces the same collection of rows.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>Constant
- ((1, 2) x 2)
- (3, 4)</code></pre>
</div></td>
</tr>
<tr>
<td><strong>Get</strong></td>
<td>Produces rows from either an existing relation
(source/view/materialized view/table) or from a previous CTE in the same
plan.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>Get materialize.public.ordered</code></td>
</tr>
<tr>
<td><strong>Project</strong></td>
<td>Produces a subset of the <a href="#explain-plan-columns">columns</a>
in the input rows. See also <a href="#explain-plan-columns">column
numbering</a>.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>Project (#2, #3)</code></td>
</tr>
<tr>
<td><strong>Map</strong></td>
<td>Appends the results of some scalar expressions to each row in the
input.<br />
<br />
<strong>Can increase data size:</strong> Each row has more data (i.e.,
longer rows but same number of rows).<br />
<strong>Uses memory:</strong> No</td>
<td><code>Map (((#1 * 10000000dec) / #2) * 1000dec)</code></td>
</tr>
<tr>
<td><strong>FlatMap</strong></td>
<td>Appends the result of some (one-to-many) <a
href="/docs/self-managed/v25.2/sql/functions/#table-functions">table
function</a> to each row in the input.<br />
<br />
<strong>Can increase data size:</strong> Depends on the <a
href="/docs/self-managed/v25.2/sql/functions/#table-functions">table
function</a> used.<br />
<strong>Uses memory:</strong> No</td>
<td><code>FlatMap jsonb_foreach(#3)</code></td>
</tr>
<tr>
<td><strong>Filter</strong></td>
<td>Removes rows of the input for which some scalar predicates return
<code>false</code>.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>Filter (#20 &lt; #21)</code></td>
</tr>
<tr>
<td><strong>Join</strong></td>
<td>Returns combinations of rows from each input whenever some equality
predicates are <code>true</code>.<br />
<br />
<strong>Can increase data size:</strong> Depends on the join order and
facts about the joined collections.<br />
<strong>Uses memory:</strong> ✅ The <code>Join</code> operator itself
uses memory only for <code>type=differential</code> with more than 2
inputs. However, <code>Join</code> operators need <a
href="/docs/self-managed/v25.2/get-started/arrangements/#arrangements">arrangements</a>
on their inputs (shown by the <code>ArrangeBy</code> operator). These
arrangements use memory proportional to the input sizes. If an input has
an <a
href="/docs/self-managed/v25.2/transform-data/optimization/#join">appropriate
index</a>, then the arrangement of the index will be reused.</td>
<td><code>Join on=(#1 = #2) type=delta</code></td>
</tr>
<tr>
<td><strong>CrossJoin</strong></td>
<td>An alias for a <code>Join</code> with an empty predicate (emits all
combinations). Note that not all cross joins are marked as
<code>CrossJoin</code>: In a join with more than 2 inputs, it can happen
that there is a cross join between some of the inputs. You can recognize
this case by <code>ArrangeBy</code> operators having empty keys, i.e.,
<code>ArrangeBy keys=[[]]</code>.<br />
<br />
<strong>Can increase data size:</strong> Cartesian product of the inputs
(|N| x |M|).<br />
<strong>Uses memory:</strong> ✅ Uses memory for 3-way or more
differential joins.</td>
<td><code>CrossJoin type=differential</code></td>
</tr>
<tr>
<td><strong>Reduce</strong></td>
<td>Groups the input rows by some scalar expressions, reduces each group
using some aggregate functions, and produces rows containing the group
key and aggregate outputs.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ <code>SUM</code>, <code>COUNT</code>,
and most other aggregations use a moderate amount of memory
(proportional either to twice the output size or to input size + output
size). <code>MIN</code> and <code>MAX</code> aggregates can use
significantly more memory. This can be improved by including group size
hints in the query, see <a
href="/docs/self-managed/v25.2/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice"><code>mz_introspection.mz_expected_group_size_advice</code></a>.</td>
<td><code>Reduce group_by=[#0] aggregates=[max((#0 * #1))]</code></td>
</tr>
<tr>
<td><strong>Distinct</strong></td>
<td>Alias for a <code>Reduce</code> with an empty aggregate list.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Uses memory proportional to twice the
output size.</td>
<td><code>Distinct</code></td>
</tr>
<tr>
<td><strong>TopK</strong></td>
<td>Groups the input rows by some scalar expressions, sorts each group
using the group key, removes the top <code>offset</code> rows in each
group, and returns the next <code>limit</code> rows.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Can use significant amount as the
operator can significantly overestimate the group sizes. Consult <a
href="/docs/self-managed/v25.2/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice"><code>mz_introspection.mz_expected_group_size_advice</code></a>.</td>
<td><code>TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5</code></td>
</tr>
<tr>
<td><strong>Negate</strong></td>
<td>Negates the row counts of the input. This is usually used in
combination with union to remove rows from the other union input.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>Negate</code></td>
</tr>
<tr>
<td><strong>Threshold</strong></td>
<td>Removes any rows with negative counts.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Uses memory proportional to the input
and output size, twice.</td>
<td><code>Threshold</code></td>
</tr>
<tr>
<td><strong>Union</strong></td>
<td>Sums the counts of each row of all inputs. (Corresponds to
<code>UNION ALL</code> rather than
<code>UNION</code>/<code>UNION DISTINCT</code>.)<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Moderate use of memory. Some union
operators force consolidation, which results in a memory spike, largely
at hydration time.</td>
<td><code>Union</code></td>
</tr>
<tr>
<td><strong>ArrangeBy</strong></td>
<td>Indicates a point that will become an <a
href="/docs/self-managed/v25.2/get-started/arrangements/#arrangements">arrangement</a>
in the dataflow engine (each <code>keys</code> element will be a
different arrangement). Note that if an appropriate index already exists
on the input or the output of the previous operator is already arranged
with a key that is also requested here, then this operator will just
pass on that existing arrangement instead of creating a new one.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Depends. If arrangements need to be
created, they use memory proportional to the input size.</td>
<td><code>ArrangeBy keys=[[#0]]</code></td>
</tr>
<tr>
<td><strong>With ... Return ...</strong></td>
<td>Introduces CTEs, i.e., makes it possible for sub-plans to be
consumed multiple times by downstream operators.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><a href="#reading-decorrelated-and-optimized-plans">See
above</a></td>
</tr>
</tbody>
</table>

<span class="caption"> </span>

- **Can increase data size:** Specifies whether the operator can
  increase the data size (can be the number of rows or the number of
  columns).
- **Uses memory:** Specifies whether the operator use memory to maintain
  state for its inputs.

</div>

<div id="tab-in-raw-plans" class="tab-pane" title="In raw plans">

The following table lists the operators that are available in the raw
plan.

- For those operators that require memory to maintain intermediate
  state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or
  columns), **Can increase data size** is marked with **Yes**.

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Operator</th>
<th>Description</th>
<th>Example</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Constant</strong></td>
<td>Always produces the same collection of rows.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>Constant
- ((1, 2) x 2)
- (3, 4)</code></pre>
</div></td>
</tr>
<tr>
<td><strong>Get</strong></td>
<td>Produces rows from either an existing relation
(source/view/materialized view/table) or from a previous CTE in the same
plan.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>Get materialize.public.ordered</code></td>
</tr>
<tr>
<td><strong>Project</strong></td>
<td>Produces a subset of the <a href="#explain-plan-columns">columns</a>
in the input rows. See also <a href="#explain-plan-columns">column
numbering</a>.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>Project (#2, #3)</code></td>
</tr>
<tr>
<td><strong>Map</strong></td>
<td>Appends the results of some scalar expressions to each row in the
input.<br />
<br />
<strong>Can increase data size:</strong> Each row has more data (i.e.,
longer rows but same number of rows).<br />
<strong>Uses memory:</strong> No</td>
<td><code>Map (((#1 * 10000000dec) / #2) * 1000dec)</code></td>
</tr>
<tr>
<td><strong>CallTable</strong></td>
<td>Appends the result of some (one-to-many) <a
href="/docs/self-managed/v25.2/sql/functions/#table-functions">table
function</a> to each row in the input.<br />
<br />
<strong>Can increase data size:</strong> Depends on the <a
href="/docs/self-managed/v25.2/sql/functions/#table-functions">table
function</a> used.<br />
<strong>Uses memory:</strong> No</td>
<td><code>CallTable generate_series(1, 7, 1)</code></td>
</tr>
<tr>
<td><strong>Filter</strong></td>
<td>Removes rows of the input for which some scalar predicates return
<code>false</code>.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>Filter (#20 &lt; #21)</code></td>
</tr>
<tr>
<td><strong>~Join</strong></td>
<td>Performs one of <code>INNER</code> / <code>LEFT</code> /
<code>RIGHT</code> / <code>FULL OUTER</code> / <code>CROSS</code> join
on the two inputs, using the given predicate.<br />
<br />
<strong>Can increase data size:</strong> For <code>CrossJoin</code>s,
Cartesian product of the inputs (|N| x |M|). Note that, in many cases, a
join that shows up as a cross join in the RAW PLAN will actually be
turned into an inner join in the OPTIMIZED PLAN, by making use of an
equality WHERE condition. For other join types, depends on the join
order and facts about the joined collections.<br />
<strong>Uses memory:</strong> ✅ Uses memory proportional to the input
sizes, unless <a
href="/docs/self-managed/v25.2/transform-data/optimization/#join">the
inputs have appropriate indexes</a>. Certain joins with more than 2
inputs use additional memory, see details in the optimized plan.</td>
<td><code>InnerJoin (#0 = #2)</code></td>
</tr>
<tr>
<td><strong>Reduce</strong></td>
<td>Groups the input rows by some scalar expressions, reduces each group
using some aggregate functions, and produces rows containing the group
key and aggregate outputs. In the case where the group key is empty and
the input is empty, returns a single row with the aggregate functions
applied to the empty input collection.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ <code>SUM</code>, <code>COUNT</code>,
and most other aggregations use a moderate amount of memory
(proportional either to twice the output size or to input size + output
size). <code>MIN</code> and <code>MAX</code> aggregates can use
significantly more memory. This can be improved by including group size
hints in the query, see <a
href="/docs/self-managed/v25.2/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice"><code>mz_introspection.mz_expected_group_size_advice</code></a>.</td>
<td><code>Reduce group_by=[#0] aggregates=[max((#0 * #1))]</code></td>
</tr>
<tr>
<td><strong>Distinct</strong></td>
<td>Removes duplicate copies of input rows.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Uses memory proportional to twice the
output size.</td>
<td><code>Distinct</code></td>
</tr>
<tr>
<td><strong>TopK</strong></td>
<td>Groups the input rows by some scalar expressions, sorts each group
using the group key, removes the top <code>offset</code> rows in each
group, and returns the next <code>limit</code> rows.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Can use significant amount as the
operator can significantly overestimate the group sizes. Consult <a
href="/docs/self-managed/v25.2/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice"><code>mz_introspection.mz_expected_group_size_advice</code></a>.</td>
<td><code>TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5</code></td>
</tr>
<tr>
<td><strong>Negate</strong></td>
<td>Negates the row counts of the input. This is usually used in
combination with union to remove rows from the other union input.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><code>Negate</code></td>
</tr>
<tr>
<td><strong>Threshold</strong></td>
<td>Removes any rows with negative counts.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Uses memory proportional to the input
and output size, twice.</td>
<td><code>Threshold</code></td>
</tr>
<tr>
<td><strong>Union</strong></td>
<td>Sums the counts of each row of all inputs. (Corresponds to
<code>UNION ALL</code> rather than
<code>UNION</code>/<code>UNION DISTINCT</code>.)<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> ✅ Moderate use of memory. Some union
operators force consolidation, which results in a memory spike, largely
at hydration time.</td>
<td><code>Union</code></td>
</tr>
<tr>
<td><strong>With ... Return ...</strong></td>
<td>Introduces CTEs, i.e., makes it possible for sub-plans to be
consumed multiple times by downstream operators.<br />
<br />
<strong>Can increase data size:</strong> No<br />
<strong>Uses memory:</strong> No</td>
<td><a href="#reading-decorrelated-and-optimized-plans">See
above</a></td>
</tr>
</tbody>
</table>

<span class="caption"> </span>

- **Can increase data size:** Specifies whether the operator can
  increase the data size (can be the number of rows or the number of
  columns).
- **Uses memory:** Specifies whether the operator use memory to maintain
  state for its inputs.

</div>

</div>

</div>

Operators are sometimes marked as `Fused ...`. We write this to mean
that the operator is fused with its input. That is, if you see a
`Fused X` operator above a `Y` operator:

```
→Fused X
  →Y
```

Then the `X` and `Y` operators will be combined into a single, more
efficient operator.

## Examples

For the following examples, let’s assume that you have [the auction
house load
generator](/docs/self-managed/v25.2/sql/create-source/load-generator/#creating-an-auction-load-generator)
created in your current environment.

### Explaining a `SELECT` query

Let’s start with a simple join query that lists the total amounts bid
per buyer.

Explain the optimized plan as text:

<div class="highlight">

``` chroma
EXPLAIN
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

</div>

Same as above, but a bit more verbose:

<div class="highlight">

``` chroma
EXPLAIN PLAN
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

</div>

Same as above, but even more verbose:

<div class="highlight">

``` chroma
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

</div>

Same as above, but every sub-plan is annotated with its schema types:

<div class="highlight">

``` chroma
EXPLAIN WITH(types) FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

</div>

Explain the physical plan as text:

<div class="highlight">

``` chroma
EXPLAIN PHYSICAL PLAN FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

</div>

### Explaining an index on a view

Let’s create a view with an index for the above query.

<div class="highlight">

``` chroma
-- create the view
CREATE VIEW my_view AS
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
-- create an index on the view
CREATE INDEX my_view_idx ON my_view(id);
```

</div>

You can inspect the plan of the dataflow that will maintain your index
with the following statements.

Explain the optimized plan as text:

<div class="highlight">

``` chroma
EXPLAIN
INDEX my_view_idx;
```

</div>

Same as above, but a bit more verbose:

<div class="highlight">

``` chroma
EXPLAIN PLAN FOR
INDEX my_view_idx;
```

</div>

Same as above, but even more verbose:

<div class="highlight">

``` chroma
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
INDEX my_view_idx;
```

</div>

Same as above, but every sub-plan is annotated with its schema types:

<div class="highlight">

``` chroma
EXPLAIN WITH(types) FOR
INDEX my_view_idx;
```

</div>

Explain the physical plan as text:

<div class="highlight">

``` chroma
EXPLAIN PHYSICAL PLAN FOR
INDEX my_view_idx;
```

</div>

### Explaining a materialized view

Let’s create a materialized view for the above `SELECT` query.

<div class="highlight">

``` chroma
CREATE MATERIALIZED VIEW my_mat_view AS
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

</div>

You can inspect the plan of the dataflow that will maintain your view
with the following statements.

Explain the optimized plan as text:

<div class="highlight">

``` chroma
EXPLAIN
MATERIALIZED VIEW my_mat_view;
```

</div>

Same as above, but a bit more verbose:

<div class="highlight">

``` chroma
EXPLAIN PLAN FOR
MATERIALIZED VIEW my_mat_view;
```

</div>

Same as above, but even more verbose:

<div class="highlight">

``` chroma
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
MATERIALIZED VIEW my_mat_view;
```

</div>

Same as above, but every sub-plan is annotated with its schema types:

<div class="highlight">

``` chroma
EXPLAIN WITH(types)
MATERIALIZED VIEW my_mat_view;
```

</div>

Explain the physical plan as text:

<div class="highlight">

``` chroma
EXPLAIN PHYSICAL PLAN FOR
MATERIALIZED VIEW my_mat_view;
```

</div>

## Debugging running dataflows

The [`EXPLAIN ANALYZE`](/docs/self-managed/v25.2/sql/explain-analyze/)
statement will let you debug memory and cpu usage (optionally with
information about worker skew) for existing indexes and materialized
views in terms of their physical plan operators. It can also attribute
[TopK
hints](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1)
to individual operators.

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the explainee
  are contained in.

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/explain-plan.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
