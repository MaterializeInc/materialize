# EXPLAIN PLAN
Reference page for `EXPLAIN PLAN`. `EXPLAIN PLAN` is used to inspect the plans of `SELECT` statements, indexes, and materialized views.
`EXPLAIN PLAN` displays the plans used for:

|                             |                       |
|-----------------------------|-----------------------|
| <ul><li>`SELECT` statements </li><li>`CREATE VIEW` statements</li><li>`CREATE INDEX` statements</li><li>`CREATE MATERIALIZED VIEW` statements</li></ul>|<ul><li>Existing views</li><li>Existing indexes</li><li>Existing materialized views</li></ul> |

> **Warning:** `EXPLAIN` is not part of Materialize's stable interface and is not subject to
> our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
> change arbitrarily in future versions of Materialize.


## Syntax


**FOR SELECT:**
```mzsql
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]       -- The FOR keyword is required if the PLAN keyword is specified
    <SELECT ...>
;
```

**FOR CREATE VIEW:**

```mzsql
EXPLAIN <RAW | DECORRELATED | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR
    <CREATE VIEW ...>
;
```

**FOR CREATE INDEX:**
```mzsql
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE INDEX ...>
;
```

**FOR CREATE MATERIALIZED VIEW:**
```mzsql
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]          -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE MATERIALIZED VIEW ...>
;
```

**FOR VIEW:**
```mzsql
EXPLAIN <RAW | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR
  VIEW <name>
;
```

**FOR INDEX:**
```mzsql
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
      [ WITH (<output_modifier> [, <output_modifier> ...]) ]
      [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
  INDEX <name>
;
```

**FOR MATERIALIZED VIEW:**
```mzsql
EXPLAIN [[ RAW | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ] -- The FOR keyword is required if the PLAN keyword is specified
  MATERIALIZED VIEW <name>
;
```



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
**OPTIMIZED PLAN** | _(Default)_ Display the optimized plan.
**PHYSICAL PLAN** |  Display the physical plan; this corresponds to the operators shown in [`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping).

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

Each operator can also be annotated with additional metadata. Some details are shown in the `EXPLAIN` output (`EXPLAIN PHYSICAL PLAN AS TEXT`), but are hidden elsewhere. <a
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



**In fully optimized physical (LIR) plans (Default):**
The following table lists the operators that are available in the LIR plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.| Operator | Description | Example |
| --- | --- | --- |
| **Constant** | Always produces the same collection of rows.  **Can increase data size:** No **Uses memory:** No | <code>→Constant (2 rows)</code> |
| **Stream, Arranged, Index Lookup, Read** | <p>Produces rows from either an existing relation (source/view/materialized view/table) or from a previous CTE in the same plan. A parent <code>Fused Map/Filter/Project</code> operator can combine with this operator.</p> <p>There are four types of <code>Get</code>.</p> <ol> <li> <p><code>Stream</code> indicates that the results are not <a href="/get-started/arrangements/#arrangements" >arranged</a> in memory and will be streamed directly.</p> </li> <li> <p><code>Arranged</code> indicates that the results are <a href="/get-started/arrangements/#arrangements" >arranged</a> in memory.</p> </li> <li> <p><code>Index Lookup</code> indicates the results will be <em>looked up</em> in an existing [arrangement]((/get-started/arrangements/#arrangements).</p> </li> <li> <p><code>Read</code> indicates that the results are unarranged, and will be processed as they arrive.</p> </li> </ol>   **Can increase data size:** No **Uses memory:** No | <code>Arranged materialize.public.t</code> |
| **Map/Filter/Project** | <p>Computes new columns (maps), filters columns, and projects away columns. Works row-by-row. Maps and filters will be printed, but projects will not.</p> <p>These may be marked as <strong><code>Fused</code></strong> <code>Map/Filter/Project</code>, which means they will combine with the operator beneath them to run more efficiently.</p>   **Can increase data size:** Each row may have more data, from the <code>Map</code>. Each row may also have less data, from the <code>Project</code>. There may be fewer rows, from the <code>Filter</code>. **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="k">Map</span><span class="o">/</span><span class="k">Filter</span><span class="o">/</span><span class="n">Project</span> </span></span><span class="line"><span class="cl">  <span class="k">Filter</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">}</span> <span class="o">&lt;</span> <span class="mf">7</span><span class="p">)</span> </span></span><span class="line"><span class="cl">  <span class="k">Map</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">}</span> <span class="o">+</span> <span class="o">#</span><span class="mf">1</span><span class="p">{</span><span class="n">b</span><span class="p">})</span> </span></span></code></pre></div> |
| **Table Function** | <p>Appends the result of some (one-to-many) <a href="/sql/functions/#table-functions" >table function</a> to each row in the input.</p> <p>A parent <code>Fused Table Function unnest_list</code> operator will fuse with its child <code>GroupAggregate</code> operator. Fusing these operator is part of how we efficiently compile window functions from SQL to dataflows.</p> <p>A parent <code>Fused Map/Filter/Project</code> can combine with this operator.</p>   **Can increase data size:** Depends on the <a href="/sql/functions/#table-functions" >table function</a> used. **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="k">Table</span> <span class="k">Function</span> <span class="n">generate_series</span><span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">},</span> <span class="o">#</span><span class="mf">1</span><span class="p">{</span><span class="n">b</span><span class="p">},</span> <span class="mf">1</span><span class="p">)</span> </span></span><span class="line"><span class="cl">  <span class="k">Input</span> <span class="k">key</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">})</span> </span></span></code></pre></div> |
| **Differential Join, Delta Join** | <p>Both join operators indicate the join ordering selected.</p> <p>Returns combinations of rows from each input whenever some equality predicates are <code>true</code>.</p> <p>Joins will indicate the join order of their children, starting from 0. For example, <code>Differential Join %1 » %0</code> will join its second child into its first.</p> <p>The <a href="/transform-data/optimization/#join" >two joins differ in performance characteristics</a>.</p>   **Can increase data size:** Depends on the join order and facts about the joined collections. **Uses memory:** ✅ Uses memory for 3-way or more differential joins. | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Differential</span> <span class="k">Join</span> <span class="o">%</span><span class="mf">1</span> <span class="err">»</span> <span class="o">%</span><span class="mf">0</span> </span></span><span class="line"><span class="cl">  <span class="k">Join</span> <span class="n">stage</span> <span class="o">%</span><span class="mf">0</span><span class="p">:</span> <span class="n">Lookup</span> <span class="k">key</span> <span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">}</span> <span class="k">in</span> <span class="o">%</span><span class="mf">0</span> </span></span></code></pre></div> |
| **GroupAggregate** | <p>Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs.</p> <p>There are five types of <code>GroupAggregate</code>, ordered by increasing complexity:</p> <ol> <li> <p><code>Distinct GroupAggregate</code> corresponds to the SQL <code>DISTINCT</code> operator.</p> </li> <li> <p><code>Accumulable GroupAggregate</code> (e.g., <code>SUM</code>, <code>COUNT</code>) corresponds to several easy to implement aggregations that can be executed simultaneously and efficiently.</p> </li> <li> <p><code>Hierarchical GroupAggregate</code> (e.g., <code>MIN</code>, <code>MAX</code>) corresponds to an aggregation requiring a tower of arrangements. These can be either monotonic (more efficient) or bucketed. These may benefit from a hint; <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" >see <code>mz_introspection.mz_expected_group_size_advice</code></a>. These may either be bucketed or monotonic (more efficient). These may consolidate their output, which will increase memory usage.</p> </li> <li> <p><code>Collated Multi-GroupAggregate</code> corresponds to an arbitrary mix of reductions of different types, which will be performed separately and then joined together.</p> </li> <li> <p><code>Non-incremental GroupAggregate</code> (e.g., window functions, <code>list_agg</code>) corresponds to a single non-incremental aggregation. These are the most computationally intensive reductions.</p> </li> </ol> <p>A parent <code>Fused Map/Filter/Project</code> can combine with this operator.</p>   **Can increase data size:** No **Uses memory:** ✅ <code>Distinct</code> and <code>Accumulable</code> aggregates use a moderate amount of memory (proportional to twice the output size). <code>MIN</code> and <code>MAX</code> aggregates can use significantly more memory. This can be improved by including group size hints in the query, see <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. <code>Non-incremental</code> aggregates use memory proportional to the input + output size. <code>Collated</code> aggregates use memory that is the sum of their constituents, plus some memory for the join at the end. | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Accumulable</span> <span class="n">GroupAggregate</span> </span></span><span class="line"><span class="cl">  <span class="n">Simple</span> <span class="n">aggregates</span><span class="p">:</span> <span class="k">count</span><span class="p">(</span><span class="o">*</span><span class="p">)</span> </span></span><span class="line"><span class="cl">  <span class="n">Post</span><span class="o">-</span><span class="n">process</span> <span class="k">Map</span><span class="o">/</span><span class="k">Filter</span><span class="o">/</span><span class="n">Project</span> </span></span><span class="line"><span class="cl">    <span class="k">Filter</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span> <span class="o">&gt;</span> <span class="mf">1</span><span class="p">)</span> </span></span></code></pre></div> |
| **TopK** | <p>Groups the input rows, sorts them according to some ordering, and returns at most <code>K</code> rows at some offset from the top of the list, where <code>K</code> is some (possibly computed) limit.</p> <p>There are three types of <code>TopK</code>. Two are special cased for monotonic inputs (i.e., inputs which never retract data).</p> <ol> <li><code>Monotonic Top1</code>.</li> <li><code>Monotonic TopK</code>, which may give an expression indicating the limit.</li> <li><code>Non-monotonic TopK</code>, a generic <code>TopK</code> plan.</li> </ol> <p>Each version of the <code>TopK</code> operator may include grouping, ordering, and limit directives.</p>   **Can increase data size:** No **Uses memory:** ✅ <code>Monotonic Top1</code> and <code>Monotonic TopK</code> use a moderate amount of memory. <code>Non-monotonic TopK</code> uses significantly more memory as the operator can significantly overestimate the group sizes. Consult <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Consolidating</span> <span class="n">Monotonic</span> <span class="n">TopK</span> </span></span><span class="line"><span class="cl">  <span class="k">Order</span> <span class="k">By</span> <span class="o">#</span><span class="mf">1</span> <span class="k">asc</span> <span class="n">nulls_last</span><span class="p">,</span> <span class="o">#</span><span class="mf">0</span> <span class="k">desc</span> <span class="n">nulls_first</span> </span></span><span class="line"><span class="cl">  <span class="k">Limit</span> <span class="mf">5</span> </span></span></code></pre></div> |
| **Negate Diffs** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.  **Can increase data size:** No **Uses memory:** No | <code>→Negate Diffs</code> |
| **Threshold Diffs** | Removes any rows with negative counts.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input and output size, twice. | <code>→Threshold Diffs</code> |
| **Union** | Combines its inputs into a unified output, emitting one row for each row on any input. (Corresponds to <code>UNION ALL</code> rather than <code>UNION</code>/<code>UNION DISTINCT</code>.)  **Can increase data size:** No **Uses memory:** ✅ A <code>Consolidating Union</code> will make moderate use of memory, particularly at hydration time. A <code>Union</code> that is not <code>Consolidating</code> will not consume memory. | <code>→Consolidating Union</code> |
| **Arrange** | Indicates a point that will become an <a href="/get-started/arrangements/#arrangements" >arrangement</a> in the dataflow engine, i.e., it will consume memory to cache results.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input size. Note that in the LIR / physical plan, <code>Arrange</code>/<code>ArrangeBy</code> almost always means that an arrangement will actually be created. (This is in contrast to the &ldquo;optimized&rdquo; plan, where an <code>ArrangeBy</code> being present in the plan often does not mean that an arrangement will actually be created.) | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Arrange</span> </span></span><span class="line"><span class="cl">    <span class="k">Keys</span><span class="p">:</span> <span class="mf">1</span> <span class="k">arrangement</span> <span class="n">available</span><span class="p">,</span> <span class="n">plus</span> <span class="k">raw</span> <span class="n">stream</span> </span></span><span class="line"><span class="cl">      <span class="k">Arrangement</span> <span class="mf">0</span><span class="p">:</span> <span class="o">#</span><span class="mf">0</span> </span></span></code></pre></div> |
| **Unarranged Raw Stream** | Indicates a point where data will be streamed (even if it is somehow already arranged).  **Can increase data size:** No **Uses memory:** No | <code>→Unarranged Raw Stream</code> |
| **With ... Return ...** | Introduces CTEs, i.e., makes it possible for sub-plans to be consumed multiple times by downstream operators.  **Can increase data size:** No **Uses memory:** No | <a href="/sql/explain-plan/#reading-plans" >See Reading plans</a> |
**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.


**In decorrelated and optimized plans:**
The following table lists the operators that are available in the optimized plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.| Operator | Description | Example |
| --- | --- | --- |
| **Constant** | Always produces the same collection of rows.  **Can increase data size:** No **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="n">Constant</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">((</span><span class="mf">1</span><span class="p">,</span> <span class="mf">2</span><span class="p">)</span> <span class="n">x</span> <span class="mf">2</span><span class="p">)</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">(</span><span class="mf">3</span><span class="p">,</span> <span class="mf">4</span><span class="p">)</span> </span></span></code></pre></div> |
| **Get** | Produces rows from either an existing relation (source/view/materialized view/table) or from a previous CTE in the same plan.  **Can increase data size:** No **Uses memory:** No | <code>Get materialize.public.ordered</code> |
| **Project** | Produces a subset of the <a href="/sql/explain-plan/#explain-plan-columns" >columns</a> in the input rows. See also <a href="/sql/explain-plan/#explain-plan-columns" >column numbering</a>.  **Can increase data size:** No **Uses memory:** No | <code>Project (#2, #3)</code> |
| **Map** | Appends the results of some scalar expressions to each row in the input.  **Can increase data size:** Each row has more data (i.e., longer rows but same number of rows). **Uses memory:** No | <code>Map (((#1 * 10000000dec) / #2) * 1000dec)</code> |
| **FlatMap** | Appends the result of some (one-to-many) <a href="/sql/functions/#table-functions" >table function</a> to each row in the input.  **Can increase data size:** Depends on the <a href="/sql/functions/#table-functions" >table function</a> used. **Uses memory:** No | <code>FlatMap jsonb_foreach(#3)</code> |
| **Filter** | Removes rows of the input for which some scalar predicates return <code>false</code>.  **Can increase data size:** No **Uses memory:** No | <code>Filter (#20 &lt; #21)</code> |
| **Join** | Returns combinations of rows from each input whenever some equality predicates are <code>true</code>.  **Can increase data size:** Depends on the join order and facts about the joined collections. **Uses memory:** ✅ The <code>Join</code> operator itself uses memory only for <code>type=differential</code> with more than 2 inputs. However, <code>Join</code> operators need <a href="/get-started/arrangements/#arrangements" >arrangements</a> on their inputs (shown by the <code>ArrangeBy</code> operator). These arrangements use memory proportional to the input sizes. If an input has an <a href="/transform-data/optimization/#join" >appropriate index</a>, then the arrangement of the index will be reused. | <code>Join on=(#1 = #2) type=delta</code> |
| **CrossJoin** | An alias for a <code>Join</code> with an empty predicate (emits all combinations). Note that not all cross joins are marked as <code>CrossJoin</code>: In a join with more than 2 inputs, it can happen that there is a cross join between some of the inputs. You can recognize this case by <code>ArrangeBy</code> operators having empty keys, i.e., <code>ArrangeBy keys=[[]]</code>.  **Can increase data size:** Cartesian product of the inputs (\|N\| x \|M\|). **Uses memory:** ✅ Uses memory for 3-way or more differential joins. | <code>CrossJoin type=differential</code> |
| **Reduce** | Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs.  **Can increase data size:** No **Uses memory:** ✅ <code>SUM</code>, <code>COUNT</code>, and most other aggregations use a moderate amount of memory (proportional either to twice the output size or to input size + output size). <code>MIN</code> and <code>MAX</code> aggregates can use significantly more memory. This can be improved by including group size hints in the query, see <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>Reduce group_by=[#0] aggregates=[max((#0 * #1))]</code> |
| **Distinct** | Alias for a <code>Reduce</code> with an empty aggregate list.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to twice the output size. | <code>Distinct</code> |
| **TopK** | Groups the input rows by some scalar expressions, sorts each group using the group key, removes the top <code>offset</code> rows in each group, and returns the next <code>limit</code> rows.  **Can increase data size:** No **Uses memory:** ✅ Can use significant amount as the operator can significantly overestimate the group sizes. Consult <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5</code> |
| **Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.  **Can increase data size:** No **Uses memory:** No | <code>Negate</code> |
| **Threshold** | Removes any rows with negative counts.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input and output size, twice. | <code>Threshold</code> |
| **Union** | Sums the counts of each row of all inputs. (Corresponds to <code>UNION ALL</code> rather than <code>UNION</code>/<code>UNION DISTINCT</code>.)  **Can increase data size:** No **Uses memory:** ✅ Moderate use of memory. Some union operators force consolidation, which results in a memory spike, largely at hydration time. | <code>Union</code> |
| **ArrangeBy** | Indicates a point that will become an <a href="/get-started/arrangements/#arrangements" >arrangement</a> in the dataflow engine (each <code>keys</code> element will be a different arrangement). Note that if an appropriate index already exists on the input or the output of the previous operator is already arranged with a key that is also requested here, then this operator will just pass on that existing arrangement instead of creating a new one.  **Can increase data size:** No **Uses memory:** ✅ Depends. If arrangements need to be created, they use memory proportional to the input size. | <code>ArrangeBy keys=[[#0]]</code> |
| **With ... Return ...** | Introduces CTEs, i.e., makes it possible for sub-plans to be consumed multiple times by downstream operators.  **Can increase data size:** No **Uses memory:** No | <a href="/sql/explain-plan/#reading-plans" >See Reading plans</a> |
**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.


**In raw plans:**
The following table lists the operators that are available in the raw plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.| Operator | Description | Example |
| --- | --- | --- |
| **Constant** | Always produces the same collection of rows.  **Can increase data size:** No **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="n">Constant</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">((</span><span class="mf">1</span><span class="p">,</span> <span class="mf">2</span><span class="p">)</span> <span class="n">x</span> <span class="mf">2</span><span class="p">)</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">(</span><span class="mf">3</span><span class="p">,</span> <span class="mf">4</span><span class="p">)</span> </span></span></code></pre></div> |
| **Get** | Produces rows from either an existing relation (source/view/materialized view/table) or from a previous CTE in the same plan.  **Can increase data size:** No **Uses memory:** No | <code>Get materialize.public.ordered</code> |
| **Project** | Produces a subset of the <a href="/sql/explain-plan/#explain-plan-columns" >columns</a> in the input rows. See also <a href="/sql/explain-plan/#explain-plan-columns" >column numbering</a>.  **Can increase data size:** No **Uses memory:** No | <code>Project (#2, #3)</code> |
| **Map** | Appends the results of some scalar expressions to each row in the input.  **Can increase data size:** Each row has more data (i.e., longer rows but same number of rows). **Uses memory:** No | <code>Map (((#1 * 10000000dec) / #2) * 1000dec)</code> |
| **CallTable** | Appends the result of some (one-to-many) <a href="/sql/functions/#table-functions" >table function</a> to each row in the input.  **Can increase data size:** Depends on the <a href="/sql/functions/#table-functions" >table function</a> used. **Uses memory:** No | <code>CallTable generate_series(1, 7, 1)</code> |
| **Filter** | Removes rows of the input for which some scalar predicates return <code>false</code>.  **Can increase data size:** No **Uses memory:** No | <code>Filter (#20 &lt; #21)</code> |
| **~Join** | Performs one of <code>INNER</code> / <code>LEFT</code> / <code>RIGHT</code> / <code>FULL OUTER</code> / <code>CROSS</code> join on the two inputs, using the given predicate.  **Can increase data size:** For <code>CrossJoin</code>s, Cartesian product of the inputs (\|N\| x \|M\|). Note that, in many cases, a join that shows up as a cross join in the RAW PLAN will actually be turned into an inner join in the OPTIMIZED PLAN, by making use of an equality WHERE condition. For other join types, depends on the join order and facts about the joined collections. **Uses memory:** ✅ Uses memory proportional to the input sizes, unless <a href="/transform-data/optimization/#join" >the inputs have appropriate indexes</a>. Certain joins with more than 2 inputs use additional memory, see details in the optimized plan. | <code>InnerJoin (#0 = #2)</code> |
| **Reduce** | Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs.  In the case where the group key is empty and the input is empty, returns a single row with the aggregate functions applied to the empty input collection.  **Can increase data size:** No **Uses memory:** ✅ <code>SUM</code>, <code>COUNT</code>, and most other aggregations use a moderate amount of memory (proportional either to twice the output size or to input size + output size). <code>MIN</code> and <code>MAX</code> aggregates can use significantly more memory. This can be improved by including group size hints in the query, see <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>Reduce group_by=[#0] aggregates=[max((#0 * #1))]</code> |
| **Distinct** | Removes duplicate copies of input rows.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to twice the output size. | <code>Distinct</code> |
| **TopK** | Groups the input rows by some scalar expressions, sorts each group using the group key, removes the top <code>offset</code> rows in each group, and returns the next <code>limit</code> rows.  **Can increase data size:** No **Uses memory:** ✅ Can use significant amount as the operator can significantly overestimate the group sizes. Consult <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5</code> |
| **Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.  **Can increase data size:** No **Uses memory:** No | <code>Negate</code> |
| **Threshold** | Removes any rows with negative counts.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input and output size, twice. | <code>Threshold</code> |
| **Union** | Sums the counts of each row of all inputs. (Corresponds to <code>UNION ALL</code> rather than <code>UNION</code>/<code>UNION DISTINCT</code>.)  **Can increase data size:** No **Uses memory:** ✅ Moderate use of memory. Some union operators force consolidation, which results in a memory spike, largely at hydration time. | <code>Union</code> |
| **With ... Return ...** | Introduces CTEs, i.e., makes it possible for sub-plans to be consumed multiple times by downstream operators.  **Can increase data size:** No **Uses memory:** No | <a href="/sql/explain-plan/#reading-plans" >See Reading plans</a> |
**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.




Operators are sometimes marked as `Fused ...`. This indicates that the operator is fused with its input, i.e., the operator below it. That is, if you see a `Fused X` operator above a `Y` operator:

```
→Fused X
  →Y
```

Then the `X` and `Y` operators will be combined into a single, more efficient operator.

## Examples

For the following examples, let's assume that you have [the auction house load generator](/sql/create-source/load-generator/#creating-an-auction-load-generator) created in your current environment.

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

- `USAGE` privileges on the schemas that all relations in the explainee are
  contained in.
