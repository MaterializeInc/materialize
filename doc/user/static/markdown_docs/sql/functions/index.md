<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# SQL functions & operators

This page details Materialize’s supported SQL [functions](#functions)
and [operators](#operators).

## Functions

### Unmaterializable functions

Several functions in Materialize are **unmaterializable** because their
output depends upon state besides their input parameters, like the value
of a session parameter or the timestamp of the current transaction. You
cannot create an [index](/docs/self-managed/v25.2/sql/create-index) or
materialized view that depends on an unmaterializable function, but you
can use them in non-materialized views and one-off
[`SELECT`](/docs/self-managed/v25.2/sql/select) statements.

Unmaterializable functions are marked as such in the table below.

### Side-effecting functions

Several functions in Materialize are **side-effecting** because their
evaluation changes system state. For example, the `pg_cancel_backend`
function allows canceling a query running on another connection.

Materialize offers only limited support for these functions. They may be
called only at the top level of a `SELECT` statement, like so:

<div class="highlight">

``` chroma
SELECT side_effecting_function(arg, ...);
```

</div>

You cannot manipulate or alias the function call expression, call
multiple side-effecting functions in the same `SELECT` statement, nor
add any additional clauses to the `SELECT` statement (e.g., `FROM`,
`WHERE`).

Side-effecting functions are marked as such in the table below.

### Generic functions

Generic functions can typically take arguments of any type.

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="cast" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>CAST (cast_expr) -&gt; T</code></pre>
</div>
</div>
<p>Value as type <code>T</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/cast">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="coalesce" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>coalesce(x: T...) -&gt; T?</code></pre>
</div>
</div>
<p>First non-<em>NULL</em> arg, or <em>NULL</em> if all are
<em>NULL</em>.</p></td>
</tr>
<tr>
<td><div id="greatest" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>greatest(x: T...) -&gt; T?</code></pre>
</div>
</div>
<p>The maximum argument, or <em>NULL</em> if all are
<em>NULL</em>.</p></td>
</tr>
<tr>
<td><div id="least" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>least(x: T...) -&gt; T?</code></pre>
</div>
</div>
<p>The minimum argument, or <em>NULL</em> if all are
<em>NULL</em>.</p></td>
</tr>
<tr>
<td><div id="nullif" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>nullif(x: T, y: T) -&gt; T?</code></pre>
</div>
</div>
<p><em>NULL</em> if <code>x == y</code>, else <code>x</code>.</p></td>
</tr>
</tbody>
</table>

### Aggregate functions

Aggregate functions take one or more of the same element type as
arguments.

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="array_agg" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>array_agg(x: T) -&gt; T[]</code></pre>
</div>
</div>
<p>Aggregate values (including nulls) as an array</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/array_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="avg" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>avg(x: T) -&gt; U</code></pre>
</div>
</div>
<p>Average of <code>T</code>’s values.</p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>,
<code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="bool_and" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>bool_and(x: T) -&gt; T</code></pre>
</div>
</div>
<p><em>NULL</em> if all values of <code>x</code> are <em>NULL</em>,
otherwise true if all values of <code>x</code> are true, otherwise
false.</p></td>
</tr>
<tr>
<td><div id="bool_or" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>bool_or(x: T) -&gt; T</code></pre>
</div>
</div>
<p><em>NULL</em> if all values of <code>x</code> are <em>NULL</em>,
otherwise true if any values of <code>x</code> are true, otherwise
false.</p></td>
</tr>
<tr>
<td><div id="count" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>count(x: T) -&gt; bigint</code></pre>
</div>
</div>
<p>Number of non-<em>NULL</em> inputs.</p></td>
</tr>
<tr>
<td><div id="jsonb_agg" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_agg(expression) -&gt; jsonb</code></pre>
</div>
</div>
<p>Aggregate values (including nulls) as a jsonb array</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/jsonb_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_object_agg" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_object_agg(keys, values) -&gt; jsonb</code></pre>
</div>
</div>
<p>Aggregate keys and values (including nulls) as a jsonb object</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/jsonb_object_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="max" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>max(x: T) -&gt; T</code></pre>
</div>
</div>
<p>Maximum value among <code>T</code>.</p></td>
</tr>
<tr>
<td><div id="min" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>min(x: T) -&gt; T</code></pre>
</div>
</div>
<p>Minimum value among <code>T</code>.</p></td>
</tr>
<tr>
<td><div id="stddev" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>stddev(x: T) -&gt; U</code></pre>
</div>
</div>
<p>Historical alias for <code>stddev_samp</code>.
<em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>,
<code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="stddev_pop" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>stddev_pop(x: T) -&gt; U</code></pre>
</div>
</div>
<p>Population standard deviation of <code>T</code>’s values.
<em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>,
<code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="stddev_samp" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>stddev_samp(x: T) -&gt; U</code></pre>
</div>
</div>
<p>Sample standard deviation of <code>T</code>’s values.
<em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>,
<code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="string_agg" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>string_agg(value: text, delimiter: text) -&gt; text</code></pre>
</div>
</div>
<p>Concatenates the non-null input values into text. Each value after
the first is preceded by the corresponding delimiter</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/string_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="sum" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>sum(x: T) -&gt; U</code></pre>
</div>
</div>
<p>Sum of <code>T</code>’s values</p>
<p>Returns <code>bigint</code> if <code>x</code> is <code>int</code> or
<code>smallint</code>, <code>numeric</code> if <code>x</code> is
<code>bigint</code> or <code>uint8</code>, <code>uint8</code> if
<code>x</code> is <code>uint4</code> or <code>uint2</code>, else returns
same type as <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="variance" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>variance(x: T) -&gt; U</code></pre>
</div>
</div>
<p>Historical alias for <code>var_samp</code>. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>,
<code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="var_pop" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>var_pop(x: T) -&gt; U</code></pre>
</div>
</div>
<p>Population variance of <code>T</code>’s values.
<em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>,
<code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="var_samp" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>var_samp(x: T) -&gt; U</code></pre>
</div>
</div>
<p>Sample variance of <code>T</code>’s values. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>,
<code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p></td>
</tr>
</tbody>
</table>

### List functions

List functions take [`list`](../types/list) arguments, and are
[polymorphic](../types/list/#polymorphism).

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="list_agg" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_agg(x: any) -&gt; L</code></pre>
</div>
</div>
<p>Aggregate values (including nulls) as a list</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/list_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="list_append" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_append(l: listany, e: listelementany) -&gt; L</code></pre>
</div>
</div>
<p>Appends <code>e</code> to <code>l</code>.</p></td>
</tr>
<tr>
<td><div id="list_cat" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_cat(l1: listany, l2: listany) -&gt; L</code></pre>
</div>
</div>
<p>Concatenates <code>l1</code> and <code>l2</code>.</p></td>
</tr>
<tr>
<td><div id="list_length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_length(l: listany) -&gt; int</code></pre>
</div>
</div>
<p>Return the number of elements in <code>l</code>.</p></td>
</tr>
<tr>
<td><div id="list_prepend" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>list_prepend(e: listelementany, l: listany) -&gt; listany</code></pre>
</div>
</div>
<p>Prepends <code>e</code> to <code>l</code>.</p></td>
</tr>
</tbody>
</table>

### Map functions

Map functions take [`map`](../types/map) arguments, and are
[polymorphic](../types/#polymorphism).

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="map_length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>map_length(m: mapany) -&gt; int</code></pre>
</div>
</div>
<p>Return the number of elements in <code>m</code>.</p></td>
</tr>
<tr>
<td><div id="map_build" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>map_build(kvs: list record(text, T)) -&gt; map[text=&gt;T]</code></pre>
</div>
</div>
<p>Builds a map from a list of records whose fields are two elements,
the first of which is <code>text</code>. In the face of duplicate keys,
<code>map_build</code> retains value from the record in the latest
positition. This function is purpose-built to process <a
href="/docs/self-managed/v25.2/sql/create-source/kafka/#headers">Kafka
headers</a>.</p></td>
</tr>
<tr>
<td><div id="map_agg" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>map_agg(keys: text, values: T) -&gt; map[text=&gt;T]</code></pre>
</div>
</div>
<p>Aggregate keys and values (including nulls) as a map</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/map_agg">(docs)</a>.</p></td>
</tr>
</tbody>
</table>

### Numbers functions

Number functions take number-like arguments, e.g. [`int`](../types/int),
[`float`](../types/float), [`numeric`](../types/numeric), unless
otherwise specified.

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="abs" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>abs(x: N) -&gt; N</code></pre>
</div>
</div>
<p>The absolute value of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="cbrt" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>cbrt(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The cube root of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="ceil" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>ceil(x: N) -&gt; N</code></pre>
</div>
</div>
<p>The smallest integer &gt;= <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="ceiling" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>ceiling(x: N) -&gt; N</code></pre>
</div>
</div>
<p>Alias of <code>ceil</code>.</p></td>
</tr>
<tr>
<td><div id="exp" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>exp(x: N) -&gt; N</code></pre>
</div>
</div>
<p>Exponential of <code>x</code> (e raised to the given power)</p></td>
</tr>
<tr>
<td><div id="floor" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>floor(x: N) -&gt; N</code></pre>
</div>
</div>
<p>The largest integer &lt;= <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="ln" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>ln(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>Natural logarithm of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="ln" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>ln(x: numeric) -&gt; numeric</code></pre>
</div>
</div>
<p>Natural logarithm of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="log" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>log(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>Base 10 logarithm of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="log" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>log(x: numeric) -&gt; numeric</code></pre>
</div>
</div>
<p>Base 10 logarithm of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="log10" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>log10(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>Base 10 logarithm of <code>x</code>, same as
<code>log</code>.</p></td>
</tr>
<tr>
<td><div id="log10" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>log10(x: numeric) -&gt; numeric</code></pre>
</div>
</div>
<p>Base 10 logarithm of <code>x</code>, same as
<code>log</code>.</p></td>
</tr>
<tr>
<td><div id="log" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>log(b: numeric, x: numeric) -&gt; numeric</code></pre>
</div>
</div>
<p>Base <code>b</code> logarithm of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="mod" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>mod(x: N, y: N) -&gt; N</code></pre>
</div>
</div>
<p><code>x % y</code></p></td>
</tr>
<tr>
<td><div id="pow" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pow(x: double precision, y: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>Alias of <code>power</code>.</p></td>
</tr>
<tr>
<td><div id="pow" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pow(x: numeric, y: numeric) -&gt; numeric</code></pre>
</div>
</div>
<p>Alias of <code>power</code>.</p></td>
</tr>
<tr>
<td><div id="power" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>power(x: double precision, y: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p><code>x</code> raised to the power of <code>y</code>.</p></td>
</tr>
<tr>
<td><div id="power" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>power(x: numeric, y: numeric) -&gt; numeric</code></pre>
</div>
</div>
<p><code>x</code> raised to the power of <code>y</code>.</p></td>
</tr>
<tr>
<td><div id="round" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>round(x: N) -&gt; N</code></pre>
</div>
</div>
<p><code>x</code> rounded to the nearest whole number. If <code>N</code>
is <code>real</code> or <code>double precision</code>, rounds ties to
the nearest even number. If <code>N</code> is <code>numeric</code>,
rounds ties away from zero.</p></td>
</tr>
<tr>
<td><div id="round" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>round(x: numeric, y: int) -&gt; numeric</code></pre>
</div>
</div>
<p><code>x</code> rounded to <code>y</code> decimal places, while
retaining the same <a href="../types/numeric"><code>numeric</code></a>
scale; rounds ties away from zero.</p></td>
</tr>
<tr>
<td><div id="sqrt" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>sqrt(x: numeric) -&gt; numeric</code></pre>
</div>
</div>
<p>The square root of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="sqrt" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>sqrt(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The square root of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="trunc" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>trunc(x: N) -&gt; N</code></pre>
</div>
</div>
<p><code>x</code> truncated toward zero to a whole number.</p></td>
</tr>
</tbody>
</table>

### Trigonometric functions

Trigonometric functions take and return `double precision` values.

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="cos" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>cos(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The cosine of <code>x</code>, with <code>x</code> in
radians.</p></td>
</tr>
<tr>
<td><div id="acos" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>acos(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The inverse cosine of <code>x</code>, result in radians.</p></td>
</tr>
<tr>
<td><div id="cosh" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>cosh(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The hyperbolic cosine of <code>x</code>, with <code>x</code> as a
hyperbolic angle.</p></td>
</tr>
<tr>
<td><div id="acosh" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>acosh(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The inverse hyperbolic cosine of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="cot" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>cot(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The cotangent of <code>x</code>, with <code>x</code> in
radians.</p></td>
</tr>
<tr>
<td><div id="sin" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>sin(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The sine of <code>x</code>, with <code>x</code> in radians.</p></td>
</tr>
<tr>
<td><div id="asin" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>asin(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The inverse sine of <code>x</code>, result in radians.</p></td>
</tr>
<tr>
<td><div id="sinh" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>sinh(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The hyperbolic sine of <code>x</code>, with <code>x</code> as a
hyperbolic angle.</p></td>
</tr>
<tr>
<td><div id="asinh" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>asinh(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The inverse hyperbolic sine of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="tan" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>tan(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The tangent of <code>x</code>, with <code>x</code> in
radians.</p></td>
</tr>
<tr>
<td><div id="atan" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>atan(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The inverse tangent of <code>x</code>, result in radians.</p></td>
</tr>
<tr>
<td><div id="tanh" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>tanh(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The hyperbolic tangent of <code>x</code>, with <code>x</code> as a
hyperbolic angle.</p></td>
</tr>
<tr>
<td><div id="atanh" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>atanh(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>The inverse hyperbolic tangent of <code>x</code>.</p></td>
</tr>
<tr>
<td><div id="radians" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>radians(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>Converts degrees to radians.</p></td>
</tr>
<tr>
<td><div id="degrees" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>degrees(x: double precision) -&gt; double precision</code></pre>
</div>
</div>
<p>Converts radians to degrees.</p></td>
</tr>
</tbody>
</table>

### String functions

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="ascii" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>ascii(s: str) -&gt; int</code></pre>
</div>
</div>
<p>The ASCII value of <code>s</code>’s left-most character.</p></td>
</tr>
<tr>
<td><div id="btrim" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>btrim(s: str) -&gt; str</code></pre>
</div>
</div>
<p>Trim all spaces from both sides of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="btrim" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>btrim(s: str, c: str) -&gt; str</code></pre>
</div>
</div>
<p>Trim any character in <code>c</code> from both sides of
<code>s</code>.</p></td>
</tr>
<tr>
<td><div id="bit_count" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>bit_count(b: bytea) -&gt; int</code></pre>
</div>
</div>
<p>Returns the number of bits set in the bit string (aka
<em>popcount</em>).</p></td>
</tr>
<tr>
<td><div id="bit_length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>bit_length(s: str) -&gt; int</code></pre>
</div>
</div>
<p>Number of bits in <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="bit_length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>bit_length(b: bytea) -&gt; int</code></pre>
</div>
</div>
<p>Number of bits in <code>b</code>.</p></td>
</tr>
<tr>
<td><div id="char_length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>char_length(s: str) -&gt; int</code></pre>
</div>
</div>
<p>Number of code points in <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="chr" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>chr(i: int) -&gt; str</code></pre>
</div>
</div>
<p>Character with the given Unicode codepoint. Only supports codepoints
that can be encoded in UTF-8. The NULL (0) character is not
allowed.</p></td>
</tr>
<tr>
<td><div id="concat" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>concat(f: any, r: any...) -&gt; text</code></pre>
</div>
</div>
<p>Concatenates the text representation of non-NULL arguments.</p></td>
</tr>
<tr>
<td><div id="concat_ws" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>concat_ws(sep: str, f: any, r: any...) -&gt; text</code></pre>
</div>
</div>
<p>Concatenates the text representation of non-NULL arguments from
<code>f</code> and <code>r</code> separated by
<code>sep</code>.</p></td>
</tr>
<tr>
<td><div id="convert_from" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>convert_from(b: bytea, src_encoding: text) -&gt; text</code></pre>
</div>
</div>
<p>Convert data <code>b</code> from original encoding specified by
<code>src_encoding</code> into <code>text</code>.</p></td>
</tr>
<tr>
<td><div id="decode" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>decode(s: text, format: text) -&gt; bytea</code></pre>
</div>
</div>
<p>Decode <code>s</code> using the specified textual representation</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/encode">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="encode" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>encode(b: bytea, format: text) -&gt; text</code></pre>
</div>
</div>
<p>Encode <code>b</code> using the specified textual representation</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/encode">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="get_bit" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>get_bit(b: bytea, n: int) -&gt; int</code></pre>
</div>
</div>
<p>Return the <code>n</code>th bit from <code>b</code>, where the
left-most bit in <code>b</code> is at the 0th position.</p></td>
</tr>
<tr>
<td><div id="get_byte" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>get_byte(b: bytea, n: int) -&gt; int</code></pre>
</div>
</div>
<p>Return the <code>n</code>th byte from <code>b</code>, where the
left-most byte in <code>b</code> is at the 0th position.</p></td>
</tr>
<tr>
<td><div id="constant_time_eq" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>constant_time_eq(a: bytea, b: bytea) -&gt; bool</code></pre>
</div>
</div>
<p>Returns <code>true</code> if the arrays are identical, otherwise
returns <code>false</code>. The implementation mitigates timing attacks
by making a best-effort attempt to execute in constant time if the
arrays have the same length, regardless of their contents.</p></td>
</tr>
<tr>
<td><div id="constant_time_eq" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>constant_time_eq(a: text, b: text) -&gt; bool</code></pre>
</div>
</div>
<p>Returns <code>true</code> if the strings are identical, otherwise
returns <code>false</code>. The implementation mitigates timing attacks
by making a best-effort attempt to execute in constant time if the
strings have the same length, regardless of their contents.</p></td>
</tr>
<tr>
<td><div id="initcap" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>initcap(a: text) -&gt; text</code></pre>
</div>
</div>
<p>Returns <code>a</code> with the first character of every word in
upper case and all other characters in lower case. Words are separated
by non-alphanumeric characters.</p></td>
</tr>
<tr>
<td><div id="left" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>left(s: str, n: int) -&gt; str</code></pre>
</div>
</div>
<p>The first <code>n</code> characters of <code>s</code>. If
<code>n</code> is negative, all but the last <code>|n|</code> characters
of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>length(s: str) -&gt; int</code></pre>
</div>
</div>
<p>Number of code points in <code>s</code>.</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/length">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>length(b: bytea) -&gt; int</code></pre>
</div>
</div>
<p>Number of bytes in <code>s</code>.</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/length">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>length(s: bytea, encoding_name: str) -&gt; int</code></pre>
</div>
</div>
<p>Number of code points in <code>s</code> after encoding</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/length">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="lower" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>lower(s: str) -&gt; str</code></pre>
</div>
</div>
<p>Convert <code>s</code> to lowercase.</p></td>
</tr>
<tr>
<td><div id="lpad" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>lpad(s: str, len: int) -&gt; str</code></pre>
</div>
</div>
<p>Prepend <code>s</code> with spaces up to length <code>len</code>, or
right truncate if <code>len</code> is less than the length of
<code>s</code>.</p></td>
</tr>
<tr>
<td><div id="lpad" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>lpad(s: str, len: int, p: str) -&gt; str</code></pre>
</div>
</div>
<p>Prepend <code>s</code> with characters pulled from <code>p</code> up
to length <code>len</code>, or right truncate if <code>len</code> is
less than the length of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="ltrim" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>ltrim(s: str) -&gt; str</code></pre>
</div>
</div>
<p>Trim all spaces from the left side of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="ltrim" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>ltrim(s: str, c: str) -&gt; str</code></pre>
</div>
</div>
<p>Trim any character in <code>c</code> from the left side of
<code>s</code>.</p></td>
</tr>
<tr>
<td><div id="octet_length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>octet_length(s: str) -&gt; int</code></pre>
</div>
</div>
<p>Number of bytes in <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="octet_length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>octet_length(b: bytea) -&gt; int</code></pre>
</div>
</div>
<p>Number of bytes in <code>b</code>.</p></td>
</tr>
<tr>
<td><div id="parse_ident" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>parse_ident(ident: str[, strict_mode: bool]) -&gt; str[]</code></pre>
</div>
</div>
<p>Given a qualified identifier like <code>a."b".c</code>, splits into
an array of the constituent identifiers with quoting removed and escape
sequences decoded. Extra characters after the last identifier are
ignored unless the <code>strict_mode</code> parameter is
<code>true</code> (defaults to <code>false</code>).</p></td>
</tr>
<tr>
<td><div id="position" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>position(sub: str IN s: str) -&gt; int</code></pre>
</div>
</div>
<p>The starting index of <code>sub</code> within <code>s</code> or
<code>0</code> if <code>sub</code> is not a substring of
<code>s</code>.</p></td>
</tr>
<tr>
<td><div id="regexp_match" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>regexp_match(haystack: str, needle: str [, flags: str]]) -&gt; str[]</code></pre>
</div>
</div>
<p>Matches the regular expression <code>needle</code> against haystack,
returning a string array that contains the value of each capture group
specified in <code>needle</code>, in order. If <code>flags</code> is set
to the string <code>i</code> matches case-insensitively.</p></td>
</tr>
<tr>
<td><div id="regexp_replace" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>regexp_replace(source: str, pattern: str, replacement: str [, flags: str]]) -&gt; str</code></pre>
</div>
</div>
<p>Replaces the first occurrence of <code>pattern</code> with
<code>replacement</code> in <code>source</code>. No match will return
<code>source</code> unchanged.</p>
<p>If <code>flags</code> is set to <code>g</code>, all occurrences are
replaced. If <code>flags</code> is set to <code>i</code>, matches
case-insensitively.</p>
<p><code>$N</code> or <code>$name</code> in <code>replacement</code> can
be used to match capture groups. <code>${N}</code> must be used to
disambiguate capture group indexes from names if other characters follow
<code>N</code>. A <code>$$</code> in <code>replacement</code> will write
a literal <code>$</code>.</p>
<p>See the <a
href="https://docs.rs/regex/latest/regex/struct.Regex.html#method.replace">rust
regex docs</a> for more details about replacement.</p></td>
</tr>
<tr>
<td><div id="regexp_matches" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>regexp_matches(haystack: str, needle: str [, flags: str]]) -&gt; str[]</code></pre>
</div>
</div>
<p>Matches the regular expression <code>needle</code> against haystack,
returning a string array that contains the value of each capture group
specified in <code>needle</code>, in order. If <code>flags</code> is set
to the string <code>i</code> matches case-insensitively. If
<code>flags</code> is set to the string <code>g</code> all matches are
returned, otherwise only the first match is returned. Without the
<code>g</code> flag, the behavior is the same as
<code>regexp_match</code>.</p></td>
</tr>
<tr>
<td><div id="regexp_split_to_array" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>regexp_split_to_array(text: str, pattern: str [, flags: str]]) -&gt; str[]</code></pre>
</div>
</div>
<p>Splits <code>text</code> by the regular expression
<code>pattern</code> into an array. If <code>flags</code> is set to
<code>i</code>, matches case-insensitively.</p></td>
</tr>
<tr>
<td><div id="repeat" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>repeat(s: str, n: int) -&gt; str</code></pre>
</div>
</div>
<p>Replicate the string <code>n</code> times.</p></td>
</tr>
<tr>
<td><div id="replace" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>replace(s: str, f: str, r: str) -&gt; str</code></pre>
</div>
</div>
<p><code>s</code> with all instances of <code>f</code> replaced with
<code>r</code>.</p></td>
</tr>
<tr>
<td><div id="right" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>right(s: str, n: int) -&gt; str</code></pre>
</div>
</div>
<p>The last <code>n</code> characters of <code>s</code>. If
<code>n</code> is negative, all but the first <code>|n|</code>
characters of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="rtrim" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>rtrim(s: str) -&gt; str</code></pre>
</div>
</div>
<p>Trim all spaces from the right side of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="rtrim" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>rtrim(s: str, c: str) -&gt; str</code></pre>
</div>
</div>
<p>Trim any character in <code>c</code> from the right side of
<code>s</code>.</p></td>
</tr>
<tr>
<td><div id="split_part" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>split_part(s: str, d: s, i: int) -&gt; str</code></pre>
</div>
</div>
<p>Split <code>s</code> on delimiter <code>d</code>. Return the
<code>str</code> at index <code>i</code>, counting from 1.</p></td>
</tr>
<tr>
<td><div id="starts_with" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>starts_with(s: str, prefix: str) -&gt; bool</code></pre>
</div>
</div>
<p>Report whether <code>s</code> starts with
<code>prefix</code>.</p></td>
</tr>
<tr>
<td><div id="substring" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>substring(s: str, start_pos: int) -&gt; str</code></pre>
</div>
</div>
<p>Substring of <code>s</code> starting at <code>start_pos</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/substring">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="substring" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>substring(s: str, start_pos: int, l: int) -&gt; str</code></pre>
</div>
</div>
<p>Substring starting at <code>start_pos</code> of length
<code>l</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/substring">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="substring" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>substring(&#39;s&#39; [FROM &#39;start_pos&#39;]? [FOR &#39;l&#39;]?) -&gt; str</code></pre>
</div>
</div>
<p>Substring starting at <code>start_pos</code> of length
<code>l</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/substring">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="translate" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>translate(s: str, from: str, to: str) -&gt; str</code></pre>
</div>
</div>
<p>Any character in <code>s</code> that matches a character in
<code>from</code> is replaced by the corresponding character in
<code>to</code>. If <code>from</code> is longer than <code>to</code>,
occurrences of the extra characters in <code>from</code> are
removed.</p></td>
</tr>
<tr>
<td><div id="trim" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>trim([BOTH | LEADING | TRAILING]? [&#39;c&#39;? FROM]? &#39;s&#39;) -&gt; str</code></pre>
</div>
</div>
<p>Trims any character in <code>c</code> from <code>s</code> on the
specified side.</p>
<p>Defaults:</p>
<p>• Side: <code>BOTH</code></p>
<p>• <code>'c'</code>: <code>' '</code> (space)</p></td>
</tr>
<tr>
<td><div id="try_parse_monotonic_iso8601_timestamp"
class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>try_parse_monotonic_iso8601_timestamp(s: str) -&gt; timestamp</code></pre>
</div>
</div>
<p>Parses a specific subset of ISO8601 timestamps, returning
<code>NULL</code> instead of error on failure:
<code>YYYY-MM-DDThh:mm:ss.sssZ</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/pushdown">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="upper" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>upper(s: str) -&gt; str</code></pre>
</div>
</div>
<p>Convert <code>s</code> to uppercase.</p></td>
</tr>
<tr>
<td><div id="reverse" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>reverse(s: str) -&gt; str</code></pre>
</div>
</div>
<p>Reverse the characters in <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="string_to_array" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>string_to_array(s: str, delimiter: str [, null_string: str]) -&gt; str[]</code></pre>
</div>
</div>
<p>Splits the string at occurrences of delimiter and returns a text
array of the split segments.</p>
<p>If <code>delimiter</code> is NULL, each character in the string will
become a separate element in the array.</p>
<p>If <code>delimiter</code> is an empty string, then the string is
treated as a single field.</p>
<p>If <code>null_string</code> is supplied and is not NULL, fields
matching that string are replaced by NULL.</p>
<p>For example: <code>string_to_array('xx~~yy~~zz', '~~', 'yy')</code> →
<code>{xx,NULL,zz}</code></p></td>
</tr>
</tbody>
</table>

### Scalar functions

Scalar functions take a list of scalar expressions

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="expression-bool_op-all" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression bool_op ALL(s: Scalars) -&gt; bool</code></pre>
</div>
</div>
<p><code>true</code> if applying <a
href="#boolean-operators">bool_op</a> to <code>expression</code> and
every value of <code>s</code> evaluates to <code>true</code>.</p></td>
</tr>
<tr>
<td><div id="expression-bool_op-any" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression bool_op ANY(s: Scalars) -&gt; bool</code></pre>
</div>
</div>
<p><code>true</code> if applying <a
href="#boolean-operators">bool_op</a> to <code>expression</code> and any
value of <code>s</code> evaluates to <code>true</code>. Avoid using in
equi-join conditions as its use in the equi-join condition can lead to a
significant increase in memory usage. See <a
href="/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/any">idiomatic
Materialize SQL</a> for the alternative.</p></td>
</tr>
<tr>
<td><div id="expression-in" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression IN(s: Scalars) -&gt; bool</code></pre>
</div>
</div>
<p><code>true</code> for each value in <code>expression</code> if it
matches at least one element of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="expression-not-in" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression NOT IN(s: Scalars) -&gt; bool</code></pre>
</div>
</div>
<p><code>true</code> for each value in <code>expression</code> if it
does not match any elements of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="expression-bool_op-some" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression bool_op SOME(s: Scalars) -&gt; bool</code></pre>
</div>
</div>
<p><code>true</code> if applying <a
href="#boolean-operators">bool_op</a> to <code>expression</code> and any
value of <code>s</code> evaluates to <code>true</code>.</p></td>
</tr>
</tbody>
</table>

### Subquery functions

Subquery functions take a query, e.g.
[`SELECT`](/docs/self-managed/v25.2/sql/select)

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="expression-bool_op-all" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression bool_op ALL(s: Query) -&gt; bool</code></pre>
</div>
</div>
<p><code>s</code> must return exactly one column; <code>true</code> if
applying <a href="#boolean-operators">bool_op</a> to
<code>expression</code> and every value of <code>s</code> evaluates to
<code>true</code>.</p></td>
</tr>
<tr>
<td><div id="expression-bool_op-any" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression bool_op ANY(s: Query) -&gt; bool</code></pre>
</div>
</div>
<p><code>s</code> must return exactly one column; <code>true</code> if
applying <a href="#boolean-operators">bool_op</a> to
<code>expression</code> and any value of <code>s</code> evaluates to
<code>true</code>.</p></td>
</tr>
<tr>
<td><div id="csv_extract" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>csv_extract(num_csv_col: int, col_name: string) -&gt; col1: string, ... coln: string</code></pre>
</div>
</div>
<p>Extracts separated values from a column containing a CSV file
formatted as a string</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/csv_extract">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="exists" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>EXISTS(s: Query) -&gt; bool</code></pre>
</div>
</div>
<p><code>true</code> if <code>s</code> returns at least one
row.</p></td>
</tr>
<tr>
<td><div id="expression-in" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression IN(s: Query) -&gt; bool</code></pre>
</div>
</div>
<p><code>s</code> must return exactly one column; <code>true</code> for
each value in <code>expression</code> if it matches at least one element
of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="not-exists" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>NOT EXISTS(s: Query) -&gt; bool</code></pre>
</div>
</div>
<p><code>true</code> if <code>s</code> returns zero rows.</p></td>
</tr>
<tr>
<td><div id="expression-not-in" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression NOT IN(s: Query) -&gt; bool</code></pre>
</div>
</div>
<p><code>s</code> must return exactly one column; <code>true</code> for
each value in <code>expression</code> if it does not match any elements
of <code>s</code>.</p></td>
</tr>
<tr>
<td><div id="expression-bool_op-some" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>expression bool_op SOME(s: Query) -&gt; bool</code></pre>
</div>
</div>
<p><code>s</code> must return exactly one column; <code>true</code> if
applying <a href="#boolean-operators">bool_op</a> to
<code>expression</code> and any value of <code>s</code> evaluates to
<code>true</code>.</p></td>
</tr>
</tbody>
</table>

### Date and time functions

Time functions take or produce a time-like type, e.g.
[`date`](../types/date), [`timestamp`](../types/timestamp),
[`timestamp with time zone`](../types/timestamptz).

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="age" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>age(timestamp, timestamp) -&gt; interval</code></pre>
</div>
</div>
<p>Subtracts one timestamp from another, producing a “symbolic” result
that uses years and months, rather than just days.</p></td>
</tr>
<tr>
<td><div id="current_timestamp" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>current_timestamp() -&gt; timestamptz</code></pre>
</div>
</div>
<p>The <code>timestamp with time zone</code> representing when the query
was executed.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="date_bin" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>date_bin(stride: interval, source: timestamp, origin: timestamp) -&gt; timestamp</code></pre>
</div>
</div>
<p>Align <code>source</code> with <code>origin</code> along
<code>stride</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/date-bin">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="date_trunc" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>date_trunc(time_component: str, val: timestamp) -&gt; timestamp</code></pre>
</div>
</div>
<p>Largest <code>time_component</code> &lt;= <code>val</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/date-trunc">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="date_trunc" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>date_trunc(time_component: str, val: interval) -&gt; interval</code></pre>
</div>
</div>
<p>Largest <code>time_component</code> &lt;= <code>val</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/date-trunc">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="extract" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>EXTRACT(extract_expr) -&gt; numeric</code></pre>
</div>
</div>
<p>Specified time component from value</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/extract">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="date_part" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>date_part(time_component: str, val: timestamp) -&gt; float</code></pre>
</div>
</div>
<p>Specified time component from value</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/date-part">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="mz_now" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>mz_now() -&gt; mz_timestamp</code></pre>
</div>
</div>
<p>The logical time at which a query executes. Used for temporal filters
and query timestamp introspection</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/now_and_mz_now">(docs)</a>.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>, but can be used
in limited contexts in materialized views as a <a
href="/docs/self-managed/v25.2/transform-data/patterns/temporal-filters/">temporal
filter</a>.</p></td>
</tr>
<tr>
<td><div id="now" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>now() -&gt; timestamptz</code></pre>
</div>
</div>
<p>The <code>timestamp with time zone</code> representing when the query
was executed</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/now_and_mz_now">(docs)</a>.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="timestamp-at-time-zone-zone-timestamptz"
class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>timestamp AT TIME ZONE zone -&gt; timestamptz</code></pre>
</div>
</div>
<p>Converts <code>timestamp</code> to the specified time zone, expressed
as an offset from UTC</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/timezone-and-at-time-zone">(docs)</a>.</p>
<p><strong>Known limitation:</strong> You must explicitly cast the type
for the time zone.</p></td>
</tr>
<tr>
<td><div id="timestamptz-at-time-zone-zone-timestamp"
class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>timestamptz AT TIME ZONE zone -&gt; timestamp</code></pre>
</div>
</div>
<p>Converts <code>timestamp with time zone</code> from UTC to the
specified time zone, expressed as the local time</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/timezone-and-at-time-zone">(docs)</a>.</p>
<p><strong>Known limitation:</strong> You must explicitly cast the type
for the time zone.</p></td>
</tr>
<tr>
<td><div id="timezone" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>timezone(zone, timestamp) -&gt; timestamptz</code></pre>
</div>
</div>
<p>Converts <code>timestamp</code> to specified time zone, expressed as
an offset from UTC</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/timezone-and-at-time-zone">(docs)</a>.</p>
<p><strong>Known limitation:</strong> You must explicitly cast the type
for the time zone.</p></td>
</tr>
<tr>
<td><div id="timezone" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>timezone(zone, timestamptz) -&gt; timestamp</code></pre>
</div>
</div>
<p>Converts <code>timestamp with time zone</code> from UTC to specified
time zone, expressed as the local time</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/timezone-and-at-time-zone">(docs)</a>.</p>
<p><strong>Known limitation:</strong> You must explicitly cast the type
for the time zone.</p></td>
</tr>
<tr>
<td><div id="timezone_offset" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>timezone_offset(zone: str, when: timestamptz) -&gt;
(abbrev: str, base_utc_offset: interval, dst_offset: interval)</code></pre>
</div>
</div>
<p>Describes a time zone’s offset from UTC at a specified moment.</p>
<p><code>zone</code> must be a valid IANA Time Zone Database
identifier.</p>
<p><code>when</code> is a <code>timestamp with time zone</code> that
specifies the moment at which to determine <code>zone</code>’s offset
from UTC.</p>
<p><code>abbrev</code> is the abbreviation for <code>zone</code> that is
in use at the specified moment (e.g., <code>EST</code> or
<code>EDT</code>).</p>
<p><code>base_utc_offset</code> is the base offset from UTC at the
specified moment (e.g., <code>-5 hours</code>). Positive offsets mean
east of Greenwich; negative offsets mean west of Greenwich.</p>
<p><code>dst_offset</code> is the additional offset at the specified
moment due to Daylight Saving Time rules (e.g., <code>1 hours</code>).
If non-zero, Daylight Saving Time is in effect.</p></td>
</tr>
<tr>
<td><div id="to_timestamp" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>to_timestamp(val: double precision) -&gt; timestamptz</code></pre>
</div>
</div>
<p>Converts Unix epoch (seconds since 00:00:00 UTC on January 1, 1970)
to timestamp.</p></td>
</tr>
<tr>
<td><div id="to_char" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>to_char(val: timestamp, format: str)</code></pre>
</div>
</div>
<p>Converts a timestamp into a string using the specified format</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/to_char">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="justify_days" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>justify_days(val: interval) -&gt; interval</code></pre>
</div>
</div>
<p>Adjust interval so 30-day time periods are represented as months</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/justify-days">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="justify_hours" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>justify_hours(val: interval) -&gt; interval</code></pre>
</div>
</div>
<p>Adjust interval so 24-hour time periods are represented as days</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/justify-hours">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="justify_interval" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>justify_interval(val: interval) -&gt; interval</code></pre>
</div>
</div>
<p>Adjust interval using justify_days and justify_hours, with additional
sign adjustments</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/justify-interval">(docs)</a>.</p></td>
</tr>
</tbody>
</table>

### UUID functions

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="uuid_generate_v5" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>uuid_generate_v5(namespace: uuid, name: text) -&gt; uuid</code></pre>
</div>
</div>
<p>Generates a <a
href="https://www.rfc-editor.org/rfc/rfc4122#page-7">version 5 UUID</a>
(SHA-1) in the given namespace using the specified input name.</p></td>
</tr>
</tbody>
</table>

### JSON functions

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="jsonb_agg" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_agg(expression) -&gt; jsonb</code></pre>
</div>
</div>
<p>Aggregate values (including nulls) as a jsonb array</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/jsonb_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_array_elements" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_array_elements(j: jsonb) -&gt; Col&lt;jsonb&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s elements if <code>j</code> is an array</p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_array_elements">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_array_elements_text" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_array_elements_text(j: jsonb) -&gt; Col&lt;string&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s elements if <code>j</code> is an array</p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_array_elements_text">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_array_length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_array_length(j: jsonb) -&gt; int</code></pre>
</div>
</div>
<p>Number of elements in <code>j</code>’s outermost array</p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_array_length">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_build_array" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_build_array(x: ...) -&gt; jsonb</code></pre>
</div>
</div>
<p>Output each element of <code>x</code> as a <code>jsonb</code> array.
Elements can be of heterogenous types</p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_build_array">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_build_object" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_build_object(x: ...) -&gt; jsonb</code></pre>
</div>
</div>
<p>The elements of x as a <code>jsonb</code> object. The argument list
alternates between keys and values</p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_build_object">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_each" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_each(j: jsonb) -&gt; Col&lt;(key: string, value: jsonb)&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s outermost elements if <code>j</code> is an
object</p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_each">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_each_text" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_each_text(j: jsonb) -&gt; Col&lt;(key: string, value: string)&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s outermost elements if <code>j</code> is an
object</p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_each_text">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_object_agg" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_object_agg(keys, values) -&gt; jsonb</code></pre>
</div>
</div>
<p>Aggregate keys and values (including nulls) as a <code>jsonb</code>
object</p>
<p><a
href="/docs/self-managed/v25.2/sql/functions/jsonb_object_agg">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_object_keys" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_object_keys(j: jsonb) -&gt; Col&lt;string&gt;</code></pre>
</div>
</div>
<p><code>j</code>’s outermost keys if <code>j</code> is an object</p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_object_keys">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_pretty" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_pretty(j: jsonb) -&gt; string</code></pre>
</div>
</div>
<p>Pretty printed (i.e. indented) <code>j</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_pretty">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_typeof" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_typeof(j: jsonb) -&gt; string</code></pre>
</div>
</div>
<p>Type of <code>j</code>’s outermost value. One of <code>object</code>,
<code>array</code>, <code>string</code>, <code>number</code>,
<code>boolean</code>, and <code>null</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_typeof">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="jsonb_strip_nulls" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>jsonb_strip_nulls(j: jsonb) -&gt; jsonb</code></pre>
</div>
</div>
<p><code>j</code> with all object fields with a value of
<code>null</code> removed. Other <code>null</code> values remain</p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#jsonb_strip_nulls">(docs)</a>.</p></td>
</tr>
<tr>
<td><div id="to_jsonb" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>to_jsonb(v: T) -&gt; jsonb</code></pre>
</div>
</div>
<p><code>v</code> as <code>jsonb</code></p>
<p><a
href="/docs/self-managed/v25.2/sql/types/jsonb#to_jsonb">(docs)</a>.</p></td>
</tr>
</tbody>
</table>

### Table functions

Table functions evaluate to a collection of rows rather than a single
row. You can use the `WITH ORDINALITY` and `ROWS FROM` clauses together
with table functions. For more details, see [Table
functions](/docs/self-managed/v25.2/sql/functions/table-functions).

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="generate_series" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>generate_series(start: int, stop: int) -&gt; Col&lt;int&gt;</code></pre>
</div>
</div>
<p>Generate all integer values between <code>start</code> and
<code>stop</code>, inclusive.</p></td>
</tr>
<tr>
<td><div id="generate_series" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>generate_series(start: int, stop: int, step: int) -&gt; Col&lt;int&gt;</code></pre>
</div>
</div>
<p>Generate all integer values between <code>start</code> and
<code>stop</code>, inclusive, incrementing by <code>step</code> each
time.</p></td>
</tr>
<tr>
<td><div id="generate_series" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>generate_series(start: timestamp, stop: timestamp, step: interval) -&gt; Col&lt;timestamp&gt;</code></pre>
</div>
</div>
<p>Generate all timestamp values between <code>start</code> and
<code>stop</code>, inclusive, incrementing by <code>step</code> each
time.</p></td>
</tr>
<tr>
<td><div id="generate_subscripts" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>generate_subscripts(a: anyarray, dim: int) -&gt; Col&lt;int&gt;</code></pre>
</div>
</div>
<p>Generates a series comprising the valid subscripts of the
<code>dim</code>‘th dimension of the given array
<code>a</code>.</p></td>
</tr>
<tr>
<td><div id="regexp_extract" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>regexp_extract(regex: str, haystack: str) -&gt; Col&lt;string&gt;</code></pre>
</div>
</div>
<p>Values of the capture groups of <code>regex</code> as matched in
<code>haystack</code>. Outputs each capture group in a separate column.
At least one capture group is needed. (The capture groups are the parts
of the regular expression between parentheses.)</p></td>
</tr>
<tr>
<td><div id="regexp_split_to_table" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>regexp_split_to_table(text: str, pattern: str [, flags: str]]) -&gt; Col&lt;string&gt;</code></pre>
</div>
</div>
<p>Splits <code>text</code> by the regular expression
<code>pattern</code>. If <code>flags</code> is set to <code>i</code>,
matches case-insensitively.</p></td>
</tr>
<tr>
<td><div id="unnest" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>unnest(a: anyarray)</code></pre>
</div>
</div>
<p>Expands the array <code>a</code> into a set of rows.</p></td>
</tr>
<tr>
<td><div id="unnest" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>unnest(l: anylist)</code></pre>
</div>
</div>
<p>Expands the list <code>l</code> into a set of rows.</p></td>
</tr>
<tr>
<td><div id="unnest" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>unnest(m: anymap)</code></pre>
</div>
</div>
<p>Expands the map <code>m</code> in a set of rows with the columns
<code>key</code> and <code>value</code>.</p></td>
</tr>
</tbody>
</table>

### Array functions

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="array_cat" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>array_cat(a1: arrayany, a2: arrayany) -&gt; arrayany</code></pre>
</div>
</div>
<p>Concatenates <code>a1</code> and <code>a2</code>.</p></td>
</tr>
<tr>
<td><div id="array_fill" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>array_fill(anyelement, int[], [, int[]]) -&gt; anyarray</code></pre>
</div>
</div>
<p>Returns an array initialized with supplied value and dimensions,
optionally with lower bounds other than 1.</p></td>
</tr>
<tr>
<td><div id="array_length" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>array_length(a: arrayany, dim: bigint) -&gt; int</code></pre>
</div>
</div>
<p>Returns the length of the specified dimension of the array.</p></td>
</tr>
<tr>
<td><div id="array_position" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>array_position(haystack: anycompatiblearray, needle: anycompatible) -&gt; int</code></pre>
</div>
</div>
<p>Returns the subscript of <code>needle</code> in
<code>haystack</code>. Returns <code>null</code> if not found.</p></td>
</tr>
<tr>
<td><div id="array_position" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>array_position(haystack: anycompatiblearray, needle: anycompatible, skip: int) -&gt; int</code></pre>
</div>
</div>
<p>Returns the subscript of <code>needle</code> in
<code>haystack</code>, skipping the first <code>skip</code> elements.
Returns <code>null</code> if not found.</p></td>
</tr>
<tr>
<td><div id="array_to_string" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>array_to_string(a: anyarray, sep: text [, ifnull: text]) -&gt; text</code></pre>
</div>
</div>
<p>Concatenates the elements of <code>array</code> together separated by
<code>sep</code>. Null elements are omitted unless <code>ifnull</code>
is non-null, in which case null elements are replaced with the value of
<code>ifnull</code>.</p></td>
</tr>
<tr>
<td><div id="array_remove" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>array_remove(a: anyarray, e: anyelement) -&gt; anyarray</code></pre>
</div>
</div>
<p>Returns the array <code>a</code> without any elements equal to the
given value <code>e</code>. The array must be one-dimensional.
Comparisons are done using `IS NOT DISTINCT FROM semantics, so it is
possible to remove NULLs.</p></td>
</tr>
</tbody>
</table>

### Hash functions

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="crc32" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>crc32(data: bytea) -&gt; uint32</code></pre>
</div>
</div>
<p>Computes the 32-bit cyclic redundancy check of the given bytea
<code>data</code> using the IEEE 802.3 polynomial.</p></td>
</tr>
<tr>
<td><div id="crc32" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>crc32(data: text) -&gt; uint32</code></pre>
</div>
</div>
<p>Computes the 32-bit cyclic redundancy check of the given text
<code>data</code> using the IEEE 802.3 polynomial.</p></td>
</tr>
<tr>
<td><div id="digest" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>digest(data: text, type: text) -&gt; bytea</code></pre>
</div>
</div>
<p>Computes a binary hash of the given text <code>data</code> using the
specified <code>type</code> algorithm. Supported hash algorithms are:
<code>md5</code>, <code>sha1</code>, <code>sha224</code>,
<code>sha256</code>, <code>sha384</code>, and
<code>sha512</code>.</p></td>
</tr>
<tr>
<td><div id="digest" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>digest(data: bytea, type: text) -&gt; bytea</code></pre>
</div>
</div>
<p>Computes a binary hash of the given bytea <code>data</code> using the
specified <code>type</code> algorithm. The supported hash algorithms are
the same as for the text variant of this function.</p></td>
</tr>
<tr>
<td><div id="hmac" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>hmac(data: text, key: text, type: text) -&gt; bytea</code></pre>
</div>
</div>
<p>Computes a hashed MAC of the given text <code>data</code> using the
specified <code>key</code> and <code>type</code> algorithm. Supported
hash algorithms are the same as for <code>digest</code>.</p></td>
</tr>
<tr>
<td><div id="hmac" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>hmac(data: bytea, key: bytea, type: text) -&gt; bytea</code></pre>
</div>
</div>
<p>Computes a hashed MAC of the given bytea <code>data</code> using the
specified <code>key</code> and <code>type</code> algorithm. The
supported hash algorithms are the same as for
<code>digest</code>.</p></td>
</tr>
<tr>
<td><div id="kafka_murmur2" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>kafka_murmur2(data: bytea) -&gt; integer</code></pre>
</div>
</div>
<p>Computes the Murmur2 hash of the given bytea <code>data</code> using
the seed used by Kafka’s default partitioner and with the high bit
cleared.</p></td>
</tr>
<tr>
<td><div id="kafka_murmur2" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>kafka_murmur2(data: text) -&gt; integer</code></pre>
</div>
</div>
<p>Computes the Murmur2 hash of the given text <code>data</code> using
the seed used by Kafka’s default partitioner and with the high bit
cleared.</p></td>
</tr>
<tr>
<td><div id="md5" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>md5(data: bytea) -&gt; text</code></pre>
</div>
</div>
<p>Computes the MD5 hash of the given bytea <code>data</code>. For
PostgreSQL compatibility, returns a hex-encoded value of type
<code>text</code> rather than <code>bytea</code>.</p></td>
</tr>
<tr>
<td><div id="seahash" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>seahash(data: bytea) -&gt; u64</code></pre>
</div>
</div>
<p>Computes the <a href="https://docs.rs/seahash">SeaHash</a> hash of
the given bytea <code>data</code>.</p></td>
</tr>
<tr>
<td><div id="seahash" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>seahash(data: text) -&gt; u64</code></pre>
</div>
</div>
<p>Computes the <a href="https://docs.rs/seahash">SeaHash</a> hash of
the given text <code>data</code>.</p></td>
</tr>
<tr>
<td><div id="sha224" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>sha224(data: bytea) -&gt; bytea</code></pre>
</div>
</div>
<p>Computes the SHA-224 hash of the given bytea
<code>data</code>.</p></td>
</tr>
<tr>
<td><div id="sha256" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>sha256(data: bytea) -&gt; bytea</code></pre>
</div>
</div>
<p>Computes the SHA-256 hash of the given bytea
<code>data</code>.</p></td>
</tr>
<tr>
<td><div id="sha384" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>sha384(data: bytea) -&gt; bytea</code></pre>
</div>
</div>
<p>Computes the SHA-384 hash of the given bytea
<code>data</code>.</p></td>
</tr>
<tr>
<td><div id="sha512" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>sha512(data: bytea) -&gt; bytea</code></pre>
</div>
</div>
<p>Computes the SHA-512 hash of the given bytea
<code>data</code>.</p></td>
</tr>
</tbody>
</table>

### Window functions

<div class="tip">

**💡 Tip:** For some window function query patterns, rewriting your
query to not use window functions can yield better performance. See
[Idiomatic Materialize
SQL](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/)
for details.

</div>

Window functions compute values across sets of rows related to the
current row. For example, you can use a window aggregation to smooth
measurement data by computing the average of the last 5 measurements
before every row as follows:

```
SELECT
  avg(measurement) OVER (ORDER BY time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
FROM measurements;
```

Window functions always need an `OVER` clause. For the `OVER` clause,
Materialize supports the same [syntax as
PostgreSQL](https://www.postgresql.org/docs/current/tutorial-window.html),
but supports only the following frame modes:

- the `ROWS` frame mode.

- the default frame, which is
  `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

<div class="note">

**NOTE:**

For [window
functions](/docs/self-managed/v25.2/sql/functions/#window-functions),
when an input record in a partition (as determined by the `PARTITION BY`
clause of your window function) is added/removed/changed, Materialize
recomputes the results for the entire window partition. This means that
when a new batch of input data arrives (that is, every second), **the
amount of computation performed is proportional to the total size of the
touched partitions**.

For example, assume that in a given second, 20 input records change, and
these records belong to **10** different partitions, where the average
size of each partition is **100**. Then, amount of work to perform is
proportional to computing the window function results for
**10\*100=1000** rows.

To avoid performance issues that may arise as the number of records
grows, consider rewriting your query to use idiomatic Materialize SQL
instead of window functions. If your query cannot be rewritten without
the window functions and the performance of window functions is
insufficient for your use case, please [contact our
team](/docs/self-managed/v25.2/support/).

See [Idiomatic Materialize
SQL](/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/)
for examples of rewriting window functions.

</div>

In addition to the below window functions, you can use the `OVER` clause
with any [aggregation function](#aggregate-functions) (e.g., `sum`,
`avg`) as well. Using an aggregation with an `OVER` clause is called a
*window aggregation*. A window aggregation computes the aggregate not on
the groups specified by the `GROUP BY` clause, but on the frames
specified inside the `OVER` clause. (Note that a window aggregation
produces exactly one output value *for each input row*. This is
different from a standard aggregation, which produces one output value
for each *group* specified by the `GROUP BY` clause.)

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="dense_rank" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>dense_rank() -&gt; int</code></pre>
</div>
</div>
<p>Returns the rank of the current row within its partition without
gaps, counting from 1. Rows that compare equal will have the same
rank.</p></td>
</tr>
<tr>
<td><div id="first_value" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>first_value(value anycompatible) -&gt; anyelement</code></pre>
</div>
</div>
<p>Returns <code>value</code> evaluated at the first row of the window
frame. The default window frame is
<code>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</code>.</p>
<p>See also <a
href="/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/first-value/">Idiomatic
Materialize SQL: First value</a>.</p></td>
</tr>
<tr>
<td><div id="lag" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>lag(value anycompatible [, offset integer [, default anycompatible ]]) -&gt; int</code></pre>
</div>
</div>
<p>Returns <code>value</code> evaluated at the row that is
<code>offset</code> rows before the current row within the partition; if
there is no such row, instead returns <code>default</code> (which must
be of a type compatible with <code>value</code>). If <code>offset</code>
is <code>NULL</code>, <code>NULL</code> is returned instead. Both
<code>offset</code> and <code>default</code> are evaluated with respect
to the current row. If omitted, <code>offset</code> defaults to 1 and
<code>default</code> to <code>NULL</code>.</p>
<p>See also <a
href="/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/lag/">Idiomatic
Materialize SQL: Lag over</a>.</p></td>
</tr>
<tr>
<td><div id="last_value" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>last_value(value anycompatible) -&gt; anyelement</code></pre>
</div>
</div>
<p>Returns <code>value</code> evaluated at the last row of the window
frame. The default window frame is
<code>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</code>.</p>
<p>See also <a
href="/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/last-value/">Idiomatic
Materialize SQL: Last value</a>.</p></td>
</tr>
<tr>
<td><div id="lead" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>lead(value anycompatible [, offset integer [, default anycompatible ]]) -&gt; int</code></pre>
</div>
</div>
<p>Returns <code>value</code> evaluated at the row that is
<code>offset</code> rows after the current row within the partition; if
there is no such row, instead returns <code>default</code> (which must
be of a type compatible with <code>value</code>). If <code>offset</code>
is <code>NULL</code>, <code>NULL</code> is returned instead. Both
<code>offset</code> and <code>default</code> are evaluated with respect
to the current row. If omitted, <code>offset</code> defaults to 1 and
<code>default</code> to <code>NULL</code>.</p>
<p>See also <a
href="/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/lead/">Idiomatic
Materialize SQL: Lead over</a>.</p></td>
</tr>
<tr>
<td><div id="rank" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>rank() -&gt; int</code></pre>
</div>
</div>
<p>Returns the rank of the current row within its partition with gaps
(counting from 1): rows that compare equal will have the same rank, and
then the rank is incremented by the number of rows that compared
equal.</p></td>
</tr>
<tr>
<td><div id="row_number" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>row_number() -&gt; int</code></pre>
</div>
</div>
<p>Returns the number of the current row within its partition, counting
from 1. Rows that compare equal will be ordered in an unspecified
way.</p>
<p>See also <a
href="/docs/self-managed/v25.2/transform-data/idiomatic-materialize-sql/top-k/">Idiomatic
Materialize SQL: Top-K</a>.</p></td>
</tr>
</tbody>
</table>

### System information functions

Functions that return information about the system.

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="mz_environment_id" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>mz_environment_id() -&gt; text</code></pre>
</div>
</div>
<p>Returns a string containing a <code>uuid</code> uniquely identifying
the Materialize environment.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="mz_uptime" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>mz_uptime() -&gt; interval</code></pre>
</div>
</div>
<p>Returns the length of time that the materialized process has been
running.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="mz_version" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>mz_version() -&gt; text</code></pre>
</div>
</div>
<p>Returns the server’s version information as a human-readable
string.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="mz_version_num" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>mz_version_num() -&gt; int</code></pre>
</div>
</div>
<p>Returns the server’s version as an integer having the format
<code>XXYYYZZ</code>, where <code>XX</code> is the major version,
<code>YYY</code> is the minor version and <code>ZZ</code> is the patch
version.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="current_database" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>current_database() -&gt; text</code></pre>
</div>
</div>
<p>Returns the name of the current database.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="current_catalog" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>current_catalog() -&gt; text</code></pre>
</div>
</div>
<p>Alias for <code>current_database</code>.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="current_user" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>current_user() -&gt; text</code></pre>
</div>
</div>
<p>Returns the name of the user who executed the containing query.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="current_role" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>current_role() -&gt; text</code></pre>
</div>
</div>
<p>Alias for <code>current_user</code>.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="user" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>user() -&gt; text</code></pre>
</div>
</div>
<p>Alias for <code>current_user</code>.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="session_user" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>session_user() -&gt; text</code></pre>
</div>
</div>
<p>Returns the name of the user who initiated the database
connection.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="mz_row_size" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>mz_row_size(expr: Record) -&gt; int</code></pre>
</div>
</div>
<p>Returns the number of bytes used to store a row.</p></td>
</tr>
</tbody>
</table>

### PostgreSQL compatibility functions

Functions whose primary purpose is to facilitate compatibility with
PostgreSQL tools. These functions may have suboptimal performance
characteristics.

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="format_type" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>format_type(oid: int, typemod: int) -&gt; text</code></pre>
</div>
</div>
<p>Returns the canonical SQL name for the type specified by
<code>oid</code> with <code>typemod</code> applied.</p></td>
</tr>
<tr>
<td><div id="current_schema" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>current_schema() -&gt; text</code></pre>
</div>
</div>
<p>Returns the name of the first non-implicit schema on the search path,
or <code>NULL</code> if the search path is empty.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="current_schemas" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>current_schemas(include_implicit: bool) -&gt; text[]</code></pre>
</div>
</div>
<p>Returns the names of the schemas on the search path. The
<code>include_implicit</code> parameter controls whether implicit
schemas like <code>mz_catalog</code> and <code>pg_catalog</code> are
included in the output.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="current_setting" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>current_setting(setting_name: text[, missing_ok: bool]) -&gt; text</code></pre>
</div>
</div>
<p>Returns the value of the named setting or error if it does not exist.
If <code>missing_ok</code> is true, return NULL if it does not
exist.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="obj_description" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>obj_description(oid: oid, catalog: text) -&gt; text</code></pre>
</div>
</div>
<p>Returns the comment for a database object specified by its
<code>oid</code> and the name of the containing system catalog.</p></td>
</tr>
<tr>
<td><div id="col_description" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>col_description(oid: oid, column: int) -&gt; text</code></pre>
</div>
</div>
<p>Returns the comment for a table column, which is specified by the
<code>oid</code> of its table and its column number.</p></td>
</tr>
<tr>
<td><div id="pg_backend_pid" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_backend_pid() -&gt; int</code></pre>
</div>
</div>
<p>Returns the internal connection ID.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="pg_cancel_backend" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_cancel_backend(connection_id: int) -&gt; bool</code></pre>
</div>
</div>
<p>Cancels an in-progress query on the specified connection ID. Returns
whether the connection ID existed (not if it cancelled a query).</p>
<p><strong>Note:</strong> This function is <a
href="#side-effecting-functions">side-effecting</a>.</p></td>
</tr>
<tr>
<td><div id="pg_column_size" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_column_size(expr: any) -&gt; int</code></pre>
</div>
</div>
<p>Returns the number of bytes used to store any individual data
value.</p></td>
</tr>
<tr>
<td><div id="pg_size_pretty" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_size_pretty(expr: numeric) -&gt; text</code></pre>
</div>
</div>
<p>Converts a size in bytes into a human-readable format.</p></td>
</tr>
<tr>
<td><div id="pg_get_constraintdef" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_get_constraintdef(oid: oid[, pretty: bool]) -&gt; text</code></pre>
</div>
</div>
<p>Returns the constraint definition for the given <code>oid</code>.
Currently always returns NULL since constraints aren’t
supported.</p></td>
</tr>
<tr>
<td><div id="pg_get_indexdef" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_get_indexdef(index: oid[, column: integer, pretty: bool]) -&gt; text</code></pre>
</div>
</div>
<p>Reconstructs the creating command for an index. (This is a decompiled
reconstruction, not the original text of the command.) If column is
supplied and is not zero, only the definition of that column is
reconstructed.</p></td>
</tr>
<tr>
<td><div id="pg_get_ruledef" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_get_ruledef(rule_oid: oid[, pretty bool]) -&gt; text</code></pre>
</div>
</div>
<p>Reconstructs the creating command for a rule. This function always
returns NULL because Materialize does not support rules.</p></td>
</tr>
<tr>
<td><div id="pg_get_userbyid" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_get_userbyid(role: oid) -&gt; text</code></pre>
</div>
</div>
<p>Returns the role (user) name for the given <code>oid</code>. If no
role matches the specified OID, the string
<code>unknown (OID=oid)</code> is returned.</p></td>
</tr>
<tr>
<td><div id="pg_get_viewdef" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_get_viewdef(view_name: text[, pretty: bool]) -&gt; text</code></pre>
</div>
</div>
<p>Returns the underlying SELECT command for the given view.</p></td>
</tr>
<tr>
<td><div id="pg_get_viewdef" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_get_viewdef(view_oid: oid[, pretty: bool]) -&gt; text</code></pre>
</div>
</div>
<p>Returns the underlying SELECT command for the given view.</p></td>
</tr>
<tr>
<td><div id="pg_get_viewdef" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_get_viewdef(view_oid: oid[, wrap_column: integer]) -&gt; text</code></pre>
</div>
</div>
<p>Returns the underlying SELECT command for the given view.</p></td>
</tr>
<tr>
<td><div id="pg_has_role" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_has_role([user: name or oid,] role: text or oid, privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Alias for <code>has_role</code> for PostgreSQL
compatibility.</p></td>
</tr>
<tr>
<td><div id="pg_is_in_recovery" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_is_in_recovery() -&gt; bool</code></pre>
</div>
</div>
<p>Returns if the a recovery is still in progress.</p></td>
</tr>
<tr>
<td><div id="pg_table_is_visible" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_table_is_visible(relation: oid) -&gt; boolean</code></pre>
</div>
</div>
<p>Reports whether the relation with the specified OID is visible in the
search path.</p></td>
</tr>
<tr>
<td><div id="pg_tablespace_location" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_tablespace_location(tablespace: oid) -&gt; text</code></pre>
</div>
</div>
<p>Returns the path in the file system that the provided tablespace is
on.</p></td>
</tr>
<tr>
<td><div id="pg_type_is_visible" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_type_is_visible(relation: oid) -&gt; boolean</code></pre>
</div>
</div>
<p>Reports whether the type with the specified OID is visible in the
search path.</p></td>
</tr>
<tr>
<td><div id="pg_function_is_visible" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_function_is_visible(relation: oid) -&gt; boolean</code></pre>
</div>
</div>
<p>Reports whether the function with the specified OID is visible in the
search path.</p></td>
</tr>
<tr>
<td><div id="pg_typeof" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_typeof(expr: any) -&gt; text</code></pre>
</div>
</div>
<p>Returns the type of its input argument as a string.</p></td>
</tr>
<tr>
<td><div id="pg_encoding_to_char" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_encoding_to_char(encoding_id: integer) -&gt; text</code></pre>
</div>
</div>
<p>PostgreSQL compatibility shim. Not intended for direct use.</p></td>
</tr>
<tr>
<td><div id="pg_postmaster_start_time" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_postmaster_start_time() -&gt; timestamptz</code></pre>
</div>
</div>
<p>Returns the time when the server started.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
<tr>
<td><div id="pg_relation_size" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_relation_size(relation: regclass[, fork: text]) -&gt; bigint</code></pre>
</div>
</div>
<p>Disk space used by the specified fork (‘main’, ‘fsm’, ‘vm’, or
‘init’) of the specified table or index. If no fork is specified, it
defaults to ‘main’. This function always returns -1 because Materialize
does not store tables and indexes on local disk.</p></td>
</tr>
<tr>
<td><div id="pg_stat_get_numscans" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>pg_stat_get_numscans(oid: oid) -&gt; bigint</code></pre>
</div>
</div>
<p>Number of sequential scans done when argument is a table, or number
of index scans done when argument is an index. This function always
returns -1 because Materialize does not collect statistics.</p></td>
</tr>
<tr>
<td><div id="version" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>version() -&gt; text</code></pre>
</div>
</div>
<p>Returns a PostgreSQL-compatible version string.</p>
<p><strong>Note:</strong> This function is <a
href="#unmaterializable-functions">unmaterializable</a>.</p></td>
</tr>
</tbody>
</table>

### Access privilege inquiry functions

Functions that allow querying object access privileges. None of the
following functions consider whether the provided role is a *superuser*
or not.

<table class="inline-headings">
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr>
<th><p>Function</p></th>
</tr>
</thead>
<tbody>
<tr>
<td><div id="has_cluster_privilege" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>has_cluster_privilege([role: text or oid,] cluster: text, privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the role with the specified role name or OID has the
privilege on the cluster with the specified cluster name. If the role is
omitted then the <code>current_role</code> is assumed.</p></td>
</tr>
<tr>
<td><div id="has_connection_privilege" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>has_connection_privilege([role: text or oid,] connection: text or oid, privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the role with the specified role name or OID has the
privilege on the connection with the specified connection name or OID.
If the role is omitted then the <code>current_role</code> is
assumed.</p></td>
</tr>
<tr>
<td><div id="has_database_privilege" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>has_database_privilege([role: text or oid,] database: text or oid, privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the role with the specified role name or OID has the
privilege on the database with the specified database name or OID. If
the role is omitted then the <code>current_role</code> is
assumed.</p></td>
</tr>
<tr>
<td><div id="has_schema_privilege" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>has_schema_privilege([role: text or oid,] schema: text or oid, privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the role with the specified role name or OID has the
privilege on the schema with the specified schema name or OID. If the
role is omitted then the <code>current_role</code> is assumed.</p></td>
</tr>
<tr>
<td><div id="has_role" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>has_role([user: name or oid,] role: text or oid, privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the <code>user</code> has the privilege for
<code>role</code>. <code>privilege</code> can either be
<code>MEMBER</code> or <code>USAGE</code>, however currently this value
is ignored. The <code>PUBLIC</code> pseudo-role cannot be used for the
<code>user</code> nor the <code>role</code>. If the <code>user</code> is
omitted then the <code>current_role</code> is assumed.</p></td>
</tr>
<tr>
<td><div id="has_secret_privilege" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>has_secret_privilege([role: text or oid,] secret: text or oid, privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the role with the specified role name or OID has the
privilege on the secret with the specified secret name or OID. If the
role is omitted then the <code>current_role</code> is assumed.</p></td>
</tr>
<tr>
<td><div id="has_system_privilege" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>has_system_privilege([role: text or oid,] privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the role with the specified role name or OID has the
system privilege. If the role is omitted then the
<code>current_role</code> is assumed.</p></td>
</tr>
<tr>
<td><div id="has_table_privilege" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>has_table_privilege([role: text or oid,] relation: text or oid, privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the role with the specified role name or OID has the
privilege on the relation with the specified relation name or OID. If
the role is omitted then the <code>current_role</code> is
assumed.</p></td>
</tr>
<tr>
<td><div id="has_type_privilege" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>has_type_privilege([role: text or oid,] type: text or oid, privilege: text) -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the role with the specified role name or OID has the
privilege on the type with the specified type name or OID. If the role
is omitted then the <code>current_role</code> is assumed.</p></td>
</tr>
<tr>
<td><div id="mz_is_superuser" class="heading docsearch_l3">
<div class="highlight">
<pre class="chroma" tabindex="0"><code>mz_is_superuser() -&gt; bool</code></pre>
</div>
</div>
<p>Reports whether the <code>current_role</code> is a
superuser.</p></td>
</tr>
</tbody>
</table>

## Operators

### Generic operators

| Operator    | Computes                               |
|-------------|----------------------------------------|
| `val::type` | Cast of `val` as `type` ([docs](cast)) |

### Boolean operators

| Operator | Computes |
|----|----|
| `AND` | Boolean “and” |
| `OR` | Boolean “or” |
| `=` | Equality |
| `<>` | Inequality |
| `!=` | Inequality |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal to |
| `>=` | Greater than or equal to |
| `a BETWEEN x AND y` | `a >= x AND a <= y` |
| `a NOT BETWEEN x AND y` | `a < x OR a > y` |
| `a IS NULL` | `a = NULL` |
| `a ISNULL` | `a = NULL` |
| `a IS NOT NULL` | `a != NULL` |
| `a IS TRUE` | `a` is true, requiring `a` to be a boolean |
| `a IS NOT TRUE` | `a` is not true, requiring `a` to be a boolean |
| `a IS FALSE` | `a` is false, requiring `a` to be a boolean |
| `a IS NOT FALSE` | `a` is not false, requiring `a` to be a boolean |
| `a IS UNKNOWN` | `a = NULL`, requiring `a` to be a boolean |
| `a IS NOT UNKNOWN` | `a != NULL`, requiring `a` to be a boolean |
| `a LIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE) |
| `a ILIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using case-insensitive [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE) |

### Numbers operators

| Operator | Computes            |
|----------|---------------------|
| `+`      | Addition            |
| `-`      | Subtraction         |
| `*`      | Multiplication      |
| `/`      | Division            |
| `%`      | Modulo              |
| `&`      | Bitwise AND         |
| `|`      | Bitwise OR          |
| `#`      | Bitwise XOR         |
| `~`      | Bitwise NOT         |
| `<<`     | Bitwise left shift  |
| `>>`     | Bitwise right shift |

### String operators

| Operator | Computes |
|----|----|
| `||` | Concatenation |
| `~~` | Matches LIKE pattern case sensitively, see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE) |
| `~~*` | Matches LIKE pattern case insensitively (ILIKE), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE) |
| `!~~` | Matches NOT LIKE pattern (case sensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE) |
| `!~~*` | Matches NOT ILIKE pattern (case insensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE) |
| `~` | Matches regular expression, case sensitive |
| `~*` | Matches regular expression, case insensitive |
| `!~` | Matches regular expression case sensitively, and inverts the match |
| `!~*` | Match regular expression case insensitively, and inverts the match |

The regular expression syntax supported by Materialize is documented by
the [Rust `regex` crate](https://docs.rs/regex/*/#syntax).

<div class="warning">

**WARNING!** Materialize regular expressions are similar to, but not
identical to, PostgreSQL regular expressions.

</div>

### Time-like operators

| Operation | Computes |
|----|----|
| [`date`](../types/date) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp) |
| [`date`](../types/date) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp) |
| [`date`](../types/date) `+` [`time`](../types/time) | [`timestamp`](../types/timestamp) |
| [`date`](../types/date) `-` [`date`](../types/date) | [`interval`](../types/interval) |
| [`timestamp`](../types/timestamp) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp) |
| [`timestamp`](../types/timestamp) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp) |
| [`timestamp`](../types/timestamp) `-` [`timestamp`](../types/timestamp) | [`interval`](../types/interval) |
| [`time`](../types/time) `+` [`interval`](../types/interval) | `time` |
| [`time`](../types/time) `-` [`interval`](../types/interval) | `time` |
| [`time`](../types/time) `-` [`time`](../types/time) | [`interval`](../types/interval) |

### JSON operators

| Operator | RHS Type | Description |
|----|----|----|
| `->` | `text`, `int` | Access field by name or index position, and return `jsonb` ([docs](/docs/self-managed/v25.2/sql/types/jsonb/#field-access-as-jsonb--)) |
| `->>` | `text`, `int` | Access field by name or index position, and return `text` ([docs](/docs/self-managed/v25.2/sql/types/jsonb/#field-access-as-text--)) |
| `#>` | `text[]` | Access field by path, and return `jsonb` ([docs](/docs/self-managed/v25.2/sql/types/jsonb/#path-access-as-jsonb-)) |
| `#>>` | `text[]` | Access field by path, and return `text` ([docs](/docs/self-managed/v25.2/sql/types/jsonb/#path-access-as-text-)) |
| `||` | `jsonb` | Concatenate LHS and RHS ([docs](/docs/self-managed/v25.2/sql/types/jsonb/#jsonb-concat-)) |
| `-` | `text` | Delete all values with key of RHS ([docs](/docs/self-managed/v25.2/sql/types/jsonb/#remove-key--)) |
| `@>` | `jsonb` | Does element contain RHS? ([docs](/docs/self-managed/v25.2/sql/types/jsonb/#lhs-contains-rhs-)) |
| `<@` | `jsonb` | Does RHS contain element? ([docs](/docs/self-managed/v25.2/sql/types/jsonb/#rhs-contains-lhs-)) |
| `?` | `text` | Is RHS a top-level key? ([docs](/docs/self-managed/v25.2/sql/types/jsonb/#search-top-level-keys-)) |

### Map operators

| Operator | RHS Type | Description |
|----|----|----|
| `->` | `string` | Access field by name, and return target field ([docs](/docs/self-managed/v25.2/sql/types/map/#retrieve-value-with-key--)) |
| `@>` | `map` | Does element contain RHS? ([docs](/docs/self-managed/v25.2/sql/types/map/#lhs-contains-rhs-)) |
| `<@` | `map` | Does RHS contain element? ([docs](/docs/self-managed/v25.2/sql/types/map/#rhs-contains-lhs-)) |
| `?` | `string` | Is RHS a top-level key? ([docs](/docs/self-managed/v25.2/sql/types/map/#search-top-level-keys-)) |
| `?&` | `string[]` | Does LHS contain all RHS top-level keys? ([docs](/docs/self-managed/v25.2/sql/types/map/#search-for-all-top-level-keys-)) |
| `?|` | `string[]` | Does LHS contain any RHS top-level keys? ([docs](/docs/self-managed/v25.2/sql/types/map/#search-for-any-top-level-keys-)) |

### List operators

List operators are [polymorphic](../types/list/#polymorphism).

| Operator | Description |
|----|----|
| `listany || listany` | Concatenate the two lists. |
| `listany || listelementany` | Append the element to the list. |
| `listelementany || listany` | Prepend the element to the list. |
| `listany @> listany` | Check if the first list contains all elements of the second list. |
| `listany <@ listany` | Check if all elements of the first list are contained in the second list. |

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/functions/_index.md"
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
