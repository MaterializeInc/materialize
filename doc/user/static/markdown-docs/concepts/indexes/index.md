# Indexes

Learn about indexes in Materialize.



## Overview

In Materialize, indexes represent query results stored in memory **within a
[cluster](/concepts/clusters/)**. You can create indexes on
[sources](/concepts/sources/), [views](/concepts/views/#views), or [materialized
views](/concepts/views/#materialized-views).

## Indexes on sources

> **Note:** In practice, you may find that you rarely need to index a source
> without performing some transformation using a view, etc.
>


In Materialize, you can create indexes on a [source](/concepts/sources/) to
maintain in-memory up-to-date source data within the cluster you create the
index. This can help improve [query
performance](#indexes-and-query-optimizations) when serving results directly
from the source or when [using joins](/transform-data/optimization/#join).
However, in practice, you may find that you rarely need to index a source
directly.

```mzsql
CREATE INDEX idx_on_my_source ON my_source (...);
```

## Indexes on views

In Materialize, you can create indexes on a [view](/concepts/views/#views "query
saved under a name") to maintain **up-to-date view results in memory** within
the [cluster](/concepts/clusters/) you create the index.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

During the index creation on a [view](/concepts/views/#views "query saved under
a name"), the view is executed and the view results are stored in memory within
the cluster. **As new data arrives**, the index **incrementally updates** the
view results in memory.

Within the cluster, querying an indexed view is:

- **fast** because the results are served from memory, and

- **computationally free** because no computation is performed on read.

For best practices on using indexes, and understanding when to use indexed views
vs. materialized views, see [Usage patterns](#usage-patterns).

## Indexes on materialized views

In Materialize, materialized view results are stored in durable storage and
**incrementally updated** as new data arrives. Indexing a materialized view
makes the already up-to-date view results available **in memory** within the
[cluster](/concepts/clusters/) you create the index. That is, indexes on
materialized views require no additional computation to keep results up-to-date.

> **Note:** A materialized view can be queried from any cluster whereas its indexed results
> are available only within the cluster you create the index. Querying a
> materialized view, whether indexed or not, from any cluster is computationally
> free. However, querying an indexed materialized view within the cluster where
> the index is created is faster since the results are served from memory rather
> than from storage.
>
>


For best practices on using indexes, and understanding when to use indexed views
vs. materialized views, see [Usage patterns](#usage-patterns).

```mzsql
CREATE INDEX idx_on_my_mat_view ON my_mat_view_name(...) ;
```

## Indexes and clusters

Indexes are local to a cluster. Queries in a different cluster cannot use the
indexes in another cluster.

For example, to create an index in the current cluster:

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

You can also explicitly specify the cluster:

```mzsql
CREATE INDEX idx_on_my_view IN CLUSTER active_cluster ON my_view (...);
```

## Usage patterns

### Index usage

> **Important:** Indexes are local to a cluster. Queries in one cluster cannot use the indexes in another, different cluster.
>


Unlike some other databases, Materialize can use an index to serve query results
even if the query does not specify a `WHERE` condition on the index key. Serving
queries from an index is fast since the results are already up-to-date and in
memory.

For example, consider the following index:

```mzsql
CREATE INDEX idx_orders_view_qty ON orders_view (quantity);
```

Materialize will maintain the `orders_view` in memory in `idx_orders_view_qty`,
and it will be able to use the index to serve a various queries on the
`orders_view` (and not just queries that specify conditions on
`orders_view.quantity`).

Materialize can use the index for the following queries (issued from the same
cluster as the index) on `orders_view`:

```mzsql
SELECT * FROM orders_view;  -- scans the index
SELECT * FROM orders_view WHERE status = 'shipped';  -- scans the index
SELECT * FROM orders_view WHERE quantity = 10;  -- point lookup on the index
```

For the queries that do not specify a condition on the indexed field,
Materialize scans the index. For the query that specifies an equality condition
on the indexed field, Materialize performs a **point lookup** on the index
(i.e., reads just the matching records from the index). Point lookups are the
most efficient use of an index.

#### Point lookups

Materialize performs **point lookup** (i.e., reads just the matching records
from the index) on the index if the query's `WHERE` clause:

- Specifies equality (`=` or `IN`) condition and **only** equality conditions on
  **all** the indexed fields. The equality conditions must specify the **exact**
  index key expression (including type) for point lookups. For example:

  - If the index is on `round(quantity)`, the query must specify equality
    condition on `round(quantity)` (and not just `quanity`) for Materialize to
    perform a point lookup.

  - If the index is on `quantity * price`, the query must specify equality
    condition on `quantity * price` (and not `price * quantity`) for Materialize
    to perform a point lookup.

  - If the index is on the `quantity` field which is an integer, the query must
    specify an equality condition on `quantity` with a value that is an integer.

- Only uses `AND` (conjunction) to combine conditions for **different** fields.

Point lookups are the most efficient use of an index.

For queries whose `WHERE` clause meets the point lookup criteria and includes
conditions on additional fields (also using `AND` conjunction), Materialize
performs a point lookup on the index keys and then filters the results using the
additional conditions on the non-indexed fields.

For queries that do not meet the point lookup criteria, Materialize performs a
full index scan (including for range queries). That is, Materialize performs a
full index scan if the `WHERE` clause:

- Does not specify **all** the indexed fields.
- Does not specify only equality conditions on the index fields or specifies an
  equality condition that specifies a different value type than the index key
  type.
- Uses OR (disjunction) to combine conditions for **different** fields.

Full index scans are less efficient than point lookups.  The performance of full
index scans will degrade with data volume; i.e., as you get more data, full
scans will get slower.

#### Examples

Consider again the following index on a view:

```mzsql
CREATE INDEX idx_orders_view_qty on orders_view (quantity);
```

The following table shows various queries and whether Materialize performs a
point lookup or an index scan.




<table>
<thead>
<tr>
<th>Query</th>
<th>Index usage</th>

</tr>
</thead>
<tbody>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Index scan.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Point lookup.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="k">IN</span> <span class="p">(</span><span class="mf">10</span><span class="p">,</span> <span class="mf">20</span><span class="p">);</span>
</span></span></code></pre></div></td>
<td>
Point lookup.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span> <span class="k">OR</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">20</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Point lookup. Query uses <code>OR</code> to combine conditions on the <strong>same</strong> field.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span> <span class="k">AND</span> <span class="n">price</span> <span class="o">=</span> <span class="mf">5.00</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Point lookup on <code>quantity</code>, then filter on <code>price</code>.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="p">(</span><span class="n">quantity</span><span class="p">,</span> <span class="n">price</span><span class="p">)</span> <span class="o">=</span> <span class="p">(</span><span class="mf">10</span><span class="p">,</span> <span class="mf">5.00</span><span class="p">);</span>
</span></span></code></pre></div></td>
<td>
Point lookup on <code>quantity</code>, then filter on <code>price</code>.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span> <span class="k">OR</span> <span class="n">price</span> <span class="o">=</span> <span class="mf">5.00</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Index scan. Query uses <code>OR</code> to combine conditions on <strong>different</strong> fields.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">&lt;=</span> <span class="mf">10</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Index scan.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">round</span><span class="p">(</span><span class="n">quantity</span><span class="p">)</span> <span class="o">=</span> <span class="mf">20</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Index scan.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="c1">-- Assume quantity is an integer
</span></span></span><span class="line"><span class="cl"><span class="c1"></span><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="s1">&#39;hello&#39;</span><span class="p">;</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span><span class="o">::</span><span class="nb">TEXT</span> <span class="o">=</span> <span class="s1">&#39;hello&#39;</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Index scan, assuming <code>quantity</code> field in <code>orders_view</code> is an integer.
In the first query, the quantity is implicitly cast to text.
In the second query, the quantity is explicitly cast to text.
</td>
</tr>

</tbody>
</table>



Consider that the view has an index on the `quantity` and `price` fields
instead of an index on the `quantity` field:

```mzsql
DROP INDEX idx_orders_view_qty;
CREATE INDEX idx_orders_view_qty_price on orders_view (quantity, price);
```




<table>
<thead>
<tr>
<th>Query</th>
<th>Index usage</th>

</tr>
</thead>
<tbody>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Index scan.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Index scan. Query does not include equality conditions on <strong>all</strong> indexed
fields.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span> <span class="k">AND</span> <span class="n">price</span> <span class="o">=</span> <span class="mf">2.50</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Point lookup.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span> <span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span> <span class="k">OR</span> <span class="n">price</span> <span class="o">=</span> <span class="mf">2.50</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Index scan. Query uses <code>OR</code> to combine conditions on <strong>different</strong> fields.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span> <span class="k">AND</span> <span class="p">(</span><span class="n">price</span> <span class="o">=</span> <span class="mf">2.50</span> <span class="k">OR</span> <span class="n">price</span> <span class="o">=</span> <span class="mf">3.00</span><span class="p">);</span>
</span></span></code></pre></div></td>
<td>
Point lookup. Query uses <code>OR</code> to combine conditions on <strong>same</strong> field and <code>AND</code> to combine conditions on <strong>different</strong> fields.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span> <span class="k">AND</span> <span class="n">price</span> <span class="o">=</span> <span class="mf">2.50</span> <span class="k">AND</span> <span class="n">item</span> <span class="o">=</span> <span class="s1">&#39;cupcake&#39;</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Point lookup on the index keys <code>quantity</code> and <code>price</code>, then filter on
<code>item</code>.
</td>
</tr>

<tr>
<td><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders_view</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">quantity</span> <span class="o">=</span> <span class="mf">10</span> <span class="k">AND</span> <span class="n">price</span> <span class="o">=</span> <span class="mf">2.50</span> <span class="k">OR</span> <span class="n">item</span> <span class="o">=</span> <span class="s1">&#39;cupcake&#39;</span><span class="p">;</span>
</span></span></code></pre></div></td>
<td>
Index scan. Query uses <code>OR</code> to combine conditions on <strong>different</strong> fields.
</td>
</tr>

</tbody>
</table>



#### Limitations

Indexes in Materialize do not order their keys using the data type's natural
ordering and instead orders by its internal representation of the key (the tuple
of key length and value).

As such, indexes in Materialize currently do not provide optimizations for:

- Range queries; that is queries using <code>&gt;</code>, <code>&gt;=</code>,
  <code>&lt;</code>, <code>&lt;=</code>, `BETWEEN` clauses (e.g., `WHERE
  quantity > 10`,  <code>price >= 10 AND price &lt;= 50</code>, and `WHERE quantity
  BETWEEN 10 AND 20`).

- `GROUP BY`, `ORDER BY` and `LIMIT` clauses.


### Indexes on views vs. materialized views

In Materialize, both <a href="/concepts/indexes" >indexes</a> on views and <a href="/concepts/views/#materialized-views" >materialized
views</a> incrementally update the view
results when Materialize ingests new data. Whereas materialized views persist
the view results in durable storage and can be accessed across clusters, indexes
on views compute and store view results in memory within a <strong>single</strong> cluster.
<p>Some general guidelines for usage patterns include:</p>
<table>
<thead>
<tr>
<th>Usage Pattern</th>
<th>General Guideline</th>
</tr>
</thead>
<tbody>
<tr>
<td>View results are accessed from a single cluster only;<br>such as in a 1-cluster or a 2-cluster architecture.</td>
<td>View with an <a href="/sql/create-index" >index</a></td>
</tr>
<tr>
<td>View used as a building block for stacked views; i.e., views not used to serve results.</td>
<td>View</td>
</tr>
<tr>
<td>View results are accessed across <a href="/concepts/clusters" >clusters</a>;<br>such as in a 3-cluster architecture.</td>
<td>Materialized view (in the transform cluster)<br>Index on the materialized view (in the serving cluster)</td>
</tr>
<tr>
<td>Use with a <a href="/serve-results/sink/" >sink</a> or a <a href="/sql/subscribe" ><code>SUBSCRIBE</code></a> operation</td>
<td>Materialized view</td>
</tr>
<tr>
<td>Use with <a href="/transform-data/patterns/temporal-filters/" >temporal filters</a></td>
<td>Materialized view</td>
</tr>
</tbody>
</table>
<p>For example:</p>

**3-tier architecture:**

![Image of the 3-tier-architecture
architecture](/images/3-tier-architecture.svg)

In a [3-tier
architecture](/manage/operational-guidelines/#three-tier-architecture)
where queries are served from a cluster different from the compute/transform
cluster that maintains the view results:

- Use materialized view(s) in the compute/transform cluster for the query
  results that will be served.

  If you are using <strong>stacked views</strong> (i.e., views whose definition depends
  on other views) to reduce SQL complexity, generally, only the topmost
  view (i.e., the view whose results will be served) should be a
  materialized view. The underlying views that do not serve results do not
  need to be materialized.

- Index the materialized view in the serving cluster(s) to serve the results
from memory.



**2-tier architecture:**

![Image of the 2-tier-architecture](/images/2-tier-architecture.svg)

In a [2-tier
architecture](/manage/appendix-alternative-cluster-architectures/#two-tier-architecture)
where queries are served from the same cluster that performs the
compute/transform operations:

- Use view(s) in the shared cluster.

- Index the view(s) to incrementally update the view results and serve the
results from memory.

> **Tip:** Except for when used with a [sink](/serve-results/sink/),
> [subscribe](/sql/subscribe/), or [temporal
> filters](/transform-data/patterns/temporal-filters/), avoid creating
> materialized views on a shared cluster used for both compute/transformat
> operations and serving queries. Use indexed views instead.
>




**1-tier architecture:**

![Image of the 1-tier-architecture](/images/1-tier-architecture.svg)

In a [1-tier
architecture](/manage/appendix-alternative-cluster-architectures/#one-tier-architecture)
where queries are served from the same cluster that performs the
compute/transform operations:

- Use view(s) in the shared cluster.

- Index the view(s) to incrementally update the view results and serve the
results from memory.

> **Tip:** Except for when used with a [sink](/serve-results/sink/),
> [subscribe](/sql/subscribe/), or [temporal
> filters](/transform-data/patterns/temporal-filters/), avoid creating
> materialized views on a shared cluster used for both compute/transformat
> operations and serving queries. Use indexed views instead.
>

### Indexes and query optimizations

By making up-to-date results available in memory, indexes can help [optimize
query performance](/transform-data/optimization/), such as:

- Provide faster sequential access than unindexed data.

- Provide fast random access for lookup queries (i.e., selecting individual
  keys).

<p>Specific instances where indexes can be useful to improve performance include:</p>
<ul>
<li>
<p>When used in ad-hoc queries.</p>
</li>
<li>
<p>When used by multiple queries within the same cluster.</p>
</li>
<li>
<p>When used to enable <a href="/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins" >delta
joins</a>.</p>
</li>
</ul>
<p>For more information, see <a href="/transform-data/optimization" >Optimization</a>.</p>


### Best practices

<p>Before creating an index, consider the following:</p>
<ul>
<li>
<p>If you create stacked views (i.e., views that depend on other views) to
reduce SQL complexity, we recommend that you create an index <strong>only</strong> on the
view that will serve results, taking into account the expected data access
patterns.</p>
</li>
<li>
<p>Materialize can reuse indexes across queries that concurrently access the same
data in memory, which reduces redundancy and resource utilization per query.
In particular, this means that joins do <strong>not</strong> need to store data in memory
multiple times.</p>
</li>
<li>
<p>For queries that have no supporting indexes, Materialize uses the same
mechanics used by indexes to optimize computations. However, since this
underlying work is discarded after each query run, take into account the
expected data access patterns to determine if you need to index or not.</p>
</li>
</ul>


## Related pages

- [Optimization](/transform-data/optimization)
- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)

<style>
red { color: Red; font-weight: 500; }
</style>
