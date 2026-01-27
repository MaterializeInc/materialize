# now and mz_now functions
Details the differences between the `now()` and `mz_now()` functions.
In Materialize, `now()` returns the value of the system clock when the
transaction began as a [`timestamp with time zone`] value.

By contrast, `mz_now()` returns the logical time at which the query was executed
as a [`mz_timestamp`] value.

## Details

### `mz_now()` clause

<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="n">mz_now</span><span class="p">()</span> <span class="o">&lt;</span><span class="n">comparison_operator</span><span class="o">&gt;</span> <span class="o">&lt;</span><span class="n">numeric_expr</span> <span class="o">|</span> <span class="n">timestamp_expr</span><span class="o">&gt;</span>
</span></span></code></pre></div><ul>
<li>
<code>mz_now()</code> must be used with one of the following comparison operators: <code>=</code>,
<code>&lt;</code>, <code>&lt;=</code>, <code>&gt;</code>, <code>&gt;=</code>, or an operator that desugars to them or to a conjunction
(<code>AND</code>) of them (for example, <code>BETWEEN...AND...</code>). That is, you cannot use
date/time operations directly on  <code>mz_now()</code> to calculate a timestamp in the
past or future. Instead, rewrite the query expression to move the operation to
the other side of the comparison.
</li>
<li>
<p><code>mz_now()</code> can only be compared to either a
<a href="/sql/types/numeric" ><code>numeric</code></a> expression or a
<a href="/sql/types/timestamp" ><code>timestamp</code></a> expression not containing <code>mz_now()</code>.</p>
</li>
</ul>

### Usage patterns

The typical uses of `now()` and `mz_now()` are:

* **Temporal filters**

  You can use `mz_now()` in a `WHERE` or `HAVING` clause to limit the working dataset.
  This is referred to as a **temporal filter**.
  See the [temporal filter](/sql/patterns/temporal-filters) pattern for more details.

* **Query timestamp introspection**

  An ad hoc `SELECT` query with `now()` and `mz_now()` can be useful if you need to understand how up to date the data returned by a query is.
  The data returned by the query reflects the results as of the logical time returned by a call to `mz_now()` in that query.

### Logical timestamp selection

When using the [serializable](/get-started/isolation-level#serializable)
isolation level, the logical timestamp may be arbitrarily ahead of or behind the
system clock. For example, at a wall clock time of 9pm, Materialize may choose
to execute a serializable query as of logical time 8:30pm, perhaps because data
for 8:30–9pm has not yet arrived. In this scenario, `now()` would return 9pm,
while `mz_now()` would return 8:30pm.

When using the [strict serializable](/get-started/isolation-level#strict-serializable)
isolation level, Materialize attempts to keep the logical timestamp reasonably
close to wall clock time. In most cases, the logical timestamp of a query will
be within a few seconds of the wall clock time. For example, when executing
a strict serializable query at a wall clock time of 9pm, Materialize will choose
a logical timestamp within a few seconds of 9pm, even if data for 8:30–9pm has
not yet arrived and the query will need to block until the data for 9pm arrives.
In this scenario, both `now()` and `mz_now()` would return 9pm.

### Limitations

#### Materialization

* Queries that use `now()` cannot be materialized. In other words, you cannot
  create an index or a materialized view on a query that calls `now()`.

* Queries that use `mz_now()` can only be materialized if the call to
  `mz_now()` is used in a [temporal filter](/sql/patterns/temporal-filters).

These limitations are in place because `now()` changes every microsecond and
`mz_now()` changes every millisecond. Allowing these functions to be
materialized would be resource prohibitive.

#### `mz_now()` restrictions

The [`mz_now()`](/sql/functions/now_and_mz_now) clause has the following
restrictions:

- <p>When used in a materialized view definition, a view definition that is being
indexed (i.e., although you can create the view and perform ad-hoc query on
the view, you cannot create an index on that view), or a <code>SUBSCRIBE</code>
statement:</p>
<ul>
<li>
<p><code>mz_now()</code> clauses can only be combined using an <code>AND</code>, and</p>
</li>
<li>
<p>All top-level <code>WHERE</code> or <code>HAVING</code> conditions must be combined using an <code>AND</code>,
even if the <code>mz_now()</code> clause is nested.</p>
</li>
</ul>


  For example:


  | mz_now() Compound Clause | Valid/Invalid |
  | --- | --- |
  | <span class="copyableCode"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span> </span></span><span class="line"><span class="cl"><span class="k">OR</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span></code></pre></div></span>  | <p>✅ <strong>Valid</strong></p> <p>Ad-hoc queries do not have the same restrictions.</p>  |
  | <span class="copyableCode"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">MATERIALIZED</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;3&#39;</span> <span class="k">days</span> <span class="o">&gt;</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="k">AND</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span></code></pre></div></span>  | ✅ <strong>Valid</strong> |
  | <span class="copyableCode"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">MATERIALIZED</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="p">(</span><span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Complete&#39;</span> <span class="k">OR</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span><span class="p">)</span> </span></span><span class="line"><span class="cl"><span class="k">AND</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span></code></pre></div></span>  | ✅ <strong>Valid</strong> |
  | <div style="background-color: var(--code-block)"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">MATERIALIZED</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span> </span></span><span class="line"><span class="cl"><span class="k">OR</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">()</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span></code></pre></div></div>  | <p>❌ <strong>Invalid</strong></p> <p>In materialized view definitions, <code>mz_now()</code> clause can only be combined using an <code>AND</code>.</p>  |
  | <div style="background-color: var(--code-block)"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">MATERIALIZED</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Complete&#39;</span> </span></span><span class="line"><span class="cl"><span class="k">OR</span> <span class="p">(</span><span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span> <span class="k">AND</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">())</span> </span></span></code></pre></div></div>  | <p>❌ <strong>Invalid</strong></p> <p>In materialized view definitions with <code>mz_now()</code> clauses, top-level conditions must be combined using an <code>AND</code>.</p>  |
  | <div style="background-color: var(--code-block)"> <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">VIEW</span> <span class="n">forecast_completed_orders</span> <span class="k">AS</span> </span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">FROM</span> <span class="n">orders</span> </span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Complete&#39;</span> </span></span><span class="line"><span class="cl"><span class="k">OR</span> <span class="p">(</span><span class="n">status</span> <span class="o">=</span> <span class="s1">&#39;Shipped&#39;</span> <span class="k">AND</span> <span class="n">order_date</span> <span class="o">+</span> <span class="nb">interval</span> <span class="s1">&#39;1&#39;</span> <span class="k">days</span> <span class="o">&lt;=</span> <span class="n">mz_now</span><span class="p">())</span> </span></span><span class="line"><span class="cl"><span class="p">;</span> </span></span><span class="line"><span class="cl"> </span></span><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">INDEX</span> <span class="n">idx_forecast_completed_orders</span> <span class="k">ON</span> <span class="n">forecast_completed_orders</span> </span></span><span class="line"><span class="cl"><span class="p">(</span><span class="n">order_date</span><span class="p">);</span> <span class="c1">-- Unsupported because of the `mz_now()` clause </span></span></span></code></pre></div></div>  | <p>❌ <strong>Invalid</strong></p> <p>To index a view whose definitions includes <code>mz_now()</code> clauses, top-level conditions must be combined using an <code>AND</code> in the view definition.</p>  |


  For alternatives, see [Disjunction (OR)
  alternatives](http://localhost:1313/docs/transform-data/idiomatic-materialize-sql/mz_now/#disjunctions-or).

- If part of a  `WHERE` clause, the `WHERE` clause cannot be an [aggregate
 `FILTER` expression](/sql/functions/filters).

## Examples

### Temporal filters

<!-- This example also appears in temporal-filters -->
It is common for real-time applications to be concerned with only a recent period of time.
In this case, we will filter a table to only include records from the last 30 seconds.

```mzsql
-- Create a table of timestamped events.
CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
);

-- Create a view of events from the last 30 seconds.
CREATE VIEW last_30_sec AS
SELECT event_ts, content
FROM events
WHERE mz_now() <= event_ts + INTERVAL '30s';
```

Next, subscribe to the results of the view.

```mzsql
COPY (SUBSCRIBE (SELECT event_ts, content FROM last_30_sec)) TO STDOUT;
```

In a separate session, insert a record.

```mzsql
INSERT INTO events VALUES (
    'hello',
    now()
);
```

Back in the first session, watch the record expire after 30 seconds. Press `Ctrl+C` to quit the `SUBSCRIBE` when you are ready.

```nofmt
1686868190714   1       2023-06-15 22:29:50.711 hello
1686868220712   -1      2023-06-15 22:29:50.711 hello
```

You can materialize the `last_30_sec` view by creating an index on it (results stored in memory) or by recreating it as a `MATERIALIZED VIEW` (results persisted to storage). When you do so, Materialize will keep the results up to date with records expiring automatically according to the temporal filter.

### Query timestamp introspection

If you haven't already done so in the previous example, create a table called `events` and add a few records.

```mzsql
-- Create a table of timestamped events.
CREATE TABLE events (
    content TEXT,
    event_ts TIMESTAMP
);
-- Insert records
INSERT INTO events VALUES (
    'hello',
    now()
);
INSERT INTO events VALUES (
    'welcome',
    now()
);
INSERT INTO events VALUES (
    'goodbye',
    now()
);
```

Execute this ad hoc query that adds the current system timestamp and current logical timestamp to the events in the `events` table.

```mzsql
SELECT now(), mz_now(), * FROM events
```

```nofmt
            now            |    mz_now     | content |       event_ts
---------------------------+---------------+---------+-------------------------
 2023-06-15 22:38:14.18+00 | 1686868693480 | hello   | 2023-06-15 22:29:50.711
 2023-06-15 22:38:14.18+00 | 1686868693480 | goodbye | 2023-06-15 22:29:51.233
 2023-06-15 22:38:14.18+00 | 1686868693480 | welcome | 2023-06-15 22:29:50.874
(3 rows)
```

Notice when you try to materialize this query, you get errors:

```mzsql
CREATE MATERIALIZED VIEW cant_materialize
    AS SELECT now(), mz_now(), * FROM events;
```

```nofmt
ERROR:  cannot materialize call to current_timestamp
ERROR:  cannot materialize call to mz_now
```

[`mz_timestamp`]: /sql/types/mz_timestamp
[`timestamp with time zone`]: /sql/types/timestamptz
