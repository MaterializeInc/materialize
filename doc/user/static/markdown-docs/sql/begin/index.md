# BEGIN

`BEGIN` starts a transaction block.



<p><a href="/sql/begin/" ><code>BEGIN</code></a> starts a transaction block. Once a transaction is started:</p>
<ul>
<li>
<p>Statements within the transaction are executed sequentially.</p>
</li>
<li>
<p>A transaction ends with either a <a href="/sql/commit/" ><code>COMMIT</code></a> or a
<a href="/sql/rollback/" ><code>ROLLBACK</code></a> statement.</p>
<ul>
<li>
<p>If all transaction statements succeed and a <a href="/sql/commit/" ><code>COMMIT</code></a> is
<a href="/sql/commit/#details" >issued</a>, all changes are saved.</p>
</li>
<li>
<p>If all transaction statements succeed and a <a href="/sql/rollback/" ><code>ROLLBACK</code></a>
is issued, all changes are discarded.</p>
</li>
<li>
<p>If an error occurs and either a <a href="/sql/commit/" ><code>COMMIT</code></a> or a
<a href="/sql/rollback/" ><code>ROLLBACK</code></a> is issued, all changes are discarded.</p>
</li>
</ul>
</li>
</ul>


Materialize only supports [**read-only** transactions](#read-only-transactions)
or [**write-only** (specifically, insert-only)
transactions](#write-only-transactions). See [Details](#details) for more
information.

## Syntax

```mzsql
BEGIN [ <option>, ... ];
```

You can specify the following optional settings for `BEGIN`:

Option | Description
-------|----------
`ISOLATION LEVEL <level>` | *Optional*. If specified, sets the transaction [isolation level](/get-started/isolation-level).
`READ ONLY` | <a name="begin-option-read-only"></a> *Optional*. If specified, restricts the transaction to read-only operations. If unspecified, Materialize restricts the transaction to read-only or insert-only operations based on the first statement in the transaction.

## Details

Transactions in Materialize are either [**read-only**
transactions](#read-only-transactions) or [**write-only**
transactions](#insert-only-transactions) as determined by either:

- The first statement after the `BEGIN`, or

- The [`READ ONLY`](#begin-option-read-only) option is specified.

### Read-only transactions

In Materialize, read-only transactions can be either:

- a [`SELECT`only transaction](#select-only-transactions) that only contains
  [`SELECT`] statements or

- a [`SUBSCRIBE`-based transactions](#subscribe-based-transactions) that only
    contains a single [`DECLARE ... CURSOR FOR`] [`SUBSCRIBE`] statement
    followed by subsequent [`FETCH`](/sql/fetch) statement(s). [^1]

> **Note:** - During the first query, a timestamp is chosen that is valid for all of the
>   objects referenced in the query. This timestamp will be used for all other
>   queries in the transaction.
>
> - The transaction will additionally hold back normal compaction of the objects,
>   potentially increasing memory usage for very long running transactions.
>
>


#### SELECT-only transactions

A **SELECT-only** transaction only contains [`SELECT`](/sql/select) statement.

The first [`SELECT`](/sql/select) statement:

- Determines the timestamp that will be used for all other queries in the
  transaction.

- Determines  which objects can be queried in the transaction block.

Specifically,

- Subsequent [`SELECT`](/sql/select) statements in the transaction can only
  reference objects from the [schema(s)](/sql/namespaces/) referenced in the
  first [`SELECT`](/sql/select) statement (as well as a subset of objects from
  the `mz_catalog` and `mz_internal` schemas).

- These objects must have existed at beginning of the transaction.

For example, in the transaction block below, first `SELECT` statement in the
transaction restricts subsequent selects to objects from `test` and `public`
schemas.

```mzsql
BEGIN;
SELECT o.*,i.price,o.quantity * i.price as subtotal
FROM test.orders as o
JOIN public.items as i ON o.item = i.item;

-- Subsequent queries must only reference objects from the test and public schemas that existed at the start of the transaction.

SELECT * FROM test.auctions limit 1;
SELECT * FROM public.sales_items;
COMMIT;
```

Reading from a schema not referenced in the first statement or querying objects
created after the transaction started (even if in the allowed schema(s)) will
produce a [Same timedomain error](#same-timedomain-error).  [Same timedomain
error](#same-timedomain-error) provides a list of the allowed objects in the
transaction.

##### Same timedomain error

```none
Transactions can only reference objects in the same timedomain.
```

The first `SELECT` statement in a transaction determines which schemas the
subsequent `SELECT` statements in the transaction can query. If a subsequent
`SELECT` references an object from another schema or an object created after the
transaction started, the transaction will error with the same time domain error.

The timedomain error lists both the objects that are not in the timedomain as
well as the objects that can be referenced in the transaction (i.e., in the
timedomain).

If an object in the timedomain is a view, it will be replaced with the objects
in the view definition.

#### SUBSCRIBE-based transactions

A [`SUBSCRIBE`]-based transaction only contains a single [`DECLARE ... CURSOR
FOR`] [`SUBSCRIBE`] statement followed by subsequent [`FETCH`](/sql/fetch)
statement(s). [^1]

```mzsql
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM flippers);

-- Subsequent queries must only FETCH from the cursor

FETCH 10 c WITH (timeout='1s');
FETCH 20 c WITH (timeout='1s');
COMMIT;
```

[^1]: A [`SUBSCRIBE`-based transaction](#subscribe-based-transactions) can start
with a  [`SUBSCRIBE`] statement (or `COPY (SUBSCRIBE ...) TO STDOUT`) instead of
a `DECLARE ... FOR SUBSCRIBE` but will end with a rollback since you must cancel
the SUBSCRIBE statementin order to issue the `COMMIT`/`ROLLBACK` statement to
end the transaction block.

### Write-only transactions

In Materialize, a write-only transaction is an [INSERT-only
transaction](#insert-only-transactions) that only contains [`INSERT`]
statements.

#### INSERT-only transactions

<p>An <strong>insert-only</strong> transaction only contains <a href="/sql/insert/" ><code>INSERT</code></a>
statements that insert into the <strong>same</strong> table.</p>
<p>On a successful <a href="/sql/commit/" ><code>COMMIT</code></a>, all statements from the
transaction are committed at the same timestamp.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">BEGIN</span><span class="p">;</span>
</span></span><span class="line"><span class="cl"><span class="k">INSERT</span> <span class="k">INTO</span> <span class="n">orders</span> <span class="k">VALUES</span> <span class="p">(</span><span class="mf">11</span><span class="p">,</span><span class="n">current_timestamp</span><span class="p">,</span><span class="s1">&#39;brownie&#39;</span><span class="p">,</span><span class="mf">10</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="c1">-- Subsequent INSERTs must write to sales_items table only
</span></span></span><span class="line"><span class="cl"><span class="c1">-- Otherwise, the COMMIT will error and roll back the transaction.
</span></span></span><span class="line"><span class="cl"><span class="c1"></span>
</span></span><span class="line"><span class="cl"><span class="k">INSERT</span> <span class="k">INTO</span> <span class="n">orders</span> <span class="k">VALUES</span> <span class="p">(</span><span class="mf">11</span><span class="p">,</span><span class="n">current_timestamp</span><span class="p">,</span><span class="s1">&#39;chocolate cake&#39;</span><span class="p">,</span><span class="mf">1</span><span class="p">);</span>
</span></span><span class="line"><span class="cl"><span class="k">INSERT</span> <span class="k">INTO</span> <span class="n">orders</span> <span class="k">VALUES</span> <span class="p">(</span><span class="mf">11</span><span class="p">,</span><span class="n">current_timestamp</span><span class="p">,</span><span class="s1">&#39;chocolate chip cookie&#39;</span><span class="p">,</span><span class="mf">20</span><span class="p">);</span>
</span></span><span class="line"><span class="cl"><span class="k">COMMIT</span><span class="p">;</span>
</span></span></code></pre></div><p>If, within the transaction, a statement inserts into a table different from
that of the first statement, on <a href="/sql/commit/" ><code>COMMIT</code></a>, the transaction
encounters an <strong>internal ERROR</strong> and rolls back:</p>
<pre tabindex="0"><code class="language-none" data-lang="none">ERROR:  internal error, wrong set of locks acquired
</code></pre>

## See also

- [`COMMIT`](/sql/commit)
- [`ROLLBACK`](/sql/rollback)

[`BEGIN`]: /sql/begin/
[`ROLLBACK`]: /sql/rollback/
[`COMMIT`]: /sql/commit/
[`SELECT`]: /sql/select/
[`SUBSCRIBE`]: /sql/subscribe/
[`DECLARE ... CURSOR FOR`]: /sql/declare/
[`INSERT`]: /sql/insert/
