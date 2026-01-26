# COMMIT

`COMMIT` ends a transaction block and commits all changes if the transaction statements succeed.



`COMMIT` ends the current [transaction](/sql/begin/#details). Upon the `COMMIT`
statement:

- If all transaction statements succeed, all changes are committed.

- If an error occurs, all changes are discarded; i.e., rolled back.

## Syntax

```mzsql
COMMIT;
```

## Details

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


Transactions in Materialize are either **read-only** transactions or
**write-only** (more specifically, **insert-only**) transactions.

For a [write-only (i.e., insert-only)
transaction](/sql/begin/#write-only-transactions), all statements in the
transaction are committed at the same timestamp.

## Examples

### Commit a write-only transaction {#write-only-transactions}

In Materialize, write-only transactions are **insert-only** transactions.

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

### Commit a read-only transaction

In Materialize, read-only transactions can be either:

- a `SELECT` only transaction that only contains [`SELECT`] statements or

- a `SUBSCRIBE`-based transactions that only contains a single[`DECLARE ...
  CURSOR FOR`] [`SUBSCRIBE`] statement followed by subsequent
  [`FETCH`](/sql/fetch) statement(s).

For example:

```mzsql
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM flippers);

-- Subsequent queries must only FETCH from the cursor

FETCH 10 c WITH (timeout='1s');
FETCH 20 c WITH (timeout='1s');
COMMIT;
```

During the first query, a timestamp is chosen that is valid for all of the
objects referenced in the query. This timestamp will be used for all other
queries in the transaction.

> **Note:** The transaction will additionally hold back normal compaction of the objects,
> potentially increasing memory usage for very long running transactions.
>


## See also

- [`BEGIN`]
- [`ROLLBACK`]

[`BEGIN`]: /sql/begin/
[`ROLLBACK`]: /sql/rollback/
[`COMMIT`]: /sql/commit/
[`SELECT`]: /sql/select/
[`SUBSCRIBE`]: /sql/subscribe/
[`DECLARE ... CURSOR FOR`]: /sql/declare/
[`INSERT`]: /sql/insert
