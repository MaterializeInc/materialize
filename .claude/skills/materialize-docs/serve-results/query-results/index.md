# `SELECT` and `SUBSCRIBE`

Querying results from Materialize using `SELECT` and `SUBSCRIBE`.



You can query results from Materialize using `SELECT` and `SUBSCRIBE` SQL
statements. Because Materialize uses the PostgreSQL wire protocol, it works
out-of-the-box with a wide range of SQL clients and tools that support
PostgreSQL.

## SELECT

You can query data in Materialize using the [`SELECT` statement](/sql/select/).
For example:

```mzsql
SELECT region.id, sum(purchase.total)
FROM mysql_simple_purchase AS purchase
JOIN mysql_simple_user AS user ON purchase.user_id = user.id
JOIN mysql_simple_region AS region ON user.region_id = region.id
GROUP BY region.id;
```

Performing a `SELECT` on an indexed view or a materialized view is
Materialize's ideal operation. When Materialize receives such a `SELECT` query,
it quickly returns the maintained results from memory.

Materialize also quickly returns results for queries that only filter, project,
transform with scalar functions, and re-order data that is maintained by an
index.

Queries that can't simply read out from an index will create an ephemeral dataflow to compute
the results. These dataflows are bound to the active [cluster](/concepts/clusters/),
 which you can change using:

```mzsql
SET cluster = <cluster name>;
```

Materialize will remove the dataflow as soon as it has returned the query
results to you.

For more information, see [`SELECT`](/sql/select/) reference page.  See
also the following client library guides:

{{< multicolumn-list columns="2" >}}
- [Go](/integrations/client-libraries/golang/#query)</li>
- [Java](/integrations/client-libraries/java-jdbc/#query)</li>
- [Node.js](/integrations/client-libraries/node-js/#query)</li>
- [PHP](/integrations/client-libraries/php/#query)</li>
- [Python](/integrations/client-libraries/python/#query)</li>
- [Ruby](/integrations/client-libraries/ruby/#query)</li>
- [Rust](/integrations/client-libraries/rust/#query)</li>
{{</ multicolumn-list >}}

## SUBSCRIBE

You can use [`SUBSCRIBE`](/sql/subscribe/) to stream query results.  For
example:

```mzsql
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM mv_counter_sum);
FETCH 10 c WITH (timeout='1s');
FETCH 20 c WITH (timeout='1s');
COMMIT;
```

The [`SUBSCRIBE`](/sql/subscribe/) statement is a more general form of a `SELECT` statement. While a `SELECT` statement computes a relation at a moment in time, a `SUBSCRIBE` operation computes how a relation changes over time.

You can use `SUBSCRIBE` to:

- Power event processors that react to every change to a relation or an
  arbitrary `SELECT` statement.

- Replicate the complete history of a relation while `SUBSCRIBE` is active.

{{< tip >}}
Use materialized view (instead of an indexed view) with `SUBSCRIBE`.
{{</ tip >}}

For more information, see [`SUBSCRIBE`](/sql/subscribe/) reference page.  See
also the following client library guides:

{{< multicolumn-list columns="2" >}}
- [Go](/integrations/client-libraries/golang/#stream)</li>
- [Java](/integrations/client-libraries/java-jdbc/#stream)</li>
- [Node.js](/integrations/client-libraries/node-js/#stream)</li>
- [PHP](/integrations/client-libraries/php/#stream)</li>
- [Python](/integrations/client-libraries/python/#stream)</li>
- [Ruby](/integrations/client-libraries/ruby/#stream)</li>
- [Rust](/integrations/client-libraries/rust/#stream)</li>
{{</ multicolumn-list >}}

