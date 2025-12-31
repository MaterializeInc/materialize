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

{{% txns/txn-details %}}

Transactions in Materialize are either **read-only** transactions or
**write-only** (more specifically, **insert-only**) transactions.

For a [write-only (i.e., insert-only)
transaction](/sql/begin/#write-only-transactions), all statements in the
transaction are committed at the same timestamp.

## Examples

### Commit a write-only transaction {#write-only-transactions}

In Materialize, write-only transactions are **insert-only** transactions.

{{% txns/txn-insert-only %}}

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

{{< note >}}
The transaction will additionally hold back normal compaction of the objects,
potentially increasing memory usage for very long running transactions.
{{</ note >}}

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

