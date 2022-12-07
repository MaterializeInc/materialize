---
title: "BEGIN"
menu:
  main:
    parent: "commands"
---

`BEGIN` starts a transaction block.

## Syntax

{{< diagram "begin.svg" >}}

Supported `transaction_mode` option values:

Value | Description
------|----------
`ISOLATION LEVEL <level>` | Sets the transaction [isolation level](/overview/isolation-level).
`READ ONLY` | Limits the transaction to read-only operations.

## Details

`BEGIN` starts a transaction block.
All statements in a transaction block will be executed in a single transaction until an explicit [`COMMIT`](/sql/commit) or [`ROLLBACK`](/sql/rollback) is given.

Transactions in Materialize do not support interleaving arbitrary kinds of statements, but instead are either **read only** or **write only**, determined by the first statement after the `BEGIN`.

### Read-only transactions

A **read-only** transaction starts with a [`SELECT`](/sql/select) statement and allows only `SELECT` statements.
Because Materialize does not know which objects (sources, tables, or views) will be queried during the transaction, the objects in the first `SELECT` and any other object in the same schemas are assumed to be possible query targets.
Other queries can only reference these same-schema objects.
During the first query, a timestamp is chosen that is valid for all of those objects.
This timestamp will be used for all other queries.
The transaction will additionally hold back normal compaction of the objects, potentially increasing memory usage for very long running transactions.

A second kind of **read-only** transaction can contain an initial [`SUBSCRIBE`](/sql/subscribe), which can appear in a transaction block along with [`DECLARE`](/sql/declare) and [`FETCH`](/sql/fetch).

### Write-only transactions

A **write-only** transaction starts with an [`INSERT`](/sql/insert) and allows only `INSERT` statements.
Different statements can not reference different tables.
On `COMMIT`, all statements from the transaction are committed at the same timestamp.

### Same timedomain error

A **read-only** transaction can produce an error with the text:

> Transactions can only reference objects in the same timedomain.

The first `SELECT` in a transaction assumes that any object in that `SELECT` and any other object in the same schemas are assumed to be possible query targets.
If a later `SELECT` references another object, the transaction will fail.
This can happen if the object is in a schema not referenced by the first `SELECT`.
It can also happen if a new object (table, view, source, or index) was created after the transaction started, even if the new object is in the same schemas as the first `SELECT`.
