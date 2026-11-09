---
title: "DELETE"
description: "`DELETE` removes values stored in tables."
menu:
  main:
    parent: 'commands'
---

`DELETE` removes values stored in [user-created tables](../create-table).

## Syntax

```mzsql
DELETE FROM <table_name> [AS <alias>]
[USING <from_item> [, ...]]
[WHERE <condition>]
;
```

Syntax element | Description
---------------|------------
`<table_name>` | The table whose values you want to remove.
**AS** `<alias>` | Optional. The alias for the table. If specified, only permit references to `<table_name>` as `<alias>`.
**USING** _from_item_ | Optional. Table expressions whose columns you want to reference in the `WHERE` clause. This supports the same syntax as the **FROM** clause in [`SELECT`](../select) statements, e.g. supporting aliases.
**WHERE** `<condition>` | Optional. Only remove rows which evaluate to `true` for _condition_.

## Details

### Known limitations

* `DELETE` cannot be used inside [transactions](../begin).
* `DELETE` can reference [read-write tables](../create-table) but not
  [sources](../create-source) or read-only tables.
* **Concurrency.** `DELETE` does not take a table-wide write lock. `DELETE`,
  `UPDATE`, and `INSERT ... SELECT` run under optimistic concurrency control: a
  bounded number run concurrently, so a long-running one can delay other such
  statements (including on unrelated tables), and a statement that conflicts with
  a concurrent write to the same table is retried automatically. Plain
  `INSERT ... VALUES` is not blocked.
* **Large deletes.** A single `DELETE` that matches a very large number of rows
  can exceed `max_result_size` or `statement_timeout`. See
  [Large `DELETE` statements](#large-delete-statements).

### Large `DELETE` statements

Materialize processes a `DELETE` by reading every row the statement matches back
through the cluster so it can retract it. The work therefore scales with the
number of rows matched, not just the size of the table or the complexity of the
`WHERE` clause.

A `DELETE` is bound by [`statement_timeout`](../set) (default 60 seconds): a
statement that cannot finish within that window is cancelled, and a very large
read-back can also disconnect the cluster replica before it completes. There is
no fixed row or byte limit; how many rows you can delete in a single statement
depends on how wide those rows are and the size of the cluster. A
`DELETE ... RETURNING` is additionally subject to the `max_result_size`
configuration parameter (default 1 GiB) on the rows it returns.

To tell whether a `DELETE` is at risk before running it, time the matching
`SELECT`: a `DELETE` does roughly the same work as selecting the rows it matches.
If `SELECT * FROM <table> WHERE <condition>` does not return comfortably within
`statement_timeout`, a `DELETE` with the same `WHERE` clause will not complete
either. A `DELETE` that exceeds the timeout is cancelled and removes no rows (the
statement is atomic), so it is safe to retry as smaller batches.

To delete more than fits in one statement, split it into smaller statements that
each match fewer rows. Use a predicate on an [indexed](../create-index) column so
each batch is cheap to locate, for example by key range:

```mzsql
-- Instead of one large delete:
DELETE FROM events WHERE ts < '2026-01-01';

-- Delete in smaller batches by key range:
DELETE FROM events WHERE ts < '2026-01-01' AND id BETWEEN 0 AND 99999;
DELETE FROM events WHERE ts < '2026-01-01' AND id BETWEEN 100000 AND 199999;
-- ...continue until all matching rows are removed.
```

Raising `statement_timeout` or increasing the cluster size can help a moderately
large `DELETE` complete, but a very large read-back can still fail, so splitting
the statement is the reliable approach. The same guidance applies to large
[`UPDATE`](../update) statements, which use the same mechanism.

## Examples

```mzsql
CREATE TABLE delete_me (a int, b text);
INSERT INTO delete_me
    VALUES
    (1, 'hello'),
    (2, 'goodbye'),
    (3, 'ok');
DELETE FROM delete_me WHERE b = 'hello';
SELECT * FROM delete_me ORDER BY a;
```
```
 a |    b
---+---------
 2 | goodbye
 3 | ok
```
```mzsql
CREATE TABLE delete_using (b text);
INSERT INTO delete_using VALUES ('goodbye'), ('ciao');
DELETE FROM delete_me
    USING delete_using
    WHERE delete_me.b = delete_using.b;
SELECT * FROM delete_me;
```
```
 a | b
---+----
 3 | ok
```
```mzsql
DELETE FROM delete_me;
SELECT * FROM delete_me;
```
```
 a | b
---+---
```

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/delete" %}}

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`SELECT`](../select)
