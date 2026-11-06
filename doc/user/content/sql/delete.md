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
* **Low performance.** While processing a `DELETE` statement, Materialize cannot
  process other `INSERT`, `UPDATE`, or `DELETE` statements.
* **Large deletes.** A single `DELETE` that matches a very large number of rows
  can fail with a timeout or replica-disconnect error. See
  [Large `DELETE` statements](#large-delete-statements).

### Large `DELETE` statements

Materialize processes a `DELETE` by reading every row the statement matches back
through the cluster so it can retract it. The work therefore scales with the
number of rows matched, not just the size of the table or the complexity of the
`WHERE` clause.

A single `DELETE` that matches a very large number of rows can fail (for example,
with a timeout or a `replica ... disconnected` error) when reading the matched
rows back takes longer than the cluster can complete in one statement. There is
no fixed row or byte limit: the threshold depends on how many rows the statement
matches, how wide those rows are, and the size of the cluster.

If a large `DELETE` fails this way, split it into smaller statements that each
match fewer rows. Use a predicate on an [indexed](../create-index) column so each
batch is cheap to locate, for example by key range:

```mzsql
-- Instead of one large delete:
DELETE FROM events WHERE ts < '2026-01-01';

-- Delete in smaller batches by key range:
DELETE FROM events WHERE ts < '2026-01-01' AND id BETWEEN 0 AND 99999;
DELETE FROM events WHERE ts < '2026-01-01' AND id BETWEEN 100000 AND 199999;
-- ...continue until all matching rows are removed.
```

Increasing the size of the cluster can raise the threshold, but splitting the
statement is the reliable approach. The same guidance applies to large
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
