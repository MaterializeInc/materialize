---
title: "DELETE"
description: "`DELETE` removes values stored in tables."
menu:
  main:
    parent: 'commands'
---

`DELETE` removes values stored in [user-created tables](../create-table).

## Syntax

{{< diagram "delete-stmt.svg" >}}

Field | Use
------|-----
**DELETE FROM** _table_name_ | The table whose values you want to remove.
_alias_ | Only permit references to _table_name_ as _alias_.
**USING** _from_item_ | Table expressions whose columns you want to reference in the `WHERE` clause. This supports the same syntax as the **FROM** clause in [`SELECT`](../select) statements, e.g. supporting aliases.
**WHERE** _condition_ | Only remove rows which evaluate to `true` for _condition_.

## Details

### Known limitations

* `DELETE` cannot be used inside [transactions](../begin).

* **Low performance.** While processing a `DELETE` statement, Materialize cannot
  process other `INSERT`, `UPDATE`, or `DELETE` statements.

## Examples

```sql
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
```sql
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
```sql
DELETE FROM delete_me;
SELECT * FROM delete_me;
```
```
 a | b
---+---
```

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`SELECT`](../select)
