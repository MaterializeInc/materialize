---
title: "UPDATE"
description: "`UPDATE` changes values stored in tables."
menu:
  main:
    parent: commands
---

`UPDATE` changes values stored in [user-created tables](../create-table).

## Syntax

{{< diagram "update-stmt.svg" >}}

Field | Use
------|-----
**UPDATE** _table_name_ | The table whose values you want to update.
_alias_ | Only permit references to _table_name_ as _alias_.
**SET** _col_ref_ **=** _expr_ | Assign the value of `expr` to `col_ref`.
**WHERE** _condition_ | Only update rows which evaluate to `true` for _condition_.

## Details

### Known limitations

* `UPDATE` cannot be used inside [transactions](../begin).
* `UPDATE` can reference [user-created tables](../create-table) but not [sources](../create-source).
* **Low performance.** While processing an `UPDATE` statement, Materialize cannot
  process other `INSERT`, `UPDATE`, or `DELETE` statements.

## Examples

```sql
CREATE TABLE update_me (a int, b text);
INSERT INTO update_me VALUES (1, 'hello'), (2, 'goodbye');
UPDATE update_me SET a = a + 2 WHERE b = 'hello';
SELECT * FROM update_me;
```
```
 a |    b
---+---------
 3 | hello
 2 | goodbye
```
```sql
UPDATE update_me SET b = 'aloha';
SELECT * FROM update_me;
```
```
 a |   b
---+-------
 2 | aloha
 3 | aloha
```

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the query are contained in.
- `UPDATE` privileges on `table_name`.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
    execute the view definition.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`SELECT`](../select)
