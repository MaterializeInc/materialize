---
title: "UPDATE"
description: "`UPDATE` changes values stored in tables."
menu:
  main:
    parent: 'sql'
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

`UPDATE` cannot currently...
- Be used inside [transactions](../begin)
- Reference other tables

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

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`SELECT`](../select)
