---
title: "DROP DATABASE"
description: "`DROP DATABASE` removes a database from your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`DROP DATABASE` removes a database from your Materialize instances.

{{< warning >}} `DROP DATABASE` immediately removes all objects within the
database without confirmation. Use with care! {{< /warning >}}

## Syntax

{{< diagram "drop-database.html" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified database does not exist.
_database&lowbar;name_ | The database you want to drop. For available databases, see [`SHOW DATABASES`](../show-databases).

## Example

```sql
SHOW DATABASES;
```
