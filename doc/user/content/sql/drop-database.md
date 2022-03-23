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

```sql
DROP DATABASE [ IF EXISTS ] database_name [ CASCADE | RESTRICT ]
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "drop-database.svg" >}}

</details>
<br/>

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified database does not exist.
_database&lowbar;name_ | The database you want to drop. For available databases, see [`SHOW DATABASES`](../show-databases).
**CASCADE** | Remove the database and its dependent objects. _(Default)_
**RESTRICT** | Do not remove this database if it contains any schemas.

## Example

### Remove a database containing schemas
You can use either of the following commands:

- ```sql
  DROP DATABASE my_db;
  ```
- ```sql
  DROP DATABASE my_db CASCADE;
  ```

### Remove a database only if it contains no schemas
```sql
DROP DATABASE my_db RESTRICT;
```

### Do not issue an error if attempting to remove a nonexistent database
```sql
DROP DATABASE IF EXISTS my_db;
```
