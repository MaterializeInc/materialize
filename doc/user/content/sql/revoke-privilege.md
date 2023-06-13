---
title: "REVOKE PRIVILEGE"
description: "`REVOKE` revokes privileges from a database object."
menu:
  main:
    parent: commands
---

`REVOKE` revokes privileges from a database object. The `PUBLIC` pseudo-role can
be used to indicate that the privileges should be revoked from all roles
(including roles that might not exist yet).

{{< alpha />}}

## Syntax

{{< diagram "revoke-privilege.svg" >}}

### `privilege`

{{< diagram "privilege.svg" >}}

Field                                               | Use
----------------------------------------------------|--------------------------------------------------
_object_name_                                       | The object that privileges are being revoked from.
**ALL** _object_type_ **IN SCHEMA** schema_name     | The privilege will be revoked from all objects of _object_type_ in _schema_name_.
**ALL** _object_type_ **IN DATABASE** database_name | The privilege will be revoked from all objects of _object_type_ in _database_name_.
**ALL** _object_type_                               | The privilege will be revoked from all objects of _object_type_, excluding system objects.
_role_name_                                         | The role name that is losing privileges. Use the `PUBLIC` pseudo-role to revoke privileges from all roles.
**SELECT**                                          | Allows reading rows from an object. The abbreviation for this privilege is 'r' (read).
**INSERT**                                          | Allows inserting into an object. The abbreviation for this privilege is 'a' (append).
**UPDATE**                                          | Allows updating an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'w' (write).
**DELETE**                                          | Allows deleting from an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'd'.
**CREATE**                                          | Allows creating a new object within another object. The abbreviation for this privilege is 'C'.
**USAGE**                                           | Allows using an object or looking up members of an object. The abbreviation for this privilege is 'U'.
**ALL PRIVILEGES**                                  | All applicable privileges for the provided object type.

## Details

The following tables describes which privileges are applicable to which objects:

{{< note >}}
For PostgreSQL compatibility reasons, you must specify `TABLE` as the object
type for sources, views, and materialized views, or omit the object type.
{{</ note >}}

| Object type           | All privileges |
|-----------------------|----------------|
| `DATABASE`            | UC             |
| `SCHEMA`              | UC             |
| `TABLE`               | arwd           |
| (`VIEW`)              | r              |
| (`MATERIALIZED VIEW`) | r              |
| `INDEX`               |                |
| `TYPE`                | U              |
| (`SOURCE`)            | r              |
| `SINK`                |                |
| `CONNECTION`          | U              |
| `SECRET`              | U              |
| `CLUSTER`             | UC             |

Unlike PostgreSQL, `UPDATE` and `DELETE` always require `SELECT` privileges on the object being
updated.

### Compatibility

For PostgreSQL compatibility reasons, you must specify `TABLE` as the object
type for sources, views, and materialized views, or omit the object type.

## Examples

```sql
REVOKE SELECT ON mv FROM joe, mike;
```

```sql
REVOKE USAGE, CREATE ON DATABASE materialize FROM joe;
```

```sql
REVOKE ALL ON CLUSTER dev FROM joe;
```


## Related pages

- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../revoke-privilege)
