---
title: "REVOKE PRIVILEGE"
description: "`REVOK` revokes privileges on a database object."
menu:
  main:
    parent: commands
---

`REVOKE` revokes privileges on a database object.

{{< warning >}}
Currently, roles have limited functionality in Materialize. This is part of the
work to enable **Role-based access control** (RBAC) in a future release {{% gh 11579 %}}.
{{< /warning >}}


## Syntax

{{< diagram "revoke-privilege.svg" >}}

### `privilege`

{{< diagram "privilege.svg" >}}

Field         | Use
--------------|--------------------------------------------------
_object_name_ | The object that privileges are being granted on.
_role_name_   | The role name that is gaining privileges.
**SELECT**    | Allows reading rows from an object. The abbreviation for this privilege is 'r' (read).
**INSERT**    | Allows inserting into an object. The abbreviation for this privilege is 'a' (append).
**UPDATE**    | Allows updating an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'w' (write).
**DELETE**    | Allows deleting from an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'd'.
**CREATE**    | Allows creating a new object within another object. The abbreviation for this privilege is 'C'.
**USAGE**     | Allows using an object or looking up members of an object. The abbreviation for this privilege is 'U'.

## Details

For Views, Materialized Views, and Sources, the object type of 'TABLE' must be specified. This is
for PostgreSQL compatibility.

The following tables describes which privileges are applicable on which objects:

| Object Type          | All Privileges |
|----------------------|----------------|
| `DATABASE`           | UC             |
| `SCHEMA`             | UC             |
| `TABLE`              | arwd           |
| `VIEW`               | r              |
| `MATERIALIZED  VIEW` | r              |
| `INDEX`              |                |
| `TYPE`               | U              |
| `SOURCE`             | r              |
| `SINK`               |                |
| `CONNECTION`         | U              |
| `SECRET`             | U              |
| `CLUSTER`            | UC             |

The keyword `PUBLIC` can be used as the _role_name_. This pseudo-role indicates that these
privileges are being revoked from all roles, even those that don't exist yet.

## Examples

```sql
REVOKE SELECT ON TABLE t FROM joe;
```

```sql
REVOKE USAGE, CREATE ON DATABASE materialize TO joe;
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
