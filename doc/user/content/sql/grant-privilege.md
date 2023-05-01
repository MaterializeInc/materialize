---
title: "GRANT PRIVILEGE"
description: "`GRANT` grants privileges on a database object."
menu:
  main:
    parent: commands
---

`GRANT` grants privileges on a database object. The `PUBLIC` pseudo-role can
be used to indicate that the privileges should be granted to all roles
(including roles that might not exist yet).

Privileges are cumulative: revoking a privilege from `PUBLIC` does not mean all
roles have lost that privilege, if certain roles were explicitly granted that
privilege.

{{< warning >}}
Currently, privileges have limited functionality in Materialize. This is part of the
work to enable **Role-based access control** (RBAC) in a future release {{% gh 11579 %}}.
{{< /warning >}}


## Syntax

{{< diagram "grant-privilege.svg" >}}

### `privilege`

{{< diagram "privilege.svg" >}}

Field         | Use
--------------|--------------------------------------------------
_object_name_ | The object that privileges are being granted on.
_role_name_   | The role name that is gaining privileges. Use the `PUBLIC` pseudo-role to grant privileges to all roles.
**SELECT**    | Allows reading rows from an object. The abbreviation for this privilege is 'r' (read).
**INSERT**    | Allows inserting into an object. The abbreviation for this privilege is 'a' (append).
**UPDATE**    | Allows updating an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'w' (write).
**DELETE**    | Allows deleting from an object (requires **SELECT** if a read is necessary). The abbreviation for this privilege is 'd'.
**CREATE**    | Allows creating a new object within another object. The abbreviation for this privilege is 'C'.
**USAGE**     | Allows using an object or looking up members of an object. The abbreviation for this privilege is 'U'.

## Details

The following table describes which privileges are applicable to which objects:

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

### Compatibility

For PostgreSQL compatibility reasons, you must specify `TABLE` as the object
type for sources, views, and materialized views, or omit the object type.

## Examples

```sql
GRANT SELECT ON mv TO joe;
```

```sql
GRANT USAGE, CREATE ON DATABASE materialize TO joe;
```

## Related pages

- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [REVOKE PRIVILEGE](../revoke-privilege)
