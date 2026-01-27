# GRANT ROLE
`GRANT` grants membership of one role to another role.
`GRANT` grants membership of one role to another role. Roles can be members of
other roles, as well as inherit all the privileges of those roles.

## Syntax



```mzsql
GRANT <role_name> [, ...] TO <grantee> [, ...]

```

| Syntax element | Description |
| --- | --- |
| `<role_name>` | The name of the role being granted.  |
| `<grantee>` | The name of the receiving role; i.e., the grantee.  |


## Examples

```mzsql
GRANT data_scientist TO joe;
```

```mzsql
GRANT data_scientist TO joe, mike;
```

## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.

## Useful views

- [`mz_internal.mz_show_role_members`](/sql/system-catalog/mz_internal/#mz_show_role_members)
- [`mz_internal.mz_show_my_role_members`](/sql/system-catalog/mz_internal/#mz_show_my_role_members)

## Related pages

- [`SHOW ROLE MEMBERSHIP`](../show-role-membership)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
