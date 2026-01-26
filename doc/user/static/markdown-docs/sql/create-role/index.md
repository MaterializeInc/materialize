# CREATE ROLE

`CREATE ROLE` creates a new role.



`CREATE ROLE` creates a new role, which is a user account in Materialize.[^1]

When you connect to Materialize, you must specify the name of a valid role in
the system.

[^1]: Materialize does not support the `CREATE USER` command.

## Syntax



**Cloud:**

### Cloud

The following syntax is used to create a role in Materialize Cloud.


```mzsql
CREATE ROLE <role_name> [[WITH] INHERIT];

```

| Syntax element | Description |
| --- | --- |
| `INHERIT` | *Optional.* If specified, grants the role the ability to inherit privileges of other roles. *Default.*  |


**Note:**
- Materialize Cloud does not support the `NOINHERIT` option for `CREATE
ROLE`.
- Materialize Cloud does not support the `LOGIN` and `SUPERUSER` attributes
  for `CREATE ROLE`.  See [Organization
  roles](/security/cloud/users-service-accounts/#organization-roles)
  instead.
- Materialize Cloud does not use role attributes to determine a role's
ability to create top level objects such as databases and other roles.
Instead, Materialize uses system level privileges. See [GRANT
PRIVILEGE](../grant-privilege) for more details.


**Self-Managed:**
### Self-Managed

The following syntax is used to create a role in Materialize Self-Managed.


```mzsql
CREATE ROLE <role_name>
[WITH]
    [ SUPERUSER | NOSUPERUSER ],
    [ LOGIN | NOLOGIN ]
    [ INHERIT ]
    [ PASSWORD <text> ]
;

```

| Syntax element | Description |
| --- | --- |
| `INHERIT` | *Optional.* If specified, grants the role the ability to inherit privileges of other roles. *Default.*  |
| `LOGIN` | *Optional.* If specified, allows a role to login via the PostgreSQL or web endpoints  |
| `NOLOGIN` | *Optional.* If specified, prevents a role from logging in. This is the default behavior if `LOGIN` is not specified.  |
| `SUPERUSER` | *Optional.* If specified, grants the role superuser privileges.  |
| `NOSUPERUSER` | *Optional.* If specified, prevents the role from having superuser privileges. This is the default behavior if `SUPERUSER` is not specified.  |
| `PASSWORD` | ***Public Preview***  *Optional.* This feature may have minor stability issues. If specified, allows you to set a password for the role.  |


**Note:**
- Self-Managed Materialize does not support the `NOINHERIT` option for
`CREATE ROLE`.
- With the exception of the `SUPERUSER` attribute, Self-Managed Materialize
does not use role attributes to determine a role's ability to create top
level objects such as databases and other roles. Instead, Self-Managed
Materialize uses system level privileges. See [GRANT PRIVILEGE](../grant-privilege) for more details.




## Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... INHERIT INHERIT`.

## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>CREATEROLE</code> privileges on the system.</li>
</ul>


## Examples

### Create a functional role

In Materialize Cloud and Self-Managed, you can create a functional role:

```mzsql
CREATE ROLE db_reader;
```

### Create a role with login and password (Self-Managed)

```mzsql
CREATE ROLE db_reader WITH LOGIN PASSWORD 'password';
```

You can verify that the role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
```

### Create a superuser role (Self-Managed)

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
CREATE ROLE super_user WITH SUPERUSER LOGIN PASSWORD 'password';
```

You can verify that the superuser role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
 super_user
```

You can also verify that the role has superuser privileges by checking the `pg_authid` system catalog:

```mzsql
SELECT rolsuper FROM pg_authid WHERE rolname = 'super_user';
```

```nofmt
 true
```



## Related pages

- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
