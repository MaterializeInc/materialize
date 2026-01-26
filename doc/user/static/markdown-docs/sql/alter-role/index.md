# ALTER ROLE

`ALTER ROLE` alters the attributes of an existing role.



`ALTER ROLE` alters the attributes of an existing role.[^1]

[^1]: Materialize does not support the `SET ROLE` command.

## Syntax




**Cloud:**

### Cloud

The following syntax is used to alter a role in Materialize Cloud.


```mzsql
ALTER ROLE <role_name>
[[WITH] INHERIT]
[SET <config> =|TO <value|DEFAULT> ]
[RESET <config>];

```

| Syntax element | Description |
| --- | --- |
| `INHERIT` | *Optional.* If specified, grants the role the ability to inherit privileges of other roles. *Default.*  |
| `SET <name> TO <value\|DEFAULT>` | *Optional.* If specified, sets the configuration parameter for the role to the `<value>` or if the value specified is `DEFAULT`, the system's default (equivalent to `ALTER ROLE ... RESET <name>`).  To view the configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).  {{< note >}}  - Altering the configuration parameter for a role only affects **new sessions**. - Role configuration parameters are **not inherited**.  {{< /note >}}  |
| `RESET <name>` | *Optional.* If specified, resets the configuration parameter for the role to the system's default.  To view the configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).  {{< note >}}  - Altering the configuration parameter for a role only affects **new sessions**. - Role configuration parameters are **not inherited**.  {{< /note >}}  |


**Note:**
- Materialize Cloud does not support the `NOINHERIT` option for `ALTER
ROLE`.
- Materialize Cloud does not support the `LOGIN` and `SUPERUSER` attributes
  for `ALTER ROLE`.  See [Organization
  roles](/security/cloud/users-service-accounts/#organization-roles)
  instead.
- Materialize Cloud does not use role attributes to determine a role's
ability to alter top level objects such as databases and other roles.
Instead, Materialize Cloud uses system level privileges. See [GRANT
PRIVILEGE](../grant-privilege) for more details.


**Self-Managed:**
### Self-Managed

The following syntax is used to alter a role in Materialize Self-Managed.


```mzsql
ALTER ROLE <role_name>
[WITH]
  [ SUPERUSER | NOSUPERUSER ]
  [ LOGIN | NOLOGIN ]
  [ INHERIT | NOINHERIT ]
  [ PASSWORD <text> ]]
[SET <name> TO <value|DEFAULT> ]
[RESET <name>]
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
| `SET <name> TO <value\|DEFAULT>` | *Optional.* If specified, sets the configuration parameter for the role to the `<value>` or if the value specified is `DEFAULT`, the system's default (equivalent to `ALTER ROLE ... RESET <name>`).  To view the configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).  {{< note >}}  - Altering the configuration parameter for a role only affects **new sessions**. - Role configuration parameters are **not inherited**.  {{< /note >}}  |
| `RESET <name>` | *Optional.* If specified, resets the configuration parameter for the role to the system's default.  To view the configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).  {{< note >}}  - Altering the configuration parameter for a role only affects **new sessions**. - Role configuration parameters are **not inherited**.  {{< /note >}}  |


**Note:**
- Self-Managed Materialize does not support the `NOINHERIT` option for
`ALTER ROLE`.
- With the exception of the `SUPERUSER` attribute, Self-Managed Materialize
does not use role attributes to determine a role's ability to create top
level objects such as databases and other roles. Instead, Self-Managed
Materialize uses system level privileges. See [GRANT
PRIVILEGE](../grant-privilege) for more details.




## Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `ALTER ROLE ... INHERIT INHERIT`.

## Examples

#### Altering the attributes of a role

```mzsql
ALTER ROLE rj INHERIT;
```
```mzsql
SELECT name, inherit FROM mz_roles WHERE name = 'rj';
```
```nofmt
rj  true
```

#### Setting configuration parameters for a role

```mzsql
SHOW cluster;
quickstart

ALTER ROLE rj SET cluster TO rj_compute;

-- Role parameters only take effect for new sessions.
SHOW cluster;
quickstart

-- Start a new SQL session with the role 'rj'.
SHOW cluster;
rj_compute

-- In a new SQL session with a role that is not 'rj'.
SHOW cluster;
quickstart
```


#### Making a role a superuser  (Self-Managed)

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
ALTER ROLE rj SUPERUSER;
```

To verify that the role has superuser privileges, you can query the `pg_authid` system catalog:

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```

```nofmt
rj  t
```

#### Removing the superuser attribute from a role (Self-Managed)

NOSUPERUSER will remove the superuser attribute from a role, preventing it from having unrestricted access to all objects in the system.

```mzsql
ALTER ROLE rj NOSUPERUSER;
```

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```

```nofmt
rj  f
```

#### Removing a role's password (Self-Managed)

> **Warning:** Setting a NULL password removes the password.
>


```mzsql
ALTER ROLE rj PASSWORD NULL;
```

#### Changing a role's password (Self-Managed)

```mzsql
ALTER ROLE rj PASSWORD 'new_password';
```
## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>CREATEROLE</code> privileges on the system.</li>
</ul>


## Related pages

- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
