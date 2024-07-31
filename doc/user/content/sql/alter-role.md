---
title: "ALTER ROLE"
description: "`ALTER ROLE` alters the attributes of an existing role."
menu:
  main:
    parent: commands
---

`ALTER ROLE` alters the attributes of an existing role.

## Syntax

{{< diagram "alter-role.svg" >}}

Field               | Use
--------------------|-------------------------------------------------------------------------
_role_name_         | A name for the role.

#### `alter_role_attributes`

{{< diagram "alter-role-attributes.svg" >}}

Field               | Use
--------------------|-------------------------------------------------------------------------
**INHERIT**         | Grants the role the ability to inherit privileges of other roles.

#### `alter_role_set`

{{< diagram "alter-role-set.svg" >}}

Field               | Use
--------------------|-------------------------------------------------------------------------
_name_              | The name of the configuration parameter to modify.
_value_             | The value to assign to the configuration parameter.
**DEFAULT**         | Reset the value of the configuration parameter for the specified role to the system's default. Equivalent to `ALTER ROLE ... RESET`.

## Details

Unlike PostgreSQL, Materialize derives the `LOGIN` and `SUPERUSER`
attributes for a role during authentication, every time that role tries
to connect to Materialize. Therefore, you cannot specify either
attribute when altering an existing role.

Unlike PostgreSQL, Materialize does not currently support the `NOINHERIT` attribute and the `SET
ROLE` command.

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `ALTER ROLE ... INHERIT INHERIT`.

Unlike PostgreSQL, Materialize does not use role attributes to determine a roles ability to create
top level objects such as databases and other roles. Instead, Materialize uses system level
privileges. See [GRANT PRIVILEGE](../grant-privilege) for more details.

Like PostgreSQL, altering the configuration parameter for a role only affects **new sessions**.
Also like PostgreSQL, role configuration parameters are **not inherited**. To view the
current configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).

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

##### Non-inheritance
```mzsql
CREATE ROLE team;
CREATE ROLE member;

ALTER ROLE team SET cluster = 'team_compute';
GRANT team TO member;

-- Start a new SQL session with the Role 'member'.
SHOW cluster;
quickstart
```

## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.

## Related pages

- [CREATE ROLE](../create-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
