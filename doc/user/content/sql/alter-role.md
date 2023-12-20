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
**INHERIT**         | Grants the role the ability to inheritance of privileges of other roles.

#### `alter_role_variables`

{{< public-preview />}}

{{< diagram "alter-role-variables.svg" >}}

Field               | Use
--------------------|-------------------------------------------------------------------------
_variable_name_     | The name of the session variable to modify.
_variable_value_    | The value to assign to the session variable.
**DEFAULT**         | Reset the Role's value for this variable, to the system's default. Equivalent to `ALTER ROLE ... RESET`.

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

When RBAC is enabled a role must have the `CREATEROLE` system privilege to alter another role.

Like PostgreSQL, when altering the variable for a Role only _new_ sessions will observe the default
value. Also like PostgreSQL, Role variable defaults _do not get inherited_.

## Examples

#### Altering a Role's Attributes

```sql
ALTER ROLE rj INHERIT;
```
```sql
SELECT name, inherit FROM mz_roles WHERE name = 'rj';
```
```nofmt
rj  true
```

#### Altering a Role's Variables

```sql
SHOW cluster;
quickstart

ALTER ROLE rj SET cluster TO rj_compute;

-- Role variables only take effect for new sessions.
SHOW cluster;
quickstart

-- Start a new SQL session with the Role 'rj'.
SHOW cluster;
rj_compute

-- In a new SQL session with a Role that is not 'rj'.
SHOW cluster;
quickstart
```

##### Non-inheritence
```sql
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
