# System Privileges

- Associated: [RBAC](20230216_role_based_access_control.md)

## Context

Role based access control comprises a set of features that allow customers to control the
privileges of individual users. Object privileges control user's access to database objects, they
are granted with the `GRANT` keyword and revoked with the `REVOKE` keyword. For example,
`GRANT SELECT ON TABLE t TO r` will grant the ability to read table `t` to role `r`. `GRANT CREATE
ON SCHEMA s TO r` will grant the ability to create new objects in schema `s` to role `r`. Object
privileges can be inherited through role membership. Role attributes control the user's ability to
create (and sometimes modify) top level objects, they are granted and revoked with the `ALTER ROLE`
keywords. For example, `ALTER ROLE r CREATEDB` will grant the ability to create databases to
role `r`. `ALTER ROLE r CREATECLUSTER` will grant the ability to create clusters to role `r`. Role
attributes cannot be inherited through role membership.

Role attributes always exist in a boolean state. To change the state of a role attribute you must
alter the role. For example `ALTER ROLE joe CREATEDB` sets the `CREATEDB` attribute to `true`,
and `ALTER ROLE NOCREATEDB` sets the `CREATEDB` attribute to `false`.

On the other hand, privileges either exist or they don't exist. You create them by granting a
privilege via `GRANT ...` and you destroy them via `REVOKE ...`.

Role attributes present some problems as they currently exist:

- It can be confusing that there are two separate privilege primitives that control a role's
  privilege, and they have different semantics.
- Role attributes are not inherited which makes it cumbersome to grant attributes to a large
  number of roles.
- Users are not able to easily grant the ability to create top level objects to many current and
  future users in a short amount of time.

## Goals

- Reduce confusion around privileges.
- Allow the ability to create databases, clusters, and roles to be inherited through role
  membership.
- Allow users to easily grant the ability to create top level objects to many current and future
  users in a short amount of time.

## Overview

We will add a new type of privilege called System Privileges. They will look and act just like
normal object privileges, but they will grant the abilities that would otherwise be granted through
role attributes. These will replace role attributes.

NOTE: Other databases, such as CockroachDB and Snowflake, have some form of system privileges that
control similar privileges to role attributes.

## Detailed description

System privileges will be granted with the following syntax:
`GRANT <privilege-specification> ON SYSTEM TO <role-specification>`

System privileges will be revoked with the following syntax:
`REVOKE <privilege-specification> ON SYSTEM FROM <role-specification>`

We will support the following system privileges:

- `CREATEDB`: Allows users to create databases.
- `CREATECLUSTER`: Allows users to create clusters.
- `CREATEROLE`: Allows users to create, modify, and delete roles.

System privileges are inherited through role membership.

System privileges will be exposed through a system table called `mz_system_privileges`. It will have
a single column of type `mz_aclitem` that will store all the privileges.

Role attributes can be added back in at a later point if we determine that they are necessary.

### Observability

We will add the following function:
`has_system_privilege([role: text or oid, ]privileges: text) -> bool`
to indicate whether a role has a system privilege. The semantics of this function will match our
existing
[access privilege inquiry functions](https://materialize.com/docs/sql/functions/#access-privilege-inquiry-func).

## Future Work

The overall observability for privileges could be improved by adding `SHOW` commands that show a
user their current privileges. Some examples may be:

- `SHOW PRIVILEGES` would show all privileges for the current user.
- `SHOW PRIVILEGES ON SYSTEM` would show all system privileges for the current user.
- `SHOW PRIVILEGES ON TABLE t` would show all table privileges for the current user on `t`.
- etc.

While these aren't specific to system privileges, they are relevant.

## Alternatives

- Add system privileges and don't remove role attributes.
- Make role attributes inheritable and lose some PostgreSQL compatibility.
- Keep the current setup.

## Open questions

None currently.
