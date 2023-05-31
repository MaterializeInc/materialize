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

Role attributes present some problems as they currently exist:

- It can be confusing that there are two separate privilege primitives that control a role's
  privilege.
- Role attributes are not inherited which makes it cumbersome to grant attributes to a large
  number of roles.

## Goals

- Reduce confusion around privileges.
- Allow the ability to create databases, clusters, and roles to be inherited through role
  membership.
- Maintain PostgreSQL compatibility.

## Overview

We will add a new type of privilege called System Privileges. They will look and act just like
normal object privileges, but they will grant the abilities that would otherwise be granted through
role attributes. They will exist in parallel with role attributes.

NOTE: Other databases, such as CockroachDB and Snowflake, have some form of system privileges that
control similar privileges to role attributes.

## Detailed description

System privileges will be granted with the following syntax:
`GRANT <privilege-specification> ON SYSTEM TO <role-specification>`

System privileges will be revoked with the following syntax:
`REVOKE <privilege-specification> ON SYSTEM TO <role-specification>`

System privileges and role attributes will interact in the following ways:

- Granting a system privilege will grant the privilege AND the attribute.
- Revoking a system privilege will revoke the privilege AND the attribute.
- Altering a role attribute will affect the role attribute but will not change the system privilege.
- When checking to see if a role is allowed to execute a command we will check if they have the
  system privilege or the role attribute.

This approach has the following nice properties:

- The ability to use system privileges without thinking about role attributes.
- The ability to use role attributes without thinking about system privileges.
- System privileges always take priority over role attributes when using both.

We will support the following system privileges:

- `CREATEDB`: Allows users to create databases.
- `CREATECLUSTER`: Allows users to create clusters.
- `CREATEROLE`: Allows users to create, modify, and delete roles.

System privileges are inherited through role membership.

Whenever someone alters a role attribute we will issue a warning indicating that system privileges
take precedence and are preferred to role attributes. Additionally, the documentation will focus on
system privileges and indicate that role attributes are there for PostgreSQL compatibility and not
the preferred approach.

## Alternatives

- Drop role attributes completely, lose PostgreSQL compatibility, and add system privileges.
- Make role attributes inheritable and lose some PostgreSQL compatibility.
- Keep the current setup.

## Open questions

- Where in the catalog should system privileges be stored? One option is in `mz_roles`. Another
  option is in a new table with a single column for system privileges.
- Should we use the existing `mz_acl_item` for system privileges or create a new type? 
