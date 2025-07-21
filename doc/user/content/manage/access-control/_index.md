---
title: "Access control (Role-based)"
description: "How to configure and manage role-based access control (RBAC) in Materialize."
disable_list: true
aliases:
  - /manage/access-control/rbac/
menu:
  main:
    parent: manage
    identifier: 'access-control'
    weight: 12
---

{{< note >}}
Configuring and managing access control in Materialize
requires **administrator** privileges; e.g., **Organization Admin** role.
{{</ note >}}

<a name="role-based-access-control-rbac" ></a>

In Materialize, organizations can use role-based access management (RBAC) to
help prevent unauthorized or improper access to sensitive objects. For example,
organization administrators can sue RBAC to:

* Determine which users have read or write privileges for specific objects.

* Control how users interact with clusters by giving them different levels of
access to resources.

* Prevent accidental operations from unauthorized users.

* Isolate access to user-facing data from internal organization data.

A role is a collection of privileges you can apply to users. Roles make it
easier to assign or revoke privileges on Materialize objects. You can group
users into specified roles with different levels of privileges and adjust those
privileges to ensure they have the correct level of access to objects.

In Materialize, you have both:

- Predefined [Organization roles](#organization-roles)

- Custom user roles (associated with a specific user/function/etc.)

## Organization roles

Each user/service account in Materialize is associated with an organization
role:

{{< yaml-table data="access-controls/organization_roles" >}}

The default level of access to the database is determined by the
organization-level role a user/service account is assigned.

The first user in an organization, and subsequently any user that is assigned
`Organization Admin`, is a database _superuser_, and has unrestricted access to
all resources in a Materialize region. Users that are assigned `Organization
Member` are restricted to a [default set of basic
privileges](#modifying-default-privileges) that need to be configured and
modified via role-based access control (RBAC).

## User roles

When an invited user logs in for the first time (and only then), a role with the
same name as their e-mail address is created. Administrators can also separately
create roles.

### Role attributes

Role attributes are actions available to any role you create. Attributes are
independent of any other object in Materialize and apply to the entire
organization. You can edit these actions when you create the role:

| Name              | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `INHERIT`         | **Read-only.** Can inherit privileges of other roles.                       |

PostgreSQL uses role attributes to determine if a role is allowed to execute certain statements. In
Materialize these have all been replaced by system privileges.

### Privileges

Privileges are the actions or operations a role is allowed to perform on a
specific object. After you create a role, you can grant it the following
object-specific privileges in Materialize:

| Privilege             | Description                                                                                    | `psql` |
|-----------------------|------------------------------------------------------------------------------------------------|--------|
| `SELECT`              | Allows selecting rows from an object.                                                          | `r`    |
| `INSERT`              | Allows inserting into an object.                                                               | `a`    |
| `UPDATE`              | Allows updating an object (requires `SELECT`).                                                 | `w`    |
| `DELETE`              | Allows deleting from an object (requires `SELECT`).                                            | `d`    |
| `CREATE`              | Allows creating a new object within another object.                                            | `C`    |
| `USAGE`               | Allows using an object or looking up members of an object.                                     | `U`    |
| `CREATEROLE`          | Allows creating, altering, deleting roles and the ability to grant and revoke role membership. | `R`    |
| `CREATEDB`            | Allows creating databases.                                                                     | `B`    |
| `CREATECLUSTER`       | Allows creating clusters.                                                                      | `N`    |
| `CREATENETWORKPOLICY` | Allows creating network policies.                                                              | `P`    |


Note that the system catalog uses the abbreviation of the privilege name.

Objects in Materialize have different levels of privileges available to them.
Materialize supports the following object type privileges:

| Object Type          | Privileges                                                       |
|----------------------|------------------------------------------------------------------|
| `SYSTEM`             | `CREATEROLE`, `CREATEDB`, `CREATECLUSTER`, `CREATENETWORKPOLICY` |
| `DATABASE`           | `USAGE`, `CREATE`                                                |
| `SCHEMA`             | `USAGE`, `CREATE`                                                |
| `TABLE`              | `INSERT`, `SELECT`, `UPDATE`, `DELETE`                           |
| `VIEW`               | `SELECT`                                                         |
| `MATERIALIZED  VIEW` | `SELECT`                                                         |
| `TYPE`               | `USAGE`                                                          |
| `SOURCE`             | `SELECT`                                                         |
| `CONNECTION`         | `USAGE`                                                          |
| `SECRET`             | `USAGE`                                                          |
| `CLUSTER`            | `USAGE`, `CREATE`                                                |

Materialize object access is also dependent on cluster privileges.
Roles that need access to an object that use compute resources must also have
the same level of access to the cluster. Materialize objects that use compute
resources are:

* Replicas
* Sources
* Sinks
* Indexes
* Materialized views

### Inheritance

Inheritance in RBAC allows you to create roles that inherit privileges from
other roles. Inheritance only applies to role privileges; role attributes and
parameters are **not inherited**. Inheriting privileges allows you to minimize
the number of roles you have to manage.

### Configuring basic RBAC

{{< warning >}}
Here be dragons. 🐉 This setup is **not recommended** unless you are trialing
Materialize.
{{< /warning >}}

If you're just getting started and haven't yet decided on an [RBAC strategy](#configuring-advanced-rbac)
for your organization, we recommend [modifying default privileges](#modifying-default-privileges)
or — if you just need to move fast and break things — [inviting users as administrators](#inviting-users-as-administrators).

#### Modifying default privileges

Every Materialize region has a `PUBLIC` system role that determines the default
privileges available to all other roles. On creation, users are automatically
granted membership in `PUBLIC`, and inherit the privileges assigned to it. By
default, members of this role (and therefore **all users**) have the following
[privileges](/sql/grant-privilege/#privilege):

Privilege                            | Scope     |
-------------------------------------|-----------|
`USAGE`                              | All types, all system catalog schemas, the `materialize.public` schema, the `materialize` database, and the `quickstart` cluster.|
`SELECT`                             | All system catalog objects.  |

This means that new, non-administrator users have limited access to resources in
a Materialize region, and don't have the ability to e.g., create new clusters,
databases, or schemas. To modify the default privileges available to all other
roles in a Materialize region, you can use the [`ALTER DEFAULT PRIVILEGES`](/sql/alter-default-privileges/)
command.

```mzsql
# Use SHOW ROLES to list existing roles in the system, which are 1:1 with invited users
SHOW ROLES;

-- Example: grant read-only access to all object types, to all roles in the
-- system via the PUBLIC role For PostgreSQL compatibility reasons, TABLE is
-- the object type for sources, views, tables and materialized views
ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON TABLES TO PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON DATABASES TO PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON SCHEMAS TO PUBLIC;
```

#### Inviting users as administrators

The path of least resistance to setting up RBAC is...not setting it up. If you
invite all users as `Organization Admin`, all users will have the _superuser_
role.

Keep in mind that you will be giving all users unrestricted access to all
resources in your Materialize region, as well as all features in the
Materialize console. While this might be a viable solution when trialing
Materialize with a small number of users, it can be **hard to revert** once you
decide to roll out a RBAC strategy for your Materialize organization.

As an alternative, you can approximate the set of privileges of a _superuser_ by
instead modifying the default privileges to be wildly permissive:

```mzsql
-- Use SHOW ROLES to list existing roles in the system, which are 1:1 with invited users
SHOW ROLES;

-- Example: approximate full-blown admin access by modifying the default
-- privileges inherited by all roles via the PUBLIC role
GRANT ALL PRIVILEGES ON SCHEMA materialize.public TO PUBLIC;
GRANT ALL PRIVILEGES ON DATABASE materialize TO PUBLIC;
GRANT ALL PRIVILEGES ON SCHEMA materialize.public TO PUBLIC;
GRANT ALL PRIVILEGES ON CLUSTER default TO PUBLIC;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA materialize.public TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL TYPES IN SCHEMA materialize.public TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL SECRETS IN SCHEMA materialize.public TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL CONNECTIONS IN SCHEMA materialize.public TO PUBLIC;

ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA materialize.public GRANT ALL PRIVILEGES ON TABLES TO PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA materialize.public GRANT ALL PRIVILEGES ON TYPES TO PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA materialize.public GRANT ALL PRIVILEGES ON SECRETS TO PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA materialize.public GRANT ALL PRIVILEGES ON CONNECTIONS TO PUBLIC;
```

It's important to note that, while [`GRANT ALL PRIVILEGES`](/sql/grant-privilege/)
applies to all objects that exist when the grant is run, [`ALTER DEFAULT PRIVILEGES`](/sql/alter-default-privileges/)
applies to objects created in the future (aka future grants).

### Configuring advanced RBAC

There is no one-size-fits-all solution for RBAC: the right strategy will depend
on your team size, and existing security policies. Being able to create an
unbounded amount of custom roles with custom privileges and structural
hierarchy can be overwhelming, so following are examples on how to roll out
three common RBAC strategies: user roles, functional roles, and environment
roles.

#### User roles

If your Materialize user base is small and you don't expect it to grow
significantly over time, you can grant and revoke privileges directly to/from
user roles.

```mzsql
-- Use SHOW ROLES to list existing roles in the system.
SHOW ROLES;

-- Example: grant usage on a specific database & schema to individual roles
GRANT USAGE ON DATABASE d1 TO "user1@company.com", "user2@company.com";
GRANT USAGE ON SCHEMA d1.s1 TO "user1@company.com", "user2@company.com";
GRANT USAGE ON CLUSTER c1 to "user1@company.com", "user2@company.com";

-- Example: grant individual roles permission to create clusters and databases
GRANT CREATECLUSTER, CREATEDB on system to "user1@company.com";
```

This strategy allows you to skip setting up a complex hierarchy of roles
upfront, at the cost of manual work. As you might expect, this strategy does
not scale, so you might want to rethink your RBAC strategy if your user base
grows past a reasonable number (say, 10+).

#### Functional roles

If your Materialize user base is made up of business functions that clearly map
to distinct access profiles, a common strategy is to map those functions to
roles in your RBAC hierarchy. You can then grant and revoke privileges to/from
functional roles, and any user that is a member of that role will inherit
them.

As an example, Data Engineers might need a larger scope of permissions to create
and evolve the data model, while Data Analysts might only need read permissions
to query Materialize using BI tools.

```mzsql
-- Use SHOW ROLES to list existing roles in the system, which are 1:1 with invited users
SHOW ROLES;

-- Step 1: create the data_analyst role
CREATE ROLE data_analyst;

-- Step 2: grant usage on the database & schema to the data_analyst role
GRANT USAGE ON DATABASE d1 TO data_analyst;
GRANT USAGE ON SCHEMA d1.s1 TO data_analyst;
GRANT USAGE ON CLUSTER c1 to data_analyst;

-- Step 3: grant select to all objects in the d1.s1 schema to the data_analyst role.
-- For pre-existing objects in the schema (skip if there are none!)
GRANT SELECT ON ALL TABLES IN SCHEMA d1.s1 TO data_analyst;
-- For future objects created in the schema
ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA d1.s1 GRANT SELECT ON TABLES TO data_analyst;

-- Step 4: create the data_engineer role
CREATE ROLE data_engineer;

-- Step 5: grant usage & create on the database & schema to the data_engineer role
GRANT USAGE, CREATE ON DATABASE d1 TO data_engineer;
GRANT USAGE, CREATE ON SCHEMA d1.s1 TO data_engineer;
GRANT USAGE, CREATE ON CLUSTER c1 to data_engineer;

-- Step 6: grant all privileges to all objects in the d1.s1 schema to the data_engineer role
-- For pre-existing objects in the schema (skip if there are none!)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA d1.s1 TO data_engineer;
-- For future objects created in the schema
ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA d1.s1 GRANT ALL PRIVILEGES ON TABLES TO data_engineer;

-- Step 7: add member(s) to the respective role
GRANT data_analyst TO "user1@company.com", "user2@company.com";
GRANT data_engineer TO "user3@company.com";
```

This strategy allows you to more efficiently manage permissions for multiple
users at a time, as well as create an intuitive hierarchy based on each
functional role's expected usage of Materialize.

#### Environment roles

[//]: # "NOTE(morsapaes) We are pointing users to different links when we ask
them to reach out, across the documentation. Standardize on one."

{{< warning >}}
This strategy relies on the `NOINHERIT` role attribute and the `SET ROLE`
command, which are unimplemented in Materialize.
Please [reach out](https://materialize.com/contact/) if you're
interested in this strategy!
{{< /warning >}}

If your Materialize user base frequently switches between multiple development
environments, you might want to ensure that users don't accidentally run
commands in the wrong environment e.g., production. You can create an
environment-specific role with the `NOINHERIT` attribute, which prevents other
roles from inheriting it. This means that users have to explicitly run e.g.,
`SET ROLE production` before being able to run any commands in the specified
environment.

```mzsql
-- Step 1: create the dev and prod roles
CREATE ROLE dev;
CREATE ROLE prod NOINHERIT;

-- Step 2: grant usage & create on the respective database & cluster for each role
GRANT ALL PRIVILEGES ON DATABASE d_dev TO dev;
GRANT ALL PRIVILEGES ON DATABASE d_prod TO prod;
GRANT ALL PRIVILEGES ON CLUSTER c_dev TO dev;
GRANT ALL PRIVILEGES ON CLUSTER c_prod TO prod;

-- Step 3: grant all privileges to all objects on the database to the respective roles
ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN DATABASE d_dev GRANT ALL PRIVILEGES ON TABLES TO dev;
ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN DATABASE d_prod GRANT ALL PRIVILEGES ON TABLES TO prod;

-- Step 4: add member(s) to the respective role - user1 has both prod & dev access, user2 has only dev access
GRANT dev TO "user1@company.com", "user2@company.com";
GRANT prod TO "user1@company.com";

-- To run queries against the prod database and cluster, user1 will first need to run
SET ROLE prod;
```

This strategy allows you to add an extra layer of access control to your
critical environments, and ensure that any user with privileges to perform
destructive actions is performing them intentionally in that specific
environment. It's like `sudo` for your database!
