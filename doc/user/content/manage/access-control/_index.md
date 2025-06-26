---
title: "Access control"
description: "How to configure and manage access control in Materialize."
disable_list: true
aliases:
  - /manage/access-control/sso/
  - /manage/access-control/create-service-accounts/
  - /manage/access-control/manage-network-policies/
menu:
  main:
    parent: manage
    name: Access control
    identifier: 'access-control'
    weight: 10
---

{{< note >}}
Configuring and managing access control in Materialize
requires **administrator** privileges.
{{</ note >}}

## Role-based Access Control (RBAC)

### Enabling RBAC

{{< include-md file="shared-content/enable-rbac.md" >}}

### Configuring RBAC

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

[//]: # "TODO(morsapaes) It feels too specific to add the RBAC observability
views here. Need to think about where to work these in."
