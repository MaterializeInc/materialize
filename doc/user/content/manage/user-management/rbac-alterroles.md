---
title: "Update and drop roles"
description: ""
menu:
  main:
    parent: user-management
    weight: 16
---

In the previous guide, you created a new role and assigned privileges for
specific objects. This guide walks you through updating your previously created
roles and removing users or roles when they are no longer needed.

## Before you begin

In this guide, you'll use `psql` to manage users in Materialize, so be sure you
have `psql` installed locally.

If you have not completed the [New User guide](/rbac-newrole.md), you'll need to
create an example environment with a new user, new role, cluster, database,
schema, and table.

## Create a second role

If you completed the previous new user guide and did not destroy your
environment, your `dev` role still exists in your `mz_roles` table. For this
tutorial, you will create a new role with different privileges to other objects.
Then you will apply those privileges to the `dev` role and alter or drop
privileges as needed.

1. Create a `qa` role in your Materialize account.
   
   ```sql
   CREATE ROLE qa;
   ```
   
2. Create a `qa_db`.
    
   ```sql
   CREATE DATABASE qa_db
   ```

3. Apply `USAGE` and `CREATE` privileges to the `qa` role for the new database.

   ```sql
   ```

## Add inherited privileges to a role

Your `dev` role also needs access to the `qa` database. You can apply these
privileges individually or you can choose to grant the `dev` role the same
permissions as the `qa` role.

1. Add the `dev` role as a member of the `qa` role.

   ```sql
   GRANT qa TO dev_role;
   ```
   
   Roles also inherit all the attributes and privileges of the granted role.
   Making roles members of other roles allows you to manage a sets of
   permissions, rather than granting privileges to roles on an individual basis.
   
2. Review the privileges of the `qa` and `dev` roles.

   ```sql
   
   ```
   
## Revoke privileges and alter role attributes
