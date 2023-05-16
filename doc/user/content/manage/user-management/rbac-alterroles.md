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

If you have not completed the New User guide, you'll need to
create an example environment with a new user, new role, cluster, database,
schema, and table.

## Create a second role

If you completed the previous new user guide and did not destroy your
environment, your `dev_role` still exists in your `mz_roles` table. For this
tutorial, you will create a new role with different privileges to other objects.
Then you will apply those privileges to the `dev` role and alter or drop
privileges as needed.

1. Create a second role your Materialize account:

   ```sql
   CREATE ROLE qa WITH CREATEDB;
   ```

   This role has permission to create a new database in the Materialize account.

2. Create a new `qa_db` database:

   ```sql
   CREATE DATABASE qa_db;
   ```

3. Apply `USAGE` and `CREATE` privileges to the `qa_role` role for the new database:

   ```sql
   ```

## Add inherited privileges to a role

Your `dev_role` also needs access to `qa_db`. You can apply these
privileges individually or you can choose to grant the `dev_role` the same
permissions as the `qa_role`.

1. Add `dev_role` as a member of `qa_role`:

   ```sql
   GRANT qa_role TO dev_role;
   ```

   Roles also inherit all the attributes and privileges of the granted role.
   Making roles members of other roles allows you to manage sets of
   permissions, rather than granting privileges to roles on an individual basis.

2. Review the privileges of `qa_role` and `dev_role`:

   ```sql
   SELECT name, privileges FROM mz_databases WHERE name='qa_db';
   ```

   Your output will be similar to the example below:

   ```nofmt
   name|privileges
   qa_db|{u1=UC/u1,u9=UC/u1}
   (1 row)
   ```

   Both `dev_role` and `qa_role` have usage and create access to the `qa_db`. In
   the next section, you will edit role attributes for these roles and drop
   privileges.

## Revoke privileges and alter role attributes

Your `dev_role` and `qa_role` have the same role attributes. You can alter or
revoke certain attributes and privileges for each role, even if they are
inherited from another role.

1. Update the `CREATEROLE` attribute for the `dev_role`:

   ```sql
   ALTER ROLE dev WITH CREATEROLE;
   ```

   The `qa_role` will not have the `CREATEROLE` attribute enabled because the
   attribute was enabled _after_ the role grant.

2. Compare the attributes of the `qa_role` and `dev_role`:

   ```sql
   SELECT * FROM mz_roles;
   ```

   Your output should contain the role names and the updated attributes:

   ```sql
   id|oid|name|inherit|create_role|create_db|create_cluster
   u9|22444|qa_role|t|f|f|f
   u8|20016|dev_role|t|t|f|f
   ```

3. Let's say you decide `dev_role` no longer needs `CREATE` privileges on the
   `qa_db` object. You can revoke that privilege for the role:

   ```sql
   REVOKE CREATE ON DATABASE qa_db FROM dev_role;
   ```

   Your output should contain the new privileges for `dev_role`:

   ```nofmt
   name|privileges
   qa_db|{u1=UC/u1,u8=U/u1,u9=UC/u1}
   (1 row)
   ```

## Next steps

You just altered privileges and attributes on your Materialize roles! Remember
to destroy the objects you created for this guide.

1. Drop the roles you created:

   ```sql
   DROP ROLE qa_role;
   DROP ROLE dev_role;
   ```

1. Drop the other objects you created:

   ```sql
   DROP CLUSTER dev_cluster CASCADE;
   DROP DATABASE dev_db CASCADE;
   DROP TABLE dev_table;
   DROP DATABASE qa_db CASCADE;
   ```

For more information on RBAC in Materialize, review the reference documentation:

* [`ALTER ROLE`](https://materialize.com/docs/sql/alter-role/)
* [`REVOKE PRIVILEGE`](https://materialize.com/docs/sql/revoke-privilege/)
* [`DROP ROLE`](https://materialize.com/docs/sql/drop-role/)
