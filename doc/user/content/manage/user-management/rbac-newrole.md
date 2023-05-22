---
title: "Create a new user and role"
description: ""
menu:
  main:
    parent: user-management
    weight: 15
---

This guide walks you through creating a new user and new role in Materialize. By
the end of this tutorial you will:

* Invite a new user in the Materialize Console
* Create a new role in your Materialize account using `psql`
* Apply privileges to the new role
* Assign the new role to the new user

## Before you begin

* Make sure you have `psql` installed locally.

* Make sure you have a [Materialize account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).

## Step 1. Invite a new user

1. [Login to the Materialize console](https://console.materialize.com/) and navigate to Account > Account
Settings > Users.

1. Click **Invite User** and fill in the user information.

The **Organization Admin** and **Organization Member** roles refer to `SUPERUSER`
privileges

## Step 2. Connect to Materialize

Materialize stores role names and role IDs in the mz_roles catalog table. You need to connect to your Materialize instance with `psql` to add roles
to this table.

1. In the [Materialize UI](https://console.materialize.com/), go to the **Connect** screen.

1. Create a new app password.

    The app password will be displayed only once, so be sure to copy the password somewhere safe. If you forget your password, you can create a new one.

1. Copy the `psql` command.

## Step 3. Create a new role

1. Open a new terminal window, run the `psql` command, and enter your app password.

1. Now that you're logged in, you can create a new role in the `mz_roles` table:

    ```sql
    CREATE ROLE dev_role;
    ```

1. Each role you create has default **role attributes** that determine how they
    can interact with Materialize objects. Let's look at the role attributes of
    the role you created:

    ```sql
    SELECT * FROM mz_roles WHERE 'name' = dev_role;
    ```

    Your `dev_role` returns attributes similar to the following:

    ```nofmt
    -[ RECORD 1 ]--+------
    id             | u8
    oid            | 50991
    name           | dev_role
    inherit        | t
    create_role    | f
    create_db      | f
    create_cluster | f
    ```
    Your `id` and `oid` values will look different.

  The `inherit`, `create_role`, `create_db`, and `create_cluster` are the
  role attributes assigned to a role when it is created. These attributes
  determine the system-level permissions of a role and do not impact object-level privileges.

  * `INHERIT` is set to true by default and allows roles to inherit the
    privileges of roles it is a member of.

  * `CREATEROLE` allows the role to create, change, or delete other roles or
    assign role membership.

  * `CREATEDB` allows the role to create new databases.

  * `CREATECLUSTER` allows the role to create Materialize clusters. This
    attribute is unique to the Materialize concept of clusters.

## Step 4. Create example objects

Your `dev_role` has the default system-level permissions and needs object-level privileges. RBAC allows you to apply granular privileges to objects in the SQL hierarchy. Let's create some example objects in the system and determine what
privileges the role needs.

1. In your `psql` terminal, create a new example cluster to avoid impacting
   other environments:

   ```sql
   CREATE CLUSTER dev_cluster REPLICAS (devr1 (SIZE = '3xsmall'));
   ```

1. Change into the example cluster:


   ```sql
   SET CLUSTER TO dev_cluster;
   ```

1. Create a new database, schema, and table:

   ```sql
   CREATE DATABASE dev_db;
   ```

   ```sql
   CREATE SCHEMA dev_db.schema;
   ```

   ```sql
   CREATE TABLE dev_table (a int, b text NOT NULL);
   ```

You just created a set of objects. Your schema object belongs to
the database. You can access the cluster from any database. The next
step is to grant privileges to your role based on the role needs.

## Step 5. Grant privileges to a role

In this example, let's say your `dev_role` needs the following permissions:

* Read, write, and append privileges on the table
* Usage privileges on the schema
* All available privileges on the database

1. In your terminal, grant table-level privileges to the `dev_role`:

   ```sql
   GRANT SELECT, UPDATE, INSERT ON dev_table TO dev_role;
   ```

   Table objects have four available privileges - `read`, `write`, `insert`, and
   `delete`. The `dev_role` doesn't need `delete` permissions, so it is not
   applied in the `GRANT` statement above.

2. Grant schema privileges to the `dev_role`:

   ```sql
   GRANT USAGE, CREATE ON SCHEMA dev_db.schema TO dev_role;
   ```

   Schemas have `USAGE` and `CREATE` privileges available to grant.

3. Grant database privileges to the `dev_role`:

   ```sql
   GRANT USAGE, CREATE ON DATABASE dev_db TO dev_role;
   ```

4. Grant cluster privileges to the `dev_role`:

   ```sql
   GRANT USAGE, CREATE ON CLUSTER dev_cluster TO dev_role;
   ```

   Materialize cluster privileges are unique to the Materialize RBAC structure.
   To have access to the objects within a cluster, you must also have the same
   level of access to the cluster itself.

## Step 6. Assign a role to a user

The `dev_role` now has the acceptable privileges it needs. Let's apply this role
to a user in your Materialize organization.

1. In your terminal, use the `GRANT` statement to apply a role to your new user:

   ```sql
   GRANT dev_role TO <new_user>;
   ```

1. To review the permissions a role has, you can view the object data:

   ```sql
   SELECT name, privileges FROM mz_tables WHERE name='dev_table';
   ```

   The output should return the owner object ID, the level of permission, and the object ID of the role on the table.

   ```nofmt
   name|privileges
   dev_table|{u1=arwd/u1,u8=arw/u1}
   (1 row)
   ```

   In this example, object ID 'u1` has append, read, write, and delete
   privileges on the table. Object ID `u8` is the `dev_role` and has append, read, and write privileges,
   which were assigned by the `u1` user.


## Next steps

You just used RBAC to manage user privileges in Materialize! The next guide will
cover altering and dropping user roles. Move to the next guide in the series, or
destroy the objects you created for this guide.

```
DROP CLUSTER dev_cluster CASCADE;
DROP DATABASE dev_db CASCADE;
DROP TABLE dev_table;
```


For more information on RBAC in Materialize, review the reference documentation:

* [GRANT ROLE](https://materialize.com/docs/sql/grant-role/)
* [CREATE ROLE](https://materialize.com/docs/sql/create-role/)
* [GRANT PRIVILEGE](https://materialize.com/docs/sql/grant-privilege/)
