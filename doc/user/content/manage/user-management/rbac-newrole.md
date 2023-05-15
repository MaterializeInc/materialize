---
title: "Create a New User and Role"
description: ""
menu:
  main:
    parent: user-management
    weight: 16
---

This guide walks you through creating a new user and new role in Materialize. By
the end of this tutorial you will:

* Invite a new user in the Materialize Console
* Create a new role in your Materialize account using `psql`
* Apply privileges to the new role
* Assign the new role to the new user

## Before you begin

In this guide, you'll use `psql` to manage users in Materialize, so be sure you
have `psql` installed locally.

If you have not signed up for a free trial, [sign up now](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).


## Invite a new user

First, login to your Materialize console and navigate to Account > Account
Settings > Users. 

Click **Invite User** and fill in the user information.

The **Organization Admin** and **Organization Member** roles refer to console
privileges and are not related to RBAC roles.

## Create a new role

Materialize stores role names, IDs, and object IDs in the `mz_roles` catalog
table. You need to connect to your Materialize instance with `psql` to add roles
to this table.

1. In the [Materialize UI](https://console.materialize.com/), enable the region where you want to run Materialize.

1. On the **Connect** screen, create a new app password and then copy the `psql` command.

    The app password will be displayed only once, so be sure to copy the password somewhere safe. If you forget your password, you can create a new one.

1. Open a new terminal window, run the `psql` command, and enter your app password.
    
    Now that you're logged in, you can create a new role in the `mz_roles`
    table.
    
    ```sql
    CREATE ROLE dev_role;
    ```

1. Each role you create has default **role attributes** that determine how they
    can interact with Materialize objects. Let's look at the role attributes of
    the role you created.
    
    ```sql
    SELECT * FROM mz_roles WHERE 'name' = dev_role;
    ```
    
    Your `dev` role returns the following attributes:
    
    ```
    -[ RECORD 1 ]--+------
    id             | u8
    oid            | 50991
    name           | dev_role
    inherit        | t
    create_role    | f
    create_db      | f
    create_cluster | f
    ```
    
    The `inherit`, `create_role`, `create_db`, and `create_cluster` are the
    role attributes assigned to a role when it is created. These attributes
    determine the system level permissions of a role and do not impact object
    level privileges.
    
    `INHERIT` is set to true by default and allows roles to inherit the
    privileges of roles it is a member of.
    
    `CREATEROLE` allows the role to create, change, or delete other roles or
    assign role membership.
    
    `CREATEDB` allows the role to create new databases.
    
    `CREATECLUSTER` allows the role to create Materialize clusters. This
    attribute is unique to the Materialize concept of clusters.
    
## Apply privileges to a role

Your `dev` role has the default system level permissions and needs object
specific privileges. RBAC allows you to apply granular privileges to objects in the SQL hierarchy. Let's create some example objects in the system and determine what
privileges the role needs.

1. In your `psql` terminal, create a new example cluster to avoid impacting
   other environments.
   
   ```sql
   CREATE CLUSTER devcluster;
   ```
   
1. Change into the example cluster.
   
   
   ```sql
   SET CLUSTER TO devcluster;
   ```
   
1. Create a new database, schema, and table in the cluster.
   
   ```sql
   CREATE DATABASE devdb;
   ```
   
   ```sql
   CREATE SCHEMA devdb.schema;
   ```
   
   ```sql
   CREATE TABLE d (a int, b text NOT NULL);
   ```
   
You just created a nested hierarchy of objects. Your schema object belongs to
the database and the database is only accessible within the cluster. The next
step is to grant privileges to your role based on the role needs.

In this example, let's say your `dev` role needs the following permissions:

* Read, write, and append privileges on the table
* Update privileges on the schema
* All available privileges on the database
   
1. In your terminal, grant table-level privileges to the `dev` role.

   ```sql
   GRANT SELECT, UPDATE, INSERT ON d TO dev_role;
   ```
   
   Table objects have four available privileges - `read`, `write`, `insert`, and
   `delete`. The `dev` role doesn't need `delete` permissions, so it is not
   applied in the GRANT statement above.
   
2. Grant schema privileges to the `dev` role.

   ```sql
   GRANT USAGE, CREATE ON SCHEMA devdb.schema TO dev_role;
   ```

   Schemas have `UPDATE` and `CREATE` privileges available to grant. 
   
3. Grant database privileges to the `dev` role.

   ```sql
   GRANT USAGE, CREATE ON DATABASE devdb TO dev_role;
   ```
   
4. Grant cluster privileges to the `dev` role.
   
   ```sql
   GRANT USAGE, CREATE ON CLUSTER devcluster TO dev_role;
   ```

   Materialize cluster privileges are unique to the Materialize RBAC structure.
   To have access to the objects within a cluster, you must also have the same
   level of access to the cluster itself.
   
## Assign a role to a user

The `dev` role now has the acceptable privileges it needs. Let's apply this role
to a user in your Materialize organization.

1. In your terminal, use the `GRANT` statement to apply a role to your new user.

   ```sql
   GRANT dev TO <new_user>;
   ```
   
1. To review the permissions a role has, you can view the object data.
   
   ```sql
   SELECT name, privileges::text FROM mz_tables WHERE name='t';
   ```
   
   The output should return the owner, the level of permission, and the name of
   the role on the table.
   
   ```shell
   name|privileges
   t|{admin@mz.com=arwd/admin@mz.com,dev_role=arw/admin@mz.com}
   (1 row)
   ```
   
   In this example, the administrator has append, read, write, and delete
   privileges on the table. The `dev` role has append, read, and write privileges,
   which were assigned by the admin user.
   

## Next Steps

You just used RBAC to manage user privileges in Materialize! The next guide will
cover altering and dropping user roles. Move to the next guide in the series, or
destroy the objects you created for this guide.

```
DROP CLUSTER devcluster CASCADE;
```
    
    
For more information on RBAC in Materialize, review the reference documentation:

* [GRANT ROLE](https://materialize.com/docs/sql/grant-role/) 
* [CREATE ROLE](https://materialize.com/docs/sql/create-role/)
* [GRANT PRIVILEGE](https://materialize.com/docs/sql/grant-privilege/)
