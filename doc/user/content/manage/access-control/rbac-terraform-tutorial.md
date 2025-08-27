---
title: "Tutorial: Manage privileges with Terraform"
description: "Add users, create roles, and assign privileges in Materialize with Terraform"
menu:
  main:
    parent: access-control
    weight: 30
---

This tutorial walks you through managing roles in Materialize with [Terraform](https://www.terraform.io/). By the end of this tutorial you will:

* Create two new roles in your Materialize
* Apply privileges to the new roles
* Assign a role to a user
* Modify and remove privileges on roles

In this scenario, you are a DevOps engineer responsible for managing your Materialize account with code. You recently hired a new developer who needs privileges in a non-production cluster. You will create specific privileges for the new role that align with your business needs and restrict the developer role from having access to your production cluster.

## Before you begin

* Make sure you have a [Materialize account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation) and already have a password to connect with.

* You should be familiar with setting up a [Terraform project in Materialize](https://materialize.com/docs/manage/terraform/).

* Have an understanding of permissions in Materialize.

## Step 1. Create Role

1. You can create a functional role with a set of object-specific privileges.
   First, we will create a role resource in Terraform.

    ```hcl
    resource "materialize_role" "dev_role" {
      name = "dev_role"
    }
    ```

2. We will run Terraform to create this role.

    ```shell
    terraform apply
    ```

    {{< note >}}
  All of the resources in this tutorial can be run with a single terraform apply but we will add and apply resources incrementally to better illustrate grants.
    {{</ note >}}

3. Each role you create has default role attributes that determine how they can interact with Materialize objects. Let’s look at the role attributes of the role you created:

    ```mzsql
    SELECT * FROM mz_roles WHERE name = 'dev_role';
    ```

    <p></p>

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

## Step 2. Create example objects

Your `dev_role` has the default system-level permissions and needs object-level privileges. RBAC allows you to apply granular privileges to objects in the SQL hierarchy. Let's create some example objects in the system and determine what privileges the role needs.

1. In the Terraform project we will add a cluster, cluster replica, database, schema and table.

    ```hcl
    resource "materialize_cluster" "cluster" {
      name = "dev_cluster"
    }

    resource "materialize_cluster_replica" "cluster_replica" {
      name         = "devr1"
      cluster_name = materialize_cluster.cluster.name
      size         = "25cc"
    }

    resource "materialize_database" "database" {
      name = "dev_db"
    }

    resource "materialize_schema" "schema" {
      name          = "schema"
      database_name = materialize_database.database.name
    }

    resource "materialize_table" "table" {
      name          = "dev_table"
      schema_name   = materialize_schema.schema.name
      database_name = materialize_database.database.name

      column {
        name = "a"
        type = "int"
      }
      column {
        name     = "b"
        type     = "text"
        nullable = true
      }
    }
    ```

2. We will apply our Terraform project again to create the object resources.

    ```shell
    terraform apply
    ```

3. Now that our resources exist, we can query their privileges before they have been associated with our role created in step 1.

    ```mzsql
    SELECT name, privileges FROM mz_tables WHERE name = 'dev_table';
    ```

    <p></p>

    ```nofmt
    name|privileges
    dev_table|{u1=arwd/u1,u8=arw/u1}
    (1 row)
    ```

Currently, the `dev_role` has no permissions on the table `dev_table`.

## Step 3. Grant privileges on example objects

In this example, let's say your `dev_role` needs the following permissions:

* Read, write, and append privileges on the table
* Usage privileges on the schema
* All available privileges on the database
* Usage and create privileges on the cluster

1. We will add the grant resources to our Terraform project.

    ```hcl
    resource "materialize_table_grant" "dev_role_table_grant" {
      for_each = toset(["SELECT", "INSERT", "UPDATE"])

      role_name     = materialize_role.dev_role.name
      privilege     = each.value
      database_name = materialize_table.table.database_name
      schema_name   = materialize_table.table.schema_name
      table_name    = materialize_table.table.name
    }
    ```

    {{< note >}}
  All of the grant resources are a 1:1 between a specific role, object and privilege. So adding three privileges to the `dev_role` will require three Terraform resources which can can be accomplished with the `for_each` meta-argument.
    {{</ note >}}

2. We will run Terraform to grant these privileges on the `dev_table` table.

    ```shell
    terraform apply
    ```

3. We can now check the privileges on our table again

    ```mzsql
    SELECT name, privileges FROM mz_tables WHERE name = 'dev_table';
    ```

    <p></p>

    ```nofmt
    name|privileges
    dev_table|{u1=arwd/u1,u8=arw/u1}
    (1 row)
    ```

4. Now we will include the additional grants for the schema, database and cluster.

    ```hcl
    resource "materialize_schema_grant" "dev_role_schema_grant_usage" {
      role_name     = materialize_role.dev_role.name
      privilege     = "USAGE"
      database_name = materialize_schema.schema.database_name
      schema_name   = materialize_schema.schema.name
    }

    resource "materialize_database_grant" "dev_role_database_grant" {
      for_each = toset(["USAGE", "CREATE"])

      role_name     = materialize_role.dev_role.name
      privilege     = each.value
      database_name = materialize_database.database.name
    }

    resource "materialize_cluster_grant" "dev_role_cluster_grant" {
      for_each = toset(["USAGE", "CREATE"])

      role_name    = materialize_role.dev_role.name
      privilege    = each.value
      cluster_name = materialize_cluster.cluster.name
    }
    ```

5. Run Terraform again to grant these additional privileges on the database, schema and cluster.

    ```shell
    terraform apply
    ```

## Step 4. Assign the role to a user

The dev_role now has the acceptable privileges it needs. Let’s apply this role to a user in your Materialize organization.

1. Include a Terraform resource that grants the role we have created in our Terraform project to a Materialize user.

    ```hcl
    resource "materialize_role_grant" "dev_role_grant_user" {
      role_name   = materialize_role.dev_role.name
      member_name = "<user>"
    }
    ```

2. Apply our Terraform change.

    ```shell
    terraform apply
    ```

3. To review the permissions a roles, you can view the object data:

    ```mzsql
    SELECT name, privileges FROM mz_tables WHERE name = 'dev_table';
    ```

    The output should return the object ID, the level of permission, and the assigning role ID.


    ```nofmt
    name|privileges
    dev_table|{u1=arwd/u1,u8=arw/u1}
    (1 row)
    ```
    In this example, role ID `u1` has append, read, write, and delete privileges on the table. Object ID `u8` is the `dev_role` and has append, read, and write privileges, which were assigned by the `u1` user.

## Step 5. Create a second role

Next, you will create a new role with different privileges to other objects. Then you will apply those privileges to the dev role and alter or drop privileges as needed.

1. Create a second role your Materialize account:

    ```hcl
    resource "materialize_role" "qa_role" {
      name = "qa_role"
    }
    ```

2. Apply `CREATEDB` privileges to the `qa_role`:

    ```hcl
    resource "materialize_grant_system_privilege" "qa_role_system_createdb" {
      role_name = materialize_role.qa_role.name
      privilege = "CREATEDB"
    }
    ```

3. Create a new `qa_db` database:

    ```hcl
    resource "materialize_database" "database" {
      name = "dev_db"
    }
    ```

4. Apply `USAGE` and `CREATE` privileges to the `qa_role` role for the new database:

    ```hcl
    resource "materialize_database_grant" "qa_role_database_grant" {
      for_each = toset(["USAGE", "CREATE"])

      role_name     = materialize_role.qa_role.name
      privilege     = each.value
      database_name = materialize_database.database.name
    }
    ```

## Step 6. Add inherited privileges

Your `dev_role` also needs access to `qa_db`. You can apply these privileges individually or you can choose to grant the `dev_role` the same permissions as the `qa_role`.

1. Add `dev_role` as a member of `qa_role`:

    ```hcl
    resource "materialize_role_grant" "qa_role_grant_dev_role" {
      role_name   = materialize_role.qa_role.name
      member_name = materialize_role.dev_role.name
    }
    ```

2. We will run Terraform to grant these the inherited privileges.

    ```shell
    terraform apply
    ```

3. Review the privileges of `qa_role` and `dev_role`:

   ```mzsql
   SELECT name, privileges FROM mz_databases WHERE name='qa_db';
   ```

   Your output will be similar to the example below:

   ```nofmt
   name|privileges
   qa_db|{u1=UC/u1,u9=UC/u1}
   (1 row)
   ```

   Both `dev_role` and `qa_role` have usage and create access to the `qa_db`. In the next section, you will edit role attributes for these roles and drop privileges.


## Step 7. Revoke privileges

You can revoke certain privileges for each role, even if they are inherited from another role.

1. Remove the resource `materialize_database_grant.qa_role_database_grant_create` from the Terraform project.

2. We will run Terraform to revoke privileges.

    ```shell
    terraform apply
    ```

    Because Terraform is responsible for maintaining the state of our project, removing this grant resource and running an `apply` is the equivalent of running a revoke statement:

    ```mzsql
    REVOKE CREATE ON DATABASE dev_table FROM dev_role;
    ```

## Next steps

To destroy the roles and objects you created, you can remove all resources from your Terraform project. Running a `terraform apply` will `DROP` all objects.

## Related pages

For more information on RBAC in Materialize, review the reference documentation:

* [`GRANT ROLE`](https://materialize.com/docs/sql/grant-role/)
* [`CREATE ROLE`](https://materialize.com/docs/sql/create-role/)
* [`GRANT PRIVILEGE`](https://materialize.com/docs/sql/grant-privilege/)
* [`ALTER ROLE`](https://materialize.com/docs/sql/alter-role/)
* [`REVOKE PRIVILEGE`](https://materialize.com/docs/sql/revoke-privilege/)
* [`DROP ROLE`](https://materialize.com/docs/sql/drop-role/)
