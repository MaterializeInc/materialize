<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Manage
Materialize](/docs/self-managed/v25.2/manage/)  /  [Access control
(Role-based)](/docs/self-managed/v25.2/manage/access-control/)

</div>

# Tutorial: Manage privileges with Terraform

This tutorial walks you through managing roles in Materialize with
[Terraform](https://www.terraform.io/). By the end of this tutorial you
will:

- Create two new roles in your Materialize
- Apply privileges to the new roles
- Assign a role to a user
- Modify and remove privileges on roles

In this scenario, you are a DevOps engineer responsible for managing
your Materialize account with code. You recently hired a new developer
who needs privileges in a non-production cluster. You will create
specific privileges for the new role that align with your business needs
and restrict the developer role from having access to your production
cluster.

## Before you begin

- Make sure you have a [Materialize
  account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation)
  and already have a password to connect with.

- You should be familiar with setting up a [Terraform project in
  Materialize](https://materialize.com/docs/manage/terraform/).

- Have an understanding of permissions in Materialize.

## Step 1. Create Role

1.  You can create a functional role with a set of object-specific
    privileges. First, we will create a role resource in Terraform.

    <div class="highlight">

    ``` chroma
    resource "materialize_role" "dev_role" {
      name = "dev_role"
    }
    ```

    </div>

2.  We will run Terraform to create this role.

    <div class="highlight">

    ``` chroma
    terraform apply
    ```

    </div>

    <div class="note">

    **NOTE:** All of the resources in this tutorial can be run with a
    single terraform apply but we will add and apply resources
    incrementally to better illustrate grants.

    </div>

3.  Each role you create has default role attributes that determine how
    they can interact with Materialize objects. Let’s look at the role
    attributes of the role you created:

    <div class="highlight">

    ``` chroma
    SELECT * FROM mz_roles WHERE name = 'dev_role';
    ```

    </div>

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

    Your `id` and `oid` values will look different.

## Step 2. Create example objects

Your `dev_role` has the default system-level permissions and needs
object-level privileges. RBAC allows you to apply granular privileges to
objects in the SQL hierarchy. Let’s create some example objects in the
system and determine what privileges the role needs.

1.  In the Terraform project we will add a cluster, cluster replica,
    database, schema and table.

    <div class="highlight">

    ``` chroma
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

    </div>

2.  We will apply our Terraform project again to create the object
    resources.

    <div class="highlight">

    ``` chroma
    terraform apply
    ```

    </div>

3.  Now that our resources exist, we can query their privileges before
    they have been associated with our role created in step 1.

    <div class="highlight">

    ``` chroma
    SELECT name, privileges FROM mz_tables WHERE name = 'dev_table';
    ```

    </div>

    ```
    name|privileges
    dev_table|{u1=arwd/u1,u8=arw/u1}
    (1 row)
    ```

Currently, the `dev_role` has no permissions on the table `dev_table`.

## Step 3. Grant privileges on example objects

In this example, let’s say your `dev_role` needs the following
permissions:

- Read, write, and append privileges on the table
- Usage privileges on the schema
- All available privileges on the database
- Usage and create privileges on the cluster

1.  We will add the grant resources to our Terraform project.

    <div class="highlight">

    ``` chroma
    resource "materialize_table_grant" "dev_role_table_grant" {
      for_each = toset(["SELECT", "INSERT", "UPDATE"])

      role_name     = materialize_role.dev_role.name
      privilege     = each.value
      database_name = materialize_table.table.database_name
      schema_name   = materialize_table.table.schema_name
      table_name    = materialize_table.table.name
    }
    ```

    </div>

    <div class="note">

    **NOTE:** All of the grant resources are a 1:1 between a specific
    role, object and privilege. So adding three privileges to the
    `dev_role` will require three Terraform resources which can can be
    accomplished with the `for_each` meta-argument.

    </div>

2.  We will run Terraform to grant these privileges on the `dev_table`
    table.

    <div class="highlight">

    ``` chroma
    terraform apply
    ```

    </div>

3.  We can now check the privileges on our table again

    <div class="highlight">

    ``` chroma
    SELECT name, privileges FROM mz_tables WHERE name = 'dev_table';
    ```

    </div>

    ```
    name|privileges
    dev_table|{u1=arwd/u1,u8=arw/u1}
    (1 row)
    ```

4.  Now we will include the additional grants for the schema, database
    and cluster.

    <div class="highlight">

    ``` chroma
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

    </div>

5.  Run Terraform again to grant these additional privileges on the
    database, schema and cluster.

    <div class="highlight">

    ``` chroma
    terraform apply
    ```

    </div>

## Step 4. Assign the role to a user

The dev_role now has the acceptable privileges it needs. Let’s apply
this role to a user in your Materialize organization.

1.  Include a Terraform resource that grants the role we have created in
    our Terraform project to a Materialize user.

    <div class="highlight">

    ``` chroma
    resource "materialize_role_grant" "dev_role_grant_user" {
      role_name   = materialize_role.dev_role.name
      member_name = "<user>"
    }
    ```

    </div>

2.  Apply our Terraform change.

    <div class="highlight">

    ``` chroma
    terraform apply
    ```

    </div>

3.  To review the permissions a roles, you can view the object data:

    <div class="highlight">

    ``` chroma
    SELECT name, privileges FROM mz_tables WHERE name = 'dev_table';
    ```

    </div>

    The output should return the object ID, the level of permission, and
    the assigning role ID.

    ```
    name|privileges
    dev_table|{u1=arwd/u1,u8=arw/u1}
    (1 row)
    ```

    In this example, role ID `u1` has append, read, write, and delete
    privileges on the table. Object ID `u8` is the `dev_role` and has
    append, read, and write privileges, which were assigned by the `u1`
    user.

## Step 5. Create a second role

Next, you will create a new role with different privileges to other
objects. Then you will apply those privileges to the dev role and alter
or drop privileges as needed.

1.  Create a second role your Materialize account:

    <div class="highlight">

    ``` chroma
    resource "materialize_role" "qa_role" {
      name = "qa_role"
    }
    ```

    </div>

2.  Apply `CREATEDB` privileges to the `qa_role`:

    <div class="highlight">

    ``` chroma
    resource "materialize_grant_system_privilege" "qa_role_system_createdb" {
      role_name = materialize_role.qa_role.name
      privilege = "CREATEDB"
    }
    ```

    </div>

3.  Create a new `qa_db` database:

    <div class="highlight">

    ``` chroma
    resource "materialize_database" "database" {
      name = "dev_db"
    }
    ```

    </div>

4.  Apply `USAGE` and `CREATE` privileges to the `qa_role` role for the
    new database:

    <div class="highlight">

    ``` chroma
    resource "materialize_database_grant" "qa_role_database_grant" {
      for_each = toset(["USAGE", "CREATE"])

      role_name     = materialize_role.qa_role.name
      privilege     = each.value
      database_name = materialize_database.database.name
    }
    ```

    </div>

## Step 6. Add inherited privileges

Your `dev_role` also needs access to `qa_db`. You can apply these
privileges individually or you can choose to grant the `dev_role` the
same permissions as the `qa_role`.

1.  Add `dev_role` as a member of `qa_role`:

    <div class="highlight">

    ``` chroma
    resource "materialize_role_grant" "qa_role_grant_dev_role" {
      role_name   = materialize_role.qa_role.name
      member_name = materialize_role.dev_role.name
    }
    ```

    </div>

2.  We will run Terraform to grant these the inherited privileges.

    <div class="highlight">

    ``` chroma
    terraform apply
    ```

    </div>

3.  Review the privileges of `qa_role` and `dev_role`:

    <div class="highlight">

    ``` chroma
    SELECT name, privileges FROM mz_databases WHERE name='qa_db';
    ```

    </div>

    Your output will be similar to the example below:

    ```
    name|privileges
    qa_db|{u1=UC/u1,u9=UC/u1}
    (1 row)
    ```

    Both `dev_role` and `qa_role` have usage and create access to the
    `qa_db`. In the next section, you will edit role attributes for
    these roles and drop privileges.

## Step 7. Revoke privileges

You can revoke certain privileges for each role, even if they are
inherited from another role.

1.  Remove the resource
    `materialize_database_grant.qa_role_database_grant_create` from the
    Terraform project.

2.  We will run Terraform to revoke privileges.

    <div class="highlight">

    ``` chroma
    terraform apply
    ```

    </div>

    Because Terraform is responsible for maintaining the state of our
    project, removing this grant resource and running an `apply` is the
    equivalent of running a revoke statement:

    <div class="highlight">

    ``` chroma
    REVOKE CREATE ON DATABASE dev_table FROM dev_role;
    ```

    </div>

## Next steps

To destroy the roles and objects you created, you can remove all
resources from your Terraform project. Running a `terraform apply` will
`DROP` all objects.

## Related pages

For more information on RBAC in Materialize, review the reference
documentation:

- [`GRANT ROLE`](https://materialize.com/docs/sql/grant-role/)
- [`CREATE ROLE`](https://materialize.com/docs/sql/create-role/)
- [`GRANT PRIVILEGE`](https://materialize.com/docs/sql/grant-privilege/)
- [`ALTER ROLE`](https://materialize.com/docs/sql/alter-role/)
- [`REVOKE PRIVILEGE`](https://materialize.com/docs/sql/revoke-privilege/)
- [`DROP ROLE`](https://materialize.com/docs/sql/drop-role/)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/manage/access-control/rbac-terraform-tutorial.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
