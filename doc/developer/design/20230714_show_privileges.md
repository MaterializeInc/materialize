# SHOW PRIVILEGES

Associated:

- [GitHub issue](https://github.com/MaterializeInc/materialize/issues/20452)
- [RBAC design doc](20230216_role_based_access_control.md)
- [System privilege design doc](20230531_system_privileges.md)
- [Default privilege design doc](20230602_default_privileges.md)

## Context

Role based access control comprises a set of features that allow customers to control the
privileges of individual users. Privileges can be granted to a user which allows them to take
certain actions in a Materialize deployment. All objects have an owner, and certain actions are
limited to only object owners. Roles can be members of other roles and inherit all of their
privileges. Privileges, owners, and role membership are all stored within the database catalog.

Currently, it's difficult for a user to discover RBAC related information.

## Goals

- Make it easy for users to discover the set of privileges that they have.
- Make it easy for users to discover the owner of objects.
- Make it easy for users to discover the roles that they are a member of.

## Overview

We will add the following SQL commands:

- `SHOW PRIVILEGES`: shows the privileges of the current user.
- `SHOW DEFAULT PRIVILEGES`: shows default privileges.
- `SHOW ROLE MEMBERSHIPS`

We will also add an `owner` column to the existing show commands.

## Detailed description

### `SHOW PRIVILEGES`

Will show object privileges.

The syntax will be: `SHOW [<object-type>] PRIVILEGES [FOR {<role> | ALL ROLES}] [FROM {SCHEMA <schema-name> | ALL SCHEMAS | DATABASE <database> | ALL DATABASES}]`.

- `<object-type>` will filter the output to only include objects of type `<object-type>`. Valid
values are:
  - `OBJECT`
  - `DATABASE`
  - `SCHEMA`
  - `CLUSTER`
  - `SYSTEM`
- `FOR <role>` will filter the output to only include privileges granted to `<role>`.
  - If a role or `ALL ROLES` is not specified then the current role is assumed.
- `FOR ALL ROLES` will display privileges granted to all roles.
- `FROM SCHEMA <schema-name>` will filter the output to exclude items not in schema `<schema-name>`.
  - This has no effect on database, schema, cluster, or system privileges.
  - If a schema or `ALL SCHEMAS` is not specified then the active schema is assumed.
  - Invalid if an object type of `DATABASE`, `SCHEMA`, `CLUSTER`, or `SYSTEM` has been specified.
- `FROM ALL SCHEMAS` will include all items.
  - Invalid if an object type of `DATABASE`, `SCHEMA`, `CLUSTER`, or `SYSTEM` has been specified.
- `FROM DATABASE <database>` will filter the output to exclude schemas not in database `<database>`.
  - This has no effect on item, database, cluster, or cluster privileges.
  - If a database or `ALL DATABASES` is not specified then the active database is assumed.
  - Invalid if an object type of `DATABASE`, `CLUSTER`, or `SYSTEM` has been specified. 
- `FROM ALL DATABASES` will include all schemas.
  - Invalid if an object type of `DATABASE`, `CLUSTER`, or `SYSTEM` has been specified. 

The output will include the following columns:

- `database: text` - The database of the object. `NULL` for databases and clusters.
- `schema: text` - The schema of the object. `NULL` for databases, schemas, and clusters.
- `name: text` - The name of the object.
- `object_type: text` -  The type of the object.
- `privileges: text` - Human readable version of the privileges that held by the current session.

Here are some example queries:

```sql
SHOW PRIVILEGES;

 database    | schema | name        | object_type | privileges
-------------+--------+-------------+-------------+----------------
 materialize | public | my_table    | table       | SELECT, INSERT
 materialize | public | my_view     | view        | SELECT
 materialize | NULL   | public      | schema      | USAGE
 NULL        | NULL   | materialize | database    | USAGE
 NULL        | NULL   | my_db       | database    | USAGE
 NULL        | NULL   | my_cluster  | cluster     | CREATE
```

```sql
SHOW PRIVILEGES FOR ROLE ceo;

 database    | schema | name        | object_type | privileges
-------------+--------+-------------+-------------+--------------------------------
 materialize | public | my_table    | table       | SELECT, INSERT, UPDATE, DELETE
 materialize | NULL   | public      | schema      | CREATE, USAGE
 NULL        | NULL   | materialize | database    | CREATE, USAGE
 NULL        | NULL   | my_db       | database    | CREATE, USAGE
 NULL        | NULL   | my_cluster  | cluster     | CREATE, USAGE
```

```sql
SHOW OBJECT PRIVILEGES;

 database    | schema | name       | object_type | privileges
-------------+--------+------------+-------------+----------------
 materialize | public | my_table   | table       | SELECT, INSERT
 materialize | public | my_view    | view        | SELECT
```

```sql
SHOW PRIVILEGES FROM SCHEMA qa;

 database    | schema | name        | object_type | privileges
-------------+--------+-------------+-------------+----------------
 materialize | qa     | my_secret   | secret      | USAGE
 materialize | NULL   | public      | schema      | USAGE
 NULL        | NULL   | materialize | database    | USAGE
 NULL        | NULL   | my_db       | database    | USAGE
 NULL        | NULL   | my_cluster  | cluster     | CREATE
```

```sql
SHOW OBJECT PRIVILEGES FROM SCHEMA public;

 database    | schema | name       | object_type | privileges
-------------+--------+------------+-------------+----------------
 materialize | public | my_table   | table       | SELECT, INSERT
 materialize | public | my_view    | view        | SELECT
```

```sql
SHOW PRIVILEGES FROM DATABASE db2;

 database    | schema | name        | object_type | privileges
-------------+--------+-------------+-------------+----------------
 db2         | NULL   | my_schema   | schema      | CREATE
 NULL        | NULL   | materialize | database    | USAGE
 NULL        | NULL   | my_db       | database    | USAGE
 NULL        | NULL   | my_cluster  | cluster     | CREATE
```

```sql
SHOW PRIVILEGES FROM ALL SCHEMAS;

 database    | schema | name        | object_type | privileges
-------------+--------+-------------+-------------+----------------
 materialize | public | my_table    | table       | SELECT, INSERT
 materialize | public | my_view     | view        | SELECT
 materialize | qa     | my_secret   | secret      | USAGE
 materialize | NULL   | public      | schema      | USAGE
 NULL        | NULL   | materialize | database    | USAGE
 NULL        | NULL   | my_db       | database    | USAGE
 NULL        | NULL   | my_cluster  | cluster     | CREATE
```

```sql
SHOW PRIVILEGES FROM ALL DATABASES;

 database    | schema | name        | object_type | privileges
-------------+--------+-------------+-------------+----------------
 materialize | public | my_table    | table       | SELECT, INSERT
 materialize | public | my_view     | view        | SELECT
 materialize | NULL   | public      | schema      | USAGE
 db2         | NULL   | my_schema   | schema      | CREATE
 NULL        | NULL   | materialize | database    | USAGE
 NULL        | NULL   | my_db       | database    | USAGE
 NULL        | NULL   | my_cluster  | cluster     | CREATE
```

### `SHOW DEFAULT PRIVILEGES`

Will show all default privileges

The syntax will be `SHOW DEFAULT [<object-type>] PRIVILEGES [ON <object-type>]`.

- `<object-type>` will filter the output to only include objects of type `<object-type>`. Valid
values are:
  - `OBJECT`
  - `DATABASE`
  - `SCHEMA`
  - `CLUSTER`
  - `SYSTEM`

The output will include the following columns:

- `role: text` - The name of the affected role.
- `database: text` - The name of the affected database.
- `schema: text` - The name of the affected schema.
- `object_type: text` - The type of object.
- `grantee: text` - The role being granted privileges.
- `privileges: text` - Human readable version of the privileges that will be granted.

Here are some example queries:

```sql
SHOW DEFAULT PRIVILEGES;

 role   | database    | schema | object_type | grantee   | privileges
--------+-------------+--------+-------------+-----------+----------------
 PUBLIC | NULL        | NULL   | table       | dev       | INSERT, SELECT
 joe    | materialize | NULL   | table       | PUBLIC    | SELECT
 qa     | materialize | public | type        | scientist | USAGE
 PUBLIC | NULL        | NULL   | cluster     | mike      | CREATE
```

```sql
SHOW DEFAULT OBJECT PRIVILEGES ON TABLES;

 role   | database    | schema | object_type | grantee   | privileges
--------+-------------+--------+-------------+-----------+----------------
 PUBLIC | NULL        | NULL   | table       | dev       | INSERT, SELECT
 joe    | materialize | NULL   | table       | PUBLIC    | SELECT
 qa     | materialize | public | type        | scientist | USAGE
```

### `SHOW ROLE MEMBERSHIPS`

Will show the role membership of the current session.

The syntax will be: `SHOW ROLE MEMBERSHIPS [FOR {<role> | ALL ROLES}]`.

- `FOR <role>` will filter the output to only include roles that `<role>` is a member of directly
or indirectly.
  - If a role or `ALL ROLES` is not specified then the current role is assumed.
- `FOR ALL ROLES` will display all role memberships.

The output will include the following columns:

- `role_name: text` - Name of the role.
- `member_name: text` - Name of a role that is a member of `role_name`.

Here is an example query:

```sql
SHOW ROLE MEMBERSHIPS;

 role_name      | member_name
----------------+-------------
 data_scientist | joe
 dev            | mike
 qa             | arjun
```

### `SHOW <OBJECTS>`

The following columns will be added to all existing `SHOW <OBJECTS>` commands:

- `owner: text` the role name of the object owner.

## Alternatives

- Create documentation with queries against catalog tables that would return the equivalent results
  as the `SHOW` commands.
- Add a column to `SHOW PRIVILEGES` to indicate which role a privilege comes from.
- Don't limit the results of `SHOW PRIVILEGES` to a single schema.
- Split `SHOW PRIVILEGES` into multiple commands:
  - `SHOW ITEM PRIVILEGES`
  - `SHOW SCHEMA PRIVILEGES`
  - `SHOW DATABASE PRIVILEGES`
  - `SHOW CLUSTER PRIVILEGES`

## Open questions

- How should we filter `SHOW PRIVILEGES`?
- Do we want to include the grantor in the output?
