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
- `SHOW ROLE MEMBERSHIP`

We will also add an `owner` column to the existing show commands.

## Detailed description

- We will add an internal unmaterializable function, `mz_internal.mz_role_membership() -> text[]`,
  that returns an array of all the role IDs that the session's current role is a member of.
- We will add an internal unary function, `mz_internal.mz_format_privileges(text) -> text`,
  that accepts a privilege string descriptor and returns a human-readable version. For
  example: `mz_internal.mz_format_privileges('ar') = 'INSERT, SELECT'`.

These two functions will help implement the `SHOW` commands described below.

### `SHOW PRIVILEGES`

Will show all privileges held by the current session.

The syntax will be: `SHOW PRIVILEGES [ON <object-type>] [FROM <schema-name>]`.

- `ON <object-type>` will filter the output to only include objects of type `<object-type>`. Valid
  values are:
    - `TABLES`
    - `VIEWS`
    - `MATERIALIZED VIEWS`
    - `SOURCES`
    - `SINKS`
    - `TYPES`
    - `CLUSTERS`
    - `SECRETS`
    - `CONNECTIONS`
    - `DATABASES`
    - `SCHEMAS`
    - `SYSTEM`
- `FROM <schema-name>` will filter the output to only exclude objects in a schema other `<schema-name>`.
    - If specified, top level objects, such as database, schemas, and clusters are not included.
    - If not specified then objects are limited to the current schema and top level objects are
    - shown.

The output will include the following columns:

- `name: text` The name of the object.
- `object_type: text` The type of the object.
- `privileges: text` Human readable version of the privileges that held by the current session.

Here are some example queries:

```sql
SHOW PRIVILEGES;

 name       | object_type | privileges
------------+-------------+----------------
 my_table   | TABLE       | SELECT, INSERT
 my_view    | VIEW        | SELECT
 my_cluster | CLUSTER     | CREATE
```

```sql
SHOW PRIVILEGES ON TABLES;

 name       | object_type | privileges
------------+-------------+----------------
 my_table   | TABLE       | SELECT, INSERT
```

```sql
SHOW PRIVILEGES FROM public;

 name       | object_type | privileges
------------+-------------+----------------
 my_table   | TABLE       | SELECT, INSERT
 my_view    | VIEW        | SELECT
```

```sql
SHOW PRIVILEGES ON VIEWS FROM public;

 name       | object_type | privileges
------------+-------------+----------------
 my_view    | VIEW        | SELECT
```

### `SHOW DEFAULT PRIVILEGES`

Will show all default privileges

The syntax will be `SHOW DEFAULT PRIVILEGES [ON <object-type>]`.

- `ON <object-type>` will filter the output to only include objects of type `<object-type>`. Valid
  values are:
  - `TABLES`
  - `VIEWS`
  - `MATERIALIZED VIEWS`
  - `SOURCES`
  - `SINKS`
  - `TYPES`
  - `CLUSTERS`
  - `SECRETS`
  - `CONNECTIONS`
  - `DATABASES`
  - `SCHEMAS`

The output will include the following columns:

- `role: text` The name of the affected role.
- `database: text` The name of the affected database.
- `schema: text` The name of the affected schema.
- `object_type: text` The type of object.
- `grantee: text` The role being granted privileges.
- `privileges: text` Human readable version of the privileges that will be granted.

Here are some example queries:

```sql
SHOW DEFAULT PRIVILEGES;

 role   | database    | schema | object_type | grantee   | privileges
--------+-------------+--------+-------------+-----------+----------------
 PUBLIC | NULL        | NULL   | TABLE       | dev       | INSERT, SELECT
 joe    | materialize | NULL   | VIEW        | PUBLIC    | SELECT
 qa     | materialize | public | TYPE        | scientist | USAGE
```

```sql
SHOW DEFAULT PRIVILEGES ON TABLES;

 role   | database    | schema | object_type | grantee   | privileges
--------+-------------+--------+-------------+-----------+----------------
 PUBLIC | NULL        | NULL   | TABLE       | dev       | INSERT, SELECT
```

### `SHOW ROLE MEMBERSHIP`

Will show the role membership of the current session.

The syntax will be: `SHOW ROLE MEMBERSHIP`.

The output will include the following columns:

- `name: text` The name of the role.

Here is an example query:

```sql
SHOW ROLE MEMBERSHIP;

 name
----------------
 data_scientist
 dev
 qa
```

### `SHOW <OBJECTS>`

The following columns will be added to all existing `SHOW <OBJECTS>` commands:

- `owner: text` the role name of the object owner.

## Alternatives

- Create documentation with queries against catalog tables that would return the equivalent results
  as the `SHOW` commands.
- Add syntax, to specify the database instead of the schema in `SHOW PRIVILEGES`.
- Return a list from `mz_internal.mz_role_membership()` instead of an array.
- Add a column to `SHOW PRIVILEGES` to indicate which role a privilege comes from.
- Don't limit the results of `SHOW PRIVILEGES` to a single schema.

## Open questions

- None.
