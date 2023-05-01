# Role Based Access Control (RBAC)

## Summary

RBAC places restrictions on what actions a user can do based on the privileges granted to that user. PostgreSQL has a
rich and well tested RBAC design and implementation, which we will base our implementation off of.

## Goals

- Allow users to create restrictions around who is allowed to do what.

## Non-Goals

- Data governance.

## Description

If you want to skim this doc, then skip to the [Phase 1 - Attributes](#phase-1---attributes) section and only look at
the subsection headers, tables, and SQL statements.

### Existing RBAC Features in Materialize

- Creating/Deleting roles
    - We currently require clients to specify the `LOGIN` and `SUPERUSER` attributes when creating a role.
    - We support parsing `NOLOGIN` and `NOSUPERUSER` attributes, but will fail the query if those are present.
    - `CREATE ROLE <role-name> LOGIN SUPERUSER`.
    - `DROP ROLE <role-name>`.
    - We support `CREATE USER` as is treated as an alias for `CREATE ROLE LOGIN`.
- `mz_roles`: catalog table that stores role names, id, and oid.
- When a new user connects, a new role is automatically created for them with `LOGIN` and `SUPERUSER`.

### PostgreSQL Background

#### Docs

- [Roles](https://www.postgresql.org/docs/15/user-manag.html)
- [Privileges](https://www.postgresql.org/docs/15/ddl-priv.html)
- [Rules and Privileges](https://www.postgresql.org/docs/15/rules-privileges.html)
- [Schemas and Privileges](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PRIV)
- [`CREATE ROLE`](https://www.postgresql.org/docs/15/sql-createrole.html)
- [`DROP ROLE`](https://www.postgresql.org/docs/15/sql-droprole.html)
- [`ALTER ROLE`](https://www.postgresql.org/docs/15/sql-alterrole.html)
- [`GRANT`](https://www.postgresql.org/docs/15/sql-grant.html)
- [`REVOKE`](https://www.postgresql.org/docs/15/sql-revoke.html)
- [`SET ROLE`](https://www.postgresql.org/docs/15/sql-set-role.html)
- [`REASSIGN OWNED`](https://www.postgresql.org/docs/15/sql-reassign-owned.html)
- [`DROP OWNED`](https://www.postgresql.org/docs/15/sql-drop-owned.html)

#### Implementation

- [`acl.h`](https://github.com/postgres/postgres/blob/master/src/include/utils/acl.h)
- [`acl.c`](https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/acl.c)
- [`aclchk_internal.h`](https://github.com/postgres/postgres/blob/master/src/include/utils/aclchk_internal.h)
- [`aclcheck.c`](https://github.com/postgres/postgres/blob/master/src/backend/catalog/aclchk.c)

- Note: There may be more, but this is a good starting point.

### Phase 1 - Attributes

See [Role Attributes](https://www.postgresql.org/docs/current/role-attributes.html) for all PostgreSQL attributes.

Attributes belong to a role and describe what that role is allowed to do in the system. They are independent of any
particular object. We will support the following attributes:

| Name                      | Option Name     | Description                                                                                                                                           | From PostgreSQL |
|---------------------------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|
| database creation         | `CREATEDB`      | Can create a database.                                                                                                                                | Yes             |
| role creation             | `CREATEROLE`    | Can create, alter, drop, grant membership to, and revoke membership from roles.                                                                       | Yes             |
| inheritance of privileges | `INHERIT`       | Can inherit the privileges of roles that it is a member of. On by default. For this project we can keep this as a mandatory attribute.                | Yes             |
| cluster creation          | `CREATECLUSTER` | Can create a cluster.                                                                                                                                 | No              |

These attributes will be added to the attributes accepted by the `CREATE ROLE` statement.

We will add the following SQL statement:

- `ALTER ROLE <role_name> [ WITH ] <option> [ ... ]`
    - `<role_name>` is the name of the role to alter.
    - `<option>`: is one of the Option Name’s from above OR one of the option names from above with a `NO` prepended (
      ex: `NOLOGIN`). An option without a `NO` grants the attribute, an option with a `NO` revokes the attribute. All
      unmentioned attributes are left unchanged.
    - Anyone with the `CREATEROLE` attribute can run this on any other role
        - `SUPERUSER` attribute is required to change the `SUPERUSER` attribute on another role.
        - `SUPERUSER` can run this without `CREATEROLE`.
    - `WITH` is ignored.

When a new user logs in, as long as they were successfully authenticated through an external port, we will create a new
role for them with only the `INHERIT` attribute. This will also allow us to delete the `materialize` role without
breaking local development.

We will also support the following session specific attributes:

| Name                      | Option Name     | Description                                                                                                                                           | From PostgreSQL |
|---------------------------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|
| login privilege           | `LOGIN`         | Roles with this attribute can establish a database connection.                                                                                        | Yes             |
| superuser status          | `SUPERUSER`     | Can bypass all permission checks, except login.                                                                                                       | Yes             |

These attributes are derived everytime a user tries to log in and only lasts as long as the session is active. You
cannot specify these attributes in `CREATE ROLE` or `ALTER ROLE`. We use the following logic at every login:

- When you log in to a role and are successfully authenticated, then you implicitly get the `LOGIN` attribute for that
  session.
- When you log in to a role with Frontegg and your JWT has the "Organization Admin" role, you implicitly get
  the `SUPERUSER` attribute for that session.
    - The `SUPERUSER` attribute will be periodically updated as part of the periodic Frontegg re-validation done for
      pgwire connections.
    - The `mz_system` role will always have the `SUPERUSER` attribute.

This differs from PostgreSQL, which treats these as normal role attributes that persists between sessions and can be
specified in `CREATE ROLE` and `ALTER ROLE`.

We will add the following read-only session parameter:
`IS_SUPERUSER`: True if the current role has superuser privileges.

#### Implementation Details

- Each attribute will be added as a column to `mz_roles` with boolean values.
- Attributes will be checked before operations in the sequencer.

#### Out of Scope for Phase

- `IN ROLE`, `IN GROUP` options for `CREATE ROLE`.
- `ROLE`, `USER` options for `CREATE ROLE`.

#### Out of Scope for Project

- The following attributes:
    - password
    - bypassing row-level security
    - connection limit
- The following `CREATE ROLE` options:
    - `VALID UNTIL`
    - `SYSID`
    - `ADMIN`
- `ALTER ROLE RENAME`
- `ALTER ROLE SET`
- `ALTER ROLE RESET`
- `CURRENT_ROLE`, `CURRENT_USER`, `SESSION_USER` aliases for `<role_name>` in `ALTER ROLE`.

### Phase 2 - Role Membership

See [Role Membership](https://www.postgresql.org/docs/current/role-membership.html) for PostgreSQL role membership.

Role membership involves the ability of one role to be a member of another role. A role inherits all the privileges (not
attributes) of the roles it is a member of, unless `NOINHERIT` is set. Even if `NOINHERIT` is set, you can still use the
privileges of that role via `SET ROLE`. We will add the following SQL commands:

- `GRANT <group_role> TO [ GROUP ] <role>`
    - Adds `<role>` as a member of `<group_role>`.
    - Any role with the `CREATEROLE` attribute OR superusers can grant membership to any other role.
        - `CREATEROLE` roles cannot grant roles with `SUPERUSER`.
        - Note: PostgreSQL allows other roles to grant membership with the `WITH ADMIN OPTION` option. We will leave
          this as future work.
    - Circular memberships are NOT allowed.
    - `GROUP` is ignored.
- `REVOKE <group_role> FROM [ GROUP ] <role>`
    - Removes `<role>` as a member from `<group_role>`.
    - Any role with the `CREATEROLE` attribute OR superusers can revoke membership from any other member.
        - `CREATEROLE` roles cannot revoke roles with `SUPERUSER`.
        - Note: PostgreSQL allows other roles to grant membership with the `WITH ADMIN OPTION` option. We will leave
          this as future work.
    - `GROUP` is ignored.

We will modify `DROP ROLE <role_name>` so that when `<role_name>` is dropped, all other roles have their membership
revoked.

We will add the following functions:

- `current_role()`
    - Returns the current role of the session.
- `current_user()`
    - Alias for `current_role()`.
- `session_user()`
    - Returns the role that initiated the database connection.

NOTE: Since we won't support `SET ROLE` yet, these functions will all behave identically.

#### Implementation Details

- The catalog will store role membership in a new table called `mz_auth_members` modeled
  after [`pg-auth-members`](https://www.postgresql.org/docs/current/catalog-pg-auth-members.html), which has the
  following columns:
    - `roleid`: `RoleId`
    - `member`: `RoleId`
    - `grantor`: `RoleId`
- When attributes are checked in the sequencer, we will check the attributes of all roles that the current role is a
  member of.

#### Out of Scope for Phase

- `GRANT` privileges.
- `REVOKE` privileges.
- `PUBLIC` alias for `<role>` in `GRANT` and `REVOKE`.
- The following options to `CREATE ROLE`:`IN ROLE`,`IN GROUP`,`ROLE`,`USER`.

#### Out of Scope for Project

- `CASCADE` and `RESTRICT` options in `REVOKE`.
- `CURRENT_ROLE`, `CURRENT_USER`, and `SESSION_USER` aliases for `<role>` in `GRANT` and `REVOKE`.
- `GRANTED BY` option for `GRANT` and `REVOKE`.
- `[ WITH ADMIN OPTION ]` option for `GRANT`.
- `[ADMIN OPTION FOR ]` option for `REVOKE`.
- `SET ROLE`
- `RESET ROLE`

### Phase 3 - `PUBLIC` role

See [Grant](https://www.postgresql.org/docs/current/sql-grant.html) for PostgreSQL `PUBLIC` details (grep for PUBLIC).

`PUBLIC` is a special keyword that is accepted in some locations where a role name would be accepted. The key word
PUBLIC indicates that the changes are to be applied to all roles, including those that might be created later. `PUBLIC`
is not allowed in `CREATE ROLE`, `ALTER ROLE`, `DROP ROLE`, `GRANT <group_role> TO <role>`,
and `REVOKE <group_role> FROM <role>`.

#### Implementation Details

- A new variant to `RoleId` will be added called `Public`.
- The `PUBLIC` role will be added to the catalog with an id of `RoleId::Public`.
- The sequencer/catalog will reject the `RoleId::Public` variant in all disallowed operations.

### Phase 4 - Ownership

See [Privileges](https://www.postgresql.org/docs/current/ddl-priv.html) for PostgreSQL ownership.

When any object is created, the creating role is assigned as the owner of that object. Only the
owner (and superusers) can destroy or alter that object.

Below is a summary of the default ownership of all builtin objects:

- The `mz_system` cluster will be owned by the `mz_system` role.
- The `mz_introspection` cluster will be owned by the `mz_system` role.
- The `default` cluster will be owned by the `mz_system` role.
- The `materialize` database will be owned by the `mz_system` role.
- The `materialize.public` schema will be owned by the `mz_system` role.
- The `mz_system` role will own all catalog schemas [`pg_catalog`, `mz_catalog`, `mz_internal`, `information_schema`].

We will add the following SQL commands:

- `ALTER <object_type> <object_name> OWNER TO <new_owner>`
    - Transfers ownership of `<object_name>` to `<new_owner>`.
    - Can only be run by the current owner (or member of owning role) or a superuser.
    - Requires membership of `<new_owner>`.

The owner of an index is always kept in-sync with the owner of the underlying relation. Trying to
alter the owner of an index will return successfully with a warning, but will not actually change
the owner of the index. This is for PostgreSQL compatibility reasons.

We will update `DROP ROLE` so that roles cannot be dropped unless no objects are owned by the role.

#### Implementation Details

- The following catalog tables/views will have an additional column called "owner_id" of type string. It will be the
  role ID of the owner of the object. Additionally, the corresponding stash collections will have an "owner_id"
  field.
    - mz_sinks
    - mz_indexes
    - mz_connections
    - mz_types
    - mz_functions
    - mz_secrets
    - mz_tables
    - mz_sources
    - mz_views
    - mz_materialized_views
    - mz_databases
    - mz_clusters
    - mz_cluster_replica
    - mz_schemas
    - mz_relations
    - mz_objects
- Ownership will be checked before operations in the sequencer.

#### Out of Scope for Phase

- `REASSIGN OWNED`
- `DROP OWNED`

### Phase 5 - Privileges

See [Privileges](https://www.postgresql.org/docs/current/ddl-priv.html) for PostgreSQL privileges.

Roles can be granted and revoked certain privileges on objects, that allow them to perform some action with that object.

We will support the following privileges:

| Privilege | Description                                                              | Abbreviation | Applicable Object Types                       | From PostgreSQL |
|-----------|--------------------------------------------------------------------------|--------------|-----------------------------------------------|-----------------|
| `SELECT`  | Allows reading rows from an object.                                      | r(”read”)    | Table, View, Materialized View, Source        | Yes             |
| `INSERT`  | Allows inserting into an object.                                         | a(”append”)  | Table                                         | Yes             |
| `UPDATE`  | Allows updating an object (requires SELECT if a read is necessary).      | w(”write”)   | Table                                         | Yes             |
| `DELETE`  | Allows deleting from an object (requires SELECT if a read is necessary). | d            | Table                                         | Yes             |
| `CREATE`  | Allows creating a new object within another object.                      | C            | Database, Schema, Cluster                     | Yes             |
| `USAGE`   | Allows using an object or looking up members of an object.               | U            | Database, Schema, Connection, Secret, Cluster | Yes             |

We will support the following object types:

| Object Type          | All Privileges | From PostgreSQL |
|----------------------|----------------|-----------------|
| `DATABASE`           | UC             | Yes             |
| `SCHEMA`             | UC             | Yes             |
| `TABLE`              | arwd           | Yes             |
| `VIEW`               | r              | Yes             |
| `MATERIALIZED  VIEW` | r              | Yes             |
| `INDEX`              |                | Yes             |
| `TYPE`               | U              | Yes             |
| `SOURCE`             | r              | No              |
| `SINK`               |                | No              |
| `CONNECTION`         | U              | No              |
| `SECRET`             | U              | No              |
| `CLUSTER`            | UC             | No              |

The object owner is given all privileges on an object by default, though these privileges can be
revoked.

PostgreSQL allows arwd privileges on all table like objects (view, materialized view, etc.) even though they aren't
useful. We remove privileges that don't make sense.

Below is a summary of the default privileges of all builtin objects:

- The `mz_introspection` role will have `UC` privileges on the `mz_introspection` cluster.
- The `PUBLIC` pseudo-role will have `U` privileges on the `mz_introspection` cluster.
- The `PUBLIC` psuedo-role will have `U` privileges on the `default` cluster.
- The `PUBLIC` psuedo-role will have `U` privileges on the default `materialize` database.
- The `PUBLIC` psuedo-role will have `U` privileges on all `public` schemas (the default schema
  created in every database).
- The `PUBLIC` pseudo-role will have `U` privileges on all catalog schemas.
- The `PUBLIC` pseudo-role will have `r` privileges on all applicable objects within all catalog
  schemas.
- The `PUBLIC` psuedo-role will have `U` privileges on all types.

Here is a summary of all the privileges, attributes, and ownership needed to perform certain actions:

| Operation                             | Privilege, Attribute, and OwnerShip                                               |
|---------------------------------------|-----------------------------------------------------------------------------------|
| `ALTER <item>`                        | Ownership of the item, `SCHEMA(C)`                                                |
| `ALTER DATABASE`                      | Ownership of the database, `CREATEDB`                                             |
| `ALTER SCHEMA`                        | Ownership of the schema, `DATABASE(C)`                                            |
| `ALTER CLUSTER`                       | Ownership of the cluster, `CREATECLUSTER`                                         |
| `ALTER CLUSTER REPLICA`               | Ownership of the replica, `CLUSTER(C)`                                            |
| `ALTER ROLE`                          | `CREATEROLE`                                                                      |
| `CREATE CLUSTER`                      | `CREATECLUSTER`                                                                   |
| `CREATE CLUSTER REPLICA`              | `CLUSTER(C)`                                                                      |
| `CREATE SECRET`                       | `SCHEMA(C)`                                                                       |
| `CREATE {TABLE, TYPE, VIEW}`          | `SCHEMA(C)`, sometimes `TYPE(U)`                                                  |
| `CREATE CONNECTION`                   | `SCHEMA(C)`, sometimes `SECRET(U)`, `CONNECTION(U)`                               |
| `CREATE DATABASE`                     | `CREATEDATABASE`                                                                  |
| `CREATE MATERIALIZED VIEW`            | `SCHEMA(C)`, `CLUSTER(C)`, sometimes `TYPE(U)`                                    |
| `CREATE INDEX`                        | `SCHEMA(C)`, `CLUSTER(C)`, Ownership of relation, sometimes `TYPE(U)`             |
| `CREATE SOURCE`                       | `SCHEMA(C)` sometimes `CLUSTER(U)`, `CONNECTION(U)`, `CREATECLUSTER`              |
| `CREATE SINK`                         | `SCHEMA(C)`, `OBJECT(r)` sometimes `CLUSTER(U)`, `CONNECTION(U)`, `CREATECLUSTER` |
| `CREATE ROLE`                         | `CREATEROLE`                                                                      |
| `CREATE SCHEMA`                       | `DATABASE(C)`                                                                     |
| `DROP <item>`                         | Ownership of the item, `SCHEMA(U)`                                                |
| `DROP DATABASE`                       | Ownership of the database                                                         |
| `DROP SCHEMA`                         | Ownership of the schema, `DATABASE(U)`                                            |
| `DROP CLUSTER`                        | Ownership of the cluster                                                          |
| `DROP CLUSTER REPLICA`                | Ownership of the replica, `CLUSTER(U)`                                            |
| `DROP ROLE`                           | `CREATEROLE`                                                                      |
| `{INSERT INTO ... VALUES, COPY FROM}` | `OBJECT(a)`, sometimes `TYPE(U)`                                                  |
| `INSERT INTO ... SELECT`              | `OBJECT(a)` usually `CLUSTER(U)`, `OBJECT(r)`, `SCHEMA(U)`, sometimes `TYPE(U)`   |
| `DELETE`                              | `OBJECT(d)` usually `CLUSTER(U)`, `OBJECT(r)`, `SCHEMA(U)`, sometimes `TYPE(U)`   |
| `UPDATE`                              | `OBJECT(w)` usually `CLUSTER(U)`, `OBJECT(r)`, `SCHEMA(U)`, sometimes `TYPE(U)`   |
| `{SELECT, SHOW, SUBSCRIBE}`           | usually `CLUSTER(U)`, `OBJECT(r)`, `SCHEMA(U)`, sometimes `TYPE(U)`               |
| `EXPLAIN`                             | usually `OBJECT(r)`, `SCHEMA(U)`, sometimes `TYPE(U)`                             |
| `SHOW CREATE`                         | `SCHEMA(U)`                                                                       |

Superusers can do anything in the above table.

In order to execute a read from a view/materialized view, the role needs `r` permission on the
view/materialized view and the view owner needs the required permissions to perform the query in
the view definition.

When creating a sink, Materialize will check the users privileges only at the time of sink
creation. If the user loses privileges, the sink will keep running an potentially exposes
information that the user no longer has access to.

We will add the following SQL commands:

- `GRANT <privilege> ON <object-type> <object> TO [ GROUP ] <role>`
    - Gives `<privilege>` on `<object>` to `<role>`.
    - Only the owner of `<object>` can grant privileges on it.
        - Note: PostgreSQL allows other roles to grant privileges with the `WITH GRANT OPTION` option.
    - `GROUP` is ignored.
    - For tables, views, materialized, and source, the object type can be omitted.
    - For views, materialized views, and sources, the object type must be `TABLE` if not omitted.
- `GRANT ALL [ PRIVILEGES ] ON <object-type> <object> TO [ GROUP ] <role>`
    - Same as grant above, but for all privileges.
    - `PRIVILEGES` is ignored.
- `REVOKE <privilege> ON <object-type> <object> FROM [ GROUP ] <role>`
    - Revokes `<privilege>` on `<object>` from `<role>`.
    - Only the owner of `<object>` can revoke privileges from it.
        - Note: PostgreSQL allows other roles to revoke privileges with the `WITH GRANT OPTION` option.
    - `GROUP` is ignored.
    - For tables, views, materialized, and source, the object type can be omitted.
    - For views, materialized views, and sources, the object type must be `TABLE` if not omitted.
- `REVOKE ALL [ PRIVILEGES ] ON <object-type> <object> FROM [ GROUP ] <role>`
    - Same as revoke above but for all privileges.
    - `PRIVILEGES` is ignored.

We will update `ALTER <object_type> <object_name> OWNER TO <new_owner>` such that it:

- Requires `CREATE` privilege on the schema where `<object_name>` resides if the object resides in a schema.
  - Rationale is that this is equivalent to `DROP` then `CREATE`.
- Requires `CREATE` privilege on the database where `<object_name>` resides if the object is a database.

We will update `DROP ROLE` so that roles cannot be dropped unless the role contains no privileges.

We will update `DROP <object>` so that it revokes all privileges on `<object>`.

#### Implementation Details

- We will create a new Rust struct called `AclMode` that is a bit flag containing all privileges.
    - See https://github.com/postgres/postgres/blob/3aa961378b4e517908a4400cdc476ca299693de9/src/include/nodes/parsenodes.h#L74-L101
    for PostgreSQL's implementation.
- We will create a new datatype called `maclitem` in the `mz_internal` schema to represent a single privilege.
    - This is modeled after the `aclitem` item in PostgreSQL, see
    https://github.com/postgres/postgres/blob/3aa961378b4e517908a4400cdc476ca299693de9/src/include/utils/acl.h#L48-L59.
    - It will include the following fields:
        - `grantee: RoleId`
        - `grantor: RoleId`
        - `privs: AclMode`
    - We will encode it as text using the following format: `"<grantee>=<privs>/<grantor>"`
        - `"<grantee>"` is the raw RoleId of `grantee`.
            - This will be left empty for the PUBLIC role (to match PostgreSQL).
        - `"<grantor>"` is the raw RoleId of `grantor`.
        - `"<privs>"` is the letter codes of all the granted privileges concatenated together.
        - NOTE: This is not the same as PostgreSQL. PostgreSQL encodes `aclitem` to text using human
        read-able names instead of IDs. We are unable to do this because our encoder does not have
        access to the catalog. We have the same issue with the `regtype` and `regproc` types.
    - We will encode it as binary matching PostgreSQL's binary encoding for `aclitem`, except
    swapping out `oid`s for `RoleId`s.
    - The reason we need a custom type instead of reusing the PostgreSQL's `aclitem` type is because
    `aclitem` uses `oid` types, which Materialize does not use as a persistent identifier.
- We will also add an array type with `maclitem` elements.
- We will add the following operators for `maclitem`:
    - `maclitem = maclitem → boolean`: Are `maclitems` equal?
    - `maclitem[] @> maclitem → boolean`: Does array contain the specified privileges?
- We will support to following casts involving `maclitem`:
    - From `maclitem` to `text`.
- The following catalog table/views will have an additional column called "acl" of type `maclitem[]`.
that stores all privileges belonging to an object.
    - mz_sinks
    - mz_indexes
    - mz_connections
    - mz_types
    - mz_functions
    - mz_secrets
    - mz_tables
    - mz_sources
    - mz_views
    - mz_materialized_views
    - mz_databases
    - mz_clusters
    - mz_cluster_replica
    - mz_schemas
    - mz_relations
    - mz_objects
- Privileges will be checked before operations in the sequencer.

#### Out of Scope for Project

- `GRANTED BY` option for `GRANT` and `REVOKE`.
- `WITH GRANT OPTION` in `GRANT`.
- `GRANT OPTION FOR` in `REVOKE`.
- The following privileges:
    - `TRUNCATE`
    - `REFERENCES`
    - `TRIGGER`
    - `CONNECT`
    - `TEMPORARY`
    - `EXECUTE`
    - `SET`
    - `ALTER SYSTEM`
- The following object types:
    - `DOMAIN`
    - `FUNCTION` or `PROCEDURE`
    - `FOREIGN DATA WRAPPER`
    - `FOREIGN SERVER`
    - `LANGUAGE`
    - `LARGE OBJECT`
    - `PARAMETER`
    - `SEQUENCE`
    - `Table column`
    - `TABLESPACE`
- Adding the necessary pg views to support all role based `psql` meta-commands.

### Phase 6 - Utility Commands/Syntactic Sugar (Optional)

This is an optional phase to add some utility commands and syntactic sugar present in PostgreSQL. We will add the
following SQL commands and options to existing commands:

- `REASSIGN OWNED BY <old_role> TO <new_role>`
    - Transfers ownership of all objects owned by `<old_role>` to `<new_role>`.
    - Can only be run by a member of `<old_role>` and `<new_role>` or a superuser.
    - Requires `CREATE` privilege on all schemas and databases where all objects reside.
        - TODO: This isn't explicitly stated in th docs, but would make sense base on the `ALTER` privileges. I will
          double-check this.
    - In PostgreSQL, this only affects the current database, and does not reassign the database itself. We will diverge
      here and have it affect all databases, including the databases themselves.
- `DROP OWNED BY <name> [ CASCADE | RESTRICT]`
    - Drops all objects owned by `<name>`.
    - Requires membership of `<name>`.
    - In PostgreSQL, this only affects the current database, and does not drop the database itself. We will diverge here
      and have it affect all databases, including the databases themselves.
    - Revokes all privileges granted to `<name>`.
    - Default is `RESTRICT`.
- The following options to `CREATE ROLE`:`IN ROLE`,`IN GROUP`,`ROLE`,`USER`.

#### Out of Scope for Project

- `CURRENT_ROLE`, `CURRENT_USER`, `SESSION_USER`, aliases in `GRANT`, `REVOKE`, `ALTER`, `REASSIGN OWNED`
  and `DROP OWNED`.
- Row level security policies (https://www.postgresql.org/docs/current/ddl-rowsecurity.html). If any catalog object
  contains sensitive information, then all users will be able to read all the contents. As a follow-up project, we can
  implement row level security to prevent this.

## Rollout Plan

A user role called `mz_default_owner` will be created in all existing deployments. A migrations
will assign `mz_default_owner` as the owner of all existing unowned objects. Superusers will be
able to re-assign ownership for all of these objects.

As we develop RBAC we will roll out the following three parameters:

  - A system parameter called "enable_ld_rbac_checks". This parameter will only be able to be set
  by Launch Darkly, no users can toggle this parameter. When this is off, then no user can enable
  RBAC. When this is on, then a user can enable RBAC through one of the other parameters. This acts
  as a global off switch for RBAC in case we ever need to ensure that RBAC is off for a customer.
  - A system parameter called "enable_rbac_checks". This parameter can be set by any
  superuser. When this is on, then RBAC is enabled for all users, as long as
  "enable_ld_rbac_checks" is also on.
  - A session parameter called "enable_session_rbac_checks". This parameter is session local and
  can be set by any user. When this is on, then RBAC is enabled for the current session, as long as
  "enable_ld_rbac_checks" is also on. The intention is that this can be used to test RBAC in a
  single session, without having to turn it on for everyone.

They will all default to false initially. Once we have finished implementing everything, we will
update them to true for certain candidate customers. Once we are confident in our implementation,
we will remove "enable_ld_rbac_checks" and "enable_session_rbac_checks", and change the default of
"enable_rbac_checks" to true.

All new SQL commands will be available to all users. The SQL commands will update user privileges,
but emit a notice if RBAC is disabled.

## Testing Plan

- A lot of SLT and Testdrive tests that create various roles, grant them various attributes and privileges, and test
  what they are and are not allowed to do.
    - TODO: Come up with a matrix of all permutations
- Rust test that ensures Frontegg admins are given the `SUPERUSER` session attribute.
- Rust test that ensures non Frontegg admins are not given the `SUPERUSER` session attribute.
- Rust test(s) that ensures `SUPERUSER`s can do all actions.
    - This cannot be done through SLT or Testdrive because `SUPERUSER` can only be derived from Frontegg, which is not
      available in those types of tests.
- Port any ACL (Access Control List, which is what PostgreSQL calls this feature) tests that exist in PostgreSQL to
  Materialize.

## Alternatives

- PostgreSQL breaks from the SQL standard in a couple of places in their RBAC implementation. We may want to consider a
  design that is more in line with the SQL standard than PostgreSQL.

## Open questions

- Do we want different `SELECT` privileges based on if a new dataflow will be spun up or if we can use an existing one?
    - Pros: We can differentiate between using existing compute resources vs creating new ones when reading.
    - Cons: Users (and the database) are unable to determine if they're allowed to execute a read until after that read
      has been fully planned.
- What views/functions/commands do we want to add to help users query the current set of privileges.
- What are security labels in PostgreSQL and do we want them?
