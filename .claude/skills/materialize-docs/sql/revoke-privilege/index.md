---
audience: developer
canonical_url: https://materialize.com/docs/sql/revoke-privilege/
complexity: advanced
description: '`REVOKE` revokes privileges from a database object.'
doc_type: reference
keywords:
- SHOW PRIVILEGES
- CREATE ON
- 'Note:'
- REVOKE PRIVILEGE
- ALTER ROLE
- SELECT ON
- CREATE ROLE
product_area: Indexes
status: stable
title: REVOKE PRIVILEGE
---

# REVOKE PRIVILEGE

## Purpose
`REVOKE` revokes privileges from a database object.

If you need to understand the syntax and options for this command, you're in the right place.


`REVOKE` revokes privileges from a database object.


`REVOKE` revokes privileges from a database object. The `PUBLIC` pseudo-role can
be used to indicate that the privileges should be revoked from all roles
(including roles that might not exist yet).

## Syntax

> **Note:** 

The syntax supports the `ALL [PRIVILEGES]` shorthand to refer to all
[*applicable* privileges](#applicable-privileges-to-revoke) for the
object type.


<!-- ============ CLUSTER syntax ==============  -->

#### Cluster


For specific cluster(s):

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON CLUSTER <name> [, ...]
FROM <role_name> [, ... ]
;
```text

For all clusters:

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL CLUSTERS
FROM <role_name> [, ... ]
;
```json


<!-- ================== Connection syntax ======================  -->

#### Connection


For specific connection(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON CONNECTION <name> [, ...]
FROM <role_name> [, ... ];
```text

For all connections or all connections in specific schema(s) or in database(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON ALL CONNECTIONS
 [ IN <SCHEMA | DATABASE> <name> [, <name> ...] ]
FROM <role_name> [, ... ];
```json


<!-- ================== Database syntax =====================  -->

#### Database


For specific database(s):

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON DATABASE <name> [, ...]
FROM <role_name> [, ... ];
```text

For all database:

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL DATABASES
FROM <role_name> [, ... ];
```json


<!-- =============== Materialized view syntax ===================  -->

#### Materialized view/view/source


> **Note:** 
To read from a views or a materialized views, you must have `SELECT` privileges
on the view/materialized views. That is, having `SELECT` privileges on the
underlying objects defining the view/materialized view is insufficient.


For specific materialized view(s)/view(s)/source(s):

```mzsql
REVOKE <SELECT | ALL [PRIVILEGES]>
ON [TABLE] <name> [, <name> ...] -- For PostgreSQL compatibility, if specifying type, use TABLE
FROM <role_name> [, ... ];
```json


<!-- ==================== Schema syntax =====================  -->

#### Schema


For specific schema(s):

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON SCHEMA <name> [, ...]
FROM <role_name> [, ... ];
```text

For all schemas or all schemas in a specific database(s):

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL SCHEMAS [IN DATABASE <name> [, <name> ...]]
FROM <role_name> [, ... ];
```json


<!-- ==================== Secret syntax =====================  -->

#### Secret


For specific secret(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]> [, ... ]
ON SECRET <name> [, ...]
FROM <role_name> [, ... ];
```text

For all secrets or all secrets in a specific database(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]> [, ... ]
ON ALL SECRET [IN DATABASE <name> [, <name> ...]]
FROM <role_name> [, ... ];
```json


<!-- ==================== System syntax =====================  -->

#### System


```mzsql
REVOKE <CREATEROLE | CREATEDB | CREATECLUSTER | CREATENETWORKPOLICY | ALL [PRIVILEGES]> [, ... ]
ON SYSTEM
FROM <role_name> [, ... ];
```json


<!-- ==================== Type syntax =======================  -->

#### Type


For specific view(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON TYPE <name> [, <name> ...]
FROM <role_name> [, ... ];
```text

For all types or all types in a specific schema(s) or in a specific database(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON ALL TYPES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
FROM <role_name> [, ... ];
```json


<!-- ======================= Table syntax =====================  -->

#### Table


For specific table(s):

```mzsql
REVOKE <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON [TABLE] <name> [, <name> ...]
FROM <role_name> [, ... ];
```text

For all tables or all tables in a specific schema(s) or in a specific database(s):

> **Note:** 

Granting privileges via `ALL TABLES [...]` also applies to sources, views, and
materialized views (for the applicable privileges).


```mzsql
REVOKE <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON ALL TABLES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
FROM <role_name> [, ... ];
```json


## Details

This section covers details.

### Applicable privileges to revoke


#### By Privilege

<!-- Dynamic table: rbac/privileges_objects - see original docs -->

#### By Object

<!-- Dynamic table: rbac/object_privileges - see original docs -->


### Privileges

The privileges required to execute this statement are:

- Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object is a schema.
- `USAGE` privileges on the containing schema if the affected object is namespaced by a schema.
- _superuser_ status if the privilege is a system privilege.


## Examples

This section covers examples.

```mzsql
REVOKE SELECT ON mv FROM joe, mike;
```text

```mzsql
REVOKE USAGE, CREATE ON DATABASE materialize FROM joe;
```text

```mzsql
REVOKE ALL ON CLUSTER dev FROM joe;
```text

```mzsql
REVOKE CREATEDB ON SYSTEM FROM joe;
```


## Useful views

- [`mz_internal.mz_show_system_privileges`](/sql/system-catalog/mz_internal/#mz_show_system_privileges)
- [`mz_internal.mz_show_my_system_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_system_privileges)
- [`mz_internal.mz_show_cluster_privileges`](/sql/system-catalog/mz_internal/#mz_show_cluster_privileges)
- [`mz_internal.mz_show_my_cluster_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_cluster_privileges)
- [`mz_internal.mz_show_database_privileges`](/sql/system-catalog/mz_internal/#mz_show_database_privileges)
- [`mz_internal.mz_show_my_database_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_database_privileges)
- [`mz_internal.mz_show_schema_privileges`](/sql/system-catalog/mz_internal/#mz_show_schema_privileges)
- [`mz_internal.mz_show_my_schema_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_schema_privileges)
- [`mz_internal.mz_show_object_privileges`](/sql/system-catalog/mz_internal/#mz_show_object_privileges)
- [`mz_internal.mz_show_my_object_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_object_privileges)
- [`mz_internal.mz_show_all_privileges`](/sql/system-catalog/mz_internal/#mz_show_all_privileges)
- [`mz_internal.mz_show_all_my_privileges`](/sql/system-catalog/mz_internal/#mz_show_all_my_privileges)

## Related pages

- [`SHOW PRIVILEGES`](../show-privileges)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../revoke-privilege)
- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)