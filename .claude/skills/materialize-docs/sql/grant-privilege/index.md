---
audience: developer
canonical_url: https://materialize.com/docs/sql/grant-privilege/
complexity: advanced
description: Grant privileges on a database object.
doc_type: reference
keywords:
- SHOW PRIVILEGES
- CREATE ON
- 'Note:'
- GRANT PRIVILEGE
- ALTER ROLE
- SELECT ON
- CREATE ROLE
product_area: Indexes
status: stable
title: GRANT PRIVILEGE
---

# GRANT PRIVILEGE

## Purpose
Grant privileges on a database object.

If you need to understand the syntax and options for this command, you're in the right place.


Grant privileges on a database object.


`GRANT PRIVILEGE` grants privileges to [database
role(s)](/sql/create-role/).

## Syntax

> **Note:** 

The syntax supports the `ALL [PRIVILEGES]` shorthand to refer to all
[*applicable* privileges](/sql/grant-privilege/#available-privileges) for the
object type.


<!-- ============ CLUSTER syntax ==============  -->

#### Cluster


For specific cluster(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON CLUSTER <name> [, ...]
TO <role_name> [, ... ];
```text

For all clusters:

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL CLUSTERS
TO <role_name> [, ... ];
```json


<!-- ================== Connection syntax ======================  -->

#### Connection


For specific connection(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]>
ON CONNECTION <name> [, ...]
TO <role_name> [, ... ];
```text

For all connections or all connections in specific schema(s) or in database(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]>
ON ALL CONNECTIONS
 [ IN <SCHEMA | DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```json


<!-- ================== Database syntax =====================  -->

#### Database


For specific database(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON DATABASE <name> [, ...]
TO <role_name> [, ... ];
```text

For all database:

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL DATABASES
TO <role_name> [, ... ];
```json


<!-- =============== Materialized view syntax ===================  -->

#### Materialized view/view/source


> **Note:** 
To read from a views or a materialized views, you must have `SELECT` privileges
on the view/materialized views. That is, having `SELECT` privileges on the
underlying objects defining the view/materialized view is insufficient.


For specific materialized view(s)/view(s)/source(s):

```mzsql
GRANT <SELECT | ALL [PRIVILEGES]>
ON [TABLE] <name> [, <name> ...] -- For PostgreSQL compatibility, if specifying type, use TABLE
TO <role_name> [, ... ];
```json


<!-- ==================== Schema syntax =====================  -->

#### Schema


For specific schema(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON SCHEMA <name> [, ...]
TO <role_name> [, ... ];
```text

For all schemas or all schemas in a specific database(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL SCHEMAS [IN DATABASE <name> [, <name> ...]]
TO <role_name> [, ... ];
```json


<!-- ==================== Secret syntax =====================  -->

#### Secret


For specific secret(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]> [, ... ]
ON SECRET <name> [, ...]
TO <role_name> [, ... ];
```text

For all secrets or all secrets in a specific database(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]> [, ... ]
ON ALL SECRET [IN DATABASE <name> [, <name> ...]]
TO <role_name> [, ... ];
```json


<!-- ==================== System syntax =====================  -->

#### System


```mzsql
GRANT <CREATEROLE | CREATEDB | CREATECLUSTER | CREATENETWORKPOLICY | ALL [PRIVILEGES]> [, ... ]
ON SYSTEM
TO <role_name> [, ... ];
```json


<!-- ==================== Type syntax =======================  -->

#### Type


For specific view(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]>
ON TYPE <name> [, <name> ...]
TO <role_name> [, ... ];
```text

For all types or all types in a specific schema(s) or in a specific database(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]>
ON ALL TYPES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```json


<!-- ======================= Table syntax =====================  -->

#### Table


For specific table(s):

```mzsql
GRANT <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON [TABLE] <name> [, <name> ...]
TO <role_name> [, ... ];
```text

For all tables or all tables in a specific schema(s) or in a specific database(s):

> **Note:** 

Granting privileges via `ALL TABLES [...]` also applies to sources, views, and
materialized views (for the applicable privileges).


```mzsql
GRANT <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON ALL TABLES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```json


## Details

This section covers details.

### Available privileges


#### By Privilege

<!-- Dynamic table: rbac/privileges_objects - see original docs -->

#### By Object

<!-- Dynamic table: rbac/object_privileges - see original docs -->


## Privileges

The privileges required to execute this statement are:

- Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object is a schema.
- `USAGE` privileges on the containing schema if the affected object is namespaced by a schema.
- _superuser_ status if the privilege is a system privilege.


## Examples

This section covers examples.

```mzsql
GRANT SELECT ON mv_quarterly_sales TO data_analysts, reporting;
```text

```mzsql
GRANT USAGE, CREATE ON DATABASE materialize TO data_analysts;
```text

```mzsql
GRANT ALL ON CLUSTER dev_cluster TO data_analysts, developers;
```text

```mzsql
GRANT CREATEDB ON SYSTEM TO source_owners;
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
- [`REVOKE PRIVILEGE`](../revoke-privilege)
- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)