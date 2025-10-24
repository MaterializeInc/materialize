---
title: "GRANT PRIVILEGE"
description: "Grant privileges on a database object."
menu:
  main:
    parent: commands
---

`GRANT PRIVILEGE` grants privileges to [database
role(s)](/security/access-control/manage-roles/).

## Syntax

{{< note >}}

The syntax supports the `ALL [PRIVILEGES]` shorthand to refer to all
[*applicable* privileges](/sql/grant-privilege/#available-privileges) for the
object type.

{{</note>}}

{{< tabs >}}

<!-- ============ CLUSTER syntax ==============  -->

{{< tab "Cluster" >}}

For specific cluster(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON CLUSTER <name> [, ...]
TO <role_name> [, ... ];
```

For all clusters:

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL CLUSTERS
TO <role_name> [, ... ];
```
{{</ tab >}}

<!-- ================== Connection syntax ======================  -->

{{< tab "Connection">}}

For specific connection(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]>
ON CONNECTION <name> [, ...]
TO <role_name> [, ... ];
```

For all connections or all connections in specific schema(s) or in database(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]>
ON ALL CONNECTIONS
 [ IN <SCHEMA | DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```

{{</ tab >}}

<!-- ================== Database syntax =====================  -->

{{< tab "Database">}}

For specific database(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON DATABASE <name> [, ...]
TO <role_name> [, ... ];
```

For all database:

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL DATABASES
TO <role_name> [, ... ];
```

{{</ tab >}}

<!-- =============== Materialized view syntax ===================  -->

{{< tab "Materialized view/view/source">}}

{{< note >}}
{{< include-md file="shared-content/rbac/privilege-for-views-mat-views.md" >}}
{{</ note >}}

For specific materialized view(s)/view(s)/source(s):

```mzsql
GRANT <SELECT | ALL [PRIVILEGES]>
ON [TABLE] <name> [, <name> ...] -- For PostgreSQL compatibility, if specifying type, use TABLE
TO <role_name> [, ... ];
```

{{</ tab >}}

<!-- ==================== Schema syntax =====================  -->

{{< tab "Schema">}}

For specific schema(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON SCHEMA <name> [, ...]
TO <role_name> [, ... ];
```

For all schemas or all schemas in a specific database(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL SCHEMAS [IN DATABASE <name> [, <name> ...]]
TO <role_name> [, ... ];
```

{{</ tab >}}

<!-- ==================== Secret syntax =====================  -->

{{< tab "Secret">}}

For specific secret(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON SECRET <name> [, ...]
TO <role_name> [, ... ];
```

For all secrets or all secrets in a specific database(s):

```mzsql
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL SECRET [IN DATABASE <name> [, <name> ...]]
TO <role_name> [, ... ];
```

{{</ tab >}}

<!-- ==================== System syntax =====================  -->

{{< tab "System">}}

```mzsql
GRANT <CREATEROLE | CREATEDB | CREATECLUSTER | CREATENETWORKPOLICY | ALL [PRIVILEGES]> [, ... ]
ON SYSTEM
TO <role_name> [, ... ];
```

{{</ tab >}}

<!-- ==================== Type syntax =======================  -->

{{< tab "Type">}}

For specific view(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]>
ON TYPE <name> [, <name> ...]
TO <role_name> [, ... ];
```

For all types or all types in a specific schema(s) or in a specific database(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]>
ON ALL TYPES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```

{{</ tab >}}

<!-- ======================= Table syntax =====================  -->

{{< tab "Table">}}

For specific table(s):

```mzsql
GRANT <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON [TABLE] <name> [, <name> ...]
TO <role_name> [, ... ];
```

For all tables or all tables in a specific schema(s) or in a specific database(s):

{{< note >}}

{{< include-md file="shared-content/rbac/grant-privilege-all-tables.md" >}}

{{</ note >}}

```mzsql
GRANT <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON ALL TABLES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```

{{</ tab >}}

{{</ tabs >}}

## Details

### Available privileges

{{< tabs >}}
{{< tab "By Privilege" >}}
{{< yaml-table data="rbac/privileges_objects" >}}
{{</ tab >}}
{{< tab "By Object" >}}
{{< yaml-table data="rbac/object_privileges" >}}
{{</ tab >}}
{{</ tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/grant-privilege.md"
>}}

## Examples

```mzsql
GRANT SELECT ON mv_quarterly_sales TO data_analysts, reporting;
```

```mzsql
GRANT USAGE, CREATE ON DATABASE materialize TO data_analysts;
```

```mzsql
GRANT ALL ON CLUSTER dev_cluster TO data_analysts, developers;
```

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
- [`ALTER OWNER`](../alter-owner)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)
