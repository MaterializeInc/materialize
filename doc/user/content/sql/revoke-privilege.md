---
title: "REVOKE PRIVILEGE"
description: "`REVOKE` revokes privileges from a database object."
menu:
  main:
    parent: commands
---

`REVOKE` revokes privileges from a database object. The `PUBLIC` pseudo-role can
be used to indicate that the privileges should be revoked from all roles
(including roles that might not exist yet).

## Syntax

{{< note >}}

The syntax supports the `ALL [PRIVILEGES]` shorthand to refer to all
[*applicable* privileges](#applicable-privileges-to-revoke) for the
object type.

For PostgreSQL compatibility, you can reference views, materialized views,
and sources with the `TABLE` keyword. With `ON TABLE`, the `ALL [PRIVILEGES]`
shorthand expands to the full table privilege set
(`SELECT, INSERT, UPDATE, DELETE`). This clears every privilege previously
granted through the same shorthand, including the non-applicable ones that
have no runtime effect.

{{</note>}}


{{< tabs >}}

<!-- ============ CLUSTER syntax ==============  -->

{{< tab "Cluster" >}}

For specific cluster(s):

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON CLUSTER <name> [, ...]
FROM <role_name> [, ... ]
;
```

For all clusters:

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL CLUSTERS
FROM <role_name> [, ... ]
;
```
{{</ tab >}}

<!-- ================== Connection syntax ======================  -->

{{< tab "Connection">}}

For specific connection(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON CONNECTION <name> [, ...]
FROM <role_name> [, ... ];
```

For all connections or all connections in specific schema(s) or in database(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON ALL CONNECTIONS
 [ IN <SCHEMA | DATABASE> <name> [, <name> ...] ]
FROM <role_name> [, ... ];
```

{{</ tab >}}

<!-- ================== Database syntax =====================  -->

{{< tab "Database">}}

For specific database(s):

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON DATABASE <name> [, ...]
FROM <role_name> [, ... ];
```

For all database:

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL DATABASES
FROM <role_name> [, ... ];
```

{{</ tab >}}

<!-- =============== Materialized view syntax ===================  -->

{{< tab "Materialized view/view/source">}}

{{< note >}}
{{% include-headless "/headless/rbac-cloud/privilege-for-views-mat-views" %}}
{{</ note >}}

For specific materialized view(s)/view(s)/source(s):

```mzsql
REVOKE <SELECT | ALL [PRIVILEGES]>
ON <name> [, <name> ...]
FROM <role_name> [, ... ];
```

For PostgreSQL compatibility, the `TABLE` keyword is also accepted. With the
`TABLE` syntax, you can name `INSERT`, `UPDATE`, and `DELETE` explicitly, and
`ALL [PRIVILEGES]` expands to the full table privilege set
(`SELECT, INSERT, UPDATE, DELETE`). This clears every privilege previously
granted through the matching `GRANT ALL ON TABLE` shorthand.

```mzsql
REVOKE <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON TABLE <name> [, <name> ...]
FROM <role_name> [, ... ];
```

{{</ tab >}}

<!-- ================== Network policy syntax ==================  -->

{{< tab "Network policy">}}

For specific network policies:

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON NETWORK POLICY <name> [, ...]
FROM <role_name> [, ... ];
```

For all network policies:

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON ALL POLICIES
FROM <role_name> [, ... ];
```

{{</ tab >}}

<!-- ==================== Schema syntax =====================  -->

{{< tab "Schema">}}

For specific schema(s):

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON SCHEMA <name> [, ...]
FROM <role_name> [, ... ];
```

For all schemas or all schemas in a specific database(s):

```mzsql
REVOKE <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL SCHEMAS [IN DATABASE <name> [, <name> ...]]
FROM <role_name> [, ... ];
```

{{</ tab >}}

<!-- ==================== Secret syntax =====================  -->

{{< tab "Secret">}}

For specific secret(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]> [, ... ]
ON SECRET <name> [, ...]
FROM <role_name> [, ... ];
```

For all secrets or all secrets in a specific database(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]> [, ... ]
ON ALL SECRET [IN DATABASE <name> [, <name> ...]]
FROM <role_name> [, ... ];
```

{{</ tab >}}

<!-- ==================== System syntax =====================  -->

{{< tab "System">}}

```mzsql
REVOKE <CREATEROLE | CREATEDB | CREATECLUSTER | CREATENETWORKPOLICY | ALL [PRIVILEGES]> [, ... ]
ON SYSTEM
FROM <role_name> [, ... ];
```

{{</ tab >}}

<!-- ==================== Type syntax =======================  -->

{{< tab "Type">}}

For specific view(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON TYPE <name> [, <name> ...]
FROM <role_name> [, ... ];
```

For all types or all types in a specific schema(s) or in a specific database(s):

```mzsql
REVOKE <USAGE | ALL [PRIVILEGES]>
ON ALL TYPES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
FROM <role_name> [, ... ];
```

{{</ tab >}}

<!-- ======================= Table syntax =====================  -->

{{< tab "Table">}}

For specific table(s):

```mzsql
REVOKE <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON [TABLE] <name> [, <name> ...]
FROM <role_name> [, ... ];
```

For all tables or all tables in a specific schema(s) or in a specific database(s):

{{< note >}}

{{% include-headless "/headless/rbac-cloud/grant-privilege-all-tables" %}}

{{</ note >}}

```mzsql
REVOKE <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON ALL TABLES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
FROM <role_name> [, ... ];
```

{{</ tab >}}

{{</ tabs >}}

## Details

### Applicable privileges to revoke

{{< tabs >}}
{{< tab "By Privilege" >}}
{{< yaml-table data="rbac/privileges_objects" >}}
{{</ tab >}}
{{< tab "By Object" >}}
{{< yaml-table data="rbac/object_privileges" >}}
{{</ tab >}}
{{</ tabs >}}


### Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/revoke-privilege" %}}


## Examples

```mzsql
REVOKE SELECT ON mv FROM joe, mike;
```

```mzsql
REVOKE USAGE, CREATE ON DATABASE materialize FROM joe;
```

```mzsql
REVOKE ALL ON CLUSTER dev FROM joe;
```

```mzsql
REVOKE CREATEDB ON SYSTEM FROM joe;
```


## Useful views

- [`mz_internal.mz_show_system_privileges`](/reference/system-catalog/mz_internal/#mz_show_system_privileges)
- [`mz_internal.mz_show_my_system_privileges`](/reference/system-catalog/mz_internal/#mz_show_my_system_privileges)
- [`mz_internal.mz_show_cluster_privileges`](/reference/system-catalog/mz_internal/#mz_show_cluster_privileges)
- [`mz_internal.mz_show_my_cluster_privileges`](/reference/system-catalog/mz_internal/#mz_show_my_cluster_privileges)
- [`mz_internal.mz_show_database_privileges`](/reference/system-catalog/mz_internal/#mz_show_database_privileges)
- [`mz_internal.mz_show_my_database_privileges`](/reference/system-catalog/mz_internal/#mz_show_my_database_privileges)
- [`mz_internal.mz_show_schema_privileges`](/reference/system-catalog/mz_internal/#mz_show_schema_privileges)
- [`mz_internal.mz_show_my_schema_privileges`](/reference/system-catalog/mz_internal/#mz_show_my_schema_privileges)
- [`mz_internal.mz_show_object_privileges`](/reference/system-catalog/mz_internal/#mz_show_object_privileges)
- [`mz_internal.mz_show_my_object_privileges`](/reference/system-catalog/mz_internal/#mz_show_my_object_privileges)
- [`mz_internal.mz_show_all_privileges`](/reference/system-catalog/mz_internal/#mz_show_all_privileges)
- [`mz_internal.mz_show_all_my_privileges`](/reference/system-catalog/mz_internal/#mz_show_all_my_privileges)

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
