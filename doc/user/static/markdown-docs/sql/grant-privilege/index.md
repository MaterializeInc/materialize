# GRANT PRIVILEGE

Grant privileges on a database object.



`GRANT PRIVILEGE` grants privileges to [database
role(s)](/sql/create-role/).

## Syntax

> **Note:** The syntax supports the `ALL [PRIVILEGES]` shorthand to refer to all
> [*applicable* privileges](/sql/grant-privilege/#available-privileges) for the
> object type.
>
>




<!-- ============ CLUSTER syntax ==============  -->

**Cluster:**

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


<!-- ================== Connection syntax ======================  -->

**Connection:**

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



<!-- ================== Database syntax =====================  -->

**Database:**

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



<!-- =============== Materialized view syntax ===================  -->

**Materialized view/view/source:**

> **Note:** To read from a views or a materialized views, you must have <code>SELECT</code> privileges
> on the view/materialized views. That is, having <code>SELECT</code> privileges on the
> underlying objects defining the view/materialized view is insufficient.
>


For specific materialized view(s)/view(s)/source(s):

```mzsql
GRANT <SELECT | ALL [PRIVILEGES]>
ON [TABLE] <name> [, <name> ...] -- For PostgreSQL compatibility, if specifying type, use TABLE
TO <role_name> [, ... ];
```



<!-- ==================== Schema syntax =====================  -->

**Schema:**

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



<!-- ==================== Secret syntax =====================  -->

**Secret:**

For specific secret(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]> [, ... ]
ON SECRET <name> [, ...]
TO <role_name> [, ... ];
```

For all secrets or all secrets in a specific database(s):

```mzsql
GRANT <USAGE | ALL [PRIVILEGES]> [, ... ]
ON ALL SECRET [IN DATABASE <name> [, <name> ...]]
TO <role_name> [, ... ];
```



<!-- ==================== System syntax =====================  -->

**System:**

```mzsql
GRANT <CREATEROLE | CREATEDB | CREATECLUSTER | CREATENETWORKPOLICY | ALL [PRIVILEGES]> [, ... ]
ON SYSTEM
TO <role_name> [, ... ];
```



<!-- ==================== Type syntax =======================  -->

**Type:**

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



<!-- ======================= Table syntax =====================  -->

**Table:**

For specific table(s):

```mzsql
GRANT <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON [TABLE] <name> [, <name> ...]
TO <role_name> [, ... ];
```

For all tables or all tables in a specific schema(s) or in a specific database(s):

> **Note:** Granting privileges via <code>ALL TABLES [...]</code> also applies to sources, views, and
> materialized views (for the applicable privileges).
>
>


```mzsql
GRANT <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON ALL TABLES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```





## Details

### Available privileges


**By Privilege:**

| Privilege | Description | Abbreviation | Applies to |
| --- | --- | --- | --- |
| <strong>SELECT</strong> | Permission to read rows from an object. | <code>r</code> | <ul> <li><code>MATERIALIZED VIEW</code></li> <li><code>SOURCE</code></li> <li><code>TABLE</code></li> <li><code>VIEW</code></li> </ul>  |
| <strong>INSERT</strong> | Permission to insert rows into an object. | <code>a</code> | <ul> <li><code>TABLE</code></li> </ul>  |
| <strong>UPDATE</strong> | <p>Permission to modify rows in an object.</p> <p>Modifying rows may also require <strong>SELECT</strong> if a read is needed to determine which rows to update.</p>  | <code>w</code> | <ul> <li><code>TABLE</code></li> </ul>  |
| <strong>DELETE</strong> | <p>Permission to delete rows from an object.</p> <p>Deleting rows may also require <strong>SELECT</strong> if a read is needed to determine which rows to delete.</p>  | <code>d</code> | <ul> <li><code>TABLE</code></li> </ul>  |
| <strong>CREATE</strong> | Permission to create a new objects within the specified object. | <code>C</code> | <ul> <li><code>DATABASE</code></li> <li><code>SCHEMA</code></li> <li><code>CLUSTER</code></li> </ul>  |
| <strong>USAGE</strong> | <a name="privilege-usage"></a> Permission to use or reference an object (e.g., schema/type lookup). | <code>U</code> | <ul> <li><code>CLUSTER</code></li> <li><code>CONNECTION</code></li> <li><code>DATABASE</code></li> <li><code>SCHEMA</code></li> <li><code>SECRET</code></li> <li><code>TYPE</code></li> </ul>  |
| <strong>CREATEROLE</strong> | <p>Permission to create/modify/delete roles and manage role memberships for any role in the system.</p> > **Warning:** Roles with the <code>CREATEROLE</code> privilege can obtain the privileges of any other > role in the system by granting themselves that role. Avoid granting > <code>CREATEROLE</code> unnecessarily. > | <code>R</code> | <ul> <li><code>SYSTEM</code></li> </ul>  |
| <strong>CREATEDB</strong> | Permission to create new databases. | <code>B</code> | <ul> <li><code>SYSTEM</code></li> </ul>  |
| <strong>CREATECLUSTER</strong> | Permission to create new clusters. | <code>N</code> | <ul> <li><code>SYSTEM</code></li> </ul>  |
| <strong>CREATENETWORKPOLICY</strong> | Permission to create network policies to control access at the network layer. | <code>P</code> | <ul> <li><code>SYSTEM</code></li> </ul>  |


**By Object:**

| Object | Privileges |
| --- | --- |
| <code>CLUSTER</code> | <ul> <li><code>USAGE</code></li> <li><code>CREATE</code></li> </ul>  |
| <code>CONNECTION</code> | <ul> <li><code>USAGE</code></li> </ul>  |
| <code>DATABASE</code> | <ul> <li><code>USAGE</code></li> <li><code>CREATE</code></li> </ul>  |
| <code>MATERIALIZED VIEW</code> | <ul> <li><code>SELECT</code></li> </ul>  |
| <code>SCHEMA</code> | <ul> <li><code>USAGE</code></li> <li><code>CREATE</code></li> </ul>  |
| <code>SECRET</code> | <ul> <li><code>USAGE</code></li> </ul>  |
| <code>SOURCE</code> | <ul> <li><code>SELECT</code></li> </ul>  |
| <code>SYSTEM</code> | <ul> <li><code>CREATEROLE</code></li> <li><code>CREATEDB</code></li> <li><code>CREATECLUSTER</code></li> <li><code>CREATENETWORKPOLICY</code></li> </ul>  |
| <code>TABLE</code> | <ul> <li><code>INSERT</code></li> <li><code>SELECT</code></li> <li><code>UPDATE</code></li> <li><code>DELETE</code></li> </ul>  |
| <code>TYPE</code> | <ul> <li><code>USAGE</code></li> </ul>  |
| <code>VIEW</code> | <ul> <li><code>SELECT</code></li> </ul>  |




## Privileges

The privileges required to execute this statement are:

<ul>
<li>Ownership of affected objects.</li>
<li><code>USAGE</code> privileges on the containing database if the affected object is a schema.</li>
<li><code>USAGE</code> privileges on the containing schema if the affected object is namespaced by a schema.</li>
<li><em>superuser</em> status if the privilege is a system privilege.</li>
</ul>


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
- [`ALTER OWNER`](/sql/#rbac)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)
