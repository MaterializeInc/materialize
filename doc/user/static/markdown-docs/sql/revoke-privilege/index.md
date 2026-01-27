# REVOKE PRIVILEGE
`REVOKE` revokes privileges from a database object.
`REVOKE` revokes privileges from a database object. The `PUBLIC` pseudo-role can
be used to indicate that the privileges should be revoked from all roles
(including roles that might not exist yet).

## Syntax

> **Note:** The syntax supports the `ALL [PRIVILEGES]` shorthand to refer to all
> [*applicable* privileges](#applicable-privileges-to-revoke) for the
> object type.





<!-- ============ CLUSTER syntax ==============  -->

**Cluster:**

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


<!-- ================== Connection syntax ======================  -->

**Connection:**

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



<!-- ================== Database syntax =====================  -->

**Database:**

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



<!-- =============== Materialized view syntax ===================  -->

**Materialized view/view/source:**

> **Note:** To read from a views or a materialized views, you must have `SELECT` privileges
> on the view/materialized views. That is, having `SELECT` privileges on the
> underlying objects defining the view/materialized view is insufficient.


For specific materialized view(s)/view(s)/source(s):

```mzsql
REVOKE <SELECT | ALL [PRIVILEGES]>
ON [TABLE] <name> [, <name> ...] -- For PostgreSQL compatibility, if specifying type, use TABLE
FROM <role_name> [, ... ];
```



<!-- ==================== Schema syntax =====================  -->

**Schema:**

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



<!-- ==================== Secret syntax =====================  -->

**Secret:**

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



<!-- ==================== System syntax =====================  -->

**System:**

```mzsql
REVOKE <CREATEROLE | CREATEDB | CREATECLUSTER | CREATENETWORKPOLICY | ALL [PRIVILEGES]> [, ... ]
ON SYSTEM
FROM <role_name> [, ... ];
```



<!-- ==================== Type syntax =======================  -->

**Type:**

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



<!-- ======================= Table syntax =====================  -->

**Table:**

For specific table(s):

```mzsql
REVOKE <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON [TABLE] <name> [, <name> ...]
FROM <role_name> [, ... ];
```

For all tables or all tables in a specific schema(s) or in a specific database(s):

> **Note:** Granting privileges via `ALL TABLES [...]` also applies to sources, views, and
> materialized views (for the applicable privileges).


```mzsql
REVOKE <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON ALL TABLES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
FROM <role_name> [, ... ];
```





## Details

### Applicable privileges to revoke


**By Privilege:**

| Privilege | Description | Abbreviation | Applies to |
| --- | --- | --- | --- |
| <strong>SELECT</strong> | Permission to read rows from an object. | <code>r</code> | <ul> <li><code>MATERIALIZED VIEW</code></li> <li><code>SOURCE</code></li> <li><code>TABLE</code></li> <li><code>VIEW</code></li> </ul>  |
| <strong>INSERT</strong> | Permission to insert rows into an object. | <code>a</code> | <ul> <li><code>TABLE</code></li> </ul>  |
| <strong>UPDATE</strong> | <p>Permission to modify rows in an object.</p> <p>Modifying rows may also require <strong>SELECT</strong> if a read is needed to determine which rows to update.</p>  | <code>w</code> | <ul> <li><code>TABLE</code></li> </ul>  |
| <strong>DELETE</strong> | <p>Permission to delete rows from an object.</p> <p>Deleting rows may also require <strong>SELECT</strong> if a read is needed to determine which rows to delete.</p>  | <code>d</code> | <ul> <li><code>TABLE</code></li> </ul>  |
| <strong>CREATE</strong> | Permission to create a new objects within the specified object. | <code>C</code> | <ul> <li><code>DATABASE</code></li> <li><code>SCHEMA</code></li> <li><code>CLUSTER</code></li> </ul>  |
| <strong>USAGE</strong> | <a name="privilege-usage"></a> Permission to use or reference an object (e.g., schema/type lookup). | <code>U</code> | <ul> <li><code>CLUSTER</code></li> <li><code>CONNECTION</code></li> <li><code>DATABASE</code></li> <li><code>SCHEMA</code></li> <li><code>SECRET</code></li> <li><code>TYPE</code></li> </ul>  |
| <strong>CREATEROLE</strong> | <p>Permission to create/modify/delete roles and manage role memberships for any role in the system.</p> > **Warning:** Roles with the `CREATEROLE` privilege can obtain the privileges of any other > role in the system by granting themselves that role. Avoid granting > `CREATEROLE` unnecessarily. | <code>R</code> | <ul> <li><code>SYSTEM</code></li> </ul>  |
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





### Privileges

The privileges required to execute this statement are:

- Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object is a schema.
- `USAGE` privileges on the containing schema if the affected object is namespaced by a schema.
- _superuser_ status if the privilege is a system privilege.


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
