# ALTER DEFAULT PRIVILEGES

`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to objects created in the future.



Use `ALTER DEFAULT PRIVILEGES` to:

- Define default privileges that will be applied to objects created in the
future. It does not affect any existing objects.

- Revoke previously created default privileges on objects created in the future.

All new environments are created with a single default privilege, `USAGE` is
granted on all `TYPES` to the `PUBLIC` role. This can be revoked like any other
default privilege.

## Syntax


**GRANT:**
### GRANT

`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to
objects created by a role in the future. It does not affect any existing
objects.

Default privileges are specified for a certain object type and can be applied to
all objects of that type, all objects of that type created within a specific set
of databases, or all objects of that type created within a specific set of
schemas. Default privileges are also specified for objects created by a certain
set of roles or by all roles.



```mzsql
ALTER DEFAULT PRIVILEGES
  FOR ROLE <object_creator> [, ...] | ALL ROLES
  [IN SCHEMA <schema_name> [, ...] | IN DATABASE <database_name> [, ...]]
  GRANT [<privilege> [, ...] | ALL [PRIVILEGES]]
  ON TABLES | TYPES | SECRETS | CONNECTIONS | DATABASES | SCHEMAS | CLUSTERS
  TO <target_role> [, ...]
;

```

| Syntax element | Description |
| --- | --- |
| `<object_creator>` | The default privilege will apply to objects created by this role. Use the `PUBLIC` pseudo-role to target objects created by all roles.  |
| **ALL ROLES** | The default privilege will apply to objects created by all roles. This is shorthand for specifying `PUBLIC` as the target role.  |
| **IN SCHEMA** `<schema_name>` | Optional. The default privilege will apply only to objects created in this schema.  |
| **IN DATABASE** `<database_name>` | Optional. The default privilege will apply only to objects created in this database.  |
| `<privilege>` | A specific privilege (e.g., `SELECT`, `USAGE`, `CREATE`). See [Available privileges](#available-privileges).  |
| **ALL [PRIVILEGES]** | All applicable privileges for the provided object type.  |
| **TO** `<target_role>` | The role who will be granted the default privilege. Use the `PUBLIC` pseudo-role to grant privileges to all roles.  |



**REVOKE:**
### REVOKE

> **Note:** `ALTER DEFAULT PRIVILEGES` cannot be used to revoke the default owner privileges
> on objects. Those privileges must be revoked manually after the object is
> created. Though owners can always re-grant themselves any privilege on an object
> that they own.
>


The `REVOKE` variant of `ALTER DEFAULT PRIVILEGES` is used to revoke previously
created default privileges on objects created in the future. It will not revoke
any privileges on objects that have already been created. When revoking a
default privilege, all the fields in the revoke statement (`creator_role`,
`schema_name`, `database_name`, `privilege`, `target_role`) must exactly match
an existing default privilege. The existing default privileges can easily be
viewed by the following query: `SELECT * FROM
mz_internal.mz_show_default_privileges`.



```mzsql
ALTER DEFAULT PRIVILEGES
  FOR ROLE <creator_role> [, ...] | ALL ROLES
  [IN SCHEMA <schema_name> [, ...] | IN DATABASE <database_name> [, ...]]
  REVOKE [<privilege> [, ...] | ALL [PRIVILEGES]]
  ON TABLES | TYPES | SECRETS | CONNECTIONS | DATABASES | SCHEMAS | CLUSTERS
  FROM <target_role> [, ...]
;

```

| Syntax element | Description |
| --- | --- |
| `<creator_role>` | The default privileges for objects created by this role. Use the `PUBLIC` pseudo-role to specify objects created by all roles.  |
| **ALL ROLES** | The default privilege for objects created by all roles. This is shorthand for specifying `PUBLIC` as the target role.  |
| **IN SCHEMA** `<schema_name>` | Optional. The default privileges for objects created in this schema.  |
| **IN DATABASE** `<database_name>` | Optional. The default privilege for objects created in this database.  |
| `<privilege>` | A specific privilege (e.g., `SELECT`, `USAGE`, `CREATE`). See [Available privileges](#available-privileges).  |
| **ALL [PRIVILEGES]** | All applicable privileges for the provided object type.  |
| **FROM** `<target_role>` | The role from whom to remove the default privilege. Use the `PUBLIC` pseudo-role to remove default privileges previously granted to `PUBLIC`.  |





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





### Compatibility

For PostgreSQL compatibility reasons, you must specify `TABLES` as the object
type for sources, views, and materialized views.

## Examples

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE mike GRANT SELECT ON TABLES TO joe;
```

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE interns IN DATABASE dev GRANT ALL PRIVILEGES ON TABLES TO intern_managers;
```

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE developers REVOKE USAGE ON SECRETS FROM project_managers;
```

```mzsql
ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON TABLES TO managers;
```

## Privileges

The privileges required to execute this statement are:

<ul>
<li>Role membership in <code>role_name</code>.</li>
<li><code>USAGE</code> privileges on the containing database if <code>database_name</code> is specified.</li>
<li><code>USAGE</code> privileges on the containing schema if <code>schema_name</code> is specified.</li>
<li><em>superuser</em> status if the <em>target_role</em> is <code>PUBLIC</code> or <strong>ALL ROLES</strong> is
specified.</li>
</ul>


## Useful views

- [`mz_internal.mz_show_default_privileges`](/sql/system-catalog/mz_internal/#mz_show_default_privileges)
- [`mz_internal.mz_show_my_default_privileges`](/sql/system-catalog/mz_internal/#mz_show_my_default_privileges)

## Related pages

- [`SHOW DEFAULT PRIVILEGES`](../show-default-privileges)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
