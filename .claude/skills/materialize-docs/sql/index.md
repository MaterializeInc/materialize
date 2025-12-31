# SQL commands

SQL commands reference.



## Create/Alter/Drop Objects

{{< sql-commands-table-by-label label="object" group_by="object" >}}

## Create/Read/Update/Delete Data

The following commands perform CRUD operations on materialized views, views,
sources, and tables:

{{< yaml-table data="sql_commands_crud" noHeader=true >}}

## RBAC

Commands to manage roles and privileges and owners:

{{< yaml-table data="sql_commands_rbac" noHeader=true >}}

## Query Introspection (`Explain`)

{{< yaml-list data="sql_commands_all" label="explain" numColumns="1" >}}

## Object Introspection (`SHOW`) { #show }

{{< yaml-list data="sql_commands_all" label="show" numColumns="3" >}}

## Session

Commands related with session state and configurations:

{{< yaml-list data="sql_commands_all" label="session" numColumns="1" >}}

## Validations

{{< yaml-list data="sql_commands_all" label="other" >}}

## Prepared Statements

{{< yaml-list data="sql_commands_all" label="prepared statements" >}}




---

## Namespaces


{{< include-md file="shared-content/namespaces-content.md" >}}




---

## ALTER CLUSTER


Use `ALTER CLUSTER` to:

- Change configuration of a cluster, such as the `SIZE` or
`REPLICATON FACTOR`.
- Rename a cluster.
- Change owner of a cluster.

For completeness, the syntax for `SWAP WITH` operation is provided. However, in
general, you will not need to manually perform this operation.

## Syntax

`ALTER CLUSTER` has the following syntax variations:

{{< tabs >}}
{{< tab "Set a configuration" >}}

### Set a configuration

To set a cluster configuration:

{{% include-syntax file="examples/alter_cluster" example="syntax-set-configuration" %}}

{{< /tab >}}
{{< tab "Reset to default" >}}

### Reset to default

To reset a cluster configuration back to its default value:

{{% include-syntax file="examples/alter_cluster" example="syntax-reset-to-default" %}}

{{< /tab >}}
{{< tab "Rename" >}}

### Rename

To rename a cluster:

{{% include-syntax file="examples/alter_cluster" example="syntax-rename" %}}

{{< note >}}
You cannot rename system clusters, such as `mz_system` and `mz_catalog_server`.
{{< /note >}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a cluster:

{{% include-syntax file="examples/alter_cluster" example="syntax-change-owner" %}}

{{< /tab >}}
{{< tab "Swap with" >}}

### Swap with

{{< important >}}

Information about the `SWAP WITH` operation is provided for completeness.  The
`SWAP WITH` operation is used for blue/green deployments. In general, you will
not need to manually perform this operation.

{{< /important >}}

To swap the name of this cluster with another cluster:

{{% include-syntax file="examples/alter_cluster" example="syntax-swap-with" %}}

{{< /tab >}}
{{< /tabs >}}

### Cluster configuration

{{% yaml-table data="syntax_options/alter_cluster_options" %}}

### `WITH` options


| Command options (optional) | Value | Description |
|----------------------------|-------|-----------------|
| `WAIT UNTIL READY(...)`    |  | ***Private preview.** This option has known performance or stability issues and is under active development.* {{< alter-cluster/alter-clusters-cmd-options >}} |
| `WAIT FOR` | [`interval`](/sql/types/interval/) | ***Private preview.** This option has known performance or stability issues and is under active development.* A fixed duration to wait for the new replicas to be ready. This option can lead to downtime. As such, we recommend using the `WAIT UNTIL READY` option instead.|

## Considerations

### Resizing

{{< tip >}}

For help sizing your clusters, navigate to **Materialize Console >**
[**Monitoring**](/console/monitoring/)>**Environment Overview**. This page
displays cluster resource utilization and sizing advice.

{{< /tip >}}

#### Available sizes

{{< tabs >}}
{{< tab "M.1 Clusters" >}}

{{< include-md file="shared-content/cluster-size-disclaimer.md" >}}

{{< yaml-table data="m1_cluster_sizing" >}}

{{< /tab >}}
{{< tab "Legacy cc Clusters" >}}

{{< tip >}}
In most cases, you **should not** use legacy sizes. [M.1 sizes](#available-sizes)
offer better performance per credit for nearly all workloads. We recommend using
M.1 sizes for all new clusters, and recommend migrating existing
legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
customers during the transition period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.
{{< /tip >}}

Valid legacy cc cluster sizes are:

* `25cc`
* `50cc`
* `100cc`
* `200cc`
* `300cc`
* `400cc`
* `600cc`
* `800cc`
* `1200cc`
* `1600cc`
* `3200cc`
* `6400cc`
* `128C`
* `256C`
* `512C`

For clusters using legacy cc sizes, resource allocations are proportional to the
number in the size name. For example, a cluster of size `600cc` has 2x as much
CPU, memory, and disk as a cluster of size `300cc`, and 1.5x as much CPU,
memory, and disk as a cluster of size `400cc`.

Clusters of larger sizes can process data faster and handle larger data volumes.
{{< /tab >}}
{{< /tabs >}}

See also:

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).

#### Resource allocation

To determine the specific resource allocation for a given cluster size, query
the [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table.

{{< warning >}}
The values in the `mz_cluster_replica_sizes` table may change at any
time. You should not rely on them for any kind of capacity planning.
{{< /warning >}}

#### Downtime

Resizing operation can incur downtime unless used with WAIT UNTIL READY option.
See [zero-downtime cluster resizing](#zero-downtime-cluster-resizing) for
details.

#### Zero-downtime cluster resizing

{{< private-preview />}}

You can use the `WAIT UNTIL READY` option to perform a zero-downtime resizing,
which incurs **no downtime**. Instead of restarting the cluster, this approach
spins up an additional cluster replica under the covers with the desired new
size, waits for the replica to be hydrated, and then replaces the original
replica.

```sql
ALTER CLUSTER c1
SET (SIZE 'M.1-xsmall') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

The `ALTER` statement is blocking and will return only when the new replica
becomes ready. This could take as long as the specified timeout. During this
operation, any other reconfiguration command issued against this cluster will
fail. Additionally, any connection interruption or statement cancelation will
cause a rollback — no size change will take effect in that case.

{{< include-md file="shared-content/alter-cluster-wait-until-ready-note.md" >}}

### Replication factor

The `REPLICATION FACTOR` option determines the number of replicas provisioned
for the cluster. Each replica of the cluster provisions a new pool of compute
resources to perform exactly the same computations on exactly the same data.
Each replica incurs cost, calculated as `cluster size * replication factor` per
second. See [Usage & billing](/administration/billing/) for more details.

#### Replication factor and fault tolerance

Provisioning more than one replica provides **fault tolerance**. Clusters with
multiple replicas can tolerate failures of the underlying hardware that cause a
replica to become unreachable. As long as one replica of the cluster remains
available, the cluster can continue to maintain dataflows and serve queries.

{{< note >}}

- Each replica incurs cost, calculated as `cluster size *
  replication factor` per second. See [Usage &
  billing](/administration/billing/) for more details.

- Increasing the replication factor does **not** increase the cluster's work
  capacity. Replicas are exact copies of one another: each replica must do
  exactly the same work (i.e., maintain the same dataflows and process the same
  queries) as all the other replicas of the cluster.

  To increase the capacity of a cluster, you must increase its
  [size](#resizing).

{{< /note >}}

Materialize automatically assigns names to replicas (e.g., `r1`, `r2`). You can
view information about individual replicas in the Materialize console and the system
catalog.

#### Availability guarantees

When provisioning replicas,

- For clusters sized **under `3200cc`**, Materialize guarantees that all
  provisioned replicas in a cluster are spread across the underlying cloud
  provider's availability zones.

- For clusters sized at **`3200cc` and above**, even distribution of replicas
  across availability zones **cannot** be guaranteed.


## Required privileges

To execute the `ALTER CLUSTER` command, you need:

{{< include-md file="shared-content/sql-command-privileges/alter-cluster.md" >}}

See also:

- [Access control (Materialize Cloud)](/security/cloud/access-control/)
- [Access control (Materialize
  Self-Managed)](/security/self-managed/access-control/)

### Rename restrictions

You cannot rename system clusters, such as `mz_system` and `mz_catalog_server`.


## Examples

### Replication factor

The following example uses `ALTER CLUSTER` to update the `REPLICATION
FACTOR` of cluster `c1` to ``2``:

```mzsql
ALTER CLUSTER c1 SET (REPLICATION FACTOR 2);
```

Increasing the `REPLICATION FACTOR` increases the cluster's [fault
tolerance](#replication-factor-and-fault-tolerance), not its work capacity.


### Resizing

You can alter the cluster size with **no downtime** (i.e., [zero-downtime
cluster resizing](#zero-downtime-cluster-resizing)) by running the `ALTER
CLUSTER` command with the `WAIT UNTIL READY` [option](#with-options):

```mzsql
ALTER CLUSTER c1
SET (SIZE 'M.1-xsmall') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

{{< include-md file="shared-content/alter-cluster-wait-until-ready-note.md" >}}

Alternatively, you can alter the cluster size immediately, without waiting, by
running the `ALTER CLUSTER` command:

```mzsql
ALTER CLUSTER c1 SET (SIZE 'M.1-xsmall');
```

This will incur downtime when the cluster contains objects that need
re-hydration before they are ready. This includes indexes, materialized views,
and some types of sources.

### Schedule

{{< private-preview />}}

For use cases that require using [scheduled clusters](/sql/create-cluster/#scheduling),
you can set or change the originally configured schedule and related options
using the `ALTER CLUSTER` command.
```sql
ALTER CLUSTER c1 SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

See the reference documentation for [`CREATE
CLUSTER`](../create-cluster/#scheduling) or [`CREATE MATERIALIZED
VIEW`](../create-materialized-view/#refresh-strategies) for more details on
scheduled clusters.

### Converting unmanaged to managed clusters

{{< note >}}

When getting started with Materialize, we recommend using managed clusters. You
can convert any unmanaged clusters to managed clusters by following the
instructions below.

{{< /note >}}

Alter the `managed` status of a cluster to managed:

```mzsql
ALTER CLUSTER c1 SET (MANAGED);
```

Materialize permits converting an unmanged cluster to a managed cluster if
the following conditions are met:

* The cluster replica names are `r1`, `r2`, ..., `rN`.
* All replicas have the same size.
* If there are no replicas, `SIZE` needs to be specified.
* If specified, the replication factor must match the number of replicas.

Note that the cluster will not have settings for the availability zones, and
compute-specific settings. If needed, these can be set explicitly.

## See also

- [`CREATE CLUSTER`](/sql/create-cluster/)
- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)




---

## ALTER CLUSTER REPLICA


Use `ALTER CLUSTER REPLICA` to:
- Rename a cluster replica.
- Change owner of a cluster replica.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a cluster replica:

{{% include-syntax file="examples/alter_cluster_replica" example="syntax-rename" %}}

{{< note >}}
You cannot rename replicas in system clusters.
{{< /note >}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a cluster replica:

{{% include-syntax file="examples/alter_cluster_replica" example="syntax-change-owner" %}}

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-cluster-replica.md" >}}

## Example

The following changes the owner of the cluster replica `production.r1` to
`admin`.  The user running the command must:
- Be the current owner;
- Be a member of `admin`; and
- Have `CREATE` privilege on the `production` cluster.

```mzsql
ALTER CLUSTER REPLICA production.r1 OWNER TO admin;
```




---

## ALTER CONNECTION


Use `ALTER CONNECTION` to:

- Modify the parameters of a connection, such as the hostname to which it
  points.
- Rotate the key pairs associated with an [SSH tunnel connection].
- Rename a connection.
- Change owner of a connection.

## Syntax

{{< tabs >}}
{{< tab "SET/DROP/RESET options" >}}

### SET/DROP/RESET options

To modify connection parameters:

{{% include-syntax file="examples/alter_connection" example="syntax-set-drop-reset" %}}

{{< /tab >}}
{{< tab "ROTATE KEYS" >}}

### ROTATE KEYS

To rotate SSH tunnel connection key pairs:

{{% include-syntax file="examples/alter_connection" example="syntax-rotate-keys" %}}


{{< /tab >}}

{{< tab "Rename" >}}

### Rename

To rename a connection

{{% include-syntax file="examples/alter_connection" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a connection:

{{% include-syntax file="examples/alter_connection" example="syntax-change-owner" %}}

{{< /tab >}}
{{< /tabs >}}

## Details

### `SET`, `RESET`, `DROP`

These subcommands let you modify the parameters of a connection.

* **RESET** and **DROP** are synonyms and will return the parameter to its
    original state. For instance, if the connection has a default port, **DROP**
    will return it to the default value.
* All provided changes are applied atomically.
* The same parameter cannot have multiple modifications.

For the available parameters for each type of connection, see [`CREATE
CONNECTION`](/sql/create-connection).

### `ROTATE KEYS`

The `ROTATE KEYS` command can be used to change the key pairs associated with
an [SSH tunnel connection] without causing downtime.

Each SSH tunnel connection is associated with two key pairs. The public keys
for the key pairs are announced in the [`mz_ssh_tunnel_connections`]
system table in the `public_key_1` and `public_key_2` columns.

Upon executing the `ROTATE KEYS` command, Materialize deletes the first key
pair, promotes the second key pair to the first key pair, and generates a new
second key pair. The connection's row in `mz_ssh_tunnel_connections` is updated
accordingly: the `public_key_1` column will contain the public key that was
formely in the `public_key_2` column, and the `public_key_2` column will contain
a new public key.

After executing `ROTATE KEYS`, you should update your SSH bastion server with
the new public keys:

* Remove the public key that was formely in the `public_key_1` column.
* Add the new public key from the `public_key_2` column.

Throughout the entire process, the SSH bastion server is configured to permit
authentication from at least one of the keys that Materialize will authenticate
with, so Materialize's ability to connect is never interrupted.

You must take care to update the SSH bastion server with the new keys after
every execution of the `ROTATE KEYS` command. If you rotate keys twice in
succession without adding the new keys to the bastion server, Materialize will
be unable to authenticate with the bastion server.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-connection.md" >}}

## Related pages

-   [`CREATE CONNECTION`](/sql/create-connection/)
-   [`SHOW CONNECTIONS`](/sql/show-connections)

[SSH tunnel connection]: /sql/create-connection/#ssh-tunnel
[`mz_ssh_tunnel_connections`]: /sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections




---

## ALTER DATABASE


Use `ALTER DATABASE` to:
- Rename a database.
- Change owner of a database.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a database:

{{% include-syntax file="examples/alter_database" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a database:

{{% include-syntax file="examples/alter_database" example="syntax-change-owner" %}}

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-database.md" >}}




---

## ALTER DEFAULT PRIVILEGES


Use `ALTER DEFAULT PRIVILEGES` to:

- Define default privileges that will be applied to objects created in the
future. It does not affect any existing objects.

- Revoke previously created default privileges on objects created in the future.

All new environments are created with a single default privilege, `USAGE` is
granted on all `TYPES` to the `PUBLIC` role. This can be revoked like any other
default privilege.

## Syntax

{{< tabs >}}
{{< tab "GRANT" >}}
### GRANT

`ALTER DEFAULT PRIVILEGES` defines default privileges that will be applied to
objects created by a role in the future. It does not affect any existing
objects.

Default privileges are specified for a certain object type and can be applied to
all objects of that type, all objects of that type created within a specific set
of databases, or all objects of that type created within a specific set of
schemas. Default privileges are also specified for objects created by a certain
set of roles or by all roles.

{{% include-syntax file="examples/alter_default_privileges" example="syntax-grant" %}}

{{< /tab >}}
{{< tab "REVOKE" >}}
### REVOKE

{{< note >}}
`ALTER DEFAULT PRIVILEGES` cannot be used to revoke the default owner privileges
on objects. Those privileges must be revoked manually after the object is
created. Though owners can always re-grant themselves any privilege on an object
that they own.
{{< /note >}}

The `REVOKE` variant of `ALTER DEFAULT PRIVILEGES` is used to revoke previously
created default privileges on objects created in the future. It will not revoke
any privileges on objects that have already been created. When revoking a
default privilege, all the fields in the revoke statement (`creator_role`,
`schema_name`, `database_name`, `privilege`, `target_role`) must exactly match
an existing default privilege. The existing default privileges can easily be
viewed by the following query: `SELECT * FROM
mz_internal.mz_show_default_privileges`.

{{% include-syntax file="examples/alter_default_privileges" example="syntax-revoke" %}}

{{< /tab >}}
{{< /tabs >}}

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

{{< include-md file="shared-content/sql-command-privileges/alter-default-privileges.md" >}}

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




---

## ALTER INDEX


Use `ALTER INDEX` to:
- Rename an index.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename an index:

{{% include-syntax file="examples/alter_index" example="syntax-rename" %}}

{{< /tab >}}

{{< /tabs >}}


## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-index.md" >}}

## Related pages

- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW SINKS`](/sql/show-sinks)




---

## ALTER MATERIALIZED VIEW


Use `ALTER  MATERIALIZED VIEW` to:

- Rename a materialized view.
- Change owner of a materialized view.
- Change retain history configuration for the materialized view.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-change-owner" %}}

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

### (Re)Set retain history config

To set the retention history for a materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-set-retain-history" %}}

To reset the retention history to the default for a materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-reset-retain-history" %}}

{{< /tab >}}
{{< /tabs >}}

## Details

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-materialized-view.md" >}}

## Related pages

- [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](/sql/show-create-materialized-view)




---

## ALTER NETWORK POLICY (Cloud)


*Available for Materialize Cloud only*

`ALTER NETWORK POLICY` alters an existing network policy. Network policies are
part of Materialize's framework for [access control](/security/cloud/).

Changes to a network policy will only affect new connections
and **will not** terminate active connections.

## Syntax

{{% include-syntax file="examples/alter_network_policy" example="syntax" %}}

## Details

### Pre-installed network policy

When you enable a Materialize region, a default network policy named `default`
will be pre-installed. This policy has a wide open ingress rule `allow
0.0.0.0/0`. You can modify or drop this network policy at any time.

{{< note >}}
The default value for the `network_policy` session parameter is `default`.
Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
change the default value.
{{< /note >}}

### Lockout prevention

To prevent lockout, the IP of the active user is validated against the policy
changes requested. This prevents users from modifying network policies in a way
that could lock them out of the system.

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-network-policy.md" >}}

## Examples

```mzsql
CREATE NETWORK POLICY office_access_policy (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32')
  )
);
```

```mzsql
ALTER NETWORK POLICY office_access_policy SET (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32'),
    boston (action='allow',direction='ingress',address='4.5.6.7/32')
  )
);
```

```mzsql
ALTER SYSTEM SET network_policy = office_access_policy;
```

## Related pages
- [`CREATE NETWORK POLICY`](../create-network-policy)
- [`DROP NETWORK POLICY`](../drop-network-policy)




---

## ALTER ROLE


`ALTER ROLE` alters the attributes of an existing role.[^1]

[^1]: Materialize does not support the `SET ROLE` command.

## Syntax


{{< tabs >}}

{{< tab "Cloud" >}}

### Cloud

{{% include-example file="examples/rbac-cloud/alter_roles" example="alter-role-syntax" %}}

{{% include-example file="examples/rbac-cloud/alter_roles"
example="alter-role-options" %}}

**Note:**
{{% include-example file="examples/rbac-cloud/alter_roles"
example="alter-role-details" %}}
{{< /tab >}}
{{< tab "Self-Managed" >}}
### Self-Managed

{{% include-example file="examples/rbac-sm/alter_roles" example="alter-role-syntax" %}}

{{% include-example file="examples/rbac-sm/alter_roles"
example="alter-role-options" %}}

**Note:**
{{% include-example file="examples/rbac-sm/alter_roles"
example="alter-role-details" %}}
{{< /tab >}}
{{< /tabs >}}

## Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `ALTER ROLE ... INHERIT INHERIT`.

## Examples

#### Altering the attributes of a role

```mzsql
ALTER ROLE rj INHERIT;
```
```mzsql
SELECT name, inherit FROM mz_roles WHERE name = 'rj';
```
```nofmt
rj  true
```

#### Setting configuration parameters for a role

```mzsql
SHOW cluster;
quickstart

ALTER ROLE rj SET cluster TO rj_compute;

-- Role parameters only take effect for new sessions.
SHOW cluster;
quickstart

-- Start a new SQL session with the role 'rj'.
SHOW cluster;
rj_compute

-- In a new SQL session with a role that is not 'rj'.
SHOW cluster;
quickstart
```


#### Making a role a superuser  (Self-Managed)

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
ALTER ROLE rj SUPERUSER;
```

To verify that the role has superuser privileges, you can query the `pg_authid` system catalog:

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```

```nofmt
rj  t
```

#### Removing the superuser attribute from a role (Self-Managed)

NOSUPERUSER will remove the superuser attribute from a role, preventing it from having unrestricted access to all objects in the system.

```mzsql
ALTER ROLE rj NOSUPERUSER;
```

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```

```nofmt
rj  f
```

#### Removing a role's password (Self-Managed)

{{< warning >}}
Setting a NULL password removes the password.
{{< /warning >}}

```mzsql
ALTER ROLE rj PASSWORD NULL;
```

#### Changing a role's password (Self-Managed)

```mzsql
ALTER ROLE rj PASSWORD 'new_password';
```
## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-role.md" >}}

## Related pages

- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)




---

## ALTER SCHEMA


Use `ALTER SCHEMA` to:
- Swap the name of a schema with that of another schema.
- Rename a schema.
- Change owner of a schema.


## Syntax

{{< tabs >}}
{{< tab "Swap with" >}}

### Swap with

To swap the name of a schema with that of another schema:

{{% include-syntax file="examples/alter_schema" example="syntax-swap-with" %}}

{{< /tab >}}
{{< tab "Rename schema" >}}

### Rename schema

To rename a schema:

{{% include-syntax file="examples/alter_schema" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner to" >}}

### Change owner to

To change the owner of a schema:

{{% include-syntax file="examples/alter_schema" example="syntax-change-owner" %}}

{{< /tab >}}

{{< /tabs >}}


## Examples

### Swap schema names

Swapping two schemas is useful for a blue/green deployment. The following swaps
the names of the `blue` and `green` schemas.

```mzsql
CREATE SCHEMA blue;
CREATE TABLE blue.numbers (n int);

CREATE SCHEMA green;
CREATE TABLE green.tags (tag text);

ALTER SCHEMA blue SWAP WITH green;

-- The schema which was previously named 'green' is now named 'blue'.
SELECT * FROM blue.tags;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-schema.md" >}}

## See also

- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW SECRETS`](/sql/show-secrets)
- [`SHOW SINKS`](/sql/show-sinks)




---

## ALTER SECRET


Use `ALTER SECRET` to:

- Change the value of the secret.
- Rename a secret.
- Change owner of a secret.

## Syntax

{{< tabs >}}
{{< tab "Change value" >}}

### Change value

To change the value of a secret:

{{% include-syntax file="examples/alter_secret" example="syntax-change-value" %}}

{{< /tab >}}
{{< tab "Rename" >}}

### Rename

To rename a secret:

{{% include-syntax file="examples/alter_secret" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a secret:

{{% include-syntax file="examples/alter_secret" example="syntax-change-owner" %}}

{{< /tab >}}
{{< /tabs >}}

## Details

### Changing the secret value

After an `ALTER SECRET` command is executed:

  * Future [`CREATE CONNECTION`], [`CREATE SOURCE`], and [`CREATE SINK`]
    commands will use the new value of the secret immediately.

  * Running sources and sinks that reference the secret will **not** immediately
    use the new value of the secret. Sources and sinks may cache the old secret
    value for several weeks.

    To force a running source or sink to refresh its secrets, drop and recreate
    all replicas of the cluster hosting the source or sink.

    For a managed cluster:

    ```
    ALTER CLUSTER storage_cluster SET (REPLICATION FACTOR = 0);
    ALTER CLUSTER storage_cluster SET (REPLICATION FACTOR = 1);
    ```

    For an unmanaged cluster:

    ```
    DROP CLUSTER REPLICA storage_cluster.r1;
    CREATE CLUSTER REPLICA storage_cluster.r1 (SIZE = '<original size>');
        ```

## Examples

```mzsql
ALTER SECRET kafka_ca_cert AS decode('c2VjcmV0Cg==', 'base64');
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-secret.md" >}}

## Related pages

- [`SHOW SECRETS`](/sql/show-secrets)
- [`DROP SECRET`](/sql/drop-secret)

[`CREATE CONNECTION`]: /sql/create-connection/
[`CREATE SOURCE`]: /sql/create-source
[`CREATE SINK`]: /sql/create-sink
[`ALTER SOURCE`]: /sql/alter-source
[`ALTER SINK`]: /sql/alter-sink




---

## ALTER SINK


Use `ALTER SINK` to:
- Change the relation you want to sink from. This is useful in the context of
[blue/green deployments](/manage/dbt/blue-green-deployments/).
- Rename a sink.
- Change owner of a sink.

## Syntax

{{< tabs>}}
{{< tab "Change sink from relation" >}}

### Change sink from relation

To change the relation you want to sink from:

{{% include-syntax file="examples/alter_sink" example="syntax-set-from" %}}

{{< /tab >}}
{{< tab "Rename" >}}

### Rename

To rename a sink:

{{% include-syntax file="examples/alter_sink" example="syntax-rename" %}}



{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a sink:

{{% include-syntax file="examples/alter_sink" example="syntax-change-owner" %}}
{{< /tab >}}
{{< /tabs >}}

## Details

### Changing sink from relation

#### Valid schema changes

For `ALTER SINK` to be successful, the newly specified relation must lead to a
valid sink definition with the same conditions as the original `CREATE SINK`
statement.

When using the Avro format with a schema registry, the generated Avro
schema for the new relation must be compatible with the previously published
schema. If that's not the case, the `ALTER SINK` command will succeed, but the
subsequent execution of the sink will result in errors and will not be able to
make progress.

To monitor the status of a sink after an `ALTER SINK` command, navigate to the
respective object page in the [Materialize console](/console/),
or query the [`mz_internal.mz_sink_statuses`](/sql/system-catalog/mz_internal/#mz_sink_statuses)
system catalog view.

#### Cutover timestamp

To alter the upstream relation a sink depends on while ensuring continuity in
data processing, Materialize must pick a consistent cutover timestamp. When you
execute an `ALTER SINK` command, the resulting output will contain:
- all updates that happened before the cutover timestamp for the old
relation, and
- all updates that happened after the cutover timestamp for the new
relation.

{{< note >}}
To select a consistent timestamp, Materialize must wait for the previous
definition of the sink to emit results up until the oldest timestamp at which
the contents of the new upstream relation are known. Attempting to `ALTER` an
unhealthy sink that can't make progress will result in the command timing out.
{{</ note >}}

#### Cutover scenarios and workarounds

Because Materialize emits updates from the new relation **only** if
they occur after the cutover timestamp, the following scenarios may occur:

##### Scenario 1: Topic contains stale value for a key

Since cutting over a sink to a new upstream relation using `ALTER SINK` does not
emit a snapshot of the new relation, all keys will appear to have the old value
for the key in the previous relation until an update happens to them. At that
point, the current value will be published to the topic.

Consumers of the topic must be prepared to handle an old value for a key, for
example by filling in additional columns with default values.

**Workarounds**:

- Use an intermediary, temporary view to handle the cutover scenario difference.
See [Example: Handle cutover scenarios](#handle-cutover-scenarios).

- Alternatively, forcing an update to all the keys after `ALTER SINK` will force
the sink to re-emit all the updates.

##### Scenario 2: Topic is missing a key that exists in the new relation

As a consequence of not re-emitting a snapshot after `ALTER SINK`, if additional
keys exist in the new relation that are not present in the old one, these will
not be visible in the topic after the cutover. The keys will remain absent until
an update occurs for the keys, at which point Materialize will emit a record to
the topic containing the new value.

**Workarounds**:

- Use an intermediary, temporary view to handle the cutover scenario difference.
See [Example: Handle cutover scenarios](#handle-cutover-scenarios).

- Alternatively, ensure that both the old and the new relations have identical
keyspaces to avoid the scenario.

##### Scenario 3: Topic contains a key that does not exist in the new relation

Materialize does not compare the contents of the old relation with the new
relation when cutting a sink over. This means that, if the old relation
contains additional keys that are not present in the new one, these records
will remain in the topic without a corresponding tombstone record. This may
cause readers to assume that certain keys exist when they don't.

**Workarounds**:

- Use an intermediary, temporary view to handle the cutover scenario difference.
See [Example: Handle cutover scenarios](#handle-cutover-scenarios).

- Alternatively, ensure that both the old and the new relations have identical
keyspaces to avoid the scenario.

### Catalog objects

A sink cannot be created directly on a [catalog object](/sql/system-catalog/).
As a workaround, you can create a materialized view on a catalog object and
create a sink on the materialized view.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-sink.md" >}}

## Examples

### Alter sink

The following example alters a sink originally created from `matview_old` to use
`matview_new` instead.

{{% include-example file="examples/alter_sink"
example="alter-sink-create-original-sink" %}}

{{% include-example file="examples/alter_sink" example="alter-sink-simple" %}}

### Handle cutover scenarios

{{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-intro" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-1-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-create-transition-mv" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-2-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-alter-sink-to-transition" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-3-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-update-switch" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-4-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-alter-sink-to-new-mv" %}}

1. {{< include-example file="examples/alter_sink"
example="handle-cutover-scenarios-step-5-intro" >}}

   {{% include-example file="examples/alter_sink"
example="handle-cutover-scenarios-drop-intermediary-objects" %}}

## See also

- [`CREATE SINK`](/sql/create-sink/)
- [`SHOW SINKS`](/sql/show-sinks)




---

## ALTER SOURCE


Use `ALTER SOURCE` to:

- Add a subsource to a source.
- Rename a source.
- Change owner of a source.
- Change retain history configuration for the source.

## Syntax

{{< tabs >}}
{{< tab "Add subsource" >}}

### Add subsource

To add the specified upstream table(s) to the specified PostgreSQL/MySQL/SQL Server source:

{{% include-syntax file="examples/alter_source" example="syntax-add-subsource" %}}

{{< note >}}
{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}
{{< /note >}}

{{< /tab >}}

{{< tab "Rename" >}}

### Rename

To rename a source:

{{% include-syntax file="examples/alter_source" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a source:

{{% include-syntax file="examples/alter_source" example="syntax-change-owner" %}}

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

### (Re)Set retain history config

To set the retention history for a source:

{{% include-syntax file="examples/alter_source" example="syntax-set-retain-history" %}}

To reset the retention history to the default for a source:

{{% include-syntax file="examples/alter_source" example="syntax-reset-retain-history" %}}

{{< /tab >}}
{{< /tabs >}}


## Context

### Adding subsources to a PostgreSQL/MySQL/SQL Server source

Note that using a combination of dropping and adding subsources lets you change
the schema of the PostgreSQL/MySQL/SQL Server tables that are ingested.

{{< important >}}
{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}
{{< /important >}}

### Dropping subsources from a PostgreSQL/MySQL/SQL Server source

Dropping a subsource prevents Materialize from ingesting any data from it, in
addition to dropping any state that Materialize previously had for the table
(such as its contents).

If a subsource encounters a deterministic error, such as an incompatible schema
change (e.g. dropping an ingested column), you can drop the subsource. If you
want to ingest it with its new schema, you can then add it as a new subsource.

You cannot drop the "progress subsource".

## Examples

### Adding subsources

```mzsql
ALTER SOURCE pg_src ADD SUBSOURCE tbl_a, tbl_b AS b WITH (TEXT COLUMNS [tbl_a.col]);
```

{{< important >}}
{{< include-md file="shared-content/alter-source-snapshot-blocking-behavior.md"
>}}
{{< /important >}}

### Dropping subsources

To drop a subsource, use the [`DROP SOURCE`](/sql/drop-source/) command:

```mzsql
DROP SOURCE tbl_a, b CASCADE;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-source.md" >}}

## See also

- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP SOURCE`](/sql/drop-source/)
- [`SHOW SOURCES`](/sql/show-sources)




---

## ALTER SYSTEM RESET


Use `ALTER SYSTEM RESET` to globally restore the value of a configuration
parameter to its default value. This command is an alternative spelling for
[`ALTER SYSTEM SET...TO DEFAULT`](../alter-system-set).

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

```mzsql
ALTER SYSTEM RESET <config>;
```

Syntax element | Description
---------------|------------
`<config>`     | The configuration parameter's name.

{{% configuration-parameters %}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-system-reset.md" >}}

## Related pages

- [`SHOW`](../show)
- [`ALTER SYSTEM SET`](../alter-system-set)




---

## ALTER SYSTEM SET


Use `ALTER SYSTEM SET` to globally modify the value of a configuration parameter.

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

```mzsql
ALTER SYSTEM SET <config> [TO|=] <value|DEFAULT>
```

Syntax element | Description
---------------|------------
`<config>`              | The name of the configuration parameter to modify.
`<value>`               | The value to assign to the configuration parameter.
**DEFAULT**             | Reset the configuration parameter's default value. Equivalent to [`ALTER SYSTEM RESET`](../alter-system-reset).

{{% configuration-parameters %}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-system-set.md" >}}

## Related pages

- [`ALTER SYSTEM RESET`](../alter-system-reset)
- [`SHOW`](../show)




---

## ALTER TABLE


Use `ALTER TABLE` to:

- Rename a table.
- Change owner of a table.
- Change retain history configuration for the table.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a table:

{{% include-syntax file="examples/alter_table" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a table:

{{% include-syntax file="examples/alter_table" example="syntax-change-owner" %}}

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

### (Re)Set retain history config

To set the retention history for a user-populated table:

{{% include-syntax file="examples/alter_table" example="syntax-set-retain-history" %}}

To reset the retention history to the default for a user-populated table:

{{% include-syntax file="examples/alter_table" example="syntax-reset-retain-history" %}}

{{< /tab >}}
{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-table.md" >}}




---

## ALTER TYPE


Use `ALTER TYPE` to:
- Rename a type.
- Change owner of a type.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a type:

{{% include-syntax file="examples/alter_type" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a type:

{{% include-syntax file="examples/alter_type" example="syntax-change-owner" %}}

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-type.md" >}}




---

## ALTER VIEW


Use `ALTER VIEW` to:
- Rename a view.
- Change owner of a view.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a view:

{{% include-syntax file="examples/alter_view" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a view:

{{% include-syntax file="examples/alter_view" example="syntax-change-owner" %}}

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-view.md" >}}




---

## BEGIN


{{% txns/txn-details %}}

Materialize only supports [**read-only** transactions](#read-only-transactions)
or [**write-only** (specifically, insert-only)
transactions](#write-only-transactions). See [Details](#details) for more
information.

## Syntax

```mzsql
BEGIN [ <option>, ... ];
```

You can specify the following optional settings for `BEGIN`:

Option | Description
-------|----------
`ISOLATION LEVEL <level>` | *Optional*. If specified, sets the transaction [isolation level](/get-started/isolation-level).
`READ ONLY` | <a name="begin-option-read-only"></a> *Optional*. If specified, restricts the transaction to read-only operations. If unspecified, Materialize restricts the transaction to read-only or insert-only operations based on the first statement in the transaction.

## Details

Transactions in Materialize are either [**read-only**
transactions](#read-only-transactions) or [**write-only**
transactions](#insert-only-transactions) as determined by either:

- The first statement after the `BEGIN`, or

- The [`READ ONLY`](#begin-option-read-only) option is specified.

### Read-only transactions

In Materialize, read-only transactions can be either:

- a [`SELECT`only transaction](#select-only-transactions) that only contains
  [`SELECT`] statements or

- a [`SUBSCRIBE`-based transactions](#subscribe-based-transactions) that only
    contains a single [`DECLARE ... CURSOR FOR`] [`SUBSCRIBE`] statement
    followed by subsequent [`FETCH`](/sql/fetch) statement(s). [^1]

{{< note >}}

- During the first query, a timestamp is chosen that is valid for all of the
  objects referenced in the query. This timestamp will be used for all other
  queries in the transaction.

- The transaction will additionally hold back normal compaction of the objects,
  potentially increasing memory usage for very long running transactions.

{{</ note >}}

#### SELECT-only transactions

A **SELECT-only** transaction only contains [`SELECT`](/sql/select) statement.

The first [`SELECT`](/sql/select) statement:

- Determines the timestamp that will be used for all other queries in the
  transaction.

- Determines  which objects can be queried in the transaction block.

Specifically,

- Subsequent [`SELECT`](/sql/select) statements in the transaction can only
  reference objects from the [schema(s)](/sql/namespaces/) referenced in the
  first [`SELECT`](/sql/select) statement (as well as a subset of objects from
  the `mz_catalog` and `mz_internal` schemas).

- These objects must have existed at beginning of the transaction.

For example, in the transaction block below, first `SELECT` statement in the
transaction restricts subsequent selects to objects from `test` and `public`
schemas.

```mzsql
BEGIN;
SELECT o.*,i.price,o.quantity * i.price as subtotal
FROM test.orders as o
JOIN public.items as i ON o.item = i.item;

-- Subsequent queries must only reference objects from the test and public schemas that existed at the start of the transaction.

SELECT * FROM test.auctions limit 1;
SELECT * FROM public.sales_items;
COMMIT;
```

Reading from a schema not referenced in the first statement or querying objects
created after the transaction started (even if in the allowed schema(s)) will
produce a [Same timedomain error](#same-timedomain-error).  [Same timedomain
error](#same-timedomain-error) provides a list of the allowed objects in the
transaction.

##### Same timedomain error

```none
Transactions can only reference objects in the same timedomain.
```

The first `SELECT` statement in a transaction determines which schemas the
subsequent `SELECT` statements in the transaction can query. If a subsequent
`SELECT` references an object from another schema or an object created after the
transaction started, the transaction will error with the same time domain error.

The timedomain error lists both the objects that are not in the timedomain as
well as the objects that can be referenced in the transaction (i.e., in the
timedomain).

If an object in the timedomain is a view, it will be replaced with the objects
in the view definition.

#### SUBSCRIBE-based transactions

A [`SUBSCRIBE`]-based transaction only contains a single [`DECLARE ... CURSOR
FOR`] [`SUBSCRIBE`] statement followed by subsequent [`FETCH`](/sql/fetch)
statement(s). [^1]

```mzsql
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM flippers);

-- Subsequent queries must only FETCH from the cursor

FETCH 10 c WITH (timeout='1s');
FETCH 20 c WITH (timeout='1s');
COMMIT;
```

[^1]: A [`SUBSCRIBE`-based transaction](#subscribe-based-transactions) can start
with a  [`SUBSCRIBE`] statement (or `COPY (SUBSCRIBE ...) TO STDOUT`) instead of
a `DECLARE ... FOR SUBSCRIBE` but will end with a rollback since you must cancel
the SUBSCRIBE statementin order to issue the `COMMIT`/`ROLLBACK` statement to
end the transaction block.

### Write-only transactions

In Materialize, a write-only transaction is an [INSERT-only
transaction](#insert-only-transactions) that only contains [`INSERT`]
statements.

#### INSERT-only transactions

{{% txns/txn-insert-only %}}

## See also

- [`COMMIT`](/sql/commit)
- [`ROLLBACK`](/sql/rollback)

[`BEGIN`]: /sql/begin/
[`ROLLBACK`]: /sql/rollback/
[`COMMIT`]: /sql/commit/
[`SELECT`]: /sql/select/
[`SUBSCRIBE`]: /sql/subscribe/
[`DECLARE ... CURSOR FOR`]: /sql/declare/
[`INSERT`]: /sql/insert/




---

## CLOSE


Use `CLOSE` to close a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

```mzsql
CLOSE <cursor_name>;
```

Syntax element | Description
---------------|------------
`<cursor_name>` | The name of an open cursor to close.




---

## COMMENT ON


`COMMENT ON ...` adds or updates the comment of an object.

## Syntax

{{< diagram "comment-on.svg" >}}

## Details

`COMMENT ON` stores a comment about an object in the database. Each object can only have one
comment associated with it, so successive calls of `COMMENT ON` to a single object will overwrite
the previous comment.

To read the comment on an object you need to query the [mz_internal.mz_comments](/sql/system-catalog/mz_internal/#mz_comments)
catalog table.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/comment-on.md" >}}

For more information on ownership and privileges, see [Role-based access
control](/security/).

## Examples

```mzsql
--- Add comments.
COMMENT ON TABLE foo IS 'this table is important';
COMMENT ON COLUMN foo.x IS 'holds all of the important data';

--- Update a comment.
COMMENT ON TABLE foo IS 'holds non-important data';

--- Remove a comment.
COMMENT ON TABLE foo IS NULL;

--- Read comments.
SELECT * FROM mz_internal.mz_comments;
```




---

## COMMIT


`COMMIT` ends the current [transaction](/sql/begin/#details). Upon the `COMMIT`
statement:

- If all transaction statements succeed, all changes are committed.

- If an error occurs, all changes are discarded; i.e., rolled back.

## Syntax

```mzsql
COMMIT;
```

## Details

{{% txns/txn-details %}}

Transactions in Materialize are either **read-only** transactions or
**write-only** (more specifically, **insert-only**) transactions.

For a [write-only (i.e., insert-only)
transaction](/sql/begin/#write-only-transactions), all statements in the
transaction are committed at the same timestamp.

## Examples

### Commit a write-only transaction {#write-only-transactions}

In Materialize, write-only transactions are **insert-only** transactions.

{{% txns/txn-insert-only %}}

### Commit a read-only transaction

In Materialize, read-only transactions can be either:

- a `SELECT` only transaction that only contains [`SELECT`] statements or

- a `SUBSCRIBE`-based transactions that only contains a single[`DECLARE ...
  CURSOR FOR`] [`SUBSCRIBE`] statement followed by subsequent
  [`FETCH`](/sql/fetch) statement(s).

For example:

```mzsql
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM flippers);

-- Subsequent queries must only FETCH from the cursor

FETCH 10 c WITH (timeout='1s');
FETCH 20 c WITH (timeout='1s');
COMMIT;
```

During the first query, a timestamp is chosen that is valid for all of the
objects referenced in the query. This timestamp will be used for all other
queries in the transaction.

{{< note >}}
The transaction will additionally hold back normal compaction of the objects,
potentially increasing memory usage for very long running transactions.
{{</ note >}}

## See also

- [`BEGIN`]
- [`ROLLBACK`]

[`BEGIN`]: /sql/begin/
[`ROLLBACK`]: /sql/rollback/
[`COMMIT`]: /sql/commit/
[`SELECT`]: /sql/select/
[`SUBSCRIBE`]: /sql/subscribe/
[`DECLARE ... CURSOR FOR`]: /sql/declare/
[`INSERT`]: /sql/insert




---

## COPY FROM


`COPY FROM` copies data into a table using the [Postgres `COPY` protocol][pg-copy-from].

## Syntax

{{< diagram "copy-from.svg" >}}

Field | Use
------|-----
_table_name_ | Copy values to this table.
**(**_column_...**)** | Correlate the inserted rows' columns to _table_name_'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column. <br/><br/>Without a column list, all columns must have data provided, and will be referenced using their order in the table. With a partial column list, all unreferenced columns will receive their default value.
_field_ | The name of the option you want to set.
_val_ | The value for the option.

### `WITH` options

The following options are valid within the `WITH` clause.

Name | Value type | Default value | Description
-----|-----------------|---------------|------------
`FORMAT` | `TEXT`, `CSV` | `TEXT` | Sets the input formatting method. For more information see [Text formatting](#text-formatting), [CSV formatting](#csv-formatting).
`DELIMITER` | Single-quoted one-byte character | Format-dependent | Overrides the format's default column delimiter.
`NULL` | Single-quoted strings | Format-dependent | Specifies the string that represents a _NULL_ value.
`QUOTE` | Single-quoted one-byte character | `"` | Specifies the character to signal a quoted string, which may contain the `DELIMITER` value (without beginning new columns). To include the `QUOTE` character itself in column, wrap the column's value in the `QUOTE` character and prefix all instance of the value you want to literally interpret with the `ESCAPE` value. _`FORMAT CSV` only_
`ESCAPE` | Single-quoted strings | `QUOTE`'s value | Specifies the character to allow instances of the `QUOTE` character to be parsed literally as part of a column's value. _`FORMAT CSV` only_
`HEADER`  | `boolean`   | `boolean`  | Specifies that the file contains a header line with the names of each column in the file. The first line is ignored on input.  _`FORMAT CSV` only._

Note that `DELIMITER` and `QUOTE` must use distinct values.

## Details

### Text formatting

As described in the **Text Format** section of [PostgreSQL's documentation][pg-copy-from].

### CSV formatting

As described in the **CSV Format** section of [PostgreSQL's documentation][pg-copy-from]
except that:

- More than one layer of escaped quote characters returns the wrong result.

- Quote characters must immediately follow a delimiter to be treated as
  expected.

- Single-column rows containing quoted end-of-data markers (e.g. `"\."`) will be
  treated as end-of-data markers despite being quoted. In PostgreSQL, this data
  would be escaped and would not terminate the data processing.

- Quoted null strings will be parsed as nulls, despite being quoted. In
  PostgreSQL, this data would be escaped.

  To ensure proper null handling, we recommend specifying a unique string for
  null values, and ensuring it is never quoted.

- Unterminated quotes are allowed, i.e. they do not generate errors. In
  PostgreSQL, all open unescaped quotation punctuation must have a matching
  piece of unescaped quotation punctuation or it generates an error.

## Example

```mzsql
COPY t FROM STDIN WITH (DELIMITER '|');
```

```mzsql
COPY t FROM STDIN (FORMAT CSV);
```

```mzsql
COPY t FROM STDIN (DELIMITER '|');
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/copy-from.md" >}}

[pg-copy-from]: https://www.postgresql.org/docs/14/sql-copy.html

## Limits

You can only copy up to 1 GiB of data at a time. If you need this limit increased, please [chat with our team](http://materialize.com/convert-account/).




---

## COPY TO


`COPY TO` outputs results from Materialize to standard output or object storage.
This command is useful to output [`SUBSCRIBE`](/sql/subscribe/) results
[to `stdout`](#copy-to-stdout), or perform [bulk exports to Amazon S3](#copy-to-s3).

## Copy to `stdout`

Copying results to `stdout` is useful to output the stream of updates from a
[`SUBSCRIBE`](/sql/subscribe/) command in interactive SQL clients like `psql`.

### Syntax {#copy-to-stdout-syntax}

{{< diagram "copy-to-stdout.svg" >}}

Field         | Use
--------------|-----
_query_       | The [`SELECT`](/sql/select) or [`SUBSCRIBE`](/sql/subscribe) query to output results for.
_field_       | The name of the option you want to set.
_val_         | The value for the option.

### `WITH` options {#copy-to-stdout-with-options}

Name     | Values                 | Default value | Description
---------|------------------------|---------------|-----------------------------------
`FORMAT` | `TEXT`,`BINARY`, `CSV` | `TEXT`        | Sets the output formatting method.

### Examples {#copy-to-stdout-examples}

#### Subscribing to a view with binary output

```mzsql
COPY (SUBSCRIBE some_view) TO STDOUT WITH (FORMAT binary);
```

## Copy to Amazon S3 and S3 compatible services {#copy-to-s3}

Copying results to Amazon S3 (or S3-compatible services) is useful to perform
tasks like periodic backups for auditing, or downstream processing in
analytical data warehouses like Snowflake, Databricks or BigQuery. For
step-by-step instructions, see the integration guide for [Amazon S3](/serve-results/s3/).

The `COPY TO` command is _one-shot_: every time you want to export results, you
must run the command. To automate exporting results on a regular basis, you can
set up scheduling, for example using a simple `cron`-like service or an
orchestration platform like Airflow or Dagster.

### Syntax {#copy-to-s3-syntax}

{{< diagram "copy-to-s3.svg" >}}

Field         | Use
--------------|-----
_query_       | The [`SELECT`](/sql/select) query to copy results out for.
_object_name_ | The name of the object to copy results out for.
**AWS CONNECTION** _connection_name_ | The name of the AWS connection to use in the `COPY TO` command. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#aws) documentation page.
_s3_uri_      | The unique resource identifier (URI) of the Amazon S3 bucket (and prefix) to store the output results in.
**FORMAT**    | The file format to write.
_field_       | The name of the option you want to set.
_val_         | The value for the option.

### `WITH` options {#copy-to-s3-with-options}

Name             | Values          | Default value | Description                       |
-----------------|-----------------|---------------|-----------------------------------|
`MAX FILE SIZE`  | `integer`       |               | Sets the approximate maximum file size (in bytes) of each file uploaded to the S3 bucket. |

### Supported formats {#copy-to-s3-supported-formats}

#### CSV {#copy-to-s3-csv}

**Syntax:** `FORMAT = 'csv'`

By default, Materialize writes CSV files using the following writer settings:

Setting     | Value
------------|---------------
delimiter   | `,`
quote       | `"`
escape      | `"`
header      | `false`

#### Parquet {#copy-to-s3-parquet}

**Syntax:** `FORMAT = 'parquet'`

Materialize writes Parquet files that aim for maximum compatibility with
downstream systems. By default, the following Parquet writer settings are
used:

Setting                       | Value
------------------------------|---------------
Writer version                | 1.0
Compression                   | `snappy`
Default column encoding       | Dictionary
Fallback column encoding      | Plain
Dictionary page encoding      | Plain
Dictionary data page encoding | `RLE_DICTIONARY`

If you run into a snag trying to ingest Parquet files produced by Materialize
into your downstream systems, please [contact our team](/support/)
or [open a bug report](https://github.com/MaterializeInc/materialize/discussions/new?category=bug-reports)!

##### Data types {#copy-to-s3-parquet-data-types}

Materialize converts the values in the result set to [Apache Arrow](https://arrow.apache.org/docs/index.html),
and then serializes this Arrow representation to Parquet. The Arrow schema is
embedded in the Parquet file metadata and allows reconstructing the Arrow
representation using a compatible reader.

Materialize also includes [Parquet `LogicalType` annotations](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#metadata)
where possible. However, many newer `LogicalType` annotations are not supported
in the 1.0 writer version.

Materialize also embeds its own type information into the Apache Arrow schema.
The field metadata in the schema contains an `ARROW:extension:name` annotation
to indicate the Materialize native type the field originated from.

Materialize type | Arrow extension name | [Arrow type](https://github.com/apache/arrow/blob/main/format/Schema.fbs) | [Parquet primitive type](https://parquet.apache.org/docs/file-format/types/) | [Parquet logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md)
----------------------------------|----------------------------|------------|-------------------|--------------
[`bigint`](/sql/types/integer/#bigint-info)         | `materialize.v1.bigint`    | `int64` | `INT64`
[`boolean`](/sql/types/boolean/)        | `materialize.v1.boolean`   | `bool` | `BOOLEAN`
[`bytea`](/sql/types/bytea/)            | `materialize.v1.bytea`     | `large_binary` | `BYTE_ARRAY`
[`date`](/sql/types/date/)              | `materialize.v1.date`      | `date32` | `INT32` | `DATE`
[`double precision`](/sql/types/float/#double-precision-info) | `materialize.v1.double`    | `float64` | `DOUBLE`
[`integer`](/sql/types/integer/#integer-info)        | `materialize.v1.integer`   | `int32` | `INT32`
[`jsonb`](/sql/types/jsonb/)            | `materialize.v1.jsonb`     | `large_utf8` | `BYTE_ARRAY`
[`map`](/sql/types/map/)                | `materialize.v1.map`       | `map` (`struct` with fields `keys` and `values`) | Nested | `MAP`
[`list`](/sql/types/list/)              | `materialize.v1.list`      | `list` | Nested
[`numeric`](/sql/types/numeric/)        | `materialize.v1.numeric`   | `decimal128[38, 10 or max-scale]` | `FIXED_LEN_BYTE_ARRAY`             | `DECIMAL`
[`real`](/sql/types/float/#real-info)             | `materialize.v1.real`      | `float32` | `FLOAT`
[`smallint`](/sql/types/integer/#smallint-info)       | `materialize.v1.smallint`  | `int16` | `INT32` | `INT(16, true)`
[`text`](/sql/types/text/)              | `materialize.v1.text`      | `utf8` or `large_utf8` | `BYTE_ARRAY` | `STRING`
[`time`](/sql/types/time/)              | `materialize.v1.time`      | `time64[nanosecond]` | `INT64` | `TIME[isAdjustedToUTC = false, unit = NANOS]`
[`uint2`](/sql/types/uint/#uint2-info)             | `materialize.v1.uint2`     | `uint16` | `INT32` | `INT(16, false)`
[`uint4`](/sql/types/uint/#uint4-info)             | `materialize.v1.uint4`     | `uint32` | `INT32` | `INT(32, false)`
[`uint8`](/sql/types/uint/#uint8-info)             | `materialize.v1.uint8`     | `uint64` | `INT64` | `INT(64, false)`
[`timestamp`](/sql/types/timestamp/#timestamp-info)    | `materialize.v1.timestamp` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = false, unit = MICROS]`
[`timestamp with time zone`](/sql/types/timestamp/#timestamp-with-time-zone-info) | `materialize.v1.timestampz` | `time64[microsecond]` | `INT64` | `TIMESTAMP[isAdjustedToUTC = true, unit = MICROS]`
[Arrays](/sql/types/array/) (`[]`)      | `materialize.v1.array`     | `struct` with `list` field `items` and `uint8` field `dimensions` | Nested
[`uuid`](/sql/types/uuid/)              | `materialize.v1.uuid`      | `fixed_size_binary(16)` | `FIXED_LEN_BYTE_ARRAY`
[`oid`](/sql/types/oid/)                      | Unsupported
[`interval`](/sql/types/interval/)            | Unsupported
[`record`](/sql/types/record/)                | Unsupported

### Examples {#copy-to-s3-examples}

{{< tabs >}}
{{< tab "Parquet">}}

```mzsql
COPY some_view TO 's3://mz-to-snow/parquet/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'parquet'
  );
```

{{< /tab >}}

{{< tab "CSV">}}

```mzsql
COPY some_view TO 's3://mz-to-snow/csv/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'csv'
  );
```

{{< /tab >}}
{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/copy-to.md" >}}

## Related pages

- [`CREATE CONNECTION`](/sql/create-connection)
- Integration guides:
  - [Amazon S3](/serve-results/s3/)
  - [Snowflake (via S3)](/serve-results/snowflake/)




---

## CREATE CLUSTER


`CREATE CLUSTER` creates a new [cluster](/concepts/clusters/).

## Conceptual framework

A cluster is a pool of compute resources (CPU, memory, and scratch disk space)
for running your workloads.

The following operations require compute resources in Materialize, and so need
to be associated with a cluster:

- Executing [`SELECT`] and [`SUBSCRIBE`] statements.
- Maintaining [indexes](/concepts/indexes/) and [materialized views](/concepts/views/#materialized-views).
- Maintaining [sources](/concepts/sources/) and [sinks](/concepts/sinks/).

## Syntax

{{< diagram "create-managed-cluster.svg" >}}

### Options

{{< yaml-table data="syntax_options/create_cluster_options" >}}

## Details

### Initial state

Each Materialize region initially contains a [pre-installed cluster](/sql/show-clusters/#pre-installed-clusters)
named `quickstart` with a size of `25cc` and a replication factor of `1`. You
can drop or alter this cluster to suit your needs.

### Choosing a cluster

When performing an operation that requires a cluster, you must specify which
cluster you want to use. Not explicitly naming a cluster uses your session's
active cluster.

To show your session's active cluster, use the [`SHOW`](/sql/show) command:

```mzsql
SHOW cluster;
```

To switch your session's active cluster, use the [`SET`](/sql/set) command:

```mzsql
SET cluster = other_cluster;
```

### Resource isolation

Clusters provide **resource isolation.** Each cluster provisions a dedicated
pool of CPU, memory, and, optionally, scratch disk space.

All workloads on a given cluster will compete for access to these compute
resources. However, workloads on different clusters are strictly isolated from
one another. A given workload has access only to the CPU, memory, and scratch
disk of the cluster that it is running on.

Clusters are commonly used to isolate different classes of workloads. For
example, you could place your development workloads in a cluster named
`dev` and your production workloads in a cluster named `prod`.

<a name="legacy-sizes"></a>

### Size

The `SIZE` option determines the amount of compute resources available to the
cluster.

{{< tabs >}}
{{< tab "M.1 Clusters" >}}

{{< include-md file="shared-content/cluster-size-disclaimer.md" >}}

{{< yaml-table data="m1_cluster_sizing" >}}

{{< /tab >}}
{{< tab "Legacy cc Clusters" >}}

Materialize offers the following legacy cc cluster sizes:

{{< tip >}}
In most cases, you **should not** use legacy sizes. [M.1 sizes](#size)
offer better performance per credit for nearly all workloads. We recommend using
M.1 sizes for all new clusters, and recommend migrating existing
legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
customers during the transition period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.
{{< /tip >}}

* `25cc`
* `50cc`
* `100cc`
* `200cc`
* `300cc`
* `400cc`
* `600cc`
* `800cc`
* `1200cc`
* `1600cc`
* `3200cc`
* `6400cc`
* `128C`
* `256C`
* `512C`

The resource allocations are proportional to the number in the size name. For
example, a cluster of size `600cc` has 2x as much CPU, memory, and disk as a
cluster of size `300cc`, and 1.5x as much CPU, memory, and disk as a cluster of
size `400cc`. To determine the specific resource allocations for a size,
query the [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes) table.

{{< warning >}}
The values in the `mz_cluster_replica_sizes` table may change at any
time. You should not rely on them for any kind of capacity planning.
{{< /warning >}}

Clusters of larger sizes can process data faster and handle larger data volumes.
{{< /tab >}}
{{< tab "Legacy t-shirt Clusters" >}}

Materialize also offers some legacy t-shirt cluster sizes for upsert sources.

{{< tip >}}
In most cases, you **should not** use legacy t-shirt sizes. [M.1 sizes](#size)
offer better performance per credit for nearly all workloads. We recommend using
M.1 sizes for all new clusters, and recommend migrating existing
legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
customers during the transition period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.

{{< /tip >}}

{{< if-past "2024-04-15" >}}
{{< warning >}}
Materialize regions that were enabled after 15 April 2024 do not have access
to legacy sizes.
{{< /warning >}}
{{< /if-past >}}

When legacy sizes are enabled for a region, the following sizes are available:

* `3xsmall`
* `2xsmall`
* `xsmall`
* `small`
* `medium`
* `large`
* `xlarge`
* `2xlarge`
* `3xlarge`
* `4xlarge`
* `5xlarge`
* `6xlarge`

{{< /tab >}}
{{< /tabs >}}

See also:

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).

#### Cluster resizing

You can change the size of a cluster to respond to changes in your workload
using [`ALTER CLUSTER`](/sql/alter-cluster). Depending on the type of objects
the cluster is hosting, this operation **might incur downtime**.

See the reference documentation for [`ALTER
CLUSTER`](/sql/alter-cluster#zero-downtime-cluster-resizing) for more details
on cluster resizing.



### Replication factor

The `REPLICATION FACTOR` option determines the number of replicas provisioned
for the cluster. Each replica of the cluster provisions a new pool of compute
resources to perform exactly the same computations on exactly the same data.

Provisioning more than one replica improves **fault tolerance**. Clusters with
multiple replicas can tolerate failures of the underlying hardware that cause a
replica to become unreachable. As long as one replica of the cluster remains
available, the cluster can continue to maintain dataflows and serve queries.

Materialize makes the following guarantees when provisioning replicas:

- Replicas of a given cluster are never provisioned on the same underlying
  hardware.
- Replicas of a given cluster are spread as evenly as possible across the
  underlying cloud provider's availability zones.

Materialize automatically assigns names to replicas like `r1`, `r2`, etc. You
can view information about individual replicas in the console and the system
catalog, but you cannot directly modify individual replicas.

You can pause a cluster's work by specifying a replication factor of `0`. Doing
so removes all replicas of the cluster. Any indexes, materialized views,
sources, and sinks on the cluster will cease to make progress, and any queries
directed to the cluster will block. You can later resume the cluster's work by
using [`ALTER CLUSTER`] to set a nonzero replication factor.

{{< note >}}
A common misconception is that increasing a cluster's replication
factor will increase its capacity for work. This is not the case. Increasing
the replication factor increases the **fault tolerance** of the cluster, not its
capacity for work. Replicas are exact copies of one another: each replica must
do exactly the same work (i.e., maintain the same dataflows and process the same
queries) as all the other replicas of the cluster.

To increase a cluster's capacity, you should instead increase the cluster's
[size](#size).
{{< /note >}}

### Credit usage

Each [replica](#replication-factor) of the cluster consumes credits at a rate
determined by the cluster's size:

Size      | Legacy size  | Credits per replica per hour
----------|--------------|-----------------------------
`25cc`    | `3xsmall`    | 0.25
`50cc`    | `2xsmall`    | 0.5
`100cc`   | `xsmall`     | 1
`200cc`   | `small`      | 2
`300cc`   | &nbsp;       | 3
`400cc`   | `medium`     | 4
`600cc`   | &nbsp;       | 6
`800cc`   | `large`      | 8
`1200cc`  | &nbsp;       | 12
`1600cc`  | `xlarge`     | 16
`3200cc`  | `2xlarge`    | 32
`6400cc`  | `3xlarge`    | 64
`128C`    | `4xlarge`    | 128
`256C`    | `5xlarge`    | 256
`512C`    | `6xlarge`    | 512

Credit usage is measured at a one second granularity. For a given replica,
credit usage begins when a `CREATE CLUSTER` or [`ALTER CLUSTER`] statement
provisions the replica and ends when an [`ALTER CLUSTER`] or [`DROP CLUSTER`]
statement deprovisions the replica.

A cluster with a [replication factor](#replication-factor) of zero uses no
credits.

As an example, consider the following sequence of events:

Time                | Event
--------------------|---------------------------------------------------------
2023-08-29 3:45:00  | `CREATE CLUSTER c (SIZE '400cc', REPLICATION FACTOR 2`)
2023-08-29 3:45:45  | `ALTER CLUSTER c SET (REPLICATION FACTOR 1)`
2023-08-29 3:47:15  | `DROP CLUSTER c`

Cluster `c` will have consumed 0.4 credits in total:

  * Replica `c.r1` was provisioned from 3:45:00 to 3:47:15, consuming 0.3
    credits.
  * Replica `c.r2` was provisioned from 3:45:00 to 3:45:45, consuming 0.1
    credits.

### Scheduling

{{< private-preview />}}

To support [scheduled refreshes in materialized views](../create-materialized-view/#refresh-strategies),
you can configure a cluster to automatically turn on and off using the
`SCHEDULE...ON REFRESH` syntax.

```mzsql
CREATE CLUSTER my_scheduled_cluster (
  SIZE = 'M.1-large',
  SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour')
);
```

Scheduled clusters should **only** contain materialized views configured with a
non-default [refresh strategy](../create-materialized-view/#refresh-strategies)
(and any indexes built on these views). These clusters will automatically turn
on (i.e., be provisioned with compute resources) based on the configured
refresh strategies, and **only** consume credits for the duration of the
refreshes.

It's not possible to manually turn on a cluster with `ON REFRESH` scheduling. If
you need to turn on a cluster outside its schedule, you can temporarily disable
scheduling and provision compute resources using [`ALTER CLUSTER`](../alter-cluster/#schedule):

```mzsql
ALTER CLUSTER my_scheduled_cluster SET (SCHEDULE = MANUAL, REPLICATION FACTOR = 1);
```

To re-enable scheduling:

```mzsql
ALTER CLUSTER my_scheduled_cluster
SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

#### Hydration time estimate

<p style="font-size:14px"><b>Syntax:</b> <code>HYDRATION TIME ESTIMATE</code> <i>interval</i></p>

By default, scheduled clusters will turn on at the scheduled refresh time. To
avoid [unavailability of the objects scheduled for refresh](/sql/create-materialized-view/#querying-materialized-views-with-refresh-strategies) during the refresh
operation, we recommend turning the cluster on ahead of the scheduled time to
allow hydration to complete. This can be controlled using the `HYDRATION
TIME ESTIMATE` clause.

#### Scheduling strategy

To check the scheduling strategy associated with a cluster, you can query the
[`mz_internal.mz_cluster_schedules`](/sql/system-catalog/mz_internal/#mz_cluster_schedules)
system catalog table:

```mzsql
SELECT c.id AS cluster_id,
       c.name AS cluster_name,
       cs.type AS schedule_type,
       cs.refresh_hydration_time_estimate
FROM mz_internal.mz_cluster_schedules cs
JOIN mz_clusters c ON cs.cluster_id = c.id
WHERE c.name = 'my_refresh_cluster';
```

To check if a scheduled cluster is turned on, you can query the
[`mz_catalog.mz_cluster_replicas`](/sql/system-catalog/mz_catalog/#mz_cluster_replicas)
system catalog table:

```mzsql
SELECT cs.cluster_id,
       -- A cluster with scheduling is "on" when it has compute resources
       -- (i.e. a replica) attached.
       CASE WHEN cr.id IS NOT NULL THEN true
       ELSE false END AS is_on
FROM mz_internal.mz_cluster_schedules cs
JOIN mz_clusters c ON cs.cluster_id = c.id AND cs.type = 'on-refresh'
LEFT JOIN mz_cluster_replicas cr ON c.id = cr.cluster_id;
```

You can also use the [audit log](../system-catalog/mz_catalog/#mz_audit_events)
to observe the commands that are automatically run when a scheduled cluster is
turned on and off for materialized view refreshes:

```mzsql
SELECT *
FROM mz_audit_events
WHERE object_type = 'cluster-replica'
ORDER BY occurred_at DESC;
```

Any commands attributed to scheduled refreshes will be marked with
`"reason":"schedule"` under the `details` column.

### Known limitations

Clusters have several known limitations:

* When a cluster using legacy cc size of `3200cc` or larger uses multiple
  replicas, those replicas are not guaranteed to be spread evenly across the
  underlying cloud provider's availability zones.

## Examples

### Basic

Create a cluster with two `M.1-large` replicas:

```mzsql
CREATE CLUSTER c1 (SIZE = 'M.1-large', REPLICATION FACTOR = 2);
```

### Empty

Create a cluster with no replicas:

```mzsql
CREATE CLUSTER c1 (SIZE 'M.1-xsmall', REPLICATION FACTOR = 0);
```

You can later add replicas to this cluster with [`ALTER CLUSTER`].

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-cluster.md"
>}}

## See also

- [`ALTER CLUSTER`]
- [`DROP CLUSTER`]

[AWS availability zone IDs]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
[`ALTER CLUSTER`]: /sql/alter-cluster/
[`DROP CLUSTER`]: /sql/drop-cluster/
[`SELECT`]: /sql/select
[`SUBSCRIBE`]: /sql/subscribe
[`mz_cluster_replica_sizes`]: /sql/system-catalog/mz_catalog#mz_cluster_replica_sizes




---

## CREATE CLUSTER REPLICA



`CREATE CLUSTER REPLICA` provisions a new replica for an [**unmanaged**
cluster](/sql/create-cluster/#unmanaged-clusters).

{{< tip >}}
When getting started with Materialize, we recommend starting with managed
clusters.
{{</ tip >}}

## Conceptual framework

A cluster consists of zero or more replicas. Each replica of a cluster is a pool
of compute resources that performs exactly the same computations on exactly the
same data.

Using multiple replicas of a cluster facilitates **fault tolerance**. Clusters
with multiple replicas can tolerate failures of the underlying hardware or
network. As long as one replica remains reachable, the cluster as a whole
remains available.

## Syntax

{{< diagram "create-cluster-replica.svg" >}}

Field | Use
------|-----
_cluster_name_ | The cluster you want to attach a replica to.
_replica_name_ | A name for this replica.

### Options

{{% replica-options %}}

## Details

### Size

The `SIZE` option for replicas is identical to the [`SIZE` option for
clusters](/sql/create-cluster/#size) option, except that the size applies only
to the new replica.

{{< tabs >}}
{{< tab "M.1 Clusters" >}}

{{< include-md file="shared-content/cluster-size-disclaimer.md" >}}

{{< yaml-table data="m1_cluster_sizing" >}}

{{< /tab >}}

{{< tab "Legacy cc Clusters" >}}

Materialize offers the following legacy cc cluster sizes:

{{< tip >}}
In most cases, you **should not** use legacy sizes. [M.1 sizes](#size)
offer better performance per credit for nearly all workloads. We recommend using
M.1 sizes for all new clusters, and recommend migrating existing
legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
customers during the transition period as we move to deprecate legacy sizes.

The legacy size information is provided for completeness.
{{< /tip >}}

* `25cc`
* `50cc`
* `100cc`
* `200cc`
* `300cc`
* `400cc`
* `600cc`
* `800cc`
* `1200cc`
* `1600cc`
* `3200cc`
* `6400cc`
* `128C`
* `256C`
* `512C`

The resource allocations are proportional to the number in the size name. For
example, a cluster of size `600cc` has 2x as much CPU, memory, and disk as a
cluster of size `300cc`, and 1.5x as much CPU, memory, and disk as a cluster of
size `400cc`. To determine the specific resource allocations for a size,
query the [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes) table.

{{< warning >}}
The values in the `mz_cluster_replica_sizes` table may change at any
time. You should not rely on them for any kind of capacity planning.
{{< /warning >}}

Clusters of larger sizes can process data faster and handle larger data volumes.
{{< /tab >}}
{{< /tabs >}}

See also:

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).


### Homogeneous vs. heterogeneous hardware provisioning

Because Materialize uses active replication, all replicas will be instructed to
do the same work, irrespective of their resource allocation.

For the most stable performance, we recommend using the same size and disk
configuration for all replicas.

However, it is possible to use different replica configurations in the same
cluster. In these cases, the replicas with less resources will likely be
continually burdened with a backlog of work. If all of the faster replicas
become unreachable, the system might experience delays in replying to requests
while the slower replicas catch up to the last known time that the faster
machines had computed.

## Example

```mzsql
CREATE CLUSTER REPLICA c1.r1 (SIZE = 'M.1-large');
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/create-cluster-replica.md" >}}

## See also

- [`DROP CLUSTER REPLICA`]

[AWS availability zone ID]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
[`DROP CLUSTER REPLICA`]: /sql/drop-cluster-replica




---

## CREATE CONNECTION


A connection describes how to connect and authenticate to an external system you
want Materialize to read from or write to. Once created, a connection
is **reusable** across multiple [`CREATE SOURCE`](/sql/create-source) and
[`CREATE SINK`](/sql/create-sink) statements.

To use credentials that contain sensitive information (like passwords and SSL
keys) in a connection, you must first [create secrets](/sql/create-secret) to
securely store each credential in Materialize's secret management system.
Credentials that are generally not sensitive (like usernames and SSL
certificates) can be specified as plain `text`, or also stored as secrets.

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

## Source and sink connections

### AWS

An Amazon Web Services (AWS) connection provides Materialize with access to an
Identity and Access Management (IAM) user or role in your AWS account. You can
use AWS connections to perform [bulk exports to Amazon S3](/serve-results/s3/),
perform [authentication with an Amazon MSK cluster](#kafka-aws-connection), or
perform [authentication with an Amazon RDS MySQL database](#mysql-aws-connection).

{{< diagram "create-connection-aws.svg" >}}

#### Connection options {#aws-options}

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------
| `ENDPOINT`                                | `text`           | *Advanced.* Override the default AWS endpoint URL. Allows targeting S3-compatible services like MinIO.
| `REGION`                                  | `text`           | *For Materialize Cloud only* The AWS region to connect to. Defaults to the current Materialize region.
| `ACCESS KEY ID`                           | secret or `text` | The access key ID to connect with. Triggers credentials-based authentication.<br><br><strong>Warning!</strong> Use of credentials-based authentication is deprecated. AWS strongly encourages the use of role assumption-based authentication instead.
| `SECRET ACCESS KEY`                       | secret           | The secret access key corresponding to the specified access key ID.<br><br>Required and only valid when `ACCESS KEY ID` is specified.
| `SESSION TOKEN`                           | secret or `text` | The session token corresponding to the specified access key ID.<br><br>Only valid when `ACCESS KEY ID` is specified.
| `ASSUME ROLE ARN`                         | `text`           | The Amazon Resource Name (ARN) of the IAM role to assume. Triggers role assumption-based authentication.
| `ASSUME ROLE SESSION NAME`                | `text`           | The session name to use when assuming the role.<br><br>Only valid when `ASSUME ROLE ARN` is specified.

#### `WITH` options {#aws-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Whether [connection validation](#connection-validation) should be performed on connection creation.<br><br>Defaults to `false`.

#### Permissions {#aws-permissions}

{{< warning >}}
Failing to constrain the external ID in your role trust policy will allow
other Materialize customers to assume your role and use AWS privileges you
have granted the role!
{{< /warning >}}

When using role assumption-based authentication, you must configure a [trust
policy] on the IAM role that permits Materialize to assume the role.

Materialize always uses the following IAM principal to assume the role:

```
arn:aws:iam::664411391173:role/MaterializeConnection
```

Materialize additionally generates an [external ID] which uniquely identifies
your AWS connection across all Materialize regions. To ensure that other
Materialize customers cannot assume your role, your IAM trust policy **must**
constrain access to only the external ID that Materialize generates for the
connection:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::664411391173:role/MaterializeConnection"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<EXTERNAL ID FOR CONNECTION>"
                }
            }
        }
    ]
}
```

You can retrieve the external ID for the connection, as well as an example trust
policy, by querying the
[`mz_internal.mz_aws_connections`](/sql/system-catalog/mz_internal/#mz_aws_connections)
table:

```mzsql
SELECT id, external_id, example_trust_policy FROM mz_internal.mz_aws_connections;
```

#### Examples {#aws-examples}

{{< tabs >}}
{{< tab "Role assumption">}}

In this example, we have created the following IAM role for Materialize to
assume:

<table>
<tr>
<th>Name</th>
<th>AWS account ID</th>
<th>Trust policy</th>
<tr>
<td><code>WarehouseExport</code></td>
<td>000000000000</td>
<td>

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::000000000000:role/MaterializeConnection"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "mz_00000000-0000-0000-0000-000000000000_u0"
                }
            }
        }
    ]
}
```

</td>
</tr>
</table>

To create an AWS connection that will assume the `WarehouseExport` role:

```mzsql
CREATE CONNECTION aws_role_assumption TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::000000000000:role/WarehouseExport',
    REGION = 'us-east-1'
);
```
{{< /tab >}}

{{< tab "Credentials">}}
{{< warning >}}

Use of credentials-based authentication is deprecated.  AWS strongly encourages
the use of role assumption-based authentication instead.

{{< /warning >}}

To create an AWS connection that uses static access key credentials:

```mzsql
CREATE SECRET aws_secret_access_key AS '...';
CREATE CONNECTION aws_credentials TO AWS (
    ACCESS KEY ID = 'ASIAV2KIV5LPTG6HGXG6',
    SECRET ACCESS KEY = SECRET aws_secret_access_key
);
```
{{< /tab >}}

{{< /tabs >}}

### S3 compatible object storage
You can use an AWS connection to perform bulk exports to any S3 compatible object storage service,
such as Google Cloud Storage. While connecting to S3 compatible object storage, you need to provide
static access key credentials, specify the endpoint, and the region.

To create a connection that uses static access key credentials:

```mzsql
CREATE SECRET secret_access_key AS '...';
CREATE CONNECTION gcs_connection TO AWS (
    ACCESS KEY ID = 'ASIAV2KIV5LPTG6HGXG6',
    SECRET ACCESS KEY = SECRET secret_access_key,
    ENDPOINT = 'https://storage.googleapis.com',
    REGION = 'us'
);
```

### Kafka

A Kafka connection establishes a link to a [Kafka] cluster. You can use Kafka
connections to create [sources](/sql/create-source/kafka) and [sinks](/sql/create-sink/kafka/).

#### Syntax {#kafka-syntax}

{{< diagram "create-connection-kafka.svg" >}}

#### Connection options {#kafka-options}

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------
| `BROKER`                                  | `text`           | The Kafka bootstrap server.<br><br>Exactly one of `BROKER`, `BROKERS`, or `AWS PRIVATELINK` must be specified.
| `BROKERS`                                 | `text[]`         | A comma-separated list of Kafka bootstrap servers.<br><br>Exactly one of `BROKER`, `BROKERS`, or `AWS PRIVATELINK` must be specified.
| `SECURITY PROTOCOL`                       | `text`           | The security protocol to use: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`.<br><br>Defaults to `SASL_SSL` if any `SASL ...` options are specified or if the `AWS CONNECTION` option is specified, otherwise defaults to `SSL`.
| `SASL MECHANISMS`                         | `text`           | The SASL mechanism to use for authentication: `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. Despite the name, this option only allows a single mechanism to be specified.<br><br>Required if the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.<br>Cannot be specified if `AWS CONNECTION` is specified.
| `SASL USERNAME`                           | secret or `text` | Your SASL username.<br><br>Required and only valid when the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.
| `SASL PASSWORD`                           | secret           | Your SASL password.<br><br>Required and only valid when the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.
| `SSL CERTIFICATE AUTHORITY`               | secret or `text` | The certificate authority (CA) certificate in PEM format. Used to validate the brokers' TLS certificates. If unspecified, uses the system's default CA certificates.<br><br>Only valid when the security protocol is `SSL` or `SASL_SSL`.
| `SSL CERTIFICATE`                         | secret or `text` | Your TLS certificate in PEM format for SSL client authentication. If unspecified, no client authentication is performed.<br><br>Only valid when the security protocol is `SSL` or `SASL_SSL`.
| `SSL KEY`                                 | secret           | Your TLS certificate's key in PEM format.<br><br>Required and only valid when `SSL CERTIFICATE` is specified.
| `SSH TUNNEL`                              | object name      | The name of an [SSH tunnel connection](#ssh-tunnel) to route network traffic through by default.
| `AWS CONNECTION` <a name="kafka-aws-connection"></a>  | object name      | The name of an [AWS connection](#aws) to use when performing IAM authentication with an Amazon MSK cluster.<br><br>Only valid if the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.
| `AWS PRIVATELINK`                         | object name      | The name of an [AWS PrivateLink connection](#aws-privatelink) to route network traffic through. <br><br>Exactly one of `BROKER`, `BROKERS`, or `AWS PRIVATELINK` must be specified.
| `PROGRESS TOPIC`                          | `text`           | The name of a topic that Kafka sinks can use to track internal consistency metadata. Default: `_materialize-progress-{REGION ID}-{CONNECTION ID}`.
| `PROGRESS TOPIC REPLICATION FACTOR`       | `int`            | The partition count to use when creating the progress topic (if the Kafka topic does not already exist).<br>Default: Broker's default.

#### `WITH` options {#kafka-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Whether [connection validation](#connection-validation) should be performed on connection creation.<br><br>Defaults to `true`.

To connect to a Kafka cluster with multiple bootstrap servers, use the `BROKERS`
option:

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS ('broker1:9092', 'broker2:9092')
);
```

#### Security protocol examples {#kafka-auth}

{{< tabs >}}
{{< tab "PLAINTEXT">}}
{{< warning >}}
It is insecure to use the `PLAINTEXT` security protocol unless
you are using a [network security connection](#network-security-connections)
to tunnel into a private network, as shown below.
{{< /warning >}}
```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.prd.cloud.redpanda.com:9092',
    SECURITY PROTOCOL = 'PLAINTEXT',
    SSH TUNNEL ssh_connection
);
```
{{< /tab >}}

{{< tab "SSL">}}
With both TLS encryption and TLS client authentication:
```mzsql
CREATE SECRET kafka_ssl_cert AS '-----BEGIN CERTIFICATE----- ...';
CREATE SECRET kafka_ssl_key AS '-----BEGIN PRIVATE KEY----- ...';
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'rp-f00000bar.cloud.redpanda.com:30365',
    SECURITY PROTOCOL = 'SSL'
    SSL CERTIFICATE = SECRET kafka_ssl_cert,
    SSL KEY = SECRET kafka_ssl_key,
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
);
```

With only TLS encryption:
{{< warning >}}
It is insecure to use TLS encryption with no authentication unless
you are using a [network security connection](#network-security-connections)
to tunnel into a private network as shown below.
{{< /warning >}}
```mzsql
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER = 'rp-f00000bar.cloud.redpanda.com:30365',
    SECURITY PROTOCOL = 'SSL',
    SSH TUNNEL ssh_connection,
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
);
```
{{< /tab >}}

{{< tab "SASL_PLAINTEXT">}}
{{< warning >}}
It is insecure to use the `SASL_PLAINTEXT` security protocol unless
you are using a [network security connection](#network-security-connections)
to tunnel into a private network, as shown below.
{{< /warning >}}

```mzsql
CREATE SECRET kafka_password AS '...';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092',
    SECURITY PROTOCOL = 'SASL_PLAINTEXT',
    SASL MECHANISMS = 'SCRAM-SHA-256', -- or `PLAIN` or `SCRAM-SHA-512`
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password,
    SSH TUNNEL ssh_connection
);
```
{{< /tab >}}

{{< tab "SASL_SSL">}}
```mzsql
CREATE SECRET kafka_password AS '...';
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092',
    SECURITY PROTOCOL = 'SASL_SSL',
    SASL MECHANISMS = 'SCRAM-SHA-256', -- or `PLAIN` or `SCRAM-SHA-512`
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password,
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
);
```
{{< /tab >}}

{{< tab "AWS IAM">}}

```mzsql
CREATE CONNECTION aws_msk TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::000000000000:role/MaterializeMSK',
    REGION = 'us-east-1'
);

CREATE CONNECTION kafka_msk TO KAFKA (
    BROKER 'msk.mycorp.com:9092',
    SECURITY PROTOCOL = 'SASL_SSL',
    AWS CONNECTION = aws_msk
);
```
{{< /tab >}}
{{< /tabs >}}

#### Network security {#kafka-network-security}

If your Kafka broker is not exposed to the public internet, you can tunnel the
connection through an AWS PrivateLink service (Materialize Cloud) or an
SSH bastion host.

{{< tabs >}}
{{< tab "AWS PrivateLink (Materialize Cloud)">}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

Depending on the hosted service you are connecting to, you might need to specify
a PrivateLink connection [per advertised broker](#kafka-privatelink-syntax)
(e.g. Amazon MSK), or a single [default PrivateLink connection](#kafka-privatelink-default) (e.g. Redpanda Cloud).

##### Broker connection syntax {#kafka-privatelink-syntax}

{{< warning >}}
If your Kafka cluster advertises brokers that are not specified
in the `BROKERS` clause, Materialize will attempt to connect to
those brokers without any tunneling.
{{< /warning >}}

{{< diagram "create-connection-kafka-brokers.svg" >}}

##### `kafka_broker`

{{< diagram "create-connection-kafka-broker-aws-privatelink.svg" >}}

##### `broker_option`

{{< diagram "broker-option.svg" >}}

The `USING` clause specifies that Materialize Cloud should connect to the
designated broker via an AWS PrivateLink service. Brokers do not need to be
configured the same way, but the clause must be individually attached to each
broker that you want to connect to via the tunnel.

##### Broker connection options {#kafka-privatelink-options}

Field                                   | Value            | Required | Description
----------------------------------------|------------------|:--------:|-------------------------------
`AWS PRIVATELINK`                       | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic for this broker should be routed.
`AVAILABILITY ZONE`                     | `text`           |          | The ID of the availability zone of the AWS PrivateLink service in which the broker is accessible. If unspecified, traffic will be routed to each availability zone declared in the [AWS PrivateLink connection](#aws-privatelink) in sequence until the correct availability zone for the broker is discovered. If specified, Materialize will always route connections via the specified availability zone.
`PORT`                                  | `integer`        |          | The port of the AWS PrivateLink service to connect to. Defaults to the broker's port.

##### Example {#kafka-privatelink-example}

Suppose you have the following infrastructure:

  * A Kafka cluster consisting of two brokers named `broker1` and `broker2`,
    both listening on port 9092.

  * A Network Load Balancer that forwards port 9092 to `broker1:9092` and port
    9093 to `broker2:9092`.

  * A PrivateLink endpoint service attached to the load balancer.

You can create a connection to this Kafka broker in Materialize like so:

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS (
        'broker1:9092' USING AWS PRIVATELINK privatelink_svc,
        'broker2:9092' USING AWS PRIVATELINK privatelink_svc (PORT 9093)
    )
);
```

##### Default connections {#kafka-privatelink-default}

[Redpanda Cloud](/ingest-data/redpanda/redpanda-cloud/)) does not require
listing every broker individually. In this case, you should specify a
PrivateLink connection and the port of the bootstrap server instead.

##### Default connection syntax {#kafka-privatelink-default-syntax}

{{< diagram "create-connection-kafka-default-aws-privatelink.svg" >}}

##### Default connection options {#kafka-privatelink-default-options}

Field                                   | Value            | Required | Description
----------------------------------------|------------------|:--------:|-------------------------------
`AWS PRIVATELINK`                       | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic for this broker should be routed.
`PORT`                                  | `integer`        |          | The port of the AWS PrivateLink service to connect to. Defaults to the broker's port.

##### Example {#kafka-privatelink-default-example}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1')
);

CREATE CONNECTION kafka_connection TO KAFKA (
    AWS PRIVATELINK (PORT 30292)
    SECURITY PROTOCOL = 'SASL_PLAINTEXT',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET red_panda_password
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}

##### Syntax {#kafka-ssh-syntax}

{{< warning >}}
If you do not specify a default [`SSH TUNNEL`](#kafka-options) and your Kafka
cluster advertises brokers that are not listed in the `BROKERS` clause,
Materialize will attempt to connect to those brokers without any tunneling.
{{< /warning >}}

{{< diagram "create-connection-kafka-brokers.svg" >}}

##### `kafka_broker`

{{< diagram "create-connection-kafka-broker-ssh-tunnel.svg" >}}

The `USING` clause specifies that Materialize should connect to the designated
broker via an SSH bastion server. Brokers do not need to be configured the same
way, but the clause must be individually attached to each broker that you want
to connect to via the tunnel.

##### Connection options {#kafka-ssh-options}

Field           | Value            | Required | Description
----------------|------------------|:--------:|-------------------------------
`SSH TUNNEL`    | object name      | ✓        | The name of an [SSH tunnel connection](#ssh-tunnel) through which network traffic for this broker should be routed.


##### Example {#kafka-ssh-example}

Using a default SSH tunnel:

```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'broker1:9092',
    SSH TUNNEL ssh_connection
);
```

Using different SSH tunnels for each broker, with a default for brokers that are
not listed:

```mzsql
CREATE CONNECTION ssh1 TO SSH TUNNEL (HOST 'ssh1', ...);
CREATE CONNECTION ssh2 TO SSH TUNNEL (HOST 'ssh2', ...);

CREATE CONNECTION kafka_connection TO KAFKA (
BROKERS (
    'broker1:9092' USING SSH TUNNEL ssh1,
    'broker2:9092' USING SSH TUNNEL ssh2
    )
    SSH TUNNEL ssh_1
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

### Confluent Schema Registry

A Confluent Schema Registry connection establishes a link to a [Confluent Schema
Registry] server. You can use Confluent Schema Registry connections in the
[`FORMAT`] clause of [`CREATE SOURCE`] and [`CREATE SINK`] statements.

#### Syntax {#csr-syntax}

{{< diagram "create-connection-csr.svg" >}}

#### Connection options {#csr-options}

| <div style="min-width:220px">Field</div>    | Value            | Description
| --------------------------------------------|------------------|------------
| `URL`                                       | `text`           | The schema registry URL.<br><br>Required.
| `USERNAME`                                  | secret or `text` | The username to use for basic HTTP authentication.
| `PASSWORD`                                  | secret           | The password to use for basic HTTP authentication.<br><br>Required and only valid if `USERNAME` is specified.
| `SSL CERTIFICATE`                           | secret or `text` | Your TLS certificate in PEM format for TLS client authentication. If unspecified, no TLS client authentication is performed.<br><br>Only respected if the URL uses the `https` protocol.
| `SSL KEY`                                   | secret           | Your TLS certificate's key in PEM format.<br><br>Required and only valid if `SSL CERTIFICATE` is specified.
| `SSL CERTIFICATE AUTHORITY`                 | secret or `text` | The certificate authority (CA) certificate in PEM format. Used to validate the server's TLS certificate. If unspecified, uses the system's default CA certificates.<br><br>Only respected if the URL uses the `https` protocol.

#### `WITH` options {#csr-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Default: `true`. Whether [connection validation](#connection-validation) should be performed on connection creation.

#### Examples {#csr-example}

Using username and password authentication with TLS encryption:

```mzsql
CREATE SECRET csr_password AS '...';
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION csr_basic TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://rp-f00000bar.cloud.redpanda.com:30993',
    USERNAME = 'foo',
    PASSWORD = SECRET csr_password
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
);
```

Using TLS for encryption and authentication:

```mzsql
CREATE SECRET csr_ssl_cert AS '-----BEGIN CERTIFICATE----- ...';
CREATE SECRET csr_ssl_key AS '-----BEGIN PRIVATE KEY----- ...';
CREATE SECRET ca_cert AS '-----BEGIN CERTIFICATE----- ...';

CREATE CONNECTION csr_ssl TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://rp-f00000bar.cloud.redpanda.com:30993',
    SSL CERTIFICATE = SECRET csr_ssl_cert,
    SSL KEY = SECRET csr_ssl_key,
    -- Specifying a certificate authority is only required if your cluster's
    -- certificates are not issued by a CA trusted by the Mozilla root store.
    SSL CERTIFICATE AUTHORITY = SECRET ca_cert
);
```

#### Network security {#csr-network-security}

If your Confluent Schema Registry server is not exposed to the public internet,
you can tunnel the connection through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion host.

{{< tabs >}}
{{< tab "AWS PrivateLink (Materialize Cloud)">}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

##### Connection options {#csr-privatelink-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`AWS PRIVATELINK`           | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic should be routed.

##### Example {#csr-privatelink-example}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION csr_privatelink TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    AWS PRIVATELINK privatelink_svc
);
```

{{< /tab >}}
{{< tab "SSH tunnel">}}

##### Connection options {#csr-ssh-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`SSH TUNNEL`                | object name      | ✓        | The name of an [SSH tunnel connection](#ssh-tunnel) through which network traffic should be routed.

##### Example {#csr-ssh-example}

```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);

CREATE CONNECTION csr_ssh TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    SSH TUNNEL ssh_connection
);
```

{{< /tab >}}
{{< /tabs >}}

### MySQL

A MySQL connection establishes a link to a [MySQL] server. You can use
MySQL connections to create [sources](/sql/create-source/mysql).

#### Syntax {#mysql-syntax}

{{< diagram "create-connection-mysql.svg" >}}

#### Connection options {#mysql-options}

Field                                                | Value            | Required | Description
-----------------------------------------------------|------------------|:--------:|-----------------------------
`HOST`                                               | `text`           | ✓        | Database hostname.
`PORT`                                               | `integer`        |          | Default: `3306`. Port number to connect to at the server host.
`USER`                                               | `text`           | ✓        | Database username.
`PASSWORD`                                           | secret           |          | Password for the connection.
`SSL CERTIFICATE AUTHORITY`                          | secret or `text` |          | The certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`AWS CONNECTION` <a name="mysql-aws-connection"></a> | object name      |          | The name of an [AWS connection](#aws) to use when performing IAM authentication with an Amazon RDS MySQL cluster.<br><br>Only valid if `SSL MODE` is set to `required`, `verify_ca`, or `verify_identity`. <br><br>Incompatible with `PASSWORD` being set.
`SSL MODE`                                           | `text`           |          | Default: `disabled`. Enables SSL connections if set to `required`, `verify_ca`, or `verify_identity`. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/using-encrypted-connections.html) for more details.
`SSL CERTIFICATE`                                    | secret or `text` |          | Client SSL certificate in PEM format.
`SSL KEY`                                            | secret           |          | Client SSL key in PEM format.

#### `WITH` options {#mysql-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Default: `true`. Whether [connection validation](#connection-validation) should be performed on connection creation.

#### Example {#mysql-example}

```mzsql
CREATE SECRET mysqlpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'root',
    PASSWORD SECRET mysqlpass
);
```

#### Network security {#mysql-network-security}

If your MySQL server is not exposed to the public internet, you can tunnel the
connection through an AWS PrivateLink service (Materialize Cloud) or an
SSH bastion host.

{{< tabs >}}
{{< tab "AWS PrivateLink (Materialize Cloud)">}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

##### Connection options {#mysql-privatelink-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`AWS PRIVATELINK`           | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic should be routed.

##### Example {#mysql-privatelink-example}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
   SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
   AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'root',
    PASSWORD SECRET mysqlpass,
    AWS PRIVATELINK privatelink_svc
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}

##### Connection options {#mysql-ssh-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`SSH TUNNEL`                | object name      | ✓        | The name of an [SSH tunnel connection](#ssh-tunnel) through which network traffic should be routed.

##### Example {#mysql-ssh-example}

```mzsql
CREATE CONNECTION tunnel TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize'
);

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    SSH TUNNEL ssh_connection
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}

{{< tab "AWS IAM">}}

##### Example {#mysql-aws-connection-example}

```mzsql
CREATE CONNECTION aws_rds_mysql TO AWS (
    ASSUME ROLE ARN = 'arn:aws:iam::000000000000:role/MaterializeRDS',
    REGION = 'us-west-1'
);

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'root',
    AWS CONNECTION aws_rds_mysql,
    SSL MODE 'verify_identity'
);
```
{{< /tab >}}
{{< /tabs >}}

### PostgreSQL

A Postgres connection establishes a link to a single database of a
[PostgreSQL] server. You can use Postgres connections to create [sources](/sql/create-source/postgres).

#### Syntax {#postgres-syntax}

{{< diagram "create-connection-postgres.svg" >}}

#### Connection options {#postgres-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`HOST`                      | `text`           | ✓        | Database hostname.
`PORT`                      | `integer`        |          | Default: `5432`. Port number to connect to at the server host.
`DATABASE`                  | `text`           | ✓        | Target database.
`USER`                      | `text`           | ✓        | Database username.
`PASSWORD`                  | secret           |          | Password for the connection.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | The certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.
`SSL MODE`                  | `text`           |          | Default: `disable`. Enables SSL connections if set to `require`, `verify_ca`, or `verify_full`.
`SSL CERTIFICATE`           | secret or `text` |          | Client SSL certificate in PEM format.
`SSL KEY`                   | secret           |          | Client SSL key in PEM format.

#### `WITH` options {#postgres-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Default: `true`. Whether [connection validation](#connection-validation) should be performed on connection creation.

#### Example {#postgres-example}

```mzsql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    SSL MODE 'require',
    DATABASE 'postgres'
);
```

#### Network security {#postgres-network-security}

If your PostgreSQL server is not exposed to the public internet, you can tunnel
the connection through an AWS PrivateLink service (Materialize Cloud)or an SSH bastion host.

{{< tabs >}}
{{< tab "AWS PrivateLink">}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

##### Connection options {#postgres-privatelink-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`AWS PRIVATELINK`           | object name      | ✓        | The name of an [AWS PrivateLink connection](#aws-privatelink) through which network traffic should be routed.

##### Example {#postgres-privatelink-example}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
   SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
   AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    AWS PRIVATELINK privatelink_svc
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}

##### Connection options {#postgres-ssh-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`SSH TUNNEL`                | object name      | ✓        | The name of an [SSH tunnel connection](#ssh-tunnel) through which network traffic should be routed.

##### Example {#postgres-ssh-example}

```mzsql
CREATE CONNECTION tunnel TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize'
);

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    SSH TUNNEL tunnel,
    DATABASE 'postgres'
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

### SQL Server

{{< private-preview />}}

A SQL Server connection establishes a link to a single database of a
[SQL Server] instance. You can use SQL Server connections to create [sources](/sql/create-source/sql-server).

#### Syntax {#sql-server-syntax}

{{< diagram "create-connection-sql-server.svg" >}}

#### Connection options {#sql-server-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|-----------------------------
`HOST`                      | `text`           | ✓        | Database hostname.
`PORT`                      | `integer`        |          | Default: `1433`. Port number to connect to at the server host.
`DATABASE`                  | `text`           | ✓        | Target database.
`USER`                      | `text`           | ✓        | Database username.
`PASSWORD`                  | secret           | ✓        | Password for the connection.
`SSL MODE`                  | `text`           |          | Default: `disabled`. Enables SSL connections if set to `required`, `verify_ca`, or `verify`. See the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-sql-server-encryption) for more details.
`SSL CERTIFICATE AUTHORITY` | secret or `text` |          | One or more client SSL certificates in PEM format.

##### SSL MODE
`disabled` - no encryption.
`required` - encryption required, no certificate validation.
`verify` - encryption required, validate server certificate using OS configured CA.
`verify_ca` - encryption required, validate server certificate using provided CA certificates (requires `SSL CERTIFICATE AUTHORITY`).

#### `WITH` options {#sql-server-with-options}

Field         | Value     | Description
--------------|-----------|-------------------------------------
`VALIDATE`    | `boolean` | Default: `true`. Whether [connection validation](#connection-validation) should be performed on connection creation.

#### Example {#sql-server-example}

```mzsql
CREATE SECRET sqlserver_pass AS '<SQL_SERVER_PASSWORD>';

CREATE CONNECTION sqlserver_connection TO SQL SERVER (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 1433,
    USER 'SA',
    PASSWORD SECRET sqlserver_pass,
    DATABASE 'my_db'
);
```

## Network security connections



### AWS PrivateLink (Materialize Cloud) {#aws-privatelink}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

An AWS PrivateLink connection establishes a link to an [AWS PrivateLink] service.
You can use AWS PrivateLink connections in [Confluent Schema Registry connections](#confluent-schema-registry),
[Kafka connections](#kafka), and [Postgres connections](#postgresql).

#### Syntax {#aws-privatelink-syntax}

{{< diagram "create-connection-aws-privatelink.svg" >}}

#### Connection options {#aws-privatelink-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:| ------------
`SERVICE NAME`              | `text`           | ✓        | The name of the AWS PrivateLink service.
`AVAILABILITY ZONES`        | `text[]`         | ✓        | The IDs of the AWS availability zones in which the service is accessible.

#### Permissions {#aws-privatelink-permissions}

Materialize assigns a unique principal to each AWS PrivateLink connection in
your region using an Amazon Resource Name of the
following form:

```
arn:aws:iam::664411391173:role/mz_<REGION-ID>_<CONNECTION-ID>
```

After creating the connection, you must configure the AWS PrivateLink service
to accept connections from the AWS principal Materialize will connect as. The
principals for AWS PrivateLink connections in your region are stored in
the [`mz_aws_privatelink_connections`](/sql/system-catalog/mz_catalog/#mz_aws_privatelink_connections)
system table.

```mzsql
SELECT * FROM mz_aws_privatelink_connections;
```
```
   id   |                                 principal
--------+---------------------------------------------------------------------------
 u1     | arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
 u7     | arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u7
```

For more details on configuring a trusted principal for your AWS PrivateLink service,
see the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#add-remove-permissions).

{{< warning >}}
Do **not** grant access to the root principal for the Materialize AWS account.
Doing so will allow any Materialize customer to create a connection to your
AWS PrivateLink service.
{{< /warning >}}

#### Accepting connection requests {#aws-privatelink-requests}

If your AWS PrivateLink service is configured to require acceptance of
connection requests, you must additionally approve the connection request from
Materialize after creating the connection. For more details on manually
accepting connection requests, see the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

#### Example {#aws-privatelink-example}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

### SSH tunnel

An SSH tunnel connection establishes a link to an SSH bastion server. You can
use SSH tunnel connections in [Kafka connections](#kafka), [MySQL connections](#mysql),
and [Postgres connections](#postgresql).

#### Syntax {#ssh-tunnel-syntax}

{{< diagram "create-connection-ssh-tunnel.svg" >}}

#### Connection options {#ssh-tunnel-options}

Field                       | Value            | Required | Description
----------------------------|------------------|:--------:|------------------------------
`HOST`                      | `text`           | ✓        | The hostname of the SSH bastion server.
`PORT`                      | `integer`        | ✓        | The port to connect to.
`USER`                      | `text`           | ✓        | The name of the user to connect as.

#### Key pairs {#ssh-tunnel-keypairs}

Materialize automatically manages the key pairs for an SSH tunnel connection.
Each connection is associated with two key pairs. The private key for each key
pair is stored securely within your region and cannot be retrieved. The public
key for each key pair is stored in the [`mz_ssh_tunnel_connections`] system
table.

When Materialize connects to the SSH bastion server, it presents both keys for
authentication. To allow key pair rotation without downtime, you should
configure your SSH bastion server to accept both key pairs. You can
then **rotate the key pairs** using [`ALTER CONNECTION`].

Materialize currently generates SSH key pairs using the [Ed25519 algorithm],
which is fast, secure, and [recommended by security
professionals][latacora-crypto]. Some legacy SSH servers do not support the
Ed25519 algorithm. You will not be able to use these servers with Materialize's
SSH tunnel connections.

We routinely evaluate the security of the cryptographic algorithms in use in
Materialize. Future versions of Materialize may use a different SSH key
generation algorithm as security best practices evolve.

#### Examples {#ssh-tunnel-example}

Create an SSH tunnel connection:

```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize'
);
```

Retrieve the public keys for the SSH tunnel connection you just created:

```mzsql
SELECT
    mz_connections.name,
    mz_ssh_tunnel_connections.*
FROM
    mz_connections
JOIN
    mz_ssh_tunnel_connections USING(id)
WHERE
    mz_connections.name = 'ssh_connection';
```
```
 id    | public_key_1                          | public_key_2
-------+---------------------------------------+---------------------------------------
 ...   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize
```

## Connection validation {#connection-validation}

Materialize automatically validates the connection and authentication parameters
for most connection types on connection creation:

Connection type             | Validated by default |
----------------------------|----------------------|
AWS                         |                      |
Kafka                       | ✓                    |
Confluent Schema Registry   | ✓                    |
MySQL                       | ✓                    |
PostgreSQL                  | ✓                    |
SSH Tunnel                  |                      |
AWS PrivateLink             |                      |

For connection types that are validated by default, if the validation step
fails, the creation of the connection will also fail and a validation error is
returned. You can disable connection validation by setting the `VALIDATE`
option to `false`. This is useful, for example, when the parameters are known
to be correct but the external system is unavailable at the time of creation.

Connection types that require additional setup steps after creation, like AWS
and SSH tunnel connections, can be **manually validated** using the [`VALIDATE
CONNECTION`](/sql/validate-connection) syntax once all setup steps are
completed.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-connection.md"
>}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE SINK`](/sql/create-sink)

[AWS PrivateLink]: https://aws.amazon.com/privatelink/
[Confluent Schema Registry]: https://docs.confluent.io/platform/current/schema-registry/index.html#sr-overview
[Kafka]: https://kafka.apache.org
[MySQL]: https://www.mysql.com/
[PostgreSQL]: https://www.postgresql.org
[SQL Server]: https://www.microsoft.com/en-us/sql-server
[`ALTER CONNECTION`]: /sql/alter-connection
[`CREATE SOURCE`]: /sql/create-source
[`CREATE SINK`]: /sql/create-sink
[`FORMAT`]: /sql/create-source/#formats
[`mz_aws_privatelink_connections`]: /sql/system-catalog/mz_catalog/#mz_aws_privatelink_connections
[`mz_connections`]: /sql/system-catalog/mz_catalog/#mz_connections
[`mz_ssh_tunnel_connections`]: /sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections
[Ed25519 algorithm]: https://ed25519.cr.yp.to
[latacora-crypto]: https://latacora.micro.blog/2018/04/03/cryptographic-right-answers.html
[trust policy]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_terms-and-concepts.html#term_trust-policy
[external ID]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html




---

## CREATE DATABASE


`CREATE DATABASE` creates a new database.

## Conceptual framework

Materialize mimics SQL standard's namespace hierarchy, which is:

- Databases (highest level)
- Schemas
- Tables, views, sources
- Columns (lowest level)

Each layer in the hierarchy can contain elements directly beneath it. In this
instance, databases can contain schemas.

For more information, see [Namespaces](../namespaces).

## Syntax

{{< diagram "create-database.svg" >}}

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a database of the same name already exists. <br/><br/>If _not_ specified, throw an error if a database of the same name already exists. _(Default)_
_database&lowbar;name_ | A name for the database.

## Details

For details about databases, see [Namespaces: Database
details](../namespaces/#database-details).

## Examples

```mzsql
CREATE DATABASE IF NOT EXISTS my_db;
```
```mzsql
SHOW DATABASES;
```
```nofmt
materialize
my_db
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-database.md"
>}}

## Related pages

- [`DROP DATABASE`](../drop-database)
- [`SHOW DATABASES`](../show-databases)




---

## CREATE INDEX


`CREATE INDEX` creates an in-memory [index](/concepts/indexes/) on a source, view, or materialized view.

In Materialize, indexes store query results in memory within a specific [cluster](/concepts/clusters/), and keep these results **incrementally updated** as new data arrives. This ensures that indexed data remains [fresh](/concepts/reaction-time), reflecting the latest changes with minimal latency.

The primary use case for indexes is to accelerate direct queries issued via [`SELECT`](/sql/select/) statements.
By maintaining fresh, up-to-date results in memory, indexes can significantly [optimize query performance](/transform-data/optimization/), reducing both response time and compute load—especially for resource-intensive operations such as joins, aggregations, and repeated subqueries.

Because indexes are scoped to a single cluster, they are most useful for accelerating queries within that cluster. For results that must be shared across clusters or persisted to durable storage, consider using a [materialized view](/sql/create-materialized-view), which also maintains fresh results but is accessible system-wide.

### Usage patterns

#### Indexes on views vs. materialized views

{{% views-indexes/table-usage-pattern-intro %}}
{{% views-indexes/table-usage-pattern %}}

#### Indexes and query optimizations

You might want to create indexes when...

-   You want to use non-primary keys (e.g. foreign keys) as a join condition. In
    this case, you could create an index on the columns in the join condition.
-   You want to speed up searches filtering by literal values or expressions.

{{% views-indexes/index-query-optimization-specific-instances %}}

#### Best practices

{{% views-indexes/index-best-practices %}}

## Syntax

{{< diagram "create-index.svg" >}}

### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

Field | Use
------|-----
**DEFAULT** | Creates a default index using a set of columns that uniquely identify each row. If this set of columns can't be inferred, all columns are used.
_index&lowbar;name_ | A name for the index.
_obj&lowbar;name_ | The name of the source, view, or materialized view on which you want to create an index.
_cluster_name_ | The [cluster](/sql/create-cluster) to maintain this index. If not specified, defaults to the active cluster.
_method_ | The name of the index method to use. The only supported method is [`arrangement`](/overview/arrangements).
_col&lowbar;expr_**...** | The expressions to use as the key for the index.
_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* <br>Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). **Note:** Configuring indexes to retain history is not recommended. As an alternative, consider creating a materialized view for your subscription query and configuring the history retention period on the view instead. See [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). <br>Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). <br>Default: `1s`.

## Details

### Restrictions

-   You can only reference the columns available in the `SELECT` list of the query
    that defines the view. For example, if your view was defined as `SELECT a, b FROM src`, you can only reference columns `a` and `b`, even if `src` contains
    additional columns.

-   You cannot exclude any columns from being in the index's "value" set. For
    example, if your view is defined as `SELECT a, b FROM ...`, all indexes will
    contain `{a, b}` as their values.

    If you want to create an index that only stores a subset of these columns,
    consider creating another materialized view that uses `SELECT some_subset FROM this_view...`.

### Structure

Indexes in Materialize have the following structure for each unique row:

```nofmt
((tuple of indexed expressions), (tuple of the row, i.e. stored columns))
```

#### Indexed expressions vs. stored columns

Automatically created indexes will use all columns as key expressions for the
index, unless Materialize is provided or can infer a unique key for the source
or view.

For instance, unique keys can be...

-   **Provided** by the schema provided for the source, e.g. through the Confluent
    Schema Registry.
-   **Inferred** when the query...
    -   Concludes with a `GROUP BY`.
    -   Uses sources or views that have a unique key without damaging this property.
        For example, joining a view with unique keys against a second, where the join
        constraint uses foreign keys.

When creating your own indexes, you can choose the indexed expressions.

### Memory footprint

The in-memory sizes of indexes are proportional to the current size of the source
or view they represent. The actual amount of memory required depends on several
details related to the rate of compaction and the representation of the types of
data in the source or view.

Creating an index may also force the first materialization of a view, which may
cause Materialize to install a dataflow to determine and maintain the results of
the view. This dataflow may have a memory footprint itself, in addition to that
of the index.

## Examples

### Optimizing joins with indexes

You can optimize the performance of `JOIN` on two relations by ensuring their
join keys are the key columns in an index.

```mzsql
CREATE MATERIALIZED VIEW active_customers AS
    SELECT guid, geo_id, last_active_on
    FROM customer_source
    WHERE last_active_on > now() - INTERVAL '30' DAYS;

CREATE INDEX active_customers_geo_idx ON active_customers (geo_id);

CREATE MATERIALIZED VIEW active_customer_per_geo AS
    SELECT geo.name, count(*)
    FROM geo_regions AS geo
    JOIN active_customers ON active_customers.geo_id = geo.id
    GROUP BY geo.name;
```

In the above example, the index `active_customers_geo_idx`...

-   Helps us because it contains a key that the view `active_customer_per_geo` can
    use to look up values for the join condition (`active_customers.geo_id`).

    Because this index is exactly what the query requires, the Materialize
    optimizer will choose to use `active_customers_geo_idx` rather than build
    and maintain a private copy of the index just for this query.

-   Obeys our restrictions by containing only a subset of columns in the result
    set.

### Speed up filtering with indexes

If you commonly filter by a certain column being equal to a literal value, you can set up an index over that column to speed up your queries:

```mzsql
CREATE MATERIALIZED VIEW active_customers AS
    SELECT guid, geo_id, last_active_on
    FROM customer_source
    GROUP BY geo_id;

CREATE INDEX active_customers_idx ON active_customers (guid);

-- This should now be very fast!
SELECT * FROM active_customers WHERE guid = 'd868a5bf-2430-461d-a665-40418b1125e7';

-- Using indexed expressions:
CREATE INDEX active_customers_exp_idx ON active_customers (upper(guid));
SELECT * FROM active_customers WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7';

-- Filter using an expression in one field and a literal in another field:
CREATE INDEX active_customers_exp_field_idx ON active_customers (upper(guid), geo_id);
SELECT * FROM active_customers WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7' and geo_id = 'ID_8482';
```

Create an index with an expression to improve query performance over a frequently used expression, and
avoid building downstream views to apply the function like the one used in the example: `upper()`.
Take into account that aggregations like `count()` cannot be used as indexed expressions.

For more details on using indexes to optimize queries, see [Optimization](../../ops/optimization/).

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-index.md" >}}

## Related pages

-   [`SHOW INDEXES`](../show-indexes)
-   [`DROP INDEX`](../drop-index)




---

## CREATE MATERIALIZED VIEW


`CREATE MATERIALIZED VIEW` defines a view that maintains [fresh results](/concepts/reaction-time) by persisting them in durable storage and incrementally updating them as new data arrives.

Materialized views are particularly useful when you need **cross-cluster access** to results or want to sink data to external systems like [Kafka](/sql/create-sink). When you create a materialized view, you specify a [cluster](/concepts/clusters/) responsible for maintaining it, but the results can be **queried from any cluster**. This allows you to separate the compute resources used for view maintenance from those used for serving queries.

If you do not need durability or cross-cluster sharing, and you are primarily interested in fast query performance within a single cluster, you may prefer to [create a view and index it](/concepts/views/#views). In Materialize, [indexes on views](/concepts/indexes/) also maintain results incrementally, but store them in memory, scoped to the cluster where the index was created. This approach offers lower latency for direct querying within that cluster.

## Syntax

{{< diagram "create-materialized-view.svg" >}}

Field | Use
------|-----
**OR REPLACE** | If a materialized view exists with the same name, replace it with the view defined in this statement. You cannot replace views that other views or sinks depend on, nor can you replace a non-view object with a view.
**IF NOT EXISTS** | If specified, _do not_ generate an error if a materialized view of the same name already exists. <br/><br/>If _not_ specified, throw an error if a view of the same name already exists. _(Default)_
_view&lowbar;name_ | A name for the materialized view.
**(** _col_ident_... **)** | Rename the `SELECT` statement's columns to the list of identifiers, both of which must be the same length. Note that this is required for statements that return multiple columns with the same identifier.
_cluster&lowbar;name_ | The cluster to maintain this materialized view. If not specified, defaults to the active cluster.
_select&lowbar;stmt_ | The [`SELECT` statement](../select) whose results you want to maintain incrementally updated.

#### `with_options`

{{< diagram "with-options.svg" >}}

| Field                                     | Value               | Description                                                                                                                                                       |
|-------------------------------------------|---------------------| ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ASSERT NOT NULL** _col_ident_           | `text`              | The column identifier for which to create a [non-null assertion](#non-null-assertions). To specify multiple columns, use the option multiple times. |
| **PARTITION BY** _columns_                | `(ident [, ident]*)` | The key by which Materialize should internally partition this durable collection. See the [partitioning guide](/transform-data/patterns/partition-by/) for restrictions on valid values and other details.
| **RETAIN HISTORY FOR** _retention_period_ | `interval`          | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.
| **REFRESH** _refresh_strategy_             |                     | ***Private preview.** This option has known performance or stability issues and is under active development.* The refresh strategy for the materialized view. See [Refresh strategies](#refresh-strategies) for syntax options. <br>Default: `ON COMMIT`. |

## Details

### Usage patterns

{{% views-indexes/table-usage-pattern-intro %}}
{{% views-indexes/table-usage-pattern %}}

### Indexes

Although you can query a materialized view directly, these queries will be
issued against Materialize's storage layer. This is expected to be fast, but
still slower than reading from memory. To improve the speed of queries on
materialized views, we recommend creating [indexes](../create-index) based on
common query patterns.

It's important to keep in mind that indexes are **local** to a cluster, and
maintained in memory. As an example, if you create a materialized view and
build an index on it in the `quickstart` cluster, querying the view from a
different cluster will _not_ use the index; you should create the appropriate
indexes in each cluster you are referencing the materialized view in.

[//]: # "TODO(morsapaes) Point to relevant operational guide on indexes once
this exists+add detail about using indexes to optimize materialized view
stacking."


### Non-null assertions

Because materialized views may be created on arbitrary queries, it may not in
all cases be possible for Materialize to automatically infer non-nullability of
some columns that can in fact never be null. In such a case, `ASSERT NOT NULL`
clauses may be used as described in the syntax section above. Specifying
`ASSERT NOT NULL` for a column forces that column's type in the materialized
view to include `NOT NULL`. If this clause is used erroneously, and a `NULL`
value is in fact produced in a column for which `ASSERT NOT NULL` was
specified, querying the materialized view will produce an error until the
offending row is deleted.

### Refresh strategies

{{< private-preview />}}

Materialized views in Materialize are incrementally maintained by default, meaning their results are automatically updated as soon as new data arrives.
This guarantees that queries returns the most up-to-date information available with minimal delay and that results are always as [fresh](/concepts/reaction-time) as the input data itself.

In most cases, this default behavior is ideal.
However, in some very specific scenarios like reporting over slow changing historical data, it may be acceptable to relax freshness in order to reduce compute usage.
For these cases, Materialize supports refresh strategies, which allow you to configure a materialized view to recompute itself on a fixed schedule rather than maintaining them incrementally.

{{< note >}}

The use of refresh strategies is discouraged unless you have a clear and measurable need to reduce maintenance costs on stale or archival data. For most use cases, the default incremental maintenance model provides a better experience.

{{< /note >}}


[//]: # "TODO(morsapaes) We should add a SQL pattern that walks through a
full-blown example of how to implement the cold, warm, hot path with refresh
strategies."

#### Refresh on commit

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH ON COMMIT</code></p>

Materialized views in Materialize are incrementally updated by default. This means that as soon as new data arrives in the system, any dependent materialized views are automatically and continuously updated. This behavior, known as **refresh on commit**, ensures that the view's contents are always as fresh as the underlying data.

**`REFRESH ON COMMIT` is:**

* **Generally available**
* The **default behavior** for all materialized views
* **Implicit** and does not need to be manually specified
* **Strongly recommended** for the vast majority of use cases

With `REFRESH ON COMMIT`, Materialize provides low-latency, up-to-date results without requiring user-defined schedules or manual refreshes. This model is ideal for most workloads, including streaming analytics, live dashboards, customer-facing queries, and applications that rely on timely, accurate results.

Only in rare cases—such as batch-oriented processing or reporting over slowly changing historical datasets—might it make sense to trade off freshness for potential cost savings. In such cases, consider defining an explicit [refresh strategy](#refresh-strategies) to control when recomputation occurs.

#### Refresh at

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH AT</code> { <code>CREATION</code> | <i>timestamp</i> }</p>

This strategy allows configuring a materialized view to **refresh at a specific
time**. The refresh time can be specified as a timestamp, or using the `AT CREATION`
clause, which triggers a first refresh when the materialized view is created.

**Example**

To create a materialized view that is refreshed at creation, and then at the
specified times:

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_at
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh at a user-specified (future) time
  REFRESH AT '2024-06-06 12:00:00',
  -- Refresh at another user-specified (future) time
  REFRESH AT '2024-06-08 22:00:00'
)
AS SELECT ... FROM ...;
```

You can specify multiple `REFRESH AT` strategies in the same `CREATE` statement,
and combine them with the [`REFRESH EVERY` strategy](#refresh-every).

#### Refresh every

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH EVERY</code> <i>interval</i> [ <code>ALIGNED TO</code> <i>timestamp</i> ]</code></p>

This strategy allows configuring a materialized view to **refresh at regular
intervals**. The `ALIGNED TO` clause additionally allows specifying the _phase_
of the scheduled refreshes: for daily refreshes, it specifies the time of the
day when the refresh will happen; for weekly refreshes, it specifies the day of
the week and the time of the day when the refresh will happen. If `ALIGNED TO`
is not specified, it defaults to the time when the materialized view is
created.

**Example**

To create a materialized view that is refreshed at creation, and then once a day
at 10PM UTC:

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_every
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at 10PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-06-06 22:00:00'
) AS
SELECT ...;
```

You can specify multiple `REFRESH EVERY` strategies in the same `CREATE`
statement, and combine them with the [`REFRESH AT` strategy](#refresh-at). When
this strategy, we recommend **always** using the [`REFRESH AT CREATION`](#refresh-at)
clause, so the materialized view is available for querying ahead of the first
user-specified refresh time.

#### Querying materialized views with refresh strategies

Materialized views configured with [`REFRESH EVERY` strategies](#refresh-every)
have a period of unavailability around the scheduled refresh times — during this
period, the view **will not return any results**. To avoid unavailability
during the refresh operation, you must host these views in
[**scheduled clusters**](/sql/create-cluster/#scheduling), which can be
configured to automatically [turn on ahead of the scheduled refresh time](/sql/create-cluster/#hydration-time-estimate).

**Example**

To create a scheduled cluster that turns on 1 hour ahead of any scheduled
refresh times:

```mzsql
CREATE CLUSTER my_scheduled_cluster (
  SIZE = '3200cc',
  SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour')
);
```

You can then create a materialized view in this cluster, configured to refresh
at creation, then once a day at 12PM UTC:

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_every
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at 12PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-06-18 00:00:00'
) AS
SELECT ...;
```

Because the materialized view is hosted on a scheduled cluster that is
configured to **turn on ahead of any scheduled refreshes**, you can expect
`my_scheduled_cluster` to be provisioned at 11PM UTC — or, 1 hour ahead of the
scheduled refresh time for `mv_refresh_every`. This means that the cluster can
backfill the view with pre-existing data — a process known as [_hydration_](/transform-data/troubleshooting/#hydrating-upstream-objects)
— ahead of the refresh operation, which **reduces the total unavailability window
of the view** to just the duration of the refresh.

If the cluster is **not** configured to turn on ahead of scheduled refreshes
(i.e., using the `HYDRATION TIME ESTIMATE` option), the total unavailability
window of the view will be a combination of the hydration time for all objects
in the cluster (typically long) and the duration of the refresh for the
materialized view (typically short).

Depending on the actual time it takes to hydrate the view or set of views in the
cluster, you can later adjust the hydration time estimate value for the
cluster using [`ALTER CLUSTER`](../alter-cluster/#schedule):

```mzsql
ALTER CLUSTER my_scheduled_cluster
SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '30 minutes'));
```

#### Introspection

To check details about the (non-default) refresh strategies associated with any materialized
view in the system, you can query
the [`mz_internal.mz_materialized_view_refresh_strategies`](../system-catalog/mz_internal/#mz_materialized_view_refresh_strategies)
and [`mz_internal.mz_materialized_view_refreshes`](../system-catalog/mz_internal/#mz_materialized_view_refreshes)
system catalog tables:

```mzsql
SELECT mv.id AS materialized_view_id,
       mv.name AS materialized_view_name,
       rs.type AS refresh_strategy,
       rs.interval AS refresh_interval,
       rs.aligned_to AS refresh_interval_phase,
       rs.at AS refresh_time,
       r.last_completed_refresh,
       r.next_refresh
FROM mz_internal.mz_materialized_view_refresh_strategies rs
JOIN mz_internal.mz_materialized_view_refreshes r ON r.materialized_view_id = rs.materialized_view_id
JOIN mz_materialized_views mv ON rs.materialized_view_id = mv.id;
```

## Examples

### Creating a materialized view

```mzsql
CREATE MATERIALIZED VIEW winning_bids AS
SELECT auction_id,
       bid_id,
       item,
       amount
FROM highest_bid_per_auction
WHERE end_time < mz_now();
```

### Using non-null assertions

```mzsql
CREATE MATERIALIZED VIEW users_and_orders WITH (
  -- The semantics of a FULL OUTER JOIN guarantee that user_id is not null,
  -- because one of `users.id` or `orders.user_id` must be not null, but
  -- Materialize cannot yet automatically infer that fact.
  ASSERT NOT NULL user_id
)
AS
SELECT
  coalesce(users.id, orders.user_id) AS user_id,
  ...
FROM users FULL OUTER JOIN orders ON users.id = orders.user_id
```

### Using refresh strategies

```mzsql
CREATE MATERIALIZED VIEW mv
IN CLUSTER my_refresh_cluster
WITH (
  -- Refresh every Tuesday at 12PM UTC
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-04 12:00:00',
  -- Refresh every Thursday at 12PM UTC
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-06 12:00:00',
  -- Refresh on creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION
)
AS SELECT ... FROM ...;
```

[//]: # "TODO(morsapaes) Add more elaborate examples with \timing that show
things like querying materialized views from different clusters, indexed vs.
non-indexed, and so on."

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/create-materialized-view.md" >}}

## Additional information

- Materialized views are not monotonic; that is, materialized views cannot be
  recognized as append-only.

## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`DROP MATERIALIZED VIEW`](../drop-materialized-view)




---

## CREATE NETWORK POLICY (Cloud)


*Available for Materialize Cloud only*

`CREATE NETWORK POLICY` creates a network policy that restricts access to a
Materialize region using IP-based rules. Network policies are part of
Materialize's framework for [access control](/security/cloud/).

## Syntax

{{< diagram "create-network-policy.svg" >}}

### `network_policy_rule`

{{< diagram "network-policy-rule.svg" >}}

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------------------------
| _name_                                    | `text`           | A name for the network policy.
| `RULES`                                   | `text[]`         | A comma-separated list of network policy rules.

#### Network policy rule options

| <div style="min-width:240px">Field</div>  | Value            | Description
|-------------------------------------------|------------------|------------------------------------------------
| _name_                                    | `text`           | A name for the network policy rule.
| `ACTION`                                  | `text`           | The action to take for this rule. `ALLOW` is the only valid option.
| `DIRECTION`                               | `text`           | The direction of traffic the rule applies to. `INGRESS` is the only valid option.
| `ADDRESS`                                 | `text`           | The Classless Inter-Domain Routing (CIDR) block the rule will be applied to.

## Details

### Pre-installed network policy

When you enable a Materialize region, a default network policy named `default`
will be pre-installed. This policy has a wide open ingress rule `allow
0.0.0.0/0`. You can modify or drop this network policy at any time.

{{< note >}}
The default value for the `network_policy` session parameter is `default`.
Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
change the default value.
{{< /note >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/create-network-policy.md" >}}

## Examples

```mzsql
CREATE NETWORK POLICY office_access_policy (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32')
  )
);
```

```mzsql
ALTER SYSTEM SET network_policy = office_access_policy;
```

## Related pages

- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`DROP NETWORK POLICY`](../drop-network-policy)
- [`GRANT ROLE`](../grant-role)




---

## CREATE ROLE


`CREATE ROLE` creates a new role, which is a user account in Materialize.[^1]

When you connect to Materialize, you must specify the name of a valid role in
the system.

[^1]: Materialize does not support the `CREATE USER` command.

## Syntax

{{< tabs >}}

{{< tab "Cloud" >}}

### Cloud
{{% include-example file="examples/rbac-cloud/create_roles" example="create-role-syntax" %}}

{{% include-example file="examples/rbac-cloud/create_roles" example="create-role-options" %}}

**Note:**
{{% include-example file="examples/rbac-cloud/create_roles" example="create-role-details" %}}
{{< /tab >}}
{{< tab "Self-Managed" >}}
### Self-Managed
{{% include-example file="examples/rbac-sm/create_roles" example="create-role-syntax" %}}

{{% include-example file="examples/rbac-sm/create_roles"
example="create-role-options" %}}

**Note:**
{{% include-example file="examples/rbac-sm/create_roles" example="create-role-details" %}}
{{< /tab >}}
{{< /tabs >}}

## Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... INHERIT INHERIT`.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-role.md" >}}

## Examples

### Create a functional role

In Materialize Cloud and Self-Managed, you can create a functional role:

```mzsql
CREATE ROLE db_reader;
```

### Create a role with login and password (Self-Managed)

```mzsql
CREATE ROLE db_reader WITH LOGIN PASSWORD 'password';
```

You can verify that the role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
```

### Create a superuser role (Self-Managed)

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
CREATE ROLE super_user WITH SUPERUSER LOGIN PASSWORD 'password';
```

You can verify that the superuser role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
 super_user
```

You can also verify that the role has superuser privileges by checking the `pg_authid` system catalog:

```mzsql
SELECT rolsuper FROM pg_authid WHERE rolname = 'super_user';
```

```nofmt
 true
```



## Related pages

- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)




---

## CREATE SCHEMA


`CREATE SCHEMA` creates a new schema.

## Conceptual framework

Materialize mimics SQL standard's namespace hierarchy, which is:

- Databases (highest level)
- Schemas
- Tables, views, sources
- Columns (lowest level)

Each layer in the hierarchy can contain elements directly beneath it. In this
instance, schemas can contain tables, views, and sources.

For more information, see [Namespaces](../namespaces).

## Syntax

{{< diagram "create-schema.svg" >}}

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a schema of the same name already exists. <br/><br/>If _not_ specified, throw an error if a schema of the same name already exists. _(Default)_
_schema&lowbar;name_ | A name for the schema. <br/><br/>You can specify the database for the schema with a preceding `database_name.schema_name`, e.g. `my_db.my_schema`, otherwise the schema is created in the current database.

## Examples

```mzsql
CREATE SCHEMA my_db.my_schema;
```
```mzsql
SHOW SCHEMAS FROM my_db;
```
```nofmt
public
my_schema
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-schema.md" >}}

## Related pages

- [`DROP DATABASE`](../drop-database)
- [`SHOW DATABASES`](../show-databases)




---

## CREATE SECRET


A secret securely stores sensitive credentials (like passwords and SSL keys) in Materialize's secret management system. Optionally, a secret can also be used to store credentials that are generally not sensitive (like usernames and SSL certificates), so that all your credentials are managed uniformly.

## Syntax

{{< diagram "create-secret.svg" >}}

Field   | Use
--------|-----
_name_  | The identifier for the secret.
_value_ | The value for the secret. The _value_ expression may not reference any relations, and must be implicitly castable to `bytea`.

## Examples

```mzsql
CREATE SECRET kafka_ca_cert AS decode('c2VjcmV0Cg==', 'base64');
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-secret.md" >}}

## Related pages

- [`CREATE CONNECTION`](../create-connection)
- [`ALTER SECRET`](../alter-secret)
- [`DROP SECRET`](../drop-secret)
- [`SHOW SECRETS`](../show-secrets)




---

## CREATE SINK


A [sink](/concepts/sinks/) describes an external system you
want Materialize to write data to, and provides details about how to encode
that data. You can define a sink over a materialized view, source, or table.

## Connectors

Materialize bundles **native connectors** that allow writing data to the
following external systems:

{{< multilinkbox >}}
{{< linkbox title="Message Brokers" >}}
- [Kafka/Redpanda](/sql/create-sink/kafka)
{{</ linkbox >}}
{{</ multilinkbox >}}

For details on the syntax, supported formats and features of each connector,
check out the dedicated `CREATE SINK` documentation pages.

## Best practices

### Sizing a sink

Some sinks require relatively few resources to handle data ingestion, while
others are high traffic and require hefty resource allocations. The cluster in
which you place a sink determines the amount of CPU and memory available to the
sink.

Sinks share the resource allocation of their cluster with all other objects in
the cluster. Colocating multiple sinks onto the same cluster can be more
resource efficient when you have many low-traffic sinks that occasionally need
some burst capacity.

## Details

A sink cannot be created directly on a catalog object. As a workaround you can
create a materialized view on a catalog object and create a sink on the
materialized view.

### Kafka transaction markers

{{< include-md file="shared-content/kafka-transaction-markers.md" >}}

[//]: # "TODO(morsapaes) Add best practices for sizing sinks."

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-sink.md" >}}

## Related pages

- [Sinks](/concepts/sinks/)
- [`SHOW SINKS`](/sql/show-sinks/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SINK`](/sql/show-create-sink/)




---

## CREATE SOURCE


A [source](/concepts/sources/) describes an external system you want Materialize to read data from, and provides details about how to decode and interpret that data. To create a source, you must specify a [connector](#connectors), a [format](#formats) and an [envelope](#envelopes).
Like other relations, sources are [namespaced](../namespaces/) by a database and schema.

[//]: # "TODO(morsapaes) Add short description about what the command gets going in the background."

## Connectors

Materialize bundles **native connectors** that allow ingesting data from the following external systems:

{{< include-md file="shared-content/multilink-box-native-connectors.md" >}}

For details on the syntax, supported formats and features of each connector, check out the dedicated `CREATE SOURCE` documentation pages.

**Sample data**

To get started with no external dependencies, you can use the [load generator source](/sql/create-source/load-generator/)
to produce sample data that is suitable for demo and performance test
scenarios.

## Formats

To read from an external data source, Materialize must be able to determine how to decode raw bytes from different formats into data structures it can understand at runtime. This is handled by specifying a `FORMAT` in the `CREATE SOURCE` statement.

### Avro

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT AVRO</code></p>

Materialize can decode Avro messages by integrating with a schema registry to retrieve a schema, and automatically determine the columns and data types to use in the source.

##### Schema versioning

The _latest_ schema is retrieved using the [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html) strategy at the time the `CREATE SOURCE` statement is issued.

##### Schema evolution

As long as the writer schema changes in a [compatible way](https://avro.apache.org/docs/++version++/specification/#schema-resolution), Materialize will continue using the original reader schema definition by mapping values from the new to the old schema version. To use the new version of the writer schema in Materialize, you need to **drop and recreate** the source.

##### Name collision

To avoid [case-sensitivity](/sql/identifiers/#case-sensitivity) conflicts with Materialize identifiers, we recommend double-quoting all field names when working with Avro-formatted sources.

##### Supported types

Materialize supports all [Avro types](https://avro.apache.org/docs/++version++/specification/), _except for_ recursive types and union types in arrays.

### JSON

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT JSON</code></p>

Materialize can decode JSON messages into a single column named `data` with type
`jsonb`. Refer to the [`jsonb` type](/sql/types/jsonb) documentation for the
supported operations on this type.

If your JSON messages have a consistent shape, we recommend creating a parsing
[view](/concepts/views) that maps the individual fields to
columns with the required data types:

```mzsql
-- extract jsonb into typed columns
CREATE VIEW my_typed_source AS
  SELECT
    (data->>'field1')::boolean AS field_1,
    (data->>'field2')::int AS field_2,
    (data->>'field3')::float AS field_3
  FROM my_jsonb_source;
```

To avoid doing this tedious task manually, you can use [this **JSON parsing widget**](/sql/types/jsonb/#parsing)!

##### Schema registry integration

Retrieving schemas from a schema registry is not supported yet for JSON-formatted sources. This means that Materialize cannot decode messages serialized using the [JSON Schema](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-serializer-and-deserializer) serialization format (`JSON_SR`).

### Protobuf

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT PROTOBUF</code></p>

Materialize can decode Protobuf messages by integrating with a schema registry or parsing an inline schema to retrieve a `.proto` schema definition. It can then automatically define the columns and data types to use in the source. Unlike Avro, Protobuf does not serialize a schema with the message, so Materialize expects:

* A `FileDescriptorSet` that encodes the Protobuf message schema. You can generate the `FileDescriptorSet` with [`protoc`](https://grpc.io/docs/protoc-installation/), for example:

  ```shell
  protoc --include_imports --descriptor_set_out=SCHEMA billing.proto
  ```

* A top-level message name and its package name, so Materialize knows which message from the `FileDescriptorSet` is the top-level message to decode, in the following format:

  ```shell
  <package name>.<top-level message>
  ```

  For example, if the `FileDescriptorSet` were from a `.proto` file in the
    `billing` package, and the top-level message was called `Batch`, the
    _message&lowbar;name_ value would be `billing.Batch`.

##### Schema versioning

The _latest_ schema is retrieved using the [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html) strategy at the time the `CREATE SOURCE` statement is issued.

##### Schema evolution

As long as the `.proto` schema definition changes in a [compatible way](https://developers.google.com/protocol-buffers/docs/overview#updating-defs), Materialize will continue using the original schema definition by mapping values from the new to the old schema version. To use the new version of the schema in Materialize, you need to **drop and recreate** the source.

##### Supported types

Materialize supports all [well-known](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf) Protobuf types from the `proto2` and `proto3` specs, _except for_ recursive `Struct` values and map types.

##### Multiple message schemas

When using a schema registry with Protobuf sources, the registered schemas must contain exactly one `Message` definition.

### Text/bytes

#### Text

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT TEXT</code></p>

Materialize can parse **new-line delimited** data as plain text. Data is assumed to be **valid unicode** (UTF-8), and discarded if it cannot be converted to UTF-8. Text-formatted sources have a single column, by default named `text`.

For details on casting, check the [`text`](/sql/types/text/) documentation.

#### Bytes

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT BYTES</code></p>

Materialize can read raw bytes without applying any formatting or decoding. Raw byte-formatted sources have a single column, by default named `data`.

For details on encodings and casting, check the [`bytea`](/sql/types/bytea/) documentation.

### CSV

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT CSV</code></p>

Materialize can parse CSV-formatted data using different methods to determine the number of columns to create and their respective names:

Method                 | Description
-----------------------|-----------------------
**HEADER**             | Materialize determines the _number of columns_ and the _name_ of each column using the header row. The header is **not** ingested as data.
**HEADER (** _name_list_ **)** | Same behavior as **HEADER**, with additional validation of the column names against the _name list_ specified. This allows decoding files that have headers but may not be populated yet, as well as overriding the source column names.
_n_ **COLUMNS**        | Materialize treats the source data as if it has _n_ columns. By default, columns are named `column1`, `column2`...`columnN`.

The data in CSV sources is read as [`text`](/sql/types/text). You can then handle the conversion to other types using explicit [casts](/sql/functions/cast/) when creating views.

##### Invalid rows

Any row that doesn't match the number of columns determined by the format is ignored, and Materialize logs an error.

## Envelopes

In addition to determining how to decode incoming records, Materialize also needs to understand how to interpret them. Whether a new record inserts, updates, or deletes existing data in Materialize depends on the `ENVELOPE` specified in the `CREATE SOURCE` statement.

### Append-only envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE NONE</code></p>

The append-only envelope treats all records as inserts. This is the **default** envelope, if no envelope is specified.

### Upsert envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE UPSERT</code></p>

The upsert envelope treats all records as having a **key** and a **value**, and supports inserts, updates and deletes within Materialize:

- If the key does not match a preexisting record, it inserts the record's key and value.

- If the key matches a preexisting record and the value is _non-null_, Materialize updates
  the existing record with the new value.

- If the key matches a preexisting record and the value is _null_, Materialize deletes the record.

### Debezium envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE DEBEZIUM</code></p>

Materialize provides a dedicated envelope to decode messages produced by [Debezium](https://debezium.io/). This envelope treats all records as [change events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events) with a diff structure that indicates whether each record should be interpreted as an insert, update or delete within Materialize:

- If the `before` field is _null_, the record represents an upstream [`create` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events) and Materialize inserts the record's key and value.

- If the `before` and `after` fields are _non-null_, the record represents an upstream [`update` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events) and Materialize updates the existing record with the new value.

- If the `after` field is _null_, the record represents an upstream [`delete` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-delete-events) and Materialize deletes the record.

Materialize expects a specific message structure that includes the row data before and after the change event, which is **not guaranteed** for every Debezium connector. For more details, check the [Debezium integration guide](/integrations/debezium/).

[//]: # "TODO(morsapaes) Once DBZ transaction support is stable, add a dedicated sub-section here and adapt the respective snippet in both CDC guides."

##### Truncation

The Debezium envelope does not support upstream [`truncate` events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-truncate-events).

##### Debezium metadata

The envelope exposes the `before` and `after` value fields from change events.

##### Duplicate handling

Debezium may produce duplicate records if the connector is interrupted. Materialize makes a best-effort attempt to detect and filter out duplicates.

## Best practices

### Sizing a source

Some sources are low traffic and require relatively few resources to handle data ingestion, while others are high traffic and require hefty resource allocations. The cluster in which you place a source determines the amount of CPU, memory, and disk available to the source.

It's a good idea to size up the cluster hosting a source when:

  * You want to **increase throughput**. Larger sources will typically ingest data
    faster, as there is more CPU available to read and decode data from the
    upstream external system.

  * You are using the [upsert envelope](#upsert-envelope) or [Debezium
    envelope](#debezium-envelope), and your source contains **many unique
    keys**. These envelopes maintain state proportional to the number of unique
    keys in the upstream external system. Larger sizes can store more unique
    keys.

Sources share the resource allocation of their cluster with all other objects in
the cluster. Colocating multiple sources onto the same cluster can be more
resource efficient when you have many low-traffic sources that occasionally need
some burst capacity.

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster if the source is created in an existing cluster.
- `CREATECLUSTER` privileges on the system if the source is not created in an existing cluster.
- `USAGE` privileges on all connections and secrets used in the source definition.
- `USAGE` privileges on the schemas that all connections and secrets in the statement are contained in.

## Related pages

- [Sources](/concepts/sources/)
- [`SHOW SOURCES`](/sql/show-sources/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SOURCE`](/sql/show-create-source/)




---

## CREATE TABLE


`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create:
- Read-write tables. With read-write tables, users can read ([`SELECT`]) and
  write to the tables ([`INSERT`], [`UPDATE`], [`DELETE`]).

-  ***Private Preview***. Read-only tables from [PostgreSQL sources (new
  syntax)](/sql/create-source/postgres-v2/). Users cannot be write ([`INSERT`],
  [`UPDATE`], [`DELETE`]) to these tables. These tables are populated by [data
  ingestion from a source](/ingest-data/postgres/). {{% include-example file="examples/create_table/example_postgres_table"
example="syntax-version-requirement" %}}


Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, views, and
subsources; and you can create views/materialized views/indexes on tables.


[//]: # "TODO(morsapaes) Bring back When to use a table? once there's more
clarity around best practices."

## Syntax

{{< tabs >}}
{{< tab "Read-write table" >}}
### Read-write table

{{% include-example file="examples/create_table/example_user_populated_table" example="syntax" %}}

{{% include-example file="examples/create_table/example_user_populated_table" example="syntax-options" %}}

{{< /tab >}}
{{< tab "PostgreSQL source table" >}}
### PostgreSQL source table

{{< private-preview />}}

{{< note >}}
{{% include-example file="examples/create_table/example_postgres_table"
example="syntax-version-requirement" %}}
{{< /note >}}

{{% include-example file="examples/create_table/example_postgres_table" example="syntax" %}}

{{% include-example file="examples/create_table/example_postgres_table" example="syntax-options" %}}
{{< /tab >}}

{{< /tabs >}}


## Read-write tables

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

### Known limitations

Tables do not currently support:

- Primary keys
- Unique constraints
- Check constraints

See also the known limitations for [`INSERT`](/sql/insert#known-limitations),
[`UPDATE`](/sql/update#known-limitations), and [`DELETE`](/sql/delete#known-limitations).

## PostgreSQL source tables

{{< private-preview />}}

{{< note >}}
{{% include-example file="examples/create_table/example_postgres_table"
example="syntax-version-requirement" %}}
{{< /note >}}

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

<a name="supported-db-source-types"></a>

### Read-only tables

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

### Source-populated tables and snapshotting

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

### Supported data types

{{% include-from-yaml data="postgres_source_details" name="postgres-supported-types" %}}

{{% include-from-yaml data="postgres_source_details" name="postgres-unsupported-types" %}}

### Handling table schema changes

{{% include-from-yaml data="postgres_source_details"
name="postgres-compatible-schema-changes" %}}

#### Incompatible schema changes

{{% include-from-yaml data="postgres_source_details"
name="postgres-incompatible-schema-changes" %}}

### Upstream table truncation restrictions

{{% include-from-yaml data="postgres_source_details"
name="postgres-truncation-restriction" %}}

### Inherited tables

{{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables-action" %}}

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-table.md" >}}

## Examples

### Create a table (User-populated)

{{% include-example file="examples/create_table/example_user_populated_table"
 example="create-table" %}}

Once a user-populated table is created, you can perform CRUD
(Create/Read/Update/Write) operations on it.

{{% include-example file="examples/create_table/example_user_populated_table"
 example="write-to-table" %}}

{{% include-example file="examples/create_table/example_user_populated_table"
 example="read-from-table" %}}

### Create a table (PostgreSQL source)

{{< private-preview />}}

{{< note >}}

{{% include-example file="examples/create_table/example_postgres_table"
example="syntax-version-requirement" %}}

The example assumes you have configured your upstream PostgreSQL 11+ (i.e.,
enabled logical replication, created the publication for the various tables and
replication user, and updated the network configuration).

For details about configuring your upstream system, see the [PostgreSQL
integration guides](/ingest-data/postgres/#supported-versions-and-services).

{{</ note >}}

{{% include-example file="examples/create_table/example_postgres_table"
 example="create-table" %}}

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}


{{% include-example file="examples/create_table/example_postgres_table"
 example="read-from-table" %}}


## Related pages

- [`INSERT`]
- [`DROP TABLE`](/sql/drop-table)

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/




---

## CREATE TYPE


`CREATE TYPE` defines a new data type.

## Conceptual framework

`CREATE TYPE` creates custom types, which let you create named versions of
anonymous types. For more information, see [SQL Data Types: Custom
types](../types/#custom-types).

### Use

Currently, custom types provide a shorthand for referring to
otherwise-annoying-to-type names.

## Syntax

{{< diagram "create-type.svg" >}}

 Field               | Use
---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------
 _type&lowbar;name_  | A name for the type.
 **MAP / LIST**      | The data type. If not specified, a row type is assumed.
 _property_ **=** _val_ | A property of the new type. This is required when specifying a `LIST` or `MAP` type. Note that type properties can only refer to data types within the catalog, i.e. they cannot refer to anonymous `list` or `map` types.

### `row` properties

Field               | Use
--------------------|----------------------------------------------------
_field_name_        | The name of a field in a row type.
_field_type_        | The data type of a field indicated by _field_name_.

### `list` properties

Field | Use
-----|-----
`ELEMENT TYPE` | Creates a custom [`list`](../types/list) whose elements are of `ELEMENT TYPE`.

### `map` properties

Field | Use
-----|-----
`KEY TYPE` | Creates a custom [`map`](../types/map) whose keys are of `KEY TYPE`. `KEY TYPE` must resolve to [`text`](../types/text).
`VALUE TYPE` | Creates a custom [`map`](../types/map) whose values are of `VALUE TYPE`.

## Details

For details about the custom types `CREATE TYPE` creates, see [SQL Data Types:
Custom types](../types/#custom-types).

### Properties

All custom type properties' values must refer to [named types](/sql/types), e.g.
`integer`.

To create a custom nested `list` or `map`, you must first create a custom `list`
or `map`. This creates a named type, which can then be referred to in another
custom type's properties.

## Examples

### Custom `list`

```mzsql
CREATE TYPE int4_list AS LIST (ELEMENT TYPE = int4);

SELECT '{1,2}'::int4_list::text AS custom_list;
```
```
 custom_list
-------------
 {1,2}
```

### Nested custom `list`

```mzsql
CREATE TYPE int4_list_list AS LIST (ELEMENT TYPE = int4_list);

SELECT '{{1,2}}'::int4_list_list::text AS custom_nested_list;
```
```
 custom_nested_list
--------------------
 {{1,2}}
```

### Custom `map`

```mzsql
CREATE TYPE int4_map AS MAP (KEY TYPE = text, VALUE TYPE = int4);

SELECT '{a=>1}'::int4_map::text AS custom_map;
```
```
 custom_map
------------
 {a=>1}
```

### Nested custom `map`

```mzsql
CREATE TYPE int4_map_map AS MAP (KEY TYPE = text, VALUE TYPE = int4_map);

SELECT '{a=>{a=>1}}'::int4_map_map::text AS custom_nested_map;
```
```
 custom_nested_map
-------------------
{a=>{a=>1}}
```

### Custom `row` type
```mzsql
CREATE TYPE row_type AS (a int, b text);
SELECT ROW(1, 'a')::row_type as custom_row_type;
```
```
custom_row_type
-----------------
(1,a)
```

### Nested `row` type
```mzsql
CREATE TYPE nested_row_type AS (a row_type, b float8);
SELECT ROW(ROW(1, 'a'), 2.3)::nested_row_type AS custom_nested_row_type;
```
```
custom_nested_row_type
------------------------
("(1,a)",2.3)
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-type.md" >}}

## Related pages

* [`DROP TYPE`](../drop-type)
* [`SHOW TYPES`](../show-types)




---

## CREATE VIEW


`CREATE VIEW` defines a view, which simply provides an alias
for the embedded `SELECT` statement.

The results of a view can be incrementally maintained **in memory** within a
[cluster](/concepts/clusters/) by creating an [index](../create-index).
This allows you to serve queries without the overhead of
materializing the view.

### Usage patterns

{{% views-indexes/table-usage-pattern-intro %}}
{{% views-indexes/table-usage-pattern %}}

## Syntax

{{< diagram "create-view.svg" >}}

Field | Use
------|-----
**TEMP** / **TEMPORARY** | Mark the view as [temporary](#temporary-views).
**OR REPLACE** | If a view exists with the same name, replace it with the view defined in this statement. You cannot replace views that other views depend on, nor can you replace a non-view object with a view.
**IF NOT EXISTS** | If specified, _do not_ generate an error if a view of the same name already exists. <br/><br/>If _not_ specified, throw an error if a view of the same name already exists. _(Default)_
_view&lowbar;name_ | A name for the view.
**(** _col_ident_... **)** | Rename the `SELECT` statement's columns to the list of identifiers, both of which must be the same length. Note that this is required for statements that return multiple columns with the same identifier.
_select&lowbar;stmt_ | The [`SELECT` statement](../select) to embed in the view.

## Details

[//]: # "TODO(morsapaes) Add short usage patterns section + point to relevant
architecture patterns once these exist."

### Temporary views

The `TEMP`/`TEMPORARY` keyword creates a temporary view. Temporary views are
automatically dropped at the end of the SQL session and are not visible to other
connections. They are always created in the special `mz_temp` schema.

Temporary views may depend upon other temporary database objects, but non-temporary
views may not depend on temporary objects.

## Examples

### Creating a view

```mzsql
CREATE VIEW purchase_sum_by_region
AS
    SELECT sum(purchase.amount) AS region_sum,
           region.id AS region_id
    FROM region
    INNER JOIN user
        ON region.id = user.region_id
    INNER JOIN purchase
        ON purchase.user_id = user.id
    GROUP BY region.id;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-view.md" >}}

## Additional information

- Views can be monotonic; that is, views can be recognized as append-only.

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`SHOW CREATE VIEW`](../show-create-view)
- [`DROP VIEW`](../drop-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`CREATE INDEX`](../create-index)




---

## DEALLOCATE


`DEALLOCATE` clears [prepared statements](../prepare) that have been created during the current session. Even without an explicit `DEALLOCATE` command, all prepared statements will be cleared at the end of a session.

## Syntax

```mzsql
DEALLOCATE <name>|ALL ;
```

Syntax element | Description
---------------|------------
`<name>`  | The name of the prepared statement to clear.
**ALL**  |  Clear all prepared statements from this session.

## Example

```mzsql
DEALLOCATE a;
```

## Related pages

- [`PREPARE`]
- [`EXECUTE`]

[`PREPARE`]:../prepare
[`EXECUTE`]:../execute




---

## DECLARE


`DECLARE` creates a cursor, which can be used with
[`FETCH`](/sql/fetch), to retrieve a limited number of rows at a time
from a larger query. Large queries or queries that don't ever complete
([`SUBSCRIBE`](/sql/subscribe)) can be difficult to use with many PostgreSQL drivers
that wait until all rows are returned before returning control to an
application. Using `DECLARE` and [`FETCH`](/sql/fetch) allows you to
fetch only some of the rows at a time.

## Syntax

```mzsql
DECLARE <cursor_name> CURSOR FOR <query>;
```

Syntax element | Description
---------------|------------
`<cursor_name>` | The name of the cursor to be created.
`<query>` | The query ([`SELECT`](/sql/select) or [`SUBSCRIBE`](/sql/subscribe)) that will provide the rows to be returned by the cursor.




---

## DELETE


`DELETE` removes values stored in [user-created tables](../create-table).

## Syntax

```mzsql
DELETE FROM <table_name> [AS <alias>]
[USING <from_item> [, ...]]
[WHERE <condition>]
;
```

Syntax element | Description
---------------|------------
`<table_name>` | The table whose values you want to remove.
**AS** `<alias>` | Optional. The alias for the table. If specified, only permit references to `<table_name>` as `<alias>`.
**USING** _from_item_ | Optional. Table expressions whose columns you want to reference in the `WHERE` clause. This supports the same syntax as the **FROM** clause in [`SELECT`](../select) statements, e.g. supporting aliases.
**WHERE** `<condition>` | Optional. Only remove rows which evaluate to `true` for _condition_.

## Details

### Known limitations

* `DELETE` cannot be used inside [transactions](../begin).
* `DELETE` can reference [read-write tables](../create-table) but not
  [sources](../create-source) or read-only tables.
* **Low performance.** While processing a `DELETE` statement, Materialize cannot
  process other `INSERT`, `UPDATE`, or `DELETE` statements.

## Examples

```mzsql
CREATE TABLE delete_me (a int, b text);
INSERT INTO delete_me
    VALUES
    (1, 'hello'),
    (2, 'goodbye'),
    (3, 'ok');
DELETE FROM delete_me WHERE b = 'hello';
SELECT * FROM delete_me ORDER BY a;
```
```
 a |    b
---+---------
 2 | goodbye
 3 | ok
```
```mzsql
CREATE TABLE delete_using (b text);
INSERT INTO delete_using VALUES ('goodbye'), ('ciao');
DELETE FROM delete_me
    USING delete_using
    WHERE delete_me.b = delete_using.b;
SELECT * FROM delete_me;
```
```
 a | b
---+----
 3 | ok
```
```mzsql
DELETE FROM delete_me;
SELECT * FROM delete_me;
```
```
 a | b
---+---
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/delete.md" >}}

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`SELECT`](../select)




---

## DISCARD


`DISCARD` resets state associated with the current session.

## Syntax

```mzsql
DISCARD TEMP|TEMPORARY|ALL ;
```


Syntax element | Description
---------------|------------
**TEMP**  | Drops any temporary objects created by the current session.
**TEMPORARY** | Alias for `TEMP`.
**ALL** | Drops any temporary objects, deallocates any extant prepared statements, and closes any extant cursors that were created by the current session.




---

## DROP CLUSTER


`DROP CLUSTER` removes an existing cluster from Materialize. If there are indexes or materialized views depending on the cluster, you must explicitly drop them first, or use the `CASCADE` option.

## Syntax

```mzsql
DROP CLUSTER [IF EXISTS] <cluster_name> [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional.  If specified, do not return an error if the specified cluster does not exist.
`<cluster_name>` | The cluster you want to drop. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).
**CASCADE** | Optional. If specified, remove the cluster and its dependent objects.
**RESTRICT** | Optional. Do not drop the cluster if it has dependencies. _(Default)_

## Examples

### Dropping a cluster with no dependencies

To drop an existing cluster, run:

```mzsql
DROP CLUSTER auction_house;
```

To avoid issuing an error if the specified cluster does not exist, use the `IF EXISTS` option:

```mzsql
DROP CLUSTER IF EXISTS auction_house;
```

### Dropping a cluster with dependencies

If the cluster has dependencies, Materialize will throw an error similar to:

```mzsql
DROP CLUSTER auction_house;
```

```nofmt
ERROR:  cannot drop cluster with active indexes or materialized views
```

, and you'll have to explicitly ask to also remove any dependent objects using the `CASCADE` option:

```mzsql
DROP CLUSTER auction_house CASCADE;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-cluster.md" >}}

## Related pages

- [`SHOW CLUSTERS`](../show-clusters)
- [`DROP OWNED`](../drop-owned)




---

## DROP CLUSTER REPLICA


`DROP CLUSTER REPLICA` deprovisions an existing replica of the specified
[unmanaged cluster](/sql/create-cluster/#unmanaged-clusters). To remove
the cluster itself, use the [`DROP CLUSTER`](/sql/drop-cluster) command.

{{< tip >}}
When getting started with Materialize, we recommend starting with managed
clusters.
{{</ tip >}}

## Syntax

```mzsql
DROP CLUSTER REPLICA [IF EXISTS] <cluster_name>.<replica_name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified cluster replica does not exist.
`<cluster_name>` | The cluster you want to remove a replica from. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).
`<replica_name>` | The cluster replica you want to drop. For available cluster replicas, see [`SHOW CLUSTER REPLICAS`](../show-cluster-replicas).

## Examples

```mzsql
SHOW CLUSTER REPLICAS WHERE cluster = 'auction_house';
```

```nofmt
    cluster    | replica
---------------+---------
 auction_house | bigger
```

```mzsql
DROP CLUSTER REPLICA auction_house.bigger;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/drop-cluster-replica.md" >}}


## Related pages

- [`CREATE CLUSTER REPLICA`](../create-cluster-replica)
- [`SHOW CLUSTER REPLICAS`](../show-cluster-replicas)
- [`DROP OWNED`](../drop-owned)




---

## DROP CONNECTION


`DROP CONNECTION` removes a connection from Materialize. If there are sources
depending on the connection, you must explicitly drop them first, or use the
`CASCADE` option.

## Syntax

```mzsql
DROP CONNECTION [IF EXISTS] <connection_name> [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified connection does not exist.
`connection_name>` | The connection you want to drop. For available connections, see [`SHOW CONNECTIONS`](../show-connections).
**CASCADE** | Optional. If specified, remove the connection and its dependent objects.
**RESTRICT** | Optional. Do not drop the connection if it has dependencies. _(Default)_

## Examples

### Dropping a connection with no dependencies

To drop an existing connection, run:

```mzsql
DROP CONNECTION kafka_connection;
```

To avoid issuing an error if the specified connection does not exist, use the `IF EXISTS` option:

```mzsql
DROP CONNECTION IF EXISTS kafka_connection;
```

### Dropping a connection with dependencies

If the connection has dependencies, Materialize will throw an error similar to:

```mzsql
DROP CONNECTION kafka_connection;
```

```nofmt
ERROR:  cannot drop materialize.public.kafka_connection: still depended upon by catalog item
'materialize.public.kafka_source'
```

, and you'll have to explicitly ask to also remove any dependent objects using the `CASCADE` option:

```mzsql
DROP CONNECTION kafka_connection CASCADE;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-connection.md"
>}}


## Related pages

- [`SHOW CONNECTIONS`](../show-connections)
- [`DROP OWNED`](../drop-owned)




---

## DROP DATABASE


`DROP DATABASE` removes a database from Materialize.

{{< warning >}} `DROP DATABASE` immediately removes all objects within the
database without confirmation. Use with care! {{< /warning >}}

## Syntax

```mzsql
DROP DATABASE [IF EXISTS] <database_name> [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional.  If specified, do not return an error if the specified database does not exist.
`<database_name>` | The database you want to drop. For available databases, see [`SHOW DATABASES`](../show-databases).
**CASCADE** | Optional. Remove the database and its dependent objects. _(Default)_
**RESTRICT** | Optional. If specified, do not remove this database if it contains any schemas.

## Example

### Remove a database containing schemas
You can use either of the following commands:

- ```mzsql
  DROP DATABASE my_db;
  ```
- ```mzsql
  DROP DATABASE my_db CASCADE;
  ```

### Remove a database only if it contains no schemas
```mzsql
DROP DATABASE my_db RESTRICT;
```

### Do not issue an error if attempting to remove a nonexistent database
```mzsql
DROP DATABASE IF EXISTS my_db;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-database.md" >}}

## Related pages

- [`DROP OWNED`](../drop-owned)




---

## DROP INDEX


`DROP INDEX` removes an index from Materialize.

## Syntax

```mzsql
DROP INDEX [IF EXISTS] <index_name> [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified index does not exist.
`<index_name>` | Index to drop.
**CASCADE** | Optional. If specified, remove the index and its dependent objects.
**RESTRICT** | Optional. Remove the index. _(Default.)_

{{< note >}}

Since indexes do not have dependent objects, `DROP INDEX`, `DROP INDEX
RESTRICT`, and `DROP INDEX CASCADE` are equivalent.

{{< /note >}}

## Privileges

To execute the `DROP INDEX` statement, you need:

{{< include-md file="shared-content/sql-command-privileges/drop-index.md" >}}

## Examples

### Remove an index

{{< tip >}}

In the **Materialize Console**, you can view existing indexes in the [**Database
object explorer**](/console/data/). Alternatively, you can use the
[`SHOW INDEXES`](/sql/show-indexes) command.

{{< /tip >}}

Using the  `DROP INDEX` commands, the following example drops an index named `q01_geo_idx`.

```mzsql
DROP INDEX q01_geo_idx;
```

If the index `q01_geo_idx` does not exist, the above operation returns an error.

### Remove an index without erroring if the index does not exist

You can specify the `IF EXISTS` option so that the `DROP INDEX` command does
not return an error if the index to drop does not exist.

```mzsql
DROP INDEX IF EXISTS q01_geo_idx;
```

## Related pages

- [`CREATE INDEX`](/sql/create-index)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW INDEXES`](/sql/show-indexes)
- [`DROP OWNED`](/sql/drop-owned)




---

## DROP MATERIALIZED VIEW


`DROP MATERIALIZED VIEW` removes a materialized view from Materialize. If there
are other views depending on the materialized view, you must explicitly drop
them first, or use the `CASCADE` option.

## Syntax

```mzsql
DROP MATERIALIZED VIEW [IF EXISTS] <view_name> [RESTRICT|CASCADE];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named materialized view does not exist.
`<view_name>` | The materialized view you want to drop. For available materialized views, see [`SHOW MATERIALIZED VIEWS`](../show-materialized-views).
**RESTRICT** | Optional. Do not drop this materialized view if any other views depend on it. _(Default)_
**CASCADE** | Optional. If specified, drop the materialized view and all views that depend on this materialized view.

## Examples

### Dropping a materialized view with no dependencies

```mzsql
DROP MATERIALIZED VIEW winning_bids;
```
```nofmt
DROP MATERIALIZED VIEW
```

### Dropping a materialized view with dependencies

```mzsql
DROP MATERIALIZED VIEW winning_bids;
```

```nofmt
ERROR:  cannot drop materialize.public.winning_bids: still depended
upon by catalog item 'materialize.public.wb_custom_art'
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/drop-materialized-view.md" >}}

## Related pages

- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`DROP OWNED`](../drop-owned)




---

## DROP NETWORK POLICY (Cloud)


*Available for Materialize Cloud only*

`DROP NETWORK POLICY` removes an existing network policy from Materialize.
Network policies are part of Materialize's framework for [access control](/security/cloud/).

To alter the rules in a network policy, use the [`ALTER NETWORK POLICY`](../alter-network-policy)
command.

## Syntax

```mzsql
DROP NETWORK POLICY [IF EXISTS] <name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified network policy does not exist.
`<name>`        | The network policy you want to drop. For available network policies, see [`SHOW NETWORK POLICIES`](../show-network-policies).

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/drop-network-policy.md" >}}

## Related pages

- [`SHOW NETWORK POLICIES`](../show-network-policies)
- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`CREATE NETWORK POLICY`](../create-network-policy)




---

## DROP OWNED


`DROP OWNED` drops all the objects that are owned by one of the specified roles.
Any privileges granted to the given roles on objects will also be revoked.

{{< note >}}
Unlike [PostgreSQL](https://www.postgresql.org/docs/current/sql-drop-owned.html), Materialize drops
all objects across all databases, including the database itself.
{{< /note >}}

## Syntax

```mzsql
DROP OWNED BY <role_name> [, ...] [RESTRICT|CASCADE];
```

Syntax element | Description
---------------|------------
`<role_name>`   | The role name whose owned objects will be dropped.
**CASCADE** | Optional. If specified, remove all dependent objects.
**RESTRICT**  | Optional. Do not drop anything if any non-index objects depencies exist. _(Default.)_

## Examples

```mzsql
DROP OWNED BY joe;
```

```mzsql
DROP OWNED BY joe, george CASCADE;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-owned.md" >}}

## Related pages

- [`REVOKE PRIVILEGE`](../revoke-privilege)
- [`CREATE ROLE`](../create-role)
- [`REASSIGN OWNED`](../reassign-owned)
- [`DROP CLUSTER`](../drop-cluster)
- [`DROP CLUSTER REPLICA`](../drop-cluster-replica)
- [`DROP CONNECTION`](../drop-connection)
- [`DROP DATABASE`](../drop-database)
- [`DROP INDEX`](../drop-index)
- [`DROP MATERIALIZED VIEW`](../drop-materialized-view)
- [`DROP SCHEMA`](../drop-schema)
- [`DROP SECRET`](../drop-secret)
- [`DROP SINK`](../drop-sink)
- [`DROP SOURCE`](../drop-source)
- [`DROP TABLE`](../drop-table)
- [`DROP TYPE`](../drop-type)
- [`DROP VIEW`](../drop-view)




---

## DROP ROLE


`DROP ROLE` removes a role from Materialize.

## Syntax

```mzsql
DROP ROLE [IF EXISTS] <role_name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified role does not exist.
`<role_name>` | The role you want to drop. For available roles, see [`mz_roles`](/sql/system-catalog/mz_catalog#mz_roles).

## Details

You cannot drop the current role.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-role.md" >}}

## Related pages

- [`ALTER ROLE`](../alter-role)
- [`CREATE ROLE`](../create-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)




---

## DROP SCHEMA


`DROP SCHEMA` removes a schema from Materialize.

## Syntax

```mzsql
DROP SCHEMA [IF EXISTS] <schema_name> [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named schema does not exist.
`<schema_name>` | The schema you want to remove. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**CASCADE** | Remove the schema and its dependent objects.
**RESTRICT** | Do not remove this schema if it contains any sources or views. _(Default)_

## Details

Before you can drop a schema, you must [drop all sources](../drop-source) and
[views](../drop-view) it contains, or use the **CASCADE** option.

## Example

### Remove a schema with no dependent objects
```mzsql
SHOW SOURCES FROM my_schema;
```
```nofmt
my_file_source
```
```mzsql
DROP SCHEMA my_schema;
```

### Remove a schema with dependent objects
```mzsql
SHOW SOURCES FROM my_schema;
```
```nofmt
my_file_source
```
```mzsql
DROP SCHEMA my_schema CASCADE;
```

### Remove a schema only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP SCHEMA my_schema;
  ```
- ```mzsql
  DROP SCHEMA my_schema RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent schema

```mzsql
DROP SCHEMA IF EXISTS my_schema;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-schema.md" >}}

## Related pages

- [`SHOW SCHEMAS`](../show-schemas)
- [`CREATE SCHEMA`](../create-schema)
- [`DROP OWNED`](../drop-owned)




---

## DROP SECRET


`DROP SECRET` removes a secret from Materialize's secret management system. If
there are connections depending on the secret, you must explicitly drop them
first, or use the `CASCADE` option.

## Syntax

```mzsql
DROP SECRET [IF EXISTS] <secret_name> [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified secret does not exist.
_secret&lowbar;name_ | The secret you want to drop. For available secrets, see [`SHOW SECRETS`](../show-secrets).
**CASCADE** | Optional. If specified, remove the secret and its dependent objects.
**RESTRICT** | Optional. Do not drop the secret if it has dependencies. _(Default)_

## Examples

### Dropping a secret with no dependencies

To drop an existing secret, run:

```mzsql
DROP SECRET kafka_sasl_password;
```

To avoid issuing an error if the specified secret does not exist, use the `IF EXISTS` option:

```mzsql
DROP SECRET IF EXISTS kafka_sasl_password;
```

### Dropping a secret with dependencies

If the secret has dependencies, Materialize will throw an error similar to:

```mzsql
DROP SECRET kafka_sasl_password;
```

```nofmt
ERROR:  cannot drop materialize.public.kafka_sasl_password: still depended upon by catalog
 item 'materialize.public.kafka_connection'
```

, and you'll have to explicitly ask to also remove any dependent objects using the `CASCADE` option:

```mzsql
DROP SECRET kafka_sasl_password CASCADE;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-secret.md" >}}

## Related pages

- [`SHOW SECRETS`](../show-secrets)
- [`DROP OWNED`](../drop-owned)




---

## DROP SINK


`DROP SINK` removes a sink from Materialize.

{{% kafka-sink-drop  %}}

## Syntax

```mzsql
DROP SINK [IF EXISTS] <sink_name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified sink does not exist.
`<sink_name>` | The sink you want to drop. You can find available sink names through [`SHOW SINKS`](../show-sinks).

## Examples

```mzsql
SHOW SINKS;
```
```nofmt
my_sink
```
```mzsql
DROP SINK my_sink;
```
```nofmt
DROP SINK
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-sink.md" >}}

## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)
- [`DROP OWNED`](../drop-owned)




---

## DROP SOURCE


`DROP SOURCE` removes a source from Materialize. If there are objects depending
on the source, you must explicitly drop them first, or use the `CASCADE`
option.

## Syntax

```mzsql
DROP SOURCE [IF EXISTS] <source_name> [RESTRICT|CASCADE];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named source does not exist.
`<source_name>` | The name of the source you want to remove.
**CASCADE** | Optional. If specified, remove the source and its dependent objects.
**RESTRICT** | Optional. Do not remove this source if it has dependent objects. _(Default.)_

## Examples

### Remove a source with no dependent objects

```mzsql
SHOW SOURCES;
```
```nofmt
...
my_source
```
```mzsql
DROP SOURCE my_source;
```

### Remove a source with dependent objects

```mzsql
SHOW SOURCES;
```
```nofmt
...
my_source
```
```mzsql
DROP SOURCE my_source CASCADE;
```

### Remove a source only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP SOURCE my_source;
  ```
- ```mzsql
  DROP SOURCE my_source RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent source

```mzsql
DROP SOURCE IF EXISTS my_source;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-source.md" >}}

## Related pages

- [`CREATE SOURCE`](../create-source)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW CREATE SOURCE`](../show-create-source)
- [`DROP OWNED`](../drop-owned)




---

## DROP TABLE


`DROP TABLE` removes a table from Materialize.

## Syntax

```mzsql
DROP TABLE [IF EXISTS] <table_name> [RESTRICT|CASCADE];
```

Syntax element | Description
---------------|------------
**IF EXISTS**  | Optional. If specified, do not return an error if the named table doesn't exist.
`<table_name>` | The name of the table to remove.
**CASCADE** | Optional. If specified, remove the table and its dependent objects.
**RESTRICT**  | Optional. Don't remove the table if any non-index objects depend on it. _(Default.)_

## Examples

### Remove a table with no dependent objects
Create a table *t* and verify that it was created:

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
SHOW TABLES;
```
```
TABLES
------
t
```

Remove the table:

```mzsql
DROP TABLE t;
```
### Remove a table with dependent objects

Create a table *t*:

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
INSERT INTO t VALUES (1, 'yes'), (2, 'no'), (3, 'maybe');
SELECT * FROM t;
```
```
a |   b
---+-------
2 | no
1 | yes
3 | maybe
(3 rows)
```

Create a materialized view from *t*:

```mzsql
CREATE MATERIALIZED VIEW t_view AS SELECT sum(a) AS sum FROM t;
SHOW MATERIALIZED VIEWS;
```
```
name    | cluster
--------+---------
t_view  | default
(1 row)
```

Remove table *t*:

```mzsql
DROP TABLE t CASCADE;
```

### Remove a table only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP TABLE t;
  ```
- ```mzsql
  DROP TABLE t RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent table

```mzsql
DROP TABLE IF EXISTS t;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-table.md" >}}

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`DROP OWNED`](../drop-owned)




---

## DROP TYPE


`DROP TYPE` removes a [custom data type](../create-type). You cannot use it on default data types.

## Syntax

```mzsql
DROP TYPE [IF EXISTS] <data_type_name> [RESTRICT|CASCADE];
```

Syntax element | Description
---------------|------------
**IF EXISTS**  | Optional. If specified, do not return an error if the named type doesn't exist.
`<data_type_name>` | The name of the type to remove.
**CASCADE** | Optional. If specified, remove the type and its dependent objects, such as tables or other types.
**RESTRICT** | Optional. Don't remove the type if any objects depend on it. _(Default.)_

## Examples

### Remove a type with no dependent objects
```mzsql
CREATE TYPE int4_map AS MAP (KEY TYPE = text, VALUE TYPE = int4);

SHOW TYPES;
```
```
    name
--------------
  int4_map
(1 row)
```

```mzsql
DROP TYPE int4_map;

SHOW TYPES;
```
```
  name
--------------
(0 rows)
```

### Remove a type with dependent objects

By default, `DROP TYPE` will not remove a type with dependent objects. The **CASCADE** switch will remove both the specified type and *all its dependent objects*.

In the example below, the **CASCADE** switch removes `int4_list`, `int4_list_list` (which depends on `int4_list`), and the table *t*, which has a column of data type `int4_list`.

```mzsql
CREATE TYPE int4_list AS LIST (ELEMENT TYPE = int4);

CREATE TYPE int4_list_list AS LIST (ELEMENT TYPE = int4_list);

CREATE TABLE t (a int4_list);

SHOW TYPES;
```
```
      name
----------------
 int4_list
 int4_list_list
(2 rows)
```

```mzsql
DROP TYPE int4_list CASCADE;

SHOW TYPES;

SELECT * FROM t;
```
```
 name
------
(0 rows)
ERROR:  unknown catalog item 't'
```

### Remove a type only if it has no dependent objects

You can use either of the following commands:

- ```mzsql
  DROP TYPE int4_list;
  ```
- ```mzsql
  DROP TYPE int4_list RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent type

```mzsql
DROP TYPE IF EXISTS int4_list;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-type.md" >}}

## Related pages

* [`CREATE TYPE`](../create-type)
* [`SHOW TYPES`](../show-types)
* [`DROP OWNED`](../drop-owned)




---

## DROP USER


`DROP USER` removes a role from Materialize. `DROP USER` is an alias for [`DROP ROLE`](../drop-role).


## Syntax

```mzsql
DROP USER [IF EXISTS] <role_name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified role does not exist.
`<role_name>` | The role you want to drop. For available roles, see [`mz_roles`](/sql/system-catalog/mz_catalog#mz_roles).

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-user.md" >}}

## Related pages

- [`ALTER ROLE`](../alter-role)
- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)




---

## DROP VIEW


`DROP VIEW` removes a view from Materialize.

## Conceptual framework

Materialize maintains views after you create them. If you no longer need the
view, you can remove it.

Because views rely on receiving data from sources, you must drop all views that
rely on a source before you can [drop the source](../drop-source) itself. You can achieve this easily using **DROP SOURCE...CASCADE**.

## Syntax

```mzsql
DROP VIEW [IF EXISTS] <view_name> [RESTRICT|CASCADE];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named view does not exist.
`<view_name>` | The view you want to drop. You can find available view names through [`SHOW VIEWS`](../show-views).
**RESTRICT** | Optional. Do not drop this view if any other views depend on it. _(Default)_
**CASCADE** | Optional. Drop all views that depend on this view.

## Examples

```mzsql
SHOW VIEWS;
```
```nofmt
  name
---------
 my_view
```
```mzsql
DROP VIEW my_view;
```
```nofmt
DROP VIEW
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-view.md" >}}

## Related pages

- [`CREATE VIEW`](../create-view)
- [`SHOW VIEWS`](../show-views)
- [`SHOW CREATE VIEW`](../show-create-view)
- [`DROP OWNED`](../drop-owned)




---

## EXECUTE


`EXECUTE` plans and executes [prepared statements](../prepare). Since prepared statements only last the duration of a session, the statement must have been prepared during the current session.

If the `PREPARE` statement specified some parameters, you must pass values compatible with those parameters to `EXECUTE`. Values are considered compatible here when they can be [_assignment cast_](../../sql/functions/cast/#valid-casts). (This is the same category of casting that happens for `INSERT`.)


## Syntax

{{< diagram "execute.svg" >}}

Field | Use
------|-----
**name**  | The name of the prepared statement to execute.
**parameter**  |  The actual value of a parameter to the prepared statement.

## Example

The following example [prepares a statement](/sql/prepare/) `a` and runs it
using the `EXECUTE` statement:

```mzsql
PREPARE a AS SELECT 1 + $1;
EXECUTE a (2);
```

All prepared statements will be cleared at the end of a session. You can also
explicitly deallocate the statement using [`DEALLOCATE`].


## Related pages

- [`PREPARE`]
- [`DEALLOCATE`]

[`PREPARE`]:../prepare
[`DEALLOCATE`]:../deallocate




---

## EXPLAIN ANALYZE


`EXPLAIN ANALYZE`:

- Summarizes cluster status.
- Reports on the performance of indexes and materialized views.
- Provide the execution plan annotated with TopK hints. The TopK
  query pattern groups by some key and return the first K elements within each
  group according to some ordering.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

```mzsql
EXPLAIN ANALYZE
      CPU [, MEMORY] [WITH SKEW]
    | MEMORY [, CPU] [WITH SKEW]
    | HINTS
FOR INDEX <name> | MATERIALIZED VIEW <name>
[ AS SQL ]
;

EXPLAIN ANALYZE CLUSTER
      CPU [, MEMORY] [WITH SKEW]
    | MEMORY [, CPU] [WITH SKEW]
[ AS SQL ]
;
```
{{< tip >}}
If you want to specify both `CPU` or `MEMORY`, they may be listed in any order;
however, each may appear only once.
{{</ tip >}}

Parameter    | Description
-------------|-----
**CPU**      | Reports consumed CPU time information `total_elapsed` for each operator (not inclusive of its child operators; `FOR INDEX`, `FOR MATERIALIZED VIEW`) or for each object in the current cluster (`CLUSTER`).
**MEMORY**   | Reports consumed memory information `total_memory` and number of records `total_records` for each operator (not including child operators; `FOR INDEX`, `FOR MATERIALIZED VIEW`) or for each object in the current cluster (`CLUSTER`).
**WITH SKEW** | *Optional.* If specified, includes additional information about average and per-worker consumption and ratios (of `CPU` and/or `MEMORY`).
**HINTS**    | Annotates the LIR plan with [TopK hints] (`FOR INDEX`, `FOR MATERIALIZED VIEW`).
**AS SQL**   | *Optional.* If specified, returns the SQL associated with the specified `EXPLAIN ANALYZE` command without executing it. You can modify this SQL as a starting point to create customized queries.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/explain-analyze.md"
>}}

## Examples

`EXPLAIN ANALYZE` attributes runtime metrics to [`PHYSICAL PLAN` operators](/sql/explain-plan#reference-plan-operators).

The attribution examples in this
section reference the `wins_by_item` index (and the underlying `winning_bids`
view) from the [quickstart
guide](/get-started/quickstart/#step-2-create-the-source):

```sql
CREATE SOURCE auction_house
FROM LOAD GENERATOR AUCTION
(TICK INTERVAL '1s', AS OF 100000)
FOR ALL TABLES;

CREATE VIEW winning_bids AS
  SELECT DISTINCT ON (a.id) b.*, a.item, a.seller
    FROM auctions AS a
    JOIN bids AS b
      ON a.id = b.auction_id
   WHERE b.bid_time < a.end_time
     AND mz_now() >= a.end_time
   ORDER BY a.id, b.amount DESC, b.bid_time, b.buyer;

CREATE INDEX wins_by_item ON winning_bids (item);
```

### `EXPLAIN ANALYZE MEMORY`

The following examples reports on the memory usage of the index `wins_by_item`:

```mzsql
EXPLAIN ANALYZE MEMORY FOR INDEX wins_by_item;
```

For the index, `EXPLAIN ANALYZE MEMORY` reports on the memory usage and the
number of records for each operator in the dataflow:

|         operator            | total_memory | total_records |
|-----------------------------|-------------:|--------------:|
| Arrange                     | 386 kB       |         15409 |
|   Stream u8                 |              |               |
| **Non-monotonic TopK**      | **36 MB**    |    **731975** |
|   Differential Join %0 » %1 |              |               |
|     Arrange                 | 2010 kB      |         84622 |
|       Stream u5             |              |               |
|     Arrange                 | 591 kB       |         15410 |
|       Read u4               |              |               |

The results show the `TopK` operator is overwhelmingly responsible for memory
usage.

### `EXPLAIN ANALYZE CPU`

The following examples reports on the cpu usage of the index `wins_by_item`:

```mzsql
EXPLAIN ANALYZE CPU FOR INDEX wins_by_item;
```

For the index, `EXPLAIN ANALYZE CPU` reports on total time spent in each
operator (not inclusive of its child operators) in the dataflow:

|         operator            |  total_elapsed  |
|:----------------------------|----------------:|
| Arrange                     | 00:00:00.161341 |
|   Stream u8                 |                 |
| Non-monotonic TopK          | 00:00:15.153963 |
|   Differential Join %0 » %1 | 00:00:00.978381 |
|     Arrange                 | 00:00:00.536282 |
|       Stream u5             |                 |
|     Arrange                 | 00:00:00.171586 |
|       Read u4               |                 |


### `EXPLAIN ANALYZE CPU, MEMORY`

You can report on both CPU and memory usage simultaneously:

```mzsql
EXPLAIN ANALYZE CPU, MEMORY FOR INDEX wins_by_item;
```

You can specify both `CPU` or `MEMORY` in any order; however, each may appear
only once. The order of `CPU` and `MEMORY` in the statement determines the order
of the output columns

For example, in the above example where the `CPU` was listed before `MEMORY`,
the CPU time (`total_elasped`) column is listed before the `MEMORY` information
`total_memory` and `total_records`.

|         operator            |  total_elapsed  | total_memory | total_records |
|:----------------------------|----------------:|-------------:|--------------:|
| Arrange                     | 00:00:00.190801 | 389 kB       |         15435 |
|   Stream u8                 |                 |              |               |
| Non-monotonic TopK          | 00:00:16.193381 | 36 MB        |        733457 |
|   Differential Join %0 » %1 | 00:00:01.107056 |              |               |
|     Arrange                 | 00:00:00.592818 | 2017 kB      |         84793 |
|       Stream u5             |                 |              |               |
|     Arrange                 | 00:00:00.214064 | 595 kB       |         15436 |
|       Read u4               |                 |              |               |

### `EXPLAIN ANALYZE ... WITH SKEW`

In clusters with more than one worker, [worker
skew](/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers)
can occur when data is unevenly distributed across workers. Extreme cases of
skew can seriously impact performance. You can use `EXPLAIN ANALYZE ... WITH
SKEW` to identify this scenario. The `WITH SKEW` option includes the per worker
and average worker performance numbers for each operator, along with each
worker's ratio compared to the average.

For the below example, assume there are 2 workers in the cluster.

{{< tip >}}

To determine how many workers a given cluster size has, you can query
[`mz_catalog.mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes).

{{</ tip >}}

You can explain `MEMORY` and/or `CPU` with the `WITH SKEW` option. For example,
the following runs `EXPLAIN ANALYZE MEMORY WITH SKEW`:

```mzsql
EXPLAIN ANALYZE MEMORY WITH SKEW FOR INDEX wins_by_item;
```

The results include the per worker and average worker performance numbers for
each operator, along with each worker's ratio compared to the average:

|         operator            | worker_id | memory_ratio | worker_memory | avg_memory | total_memory | records_ratio | worker_records | avg_records | total_records |
|:----------------------------|----------:|-------------:|--------------:|-----------:|-------------:|--------------:|---------------:|------------:|--------------:|
| Arrange                     | 0         |          0.8 | 78 kB         | 97 kB      | 389 kB       |           0.8 |           3099 |        3862 |         15448 |
| Arrange                     | 1         |         1.59 | 154 kB        | 97 kB      | 389 kB       |          1.58 |           6113 |        3862 |         15448 |
| Arrange                     | 2         |         1.61 | 157 kB        | 97 kB      | 389 kB       |          1.61 |           6236 |        3862 |         15448 |
| **Arrange**                 | **3**     |        **0** | **272 bytes** | **97 kB**  | **389 kB**   |         **0** |          **0** |    **3862** |     **15448** |
|   Stream u8                 |           |              |               |            |              |               |                |             |               |
| Non-monotonic TopK          | 0         |            1 | 9225 kB       | 9261 kB    | 36 MB        |             1 |         183148 |   183486.75 |        733947 |
| Non-monotonic TopK          | 1         |            1 | 9222 kB       | 9261 kB    | 36 MB        |             1 |         183319 |   183486.75 |        733947 |
| Non-monotonic TopK          | 2         |            1 | 9301 kB       | 9261 kB    | 36 MB        |             1 |         183585 |   183486.75 |        733947 |
| Non-monotonic TopK          | 3         |            1 | 9293 kB       | 9261 kB    | 36 MB        |             1 |         183895 |   183486.75 |        733947 |
|   Differential Join %0 » %1 |           |              |               |            |              |               |                |             |               |
|     Arrange                 | 0         |         0.97 | 487 kB        | 505 kB     | 2019 kB      |             1 |          21165 |     21213.5 |         84854 |
|     Arrange                 | 1         |         0.97 | 489 kB        | 505 kB     | 2019 kB      |             1 |          21274 |     21213.5 |         84854 |
|     Arrange                 | 2         |          1.1 | 555 kB        | 505 kB     | 2019 kB      |             1 |          21298 |     21213.5 |         84854 |
|     Arrange                 | 3         |         0.96 | 487 kB        | 505 kB     | 2019 kB      |             1 |          21117 |     21213.5 |         84854 |
|       Stream u5             |           |              |               |            |              |               |                |             |               |
|     Arrange                 | 0         |            1 | 149 kB        | 149 kB     | 595 kB       |             1 |           3862 |      3862.5 |         15450 |
|     Arrange                 | 1         |            1 | 148 kB        | 149 kB     | 595 kB       |             1 |           3862 |      3862.5 |         15450 |
|     Arrange                 | 2         |            1 | 149 kB        | 149 kB     | 595 kB       |             1 |           3863 |      3862.5 |         15450 |
|     Arrange                 | 3         |            1 | 149 kB        | 149 kB     | 595 kB       |             1 |           3863 |      3862.5 |         15450 |
|       Read u4               |           |              |               |            |              |               |                |             |               |

The `ratio` column tells you whether a worker is particularly over- or
under-loaded:

- a `ratio` below 1 indicates a worker doing a below average amount of work.

- a `ratio` above 1 indicates a worker doing an above average amount of work.

While there will always be some amount of variation, very high ratios indicate a
skewed workload. Here the memory ratios are mostly close to 1, indicating there is very
little worker skew everywhere but at the top level arrangement, where worker 3 has no records.

### `EXPLAIN ANALYZE HINTS`

`EXPLAIN ANALYZE HINTS` can annotate your plan (specifically, each TopK
operator) with suggested [TopK hints]; i.e., [`DISTINCT ON INPUT GROUP SIZE=`
value](/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1).

For example, the following runs `EXPLAIN ANALYZE HINTS` on the `wins_by_item`
index:

```mzsql
EXPLAIN ANALYZE HINTS FOR INDEX wins_by_item;
```

The result shows that the `wins_by_item` index has only one `TopK` operator and
suggests the hint (i.e, the `DISTINCT ON INPUT GROUP SIZE=` value) of `255.0`.

|         operator            | levels | to_cut | hint | savings |
|:----------------------------|-------:|-------:|-----:|--------:|
| Arrange                     |        |        |      |         |
|   Stream u8                 |        |        |      |         |
| Non-monotonic TopK          |      8 |      6 |  255 | 26 MB   |
|   Differential Join %0 » %1 |        |        |      |         |
|     Arrange                 |        |        |      |         |
|       Stream u5             |        |        |      |         |
|     Arrange                 |        |        |      |         |
|       Read u4               |        |        |      |         |


With the hint information, you can recreate the view and index to improve memory
usage:

```sql
DROP VIEW winning_bids CASCADE;

CREATE VIEW winning_bids AS
    SELECT DISTINCT ON (a.id) b.*, a.item, a.seller
      FROM auctions AS a
      JOIN bids AS b
        ON a.id = b.auction_id
     WHERE b.bid_time < a.end_time
       AND mz_now() >= a.end_time
   OPTIONS (DISTINCT ON INPUT GROUP SIZE = 255) -- use hint!
  ORDER BY a.id,
    b.amount DESC,
    b.bid_time,
    b.buyer;

CREATE INDEX wins_by_item ON winning_bids (item);
```

Re-running the `TopK`-hints query will show only `null` hints; i.e., there are
no hints because our `TopK` is now appropriately sized.

To see if the indexe's memory usage has improved with the hint, rerun the
following `EXPLAIN ANALYZE MEMORY` command:

```mzsql
EXPLAIN ANALYZE MEMORY FOR INDEX wins_by_item;
```

The results show that the `TopK` operator uses `11MB` of memory, less than a third of the
[~36MB of memory it was using before](#explain-analyze-memory):

|         operator            | total_memory | total_records |
|:----------------------------|-------------:|--------------:|
| Arrange                     | 391 kB       |         15501 |
|   Stream u10                |              |               |
| **Non-monotonic TopK**      | **11 MB**    |    **226706** |
|   Differential Join %0 » %1 |              |               |
|     Arrange                 | 1994 kB      |         85150 |
|       Stream u5             |              |               |
|     Arrange                 | 601 kB       |         15502 |
|       Read u4               |              |               |

### `EXPLAIN ANALYZE CLUSTER ...`

It is possible to look at overall cluster status, rather than individual indexes or materialized views. This is useful for quickly identifying skewed dataflows as well as which dataflows are taking up the most resources.

Running `EXPLAIN ANALYZE CLUSTER MEMORY, CPU` will identify which dataflows are using the most resources. Running this statement on a small cluster with 4 workers, we find:

|             object              | global_id |  total_elapsed  | total_memory | total_records |
|:--------------------------------|----------:|----------------:|-------------:|--------------:|
| materialize.public.wins_by_item | u8        | 00:00:50.731033 | 42 MB        |        861512 |
| materialize.public.wins_by_item | u9        | 00:00:00.992696 | 406 kB       |         15950 |

Note that the output is sorted by `total_elapsed`---the output is ordered by whichever property is listed first. Here it _also_ happens to be sorted by `total_memory` and `total_records`: the dataflows processing the most data took the most time. On a cluster with dozens of indexes and materialized views, `EXPLAIN ANALYZE CLUSTER` reveals which dataflows are consuming the most resources.

We can quickly find skewed dataflows on a cluster by running `EXPLAIN ANALYZE CLUSTER MEMORY WITH SKEW`; here is an example on a small cluster with 4 workers:

| object                          | global_id | worker_id | max_operator_memory_ratio | worker_memory | avg_memory | total_memory | max_operator_records_ratio | worker_records | avg_records | total_records |
|:--------------------------------|----------:|----------:|--------------------------:|--------------:|-----------:|-------------:|---------------------------:|---------------:|------------:|--------------:|
| materialize.public.wins_by_item | u9        | 2         |                      1.63 | 164 kB        | 101 kB     | 404 kB       |                       1.62 |           6411 |      3968.5 |         15874 |
| materialize.public.wins_by_item | u9        | 1         |                      1.58 | 159 kB        | 101 kB     | 404 kB       |                       1.58 |           6286 |      3968.5 |         15874 |
| materialize.public.wins_by_item | u8        | 1         |                      1.06 | 10 MB         | 10 MB      | 41 MB        |                          1 |         213718 |    214325.5 |        857302 |
| materialize.public.wins_by_item | u8        | 0         |                      1.01 | 10 MB         | 10 MB      | 41 MB        |                          1 |         215075 |    214325.5 |        857302 |
| materialize.public.wins_by_item | u8        | 3         |                      1.01 | 10 MB         | 10 MB      | 41 MB        |                          1 |         214020 |    214325.5 |        857302 |
| materialize.public.wins_by_item | u8        | 2         |                         1 | 10 MB         | 10 MB      | 41 MB        |                          1 |         214489 |    214325.5 |        857302 |
| materialize.public.wins_by_item | u9        | 0         |                      0.79 | 80 kB         | 101 kB     | 404 kB       |                        0.8 |           3177 |      3968.5 |         15874 |
| materialize.public.wins_by_item | u9        | 3         |                         0 | 272 bytes     | 101 kB     | 404 kB       |                            |                |      3968.5 |         15874 |

The `u9` and `u8` dataflows make up the `wins_by_item` dataflow (where `u8` does the work and `u9` arranges it).
Both dataflows run on all four workers.
We report the `max_operator_memory_ratio` for each worker on each dataflow: what is the ratio of that dataflow's memory usage to the average memory usage across all workers?
Note that the output results are sorted by `max_operator_memory_ratio`, making it easy to spot skew. Here, workers 1 and 2 hold most of the records; worker 0 has half as many, and worker 3 has none at all.

### `EXPLAIN ANALYZE ... AS SQL`

Under the hood:

- For returning Memory/CPU information, `EXPLAIN ANALYZE` runs SQL queries that
correlate [`mz_introspection` performance
information](https://materialize.com/docs/sql/system-catalog/mz_introspection/)
with the LIR operators in
[`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping).

- For TopK hints, `EXPLAIN ANALYZE` uses
[`mz_introspection.mz_expected_group_size_advice`](/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice)
introspection source to offer hints on sizing `TopK` operators.

You can append `AS SQL` to any `EXPLAIN ANALYZE` statement to see the SQL that
would be run (without running it). You can then customize this SQL to report
finer grained or other information. For example:

```mzsql
EXPLAIN ANALYZE HINTS FOR INDEX wins_by_item AS SQL;
```

The results show the SQL that `EXPLAIN ANALYZE` would run to get the TopK hints
for the `wins_by_items` index.

[TopK hints]: /transform-data/idiomatic-materialize-sql/top-k/#query-hints-1




---

## EXPLAIN FILTER PUSHDOWN



{{< public-preview />}}

`EXPLAIN FILTER PUSHDOWN` reports filter pushdown statistics for `SELECT`
statements and materialized views.

## Syntax

{{< diagram "explain-filter-pushdown.svg" >}}

### Explained object

The following objects can be explained with `EXPLAIN FILTER PUSHDOWN`:

 Explained object           | Description
----------------------------|-------------------------------------------------------------------------------
 **select_stmt**            | Display statistics for an ad-hoc [`SELECT` statement](../select).
 **MATERIALIZED VIEW name** | Display statistics for an existing materialized view.

## Details

Materialize's [filter pushdown optimization](../../transform-data/patterns/temporal-filters/#temporal-filter-pushdown)
can be critical for large or append-only collections, since queries with a short
temporal filter many only need to fetch a small number of recent updates
instead of the full history of the data over time. However, it can be hard to
predict how well this optimization will behave for a particular query and
dataset. The standard [`EXPLAIN PLAN`](../../sql/explain-plan/#output-modifiers)
command can give some guidance as to whether this optimization applies to a
query at all... but exactly how much data gets filtered out will depend on both
statistics about the data itself, as well as how the data is chunked into
parts.

`EXPLAIN FILTER PUSHDOWN` looks at the current durable state of the collection,
determines exactly which parts are necessary to answer a `SELECT` query or
rehydrate a materialized view, and reports the total number of parts and bytes
that the query has selected, along with the total number of parts and bytes in
the shard.

## Examples

For the following examples, assume that you have created [an auction house load
generator source](/sql/create-source/load-generator/#creating-an-auction-load-generator)
in your environment.

### Explaining a `SELECT` query

Suppose you're interested in checking the number of recent bids.

```mzsql
SELECT count(*) FROM bids WHERE bid_time + '5 minutes' > mz_now();
```

Over time, the number of bids will grow indefinitely, but the number of recent
bids should stay about the same. If the filter pushdown optimization can make
sure that this query only needs to fetch recent bids from the storage layer
instead of _all_ historical bids, that could have an extreme impact in
performance.

Explaining this query includes a `pushdown=` field under `Source materialize.public.bids`,
which indicates that this filter can be pushed down.

```mzsql
EXPLAIN
SELECT count(*) FROM bids WHERE bid_time + '5 minutes' > mz_now();
```

```nofmt
...

 Source materialize.public.bids
   filter=((timestamp_tz_to_mz_timestamp((#4{bid_time} + 00:05:00)) > mz_now()))
   pushdown=((timestamp_tz_to_mz_timestamp((#4{bid_time} + 00:05:00)) > mz_now()))
```

However, this doesn't suggest how effective filter pushdown will be. Suppose you
have two queries: one which filters to the last minute and one to the last
hour; both can be pushed down, but the second will probably fetch much more
data.

Suppose it's been \~1 hour since you set up the auction house load generator
source, and you'd like to get a sense of how much data your query would need to
fetch.

```mzsql
EXPLAIN FILTER PUSHDOWN FOR
SELECT count(*) FROM bids WHERE bid_time + '5 minutes' > mz_now();
```

```nofmt
         Source          | Total Bytes | Selected Bytes | Total Parts | Selected Parts
-------------------------+-------------+----------------+-------------+----------------
 materialize.public.bids | 146508      | 34621          | 19          | 11
```

It looks like Materialize is fetching about a fifth of the data in terms of
bytes, and about half of the individual parts (this is not unexpected:
Materialize stores older data in larger chunks). If you run this query again,
you'll see the numbers change as more data is ingested and Materialize compacts
it into more efficient representations in the background.

If you instead query for the last hour of data, you can see that since you only
created the auction house source \~1 hour ago, Materialize needs to fetch
almost everything.

```mzsql
EXPLAIN FILTER PUSHDOWN FOR
SELECT count(*) FROM bids WHERE bid_time + '1 hour' > mz_now();
```

```nofmt
         Source          | Total Bytes | Selected Bytes | Total Parts | Selected Parts
-------------------------+-------------+----------------+-------------+----------------
 materialize.public.bids | 162473      | 162473         | 17          | 17
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/explain-filter-pushdown.md" >}}




---

## EXPLAIN PLAN


`EXPLAIN PLAN` displays the plans used for:

|                             |                       |
|-----------------------------|-----------------------|
| <ul><li>`SELECT` statements </li><li>`CREATE VIEW` statements</li><li>`CREATE INDEX` statements</li><li>`CREATE MATERIALIZED VIEW` statements</li></ul>|<ul><li>Existing views</li><li>Existing indexes</li><li>Existing materialized views</li></ul> |

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

{{< tabs >}}
{{< tab "FOR SELECT">}}
```mzsql
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]       -- The FOR keyword is required if the PLAN keyword is specified
    <SELECT ...>
;
```
{{</tab>}}
{{< tab "FOR CREATE VIEW">}}

```mzsql
EXPLAIN <RAW | DECORRELATED | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR
    <CREATE VIEW ...>
;
```
{{</tab>}}
{{< tab "FOR CREATE INDEX">}}
```mzsql
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE INDEX ...>
;
```
{{</tab>}}
{{< tab "FOR CREATE MATERIALIZED VIEW">}}
```mzsql
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]          -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE MATERIALIZED VIEW ...>
;
```
{{</tab>}}
{{< tab "FOR VIEW">}}
```mzsql
EXPLAIN <RAW | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR
  VIEW <name>
;
```
{{</tab>}}
{{< tab "FOR INDEX">}}
```mzsql
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
      [ WITH (<output_modifier> [, <output_modifier> ...]) ]
      [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
  INDEX <name>
;
```
{{</tab>}}
{{< tab "FOR MATERIALIZED VIEW">}}
```mzsql
EXPLAIN [[ RAW | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ] -- The FOR keyword is required if the PLAN keyword is specified
  MATERIALIZED VIEW <name>
;
```
{{</tab>}}
{{</tabs>}}

Note that the `FOR` keyword is required if the `PLAN` keyword is present. The following three statements are equivalent:

```mzsql
EXPLAIN <explainee>;
EXPLAIN PLAN FOR <explainee>;
EXPLAIN PHYSICAL PLAN AS TEXT FOR <explainee>;
```

If `PHSYICAL PLAN` is specified without an `AS`-format, we will provide output similar to the above, but more verbose. The following two statements are equivalent (and produce the more verbose output):

```mzsql
EXPLAIN PHYSICAL PLAN FOR <explainee>;
EXPLAIN PHYSICAL PLAN AS VERBOSE TEXT FOR <explainee>;
```

### Explained object

The following object types can be explained.

Explained object | Description
------|-----
**select_stmt** | Display a plan for an ad-hoc [`SELECT` statement](../select).
**create_view** | Display a plan for a [`CREATE VIEW` statement](../create-view).
**create_index** | Display a plan for a [`CREATE INDEX` statement](../create-index).
**create_materialized_view** | Display a plan for a [`CREATE MATERIALIZED VIEW` statement](../create-materialized-view).
**VIEW name** | Display the `RAW` or `LOCALLY OPTIMIZED` plan for an existing view.
**INDEX name** | Display the `OPTIMIZED` or `PHYSICAL` plan for an existing index.
**MATERIALIZED VIEW name** | Display the `OPTIMIZED` or `PHYSICAL` plan for an existing materialized view.

### Output format

You can select between `JSON` and `TEXT` for the output format of `EXPLAIN PLAN`. Non-text
output is more machine-readable and can be parsed by common graph visualization libraries,
while formatted text is more human-readable.

Output type | Description
------|-----
**TEXT** | Format the explanation output as UTF-8 text.
**JSON** | Format the explanation output as a JSON object.

### Explained stage

This stage determines the query optimization stage at which the plan snapshot will be taken.

Plan Stage | Description
------|-----
**RAW PLAN** | Display the raw plan; this is closest to the original SQL.
**DECORRELATED PLAN** | Display the decorrelated but not-yet-optimized plan.
**LOCALLY OPTIMIZED** | Display the locally optimized plan (before view inlining and access path selection). This is the final stage for regular `CREATE VIEW` optimization.
**OPTIMIZED PLAN** | Display the optimized plan.
**PHYSICAL PLAN** | _(Default)_ Display the physical plan; this corresponds to the operators shown in [`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping).

### Output modifiers

Output modifiers act as boolean toggles and can be combined in order to slightly tweak
the information and rendering style of the generated explanation output.

Modifier | Description
------|-----
**arity** | _(on by default)_ Annotate each subplan with its number of produced columns. This is useful due to the use of offset-based column names.
**cardinality** | Annotate each subplan with a symbolic estimate of its cardinality.
**join implementations** | Render details about the [implementation strategy of optimized MIR `Join` nodes](#explain-with-join-implementations).
**keys** | Annotates each subplan with a parenthesized list of unique keys. Each unique key is presented as a bracketed list of column identifiers. A list of column identifiers is reported as a unique key when for each setting of those columns to values there is at most one record in the collection. For example, `([0], [1,2])` is a list of two unique keys: column zero is a unique key, and columns 1 and 2 also form a unique key. Materialize only reports the most succinct form of keys, so for example while `[0]` and `[0, 1]` might both be unique keys, the latter is implied by the former and omitted. `()` indicates that the collection does not have any unique keys, while `([])` indicates that the empty projection is a unique key, meaning that the collection consists of 0 or 1 rows.
**node identifiers** | Annotate each subplan in a `PHYSICAL PLAN` with its node ID.
**redacted** | Anonymize literals in the output.
**timing** | Annotate the output with the optimization time.
**types** | Annotate each subplan with its inferred type.
**humanized expressions** | _(on by default)_ Add human-readable column names to column references. For example, `#0{id}` refers to column 0, whose name is `id`. Note that SQL-level aliasing is not considered when inferring column names, which means that the displayed column names can be ambiguous.
**filter pushdown** | _(on by default)_ For each source, include a `pushdown` field that explains which filters [can be pushed down to the storage layer](../../transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

Note that most modifiers are currently only supported for the `AS TEXT` output.

## Details

To execute `SELECT` statements, Materialize generates a plan consisting of
operators that interface with our underlying Differential dataflow engine.
`EXPLAIN PLAN` lets you see the plan for a given query, which can provide insight
into Materialize's behavior for specific queries, e.g. performance.

### Query compilation pipeline

The job of the Materialize planner is to turn SQL code into a differential
dataflow program. We get there via a series of progressively lower-level plans:

```text
SQL ⇒ raw plan ⇒ decorrelated plan ⇒ optimized plan ⇒ physical plan ⇒ dataflow
```

#### From SQL to raw plan

In this stage, the planner:

- Replaces SQL variable names with column numbers.
- Infers the type of each expression.
- Chooses the correct overload for each function.

#### From raw plan to decorrelated plan

In this stage, the planner:

- Replaces subqueries and lateral joins with non-nested operations.
- Replaces `OUTER` joins with lower-level operations.
- Replaces global aggregate default values with lower-level operations.

#### From decorrelated plan to optimized plan

In this stage, the planner performs various optimizing rewrites:

- Coalesces joins.
- Chooses join order and implementation.
- Fuses adjacent operations.
- Removes redundant operations.
- Pushes down predicates.
- Evaluates any operations on constants.

#### From optimized plan to physical plan

In this stage, the planner:

- Decides on the exact execution details of each operator, and maps plan operators to differential dataflow operators.
- Makes the final choices about creating or reusing [arrangements](/get-started/arrangements/#arrangements).

#### From physical plan to dataflow

In the final stage, the planner:

- Renders an actual dataflow from the physical plan, and
- Installs the new dataflow into the running system.

The rendering step does not make any further optimization choices, as the physical plan is meant to
be a definitive and complete description of the rendered dataflow.

### Fast path queries

Queries are sometimes implemented using a _fast path_.
In this mode, the program that implements the query will just hit an existing index,
transform the results, and optionally apply a finishing action. For fast path queries,
all of these actions happen outside of the regular dataflow engine. The fast path is
indicated by an "Explained Query (fast path):" heading before the explained query in the `EXPLAIN`,
`EXPLAIN OPTIMIZED PLAN` and `EXPLAIN PHYSICAL PLAN` result.

```text
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=materialize.public.t1 t1_x_idx=[lookup value=(5)]

Used Indexes:
  - materialize.public.t1_x_idx (lookup)
```


### Reading plans

Materialize plans are directed, potentially cyclic, graphs of operators. Each operator in the graph
receives inputs from zero or more other operators and produces a single output.
Sub-graphs where each output is consumed only once are rendered as tree-shaped fragments.
Sub-graphs consumed more than once are represented as common table expressions (CTEs).
In the example below, the CTE `l0` represents a linear sub-plan (a chain of `Read` from the table `t`)
which is used in both inputs of a self-join (`Differential Join`).

```text
> CREATE TABLE t(x INT NOT NULL, y INT NOT NULL);
CREATE TABLE
> EXPLAIN SELECT t1.x, t1.y
          FROM (SELECT * FROM t WHERE x > y) AS t1,
               (SELECT * FROM t where x > y) AS t2
          WHERE t1.y = t2.y;
                     Physical Plan
--------------------------------------------------------
 Explained Query:                                      +
   →With                                               +
     cte l0 =                                          +
       →Read materialize.public.t                      +
   →Return                                             +
     →Differential Join %0 » %1                        +
       Join stage %0: Lookup key #0{y} in %1           +
       →Arrange                                        +
         Keys: 1 arrangement available, plus raw stream+
           Arrangement 0: #1{y}                        +
         →Stream l0                                    +
       →Arrange                                        +
         Keys: 1 arrangement available, plus raw stream+
           Arrangement 0: #0{y}                        +
         →Read l0                                      +
                                                       +
 Source materialize.public.t                           +
   filter=((#0{x} > #1{y}))                            +
                                                       +
 Target cluster: quickstart                            +
```

Note that CTEs in optimized plans do not directly correspond to CTEs in your original SQL query: For example, CTEs might disappear due to inlining (i.e., when a CTE is used only once, its definition is copied to that usage site); new CTEs can appear due to the optimizer recognizing that a part of the query appears more than once (aka common subexpression elimination). Also, certain SQL-level concepts, such as outer joins or subqueries, do not have an explicit representation in optimized plans, and are instead expressed as a pattern of operators involving CTEs. CTE names are always `l0`, `l1`, `l2`, ..., and do not correspond to SQL-level CTE names.

<a name="explain-plan-columns"></a>

Many operators need to refer to columns in their input. These are displayed like
`#3` for column number 3. (Columns are numbered starting from column 0). To get a better sense of
columns assigned to `Map` operators, it might be useful to request [the `arity` output modifier](#output-modifiers).

Each operator can also be annotated with additional metadata. Some details are shown in the default `EXPLAIN` output (`EXPLAIN PHYSICAL PLAN AS TEXT`), but are hidden elsewhere. <a
name="explain-with-join-implementations"></a>In `EXPLAIN OPTIMIZED
PLAN`, details about the implementation in the `Join` operator can be requested
with [the `join implementations` output modifier](#output-modifiers) (that is,
`EXPLAIN OPTIMIZED PLAN WITH (join implementations) FOR ...`).

```text
Join on=(#1 = #2 AND #3 = #4) type=delta
  implementation
    %0:t » %1:u[#0]K » %2:v[#0]K
    %1:u » %0:t[#1]K » %2:v[#0]K
    %2:v » %1:u[#1]K » %0:t[#1]K
  ArrangeBy keys=[[#1]]
    ReadStorage materialize.public.t
  ArrangeBy keys=[[#0], [#1]]
    ReadStorage materialize.public.u
  ArrangeBy keys=[[#0]]
    ReadStorage materialize.public.v
```
The `%0`, `%1`, etc. refer to each of the join inputs.
A *differential* join shows one join path, which is simply a sequence of binary
joins (each of whose results need to be maintained as state).
A [*delta* join](/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins)
shows a join path for each of the inputs.
The expressions in
a bracket show the key for joining with that input. The letters after the brackets
indicate the input characteristics used for join ordering. `U` means unique, the
number of `K`s means the key length, `A` means already arranged (e.g., an index
exists). The small letters refer to filter characteristics:
**e**quality to a literal,
**l**ike,
is **n**ull,
**i**nequality to a literal,
any **f**ilter.

A plan can optionally end with a finishing action, which can sort, limit and
project the result data. This operator is special, as it can only occur at the
top of the plan. Finishing actions are executed outside the parallel dataflow
that implements the rest of the plan.

```
Finish order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5 output=[#0, #1]
  CrossJoin
    ReadStorage materialize.public.r
    ReadStorage materialize.public.s
```

Below the plan, a "Used indexes" section indicates which indexes will be used by the query, [and in what way](/transform-data/optimization/#use-explain-to-verify-index-usage).

### Reference: Plan operators

Materialize offers several output formats for `EXPLAIN` and debugging.
LIR plans as rendered in
[`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping)
are deliberately succinct, while the plans in other formats give more
detail.

The decorrelated and optimized plans from `EXPLAIN DECORRELATED PLAN
FOR ...`, `EXPLAIN LOCALLY OPTIMIZED PLAN FOR ...`, and `EXPLAIN
OPTIMIZED PLAN FOR ...` are in a mid-level representation that is
closer to LIR than SQL. The raw plans from `EXPLAIN RAW PLAN FOR ...`
are closer to SQL (and therefore less indicative of how the query will
actually run).

{{< tabs >}}

{{< tab "In fully optimized physical (LIR) plans" >}}
{{< explain-plans/operator-table data="explain_plan_operators" planType="LIR" >}}
{{< /tab >}}

{{< tab "In decorrelated and optimized plans (default EXPLAIN)" >}}
{{< explain-plans/operator-table data="explain_plan_operators" planType="optimized" >}}
{{< /tab >}}

{{< tab "In raw plans" >}}
{{< explain-plans/operator-table data="explain_plan_operators" planType="raw" >}}
{{< /tab >}}

{{< /tabs >}}

Operators are sometimes marked as `Fused ...`. We write this to mean that the operator is fused with its input, i.e., the operator below it. That is, if you see a `Fused X` operator above a `Y` operator:

```
→Fused X
  →Y
```

Then the `X` and `Y` operators will be combined into a single, more efficient operator.

## Examples

For the following examples, let's assume that you have [the auction house load generator](/sql/create-source/load-generator/#creating-an-auction-load-generator) created in your current environment.

### Explaining a `SELECT` query

Let's start with a simple join query that lists the total amounts bid per buyer.

Explain the optimized plan as text:

```mzsql
EXPLAIN
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Same explanation as above, but with the `EXPLAIN` expressed a bit more verbosely:

```mzsql
EXPLAIN PLAN FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Same explanation as above, but expressed even more verbosely:

```mzsql
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Same as above, but every sub-plan is annotated with its schema types:

```mzsql
EXPLAIN WITH(types) FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

Explain the physical plan as verbose text (i.e., in complete detail):

```mzsql
EXPLAIN PHYSICAL PLAN FOR
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

### Explaining an index on a view

Let's create a view with an index for the above query.

```mzsql
-- create the view
CREATE VIEW my_view AS
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
-- create an index on the view
CREATE INDEX my_view_idx ON my_view(id);
```

You can inspect the plan of the dataflow that will maintain your index with the following statements.

Explain the optimized plan as text:

```mzsql
EXPLAIN
INDEX my_view_idx;
```

Same as above, but a bit more verbose:

```mzsql
EXPLAIN PLAN FOR
INDEX my_view_idx;
```

Same as above, but even more verbose:

```mzsql
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
INDEX my_view_idx;
```

Same as above, but every sub-plan is annotated with its schema types:

```mzsql
EXPLAIN WITH(types) FOR
INDEX my_view_idx;
```

Explain the physical plan as verbose text:

```mzsql
EXPLAIN PHYSICAL PLAN FOR
INDEX my_view_idx;
```

### Explaining a materialized view

Let's create a materialized view for the above `SELECT` query.

```mzsql
CREATE MATERIALIZED VIEW my_mat_view AS
SELECT a.id, sum(b.amount) FROM accounts a JOIN bids b ON(a.id = b.buyer) GROUP BY a.id;
```

You can inspect the plan of the dataflow that will maintain your view with the following statements.

Explain the optimized plan as text:

```mzsql
EXPLAIN
MATERIALIZED VIEW my_mat_view;
```

Same as above, but a bit more verbose:

```mzsql
EXPLAIN PLAN FOR
MATERIALIZED VIEW my_mat_view;
```

Same as above, but even more verbose:

```mzsql
EXPLAIN OPTIMIZED PLAN AS TEXT FOR
MATERIALIZED VIEW my_mat_view;
```

Same as above, but every sub-plan is annotated with its schema types:

```mzsql
EXPLAIN WITH(types)
MATERIALIZED VIEW my_mat_view;
```

Explain the physical plan as verbose text:

```mzsql
EXPLAIN PHYSICAL PLAN FOR
MATERIALIZED VIEW my_mat_view;
```

## Debugging running dataflows

The [`EXPLAIN ANALYZE`](/sql/explain-analyze/) statement will let you debug memory and cpu usage (optionally with information about worker skew) for existing indexes and materialized views in terms of their physical plan operators. It can also attribute [TopK hints](/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1) to individual operators.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/explain-plan.md" >}}




---

## EXPLAIN SCHEMA


`EXPLAIN KEY SCHEMA` or `EXPLAIN VALUE SCHEMA` shows the generated schemas for a `CREATE SINK` statement without creating the sink.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

{{< diagram "explain-schema.svg" >}}

#### `sink_definition`

{{< diagram "sink-definition.svg" >}}

### Output format

Only `JSON` can be specified as the output format.

Output type | Description
------|-----
**JSON** | Format the explanation output as a JSON object.

## Details
When creating a an Avro-formatted Kafka sink, Materialize automatically generates Avro schemas for the message key and value and publishes them to a schema registry.
This command shows what the generated schemas would look like, without creating the sink.

## Examples

```mzsql
CREATE TABLE t (c1 int, c2 text);
COMMENT ON TABLE t IS 'materialize comment on t';
COMMENT ON COLUMN t.c2 IS 'materialize comment on t.c2';

EXPLAIN VALUE SCHEMA FOR
  CREATE SINK
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'test_avro_topic')
  KEY (c1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
```

```
                   Schema
--------------------------------------------
 {                                         +
   "type": "record",                       +
   "name": "envelope",                     +
   "doc": "materialize comment on t",      +
   "fields": [                             +
     {                                     +
       "name": "c1",                       +
       "type": [                           +
         "null",                           +
         "int"                             +
       ]                                   +
     },                                    +
     {                                     +
       "name": "c2",                       +
       "type": [                           +
         "null",                           +
         "string"                          +
       ],                                  +
       "doc": "materialize comment on t.c2"+
     }                                     +
   ]                                       +
 }
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/explain-schema.md"
>}}




---

## EXPLAIN TIMESTAMP


`EXPLAIN TIMESTAMP` displays the timestamps used for a `SELECT` statement -- valuable information to investigate query delays.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

{{< diagram "explain-timestamp.svg" >}}

### Output format

You can select between `JSON` and `TEXT` for the output format of `EXPLAIN TIMESTAMP`. Non-text
output is more machine-readable and can be parsed by common graph visualization libraries,
while formatted text is more human-readable.

Output type | Description
------|-----
**TEXT** | Format the explanation output as UTF-8 text.
**JSON** | Format the explanation output as a JSON object.

## Details

The explanation is divided in two parts:

1. Determinations for a timestamp
2. Sources frontiers

Having a _query timestamp_ outside the _[read, write)_ frontier values of a source can explain the presence of delays. While in the middle, the space of processed but not yet compacted data, allows building and returning a correct result immediately.

### Determinations for a timestamp

Queries in Materialize have a logical timestamp, known as _query timestamp_. It plays a critical role to return a correct result. Returning a correct result implies retrieving data with the same logical time from each source present in a query.

In this case, sources are objects providing data: materialized views, views, indexes, tables, or sources. Each will have a pair of logical timestamps frontiers, denoted as _sources frontiers_.

This section contains the following fields:

Field | Meaning | Example
---------|---------|---------
**query timestamp** | The query timestamp value |`1673612424151 (2023-01-13 12:20:24.151)`
**oracle read** | The value of the timeline's oracle timestamp, if used. | `1673612424151 (2023-01-13 12:20:24.151)`
**largest not in advance of upper** | The largest timestamp not in advance of upper. | `1673612424151 (2023-01-13 12:20:24.151)`
**since** | The maximum read frontier of all involved sources. | `[1673612423000 (2023-01-13 12:20:23.000)]`
**upper** | The minimum write frontier of all involved sources | `[1673612424152 (2023-01-13 12:20:24.152)]`
**can respond immediately** | Returns true when the **query timestamp** is greater or equal to **since** and lower than **upper** | `true`
**timeline** | The type of timeline the query's timestamp belongs | `Some(EpochMilliseconds)`

A timeline value of `None` means the query is known to be constant across all timestamps.

### Sources frontiers

Every source has a beginning _read frontier_ and an ending _write frontier_.
They stand for a source’s limits to return a correct result immediately:

* Read frontier: Indicates the minimum logical timestamp to return a correct result (advanced by _compaction_)
* Write frontier: Indicates the maximum timestamp to build a correct result without waiting for unprocessed data.

Each source has its own output section consisting of the following fields:

Field | Meaning | Example
---------|---------|---------
**source** | Source’s identifiers | `source materialize.public.raw_users (u2014, storage)`
**read frontier** | Minimum logical timestamp. |`[1673612423000 (2023-01-13 12:20:23.000)]`
**write frontier** | Maximum logical timestamp. | `[1673612424152 (2023-01-13 12:20:24.152)]`


## Examples

```mzsql
EXPLAIN TIMESTAMP FOR SELECT * FROM users;
```
```
                                 Timestamp
---------------------------------------------------------------------------
                 query timestamp: 1673618185152 (2023-01-13 13:56:25.152) +
           oracle read timestamp: 1673618185152 (2023-01-13 13:56:25.152) +
 largest not in advance of upper: 1673618185152 (2023-01-13 13:56:25.152) +
                           upper:[1673618185153 (2023-01-13 13:56:25.153)]+
                           since:[1673618184000 (2023-01-13 13:56:24.000)]+
         can respond immediately: true                                    +
                        timeline: Some(EpochMilliseconds)                 +
                                                                          +
 source materialize.public.raw_users (u2014, storage):                    +
                   read frontier:[1673618184000 (2023-01-13 13:56:24.000)]+
                  write frontier:[1673618185153 (2023-01-13 13:56:25.153)]+
```

<!-- We think of `since` as the "read frontier": times not later than or equal to
`since` cannot be correctly read. We think of `upper` as the "write frontier":
times later than or equal to `upper` may still be written to the TVC. -->
<!-- Who is the oracle? -->
<!--
We maintain a timestamp oracle that returns strictly increasing timestamps
Mentions that this is inspired/similar to Percolator.

Timestamp oracle is periodically bumped up to the current system clock
We never revert oracle if system clock goes backwards.

https://tikv.org/deep-dive/distributed-transaction/timestamp-oracle/
-->
<!-- Materialize's objects request timestamp to the oracle, a timestamp provider. The oracle's timestamp bumps up periodically to match the current system clock, and never goes backwards.

It relies on an oracle, a timestamp provider, to handle them correctly.

The oracle it is a timestamp provider. It bumps up periodically internal value to the current system clock, never going backwards.

Issuing a select statement in Materialize
When a select statement runs, Materialize will pick a timestamp between all the sources:

`max(max(read_frontiers), min(write_frontiers) - 1)` -->

<!-- /// Information used when determining the timestamp for a query.
#[derive(Serialize, Deserialize)]
pub struct TimestampDetermination<T> {
    /// The chosen timestamp context from `determine_timestamp`.
    pub timestamp_context: TimestampContext<T>,
    /// The largest timestamp not in advance of upper.
    pub largest_not_in_advance_of_upper: T,
}


*Query timestamp: The timestamp in a timeline at which the query makes the read
oracle read: The value of the timeline's oracle timestamp, if used.
largest not in advance of upper: The largest timestamp not in advance of upper.
upper: The write frontier of all involved sources.
since: The read frontier of all involved sources.
can respond immediately: True when the write frontier is greater than the query timestamp.
timeline: The type of timeline the query's timestamp belongs:
      /// EpochMilliseconds means the timestamp is the number of milliseconds since
      /// the Unix epoch.
      EpochMilliseconds,
      /// External means the timestamp comes from an external data source and we
      /// don't know what the number means. The attached String is the source's name,
      /// which will result in different sources being incomparable.
      External(String),
      /// User means the user has manually specified a timeline. The attached
      /// String is specified by the user, allowing them to decide sources that are
      /// joinable.
      User(String),

Each source contains two frontiers:
  Read: At which time
  Write:

                 query timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
           oracle read timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
 largest not in advance of upper: 1673612424151 (2023-01-13 12:20:24.151) +
                           upper:[1673612424152 (2023-01-13 12:20:24.152)]+
                           since:[1673612423000 (2023-01-13 12:20:23.000)]+


                                 Timestamp
---------------------------------------------------------------------------
                 query timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
           oracle read timestamp: 1673612424151 (2023-01-13 12:20:24.151) +
 largest not in advance of upper: 1673612424151 (2023-01-13 12:20:24.151) +
                           upper:[1673612424152 (2023-01-13 12:20:24.152)]+
                           since:[1673612423000 (2023-01-13 12:20:23.000)]+
         can respond immediately: true                                    +
                        timeline: Some(EpochMilliseconds)                 +
                                                                          +
 source materialize.public.a (u2014, storage):                            +
                   read frontier:[1673612423000 (2023-01-13 12:20:23.000)]+
                  write frontier:[1673612424152 (2023-01-13 12:20:24.152)]+ -->

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/explain-timestamp.md"
>}}




---

## FETCH


`FETCH` retrieves rows from a query using a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

{{< diagram "fetch.svg" >}}

Field | Use
------|-----
_count_ | The number of rows to retrieve. Defaults to `1` if unspecified.
_cursor&lowbar;name_ | The name of an open cursor.

### `WITH` option

The following option is valid within the `WITH` clause.

Option name | Value type | Default | Describes
------------|------------|---------|----------
`timeout`   | `interval` | None    | When fetching from a [`SUBSCRIBE`](/sql/subscribe) cursor, complete if there are no more rows ready after this timeout. The default will cause `FETCH` to wait for at least one row to be available.

## Details

`FETCH` will return at most the specified _count_ of available rows. Specifying a _count_ of `ALL` indicates that there is no limit on the number of
rows to be returned.

For [`SUBSCRIBE`](/sql/subscribe) queries, `FETCH` by default will wait for rows to be available before returning.
Specifying a _timeout_ of `0s` returns only rows that are immediately available.




---

## GRANT PRIVILEGE


`GRANT PRIVILEGE` grants privileges to [database
role(s)](/sql/create-role/).

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
{{< include-md file="shared-content/rbac-cloud/privilege-for-views-mat-views.md" >}}
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

{{< include-md file="shared-content/rbac-cloud/grant-privilege-all-tables.md" >}}

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
- [`ALTER OWNER`](/sql/#rbac)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)




---

## GRANT ROLE


`GRANT` grants membership of one role to another role. Roles can be members of
other roles, as well as inherit all the privileges of those roles.

## Syntax

{{< diagram "grant-role.svg" >}}

Field         | Use
--------------|--------------------------------------------------
_role_name_   | The role name to add _member_name_ as a member.
_member_name_ | The role name to add to _role_name_ as a member.

## Examples

```mzsql
GRANT data_scientist TO joe;
```

```mzsql
GRANT data_scientist TO joe, mike;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/grant-role.md" >}}

## Useful views

- [`mz_internal.mz_show_role_members`](/sql/system-catalog/mz_internal/#mz_show_role_members)
- [`mz_internal.mz_show_my_role_members`](/sql/system-catalog/mz_internal/#mz_show_my_role_members)

## Related pages

- [`SHOW ROLE MEMBERSHIP`](../show-role-membership)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)




---

## Identifiers


In Materialize, identifiers are used to refer to columns and database objects
like sources, views, and indexes.

## Naming restrictions

Materialize has the following naming restrictions for identifiers:

| Position            | Allowed Characters                                                                 |
|---------------------|------------------------------------------------------------------------------------|
| **First character** | ASCII letters (`a`-`z`, `A`-`Z`), underscore (`_`), or any non-ASCII character     |
| **Remaining**       | ASCII letters (`a`-`z`, `A`-`Z`), digits (`0`-`9`), underscores (`_`), dollar sign (`$`), or any non-ASCII character |

To override these restrictions, you can enclose the identifier in double quotes;
e.g., `"123_source"` or `"fun_source_@"`. Inside double quotes, characters are interpreted literally, except for the double-quote character itself. To include a double quote within a double-quoted identifier, escape it by writing two adjacent double quotes, as in "includes""quote".

{{< note >}}
The identifiers `"."` and `".."` are not allowed.
{{</ note >}}

## Case sensitivity

Materialize performs case folding (the caseless comparison of text) for identifiers, which means that identifiers are effectively case-insensitive (`foo` is the same as `FOO` is the same as `fOo`). This can cause issues when column names come from data sources which do support case-sensitive names, such as Avro-formatted sources.

To avoid conflicts, double-quote all field names (`"field_name"`) when working with case-sensitive sources.

## Renaming restrictions

You cannot rename an item if any of the following are true:

- **It is not uniquely qualified across all dependent references.**

  For example, suppose you have:

  - Two views named `v1` in different databases (`d1` and `d2`) under the same schema name (`s1`), and
  - Both `v1` views are referenced by another view.

  You may rename either `v1` only if every dependent query that mentions both views **fully qualifies all such references**, e.g.:

  ```mzsql
  CREATE VIEW v2 AS
  SELECT *
  FROM d1.s1.v1
  JOIN d2.s1.v1
  ON d1.s1.v1.a = d2.s1.v1.a;
  ```

  If the two views were instead in schemas with distinct names, qualifying by schema alone would be sufficient (you would not need to include the database name).

- Renaming would cause any identifier collision with a dependent query.

  - An existing collision: a dependent query already uses the item’s current
    identifier for some database, schema, object, or column, so changing the
    item’s name would change how that identifier resolves.

    In the examples below, v1 cannot be renamed because dependent queries already use the identifier v1:

    - Any dependent query references a database, schema, or column that uses the same identifier.

        In the following examples, `v1` could _not_ be renamed:

        ```mzsql
        CREATE VIEW v3 AS
        SELECT *
        FROM v1
        JOIN v2
        ON v1.a = v2.v1
        ```

        ```mzsql
        CREATE VIEW v4 AS
        SELECT *
        FROM v1
        JOIN v1.v2
        ON v1.a = v2.a
        ```

  - A proposed-name collision: the new name matches any identifier referenced in
    a dependent query, whether that identifier is referenced explicitly or
    implicitly.

    Consider this example:

    ```mzsql
    CREATE VIEW v5 AS
    SELECT *
    FROM d1.s1.v2
    JOIN v1
    ON v1.a = v2.b
    ```

    You could not rename `v1` to:

    - `a`
    - `b`
    - `v2`
    - `s1`
    - `d1`
    - `materialize` or `public` (implicitly referenced by `materialize.public.v1` using the default database and schema)

## Keyword collision

Materialize is very permissive with accepting SQL keywords as identifiers (e.g.
`offset`, `user`). If Materialize cannot use a keyword as an
identifier in a particular location, it throws a syntax error. You can wrap the
identifier in double quotes to force Materialize to interpret the word as an
identifier instead of a keyword.

For example, `SELECT offset` is invalid, because it looks like a mistyping of
`SELECT OFFSET <n>`. You can wrap the identifier in double quotes, as in
`SELECT "offset"`, to resolve the error.

We recommend that you avoid using keywords as identifiers whenever possible, as
the syntax errors that result are not always obvious.

The current keywords are listed below.

{{< kwlist >}}




---

## INSERT


`INSERT` writes values to [user-defined tables](../create-table).

## Conceptual framework

You might want to `INSERT` data into tables when:

- Manually inserting rows into Materialize from a non-streaming data source.
- Testing Materialize's features without setting up a data stream.

## Syntax

{{< diagram "insert.svg" >}}

Field | Use
------|-----
**INSERT INTO** _table_name_ | The table to write values to.
_alias_ | Only permit references to _table_name_ as _alias_.
_column_name_... | Correlates the inserted rows' columns to _table_name_'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column. <br/><br/>If some but not all of _table_name_'s columns are provided, the unprovided columns receive their type's default value, or `NULL` if no default value was specified.
_expr_... | The expression or value to be inserted into the column. If a given column is nullable, a `NULL` value may be provided.
_query_ | A [`SELECT`](../select) statements whose returned rows you want to write to the table.

## Details

The optional `RETURNING` clause causes `INSERT` to return values based on each inserted row.

### Known limitations

* `INSERT ... SELECT` can reference [read-write tables](../create-table) but not
  [sources](../create-source) or read-only tables _(or views, materialized views, and indexes that
  depend on sources)_.
* **Low performance.** While processing an `INSERT ... SELECT` statement,
  Materialize cannot process other `INSERT`, `UPDATE`, or `DELETE` statements.

## Examples

To insert data into a table, execute an `INSERT` statement where the `VALUES` clause
is followed by a list of tuples. Each tuple in the `VALUES` clause must have a value
for each column in the table. If a column is nullable, a `NULL` value may be provided.

```mzsql
CREATE TABLE t (a int, b text NOT NULL);

INSERT INTO t VALUES (1, 'a'), (NULL, 'b');

SELECT * FROM t;
 a | b
---+---
   | b
 1 | a
```

In the above example, the second tuple provides a `NULL` value for column `a`, which
is nullable. `NULL` values may not be inserted into column `b`, which is not nullable.

You may also insert data using a column specification.

```mzsql
CREATE TABLE t (a int, b text NOT NULL);

INSERT INTO t (b, a) VALUES ('a', 1), ('b', NULL);

SELECT * FROM t;
```
```
 a | b
---+---
   | b
 1 | a
```

You can also insert the values returned from `SELECT` statements:

```mzsql
CREATE TABLE s (a text);

INSERT INTO s VALUES ('c');

INSERT INTO t (b) SELECT * FROM s;

SELECT * FROM t;
```
```
 a | b
---+---
   | b
   | c
 1 | a
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/insert.md" >}}

## Related pages

- [`CREATE TABLE`](../create-table)
- [`DROP TABLE`](../drop-table)
- [`SELECT`](../select)




---

## PREPARE


`PREPARE` creates a prepared statement by parsing the initial `SELECT`, `INSERT`, `UPDATE`, or `DELETE` statement. A subsequent [`EXECUTE`] statement then plans and executes the statement.

## Syntax

```mzsql
PREPARE <name> AS <statement>;
```

Syntax element | Description
---------------|------------
`<name>` | A name for this particular prepared statement that you can later use to execute or deallocate a statement. The name must be unique within a session.
`<statement>`  |  Any `SELECT`, `INSERT`, `UPDATE`, `DELETE`, or `FETCH` statement.

## Details

Prepared statements can take parameters: values that are substituted into the statement when it is executed. The data type is inferred from the context in which the parameter is first referenced. To refer to the parameters in the prepared statement itself, use `$1`, `$2`, etc.

Prepared statements only last for the duration of the current database session. You can also delete them during a session with the [`DEALLOCATE`] command.

## Examples

### Create a prepared statement

```mzsql
PREPARE a AS SELECT 1 + $1;
```

### Execute a prepared statement

```mzsql
EXECUTE a(2);
```

### Deallocate a prepared statement

```mzsql
DEALLOCATE a;
```

## Related pages

- [`DEALLOCATE`]
- [`EXECUTE`]

[`DEALLOCATE`]:../deallocate
[`EXECUTE`]:../execute




---

## REASSIGN OWNED


`REASSIGN OWNED` reassigns the owner of all the objects that are owned by one of the specified roles.

{{< note >}}
Unlike [PostgreSQL](https://www.postgresql.org/docs/current/sql-drop-owned.html), Materialize reassigns
all objects across all databases, including the databases themselves.
{{< /note >}}

## Syntax

```mzsql
REASSIGN OWNED BY <current_owner> [, ...] TO <new_owner>;
```

Syntax element | Description
---------------|------------
`<current_owner>` | The role whose objects are to be reassigned.
`<new_owner>` | The role name of the new owner of these objects.

## Examples

```mzsql
REASSIGN OWNED BY joe TO mike;
```

```mzsql
REASSIGN OWNED BY joe, george TO mike;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/reassign-owned.md"
>}}

## Related pages

- [`ALTER OWNER`](/sql/#rbac)
- [`DROP OWNED`](../drop-owned)




---

## RESET


`RESET` restores the value of a configuration parameter to its default value.
This command is an alternative spelling for [`SET...TO DEFAULT`](../set).

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

```mzsql
RESET <parameter_name>;
```


Syntax element | Description
---------------|------------
`<parameter_name>` | The configuration parameter's name.

{{% configuration-parameters %}}

## Examples

### Reset search path

```mzsql
SHOW search_path;

 search_path
-------------
 qck

RESET search_path;

SHOW search_path;

 search_path
-------------
 public
```

## Related pages

- [`SHOW`](../show)
- [`SET`](../set)




---

## REVOKE PRIVILEGE


`REVOKE` revokes privileges from a database object. The `PUBLIC` pseudo-role can
be used to indicate that the privileges should be revoked from all roles
(including roles that might not exist yet).

## Syntax

{{< note >}}

The syntax supports the `ALL [PRIVILEGES]` shorthand to refer to all
[*applicable* privileges](#applicable-privileges-to-revoke) for the
object type.

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
{{< include-md file="shared-content/rbac-cloud/privilege-for-views-mat-views.md" >}}
{{</ note >}}

For specific materialized view(s)/view(s)/source(s):

```mzsql
REVOKE <SELECT | ALL [PRIVILEGES]>
ON [TABLE] <name> [, <name> ...] -- For PostgreSQL compatibility, if specifying type, use TABLE
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

{{< include-md file="shared-content/rbac-cloud/grant-privilege-all-tables.md" >}}

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

{{< include-md file="shared-content/sql-command-privileges/revoke-privilege.md"
>}}


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




---

## REVOKE ROLE


`REVOKE` revokes membership of a role from the target role.

## Syntax

```mzsql
REVOKE <role_to_remove> [, ...] FROM <target_role> [, ...];
```

Syntax element       | Description
---------------------|------------------
`<role_to_remove>`   | The name of the role to remove from the `<target_role>`.
`<target_role>`      | The name of the role from which the to remove the `<role_to_remove>`.


## Examples

```mzsql
REVOKE data_scientist FROM joe;
```

```mzsql
REVOKE data_scientist FROM joe, mike;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/revoke-role.md" >}}

## Useful views

- [`mz_internal.mz_show_role_members`](/sql/system-catalog/mz_internal/#mz_show_role_members)
- [`mz_internal.mz_show_my_role_members`](/sql/system-catalog/mz_internal/#mz_show_my_role_members)

## Related pages

- [`SHOW ROLE MEMBERSHIP`](../show-role-membership)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)




---

## ROLLBACK


`ROLLBACK` aborts the current [transaction](/sql/begin/#details) and all changes
in the transaction are discarded.

## Syntax

```mzsql
ROLLBACK;
```

## Details

Rolls back the current transaction, discarding all changes made by the transaction.




---

## SELECT


[//]: # "TODO(morsapaes) More than adapting this to the new architecture,
rewrite the page entirely at some point."

The `SELECT` statement is the root of a SQL query, and is used both to bind SQL
queries to named [views](../create-view) or [materialized views](../create-materialized-view),
 and to interactively query data maintained in Materialize. For interactive queries, you should consider creating [indexes](../create-index)
on the underlying relations based on common query patterns.

## Syntax

### select_stmt

{{< diagram "select-stmt.svg" >}}

### simple_select_stmt

{{< diagram "simple-select-stmt.svg" >}}

Field | Use
------|-----
_select&lowbar;with&lowbar;ctes_, _select&lowbar;with&lowbar;recursive&lowbar;ctes_ | [Common table expressions](#common-table-expressions-ctes) (CTEs) for this query.
**(** _col&lowbar;ident_... **)** | Rename the CTE's columns to the list of identifiers, both of which must be the same length.
**ALL** | Return all rows from query _(Default)_.
**DISTINCT** | <a name="select-distinct"></a>Return only distinct values.
**DISTINCT ON (** _col&lowbar;ref_... **)**  | <a name="select-distinct-on"></a>Return only the first row with a distinct value for _col&lowbar;ref_. If an `ORDER BY` clause is also present, then `DISTINCT ON` will respect that ordering when choosing which row to return for each distinct value of `col_ref...`. Please note that in this case, you should start the `ORDER BY` clause with the same `col_ref...` as the `DISTINCT ON` clause. For an example, see [Top K](/transform-data/idiomatic-materialize-sql/top-k/#select-top-1-item).
_target&lowbar;elem_ | Return identified columns or functions.
**FROM** _table&lowbar;expr_ | The tables you want to read from; note that these can also be other `SELECT` statements, [Common Table Expressions](#common-table-expressions-ctes) (CTEs), or [table function calls](/sql/functions/table-functions).
_join&lowbar;expr_ | A join expression; for more details, see the [`JOIN` documentation](/sql/select/join/).
**WHERE** _expression_ | Filter tuples by _expression_.
**GROUP BY** _col&lowbar;ref_ | Group aggregations by _col&lowbar;ref_.
**OPTIONS (** _hint&lowbar;list_ **)** | Specify one or more [query hints](#query-hints).
**HAVING** _expression_ | Filter aggregations by _expression_.
**ORDER BY** _col&lowbar;ref_... | Sort results in either **ASC** or **DESC** order (_default: **ASC**_).<br/><br/>Use the **NULLS FIRST** and **NULLS LAST** options to determine whether nulls appear before or after non-null values in the sort ordering _(default: **NULLS LAST** for **ASC**, **NULLS FIRST** for **DESC**)_.<br/><br>
**LIMIT** _expression_ | Limit the number of returned results to _expression_.
**OFFSET** _integer_ | Skip the first _integer_ number of rows.
**UNION** | Records present in `select_stmt` or `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the sum of the times it occurs in each input statement.
**INTERSECT** | Records present in both `select_stmt` and `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the lesser of the times it occurs in each input statement.
**EXCEPT** | Records present in `select_stmt` but not in `another_select_stmt`.<br/><br/>**DISTINCT** returns only unique rows from these results _(implied default)_.<br/><br/>With **ALL** specified, each record occurs a number of times equal to the times it occurs in `select_stmt` less the times it occurs in `another_select_stmt`, or not at all if the former is greater than latter.

## Details

Because Materialize works very differently from a traditional RDBMS, it's
important to understand the implications that certain features of `SELECT` will
have on Materialize.

### Creating materialized views

Creating a [materialized view](/sql/create-materialized-view) generates a persistent dataflow, which has a
different performance profile from performing a `SELECT` in an RDBMS.

A materialized view has resource and latency costs that should
be carefully considered depending on its main usage. Materialize must maintain
the results of the query in durable storage, but often it must also maintain
additional intermediate state.

### Creating indexes

Creating an [index](/sql/create-index) also generates a persistent dataflow. The difference from a materialized view is that the results are maintained in memory rather than on persistent storage. This allows ad hoc queries to perform efficient point-lookups in indexes.

### Ad hoc queries

An ad hoc query (a.k.a. one-off `SELECT`) simply performs the query once and returns the results. Ad hoc queries can either read from an existing index, or they can start an ephemeral dataflow to compute the results.

Performing a `SELECT` on an **indexed** source, view or materialized view is
Materialize's ideal operation. When Materialize receives such a `SELECT` query,
it quickly returns the maintained results from memory.
Materialize also quickly returns results for queries that only filter, project, transform with scalar functions,
and re-order data that is maintained by an index.

Queries that can't simply read out from an index will create an ephemeral dataflow to compute
the results. These dataflows are bound to the active [cluster](/concepts/clusters/),
 which you can change using:

```mzsql
SET cluster = <cluster name>;
```

Materialize will remove the dataflow as soon as it has returned the query results to you.

### Common table expressions (CTEs)

Common table expressions, also known as CTEs or `WITH` queries, create aliases for statements.

#### Regular CTEs

{{< diagram "with-ctes.svg" >}}

##### cte_binding

{{< diagram "cte-binding.svg" >}}

With _regular CTEs_, any `cte_ident` alias can be referenced in subsequent `cte_binding` definitions and in the final `select_stmt`.
Regular CTEs can enhance legibility of complex queries, but doesn't alter the queries' semantics.
For an example, see [Using regular CTEs](#using-regular-ctes).

#### Recursive CTEs


In addition, Materialize also provides support for _recursive CTEs_ that can mutually reference each other.
Recursive CTEs can be used to define computations on recursively defined structures (such as trees or graphs) implied by your data.
For details and examples, see the [Recursive CTEs](/sql/select/recursive-ctes)
page.

#### Known limitations

CTEs have the following limitations, which we are working to improve:

- `INSERT`/`UPDATE`/`DELETE` (with `RETURNING`) is not supported inside a CTE.
- SQL99-compliant `WITH RECURSIVE` CTEs are not supported (use the [non-standard flavor](/sql/select/recursive-ctes) instead).

### Query hints

Users can specify query hints to help Materialize optimize queries.

The following query hints are valid within the `OPTIONS` clause.

Hint | Value type | Description
------|------------|------------
`AGGREGATE INPUT GROUP SIZE` | `uint8` | How many rows will have the same group key in an aggregation. Materialize can render `min` and `max` expressions more efficiently with this information.
`DISTINCT ON INPUT GROUP SIZE` | `uint8` | How many rows will have the same group key in a `DISTINCT ON` expression. Materialize can render [Top K patterns](/transform-data/idiomatic-materialize-sql/top-k/) based on `DISTINCT ON` more efficiently with this information. To determine the query hint size, see [`EXPLAIN ANALYZE HINTS`](/sql/explain-analyze/#explain-analyze-hints).
`LIMIT INPUT GROUP SIZE` | `uint8` | How many rows will be given as a group to a `LIMIT` restriction. Materialize can render [Top K patterns](/transform-data/idiomatic-materialize-sql/top-k/) based on `LIMIT` more efficiently with this information.

For examples, see the [Optimization](/transform-data/optimization/#query-hints) page.

### Column references

Within a given `SELECT` statement, we refer to the columns from the tables in
the `FROM` clause as the **input columns**, and columns in the `SELECT` list as
the **output columns**.

Expressions in the `SELECT` list, `WHERE` clause, and `HAVING` clause may refer
only to input columns.

Column references in the `ORDER BY` and `DISTINCT ON` clauses may be the name of
an output column, the ordinal number of an output column, or an arbitrary
expression of only input columns. If an unqualified name refers to both an input
and output column, `ORDER BY` chooses the output column.

Column references in the `GROUP BY` clause may be the name of an output column,
the ordinal number of an output column, or an arbitrary expression of only input
columns. If an unqualified name refers to both an input and output column,
`GROUP BY` chooses the input column.

### Connection pooling

Because Materialize is wire-compatible with PostgreSQL, you can use any
PostgreSQL connection pooler with Materialize. For example in using PgBouncer,
see [Connection Pooling](/integrations/connection-pooling).

## Examples

### Creating an indexed view

This assumes you've already [created a source](../create-source).

The following query creates a view representing the total of all
purchases made by users per region, and then creates an index on this view.

```mzsql
CREATE VIEW purchases_by_region AS
    SELECT region.id, sum(purchase.total)
    FROM mysql_simple_purchase AS purchase
    JOIN mysql_simple_user AS user ON purchase.user_id = user.id
    JOIN mysql_simple_region AS region ON user.region_id = region.id
    GROUP BY region.id;

CREATE INDEX purchases_by_region_idx ON purchases_by_region(id);
```

In this case, Materialize will create a dataflow to maintain the results of
this query, and that dataflow will live on until the index it's maintaining is
dropped.

### Reading from a view

Assuming you've created the indexed view listed above, named `purchases_by_region`, you can simply read from the index with an ad hoc `SELECT` query:

```mzsql
SELECT * FROM purchases_by_region;
```

In this case, Materialize simply returns the results that the index is maintaining, by reading from memory.

### Ad hoc querying

```mzsql
SELECT region.id, sum(purchase.total)
FROM mysql_simple_purchase AS purchase
JOIN mysql_simple_user AS user ON purchase.user_id = user.id
JOIN mysql_simple_region AS region ON user.region_id = region.id
GROUP BY region.id;
```

In this case, Materialize will spin up a similar dataflow as it did for creating
the above indexed view, but it will tear down the dataflow once it's returned its
results to the client. If you regularly want to view the results of this query,
you may want to create an [index](/sql/create-index) (in memory) and/or a [materialized view](/sql/create-materialized-view) (on persistent storage) for it.

### Using regular CTEs

```mzsql
WITH
  regional_sales (region, total_sales) AS (
    SELECT region, sum(amount)
    FROM orders
    GROUP BY region
  ),
  top_regions AS (
    SELECT region
    FROM regional_sales
    ORDER BY total_sales DESC
    LIMIT 5
  )
SELECT region,
       product,
       SUM(quantity) AS product_units,
       SUM(amount) AS product_sales
FROM orders
WHERE region IN (SELECT region FROM top_regions)
GROUP BY region, product;
```

Both `regional_sales` and `top_regions` are CTEs. You could write a query that
produces the same results by replacing references to the CTE with the query it
names, but the CTEs make the entire query simpler to understand.

With regard to dataflows, this is similar to [ad hoc querying](#ad-hoc-querying)
above: Materialize tears down the created dataflow after returning the results.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/select.md" >}}

## Related pages

- [`CREATE VIEW`](../create-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW FULL VIEWS`](../show-views)




---

## SET


`SET` modifies the value of a configuration parameter for the current session.
By default, values are set for the duration of the current session.

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

```mzsql
SET [SESSION|LOCAL] <config> TO|= <value|DEFAULT>;
```

Syntax element                | Description
------------------------------|------------
**SESSION**               | Optional. Set the value for the duration of the current session. **_(Default)_**
**LOCAL**                 | Optional. If specified, set the value for the duration of a single transaction.
`<config>`                | The name of the configuration parameter to modify.
`<value>`                 | The value to assign to the parameter.
**DEFAULT**               | Use the parameter's default value. Equivalent to [`RESET`](../reset).

{{% configuration-parameters %}}

### Aliased configuration parameters

There are a few configuration parameters that act as aliases for other
configuration parameters.

- `schema`: `schema` is an alias for `search_path`. Only one schema can be specified using this syntax. The `TO` and `=` syntax are optional.
- `names`: `names` is an alias for `client_encoding`. The `TO` and `=` syntax must be omitted.
- `time zone`: `time zone` is an alias for `timezone`. The `TO` and `=` syntax must be omitted.

## Examples

### Set active cluster

```mzsql
SHOW cluster;

 cluster
---------
 default

SET cluster = 'quickstart';

SHOW cluster;

  cluster
------------
 quickstart
```

### Set transaction isolation level

```mzsql
SET transaction_isolation = 'serializable';
```

### Set search path

```mzsql
SET search_path = public, qck;
```

```mzsql
SET schema = qck;
```

## Related pages

- [`RESET`](../reset)
- [`SHOW`](../show)




---

## SHOW


`SHOW` displays the value of either a specified configuration parameter or all
configuration parameters.

## Syntax

```sql
SHOW [ <name> | ALL ];
```

### Aliased configuration parameters

There are a few configuration parameters that act as aliases for other
configuration parameters.

- `schema`: an alias for showing the first resolvable schema in `search_path`
- `time zone`: an alias for `timezone`

{{% configuration-parameters %}}

## Examples

### Show active cluster

```mzsql
SHOW cluster;
```
```
 cluster
---------
 quickstart
```

### Show transaction isolation level

```mzsql
SHOW transaction_isolation;
```
```
 transaction_isolation
-----------------------
 strict serializable
```

## Related pages

- [`RESET`](../reset)
- [`SET`](../set)




---

## SHOW CLUSTER REPLICAS


`SHOW CLUSTER REPLICAS` lists the [replicas](/sql/create-cluster#replication-factor) for each
cluster configured in Materialize.

## Syntax

```sql
SHOW CLUSTER REPLICAS
[LIKE <pattern> | WHERE <condition(s)>]
;
```

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show clusters that match the pattern.
**WHERE** <condition(s)>      | If specified, only show clusters that match the condition(s).

## Examples

```mzsql
SHOW CLUSTER REPLICAS;
```

```nofmt
    cluster    | replica |  size  | ready |
---------------+---------|--------|-------|
 auction_house | bigger  | 1600cc | t     |
 quickstart    | r1      | 25cc   | t     |
```

```mzsql
SHOW CLUSTER REPLICAS WHERE cluster = 'quickstart';
```

```nofmt
    cluster    | replica |  size  | ready|
---------------+---------|--------|-------
 quickstart    | r1      | 25cc   | t    |
```


## Related pages

- [`CREATE CLUSTER REPLICA`](../create-cluster-replica)
- [`DROP CLUSTER REPLICA`](../drop-cluster-replica)




---

## SHOW CLUSTERS


`SHOW CLUSTERS` lists the [clusters](/concepts/clusters/) configured in Materialize.

## Syntax

```sql
SHOW CLUSTERS
[LIKE <pattern> | WHERE <condition(s)>]
;
```

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show clusters that match the pattern.
**WHERE** <condition(s)>      | If specified, only show clusters that match the condition(s).

## Pre-installed clusters

When you enable a Materialize region, several clusters that are used to improve
the user experience, as well as support system administration tasks, will be
pre-installed.

### `quickstart` cluster

A cluster named `quickstart` with a size of `25cc` and a replication factor of
`1` will be pre-installed in every environment. You can modify or drop this
cluster at any time.

{{< note >}}
The default value for the `cluster` session parameter is `quickstart`.
If the `quickstart` cluster is dropped, you must run [`SET cluster`](/sql/select/#ad-hoc-queries)
to choose a valid cluster in order to run `SELECT` queries. A _superuser_ (i.e. `Organization Admin`)
can also run [`ALTER SYSTEM SET cluster`](/sql/alter-system-set) to change the
default value.
{{< /note >}}

### `mz_catalog_server` system cluster

A system cluster named `mz_catalog_server` will be pre-installed in every
environment. This cluster has several indexes installed to speed up `SHOW`
commands and queries using the system catalog.

To take advantage of these indexes, Materialize will automatically re-route
`SHOW` commands and queries using system catalog objects to the
`mz_catalog_server` system cluster. You can disable this behavior in
your session via the `auto_route_catalog_queries`
[configuration parameter](/sql/show/#other-configuration-parameters).

The following characteristics apply to the `mz_catalog_server` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You can run `SELECT` or `SUBSCRIBE` queries in this cluster as long
    as you only reference objects in the [system catalog](/sql/system-catalog/).

### `mz_probe` system cluster

A system cluster named `mz_probe` will be pre-installed in every environment.
This cluster is used for internal uptime monitoring.

The following characteristics apply to the `mz_probe` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` queries in this cluster.

### `mz_support` system cluster

A system cluster named `mz_support` will be pre-installed in every environment.
This cluster is used for internal support tasks.

The following characteristics apply to the `mz_support` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` queries in this cluster.

### `mz_system` system cluster

A system cluster named `mz_system` will be pre-installed in every environment.
This cluster is used for internal system jobs.

The following characteristics apply to the `mz_system` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` queries in this cluster.


## Examples

```mzsql
SET CLUSTER = mz_catalog_server;

SHOW CLUSTERS;
```

```nofmt
       name                  replicas
--------------------- | ------------------
 default              |  r1 (25cc)
 auction_house        |  r1 (25cc)
 mz_catalog_server    |  r1 (50cc)
 mz_system            |  r1 (50cc)
 mz_probe             |  r1 (mz_probe)
 mz_support           |
```

```mzsql
SHOW CLUSTERS LIKE 'auction_%';
```

```nofmt
      name                  replicas
--------------------- | ------------------
 auction_house        |  r1 (25cc)
```


## Related pages

- [`CREATE CLUSTER`](../create-cluster)
- [`DROP CLUSTER`](../drop-cluster)




---

## SHOW COLUMNS


`SHOW COLUMNS` lists the columns available for an object. This can be a source,
subsource, materialized view, view, or table.

## Syntax

```sql
SHOW COLUMNS FROM <object_name>
[LIKE <pattern> | WHERE <condition(s)>]
;
```

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show columns that match the pattern.
**WHERE** <condition(s)>      | If specified, only show columns that match the condition(s).

## Details

### Output format

`SHOW COLUMNS`'s output is a table, with this structure:

```nofmt
+---------+------------+--------+
| name    | nullable   | type   |
|---------+------------+--------|
| ...     | ...        | ...    |
+---------+------------+--------+
```

Field | Meaning
------|--------
**name** | The name of the column
**nullable** | Does the column accept `null` values?
**type** | The column's [type](../types)

Rows are sorted by the order in which the fields are defined in the targeted
object.

## Examples

```mzsql
SHOW SOURCES;
```
```nofmt
   name
----------
my_sources
```
```mzsql
SHOW COLUMNS FROM my_source;
```
```nofmt
  name  | nullable | type
---------+----------+------
 column1 | f       | int4
 column2 | f       | text
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/show-columns.md" >}}

## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)




---

## SHOW CONNECTIONS


`SHOW CONNECTIONS` lists the connections configured in Materialize.

## Syntax

```sql
SHOW CONNECTIONS
[FROM <schema_name>]
[LIKE <pattern> | WHERE <condition(s)>]
;
```

Syntax element                | Description
------------------------------|------------
**FROM** \<schema_name\>      | If specified, only show connections from the specified schema. For available schema names, see [`SHOW SCHEMAS`](/sql/show-schemas).
**LIKE** \<pattern\>          | If specified, only show connections that match the pattern.
**WHERE** <condition(s)>      | If specified, only show connections that match the condition(s).

## Examples

```mzsql
SHOW CONNECTIONS;
```

```nofmt
       name          | type
---------------------+---------
 kafka_connection    | kafka
 postgres_connection | postgres
```

```mzsql
SHOW CONNECTIONS LIKE 'kafka%';
```

```nofmt
       name       | type
------------------+------
 kafka_connection | kafka
```


## Related pages

- [`CREATE CONNECTION`](../create-connection)
- [`DROP CONNECTION`](../drop-connection)




---

## SHOW CREATE CLUSTER


`SHOW CREATE CLUSTER` returns the DDL statement used to create the cluster.

## Syntax

```sql
SHOW CREATE CLUSTER <cluster_name>
```

For available cluster names, see [`SHOW CLUSTERS`](/sql/show-clusters).

## Examples

```sql
SHOW CREATE CLUSTER c;
```

```nofmt
    name          |    create_sql
------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 c                | CREATE CLUSTER "c" (INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 1, SIZE = '100cc', SCHEDULE = MANUAL)
```

## Privileges

{{< include-md
file="shared-content/sql-command-privileges/show-create-cluster.md" >}}

## Related pages

- [`SHOW CLUSTERS`](../show-clusters)
- [`CREATE CLUSTER`](../create-cluster)




---

## SHOW CREATE CONNECTION


`SHOW CREATE CONNECTION` returns the DDL statement used to create the connection.

## Syntax

```sql
SHOW [REDACTED] CREATE CONNECTION <connection_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available connection names, see [`SHOW CONNECTIONS`](/sql/show-connections).

## Examples

```mzsql
SHOW CREATE CONNECTION kafka_connection;
```

```nofmt
    name          |    create_sql
------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 kafka_connection | CREATE CONNECTION "materialize"."public"."kafka_connection" TO KAFKA (BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092', SASL MECHANISMS = 'PLAIN', SASL USERNAME = SECRET sasl_username, SASL PASSWORD = SECRET sasl_password)
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/show-create-connection.md" >}}

## Related pages

- [`SHOW CONNECTIONS`](../show-sources)
- [`CREATE CONNECTION`](../create-connection)




---

## SHOW CREATE INDEX


`SHOW CREATE INDEX` returns the DDL statement used to create the index.

## Syntax

```sql
SHOW [REDACTED] CREATE INDEX <index_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available index names, see [`SHOW INDEXES`](/sql/show-indexes).

## Examples

```mzsql
SHOW INDEXES FROM my_view;
```

```nofmt
     name    | on  | cluster    | key
-------------+-----+------------+--------------------------------------------
 my_view_idx | t   | quickstart | {a, b}
```

```mzsql
SHOW CREATE INDEX my_view_idx;
```

```nofmt
              name              |                                           create_sql
--------------------------------+------------------------------------------------------------------------------------------------
 materialize.public.my_view_idx | CREATE INDEX "my_view_idx" IN CLUSTER "default" ON "materialize"."public"."my_view" ("a", "b")
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/show-create-index.md"
>}}

## Related pages

- [`SHOW INDEXES`](../show-indexes)
- [`CREATE INDEX`](../create-index)




---

## SHOW CREATE MATERIALIZED VIEW


`SHOW CREATE MATERIALIZED VIEW` returns the DDL statement used to create the materialized view.

## Syntax

```sql
SHOW [REDACTED] CREATE MATERIALIZED VIEW <view_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available materialized view names, see [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views).

## Examples

```mzsql
SHOW CREATE MATERIALIZED VIEW winning_bids;
```
```nofmt
              name               |                                                                                                                       create_sql
---------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.winning_bids | CREATE MATERIALIZED VIEW "materialize"."public"."winning_bids" IN CLUSTER "quickstart" AS SELECT * FROM "materialize"."public"."highest_bid_per_auction" WHERE "end_time" < "mz_catalog"."mz_now"()
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/show-create-materialized-view.md"
>}}

## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)




---

## SHOW CREATE SINK


`SHOW CREATE SINK` returns the DDL statement used to create the sink.

## Syntax

```sql
SHOW [REDACTED] CREATE SINK <sink_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available sink names, see [`SHOW SINKS`](/sql/show-sinks).

## Examples

```mzsql
SHOW SINKS
```

```nofmt
     name
--------------
 my_view_sink
```

```mzsql
SHOW CREATE SINK my_view_sink;
```

```nofmt
               name              |                                                                                                        create_sql
---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.my_view_sink | CREATE SINK "materialize"."public"."my_view_sink" IN CLUSTER "c" FROM "materialize"."public"."my_view" INTO KAFKA CONNECTION "materialize"."public"."kafka_conn" (TOPIC 'my_view_sink') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection ENVELOPE DEBEZIUM
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/show-create-sink.md"
>}}

## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)




---

## SHOW CREATE SOURCE


`SHOW CREATE SOURCE` returns the DDL statement used to create the source.

## Syntax

```sql
SHOW [REDACTED] CREATE SOURCE <source_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available source names, see [`SHOW SOURCES`](/sql/show-sources).

## Examples

```mzsql
SHOW CREATE SOURCE market_orders_raw;
```

```nofmt
                 name                 |                                      create_sql
--------------------------------------+--------------------------------------------------------------------------------------------------------------
 materialize.public.market_orders_raw | CREATE SOURCE "materialize"."public"."market_orders_raw" IN CLUSTER "c" FROM LOAD GENERATOR COUNTER
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/show-create-source.md" >}}

## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`CREATE SOURCE`](../create-source)




---

## SHOW CREATE TABLE


`SHOW CREATE TABLE` returns the SQL used to create the table.

## Syntax

```sql
SHOW [REDACTED] CREATE TABLE <table_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available table names, see [`SHOW TABLES`](/sql/show-tables).

## Examples

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
```

```mzsql
SHOW CREATE TABLE t;
```
```nofmt
         name         |                                             create_sql
----------------------+-----------------------------------------------------------------------------------------------------
 materialize.public.t | CREATE TABLE "materialize"."public"."t" ("a" "pg_catalog"."int4", "b" "pg_catalog"."text" NOT NULL)
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/show-create-table.md"
>}}

## Related pages

- [`SHOW TABLES`](../show-tables)
- [`CREATE TABLE`](../create-table)




---

## SHOW CREATE TYPE


`SHOW CREATE TYPE` returns the DDL statement used to create the custom type.

## Syntax

```sql
SHOW [REDACTED] CREATE TYPE <type_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available type names names, see [`SHOW TYPES`](/sql/show-types).

## Examples

```sql
SHOW CREATE TYPE point;

```

```nofmt
    name          |    create_sql
------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 point            | CREATE TYPE materialize.public.point AS (x pg_catalog.int4, y pg_catalog.int4);
```

## Privileges

{{< include-md
file="shared-content/sql-command-privileges/show-create-type.md" >}}

## Related pages

- [`SHOW TYPES`](../show-types)
- [`CREATE TYPE`](../create-type)




---

## SHOW CREATE VIEW


`SHOW CREATE VIEW` returns the [`SELECT`](../select) statement used to create the view.

## Syntax

```sql
SHOW [REDACTED] CREATE VIEW <view_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available view names, see [`SHOW VIEWS`](/sql/show-views).

## Examples

```mzsql
SHOW CREATE VIEW my_view;
```
```nofmt
            name            |                                            create_sql
----------------------------+--------------------------------------------------------------------------------------------------
 materialize.public.my_view | CREATE VIEW "materialize"."public"."my_view" AS SELECT * FROM "materialize"."public"."my_source"
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/show-create-view.md"
>}}

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`CREATE VIEW`](../create-view)




---

## SHOW DATABASES


`SHOW DATABASES` returns a list of all databases in Materialize.

## Syntax

```sql
SHOW DATABASES
[LIKE <pattern> | WHERE <condition(s)>]
;
```

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>          | If specified, only show databases that match the pattern.
**WHERE** <condition(s)>      | If specified, only show databases that match the condition(s).

## Details

### Output format

`SHOW DATABASES`'s output is a table with one column, `name`.

## Examples

```mzsql
CREATE DATABASE my_db;
```
```mzsql
SHOW DATABASES;
```
```nofmt
materialize
my_db
```




---

## SHOW DEFAULT PRIVILEGES


`SHOW DEFAULT PRIVILEGES` lists the default privileges granted on objects in Materialize.

## Syntax

```sql
SHOW DEFAULT PRIVILEGES [ON <object_type>] [FOR <role_name>];
```


Syntax element               | Description
-----------------------------|--------------------------------------
**ON** <object_type>         | If specified, only show default privileges for the specified object type. Accepted object types: <div style="display: flex;"> <ul style="margin-right: 20px;"> <li><strong>CLUSTERS</strong></li> <li><strong>CONNECTION</strong></li> <li><strong>DATABASES</strong></li> <li><strong>SCHEMAS</strong></li> </ul> <ul> <li><strong>SECRETS</strong></li> <li><strong>TABLES</strong></li> <li><strong>TYPES</strong></li> </ul> </div>
**FOR** <role_name>          | If specified, only show default privileges granted directly or indirectly to the specified role. For available role names, see [`SHOW ROLES`](/sql/show-roles).

[//]: # "TODO(morsapaes) Improve examples."

## Examples

```mzsql
SHOW DEFAULT PRIVILEGES;
```

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | cluster     | interns | USAGE
 PUBLIC       |          |        | schema      | mike    | CREATE
 PUBLIC       |          |        | type        | PUBLIC  | USAGE
 mike         |          |        | table       | joe     | SELECT
```

```mzsql
SHOW DEFAULT PRIVILEGES ON SCHEMAS;
```

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | schema      | mike    | CREATE
```

```mzsql
SHOW DEFAULT PRIVILEGES FOR joe;
```

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | cluster     | interns | USAGE
 PUBLIC       |          |        | type        | PUBLIC  | USAGE
 mike         |          |        | table       | joe     | SELECT
```

## Related pages

- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)




---

## SHOW INDEXES


`SHOW INDEXES` provides details about indexes built on a source, view, or materialized view.

## Syntax

```mzsql
SHOW INDEXES [ FROM <schema_name> | ON <object_name> ]
[ IN CLUSTER <cluster_name> ]
[ LIKE <pattern> | WHERE <condition(s)> ]
;
```

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show indexes from the specified schema. Defaults to first resolvable schema in the search path if neither `ON <object_name>` nor `IN CLUSTER <cluster_name>` are specified. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**ON** <object_name>          | If specified, only show indexes for the specified object.
**IN CLUSTER** <cluster_name> | If specified, only show indexes from the specified cluster.
**LIKE** \<pattern\>          | If specified, only show indexes that match the pattern.
**WHERE** <condition(s)>      | If specified, only show indexes that match the condition(s).

## Details

### Output format

`SHOW INDEX`'s output is a table with the following structure:

```nofmt
name | on  | cluster | key
-----+-----+---------+----
 ... | ... | ...     | ...
```

Field | Meaning
------|--------
**name** | The name of the index.
**on** | The name of the table, source, or view the index belongs to.
**cluster** | The name of the [cluster](/concepts/clusters/) containing the index.
**key** | A text array describing the expressions in the index key.

## Examples

```mzsql
SHOW VIEWS;
```
```nofmt
          name
-------------------------
 my_nonmaterialized_view
 my_materialized_view
```

```mzsql
SHOW INDEXES ON my_materialized_view;
```
```nofmt
 name | on  | cluster | key
------+-----+---------+----
 ...  | ... | ...     | ...
```

## Related pages

- [`SHOW CREATE INDEX`](../show-create-index)
- [`DROP INDEX`](../drop-index)




---

## SHOW MATERIALIZED VIEWS


`SHOW MATERIALIZED VIEWS` returns a list of materialized views being maintained
in Materialize.

## Syntax

```mzsql
SHOW MATERIALIZED VIEWS [ FROM <schema_name> ] [ IN <cluster_name> ];
```

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>      | If specified, only show materialized views from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**IN** <cluster_name>       | If specified, only show materialized views from the specified cluster.

## Examples

```mzsql
SHOW MATERIALIZED VIEWS;
```

```nofmt
     name     | cluster
--------------+----------
 winning_bids | quickstart
```

```mzsql
SHOW MATERIALIZED VIEWS LIKE '%bid%';
```

```nofmt
     name     | cluster
--------------+----------
 winning_bids | quickstart
```

## Related pages

- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)




---

## SHOW NETWORK POLICIES (Cloud)


*Available for Materialize Cloud only*

`SHOW NETWORK POLICIES` returns a list of all network policies configured in
Materialize. Network policies are part of Materialize's framework for
[access control](/security/cloud/).

## Syntax

```mzsql
SHOW NETWORK POLICIES [ LIKE <pattern> ];
```

Syntax element                | Description
------------------------------|------------
**LIKE** \<pattern\>       | If specified, only show network policies whose name matches the pattern.

## Pre-installed network policy

When you enable a Materialize region, a default network policy named `default`
will be pre-installed. This policy has a wide open ingress rule `allow
0.0.0.0/0`. You can modify or drop this network policy at any time.

{{< note >}}
The default value for the `network_policy` session parameter is `default`.
Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
change the default value.
{{< /note >}}

## Examples

```mzsql
SHOW NETWORK POLICIES;
```
```nofmt
| name                 | rules              | comment |
| -------------------- | ------------------ | ------- |
| default              | open_ingress       |         |
| office_access_policy | minnesota,new_york |         |
```

To see details for each rule in a network policy, you can query the
[`mz_internal.mz_network_policy_rules`](/sql/system-catalog/mz_internal/#mz_network_policy_rules)
system catalog table.

```mzsql
SELECT * FROM mz_internal.mz_network_policy_rules;
```
```nofmt
| name         | policy_id | action | address    | direction |
| ------------ | --------- | ------ | ---------- | --------- |
| new_york     | u3        | allow  | 1.2.3.4/28 | ingress   |
| minnesota    | u3        | allow  | 2.3.4.5/32 | ingress   |
| open_ingress | u1        | allow  | 0.0.0.0/0  | ingress   |
```

## Related pages

- [`CREATE NETWORK POLICY`](../create-network-policy)
- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`DROP NETWORK POLICY`](../drop-network-policy)




---

## SHOW OBJECTS


`SHOW OBJECTS` returns a list of all objects in Materialize for a given schema.
Objects include tables, sources, sinks, views, materialized views, indexes,
secrets and connections.

## Syntax

```mzsql
SHOW OBJECTS [ FROM <schema_name> ];
```

Syntax element               | Description
-----------------------------|------------
**FROM** <schema_name>       | If specified, only show objects from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Output format

`SHOW OBJECTS` will output a table with two columns, `name`and `type`.

## Examples

```mzsql
SHOW SCHEMAS;
```
```nofmt
  name
--------
 public
```
```mzsql
SHOW OBJECTS FROM public;
```
```nofmt
  name          | type
----------------+-------
my_table        | table
my_source       | source
my_view         | view
my_other_source | source
```
```mzsql
SHOW OBJECTS;
```
```nofmt
  name    | type
----------+-------
my_table  | table
my_source | source
my_view   | view
```

## Related pages

- [`SHOW TABLES`](../show-tables)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW SINKS`](../show-sinks)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
- [`SHOW SECRETS`](../show-secrets)
- [`SHOW CONNECTIONS`](../show-connections)




---

## SHOW PRIVILEGES


`SHOW PRIVILEGES` lists the privileges granted on all objects via
[role-based access control](/security/) (RBAC).

## Syntax

```mzsql
SHOW PRIVILEGES [ ON <object_type> ] [ FOR <role_name> ];
```

Syntax element               | Description
-----------------------------|-----------------------------------------------
**ON** <object_type>         | If specified, only show privileges for the specified object type. Accepted object types: <div style="display: flex;"> <ul style="margin-right: 20px;"> <li><strong>CLUSTERS</strong></li> <li><strong>CONNECTION</strong></li> <li><strong>DATABASES</strong></li> <li><strong>SCHEMAS</strong></li> </ul> <ul> <li><strong>SECRETS</strong></li> <li><strong>SYSTEM</strong></li> <li><strong>TABLES</strong></li> <li><strong>TYPES</strong></li> </ul> </div>
**FOR** <role_name>          | If specified, only show privileges for the specified role.

[//]: # "TODO(morsapaes) Improve examples."

## Examples

```mzsql
SHOW PRIVILEGES;
```

```nofmt
  grantor  |   grantee   |  database   | schema |    name     | object_type | privilege_type
-----------+-------------+-------------+--------+-------------+-------------+----------------
 mz_system | PUBLIC      | materialize |        | public      | schema      | USAGE
 mz_system | PUBLIC      |             |        | quickstart  | cluster     | USAGE
 mz_system | PUBLIC      |             |        | materialize | database    | USAGE
 mz_system | materialize | materialize |        | public      | schema      | CREATE
 mz_system | materialize | materialize |        | public      | schema      | USAGE
 mz_system | materialize |             |        | quickstart  | cluster     | CREATE
 mz_system | materialize |             |        | quickstart  | cluster     | USAGE
 mz_system | materialize |             |        | materialize | database    | CREATE
 mz_system | materialize |             |        | materialize | database    | USAGE
 mz_system | materialize |             |        |             | system      | CREATECLUSTER
 mz_system | materialize |             |        |             | system      | CREATEDB
 mz_system | materialize |             |        |             | system      | CREATEROLE
```

```mzsql
SHOW PRIVILEGES ON SCHEMAS;
```

```nofmt
  grantor  |   grantee   |  database   | schema |  name  | object_type | privilege_type
-----------+-------------+-------------+--------+--------+-------------+----------------
 mz_system | PUBLIC      | materialize |        | public | schema      | USAGE
 mz_system | materialize | materialize |        | public | schema      | CREATE
 mz_system | materialize | materialize |        | public | schema      | USAGE
```

```mzsql
SHOW PRIVILEGES FOR materialize;
```

```nofmt
  grantor  |   grantee   |  database   | schema |    name     | object_type | privilege_type
-----------+-------------+-------------+--------+-------------+-------------+----------------
 mz_system | materialize | materialize |        | public      | schema      | CREATE
 mz_system | materialize | materialize |        | public      | schema      | USAGE
 mz_system | materialize |             |        | quickstart  | cluster     | CREATE
 mz_system | materialize |             |        | quickstart  | cluster     | USAGE
 mz_system | materialize |             |        | materialize | database    | CREATE
 mz_system | materialize |             |        | materialize | database    | USAGE
 mz_system | materialize |             |        |             | system      | CREATECLUSTER
 mz_system | materialize |             |        |             | system      | CREATEDB
 mz_system | materialize |             |        |             | system      | CREATEROLE
```

## Related pages

- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)




---

## SHOW ROLE MEMBERSHIP


`SHOW ROLE MEMBERSHIP` lists the members of each role granted (directly or
indirectly) via [role-based access
control](/security/) (RBAC).

## Syntax

```mzsql
SHOW ROLE MEMBERSHIP [ FOR <role_name> ];
```

Syntax element             | Description
---------------------------|------------
**FOR** <role_name>        | If specified, only show membership for the specified role.

[//]: # "TODO(morsapaes) Improve examples."

## Examples

```mzsql
SHOW ROLE MEMBERSHIP;
```

```nofmt
 role | member |  grantor
------+--------+-----------
 r2   | r1     | mz_system
 r3   | r2     | mz_system
 r4   | r3     | mz_system
 r6   | r5     | mz_system
```

```mzsql
SHOW ROLE MEMBERSHIP FOR r2;
```

```nofmt
 role | member |  grantor
------+--------+-----------
 r3   | r2     | mz_system
 r4   | r3     | mz_system
```

## Related pages

- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)




---

## SHOW ROLES


`SHOW ROLES` lists the roles available in Materialize.

## Syntax

```mzsql
SHOW ROLES [ LIKE <pattern>  | WHERE <condition(s)> ];
```

Syntax element             | Description
---------------------------|------------
**LIKE** \<pattern\>       | If specified, only show roles whose name matches the pattern.
**WHERE** <condition(s)>   | If specified, only show roles that meet the condition(s).

## Examples

```mzsql
SHOW ROLES;
```
```nofmt
 name
----------------
 joe@ko.sh
 mike@ko.sh
```

```mzsql
SHOW ROLES LIKE 'jo%';
```
```nofmt
 name
----------------
 joe@ko.sh
```

```mzsql
SHOW ROLES WHERE name = 'mike@ko.sh';
```
```nofmt
 name
----------------
 mike@ko.sh
```


## Related pages

- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)




---

## SHOW SCHEMAS


`SHOW SCHEMAS` returns a list of all schemas available in Materialize.

## Syntax

```mzsql
SHOW SCHEMAS [ FROM <database_name> ];
```

Syntax element                | Description
------------------------------|------------
**FROM** <database_name>      | If specified, only show schemas from the specified database. Defaults to the current database. For available databases, see [`SHOW DATABASES`](../show-databases).

## Details

### Output format

`SHOW SCHEMAS`'s output is a table with one column, `name`.

## Examples

```mzsql
SHOW DATABASES;
```
```nofmt
   name
-----------
materialize
my_db
```
```mzsql
SHOW SCHEMAS FROM my_db
```
```nofmt
  name
--------
 public
```

## Related pages

- [`CREATE SCHEMA`](../create-schema)
- [`DROP SCHEMA`](../drop-schema)




---

## SHOW SECRETS


`SHOW SECRETS` lists the names of the secrets securely stored in Materialize's
secret management system. There is no way to show the contents of an existing
secret, though you can override it using the [`ALTER SECRET`](../alter-secret)
statement.

## Syntax

```mzsql
SHOW SECRETS [ FROM <schema_name> ] [ LIKE <pattern>  | WHERE <condition(s)> ];
```

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show secrets from the specified schema.  Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**LIKE** \<pattern\>          | If specified, only show secrets whose name matches the pattern.
**WHERE** <condition(s)>      | If specified, only show secrets that meet the condition(s).

## Examples

```mzsql
SHOW SECRETS;
```

```nofmt
         name
-----------------------
 kafka_ca_cert
 kafka_sasl_password
 kafka_sasl_username
```

```mzsql
SHOW SECRETS FROM public LIKE '%cert%';
```

```nofmt
         name
-----------------------
 kafka_ca_cert
```

## Related pages

- [`CREATE SECRET`](../create-secret)
- [`ALTER SECRET`](../alter-secret)
- [`DROP SECRET`](../drop-secret)




---

## SHOW SINKS


`SHOW SINKS` returns a list of all sinks available in Materialize.

## Syntax

```mzsql
SHOW SINKS [ FROM <schema_name> ] [ IN CLUSTER <cluster_name> ];
```

## Details

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show sinks from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**IN CLUSTER** <cluster_name> | If specified, only show sinks from the specified cluster. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).

### Output format

`SHOW SINKS`'s output is a table, with this structure:

```nofmt
name  | type | cluster
------+------+--------
...   | ...  | ...
```

Field       | Meaning
------------|--------
**name**    | The name of the sink.
**type**    | The type of the sink: currently only `kafka` is supported.
**cluster** | The cluster the sink is associated with.

## Examples

```mzsql
SHOW SINKS;
```
```nofmt
name          | type  | cluster
--------------+-------+--------
my_sink       | kafka | c1
my_other_sink | kafka | c2
```

```mzsql
SHOW SINKS IN CLUSTER c1;
```
```nofmt
name    | type  | cluster
--------+-------+--------
my_sink | kafka | c1
```

## Related pages

- [`CREATE SINK`](../create-sink)
- [`DROP SINK`](../drop-sink)
- [`SHOW CREATE SINK`](../show-create-sink)




---

## SHOW SOURCES


`SHOW SOURCES` returns a list of all sources available in Materialize.

## Syntax

```mzsql
SHOW SOURCES [ FROM <schema_name> ] [ IN CLUSTER <cluster_name> ];
```

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show sources from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**IN CLUSTER** <cluster_name> | If specified, only show sources from the specified cluster. For available clusters, see [`SHOW CLUSTERS`](../show-clusters).

## Details

### Output format for `SHOW SOURCES`

`SHOW SOURCES`'s output is a table, with this structure:

```nofmt
name  | type | cluster
------+------+--------
...   | ...  | ...
```

Field | Meaning
------|--------
**name** | The name of the source.
**type** | The type of the source: `kafka`, `postgres`, `load-generator`, `progress`, or `subsource`.
**cluster** | The cluster the source is associated with.

## Examples

```mzsql
SHOW SOURCES;
```
```nofmt
            name    | type     | cluster
--------------------+----------+---------
 my_kafka_source    | kafka    | c1
 my_postgres_source | postgres | c2
```

```mzsql
SHOW SOURCES IN CLUSTER c2;
```
```nofmt
name               | type     | cluster
-------------------+----------+--------
my_postgres_source | postgres | c2
```

## Related pages

- [`CREATE SOURCE`](../create-source)
- [`DROP SOURCE`](../drop-source)
- [`SHOW CREATE SOURCE`](../show-create-source)




---

## SHOW SUBSOURCES


`SHOW SUBSOURCES` returns the subsources in the current schema.

## Syntax

```mzsql
SHOW SUBSOURCES [ FROM <schema_name> | ON <source_name> ];
```

Syntax element         | Description
-----------------------|------------
**FROM** <schema_name> | If specified, only show subsources from the specified schema. Defaults to first resolvable schema in the search path.
**ON** <source_name>   | If specified, only show subsources on the specified source.

## Details

A subsource is a relation associated with a source. There are two types of
subsources:

  * Subsources of type `progress` describe Materialize's ingestion progress for
    the parent source. Every source has exactly one subsource of type
    `progress`.

  * Subsources of type `subsource` are used by sources that need to ingest data
    into multiple tables, like PostgreSQL sources. For each upstream table that
    is selected for ingestion, Materialize creates a subsource of type
    `subsource`.

### Output format for `SHOW SUBSOURCES`

`SHOW SUBSOURCES`'s output is a table, with this structure:

```nofmt
 name  | type
-------+-----
 ...   | ...
```

Field    | Meaning
---------|--------
**name** | The name of the subsource.
**type** | The type of the subsource: `subsource` or `progress`.

## Examples

```mzsql
SHOW SOURCES;
```
```nofmt
    name
----------
 postgres
 kafka
```
```mzsql
SHOW SUBSOURCES ON pg;
```
```nofmt
        name        | type
--------------------+-----------
 postgres_progress  | progress
 table1_in_postgres | subsource
 table2_in_postgres | subsource
```
```mzsql
SHOW SUBSOURCES ON kafka;
```
```nofmt
            name    | typef
--------------------+----------
 kafka_progress     | progress
```

## Related pages

- [`SHOW CREATE SOURCE`](../show-create-source)
- [`CREATE SOURCE`](../create-source)




---

## SHOW TABLES


`SHOW TABLES` returns a list of all tables available in Materialize.

## Syntax

```mzsql
SHOW TABLES [FROM <schema_name>];
```

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show tables from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Output format

`SHOW TABLES`'s output is a table with one column, `name`.

## Examples

### Show user-created tables
```mzsql
SHOW TABLES;
```
```nofmt
 name
----------------
 my_table
 my_other_table
```

### Show tables from specified schema
```mzsql
SHOW SCHEMAS;
```
```nofmt
  name
--------
 public
```
```mzsql
SHOW TABLES FROM public;
```
```nofmt
 name
----------------
 my_table
 my_other_table
```

## Related pages

- [`SHOW CREATE TABLE`](../show-create-table)
- [`CREATE TABLE`](../create-table)




---

## SHOW TYPES


`SHOW TYPES` returns a list of the data types in Materialize. Only custom types
are returned.

## Syntax

```mzsql
SHOW TYPES [FROM <schema_name>];
```

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show types from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Examples

### Show custom data types

```mzsql
SHOW TYPES;
```
```
   name
-----------
 int4_list
```

## Related pages

* [`CREATE TYPE`](../create-type)
* [`DROP TYPE`](../drop-type)




---

## SHOW VIEWS


`SHOW VIEWS` returns a list of views in Materialize.

## Syntax

```mzsql
SHOW VIEWS [FROM <schema_name>];
```

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show views from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Output format for `SHOW VIEWS`

`SHOW VIEWS`'s output is a table, with this structure:

```nofmt
 name
-------
 ...
```

Field | Meaning
------|--------
**name** | The name of the view.

## Examples

```mzsql
SHOW VIEWS;
```
```nofmt
  name
---------
 my_view
```

## Related pages

- [`SHOW CREATE VIEW`](../show-create-view)
- [`CREATE VIEW`](../create-view)




---

## SQL data types


Materialize's type system consists of two classes of types:

- [Built-in types](#built-in-types)
- [Custom types](#custom-types) created through [`CREATE TYPE`][create-type]

## Built-in types

Type | Aliases | Use | Size (bytes) | Catalog name | Syntax
-----|-------|-----|--------------|----------------|-----
[`bigint`](integer) | `int8` | Large signed integer | 8 | Named | `123`
[`boolean`](boolean) | `bool` | State of `TRUE` or `FALSE` | 1 | Named | `TRUE`, `FALSE`
[`bytea`](bytea) | `bytea` | Unicode string | Variable | Named | `'\xDEADBEEF'` or `'\\000'`
[`date`](date) | | Date without a specified time | 4 | Named | `DATE '2007-02-01'`
[`double precision`](float) | `float`, `float8`, `double` | Double precision floating-point number | 8 | Named | `1.23`
[`integer`](integer) | `int`, `int4` | Signed integer | 4 | Named | `123`
[`interval`](interval) | | Duration of time | 32 | Named | `INTERVAL '1-2 3 4:5:6.7'`
[`jsonb`](jsonb) | `json` | JSON | Variable | Named | `'{"1":2,"3":4}'::jsonb`
[`map`](map) | | Map with [`text`](text) keys and a uniform value type | Variable | Anonymous | `'{a => 1, b => 2}'::map[text=>int]`
[`list`](list) | | Multidimensional list | Variable | Anonymous | `LIST[[1,2],[3]]`
[`numeric`](numeric) | `decimal` | Signed exact number with user-defined precision and scale | 16 | Named | `1.23`
[`oid`](oid) | | PostgreSQL object identifier | 4 | Named | `123`
[`real`](float) | `float4` | Single precision floating-point number | 4 | Named | `1.23`
[`record`](record) | | Tuple with arbitrary contents | Variable | Unnameable | `ROW($expr, ...)`
[`smallint`](integer) | `int2` | Small signed integer | 2 | Named | `123`
[`text`](text) | `string` | Unicode string | Variable | Named | `'foo'`
[`time`](time) | | Time without date | 4 | Named | `TIME '01:23:45'`
[`uint2`](uint) | | Small unsigned integer | 2 | Named | `123`
[`uint4`](uint) | | Unsigned integer | 4 | Named | `123`
[`uint8`](uint) | | Large unsigned integer | 8 | Named | `123`
[`timestamp`](timestamp) | | Date and time | 8 | Named | `TIMESTAMP '2007-02-01 15:04:05'`
[`timestamp with time zone`](timestamp) | `timestamp with time zone` | Date and time with timezone | 8 | Named | `TIMESTAMPTZ '2007-02-01 15:04:05+06'`
[Arrays](array) (`[]`) | | Multidimensional array | Variable | Named | `ARRAY[...]`
[`uuid`](uuid) | | UUID | 16 | Named | `UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`

#### Catalog name

Value | Description
------|------------
**Named** | Named types can be referred to using a qualified object name, i.e. they are objects within the `pg_catalog` schema. Each named type a unique OID.
**Anonymous** | Anonymous types cannot be referred to using a qualified object name, i.e. they do not exist as objects anywhere.<br/><br/>Anonymous types do not have unique OIDs for all of their possible permutations, e.g. `int4 list`, `float8 list`, and `date list list` share the same OID.<br/><br/>You can create named versions of some anonymous types using [custom types](#custom-types).
**Unnameable** | Unnameable types are anonymous and do not yet support being custom types.

## Custom types

Custom types, in general, provide a mechanism to create names for specific
instances of anonymous types to suit users' needs.

However, types are considered custom if the type:
- Was created through [`CREATE TYPE`][create-type].
- Contains a reference to a custom type.

To create custom types, see [`CREATE TYPE`][create-type].

### Use

Currently, custom types only provides a shorthand for referring to
otherwise-annoying-to-type names.

### Casts

Structurally equivalent types can be cast to and from one another; the required
context depends on the types themselves, though.

From | To | Cast permitted
-----|----|-----------------
Custom type | Built-in type | Implicitly
Built-in type | Custom type | Implicitly
Custom type 1 | Custom type 2 | [For explicit casts](../functions/cast/)

### Equality

Values in custom types are _never_ considered equal to:

- Other custom types, irrespective of their structure or value.
- Built-in types, but built-in types can be coerced to and from structurally
  equivalent custom types.

### Polymorphism

When using custom types as values for [polymorphic
functions](list/#polymorphism), the following additional constraints apply:

- If any value passed to a polymorphic parameter is a custom type, the resultant
  type must use the custom type in the appropriate location.

  For example, if a custom type is used as:
  - `listany`, the resultant `list` must be of exactly the same type.
  - `listelementany`, the resultant `list`'s element must be of the custom type.
- If custom types and built-in types are both used, the resultant type is the
  "least custom type" that can be derived––i.e. the resultant type will have the
  fewest possible layers of custom types that still fulfill all constraints.
  Materialize will neither create nor discover a custom type that fills the
  constraints, nor will it coerce a custom type to a built-in type.

  For example, if appending a custom `list` to a built-in `list list`, the
  resultant type will be a `list` of custom `list`s.

#### Examples

This is a little easier to understand if we make it concrete, so we'll focus on
concatenating two lists and appending an element to list.

For these operations, Materialize uses the following polymorphic parameters:

- `listany`, which accepts any `list`, and constrains all lists to being of the
  same structurally equivalent type.
- `listelementany`, which accepts any type, but must be equal to the element
  type of the `list` type used with `listany`. For instance, if `listany` is
  constrained to being `int4 list`, `listelementany` must be `int4`.

When concatenating two lists, we'll use `list_cat` whose signature is
`list_cat(l: listany, r: listany)`.

If we concatenate a custom `list` (in this example, `custom_list`) and a
structurally equivalent built-in `list` (`int4 list`), the result is of the same
type as the custom `list` (`custom_list`).

```mzsql
CREATE TYPE custom_list AS LIST (ELEMENT TYPE int4);

SELECT pg_typeof(
  list_cat('{1}'::custom_list, '{2}'::int4 list)
) AS custom_list_built_in_list_cat;

```
```nofmt
 custom_list_built_in_list_cat
-------------------------------
 custom_list
```

When appending an element to a list, we'll use `list_append` whose signature is
`list_append(l: listany, e: listelementany)`.

If we append a structurally appropriate element (`int4`) to a custom `list`
(`custom_list`), the result is of the same type as the custom `list`
(`custom_list`).

```mzsql
SELECT pg_typeof(
  list_append('{1}'::custom_list, 2)
) AS custom_list_built_in_element_cat;

```
```nofmt
 custom_list_built_in_element_cat
----------------------------------
 custom_list
```

If we append a structurally appropriate custom element (`custom_list`) to a
built-in `list` (`int4 list list`), the result is a `list` of custom elements.

```mzsql
SELECT pg_typeof(
  list_append('{{1}}'::int4 list list, '{2}'::custom_list)
) AS built_in_list_custom_element_append;

```
```nofmt
 built_in_list_custom_element_append
-------------------------------------
 custom_list list
```

This is the "least custom type" we could support for these values––i.e.
Materialize will not create or discover a custom type whose elements are
`custom_list`, nor will it coerce `custom_list` into an anonymous built-in
list.

Note that `custom_list list` is considered a custom type because it contains a
reference to a custom type. Because it's a custom type, it enforces custom
types' polymorphic constraints.

For example, values of type `custom_list list` and `custom_nested_list` cannot
both be used as `listany` values for the same function:

```mzsql
CREATE TYPE custom_nested_list AS LIST (element_type=custom_list);

SELECT list_cat(
  -- result is "custom_list list"
  list_append('{{1}}'::int4 list list, '{2}'::custom_list),
  -- result is custom_nested_list
  '{{3}}'::custom_nested_list
);
```
```nofmt
ERROR: Cannot call function list_cat(custom_list list, custom_nested_list)...
```

As another example, when using `custom_list list` values for `listany`
parameters, you can only use `custom_list` or `int4 list` values for
`listelementany` parameters––using any other custom type will fail:

```mzsql
CREATE TYPE second_custom_list AS LIST (element_type=int4);

SELECT list_append(
  -- elements are custom_list
  '{{1}}'::custom_nested_list,
  -- second_custom_list is not interoperable with custom_list because both
  -- are custom
  '{2}'::second_custom_list
);
```
```nofmt
ERROR:  Cannot call function list_append(custom_nested_list, second_custom_list)...
```

To make custom types interoperable, you must cast them to the same type. For
example, casting `custom_nested_list` to `custom_list list` (or vice versa)
makes the values passed to `listany` parameters of the same custom type:

```mzsql
SELECT pg_typeof(
  list_cat(
    -- result is "custom_list list"
    list_append(
      '{{1}}'::int4 list list,
      '{2}'::custom_list
    ),
    -- result is "custom_list list"
    '{{3}}'::custom_nested_list::custom_list list
  )
) AS complex_list_cat;
```
```nofmt
 complex_list_cat
------------------
 custom_list list
```

[create-type]: ../create-type




---

## SQL functions & operators


This page details Materialize's supported SQL [functions](#functions) and [operators](#operators).

## Functions

### Unmaterializable functions

Several functions in Materialize are **unmaterializable** because their output
depends upon state besides their input parameters, like the value of a session
parameter or the timestamp of the current transaction. You cannot create an
[index](/sql/create-index) or materialized view that depends on an
unmaterializable function, but you can use them in non-materialized views and
one-off [`SELECT`](/sql/select) statements.

Unmaterializable functions are marked as such in the table below.

### Side-effecting functions

Several functions in Materialize are **side-effecting** because their evaluation
changes system state. For example, the `pg_cancel_backend` function allows
canceling a query running on another connection.

Materialize offers only limited support for these functions. They may be called
only at the top level of a `SELECT` statement, like so:

```mzsql
SELECT side_effecting_function(arg, ...);
```

You cannot manipulate or alias the function call expression, call multiple
side-effecting functions in the same `SELECT` statement, nor add any additional
clauses to the `SELECT` statement (e.g., `FROM`, `WHERE`).

Side-effecting functions are marked as such in the table below.

{{% fnlist %}}

## Operators

### Generic operators

Operator | Computes
---------|---------
`val::type` | Cast of `val` as `type` ([docs](cast))

### Boolean operators

Operator | Computes
---------|---------
`AND` | Boolean "and"
`OR` | Boolean "or"
`=` | Equality
`<>` | Inequality
`!=` | Inequality
`<` | Less than
`>` | Greater than
`<=` | Less than or equal to
`>=` | Greater than or equal to
`a BETWEEN x AND y` | `a >= x AND a <= y`
`a NOT BETWEEN x AND y` | `a < x OR a > y`
`a IS NULL` | `a = NULL`
`a ISNULL` | `a = NULL`
`a IS NOT NULL` | `a != NULL`
`a IS TRUE` | `a` is true, requiring `a` to be a boolean
`a IS NOT TRUE` | `a` is not true, requiring `a` to be a boolean
`a IS FALSE` | `a` is false, requiring `a` to be a boolean
`a IS NOT FALSE` | `a` is not false, requiring `a` to be a boolean
`a IS UNKNOWN` | `a = NULL`, requiring `a` to be a boolean
`a IS NOT UNKNOWN` | `a != NULL`, requiring `a` to be a boolean
`a LIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`a ILIKE match_expr [ ESCAPE escape_char ]` | `a` matches `match_expr`, using case-insensitive [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)

### Numbers operators

Operator | Computes
---------|---------
`+` | Addition
`-` | Subtraction
`*` | Multiplication
`/` | Division
`%` | Modulo
`&` | Bitwise AND
<code>&vert;</code> | Bitwise OR
`#` | Bitwise XOR
`~` | Bitwise NOT
`<<`| Bitwise left shift
`>>`| Bitwise right shift

### String operators

Operator | Computes
---------|---------
<code>&vert;&vert;</code> | Concatenation
`~~` | Matches LIKE pattern case sensitively, see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`~~*` | Matches LIKE pattern case insensitively (ILIKE), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`!~~` | Matches NOT LIKE pattern (case sensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`!~~*` | Matches NOT ILIKE pattern (case insensitive), see [SQL LIKE matching](https://www.postgresql.org/docs/13/functions-matching.html#FUNCTIONS-LIKE)
`~` | Matches regular expression, case sensitive
`~*` | Matches regular expression, case insensitive
`!~` | Matches regular expression case sensitively, and inverts the match
`!~*` | Match regular expression case insensitively, and inverts the match

The regular expression syntax supported by Materialize is documented by the
[Rust `regex` crate](https://docs.rs/regex/*/#syntax).
The maximum length of a regular expression is 1 MiB in its raw form, and 10 MiB
after compiling it.

{{< warning >}}
Materialize regular expressions are similar to, but not identical to, PostgreSQL
regular expressions.
{{< /warning >}}

### Time-like operators

Operation | Computes
----------|------------
[`date`](../types/date) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `+` [`time`](../types/time) | [`timestamp`](../types/timestamp)
[`date`](../types/date) `-` [`date`](../types/date) | [`integer`](../types/integer)
[`timestamp`](../types/timestamp) `+` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`interval`](../types/interval) | [`timestamp`](../types/timestamp)
[`timestamp`](../types/timestamp) `-` [`timestamp`](../types/timestamp) | [`interval`](../types/interval)
[`time`](../types/time) `+` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`interval`](../types/interval) | `time`
[`time`](../types/time) `-` [`time`](../types/time) | [`interval`](../types/interval)

### JSON operators

{{% json-operators %}}

### Map operators

{{% map-operators %}}

### List operators

List operators are [polymorphic](../types/list/#polymorphism).

{{% list-operators %}}




---

## SUBSCRIBE


`SUBSCRIBE` streams updates from a source, table, view, or materialized view as
they occur.

## Conceptual framework

The `SUBSCRIBE` statement is a more general form of a [`SELECT`](/sql/select)
statement. While a `SELECT` statement computes a relation at a moment in time, a
subscribe operation computes how a relation *changes* over time.

Fundamentally, `SUBSCRIBE` produces a sequence of updates. An update describes either
the insertion or deletion of a row to the relation at a specific time. Taken
together, the updates describe the complete set of changes to a relation, in
order, while `SUBSCRIBE` is active.

You can use `SUBSCRIBE` to:

-   Power event processors that react to every change to a relation or an arbitrary `SELECT` statement.
-   Replicate the complete history of a relation while `SUBSCRIBE` is active.

## Syntax

```mzsql
SUBSCRIBE [TO] <object_name | (SELECT ...)>
[ENVELOPE UPSERT (KEY (<key1>, ...)) | ENVELOPE DEBEZIUM (KEY (<key1>, ...))]
[WITHIN TIMESTAMP ORDER BY <column1> [ASC | DESC] [NULLS LAST | NULLS FIRST], ...]
[WITH (<option_name> [= <option_value>], ...)]
[AS OF [AT LEAST] <timestamp_expression>]
[UP TO <timestamp_expression>]
;

```

where:

- `<object_name>` is the name of the source, table, view, or materialized view
  that you want to subscribe to.
- `<select_stmt>` is the [`SELECT` statement](/sql/select) whose output you want
  to subscribe to.

The generated schemas have a Debezium-style diff envelope to capture changes in
the input view or source.

| Syntax element                  | Description                                                                                                                                      |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **ENVELOPE UPSERT (KEY (**\<key1\>, ...**))**                | If specified, use the upsert envelope, which takes a list of `KEY` columns. The upsert envelope supports inserts, updates and deletes in the subscription output. For more information, see [Modifying the output format](#modifying-the-output-format). |
| **ENVELOPE DEBEZIUM (KEY (**\<key1\>, ...**))**           | If specified, use a [Debezium-style diff envelope](/sql/create-sink/kafka/#debezium-envelope), which takes a list of `KEY` columns. The Debezium envelope supports inserts, updates and deletes in the subscription output along with the previous state of the key. For more information, see [Modifying the output format](#modifying-the-output-format). |
| **WITHIN TIMESTAMP ORDER BY** \<column1\>, ... | If specified, use an `ORDER BY` clause to sort the subscription output within a timestamp. For each `ORDER BY` column, you can optionally specify: <ul><li> `ASC` or `DESC`</li><li> `NULLS FIRST` or `NULLS LAST`</li></ul> For more information, see [Modifying the output format](#modifying-the-output-format). |
| **WITH** \<option_name\> [= \<option_value\>] | If specified, use the specified option. For more information, see [`WITH` options](#with-options). |
| **AS OF** \<timestamp_expression\> | If specified, no rows whose timestamp is earlier than the specified timestamp will be returned. For more information, see [`AS OF`](#as-of). |
| **UP TO** \<timestamp_expression\> | If specified, no rows whose timestamp is greater than or equal to the specified timestamp will be returned. For more information, see [`UP TO`](#up-to). |


#### `WITH` options

The following options are valid within the `WITH` clause.

| Option name | Value type | Default | Describes                                                                                                                         |
| ----------- | ---------- | ------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `SNAPSHOT`  | `boolean`  | `true`  | Whether to emit a snapshot of the current state of the relation at the start of the operation. See [`SNAPSHOT`](#snapshot). |
| `PROGRESS`  | `boolean`  | `false` | Whether to include detailed progress information. See [`PROGRESS`](#progress).                                              |

## Details

### Output

`SUBSCRIBE` emits a sequence of updates as rows. Each row contains all of the
columns of the subscribed relation or derived from the `SELECT` statement, prepended
with several additional columns that describe the nature of the update:

<table>
<thead>
  <tr>
    <th>Column</th>
    <th>Type</th>
    <th>Represents</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td><code>mz_timestamp</code></td>
    <td><code>numeric</code></td>
    <td>
      Materialize's internal logical timestamp. This will never be less than any
      timestamp previously emitted by the same <code>SUBSCRIBE</code> operation.
    </td>
  </tr>
  <tr>
    <td><code>mz_progressed</code></td>
    <td><code>boolean</code></td>
    <td>
      <p>
        <em>
          This column is only present if the
          <a href="#progress"><code>PROGRESS</code></a> option is specified.
        </em>
      </p>
      If <code>true</code>, indicates that the <code>SUBSCRIBE</code> will not emit
      additional records at times strictly less than <code>mz_timestamp</code>. See
      <a href="#progress"><code>PROGRESS</code></a> below.
    </td>
  </tr>
  <tr>
    <td><code>mz_diff</code></td>
    <td><code>bigint</code></td>
    <td>
      The change in frequency of the row. A positive number indicates that
      <code>mz_diff</code> copies of the row were inserted, while a negative
      number indicates that <code>|mz_diff|</code> copies of the row were deleted.
    </td>
  </tr>
  <tr>
    <td>Column 1</td>
    <td>Varies</td>
    <td rowspan="3" style="vertical-align: middle; border-left: 1px solid #ccc">
        The columns from the subscribed relation, each as its own column,
        representing the data that was inserted into or deleted from the
        relation.
    </td>
  </tr>
  <tr>
    <td colspan="2" style="text-align: center">...</td>
  </tr>
  <tr>
    <td>Column <em>N</em></td>
    <td>Varies</td>
  </tr>
</tbody>
</table>

### `AS OF`

When a [history rentention
period](/transform-data/patterns/durable-subscriptions/#history-retention-period)
is configured for the object(s) powering the subscription, the `AS OF` clause
allows specifying a timestamp at which the `SUBSCRIBE` command should begin
returning results. If `AS OF` is specified, no rows whose timestamp is earlier
than the specified timestamp will be returned. If the timestamp specified is
earlier than the earliest historical state retained by the underlying objects,
an error is thrown.

To configure the history retention period for objects used in a subscription,
see [Durable
subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period).
If `AS OF` is unspecified, the system automatically chooses an `AS OF`
timestamp.

The value in the `AS OF` clause is automatically [cast to `mz_timestamp`](../../sql/types/mz_timestamp/#valid-casts) with an assignment or implicit cast.

### `UP TO`

The `UP TO` clause allows specifying a timestamp at which the `SUBSCRIBE` will cease running. If `UP TO` is specified, no rows whose timestamp is greater than or equal to the specified timestamp will be returned.

The value in the `UP TO` clause is automatically [cast to `mz_timestamp`](../../sql/types/mz_timestamp/#valid-casts) with an assignment or implicit cast.

### Interaction of `AS OF` and `UP TO`

The lower timestamp bound specified by `AS OF` is inclusive, whereas the upper bound specified by `UP TO` is exclusive. Thus, a `SUBSCRIBE` query whose `AS OF` is equal to its `UP TO` will terminate after returning zero rows.

A `SUBSCRIBE` whose `UP TO` is less than its `AS OF` timestamp (whether that
timestamp was specified in an `AS OF` clause or chosen by the system) will
signal an error.

### Duration

`SUBSCRIBE` will continue to run until canceled, the session ends, the `UP TO` timestamp is reached, or all updates have been presented. The latter case typically occurs when
tailing constant views (e.g. `CREATE VIEW v AS SELECT 1`).

{{< warning >}}

Many PostgreSQL drivers wait for a query to complete before returning its
results. Since `SUBSCRIBE` can run forever, naively executing a `SUBSCRIBE` using your
driver's standard query API may never return.

Either use an API in your driver that does not buffer rows or use the
[`FETCH`](/sql/fetch) statement or `AS OF` and `UP TO` bounds
to fetch rows from `SUBSCRIBE` in batches.
See the [examples](#examples) for details.

{{< /warning >}}

### `SNAPSHOT`

By default, `SUBSCRIBE` begins by emitting a snapshot of the subscribed relation, which
consists of a series of updates at its [`AS OF`](#as-of) timestamp describing the
contents of the relation. After the snapshot, `SUBSCRIBE` emits further updates as
they occur.

For updates in the snapshot, the `mz_timestamp` field will be fast-forwarded to the `AS OF` timestamp.
For example, an insert that occurred before the `SUBSCRIBE` began would appear in the snapshot.

To see only updates after the initial timestamp, specify `WITH (SNAPSHOT = false)`.

### `PROGRESS`

If the `PROGRESS` option is specified via `WITH (PROGRESS)`:

  * An additional `mz_progressed` column appears in the output. When the column
    is `false`, the rest of the row is a valid update. When the column is `true`
    everything in the row except for `mz_timestamp` is not a valid update and its
    content should be ignored; the row exists only to communicate that timestamps have advanced.

  * The first update emitted by the `SUBSCRIBE` is guaranteed to be a progress
    message indicating the subscribe's [`AS OF`](#as-of) timestamp.

Intuitively, progress messages communicate that no updates have occurred in a
given time window. Without explicit progress messages, it is impossible to
distinguish between a stall in Materialize and a legitimate period of no
updates.

Not all timestamps that appear will have a corresponding row with `mz_progressed` set to `true`.
For example, the following is a valid sequence of updates:

```nofmt
mz_timestamp | mz_progressed | mz_diff | column1
-------------|---------------|---------|--------------
1            | false         | 1       | data
2            | false         | 1       | more data
3            | false         | 1       | even more data
4            | true          | NULL    | NULL
```

Notice how Materialize did not emit explicit progress messages for timestamps
`1` or `2`. The receipt of the update at timestamp `2` implies that there
are no more updates for timestamp `1`, because timestamps are always presented
in non-decreasing order. The receipt of the explicit progress message at
timestamp `4` implies that there are no more updates for either timestamp
`2` or `3`—but that there may be more data arriving at timestamp `4`.

### Connection pooling

Because Materialize is wire-compatible with PostgreSQL, you can use any
PostgreSQL connection pooler with Materialize. For example in using PgBouncer,
see [Connection Pooling](/integrations/connection-pooling).

## Examples

`SUBSCRIBE` produces rows similar to a `SELECT` statement, except that `SUBSCRIBE` may never complete.
Many drivers buffer all results until a query is complete, and so will never return.
Below are the recommended ways to work around this.

### Creating an auction load generator

As an example, we'll create a [auction load generator](/sql/create-source/load-generator/#creating-an-auction-load-generator) that emits a row every second:

```mzsql
CREATE SOURCE auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES;
```

### Subscribing with `FETCH`

The recommended way to use `SUBSCRIBE` is with [`DECLARE`](/sql/declare) and [`FETCH`](/sql/fetch).
These must be used within a transaction, with [a single `DECLARE`](/sql/begin/#read-only-transactions) per transaction.
This allows you to limit the number of rows and the time window of your requests.
Next, let's subscribe to the `bids` table of the `auction` load generator source that we've created above.

First, declare a `SUBSCRIBE` cursor:

```mzsql
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM bids);
```

Then, use [`FETCH`](/sql/fetch) in a loop to retrieve each batch of results as soon as it's ready:

```mzsql
FETCH ALL c;
```

That will retrieve all of the rows that are currently available.
If there are no rows available, it will wait until there are some ready and return those.
A `timeout` can be used to specify a window in which to wait for rows. This will return up to the specified count (or `ALL`) of rows that are ready within the timeout. To retrieve up to 100 rows that are available in at most the next `1s`:

```mzsql
FETCH 100 c WITH (timeout='1s');
```

To retrieve all available rows available over the next `1s`:

```mzsql
FETCH ALL c WITH (timeout='1s');
```

A `0s` timeout can be used to return rows that are available now without waiting:

```mzsql
FETCH ALL c WITH (timeout='0s');
```

### Using clients

If you want to use `SUBSCRIBE` from an interactive SQL session (e.g.`psql`), wrap the query in `COPY`:

```mzsql
COPY (SUBSCRIBE (SELECT * FROM bids)) TO STDOUT;
```

| Additional guides |
| ---------------------- |
| [Go](/integrations/client-libraries/golang/#stream)|
| [Java](/integrations/client-libraries/java-jdbc/#stream)|
| [Node.js](/integrations/client-libraries/node-js/#stream)|
| [PHP](/integrations/client-libraries/php/#stream)|
| [Python](/integrations/client-libraries/python/#stream)|
| [Ruby](/integrations/client-libraries/ruby/#stream)|
| [Rust](/integrations/client-libraries/rust/#stream)|

### Mapping rows to their updates

After all the rows from the [`SNAPSHOT`](#snapshot) have been transmitted, the updates will be emitted as they occur. How can you map each row to its corresponding update?

| mz_timestamp | mz_progressed | mz_diff | Column 1 | .... | Column N |
| ------------ | ------------- | ------- | -------- | ---- | -------- | --------------------------- |
| 1            | false         | 1       | id1      |      | value1   |
| 1            | false         | 1       | id2      |      | value2   |
| 1            | false         | 1       | id3      |      | value3   | <- Last row from `SNAPSHOT` |
| 2            | false         | -1      | id1      |      | value1   |
| 2            | false         | 1       | id1      |      | value4   |

If your row has a unique column key, it is possible to map the update to its corresponding origin row; if the key is unknown, you can use the output of `hash(columns_values)` instead.

In the example above, `Column 1` acts as the column key that uniquely identifies the origin row the update refers to; in case this was unknown, hashing the values from `Column 1` to `Column N` would identify the origin row.

[//]: # "TODO(morsapaes) This page is now complex enough that we should
restructure it using the same feature-oriented approach as CREATE SOURCE/SINK.
See materialize#18829 for the design doc."

### Modifying the output format

#### `ENVELOPE UPSERT`

To modify the output of `SUBSCRIBE` to support upserts, use `ENVELOPE UPSERT`.
This clause allows you to specify a `KEY` that Materialize uses to interpret
the rows as a series of inserts, updates and deletes within each distinct
timestamp.

The output columns are reordered so that all the key columns come before the
value columns.

* Using this modifier, the output rows will have the following
structure:

   ```mzsql
   SUBSCRIBE mview ENVELOPE UPSERT (KEY (key));
   ```

   ```mzsql
   mz_timestamp | mz_state | key  | value
   -------------|----------|------|--------
   100          | upsert   | 1    | 2
   100          | upsert   | 2    | 4
   ```

* For inserts and updates, the value columns for each key are set to the
  resulting value of the series of operations, and `mz_state` is set to
  `upsert`.

  _Insert_

  ```mzsql
   -- at time 200, add a new row with key=3, value=6
   mz_timestamp | mz_state | key  | value
   -------------|----------|------|--------
   ...
   200          | upsert   | 3    | 6
   ...
  ```

  _Update_

  ```mzsql
   -- at time 300, update key=1's value to 10
   mz_timestamp | mz_state | key  | value
   -------------|----------|------|--------
   ...
   300          | upsert   | 1    | 10
   ...
  ```

* If only deletes occur within a timestamp, the value columns for each key are
  set to `NULL`, and `mz_state` is set to `delete`.

  _Delete_

  ```mzsql
   -- at time 400, delete all rows
   mz_timestamp | mz_state | key  | value
   -------------|----------|------|--------
   ...
   400          | delete   | 1    | NULL
   400          | delete   | 2    | NULL
   400          | delete   | 3    | NULL
   ...
  ```

* Only use `ENVELOPE UPSERT` when there is at most one live value per key.
  If materialize detects that a given key has multiple values, it will generate
  an update with `mz_state` set to `"key_violation"`, the problematic key, and all
  the values nulled out. Materialize is not guaranteed to detect this case,
  please don't rely on it.

  _Key violation_

  ```mzsql
   -- at time 500, introduce a key_violation
   mz_timestamp | mz_state        | key  | value
   -------------|-----------------|------|--------
   ...
   500          | key_violation   | 1    | NULL
   ...
  ```

* If [`PROGRESS`](#progress) is set, Materialize also returns the `mz_progressed`
column. Each progress row will have a `NULL` key and a `NULL` value.

#### `ENVELOPE DEBEZIUM`

{{< private-preview />}}

To modify the output of `SUBSCRIBE` to support upserts using a
[Debezium-style diff envelope](/sql/create-sink/kafka/#debezium-envelope),
use `ENVELOPE DEBEZIUM`. This clause allows you to specify a `KEY` that
Materialize uses to interpret the rows as a series of inserts, updates and
deletes within each distinct timestamp. Unlike `ENVELOPE UPSERT`, the output
includes the state of the row before and after the upsert operation.

The output columns are reordered so that all the key columns come before the
value columns. There are two copies of the value columns: one prefixed with
`before_`, which represents the value of the columns before the upsert
operation; and another prefixed with `after_`, which represents the current
value of the columns.

* Using this modifier, the output rows will have the following
structure:

   ```mzsql
   SUBSCRIBE mview ENVELOPE DEBEZIUM (KEY (key));
   ```

   ```mzsql
   mz_timestamp | mz_state | key  | before_value | after_value
   -------------|----------|------|--------------|-------
   100          | upsert   | 1    | NULL         | 2
   100          | upsert   | 2    | NULL         | 4
   ```

* For inserts: the before values are `NULL`, the current value is the newly inserted
  value and `mz_state` is set to `insert`.

  _Insert_

  ```mzsql
   -- at time 200, add a new row with key=3, value=6
   mz_timestamp | mz_state | key  | before_value | after_value
   -------------|----------|------|--------------|-------
   ...
   200          | insert   | 3    | NULL         | 6
   ...
  ```

* For updates: the before values are the old values, the value columns are the resulting
  values of the update, and `mz_state` is set to`upsert`.

  _Update_

  ```mzsql
   -- at time 300, update key=1's value to 10
   mz_timestamp | mz_state | key  | before_value | after_value
   -------------|----------|------|--------------|-------
   ...
   300          | upsert   | 1    | 2            | 10
   ...
  ```

* If only deletes occur within a timestamp, the value columns for each key are
  set to `NULL`, the before values are set to the old value and `mz_state` is set to `delete`.

  _Delete_

  ```mzsql
   -- at time 400, delete all rows
   mz_timestamp | mz_state | key  | before_value | after_value
   -------------|----------|------|--------------|-------
   ...
   400          | delete   | 1    | 10           | NULL
   400          | delete   | 2    | 4            | NULL
   400          | delete   | 3    | 6            | NULL
   ...
  ```

* Like `ENVELOPE UPSERT`, using `ENVELOPE DEBEZIUM` requires that there is at
  most one live value per key. If Materialize detects that a given key has
  multiple values, it will generate an update with `mz_state` set to
  `"key_violation"`, the problematic key, and all the values nulled out.
  Materialize identifies key violations on a best-effort basis.

  _Key violation_

  ```mzsql
   -- at time 500, introduce a key_violation
   mz_timestamp | mz_state        | key  | before_value | after_value
   -------------|-----------------|------|--------------|-------
   ...
   500          | key_violation   | 1    | NULL         | NULL
   ...
  ```

* If [`PROGRESS`](#progress) is set, Materialize also returns the
`mz_progressed` column. Each progress row will have a `NULL` key and a `NULL`
before and after value.

#### `WITHIN TIMESTAMP ORDER BY`

{{< private-preview />}}

To modify the ordering of the output of `SUBSCRIBE`, use `WITHIN TIMESTAMP ORDER
BY`. This clause allows you to specify an `ORDER BY` expression which is used
to sort the rows within each distinct timestamp.

* The `ORDER BY` expression can take any column in the underlying object or
  query, including `mz_diff`.

   ```mzsql
   SUBSCRIBE mview WITHIN TIMESTAMP ORDER BY c1, c2 DESC NULLS LAST, mz_diff;

   mz_timestamp | mz_diff | c1            | c2   | c3
   -------------|---------|---------------|------|-----
   100          | +1      | 1             | 20   | foo
   100          | -1      | 1             | 2    | bar
   100          | +1      | 1             | 2    | boo
   100          | +1      | 1             | 0    | data
   100          | -1      | 2             | 0    | old
   100          | +1      | 2             | 0    | new
   ```

* If [`PROGRESS`](#progress) is set, progress messages are unaffected.

### Dropping the `auction` load generator source

When you're done, you can drop the `auction` load generator source:

```mzsql
DROP SOURCE auction CASCADE;
```

### Durable subscriptions

Because `SUBSCRIBE` requests happen over the network, these connections might
get disrupted for both expected and unexpected reasons. You can adjust the
[history retention
period](/transform-data/patterns/durable-subscriptions/#history-retention-period)
for the objects a subscription depends on, and then use [`AS OF`](#as-of) to
pick up where you left off on connection drops—this ensures that no data is lost
in the subscription process, and avoids the need for re-snapshotting the data.

For more information, see [durable
subscriptions](/transform-data/patterns/durable-subscriptions/).

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/subscribe.md" >}}




---

## System catalog


Materialize exposes a system catalog that contains metadata about the running
Materialize instance.

The system catalog consists of several schemas that are implicitly available in
all databases. These schemas contain sources, tables, and views that expose
different types of metadata.

  * [`mz_catalog`](mz_catalog), which exposes metadata in Materialize's
    native format.

  * [`pg_catalog`](pg_catalog), which presents the data in `mz_catalog` in
    the format used by PostgreSQL.

  * [`information_schema`](information_schema), which presents the data in
    `mz_catalog` in the format used by the SQL standard's information_schema.

  * [`mz_internal`](mz_internal), which exposes internal metadata about
    Materialize in an unstable format that is likely to change.

  * [`mz_introspection`](mz_introspection), which contains replica
    introspection relations.

These schemas contain sources, tables, and views that expose metadata like:

  * Descriptions of each database, schema, source, table, view, sink, and
    index in the system.

  * Descriptions of all running dataflows.

  * Metrics about dataflow execution.

Whenever possible, applications should prefer to query `mz_catalog` over
`pg_catalog`. The mapping between Materialize concepts and PostgreSQL concepts
is not one-to-one, and so the data in `pg_catalog` cannot accurately represent
the particulars of Materialize.




---

## System clusters


## Overview

When you enable a Materialize region, various [system
clusters](/sql/system-clusters/) are pre-installed to improve the user
experience as well as support system administration tasks.

### `quickstart` cluster

A cluster named `quickstart` with a size of `25cc` and a replication factor of
`1` will be pre-installed in every environment. You can modify or drop this
cluster at any time.

{{< note >}}
The default value for the `cluster` session parameter is `quickstart`.
This cluster functions as a default option, pre-created for your convenience.
It allows you to quickly start running queries without needing to configure a cluster first.
If the `quickstart` cluster is dropped, you must run [`SET cluster`](/sql/select/#ad-hoc-queries)
to choose a valid cluster in order to run `SELECT` queries. A _superuser_ (i.e. `Organization Admin`)
can also run [`ALTER SYSTEM SET cluster`](/sql/alter-system-set) to change the
default value.
{{< /note >}}

### `mz_catalog_server` system cluster

A system cluster named `mz_catalog_server` will be pre-installed in every
environment. This cluster has several indexes installed to speed up `SHOW`
commands and queries using the system catalog.

To take advantage of these indexes, Materialize will automatically re-route
`SHOW` commands and queries using system catalog objects to the
`mz_catalog_server` system cluster. You can disable this behavior in
your session via the `auto_route_catalog_queries`
[configuration parameter](/sql/show/#other-configuration-parameters).

The following characteristics apply to the `mz_catalog_server` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You can run `SELECT` or `SUBSCRIBE` queries in this cluster as long
    as you only reference objects in the [system catalog](/sql/system-catalog/).

### `mz_probe` system cluster

A system cluster named `mz_probe` will be pre-installed in every environment.
This cluster is used for internal uptime monitoring.

The following characteristics apply to the `mz_probe` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` queries in this cluster.

### `mz_support` system cluster

A system cluster named `mz_support` will be pre-installed in every environment.
This cluster is used for internal support tasks.

The following characteristics apply to the `mz_support` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` queries in this cluster.

### `mz_system` system cluster

A system cluster named `mz_system` will be pre-installed in every environment.
This cluster is used for internal system jobs.

The following characteristics apply to the `mz_system` cluster:

  * You are **not billed** for this cluster.
  * You cannot create objects in this cluster.
  * You cannot drop this cluster.
  * You cannot run `SELECT` or `SUBSCRIBE` queries in this cluster.


## Related pages

- [`CREATE CLUSTER`](/sql/create-cluster)
- [`SHOW CLUSTER`](/sql/show-clusters)
- [`DROP CLUSTER`](/sql/drop-cluster)




---

## TABLE


The `TABLE` expression retrieves all rows from a single table.

## Syntax

```mzsql
TABLE <table_name>;
```

## Details

`TABLE` expressions can be used anywhere that [`SELECT`] expressions are valid.

The expression `TABLE t` is exactly equivalent to the following [`SELECT`]
expression:

```mzsql
SELECT * FROM t;
```

## Examples

Using a `TABLE` expression as a standalone statement:

```mzsql
TABLE t;
```
```nofmt
 a
---
 1
 2
```

Using a `TABLE` expression in place of a [`SELECT`] expression:

```mzsql
TABLE t ORDER BY a DESC LIMIT 1;
```
```nofmt
 a
---
 2
```

[`SELECT`]: ../select




---

## UPDATE


`UPDATE` changes values stored in [user-created tables](../create-table).

## Syntax

```mzsql
UPDATE <table_name> [ AS <table_alias> ]
   SET <column_name> = <expression> [, <column2_name> = <expression2>, ...]
[WHERE <condition(s)> ];
```

Syntax element                | Description
------------------------------|------------
**AS** <table_alias>          | If specified, you can only use the alias to refer to the table within that `UPDATE` statement.
**WHERE** <condition(s)>      | If specified, only update rows that meet the condition(s).

## Details

### Known limitations

* `UPDATE` cannot be used inside [transactions](../begin).
* `UPDATE` can reference [read-write tables](../create-table) but not
  [sources](../create-source) or read-only tables.
* **Low performance.** While processing an `UPDATE` statement, Materialize cannot
  process other `INSERT`, `UPDATE`, or `DELETE` statements.

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `UPDATE` privileges on the table being updated.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
    execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
    granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

## Examples

All examples below will use the `example_table` table:

```mzsql
CREATE TABLE example_table (a int, b text);
INSERT INTO example_table VALUES (1, 'hello'), (2, 'goodbye');
```

To verify the initial state of the table, run the following `SELECT` statement:

```mzsql
SELECT * FROM example_table;
```

The `SELECT` statement above should return two rows:

```
 a |    b
---+---------
 1 | hello
 2 | goodbye
```

### Update based on a condition

The following `UPDATE` example includes a `WHERE` clause to specify which rows
to update:

```mzsql
UPDATE example_table
SET a = a + 2
WHERE b = 'hello';
```

Only one row should be updated, namely the row with `b = 'hello'`. To verify
that the operation updated the `a` column only for that row, run the following
`SELECT` statement:

```mzsql
SELECT * FROM example_table;
```

The returned results show that column `a` was updated only for the row with `b
= 'hello'`:

```
 a |    b
---+---------
 3 | hello         -- Previous value: 1
 2 | goodbye
```

### Update all rows

The following `UPDATE` example updates all rows in the table to set `a` to 0 and
`b` to `aloha`:

```mzsql
UPDATE example_table
SET a = 0, b = 'aloha';
```

To verify the results, run the following `SELECT` statement:

```mzsql
SELECT * FROM example_table;
```

The returned results show that all rows were updated:

```
 a |   b
---+-------
 0 | aloha
 0 | aloha
```

## Related pages

- [`CREATE TABLE`](../create-table)
- [`INSERT`](../insert)
- [`SELECT`](../select)




---

## VALIDATE CONNECTION


`VALIDATE CONNECTION` validates the connection and authentication parameters
provided in a `CREATE CONNECTION` statement against the target external
system.

## Syntax

```mzsql
VALIDATE CONNECTION <connection_name>;
```

## Description

Upon executing the `VALIDATE CONNECTION` command, Materialize initiates a
connection to the target external system and attempts to authenticate in the
same way as if the connection were used in a `CREATE SOURCE` or `CREATE SINK`
statement. A successful connection and authentication attempt returns
successfully with no rows. If an issue is encountered, the command will return
a validation error.

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/validate-connection.md" >}}

## Related pages

- [`CREATE CONNECTION`](/sql/create-connection/)
- [`SHOW CONNECTIONS`](/sql/show-connections)




---

## VALUES


`VALUES` constructs a relation from a list of parenthesized value expressions.

## Syntax

```mzsql
VALUES ( expr [, ...] ) [, ( expr [, ...] ) ... ];
```

Each parenthesis represents a single row. The comma-delimited expressions in
the parenthesis represent the values of the columns in that row.

## Details

`VALUES` expressionscan be used anywhere that [`SELECT`] statements are valid.
They are most commonly used in [`INSERT`] statements, but can also be used
on their own.

## Examples

### Use `VALUES` in an `INSERT`

`VALUES` expressions are most commonly used in [`INSERT`] statements. The
following example uses a `VALUES` expression in an [`INSERT`] statement:

```mzsql
INSERT INTO my_table VALUES (1, 2), (3, 4);
```

### Use `VALUES` as a standalone expression

`VALUES` expression can be used anywhere that [`SELECT`] statements are valid.

For example:

- As a standalone expression:

  ```mzsql
  VALUES (1, 2, 3), (4, 5, 6);
  ```

  The above expression returns the following results:

  ```nofmt
  column1 | column2 | column3
  --------+---------+---------
        1 |       2 |       3
        4 |       5 |       6
  ```

- With an `ORDER BY` and `LIMIT`:

  ```mzsql
  VALUES (1), (2), (3) ORDER BY column1 DESC LIMIT 2;
  ```

  The above expression returns the following results:

  ```nofmt
  column1
  --------
        3
        2
  ```

[`INSERT`]: ../insert
[`SELECT`]: ../select



