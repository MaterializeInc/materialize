# SQL commands

SQL commands reference.



## Create/Alter/Drop Objects

| CREATE | ALTER | DROP |
| --- | --- | --- |
| [`CREATE CLUSTER`](/sql/create-cluster) | [`ALTER CLUSTER`](/sql/alter-cluster) | [`DROP CLUSTER`](/sql/drop-cluster) |
| [`CREATE CLUSTER REPLICA`](/sql/create-cluster-replica) | [`ALTER CLUSTER REPLICA`](/sql/alter-cluster-replica) | [`DROP CLUSTER REPLICA`](/sql/drop-cluster-replica) |
| [`CREATE CONNECTION`](/sql/create-connection) | [`ALTER CONNECTION`](/sql/alter-connection) | [`DROP CONNECTION`](/sql/drop-connection) |
| [`CREATE DATABASE`](/sql/create-database) | [`ALTER DATABASE`](/sql/alter-database) | [`DROP DATABASE`](/sql/drop-database) |
| [`CREATE INDEX`](/sql/create-index) | [`ALTER INDEX`](/sql/alter-index) | [`DROP INDEX`](/sql/drop-index) |
| [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view) | [`ALTER MATERIALIZED VIEW`](/sql/alter-materialized-view) | [`DROP MATERIALIZED VIEW`](/sql/drop-materialized-view) |
| [`CREATE NETWORK POLICY`](/sql/create-network-policy) | [`ALTER NETWORK POLICY`](/sql/alter-network-policy) | [`DROP NETWORK POLICY`](/sql/drop-network-policy) |
| [`CREATE ROLE`](/sql/create-role) | [`ALTER ROLE`](/sql/alter-role) | [`DROP ROLE`](/sql/drop-role) <br>[`DROP USER`](/sql/drop-user) |
| [`CREATE SCHEMA`](/sql/create-schema) | [`ALTER SCHEMA`](/sql/alter-schema) | [`DROP SCHEMA`](/sql/drop-schema) |
| [`CREATE SECRET`](/sql/create-secret) | [`ALTER SECRET`](/sql/alter-secret) | [`DROP SECRET`](/sql/drop-secret) |
| [`CREATE SINK`](/sql/create-sink) | [`ALTER SINK`](/sql/alter-sink) | [`DROP SINK`](/sql/drop-sink) |
| [`CREATE SOURCE`](/sql/create-source) | [`ALTER SOURCE`](/sql/alter-source) | [`DROP SOURCE`](/sql/drop-source) |
| [`CREATE TABLE`](/sql/create-table) | [`ALTER TABLE`](/sql/alter-table) | [`DROP TABLE`](/sql/drop-table) |
| [`CREATE TYPE`](/sql/create-type) | [`ALTER TYPE`](/sql/alter-type) | [`DROP TYPE`](/sql/drop-type) |
| [`CREATE VIEW`](/sql/create-view) | [`ALTER VIEW`](/sql/alter-view) | [`DROP VIEW`](/sql/drop-view) |


## Create/Read/Update/Delete Data

The following commands perform CRUD operations on materialized views, views,
sources, and tables:


| <strong>Select/Subscribe</strong> |  - [`SELECT`](/sql/select)  - [`SUBSCRIBE`](/sql/subscribe)    |
| <strong>Cursor</strong> |  - [`CLOSE`](/sql/close)  - [`DECLARE`](/sql/declare)  - [`FETCH`](/sql/fetch)   |
| <strong>Sink</strong> |  - [`ALTER SINK`](/sql/alter-sink)  - [`CREATE SINK`](/sql/create-sink)  - [`DROP SINK`](/sql/drop-sink)   |
| <strong>Transactions</strong> |  - [`BEGIN`](/sql/begin)  - [`COMMIT`](/sql/commit)  - [`ROLLBACK`](/sql/rollback)   |
| <strong>Copy</strong> |  - [`COPY FROM`](/sql/copy-from)  - [`COPY TO`](/sql/copy-to)   |


## RBAC

Commands to manage roles and privileges and owners:


| <strong>Roles</strong> |  - [`ALTER ROLE`](/sql/alter-role)  - [`CREATE ROLE`](/sql/create-role)  - [`DROP ROLE`](/sql/drop-role) <br>[`DROP USER`](/sql/drop-user)  - [`GRANT ROLE`](/sql/grant-role)  - [`REVOKE ROLE`](/sql/revoke-role)  - [`SHOW ROLES`](/sql/show-roles)   |
| <strong>Privileges</strong> |  - [`ALTER DEFAULT PRIVILEGES`](/sql/alter-default-privileges)  - [`GRANT PRIVILEGE`](/sql/grant-privilege)  - [`REVOKE PRIVILEGE`](/sql/revoke-privilege)   |
| <strong>Owners</strong> |  - [`ALTER CLUSTER`](/sql/alter-cluster)  - [`ALTER CLUSTER REPLICA`](/sql/alter-cluster-replica)  - [`ALTER CONNECTION`](/sql/alter-connection)  - [`ALTER DATABASE`](/sql/alter-database)  - [`ALTER MATERIALIZED VIEW`](/sql/alter-materialized-view)  - [`ALTER SCHEMA`](/sql/alter-schema)  - [`ALTER SECRET`](/sql/alter-secret)  - [`ALTER SINK`](/sql/alter-sink)  - [`ALTER SOURCE`](/sql/alter-source)  - [`ALTER TABLE`](/sql/alter-table)  - [`ALTER TYPE`](/sql/alter-type)  - [`ALTER VIEW`](/sql/alter-view)  - [`DROP OWNED`](/sql/drop-owned)  - [`REASSIGN OWNED`](/sql/reassign-owned)   |


## Query Introspection (`Explain`)


- [`EXPLAIN ANALYZE`](/sql/explain-analyze)

- [`EXPLAIN FILTER PUSHDOWN`](/sql/explain-filter-pushdown)

- [`EXPLAIN PLAN`](/sql/explain-plan)

- [`EXPLAIN SCHEMA`](/sql/explain-schema)

- [`EXPLAIN TIMESTAMP`](/sql/explain-timestamp)


## Object Introspection (`SHOW`) { #show }


- [`SHOW`](/sql/show)

- [`SHOW CLUSTER REPLICAS`](/sql/show-cluster-replicas)

- [`SHOW CLUSTERS`](/sql/show-clusters)

- [`SHOW COLUMNS`](/sql/show-columns)

- [`SHOW CONNECTIONS`](/sql/show-connections)

- [`SHOW CREATE CLUSTER`](/sql/show-create-cluster)

- [`SHOW CREATE CONNECTION`](/sql/show-create-connection)

- [`SHOW CREATE INDEX`](/sql/show-create-index)

- [`SHOW CREATE MATERIALIZED VIEW`](/sql/show-create-materialized-view)

- [`SHOW CREATE SINK`](/sql/show-create-sink)

- [`SHOW CREATE SOURCE`](/sql/show-create-source)

- [`SHOW CREATE TABLE`](/sql/show-create-table)

- [`SHOW CREATE TYPE`](/sql/show-create-type)

- [`SHOW CREATE VIEW`](/sql/show-create-view)

- [`SHOW DATABASES`](/sql/show-databases)

- [`SHOW DEFAULT PRIVILEGES`](/sql/show-default-privileges)

- [`SHOW INDEXES`](/sql/show-indexes)

- [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views)

- [`SHOW NETWORK POLICIES (Cloud)`](/sql/show-network-policies)

- [`SHOW OBJECTS`](/sql/show-objects)

- [`SHOW PRIVILEGES`](/sql/show-privileges)

- [`SHOW ROLE MEMBERSHIP`](/sql/show-role-membership)

- [`SHOW ROLES`](/sql/show-roles)

- [`SHOW SCHEMAS`](/sql/show-schemas)

- [`SHOW SECRETS`](/sql/show-secrets)

- [`SHOW SINKS`](/sql/show-sinks)

- [`SHOW SOURCES`](/sql/show-sources)

- [`SHOW SUBSOURCES`](/sql/show-subsources)

- [`SHOW TABLES`](/sql/show-tables)

- [`SHOW TYPES`](/sql/show-types)

- [`SHOW VIEWS`](/sql/show-views)


## Session

Commands related with session state and configurations:


- [`DISCARD`](/sql/discard)

- [`RESET`](/sql/reset)

- [`SET`](/sql/set)

- [`SHOW`](/sql/show)


## Validations


- [`VALIDATE CONNECTION`](/sql/validate-connection)


## Prepared Statements


- [`DEALLOCATE`](/sql/deallocate)

- [`EXECUTE`](/sql/execute)

- [`PREPARE`](/sql/prepare)




---

## Namespaces


Namespaces are a way to organize Materialize objects logically. In organizations
with multiple objects, namespaces help avoid naming conflicts and make it easier
to manage objects.

## Namespace hierarchy

Materialize follows SQL standard's namespace hierarchy for most objects (for the
exceptions, see [Other objects](#other-objects)).

|                           |             |
|---------------------------| ------------|
| 1st/Highest level:        |  **Database** |
| 2nd level:                |  **Schema**   |
| 3rd level:                | <table><tbody><tr><td><ul><li>**Table**</li><li>**View**</li><li>**Materialized view**</li><li>**Connection**</li></ul></td><td><ul><li>**Source**</li><li>**Sink**</li><li>**Index**</li></ul></td><td><ul><li>**Type**</li><li>**Function**</li><li>**Secret**</li></ul></td></tr></tbody></table>|
| 4th/Lowest level:             | **Column**     |

Each layer in the hierarchy can contain elements from the level immediately
beneath it. That is,

- Databases can contain: schemas;
- Schemas can contain: tables, views, materialized views, connections, sources,
sinks, indexes, types, functions, and secrets;
- Tables, views, and materialized views can contain: columns.


### Qualifying names

Namespaces enable disambiguation and access to objects across different
databases and schemas. Namespaces use the dot notation format
(`<database>.<schema>....`) and allow you to refer to objects by:

- **Fully qualified names**

  Used to reference objects in a different database (Materialize allows
  cross-database queries); e.g.,

  ```
  <Database>.<Schema>
  <Database>.<Schema>.<Source>
  <Database>.<Schema>.<View>
  <Database>.<Schema>.<Table>.<Column>
  ```

  > **Tip:** You can use fully qualified names to reference objects within the same
>   database (or within the same database and schema). However, for brevity and
>   readability, you may prefer to use qualified names instead.


- **Qualified names**

  - Used to reference objects within the same database but different schema, use
    the schema and object name; e.g.,

    ```
    <Schema>.<Source>
    <Schema>.<View>
    <Schema>.<Table>.<Column>
    ```

  - Used to reference objects within the same database and schema, use the
    object name; e.g.,

    ```
    <Source>
    <View>
    <Table>.<Column>
    <View>.<Column>
    ```

## Namespace constraints

All namespaces must adhere to [identifier rules](/sql/identifiers).


## Other objects

The following Materialize objects  exist outside the standard SQL namespace
hierarchy:

- **Clusters**: Referenced directly by its name.

  For example, to create a materialized view in the cluster `cluster1`:

  ```mzsql
  CREATE MATERIALIZED VIEW mv IN CLUSTER cluster1 AS ...;
  ```

- **Cluster replicas**: Referenced as `<cluster-name>.<replica-name>`.

  For example, to delete replica `r1` in cluster `cluster1`:

  ```mzsql
  DROP CLUSTER REPLICA cluster1.r1
  ```

- **Roles**: Referenced by their name. For example, to alter the `manager` role, your SQL statement would be:

  ```mzsql
  ALTER ROLE manager ...
  ```

### Other object namespace constraints

- Two clusters or two roles cannot have the same name. However, a cluster and a
  role can have the same name.

- Replicas can have the same names as long as they belong to different clusters.
  Materialize automatically assigns names to replicas (e.g., `r1`, `r2`).

## Database details

- By default, Materialize regions have a database named `materialize`.
- By default, each database has a schema called `public`.
- You can specify which database you connect to either when you connect (e.g.
  `psql -d my_db ...`) or within SQL using [`SET DATABASE`](/sql/set/) (e.g.
  `SET DATABASE = my_db`).
- Materialize allows cross-database queries.


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


**Set a configuration:**

### Set a configuration

To set a cluster configuration:



```mzsql
ALTER CLUSTER <cluster_name>
SET (
    SIZE = <text>
    [, REPLICATION FACTOR = <int>]
    [, MANAGED = <bool>]
    [, SCHEDULE = MANUAL|ON REFRESH(...)]
)
[WITH ( <with_option>[,...])]
;

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The name of the cluster you want to alter.  |
| `SIZE` | <a name="alter-cluster-size"></a> The size of the resource allocations for the cluster. {{< yaml-list column="Cluster size" data="m1_cluster_sizing" numColumns="3" >}} See [Size](#available-sizes) for details as well as legacy sizes available. {{< warning >}} Changing the size of a cluster may incur downtime. For more information, see [Resizing considerations](#resizing). {{< /warning >}} Not available for `ALTER CLUSTER ... RESET` since there is no default `SIZE` value. |
| `REPLICATION FACTOR` | Optional.The number of replicas to provision for the cluster. Each replica of the cluster provisions a new pool of compute resources to perform exactly the same computations on exactly the same data. For more information, see [Replication factor considerations](#replication-factor).  Default: `1`  |
| `MANAGED` | Optional. Whether to automatically manage the cluster's replicas based on the configured size and replication factor.  If `FALSE`, enables the use of the <em>deprecated</em> [`CREATE CLUSTER REPLICA`](/sql/create-cluster-replica) command.  Default: `TRUE`  |
| `SCHEDULE` | Optional. The [scheduling type](/sql/create-cluster/#scheduling) for the cluster. Valid values are `MANUAL` and `ON REFRESH`.  Default: `MANUAL`  |
| `WITH (<with_option>[,...])` |  The following `<with_option>`s are supported: \| Option  \| Description \| \|--------\|-------------\| \| `WAIT UNTIL READY(...)`    \| ***Private preview.** This option has known performance or stability issues and is under activedevelopment.* {{< include-from-yaml data="examples/alter_cluster" name="wait-until-ready-cmd-option" >}} \| \| `WAIT FOR` \|  ***Private preview.** This option has known performance or stability issues and is under active development.* A fixed duration to wait for the new replicas to be ready. This option can lead to downtime. As such, we recommend using the `WAIT UNTIL READY` option instead.\|  |



**Reset to default:**

### Reset to default

To reset a cluster configuration back to its default value:



```mzsql
ALTER CLUSTER <cluster_name>
RESET (
    REPLICATION FACTOR | MANAGED | SCHEDULE,
    ...
)
;

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The name of the cluster you want to alter.  |
| `REPLICATION FACTOR` | Optional. The number of replicas to provision for the cluster.  Default: `1`  |
| `MANAGED` | Optional. Whether to automatically manage the cluster's replicas based on the configured size and replication factor.  Default: `TRUE`  |
| `SCHEDULE` | Optional. The [scheduling type](/sql/create-cluster/#scheduling) for the cluster.  Default: `MANUAL`  |



**Rename:**

### Rename

To rename a cluster:



```mzsql
ALTER CLUSTER <cluster_name> RENAME TO <new_cluster_name>;

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The current name of the cluster.  |
| `<new_cluster_name>` | The new name of the cluster.  |


> **Note:** You cannot rename system clusters, such as `mz_system` and `mz_catalog_server`.



**Change owner:**

### Change owner

To change the owner of a cluster:



```mzsql
ALTER CLUSTER <cluster_name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The name of the cluster you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the cluster.  |
To change the owner, you must have ownership of the cluster and membership in
the `<new_owner_role>`. See also [Required privileges](#required-privileges).



**Swap with:**

### Swap with

> **Important:** Information about the `SWAP WITH` operation is provided for completeness.  The
> `SWAP WITH` operation is used for blue/green deployments. In general, you will
> not need to manually perform this operation.


To swap the name of this cluster with another cluster:



```mzsql
ALTER CLUSTER <cluster1> SWAP WITH <cluster2>;

```

| Syntax element | Description |
| --- | --- |
| `<cluster1>` | The name of the first cluster.  |
| `<cluster2>` | The name of the second cluster.  |





## Considerations

### Resizing

> **Tip:** For help sizing your clusters, navigate to **Materialize Console >**
> [**Monitoring**](/console/monitoring/)>**Environment Overview**. This page
> displays cluster resource utilization and sizing advice.


#### Available sizes


**M.1 Clusters:**

> **Note:** The values set forth in the table are solely for illustrative purposes.
> Materialize reserves the right to change the capacity at any time. As such, you
> acknowledge and agree that those values in this table may change at any time,
> and you should not rely on these values for any capacity planning.




| Cluster size | Compute Credits/Hour | Total Capacity | Notes |
| --- | --- | --- | --- |
| <strong>M.1-nano</strong> | 0.75 | 26 GiB |  |
| <strong>M.1-micro</strong> | 1.5 | 53 GiB |  |
| <strong>M.1-xsmall</strong> | 3 | 106 GiB |  |
| <strong>M.1-small</strong> | 6 | 212 GiB |  |
| <strong>M.1-medium</strong> | 9 | 318 GiB |  |
| <strong>M.1-large</strong> | 12 | 424 GiB |  |
| <strong>M.1-1.5xlarge</strong> | 18 | 636 GiB |  |
| <strong>M.1-2xlarge</strong> | 24 | 849 GiB |  |
| <strong>M.1-3xlarge</strong> | 36 | 1273 GiB |  |
| <strong>M.1-4xlarge</strong> | 48 | 1645 GiB |  |
| <strong>M.1-8xlarge</strong> | 96 | 3290 GiB |  |
| <strong>M.1-16xlarge</strong> | 192 | 6580 GiB | Available upon request |
| <strong>M.1-32xlarge</strong> | 384 | 13160 GiB | Available upon request |
| <strong>M.1-64xlarge</strong> | 768 | 26320 GiB | Available upon request |
| <strong>M.1-128xlarge</strong> | 1536 | 52640 GiB | Available upon request |



**Legacy cc Clusters:**

> **Tip:** In most cases, you **should not** use legacy sizes. [M.1 sizes](#available-sizes)
> offer better performance per credit for nearly all workloads. We recommend using
> M.1 sizes for all new clusters, and recommend migrating existing
> legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
> customers during the transition period as we move to deprecate legacy sizes.
> The legacy size information is provided for completeness.


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



See also:

- [M.1 to cc size mapping](/sql/m1-cc-mapping/).

- [Materialize service consumption
  table](https://materialize.com/pdfs/pricing.pdf).

- [Blog:Scaling Beyond Memory: How Materialize Uses Swap for Larger
  Workloads](https://materialize.com/blog/scaling-beyond-memory/).

#### Resource allocation

To determine the specific resource allocation for a given cluster size, query
the [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
system catalog table.

> **Warning:** The values in the `mz_cluster_replica_sizes` table may change at any
> time. You should not rely on them for any kind of capacity planning.


#### Downtime

Resizing operation can incur downtime unless used with WAIT UNTIL READY option.
See [zero-downtime cluster resizing](#zero-downtime-cluster-resizing) for
details.

#### Zero-downtime cluster resizing



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

> **Note:** Using `WAIT UNTIL READY` requires that the session remain open: you need to
> make sure the Console tab remains open or that your `psql` connection remains
> stable.
> Any interruption will cause a cancellation, no cluster changes will take
> effect.

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

> **Note:** - Each replica incurs cost, calculated as `cluster size *
>   replication factor` per second. See [Usage &
>   billing](/administration/billing/) for more details.
> - Increasing the replication factor does **not** increase the cluster's work
>   capacity. Replicas are exact copies of one another: each replica must do
>   exactly the same work (i.e., maintain the same dataflows and process the same
>   queries) as all the other replicas of the cluster.
>   To increase the capacity of a cluster, you must increase its
>   [size](#resizing).


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

- Ownership of the cluster.

- To rename a cluster, you must also have membership in the `<new_owner_role>`.

- To swap names with another cluster, you must also have ownership of the other
  cluster.

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
CLUSTER` command with the `WAIT UNTIL READY` [option](#syntax):

```mzsql
ALTER CLUSTER c1
SET (SIZE 'M.1-xsmall') WITH (WAIT UNTIL READY (TIMEOUT = '10m', ON TIMEOUT = 'COMMIT'));
```

> **Note:** Using `WAIT UNTIL READY` requires that the session remain open: you need to
> make sure the Console tab remains open or that your `psql` connection remains
> stable.
> Any interruption will cause a cancellation, no cluster changes will take
> effect.

Alternatively, you can alter the cluster size immediately, without waiting, by
running the `ALTER CLUSTER` command:

```mzsql
ALTER CLUSTER c1 SET (SIZE 'M.1-xsmall');
```

This will incur downtime when the cluster contains objects that need
re-hydration before they are ready. This includes indexes, materialized views,
and some types of sources.

### Schedule



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

> **Note:** When getting started with Materialize, we recommend using managed clusters. You
> can convert any unmanaged clusters to managed clusters by following the
> instructions below.


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


**Rename:**

### Rename

To rename a cluster replica:



```mzsql
ALTER CLUSTER REPLICA <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the cluster replica.  |
| `<new_name>` | The new name of the cluster replica.  |


> **Note:** You cannot rename replicas in system clusters.



**Change owner:**

### Change owner

To change the owner of a cluster replica:



```mzsql
ALTER CLUSTER REPLICA <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the cluster replica you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the cluster replica.  |
To change the owner of a cluster replica, you must be the current owner and have
membership in the `<new_owner_role>`.






## Privileges

The privileges required to execute this statement are:

- Ownership of the cluster replica.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing cluster.

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


**SET/DROP/RESET options:**

### SET/DROP/RESET options

To modify connection parameters:



```mzsql
ALTER CONNECTION [IF EXISTS] <name>
  SET (<option> = <value>) | DROP (<option>) | RESET (<option>)
  [, ...]
  [WITH (VALIDATE [true|false])]
;

```

| Syntax element | Description |
| --- | --- |
| **IF EXISTS** | Optional. If specified, do not return an error if the specified connection does not exist.  |
| `<name>` | The identifier of the connection you want to alter.  |
| **SET** | Sets the option to the specified value.  |
| **DROP** | Resets the specified option to its default value. Synonym for **RESET**.  |
| **RESET** | Resets the specified option to its default value. Synonym for **DROP**.  |
| `<option>` | The connection option to modify. See [`CREATE CONNECTION`](/sql/create-connection) for available options.  |
| `<value>` | The value to assign to the option.  |
| **WITH (VALIDATE `<bool>`)** | Optional. Whether [connection validation](/sql/create-connection#connection-validation) should be performed. Defaults to `true`.  |



**ROTATE KEYS:**

### ROTATE KEYS

To rotate SSH tunnel connection key pairs:



```mzsql
ALTER CONNECTION [IF EXISTS] <name> ROTATE KEYS;

```

| Syntax element | Description |
| --- | --- |
| **IF EXISTS** | Optional. If specified, do not return an error if the specified connection does not exist.  |
| `<name>` | The identifier of the SSH tunnel connection.  |





**Rename:**

### Rename

To rename a connection



```mzsql
ALTER CONNECTION <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the connection.  |
| `<new_name>` | The new name of the connection.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a connection:



```mzsql
ALTER CONNECTION <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the connection you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the connection.  |
To change the owner of a connection, you must be the owner of the connection and
have membership in the `<new_owner_role>`. See also [Privileges](#privileges).





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

- Ownership of the connection.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the connection is namespaced
  by a schema.

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


**Rename:**

### Rename

To rename a database:



```mzsql
ALTER DATABASE <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the database.  |
| `<new_name>` | The new name of the database.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a database:



```mzsql
ALTER DATABASE <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the database you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the database.  |
To change the owner of a database, you must be the current owner and have
membership in the `<new_owner_role>`.






## Privileges

The privileges required to execute this statement are:

- Ownership of the database.
- In addition, to change owners:
  - Role membership in `new_owner`.


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

- Role membership in `role_name`.
- `USAGE` privileges on the containing database if `database_name` is specified.
- `USAGE` privileges on the containing schema if `schema_name` is specified.
- _superuser_ status if the _target_role_ is `PUBLIC` or **ALL ROLES** is
  specified.

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


**Rename:**

### Rename

To rename an index:



```mzsql
ALTER INDEX <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the index you want to alter.  |
| `<new_name>` | The new name of the index.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).







## Privileges

The privileges required to execute this statement are:

- Ownership of the index.

## Related pages

- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW SINKS`](/sql/show-sinks)


---

## ALTER MATERIALIZED VIEW


Use `ALTER MATERIALIZED VIEW` to:

- Rename a materialized view.
- Change owner of a materialized view.
- Change retain history configuration for the materialized view.










## Syntax


**Rename:**

### Rename

To rename a materialized view:



```mzsql
ALTER MATERIALIZED VIEW <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the materialized view you want to alter.  |
| `<new_name>` | The new name of the materialized view.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a materialized view:



```mzsql
ALTER MATERIALIZED VIEW <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the materialized view you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the materialized view.  |
To change the owner of a materialized view, you must be the owner of the materialized view and have
membership in the `<new_owner_role>`. See also [Privileges](#privileges).



**(Re)Set retain history config:**

### (Re)Set retain history config

To set the retention history for a materialized view:



```mzsql
ALTER MATERIALIZED VIEW <name> SET (RETAIN HISTORY [=] FOR <retention_period>);

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the materialized view you want to alter.  |
| `<retention_period>` | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.  |


To reset the retention history to the default for a materialized view:



```mzsql
ALTER MATERIALIZED VIEW <name> RESET (RETAIN HISTORY);

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the materialized view you want to alter.  |













## Privileges

The privileges required to execute this statement are:

- Ownership of the materialized view.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the materialized view is
  namespaced by a schema.

## Related pages

- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](/sql/show-create-materialized-view)
- [`DROP MATERIALIZED VIEW`](/sql/drop-materialized-view)


---

## ALTER NETWORK POLICY (Cloud)


*Available for Materialize Cloud only*

`ALTER NETWORK POLICY` alters an existing network policy. Network policies are
part of Materialize's framework for [access control](/security/cloud/).

Changes to a network policy will only affect new connections
and **will not** terminate active connections.

## Syntax



```mzsql
ALTER NETWORK POLICY <name> SET (
  RULES (
    <rule_name> (action='allow', direction='ingress', address=<address>)
    [, ...]
  )
)
;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the network policy to modify.  |
| `<rule_name>` | The name for the network policy rule. Must be unique within the network policy.  |
| `<address>` | The Classless Inter-Domain Routing (CIDR) block to which the rule applies.  |


## Details

### Pre-installed network policy

When you enable a Materialize region, a default network policy named `default`
will be pre-installed. This policy has a wide open ingress rule `allow
0.0.0.0/0`. You can modify or drop this network policy at any time.

> **Note:** The default value for the `network_policy` session parameter is `default`.
> Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
> Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
> change the default value.


### Lockout prevention

To prevent lockout, the IP of the active user is validated against the policy
changes requested. This prevents users from modifying network policies in a way
that could lock them out of the system.

## Privileges

The privileges required to execute this statement are:

- Ownership of the network policy.

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




**Cloud:**

### Cloud

The following syntax is used to alter a role in Materialize Cloud.


```mzsql
ALTER ROLE <role_name>
[[WITH] INHERIT]
[SET <config> =|TO <value|DEFAULT> ]
[RESET <config>];

```

| Syntax element | Description |
| --- | --- |
| `INHERIT` | *Optional.* If specified, grants the role the ability to inherit privileges of other roles. *Default.*  |
| `SET <name> TO <value\|DEFAULT>` | *Optional.* If specified, sets the configuration parameter for the role to the `<value>` or if the value specified is `DEFAULT`, the system's default (equivalent to `ALTER ROLE ... RESET <name>`).  To view the configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).  {{< note >}}  - Altering the configuration parameter for a role only affects **new sessions**. - Role configuration parameters are **not inherited**.  {{< /note >}}  |
| `RESET <name>` | *Optional.* If specified, resets the configuration parameter for the role to the system's default.  To view the configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).  {{< note >}}  - Altering the configuration parameter for a role only affects **new sessions**. - Role configuration parameters are **not inherited**.  {{< /note >}}  |


**Note:**
- Materialize Cloud does not support the `NOINHERIT` option for `ALTER
ROLE`.
- Materialize Cloud does not support the `LOGIN` and `SUPERUSER` attributes
  for `ALTER ROLE`.  See [Organization
  roles](/security/cloud/users-service-accounts/#organization-roles)
  instead.
- Materialize Cloud does not use role attributes to determine a role's
ability to alter top level objects such as databases and other roles.
Instead, Materialize Cloud uses system level privileges. See [GRANT
PRIVILEGE](../grant-privilege) for more details.


**Self-Managed:**
### Self-Managed

The following syntax is used to alter a role in Materialize Self-Managed.


```mzsql
ALTER ROLE <role_name>
[WITH]
  [ SUPERUSER | NOSUPERUSER ]
  [ LOGIN | NOLOGIN ]
  [ INHERIT | NOINHERIT ]
  [ PASSWORD <text> ]]
[SET <name> TO <value|DEFAULT> ]
[RESET <name>]
;

```

| Syntax element | Description |
| --- | --- |
| `INHERIT` | *Optional.* If specified, grants the role the ability to inherit privileges of other roles. *Default.*  |
| `LOGIN` | *Optional.* If specified, allows a role to login via the PostgreSQL or web endpoints  |
| `NOLOGIN` | *Optional.* If specified, prevents a role from logging in. This is the default behavior if `LOGIN` is not specified.  |
| `SUPERUSER` | *Optional.* If specified, grants the role superuser privileges.  |
| `NOSUPERUSER` | *Optional.* If specified, prevents the role from having superuser privileges. This is the default behavior if `SUPERUSER` is not specified.  |
| `PASSWORD` | ***Public Preview***  *Optional.* This feature may have minor stability issues. If specified, allows you to set a password for the role.  |
| `SET <name> TO <value\|DEFAULT>` | *Optional.* If specified, sets the configuration parameter for the role to the `<value>` or if the value specified is `DEFAULT`, the system's default (equivalent to `ALTER ROLE ... RESET <name>`).  To view the configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).  {{< note >}}  - Altering the configuration parameter for a role only affects **new sessions**. - Role configuration parameters are **not inherited**.  {{< /note >}}  |
| `RESET <name>` | *Optional.* If specified, resets the configuration parameter for the role to the system's default.  To view the configuration parameter defaults for a role, see [`mz_role_parameters`](/sql/system-catalog/mz_catalog#mz_role_parameters).  {{< note >}}  - Altering the configuration parameter for a role only affects **new sessions**. - Role configuration parameters are **not inherited**.  {{< /note >}}  |


**Note:**
- Self-Managed Materialize does not support the `NOINHERIT` option for
`ALTER ROLE`.
- With the exception of the `SUPERUSER` attribute, Self-Managed Materialize
does not use role attributes to determine a role's ability to create top
level objects such as databases and other roles. Instead, Self-Managed
Materialize uses system level privileges. See [GRANT
PRIVILEGE](../grant-privilege) for more details.




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

> **Warning:** Setting a NULL password removes the password.


```mzsql
ALTER ROLE rj PASSWORD NULL;
```

#### Changing a role's password (Self-Managed)

```mzsql
ALTER ROLE rj PASSWORD 'new_password';
```
## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.

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


**Swap with:**

### Swap with

To swap the name of a schema with that of another schema:



```mzsql
ALTER SCHEMA <schema1> SWAP WITH <schema2>;

```

| Syntax element | Description |
| --- | --- |
| `<schema1>` | The name of the schema you want to swap.  |
| `<schema2>` | The name of the other schema you want to swap with.  |



**Rename schema:**

### Rename schema

To rename a schema:



```mzsql
ALTER SCHEMA <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the schema.  |
| `<new_name>` | The new name of the schema.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner to:**

### Change owner to

To change the owner of a schema:



```mzsql
ALTER SCHEMA <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the schema you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the schema.  |
To change the owner of a schema, you must be the owner of the schema and have
membership in the `<new_owner_role>`. See also [Privileges](#privileges).







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

- Ownership of the schema.
- In addition,
  - To swap with another schema:
    - Ownership of the other schema
  - To change owners:
    - Role membership in `new_owner`.
    - `CREATE` privileges on the containing database.

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


**Change value:**

### Change value

To change the value of a secret:



```mzsql
ALTER SECRET [IF EXISTS] <name> AS <value>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The identifier of the secret you want to alter.  |
| `<value>` | The new value for the secret. The _value_ expression may not reference any relations, and must be implicitly castable to `bytea`.  |



**Rename:**

### Rename

To rename a secret:



```mzsql
ALTER SECRET [IF EXISTS] <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the secret.  |
| `<new_name>` | The new name of the secret.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a secret:



```mzsql
ALTER SECRET [IF EXISTS] <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the secret you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the secret.  |
To change the owner, you must be a current owner as well as have membership in
the `<new_owner_role>`.





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

- Ownership of the secret being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the secret is namespaced
  by a schema.

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


**Change sink from relation:**

### Change sink from relation

To change the relation you want to sink from:



```mzsql
ALTER SINK <name> SET FROM <relation_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>`  | The name of the sink you want to change.  |
| `<relation_name>`  | The name of the relation you want to sink from.  |



**Rename:**

### Rename

To rename a sink:



```mzsql
ALTER SINK <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the sink.  |
| `<new_name>` | The new name of the sink.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).





**Change owner:**

### Change owner

To change the owner of a sink:



```mzsql
ALTER SINK <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the sink you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the sink.  |
To change the owner, you must be a current owner as well as have membership
in the `<new_owner_role>`.




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

> **Note:** To select a consistent timestamp, Materialize must wait for the previous
> definition of the sink to emit results up until the oldest timestamp at which
> the contents of the new upstream relation are known. Attempting to `ALTER` an
> unhealthy sink that can't make progress will result in the command timing out.


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

- Ownership of the sink being altered.
- In addition,
  - To change the sink from relation:
    - `SELECT` privileges on the new relation being written out to an external system.
    - `CREATE` privileges on the cluster maintaining the sink.
    - `USAGE` privileges on all connections and secrets used in the sink definition.
    - `USAGE` privileges on the schemas that all connections and secrets in the
      statement are contained in.
  - To change owners:
    - Role membership in `new_owner`.
    - `CREATE` privileges on the containing schema if the sink is namespaced
  by a schema.

## Examples

### Alter sink

The following example alters a sink originally created from `matview_old` to use
`matview_new` instead.

That is, assume you have a Kafka sink `avro_sink` created from `matview_old`
(See [`CREATE SINK`:Kafka/Redpanda](/sql/create-sink/kafka/) for more
information):
```mzsql
CREATE SINK avro_sink
  FROM matview_old
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  KEY (key_col)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT
;

```


To have the sink read from `matview_new` instead of `matview_old`, you can
use `ALTER SINK` to change the `FROM <relation>`:

{{< note >}}
`matview_new` must be compatible with the previously published
schema. Otherwise, the `ALTER SINK` command will succeed, but the
subsequent execution of the sink will result in errors and will not be able
to make progress. See [Valid schema changes](#valid-schema-changes) for
details.
{{< /note >}}
```mzsql
ALTER SINK avro_sink
  SET FROM matview_new
;

```
{{< tip >}}

Because Materialize emits updates from the newly specified relation **only** if
they happen after the cutover timestamp, you might observe the following
scenarios:
- [Topic contains stale value for a
  key](#scenario-1-topic-contains-stale-value-for-a-key)
- [Topic is missing a key that exists in the new relation](#scenario-2-topic-is-missing-a-key-that-exists-in-the-new-relation)
- [Topic contains a key that does not exist in the new relation](#scenario-3-topic-contains-a-key-that-does-not-exist-in-the-new-relation)

For workaround, see [Example: Handle cutover scenarios](#handle-cutover-scenarios)
{{< /tip >}}


### Handle cutover scenarios

Because Materialize emits updates from the newly specified relation **only** if
they happen after the cutover timestamp, you might observe the following
scenarios:
- [Topic contains stale value for a
  key](#scenario-1-topic-contains-stale-value-for-a-key)
- [Topic is missing a key that exists in the new relation](#scenario-2-topic-is-missing-a-key-that-exists-in-the-new-relation)
- [Topic contains a key that does not exist in the new relation](#scenario-3-topic-contains-a-key-that-does-not-exist-in-the-new-relation)

To handle these scenarios, you can first alter sink to an intermediary
materialized view. The intermediary materialized view uses a temporary table
`switch` that switches the view's contents from old relation content to new
relation content. At the time of the switch, Materialize emits the diff of
the changes. Then, after the sink upper has advanced beyond the time of the
switch, you can `ALTER SINK` to the new relation (and remove the temporary
intermediary materialized view and table).


1. For example, create a table `switch` and a temporary materialized view
`transition` that contains either:
- the `matview_old` content if `switch.value` is `false`.
- the `matview_new` content if `switch.value` is `true`.

At first, the `switch.value` is `false`, so the `transition` materialized view contains the `matview_old` content.


   <no value>```mzsql
   CREATE TABLE switch (value bool);
   INSERT INTO switch VALUES (false); -- controls whether we want the new or the old materialized view.

   CREATE MATERIALIZED VIEW transition AS
   (SELECT matview_old.* FROM matview_old JOIN switch ON switch.value = false)
   UNION ALL
   (SELECT matview_new.* FROM matview_new JOIN switch ON switch.value = true)
   ;

   ```

1. `ALTER SINK` to use `transition`, which currently contains `matview_old` content:


   <no value>```mzsql
   ALTER SINK avro_sink SET FROM transition;

   ```

1. Update `switch.value` to `true`, which causes the `transition` materialized view to contain `matview_new` content:


   <no value>```mzsql
   UPDATE switch SET value = true;

   ```

1. Wait for the sink's upper frontier
([`mz_frontiers`](/sql/system-catalog/mz_internal/#mz_frontiers)) to advance
beyond the time of the switch update. Once advanced, alter sink to use
`matview_new`:


   <no value>```mzsql
   -- After sink upper has advanced beyond the time of the switch UPDATE.
   ALTER SINK avro_sink SET FROM matview_new;

   ```

1. Drop the `transition` materialized view and the `switch` table:


   <no value>```mzsql
   DROP MATERIALIZED VIEW transition;
   DROP TABLE switch;

   ```

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


**Add subsource:**

### Add subsource

To add the specified upstream table(s) to the specified PostgreSQL/MySQL/SQL Server source:



```mzsql
ALTER SOURCE [IF EXISTS] <name>
  ADD SUBSOURCE|TABLE <table> [AS <subsrc>] [, ...]
  [WITH (<options>)]
;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the PostgreSQL/MySQL/SQL Server source you want to alter.  |
| `<table>` | The upstream table to add to the source.  |
| **AS** `<subsrc>` | Optional. The name for the subsource in Materialize.  |
| **WITH (TEXT COLUMNS (`<col>` [, ...]))** | Optional. List of columns to decode as `text` for types that are unsupported in Materialize.  |


> **Note:** When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
> SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
> process for the new subsource. During this snapshotting, the data ingestion for
> the existing subsources for the same source is temporarily blocked. As such, if
> possible, you can resize the cluster to speed up the snapshotting process and
> once the process finishes, resize the cluster for steady-state.




**Rename:**

### Rename

To rename a source:



```mzsql
ALTER SOURCE <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the source you want to alter.  |
| `<new_name>` | The new name of the source.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a source:



```mzsql
ALTER SOURCE <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the source you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the source.  |
To change the owner of a source, you must be the owner of the source and have
membership in the `<new_owner_role>`. See also [Privileges](#privileges).



**(Re)Set retain history config:**

### (Re)Set retain history config

To set the retention history for a source:



```mzsql
ALTER SOURCE [IF EXISTS] <name> SET (RETAIN HISTORY [=] FOR <retention_period>);

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the source you want to alter.  |
| `<retention_period>` | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.  |


To reset the retention history to the default for a source:



```mzsql
ALTER SOURCE [IF EXISTS] <name>  RESET (RETAIN HISTORY);

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the source you want to alter.  |






## Context

### Adding subsources to a PostgreSQL/MySQL/SQL Server source

Note that using a combination of dropping and adding subsources lets you change
the schema of the PostgreSQL/MySQL/SQL Server tables that are ingested.

> **Important:** When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
> SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
> process for the new subsource. During this snapshotting, the data ingestion for
> the existing subsources for the same source is temporarily blocked. As such, if
> possible, you can resize the cluster to speed up the snapshotting process and
> once the process finishes, resize the cluster for steady-state.


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

> **Important:** When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
> SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
> process for the new subsource. During this snapshotting, the data ingestion for
> the existing subsources for the same source is temporarily blocked. As such, if
> possible, you can resize the cluster to speed up the snapshotting process and
> once the process finishes, resize the cluster for steady-state.


### Dropping subsources

To drop a subsource, use the [`DROP SOURCE`](/sql/drop-source/) command:

```mzsql
DROP SOURCE tbl_a, b CASCADE;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the source being altered.
- In addition, to change owners:
   - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the source is namespaced
  by a schema.

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

### Key configuration parameters

Name                                        | Default value             |  Description                                                          | Modifiable?
--------------------------------------------|---------------------------|-----------------------------------------------------------------------|--------------
`cluster`                                   | `quickstart`              | The current cluster.                                                  | Yes
`cluster_replica`                           |                           | The target cluster replica for `SELECT` queries.                      | Yes
`database`                                  | `materialize`             | The current database.                                                 | Yes
`search_path`                               | `public`                  | The schema search order for names that are not schema-qualified.      | Yes
`transaction_isolation`                     | `strict serializable`     | The transaction isolation level. For more information, see [Consistency guarantees](/overview/isolation-level/). <br/><br/> Accepts values: `serializable`, `strict serializable`. | Yes

### Other configuration parameters

Name                                        | Default value             |  Description                                                                                                                                                           | Modifiable?
--------------------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------
`allowed_cluster_replica_sizes`             | *Varies*                  | The allowed sizes when creating a new cluster replica.                                                                                                                 | [Contact support]
`application_name`                          |                           | The application name to be reported in statistics and logs. This parameter is typically set by an application upon connection to Materialize (e.g. `psql`).            | Yes
`auto_route_catalog_queries`                | `true`                    | Boolean flag indicating whether to force queries that depend only on system tables to run on the `mz_catalog_server` cluster for improved performance.                 | Yes
`client_encoding`                           | `UTF8`                    | The client's character set encoding. The only supported value is `UTF-8`.                                                                                              | Yes
`client_min_messages`                       | `notice`                  | The message levels that are sent to the client. <br/><br/> Accepts values: `debug5`, `debug4`, `debug3`, `debug2`, `debug1`, `log`, `notice`, `warning`, `error`. Each level includes all the levels that follow it. | Yes
`datestyle`                                 | `ISO, MDY`                | The display format for date and time values. The only supported value is `ISO, MDY`.                                                                                   | Yes
`emit_introspection_query_notice`           | `true`                    | Whether to print a notice when querying replica introspection relations.                                                                                               | Yes
`emit_timestamp_notice`                     | `false`                   | Boolean flag indicating whether to send a `notice` specifying query timestamps.                                                                                        | Yes
`emit_trace_id_notice`                      | `false`                   | Boolean flag indicating whether to send a `notice` specifying the trace ID, when available.                                                                            | Yes
`enable_rbac_checks`                        | `true`                    | Boolean flag indicating whether to apply RBAC checks before executing statements.                                                                                      | Yes
`enable_session_rbac_checks`                | `false`                   | Boolean flag indicating whether RBAC is enabled for the current session.                                                                                               | No
`extra_float_digits`                        | `3`                       | Boolean flag indicating whether to adjust the number of digits displayed for floating-point values.                                                                    | Yes
`failpoints`                                |                           | Allows failpoints to be dynamically activated.                                                                                                                         | No
`idle_in_transaction_session_timeout`       | `120s`                    | The maximum allowed duration that a session can sit idle in a transaction before being terminated. If this value is specified without units, it is taken as milliseconds (`ms`). A value of zero disables the timeout. | Yes
`integer_datetimes`                         | `true`                    | Boolean flag indicating whether the server uses 64-bit-integer dates and times.                                                                                        | No
`intervalstyle`                             | `postgres`                | The display format for interval values. The only supported value is `postgres`.                                                                                        | Yes
`is_superuser`                              |                           | Reports whether the current session is a _superuser_ with admin privileges.                                                                                            | No
`max_aws_privatelink_connections`           | `0`                       | The maximum number of AWS PrivateLink connections in the region, across all schemas.                                                                                   | [Contact support]
`max_clusters`                              | `10`                      | The maximum number of clusters in the region                                                                                                                           | [Contact support]
`max_connections`                           | `5000`                    | The maximum number of concurrent connections in the region                                                                                                             | [Contact support]
`max_credit_consumption_rate`               | `1024`                    | The maximum rate of credit consumption in a region. Credits are consumed based on the size of cluster replicas in use.                                                 | [Contact support]
`max_databases`                             | `1000`                    | The maximum number of databases in the region.                                                                                                                         | [Contact support]
`max_identifier_length`                     | `255`                     | The maximum length in bytes of object identifiers.                                                                                                                     | No
`max_kafka_connections`                     | `1000`                    | The maximum number of Kafka connections in the region, across all schemas.                                                                                             | [Contact support]
`max_mysql_connections`                     | `1000`                    | The maximum number of MySQL connections in the region, across all schemas.                                                                                             | [Contact support]
`max_objects_per_schema`                    | `1000`                    | The maximum number of objects in a schema.                                                                                                                             | [Contact support]
`max_postgres_connections`                  | `1000`                    | The maximum number of PostgreSQL connections in the region, across all schemas.                                                                                        | [Contact support]
`max_query_result_size`                     | `1073741824`              | The maximum size in bytes for a single query's result.                                                                                                                 | Yes
`max_replicas_per_cluster`                  | `5`                       | The maximum number of replicas of a single cluster                                                                                                                     | [Contact support]
`max_result_size`                           | `1 GiB`                   | The maximum size in bytes for a single query's result.                                                                                                                 | [Contact support]
`max_roles`                                 | `1000`                    | The maximum number of roles in the region.                                                                                                                             | [Contact support]
`max_schemas_per_database`                  | `1000`                    | The maximum number of schemas in a database.                                                                                                                           | [Contact support]
`max_secrets`                               | `100`                     | The maximum number of secrets in the region, across all schemas.                                                                                                       | [Contact support]
`max_sinks`                                 | `1000`                    | The maximum number of sinks in the region, across all schemas.                                                                                                         | [Contact support]
`max_sources`                               | `25`                      | The maximum number of sources in the region, across all schemas.                                                                                                       | [Contact support]
`max_tables`                                | `200`                     | The maximum number of tables in the region, across all schemas                                                                                                         | [Contact support]
`mz_version`                                | Version-dependent         | Shows the Materialize server version.                                                                                                                                  | No
`network_policy`                            | `default`                 | The default network policy for the region. | Yes
`real_time_recency`                         | `false`                   | Boolean flag indicating whether [real-time recency](/get-started/isolation-level/#real-time-recency) is enabled for the current session.                               | [Contact support]
`real_time_recency_timeout`                 | `10s`                     | Sets the maximum allowed duration of `SELECT` statements that actively use [real-time recency](/get-started/isolation-level/#real-time-recency). If this value is specified without units, it is taken as milliseconds (`ms`).                      | Yes
`server_version_num`                        | Version-dependent         | The PostgreSQL compatible server version as an integer.                                                                                                                | No
`server_version`                            | Version-dependent         | The PostgreSQL compatible server version.                                                                                                                              | No
`sql_safe_updates`                          | `false`                   | Boolean flag indicating whether to prohibit SQL statements that may be overly destructive.                                                                             | Yes
`standard_conforming_strings`               | `true`                    | Boolean flag indicating whether ordinary string literals (`'...'`) should treat backslashes literally. The only supported value is `true`.                             | Yes
`statement_timeout`                         | `10s`                     | The maximum allowed duration of the read portion of write operations; i.e., the `SELECT` portion of `INSERT INTO ... (SELECT ...)`; the `WHERE` portion of `UPDATE ... WHERE ...` and `DELETE FROM ... WHERE ...`. If this value is specified without units, it is taken as milliseconds (`ms`). | Yes
`timezone`                                  | `UTC`                     | The time zone for displaying and interpreting timestamps. The only supported value is `UTC`.                                                                           | Yes

[Contact support]: /support

## Privileges

The privileges required to execute this statement are:

- [_Superuser_ privileges](/security/cloud/users-service-accounts/#organization-roles)

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

### Key configuration parameters

Name                                        | Default value             |  Description                                                          | Modifiable?
--------------------------------------------|---------------------------|-----------------------------------------------------------------------|--------------
`cluster`                                   | `quickstart`              | The current cluster.                                                  | Yes
`cluster_replica`                           |                           | The target cluster replica for `SELECT` queries.                      | Yes
`database`                                  | `materialize`             | The current database.                                                 | Yes
`search_path`                               | `public`                  | The schema search order for names that are not schema-qualified.      | Yes
`transaction_isolation`                     | `strict serializable`     | The transaction isolation level. For more information, see [Consistency guarantees](/overview/isolation-level/). <br/><br/> Accepts values: `serializable`, `strict serializable`. | Yes

### Other configuration parameters

Name                                        | Default value             |  Description                                                                                                                                                           | Modifiable?
--------------------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------
`allowed_cluster_replica_sizes`             | *Varies*                  | The allowed sizes when creating a new cluster replica.                                                                                                                 | [Contact support]
`application_name`                          |                           | The application name to be reported in statistics and logs. This parameter is typically set by an application upon connection to Materialize (e.g. `psql`).            | Yes
`auto_route_catalog_queries`                | `true`                    | Boolean flag indicating whether to force queries that depend only on system tables to run on the `mz_catalog_server` cluster for improved performance.                 | Yes
`client_encoding`                           | `UTF8`                    | The client's character set encoding. The only supported value is `UTF-8`.                                                                                              | Yes
`client_min_messages`                       | `notice`                  | The message levels that are sent to the client. <br/><br/> Accepts values: `debug5`, `debug4`, `debug3`, `debug2`, `debug1`, `log`, `notice`, `warning`, `error`. Each level includes all the levels that follow it. | Yes
`datestyle`                                 | `ISO, MDY`                | The display format for date and time values. The only supported value is `ISO, MDY`.                                                                                   | Yes
`emit_introspection_query_notice`           | `true`                    | Whether to print a notice when querying replica introspection relations.                                                                                               | Yes
`emit_timestamp_notice`                     | `false`                   | Boolean flag indicating whether to send a `notice` specifying query timestamps.                                                                                        | Yes
`emit_trace_id_notice`                      | `false`                   | Boolean flag indicating whether to send a `notice` specifying the trace ID, when available.                                                                            | Yes
`enable_rbac_checks`                        | `true`                    | Boolean flag indicating whether to apply RBAC checks before executing statements.                                                                                      | Yes
`enable_session_rbac_checks`                | `false`                   | Boolean flag indicating whether RBAC is enabled for the current session.                                                                                               | No
`extra_float_digits`                        | `3`                       | Boolean flag indicating whether to adjust the number of digits displayed for floating-point values.                                                                    | Yes
`failpoints`                                |                           | Allows failpoints to be dynamically activated.                                                                                                                         | No
`idle_in_transaction_session_timeout`       | `120s`                    | The maximum allowed duration that a session can sit idle in a transaction before being terminated. If this value is specified without units, it is taken as milliseconds (`ms`). A value of zero disables the timeout. | Yes
`integer_datetimes`                         | `true`                    | Boolean flag indicating whether the server uses 64-bit-integer dates and times.                                                                                        | No
`intervalstyle`                             | `postgres`                | The display format for interval values. The only supported value is `postgres`.                                                                                        | Yes
`is_superuser`                              |                           | Reports whether the current session is a _superuser_ with admin privileges.                                                                                            | No
`max_aws_privatelink_connections`           | `0`                       | The maximum number of AWS PrivateLink connections in the region, across all schemas.                                                                                   | [Contact support]
`max_clusters`                              | `10`                      | The maximum number of clusters in the region                                                                                                                           | [Contact support]
`max_connections`                           | `5000`                    | The maximum number of concurrent connections in the region                                                                                                             | [Contact support]
`max_credit_consumption_rate`               | `1024`                    | The maximum rate of credit consumption in a region. Credits are consumed based on the size of cluster replicas in use.                                                 | [Contact support]
`max_databases`                             | `1000`                    | The maximum number of databases in the region.                                                                                                                         | [Contact support]
`max_identifier_length`                     | `255`                     | The maximum length in bytes of object identifiers.                                                                                                                     | No
`max_kafka_connections`                     | `1000`                    | The maximum number of Kafka connections in the region, across all schemas.                                                                                             | [Contact support]
`max_mysql_connections`                     | `1000`                    | The maximum number of MySQL connections in the region, across all schemas.                                                                                             | [Contact support]
`max_objects_per_schema`                    | `1000`                    | The maximum number of objects in a schema.                                                                                                                             | [Contact support]
`max_postgres_connections`                  | `1000`                    | The maximum number of PostgreSQL connections in the region, across all schemas.                                                                                        | [Contact support]
`max_query_result_size`                     | `1073741824`              | The maximum size in bytes for a single query's result.                                                                                                                 | Yes
`max_replicas_per_cluster`                  | `5`                       | The maximum number of replicas of a single cluster                                                                                                                     | [Contact support]
`max_result_size`                           | `1 GiB`                   | The maximum size in bytes for a single query's result.                                                                                                                 | [Contact support]
`max_roles`                                 | `1000`                    | The maximum number of roles in the region.                                                                                                                             | [Contact support]
`max_schemas_per_database`                  | `1000`                    | The maximum number of schemas in a database.                                                                                                                           | [Contact support]
`max_secrets`                               | `100`                     | The maximum number of secrets in the region, across all schemas.                                                                                                       | [Contact support]
`max_sinks`                                 | `1000`                    | The maximum number of sinks in the region, across all schemas.                                                                                                         | [Contact support]
`max_sources`                               | `25`                      | The maximum number of sources in the region, across all schemas.                                                                                                       | [Contact support]
`max_tables`                                | `200`                     | The maximum number of tables in the region, across all schemas                                                                                                         | [Contact support]
`mz_version`                                | Version-dependent         | Shows the Materialize server version.                                                                                                                                  | No
`network_policy`                            | `default`                 | The default network policy for the region. | Yes
`real_time_recency`                         | `false`                   | Boolean flag indicating whether [real-time recency](/get-started/isolation-level/#real-time-recency) is enabled for the current session.                               | [Contact support]
`real_time_recency_timeout`                 | `10s`                     | Sets the maximum allowed duration of `SELECT` statements that actively use [real-time recency](/get-started/isolation-level/#real-time-recency). If this value is specified without units, it is taken as milliseconds (`ms`).                      | Yes
`server_version_num`                        | Version-dependent         | The PostgreSQL compatible server version as an integer.                                                                                                                | No
`server_version`                            | Version-dependent         | The PostgreSQL compatible server version.                                                                                                                              | No
`sql_safe_updates`                          | `false`                   | Boolean flag indicating whether to prohibit SQL statements that may be overly destructive.                                                                             | Yes
`standard_conforming_strings`               | `true`                    | Boolean flag indicating whether ordinary string literals (`'...'`) should treat backslashes literally. The only supported value is `true`.                             | Yes
`statement_timeout`                         | `10s`                     | The maximum allowed duration of the read portion of write operations; i.e., the `SELECT` portion of `INSERT INTO ... (SELECT ...)`; the `WHERE` portion of `UPDATE ... WHERE ...` and `DELETE FROM ... WHERE ...`. If this value is specified without units, it is taken as milliseconds (`ms`). | Yes
`timezone`                                  | `UTC`                     | The time zone for displaying and interpreting timestamps. The only supported value is `UTC`.                                                                           | Yes

[Contact support]: /support

## Privileges

The privileges required to execute this statement are:

- [_Superuser_ privileges](/security/cloud/users-service-accounts/#organization-roles)

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


**Rename:**

### Rename

To rename a table:



```mzsql
ALTER TABLE <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the table you want to alter.  |
| `<new_name>` | The new name of the table.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a table:



```mzsql
ALTER TABLE <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the table you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the table.  |
To change the owner of a table, you must be the owner of the table and have
membership in the `<new_owner_role>`. See also [Privileges](#privileges).



**(Re)Set retain history config:**

### (Re)Set retain history config

To set the retention history for a user-populated table:



```mzsql
ALTER TABLE <name> SET (RETAIN HISTORY [=] FOR <retention_period>);

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the table you want to alter.  |
| `<retention_period>` | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.  |


To reset the retention history to the default for a user-populated table:



```mzsql
ALTER TABLE <name> RESET (RETAIN HISTORY);

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the table you want to alter.  |





## Privileges

The privileges required to execute this statement are:

- Ownership of the table being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the table is namespaced by
  a schema.


---

## ALTER TYPE


Use `ALTER TYPE` to:
- Rename a type.
- Change owner of a type.

## Syntax


**Rename:**

### Rename

To rename a type:



```mzsql
ALTER TYPE <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the type.  |
| `<new_name>` | The new name of the type.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a type:



```mzsql
ALTER TYPE <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the type you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the type.  |
To change the owner of a type, you must be the current owner and have
membership in the `<new_owner_role>`.






## Privileges

The privileges required to execute this statement are:

- Ownership of the type being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the type is namespaced by a
    schema.


---

## ALTER VIEW


Use `ALTER VIEW` to:
- Rename a view.
- Change owner of a view.

## Syntax


**Rename:**

### Rename

To rename a view:



```mzsql
ALTER VIEW <name> RENAME TO <new_name>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The current name of the view.  |
| `<new_name>` | The new name of the view.  |
See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).



**Change owner:**

### Change owner

To change the owner of a view:



```mzsql
ALTER VIEW <name> OWNER TO <new_owner_role>;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the view you want to change ownership of.  |
| `<new_owner_role>` | The new owner of the view.  |
To change the owner of a view, you must be the current owner and have
membership in the `<new_owner_role>`.






## Privileges

The privileges required to execute this statement are:

- Ownership of the view being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the view is namespaced by
  a schema.


---

## BEGIN


<p><a href="/sql/begin/" ><code>BEGIN</code></a> starts a transaction block. Once a transaction is started:</p>
<ul>
<li>
<p>Statements within the transaction are executed sequentially.</p>
</li>
<li>
<p>A transaction ends with either a <a href="/sql/commit/" ><code>COMMIT</code></a> or a
<a href="/sql/rollback/" ><code>ROLLBACK</code></a> statement.</p>
<ul>
<li>
<p>If all transaction statements succeed and a <a href="/sql/commit/" ><code>COMMIT</code></a> is
<a href="/sql/commit/#details" >issued</a>, all changes are saved.</p>
</li>
<li>
<p>If all transaction statements succeed and a <a href="/sql/rollback/" ><code>ROLLBACK</code></a>
is issued, all changes are discarded.</p>
</li>
<li>
<p>If an error occurs and either a <a href="/sql/commit/" ><code>COMMIT</code></a> or a
<a href="/sql/rollback/" ><code>ROLLBACK</code></a> is issued, all changes are discarded.</p>
</li>
</ul>
</li>
</ul>


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

> **Note:** - During the first query, a timestamp is chosen that is valid for all of the
>   objects referenced in the query. This timestamp will be used for all other
>   queries in the transaction.
> - The transaction will additionally hold back normal compaction of the objects,
>   potentially increasing memory usage for very long running transactions.


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

<p>An <strong>insert-only</strong> transaction only contains <a href="/sql/insert/" ><code>INSERT</code></a>
statements that insert into the <strong>same</strong> table.</p>
<p>On a successful <a href="/sql/commit/" ><code>COMMIT</code></a>, all statements from the
transaction are committed at the same timestamp.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">BEGIN</span><span class="p">;</span>
</span></span><span class="line"><span class="cl"><span class="k">INSERT</span> <span class="k">INTO</span> <span class="n">orders</span> <span class="k">VALUES</span> <span class="p">(</span><span class="mf">11</span><span class="p">,</span><span class="n">current_timestamp</span><span class="p">,</span><span class="s1">&#39;brownie&#39;</span><span class="p">,</span><span class="mf">10</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="c1">-- Subsequent INSERTs must write to sales_items table only
</span></span></span><span class="line"><span class="cl"><span class="c1">-- Otherwise, the COMMIT will error and roll back the transaction.
</span></span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">INSERT</span> <span class="k">INTO</span> <span class="n">orders</span> <span class="k">VALUES</span> <span class="p">(</span><span class="mf">11</span><span class="p">,</span><span class="n">current_timestamp</span><span class="p">,</span><span class="s1">&#39;chocolate cake&#39;</span><span class="p">,</span><span class="mf">1</span><span class="p">);</span>
</span></span><span class="line"><span class="cl"><span class="k">INSERT</span> <span class="k">INTO</span> <span class="n">orders</span> <span class="k">VALUES</span> <span class="p">(</span><span class="mf">11</span><span class="p">,</span><span class="n">current_timestamp</span><span class="p">,</span><span class="s1">&#39;chocolate chip cookie&#39;</span><span class="p">,</span><span class="mf">20</span><span class="p">);</span>
</span></span><span class="line"><span class="cl"><span class="k">COMMIT</span><span class="p">;</span>
</span></span></code></pre></div><p>If, within the transaction, a statement inserts into a table different from
that of the first statement, on <a href="/sql/commit/" ><code>COMMIT</code></a>, the transaction
encounters an <strong>internal ERROR</strong> and rolls back:</p>
<pre tabindex="0"><code class="language-none" data-lang="none">ERROR:  internal error, wrong set of locks acquired
</code></pre>

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


Use `COMMENT ON` to:

- Add a comment to an object.
- Update the comment to an object.
- Remove the comment from an object.

## Syntax



```mzsql
COMMENT ON <object_type> <name> IS <comment | NULL>;

```

| Syntax element | Description |
| --- | --- |
| `<object_type>` | The type of the object. Supported object types:  - `CLUSTER` - `CLUSTER REPLICA` - `COLUMN` - `CONNECTION` - `DATABASE` - `FUNCTION` - `INDEX` - `MATERIALIZED VIEW` - `NETWORK POLICY` - `ROLE` - `SCHEMA` - `SECRET` - `SINK` - `SOURCE` - `TABLE` - `TYPE` - `VIEW`  |
| `<name>` | The fully qualified name of the object.  |
| `<comment \| NULL>` | - The comment string for the object. - Use `NULL` to remove an existing comment.  |


## Details

`COMMENT ON` stores a comment about an object in the database. Each object can only have one
comment associated with it, so successive calls of `COMMENT ON` to a single object will overwrite
the previous comment.

To read the comment on an object you need to query the [mz_internal.mz_comments](/sql/system-catalog/mz_internal/#mz_comments)
catalog table.

## Privileges

The privileges required to execute this statement are:

- Ownership of the object being commented on (unless the object is a role).
- To comment on a role, you must have the `CREATEROLE` privilege.

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

<p><a href="/sql/begin/" ><code>BEGIN</code></a> starts a transaction block. Once a transaction is started:</p>
<ul>
<li>
<p>Statements within the transaction are executed sequentially.</p>
</li>
<li>
<p>A transaction ends with either a <a href="/sql/commit/" ><code>COMMIT</code></a> or a
<a href="/sql/rollback/" ><code>ROLLBACK</code></a> statement.</p>
<ul>
<li>
<p>If all transaction statements succeed and a <a href="/sql/commit/" ><code>COMMIT</code></a> is
<a href="/sql/commit/#details" >issued</a>, all changes are saved.</p>
</li>
<li>
<p>If all transaction statements succeed and a <a href="/sql/rollback/" ><code>ROLLBACK</code></a>
is issued, all changes are discarded.</p>
</li>
<li>
<p>If an error occurs and either a <a href="/sql/commit/" ><code>COMMIT</code></a> or a
<a href="/sql/rollback/" ><code>ROLLBACK</code></a> is issued, all changes are discarded.</p>
</li>
</ul>
</li>
</ul>


Transactions in Materialize are either **read-only** transactions or
**write-only** (more specifically, **insert-only**) transactions.

For a [write-only (i.e., insert-only)
transaction](/sql/begin/#write-only-transactions), all statements in the
transaction are committed at the same timestamp.

## Examples

### Commit a write-only transaction {#write-only-transactions}

In Materialize, write-only transactions are **insert-only** transactions.

<p>An <strong>insert-only</strong> transaction only contains <a href="/sql/insert/" ><code>INSERT</code></a>
statements that insert into the <strong>same</strong> table.</p>
<p>On a successful <a href="/sql/commit/" ><code>COMMIT</code></a>, all statements from the
transaction are committed at the same timestamp.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">BEGIN</span><span class="p">;</span>
</span></span><span class="line"><span class="cl"><span class="k">INSERT</span> <span class="k">INTO</span> <span class="n">orders</span> <span class="k">VALUES</span> <span class="p">(</span><span class="mf">11</span><span class="p">,</span><span class="n">current_timestamp</span><span class="p">,</span><span class="s1">&#39;brownie&#39;</span><span class="p">,</span><span class="mf">10</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="c1">-- Subsequent INSERTs must write to sales_items table only
</span></span></span><span class="line"><span class="cl"><span class="c1">-- Otherwise, the COMMIT will error and roll back the transaction.
</span></span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">INSERT</span> <span class="k">INTO</span> <span class="n">orders</span> <span class="k">VALUES</span> <span class="p">(</span><span class="mf">11</span><span class="p">,</span><span class="n">current_timestamp</span><span class="p">,</span><span class="s1">&#39;chocolate cake&#39;</span><span class="p">,</span><span class="mf">1</span><span class="p">);</span>
</span></span><span class="line"><span class="cl"><span class="k">INSERT</span> <span class="k">INTO</span> <span class="n">orders</span> <span class="k">VALUES</span> <span class="p">(</span><span class="mf">11</span><span class="p">,</span><span class="n">current_timestamp</span><span class="p">,</span><span class="s1">&#39;chocolate chip cookie&#39;</span><span class="p">,</span><span class="mf">20</span><span class="p">);</span>
</span></span><span class="line"><span class="cl"><span class="k">COMMIT</span><span class="p">;</span>
</span></span></code></pre></div><p>If, within the transaction, a statement inserts into a table different from
that of the first statement, on <a href="/sql/commit/" ><code>COMMIT</code></a>, the transaction
encounters an <strong>internal ERROR</strong> and rolls back:</p>
<pre tabindex="0"><code class="language-none" data-lang="none">ERROR:  internal error, wrong set of locks acquired
</code></pre>

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

> **Note:** The transaction will additionally hold back normal compaction of the objects,
> potentially increasing memory usage for very long running transactions.


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



```mzsql
COPY [INTO] <table_name> [ ( <column> [, ...] ) ] FROM STDIN
[[WITH] ( <option1> [=] <val1> [, ...] ] )]
;

```

| Syntax element | Description |
| --- | --- |
| `<table_name>` | Name of an existing table to copy data into.  |
| `( <column> [, ...] )` | If specified, correlate the inserted rows' columns to `<table_name>`'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column. If not specified, all columns must have data provided, and will be referenced using their order in the table. With a partial column list, all unreferenced columns will receive their default value.  |
| `[WITH] ( <option1> [=] <val1> [, ...] )` | The following `<options>` are supported for the `COPY FROM` operation: \| Name \|  Description \| \|------\|---------------\| \| `FORMAT` \|  Sets the input formatting method. Valid input formats are `TEXT` and `CSV`. For more information see [Text formatting](#text-formatting) and [CSV formatting](#csv-formatting).<br><br> Default: `TEXT`. \| `DELIMITER` \| A single-quoted one-byte character to use as the column delimiter. Must be different from `QUOTE`.<br><br> Default: A tab character in `TEXT`  format, a comma in `CSV` format. \| `NULL`  \| A single-quoted string that represents a _NULL_ value.<br><br> Default: `\N` (backslash-N) in text format, an unquoted empty string in CSV format. \| `QUOTE` \| _For `FORMAT CSV` only._ A single-quoted one-byte character that specifies the character to signal a quoted string, which may contain the `DELIMITER` value (without beginning new columns). To include the `QUOTE` character itself in column, wrap the column's value in the `QUOTE` character and prefix all instance of the value you want to literally interpret with the `ESCAPE` value. Must be different from `DELIMITER`.<br><br> Default: `"`. \| `ESCAPE` \| _For `FORMAT CSV` only._ A single-quoted string that specifies the character to allow instances of the `QUOTE` character to be parsed literally as part of a column's value. <br><br> Default: `QUOTE`'s value. \| `HEADER`  \| _For `FORMAT CSV` only._ A boolean that specifies that the file contains a header line with the names of each column in the file. The first line is ignored on input. <br><br> Default: `false`.  |


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

- `USAGE` privileges on the schema containing the table.
- `INSERT` privileges on the table.

[pg-copy-from]: https://www.postgresql.org/docs/14/sql-copy.html

## Limits

You can only copy up to 1 GiB of data at a time. If you need this limit increased, please [chat with our team](http://materialize.com/convert-account/).


---

## COPY TO


`COPY TO` outputs results from Materialize to standard output or object storage.
This command is useful to output [`SUBSCRIBE`](/sql/subscribe/) results
[to `stdout`](#copy-to-stdout), or perform [bulk exports to Amazon S3](#copy-to-s3).

## Syntax

**Copy to stdout:**
### Copy to `stdout`  {#copy-to-stdout}

Copying results to `stdout` is useful to output the stream of updates from a
[`SUBSCRIBE`](/sql/subscribe/) command in interactive SQL clients like `psql`.



```mzsql
COPY ( <query> ) TO STDOUT [WITH ( <option> = <val> )];

```

| Syntax element | Description |
| --- | --- |
| `<query>` | The [`SELECT`](/sql/select) or [`SUBSCRIBE`](/sql/subscribe) query whose results are copied.  |
| `WITH ( <option> = <val> )` | Optional. The following `<option>` are supported: \| Name \|  Description \| \|------\|---------------\| `FORMAT` \| Sets the output format. Valid output formats are: `TEXT`,`BINARY`, `CSV`.<br><br> Default: `TEXT`.  |



**Copy to Amazon S3 and S3 compatible services:**
### Copy to Amazon S3 and S3 compatible services {#copy-to-s3}

Copying results to Amazon S3 (or S3-compatible services) is useful to perform
tasks like periodic backups for auditing, or downstream processing in
analytical data warehouses like Snowflake, Databricks or BigQuery. For
step-by-step instructions, see the integration guide for [Amazon S3](/serve-results/s3/).

The `COPY TO` command is _one-shot_: every time you want to export results, you
must run the command. To automate exporting results on a regular basis, you can
set up scheduling, for example using a simple `cron`-like service or an
orchestration platform like Airflow or Dagster.



```mzsql
COPY <query> TO '<s3_uri>'
WITH (
  AWS CONNECTION = <connection_name>,
  FORMAT = <format>
  [, MAX FILE SIZE = <size> ]
);

```

| Syntax element | Description |
| --- | --- |
| `<query>` | The [`SELECT`](/sql/select) query whose results are copied.  |
| `<s3_uri>` | The unique resource identifier (URI) of the Amazon S3 bucket (and prefix) to store the output results in.  |
| `AWS CONNECTION = <connection_name>` | The name of the AWS connection to use in the `COPY TO` command. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#aws) documentation page.  |
| `FORMAT = '<format>'` | The file format to write. Valid formats are `'csv'` and `'parquet'`.  - {{< include-from-yaml data="examples/copy_to" name="csv-writer-settings" >}}  - {{< include-from-yaml data="examples/copy_to" name="parquet-writer-settings" >}}  |
| [`MAX FILE SIZE = <size>`] | Optional. Sets the approximate maximum file size (in bytes) of each file uploaded to the S3 bucket.  |






## Details

### Copy to S3: CSV {#copy-to-s3-csv}

#### Writer settings

<p>For <code>'csv'</code> format, Materialize writes CSV files using the following
writer settings:</p>
<table>
  <thead>
      <tr>
          <th>Setting</th>
          <th>Value</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>delimiter</td>
          <td><code>,</code></td>
      </tr>
      <tr>
          <td>quote</td>
          <td><code>&quot;</code></td>
      </tr>
      <tr>
          <td>escape</td>
          <td><code>&quot;</code></td>
      </tr>
      <tr>
          <td>header</td>
          <td><code>false</code></td>
      </tr>
  </tbody>
</table>

### Copy to S3: Parquet {#copy-to-s3-parquet}

#### Writer settings

<p>For <code>'parquet'</code> format, Materialize writes Parquet files that aim for
maximum compatibility with downstream systems. The following Parquet
writer settings are used:</p>
<table>
  <thead>
      <tr>
          <th>Setting</th>
          <th>Value</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>Writer version</td>
          <td>1.0</td>
      </tr>
      <tr>
          <td>Compression</td>
          <td><code>snappy</code></td>
      </tr>
      <tr>
          <td>Default column encoding</td>
          <td>Dictionary</td>
      </tr>
      <tr>
          <td>Fallback column encoding</td>
          <td>Plain</td>
      </tr>
      <tr>
          <td>Dictionary page encoding</td>
          <td>Plain</td>
      </tr>
      <tr>
          <td>Dictionary data page encoding</td>
          <td><code>RLE_DICTIONARY</code></td>
      </tr>
  </tbody>
</table>
<p>If you encounter issues trying to ingest Parquet files produced by
Materialize into your downstream systems, please <a href="/support/" >contact our
team</a>.</p>


#### Parquet data types

<p>When using the <code>parquet</code> format, Materialize converts the values in the
result set to <a href="https://arrow.apache.org/docs/index.html" >Apache Arrow</a>,
and then serializes this Arrow representation to Parquet. The Arrow schema is
embedded in the Parquet file metadata and allows reconstructing the Arrow
representation using a compatible reader.</p>
<p>Materialize also includes <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#metadata" >Parquet <code>LogicalType</code> annotations</a>
where possible. However, many newer <code>LogicalType</code> annotations are not supported
in the 1.0 writer version.</p>
<p>Materialize also embeds its own type information into the Apache Arrow schema.
The field metadata in the schema contains an <code>ARROW:extension:name</code> annotation
to indicate the Materialize native type the field originated from.</p>
<table>
  <thead>
      <tr>
          <th>Materialize type</th>
          <th>Arrow extension name</th>
          <th><a href="https://github.com/apache/arrow/blob/main/format/Schema.fbs" >Arrow type</a></th>
          <th><a href="https://parquet.apache.org/docs/file-format/types/" >Parquet primitive type</a></th>
          <th><a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md" >Parquet logical type</a></th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/sql/types/integer/#bigint-info" ><code>bigint</code></a></td>
          <td><code>materialize.v1.bigint</code></td>
          <td><code>int64</code></td>
          <td><code>INT64</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/boolean/" ><code>boolean</code></a></td>
          <td><code>materialize.v1.boolean</code></td>
          <td><code>bool</code></td>
          <td><code>BOOLEAN</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/bytea/" ><code>bytea</code></a></td>
          <td><code>materialize.v1.bytea</code></td>
          <td><code>large_binary</code></td>
          <td><code>BYTE_ARRAY</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/date/" ><code>date</code></a></td>
          <td><code>materialize.v1.date</code></td>
          <td><code>date32</code></td>
          <td><code>INT32</code></td>
          <td><code>DATE</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/float/#double-precision-info" ><code>double precision</code></a></td>
          <td><code>materialize.v1.double</code></td>
          <td><code>float64</code></td>
          <td><code>DOUBLE</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/integer/#integer-info" ><code>integer</code></a></td>
          <td><code>materialize.v1.integer</code></td>
          <td><code>int32</code></td>
          <td><code>INT32</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/jsonb/" ><code>jsonb</code></a></td>
          <td><code>materialize.v1.jsonb</code></td>
          <td><code>large_utf8</code></td>
          <td><code>BYTE_ARRAY</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/map/" ><code>map</code></a></td>
          <td><code>materialize.v1.map</code></td>
          <td><code>map</code> (<code>struct</code> with fields <code>keys</code> and <code>values</code>)</td>
          <td>Nested</td>
          <td><code>MAP</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/list/" ><code>list</code></a></td>
          <td><code>materialize.v1.list</code></td>
          <td><code>list</code></td>
          <td>Nested</td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/numeric/" ><code>numeric</code></a></td>
          <td><code>materialize.v1.numeric</code></td>
          <td><code>decimal128[38, 10 or max-scale]</code></td>
          <td><code>FIXED_LEN_BYTE_ARRAY</code></td>
          <td><code>DECIMAL</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/float/#real-info" ><code>real</code></a></td>
          <td><code>materialize.v1.real</code></td>
          <td><code>float32</code></td>
          <td><code>FLOAT</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/integer/#smallint-info" ><code>smallint</code></a></td>
          <td><code>materialize.v1.smallint</code></td>
          <td><code>int16</code></td>
          <td><code>INT32</code></td>
          <td><code>INT(16, true)</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/text/" ><code>text</code></a></td>
          <td><code>materialize.v1.text</code></td>
          <td><code>utf8</code> or <code>large_utf8</code></td>
          <td><code>BYTE_ARRAY</code></td>
          <td><code>STRING</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/time/" ><code>time</code></a></td>
          <td><code>materialize.v1.time</code></td>
          <td><code>time64[nanosecond]</code></td>
          <td><code>INT64</code></td>
          <td><code>TIME[isAdjustedToUTC = false, unit = NANOS]</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/uint/#uint2-info" ><code>uint2</code></a></td>
          <td><code>materialize.v1.uint2</code></td>
          <td><code>uint16</code></td>
          <td><code>INT32</code></td>
          <td><code>INT(16, false)</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/uint/#uint4-info" ><code>uint4</code></a></td>
          <td><code>materialize.v1.uint4</code></td>
          <td><code>uint32</code></td>
          <td><code>INT32</code></td>
          <td><code>INT(32, false)</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/uint/#uint8-info" ><code>uint8</code></a></td>
          <td><code>materialize.v1.uint8</code></td>
          <td><code>uint64</code></td>
          <td><code>INT64</code></td>
          <td><code>INT(64, false)</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/timestamp/#timestamp-info" ><code>timestamp</code></a></td>
          <td><code>materialize.v1.timestamp</code></td>
          <td><code>time64[microsecond]</code></td>
          <td><code>INT64</code></td>
          <td><code>TIMESTAMP[isAdjustedToUTC = false, unit = MICROS]</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/timestamp/#timestamp-with-time-zone-info" ><code>timestamp with time zone</code></a></td>
          <td><code>materialize.v1.timestampz</code></td>
          <td><code>time64[microsecond]</code></td>
          <td><code>INT64</code></td>
          <td><code>TIMESTAMP[isAdjustedToUTC = true, unit = MICROS]</code></td>
      </tr>
      <tr>
          <td><a href="/sql/types/array/" >Arrays</a> (<code>[]</code>)</td>
          <td><code>materialize.v1.array</code></td>
          <td><code>struct</code> with <code>list</code> field <code>items</code> and <code>uint8</code> field <code>dimensions</code></td>
          <td>Nested</td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/uuid/" ><code>uuid</code></a></td>
          <td><code>materialize.v1.uuid</code></td>
          <td><code>fixed_size_binary(16)</code></td>
          <td><code>FIXED_LEN_BYTE_ARRAY</code></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/oid/" ><code>oid</code></a></td>
          <td>Unsupported</td>
          <td></td>
          <td></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/interval/" ><code>interval</code></a></td>
          <td>Unsupported</td>
          <td></td>
          <td></td>
          <td></td>
      </tr>
      <tr>
          <td><a href="/sql/types/record/" ><code>record</code></a></td>
          <td>Unsupported</td>
          <td></td>
          <td></td>
          <td></td>
      </tr>
  </tbody>
</table>


## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `SELECT` privileges on all relations in the query.
    - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
      execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
      granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

## Examples

### Copy to stdout {#copy-to-stdout-examples}

```mzsql
COPY (SUBSCRIBE some_view) TO STDOUT WITH (FORMAT binary);
```

### Copy to S3 {#copy-to-s3-examples}

#### File format Parquet

```mzsql
COPY some_view TO 's3://mz-to-snow/parquet/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'parquet'
  );
```

<p>For <code>'parquet'</code> format, Materialize writes Parquet files that aim for
maximum compatibility with downstream systems. The following Parquet
writer settings are used:</p>
<table>
  <thead>
      <tr>
          <th>Setting</th>
          <th>Value</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>Writer version</td>
          <td>1.0</td>
      </tr>
      <tr>
          <td>Compression</td>
          <td><code>snappy</code></td>
      </tr>
      <tr>
          <td>Default column encoding</td>
          <td>Dictionary</td>
      </tr>
      <tr>
          <td>Fallback column encoding</td>
          <td>Plain</td>
      </tr>
      <tr>
          <td>Dictionary page encoding</td>
          <td>Plain</td>
      </tr>
      <tr>
          <td>Dictionary data page encoding</td>
          <td><code>RLE_DICTIONARY</code></td>
      </tr>
  </tbody>
</table>
<p>If you encounter issues trying to ingest Parquet files produced by
Materialize into your downstream systems, please <a href="/support/" >contact our
team</a>.</p>


See also [Copy to S3: Parquet Data Types](#parquet-data-types).

#### File format CSV

```mzsql
COPY some_view TO 's3://mz-to-snow/csv/'
WITH (
    AWS CONNECTION = aws_role_assumption,
    FORMAT = 'csv'
  );
```

<p>For <code>'csv'</code> format, Materialize writes CSV files using the following
writer settings:</p>
<table>
  <thead>
      <tr>
          <th>Setting</th>
          <th>Value</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>delimiter</td>
          <td><code>,</code></td>
      </tr>
      <tr>
          <td>quote</td>
          <td><code>&quot;</code></td>
      </tr>
      <tr>
          <td>escape</td>
          <td><code>&quot;</code></td>
      </tr>
      <tr>
          <td>header</td>
          <td><code>false</code></td>
      </tr>
  </tbody>
</table>


## Related pages

- [`CREATE CONNECTION`](/sql/create-connection)
- Integration guides:
  - [Amazon S3](/serve-results/s3/)
  - [Snowflake (via S3)](/serve-results/snowflake/)


---

## CREATE CLUSTER


`CREATE CLUSTER` creates a new [cluster](/concepts/clusters/).

## Syntax



```mzsql
CREATE CLUSTER <cluster_name> (
    SIZE = <text>
    [, REPLICATION FACTOR = <int>]
    [, MANAGED = <bool>]
    [, SCHEDULE = MANUAL|ON REFRESH(...)]
);

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | A name for the cluster.  |
| `SIZE` | The size of the resource allocations for the cluster.  {{< yaml-list column="Cluster size" data="m1_cluster_sizing" numColumns="3" >}}  See [Size](#size) for details as well as legacy sizes available.  |
| `REPLICATION FACTOR` | Optional. The number of replicas to provision for the cluster. See [Replication factor](#replication-factor) for details.  Default: `1`  |
| `MANAGED` | Optional. Whether to automatically manage the cluster's replicas based on the configured size and replication factor.  <a name="unmanaged-clusters"></a>  Specify `FALSE` to create an **unmanaged** cluster. With unmanaged clusters, you need to manually manage the cluster's replicas using the the [`CREATE CLUSTER REPLICA`](/sql/create-cluster-replica) and [`DROP CLUSTER REPLICA`](/sql/drop-cluster-replica) commands. When creating an unmanaged cluster, you must specify the `REPLICAS` option as well.  {{< tip >}} When getting started with Materialize, we recommend starting with managed clusters. {{</ tip >}}  Default: `TRUE`  |
| `SCHEDULE` | Optional. The [scheduling type](#scheduling) for the cluster. Valid values are: - `MANUAL` - `ON REFRESH`  Default: `MANUAL`  |


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


**M.1 Clusters:**

> **Note:** The values set forth in the table are solely for illustrative purposes.
> Materialize reserves the right to change the capacity at any time. As such, you
> acknowledge and agree that those values in this table may change at any time,
> and you should not rely on these values for any capacity planning.




| Cluster size | Compute Credits/Hour | Total Capacity | Notes |
| --- | --- | --- | --- |
| <strong>M.1-nano</strong> | 0.75 | 26 GiB |  |
| <strong>M.1-micro</strong> | 1.5 | 53 GiB |  |
| <strong>M.1-xsmall</strong> | 3 | 106 GiB |  |
| <strong>M.1-small</strong> | 6 | 212 GiB |  |
| <strong>M.1-medium</strong> | 9 | 318 GiB |  |
| <strong>M.1-large</strong> | 12 | 424 GiB |  |
| <strong>M.1-1.5xlarge</strong> | 18 | 636 GiB |  |
| <strong>M.1-2xlarge</strong> | 24 | 849 GiB |  |
| <strong>M.1-3xlarge</strong> | 36 | 1273 GiB |  |
| <strong>M.1-4xlarge</strong> | 48 | 1645 GiB |  |
| <strong>M.1-8xlarge</strong> | 96 | 3290 GiB |  |
| <strong>M.1-16xlarge</strong> | 192 | 6580 GiB | Available upon request |
| <strong>M.1-32xlarge</strong> | 384 | 13160 GiB | Available upon request |
| <strong>M.1-64xlarge</strong> | 768 | 26320 GiB | Available upon request |
| <strong>M.1-128xlarge</strong> | 1536 | 52640 GiB | Available upon request |



**Legacy cc Clusters:**

Materialize offers the following legacy cc cluster sizes:

> **Tip:** In most cases, you **should not** use legacy sizes. [M.1 sizes](#size)
> offer better performance per credit for nearly all workloads. We recommend using
> M.1 sizes for all new clusters, and recommend migrating existing
> legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
> customers during the transition period as we move to deprecate legacy sizes.
> The legacy size information is provided for completeness.


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

> **Warning:** The values in the `mz_cluster_replica_sizes` table may change at any
> time. You should not rely on them for any kind of capacity planning.


Clusters of larger sizes can process data faster and handle larger data volumes.

**Legacy t-shirt Clusters:**

Materialize also offers some legacy t-shirt cluster sizes for upsert sources.

> **Tip:** In most cases, you **should not** use legacy t-shirt sizes. [M.1 sizes](#size)
> offer better performance per credit for nearly all workloads. We recommend using
> M.1 sizes for all new clusters, and recommend migrating existing
> legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
> customers during the transition period as we move to deprecate legacy sizes.
> The legacy size information is provided for completeness.




<blockquote>
<p><strong>Warning:</strong> Materialize regions that were enabled after 15 April 2024 do not have access
to legacy sizes.</p>
</blockquote>



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




See also:

- [M.1 to cc size mapping](/sql/m1-cc-mapping/).

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

> **Note:** A common misconception is that increasing a cluster's replication
> factor will increase its capacity for work. This is not the case. Increasing
> the replication factor increases the **fault tolerance** of the cluster, not its
> capacity for work. Replicas are exact copies of one another: each replica must
> do exactly the same work (i.e., maintain the same dataflows and process the same
> queries) as all the other replicas of the cluster.
> To increase a cluster's capacity, you should instead increase the cluster's
> [size](#size).


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

- `CREATECLUSTER` privileges on the system.

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

> **Tip:** When getting started with Materialize, we recommend starting with managed
> clusters.


## Syntax



```mzsql
CREATE CLUSTER REPLICA <cluster_name>.<replica_name> (
    SIZE = <text>
);

```

| Syntax element | Description |
| --- | --- |
| `<cluster_name>` | The cluster you want to attach a replica to.  |
| `<replica_name>` | A name for this replica.  |
| `SIZE` | The size of the resource allocations for the cluster.  {{< yaml-list column="Cluster size" data="m1_cluster_sizing" numColumns="3" >}}  See [Size](#size) for details as well as legacy sizes available.  |


## Details

### Size

The `SIZE` option for replicas is identical to the [`SIZE` option for
clusters](/sql/create-cluster/#size) option, except that the size applies only
to the new replica.


**M.1 Clusters:**

> **Note:** The values set forth in the table are solely for illustrative purposes.
> Materialize reserves the right to change the capacity at any time. As such, you
> acknowledge and agree that those values in this table may change at any time,
> and you should not rely on these values for any capacity planning.




| Cluster size | Compute Credits/Hour | Total Capacity | Notes |
| --- | --- | --- | --- |
| <strong>M.1-nano</strong> | 0.75 | 26 GiB |  |
| <strong>M.1-micro</strong> | 1.5 | 53 GiB |  |
| <strong>M.1-xsmall</strong> | 3 | 106 GiB |  |
| <strong>M.1-small</strong> | 6 | 212 GiB |  |
| <strong>M.1-medium</strong> | 9 | 318 GiB |  |
| <strong>M.1-large</strong> | 12 | 424 GiB |  |
| <strong>M.1-1.5xlarge</strong> | 18 | 636 GiB |  |
| <strong>M.1-2xlarge</strong> | 24 | 849 GiB |  |
| <strong>M.1-3xlarge</strong> | 36 | 1273 GiB |  |
| <strong>M.1-4xlarge</strong> | 48 | 1645 GiB |  |
| <strong>M.1-8xlarge</strong> | 96 | 3290 GiB |  |
| <strong>M.1-16xlarge</strong> | 192 | 6580 GiB | Available upon request |
| <strong>M.1-32xlarge</strong> | 384 | 13160 GiB | Available upon request |
| <strong>M.1-64xlarge</strong> | 768 | 26320 GiB | Available upon request |
| <strong>M.1-128xlarge</strong> | 1536 | 52640 GiB | Available upon request |




**Legacy cc Clusters:**

Materialize offers the following legacy cc cluster sizes:

> **Tip:** In most cases, you **should not** use legacy sizes. [M.1 sizes](#size)
> offer better performance per credit for nearly all workloads. We recommend using
> M.1 sizes for all new clusters, and recommend migrating existing
> legacy-sized clusters to M.1 sizes. Materialize is committed to supporting
> customers during the transition period as we move to deprecate legacy sizes.
> The legacy size information is provided for completeness.


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

> **Warning:** The values in the `mz_cluster_replica_sizes` table may change at any
> time. You should not rely on them for any kind of capacity planning.


Clusters of larger sizes can process data faster and handle larger data volumes.



See also:

- [M.1 to cc size mapping](/sql/m1-cc-mapping/).

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

- Ownership of the cluster.

## See also

- [`DROP CLUSTER REPLICA`]

[AWS availability zone ID]: https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html
[`DROP CLUSTER REPLICA`]: /sql/drop-cluster-replica


---

## CREATE CONNECTION


[//]: # "TODO: This page could be broken up."

A connection describes how to connect and authenticate to an external system you
want Materialize to read from or write to. Once created, a connection
is **reusable** across multiple [`CREATE SOURCE`](/sql/create-source) and
[`CREATE SINK`](/sql/create-sink) statements.

To use credentials that contain sensitive information (like passwords and SSL
keys) in a connection, you must first [create secrets](/sql/create-secret) to
securely store each credential in Materialize's secret management system.
Credentials that are generally not sensitive (like usernames and SSL
certificates) can be specified as plain `text`, or also stored as secrets.

> **Note:** Connections using AWS PrivateLink is for Materialize Cloud only.



## Source and sink connections

### AWS

An Amazon Web Services (AWS) connection provides Materialize with access to an
Identity and Access Management (IAM) user or role in your AWS account. You can
use AWS connections to perform [bulk exports to Amazon S3](/serve-results/s3/),
perform [authentication with an Amazon MSK cluster](#kafka-aws-connection), or
perform [authentication with an Amazon RDS MySQL database](#mysql-aws-connection).



```mzsql
CREATE CONNECTION <connection_name> TO AWS (
    ENDPOINT = '<endpoint>',
    REGION = '<region>',
    ACCESS KEY ID = { '<access_key_id>' | SECRET <secret_name> },
    SECRET ACCESS KEY = SECRET <secret_name>,
    SESSION TOKEN = { '<session_token>' | SECRET <secret_name> },
    ASSUME ROLE ARN = '<role_arn>',
    ASSUME ROLE SESSION NAME = '<session_name>'
)
[WITH (<with_options>)];

```

| Syntax element | Description |
| --- | --- |
| `<connection_name>` | A name for the connection.  |
| `ENDPOINT` | *Value:* `text`  *Advanced.* Override the default AWS endpoint URL. Allows targeting S3-compatible services like MinIO.  |
| `REGION` | *Value:* `text`  *For Materialize Cloud only* The AWS region to connect to. Defaults to the current Materialize region.  |
| `ACCESS KEY ID` | *Value:* secret or `text`  The access key ID to connect with. Triggers credentials-based authentication.  **Warning!** Use of credentials-based authentication is deprecated. AWS strongly encourages the use of role assumption-based authentication instead.  |
| `SECRET ACCESS KEY` | *Value:* secret  The secret access key corresponding to the specified access key ID.  Required and only valid when `ACCESS KEY ID` is specified.  |
| `SESSION TOKEN` | *Value:* secret or `text`  The session token corresponding to the specified access key ID.  Only valid when `ACCESS KEY ID` is specified.  |
| `ASSUME ROLE ARN` | *Value:* `text`  The Amazon Resource Name (ARN) of the IAM role to assume. Triggers role assumption-based authentication.  |
| `ASSUME ROLE SESSION NAME` | *Value:* `text`  The session name to use when assuming the role.  Only valid when `ASSUME ROLE ARN` is specified.  |
| `WITH (<with_options>)` | The following `<with_options>` are supported:  \| Field \| Value \| Description \| \|-------\|-------\|-------------\| \| `VALIDATE` \| `boolean` \| Whether [connection validation](#connection-validation) should be performed on connection creation. Default: `false`. \|  |


#### Permissions {#aws-permissions}

> **Warning:** Failing to constrain the external ID in your role trust policy will allow
> other Materialize customers to assume your role and use AWS privileges you
> have granted the role!


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


**Role assumption:**

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


**Credentials:**
> **Warning:** Use of credentials-based authentication is deprecated.  AWS strongly encourages
> the use of role assumption-based authentication instead.


To create an AWS connection that uses static access key credentials:

```mzsql
CREATE SECRET aws_secret_access_key AS '...';
CREATE CONNECTION aws_credentials TO AWS (
    ACCESS KEY ID = 'ASIAV2KIV5LPTG6HGXG6',
    SECRET ACCESS KEY = SECRET aws_secret_access_key
);
```




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



```mzsql
CREATE CONNECTION <connection_name> TO KAFKA (
    BROKER '<broker>' | BROKERS ('<broker1>', '<broker2>', ...),
    SECURITY PROTOCOL = { 'PLAINTEXT' | 'SSL' | 'SASL_PLAINTEXT' | 'SASL_SSL' },
    SASL MECHANISMS = { 'PLAIN' | 'SCRAM-SHA-256' | 'SCRAM-SHA-512' },
    SASL USERNAME = { '<username>' | SECRET <secret_name> },
    SASL PASSWORD = SECRET <secret_name>,
    SSL CERTIFICATE AUTHORITY = { '<pem>' | SECRET <secret_name> },
    SSL CERTIFICATE = { '<pem>' | SECRET <secret_name> },
    SSL KEY = SECRET <secret_name>,
    SSH TUNNEL = <ssh_connection_name>,
    AWS CONNECTION = <aws_connection_name>,
    AWS PRIVATELINK <privatelink_connection_name> (PORT <port>),
    PROGRESS TOPIC = '<topic_name>',
    PROGRESS TOPIC REPLICATION FACTOR = <int>
)
[WITH (<with_options>)];

```

| Syntax element | Description |
| --- | --- |
| `<connection_name>` | A name for the connection.  |
| `BROKER` / `BROKERS` | *Value:* `text` / `text[]`  The Kafka bootstrap server(s). Exactly one of `BROKER`, `BROKERS`, or `AWS PRIVATELINK` must be specified.  |
| `SECURITY PROTOCOL` | *Value:* `text`  The security protocol to use: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`.  Defaults to `SASL_SSL` if any `SASL ...` options are specified or if the `AWS CONNECTION` option is specified, otherwise defaults to `SSL`.  |
| `SASL MECHANISMS` | *Value:* `text`  The SASL mechanism to use for authentication: `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. Despite the name, this option only allows a single mechanism to be specified.  Required if the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`. Cannot be specified if `AWS CONNECTION` is specified.  |
| `SASL USERNAME` / `SASL PASSWORD` | *Value:* secret or `text` / secret  Your SASL credentials.  Required and only valid when the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.  |
| `SSL CERTIFICATE AUTHORITY` | *Value:* secret or `text`  The certificate authority (CA) certificate in PEM format. Used to validate the brokers' TLS certificates. If unspecified, uses the system's default CA certificates.  Only valid when the security protocol is `SSL` or `SASL_SSL`.  |
| `SSL CERTIFICATE` / `SSL KEY` | *Value:* secret or `text` / secret  Your TLS certificate and key in PEM format for SSL client authentication. If unspecified, no client authentication is performed.  Only valid when the security protocol is `SSL` or `SASL_SSL`.  |
| `SSH TUNNEL` | *Value:* object name  The name of an [SSH tunnel connection](#ssh-tunnel) to route network traffic through by default.  |
| `AWS CONNECTION` | <a name="kafka-aws-connection"></a> *Value:* object name  The name of an [AWS connection](#aws) to use when performing IAM authentication with an Amazon MSK cluster.  Only valid if the security protocol is `SASL_PLAINTEXT` or `SASL_SSL`.  |
| `AWS PRIVATELINK` | *Value:* object name  The name of an [AWS PrivateLink connection](#aws-privatelink) to route network traffic through.  Exactly one of `BROKER`, `BROKERS`, or `AWS PRIVATELINK` must be specified.  |
| `PROGRESS TOPIC` | *Value:* `text`  The name of a topic that Kafka sinks can use to track internal consistency metadata.  Default: `_materialize-progress-{REGION ID}-{CONNECTION ID}`.  |
| `PROGRESS TOPIC REPLICATION FACTOR` | *Value:* `int`  The partition count to use when creating the progress topic (if the Kafka topic does not already exist).  Default: Broker's default.  |
| `WITH (<with_options>)` | The following `<with_options>` are supported:  \| Field \| Value \| Description \| \|-------\|-------\|-------------\| \| `VALIDATE` \| `boolean` \| Whether [connection validation](#connection-validation) should be performed on connection creation. Default: `true`. \|  |


To connect to a Kafka cluster with multiple bootstrap servers, use the `BROKERS`
option:

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS ('broker1:9092', 'broker2:9092')
);
```

#### Security protocol examples {#kafka-auth}


**PLAINTEXT:**
> **Warning:** It is insecure to use the `PLAINTEXT` security protocol unless
> you are using a [network security connection](#network-security-connections)
> to tunnel into a private network, as shown below.

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.prd.cloud.redpanda.com:9092',
    SECURITY PROTOCOL = 'PLAINTEXT',
    SSH TUNNEL ssh_connection
);
```


**SSL:**
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
> **Warning:** It is insecure to use TLS encryption with no authentication unless
> you are using a [network security connection](#network-security-connections)
> to tunnel into a private network as shown below.

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


**SASL_PLAINTEXT:**
> **Warning:** It is insecure to use the `SASL_PLAINTEXT` security protocol unless
> you are using a [network security connection](#network-security-connections)
> to tunnel into a private network, as shown below.


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


**SASL_SSL:**
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


**AWS IAM:**

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



#### Network security {#kafka-network-security}

If your Kafka broker is not exposed to the public internet, you can tunnel the
connection through an AWS PrivateLink service (Materialize Cloud) or an
SSH bastion host.


**AWS PrivateLink (Materialize Cloud):**

> **Note:** Connections using AWS PrivateLink is for Materialize Cloud only.



Depending on the hosted service you are connecting to, you might need to specify
a PrivateLink connection [per advertised broker](#kafka-privatelink-syntax)
(e.g. Amazon MSK), or a single [default PrivateLink connection](#kafka-privatelink-default) (e.g. Redpanda Cloud).

##### Broker connection syntax {#kafka-privatelink-syntax}

> **Warning:** If your Kafka cluster advertises brokers that are not specified
> in the `BROKERS` clause, Materialize will attempt to connect to
> those brokers without any tunneling.




```mzsql
CREATE CONNECTION <connection_name> TO KAFKA (
    BROKERS (
        '<broker1>:<port1>' USING <tunnel_option>,
        '<broker2>:<port2>' USING <tunnel_option>
    ),
    ...
);

```

| Syntax element | Description |
| --- | --- |
| `<broker>:<port>` | The hostname and port of each Kafka broker.  |
| `USING <tunnel_option>` | Specifies how to connect to each broker (e.g., via AWS PrivateLink or SSH tunnel).  |


##### `kafka_broker`



```mzsql
'<broker>:<port>' USING AWS PRIVATELINK <connection_name> (
    AVAILABILITY ZONE = '<az_id>',
    PORT = <port>
)

```

| Syntax element | Description |
| --- | --- |
| `AWS PRIVATELINK <connection_name>` | The name of an AWS PrivateLink connection through which network traffic for this broker should be routed.  |
| `AVAILABILITY ZONE` | The ID of the availability zone of the AWS PrivateLink service in which the broker is accessible.  |
| `PORT` | The port of the AWS PrivateLink service to connect to.  |


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



```mzsql
CREATE CONNECTION <connection_name> TO KAFKA (
    AWS PRIVATELINK <privatelink_connection_name> (PORT <port>),
    ...
);

```

| Syntax element | Description |
| --- | --- |
| `AWS PRIVATELINK <privatelink_connection_name>` | The name of an AWS PrivateLink connection through which network traffic should be routed.  |
| `PORT` | The port of the AWS PrivateLink service to connect to.  |


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


**SSH tunnel:**

##### Syntax {#kafka-ssh-syntax}

> **Warning:** If you do not specify a default `SSH TUNNEL` and your Kafka
> cluster advertises brokers that are not listed in the `BROKERS` clause,
> Materialize will attempt to connect to those brokers without any tunneling.




```mzsql
CREATE CONNECTION <connection_name> TO KAFKA (
    BROKERS (
        '<broker1>:<port1>' USING <tunnel_option>,
        '<broker2>:<port2>' USING <tunnel_option>
    ),
    ...
);

```

| Syntax element | Description |
| --- | --- |
| `<broker>:<port>` | The hostname and port of each Kafka broker.  |
| `USING <tunnel_option>` | Specifies how to connect to each broker (e.g., via AWS PrivateLink or SSH tunnel).  |


##### `kafka_broker`



```mzsql
'<broker>:<port>' USING SSH TUNNEL <connection_name>

```

| Syntax element | Description |
| --- | --- |
| `SSH TUNNEL <connection_name>` | The name of an SSH tunnel connection through which network traffic for this broker should be routed.  |


The `USING` clause specifies that Materialize should connect to the designated
broker via an SSH bastion server. Brokers do not need to be configured the same
way, but the clause must be individually attached to each broker that you want
to connect to via the tunnel.

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




### Confluent Schema Registry

A Confluent Schema Registry connection establishes a link to a [Confluent Schema
Registry] server. You can use Confluent Schema Registry connections in the
`FORMAT` clause of [`CREATE SOURCE`] and [`CREATE SINK`] statements.

#### Syntax {#csr-syntax}



```mzsql
CREATE CONNECTION <connection_name> TO CONFLUENT SCHEMA REGISTRY (
    URL '<url>',
    USERNAME = { '<username>' | SECRET <secret_name> },
    PASSWORD = SECRET <secret_name>,
    SSL CERTIFICATE = { '<pem>' | SECRET <secret_name> },
    SSL KEY = SECRET <secret_name>,
    SSL CERTIFICATE AUTHORITY = { '<pem>' | SECRET <secret_name> },
    AWS PRIVATELINK <privatelink_connection_name>,
    SSH TUNNEL <ssh_connection_name>
)
[WITH (<with_options>)];

```

| Syntax element | Description |
| --- | --- |
| `<connection_name>` | A name for the connection.  |
| `URL` | *Value:* `text`. Required.  The schema registry URL.  |
| `USERNAME` / `PASSWORD` | *Value:* secret or `text` / secret  Credentials for basic HTTP authentication. `PASSWORD` is required and only valid if `USERNAME` is specified.  |
| `SSL CERTIFICATE` / `SSL KEY` | *Value:* secret or `text` / secret  Your TLS certificate and key in PEM format for TLS client authentication. If unspecified, no TLS client authentication is performed.  Only respected if the URL uses the `https` protocol.  |
| `SSL CERTIFICATE AUTHORITY` | *Value:* secret or `text`  The certificate authority (CA) certificate in PEM format. Used to validate the server's TLS certificate. If unspecified, uses the system's default CA certificates.  Only respected if the URL uses the `https` protocol.  |
| `AWS PRIVATELINK` | *Value:* object name  The name of an [AWS PrivateLink connection](#aws-privatelink) to route network traffic through.  |
| `SSH TUNNEL` | *Value:* object name  The name of an [SSH tunnel connection](#ssh-tunnel) to route network traffic through.  |
| `WITH (<with_options>)` | The following `<with_options>` are supported:  \| Field \| Value \| Description \| \|-------\|-------\|-------------\| \| `VALIDATE` \| `boolean` \| Whether [connection validation](#connection-validation) should be performed on connection creation. Default: `true`. \|  |


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


**AWS PrivateLink (Materialize Cloud):**

> **Note:** Connections using AWS PrivateLink is for Materialize Cloud only.



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


**SSH tunnel:**

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




### MySQL

A MySQL connection establishes a link to a [MySQL] server. You can use
MySQL connections to create [sources](/sql/create-source/mysql).

#### Syntax {#mysql-syntax}



```mzsql
CREATE CONNECTION <connection_name> TO MYSQL (
    HOST '<hostname>',
    PORT <port>,
    USER '<username>',
    PASSWORD SECRET <secret_name>,
    SSL MODE = { 'disabled' | 'required' | 'verify_ca' | 'verify_identity' },
    SSL CERTIFICATE AUTHORITY = { '<pem>' | SECRET <secret_name> },
    SSL CERTIFICATE = { '<pem>' | SECRET <secret_name> },
    SSL KEY = SECRET <secret_name>,
    AWS CONNECTION <aws_connection_name>,
    AWS PRIVATELINK <privatelink_connection_name>,
    SSH TUNNEL <ssh_connection_name>
)
[WITH (<with_options>)];

```

| Syntax element | Description |
| --- | --- |
| `<connection_name>` | A name for the connection.  |
| `HOST` | *Value:* `text`. Required.  Database hostname.  |
| `PORT` | *Value:* `integer`  Port number to connect to at the server host.  Default: `3306`.  |
| `USER` | *Value:* `text`. Required.  Database username.  |
| `PASSWORD` | *Value:* secret  Password for the connection.  |
| `SSL MODE` | *Value:* `text`  Enables SSL connections if set to `required`, `verify_ca`, or `verify_identity`. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/using-encrypted-connections.html) for more details.  Default: `disabled`.  |
| `SSL CERTIFICATE AUTHORITY` | *Value:* secret or `text`  The certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.  |
| `SSL CERTIFICATE` / `SSL KEY` | *Value:* secret or `text` / secret  Client SSL certificate and key in PEM format.  |
| `AWS CONNECTION` | <a name="mysql-aws-connection"></a> *Value:* object name  The name of an [AWS connection](#aws) to use when performing IAM authentication with an Amazon RDS MySQL cluster.  Only valid if `SSL MODE` is set to `required`, `verify_ca`, or `verify_identity`. Incompatible with `PASSWORD` being set.  |
| `AWS PRIVATELINK` | *Value:* object name  The name of an [AWS PrivateLink connection](#aws-privatelink) to route network traffic through.  |
| `SSH TUNNEL` | *Value:* object name  The name of an [SSH tunnel connection](#ssh-tunnel) to route network traffic through.  |
| `WITH (<with_options>)` | The following `<with_options>` are supported:  \| Field \| Value \| Description \| \|-------\|-------\|-------------\| \| `VALIDATE` \| `boolean` \| Whether [connection validation](#connection-validation) should be performed on connection creation. Default: `true`. \|  |


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


**AWS PrivateLink (Materialize Cloud):**

> **Note:** Connections using AWS PrivateLink is for Materialize Cloud only.



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


**SSH tunnel:**

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



**AWS IAM:**

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



### PostgreSQL

A Postgres connection establishes a link to a single database of a
[PostgreSQL] server. You can use Postgres connections to create [sources](/sql/create-source/postgres).

#### Syntax {#postgres-syntax}



```mzsql
CREATE CONNECTION <connection_name> TO POSTGRES (
    HOST '<hostname>',
    PORT <port>,
    DATABASE '<database>',
    USER '<username>',
    PASSWORD SECRET <secret_name>,
    SSL MODE = { 'disable' | 'require' | 'verify_ca' | 'verify_full' },
    SSL CERTIFICATE AUTHORITY = { '<pem>' | SECRET <secret_name> },
    SSL CERTIFICATE = { '<pem>' | SECRET <secret_name> },
    SSL KEY = SECRET <secret_name>,
    AWS PRIVATELINK <privatelink_connection_name>,
    SSH TUNNEL <ssh_connection_name>
)
[WITH (<with_options>)];

```

| Syntax element | Description |
| --- | --- |
| `<connection_name>` | A name for the connection.  |
| `HOST` | *Value:* `text`. Required.  Database hostname.  |
| `PORT` | *Value:* `integer`  Port number to connect to at the server host.  Default: `5432`.  |
| `DATABASE` | *Value:* `text`. Required.  Target database.  |
| `USER` | *Value:* `text`. Required.  Database username.  |
| `PASSWORD` | *Value:* secret  Password for the connection.  |
| `SSL MODE` | *Value:* `text`  Enables SSL connections if set to `require`, `verify_ca`, or `verify_full`.  Default: `disable`.  |
| `SSL CERTIFICATE AUTHORITY` | *Value:* secret or `text`  The certificate authority (CA) certificate in PEM format. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.  |
| `SSL CERTIFICATE` / `SSL KEY` | *Value:* secret or `text` / secret  Client SSL certificate and key in PEM format.  |
| `AWS PRIVATELINK` | *Value:* object name  The name of an [AWS PrivateLink connection](#aws-privatelink) to route network traffic through.  |
| `SSH TUNNEL` | *Value:* object name  The name of an [SSH tunnel connection](#ssh-tunnel) to route network traffic through.  |
| `WITH (<with_options>)` | The following `<with_options>` are supported:  \| Field \| Value \| Description \| \|-------\|-------\|-------------\| \| `VALIDATE` \| `boolean` \| Whether [connection validation](#connection-validation) should be performed on connection creation. Default: `true`. \|  |


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


**AWS PrivateLink:**

> **Note:** Connections using AWS PrivateLink is for Materialize Cloud only.



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


**SSH tunnel:**

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




### SQL Server



A SQL Server connection establishes a link to a single database of a
[SQL Server] instance. You can use SQL Server connections to create [sources](/sql/create-source/sql-server).

#### Syntax {#sql-server-syntax}



```mzsql
CREATE CONNECTION <connection_name> TO SQL SERVER (
    HOST '<hostname>',
    PORT <port>,
    DATABASE '<database>',
    USER '<username>',
    PASSWORD SECRET <secret_name>,
    SSL MODE = { 'disabled' | 'required' | 'verify_ca' | 'verify' },
    SSL CERTIFICATE AUTHORITY = { '<pem>' | SECRET <secret_name> }
)
[WITH (<with_options>)];

```

| Syntax element | Description |
| --- | --- |
| `<connection_name>` | A name for the connection.  |
| `HOST` | *Value:* `text`. Required.  Database hostname.  |
| `PORT` | *Value:* `integer`  Port number to connect to at the server host.  Default: `1433`.  |
| `DATABASE` | *Value:* `text`. Required.  Target database.  |
| `USER` | *Value:* `text`. Required.  Database username.  |
| `PASSWORD` | *Value:* secret. Required.  Password for the connection.  |
| `SSL MODE` | *Value:* `text`  Enables SSL connections if set to `required`, `verify_ca`, or `verify`. See the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-sql-server-encryption) for more details.  - `disabled` - no encryption. - `required` - encryption required, no certificate validation. - `verify` - encryption required, validate server certificate using OS configured CA. - `verify_ca` - encryption required, validate server certificate using provided CA certificates (requires `SSL CERTIFICATE AUTHORITY`).  Default: `disabled`.  |
| `SSL CERTIFICATE AUTHORITY` | *Value:* secret or `text`  One or more client SSL certificates in PEM format.  |
| `WITH (<with_options>)` | The following `<with_options>` are supported:  \| Field \| Value \| Description \| \|-------\|-------\|-------------\| \| `VALIDATE` \| `boolean` \| Whether [connection validation](#connection-validation) should be performed on connection creation. Default: `true`. \|  |


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

> **Note:** Connections using AWS PrivateLink is for Materialize Cloud only.



An AWS PrivateLink connection establishes a link to an [AWS PrivateLink] service.
You can use AWS PrivateLink connections in [Confluent Schema Registry connections](#confluent-schema-registry),
[Kafka connections](#kafka), and [Postgres connections](#postgresql).

#### Syntax {#aws-privatelink-syntax}



```mzsql
CREATE CONNECTION <connection_name> TO AWS PRIVATELINK (
    SERVICE NAME '<service_name>',
    AVAILABILITY ZONES ('<az_id1>', '<az_id2>', ...)
);

```

| Syntax element | Description |
| --- | --- |
| `<connection_name>` | A name for the connection.  |
| `SERVICE NAME` | *Value:* `text`. Required.  The name of the AWS PrivateLink service.  |
| `AVAILABILITY ZONES` | *Value:* `text[]`. Required.  The IDs of the AWS availability zones in which the service is accessible.  |


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

> **Warning:** Do **not** grant access to the root principal for the Materialize AWS account.
> Doing so will allow any Materialize customer to create a connection to your
> AWS PrivateLink service.


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



```mzsql
CREATE CONNECTION <connection_name> TO SSH TUNNEL (
    HOST '<hostname>',
    PORT <port>,
    USER '<username>'
);

```

| Syntax element | Description |
| --- | --- |
| `<connection_name>` | A name for the connection.  |
| `HOST` | *Value:* `text`. Required.  The hostname of the SSH bastion server.  |
| `PORT` | *Value:* `integer`. Required.  The port to connect to.  |
| `USER` | *Value:* `text`. Required.  The name of the user to connect as.  |


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

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all connections and secrets used in the connection definition.
- `USAGE` privileges on the schemas that all connections and secrets in the statement are contained in.

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
[`mz_aws_privatelink_connections`]: /sql/system-catalog/mz_catalog/#mz_aws_privatelink_connections
[`mz_connections`]: /sql/system-catalog/mz_catalog/#mz_connections
[`mz_ssh_tunnel_connections`]: /sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections
[Ed25519 algorithm]: https://ed25519.cr.yp.to
[latacora-crypto]: https://latacora.micro.blog/2018/04/03/cryptographic-right-answers.html
[trust policy]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_terms-and-concepts.html#term_trust-policy
[external ID]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html


---

## CREATE DATABASE


Use `CREATE DATABASE` to create a new database.

## Syntax



```mzsql
CREATE DATABASE [IF NOT EXISTS] <database_name>;

```

| Syntax element | Description |
| --- | --- |
| `IF NOT EXISTS` | If specified, do not generate an error if a database of the same name already exists. If not specified, throw an error if a database of the same name already exists.  |
| `<database_name>` | A name for the database.  |


## Details

Databases can contain schemas. By default, each database has a schema called
`public`. For more information about databases, see
[Namespaces](/sql/namespaces).

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

- `CREATEDB` privileges on the system.

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


## Syntax


**CREATE INDEX:**
### Create index

Create an index using the specified columns as the index key.



```mzsql
CREATE INDEX [<index_name>]
[IN CLUSTER <cluster_name>]
ON <obj_name> [USING <method>] (<col_expr>, ...)
[WITH (<with_options>)];

```

| Syntax element | Description |
| --- | --- |
| `<index_name>` | A name for the index.  |
| `IN CLUSTER <cluster_name>` | The [cluster](/sql/create-cluster) to maintain this index. If not specified, defaults to the active cluster.  |
| `<obj_name>` | The name of the source, view, or materialized view on which you want to create an index.  |
| `USING <method>` | The name of the index method to use. The only supported method is [`arrangement`](/overview/arrangements).  |
| `(<col_expr>, ...)` | The expressions to use as the key for the index.  |
| `WITH (<with_option>[,...])` | The following `<with_option>` is supported: \| Option                     \| Description \| \|----------------------------\|-------------\| \| `RETAIN HISTORY FOR`    \|  ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). **Note:** Configuring indexes to retain history is not recommended. Instead, consider creating a materialized view for your subscription query and configuring the history retention period on the view instead. See [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`. \|  |



**CREATE DEFAULT INDEX:**
### Create default index

Create a default index using a set of columns that uniquely identify each row.
If this set of columns cannot be inferred, all columns are used.



```mzsql
CREATE DEFAULT INDEX
[IN CLUSTER <cluster_name>]
ON <obj_name> [USING <method>]
[WITH (<with_options>)];

```

| Syntax element | Description |
| --- | --- |
| `IN CLUSTER <cluster_name>` | The [cluster](/sql/create-cluster) to maintain this index. If not specified, defaults to the active cluster.  |
| `<obj_name>` | The name of the source, view, or materialized view on which you want to create an index.  |
| `USING <method>` | The name of the index method to use. The only supported method is [`arrangement`](/overview/arrangements).  |
| `WITH (<with_option>[,...])` | The following `<with_option>` is supported: \| Option                     \| Description \| \|----------------------------\|-------------\| \| `RETAIN HISTORY FOR`    \|  ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). **Note:** Configuring indexes to retain history is not recommended. Instead, consider creating a materialized view for your subscription query and configuring the history retention period on the view instead. See [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`. \|  |





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

#### Best practices

<p>Before creating an index, consider the following:</p>
<ul>
<li>
<p>If you create stacked views (i.e., views that depend on other views) to
reduce SQL complexity, we recommend that you create an index <strong>only</strong> on the
view that will serve results, taking into account the expected data access
patterns.</p>
</li>
<li>
<p>Materialize can reuse indexes across queries that concurrently access the same
data in memory, which reduces redundancy and resource utilization per query.
In particular, this means that joins do <strong>not</strong> need to store data in memory
multiple times.</p>
</li>
<li>
<p>For queries that have no supporting indexes, Materialize uses the same
mechanics used by indexes to optimize computations. However, since this
underlying work is discarded after each query run, take into account the
expected data access patterns to determine if you need to index or not.</p>
</li>
</ul>


### Usage patterns

#### Indexes on views vs. materialized views

In Materialize, both <a href="/concepts/indexes" >indexes</a> on views and <a href="/concepts/views/#materialized-views" >materialized
views</a> incrementally update the view
results when Materialize ingests new data. Whereas materialized views persist
the view results in durable storage and can be accessed across clusters, indexes
on views compute and store view results in memory within a <strong>single</strong> cluster.
<p>Some general guidelines for usage patterns include:</p>
<table>
  <thead>
      <tr>
          <th>Usage Pattern</th>
          <th>General Guideline</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>View results are accessed from a single cluster only;<br>such as in a 1-cluster or a 2-cluster architecture.</td>
          <td>View with an <a href="/sql/create-index" >index</a></td>
      </tr>
      <tr>
          <td>View used as a building block for stacked views; i.e., views not used to serve results.</td>
          <td>View</td>
      </tr>
      <tr>
          <td>View results are accessed across <a href="/concepts/clusters" >clusters</a>;<br>such as in a 3-cluster architecture.</td>
          <td>Materialized view (in the transform cluster)<br>Index on the materialized view (in the serving cluster)</td>
      </tr>
      <tr>
          <td>Use with a <a href="/serve-results/sink/" >sink</a> or a <a href="/sql/subscribe" ><code>SUBSCRIBE</code></a> operation</td>
          <td>Materialized view</td>
      </tr>
      <tr>
          <td>Use with <a href="/transform-data/patterns/temporal-filters/" >temporal filters</a></td>
          <td>Materialized view</td>
      </tr>
  </tbody>
</table>

#### Indexes and query optimizations

You might want to create indexes when...

-   You want to use non-primary keys (e.g. foreign keys) as a join condition. In
    this case, you could create an index on the columns in the join condition.
-   You want to speed up searches filtering by literal values or expressions.

<p>Specific instances where indexes can be useful to improve performance include:</p>
<ul>
<li>
<p>When used in ad-hoc queries.</p>
</li>
<li>
<p>When used by multiple queries within the same cluster.</p>
</li>
<li>
<p>When used to enable <a href="/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins" >delta
joins</a>.</p>
</li>
</ul>
<p>For more information, see <a href="/transform-data/optimization" >Optimization</a>.</p>



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

- Ownership of the object on which to create the index.
- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster.
- `USAGE` privileges on all types used in the index definition.
- `USAGE` privileges on the schemas that all types in the statement are contained in.

## Related pages

-   [`SHOW INDEXES`](../show-indexes)
-   [`DROP INDEX`](../drop-index)


---

## CREATE MATERIALIZED VIEW


`CREATE MATERIALIZED VIEW` defines a view that maintains [fresh results](/concepts/reaction-time) by persisting them in durable storage and incrementally updating them as new data arrives.

Materialized views are particularly useful when you need **cross-cluster access** to results or want to sink data to external systems like [Kafka](/sql/create-sink). When you create a materialized view, you specify a [cluster](/concepts/clusters/) responsible for maintaining it, but the results can be **queried from any cluster**. This allows you to separate the compute resources used for view maintenance from those used for serving queries.

If you do not need durability or cross-cluster sharing, and you are primarily interested in fast query performance within a single cluster, you may prefer to [create a view and index it](/concepts/views/#views). In Materialize, [indexes on views](/concepts/indexes/) also maintain results incrementally, but store them in memory, scoped to the cluster where the index was created. This approach offers lower latency for direct querying within that cluster.

## Syntax


**CREATE MATERIALIZED VIEW:**

### Create materialized view



```mzsql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] <view_name>
[(<col_ident>, ...)]
[IN CLUSTER <cluster_name>]
[WITH (<with_options>)]
AS <select_stmt>;

```

| Syntax element | Description |
| --- | --- |
| `IF NOT EXISTS` | If specified, do not generate an error if a materialized view of the same name already exists.  |
| `<view_name>` | A name for the materialized view.  |
| `(<col_ident>, ...)` | Rename the `SELECT` statement's columns to the list of identifiers. Both must be the same length. Note that this is required for statements that return multiple columns with the same identifier.  |
| `IN CLUSTER <cluster_name>` | The cluster to maintain this materialized view. If not specified, defaults to the active cluster.  |
| `WITH (<with_options>)` | The following `<with_options>` are supported:  \| Field \| Value \| Description \| \|-------\|-------\|-------------\| \| `ASSERT NOT NULL` *col_ident* \| `text` \| The column identifier for which to create a [non-null assertion](#non-null-assertions). To specify multiple columns, use the option multiple times. \| \| `PARTITION BY` *columns* \| `(ident [, ident]*)` \| The key by which Materialize should internally partition this durable collection. See the [partitioning guide](/transform-data/patterns/partition-by/) for restrictions on valid values and other details. \| \| `RETAIN HISTORY FOR` *retention_period* \| `interval` \| ***Private preview.*** Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`. \| \| `REFRESH` *refresh_strategy* \| \| ***Private preview.*** The refresh strategy for the materialized view. See [Refresh strategies](#refresh-strategies) for syntax options. Default: `ON COMMIT`. \|  |
| `<select_stmt>` | The [`SELECT` statement](/sql/select) whose results you want to maintain incrementally updated.  |














## Details

### Usage patterns

In Materialize, both <a href="/concepts/indexes" >indexes</a> on views and <a href="/concepts/views/#materialized-views" >materialized
views</a> incrementally update the view
results when Materialize ingests new data. Whereas materialized views persist
the view results in durable storage and can be accessed across clusters, indexes
on views compute and store view results in memory within a <strong>single</strong> cluster.
<p>Some general guidelines for usage patterns include:</p>
<table>
  <thead>
      <tr>
          <th>Usage Pattern</th>
          <th>General Guideline</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>View results are accessed from a single cluster only;<br>such as in a 1-cluster or a 2-cluster architecture.</td>
          <td>View with an <a href="/sql/create-index" >index</a></td>
      </tr>
      <tr>
          <td>View used as a building block for stacked views; i.e., views not used to serve results.</td>
          <td>View</td>
      </tr>
      <tr>
          <td>View results are accessed across <a href="/concepts/clusters" >clusters</a>;<br>such as in a 3-cluster architecture.</td>
          <td>Materialized view (in the transform cluster)<br>Index on the materialized view (in the serving cluster)</td>
      </tr>
      <tr>
          <td>Use with a <a href="/serve-results/sink/" >sink</a> or a <a href="/sql/subscribe" ><code>SUBSCRIBE</code></a> operation</td>
          <td>Materialized view</td>
      </tr>
      <tr>
          <td>Use with <a href="/transform-data/patterns/temporal-filters/" >temporal filters</a></td>
          <td>Materialized view</td>
      </tr>
  </tbody>
</table>

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



Materialized views in Materialize are incrementally maintained by default, meaning their results are automatically updated as soon as new data arrives.
This guarantees that queries returns the most up-to-date information available with minimal delay and that results are always as [fresh](/concepts/reaction-time) as the input data itself.

In most cases, this default behavior is ideal.
However, in some very specific scenarios like reporting over slow changing historical data, it may be acceptable to relax freshness in order to reduce compute usage.
For these cases, Materialize supports refresh strategies, which allow you to configure a materialized view to recompute itself on a fixed schedule rather than maintaining them incrementally.

> **Note:** The use of refresh strategies is discouraged unless you have a clear and measurable need to reduce maintenance costs on stale or archival data. For most use cases, the default incremental maintenance model provides a better experience.



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

- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster.
- `USAGE` privileges on all types used in the materialized view definition.
- `USAGE` privileges on the schemas for the types used in the statement.

## Additional information

- Materialized views are not monotonic; that is, materialized views cannot be
  recognized as append-only.

## Related pages

- [`ALTER MATERIALIZED VIEW`](../alter-materialized-view)
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



```mzsql
CREATE NETWORK POLICY <name> (
  RULES (
    <rule_name> (action='allow', direction='ingress', address=<address>)
    [, ...]
  )
)
;

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the network policy to modify.  |
| `<rule_name>` | The name for the network policy rule. Must be unique within the network policy.  |
| `<address>` | The Classless Inter-Domain Routing (CIDR) block to which the rule applies.  |



## Details

### Pre-installed network policy

When you enable a Materialize region, a default network policy named `default`
will be pre-installed. This policy has a wide open ingress rule `allow
0.0.0.0/0`. You can modify or drop this network policy at any time.

> **Note:** The default value for the `network_policy` session parameter is `default`.
> Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
> Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
> change the default value.


## Privileges

The privileges required to execute this statement are:

- `CREATENETWORKPOLICY` privileges on the system.

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



**Cloud:**

### Cloud

The following syntax is used to create a role in Materialize Cloud.


```mzsql
CREATE ROLE <role_name> [[WITH] INHERIT];

```

| Syntax element | Description |
| --- | --- |
| `INHERIT` | *Optional.* If specified, grants the role the ability to inherit privileges of other roles. *Default.*  |


**Note:**
- Materialize Cloud does not support the `NOINHERIT` option for `CREATE
ROLE`.
- Materialize Cloud does not support the `LOGIN` and `SUPERUSER` attributes
  for `CREATE ROLE`.  See [Organization
  roles](/security/cloud/users-service-accounts/#organization-roles)
  instead.
- Materialize Cloud does not use role attributes to determine a role's
ability to create top level objects such as databases and other roles.
Instead, Materialize uses system level privileges. See [GRANT
PRIVILEGE](../grant-privilege) for more details.


**Self-Managed:**
### Self-Managed

The following syntax is used to create a role in Materialize Self-Managed.


```mzsql
CREATE ROLE <role_name>
[WITH]
    [ SUPERUSER | NOSUPERUSER ],
    [ LOGIN | NOLOGIN ]
    [ INHERIT ]
    [ PASSWORD <text> ]
;

```

| Syntax element | Description |
| --- | --- |
| `INHERIT` | *Optional.* If specified, grants the role the ability to inherit privileges of other roles. *Default.*  |
| `LOGIN` | *Optional.* If specified, allows a role to login via the PostgreSQL or web endpoints  |
| `NOLOGIN` | *Optional.* If specified, prevents a role from logging in. This is the default behavior if `LOGIN` is not specified.  |
| `SUPERUSER` | *Optional.* If specified, grants the role superuser privileges.  |
| `NOSUPERUSER` | *Optional.* If specified, prevents the role from having superuser privileges. This is the default behavior if `SUPERUSER` is not specified.  |
| `PASSWORD` | ***Public Preview***  *Optional.* This feature may have minor stability issues. If specified, allows you to set a password for the role.  |


**Note:**
- Self-Managed Materialize does not support the `NOINHERIT` option for
`CREATE ROLE`.
- With the exception of the `SUPERUSER` attribute, Self-Managed Materialize
does not use role attributes to determine a role's ability to create top
level objects such as databases and other roles. Instead, Self-Managed
Materialize uses system level privileges. See [GRANT PRIVILEGE](../grant-privilege) for more details.




## Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... INHERIT INHERIT`.

## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.

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

## Syntax



```mzsql
CREATE SCHEMA [IF NOT EXISTS] <schema_name>;

```

| Syntax element | Description |
| --- | --- |
| `IF NOT EXISTS` | If specified, do not generate an error if a schema of the same name already exists. If not specified, throw an error if a schema of the same name already exists.  |
| `<schema_name>` | A name for the schema. You can specify the database for the schema with a preceding `database_name.schema_name`, e.g. `my_db.my_schema`, otherwise the schema is created in the current database.  |


## Details

By default, each database has a schema called `public`.

For more information, see [Namespaces](../namespaces).

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

- `CREATE` privileges on the containing database.

## Related pages

- [`DROP DATABASE`](../drop-database)
- [`SHOW DATABASES`](../show-databases)


---

## CREATE SECRET


A secret securely stores sensitive credentials (like passwords and SSL keys) in Materialize's secret management system. Optionally, a secret can also be used to store credentials that are generally not sensitive (like usernames and SSL certificates), so that all your credentials are managed uniformly.

## Syntax



```mzsql
CREATE SECRET [IF NOT EXISTS] <name> AS <value>;

```

| Syntax element | Description |
| --- | --- |
| `IF NOT EXISTS` | If specified, do not generate an error if a secret of the same name already exists.  |
| `<name>` | The identifier for the secret.  |
| `<value>` | The value for the secret. The value expression may not reference any relations, and must be implicitly castable to `bytea`.  |


## Examples

```mzsql
CREATE SECRET kafka_ca_cert AS decode('c2VjcmV0Cg==', 'base64');
```

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.

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

## Syntax summary

<!--"Docs Note: Using include-example shortcode instead of include-syntax since only want the code snippet on this page."
-->



**Kafka/Redpanda:**



**Format Avro:**

<no value>```mzsql
CREATE SINK [IF NOT EXISTS] <sink_name>
[IN CLUSTER <cluster_name>]
FROM <item_name>
INTO KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, COMPRESSION TYPE <compression_type>]
  [, TRANSACTIONAL ID PREFIX '<transactional_id_prefix>']
  [, PARTITION BY = <expression>]
  [, PROGRESS GROUP ID PREFIX '<progress_group_id_prefix>']
  [, TOPIC REPLICATION FACTOR <replication_factor>]
  [, TOPIC PARTITION COUNT <partition_count>]
  [, TOPIC CONFIG <topic_config>]
)
[KEY ( <key_col1> [, ...] ) [NOT ENFORCED]]
[HEADERS <headers_column>]
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name> [
  (
    [AVRO KEY FULLNAME '<avro_key_fullname>']
    [, AVRO VALUE FULLNAME '<avro_value_fullname>']
    [, NULL DEFAULTS <null_defaults>]
    [, DOC ON <doc_on_option> [, ...]]
    [, KEY COMPATIBILITY LEVEL '<key_compatibility_level>']
    [, VALUE COMPATIBILITY LEVEL '<value_compatibility_level>']
  )
]
[ENVELOPE DEBEZIUM | UPSERT]
[WITH (SNAPSHOT = <snapshot>)]

```



**Format JSON:**

<no value>```mzsql
CREATE SINK [IF NOT EXISTS] <sink_name>
[IN CLUSTER <cluster_name>]
FROM <item_name>
INTO KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, COMPRESSION TYPE <compression_type>]
  [, TRANSACTIONAL ID PREFIX '<transactional_id_prefix>']
  [, PARTITION BY = <expression>]
  [, PROGRESS GROUP ID PREFIX '<progress_group_id_prefix>']
  [, TOPIC REPLICATION FACTOR <replication_factor>]
  [, TOPIC PARTITION COUNT <partition_count>]
  [, TOPIC CONFIG <topic_config>]
)
[KEY ( <key_col1> [, ...] ) [NOT ENFORCED]]
[HEADERS <headers_column>]
FORMAT JSON
[ENVELOPE DEBEZIUM | UPSERT]
[WITH (SNAPSHOT = <snapshot>)]

```



**Format TEXT/BYTES:**

<no value>```mzsql
CREATE SINK [IF NOT EXISTS] <sink_name>
[IN CLUSTER <cluster_name>]
FROM <item_name>
INTO KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, COMPRESSION TYPE <compression_type>]
  [, TRANSACTIONAL ID PREFIX '<transactional_id_prefix>']
  [, PARTITION BY = <expression>]
  [, PROGRESS GROUP ID PREFIX '<progress_group_id_prefix>']
  [, TOPIC REPLICATION FACTOR <replication_factor>]
  [, TOPIC PARTITION COUNT <partition_count>]
  [, TOPIC CONFIG <topic_config>]
)
FORMAT TEXT | BYTES
[ENVELOPE DEBEZIUM | UPSERT]
[WITH (SNAPSHOT = <snapshot>)]

```



**KEY FORMAT VALUE FORMAT:**

By default, the message key is encoded using the same format as the message value. However, you can set the key and value encodings explicitly using the `KEY FORMAT ... VALUE FORMAT`.

<no value>```mzsql
CREATE SINK [IF NOT EXISTS] <sink_name>
[IN CLUSTER <cluster_name>]
FROM <item_name>
INTO KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, COMPRESSION TYPE <compression_type>]
  [, TRANSACTIONAL ID PREFIX '<transactional_id_prefix>']
  [, PARTITION BY = <expression>]
  [, PROGRESS GROUP ID PREFIX '<progress_group_id_prefix>']
  [, TOPIC REPLICATION FACTOR <replication_factor>]
  [, TOPIC PARTITION COUNT <partition_count>]
  [, TOPIC CONFIG <topic_config>]
)
[KEY ( <key_col1> [, ...] ) [NOT ENFORCED]]
[HEADERS <headers_column>]
KEY FORMAT <key_format> VALUE FORMAT <value_format>
-- <key_format> and <value_format> can be:
-- AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name> [
--     (
--       [AVRO KEY FULLNAME '<avro_key_fullname>']
--       [, AVRO VALUE FULLNAME '<avro_value_fullname>']
--       [, NULL DEFAULTS <null_defaults>]
--       [, DOC ON <doc_on_option> [, ...]]
--       [, KEY COMPATIBILITY LEVEL '<key_compatibility_level>']
--       [, VALUE COMPATIBILITY LEVEL '<value_compatibility_level>']
--     )
-- ]
-- | JSON | TEXT | BYTES
[ENVELOPE DEBEZIUM | UPSERT]
[WITH (SNAPSHOT = <snapshot>)]

```






For details, see [CREATE Sink: Kafka/Redpanda](/sql/create-sink/kafka/).



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

Materialize uses <a href="https://www.confluent.io/blog/transactions-apache-kafka/" >Kafka
transactions</a>. When
Kafka transactions are used, special control messages known as <strong>transaction
markers</strong> are published to the topic. Transaction markers inform both the broker
and clients about the status of a transaction. When a topic is read using a
standard Kafka consumer, these markers are not exposed to the application, which
can give the impression that some offsets are being skipped.

[//]: # "TODO(morsapaes) Add best practices for sizing sinks."

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `SELECT` privileges on the item being written out to an external system.
  - NOTE: if the item is a materialized view, then the view owner must also have the necessary privileges to
    execute the view definition.
- `CREATE` privileges on the containing cluster if the sink is created in an existing cluster.
- `CREATECLUSTER` privileges on the system if the sink is not created in an existing cluster.
- `USAGE` privileges on all connections and secrets used in the sink definition.
- `USAGE` privileges on the schemas that all connections and secrets in the
  statement are contained in.

## Related pages

- [Sinks](/concepts/sinks/)
- [`SHOW SINKS`](/sql/show-sinks/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SINK`](/sql/show-create-sink/)


---

## CREATE SOURCE


A [source](/concepts/sources/) describes an external system you want Materialize to read data from, and provides details about how to decode and interpret that data.

## Syntax summary

<!--"Docs Note: Using include-example shortcode instead of include-syntax since only want the code snippet on this page."
-->



**PostgreSQL (New):**

To create a source from an external PostgreSQL:
```mzsql
CREATE SOURCE [IF NOT EXISTS] <source_name>
[IN CLUSTER <cluster_name>]
FROM POSTGRES CONNECTION <connection_name> (PUBLICATION '<publication_name>')
;

```

For details, see [CREATE SOURCE: PostgreSQL (New Syntax)](/sql/create-source/postgres-v2/).


**PostgreSQL (Legacy):**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM POSTGRES CONNECTION <connection_name> (
  PUBLICATION '<publication_name>'
  [, TEXT COLUMNS ( <col1> [, ...] ) ]
  [, EXCLUDE COLUMNS ( <col1> [, ...] ) ]
)
<FOR ALL TABLES | FOR SCHEMAS ( <schema1> [, ...] ) | FOR TABLES ( <table1> [AS <subsrc_name>] [, ...] )>
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]

```

For details, see [CREATE SOURCE: PostgreSQL (Legacy)](/sql/create-source/postgres/).

**MySQL:**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM MYSQL CONNECTION <connection_name> [
  (
    [TEXT COLUMNS ( <col1> [, ...] ) ]
    [, EXCLUDE COLUMNS ( <col1> [, ...] ) ]
  )
]
<FOR ALL TABLES | FOR SCHEMAS ( <schema1> [, ...] ) | FOR TABLES ( <table1> [AS <subsrc_name>] [, ...] )>
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]

```

For details, see [CREATE SOURCE: MySQL](/sql/create-source/mysql/).


**SQL Server:**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM SQL SERVER CONNECTION <connection_name>
  [ ( EXCLUDE COLUMNS (<col1> [, ...]) ) ]
<FOR ALL TABLES | FOR TABLES ( <table1> [AS <subsrc_name>] [, ...] )>
[WITH (RETAIN HISTORY FOR <retention_period>)]

```

For details, see [CREATE SOURCE: SQL Server](/sql/create-source/sql-server/).



**Kafka/Redpanda:**



**Format Avro:**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name>
  [KEY STRATEGY <key_strategy>]
  [VALUE STRATEGY <value_strategy>]
[INCLUDE
    KEY [AS <name>]
  | PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE
    NONE
  | DEBEZIUM
  | UPSERT [ ( VALUE DECODING ERRORS = INLINE [AS <name>] ) ]
]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]

```



**Format JSON:**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT JSON
[INCLUDE
    PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]

```



**Format TEXT/BYTES:**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT TEXT | BYTES
[INCLUDE
    PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]

```



**Format CSV:**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name> ( <col_name> [, ...] )
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT CSV WITH <n> COLUMNS | WITH HEADER [ ( <col_name> [, ...] ) ]
[INCLUDE
    PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE NONE]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]

```


**Format Protobuf:**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name>
  | FORMAT PROTOBUF MESSAGE '<message_name>' USING SCHEMA '<schema_bytes>'
[INCLUDE
    KEY [AS <name>]
  | PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE
    NONE
  | UPSERT [ ( VALUE DECODING ERRORS = INLINE [AS <name>] ) ]
]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]

```


**KEY FORMAT VALUE FORMAT:**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
KEY FORMAT <key_format> VALUE FORMAT <value_format>
-- <key_format> and <value_format> can be:
-- AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <conn_name>
--     [KEY STRATEGY <strategy>]
--     [VALUE STRATEGY <strategy>]
-- | CSV WITH <num> COLUMNS DELIMITED BY <char>
-- | JSON | TEXT | BYTES
-- | PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION <conn_name>
-- | PROTOBUF MESSAGE '<message_name>' USING SCHEMA '<schema_bytes>'
[INCLUDE
    KEY [AS <name>]
  | PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE
    NONE
  | DEBEZIUM
  | UPSERT [(VALUE DECODING ERRORS = INLINE [AS name])]
]
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]

```





For details, see [CREATE SOURCE: Kafka/Redpanda](/sql/create-source/kafka/).


**Webhook:**

<no value>```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM WEBHOOK
  BODY FORMAT <TEXT | JSON [ARRAY] | BYTES>
  [INCLUDE HEADER <header_name> AS <column_alias> [BYTES] |
   INCLUDE HEADERS [ ( [NOT] <header_name> [, [NOT] <header_name> ... ] ) ]
  ][...]
  [CHECK (
      [WITH ( <BODY|HEADERS|SECRET <secret_name>> [AS <alias>] [BYTES] [, ...])]
      <check_expression>
    )
  ]

```

For details, see [CREATE SOURCE: Webhook](/sql/create-source/webhook/).




## Privileges

The privileges required to execute `CREATE SOURCE` are:

- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster if the source is created in an existing cluster.
- `CREATECLUSTER` privileges on the system if the source is not created in an existing cluster.
- `USAGE` privileges on all connections and secrets used in the source definition.
- `USAGE` privileges on the schemas that all connections and secrets in the
  statement are contained in.

## Available guides

The following guides step you through setting up sources:

<div class="multilinkbox">
<div class="linkbox ">
  <div class="title">
    Databases (CDC)
  </div>
  <ul>
<li><a href="/ingest-data/postgres/" >PostgreSQL</a></li>
<li><a href="/ingest-data/mysql/" >MySQL</a></li>
<li><a href="/ingest-data/sql-server/" >SQL Server</a></li>
<li><a href="/ingest-data/cdc-cockroachdb/" >CockroachDB</a></li>
<li><a href="/ingest-data/mongodb/" >MongoDB</a></li>
</ul>

</div>

<div class="linkbox ">
  <div class="title">
    Message Brokers
  </div>
  <ul>
<li><a href="/ingest-data/kafka/" >Kafka</a></li>
<li><a href="/sql/create-source/kafka" >Redpanda</a></li>
</ul>

</div>

<div class="linkbox ">
  <div class="title">
    Webhooks
  </div>
  <ul>
<li><a href="/ingest-data/webhooks/amazon-eventbridge/" >Amazon EventBridge</a></li>
<li><a href="/ingest-data/webhooks/segment/" >Segment</a></li>
<li><a href="/sql/create-source/webhook" >Other webhooks</a></li>
</ul>

</div>

</div>



## Best practices

### Separate cluster(s) for sources

In production, if possible, use a dedicated cluster for
[sources](/concepts/sources/); i.e., avoid putting sources on the same cluster
that hosts compute objects, sinks, and/or serves queries.

<p>In addition, for upsert sources:</p>
<ul>
<li>
<p>Consider separating upsert sources from your other sources. Upsert sources
have higher resource requirements (since, for upsert sources, Materialize
maintains each key and associated last value for the key as well as to perform
deduplication). As such, if possible, use a separate source cluster for upsert
sources.</p>
</li>
<li>
<p>Consider using a larger cluster size during snapshotting for upsert sources.
Once the snapshotting operation is complete, you can downsize the cluster to
align with the steady-state ingestion.</p>
</li>
</ul>



### Sizing a source

Some sources are low traffic and require relatively few resources to handle data ingestion, while others are high traffic and require hefty resource allocations. The cluster in which you place a source determines the amount of CPU, memory, and disk available to the source.

It's a good idea to size up the cluster hosting a source when:

  * You want to **increase throughput**. Larger sources will typically ingest data
    faster, as there is more CPU available to read and decode data from the
    upstream external system.

  * You are using the [upsert
    envelope](/sql/create-source/kafka/#upsert-envelope) or [Debezium
    envelope](/sql/create-source/kafka/#debezium-envelope), and your source
    contains **many unique keys**. These envelopes maintain state proportional
    to the number of unique keys in the upstream external system. Larger sizes
    can store more unique keys.

Sources share the resource allocation of their cluster with all other objects in
the cluster. Colocating multiple sources onto the same cluster can be more
resource efficient when you have many low-traffic sources that occasionally need
some burst capacity.


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
  ingestion from a source](/ingest-data/postgres/).


Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, views, and
subsources; and you can create views/materialized views/indexes on tables.


[//]: # "TODO(morsapaes) Bring back When to use a table? once there's more
clarity around best practices."

## Syntax


**Read-write table:**
### Read-write table

To create a new read-write table (i.e., users can perform
[`SELECT`](/sql/select/), [`INSERT`](/sql/insert/),
[`UPDATE`](/sql/update/), and [`DELETE`](/sql/delete/) operations):


```mzsql
CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS] <table_name> (
  <column_name> <column_type> [NOT NULL][DEFAULT <default_expr>]
  [, ...]
)
[WITH (
  PARTITION BY (<column_name> [, ...]) |
  RETAIN HISTORY [=] FOR <duration>
)]
;

```

| Syntax element | Description |
| --- | --- |
| **TEMP** / **TEMPORARY** | *Optional.* If specified, mark the table as temporary.  Temporary tables are: - Automatically dropped at the end of the session; - Not visible to other connections; - Created in the special `mz_temp` schema.  Temporary tables may depend upon other temporary database objects, but non-temporary tables may not depend on temporary objects.  |
| **IF NOT EXISTS** | *Optional.* If specified, do not throw an error if the table with the same name already exists. Instead, issue a notice and skip the table creation.  |
| `<table_name>` |  The name of the table to create. Names for tables must follow the [naming guidelines](/sql/identifiers/#naming-restrictions).  |
| `<column_name>` |  The name of a column to be created in the new table. Names for columns must follow the [naming guidelines](/sql/identifiers/#naming-restrictions).  |
| `<column_type>` |  The type of the column. For supported types, see [SQL data types](/sql/types/).  |
| **NOT NULL** | *Optional.* If specified, disallow  _NULL_ values for the column. Columns without this constraint can contain _NULL_ values.  |
| **DEFAULT <default_expr>** | *Optional.* If specified, use the `<default_expr>` as the default value for the column. If not specified, `NULL` is used as the default value.  |
| **WITH (<with_option>[,...])** |  The following `<with_option>`s are supported:  \| Option \| Description \| \|--------\|-------------\| \| `PARTITION BY (<column> [, ...])` \| {{< include-md file="shared-content/partition-by-option-description.md" >}} \| \| `RETAIN HISTORY <duration>` \| *Optional.* ***Private preview.** This option has known performance or stability issues and is under active development.* <br>If specified, Materialize retains historical data for the specified duration, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period).<br>Accepts positive [interval](/sql/types/interval/) values (e.g., `'1hr'`).\|  |



**PostgreSQL source table:**
### PostgreSQL source table



> **Note:** You must be on **v26+** to use the new syntax.


To create a read-only table from a [source](/sql/create-source/) connected
(via native connector) to an external PostgreSQL:


```mzsql
CREATE TABLE [IF NOT EXISTS] <table_name> FROM SOURCE <source_name> (REFERENCE <upstream_table>)
[WITH (
    TEXT COLUMNS (<column_name> [, ...])
  | EXCLUDE COLUMNS (<column_name> [, ...])
  | PARTITION BY (<column_name> [, ...])
  [, ...]
)]
;

```

| Syntax element | Description |
| --- | --- |
| **IF NOT EXISTS** | *Optional.* If specified, do not throw an error if the table with the same name already exists. Instead, issue a notice and skip the table creation.  {{< include-md file="shared-content/create-table-if-not-exists-tip.md" >}}  |
| `<table_name>` |  The name of the table to create. Names for tables must follow the [naming guidelines](/sql/identifiers/#naming-restrictions).  |
| `<source_name>` |  The name of the [source](/sql/create-source/) associated with the reference object from which to create the table.  |
| **(REFERENCE <upstream_table>)** |  The name of the upstream table from which to create the table. You can create multiple tables from the same upstream table.  To find the upstream tables available in your [source](/sql/create-source/), you can use the following query, substituting your source name for `<source_name>`:  <br>  ```mzsql SELECT refs.* FROM mz_internal.mz_source_references refs, mz_sources s WHERE s.name = '<source_name>' -- substitute with your source name AND refs.source_id = s.id; ```  |
| **WITH (<with_option>[,...])** | The following `<with_option>`s are supported:  \| Option \| Description \| \|--------\|-------------\| \| `TEXT COLUMNS (<column_name> [, ...])` \|*Optional.* If specified, decode data as `text` for the listed column(s),such as for unsupported data types. See also [supported types](#supported-data-types). \| \| `EXCLUDE COLUMNS (<column_name> [, ...])`\| *Optional.* If specified,exclude the listed column(s) from the table, such as for unsupported data types. See also [supported types](#supported-data-types).\| \| `PARTITION BY (<column_name> [, ...])` \| {{< include-md file="shared-content/partition-by-option-description.md" >}} \|  |


For an example, see [Create a table (PostgreSQL
source)](/sql/create-table/#create-a-table-postgresql-source).






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



> **Note:** You must be on **v26+** to use the new syntax.


### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

<a name="supported-db-source-types"></a>

### Read-only tables

Source-populated tables are <strong>read-only</strong> tables. Users <strong>cannot</strong> perform write
operations
(<a href="/sql/insert/" ><code>INSERT</code></a>/<a href="/sql/update/" ><code>UPDATE</code></a>/<a href="/sql/delete/" ><code>DELETE</code></a>) on
these tables.

### Source-populated tables and snapshotting

<p>Creating the tables from sources starts the <a href="/ingest-data/#snapshotting" >snapshotting</a> process. Snapshotting syncs the
currently available data into Materialize. Because the initial snapshot is
persisted in the storage layer atomically (i.e., at the same ingestion
timestamp), you are not able to query the table until snapshotting is complete.</p>
> **Note:** During the snapshotting, the data ingestion for
> the existing tables for the same source is temporarily blocked. As such, if
> possible, you can resize the cluster to speed up the snapshotting process and
> once the process finishes, resize the cluster for steady-state.

### Supported data types

<p>Materialize natively supports the following PostgreSQL types (including the
array type for each of the types):</p>
<ul style="column-count: 3">
<li><code>bool</code></li>
<li><code>bpchar</code></li>
<li><code>bytea</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>daterange</code></li>
<li><code>float4</code></li>
<li><code>float8</code></li>
<li><code>int2</code></li>
<li><code>int2vector</code></li>
<li><code>int4</code></li>
<li><code>int4range</code></li>
<li><code>int8</code></li>
<li><code>int8range</code></li>
<li><code>interval</code></li>
<li><code>json</code></li>
<li><code>jsonb</code></li>
<li><code>numeric</code></li>
<li><code>numrange</code></li>
<li><code>oid</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>timestamptz</code></li>
<li><code>tsrange</code></li>
<li><code>tstzrange</code></li>
<li><code>uuid</code></li>
<li><code>varchar</code></li>
</ul>

<p>Replicating tables that contain <strong>unsupported <a href="/sql/types/" >data types</a></strong> is
possible via the <code>TEXT COLUMNS</code> option. The specified columns will be
treated as <code>text</code>; i.e., will not have the expected PostgreSQL type
features. For example:</p>
<ul>
<li>
<p><a href="https://www.postgresql.org/docs/current/datatype-enum.html" ><code>enum</code></a>: When decoded as <code>text</code>, the implicit ordering of the original
PostgreSQL <code>enum</code> type is not preserved; instead, Materialize will sort values
as <code>text</code>.</p>
</li>
<li>
<p><a href="https://www.postgresql.org/docs/current/datatype-money.html" ><code>money</code></a>: When decoded as <code>text</code>, resulting <code>text</code> value cannot be cast
back to <code>numeric</code>, since PostgreSQL adds typical currency formatting to the
output.</p>
</li>
</ul>


### Handling table schema changes

The use of <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE</code></a> with <code>CREATE TABLE FROM SOURCE</code> allows for the handling of the upstream DDL changes,
specifically adding or dropping columns in the upstream tables, without
downtime. See <a href="/ingest-data/postgres/source-versioning/" >Handling upstream schema changes with zero
downtime</a> for more details.

#### Incompatible schema changes

<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>


### Upstream table truncation restrictions

<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>

### Inherited tables

<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>


You can mimic PostgreSQL&rsquo;s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the table definition.
- `USAGE` privileges on the schemas that all types in the statement are
  contained in.

## Examples

### Create a table (User-populated)

The following example uses `CREATE TABLE` to create a new read-write table
`mytable` with two columns `a` (of type `int`) and `b` (of type `text` and
not nullable):
```mzsql
CREATE TABLE mytable (a int, b text NOT NULL);

```

Once a user-populated table is created, you can perform CRUD
(Create/Read/Update/Write) operations on it.

The following example uses [`INSERT`](/sql/insert/) to write two rows to the table:
```mzsql
INSERT INTO mytable VALUES
(1, 'hello'),
(2, 'goodbye')
;

```

The following example uses [`SELECT`](/sql/select/) to read all rows from the table:
```mzsql
SELECT * FROM mytable;

```The results should display the two rows inserted:

```hc {hl_lines="3-4"}
| a | b       |
| - | ------- |
| 1 | hello   |
| 2 | goodbye |
```


### Create a table (PostgreSQL source)



> **Note:** You must be on **v26+** to use the new syntax.
> The example assumes you have configured your upstream PostgreSQL 11+ (i.e.,
> enabled logical replication, created the publication for the various tables and
> replication user, and updated the network configuration).
> For details about configuring your upstream system, see the [PostgreSQL
> integration guides](/ingest-data/postgres/#supported-versions-and-services).



To create new **read-only** tables from a source table, use the `CREATE
TABLE ... FROM SOURCE ... (REFERENCE <upstream_table>)` statement. The
following example creates **read-only** tables `items` and `orders` from the
PostgreSQL source's `public.items` and `public.orders` tables (the schema is `public`).

{{< note >}}

- Although the example creates the tables with the same names as the
upstream tables, the tables in Materialize can have names that differ from
the referenced table names.

- For supported PostgreSQL data types, refer to [supported
types](/sql/create-table/#supported-data-types).

{{< /note >}}
```mzsql
/* This example assumes:
  - In the upstream PostgreSQL, you have defined:
    - replication user and password with the appropriate access.
    - a publication named `mz_source` for the `items` and `orders` tables.
  - In Materialize:
    - You have created a secret for the PostgreSQL password.
    - You have defined the connection to the upstream PostgreSQL.
    - You have used the connection to create a source.

   For example (substitute with your configuration):
      CREATE SECRET pgpass AS '<replication user password>'; -- substitute
      CREATE CONNECTION pg_connection TO POSTGRES (
        HOST '<hostname>',          -- substitute
        DATABASE <db>,              -- substitute
        USER <replication user>,    -- substitute
        PASSWORD SECRET pgpass
        -- [, <network security configuration> ]
      );

      CREATE SOURCE pg_source
      FROM POSTGRES CONNECTION pg_connection (
        PUBLICATION 'mz_source'       -- substitute
      );
*/

CREATE TABLE items
FROM SOURCE pg_source(REFERENCE public.items)
;
CREATE TABLE orders
FROM SOURCE pg_source(REFERENCE public.orders)
;

```
{{< include-md
file="shared-content/create-table-from-source-snapshotting.md" >}}

{{< include-md file="shared-content/create-table-if-not-exists-tip.md" >}}


Source-populated tables are <strong>read-only</strong> tables. Users <strong>cannot</strong> perform write
operations
(<a href="/sql/insert/" ><code>INSERT</code></a>/<a href="/sql/update/" ><code>UPDATE</code></a>/<a href="/sql/delete/" ><code>DELETE</code></a>) on
these tables.


Once the snapshotting process completes and the table is in the running state, you can query the table:
```mzsql
SELECT * FROM items;

```


## Related pages

- [`INSERT`]
- [`DROP TABLE`](/sql/drop-table)

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/


---

## CREATE TYPE


`CREATE TYPE` defines a custom data type, which let you create named versions of
anonymous types or provide a shorthand for other types. For more information,
see [SQL Data Types: Custom types](../types/#custom-types).

## Syntax


**Row type:**
### Row type


```mzsql
CREATE TYPE <type_name> AS (<field_name> <field_type>, ...);

```

| Syntax element | Description |
| --- | --- |
| `<type_name>` | A name for the type.  |
| `<field_name>` | The name of a field in a row type.  |
| `<field_type>` | The data type of a field indicated by `field_name`.  |



**List type:**
### List type


```mzsql
CREATE TYPE <type_name> AS LIST (ELEMENT TYPE = <element_type>);

```

| Syntax element | Description |
| --- | --- |
| `<type_name>` | A name for the type.  |
| `<element_type>` | Creates a custom [`list`](/sql/types/list) whose elements are of `<element_type>`.  |



**Map type:**
### Map type


```mzsql
CREATE TYPE <type_name> AS MAP (KEY TYPE = <key_type>, VALUE TYPE = <value_type>);

```

| Syntax element | Description |
| --- | --- |
| `<type_name>` | A name for the type.  |
| `<key_type>` | Creates a custom [`map`](/sql/types/map) whose keys are of `<key_type>`. Must resolve to [`text`](/sql/types/text).  |
| `<value_type>` | Creates a custom [`map`](/sql/types/map) whose values are of `<value_type>`.  |






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

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the type definition.
- `USAGE` privileges on the schemas that all types in the statement are
  contained in.

## Related pages

* [`DROP TYPE`](../drop-type)
* [`SHOW TYPES`](../show-types)


---

## CREATE VIEW


Use `CREATE VIEW` to define a view, which simply provides an alias for the
embedded `SELECT` statement. The results of a view can be incrementally
maintained **in memory** within a [cluster](/concepts/clusters/) by creating an
[index](../create-index). This allows you to serve queries without the overhead
of materializing the view.

## Syntax


**CREATE VIEW:**
### Create view
To create a view:



```mzsql
CREATE [TEMP|TEMPORARY] VIEW [IF NOT EXISTS] <view_name>[(<col_ident>, ...)] AS
<select_stmt>;

```

| Syntax element | Description |
| --- | --- |
| `TEMP` / `TEMPORARY` | Optional. Mark the view as [temporary](/sql/create-view/#temporary-views). Temporary views are: - Created in the `mz_temp` schema. - Not visible to other connections. - Automatically dropped at the end of the SQL session  |
| `IF NOT EXISTS` | Optional. If specified, do not generate an error if a view with the same name already exists. If not specified, an error is generated if a view with the same name already exists.  |
| `<view_name>` | A name for the view.  |
| `(<col_ident>, ...)` | Optional if the `SELECT` statement return columns with unique names; else, is required if the `SELECT` statement returns multiple columns with the same identifier. If specified, renames the `SELECT` statement's columns to the list of identifiers. Both must be the same length.  |
| `<select_stmt>` | The [`SELECT` statement](/sql/select) that defines the view.  |


**CREATE OR REPLACE VIEW:**
### Create or replace view
To create, or if a view exists with the same name, replace it with the view
defined in this statement:

> **Note:** You cannot replace views that other views depend on,
> nor can you replace a non-view object with a view.




```mzsql
CREATE OR REPLACE VIEW <view_name> [(<col_ident>, ...)] AS <select_stmt>;

```

| Syntax element | Description |
| --- | --- |
| `<view_name>` | A name for the view.  |
| `(<col_ident>, ...)` | Optional if the `SELECT` statement return columns with unique names; else, is required if the `SELECT` statement returns multiple columns with the same identifier. If specified, renames the `SELECT` statement's columns to the list of identifiers. Both must be the same length.  |
| `<select_stmt>` | The [`SELECT` statement](/sql/select) that defines the view.  |




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

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the view definition.
- `USAGE` privileges on the schemas for the types in the statement.
- Ownership of the existing view if replacing an existing
  view with the same name (i.e., `OR REPLACE` is specified in `CREATE VIEW` command).

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

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `DELETE` privileges on `table_name`.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
    execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
    granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

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

- Ownership of the dropped cluster.

## Related pages

- [`SHOW CLUSTERS`](../show-clusters)
- [`DROP OWNED`](../drop-owned)


---

## DROP CLUSTER REPLICA


`DROP CLUSTER REPLICA` deprovisions an existing replica of the specified
[unmanaged cluster](/sql/create-cluster/#unmanaged-clusters). To remove
the cluster itself, use the [`DROP CLUSTER`](/sql/drop-cluster) command.

> **Tip:** When getting started with Materialize, we recommend starting with managed
> clusters.


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

- Ownership of the dropped cluster replica.
- `USAGE` privileges on the containing cluster.


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

- Ownership of the dropped connection.
- `USAGE` privileges on the containing schema.


## Related pages

- [`SHOW CONNECTIONS`](../show-connections)
- [`DROP OWNED`](../drop-owned)


---

## DROP DATABASE


`DROP DATABASE` removes a database from Materialize.

> **Warning:** `DROP DATABASE` immediately removes all objects within the
> database without confirmation. Use with care!


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

- Ownership of the dropped database.

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

> **Note:** Since indexes do not have dependent objects, `DROP INDEX`, `DROP INDEX
> RESTRICT`, and `DROP INDEX CASCADE` are equivalent.


## Privileges

To execute the `DROP INDEX` statement, you need:

- Ownership of the dropped index.
- `USAGE` privileges on the containing schema.

## Examples

### Remove an index

> **Tip:** In the **Materialize Console**, you can view existing indexes in the [**Database
> object explorer**](/console/data/). Alternatively, you can use the
> [`SHOW INDEXES`](/sql/show-indexes) command.


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

- Ownership of the dropped materialized view.
- `USAGE` privileges on the containing schema.

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

- `CREATENETWORKPOLICY` privileges on the system.

## Related pages

- [`SHOW NETWORK POLICIES`](../show-network-policies)
- [`ALTER NETWORK POLICY`](../alter-network-policy)
- [`CREATE NETWORK POLICY`](../create-network-policy)


---

## DROP OWNED


`DROP OWNED` drops all the objects that are owned by one of the specified roles.
Any privileges granted to the given roles on objects will also be revoked.

> **Note:** Unlike [PostgreSQL](https://www.postgresql.org/docs/current/sql-drop-owned.html), Materialize drops
> all objects across all databases, including the database itself.


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

- Role membership in `role_name`.

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

- `CREATEROLE` privileges on the system.

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

- Ownership of the dropped schema.
- `USAGE` privileges on the containing database.

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

- Ownership of the dropped secret.
- `USAGE` privileges on the containing schema.

## Related pages

- [`SHOW SECRETS`](../show-secrets)
- [`DROP OWNED`](../drop-owned)


---

## DROP SINK


`DROP SINK` removes a sink from Materialize.

Dropping a Kafka sink doesn't drop the corresponding topic. For more information, see the [Kafka documentation](https://kafka.apache.org/documentation/).


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

- Ownership of the dropped sink.
- `USAGE` privileges on the containing schema.

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

- Ownership of the dropped source.
- `USAGE` privileges on the containing schema.

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

- Ownership of the dropped table.
- `USAGE` privileges on the containing schema.

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

- Ownership of the dropped type.
- `USAGE` privileges on the containing schema.

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

- `CREATEROLE` privileges on the system.

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

- Ownership of the dropped view.
- `USAGE` privileges on the containing schema.

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



```mzsql
EXECUTE <name> [ ( <parameter> [, ...] ) ]

```

| Syntax element | Description |
| --- | --- |
| `<name>` | The name of the prepared statement to execute.  |
| `<parameter>` | The actual value of a parameter to the prepared statement.  |


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

> **Warning:** `EXPLAIN` is not part of Materialize's stable interface and is not subject to
> our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
> change arbitrarily in future versions of Materialize.


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
> **Tip:** If you want to specify both `CPU` or `MEMORY`, they may be listed in any order;
> however, each may appear only once.


Parameter    | Description
-------------|-----
**CPU**      | Reports consumed CPU time information `total_elapsed` for each operator (not inclusive of its child operators; `FOR INDEX`, `FOR MATERIALIZED VIEW`) or for each object in the current cluster (`CLUSTER`).
**MEMORY**   | Reports consumed memory information `total_memory` and number of records `total_records` for each operator (not including child operators; `FOR INDEX`, `FOR MATERIALIZED VIEW`) or for each object in the current cluster (`CLUSTER`).
**WITH SKEW** | *Optional.* If specified, includes additional information about average and per-worker consumption and ratios (of `CPU` and/or `MEMORY`).
**HINTS**    | Annotates the LIR plan with [TopK hints] (`FOR INDEX`, `FOR MATERIALIZED VIEW`).
**AS SQL**   | *Optional.* If specified, returns the SQL associated with the specified `EXPLAIN ANALYZE` command without executing it. You can modify this SQL as a starting point to create customized queries.

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the explainee are
  contained in.

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

> **Tip:** To determine how many workers a given cluster size has, you can query
> [`mz_catalog.mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes).


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



> **Public Preview:** This feature is in public preview.


`EXPLAIN FILTER PUSHDOWN` reports filter pushdown statistics for `SELECT`
statements and materialized views.

## Syntax



```mzsql
EXPLAIN FILTER PUSHDOWN
FOR <select_stmt | MATERIALIZED VIEW <name>>

```

| Syntax element | Description |
| --- | --- |
| **FOR** `<select_stmt>` | Display statistics for an ad-hoc [`SELECT`](/sql/select) statement.  |
| **FOR MATERIALIZED VIEW** `<name>` | Display statistics for an existing materialized view.  |


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

- `USAGE` privileges on the schemas that all relations in the explainee are
  contained in.


---

## EXPLAIN PLAN


`EXPLAIN PLAN` displays the plans used for:

|                             |                       |
|-----------------------------|-----------------------|
| <ul><li>`SELECT` statements </li><li>`CREATE VIEW` statements</li><li>`CREATE INDEX` statements</li><li>`CREATE MATERIALIZED VIEW` statements</li></ul>|<ul><li>Existing views</li><li>Existing indexes</li><li>Existing materialized views</li></ul> |

> **Warning:** `EXPLAIN` is not part of Materialize's stable interface and is not subject to
> our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
> change arbitrarily in future versions of Materialize.


## Syntax


**FOR SELECT:**
```mzsql
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]       -- The FOR keyword is required if the PLAN keyword is specified
    <SELECT ...>
;
```

**FOR CREATE VIEW:**

```mzsql
EXPLAIN <RAW | DECORRELATED | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR
    <CREATE VIEW ...>
;
```

**FOR CREATE INDEX:**
```mzsql
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE INDEX ...>
;
```

**FOR CREATE MATERIALIZED VIEW:**
```mzsql
EXPLAIN [ [ RAW | DECORRELATED | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR ]          -- The FOR keyword is required if the PLAN keyword is specified
    <CREATE MATERIALIZED VIEW ...>
;
```

**FOR VIEW:**
```mzsql
EXPLAIN <RAW | LOCALLY OPTIMIZED> PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...])]
    [ AS TEXT | AS JSON ]
FOR
  VIEW <name>
;
```

**FOR INDEX:**
```mzsql
EXPLAIN [ [ OPTIMIZED | PHYSICAL ] PLAN
      [ WITH (<output_modifier> [, <output_modifier> ...]) ]
      [ AS TEXT | AS JSON ]
FOR ]  -- The FOR keyword is required if the PLAN keyword is specified
  INDEX <name>
;
```

**FOR MATERIALIZED VIEW:**
```mzsql
EXPLAIN [[ RAW | [LOCALLY] OPTIMIZED | PHYSICAL ] PLAN
    [ WITH (<output_modifier> [, <output_modifier> ...]) ]
    [ AS TEXT | AS JSON ]
FOR ] -- The FOR keyword is required if the PLAN keyword is specified
  MATERIALIZED VIEW <name>
;
```



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
**OPTIMIZED PLAN** | _(Default)_ Display the optimized plan.
**PHYSICAL PLAN** |  Display the physical plan; this corresponds to the operators shown in [`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping).

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

Each operator can also be annotated with additional metadata. Some details are shown in the `EXPLAIN` output (`EXPLAIN PHYSICAL PLAN AS TEXT`), but are hidden elsewhere. <a
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



**In fully optimized physical (LIR) plans (Default):**
The following table lists the operators that are available in the LIR plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.| Operator | Description | Example |
| --- | --- | --- |
| **Constant** | Always produces the same collection of rows.  **Can increase data size:** No **Uses memory:** No | <code>→Constant (2 rows)</code> |
| **Stream, Arranged, Index Lookup, Read** | <p>Produces rows from either an existing relation (source/view/materialized view/table) or from a previous CTE in the same plan. A parent <code>Fused Map/Filter/Project</code> operator can combine with this operator.</p> <p>There are four types of <code>Get</code>.</p> <ol> <li> <p><code>Stream</code> indicates that the results are not <a href="/get-started/arrangements/#arrangements" >arranged</a> in memory and will be streamed directly.</p> </li> <li> <p><code>Arranged</code> indicates that the results are <a href="/get-started/arrangements/#arrangements" >arranged</a> in memory.</p> </li> <li> <p><code>Index Lookup</code> indicates the results will be <em>looked up</em> in an existing [arrangement]((/get-started/arrangements/#arrangements).</p> </li> <li> <p><code>Read</code> indicates that the results are unarranged, and will be processed as they arrive.</p> </li> </ol>   **Can increase data size:** No **Uses memory:** No | <code>Arranged materialize.public.t</code> |
| **Map/Filter/Project** | <p>Computes new columns (maps), filters columns, and projects away columns. Works row-by-row. Maps and filters will be printed, but projects will not.</p> <p>These may be marked as <strong><code>Fused</code></strong> <code>Map/Filter/Project</code>, which means they will combine with the operator beneath them to run more efficiently.</p>   **Can increase data size:** Each row may have more data, from the <code>Map</code>. Each row may also have less data, from the <code>Project</code>. There may be fewer rows, from the <code>Filter</code>. **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="k">Map</span><span class="o">/</span><span class="k">Filter</span><span class="o">/</span><span class="n">Project</span> </span></span><span class="line"><span class="cl">  <span class="k">Filter</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">}</span> <span class="o">&lt;</span> <span class="mf">7</span><span class="p">)</span> </span></span><span class="line"><span class="cl">  <span class="k">Map</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">}</span> <span class="o">+</span> <span class="o">#</span><span class="mf">1</span><span class="p">{</span><span class="n">b</span><span class="p">})</span> </span></span></code></pre></div> |
| **Table Function** | <p>Appends the result of some (one-to-many) <a href="/sql/functions/#table-functions" >table function</a> to each row in the input.</p> <p>A parent <code>Fused Table Function unnest_list</code> operator will fuse with its child <code>GroupAggregate</code> operator. Fusing these operator is part of how we efficiently compile window functions from SQL to dataflows.</p> <p>A parent <code>Fused Map/Filter/Project</code> can combine with this operator.</p>   **Can increase data size:** Depends on the <a href="/sql/functions/#table-functions" >table function</a> used. **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="k">Table</span> <span class="k">Function</span> <span class="n">generate_series</span><span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">},</span> <span class="o">#</span><span class="mf">1</span><span class="p">{</span><span class="n">b</span><span class="p">},</span> <span class="mf">1</span><span class="p">)</span> </span></span><span class="line"><span class="cl">  <span class="k">Input</span> <span class="k">key</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">})</span> </span></span></code></pre></div> |
| **Differential Join, Delta Join** | <p>Both join operators indicate the join ordering selected.</p> <p>Returns combinations of rows from each input whenever some equality predicates are <code>true</code>.</p> <p>Joins will indicate the join order of their children, starting from 0. For example, <code>Differential Join %1 » %0</code> will join its second child into its first.</p> <p>The <a href="/transform-data/optimization/#join" >two joins differ in performance characteristics</a>.</p>   **Can increase data size:** Depends on the join order and facts about the joined collections. **Uses memory:** ✅ Uses memory for 3-way or more differential joins. | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Differential</span> <span class="k">Join</span> <span class="o">%</span><span class="mf">1</span> <span class="err">»</span> <span class="o">%</span><span class="mf">0</span> </span></span><span class="line"><span class="cl">  <span class="k">Join</span> <span class="n">stage</span> <span class="o">%</span><span class="mf">0</span><span class="p">:</span> <span class="n">Lookup</span> <span class="k">key</span> <span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">}</span> <span class="k">in</span> <span class="o">%</span><span class="mf">0</span> </span></span></code></pre></div> |
| **GroupAggregate** | <p>Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs.</p> <p>There are five types of <code>GroupAggregate</code>, ordered by increasing complexity:</p> <ol> <li> <p><code>Distinct GroupAggregate</code> corresponds to the SQL <code>DISTINCT</code> operator.</p> </li> <li> <p><code>Accumulable GroupAggregate</code> (e.g., <code>SUM</code>, <code>COUNT</code>) corresponds to several easy to implement aggregations that can be executed simultaneously and efficiently.</p> </li> <li> <p><code>Hierarchical GroupAggregate</code> (e.g., <code>MIN</code>, <code>MAX</code>) corresponds to an aggregation requiring a tower of arrangements. These can be either monotonic (more efficient) or bucketed. These may benefit from a hint; <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" >see <code>mz_introspection.mz_expected_group_size_advice</code></a>. These may either be bucketed or monotonic (more efficient). These may consolidate their output, which will increase memory usage.</p> </li> <li> <p><code>Collated Multi-GroupAggregate</code> corresponds to an arbitrary mix of reductions of different types, which will be performed separately and then joined together.</p> </li> <li> <p><code>Non-incremental GroupAggregate</code> (e.g., window functions, <code>list_agg</code>) corresponds to a single non-incremental aggregation. These are the most computationally intensive reductions.</p> </li> </ol> <p>A parent <code>Fused Map/Filter/Project</code> can combine with this operator.</p>   **Can increase data size:** No **Uses memory:** ✅ <code>Distinct</code> and <code>Accumulable</code> aggregates use a moderate amount of memory (proportional to twice the output size). <code>MIN</code> and <code>MAX</code> aggregates can use significantly more memory. This can be improved by including group size hints in the query, see <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. <code>Non-incremental</code> aggregates use memory proportional to the input + output size. <code>Collated</code> aggregates use memory that is the sum of their constituents, plus some memory for the join at the end. | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Accumulable</span> <span class="n">GroupAggregate</span> </span></span><span class="line"><span class="cl">  <span class="n">Simple</span> <span class="n">aggregates</span><span class="p">:</span> <span class="k">count</span><span class="p">(</span><span class="o">*</span><span class="p">)</span> </span></span><span class="line"><span class="cl">  <span class="n">Post</span><span class="o">-</span><span class="n">process</span> <span class="k">Map</span><span class="o">/</span><span class="k">Filter</span><span class="o">/</span><span class="n">Project</span> </span></span><span class="line"><span class="cl">    <span class="k">Filter</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span> <span class="o">&gt;</span> <span class="mf">1</span><span class="p">)</span> </span></span></code></pre></div> |
| **TopK** | <p>Groups the input rows, sorts them according to some ordering, and returns at most <code>K</code> rows at some offset from the top of the list, where <code>K</code> is some (possibly computed) limit.</p> <p>There are three types of <code>TopK</code>. Two are special cased for monotonic inputs (i.e., inputs which never retract data).</p> <ol> <li><code>Monotonic Top1</code>.</li> <li><code>Monotonic TopK</code>, which may give an expression indicating the limit.</li> <li><code>Non-monotonic TopK</code>, a generic <code>TopK</code> plan.</li> </ol> <p>Each version of the <code>TopK</code> operator may include grouping, ordering, and limit directives.</p>   **Can increase data size:** No **Uses memory:** ✅ <code>Monotonic Top1</code> and <code>Monotonic TopK</code> use a moderate amount of memory. <code>Non-monotonic TopK</code> uses significantly more memory as the operator can significantly overestimate the group sizes. Consult <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Consolidating</span> <span class="n">Monotonic</span> <span class="n">TopK</span> </span></span><span class="line"><span class="cl">  <span class="k">Order</span> <span class="k">By</span> <span class="o">#</span><span class="mf">1</span> <span class="k">asc</span> <span class="n">nulls_last</span><span class="p">,</span> <span class="o">#</span><span class="mf">0</span> <span class="k">desc</span> <span class="n">nulls_first</span> </span></span><span class="line"><span class="cl">  <span class="k">Limit</span> <span class="mf">5</span> </span></span></code></pre></div> |
| **Negate Diffs** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.  **Can increase data size:** No **Uses memory:** No | <code>→Negate Diffs</code> |
| **Threshold Diffs** | Removes any rows with negative counts.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input and output size, twice. | <code>→Threshold Diffs</code> |
| **Union** | Combines its inputs into a unified output, emitting one row for each row on any input. (Corresponds to <code>UNION ALL</code> rather than <code>UNION</code>/<code>UNION DISTINCT</code>.)  **Can increase data size:** No **Uses memory:** ✅ A <code>Consolidating Union</code> will make moderate use of memory, particularly at hydration time. A <code>Union</code> that is not <code>Consolidating</code> will not consume memory. | <code>→Consolidating Union</code> |
| **Arrange** | Indicates a point that will become an <a href="/get-started/arrangements/#arrangements" >arrangement</a> in the dataflow engine, i.e., it will consume memory to cache results.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input size. Note that in the LIR / physical plan, <code>Arrange</code>/<code>ArrangeBy</code> almost always means that an arrangement will actually be created. (This is in contrast to the &ldquo;optimized&rdquo; plan, where an <code>ArrangeBy</code> being present in the plan often does not mean that an arrangement will actually be created.) | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Arrange</span> </span></span><span class="line"><span class="cl">    <span class="k">Keys</span><span class="p">:</span> <span class="mf">1</span> <span class="k">arrangement</span> <span class="n">available</span><span class="p">,</span> <span class="n">plus</span> <span class="k">raw</span> <span class="n">stream</span> </span></span><span class="line"><span class="cl">      <span class="k">Arrangement</span> <span class="mf">0</span><span class="p">:</span> <span class="o">#</span><span class="mf">0</span> </span></span></code></pre></div> |
| **Unarranged Raw Stream** | Indicates a point where data will be streamed (even if it is somehow already arranged).  **Can increase data size:** No **Uses memory:** No | <code>→Unarranged Raw Stream</code> |
| **With ... Return ...** | Introduces CTEs, i.e., makes it possible for sub-plans to be consumed multiple times by downstream operators.  **Can increase data size:** No **Uses memory:** No | <a href="/sql/explain-plan/#reading-plans" >See Reading plans</a> |
**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.


**In decorrelated and optimized plans:**
The following table lists the operators that are available in the optimized plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.| Operator | Description | Example |
| --- | --- | --- |
| **Constant** | Always produces the same collection of rows.  **Can increase data size:** No **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="n">Constant</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">((</span><span class="mf">1</span><span class="p">,</span> <span class="mf">2</span><span class="p">)</span> <span class="n">x</span> <span class="mf">2</span><span class="p">)</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">(</span><span class="mf">3</span><span class="p">,</span> <span class="mf">4</span><span class="p">)</span> </span></span></code></pre></div> |
| **Get** | Produces rows from either an existing relation (source/view/materialized view/table) or from a previous CTE in the same plan.  **Can increase data size:** No **Uses memory:** No | <code>Get materialize.public.ordered</code> |
| **Project** | Produces a subset of the <a href="/sql/explain-plan/#explain-plan-columns" >columns</a> in the input rows. See also <a href="/sql/explain-plan/#explain-plan-columns" >column numbering</a>.  **Can increase data size:** No **Uses memory:** No | <code>Project (#2, #3)</code> |
| **Map** | Appends the results of some scalar expressions to each row in the input.  **Can increase data size:** Each row has more data (i.e., longer rows but same number of rows). **Uses memory:** No | <code>Map (((#1 * 10000000dec) / #2) * 1000dec)</code> |
| **FlatMap** | Appends the result of some (one-to-many) <a href="/sql/functions/#table-functions" >table function</a> to each row in the input.  **Can increase data size:** Depends on the <a href="/sql/functions/#table-functions" >table function</a> used. **Uses memory:** No | <code>FlatMap jsonb_foreach(#3)</code> |
| **Filter** | Removes rows of the input for which some scalar predicates return <code>false</code>.  **Can increase data size:** No **Uses memory:** No | <code>Filter (#20 &lt; #21)</code> |
| **Join** | Returns combinations of rows from each input whenever some equality predicates are <code>true</code>.  **Can increase data size:** Depends on the join order and facts about the joined collections. **Uses memory:** ✅ The <code>Join</code> operator itself uses memory only for <code>type=differential</code> with more than 2 inputs. However, <code>Join</code> operators need <a href="/get-started/arrangements/#arrangements" >arrangements</a> on their inputs (shown by the <code>ArrangeBy</code> operator). These arrangements use memory proportional to the input sizes. If an input has an <a href="/transform-data/optimization/#join" >appropriate index</a>, then the arrangement of the index will be reused. | <code>Join on=(#1 = #2) type=delta</code> |
| **CrossJoin** | An alias for a <code>Join</code> with an empty predicate (emits all combinations). Note that not all cross joins are marked as <code>CrossJoin</code>: In a join with more than 2 inputs, it can happen that there is a cross join between some of the inputs. You can recognize this case by <code>ArrangeBy</code> operators having empty keys, i.e., <code>ArrangeBy keys=[[]]</code>.  **Can increase data size:** Cartesian product of the inputs (\|N\| x \|M\|). **Uses memory:** ✅ Uses memory for 3-way or more differential joins. | <code>CrossJoin type=differential</code> |
| **Reduce** | Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs.  **Can increase data size:** No **Uses memory:** ✅ <code>SUM</code>, <code>COUNT</code>, and most other aggregations use a moderate amount of memory (proportional either to twice the output size or to input size + output size). <code>MIN</code> and <code>MAX</code> aggregates can use significantly more memory. This can be improved by including group size hints in the query, see <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>Reduce group_by=[#0] aggregates=[max((#0 * #1))]</code> |
| **Distinct** | Alias for a <code>Reduce</code> with an empty aggregate list.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to twice the output size. | <code>Distinct</code> |
| **TopK** | Groups the input rows by some scalar expressions, sorts each group using the group key, removes the top <code>offset</code> rows in each group, and returns the next <code>limit</code> rows.  **Can increase data size:** No **Uses memory:** ✅ Can use significant amount as the operator can significantly overestimate the group sizes. Consult <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5</code> |
| **Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.  **Can increase data size:** No **Uses memory:** No | <code>Negate</code> |
| **Threshold** | Removes any rows with negative counts.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input and output size, twice. | <code>Threshold</code> |
| **Union** | Sums the counts of each row of all inputs. (Corresponds to <code>UNION ALL</code> rather than <code>UNION</code>/<code>UNION DISTINCT</code>.)  **Can increase data size:** No **Uses memory:** ✅ Moderate use of memory. Some union operators force consolidation, which results in a memory spike, largely at hydration time. | <code>Union</code> |
| **ArrangeBy** | Indicates a point that will become an <a href="/get-started/arrangements/#arrangements" >arrangement</a> in the dataflow engine (each <code>keys</code> element will be a different arrangement). Note that if an appropriate index already exists on the input or the output of the previous operator is already arranged with a key that is also requested here, then this operator will just pass on that existing arrangement instead of creating a new one.  **Can increase data size:** No **Uses memory:** ✅ Depends. If arrangements need to be created, they use memory proportional to the input size. | <code>ArrangeBy keys=[[#0]]</code> |
| **With ... Return ...** | Introduces CTEs, i.e., makes it possible for sub-plans to be consumed multiple times by downstream operators.  **Can increase data size:** No **Uses memory:** No | <a href="/sql/explain-plan/#reading-plans" >See Reading plans</a> |
**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.


**In raw plans:**
The following table lists the operators that are available in the raw plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.| Operator | Description | Example |
| --- | --- | --- |
| **Constant** | Always produces the same collection of rows.  **Can increase data size:** No **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="n">Constant</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">((</span><span class="mf">1</span><span class="p">,</span> <span class="mf">2</span><span class="p">)</span> <span class="n">x</span> <span class="mf">2</span><span class="p">)</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">(</span><span class="mf">3</span><span class="p">,</span> <span class="mf">4</span><span class="p">)</span> </span></span></code></pre></div> |
| **Get** | Produces rows from either an existing relation (source/view/materialized view/table) or from a previous CTE in the same plan.  **Can increase data size:** No **Uses memory:** No | <code>Get materialize.public.ordered</code> |
| **Project** | Produces a subset of the <a href="/sql/explain-plan/#explain-plan-columns" >columns</a> in the input rows. See also <a href="/sql/explain-plan/#explain-plan-columns" >column numbering</a>.  **Can increase data size:** No **Uses memory:** No | <code>Project (#2, #3)</code> |
| **Map** | Appends the results of some scalar expressions to each row in the input.  **Can increase data size:** Each row has more data (i.e., longer rows but same number of rows). **Uses memory:** No | <code>Map (((#1 * 10000000dec) / #2) * 1000dec)</code> |
| **CallTable** | Appends the result of some (one-to-many) <a href="/sql/functions/#table-functions" >table function</a> to each row in the input.  **Can increase data size:** Depends on the <a href="/sql/functions/#table-functions" >table function</a> used. **Uses memory:** No | <code>CallTable generate_series(1, 7, 1)</code> |
| **Filter** | Removes rows of the input for which some scalar predicates return <code>false</code>.  **Can increase data size:** No **Uses memory:** No | <code>Filter (#20 &lt; #21)</code> |
| **~Join** | Performs one of <code>INNER</code> / <code>LEFT</code> / <code>RIGHT</code> / <code>FULL OUTER</code> / <code>CROSS</code> join on the two inputs, using the given predicate.  **Can increase data size:** For <code>CrossJoin</code>s, Cartesian product of the inputs (\|N\| x \|M\|). Note that, in many cases, a join that shows up as a cross join in the RAW PLAN will actually be turned into an inner join in the OPTIMIZED PLAN, by making use of an equality WHERE condition. For other join types, depends on the join order and facts about the joined collections. **Uses memory:** ✅ Uses memory proportional to the input sizes, unless <a href="/transform-data/optimization/#join" >the inputs have appropriate indexes</a>. Certain joins with more than 2 inputs use additional memory, see details in the optimized plan. | <code>InnerJoin (#0 = #2)</code> |
| **Reduce** | Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs.  In the case where the group key is empty and the input is empty, returns a single row with the aggregate functions applied to the empty input collection.  **Can increase data size:** No **Uses memory:** ✅ <code>SUM</code>, <code>COUNT</code>, and most other aggregations use a moderate amount of memory (proportional either to twice the output size or to input size + output size). <code>MIN</code> and <code>MAX</code> aggregates can use significantly more memory. This can be improved by including group size hints in the query, see <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>Reduce group_by=[#0] aggregates=[max((#0 * #1))]</code> |
| **Distinct** | Removes duplicate copies of input rows.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to twice the output size. | <code>Distinct</code> |
| **TopK** | Groups the input rows by some scalar expressions, sorts each group using the group key, removes the top <code>offset</code> rows in each group, and returns the next <code>limit</code> rows.  **Can increase data size:** No **Uses memory:** ✅ Can use significant amount as the operator can significantly overestimate the group sizes. Consult <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5</code> |
| **Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.  **Can increase data size:** No **Uses memory:** No | <code>Negate</code> |
| **Threshold** | Removes any rows with negative counts.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input and output size, twice. | <code>Threshold</code> |
| **Union** | Sums the counts of each row of all inputs. (Corresponds to <code>UNION ALL</code> rather than <code>UNION</code>/<code>UNION DISTINCT</code>.)  **Can increase data size:** No **Uses memory:** ✅ Moderate use of memory. Some union operators force consolidation, which results in a memory spike, largely at hydration time. | <code>Union</code> |
| **With ... Return ...** | Introduces CTEs, i.e., makes it possible for sub-plans to be consumed multiple times by downstream operators.  **Can increase data size:** No **Uses memory:** No | <a href="/sql/explain-plan/#reading-plans" >See Reading plans</a> |
**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.




Operators are sometimes marked as `Fused ...`. This indicates that the operator is fused with its input, i.e., the operator below it. That is, if you see a `Fused X` operator above a `Y` operator:

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

- `USAGE` privileges on the schemas that all relations in the explainee are
  contained in.


---

## Explain plan operators


Materialize offers several output formats for [`EXPLAIN
PLAN`](/sql/explain-plan/) and debugging. LIR plans as rendered in
[`mz_introspection.mz_lir_mapping`](../../sql/system-catalog/mz_introspection/#mz_lir_mapping)
are deliberately succinct, while the plans in other formats give more detail.

The decorrelated and optimized plans from `EXPLAIN DECORRELATED PLAN
FOR ...`, `EXPLAIN LOCALLY OPTIMIZED PLAN FOR ...`, and `EXPLAIN
OPTIMIZED PLAN FOR ...` are in a mid-level representation that is
closer to LIR than SQL. The raw plans from `EXPLAIN RAW PLAN FOR ...`
are closer to SQL (and therefore less indicative of how the query will
actually run).



**In fully optimized physical (LIR) plans (Default):**
The following table lists the operators that are available in the LIR plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.| Operator | Description | Example |
| --- | --- | --- |
| **Constant** | Always produces the same collection of rows.  **Can increase data size:** No **Uses memory:** No | <code>→Constant (2 rows)</code> |
| **Stream, Arranged, Index Lookup, Read** | <p>Produces rows from either an existing relation (source/view/materialized view/table) or from a previous CTE in the same plan. A parent <code>Fused Map/Filter/Project</code> operator can combine with this operator.</p> <p>There are four types of <code>Get</code>.</p> <ol> <li> <p><code>Stream</code> indicates that the results are not <a href="/get-started/arrangements/#arrangements" >arranged</a> in memory and will be streamed directly.</p> </li> <li> <p><code>Arranged</code> indicates that the results are <a href="/get-started/arrangements/#arrangements" >arranged</a> in memory.</p> </li> <li> <p><code>Index Lookup</code> indicates the results will be <em>looked up</em> in an existing [arrangement]((/get-started/arrangements/#arrangements).</p> </li> <li> <p><code>Read</code> indicates that the results are unarranged, and will be processed as they arrive.</p> </li> </ol>   **Can increase data size:** No **Uses memory:** No | <code>Arranged materialize.public.t</code> |
| **Map/Filter/Project** | <p>Computes new columns (maps), filters columns, and projects away columns. Works row-by-row. Maps and filters will be printed, but projects will not.</p> <p>These may be marked as <strong><code>Fused</code></strong> <code>Map/Filter/Project</code>, which means they will combine with the operator beneath them to run more efficiently.</p>   **Can increase data size:** Each row may have more data, from the <code>Map</code>. Each row may also have less data, from the <code>Project</code>. There may be fewer rows, from the <code>Filter</code>. **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="k">Map</span><span class="o">/</span><span class="k">Filter</span><span class="o">/</span><span class="n">Project</span> </span></span><span class="line"><span class="cl">  <span class="k">Filter</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">}</span> <span class="o">&lt;</span> <span class="mf">7</span><span class="p">)</span> </span></span><span class="line"><span class="cl">  <span class="k">Map</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">}</span> <span class="o">+</span> <span class="o">#</span><span class="mf">1</span><span class="p">{</span><span class="n">b</span><span class="p">})</span> </span></span></code></pre></div> |
| **Table Function** | <p>Appends the result of some (one-to-many) <a href="/sql/functions/#table-functions" >table function</a> to each row in the input.</p> <p>A parent <code>Fused Table Function unnest_list</code> operator will fuse with its child <code>GroupAggregate</code> operator. Fusing these operator is part of how we efficiently compile window functions from SQL to dataflows.</p> <p>A parent <code>Fused Map/Filter/Project</code> can combine with this operator.</p>   **Can increase data size:** Depends on the <a href="/sql/functions/#table-functions" >table function</a> used. **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="k">Table</span> <span class="k">Function</span> <span class="n">generate_series</span><span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">},</span> <span class="o">#</span><span class="mf">1</span><span class="p">{</span><span class="n">b</span><span class="p">},</span> <span class="mf">1</span><span class="p">)</span> </span></span><span class="line"><span class="cl">  <span class="k">Input</span> <span class="k">key</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">})</span> </span></span></code></pre></div> |
| **Differential Join, Delta Join** | <p>Both join operators indicate the join ordering selected.</p> <p>Returns combinations of rows from each input whenever some equality predicates are <code>true</code>.</p> <p>Joins will indicate the join order of their children, starting from 0. For example, <code>Differential Join %1 » %0</code> will join its second child into its first.</p> <p>The <a href="/transform-data/optimization/#join" >two joins differ in performance characteristics</a>.</p>   **Can increase data size:** Depends on the join order and facts about the joined collections. **Uses memory:** ✅ Uses memory for 3-way or more differential joins. | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Differential</span> <span class="k">Join</span> <span class="o">%</span><span class="mf">1</span> <span class="err">»</span> <span class="o">%</span><span class="mf">0</span> </span></span><span class="line"><span class="cl">  <span class="k">Join</span> <span class="n">stage</span> <span class="o">%</span><span class="mf">0</span><span class="p">:</span> <span class="n">Lookup</span> <span class="k">key</span> <span class="o">#</span><span class="mf">0</span><span class="p">{</span><span class="n">a</span><span class="p">}</span> <span class="k">in</span> <span class="o">%</span><span class="mf">0</span> </span></span></code></pre></div> |
| **GroupAggregate** | <p>Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs.</p> <p>There are five types of <code>GroupAggregate</code>, ordered by increasing complexity:</p> <ol> <li> <p><code>Distinct GroupAggregate</code> corresponds to the SQL <code>DISTINCT</code> operator.</p> </li> <li> <p><code>Accumulable GroupAggregate</code> (e.g., <code>SUM</code>, <code>COUNT</code>) corresponds to several easy to implement aggregations that can be executed simultaneously and efficiently.</p> </li> <li> <p><code>Hierarchical GroupAggregate</code> (e.g., <code>MIN</code>, <code>MAX</code>) corresponds to an aggregation requiring a tower of arrangements. These can be either monotonic (more efficient) or bucketed. These may benefit from a hint; <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" >see <code>mz_introspection.mz_expected_group_size_advice</code></a>. These may either be bucketed or monotonic (more efficient). These may consolidate their output, which will increase memory usage.</p> </li> <li> <p><code>Collated Multi-GroupAggregate</code> corresponds to an arbitrary mix of reductions of different types, which will be performed separately and then joined together.</p> </li> <li> <p><code>Non-incremental GroupAggregate</code> (e.g., window functions, <code>list_agg</code>) corresponds to a single non-incremental aggregation. These are the most computationally intensive reductions.</p> </li> </ol> <p>A parent <code>Fused Map/Filter/Project</code> can combine with this operator.</p>   **Can increase data size:** No **Uses memory:** ✅ <code>Distinct</code> and <code>Accumulable</code> aggregates use a moderate amount of memory (proportional to twice the output size). <code>MIN</code> and <code>MAX</code> aggregates can use significantly more memory. This can be improved by including group size hints in the query, see <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. <code>Non-incremental</code> aggregates use memory proportional to the input + output size. <code>Collated</code> aggregates use memory that is the sum of their constituents, plus some memory for the join at the end. | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Accumulable</span> <span class="n">GroupAggregate</span> </span></span><span class="line"><span class="cl">  <span class="n">Simple</span> <span class="n">aggregates</span><span class="p">:</span> <span class="k">count</span><span class="p">(</span><span class="o">*</span><span class="p">)</span> </span></span><span class="line"><span class="cl">  <span class="n">Post</span><span class="o">-</span><span class="n">process</span> <span class="k">Map</span><span class="o">/</span><span class="k">Filter</span><span class="o">/</span><span class="n">Project</span> </span></span><span class="line"><span class="cl">    <span class="k">Filter</span><span class="p">:</span> <span class="p">(</span><span class="o">#</span><span class="mf">0</span> <span class="o">&gt;</span> <span class="mf">1</span><span class="p">)</span> </span></span></code></pre></div> |
| **TopK** | <p>Groups the input rows, sorts them according to some ordering, and returns at most <code>K</code> rows at some offset from the top of the list, where <code>K</code> is some (possibly computed) limit.</p> <p>There are three types of <code>TopK</code>. Two are special cased for monotonic inputs (i.e., inputs which never retract data).</p> <ol> <li><code>Monotonic Top1</code>.</li> <li><code>Monotonic TopK</code>, which may give an expression indicating the limit.</li> <li><code>Non-monotonic TopK</code>, a generic <code>TopK</code> plan.</li> </ol> <p>Each version of the <code>TopK</code> operator may include grouping, ordering, and limit directives.</p>   **Can increase data size:** No **Uses memory:** ✅ <code>Monotonic Top1</code> and <code>Monotonic TopK</code> use a moderate amount of memory. <code>Non-monotonic TopK</code> uses significantly more memory as the operator can significantly overestimate the group sizes. Consult <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Consolidating</span> <span class="n">Monotonic</span> <span class="n">TopK</span> </span></span><span class="line"><span class="cl">  <span class="k">Order</span> <span class="k">By</span> <span class="o">#</span><span class="mf">1</span> <span class="k">asc</span> <span class="n">nulls_last</span><span class="p">,</span> <span class="o">#</span><span class="mf">0</span> <span class="k">desc</span> <span class="n">nulls_first</span> </span></span><span class="line"><span class="cl">  <span class="k">Limit</span> <span class="mf">5</span> </span></span></code></pre></div> |
| **Negate Diffs** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.  **Can increase data size:** No **Uses memory:** No | <code>→Negate Diffs</code> |
| **Threshold Diffs** | Removes any rows with negative counts.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input and output size, twice. | <code>→Threshold Diffs</code> |
| **Union** | Combines its inputs into a unified output, emitting one row for each row on any input. (Corresponds to <code>UNION ALL</code> rather than <code>UNION</code>/<code>UNION DISTINCT</code>.)  **Can increase data size:** No **Uses memory:** ✅ A <code>Consolidating Union</code> will make moderate use of memory, particularly at hydration time. A <code>Union</code> that is not <code>Consolidating</code> will not consume memory. | <code>→Consolidating Union</code> |
| **Arrange** | Indicates a point that will become an <a href="/get-started/arrangements/#arrangements" >arrangement</a> in the dataflow engine, i.e., it will consume memory to cache results.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input size. Note that in the LIR / physical plan, <code>Arrange</code>/<code>ArrangeBy</code> almost always means that an arrangement will actually be created. (This is in contrast to the &ldquo;optimized&rdquo; plan, where an <code>ArrangeBy</code> being present in the plan often does not mean that an arrangement will actually be created.) | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="err">→</span><span class="n">Arrange</span> </span></span><span class="line"><span class="cl">    <span class="k">Keys</span><span class="p">:</span> <span class="mf">1</span> <span class="k">arrangement</span> <span class="n">available</span><span class="p">,</span> <span class="n">plus</span> <span class="k">raw</span> <span class="n">stream</span> </span></span><span class="line"><span class="cl">      <span class="k">Arrangement</span> <span class="mf">0</span><span class="p">:</span> <span class="o">#</span><span class="mf">0</span> </span></span></code></pre></div> |
| **Unarranged Raw Stream** | Indicates a point where data will be streamed (even if it is somehow already arranged).  **Can increase data size:** No **Uses memory:** No | <code>→Unarranged Raw Stream</code> |
| **With ... Return ...** | Introduces CTEs, i.e., makes it possible for sub-plans to be consumed multiple times by downstream operators.  **Can increase data size:** No **Uses memory:** No | <a href="/sql/explain-plan/#reading-plans" >See Reading plans</a> |
**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.


**In decorrelated and optimized plans:**
The following table lists the operators that are available in the optimized plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.| Operator | Description | Example |
| --- | --- | --- |
| **Constant** | Always produces the same collection of rows.  **Can increase data size:** No **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="n">Constant</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">((</span><span class="mf">1</span><span class="p">,</span> <span class="mf">2</span><span class="p">)</span> <span class="n">x</span> <span class="mf">2</span><span class="p">)</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">(</span><span class="mf">3</span><span class="p">,</span> <span class="mf">4</span><span class="p">)</span> </span></span></code></pre></div> |
| **Get** | Produces rows from either an existing relation (source/view/materialized view/table) or from a previous CTE in the same plan.  **Can increase data size:** No **Uses memory:** No | <code>Get materialize.public.ordered</code> |
| **Project** | Produces a subset of the <a href="/sql/explain-plan/#explain-plan-columns" >columns</a> in the input rows. See also <a href="/sql/explain-plan/#explain-plan-columns" >column numbering</a>.  **Can increase data size:** No **Uses memory:** No | <code>Project (#2, #3)</code> |
| **Map** | Appends the results of some scalar expressions to each row in the input.  **Can increase data size:** Each row has more data (i.e., longer rows but same number of rows). **Uses memory:** No | <code>Map (((#1 * 10000000dec) / #2) * 1000dec)</code> |
| **FlatMap** | Appends the result of some (one-to-many) <a href="/sql/functions/#table-functions" >table function</a> to each row in the input.  **Can increase data size:** Depends on the <a href="/sql/functions/#table-functions" >table function</a> used. **Uses memory:** No | <code>FlatMap jsonb_foreach(#3)</code> |
| **Filter** | Removes rows of the input for which some scalar predicates return <code>false</code>.  **Can increase data size:** No **Uses memory:** No | <code>Filter (#20 &lt; #21)</code> |
| **Join** | Returns combinations of rows from each input whenever some equality predicates are <code>true</code>.  **Can increase data size:** Depends on the join order and facts about the joined collections. **Uses memory:** ✅ The <code>Join</code> operator itself uses memory only for <code>type=differential</code> with more than 2 inputs. However, <code>Join</code> operators need <a href="/get-started/arrangements/#arrangements" >arrangements</a> on their inputs (shown by the <code>ArrangeBy</code> operator). These arrangements use memory proportional to the input sizes. If an input has an <a href="/transform-data/optimization/#join" >appropriate index</a>, then the arrangement of the index will be reused. | <code>Join on=(#1 = #2) type=delta</code> |
| **CrossJoin** | An alias for a <code>Join</code> with an empty predicate (emits all combinations). Note that not all cross joins are marked as <code>CrossJoin</code>: In a join with more than 2 inputs, it can happen that there is a cross join between some of the inputs. You can recognize this case by <code>ArrangeBy</code> operators having empty keys, i.e., <code>ArrangeBy keys=[[]]</code>.  **Can increase data size:** Cartesian product of the inputs (\|N\| x \|M\|). **Uses memory:** ✅ Uses memory for 3-way or more differential joins. | <code>CrossJoin type=differential</code> |
| **Reduce** | Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs.  **Can increase data size:** No **Uses memory:** ✅ <code>SUM</code>, <code>COUNT</code>, and most other aggregations use a moderate amount of memory (proportional either to twice the output size or to input size + output size). <code>MIN</code> and <code>MAX</code> aggregates can use significantly more memory. This can be improved by including group size hints in the query, see <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>Reduce group_by=[#0] aggregates=[max((#0 * #1))]</code> |
| **Distinct** | Alias for a <code>Reduce</code> with an empty aggregate list.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to twice the output size. | <code>Distinct</code> |
| **TopK** | Groups the input rows by some scalar expressions, sorts each group using the group key, removes the top <code>offset</code> rows in each group, and returns the next <code>limit</code> rows.  **Can increase data size:** No **Uses memory:** ✅ Can use significant amount as the operator can significantly overestimate the group sizes. Consult <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5</code> |
| **Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.  **Can increase data size:** No **Uses memory:** No | <code>Negate</code> |
| **Threshold** | Removes any rows with negative counts.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input and output size, twice. | <code>Threshold</code> |
| **Union** | Sums the counts of each row of all inputs. (Corresponds to <code>UNION ALL</code> rather than <code>UNION</code>/<code>UNION DISTINCT</code>.)  **Can increase data size:** No **Uses memory:** ✅ Moderate use of memory. Some union operators force consolidation, which results in a memory spike, largely at hydration time. | <code>Union</code> |
| **ArrangeBy** | Indicates a point that will become an <a href="/get-started/arrangements/#arrangements" >arrangement</a> in the dataflow engine (each <code>keys</code> element will be a different arrangement). Note that if an appropriate index already exists on the input or the output of the previous operator is already arranged with a key that is also requested here, then this operator will just pass on that existing arrangement instead of creating a new one.  **Can increase data size:** No **Uses memory:** ✅ Depends. If arrangements need to be created, they use memory proportional to the input size. | <code>ArrangeBy keys=[[#0]]</code> |
| **With ... Return ...** | Introduces CTEs, i.e., makes it possible for sub-plans to be consumed multiple times by downstream operators.  **Can increase data size:** No **Uses memory:** No | <a href="/sql/explain-plan/#reading-plans" >See Reading plans</a> |
**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.


**In raw plans:**
The following table lists the operators that are available in the raw plan.

- For those operators that require memory to maintain intermediate state, **Uses memory** is marked with **Yes**.
- For those operators that expand the data size (either rows or columns), **Can increase data size** is marked with **Yes**.| Operator | Description | Example |
| --- | --- | --- |
| **Constant** | Always produces the same collection of rows.  **Can increase data size:** No **Uses memory:** No | <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="n">Constant</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">((</span><span class="mf">1</span><span class="p">,</span> <span class="mf">2</span><span class="p">)</span> <span class="n">x</span> <span class="mf">2</span><span class="p">)</span> </span></span><span class="line"><span class="cl"><span class="o">-</span> <span class="p">(</span><span class="mf">3</span><span class="p">,</span> <span class="mf">4</span><span class="p">)</span> </span></span></code></pre></div> |
| **Get** | Produces rows from either an existing relation (source/view/materialized view/table) or from a previous CTE in the same plan.  **Can increase data size:** No **Uses memory:** No | <code>Get materialize.public.ordered</code> |
| **Project** | Produces a subset of the <a href="/sql/explain-plan/#explain-plan-columns" >columns</a> in the input rows. See also <a href="/sql/explain-plan/#explain-plan-columns" >column numbering</a>.  **Can increase data size:** No **Uses memory:** No | <code>Project (#2, #3)</code> |
| **Map** | Appends the results of some scalar expressions to each row in the input.  **Can increase data size:** Each row has more data (i.e., longer rows but same number of rows). **Uses memory:** No | <code>Map (((#1 * 10000000dec) / #2) * 1000dec)</code> |
| **CallTable** | Appends the result of some (one-to-many) <a href="/sql/functions/#table-functions" >table function</a> to each row in the input.  **Can increase data size:** Depends on the <a href="/sql/functions/#table-functions" >table function</a> used. **Uses memory:** No | <code>CallTable generate_series(1, 7, 1)</code> |
| **Filter** | Removes rows of the input for which some scalar predicates return <code>false</code>.  **Can increase data size:** No **Uses memory:** No | <code>Filter (#20 &lt; #21)</code> |
| **~Join** | Performs one of <code>INNER</code> / <code>LEFT</code> / <code>RIGHT</code> / <code>FULL OUTER</code> / <code>CROSS</code> join on the two inputs, using the given predicate.  **Can increase data size:** For <code>CrossJoin</code>s, Cartesian product of the inputs (\|N\| x \|M\|). Note that, in many cases, a join that shows up as a cross join in the RAW PLAN will actually be turned into an inner join in the OPTIMIZED PLAN, by making use of an equality WHERE condition. For other join types, depends on the join order and facts about the joined collections. **Uses memory:** ✅ Uses memory proportional to the input sizes, unless <a href="/transform-data/optimization/#join" >the inputs have appropriate indexes</a>. Certain joins with more than 2 inputs use additional memory, see details in the optimized plan. | <code>InnerJoin (#0 = #2)</code> |
| **Reduce** | Groups the input rows by some scalar expressions, reduces each group using some aggregate functions, and produces rows containing the group key and aggregate outputs.  In the case where the group key is empty and the input is empty, returns a single row with the aggregate functions applied to the empty input collection.  **Can increase data size:** No **Uses memory:** ✅ <code>SUM</code>, <code>COUNT</code>, and most other aggregations use a moderate amount of memory (proportional either to twice the output size or to input size + output size). <code>MIN</code> and <code>MAX</code> aggregates can use significantly more memory. This can be improved by including group size hints in the query, see <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>Reduce group_by=[#0] aggregates=[max((#0 * #1))]</code> |
| **Distinct** | Removes duplicate copies of input rows.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to twice the output size. | <code>Distinct</code> |
| **TopK** | Groups the input rows by some scalar expressions, sorts each group using the group key, removes the top <code>offset</code> rows in each group, and returns the next <code>limit</code> rows.  **Can increase data size:** No **Uses memory:** ✅ Can use significant amount as the operator can significantly overestimate the group sizes. Consult <a href="/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice" ><code>mz_introspection.mz_expected_group_size_advice</code></a>. | <code>TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5</code> |
| **Negate** | Negates the row counts of the input. This is usually used in combination with union to remove rows from the other union input.  **Can increase data size:** No **Uses memory:** No | <code>Negate</code> |
| **Threshold** | Removes any rows with negative counts.  **Can increase data size:** No **Uses memory:** ✅ Uses memory proportional to the input and output size, twice. | <code>Threshold</code> |
| **Union** | Sums the counts of each row of all inputs. (Corresponds to <code>UNION ALL</code> rather than <code>UNION</code>/<code>UNION DISTINCT</code>.)  **Can increase data size:** No **Uses memory:** ✅ Moderate use of memory. Some union operators force consolidation, which results in a memory spike, largely at hydration time. | <code>Union</code> |
| **With ... Return ...** | Introduces CTEs, i.e., makes it possible for sub-plans to be consumed multiple times by downstream operators.  **Can increase data size:** No **Uses memory:** No | <a href="/sql/explain-plan/#reading-plans" >See Reading plans</a> |
**Notes:**
- **Can increase data size:** Specifies whether the operator can increase the data size (can be the number of rows or the number of columns).
- **Uses memory:** Specifies whether the operator use memory to maintain state for its inputs.




Operators are sometimes marked as `Fused ...`. This indicates that the operator is fused with its input, i.e., the operator below it. That is, if you see a `Fused X` operator above a `Y` operator:

```
→Fused X
  →Y
```

Then the `X` and `Y` operators will be combined into a single, more efficient operator.

See also:

- [`EXPLAIN PLAn`](/sql/explain-plan/)


---

## EXPLAIN SCHEMA


`EXPLAIN KEY SCHEMA` or `EXPLAIN VALUE SCHEMA` shows the generated schemas for a `CREATE SINK` statement without creating the sink.

> **Warning:** `EXPLAIN` is not part of Materialize's stable interface and is not subject to
> our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
> change arbitrarily in future versions of Materialize.


## Syntax



```mzsql
EXPLAIN (KEY | VALUE) SCHEMA [AS JSON]
FOR CREATE SINK [<sink_name>]
[IN CLUSTER <cluster_name>]
FROM <item_name>
INTO KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, COMPRESSION TYPE <compression_type>]
  [, TRANSACTIONAL ID PREFIX '<transactional_id_prefix>']
  [, PARTITION BY = <expression>]
  [, PROGRESS GROUP ID PREFIX '<progress_group_id_prefix>']
  [, TOPIC REPLICATION FACTOR <replication_factor>]
  [, TOPIC PARTITION COUNT <partition_count>]
  [, TOPIC CONFIG <topic_config>]
)
[KEY ( <key_column> [, ...] ) [NOT ENFORCED]]
[HEADERS <headers_column>]
[FORMAT <sink_format_spec> | KEY FORMAT <sink_format_spec> VALUE FORMAT <sink_format_spec>]
[ENVELOPE (DEBEZIUM | UPSERT)]
[WITH (<with_option> [, ...])]

```

| Syntax element | Description |
| --- | --- |
| **KEY** \| **VALUE** | Specifies whether to explain the key schema or value schema for the sink.  |
| **AS JSON** | Optional. Format the explanation output as a JSON object. If not specified, the output is formatted as text.  |
| **FOR CREATE SINK** `[<sink_name>]` | The `CREATE SINK` statement to explain. The sink name is optional.  |
| **IN CLUSTER** `<cluster_name>` | Optional. The [cluster](/sql/create-cluster) to maintain this sink.  |
| **FROM** `<item_name>` | The name of the source, table, or materialized view you want to send to the sink.  |
| **INTO KAFKA CONNECTION** `<connection_name>` | The name of the Kafka connection to use in the sink. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection) documentation page.  |
| **TOPIC** `'<topic>'` | The name of the Kafka topic to write to.  |
| **COMPRESSION TYPE** `<compression_type>` | Optional. The type of compression to apply to messages before they are sent to Kafka: `none`, `gzip`, `snappy`, `lz4`, or `zstd`.  |
| **TRANSACTIONAL ID PREFIX** `'<transactional_id_prefix>'` | Optional. The prefix of the transactional ID to use when producing to the Kafka topic.  |
| **PARTITION BY** `= <expression>` | Optional. A SQL expression returning a hash that can be used for partition assignment. See [Partitioning](/sql/create-sink/kafka/#partitioning) for details.  |
| **PROGRESS GROUP ID PREFIX** `'<progress_group_id_prefix>'` | Optional. The prefix of the consumer group ID to use when reading from the progress topic.  |
| **TOPIC REPLICATION FACTOR** `<replication_factor>` | Optional. The replication factor to use when creating the Kafka topic (if the Kafka topic does not already exist).  |
| **TOPIC PARTITION COUNT** `<partition_count>` | Optional. The partition count to use when creating the Kafka topic (if the Kafka topic does not already exist).  |
| **TOPIC CONFIG** `<topic_config>` | Optional. Any topic-level configs to use when creating the Kafka topic (if the Kafka topic does not already exist). See the [Kafka documentation](https://kafka.apache.org/documentation/#topicconfigs) for available configs.  |
| **KEY** ( `<key_column>` [, ...] ) [**NOT ENFORCED**] | Optional. A list of columns to use as the Kafka message key. If unspecified, the Kafka key is left unset. When using the upsert envelope, the key must be unique. Use **NOT ENFORCED** to disable validation of key uniqueness. See [Upsert key selection](/sql/create-sink/kafka/#upsert-key-selection) for details.  |
| **HEADERS** `<headers_column>` | Optional. A column containing headers to add to each Kafka message emitted by the sink. The column must be of type `map[text => text]` or `map[text => bytea]`. See [Headers](/sql/create-sink/kafka/#headers) for details.  |
| **FORMAT** `<sink_format_spec>` | Optional. Specifies the format to use for both keys and values: `AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION <csr_connection_name>`, `JSON`, `TEXT`, or `BYTES`. See [Formats](/sql/create-sink/kafka/#formats) for details.  |
| **KEY FORMAT** `<sink_format_spec>` **VALUE FORMAT** `<sink_format_spec>` | Optional. Specifies the key format and value formats separately. See [Formats](/sql/create-sink/kafka/#formats) for details.  |
| **ENVELOPE** (`DEBEZIUM` \| `UPSERT`) | Optional. Specifies how changes to the sink's upstream relation are mapped to Kafka messages. Valid envelope types:  \| Envelope \| Description \| \|----------\|-------------\| \| `DEBEZIUM` \| The generated schemas have a [Debezium-style diff envelope](/sql/create-sink/kafka/#debezium) to capture changes in the input view or source. \| \| `UPSERT` \| The sink emits data with [upsert semantics](/sql/create-sink/kafka/#upsert). Requires a unique key specified using the `KEY` option. \|  |
| **WITH** (`<with_option>` [, ...]) | Optional. The following `<with_option>`s are supported:  \| Option \| Description \| \|--------\|-------------\| \| `SNAPSHOT = <snapshot>` \| Default: `true`. Whether to emit the consolidated results of the query before the sink was created at the start of the sink. To see only results after the sink is created, specify `WITH (SNAPSHOT = false)`. \|  |


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

- `USAGE` privileges on the schemas that all items in the query are contained
  in.


---

## EXPLAIN TIMESTAMP


`EXPLAIN TIMESTAMP` displays the timestamps used for a `SELECT` statement -- valuable information to investigate query delays.

> **Warning:** `EXPLAIN` is not part of Materialize's stable interface and is not subject to
> our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
> change arbitrarily in future versions of Materialize.


## Syntax



```mzsql
EXPLAIN TIMESTAMP [AS (TEXT | JSON)]
FOR <select_stmt>

```

| Syntax element | Description |
| --- | --- |
| **AS** (`TEXT` \| `JSON`) | Optional. Specifies the output format of the explanation:  \| Format \| Description \| \|--------\|-------------\| \| `TEXT` \| Format the explanation output as UTF-8 text (default). \| \| `JSON` \| Format the explanation output as a JSON object. \|  |
| **FOR** `<select_stmt>` | The [`SELECT`](/sql/select) statement to explain.  |


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

- `USAGE` privileges on the schemas that all relations in the query are
  contained in.


---

## FETCH


`FETCH` retrieves rows from a query using a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax



```mzsql
FETCH [ <count> | ALL ] [FROM] <cursor_name> [ WITH ( TIMEOUT = <interval> ) ]

```

| Syntax element | Description |
| --- | --- |
| `<count>` | The number of rows to retrieve. Defaults to `1` if unspecified.  |
| `ALL` | Indicates that there is no limit on the number of rows to be returned.  |
| `<cursor_name>` | The name of an open cursor.  |
| `TIMEOUT` | When fetching from a [`SUBSCRIBE`](/sql/subscribe) cursor, complete if there are no more rows ready after this timeout. The default will cause `FETCH` to wait for at least one row to be available.  |


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

> **Note:** The syntax supports the `ALL [PRIVILEGES]` shorthand to refer to all
> [*applicable* privileges](/sql/grant-privilege/#available-privileges) for the
> object type.




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

> **Note:** To read from a views or a materialized views, you must have `SELECT` privileges
> on the view/materialized views. That is, having `SELECT` privileges on the
> underlying objects defining the view/materialized view is insufficient.


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

> **Note:** Granting privileges via `ALL TABLES [...]` also applies to sources, views, and
> materialized views (for the applicable privileges).


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




## Privileges

The privileges required to execute this statement are:

- Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object is a schema.
- `USAGE` privileges on the containing schema if the affected object is namespaced by a schema.
- _superuser_ status if the privilege is a system privilege.

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



```mzsql
GRANT <role_name> [, ...] TO <grantee> [, ...]

```

| Syntax element | Description |
| --- | --- |
| `<role_name>` | The name of the role being granted.  |
| `<grantee>` | The name of the receiving role; i.e., the grantee.  |


## Examples

```mzsql
GRANT data_scientist TO joe;
```

```mzsql
GRANT data_scientist TO joe, mike;
```

## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.

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

> **Note:** The identifiers `"."` and `".."` are not allowed.


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

| | | | |
|--|--|--|--||`ABORT` |`ACCESS` |`ACTION` |`ADD`||`ADDED` |`ADDRESS` |`ADDRESSES` |`AFTER`||`AGGREGATE` |`AGGREGATION` |`ALIGNED` |`ALL`||`ALTER` |`ANALYSE` |`ANALYSIS` |`ANALYZE`||`AND` |`ANY` |`APPLY` |`ARITY`||`ARN` |`ARRANGED` |`ARRANGEMENT` |`ARRAY`||`AS` |`ASC` |`ASSERT` |`ASSUME`||`AT` |`AUCTION` |`AUTHORITY` |`AVAILABILITY`||`AVRO` |`AWS` |`BATCH` |`BEGIN`||`BETWEEN` |`BIGINT` |`BILLED` |`BODY`||`BOOLEAN` |`BOTH` |`BPCHAR` |`BROKEN`||`BROKER` |`BROKERS` |`BY` |`BYTES`||`CAPTURE` |`CARDINALITY` |`CASCADE` |`CASE`||`CAST` |`CATALOG` |`CERTIFICATE` |`CHAIN`||`CHAINS` |`CHAR` |`CHARACTER` |`CHARACTERISTICS`||`CHECK` |`CLASS` |`CLIENT` |`CLOCK`||`CLOSE` |`CLUSTER` |`CLUSTERS` |`COALESCE`||`COLLATE` |`COLUMN` |`COLUMNS` |`COMMENT`||`COMMIT` |`COMMITTED` |`COMPACTION` |`COMPATIBILITY`||`COMPRESSION` |`COMPUTE` |`COMPUTECTL` |`CONFIG`||`CONFLUENT` |`CONNECTION` |`CONNECTIONS` |`CONSTRAINT`||`CONTINUAL` |`COPY` |`COUNT` |`COUNTER`||`CPU` |`CREATE` |`CREATECLUSTER` |`CREATEDB`||`CREATENETWORKPOLICY` |`CREATEROLE` |`CREATION` |`CREDENTIAL`||`CROSS` |`CSE` |`CSV` |`CURRENT`||`CURSOR` |`DATABASE` |`DATABASES` |`DATUMS`||`DAY` |`DAYS` |`DEALLOCATE` |`DEBEZIUM`||`DEBUG` |`DEBUGGING` |`DEC` |`DECIMAL`||`DECLARE` |`DECODING` |`DECORRELATED` |`DEFAULT`||`DEFAULTS` |`DELETE` |`DELIMITED` |`DELIMITER`||`DELTA` |`DESC` |`DETAILS` |`DIRECTION`||`DISCARD` |`DISK` |`DISTINCT` |`DOC`||`DOT` |`DOUBLE` |`DROP` |`EAGER`||`ELEMENT` |`ELSE` |`ENABLE` |`END`||`ENDPOINT` |`ENFORCED` |`ENVELOPE` |`EQUIVALENCES`||`ERROR` |`ERRORS` |`ESCAPE` |`ESTIMATE`||`EVERY` |`EXCEPT` |`EXCLUDE` |`EXECUTE`||`EXISTS` |`EXPECTED` |`EXPLAIN` |`EXPOSE`||`EXPRESSIONS` |`EXTERNAL` |`EXTRACT` |`FACTOR`||`FALSE` |`FAST` |`FEATURES` |`FETCH`||`FIELDS` |`FILE` |`FILES` |`FILTER`||`FIRST` |`FIXPOINT` |`FLOAT` |`FOLLOWING`||`FOR` |`FOREIGN` |`FORMAT` |`FORWARD`||`FROM` |`FULL` |`FULLNAME` |`FUNCTION`||`FUSION` |`GENERATOR` |`GRANT` |`GREATEST`||`GROUP` |`GROUPS` |`HAVING` |`HEADER`||`HEADERS` |`HINTS` |`HISTORY` |`HOLD`||`HOST` |`HOUR` |`HOURS` |`HUMANIZED`||`HYDRATION` |`ICEBERG` |`ID` |`IDENTIFIERS`||`IDS` |`IF` |`IGNORE` |`ILIKE`||`IMPLEMENTATIONS` |`IMPORTED` |`IN` |`INCLUDE`||`INDEX` |`INDEXES` |`INFO` |`INHERIT`||`INLINE` |`INNER` |`INPUT` |`INSERT`||`INSIGHTS` |`INSPECT` |`INSTANCE` |`INT`||`INTEGER` |`INTERNAL` |`INTERSECT` |`INTERVAL`||`INTO` |`INTROSPECTION` |`IS` |`ISNULL`||`ISOLATION` |`JOIN` |`JOINS` |`JSON`||`KAFKA` |`KEY` |`KEYS` |`LAST`||`LATERAL` |`LATEST` |`LEADING` |`LEAST`||`LEFT` |`LEGACY` |`LETREC` |`LEVEL`||`LIKE` |`LIMIT` |`LINEAR` |`LIST`||`LOAD` |`LOCAL` |`LOCALLY` |`LOG`||`LOGICAL` |`LOGIN` |`LOWERING` |`MANAGED`||`MANUAL` |`MAP` |`MARKETING` |`MATERIALIZE`||`MATERIALIZED` |`MAX` |`MECHANISMS` |`MEMBERSHIP`||`MEMORY` |`MESSAGE` |`METADATA` |`MINUTE`||`MINUTES` |`MODE` |`MONTH` |`MONTHS`||`MUTUALLY` |`MYSQL` |`NAME` |`NAMES`||`NAMESPACE` |`NATURAL` |`NEGATIVE` |`NETWORK`||`NEW` |`NEXT` |`NFC` |`NFD`||`NFKC` |`NFKD` |`NO` |`NOCREATECLUSTER`||`NOCREATEDB` |`NOCREATEROLE` |`NODE` |`NOINHERIT`||`NOLOGIN` |`NON` |`NONE` |`NORMALIZE`||`NOSUPERUSER` |`NOT` |`NOTICE` |`NOTICES`||`NULL` |`NULLIF` |`NULLS` |`OBJECTS`||`OF` |`OFFSET` |`ON` |`ONLY`||`OPERATOR` |`OPTIMIZED` |`OPTIMIZER` |`OPTIONS`||`OR` |`ORDER` |`ORDINALITY` |`OUTER`||`OVER` |`OWNED` |`OWNER` |`PARTITION`||`PARTITIONS` |`PASSWORD` |`PATH` |`PATTERN`||`PHYSICAL` |`PLAN` |`PLANS` |`POLICIES`||`POLICY` |`PORT` |`POSITION` |`POSTGRES`||`PRECEDING` |`PRECISION` |`PREFIX` |`PREPARE`||`PRIMARY` |`PRIORITIZE` |`PRIVATELINK` |`PRIVILEGES`||`PROGRESS` |`PROJECTION` |`PROTOBUF` |`PROTOCOL`||`PUBLIC` |`PUBLICATION` |`PUSHDOWN` |`QUALIFY`||`QUERY` |`QUOTE` |`RAISE` |`RANGE`||`RATE` |`RAW` |`READ` |`READY`||`REAL` |`REASSIGN` |`RECURSION` |`RECURSIVE`||`REDACTED` |`REDUCE` |`REFERENCE` |`REFERENCES`||`REFRESH` |`REGEX` |`REGION` |`REGISTRY`||`RELATION` |`RENAME` |`REOPTIMIZE` |`REPEATABLE`||`REPLACE` |`REPLACEMENT` |`REPLAN` |`REPLICA`||`REPLICAS` |`REPLICATION` |`RESET` |`RESPECT`||`RESTRICT` |`RETAIN` |`RETURN` |`RETURNING`||`REVOKE` |`RIGHT` |`ROLE` |`ROLES`||`ROLLBACK` |`ROTATE` |`ROUNDS` |`ROW`||`ROWS` |`RULES` |`SASL` |`SCALE`||`SCHEDULE` |`SCHEMA` |`SCHEMAS` |`SCOPE`||`SECOND` |`SECONDS` |`SECRET` |`SECRETS`||`SECURITY` |`SEED` |`SELECT` |`SEQUENCES`||`SERIALIZABLE` |`SERVER` |`SERVICE` |`SESSION`||`SET` |`SHARD` |`SHOW` |`SINK`||`SINKS` |`SIZE` |`SKEW` |`SMALLINT`||`SNAPSHOT` |`SOME` |`SOURCE` |`SOURCES`||`SQL` |`SSH` |`SSL` |`START`||`STDIN` |`STDOUT` |`STORAGE` |`STORAGECTL`||`STRATEGY` |`STRICT` |`STRING` |`STRONG`||`SUBSCRIBE` |`SUBSOURCE` |`SUBSOURCES` |`SUBSTRING`||`SUBTREE` |`SUPERUSER` |`SWAP` |`SYNTAX`||`SYSTEM` |`TABLE` |`TABLES` |`TAIL`||`TASK` |`TASKS` |`TEMP` |`TEMPORARY`||`TEXT` |`THEN` |`TICK` |`TIES`||`TIME` |`TIMEOUT` |`TIMESTAMP` |`TIMESTAMPTZ`||`TIMING` |`TO` |`TOKEN` |`TOPIC`||`TPCH` |`TRACE` |`TRAILING` |`TRANSACTION`||`TRANSACTIONAL` |`TRANSFORM` |`TRIM` |`TRUE`||`TUNNEL` |`TYPE` |`TYPES` |`UNBOUNDED`||`UNCOMMITTED` |`UNION` |`UNIQUE` |`UNKNOWN`||`UNNEST` |`UNTIL` |`UP` |`UPDATE`||`UPSERT` |`URL` |`USAGE` |`USER`||`USERNAME` |`USERS` |`USING` |`VALIDATE`||`VALUE` |`VALUES` |`VARCHAR` |`VARIADIC`||`VARYING` |`VERBOSE` |`VERSION` |`VIEW`||`VIEWS` |`WAIT` |`WAREHOUSE` |`WARNING`||`WEBHOOK` |`WHEN` |`WHERE` |`WHILE`||`WINDOW` |`WIRE` |`WITH` |`WITHIN`||`WITHOUT` |`WORK` |`WORKERS` |`WORKLOAD`||`WRITE` |`YEAR` |`YEARS` |`ZONE`||`ZONES` |&nbsp; |&nbsp; |&nbsp;|


---

## INSERT


`INSERT` writes values to [user-defined tables](../create-table).

## Syntax



```mzsql
INSERT INTO <table_name> [[AS] <alias>] [ ( <col1> [, ...] ) ]
VALUES ( <expr1> [, ...] ) [, ...] | DEFAULT VALUES | <query>
[RETURNING <output_expr | *> [, ...] ]

```

| Syntax element | Description |
| --- | --- |
| `<table_name>` | The table to write values to.  |
| `<col1> [, ...]` | Correlates the inserted rows' columns to `<table_name>`'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column.  If some but not all of `<table_name>`'s columns are provided, the unprovided columns receive their type's default value, or `NULL` if no default value was specified.  |
| `VALUES ( <expr1> [, ...] ) [, ...]` | A list of tuples `( <expr1> [, ...] ) [, ...]` to insert. Each tuple contains expressions or values to be inserted into the columns. If a given column is nullable, a `NULL` value may be provided.  |
| `DEFAULT VALUES` | Insert a single row using the default value for all columns.  |
| `<query>` | A [`SELECT`](/sql/select) statement whose returned rows you want to write to the table.  |
| `RETURNING <output_expr \| *> [, ...]` | Causes `INSERT` to return values based on each inserted row: - `*` to return all columns - `<output_expr> [[AS] <alias>]`. [Aggregate functions](/sql/functions/#aggregate-functions) are not allowed in the `RETURNING` clause.  |


## Details

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

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `INSERT` privileges on `table_name`.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
    execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
    granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

## Related pages

- [`CREATE TABLE`](../create-table)
- [`DROP TABLE`](../drop-table)
- [`SELECT`](../select)


---

## M.1 to cc size mapping


The following table provides a general mapping of M.1 to cc cluster sizes:


**M.1 to cc:**

| M.1 Size | cc Size |
| --- | --- |
| <strong>M.1-nano</strong> | 25cc |
| <strong>M.1-nano</strong> | 50cc |
| <strong>M.1-micro</strong> | 100cc |
| <strong>M.1-xsmall</strong> | 200cc |
| <strong>M.1-small</strong> | 300cc or 400cc |
| <strong>M.1-medium</strong> | 600cc or 800cc |
| <strong>M.1-large</strong> | 800cc |
| <strong>M.1-1.5xlarge</strong> | 1200cc or 1600cc |
| <strong>M.1-2xlarge</strong> | 1600cc |
| <strong>M.1-3xlarge</strong> | 3200cc |
| <strong>M.1-4xlarge</strong> | 3200cc |
| <strong>M.1-8xlarge</strong> | 3200cc |
| <strong>M.1-16xlarge</strong> | 6400cc |
| <strong>M.1-32xlarge</strong> | 128C |
| <strong>M.1-64xlarge</strong> | 256C |
| <strong>M.1-128xlarge</strong> | 512C |


**cc to M.1:**

| cc Size | M.1 Size |
| --- | --- |
| 25cc | <strong>M.1-nano</strong> |
| 50cc | <strong>M.1-nano</strong> |
| 100cc | <strong>M.1-micro</strong> |
| 200cc | <strong>M.1-xsmall</strong> |
| 300cc | <strong>M.1-small</strong> |
| 400cc | <strong>M.1-small</strong> |
| 600cc | <strong>M.1-medium</strong> |
| 800cc | <strong>M.1-large</strong> or <strong>M.1-medium</strong> |
| 1200cc | <strong>M.1-1.5xlarge</strong> |
| 1600cc | <strong>M.1-2xlarge</strong> or <strong>M.1-1.5xlarge</strong> |
| 3200cc | <strong>M.1-8xlarge</strong> or <strong>M.1-4xlarge</strong> or <strong>M.1-3xlarge</strong> |
| 6400cc | <strong>M.1-16xlarge</strong> |
| 128C | <strong>M.1-32xlarge</strong> |
| 256C | <strong>M.1-64xlarge</strong> |
| 512C | <strong>M.1-128xlarge</strong> |




Some sizes have multiple mappings. When converting between M.1 and cc sizing, we
recommend choosing the larger mapping size first.


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

> **Note:** Unlike [PostgreSQL](https://www.postgresql.org/docs/current/sql-drop-owned.html), Materialize reassigns
> all objects across all databases, including the databases themselves.


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

- Role membership in `old_role` and `new_role`.

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

### Key configuration parameters

Name                                        | Default value             |  Description                                                          | Modifiable?
--------------------------------------------|---------------------------|-----------------------------------------------------------------------|--------------
`cluster`                                   | `quickstart`              | The current cluster.                                                  | Yes
`cluster_replica`                           |                           | The target cluster replica for `SELECT` queries.                      | Yes
`database`                                  | `materialize`             | The current database.                                                 | Yes
`search_path`                               | `public`                  | The schema search order for names that are not schema-qualified.      | Yes
`transaction_isolation`                     | `strict serializable`     | The transaction isolation level. For more information, see [Consistency guarantees](/overview/isolation-level/). <br/><br/> Accepts values: `serializable`, `strict serializable`. | Yes

### Other configuration parameters

Name                                        | Default value             |  Description                                                                                                                                                           | Modifiable?
--------------------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------
`allowed_cluster_replica_sizes`             | *Varies*                  | The allowed sizes when creating a new cluster replica.                                                                                                                 | [Contact support]
`application_name`                          |                           | The application name to be reported in statistics and logs. This parameter is typically set by an application upon connection to Materialize (e.g. `psql`).            | Yes
`auto_route_catalog_queries`                | `true`                    | Boolean flag indicating whether to force queries that depend only on system tables to run on the `mz_catalog_server` cluster for improved performance.                 | Yes
`client_encoding`                           | `UTF8`                    | The client's character set encoding. The only supported value is `UTF-8`.                                                                                              | Yes
`client_min_messages`                       | `notice`                  | The message levels that are sent to the client. <br/><br/> Accepts values: `debug5`, `debug4`, `debug3`, `debug2`, `debug1`, `log`, `notice`, `warning`, `error`. Each level includes all the levels that follow it. | Yes
`datestyle`                                 | `ISO, MDY`                | The display format for date and time values. The only supported value is `ISO, MDY`.                                                                                   | Yes
`emit_introspection_query_notice`           | `true`                    | Whether to print a notice when querying replica introspection relations.                                                                                               | Yes
`emit_timestamp_notice`                     | `false`                   | Boolean flag indicating whether to send a `notice` specifying query timestamps.                                                                                        | Yes
`emit_trace_id_notice`                      | `false`                   | Boolean flag indicating whether to send a `notice` specifying the trace ID, when available.                                                                            | Yes
`enable_rbac_checks`                        | `true`                    | Boolean flag indicating whether to apply RBAC checks before executing statements.                                                                                      | Yes
`enable_session_rbac_checks`                | `false`                   | Boolean flag indicating whether RBAC is enabled for the current session.                                                                                               | No
`extra_float_digits`                        | `3`                       | Boolean flag indicating whether to adjust the number of digits displayed for floating-point values.                                                                    | Yes
`failpoints`                                |                           | Allows failpoints to be dynamically activated.                                                                                                                         | No
`idle_in_transaction_session_timeout`       | `120s`                    | The maximum allowed duration that a session can sit idle in a transaction before being terminated. If this value is specified without units, it is taken as milliseconds (`ms`). A value of zero disables the timeout. | Yes
`integer_datetimes`                         | `true`                    | Boolean flag indicating whether the server uses 64-bit-integer dates and times.                                                                                        | No
`intervalstyle`                             | `postgres`                | The display format for interval values. The only supported value is `postgres`.                                                                                        | Yes
`is_superuser`                              |                           | Reports whether the current session is a _superuser_ with admin privileges.                                                                                            | No
`max_aws_privatelink_connections`           | `0`                       | The maximum number of AWS PrivateLink connections in the region, across all schemas.                                                                                   | [Contact support]
`max_clusters`                              | `10`                      | The maximum number of clusters in the region                                                                                                                           | [Contact support]
`max_connections`                           | `5000`                    | The maximum number of concurrent connections in the region                                                                                                             | [Contact support]
`max_credit_consumption_rate`               | `1024`                    | The maximum rate of credit consumption in a region. Credits are consumed based on the size of cluster replicas in use.                                                 | [Contact support]
`max_databases`                             | `1000`                    | The maximum number of databases in the region.                                                                                                                         | [Contact support]
`max_identifier_length`                     | `255`                     | The maximum length in bytes of object identifiers.                                                                                                                     | No
`max_kafka_connections`                     | `1000`                    | The maximum number of Kafka connections in the region, across all schemas.                                                                                             | [Contact support]
`max_mysql_connections`                     | `1000`                    | The maximum number of MySQL connections in the region, across all schemas.                                                                                             | [Contact support]
`max_objects_per_schema`                    | `1000`                    | The maximum number of objects in a schema.                                                                                                                             | [Contact support]
`max_postgres_connections`                  | `1000`                    | The maximum number of PostgreSQL connections in the region, across all schemas.                                                                                        | [Contact support]
`max_query_result_size`                     | `1073741824`              | The maximum size in bytes for a single query's result.                                                                                                                 | Yes
`max_replicas_per_cluster`                  | `5`                       | The maximum number of replicas of a single cluster                                                                                                                     | [Contact support]
`max_result_size`                           | `1 GiB`                   | The maximum size in bytes for a single query's result.                                                                                                                 | [Contact support]
`max_roles`                                 | `1000`                    | The maximum number of roles in the region.                                                                                                                             | [Contact support]
`max_schemas_per_database`                  | `1000`                    | The maximum number of schemas in a database.                                                                                                                           | [Contact support]
`max_secrets`                               | `100`                     | The maximum number of secrets in the region, across all schemas.                                                                                                       | [Contact support]
`max_sinks`                                 | `1000`                    | The maximum number of sinks in the region, across all schemas.                                                                                                         | [Contact support]
`max_sources`                               | `25`                      | The maximum number of sources in the region, across all schemas.                                                                                                       | [Contact support]
`max_tables`                                | `200`                     | The maximum number of tables in the region, across all schemas                                                                                                         | [Contact support]
`mz_version`                                | Version-dependent         | Shows the Materialize server version.                                                                                                                                  | No
`network_policy`                            | `default`                 | The default network policy for the region. | Yes
`real_time_recency`                         | `false`                   | Boolean flag indicating whether [real-time recency](/get-started/isolation-level/#real-time-recency) is enabled for the current session.                               | [Contact support]
`real_time_recency_timeout`                 | `10s`                     | Sets the maximum allowed duration of `SELECT` statements that actively use [real-time recency](/get-started/isolation-level/#real-time-recency). If this value is specified without units, it is taken as milliseconds (`ms`).                      | Yes
`server_version_num`                        | Version-dependent         | The PostgreSQL compatible server version as an integer.                                                                                                                | No
`server_version`                            | Version-dependent         | The PostgreSQL compatible server version.                                                                                                                              | No
`sql_safe_updates`                          | `false`                   | Boolean flag indicating whether to prohibit SQL statements that may be overly destructive.                                                                             | Yes
`standard_conforming_strings`               | `true`                    | Boolean flag indicating whether ordinary string literals (`'...'`) should treat backslashes literally. The only supported value is `true`.                             | Yes
`statement_timeout`                         | `10s`                     | The maximum allowed duration of the read portion of write operations; i.e., the `SELECT` portion of `INSERT INTO ... (SELECT ...)`; the `WHERE` portion of `UPDATE ... WHERE ...` and `DELETE FROM ... WHERE ...`. If this value is specified without units, it is taken as milliseconds (`ms`). | Yes
`timezone`                                  | `UTC`                     | The time zone for displaying and interpreting timestamps. The only supported value is `UTC`.                                                                           | Yes

[Contact support]: /support

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

- `CREATEROLE` privileges on the systems.

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



```mzsql
[WITH <cte_binding> [, ...]]
SELECT [ALL | DISTINCT [ON ( <col_ref> [, ...] )]]
  <target_elem> [, ...]
[FROM <table_expr> [, ...] [<join_expr>]]
[WHERE <expression>]
[GROUP BY <col_ref> [, ...]]
[OPTIONS ( <option> = <val> [, ...] )]
[HAVING <expression>]
[ORDER BY <col_ref> [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
[LIMIT <expression>]
[OFFSET <integer>]
[{UNION | INTERSECT | EXCEPT} [ALL | DISTINCT] <another_select_stmt>]

```

| Syntax element | Description |
| --- | --- |
| **WITH** `<cte_binding>` [, ...] | Optional. [Common table expressions](#common-table-expressions-ctes) (CTEs) for this query. See [Regular CTEs](#regular-ctes) for details.  |
| **ALL** \| **DISTINCT** [**ON** ( `<col_ref>` [, ...] )] | Optional. Specifies which rows to return:  \| Option \| Description \| \|--------\|-------------\| \| `ALL` \| Return all rows from query (default). \| \| `DISTINCT` \| <a id="select-distinct"></a>Return only distinct values. \| \| `DISTINCT ON ( <col_ref> [, ...] )` \| <a id="select-distinct-on"></a>Return only the first row with a distinct value for `<col_ref>`. If an `ORDER BY` clause is also present, then `DISTINCT ON` will respect that ordering when choosing which row to return for each distinct value. You should start the `ORDER BY` clause with the same `<col_ref>` as the `DISTINCT ON` clause. \|  |
| `<target_elem>` [, ...] | The columns or expressions to return. Can include column names, functions, or expressions.  |
| **FROM** `<table_expr>` [, ...] | The tables you want to read from. These can be table names, other `SELECT` statements, [Common Table Expressions](#common-table-expressions-ctes) (CTEs), or [table function calls](/sql/functions/table-functions).  |
| `<join_expr>` | Optional. A join expression to combine table expressions. For more details, see the [`JOIN` documentation](/sql/select/join/).  |
| **WHERE** `<expression>` | Optional. Filter tuples by `<expression>`.  |
| **GROUP BY** `<col_ref>` [, ...] | Optional. Group aggregations by `<col_ref>`. Column references may be the name of an output column, the ordinal number of an output column, or an arbitrary expression of only input columns.  |
| **OPTIONS** ( `<option>` = `<val>` [, ...] ) | Optional. Specify one or more [query hints](#query-hints). Valid hints:  \| Hint \| Value type \| Description \| \|------\|------------\|-------------\| \| `AGGREGATE INPUT GROUP SIZE` \| `uint8` \| How many rows will have the same group key in an aggregation. Materialize can render `min` and `max` expressions more efficiently with this information. \| \| `DISTINCT ON INPUT GROUP SIZE` \| `uint8` \| How many rows will have the same group key in a `DISTINCT ON` expression. Materialize can render [Top K patterns](/transform-data/idiomatic-materialize-sql/top-k/) based on `DISTINCT ON` more efficiently with this information. \| \| `LIMIT INPUT GROUP SIZE` \| `uint8` \| How many rows will be given as a group to a `LIMIT` restriction. Materialize can render [Top K patterns](/transform-data/idiomatic-materialize-sql/top-k/) based on `LIMIT` more efficiently with this information. \|  |
| **HAVING** `<expression>` | Optional. Filter aggregations by `<expression>`.  |
| **ORDER BY** `<col_ref>` [**ASC** \| **DESC**] [**NULLS FIRST** \| **NULLS LAST**] [, ...] | Optional. Sort results in either `ASC` (default) or `DESC` order. Use the `NULLS FIRST` and `NULLS LAST` options to determine whether nulls appear before or after non-null values in the sort ordering (default: `NULLS LAST` for `ASC`, `NULLS FIRST` for `DESC`). Column references may be the name of an output column, the ordinal number of an output column, or an arbitrary expression of only input columns.  |
| **LIMIT** `<expression>` | Optional. Limit the number of returned results to `<expression>`.  |
| **OFFSET** `<integer>` | Optional. Skip the first `<integer>` number of rows.  |
| **UNION** [**ALL** \| **DISTINCT**] `<another_select_stmt>` | Optional. Records present in `select_stmt` or `another_select_stmt`. `DISTINCT` returns only unique rows from these results (implied default). With `ALL` specified, each record occurs a number of times equal to the sum of the times it occurs in each input statement.  |
| **INTERSECT** [**ALL** \| **DISTINCT**] `<another_select_stmt>` | Optional. Records present in both `select_stmt` and `another_select_stmt`. `DISTINCT` returns only unique rows from these results (implied default). With `ALL` specified, each record occurs a number of times equal to the lesser of the times it occurs in each input statement.  |
| **EXCEPT** [**ALL** \| **DISTINCT**] `<another_select_stmt>` | Optional. Records present in `select_stmt` but not in `another_select_stmt`. `DISTINCT` returns only unique rows from these results (implied default). With `ALL` specified, each record occurs a number of times equal to the times it occurs in `select_stmt` less the times it occurs in `another_select_stmt`, or not at all if the former is greater than latter.  |


### Common table expressions (CTEs)

#### Regular CTEs



```mzsql
WITH <cte_ident> [( <col_ident> [, ...] )] AS ( <select_stmt> )
  [, <cte_ident> [( <col_ident> [, ...] )] AS ( <select_stmt> ) [, ...]]
<select_stmt>

```

| Syntax element | Description |
| --- | --- |
| `<cte_ident>` | The name of the common table expression (CTE).  |
| ( `<col_ident>` [, ...] ) | Optional. Rename the CTE's columns to the list of identifiers. The number of identifiers must match the number of columns returned by the CTE's `select_stmt`.  |
| **AS** ( `<select_stmt>` ) | The `SELECT` statement that defines the CTE. Any `cte_ident` alias can be referenced in subsequent `cte_binding` definitions and in the final `select_stmt`.  |


#### Recursive CTEs



```mzsql
WITH MUTUALLY RECURSIVE
  [((RETURN AT | ERROR AT) RECURSION LIMIT <limit>)]
  <cte_ident> ( <col_ident> <col_type> [, ...] ) AS ( <select_stmt> )
  [, <cte_ident> ( <col_ident> <col_type> [, ...] ) AS ( <select_stmt> ) [, ...]]
<select_stmt>

```

| Syntax element | Description |
| --- | --- |
| **(RETURN AT \| ERROR AT) RECURSION LIMIT** `<limit>` | Optional. Control the recursion behavior:  \| Option \| Description \| \|--------\|-------------\| \| `RETURN AT RECURSION LIMIT <limit>` \| Stop the fixpoint computation after `<limit>` iterations and use the current values computed for each recursive CTE binding in the `select_stmt`. Useful when debugging and validating the correctness of recursive queries. \| \| `ERROR AT RECURSION LIMIT <limit>` \| Stop the fixpoint computation after `<limit>` iterations and fail the query with an error. A good safeguard against accidentally running a non-terminating dataflow in production clusters. \|  |
| `<cte_ident>` ( `<col_ident>` `<col_type>` [, ...] ) | A binding that gives the SQL fragment defined under `select_stmt` a `cte_ident` alias. Unlike regular CTEs, a recursive CTE binding must explicitly state its type as a comma-separated list of (`col_ident` `col_type`) pairs. This alias can be used in the same binding or in all other (preceding and subsequent) bindings in the enclosing recursive CTE block.  |
| **AS** ( `<select_stmt>` ) | The `SELECT` statement that defines the recursive CTE. Any `cte_ident` alias can be referenced in all `recursive_cte_binding` definitions that live under the same block, as well as in the final `select_stmt` for that block.  |


For details and examples, see the [Recursive CTEs](/sql/select/recursive-ctes) page.

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

- `SELECT` privileges on all **directly** referenced relations in the query. If
  the directly referenced relation is a view or materialized view: - `SELECT` privileges are required only on the directly referenced
  view/materialized view. `SELECT` privileges are **not** required for the
  underlying relations referenced in the view/materialized view definition
  unless those relations themselves are directly referenced in the query.

- However, the owner of the view/materialized view (including those with
  **superuser** privileges) must have all required `SELECT` and `USAGE`
  privileges to run the view definition regardless of who is selecting from the
  view/materialized view.

- `USAGE` privileges on the schemas that contain the relations in the query.
- `USAGE` privileges on the active cluster.

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

### Key configuration parameters

Name                                        | Default value             |  Description                                                          | Modifiable?
--------------------------------------------|---------------------------|-----------------------------------------------------------------------|--------------
`cluster`                                   | `quickstart`              | The current cluster.                                                  | Yes
`cluster_replica`                           |                           | The target cluster replica for `SELECT` queries.                      | Yes
`database`                                  | `materialize`             | The current database.                                                 | Yes
`search_path`                               | `public`                  | The schema search order for names that are not schema-qualified.      | Yes
`transaction_isolation`                     | `strict serializable`     | The transaction isolation level. For more information, see [Consistency guarantees](/overview/isolation-level/). <br/><br/> Accepts values: `serializable`, `strict serializable`. | Yes

### Other configuration parameters

Name                                        | Default value             |  Description                                                                                                                                                           | Modifiable?
--------------------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------
`allowed_cluster_replica_sizes`             | *Varies*                  | The allowed sizes when creating a new cluster replica.                                                                                                                 | [Contact support]
`application_name`                          |                           | The application name to be reported in statistics and logs. This parameter is typically set by an application upon connection to Materialize (e.g. `psql`).            | Yes
`auto_route_catalog_queries`                | `true`                    | Boolean flag indicating whether to force queries that depend only on system tables to run on the `mz_catalog_server` cluster for improved performance.                 | Yes
`client_encoding`                           | `UTF8`                    | The client's character set encoding. The only supported value is `UTF-8`.                                                                                              | Yes
`client_min_messages`                       | `notice`                  | The message levels that are sent to the client. <br/><br/> Accepts values: `debug5`, `debug4`, `debug3`, `debug2`, `debug1`, `log`, `notice`, `warning`, `error`. Each level includes all the levels that follow it. | Yes
`datestyle`                                 | `ISO, MDY`                | The display format for date and time values. The only supported value is `ISO, MDY`.                                                                                   | Yes
`emit_introspection_query_notice`           | `true`                    | Whether to print a notice when querying replica introspection relations.                                                                                               | Yes
`emit_timestamp_notice`                     | `false`                   | Boolean flag indicating whether to send a `notice` specifying query timestamps.                                                                                        | Yes
`emit_trace_id_notice`                      | `false`                   | Boolean flag indicating whether to send a `notice` specifying the trace ID, when available.                                                                            | Yes
`enable_rbac_checks`                        | `true`                    | Boolean flag indicating whether to apply RBAC checks before executing statements.                                                                                      | Yes
`enable_session_rbac_checks`                | `false`                   | Boolean flag indicating whether RBAC is enabled for the current session.                                                                                               | No
`extra_float_digits`                        | `3`                       | Boolean flag indicating whether to adjust the number of digits displayed for floating-point values.                                                                    | Yes
`failpoints`                                |                           | Allows failpoints to be dynamically activated.                                                                                                                         | No
`idle_in_transaction_session_timeout`       | `120s`                    | The maximum allowed duration that a session can sit idle in a transaction before being terminated. If this value is specified without units, it is taken as milliseconds (`ms`). A value of zero disables the timeout. | Yes
`integer_datetimes`                         | `true`                    | Boolean flag indicating whether the server uses 64-bit-integer dates and times.                                                                                        | No
`intervalstyle`                             | `postgres`                | The display format for interval values. The only supported value is `postgres`.                                                                                        | Yes
`is_superuser`                              |                           | Reports whether the current session is a _superuser_ with admin privileges.                                                                                            | No
`max_aws_privatelink_connections`           | `0`                       | The maximum number of AWS PrivateLink connections in the region, across all schemas.                                                                                   | [Contact support]
`max_clusters`                              | `10`                      | The maximum number of clusters in the region                                                                                                                           | [Contact support]
`max_connections`                           | `5000`                    | The maximum number of concurrent connections in the region                                                                                                             | [Contact support]
`max_credit_consumption_rate`               | `1024`                    | The maximum rate of credit consumption in a region. Credits are consumed based on the size of cluster replicas in use.                                                 | [Contact support]
`max_databases`                             | `1000`                    | The maximum number of databases in the region.                                                                                                                         | [Contact support]
`max_identifier_length`                     | `255`                     | The maximum length in bytes of object identifiers.                                                                                                                     | No
`max_kafka_connections`                     | `1000`                    | The maximum number of Kafka connections in the region, across all schemas.                                                                                             | [Contact support]
`max_mysql_connections`                     | `1000`                    | The maximum number of MySQL connections in the region, across all schemas.                                                                                             | [Contact support]
`max_objects_per_schema`                    | `1000`                    | The maximum number of objects in a schema.                                                                                                                             | [Contact support]
`max_postgres_connections`                  | `1000`                    | The maximum number of PostgreSQL connections in the region, across all schemas.                                                                                        | [Contact support]
`max_query_result_size`                     | `1073741824`              | The maximum size in bytes for a single query's result.                                                                                                                 | Yes
`max_replicas_per_cluster`                  | `5`                       | The maximum number of replicas of a single cluster                                                                                                                     | [Contact support]
`max_result_size`                           | `1 GiB`                   | The maximum size in bytes for a single query's result.                                                                                                                 | [Contact support]
`max_roles`                                 | `1000`                    | The maximum number of roles in the region.                                                                                                                             | [Contact support]
`max_schemas_per_database`                  | `1000`                    | The maximum number of schemas in a database.                                                                                                                           | [Contact support]
`max_secrets`                               | `100`                     | The maximum number of secrets in the region, across all schemas.                                                                                                       | [Contact support]
`max_sinks`                                 | `1000`                    | The maximum number of sinks in the region, across all schemas.                                                                                                         | [Contact support]
`max_sources`                               | `25`                      | The maximum number of sources in the region, across all schemas.                                                                                                       | [Contact support]
`max_tables`                                | `200`                     | The maximum number of tables in the region, across all schemas                                                                                                         | [Contact support]
`mz_version`                                | Version-dependent         | Shows the Materialize server version.                                                                                                                                  | No
`network_policy`                            | `default`                 | The default network policy for the region. | Yes
`real_time_recency`                         | `false`                   | Boolean flag indicating whether [real-time recency](/get-started/isolation-level/#real-time-recency) is enabled for the current session.                               | [Contact support]
`real_time_recency_timeout`                 | `10s`                     | Sets the maximum allowed duration of `SELECT` statements that actively use [real-time recency](/get-started/isolation-level/#real-time-recency). If this value is specified without units, it is taken as milliseconds (`ms`).                      | Yes
`server_version_num`                        | Version-dependent         | The PostgreSQL compatible server version as an integer.                                                                                                                | No
`server_version`                            | Version-dependent         | The PostgreSQL compatible server version.                                                                                                                              | No
`sql_safe_updates`                          | `false`                   | Boolean flag indicating whether to prohibit SQL statements that may be overly destructive.                                                                             | Yes
`standard_conforming_strings`               | `true`                    | Boolean flag indicating whether ordinary string literals (`'...'`) should treat backslashes literally. The only supported value is `true`.                             | Yes
`statement_timeout`                         | `10s`                     | The maximum allowed duration of the read portion of write operations; i.e., the `SELECT` portion of `INSERT INTO ... (SELECT ...)`; the `WHERE` portion of `UPDATE ... WHERE ...` and `DELETE FROM ... WHERE ...`. If this value is specified without units, it is taken as milliseconds (`ms`). | Yes
`timezone`                                  | `UTC`                     | The time zone for displaying and interpreting timestamps. The only supported value is `UTC`.                                                                           | Yes

[Contact support]: /support

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

### Key configuration parameters

Name                                        | Default value             |  Description                                                          | Modifiable?
--------------------------------------------|---------------------------|-----------------------------------------------------------------------|--------------
`cluster`                                   | `quickstart`              | The current cluster.                                                  | Yes
`cluster_replica`                           |                           | The target cluster replica for `SELECT` queries.                      | Yes
`database`                                  | `materialize`             | The current database.                                                 | Yes
`search_path`                               | `public`                  | The schema search order for names that are not schema-qualified.      | Yes
`transaction_isolation`                     | `strict serializable`     | The transaction isolation level. For more information, see [Consistency guarantees](/overview/isolation-level/). <br/><br/> Accepts values: `serializable`, `strict serializable`. | Yes

### Other configuration parameters

Name                                        | Default value             |  Description                                                                                                                                                           | Modifiable?
--------------------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------
`allowed_cluster_replica_sizes`             | *Varies*                  | The allowed sizes when creating a new cluster replica.                                                                                                                 | [Contact support]
`application_name`                          |                           | The application name to be reported in statistics and logs. This parameter is typically set by an application upon connection to Materialize (e.g. `psql`).            | Yes
`auto_route_catalog_queries`                | `true`                    | Boolean flag indicating whether to force queries that depend only on system tables to run on the `mz_catalog_server` cluster for improved performance.                 | Yes
`client_encoding`                           | `UTF8`                    | The client's character set encoding. The only supported value is `UTF-8`.                                                                                              | Yes
`client_min_messages`                       | `notice`                  | The message levels that are sent to the client. <br/><br/> Accepts values: `debug5`, `debug4`, `debug3`, `debug2`, `debug1`, `log`, `notice`, `warning`, `error`. Each level includes all the levels that follow it. | Yes
`datestyle`                                 | `ISO, MDY`                | The display format for date and time values. The only supported value is `ISO, MDY`.                                                                                   | Yes
`emit_introspection_query_notice`           | `true`                    | Whether to print a notice when querying replica introspection relations.                                                                                               | Yes
`emit_timestamp_notice`                     | `false`                   | Boolean flag indicating whether to send a `notice` specifying query timestamps.                                                                                        | Yes
`emit_trace_id_notice`                      | `false`                   | Boolean flag indicating whether to send a `notice` specifying the trace ID, when available.                                                                            | Yes
`enable_rbac_checks`                        | `true`                    | Boolean flag indicating whether to apply RBAC checks before executing statements.                                                                                      | Yes
`enable_session_rbac_checks`                | `false`                   | Boolean flag indicating whether RBAC is enabled for the current session.                                                                                               | No
`extra_float_digits`                        | `3`                       | Boolean flag indicating whether to adjust the number of digits displayed for floating-point values.                                                                    | Yes
`failpoints`                                |                           | Allows failpoints to be dynamically activated.                                                                                                                         | No
`idle_in_transaction_session_timeout`       | `120s`                    | The maximum allowed duration that a session can sit idle in a transaction before being terminated. If this value is specified without units, it is taken as milliseconds (`ms`). A value of zero disables the timeout. | Yes
`integer_datetimes`                         | `true`                    | Boolean flag indicating whether the server uses 64-bit-integer dates and times.                                                                                        | No
`intervalstyle`                             | `postgres`                | The display format for interval values. The only supported value is `postgres`.                                                                                        | Yes
`is_superuser`                              |                           | Reports whether the current session is a _superuser_ with admin privileges.                                                                                            | No
`max_aws_privatelink_connections`           | `0`                       | The maximum number of AWS PrivateLink connections in the region, across all schemas.                                                                                   | [Contact support]
`max_clusters`                              | `10`                      | The maximum number of clusters in the region                                                                                                                           | [Contact support]
`max_connections`                           | `5000`                    | The maximum number of concurrent connections in the region                                                                                                             | [Contact support]
`max_credit_consumption_rate`               | `1024`                    | The maximum rate of credit consumption in a region. Credits are consumed based on the size of cluster replicas in use.                                                 | [Contact support]
`max_databases`                             | `1000`                    | The maximum number of databases in the region.                                                                                                                         | [Contact support]
`max_identifier_length`                     | `255`                     | The maximum length in bytes of object identifiers.                                                                                                                     | No
`max_kafka_connections`                     | `1000`                    | The maximum number of Kafka connections in the region, across all schemas.                                                                                             | [Contact support]
`max_mysql_connections`                     | `1000`                    | The maximum number of MySQL connections in the region, across all schemas.                                                                                             | [Contact support]
`max_objects_per_schema`                    | `1000`                    | The maximum number of objects in a schema.                                                                                                                             | [Contact support]
`max_postgres_connections`                  | `1000`                    | The maximum number of PostgreSQL connections in the region, across all schemas.                                                                                        | [Contact support]
`max_query_result_size`                     | `1073741824`              | The maximum size in bytes for a single query's result.                                                                                                                 | Yes
`max_replicas_per_cluster`                  | `5`                       | The maximum number of replicas of a single cluster                                                                                                                     | [Contact support]
`max_result_size`                           | `1 GiB`                   | The maximum size in bytes for a single query's result.                                                                                                                 | [Contact support]
`max_roles`                                 | `1000`                    | The maximum number of roles in the region.                                                                                                                             | [Contact support]
`max_schemas_per_database`                  | `1000`                    | The maximum number of schemas in a database.                                                                                                                           | [Contact support]
`max_secrets`                               | `100`                     | The maximum number of secrets in the region, across all schemas.                                                                                                       | [Contact support]
`max_sinks`                                 | `1000`                    | The maximum number of sinks in the region, across all schemas.                                                                                                         | [Contact support]
`max_sources`                               | `25`                      | The maximum number of sources in the region, across all schemas.                                                                                                       | [Contact support]
`max_tables`                                | `200`                     | The maximum number of tables in the region, across all schemas                                                                                                         | [Contact support]
`mz_version`                                | Version-dependent         | Shows the Materialize server version.                                                                                                                                  | No
`network_policy`                            | `default`                 | The default network policy for the region. | Yes
`real_time_recency`                         | `false`                   | Boolean flag indicating whether [real-time recency](/get-started/isolation-level/#real-time-recency) is enabled for the current session.                               | [Contact support]
`real_time_recency_timeout`                 | `10s`                     | Sets the maximum allowed duration of `SELECT` statements that actively use [real-time recency](/get-started/isolation-level/#real-time-recency). If this value is specified without units, it is taken as milliseconds (`ms`).                      | Yes
`server_version_num`                        | Version-dependent         | The PostgreSQL compatible server version as an integer.                                                                                                                | No
`server_version`                            | Version-dependent         | The PostgreSQL compatible server version.                                                                                                                              | No
`sql_safe_updates`                          | `false`                   | Boolean flag indicating whether to prohibit SQL statements that may be overly destructive.                                                                             | Yes
`standard_conforming_strings`               | `true`                    | Boolean flag indicating whether ordinary string literals (`'...'`) should treat backslashes literally. The only supported value is `true`.                             | Yes
`statement_timeout`                         | `10s`                     | The maximum allowed duration of the read portion of write operations; i.e., the `SELECT` portion of `INSERT INTO ... (SELECT ...)`; the `WHERE` portion of `UPDATE ... WHERE ...` and `DELETE FROM ... WHERE ...`. If this value is specified without units, it is taken as milliseconds (`ms`). | Yes
`timezone`                                  | `UTC`                     | The time zone for displaying and interpreting timestamps. The only supported value is `UTC`.                                                                           | Yes

[Contact support]: /support

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

> **Note:** The default value for the `cluster` session parameter is `quickstart`.
> If the `quickstart` cluster is dropped, you must run [`SET cluster`](/sql/select/#ad-hoc-queries)
> to choose a valid cluster in order to run `SELECT` queries. A _superuser_ (i.e. `Organization Admin`)
> can also run [`ALTER SYSTEM SET cluster`](/sql/alter-system-set) to change the
> default value.


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

- `USAGE` privileges on the schema containing `item_ref`.

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

There are no privileges required to execute this statement.

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


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


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

- `USAGE` privileges on the schema containing the connection.

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


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


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

- `USAGE` privileges on the schema containing the index.

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


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


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

- `USAGE` privileges on the schema containing the materialized view.

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


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


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

- `USAGE` privileges on the schema containing the sink.

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


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


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

- `USAGE` privileges on the schema containing the source.

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


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


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

- `USAGE` privileges on the schema containing the table.

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


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


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

- `USAGE` privileges on the schema containing the table.

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


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


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

- `USAGE` privileges on the schema containing the view.

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

> **Note:** The default value for the `network_policy` session parameter is `default`.
> Before dropping the `default` network policy, a _superuser_ (i.e. `Organization
> Admin`) must run [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) to
> change the default value.


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

### Generic functionsGeneric functions can typically take arguments of any type.#### `CAST (cast_expr) -> T`

Value as type <code>T</code> [(docs)](/sql/functions/cast)#### `coalesce(x: T...) -> T?`

First non-<em>NULL</em> arg, or <em>NULL</em> if all are <em>NULL</em>.#### `greatest(x: T...) -> T?`

The maximum argument, or <em>NULL</em> if all are <em>NULL</em>.#### `least(x: T...) -> T?`

The minimum argument, or <em>NULL</em> if all are <em>NULL</em>.#### `nullif(x: T, y: T) -> T?`

<em>NULL</em> if <code>x == y</code>, else <code>x</code>.### Aggregate functionsAggregate functions take one or more of the same element type as arguments.#### `array_agg(x: T) -> T[]`

Aggregate values (including nulls) as an array [(docs)](/sql/functions/array_agg)#### `avg(x: T) -> U`

<p>Average of <code>T</code>&rsquo;s values.</p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `bool_and(x: T) -> T`

<em>NULL</em> if all values of <code>x</code> are <em>NULL</em>, otherwise true if all values of <code>x</code> are true, otherwise false.#### `bool_or(x: T) -> T`

<em>NULL</em> if all values of <code>x</code> are <em>NULL</em>, otherwise true if any values of <code>x</code> are true, otherwise false.#### `count(x: T) -> bigint`

Number of non-<em>NULL</em> inputs.#### `jsonb_agg(expression) -> jsonb`

Aggregate values (including nulls) as a jsonb array [(docs)](/sql/functions/jsonb_agg)#### `jsonb_object_agg(keys, values) -> jsonb`

Aggregate keys and values (including nulls) as a jsonb object [(docs)](/sql/functions/jsonb_object_agg)#### `max(x: T) -> T`

Maximum value among <code>T</code>.#### `min(x: T) -> T`

Minimum value among <code>T</code>.#### `stddev(x: T) -> U`

<p>Historical alias for <code>stddev_samp</code>. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `stddev_pop(x: T) -> U`

<p>Population standard deviation of <code>T</code>&rsquo;s values. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `stddev_samp(x: T) -> U`

<p>Sample standard deviation of <code>T</code>&rsquo;s values. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `string_agg(value: text, delimiter: text) -> text`

Concatenates the non-null input values into text. Each value after the first is preceded by the corresponding delimiter [(docs)](/sql/functions/string_agg)#### `sum(x: T) -> U`

<p>Sum of <code>T</code>&rsquo;s values</p>
<p>Returns <code>bigint</code> if <code>x</code> is <code>int</code> or <code>smallint</code>, <code>numeric</code> if <code>x</code> is <code>bigint</code> or <code>uint8</code>,
<code>uint8</code> if <code>x</code> is <code>uint4</code> or <code>uint2</code>, else returns same type as <code>x</code>.</p>
#### `variance(x: T) -> U`

<p>Historical alias for <code>var_samp</code>. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `var_pop(x: T) -> U`

<p>Population variance of <code>T</code>&rsquo;s values. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
#### `var_samp(x: T) -> U`

<p>Sample variance of <code>T</code>&rsquo;s values. <em>(imprecise)</em></p>
<p>Returns <code>numeric</code> if <code>x</code> is <code>int</code>, <code>double</code> if <code>x</code> is <code>real</code>, else returns
same type as <code>x</code>.</p>
### List functionsList functions take <a href="../types/list" ><code>list</code></a> arguments, and are <a href="../types/list/#polymorphism" >polymorphic</a>.#### `list_agg(x: any) -> L`

Aggregate values (including nulls) as a list [(docs)](/sql/functions/list_agg)#### `list_append(l: listany, e: listelementany) -> L`

Appends <code>e</code> to <code>l</code>.#### `list_cat(l1: listany, l2: listany) -> L`

Concatenates <code>l1</code> and <code>l2</code>.#### `list_length(l: listany) -> int`

Return the number of elements in <code>l</code>.#### `list_prepend(e: listelementany, l: listany) -> listany`

Prepends <code>e</code> to <code>l</code>.### Map functionsMap functions take <a href="../types/map" ><code>map</code></a> arguments, and are <a href="../types/#polymorphism" >polymorphic</a>.#### `map_length(m: mapany) -> int`

Return the number of elements in <code>m</code>.#### `map_build(kvs: list record(text, T)) -> map[text=>T]`

Builds a map from a list of records whose fields are two elements, the
first of which is <code>text</code>. In the face of duplicate keys, <code>map_build</code> retains
value from the record in the latest positition. This function is
purpose-built to process <a href="/sql/create-source/kafka/#headers" >Kafka headers</a>.#### `map_agg(keys: text, values: T) -> map[text=>T]`

Aggregate keys and values (including nulls) as a map [(docs)](/sql/functions/map_agg)### Numbers functionsNumber functions take number-like arguments, e.g. <a href="../types/int" ><code>int</code></a>, <a href="../types/float" ><code>float</code></a>, <a href="../types/numeric" ><code>numeric</code></a>, unless otherwise specified.#### `abs(x: N) -> N`

The absolute value of <code>x</code>.#### `cbrt(x: double precision) -> double precision`

The cube root of <code>x</code>.#### `ceil(x: N) -> double precision`

The smallest integer &gt;= <code>x</code>.#### `ceiling(x: N) -> double precision`

Alias of <code>ceil</code>.#### `exp(x: N) -> double precision`

Exponential of <code>x</code> (e raised to the given power)#### `floor(x: N) -> double precision`

The largest integer &lt;= <code>x</code>.#### `ln(x: double precision) -> double precision`

Natural logarithm of <code>x</code>.#### `ln(x: numeric) -> numeric`

Natural logarithm of <code>x</code>.#### `log(x: double precision) -> double precision`

Base 10 logarithm of <code>x</code>.#### `log(x: numeric) -> numeric`

Base 10 logarithm of <code>x</code>.#### `log10(x: double precision) -> double precision`

Base 10 logarithm of <code>x</code>, same as <code>log</code>.#### `log10(x: numeric) -> numeric`

Base 10 logarithm of <code>x</code>, same as <code>log</code>.#### `log(b: numeric, x: numeric) -> numeric`

Base <code>b</code> logarithm of <code>x</code>.#### `mod(x: N, y: N) -> N`

<code>x % y</code>#### `pow(x: double precision, y: double precision) -> double precision`

Alias of <code>power</code>.#### `pow(x: numeric, y: numeric) -> numeric`

Alias of <code>power</code>.#### `power(x: double precision, y: double precision) -> double precision`

<code>x</code> raised to the power of <code>y</code>.#### `power(x: numeric, y: numeric) -> numeric`

<code>x</code> raised to the power of <code>y</code>.#### `round(x: N) -> double precision`

<code>x</code> rounded to the nearest whole number.
If <code>N</code> is <code>real</code> or <code>double precision</code>, rounds ties to the nearest even number.
If <code>N</code> is <code>numeric</code>, rounds ties away from zero.#### `round(x: numeric, y: int) -> numeric`

<code>x</code> rounded to <code>y</code> decimal places, while retaining the same <a href="../types/numeric" ><code>numeric</code></a> scale; rounds ties away from zero.#### `sqrt(x: numeric) -> numeric`

The square root of <code>x</code>.#### `sqrt(x: double precision) -> double precision`

The square root of <code>x</code>.#### `trunc(x: N) -> double precision`

<code>x</code> truncated toward zero to a whole number.### Trigonometric functionsTrigonometric functions take and return <code>double precision</code> values.#### `cos(x: double precision) -> double precision`

The cosine of <code>x</code>, with <code>x</code> in radians.#### `acos(x: double precision) -> double precision`

The inverse cosine of <code>x</code>, result in radians.#### `cosh(x: double precision) -> double precision`

The hyperbolic cosine of <code>x</code>, with <code>x</code> as a hyperbolic angle.#### `acosh(x: double precision) -> double precision`

The inverse hyperbolic cosine of <code>x</code>.#### `cot(x: double precision) -> double precision`

The cotangent of <code>x</code>, with <code>x</code> in radians.#### `sin(x: double precision) -> double precision`

The sine of <code>x</code>, with <code>x</code> in radians.#### `asin(x: double precision) -> double precision`

The inverse sine of <code>x</code>, result in radians.#### `sinh(x: double precision) -> double precision`

The hyperbolic sine of <code>x</code>, with <code>x</code> as a hyperbolic angle.#### `asinh(x: double precision) -> double precision`

The inverse hyperbolic sine of <code>x</code>.#### `tan(x: double precision) -> double precision`

The tangent of <code>x</code>, with <code>x</code> in radians.#### `atan(x: double precision) -> double precision`

The inverse tangent of <code>x</code>, result in radians.#### `tanh(x: double precision) -> double precision`

The hyperbolic tangent of <code>x</code>, with <code>x</code> as a hyperbolic angle.#### `atanh(x: double precision) -> double precision`

The inverse hyperbolic tangent of <code>x</code>.#### `radians(x: double precision) -> double precision`

Converts degrees to radians.#### `degrees(x: double precision) -> double precision`

Converts radians to degrees.### String functions#### `ascii(s: str) -> int`

The ASCII value of <code>s</code>&rsquo;s left-most character.#### `btrim(s: str) -> str`

Trim all spaces from both sides of <code>s</code>.#### `btrim(s: str, c: str) -> str`

Trim any character in <code>c</code> from both sides of <code>s</code>.#### `bit_count(b: bytea) -> bigint`

Returns the number of bits set in the bit string (aka <em>popcount</em>).#### `bit_length(s: str) -> int`

Number of bits in <code>s</code>.#### `bit_length(b: bytea) -> int`

Number of bits in <code>b</code>.#### `char_length(s: str) -> int`

Number of code points in <code>s</code>.#### `chr(i: int) -> str`

Character with the given Unicode codepoint.
Only supports codepoints that can be encoded in UTF-8.
The NULL (0) character is not allowed.#### `concat(f: any, r: any...) -> text`

Concatenates the text representation of non-NULL arguments. The maximum length of the result string is 100 MiB.#### `concat_ws(sep: str, f: any, r: any...) -> text`

Concatenates the text representation of non-NULL arguments from <code>f</code> and <code>r</code> separated by <code>sep</code>. The maximum length of the result string is 100 MiB.#### `convert_from(b: bytea, src_encoding: text) -> text`

Convert data <code>b</code> from original encoding specified by <code>src_encoding</code> into <code>text</code>.#### `decode(s: text, format: text) -> bytea`

Decode <code>s</code> using the specified textual representation. The maximum size of the result is 100 MiB. [(docs)](/sql/functions/encode)#### `encode(b: bytea, format: text) -> text`

Encode <code>b</code> using the specified textual representation [(docs)](/sql/functions/encode)#### `get_bit(b: bytea, n: int) -> int`

Return the <code>n</code>th bit from <code>b</code>, where the left-most bit in <code>b</code> is at the 0th position.#### `get_byte(b: bytea, n: int) -> int`

Return the <code>n</code>th byte from <code>b</code>, where the left-most byte in <code>b</code> is at the 0th position.#### `constant_time_eq(a: bytea, b: bytea) -> bool`

Returns <code>true</code> if the arrays are identical, otherwise returns <code>false</code>. The implementation mitigates timing attacks by making a best-effort attempt to execute in constant time if the arrays have the same length, regardless of their contents.#### `constant_time_eq(a: text, b: text) -> bool`

Returns <code>true</code> if the strings are identical, otherwise returns <code>false</code>. The implementation mitigates timing attacks by making a best-effort attempt to execute in constant time if the strings have the same length, regardless of their contents.#### `initcap(a: text) -> text`

Returns <code>a</code> with the first character of every word in upper case and all
other characters in lower case. Words are separated by non-alphanumeric
characters.#### `left(s: str, n: int) -> str`

The first <code>n</code> characters of <code>s</code>. If <code>n</code> is negative, all but the last <code>|n|</code> characters of <code>s</code>.#### `length(s: str) -> int`

Number of code points in <code>s</code>. [(docs)](/sql/functions/length)#### `length(b: bytea) -> int`

Number of bytes in <code>s</code>. [(docs)](/sql/functions/length)#### `length(s: bytea, encoding_name: str) -> int`

Number of code points in <code>s</code> after encoding [(docs)](/sql/functions/length)#### `lower(s: str) -> str`

Convert <code>s</code> to lowercase.#### `lpad(s: str, len: int) -> str`

Prepend <code>s</code> with spaces up to length <code>len</code>, or right truncate if <code>len</code> is less than the length of <code>s</code>. The maximum length of the result string is 100 MiB.#### `lpad(s: str, len: int, p: str) -> str`

Prepend <code>s</code> with characters pulled from <code>p</code> up to length <code>len</code>, or right truncate if <code>len</code> is less than the length of <code>s</code>. The maximum length of the result string is 100 MiB.#### `ltrim(s: str) -> str`

Trim all spaces from the left side of <code>s</code>.#### `ltrim(s: str, c: str) -> str`

Trim any character in <code>c</code> from the left side of <code>s</code>.#### `normalize(s: str) -> str`

Normalize <code>s</code> to Unicode Normalization Form NFC (default). [(docs)](/sql/functions/normalize)#### `normalize(s: str, form: keyword) -> str`

Normalize <code>s</code> to the specified Unicode normalization form (NFC, NFD, NFKC, or NFKD). [(docs)](/sql/functions/normalize)#### `octet_length(s: str) -> int`

Number of bytes in <code>s</code>.#### `octet_length(b: bytea) -> int`

Number of bytes in <code>b</code>.#### `parse_ident(ident: str[, strict_mode: bool]) -> str[]`

Given a qualified identifier like <code>a.&quot;b&quot;.c</code>, splits into an array of the
constituent identifiers with quoting removed and escape sequences decoded.
Extra characters after the last identifier are ignored unless the
<code>strict_mode</code> parameter is <code>true</code> (defaults to <code>false</code>).#### `position(sub: str IN s: str) -> int`

The starting index of <code>sub</code> within <code>s</code> or <code>0</code> if <code>sub</code> is not a substring of <code>s</code>.#### `regexp_match(haystack: str, needle: str [, flags: str]]) -> str[]`

Matches the regular expression <code>needle</code> against haystack, returning a
string array that contains the value of each capture group specified in
<code>needle</code>, in order. If <code>flags</code> is set to the string <code>i</code> matches
case-insensitively.#### `regexp_replace(source: str, pattern: str, replacement: str [, flags: str]]) -> str`

> **Warning:** This function has the potential to produce very large strings and
> may cause queries to run out of memory or crash. Use with caution.

<p>Replaces the first occurrence of <code>pattern</code> with <code>replacement</code> in <code>source</code>.
No match will return <code>source</code> unchanged.</p>
<p>If <code>flags</code> is set to <code>g</code>, all occurrences are replaced.
If <code>flags</code> is set to <code>i</code>, matches case-insensitively.</p>
<p><code>$N</code> or <code>$name</code> in <code>replacement</code> can be used to match capture groups.
<code>${N}</code> must be used to disambiguate capture group indexes from names if other characters follow <code>N</code>.
A <code>$$</code> in <code>replacement</code> will write a literal <code>$</code>.</p>
<p>See the <a href="https://docs.rs/regex/latest/regex/struct.Regex.html#method.replace" >rust regex docs</a> for more details about replacement.</p>
#### `regexp_matches(haystack: str, needle: str [, flags: str]]) -> str[]`

Matches the regular expression <code>needle</code> against haystack, returning a
string array that contains the value of each capture group specified in
<code>needle</code>, in order. If <code>flags</code> is set to the string <code>i</code> matches
case-insensitively. If <code>flags</code> is set to the string <code>g</code> all matches are
returned, otherwise only the first match is returned. Without the <code>g</code>
flag, the behavior is the same as <code>regexp_match</code>.#### `regexp_split_to_array(text: str, pattern: str [, flags: str]]) -> str[]`

Splits <code>text</code> by the regular expression <code>pattern</code> into an array.
If <code>flags</code> is set to <code>i</code>, matches case-insensitively.#### `repeat(s: str, n: int) -> str`

Replicate the string <code>n</code> times. The maximum length of the result string is 100 MiB.#### `replace(s: str, f: str, r: str) -> str`

<code>s</code> with all instances of <code>f</code> replaced with <code>r</code>. The maximum length of the result string is 100 MiB.#### `right(s: str, n: int) -> str`

The last <code>n</code> characters of <code>s</code>. If <code>n</code> is negative, all but the first <code>|n|</code> characters of <code>s</code>.#### `rtrim(s: str) -> str`

Trim all spaces from the right side of <code>s</code>.#### `rtrim(s: str, c: str) -> str`

Trim any character in <code>c</code> from the right side of <code>s</code>.#### `split_part(s: str, d: s, i: int) -> str`

Split <code>s</code> on delimiter <code>d</code>. Return the <code>str</code> at index <code>i</code>, counting from 1.#### `starts_with(s: str, prefix: str) -> bool`

Report whether <code>s</code> starts with <code>prefix</code>.#### `substring(s: str, start_pos: int) -> str`

Substring of <code>s</code> starting at <code>start_pos</code> [(docs)](/sql/functions/substring)#### `substring(s: str, start_pos: int, l: int) -> str`

Substring starting at <code>start_pos</code> of length <code>l</code> [(docs)](/sql/functions/substring)#### `substring('s' [FROM 'start_pos']? [FOR 'l']?) -> str`

Substring starting at <code>start_pos</code> of length <code>l</code> [(docs)](/sql/functions/substring)#### `translate(s: str, from: str, to: str) -> str`

Any character in <code>s</code> that matches a character in <code>from</code> is replaced by the corresponding character in <code>to</code>.
If <code>from</code> is longer than <code>to</code>, occurrences of the extra characters in <code>from</code> are removed.#### `trim([BOTH | LEADING | TRAILING]? ['c'? FROM]? 's') -> str`

<p>Trims any character in <code>c</code> from <code>s</code> on the specified side.</p>
<p>Defaults:</p>
<p>• Side: <code>BOTH</code></p>
<p>• <code>'c'</code>: <code>' '</code> (space)</p>
#### `try_parse_monotonic_iso8601_timestamp(s: str) -> timestamp`

Parses a specific subset of ISO8601 timestamps, returning <code>NULL</code> instead of
error on failure: <code>YYYY-MM-DDThh:mm:ss.sssZ</code> [(docs)](/sql/functions/pushdown)#### `upper(s: str) -> str`

Convert <code>s</code> to uppercase.#### `reverse(s: str) -> str`

Reverse the characters in <code>s</code>.#### `string_to_array(s: str, delimiter: str [, null_string: str]) -> str[]`

<p>Splits the string at occurrences of delimiter and returns a text array of
the split segments.</p>
<p>If <code>delimiter</code> is NULL, each character in the string will become a
separate element in the array.</p>
<p>If <code>delimiter</code> is an empty string, then the string is treated as a single
field.</p>
<p>If <code>null_string</code> is supplied and is not NULL, fields matching that string
are replaced by NULL.</p>
<p>For example: <code>string_to_array('xx~~yy~~zz', '~~', 'yy')</code> → <code>{xx,NULL,zz}</code></p>
### Scalar functionsScalar functions take a list of scalar expressions#### `expression bool_op ALL(s: Scalars) -> bool`

<code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and every value of <code>s</code> evaluates to <code>true</code>.#### `expression bool_op ANY(s: Scalars) -> bool`

<code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and any
value of <code>s</code> evaluates to <code>true</code>. Avoid using in equi-join conditions as
its use in the equi-join condition can lead to a significant increase in
memory usage. See <a href="/transform-data/idiomatic-materialize-sql/any" >idiomatic Materialize
SQL</a> for the alternative.#### `expression IN(s: Scalars) -> bool`

<code>true</code> for each value in <code>expression</code> if it matches at least one element of <code>s</code>.#### `expression NOT IN(s: Scalars) -> bool`

<code>true</code> for each value in <code>expression</code> if it does not match any elements of <code>s</code>.#### `expression bool_op SOME(s: Scalars) -> bool`

<code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and any value of <code>s</code> evaluates to <code>true</code>.### Subquery functionsSubquery functions take a query, e.g. <a href="/sql/select" ><code>SELECT</code></a>#### `expression bool_op ALL(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and every value of <code>s</code> evaluates to <code>true</code>.#### `expression bool_op ANY(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and any value of <code>s</code> evaluates to <code>true</code>.#### `csv_extract(num_csv_col: int, col_name: string) -> col1: string, ... coln: string`

Extracts separated values from a column containing a CSV file formatted as a string [(docs)](/sql/functions/csv_extract)#### `EXISTS(s: Query) -> bool`

<code>true</code> if <code>s</code> returns at least one row.#### `expression IN(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> for each value in <code>expression</code> if it matches at least one element of <code>s</code>.#### `NOT EXISTS(s: Query) -> bool`

<code>true</code> if <code>s</code> returns zero rows.#### `expression NOT IN(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> for each value in <code>expression</code> if it does not match any elements of <code>s</code>.#### `expression bool_op SOME(s: Query) -> bool`

<code>s</code> must return exactly one column; <code>true</code> if applying <a href="#boolean-operators" >bool_op</a> to <code>expression</code> and any value of <code>s</code> evaluates to <code>true</code>.### Date and time functionsTime functions take or produce a time-like type, e.g. <a href="../types/date" ><code>date</code></a>, <a href="../types/timestamp" ><code>timestamp</code></a>, <a href="../types/timestamptz" ><code>timestamp with time zone</code></a>.#### `age(timestamp, timestamp) -> interval`

Subtracts one timestamp from another, producing a &ldquo;symbolic&rdquo; result that uses years and months, rather than just days.#### `current_timestamp() -> timestamptz`

The <code>timestamp with time zone</code> representing when the query was executed.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `date_bin(stride: interval, source: timestamp, origin: timestamp) -> timestamp`

Align <code>source</code> with <code>origin</code> along <code>stride</code> [(docs)](/sql/functions/date-bin)#### `date_trunc(time_component: str, val: timestamp) -> timestamp`

Largest <code>time_component</code> &lt;= <code>val</code> [(docs)](/sql/functions/date-trunc)#### `date_trunc(time_component: str, val: interval) -> interval`

Largest <code>time_component</code> &lt;= <code>val</code> [(docs)](/sql/functions/date-trunc)#### `EXTRACT(extract_expr) -> numeric`

Specified time component from value [(docs)](/sql/functions/extract)#### `date_part(time_component: str, val: timestamp) -> float`

Specified time component from value [(docs)](/sql/functions/date-part)#### `mz_now() -> mz_timestamp`

The logical time at which a query executes. Used for temporal filters and query timestamp introspection [(docs)](/sql/functions/now_and_mz_now)

**Note:** This function is [unmaterializable](#unmaterializable-functions), but can be used in limited contexts in materialized views as a [temporal filter](/transform-data/patterns/temporal-filters/).#### `now() -> timestamptz`

The <code>timestamp with time zone</code> representing when the query was executed [(docs)](/sql/functions/now_and_mz_now)

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `timestamp AT TIME ZONE zone -> timestamptz`

Converts <code>timestamp</code> to the specified time zone, expressed as an offset from UTC [(docs)](/sql/functions/timezone-and-at-time-zone)

**Known limitation:** You must explicitly cast the type for the time zone.#### `timestamptz AT TIME ZONE zone -> timestamp`

Converts <code>timestamp with time zone</code> from UTC to the specified time zone, expressed as the local time [(docs)](/sql/functions/timezone-and-at-time-zone)

**Known limitation:** You must explicitly cast the type for the time zone.#### `timezone(zone, timestamp) -> timestamptz`

Converts <code>timestamp</code> to specified time zone, expressed as an offset from UTC [(docs)](/sql/functions/timezone-and-at-time-zone)

**Known limitation:** You must explicitly cast the type for the time zone.#### `timezone(zone, timestamptz) -> timestamp`

Converts <code>timestamp with time zone</code> from UTC to specified time zone, expressed as the local time [(docs)](/sql/functions/timezone-and-at-time-zone)

**Known limitation:** You must explicitly cast the type for the time zone.#### `timezone_offset(zone: str, when: timestamptz) ->
(abbrev: str, base_utc_offset: interval, dst_offset: interval)`

<p>Describes a time zone&rsquo;s offset from UTC at a specified moment.</p>
<p><code>zone</code> must be a valid IANA Time Zone Database identifier.</p>
<p><code>when</code> is a <code>timestamp with time zone</code> that specifies the moment at which to determine <code>zone</code>&rsquo;s offset from UTC.</p>
<p><code>abbrev</code> is the abbreviation for <code>zone</code> that is in use at the specified moment (e.g., <code>EST</code> or <code>EDT</code>).</p>
<p><code>base_utc_offset</code> is the base offset from UTC at the specified moment (e.g., <code>-5 hours</code>). Positive offsets mean east of Greenwich; negative offsets mean west of Greenwich.</p>
<p><code>dst_offset</code> is the additional offset at the specified moment due to Daylight Saving Time rules (e.g., <code>1 hours</code>). If non-zero, Daylight Saving Time is in effect.</p>
#### `to_timestamp(val: double precision) -> timestamptz`

Converts Unix epoch (seconds since 00:00:00 UTC on January 1, 1970) to timestamp.#### `to_char(val: timestamp, format: str)`

Converts a timestamp into a string using the specified format [(docs)](/sql/functions/to_char)#### `justify_days(val: interval) -> interval`

Adjust interval so 30-day time periods are represented as months [(docs)](/sql/functions/justify-days)#### `justify_hours(val: interval) -> interval`

Adjust interval so 24-hour time periods are represented as days [(docs)](/sql/functions/justify-hours)#### `justify_interval(val: interval) -> interval`

Adjust interval using justify_days and justify_hours, with additional sign adjustments [(docs)](/sql/functions/justify-interval)### UUID functions#### `uuid_generate_v5(namespace: uuid, name: text) -> uuid`

Generates a <a href="https://www.rfc-editor.org/rfc/rfc4122#page-7" >version 5 UUID</a> (SHA-1) in the given namespace using the specified input name.### JSON functions#### `jsonb_agg(expression) -> jsonb`

Aggregate values (including nulls) as a jsonb array [(docs)](/sql/functions/jsonb_agg)#### `jsonb_array_elements(j: jsonb) -> Col<jsonb>`

<code>j</code>&rsquo;s elements if <code>j</code> is an array [(docs)](/sql/types/jsonb#jsonb_array_elements)#### `jsonb_array_elements_text(j: jsonb) -> Col<string>`

<code>j</code>&rsquo;s elements if <code>j</code> is an array [(docs)](/sql/types/jsonb#jsonb_array_elements_text)#### `jsonb_array_length(j: jsonb) -> int`

Number of elements in <code>j</code>&rsquo;s outermost array [(docs)](/sql/types/jsonb#jsonb_array_length)#### `jsonb_build_array(x: ...) -> jsonb`

Output each element of <code>x</code> as a <code>jsonb</code> array. Elements can be of heterogenous types [(docs)](/sql/types/jsonb#jsonb_build_array)#### `jsonb_build_object(x: ...) -> jsonb`

The elements of x as a <code>jsonb</code> object. The argument list alternates between keys and values [(docs)](/sql/types/jsonb#jsonb_build_object)#### `jsonb_each(j: jsonb) -> Col<(key: string, value: jsonb)>`

<code>j</code>&rsquo;s outermost elements if <code>j</code> is an object [(docs)](/sql/types/jsonb#jsonb_each)#### `jsonb_each_text(j: jsonb) -> Col<(key: string, value: string)>`

<code>j</code>&rsquo;s outermost elements if <code>j</code> is an object [(docs)](/sql/types/jsonb#jsonb_each_text)#### `jsonb_object_agg(keys, values) -> jsonb`

Aggregate keys and values (including nulls) as a <code>jsonb</code> object [(docs)](/sql/functions/jsonb_object_agg)#### `jsonb_object_keys(j: jsonb) -> Col<string>`

<code>j</code>&rsquo;s outermost keys if <code>j</code> is an object [(docs)](/sql/types/jsonb#jsonb_object_keys)#### `jsonb_pretty(j: jsonb) -> string`

Pretty printed (i.e. indented) <code>j</code> [(docs)](/sql/types/jsonb#jsonb_pretty)#### `jsonb_typeof(j: jsonb) -> string`

Type of <code>j</code>&rsquo;s outermost value. One of <code>object</code>, <code>array</code>, <code>string</code>, <code>number</code>, <code>boolean</code>, and <code>null</code> [(docs)](/sql/types/jsonb#jsonb_typeof)#### `jsonb_strip_nulls(j: jsonb) -> jsonb`

<code>j</code> with all object fields with a value of <code>null</code> removed. Other <code>null</code> values remain [(docs)](/sql/types/jsonb#jsonb_strip_nulls)#### `to_jsonb(v: T) -> jsonb`

<code>v</code> as <code>jsonb</code> [(docs)](/sql/types/jsonb#to_jsonb)### Table functionsTable functions evaluate to a collection of rows rather than a single row. You can use the <code>WITH ORDINALITY</code> and
<code>ROWS FROM</code> clauses together with table functions. For more details, see <a href="/sql/functions/table-functions" >Table functions</a>.#### `generate_series(start: int, stop: int) -> Col<int>`

Generate all integer values between <code>start</code> and <code>stop</code>, inclusive.#### `generate_series(start: int, stop: int, step: int) -> Col<int>`

Generate all integer values between <code>start</code> and <code>stop</code>, inclusive, incrementing by <code>step</code> each time.#### `generate_series(start: timestamp, stop: timestamp, step: interval) -> Col<timestamp>`

Generate all timestamp values between <code>start</code> and <code>stop</code>, inclusive, incrementing by <code>step</code> each time.#### `generate_subscripts(a: anyarray, dim: int) -> Col<int>`

Generates a series comprising the valid subscripts of the <code>dim</code>&lsquo;th dimension of the given array <code>a</code>.#### `regexp_extract(regex: str, haystack: str) -> Col<string>`

Values of the capture groups of <code>regex</code> as matched in <code>haystack</code>. Outputs each capture group in a separate column. At least one capture group is needed. (The capture groups are the parts of the regular expression between parentheses.)#### `regexp_split_to_table(text: str, pattern: str [, flags: str]]) -> Col<string>`

Splits <code>text</code> by the regular expression <code>pattern</code>.
If <code>flags</code> is set to <code>i</code>, matches case-insensitively.#### `unnest(a: anyarray)`

Expands the array <code>a</code> into a set of rows.#### `unnest(l: anylist)`

Expands the list <code>l</code> into a set of rows.#### `unnest(m: anymap)`

Expands the map <code>m</code> in a set of rows with the columns <code>key</code> and <code>value</code>.### Array functions#### `array_cat(a1: arrayany, a2: arrayany) -> arrayany`

Concatenates <code>a1</code> and <code>a2</code>.#### `array_fill(anyelement, int[], [, int[]]) -> anyarray`

Returns an array initialized with supplied value and dimensions, optionally with lower bounds other than 1.#### `array_length(a: arrayany, dim: bigint) -> int`

Returns the length of the specified dimension of the array.#### `array_position(haystack: anycompatiblearray, needle: anycompatible) -> int`

Returns the subscript of <code>needle</code> in <code>haystack</code>. Returns <code>null</code> if not found.#### `array_position(haystack: anycompatiblearray, needle: anycompatible, skip: int) -> int`

Returns the subscript of <code>needle</code> in <code>haystack</code>, skipping the first <code>skip</code> elements. Returns <code>null</code> if not found.#### `array_to_string(a: anyarray, sep: text [, ifnull: text]) -> text`

> **Warning:** This function has the potential to produce very large strings and
> may cause queries to run out of memory or crash. Use with caution.

<p>Concatenates the elements of <code>array</code> together separated by <code>sep</code>.
Null elements are omitted unless <code>ifnull</code> is non-null, in which case
null elements are replaced with the value of <code>ifnull</code>.</p>#### `array_remove(a: anyarray, e: anyelement) -> anyarray`

Returns the array <code>a</code> without any elements equal to the given value <code>e</code>.
The array must be one-dimensional. Comparisons are done using `IS NOT
DISTINCT FROM semantics, so it is possible to remove NULLs.### Hash functions#### `crc32(data: bytea) -> uint32`

Computes the 32-bit cyclic redundancy check of the given bytea <code>data</code> using the IEEE 802.3 polynomial.#### `crc32(data: text) -> uint32`

Computes the 32-bit cyclic redundancy check of the given text <code>data</code> using the IEEE 802.3 polynomial.#### `digest(data: text, type: text) -> bytea`

Computes a binary hash of the given text <code>data</code> using the specified <code>type</code> algorithm.
Supported hash algorithms are: <code>md5</code>, <code>sha1</code>, <code>sha224</code>, <code>sha256</code>, <code>sha384</code>, and <code>sha512</code>.#### `digest(data: bytea, type: text) -> bytea`

Computes a binary hash of the given bytea <code>data</code> using the specified <code>type</code> algorithm.
The supported hash algorithms are the same as for the text variant of this function.#### `hmac(data: text, key: text, type: text) -> bytea`

Computes a hashed MAC of the given text <code>data</code> using the specified <code>key</code> and
<code>type</code> algorithm. Supported hash algorithms are the same as for <code>digest</code>.#### `hmac(data: bytea, key: bytea, type: text) -> bytea`

Computes a hashed MAC of the given bytea <code>data</code> using the specified <code>key</code> and
<code>type</code> algorithm. The supported hash algorithms are the same as for <code>digest</code>.#### `kafka_murmur2(data: bytea) -> integer`

Computes the Murmur2 hash of the given bytea <code>data</code> using the seed used by Kafka&rsquo;s default partitioner and with the high bit cleared.#### `kafka_murmur2(data: text) -> integer`

Computes the Murmur2 hash of the given text <code>data</code> using the seed used by Kafka&rsquo;s default partitioner and with the high bit cleared.#### `md5(data: bytea) -> text`

Computes the MD5 hash of the given bytea <code>data</code>.
For PostgreSQL compatibility, returns a hex-encoded value of type <code>text</code> rather than <code>bytea</code>.#### `seahash(data: bytea) -> u64`

Computes the <a href="https://docs.rs/seahash" >SeaHash</a> hash of the given bytea <code>data</code>.#### `seahash(data: text) -> u64`

Computes the <a href="https://docs.rs/seahash" >SeaHash</a> hash of the given text <code>data</code>.#### `sha224(data: bytea) -> bytea`

Computes the SHA-224 hash of the given bytea <code>data</code>.#### `sha256(data: bytea) -> bytea`

Computes the SHA-256 hash of the given bytea <code>data</code>.#### `sha384(data: bytea) -> bytea`

Computes the SHA-384 hash of the given bytea <code>data</code>.#### `sha512(data: bytea) -> bytea`

Computes the SHA-512 hash of the given bytea <code>data</code>.### Window functions> **Tip:** For some window function query patterns, rewriting your query to not use
> window functions can yield better performance.  See [Idiomatic Materialize SQL](/transform-data/idiomatic-materialize-sql/) for details.

<p>Window functions compute values across sets of rows related to the current row.
For example, you can use a window aggregation to smooth measurement data by computing the average of the last 5
measurements before every row as follows:</p>
<pre tabindex="0"><code>SELECT
  avg(measurement) OVER (ORDER BY time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
FROM measurements;
</code></pre><p>Window functions always need an <code>OVER</code> clause. For the <code>OVER</code> clause, Materialize supports the same
<a href="https://www.postgresql.org/docs/current/tutorial-window.html" >syntax as
PostgreSQL</a>,
but supports only the following frame modes:</p>
<ul>
<li>
<p>the <code>ROWS</code> frame mode.</p>
</li>
<li>
<p>the default frame, which is <code>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</code>.</p>
</li>
</ul>
> **Note:** For [window functions](/sql/functions/#window-functions), when an input record
> in a partition (as determined by the `PARTITION BY` clause of your window
> function) is added/removed/changed, Materialize recomputes the results for the
> entire window partition. This means that when a new batch of input data arrives
> (that is, every second), **the amount of computation performed is proportional
> to the total size of the touched partitions**.
> For example, assume that in a given second, 20 input records change, and these
> records belong to **10** different partitions, where the average size of each
> partition is **100**. Then, amount of work to perform is proportional to
> computing the window function results for **10\*100=1000** rows.
> To avoid performance issues that may arise as the number of records grows,
> consider rewriting your query to use idiomatic Materialize SQL instead of window
> functions. If your query cannot be rewritten without the window functions and
> the performance of window functions is insufficient for your use case, please
> [contact our team](/support/).
> See [Idiomatic Materialize SQL](/transform-data/idiomatic-materialize-sql/)
> for examples of rewriting window functions.

<p>In addition to the below window functions, you can use the <code>OVER</code> clause with any <a href="#aggregate-functions" >aggregation function</a>
(e.g., <code>sum</code>, <code>avg</code>) as well. Using an aggregation with an <code>OVER</code> clause is called a <em>window aggregation</em>. A
window aggregation computes the aggregate not on the groups specified by the <code>GROUP BY</code> clause, but on the frames
specified inside the <code>OVER</code> clause. (Note that a window aggregation produces exactly one output value <em>for each input
row</em>. This is different from a standard aggregation, which produces one output value for each <em>group</em> specified by
the <code>GROUP BY</code> clause.)</p>
#### `dense_rank() -> int`

Returns the rank of the current row within its partition without gaps, counting from 1.
Rows that compare equal will have the same rank.#### `first_value(value anycompatible) -> anyelement`

<p>Returns <code>value</code> evaluated at the first row of the window frame. The default window frame is
<code>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</code>.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/first-value/" >Idiomatic Materialize SQL: First value</a>.</p>
#### `lag(value anycompatible [, offset integer [, default anycompatible ]]) -> int`

<p>Returns <code>value</code> evaluated at the row that is <code>offset</code> rows before the current row within the partition;
if there is no such row, instead returns <code>default</code> (which must be of a type compatible with <code>value</code>).
If <code>offset</code> is <code>NULL</code>, <code>NULL</code> is returned instead.
Both <code>offset</code> and <code>default</code> are evaluated with respect to the current row.
If omitted, <code>offset</code> defaults to 1 and <code>default</code> to <code>NULL</code>.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/lag/" >Idiomatic Materialize SQL: Lag over</a>.</p>
#### `last_value(value anycompatible) -> anyelement`

<p>Returns <code>value</code> evaluated at the last row of the window frame. The default window frame is
<code>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</code>.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/last-value/" >Idiomatic Materialize SQL: Last
value</a>.</p>
#### `lead(value anycompatible [, offset integer [, default anycompatible ]]) -> int`

<p>Returns <code>value</code> evaluated at the row that is <code>offset</code> rows after the current row within the partition;
if there is no such row, instead returns <code>default</code> (which must be of a type compatible with <code>value</code>).
If <code>offset</code> is <code>NULL</code>, <code>NULL</code> is returned instead.
Both <code>offset</code> and <code>default</code> are evaluated with respect to the current row.
If omitted, <code>offset</code> defaults to 1 and <code>default</code> to <code>NULL</code>.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/lead/" >Idiomatic Materialize SQL: Lead
over</a>.</p>
#### `rank() -> int`

Returns the rank of the current row within its partition with gaps (counting from 1):
rows that compare equal will have the same rank, and then the rank is incremented by the number of rows that
compared equal.#### `row_number() -> int`

<p>Returns the number of the current row within its partition, counting from 1.
Rows that compare equal will be ordered in an unspecified way.</p>
<p>See also <a href="/transform-data/idiomatic-materialize-sql/top-k/" >Idiomatic Materialize SQL: Top-K</a>.</p>
### System information functionsFunctions that return information about the system.#### `mz_environment_id() -> text`

Returns a string containing a <code>uuid</code> uniquely identifying the Materialize environment.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `mz_uptime() -> interval`

Returns the length of time that the materialized process has been running.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `mz_version() -> text`

Returns the server&rsquo;s version information as a human-readable string.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `mz_version_num() -> int`

Returns the server&rsquo;s version as an integer having the format <code>XXYYYZZ</code>, where <code>XX</code> is the major version, <code>YYY</code> is the minor version and <code>ZZ</code> is the patch version.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_database() -> text`

Returns the name of the current database.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_catalog() -> text`

Alias for <code>current_database</code>.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_user() -> text`

Returns the name of the user who executed the containing query.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_role() -> text`

Alias for <code>current_user</code>.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `user() -> text`

Alias for <code>current_user</code>.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `session_user() -> text`

Returns the name of the user who initiated the database connection.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `mz_row_size(expr: Record) -> int`

Returns the number of bytes used to store a row.### PostgreSQL compatibility functionsFunctions whose primary purpose is to facilitate compatibility with PostgreSQL tools.
These functions may have suboptimal performance characteristics.#### `format_type(oid: int, typemod: int) -> text`

Returns the canonical SQL name for the type specified by <code>oid</code> with <code>typemod</code> applied.#### `current_schema() -> text`

Returns the name of the first non-implicit schema on the search path, or
<code>NULL</code> if the search path is empty.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_schemas(include_implicit: bool) -> text[]`

Returns the names of the schemas on the search path.
The <code>include_implicit</code> parameter controls whether implicit schemas like
<code>mz_catalog</code> and <code>pg_catalog</code> are included in the output.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `current_setting(setting_name: text[, missing_ok: bool]) -> text`

Returns the value of the named setting or error if it does not exist.
If <code>missing_ok</code> is true, return NULL if it does not exist.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `obj_description(oid: oid, catalog: text) -> text`

Returns the comment for a database object specified by its <code>oid</code> and the name of the containing system catalog.#### `col_description(oid: oid, column: int) -> text`

Returns the comment for a table column, which is specified by the <code>oid</code> of its table and its column number.#### `pg_backend_pid() -> int`

Returns the internal connection ID.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `pg_cancel_backend(connection_id: int) -> bool`

Cancels an in-progress query on the specified connection ID.
Returns whether the connection ID existed (not if it cancelled a query).

**Note:** This function is [side-effecting](#side-effecting-functions).#### `pg_column_size(expr: any) -> int`

Returns the number of bytes used to store any individual data value.#### `pg_size_pretty(expr: numeric) -> text`

Converts a size in bytes into a human-readable format.#### `pg_get_constraintdef(oid: oid[, pretty: bool]) -> text`

Returns the constraint definition for the given <code>oid</code>. Currently always
returns NULL since constraints aren&rsquo;t supported.#### `pg_get_indexdef(index: oid[, column: integer, pretty: bool]) -> text`

Reconstructs the creating command for an index. (This is a decompiled
reconstruction, not the original text of the command.) If column is
supplied and is not zero, only the definition of that column is reconstructed.#### `pg_get_ruledef(rule_oid: oid[, pretty bool]) -> text`

Reconstructs the creating command for a rule. This function
always returns NULL because Materialize does not support rules.#### `pg_get_userbyid(role: oid) -> text`

Returns the role (user) name for the given <code>oid</code>. If no role matches the
specified OID, the string <code>unknown (OID=oid)</code> is returned.#### `pg_get_viewdef(view_name: text[, pretty: bool]) -> text`

Returns the underlying SELECT command for the given view.#### `pg_get_viewdef(view_oid: oid[, pretty: bool]) -> text`

Returns the underlying SELECT command for the given view.#### `pg_get_viewdef(view_oid: oid[, wrap_column: integer]) -> text`

Returns the underlying SELECT command for the given view.#### `pg_has_role([user: name or oid,] role: text or oid, privilege: text) -> bool`

Alias for <code>has_role</code> for PostgreSQL compatibility.#### `pg_is_in_recovery() -> bool`

Returns if the a recovery is still in progress.#### `pg_table_is_visible(relation: oid) -> boolean`

Reports whether the relation with the specified OID is visible in the search path.#### `pg_tablespace_location(tablespace: oid) -> text`

Returns the path in the file system that the provided tablespace is on.#### `pg_type_is_visible(relation: oid) -> boolean`

Reports whether the type with the specified OID is visible in the search path.#### `pg_function_is_visible(relation: oid) -> boolean`

Reports whether the function with the specified OID is visible in the search path.#### `pg_typeof(expr: any) -> text`

Returns the type of its input argument as a string.#### `pg_encoding_to_char(encoding_id: integer) -> text`

PostgreSQL compatibility shim. Not intended for direct use.#### `pg_postmaster_start_time() -> timestamptz`

Returns the time when the server started.

**Note:** This function is [unmaterializable](#unmaterializable-functions).#### `pg_relation_size(relation: regclass[, fork: text]) -> bigint`

Disk space used by the specified fork (&lsquo;main&rsquo;, &lsquo;fsm&rsquo;, &lsquo;vm&rsquo;, or &lsquo;init&rsquo;)
of the specified table or index. If no fork is specified, it defaults
to &lsquo;main&rsquo;. This function always returns -1 because Materialize does
not store tables and indexes on local disk.#### `pg_stat_get_numscans(oid: oid) -> bigint`

Number of sequential scans done when argument is a table,
or number of index scans done when argument is an index.
This function always returns -1 because Materialize does
not collect statistics.#### `version() -> text`

Returns a PostgreSQL-compatible version string.

**Note:** This function is [unmaterializable](#unmaterializable-functions).### Access privilege inquiry functionsFunctions that allow querying object access privileges. None of the following functions consider
whether the provided role is a <em>superuser</em> or not.#### `has_cluster_privilege([role: text or oid,] cluster: text, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the cluster with the specified cluster name. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_connection_privilege([role: text or oid,] connection: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the connection with the specified connection name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_database_privilege([role: text or oid,] database: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the database with the specified database name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_schema_privilege([role: text or oid,] schema: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the schema with the specified schema name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_role([user: name or oid,] role: text or oid, privilege: text) -> bool`

Reports whether the <code>user</code> has the privilege for <code>role</code>. <code>privilege</code> can either be <code>MEMBER</code>
or <code>USAGE</code>, however currently this value is ignored. The <code>PUBLIC</code> pseudo-role cannot be used
for the <code>user</code> nor the <code>role</code>. If the <code>user</code> is omitted then the <code>current_role</code> is assumed.#### `has_secret_privilege([role: text or oid,] secret: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the secret with the specified secret name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_system_privilege([role: text or oid,] privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the system privilege.
If the role is omitted then the <code>current_role</code> is assumed.#### `has_table_privilege([role: text or oid,] relation: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the relation with the specified relation name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `has_type_privilege([role: text or oid,] type: text or oid, privilege: text) -> bool`

Reports whether the role with the specified role name or OID has the privilege on
the type with the specified type name or OID. If the role is omitted then
the <code>current_role</code> is assumed.#### `mz_is_superuser() -> bool`

Reports whether the <code>current_role</code> is a superuser.

**Note:** This function is [unmaterializable](#unmaterializable-functions).

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
`=` | Equality. Do not use with `NULL` as `= NULL` always evaluates to `NULL`; instead, use `IS NULL` for null checks.
`<>` | Inequality. Do not use with `NULL` as `<> NULL` always evaluates to `NULL`; instead, use `IS NOT NULL` for null checks.
`!=` | Inequality. Do not use with `NULL` as `!= NULL` always evaluates to `NULL`; instead, use `IS NOT NULL` for null checks.
`<` | Less than
`>` | Greater than
`<=` | Less than or equal to
`>=` | Greater than or equal to
`a BETWEEN x AND y` | `a >= x AND a <= y`
`a NOT BETWEEN x AND y` | `a < x OR a > y`
`a IS NULL` | Evaluates to true if the value of a is `NULL`.
`a ISNULL` | Evaluates to true if the value of a is `NULL`.
`a IS NOT NULL` | Evaluates to true if the value of a is **NOT** `NULL`.
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

> **Warning:** Materialize regular expressions are similar to, but not identical to, PostgreSQL
> regular expressions.


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

Operator | RHS Type | Description
---------|----------|-------------
`->` | `text`, `int`| Access field by name or index position, and return `jsonb` ([docs](/sql/types/jsonb/#field-access-as-jsonb--))
`->>` | `text`, `int`| Access field by name or index position, and return `text` ([docs](/sql/types/jsonb/#field-access-as-text--))
`#>` | `text[]` | Access field by path, and return `jsonb` ([docs](/sql/types/jsonb/#path-access-as-jsonb-))
`#>>` | `text[]` | Access field by path, and return `text` ([docs](/sql/types/jsonb/#path-access-as-text-))
<code>&vert;&vert;</code> | `jsonb` | Concatenate LHS and RHS ([docs](/sql/types/jsonb/#jsonb-concat-))
`-` | `text` | Delete all values with key of RHS ([docs](/sql/types/jsonb/#remove-key--))
`@>` | `jsonb` | Does element contain RHS? ([docs](/sql/types/jsonb/#lhs-contains-rhs-))
<code>&lt;@</code> | `jsonb` | Does RHS contain element? ([docs](/sql/types/jsonb/#rhs-contains-lhs-))
`?` | `text` | Is RHS a top-level key? ([docs](/sql/types/jsonb/#search-top-level-keys-))


### Map operators

Operator | RHS Type | Description
---------|----------|-------------
`->` | `string` | Access field by name, and return target field ([docs](/sql/types/map/#retrieve-value-with-key--))
`@>` | `map` | Does element contain RHS? ([docs](/sql/types/map/#lhs-contains-rhs-))
<code>&lt;@</code> | `map` | Does RHS contain element? ([docs](/sql/types/map/#rhs-contains-lhs-))
`?` | `string` | Is RHS a top-level key? ([docs](/sql/types/map/#search-top-level-keys-))
`?&` | `string[]` | Does LHS contain all RHS top-level keys? ([docs](/sql/types/map/#search-for-all-top-level-keys-))
<code>?&#124;</code> | `string[]` | Does LHS contain any RHS top-level keys? ([docs](/sql/types/map/#search-for-any-top-level-keys-))


### List operators

List operators are [polymorphic](../types/list/#polymorphism).

Operator | Description
---------|-------------
<code>listany &vert;&vert; listany</code> | Concatenate the two lists.
<code>listany &vert;&vert; listelementany</code> | Append the element to the list.
<code>listelementany &vert;&vert; listany</code> | Prepend the element to the list.
<code>listany @&gt; listany</code> | Check if the first list contains all elements of the second list.
<code>listany &lt;@ listany</code> | Check if all elements of the first list are contained in the second list.



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

> **Warning:** Many PostgreSQL drivers wait for a query to complete before returning its
> results. Since `SUBSCRIBE` can run forever, naively executing a `SUBSCRIBE` using your
> driver's standard query API may never return.
> Either use an API in your driver that does not buffer rows or use the
> [`FETCH`](/sql/fetch) statement or `AS OF` and `UP TO` bounds
> to fetch rows from `SUBSCRIBE` in batches.
> See the [examples](#examples) for details.


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

- `USAGE` privileges on the schemas that all relations and types in the query are contained in.
- `SELECT` privileges on all relations in the query.
  - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
  execute the view definition. Even if the view owner is a _superuser_, they still must explicitly be
    granted the necessary privileges.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.


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

> **Note:** The default value for the `cluster` session parameter is `quickstart`.
> This cluster functions as a default option, pre-created for your convenience.
> It allows you to quickly start running queries without needing to configure a cluster first.
> If the `quickstart` cluster is dropped, you must run [`SET cluster`](/sql/select/#ad-hoc-queries)
> to choose a valid cluster in order to run `SELECT` queries. A _superuser_ (i.e. `Organization Admin`)
> can also run [`ALTER SYSTEM SET cluster`](/sql/alter-system-set) to change the
> default value.


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

- `USAGE` privileges on the containing schema.
- `USAGE` privileges on the connection.

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
