# Initial cluster SQL API

## Summary

This document proposes the initial SQL API changes to introduce
[clusters](../platform/ux.md#cluster) to Materialize.

## Design

### Cluster SQL syntax and semantics

Four new SQL statements will be introduced with the following grammar to
create, alter, drop, and list clusters, respectively:

```
create_cluster_stmt ::=
  CREATE CLUSTER [IF NOT EXISTS] <name> [[WITH] <option> [, <option>...]]

alter_cluster_stmt ::=
  ALTER CLUSTER <name> [[SET] <option> [, <option>...]]

drop_cluster_stmt ::=
  DROP CLUSTER [IF EXISTS] <name> [CASCADE]

show_cluster_stmt: SHOW CLUSTERS [{ LIKE 'pattern' | WHERE <expr> }]

<option> ::=
      VIRTUAL
    | SIZE '<size>'

<name> ::= <unqualified-identifier>
```

**All of these statements will be available in experimental mode only.**

The name of a cluster is an unqualified identifier. Clusters are scoped to
a catalog, and so do not belong in the usual namespace of databases and schemas.
For a similarly scoped object in PostgreSQL, see [publications].

The `CREATE CLUSTER` statement creates a new cluster. It returns an error if
a cluster with the specified name already exists unless the `IF NOT EXISTS`
clause is present.

The `ALTER CLUSTER` statement changes properties of an existing cluster.

The `DROP CLUSTER` statement drops an existing cluster. It returns an error
if a cluster with the specified name does not exist unless the `IF EXIST`
clause is present. Dropping a non-empty cluster (i.e., a cluster with at least
one index) is not permitted unless `CASCADE` is present.

THe `SHOW CLUSTERS` statement lists the clusters in the system, optionally
filtered by the provided `LIKE` pattern or `WHERE` expression, which work
analogously to the same clauses in the `SHOW DATABASES` statement.

`CREATE CLUSTER` and `ALTER CLUSTER` allow specifying the same set of cluster
options. Initially, there are only two options:

  * `VIRTUAL` requests a virtual cluster. A virtual cluster may or may not
    correspond to actual physical resources.

    We suspect that the concept of a virtual cluster is primarily useful as a
    way to exercise the cluster logic before we can allocate true clusters, and
    it is likely that the `VIRTUAL` option is never stabilized. But they may
    also prove to be a useful user-facing option, in which case we can consider
    stabilization.

  * `SIZE '<size>'` specifies a cluster of a fixed size. The size is an opaque
    string from the perspective of `materialized`, but will be validated by the
    cloud orchestration backend.

    In the initial implementation, specifying the `SIZE` option will result in
    an error during planning (i.e., not during parsing). The cloud team will be
    along shortly to wire up this option to turn on actual physical clusters.

Specifying both `VIRTUAL` and `SIZE` options in the same `CREATE CLUSTER` or
`ALTER CLUSTER` is an error. Specifying the same option more than once is also
an error.

### Index SQL syntax and semantics

The `CREATE INDEX` statement will be extended like so to allow specifying in
which cluster to create an index:

```
CREATE INDEX <name> [IN CLUSTER <cluster>] ON <object> (<col>, ...)
```

If the `IN CLUSTER` clause is not specified explicitly, the index is created in
the cluster specified by the `cluster` session variable (see below). It is an
error if that session variable does not name a valid cluster.

Normalizing a `CREATE INDEX` statement will install the explicit `IN CLUSTER`
clause if it was not explicitly specified.

The syntax of `SHOW INDEXES` will be extended to allow limiting the indexes
shown to one cluster:

```
SHOW { INDEX | INDEXES | KEYS } { FROM | IN } <on_name>
[IN CLUSTER <cluster_name>]
{ LIKE 'pattern | EXPR <expr> }
```

The output of `SHOW INDEXES` will be extended with an additional `cluster_name`
column which displays the name of the cluster in which the index is
materialized.

### Sink SQL syntax and semantics

The `CREATE SINK` statement will be extended like so to allow specifying in
which cluster to create an index:

```
CREATE SINK <name> [IN CLUSTER <cluster>] INTO ...
```

If the `IN CLUSTER` clause is not specified explicitly, the sink is created in
the cluster specified by the `cluster` session variable (see below). It is an
error if that session variable does not name a valid cluster.

Normalizing a `CREATE SINK` statement will install the explicit `IN CLUSTER`
clause if it was not explicitly specified.

The syntax of `SHOW SINKS` will be extended to allow limiting the sinks
shown to one cluster:

```
SHOW SINKS FROM <schema>
[IN CLUSTER <cluster_name>]
{ LIKE 'pattern | EXPR <expr> }
```

The output of `SHOW SINKS` will be extended with an additional `cluster_name`
column which displays the name of the cluster in which the index is
materialized.

### Materialized source/view SQL syntax and semantics

The `CREATE MATERIALIZED { SOURCE | VIEW }` statements will be extended like so
to allow specifying in which cluster to create the source or view's default
index:

```
CREATE MATERIALIZED { SOURCE | VIEW } <name> [IN CLUSTER <cluster>] AS SELECT ...
```

This is simple syntax sugar for the following sequence of statements:

```
CREATE { SOURCE | VIEW } <name> AS SELECT ...
CREATE DEFAULT INDEX [IN CLUSTER <cluster>] ON <name>
```

### SQL session changes

A new `cluster` session variable will be introduced that specifies the "active"
cluster for a SQL session. It is analogous to the existing `database` session
variable.

The default value for the `cluster` session variable will be initially hardcoded
to `default`. Users can change the value on a per-session basis via the usual
mechanisms, like the pgwire startup protocol and the `SET` command.

See [future work](#future-work) for details on how we plan to make the default
configurable in the future.

`SELECT` and `TAIL` statements issued in a session will only use the indexes
from the active cluster. In the future, we may add optional `IN CLUSTER` clauses
to these statements, but for simplicitly, they are omitted for now; reliably
extending the `SELECT` grammar is difficult.

### System catalog changes

A new `mz_clusters` table with the following structure will describe the
available clusters in the system:

Field   | Type      | Meaning
--------|-----------|--------
id      | `bigint`  | The ID of the cluster.
name    | `text`    | The name of the cluster.
virtual | `boolean` | Whether the cluster is virtual.
size    | `text`    | The size of the cluster. `NULL` if the cluster is virtual.

The existing `mz_indexes` table will be extended with a `cluster_id` field:

Field      | Type      | Meaning
-----------|-----------|--------
...        | ...       | ...
cluster_id | `bigint`  | The ID of the cluster in which the index is materialized.

The existing `mz_sinks` table will be extended with a `cluster_id` field:

Field      | Type      | Meaning
-----------|-----------|--------
...        | ...       | ...
cluster_id | `bigint`  | The ID of the cluster in which the sink is maintained.

### Coordinator changes

Internally, each cluster will be identified by an identifier in the range `[1,
i64::MAX]`. The coordinator will be responsible for allocating identifiers and
durably recording the mapping from names to identifiers via the following new
table in the SQLite catalog:

```sql
CREATE TABLE clusters (
    id   integer PRIMARY KEY,
    name text NOT NULL UNIQUE
);
```

This table works analogously to the existing `databases` table in the catalog.
Initially, all clusters in the table will be virtual. Storing the `SIZE` option
of a non-virtual cluster is deferred to future work.

The migration that creates the table will additionally install a default row
of `(1, 'default')`. This creates a cluster named `default` that is present by
default in every Materialize installation. The default cluster is not otherwise
special and can be dropped if desired.

We will write a catalog migration that adds the `IN CLUSTER default` clause to
all existing indexes.

The various bookkeeping in the coordinator will need to change to account for
clusters. Execution of a `SELECT` statement, for example, will need to select
from only the indexes in the active cluster.

TODO(mcsherry?): can we more specifically describe what bookkeeping will change?

### `mz_cluster_id`

The existing `mz_cluster_id` function uses "cluster" to mean something entirely
unrelated to the new concept of clusters. To reduce confusion, we need to
introduce a breaking change to rename this function ASAP. I propose
`mz_legacy_installation_id` as a strawman.

In the future, I believe we'll instead want a function like `mz_account_id` that
corresponds to the [account](../platform/ux.md#account) ID in Materialize Platform.

## Future work

* Introduce `ALTER SYSTEM` to change the system default for the `cluster`
  server configuration parameter.

* Allow individual users to configure a default for the `cluster` session
  variable.

  Note that this necessitates building out a variable/parameter hierarchy where
  `cluster` can be set on the server (i.e. `ALTER SYSTEM SET`), but also
  overridden by a session setting (`SET`).

* Introduce logging sources in the system catalog that show the active cluster's
  resource usage (e.g., `mz_cluster_cpu_usage` and `mz_cluster_memory_usage`).

[publications]: https://www.postgresql.org/docs/current/sql-createpublication.html
