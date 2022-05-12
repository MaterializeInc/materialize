# Cluster replicas

## Summary

This document proposes an explicit SQL API for managing
[cluster replicas](../platform/ux.md#cluster-replica).

## Design

### SQL syntax and semantics

The `ALTER CLUSTER` statement will be removed.

The `CREATE CLUSTER` and `CREATE CLUSTER REPLICA` statement will be evolved as
follows:

```sql
create_cluster_stmt ::=
  CREATE CLUSTER <name>
    [<inline_replica> [, <inline_replica> ...]]

create_cluster_replica_stmt ::=
  CREATE CLUSTER REPLICA <cluster_name>.<replica_name>
    [<replica_option> [, <replica_option> ...]]

inline_replica ::= REPLICA <name> [(<replica_option> [, <replica_option> ...])]

replica_option ::=
    | SIZE '<size>'
    | AVAILABILITY ZONE '<az>'
    | REMOTE (['address' [, 'address' ...]])
```

It will be valid to create a cluster with no replicas. Creating a replica will
be the moment at which compute resources are provisioned.

The `SIZE` and `REMOTE` options must not be specified together.

The `AVAILABILITY ZONE` option is new. The set of allowable availability zones
will be determined by a new `--availability-zone=AZ` command line option.
If no availability zone is specified explicitly, and at least one availability
zone is available, Materialize should automatically assign the availability
zone with the least existing replicas.

The `SIZE` option will be changed from the existing `SIZE` option to validate
the set of allowable sizes against a new `--cluster-replica-sizes`
command line option.

The `DROP CLUSTER REPLICA` statement will be added:

```
DROP CLUSTER REPLICA [IF EXISTS] <cluster-name>.<replica-name>
```

It will have the usual semantics for a `DROP` operation.

The `SHOW CLUSTER REPLICAS` statement

```
SHOW CLUSTER REPLICAS [FOR <cluster>] [{ LIKE 'pattern' | WHERE <expr> }]
```

will list the replicas of the provided cluster, or of the current cluster if no
cluster is specified. The results will be optionally filtered by the provided
`LIKE` pattern or `WHERE` expression, which work analogously to the same clauses
in the `SHOW SCHEMA` statement.

### System catalog changes

A new `mz_cluster_replicas` table will be added with the following schema:

Field             | Type      | Meaning
------------------|-----------|--------
cluster_id        | `bigint`  | The ID of the cluster.
name              | `text`    | The name of the cluster replica.
size              | `text`    | The size of the cluster replica. `NULL` for a remote cluster.
availability_zone | `text`    | The availability zone of the cluster replica, if any. May be `NULL`.
status            | `text`    | The status of the replica. Either `unhealthy` or `healthy`.

### Controller changes

#### Replica size

The controller will use the replica size to determine the following properties
of a cluster replica:

  * [Memory limit](https://dev.materialize.com/api/rust/mz_orchestrator/struct.ServiceConfig.html#structfield.memory_limit)
  * [CPU limit](https://dev.materialize.com/api/rust/mz_orchestrator/struct.ServiceConfig.html#structfield.cpu_limit)
  * [Scale](https://dev.materialize.com/api/rust/mz_orchestrator/struct.ServiceConfig.html#structfield.scale)
  * Timely worker threads, specified as the `--workers` argument to `computed`

The mapping between replica size and the above properties will be
determined by the new `--cluster-replica-sizes` comand line option, which
accepts a JSON object that can be deserialized into `ClusterReplicaSizeMap`:

```rust
#[derive(Deserialize)]
struct ClusterReplicaSizeConfig {
    memory_limit: Option<MemoryLimit>,
    cpu_limit: Option<CpuLimit>,
    scale: usize,
    workers: usize,
}

type ClusterReplicaSizeMap = HashMap<String, ClusterReplicaSizeMap>;
```

The default mapping if unspecified will be:

```jsonc
{
    "1": {"scale": 1, "workers": 1},
    "2": {"scale": 1, "workers": 2},
    "4": {"scale": 1, "workers": 4},
    // ...
    "32": {"scale": 1, "workers": 32},
    // Testing with multiple processes on a single machine is a novelty, so
    // we don't bother providing many options.
    "2-1": {"scale": 2, "workers": 1},
    "2-2": {"scale": 2, "workers": 2},
    "2-4": {"scale": 2, "workers": 4},
}
```

This is convenient for development in local process mode, where the memory and
size limits don't have any effect.

Materialize Cloud should use a mapping along the lines of the following:

```json
{
    "small": {"cpu_limit": 2, "memory_limit": 8, "scale": 1, "workers": 1},
    "medium": {"cpu_limit": 8, "memory_limit": 32, "scale": 1, "workers": 4},
    "large": {"cpu_limit": 32, "memory_limit": 128, "scale": 1, "workers": 16},
    "xlarge": {"cpu_limit": 128, "memory_limit": 512, "scale": 1, "workers": 64},
    "2xlarge": {"cpu_limit": 128, "memory_limit": 512, "scale": 4, "workers": 64},
    "3xlarge": {"cpu_limit": 128, "memory_limit": 512, "scale": 16, "workers": 64},
}
```

The actual names and limits are strawmen. The key point is that the sizes have
friendly names that abstract away the details.

Determining when to transition from scaling vertically to scaling horizontally
is an open question that will need to be answered by running experiments on
representative workloads. Prior work on benchmarking timely/differential in
isolation showed performance improvements when switching to horizontal
scalability before maxing out the resources on a single machine due to
allocator overhead.

#### Replica status

The controller will need to inform the adapter when a replica changes status, so
that the new status can be reflected in the `mz_cluster_replicas` table. The
easiest way to do this is likely via a new
`ComputeResponse::ReplicaStatusChange` variant.

### Orchestrator changes

The `ServiceConfig` trait will be evolved with the option to specify an
`availability_zone: Option<String>`.

When an availability zone is specified on a service, the
`KubernetesOrchestrator` will attach a `nodeSelector` to the pods created by the
service of `availability-zone=AZ`.

Note that the availability zone for AWS should be the
[stable AZ ID](https://docs.aws.amazon.com/ram/latest/userguide/working-with-az-ids.html),
like `use1-az1`, not the name like `us-east-1a`, which can map to different
physical AZs in different accounts.

### Cloud changes

The Pulumi infrastructure will need to provide nodes groups in several
availability zones in production. The environment controller will plumb
the list of... available... availability zones to `materialized` via the
new `--availability-zone` argument.

## Future work

Manually managing replicas is good for power users who want the flexibility, but
adds friction for average users. In the future, we intend to support
auto-scaling and auto-replicating clusters, where users declare the desired
properties of their replica set, e.g.:

```
CREATE CLUSTER foo REPLICATION FACTOR 3, MIN SIZE 'small', MAX SIZE 'xlarge'
```

and Materialize automatically turns on the appropriate number of replicas,
scaling them up and down in size in response to observed load.
