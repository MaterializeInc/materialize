# User-Defined Cluster Replica Sizes

- Associated: [PR #35971](https://github.com/MaterializeInc/materialize/pull/35971)

## The Problem

Cluster replica sizes in Materialize are configured exclusively through
the `CLUSTER_REPLICA_SIZES` environment variable, passed as a JSON blob
at startup. This creates several problems:

* **Adding or changing sizes requires an environmentd restart.** Any
  modification to the set of available sizes -- adding a new size,
  adjusting resource limits, disabling an existing one -- requires
  updating a cli flag and restarting environmentd. In cloud
  environments this means a deployment rollout; in self-managed
  environments it means coordinating downtime.
* **No ad-hoc experimentation.** Operators cannot quickly test a new
  size to explore whether a different CPU/memory/worker configuration
  better fits a workload. Every experiment requires the full
  restart-and-redeploy cycle, making iterative sizing impractical.
* **No durability or safety guarantees.** Sizes are held only in
  memory. If the env var changes between restarts, sizes can silently
  appear or disappear with no audit trail and no protection against
  removing a size that existing replicas depend on.

## Success Criteria

* Operators can create and remove cluster replica sizes without
  restarting environmentd or coordinating a deployment rollout.
* Operators can inspect the full definition of a cluster replica
  size — including node selectors, swap configuration, and resource
  limits — through SQL.
* Removing a size that is actively in use by a replica is prevented,
  avoiding silent breakage of running workloads.
* Existing CLI flag based sizes continue to work unchanged. Operators
  who don't use the new DDL see no behavior difference.

## Out of Scope

* **ALTER CLUSTER REPLICA SIZE.** Sizes are immutable once created.
  To change a size definition, drop and recreate it (after migrating
  replicas off it). This avoids the complexity of drift between
  running replicas and their declared size, and avoids needing to
  track size history per replica.

## Solution Proposal

### Durable storage

`ClusterReplicaSize` is added as a new durable catalog object,
following the established pattern used by `NetworkPolicy`, `Cluster`,
and other catalog objects.

#### Identity

A new `ClusterReplicaSizeId` type in `mz-repr` with `User(u64)` and
`System(u64)` variants, matching `NetworkPolicyId`, `ClusterId`,
`RoleId`, etc. Two allocator keys track the next ID in each namespace.
Builtin sizes synced from the env var get `System` IDs; user-defined
sizes created via SQL get `User` IDs.

Replicas still reference sizes by name (`ManagedLocation.size` remains
a `String`). The ID is catalog-internal identity only; the name is the
user-facing reference. This avoids migrating the replica storage format.

#### Proto definitions

```rust
// Key — the durable identity
pub struct ClusterReplicaSizeKey {
    pub id: ClusterReplicaSizeId,
}

pub enum ClusterReplicaSizeId {
    System(u64),
    User(u64),
}

// Value — the allocation details and metadata
pub struct ClusterReplicaSizeValue {
    pub name: String,
    pub memory_limit: Option<u64>,      // bytes
    pub memory_request: Option<u64>,
    pub cpu_limit: Option<u64>,         // nanocpus
    pub cpu_request: Option<u64>,
    pub disk_limit: Option<u64>,        // bytes
    pub scale: u16,
    pub workers: u64,
    pub credits_per_hour: String,       // Numeric as string
    pub cpu_exclusive: bool,
    pub is_cc: bool,
    pub swap_enabled: bool,
    pub disabled: bool,
    pub selectors: BTreeMap<String, String>,
    pub builtin: bool,
}
```

#### Combined durable type

```rust
pub struct ClusterReplicaSize {
    pub id: ClusterReplicaSizeId,
    pub name: String,
    pub allocation: ReplicaAllocation,
    pub builtin: bool,
}
```

Name uniqueness across the table is enforced by the `UniqueName`
trait on `ClusterReplicaSizeValue`.

### Builtin sync

On every catalog open, `sync_builtin_cluster_replica_sizes()` reconciles
the env-var `ClusterReplicaSizeMap` with durable state:

* **New sizes** in the env var are inserted with `builtin: true`.
* **Changed allocations** are retracted and reinserted.
* **Removed sizes** are handled based on usage:
  * If no replica references the size, it is deleted.
  * If a replica still uses it, it is marked `disabled: true` with a
    warning log, preventing new replicas from using it while avoiding
    panics in `concretize_replica_location`.

`CatalogState.cluster_replica_sizes` is initialized empty and populated
from durable state updates (via `apply_cluster_replica_size_update`)
rather than directly from config.

### SQL DDL

```sql
CREATE CLUSTER REPLICA SIZE <name> (
    CREDITS PER HOUR = '<numeric>',       -- required
    WORKERS = <n>,                        -- default 1
    SCALE = <n>,                          -- default 1
    MEMORY LIMIT = '<size>',              -- e.g. '4GiB', '512MiB'
    CPU LIMIT = '<cpu>',                  -- e.g. '0.5', '500m'
    DISK LIMIT = '<size>',
    CPU EXCLUSIVE = <bool>,
    DISABLED = <bool>,
    IS CC = <bool>,                       -- default true
    SWAP ENABLED = <bool>,
    NODE SELECTORS = '<json>'             -- e.g. '{"k8s.io/arch": "arm64"}'
);

DROP CLUSTER REPLICA SIZE <name>;
```

**Human-readable units:**
* Memory/disk: `GiB`, `MiB`, `GB`, `MB`, `kB`, or raw bytes.
* CPU: cores (`0.5`, `2`), millicpus (`500m`), or raw nanocpus.

**Access control:**
* Gated behind `enable_custom_cluster_replica_sizes` feature flag
  (default off).
* `mz_system` bypasses the feature flag.
* RBAC requires superuser for both CREATE and DROP.
* Cannot drop builtin sizes or sizes in use by existing replicas.
* Cannot create a size with a name that already exists.

**Credit enforcement:** In self-managed deployments, `credits_per_hour`
is automatically calculated as `(memory_limit * scale) / 1 GiB`,
matching the existing behavior of env-var sizes. The user-provided
`CREDITS PER HOUR` value is ignored. This ensures the DDL cannot be
used to bypass license-based billing limits. Cloud deployments (where
the license allows credit consumption overrides) honor the user-provided
value.

**Immutability:** Sizes cannot be altered after creation. To change a
size definition, drop it (after migrating replicas off it) and recreate
with the new parameters.

### System tables

**`mz_catalog.mz_cluster_replica_sizes`** (existing, public): Updated
via durable state updates instead of manual startup packing. Shows
`size`, `processes`, `workers`, `cpu_nano_cores`, `memory_bytes`,
`disk_bytes`, `credits_per_hour`.

**`mz_internal.mz_cluster_replica_size_details`** (new, public):
Exposes the full allocation including `cpu_exclusive`, `is_cc`,
`swap_enabled`, `disabled`, `builtin`, and `node_selectors` (as JSONB).

### Audit logging

`ObjectType::ClusterReplicaSize` added to the audit log enum. CREATE
and DROP operations emit audit events with `EventDetails::IdNameV1`.

## Alternatives

### String key (name) as the catalog key

An earlier iteration used the size name as the durable key directly,
avoiding the need for a separate ID type. This worked but was
inconsistent with every other catalog object and would have made
size rename impossible without replica migration (since the name
would be the primary identity). The ID approach adds minor
complexity in exchange for pattern consistency and future rename
support, without requiring any changes to how replicas reference
sizes today.

### Mutable sizes with drift tracking

An alternative design would allow `ALTER CLUSTER REPLICA SIZE` and
track which allocation each replica was created with. This enables
live reconfiguration but introduces significant complexity: snapshot
allocation at creation time, drift detection columns, history tables.
The immutable approach is simpler and sufficient for the initial use
case. ALTER can be added later if needed.

### Storing `ReplicaAllocation` directly in the durable value

The durable `ClusterReplicaSizeValue` could embed `ReplicaAllocation`
directly. However, `ReplicaAllocation` contains `Numeric` (for
`credits_per_hour`) which doesn't implement `Eq`/`Ord`, both required
by `TableTransaction`. Storing raw `u64`/`String` fields and
converting in `DurableType::from_key_value` avoids this constraint.

## Open Questions

* Should we actually provide this to self-managed customers?
* Adding new replica sizes in cloud is tricky and requires input from finance.
  How do we prevent support from going wild and creating new replica sizes.
