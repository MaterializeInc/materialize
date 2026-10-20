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
sizes created via SQL get `User` IDs. A `System` ID is what marks a size
as builtin; there is no separate flag.

#### References

The first step keeps replicas referencing sizes by name
(`ManagedLocation.size` and `ClusterVariantManaged.size` remain
`String`s). A follow-up, which must land before the SQL surface, switches
replicas and managed cluster configs to store `ClusterReplicaSizeId`.
Names then serve only display and SQL lookup, and in-use checks become
ordinary dependency checks.

The name-to-ID rewrite cannot be a durable catalog migration. Migrations
cannot read `--cluster-replica-sizes`, and `upgrade()` runs all pending
migrations back to back before the builtin sync has populated the size
collection, so a migration would find no size records to point at. The
rewrite runs at catalog open instead, after the sync. System views such
as `mz_cluster_replicas` then resolve the size name through a join
rather than reading it from the replica's stored location.

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
    pub cpu_limit: Option<u64>,         // nanocpus
    pub cpu_request: Option<u64>,
    pub disk_limit: Option<u64>,        // bytes
    pub scale: u16,
    pub workers: u64,
    pub credits_per_hour: String,       // Numeric as string
    pub cpu_exclusive: bool,
    pub is_cc: bool,
    pub family: Option<String>,
    pub swap_enabled: bool,
    pub disabled: bool,
    pub selectors: BTreeMap<String, String>,
}
```

#### Combined durable type

```rust
pub struct ClusterReplicaSize {
    pub id: ClusterReplicaSizeId,
    pub name: String,
    pub allocation: ReplicaAllocation,
}
```

Name uniqueness across the table is enforced by the `UniqueName`
trait on `ClusterReplicaSizeValue`.

### Builtin sync

On every catalog open, `sync_builtin_cluster_replica_sizes()` reconciles
the env-var `ClusterReplicaSizeMap` with durable state:

* **New sizes** in the env var are inserted with a `System` ID.
* **Changed allocations** are updated in place, keeping the size's ID.
* **Removed sizes** are handled based on usage:
  * If nothing uses the size, it is deleted.
  * If something still uses it, it is marked `disabled: true` with a
    warning log, preventing new replicas from using it while avoiding
    panics in `concretize_replica_location`.

A size is in use if any of these name it:

* a managed replica's location;
* a managed cluster's configured size;
* a managed cluster's in-flight graceful reconfiguration target;
* a managed cluster's in-flight hydration burst size.

The cluster references matter because a managed cluster can depend on a
size without having a replica on it, for example a cluster with
`SCHEDULE = ON REFRESH` between refreshes, or one at replication factor
0. Counting only replicas would delete such a size, and the next replica
creation would fail, so a scheduled materialized view would stop
refreshing.

A disabled size keeps existing replicas running, but the cluster
controller still refuses disabled sizes when it creates replicas
(`allow_disabled = false`), so a scheduled cluster pinned to a disabled
size cannot bring its replicas back. Letting the controller use a
disabled size that matches the cluster's configured size is a
follow-up.

`CatalogState.cluster_replica_sizes` is initialized empty and populated
from durable state updates (via `apply_cluster_replica_size_update`)
rather than directly from config.

#### Editing a size

A size definition and the replicas using it can disagree only until the
replica is next provisioned. The contract:

* **Builtin sizes.** Editing a size in `--cluster-replica-sizes` and
  rolling out environmentd resizes every replica on that size. This is
  current behavior, not new: at boot, replica locations are re-derived
  from the size map. Creating a new `System` ID per edit instead would
  leave existing replicas on the old allocation indefinitely, which
  breaks how cloud tunes sizes today.
* **User-defined sizes.** Immutable once created; see "Immutability"
  below.
* **Goal.** A size change should apply only when a replica is recreated,
  not to replicas that are already running. Today an environmentd
  rollout re-provisions every replica, so for builtin sizes a rollout
  counts as recreation.

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
* Cannot drop builtin sizes. Dropping a size that anything uses (see
  "Builtin sync" for the full list of references) is refused as a
  dependency check at the `DROP`, consistent with other catalog objects,
  not only at boot.
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

* **Name collisions between builtin and user sizes.** Size names are
  unique across `System` and `User` IDs. If a user creates `foo` and a
  later `--cluster-replica-sizes` adds `foo`, the sync's insert fails and
  so does catalog open, blocking the rollout. Self-managed customers
  already define their own sizes in the Helm chart, so sizes we add to
  the chart can overlap with theirs too. Options:
  * Fail catalog open. Simple and loud. With zero-downtime upgrades the
    new environmentd fails in read-only mode and the old one keeps
    serving, but other rollout strategies cause an outage.
  * Skip the conflicting builtin size, log a warning, and keep booting.
    Once references are by ID, a name collision cannot change what any
    existing replica runs on; the user's size keeps the name.
  * Reserve builtin name patterns (for example `\d+cc` and `M.\d+-*`)
    and reject user sizes matching them, or require a prefix for user
    sizes.

* Should we actually provide this to self-managed customers?
* Adding new replica sizes in cloud is tricky and requires input from finance.
  How do we prevent support from going wild and creating new replica sizes.
