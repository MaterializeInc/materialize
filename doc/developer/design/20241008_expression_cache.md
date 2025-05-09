# Expression Cache

## The Problem

Optimization is a slow process that is on the critical path for `environmentd` startup times. Some
environments spend 30 seconds just on optimization. All optimization time spent in startup is
experienced  downtime for users when `environmentd` restarts.

## Success Criteria

Startup spends less than 1 second optimizing expressions.

## Solution Proposal

The solution being proposed in this document is a cache of optimized expressions. During startup,
`environmentd` will first look in the cache for optimized expressions and only compute a new
expression if it isn't present in the cache. If enough expressions are cached and the cache is fast
enough, then the time spent on this part of startup should be small.

The cache will present similarly as a key-value value store where the key is a composite of

  - build version
  - object global ID

The value will be a serialized version of the optimized expression. An `environmentd` process with
build version `n`, will never be expected to look at a serialized expression with a build
version `m` s.t. `n != m`. Therefore, there are no forwards or backwards compatibility needed on
the serialized representation of expressions. The cache will also be made  durable so that it's
available after a restart, at least within the same build version.

Upgrading an environment will look something like this:

1. Start build version `n` in read-only mode.
2. Populate the expression cache for version `n`.
3. Start build version `n` in read-write mode.
4. Read optimized expressions from cache.

Restarting an environment will look something like this:

1. Start build version `n` in read-write mode.
2. Read optimized expressions from cache.

### Prior Art

The catalog currently has an in-memory expression cache.

  - [https://github.com/MaterializeInc/materialize/blob/bff231953f4bb97b70cae81bdd6dd1716dbf8cec/src/adapter/src/catalog.rs#L127](https://github.com/MaterializeInc/materialize/blob/bff231953f4bb97b70cae81bdd6dd1716dbf8cec/src/adapter/src/catalog.rs#L127)
  - [https://github.com/MaterializeInc/materialize/blob/bff231953f4bb97b70cae81bdd6dd1716dbf8cec/src/adapter/src/catalog.rs#L145-L345](https://github.com/MaterializeInc/materialize/blob/bff231953f4bb97b70cae81bdd6dd1716dbf8cec/src/adapter/src/catalog.rs#L145-L345)

This cache is used to serve `EXPLAIN` queries to ensure accurate and consistent responses. When an
index is dropped, it may change how an object _would_ be optimized, but it does not change how the
object is currently deployed in a cluster. This cache contains the expressions that are deployed in
a cluster, but not necessarily the expressions that would result from optimization from the current
catalog contents.

### Cache API

Below is the API that the cache will present. It may be further wrapped with typed methods that
take care of serializing and deserializing bytes. Additionally, we probably don't need a trait when
implementing.

```Rust
/// All the cached expressions for a single `GlobalId`.
///
/// Note: This is just a placeholder for now, don't index too hard on the exact fields. I haven't
/// done the necessary research to figure out what they are.
struct Expressions {
    local_mir: OptimizedMirRelationExpr,
    global_mir: DataflowDescription<OptimizedMirRelationExpr>,
    physical_plan: DataflowDescription<mz_compute_types::plan::Plan>,
    dataflow_metainfos: DataflowMetainfo<Arc<OptimizerNotice>>,
    notices: SmallVec<[Arc<OptimizerNotice>; 4]>,
    optimizer_feature: OptimizerFeatures,
}

struct ExpressionCache {
    build_version: Version,
    information_needed_to_connect_to_durable_store: _,
}

impl ExpressionCache {
    /// Creates a new [`ExpressionCache`] for `build_version`.
    fn new(&mut self, build_version: Version, information_needed_to_connect_to_durable_store: _) -> Self;

    /// Reconciles all entries in current build version with the current objects, `current_ids`,
    /// and current optimizer features, `optimizer_features`.
    ///
    /// If `remove_prior_versions` is `true`, all previous versions are durably removed from the
    /// cache.
    ///
    /// Returns all cached expressions in the current build version, after reconciliation.
    fn open(&mut self, current_ids: &BTreeSet<GlobalId>, optimizer_features: &OptimizerFeatures, remove_prior_versions: bool) -> Vec<(GlobalId, Expressions)>;

    /// Durably removes all entries given by `invalidate_ids` and inserts `new_entries` into
    /// current build version.
    ///
    /// If there is a duplicate ID in both `invalidate_ids` and `new_entries`, then the final value
    /// will be taken from `new_entries`.
    fn insert_expressions(&mut self, new_entries: Vec<(GlobalId, Expressions)>, invalidate_ids: BTreeSet<GlobalId>);

    /// Durably remove and return all entries in current build version that depend on an ID in
    /// `dropped_ids` .
    ///
    /// Optional for v1.
    fn invalidate_entries(&mut self, dropped_ids: BTreeSet<GlobalId>) -> Vec<(GlobalId, Expressions)>;
}
```

### Startup

Below is a detailed set of steps that will happen in startup.

1. Call `ExpressionCache::open` to read the cache into memory and perform reconciliation (See
   [Startup Reconciliation](#startup reconciliation)). When passing in the arguments,
   `remove_prior_versions == !read_only_mode`.
2. While opening the catalog, for each object:
    a. If the object is present in the cache, use cached optimized expression.
    b. Else generate the optimized expressions and insert the expressions via
       `ExpressionCache::insert_expressions`. This will also perform any necessary invalidations if
       the new expression is an index. See ([Create Invalidations](#create invalidations)).

#### Startup Reconciliation
When opening the cache for the first time, we need to perform the following reconciliation tasks:

  - Remove any entries that exist in the cache but not in the catalog.
  - If `remove_prior_versions` is true, then remove all prior versions.


### DDL - Create

1. Execute catalog transaction.
2. Update cache via `ExpressionCache::insert_expressions`.
3. [Optional for v1] Recompute and insert any invalidated expressions.

#### Create Invalidations

When creating and inserting a new index, we need to invalidate some entries that may optimize to
new expressions. When creating index `i` on object `o`, we need to invalidate the following objects:

  - `o`.
  - All compute objects that depend directly on `o`.
  - All compute objects that would directly depend on `o`, if all views were inlined.

### DDL - Drop

This is optional for v1, on startup `ExpressionCache::open` will update the cache to the
correct state.

1. Execute catalog transaction.
2. Invalidate cache entries via `ExpressionCache::invalidate_entries`.
3. Re-compute and repopulate cache entries that depended on dropped entries via
   `ExpressionCache::insert_expressions`.

### Implementation

The implementation will use persist for durability. The cache will be a single dedicated shard.
Each cache entry will be keyed by `(build_version, global_id)` and the value will be a
serialized version of the expression.

#### Conflict Resolution

It is possible and expected that multiple environments will be writing to the cache at the same
time. This would manifest in an upper mismatch error during an insert or invalidation. In case of
this error, the cache should read in all new updates, apply each update as described below, and
retry the operation from the beginning.

If the update is in a different build version as the current cache, then ignore it. It is in a
different logical namespace and won't conflict with the operation.

If the update is in the same build version, then we must be in a split-brain scenario where
both the current process and another process think they are the leader. We should still update any
in-memory state as if the current cache had made that change. This relies on the following
invariants:

  - Two processes with the same build version MUST be running the same version of code.
  - A global ID only ever maps to a single object.
  - Optimization is deterministic.

Therefore, we can be sure that any new global IDs refer to the same object that the current cache
thinks it refers to. Also, the optimized expressions that the other process produced is identical
to the optimized expression that the current process would have produced. Eventually, one of the
processes will be fenced out on some other operation. The reason that we don't panic immediately,
is because the current process may actually be the leader and enter a live-lock scenario like the
following:

1. Process `A` starts up and becomes the leader.
2. Process `B` starts up and becomes the leader.
3. Process `A` writes to the cache.
4. Process `B` panics.
5. Process `A` is fenced.
6. Go back to step (1).

#### Pros
- Flushing, concurrency, atomicity, mocking are already implemented by persist.

#### Cons
- We need to worry about coordinating access across multiple pods. It's expected that during
  upgrades at least two `environmentd`s will be communicating with the cache.
- We need to worry about compaction and read latency during startup.

## Alternatives

- For the persist implementation, we could mint a new shard for each build version. This would
require us to finalize old shards during startup which would accumulate shard tombstones in CRDB.
- We could use persist's `FileBlob` for durability. It's extremely well tested (most of CI uses it
  for persist) and solves at least some of the file system cons.
- We could use persist for durability, but swap in the `FileBlob` as the blob store and some local
  consensus implementation.

### File System Implementation

One potential implementation is via the filesystem of an attached durable storage to `environmentd`.
Each cache entry would be saved as a file of the format
`/path/to/cache/<build_version>/<global_id>`.

#### Pros
- No need to worry about coordination across K8s pods.
- Bulk deletion is a simple directory delete.

#### Cons
- Need to worry about Flushing/fsync.
- Need to worry about concurrency.
- Need to worry about atomicity.
- Need to worry about mocking things in memory for tests.
- If we lose the pod, then we also lose the cache.

## Open questions

- Which implementation should we use?
- If we use the persist implementation, how do we coordinate writes across pods?
  - I haven't thought much about this, but here's one idea. The cache will maintain a subscribe on
    the persist shard. Everytime it experiences an upper mismatch, it will listen for all new
    changes. If any of the changes contain the current build version, then panic, else ignore
    them.
