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

  - deployment generation
  - object global ID
  - expression type (local MIR, global MIR, LIR, etc)

The value will be a serialized version of the optimized expression. An `environmentd` process with
deploy generation `n`, will never be expected to look at a serialized expression with a deploy
generation `m` s.t. `n != m`. Therefore, there are no forwards or backwards compatibility needed on
the serialized representation of expressions. The cache will also be made  durable so that it's
available after a restart, at least within the same deployment generation.

Upgrading an environment will look something like this:

1. Start deploy generation `n` in read-only mode.
2. Populate the expression cache for generation `n`.
3. Start deploy generation `n` in read-write mode.
4. Read optimized expressions from cache.

Restarting an environment will look something like this:

1. Start deploy generation `n` in read-write mode.
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
trait ExpressionCache {
    /// Opens a new [`ExpressionCache`] for `deploy_generation`.
    fn open(deploy_generation: u64) -> Self;
    
    /// Returns the `expression_type` of `global_id` that is currently deployed in a cluster. This
    /// will not change in-between restarts as result of DDL, as long as `global_id` exists.
    /// 
    /// This is useful for serving `EXPLAIN` queries.
    fn get_deployed_expression(&self, global_id: GlobalId, expression_type: ExpressionType) -> Option<Bytes>;

    /// Returns the `expression_type` of `global_id` based on the current catalog contents. This
    /// may change in-between restarts as result of DDL.
    fn get_durable_expression(&self, global_id: GlobalId, expression_type: ExpressionType) -> Option<Bytes>;

    /// Durably inserts `expression`, with key `(deploy_generation, global_id, expression_type)`.
    ///
    /// Panics if `(deploy_generation, global_id, expression_type)` already exists.
    fn insert_expression(&mut self, global_id: GlobalId, expression_type: ExpressionType, expression: Bytes);

    /// Durably remove and return all entries in `deploy_generation` that depend on an ID in
    /// `dropped_ids`.
    fn invalidate_entries(&mut self, dropped_ids: BTreeSet<GlobalId>) -> Vec<(GlobalId, ExpressionType)>;

    /// Durably removes all entries with a deploy generation less than this cache's deploy
    /// generation.
    fn cleanup_all_prior_deploy_generations(&mut self);

    /// Remove all entries that depend on a global ID that is not present in `txn`.
    fn reconcile(&mut self, txn: mz_catalog::durable::Transaction);
}
```

### Startup

Below is a detailed set of steps that will happen in startup.

1. Call `ExpressionCache::reconcile` to remove any invalid entries.
2. While opening the catalog, for each object:
    a. If the object is present in the cache, read the cached optimized expression via
       `ExpressionCache::get_durable_expression`.
    b. Else generate the optimized expressions and insert the expressions via
       `ExpressionCache::insert_expression`.
3. If in read-write mode, call `ExpressionCache::remove_deploy_generation` to remove the previous
   deploy generation.

### DDL - Create

1. Execute catalog transaction.
2. Update cache via `ExpressionCache::insert_expression`.

### DDL - Drop
1. Execute catalog transaction.
2. Invalidate cache entries via `ExpressionCache::invalidate_entries`.
3. Re-compute and repopulate cache entries that depended on dropped entries via
   `ExpressionCache::insert_expression`.

### File System Implementation

One potential implementation is via the filesystem of an attached durable storage to `environmentd`.
Each cache entry would be saved as a file of the format
`/path/to/cache/<deploy_generation>/<global_id>/<expression_type>`.

#### Pros
- No need to worry about coordination across K8s pods.
- Bulk deletion is a simple directory delete.

#### Cons
- Need to worry about Flushing/fsync.
- Need to worry about concurrency.
- Need to worry about atomicity.
- Need to worry about mocking things in memory for tests.
- If we lose the pod, then we also lose the cache.

### Persist implementation

Another potential implementation is via persist. Each cache entry would be keyed by
`(deploy_generation, global_id, expression_type)` and the value would be a serialized version of the
expression.

#### Pros
- Flushing, concurrency, atomicity, mocking are already implemented by persist.

#### Cons
- We need to worry about coordinating access across multiple pods. It's expected that during
  upgrades at least two `environmentd`s will be communicating with the cache.
- We need to worry about compaction and read latency during startup.

## Alternatives

For the persist implementation, we could mint a new shard for each deploy generation. This would
require us to finalize old shards during startup which would accumulate shard tombstones in CRDB.

## Open questions

- Which implementation should we use?
- If we use the persist implementation, how do we coordinate writes across pods?
  - I haven't thought much about this, but here's one idea. The cache will maintain a subscribe on
    the persist shard. Everytime it experiences an upper mismatch, it will listen for all new
    changes. If any of the changes contain the current deploy generation, then panic, else ignore
    them.
