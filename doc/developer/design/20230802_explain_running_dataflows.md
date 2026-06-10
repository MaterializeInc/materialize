- Feature name: `EXPLAIN <catalog item>`
- Associated: MaterializeInc/database-issues#6222 (tracked in MaterializeInc/console#549)

# Summary
[summary]: #summary

At the moment, `EXPLAIN` variations that directly reference catalog items return output based on IRs obtained by re-optimizing the SQL statement for that item.
This could be surprising for users that run `EXPLAIN MATERIALIZED VIEW ...` in order to diagnose a running view.
We should change this behavior so `EXPLAIN MATERIALIZED VIEW ...` and `EXPLAIN INDEX ...` reflect the optimizer state at the time when the dataflow was originally created.

# Motivation
[motivation]: #motivation

Various troubleshooting and optimization techniques for long-running dataflows depend on looking and interpreting the optimized and physical plans for the index or materialized view associated with that dataflow.
At the moment, both customers and field engineering rely on

```sql
EXPLAIN OPTIMIZED PLAN FOR MATERIALIZED VIEW $name
```

to get this information for materialized views, and we still don't support the counterpart syntax for index items.

However, even for the `MATERIALIZED VIEW` syntax, the information can be misleading because the `EXPLAIN` output is based on an a new optimization run that is executed while handling the `EXPLAIN` statement.
Even though our optimizer is deterministic, this will only provide results consistent with the dataflow that is actually running while the environment context that guides the optimizer remains unchanged.
For example, if the user does:

```sql
-- The definition of V optimized with an empty set of indexes.
CREATE MATERIALIZED VIEW V AS (SELECT * FROM T where a = 5);
-- A new index on T(a) is created.
CREATE T_a INDEX ON T(a);
-- The explain is produced from an optimization run that assumes that an index on T(a) exists.
EXPLAIN MATERIALIZED VIEW V;
```

The `EXPLAIN` output will indicate that we are using `T_a` for the point lookup, while in practice this will not be the case because the index was created _after_ installing the dataflow that maintains `V`.

This design doc proposes a sequence of changes to the catalog and our `EXPLAIN` handling code that would allow us to change the behavior of

```sql
EXPLAIN OPTIMIZED PLAN FOR MATERIALIZED VIEW $name
EXPLAIN PHYSICAL PLAN FOR MATERIALIZED VIEW $name
```

and implement similar behavior for

```sql
EXPLAIN OPTIMIZED PLAN FOR INDEX $name
EXPLAIN PHYSICAL PLAN FOR MATERIALIZED VIEW $name
```

# Non-goals

Even though the problems with `OPTIMIZED PLAN` and `PHYSICAL PLAN` are identical, a solution that supports both might be more complicated that one that supports only the former.

Depending on the agreed solution, we might want to punt on


```sql
EXPLAIN PHYSICAL PLAN FOR [INDEX $name | MATERIALIZED VIEW $name]
```

queries for the MVP delivered by MaterializeInc/database-issues#6222.

# Explanation
[explanation]: #explanation

At the moment, `sequence_explain_plan` constructs its output using information derived from optimizations done at the time when `sequence_explain_plan` runs.

At the high level, the API that constructs an `EXPLAIN` output for a given plan looks like that:

```rust
let context = ExplainContext {
    config,
    humanizer,
    used_indexes,
    finishing,
    duration,
};

Explainable::new(&mut plan).explain(format, context)
```

We need to
(1) analyze which values in the above code snippet represent information that should reflect the state of the world when the dataflow was originally created, and
(2) suggest mechanisms to make these values available for subsequent `EXPLAIN <catalog item>` calls.

In general, we can persist the required data
(a) in-memory in the `Coordinator` instance of the running `environmentd` process, or
(b) durably, as extensions of the `SerializedCatalogItem` structures that represent the part of the catalog state that survives restarts.

Since at the moment every `CatalogItem` is fully re-optimized upon an `environmentd` restart in the catalog re-hydration phase, the current proposal focuses on option (a).
However, note that if we introduce plan pinning or similar features in the future, we might need to revisit this decision.

The comments in the following snippet summarize the high-level implementation plan:

```rust
let context = ExplainContext {
    config: &config,     // Derived from the enclosing `EXPLAIN` statement plan.
    humanizer: &catalog, // Passing the current catalog state.
    used_indexes,        // Derived from the saved `DataflowDescription`.
    finishing,           // Always use `None` here.
    duration,            // Always use `Duration::default()` here.
};

// The `plan` is saved in memory when the `CatalogItem` is processed.
// Use `format` from the enclosing `EXPLAIN` plan.
Explainable::new(&mut plan).explain(format, context)
```

Basically

- The `config` is something only depends on the current catalog state.
- The `humanizer` value can be the current catalog state because item IDs are stable and cannot be recycled.
- The `finishing` and `duration` values can be constants.

so the only time-dependent information that we need to record is the explained `plan`, which is a `DataflowDescription` instance.
The next section expands how we plan to do that with more concrete implementation details.

# Reference explanation
[reference-explanation]: #reference-explanation

There are two competing approaches that both follow the same high-level structure.

The [alternatives](#considered-alternatives) presents a design that overall seems to be the obvious solution.
However, due to some inerent issues that arise when trying to implement it, my proposal is slightly different.
It will allow us to ship the feature and buy us some time to figure out how to properly refactor the code in order to ultimately move to the (conceptually better) alternative solution.
Most likely, steps in this direction can be made with MaterializeInc/materialize#20569, but I don't think we should take this as a hard prerequisite for shipping MaterializeInc/database-issues#6222.

## Provision plan memoization infrastructure in the `Catalog`

Ideally, we want to provision the structures to hold the `OPTIMIZED` and `PHYSICAL` plans directly in the `CatalogState`.
However, this turns out to be a non trivial task (see the discussion for the [alternative solution](#considered-alternatives)).

Instead, we propose to directly extend `Catalog` with a new field

```rust
pub struct Catalog {
    ...
    plans: CatalogPlans,
}
```

where `CatalogPlans` is an in-memory side container for the derived plans

```rust
pub struct CatalogPlans {
    optimized_plan_by_id: BTreeMap<GlobalId, DataflowDesc>,
    physical_plan_by_id: BTreeMap<GlobalId, DataflowDesc>,
}
```

and the `Catalog` exposes the following interface for memoizing and retrieving plans:

```rust
impl Catalog {
    pub fn set_optimized_plan(&mut self, id: GlobalId, plan: DataflowDesc) {}

    pub fn set_physical_plan(&mut self, id: GlobalId, plan: DataflowDesc) {}

    pub fn try_get_optimized_plan(&self, id: &GlobalId) -> Option<&DataflowDesc> {}

    pub fn try_get_physical_plan(&self, id: &GlobalId) -> Option<&DataflowDesc> {}

    pub fn drop_plans(&mut self, id: GlobalId)  {}
}
```

## Memoize plans

Producing the `OPTIMIZED` and `PHYSICAL` plans before the `CatalogItem` is constructed comes at considerable refactoring effort (see the [alternative solution](#considered-alternatives) discussion).
Instead, we can just use the new `Catalog` API to manage the lifecycle of plans in alignment with the lifecycle of the associated `CatalogItem` entries.

1. Add `set_optimized_plan` and `set_physical_plan` calls at the places where we currently produce those plans, and as close as possible to the places where the corresponding `CatalogItem` entries are created:
    1. Towards the end (after the catalog transaction commit) in `sequence_create_materialized_view` and `sequence_create_index`.
    2. After the `Catalog::open` call (which calls `load_catalog_items` in order to re-hydrate the `CatalogItem` state) in `Coordinator::bootstrap`.
2. Use `try_get_optimized_plan` / `try_get_physical_plan` to obtain the plan when explaining the corresponding catalog item.
3. Add `drop_plans` calls close to `CatalogState::drop_item` calls in order to delete the memoized
    1. Towards the end of the `Catalog::transact` body (`transact` calls `transact_inner` which has `drop_item` calls, but lacks a `self` parameter).
    2. After `Catalog::apply_in_memory_builtin_migration` calls.

With this solution, we are not forced to commit to the refactoring refort as part of delivering MaterializeInc/database-issues#6222.

## Refactor `EXPLAIN` handling

Add new `explain_index` and `explain_materialized_view` methods to the `Coordinator` and dispatch on these methods from `sequence_explain`.

The workflow in these methods should be roughly as follows:

1. Grab a `CatalogItem` for the explained object from the `CatalogState`.
3. Grab the `DataflowDescription` for the requested optimization stage from the new `Catalog` API.
2. Create an `ExplainContext` from the `DataflowDescription` and the enclosing `ExplainPlan`.
4. Call `Explainable::new(&mut plan).explain(format, context)`.


# Rollout
[rollout]: #rollout

The proposed implementation path allows to stage the rollout by first merging the necessary changes to the `CatalogItem` variants and observing how the additional data affects our existing environments.
Given the relatively low number of indexes and materialized views that we currently expect to see per environment dramatic changes in the `environmentd` resource footprint and performance due to this change seems unlikely.
Based on offline discussion, it seems that his is low risk, so we are currently aiming in rolling this out with a series of PR that introduce the new functionality.

To make reviews more digestabdle, I can envision the following sequence of PRs:

1. A PR that contains the groundwork and the code changes required for `EXPLAIN OPTIMIZED PLAN FOR MATERIALIZED VIEW`.
1. A PR that contains the groundwork and the code changes required for `EXPLAIN PHYSICAL PLAN FOR MATERIALIZED VIEW`.
1. A PR that adds support for `EXPLAIN INDEX` (mostly copy-paste from the above two).
1. A PR that either refactors or deprecates `EXPLAIN VIEW`.

## Testing and observability
[testing-and-observability]: #testing-and-observability

We will ensure that we have sufficient code coverage for the `EXPLAIN <catalog item>` behavior with new `*.slt` tests.

At the moment, there is no plan to gate the changes behind a feature flag because this will complicate the development process, but they are considered risky this can be revisited.

## Lifecycle
[lifecycle]: #lifecycle

If the design is risky or has the potential to be destabilizing, you should plan to roll the implementation out behind a feature flag.
List all feature flags, their behavior and when it is safe to change their value.

# Drawbacks
[drawbacks]: #drawbacks

A somewhat hot take is that people should not rely on `EXPLAIN` to get runtime information.
Instead, we can try to make our dataflow graph visualization better and remove the `EXPLAIN <catalog item>` variants altogether.
The `EXPLAIN CREATE ...` syntax proposed in MaterializeInc/database-issues#5301 should be sufficient for people actively developing SQL queries that are meant to be installed as new dataflows.

# Considered alternatives
[considreed-alternatives]: #considreed-alternatives

A more direct approach would be to directly attach the new fields to the `CatalogItem` variants.

## Provision plan memoization infrastructure in the `Catalog`

We do this by directly modifying the `CatalogState`.
In particular, we extend the `CatalogItem::Index` and `CatalogItem::MaterializedView` variants with extra fields that record the `DataflowDescription` for the `OPTIMIZED` and `PHYSICAL` plans:

```rust
// idential extensions to `index`
struct MaterializedView {
    ...
    optimized_plan: DataflowDescription<OptimizedMirRelationExpr, ()>,
    physical_plan: DataflowDescription<Plan, ()>,
}
```

## Change `CatalogItem` constructors

Change the code that creates new `Index` and `MaterializedView` structs for the corresponding `CatalogItem` variants in order to populate these fields.
This means that we will need to complete the entire optimization path upfront and obtain the two `plan` values.
It is especially hard to do so for the following reasons:

- When re-hydrating the catalog, these variants are re-created in the `Catalog::parse_item` method, which does not have access to the `Coordinator`.
  However, the `Coordinator` is required for the global `MIR ⇒ MIR` and the `MIR ⇒ LIR` phases.
  These happen only once we can create a `DataflowDescription`, but the `DataflowBuilder` depends on the `Coordinator`.
- Similarly, in the `sequence_~` methods it will be substantial heavy-lifting to move pull all optimization code up.
  At the moment, the `DataflowBuilder` is invoked the catalog transaction is committed, at which point we can no longer modify the `CatalogEntry`.
  This is especially problematic for the `MIR ⇒ LIR` lowering and the `LIR ⇒ LIR` transformation phases, which happen in `finalize_dataflow` (see the [Future work](#future-work) section).

## Refactor `EXPLAIN` handling

Add new `explain_index` and `explain_materialized_view` methods to the `Coordinator` and dispatch on these methods from `sequence_explain`.

The workflow in these methods should be roughly as follows:

1. Grab a `CatalogItem` for the explained object from the `CatalogState`.
3. Grab the `DataflowDescription` for the requested optimization stage from the new `CatalogItem` field.
2. Create an `ExplainContext` from the `DataflowDescription` and the enclosing `ExplainPlan`.
4. Call `Explainable::new(&mut plan).explain(format, context)`.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

The reference guide presrcibes:

> Change client code that creates new `Index` and `MaterializedView` in order to complete the entire optimization path upfront and obtain values for the new fields to the `CatalogItem` instance.

However, the draft PR implementation revealed some issues with that (see [this PR comment](https://github.com/MaterializeInc/materialize/pull/21261#issuecomment-1684147730)).


# Future work
[future-work]: #future-work

Note that `UsedIndexes` is currently computed inside `optimize_dataflow_index_imports`, which is  called towards the end of the “global” MIR optimization phase in `optimize_dataflow`.
This will change with MaterializeInc/database-issues#4806 and will most certainly cause conflicts if we don't coordinate the work between MaterializeInc/database-issues#6222 (this doc) and MaterializeInc/database-issues#4806.

---

MIR ⇒ LIR lowering happens in the following stack (in reverse order):

- `mz_compute_types::plan::Plan::lower_dataflow`
  - `mz_compute_types::plan::Plan::finalize_dataflow`
    - `Coordinator::finalize_dataflow`
      - `Coordinator::create_peek_plan`
        - `Coordinator::plan_peek`
          - `Coordinator::peek_stage_finish`
            - `Coordinator::sequence_peek_stage`
              - `Coordinator::sequence_peek`
      - `Coordinator::sequence_explain_pipeline`
        - `Coordinator::sequence_explain_plan`
          - `Coordinator::sequence_explain`
      - `Coordinator::must_finalize_dataflow`
          - `Coordinator::bootstrap` (for `Index`)
      - `Coordinator::ship_dataflow`
        - `Coordinator::must_ship_dataflow`
          - `Coordinator::bootstrap` (for `MaterializedView`)
          - `Coordinator::sequence_create_materialized_view`
          - `Coordinator::sequence_create_index`
        - `Coordinator::sequence_subscribe`

For the `sequence_create_materialized_view` and `sequence_create_index` paths in the sequence methods, the call chain happens _after_ the corresponding `catalog::Op::CreateItem` instance is created.
Consequently, setting the `physical_plan` field in the `MaterializedView` and `Index` catalog items will require some (probably brittle) refactoring to the call chains in the above tree.
However, without this field we won't be able to handle `EXPLAIN PHYSICAL PLAN` requests for these two catalog items.

---

Some more changes will be required if we decide to implement plan pinning. A naive approach would be to the LIR plan to `SerializedCatalogItem::V2`. A question remains what to do the other `ExplainInfo` fields. If we don’t serialize them, but instead try re-construct them during catalog re-hydration, we won’t get a faithful representation of the running plans in the `EXPLAIN <catalog item>` output.

# Appendix A: `ExplainConfig` support

The below code snippet summarizes which `ExplainConfig` options will be supported in the new `EXPLAIN <catalog item>` syntax.

```rust
pub struct ExplainConfig {
    pub arity: bool,           // supported
    pub join_impls: bool,      // supported
    pub keys: bool,            // supported
    pub linear_chains: bool,   // supported
    pub non_negative: bool,    // supported
    pub no_fast_path: bool,    // ignored (there is no fast path either way)
    pub raw_plans: bool,       // supported
    pub raw_syntax: bool,      // supported
    pub subtree_size: bool,    // supported
    pub timing: bool,          // ignored
    pub types: bool,           // supported
    pub filter_pushdown: bool, // unclear
    pub cardinality: bool,     // ignored
}
```

Note that we need need to be careful with the `filter_pushdown` handling. There are some options:

1. Don't respect the `filter_pushdown` value. Treat it as `true` for catalog items that were created with MFP pushdown enabled, and as `false` otherwise.
2. Don't respect the `filter_pushdown` value. Treat it as `false` for all catalog items.
3. Continue respecting the `filter_pushdown` value. Treat it as "what if" condition that explains what would be pushed.

My preferred behavior is option (1). Option (2) is a possibility if option (1) is too complicated. I have rejected option (3) because this is an instance of the behavior that we want to deprecate with #20652.

In terms of implementation guidelines for option (1), as long as re-optimization continues to happen in the catalog re-hydration phase, it should be OK to just hard-code the value of the `filter_pushdown` flag based on the current persist MFP behavior (enabled or not).

# Appendix B: Code structure analysis

This is useful for keeping track of relevant structures and call sites.

For in-memory catalog state:

- [`catalog::CatalogItem`](https://github.com/MaterializeInc/materialize/blob/500a357ee1317ea3c612ec98552bb05cff7e5493/src/adapter/src/catalog.rs#L1942-L1954), more specifically the variants that wrap
    - [`catalog::MaterializedView`](https://github.com/MaterializeInc/materialize/blob/500a357ee1317ea3c612ec98552bb05cff7e5493/src/adapter/src/catalog.rs#L2278-L2284)
        - constructed in
            - [`Catalog::parse_item`](https://github.com/MaterializeInc/materialize/blob/500a357ee1317ea3c612ec98552bb05cff7e5493/src/adapter/src/catalog.rs#L7415-L7421)
            - [`Coordinator::sequence_create_materialized_view`](https://github.com/MaterializeInc/materialize/blob/500a357ee1317ea3c612ec98552bb05cff7e5493/src/adapter/src/coord/sequencer/inner.rs#L985-L991)
    - [`catalog::Index`](https://github.com/MaterializeInc/materialize/blob/500a357ee1317ea3c612ec98552bb05cff7e5493/src/adapter/src/catalog.rs#L2287-L2296)
        - constructed in
            - [`CatalogState::insert_cluster`](https://github.com/MaterializeInc/materialize/blob/e755d81d2078cee715aa2aafa1c61e7262b33f1c/src/adapter/src/catalog.rs#L954-L974)
            - [`Catalog::parse_item`](https://github.com/MaterializeInc/materialize/blob/e755d81d2078cee715aa2aafa1c61e7262b33f1c/src/adapter/src/catalog.rs#L7423-L7432)
            - [`Coordinator::sequence_create_index`](https://github.com/MaterializeInc/materialize/blob/e755d81d2078cee715aa2aafa1c61e7262b33f1c/src/adapter/src/coord/sequencer/inner.rs#L1075-L1084)

For persisted catalog state, which serialized as Protobuf and persisted in CRDB:

- [`catalog::storage::ItemKey`](https://github.com/MaterializeInc/materialize/blob/e755d81d2078cee715aa2aafa1c61e7262b33f1c/src/adapter/src/catalog/storage.rs#L2050-L2052)
- [`catalog::storage::ItemValue`](https://github.com/MaterializeInc/materialize/blob/e755d81d2078cee715aa2aafa1c61e7262b33f1c/src/adapter/src/catalog/storage.rs#L2055-L2061)
    - wraps a [`catalog::SerializedCatalogItem`](https://github.com/MaterializeInc/materialize/blob/e755d81d2078cee715aa2aafa1c61e7262b33f1c/src/adapter/src/catalog.rs#L7876-L7878)
        - has only one variant `V1 { create_sql: String }`

The `CatalogItem ⇒ SerializedCatalogItem` conversion happens in [`Catalog::serialize_item`](https://github.com/MaterializeInc/materialize/blob/e755d81d2078cee715aa2aafa1c61e7262b33f1c/src/adapter/src/catalog.rs#L7273-L7314).
