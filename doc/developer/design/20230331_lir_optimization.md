- Feature name: Context-guided MIR-to-LIR lowering and LIR-to-LIR rewrites
- Associated: MaterializeInc#17437, MaterializeInc#14442

# Summary
[summary]: #summary

There is a potential disconnect between the expression of physical properties of streams and operators across MIR, LIR,
and rendering. This disconnect can lead to suboptimal implementations under certain contexts to go undetected, as well
as to difficulties in implementing optimizations that depend on low-level physical properties of operator implementations.
We propose to express more of these physical properties in LIR by: (1) Reifying rendering implementation decisions such as
arrangement creation for consolidation as LIR `Plan` nodes; (2) Guiding MIR-to-LIR lowering by contextual properties of
streams such as monotonicity or presence of a single timestamp; (3) Introducing LIR-to-LIR rewrites to create plan refinements
that are sensitive to these physical properties. For one-shot `SELECT`s as well as regular dataflows, these LIR-level
optimizations can bring about lower query processing time, improving interactivity, and reduced memory footprint, increasing stability.

# Motivation
[motivation]: #motivation

Increasingly, Materialize will be used for mixed workloads, where materialized views, indexed views, and non-trivial one-shot `SELECT`s need to co-exist.
Additionally, these mixed workloads will include sources that are classic in incremental view maintenance (IVM) settings, such as ones obtained
by database replication, but also include a few sources that are append-only. Thus, we need ways to make query planning in Materialize respond
to different physical source characteristics, planning contexts, and goals, while delivering the best interactive experience and stability.

# Explanation
[explanation]: #explanation

We presently have a potential disconnect between physical properties that MIR assumes and that LIR/rendering implement,
with potential consequences for the performance and memory footprint of dataflows. For example, MIR contains an analysis of
monotonicity, which affects `Plan` operator choices. This analysis is intended to derive that streams contain no retractions in
their physical representation. LIR encodes that monotonic `Plan` options exist. However, some of the rendering implementations
introduce `consolidate_named` calls that are opaque to LIR and introduce unnecessary arrangements for monotonic streams. If
the enforcement of these arrangements had to be represented in LIR, we would have the opportunity to spot that they can be
optimized away. This problem can cause us to OOM in an append-only source computing a simple maximum of all elements seen
so far in a large stream, when such a computation should only require modest memory.

Additionally, one-shot `SELECT`s could profit from specific optimizations in the context where we know that only a single
timestamp will be presented in the input streams. For example, `ArrangeBy` LIR enforcers could be, in this case, introduced to
make non-monotonic sources become monotonic. This can unlock the use of monotonic `Plan` variants, often associated with large
improvements in query processing time and lower memory allocation spikes. Moreover, reductions also produce monotonic output in
this context. Optimizations that lift or remove `ArrangeBy` enforcers depending on monotonicity and one-shot planning contexts
can therefore bring about gains. These optimizations are arguably best expressed as LIR-level rewrites.

Finally, in some situations, characteristics of rendering implementations may introduce worst-case regressions that we wish to
mitigate by plan rewrites. In [#17013](https://github.com/MaterializeInc/materialize/pull/17013), an MIR rewrite is introduced that
breaks complex MIR `Reduce` nodes into column-wise delta joins instead of a reduce collation. Despite providing significant
worst-case improvements, this is not a generally desirable optimization, as it degrades performance, e.g., when the hierarchical
aggregates are monotonic or when they are over the same column. Reasoning about these considerations is conceptually more related
to LIR optimizations, and could potentially be expressed as an LIR rewrite. The impact would be eliminating worst-case
regressions while reducing the impact on the average case by leveraging physical properties to guide planning.

# Reference explanation
[reference-explanation]: #reference-explanation

We introduce three core ideas:

1. Exposing more information on arrangements created in rendering by introducing them explicitly as `Plan::ArrangeBy` LIR nodes, whenever possible.
2. Introducing two additional data structures that support MIR-to-LIR lowering, namely `LIRContext` and `PhysicalProperties`:
```Rust
    pub fn from_mir(
        expr: &MirRelationExpr,
        arrangements: &mut BTreeMap<Id, AvailableCollections>,
        debug_info: LirDebugInfo<'_>,
        context: LirContext,
    ) -> Result<(Self, AvailableCollections, PhysicalProperties), String>
```
3. Creating LIR-to-LIR rewrites to capture physical plan transformations such as lifting of `Plan::ArrangeBy` LIR nodes.

Initially `LirContext` could contain similar to what is transited in `MonotonicFlag` for determining monotonicity, e.g.:

```Rust
struct LIRContext {
    mon_ids: &BTreeSet<GlobalId>,
    locals: &mut BTreeSet<LocalId>,
}
```

Similarly, `PhysicalProperties` would then contain:

```Rust
struct PhysicalProperties {
    monotonic: bool,
}
```

Over time, we can add other information to `LirContext`, e.g., a boolean indicating a single-timestamp context for one-shot `SELECT`s,
as well as to `PhysicalProperties`. In single-timestamp contexts, lowering can introduce `Plan::ArrangeBy` nodes as enforcers of monotonicity,
i.e., add `Plan::ArrangeBy` to each input that is not already arranged and that is not monotonic. Additionally, in single-timestamp contexts,
we can indicate monotonicity in `PhysicalProperties` at the output of `Plan::ArrangeBy` and `Plan::Reduce` nodes. The MIR-to-LIR lowering
procedure can utilize the `LirContext` and `PhysicalProperties` to effectively select the best `Plan` variants for each case.

To exemplify LIR-to-LIR rewrites, consider the case of one-shot `SELECT`s. Here, we know that we are operating in a single-timestamp context.
Above, we argued that in this context, `Plan::ArrangeBy` enforcers could be introduced on non-monotonic inputs during lowering. However, these
enforcers are not always necessary (e.g., the plan could only contain monotonic operators after reductions). Thus, we could introduce a LIR-to-LIR
rewrite `ArrangeByLifting`, where lift `ArrangeBy` nodes to eliminate them if they meet operators that arrange their input or we do not find any
operator downstream that requires arranged data.

# Rollout
[rollout]: #rollout

Describe what steps are necessary to enable this feature for users.
How do we validate that the feature performs as expected? What monitoring and observability does it require?

Context-guided MIR-to-LIR lowering and LIR rewrites should be performed gradually. We outline a few
potential first steps to validate this approach:

1. We could start by reifying `consolidate_named` calls in rendering of monotonic operators as `Plan::ArrangeBy` nodes. This change should not change
the physical execution characteristics of any plan, but allows us to start providing more control to LIR of where arrangements are placed.
2. We could then move monotonicity analysis out of MIR and into LIR lowering as suggested above. This will allow us to safely remove `Plan::ArrangeBy`
calls and eliminate the `MonotonicFlag` MIR analysis. We will here need to monitor plan regressions and ensure that testing can still observe monotonic
plans via `EXPLAIN PHYSICAL PLAN`.
3. Subsequently, we can start iterating on improvements for one-shot `SELECT`s as outlined in the previous section. Here, some care should be exercised
to ensure that our test coverage for IVM plans does not decrease, e.g., by gating the plan changes behind a feature flag, and desiging an evolution strategy
for our tests. Additionally, we would need to evolve `EXPLAIN` to provide observability into different planning strategies (for IVM vs. one-shot `SELECT`s).

## Testing and observability
[testing-and-observability]: #testing-and-observability

## Lifecycle
[lifecycle]: #lifecycle

# Drawbacks
[drawbacks]: #drawbacks

The approach advocated in this design document requires investment in LIR planning. It is possible that we could make incremental progress with MIR and manage
the disconnects with physical planning on a case-by-case basis without making this investment. However, this has the risk that further disconnects result
in plans that lower interactive performance or stability.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

# Unresolved questions
[unresolved-questions]: #unresolved-questions

Should we continue advancing this design?

What belongs as an MIR transform and what should be an LIR-to-LIR rewrite?

# Future work
[future-work]: #future-work

This design does not exhaust the possible evolutions of `LirContext` and `PhysicalProperties` nor the possible LIR-to-LIR rewrites. By contrast, it opens up
many possibilities to further evolve plans in response to observations of the performance characteristics of rendered dataflows.
