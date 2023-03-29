- Feature name: Monotonic rendering of SELECT queries
- Associated: (Insert list of associated epics, issues, or PRs)

# Summary
[summary]: #summary

<!--
One paragraph to explain the feature: What's wrong, what's the problem, what's the fix, what are the consequences?
-->

At the moment, we render dataflows for `SELECT` queries in the same way as dataflows serving indexes and materialized views.  This prevents us from using monotonic operators, which by design have a lower runtime memory footprint.  Instead, we want to exploit the fact that a `SELECT` queries the data at a single timestamp, which means that all data can be interpreted as monotonic.  With this change, we can render `SELECT` queries with more monotonic operators and we can teach the optimizer to distinguish several monotonicity variants.

# Motivation
[motivation]: #motivation

<!--
Why are we doing this? What problems does it solve? What use cases does it enable? What is the desired outcome?
-->

Materialize currently focuses on rendering a good plan for workloads that need incremental view maintenance (IVM).  This misses opportunities to optimize plans based on the time bounds of a dataflow, which are in the case for `SELECT` queries a single timestamp.  In this design, we present an updated approach for the optimizer to handle monotonicity information, and to tighten the contract between plans and how they're rendered.

# Explanation
[explanation]: #explanation

<!--
Explain the design as if it were part of Materialize and you were teaching the team about it.
This can mean:

- Introduce new named concepts.
- Explain the feature using examples that demonstrate product-level changes.
- Explain how it builds on the current architecture.
- Explain how engineers and users should think about this change, and how it influences how everyone uses the product.
- If needed, talk though errors, backwards-compatibility, or migration strategies.
- Discuss how this affects maintainability, or whether it introduces concepts that might be hard to change in the future.
-->

To start, we introduce the following monotonicity variants:
* *Non-monotonic*: The data can contain retractions over time.
* *Physically monotonic*: The data never contains retractions.
* *Logically monotonic*: The data can contain retractions, but once consolidated it is physically monotonic.

At the moment, the optimizer thinks in terms of the first two variants, but rendering assumes logical monotonicity.

We need to change the following parts of Materialize:

1. The optimizer derives monotonicity information based on the inputs and MIR expressions.  It does not take time bounds into consideration.  Here, it needs to flag inputs as logically monotonic if the dataflow only consumes a single point in time. This is partially achieved by MaterializeInc/materialize#14607.
2. The [monotonic flag calculation](../../../src/transform/src/monotonic.rs) needs to distinguish logical vs. physical monotonicity.  It seems we need to extend the optimizer to actively adjust plans based on monotonicity information and requirements.
3. LIR operators need to define what kind of monotonicity they can handle and produce.
4. LIR operators must not make assumptions about the monotonicity characteristics of the data but should expect their requirements to be fulfilled.  This means that operators requiring physical monotonicity should not by default insert a defensive consolidation.

## Testing

At the moment, we use `SELECT` queries to test non-monotonic rendering of dataflows.  With this work, we change the rendering of `SELECT` queries, which shifts what they test.  To not lose coverage, we present the following alternatives:
* *Feature flag*: We add a feature flag that determines whether we render `SELECTS` as monotonic or not.  This is easy to implement, but harder to manage.
* *Duplicate & update tests*: We duplicate tests such that each `SELECT` is converted into a `SELECT` and an index plus a select.  This is more implementation work.

We prefer the second option because it'll make the meaning of `SELECTS` clear without accumulating a maintainability burden.  `SELECT` queries *should* use the monotonic optimization, and not be used to test non-monotonic behavior.

## Explain

Our explain infrastructure uses a separate pipeline from rendering dataflows.  We need to make sure that they stay in sync, or better, that they use the same pipeline.

# Reference explanation
[reference-explanation]: #reference-explanation

<!--
Focus on the implementation of the feature.
This is the technical part of the design.

- Is it reasonably clear how the feature is implemented?
- What dependencies does the feature have and introduce?
- Focus on corner cases.
- How can we test the feature and protect against regressions?
-->

We change monotonicity information from a boolean value to the following enumeration:
```rust
enum Monotonicity {
    None,
    Physical,
    Logical
}
```

1. When creating a plan for `SELECT` queries, we upgrade the monotonicity information of sources to *logical monotonicity*, unless they're physically monotonic, in which case they're not changed.
2. The optimizer uses this information to derive the monotonicity information for plans, and ensures that all operators can expect data fulfilling their monotonicity requirements.
3. Rendering removes defensive consolidate operators.

# Rollout
[rollout]: #rollout

<!--
Describe what steps are necessary to enable this feature for users.
How do we validate that the feature performs as expected? What monitoring and observability does it require?
-->

To enable this feature in production we should take the following steps:
1. Implement the feature, but guard it with a flag.
2. Adjust tests based on what's discussed above.
3. Enable the feature in staging environments.
4. Enable the feature in production environments.
5. Remove the feature flag.

## Testing and observability
[testing-and-observability]: #testing-and-observability

<!--
Testability and explainability are top-tier design concerns!
Describe how you will test and roll out the implementation.
When the deliverable is a refactoring, the existing tests may be sufficient.
When the deliverable is a new feature, new tests are imperative.

Describe what metrics can be used to monitor and observe the feature.
What information do we need to expose internally, and what information is interesting to the user?
How do we expose the information?

Basic guidelines:

* Nearly every feature requires either Rust unit tests, sqllogictest tests, or testdrive tests.
* Features that interact with Kubernetes additionally need a cloudtest test.
* Features that interact with external systems additionally should be tested manually in a staging environment.
* Features or changes to performance-critical parts of the system should be load tested.
-->

## Lifecycle
[lifecycle]: #lifecycle

<!--
If the design is risky or has the potential to be destabilizing, you should plan to roll the implementation out behind a feature flag.
List all feature flags, their behavior and when it is safe to change their value.
Describe the [lifecycle of the feature](https://www.notion.so/Feature-lifecycle-2fb13301803b4b7e9ba0868238bd4cfb).
Will it start as an alpha feature behind a feature flag?
What level of testing will be required to promote to beta?
To stable?
-->

# Drawbacks
[drawbacks]: #drawbacks

<!--
Why should we *not* do this?
-->

The feature adds complexity to the optimizer and tightens the contract between the optimizer and rendering.  This imposes some risk to surface undefined behavior, which could in the worst case lead to crashes or incorrect data.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

<!--
- Why is the design the best to solve the problem?
- What other designs have been considered, and what were the reasons to not pick any other?
- What is the impact of not implementing this design?
-->

# Unresolved questions
[unresolved-questions]: #unresolved-questions

<!--
- What questions need to be resolved to finalize the design?
- What questions will need to be resolved during the implementation of the design?
- What questions does this design raise that we should address separately?
-->

It is not clear to me where in the optimization pipeline and within the IRs we want to encode the monotonicity information and who is responsible to filfill operator's requirements.  At the moment, converting a MIR plan to LIR is where we decide what operator implementations to pick, but this seem to be too late to optimize specifically for monotonicity requirements.

# Future work
[future-work]: #future-work

<!--
Describe what work should follow from this design, which new aspects it enables, and how it might affect individual parts of Materialize.
Think in larger terms.
This section can also serve as a place to dump ideas that are related but not part of the design.

If you can't think of any, please note this down.
-->

In the future, we can use a similar approach to insert defensive consolidate operators in the plan to ensure compact data, specifically after operators that do not guarantee this.  Here, we also need to have a tigther interface between the optimizer and rendering.
