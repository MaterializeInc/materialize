- Feature name: Stabilize `WITH MUTUALLY RECURSIVE`
- Associated:
  MaterializeInc/materialize#11176 (first iteration),
  MaterializeInc/materialize#17012 (current epic).

# Summary
[Summary]: #summary

The `WITH MUTUALLY RECURSIVE` (WMR) implementation that was shipped as part of the previous design doc[^wmr] has some loose ends.
The aim of the design doc is to identify those and come up with a rollout plan for WMR to production environments.

# Motivation
[Motivation]: #motivation

Stabilizing support for `WITH MUTUALLY RECURSIVE` is one of the technical bets that we are making in FY2024.
Adding first-class support for recursive queries will:

1. Exercise one of the key strengths of the underlying runtime (support for incremental maintenance of iterative dataflows).
1. Enable new use cases across different domains, most likely based on various forms of graph analysis (for example for social networks, fraud detection, software security).
1. Enable tractable encodings of high-level concepts such as session windows in terms of SQL (see MaterializeInc/materialize#8698).

We should fill in the implementation gaps that were intentionally left as TODOs during MaterializeInc/materialize#11176 and bring the feature into a shape where it can be gradually rolled out behind a feature toggle and ultimately stabilized.

# Explanation
[Explanation]: #explanation
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

The original WMR design doc[^wmr] and the technical article[^wmr_article] on our website provide a good explanation about the syntax and semantics of WMR.
From the implementation point of view, adding WMR support requires extensions across every stage of the query lifecycle.
The original design doc laid out a plan for adding support in each of the following layers.

1. SQL parsing (&check; in MaterializeInc/materialize#16509)
2. SQL name resolution (&check; in MaterializeInc/materialize#16509)
3. SQL planning (&check; in MaterializeInc/materialize#16509)
4. HIR generalization (&check; in MaterializeInc/materialize#16561)
5. Lowering (&check; in MaterializeInc/materialize#16561)
6. MIR generalization (&check; in MaterializeInc/materialize#16561)
7. MIR optimization corrections (focus of this document)
8. LIR generalization (&check; in MaterializeInc/materialize#16656, MaterializeInc/materialize#17705)
9. Rendering (&check; in MaterializeInc/materialize#16787, TODO: MaterializeInc/materialize#16800)

The outstanding tracks of work can be summarized as follows:

1. Complete TODOs that fall under the "MIR optimization corrections".
1. Enumerate and resolve TODOs in the other stages.
1. Design and execute on a testing plan for the feature.
1. Design and execute on a rollout plan for the feature.

Progress on all tracks can happen concurrently.

# Reference explanation
[Reference explanation]: #reference-explanation
<!--
Focus on the implementation of the feature.
This is the technical part of the design.

- Is it reasonably clear how the feature is implemented?
- What dependencies does the feature have and introduce?
- Focus on corner cases.
- How can we test the feature and protect against regressions?
-->

Mostly, the actual work revolves around enumerating and addressing unimplemented code blocks where the corresponding part of our compilation pipeline needs to handle WMR fragments.

## `hir_to_mir` lowering

We might have to check again the changes from MaterializeInc/materialize#16561.
Mostly, I am concerned is what happens in the presence of:

1. Nested LIR blocks.
2. Different outer contexts when referencing the same recursive symbol.

## `mir` transformations

MaterializeInc/materialize#16561 extended `MirRelationExpr` with a new `LetRec` variant.
The `Transform` trait was extended with a `recursion_safe` method which returns `true` iff the `Transform` implementation is claiming to operate correctly in the presence of `LetRec` nodes.
At the moment, the optimizer skips `Transform` implementations that are not `recursion_safe`.

The following table summarizes work that needs to be done for each transform.
Work estimates for each transform are given in relative t-shirt sizes.
The `?` suffix denotes uncertainty of absolute size 1 (`M?` can be `L` or `S`).
The proposed implementation plan is summarized after the table.

transformation              | estimate | solution | tracked in
----------------------------|----------|----------|-------------------------------------------------
`canonicalize_mfp`          | &check;  | trivial  | MaterializeInc/materialize#18123
`column_knowledge`          | &check;  | advanced | MaterializeInc/materialize#18161
`demand`                    | &check;  | basic    | MaterializeInc/materialize#18162
`filter_fusion`             | &check;  | trivial  | MaterializeInc/materialize#18123 (depends on type inference)
`fixpoint`                  | &check;  | trivial  | MaterializeInc/materialize#16561
`flatmap_to_map`            | &check;  | trivial  | MaterializeInc/materialize#18123
`fold_constants`            | &check;  | basic    | MaterializeInc/materialize#18163
`fuse_and_collapse`         | &check;  | trivial  | MaterializeInc/materialize#18164
`fusion`                    | &check;  | trivial  | MaterializeInc/materialize#18123
`join_fusion`               | &check;  | trivial  | MaterializeInc/materialize#18123
`join_implementation`       | &check;  | advanced | MaterializeInc/materialize#16561
`literal_constraints`       | &check;  | trivial  | MaterializeInc/materialize#18123
`literal_lifting`           | &check;  | basic    | MaterializeInc/materialize#18165
`map_fusion`                | &check;  | trivial  | MaterializeInc/materialize#18123
`monotonic_flag`            | &check;  | advanced | MaterializeInc/materialize#18472
`negate_fusion`             | &check;  | trivial  | MaterializeInc/materialize#18123
`non_null_requirements`     | &check;  | basic    | MaterializeInc/materialize#18166
`non_nullable`              | &check;  | trivial  | MaterializeInc/materialize#18123 (somewhat restricted)
`normalize_ops`             | &check;  | trivial  | MaterializeInc/materialize#18123
`normalize_lets`            | &check;  | advanced | MaterializeInc/materialize#16665
`predicate_pushdown`        | &check;  | basic    | MaterializeInc/materialize#18167
`project_fusion`            | &check;  | trivial  | MaterializeInc/materialize#18123
`projection_extraction`     | &check;  | trivial  | MaterializeInc/materialize#18123
`projection_lifting`        | &check;  | basic    | MaterializeInc/materialize#18168
`projection_pushdown`       | &check;  | basic    | MaterializeInc/materialize#18169 (depends on MaterializeInc/materialize#18553)
`reduce_elision`            | &check;  | basic    | MaterializeInc/materialize#18170 (depends on MaterializeInc/materialize#18553)
`reduce_fusion`             | &check;  | trivial  | MaterializeInc/materialize#18123
`reduction_pushdown`        | &check;  | basic    | MaterializeInc/materialize#18171 (depends on MaterializeInc/materialize#18553)
`redundant_join`            | &check;  | basic    | MaterializeInc/materialize#18172
`relation_cse`              | &check;  | basic    | MaterializeInc/materialize#18173
`semijoin_idempotence`      | &check;  | basic    | MaterializeInc/materialize#18174 (depends on MaterializeInc/materialize#18553)
`threshold_elision`         | &check;  | basic    | MaterializeInc/materialize#18175
`topk_elision`              | &check;  | trivial  | MaterializeInc/materialize#18123
`topk_fusion`               | &check;  | trivial  | MaterializeInc/materialize#18123
`union`                     | &check;  | trivial  | MaterializeInc/materialize#18123
`union_branch_cancellation` | &check;  | trivial  | MaterializeInc/materialize#18176
`union_negate`              | &check;  | trivial  | MaterializeInc/materialize#18123

We have 36 `Transform` implementations, of which 3 are currently marked as `recursion_safe`.
All but 16 can be trivially marked as recursion safe (done in MaterializeInc/materialize#18123) because they represent local transformations that don't depend on transformation context that depends on the `Let` bindings that are currently in scope.

From the remaining 16, based on an initial analysis it seems that:
- 4 are relatively straight-forward to fix (size estimate `M?`),
- 12 maintain `Let`-based context and need case-by-case reasoning (marked with `L?`).

For most non-trivial transforms, we have multiple solutions at our disposal:

1. A _basic_ solution which only applies the transform to bindings that are not actually recursive and treats recursive bindings as an optimization barrier.
   Transforms using this we have are marked with _solution = basic_ in the table above.
2. An _advanced_ solution which uses abstract interpretation based on lattice theory to propagate information through `LetRec` nodes.
   Transforms using this solution are marked with _solution = advanced_ in the table above.
   For the _basic_ transforms the _advanced_ solution is sketched in the linked issue in case we want to improve them as future work.
3. An _advanced_ solution that we will get without changes to the actual `Transform` code if we implement MaterializeInc/materialize#18174.
   Those are marked with _solution = basic_ and the corresponding issue as depending on MaterializeInc/materialize#18553.

## Generalization of LIR rendering

This should be mostly handled by MaterializeInc/materialize#17705.
There is also an additional feature request for an optional max recursion limit in MaterializeInc/materialize#16800 which will affect how plans are rendered.
We might have to add more tests for that (see [Testing and observability](#testing-and-observability)).

# Rollout
[Rollout]: #rollout
<!--
Describe what steps are necessary to enable this feature for users.
How do we validate that the feature performs as expected? What monitoring and observability does it require?
-->
The WMR feature is currently only enabled in `--unsafe-mode`.
As part of the enclosing epic, we will introduce a dedicated `with_mutually_recursive` feature flag.
The feature will be first made available on all `staging` environments (alpha testers) and then rolled out to `production` environments for "public preview".
The following aspects need special attention:
1. Queries producing wrong results (discussed in [Testing and observability](testing-and-observability)).
2. Queries that do not terminate.
   This is tricky because some queries might be divergent because of a bad query definition (a user error) instead of an optimization or interpretation bug (a system error).
   A related issue to track this is MaterializeInc/materialize#16800. The plan is to have maximum iteration limit as a safeguard. Edit: We won't have a default limit, because we now have proper dataflow cancellation between iterations. However, the user can set `ERROR AT RECURSION LIMIT 1000`, if she wants an additional guardrail.

To validate (1), I suggest to:

- Ask [the DevEx team](https://github.com/orgs/MaterializeInc/teams/devex) to deploy WMR materialized views on their canary environments.
- Use the internal observability metrics as early adopters for WMR.

Validating (2) is [an open question](#unresolved-questions).

## Testing and observability
[Testing and observability]: #testing-and-observability
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

We plan to build up confidence in the updated query optimization pipeline by adding new tests and revisiting existing tests.
Test scenarios can be categorized along two dimensions:

### By type

1. **Unit tests.**
We aim to have one unit test per transform.
We can invest time proportional to the complexity of the transform to ensure that each transform is correct.

1. **Integration tests.**
We will add test scenarios inspired by the use cases of our prospects as end-to-end `*.slt` tests.
We will also add at least one long-running `mzcompose` test runs [as part of our nightly tests](https://buildkite.com/materialize/nightlies) and is used when qualifying future releases.
As those tests will include expected results, it will be great if we have a reference external iteration driver for the semantics proposed in the original design doc[^wmr].
That way we can cross-check the results of the reference and the internal implementation of WMR support and ensure that both produce equal results.
We can implement such driver in Python and integrate it in our `mzcompose` tests.

1. **End-to-end experiments.**
We will perform a bunch of end-to-end experiments (available in [the `letrec-bench` GitHub repository](https://github.com/MaterializeInc/letrec-bench)) to get a sense of the resource consumption and stability of the feature under a sustained load of concurrently occurring updates.

### By test scenario
[By test scenario]: #by-test-scenario

1. **Synthetic tests.** (punted as follow-up work)
The best synthetic use case that we have identified so far seems to be the LDBC social network benchmark[^ldbc].
With the scope of the dedicated epic (MaterializeInc/materialize#17591), we will select a subset of the work in order to bootstrap a testing environment that consists of
  (a) LDBC data + updates, and
  (b) several of the recursive queries defined by the benchmark.
We can use the choke-point characterization of each query to figure out the most representative subset.

1. **Use-case driven.**
It is unclear how useful these will be as load tests, as we don't have the resources to write realistic data generators to replicate the domain of specific customers.
However, we can try to map some of the customer use cases to the LDBC dataset.
Also, might need to be careful about the specific problems we try to solve and use to showcase WMR.
The power of incremental recursive computation only shines if the data dependency that is carried across iterations is somewhat bounded.
Intuitively, this means an algorithm that does something like dynamic programming or reachability on graphs with some locality properties might handle small deltas in its input better than something like gradient descent.

1. **Sourced from elsewhere.** (punted as follow-up work)
We can check what tests Postgres has for their `WITH RECURSIVE` support.

## Lifecycle
[Lifecycle]: #lifecycle
<!--
If the design is risky or has the potential to be destabilizing, you should plan to roll the implementation out behind a feature flag.
List all feature flags, their behavior and when it is safe to change their value.
Describe the [lifecycle of the feature](https://www.notion.so/Feature-lifecycle-2fb13301803b4b7e9ba0868238bd4cfb).
Will it start as an alpha feature behind a feature flag?
What level of testing will be required to promote to beta?
To stable?
-->

We plan to roll the implementation behind a `with_mutually_recursive` feature flag.
It should be OK to turn the feature flag on for individual environments at all times.
It should be OK to turn the feature flag off for customers as long as they don't have catalog objects that use the feature.

The feature will go through an `alpha`/`beta`/`stable` lifecycle.
Once we have reworked WMR to be behind a dedicated feature flag, we will enable this flag for all `staging` environments, thereby entering the alpha stage.
The feature will be promoted to `beta` (public preview) when the following conditions are met:

1. We have enabled sufficient MIR transformations to not feel horrible about the optimization opportunities that are lost in a WMR context.
2. We have sufficient test coverage to feel good about potential regressions to existing workloads.

In the `beta` testing phase, we will work with selected customers / prospects, who have previously explicitly voiced their interest in the feature and have a clear use case to demonstrate its value.
We will remain in close contact with those customers and treat their use cases as proof-of-concept in order to iron out potential operational and stability issues.

Once we have established the above and have build up confidence about the optimizer and runtime stability of recursive dataflows running in production, we will open the feature to everybody.
This needs to be coordinated with the GTM team, as most probably we will want to advertise this accordingly.

# Drawbacks
[Drawbacks]: #drawbacks
<!--
Why should we *not* do this?
-->

I think the main question here is

> Why should we *not* do this *at the moment*?

I can think of two reasons:

- Working on this with high degree of confidence in minimizing disruptions for existing customers will be much easier if we have some basic infrastructure to test for plan regressions in our `production` environment.
- The developer resources in the compute team are scarce. There might be epics that bring more value to a wider range of customers.

I think that we can re-evaluate these points as part of an "end of epic" retrospective.

# Conclusion and alternatives
[Conclusion and alternatives]: #conclusion-and-alternatives
<!--
- Why is the design the best to solve the problem?
- What other designs have been considered, and what were the reasons to not pick any other?
- What is the impact of not implementing this design?
-->

- For the design for WMR see the original design doc[^wmr].
- For the implementation and rollout plan laid out here, we believe that this is the safest possible path to evolve the optimizer pipeline given the tools and infrastructure.

# Unresolved questions
[Unresolved questions]: #unresolved-questions
<!--
- What questions need to be resolved to finalize the design?
- What questions will need to be resolved during the implementation of the design?
- What questions does this design raise that we should address separately?
-->

- Do we want to focus / target use cases where `WITH MUTUALLY RECURSIVE` is known to play well with incremental computations?
  See discussion of use-case driven tests in [By test scenario].
- Can we measure / observe the amount of work / data diff that a specific change to the input introduces?
  See discussion of use-case driven tests in [By test scenario].
  Tracked in MaterializeInc/materialize#18022.

# Future work
[Future work]: #future-work
<!--
Describe what work should follow from this design, which new aspects it enables, and how it might affect individual parts of Materialize.
Think in larger terms.
This section can also serve as a place to dump ideas that are related but not part of the design.

If you can't think of any, please note this down.
-->

We are still lacking operational observability (tracked in MaterializeInc/materialize#18022).

At the very least, we can export a Prometheus metric that tracks the number of indexed or materialized views that have recursive CTEs.

Once we have anonymized query logging, we can get some deper insights which would be useful for product analytics.

---

Within the scope of MaterializeInc/materialize#17012 we only provided the _basic case_ for most non-trivial transforms.
Issues marked in [the above table](#mir-transformations) with _solution = basic_ represent opportunities for improvement.

---

UI/UX improvements:

- In "linear chains" mode the `EXPLAIN` output of plans that have recursive queries does not work.
  We will need to revisit this if we ever decide to make this the default or we have people that use it on a daily basis.
  Tracked in MaterializeInc/materialize#19012.
- Similarly, the graph visualizer for dataflows that have iterative scopes might need to be fixed.
- As we gain insights how people use the feature, we might want to follow-up with more focused "guidance docs" that go in depth of some common considerations and pitfalls.
  Currently, this is partially covered by [the final two sections of the reference docs](https://materialize.com/docs/sql/recursive-ctes/#examples), but this might not be sufficient to cover everything in the long run.
  Tracked in MaterializeInc/materialize#19334.

---

Due to time constraints benchmarking of WMR based on LDBC has been punted in favor of a more limited benchmark available in [the `letrec-bench` GitHub repository](https://github.com/MaterializeInc/letrec-bench).
The tracking epic for this is MaterializeInc/materialize#17591.

---

Improve query planning by [implementing the TODO from the `plan_ctes` function](https://github.com/MaterializeInc/materialize/blob/dcd02a44a4355d9b6841d609e0097cd50b5bbdd3/src/sql/src/plan/query.rs#L1207-L1223).

This should be done only after investigating the impacts of having an extra `Map` and `Project` on our optimization potential.

# Appendix: Internal Use Cases

The transitive closure of `mz_internal.mz_object_dependencies` might be of interest to [@umanwizard](https://github.com/umanwizard) for MaterializeInc/materialize#17836.

```sql
with mutually recursive
  base(src text, tgt text) as(
    select object_id, referenced_object_id from mz_internal.mz_object_dependencies
  ),
  reach(src text, tgt text) as (
    select * from base
    union
    select r1.src, r2.tgt from reach r1 join reach r2 on r1.tgt = r2.src
  )
select * from reach;
```

---

Session windows can be defined in an easier way (see MaterializeInc/materialize#8698).
[@sploiselle](https://github.com/sploiselle) was kind enough to add a PR for a prototype of that function in MaterializeInc/materialize#18330.

---

One of our cluster `mzcompose` tests already uses `WITH MUTUALLY RECURSIVE` (see MaterializeInc/materialize#18295).

---

[@parkerhendo](https://github.com/parkerhendo) wants to answer the following question ([slack](https://materializeinc.slack.com/archives/C02CB7L4TCG/p1679594768833129)):

> What sources rely on this particular connection?

Using data from `mz_sources` and `mz_object_dependencies`.

[^wmr]: Original [`WITH MUTUALLY RECURSIVE`](20221204_with_mutually_recursive.md) design doc
[^wmr_article]: [Recursion in Materialize](https://materialize.com/blog/recursion-in-materialize/) blog post
[^ldbc]: [LDBC Social Network Benchmark (LDBC-SNB)](https://ldbcouncil.org/benchmarks/snb/)
