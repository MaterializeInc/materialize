- Feature name: Monotonic One-Shot `SELECT`s
- Associated:
  MaterializeInc/materialize#14442 (epic),
  MaterializeInc/materialize#18546 (MVP PR),
  MaterializeInc/materialize#18089 (required issue),
  MaterializeInc/materialize#18734 (required issue),
  MaterializeInc/materialize#17358 (required issue),

# Summary
[summary]: #summary

With PR [#18546](MaterializeInc/materialize#18546), it became possible to enable a feature flag to allow for execution of one-shot `SELECT` queries with plans utilizing, whenever possible, monotonic operators instead of their non-monotonic variants. This feature has a potentially high payoff by reducing query processing time and memory demands of one-shot `SELECT`s that are not supported by indexes or materialized views. However, a few issues remain to be addressed before we can stabilize the feature, the main of which are: (a) Extending the syntax of `EXPLAIN` to produce one-shot as well as indexed / materialized view plans ([#18089](MaterializeInc/materialize#18089)); (b) Managing the impact in our tests, to keep coverage high ([#18734](MaterializeInc/materialize#18734)); (c) Refining the planning of monotonic one-shot `SELECT`s to avoid consolidation when possible ([#18732](MaterializeInc/materialize#118732)) or to otherwise ameliorate its execution ([#18890](MaterializeInc/materialize#18890)). Point (c) can be seen as the remaining work to completely exploit the almost monotonicity of one-shot `SELECT`s ([#17358](MaterializeInc/materialize#17358)).

# Motivation
[motivation]: #motivation

Interactive queries that cannot exploit a pre-existing index or materialized view are by default executed in Materialize with the same plans that would be produced for incremental view maintenance, wherein a temporary dataflow is set up to service a one-shot `SELECT` query. As a consequence, these plans employ potentially expensive operators, e.g., for top-k and `MIN`/`MAX` aggregations, which allocate significant state due to the construction of arrangement/reduction hierarchies.

One-shot `SELECT` queries could be accelerated by use of monotonic operators, exploiting the observation that single-time dataflows operate on logically monotonic input. The latter can be performed safely by selectively adding consolidation to monotonic plan variants, when necessary, to coerce logical into physical monotonicity. In many cases, the resulting queries can require less memory to process as well as execute in much less query processing time.

An MVP version of this approach was introduced in PR [#18546](MaterializeInc/materialize#18546). In an [initial evaluation](https://docs.google.com/spreadsheets/d/1xiGX6gq8JWFgPXc3fOIeQMO8QtWTMtini4M0xtjomG4/edit#gid=0), it was shown that speedups are achieved in a variety of queries. In some queries, speedups can reach up to an order of magnitude. In only one out of 10 queries in the initial evaluation was a slowdown observed, and only when that query was executed with parallelism (for details of why, see issue [#18890](MaterializeInc/materialize#18890)).

The MVP PR gated the execution of monotonic one-shot `SELECT`s behind the feature flag `enable_monotonic_oneshot_selects`, turned off by default. The goal of this design is to outline the necessary steps to stabilize this feature and retire the feature flag.

# Concepts and Challanges
[explanation]: #explanation

This section explains relevant concepts for monotonic one-shot `SELECT`s. We focus here on important background and challenges to stabilization, while the [Reference Explanation](#reference-explanation) section introduces proposed solution approaches to the challenges identified.

## Logical vs. Physical Monotonicity

Sources can classified into monotonic, i.e., present no retractions to Materialize, or non-monotonic. A monotonic source exhibits a property that we refer to as _physical monotonicity_, namely that the stream of updates produced by the source is retraction-free. One could also consider a relaxed property, which we term _logical monotonicity_. In logical monotonicity, the result of applying a `consolidate_named` operator to the stream is retraction-free, i.e., the stream may contain both additions and retractions for the same row in the same timestamp, but the consolidated view of the stream is retraction-free at every timestamp.

## One-Shot `SELECT`s: Almost Monotonic

When planning a one-shot `SELECT`, a `DataflowDescription` is created. A `DataflowDescription` is given both `as_of` and `until` frontiers. The interpretation of these frontiers is that when reading data from a source, a dataflow will fast-forward updates less than the `as_of` to the `as_of` frontier so as to construct a logical snapshot of the input. Then, further updates available in the input will be streamed continuously; however, any updates that are not less than the `until` will be discarded. When the coordinator creates a peek plan, the `until` value of a `DataflowDescription` will be set to the empty frontier by default; however, when an `as_of` value is provided, which is particularly the case for a one-shot `SELECT`, `until` will correspondingly contain a single-coordinate timestamp set to `as_of + 1`. This behavior ensures that the input streams produced, e.g., by `persist_source`, will comply with logical, but not physical, monotonicity.

## MVP of the Feature

The MVP PR [#18546](MaterializeInc/materialize#18546) exploited the fact that one-shot `SELECT`s are almost monotonic, i.e., already exhibit logical monotonicity, to improve their plans. Each `Plan` LIR operator that has a monotonic variant, namely hierarchical reduction and top-k, is given the possibility to convert a non-monotonic operator `Plan` into a monotonic one with forced consolidation, represented by a `must_consolidate` flag. The rendering of these LIR operators then respects the `must_consolidate` flag by either introducing a `consolidate_named` or not on the key-value input obtained after computing group keys and relevant values.

## Relationship to Monotonicity Analysis in MIR and `EXPLAIN`

The `MonotonicFlag` transform analyzes the preservation of monotonicity in an `MirRelationExpr` and sets the corresponding monotonicity flags of, importantly, `Reduce` and `TopK` nodes. The transform expects as input the IDs of the monotonic sources and indexes in the `MirRelationExpr`. In turn, a source is deemed monotonic by a `DataflowBuilder` if it is physically monotonic, and an index is deemed monotonic if it is created on a monotonic view.

Since monotonicity analysis is unchanged in MIR, in the case of one-shot `SELECT`s, lowering to LIR first considers only source monotonicity as derived by `MonotonicFlag` for operator selection. Importantly, during lowering, it is thus safe to set the `must_consolidate` flag of monotonic LIR `Plan` operators to `false`, since the `MonotonicFlag` analysis asserted that the input stream at that point in the plan respects physical monotonicity. Subsequently, a plan refinement pass in LIR is performed where the operator selection is revisited if the dataflow operates on a single time. In this case, only logical monotonicity can be asserted, and thus it is necessary to set `must_consolidate` to `true` whenever an LIR `Plan` operator is converted to its monotonic variant.

Given the mechanic above, consider the output produced by `EXPLAIN OPTIMIZED PLAN` vs. `EXPLAIN PHYSICAL PLAN`. Firstly, we observe that the output in either case currently does not consider that the plan was produced as a one-shot `SELECT`. Rather, the output reflects the planning as if the `SELECT` statement would be created as an indexed or materialized view. When changing this behavior to differentiate between one-shot `SELECT`s and other uses, we note first that `EXPLAIN OPTIMIZED PLAN` should produce the same output, since plan refinement for one-shot `SELECT`s only occurs at LIR level. However, the output of `EXPLAIN PHYSICAL PLAN` should differ for one-shot `SELECT`s where monotonic operator selection can take place.

## Relationship to CI Testing

In the MVP PR [#18546](MaterializeInc/materialize#18546), the LIR plan refinement step to employ monotonic operators is gated behind a feature flag called `enable_monotonic_oneshot_selects`. By default, the feature flag is off and thus one-shot `SELECT`s are planned as if they were going to be executed in indexed or materialized views. However, stabilization of the feature implies that we would like to enable the feature by defaul and eventually even retire the feature flag. In this case, the plans for one-shot `SELECTs` will be specialized.

Currently, we maintain, but also reuse `sqllogictest` corpora. The reused tests come from other DBMS where the distinction between one-shot `SELECT` planning and planning for indexed or materialized views may not be relevant. Many of these tests thus issue directly one-shot `SELECT`s, and we assume that these tests exercise enough of our planning and execution for indexed or materialized views. Thus, enabling the feature brings about the risk that we will no longer extensively test plans for indexed or materialized views if the tests only employ one-shot `SELECT`s.

## Consolidation: Not for Free

When physical monotonicity originating from monotonic sources is recognized in MIR, the lowering to LIR sets the `must_consolidate` flag of relevant `Plan` nodes to `false`. Since `consolidate_name` operators will reduce the input size by aggregating updates to the same rows, this reduction comes at the cost of potentially significant memory allocation in the form of an arrangement, proportional in size to the number of distinct rows. So recognizing physical monotonicity and skipping consolidation can result in saving potentially significant memory as well as avoiding unnecessary overhead. In single-time dataflows, one example where operator output becomes physically monotonic is for `Plan::Reduce`. Reductions never emit retractions in a single-time context. Thus, if their output is given as input to a monotonic LIR `Plan` operator, then the corresponding `must_consolidate` flag could be set to `false`. The LIR plan refinement step for single-time dataflows needs to be evolved to recorgnize such situations.

# Reference explanation
[reference-explanation]: #reference-explanation

In the following, we consider the main outstanding issues to stabilize monotonic one-shot `SELECT`s and propose strategies to address each in turn.

## Revisiting the Output of `EXPLAIN` ([#18089](MaterializeInc/materialize#18089))

A monotonic one-shot `SELECT` has a physical plan that differs from the one for the same `SELECT` in an indexed or materialized view. We therefore propose that syntax be provided to enable `EXPLAIN`, and in particular `EXPLAIN PHYSICAL PLAN`, to either produce one-shot `SELECT` plans or plans for other optimization paths.

Issue ([#18089](MaterializeInc/materialize#18089)) captures the discussion and design of the syntax for `EXPLAIN` to reflect different optimization paths. We defer the proposal of a concrete syntax to the design originating from that issue.

Given an adequate syntax, the optimization path for one-shot `SELECT`s in `EXPLAIN` should then include a setting of both `as_of` and `until` prior to invoking `finalize_dataflow`. It is critical that `until` be set to `as_of + 1`, assuming both attributes refer to a single-timestamp, one-dimensional frontier. This proper setting of `as_of` and `until` was already implemented in [#19223](MaterializeInc/materialize#19223).

## Running Tests in CI with Both Monotonic and Non-Monotonic Plan Variants

Reusing `sqllogictest` corpora allows us to get a high coverage of SQL constructs. So it is ideal that we be able to continue reusing corpora designed for other DBMS with low effort, even after the feature flag `enable_monotonic_oneshot_selects` is retired. At the same time, other DBMS may not introduce the same differentiation between one-shot `SELECT` plans and plans for indexed or materialized views as in Materialize. Therefore, their corpora would start to fundamentallly lack coverage of the plans in Materialize that are specialized to view maintenance, once the feature of monotonic one-shot `SELECT`s is enabled.

We propose to introduce a mode in our `sqllogictest` runner wherein a `Query` directive is run in two different ways. Firstly, the query is executed as a normal one-shot `SELECT`, as done now in `run_query`. Secondly, we issue a `CREATE VIEW` for the query SQL followed by a `CREATE DEFAULT INDEX` on the view. The query is then executed again as a `SELECT * FROM v`, where `v` is the view name provided. The outputs of the two executions are compared against the reference output.

Automatically created view names above should be generated so as to avoid clashes with view names defined in tests. A simple strategy is, for example, to include a UUID in the view name string. Additonally, the special mode referred to above can be invoked by providing an additional option to the `sqllogictest` executable, e.g., `--auto_index_selects`, inspired by the existing option `auto_index_tables`.

### Alternatives

Note that the proposal above changes the semantics of our runner rather than changing the test corpora. Thus, reuse of existing corpora with low effort is achieved. However, the trade-off is reduced visibility of what all the statements actually run by a test are. Furthermore, we observe that not all tests need to be run with the option `auto_index_selects`. A subset of the tests in the corpora that are candidates to produce different monotonic vs. non-monotonic plans can be selected. The latter can be achieved, e.g., by matching patterns for `MIN`, `MAX`, `DISTINCT ON`, and `LIMIT` against the test corpora to narrow down the search for relevant tests.

Another alternative to the above proposal is to not retire the feature flag `enable_monotonic_oneshot_selects` and run selected tests from existing corpora with the flag either turned on or off. While simpler to implement, this solution goes against the notion that feature flags should eventually meet an endpoint in their lifecycle.

## Lowering the Expense of Forced Consolidation

There are two strategies that we propose to lower the expense of monotonic operators introduced with forced consolidation during single-time LIR `Plan` refinement.

### Improve `refine_single_time_dataflow` LIR-based Refinement

It is not always the case the `must_consolidate` must be set for every operator coverted to monotonic in a one-shot `SELECT`. For example, in a single-time dataflow, some operators may produce physically monotonic output regardless of whether their input was even logically monotonic, e.g., reductions. Furthermore, some operators will convert single-time, logically monotonic input into physically monotonic input, e.g., operators creating an arrangement. So if the input to a monotonic operator with forced consolidation came from a reduction, then this operator's `must_consolidate` flag could instead be toggled to `false`.

We propose to split the `refine_single_time_dataflow` LIR-based refinement into two sub-actions. The first consists of the present code, which replaces non-monotonic operators with monotonic ones where consolidation is enforced (e.g., `refine_single_time_operator_selection`). The second takes as input a collection of references for the monotonic operators that were replaced in the plan and refines the setting of the `must_consolidate` flag for these `Plan` nodes (e.g., `refine_single_time_consolidation`).

The `must_consolidate` flag refinement can be implemented as a series of iterative child traversals starting from each monotonic `Plan` node replaced in the first sub-action. For a given monotonic `Plan` node, we would like to assert whether its input is already physically monotonic or whether consolidation must be forced in the `Plan` node itself. To support this decision in a single-time context, we note that `Plan` nodes can be classified into: (a) Monotonicity markers, e.g., `Plan::Reduce` or `Plan::Join`, which are LIR nodes that require either arranged input or enforce arranged output; (b) Monotonicity keepers, e.g., `Plan::FlatMap`, which just maintain the monotonicity property of their input unchanged; or (c) Monotonicity breakers, e.g., `Plan::Negate` or `Plan::Get` on a non-arranged and non-monotonic source, which introduce negative multiplicities, thus destroying physical monotonicity.

To turn off forced consolidation, it must be true that we cannot find a monotonicity breaker prior to a monotonicity marker in any path starting at the monotonic `Plan` node towards the sources. Note that since the `Plan` nodes replaced in the first step are outside of recursive contexts, we know that there are no cycles in the subgraphs rooted at each monotonic `Plan` node, which implies that paths starting at the node must eventually meet a source operator. So the property can be checked for each monotonic `Plan` node by an iterative traversal of the children and descendants of the node where a path traversal is not further expanded at monotonicity markers and the `must_consolidate` flag can only be set to `false` if the traversal does not meet any monotonicity breaker.

The full set of single-time monotonicity markers is: `Plan::Join`, `Plan::Reduce`, `Plan::TopK`, `Plan::Threshold`, `Plan::ArrangeBy`, `Plan::Constant` iff the constant is an `EvalError` or all its rows have `Diff` values greater than zero, and `Plan::Get` iff one of the following conditions is true: (1) the correspoding `GetPlan` is either `GetPlan::PassArrangements` or `GetPlan::Arrangement`; (2) The corresponding `GetPlan` is `GetPlan::Collection`, the `id` in `Plan::Get` is a global ID for a monotonic source or for an index.

For the purposes of this proposal, we consider the full set of single-time monotonicity breakers to be: `Plan::Negate`, `Plan::Constant` iff the constant contains rows with non-positive `Diff` values, and `Plan::Get` nodes iff the corresponding `GetPlan` is `GetPlan::Collection` and the `id` in `Plan::Get` is either a local ID or a global ID for a non-monotonic source. Note that including local IDs in the set of single-time monotonicity breakers is imprecise: The local ID could in fact be produced by a plan subgraph that would result in physical monotonicity in a single-time context. We believe this conservative approximation is sufficient for most one-shot queries, and avoids the complexity of needing to perform a complete analysis of `Plan::Let` nodes to enrich local IDs with single-time physical monotonicity information.

All other `Plan` nodes or variations not covered are monotonicity keepers.

#### Alternatives

Instead of breaking down the single-time LIR-based refinement step into two sub-actions, we could try to achieve both replacement of monotonic plan nodes and correct setting of their `must_consolidate` flags in a single action. This increases the complexity of the code, as analysis of subgraphs needs to be part of applying the action recursively to a given `Plan` node. To avoid the a recursive `Plan` traversal during plan refinement, an alternative would be to integrate the consolidation analysis during MIR-to-LIR lowering itself; however, that would polute the lowering code with considerations only applicable in single-time contexts. A separate recursive traversal in LIR-based refinement would allow us to analyze the single-time physical monotonicity of `Plan::Let` nodes. However, other than code complexity, this strategy is likely to introduce some additional runtime planning cost compared with the proposal above, as it needs to traverse more of the `Plan` graph. Additionally, this strategy would duplicate logic that is implemented at MIR level in the `MonotonicFlag` analysis.

### Selectively Perform In-place Consolidation

Among the work items to stabilize monotonic one-shot `SELECT`s, this is the least controversial, since it emerges from experimental observation of the cost of forced consolidation in a subset of simple interactive queries, especifically computing `MIN`/`MAX` over an entire collection. The proposed solution is to include optional in-place consolidation with a `ConsolidateBuffer` in the implementation of `consolidate_named_if`. Subsequently, the call sites in monotonic operators can be adapted to request in-place consolidation. The change should be evaluated at a minimum with the same queries and setup used in the detection of the performance issue.

## Rollout and Lifecycle
[lifecycle]: #lifecycle

As mentioned previously, the feature is now gated behind the flag `enable_monotonic_oneshot_selects`. The proposals in this document assume that we would like to eventually retire this feature flag. Based on this observation, the following lifecycle is proposed:

* Once [#18734](MaterializeInc/materialize#18734) is tackled, we can let the feature be continuously tested in CI and nightlies during at least 2 weeks.
* Once [#18089](MaterializeInc/materialize#18089) is tackled, we can enable the flag in staging. Ideally, we should let people experiment with the feature in staging for 1-2 weeks. Note that this step can be carried out in paralell with the step above.
* We consider solutions to [#18732](MaterializeInc/materialize#18732) and [#18890](MaterializeInc/materialize#18890) as highly desirable work items that bring about performance improvements for this feature. We can consider these improvements, however, optional wrt. enablement of the feature in staging. It would be ideal to tackle at a minimum [#18890](MaterializeInc/materialize#18890) before enabling the feature in production, as we observed potential regressions in simple plans (e.g., for `MIN`/`MAX` over an entire collection) that would be ameliorated by a fix. If any of these fixes lands after the observation period for the previous two bullet points, we propose an additional 1 week of observation in staging and CI for derisking.
* Once the feature is enabled in production, we can monitor for 1-2 weeks, and then plan the retirement of the feature flag in the next release cycle.

The relatively short observation periods above are introduced based on the judgment that this feature is of medium risk. We already render monotonic operators in production at the present time when Materialize is faced with queries that operate on monotonic sources. Additionally, we have a small set of `testdrive` tests that are focused on monotonic operators. However, with this feature, usage of monotonic operators will increase substantially. So we believe that increases in test diversity ([#18734](MaterializeInc/materialize#18734)) and observability ([#18089](MaterializeInc/materialize#18089)) are pre-conditions to move forward safely.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

- Should we go for the iterative, but less precise plan refinement [proposal](#improve-refine_single_time_dataflow-lir-based-refinement) or should we just bite the bullet and perform a recursive traversal while reanalyzing monotonicity for the single-time context as outlined in one of the alternatives?
- Are we happy to adopt the [proposal](#running-tests-in-ci-with-both-monotonic-and-non-monotonic-plan-variants) to change `sqllogictest` to have an additional `--auto_index_selects` flag and add a CI workflow using that flag? Or should we follow one of the other alternatives?

# Future work
[future-work]: #future-work

If we choose to adopt the [proposal](#improve-refine_single_time_dataflow-lir-based-refinement) of an iterative, but less precise plan refinement, then we could add an issue for future work to rethink the relationship between monotonicity analysis in MIR and a potential single-time physical monotonicity analysis pass in LIR. It feels odd to have two monotonicity analysis passes that achieve slightly different objectives with somewhat similar code, but at different IR levels of abstraction.
This is why the iterative and less precise approach is proposed to make progress for now.
