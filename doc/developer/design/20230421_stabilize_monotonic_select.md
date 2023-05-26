- Feature name: Monotonic One-Shot `SELECT`s
- Associated:
  MaterializeInc/materialize#14442 (epic),
  MaterializeInc/materialize#18546 (MVP PR),
  MaterializeInc/materialize#18089 (required issue),
  MaterializeInc/materialize#18734 (required issue),
  MaterializeInc/materialize#17358 (required issue),

# Summary
[summary]: #summary

With PR MaterializeInc/materialize#18546, it became possible to enable a feature flag to allow for execution of one-shot `SELECT` queries with plans utilizing, whenever possible, monotonic operators instead of their non-monotonic variants. This feature has a potentially high payoff by reducing query processing time and memory demands of one-shot `SELECT`s that are not supported by indexes or materialized views. However, a few issues remain to be addressed before we can stabilize the feature, the main of which are: (a) Extending the syntax of `EXPLAIN` to produce one-shot as well as indexed / materialized view plans (MaterializeInc/materialize#18089); (b) Managing the impact in our tests, to keep coverage high (MaterializeInc/materialize#18734); (c) Refining the planning of monotonic one-shot `SELECT`s to avoid consolidation when possible (MaterializeInc/materialize#18732) or to otherwise ameliorate its execution (MaterializeInc/materialize#18890). Point (c) can be seen as the remaining work to completely exploit the almost monotonicity of one-shot `SELECT`s (MaterializeInc/materialize#17358).

# Motivation
[motivation]: #motivation

Interactive queries that cannot exploit a fast path are processed in Materialize with the same plans that would be produced for incremental view maintenance of the corresponding SQL statement, but with the difference that the corresponding dataflow to service such a one-shot `SELECT` query is only temporary. As a consequence, it is possible that these plans employ potentially expensive operators, e.g., for top-k and `MIN`/`MAX` aggregations, which allocate significant state due to the construction of arrangement/reduction hierarchies.

Thus, some one-shot `SELECT` queries could be accelerated by use of monotonic operators, exploiting the observation that single-time dataflows operate on logically monotonic input. The latter can be performed safely by selectively adding consolidation to monotonic plan variants, when necessary, to coerce logical into physical monotonicity. In many cases, the resulting queries can require less memory to process as well as execute in much less query processing time.

An MVP version of this approach was introduced in PR MaterializeInc/materialize#18546. In an [initial evaluation](https://docs.google.com/spreadsheets/d/1xiGX6gq8JWFgPXc3fOIeQMO8QtWTMtini4M0xtjomG4/edit#gid=0), it was shown that speedups are achieved in a variety of queries. In some queries, speedups can reach up to an order of magnitude. In only one out of 10 queries in the initial evaluation was a slowdown observed, and only when that query was executed with parallelism (for details of why, see issue MaterializeInc/materialize#18890).

The MVP PR gated the execution of monotonic one-shot `SELECT`s behind the feature flag `enable_monotonic_oneshot_selects`, turned off by default. The goal of this design is to outline the necessary steps to stabilize this feature and retire the feature flag.

# Concepts and Challenges
[explanation]: #explanation

This section explains relevant concepts for monotonic one-shot `SELECT`s. We focus here on important background and challenges to stabilization, while the [Reference Explanation](#reference-explanation) section introduces proposed solution approaches to the challenges identified.

## Logical vs. Physical Monotonicity

Sources can classified into monotonic, i.e., present no retractions to Materialize, or non-monotonic. A monotonic source exhibits a property that we refer to as _physical monotonicity_, namely that the stream of updates produced by the source is retraction-free. One could also consider a relaxed property, which we term _logical monotonicity_. In logical monotonicity, the result of applying a `consolidate_named` operator to the stream is retraction-free, i.e., the stream may contain both additions and retractions for the same row in the same timestamp, but the consolidated view of the stream is retraction-free at every timestamp.

## One-Shot `SELECT`s: Almost Monotonic

When planning a one-shot `SELECT`, a `DataflowDescription` is created. A `DataflowDescription` contains both `as_of` and `until` frontiers. The interpretation of these frontiers is that when reading data from a source, a dataflow will fast-forward updates less than the `as_of` to the `as_of` frontier so as to construct a logical snapshot of the input. Then, further updates available in the input will be streamed continuously; however, any updates that are not less than the `until` will be discarded. When the coordinator creates a peek plan, an `as_of` value is provided. Should the planning not hit a fast path, then the `until` value of the `DataflowDescription` will be set to a single-coordinate timestamp conceptually equivalent to `as_of + 1`. This behavior ensures that the input streams produced, e.g., by `persist_source`, will comply with logical, but not physical, monotonicity.

## MVP of the Feature

The MVP PR MaterializeInc/materialize#18546 exploited the fact that one-shot `SELECT`s are almost monotonic, i.e., already exhibit logical monotonicity, to improve their plans. Each `Plan` LIR operator that has a monotonic variant, namely hierarchical reduction and top-k, is given the possibility to convert a non-monotonic operator `Plan` into a monotonic one with forced consolidation, represented by a `must_consolidate` flag. The rendering of these LIR operators then respects the `must_consolidate` flag by either introducing a `consolidate_named` or not on the key-value input obtained after computing group keys and relevant values. Additionally, the MVP PR exposed the separate stages of `finalize_dataflow` through `EXPLAIN OPTIMIZER TRACE`, including MIR-to-LIR lowering and plan refinements for source MFPs as well as for single-time monotonic operator selection.

## Relationship to Monotonicity Analysis in MIR and `EXPLAIN`

The `MonotonicFlag` transform analyzes the preservation of monotonicity in an `MirRelationExpr` and sets the corresponding monotonicity flags of, importantly, `Reduce` and `TopK` nodes. The transform expects as input the IDs of the monotonic sources and indexes in the `MirRelationExpr`. In turn, a source is deemed monotonic by a `DataflowBuilder` if it is physically monotonic, and an index is deemed monotonic if it is created on a monotonic view.

Since monotonicity analysis is unchanged in MIR, in the case of one-shot `SELECT`s, lowering to LIR first considers only source monotonicity as derived by `MonotonicFlag` for operator selection. Importantly, during lowering, it is thus safe to set the `must_consolidate` flag of monotonic LIR `Plan` operators to `false`, since the `MonotonicFlag` analysis asserted that the input stream at that point in the plan respects physical monotonicity. Subsequently, a plan refinement pass in LIR is performed where the operator selection is revisited if the dataflow operates on a single time. In this case, only logical monotonicity can be asserted, and thus it is necessary to set `must_consolidate` to `true` whenever an LIR `Plan` operator is converted to its monotonic variant.

Given the mechanic above, consider the output produced by `EXPLAIN OPTIMIZED PLAN` vs. `EXPLAIN PHYSICAL PLAN`. It may not be obvious to users if the output of these `EXPLAIN` statements reflects planning as if the `SELECT` statement would be created as an indexed or materialized view or alternatively executed as a one-shot `SELECT`. When changing this behavior to differentiate between one-shot `SELECT`s and other uses, we note first that `EXPLAIN OPTIMIZED PLAN` should produce the same output, since plan refinement for one-shot `SELECT`s only occurs at LIR level. However, the output of `EXPLAIN PHYSICAL PLAN` should differ for one-shot `SELECT`s where monotonic operator selection can take place. It is also important to ensure that the syntax of `EXPLAIN` be adapted to make it obvious to users which optimization path is being shown.

## Relationship to CI Testing

In the MVP PR MaterializeInc/materialize#18546, the LIR plan refinement step to employ monotonic operators is gated behind a feature flag called `enable_monotonic_oneshot_selects`. By default, the feature flag is off and thus one-shot `SELECT`s are planned as if they were going to be executed in indexed or materialized views. However, stabilization of the feature implies that we would like to enable the feature by default and eventually even retire the feature flag. In this case, the plans for one-shot `SELECTs` will be specialized.

Currently, we maintain, but also reuse `sqllogictest` corpora. The reused tests come from other DBMS where the distinction between one-shot `SELECT` planning and planning for indexed or materialized views may not be relevant. Many of these tests thus issue directly one-shot `SELECT`s, and we assume that these tests exercise enough of our planning and execution for indexed or materialized views. Thus, enabling the feature brings about the risk that we will no longer extensively test plans for indexed or materialized views if the tests only employ one-shot `SELECT`s.

## Consolidation: Not for Free

When physical monotonicity originating from monotonic sources is recognized in MIR, the lowering to LIR sets the `must_consolidate` flag of relevant `Plan` nodes to `false`. Since `consolidate_name` operators will reduce the input size by aggregating updates to the same rows, this reduction comes at the cost of potentially significant memory allocation in the form of an arrangement, proportional in size to the number of distinct rows. So recognizing physical monotonicity and skipping consolidation can result in saving potentially significant memory as well as avoiding unnecessary overhead. In single-time dataflows, one example where operator output becomes physically monotonic is for `Plan::Reduce`. Reductions never emit retractions in a single-time context. Thus, if their output is given as input to a monotonic LIR `Plan` operator, then the corresponding `must_consolidate` flag could be set to `false`. The LIR plan refinement step for single-time dataflows needs to be evolved to recorgnize such situations.

# Reference explanation
[reference-explanation]: #reference-explanation

In the following, we consider the main outstanding issues to stabilize monotonic one-shot `SELECT`s and propose strategies to address each in turn.

## Revisiting the Output of `EXPLAIN` (MaterializeInc/materialize#18089)

A monotonic one-shot `SELECT` has a physical plan that differs from the one for the same `SELECT` in an indexed or materialized view. We therefore propose that syntax be provided to enable `EXPLAIN`, and in particular `EXPLAIN PHYSICAL PLAN`, to either produce one-shot `SELECT` plans or plans for other optimization paths.

Issue MaterializeInc/materialize#18089 captures the discussion and design of the syntax for `EXPLAIN` to reflect different optimization paths. We defer the proposal of a concrete syntax to the design originating from that issue.

Given an adequate syntax, the optimization path for one-shot `SELECT`s in `EXPLAIN` should then include a setting of both `as_of` and `until` prior to invoking `finalize_dataflow`. It is critical that `until` be set to `as_of + 1`, assuming both attributes refer to a single-timestamp, one-dimensional frontier. This proper setting of `as_of` and `until` was already implemented in MaterializeInc/materialize#19223.

## Running Tests in CI with Both Monotonic and Non-Monotonic Plan Variants (MaterializeInc/materialize#18734)

Reusing `sqllogictest` corpora allows us to get a high coverage of SQL constructs. So it is ideal that we be able to continue reusing corpora designed for other DBMS with low effort, even after the feature flag `enable_monotonic_oneshot_selects` is retired. At the same time, other DBMS may not introduce the same differentiation between one-shot `SELECT` plans and plans for indexed or materialized views as in Materialize. Therefore, their corpora would start to fundamentallly lack coverage of the plans in Materialize that are specialized to view maintenance, once the feature of monotonic one-shot `SELECT`s is enabled.

We propose to introduce a mode in our `sqllogictest` runner wherein a `Query` directive is run in two different ways. Firstly, the query is executed as a normal one-shot `SELECT`, as done now in `run_query`. Secondly, we issue a `CREATE VIEW` for the query SQL followed by a `CREATE DEFAULT INDEX` on the view. The query is then executed again as a `SELECT * FROM v`, where `v` is the view name provided. The outputs of the two executions are compared against the reference output.

Automatically created view names above should be generated so as to avoid clashes with view names defined in tests. A simple strategy is, for example, to include a UUID in the view name string. Additonally, the special mode referred to above can be invoked by providing an additional option to the `sqllogictest` executable, e.g., `--auto_index_selects`, inspired by the existing option `auto_index_tables`.

There is some degree of sensitivity to the amount of time that our SLT pipelines take to run. We consider three core scenarios that need further evaluation: (1) The CI run for each PR; (2) The nightly CI run; (3) The slow SLT pipeline. For (1), we may be able to run SLTs with `--auto_index_selects` turned on, depending on the performance impact that the additional DDL and redundant `SELECT` executions might have. If an evaluation shows that the time to completion of the pipeline is too large, a subset of the tests in the corpora that are candidates to produce different monotonic vs. non-monotonic plans can be selected. The latter can be achieved, e.g., by matching patterns for `MIN`, `MAX`, `DISTINCT ON`, and `LIMIT` against the test corpora to narrow down the search for relevant tests.

For (2), one primary issue is that SQLancer / SQLsmith would be primarily run on the monotonic one-shot plan variants. We propose that this behavior be accepted for the stabilization of monotonic one-shot SELECTs, and that a follow-up issue be pursued subsequently.

For (3), it is highly likely that the total time needed to execute these tests be excessive, as the current pipeline takes 7-10 hours to run. A first strategy would be to try pattern-based selection of a subset of tests; if the time would still be too large, we could refine the subset selection by sampling. The latter can be follow a random or just a simple round-robin strategy. Here, round-robin would guarantee that every SLT would eventually get a chance to run over a given number of repetitions of the pipeline, but it requires us to save state or implement a deterministic mapping (e.g., tests to day-of-week).

### Alternatives

Note that the proposal above changes the semantics of our runner rather than changing the test corpora. Thus, reuse of existing corpora with low effort is achieved. However, the trade-off is reduced visibility of what all the statements actually run by a test are.

Another alternative to the above proposal is to not retire the feature flag `enable_monotonic_oneshot_selects` and run selected tests from existing corpora with the flag either turned on or off. While simpler to implement, this solution goes against the notion that feature flags should eventually meet an endpoint in their lifecycle.

A final alternative to be considered is a refinement of the proposal above in which we also run SQLancer / SQLsmith tests in the nightly CI pipeline by redundantly creating the SQL statements as indexed views in addition to one-shot `SELECT`s. In other words, in this alternative, we would consider running the nighltlies in this configuration also a requirement for stabilization of monotonic one-shot `SELECT`s.

## Lowering the Expense of Forced Consolidation

There are two strategies that we propose to lower the expense of monotonic operators introduced with forced consolidation during single-time LIR `Plan` refinement.

### Improve `refine_single_time_dataflow` LIR-based Refinement (MaterializeInc/materialize#18732)

It is not always the case the `must_consolidate` must be set for every operator coverted to monotonic in a one-shot `SELECT`. For example, in a single-time dataflow, some operators may produce physically monotonic output regardless of whether their input was even logically monotonic, e.g., reductions. Furthermore, some operators will convert single-time, logically monotonic input into physically monotonic input, e.g., operators creating an arrangement. So if the input to a monotonic operator with forced consolidation came from a reduction, then this operator's `must_consolidate` flag could instead be toggled to `false`.

We propose to split the `refine_single_time_dataflow` LIR-based refinement into two sub-actions. The first consists of the present code, which replaces non-monotonic operators with monotonic ones where consolidation is enforced (e.g., `refine_single_time_operator_selection`). The second performs an analysis pass over the entire `Plan` to refine the setting of `must_consolidate` in case any replacements were done in the first sub-action (e.g., `refine_single_time_consolidation`).

To support the latter refinement in a single-time context, we note that `Plan` nodes can be classified into: (a) Monotonicity enforcers, e.g., `Plan::Reduce`, which are LIR nodes that enforce arranged output; (b) Monotonicity keepers, e.g., `Plan::FlatMap`, which just maintain the monotonicity property of their input unchanged; or (c) Monotonicity breakers, e.g., `Plan::Negate` or `Plan::Get` on a non-arranged and non-monotonic source, which introduce negative multiplicities, thus destroying physical monotonicity.

To turn off forced consolidation at the root of an LIR plan, it must be true that we cannot find a monotonicity breaker after to a monotonicity enforcer in any path from the sources to the root. The analysis of this property can be modeled as an instance of abstract interpretation, which in turn can be implemented following an `Interpreter` structure similar to MaterializeInc/materialize#18932. The analysis pass will derive a `PhysicallyMonotonic` property, and will need to carry along this property annotation for bindings when evaluating an expression. All monotonic `Plan::Reduce` nodes with a `must_consolidate` flag previously set to `true` can have the flag toggled if we obtain `PhysicallyMonotonic::Yes` for their input.

The logic in the analysis above is similar to what is implemented at MIR level in the `MonotonicFlag` analysis. However, we consider only single-time contexts and a different level of abstraction, since the analysis is performed in LIR.

The full set of single-time monotonicity enforcers is: `Plan::Reduce`, `Plan::TopK`, `Plan::Threshold`, `Plan::ArrangeBy`, `Plan::Constant` iff the constant is an `EvalError` or all its rows have `Diff` values greater than zero, and `Plan::Get` iff one of the following conditions is true: (1) the correspoding `GetPlan` is either `GetPlan::PassArrangements` or `GetPlan::Arrangement`; (2) The corresponding `GetPlan` is `GetPlan::Collection`, the `id` in `Plan::Get` is a local or global ID for a monotonic source or for an index.

For monotonicity breakers, we note that any `Plan` nodes contained in the `values` of a `Plan::LetRec` expression are not in a single-time context due to iteration. This is why `refine_single_time_operator_selection` only considers the `body` of a `Plan::LetRec`, explicitly excluding traversal of `values`. So we must be careful in the analysis to consider the bindings introduced by `Plan::LetRec` to be monotonicity breakers. Additionally, monotonicity breakers comprise `Plan::Negate`, `Plan::Constant` iff the constant contains rows with non-positive `Diff` values, and `Plan::Get` nodes iff the corresponding `GetPlan` is `GetPlan::Collection` and the `id` in `Plan::Get` is either a local or global ID for a non-monotonic source.

All other `Plan` nodes or variations not covered are monotonicity keepers. Special care must be taken to keep a binding map with partial analysis results as `Plan::Let` nodes are visited.

#### Alternatives

Instead of breaking down the single-time LIR-based refinement step into two sub-actions, we could try to achieve both replacement of monotonic plan nodes and correct setting of their `must_consolidate` flags in a single action. This increases the complexity of the code, as analysis of subgraphs needs to be part of applying the action recursively to a given `Plan` node. To avoid the a recursive `Plan` traversal during plan refinement, an alternative would be to integrate the consolidation analysis during MIR-to-LIR lowering itself; however, that would polute the lowering code with considerations only applicable in single-time contexts. In the future, we may be possible to rework the present code by applying the `Interpreter` structure also during lowering.

We also considered an iterative alternative for `refine_single_time_consolidation` that could be implemented as a series of iterative child traversals starting from each monotonic `Plan` node replaced in the `refine_single_time_operator_selection` sub-action. However, that alternative has the potential for poor runtime complexity in the presence of nesting of monotonic `Plan::Reduce` nodes or if the expressions defined in `Plan::Let` nodes need to be revisited many times. These issues can be ameliorated by pruning the iterative traversal in monotonicity enforcers (`Plan::Reduce` being one of them, and thus addressing the nesting issue)  and by not expading `Plan:Let` expressions. Unfortunately, the latter results in imprecision in the analysis, wherein some opportunities to turn off consolidation would be lost.

### Improve `Exchange` in Forced Consolidation (MaterializeInc/materialize#18890)

Among the work items to stabilize monotonic one-shot `SELECT`s, this is the least controversial, since it emerges from experimental observation of the cost of forced consolidation in a subset of simple interactive queries, especifically computing `MIN`/`MAX` over an entire collection. After investigation, it was determined that the expense of forced consolidation manifested primarily in a multi-worker setting, and it was due to load imbalance among workers. PR MaterializeInc/materialize#19372 goes into more depth, determining that the problem was caused by low entropy in DD's default `FnvHasher`. The PR then fixes the problem by changing the hash function used for the `Exchange` in forced consolidation to `AHash`. While `AHash` does not provide theoretical guarantees (in contrast to twisted tabulation hashing), it strikes an adequate trade-off in ecosystem support, development agility, and empirically observed hashing quality and speed.

## Rollout and Lifecycle
[lifecycle]: #lifecycle

As mentioned previously, the feature is now gated behind the flag `enable_monotonic_oneshot_selects`. The proposals in this document assume that we would like to eventually retire this feature flag. Based on this observation, the following lifecycle is proposed:

* Once MaterializeInc/materialize#18734 is tackled, we can let the feature be continuously tested in CI and nightlies during at least 2 weeks.
* Once MaterializeInc/materialize#18089 is tackled, we can enable the flag in staging. Ideally, we should let people experiment with the feature in staging for 1-2 weeks. Note that this step can be carried out in paralell with the step above.
* We consider solutions to MaterializeInc/materialize#18732 and MaterializeInc/materialize#18890 as highly desirable work items that bring about performance improvements for this feature. We can consider these improvements, however, optional wrt. enablement of the feature in staging. It would be ideal to tackle at a minimum MaterializeInc/materialize#18890 before enabling the feature in production, as we observed potential regressions in simple plans (e.g., for `MIN`/`MAX` over an entire collection) that would be ameliorated by a fix. If any of these fixes lands after the observation period for the previous two bullet points, we propose an additional 1 week of observation in staging and CI for derisking.
* Once the feature is enabled in production, we can monitor for 1-2 weeks, and then plan the retirement of the feature flag in the next release cycle.

The relatively short observation periods above are introduced based on the judgment that this feature is of medium risk. We already render monotonic operators in production at the present time when Materialize is faced with queries that operate on monotonic sources. Additionally, we have a small set of `testdrive` tests that are focused on monotonic operators. However, with this feature, usage of monotonic operators will increase substantially. So we believe that increases in test diversity (MaterializeInc/materialize#18734) and observability (MaterializeInc/materialize#18089) are pre-conditions to move forward safely.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

- Should we go for the iterative, but less precise plan refinement [proposal](#improve-refine_single_time_dataflow-lir-based-refinement) or should we just bite the bullet and perform a recursive traversal while reanalyzing monotonicity for the single-time context as outlined in one of the alternatives?
- Are we happy to adopt the [proposal](#running-tests-in-ci-with-both-monotonic-and-non-monotonic-plan-variants) to change `sqllogictest` to have an additional `--auto_index_selects` flag and add a CI workflow using that flag? Or should we follow one of the other alternatives?

# Future work
[future-work]: #future-work

If we choose to adopt the [proposal](#improve-refine_single_time_dataflow-lir-based-refinement) of an iterative, but less precise plan refinement, then we could add an issue for future work to rethink the relationship between monotonicity analysis in MIR and a potential single-time physical monotonicity analysis pass in LIR. It feels odd to have two monotonicity analysis passes that achieve slightly different objectives with somewhat similar code, but at different IR levels of abstraction.
This is why the iterative and less precise approach is proposed to make progress for now.
