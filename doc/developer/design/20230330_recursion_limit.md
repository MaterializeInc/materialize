- Feature name: (Recursion limit in WITH MUTUALLY RECURSIVE)
- Associated: (Issues: [Stabilizing WMR (#17012)](https://github.com/MaterializeInc/materialize/issues/17012), [#16800](https://github.com/MaterializeInc/materialize/issues/16800), [#18362](https://github.com/MaterializeInc/materialize/issues/18362), [#2392](https://github.com/MaterializeInc/materialize/issues/2392))

# Summary
[summary]: #summary

WITH MUTUALLY RECURSIVE (WMR) currently runs until fixpoint. It would be useful to enable the user to limit the number of iterations for, e.g., making it easier for users to debug WMR queries.

I propose implementing only a _soft limit_ for now. By _soft_, I mean that upon reaching the limit, we would stop iterating and simply consider the current state to be the final result (rather than throwing an error).
The default would be no limit.
The user can specify a limit using a SQL syntax extension at the level of WMR blocks. Therefore, each WMR block can have its own limit.

# Motivation
[motivation]: #motivation

I can imagine 3 use cases, of which the 2. seems urgent to me:
1. _Stopping divergent queries ([#16800](https://github.com/MaterializeInc/materialize/issues/16800)):_ It is very easy to accidentally write a WMR query that runs forever (actually, until OOM in most cases, but that would often take a very long time). We used to have the additional problem that Ctrl+C (or dropping a materialized view or index) didn't cancel a dataflow ([#2392](https://github.com/MaterializeInc/materialize/issues/2392)), which made for quite an unpleasant user experience, as the user had to manually DROP the entire replica. However, proper dataflow cancellation for WMR queries has been [recently implemented](https://github.com/MaterializeInc/materialize/pull/18718), and therefore this use case of recursion limits is not so important anymore.
2. _Debugging state between iterations ([#18362](https://github.com/MaterializeInc/materialize/issues/18362)):_ WMR queries are not so obvious to write, so users often need to debug their queries during development. In such cases, it can be enlightening to inspect the intermediate states between iterations. If we had an option to limit the number of iterations, the user could just run the query repeatedly with successively larger limits. Multiple people expressed their wish for this feature, e.g., [here](https://materializeinc.slack.com/archives/C02CB7L4TCG/p1678266977745819?thread_ts=1678266846.169599&cid=C02CB7L4TCG).
3. _Algorithms requiring a fixed number of iterations:_ Some algorithms are unintuitive to express as a loop running until a fixpoint (or reaching a fixpoint can't be ensured), and instead need a loop that runs a specific number of times. Note that in the above two use cases, we don't expect to run into the limit in production under normal operation, but in this use case a limit will be part of the logic of production queries.

# Explanation
[explanation]: #explanation

## What to do when we reach the limit? -- _hard limit_ and _soft limit_

When a computation reaches the limit, we could either gracefully stop and simply output the current state of the WMR variables as the final result of the WMR clause, or we could error out. Interestingly, we have conflicting requirements for the different use cases mentioned above:
- For use cases 2. and 3., we obviously need graceful stopping.
- For use case 1., I'd argue that erroring out is important. This is because in use case 1., the query hits the limit _unexpectedly_. This means that if we didn't error out, the user might not notice that the limit was hit, and just keep working with the result under the incorrect assumption that it's a fixpoint.

If we wanted to support all three use cases, then we would need two different kinds of limits to accommodate the conflicting requirements above:
a _hard limit_ and a _soft limit_ with different effects upon reaching the limit:
- For the _hard limit_, the query would error out, thus making a noise to the user.
- For the _soft limit_, we just gracefully stop and output the current state as the final result.

For now, I would propose to implement only the soft limit. [Later we can consider implementing also the hard limit.](https://github.com/MaterializeInc/materialize/issues/18832) Note that implementing the hard limit will be easy to do after having implemented the soft limit.

## Syntax

An example for the proposed syntax:
```
WITH MUTUALLY RECURSIVE (ITERATION LIMIT 100)
  cnt (i int) AS (
    SELECT 1 AS i
    UNION
    SELECT i+1 FROM cnt)
SELECT * FROM cnt;
```

Our WMR implementation supports having multiple WMR blocks inside a single query. An example is computing [strongly connected components](https://github.com/MaterializeInc/materialize/blob/9037f58c398c029924bbb59c68a3e6278a6a6f25/test/sqllogictest/with_mutually_recursive.slt#L222-L274), which can be implemented by nested WMRs. In case of multiple WMR blocks, the above syntax allows for setting different limits for each WMR block.

Having separate limits for separate WMR blocks in one query is especially important for the **compositionality** of views. Let's say that Alice writes a view that has just a single WMR. Then Bob writes another query that uses Alice's view, and has a WMR itself. The view is inlined, so Bob's query now has two WMRs. But Bob shouldn't need to care about the internals of Alice's view, and might not even know that the view has a WMR, so he thinks his WMR is the only one. And then he sets a limit for the entire query, which will unexpectedly mess up Alice's WMR that is inlined from inside the view.

The syntax follows [our SQL Design Principles](https://www.notion.so/materialize/SQL-design-principles-2e2069797f7c46b8b4c7dd10043afdcd):
- The options block is parenthesized.
- There is a space between ITERATION and LIMIT.
- We accept an optional `=` before the number.
- We use the `generate_extracted_config!` macro to process the options.

The exact keywords are not final yet, and will also depend on what the exact keywords will be for WITH MUTUALLY RECURSIVE. Some other possibilities: `RECURSION LIMIT`, `MAX RECURSION`, `MAX ITERATIONS`. An [observation](https://github.com/MaterializeInc/materialize/pull/18966#pullrequestreview-1405005051) from Nikhil was that if the main WMR keywords have the word `RECURSIVE`, then we might not want to say `ITERATION` here, because these are often opposing terms, so both being present might confuse users. An [observation](https://github.com/MaterializeInc/materialize/pull/18538#discussion_r1189689052) from Jan is that the word `LIMIT` might not be needed, because we can explain this to users as doing exactly this number of iterations, which is the same thing as stopping at either fixpoint or this number of iterations.

## Rendering

The rendering part of the design is simple and [uncontroversial](https://materializeinc.slack.com/archives/C041KPFCR98/p1679655472232159?thread_ts=1679653114.252349&cid=C041KPFCR98). We will use `branch_when` ([from Timely](https://github.com/TimelyDataflow/timely-dataflow/blob/432ef57fae761f1e4773833b0474b41f1efe7e7c/timely/src/dataflow/operators/branch.rs#L94)) to access the pointstamps containing the iteration numbers (after [the consolidation of the result of an iteration](https://github.com/MaterializeInc/materialize/blob/26f23fd7271cf92c7c8b0dc4ba4711999b45999a/src/compute/src/render/mod.rs#L643)). This will give us two streams: rows that are within the limit, and rows that are outside. The latter one we can either throw away or route into the error stream, depending on whether we want to just gracefully stop at the limit with the current state as the final result (soft limit), or error out when reaching the limit (hard limit).

## IRs

At the AST and HIR levels we can simply add a field to our WMR block representation. However, in MIR we have the problem that if we simply put one limit into each WMR block, then limit settings would not survive `NormalizeLets`. This is because `NormalizeLets` sometimes merges WMR blocks (and moves bindings around in some other ways). For example, if there are two independent WMR clauses (whose results eventually flow together into, e.g., a union or join), then `NormalizeLets` merges these two WMR blocks. In this case, setting just one limit of this merged WMR clause would be wrong.

To solve the above problem, I propose to have a per-let-binding limit in MIR and LIR (and in rendering), so that different let bindings that came from different WMR clauses with differing limits could retain their differing limits. This needs some improvements in `NormalizeLets`:
- We need to attend to the `LetRec` construction in `NormalizeLets` at the end of `digest_lets`. We need to come up with limits for each element of `bindings`. What goes into `bindings` is what `digest_lets_helper` puts into `bindings` and `worklist`. Smartening the insertion into `worklist` is easy, because that is a non-recursive `Let`, so the limit can be infinite. Smartening the insertion into `bindings` is also easy, because the binding directly comes from a `LetRec` binding, from which we could copy the limit.
- In some situations, we modify the list of bindings of an existing `LetRec` in-place:
  - The inlining doesn't need any changes, because the limit is only relevant when a back-edge goes out from a binding (the rendering puts the limit handling only on such edges), but in this case the binding can't be inlined anyway.
  - At the end of `action`, we add bindings from the result of a `harvest_non_recursive`. Again, non-recursive bindings can have an infinite limit.
  - `post_order_harvest_lets` also adds bindings from `harvest_non_recursive`.
  - `harvest_non_recursive` itself just removes bindings.

An additional difficulty is that various optimizer transforms will have fixpoint loops that simulate some aspects of a `LetRec` execution. We will need to be careful in each of these. The current (or soon) ones that I'm aware of:
- `ColumnKnowledge`: We can just lower `max_iterations` to the smallest of the limits of all the bindings, because `ColumnKnowledge` [is already prepared](https://github.com/MaterializeInc/materialize/blob/db0d891b10c5131e0cfbe4a1e5a0715a82178f31/src/transform/src/column_knowledge.rs#L137-L139) to handle an early exit that doesn't exactly simulate the fixpoint semantics of `LetRec`.
- `FoldConstants`: This one will have to precisely handle the limits of each binding.

We should make MIR EXPLAIN specially handle the case when all bindings of a `LetRec` have the same limit, and print the limit simply on the `LetRec` in this case, to avoid confusing users in simple cases.

LIR EXPLAIN should print the limit of a binding only when it is non-infinite.

# Future Work: Hard limit

As mentioned before, we could also implement a hard limit, building on the implementation of the soft limit. The main difference would be in the rendering, as already mentioned above.

Before we had proper dataflow cancellation for WMR queries, a default hard limit would have been important, but now it's not so urgent anymore. It is not even clear whether we need it at all. A default hard limit could prevent the dataflow from OOMing in some cases, but the set of scenarios where this actually happens is very narrow:
- If the dataflow operates on a small dataset (e.g., a toy dataset during the development of a query), then it will take a long time to OOM, because each iteration has a non-trivial time overhead.
- On a large dataset, the dataflow will probably OOM before reaching the limit. E.g., let's say the user expects tens of iterations, but the query diverges, and we have a default limit of 1000. In this case, the memory utilisation will be more than an order of magnitude larger than what the user expected.

If we implement a hard limit, we should probably set a finite default value, since users would hit the hard limit unexpectedly in most cases. Unfortunately, determining the default value [seems to be not obvious at all](https://materializeinc.slack.com/archives/C02PPB50ZHS/p1680178187740439?thread_ts=1680177489.365979&cid=C02PPB50ZHS).

First of all, note that a default limit that is over something like 100000 is not very useful, because then even a very simple WMR query takes tens of seconds to reach the limit with even a single worker thread, due to each iteration having a fixed overhead. (And the overhead would probably be larger for realistic WMR queries, and when running on multiple threads/machines.)

One might think that having a default limit of 10000 would be totally uncontroversial, because nobody would ever hit it with a correct query on correct input data. However, [Frank says he likes his iterative dataflows untamed](https://materializeinc.slack.com/archives/C02PPB50ZHS/p1680178259072829?thread_ts=1680177489.365979&cid=C02PPB50ZHS), and makes a good point with an example use case: "Let's say you want to do reachability / distances on a road network (which doesn't have exponential growth), where edges are roads sliced up to 100 feet." In graph queries, the number of iterations often depends on the diameter of the input graph, which will be quite high in the above example.

Several other systems are surprisingly strict about their default limits: In Snowflake and Google BigQuery, the limit is only a few hundred, and the limit can only be modified by support. I'm not sure why these systems are so strict about this, but it doesn't feel right to me. I think we should make the default easily overridable by users. ([And Frank agrees.](https://materializeinc.slack.com/archives/C02PPB50ZHS/p1680178110662319?thread_ts=1680177489.365979&cid=C02PPB50ZHS)) We could simply have the same syntax for the hard limit as for the soft limit. Also note that our recursive query support is much more general than other systems, so we shouldn't necessarily follow their path.

We might also add a feature flag for a default hard limit, so that we can change it without a release.

# Alternatives

## Where to set the option?

### System / session variable

Alternatively, we could set the limits in a system / session variable. Note that neither of these would be suitable for the 3. use case.

A system variable applies to all queries in the entire system. (Even if we make a dataflow pick up the current value only at dataflow creation, this can affect all queries if a restart suddenly happens.) Therefore, a system variable is not suitable for the 2. and 3. use cases. However, it is suitable for 1. (the hard limit).

[Most system variables can't be modified by a user on her own, but only by support.](https://materialize.com/docs/sql/show/#system-variables) However, [there is a precedent](https://materializeinc.slack.com/archives/C02FWJ94HME/p1680209826137869?thread_ts=1680209764.706779&cid=C02FWJ94HME) for a system variable that is modifiable by users. As mentioned above, we would like the hard limit to be easily modifiable by users, and therefore if we decide to put the hard limit in a system variable, we should definitely make it modifiable by users.

Session variables have the problem that a custom value will be lost across environmentd restarts, meaning that it is impossible to install a view where the limit is different from the default, and have the custom limit stick between restarts. (We could conceivably add the value to the catalog along with the query's SQL definition, but this is probably not worth the trouble.) Therefore, a session variable wouldn't be suitable for the 1. (the hard limit) and 3. use cases, because for these one has to raise the limit for a production view in a manner that survives restarts. It might be kind of ok for the 2. use case (debugging): one can temporarily change the limit, and run the query that is being debugged. However, there is still the risk that somebody forgets to change the limit back. Also note that this wouldn't allow for different limits in different WMR blocks inside the same query.

In general, it's inadvisable for system / session variables to affect query semantics in a major way, so we rejected this option.

### Query-level option

As discussed [above](#syntax), we would like to make it possible to set different limits for different WMR blocks inside the same query, which wouldn't be possible with a query-level option.

Also note that we still wouldn't avoid adding a new options clause, because the current OPTIONS clause is attached to [a Select block](https://github.com/MaterializeInc/materialize/blob/48d32193a38b7df2756988af64529f2dd263b1ef/src/sql-parser/src/ast/defs/query.rs#L219-L234) as opposed to [a Query block](https://github.com/MaterializeInc/materialize/blob/48d32193a38b7df2756988af64529f2dd263b1ef/src/sql-parser/src/ast/defs/query.rs#L33-L45).

### WHERE clause with a special, unmaterializable function

Theoretically, we could put the limit in WHERE clauses with a special, unmaterializable function, e.g., `WHERE iteration_number() < 100`. However, this would have the difficulty that `PredicatePushdown` would happily push `iteration_number() < 100` all the way to the sources, and therefore out of the recursion. We could of course change `PredicatePushdown` to have a special case for expressions involving `iteration_number()`, but the problem is that it's hard to know what other transforms might have similar problems.

Our current `UnmaterializableFunc` enum uses the term "unmaterializable" as a substitute for "depends on the read-only environmentd context (that is, they can be substituted at query optimization / compilation time). However, the proposed `iteration_number()` function goes even further, as it depends on the execution context of the rendered dataflow (so the value can only be substituted at runtime). This entails that our function evaluation logic would need to be augmented with some notion of a runtime context.

One trick would be to implement an HIR rewrite to an extra binding that counts iterations (see the [Workaround section](#workaround--manually-setting-a-limit-using-existing-sql-building-blocks)). However, currently, this would have [performance problems](#workaround-manually-setting-a-limit-using-existing-sql-building-blocks).

### Workaround: Manually setting a limit using existing SQL building blocks

[Users can manually write SQL that counts the number of iterations, and converts reaching the iteration count limit into a fixpoint.](https://github.com/MaterializeInc/materialize/issues/18362#issuecomment-1481605865)
However, it would be cumbersome for users to do this every time they want to debug a query.
Also note that, currently, this would have the problem for use case 3. that it would involve a cross join with a singleton collection, which would need a broadcast join to avoid grabbing everything onto one worker.

## Testing and observability
[testing-and-observability]: #testing-and-observability

We will add the limit setting to EXPLAIN, so that tests can verify that the limit settings are correctly picked up.

We'll have to have several tests that involve multiple WMR clauses in various relative positions (independent, sequenced, nested) to see that the limits are not messed up by optimizer transforms.

We'll need to test that the soft limit does the correct thing when reaching the limit, i.e., not erroring out but emitting the current state.

We should keep in mind that some optimizer transforms perform loops when analyzing and transforming a `LetRec`, and in these cases they should respect the limits.


# Drawbacks
[drawbacks]: #drawbacks

The bookkeeping for different limits for different WMR blocks inside the same query introduces a slight maintenance burden in some optimizer transforms (mainly in `NormalizeLets`).

# Unresolved questions
[unresolved-questions]: #unresolved-questions

What should be the keyword? This also depends on what will the final keywords be for `WITH MUTUALLY RECURSIVE` itself. (The latest suggestion was `WITH REPEATEDLY`.)

[Do we want also a hard limit later?](https://github.com/MaterializeInc/materialize/issues/18832) If yes, what should be the default value?
