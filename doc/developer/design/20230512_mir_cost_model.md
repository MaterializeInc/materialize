- Feature name: MIR cost model
- Associated:
  * https://github.com/MaterializeInc/materialize/issues/17883
  * https://github.com/MaterializeInc/materialize/issues/16511
  * https://github.com/MaterializeInc/materialize/issues/15244
  * https://github.com/MaterializeInc/materialize/issues/13140
  * https://github.com/MaterializeInc/materialize/issues/12069
  * https://github.com/MaterializeInc/materialize/issues/9157
  * https://github.com/MaterializeInc/materialize/issues/14663
  * https://github.com/MaterializeInc/materialize/issues/13046

Authors: Michael Greenberg
Created: May 5, 2023 10:25 AM
Status: Draft
Tags: A-COMPUTE, A-optimization, T-performance

# Summary
[summary]: #summary

An optimizer’s goal is to take a program and make it ‘better’ in some sense: running faster (as a batch or in terms of startup and per-update latency), in less space, more scalably. Our optimizer makes a number of heuristic choices, where the heuristic is informed by intuition and experiment.

A *cost model* tries to predict a program’s behavior given information about its inputs. Given a program and some contextual information, a cost model produces an *estimated cost* for that query. Optimizers use cost models to make better informed decisions.

Our cost model should be in terms of the dimensions that are relevant to us:

- Latency (hydration time; latency per update)
- Memory (memory footprint/implicit storage)
- Scalability (to what degree can this query be spread across workers)

Our cost model should be *executable* and *accurate:*

- Our cost model will be code that runs on actual MIR, producing a symbolic expression that represents the cost of the query along various dimensions. A user can use `EXPLAIN WITH(cost)`  to predict costs.
- Our cost model should be tested on a variety of loads to ensure that our estimates aren’t total nonsense.

# Motivation
[motivation]: #motivation

## Goals

- Reify what we learn from various benchmarks/experiments.
    - https://github.com/MaterializeInc/materialize/issues/17883
    - https://github.com/MaterializeInc/materialize/issues/16511
    - https://github.com/MaterializeInc/materialize/issues/15244
    - https://github.com/MaterializeInc/materialize/issues/13140
    - https://github.com/MaterializeInc/materialize/issues/12069
    - https://github.com/MaterializeInc/materialize/issues/9157
- Offer visibility/predicability.
    - https://github.com/MaterializeInc/materialize/issues/14663
    - https://github.com/MaterializeInc/materialize/issues/13046
- Open opportunities for principled/well-informed choices in the optimizer.

## Non-goals

- Identify all opportunities to use the cost model in the optimizer.
- Refactor the optimizer to be focused on the cost model.
- Implement dynamic re-optimization based on changes in input statistics.
- Collect all relevant statistics in storage and make them available to the cost model.

# Explanation
[explanation]: #explanation

## Proposal

A cost model is an abstract interpretation of the query: rather than running the query on relations and getting new relation out, we run the query on a symbolic description of its input relations and get a symbolic description of the result out.

The general approach can be the same as `mz_transform::Typecheck`: recursively walk the `MirRelationExpr` with some contextual information, returning a symbolic term characterizing the query.

### Context information

Traditional databases track cardinality data and statistics about various columns. We will be interested in other statistics, too. Some possible examples:

- How frequently does data arrive?
- What are the key uniqueness and foreign key constraints?
- Is the given input physically monotonic or append-only? (I.e., there are no retractions.)
- At what rate do we expect new data to arrive?
- Some kind of locality property?

### Symbolic costs

A symbolic cost is a polynomial over the features of the input relations. For example, a batch query that loops over relation `R` twice and then computes its cross product with relation `Q` will have cost `2|R| + |R|*|Q|`. A traditional database typically computes cost just in terms of overall batch query time, but our costs are multi-dimensional. In priority order:

| Dimension          | Input features                                               | Outputs                         | Output type                         |
| ------------------ | ------------------------------------------------------------ | ------------------------------- | ----------------------------------- |
| Memory             | Source cardinality, existing indices, cardinality of groups  | Arrangements and size estimates | (Vec<Arrangement>, Formula<Source>) |
| Hydration time     | Source cardinality                                           | Time estimate                   | Formula<Source>                     |
| Latency per update | Source cardinality, expected per-update delta on each source | Time estimate (per source)      | Map<Source, Formula<Source>>        |
| Scalability        | Distribution of keys (e.g., cardinality of distinct keys)    | Data parallelism estimate       | Map<Source, Vec<Column>>            |

In the output type, `Arrangement` refers to summary information about the arrangements necessary; `Formula<V>` refers to an algebraic expression with variables drawn from the type `V`.

The *memory* dimension is meant to identify particularly expensive new arrangements: for example, a count query grouping by US state code (just 50, no big deal) vs. grouping by user ID (as big as your user table).

The *hydration time* dimension is effectively a classical batch database cost model: given cardinalities on the input relations, predict how much work a given plan has to do.

The *latency per update* dimension is unique to our streaming setting. Different query plans will lead to different costs when different sources produce new data. If a calculus metaphor is helpful, you can think of the cost model here producing a partial derivative with respect to each input; alternatively, the cost model’s aggregate *latency per update* cost is the [Jacobian](https://en.wikipedia.org/wiki/Jacobian_matrix_and_determinant) of that query.

The *scalability* dimension is meant to characterize how much the work for the given query can be spread across multiple nodes; the output should identify which columns (per source) will allow for distribution.

# Reference explanation
[reference-explanation]: #reference-explanation

This hasn't been implemented yet.

# Rollout
[rollout]: #rollout

## Implementation plan
[testing-and-observability]: #implementation-plan

The current plan is to work towards a minimally viable cost model, with the following priority ordering memory > hydration > latency > scalability.

- Start with a source cardinality analysis that maps (symbolic) input cardinalities to (symbolic) output cardinalities.

  + Cardinality analysis can work on MIR or LIR, since cardinality ought to be constant under optimization.

  + Selectivity factors will need to be just made up for now.

- Use the cardinality analysis to build the memory cost model.

  + The memory cost model will work on LIR using the "lower and check cost" approach when planning joins in MIR.

- Try to validate the memory cost model on a variety of queries.

- Experiment with join ordering to see if the memory cost model will improve on our heuristics.

## Testing and observability
[testing-and-observability]: #testing-and-observability

Testing can be split into two categories:

- High-level _validation_ of the cost model in terms of a corpus of interesting/worthwhile queries.
- Low-level _correctness_ of the cost model in terms of the costs it assigns to particular small queries.

High-level validation requires running queries on meaningful amounts of data; low-level correctness checking might be able to run via `datadriven::walk` without using any data at all, just checking that certain queries are always given better costs than other queries.

Low-level correctness can live in CI, but high-level validation may be costly enough to need to live elsewhere. We should ensure that high-level validation is run with some frequency to avoid drift.

## Lifecycle
[lifecycle]: #lifecycle

This feature will only be visible as `EXPLAIN WITH(cost)`, with no real contract with users about the meaning of costs (at least for now).

# Drawbacks
[drawbacks]: #drawbacks

A bad cost model may be worse than no cost model.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

## Alternatives

Cost models all look more or less the same, though the precise input features and outputs vary. These four factors and their input features are what we came up with as sensible things to go for at first.

An alternative implementation approach would be to build a completely empirical cost model. That is, rather than writing a symbolic cost model *a priori*, we could curate a corpus of candidate queries, observe their performance on a variety of inputs, and then try to fit a cost model based on those results. Such an empirical cost model will almost surely be overfit and not particularly robust; it would also be hard to maintain.

Should the multi-dimensional cost model be separate analyses (each running in a linear pass over the plan) or as a single analysis (running one pass)? The shared dependency on source cardinality information suggests that a multi-pass approach will be logistically easier. We can try to combine passes later, though the overall time spent _calculating_ cost should be quite low.

## Unresolved questions
[unresolved-questions]: #unresolved-questions

Which loads should we test on? No cost model is perfect, but more testing with better/more realistic loads will give us some confidence that our model will help us make good decisions.

Should the cost model work on `MirRelationExpression` or LIR's `Plan`? Join planning happens at the MIR level, and join planning is a natural first client of the cost model. Even so, join planning is late enough that we could have the cost model work on LIR and plan joins by lowering the joined relations to LIR, considering their cost, and then throwing away that lowering. It's more expensive, but working on LIR means the cost model never has to guess how the query will lower.

How should multi-dimensional costs be rendered in `EXPLAIN WITH(cost)`?

How should we communicate source features to the cost model?

# Future work
[future-work]: #future-work

When we have a candidate cost model, we will want to identify opportunities to use the cost model in the optimizer and pick a good first place to use it—most likely join planning. Actually using the cost model in the optimizer will require more validation and testing—at a minimum on LDBC, but ideally on client queries—to ensure that we don’t regress performance.

We've identified the following early candidate clients for the cost model:

1. **Join ordering.** There are some trade offs here between set enumeration and more heuristic approaches---in an ideal world, we'd have an explicit "optimization budget" to determine how much of the join ordering space to explore.

2. **CTE filter pushdown.** Pushing filters all the way down can be advantageous, depending on selectivity.

3. **Reduction pushdown.** Pushing reductions down is not always advantageous.

4. **Late materialization.** Given a join on a number of tables, it may be advantageous to break the join in two: a small join on a projection to set of common keys and a larger join to collect the other fields. Whether or not late materialization is worthwhile depends on which arrangements already exist.
