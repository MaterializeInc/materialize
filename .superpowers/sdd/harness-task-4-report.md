# Harness task 4: the join-order arrangement lever

## Question

On a multi-way join, is the order production `JoinImplementation` (JI) commits arrangement-suboptimal?
That is, does there exist a different, correctness-valid join order that needs strictly fewer maintained arrangements than JI's order, counted the same way for both?
If yes, eqsat's order enumeration could beat JI and the order lever is real.
If no, JI's order is already arrangement-minimal and the lever is empty.

## Method used

Brute-force ground truth (the preferred, definitive method), with two production cross-checks.

For each multi-way-join fixture I enumerate every input ordering (`n!`, with `n <= 5` so it is tractable), compute the arrangement count each order would require under the differential cascade model, and take the minimum over valid orders.
I then read JI's committed order and count from the real production pipeline (`optimize_full(.., Eqsat::Off)`, then `Join.implementation`) and compare.

### Arrangement model (grounded in `src/transform/src/join_implementation.rs`)

* Differential (left-deep). The production count for a linear differential join of `n` inputs is `(n - 2) + new_input_arrangements`.
  The `(n - 2)` intermediate arrangements are FIXED for any order (one per internal join result); see `differential::plan` setting `new_arrangements = inputs.len().saturating_sub(2) + new_input_arrangements[start]`.
  Because `(n - 2)` is order-independent, the differential order lever is entirely the set of distinct `(input, key)` input arrangements the walk induces, net of reuse from existing indexes.
  Per order I bind inputs left-deep: each non-start input is arranged by the composite of its local columns equated with the already-placed prefix, the start by the columns equating it with the rest, deduplicated by `(input, sorted_key)`, dropping keys already present in `available`.
  An input whose key against the prefix is empty (a cross join) makes the order invalid and is skipped.
* Delta query (reported as STRATEGY-lever context, not an order result).
  Each input is arranged by every distinct join key it participates in (one arrangement per edge), order-independent.

### Validations (honesty guards)

1. Model sanity: a hand-computed 3-chain gives differential min 4 and delta 4 (`join_order_model_sanity`); the test asserts the model matches.
2. Differential model vs production, on JI's own order: for every fixture where JI committed differential, I reconstruct JI's exact committed order and assert the model evaluated on that order reproduces JI's committed count exactly.
   This anchors the model to production before trusting its minimum over all orders. It passed for chain4, chain5, star_diff, cycle4, and chain4_idx.
3. Delta model vs the real planner: I run `plan_as_delta_query` and compare. They agree on chains and stars.
   On denser graphs (cycle4, chain4_idx) the real delta planner derives composite keys covering several edges with one arrangement, so the hand model over-counts; the real planner's value is authoritative and the divergence is printed, not asserted, because delta is not the order-lever verdict.
4. Order-win grounding: when the model claims a cheaper differential order, I also ask the real `plan_join_min_arrangements` (with the same indexes) what it commits.

## Fixtures and per-fixture results

All fixtures are joins of arity-2 `Get`s; the join graph is the equated columns. Counts are total maintained arrangements (differential includes the `(n - 2)` intermediate term).

| fixture | n | join graph | JI kind | JI count | min differential over orders | delta (real) | verdict |
|---|---|---|---|---|---|---|---|
| chain4 | 4 | A.#1=B.#2, B.#3=C.#4, C.#5=D.#6 (linear chain) | differential | 6 | 6 | 6 | tie (JI order minimal) |
| chain5 | 5 | consecutive chain A-B-C-D-E | differential | 8 | 8 | 8 | tie (JI order minimal) |
| star_same | 4 | center C joined to L1,L2,L3 all on C.#0 | delta | 4 | 6 | 4 | delta (strategy, not order) |
| star_diff | 4 | C to L1 on C.#0, to L2 on C.#1, to L3 on C.#0 | differential | 6 | 6 | 5 (model) | tie (JI order minimal) |
| cycle4 | 4 | A-B-C-D-A 4-cycle | differential | 6 | 6 | 7 (real) | tie (JI order minimal) |
| clique4 | 4 | all of A,B,C,D equated on one shared column | delta | 4 | 6 | 4 | delta (strategy, not order) |
| chain4_idx | 4 | chain4 with an index on C by its left join column {#0} | differential | 6 | 5 | 6 (real) | ORDER WIN (differential) |

## Findings

For every plain (index-free) join graph (chain4, chain5, star_diff, cycle4), JI's committed differential ORDER is arrangement-minimal over all join orders under the same strategy.
The minimum differential over orders equals JI's committed count.
The `(n - 2)` intermediate term is order-independent, and with no index every input must be arranged once regardless of order, so the differential order lever has no slack on these graphs.
This is a STRONG NEGATIVE for the order lever in the index-free case.

The differences observed on star_same and clique4 (JI count 4 vs differential 6) are delta-vs-differential STRATEGY differences: JI committed a delta plan there, which the differential brute force cannot match because delta avoids the `(n - 2)` intermediates.
These are not order wins; they are the strategy lever, which is out of scope for this question.

The one ORDER WIN is chain4_idx. With an index on C's left join column {#0}, JI commits a differential plan needing 6 arrangements, while a different valid left-deep order (for example start B, then C, then D, then A) keys C by exactly {#0} when it enters the cascade and reuses the index, needing only 5.
The same chain WITHOUT the index has minimum 6 (= chain4), so the saving comes entirely from an order that makes the existing index reusable.
The differential model is validated against JI on JI's own order (it reproduces 6), so the model is faithful, and the brute force finds the 5 order.

### Caveat on chain4_idx (stated plainly)

The win is in the differential STRATEGY: JI's characteristics-driven start choice does not position C for {#0}-reuse, while another valid order does, so JI's differential order is arrangement-suboptimal by 1.
However, when the real `plan_join_min_arrangements` is offered the same index it commits a DELTA plan of count 6 (it prefers delta once arrangements are cheap), so the practically deployed outcome is 6 either way on this fixture.
The order lever is therefore demonstrably real (a cheaper valid order exists and JI's differential branch misses it), but on this particular graph production sidesteps the cost through the strategy lever rather than the order lever.

## Verdict

ORDER LEVER: demonstrably real but narrow.
On index-free multi-way joins (chains, stars, cycles, cliques) JI's order is arrangement-minimal: STRONG NEGATIVE.
The only order-dependent saving appears when an existing index is reusable under one order but not under the order JI picks (chain4_idx: 5 vs 6), and even there production's strategy lever (delta) lands at the same count.
So eqsat's order enumeration could in principle beat JI's differential order only in the index-reuse regime, and the practical net benefit is bounded by production's strategy choice.
