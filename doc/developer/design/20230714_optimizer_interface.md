- Feature name: Unified optimizer API
- Associated: (Insert list of associated epics, issues, or PRs)

# Summary
[summary]: #summary

The adapter triggers the MIR optimizer by calling
`optimize_dataflow`---which it does by building `DataflowDesc`s and
`*Oracle`s and passing these along. This results in redundant
or---worse still---_near_ redundant code in various parts of the
adapter code (namely, `adapter/src/coord/sequencer/inner.rs`).

This design doc proposes an API cleanup that will define a clear
contract between code in the adapter and the optimizer in compute.

# Motivation
[motivation]: #motivation

A clear interface is a maintenance boon. Additionally, clarifying the
interface will make it easier for us to address issues with `EXPLAIN`
drift, where the code for running queries and the code for
`EXPLAIN`ing what a query talk to the optimizer in slightly different
ways (and so may see slightly different results!).

# Explanation
[explanation]: #explanation

A clean API will consist of an `Optimizer` struct with just a few
methods: enough to initialize and configure the optimizer for each
use and run it on MIR, possibly capturing output in an optimizer trace.

An `Optimizer` will need to carry several bits of configuration:

  - Is it really running a query, or just `EXPLAIN`ing?
  - Is it a one-shot query or a longer lived view?
  - What are the indices? (`IndexOracle`)
  - What are the input relation statistics? (`StatisticsOracle`)
  - Presumably, others: "If you have a procedure with ten parameters, you probably missed some." --[Alan Perlis](http://www.cs.yale.edu/homes/perlis-alan/quotes.html)

# Reference explanation
[reference-explanation]: #reference-explanation

There are several calls to `optimize_dataflow` and related functions
outside of the `mz-transform` crate. Ideally, each of these uses will
turn into a use of an `Optimizer`.

 - `src/adapter/src/coord/dataflows.rs` and
   `src/adapter/src/coord/sequencer/inner.rs`

   The main use of the optimizer, for `CREATE VIEW`, `SELECT`, and
   `EXPLAIN`.

 - `src/transform/tests/test_runner.rs`

    Some testing of the optimizer.

# Rollout
[rollout]: #rollout

A single PR should suffice. The hard part is getting the interface just right!

## Testing and observability
[testing-and-observability]: #testing-and-observability

Fixing existing tests to work with the new optimizer and the existing
test suites will prevent regressions.

The new API may allow us to observe new things about the optimizer,
e.g., making it easier to measure (and report in Prometheus) "total
time in the optimizer". We could fold this new metric in to the PR or make it a
follow-up PR.

## Lifecycle
[lifecycle]: #lifecycle

Again: a single PR should suffice. But that PR amounts to a contract
between adapter and compute, so we should make sure we get it right:
bad API choices will result in pain later.

# Drawbacks
[drawbacks]: #drawbacks

This refactoring work doesn't offer concrete dividends right now---it's paying down technical debt.

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

The optimizer is a core notion in the database engine, and it should
be representable/nameable as an object.

An alternative design might compose optimizers out of parts---such a
design would be apt if we had many different optimizer scenarios with
vastly different needs. As it stands, we seem to have two optimizer
scenarios: long-lived views and one-shot queries.

Another alternative is to do nothing. We have been doing okay with
core optimizer code scattered about, and nothing urgently cries out
for fixing. That said, a clear interface will clean things up and---if
we design the API correctly---help prevent `EXPLAIN` drift.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

- What is the exact interface we want?

- What should we do about caching, if anything? Is there one
  `Optimizer` per `Coordinator` invocation? What can we keep between
  runs?

# Future work
[future-work]: #future-work

The new API will also allow us to have a clearer notion of "optimizer
version", which could be a useful tool in migration. (Alexander
suggests doing A/B migration testing: when we are considering a change
to the optimizer, we can optimizer all queries with both---identifying
and anonymously logging production queries that will behave
differently in the new optimizer.)
