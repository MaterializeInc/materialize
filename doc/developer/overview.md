
# Materialize architecture

Materialize presents as a SQL-ish system that allows users to name, manipulate, query, and produce continually changing data.
To perform these tasks Materialize is broken into several components that interact through relatively narrow interfaces.
These interfaces both provide and require guarantees, which are important for Materialize to continue to work correctly and to minimize the cognitive burden of working *within* Materialize.

```
SQL front-end   <-\   Coordinator       Dataflow
SQL front-end   <-+->  *Optimizer <-->   *Sources
SQL front-end   <-/    *Catalog          *Sinks
```

This document means to frame these components, the properties they require, and the guarantees they provide.

## SQL front end

<TBD: @benesch>

### Lowering

## Coordinator

The coordinator is the "brains" of Materialize.
It records the "metadata" of what data users can access, and the properties those data have.
It manages the execution of queries, providing instruction to the dataflow layer of what computations to perform.
It selects timestamps at which queries will execute, which is a critical component of providing the experience of consistency (here: most likely "linearizability").

### Catalog manipulation

<TBD: @benesch>

### Timestamp determination

When users interact with Materialize we mean to give them the experience of a strongly consistent database.
In particular we aim to provide the property of [linearizability](https://en.wikipedia.org/wiki/Linearizability).
The underlying computation engine, [differential dataflow](https://github.com/TimelyDataflow/differential-dataflow), can compute and then maintain correct answers to queries at various times.
One central role of the coordinator is then to determine which times to use for queries made by users, to provide the experience of collections of data that continually and consistently advance through time.

<TBD: @mjibson>

### Frontier tracking

The coordinator tracks information about the timestamp progress of data maintained in the dataflow layer.
This is primarily for each collection of data an *interval of time* `[lower, upper)` describing those times for which we 1. have valid data (beyond `lower`), and 2. have complete data (not beyond `upper`).
This information drives timestamp determination for queries that use these data: we must use one common timestamp, it *must* be large enough to be valid for all inputs (for correctness), and ideally it should be small enough that the data are complete (for promptness).

## Query optimization

Materialize translate SQL queries (through a process called "lowering") into an intermediate representation (specifically "MIR") that we are then able to optimize.
The optimizations are intended foremost to not change the "semantics" of the query (more on that in a moment).
The optimizations are otherwise intended to improve the performance, resource use, and general tractability of dataflow execution.

### Intended semantics

Each of the `MirRelationExpr` variants are data-parallel operators that act on collections of input data.
Each variant represents (or intends to represent) a deterministic function of its input data.
This function maps collections of non-negative records to an output collection and error collection.
Errors are a natural and expected result of queries with flaws; they are not meant to represent bugs in Materialize.
Error collections are accumulated across the operators of a query; operators do not act on them.

The main caveat here is that following SQL, we may arbitrarily move `MirScalarExpr` expressions throughout a query plan.
The order of predicates within a `MirRelationExpr::Filter` or the placement of erroring expressions after a `Filter` or `Join` may still result in errors.
The `MirScalarExpr::If` expression can be used to guard the evaluation of expressions and prevent errors.
This movement of expressions should not change the correct output of a computation, but may change the content of the sidecar error collection.

### Caveats

The optimization layer relies on several properties of the expressions that it optimizes.
The *types* of the expressions, including the data types themselves, non-nullability, and unique key structure, should be correct as stated: optimizations may take advantage of this information and change the semantics (or error, crash) on collections that violate them.
The input collections are presumed to not contain records that accumulate to negative totals.
The scalar expressions (`MirScalarExpr`) are presumed to be deterministic; the optimizer may change the structure of expressions to remove, reduce, or reuse apparently equivalent expressions.

## Dataflow execution

The differential dataflow engine communicates and operates on "updates" to collections of data.
These updates are structured as `(data, time, diff)` triples, where

1. `data` is usually a `Row` of data,
2. `time` is usually the timestamp type of the coordinator,
3. `diff` is usually a signed integer.

Differential dataflow creates dataflow topologies of deterministic operators, each of which responds to updates in their inputs with the corresponding updates to their outputs: how should the output *change* in order to correct it to correspond to the operators logic applied to the changed input.

These topologies are static, determined at dataflow construction time.
It is important that the operators be deterministic, as determinism is the basis for "incremental recomputation" as a function of changed inputs.
Several of the operators will hold back updates until they have received all of their updates for a given timestamp (primarily the "stateful" operators that record their input and want to await potential cancellation).

### Determinism

Operator logic should be deterministic.
For example, the `Filter` operator will use a predicate to retain or discard updates; if the predicate produces different results on the insertion than it does on the deletion, the operator no longer correctly implements the `Filter` operator.
The `Map`operator
Imagine an expression that consults the current time, or a random number generator; repeated evaluation of the expression may produce different results that do not result in
All expressions provided to the dataflow layer should correspond to deterministic expressions.

### Asynchronous / cooperative scheduling

All timely dataflow operators are [cooperatively scheduled](https://en.wikipedia.org/wiki/Cooperative_multitasking).
This means that you should never write code that "blocks", and should instead record state and yield control.
For as long as your code is blocked, that entire worker thread (a scarce resource) will be blocked as well.
You may find Rust's `async` idioms to be helpful here, but ask around if you need help.

### Arrangements

Differential dataflow maintains "arrangements", which are multiversioned indexes of collections of data.
They can be viewed as indexes of data that store a history of their indexed data, so that you can visit the data at times in the past.
Arrangements are how we share data across dataflows; many dataflows conclude by building an arrangement out of a collection of interest, and many dataflows start by importing the arrangements of collections of interest.

To reduce memory use, arrangements allow users to enable "logical compaction" which removes the ability to access the data at distant historical moments (those not beyond the "logical compaction frontier").
The more recent this logical compaction frontier the more compaction is allowed.
Arrangements are only correct for times from the logical compaction frontier onward, which explains why it is important for the coordinator to track this information for timestamp determination.

### ASOF Times

All dataflows are created ASOF a time.
This time indicates the lower bound of correct results that the dataflow will produce.
All updates at times not beyond the ASOF time will be zero, and all updates at times beyond the ASOF time will be exactly correct.
The ASOF time allows us to start a dataflow only once we are sure the data are correct, and only ever present valid data.
Most commonly, the ASOF time is used to paper over potentially disparate logical compaction in input arrangements, and bring them all to a common first time.

## Sources and Sinks

<TBD: @umanwizard>
