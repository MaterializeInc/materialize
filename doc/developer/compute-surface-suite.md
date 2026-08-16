# The compute surface suite

A generated correctness suite for compute's rendering path. It draws MIR plans,
renders them on a real `clusterd`, and compares the result against independent
references. It lives in `src/clusterd-test-driver` and shares its plan generator
with the `mz-transform` fuzz targets (`src/transform/src/mirgen.rs`).

## Running it

```
# the fixed-seed regression corpus
WORKLOAD_SEED=default bin/pyactivate test/clusterd-test-driver/run-local.py

# soak: freshly drawn plans, two timely workers
WORKERS=2 WORKLOAD_SEED=12345 WORKLOAD_SOAK=250 \
    bin/pyactivate test/clusterd-test-driver/run-local.py

# every hand-written scenario, each against a fresh clusterd
SCRIPT=all bin/pyactivate test/clusterd-test-driver/run-local.py

# dump a corpus as readable JSON, for diagnosing a failure
cargo run -p mz-clusterd-test-driver --bin gen-workloads -- --out /tmp/corpus
```

Nightly runs three steps: `scripts` (the hand-written scenarios), `workloads`
(the fixed-seed corpus at one and two workers), and `soak` (new plans each run,
seeded from the build number). They are separate steps because a failure in one
would otherwise mask the others.

## What it checks

Every oracle runs at every timestamp the workload writes, against three exports
(index, materialized view, subscribe) rendered as three separate dataflows.

* **FoldConstants.** Substitute each input's actual contents into the plan as
  literals and evaluate with the optimizer's constant folder, an implementation
  independent of the renderer. The strongest oracle, and blind to `LetRec`.
* **Export invariance.** The three exports must agree. Needs no reference
  implementation, so it still works where folding does not.
* **Incremental.** The maintained collection must equal a dataflow created with
  its `as_of` already at that timestamp. The only oracle that can judge a
  recursive collection.
* **Strategy invariance.** The same workload under a pairwise matrix over the
  compute strategy dyncfgs must produce the same answer.

## Design notes

These are the decisions that are load-bearing and not obvious from the code.

### Coverage is measured, not asserted

A surface cell is a property of the *lowered* plan. The generator draws random
MIR, lowers it, asks `surface.rs` which cells came out, and keeps a candidate
only if it covers something nothing covers yet. Each kept workload records its
cells as a claim, and the runner re-derives them at run time and fails if they
differ, so a generator that stops producing a shape fails instead of quietly
testing less. `KNOWN_GAPS` lists what the corpus does not reach and why, and a
test fails if an entry names a cell that is now covered.

### An oracle that declines to answer is not a passing oracle

`FoldOutcome` distinguishes rows, an error, and "did not fold". The runner
treats an inert fold oracle as a failure, and the generator only attaches the
oracle to plans that fold. Comparisons that genuinely cannot conclude are
counted and named in the run summary rather than skipped.

### Size is a dimension, not a detail

The strategy dyncfgs select between implementations that only diverge once there
is enough data to page a batcher, fill a dictionary or split a spine. At four
rows per input the whole matrix runs eight indistinguishable times, so a few
shapes carry a `volume`: rows synthesized at setup rather than declared in the
JSON, which would otherwise be unreadable.

Those shapes are built so the size stays *upstream*. Column 0 of a synthesized
row is unique, so a join on it emits as many rows as it reads instead of squaring
them, and column 1 takes 64 values, so grouping on it leaves 64 rows to read
back. The reduce, the join and the arrangement each see a hundred thousand rows
while the peek that checks them returns 64. Retractions at that size are the
obvious next shape and are not there yet, which leaves `enable_compute_correction_v2`
still judged at small data.

### Vacuity is the failure mode this suite is most prone to

Every oracle compares a rendered result against a reference, and two empty
collections agree. A plan that computes nothing therefore passes everything
while exercising nothing, and the cell count reports its operators as covered
either way. Three mechanisms guard against it:

* Drawn values are folded into a handful of candidates, so equi-join keys and
  group keys meet. Full-width integer draws never coincide.
* `is_live` rejects a candidate that is empty at every timestamp, or whose joins
  find no partners, or whose only verdict is an error the renderer will not
  raise. `default_corpus_is_not_vacuous` pins that the corpus came out that way.
* The run reports every cell that was only ever rendered over an empty
  collection.

### A subscribe does not compare like the other exports

A subscribe is poisoned by the first error it observes and repeats that error in
every later batch, while an index and a persist sink stop reporting an error as
soon as it is retracted. Export invariance therefore holds the subscribe to the
reference *made sticky*. The protocol's `Err` variant carries no timestamps, so
when a replica batches several together, which one first errored is not
recoverable, and those comparisons are reported as inconclusive.

### Errors do not survive both evaluation paths identically

Errors travel in a dataflow's `err` collection, unioned through operators
independently of `ok`. A join with an empty input still forwards its inputs'
errors, while constant folding computes the join, gets no rows, and drops the
error with them. The reverse also happens: the folder propagates a literal error
out of a relation with no rows, where the renderer evaluates per row and raises
nothing. Materialize promises no correspondence, so a rows-versus-error
comparison is an explicit inconclusive verdict. So is a both-errored comparison
where the two spellings differ: the folder and the renderer have independent
error vocabularies for the same condition.

### Byte-consumption fidelity in the shared generator

`gen_rel`'s draw sequence determines which plan a stored fuzz corpus entry
decodes to, and release qualification carries a minimized corpus between runs.
Changing how many bytes a generator draws silently remaps every entry. New
coverage therefore goes in `shapes.rs` rather than into `gen_rel`, and anything
this suite needs that fuzzing does not (a small value domain, non-empty leaves)
is applied after the draw rather than inside it.

### Monotonic shapes need append-only input

A monotonic operator over a retracting collection is incorrect, and the oracles
would report it: the suite finding a bug in its own test data. A test enforces
that any shape declaring monotonicity declares insert-only input.

### Per-configuration ids

Everything the runner creates is offset by the configuration index. Each
configuration reconnects to drop the previous one's dataflows, and the replica's
teardown is not synchronous with the next session's commands, so reusing ids
puts a new collection and a dying one of the same name in the same window. That
showed up as a dataflow that never reported a frontier. Workload ids start at
`WORKLOAD_ID_BASE = 100_000`, above the range the `.spec` scenarios use, because
reconciliation matches dataflows to keep *by id*.

## Findings

### An error's multiplicity can go negative, and the accounting decides the answer

The suite's first product finding, from a soak run: an index peek and a
materialized-view read of the same collection at the same timestamp reported
different errors.

    Index:            Invalid data in source errors, saw retractions (12) for
                      row that does not exist: Evaluation error: division by zero
    MaterializedView: Evaluation error: division by zero

An error is emitted with the multiplicity of the row it was raised on, and
`Negate` negates its input's rows while passing its errors through
(`render.rs`), so an expression evaluated above a `Negate` raises errors with
negative multiplicity. `EXCEPT ALL` plans to `Threshold(Union(lhs,
Negate(rhs)))`, and predicate pushdown deliberately leaves a literal-error
predicate above the `Negate` so that those errors cancel against the other
branch's. That cancellation is load-bearing: it is what stops the null-extended
branch of an outer join from raising spurious errors
(database-issues#5691, whose regression test breaks if the predicate is pushed
through instead).

The cost is that errors cancel across unrelated rows, so how many rows each
branch contributes decides the outcome of

    SELECT * FROM (SELECT x FROM a EXCEPT ALL SELECT x FROM b) WHERE 1/0 > 0

* equal contributions: the errors cancel, no error, an empty result
* right side heavier: a negative remainder, reported by an index peek as
  "Invalid data in source errors" and logged at `error!`, from a plain user
  query
* left side heavier: the error

Only the reporting is fixed ("compute: stop reporting a negated error
accumulation as source corruption"), and deliberately so. Whether those errors
should cancel is not settled: Materialize has no error semantics, they are a
byproduct of `render.rs`, and STG-54 is the standing account of what still has to
be decided. This case is its 1.8.1, which leans towards errors from different
records never cancelling while noting that we rely on the cancellation
internally. The `Negate` mechanism is its own entry under "how the rendering of
various operators handle the error stream", down to needing error provenance to
resolve. `test/sqllogictest/error_semantics.slt` records all three cases, which
is what that file is for.

What the suite is entitled to call a defect, and what the fix addresses, is
narrower and independent of any of that: two read paths over one collection
disagreed at one timestamp, and a condition a plain query can reach was reported
in the storage layer's vocabulary and paged on.

### From building the suite

Everything else the work turned up was dead code, a defect in the test driver,
or a legitimate asymmetry between two evaluation paths, in the order found:

1. **`CollationPlan` was dead code** (`src/compute-types/src/plan/reduce.rs`).
   Collation is how a reduce over mixed reduction types used to be executed
   inside the renderer; `ReduceReduction` in the optimizer does that job now, and
   the type was left behind. Deleted.

2. **A mixed-reduction-type `Reduce` panics LIR lowering**, on an `assert_eq!` in
   `ReducePlan::create_from`. SQL-unreachable, because `ReduceReduction` sits
   unconditionally in the logical optimizer, so it is not a product bug. It did
   break `DataflowBuilder::finish`'s documented contract of reporting a malformed
   plan as an error, which the driver now screens for.

3. **`fold_to_multiset` conflated "folded to an error" with "did not fold"**,
   which silently removed every error-propagating plan from the oracle's reach in
   the three `mz-transform` fuzz targets. Split into `FoldOutcome`.

4. **The response pump dropped any response that arrived before a waiter.**
   `watch::Sender::send` fails when no receiver exists yet *and leaves the stored
   value unchanged*, so a frontier the replica had already reported was lost and
   the waiter blocked to timeout. Fixed with `send_replace`. It presented as
   flakiness that moved under every unrelated change, and what localized it was
   dumping raw responses. `expect_frontier` now reports the last frontier it saw.

5. **The exports were created with `as_of` already at the assertion timestamp**,
   so no dataflow ever maintained anything: each read a snapshot at the final
   timestamp and stopped. The incremental oracle compared two snapshot dataflows
   and could not have distinguished a correct incremental update from a wrong
   one, which is the property it exists to check. The exports now start at `0`
   and every timestamp is asserted.

6. **The corpus computed almost nothing.** A fifth of all drawn leaves were
   empty, and full-width integer draws meant equi-join keys never met: of six
   join workloads, four had an empty input, one was a cross product and one had a
   single row. Nothing in the suite could see this, because an empty reference
   and an empty read agree. Fixed by the value domain, the liveness filter, and
   the vacuity reporting described above.

7. **A subscribe error was attributed to every timestamp**, since the pump kept
   one error for the whole subscribe. That reported intended poisoning behaviour
   as a divergence between exports.
