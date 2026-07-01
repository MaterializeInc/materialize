# Task 1d report: scalar If-condition resolution rules

## Status

DONE

## What was implemented

Three rewrite rules added to `src/transform/src/eqsat/scalar/rules.rs`:

* `if_true`: `if(true, a, b) -> a`. Fires when the condition class carries a
  `true` literal. Mirrors `reduce_if`'s `Ok(Datum::True)` arm.
* `if_false_or_null`: `if(false, a, b) -> b` and `if(null, a, b) -> b`. Fires
  when the condition class carries a `false` or `null` literal. Mirrors
  `reduce_if`'s `Ok(Datum::False) | Ok(Datum::Null)` arm. A null condition
  takes the else branch per SQL semantics.
* `if_same_branches`: `if(c, x, x) -> x`. Fires when the then and else classes
  are canonically identical. Mirrors `reduce_if`'s `then == els` arm.

A helper `lit_bool_or_null` was added alongside the existing `lit_bool` to
distinguish all three boolean-or-null cases from the condition's literal
analysis. Error literals are excluded from all three rules, matching the brief's
DEFER directive.

All rules follow the existing `and_or_single` / `not_not` pattern: they return
`eg.nodes(child)` to union via the existing node-set mechanism, and do not
mutate the e-graph or construct nested new nodes.

## Deferred (not implemented)

* The `Err` condition arm of `reduce_if` (needs `then.typ.union(els.typ)`, which
  requires `col_types` the canonicalizer does not thread).
* The boolean-then/els If-to-AND/OR rewrites (need nested new node construction
  and col_types).

## Tests

Four new tests in the `eqsat::scalar::rules::tests` module:

* `test_if_true`: `if(true, c0, c1)` canonicalizes to `c0`. Differential over
  the 2-column `{true, false, null}^2` cube.
* `test_if_false`: `if(false, c0, c1)` canonicalizes to `c1`. Differential over
  the same cube.
* `test_if_null`: `if(null::bool, c0, c1)` canonicalizes to `c1`. Differential
  over the same cube.
* `test_if_same_branches`: `if(c0, c1, c1)` canonicalizes to `c1` with a column
  condition so const_fold cannot fire. Differential over the 2-column cube.

Each test asserts both the structural canonical form and the eval-differential.

## Checks

* `cargo check -p mz-transform --tests`: clean.
* `cargo clippy -p mz-transform --tests`: clean.
* `cargo fmt -p mz-transform`: applied.
* All 43 scalar tests pass (39 prior + 4 new).
