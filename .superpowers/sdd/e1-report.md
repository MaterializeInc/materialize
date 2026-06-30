# E-T1 Report: Wire CSE into the live path

## Changes

### raise.rs: scope-threaded raise

Refactored `raise(rel, commit_wcoj)` to delegate to `raise_inner(rel, commit_wcoj, scope: &mut BTreeMap<usize, ReprRelationType>)`.
The `Rel::Let` arm now raises the value, calls `.typ()` on the result, inserts `(id, typ)` into scope, raises the body, and removes the id from scope on exit.
The `Rel::LocalGet { get: None }` arm now looks up `scope[id]` and emits `MirRelationExpr::Get { id: Id::Local(LocalId::new(u64::cast_from(id))), typ: scope[id].clone(), access_strategy: AccessStrategy::UnknownOrLocal }`.
The `Rel::LocalGet { get: Some(g) }` arm returns `g.clone()` unchanged (existing behavior preserved).
Imports added: `std::collections::BTreeMap`, `mz_expr::{AccessStrategy, Id}`.

### cse.rs: fresh ids + closed-subtree guard

Fresh ids: added `max_local_id(rel)` walker that collects the maximum id from all `Rel::Let` and `Rel::LocalGet` nodes; CSE ids now start at `max + 1`.

Closed-subtree guard (the key bug fix): added `is_closed(rel)` which returns false if the subtree contains any `Rel::LocalGet { get: Some(_) }` or `Rel::Let` nodes.
Updated `worth_binding` to require `is_closed`: CSE only binds subtrees that do not reference lower'd locals.
Without this guard, CSE would hoist subtrees like `Project(Get l2)` OUTSIDE the scope where `l2` is defined, causing Typecheck to panic with "l2 is unbound".

### eqsat.rs: call CSE in live path

Added `let best = crate::eqsat::cse::eliminate_common_subexpressions(&best);` between extraction and raise in `optimize_inner`, with a comment explaining it subsumes RelationCSE.

## Datadriven witness

Case (j) in `eqsat.spec`: a Union of two identical `Filter(#0 = 1)(Get t0)` branches.
The extracted plan has the filter as a shared subterm.
After CSE, the output shows a `With` (Let) binding:

```
With
  cte l1 =
    Filter (#0 = 1)
      Get t0
Return
  Union
    Get l1
    Get l1
```

A `Let` binding appears as expected.

## arithmetic.slt gate result

Command: `bin/sqllogictest --optimized -- test/sqllogictest/arithmetic.slt`
Wall time: ~3:31
Pass count: 206/206
Typecheck panic: none
Non-positive multiplicity: none

## Test summary

* `cargo test -p mz-transform --lib eqsat`: 53 passed (includes new `raise_cse_let_with_placeholder_local_get` test)
* `cargo test -p mz-transform --test test_transforms`: 1 passed (datadriven, including CSE witness case (j))
* `cargo test -p mz-transform --test wcoj_decision`: 6 passed
* `cargo test -p mz-transform --test roundtrip`: 22 passed
* `cargo test -p mz-transform --test compare_real`: 1 passed
* `cargo clippy -p mz-transform`: clean
* `bin/lint`: failures only for `buf`/`trufflehog` (infra tools not installed, expected)

## Concerns

The `is_closed` guard restricts CSE to subtrees that contain no lowered-local references.
This is conservative: a shared subtree that references a lower'd local could in principle be bound within its defining scope.
However, the simpler approach (no scope-aware hoisting) is correct and sufficient for the e-graph use case: the e-graph never produces Let/LocalGet nodes itself (it bails to opaque leaves for LetRec), so the only locals in the extracted tree come from original MIR Lets, which are preserved verbatim.
The CSE still binds shared compound subterms that are "pure" (no local references), which covers the common case of a shared filtered or projected base table.
