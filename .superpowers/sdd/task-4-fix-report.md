# Task 4 fix report: remove unsound drop_equiv_filter rule

## What was removed (cross-checked against git show 58e7764180)

### src/transform/src/eqsat/dsl.rs

Removed `Cond::Equiv { pred, rel }` and `Cond::NotSelfRef { rel }` variants (lines
237-250 in the pre-fix file).

### src/transform/src/eqsat/parser.rs

Removed the `"equiv"` and `"not_self_ref"` match arms in `parse_cond` (both
were added wholesale by commit 58e7764180).

### src/transform/src/eqsat/egraph.rs

Removed the `Cond::Equiv` and `Cond::NotSelfRef` arms (72 lines total) from
`check_conds`.

### src/transform/src/eqsat/rules/relational.rewrite

* Removed the `drop_equiv_filter` rule and its 12-line doc block.
* Restored `merge_filters` to its pre-58e7764180 form: reverted "Guard 1" label
  back to "Guard", removed the 9-line "Guard 2" comment block, and removed the
  `where not_self_ref(r)` guard line.
* Added a short NOTE comment in place of `drop_equiv_filter` explaining that the
  drop is deferred because the equivalences analysis has no nullability facts.

## What was kept

* `Cond::Unsatisfiable` and its parser/egraph arms (added in an earlier sound commit).
* The Equivalences analysis, canonicalization step, `unsatisfiable` Cond, and all
  other rules (none were touched by 58e7764180).
* The pre-existing `not_rel_empty(r)` guard on `merge_filters`.

## Case (h) handling

After removing `drop_equiv_filter`, the REWRITE run produced:

```
Join on=(#0 = #0)
  Filter (#0 = #0)
    Get t0
  Get t1
```

This is sensible: canonicalization still rewrites #2→#0 (turning Filter[#0=#2]
into Filter[#0=#0] and the join equivalence into #0=#0), and push_filter_into_join_first
pushes the filter into the first join input. The filter is retained because the
drop is now deferred.

The case (h) comment was updated to explain the new (sound) behavior: predicate
canonicalized but not dropped, with a note that dropping is deferred to the
typed/physical phase.

## Test results

* `cargo check -p mz-transform --lib`: clean.
* `cargo test -p mz-transform --lib eqsat`: 44 passed, 0 failed, no hang.
  Saturation is stable (blowup gone: the 4→566 node explosion required
  drop_equiv_filter to fire, which created self-referential classes; without
  the rule no such cycles form).
* `cargo test -p mz-transform --test test_transforms`: 1 passed, 0 failed.
* `cargo test -p mz-transform --test wcoj_decision`: 4 passed, 0 failed.
* `cargo clippy -p mz-transform`: no warnings.
* `bin/lint`: buf/protobuf failures are pre-existing environment issues (buf
  binary not installed); all Rust/Python/shell checks pass.
