# Task 1h Report: rule contract refactor

## Status: DONE

## Commit

`ec01775d08` — eqsat: rule contract returns class ids so rules can build nested nodes (Phase 1)

## What changed

### `egraph.rs`

- `MATCH_LIMIT` doc updated: now documents it as a "work-item cap" (one item = one `(class id, e-node)` snapshot), not a per-rule rewrite pair count.
- `saturate` doc updated to describe the new flow: snapshot read-only, then apply with `&mut self`.
- `saturate` body replaced: collect phase builds `Vec<(Id, SNode)>` work items (bounded by `MATCH_LIMIT`), apply phase calls `rule(self, &node)` and unions each returned `Id` directly.

### `rules.rs`

- Module doc updated to describe new contract (rules may call `eg.add(..)`).
- `Rule` type: `fn(&ScalarEGraph, &SNode) -> Vec<SNode>` → `fn(&mut ScalarEGraph, &SNode) -> Vec<Id>`.
- All 16 rules migrated per the brief's migration table:
  - `vec![eg.find(child)]` group: `and_or_single`, `not_not`, `if_true`, `if_false_or_null`, `if_same_branches`.
  - `vec![eg.add(some_snode)]` group: `const_fold`, `and_or_dedup`, `and_or_empty`, `and_or_short_circuit`, `and_or_drop_unit`, `not_binary_negate`, `null_prop_binary`, `null_prop_variadic`, `err_prop_binary`, `err_prop_variadic`, `if_err_cond`.
- Inline comments updated where they described the old contract mechanism.
- Borrow discipline: reads that produce owned values (cloned analyses, `find` returning `Id: Copy`) complete before `eg.add(..)` calls. `eg.nodes(..)` returns `Vec<SNode>` (owned), so looping then calling `eg.add` inside the loop is safe.

## Test summary

56/56 scalar tests pass, unchanged assertions. `cargo check -p mz-transform --tests` and `cargo clippy -p mz-transform --tests` both clean. `bin/fmt` produced no further changes.

## Concerns

None. Pure refactor, no behavior change, no test assertions modified.
