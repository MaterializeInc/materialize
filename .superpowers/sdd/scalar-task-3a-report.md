# Task 3a Report: AND/OR associativity flattening rule

## Status

DONE.

## Commit

SHA `f7cfac08de`, subject: `eqsat: scalar AND/OR associativity flattening rule (Phase 3)`.

## Implementation

Added `flatten_assoc` to `src/transform/src/eqsat/scalar/rules.rs` (registered before `factor_and_or`
in `rules()`). The rule:

1. Matches `CallVariadic { func, exprs }` where `func.is_associative()`.
2. For each operand, scans its e-class nodes for a same-func variadic to splice.
3. Guards against circular self-referential splicing (see below).
4. Returns `vec![]` when nothing was spliced; otherwise `eg.add(flat_node)`.

## Circular-reference guard (non-obvious, important)

`and_or_single` collapses `OR(c0)` into c0's class. After that, c0's class contains OR
nodes whose children's canonical ids are the SAME class as c0 itself. Without a guard,
`flatten_assoc` on `OR(c0, c0)` would find `OR(c0, c0)` in c0's class and splice,
producing `OR(c0 x 4)`, then `OR(c0 x 16)`, then `OR(c0 x 256)`, growing each node
by the square of its operand count per iteration — causing test timeouts before the
MAX_ENODES budget kicks in.

The fix: skip any inner node whose canonicalized children include the current operand's
canonical class (`inner_canons.contains(&canon)`). This precisely blocks the self-referential
case without suppressing legitimate flattening (where the nested call's children are in
distinct classes from the nested call itself).

## Tests (new total: 76 passed, 199 skipped)

Four new tests added:

- `test_flatten_assoc_firing_and`: AND(c0, AND(c1, c2)) → flat AND(c0, c1, c2) with eval-equiv.
- `test_flatten_assoc_firing_or`: OR(c0, OR(c1, c2)) → flat OR(c0, c1, c2) with eval-equiv.
- `test_flatten_assoc_deep_nesting`: AND(c0, AND(c1, AND(c2, c3))) → fully flat AND(c0,c1,c2,c3).
- `test_flatten_assoc_error_operand_preservation`: nested AND with (1/c1=5) erroring operand,
  differential over {true,false,null} x {0,1,2} confirms error preserved exactly.
- `test_flatten_assoc_noop`: flat AND(c0,c1) unchanged; AND(c0, OR(c1,c2)) does not cross-splice.

All 76 prior tests pass including `test_canonicalize_eval_differential`.

## Concerns

None. The circular-reference guard is the key insight; without it tests timeout from
exponential node growth. With it, all tests pass in under 120ms total.

## Report path

This file: `.superpowers/sdd/scalar-task-3a-report.md`
