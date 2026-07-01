# Task final-fix report

## C1: reproduction

Before the fix, `Rel::Constant { .. } => unreachable!(...)` in `raise.rs` caused
a panic whenever the saturation engine synthesized an `Empty(r)` node.
The two active rules that produce `Empty`:
- `empty_false_filter`: `Filter[p] r => Empty(r)` when predicate is literal-false.
- `union_cancel`: `Union(a, Negate a) => Empty(a)`.

The engine encodes `Empty(r)` as `ENode::Constant { card: 0, arity }`.
Extraction picks the cheapest node, which for either rule is the new `Constant`.
`raise` then hit `unreachable!("lowering never emits Rel::Constant in M1; it bails to a leaf")`.

Adding the two new roundtrip tests (`filter_false_optimizes_to_empty`,
`union_cancel_optimizes`) confirmed the panic reproducibly before the fix —
both tests panicked at the `unreachable!` macro.

## C1: fix

File: `src/transform-egraph/src/raise.rs`

Changes:
1. Added `use mz_repr::{ReprRelationType, ReprScalarType};` import.
2. Replaced the `Rel::Constant { .. } => unreachable!(...)` arm with:
   ```rust
   Rel::Constant { arity, .. } => {
       // Saturation rules (`empty_false_filter`, `union_cancel`) synthesize
       // `Empty(r)` nodes that the engine encodes as `Constant { card: 0,
       // arity }`. Extraction can pick such a node as cheapest; raise it to
       // an empty relation of the correct arity.
       MirRelationExpr::constant(vec![], repr_type_of_arity(*arity))
   }
   ```
3. Added helper function `repr_type_of_arity(arity: usize) -> ReprRelationType`
   that builds a type with `arity` nullable `Int64` columns.

`Rel::Reduce`, `Rel::WcoJoin`, and `Rel::LetRec` remain `unreachable!` as they
genuinely never appear in the M1 egraph.

## New tests

### `src/transform-egraph/tests/roundtrip.rs`

Two tests added:

**`filter_false_optimizes_to_empty`**
Builds `base(2).filter(vec![MirScalarExpr::literal_false()])`, calls `optimize`,
asserts `out.arity() == 2`. Verifies `empty_false_filter` path does not panic.

**`union_cancel_optimizes`**
Builds `Union(base(2), Negate(base(2)))`, calls `optimize`, asserts `out.arity() == 2`.
Verifies `union_cancel` path does not panic.

### `src/transform/tests/test_transforms/eqsat.spec`

Added Case (d): `Filter (false)` over `Get t0` (arity-2 source).
After `REWRITE=1 cargo test -p mz-transform --test test_transforms`, the
expected output was written as:
```
Constant <empty>
```

## Commands run and results

```
cargo test -p mz-transform-egraph
```
Result: **5 passed; 0 failed** (3 original + 2 new tests)

```
cargo test -p mz-transform --test test_transforms
```
Result: **1 passed; 0 failed** (datadriven run_tests, including new eqsat Case (d))

```
REWRITE=1 cargo test -p mz-transform --test test_transforms
```
Rewrote eqsat.spec Case (d) output to `Constant <empty>`.

## eqsat.spec filter-false output

```
apply pipeline=eqsat
Filter (false)
  Get t0
----
Constant <empty>
```

## M1: Join comment

Added to `raise.rs` `Rel::Join` arm:
```rust
// `join_scalars` intentionally drops constant-singleton (arity-0,
// 1-row) inputs that act as join identities, so a round-trip is not
// byte-identical for such inputs but is arity- and
// semantics-preserving.
```

## M2: debug_assert comment

Added to `lib.rs` above `debug_assert_eq!`:
```rust
// Debug-only guard adequate for the offline milestone. Wiring this pass
// into the live optimizer pipeline should promote it to a hard assert!.
```

## Commit hash

`5657bd9057` — `transform-egraph: raise synthesized Empty constants; review fixes`
