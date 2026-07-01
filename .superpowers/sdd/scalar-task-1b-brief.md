# Task 1b: scalar rewrite-rule infrastructure + constant folding

Part of Phase 1 of the scalar equality-saturation canonicalizer. This task adds
the rule-application machinery to the saturation loop and ONE rule that proves
it end to end: constant folding. Later tasks (1c, 1d) add more rules using this
infrastructure without changing it. Keep the rule set to constant folding only.

## Goal

1. A rule abstraction: a rule inspects an e-node and its children's analyses
   read-only, and returns zero or more equivalent e-nodes to union into the same
   class. Non-destructive by construction (the original node stays).
2. Wire rule application into `ScalarEGraph::saturate` at the marked insertion
   point, bounded by `MATCH_LIMIT`, tracking whether anything changed.
3. Implement constant folding as the first rule: a call whose operands are all
   literals folds to the literal it evaluates to.

## Files

* Create: `src/transform/src/eqsat/scalar/rules.rs`
* Modify: `src/transform/src/eqsat/scalar/egraph.rs` (the `saturate` insertion
  point at the `let changed = false;` slot, currently around line 224 after the
  1a changes; re-confirm the exact line)
* Modify: `src/transform/src/eqsat/scalar.rs` (`pub mod rules;`)

## Verified API you build on (do not re-derive)

`ScalarEGraph` (egraph.rs):
* `add(&mut self, SNode) -> Id`, `union(&mut self, a, b) -> bool` (true if distinct), `find(&self, Id) -> Id`, `nodes(&self, Id) -> Vec<SNode>` (canonicalized class contents), `analysis(&self, Id) -> &ClassAnalysis`, `rebuild(&mut self)`, `saturate(&mut self) -> usize`.
* `classes` is private; iterate classes through a method you add if needed, or expose a `pub fn class_ids(&self) -> Vec<Id>` accessor (add one if absent, document it).
* Bounds constants: `MATCH_LIMIT = 1_000` (currently `#[allow(dead_code)]` -- you will read it now, so remove the allow), `MAX_ENODES`, `MAX_ITERS`.

`ClassAnalysis` (analysis.rs): `{ could_error: bool, literal: Option<(Result<Row, EvalError>, ReprColumnType)> }`. Read `analysis(child).literal` to test if a child class is a literal and get its value.

`SNode` (node.rs): `Column(usize, TreatAsEqual<Option<Arc<str>>>)`, `Literal(Result<Row, EvalError>, ReprColumnType)`, `CallUnmaterializable(UnmaterializableFunc)`, `CallUnary{func, expr: Id}`, `CallBinary{func, expr1, expr2: Id}`, `CallVariadic{func, exprs: Vec<Id>}`, `If{cond, then, els: Id}`. Helpers `children() -> Vec<Id>`, `map_children`.

`lower(&mut ScalarEGraph, &MirScalarExpr) -> Id` (lower.rs), `raise(&ScalarEGraph, Id) -> MirScalarExpr` (raise.rs).

## Rule abstraction

Define in `rules.rs`:

```rust
/// A local rewrite rule. Given a class `id` and one e-node `node` in that class,
/// returns e-nodes that are semantically equal to `node` and should be unioned
/// into `id`'s class. Read-only: a rule must not mutate the e-graph. Returning
/// an empty vec means the rule does not fire on this node.
pub type Rule = fn(eg: &ScalarEGraph, node: &SNode) -> Vec<SNode>;

/// The active rule set. Phase 1 grows this; the infrastructure does not change.
pub fn rules() -> &'static [Rule] {
    &[const_fold]
}
```

A returned `SNode` may reference existing child `Id`s (they already exist in the
graph) or be a leaf. Constant folding returns a leaf `SNode::Literal`.

Note: a rule receives the node but not the class id, because the apply step
already knows which class the node came from. If a later rule needs child
analyses, it reads them via `eg.analysis(child_id)` using the child ids in the
node. That is why `eg` is passed.

## Saturate wiring

Replace the `let changed = false;` placeholder in `saturate`. The standard
read-collect / write-apply split (do NOT mutate while iterating):

```
// Collect read-only: for every class, every node, every rule.
let mut rewrites: Vec<(Id, SNode)> = Vec::new();
'collect: for id in self.class_ids() {
    for node in self.nodes(id) {
        for rule in rules::rules() {
            for new_node in rule(self, &node) {
                rewrites.push((id, new_node));
                if rewrites.len() >= MATCH_LIMIT {
                    // Bound a single pass. The next iteration continues from a
                    // rebuilt graph, so this does not lose rewrites, it defers
                    // them.
                    break 'collect;
                }
            }
        }
    }
}
// Apply: add each new node and union it into its source class.
let mut changed = false;
for (id, new_node) in rewrites {
    let new_id = self.add(new_node);
    if self.union(id, new_id) {
        changed = true;
    }
}
```

Keep the existing `rebuild()` at the top of the loop and the `if !changed break`
at the bottom. `add` may return an existing class (hash-cons hit); `union` then
returns false and does not set `changed`, which correctly reaches fixpoint.

Re-confirm the exact placement against the current `saturate` body before
editing. Do not change the loop's bound checks or the `MAX_ENODES` break.

## Constant folding rule

```rust
/// Fold a call whose operands are all literals into the literal it evaluates to.
/// Sound because evaluation IS the meaning of the call. Non-destructive: the
/// original call node remains in the class; extraction later prefers the
/// cheaper literal.
fn const_fold(eg: &ScalarEGraph, node: &SNode) -> Vec<SNode> { ... }
```

Fire only when EVERY child class is a literal (`eg.analysis(child).literal` is
`Some`). Leaves (`Column`, `Literal`, `CallUnmaterializable`) never fold:
`Column`/`CallUnmaterializable` are not all-literal, and a `Literal` has no
children to fold.

To fold:

1. For each child id, take `eg.analysis(child).literal` -> `(Result<Row, EvalError>, ReprColumnType)`. Rebuild the child as a `MirScalarExpr::Literal(row.clone(), col_type.clone())`.
2. Assemble the parent `MirScalarExpr` from the node's func and the child literal exprs (e.g. `MirScalarExpr::CallBinary { func: func.clone(), expr1: Box::new(lit1), expr2: Box::new(lit2) }`; analogous for unary/variadic/if).
3. Evaluate and wrap, exactly as `reduce` does at `src/expr/src/scalar/reduce/binary.rs:38-39`:
   ```rust
   let temp = mz_repr::RowArena::new();
   let folded = MirScalarExpr::literal(call.eval(&[], &temp), call.typ(&[]).scalar_type);
   ```
   `call.typ(&[])` with EMPTY column types is sound here: a fully-literal
   expression has no `Column` references, so the type does not depend on the
   surrounding columns. This is the reason the scalar canonicalizer needs no
   `col_types` argument. Add a one-line comment saying so.
4. `folded` is a `MirScalarExpr::Literal(result_row, result_type)`. Destructure
   it into `SNode::Literal(result_row, result_type)` and return `vec![that]`. If
   `eval` returned `Err`, the literal carries the `Err` arm, which is correct: a
   constant subtree that always errors folds to an error literal.

Do not call `lower` (it needs `&mut`); the rule is read-only. Build the
`SNode::Literal` leaf directly from the destructured `folded`.

`eval` never panics here because `CallUnmaterializable` operands are excluded by
the all-literal guard (an unmaterializable is not a literal).

## Constraints (binding)

* Separate scalar engine: do not touch the relational `ENode`/egraph/rules.
* No `as` conversions (use `mz_ore::cast`).
* Comments: no em-dashes, no clause-joining semicolons; doc states the contract,
  reasoning inline at the decision point.
* The rule must be NON-DESTRUCTIVE: it returns nodes to union, it never removes
  the source node. The saturate apply step only adds and unions.
* Do not add rules other than `const_fold`. The boolean/null/if rules are 1c/1d.

## Tests

In `rules.rs` under `#[cfg(test)]`, `#[mz_ore::test]`. Drive through the public
`canonicalize` (lower, saturate, raise) so you test the whole loop, and assert
on the raised `MirScalarExpr`.

1. Binary fold: `canonicalize(lit(2) + lit(3))` raises to `lit(5)` (a single
   `Literal`, not a `CallBinary`). Pick a non-erroring func/value.
2. Nested fold: `canonicalize((lit(2) + lit(3)) * lit(4))` raises to `lit(20)`.
   Confirms folding composes across iterations (inner folds, then outer).
3. Error fold: a constant division by zero (or an overflowing add) raises to the
   error literal `Literal(Err(...), _)`, matching `expr.eval(&[], &RowArena)`.
4. No fold with a column: `canonicalize(col(0) + lit(1))` is unchanged (still a
   `CallBinary`; no literal produced), because not all operands are literals.
5. Differential check on a small corpus of all-literal expressions: for each,
   `canonicalize(e)` equals `MirScalarExpr::literal(e.clone().eval(&[], &RowArena), e.typ(&[]).scalar_type)`. This ties the fold to the evaluation semantics directly.
6. Round-trip regression: the Phase 0 `eqsat::scalar::tests` and the 1a
   `analysis` tests still pass (you changed `saturate`).

Also confirm folding terminates (the existing `MAX_ITERS`/`MAX_ENODES` bounds
plus hash-consing make a folded literal idempotent: re-folding a `Literal` does
not fire the rule, so the loop reaches `changed == false`).

## Done criteria

* `cargo check -p mz-transform --tests` clean.
* `cargo clippy -p mz-transform --tests` clean (no warnings; remove the
  `#[allow(dead_code)]` on `MATCH_LIMIT` now that it is read).
* `cargo fmt -p mz-transform` applied.
* All new rule tests pass; Phase 0 + 1a tests still pass.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: scalar rule infrastructure + constant folding (Phase 1)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-1b-report.md` and return
only: status, commit sha, one-line test summary, concerns.
