# Task 1a: scalar e-graph e-class analyses

Part of Phase 1 of the scalar equality-saturation canonicalizer. This task adds
the e-class analysis framework that Phase 1's rewrite rules read. It ships NO
rewrite rules. It GATES tasks 1b/1c/1d (rules consult analyses for side
conditions).

## Goal

Add e-class analyses to `ScalarEGraph`, maintained incrementally as the graph
grows and as classes merge, so a rule can ask of any class: "could a value in
this class produce an evaluation error?" and "is this class a literal constant,
and if so which one?".

## Background: what an e-class analysis is

An e-class analysis attaches a value to every e-class, computed from the class's
e-nodes (and the analysis values of their child classes), and combined by a
`merge` (join) when two classes are unioned. The invariant: every class's
analysis value is consistent with its current node set. Because all nodes in a
class are semantically equal, the analysis is a property of the equivalence
class, not of any single node.

## Files

* Create: `src/transform/src/eqsat/scalar/analysis.rs`
* Modify: `src/transform/src/eqsat/scalar/egraph.rs` (maintain the analysis store
  across `add`, `union`, `rebuild`)
* Modify: `src/transform/src/eqsat/scalar.rs` (`pub mod analysis;`)

## Verified Phase 0 API you build on (do not re-derive)

`ScalarEGraph` in `egraph.rs`:
* `uf: Vec<Id>`, `classes: HashMap<Id, HashSet<SNode>>`, `hashcons: HashMap<SNode, Id>`. `Id = usize`.
* `find(&self, id) -> Id`, `add(&mut self, SNode) -> Id`, `union(&mut self, a, b) -> bool`, `rebuild(&mut self)`, `nodes(&self, id) -> Vec<SNode>`, `saturate(&mut self) -> usize`.
* `add` canonicalizes children, hash-conses, allocates via `new_class` on miss.
* `union` folds `rb` into `ra` (parent pointer + node-set merge); congruence restored lazily by `rebuild`.
* `rebuild` iterates to a fixpoint: recanonicalizes nodes, merges newly congruent classes, then rebuilds `hashcons`.

`SNode` in `node.rs` (variants): `Column(usize, TreatAsEqual<Option<Arc<str>>>)`, `Literal(Result<Row, EvalError>, ReprColumnType)`, `CallUnmaterializable(UnmaterializableFunc)`, `CallUnary{func: UnaryFunc, expr: Id}`, `CallBinary{func: BinaryFunc, expr1: Id, expr2: Id}`, `CallVariadic{func: VariadicFunc, exprs: Vec<Id>}`, `If{cond: Id, then: Id, els: Id}`. Helpers `children() -> Vec<Id>`, `map_children`.

## The analyses to implement

Define in `analysis.rs` a concrete per-class value:

```rust
pub struct ClassAnalysis {
    /// Conservative upper bound: could SOME evaluation of a value in this class
    /// raise an EvalError? Over-approximation is sound here because every
    /// consumer (the Phase 2 absorption gate) only ever uses it to BLOCK a
    /// rewrite, never to enable one.
    pub could_error: bool,
    /// `Some` iff this class is a literal constant. Carries the same payload as
    /// `SNode::Literal` so a rule can read the constant.
    pub literal: Option<(Result<Row, EvalError>, ReprColumnType)>,
}
```

### could_error

Mirror the AST-level recursion `MirScalarExpr::could_error` at
`src/expr/src/scalar.rs:1226` exactly, but read children's `could_error` from
their class analysis instead of recursing into sub-expressions:

* `Column(..)` => `false`
* `Literal(row, _)` => `row.is_err()`
* `CallUnmaterializable(_)` => `true`
* `CallUnary { func, expr }` => `func.could_error() || analysis(expr).could_error`
* `CallBinary { func, expr1, expr2 }` => `func.could_error() || analysis(expr1).could_error || analysis(expr2).could_error`
* `CallVariadic { func, exprs }` => `func.could_error() || exprs.any(|e| analysis(e).could_error)`
* `If { cond, then, els }` => OR of the three children's `could_error`

`UnaryFunc`/`BinaryFunc`/`VariadicFunc` each expose `fn could_error(&self) -> bool` (see `src/expr/src/scalar/func/binary.rs:49`, `macros.rs:204`).

The class-level `could_error` is the OR over the class's nodes (a class could
error if ANY of its e-nodes could). This is the conservative direction.

### literal

A class is a literal iff it contains a `Literal` node. Value = that node's
`(Result<Row, EvalError>, ReprColumnType)`. If a class somehow holds more than
one distinct literal node they are by construction proven-equal, so taking any
one is fine. (In Phase 0/1 a class holds at most one literal.)

### arity

The plan lists "arity" as a third analysis. It is ill-defined as an e-class
analysis (a class holds nodes of possibly different shapes). DO NOT invent a
class-level arity. If a later rule needs operand count it reads it from the
concrete `SNode` it is matching. Omit arity from `ClassAnalysis`. Note this
decision in a comment.

## Maintenance: where the store lives and updates

Add a field `analysis: HashMap<Id, ClassAnalysis>` to `ScalarEGraph` (keyed by
canonical class id). Maintain it so that after any public operation followed by
`rebuild`, every canonical class has a correct analysis value:

* A `make(&self, node: &SNode) -> ClassAnalysis` free function (or method) in
  `analysis.rs` computes a node's contribution from its children's current
  analyses (looked up via `&HashMap<Id, ClassAnalysis>` + `find`). The egraph
  passes a child-analysis lookup in.
* A `merge(a, b) -> ClassAnalysis` combines two class values:
  `could_error: a.could_error || b.could_error`; `literal: a.literal.or(b.literal)`.
* `add`: after interning, compute the new class's analysis from the node (its
  children already have analyses since `add` is bottom-up). On a hash-cons hit,
  no new class, no change.
* `union`: when folding `rb` into `ra`, set `analysis[ra] = merge(analysis[ra], analysis[rb])` and remove `analysis[rb]`.
* `rebuild`: after the congruence fixpoint recanonicalizes classes, recompute
  each surviving class's analysis as the `merge`-fold over its nodes'
  `make` values (so values stay consistent after structural merges). Do this in
  the same place `rebuild` reconstructs `classes`/`hashcons`. Because `rebuild`
  iterates, a single consistent recompute pass at the end is sufficient; if a
  node's child analysis is missing mid-pass, order the recompute so children are
  resolved first (a fixpoint over the recompute, or process in a dependency-safe
  order). Keep it simple and correct over clever.

Expose a read accessor: `pub fn analysis(&self, id: Id) -> &ClassAnalysis` (panic
or default for an unknown id, document which). Rules in 1b read through this.

## Constraints (binding)

* Separate scalar engine: do not touch the relational `ENode`/`egraph`.
* No `as` conversions. Use `mz_ore::cast` if a numeric cast is needed (unlikely here).
* Comments: no em-dashes, no clause-joining semicolons. Doc comment states the
  contract; put reasoning inline at the decision point.
* Do not add rewrite rules. Do not change `lower`/`raise`/`node` beyond what the
  analysis read path needs (it should need nothing in `node.rs`).
* Keep the `saturate` no-op behavior; analyses must be correct after `saturate`
  too (saturate calls rebuild).

## Tests (in `analysis.rs` under `#[cfg(test)]`, `#[mz_ore::test]`)

Build small graphs via `add`/`lower` and assert analyses. Cover at minimum:

1. `could_error` false for a column and an Ok literal.
2. `could_error` true for an Err literal.
3. `could_error` true for `CallUnmaterializable(MzNow)`.
4. `could_error` propagates up: a `CallBinary` of a non-erroring func over an
   Err-literal child is `could_error == true`; over two safe children with a
   non-erroring func is `false`; with an erroring func (`func.could_error()`)
   is `true`. Pick a known non-erroring binary func and a known erroring one
   (check `func.could_error()`; e.g. division-style funcs error).
5. `literal` is `Some` with the right payload for a literal class, `None` for a
   column or a call.
6. Merge: after `union`-ing a safe class with an erroring class and `rebuild`,
   the merged class is `could_error == true` (conservative OR survives merge).
7. Merge preserves a literal: union a literal class with an equal-by-construction
   class, `rebuild`, assert `literal` still `Some`.

## Done criteria

* `cargo check -p mz-transform --tests` clean.
* `cargo clippy -p mz-transform --tests` clean (no warnings, no new dead_code
  unless a Phase-1-reserved item is `#[allow]`'d with a TODO like `MATCH_LIMIT`).
* `cargo fmt -p mz-transform` applied.
* All new analysis tests pass.
* `eqsat::scalar` round-trip tests still pass (you changed egraph internals).

## Commit

Commit on `claude/mir-equality-optimizer-sodbej`. Message subject e.g.
`eqsat: scalar e-class analyses (could_error, literal) for Phase 1`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-1a-report.md` and return
only: status (DONE / BLOCKED / NEEDS_CONTEXT / DONE_WITH_CONCERNS), the commit
sha, a one-line test summary, and any concerns.
