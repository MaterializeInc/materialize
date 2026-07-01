# Scalar eqsat canonicalizer: Phase 0 report

Phase 0 ships the contained scalar e-graph skeleton with a faithful lower/raise bridge and no rewrite rules.
Commit `09b1728df2` on branch `claude/mir-equality-optimizer-sodbej`.

## Module layout

New module under `src/transform/src/eqsat/scalar/`, declared via `pub mod scalar;` in `src/transform/src/eqsat.rs`.
The repo lint forbids `mod.rs`, so the module root is `eqsat/scalar.rs` (mirroring `eqsat.rs` + `eqsat/`), with submodules in `eqsat/scalar/`.

* `src/transform/src/eqsat/scalar.rs` — module root.
  Declares `egraph`, `lower`, `node`, `raise`.
  Public `canonicalize(&MirScalarExpr) -> MirScalarExpr` Phase-0 stub that does `raise(saturate(lower(e)))`, identity with zero rules.
  Holds the round-trip test module.
* `src/transform/src/eqsat/scalar/node.rs` — `SNode` enum.
* `src/transform/src/eqsat/scalar/egraph.rs` — `Id`, `ScalarEGraph`, union-find, congruence closure, saturation scaffold.
* `src/transform/src/eqsat/scalar/lower.rs` — `lower(&mut ScalarEGraph, &MirScalarExpr) -> Id`.
* `src/transform/src/eqsat/scalar/raise.rs` — `raise(&ScalarEGraph, Id) -> MirScalarExpr`.

## SNode mapping and the type fidelity decisions

`SNode` mirrors `MirScalarExpr`, with operator children replaced by `Id` and leaves carrying full payload.

The plan's spec wrote `Column(usize)` and `Literal(Row, ColumnType)`, but the real `mz_expr` types carry more, and dropping it would break the identity round-trip.
Two deliberate deviations, both required for a faithful bridge:

* `Column(usize, TreatAsEqual<Option<Arc<str>>>)`.
  The real `MirScalarExpr::Column` carries a column name.
  `TreatAsEqual` gives the name trivial `Eq`/`Hash`/`Ord`, so two references to the same column index intern to one e-node regardless of name, exactly as `MirScalarExpr` already treats it.
  Carrying the payload lets raise reconstruct the original column.
* `Literal(Result<Row, EvalError>, ReprColumnType)`.
  The real `MirScalarExpr::Literal` is `Result<Row, EvalError>` with a `ReprColumnType` (not bare `Row`/`ColumnType`).
  The `Err` arm is an evaluation error that the literal produces unconditionally.
  Keeping the whole `Result` and the real `ReprColumnType` round-trips error literals.

The remaining variants map structurally:
`CallUnmaterializable(UnmaterializableFunc)` (leaf), `CallUnary { func: UnaryFunc, expr: Id }`, `CallBinary { func, expr1, expr2 }`, `CallVariadic { func, exprs: Vec<Id> }`, `If { cond, then, els }`.
Functions stay as payload.
`SNode` derives `Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord` (Ord for deterministic extraction tie-breaks, same rationale as the relational `ENode`).
All component types already derive these because `MirScalarExpr` itself does.
`SNode` also has `map_children` and `children` helpers used by canonicalization and extraction.

## E-graph design

`ScalarEGraph` duplicates the relational engine's contained core (`crate::eqsat::egraph`) on purpose: the relational matcher and arrangement-count cost model must never see scalar nodes, and a small duplicated core is lower risk than generalizing the live engine.
Shared-core extraction is deferred until scalar is a proven second user.

* `uf: Vec<Id>` union-find with path-following `find`.
* `classes: HashMap<Id, HashSet<SNode>>` e-class contents.
* `hashcons: HashMap<SNode, Id>` interning table.
* `add(node) -> Id` canonicalizes children, hash-conses, allocates a new class on miss.
* `union(a, b) -> bool` folds one class into the other (congruence restored lazily).
* `rebuild()` iterates to a fixpoint, recanonicalizing nodes and merging newly congruent classes, then rebuilds the hash-cons table.
  This is a structural copy of the relational `rebuild`.
* `saturate() -> usize` is the bounded loop scaffold.
  It mirrors the relational bounds: `MAX_ENODES = 600` (growth budget, break and extract when exceeded), `MAX_ITERS = 100` (iteration cap), and `MATCH_LIMIT = 1_000` (kept for Phase 1, marked `#[allow(dead_code)]` with a `// TODO: read by ... Phase 1` note since no rule reads it yet).
  With zero rules the loop rebuilds once and exits (`changed = false`), so it is a no-op apart from invariant restoration.
  The match-collect/apply step has a clearly marked insertion point for Phase 1.

`lower` interns bottom-up, so structurally equal subterms share a class via hash-consing.
`raise` is a bottom-up DP: `compute_costs` fills per-class min cost (size = node count) with a cycle guard, then `build` picks the cheapest node per class (ties broken by `Ord`), memoized.
The cycle guard and finite-cost filter make extraction total once Phase 1 rules introduce cycles, though Phase 0 graphs are acyclic.

## Round-trip invariant used

Identity: `raise(lower(e)) == e`.

This is the correction the task called out.
The plan wrote the gate as `raise(lower(e)) == e.reduce(col_types)`, but with zero rules the faithful invariant is identity, because a no-rule bridge must reproduce the input exactly while `reduce` actively rewrites the term.
The `reduce` differential belongs to Phase 1.
Every test asserts strict structural equality (`assert_eq!`); no observational fallback was needed, because the bridge reproduces every corpus expression byte-for-byte.
This is documented in the `assert_round_trip` doc comment.

## Tests

`bin/cargo-test -p mz-transform -- eqsat::scalar`: 9 tests, all pass.

* `round_trip_column`, `round_trip_literal` (incl. null), `round_trip_unmaterializable` (`MzNow`), `round_trip_unary` (`Not`), `round_trip_binary` (`AddInt64`), `round_trip_variadic` (3-arg `And` and `Or`), `round_trip_if`, `round_trip_nested_mix` (nested `If`/`And`/`Not` with a repeated subterm to exercise hash-cons sharing), `canonicalize_is_identity`.

## Checks

* `cargo check -p mz-transform --tests`: clean.
* `cargo clippy -p mz-transform --tests`: clean (no warnings, no dead-code warnings).
* `cargo fmt -p mz-transform`: applied.
* `bin/lint`: all real checks pass; the only reported failure is `check-no-diff.sh`, which flags the (now committed) working-tree change and is expected pre-commit.

## Notes for later phases

* `MATCH_LIMIT` and the saturate match-application slot are stubbed and marked for Phase 1.
* The `eqsat.rs` module-level `#![allow(...)]` (as_conversions, missing_docs, ...) covers the scalar subtree, but the Phase-0 code uses no `as` conversions and is fully documented.
* The design note `doc/developer/design/20260625_eqsat_scalar_expressions.md` is untracked in the worktree and was not committed (pre-existing, outside Phase 0 scope).
