# P1: Reify `ArrangeBy` as a first-class eqsat IR + e-node

Foundational physical primitive (roadmap `docs/superpowers/plans/2026-06-21-physical-eqsat-gap-coverage.md`).
Today MIR `ArrangeBy` bails to `Rel::Opaque` (lower.rs:179), so the optimizer cannot see, share, remove, or look up through arrangements.
This phase adds an explicit node that lower/raise round-trip faithfully; it must NOT change any extracted plan yet (no rule fires on it until P2).

## Node shape

* `Rel::ArrangeBy { input: Box<Rel>, key: Vec<EScalar> }` (ir.rs).
* `ENode::ArrangeBy { input: Id, key: Vec<EScalar> }` (egraph.rs).
* `Sym::ArrangeBy`.
* MIR `ArrangeBy` carries `Vec<Vec<MirScalarExpr>>` (multiple keys). DECISION: model one key list per node; a multi-key MIR `ArrangeBy` lowers to one `Rel::ArrangeBy` per key wrapping the input, or stays `Opaque` if that is simpler for the round-trip. Start by lowering ONLY single-key `ArrangeBy`; multi-key stays `Opaque` (sound, faithful). Revisit in P2/P3 when a rule needs multi-key.
* Identity: `key` is part of the e-node hash/eq (derived on `ENode`), so `ArrangeBy(x,k1)` and `ArrangeBy(x,k2)` never merge. No extra work, the derive covers it.
* Arity = input arity. Children = `[input]`. Multiset identity: all analyses pass the input value through; extraction propagates parent `Demand` unchanged.

## Change-site checklist (template: follow `WcoJoin`, except ArrangeBy is also lowered from MIR and is single-input)

### ir.rs
* [ ] Add `Rel::ArrangeBy { input: Box<Rel>, key: Vec<EScalar> }` variant (after `WcoJoin`).
* [ ] `arity`: add to the `Filter | Negate | Threshold | TopK` passthrough arm (returns `input.arity()`).
* [ ] `children`: add to the single-`input` arm (`vec![input]`).
* [ ] `with_children`: add arm `Rel::ArrangeBy { key, .. } => Rel::ArrangeBy { input: Box::new(take(0)), key: key.clone() }`.
* [ ] `pretty`: add arm printing `ArrangeBy [keys]` then `input.pretty(indent+1)`.
* [ ] `Display`/any other exhaustive match in ir.rs.

### egraph.rs
* [ ] Add `ENode::ArrangeBy { input: Id, key: Vec<EScalar> }` (after `WcoJoin`).
* [ ] `Sym::ArrangeBy` + `sym()` arm.
* [ ] `children()` and `map_children()`: add to the single-`input` arm (Project/Map/Filter/...).
* [ ] `add_rel` (Rel->ENode, ~line 379): `Rel::ArrangeBy { input, key } => ENode::ArrangeBy { input: self.add_rel(input), key: key.clone() }`.
* [ ] `arity_guarded` (~line 519): add to the `Filter | TopK | Negate | Threshold` passthrough arm.
* [ ] col_types derivation (~line 544+): ArrangeBy preserves the input's column types, add a passthrough arm.
* [ ] ENode->Rel extraction (~line 1283): `ENode::ArrangeBy { input, key } => Rel::ArrangeBy { input: Box::new(get(*input, demand)?), key: key.clone() }` (sign-preserving, propagate `demand`).
* [ ] `rewrite_escalars` (~line 1331): treat the `key` like `Map` scalars (rewrite by the e-class reducer for canonicalization, `lit: None`). This makes P2's `produces_key` match modulo equivalence. Add to the comment that lists which operators carry rewritable payloads.

### analysis.rs
* [ ] Every `Analysis::make` match (NonNeg, Keys, Monotonic, Equivalences, ConstantColumns): ArrangeBy is a multiset identity, so it passes the input's domain value through unchanged. Add an arm wherever the analysis matches single-input passthrough nodes (Filter/TopK/etc. are the template). Keys in particular: an arrangement preserves unique keys, so propagate.

### lower.rs
* [ ] Remove `ArrangeBy { .. }` from the bail set (line 179). Add an arm that lowers a SINGLE-key MIR `ArrangeBy { input, keys }` to `Rel::ArrangeBy { input: lower(input), key: keys[0] as Vec<EScalar> }`; multi-key (`keys.len() != 1`) stays `Rel::Opaque`.
* [ ] Update the module doc (lines 26-27) so `ArrangeBy` is no longer listed as bailed.
* [ ] Update the lower.rs tests that assert `ArrangeBy` becomes `Opaque` (lines 288-303): a single-key ArrangeBy now lowers to `Rel::ArrangeBy`; keep a multi-key case asserting `Opaque`.

### raise.rs
* [ ] Add `Rel::ArrangeBy { input, key } => MirRelationExpr::ArrangeBy { input: Box::new(raise(input)), keys: vec![key exprs], ... }` mirroring MIR's field shape.
* [ ] Add a roundtrip test: `MIR ArrangeBy(single key) -> lower -> raise == identity`.

### cost.rs
* [ ] `collect_memory` (~line 331): charge the arrangement memory term on `Rel::ArrangeBy` at `size_degree(input)`. This is the explicit form of what Reduce/TopK/Join charge implicitly. (P2 generalizes to the distinct-(eclass,key) set; here just emit one term per ArrangeBy.)

### dsl.rs / lean.rs
* [ ] dsl.rs and lean.rs have exhaustive matches over the node kinds for the rule AST / Lean emitter. ArrangeBy is not yet referenced by any rule, so add the minimal arm needed to keep matches exhaustive (mirror how WcoJoin is handled). No rule grammar change yet (P2 adds the first ArrangeBy rule).

### rules/relational.rewrite + build/
* [ ] NO change in P1. The matcher codegen enumerates node kinds it knows from the grammar; ArrangeBy appears in no rule yet. Confirm `cargo build -p mz-transform` still codegens cleanly (the build.rs parser only reads the rule file, which is unchanged).

## Verification (per the mz-test skill)

1. `cargo check -p mz-transform` after each file group; fix exhaustiveness errors (the compiler enumerates every missed match arm, which IS the rest of the checklist).
2. `cargo test -p mz-transform --lib eqsat::` (73 tests) + the new roundtrip test.
3. `cargo test -p mz-transform --test test_transforms` (eqsat.spec) MUST pass WITHOUT rewrite: P1 changes no extracted plan (ArrangeBy is created by no rule; existing Opaque-wrapped ArrangeBys that now lower explicitly must raise back identically).
   * If the spec DOES change, investigate: the only legitimate diff is an `ArrangeBy` that was inside an `Opaque` now rendering as an explicit node, which should still be byte-identical MIR after raise. A real diff means the round-trip is lossy. Fix the round-trip, do not rewrite the golden.
4. `cargo run --release -p mz-transform --example eqsat_bench 2000`: no regression (the corpus has no ArrangeBy, so this guards against accidental slowdown in shared paths).
5. `cargo fmt`, `bin/lint`, `cargo clippy -p mz-transform` clean before commit.

## Commit

Single commit `transform/eqsat: reify ArrangeBy as a first-class IR and e-node`, branch `claude/mir-equality-optimizer-sodbej`.
Trailers: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: https://claude.ai/code/session_016CwwG1wCgqf3v8g6uhf1Nx`.
