# Retarget the e-graph onto real MIR payloads

> Execution note: this is a tightly-coupled, crate-wide type refactor (shared
> types thread through every module), executed inline in one session rather than
> fanned out to independent subagents.
> A final whole-branch review runs at the end.

**Goal:**
Replace the opaque `Scalar` type and the `interner` indirection with real
`MirScalarExpr`/`AggregateExpr`/`MirRelationExpr` payloads carried directly by
`Rel` and `ENode`, so lower/raise is equality-preserving and column remapping
falls out of `MirScalarExpr::permute`.

**Architecture (option B, faithful from/to):**
Keep the thin `Rel` tree and the flat `ENode`, keep a lower/raise translation,
but make both carry real MIR payloads.
Delete `Scalar`/`ScalarId` and the whole `interner.rs`.
`cost`/`analysis`/`cse`/`engine`/`dsl`/`parser`/rules change only in the
*payload type* they read, not in structure.

**Tech Stack:** Rust, `mz-expr` (`MirScalarExpr::permute`/`permute_map`,
`support_into`, `as_column`, `reduce`, `is_literal_true`/`false`,
`AggregateExpr`), the ported e-graph engine.

## Global constraints

* Crate `src/transform-egraph`. No `unsafe`. No new `as` (use `mz_ore::cast`).
  `#[mz_ore::test]` not `#[test]`. Copyright headers. `cargo fmt` (never
  `--check`). No em-dash or semicolon in comments. Doc comment states the
  contract, reasoning lives inline.
* Equality preservation: for every plan over the modeled subset,
  `raise(lower(x)) == x` must hold (the existing round-trip tests, retargeted).
  Bailed subtrees round-trip verbatim because they are stored inline.
* Keep every comment that still applies. Do not narrate future work.
* The pass stays offline (test-only); no live-pipeline registration here.

## Design decisions (locked)

1. **`EScalar { expr: MirScalarExpr, lit: Option<bool> }`** is the scalar
   payload. `cols()` and `is_col()` are live methods over `expr`. `lit` is the
   only precomputed fact (fold against input column types at lower time, needed
   because the e-graph tracks arity not column types). `permute` rewrites
   `expr` and leaves `lit` unchanged (a column permutation cannot change
   literal-ness).
2. **Opaque leaf = `Rel::Opaque(Box<MirRelationExpr>)` / `ENode::Opaque(MirRelationExpr)`**,
   storing the bailed subtree verbatim. Replaces `Get { name: "leaf:N" }` plus
   `leaf_dedup`/`leaves`. Hashconsing dedups identical leaves. In cost and every
   analysis, `Opaque` behaves exactly as today's leaf `Get` (a base of
   `size_degree` 1.0, a non-negative/monotonic leaf with no children).
3. **`Rel::Get { name, arity }`** is retained only as the synthetic base used by
   engine/cost/analysis unit tests; lowering never emits it. `Rel::Constant
   { card, arity }` is retained only for engine-synthesized empties
   (`empty_false_filter`/`union_cancel`); original MIR `Constant`s bail to
   `Opaque`.
4. **`LocalGet { id, arity, get: Option<Box<MirRelationExpr>> }`**: `get` is
   `Some(original Get{Local})` for lowered references (raise returns it
   verbatim), `None` for engine scope placeholders (substituted out before
   raise) and cse-introduced locals.
5. **`WcoJoin`** stays an extension variant; raise emits a plain
   `MirRelationExpr::Join` (status quo). The `JoinImplementation` transform
   still chooses the physical strategy downstream.
6. **Synthesized empty typing** stays arity-based placeholder
   (`repr_type_of_arity`), unchanged from today. Type-faithful empties need
   e-graph column-type tracking; out of scope here.
7. **`Payload`** variants carry real types: `Predicates/Scalars/GroupKey ->
   Vec<EScalar>`, `Aggregates -> Vec<AggregateExpr>`, `Equivalences ->
   Vec<Vec<EScalar>>`, `Outputs -> Vec<usize>` (unchanged).

## Phases

Each phase ends with `cargo check -p mz-transform-egraph` green (the crate does
not compile mid-phase; that is expected for a type refactor). Tests run at the
end of Phase 6.

### Phase 1: payload types in `ir.rs`

* Define `EScalar` with derives `Clone, Debug, PartialEq, Eq, PartialOrd, Ord,
  Hash`. Methods: `cols(&self) -> BTreeSet<usize>` (via `support_into`),
  `is_col(&self) -> Option<usize>` (via `as_column`), `min_arity`,
  `permute_cols(&self, f) -> Result<EScalar, String>` (build a `permute_map`
  from `support`, apply to a clone of `expr`, error on negative target).
  A `Display` that prints `expr`. A test-only constructor mirroring
  `Scalar::new` semantics (from a `MirScalarExpr`, `lit = None`).
* Change `Rel` scalar fields to `Vec<EScalar>`, `aggregates` to
  `Vec<AggregateExpr>`, `equivalences` to `Vec<Vec<EScalar>>`. Add
  `Rel::Opaque(Box<MirRelationExpr>)`. Change `LocalGet` to carry
  `get: Option<Box<MirRelationExpr>>`. Update `arity`, `children`,
  `with_children`, `node_count`, `pretty`.
* Delete `Scalar`, `ScalarId`, and `ir::scalar_identity_tests`.
* Verify: `arity`/`children`/`with_children` cover every variant including
  `Opaque`.

### Phase 2: delete `interner.rs`

* Remove `pub mod interner;` from `lib.rs`. Delete `interner.rs`.
* Anything that used `Interner` (lower/raise/lib/egraph) is rewritten in later
  phases to operate without it.

### Phase 3: `egraph.rs` ENode and conditions

* `ENode` payloads -> real types mirroring `Rel`. Add `ENode::Opaque(MirRelationExpr)`,
  change `LocalGet` to carry `get: Option<Box<MirRelationExpr>>`. Update `sym`,
  `Sym`, `children`, `map_children`, `arity_guarded`, `add_rel`, `build_rel`,
  `read_payloads`, `instantiate`/`instantiate_list`, and the e-class
  `check_conds` arms (`AllColumns`/`AnyFalse`/`NoFalse`/`AllTrue` over
  `EScalar`).
* `add_rel(Rel::Opaque(m)) -> ENode::Opaque((*m).clone())`;
  `build_rel(ENode::Opaque(m)) -> Rel::Opaque(Box::new(m.clone()))`.
* `add_rel(Rel::Get{..})` / `Constant{..}` unchanged (synthetic/synthesized).
* `LocalGet` add/build round-trips `get`.

### Phase 4: `matcher.rs`

* `Payload` variants -> real types (decision 7). `columns()` via
  `EScalar::cols()`, `scalars()` returns `&[EScalar]`.
* Replace `map_scalar_cols`/`rewrite_text_cols` with `EScalar::permute_cols`.
  Delete `rewrite_text_cols`. `shift_payload`/`remap_payload` build the column
  map and call `permute_cols`; negative target is an error (as today).
* `cols_of_payload` uses `EScalar::is_col()`.
* `into_aggregates` returns `Vec<AggregateExpr>`; the rest return `Vec<EScalar>`/`Vec<usize>`.
* Condition arms (`AllColumns`/`AnyFalse`/`NoFalse`/`AllTrue`) over `EScalar`.
* Retarget the matcher unit tests (`pred(...)` helper builds `EScalar` from a
  `MirScalarExpr::column`-based expression; the `shift`/`remap` tests assert on
  `expr` support and `as_column`, not on rendered text).

### Phase 5: `cost.rs`, `analysis.rs`, `cse.rs`, `engine.rs`

* `cost.rs`: `size_degree`/`collect_*`/`Hypergraph::build` read
  `EScalar::cols()` instead of `scalar.cols`; add `Rel::Opaque` arm to
  `size_degree` (1.0, like `Get`) and to `collect_work`/`collect_memory`
  (no term, like `Get`). Retarget tests (the `eq`/`p` helpers build `EScalar`).
* `analysis.rs`: `ENode`/`Rel` arms add `Opaque` (leaf, same as `Get`); the
  `group_key.len()`/`aggregates.len()` reads are type-agnostic. Retarget tests.
* `cse.rs`: `worth_binding` adds `Rel::Opaque` to the "not worth binding"
  leaves alongside `Get`/`Constant`/`LocalGet`. Retarget tests.
* `engine.rs`: `max_local_id`/`hoist_scopes`/`substitute_locals` create
  `LocalGet { get: None, .. }`. Everything else is `Rel`-generic. No structural
  change.

### Phase 6: `lower.rs`, `raise.rs`, `lib.rs`, integration

* `lower.rs`: drop the `Interner` parameter. Build `EScalar`s directly
  (`lit` via `reduce` against `input.typ().column_types` for Filter/Map; `None`
  for join equivalences). Bailed subtrees -> `Rel::Opaque(Box::new(expr.clone()))`.
  Local `Get` -> `Rel::LocalGet { id, arity, get: Some(Box::new(expr.clone())) }`.
* `raise.rs`: drop the `Interner` parameter. `Rel::Opaque(m) -> *m.clone()`.
  `Rel::LocalGet { get: Some(g), .. } -> (*g).clone()` (panic on `None`, as the
  old `resolve_local_get` panicked). Synthesized `Constant` -> placeholder
  empty (unchanged). `WcoJoin` -> `Join` (unchanged). Scalars resolve directly
  from `EScalar::expr`.
* `lib.rs`: `optimize` becomes `raise(optimizer.optimize(lower(&expr)).plan)`
  with no interner. Keep the `debug_assert_eq!` arity guard.
* Retarget `lower.rs`/`raise.rs` unit tests (no interner; round-trip helper is
  `raise(lower(r)) == r`).
* `tests/roundtrip.rs`, `tests/compare_real.rs`, `tests/wcoj_decision.rs`: drop
  interner usage; build `EScalar` where they built `Scalar`.
* Run the full suite: `cargo test -p mz-transform-egraph` and
  `cargo test -p mz-transform --test test_transforms` (the `eqsat.spec` cases).
  The differential harness counts must not regress (record them).

## Verification after each phase

* Phases 1-5: `cargo check -p mz-transform-egraph` compiles cleanly by phase
  end (earlier phases may leave call sites broken that later phases fix; if so,
  note which and proceed).
* Phase 6: full `cargo test -p mz-transform-egraph` green; `eqsat.spec` green;
  `compare_real.rs` win/loss/tie counts equal to or better than 3/4/13.
* End: `cargo clippy -p mz-transform-egraph`, `cargo fmt`, `bin/lint`.

## Out of scope (follow-ons)

* Soundness bug #1 (unify the two condition evaluators / drop the
  `Rel`-path `GreedyOptimizer` foil or make it consult `LocalFacts`).
* Type-faithful synthesized empties (e-graph column-type tracking).
* Collapsing `Rel` into `MirRelationExpr` entirely (the maximal review
  recommendation).
* Re-enabling the three pushdown rules (now unblocked by real `permute`, but a
  separate change with its own tests).
