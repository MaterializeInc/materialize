# SP2b Slice 6c: `and_or_drop_unit` + `and_or_dedup` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port `and_or_drop_unit` and `and_or_dedup` from the standalone
`ScalarEGraph` into the CombinedLang declarative DSL, behavior-neutral against the
scalar oracle, adding a grammar-general rest-splice-with-filter primitive.

**Architecture:** A new `TElem::FilterSplice { list, filter: RestFilter }` in the
shared `ListTmpl` emits a filtered sublist of a variadic's operands as a new
variadic node. `RestFilter::DedupById` (grammar-general, canonical-id dedup) and
`RestFilter::DropScalarLit(bool)` (scalar-specific per-element predicate) back the
two rules. A new `Cond::HasDuplicateId` guards dedup. drop_unit reuses 6a's
`Cond::AnyScalarLit` as its guard. Two hand-written helpers in a new
`rest_filters.rs` do the filtering on the base `EGraph`. Lean proves the
algebraic core outright, permanent-sorry count stays 5.

**Tech Stack:** Rust (`mz-transform`), chumsky build-time parser
(`build/grammar.rs`), build-time codegen (`build/codegen.rs`), Lean 4
(`src/transform/lean`).

## Global Constraints

- Behavior-neutral vs the standalone `ScalarEGraph` oracle. Differential parity
  `new_combined == old_scalar` is the gate. No `--rewrite` on sqllogictest ever.
- No CI backstop for Lean. Aggregate `lake build` (clean rebuild) and the
  two-sided sorry-taxonomy grep (`ci/test/lean-mir-rewrite.sh` logic) are
  MANDATORY hand-run gates. Permanent-sorry count must stay **5**. Any bump is
  surfaced to the overseer before landing, never silent.
- No production-default flag flips. No `ScalarEGraph` deletion (slice 7). No
  `AnalysisNeeds` / analysis-lattice additions.
- Scalar rules must be `colored: false` (build-enforced at codegen.rs:82).
- Any new `MatchGraph` / `ApplyGraph` / codegen trait method → ALL-IMPLS sweep,
  including `ColoredView` (slice-3 E0046 lesson). Use `cargo check --tests`, not
  bare `cargo check` (slice-3 E0502 lesson).
- chumsky `choice` has a 26-tuple arity ceiling. New scalar conds go in the
  existing nested scalar sub-`choice` (6a lesson), which has ~20 slots headroom.
- `bin/fmt` + `cargo check` before every commit. Stage only your own files. Never
  broad `git restore`/`git clean` unstaged SDD scratch. `doc/developer/generated/`
  is read-only.
- Regenerate Lean after edits: `cargo run -p mz-transform --example gen-lean`.

---

### Task 1: DSL AST + grammar for `FilterSplice`, `RestFilter`, `HasDuplicateId`

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs`
- Modify: `src/transform/build/grammar.rs`

**Interfaces:**
- Produces: `TElem::FilterSplice { list: String, filter: RestFilter }`,
  `enum RestFilter { DropScalarLit(bool), DedupById }`,
  `Cond::HasDuplicateId { list: String }`. Grammar accepts `dedup(xs)`,
  `drop_scalar_lit(xs, <bool>)` inside a template list, and `has_duplicate_id(xs)`
  as a side condition.

- [ ] **Step 1: Add the AST types to `dsl.rs`.**

In the `TElem` enum (after `MapSplice`):

```rust
    /// Splice a rest list after filtering it. `RestFilter` chooses the filter.
    /// Backs `and_or_drop_unit` (drop unit literals) and `and_or_dedup`
    /// (drop later duplicates by canonical id). Grammar-general: `ListTmpl`
    /// backs both `Tmpl::Union` and `Tmpl::SVariadic`.
    FilterSplice { list: String, filter: RestFilter },
```

Add the `RestFilter` enum near `TElem`:

```rust
/// The filter a [`TElem::FilterSplice`] applies to a rest list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestFilter {
    /// Keep operands whose scalar-literal analysis is not `Some(Some(bool))`.
    /// The per-element predicate is scalar-specific. Backs `and_or_drop_unit`,
    /// where the boolean is the connective unit (`true` for And, `false` for Or).
    DropScalarLit(bool),
    /// Keep the first operand of each canonical e-class id, dropping later
    /// duplicates. Sort-agnostic (canonical id only), so grammar-general.
    DedupById,
}
```

In the `Cond` enum, add:

```rust
    /// `has_duplicate_id(list)`: two or more operands in the rest list share a
    /// canonical e-class id. The fire-guard for `and_or_dedup`, so the rule
    /// never rebuilds an unchanged operand list. Grammar-general (canonical id).
    HasDuplicateId { list: String },
```

- [ ] **Step 2: Add grammar for the two filtered splices in `grammar.rs`.**

In the `listtmpl` block (grammar.rs:454, alongside `mapsplice` / `splice` /
`item`), add before the `choice((...))`:

```rust
            let dedup = kw("dedup")
                .ignore_then(ident().delimited_by(just(Token::LParen), just(Token::RParen)))
                .map(|list| TElem::FilterSplice {
                    list,
                    filter: RestFilter::DedupById,
                });
            let drop_scalar_lit = kw("drop_scalar_lit")
                .ignore_then(just(Token::LParen))
                .ignore_then(ident())
                .then_ignore(just(Token::Comma))
                .then(bool_lit.clone())
                .then_ignore(just(Token::RParen))
                .map(|(list, value)| TElem::FilterSplice {
                    list,
                    filter: RestFilter::DropScalarLit(value),
                });
```

and extend the combinator choice to `choice((mapsplice, dedup, drop_scalar_lit, splice, item))`.
`dedup` / `drop_scalar_lit` must come before `item` so their keywords are not
swallowed as a bare `RelVar` tmpl.

If no `bool_lit` parser exists yet, add one near the top of the parser (a
`kw("true").to(true)` / `kw("false").to(false)` choice); check for an existing
one first (`SBool` / `true`/`false` literals are already parsed for `Tmpl::SBool`
— reuse that parser, do not duplicate it).

- [ ] **Step 3: Add grammar for the `has_duplicate_id` cond.**

In the nested scalar-cond sub-`choice` (grammar.rs:702–708, the group holding
`scalar_lit_true` / `scalar_any_lit_false` / etc.):

```rust
            one_ident("has_duplicate_id").map(|list| Cond::HasDuplicateId { list }),
```

Confirm the sub-`choice` stays within the 26-tuple ceiling (it had ~20 slots of
headroom after 6a; adding one is safe). Do not add a third nesting level.

- [ ] **Step 4: Add parser round-trip tests in `grammar.rs` tests.**

Mirror the existing `parses_scalar_any_lit_cond` test. Add tests asserting:
- `rule r { Variadic[and](xs...) => Variadic[and](drop_scalar_lit(xs, true)) where scalar_any_lit_true(xs) }`
  parses to an `SVariadic` template whose `inputs.elems` is
  `[FilterSplice { list: "xs", filter: DropScalarLit(true) }]` and one
  `Cond::AnyScalarLit { value: true }`.
- `rule r { Variadic[or](xs...) => Variadic[or](dedup(xs)) where has_duplicate_id(xs) }`
  parses to `FilterSplice { list: "xs", filter: DedupById }` and
  `Cond::HasDuplicateId { list: "xs" }`.

- [ ] **Step 5: Run the grammar/parser tests.**

Run: `bin/cargo-test -p mz-transform --lib eqsat::` (or the build-crate test
harness the repo uses for grammar tests; check how `parses_scalar_any_lit_cond`
is run). Codegen will not compile yet (expected E0004 non-exhaustive matches in
`codegen.rs` for the new `TElem` / `Cond` variants, resolved in Task 2). Confirm
the parser tests themselves pass and the new variants are constructed.

- [ ] **Step 6: `bin/fmt`, commit.**

```bash
bin/fmt
git add src/transform/src/eqsat/dsl.rs src/transform/build/grammar.rs
git commit -m "eqsat dsl: FilterSplice/RestFilter + HasDuplicateId cond + grammar (SP2b Slice 6c, Task 1)"
```

---

### Task 2: Filter machinery — `rest_filters` helpers, cond backing, codegen emit

**Files:**
- Create: `src/transform/src/eqsat/rest_filters.rs`
- Modify: `src/transform/src/eqsat/mod.rs` (declare the module)
- Modify: `src/transform/src/eqsat/egraph/view.rs` (MatchGraph method + BaseView impl)
- Modify: `src/transform/src/eqsat/colored/view.rs` (ColoredView inert impl)
- Modify: `src/transform/build/codegen.rs` (cond emit, is_color_exact, FilterSplice emit, telem serialization, cond serialization)

**Interfaces:**
- Consumes: `TElem::FilterSplice`, `RestFilter`, `Cond::HasDuplicateId` (Task 1).
- Produces: `rest_filters::rest_dedup_by_id(g: &EGraph, ids: &[Id]) -> Vec<Id>`,
  `rest_filters::rest_drop_scalar_lit(g: &EGraph, ids: &[Id], value: bool) -> Vec<Id>`,
  `MatchGraph::cond_has_duplicate_id(&self, ids: &[Id]) -> bool`.

- [ ] **Step 1: Write `rest_filters.rs`.**

```rust
// Copyright header (copy from a sibling file).

//! Rest-list filters for `TElem::FilterSplice` right-hand sides. Both run on the
//! base `EGraph` during apply. Scalar rules are `colored: false`, so their apply
//! bodies only ever compile against the base graph.

use std::collections::HashSet;

use crate::eqsat::egraph::combined::EGraph;
use crate::eqsat::egraph::Id;

/// First-occurrence dedup by canonical e-class id. Sort-agnostic, so this backs
/// a grammar-general `dedup(xs)` over any variadic (scalar `And`/`Or`, relational
/// `Union`).
pub(crate) fn rest_dedup_by_id(g: &EGraph, ids: &[Id]) -> Vec<Id> {
    let mut seen = HashSet::new();
    ids.iter().copied().filter(|&id| seen.insert(g.find(id))).collect()
}

/// Keep operands whose scalar-literal analysis is not `Some(Some(value))`. Drops
/// the connective unit (`true` for And, `false` for Or) from a boolean fold. The
/// per-element predicate reads the base scalar analysis, so it is scalar-specific.
pub(crate) fn rest_drop_scalar_lit(g: &EGraph, ids: &[Id], value: bool) -> Vec<Id> {
    ids.iter()
        .copied()
        .filter(|&id| scalar_lit_bool(g, id) != Some(value))
        .collect()
}

/// The boolean literal of scalar class `id`, per the base scalar `literal`
/// analysis. `None` for null / non-boolean / non-literal / no-analysis classes.
/// Mirrors `BaseView::scalar_lit_bool_or_null` collapsed to the bool case.
fn scalar_lit_bool(g: &EGraph, id: Id) -> Option<bool> {
    let (row, _ty) = g.data().scalar.analysis.get(&g.find(id))?.literal.as_ref()?;
    let row = row.as_ref().ok()?;
    match row.unpack_first() {
        mz_repr::Datum::True => Some(true),
        mz_repr::Datum::False => Some(false),
        _ => None,
    }
}
```

Verify the exact paths (`EGraph`, `Id`, `g.data().scalar.analysis`,
`g.find`) against `egraph/view.rs:147` (`BaseView::scalar_lit_bool_or_null`) and
`egraph/combined.rs`. Match whatever those use. Do not add a public API surface
beyond `pub(crate)`.

- [ ] **Step 2: Declare the module in `eqsat/mod.rs`.**

Add `mod rest_filters;` alongside the existing `mod scalar_builtins;` (or
`pub(crate) mod` if the generated code path requires it — match how
`scalar_builtins` is declared, since the generated apply references
`crate::eqsat::scalar_builtins::`).

- [ ] **Step 3: Add `cond_has_duplicate_id` to `MatchGraph` + BaseView.**

In `egraph/view.rs`, add to the `MatchGraph` trait (near `cond_any_scalar_lit`):

```rust
    /// Whether two or more of `ids` share a canonical e-class id. The backing
    /// for `Cond::HasDuplicateId`, the `and_or_dedup` fire-guard.
    fn cond_has_duplicate_id(&self, ids: &[Id]) -> bool;
```

BaseView impl:

```rust
    fn cond_has_duplicate_id(&self, ids: &[Id]) -> bool {
        let mut seen = std::collections::HashSet::new();
        !ids.iter().all(|&id| seen.insert(self.eg.find(id)))
    }
```

- [ ] **Step 4: Add the inert ColoredView impl.**

In `colored/view.rs` (near `scalar_lit_bool_or_null` at line 403), add:

```rust
    fn cond_has_duplicate_id(&self, _ids: &[Id]) -> bool {
        // Colored saturation never runs scalar rules (they are `colored: false`),
        // so this is unreachable in practice. Inert `false` keeps the rule from
        // firing if ever reached, matching the analysis-gated conds.
        false
    }
```

- [ ] **Step 5: Codegen — cond emit + is_color_exact + serialization.**

In `build/codegen.rs`:
- `cond_is_color_exact` (line 28–64): add `Cond::HasDuplicateId { .. } => false,`.
- Cond emit (near line 550, the `Cond::AnyScalarLit` arm):
  ```rust
  Cond::HasDuplicateId { list } => {
      format!("g.cond_has_duplicate_id(&{})", m.rest_local(list))
  }
  ```
- Cond serialization (near line 1646):
  ```rust
  Cond::HasDuplicateId { list } => format!(
      "{P}::Cond::HasDuplicateId {{ list: {} }}", s(list)
  ),
  ```

- [ ] **Step 6: Codegen — `FilterSplice` emit in `listtmpl_stmts`.**

In `listtmpl_stmts` (codegen.rs:1098, the `match elem`), add:

```rust
            TElem::FilterSplice { list, filter } => {
                let call = match filter {
                    RestFilter::DedupById => format!(
                        "crate::eqsat::rest_filters::rest_dedup_by_id(g, b.rests.get({list:?}).ok_or_else(|| \"unbound rest metavariable {list}\".to_string())?)"
                    ),
                    RestFilter::DropScalarLit(value) => format!(
                        "crate::eqsat::rest_filters::rest_drop_scalar_lit(g, b.rests.get({list:?}).ok_or_else(|| \"unbound rest metavariable {list}\".to_string())?, {value})"
                    ),
                };
                out.push_str(&format!("{v}.extend({call});\n"));
            }
```

Confirm `g` in an apply body is the concrete `&mut EGraph` (codegen.rs:1139) so
`rest_dedup_by_id(g, ..)` / the `g.find` inside it typecheck. These helpers are
only reachable from scalar (`colored: false`) rules, whose apply bodies are
emitted solely into `apply_NAME_base(g: &mut EGraph, ..)`, never into
`colored_apply`.

- [ ] **Step 7: Codegen — `TElem::FilterSplice` serialization in `telem`.**

In `telem` (codegen.rs:1472):

```rust
        TElem::FilterSplice { list, filter } => {
            let f = match filter {
                RestFilter::DedupById => format!("{P}::RestFilter::DedupById"),
                RestFilter::DropScalarLit(v) => format!("{P}::RestFilter::DropScalarLit({v})"),
            };
            format!("{P}::TElem::FilterSplice {{ list: {}, filter: {} }}", s(list), f)
        }
```

- [ ] **Step 8: `cargo check --tests` and the view unit tests.**

Run: `bin/cargo-test -p mz-transform --lib eqsat::egraph::view` and
`cargo check -p mz-transform --tests`. Expect a clean build (no rule uses the new
machinery yet, so `COMPILED_RULES` is unchanged). If `lean.rs` has non-exhaustive
matches on `TElem` / `Cond`, they should be covered by existing wildcard arms; if
not, that is Task 4's surface, but the crate must still compile now — add a
minimal `todo!()`-free wildcard-safe arm only if the compiler forces it, and note
it for Task 4.

- [ ] **Step 9: `bin/fmt`, commit.**

```bash
bin/fmt
git add src/transform/src/eqsat/rest_filters.rs src/transform/src/eqsat/mod.rs \
        src/transform/src/eqsat/egraph/view.rs src/transform/src/eqsat/colored/view.rs \
        src/transform/build/codegen.rs
git commit -m "eqsat: rest-filter emit machinery + has_duplicate_id cond backing (SP2b Slice 6c, Task 2)"
```

---

### Task 3: Port the four rules to `scalar.rewrite`

**Files:**
- Modify: `src/transform/src/eqsat/rules/scalar.rewrite`

**Interfaces:**
- Consumes: all Task 1–2 machinery. `SCALAR_COMPILED_RULES` grows 18 → 22,
  `COMPILED_RULES` stays 37 (no relational leak).

- [ ] **Step 1: Add the four rules.**

Add after the `and_short_circuit` / `or_short_circuit` rules:

```
rule and_drop_unit { doc "And([.., true, ..]) = And([.. without true])"
    Variadic[and](xs...) => Variadic[and](drop_scalar_lit(xs, true))  where scalar_any_lit_true(xs) }
rule or_drop_unit  { doc "Or([.., false, ..]) = Or([.. without false])"
    Variadic[or](xs...)  => Variadic[or](drop_scalar_lit(xs, false))  where scalar_any_lit_false(xs) }
rule and_dedup     { doc "And([a, a, b]) = And([a, b])"
    Variadic[and](xs...) => Variadic[and](dedup(xs))                  where has_duplicate_id(xs) }
rule or_dedup      { doc "Or([a, a, b]) = Or([a, b])"
    Variadic[or](xs...)  => Variadic[or](dedup(xs))                   where has_duplicate_id(xs) }
```

Match the surrounding rule's formatting (the repo uses a specific column
alignment; follow the existing rules, do not invent one).

- [ ] **Step 2: Build and inspect the generated rules.**

Run: `cargo check -p mz-transform --tests`. Then inspect the generated
`apply_and_drop_unit_base` / `apply_and_dedup_base` in the codegen output
(`target/.../out/eqsat_rules.rs` or via the build script's emitted file) to
confirm:
- `and_drop_unit` binds `rest1 = ins0[0..]`, guards on
  `g.cond_any_scalar_lit(&rest1, true)`, and its apply extends a fresh vec with
  `rest_drop_scalar_lit(g, .., true)` then builds `CallVariadic { func: And, .. }`.
- `and_dedup` guards on `g.cond_has_duplicate_id(&rest1)` and its apply uses
  `rest_dedup_by_id`.
- The Or variants use `false` / the Or func.

- [ ] **Step 3: Confirm no relational leak.**

Confirm `COMPILED_RULES` length is unchanged (37) and `SCALAR_COMPILED_RULES` is
22. The four new rules are rooted at `Pat::SVariadic`, so `is_scalar_rule` routes
them into `SCALAR_COMPILED_RULES` only. Verify via the generated table or a small
assertion in an existing rule-count test if one exists.

- [ ] **Step 4: Run the scalar rule unit tests.**

Run: `bin/cargo-test -p mz-transform --lib eqsat::scalar` (and any generated-rule
test). Expect all green.

- [ ] **Step 5: `bin/fmt`, commit.**

```bash
bin/fmt
git add src/transform/src/eqsat/rules/scalar.rewrite
git commit -m "eqsat: port and_or_drop_unit + and_or_dedup to scalar.rewrite (SP2b Slice 6c, Task 3)"
```

---

### Task 4: Lean — fold-invariance lemmas + render + proofs, permanent stays 5

**Files:**
- Modify: `src/transform/lean/MirRewrite/Semantics.lean`
- Modify: `src/transform/src/eqsat/lean.rs`
- Regenerate: `src/transform/lean/MirRewrite/Generated.lean`

**Interfaces:**
- Consumes: the four rules (Task 3). Produces four proved theorems, permanent
  sorry count unchanged at 5.

- [ ] **Step 1: Add the fold-invariance lemmas to `Semantics.lean`.**

Recommended path (robust, avoids `List.dedup`/Mathlib): prove the fold equals the
`List.all` / `List.any` of the operands, then reduce both rules to membership
invariance.

```lean
/-! ### Evidence for the drop-unit and dedup rules

`and_or_drop_unit` removes operands equal to the connective unit; `and_or_dedup`
removes later duplicates. Both preserve the AND/OR fold because the fold is the
`all`/`any` of the operands (idempotent and commutative over `Bool`), so it
depends only on which operands are present, not their multiplicity or an
identity operand. -/

theorem denoteSFold_and_eq_all (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env xs true (· && ·) = xs.all (fun x => denoteS env x) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [denoteSFold, List.all_cons, ih]

theorem denoteSFold_or_eq_any (env : Nat → Bool) (xs : List ScalarExpr) :
    denoteSFold env xs false (· || ·) = xs.any (fun x => denoteS env x) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [denoteSFold, List.any_cons, ih]
```

Then the two rule shapes reduce to:
- drop_unit: `(xs.filter p).all f = xs.all f` when every filtered-out element has
  `f x = true` (for `p x := x ≠ litB true`, dropped elements are `litB true`,
  whose `denoteS` is `true`; a `true` conjunct does not change `all`).
- dedup: `(dedup xs).all f = xs.all f` because dedup preserves membership and
  `List.all` depends only on membership.

Prove the specific helper the render emits (see Step 2 for the exact RHS list
expression). Keep every lemma `sorry`-free. If the dedup membership-invariance
proof needs a local dedup definition (rather than `List.dedup`), define a small
`dedupById : List ScalarExpr → List ScalarExpr` and its `mem_dedup` lemma
locally; derive `DecidableEq ScalarExpr` if required. **If proving dedup outright
proves intractable, STOP and surface it to the overseer before adding any
`sorry`.** The permanent count must not move without an explicit decision.

- [ ] **Step 2: Render `FilterSplice` in `lean.rs` `tmpl_list_expr`.**

In `lean.rs` (line 561, the `TElem` match in `tmpl_list_expr`), add an arm for
`FilterSplice`:
- `DropScalarLit(value)` → `List.filter (fun x => x != ScalarExpr.litB <value>) <list>`
  (use the Lean boolean-inequality that the proof in Step 1 expects; match the
  predicate to the lemma).
- `DedupById` → the local `dedupById <list>` (or `List.dedup <list>` if the
  project's Lean env has it and keeps first-occurrence).

Match the exact Lean identifiers to what Step 1 proves. The render and the lemma
must line up, or the `simp`/`rw` will not close.

- [ ] **Step 3: Add `choose_proof` arms for the four rules.**

In `lean.rs` `choose_proof` (near line 813, after the short-circuit arm), add
arms that key on the RHS shape (`andE`/`orE` applied to a `List.filter` /
`dedupById` term), NOT on `AnyScalarLit` value alone, so they do not clobber the
short-circuit arm (short_circuit RHS is a `litB`, drop_unit/dedup RHS is an
`andE`/`orE` of a filtered list). Apply `denoteSFold_and_eq_all` /
`denoteSFold_or_eq_any` plus the membership lemma from Step 1.

The `Cond::HasDuplicateId` and the (reused) `Cond::AnyScalarLit` hypotheses are
NOT needed by these proofs (both rules hold unconditionally over the fold). Ensure
the conds-to-hypotheses collection (lean.rs:94) either ignores `HasDuplicateId` or
emits an unused hypothesis. An unused `AnyScalarLit` hypothesis on drop_unit is
fine (a linter warning, as with `if_true` in 6a); do not remove it.

- [ ] **Step 4: Regenerate and diff.**

Run: `cargo run -p mz-transform --example gen-lean`. Confirm `Generated.lean`
changed only by the four added theorems (parity: the emitter output equals the
committed file for all prior theorems).

- [ ] **Step 5: Aggregate `lake build` + sorry taxonomy (hand-run gate).**

Build the aggregate Lean target on a clean rebuild. Confirm it is green. Run the
two-sided sorry-taxonomy grep (`ci/test/lean-mir-rewrite.sh` logic): permanent
sorry count == 5, no non-permanent sorry. Kernel-check the four new theorems'
axioms (`#print axioms`) show only `[propext, Quot.sound]` (no `sorryAx`).

- [ ] **Step 6: `bin/fmt`, commit.**

```bash
bin/fmt
git add src/transform/lean/MirRewrite/Semantics.lean src/transform/src/eqsat/lean.rs \
        src/transform/lean/MirRewrite/Generated.lean
git commit -m "eqsat lean: prove drop-unit + dedup fold-invariance outright (SP2b Slice 6c, Task 4)"
```

---

### Task 5: Differential corpus + parity + slice gate

**Files:**
- Modify: `src/transform/src/eqsat/scalar_saturate.rs` (or the differential
  parity test module used by slice 6a) and the shared `eqsat_scalar_corpus`.

**Interfaces:**
- Consumes: all prior tasks. Produces `scalar_parity_slice6c`,
  `corpus_covers_slice6c`, and a termination test.

- [ ] **Step 1: Extend the differential corpus.**

Add corpus terms exercising, for both And and Or:
- unit literal at leading / middle / trailing position (drop_unit).
- duplicates adjacent, non-adjacent, and >2 copies (dedup).
- error operands: `And([1/0, 1/0]) => And([1/0])` (same single error);
  `And([1/0, true]) => And([1/0])` (drop_unit adjacent to an error, error kept).
- nulls (3VL): a null operand must NOT be dropped by drop_unit (analysis literal
  is `Some(None)`, not `Some(Some(unit))`); confirm parity.
- interaction cascades: `And([x, true]) => And([x]) => x`;
  `And([true]) => And([]) => true`; `And([a, a]) => And([a]) => a`.

Follow the slice-6a corpus structure (`corpus_covers_slice6a`) and its helper
builders. Add a `corpus_covers_slice6c` assertion that the corpus actually
triggers each new rule.

- [ ] **Step 2: Differential parity test.**

Add `scalar_parity_slice6c`: for each corpus term, saturate in the combined
`EGraph<CombinedLang>` and in the standalone `ScalarEGraph`, extract, assert
equal `MirScalarExpr`. This is the behavior-neutrality gate.

- [ ] **Step 3: Termination test.**

Add `drop_unit_dedup_terminates`: saturating a term with both a unit literal and
duplicates converges in a small iteration count (the fire-guards make no-op fires
impossible, and each fire strictly shrinks the operand list). Assert iterations
`<< 100`.

- [ ] **Step 4: Run the parity + termination tests.**

Run: `bin/cargo-test -p mz-transform --lib eqsat::` targeting the parity and
corpus tests plus all prior slice parity tests. Expect all green.

- [ ] **Step 5: Full relational golden gate (no `--rewrite`).**

Run the relational sqllogictest suite the way slice 6a did:
`bin/sqllogictest --optimized -- test/sqllogictest/transform/*.slt` (the exact
glob slice 6a used; 1187 assertions across 37 files). Expect `success == total`,
zero diffs, and NO `--rewrite`. Production behavior must be byte-neutral (these
rules only fire under eqsat, off by default; the goldens confirm no relational
leak).

- [ ] **Step 6: Aggregate Lean gate re-confirm + `bin/fmt`, commit.**

Re-confirm the aggregate `lake build` is green and permanent sorry == 5 (Task 4
already did; re-check after any corpus-driven change). Then:

```bash
bin/fmt
git add src/transform/src/eqsat/scalar_saturate.rs  # + corpus file(s) touched
git commit -m "eqsat: differential parity + slice gate for drop-unit + dedup (SP2b Slice 6c, Task 5)"
```

---

## Self-Review Notes (controller)

- **Scope decision to surface before landing:** dedup stays in 6c (canonical-id
  filter, not the 6e cross-element structural pair-search). 6c delivers the
  rest-emit half of the union-cancel machinery, not the structural-pair half.
- **could_error:** both rules unconditional, verified vs `rules.rs:181,270`.
- **Grammar-general:** `FilterSplice` lives in the shared `ListTmpl`;
  `rest_dedup_by_id` is sort-agnostic. Colored relational reuse would need
  `ApplyGraph::canonical` (future, flagged, out of 6c scope).
- **Lean:** permanent count stays 5; dedup outright-proof is the risk item, with
  a flag-before-sorry gate.
- **Queued:** the post-slice-6 relational union-cancel re-test reuses this
  rest-emit machinery; ledger keeps it queued.
