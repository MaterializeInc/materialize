# SP2b Slice 6e: `absorb_and_or` (cross-element structural search via Rust host) — Design

## Context

Part of SP2b (governing design:
`docs/superpowers/specs/2026-07-01-eqsat-sp2b-scalar-dsl-port-design.md`), porting
the standalone `ScalarEGraph` rules into the CombinedLang DSL so slice 7 can
delete the scalar engine. Slices 1-5, 6a, 6c shipped merge-ready. Slice 6c added
the rest-splice-with-filter scaffold (`TElem::FilterSplice` + `RestFilter` +
`rest_filters` helpers on the base `EGraph`). Slice 6e adds cross-element
structural search on that scaffold, the machinery the queued relational
union-cancel re-test will consume.

Branch: `claude/mir-equality-optimizer-sodbej`.

## Goal

Port `absorb_and_or` (`src/transform/src/eqsat/scalar/rules.rs:601`) into the DSL,
behavior-neutral against the standalone `ScalarEGraph` oracle.

## What the rule actually is (confirmed from the eval, not the framing)

The rule is NOT the simple "operand `a` present, and `a` is a member of another
operand `Or(..a..)`". It is **inner-set proper-subset subsumption**:

- Each outer operand maps to an inner-set (`inner_sets`, `rules.rs:414`): if the
  operand's class holds an `inner_func` variadic node (the dual connective), that
  node's canonical operand ids are the set, else the operand is a singleton
  `{find(operand)}`.
- Drop operand `Q` when some distinct operand `P` has `inner-set(P) ⊊ inner-set(Q)`
  (proper subset). For outer `Or`/inner `And`: `Q = AND(S_Q)` has more conjuncts
  than `P = AND(S_P)`, so `Q ⟹ P` and `P ∨ Q = P`. Dual for outer `And`.
- The "`a` literally present" case is just `P` = a singleton. The rule also
  absorbs e.g. `AND(a,b) ∨ AND(a,b,c) → AND(a,b)`.

## The guard (the correctness core)

Absorption is NOT unconditionally sound under 3VL + errors. `a ∨ (a ∧ c)` with
`a = null`, `c = error`: the original evaluates to `error` (Or does not
short-circuit on null, so it reaches the erroring `And`), but the absorbed result
`a` is `null`. The error came from `c`, a dropped extra.

Confirmed gate (`rules.rs:639-646`): fire only when every id in the **dropped
extras** `inner-set(Q) \ inner-set(P)` has `could_error == false`. The retained
operand `P` is deliberately **not** gated (it stays in the result, its error still
surfaces). It is **not** an `a-not-null` guard (that needs a nullability analysis
we lack, deferred in the old engine too). This reuses the slice-3 `could_error`
analysis field, no new analysis or lattice addition.

## Machinery finding (surface #2, decided up front)

Inner-set subsumption is not expressible as a declarative cond+rest matcher. It
needs per-operand inner-set extraction, pairwise subset tests, and a
set-difference `could_error` gate. That is a **Rust helper** (the old engine's
`inner_sets` + search), hosted on the 6c `FilterSplice` / `rest_filters` scaffold.

The absorb relation (set-subsumption) is NOT the union-cancel relation (additive
inverse `X` + `Negate(X)`). They share the **scaffold** (a rest-filter backed by a
`&EGraph` helper), but NOT a single declarative pair-search `f`. So slice 6e
delivers a **generic cross-element rest-filter host** with absorb as the first
client. The union-cancel re-test (separate, later) plugs a second helper
(`CancelInverse`) into the same host. The honest answer to the readiness question:
the host generalizes, the per-rule structural search is bespoke Rust.

## Architecture

### Scaffold (6c, reused)

`apply_NAME_base(g: &mut EGraph)` for scalar (`colored: false`) rules compiles only
against the concrete base `EGraph`, so a `FilterSplice` helper may read the base
scalar analysis and `find` directly. `ListTmpl`/`TElem` is shared by `Tmpl::Union`
and `Tmpl::SVariadic`, so the new filter is grammar-general by construction.

### New machinery

1. **`RestFilter::AbsorbSubsumed { inner: String }`** in `dsl.rs` (`inner` = the
   inner/dual variadic-func keyword, `"and"` or `"or"`). The filter must carry the
   inner func because `listtmpl_stmts` does not see the enclosing `SVariadic` func.

2. **`Cond::AbsorbApplies { list: String, inner: String }`** in `dsl.rs`, the fire
   guard, so the rule never rebuilds an unchanged operand list. Fires iff a
   droppable `Q` with error-free extras exists (the guard and the filter share one
   core, so they cannot disagree).

3. **`rest_filters` additions** (`src/transform/src/eqsat/rest_filters.rs`):
   - `absorb_drop_index(g: &EGraph, ids: &[Id], inner: &VariadicFunc) -> Option<usize>`:
     the shared core. Ports `inner_sets` (per-operand inner-set as canonical,
     sorted, unique ids) + the deterministic first-`Q`-by-index subsumption search
     + the dropped-extras `could_error` gate. Returns the drop index or `None`.
   - `rest_absorb(g: &EGraph, ids: &[Id], inner: &VariadicFunc) -> Vec<Id>`: if
     `Some(q)`, return `ids` without index `q`, sorted by id (matching the old
     engine's `kept.sort()` for extraction-order parity with the oracle); else
     `ids.to_vec()`. The single-remaining-operand case is left to `and_single` /
     `or_single` downstream (as 6c drop_unit leaves collapse to those rules).
   - `could_error` via `g.data().scalar.analysis.get(&g.find(id)).map_or(false, |a| a.could_error)`
     (same read as `BaseView::scalar_could_error`).

4. **`MatchGraph::cond_absorb_applies(&self, ids: &[Id], inner: &VariadicFunc) -> bool`**:
   BaseView delegates to `absorb_drop_index(self.eg, ids, inner).is_some()`;
   ColoredView returns inert `false` (scalar rules never run colored). Required
   trait method, both impls swept. `is_color_exact = false`.

### Grammar surface (`.rewrite`)

- `absorb(xs, or)` in a template list → `FilterSplice { list: "xs", filter: AbsorbSubsumed { inner: "or" } }`.
- `absorb_applies(xs, or)` as a side condition → `Cond::AbsorbApplies { list: "xs", inner: "or" }`.

The `inner` keyword must be the dual of the enclosing outer func (`or` inside
`Variadic[and]`). A mismatch computes wrong inner-sets and the differential
oracle catches it. `FilterSplice` cannot see the outer func to assert this at
build time, so the explicit keyword plus the differential gate is the check.

### The two ported rules (`scalar.rewrite`, 22 → 24)

```
rule absorb_and { doc "And(.., a, .., Or(.., a, ..)) = And(.., a, ..), dropped extras error-free"
    Variadic[and](xs...) => Variadic[and](absorb(xs, or))  where absorb_applies(xs, or) }
rule absorb_or  { doc "Or(.., a, .., And(.., a, ..)) = Or(.., a, ..), dropped extras error-free"
    Variadic[or](xs...)  => Variadic[or](absorb(xs, and)) where absorb_applies(xs, and) }
```

Termination: each fire strictly shrinks the operand count, and the guard makes a
no-op fire impossible, so no ping-pong with fusion / single / empty /
short-circuit.

### Lean

The absorb RHS is a Rust-computed subsumption, not cleanly a syntactic list
function. The theorem it embodies is the two-valued **absorption law** (`P ∧ Q = P`
when `P ⟹ Q`, i.e. `inner-set(P) ⊆ inner-set(Q)`), which is a Boolean tautology and
invisible to the `could_error` guard (Lean's `denoteS` is two-valued, no error
model, so the guard is guard-vacuous in the `isnull_fold` sense).

Primary plan: render `AbsorbSubsumed` to a Lean list function capturing the
algebraic core and prove the fold-equality outright, with a fidelity NOTE (Rust
does general subsumption + `could_error` gate; Lean proves the Boolean algebra;
the 3VL/error soundness rests on the Rust analysis + differential oracle). Permanent
sorry count stays 5.

Fallback: if rendering the general subsumption to a provable Lean form is
intractable, treat the rule as builtin-opaque with a permanent sorry (count 5→6).
This MOVES the count, which is an overseer decision, so it is flagged before any
sorry is added, never silent. Do not sorry a provable theorem.

## Testing

- Differential parity `new_combined == old_scalar` over 6e + all prior, against the
  standalone `ScalarEGraph` oracle, no `--rewrite`. Adversarial corpus:
  - `a` inside `Or`/`And` at leading/middle/trailing; both `And(a, Or(a,b))` and
    `Or(a, And(a,b))` directions; extra operands (`And([a, Or(a,b), c]) → And([a, c])`);
    general subset (`AND(a,b) ∨ AND(a,b,c)`).
  - The guard case (highest risk): `a` null-typed with an erroring absorbed extra
    (`And(a, Or(a, (1/0)=(1/0)))`) must NOT fire. Mutation-test: disable the guard,
    confirm parity fails exactly there.
  - Error operands are Bool-typed (`(1/0)=(1/0)`, the old engine's
    `test_and_or_short_circuit_with_error` idiom), never bare `Int64` (the 6c
    ill-typed-collapse trap: `analysis.rs::merge` debug_assert on ill-typed
    variadic collapse).
  - Interactions: post-absorb `And([a]) → a` (`and_single`); nested absorption;
    absorb feeding short-circuit / drop_unit.
- Aggregate `lake build` green on a clean rebuild; two-sided sorry-taxonomy grep,
  permanent count == 5 (or 6 if the flagged fallback is taken). Both hand-run, no
  CI backstop (the Lean image does not build in CI, owner-confirmed, not to be
  wired).
- eqsat unit tests pass; relational goldens zero-diff, no `--rewrite`; production
  behavior neutral; termination converges.

## Out of scope

No 6b (`null_prop_variadic` / `err_prop_variadic`), 6d (`flatten_assoc`), 6f
(`factor_and_or`). The union-cancel re-test is a SEPARATE queued task, not part of
6e. No production reroute, no `ScalarEGraph` deletion (slice 7). No analysis-lattice
additions. `doc/developer/generated/` is read-only.
