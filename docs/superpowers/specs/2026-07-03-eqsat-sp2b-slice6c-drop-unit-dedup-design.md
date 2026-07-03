# SP2b Slice 6c: `and_or_drop_unit` + `and_or_dedup` (rest-splice-with-filter) — Design

## Context

Part of the SP2b effort to port the standalone `ScalarEGraph` rewrite rules into
the CombinedLang declarative DSL (governing design:
`docs/superpowers/specs/2026-07-01-eqsat-sp2b-scalar-dsl-port-design.md`), so the
combined `EGraph<CombinedLang>` subsumes the scalar engine and slice 7 can delete
`ScalarEGraph`. Slices 1–5 and 6a shipped merge-ready. Slice 6a added a
list-quantified *read* over a variadic's rest (`Cond::AnyScalarLit`, the
`rest_local` accessor). Slice 6c adds the matching *write* half: emitting a
filtered sublist of a variadic's operands as a new variadic node.

Governing branch: `claude/mir-equality-optimizer-sodbej`.

## Goal

Port two scalar rules from `src/transform/src/eqsat/scalar/rules.rs`, behavior
neutral against the standalone `ScalarEGraph` oracle:

- `and_or_drop_unit` (`rules.rs:270`): `And([.., true, ..]) => And([.. without true])`,
  `Or([.., false, ..]) => Or([.. without false])`. Remove operands equal to the
  connective's unit, keep the node with the filtered rest.
- `and_or_dedup` (`rules.rs:181`): `And([a, a, b]) => And([a, b])`, `Or` dual.
  Remove operands that duplicate an earlier operand by canonical e-class id, keep
  the first occurrence.

## The scope decision: both rules stay in 6c

The prompt flagged that `dedup` might require the cross-element pair-search
machinery that 6e (`absorb_and_or`) and the queued relational union-cancel need.
Reading the old engine settles it: it does not.

- `and_or_dedup` (`rules.rs:181`) compares operands by **canonical e-class id
  equality** (`eg.find(e)` into a `HashSet`), keeping the first occurrence. That
  is a stateful *filter* over the operand list (a seen-set), linear, no
  structural matching.
- `absorb_and_or` / union-cancel need to match one operand against a *transform*
  of another (`a` vs `And(a, b)`, or `X` vs `Negate(X)`). That is structural
  cross-element pairing, a genuinely different and heavier primitive.

So `dedup` fits the same "rest-splice-with-filter" family as `drop_unit`,
differing only in the per-element filter (a seen-set of canonical ids vs a
scalar-literal predicate). Both stay in 6c. 6c therefore delivers the
*rest-emit* half of the union-cancel machinery (build a computed sublist as a new
variadic node) but **not** the structural-pair-search half. That remains 6e /
union-cancel future work. This is recorded so the queued union-cancel re-test
knows exactly what 6c did and did not unblock.

## could_error: both rules are unconditional

Verified from the evaluator and the old engine, not from comments:

- `drop_unit` removes operands whose analysis literal is the connective unit
  (`true` for And, `false` for Or). A literal `true`/`false` cannot error, and
  it is the And/Or identity, so removing it preserves evaluation for all inputs
  including error-bearing operands. `rules.rs:270` carries no `could_error` guard.
- `dedup` removes an operand that is e-class-identical to a kept operand. The
  kept copy denotes exactly the same value or error, so multiplicity is
  irrelevant to And/Or and to error-as-data. `And([1/0, 1/0]) => And([1/0])`
  raises the same single error. `rules.rs:181` carries no guard.

Both rules are ported unconditional. A `could_error` guard on either would be a
defect (it would suppress legitimate firings), matching the 6a finding for
`and_or_short_circuit`.

## Architecture

### The emit boundary (why this is bounded)

`apply_NAME_base(g: &mut EGraph, ..)` (build/codegen.rs) takes the concrete base
`EGraph`. `colored_apply` reuses a rule's apply body only for `colored: true`
rules. Scalar rules are `colored: false` (build-enforced), so their RHS body
compiles and runs only against the base `EGraph`, where `g.find` and the base
scalar analysis are directly available. No `ApplyGraph` trait change is needed
for 6c.

### New machinery

1. **`TElem::FilterSplice { list: String, filter: RestFilter }`** in `dsl.rs`, a
   third splice kind alongside `Splice` and `MapSplice` in the shared `ListTmpl`.
   `ListTmpl` backs both `Tmpl::Union` (relational) and `Tmpl::SVariadic`
   (scalar), so the filtered splice is grammar-general over CombinedLang by
   construction.

2. **`enum RestFilter { DropScalarLit(bool), DedupById }`** in `dsl.rs`.
   - `DedupById` is grammar-general: it filters by canonical id only
     (`g.find`), sort-agnostic, reusable for a relational `Union(dedup(xs))`.
   - `DropScalarLit(bool)` is the sanctioned scalar-specific per-element
     predicate: keep operands whose scalar-literal analysis is not the given
     boolean.

3. **Rest-filter helpers** in a new hand-written module
   `src/transform/src/eqsat/rest_filters.rs`:
   - `rest_dedup_by_id(g: &EGraph, ids: &[Id]) -> Vec<Id>` — first-occurrence
     dedup by `g.find`. Grammar-general (sort-agnostic).
   - `rest_drop_scalar_lit(g: &EGraph, ids: &[Id], value: bool) -> Vec<Id>` —
     keep operands whose base scalar-literal analysis is not `Some(Some(value))`.
   Both take `&EGraph` (base). Scalar rules are base-only, so this is sound and
   keeps the generated code a one-line call. Grammar-generality note for the
   union-cancel re-test: `rest_dedup_by_id` is sort-agnostic and usable in a
   base/`colored: false` relational rule today. Colored relational reuse would
   additionally need an `ApplyGraph::canonical` method. That is a small future
   add, out of 6c scope, flagged here so it is not rediscovered.

4. **`Cond::HasDuplicateId { list: String }`** in `dsl.rs`, the dedup fire-guard
   (fire only when at least one duplicate is present, to avoid a wasteful no-op
   union rebuild each saturation round). Backed by `MatchGraph::cond_has_duplicate_id`
   (BaseView: real, via `find`; ColoredView: inert `false`, matching the
   analysis-gated cond pattern). `is_color_exact = false`. The `drop_unit` rules
   need no new cond: they reuse 6a's `scalar_any_lit_true` / `scalar_any_lit_false`
   as the fire-guard (fire iff a unit literal is present).

### Grammar surface (`.rewrite`)

- `dedup(xs)` in a template list → `TElem::FilterSplice { list: "xs", filter: DedupById }`.
- `drop_scalar_lit(xs, true)` / `drop_scalar_lit(xs, false)` → `FilterSplice { list, filter: DropScalarLit(bool) }`.
- `has_duplicate_id(xs)` as a side condition → `Cond::HasDuplicateId { list: "xs" }`
  (added to the nested scalar-cond sub-`choice`, per the 6a chumsky 26-tuple note).

### The four ported rules (`scalar.rewrite`, 18 → 22)

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

Post-filter a node may become single-operand (`and_single` / `or_single` collapse
it to the operand) or empty (`and_empty` / `or_empty` collapse it to the unit
literal) on a later saturation round. Each fire strictly shrinks the operand
list, and the fire-guard makes a no-op fire impossible, so no ping-pong with
fusion / single / empty.

### Lean

Declarative, no builtin, so the permanent-sorry count stays 5. Two helper
lemmas, proved outright by induction, model the *algebraic core*:

- drop-unit: dropping syntactic `litB unit` from an And/Or fold preserves the
  fold (unit is the And/Or identity).
- dedup: removing a later syntactic duplicate from an And/Or fold preserves the
  fold (And/Or idempotence over `Bool`).

`lean.rs` renders `FilterSplice` in `tmpl_list_expr` (`List.filter (· ≠ litB unit) list`
for `DropScalarLit`, a dedup over the list for `DedupById`) and adds
`choose_proof` arms for the four rules.

Fidelity note (same stance as slice-5 `isnull_fold`): Lean models the *syntactic*
filter (`litB unit`; AST-duplicate), while the Rust rules use the *semantic*
predicate (analysis literal; canonical e-class id), which is strictly stronger.
Lean proves the algebra. The soundness of the semantic lift (analysis literal is
correct; equal e-classes denote equally) rests on the analysis and the
differential oracle, not on Lean. If the dedup fold-invariance proof proves
intractable outright, that is surfaced before any permanent-sorry bump (the count
must not silently move).

## Testing

- Differential parity: `new_combined == old_scalar` over the 6c rules plus all
  prior, against the standalone `ScalarEGraph` oracle. Adversarial corpus: unit
  literals leading/middle/trailing; duplicates adjacent, non-adjacent, >2 copies;
  errors (`And([1/0, 1/0]) => And([1/0])`, `And([1/0, true]) => And([1/0])`);
  nulls (3VL); interaction cascades (`And([x, true]) => And([x]) => x`,
  `And([true]) => And([]) => true`, `And([a, a]) => And([a]) => a`).
- Aggregate `lake build` green on a clean rebuild; sorry-taxonomy grep at 5,
  two-sided (both hand-run, no CI backstop for the Lean image).
- eqsat unit tests pass; relational goldens zero-diff, no `--rewrite`.
- Production behavior neutral (passes on, no golden change); termination
  converges.

## Out of scope

No 6b (`null_prop_variadic` / `err_prop_variadic`), 6d (`flatten_assoc`), 6f
(`factor_and_or`), 6e (`absorb_and_or`). No `ApplyGraph::canonical` (colored
reuse is future). No production reroute, no `ScalarEGraph` deletion (slice 7). No
analysis-lattice additions. `doc/developer/generated/` is read-only.
