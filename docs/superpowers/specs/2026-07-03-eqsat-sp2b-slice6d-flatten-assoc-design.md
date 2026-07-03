# SP2b Slice 6d: `flatten_assoc` (nested same-op middle-splice, all associative variadics) — Design

## Context

Part of SP2b (governing design:
`docs/superpowers/specs/2026-07-01-eqsat-sp2b-scalar-dsl-port-design.md`), porting
the standalone `ScalarEGraph` rules into the CombinedLang DSL so slice 7 can delete
the scalar engine. Slices 1-5, 6a, 6b, 6c, 6e shipped merge-ready. Slice 6d ports
`flatten_assoc`, the last matcher extension before 6f.

Branch: `claude/mir-equality-optimizer-sodbej`.

## Goal

Port `flatten_assoc` (`src/transform/src/eqsat/scalar/rules.rs:334`) into the DSL,
behavior-neutral against the standalone `ScalarEGraph` oracle.

## What the rule is (confirmed from the eval)

`AND(a, AND(b, c)) -> AND(a, b, c)`, and the same for every associative variadic.
One level per fire (saturation re-applies on the newly flat node for depth). Same
function only (`inner_func != func` is skipped). It fires on any
`func.is_associative()` variadic.

### Domain: five functions, not two

`is_associative() == true` for **And, Or, Coalesce, Greatest, Least**
(`src/expr/src/scalar/func/variadic.rs`). The old-engine rule is one generic
`is_associative` check covering all five. Porting only And/Or would diverge from the
standalone oracle on nested `Coalesce`/`Greatest`/`Least` (all common in SQL), a
silent behavior gap that blocks slice-7 retirement. So this slice ports **all five**,
as five per-function rules (the DSL has no variadic-func metavar, and adding one is
out-of-scope machinery the rule does not need).

### Unconditional, with two structural (non-semantic) guards

**UNCONDITIONAL**: no `could_error` gate, no nullability gate. Associativity is
order-independent over the And/Or semilattice including error handling (errors
accumulate via `max` and surface regardless of nesting), and the same reduce-parity
argument covers Coalesce/Greatest/Least (rules.rs:308-317). The eval body carries no
semantic guard.

It carries two guards that are about **termination**, not semantics, and both must be
ported exactly:

1. **Circular-reference skip** (`rules.rs:359-370`): skip an inner same-func node
   whose canonicalized children contain the parent class id
   (`inner_canons.contains(&canon)`). After `and_or_single` collapses `OR(c0)` into
   c0's class, c0's class holds OR-nodes pointing back at the parent. Splicing them
   would replace one `c0` with N copies, exponential across iterations. This is the
   ping-pong crux.
2. **Operand cap** `FLATTEN_MAX_OPERANDS = 4096` (`rules.rs:395`): decline the splice
   when the produced vector exceeds the cap. Defense-in-depth against a two-class
   transitive cycle the one-hop skip misses. Declining only means less flattening,
   never a wrong node.

Fires only when at least one operand is actually spliced, else `vec![]` (no no-op
union). In the DSL this becomes a fire guard `Cond` so the rule never rebuilds an
unchanged list.

## Machinery

The operation is a per-operand flat-map: for each operand, if its class holds a
non-circular same-func variadic node, replace the operand with that node's (canonical)
inner ids, else keep the operand. This slots onto 6c's `TElem::FilterSplice` as a new
`RestFilter` variant carrying the parent func keyword, exactly the shape of 6e's
`AbsorbSubsumed { inner }`.

### New DSL surface (`dsl.rs`)

1. **`RestFilter::FlattenSameFunc { func: String }`** (`func` = the parent variadic
   keyword, e.g. `"and"`). The filter carries the func because `listtmpl_stmts` does
   not see the enclosing `SVariadic` func.
2. **`Cond::FlattenApplies { list: String, func: String }`**, the fire guard. Fires
   iff a non-circular same-func operand exists (guard and filter share one core, so
   they cannot disagree).

### `rest_filters` additions

- `flatten_applies(g: &EGraph, ids: &[Id], func: &VariadicFunc) -> bool`: the shared
  core. True iff some operand's class holds a same-`func` `CallVariadic` node whose
  canonicalized children do not contain that operand's own canonical id (the
  circular-ref skip).
- `rest_flatten(g: &EGraph, ids: &[Id], func: &VariadicFunc) -> Vec<Id>`: for each
  operand, splice the first non-circular same-`func` node's canonical inner ids, else
  keep the operand. Caps the result at `FLATTEN_MAX_OPERANDS` (declining = return the
  input unchanged). Ports `rules.rs:343-402` exactly, including both guards.
- The `FLATTEN_MAX_OPERANDS` const is re-declared here (or shared) with the same value
  4096 and the same rationale comment.

### `MatchGraph::cond_flatten_applies`

BaseView delegates to `flatten_applies(self.eg, ids, func)`; ColoredView returns inert
`false` (scalar rules never run colored). Required trait method, both impls swept.
`is_color_exact = false` (it reads structure via `nodes`/`find`; mirror the
`HasDuplicateId` precedent, also structural).

### Codegen (`build/codegen.rs`)

- Emit `FlattenSameFunc` in `listtmpl_stmts` (call `rest_flatten(g, &ids,
  &variadic_func_value(func))`).
- Emit `FlattenApplies` cond (call `cond_flatten_applies`); `is_color_exact` arm.
- TElem/Cond serialization arms.
- **Extend `variadic_func_value`** with three keyword mappings: `coalesce`,
  `greatest`, `least` (currently only `and`/`or` are mapped;
  `codegen.rs` around line 190). The Rust construction mirrors And/Or:
  `mz_expr::VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce)`, etc. `cargo
  check` validates the exact paths.

### The five rules (`scalar.rewrite`, 26 -> 31)

```
rule flatten_and { doc "And(.., And(inner..), ..) = And(.., inner.., ..)"
    Variadic[and](xs...) => Variadic[and](flatten(xs, and)) where flatten_applies(xs, and) }
rule flatten_or  { doc "..." Variadic[or](xs...)  => Variadic[or](flatten(xs, or))   where flatten_applies(xs, or) }
rule flatten_coalesce { doc "..." Variadic[coalesce](xs...) => Variadic[coalesce](flatten(xs, coalesce)) where flatten_applies(xs, coalesce) }
rule flatten_greatest { doc "..." Variadic[greatest](xs...) => Variadic[greatest](flatten(xs, greatest)) where flatten_applies(xs, greatest) }
rule flatten_least    { doc "..." Variadic[least](xs...)    => Variadic[least](flatten(xs, least))       where flatten_applies(xs, least) }
```

`SCALAR_COMPILED_RULES` 26 -> 31; `COMPILED_RULES` unchanged.

Termination: each fire strictly reduces nesting depth (or is declined by the guards),
and the fire guard makes a no-op fire impossible, so the flatten -> short-circuit ->
drop_unit -> dedup -> single -> empty cascade converges.

## Lean

The Lean `ScalarExpr` model is two-valued Bool (`Semantics.lean:200`): it has
`andE`/`orE` but nothing for `Coalesce`/`Greatest`/`Least`, which return non-Bool
values (first-non-null, max, min) and cannot be faithfully denoted in a Bool model.

- **`flatten_and` / `flatten_or`**: render `FlattenSameFunc` to a Lean list-flatten
  helper and prove the obligation OUTRIGHT. `denoteS (andE (flatten xs)) = denoteS
  (andE xs)`: flattening a nested `andE ys` operand into `ys` preserves the `all`/`any`
  fold (via the existing `denoteSFold_and_eq_all` / `_or_eq_any` lemmas). No sorry.
- **`flatten_coalesce` / `flatten_greatest` / `flatten_least`**: outside the Bool
  fragment. Model them with a generic OPAQUE variadic constructor plus an opaque
  denotation (mirroring the existing `binaryE` / `denoteBin` opaque precedent): a
  small `VFunc` tag (coalesce/greatest/least), a
  `ScalarExpr` constructor carrying it, and `denoteS` deferring to an `opaque`
  function of the operand list. The flatten obligation over an opaque denotation is
  unprovable (the flattened and nested lists differ in length), so each is a
  **PERMANENT SORRY** (the same honest treatment as `const_fold` / the negate table).
- `scalar_variadic_ctor` (`lean.rs:550`) gains the three keyword arms mapping to the
  opaque constructor with the func tag. `choose_proof` emits the permanent sorry for
  the opaque-outer cases, keyed on the opaque constructor, and the outright proof for
  the `andE`/`orE` cases.
- **Permanent sorry count 7 -> 10** (three new). Bump `expected_permanent` in
  `ci/test/lean-mir-rewrite.sh` 7 -> 10 and update its comment. This is an overseer
  decision, taken deliberately: the alternative (a faithful non-Bool value domain) is
  a large model generalization, off-scope for this slice, and shoehorns non-Bool
  semantics into a deliberately-Bool model. The trip-wire bump and aggregate `lake
  build` are hand-run mandatory gates (the Lean image does not build in CI,
  owner-confirmed).

## Testing

- **Differential parity** `new_combined == old_scalar` over 6d + all prior, against the
  standalone `ScalarEGraph` oracle, no `--rewrite`. Adversarial corpus:
  - Nested same-op at leading/middle/trailing: `And([a, And([b,c]), d]) -> And([a,b,c,d])`.
    Both And and Or, and at least one of Coalesce/Greatest/Least (the new-domain proof).
  - Multiply/deeply nested: `And([And([And([a])])])`, `And([c0, And([c1, And([c2,c3])])])`
    -> fully flat, converges in bounded iters.
  - Flatten feeding the cascade: nested with a `false`/`true` (-> short-circuit), with
    an identity literal (-> drop_unit), with cross-boundary duplicates (-> dedup),
    collapsing to single/empty. Assert the combined result matches the oracle through
    the whole cascade.
  - **Termination / circular-ref**: a corpus case exercising the `and_or_single`
    self-loop shape (`OR(c0)` collapsed) that the circular-ref skip guards, asserting
    saturation converges (bounded iters, no exponential growth). Mutation-note: the
    skip is the ping-pong guard; a `flatten_terminates` test asserts iters <= a small
    bound.
  - **Mixed-op non-firing negative control**: `And([Or([a,b]), c])` must NOT flatten
    (different op). Assert `combined == scalar` (neither flattens).
  - Error operands are Bool-typed (`(1/0)=(1/0)`) inside And/Or nesting, never bare
    `Int64` (the 6c `analysis.rs::merge` ill-typed-collapse trap; flatten-then-collapse
    is exactly the variadic-collapse path that trips it). For Coalesce/Greatest/Least
    corpus cases, keep operands well-typed for that function.
- **Aggregate `lake build`** green on a clean rebuild; two-sided sorry-taxonomy grep,
  permanent count == 10. Both hand-run, no CI backstop.
- eqsat unit tests pass; relational goldens zero-diff, no `--rewrite`; production
  behavior neutral; termination converges.

## Out of scope

No 6f (`factor_and_or`). No union-cancel work. No variadic-func metavar axis (per-func
rules instead). No production reroute, no `ScalarEGraph` deletion (slice 7). No
analysis-lattice additions. `doc/developer/generated/` is read-only.
