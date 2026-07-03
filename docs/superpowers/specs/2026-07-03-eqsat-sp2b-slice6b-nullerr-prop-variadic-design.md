# SP2b Slice 6b: `null_prop_variadic` + `err_prop_variadic` (null-propagating variadic builtins) — Design

## Context

Part of SP2b (governing design:
`docs/superpowers/specs/2026-07-01-eqsat-sp2b-scalar-dsl-port-design.md`), porting
the standalone `ScalarEGraph` rules into the CombinedLang DSL so slice 7 can delete
the scalar engine. Slices 1-5, 6a, 6c, 6e shipped merge-ready. Slice 6b ports the
two variadic null/error-propagation rules, the variadic analogues of slice-5's
binary `null_prop_binary` / `err_prop_binary`.

Branch: `claude/mir-equality-optimizer-sodbej`.

## Goal

Port `null_prop_variadic` (`src/transform/src/eqsat/scalar/rules.rs:1013`) and
`err_prop_variadic` (`rules.rs:1085`) into the DSL as Rust builtins, behavior-neutral
against the standalone `ScalarEGraph` oracle.

## What the rules actually are (confirmed from the eval, not the framing)

The slice was framed as "the variadic And/Or analogues" with an "absorbing-literal"
gate and a 6a short-circuit interaction. **That framing is a misread.** Both rules
open with `if !func.propagates_nulls() { return vec![]; }`, and `VariadicFunc::And`
and `VariadicFunc::Or` both return `propagates_nulls() == false`
(`src/expr/src/scalar/func/variadic.rs:105`, `:1178`). So neither rule ever fires on
And/Or. The old-engine test `test_null_prop_variadic_fires_on_safe_operands`
(`rules.rs:2626`) asserts `make_timestamp().propagates_nulls()` and exercises the
rule on `MakeTimestamp`. The real domain is **null-propagating variadic functions**
(MakeTimestamp-class), not the boolean connectives.

Consequences:
- There is **no absorbing-literal gate**. And/Or null/error folding is handled
  entirely by short-circuit (6a) plus drop_unit/single (6c), already shipped and
  untouched by 6b.
- There is **no 6a interaction**. The two rules and the boolean-connective rules
  operate on disjoint function sets.

### `null_prop_variadic` (`rules.rs:1013`)

Fires on a `CallVariadic { func, exprs }` when:
1. `func.propagates_nulls()`, and
2. some operand `is_literal_null`, and
3. every operand that is **not** a literal null has `could_error == false`.

Result: a typed null literal, `literal_null(call_scalar_type(node))`.

### `err_prop_variadic` (`rules.rs:1085`)

Fires on a `CallVariadic { func, exprs }` when:
1. `func.propagates_nulls()`, and
2. some operand is a literal error (`literal_err`), taking the **first** such by
   iteration order, and
3. every **other** operand (excluding literal-error operands via
   `literal_err(..).is_none()`) has `could_error == false`.

Result: the first error literal, `literal(Err(err), call_scalar_type(node))`. A
second literal-error operand does not block the fire (it is excluded from the "other"
set); the first error is the one left-to-right eval surfaces, so this is sound
(`rules.rs:1096-1102`).

## Null-vs-error priority (the envelope crux, re-homed)

For a null-propagating variadic with both a literal null and an erroring operand and
no other blocker: **error wins.** `makets(null, 1/c0)` at `c0 = 0` evaluates to the
division error, not null. The mechanism is the `could_error` gate:
`null_prop_variadic` is **blocked** because the `1/c0` operand can error, and
`err_prop_variadic` (for a literal error) or plain evaluation surfaces the error.
This is proven live by `test_null_prop_variadic_blocked_when_other_can_error`
(`rules.rs:2663`) and the probe comment at `rules.rs:1010`
(`eval(AddInt64(null, 1/0)) == Err(DivisionByZero)`).

## Builtin vs declarative + permanent-count delta (confirmed)

Both rules are **builtins**, not declarative rewrites. Each needs the `could_error`
analysis gate plus construction of a typed null/error literal from
`call_scalar_type`, neither of which is a syntactic template. This mirrors slice-5's
binary `null_prop_binary` / `err_prop_binary`, which are builtins in
`scalar_builtins.rs` (`:235`, `:278`) with DSL form `Scalar(e) => NAME(e)` and a
`-- PERMANENT SORRY: RHS is a Rust builtin` Lean obligation each.

Permanent sorry count moves **5 → 7** (two new builtin RHS obligations). The Lean
emitter keys the permanent sorry on any `Tmpl::Builtin` RHS generically
(`lean.rs:795`), so the two markers appear automatically on regeneration. The manual
step is bumping `expected_permanent` in `ci/test/lean-mir-rewrite.sh:55` from 5 to 7
and updating its comment. This is the first count move since slice 5 and has no CI
backstop (the Lean image does not build in CI, owner-confirmed), so the trip-wire
bump and aggregate `lake build` are hand-run mandatory gates.

## Architecture

The builtin dispatch is fully generic. `Tmpl::Builtin { name, args }` emits
`crate::eqsat::scalar_builtins::{name}({args})?` (`build/codegen.rs:1085`), and the
grammar parses any `name(args)` builtin call (`build/grammar.rs:652`). So no grammar,
codegen, DSL-AST, or dispatch-registry change is needed. The slice adds:

1. **`scalar_builtins::null_prop_variadic(g: &mut EGraph, class: Id) -> Result<Id, String>`**
   and **`err_prop_variadic(g, class)`**, mirroring `rules.rs:1013` / `:1085` exactly.
   Both reuse the existing module helpers the binary builtins use:
   `scalar_class_nodes`, `is_literal_null`, `literal_err`, `scalar_could_error`,
   `call_scalar_type`. The `CallVariadic` operand scan replaces the binary
   `CallBinary { expr1, expr2 }` destructure. On no eligible node, return `Err(..)`
   (same contract as the binary builtins).

2. **Two DSL rules** in `rules/scalar.rewrite`:
   ```
   rule null_prop_variadic {
       doc "f(.., null, ..) = null when f propagates nulls and every other operand cannot error"
       Scalar(e) => null_prop_variadic(e)
   }
   rule err_prop_variadic {
       doc "f(.., err, ..) = err when f propagates nulls and every other operand cannot error"
       Scalar(e) => err_prop_variadic(e)
   }
   ```
   `SCALAR_COMPILED_RULES` 24 → 26; `COMPILED_RULES` unchanged.

3. **Lean**: regenerate `Generated.lean` (two new permanent-sorry obligations,
   count 5 → 7), bump `expected_permanent` 5 → 7 in the trip-wire. No hand-authored
   Lean proof (builtin RHS is opaque, permanently sorried, same as the binary pair).

Both rules stay `colored: false`. Their apply bodies compile only against the base
`EGraph` (the builtin takes `&mut EGraph`).

## Testing

- **Unit tests** in `scalar_builtins.rs`, mirroring the binary builtins' tests
  (`null_prop_binary_folds_null_operand_with_safe_other` etc.): the variadic builtin
  fires on `makets(null, safe..)`, is blocked when another operand can error, errs on
  an inapplicable shape, and errs on a non-null-propagating variadic.
- **Differential parity** `new_combined == old_scalar` over 6b + all prior, against
  the standalone `ScalarEGraph` oracle, no `--rewrite`. Adversarial corpus:
  - null at leading/middle/trailing in a null-propagating variadic (`MakeTimestamp`),
    every other operand safe → folds to typed null.
  - literal error at leading/middle/trailing → folds to that error.
  - **null-vs-error priority**: `makets(null, 1/0)` (Bool-typed error via the
    `(1/0)=(1/0)` idiom where the operand type must be Bool; MakeTimestamp operands
    are Int64/F64 so a bare `1/0` Int64 literal-error is correct and well-typed here,
    since the collapse target is the variadic's own type, not a Bool connective) —
    must yield the error, not null. Both `null_prop` blocked and `err_prop` fires.
  - non-null-propagating variadic with a null/error operand → neither rule fires
    (call left intact).
  - **And/Or negative controls**: `And([null, true])`, `Or([false, 1/0])` and similar
    — assert `combined == scalar`, confirming neither engine 6b-folds them (And/Or
    handling stays short-circuit + drop_unit/single). This answers the re-homed
    "6a-interaction" concern honestly: the rules provably do not touch And/Or.
  - **Guard mutation-test** (highest-risk): disable the `could_error` gate in
    `null_prop_variadic`, confirm parity fails **exactly** on the `makets(null, 1/0)`
    case (wrongly folds to null instead of surfacing the error), mirroring
    `rules.rs:2663`. Restore the gate byte-identical.
- **Aggregate `lake build`** green on a clean rebuild; two-sided sorry-taxonomy grep,
  permanent count == 7 after the trip-wire bump. Both hand-run, no CI backstop.
- eqsat unit tests pass; relational goldens zero-diff, no `--rewrite`; production
  behavior neutral; termination converges (a folded null/error literal is not
  re-foldable, no ping-pong with short-circuit / single / empty / drop_unit).

### Error-operand typing note

The 6c/6e ill-typed-collapse trap (`analysis.rs::merge` debug_assert on a variadic
collapsing to an operand whose type conflicts with the variadic's Bool type) is
specific to the **boolean** connectives And/Or, whose result type is Bool. For a
`MakeTimestamp` err-prop, the collapse target is the variadic's own (Timestamp)
type and the error literal is built at that type via `call_scalar_type`, so a bare
`1/0` Int64 operand does not reproduce the trap. The implementer confirms typing per
`call_scalar_type` and keeps error operands well-typed for the chosen variadic.

## Out of scope

No 6d (`flatten_assoc`), 6f (`factor_and_or`). No union-cancel work. No production
reroute, no `ScalarEGraph` deletion (slice 7). No analysis-lattice additions
(slice-3 freeze). `doc/developer/generated/` is read-only.
