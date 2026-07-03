# SP2b Slice 6f: `factor_and_or` (dual-connective distributive factoring, builtin) — Design

## Context

Part of SP2b (governing design:
`docs/superpowers/specs/2026-07-01-eqsat-sp2b-scalar-dsl-port-design.md`), porting
the standalone `ScalarEGraph` rules into the CombinedLang DSL so slice 7 can delete
the scalar engine. Slices 1-5, 6a-6e shipped merge-ready. Slice 6f is the LAST scalar
rule. When it lands, all 20 scalar rules are ported and slice 7 (production reroute +
`ScalarEGraph` deletion) is next.

Branch: `claude/mir-equality-optimizer-sodbej`. Scope is LOCKED by a read-first recon:
builtin route (Option 1), no re-litigation.

## Goal

Port `factor_and_or` (`src/transform/src/eqsat/scalar/rules.rs:478`) into the DSL as a
Rust builtin, behavior-neutral against the standalone `ScalarEGraph` oracle.

## What the rule is (confirmed from the eval, recon)

Dual-connective distributive factoring, factor OUT the common subterm:

```
undistribute-OR  (outer Or, inner And):  (a∧b)∨(a∧c) → a∧(b∨c)
undistribute-AND (outer And, inner Or):  (a∨b)∧(a∨c) → a∨(b∧c)
```

- **Dual-connective** (confirmed from logic, the 6d comment mislabel was wrong): the
  outer connective's operands are the DUAL connective's calls. `inner_func =
  outer_func.switch_and_or()` (rules.rs:493).
- **Full intersection, n-ary, one step**: the factor is the inner-set intersection
  common to EVERY branch (rules.rs:499-509); residual `i` is branch `i`'s remaining
  inner operands. Subset factoring is deferred (rules.rs:475), so a branch that does
  not carry the full intersection blocks the fire.
- **Result shape**: `inner_func(factor.., outer_func(residual_1, .., residual_n))`,
  where each `residual_i` is a singleton id or an `inner_func` call of its residual
  operands (rules.rs:539-564).

### Two fire conditions beyond a non-empty intersection

1. **Non-empty residual per branch** (rules.rs:519-524): an empty residual is the
   absorption case (a branch that WAS exactly the intersection); it belongs to 6e's
   absorb rule, not here. Producing it would build an empty inner call and reintroduce
   the CLU-137 ungated-absorption bug.
2. **Residual-error gate** (rules.rs:528-534): fire only when EVERY residual operand
   has `could_error == false`. The common factor MAY error and is deliberately NOT
   gated (it is never dropped or relocated past a masking value). This is the CLU-137
   narrowing of reduce's over-broad whole-expression `could_error`. Proven by the
   old-engine tests `test_factor_and_or_erroring_common_factor` (erroring factor
   fires) and `test_factor_and_or_gate_blocks_erroring_residual` (erroring residual
   blocks).

Fires on any `is_and_or` node with `>= 2` operands (rules.rs:486-491). It is
non-destructive: it unions the factored form into the source class, so extraction
picks the cheaper form (the factored tree has fewer e-nodes when it fires).

## Recon verdict (confirm during implementation, recon was read-only)

- **Fork 4 (termination/blowup): benign.** No cap, no circular-ref guard in the old
  engine, because it is non-destructive fire-once. There is NO distribute-IN reverse
  rule anywhere (grep-confirmed), so no factor<->distribute ping-pong. The factored
  form does not re-trigger factoring (its nested dual-call's operands are not
  themselves dual-calls sharing a factor). A saturation termination test must confirm
  bounded convergence with the flatten/absorb/short-circuit cascade. If it needs a
  fire-once guard to stay bounded, port the old engine's (it has none).
- **Fork 5 (neutral-portability): portable.** Both engines are e-graphs sharing the
  same scalar extraction/cost, so both hold factored+unfactored as equal and extract
  the same min-cost form. This is NOT a batch-1 directional-vs-saturation case. The
  differential parity test is the proof. If saturation's bidirectionality causes a
  canonical-form divergence the differential catches, STOP and report (that would be
  the slice-7 blocker the recon judged absent).

## Architecture

### One builtin, not two

The old engine is one `factor_and_or` fn handling both connectives via `is_and_or`.
The builtin mirrors it: ONE `scalar_builtins::factor_and_or(g, class)`, wired by ONE
rule `Scalar(e) => factor_and_or(e)`, exactly like `null_prop_variadic`
(`scalar.rewrite`, `Scalar(e) => null_prop_variadic(e)`). This is the count-correct
design: each `Tmpl::Builtin` RHS generates one Lean theorem and one permanent sorry,
so one builtin gives the locked **permanent 10 -> 11**. Two builtins would be two
sorries (12), contradicting the locked count and the "port the old engine exactly"
directive. (The scoping answer explicitly allowed "one shared" opaque.)

### The builtin (`scalar_builtins.rs`)

`pub fn factor_and_or(g: &mut EGraph, class: Id) -> Result<Id, String>`, porting
`rules.rs:478-565` EXACTLY:
1. Find a `CallVariadic { func, exprs }` node in `class` with `is_and_or(func)` and
   `exprs.len() >= 2`; else `Err`.
2. `inner_func = func.switch_and_or()`.
3. `inner_sets` over the operands (port `rules.rs:414-433`: each operand's `inner_func`
   node's canonical/sorted/unique ids, or a singleton `{find(operand)}`). Reuse the
   `&EGraph` node-scan idiom already in `rest_filters::inner_set` / the module helpers.
4. Full intersection across all branch-sets; if empty, `Err`.
5. Per-branch residuals (inner operands not in the intersection); if any residual is
   empty, `Err` (absorption case).
6. Residual-error gate: if any residual operand `scalar_could_error`, `Err`.
7. Build the 2-level tree via `g.add(CNode::Scalar(SNode::CallVariadic { .. }))`: per
   branch a residual node (singleton passthrough or `inner_func` call), `branch_ids`
   sorted for hashcons stability, `residual_combination = outer_func(branch_ids)`,
   `factored = inner_func(intersection ++ [residual_combination])`. Return its id.

Reuses existing module helpers (`scalar_class_nodes`, `scalar_could_error`); no new
analysis, no lattice addition. `VariadicFunc::switch_and_or()` and `is_and_or` come
from `mz_expr` / a local `is_and_or` mirror.

### The rule (`scalar.rewrite`, 31 -> 32)

```
rule factor_and_or {
    doc "(a∧b)∨(a∧c) = a∧(b∨c) and dual: undistribute a common factor, residual-error gated"
    Scalar(e) => factor_and_or(e)
}
```

`SCALAR_COMPILED_RULES` 31 -> 32; `COMPILED_RULES` unchanged (37).

Termination: non-destructive fire-once; the factored form does not re-factor, and there
is no distribute-in rule, so the flatten/absorb/short-circuit cascade converges.

### Lean (the three banked refinements, non-negotiable)

1. **Own explicitly named opaque decl** `opaque factorAndOr : ScalarExpr -> ScalarExpr`
   in `Semantics.lean` (mirroring `const_eval`'s `constEval`), plus a
   `translate_tmpl` arm `"factor_and_or" => format!("factorAndOr {}", args[0])`
   (lean.rs:534 match). Explicitly NOT routed through 6d's `variadicOpaqueE`: factor's
   LHS renders to a plain var `e` (not `variadicOpaqueE`), so it never hits that arm,
   and its own named opaque is matched, never silently absorbed.
2. **Third-category sorry comment.** `choose_proof` gets a factor-specific arm placed
   BEFORE the generic `is_builtin_rhs` arm (lean.rs:826), keyed on the rendered RHS
   (`rhs.contains("factorAndOr")`), returning verbatim:
   ```
   -- PERMANENT SORRY: distributivity IS provable in the Bool model; this sorry is a
   -- representation artifact of the builtin RHS (not declaratively expressed),
   -- dischargeable by declarativizing. NOT opaque-computation (const_fold,
   -- eval-dependent) NOR outside-value-domain (6d non-Bool flatten).
   ```
   This is the THIRD sorry category: provable-in-Bool-model-but-builtin-shaped, the
   least-satisfying sorry of the port. The honesty about why is the point.
3. **Permanent 10 -> 11**, two-sided trip-wire (`ci/test/lean-mir-rewrite.sh`
   `expected_permanent` 10 -> 11, comment extended). Hand-verified: Docker `lake build`
   exit 0, `PERMANENT SORRY` marker count == 11. No CI backstop (owner-confirmed).

Verify in the regen: exactly ONE new obligation `rule_factor_and_or`, carrying the
third-category comment (NOT the generic "RHS is a Rust builtin" text and NOT the
`variadicOpaqueE` "non-Bool variadic" text).

## Testing

- **Differential parity** `new_combined == old_scalar` over 6f + all prior, against the
  standalone `ScalarEGraph` oracle, no `--rewrite`. Adversarial corpus:
  - Fires: `Or(And(a,b), And(a,c)) -> And(a, Or(b,c))` and the dual
    `And(Or(a,b), Or(a,c)) -> Or(a, And(b,c))`; n-ary (3+ branches sharing a factor);
    common factor at various positions inside the inner nodes.
  - Does NOT fire: no common factor; partial (only some branches share it -> full
    intersection empty or a branch has empty residual, match the old engine exactly);
    mixed inner connectives; single branch.
  - Residual-error gate: erroring residual blocks (unsound factored form must NOT be
    the extraction); erroring common factor still fires (CLU-137). Bool-typed errors.
  - Error operands Bool-typed `(1/0)=(1/0)`, never bare `Int64` (the 6c
    `analysis.rs::merge` ill-typed-collapse trap; factor builds/collapses variadics).
  - Interaction cascade: factor feeding / fed-by flatten, short-circuit, drop_unit,
    dedup, absorb, single, empty -> converges to the same as the old engine.
- **Termination under saturation** (fork 4): a dedicated test over factorable nesting
  converges in bounded iters (mirror `flatten_terminates` / `absorb_terminates`);
  confirm fire-once, no re-fire on own output, no flatten ping-pong.
- **Aggregate `lake build`** green on a clean rebuild; two-sided sorry-taxonomy grep,
  permanent count == 11. Both hand-run.
- eqsat unit tests pass; relational goldens zero-diff, no `--rewrite`; production
  behavior neutral.

## Out of scope

No slice 7 (production reroute, `ScalarEGraph` deletion). No union-cancel / colored-half
host. No subset factoring or the "factor cannot be null" gate disjunct (deferred in the
old engine too). The func/op-metavar equality-guard prereq is CLOSED-MOOT (factor is a
builtin, binds no metavar, and is the last rule). No analysis-lattice additions.
`doc/developer/generated/` is read-only.
