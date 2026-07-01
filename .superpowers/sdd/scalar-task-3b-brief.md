# Task 3b: flag + callback wiring into canonicalize_predicates

Phase 3 of the scalar equality-saturation canonicalizer. Wire the canonicalizer
into the optimizer behind a feature flag, replacing the per-predicate `reduce`
inside `canonicalize_predicates`. The canonicalizer lives in `mz-transform`
(`crate::eqsat::scalar::canonicalize(&MirScalarExpr, &[ReprColumnType]) ->
MirScalarExpr`); `canonicalize_predicates` lives in `mz-expr`
(`src/expr/src/relation/canonicalize.rs:216`), which CANNOT depend on
`mz-transform` (it is only a dev-dependency). So the canonicalizer is injected as
a CALLBACK from the `mz-transform` callers.

This is a cross-crate change (mz-repr, mz-sql, mz-expr, mz-transform). Default
behavior (flag off) MUST be byte-identical to today.

## 1. Feature flag (production off, test/CI on)

Add `enable_eqsat_scalar_canonicalize` to the `feature_flags!` macro in
`src/sql/src/session/vars/definitions.rs` (alongside `enable_eqsat_optimizer`,
~line 2091+):
```
{
    name: enable_eqsat_scalar_canonicalize,
    desc: "use the equality-saturation scalar canonicalizer in place of reduce in canonicalize_predicates",
    default: false,
    enable_for_item_parsing: false,
},
```
`default: false` keeps production OFF. `enable_all_feature_flags_by_default()`
(definitions.rs:1769) iterates ALL flags and sets them on, so this flag is
automatically ON in the test/CI path that calls it (sqllogictest/testdrive),
giving the new path coverage. Confirm that mechanism is what the slt harness uses
(grep for `enable_all_feature_flags_by_default`); if the harness uses a different
default-on path, follow it. The point (user directive): production default off,
test coverage on.

## 2. OptimizerFeatures plumbing

`src/repr/src/optimize.rs`: add `enable_eqsat_scalar_canonicalize: bool` to the
`OptimizerFeatures` struct (mirror `enable_eqsat_optimizer` at ~line 104,
including the `// Bound from SystemVars::...` comment).

`src/sql/src/session/vars/definitions.rs`: bind it where `OptimizerFeatures` is
constructed from `SystemVars` (mirror `enable_eqsat_optimizer` at the three sites
~2336, ~2384, ~2418: the struct construction, the `set_var!`/override plumbing,
etc.). Follow `enable_eqsat_optimizer` EXACTLY as the template.

## 3. The injection point in mz-expr

`canonicalize_predicates` (`canonicalize.rs:216`) step 1 currently does
`predicates.iter_mut().for_each(|p| p.reduce(repr_column_types))` (:230). Steps 2+
rely on `reduce` having flattened nested ANDs.

Add an injected scalar canonicalizer. Preferred shape to MINIMIZE churn to
unrelated callers: keep the existing
`canonicalize_predicates(predicates, repr_column_types)` signature as a thin
wrapper that calls a new
`canonicalize_predicates_with(predicates, repr_column_types, scalar_canon)` with
`scalar_canon = None`. The `_with` form takes
`scalar_canon: Option<&dyn Fn(&mut MirScalarExpr, &[ReprColumnType])>`. In step 1:
```
match scalar_canon {
    Some(f) => predicates.iter_mut().for_each(|p| f(p, repr_column_types)),
    None => predicates.iter_mut().for_each(|p| p.reduce(repr_column_types)),
}
```
Everything else in `canonicalize_predicates` is unchanged. (Our flatten rule makes
the eqsat output a flat conjunction, so step 2's split still works.) Choose the
wrapper approach unless you find a cleaner one that keeps all existing
non-flag-aware callers unchanged; do NOT change the behavior of the `None`/reduce
path.

## 4. mz-transform callers supply the callback when the flag is on

The eqsat callback is:
```
let scalar_canon = |e: &mut MirScalarExpr, ct: &[ReprColumnType]| {
    *e = crate::eqsat::scalar::canonicalize(e, ct);
};
```
Wire it at the call sites that have (or can get) `ctx.features`, passing
`Some(&scalar_canon)` iff `ctx.features.enable_eqsat_scalar_canonicalize`, else
`None` (or just call the plain wrapper). Primary path for CLU-137 is
CanonicalizeMfp -> `fusion::filter` -> `canonicalize_predicates`:

* `src/transform/src/fusion/filter.rs`: `Filter::action` (static, NO ctx, calls
  canonicalize_predicates at :96). Thread the flag down from `Filter::transform`
  (which has `&mut TransformCtx`) into `action` (add a parameter, e.g. a
  `bool enable_eqsat_scalar` or the `Option<&dyn Fn>` callback). Update
  `Filter::transform` to read `ctx.features.enable_eqsat_scalar_canonicalize` and
  pass it. Update any other callers of `Filter::action` (grep) to pass the
  default (false / None).
* `src/transform/src/predicate_pushdown.rs:846`: this call is inside the
  predicate_pushdown transform, which has ctx. Thread the flag to its call site
  similarly.

SCOPE: wire the `canonicalize_predicates` call sites in mz-transform
(fusion::filter, predicate_pushdown, and the mz-transform `literal_constraints`
caller at literal_constraints.rs:628 if it routes through the mz-expr function).
Do NOT wire the OTHER reduce calls in canonicalize.rs (:437, :87) in this task,
note them as a possible later widening. The goal here is the canonicalize_predicates
path (the CLU-137 path) plus broad coverage from the flag being on in CI.

If threading the flag into `Filter::action` is messy (it is called from several
places), prefer passing a small `bool` and constructing the closure inside
`action`, OR passing the `Option<&dyn Fn>`; choose the cleaner one and keep the
flag-off path identical.

## 5. Soundness / no-op-when-off

* Flag OFF: every path takes the `reduce` branch, byte-identical to today. NO
  golden, slt, or unit test should change with the flag off. This is the
  acceptance bar for the default.
* Flag ON: predicates are canonicalized by the eqsat engine. Some optimizer
  goldens MAY change (that is expected and is what 3c diffs). Do NOT regenerate
  goldens in THIS task; 3b only wires + proves the flag-off path is unchanged and
  the flag-on path compiles and runs.

## Tests

* `cargo check` for the whole workspace must pass (the signature change ripples;
  fix all call sites). Consult the `mz-test` skill for build/test commands.
* `cargo clippy` clean for touched crates; `bin/fmt` applied.
* Flag-off acceptance: run the mz-transform test suite (and a representative
  scalar/transform slt if cheap) and confirm NOTHING changes vs today with the
  flag default (off). Report this explicitly.
* Add a focused transform-level test: build the CLU-137 repro relation (a Filter
  with the DNF temporal predicate), run the relevant transform with a
  `TransformCtx` whose features have `enable_eqsat_scalar_canonicalize = true`,
  and assert the temporal predicate ends up as a top-level binary conjunct (the
  e-graph-level version already exists as `test_clu137_temporal_factors_to_top_level`;
  this is the through-the-transform version). If wiring a full TransformCtx in a
  unit test is heavy, at minimum assert the flag-on path is reached (e.g. via the
  callback) and defer the end-to-end assertion to the 3c slt/td run, noting it.

## Constraints (binding)

* Default (flag off) behavior identical to today. This is the hard bar.
* Follow `enable_eqsat_optimizer` as the exact template for flag definition +
  OptimizerFeatures binding.
* No `as` conversions. No em-dashes / clause-joining semicolons in comments.
* Do not touch the eqsat scalar engine internals; this task is wiring only.
* Keep the `mz-expr` `None`/reduce path untouched (mz-expr gains only an optional
  parameter, no dependency on mz-transform).

## Done criteria

* Whole-workspace `cargo check` passes; touched crates clippy-clean; `bin/fmt`
  applied.
* Flag off: no test/golden changes vs today (state the evidence).
* Flag on: compiles and the canonicalize_predicates path uses eqsat (focused test
  or clearly-documented deferral to 3c).

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: flag-gated wiring of the scalar canonicalizer into canonicalize_predicates (Phase 3)`.
End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-3b-report.md`. Return only:
status, commit sha, the flag name + how it gets test coverage, which call sites
were wired, evidence the flag-off path is unchanged, what was deferred (:437/:87,
goldens to 3c), concerns.
