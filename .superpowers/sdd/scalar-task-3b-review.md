# Task 3b review: flag-gated wiring of the scalar eqsat canonicalizer

Base `e6f9436202` .. Head `af9dc879a5`. Read-only review.

Sources read: the diff (`review-3b-...diff`), brief, implementer report, and the
following live files to judge correctness:
`src/repr/src/optimize.rs` (the `optimizer_feature_flags!` macro),
`src/sql/src/session/vars/definitions.rs` (`From<&SystemVars>`,
`enable_all_feature_flags_by_default`, the binding test),
`src/transform/src/eqsat/scalar.rs` (`canonicalize` signature + bridge), and
greps of every caller of the signature-changed functions plus all
`OptimizerFeatures` construction sites.

## Spec compliance / completeness

- ✅ Feature flag added with `default: false`, `enable_for_item_parsing: false`
  (`definitions.rs:2120-2126` in the new tree; diff 241-246). Matches the
  `enable_eqsat_optimizer` template shape.
- ✅ `OptimizerFeatures` field added via the macro list (`optimize.rs:160-163`;
  diff). Macro auto-derives the struct, `OptimizerFeatureOverrides`, `Default`,
  `OverrideFrom`, and the `BTreeMap` conversions over every field
  (`optimize.rs:19-75`), so only manual sites needed edits.
- ✅ `From<&SystemVars>` binds the new flag:
  `enable_eqsat_scalar_canonicalize: vars.enable_eqsat_scalar_canonicalize()`
  (`definitions.rs:2366`). The accessor is macro-generated (`definitions.rs`
  `[<$name:lower>]`).
- ✅ Exhaustive manual sites updated: `ddl.rs:4959` destructure, `dml.rs:645`
  construction, the binding test's destructure + `set_var!`
  (`definitions.rs:2397, 2446`).
- ✅ mz-expr wrapper + `_with` form added; `mz-expr` gains only an optional
  parameter, no `mz-transform` dependency (`canonicalize.rs:39-83`).
- ✅ Bridge `eqsat::scalar::canonicalize_predicates(.., enable)` is the single
  injection point (`scalar.rs:54-71` new tree; diff 563-579).
- ✅ Primary CLU-137 path wired (CanonicalizeMfp -> rebuild_mfp ->
  Filter::action) plus predicate_pushdown and literal_constraints, as the brief
  scoped. Deferred reduce calls (`canonicalize.rs:437/87`, predicate_pushdown
  literal-error reduce) left untouched and documented.
- ✅ `bin/fmt` / clippy / cargo check claims are consistent with the diff (no
  formatting noise, no stray `as`, no em-dashes in added comments).

## Flag-OFF identical verdict

Provably unchanged. Reasoning:

1. `canonicalize_predicates` is now a thin wrapper calling
   `canonicalize_predicates_with(predicates, repr_column_types, None)`
   (`canonicalize.rs:39-44`). The `None` arm runs
   `predicates.iter_mut().for_each(|p| p.reduce(repr_column_types))`
   (`canonicalize.rs:80-82`), the exact prior statement, same iteration order,
   same everything. Steps 2+ are textually unchanged.
2. The bridge's `else` branch (flag off) calls the plain
   `mz_expr::canonicalize::canonicalize_predicates(predicates, col_types)`
   (`scalar.rs` diff 576-578), i.e. today's call. So every wired site, when the
   flag is false, is byte-identical to its previous `canonicalize_predicates`
   call.
3. Every wired call site derives `enable_eqsat_scalar` from
   `ctx.features.enable_eqsat_scalar_canonicalize`, which is `false` in
   production. Confirmed at: `canonicalize_mfp.rs:55`, `fusion.rs:56`,
   `fusion/filter.rs:73`, `fusion/join.rs:71`, `predicate_pushdown.rs:142`,
   `literal_constraints.rs:75`, `normalize_ops.rs:33`, `dataflow.rs:71`.
4. The two non-ctx call sites pass a literal `false`: `eqsat/raise.rs:447`
   (rebuild_mfp on already-saturated predicates) and `test_runner.rs:474`
   (keeps existing goldens flag-off). Both correct.
5. No golden/slt/spec/out file changed: `git diff --stat` over
   `*.slt *.spec *.out test/` is empty. The only `tests/` edit is the harness
   `test_runner.rs` `false` argument.

I found no path where flag-off diverges from the prior behavior across all 16
files.

## Flag definition + OptimizerFeatures binding verdict

All sites bound. The only value-bearing source is `From<&SystemVars>`
(`definitions.rs:2338`), which is updated. The override path is fully
macro-generated over all fields (`optimize.rs:40-49`), so `ddl.rs`/`dml.rs`
(the manual `OptimizerFeatureOverrides` sites) plus the binding test are the
only hand sites, and all four are updated. The benchmark
(`eqsat_arrangement_benchmark.rs:120`) uses `OptimizerFeatures::default()` and
never sets the new field, which is correct (defaults false, test-only).

Test-on mechanism correct: `enable_all_feature_flags_by_default`
(`definitions.rs:1769`) iterates `$(...)+` over every `feature_flags!` entry and
`set_default(name, "on")`. The new flag is in that list and is not excluded, so
the `--all-features` slt/td path runs with it ON. The report's claim is
consistent with the code. `enable_for_item_parsing` only flips flags whose
`enable_for_item_parsing` is true, and the new flag's is false, so item parsing
leaves it off (guarded by `optimizer_features_no_enable_for_item_parsing`).

## Callback / bridge + Filter::action threading verdict

Sound, no borrow/aliasing bug. The injected closure is
`|e, ct| { *e = canonicalize(e, ct); }` (`scalar.rs` diff 572-574).
`canonicalize(&MirScalarExpr, &[ReprColumnType]) -> MirScalarExpr`
(`scalar.rs:35`) takes `e` by shared reborrow and returns an owned value; the
call fully completes before the assignment to `*e`. No aliasing of the `&mut`.

`Filter::action` threading complete. All callers updated and pass the right
value: `canonicalize_mfp.rs:103` (threaded), `fusion.rs:59` (threaded),
`fusion/join.rs` via `CanonicalizeMfp.action` (threaded), `fusion/filter.rs:215`
(test, `true`). `eqsat/raise.rs` reaches it via `rebuild_mfp(.., false)`. The
grep for `Filter::action` shows no unthreaded production caller.

`eqsat/raise.rs:447` passing `false` is correct and intended: the predicates
there are extracted from the saturated e-graph, so re-running the scalar
canonicalizer would be redundant work (and the comment at diff 522-524 says so).
Flag-off-safe regardless.

## Broader-wiring assessment

The implementer wired more than the brief's primary path. Each extra site is a
legitimate `canonicalize_predicates` path, sound when on, and flag-off-safe:

- `fusion/join.rs:71-82`: post-fusion re-normalization runs PredicatePushdown +
  CanonicalizeMfp; flag read from ctx. Same canonicalization, just routed
  through eqsat when on. Off-safe.
- `normalize_ops.rs:33,44`: `Fusion::action` flag from ctx. Off-safe.
- `dataflow.rs:71,353,402`: global filter pushdown threads the flag from
  `transform_ctx.features`. Reaches `PredicatePushdown::action`. Off-safe.
- `literal_constraints.rs`: `undo_preparation` and `rebuild_mfp` forward the flag
  from `action`'s ctx (`:75,236`). Off-safe.

No site enables eqsat unconditionally, and every site is reachable/bound through
ctx. The predicate-pushdown literal-error `predicate.reduce` (around
`predicate_pushdown.rs:175`) is deliberately left as `reduce` (not wired); this
is conservative and off-safe, and is noted as deferred.

## Strengths

- The wrapper/`_with` split keeps `mz-expr` dependency-free and leaves the
  reduce path textually intact. The hard bar (flag-off identical) is met
  structurally, not just by test.
- Single bridge in `eqsat/scalar.rs` centralizes the flag-to-callback decision;
  call sites only pass a `bool`, minimizing churn surface.
- Threading is exhaustive and compiles workspace-wide-all-targets, and the
  exhaustive destructure test (`definitions.rs:2388`, no `..`) guarantees no
  binding field can be silently dropped.
- Honest report: it flags that a plain Filter is not a reduce-vs-eqsat
  differential.

## Issues

#### Critical
None.

#### Important
None.

#### Minor
- `clu137_temporal_factors_to_top_level_with_flag`
  (`fusion/filter.rs:208-218`) calls `Filter::action(rel, true)` only and never
  contrasts flag-off. Because `reduce` already factors this DNF (the report's own
  concern), the assertion would also pass with the flag off, so this test does
  not actually guard that the flag changed behavior on this input. The genuine
  routing guard is the mz-expr callback test
  (`canonicalize.rs:117`). Acceptable as documented, but the test name overstates
  what it proves. Consider asserting reach via a side channel, or rename to make
  clear it is a flag-on smoke test. Defer the could_error differential to 3c (an
  acceptable deferral).
- `canonicalize_predicates_none_matches_wrapper` (`canonicalize.rs:136`) is
  near-trivial (both arms call the same function). Harmless documentation; no
  action needed.

## Assessment

**Task quality: Approved**

Reasoning: The flag-off path is provably byte-identical (wrapper delegates with
`None`, bridge `else` calls the unmodified function, every ctx-derived flag is
false in production, no goldens changed), all `OptimizerFeatures` binding sites
are covered with the test-on mechanism intact, and the callback has no
aliasing bug. The only findings are Minor test-strength notes.

Note on verification scope: per instructions I did not re-run the suite. Compile
completeness of the threading is structurally guaranteed by the exhaustive
destructure plus the caller greps; the 351-pass / cargo-check claims are
consistent with the diff but not independently re-executed.
