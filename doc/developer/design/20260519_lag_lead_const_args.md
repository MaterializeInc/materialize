# Specializing `lag` / `lead` for constant arguments

- Associated issue: [database-issues#6248](https://github.com/MaterializeInc/database-issues/issues/6248)

## The problem

In Materialize, every call to `lag(x)` / `lead(x)` is planned as `lag(x, 1, NULL)` /
`lead(x, 1, NULL)`. At HIR construction time, the lag/lead builtins in
`src/sql/src/func.rs` wrap the three arguments into a 3-field `RecordCreate`, and
the runtime executor (`lag_lead_inner_*` in `src/expr/src/relation/func.rs`)
repacks and re-unpacks that record for every row of every partition. For a
typical call where the offset and default are compile-time constants, the per-row
shape carries two redundant datums (the same `1` and `NULL` for every row) plus
the `RecordCreate` wrapper itself.

A second, smaller cost lives inside `lag_lead_inner_ignore_nulls`: it branches
on `increment.signum()` per row to choose between `skip_nulls_forward` and
`skip_nulls_backward`, and to decide whether to add or subtract the offset in
the inner loop. When the offset is a known constant, all of that is decidable at
plan time.

## Success criteria

- For `lag(x[, k_lit[, d_lit]])` / `lead(...)` with both `k_lit` and `d_lit`
  literal, the per-row payload is the bare input Datum (no record wrapper).
- For the IGNORE NULLS variant of the same case, the inner loop dispatches once
  (at plan time) to a monomorphized helper that does not branch on the sign of
  the offset.
- No regression for the non-literal case (variable offset, variable default, or
  either-but-not-both literal). Those continue through the existing per-row
  record path unchanged.
- The `v2_ind` workload shows a measurable improvement.
- The specialization is visible in EXPLAIN output so reviewers can confirm it
  fired.

## Out of scope

- Other window functions (`first_value`, `last_value`, `nth_value`, ranking
  functions). They have different per-row shapes; a separate effort.
- Speeding up the `O(partition_size * offset)` IGNORE NULLS loop for large
  constant offsets (there is an existing TODO in `lag_lead_inner_ignore_nulls`
  about turning this into a `O(partition_size)` algorithm). Independent.
- **Partial specialization**: the cases where only `k` is literal (with a
  variable default) or only `d` is literal (with a variable offset). These
  would require carrying 2-field records and `Option` fields. The only
  user-reachable partial case is `lag(x, literal, column_default)`, which we
  consciously leave on the generic path to keep the implementation simple.
- Catalog / persisted-state migrations. `AggregateFunc` is not persisted across
  restarts; rendered dataflow plans live only in memory.
- Changes to SQL surface area or PostgreSQL compatibility.
- **Specializing `offset == 0`.** When the literal offset is exactly `0`,
  `specialize_lag_lead` deliberately declines to rewrite the call and leaves
  it on the generic `LagLead` path. This is an extremely rare corner case
  (it returns the current row, modulo NULL/IGNORE NULLS edge cases), and
  bailing out keeps the const path free of a degenerate branch and avoids
  any risk of subtle behavior divergence between the const and generic
  IGNORE NULLS implementations at `offset == 0`.

## Solution proposal

Introduce a specialized representation that exists at both HIR and MIR layers,
plus a dedicated HIR transform that produces it.

### New HIR variant: `ValueWindowFunc::LagLeadConst`

In `src/sql/src/plan/hir.rs`, add a single new variant:

```rust
pub enum ValueWindowFunc {
    Lag,
    Lead,
    LagLeadConst { offset: i32, default: mz_repr::Row },
    // ...other window funcs unchanged
}
```

A single variant — not separate `LagConst` / `LeadConst` — because the only
difference between the two is the sign of the offset. We pre-apply the sign at
transform time (negative = lag, positive = lead), matching the existing runtime
convention `let offset = match lag_lead_type { Lag => -offset, Lead => offset };`.
The `Display` impl prints `lag_lead_const` (distinct from the generic
`lag_lead`) so that EXPLAIN output makes the specialization obvious.

`ValueWindowFunc::output_sql_type` for the new variant returns the args column
type made nullable (one line). `ValueWindowFunc::into_expr` returns the args
expression as-is (no `RecordCreate`) together with `AggregateFunc::LagLeadConst`.

### New MIR variant: `AggregateFunc::LagLeadConst`

In `src/expr/src/relation/func.rs`:

```rust
pub enum AggregateFunc {
    // ...
    LagLeadConst {
        order_by: Vec<ColumnOrder>,
        ignore_nulls: bool,
        offset: i32,        // sign encodes lag-vs-lead direction
        default: mz_repr::Row,
    },
    // ...
}
```

No `lag_lead: LagLeadType` field — direction is encoded in the sign of
`offset`. This keeps the runtime path branch-free.

All exhaustive matches over `AggregateFunc` gain a `LagLeadConst` arm — most
delegate to the same code as `LagLead` (e.g., `is_constant`,
`propagates_nonnull_constraint`). The interesting arms are `eval`,
`output_sql_type`, and `HumanizedExpr` (which prints `lag_lead_const[...]`).
`src/compute-types/src/plan/reduce.rs` and `src/compute/src/render/reduce.rs`
classify it as `ReductionType::Basic`, same as `LagLead`.

### New HIR transform: `specialize_lag_lead`

We considered three places where the specialization could fire (see
[Alternatives](#alternatives) for full reasoning); we picked a dedicated HIR
transform alongside `fuse_window_functions` because (a) it keeps HIR planning
(the lag/lead builtins in `src/sql/src/func.rs`) free of optimization concerns,
(b) it places the optimization next to its sibling window-fn rewrite, and
(c) it will show up as a discrete step in any future HIR optimizer tracing.

In `src/sql/src/plan/transform_hir.rs`, add a transform alongside
`fuse_window_functions`. It walks the HIR tree (using the same `visit_mut_post`
pattern) and, for every
`ValueWindowExpr { func: Lag | Lead, args: RecordCreate([value, offset_expr, default_expr]), .. }`
attempts to constant-fold the `offset_expr` and `default_expr` children via
`HirScalarExpr::simplify_to_literal_with_result` (defined in
`src/sql/src/plan/hir.rs`). We deliberately do **not** gate this with a
cheap "looks constant-ish" pre-check on the children — the common case is
that `lag`/`lead` is called with literal offset/default, so calling
`simplify_to_literal_with_result` unconditionally and using its result is
both simpler and good enough. When both calls succeed, the offset is a
non-NULL `Int32(n)` with `n != 0`, and the default folds to some `Row`:

- Replaces `args` with the bare `value` field.
- Replaces `func` with `LagLeadConst { offset: if Lag { -n } else { n }, default: row_of_default }`.

Any other outcome (either child not constant-foldable, evaluation error,
NULL offset, or `offset == 0`) leaves the call as `Lag`/`Lead` on the
generic path.

The transform is invoked from `src/sql/src/plan/lowering.rs:213`, immediately
before `transform_hir::fuse_window_functions`. Running it first means any fused
group downstream automatically carries the specialized per-call variant; no
change to `fuse_window_functions` is needed. We add a one-line comment near
`extract_options` (≈line 434) documenting that the per-call func variant —
including `LagLeadConst` — flows through fusion unchanged.

### Executor specialization

`lag_lead_const` / `lag_lead_const_no_list` mirror their generic siblings but
operate on the bare per-row Datum. They call
`lag_lead_inner_const_respect_nulls(args, offset, default)` /
`lag_lead_inner_const_ignore_nulls(args, offset, default)`. The inner const
functions do **not** take a `lag_lead_type` parameter — the sign of `offset` is
the only thing they would have used it for, and it is already baked in.

For IGNORE NULLS, we factor the body of `lag_lead_inner_ignore_nulls` into a
generic helper:

```rust
fn lag_lead_inner_ignore_nulls_const<const FORWARD: bool>(
    args: &[(Datum, Datum)],
    abs_offset: u32,
    default: Datum,
) -> Vec<Datum>;
```

`FORWARD` is set at dispatch time from `offset > 0`, and `abs_offset =
offset.unsigned_abs()`. The two monomorphizations specialize both the `j +=
increment` step and the choice of `skip_nulls_forward` / `skip_nulls_backward`,
removing the per-row sign branch. The `offset == 0` case stays on the existing
panic path.

The unspecialized `LagLead` path (variable offset) keeps the existing
implementation unchanged.

### Single-row fast path

`AggregateExpr::on_unique` is called for partitions of size 1 (typical for
`PARTITION BY <primary key>`). Today's `on_unique_lag_lead` synthesizes
`if offset IS NULL then NULL else if offset = 0 then expr else default` MIR
expressions, using `RecordGet` to peel the args record.

For `LagLeadConst` we add `on_unique_lag_lead_const`, which is a plain constant
fold: `offset == 0` returns the input expression directly; `offset != 0` returns
`MirScalarExpr::literal_ok(default)`. No `RecordGet`, no `IsNull`, no equality.
The `on_unique` dispatcher gains a `LagLeadConst` arm both for plain aggregates
(`relation.rs:2703`) and for fused-window constituents (`relation.rs:2969`).

### EXPLAIN output

`HumanizedExpr<AggregateFunc>` renders `LagLeadConst` as
`lag_lead_const[offset=<n>, default=<d>, order_by=[...]]`. The `_const` suffix
is the user-visible signal that the optimization fired; the sign of `n`
indicates lag (negative) vs lead (positive).

## Minimal viable prototype

An end-to-end query: a window view over a high-throughput source using
`lag(x) OVER (PARTITION BY p ORDER BY t)`. Confirm via EXPLAIN PHYSICAL PLAN
that `lag_lead_const[offset=-1, default=null, ...]` appears, and confirm via
the `v2_ind` workload (referenced at the top of
`src/expr/benches/window_functions.rs`) that the specialized view keeps up
with higher input rates than the unspecialized baseline.

## Testing plan

### SQL logic tests

We need coverage at two EXPLAIN levels, for two complementary reasons:

1. **`EXPLAIN OPTIMIZED PLAN` (the MIR plan)** — in
   `test/sqllogictest/window_funcs.slt`, near the existing `on_unique` /
   `FusedValueWindowFunc` tests. This is where we directly verify that the
   optimization fires: a specialized call appears as `lag_lead_const[...]`,
   an unspecialized call still appears as `lag_lead[...]`.
2. **The default `EXPLAIN`** — in `test/sqllogictest/explain/default.slt`.
   This is what users see day to day, so we check that the rendering reads
   well: all the important info (`offset`, `default`, `order_by`) appears
   in brackets, the formatting is consistent with the other aggregate
   renderings, and the `_const` suffix is clearly visible.

Cases to cover at both levels:

- Should specialize (EXPLAIN shows `lag_lead_const[...]`):
  `lag(x)`, `lag(x, 5, 'd')`, `lag(x, 0)`, `lead(x, -3) IGNORE NULLS`.
- Should NOT specialize (EXPLAIN still shows `lag_lead[...]`):
  `lag(x, NULL::int)`, `lag(x, k)` where `k` is a column reference,
  `lag(x, 1, d)` where `d` is a column reference (the all-or-nothing rule).
- A fused query mixing `lag(x)`, `lead(y, 3)`, and `first_value(z)` in the
  same `OVER` window, to confirm that `FusedValueWindowFunc` correctly
  carries the specialized per-constituent variant.

In `window_funcs.slt`, each test additionally asserts result equivalence to
the pre-change behavior, alongside the EXPLAIN-PLAN shape.

### Other test impact

Audit existing EXPLAIN snapshots that mention `lag` / `lead` (across
`test/sqllogictest/`, `test/testdrive/`, and `test/sqllogictest/explain/*`)
and refresh those that now show `lag_lead_const[...]`.

### Microbenchmark (optional, at the end)

A microbenchmark is not on the critical path for this change and is
deliberately deferred to the very end of the work — *after* the
implementation, SQL logic tests, and the `v2_ind` end-to-end check are
done. The reason is that a microbenchmark over `order_aggregate_datums`
in `src/expr/benches/window_functions.rs` only exercises a thin slice of
the window-function evaluation code (the per-row datum walk and the
inner lag/lead loop) and so wouldn't reliably predict end-to-end
performance — the customer-visible delta can be either smaller (when
surrounding dataflow machinery dominates) or larger (because the
specialization also reduces per-row payload size, allocator pressure,
and other costs the microbenchmark doesn't isolate). We rely on the
`v2_ind` workload for the headline performance signal instead.

If, after the rest of the work lands, we still want a microbenchmark
datapoint, the extension is straightforward: add a second `bench_function`
(e.g., `order_aggregate_datums_const`) that builds its `datums` vector
with the bare-Datum payload shape (`row(orig_row, x)` instead of
`row(orig_row, row(x, 1, null))`), run it alongside the existing
function via `cargo bench -p mz-expr window_functions`, and optionally
add a third variant exercising the IGNORE NULLS const path with a
negative offset to cover both `FORWARD = true` and `FORWARD = false`
monomorphizations of `lag_lead_inner_ignore_nulls_const`.

## Alternatives

1. **Extend `AggregateFunc::LagLead` with `Option<i32>` / `Option<Row>` fields,
   keep one variant.** Rejected: forces `Option` machinery into every match
   arm; the four-shape encoded-args matrix complicates the executor and the
   type-inference helper. The new-variant design is louder and easier to
   reason about.
2. **Partial specialization (specialize either or both literals
   independently).** Rejected: doubles the executor's case analysis for the
   one user-reachable partial case (`lag(x, literal, column_default)`).
   All-or-nothing keeps the per-row payload binary: bare Datum or 3-field
   record.
3. **Two HIR variants (`LagConst` and `LeadConst`).** Rejected: the only
   thing they encoded was the sign of the offset, which we already capture in
   the `i32` itself. Merging them halves the new match arms.
4. **Keep `lag_lead: LagLeadType` on `AggregateFunc::LagLeadConst` for EXPLAIN
   faithfulness.** Rejected: the EXPLAIN keyword `lag_lead_const` plus the
   sign of `offset` already convey direction. Carrying a redundant field is
   confusing and risks the runtime accidentally branching on it.
5. **Where to perform the specialization.** We weighed three placements,
   all upstream of MIR:
   - *In HIR planning, inside the lag/lead builtins in `src/sql/src/func.rs`.*
     Rejected. Each of the six lag/lead overloads would have to inspect its
     literal arguments and emit the specialized variant, which intermixes
     HIR planning (the builtin's actual job) with an optimization and spreads
     the rewrite across many call sites. It would also bypass any future
     HIR optimizer tracing, because no discrete pass runs.
   - *As a dedicated HIR transformation in `transform_hir.rs`.* **Chosen.**
     A single `visit_mut_post` walk handles every `Lag` / `Lead` call
     uniformly and lives next to `fuse_window_functions`, the existing
     sibling window-fn rewrite. Running it before `fuse_window_functions`
     means the fused form automatically carries the specialized per-call
     variant, with no change needed in the fusion code. The pass also
     becomes a discrete step in any future HIR optimizer tracing, matching
     the precedent set by other HIR transforms.
   - *In HIR→MIR lowering (`ValueWindowExpr::into_expr`).* Rejected. By the
     time we reach lowering, fusion has already grouped calls; threading
     the specialization through `into_expr` would either (a) come after
     fusion and force the fusion code to be specialization-aware, or (b)
     require re-checking each constituent of a fused group at lowering
     time. Both are more invasive than a single HIR pass run before fusion.
     (A pure-MIR transform would be even further removed and would have to
     pattern-match `FusedValueWindowFunc` structures.)
6. **Per-row `signum()` branch in IGNORE NULLS.** Rejected: with `offset`
   known at plan time, the branch is hoistable. Two monomorphized helpers
   (`FORWARD=true` / `FORWARD=false`) cost roughly nothing in code size and
   remove the per-row branch entirely.

## Open questions

None.
