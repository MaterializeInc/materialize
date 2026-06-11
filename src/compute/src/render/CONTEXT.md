# compute::render

Translates a `RenderPlan` IR into a Timely/differential dataflow program.
Entry point: `build_compute_dataflow` in `render.rs` — imports persist sources,
then recursively builds operators via `Context::render_plan` /
`Context::render_plan_expr`.

## Files

| File | LOC | What it owns |
|---|---|---|
| `render.rs` | 1,909 | `build_compute_dataflow`, `render_plan`, `render_plan_expr` dispatch, source import, index/sink export wiring |
| `context.rs` | 973 | `Context<T>` — the rendering accumulator; `CollectionBundle` (oks + errs + arrangements); `lookup_id` / `insert_id`; timestamp refinement (`RenderTimestamp`, `enter_region`) |
| `reduce.rs` | 2,460 | All `ReducePlan` variants: accumulable (diff-typed), hierarchical (tree-of-partial-aggs), basic (arbitrary aggs), collation (multi-sub-reduction join) |
| `join/` | 2,169 | `delta_join.rs` + `linear_join.rs` + `mz_join_core.rs`; re-exports `LinearJoinSpec` |
| `top_k.rs` | 1,060 | `TopKPlan`: monotone fast-path, two-phase general, hierarchical grouping |
| `continual_task.rs` | 1,109 | Continual-task sink rendering: new-delta source + output reference reader + persist sink |
| `sinks.rs` | 198 | Dispatch on `ComputeSinkConnection` → Subscribe / MV / CopyToS3 / ContinualTask; applies per-sink MFP; wires `StartSignal` hydration probe |
| `flat_map.rs` | 200 | Table-function (`FlatMap`) operator |
| `threshold.rs` | 130 | Threshold (non-negative) operator |
| `errors.rs` | 108 | `ErrorLogger` — logs errors to the compute introspection stream |

## Key concepts

- **Dual-stream invariant.** Every rendered node produces an `(oks, errs)` pair.
  `CollectionBundle` holds both. Operators that cannot fail pass `errs` through
  unchanged; fallible operators (map, join, reduce) can inject into `errs`.
- **`Context<T>`** is the *Interface* between the plan-walking driver and each
  operator renderer. It is parameterized by a `RenderTimestamp` scope `T`, which
  may be a flat timestamp or a `Product` (for iterative scopes / continual tasks).
- **`render_plan_expr` dispatch** matches on `render_plan::Expr` variants and
  delegates to submodule render functions — one per operator class.
- **`StartSignal`** — a one-shot probe that blocks sink writes until hydration
  is complete; passed into `render.rs` from `server.rs`.

## Cross-references

- Caller: `compute_state::ActiveComputeState::handle_compute_command` →
  `build_compute_dataflow`.
- IR source: `mz-compute-types::plan::render_plan::RenderPlan`.
- Sink implementations: `crate::sink::*` (called from `render::sinks`).
- Logging hooks: `crate::logging::compute` (injected via `Context`).
- Generated docs: `doc/developer/generated/compute/render/`.
