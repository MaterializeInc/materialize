# compute-types (src/)

Shared type definitions for the compute layer. Everything that must be agreed
upon between `mz-compute-client` (the protocol), `mz-compute` (rendering), and
`mz-adapter` (lowering) lives here. Nothing compute-specific is in `mz-repr`;
`mz-compute-types` is the correct home for compute-layer type boundaries.

## Files / modules (LOC ≈ 9,214)

| Path | LOC | What it owns |
|---|---|---|
| `plan/` (subtree) | 5,198 | LIR plan types — see `plan/CONTEXT.md` |
| `dataflows.rs` | 632 | `DataflowDescription<P,S,T>`: sources, indexes, objects_to_build, exports, as_of/until |
| `explain/text.rs` | 1,714 | Text/JSON `Explain` impl for `DataflowDescription<Plan>` |
| `dyncfgs.rs` | 427 | All `dyncfg::Config` constants for the compute layer |
| `plan.rs` | 822 | (see `plan/CONTEXT.md`) |
| `sinks.rs` | 116 | `ComputeSinkDesc`, `ComputeSinkConnection` variants |
| `sources.rs` | 35 | `SourceInstanceDesc` + `SourceInstanceArguments` |
| `config.rs` | 48 | `ComputeReplicaConfig`, `ComputeReplicaLogging` |

## Key concepts

- **`DataflowDescription<P,S,T>`** — generic descriptor parameterized by plan
  type (`P`), storage metadata (`S`), and timestamp (`T`). Used at three
  granularities: `<MirRelationExpr>` during optimization, `<Plan>` after
  lowering, `<RenderPlan>` on the wire.
- **LIR plan hierarchy** — `Plan<T>` (recursive tree) → `RenderPlan<T>` (flat,
  id-linked for rendering/wire). See `plan/CONTEXT.md`.
- **`dyncfgs`** — all compute dynamic config knobs in one file; consumed by
  `mz-compute` and `mz-compute-client`.
- **`Interpreter` trait** — abstract interpretation framework for LIR analysis
  passes; tagless-final style. Currently sparse doc coverage
  (TODO database-issues#7533, #7446).

## Bubbled findings for src/CONTEXT.md

- **`Interpreter` API underdocumented**: every method in `interpret/api.rs` has
  a `TODO(database-issues#7533): Add documentation` placeholder; the trait is
  non-trivial (tagless-final) and critical for plan analysis.
- **`Transform` trait may be over-engineered**: `BottomUpTransform` +
  `Transform` machinery supports only one concrete implementation
  (`RelaxMustConsolidate`); if the pass count stays small the framework adds
  indirection without payoff.
- **`DataflowDescription` triple-instantiation seam**: three type instantiations
  (`MIR`, `LIR Plan`, `RenderPlan`) share one struct with `P` generic — correct
  design but callers must know which instantiation they hold; no newtype guards.

## Cross-references

- Generated docs: `doc/developer/generated/compute-types/`
- Consumers: `mz-compute-client`, `mz-compute`, `mz-adapter`, `mz-transform`
