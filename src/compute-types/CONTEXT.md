# mz-compute-types

Shared type library for the compute layer. Provides the `DataflowDescription`
descriptor, the LIR `Plan` hierarchy, compute sink/source descriptors, replica
config, and dynamic config constants. All compute crates agree on these types at
their boundaries.

## Files / modules (LOC ≈ 9,214)

| Path | LOC | What it owns |
|---|---|---|
| `plan/` (subtree) | 5,198 | LIR plan types — see [`src/plan/CONTEXT.md`](src/plan/CONTEXT.md) |
| `dataflows.rs` | 632 | `DataflowDescription<P,S,T>`: sources, indexes, objects_to_build, exports, as_of/until |
| `explain/text.rs` | 1,714 | Text/JSON `Explain` impl for `DataflowDescription<Plan>` |
| `dyncfgs.rs` | 427 | All `dyncfg::Config` constants for the compute layer |
| `plan.rs` | 822 | (see `plan/CONTEXT.md`) |
| `sinks.rs` | 116 | `ComputeSinkDesc`, `ComputeSinkConnection` variants |
| `sources.rs` | 35 | `SourceInstanceDesc` + `SourceInstanceArguments` |
| `config.rs` | 48 | `ComputeReplicaConfig`, `ComputeReplicaLogging` |

## Key concepts

- **`DataflowDescription<P,S,T>`** — generic descriptor parameterized by plan type (`P`), storage metadata (`S`), and timestamp (`T`). Used at three granularities: `<MirRelationExpr>` during optimization, `<Plan>` after lowering, `<RenderPlan>` on the wire.
- **LIR plan hierarchy** — `Plan<T>` (recursive tree) → `RenderPlan<T>` (flat, id-linked for rendering/wire). See `src/plan/CONTEXT.md`.
- **`dyncfgs`** — all compute dynamic config knobs in one file; consumed by `mz-compute` and `mz-compute-client`.
- **`Interpreter` trait** — abstract interpretation framework for LIR analysis passes; tagless-final style. Currently sparse doc coverage (TODO database-issues#7533, #7446).

## Package identity

Crate name: `mz-compute-types`. No feature flags. Key dependencies:
`mz-expr` (MIR expressions), `mz-repr` (scalar/row types), `mz-storage-types`,
`mz-dyncfg`, `differential-dataflow`, `timely`.
Downstream: `mz-compute-client`, `mz-compute`, `mz-adapter`, `mz-transform`.

## Key interfaces (exported)

- `DataflowDescription<P,S,T>` — generic dataflow descriptor
- `Plan<T>` / `RenderPlan<T>` — LIR plan forms (recursive tree vs. flat wire)
- `Interpreter` trait — abstract interpreter framework for LIR analysis
- `Transform` / `BottomUpTransform` — LIR-level pass traits
- `dyncfgs::*` — all compute dynamic config constants

## Bubbled findings for src/CONTEXT.md

- **`Interpreter` API underdocumented**: every method tagged TODO(#7533); trait
  semantics (tagless-final) deserve documentation given their non-obviousness.
- **`BottomUpTransform` machinery over-provisioned**: framework for LIR passes
  has one concrete user; evaluate whether the abstraction pulls its weight.
- **`DataflowDescription` type-parameter ambiguity**: three valid plan
  instantiations share one struct; no newtype layering guards against wrong-form
  usage.

## Cross-references

- Generated docs: `doc/developer/generated/compute-types/`
- Consumers: `mz-compute-client`, `mz-compute`, `mz-adapter`, `mz-transform`
