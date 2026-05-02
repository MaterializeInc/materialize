# mz-compute-types

Shared type library for the compute layer. Provides the `DataflowDescription`
descriptor, the LIR `Plan` hierarchy, compute sink/source descriptors, replica
config, and dynamic config constants. All compute crates agree on these types at
their boundaries.

## Structure (≈ 9,214 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `src/` | 9,214 | All source — see `src/CONTEXT.md` |
| `src/plan/` | 5,198 | LIR plan subtree — see `src/plan/CONTEXT.md` |

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
- See `src/CONTEXT.md` and `src/plan/CONTEXT.md` for detail.
