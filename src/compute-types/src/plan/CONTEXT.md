# compute-types::plan

LIR (Low-level Intermediate Representation) plan types and supporting
infrastructure: MIR→LIR lowering, sub-plans for each physical operator,
abstract interpretation, LIR-level transforms, and the `RenderPlan` wire form.

## Files (LOC ≈ 5,198 across this directory)

| File | LOC | What it owns |
|---|---|---|
| `plan.rs` (parent) | 822 | `Plan<T>` / `PlanNode` enum, `AvailableCollections`, `GetPlan`, `LirId` |
| `lowering.rs` | 1,115 | `Context` struct; MIR→LIR lowering; arrangement selection |
| `render_plan.rs` | 1,001 | `RenderPlan` (flat, LirId-linked) for rendering and the wire protocol; `TryFrom<Plan>` |
| `reduce.rs` | 671 | `ReducePlan` variants: `Hierarchical`, `Basic`, `Monotonic`, `Collation` |
| `interpret/api.rs` | 772 | `Interpreter` trait (abstract interpreter / object algebra over `Plan`) |
| `interpret/physically_monotonic.rs` | 256 | `PhysicallyMonotonic` interpreter: derives monotonicity annotations |
| `join/` | 360 | `JoinPlan` (linear vs. delta), join input ordering |
| `top_k.rs` | 225 | `TopKPlan`: monotonic vs. basic top-K strategies |
| `threshold.rs` | 87 | `ThresholdPlan`: basic (arrangement-based) threshold |
| `transform/api.rs` | 98 | `Transform` / `BottomUpTransform` traits for LIR-level passes |
| `transform/relax_must_consolidate.rs` | 83 | Single LIR transform: relaxes `must_consolidate` flags |

## Key concepts

- **`Plan<T>` / `PlanNode`** — recursive tree representation of a LIR dataflow
  node. `LirId` uniquely labels each node; used to construct `RenderPlan`.
- **`RenderPlan`** — flat, id-linked form of `Plan` for protocol serialization
  and rendering. Avoids stack overflows on deep plans by making tree traversal
  iterative. Recursive bindings remain nested.
- **Lowering (`Context`)** — MIR → LIR; tracks `AvailableCollections` per `Id`
  to select join arrangements. `enable_reduce_mfp_fusion` is the one
  feature-flag knob at this level.
- **`Interpreter` trait** — tagless-final encoding over `Plan`; defines abstract
  analysis domains (e.g., `PhysicallyMonotonic`). TODO(database-issues#7446) to
  align variant coverage with `Plan` structure.
- **`Transform` / `BottomUpTransform`** — LIR-level pass trait. Currently only
  one implementation (`RelaxMustConsolidate`); trait machinery may outlive need
  if passes remain few.

## Cross-references

- Parent: `src/compute-types/src/CONTEXT.md`
- Generated docs: `doc/developer/generated/compute-types/plan/`
- Consumer: `mz-compute` rendering engine, `mz-compute-client` protocol
