# mz-compute-client

Public API for the compute layer. Adapter and controller use this crate to
issue compute commands, manage collections, and receive responses without
knowing replica internals.

## Structure (≈ 10,077 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `src/` | 10,077 | All source — see `src/CONTEXT.md` |

## Package identity

Crate name: `mz-compute-client`. No feature flags. Key dependencies:
`mz-compute-types` (shared plan/dataflow types), `mz-cluster-client`,
`mz-storage-client`, `mz-service` (gRPC transport), `mz-persist-types`.
Downstream: `mz-controller`, `mz-clusterd`.

## Key interfaces (exported)

- `ComputeController` — top-level handle; methods `create_dataflow`,
  `drop_collections`, `peek`, `subscribe`, `set_read_policy`
- `ComputeCommand` / `ComputeResponse` — wire protocol enums
- `ComputeCommandHistory` — compacted command log for replica reconnect
- `ComputeClient` trait — `GenericClient<ComputeCommand, ComputeResponse>`
- `PartitionedComputeState` — merges responses from multi-worker replicas
- `as_of_selection::select_as_of` — bootstrap-time as-of constraint solver

## Bubbled findings for src/CONTEXT.md

- **`as_of_selection` misplacement**: module doc explicitly notes it should be
  inside the controller API but lives here due to current API friction; migration
  candidate once controller API allows it.
- **Read-capability enforcement gap**: `read_capabilities` field in
  `SharedCollectionState` is `Arc<Mutex<…>>` with a comment-only access contract
  (TODO database-issues#teskje); no type-system enforcement.
- **`SequentialHydration` assumes single-export dataflows**: comment documents
  this assumption; would break silently if multi-export dataflows are introduced.
- **Subscribe/CopyTo frontier reporting deferred**: two TODO(database-issues#4701)
  items in `instance.rs` for reporting those frontiers through `Frontiers` responses.

## Cross-references

- Generated docs: `doc/developer/generated/compute-client/`
- See `src/CONTEXT.md` for full module breakdown.
