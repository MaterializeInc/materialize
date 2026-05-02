# mz-compute-client (src/)

Public API for the compute layer. Sits between `mz-controller` (caller) and
`mz-clusterd` (replica) and orchestrates compute instances, replicas, and the
compute protocol.

## Files / modules (LOC ≈ 10,077)

| Path | LOC | What it owns |
|---|---|---|
| `controller.rs` + `controller/` | ~4,562 | `ComputeController`, `Instance`, replica plumbing, sequential hydration, introspection |
| `controller/instance.rs` | 3,301 | Per-instance state machine: dataflow create/drop, peek, subscribe, frontier tracking, read-hold management |
| `controller/sequential_hydration.rs` | 350 | `SequentialHydration` client shim: enforces hydration concurrency between controller and `PartitionedState` |
| `as_of_selection.rs` | 1,340 | Bootstrap-time as-of selection with constraint propagation across dependent collections |
| `protocol/command.rs` | 464 | `ComputeCommand` enum (Hello → CreateInstance → dataflow/peek/subscribe commands) |
| `protocol/history.rs` | 297 | `ComputeCommandHistory`: compaction of command log for reconnecting replicas |
| `protocol/response.rs` | 337 | `ComputeResponse` variants: `Frontiers`, `PeekResponse`, `SubscribeBatch`, `CopyToResponse` |
| `service.rs` | 652 | `ComputeClient` trait alias; `PartitionedComputeState` merging multi-worker responses |
| `logging.rs` | 406 | `LoggingConfig`, `LogVariant` (introspection log identifiers) |
| `metrics.rs` | 647 | Prometheus metrics for controller, instances, and replicas |

## Key concepts

- **`ComputeController`** — manages `Instance` per compute cluster; routes
  adapter calls to the right instance; surfaces `ComputeControllerResponse` events.
- **`Instance`** — tracks collections (indexes, MVs, sinks), their `ReadHold`s,
  peek/subscribe state, and replica frontier responses. Read capabilities are
  `Arc<Mutex<MutableAntichain>>` shared with `ReadHold` objects; modification
  must go through `apply_read_hold_change` (TODO: enforce via type system,
  database-issues#teskje).
- **Protocol stages** — `Hello` → `CreateInstance` → steady state. History is
  compacted on reconnect so replicas can replay minimal state.
- **`SequentialHydration` shim** — intercepts `Schedule` commands between
  controller and `PartitionedState`; needed before partitioning so all workers
  see the same order.
- **`as_of_selection`** — bootstrap module acknowledged as misplaced: should
  live in the controller API but currently lives here due to API friction.
- **Seam to storage** — `StorageController` reference passed into
  `ComputeController`; frontier downgrade propagates back to storage via
  `ReadHold`.

## Cross-references

- Generated docs: `doc/developer/generated/compute-client/`
- Downstream: `mz-controller` (calls `ComputeController` API), `mz-clusterd` (receives commands)
- Types shared with `mz-compute-types` (`Plan`, `DataflowDescription`, `ComputeSinkDesc`)
