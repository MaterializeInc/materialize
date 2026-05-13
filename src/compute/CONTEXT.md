# mz-compute (crate)

The Timely/differential execution engine for Materialize. Receives
`ComputeCommand`s from the controller (via `mz-clusterd`), instantiates and
manages dataflows, and returns `ComputeResponse`s (peek results, subscribe
updates, frontier progress).

## Files (LOC ≈ 36,651)

| File/Dir | LOC | What it owns |
|---|---|---|
| `src/server.rs` | 822 | `serve` entry point; `Worker` event loop (step Timely, handle commands, drain peeks/subscribes, periodic maintenance); `reconcile` on reconnect; `CommandReceiver` / `ResponseSender` nonce tagging |
| `src/compute_state.rs` | 1,896 | `ComputeState` (collections, `TraceManager`, subscribe/copy-to buffers, command history); `ActiveComputeState` (handles each `ComputeCommand`, processes peeks including persist fast-path) |
| `src/compute_state/peek_result_iterator.rs` | 304 | Cursor-based row extraction for peek processing |
| `src/compute_state/peek_stash.rs` | 254 | Offloads large peek results to persist blobs |
| `src/render.rs` | 1,909 | `build_compute_dataflow` — source import, operator wiring, index/sink export |
| `src/sink/` | 3,843 | MV sink, subscribe, CopyToS3, continual-task, refresh; `Correction`/`CorrectionV2` update buffers |
| `src/logging/` | ~1,600 | Introspection dataflows (Timely, differential, reachability, compute events, Prometheus) |
| `src/arrangement.rs` | 12 | Re-exports `arrangement::manager` (`TraceManager`) |
| `src/row_spine.rs` | 715 | `Row`-specialized spine layouts (`DatumContainer`, `OffsetOptimized`) |
| `src/typedefs.rs` | 159 | Shared type aliases: spines, agents, batchers, `MzTimestamp`, `MzData` traits |
| `src/metrics.rs` | 512 | Prometheus metrics for the replica |
| `src/extensions/` | ~600 | `MzArrange` / `MzReduce` wrappers (attach heap-size logging); temporal bucket operator |
| `src/command_channel.rs` | 208 | Timely-based command fan-out from worker 0 to all workers |
| `src/memory_limiter.rs` | 348 | Process-global memory limit enforcer |
| `src/render/` | — | Translates `RenderPlan` IR into Timely operators; see [`src/render/CONTEXT.md`](src/render/CONTEXT.md) and [`src/render/ARCH_REVIEW.md`](src/render/ARCH_REVIEW.md) |

## Key concepts

- **`ComputeCommand` / `ComputeResponse`** — the protocol boundary with `mz-compute-client`. Commands include `CreateDataflow`, `Peek`, `Subscribe`, `AllowCompaction`. Responses include `PeekResponse`, `SubscribeResponse`, `FrontierUppers`.
- **`ActiveComputeState`** — a short-lived activated view of `ComputeState` bundled with the Timely worker; created each event-loop iteration.
- **Reconciliation** — on reconnect, `Worker::reconcile` diffs old command history against new commands to reuse compatible dataflows, compacting or dropping stale ones. Nonce tagging (`CommandReceiver` / `ResponseSender`) filters stale responses across reconnect boundaries.
- **`TraceManager`** — manages live differential arrangements per collection, enabling arrangement sharing across dataflows and compaction.
- **Dual-stream execution** — all dataflows carry parallel `(oks, errs)` streams (see `src/render/CONTEXT.md`).
- Source data enters exclusively via `mz-storage-operators::persist_source`; the compute layer never reads from Kafka, Postgres, etc. directly.
- `mz-persist-client` is used both for source reads (fast-path peeks, CT input) and for sink writes (MV, CopyToS3, CT output).

## Architectural notes (crate-level)

- **Compute/storage boundary** is persist exclusively — no direct external
  reads from dataflows. All inputs come from `mz-persist-client`.
- **`server::Worker::reconcile`** is the compute-side complement to controller
  command history; replays a snapshot of commands when a worker reconnects.
- **`TraceManager`** is the cross-dataflow arrangement registry — distinct
  from the controller's collection registry. Lives in this crate, not in the
  controller.

## Friction surfaced

See [`src/render/ARCH_REVIEW.md`](src/render/ARCH_REVIEW.md) (per-source
collection seam) and [`src/CONTEXT.md`](src/CONTEXT.md) (Correction V1/V2
manual two-variant enum shim with five lockstep methods — pre-deletion
checklist documented for V1 removal).

## Cross-references

- Controller: `mz-compute-client` (the client API to this crate).
- Types: `mz-compute-types` (LIR plan).
- Generated docs: `doc/developer/generated/compute/`.
