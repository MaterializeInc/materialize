# compute::src (mz-compute)

The Timely/differential execution engine for Materialize. Receives
`ComputeCommand`s from the controller (via `mz-clusterd`), instantiates and
manages dataflows, and returns `ComputeResponse`s (peek results, subscribe
updates, frontier progress).

## Subdirectory reviewed

- [`render/`](render/CONTEXT.md) — translates `RenderPlan` IR into Timely
  operators; see [`render/ARCH_REVIEW.md`](render/ARCH_REVIEW.md) for friction.

## Files

| File/Dir | LOC | What it owns |
|---|---|---|
| `server.rs` | 822 | `serve` entry point; `Worker` event loop (step Timely, handle commands, drain peeks/subscribes, periodic maintenance); `reconcile` on reconnect; `CommandReceiver` / `ResponseSender` nonce tagging |
| `compute_state.rs` | 1,896 | `ComputeState` (collections, `TraceManager`, subscribe/copy-to buffers, command history); `ActiveComputeState` (handles each `ComputeCommand`, processes peeks including persist fast-path) |
| `compute_state/peek_result_iterator.rs` | 304 | Cursor-based row extraction for peek processing |
| `compute_state/peek_stash.rs` | 254 | Offloads large peek results to persist blobs |
| `render.rs` | 1,909 | `build_compute_dataflow` — source import, operator wiring, index/sink export |
| `sink/` | 3,843 | MV sink, subscribe, CopyToS3, continual-task, refresh; `Correction`/`CorrectionV2` update buffers |
| `logging/` | ~1,600 | Introspection dataflows (Timely, differential, reachability, compute events, Prometheus) |
| `arrangement.rs` | 12 | Re-exports `arrangement::manager` (`TraceManager`) |
| `row_spine.rs` | 715 | `Row`-specialized spine layouts (`DatumContainer`, `OffsetOptimized`) |
| `typedefs.rs` | 159 | Shared type aliases: spines, agents, batchers, `MzTimestamp`, `MzData` traits |
| `metrics.rs` | 512 | Prometheus metrics for the replica |
| `extensions/` | ~600 | `MzArrange` / `MzReduce` wrappers (attach heap-size logging); temporal bucket operator |
| `command_channel.rs` | 208 | Timely-based command fan-out from worker 0 to all workers |
| `memory_limiter.rs` | 348 | Process-global memory limit enforcer |

## Key concepts

- **`ComputeCommand` / `ComputeResponse`** — the protocol boundary with
  `mz-compute-client`. Commands include `CreateDataflow`, `Peek`, `Subscribe`,
  `AllowCompaction`. Responses include `PeekResponse`, `SubscribeResponse`,
  `FrontierUppers`.
- **`ActiveComputeState`** — a short-lived activated view of `ComputeState`
  bundled with the Timely worker; created each event-loop iteration.
- **Reconciliation** — on reconnect, `Worker::reconcile` diffs old command
  history against new commands to reuse compatible dataflows, compacting or
  dropping stale ones. Nonce tagging (`CommandReceiver` / `ResponseSender`)
  filters stale responses across reconnect boundaries.
- **`TraceManager`** — manages live differential arrangements per collection,
  enabling arrangement sharing across dataflows and compaction.
- **Dual-stream execution** — all dataflows carry parallel `(oks, errs)` streams
  (see `render/CONTEXT.md`).

## Architecture notes

- `mz-compute` is entirely internal to a replica process. The only external
  boundary is the `ComputeCommand`/`ComputeResponse` protocol from
  `mz-compute-client`.
- Source data enters exclusively via `mz-storage-operators::persist_source`; the
  compute layer never reads from Kafka, Postgres, etc. directly.
- `mz-persist-client` is used both for source reads (fast-path peeks, CT input)
  and for sink writes (MV, CopyToS3, CT output).

## Friction bubbled from `render/`

- `sink/correction.rs` — `Correction<D>` enum manually dispatches to
  `CorrectionV1` / `CorrectionV2` via repeated `match self` arms; no shared
  trait. See `render/ARCH_REVIEW.md §1` for the deletion checklist and trait-
  extraction sketch.

## What should bubble to `src/CONTEXT.md`

- **Compute/storage boundary is persist.** `mz-compute` reads sources and writes
  sinks exclusively through `mz-persist-client`; the storage layer is never
  called directly from within a dataflow.
- **Reconciliation seam.** `server::Worker::reconcile` is the mechanism that
  makes compute stateful across controller reconnects; it is the compute-side
  complement to `mz-compute-client`'s command history.
- **`TraceManager` as shared-arrangement registry.** Cross-dataflow arrangement
  reuse is mediated by `TraceManager` in `mz-compute`, not by the controller.

## Cross-references

- Instantiated by: `mz-clusterd` via `server::serve`.
- Command/response protocol: `mz-compute-client`.
- Plan IR: `mz-compute-types::plan::render_plan`.
- Generated docs: `doc/developer/generated/compute/`.
