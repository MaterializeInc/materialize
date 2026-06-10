---
source: src/compute/src/lib.rs
revision: 9727769b0d
---

# mz-compute

Materialize's compute layer: receives `ComputeCommand`s from the controller and executes them as Timely/differential dataflow programs, returning `ComputeResponse`s.

## Module structure

* `server` — entry point, worker event loop, client reconnect reconciliation
* `compute_state` — per-worker state (pending peeks, subscribes, command history), peek processing, peek stash
* `arrangement` — `TraceManager` for managing differential arrangements per collection
* `render` — translates `RenderPlan` IR to Timely operators (join, reduce, top_k, threshold, flat_map, sinks, continual tasks)
* `sink` — sink implementations (materialized view, subscribe, copy-to-S3, refresh)
* `logging` — introspection dataflows (Timely, differential, reachability, compute-specific events)
* `extensions` — wrappers around `arrange` and `reduce` that attach heap-size logging; temporal bucket operator
* `typedefs` — shared type aliases for spines, agents, batchers, and data-bound traits
* `metrics` — Prometheus metrics for the replica
* `memory_limiter` — process-global memory limit enforcer
* `command_channel` — Timely dataflow-based command fan-out from worker 0 to all workers

## Key dependencies

`mz-compute-client` (command/response protocol), `mz-compute-types` (plan IR, dyncfgs), `mz-expr` (scalar expressions, MFP), `mz-repr` (Row, Datum, Timestamp), `mz-persist-client` (sink writes, fast-path peeks), `mz-storage-operators` (source reading), `mz-txn-wal` (txn-wal operator context), `timely` and `differential-dataflow` (execution runtime).

## Downstream consumers

`mz-clusterd` instantiates a compute server via `server::serve` and bridges it to the cluster client.
