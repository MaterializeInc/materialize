# mz-compute (crate)

The Timely/differential execution engine for Materialize. Receives
`ComputeCommand`s from the controller (via `mz-clusterd`), instantiates and
manages dataflows, and returns `ComputeResponse`s (peek results, subscribe
updates, frontier progress).

See [`src/CONTEXT.md`](src/CONTEXT.md) for the full module map (server,
compute_state, render, sink, logging) and architectural notes.

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
