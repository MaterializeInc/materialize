---
source: src/clusterd-test-driver/src/lib.rs
revision: 6d4c0fbb2b
---

# mz-clusterd-test-driver

Headless frontend to `clusterd` for scripted compute tests. Connects to an externally-managed clusterd over CTP (compute transport protocol) without launching one, enabling deterministic, script-driven tests of compute dataflow and peek behavior without the full Materialize stack.

## Module structure

* `ctp` — CTP connection and the `Hello` step; `connect_and_hello` opens a transport connection to clusterd and sends the `Hello` handshake message.
* `driver` — `Driver` struct: the top-level API for test use cases. Wraps a `PersistHost`, a compute address, a `ComputeSender`, and a `Responses` handle. Provides `connect`, `reconnect`, and methods for the full controller handshake (`create_instance`, `update_configuration`, `initialization_complete`) and for sending compute commands and waiting for responses.
* `responses` — receive side of the CTP connection. A background pump task routes `ComputeResponse`s into per-id frontier `watch` channels, per-uuid peek `oneshot` channels, and a raw `broadcast`. `Responses` exposes `await_frontier`, `await_peek`, `await_subscribe`, and `recv_raw`.
* `persist_host` — `PersistHost`: embeds a PubSub server and a `PersistClientCache` wired to it. `clusterd` connects to `pubsub_url()`. Provides `start` (ephemeral localhost port) and `start_on` for a fixed address.
* `dataflow` — `DataflowBuilder`: assembles `DataflowDescription<RenderPlan, CollectionMetadata>` from persist imports, MIR objects, and index exports, handling MIR-to-LIR lowering. `index_dataflow` is thin sugar for the single-index shape.
* `data` — `Cell` enum (owned scalar values for synthetic generation and explicit script values), `pack_cells`, `synth_rows`, `write_rows_single_ts`, `write_rows_spread` (direct persist writes).
* `script` — `Command` enum and script AST types; parses the structured command format used by `text`.
* `text` — text script format parser and `run` function; executes a `datadriven`-style command script against `clusterd`, comparing actual output to expected `----` blocks (with `REWRITE` support).
* `target` — resolves the clusterd compute controller address from `CLUSTERD_COMPUTE_ADDR` (default `127.0.0.1:2101`); `should_run_e2e_test` gates tests on whether the env var is set.

## Key dependencies

`mz-compute-client`, `mz-compute-types`, `mz-persist-client`, `mz-expr`, `mz-expr-parser`, `mz-repr`, `mz-storage-types`, `mz-service`.
