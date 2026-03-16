---
source: src/environmentd/src/lib.rs
revision: 3c798c488d
---

# environmentd

`mz-environmentd` is the top-level process that manages a single Materialize environment, integrating the adapter/coordinator, storage and compute controllers, HTTP and SQL network listeners, zero-downtime deployments, and telemetry.

Modules:
* `deployment` — zero-downtime deployment state machine and preflight checks
* `environmentd` — binary entry-point (`main`) and OS-level helpers (`sys`)
* `http` — embedded Axum HTTP server with auth, SQL, metrics, webhooks, MCP, and cluster-proxy endpoints
* `telemetry` — periodic Segment reporting loop
* `test_util` — integration test harness (feature-gated)

Key types: `Config` (server configuration), `Listeners` / `Listener<C>` (bound network listeners), `Server` (running server handle).
The crate depends heavily on `mz-adapter`, `mz-catalog`, `mz-controller`, `mz-pgwire`, `mz-server-core`, and `mz-persist-client`; it is the primary downstream consumer of all those crates.
