---
source: src/environmentd/src/lib.rs
revision: aaed3fa7d3
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
`Config` carries both `cors_allowed_origin` (the computed `AllowOrigin` predicate used by the CORS layer) and `cors_allowed_origin_list` (the raw `Vec<HeaderValue>` used by endpoints such as MCP for server-side origin validation against DNS rebinding attacks).
The crate depends heavily on `mz-adapter`, `mz-catalog`, `mz-controller`, `mz-pgwire`, `mz-server-core`, and `mz-persist-client`; it is the primary downstream consumer of all those crates.
