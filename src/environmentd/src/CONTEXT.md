# environmentd::src

Library root of `mz-environmentd`: integrates the adapter/coordinator, storage and compute controllers, HTTP and SQL network listeners, zero-downtime deployment, and telemetry into a single process.

## Modules

| Module | LOC | What it owns |
|---|---|---|
| `lib.rs` | ~892 | `Config`, `Listeners`, `Server`; startup sequence: TLS → HTTP boot → system-param sync → preflight → catalog open → adapter init → SQL boot → telemetry |
| `http` / `http.rs` | ~5,716 | Axum HTTP server: auth middleware, SQL/WebSocket execution, MCP, webhooks, metrics, cluster proxy *(see [http/CONTEXT.md](http/CONTEXT.md))* |
| `deployment/` | ~567 | 0dt state machine (`DeploymentState`: `Initializing → CatchingUp → ReadyToPromote → Promoting → IsLeader`) and preflight checks |
| `environmentd/` | ~thin | Binary entry-point: `main` (clap `Args` → `Config` → `Listeners::serve`) and OS-level helpers (`sys`) |
| `telemetry.rs` | ~249 | Periodic Segment reporting loop (1-hour interval); queries adapter for environment stats; emits `group` + `track` events |
| `test_util.rs` | ~1,831 | Integration test harness (feature-gated `"test"`): `TestServer`, `TestHarness`, helpers for starting a local environmentd |

## Key types

- **`Config`** — flat struct (~60 fields) covering TLS, controller, catalog, adapter, bootstrap, LaunchDarkly, AWS, and observability options.
- **`Listeners` / `Listener<C>`** — named map of pre-bound TCP sockets; split from `serve` so OS queues connections while the server boots.
- **`Server`** — running server handle; drop-order matters (SQL handles, HTTP handles, `_adapter_handle`).
- **`DeploymentState` / `DeploymentStateHandle`** — shared-state machine driving 0dt promotion; handle given to HTTP server so the orchestrator can poll and trigger promotion.

## Startup sequence (in `Listeners::serve`)

1. TLS context construction
2. HTTP servers launched (adapter client deferred via `oneshot`)
3. Catalog opened (`persist_backed_catalog_state`)
4. System parameter sync (LaunchDarkly or file)
5. 0dt preflight checks (`preflight_0dt`)
6. Durable catalog open (savepoint if read-only, full if leader)
7. Adapter init (`mz_adapter::serve`)
8. OIDC authenticator constructed
9. `adapter_client` sent to HTTP servers (unblocks deferred endpoints)
10. SQL listeners launched
11. Telemetry + system-param sync loops started

## Cross-references

- HTTP: see [`http/CONTEXT.md`](http/CONTEXT.md) and [`http/ARCH_REVIEW.md`](http/ARCH_REVIEW.md).
- Primary downstream of: `mz-adapter`, `mz-catalog`, `mz-controller`, `mz-pgwire`, `mz-server-core`, `mz-persist-client`.
- Generated developer docs: `doc/developer/generated/environmentd/`.
