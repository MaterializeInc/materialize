# environmentd

`mz-environmentd` is the top-level process binary for a single Materialize environment. It is the primary downstream consumer of `mz-adapter`, `mz-catalog`, `mz-controller`, `mz-pgwire`, `mz-server-core`, and `mz-persist-client`.

## Structure (LOC ≈ 28,430)

| Subtree | LOC | Role |
|---|---|---|
| `src/` | ~12,106 | Library: server lifecycle, HTTP, 0dt deployment, telemetry *(see [src/CONTEXT.md](src/CONTEXT.md))* |
| `src/http/` | ~5,716 | Axum HTTP server, all endpoints *(see [src/http/CONTEXT.md](src/http/CONTEXT.md))* |
| `tests/` | ~16,303 | Integration tests *(see [tests/CONTEXT.md](tests/CONTEXT.md))* |

## Responsibilities

- **Process entry-point.** Parses `clap`-derived `Args`; constructs `Config`; binds SQL and HTTP listeners; calls `Listeners::serve`.
- **Server assembly.** Wires together adapter coordinator, storage/compute controllers, pgwire, HTTP (Axum), and auth into a running server.
- **Zero-downtime deployment.** Implements the 0dt `Initializing → CatchingUp → ReadyToPromote → Promoting → IsLeader` state machine; orchestrator drives promotion via internal HTTP.
- **HTTP surface.** SQL over REST and WebSocket, Prometheus metrics, MCP AI agent interface, webhook source ingestion, cluster replica proxy, catalog introspection, memory/heap profiling, console proxy.
- **Telemetry.** Periodic Segment reporting of environment statistics.

## Architecture notes

- `environmentd` is intentionally thin on business logic: orchestration happens in `mz-adapter`; query planning/execution in `mz-sql`/`mz-compute`/`mz-storage`; catalog in `mz-catalog`. This crate is the *Adapter* in the DDD sense — it translates OS/network events into adapter method calls.
- The 0dt state machine is the most complex original logic here; it lives at the `Seam` between the orchestrator (external) and the adapter (internal) and is tested by a dedicated section of `tests/server.rs`.
- `test_util.rs` (1,831 LOC, test-feature-gated) is the integration harness used by all sibling test files. Its size reflects the complexity of standing up a real server.

## ARCH_REVIEW highlights (bubbled from http/)

Two friction points identified in `src/http/`:

1. **`execute_request` over-generalization** — single function serves REST, WebSocket, and MCP despite divergent requirements; MCP paths traverse unused COPY/multi-statement machinery. Extract a `SqlExecutor` trait to split responsibilities.
2. **Auth logic co-located with routing in `http.rs`** — 1,391-line file mixes middleware, session management, and router construction. Extract to `http/auth.rs` using the existing `AuthedClient` seam.

## What should bubble up to `src/CONTEXT.md`

- `mz-environmentd` is the process-level integration point for all major Materialize subsystems.
- The 0dt deployment protocol (preflight + state machine + internal HTTP promotion API) is unique to this crate.
- The HTTP surface is where external and internal interfaces converge: external SQL/webhooks/MCP share the same Axum server as internal metrics/profiling/console proxy.
- Test:source LOC ratio is ~1.35:1 (16K tests / 12K source), reflecting the importance of end-to-end integration coverage at this layer.

## Cross-references

- Generated developer docs: `doc/developer/generated/environmentd/`.
- Adapter entry-point: `mz-adapter::serve` (called from `lib.rs`).
- pgwire: `mz-pgwire::Server` (wrapped by `Listener<SqlListenerConfig>::serve_sql`).
