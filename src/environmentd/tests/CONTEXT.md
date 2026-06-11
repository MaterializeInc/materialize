# environmentd::tests

Integration test suite for `environmentd`. All tests run a real `environmentd` instance via `test_util::TestHarness` (feature-gated). Tests exercise the full server stack: pgwire, HTTP/WebSocket, auth, SQL semantics, 0dt deployments, tracing, and CLI.

## Files (LOC ≈ 16,303)

| File | What it covers |
|---|---|
| `server.rs` (~5,742) | End-to-end server integration: startup, shutdown, 0dt deployment lifecycle, internal HTTP endpoints (`/api/leader/status`, `/api/leader/promote`), catalog persistence, TLS, connection limits, statement logging |
| `auth.rs` (~5,370) | Auth mode coverage: `None`, `Password`, `Frontegg` (mock), `Oidc`; session cookie flow; token refresh; role-based access; TLS-required enforcement; WebSocket credential exchange |
| `sql.rs` (~3,900) | SQL correctness over HTTP and pgwire: multi-statement batches, transaction semantics, COPY, SUBSCRIBE, error propagation, parameter binding, system catalog queries |
| `pgwire.rs` (~878) | pgwire protocol: extended query, describe, notices, cancellation, connection counter |
| `tracing.rs` (~185) | Tracing/span propagation over HTTP and pgwire |
| `timezones.rs` (~153) | Timezone abbreviation and name correctness against stored CSV fixtures |
| `bootstrap_builtin_clusters.rs` (~38) | Builtin cluster creation and config on first boot |
| `cli.rs` (~37) | CLI flag parsing smoke tests |

## Key concepts

- **`TestHarness`** — configures and starts a full `environmentd` with in-memory or tmpdir persist; provides pgwire + HTTP clients; used by nearly every test.
- **0dt deployment tests** — `server.rs` drives the `Initializing → CatchingUp → ReadyToPromote → Promoting → IsLeader` lifecycle by starting two generations and using the internal HTTP API to trigger promotion.
- **Auth test coverage** — `auth.rs` is the most extensive file; tests each `AuthenticatorKind` in isolation and in combination; uses a mock Frontegg server.
- **MCP testdata** — `tests/testdata/mcp/` and `tests/testdata/http/` hold golden request/response fixtures for protocol-level testing.

## Cross-references

- Harness: `src/environmentd/src/test_util.rs`.
- Internal HTTP under test: `src/environmentd/src/http/` and the 0dt state machine in `src/environmentd/src/deployment/`.
- Generated developer docs: `doc/developer/generated/environmentd/test_util.md`.
