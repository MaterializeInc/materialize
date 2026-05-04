# Architecture review — `environmentd::http`

Scope: `src/environmentd/src/http/` and `http.rs` (≈ 5,716 LOC of Rust).

## 1. `execute_request` as an undifferentiated SQL dispatch sink

**Files**
- `src/environmentd/src/http/sql.rs` — `execute_request` (~200 lines) serves REST, WebSocket, and MCP handlers.
- `src/environmentd/src/http/mcp.rs` — calls `execute_request` for `query`/`read_data_product` tools.

**Problem**
`execute_request` is the *Implementation* for both interactive multi-statement REST execution (user-driven, arbitrary statements, COPY) and MCP tool invocations (read-only, bounded, no COPY). It carries the full complexity of both: transaction lifecycle, COPY dispatch, WebSocket state, statement logging. MCP is forced to thread through the full loop and then discard most of it. This is a *parallel predicates* pattern: the function tests `is_mcp` vs. `is_ws` vs. `is_http` inside a shared loop.

**Solution sketch**
Introduce a `SqlExecutor` trait with a `run_statement` method. The current `execute_request` loop becomes the `MultiStatementExecutor` implementation; MCP gets a `McpExecutor` that only handles single read-only statements without COPY or transaction complexity. The shared session/auth handling stays in a common preamble; only the loop body splits. Depth: the MCP path sheds ~60% of unreachable branches.

**Risk**
`execute_request` is the system's least-wrong centralization of statement logging and error normalization. A trait split must not fork those.

## 2. Auth middleware locality: `http.rs` owns too much

**Files**
- `src/environmentd/src/http.rs` — ~1,391 lines; auth middleware, router construction, TLS, session management, `WsState`, `AuthedClient` extraction, CORS, connection counting.

**Problem**
`http.rs` is the *Module* for the HTTP server but also the *Implementation* of auth, which is at least as complex as any single handler file. Auth logic (Frontegg JWT decode, OIDC exchange, password hash, session cookie) is interleaved with routing. Changes to auth touch the same file as adding a route.

**Solution sketch**
Extract auth middleware into `http/auth.rs` (or keep as a private `auth` submodule). `http.rs` becomes a thin router that assembles middleware + submodule routes. This matches how `sql.rs`, `mcp.rs`, and `cluster.rs` each own one surface. Depth: each concern gets a coherent surface; auth unit-tests can be written without standing up a full router.

**Risk**
Low. Pure relocation; the `AuthedClient` type boundary already exists as the seam.

## 3. (Honest skip) MCP protocol complexity in `mcp.rs`

`mcp.rs` at 1,724 LOC is large but coherent: it owns all MCP protocol wire handling (JSON-RPC framing, tool dispatch, feature-flag gating, DNS-rebinding protection, AST-based read-only validation). There is no sub-surface worth splitting further. The deletion test holds: the 5 tools, 2 endpoints, and 3 security checks are all independently motivated.

## What this review did not reach

- Per-endpoint auth rule correctness (which endpoints are correctly gated by which `AuthenticatorKind`).
- Webhook HMAC validation against timing attacks.
- WebSocket session lifecycle under disconnect.
