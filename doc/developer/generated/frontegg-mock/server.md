---
source: src/frontegg-mock/src/server.rs
revision: 88b6028c79
---

# frontegg-mock::server

Defines `FronteggMockServer` and the shared `Context` struct that holds all server state.
`FronteggMockServer::start` builds the complete axum `Router` with all Frontegg API routes (auth, users, groups, SCIM, SSO, tenant tokens) and the three middleware layers, then binds a `TcpListener` and spawns the serving task.
`Context` holds JWT keys, in-memory user/token/group/SSO/SCIM stores (all behind `Mutex`), the role-update channel receiver, and test-observable counters (`refreshes`, `auth_requests`, `enable_auth`).
`wait_for_auth` provides a retry-based synchronization primitive for integration tests waiting for a re-authentication event.
