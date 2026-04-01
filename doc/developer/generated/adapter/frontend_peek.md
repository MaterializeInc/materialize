---
source: src/adapter/src/frontend_peek.rs
revision: f7c755e1ed
---

# adapter::frontend_peek

Implements the "frontend peek" sequencing path, where SELECT query optimization and fast-path execution are performed in the pgwire connection task rather than the coordinator's main loop.
The module sequences the full peek pipeline — plan resolution, timestamp determination, fast-path detection, persist fast-path execution, and slow-path dispatch via `Command::ExecuteSlowPathPeek` — while registering the peek with the coordinator for cancellation support and statement logging.
`PeekClient::try_frontend_peek` is the main entry point, delegating fast-path execution to the compute or persist layer and sending slow-path plans back to the coordinator via the command channel.
The module is guarded by the `ENABLE_FRONTEND_SUBSCRIBES` dyncfg and also handles `SUBSCRIBE` statements via a parallel frontend path.
