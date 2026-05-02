---
source: src/orchestrator-process/src/lib.rs
revision: f6f7d70cc4
---

# mz-orchestrator-process

Implements the `Orchestrator` and `NamespacedOrchestrator` traits by spawning and supervising local child processes, primarily for development use.
`ProcessOrchestrator` manages service lifecycle: spawning processes from image binaries, health-monitoring via Unix sockets, propagating crashes, collecting CPU/memory metrics via `sysinfo`, and optionally proxying named ports over TCP for local debugging.
Services communicate over Unix domain sockets; a SHA-1-based naming scheme derives socket paths from service IDs.
Depends on `mz-orchestrator`, `mz-secrets`, and `mz-ore`; the `secrets` submodule provides filesystem-backed secret management.

## Module structure

* `secrets` — filesystem `SecretsController`/`SecretsReader` impl
