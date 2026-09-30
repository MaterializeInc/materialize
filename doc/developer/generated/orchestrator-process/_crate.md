---
source: src/orchestrator-process/src/lib.rs
revision: f12639fd7a
---

# mz-orchestrator-process

Implements the `Orchestrator` and `NamespacedOrchestrator` traits by spawning and supervising local child processes, primarily for development use.
`ProcessOrchestrator` manages service lifecycle: spawning processes from image binaries, health-monitoring via Unix sockets, propagating crashes, collecting CPU/memory metrics via `sysinfo`, and optionally proxying named ports over TCP for local debugging.
When dropping a service, it kills any orphaned processes left from a prior orchestrator incarnation by reading their pid files from the run directory; if a pid file references no live process, a warning is emitted rather than silently skipping it.
`ProcessState` tracks each process's current status, status time, and a `restart_count` that increments on each transition to `NotReady`; the count is monotonic for the lifetime of a given `ProcessState` and is included in every emitted `ServiceEvent` so consumers can detect restarts they might otherwise miss by only sampling the current status.
Services communicate over Unix domain sockets; a SHA-1-based naming scheme derives socket paths from service IDs.
Depends on `mz-orchestrator`, `mz-secrets`, and `mz-ore`; the `secrets` submodule provides filesystem-backed secret management.

## Module structure

* `secrets` — filesystem `SecretsController`/`SecretsReader` impl
