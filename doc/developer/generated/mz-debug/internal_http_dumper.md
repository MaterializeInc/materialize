---
source: src/mz-debug/src/internal_http_dumper.rs
revision: 047f99dfe6
---

# mz-debug::internal_http_dumper

Implements `HttpDumpClient` and top-level orchestration functions `dump_emulator_http_resources` and `dump_self_managed_http_resources` for collecting heap profiles and Prometheus metrics from running Materialize processes.
`HttpDumpClient` streams HTTP responses directly to disk and handles both HTTPS and HTTP with a fallback, as well as optional Basic authentication.
For self-managed (Kubernetes) environments, the module sets up per-service `kubectl` port-forwards to reach internal and external HTTP endpoints for each `environmentd` and `clusterd` instance; for emulator environments it connects directly to the container IP.
Port selection logic is governed by `AuthMode`: unauthenticated deployments use the internal HTTP port (6878) for all data, while password-authenticated `environmentd` uses the external port (6877) for heap profiles.
