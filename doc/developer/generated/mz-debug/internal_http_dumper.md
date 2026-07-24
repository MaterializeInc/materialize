---
source: src/mz-debug/src/internal_http_dumper.rs
revision: dc4dcf22d7
---

# mz-debug::internal_http_dumper

Implements `HttpDumpClient` and top-level orchestration functions `dump_emulator_http_resources` and `dump_self_managed_http_resources` for collecting heap profiles, CPU profiles, and Prometheus metrics from running Materialize processes.
`HttpDumpClient` streams HTTP responses directly to disk and handles both HTTPS and HTTP with a fallback, as well as optional Basic authentication.
For self-managed (Kubernetes) environments, the module sets up per-service `kubectl` port-forwards to reach internal and external HTTP endpoints for each `environmentd` and `clusterd` instance; for emulator environments it connects directly to the container IP.
Port selection logic is governed by `AuthMode`: unauthenticated deployments use the internal HTTP port (6878) for all data, while password-authenticated `environmentd` uses the external port (6877) for heap and CPU profiles.
CPU profiles are captured via a POST to each service's CPU profile endpoint after heap profiling completes; the server temporarily disables memory profiling during capture and restores it afterwards. The helper `dump_cpu_profile_and_verify_memory` probes the profiling mode endpoint to check CPU profiling support and verify memory profiling is active again after the capture.
