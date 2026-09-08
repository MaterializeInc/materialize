---
source: src/mz-debug/src/internal_http_dumper.rs
revision: 8b438b66ce
---

# mz-debug::internal_http_dumper

Implements `HttpDumpClient` and top-level orchestration functions `dump_emulator_http_resources` and `dump_self_managed_http_resources` for collecting heap profiles, CPU profiles, and Prometheus metrics from running Materialize processes.
`HttpDumpClient` streams HTTP responses directly to disk and handles both HTTPS and HTTP with a fallback, as well as optional Basic authentication.
For self-managed (Kubernetes) environments, `dump_self_managed_http_resources` expands each cluster service into its backing pods via `find_service_pods`, then scrapes heap profiles and Prometheus metrics per pod using individual `kubectl port-forward` connections to each pod (not the service). This ensures that every process in a scaled multi-pod replica is reached. A failure on one pod is logged and skipped rather than aborting the dump. CPU profiles are captured per-pod in parallel afterwards. For emulator environments, `dump_emulator_http_resources` connects directly to the container IP.
Port selection logic is governed by `AuthMode`: unauthenticated deployments use the internal HTTP port (6878) for all data, while password-authenticated `environmentd` uses the external port (6877) for heap and CPU profiles.
CPU profiles are captured via a POST to each service's CPU profile endpoint after heap profiling completes; the server temporarily disables memory profiling during capture and restores it afterwards. The helper `dump_cpu_profile_and_verify_memory` probes the profiling mode endpoint to check CPU profiling support and verify memory profiling is active again after the capture.
