---
source: src/mz-debug/src/main.rs
revision: 265f7af9cd
---

# mz-debug

A CLI diagnostic tool for collecting debug information from Materialize deployments, supporting two modes: `self-managed` (Kubernetes) and `emulator` (local Docker).

In `self-managed` mode the tool port-forwards to the `environmentd` SQL port using `kubectl`, collects Kubernetes resources (pods, services, CRDs, logs, etc.) via the kube API, and scrapes heap profiles and Prometheus metrics from internal HTTP endpoints.
In `emulator` mode it connects to a Docker container directly using its IP address.
In both modes it optionally connects to Materialize via pgwire and exports ~80 system catalog relations to CSV files, then zips everything into a single archive.

Key types: `Args` / `DebugModeArgs` (CLI argument structure), `AuthMode` (none or password credentials), `Context` / `SelfManagedContext` / `EmulatorContext` (runtime state), `ContainerDumper` (trait implemented by `K8sDumper` and `DockerDumper`).

Key dependencies: `kube`, `k8s-openapi`, `tokio-postgres`, `reqwest`, `mz-cloud-resources` (for the Materialize CRD type), `mz-server-core` (for `AuthenticatorKind`), `mz-sql-parser`, `mz-tls-util`.
Downstream consumers: primarily used as a standalone binary by operators debugging self-managed Materialize clusters.

## Module structure

* `main.rs` — entry point, CLI definitions, context initialization, top-level orchestration
* `utils.rs` — path formatting, zip creation, tracing log file, auth-mode detection from k8s
* `docker_dumper.rs` — `DockerDumper` / `ContainerDumper` for emulator environments
* `kubectl_port_forwarder.rs` — `KubectlPortForwarder`, `PortForwardConnection`, service discovery
* `internal_http_dumper.rs` — `HttpDumpClient`, heap profile and Prometheus metric collection
* `system_catalog_dumper.rs` — `SystemCatalogDumper`, pgwire-based catalog export to CSV
* `k8s_dumper.rs` — `K8sDumper` / `ContainerDumper` for Kubernetes environments
