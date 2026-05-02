---
source: src/orchestrator-kubernetes/src/lib.rs
revision: 991dfbf90c
---

# mz-orchestrator-kubernetes

Implements Materialize's `Orchestrator` trait for Kubernetes, managing services as `StatefulSet` + `Service` pairs and supporting VPC endpoints and user secrets.
The crate root (`lib.rs`) contains `KubernetesOrchestrator` and its configuration, including affinity, tolerations, topology spread, security contexts, resource limits, and scheduling logic. When a topology spread constraint is configured as soft (`ScheduleAnyway`), `minDomains` is suppressed because Kubernetes rejects that combination; a warning is logged if `min_domains` is set alongside `soft: true`.
TCP keepalive sysctls (`net.ipv4.tcp_keepalive_time=300`, `net.ipv4.tcp_keepalive_intvl=30`, `net.ipv4.tcp_keepalive_probes=3`) are set on every pod via `PodSecurityContext.sysctls`, regardless of whether a `service_fs_group` is configured.
Supporting modules handle VPC endpoint lifecycle (`cloud_resource_controller`), Kubernetes secret storage (`secrets`), and client construction (`util`).
Key dependencies are `kube`, `k8s-openapi`, `mz-orchestrator`, `mz-cloud-resources`, and `mz-secrets`; downstream consumers include `environmentd` and any component that needs to spin up or inspect Kubernetes workloads.
