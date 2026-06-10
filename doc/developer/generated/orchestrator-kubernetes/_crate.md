---
source: src/orchestrator-kubernetes/src/lib.rs
revision: 41d1ef94fe
---

# mz-orchestrator-kubernetes

Implements Materialize's `Orchestrator` trait for Kubernetes, managing services as `StatefulSet` + `Service` pairs and supporting VPC endpoints and user secrets.
The crate root (`lib.rs`) contains `KubernetesOrchestrator` and its configuration, including affinity, tolerations, topology spread, security contexts, resource limits, and scheduling logic. `minDomains` in `TopologySpreadConstraint` is suppressed when the spread is soft (`ScheduleAnyway`, because Kubernetes rejects that combination) or when `availability_zones` is set (because node affinity already constrains the eligible topology domains, and an `minDomains` value exceeding the number of pinned zones causes replicas to remain pending); warnings are logged in both suppression cases.
TCP keepalive sysctls (`net.ipv4.tcp_keepalive_time=300`, `net.ipv4.tcp_keepalive_intvl=30`, `net.ipv4.tcp_keepalive_probes=3`) are set on every pod via `PodSecurityContext.sysctls`, regardless of whether a `service_fs_group` is configured.
Supporting modules handle VPC endpoint lifecycle (`cloud_resource_controller`), Kubernetes secret storage (`secrets`), and client construction (`util`).
Key dependencies are `kube`, `k8s-openapi`, `mz-orchestrator`, `mz-cloud-resources`, and `mz-secrets`; downstream consumers include `environmentd` and any component that needs to spin up or inspect Kubernetes workloads.
