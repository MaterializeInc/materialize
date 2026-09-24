---
source: src/orchestrator-kubernetes/src/lib.rs
revision: 3311df27ec
---

# mz-orchestrator-kubernetes

Implements Materialize's `Orchestrator` trait for Kubernetes, managing services as `StatefulSet` + `Service` pairs and supporting VPC endpoints and user secrets.
The crate root (`lib.rs`) contains `KubernetesOrchestrator` and its configuration, including affinity, tolerations, topology spread, security contexts, resource limits, scheduling logic, and a `priority_class_name: Option<String>` field in `KubernetesOrchestratorConfig` for assigning a `PriorityClass` to services. `minDomains` in `TopologySpreadConstraint` is suppressed when the spread is soft (`ScheduleAnyway`, because Kubernetes rejects that combination) or when `availability_zones` is set (because node affinity already constrains the eligible topology domains, and an `minDomains` value exceeding the number of pinned zones causes replicas to remain pending); warnings are logged in both suppression cases.
TCP keepalive sysctls (`net.ipv4.tcp_keepalive_time=300`, `net.ipv4.tcp_keepalive_intvl=30`, `net.ipv4.tcp_keepalive_probes=3`) are set on every pod via `PodSecurityContext.sysctls`, regardless of whether a `service_fs_group` is configured.
`watch_services` emits a `ServiceEvent` per pod, deriving `restart_count` as the sum of container restart counts from the pod's container statuses; this count is cumulative and survives gaps in the watch stream.
The internal `retry_async` helper used for all Kubernetes API calls logs failures at warn level for the first `RETRY_WARN_ATTEMPTS` (5) attempts, then escalates to error level; this prevents transient errors (such as RBAC propagation delays for a new service account) from reaching Sentry before the retry succeeds.
Supporting modules handle VPC endpoint lifecycle (`cloud_resource_controller`), Kubernetes secret storage (`secrets`), and client construction (`util`).
Key dependencies are `kube`, `k8s-openapi`, `mz-orchestrator`, `mz-cloud-resources`, and `mz-secrets`; downstream consumers include `environmentd` and any component that needs to spin up or inspect Kubernetes workloads.
