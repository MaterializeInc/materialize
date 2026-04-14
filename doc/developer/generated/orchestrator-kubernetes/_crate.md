---
source: src/orchestrator-kubernetes/src/lib.rs
revision: 117286ee15
---

# mz-orchestrator-kubernetes

Implements Materialize's `Orchestrator` trait for Kubernetes, managing services as `StatefulSet` + `Service` pairs and supporting VPC endpoints and user secrets.
The crate root (`lib.rs`) contains `KubernetesOrchestrator` and its configuration, including affinity, tolerations, topology spread, security contexts, resource limits, and scheduling logic.
Supporting modules handle VPC endpoint lifecycle (`cloud_resource_controller`), Kubernetes secret storage (`secrets`), and client construction (`util`).
Key dependencies are `kube`, `k8s-openapi`, `mz-orchestrator`, `mz-cloud-resources`, and `mz-secrets`; downstream consumers include `environmentd` and any component that needs to spin up or inspect Kubernetes workloads.
