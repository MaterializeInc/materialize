---
source: src/orchestratord/src/lib.rs
revision: 82d92a7fad
---

# mz-orchestratord

Kubernetes operator (controller) for managing Materialize regions in cloud deployments.
Reconciles `Materialize`, `Balancer`, and `Console` custom resources by creating and updating Kubernetes StatefulSets, Deployments, Services, RBAC objects, NetworkPolicies, and cert-manager certificates.
The `Error` enum wraps `anyhow`, `kube`, and `reqwest` errors; `matching_image_from_environmentd_image_ref` constructs sibling image references (e.g., for console) from the environmentd image tag.
Key dependencies include `kube`, `k8s-openapi`, `mz-cloud-resources`, `mz-cloud-provider`, `mz-license-keys`, `mz-orchestrator-kubernetes`, and `mz-orchestrator-tracing`.
