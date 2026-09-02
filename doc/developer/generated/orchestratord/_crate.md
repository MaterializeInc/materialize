---
source: src/orchestratord/src/lib.rs
revision: 0f8d369b92
---

# mz-orchestratord

Kubernetes operator (controller) for managing Materialize regions in cloud deployments.
Reconciles `Materialize`, `Balancer`, and `Console` custom resources by creating and updating Kubernetes StatefulSets, Deployments, Services, RBAC objects, NetworkPolicies, and cert-manager certificates.
The `Error` enum wraps `anyhow`, `kube`, and `reqwest` errors; `matching_image_from_environmentd_image_ref` constructs sibling image references (e.g., for console) from the environmentd image tag; `parse_image_tag` extracts the tag from an OCI image reference, correctly ignoring registry-host ports and `@sha256:` digests.
The `webhook` module serves the CRD conversion webhook that converts between `v1` and `v1alpha1` Materialize resources.
The `gcp_node_upgrade` module watches for GCP node upgrade events and forces new rollouts via the `FORCE_ROLLOUT_ANNOTATION` annotation without modifying spec fields that may be managed by external tools such as Terraform.
Key dependencies include `kube`, `k8s-openapi`, `mz-cloud-resources`, `mz-cloud-provider`, `mz-license-keys`, `mz-orchestrator-kubernetes`, and `mz-orchestrator-tracing`.
