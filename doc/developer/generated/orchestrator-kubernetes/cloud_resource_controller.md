---
source: src/orchestrator-kubernetes/src/cloud_resource_controller.rs
revision: 20c10b7f8d
---

# mz-orchestrator-kubernetes::cloud_resource_controller

Implements `CloudResourceController` and `CloudResourceReader` for `KubernetesOrchestrator`, managing VPC endpoint custom resources in Kubernetes.
Provides `KubernetesResourceReader`, a standalone reader that can look up `VpcEndpointStatus` by catalog item ID without a full orchestrator instance.
All Kubernetes API calls are wrapped with retry logic (up to 10 minutes, clamped at 10-second backoff) to tolerate transient failures.
