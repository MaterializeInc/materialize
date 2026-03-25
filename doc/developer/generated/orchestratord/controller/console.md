---
source: src/orchestratord/src/controller/console.rs
revision: 82d92a7fad
---

# mz-orchestratord::controller::console

Implements the Kubernetes controller for `Console` custom resources.
Reconciles a `Console` CR by managing a Deployment, Services, ConfigMaps, NetworkPolicies, and optional cert-manager `Certificate` resources.
