---
source: src/orchestratord/src/controller/console.rs
revision: 63d77d5016
---

# mz-orchestratord::controller::console

Implements the Kubernetes controller for `Console` custom resources.
Reconciles a `Console` CR by managing a Deployment, Services, ConfigMaps, NetworkPolicies, and optional cert-manager `Certificate` resources.
The `AppConfig` written to the console ConfigMap includes an optional `balancerd_dns_names` field populated from `console.spec.balancerd.dns_names`.
