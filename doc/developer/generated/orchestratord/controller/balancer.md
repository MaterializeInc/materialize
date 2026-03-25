---
source: src/orchestratord/src/controller/balancer.rs
revision: 82d92a7fad
---

# mz-orchestratord::controller::balancer

Implements the Kubernetes controller for `Balancer` custom resources.
Reconciles a `Balancer` CR by creating or updating a Deployment, Services (internal and external), and optionally cert-manager `Certificate` resources for TLS.
Depends on the `k8s` helpers for apply/replace/reflector operations and the `tls` module for certificate creation.
