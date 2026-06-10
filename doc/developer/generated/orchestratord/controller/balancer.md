---
source: src/orchestratord/src/controller/balancer.rs
revision: 927cd62f6b
---

# mz-orchestratord::controller::balancer

Implements the Kubernetes controller for `Balancer` custom resources.
Reconciles a `Balancer` CR by creating or updating a Deployment, a headless ClusterIP Service, and optionally cert-manager `Certificate` resources for TLS.
Depends on the `k8s` helpers for apply/replace operations and the `tls` module for certificate creation.
