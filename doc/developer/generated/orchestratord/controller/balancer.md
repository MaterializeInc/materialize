---
source: src/orchestratord/src/controller/balancer.rs
revision: 5912b36bab
---

# mz-orchestratord::controller::balancer

Implements the Kubernetes controller for `Balancer` custom resources.
Reconciles a `Balancer` CR by creating or updating a Deployment, a headless ClusterIP Service, and optionally cert-manager `Certificate` resources for TLS.
Depends on the `k8s` helpers for apply/replace operations and the `tls` module for certificate creation.

When `BalancerSpec::configmap_name` is set, `create_deployment_object` mounts the named ConfigMap as a read-only directory volume at `/etc/balancerd` (using a directory mount rather than a `subPath` mount so that ConfigMap updates are reflected in the running container) and passes `--config-sync-file-path=/etc/balancerd/config.json` and `--config-sync-loop-interval=1s` to balancerd, causing it to reread the file every second. The ConfigMap is mounted with `optional: false`, so the pod will not start if the ConfigMap is absent. When `configmap_name` is absent, no volume or sync arguments are added.
