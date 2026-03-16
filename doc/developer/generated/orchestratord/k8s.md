---
source: src/orchestratord/src/k8s.rs
revision: 82d92a7fad
---

# mz-orchestratord::k8s

Provides generic Kubernetes helper functions used by all controllers: `get_resource`, `apply_resource` (server-side apply), `replace_resource`, `delete_resource` (foreground deletion with finalization), `register_crds` (registers Materialize, Balancer, Console, and VpcEndpoint CRDs), and `make_reflector` (creates a local cache of a Kubernetes resource kind backed by a kube-runtime reflector).
