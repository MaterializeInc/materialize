---
source: src/orchestratord/src/k8s.rs
revision: 927cd62f6b
---

# mz-orchestratord::k8s

Provides generic Kubernetes helper functions used by all controllers: `get_resource`, `apply_resource` (server-side apply), `replace_resource`, `delete_resource` (foreground deletion with finalization), and `register_crds` (registers Materialize, Balancer, Console, and VpcEndpoint CRDs).
