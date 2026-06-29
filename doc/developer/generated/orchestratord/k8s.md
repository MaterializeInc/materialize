---
source: src/orchestratord/src/k8s.rs
revision: ff4065dc30
---

# mz-orchestratord::k8s

Provides generic Kubernetes helper functions used by all controllers: `get_resource`, `apply_resource` (server-side apply), `replace_resource`, `delete_resource` (foreground deletion with finalization), and `register_crds` (registers Materialize, Balancer, Console, and VpcEndpoint CRDs).
`register_crds` accepts an optional `ConversionWebhookConfig` parameter; when present, both `v1` and `v1alpha1` Materialize CRDs are registered with a webhook conversion strategy pointing at the given service, CA cert, and port. When absent, only `v1alpha1` is registered. `ConversionWebhookConfig` holds the service name, namespace, port, and path to a CA certificate file whose contents are read and embedded in the `CustomResourceConversion`.
