---
source: src/mz-debug/src/k8s_dumper.rs
revision: 265f7af9cd
---

# mz-debug::k8s_dumper

Implements `K8sDumper`, which implements `ContainerDumper` for Kubernetes-based (self-managed) environments.
`K8sResourceDumper<K>` is a generic helper that lists resources of type `K` via the kube API and serializes each to a YAML file; it is instantiated for dozens of resource types (pods, services, deployments, stateful sets, config maps, certificates, Materialize CRs, etc.).
`K8sDumper::dump_container_resources` runs all `kubectl describe` and API-list operations concurrently with `join_all`, collecting output from one or more namespaces plus cluster-scoped resources (nodes, storage classes, CRDs).
Pod logs (current and previous) are written per-pod via the kube `logs` API.
