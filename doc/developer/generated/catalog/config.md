---
source: src/catalog/src/config.rs
revision: b1c657d28e
---

# catalog::config

Defines `Config` and `StateConfig`, the structs used to open and configure the catalog.
`Config` bundles the durable storage backend, a metrics registry, and the `StateConfig`.
`StateConfig` collects all initialization parameters: build info, environment ID, boot timestamp, cluster replica size map, builtin cluster configs, system parameter defaults, feature flags, and more.
Also defines `ClusterReplicaSizeMap` and `DefaultReplicaAllocation` for mapping user-facing size strings to resource allocations.
