---
source: src/catalog/src/config.rs
revision: c2cb53b0d0
---

# catalog::config

Defines `Config` and `StateConfig`, the structs used to open and configure the catalog.
`Config` bundles the durable storage backend, a metrics registry, and the `StateConfig`.
`StateConfig` collects all initialization parameters: build info, environment ID, boot timestamp, cluster replica size map, builtin cluster configs, system parameter defaults, feature flags, connection context, expression cache override, helm chart version, license key, and more.
Also defines `ClusterReplicaSizeMap` (mapping user-facing size strings to `ReplicaAllocation`), `BuiltinItemMigrationConfig`, `AwsPrincipalContext`, and `DefaultReplicaAllocation` for resource allocation configuration.
