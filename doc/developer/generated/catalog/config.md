---
source: src/catalog/src/config.rs
revision: 43de28997e
---

# catalog::config

Defines `Config` and `StateConfig`, the structs used to open and configure the catalog.
`Config` bundles the durable storage backend, a metrics registry, and the `StateConfig`.
`StateConfig` collects all initialization parameters: build info, environment ID, boot timestamp, cluster replica size map, builtin cluster configs, system parameter defaults, feature flags, connection context, expression cache override, helm chart version, license key, and more.
Also defines `ClusterReplicaSizeMap` (mapping user-facing size strings to `ReplicaAllocation`), `BuiltinItemMigrationConfig`, and `AwsPrincipalContext` for resource allocation and connection configuration.
