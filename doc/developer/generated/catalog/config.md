---
source: src/catalog/src/config.rs
revision: fd1dd6e62b
---

# catalog::config

Defines `Config` and `StateConfig`, the structs used to open and configure the catalog.
`Config` bundles the durable storage backend, a metrics registry, and the `StateConfig`.
`StateConfig` collects all initialization parameters: build info, environment ID, boot timestamp, cluster replica size map, builtin cluster configs, system parameter defaults, feature flags, connection context, expression cache override, helm chart version, license key, and more.
Also defines `ClusterReplicaSizeMap` (mapping user-facing size strings to `ReplicaAllocation`), `BuiltinItemMigrationConfig`, and `AwsPrincipalContext` for resource allocation and connection configuration. `ReplicaAllocation` includes a `family: Option<String>` field identifying the coarse size family (e.g. `"D"` or `"legacy"`) used as the `replica_size_family` attribute when evaluating replica-local scoped feature flags; when `None`, the family is derived from `is_cc`. `AwsPrincipalContext::to_principal_string` constructs the IAM role ARN in the format `arn:aws:iam::<account_id>:role/mz_<prefix>_<suffix>`; the `mz_catalog.mz_aws_privatelink_connections` materialized view reconstructs this same format in SQL from the `mz_aws_account_id()` and `mz_aws_external_id_prefix()` plan-time functions, so the two must be kept in sync.
