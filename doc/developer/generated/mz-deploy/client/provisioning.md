---
source: src/mz-deploy/src/client/provisioning.rs
revision: 3f3bdb0535
---

# mz-deploy::client::provisioning

DDL provisioning operations.

Methods on `ProvisioningClient` issue idempotent `CREATE … IF NOT EXISTS` and `ALTER` statements to ensure that the target region's databases, schemas, and clusters match the project definition. All `create_*` methods use `IF NOT EXISTS` or catch "already exists" errors, so re-running provisioning on an already-provisioned environment is a no-op.

Provisioning must follow referential order: databases must be created before schemas, and schemas before clusters. Callers (e.g. `DeploymentExecutor`) are responsible for invoking methods in the correct order.

`create_cluster` builds a `CREATE CLUSTER` statement with the requested size and replication factor, appending an autoscaling strategy clause via `strategy_to_cluster_option` when one is configured. `create_cluster_with_config` dispatches between managed and unmanaged cluster creation based on the `ClusterConfig` variant, then applies any associated replica and grant statements.
