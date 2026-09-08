---
source: src/mz-deploy/src/client/provisioning.rs
revision: 2e6c03ac43
---

# mz-deploy::client::provisioning

DDL provisioning operations.

Methods on `ProvisioningClient` issue idempotent `CREATE … IF NOT EXISTS` and `ALTER` statements to ensure that the target region's databases, schemas, and clusters match the project definition. All `create_*` methods use `IF NOT EXISTS` or catch "already exists" errors, so re-running provisioning on an already-provisioned environment is a no-op.

Provisioning must follow referential order: databases must be created before schemas, and schemas before clusters. Callers (e.g. `DeploymentExecutor`) are responsible for invoking methods in the correct order.

`create_cluster_with_config` dispatches between managed and unmanaged cluster creation based on the `ClusterConfig` variant. For managed clusters, it replays the production cluster's canonical `CREATE CLUSTER` statement under the new name. For unmanaged clusters, it issues `CREATE CLUSTER ... REPLICAS ()` followed by individual `CREATE CLUSTER REPLICA` statements. After the cluster is created, any associated grant statements are applied.
