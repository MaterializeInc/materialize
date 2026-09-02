---
source: src/mz-deploy/src/client/models.rs
revision: 2e6c03ac43
---

# mz-deploy::client::models

Domain models for Materialize catalog objects.

`DeploymentKind` distinguishes the type of deployment: `Tables` (apply tables command), `Objects` (stage/apply), `Sinks` (contains sinks), `Replacement` (replacement materialized views dropped after apply rather than swapped).

`DeploymentMode` is the mode tag stored alongside each deployment record. Currently only `Stage` exists; kept as an enum for schema compatibility with the `mode` column in `_mz_deploy.tables.deployments`.

`Cluster` represents a live compute cluster from the Materialize catalog.

`ClusterConfig` is an enum distinguishing `Managed` clusters (with the canonical `CREATE CLUSTER` statement and grants) from `Unmanaged` clusters (with replica definitions and grants). It is used during blue/green staging to recreate production clusters in the staging environment.
