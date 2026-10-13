---
source: src/mz-deploy/src/project/analysis/deployment_snapshot.rs
revision: 2c0add6dcc
---

# mz-deploy::project::analysis::deployment_snapshot

Captures and compares point-in-time snapshots of deployment state for blue/green deployment workflows and change detection.

A `DeploymentSnapshot` maps `ObjectId` to content hashes and tracks which schemas were deployed as atomic units along with their `DeploymentKind`. The hash is computed from the **compiled AST representation** of each object (not the source SQL file contents), so formatting and comment changes produce identical hashes and do not trigger redeployment.

`compute_typed_hash` computes a SHA-256 hash of a `compiled::DatabaseObject` by hashing the main CREATE statement (via its `Hash` impl on the AST node) followed by all indexes sorted deterministically by cluster, on_name, name, and key_parts. The output format is `sha256:<hex>`. A `Sha256Hasher` bridges `std::hash::Hasher` to `sha2::Digest` to allow using the `Hash` trait with SHA-256.

`build_snapshot_from_planned` iterates all objects in a `graph::Project` in topological order, hashing each one and collecting its schema. Tables, table-from-source, sources, secrets, and connections are excluded because they are managed by the `apply` command path and do not participate in deployment hash change detection. Schemas whose `SchemaQualifier` appears in `project.replacement_schemas` receive `DeploymentKind::Replacement`; all others receive `DeploymentKind::Objects`.

`load_from_database` loads the current snapshot for a given environment from the database via the client's `deployments()` API.

`write_to_database` persists a snapshot to two tables in the `_mz_deploy` database: `_mz_deploy.public.deployments` (per-schema deployment metadata, inserted without delete) and `_mz_deploy.public.objects` (per-object content hashes, append-only history). `DeploymentMetadata` carries the deploying user and an optional git commit hash.

`DeploymentSnapshotError` covers connection failures, graph access errors, invalid FQNs, and duplicate/missing/already-promoted deployment conditions.
