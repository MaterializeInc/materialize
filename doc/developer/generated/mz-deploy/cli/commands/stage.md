---
source: src/mz-deploy/src/cli/commands/stage.rs
revision: 3f3bdb0535
---

# mz-deploy::cli::commands::stage

Stage command: deploy project objects to a staging environment with renamed schemas and clusters, producing a deployment that can later be promoted to production via `promote`.

`StageAnalysis` is built once from the project snapshot and reused across validation, metadata recording, and resource creation phases to keep the deployment deterministic. `PartitionedObjects` classifies objects into deploy-now, sinks, replacement materialized views, and tables, with the counts used for progress reporting.

`StagePlan` (and its sub-structs `StagePlanSchema`, `StagePlanCluster`, `StagePlanObject`) are serializable plan summaries rendered to the user for `--dry-run` or `--json` output. `StageResult` is the serializable success summary returned after a completed stage run.
