---
source: src/mz-deploy/src/cli/commands/clusters.rs
revision: 3f3bdb0535
---

# mz-deploy::cli::commands::clusters

Clusters apply command: converge live cluster state to match cluster definitions loaded from `<root>/clusters/`.

`plan` iterates over definitions and calls `plan_cluster` for each. `plan_cluster` checks whether the cluster exists, creates it if absent, or computes drift and emits `ALTER CLUSTER RESET` then `ALTER CLUSTER SET` statements if options have changed. RESET runs before SET to avoid server-side validation errors when simultaneously dropping an autoscaling policy and changing the cluster size. Grant and comment reconciliation follows the create/alter step.

`diff_cluster_options` computes which options to SET and which to RESET by comparing a `ClusterDefinition` against the live `Cluster`. Autoscaling policy drift is only computed when the region supports autoscaling strategies; on older regions the live policy is unknowable and left unchanged.

`run` is the CLI entry point: it connects, calls `plan`, and executes the result unless `dry_run` is set.
