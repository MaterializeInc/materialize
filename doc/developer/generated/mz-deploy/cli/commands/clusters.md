---
source: src/mz-deploy/src/cli/commands/clusters.rs
revision: 2e6c03ac43
---

# mz-deploy::cli::commands::clusters

Clusters apply command: converge live cluster state to match cluster definitions loaded from `<root>/clusters/`.

`plan` iterates over definitions and calls `plan_cluster` for each. `plan_cluster` checks whether the cluster exists, creates it if absent, or computes drift and emits `ALTER CLUSTER RESET` then `ALTER CLUSTER SET` statements if options have changed. RESET runs before SET to avoid server-side validation errors when simultaneously dropping an autoscaling policy and changing the cluster size. Grant and comment reconciliation follows the create/alter step.

`diff_cluster_options` computes which options to SET and which to RESET by comparing a `ClusterDefinition` against the live cluster state. The comparison is generic over cluster option names: an option the definition declares is SET when its value differs from the live one; an option the definition omits is RESET unless the live value is already the server default. `default_options` supplies the server defaults, including `EXPERIMENTAL ARRANGEMENT COMPRESSION = false`. The `CanonicalIntervals` visitor normalizes interval-valued literals so that spelling differences (e.g. `'60s'` vs `'00:01:00'`) do not appear as drift. `is_discarded` filters out options the server drops (such as `DISK` and empty-block spellings), preventing false drift.

`run` is the CLI entry point: it connects, calls `plan`, and executes the result unless `dry_run` is set.
