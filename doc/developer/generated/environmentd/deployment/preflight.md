---
source: src/environmentd/src/deployment/preflight.rs
revision: a60edac7f1
---

# environmentd::deployment::preflight

Implements zero-downtime (0dt) preflight checks for new `environmentd` deployments.
Compares the catalog's deploy generation against the incoming generation to determine whether to boot in read-only mode, and spawns a background task that waits for the deployment to catch up before fencing out the old environment and rebooting as leader.
Also periodically checks for DDL changes on the old environment during the catch-up period, restarting the new process in read-only mode if new objects or replicas appear that need to be hydrated first.

The DDL check (`check_ddl_changes`) computes baseline IDs from `get_next_ids`, which derives the next user item ID and replica ID from the maximum existing IDs in the catalog rather than from the allocator counter. This avoids false negatives when batch ID allocation (`IdPool`) has advanced the counter ahead of actually-committed items.
