---
source: src/environmentd/src/deployment/preflight.rs
revision: e40e204bdb
---

# environmentd::deployment::preflight

Implements zero-downtime (0dt) preflight checks for new `environmentd` deployments.
Compares the catalog's deploy generation against the incoming generation to determine whether to boot in read-only mode, and spawns a background task that waits for the deployment to catch up before fencing out the old environment and rebooting as leader.
Also periodically checks for DDL changes on the old environment during the catch-up period, restarting the new process in read-only mode if user items or replicas have been created or dropped, so that bootstrap hydrates new objects and releases dropped resources.

The DDL check (`check_ddl_changes`) receives the baseline sets of user item and replica IDs from `get_user_ids`, which snapshots the committed IDs directly from the catalog rather than using allocator counters. Allocator counters can advance ahead of committed objects due to batch ID allocation, so comparing full ID sets detects both newly created and dropped objects correctly.
