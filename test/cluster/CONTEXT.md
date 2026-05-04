# test/cluster

**Purpose:** mzcompose integration tests that require separately-launched `clusterd`
containers, validating the compute/storage layer boundary in a realistic multi-process
topology. Unlike most suites, `clusterd` processes live outside the `materialized`
container.

**Framework:** mzcompose (Python). Single `mzcompose.py` (~6,700 LOC); no further
subdirectories contain code. Inline testdrive snippets are embedded in workflow
functions rather than `.td` files.

**Service topology:** `environmentd` (crash-tolerant, `propagate_crashes=False`) +
4 `clusterd` replicas + Kafka/Redpanda + CockroachDB/Postgres metadata store + Minio
+ Toxiproxy for fault injection.

**Workflow taxonomy (~57 `workflow_*` functions):**
- Smoke / regression: named `workflow_test_github_<N>` — per-issue regression cases.
- Compute correctness: reconciliation reuse/replace, replica-targeted
  select/subscribe abort, monotonicity enforcers.
- Storage: upsert, remote storage, pg snapshot resumption, sink failure.
- Observability: metrics retention across restart, optimizer/pgwire/controller metrics.
- Lifecycle: bootstrap vars, system table indexes, blue-green deployment.
- Fault: clusterd death detection, replica isolation, crash-restart cycles.

**Key seam:** `Composition.run_testdrive_files()` bridges mzcompose orchestration to
testdrive assertions within a workflow.

**Bubbles up:** test/cluster is the canonical location for tests that require
external-clusterd topology. Any coverage that needs a real multi-process cluster
boundary belongs here, not in `testdrive/` or `sqllogictest/`.
