# Per-cluster fault isolation for parallel-workload under Antithesis

## Problem

Antithesis fault injection operates at the docker-compose container
boundary: it kills, pauses, partitions, throttles individual containers.
For per-cluster fault coverage to be observable, each cluster the SUT
allocates needs to live in its own container — otherwise "fault one
cluster" reduces to "fault every cluster sharing this container".

The Antithesis compose has one `materialized` container running
environmentd. By default, every cluster a workload provisions becomes a
clusterd child process under that environmentd's process orchestrator.
Antithesis cannot fault a single child process; the smallest fault unit
is the whole `materialized` container, which is "the entire SUT".

The `antithesis_cluster` (the always-on user cluster the long-running
workloads target) is already an unmanaged cluster pointed at two external
clusterd containers (`clusterd1`, `clusterd2`), one per replica. That
gives us per-replica fault coverage for that cluster.

The gap is `parallel-workload` clusters. The randomized stress driver
creates new clusters as part of its action surface. Without external
clusterds, every parallel-workload cluster collapses back onto
environmentd's process orchestrator and the fault domain disappears.

## Solution

A pool of identical pre-deployed clusterd containers
(`clusterd-pool-{0..N-1}`), one container per parallel-workload
invocation. Each invocation claims one slot via filesystem locking,
provisions its sole cluster as an unmanaged replica pointed at that
slot's clusterd, and releases the slot on exit.

Components, bottom-up:

  - **`Clusterd(name="clusterd-pool-{i}", workers=4, scratch_directory=None)`**
    in `test/antithesis/mzcompose.py`. Same configuration as
    `clusterd1`/`clusterd2`: four timely workers per process (so
    Antithesis thread-pause faults have something distinct to pause),
    mem_env RocksDB (matches production, no scratch volume to fight over).
    Pool size from env (`ANTITHESIS_CLUSTERD_POOL_SIZE`, default 8).

  - **`parallel_workload.Database(pool_members=...,
    seed_scoped_names=True)`**. Opt-in framework mode: when
    `pool_members` is set, the framework provisions unmanaged clusters
    with explicit STORAGECTL/STORAGE/COMPUTECTL/COMPUTE ADDRESSES
    instead of managed SIZE/REPLICATION FACTOR; the CreateCluster /
    CreateReplica / DropReplica actions skip pool-backed clusters
    because there is no in-band allocator. `seed_scoped_names=True`
    renames `cluster{N}` / `role{N}` to `cluster-{seed}-{N}` /
    `role-{seed}-{N}` so concurrent invocations don't collide on
    global names.

  - **`_claim_pool_slot()`** in
    `test/antithesis/workload/test/parallel_driver_parallel_workload.py`.
    Contextmanager that holds `fcntl.flock(LOCK_EX | LOCK_NB)` on
    `/tmp/clusterd-pool-slots/{i}.lock` for the lifetime of the
    invocation. Slots tried in randomized order so allocation is
    decorrelated from invocation seed. The lock is released on context
    exit (normal or exception), so a crashing driver doesn't strand the
    slot.

  - **`_drop_seed_scoped_objects()`** in the same driver, called in
    `main()`'s `finally`. Drops every cluster / database / role whose
    name starts with `cluster-{seed}-` / `db-pw-{seed}-` /
    `role-{seed}-`. The DROP CLUSTER re-arms the clusterd to be
    claimed by the next invocation through the reconcile path
    (see below).

## Clusterd reuse correctness

The pool design assumes a DROP CLUSTER followed by a CREATE CLUSTER
pointed at the same clusterd is a supported transition. It is — this is
the same reconciliation path that handles environmentd restart. The
three pieces:

  1. **Transport cancels the prior connection on every new connect.**
     `src/service/src/transport.rs::serve` drops the old
     connection-task token and awaits the task before installing a
     fresh handler from `handler_fn()`. The new `ClusterClient` is a
     blank-slate wrapper around the same `Arc<Mutex<TimelyContainer>>`.

  2. **The worker `run` loop survives client disconnects.**
     `src/storage/src/storage_state.rs::Worker::run` is
     `while let Some((nonce, rx, tx)) = client_rx.blocking_recv() {
     run_client(rx, tx); }`. When the old `cmd_tx` is dropped (because
     the cancel above tore down the prior client), `run_client` returns
     and the outer loop awaits the next `(nonce, rx, tx)` — the new
     controller's connection. Worker in-memory state stays resident
     between connections.

  3. **`reconcile()` drops stale state.** The new controller's first
     batch of commands ending in `InitializationComplete` is processed
     by `storage_state::reconcile`: it computes `expected_objects` from
     the new commands, identifies `stale_objects` as anything the
     worker knows about that the new controller did not ask for, and
     `drop_collection`s each one — releasing source tokens (which tears
     down Kafka consumers, persist write handles, upsert RocksDB state),
     dropping dataflows, clearing reported frontiers.

Collection IDs do not collide across cluster lifetimes because
Materialize allocates them globally (`u<n>`, `t<n>`), not per cluster.

The one piece intentionally shared across reconnects is the
`Arc<PersistClientCache>`. It is keyed by URL+credentials, not by
cluster identity, and reusing it is the standard production behavior
(avoids reauthenticating to S3 / postgres-metadata on every reconnect).

The same analysis holds for the compute side (`src/compute/src/server.rs`
uses the same `ClusterSpec` pattern).

## Failure modes

  - **All pool slots held.** Driver tags `sometimes(...)` for
    visibility and exits cleanly. With the default pool size (8) and
    the test composer's normal concurrency this is not expected to
    fire, but if it does we'll see it in the run report.

  - **Crash before drop-on-exit runs.** The flock is released
    automatically when the process dies (kernel-level lock release).
    The clusterd is left holding stale state until the next claimant
    reconciles. Catalog leftovers (`cluster-{seed}-*`,
    `role-{seed}-*`, `db-pw-{seed}-*`) accumulate until the next
    invocation with the same seed runs its setup sweep — extremely
    unlikely since seeds are u64-random. The setup sweep is scoped
    to the current seed only, so it does not clean cross-invocation
    leftovers. A periodic external cleanup or a startup-time scan
    against `mz_clusters` / `mz_roles` / `mz_databases` would be
    needed to close this loop properly. For now the catalog growth
    is bounded by run length and not currently a problem.

  - **Pool sizing wrong vs concurrency.** If concurrency exceeds pool
    size, the late arrivals get "no slot" and exit. We do not currently
    auto-tune; bump `ANTITHESIS_CLUSTERD_POOL_SIZE` if telemetry shows
    the "no slot available" signal firing.

## v1 limitations (future work)

  - **REPLICATION FACTOR 1, no multi-replica parallel-workload coverage.**
    The pool gives each invocation one container; multi-replica
    coverage for compute/storage paths remains in `antithesis_cluster`.

  - **No in-band allocator inside the framework.** Worker threads
    cannot grab additional pool members mid-run, so
    `CreateClusterAction` / `CreateClusterReplicaAction` /
    `DropClusterReplicaAction` are skipped when pool-backed. The
    framework only ever touches the pre-allocated pool members.

  - **No global GC of cross-invocation catalog leftovers.** See
    failure modes above. A first-invocation sweep against
    `mz_clusters WHERE name LIKE 'cluster-%-%'` minus the current
    seed would close this; deferred until it becomes a problem.

## Tunables

| Variable | Default | Effect |
|---|---|---|
| `ANTITHESIS_CLUSTERD_POOL_SIZE` (compose) | 8 | Number of `clusterd-pool-{i}` containers deployed. |
| `CLUSTERD_POOL_SIZE` (driver) | 8 | Number of slots the driver will attempt to claim. Must match the compose value. |
| `CLUSTERD_POOL_SLOT_LOCK_DIR` (driver) | `/tmp/clusterd-pool-slots` | Directory holding the per-slot flock files. |
| `PW_RUNTIME_S` (driver) | 20 | Per-invocation runtime; bound to keep the fault-injection budget granular. |
| `PW_THREADS` (driver) | 4 | Worker threads inside one invocation. |
