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
(`clusterd-pool-{0..N-1}`) with a corresponding pool of long-lived
unmanaged clusters (`pool_cluster_{0..N-1}`), each bound to its slot's
clusterd. Pool clusters are bootstrapped once by the workload-entrypoint
and outlive every individual parallel-workload invocation. Each
parallel-workload invocation picks a slot at random and runs against
`pool_cluster_{slot}`. There is no coordination between concurrent
invocations: every workload object lives in a seed-scoped database
(`db-pw-{seed}-*`) with seed-scoped roles, so two invocations sharing a
pool cluster don't collide. Antithesis faults containers, not
invocations, so the per-container fault domain is preserved either way;
two invocations witnessing the same fault is a feature (more
independent reproductions per failure). The pool cluster itself is
never dropped.

Components, bottom-up:

  - **`Clusterd(name="clusterd-pool-{i}", workers=CLUSTERD_WORKERS,
    scratch_directory=None)`** in `test/antithesis/mzcompose.py`. Same
    configuration as `clusterd1`/`clusterd2`: 16 timely workers per
    process (matches the per-process worker density of larger
    production cluster sizes — single-process clusterds at workers=16
    cover the same intra-process concurrency surface as a 4-process
    scale=4,workers=4 production deployment), mem_env RocksDB (matches
    production, no scratch volume to fight over). Pool size from env
    (`ANTITHESIS_CLUSTERD_POOL_SIZE`, default 2).

  - **Pool-cluster bootstrap** in
    `test/antithesis/workload/workload-entrypoint.sh`. After materialized
    becomes healthy, the script loops over `0..POOL_SIZE-1` and issues
    `CREATE CLUSTER pool_cluster_{i} REPLICAS (r1 (STORAGECTL ADDRESSES
    ['clusterd-pool-{i}:2100'], ...))` for each pool member that doesn't
    already exist. Idempotent across compose-up cycles. Once setup-
    complete is emitted, every pool cluster is ready for the test
    composer to start invoking the parallel-workload driver.

  - **`parallel_workload.Database(existing_cluster_name=...,
    seed_scoped_names=True)`**. Opt-in framework mode: when
    `existing_cluster_name` is set, the framework's single initial
    cluster is a wrapper around the pre-existing cluster — `create()`
    and `drop()` are no-ops, `is_pool_backed` is True (which gates the
    CreateCluster / CreateReplica / DropReplica actions). `Cluster.name()`
    returns the literal cluster name supplied by the caller, bypassing
    the framework's normal `cluster-{seed}-{id}` shape. Roles still get
    seed-scoped naming (`role-{seed}-{N}`) so concurrent invocations
    don't collide on those.

  - **Slot pick** in
    `test/antithesis/workload/test/parallel_driver_parallel_workload.py`:
    `rng.randrange(CLUSTERD_POOL_SIZE)`. Stateless, no coordination,
    no failure mode. Concurrent invocations may share a pool cluster
    (see the no-collision argument above).

  - **`_drop_seed_scoped_objects()`** in the same driver, called in
    `main()`'s `finally`. Drops every database and role whose name
    starts with `db-pw-{seed}-` / `role-{seed}-`. **Pool clusters are
    NOT dropped** — they're permanent state shared across invocations.
    The DROP DATABASE CASCADE transitively drops every workload-created
    table / MV / index / source / sink, which tears down the
    corresponding dataflows on the bound clusterd container, so the
    cluster returns to an idle baseline before the next claimant.

## Why pool clusters must be permanent: the clusterd-reuse constraint

The first iteration of this design dropped and recreated the parallel-
workload cluster on every invocation. That failed on the second
invocation against the same pool slot with a clusterd halt:

> `WARN ...: halting process: new instance configuration not compatible
> with existing instance configuration: ... index_logs:
> {Timely(Operates): IntrospectionSourceIndex(144115188075856897), ...}
> vs Some(... IntrospectionSourceIndex(144115188075856641), ...)`

The check is `InstanceConfig::compatible_with` in
`src/compute-client/src/protocol/command.rs`. It compares `LoggingConfig`
including `index_logs: BTreeMap<LogVariant, IntrospectionSourceIndex>`.
Those introspection-source-index IDs are per-cluster catalog allocations
— every CREATE CLUSTER produces a fresh batch. Pointing a *different*
cluster identity at a clusterd that already saw a prior cluster's
introspection indexes trips this check and the clusterd halts on the
first `CreateInstance` command.

Reconcile (`storage_state::reconcile`, `compute::server`) handles the
case where the *same* cluster reconnects after an environmentd restart:
the worker drops stale collections, takes the new commands, and resumes.
But it does not handle the case where a different cluster claims the
clusterd, because the introspection indexes don't match.

Pinning cluster identity to clusterd identity — one permanent pool
cluster per pool clusterd container — sidesteps the check entirely. The
only reconnect events the pool clusterds see across the lifetime of a
compose are environmentd restarts (and Antithesis-injected pauses /
restarts of the pool clusterd itself), both of which exercise the same
cluster identity reconnecting. That's the path reconcile is designed for.

## Failure modes

  - **Crash before drop-on-exit runs.** The seed-scoped database and
    roles are left in the catalog until they're explicitly cleaned up.
    Catalog leftovers do not break correctness (each seed is u64-random,
    no cross-invocation collisions) but they accumulate. The next
    invocation that lands on the same pool cluster will inherit MVs /
    indexes / sources still rendered on the bound clusterd from the
    crashed invocation, which is more state pressure than a clean
    handoff. A periodic / startup-time sweep against `mz_databases` /
    `mz_roles` would close this; deferred until it shows up as a
    problem.

  - **Pool size much smaller than concurrency.** With C concurrent
    invocations and N pool slots (default N=2), ~C/N invocations share
    each cluster in steady state. That's correctness-preserving but
    increases per-cluster state pressure linearly with the ratio. The
    pool is deliberately small so each pool cluster behaves more like
    a busy production cluster; bump `ANTITHESIS_CLUSTERD_POOL_SIZE` if
    a single pool cluster runs hot enough to mask other signals.

## v1 limitations (future work)

  - **Single-replica pool clusters.** Each pool cluster has one replica
    (one clusterd container per cluster), so parallel-workload
    invocations don't exercise multi-replica compute/storage paths.
    Multi-replica coverage stays in `antithesis_cluster`. A future
    revision could pair clusterd containers into 2-replica pool
    clusters at the cost of doubling the pool footprint per
    concurrency unit.

  - **No in-band allocator inside the framework.** Worker threads
    cannot grab additional pool clusters mid-run, so
    `CreateClusterAction` / `CreateClusterReplicaAction` /
    `DropClusterReplicaAction` are skipped when pool-backed. The
    framework only ever touches the pre-existing pool cluster.

  - **State accumulation on pool clusters.** Each pool cluster runs
    through O(invocations) workload lifecycles over a long Antithesis
    run. Even with seed-scoped DBs being dropped on exit, every pool
    cluster's clusterd retains compute-side bookkeeping (catalog
    state for introspection, peek_stash subscriptions, etc.). The
    framework relies on `drop_collection` to release dataflow state;
    if that path ever leaks, the pool cluster's memory footprint will
    grow over many invocations.

## Tunables

| Variable | Default | Effect |
|---|---|---|
| `ANTITHESIS_CLUSTERD_POOL_SIZE` (compose + entrypoint) | 2 | Number of clusterd-pool-<i> containers deployed and matching pool_cluster_<i> clusters bootstrapped. |
| `CLUSTERD_POOL_SIZE` (driver) | 2 | Number of slots the driver chooses among. Mirrored from compose by mzcompose.py's Workload service so the two agree. |
| `CLUSTERD_WORKERS` (compose + entrypoint) | 16 | Timely worker threads per clusterd process. Must match every CREATE CLUSTER REPLICAS' WORKERS clause and every `Clusterd(workers=...)` Service. |
| `PW_RUNTIME_S` (driver) | 20 | Per-invocation runtime; bound to keep the fault-injection budget granular. |
| `PW_THREADS` (driver) | 4 | Worker threads inside one invocation. |
