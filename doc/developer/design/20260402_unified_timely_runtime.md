# Unified Timely Runtime for Compute and Storage

- Associated: TODO (tracking issue)

## The Problem

`clusterd` currently creates two separate Timely runtimes: one for compute and
one for storage (`src/clusterd/src/lib.rs`). Both runtimes have the same
configuration (same number of workers, same process index) but run on separate
thread pools with independent networking.

This architecture prevents us from getting **storage introspection** through
compute's existing logging infrastructure. Compute has comprehensive logging
dataflows (`src/compute/src/logging/`) that surface Timely, Differential, and
Reachability events through system tables (`mz_scheduling_elapsed`,
`mz_arrangement_sizes`, etc.). Storage has no equivalent introspection — its
Timely events are simply discarded.

We cannot solve this by forwarding events from the storage Timely instance to
the compute one (e.g. via mpsc channels) because both instances allocate
operator, channel, and dataflow IDs starting from 0 independently. There is no
way to disambiguate which instance an event originated from without changing the
logging schema.

Unifying the runtimes eliminates this problem: a single Timely instance has a
single ID allocator, and compute's existing logging dataflows automatically
capture events from storage dataflows with no ID collisions and no schema
changes.

## Success Criteria

1. **Single Timely runtime**: Both compute and storage dataflows run in the same
   Timely cluster, sharing worker threads and communication infrastructure.

2. **Storage introspection for free**: Storage operations are visible through
   compute's existing logging dataflows and system tables. No changes to the
   logging infrastructure or system table schemas are required.

3. **No behavioral change in standalone mode**: The existing dual-runtime mode
   continues to work. The unified runtime is gated behind a flag and can be
   enabled incrementally.

4. **Command distribution unchanged**: Compute's Exchange-based broadcast and
   storage's internal command sequencer are fundamentally different and remain
   untouched.

5. **No controller changes**: Both compute and storage controllers continue to
   connect via separate gRPC connections. The unification is entirely
   replica-side.

## Out of Scope

1. **Controller unification**: Merging the compute and storage controllers or
   their protocol connections is a separate effort.

2. **Cross-domain dataflow dependencies**: We do not introduce direct dataflow
   edges between compute and storage operators within the Timely cluster.

3. **Shared application state**: Compute and storage continue to maintain fully
   separate state (traces, ingestions, exports). The unification is at the
   Timely runtime level only.

4. **Performance isolation**: We do not recommend colocating compute and storage
   objects on the same cluster, so scheduling fairness between domains is not a
   concern.

5. **New storage-specific log variants**: Extending the logging schema with
   storage-specific events (e.g. `StorageLog` analogous to `ComputeLog`) is
   future work. This design only covers making existing Timely/Differential
   logging capture storage dataflows.

## Solution Proposal

### Overview

We refactor compute and storage worker state so that neither owns the
`&mut TimelyWorker` reference. Instead, a thin unified loop in `clusterd` owns
the Timely worker and passes it to each domain's step function sequentially.
This lets both domains' dataflows live in the same Timely instance while keeping
their command handling, reconciliation, and state management completely
independent.

### Key Design Decisions

**No Worker struct duplication.** A prior attempt (PR #34713) created a 658-line
`UnifiedWorker` that re-implemented both worker loops in a third location. Any
change to compute or storage worker logic required updating the unified worker
too. Our approach instead refactors the existing worker types to separate their
state from the `TimelyWorker` borrow, keeping logic in its original crate.

**Sequential, not concurrent, domain steps.** Within each iteration of the
unified loop, compute's step runs to completion, then storage's step runs to
completion. Neither domain blocks (both use `try_recv` patterns), so this is
equivalent to the existing architecture but interleaved on one thread.

**Reconciliation blocks the other domain.** During (re)connect, each domain
reconciles by draining commands until `InitializationComplete`. In unified mode,
this briefly pauses the other domain. This is acceptable because reconciliation
is infrequent and fast.

### Component Changes

#### 1. Compute: Extract `ComputeWorkerState` from `Worker`

**File: `src/compute/src/server.rs`**

Currently, compute's `Worker` struct bundles the `TimelyWorker` reference with
all compute-specific state:

```rust
struct Worker<'w, A: Allocate> {
    timely_worker: &'w mut TimelyWorker<A>,
    command_rx: CommandReceiver,
    response_tx: ResponseSender,
    compute_state: Option<ComputeState>,
    metrics: WorkerMetrics,
    // ... other fields
}
```

We split this into a `ComputeWorkerState` that holds everything except the
Timely worker:

```rust
pub struct ComputeWorkerState {
    pub command_rx: CommandReceiver,
    pub response_tx: ResponseSender,
    pub compute_state: Option<ComputeState>,
    pub metrics: WorkerMetrics,
    pub persist_clients: Arc<PersistClientCache>,
    pub txns_ctx: TxnsContext,
    pub tracing_handle: Arc<TracingHandle>,
    pub context: ComputeInstanceContext,
    pub metrics_registry: MetricsRegistry,
    pub workers_per_process: usize,
}
```

The existing `Worker` becomes a thin wrapper:

```rust
struct Worker<'w, A: Allocate> {
    timely_worker: &'w mut TimelyWorker<A>,
    state: ComputeWorkerState,
}
```

All existing methods delegate through `self.state`, so standalone behavior is
identical. New public methods on `ComputeWorkerState` take
`&mut TimelyWorker<A>` as a parameter:

```rust
impl ComputeWorkerState {
    pub fn activate_compute<A: Allocate + 'static>(
        &mut self, tw: &mut TimelyWorker<A>,
    ) -> Option<ActiveComputeState<'_, A>> { ... }

    pub fn handle_step<A: Allocate + 'static>(
        &mut self, tw: &mut TimelyWorker<A>,
    ) -> Result<(), NonceChange> { ... }

    pub fn do_maintenance<A: Allocate + 'static>(
        &mut self, tw: &mut TimelyWorker<A>,
    ) { ... }
}
```

We also make `ActiveComputeState`, `ResponseSender`, `CommandReceiver`, and
`NonceChange` public.

#### 2. Storage: Move `handle_internal_storage_command` to `StorageState`

**File: `src/storage/src/storage_state.rs`**

Storage's `Worker` uses `self.timely_worker` in only a few methods:

| Method | Usage |
|--------|-------|
| `run_client` | `step_or_park` / `step` (the main loop) |
| `handle_internal_storage_command` | Renders dataflows, checks `index()`/`peers()` |
| `handle_async_worker_response` | `assert_eq!(self.timely_worker.index(), 0)` |
| `reconcile` | `self.timely_worker.index()` |

`StorageState` already stores `timely_worker_index` and `timely_worker_peers`.
The changes:

- **`handle_internal_storage_command`**: Move the body to a new method on
  `StorageState` that takes `&mut TimelyWorker<A>` as a parameter. The existing
  `Worker` method becomes a one-line delegation. Uses of
  `self.timely_worker.index()` / `peers()` switch to `self.timely_worker_index`
  / `self.timely_worker_peers` where the actual `TimelyWorker` reference is not
  needed for dataflow rendering.

- **`handle_async_worker_response`**: Replace
  `assert_eq!(self.timely_worker.index(), 0)` with
  `assert_eq!(self.storage_state.timely_worker_index, 0)`. No signature change
  needed — the method can stay on `Worker` and we add a version on
  `StorageState`.

- **`reconcile`**: Same treatment — `worker_id` comes from
  `self.storage_state.timely_worker_index`.

The other per-step methods (`report_frontier_progress`, `report_status_updates`,
`report_storage_statistics`, `process_oneshot_ingestions`) already operate on
`self.storage_state` + `response_tx` without touching `timely_worker`. These
become callable from the unified loop with no changes.

#### 3. Unified Runtime Module

**New file: `src/clusterd/src/unified.rs`** (~200 lines)

```rust
pub struct UnifiedClientBuilders {
    pub compute: Box<dyn Fn() -> Box<dyn ComputeClient>>,
    pub storage: Box<dyn Fn() -> Box<dyn StorageClient>>,
}

pub async fn serve(
    timely_config: TimelyConfig,
    // ... all config for both domains
) -> Result<UnifiedClientBuilders, Error> {
    // 1. Set up networking (same as build_cluster)
    // 2. Create two sets of client_txs channels
    // 3. Call execute_from with unified_worker_main
    // 4. Return client builders for both domains
}
```

The per-worker entry point:

```rust
fn unified_worker_main<A: Allocate + 'static>(
    timely_worker: &mut TimelyWorker<A>,
    compute_client_rx: ...,
    storage_client_rx: ...,
    // ... configs
) {
    // Set up compute command channel (broadcast dataflow)
    let (cmd_tx, cmd_rx) = command_channel::render(timely_worker);
    let (resp_tx, resp_rx) = mpsc::unbounded_channel();
    spawn_channel_adapter(compute_client_rx, cmd_tx, resp_rx, worker_id);

    // Set up storage state
    let (internal_cmd_tx, internal_cmd_rx) =
        internal_control::setup_command_sequencer(timely_worker);
    // ... construct StorageState

    // Create compute worker state
    let mut compute = ComputeWorkerState { ... };

    // Unified loop
    unified_loop(timely_worker, &mut compute, &mut storage_state, ...);
}
```

#### 4. The Unified Loop

```rust
fn unified_loop<A: Allocate + 'static>(
    tw: &mut TimelyWorker<A>,
    compute: &mut ComputeWorkerState,
    storage_state: &mut StorageState,
    storage_cmd_rx: &mut CommandReceiver,
    storage_resp_tx: &ResponseSender,
) {
    // Wait for first compute nonce
    let NonceChange(nonce) = compute.recv_command(tw).expect_err("first nonce");
    compute.response_tx.set_nonce(nonce);

    // Storage waits for first client connection
    // (blocks until controller connects — same as standalone)

    let mut last_compute_maintenance = Instant::now();
    let mut last_storage_maintenance = Instant::now();
    let mut last_stats_time = Instant::now();

    loop {
        // Maintenance
        let compute_sleep = compute.maybe_maintenance(tw, &mut last_compute_maintenance);
        let storage_sleep = storage_maybe_maintenance(
            storage_state, storage_resp_tx, &mut last_storage_maintenance,
        );

        // Determine park-ability
        let can_park = compute.command_rx_empty()
            && storage_cmd_rx.is_empty()
            && storage_state.async_worker.is_empty();

        let stats_interval = storage_state.storage_configuration
            .parameters.statistics_collection_interval;
        let stats_remaining = stats_interval
            .saturating_sub(last_stats_time.elapsed());
        let sleep = min_durations(compute_sleep, storage_sleep, Some(stats_remaining));

        // Step timely (single call for both domains)
        if can_park {
            tw.step_or_park(sleep);
        } else {
            tw.step();
        }

        // Compute step
        compute.handle_step(tw)?; // NonceChange triggers reconciliation

        // Storage step
        for id in std::mem::take(&mut storage_state.dropped_ids) {
            storage_resp_tx.send(StorageResponse::DroppedId(id));
        }
        process_oneshot_ingestions(storage_state, storage_resp_tx);
        report_status_updates(storage_state, storage_resp_tx);

        if last_stats_time.elapsed() >= stats_interval {
            report_storage_statistics(storage_state, storage_resp_tx);
            last_stats_time = Instant::now();
        }

        // Drain storage commands
        while let Ok(cmd) = storage_cmd_rx.try_recv() {
            storage_state.handle_storage_command(cmd);
        }

        // Drain async worker responses
        while let Ok(resp) = storage_state.async_worker.try_recv() {
            storage_state.handle_async_worker_response(resp);
        }

        // Drain internal commands
        while let Some(cmd) = storage_state.internal_cmd_rx.try_recv() {
            storage_state.handle_internal_storage_command(tw, cmd);
        }
    }
}
```

#### 5. Wiring in `clusterd`

**File: `src/clusterd/src/lib.rs`**

Add a `--unified-runtime` CLI flag. When enabled, call `unified::serve()`
instead of separate `mz_storage::serve()` + `mz_compute::server::serve()`. Both
code paths coexist; the flag defaults to off.

### How Logging Works

With a unified Timely instance, logging requires **zero changes**:

1. Compute's `initialize()` registers Timely/Differential/Compute loggers with
   the worker's `log_register()`.
2. When storage dataflows are rendered in the same Timely worker, their
   operators, channels, schedule events, and arrangement events automatically
   flow through the same registered loggers.
3. The logging dataflows (demux, consolidate, arrange) process these events
   identically — they are just more Timely events from more operators.
4. System tables like `mz_scheduling_elapsed` and `mz_arrangement_sizes`
   automatically include storage operator data.

There are no ID collisions because there is only one ID allocator.

## Minimal Viable Prototype

The prototype validates:

1. A single `execute_from` call runs both compute and storage logic
2. Both controllers connect and issue commands successfully
3. Dataflows from both domains execute correctly (sources ingest, queries
   return results)
4. `SELECT * FROM mz_scheduling_elapsed` shows operators from storage
   dataflows (source ingestion operators, upsert operators, etc.)
5. No deadlock or starvation between domains

Validation:

- Run the existing SLT and testdrive suites with `--unified-runtime`
- Manually verify that `mz_internal` system tables include storage operators
- Confirm no regression in compute-only workloads

## Alternatives

### Forward events via mpsc channels

Instead of unifying runtimes, register Timely loggers in the storage instance
that send events over mpsc channels to compute's logging dataflows.

**Rejected because**: Both Timely instances allocate IDs starting from 0.
Forwarded events would collide with compute's own operator IDs, making the data
meaningless. Remapping IDs (e.g. with a large offset) is fragile — any code
assuming IDs are small or using them as array indices breaks. Adding a
`source: {compute, storage}` column to logging relations requires schema changes
and migrations. The unified runtime avoids all of this.

### Keep separate runtimes, add logging to storage

Add a parallel set of logging dataflows inside the storage Timely instance,
surfacing them through separate system tables.

**Rejected because**: This doubles the logging infrastructure, requires new
system table schemas, and doesn't reduce the thread/networking overhead of
running two Timely clusters. Users would need to query different tables for
compute vs storage introspection.

### Full Worker struct duplication (PR #34713 approach)

Create a unified `UnifiedWorker` in `clusterd` that re-implements both worker
loops.

**Rejected because**: The 658-line `UnifiedWorker` duplicated reconciliation,
command handling, maintenance, and async worker response logic. Any change to
compute or storage requires updating the unified worker. The current approach
keeps logic in its original crate and only exposes step-level methods.

## Open questions

1. **Reconciliation ordering**: In unified mode, compute reconciles first, then
   storage. Should we support concurrent reconciliation (compute reconciles
   while storage commands queue up, and vice versa)? For now, sequential
   reconciliation is simpler and reconnection is infrequent.

2. **Feature flag graduation**: What criteria should we use to make unified
   runtime the default? Proposal: pass the full test suite + run in staging for
   one release cycle with no regressions.

3. **Panic propagation**: A panic in a storage dataflow kills the compute
   worker (same thread). This matches the current behavior within each domain
   (a panic in any compute dataflow kills all compute dataflows). Is additional
   isolation needed? Current assessment: no, because we don't recommend
   colocating compute and storage objects.
