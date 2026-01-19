# Unified Timely Runtime for Compute and Storage

- Associated: TODO (link to tracking issue)

## The Problem

Currently, `clusterd` creates two separate Timely runtimes: one for compute and one for storage
(`src/clusterd/src/lib.rs:363-424`). This has several drawbacks:

1. **Doubled resource consumption**: Each Timely runtime creates its own set of worker threads,
   communication channels, and networking infrastructure. This doubles the thread count and memory
   overhead.

2. **No introspection for storage**: Compute has comprehensive logging dataflows
   (`src/compute/src/logging/`) that expose Timely, Differential, and Reachability events. Storage
   has no equivalent introspection, making it harder to debug and understand storage behavior.

3. **Increased operational complexity**: Two separate clusters means two separate network
   connections from the controllers, two separate sets of metrics, and two separate failure domains.

4. **Code duplication**: Both compute and storage implement similar patterns for worker loops,
   command distribution, and client handling, but with slight variations that have diverged over
   time.

There is already a TODO comment acknowledging this at `src/clusterd/src/lib.rs:426`:
```rust
// TODO: unify storage and compute servers to use one timely cluster.
```

## Success Criteria

A successful solution must:

1. **Single Timely runtime**: Both compute and storage dataflows run in the same Timely cluster,
   sharing worker threads and communication infrastructure.

2. **Unified introspection**: Storage operations are visible through the same logging
   infrastructure as compute, enabling operators to understand source/sink behavior through system
   tables.

3. **Independent failure handling**: A crash in a storage dataflow should not corrupt compute state
   and vice versa. The controllers must be able to independently manage their respective domains.

4. **Multiplexed or unified controller connection**: Either a single network connection serves both
   controllers, or a clean mechanism exists for both to communicate with the unified runtime.

5. **No performance regression**: The unified runtime should not introduce latency or throughput
   regressions compared to the current dual-runtime architecture.

6. **Non-blocking coordination**: Neither compute nor storage should be able to block the other's
   progress through the shared `step_or_park` mechanism.

## Out of Scope

1. **Controller unification**: This design focuses on the replica side. Unifying the compute and
   storage controllers is a separate effort.

2. **Cross-domain dataflow dependencies**: While compute already reads from persist (which storage
   writes to), this design does not introduce direct dataflow edges between compute and storage
   operators within the same Timely cluster.

3. **Shared state between domains**: Compute and storage will continue to maintain separate state
   (traces, ingestions, exports). The unification is at the Timely runtime level, not the
   application state level.

## Solution Proposal

### Overview

The proposed solution introduces a **unified worker loop** that coordinates compute and storage
operations within a single Timely runtime. Each worker thread runs a combined event loop that:

1. Receives commands from both compute and storage controllers via a multiplexed channel
2. Calls `step_or_park` once per iteration, advancing both compute and storage dataflows
3. Maintains separate application state for compute and storage
4. Reports responses back to the appropriate controller

### Architecture

```
                    ┌─────────────────────────────────────────┐
                    │            Controllers                   │
                    │  ┌────────────┐    ┌────────────────┐   │
                    │  │  Compute   │    │    Storage     │   │
                    │  │ Controller │    │   Controller   │   │
                    │  └─────┬──────┘    └───────┬────────┘   │
                    └────────┼──────────────────┼─────────────┘
                             │                  │
                    ─────────┴────────┬─────────┴───────────────
                                      │
                              ┌───────▼───────┐
                              │  Multiplexer  │
                              │   (Option A)  │
                              └───────┬───────┘
                                      │
┌─────────────────────────────────────┼─────────────────────────────────────┐
│                              Unified Timely Cluster                       │
│                                     │                                     │
│   ┌─────────────────────────────────▼─────────────────────────────────┐   │
│   │                        Unified Worker Loop                         │   │
│   │                                                                    │   │
│   │  ┌──────────────┐         ┌──────────────┐         ┌───────────┐  │   │
│   │  │ Command Recv │────────▶│ step_or_park │────────▶│  Handle   │  │   │
│   │  │  (unified)   │         │   (shared)   │         │ Responses │  │   │
│   │  └──────────────┘         └──────────────┘         └───────────┘  │   │
│   │         │                        │                       │        │   │
│   │         ▼                        ▼                       ▼        │   │
│   │  ┌─────────────────────────────────────────────────────────────┐  │   │
│   │  │                    Dataflow Runtime                         │  │   │
│   │  │  ┌─────────────────────┐  ┌─────────────────────────────┐   │  │   │
│   │  │  │  Compute Dataflows  │  │    Storage Dataflows        │   │  │   │
│   │  │  │  - Arrangements     │  │    - Sources                │   │  │   │
│   │  │  │  - Peeks           │  │    - Sinks                  │   │  │   │
│   │  │  │  - Subscribes      │  │    - Upsert operators       │   │  │   │
│   │  │  └─────────────────────┘  └─────────────────────────────┘   │  │   │
│   │  └─────────────────────────────────────────────────────────────┘  │   │
│   └────────────────────────────────────────────────────────────────────┘   │
│                                                                           │
│   ┌─────────────────────────────────────────────────────────────────────┐ │
│   │                      Logging Dataflows                               │ │
│   │  - TimelyLog (shared)                                                │ │
│   │  - DifferentialLog (shared)                                          │ │
│   │  - ComputeLog (compute-specific)                                     │ │
│   │  - StorageLog (new, storage-specific)                                │ │
│   └─────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────┘
```

### Component Details

#### 1. Multiplexed Controller Connection (Option A: Single Connection)

Create a new connection multiplexer that handles both compute and storage protocols over a single
CTP connection:

```rust
/// A multiplexed command that can be either compute or storage.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClusterCommand {
    Compute(ComputeCommand),
    Storage(StorageCommand),
}

/// A multiplexed response that can be either compute or storage.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClusterResponse {
    Compute(ComputeResponse),
    Storage(StorageResponse),
}
```

The multiplexer would:
- Accept a single CTP connection from a unified controller proxy (or directly from environmentd)
- Demultiplex incoming commands to the appropriate handler
- Multiplex outgoing responses from both handlers

**Pros:**
- Single network connection, simpler firewall/networking config
- Natural ordering between compute and storage commands if needed
- Reduced connection overhead

**Cons:**
- Requires controller-side changes to multiplex/demultiplex
- Single point of failure for both domains
- Head-of-line blocking between compute and storage

#### 2. Separate Controller Connections (Option B: Dual Connection)

Keep separate CTP connections for compute and storage controllers, but route them to the same
unified worker:

```rust
pub struct UnifiedWorker<A: Allocate> {
    timely_worker: TimelyWorker<A>,

    // Compute channel
    compute_command_rx: CommandReceiver<ComputeCommand>,
    compute_response_tx: ResponseSender<ComputeResponse>,
    compute_state: Option<ComputeState>,

    // Storage channel
    storage_command_rx: CommandReceiver<StorageCommand>,
    storage_response_tx: ResponseSender<StorageResponse>,
    storage_state: StorageState,
}
```

The worker would poll both channels in a select-like manner before calling `step_or_park`.

**Pros:**
- No controller-side changes required initially
- Independent failure domains for network connections
- Can be implemented incrementally

**Cons:**
- Two network connections per replica
- More complex worker loop with dual-channel handling
- Potential for one controller to starve the other

**Recommendation:** Start with Option B (dual connections) as it requires fewer changes and can be
implemented incrementally. Option A can be pursued later as an optimization.

#### 3. Unified Worker Loop

The core change is merging the compute and storage worker loops into a single loop. Currently:

**Compute** (`src/compute/src/server.rs:327-377`):
```rust
loop {
    // Maintenance check
    if Instant::now() >= next_maintenance { /* report frontiers, metrics */ }

    // Step timely
    self.timely_worker.step_or_park(sleep_duration);

    // Handle compute commands
    self.handle_pending_commands()?;

    // Process compute-specific work (peeks, subscribes)
    if let Some(mut compute_state) = self.activate_compute() {
        compute_state.process_peeks();
        compute_state.process_subscribes();
    }
}
```

**Storage** (`src/storage/src/storage_state.rs:430-522`):
```rust
loop {
    // Maintenance check
    if Instant::now() >= next_maintenance { /* report frontiers */ }

    // Conditional step/park based on async worker state
    if command_rx.is_empty() && async_worker.is_empty() {
        self.timely_worker.step_or_park(Some(park_duration));
    } else {
        self.timely_worker.step();
    }

    // Handle async worker responses
    self.storage_state.handle_async_responses();

    // Handle internal commands
    self.storage_state.handle_internal_commands();

    // Handle external storage commands
    self.handle_pending_commands()?;
}
```

**Unified loop:**
```rust
loop {
    // 1. Determine if we can park
    let can_park = self.compute_command_rx.is_empty()
        && self.storage_command_rx.is_empty()
        && self.storage_state.async_worker.is_empty();

    // 2. Calculate park duration (minimum of compute and storage intervals)
    let park_duration = if can_park {
        Some(min(
            self.compute_maintenance_remaining(),
            self.storage_maintenance_remaining(),
            self.storage_stats_remaining(),
        ))
    } else {
        None
    };

    // 3. Step or park (single call for both domains)
    let timer = self.metrics.timely_step_duration_seconds.start_timer();
    if let Some(duration) = park_duration {
        self.timely_worker.step_or_park(Some(duration));
    } else {
        self.timely_worker.step();
    }
    timer.observe_duration();

    // 4. Handle compute work (non-blocking)
    self.handle_compute_commands()?;
    if let Some(mut compute_state) = self.activate_compute() {
        compute_state.process_peeks();
        compute_state.process_subscribes();
        compute_state.process_copy_tos();
    }

    // 5. Handle storage work (non-blocking)
    self.storage_state.handle_async_responses();
    self.storage_state.handle_internal_commands();
    self.handle_storage_commands()?;

    // 6. Report maintenance (combined or separate intervals)
    if self.compute_maintenance_due() {
        self.report_compute_frontiers();
    }
    if self.storage_maintenance_due() {
        self.report_storage_frontiers();
        self.report_storage_statistics();
    }
}
```

**Critical design consideration:** Neither compute nor storage command handling must block. Both
currently use non-blocking `try_recv` patterns, which is essential for the unified loop. The async
storage worker pattern (where heavy persist operations are offloaded to a separate async task)
must be preserved.

#### 4. Command Distribution

Both compute and storage use Timely dataflows to distribute commands from worker 0 to all workers:

- **Compute** (`src/compute/src/server/command_channel.rs`): Worker 0 receives external commands
  and broadcasts them to all workers via an Exchange operator. This is simple and efficient.

- **Storage** (`src/storage/src/internal_control.rs`): Uses a more complex three-stage sequencer
  with explicit global indexing to handle commands that can originate from any worker (via the
  async worker).

For the unified runtime, we should adopt **compute's simpler command distribution pattern** for
external commands in both domains:

```rust
/// Command channel that broadcasts external commands from worker 0 to all workers.
/// Used for both compute and storage external commands.
pub struct CommandChannel<C> {
    /// Receiver for commands distributed via the Timely dataflow.
    command_rx: Rc<RefCell<VecDeque<C>>>,
    /// Activator to wake up the dataflow when commands arrive.
    activator: Activator,
}
```

**Rationale:**
- External commands always arrive at worker 0 first (from the CTP connection)
- The Exchange-based broadcast is simpler and has less overhead
- Storage's complex sequencer is needed for *internal* commands that can originate from any worker
  (e.g., async worker responses), but external commands don't have this requirement
- Storage should retain its internal command sequencer for async worker coordination, but use the
  compute-style broadcast for external `StorageCommand`s

The unified architecture would have:
1. **External command channels** (compute-style): For `ComputeCommand` and `StorageCommand` from
   controllers, using simple worker-0 broadcast
2. **Internal command sequencer** (storage-only): For storage's internal commands from async
   workers, retaining the existing sequencer

#### 5. Unified Introspection

The logging infrastructure from compute (`src/compute/src/logging/`) should be extended to cover
storage operations:

1. **Shared logs**: `TimelyLog`, `DifferentialLog`, and `ReachabilityLog` naturally cover all
   dataflows in the cluster, including storage dataflows.

2. **New StorageLog**: Add storage-specific logging analogous to `ComputeLog`:
   ```rust
   pub enum StorageLog {
       SourceFrontierUpdate { id: GlobalId, frontier: Antichain<Timestamp> },
       SinkFrontierUpdate { id: GlobalId, frontier: Antichain<Timestamp> },
       IngestionStarted { id: GlobalId },
       IngestionStopped { id: GlobalId },
       ExportStarted { id: GlobalId },
       ExportStopped { id: GlobalId },
       // ... other storage events
   }
   ```

3. **Logging context sharing**: The `LoggingContext` created during compute initialization should
   be shared with storage dataflows.

#### 6. State Isolation

While sharing a Timely runtime, compute and storage state must remain isolated:

```rust
pub struct UnifiedWorker<A: Allocate> {
    // Shared Timely infrastructure
    timely_worker: TimelyWorker<A>,
    metrics: UnifiedMetrics,
    logging_context: LoggingContext,

    // Isolated compute state
    compute: ComputeDomain {
        command_rx: CommandReceiver<ComputeCommand>,
        response_tx: ResponseSender<ComputeResponse>,
        state: Option<ComputeState>,
        nonce: Option<Uuid>,
    },

    // Isolated storage state
    storage: StorageDomain {
        command_rx: CommandReceiver<StorageCommand>,
        response_tx: ResponseSender<StorageResponse>,
        state: StorageState,
        nonce: Option<Uuid>,
    },
}
```

Each domain maintains its own:
- Command/response channels
- Client nonce (for reconnection handling)
- Application state (traces, ingestions, etc.)
- Reconciliation logic

### Implementation Phases

**Phase 1: Unified Worker Skeleton**
- Create `UnifiedWorker` struct that holds both compute and storage channels
- Implement merged event loop with both domains
- Keep separate `ClusterSpec` implementations initially
- Verify basic functionality with both controllers

**Phase 2: Shared Timely Cluster**
- Modify `build_cluster` to create a single `TimelyContainer`
- Both controllers connect to the same workers
- Commands route to appropriate domain based on type
- Validate no performance regression

**Phase 3: Unified Logging**
- Extend `LoggingContext` to storage dataflows
- Add `StorageLog` event type
- Surface storage logs in system tables
- Validate introspection coverage

**Phase 4: Command Channel Unification**
- Migrate storage external commands to use compute-style broadcast
- Retain storage's internal sequencer for async worker commands
- Single pattern for external command distribution in both domains

**Phase 5: Connection Multiplexing (Optional)**
- Implement `ClusterCommand`/`ClusterResponse` enums
- Add multiplexer layer in clusterd
- Coordinate with controller changes
- Migrate to single connection

## Minimal Viable Prototype

A prototype should demonstrate:

1. A single `timely::execute_from` call that runs both compute and storage logic
2. Both controllers successfully connecting and issuing commands
3. Dataflows from both domains executing correctly
4. No deadlock or starvation between domains

This can be validated by:
- Running the existing test suite with the unified runtime
- Manually verifying that sources, sinks, and queries work together
- Measuring step duration metrics to ensure fair scheduling

## Alternatives

### Alternative 1: Keep Separate Runtimes, Add Storage Logging

Instead of unifying runtimes, add logging dataflows to the storage Timely cluster separately.

**Pros:**
- Minimal changes to existing architecture
- Lower risk

**Cons:**
- Does not address resource duplication
- Storage logs would be in a separate system table namespace
- Continued code duplication

### Alternative 2: Storage as Compute Dataflows

Move storage operations (sources, sinks) into compute dataflows, eliminating the storage Timely
cluster entirely.

**Pros:**
- Complete unification
- Single abstraction for all dataflows

**Cons:**
- Major architectural change
- Different lifecycle requirements (sources run continuously, computes are demand-driven)
- Would require significant controller refactoring

### Alternative 3: Process-Level Multiplexing

Run compute and storage as separate processes that share a single Timely process allocation
through process-level coordination.

**Pros:**
- Strong isolation
- Independent failure domains

**Cons:**
- IPC overhead
- Complex coordination
- Does not solve introspection sharing

## Open Questions

1. **Controller coordination**: If compute and storage commands need ordering guarantees across
   domains (e.g., for `CREATE MATERIALIZED VIEW AS SELECT FROM source`), how do we ensure this
   with separate controller connections?

2. **Reconciliation interaction**: During reconnection, compute reconciliation may depend on
   storage sources being available. How do we handle the case where storage reconnects before
   compute, or vice versa?

3. **Resource limits**: With a unified runtime, how do we attribute memory/CPU usage to compute
   vs storage for resource limit enforcement?

4. **Panic isolation**: If a storage dataflow panics, does it bring down compute dataflows?
   Should we add catch_unwind boundaries within the unified loop?

5. **Testing strategy**: How do we test the unified runtime in isolation before deploying? Can we
   run both old and new implementations in parallel for validation?
