# QueryTracker: Centralized tracking and cancellation of peeks

## Summary

Introduce a new component, **QueryTracker**, that encapsulates:

- tracking the lifecycle of interactive read queries (initially: peeks),
- cancellation logic for those queries (by connection, timeout, and dependency/cluster drops), and
- the statement execution logging hooks that are currently tightly coupled to peek tracking.

QueryTracker runs as an **actor** (single async task) with its own state. It accepts commands via
an internal command queue and performs side effects through a small, injected **effects interface**
(implemented by the Coordinator in production, mocked in tests).

The intent is to decouple components, minimize blocking (callers are fire-and-forget), and make
the lifecycle logic easy to test in isolation.

## Practical learnings (races we must handle)

In practice, several races show up quickly once peek tracking becomes “fire-and-forget” and
cross-thread:

- **Drop-before-track / cancel-before-track**: a cluster/collection can be dropped while a peek is
  being planned/issued, and the cancellation signal may arrive before QueryTracker ever observes
  the `TrackPeek`.
- **Track-before-issue, but cancel-after-drop**: even if we send `TrackPeek` “before issuing” a
  compute peek, the *cluster can be dropped* before cancellation reaches compute. In that case the
  compute instance disappears and the peek response channel may be dropped without an explicit
  `PeekNotification`.
- **Out-of-order statement logging events**: frontend statement logging “setters” (e.g.
  `SetTimestamp`) can arrive after `EndedExecution`. These must be treated as best-effort updates
  and **must not panic** when the execution has already ended.
- **Debugging/dump paths can be observable**: `ExecuteContextExtra` is `#[must_use]` and asserts on
  drop if it contains non-trivial state. Any debug/dump rendering must avoid constructing
  temporary `ExecuteContextExtra` values that then get dropped.

The design below intentionally calls these out, because they affect invariants and error handling.

## Goals

- Centralize peek tracking state and cancellation decisions in QueryTracker.
- Replace direct access to Coordinator internals (`pending_peeks`, `client_pending_peeks`) with a
  minimal interface.
- Make query lifecycle tracking and cancellation unit-testable without a Coordinator/controller.
- Preserve existing best-effort semantics (late notifications are ignored; cancellation is
  best-effort).
- Reduce blocking between components: callers enqueue commands and continue.

## Non-goals (initially)

- Refactor compute/storage controller APIs.
- Redesign statement logging tables or write cadence.
- Track/retire subscribes, COPY TO, and writes in the first iteration (peeks are the initial scope).

## Background (current behavior and coupling)

### Peek tracking and cancellation

Coordinator currently owns peek tracking state in `src/adapter/src/coord/peek.rs`:

- `pending_peeks: Map<Uuid, PendingPeek>`
- `client_pending_peeks: Map<ConnectionId, Map<Uuid, ComputeInstanceId>>`

Key behaviors:

- Frontend peek sequencing registers a peek with Coordinator before issuing the compute peek
  (`Command::RegisterFrontendPeek`), and unregisters on issuance failure
  (`Command::UnregisterFrontendPeek`) (`src/adapter/src/peek_client.rs`).
- Coordinator cancels peeks:
  - connection cancel/terminate (`src/adapter/src/coord/command_handler.rs`),
  - statement timeout (best-effort cancel via `Message::CancelPendingPeeks`,
    `src/adapter/src/coord/sequencer/inner.rs`),
  - dependency/cluster drops (scan `pending_peeks` in `src/adapter/src/coord/catalog_implications.rs`).
- Completion is driven by controller responses:
  - `ControllerResponse::PeekNotification(uuid, notification, otel_ctx)` is handled on the main
    Coordinator loop (`src/adapter/src/coord/message_handler.rs`), which removes the pending peek
    and retires statement execution.

### Statement logging touchpoints

- “Old” sequencing uses `ExecuteContextExtra` + retirement (`Coordinator::retire_execution`).
- Frontend peek sequencing emits `Command::FrontendStatementLogging(FrontendStatementLoggingEvent)`
  events (`src/adapter/src/peek_client.rs`), which Coordinator handles in
  `src/adapter/src/coord/statement_logging.rs`.
- Peek completion/cancellation currently implies statement execution retirement (via
  `ExecuteContextExtra` stored in pending peek state).

## Proposed architecture

### QueryTracker as an actor

QueryTracker is a single-task actor:

- owns all peek-tracking state,
- updates state in response to commands,
- triggers side effects through a minimal effects trait,
- never blocks callers.

This is patterned after other “send commands to a single owner loop” designs already used in the
adapter (e.g., the Coordinator command queue itself), but scoped to query lifecycle concerns.

### Interfaces

#### 1) Handle trait (what other components use)

Fire-and-forget only:

```rust
pub trait QueryTrackerHandle: Clone + Send + Sync + 'static {
    fn send(&self, cmd: QueryTrackerCmd);
}
```

The concrete handle is typically a wrapper around an `mpsc::UnboundedSender<QueryTrackerCmd>`.

#### 2) Command protocol (what QueryTracker consumes)

```rust
pub enum QueryTrackerCmd {
    TrackPeek(TrackedPeek),
    UntrackPeek { uuid: Uuid },
    CancelConn { conn_id: ConnectionId },
    CancelByDrop(CancelByDrop),
    ObservePeekNotification {
        uuid: Uuid,
        notification: PeekNotification,
        otel_ctx: OpenTelemetryContext,
    },
}
```

Supporting types:

```rust
pub struct TrackedPeek {
    pub uuid: Uuid,
    pub conn_id: ConnectionId,
    pub cluster_id: ClusterId,
    pub depends_on: BTreeSet<GlobalId>,

    /// Non-trivial execution state that must be retired on completion/cancel.
    ///
    /// This must never be dropped without calling `retire`, including in error/dump paths.
    pub ctx_extra: ExecuteContextExtra,

    /// Used to log the correct execution strategy on completion.
    pub execution_strategy: StatementExecutionStrategy,

    /// If present, QueryTracker installs watch sets for lifecycle logging.
    pub watch_set: Option<WatchSetCreation>,
}

pub struct CancelByDrop {
    pub dropped_collections: BTreeSet<GlobalId>,
    pub dropped_clusters: BTreeSet<ClusterId>,
    /// Pre-formatted “relation …” names keyed by GlobalId.
    pub dropped_collection_names: BTreeMap<GlobalId, String>,
    /// Pre-formatted “cluster …” names keyed by ClusterId.
    pub dropped_cluster_names: BTreeMap<ClusterId, String>,
}
```

Key decisions:

- `TrackPeek` is **non-blocking**. Any failures during processing must be handled internally.
- `execution_strategy` is stored at track time to avoid losing information. This also addresses the
  current inability to distinguish `PersistFastPath` at completion time (today pending peek state
  only stores `is_fast_path`).

#### 3) Effects trait (how QueryTracker performs side effects)

QueryTracker performs all side effects through this interface:

```rust
pub trait QueryTrackerEffects: Send + Sync + 'static {
    fn cancel_compute_peek(&self, cluster_id: ClusterId, uuid: Uuid, response: PeekResponse);

    fn install_peek_watch_sets(
        &self,
        conn_id: ConnectionId,
        watch_set: WatchSetCreation,
    ) -> Result<(), CollectionLookupError>;

    fn end_statement_execution(
        &self,
        id: StatementLoggingId,
        reason: StatementEndedExecutionReason,
        otel_ctx: OpenTelemetryContext,
    );

    fn inc_canceled_peeks(&self, by: u64);
}
```

This is intentionally small to keep QueryTracker isolated and easy to mock.

## QueryTracker state and behavior

### State

- `peeks_by_uuid: HashMap<Uuid, TrackedPeek>`
- `peeks_by_conn: HashMap<ConnectionId, HashSet<Uuid>>`

### Additional “tombstone” state

To handle `CancelByDrop` arriving before `TrackPeek`, QueryTracker maintains short-lived
“tombstones” describing recently dropped clusters/collections (or a monotonically growing set
bounded by catalog IDs, if acceptable), and apply them to newly tracked peeks:

- If `TrackPeek` arrives and its `cluster_id` is in `dropped_clusters`, immediately cancel/retire
  it.
- If `TrackPeek` arrives and any `depends_on` is in `dropped_collections`, immediately
  cancel/retire it.

This makes cancellation by drop robust to message reordering.

Implementation note: the tombstones are time-bounded (TTL) and are expired opportunistically when
processing commands.

### Invariants

- If a peek exists in `peeks_by_uuid`, it is present in `peeks_by_conn[conn_id]`.
- All operations are idempotent:
  - duplicate `TrackPeek` overwrites or no-ops,
  - duplicate `UntrackPeek` no-ops,
  - late `ObservePeekNotification` after cancellation is ignored.

### Behavior

- `TrackPeek`:
  - installs watch sets if provided (best-effort; see failure semantics),
  - records state.
- `UntrackPeek`:
  - removes state and retires `ExecuteContextExtra` without ending execution (frontend already
    logs/returns the error, but we must not drop non-trivial state).
- `CancelConn`:
  - cancels all peeks for connection, retires statement executions as canceled.
- `CancelByDrop`:
  - cancels any peek that depends on a dropped collection or runs on a dropped cluster.
  - uses `PeekResponse::Error(...)` to match existing behavior where appropriate.
- `ObservePeekNotification`:
  - if tracked: removes state and ends statement execution based on the notification payload.
  - if untracked: ignore (covers late responses).

## Failure semantics (important due to fire-and-forget)

Because `TrackPeek` does not return an acknowledgement, QueryTracker must handle failures
internally.

The main expected failure today is watch set installation failing due to concurrent dependency
drop. QueryTracker should:

- remove the peek from state (avoid leaks),
- end statement execution with an errored/canceled reason (matching existing behavior as closely as
  possible),
- best-effort cancel the compute peek if it may have been issued already.

Important ordering requirement:

- Callers should send `TrackPeek` as early as possible, but the system must not rely on strict
  ordering between:
  - `TrackPeek` and `CancelByDrop`, and
  - compute “peek issued” and compute “peek canceled” when a cluster is dropped.

In particular, **the absence of an explicit `PeekNotification` is possible** when a compute
instance is dropped; the adapter must treat that as a best-effort cancellation outcome rather than
hang or crash.

## Implementation plan (mechanical changes)

This section lists concrete code locations to modify so implementers do not have to hunt.

### A) Add QueryTracker implementation

- Add module: `src/adapter/src/query_tracker.rs`.
- Contents:
  - command types (`QueryTrackerCmd`, `TrackedPeek`, …),
  - `QueryTracker` actor with `run()` loop,
  - handle implementation (mpsc-backed),
  - `QueryTrackerEffects` trait,
  - unit tests with a mocked effects implementation.

### B) Instantiate QueryTracker in Coordinator

- In Coordinator construction/init code (where other background tasks are spawned), create:
  - an mpsc channel,
  - a `QueryTracker` with `effects = CoordinatorQueryTrackerEffects { … }`,
  - spawn its task.

Coordinator-side effects implementation should:

- delegate cancellation to the compute controller (existing `controller.compute.cancel_peek`),
- delegate watch set installation to existing functions (`install_peek_watch_sets`),
- delegate statement execution ending to existing statement logging (`end_statement_execution`),
- increment existing metrics (`mz_canceled_peeks_total`).

### C) Route controller peek notifications to QueryTracker

Change `src/adapter/src/coord/message_handler.rs`:

- Instead of calling `Coordinator::handle_peek_notification`, send
  `QueryTrackerCmd::ObservePeekNotification { .. }`.

### D) Replace cancellation entrypoints with QueryTracker commands

Update each path that cancels peeks by connection:

- `src/adapter/src/coord/command_handler.rs`:
  - `handle_privileged_cancel` currently calls `cancel_pending_peeks`.
  - `handle_terminate` currently calls `cancel_pending_peeks`.
  - Replace with `query_tracker.send(CancelConn { .. })`.

- `src/adapter/src/coord/sequencer/inner.rs`:
  - statement timeout path currently sends `Message::CancelPendingPeeks`.
  - Either:
    - keep `Message::CancelPendingPeeks` and implement its handler by delegating to QueryTracker, or
    - replace the message with a direct `QueryTrackerCmd::CancelConn` send.

### E) Replace dependency/cluster-drop peek cleanup

Update `src/adapter/src/coord/catalog_implications.rs`:

- Instead of scanning Coordinator’s `pending_peeks` and canceling directly, construct a
  `CancelByDrop` and send it to QueryTracker.

### F) Frontend peek sequencing registration/unregistration

Today frontend peek sequencing calls:

- `Command::RegisterFrontendPeek` before issuing the compute peek.
- `Command::UnregisterFrontendPeek` if the compute peek failed to issue.

We change this to use `QueryTracker` directly.

- Plumb a `QueryTrackerHandle` into `PeekClient` at construction.
- Replace `RegisterFrontendPeek` with `query_tracker.send(TrackPeek { .. })`.
- Replace `UnregisterFrontendPeek` with `query_tracker.send(UntrackPeek { uuid })`.

Concrete code to change:
- `src/adapter/src/peek_client.rs`:
 - add field `query_tracker: QueryTrackerHandle`,
 - update `PeekClient::new(...)` signature and callers,
 - replace registration/unregistration calls.

### G) Coordinator-issued peeks (“old sequencing”)

When Coordinator itself issues peeks (old sequencing path in `src/adapter/src/coord/peek.rs`),
replace the direct insertion into `pending_peeks`/`client_pending_peeks` with
`query_tracker.send(TrackPeek { .. })`, including:

- `statement_logging_id` derived from `ExecuteContextExtra` (or, longer-term, have Coordinator pass
  it explicitly rather than embedding it in peek state),
- `execution_strategy` derived from the peek plan (Standard/FastPath/PersistFastPath/Constant).

Once this is done, pending peek state in Coordinator can be removed.

## Statement logging robustness (must not panic)

Statement logging events are intentionally fire-and-forget and can be reordered. The Coordinator
must treat “mutation” events for already-ended executions as **no-ops** (optionally with debug
logging), rather than panicking. This applies at least to:

- `SetTimestamp`
- `SetCluster`
- `SetTransientIndex`

Similarly, any debug/dump/introspection rendering must avoid constructing temporary
`ExecuteContextExtra` values that then get dropped without retirement.

## Testing strategy

Unit test QueryTracker in isolation by:

- driving sequences of commands (`TrackPeek`, `CancelConn`, `ObservePeekNotification`, …),
- asserting on:
  - internal state changes,
  - calls observed via a mocked `QueryTrackerEffects`.

Include tests for:

- idempotency (duplicate cancels/untracks/notifications),
- cancellation grouping (multiple uuids per connection and per cluster),
- dependency-drop cancellation selection,
- watch set installation failure handling,
- correct mapping of `PeekNotification` + stored `execution_strategy` into
  `StatementEndedExecutionReason::Success { execution_strategy: … }`.

## Open questions / follow-ups

- How should we represent “constant” queries (which do not issue a compute peek) in QueryTracker?
  Likely out of scope for v1; but the `execution_strategy` field and “end_statement_execution”
  effect can support it if needed later.
- Ensure `PersistFastPath` can be logged correctly by recording strategy in `TrackedPeek` (this is
  a behavior improvement over today and may require small downstream updates/tests).
- Controller notification routing:
  - **Keep Coordinator as the controller-drainer (v1)** and forward only
    `ControllerResponse::PeekNotification` into QueryTracker as `ObservePeekNotification`.
  - **Controller push model**: extend the controller API to allow registering a sink/callback for
    peek notifications so they can be delivered directly to QueryTracker (larger refactor; impacts
    readiness polling and shutdown).
  - **Dedicated controller-response dispatcher task**: introduce a separate component that drains
    `ControllerResponse` and fans out (peeks → QueryTracker, everything else → Coordinator). This
    relocates, but does not eliminate, routing, and changes Coordinator’s “single owner” handling of
  controller responses.

- Should the compute controller actively synthesize cancellations for outstanding peeks when
  dropping a compute instance (instead of dropping channels)? This would make downstream behavior
  more uniform, but requires controller/instance API changes.
