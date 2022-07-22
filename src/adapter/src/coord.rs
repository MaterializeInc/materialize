// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Translation of SQL commands into timestamped `Controller` commands.
//!
//! The various SQL commands instruct the system to take actions that are not
//! yet explicitly timestamped. On the other hand, the underlying data continually
//! change as time moves forward. On the third hand, we greatly benefit from the
//! information that some times are no longer of interest, so that we may
//! compact the representation of the continually changing collections.
//!
//! The [`Coordinator`] curates these interactions by observing the progress
//! collections make through time, choosing timestamps for its own commands,
//! and eventually communicating that certain times have irretrievably "passed".
//!
//! ## Frontiers another way
//!
//! If the above description of frontiers left you with questions, this
//! repackaged explanation might help.
//!
//! - `since` is the least recent time (i.e. oldest time) that you can read
//!   from sources and be guaranteed that the returned data is accurate as of
//!   that time.
//!
//!   Reads at times less than `since` may return values that were not actually
//!   seen at the specified time, but arrived later (i.e. the results are
//!   compacted).
//!
//!   For correctness' sake, the coordinator never chooses to read at a time
//!   less than an arrangement's `since`.
//!
//! - `upper` is the first time after the most recent time that you can read
//!   from sources and receive an immediate response. Alternately, it is the
//!   least time at which the data may still change (that is the reason we may
//!   not be able to respond immediately).
//!
//!   Reads at times >= `upper` may not immediately return because the answer
//!   isn't known yet. However, once the `upper` is > the specified read time,
//!   the read can return.
//!
//!   For the sake of returned values' freshness, the coordinator prefers
//!   performing reads at an arrangement's `upper`. However, because we more
//!   strongly prefer correctness, the coordinator will choose timestamps
//!   greater than an object's `upper` if it is also being accessed alongside
//!   objects whose `since` times are >= its `upper`.
//!
//! This illustration attempts to show, with time moving left to right, the
//! relationship between `since` and `upper`.
//!
//! - `#`: possibly inaccurate results
//! - `-`: immediate, correct response
//! - `?`: not yet known
//! - `s`: since
//! - `u`: upper
//! - `|`: eligible for coordinator to select
//!
//! ```nofmt
//! ####s----u?????
//!     |||||||||||
//! ```
//!

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Write;
use std::num::{NonZeroI64, NonZeroUsize};
use std::ops::Neg;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use chrono::{DateTime, DurationRound, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::StreamExt;
use itertools::Itertools;
use mz_repr::explain_new::Explain;
use rand::Rng;
use serde::{Deserialize, Serialize};
use timely::order::PartialOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::runtime::Handle as TokioHandle;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch, OwnedMutexGuard};
use tracing::{event, span, warn, Instrument, Level};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_compute_client::command::{
    BuildDesc, DataflowDesc, DataflowDescription, IndexDesc, ReplicaId,
};
use mz_compute_client::controller::ComputeInstanceId;
use mz_compute_client::explain::{
    DataflowGraphFormatter, Explanation, JsonViewFormatter, TimestampExplanation, TimestampSource,
};
use mz_compute_client::response::PeekResponse;
use mz_controller::{
    ComputeInstanceEvent, ConcreteComputeInstanceReplicaConfig,
    ConcreteComputeInstanceReplicaLogging, ControllerResponse,
};
use mz_expr::{
    permutation_for_arrangement, CollectionPlan, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_ore::retry::Retry;
use mz_ore::thread::JoinHandleExt;
use mz_ore::{stack, task};
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::{Numeric, NumericMaxScale};
use mz_repr::explain_new::ExprHumanizer;
use mz_repr::{
    Datum, Diff, GlobalId, RelationDesc, RelationType, Row, RowArena, ScalarType, Timestamp,
};
use mz_secrets::SecretsController;
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{
    CreateIndexStatement, CreateSourceStatement, ExplainStageNew, ExplainStageOld, FetchStatement,
    Ident, IndexOptionName, InsertSource, ObjectType, Query, Raw, RawClusterName, RawObjectName,
    SetExpr, Statement,
};
use mz_sql::catalog::{
    CatalogComputeInstance, CatalogError, CatalogItemType, CatalogTypeDetails, SessionCatalog as _,
};
use mz_sql::names::{
    Aug, FullObjectName, QualifiedObjectName, ResolvedDatabaseSpecifier, SchemaSpecifier,
};
use mz_sql::plan::{
    AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan, AlterItemRenamePlan, AlterSecretPlan,
    CreateComputeInstancePlan, CreateComputeInstanceReplicaPlan, CreateConnectionPlan,
    CreateDatabasePlan, CreateIndexPlan, CreateRecordedViewPlan, CreateRolePlan, CreateSchemaPlan,
    CreateSecretPlan, CreateSinkPlan, CreateSourcePlan, CreateTablePlan, CreateTypePlan,
    CreateViewPlan, CreateViewsPlan, DropComputeInstanceReplicaPlan, DropComputeInstancesPlan,
    DropDatabasePlan, DropItemsPlan, DropRolesPlan, DropSchemaPlan, ExecutePlan, ExplainPlan,
    ExplainPlanNew, ExplainPlanOld, FetchPlan, HirRelationExpr, IndexOption, InsertPlan,
    MutationKind, OptimizerConfig, Params, PeekPlan, Plan, QueryWhen, RaisePlan, ReadThenWritePlan,
    RecordedView, ResetVariablePlan, SendDiffsPlan, SetVariablePlan, ShowVariablePlan,
    StatementDesc, TailFrom, TailPlan, View,
};
use mz_stash::Append;
use mz_storage::controller::{CollectionDescription, ExportDescription, ReadPolicy};
use mz_storage::protocol::client::Update;
use mz_storage::types::connections::ConnectionContext;
use mz_storage::types::sinks::{SinkAsOf, SinkConnection, SinkDesc, TailSinkConnection};
use mz_storage::types::sources::{
    IngestionDescription, PostgresSourceConnection, SourceConnection, Timeline,
};
use mz_transform::Optimizer;

use crate::catalog::builtin::{BUILTINS, MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS};
use crate::catalog::{
    self, storage, BuiltinTableUpdate, Catalog, CatalogItem, CatalogState, ClusterReplicaSizeMap,
    ComputeInstance, Connection, Sink, SinkConnectionState,
};
use crate::client::{Client, ConnectionId, Handle};
use crate::command::{
    Canceled, Command, ExecuteResponse, Response, StartupMessage, StartupResponse,
};
use crate::coord::dataflow_builder::{prep_relation_expr, prep_scalar_expr, ExprPrepStyle};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::read_holds::ReadHolds;
use crate::error::AdapterError;
use crate::explain_new::{ExplainContext, Explainable, UsedIndexes};
use crate::session::{
    vars, EndTransactionAction, PreparedStatement, Session, TransactionOps, TransactionStatus,
    WriteOp,
};
use crate::sink_connection;
use crate::tail::PendingTail;
use crate::util::ClientTransmitter;

pub mod id_bundle;

mod dataflow_builder;
mod indexes;

/// The default is set to a second to track the default timestamp frequency for sources.
pub const DEFAULT_LOGICAL_COMPACTION_WINDOW_MS: Option<u64> = Some(1_000);

#[derive(Debug)]
pub enum Message<T = mz_repr::Timestamp> {
    Command(Command),
    ControllerReady,
    CreateSourceStatementReady(CreateSourceStatementReady),
    SinkConnectionReady(SinkConnectionReady),
    SendDiffs(SendDiffs),
    WriteLockGrant(tokio::sync::OwnedMutexGuard<()>),
    AdvanceTimelines,
    AdvanceLocalInput(AdvanceLocalInput<T>),
    GroupCommit,
    ComputeInstanceStatus(ComputeInstanceEvent),
    RemovePendingPeeks { conn_id: ConnectionId },
}

#[derive(Debug)]
pub struct AdvanceLocalInput<T> {
    advance_to: T,
    ids: Vec<GlobalId>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SendDiffs {
    session: Session,
    #[derivative(Debug = "ignore")]
    tx: ClientTransmitter<ExecuteResponse>,
    pub id: GlobalId,
    pub diffs: Result<Vec<(Row, Diff)>, AdapterError>,
    pub kind: MutationKind,
    pub returning: Vec<(Row, NonZeroUsize)>,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct CreateSourceStatementReady {
    pub session: Session,
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub result: Result<CreateSourceStatement<Aug>, AdapterError>,
    pub params: Params,
    pub depends_on: Vec<GlobalId>,
    pub original_stmt: Statement<Raw>,
}

/// An operation that is deferred while waiting for a lock.
enum Deferred {
    Plan(DeferredPlan),
    GroupCommit,
}

/// This is the struct meant to be paired with [`Message::WriteLockGrant`], but
/// could theoretically be used to queue any deferred plan.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct DeferredPlan {
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub session: Session,
    pub plan: Plan,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SinkConnectionReady {
    pub session: Session,
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub id: GlobalId,
    pub oid: u32,
    pub result: Result<SinkConnection, AdapterError>,
    pub compute_instance: ComputeInstanceId,
}

/// Configures a coordinator.
pub struct Config<S> {
    pub dataflow_client: mz_controller::Controller,
    pub storage: storage::Connection<S>,
    pub unsafe_mode: bool,
    pub build_info: &'static BuildInfo,
    pub metrics_registry: MetricsRegistry,
    pub now: NowFn,
    pub secrets_controller: Arc<dyn SecretsController>,
    pub availability_zones: Vec<String>,
    pub replica_sizes: ClusterReplicaSizeMap,
    pub connection_context: ConnectionContext,
}

struct PendingPeek {
    sender: mpsc::UnboundedSender<PeekResponse>,
    conn_id: ConnectionId,
}

/// The response from a `Peek`, with row multiplicities represented in unary.
///
/// Note that each `Peek` expects to generate exactly one `PeekResponse`, i.e.
/// we expect a 1:1 contract between `Peek` and `PeekResponseUnary`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PeekResponseUnary {
    Rows(Vec<Row>),
    Error(String),
    Canceled,
}

/// A pending write transaction that will be committing during the next group commit.
struct PendingWriteTxn {
    /// List of all write operations within the transaction.
    writes: Vec<WriteOp>,
    /// Transmitter used to send a response back to the client.
    client_transmitter: ClientTransmitter<ExecuteResponse>,
    /// Client response for transaction.
    response: Result<ExecuteResponse, AdapterError>,
    /// Session of the client who initiated the transaction.
    session: Session,
    /// The action to take at the end of the transaction.
    action: EndTransactionAction,
    /// Holds the coordinator's write lock.
    write_lock_guard: Option<OwnedMutexGuard<()>>,
}

impl PendingWriteTxn {
    fn has_write_lock(&self) -> bool {
        self.write_lock_guard.is_some()
    }
}

/// Timestamps used by writes in an Append command.
#[derive(Debug)]
pub struct WriteTimestamp {
    /// Timestamp that the write will take place on.
    timestamp: Timestamp,
    /// Timestamp to advance the appended table to.
    advance_to: Timestamp,
}

/// State provided to a catalog transaction closure.
pub struct CatalogTxn<'a, T> {
    dataflow_client: &'a mz_controller::Controller<T>,
    catalog: &'a CatalogState,
}

/// Holds tables needing advancement.
struct AdvanceTables<T> {
    /// The current number of tables to advance in a single batch.
    batch_size: usize,
    /// The set of tables to advance.
    set: HashSet<GlobalId>,
    /// An ordered set of work to ensure fairness. Elements may be duplicated here,
    /// so there's no guarantee that there is a corresponding element in `set`.
    work: VecDeque<GlobalId>,
    /// Timestamp at which to advance the tables.
    advance_to: T,
}

impl<T: CoordTimestamp> AdvanceTables<T> {
    fn new() -> Self {
        Self {
            batch_size: 1,
            set: HashSet::new(),
            work: VecDeque::new(),
            advance_to: T::minimum(),
        }
    }

    // Inserts ids to be advanced to ts.
    fn insert<I: Iterator<Item = GlobalId>>(&mut self, ts: T, ids: I) {
        assert!(self.advance_to.less_than(&ts));
        self.advance_to = ts;
        let ids = ids.collect::<Vec<_>>();
        self.set.extend(&ids);
        self.work.extend(ids);
    }

    /// Returns the set of tables to advance. Blocks forever if there are none.
    ///
    /// This method is cancel-safe because there are no await points when the set is non-empty.
    async fn recv(&mut self) -> AdvanceLocalInput<T> {
        if self.set.is_empty() {
            futures::future::pending::<()>().await;
        }
        let mut remaining = self.batch_size;
        let mut inputs = AdvanceLocalInput {
            advance_to: self.advance_to.clone(),
            ids: Vec::new(),
        };
        // Fetch out of the work queue to ensure that no table is starved from
        // advancement in the case that the periodic advancement interval is less than
        // the total time to advance all tables.
        while let Some(id) = self.work.pop_front() {
            // Items can be duplicated in work, so there's no guarantee that they will
            // always apper in set.
            if self.set.remove(&id) {
                inputs.ids.push(id);
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
        inputs
    }

    // Decreases the batch size to return from insert.
    fn decrease_batch(&mut self) {
        if self.batch_size > 1 {
            self.batch_size = self.batch_size.saturating_sub(1);
        }
    }

    // Increases the batch size to return from insert.
    fn increase_batch(&mut self) {
        self.batch_size = self.batch_size.saturating_add(1);
    }
}

/// Soft-state metadata about a compute replica
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaMetadata {
    /// The last time we heard from this replica (possibly rounded)
    pub last_heartbeat: DateTime<Utc>,
}

/// Glues the external world to the Timely workers.
pub struct Coordinator<S> {
    /// The controller for the storage and compute layers.
    controller: mz_controller::Controller,
    /// Optimizer instance for logical optimization of views.
    view_optimizer: Optimizer,
    catalog: Catalog<S>,

    /// Channel to manage internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,

    /// Mechanism for totally ordering write and read timestamps, so that all reads
    /// reflect exactly the set of writes that precede them, and no writes that follow.
    global_timelines: BTreeMap<Timeline, TimelineState<Timestamp>>,

    /// Tracks tables needing advancement, which can be processed at a low priority
    /// in the biased select loop.
    advance_tables: AdvanceTables<Timestamp>,

    transient_id_counter: u64,
    /// A map from connection ID to metadata about that connection for all
    /// active connections.
    active_conns: HashMap<ConnectionId, ConnMeta>,

    /// For each identifier, its read policy and any transaction holds on time.
    ///
    /// Transactions should introduce and remove constraints through the methods
    /// `acquire_read_holds` and `release_read_holds`, respectively. The base
    /// policy can also be updated, though one should be sure to communicate this
    /// to the controller for it to have an effect.
    read_capability: HashMap<GlobalId, ReadCapability<mz_repr::Timestamp>>,
    /// For each transaction, the pinned storage and compute identifiers and time at
    /// which they are pinned.
    ///
    /// Upon completing a transaction, this timestamp should be removed from the holds
    /// in `self.read_capability[id]`, using the `release_read_holds` method.
    txn_reads: HashMap<ConnectionId, TxnReads>,

    /// A map from pending peek ids to the queue into which responses are sent, and
    /// the connection id of the client that initiated the peek.
    pending_peeks: HashMap<Uuid, PendingPeek>,
    /// A map from client connection ids to a set of all pending peeks for that client
    client_pending_peeks: HashMap<ConnectionId, BTreeMap<Uuid, ComputeInstanceId>>,
    /// A map from pending tails to the tail description.
    pending_tails: HashMap<GlobalId, PendingTail>,

    /// Serializes accesses to write critical sections.
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// Holds plans deferred due to write lock.
    write_lock_wait_group: VecDeque<Deferred>,
    /// Pending writes waiting for a group commit
    pending_writes: Vec<PendingWriteTxn>,

    /// Handle to secret manager that can create and delete secrets from
    /// an arbitrary secret storage engine.
    secrets_controller: Arc<dyn SecretsController>,

    /// Extra context to pass through to connection creation.
    connection_context: ConnectionContext,

    /// Metadata about replicas that doesn't need to be persisted.
    /// Intended for inclusion in system tables.
    ///
    /// `None` is used as a tombstone value for replicas that have been
    /// dropped and for which no further updates should be recorded.
    transient_replica_metadata: HashMap<ReplicaId, Option<ReplicaMetadata>>,
}

/// Global state for a single timeline.
///
/// For each timeline we maintain a timestamp oracle, which is responsible for
/// providing read (and sometimes write) timestamps, and a set of read holds which
/// guarantee that those read timestamps are valid.
struct TimelineState<T> {
    oracle: timeline::DurableTimestampOracle<T>,
    read_holds: ReadHolds<T>,
}

/// Metadata about an active connection.
struct ConnMeta {
    /// A watch channel shared with the client to inform the client of
    /// cancellation requests. The coordinator sets the contained value to
    /// `Canceled::Canceled` whenever it receives a cancellation request that
    /// targets this connection. It is the client's responsibility to check this
    /// value when appropriate and to reset the value to
    /// `Canceled::NotCanceled` before starting a new operation.
    cancel_tx: Arc<watch::Sender<Canceled>>,
    /// Pgwire specifies that every connection have a 32-bit secret associated
    /// with it, that is known to both the client and the server. Cancellation
    /// requests are required to authenticate with the secret of the connection
    /// that they are targeting.
    secret_key: u32,
}

struct TxnReads {
    // True iff all statements run so far in the transaction are independent
    // of the chosen logical timestamp (not the PlanContext walltime). This
    // happens if both 1) there are no referenced sources or indexes and 2)
    // `mz_logical_timestamp()` is not present.
    timestamp_independent: bool,
    read_holds: crate::coord::read_holds::ReadHolds<mz_repr::Timestamp>,
}

/// Enforces critical section invariants for functions that perform writes to
/// tables, e.g. `INSERT`, `UPDATE`.
///
/// If the provided session doesn't currently hold the write lock, attempts to
/// grant it. If the coord cannot immediately grant the write lock, defers
/// executing the provided plan until the write lock is available, and exits the
/// function.
///
/// # Parameters
/// - `$coord: &mut Coord`
/// - `$tx: ClientTransmitter<ExecuteResponse>`
/// - `mut $session: Session`
/// - `$plan_to_defer: Plan`
///
/// Note that making this a macro rather than a function lets us avoid taking
/// ownership of e.g. session and lets us unilaterally enforce the return when
/// deferring work.
macro_rules! guard_write_critical_section {
    ($coord:expr, $tx:expr, $session:expr, $plan_to_defer: expr) => {
        if !$session.has_write_lock() {
            if $coord.try_grant_session_write_lock(&mut $session).is_err() {
                $coord.defer_write(Deferred::Plan(DeferredPlan {
                    tx: $tx,
                    session: $session,
                    plan: $plan_to_defer,
                }));
                return;
            }
        }
    };
}

impl<S: Append + 'static> Coordinator<S> {
    /// Returns a reference to the timestamp oracle used for reads and writes
    /// from/to a local input.
    fn get_local_timestamp_oracle(&self) -> &timeline::DurableTimestampOracle<Timestamp> {
        &self
            .global_timelines
            .get(&Timeline::EpochMilliseconds)
            .expect("no realtime timeline")
            .oracle
    }

    /// Returns a mutable reference to the timestamp oracle used for reads and writes
    /// from/to a local input.
    fn get_local_timestamp_oracle_mut(
        &mut self,
    ) -> &mut timeline::DurableTimestampOracle<Timestamp> {
        &mut self
            .global_timelines
            .get_mut(&Timeline::EpochMilliseconds)
            .expect("no realtime timeline")
            .oracle
    }

    /// Assign a timestamp for a read from a local input. Reads following writes
    /// must be at a time >= the write's timestamp; we choose "equal to" for
    /// simplicity's sake and to open as few new timestamps as possible.
    fn get_local_read_ts(&mut self) -> Timestamp {
        self.get_local_timestamp_oracle_mut().read_ts()
    }

    /// Assign a timestamp for creating a source. Writes following reads
    /// must ensure that they are assigned a strictly larger timestamp to ensure
    /// they are not visible to any real-time earlier reads.
    async fn get_local_write_ts(&mut self) -> Timestamp {
        self.global_timelines
            .get_mut(&Timeline::EpochMilliseconds)
            .expect("no realtime timeline")
            .oracle
            .write_ts(|ts| {
                self.catalog
                    .persist_timestamp(&Timeline::EpochMilliseconds, ts)
            })
            .await
    }

    /// Assign a timestamp for a write to a local input and increase the local ts.
    /// Writes following reads must ensure that they are assigned a strictly larger
    /// timestamp to ensure they are not visible to any real-time earlier reads.
    async fn get_and_step_local_write_ts(&mut self) -> WriteTimestamp {
        let timestamp = self
            .global_timelines
            .get_mut(&Timeline::EpochMilliseconds)
            .expect("no realtime timeline")
            .oracle
            .write_ts(|ts| {
                self.catalog
                    .persist_timestamp(&Timeline::EpochMilliseconds, ts)
            })
            .await;
        /* Without an ADAPTER side durable WAL, all writes must increase the timestamp and be made
         * durable via an APPEND command to STORAGE. Calling `read_ts()` here ensures that the
         * timestamp will go up for the next write.
         * The timestamp must be increased for every write because each call to APPEND will close
         * the provided timestamp, meaning no more writes can happen at that timestamp.
         * If we add an ADAPTER side durable WAL, then consecutive writes could all happen at the
         * same timestamp as long as they're written to the WAL first.
         */
        let _ = self.get_local_timestamp_oracle_mut().read_ts();
        let advance_to = timestamp.step_forward();
        WriteTimestamp {
            timestamp,
            advance_to,
        }
    }

    /// Peek the current timestamp used for operations on local inputs. Used to determine how much
    /// to block group commits by.
    ///
    /// NOTE: This can be removed once DDL is included in group commits.
    fn peek_local_ts(&self) -> Timestamp {
        self.get_local_timestamp_oracle().peek_ts()
    }

    fn now(&self) -> EpochMillis {
        (self.catalog.config().now)()
    }

    fn now_datetime(&self) -> DateTime<Utc> {
        to_datetime(self.now())
    }

    /// Initialize the storage read policies.
    ///
    /// This should be called only after a storage collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    async fn initialize_storage_read_policies(
        &mut self,
        ids: Vec<GlobalId>,
        compaction_window_ms: Option<Timestamp>,
    ) {
        self.initialize_read_policies(
            CollectionIdBundle {
                storage_ids: ids.into_iter().collect(),
                compute_ids: BTreeMap::new(),
            },
            compaction_window_ms,
        )
        .await;
    }

    /// Initialize the compute read policies.
    ///
    /// This should be called only after a compute collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    async fn initialize_compute_read_policies(
        &mut self,
        ids: Vec<GlobalId>,
        instance: ComputeInstanceId,
        compaction_window_ms: Option<Timestamp>,
    ) {
        let mut compute_ids: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
        compute_ids.insert(instance, ids.into_iter().collect());
        self.initialize_read_policies(
            CollectionIdBundle {
                storage_ids: BTreeSet::new(),
                compute_ids,
            },
            compaction_window_ms,
        )
        .await;
    }

    /// Initialize the storage and compute read policies.
    ///
    /// This should be called only after a collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn initialize_read_policies(
        &mut self,
        id_bundle: CollectionIdBundle,
        compaction_window_ms: Option<Timestamp>,
    ) {
        // We do compute first, and they may result in additional storage policy effects.
        for (compute_instance, compute_ids) in id_bundle.compute_ids.iter() {
            let mut compute_policy_updates = Vec::new();
            for id in compute_ids.iter() {
                let policy = match compaction_window_ms {
                    Some(time) => ReadPolicy::lag_writes_by(time),
                    None => ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum())),
                };

                let mut read_capability: ReadCapability<_> = policy.into();

                if let Some(timeline) = self.get_timeline(*id) {
                    let TimelineState { read_holds, .. } =
                        self.ensure_timeline_state(timeline).await;
                    read_capability
                        .holds
                        .update_iter(Some((read_holds.time, 1)));
                    read_holds
                        .id_bundle
                        .compute_ids
                        .entry(*compute_instance)
                        .or_default()
                        .insert(*id);
                }

                self.read_capability.insert(*id, read_capability);
                compute_policy_updates.push((*id, self.read_capability[&id].policy()));
            }
            self.controller
                .compute_mut(*compute_instance)
                .unwrap()
                .set_read_policy(compute_policy_updates)
                .await
                .unwrap();
        }

        let mut storage_policy_updates = Vec::new();
        for id in id_bundle.storage_ids.iter() {
            let policy = match compaction_window_ms {
                Some(time) => ReadPolicy::lag_writes_by(time),
                None => ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum())),
            };

            let mut read_capability: ReadCapability<_> = policy.into();

            if let Some(timeline) = self.get_timeline(*id) {
                let TimelineState { read_holds, .. } = self.ensure_timeline_state(timeline).await;
                read_capability
                    .holds
                    .update_iter(Some((read_holds.time, 1)));
                read_holds.id_bundle.storage_ids.insert(*id);
            }

            self.read_capability.insert(*id, read_capability);
            storage_policy_updates.push((*id, self.read_capability[&id].policy()));
        }
        self.controller
            .storage_mut()
            .set_read_policy(storage_policy_updates)
            .await
            .unwrap();
    }

    /// Ensures that a global timeline state exists for `timeline`.
    async fn ensure_timeline_state(&mut self, timeline: Timeline) -> &mut TimelineState<Timestamp> {
        if !self.global_timelines.contains_key(&timeline) {
            self.global_timelines.insert(
                timeline.clone(),
                TimelineState {
                    oracle: timeline::DurableTimestampOracle::new(
                        Timestamp::minimum(),
                        Timestamp::minimum,
                        *timeline::TIMESTAMP_PERSIST_INTERVAL,
                        |ts| self.catalog.persist_timestamp(&timeline, ts),
                    )
                    .await,
                    read_holds: ReadHolds::new(Timestamp::minimum()),
                },
            );
        }
        self.global_timelines
            .get_mut(&timeline)
            .expect("inserted above")
    }

    /// Initializes coordinator state based on the contained catalog. Must be
    /// called after creating the coordinator and before calling the
    /// `Coordinator::serve` method.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn bootstrap(
        &mut self,
        mut builtin_table_updates: Vec<BuiltinTableUpdate>,
    ) -> Result<(), AdapterError> {
        let mut persisted_log_ids = vec![];
        for instance in self.catalog.compute_instances() {
            self.controller
                .create_instance(instance.id, instance.logging.clone())
                .await;
            for (replica_id, replica) in instance.replicas_by_id.clone() {
                let introspection_collections = replica
                    .config
                    .persisted_logs
                    .get_logs()
                    .iter()
                    .map(|(variant, id)| (*id, variant.desc().into()))
                    .collect();

                // Create collections does not recreate existing collections, so it is safe to
                // always call it.
                self.controller
                    .storage_mut()
                    .create_collections(introspection_collections)
                    .await
                    .unwrap();

                persisted_log_ids.extend(replica.config.persisted_logs.get_log_ids().iter());

                self.controller
                    .add_replica_to_instance(instance.id, replica_id, replica.config)
                    .await
                    .unwrap();
            }
        }

        self.initialize_storage_read_policies(persisted_log_ids, None)
            .await;

        let mut entries: Vec<_> = self.catalog.entries().cloned().collect();
        // Topologically sort entries based on the used_by relationship
        entries.sort_unstable_by(|a, b| {
            use std::cmp::Ordering;
            if a.used_by().contains(&b.id()) {
                Ordering::Less
            } else if b.used_by().contains(&a.id()) {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });

        let logs: HashSet<_> = BUILTINS::logs()
            .map(|log| self.catalog.resolve_builtin_log(log))
            .collect();

        // Capture identifiers that need to have their read holds relaxed once the bootstrap completes.
        let mut policies_to_set: CollectionIdBundle = Default::default();

        let source_status_collection_id = self
            .catalog
            .resolve_builtin_storage_collection(&crate::catalog::builtin::MZ_SOURCE_STATUS_HISTORY);

        for entry in &entries {
            match entry.item() {
                // Currently catalog item rebuild assumes that sinks and
                // indexes are always built individually and does not store information
                // about how it was built. If we start building multiple sinks and/or indexes
                // using a single dataflow, we have to make sure the rebuild process re-runs
                // the same multiple-build dataflow.
                CatalogItem::Source(source) => {
                    // Re-announce the source description.
                    let mut ingestion = IngestionDescription {
                        desc: source.source_desc.clone(),
                        source_imports: BTreeMap::new(),
                        storage_metadata: (),
                        typ: source.desc.typ().clone(),
                    };

                    for id in entry.uses() {
                        if self.catalog.state().get_entry(id).source().is_some() {
                            ingestion.source_imports.insert(*id, ());
                        }
                    }

                    self.controller
                        .storage_mut()
                        .create_collections(vec![(
                            entry.id(),
                            CollectionDescription {
                                desc: source.desc.clone(),
                                ingestion: Some(ingestion),
                                remote_addr: source.remote_addr.clone(),
                                since: None,
                                status_collection_id: Some(source_status_collection_id),
                            },
                        )])
                        .await
                        .unwrap();

                    policies_to_set.storage_ids.insert(entry.id());
                }
                CatalogItem::Table(table) => {
                    let collection_desc = table.desc.clone().into();
                    self.controller
                        .storage_mut()
                        .create_collections(vec![(entry.id(), collection_desc)])
                        .await
                        .unwrap();

                    policies_to_set.storage_ids.insert(entry.id());
                }
                CatalogItem::Index(idx) => {
                    if logs.contains(&idx.on) {
                        policies_to_set
                            .compute_ids
                            .entry(idx.compute_instance)
                            .or_insert_with(BTreeSet::new)
                            .insert(entry.id());
                    } else {
                        let dataflow = self
                            .dataflow_builder(idx.compute_instance)
                            .build_index_dataflow(entry.id())?;
                        // What follows is morally equivalent to `self.ship_dataflow(df, idx.compute_instance)`,
                        // but we cannot call that as it will also downgrade the read hold on the index.
                        policies_to_set
                            .compute_ids
                            .entry(idx.compute_instance)
                            .or_insert_with(BTreeSet::new)
                            .extend(dataflow.export_ids());
                        let dataflow_plan =
                            vec![self.finalize_dataflow(dataflow, idx.compute_instance)];
                        self.controller
                            .compute_mut(idx.compute_instance)
                            .unwrap()
                            .create_dataflows(dataflow_plan)
                            .await
                            .unwrap();
                    }
                }
                CatalogItem::View(_) => (),
                CatalogItem::RecordedView(rview) => {
                    // Re-create the storage collection.
                    let collection_desc = rview.desc.clone().into();
                    self.controller
                        .storage_mut()
                        .create_collections(vec![(entry.id(), collection_desc)])
                        .await
                        .unwrap();

                    policies_to_set.storage_ids.insert(entry.id());

                    // Re-create the sink on the compute instance.
                    let id_bundle = self
                        .index_oracle(rview.compute_instance)
                        .sufficient_collections(&rview.depends_on);
                    let as_of = self.least_valid_read(&id_bundle);
                    let internal_view_id = self.allocate_transient_id()?;
                    let df = self
                        .dataflow_builder(rview.compute_instance)
                        .build_recorded_view_dataflow(entry.id(), as_of, internal_view_id)?;
                    self.ship_dataflow(df, rview.compute_instance).await;
                }
                CatalogItem::Sink(sink) => {
                    // Re-create the sink on the compute instance.
                    let builder = match &sink.connection {
                        SinkConnectionState::Pending(builder) => builder,
                        SinkConnectionState::Ready(_) => {
                            panic!("sink already initialized during catalog boot")
                        }
                    };
                    let connection = sink_connection::build(
                        builder.clone(),
                        entry.id(),
                        self.connection_context.clone(),
                    )
                    .await
                    .with_context(|| format!("recreating sink {}", entry.name()))?;
                    // `builtin_table_updates` is the desired state of the system tables. However,
                    // it already contains a (cur_sink, +1) entry from [`Catalog::open`]. The line
                    // below this will negate that entry with a (cur_sink, -1) entry. The
                    // `handle_sink_connection_ready` call will delete the current sink, create a
                    // new sink, and send the following appends to STORAGE: (cur_sink, -1),
                    // (new_sink, +1). Then we add a (new_sink, +1) entry to
                    // `builtin_table_updates`.
                    builtin_table_updates.extend(self.catalog.pack_item_update(entry.id(), -1));
                    self.handle_sink_connection_ready(
                        entry.id(),
                        entry.oid(),
                        connection,
                        // The sink should be established on a specific compute instance.
                        sink.compute_instance,
                        None,
                    )
                    .await?;
                    builtin_table_updates.extend(self.catalog.pack_item_update(entry.id(), 1));
                }
                CatalogItem::StorageCollection(coll) => {
                    let collection_desc = coll.desc.clone().into();
                    self.controller
                        .storage_mut()
                        .create_collections(vec![(entry.id(), collection_desc)])
                        .await
                        .unwrap();

                    policies_to_set.storage_ids.insert(entry.id());
                }
                // Nothing to do for these cases
                CatalogItem::Log(_)
                | CatalogItem::Type(_)
                | CatalogItem::Func(_)
                | CatalogItem::Secret(_)
                | CatalogItem::Connection(_) => {}
            }
        }

        // Having installed all entries, creating all constraints, we can now relax read policies.
        self.initialize_read_policies(policies_to_set, DEFAULT_LOGICAL_COMPACTION_WINDOW_MS)
            .await;

        // Announce the completion of initialization.
        self.controller.initialization_complete();

        // Announce primary and foreign key relationships.
        let mz_view_keys = self.catalog.resolve_builtin_table(&MZ_VIEW_KEYS);
        for log in BUILTINS::logs() {
            let log_id = &self.catalog.resolve_builtin_log(log).to_string();
            builtin_table_updates.extend(
                log.variant
                    .desc()
                    .typ()
                    .keys
                    .iter()
                    .enumerate()
                    .flat_map(move |(index, key)| {
                        key.iter().map(move |k| {
                            let row = Row::pack_slice(&[
                                Datum::String(log_id),
                                Datum::Int64(*k as i64),
                                Datum::Int64(index as i64),
                            ]);
                            BuiltinTableUpdate {
                                id: mz_view_keys,
                                row,
                                diff: 1,
                            }
                        })
                    }),
            );

            let mz_foreign_keys = self.catalog.resolve_builtin_table(&MZ_VIEW_FOREIGN_KEYS);
            builtin_table_updates.extend(
                log.variant.foreign_keys().into_iter().enumerate().flat_map(
                    |(index, (parent, pairs))| {
                        let parent_log =
                            BUILTINS::logs().find(|src| src.variant == parent).unwrap();
                        let parent_id = self.catalog.resolve_builtin_log(parent_log).to_string();
                        pairs.into_iter().map(move |(c, p)| {
                            let row = Row::pack_slice(&[
                                Datum::String(&log_id),
                                Datum::Int64(c as i64),
                                Datum::String(&parent_id),
                                Datum::Int64(p as i64),
                                Datum::Int64(index as i64),
                            ]);
                            BuiltinTableUpdate {
                                id: mz_foreign_keys,
                                row,
                                diff: 1,
                            }
                        })
                    },
                ),
            )
        }

        // Advance all tables to the current timestamp
        let WriteTimestamp {
            timestamp: _,
            advance_to,
        } = self.get_and_step_local_write_ts().await;
        let appends = entries
            .iter()
            .filter(|entry| entry.is_table())
            .map(|entry| (entry.id(), Vec::new(), advance_to))
            .collect();
        self.controller.storage_mut().append(appends).await.unwrap();

        // Add builtin table updates the clear the contents of all system tables
        let read_ts = self.get_local_read_ts();
        for system_table in entries
            .iter()
            .filter(|entry| entry.is_table() && entry.id().is_system())
        {
            let current_contents = self
                .controller
                .storage_mut()
                .snapshot(system_table.id(), read_ts)
                .await
                .unwrap();
            let retractions = current_contents
                .into_iter()
                .map(|(row, diff)| BuiltinTableUpdate {
                    id: system_table.id(),
                    row,
                    diff: diff.neg(),
                });
            builtin_table_updates.extend(retractions);
        }

        self.send_builtin_table_updates(builtin_table_updates).await;

        Ok(())
    }

    /// Serves the coordinator, receiving commands from users over `cmd_rx`
    /// and feedback from dataflow workers over `feedback_rx`.
    ///
    /// You must call `bootstrap` before calling this method.
    async fn serve(
        mut self,
        mut internal_cmd_rx: mpsc::UnboundedReceiver<Message>,
        mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    ) {
        // For the realtime timeline, an explicit SELECT or INSERT on a table will bump the
        // table's timestamps, but there are cases where timestamps are not bumped but
        // we expect the closed timestamps to advance (`AS OF X`, TAILing views over
        // RT sources and tables). To address these, spawn a task that forces table
        // timestamps to close on a regular interval. This roughly tracks the behavior
        // of realtime sources that close off timestamps on an interval.
        //
        // For non-realtime timelines, nothing pushes the timestamps forward, so we must do
        // it manually.
        let mut advance_timelines_interval =
            tokio::time::interval(self.catalog.config().timestamp_frequency);
        // Watcher that listens for and reports compute service status changes.
        let mut compute_events = self.controller.watch_compute_services();

        loop {
            // Before adding a branch to this select loop, please ensure that the branch is
            // cancellation safe and add a comment explaining why. You can refer here for more
            // info: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
            let msg = select! {
                // Order matters here. We want to process internal commands
                // before processing external commands.
                biased;

                // `recv()` on `UnboundedReceiver` is cancel-safe:
                // https://docs.rs/tokio/1.8.0/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety
                Some(m) = internal_cmd_rx.recv() => m,
                // `next()` on any stream is cancel-safe:
                // https://docs.rs/tokio-stream/0.1.9/tokio_stream/trait.StreamExt.html#cancel-safety
                Some(event) = compute_events.next() => Message::ComputeInstanceStatus(event),
                // See [`mz_controller::Controller::Controller::ready`] for notes
                // on why this is cancel-safe.
                () = self.controller.ready() => {
                    Message::ControllerReady
                }
                // `recv()` on `UnboundedReceiver` is cancellation safe:
                // https://docs.rs/tokio/1.8.0/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety
                m = cmd_rx.recv() => match m {
                    None => break,
                    Some(m) => Message::Command(m),
                },
                // `tick()` on `Interval` is cancel-safe:
                // https://docs.rs/tokio/1.19.2/tokio/time/struct.Interval.html#cancel-safety
                _ = advance_timelines_interval.tick() => Message::AdvanceTimelines,

                // At the lowest priority, process table advancements. This is a blocking
                // HashMap instead of a channel so that we can delay the determination of
                // which table to advance until we know we can process it. In the event of
                // very high traffic where a second AdvanceLocalInputs message occurs before
                // advance_tables is fully emptied, this allows us to replace an old request
                // with a new one, avoiding duplication of work, which wouldn't be possible if
                // we had already sent all AdvanceLocalInput messages on a channel.
                // See [`AdvanceTables::recv`] for notes on why this is cancel-safe.
                inputs = self.advance_tables.recv() => {
                    Message::AdvanceLocalInput(inputs)
                },
            };

            // All message processing functions trace. Start a parent span for them to make
            // it easy to find slow messages.
            let span = span!(Level::DEBUG, "coordinator message processing");
            let _enter = span.enter();

            match msg {
                Message::Command(cmd) => self.message_command(cmd).await,
                Message::ControllerReady => {
                    if let Some(m) = self.controller.process().await.unwrap() {
                        self.message_controller(m).await
                    }
                }
                Message::CreateSourceStatementReady(ready) => {
                    self.message_create_source_statement_ready(ready).await
                }
                Message::SinkConnectionReady(ready) => {
                    self.message_sink_connection_ready(ready).await
                }
                Message::WriteLockGrant(write_lock_guard) => {
                    // It's possible to have more incoming write lock grants
                    // than pending writes because of cancellations.
                    if let Some(ready) = self.write_lock_wait_group.pop_front() {
                        match ready {
                            Deferred::Plan(mut ready) => {
                                ready.session.grant_write_lock(write_lock_guard);
                                // Write statements never need to track catalog
                                // dependencies.
                                let depends_on = vec![];
                                self.sequence_plan(ready.tx, ready.session, ready.plan, depends_on)
                                    .await;
                            }
                            Deferred::GroupCommit => self.group_commit().await,
                        }
                    }
                    // N.B. if no deferred plans, write lock is released by drop
                    // here.
                }
                Message::SendDiffs(diffs) => self.message_send_diffs(diffs),
                Message::AdvanceTimelines => {
                    self.message_advance_timelines().await;
                }
                Message::AdvanceLocalInput(inputs) => {
                    self.advance_local_input(inputs).await;
                }
                Message::GroupCommit => {
                    self.try_group_commit().await;
                }
                Message::ComputeInstanceStatus(status) => {
                    self.message_compute_instance_status(status).await
                }
                // Processing this message DOES NOT send a response to the client;
                // in any situation where you use it, you must also have a code
                // path that responds to the client (e.g. reporting an error).
                Message::RemovePendingPeeks { conn_id } => {
                    self.remove_pending_peeks(conn_id).await;
                }
            }

            if let Some(timestamp) = self.get_local_timestamp_oracle_mut().should_advance_to() {
                self.advance_local_inputs(timestamp).await;
            }
        }
    }

    // Enqueue requests to advance all local inputs (tables) to the current wall
    // clock or at least a time greater than any previous table read (if wall
    // clock has gone backward). These are not processed in a single append call
    // because they currently processed serially by persist. In order to allow
    // other coordinator messages to be processed (like user queries), split up the
    // processing of this work.
    async fn advance_local_inputs(&mut self, advance_to: mz_repr::Timestamp) {
        self.advance_tables.insert(
            advance_to,
            self.catalog
                .entries()
                .filter_map(|e| if e.is_table() { Some(e.id()) } else { None }),
        );
    }

    // Advance a local input (table). This downgrades the capabilitiy of a table,
    // which means that it can no longer produce new data before this timestamp.
    #[tracing::instrument(level = "debug", skip_all, fields(num_tables = inputs.ids.len()))]
    async fn advance_local_input(&mut self, inputs: AdvanceLocalInput<mz_repr::Timestamp>) {
        // We split up table advancement into batches of requests so that user queries
        // are not blocked waiting for this periodic work to complete. MAX_WAIT is the
        // maximum amount of time we are willing to block user queries for. We could
        // process tables one at a time, but that increases the overall processing time
        // because we miss out on batching the requests to the postgres server. To
        // balance these two goals (not blocking user queries, minimizing time to
        // advance tables), we record how long a batch takes to process, and will
        // adjust the size of the next batch up or down based on the response time.
        //
        // On one extreme, should we ever be able to advance all tables in less time
        // than MAX_WAIT (probably due to connection pools or other actual parallelism
        // on the persist side), great, we've minimized the total processing time
        // without blocking user queries for more than our target. On the other extreme
        // where we can only process one table at a time (probably due to the postgres
        // server being over used or some other cloud/network slowdown inbetween), the
        // AdvanceTables struct will gracefully attempt to close tables in a bounded
        // and fair manner.
        const MAX_WAIT: Duration = Duration::from_millis(50);
        // Advancement that occurs within WINDOW from MAX_WAIT is fine, and won't
        // change the batch size.
        const WINDOW: Duration = Duration::from_millis(10);
        let start = Instant::now();
        let storage = self.controller.storage();
        let appends = inputs
            .ids
            .into_iter()
            .filter_map(|id| {
                if self.catalog.try_get_entry(&id).is_none()
                    || !storage
                        .collection(id)
                        .unwrap()
                        .write_frontier
                        .less_than(&inputs.advance_to)
                {
                    // Filter out tables that were dropped while waiting for advancement.
                    // Filter out tables whose upper is already advanced. This is not needed for
                    // correctness (advance_to and write_frontier should be equal here), just
                    // performance, as it's a no-op.
                    None
                } else {
                    Some((id, vec![], inputs.advance_to))
                }
            })
            .collect::<Vec<_>>();
        let num_updates = appends.len();
        self.controller.storage_mut().append(appends).await.unwrap();
        let elapsed = start.elapsed();
        if elapsed > (MAX_WAIT + WINDOW) {
            self.advance_tables.decrease_batch();
        } else if elapsed < (MAX_WAIT - WINDOW) && num_updates == self.advance_tables.batch_size {
            // Only increase the batch size if it completed under the window and the batch
            // was full.
            self.advance_tables.increase_batch();
        }
    }

    /// Attempts to commit all pending write transactions in a group commit. If the timestamp
    /// chosen for the writes is not ahead of `now()`, then we can execute and commit the writes
    /// immediately. Otherwise we must wait for `now()` to advance past the timestamp chosen for the
    /// writes.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn try_group_commit(&mut self) {
        if self.pending_writes.is_empty() {
            return;
        }

        // If we need to sleep below, then it's possible that some DDL may execute while we sleep.
        // In that case, the DDL will use some timestamp greater than or equal to the time that we
        // peeked, closing the peeked time and making it invalid for future writes. Therefore, we
        // must get a new valid timestamp everytime this method is called.
        // In the future we should include DDL in group commits, to avoid this issue. Then
        // `self.peek_local_write_ts()` can be removed. Instead we can call
        // `self.get_and_step_local_write_ts()` and safely use that value once we wake up.
        let timestamp = self.peek_local_ts();
        let now = (self.catalog.config().now)();
        if timestamp > now {
            // Cap retry time to 1s. In cases where the system clock has retreated by
            // some large amount of time, this prevents against then waiting for that
            // large amount of time in case the system clock then advances back to near
            // what it was.
            let remaining_ms = std::cmp::min(timestamp.saturating_sub(now), 1_000);
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "group_commit", async move {
                tokio::time::sleep(Duration::from_millis(remaining_ms)).await;
                internal_cmd_tx
                    .send(Message::GroupCommit)
                    .expect("sending to internal_cmd_tx cannot fail");
            });
        } else if self
            .pending_writes
            .iter()
            .any(|pending_write| pending_write.has_write_lock())
        {
            // If some transaction already holds the write lock, then we can execute a group
            // commit.
            self.group_commit().await;
        } else if let Ok(_guard) = Arc::clone(&self.write_lock).try_lock_owned() {
            // If no transaction holds the write lock, then we need to acquire it.
            self.group_commit().await;
        } else {
            // If some running transaction already holds the write lock, then one of the
            // following things will happen:
            //   1. The transaction will submit a write which will transfer the
            //      ownership of the lock to group commit and trigger another group
            //      group commit.
            //   2. The transaction will complete without submitting a write (abort,
            //      empty writes, etc) which will drop the lock. The deferred group
            //      commit will then acquire the lock and execute a group commit.
            self.defer_write(Deferred::GroupCommit);
        }
    }

    /// Commits all pending write transactions at the same timestamp. All pending writes will be
    /// combined into a single Append command and sent to STORAGE as a single batch. All writes will
    /// happen at the same timestamp and all involved tables will be advanced to some timestamp
    /// larger than the timestamp of the write.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn group_commit(&mut self) {
        if self.pending_writes.is_empty() {
            return;
        }

        // The value returned here still might be ahead of `now()` if `now()` has gone backwards at
        // any point during this method. We will still commit the write without waiting for `now()`
        // to advance. This is ok because the next batch of writes will trigger the wait loop in
        // `try_group_commit()` if `now()` hasn't advanced past the global timeline, preventing
        // an unbounded advancin of the global timeline ahead of `now()`.
        let WriteTimestamp {
            timestamp,
            advance_to,
        } = self.get_and_step_local_write_ts().await;
        let mut appends: HashMap<GlobalId, Vec<Update<Timestamp>>> =
            HashMap::with_capacity(self.pending_writes.len());
        let mut responses = Vec::with_capacity(self.pending_writes.len());
        for PendingWriteTxn {
            writes,
            client_transmitter,
            response,
            session,
            action,
            write_lock_guard: _,
        } in self.pending_writes.drain(..)
        {
            for WriteOp { id, rows } in writes {
                // If the table that some write was targeting has been deleted while the write was
                // waiting, then the write will be ignored and we respond to the client that the
                // write was successful. This is only possible if the write and the delete were
                // concurrent. Therefore, we are free to order the write before the delete without
                // violating any consistency guarantees.
                if self.catalog.try_get_entry(&id).is_some() {
                    let updates = rows
                        .into_iter()
                        .map(|(row, diff)| Update {
                            row,
                            diff,
                            timestamp,
                        })
                        .collect::<Vec<_>>();
                    appends.entry(id).or_default().extend(updates);
                }
            }
            responses.push((client_transmitter, response, session, action));
        }
        let appends = appends
            .into_iter()
            .map(|(id, updates)| (id, updates, advance_to))
            .collect();
        self.controller.storage_mut().append(appends).await.unwrap();
        for (client_transmitter, response, mut session, action) in responses {
            session.vars_mut().end_transaction(action);
            client_transmitter.send(response, session);
        }
    }

    /// Submit a write to be executed during the next group commit.
    fn submit_write(&mut self, pending_write_txn: PendingWriteTxn) {
        self.internal_cmd_tx
            .send(Message::GroupCommit)
            .expect("sending to internal_cmd_tx cannot fail");
        self.pending_writes.push(pending_write_txn);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_controller(&mut self, message: ControllerResponse) {
        event!(Level::TRACE, message = format!("{:?}", message));
        match message {
            ControllerResponse::PeekResponse(uuid, response, otel_ctx) => {
                // We expect exactly one peek response, which we forward. Then we clean up the
                // peek's state in the coordinator.
                if let Some(PendingPeek {
                    sender: rows_tx,
                    conn_id,
                }) = self.pending_peeks.remove(&uuid)
                {
                    otel_ctx.attach_as_parent();
                    // Peek cancellations are best effort, so we might still
                    // receive a response, even though the recipient is gone.
                    let _ = rows_tx.send(response);
                    if let Some(uuids) = self.client_pending_peeks.get_mut(&conn_id) {
                        uuids.remove(&uuid);
                        if uuids.is_empty() {
                            self.client_pending_peeks.remove(&conn_id);
                        }
                    }
                }
                // Cancellation may cause us to receive responses for peeks no
                // longer in `self.pending_peeks`, so we quietly ignore them.
            }
            ControllerResponse::TailResponse(sink_id, response) => {
                // We use an `if let` here because the peek could have been canceled already.
                // We can also potentially receive multiple `Complete` responses, followed by
                // a `Dropped` response.
                if let Some(pending_tail) = self.pending_tails.get_mut(&sink_id) {
                    let remove = pending_tail.process_response(response);
                    if remove {
                        self.pending_tails.remove(&sink_id);
                    }
                }
            }
            ControllerResponse::ComputeReplicaHeartbeat(replica_id, when) => {
                let replica_status_granularity = chrono::Duration::seconds(60);
                let when_coarsened = when
                    .duration_trunc(replica_status_granularity)
                    .expect("Time coarsening should not fail");
                let new = ReplicaMetadata {
                    last_heartbeat: when_coarsened,
                };
                let old = match self
                    .transient_replica_metadata
                    .insert(replica_id, Some(new.clone()))
                {
                    None => None,
                    // `None` is the tombstone for a removed replica
                    Some(None) => return,
                    Some(Some(md)) => Some(md),
                };

                if old.as_ref() != Some(&new) {
                    let retraction = old.map(|old| {
                        self.catalog
                            .state()
                            .pack_replica_heartbeat_update(replica_id, old, -1)
                    });
                    let insertion = self
                        .catalog
                        .state()
                        .pack_replica_heartbeat_update(replica_id, new, 1);
                    let updates = if let Some(retraction) = retraction {
                        vec![retraction, insertion]
                    } else {
                        vec![insertion]
                    };
                    self.send_builtin_table_updates(updates).await;
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, tx, session))]
    async fn message_create_source_statement_ready(
        &mut self,
        CreateSourceStatementReady {
            mut session,
            tx,
            result,
            params,
            depends_on,
            original_stmt,
        }: CreateSourceStatementReady,
    ) {
        let stmt = match result {
            Ok(stmt) => stmt,
            Err(e) => return tx.send(Err(e), session),
        };

        // Ensure that all dependencies still exist after purification, as a
        // `DROP CONNECTION` may have sneaked in. If any have gone missing, we
        // repurify the original statement. This will either produce a nice
        // "unknown connector" error, or pick up a new connector that has
        // replaced the dropped connector.
        //
        // WARNING: If we support `ALTER CONNECTION`, we'll need to also check
        // for connectors that were altered while we were purifying.
        if !depends_on
            .iter()
            .all(|id| self.catalog.try_get_entry(id).is_some())
        {
            self.handle_execute_inner(original_stmt, params, session, tx)
                .await;
            return;
        }

        let plan = match self
            .handle_statement(&mut session, Statement::CreateSource(stmt), &params)
            .await
        {
            Ok(Plan::CreateSource(plan)) => plan,
            Ok(_) => unreachable!("planning CREATE SOURCE must result in a Plan::CreateSource"),
            Err(e) => return tx.send(Err(e), session),
        };

        let result = self
            .sequence_create_source(&mut session, plan, depends_on)
            .await;
        tx.send(result, session);
    }

    #[tracing::instrument(level = "debug", skip(self, tx, session))]
    async fn message_sink_connection_ready(
        &mut self,
        SinkConnectionReady {
            session,
            tx,
            id,
            oid,
            result,
            compute_instance,
        }: SinkConnectionReady,
    ) {
        match result {
            Ok(connection) => {
                // NOTE: we must not fail from here on out. We have a
                // connection, which means there is external state (like
                // a Kafka topic) that's been created on our behalf. If
                // we fail now, we'll leak that external state.
                if self.catalog.try_get_entry(&id).is_some() {
                    // TODO(benesch): this `expect` here is possibly scary, but
                    // no better solution presents itself. Possibly sinks should
                    // have an error bit, and an error here would set the error
                    // bit on the sink.
                    self.handle_sink_connection_ready(
                        id,
                        oid,
                        connection,
                        compute_instance,
                        Some(&session),
                    )
                    .await
                    .expect("sinks should be validated by sequence_create_sink");
                } else {
                    // Another session dropped the sink while we were
                    // creating the connection. Report to the client that
                    // we created the sink, because from their
                    // perspective we did, as there is state (e.g. a
                    // Kafka topic) they need to clean up.
                }
                tx.send(Ok(ExecuteResponse::CreatedSink { existed: false }), session);
            }
            Err(e) => {
                // Drop the placeholder sink if still present.
                if self.catalog.try_get_entry(&id).is_some() {
                    self.catalog_transact(Some(&session), vec![catalog::Op::DropItem(id)], |_| {
                        Ok(())
                    })
                    .await
                    .expect("deleting placeholder sink cannot fail");
                } else {
                    // Another session may have dropped the placeholder sink while we were
                    // attempting to create the connection, in which case we don't need to do
                    // anything.
                }
                tx.send(Err(e), session);
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(id, kind))]
    fn message_send_diffs(
        &mut self,
        SendDiffs {
            mut session,
            tx,
            id,
            diffs,
            kind,
            returning,
        }: SendDiffs,
    ) {
        event!(Level::TRACE, diffs = format!("{:?}", diffs));
        match diffs {
            Ok(diffs) => {
                tx.send(
                    self.sequence_send_diffs(
                        &mut session,
                        SendDiffsPlan {
                            id,
                            updates: diffs,
                            kind,
                            returning,
                        },
                    ),
                    session,
                );
            }
            Err(e) => {
                tx.send(Err(e), session);
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn message_advance_timelines(&mut self) {
        // Convince the coordinator it needs to open a new timestamp
        // and advance inputs.
        // Fast forwarding puts the `TimestampOracle` in write mode,
        // which means the next read may have to wait for table
        // advancements. To prevent this we explicitly put
        // the `TimestampOracle` in read mode. Writes will always
        // advance a table no matter what mode the `TimestampOracle`
        // is in. We step back the value of `now()` so that the
        // next write can happen at `now()` and not a value above
        // `now()`
        let global_timelines = std::mem::take(&mut self.global_timelines);
        for (
            timeline,
            TimelineState {
                mut oracle,
                read_holds,
            },
        ) in global_timelines
        {
            let now = if timeline == Timeline::EpochMilliseconds {
                let now = self.now();
                now.step_back().unwrap_or(now)
            } else {
                // For non realtime sources, we define now as the largest timestamp, not in
                // advance of any object's upper. This is the largest timestamp that is closed
                // to writes.
                let id_bundle = self.ids_in_timeline(&timeline);
                self.largest_not_in_advance_of_upper(&id_bundle)
            };
            oracle
                .fast_forward(now, |ts| self.catalog.persist_timestamp(&timeline, ts))
                .await;
            let read_ts = oracle.read_ts();
            let read_holds = self.update_read_hold(read_holds, read_ts).await;
            self.global_timelines
                .insert(timeline, TimelineState { oracle, read_holds });
        }
    }

    fn ids_in_timeline(&self, timeline: &Timeline) -> CollectionIdBundle {
        let mut id_bundle = CollectionIdBundle::default();
        for entry in self.catalog.entries() {
            if let Some(entry_timeline) = self.get_timeline(entry.id()) {
                if timeline == &entry_timeline {
                    match entry.item() {
                        CatalogItem::Table(_)
                        | CatalogItem::Source(_)
                        | CatalogItem::RecordedView(_)
                        | CatalogItem::StorageCollection(_) => {
                            id_bundle.storage_ids.insert(entry.id());
                        }
                        CatalogItem::Index(index) => {
                            id_bundle
                                .compute_ids
                                .entry(index.compute_instance)
                                .or_default()
                                .insert(entry.id());
                        }
                        CatalogItem::View(_)
                        | CatalogItem::Sink(_)
                        | CatalogItem::Type(_)
                        | CatalogItem::Func(_)
                        | CatalogItem::Secret(_)
                        | CatalogItem::Connection(_)
                        | CatalogItem::Log(_) => {}
                    }
                }
            }
        }
        id_bundle
    }

    /// Remove all pending peeks that were initiated by `conn_id`.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn remove_pending_peeks(&mut self, conn_id: u32) -> Vec<PendingPeek> {
        // The peek is present on some specific compute instance.
        // Allow dataflow to cancel any pending peeks.
        if let Some(uuids) = self.client_pending_peeks.remove(&conn_id) {
            let mut inverse: BTreeMap<ComputeInstanceId, BTreeSet<Uuid>> = Default::default();
            for (uuid, compute_instance) in &uuids {
                inverse.entry(*compute_instance).or_default().insert(*uuid);
            }
            for (compute_instance, uuids) in inverse {
                self.controller
                    .compute_mut(compute_instance)
                    .unwrap()
                    .cancel_peeks(&uuids)
                    .await
                    .unwrap();
            }

            uuids
                .iter()
                .filter_map(|(uuid, _)| self.pending_peeks.remove(uuid))
                .collect()
        } else {
            Vec::new()
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_command(&mut self, cmd: Command) {
        event!(Level::TRACE, cmd = format!("{:?}", cmd));
        match cmd {
            Command::Startup {
                session,
                create_user_if_not_exists,
                cancel_tx,
                tx,
            } => {
                if let Err(e) = self
                    .catalog
                    .create_temporary_schema(session.conn_id())
                    .await
                {
                    let _ = tx.send(Response {
                        result: Err(e.into()),
                        session,
                    });
                    return;
                }

                if self
                    .catalog
                    .for_session(&session)
                    .resolve_role(session.user())
                    .is_err()
                {
                    if !create_user_if_not_exists {
                        let _ = tx.send(Response {
                            result: Err(AdapterError::UnknownLoginRole(session.user().into())),
                            session,
                        });
                        return;
                    }
                    let plan = CreateRolePlan {
                        name: session.user().to_string(),
                    };
                    if let Err(err) = self.sequence_create_role(&session, plan).await {
                        let _ = tx.send(Response {
                            result: Err(err),
                            session,
                        });
                        return;
                    }
                }

                let mut messages = vec![];
                let catalog = self.catalog.for_session(&session);
                if catalog.active_database().is_none() {
                    messages.push(StartupMessage::UnknownSessionDatabase(
                        session.vars().database().into(),
                    ));
                }

                let secret_key = rand::thread_rng().gen();

                self.active_conns.insert(
                    session.conn_id(),
                    ConnMeta {
                        cancel_tx,
                        secret_key,
                    },
                );

                ClientTransmitter::new(tx, self.internal_cmd_tx.clone()).send(
                    Ok(StartupResponse {
                        messages,
                        secret_key,
                    }),
                    session,
                )
            }

            Command::Execute {
                portal_name,
                session,
                tx,
                span,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());

                let span = tracing::debug_span!(parent: &span, "message_command (execute)");
                self.handle_execute(portal_name, session, tx)
                    .instrument(span)
                    .await;
            }

            Command::Declare {
                name,
                stmt,
                param_types,
                mut session,
                tx,
            } => {
                let result = self.handle_declare(&mut session, name, stmt, param_types);
                let _ = tx.send(Response { result, session });
            }

            Command::Describe {
                name,
                stmt,
                param_types,
                mut session,
                tx,
            } => {
                let result = self.handle_describe(&mut session, name, stmt, param_types);
                let _ = tx.send(Response { result, session });
            }

            Command::CancelRequest {
                conn_id,
                secret_key,
            } => {
                self.handle_cancel(conn_id, secret_key).await;
            }

            Command::DumpCatalog { session, tx } => {
                // TODO(benesch): when we have RBAC, dumping the catalog should
                // require superuser permissions.

                let _ = tx.send(Response {
                    result: Ok(self.catalog.dump()),
                    session,
                });
            }

            Command::CopyRows {
                id,
                columns,
                rows,
                mut session,
                tx,
            } => {
                let result = self.sequence_copy_rows(&mut session, id, columns, rows);
                let _ = tx.send(Response { result, session });
            }

            Command::Terminate { mut session } => {
                self.handle_terminate(&mut session).await;
            }

            Command::StartTransaction {
                implicit,
                session,
                tx,
            } => {
                let now = self.now_datetime();
                let session = match implicit {
                    None => session.start_transaction(now, None, None),
                    Some(stmts) => session.start_transaction_implicit(now, stmts),
                };
                let _ = tx.send(Response {
                    result: Ok(()),
                    session,
                });
            }

            Command::Commit {
                action,
                session,
                tx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());
                self.sequence_end_transaction(tx, session, action).await;
            }

            Command::VerifyPreparedStatement {
                name,
                mut session,
                tx,
            } => {
                let result = self.verify_prepared_statement(&mut session, &name);
                let _ = tx.send(Response { result, session });
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_compute_instance_status(&mut self, event: ComputeInstanceEvent) {
        event!(Level::TRACE, event = format!("{:?}", event));
        self.catalog_transact(
            None,
            vec![catalog::Op::UpdateComputeInstanceStatus { event }],
            |_| Ok(()),
        )
        .await
        .expect("updating compute instance status cannot fail");
    }

    async fn handle_statement(
        &mut self,
        session: &mut Session,
        stmt: mz_sql::ast::Statement<Aug>,
        params: &mz_sql::plan::Params,
    ) -> Result<mz_sql::plan::Plan, AdapterError> {
        let pcx = session.pcx();
        let plan =
            mz_sql::plan::plan(Some(&pcx), &self.catalog.for_session(session), stmt, params)?;
        Ok(plan)
    }

    fn handle_declare(
        &self,
        session: &mut Session,
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        let desc = describe(&self.catalog, stmt.clone(), &param_types, session)?;
        let params = vec![];
        let result_formats = vec![mz_pgrepr::Format::Text; desc.arity()];
        session.set_portal(
            name,
            desc,
            Some(stmt),
            params,
            result_formats,
            self.catalog.transient_revision(),
        )?;
        Ok(())
    }

    fn handle_describe(
        &self,
        session: &mut Session,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        let desc = self.describe(session, stmt.clone(), param_types)?;
        session.set_prepared_statement(
            name,
            PreparedStatement::new(stmt, desc, self.catalog.transient_revision()),
        );
        Ok(())
    }

    fn describe(
        &self,
        session: &Session,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<StatementDesc, AdapterError> {
        if let Some(stmt) = stmt {
            describe(&self.catalog, stmt, &param_types, session)
        } else {
            Ok(StatementDesc::new(None))
        }
    }

    /// Verify a prepared statement is still valid.
    fn verify_prepared_statement(
        &self,
        session: &mut Session,
        name: &str,
    ) -> Result<(), AdapterError> {
        let ps = match session.get_prepared_statement_unverified(&name) {
            Some(ps) => ps,
            None => return Err(AdapterError::UnknownPreparedStatement(name.to_string())),
        };
        if let Some(revision) =
            self.verify_statement_revision(session, ps.sql(), ps.desc(), ps.catalog_revision)?
        {
            let ps = session
                .get_prepared_statement_mut_unverified(name)
                .expect("known to exist");
            ps.catalog_revision = revision;
        }

        Ok(())
    }

    /// Verify a portal is still valid.
    fn verify_portal(&self, session: &mut Session, name: &str) -> Result<(), AdapterError> {
        let portal = match session.get_portal_unverified(&name) {
            Some(portal) => portal,
            None => return Err(AdapterError::UnknownCursor(name.to_string())),
        };
        if let Some(revision) = self.verify_statement_revision(
            &session,
            portal.stmt.as_ref(),
            &portal.desc,
            portal.catalog_revision,
        )? {
            let portal = session
                .get_portal_unverified_mut(&name)
                .expect("known to exist");
            portal.catalog_revision = revision;
        }
        Ok(())
    }

    fn verify_statement_revision(
        &self,
        session: &Session,
        stmt: Option<&Statement<Raw>>,
        desc: &StatementDesc,
        catalog_revision: u64,
    ) -> Result<Option<u64>, AdapterError> {
        let current_revision = self.catalog.transient_revision();
        if catalog_revision != current_revision {
            let current_desc = self.describe(
                session,
                stmt.cloned(),
                desc.param_types.iter().map(|ty| Some(ty.clone())).collect(),
            )?;
            if &current_desc != desc {
                Err(AdapterError::ChangedPlan)
            } else {
                Ok(Some(current_revision))
            }
        } else {
            Ok(None)
        }
    }

    /// Handles an execute command.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn handle_execute(
        &mut self,
        portal_name: String,
        mut session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
    ) {
        if let Err(err) = self.verify_portal(&mut session, &portal_name) {
            return tx.send(Err(err), session);
        }

        let portal = session
            .get_portal_unverified(&portal_name)
            .expect("known to exist");

        let stmt = match &portal.stmt {
            Some(stmt) => stmt.clone(),
            None => return tx.send(Ok(ExecuteResponse::EmptyQuery), session),
        };
        let params = portal.parameters.clone();
        self.handle_execute_inner(stmt, params, session, tx).await
    }

    #[tracing::instrument(level = "trace", skip(self, tx, session))]
    async fn handle_execute_inner(
        &mut self,
        stmt: Statement<Raw>,
        params: Params,
        mut session: Session,
        tx: ClientTransmitter<ExecuteResponse>,
    ) {
        // Verify that this statement type can be executed in the current
        // transaction state.
        match session.transaction() {
            // By this point we should be in a running transaction.
            TransactionStatus::Default => unreachable!(),

            // Started is almost always safe (started means there's a single statement
            // being executed). Failed transactions have already been checked in pgwire for
            // a safe statement (COMMIT, ROLLBACK, etc.) and can also proceed.
            TransactionStatus::Started(_) | TransactionStatus::Failed(_) => {
                if let Statement::Declare(_) = stmt {
                    // Declare is an exception. Although it's not against any spec to execute
                    // it, it will always result in nothing happening, since all portals will be
                    // immediately closed. Users don't know this detail, so this error helps them
                    // understand what's going wrong. Postgres does this too.
                    return tx.send(
                        Err(AdapterError::OperationRequiresTransaction(
                            "DECLARE CURSOR".into(),
                        )),
                        session,
                    );
                }
            }

            // Implicit or explicit transactions.
            //
            // Implicit transactions happen when a multi-statement query is executed
            // (a "simple query"). However if a "BEGIN" appears somewhere in there,
            // then the existing implicit transaction will be upgraded to an explicit
            // transaction. Thus, we should not separate what implicit and explicit
            // transactions can do unless there's some additional checking to make sure
            // something disallowed in explicit transactions did not previously take place
            // in the implicit portion.
            TransactionStatus::InTransactionImplicit(_) | TransactionStatus::InTransaction(_) => {
                match stmt {
                    // Statements that are safe in a transaction. We still need to verify that we
                    // don't interleave reads and writes since we can't perform those serializably.
                    Statement::Close(_)
                    | Statement::Commit(_)
                    | Statement::Copy(_)
                    | Statement::Deallocate(_)
                    | Statement::Declare(_)
                    | Statement::Discard(_)
                    | Statement::Execute(_)
                    | Statement::Explain(_)
                    | Statement::Fetch(_)
                    | Statement::Prepare(_)
                    | Statement::Rollback(_)
                    | Statement::Select(_)
                    | Statement::SetTransaction(_)
                    | Statement::ShowColumns(_)
                    | Statement::ShowCreateIndex(_)
                    | Statement::ShowCreateSink(_)
                    | Statement::ShowCreateSource(_)
                    | Statement::ShowCreateTable(_)
                    | Statement::ShowCreateView(_)
                    | Statement::ShowCreateRecordedView(_)
                    | Statement::ShowCreateConnection(_)
                    | Statement::ShowDatabases(_)
                    | Statement::ShowSchemas(_)
                    | Statement::ShowIndexes(_)
                    | Statement::ShowObjects(_)
                    | Statement::ShowVariable(_)
                    | Statement::SetVariable(_)
                    | Statement::ResetVariable(_)
                    | Statement::StartTransaction(_)
                    | Statement::Tail(_)
                    | Statement::Raise(_) => {
                        // Always safe.
                    }

                    Statement::Insert(ref insert_statement)
                        if matches!(
                            insert_statement.source,
                            InsertSource::Query(Query {
                                body: SetExpr::Values(..),
                                ..
                            }) | InsertSource::DefaultValues
                        ) =>
                    {
                        // Inserting from default? values statements
                        // is always safe.
                    }

                    // Statements below must by run singly (in Started).
                    Statement::AlterIndex(_)
                    | Statement::AlterSecret(_)
                    | Statement::AlterObjectRename(_)
                    | Statement::CreateConnection(_)
                    | Statement::CreateDatabase(_)
                    | Statement::CreateIndex(_)
                    | Statement::CreateRole(_)
                    | Statement::CreateCluster(_)
                    | Statement::CreateClusterReplica(_)
                    | Statement::CreateSchema(_)
                    | Statement::CreateSecret(_)
                    | Statement::CreateSink(_)
                    | Statement::CreateSource(_)
                    | Statement::CreateTable(_)
                    | Statement::CreateType(_)
                    | Statement::CreateView(_)
                    | Statement::CreateViews(_)
                    | Statement::CreateRecordedView(_)
                    | Statement::Delete(_)
                    | Statement::DropDatabase(_)
                    | Statement::DropSchema(_)
                    | Statement::DropObjects(_)
                    | Statement::DropRoles(_)
                    | Statement::DropClusters(_)
                    | Statement::DropClusterReplicas(_)
                    | Statement::Insert(_)
                    | Statement::Update(_) => {
                        return tx.send(
                            Err(AdapterError::OperationProhibitsTransaction(
                                stmt.to_string(),
                            )),
                            session,
                        )
                    }
                }
            }
        }

        let catalog = self.catalog.for_session(&session);
        let original_stmt = stmt.clone();
        let (stmt, depends_on) = match mz_sql::names::resolve(&catalog, stmt) {
            Ok(resolved) => resolved,
            Err(e) => return tx.send(Err(e.into()), session),
        };
        let depends_on = depends_on.into_iter().collect();
        // N.B. The catalog can change during purification so we must validate that the dependencies still exist after
        // purification.  This should be done back on the main thread.
        // We do the validation:
        //   - In the handler for `Message::CreateSourceStatementReady`, before we handle the purified statement.
        // If we add special handling for more types of `Statement`s, we'll need to ensure similar verification
        // occurs.
        match stmt {
            // `CREATE SOURCE` statements must be purified off the main
            // coordinator thread of control.
            Statement::CreateSource(stmt) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = session.conn_id();
                let purify_fut = mz_sql::pure::purify_create_source(
                    Box::new(catalog.into_owned()),
                    self.now(),
                    stmt,
                    self.connection_context.clone(),
                );
                task::spawn(|| format!("purify:{conn_id}"), async move {
                    let result = purify_fut.await.map_err(|e| e.into());
                    internal_cmd_tx
                        .send(Message::CreateSourceStatementReady(
                            CreateSourceStatementReady {
                                session,
                                tx,
                                result,
                                params,
                                depends_on,
                                original_stmt,
                            },
                        ))
                        .expect("sending to internal_cmd_tx cannot fail");
                });
            }

            // All other statements are handled immediately.
            _ => match self.handle_statement(&mut session, stmt, &params).await {
                Ok(plan) => self.sequence_plan(tx, session, plan, depends_on).await,
                Err(e) => tx.send(Err(e), session),
            },
        }
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id`.
    async fn handle_cancel(&mut self, conn_id: ConnectionId, secret_key: u32) {
        if let Some(conn_meta) = self.active_conns.get(&conn_id) {
            // If the secret key specified by the client doesn't match the
            // actual secret key for the target connection, we treat this as a
            // rogue cancellation request and ignore it.
            if conn_meta.secret_key != secret_key {
                return;
            }

            // Cancel pending writes. There is at most one pending write per session.
            if let Some(idx) = self
                .pending_writes
                .iter()
                .position(|PendingWriteTxn { session, .. }| session.conn_id() == conn_id)
            {
                let PendingWriteTxn {
                    client_transmitter,
                    session,
                    ..
                } = self.pending_writes.remove(idx);
                let _ = client_transmitter.send(Ok(ExecuteResponse::Canceled), session);
            }

            // Cancel deferred writes. There is at most one deferred write per session.
            if let Some(idx) = self
                .write_lock_wait_group
                .iter()
                .position(|ready| matches!(ready, Deferred::Plan(ready) if ready.session.conn_id() == conn_id))
            {
                let ready = self.write_lock_wait_group.remove(idx).unwrap();
                if let Deferred::Plan(ready) = ready {
                    ready.tx.send(Ok(ExecuteResponse::Canceled), ready.session);
                }
            }

            // Inform the target session (if it asks) about the cancellation.
            let _ = conn_meta.cancel_tx.send(Canceled::Canceled);

            for PendingPeek {
                sender: rows_tx,
                conn_id: _,
            } in self.remove_pending_peeks(conn_id).await
            {
                rows_tx
                    .send(PeekResponse::Canceled)
                    .expect("Peek endpoint terminated prematurely");
            }
        }
    }

    /// Handle termination of a client session.
    ///
    /// This cleans up any state in the coordinator associated with the session.
    async fn handle_terminate(&mut self, session: &mut Session) {
        self.clear_transaction(session).await;

        self.drop_temp_items(&session).await;
        self.catalog
            .drop_temporary_schema(session.conn_id())
            .expect("unable to drop temporary schema");
        self.active_conns.remove(&session.conn_id());
        self.remove_pending_peeks(session.conn_id()).await;
    }

    /// Handle removing in-progress transaction state regardless of the end action
    /// of the transaction.
    async fn clear_transaction(
        &mut self,
        session: &mut Session,
    ) -> TransactionStatus<mz_repr::Timestamp> {
        let (drop_sinks, txn) = session.clear_transaction();
        self.drop_sinks(drop_sinks).await;

        // Release this transaction's compaction hold on collections.
        if let Some(txn_reads) = self.txn_reads.remove(&session.conn_id()) {
            self.release_read_hold(&txn_reads.read_holds).await;
        }
        txn
    }

    /// Removes all temporary items created by the specified connection, though
    /// not the temporary schema itself.
    async fn drop_temp_items(&mut self, session: &Session) {
        let ops = self.catalog.drop_temp_item_ops(session.conn_id());
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await
            .expect("unable to drop temporary items for conn_id");
    }

    async fn handle_sink_connection_ready(
        &mut self,
        id: GlobalId,
        oid: u32,
        connection: SinkConnection<()>,
        compute_instance: ComputeInstanceId,
        session: Option<&Session>,
    ) -> Result<(), AdapterError> {
        // Update catalog entry with sink connection.
        let entry = self.catalog.get_entry(&id);
        let name = entry.name().clone();
        let mut sink = match entry.item() {
            CatalogItem::Sink(sink) => sink.clone(),
            _ => unreachable!(),
        };
        sink.connection = catalog::SinkConnectionState::Ready(connection.clone());
        // We don't try to linearize the as of for the sink; we just pick the
        // least valid read timestamp. If users want linearizability across
        // Materialize and their sink, they'll need to reason about the
        // timestamps we emit anyway, so might as emit as much historical detail
        // as we possibly can.
        let id_bundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(&[sink.from]);
        let frontier = self.least_valid_read(&id_bundle);
        let as_of = SinkAsOf {
            frontier,
            strict: !sink.with_snapshot,
        };
        let ops = vec![
            catalog::Op::DropItem(id),
            catalog::Op::CreateItem {
                id,
                oid,
                name: name.clone(),
                item: CatalogItem::Sink(sink.clone()),
            },
        ];
        let df = self
            .catalog_transact(session, ops, |txn| {
                let mut builder = txn.dataflow_builder(compute_instance);
                let from_entry = builder.catalog.get_entry(&sink.from);
                let sink_description = mz_storage::types::sinks::SinkDesc {
                    from: sink.from,
                    from_desc: from_entry
                        .desc(
                            &builder
                                .catalog
                                .resolve_full_name(from_entry.name(), from_entry.conn_id()),
                        )
                        .unwrap()
                        .into_owned(),
                    connection: connection.clone(),
                    envelope: Some(sink.envelope),
                    as_of,
                };
                Ok(builder.build_sink_dataflow(name.to_string(), id, sink_description)?)
            })
            .await?;

        Ok(self.ship_dataflow(df, compute_instance).await)
    }

    #[allow(dead_code)]
    async fn create_storage_export(&mut self, id: GlobalId, sink: &Sink) {
        let storage_sink_from_entry = self.catalog.get_entry(&sink.from);
        let storage_sink_desc = mz_storage::types::sinks::SinkDesc {
            from: sink.from,
            from_desc: storage_sink_from_entry
                .desc(&self.catalog.resolve_full_name(
                    storage_sink_from_entry.name(),
                    storage_sink_from_entry.conn_id(),
                ))
                .unwrap()
                .into_owned(),
            connection: SinkConnection::Tail(TailSinkConnection {}),
            envelope: Some(sink.envelope),
            as_of: SinkAsOf {
                frontier: Antichain::new(),
                strict: false,
            },
        };

        // TODO(chae): This is where we'll create the export/sink in storaged
        let _ = self
            .controller
            .storage_mut()
            .create_exports(vec![(
                id,
                ExportDescription {
                    sink: storage_sink_desc,
                    remote_addr: None,
                },
            )])
            .await
            .unwrap();
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn sequence_plan(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: Plan,
        depends_on: Vec<GlobalId>,
    ) {
        event!(Level::TRACE, plan = format!("{:?}", plan));
        match plan {
            Plan::CreateConnection(plan) => {
                tx.send(
                    self.sequence_create_connection(&session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateDatabase(plan) => {
                tx.send(self.sequence_create_database(&session, plan).await, session);
            }
            Plan::CreateSchema(plan) => {
                tx.send(self.sequence_create_schema(&session, plan).await, session);
            }
            Plan::CreateRole(plan) => {
                tx.send(self.sequence_create_role(&session, plan).await, session);
            }
            Plan::CreateComputeInstance(plan) => {
                tx.send(
                    self.sequence_create_compute_instance(&session, plan).await,
                    session,
                );
            }
            Plan::CreateComputeInstanceReplica(plan) => {
                tx.send(
                    self.sequence_create_compute_instance_replica(&session, plan)
                        .await,
                    session,
                );
            }
            Plan::CreateTable(plan) => {
                tx.send(
                    self.sequence_create_table(&session, plan, depends_on).await,
                    session,
                );
            }
            Plan::CreateSecret(plan) => {
                tx.send(self.sequence_create_secret(&session, plan).await, session);
            }
            Plan::CreateSource(_) => unreachable!("handled separately"),
            Plan::CreateSink(plan) => {
                self.sequence_create_sink(session, plan, depends_on, tx)
                    .await;
            }
            Plan::CreateView(plan) => {
                tx.send(
                    self.sequence_create_view(&session, plan, depends_on).await,
                    session,
                );
            }
            Plan::CreateViews(plan) => {
                tx.send(
                    self.sequence_create_views(&mut session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateRecordedView(plan) => {
                tx.send(
                    self.sequence_create_recorded_view(&session, plan, depends_on)
                        .await,
                    session,
                );
            }
            Plan::CreateIndex(plan) => {
                tx.send(
                    self.sequence_create_index(&session, plan, depends_on).await,
                    session,
                );
            }
            Plan::CreateType(plan) => {
                tx.send(
                    self.sequence_create_type(&session, plan, depends_on).await,
                    session,
                );
            }
            Plan::DropDatabase(plan) => {
                tx.send(self.sequence_drop_database(&session, plan).await, session);
            }
            Plan::DropSchema(plan) => {
                tx.send(self.sequence_drop_schema(&session, plan).await, session);
            }
            Plan::DropRoles(plan) => {
                tx.send(self.sequence_drop_roles(&session, plan).await, session);
            }
            Plan::DropComputeInstances(plan) => {
                tx.send(
                    self.sequence_drop_compute_instances(&session, plan).await,
                    session,
                );
            }
            Plan::DropComputeInstanceReplica(plan) => {
                tx.send(
                    self.sequence_drop_compute_instance_replica(&session, plan)
                        .await,
                    session,
                );
            }
            Plan::DropItems(plan) => {
                tx.send(self.sequence_drop_items(&session, plan).await, session);
            }
            Plan::EmptyQuery => {
                tx.send(Ok(ExecuteResponse::EmptyQuery), session);
            }
            Plan::ShowAllVariables => {
                tx.send(self.sequence_show_all_variables(&session), session);
            }
            Plan::ShowVariable(plan) => {
                tx.send(self.sequence_show_variable(&session, plan), session);
            }
            Plan::SetVariable(plan) => {
                tx.send(self.sequence_set_variable(&mut session, plan), session);
            }
            Plan::ResetVariable(plan) => {
                tx.send(self.sequence_reset_variable(&mut session, plan), session);
            }
            Plan::StartTransaction(plan) => {
                let duplicated =
                    matches!(session.transaction(), TransactionStatus::InTransaction(_));
                let session = session.start_transaction(
                    self.now_datetime(),
                    plan.access,
                    plan.isolation_level,
                );
                tx.send(
                    Ok(ExecuteResponse::StartedTransaction { duplicated }),
                    session,
                )
            }

            Plan::CommitTransaction | Plan::AbortTransaction => {
                let action = match plan {
                    Plan::CommitTransaction => EndTransactionAction::Commit,
                    Plan::AbortTransaction => EndTransactionAction::Rollback,
                    _ => unreachable!(),
                };
                self.sequence_end_transaction(tx, session, action).await;
            }
            Plan::Peek(plan) => {
                tx.send(self.sequence_peek(&mut session, plan).await, session);
            }
            Plan::Tail(plan) => {
                tx.send(
                    self.sequence_tail(&mut session, plan, depends_on).await,
                    session,
                );
            }
            Plan::SendRows(plan) => {
                tx.send(Ok(send_immediate_rows(plan.rows)), session);
            }

            Plan::CopyFrom(plan) => {
                tx.send(
                    Ok(ExecuteResponse::CopyFrom {
                        id: plan.id,
                        columns: plan.columns,
                        params: plan.params,
                    }),
                    session,
                );
            }
            Plan::Explain(plan) => {
                tx.send(self.sequence_explain(&session, plan), session);
            }
            Plan::SendDiffs(plan) => {
                tx.send(self.sequence_send_diffs(&mut session, plan), session);
            }
            Plan::Insert(plan) => {
                self.sequence_insert(tx, session, plan).await;
            }
            Plan::ReadThenWrite(plan) => {
                self.sequence_read_then_write(tx, session, plan).await;
            }
            Plan::AlterNoop(plan) => {
                tx.send(
                    Ok(ExecuteResponse::AlteredObject(plan.object_type)),
                    session,
                );
            }
            Plan::AlterItemRename(plan) => {
                tx.send(
                    self.sequence_alter_item_rename(&session, plan).await,
                    session,
                );
            }
            Plan::AlterIndexSetOptions(plan) => {
                tx.send(self.sequence_alter_index_set_options(plan).await, session);
            }
            Plan::AlterIndexResetOptions(plan) => {
                tx.send(self.sequence_alter_index_reset_options(plan).await, session);
            }
            Plan::AlterSecret(plan) => {
                tx.send(self.sequence_alter_secret(&session, plan).await, session);
            }
            Plan::DiscardTemp => {
                self.drop_temp_items(&session).await;
                tx.send(Ok(ExecuteResponse::DiscardedTemp), session);
            }
            Plan::DiscardAll => {
                let ret = if let TransactionStatus::Started(_) = session.transaction() {
                    self.drop_temp_items(&session).await;
                    let drop_sinks = session.reset();
                    self.drop_sinks(drop_sinks).await;
                    Ok(ExecuteResponse::DiscardedAll)
                } else {
                    Err(AdapterError::OperationProhibitsTransaction(
                        "DISCARD ALL".into(),
                    ))
                };
                tx.send(ret, session);
            }
            Plan::Declare(plan) => {
                let param_types = vec![];
                let res = self
                    .handle_declare(&mut session, plan.name, plan.stmt, param_types)
                    .map(|()| ExecuteResponse::DeclaredCursor);
                tx.send(res, session);
            }
            Plan::Fetch(FetchPlan {
                name,
                count,
                timeout,
            }) => {
                tx.send(
                    Ok(ExecuteResponse::Fetch {
                        name,
                        count,
                        timeout,
                    }),
                    session,
                );
            }
            Plan::Close(plan) => {
                if session.remove_portal(&plan.name) {
                    tx.send(Ok(ExecuteResponse::ClosedCursor), session);
                } else {
                    tx.send(Err(AdapterError::UnknownCursor(plan.name)), session);
                }
            }
            Plan::Prepare(plan) => {
                if session
                    .get_prepared_statement_unverified(&plan.name)
                    .is_some()
                {
                    tx.send(
                        Err(AdapterError::PreparedStatementExists(plan.name)),
                        session,
                    );
                } else {
                    session.set_prepared_statement(
                        plan.name,
                        PreparedStatement::new(
                            Some(plan.stmt),
                            plan.desc,
                            self.catalog.transient_revision(),
                        ),
                    );
                    tx.send(Ok(ExecuteResponse::Prepare), session);
                }
            }
            Plan::Execute(plan) => {
                match self.sequence_execute(&mut session, plan) {
                    Ok(portal_name) => {
                        self.internal_cmd_tx
                            .send(Message::Command(Command::Execute {
                                portal_name,
                                session,
                                tx: tx.take(),
                                span: tracing::Span::none(),
                            }))
                            .expect("sending to internal_cmd_tx cannot fail");
                    }
                    Err(err) => tx.send(Err(err), session),
                };
            }
            Plan::Deallocate(plan) => match plan.name {
                Some(name) => {
                    if session.remove_prepared_statement(&name) {
                        tx.send(Ok(ExecuteResponse::Deallocate { all: false }), session);
                    } else {
                        tx.send(Err(AdapterError::UnknownPreparedStatement(name)), session);
                    }
                }
                None => {
                    session.remove_all_prepared_statements();
                    tx.send(Ok(ExecuteResponse::Deallocate { all: true }), session);
                }
            },
            Plan::Raise(RaisePlan { severity }) => {
                tx.send(Ok(ExecuteResponse::Raise { severity }), session);
            }
        }
    }

    // Returns the name of the portal to execute.
    fn sequence_execute(
        &mut self,
        session: &mut Session,
        plan: ExecutePlan,
    ) -> Result<String, AdapterError> {
        // Verify the stmt is still valid.
        self.verify_prepared_statement(session, &plan.name)?;
        let ps = session
            .get_prepared_statement_unverified(&plan.name)
            .expect("known to exist");
        let sql = ps.sql().cloned();
        let desc = ps.desc().clone();
        let revision = ps.catalog_revision;
        session.create_new_portal(sql, desc, plan.params, Vec::new(), revision)
    }

    async fn sequence_create_connection(
        &mut self,
        session: &Session,
        plan: CreateConnectionPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let connection_oid = self.catalog.allocate_oid().await?;
        let connection_gid = self.catalog.allocate_user_id().await?;
        let ops = vec![catalog::Op::CreateItem {
            id: connection_gid,
            oid: connection_oid,
            name: plan.name.clone(),
            item: CatalogItem::Connection(Connection {
                create_sql: plan.connection.create_sql,
                connection: plan.connection.connection,
                depends_on,
            }),
        }];
        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
            Ok(_) => Ok(ExecuteResponse::CreatedConnection { existed: false }),
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedConnection { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_database(
        &mut self,
        session: &Session,
        plan: CreateDatabasePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let db_oid = self.catalog.allocate_oid().await?;
        let schema_oid = self.catalog.allocate_oid().await?;
        let ops = vec![catalog::Op::CreateDatabase {
            name: plan.name.clone(),
            oid: db_oid,
            public_schema_oid: schema_oid,
        }];
        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
            Ok(_) => Ok(ExecuteResponse::CreatedDatabase { existed: false }),
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::DatabaseAlreadyExists(_),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedDatabase { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_schema(
        &mut self,
        session: &Session,
        plan: CreateSchemaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let oid = self.catalog.allocate_oid().await?;
        let op = catalog::Op::CreateSchema {
            database_id: plan.database_spec,
            schema_name: plan.schema_name,
            oid,
        };
        match self
            .catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await
        {
            Ok(_) => Ok(ExecuteResponse::CreatedSchema { existed: false }),
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::SchemaAlreadyExists(_),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedSchema { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_role(
        &mut self,
        session: &Session,
        plan: CreateRolePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let oid = self.catalog.allocate_oid().await?;
        let op = catalog::Op::CreateRole {
            name: plan.name,
            oid,
        };
        self.catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await
            .map(|_| ExecuteResponse::CreatedRole)
    }

    async fn sequence_create_compute_instance(
        &mut self,
        session: &Session,
        CreateComputeInstancePlan {
            name,
            config: compute_instance_config,
            replicas,
        }: CreateComputeInstancePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_compute_instance");
        let introspection_sources = if compute_instance_config.is_some() {
            self.catalog.allocate_introspection_source_indexes().await
        } else {
            Vec::new()
        };
        let mut ops = vec![catalog::Op::CreateComputeInstance {
            name: name.clone(),
            config: compute_instance_config.clone(),
            introspection_sources,
        }];

        // This vector collects introspection sources of all replicas of this compute instance
        let mut introspection_collections = Vec::new();

        for (replica_name, replica_config) in replicas {
            // These are the persisted, per replica persisted logs
            let persisted_logs = if compute_instance_config.is_some() {
                self.catalog
                    .allocate_persisted_introspection_source_indexes()
                    .await
            } else {
                ConcreteComputeInstanceReplicaLogging::Concrete(Vec::new())
            };

            introspection_collections.extend(
                persisted_logs
                    .get_logs()
                    .iter()
                    .map(|(variant, id)| (*id, variant.desc().into())),
            );

            let config = self
                .catalog
                .concretize_replica_config(replica_config, persisted_logs)?;

            ops.push(catalog::Op::CreateComputeInstanceReplica {
                name: replica_name,
                config,
                on_cluster_name: name.clone(),
            });
        }

        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;

        let introspection_collection_ids: Vec<GlobalId> = introspection_collections
            .iter()
            .map(|(id, _)| *id)
            .collect();

        self.controller
            .storage_mut()
            .create_collections(introspection_collections)
            .await
            .unwrap();

        let instance = self
            .catalog
            .resolve_compute_instance(&name)
            .expect("compute instance must exist after creation");
        self.controller
            .create_instance(instance.id, instance.logging.clone())
            .await;
        for (replica_id, replica) in instance.replicas_by_id.clone() {
            self.controller
                .add_replica_to_instance(instance.id, replica_id, replica.config)
                .await
                .unwrap();
        }

        if !introspection_collection_ids.is_empty() {
            self.initialize_storage_read_policies(
                introspection_collection_ids,
                DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
            )
            .await;
        }

        Ok(ExecuteResponse::CreatedComputeInstance { existed: false })
    }

    async fn sequence_create_compute_instance_replica(
        &mut self,
        session: &Session,
        CreateComputeInstanceReplicaPlan {
            name,
            of_cluster,
            config,
        }: CreateComputeInstanceReplicaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let instance = self.catalog.resolve_compute_instance(&of_cluster)?;

        let persisted_logs = if instance.logging.is_some() {
            self.catalog
                .allocate_persisted_introspection_source_indexes()
                .await
        } else {
            ConcreteComputeInstanceReplicaLogging::Concrete(Vec::new())
        };

        let persisted_log_ids = persisted_logs.get_log_ids();
        let persisted_logs_collections = persisted_logs
            .get_logs()
            .iter()
            .map(|(variant, id)| (*id, variant.desc().into()))
            .collect();

        let config = self
            .catalog
            .concretize_replica_config(config, persisted_logs)?;

        let op = catalog::Op::CreateComputeInstanceReplica {
            name: name.clone(),
            config,
            on_cluster_name: of_cluster.clone(),
        };

        self.catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await?;

        self.controller
            .storage_mut()
            .create_collections(persisted_logs_collections)
            .await
            .unwrap();

        let instance = self.catalog.resolve_compute_instance(&of_cluster)?;
        let instance_id = instance.id;
        let replica_id = instance.replica_id_by_name[&name];
        let replica = instance.replicas_by_id[&replica_id].clone();

        if instance.logging.is_some() {
            self.initialize_storage_read_policies(
                persisted_log_ids,
                DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
            )
            .await;
        }

        self.controller
            .add_replica_to_instance(instance_id, replica_id, replica.config.clone())
            .await
            .unwrap();

        Ok(ExecuteResponse::CreatedComputeInstanceReplica { existed: false })
    }

    async fn sequence_create_secret(
        &mut self,
        session: &Session,
        plan: CreateSecretPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateSecretPlan {
            name,
            mut secret,
            full_name,
            if_not_exists,
        } = plan;

        let payload = self.extract_secret(session, &mut secret.secret_as)?;

        let id = self.catalog.allocate_user_id().await?;
        let oid = self.catalog.allocate_oid().await?;
        let secret = catalog::Secret {
            create_sql: format!("CREATE SECRET {} AS '********'", full_name),
        };

        self.secrets_controller.ensure(id, &payload).await?;

        let ops = vec![catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Secret(secret.clone()),
        }];

        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
            Ok(()) => Ok(ExecuteResponse::CreatedSecret { existed: false }),
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedSecret { existed: true }),
            Err(err) => {
                if let Err(e) = self.secrets_controller.delete(id).await {
                    warn!(
                        "Dropping newly created secrets has encountered an error: {}",
                        e
                    );
                }
                Err(err)
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn sequence_create_table(
        &mut self,
        session: &Session,
        plan: CreateTablePlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateTablePlan {
            name,
            table,
            if_not_exists,
        } = plan;

        let conn_id = if table.temporary {
            Some(session.conn_id())
        } else {
            None
        };
        let table_id = self.catalog.allocate_user_id().await?;
        let table = catalog::Table {
            create_sql: table.create_sql,
            desc: table.desc,
            defaults: table.defaults,
            conn_id,
            depends_on,
        };
        let table_oid = self.catalog.allocate_oid().await?;
        let ops = vec![catalog::Op::CreateItem {
            id: table_id,
            oid: table_oid,
            name,
            item: CatalogItem::Table(table.clone()),
        }];
        match self.catalog_transact(Some(session), ops, |_| Ok(())).await {
            Ok(()) => {
                // Determine the initial validity for the table.
                let since_ts = self.get_local_write_ts().await;

                let collection_desc = table.desc.clone().into();
                self.controller
                    .storage_mut()
                    .create_collections(vec![(table_id, collection_desc)])
                    .await
                    .unwrap();

                let policy = ReadPolicy::ValidFrom(Antichain::from_elem(since_ts));
                self.controller
                    .storage_mut()
                    .set_read_policy(vec![(table_id, policy)])
                    .await
                    .unwrap();

                self.initialize_storage_read_policies(
                    vec![table_id],
                    DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
                )
                .await;
                Ok(ExecuteResponse::CreatedTable { existed: false })
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedTable { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_source(
        &mut self,
        session: &mut Session,
        plan: CreateSourcePlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut ops = vec![];
        let source_id = self.catalog.allocate_user_id().await?;
        let source_oid = self.catalog.allocate_oid().await?;
        let source = catalog::Source {
            create_sql: plan.source.create_sql,
            source_desc: plan.source.source_desc,
            desc: plan.source.desc,
            timeline: plan.timeline,
            depends_on,
            remote_addr: plan.remote,
        };
        ops.push(catalog::Op::CreateItem {
            id: source_id,
            oid: source_oid,
            name: plan.name.clone(),
            item: CatalogItem::Source(source.clone()),
        });
        match self
            .catalog_transact(Some(session), ops, move |_| Ok(()))
            .await
        {
            Ok(()) => {
                // Do everything to instantiate the source at the coordinator and
                // inform the timestamper and dataflow workers of its existence before
                // shipping any dataflows that depend on its existence.

                let mut ingestion = IngestionDescription {
                    desc: source.source_desc.clone(),
                    source_imports: BTreeMap::new(),
                    storage_metadata: (),
                    typ: source.desc.typ().clone(),
                };

                for id in self.catalog.state().get_entry(&source_id).uses() {
                    if self.catalog.state().get_entry(id).source().is_some() {
                        ingestion.source_imports.insert(*id, ());
                    }
                }

                let source_status_collection_id = self.catalog.resolve_builtin_storage_collection(
                    &crate::catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
                );

                self.controller
                    .storage_mut()
                    .create_collections(vec![(
                        source_id,
                        CollectionDescription {
                            desc: source.desc.clone(),
                            ingestion: Some(ingestion),
                            remote_addr: source.remote_addr,
                            since: None,
                            status_collection_id: Some(source_status_collection_id),
                        },
                    )])
                    .await
                    .unwrap();

                self.initialize_storage_read_policies(
                    vec![source_id],
                    DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
                )
                .await;
                Ok(ExecuteResponse::CreatedSource { existed: false })
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedSource { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_sink(
        &mut self,
        session: Session,
        plan: CreateSinkPlan,
        depends_on: Vec<GlobalId>,
        tx: ClientTransmitter<ExecuteResponse>,
    ) {
        let CreateSinkPlan {
            name,
            sink,
            with_snapshot,
            if_not_exists,
        } = plan;

        // The dataflow must (eventually) be built on a specific compute instance.
        // Use this in `catalog_transact` and stash for eventual sink construction.
        let compute_instance = sink.compute_instance;

        // First try to allocate an ID and an OID. If either fails, we're done.
        let id = match self.catalog.allocate_user_id().await {
            Ok(id) => id,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };
        let oid = match self.catalog.allocate_oid().await {
            Ok(id) => id,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };

        // Then try to create a placeholder catalog item with an unknown
        // connection. If that fails, we're done, though if the client specified
        // `if_not_exists` we'll tell the client we succeeded.
        //
        // This placeholder catalog item reserves the name while we create
        // the sink connection, which could take an arbitrarily long time.
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Sink(catalog::Sink {
                create_sql: sink.create_sql,
                from: sink.from,
                connection: catalog::SinkConnectionState::Pending(sink.connection_builder.clone()),
                envelope: sink.envelope,
                with_snapshot,
                depends_on,
                compute_instance,
            }),
        };

        let transact_result = self
            .catalog_transact(
                Some(&session),
                vec![op],
                |txn| -> Result<(), AdapterError> {
                    let from_entry = txn.catalog.get_entry(&sink.from);
                    // Insert a dummy dataflow to trigger validation before we try to actually create
                    // the external sink resources (e.g. Kafka Topics)
                    txn.dataflow_builder(sink.compute_instance)
                        .build_sink_dataflow(
                            "dummy".into(),
                            id,
                            mz_storage::types::sinks::SinkDesc {
                                from: sink.from,
                                from_desc: from_entry
                                    .desc(
                                        &txn.catalog.resolve_full_name(
                                            from_entry.name(),
                                            from_entry.conn_id(),
                                        ),
                                    )
                                    .unwrap()
                                    .into_owned(),
                                connection: SinkConnection::Tail(TailSinkConnection {}),
                                envelope: Some(sink.envelope),
                                as_of: SinkAsOf {
                                    frontier: Antichain::new(),
                                    strict: false,
                                },
                            },
                        )
                        .map(|_ok| ())
                },
            )
            .await;

        match transact_result {
            Ok(()) => {}
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => {
                tx.send(Ok(ExecuteResponse::CreatedSink { existed: true }), session);
                return;
            }
            Err(e) => {
                tx.send(Err(e), session);
                return;
            }
        }

        // Now we're ready to create the sink connection. Arrange to notify the
        // main coordinator thread when the future completes.
        let connection_builder = sink.connection_builder;
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let connection_context = self.connection_context.clone();
        task::spawn(
            || format!("sink_connection_ready:{}", sink.from),
            async move {
                internal_cmd_tx
                    .send(Message::SinkConnectionReady(SinkConnectionReady {
                        session,
                        tx,
                        id,
                        oid,
                        result: sink_connection::build(connection_builder, id, connection_context)
                            .await,
                        compute_instance,
                    }))
                    .expect("sending to internal_cmd_tx cannot fail");
            },
        );
    }

    async fn generate_view_ops(
        &mut self,
        session: &Session,
        name: QualifiedObjectName,
        view: View,
        replace: Option<GlobalId>,
        materialize: bool,
        depends_on: Vec<GlobalId>,
    ) -> Result<(Vec<catalog::Op>, Option<(GlobalId, ComputeInstanceId)>), AdapterError> {
        self.validate_timeline(view.expr.depends_on())?;

        let mut ops = vec![];

        if let Some(id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[id]));
        }
        let view_id = self.catalog.allocate_user_id().await?;
        let view_oid = self.catalog.allocate_oid().await?;
        let optimized_expr = self.view_optimizer.optimize(view.expr)?;
        let desc = RelationDesc::new(optimized_expr.typ(), view.column_names);
        let view = catalog::View {
            create_sql: view.create_sql,
            optimized_expr,
            desc,
            conn_id: if view.temporary {
                Some(session.conn_id())
            } else {
                None
            },
            depends_on,
        };
        ops.push(catalog::Op::CreateItem {
            id: view_id,
            oid: view_oid,
            name: name.clone(),
            item: CatalogItem::View(view.clone()),
        });
        let index_id = if materialize {
            let compute_instance = self
                .catalog
                .resolve_compute_instance(session.vars().cluster())?
                .id;
            let mut index_name = name.clone();
            index_name.item += "_primary_idx";
            index_name = self
                .catalog
                .for_session(session)
                .find_available_name(index_name);
            let index_id = self.catalog.allocate_user_id().await?;
            let full_name = self
                .catalog
                .resolve_full_name(&name, Some(session.conn_id()));
            let index = auto_generate_primary_idx(
                index_name.item.clone(),
                compute_instance,
                full_name,
                view_id,
                &view.desc,
                view.conn_id,
                vec![view_id],
            );
            let index_oid = self.catalog.allocate_oid().await?;
            ops.push(catalog::Op::CreateItem {
                id: index_id,
                oid: index_oid,
                name: index_name,
                item: CatalogItem::Index(index),
            });
            Some((index_id, compute_instance))
        } else {
            None
        };

        Ok((ops, index_id))
    }

    async fn sequence_create_view(
        &mut self,
        session: &Session,
        plan: CreateViewPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let if_not_exists = plan.if_not_exists;
        let (ops, index) = self
            .generate_view_ops(
                session,
                plan.name,
                plan.view.clone(),
                plan.replace,
                plan.materialize,
                depends_on,
            )
            .await?;
        match self
            .catalog_transact(Some(session), ops, |txn| {
                if let Some((index_id, compute_instance)) = index {
                    let mut builder = txn.dataflow_builder(compute_instance);
                    Ok(Some((
                        builder.build_index_dataflow(index_id)?,
                        compute_instance,
                    )))
                } else {
                    Ok(None)
                }
            })
            .await
        {
            Ok(df) => {
                if let Some((df, compute_instance)) = df {
                    self.ship_dataflow(df, compute_instance).await;
                }
                Ok(ExecuteResponse::CreatedView { existed: false })
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedView { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_views(
        &mut self,
        session: &mut Session,
        plan: CreateViewsPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut ops = vec![];
        let mut indexes = vec![];

        for (name, view) in plan.views {
            let (mut view_ops, index) = self
                .generate_view_ops(
                    session,
                    name,
                    view,
                    None,
                    plan.materialize,
                    depends_on.clone(),
                )
                .await?;
            ops.append(&mut view_ops);
            indexes.extend(index);
        }
        match self
            .catalog_transact(Some(session), ops, |txn| {
                let mut dfs = HashMap::new();
                for (index_id, compute_instance) in indexes {
                    let mut builder = txn.dataflow_builder(compute_instance);
                    let df = builder.build_index_dataflow(index_id)?;
                    dfs.entry(compute_instance)
                        .or_insert_with(Vec::new)
                        .push(df);
                }
                Ok(dfs)
            })
            .await
        {
            Ok(dfs) => {
                for (compute_instance, dfs) in dfs {
                    if !dfs.is_empty() {
                        self.ship_dataflows(dfs, compute_instance).await;
                    }
                }
                Ok(ExecuteResponse::CreatedView { existed: false })
            }
            Err(_) if plan.if_not_exists => Ok(ExecuteResponse::CreatedView { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_recorded_view(
        &mut self,
        session: &Session,
        plan: CreateRecordedViewPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateRecordedViewPlan {
            name,
            recorded_view:
                RecordedView {
                    create_sql,
                    expr: view_expr,
                    column_names,
                    compute_instance,
                },
            replace,
            if_not_exists,
        } = plan;

        self.validate_timeline(depends_on.clone())?;

        // Recorded views are not allowed to depend on log sources, as replicas
        // are not producing the same definite collection for these.
        // TODO(teskje): Remove this check once arrangement-based log sources
        // are replaced with persist-based ones.
        let log_names = depends_on
            .iter()
            .flat_map(|id| self.catalog.log_dependencies(*id))
            .map(|id| self.catalog.get_entry(&id).name().item.clone())
            .collect::<Vec<_>>();
        if !log_names.is_empty() {
            return Err(AdapterError::InvalidLogDependency {
                object_type: "recorded view".into(),
                log_names,
            });
        }

        // Allocate IDs for the recorded view in the catalog.
        let id = self.catalog.allocate_user_id().await?;
        let oid = self.catalog.allocate_oid().await?;
        // Allocate a unique ID that can be used by the dataflow builder to
        // connect the view dataflow to the storage sink.
        let internal_view_id = self.allocate_transient_id()?;

        let optimized_expr = self.view_optimizer.optimize(view_expr)?;
        let desc = RelationDesc::new(optimized_expr.typ(), column_names);

        // Pick the least valid read timestamp as the as-of for the view
        // dataflow. This makes the recorded view include the maximum possible
        // amount of historical detail.
        let id_bundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(&depends_on);
        let as_of = self.least_valid_read(&id_bundle);

        let mut ops = Vec::new();
        if let Some(drop_id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[drop_id]));
        }
        ops.push(catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::RecordedView(catalog::RecordedView {
                create_sql,
                optimized_expr,
                desc: desc.clone(),
                depends_on,
                compute_instance,
            }),
        });

        match self
            .catalog_transact(Some(session), ops, |txn| {
                // Create a dataflow that materializes the view query and sinks
                // it to storage.
                let df = txn
                    .dataflow_builder(compute_instance)
                    .build_recorded_view_dataflow(id, as_of.clone(), internal_view_id)?;
                Ok(df)
            })
            .await
        {
            Ok(df) => {
                // Announce the creation of the recorded view source.
                self.controller
                    .storage_mut()
                    .create_collections(vec![(
                        id,
                        CollectionDescription {
                            desc,
                            ingestion: None,
                            remote_addr: None,
                            since: Some(as_of),
                            status_collection_id: None,
                        },
                    )])
                    .await
                    .unwrap();

                self.initialize_storage_read_policies(
                    vec![id],
                    DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
                )
                .await;

                self.ship_dataflow(df, compute_instance).await;

                Ok(ExecuteResponse::CreatedRecordedView { existed: false })
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedRecordedView { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_index(
        &mut self,
        session: &Session,
        plan: CreateIndexPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateIndexPlan {
            name,
            index,
            options,
            if_not_exists,
        } = plan;

        // An index must be created on a specific compute instance.
        let compute_instance = index.compute_instance;

        let id = self.catalog.allocate_user_id().await?;
        let index = catalog::Index {
            create_sql: index.create_sql,
            keys: index.keys,
            on: index.on,
            conn_id: None,
            depends_on,
            compute_instance,
        };
        let oid = self.catalog.allocate_oid().await?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Index(index),
        };
        match self
            .catalog_transact(Some(session), vec![op], |txn| {
                let mut builder = txn.dataflow_builder(compute_instance);
                let df = builder.build_index_dataflow(id)?;
                Ok(df)
            })
            .await
        {
            Ok(df) => {
                self.ship_dataflow(df, compute_instance).await;
                self.set_index_options(id, options)
                    .await
                    .expect("index enabled");
                Ok(ExecuteResponse::CreatedIndex { existed: false })
            }
            Err(AdapterError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedIndex { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_type(
        &mut self,
        session: &Session,
        plan: CreateTypePlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let typ = catalog::Type {
            create_sql: plan.typ.create_sql,
            details: CatalogTypeDetails {
                array_id: None,
                typ: plan.typ.inner,
            },
            depends_on,
        };
        let id = self.catalog.allocate_user_id().await?;
        let oid = self.catalog.allocate_oid().await?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: plan.name,
            item: CatalogItem::Type(typ),
        };
        match self
            .catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await
        {
            Ok(()) => Ok(ExecuteResponse::CreatedType),
            Err(err) => Err(err),
        }
    }

    async fn sequence_drop_database(
        &mut self,
        session: &Session,
        plan: DropDatabasePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = self.catalog.drop_database_ops(plan.id);
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        Ok(ExecuteResponse::DroppedDatabase)
    }

    async fn sequence_drop_schema(
        &mut self,
        session: &Session,
        plan: DropSchemaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = self.catalog.drop_schema_ops(plan.id);
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        Ok(ExecuteResponse::DroppedSchema)
    }

    async fn sequence_drop_roles(
        &mut self,
        session: &Session,
        plan: DropRolesPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = plan
            .names
            .into_iter()
            .map(|name| catalog::Op::DropRole { name })
            .collect();
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        Ok(ExecuteResponse::DroppedRole)
    }

    async fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        replica_config: ConcreteComputeInstanceReplicaConfig,
    ) -> Result<(), anyhow::Error> {
        if let Some(Some(metadata)) = self.transient_replica_metadata.insert(replica_id, None) {
            let retraction = self
                .catalog
                .state()
                .pack_replica_heartbeat_update(replica_id, metadata, -1);
            self.send_builtin_table_updates(vec![retraction]).await;
        }
        self.controller
            .drop_replica(instance_id, replica_id, replica_config)
            .await
    }

    async fn sequence_drop_compute_instances(
        &mut self,
        session: &Session,
        plan: DropComputeInstancesPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut ops = Vec::new();
        let mut instance_replica_drop_sets = Vec::with_capacity(plan.names.len());
        for name in plan.names {
            let instance = self.catalog.resolve_compute_instance(&name)?;
            instance_replica_drop_sets.push((instance.id, instance.replicas_by_id.clone()));
            for replica_name in instance.replica_id_by_name.keys() {
                ops.push(catalog::Op::DropComputeInstanceReplica {
                    name: replica_name.to_string(),
                    compute_id: instance.id,
                });
            }
            let ids_to_drop: Vec<GlobalId> = instance.exports().iter().cloned().collect();
            ops.extend(self.catalog.drop_items_ops(&ids_to_drop));
            ops.push(catalog::Op::DropComputeInstance { name });
        }

        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        for (instance_id, replicas) in instance_replica_drop_sets {
            for (replica_id, replica) in replicas {
                self.drop_replica(instance_id, replica_id, replica.config)
                    .await
                    .unwrap();
            }
            self.controller.drop_instance(instance_id).await.unwrap();
        }

        Ok(ExecuteResponse::DroppedComputeInstance)
    }

    async fn sequence_drop_compute_instance_replica(
        &mut self,
        session: &Session,
        DropComputeInstanceReplicaPlan { names }: DropComputeInstanceReplicaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        if names.is_empty() {
            return Ok(ExecuteResponse::DroppedComputeInstanceReplicas);
        }
        let mut ops = Vec::with_capacity(names.len());
        let mut replicas_to_drop = Vec::with_capacity(names.len());
        for (instance_name, replica_name) in names {
            let instance = self.catalog.resolve_compute_instance(&instance_name)?;
            ops.push(catalog::Op::DropComputeInstanceReplica {
                name: replica_name.clone(),
                compute_id: instance.id,
            });
            let replica_id = instance.replica_id_by_name[&replica_name];

            replicas_to_drop.push((
                instance.id,
                replica_id,
                instance.replicas_by_id[&replica_id].clone(),
            ));
        }

        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;

        for (compute_id, replica_id, replica) in replicas_to_drop {
            self.drop_replica(compute_id, replica_id, replica.config)
                .await
                .unwrap();
        }

        Ok(ExecuteResponse::DroppedComputeInstanceReplicas)
    }

    async fn sequence_drop_items(
        &mut self,
        session: &Session,
        plan: DropItemsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let ops = self.catalog.drop_items_ops(&plan.items);
        self.catalog_transact(Some(session), ops, |_| Ok(()))
            .await?;
        Ok(match plan.ty {
            ObjectType::Source => ExecuteResponse::DroppedSource,
            ObjectType::View => ExecuteResponse::DroppedView,
            ObjectType::RecordedView => ExecuteResponse::DroppedRecordedView,
            ObjectType::Table => ExecuteResponse::DroppedTable,
            ObjectType::Sink => ExecuteResponse::DroppedSink,
            ObjectType::Index => ExecuteResponse::DroppedIndex,
            ObjectType::Type => ExecuteResponse::DroppedType,
            ObjectType::Secret => ExecuteResponse::DroppedSecret,
            ObjectType::Connection => ExecuteResponse::DroppedConnection,
            ObjectType::Role | ObjectType::Cluster | ObjectType::ClusterReplica => {
                unreachable!("handled through their respective sequence_drop functions")
            }
            ObjectType::Object => unreachable!("generic OBJECT cannot be dropped"),
        })
    }

    fn sequence_show_all_variables(
        &mut self,
        session: &Session,
    ) -> Result<ExecuteResponse, AdapterError> {
        Ok(send_immediate_rows(
            session
                .vars()
                .iter()
                .filter(|v| !v.experimental())
                .map(|v| {
                    Row::pack_slice(&[
                        Datum::String(v.name()),
                        Datum::String(&v.value()),
                        Datum::String(v.description()),
                    ])
                })
                .collect(),
        ))
    }

    fn sequence_show_variable(
        &self,
        session: &Session,
        plan: ShowVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let variable = session.vars().get(&plan.name)?;
        let row = Row::pack_slice(&[Datum::String(&variable.value())]);
        Ok(send_immediate_rows(vec![row]))
    }

    fn sequence_set_variable(
        &self,
        session: &mut Session,
        plan: SetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        use mz_sql::ast::{SetVariableValue, Value};

        let vars = session.vars_mut();
        let (name, local) = (plan.name, plan.local);
        match plan.value {
            SetVariableValue::Literal(Value::String(s)) => vars.set(&name, &s, local)?,
            SetVariableValue::Literal(lit) => vars.set(&name, &lit.to_string(), local)?,
            SetVariableValue::Ident(ident) => vars.set(&name, &ident.into_string(), local)?,
            SetVariableValue::Default => vars.reset(&name, local)?,
        }

        Ok(ExecuteResponse::SetVariable { name, tag: "SET" })
    }

    fn sequence_reset_variable(
        &self,
        session: &mut Session,
        plan: ResetVariablePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        session.vars_mut().reset(&plan.name, false)?;
        Ok(ExecuteResponse::SetVariable {
            name: plan.name,
            tag: "RESET",
        })
    }

    async fn sequence_end_transaction(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        mut action: EndTransactionAction,
    ) {
        // If the transaction has failed, we can only rollback.
        if let (EndTransactionAction::Commit, TransactionStatus::Failed(_)) =
            (&action, session.transaction())
        {
            action = EndTransactionAction::Rollback;
        }
        let response = Ok(ExecuteResponse::TransactionExited {
            tag: action.tag(),
            was_implicit: session.transaction().is_implicit(),
        });

        let result = self
            .sequence_end_transaction_inner(&mut session, action)
            .await;

        let (response, action) = match result {
            Ok((Some(writes), _)) if writes.is_empty() => (response, action),
            Ok((Some(writes), write_lock_guard)) => {
                self.submit_write(PendingWriteTxn {
                    writes,
                    client_transmitter: tx,
                    response,
                    session,
                    action,
                    write_lock_guard,
                });
                return;
            }
            Ok((None, _)) => (response, action),
            Err(err) => (Err(err), EndTransactionAction::Rollback),
        };
        session.vars_mut().end_transaction(action);
        tx.send(response, session);
    }

    async fn sequence_end_transaction_inner(
        &mut self,
        session: &mut Session,
        action: EndTransactionAction,
    ) -> Result<(Option<Vec<WriteOp>>, Option<OwnedMutexGuard<()>>), AdapterError> {
        let txn = self.clear_transaction(session).await;

        if let EndTransactionAction::Commit = action {
            if let (Some(ops), write_lock_guard) = txn.into_ops_and_lock_guard() {
                if let TransactionOps::Writes(mut writes) = ops {
                    for WriteOp { id, .. } in &writes {
                        // Re-verify this id exists.
                        let _ = self.catalog.try_get_entry(&id).ok_or_else(|| {
                            AdapterError::SqlCatalog(CatalogError::UnknownItem(id.to_string()))
                        })?;
                    }

                    // `rows` can be empty if, say, a DELETE's WHERE clause had 0 results.
                    writes.retain(|WriteOp { rows, .. }| !rows.is_empty());
                    return Ok((Some(writes), write_lock_guard));
                }
            }
        }
        Ok((None, None))
    }

    /// Return the set of ids in a timedomain and verify timeline correctness.
    ///
    /// When a user starts a transaction, we need to prevent compaction of anything
    /// they might read from. We use a heuristic of "anything in the same database
    /// schemas with the same timeline as whatever the first query is".
    fn timedomain_for<'a, I>(
        &self,
        uses_ids: I,
        timeline: &Option<Timeline>,
        conn_id: ConnectionId,
        compute_instance: ComputeInstanceId,
    ) -> Result<CollectionIdBundle, AdapterError>
    where
        I: IntoIterator<Item = &'a GlobalId>,
    {
        // Gather all the used schemas.
        let mut schemas = HashSet::new();
        for id in uses_ids {
            let entry = self.catalog.get_entry(id);
            let name = entry.name();
            schemas.insert((&name.qualifiers.database_spec, &name.qualifiers.schema_spec));
        }

        // If any of the system schemas is specified, add the rest of the
        // system schemas.
        let system_schemas = [
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog.get_mz_catalog_schema_id().clone()),
            ),
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog.get_pg_catalog_schema_id().clone()),
            ),
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog.get_information_schema_id().clone()),
            ),
        ];
        if system_schemas.iter().any(|s| schemas.contains(s)) {
            schemas.extend(system_schemas);
        }

        // Gather the IDs of all items in all used schemas.
        let mut item_ids: HashSet<GlobalId> = HashSet::new();
        for (db, schema) in schemas {
            let schema = self.catalog.get_schema(&db, &schema, conn_id);
            item_ids.extend(schema.items.values());
        }

        // Gather the dependencies of those items.
        let mut id_bundle: CollectionIdBundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(item_ids.iter());

        // Filter out ids from different timelines.
        for ids in [
            &mut id_bundle.storage_ids,
            &mut id_bundle.compute_ids.entry(compute_instance).or_default(),
        ] {
            ids.retain(|&id| {
                let id_timeline = self
                    .validate_timeline(vec![id])
                    .expect("single id should never fail");
                match (&id_timeline, &timeline) {
                    // If this id doesn't have a timeline, we can keep it.
                    (None, _) => true,
                    // If there's no source timeline, we have the option to opt into a timeline,
                    // so optimistically choose epoch ms. This is useful when the first query in a
                    // transaction is on a static view.
                    (Some(id_timeline), None) => id_timeline == &Timeline::EpochMilliseconds,
                    // Otherwise check if timelines are the same.
                    (Some(id_timeline), Some(source_timeline)) => id_timeline == source_timeline,
                }
            });
        }

        Ok(id_bundle)
    }

    /// Sequence a peek, determining a timestamp and the most efficient dataflow interaction.
    ///
    /// Peeks are sequenced by assigning a timestamp for evaluation, and then determining and
    /// deploying the most efficient evaluation plan. The peek could evaluate to a constant,
    /// be a simple read out of an existing arrangement, or required a new dataflow to build
    /// the results to return.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn sequence_peek(
        &mut self,
        session: &mut Session,
        plan: PeekPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        event!(Level::TRACE, plan = format!("{:?}", plan));
        fn check_no_invalid_log_reads<S: Append>(
            catalog: &Catalog<S>,
            compute_instance: &ComputeInstance,
            source_ids: &BTreeSet<GlobalId>,
            target_replica: &mut Option<ReplicaId>,
        ) -> Result<(), AdapterError> {
            let log_names = source_ids
                .iter()
                .flat_map(|id| catalog.log_dependencies(*id))
                .map(|id| catalog.get_entry(&id).name().item.clone())
                .collect::<Vec<_>>();

            if log_names.is_empty() {
                return Ok(());
            }

            // If logging is not initialized for the cluster, no indexes are set
            // up for log sources. This check ensures that we don't try to read
            // from the raw sources, which is not supported.
            if compute_instance.logging.is_none() {
                return Err(AdapterError::IntrospectionDisabled { log_names });
            }

            // Reading from log sources on replicated compute instances is only
            // allowed if a target replica is selected. Otherwise, we have no
            // way of knowing which replica we read the introspection data from.
            let num_replicas = compute_instance.replicas_by_id.len();
            if target_replica.is_none() {
                if num_replicas == 1 {
                    *target_replica = compute_instance.replicas_by_id.keys().next().copied();
                } else {
                    return Err(AdapterError::UntargetedLogRead { log_names });
                }
            }
            Ok(())
        }

        let PeekPlan {
            mut source,
            when,
            finishing,
            copy_to,
        } = plan;

        let compute_instance = self
            .catalog
            .resolve_compute_instance(session.vars().cluster())?;

        let target_replica_name = session.vars().cluster_replica();
        let mut target_replica = target_replica_name
            .map(|name| {
                compute_instance
                    .replica_id_by_name
                    .get(name)
                    .copied()
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: compute_instance.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        if compute_instance.replicas_by_id.is_empty() {
            return Err(AdapterError::NoClusterReplicasAvailable(
                compute_instance.name.clone(),
            ));
        }

        let source_ids = source.depends_on();
        check_no_invalid_log_reads(
            &self.catalog,
            compute_instance,
            &source_ids,
            &mut target_replica,
        )?;

        let compute_instance = compute_instance.id;

        let timeline = self.validate_timeline(source_ids.clone())?;
        let conn_id = session.conn_id();
        let in_transaction = matches!(
            session.transaction(),
            &TransactionStatus::InTransaction(_) | &TransactionStatus::InTransactionImplicit(_)
        );
        // Queries are independent of the logical timestamp iff there are no referenced
        // sources or indexes and there is no reference to `mz_logical_timestamp()`.
        let timestamp_independent = source_ids.is_empty() && !source.contains_temporal();
        // For explicit or implicit transactions that do not use AS OF, get the
        // timestamp of the in-progress transaction or create one. If this is an AS OF
        // query, we don't care about any possible transaction timestamp. If this is a
        // single-statement transaction (TransactionStatus::Started), we don't need to
        // worry about preventing compaction or choosing a valid timestamp for future
        // queries.
        let timestamp = if in_transaction && when == QueryWhen::Immediately {
            // If all previous statements were timestamp-independent and the current one is
            // not, clear the transaction ops so it can get a new timestamp and timedomain.
            if let Some(read_txn) = self.txn_reads.get(&conn_id) {
                if read_txn.timestamp_independent && !timestamp_independent {
                    session.clear_transaction_ops();
                }
            }

            let timestamp = match session.get_transaction_timestamp() {
                Some(ts) => ts,
                _ => {
                    // Determine a timestamp that will be valid for anything in any schema
                    // referenced by the first query.
                    let id_bundle =
                        self.timedomain_for(&source_ids, &timeline, conn_id, compute_instance)?;

                    // We want to prevent compaction of the indexes consulted by
                    // determine_timestamp, not the ones listed in the query.
                    let timestamp = self.determine_timestamp(
                        session,
                        &id_bundle,
                        &QueryWhen::Immediately,
                        compute_instance,
                    )?;
                    let read_holds = read_holds::ReadHolds {
                        time: timestamp,
                        id_bundle,
                    };
                    self.acquire_read_holds(&read_holds).await;
                    let txn_reads = TxnReads {
                        timestamp_independent,
                        read_holds,
                    };
                    self.txn_reads.insert(conn_id, txn_reads);
                    timestamp
                }
            };

            // Verify that the references and indexes for this query are in the
            // current read transaction.
            let id_bundle = self
                .index_oracle(compute_instance)
                .sufficient_collections(&source_ids);
            let allowed_id_bundle = &self.txn_reads.get(&conn_id).unwrap().read_holds.id_bundle;
            // Find the first reference or index (if any) that is not in the transaction. A
            // reference could be caused by a user specifying an object in a different
            // schema than the first query. An index could be caused by a CREATE INDEX
            // after the transaction started.
            let outside = id_bundle.difference(allowed_id_bundle);
            if !outside.is_empty() {
                let mut names: Vec<_> = allowed_id_bundle
                    .iter()
                    // This could filter out a view that has been replaced in another transaction.
                    .filter_map(|id| self.catalog.try_get_entry(&id))
                    .map(|item| item.name())
                    .map(|name| {
                        self.catalog
                            .resolve_full_name(name, Some(session.conn_id()))
                            .to_string()
                    })
                    .collect();
                let mut outside: Vec<_> = outside
                    .iter()
                    .filter_map(|id| self.catalog.try_get_entry(&id))
                    .map(|item| item.name())
                    .map(|name| {
                        self.catalog
                            .resolve_full_name(name, Some(session.conn_id()))
                            .to_string()
                    })
                    .collect();
                // Sort so error messages are deterministic.
                names.sort();
                outside.sort();
                return Err(AdapterError::RelationOutsideTimeDomain {
                    relations: outside,
                    names,
                });
            }

            timestamp
        } else {
            // TODO(guswynn): acquire_read_holds for linearized reads
            let id_bundle = self
                .index_oracle(compute_instance)
                .sufficient_collections(&source_ids);
            self.determine_timestamp(session, &id_bundle, &when, compute_instance)?
        };

        // before we have the corrected timestamp ^
        // TODO(guswynn&mjibson): partition `sequence_peek` by the response to
        // `linearize_sources(source_ids.iter().collect()).await`
        // ------------------------------
        // after we have the timestamp \/

        let source = self.view_optimizer.optimize(source)?;

        // We create a dataflow and optimize it, to determine if we can avoid building it.
        // This can happen if the result optimizes to a constant, or to a `Get` expression
        // around a maintained arrangement.
        let typ = source.typ();
        let key: Vec<MirScalarExpr> = typ
            .default_key()
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect();
        let (permutation, thinning) = permutation_for_arrangement(&key, typ.arity());
        // Two transient allocations. We could reclaim these if we don't use them, potentially.
        // TODO: reclaim transient identifiers in fast path cases.
        let view_id = self.allocate_transient_id()?;
        let index_id = self.allocate_transient_id()?;
        // The assembled dataflow contains a view and an index of that view.
        let mut dataflow = DataflowDesc::new(format!("temp-view-{}", view_id));
        dataflow.set_as_of(Antichain::from_elem(timestamp));
        let mut builder = self.dataflow_builder(compute_instance);
        builder.import_view_into_dataflow(&view_id, &source, &mut dataflow)?;
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            prep_relation_expr(
                self.catalog.state(),
                plan,
                ExprPrepStyle::OneShot {
                    logical_time: Some(timestamp),
                    session,
                },
            )?;
        }
        dataflow.export_index(
            index_id,
            IndexDesc {
                on_id: view_id,
                key: key.clone(),
            },
            typ,
        );

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(&mut dataflow, &builder.index_oracle())?;

        // Finalization optimizes the dataflow as much as possible.
        let dataflow_plan = self.finalize_dataflow(dataflow, compute_instance);

        // At this point, `dataflow_plan` contains our best optimized dataflow.
        // We will check the plan to see if there is a fast path to escape full dataflow construction.
        let fast_path = fast_path_peek::create_plan(
            dataflow_plan,
            view_id,
            index_id,
            key,
            permutation,
            thinning.len(),
        )?;

        // We only track the peeks in the session if the query is in a transaction,
        // the query doesn't use AS OF, it's a non-constant or timestamp dependent query.
        if in_transaction
            && when == QueryWhen::Immediately
            && (!matches!(fast_path, fast_path_peek::Plan::Constant(_)) || !timestamp_independent)
        {
            session.add_transaction_ops(TransactionOps::Peeks(timestamp))?;
        }

        // Implement the peek, and capture the response.
        let resp = self
            .implement_fast_path_peek(
                fast_path,
                timestamp,
                finishing,
                conn_id,
                source.arity(),
                compute_instance,
                target_replica,
            )
            .await?;

        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    async fn sequence_tail(
        &mut self,
        session: &mut Session,
        plan: TailPlan,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let TailPlan {
            from,
            with_snapshot,
            when,
            copy_to,
            emit_progress,
        } = plan;

        let compute_instance = self
            .catalog
            .resolve_compute_instance(session.vars().cluster())?
            .id;

        // TAIL AS OF, similar to peeks, doesn't need to worry about transaction
        // timestamp semantics.
        if when == QueryWhen::Immediately {
            // If this isn't a TAIL AS OF, the TAIL can be in a transaction if it's the
            // only operation.
            session.add_transaction_ops(TransactionOps::Tail)?;
        }

        let make_sink_desc = |coord: &mut Coordinator<S>, from, from_desc, uses| {
            // Determine the frontier of updates to tail *from*.
            // Updates greater or equal to this frontier will be produced.
            let id_bundle = coord
                .index_oracle(compute_instance)
                .sufficient_collections(uses);
            // If a timestamp was explicitly requested, use that.
            let timestamp =
                coord.determine_timestamp(session, &id_bundle, &when, compute_instance)?;

            Ok::<_, AdapterError>(SinkDesc {
                from,
                from_desc,
                connection: SinkConnection::Tail(TailSinkConnection::default()),
                envelope: None,
                as_of: SinkAsOf {
                    frontier: Antichain::from_elem(timestamp),
                    strict: !with_snapshot,
                },
            })
        };

        let dataflow = match from {
            TailFrom::Id(from_id) => {
                let from = self.catalog.get_entry(&from_id);
                let from_desc = from
                    .desc(
                        &self
                            .catalog
                            .resolve_full_name(from.name(), Some(session.conn_id())),
                    )
                    .unwrap()
                    .into_owned();
                let sink_id = self.catalog.allocate_user_id().await?;
                let sink_desc = make_sink_desc(self, from_id, from_desc, &[from_id][..])?;
                let sink_name = format!("tail-{}", sink_id);
                self.dataflow_builder(compute_instance)
                    .build_sink_dataflow(sink_name, sink_id, sink_desc)?
            }
            TailFrom::Query { expr, desc } => {
                let id = self.allocate_transient_id()?;
                let expr = self.view_optimizer.optimize(expr)?;
                let desc = RelationDesc::new(expr.typ(), desc.iter_names());
                let sink_desc = make_sink_desc(self, id, desc, &depends_on)?;
                let mut dataflow = DataflowDesc::new(format!("tail-{}", id));
                let mut dataflow_builder = self.dataflow_builder(compute_instance);
                dataflow_builder.import_view_into_dataflow(&id, &expr, &mut dataflow)?;
                dataflow_builder.build_sink_dataflow_into(&mut dataflow, id, sink_desc)?;
                dataflow
            }
        };

        let (sink_id, sink_desc) = dataflow.sink_exports.iter().next().unwrap();
        session.add_drop_sink(compute_instance, *sink_id);
        let arity = sink_desc.from_desc.arity();
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending_tails
            .insert(*sink_id, PendingTail::new(tx, emit_progress, arity));
        self.ship_dataflow(dataflow, compute_instance).await;

        let resp = ExecuteResponse::Tailing { rx };
        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    /// The smallest common valid read frontier among the specified collections.
    fn least_valid_read(&self, id_bundle: &CollectionIdBundle) -> Antichain<mz_repr::Timestamp> {
        let mut since = Antichain::from_elem(Timestamp::minimum());
        {
            let storage = self.controller.storage();
            for id in id_bundle.storage_ids.iter() {
                since.join_assign(&storage.collection(*id).unwrap().implied_capability)
            }
        }
        {
            for (instance, compute_ids) in &id_bundle.compute_ids {
                let compute = self.controller.compute(*instance).unwrap();
                for id in compute_ids.iter() {
                    since.join_assign(&compute.collection(*id).unwrap().implied_capability)
                }
            }
        }
        since
    }

    /// The smallest common valid write frontier among the specified collections.
    ///
    /// Times that are not greater or equal to this frontier are complete for all collections
    /// identified as arguments.
    fn least_valid_write(&self, id_bundle: &CollectionIdBundle) -> Antichain<mz_repr::Timestamp> {
        let mut since = Antichain::new();
        {
            let storage = self.controller.storage();
            for id in id_bundle.storage_ids.iter() {
                since.extend(
                    storage
                        .collection(*id)
                        .unwrap()
                        .write_frontier
                        .frontier()
                        .iter()
                        .cloned(),
                );
            }
        }
        {
            for (instance, compute_ids) in &id_bundle.compute_ids {
                let compute = self.controller.compute(*instance).unwrap();
                for id in compute_ids.iter() {
                    since.extend(
                        compute
                            .collection(*id)
                            .unwrap()
                            .write_frontier
                            .frontier()
                            .iter()
                            .cloned(),
                    );
                }
            }
        }
        since
    }

    /// The largest element not in advance of any object in the collection.
    ///
    /// Times that are not greater to this frontier are complete for all collections
    /// identified as arguments.
    fn largest_not_in_advance_of_upper(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> mz_repr::Timestamp {
        let upper = self.least_valid_write(&id_bundle);

        // We peek at the largest element not in advance of `upper`, which
        // involves a subtraction. If `upper` contains a zero timestamp there
        // is no "prior" answer, and we do not want to peek at it as it risks
        // hanging awaiting the response to data that may never arrive.
        if let Some(upper) = upper.as_option() {
            upper.step_back().unwrap_or_else(Timestamp::minimum)
        } else {
            // A complete trace can be read in its final form with this time.
            //
            // This should only happen for literals that have no sources or sources that
            // are known to have completed (non-tailed files for example).
            Timestamp::MAX
        }
    }

    /// Determines the timestamp for a query.
    ///
    /// Timestamp determination may fail due to the restricted validity of
    /// traces. Each has a `since` and `upper` frontier, and are only valid
    /// after `since` and sure to be available not after `upper`.
    ///
    /// The set of storage and compute IDs used when determining the timestamp
    /// are also returned.
    fn determine_timestamp(
        &mut self,
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
    ) -> Result<Timestamp, AdapterError> {
        // Each involved trace has a validity interval `[since, upper)`.
        // The contents of a trace are only guaranteed to be correct when
        // accumulated at a time greater or equal to `since`, and they
        // are only guaranteed to be currently present for times not
        // greater or equal to `upper`.
        //
        // The plan is to first determine a timestamp, based on the requested
        // timestamp policy, and then determine if it can be satisfied using
        // the compacted arrangements we have at hand. It remains unresolved
        // what to do if it cannot be satisfied (perhaps the query should use
        // a larger timestamp and block, perhaps the user should intervene).

        let since = self.least_valid_read(&id_bundle);

        // Initialize candidate to the minimum correct time.
        let mut candidate = Timestamp::minimum();

        if let Some(mut timestamp) = when.advance_to_timestamp() {
            let temp_storage = RowArena::new();
            prep_scalar_expr(self.catalog.state(), &mut timestamp, ExprPrepStyle::AsOf)?;
            let evaled = timestamp.eval(&[], &temp_storage)?;
            if evaled.is_null() {
                coord_bail!("can't use {} as a timestamp for AS OF", evaled);
            }
            let ty = timestamp.typ(&RelationType::empty());
            let ts = match ty.scalar_type {
                ScalarType::Numeric { .. } => {
                    let n = evaled.unwrap_numeric().0;
                    u64::try_from(n)?
                }
                ScalarType::Int16 => evaled.unwrap_int16().try_into()?,
                ScalarType::Int32 => evaled.unwrap_int32().try_into()?,
                ScalarType::Int64 => evaled.unwrap_int64().try_into()?,
                ScalarType::TimestampTz => {
                    evaled.unwrap_timestamptz().timestamp_millis().try_into()?
                }
                ScalarType::Timestamp => evaled.unwrap_timestamp().timestamp_millis().try_into()?,
                _ => coord_bail!(
                    "can't use {} as a timestamp for AS OF",
                    self.catalog.for_session(session).humanize_column_type(&ty)
                ),
            };
            candidate.join_assign(&ts);
        }

        let isolation_level = session.vars().transaction_isolation();
        let timeline = self.validate_timeline(id_bundle.iter())?;
        let use_timestamp_oracle = isolation_level == &vars::IsolationLevel::StrictSerializable
            && timeline.is_some()
            && when.advance_to_global_ts();

        if use_timestamp_oracle {
            let timeline = timeline.expect("checked that timeline exists above");
            let timestamp_oracle = &mut self
                .global_timelines
                .get_mut(&timeline)
                .expect("all timelines have a timestamp oracle")
                .oracle;
            candidate.join_assign(&timestamp_oracle.read_ts());
        } else {
            if when.advance_to_since() {
                candidate.advance_by(since.borrow());
            }
            if when.advance_to_upper() {
                let upper = self.largest_not_in_advance_of_upper(&id_bundle);
                candidate.join_assign(&upper);
            }
        }

        if use_timestamp_oracle && when == &QueryWhen::Immediately {
            assert!(
                since.less_equal(&candidate),
                "the strict serializable isolation level guarantees that the timestamp chosen \
                ({candidate}) is greater than or equal to since ({:?}) via read holds",
                since
            )
        }

        // If the timestamp is greater or equal to some element in `since` we are
        // assured that the answer will be correct.
        if since.less_equal(&candidate) {
            Ok(candidate)
        } else {
            let invalid_indexes =
                if let Some(compute_ids) = id_bundle.compute_ids.get(&compute_instance) {
                    compute_ids
                        .iter()
                        .filter_map(|id| {
                            let since = self
                                .controller
                                .compute(compute_instance)
                                .unwrap()
                                .collection(*id)
                                .unwrap()
                                .read_capabilities
                                .frontier()
                                .to_owned();
                            if since.less_equal(&candidate) {
                                None
                            } else {
                                Some(since)
                            }
                        })
                        .collect()
                } else {
                    Vec::new()
                };
            let invalid_sources = id_bundle.storage_ids.iter().filter_map(|id| {
                let since = self
                    .controller
                    .storage()
                    .collection(*id)
                    .unwrap()
                    .read_capabilities
                    .frontier()
                    .to_owned();
                if since.less_equal(&candidate) {
                    None
                } else {
                    Some(since)
                }
            });
            let invalid = invalid_indexes
                .into_iter()
                .chain(invalid_sources)
                .collect::<Vec<_>>();
            coord_bail!(
                "Timestamp ({}) is not valid for all inputs: {:?}",
                candidate,
                invalid
            );
        }
    }

    fn sequence_explain(
        &mut self,
        session: &Session,
        plan: ExplainPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        match plan {
            ExplainPlan::New(plan) => self.sequence_explain_new(session, plan),
            ExplainPlan::Old(plan) => self.sequence_explain_old(session, plan),
        }
    }

    fn sequence_explain_new(
        &mut self,
        session: &Session,
        plan: ExplainPlanNew,
    ) -> Result<ExecuteResponse, AdapterError> {
        let compute_instance = self
            .catalog
            .resolve_compute_instance(session.vars().cluster())?
            .id;

        let ExplainPlanNew {
            mut raw_plan,
            row_set_finishing,
            stage,
            format,
            config,
        } = plan;

        let decorrelate = |raw_plan: HirRelationExpr| -> Result<MirRelationExpr, AdapterError> {
            let decorrelated_plan = raw_plan.optimize_and_lower(&OptimizerConfig {
                qgm_optimizations: session.vars().qgm_optimizations(),
            })?;
            Ok(decorrelated_plan)
        };

        let optimize =
            |coord: &mut Self,
             decorrelated_plan: MirRelationExpr|
             -> Result<DataflowDescription<OptimizedMirRelationExpr>, AdapterError> {
                let optimized_plan = coord.view_optimizer.optimize(decorrelated_plan)?;
                let mut dataflow = DataflowDesc::new(format!("explanation"));
                coord
                    .dataflow_builder(compute_instance)
                    .import_view_into_dataflow(
                        // TODO: If explaining a view, pipe the actual id of the view.
                        &GlobalId::Explain,
                        &optimized_plan,
                        &mut dataflow,
                    )?;
                mz_transform::optimize_dataflow(
                    &mut dataflow,
                    &coord.index_oracle(compute_instance),
                )?;
                Ok(dataflow)
            };

        let explanation_string = match stage {
            ExplainStageNew::RawPlan => {
                // construct explanation context
                let catalog = self.catalog.for_session(session);
                let context = ExplainContext {
                    config: &config,
                    humanizer: &catalog,
                    used_indexes: UsedIndexes::new(Default::default()),
                    finishing: row_set_finishing,
                    fast_path_plan: Default::default(),
                };
                // explain plan
                Explainable::new(&mut raw_plan).explain(&format, &config, &context)?
            }
            ExplainStageNew::QueryGraph => {
                // run partial pipeline
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                // construct explanation context
                let catalog = self.catalog.for_session(session);
                let context = ExplainContext {
                    config: &config,
                    humanizer: &catalog,
                    used_indexes: UsedIndexes::new(Default::default()),
                    finishing: row_set_finishing,
                    fast_path_plan: Default::default(),
                };
                // explain plan
                Explainable::new(&mut model).explain(&format, &config, &context)?
            }
            ExplainStageNew::OptimizedQueryGraph => {
                // run partial pipeline
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                model.optimize();
                // construct explanation context
                let catalog = self.catalog.for_session(session);
                let context = ExplainContext {
                    config: &config,
                    humanizer: &catalog,
                    used_indexes: UsedIndexes::new(Default::default()),
                    finishing: row_set_finishing,
                    fast_path_plan: Default::default(),
                };
                // explain plan
                Explainable::new(&mut model).explain(&format, &config, &context)?
            }
            ExplainStageNew::DecorrelatedPlan => {
                // run partial pipeline
                let decorrelated_plan = decorrelate(raw_plan)?;
                self.validate_timeline(decorrelated_plan.depends_on())?;
                let mut dataflow = OptimizedMirRelationExpr::declare_optimized(decorrelated_plan);
                // construct explanation context
                let catalog = self.catalog.for_session(session);
                let context = ExplainContext {
                    config: &config,
                    humanizer: &catalog,
                    used_indexes: UsedIndexes::new(Default::default()),
                    finishing: row_set_finishing,
                    fast_path_plan: Default::default(),
                };
                // explain plan
                Explainable::new(&mut dataflow).explain(&format, &config, &context)?
            }
            ExplainStageNew::OptimizedPlan => {
                // run partial pipeline
                let decorrelated_plan = decorrelate(raw_plan)?;
                self.validate_timeline(decorrelated_plan.depends_on())?;
                let mut dataflow = optimize(self, decorrelated_plan)?;
                // construct explanation context
                let catalog = self.catalog.for_session(session);
                let context = ExplainContext {
                    config: &config,
                    humanizer: &catalog,
                    used_indexes: UsedIndexes::new(Default::default()),
                    finishing: row_set_finishing,
                    fast_path_plan: Default::default(),
                };
                // explain plan
                Explainable::new(&mut dataflow).explain(&format, &config, &context)?
            }
            ExplainStageNew::PhysicalPlan => {
                // run partial pipeline
                let decorrelated_plan = decorrelate(raw_plan)?;
                self.validate_timeline(decorrelated_plan.depends_on())?;
                let dataflow = optimize(self, decorrelated_plan)?;
                let mut dataflow_plan =
                    mz_compute_client::plan::Plan::<mz_repr::Timestamp>::finalize_dataflow(
                        dataflow,
                    )
                    .expect("Dataflow planning failed; unrecoverable error");
                // construct explanation context
                let catalog = self.catalog.for_session(session);
                let context = ExplainContext {
                    config: &config,
                    humanizer: &catalog,
                    used_indexes: UsedIndexes::new(Default::default()),
                    finishing: row_set_finishing,
                    fast_path_plan: Default::default(),
                };
                // explain plan
                Explainable::new(&mut dataflow_plan).explain(&format, &config, &context)?
            }
            ExplainStageNew::Trace => {
                let feature = "ExplainStageNew::Trace";
                Err(AdapterError::Unsupported(feature))?
            }
        };

        let rows = vec![Row::pack_slice(&[Datum::from(&*explanation_string)])];
        Ok(send_immediate_rows(rows))
    }

    fn sequence_explain_old(
        &mut self,
        session: &Session,
        plan: ExplainPlanOld,
    ) -> Result<ExecuteResponse, AdapterError> {
        let compute_instance = self
            .catalog
            .resolve_compute_instance(session.vars().cluster())?
            .id;

        let ExplainPlanOld {
            raw_plan,
            row_set_finishing,
            stage,
            options,
        } = plan;

        struct Timings {
            decorrelation: Option<Duration>,
            optimization: Option<Duration>,
        }

        let mut timings = Timings {
            decorrelation: None,
            optimization: None,
        };

        let decorrelate = |timings: &mut Timings,
                           raw_plan: HirRelationExpr|
         -> Result<MirRelationExpr, AdapterError> {
            let start = Instant::now();
            let decorrelated_plan = raw_plan.optimize_and_lower(&OptimizerConfig {
                qgm_optimizations: session.vars().qgm_optimizations(),
            })?;
            timings.decorrelation = Some(start.elapsed());
            Ok(decorrelated_plan)
        };

        let optimize =
            |timings: &mut Timings,
             coord: &mut Self,
             decorrelated_plan: MirRelationExpr|
             -> Result<DataflowDescription<OptimizedMirRelationExpr>, AdapterError> {
                let start = Instant::now();
                let optimized_plan = coord.view_optimizer.optimize(decorrelated_plan)?;
                let mut dataflow = DataflowDesc::new(format!("explanation"));
                coord
                    .dataflow_builder(compute_instance)
                    .import_view_into_dataflow(
                        // TODO: If explaining a view, pipe the actual id of the view.
                        &GlobalId::Explain,
                        &optimized_plan,
                        &mut dataflow,
                    )?;
                mz_transform::optimize_dataflow(
                    &mut dataflow,
                    &coord.index_oracle(compute_instance),
                )?;
                timings.optimization = Some(start.elapsed());
                Ok(dataflow)
            };

        let mut explanation_string = match stage {
            ExplainStageOld::RawPlan => {
                let catalog = self.catalog.for_session(session);
                let mut explanation = mz_sql::plan::Explanation::new(&raw_plan, &catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types(&BTreeMap::new());
                }
                explanation.to_string()
            }
            ExplainStageOld::QueryGraph => {
                let catalog = self.catalog.for_session(session);
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                model.as_dot("", &catalog, options.typed)?
            }
            ExplainStageOld::OptimizedQueryGraph => {
                let catalog = self.catalog.for_session(session);
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                model.optimize();
                model.as_dot("", &catalog, options.typed)?
            }
            ExplainStageOld::DecorrelatedPlan => {
                let decorrelated_plan = OptimizedMirRelationExpr::declare_optimized(decorrelate(
                    &mut timings,
                    raw_plan,
                )?);
                let catalog = self.catalog.for_session(session);
                let formatter = DataflowGraphFormatter::new(&catalog, options.typed);
                let mut explanation = Explanation::new(&decorrelated_plan, &catalog, &formatter);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                explanation.to_string()
            }
            ExplainStageOld::OptimizedPlan => {
                let decorrelated_plan = decorrelate(&mut timings, raw_plan)?;
                self.validate_timeline(decorrelated_plan.depends_on())?;
                let dataflow = optimize(&mut timings, self, decorrelated_plan)?;
                let catalog = self.catalog.for_session(session);
                let formatter = DataflowGraphFormatter::new(&catalog, options.typed);
                let mut explanation =
                    Explanation::new_from_dataflow(&dataflow, &catalog, &formatter);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                explanation.to_string()
            }
            ExplainStageOld::PhysicalPlan => {
                let decorrelated_plan = decorrelate(&mut timings, raw_plan)?;
                self.validate_timeline(decorrelated_plan.depends_on())?;
                let dataflow = optimize(&mut timings, self, decorrelated_plan)?;
                let dataflow_plan =
                    mz_compute_client::plan::Plan::<mz_repr::Timestamp>::finalize_dataflow(
                        dataflow,
                    )
                    .expect("Dataflow planning failed; unrecoverable error");
                let catalog = self.catalog.for_session(session);
                let mut explanation =
                    Explanation::new_from_dataflow(&dataflow_plan, &catalog, &JsonViewFormatter {});
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                explanation.to_string()
            }
            ExplainStageOld::Timestamp => {
                let decorrelated_plan = decorrelate(&mut timings, raw_plan)?;
                let optimized_plan = self.view_optimizer.optimize(decorrelated_plan)?;
                self.validate_timeline(optimized_plan.depends_on())?;
                let source_ids = optimized_plan.depends_on();
                let id_bundle = self
                    .index_oracle(compute_instance)
                    .sufficient_collections(&source_ids);
                // TODO: determine_timestamp takes a mut self to track table linearizability,
                // so explaining a plan involving tables has side effects. Removing those side
                // effects would be good.
                let timestamp = self.determine_timestamp(
                    &session,
                    &id_bundle,
                    &QueryWhen::Immediately,
                    compute_instance,
                )?;
                let since = self.least_valid_read(&id_bundle).elements().to_vec();
                let upper = self.least_valid_write(&id_bundle).elements().to_vec();
                let has_table = id_bundle.iter().any(|id| self.catalog.uses_tables(id));
                let table_read_ts = if has_table {
                    Some(self.get_local_read_ts())
                } else {
                    None
                };
                let mut sources = Vec::new();
                {
                    let storage = self.controller.storage();
                    for id in id_bundle.storage_ids.iter() {
                        let state = storage.collection(*id).unwrap();
                        let name = self
                            .catalog
                            .try_get_entry(id)
                            .map(|item| item.name())
                            .map(|name| {
                                self.catalog
                                    .resolve_full_name(name, Some(session.conn_id()))
                                    .to_string()
                            })
                            .unwrap_or_else(|| id.to_string());
                        sources.push(TimestampSource {
                            name: format!("{name} ({id}, storage)"),
                            read_frontier: state.implied_capability.elements().to_vec(),
                            write_frontier: state
                                .write_frontier
                                .frontier()
                                .to_owned()
                                .elements()
                                .to_vec(),
                        });
                    }
                }
                {
                    if let Some(compute_ids) = id_bundle.compute_ids.get(&compute_instance) {
                        let compute = self.controller.compute(compute_instance).unwrap();
                        for id in compute_ids {
                            let state = compute.collection(*id).unwrap();
                            let name = self
                                .catalog
                                .try_get_entry(id)
                                .map(|item| item.name())
                                .map(|name| {
                                    self.catalog
                                        .resolve_full_name(name, Some(session.conn_id()))
                                        .to_string()
                                })
                                .unwrap_or_else(|| id.to_string());
                            sources.push(TimestampSource {
                                name: format!("{name} ({id}, compute)"),
                                read_frontier: state.implied_capability.elements().to_vec(),
                                write_frontier: state
                                    .write_frontier
                                    .frontier()
                                    .to_owned()
                                    .elements()
                                    .to_vec(),
                            });
                        }
                    }
                }
                let explanation = TimestampExplanation {
                    timestamp,
                    since,
                    upper,
                    has_table,
                    table_read_ts,
                    sources,
                };
                explanation.to_string()
            }
        };
        if options.timing {
            if let Some(decorrelation) = &timings.decorrelation {
                write!(
                    explanation_string,
                    "\nDecorrelation time: {}",
                    Interval {
                        months: 0,
                        days: 0,
                        micros: decorrelation.as_micros().try_into().unwrap(),
                    }
                )
                .expect("Write failed");
            }
            if let Some(optimization) = &timings.optimization {
                write!(
                    explanation_string,
                    "\nOptimization time: {}",
                    Interval {
                        months: 0,
                        days: 0,
                        micros: optimization.as_micros().try_into().unwrap(),
                    }
                )
                .expect("Write failed");
            }
            if timings.decorrelation.is_some() || timings.optimization.is_some() {
                explanation_string.push_str("\n");
            }
        }
        let rows = vec![Row::pack_slice(&[Datum::from(&*explanation_string)])];
        Ok(send_immediate_rows(rows))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn sequence_send_diffs(
        &mut self,
        session: &mut Session,
        mut plan: SendDiffsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let affected_rows = {
            let mut affected_rows = Diff::from(0);
            let mut all_positive_diffs = true;
            // If all diffs are positive, the number of affected rows is just the
            // sum of all unconsolidated diffs.
            for (_, diff) in plan.updates.iter() {
                if *diff < 0 {
                    all_positive_diffs = false;
                    break;
                }

                affected_rows += diff;
            }

            if !all_positive_diffs {
                // Consolidate rows. This is useful e.g. for an UPDATE where the row
                // doesn't change, and we need to reflect that in the number of
                // affected rows.
                differential_dataflow::consolidation::consolidate(&mut plan.updates);

                affected_rows = 0;
                // With retractions, the number of affected rows is not the number
                // of rows we see, but the sum of the absolute value of their diffs,
                // e.g. if one row is retracted and another is added, the total
                // number of rows affected is 2.
                for (_, diff) in plan.updates.iter() {
                    affected_rows += diff.abs();
                }
            }

            usize::try_from(affected_rows).expect("positive isize must fit")
        };
        event!(
            Level::TRACE,
            affected_rows,
            id = format!("{:?}", plan.id),
            kind = format!("{:?}", plan.kind),
            updates = plan.updates.len(),
            returning = plan.returning.len(),
        );

        session.add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
            id: plan.id,
            rows: plan.updates,
        }]))?;
        if !plan.returning.is_empty() {
            let finishing = RowSetFinishing {
                order_by: Vec::new(),
                limit: None,
                offset: 0,
                project: (0..plan.returning[0].0.iter().count()).collect(),
            };
            return Ok(send_immediate_rows(finishing.finish(plan.returning)));
        }
        Ok(match plan.kind {
            MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
            MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
            MutationKind::Update => ExecuteResponse::Updated(affected_rows / 2),
        })
    }

    async fn sequence_insert(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: InsertPlan,
    ) {
        let optimized_mir = if let MirRelationExpr::Constant { .. } = &plan.values {
            // We don't perform any optimizations on an expression that is already
            // a constant for writes, as we want to maximize bulk-insert throughput.
            OptimizedMirRelationExpr(plan.values)
        } else {
            match self.view_optimizer.optimize(plan.values) {
                Ok(m) => m,
                Err(e) => {
                    tx.send(Err(e.into()), session);
                    return;
                }
            }
        };

        match optimized_mir.into_inner() {
            constants @ MirRelationExpr::Constant { .. } if plan.returning.is_empty() => tx.send(
                self.sequence_insert_constant(&mut session, plan.id, constants),
                session,
            ),
            // All non-constant values must be planned as read-then-writes.
            mut selection => {
                let desc_arity = match self.catalog.try_get_entry(&plan.id) {
                    Some(table) => table
                        .desc(
                            &self
                                .catalog
                                .resolve_full_name(table.name(), Some(session.conn_id())),
                        )
                        .expect("desc called on table")
                        .arity(),
                    None => {
                        tx.send(
                            Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                                plan.id.to_string(),
                            ))),
                            session,
                        );
                        return;
                    }
                };

                if selection.contains_temporal() {
                    tx.send(
                        Err(AdapterError::Unsupported(
                            "calls to mz_logical_timestamp in write statements",
                        )),
                        session,
                    );
                    return;
                }

                let finishing = RowSetFinishing {
                    order_by: vec![],
                    limit: None,
                    offset: 0,
                    project: (0..desc_arity).collect(),
                };

                let read_then_write_plan = ReadThenWritePlan {
                    id: plan.id,
                    selection,
                    finishing,
                    assignments: HashMap::new(),
                    kind: MutationKind::Insert,
                    returning: plan.returning,
                };

                self.sequence_read_then_write(tx, session, read_then_write_plan)
                    .await;
            }
        }
    }

    fn sequence_insert_constant(
        &mut self,
        session: &mut Session,
        id: GlobalId,
        constants: MirRelationExpr,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Insert can be queued, so we need to re-verify the id exists.
        let desc = match self.catalog.try_get_entry(&id) {
            Some(table) => table.desc(
                &self
                    .catalog
                    .resolve_full_name(table.name(), Some(session.conn_id())),
            )?,
            None => {
                return Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                    id.to_string(),
                )))
            }
        };

        match constants {
            MirRelationExpr::Constant { rows, typ: _ } => {
                let rows = rows?;
                for (row, _) in &rows {
                    for (i, datum) in row.iter().enumerate() {
                        desc.constraints_met(i, &datum)?;
                    }
                }
                let diffs_plan = SendDiffsPlan {
                    id,
                    updates: rows,
                    kind: MutationKind::Insert,
                    returning: Vec::new(),
                };
                self.sequence_send_diffs(session, diffs_plan)
            }
            o => panic!(
                "tried using sequence_insert_constant on non-constant MirRelationExpr {:?}",
                o
            ),
        }
    }

    fn sequence_copy_rows(
        &mut self,
        session: &mut Session,
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let catalog = self.catalog.for_session(session);
        let values = mz_sql::plan::plan_copy_from(&session.pcx(), &catalog, id, columns, rows)?;
        let values = self.view_optimizer.optimize(values.lower())?;
        // Copied rows must always be constants.
        self.sequence_insert_constant(session, id, values.into_inner())
    }

    // ReadThenWrite is a plan whose writes depend on the results of a
    // read. This works by doing a Peek then queuing a SendDiffs. No writes
    // or read-then-writes can occur between the Peek and SendDiff otherwise a
    // serializability violation could occur.
    async fn sequence_read_then_write(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: ReadThenWritePlan,
    ) {
        guard_write_critical_section!(self, tx, session, Plan::ReadThenWrite(plan));

        let ReadThenWritePlan {
            id,
            kind,
            selection,
            assignments,
            finishing,
            returning,
        } = plan;

        // Read then writes can be queued, so re-verify the id exists.
        let desc = match self.catalog.try_get_entry(&id) {
            Some(table) => table
                .desc(
                    &self
                        .catalog
                        .resolve_full_name(table.name(), Some(session.conn_id())),
                )
                .expect("desc called on table")
                .into_owned(),
            None => {
                tx.send(
                    Err(AdapterError::SqlCatalog(CatalogError::UnknownItem(
                        id.to_string(),
                    ))),
                    session,
                );
                return;
            }
        };

        // Ensure all objects `selection` depends on are valid for
        // `ReadThenWrite` operations, i.e. they do not refer to any objects
        // whose notion of time moves differently than that of user tables.
        // `true` indicates they're all valid; `false` there are > 0 invalid
        // dependencies.
        //
        // This limitation is meant to ensure no writes occur between this read
        // and the subsequent write.
        fn validate_read_dependencies<S>(catalog: &Catalog<S>, id: &GlobalId) -> bool
        where
            S: mz_stash::Append,
        {
            use CatalogItemType::*;
            match catalog.try_get_entry(id) {
                Some(entry) => match entry.item().typ() {
                    typ @ (Func | View | RecordedView) => {
                        let valid_id = id.is_user() || matches!(typ, Func);
                        valid_id
                            && (
                                // empty `uses` indicates either system func or
                                // view created from constants
                                entry.uses().is_empty()
                                    || entry
                                        .uses()
                                        .iter()
                                        .all(|id| validate_read_dependencies(catalog, id))
                            )
                    }
                    Source | Secret | Connection => false,
                    // Cannot select from sinks or indexes
                    Sink | Index => unreachable!(),
                    Table => id.is_user(),
                    Type => true,
                },
                None => false,
            }
        }

        for id in selection.depends_on() {
            if !validate_read_dependencies(&self.catalog, &id) {
                tx.send(Err(AdapterError::InvalidTableMutationSelection), session);
                return;
            }
        }

        let ts = self.get_local_read_ts();
        let ts = MirScalarExpr::literal_ok(
            Datum::from(Numeric::from(ts)),
            ScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            },
        );
        let peek_response = match self
            .sequence_peek(
                &mut session,
                PeekPlan {
                    source: selection,
                    when: QueryWhen::AtTimestamp(ts),
                    finishing,
                    copy_to: None,
                },
            )
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                tx.send(Err(e), session);
                return;
            }
        };

        let timeout_dur = *session.vars().statement_timeout();

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| format!("sequence_read_then_write:{id}"), async move {
            let arena = RowArena::new();
            let diffs = match peek_response {
                ExecuteResponse::SendingRows {
                    future: batch,
                    span: _,
                } => {
                    // TODO: This timeout should be removed once #11782 lands;
                    // we should instead periodically ensure clusters are
                    // healthy and actively cancel any work waiting on unhealthy
                    // clusters.
                    match tokio::time::timeout(timeout_dur, batch).await {
                        Ok(res) => match res {
                            PeekResponseUnary::Rows(rows) => {
                                |rows: Vec<Row>| -> Result<Vec<(Row, Diff)>, AdapterError> {
                                    // Use 2x row len incase there's some assignments.
                                    let mut diffs = Vec::with_capacity(rows.len() * 2);
                                    let mut datum_vec = mz_repr::DatumVec::new();
                                    for row in rows {
                                        if !assignments.is_empty() {
                                            assert!(
                                                matches!(kind, MutationKind::Update),
                                                "only updates support assignments"
                                            );
                                            let mut datums = datum_vec.borrow_with(&row);
                                            let mut updates = vec![];
                                            for (idx, expr) in &assignments {
                                                let updated = match expr.eval(&datums, &arena) {
                                                    Ok(updated) => updated,
                                                    Err(e) => {
                                                        return Err(AdapterError::Unstructured(
                                                            anyhow!(e),
                                                        ))
                                                    }
                                                };
                                                desc.constraints_met(*idx, &updated)?;
                                                updates.push((*idx, updated));
                                            }
                                            for (idx, new_value) in updates {
                                                datums[idx] = new_value;
                                            }
                                            let updated = Row::pack_slice(&datums);
                                            diffs.push((updated, 1));
                                        }
                                        match kind {
                                            // Updates and deletes always remove the
                                            // current row. Updates will also add an
                                            // updated value.
                                            MutationKind::Update | MutationKind::Delete => {
                                                diffs.push((row, -1))
                                            }
                                            MutationKind::Insert => diffs.push((row, 1)),
                                        }
                                    }
                                    Ok(diffs)
                                }(rows)
                            }
                            PeekResponseUnary::Canceled => {
                                Err(AdapterError::Unstructured(anyhow!("execution canceled")))
                            }
                            PeekResponseUnary::Error(e) => {
                                Err(AdapterError::Unstructured(anyhow!(e)))
                            }
                        },
                        Err(_) => {
                            // We timed out, so remove the pending peek. This is
                            // best-effort and doesn't guarantee we won't
                            // receive a response.
                            internal_cmd_tx
                                .send(Message::RemovePendingPeeks {
                                    conn_id: session.conn_id(),
                                })
                                .expect("sending to internal_cmd_tx cannot fail");
                            Err(AdapterError::StatementTimeout)
                        }
                    }
                }
                _ => Err(AdapterError::Unstructured(anyhow!("expected SendingRows"))),
            };
            let mut returning_rows = Vec::new();
            let mut diff_err: Option<AdapterError> = None;
            if !returning.is_empty() && diffs.is_ok() {
                let arena = RowArena::new();
                for (row, diff) in diffs.as_ref().unwrap() {
                    if diff < &1 {
                        continue;
                    }
                    let mut returning_row = Row::with_capacity(returning.len());
                    let mut packer = returning_row.packer();
                    for expr in &returning {
                        let datums: Vec<_> = row.iter().collect();
                        match expr.eval(&datums, &arena) {
                            Ok(datum) => {
                                packer.push(datum);
                            }
                            Err(err) => {
                                diff_err = Some(err.into());
                                break;
                            }
                        }
                    }
                    let diff = NonZeroI64::try_from(*diff).expect("known to be >= 1");
                    let diff = match NonZeroUsize::try_from(diff) {
                        Ok(diff) => diff,
                        Err(err) => {
                            diff_err = Some(err.into());
                            break;
                        }
                    };
                    returning_rows.push((returning_row, diff));
                    if diff_err.is_some() {
                        break;
                    }
                }
            }
            let diffs = if let Some(err) = diff_err {
                Err(err)
            } else {
                diffs
            };
            internal_cmd_tx
                .send(Message::SendDiffs(SendDiffs {
                    session,
                    tx,
                    id,
                    diffs,
                    kind,
                    returning: returning_rows,
                }))
                .expect("sending to internal_cmd_tx cannot fail");
        });
    }

    async fn sequence_alter_item_rename(
        &mut self,
        session: &Session,
        plan: AlterItemRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::RenameItem {
            id: plan.id,
            current_full_name: plan.current_full_name,
            to_name: plan.to_name,
        };
        match self
            .catalog_transact(Some(session), vec![op], |_| Ok(()))
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(plan.object_type)),
            Err(err) => Err(err),
        }
    }

    async fn sequence_alter_index_set_options(
        &mut self,
        plan: AlterIndexSetOptionsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        self.set_index_options(plan.id, plan.options).await?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    async fn sequence_alter_index_reset_options(
        &mut self,
        plan: AlterIndexResetOptionsPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let mut options = Vec::with_capacity(plan.options.len());
        for o in plan.options {
            options.push(match o {
                IndexOptionName::LogicalCompactionWindow => IndexOption::LogicalCompactionWindow(
                    DEFAULT_LOGICAL_COMPACTION_WINDOW_MS.map(Duration::from_millis),
                ),
            });
        }

        self.set_index_options(plan.id, options).await?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    async fn sequence_alter_secret(
        &mut self,
        session: &Session,
        plan: AlterSecretPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let AlterSecretPlan { id, mut secret_as } = plan;

        let payload = self.extract_secret(session, &mut secret_as)?;

        self.secrets_controller.ensure(id, &payload).await?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Secret))
    }

    fn extract_secret(
        &mut self,
        session: &Session,
        mut secret_as: &mut MirScalarExpr,
    ) -> Result<Vec<u8>, AdapterError> {
        let temp_storage = RowArena::new();
        prep_scalar_expr(
            self.catalog.state(),
            &mut secret_as,
            ExprPrepStyle::OneShot {
                logical_time: None,
                session,
            },
        )?;
        let evaled = secret_as.eval(&[], &temp_storage)?;

        if evaled == Datum::Null {
            coord_bail!("secret value can not be null");
        }

        let payload = evaled.unwrap_bytes();

        // Limit the size of a secret to 512 KiB
        // This is the largest size of a single secret in Consul/Kubernetes
        // We are enforcing this limit across all types of Secrets Controllers
        // Most secrets are expected to be roughly 75B
        if payload.len() > 1024 * 512 {
            coord_bail!("secrets can not be bigger than 512KiB")
        }

        return Ok(Vec::from(payload));
    }

    /// Perform a catalog transaction. The closure is passed a [`CatalogTxn`]
    /// made from the prospective [`CatalogState`] (i.e., the `Catalog` with `ops`
    /// applied but before the transaction is committed). The closure can return
    /// an error to abort the transaction, or otherwise return a value that is
    /// returned by this function. This allows callers to error while building
    /// [`DataflowDesc`]s. [`Coordinator::ship_dataflow`] must be called after this
    /// function successfully returns on any built `DataflowDesc`.
    ///
    /// [`CatalogState`]: crate::catalog::CatalogState
    #[tracing::instrument(level = "debug", skip_all)]
    async fn catalog_transact<F, R>(
        &mut self,
        session: Option<&Session>,
        mut ops: Vec<catalog::Op>,
        f: F,
    ) -> Result<R, AdapterError>
    where
        F: FnOnce(CatalogTxn<Timestamp>) -> Result<R, AdapterError>,
    {
        event!(Level::TRACE, ops = format!("{:?}", ops));

        let mut sources_to_drop = vec![];
        let mut tables_to_drop = vec![];
        let mut sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut recorded_views_to_drop = vec![];
        let mut replication_slots_to_drop: Vec<(tokio_postgres::Config, String)> = vec![];
        let mut secrets_to_drop = vec![];

        for op in &ops {
            if let catalog::Op::DropItem(id) = op {
                match self.catalog.get_entry(id).item() {
                    CatalogItem::Table(_) => {
                        tables_to_drop.push(*id);
                    }
                    CatalogItem::Source(source) => {
                        sources_to_drop.push(*id);
                        match &source.source_desc.connection {
                            SourceConnection::Postgres(PostgresSourceConnection {
                                connection,
                                details,
                                ..
                            }) => {
                                let config = connection
                                    .config(&*self.connection_context.secrets_reader)
                                    .await
                                    .unwrap_or_else(|e| {
                                        panic!("Postgres source {id} missing secrets: {e}")
                                    });
                                replication_slots_to_drop.push((config, details.slot.clone()));
                            }
                            _ => {}
                        }
                    }
                    CatalogItem::Sink(catalog::Sink {
                        connection: SinkConnectionState::Ready(_),
                        compute_instance,
                        ..
                    }) => {
                        sinks_to_drop.push((*compute_instance, *id));
                    }
                    CatalogItem::Index(catalog::Index {
                        compute_instance, ..
                    }) => {
                        indexes_to_drop.push((*compute_instance, *id));
                    }
                    CatalogItem::RecordedView(catalog::RecordedView {
                        compute_instance, ..
                    }) => {
                        recorded_views_to_drop.push((*compute_instance, *id));
                    }
                    CatalogItem::Secret(_) => {
                        secrets_to_drop.push(*id);
                    }
                    _ => (),
                }
            }
        }

        let mut empty_timelines = self.remove_global_read_holds_storage(
            sources_to_drop
                .iter()
                .chain(tables_to_drop.iter())
                .chain(recorded_views_to_drop.iter().map(|(_, id)| id))
                .cloned(),
        );
        empty_timelines.extend(
            self.remove_global_read_holds_compute(
                sinks_to_drop
                    .iter()
                    .chain(indexes_to_drop.iter())
                    .chain(recorded_views_to_drop.iter())
                    .cloned(),
            ),
        );
        ops.extend(empty_timelines.into_iter().map(catalog::Op::DropTimeline));

        let (builtin_table_updates, result) = self
            .catalog
            .transact(session, ops, |catalog| {
                f(CatalogTxn {
                    dataflow_client: &self.controller,
                    catalog,
                })
            })
            .await?;

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        let _: () = async {
            self.send_builtin_table_updates(builtin_table_updates).await;

            if !sources_to_drop.is_empty() {
                self.drop_sources(sources_to_drop).await;
            }
            if !tables_to_drop.is_empty() {
                self.drop_sources(tables_to_drop).await;
            }
            if !sinks_to_drop.is_empty() {
                self.drop_sinks(sinks_to_drop).await;
            }
            if !indexes_to_drop.is_empty() {
                self.drop_indexes(indexes_to_drop).await;
            }
            if !recorded_views_to_drop.is_empty() {
                self.drop_recorded_views(recorded_views_to_drop).await;
            }
            if !secrets_to_drop.is_empty() {
                self.drop_secrets(secrets_to_drop).await;
            }

            // We don't want to block the coordinator on an external postgres server, so
            // move the drop slots to a separate task. This does mean that a failed drop
            // slot won't bubble up to the user as an error message. However, even if it
            // did (and how the code previously worked), mz has already dropped it from our
            // catalog, and so we wouldn't be able to retry anyway.
            if !replication_slots_to_drop.is_empty() {
                // TODO(guswynn): see if there is more relevant info to add to this name
                task::spawn(|| "drop_replication_slots", async move {
                    for (config, slot_name) in replication_slots_to_drop {
                        // Try to drop the replication slots, but give up after a while.
                        let _ = Retry::default()
                            .max_duration(Duration::from_secs(30))
                            .retry_async(|_state| async {
                                mz_postgres_util::drop_replication_slots(
                                    config.clone(),
                                    &[&slot_name],
                                )
                                .await
                            })
                            .await;
                    }
                });
            }
        }
        .await;

        Ok(result)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(updates = updates.len()))]
    async fn send_builtin_table_updates(&mut self, updates: Vec<BuiltinTableUpdate>) {
        // Most DDL queries cause writes to system tables. Unlike writes to user tables, system
        // table writes are not batched in a group commit. This is mostly due to the complexity
        // around checking for conflicting DDL at commit time. There is a possibility that if a user
        // is executing DDL at a rate faster than 1 query per millisecond, then the global timeline
        // will unboundedly advance past the system clock. This can cause future queries to block,
        // but will not affect correctness. Since this rate of DDL is unlikely, we are leaving DDL
        // related writes out of group commits for now.
        //
        // In the future we can add these write to group commit by:
        //  1. Checking for conflicts at commit time and aborting conflicting DDL.
        //  2. Delaying modifications to on-disk and in-memory catalog until commit time.
        let WriteTimestamp {
            timestamp,
            advance_to,
        } = self.get_and_step_local_write_ts().await;
        let mut appends: HashMap<GlobalId, Vec<(Row, Diff)>> = HashMap::new();
        for u in updates {
            appends.entry(u.id).or_default().push((u.row, u.diff));
        }
        for (_, updates) in &mut appends {
            differential_dataflow::consolidation::consolidate(updates);
        }
        let appends = appends
            .into_iter()
            .map(|(id, updates)| {
                let updates = updates
                    .into_iter()
                    .map(|(row, diff)| Update {
                        row,
                        diff,
                        timestamp,
                    })
                    .collect();
                (id, updates, advance_to)
            })
            .collect();
        self.controller.storage_mut().append(appends).await.unwrap();
    }

    async fn drop_sources(&mut self, sources: Vec<GlobalId>) {
        for id in &sources {
            self.read_capability.remove(id);
        }
        self.controller
            .storage_mut()
            .drop_sources(sources)
            .await
            .unwrap();
    }

    async fn drop_sinks(&mut self, sinks: Vec<(ComputeInstanceId, GlobalId)>) {
        // TODO(chae): Drop storage sinks when they're moved over
        let by_compute_instance = sinks.into_iter().into_group_map();
        for (compute_instance, ids) in by_compute_instance {
            // A cluster could have been dropped, so verify it exists.
            if let Some(mut compute) = self.controller.compute_mut(compute_instance) {
                compute.drop_sinks(ids).await.unwrap();
            }
        }
    }

    async fn drop_indexes(&mut self, indexes: Vec<(ComputeInstanceId, GlobalId)>) {
        let mut by_compute_instance = HashMap::new();
        for (compute_instance, id) in indexes {
            if self.read_capability.remove(&id).is_some() {
                by_compute_instance
                    .entry(compute_instance)
                    .or_insert(vec![])
                    .push(id);
            } else {
                tracing::error!("Instructed to drop a non-index index");
            }
        }
        for (compute_instance, ids) in by_compute_instance {
            self.controller
                .compute_mut(compute_instance)
                .unwrap()
                .drop_indexes(ids)
                .await
                .unwrap();
        }
    }

    async fn drop_recorded_views(&mut self, rviews: Vec<(ComputeInstanceId, GlobalId)>) {
        let mut by_compute_instance = HashMap::new();
        let mut source_ids = Vec::new();
        for (compute_instance, id) in rviews {
            if self.read_capability.remove(&id).is_some() {
                by_compute_instance
                    .entry(compute_instance)
                    .or_insert(vec![])
                    .push(id);
                source_ids.push(id);
            } else {
                tracing::error!("Instructed to drop a recorded view that isn't one");
            }
        }

        // Drop compute sinks.
        // TODO(chae): Drop storage sinks when they're moved over
        for (compute_instance, ids) in by_compute_instance {
            // A cluster could have been dropped, so verify it exists.
            if let Some(mut compute) = self.controller.compute_mut(compute_instance) {
                compute.drop_sinks(ids).await.unwrap();
            }
        }

        // Drop storage sources.
        self.controller
            .storage_mut()
            .drop_sources(source_ids)
            .await
            .unwrap();
    }

    fn remove_global_read_holds_storage<I>(&mut self, ids: I) -> Vec<Timeline>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let mut empty_timelines = Vec::new();
        for id in ids {
            if let Some(timeline) = self.get_timeline(id) {
                let TimelineState { read_holds, .. } = self
                    .global_timelines
                    .get_mut(&timeline)
                    .expect("all timelines have a timestamp oracle");
                read_holds.id_bundle.storage_ids.remove(&id);
                if read_holds.id_bundle.is_empty() {
                    self.global_timelines.remove(&timeline);
                    empty_timelines.push(timeline);
                }
            }
        }
        empty_timelines
    }

    fn remove_global_read_holds_compute<I>(&mut self, ids: I) -> Vec<Timeline>
    where
        I: IntoIterator<Item = (ComputeInstanceId, GlobalId)>,
    {
        let mut empty_timelines = Vec::new();
        for (compute_instance, id) in ids {
            if let Some(timeline) = self.get_timeline(id) {
                let TimelineState { read_holds, .. } = self
                    .global_timelines
                    .get_mut(&timeline)
                    .expect("all timelines have a timestamp oracle");
                if let Some(ids) = read_holds.id_bundle.compute_ids.get_mut(&compute_instance) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        read_holds.id_bundle.compute_ids.remove(&compute_instance);
                    }
                    if read_holds.id_bundle.is_empty() {
                        self.global_timelines.remove(&timeline);
                        empty_timelines.push(timeline);
                    }
                }
            }
        }
        empty_timelines
    }

    async fn set_index_options(
        &mut self,
        id: GlobalId,
        options: Vec<IndexOption>,
    ) -> Result<(), AdapterError> {
        let needs = self
            .read_capability
            .get_mut(&id)
            .expect("coord indexes out of sync");

        for o in options {
            match o {
                IndexOption::LogicalCompactionWindow(window) => {
                    // The index is on a specific compute instance.
                    let compute_instance = self
                        .catalog
                        .get_entry(&id)
                        .index()
                        .expect("setting options on index")
                        .compute_instance;
                    let window = window.map(duration_to_timestamp_millis);
                    let policy = match window {
                        Some(time) => ReadPolicy::lag_writes_by(time),
                        None => ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum())),
                    };
                    needs.base_policy = policy;
                    self.controller
                        .compute_mut(compute_instance)
                        .unwrap()
                        .set_read_policy(vec![(id, needs.policy())])
                        .await
                        .unwrap();
                }
            }
        }
        Ok(())
    }

    async fn drop_secrets(&mut self, secrets: Vec<GlobalId>) {
        for secret in secrets {
            if let Err(e) = self.secrets_controller.delete(secret).await {
                warn!("Dropping secrets has encountered an error: {}", e);
            }
        }
    }

    /// Finalizes a dataflow and then broadcasts it to all workers.
    /// Utility method for the more general [Self::ship_dataflows]
    async fn ship_dataflow(&mut self, dataflow: DataflowDesc, instance: ComputeInstanceId) {
        self.ship_dataflows(vec![dataflow], instance).await
    }

    /// Finalizes a list of dataflows and then broadcasts it to all workers.
    async fn ship_dataflows(&mut self, dataflows: Vec<DataflowDesc>, instance: ComputeInstanceId) {
        let mut output_ids = Vec::new();
        let mut dataflow_plans = Vec::with_capacity(dataflows.len());
        for dataflow in dataflows.into_iter() {
            output_ids.extend(dataflow.export_ids());
            dataflow_plans.push(self.finalize_dataflow(dataflow, instance));
        }
        self.controller
            .compute_mut(instance)
            .unwrap()
            .create_dataflows(dataflow_plans)
            .await
            .unwrap();
        self.initialize_compute_read_policies(
            output_ids,
            instance,
            DEFAULT_LOGICAL_COMPACTION_WINDOW_MS,
        )
        .await;
    }

    /// Finalizes a dataflow.
    ///
    /// Finalization includes optimization, but also validation of various
    /// invariants such as ensuring that the `as_of` frontier is in advance of
    /// the various `since` frontiers of participating data inputs.
    ///
    /// In particular, there are requirement on the `as_of` field for the dataflow
    /// and the `since` frontiers of created arrangements, as a function of the `since`
    /// frontiers of dataflow inputs (sources and imported arrangements).
    ///
    /// # Panics
    ///
    /// Panics if as_of is < the `since` frontiers.
    ///
    /// Panics if the dataflow descriptions contain an invalid plan.
    fn finalize_dataflow(
        &self,
        mut dataflow: DataflowDesc,
        compute_instance: ComputeInstanceId,
    ) -> DataflowDescription<mz_compute_client::plan::Plan> {
        // This function must succeed because catalog_transact has generally been run
        // before calling this function. We don't have plumbing yet to rollback catalog
        // operations if this function fails, and environmentd will be in an unsafe
        // state if we do not correctly clean up the catalog.

        let storage_ids = dataflow
            .source_imports
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        let compute_ids = dataflow
            .index_imports
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();

        let compute_ids = vec![(compute_instance, compute_ids)].into_iter().collect();
        let since = self.least_valid_read(&CollectionIdBundle {
            storage_ids,
            compute_ids,
        });

        // Ensure that the dataflow's `as_of` is at least `since`.
        if let Some(as_of) = &mut dataflow.as_of {
            // It should not be possible to request an invalid time. SINK doesn't support
            // AS OF. TAIL and Peek check that their AS OF is >= since.
            assert!(
                <_ as PartialOrder>::less_equal(&since, as_of),
                "Dataflow {} requested as_of ({:?}) not >= since ({:?})",
                dataflow.debug_name,
                as_of,
                since
            );
        } else {
            // Bind the since frontier to the dataflow description.
            dataflow.set_as_of(since);
        }

        mz_compute_client::plan::Plan::finalize_dataflow(dataflow)
            .expect("Dataflow planning failed; unrecoverable error")
    }

    fn allocate_transient_id(&mut self) -> Result<GlobalId, AdapterError> {
        let id = self.transient_id_counter;
        if id == u64::max_value() {
            coord_bail!("id counter overflows i64");
        }
        self.transient_id_counter += 1;
        Ok(GlobalId::Transient(id))
    }

    /// Return an error if the ids are from incompatible timelines. This should
    /// be used to prevent users from doing things that are either meaningless
    /// (joining data from timelines that have similar numbers with different
    /// meanings like two separate debezium topics) or will never complete (joining
    /// cdcv2 and realtime data).
    fn validate_timeline<I>(&self, ids: I) -> Result<Option<Timeline>, AdapterError>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let timelines = self.get_timelines(ids);
        // If there's more than one timeline, we will not produce meaningful
        // data to a user. Take, for example, some realtime source and a debezium
        // consistency topic source. The realtime source uses something close to now
        // for its timestamps. The debezium source starts at 1 and increments per
        // transaction. We don't want to choose some timestamp that is valid for both
        // of these because the debezium source will never get to the same value as the
        // realtime source's "milliseconds since Unix epoch" value. And even if it did,
        // it's not meaningful to join just because those two numbers happen to be the
        // same now.
        //
        // Another example: assume two separate debezium consistency topics. Both
        // start counting at 1 and thus have similarish numbers that probably overlap
        // a lot. However it's still not meaningful to join those two at a specific
        // transaction counter number because those counters are unrelated to the
        // other.
        if timelines.len() > 1 {
            return Err(AdapterError::Unsupported(
                "multiple timelines within one dataflow",
            ));
        }
        Ok(timelines.into_iter().next())
    }

    /// Return the timeline belonging to a GlobalId, if one exists.
    fn get_timeline(&self, id: GlobalId) -> Option<Timeline> {
        let timelines = self.get_timelines(vec![id]);
        assert!(
            timelines.len() <= 1,
            "impossible for a single object to belong to two timelines"
        );
        timelines.into_iter().next()
    }

    /// Return the timelines belonging to a list of GlobalIds, if any exist.
    fn get_timelines<I>(&self, ids: I) -> HashSet<Timeline>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let mut timelines: HashMap<GlobalId, Timeline> = HashMap::new();

        // Recurse through IDs to find all sources and tables, adding new ones to
        // the set until we reach the bottom. Static views will end up with an empty
        // timelines.
        let mut ids: Vec<_> = ids.into_iter().collect();
        while let Some(id) = ids.pop() {
            // Protect against possible infinite recursion. Not sure if it's possible, but
            // a cheap prevention for the future.
            if timelines.contains_key(&id) {
                continue;
            }
            if let Some(entry) = self.catalog.try_get_entry(&id) {
                match entry.item() {
                    CatalogItem::Source(source) => {
                        timelines.insert(id, source.timeline.clone());
                    }
                    CatalogItem::Index(index) => {
                        ids.push(index.on);
                    }
                    CatalogItem::View(view) => {
                        ids.extend(view.optimized_expr.depends_on());
                    }
                    CatalogItem::RecordedView(rview) => {
                        ids.extend(rview.optimized_expr.depends_on());
                    }
                    CatalogItem::Table(table) => {
                        timelines.insert(id, table.timeline());
                    }
                    CatalogItem::Log(_) => {
                        timelines.insert(id, Timeline::EpochMilliseconds);
                    }
                    CatalogItem::StorageCollection(_) => {
                        timelines.insert(id, Timeline::EpochMilliseconds);
                    }
                    _ => {}
                }
            }
        }

        timelines
            .into_iter()
            .map(|(_, timeline)| timeline)
            .collect()
    }

    /// Attempts to immediately grant `session` access to the write lock or
    /// errors if the lock is currently held.
    fn try_grant_session_write_lock(
        &self,
        session: &mut Session,
    ) -> Result<(), tokio::sync::TryLockError> {
        Arc::clone(&self.write_lock).try_lock_owned().map(|p| {
            session.grant_write_lock(p);
        })
    }

    /// Defers executing `deferred` until the write lock becomes available; waiting
    /// occurs in a green-thread, so callers of this function likely want to
    /// return after calling it.
    fn defer_write(&mut self, deferred: Deferred) {
        let id = match &deferred {
            Deferred::Plan(plan) => plan.session.conn_id().to_string(),
            Deferred::GroupCommit => "group_commit".to_string(),
        };
        self.write_lock_wait_group.push_back(deferred);

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let write_lock = Arc::clone(&self.write_lock);
        // TODO(guswynn): see if there is more relevant info to add to this name
        task::spawn(|| format!("defer_write:{id}"), async move {
            let guard = write_lock.lock_owned().await;
            internal_cmd_tx
                .send(Message::WriteLockGrant(guard))
                .expect("sending to internal_cmd_tx cannot fail");
        });
    }
}

/// Serves the coordinator based on the provided configuration.
///
/// For a high-level description of the coordinator, see the [crate
/// documentation](crate).
///
/// Returns a handle to the coordinator and a client to communicate with the
/// coordinator.
pub async fn serve<S: Append + 'static>(
    Config {
        dataflow_client,
        storage,
        unsafe_mode,
        build_info,
        metrics_registry,
        now,
        secrets_controller,
        replica_sizes,
        availability_zones,
        connection_context,
    }: Config<S>,
) -> Result<(Handle, Client), AdapterError> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();

    let (mut catalog, builtin_table_updates) = Catalog::open(catalog::Config {
        storage,
        unsafe_mode,
        build_info,
        now: now.clone(),
        skip_migrations: false,
        metrics_registry: &metrics_registry,
        replica_sizes,
        availability_zones,
    })
    .await?;
    let cluster_id = catalog.config().cluster_id;
    let session_id = catalog.config().session_id;
    let start_instant = catalog.config().start_instant;

    // In order for the coordinator to support Rc and Refcell types, it cannot be
    // sent across threads. Spawn it in a thread and have this parent thread wait
    // for bootstrap completion before proceeding.
    let (bootstrap_tx, bootstrap_rx) = oneshot::channel();
    let handle = TokioHandle::current();

    let initial_timestamps = catalog.get_all_persisted_timestamps().await?;
    let thread = thread::Builder::new()
        // The Coordinator thread tends to keep a lot of data on its stack. To
        // prevent a stack overflow we allocate a stack twice as big as the default
        // stack.
        .stack_size(2 * stack::STACK_SIZE)
        .name("coordinator".to_string())
        .spawn(move || {
            let mut timestamp_oracles = BTreeMap::new();
            for (timeline, initial_timestamp) in initial_timestamps {
                let oracle = if timeline == Timeline::EpochMilliseconds {
                    let now = now.clone();
                    handle.block_on(timeline::DurableTimestampOracle::new(
                        initial_timestamp,
                        move || (*&(now))(),
                        *timeline::TIMESTAMP_PERSIST_INTERVAL,
                        |ts| catalog.persist_timestamp(&timeline, ts),
                    ))
                } else {
                    handle.block_on(timeline::DurableTimestampOracle::new(
                        initial_timestamp,
                        Timestamp::minimum,
                        *timeline::TIMESTAMP_PERSIST_INTERVAL,
                        |ts| catalog.persist_timestamp(&timeline, ts),
                    ))
                };
                timestamp_oracles.insert(
                    timeline,
                    TimelineState {
                        oracle,
                        read_holds: ReadHolds::new(initial_timestamp),
                    },
                );
            }

            let mut coord = Coordinator {
                controller: dataflow_client,
                view_optimizer: Optimizer::logical_optimizer(),
                catalog,
                internal_cmd_tx,
                global_timelines: timestamp_oracles,
                advance_tables: AdvanceTables::new(),
                transient_id_counter: 1,
                active_conns: HashMap::new(),
                read_capability: Default::default(),
                txn_reads: Default::default(),
                pending_peeks: HashMap::new(),
                client_pending_peeks: HashMap::new(),
                pending_tails: HashMap::new(),
                write_lock: Arc::new(tokio::sync::Mutex::new(())),
                write_lock_wait_group: VecDeque::new(),
                pending_writes: Vec::new(),
                secrets_controller,
                connection_context,
                transient_replica_metadata: HashMap::new(),
            };
            let bootstrap = handle.block_on(coord.bootstrap(builtin_table_updates));
            let ok = bootstrap.is_ok();
            bootstrap_tx.send(bootstrap).unwrap();
            if ok {
                handle.block_on(coord.serve(internal_cmd_rx, cmd_rx));
            }
        })
        .unwrap();
    match bootstrap_rx.await.unwrap() {
        Ok(()) => {
            let handle = Handle {
                cluster_id,
                session_id,
                start_instant,
                _thread: thread.join_on_drop(),
            };
            let client = Client::new(cmd_tx);
            Ok((handle, client))
        }
        Err(e) => Err(e),
    }
}

/// Constructs an [`ExecuteResponse`] that that will send some rows to the
/// client immediately, as opposed to asking the dataflow layer to send along
/// the rows after some computation.
fn send_immediate_rows(rows: Vec<Row>) -> ExecuteResponse {
    ExecuteResponse::SendingRows {
        future: Box::pin(async { PeekResponseUnary::Rows(rows) }),
        span: tracing::Span::none(),
    }
}

fn auto_generate_primary_idx(
    index_name: String,
    compute_instance: ComputeInstanceId,
    on_name: FullObjectName,
    on_id: GlobalId,
    on_desc: &RelationDesc,
    conn_id: Option<ConnectionId>,
    depends_on: Vec<GlobalId>,
) -> catalog::Index {
    let default_key = on_desc.typ().default_key();
    catalog::Index {
        create_sql: index_sql(
            index_name,
            compute_instance,
            on_name,
            &on_desc,
            &default_key,
        ),
        on: on_id,
        keys: default_key
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect(),
        conn_id,
        depends_on,
        compute_instance,
    }
}

// TODO(benesch): constructing the canonical CREATE INDEX statement should be
// the responsibility of the SQL package.
pub fn index_sql(
    index_name: String,
    compute_instance: ComputeInstanceId,
    view_name: FullObjectName,
    view_desc: &RelationDesc,
    keys: &[usize],
) -> String {
    use mz_sql::ast::{Expr, Value};

    CreateIndexStatement::<Raw> {
        name: Some(Ident::new(index_name)),
        on_name: RawObjectName::Name(mz_sql::normalize::unresolve(view_name)),
        in_cluster: Some(RawClusterName::Resolved(compute_instance.to_string())),
        key_parts: Some(
            keys.iter()
                .map(|i| match view_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![Ident::new(n.to_string())]),
                    _ => Expr::Value(Value::Number((i + 1).to_string())),
                })
                .collect(),
        ),
        with_options: vec![],
        if_not_exists: false,
    }
    .to_ast_string_stable()
}

/// Converts a Duration to a Timestamp representing the number
/// of milliseconds contained in that Duration
fn duration_to_timestamp_millis(d: Duration) -> Timestamp {
    let millis = d.as_millis();
    if millis > Timestamp::max_value() as u128 {
        Timestamp::max_value()
    } else if millis < Timestamp::min_value() as u128 {
        Timestamp::min_value()
    } else {
        millis as Timestamp
    }
}

/// Creates a description of the statement `stmt`.
///
/// This function is identical to sql::plan::describe except this is also
/// supports describing FETCH statements which need access to bound portals
/// through the session.
pub fn describe<S: Append>(
    catalog: &Catalog<S>,
    stmt: Statement<Raw>,
    param_types: &[Option<ScalarType>],
    session: &Session,
) -> Result<StatementDesc, AdapterError> {
    match stmt {
        // FETCH's description depends on the current session, which describe_statement
        // doesn't (and shouldn't?) have access to, so intercept it here.
        Statement::Fetch(FetchStatement { ref name, .. }) => {
            // Unverified portal is ok here because Coordinator::execute will verify the
            // named portal during execution.
            match session
                .get_portal_unverified(name.as_str())
                .map(|p| p.desc.clone())
            {
                Some(desc) => Ok(desc),
                None => Err(AdapterError::UnknownCursor(name.to_string())),
            }
        }
        _ => {
            let catalog = &catalog.for_session(session);
            let (stmt, _) = mz_sql::names::resolve(catalog, stmt)?;
            Ok(mz_sql::plan::describe(
                &session.pcx(),
                catalog,
                stmt,
                param_types,
            )?)
        }
    }
}

/// Logic and types for fast-path determination for dataflow execution.
///
/// This module determines if a dataflow can be short-cut, by returning constant values
/// or by reading out of existing arrangements, and implements the appropriate plan.
pub mod fast_path_peek {
    use std::{collections::HashMap, num::NonZeroUsize};

    use futures::{FutureExt, StreamExt};
    use uuid::Uuid;

    use mz_compute_client::command::{DataflowDescription, ReplicaId};
    use mz_compute_client::controller::ComputeInstanceId;
    use mz_compute_client::response::PeekResponse;
    use mz_expr::{EvalError, Id, MirScalarExpr};
    use mz_repr::{Diff, GlobalId, Row};
    use mz_stash::Append;

    use crate::client::ConnectionId;
    use crate::coord::{PeekResponseUnary, PendingPeek};
    use crate::AdapterError;

    #[derive(Debug)]
    pub struct PeekDataflowPlan<T> {
        desc: DataflowDescription<mz_compute_client::plan::Plan<T>, (), T>,
        id: GlobalId,
        key: Vec<MirScalarExpr>,
        permutation: HashMap<usize, usize>,
        thinned_arity: usize,
    }

    /// Possible ways in which the coordinator could produce the result for a goal view.
    #[derive(Debug)]
    pub enum Plan<T = mz_repr::Timestamp> {
        /// The view evaluates to a constant result that can be returned.
        Constant(Result<Vec<(Row, T, Diff)>, EvalError>),
        /// The view can be read out of an existing arrangement.
        PeekExisting(GlobalId, Option<Row>, mz_expr::SafeMfpPlan),
        /// The view must be installed as a dataflow and then read.
        PeekDataflow(PeekDataflowPlan<T>),
    }

    /// Determine if the dataflow plan can be implemented without an actual dataflow.
    ///
    /// If the optimized plan is a `Constant` or a `Get` of a maintained arrangement,
    /// we can avoid building a dataflow (and either just return the results, or peek
    /// out of the arrangement, respectively).
    pub fn create_plan(
        dataflow_plan: DataflowDescription<mz_compute_client::plan::Plan>,
        view_id: GlobalId,
        index_id: GlobalId,
        index_key: Vec<MirScalarExpr>,
        index_permutation: HashMap<usize, usize>,
        index_thinned_arity: usize,
    ) -> Result<Plan, AdapterError> {
        // At this point, `dataflow_plan` contains our best optimized dataflow.
        // We will check the plan to see if there is a fast path to escape full dataflow construction.

        // We need to restrict ourselves to settings where the inserted transient view is the first thing
        // to build (no dependent views). There is likely an index to build as well, but we may not be sure.
        if dataflow_plan.objects_to_build.len() >= 1
            && dataflow_plan.objects_to_build[0].id == view_id
        {
            match &dataflow_plan.objects_to_build[0].plan {
                // In the case of a constant, we can return the result now.
                mz_compute_client::plan::Plan::Constant { rows } => {
                    return Ok(Plan::Constant(rows.clone()));
                }
                // In the case of a bare `Get`, we may be able to directly index an arrangement.
                mz_compute_client::plan::Plan::Get { id, keys, plan } => {
                    match plan {
                        mz_compute_client::plan::GetPlan::PassArrangements => {
                            // An arrangement may or may not exist. If not, nothing to be done.
                            if let Some((key, permute, thinning)) = keys.arbitrary_arrangement() {
                                // Just grab any arrangement, but be sure to de-permute the results.
                                for (index_id, (desc, _typ, _monotonic)) in
                                    dataflow_plan.index_imports.iter()
                                {
                                    if Id::Global(desc.on_id) == *id && &desc.key == key {
                                        let mut map_filter_project =
                                            mz_expr::MapFilterProject::new(_typ.arity())
                                                .into_plan()
                                                .unwrap()
                                                .into_nontemporal()
                                                .unwrap();
                                        map_filter_project
                                            .permute(permute.clone(), key.len() + thinning.len());
                                        return Ok(Plan::PeekExisting(
                                            *index_id,
                                            None,
                                            map_filter_project,
                                        ));
                                    }
                                }
                            }
                        }
                        mz_compute_client::plan::GetPlan::Arrangement(key, val, mfp) => {
                            // Convert `mfp` to an executable, non-temporal plan.
                            // It should be non-temporal, as OneShot preparation populates `mz_logical_timestamp`.
                            let map_filter_project = mfp
                                .clone()
                                .into_plan()
                                .map_err(|e| {
                                    crate::error::AdapterError::Unstructured(::anyhow::anyhow!(e))
                                })?
                                .into_nontemporal()
                                .map_err(|_e| {
                                    crate::error::AdapterError::Unstructured(::anyhow::anyhow!(
                                        "OneShot plan has temporal constraints"
                                    ))
                                })?;
                            // We should only get excited if we can track down an index for `id`.
                            // If `keys` is non-empty, that means we think one exists.
                            for (index_id, (desc, _typ, _monotonic)) in
                                dataflow_plan.index_imports.iter()
                            {
                                if Id::Global(desc.on_id) == *id && &desc.key == key {
                                    // Indicate an early exit with a specific index and key_val.
                                    return Ok(Plan::PeekExisting(
                                        *index_id,
                                        val.clone(),
                                        map_filter_project,
                                    ));
                                }
                            }
                        }
                        mz_compute_client::plan::GetPlan::Collection(_) => {
                            // No arrangement, so nothing to be done here.
                        }
                    }
                }
                // nothing can be done for non-trivial expressions.
                _ => {}
            }
        }
        return Ok(Plan::PeekDataflow(PeekDataflowPlan {
            desc: dataflow_plan,
            id: index_id,
            key: index_key,
            permutation: index_permutation,
            thinned_arity: index_thinned_arity,
        }));
    }

    impl<S: Append + 'static> crate::coord::Coordinator<S> {
        /// Implements a peek plan produced by `create_plan` above.
        #[tracing::instrument(level = "debug", skip(self))]
        pub async fn implement_fast_path_peek(
            &mut self,
            fast_path: Plan,
            timestamp: mz_repr::Timestamp,
            finishing: mz_expr::RowSetFinishing,
            conn_id: ConnectionId,
            source_arity: usize,
            compute_instance: ComputeInstanceId,
            target_replica: Option<ReplicaId>,
        ) -> Result<crate::ExecuteResponse, AdapterError> {
            // If the dataflow optimizes to a constant expression, we can immediately return the result.
            if let Plan::Constant(rows) = fast_path {
                let mut rows = match rows {
                    Ok(rows) => rows,
                    Err(e) => return Err(e.into()),
                };
                // retain exactly those updates less or equal to `timestamp`.
                for (_, time, diff) in rows.iter_mut() {
                    use timely::PartialOrder;
                    if time.less_equal(&timestamp) {
                        // clobber the timestamp, so consolidation occurs.
                        *time = timestamp.clone();
                    } else {
                        // zero the difference, to prevent a contribution.
                        *diff = 0;
                    }
                }
                // Consolidate down the results to get correct totals.
                differential_dataflow::consolidation::consolidate_updates(&mut rows);

                let mut results = Vec::new();
                for (row, _time, count) in rows {
                    if count < 0 {
                        Err(EvalError::InvalidParameterValue(format!(
                            "Negative multiplicity in constant result: {}",
                            count
                        )))?
                    };
                    if count > 0 {
                        results.push((row, NonZeroUsize::new(count as usize).unwrap()));
                    }
                }
                let results = finishing.finish(results);
                return Ok(crate::coord::send_immediate_rows(results));
            }

            // The remaining cases are a peek into a maintained arrangement, or building a dataflow.
            // In both cases we will want to peek, and the main difference is that we might want to
            // build a dataflow and drop it once the peek is issued. The peeks are also constructed
            // differently.

            // If we must build the view, ship the dataflow.
            let (peek_command, drop_dataflow) = match fast_path {
                Plan::PeekExisting(id, key, map_filter_project) => (
                    (id, key, timestamp, finishing.clone(), map_filter_project),
                    None,
                ),
                Plan::PeekDataflow(PeekDataflowPlan {
                    desc: dataflow,
                    // n.b. this index_id identifies a transient index the
                    // caller created, so it is guaranteed to be on
                    // `compute_instance`.
                    id: index_id,
                    key: index_key,
                    permutation: index_permutation,
                    thinned_arity: index_thinned_arity,
                }) => {
                    let output_ids = dataflow.export_ids().collect();

                    // Very important: actually create the dataflow (here, so we can destructure).
                    self.controller
                        .compute_mut(compute_instance)
                        .unwrap()
                        .create_dataflows(vec![dataflow])
                        .await
                        .unwrap();
                    self.initialize_compute_read_policies(
                        output_ids,
                        compute_instance,
                        // Disable compaction by using None as the compaction window so that nothing
                        // can compact before the peek occurs below.
                        None,
                    )
                    .await;

                    // Create an identity MFP operator.
                    let mut map_filter_project = mz_expr::MapFilterProject::new(source_arity);
                    map_filter_project
                        .permute(index_permutation, index_key.len() + index_thinned_arity);
                    let map_filter_project = map_filter_project
                        .into_plan()
                        .map_err(|e| {
                            crate::error::AdapterError::Unstructured(::anyhow::anyhow!(e))
                        })?
                        .into_nontemporal()
                        .map_err(|_e| {
                            crate::error::AdapterError::Unstructured(::anyhow::anyhow!(
                                "OneShot plan has temporal constraints"
                            ))
                        })?;
                    (
                        (
                            index_id, // transient identifier produced by `dataflow_plan`.
                            None,
                            timestamp,
                            finishing.clone(),
                            map_filter_project,
                        ),
                        Some(index_id),
                    )
                }
                _ => {
                    unreachable!()
                }
            };

            // Endpoints for sending and receiving peek responses.
            let (rows_tx, rows_rx) = tokio::sync::mpsc::unbounded_channel();

            // Generate unique UUID. Guaranteed to be unique to all pending peeks, there's an very
            // small but unlikely chance that it's not unique to completed peeks.
            let mut uuid = Uuid::new_v4();
            while self.pending_peeks.contains_key(&uuid) {
                uuid = Uuid::new_v4();
            }

            // The peek is ready to go for both cases, fast and non-fast.
            // Stash the response mechanism, and broadcast dataflow construction.
            self.pending_peeks.insert(
                uuid,
                PendingPeek {
                    sender: rows_tx,
                    conn_id,
                },
            );
            self.client_pending_peeks
                .entry(conn_id)
                .or_default()
                .insert(uuid, compute_instance);
            let (id, key, timestamp, _finishing, map_filter_project) = peek_command;

            self.controller
                .compute_mut(compute_instance)
                .unwrap()
                .peek(
                    id,
                    key,
                    uuid,
                    timestamp,
                    finishing.clone(),
                    map_filter_project,
                    target_replica,
                )
                .await
                .unwrap();

            // Prepare the receiver to return as a response.
            let rows_rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rows_rx)
                .fold(PeekResponse::Rows(vec![]), |memo, resp| async {
                    match (memo, resp) {
                        (PeekResponse::Rows(mut memo), PeekResponse::Rows(rows)) => {
                            memo.extend(rows);
                            PeekResponse::Rows(memo)
                        }
                        (PeekResponse::Error(e), _) | (_, PeekResponse::Error(e)) => {
                            PeekResponse::Error(e)
                        }
                        (PeekResponse::Canceled, _) | (_, PeekResponse::Canceled) => {
                            PeekResponse::Canceled
                        }
                    }
                })
                .map(move |resp| match resp {
                    PeekResponse::Rows(rows) => PeekResponseUnary::Rows(finishing.finish(rows)),
                    PeekResponse::Canceled => PeekResponseUnary::Canceled,
                    PeekResponse::Error(e) => PeekResponseUnary::Error(e),
                });

            // If it was created, drop the dataflow once the peek command is sent.
            if let Some(index_id) = drop_dataflow {
                self.remove_global_read_holds_compute(vec![(compute_instance, index_id)]);
                self.drop_indexes(vec![(compute_instance, index_id)]).await;
            }

            Ok(crate::ExecuteResponse::SendingRows {
                future: Box::pin(rows_rx),
                span: tracing::Span::current(),
            })
        }
    }
}

/// Types and methods related to acquiring and releasing read holds on collections.
///
/// A "read hold" prevents the controller from compacting the associated collections,
/// and ensures that they remain "readable" at a specific time, as long as the hold
/// is held.
///
/// These are most commonly used in support of transactions, which acquire these holds
/// to ensure that they can continue to use collections over an open-ended series of
/// queries. However, nothing is specific to transactions here.
pub mod read_holds {
    use crate::coord::id_bundle::CollectionIdBundle;

    /// Relevant information for acquiring or releasing a bundle of read holds.
    #[derive(Clone)]
    pub(super) struct ReadHolds<T> {
        pub(super) time: T,
        pub(super) id_bundle: CollectionIdBundle,
    }

    impl<T> ReadHolds<T> {
        /// Return empty `ReadHolds` at `time`.
        pub fn new(time: T) -> Self {
            ReadHolds {
                time,
                id_bundle: CollectionIdBundle::default(),
            }
        }
    }

    impl<S> crate::coord::Coordinator<S> {
        /// Acquire read holds on the indicated collections at the indicated time.
        ///
        /// This method will panic if the holds cannot be acquired. In the future,
        /// it would be polite to have it error instead, as it is not unrecoverable.
        pub(super) async fn acquire_read_holds(
            &mut self,
            read_holds: &ReadHolds<mz_repr::Timestamp>,
        ) {
            // Update STORAGE read policies.
            let mut policy_changes = Vec::new();
            let storage = self.controller.storage_mut();
            for id in read_holds.id_bundle.storage_ids.iter() {
                let collection = storage.collection(*id).unwrap();
                assert!(collection
                    .read_capabilities
                    .frontier()
                    .less_equal(&read_holds.time));
                let read_needs = self.read_capability.get_mut(id).unwrap();
                read_needs.holds.update_iter(Some((read_holds.time, 1)));
                policy_changes.push((*id, read_needs.policy()));
            }
            storage.set_read_policy(policy_changes).await.unwrap();
            // Update COMPUTE read policies
            for (compute_instance, compute_ids) in read_holds.id_bundle.compute_ids.iter() {
                let mut policy_changes = Vec::new();
                let mut compute = self.controller.compute_mut(*compute_instance).unwrap();
                for id in compute_ids.iter() {
                    let collection = compute.as_ref().collection(*id).unwrap();
                    assert!(collection
                        .read_capabilities
                        .frontier()
                        .less_equal(&read_holds.time));
                    let read_needs = self.read_capability.get_mut(id).unwrap();
                    read_needs.holds.update_iter(Some((read_holds.time, 1)));
                    policy_changes.push((*id, read_needs.policy()));
                }
                compute.set_read_policy(policy_changes).await.unwrap();
            }
        }
        /// Update the timestamp of the read holds on the indicated collections from the
        /// indicated time within `read_holds` to `new_time`.
        ///
        /// This method relies on a previous call to `acquire_read_holds` with the same
        /// `read_holds` argument or a previous call to `update_read_hold` that returned
        /// `read_holds`, and its behavior will be erratic if called on anything else.
        pub(super) async fn update_read_hold(
            &mut self,
            mut read_holds: ReadHolds<mz_repr::Timestamp>,
            new_time: mz_repr::Timestamp,
        ) -> ReadHolds<mz_repr::Timestamp> {
            let ReadHolds {
                time: old_time,
                id_bundle:
                    CollectionIdBundle {
                        storage_ids,
                        compute_ids,
                    },
            } = &read_holds;

            // Update STORAGE read policies.
            let mut policy_changes = Vec::new();
            let storage = self.controller.storage_mut();
            for id in storage_ids.iter() {
                let collection = storage.collection(*id).unwrap();
                assert!(collection
                    .read_capabilities
                    .frontier()
                    .less_equal(&new_time));
                let read_needs = self.read_capability.get_mut(id).unwrap();
                read_needs.holds.update_iter(Some((new_time, 1)));
                read_needs.holds.update_iter(Some((*old_time, -1)));
                policy_changes.push((*id, read_needs.policy()));
            }
            storage.set_read_policy(policy_changes).await.unwrap();
            // Update COMPUTE read policies
            for (compute_instance, compute_ids) in compute_ids.iter() {
                let mut policy_changes = Vec::new();
                let mut compute = self.controller.compute_mut(*compute_instance).unwrap();
                for id in compute_ids.iter() {
                    let collection = compute.as_ref().collection(*id).unwrap();
                    assert!(collection
                        .read_capabilities
                        .frontier()
                        .less_equal(&new_time));
                    let read_needs = self.read_capability.get_mut(id).unwrap();
                    read_needs.holds.update_iter(Some((new_time, 1)));
                    read_needs.holds.update_iter(Some((*old_time, -1)));
                    policy_changes.push((*id, read_needs.policy()));
                }
                compute.set_read_policy(policy_changes).await.unwrap();
            }

            read_holds.time = new_time;
            read_holds
        }
        /// Release read holds on the indicated collections at the indicated time.
        ///
        /// This method relies on a previous call to `acquire_read_holds` with the same
        /// argument, and its behavior will be erratic if called on anything else, or if
        /// called more than once on the same bundle of read holds.
        pub(super) async fn release_read_hold(
            &mut self,
            read_holds: &ReadHolds<mz_repr::Timestamp>,
        ) {
            let ReadHolds {
                time,
                id_bundle:
                    CollectionIdBundle {
                        storage_ids,
                        compute_ids,
                    },
            } = read_holds;

            // Update STORAGE read policies.
            let mut policy_changes = Vec::new();
            for id in storage_ids.iter() {
                // It's possible that a concurrent DDL statement has already dropped this GlobalId
                if let Some(read_needs) = self.read_capability.get_mut(id) {
                    read_needs.holds.update_iter(Some((*time, -1)));
                    policy_changes.push((*id, read_needs.policy()));
                }
            }
            self.controller
                .storage_mut()
                .set_read_policy(policy_changes)
                .await
                .unwrap();
            // Update COMPUTE read policies
            for (compute_instance, compute_ids) in compute_ids.iter() {
                let mut policy_changes = Vec::new();
                for id in compute_ids.iter() {
                    // It's possible that a concurrent DDL statement has already dropped this GlobalId
                    if let Some(read_needs) = self.read_capability.get_mut(id) {
                        read_needs.holds.update_iter(Some((*time, -1)));
                        policy_changes.push((*id, read_needs.policy()));
                    }
                }
                if let Some(mut compute) = self.controller.compute_mut(*compute_instance) {
                    compute.set_read_policy(policy_changes).await.unwrap();
                }
            }
        }
    }
}

/// Information about the read capability requirements of a collection.
///
/// This type tracks both a default policy, as well as various holds that may
/// be expressed, as by transactions to ensure collections remain readable.
struct ReadCapability<T = mz_repr::Timestamp>
where
    T: timely::progress::Timestamp,
{
    /// The default read policy for the collection when no holds are present.
    base_policy: ReadPolicy<T>,
    /// Holds expressed by transactions, that should prevent compaction.
    holds: MutableAntichain<T>,
}

impl<T: timely::progress::Timestamp> From<ReadPolicy<T>> for ReadCapability<T> {
    fn from(base_policy: ReadPolicy<T>) -> Self {
        Self {
            base_policy,
            holds: MutableAntichain::new(),
        }
    }
}

impl<T: timely::progress::Timestamp> ReadCapability<T> {
    /// Acquires the effective read policy, reflecting both the base policy and any holds.
    fn policy(&self) -> ReadPolicy<T> {
        // TODO: This could be "optimized" when `self.holds.frontier` is empty.
        ReadPolicy::Multiple(vec![
            ReadPolicy::ValidFrom(self.holds.frontier().to_owned()),
            self.base_policy.clone(),
        ])
    }
}

#[cfg(test)]
impl<S: Append + 'static> Coordinator<S> {
    #[allow(dead_code)]
    async fn verify_ship_dataflow_no_error(&mut self) {
        // ship_dataflow, ship_dataflows, and finalize_dataflow are not allowed
        // to have a `Result` return because these functions are called after
        // `catalog_transact`, after which no errors are allowed. This test exists to
        // prevent us from incorrectly teaching those functions how to return errors
        // (which has happened twice and is the motivation for this test).

        // An arbitrary compute instance ID to satisfy the function calls below. Note that
        // this only works because this function will never run.
        let compute_instance: ComputeInstanceId = 1;

        let df = DataflowDesc::new("".into());
        let _: () = self.ship_dataflow(df.clone(), compute_instance).await;
        let _: () = self
            .ship_dataflows(vec![df.clone()], compute_instance)
            .await;
        let _: DataflowDescription<mz_compute_client::plan::Plan> =
            self.finalize_dataflow(df, compute_instance);
    }
}

/// A mechanism to ensure that a sequence of writes and reads proceed correctly through timestamps.
mod timeline {
    use std::future::Future;
    use std::time::Duration;

    use once_cell::sync::Lazy;

    /// A timeline is either currently writing or currently reading.
    ///
    /// At each time, writes happen and then reads happen, meaning that writes at a time are
    /// visible to exactly reads at that time or greater, and no other times.
    enum TimestampOracleState<T> {
        /// The timeline is producing collection updates timestamped with the argument.
        Writing(T),
        /// The timeline is observing collections aot the time of the argument.
        Reading(T),
    }

    /// A type that provides write and read timestamps, reads observe exactly their preceding writes..
    ///
    /// Specifically, all read timestamps will be greater or equal to all previously reported write timestamps,
    /// and strictly less than all subsequently emitted write timestamps.
    pub struct TimestampOracle<T> {
        state: TimestampOracleState<T>,
        advance_to: Option<T>,
        next: Box<dyn Fn() -> T>,
    }

    impl<T: super::CoordTimestamp> TimestampOracle<T> {
        /// Create a new timeline, starting at the indicated time. `next` generates
        /// new timestamps when invoked. The timestamps have no requirements, and can
        /// retreat from previous invocations.
        pub fn new<F>(initially: T, next: F) -> Self
        where
            F: Fn() -> T + 'static,
        {
            Self {
                state: TimestampOracleState::Writing(initially.clone()),
                advance_to: Some(initially),
                next: Box::new(next),
            }
        }

        /// Peek the current value of the timestamp.
        ///
        /// No operations should be assigned to the timestamp returned by this function. The
        /// timestamp returned should only be used to compare the progress of the `TimestampOracle`
        /// against some external source of time.
        ///
        /// Subsequent values of `self.read_ts()` and `self.write_ts()` will be greater or equal to
        /// this timestamp.
        ///
        /// NOTE: This can be removed once DDL is included in group commits.
        pub fn peek_ts(&self) -> T {
            match &self.state {
                TimestampOracleState::Writing(ts) | TimestampOracleState::Reading(ts) => ts.clone(),
            }
        }

        /// Acquire a new timestamp for writing.
        ///
        /// This timestamp will be strictly greater than all prior values of
        /// `self.read_ts()`, and less than or equal to all subsequent values of
        /// `self.read_ts()`.
        pub fn write_ts(&mut self) -> T {
            match &self.state {
                TimestampOracleState::Writing(ts) => ts.clone(),
                TimestampOracleState::Reading(ts) => {
                    let mut next = (self.next)();
                    if next.less_equal(&ts) {
                        next = ts.step_forward();
                    }
                    assert!(ts.less_than(&next));
                    self.state = TimestampOracleState::Writing(next.clone());
                    self.advance_to = Some(next.clone());
                    next
                }
            }
        }
        /// Acquire a new timestamp for reading.
        ///
        /// This timestamp will be greater or equal to all prior values of `self.write_ts()`,
        /// and strictly less than all subsequent values of `self.write_ts()`.
        pub fn read_ts(&mut self) -> T {
            match &self.state {
                TimestampOracleState::Reading(ts) => ts.clone(),
                TimestampOracleState::Writing(ts) => {
                    // Avoid rust borrow complaint.
                    let ts = ts.clone();
                    self.state = TimestampOracleState::Reading(ts.clone());
                    self.advance_to = Some(ts.step_forward());
                    ts
                }
            }
        }

        /// Electively advance the tracked times.
        ///
        /// If `lower_bound` is strictly greater than the current time (of either state), the
        /// resulting state will be `Writing(lower_bound)`.
        pub fn fast_forward(&mut self, lower_bound: T) {
            match &self.state {
                TimestampOracleState::Writing(ts) => {
                    if ts.less_than(&lower_bound) {
                        self.advance_to = Some(lower_bound.clone());
                        self.state = TimestampOracleState::Writing(lower_bound);
                    }
                }
                TimestampOracleState::Reading(ts) => {
                    if ts.less_than(&lower_bound) {
                        // This may result in repetition in the case `lower_bound == ts + 1`.
                        // This is documented as fine, and concerned users can protect themselves.
                        self.advance_to = Some(lower_bound.clone());
                        self.state = TimestampOracleState::Writing(lower_bound);
                    }
                }
            }
        }
        /// Whether and to what the next value of `self.write_ts() has advanced since this method was last called.
        ///
        /// This method may produce the same value multiple times, and should not be used as a test for whether
        /// a write-to-read transition has occurred, so much as an advisory signal that write capabilities can advance.
        pub fn should_advance_to(&mut self) -> Option<T> {
            self.advance_to.take()
        }
    }

    /// Interval used to persist durable timestamps. See [`DurableTimestampOracle`] for more
    /// details.
    pub static TIMESTAMP_PERSIST_INTERVAL: Lazy<mz_repr::Timestamp> = Lazy::new(|| {
        Duration::from_secs(15)
            .as_millis()
            .try_into()
            .expect("15 seconds can fit into `Timestamp`")
    });

    /// A type that wraps a [`TimestampOracle`] and provides durable timestamps. This allows us to
    /// recover a timestamp that is larger than all previous timestamps on restart. The protocol
    /// is based on timestamp recovery from Percolator <https://research.google/pubs/pub36726/>. We
    /// "pre-allocate" a group of timestamps at once, and only durably store the largest of those
    /// timestamps. All timestamps within that interval can be served directly from memory, without
    /// going to disk. On restart, we re-initialize the current timestamp to a value one larger
    /// than the persisted timestamp.
    ///
    /// See [`TimestampOracle`] for more details on the properties of the timestamps.
    pub struct DurableTimestampOracle<T> {
        timestamp_oracle: TimestampOracle<T>,
        durable_timestamp: T,
        persist_interval: T,
    }

    impl<T: super::CoordTimestamp> DurableTimestampOracle<T> {
        /// Create a new durable timeline, starting at the indicated time. Timestamps will be
        /// allocated in groups of size `persist_interval`. Also returns the new timestamp that
        /// needs to be persisted to disk.
        ///
        /// See [`TimestampOracle::new`] for more details.
        pub async fn new<F, Fut>(
            initially: T,
            next: F,
            persist_interval: T,
            persist_fn: impl FnOnce(T) -> Fut,
        ) -> Self
        where
            F: Fn() -> T + 'static,
            Fut: Future<Output = Result<(), crate::catalog::Error>>,
        {
            let mut oracle = Self {
                timestamp_oracle: TimestampOracle::new(initially.clone(), next),
                durable_timestamp: initially.clone(),
                persist_interval,
            };
            oracle
                .maybe_allocate_new_timestamps(&initially, persist_fn)
                .await;
            oracle
        }

        /// Peek the current value of the timestamp.
        ///
        /// See [`TimestampOracle::peek_ts`] for more details.
        pub fn peek_ts(&self) -> T {
            self.timestamp_oracle.peek_ts()
        }

        /// Acquire a new timestamp for writing. Optionally returns a timestamp that needs to be
        /// persisted to disk.
        ///
        /// See [`TimestampOracle::write_ts`] for more details.
        pub async fn write_ts<Fut>(&mut self, persist_fn: impl FnOnce(T) -> Fut) -> T
        where
            Fut: Future<Output = Result<(), crate::catalog::Error>>,
        {
            let ts = self.timestamp_oracle.write_ts();
            self.maybe_allocate_new_timestamps(&ts, persist_fn).await;
            ts
        }

        /// Acquire a new timestamp for reading. Optionally returns a timestamp that needs to be
        /// persisted to disk.
        ///
        /// See [`TimestampOracle::read_ts`] for more details.
        pub fn read_ts(&mut self) -> T {
            let ts = self.timestamp_oracle.read_ts();
            assert!(
                ts.less_than(&self.durable_timestamp),
                "read_ts should not advance the global timestamp"
            );
            ts
        }

        /// Electively advance the tracked times. Optionally returns a timestamp that needs to be
        /// persisted to disk.
        ///
        /// See [`TimestampOracle::fast_forward`] for more details.
        pub async fn fast_forward<Fut>(&mut self, lower_bound: T, persist_fn: impl FnOnce(T) -> Fut)
        where
            Fut: Future<Output = Result<(), crate::catalog::Error>>,
        {
            self.timestamp_oracle.fast_forward(lower_bound.clone());
            self.maybe_allocate_new_timestamps(&lower_bound, persist_fn)
                .await;
        }

        /// See [`TimestampOracle::should_advance_to`] for more details.
        pub fn should_advance_to(&mut self) -> Option<T> {
            self.timestamp_oracle.should_advance_to()
        }

        /// Checks to see if we can serve the timestamp from memory, or if we need to durably store
        /// a new timestamp.
        ///
        /// If `ts` is less than the persisted timestamp then we can serve `ts` from memory,
        /// otherwise we need to durably store some timestamp greater than `ts`.
        async fn maybe_allocate_new_timestamps<Fut>(
            &mut self,
            ts: &T,
            persist_fn: impl FnOnce(T) -> Fut,
        ) where
            Fut: Future<Output = Result<(), crate::catalog::Error>>,
        {
            if self.durable_timestamp.less_equal(ts)
                // Since the timestamp is at its max value, we know that no other Coord can
                // allocate a higher value.
                && self.durable_timestamp.less_than(&T::maximum())
            {
                self.durable_timestamp = ts.step_forward_by(&self.persist_interval);
                persist_fn(self.durable_timestamp.clone())
                    .await
                    .expect("can't persist timestamp");
            }
        }
    }
}

pub trait CoordTimestamp:
    timely::progress::Timestamp
    + timely::order::TotalOrder
    + differential_dataflow::lattice::Lattice
    + std::fmt::Debug
{
    /// Advance a timestamp by the least amount possible such that
    /// `ts.less_than(ts.step_forward())` is true. Panic if unable to do so.
    fn step_forward(&self) -> Self;

    /// Advance a timestamp forward by the given `amount`. Panic if unable to do so.
    fn step_forward_by(&self, amount: &Self) -> Self;

    /// Retreat a timestamp by the least amount possible such that
    /// `ts.step_back().unwrap().less_than(ts)` is true. Return `None` if unable,
    /// which must only happen if the timestamp is `Timestamp::minimum()`.
    fn step_back(&self) -> Option<Self>;

    /// Return the maximum value for this timestamp.
    fn maximum() -> Self;
}

impl CoordTimestamp for mz_repr::Timestamp {
    fn step_forward(&self) -> Self {
        match self.checked_add(1) {
            Some(ts) => ts,
            None => panic!("could not step forward"),
        }
    }

    fn step_forward_by(&self, amount: &Self) -> Self {
        match self.checked_add(*amount) {
            Some(ts) => ts,
            None => panic!("could not step {self} forward by {amount}"),
        }
    }

    fn step_back(&self) -> Option<Self> {
        self.checked_sub(1)
    }

    fn maximum() -> Self {
        Self::MAX
    }
}
