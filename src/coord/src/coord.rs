// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordination of installed views, available timestamps, compacted
//! timestamps, and transactions.
//!
//! The command coordinator maintains a view of the installed views, and for
//! each tracks the frontier of available times
//! ([`upper`](arrangement_state::Frontiers::upper)) and the frontier of
//! compacted times ([`since`](arrangement_state::Frontiers::since)). The upper
//! frontier describes times that may not return immediately, as any timestamps
//! in advance of the frontier are still open. The since frontier constrains
//! those times for which the maintained view will be correct, as any
//! timestamps in advance of the frontier must accumulate to the same value as
//! would an un-compacted trace. The since frontier cannot be directly mutated,
//! but instead can have multiple handles to it which forward changes from an
//! internal MutableAntichain to the since.
//!
//! The [`Coordinator`] tracks various compaction frontiers so that source,
//! indexes, compaction, and transactions can work together.
//! [`determine_timestamp()`](Coordinator::determine_timestamp)
//! returns the least valid since of its sources. Any new transactions should
//! thus always be >= the current compaction frontier and so should never change
//! the frontier when being added to [`txn_reads`](Coordinator::txn_reads). The
//! compaction frontier may change when a transaction ends (if it was the oldest
//! transaction and the since was advanced after the transaction started) or
//! when [`update_upper()`](Coordinator::update_upper) is run (if there
//! are no in progress transactions before the new since). When it does, it is
//! added to [`index_since_updates`](Coordinator::index_since_updates) or
//! [`source_since_updates`](Coordinator::source_since_updates) and will be
//! processed during the next [`maintenance()`](Coordinator::maintenance) call.
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

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::future::{self, FutureExt, TryFutureExt};
use futures::stream::StreamExt;
use itertools::Itertools;
use rand::Rng;
use timely::order::PartialOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp as _};
use tokio::runtime::Handle as TokioHandle;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::error;

use mz_build_info::BuildInfo;
use mz_dataflow_types::client::DEFAULT_COMPUTE_INSTANCE_ID;
use mz_dataflow_types::client::{ComputeResponse, TimestampBindingFeedback};
use mz_dataflow_types::client::{
    CreateSourceCommand, Response as DataflowResponse, StorageResponse,
};
use mz_dataflow_types::logging::LoggingConfig as DataflowLoggingConfig;
use mz_dataflow_types::sinks::{SinkAsOf, SinkConnector, SinkDesc, TailSinkConnector};
use mz_dataflow_types::sources::{
    AwsExternalId, ExternalSourceConnector, PostgresSourceConnector, SourceConnector, Timeline,
};
use mz_dataflow_types::{
    BuildDesc, DataflowDesc, DataflowDescription, IndexDesc, PeekResponse, PeekResponseUnary,
    Update,
};
use mz_expr::{
    permutation_for_arrangement, ExprHumanizer, GlobalId, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{to_datetime, NowFn};
use mz_ore::retry::Retry;
use mz_ore::soft_assert_eq;
use mz_ore::task;
use mz_ore::thread::JoinHandleExt;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::{Numeric, NumericMaxScale};
use mz_repr::{Datum, Diff, RelationDesc, RelationType, Row, RowArena, ScalarType, Timestamp};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{
    ConnectorType, CreateIndexStatement, CreateSinkStatement, CreateSourceStatement, ExplainStage,
    FetchStatement, Ident, InsertSource, ObjectType, Query, Raw, SetExpr, Statement,
};
use mz_sql::catalog::{CatalogError, CatalogTypeDetails, SessionCatalog as _};
use mz_sql::names::{DatabaseSpecifier, FullName};
use mz_sql::plan::{
    AlterIndexEnablePlan, AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan,
    AlterItemRenamePlan, CreateDatabasePlan, CreateIndexPlan, CreateRolePlan, CreateSchemaPlan,
    CreateSinkPlan, CreateSourcePlan, CreateTablePlan, CreateTypePlan, CreateViewPlan,
    CreateViewsPlan, DropDatabasePlan, DropItemsPlan, DropRolesPlan, DropSchemaPlan, ExecutePlan,
    ExplainPlan, FetchPlan, HirRelationExpr, IndexOption, IndexOptionName, InsertPlan,
    MutationKind, Params, PeekPlan, PeekWhen, Plan, RaisePlan, ReadThenWritePlan, SendDiffsPlan,
    SetVariablePlan, ShowVariablePlan, TailFrom, TailPlan,
};
use mz_sql::plan::{OptimizerConfig, StatementDesc, View};
use mz_transform::Optimizer;

use self::arrangement_state::{ArrangementFrontiers, Frontiers, SinkWrites};
use self::prometheus::Scraper;
use crate::catalog::builtin::{BUILTINS, MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS};
use crate::catalog::{self, storage, BuiltinTableUpdate, Catalog, CatalogItem, SinkConnectorState};
use crate::client::{Client, Handle};
use crate::command::{
    Canceled, Command, ExecuteResponse, Response, StartupMessage, StartupResponse,
};
use crate::coord::antichain::AntichainToken;
use crate::coord::dataflow_builder::{DataflowBuilder, ExprPrepStyle};
use crate::error::CoordError;
use crate::persistcfg::PersisterWithConfig;
use crate::session::{
    EndTransactionAction, PreparedStatement, Session, Transaction, TransactionOps,
    TransactionStatus, WriteOp,
};
use crate::sink_connector;
use crate::tail::PendingTail;
use crate::util::ClientTransmitter;

mod antichain;
mod arrangement_state;
mod dataflow_builder;
mod prometheus;

#[derive(Debug)]
pub enum Message {
    Command(Command),
    Worker(mz_dataflow_types::client::Response),
    StatementReady(StatementReady),
    SinkConnectorReady(SinkConnectorReady),
    ScrapeMetrics,
    SendDiffs(SendDiffs),
    WriteLockGrant(tokio::sync::OwnedMutexGuard<()>),
    AdvanceLocalInputs,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SendDiffs {
    session: Session,
    #[derivative(Debug = "ignore")]
    tx: ClientTransmitter<ExecuteResponse>,
    pub id: GlobalId,
    pub diffs: Result<Vec<(Row, Diff)>, CoordError>,
    pub kind: MutationKind,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StatementReady {
    pub session: Session,
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub result: Result<mz_sql::ast::Statement<Raw>, CoordError>,
    pub params: Params,
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
pub struct SinkConnectorReady {
    pub session: Session,
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub id: GlobalId,
    pub oid: u32,
    pub result: Result<SinkConnector, CoordError>,
}

#[derive(Debug)]
pub struct TimestampedUpdate {
    pub updates: Vec<BuiltinTableUpdate>,
    pub timestamp_offset: u64,
}

/// Configures dataflow worker logging.
#[derive(Clone, Debug)]
pub struct LoggingConfig {
    pub granularity: Duration,
    pub log_logging: bool,
    pub retain_readings_for: Duration,
    pub metrics_scraping_interval: Option<Duration>,
}

/// Configures a coordinator.
pub struct Config {
    pub dataflow_client: Box<dyn mz_dataflow_types::client::Client + Send>,
    pub logging: Option<LoggingConfig>,
    pub storage: storage::Connection,
    pub timestamp_frequency: Duration,
    pub logical_compaction_window: Option<Duration>,
    pub experimental_mode: bool,
    pub disable_user_indexes: bool,
    pub safe_mode: bool,
    pub build_info: &'static BuildInfo,
    pub aws_external_id: AwsExternalId,
    pub metrics_registry: MetricsRegistry,
    pub persister: PersisterWithConfig,
    pub now: NowFn,
}

/// Glues the external world to the Timely workers.
pub struct Coordinator {
    /// A client to a running dataflow cluster.
    dataflow_client:
        mz_dataflow_types::client::Controller<Box<dyn mz_dataflow_types::client::Client>>,
    /// Optimizer instance for logical optimization of views.
    view_optimizer: Optimizer,
    catalog: Catalog,
    /// A runtime for the `persist` crate alongside its configuration.
    persister: PersisterWithConfig,

    /// Maps (global Id of arrangement) -> (frontier information). This tracks the
    /// `upper` and computed `since` of the indexes. The `since` is the time at
    /// which we are willing to compact up to. `determine_timestamp()` uses this as
    /// part of its heuristic when determining a viable timestamp for queries.
    indexes: ArrangementFrontiers<Timestamp>,
    /// Map of frontier information for sources
    sources: ArrangementFrontiers<Timestamp>,
    /// Delta from leading edge of an arrangement from which we allow compaction.
    logical_compaction_window_ms: Option<Timestamp>,
    /// Whether base sources are enabled.
    logging_enabled: bool,
    /// Channel to manange internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    /// Channel to communicate source status updates to the timestamper thread.
    metric_scraper: Scraper,

    /// The last known timestamp that was considered "open" (i.e. where writes
    /// may occur). However, this timestamp is _not_ open when
    /// `read_writes_at_open_ts == true`; in this case, reads will occur at
    /// `last_open_local_ts`, and the Coordinator must open a new timestamp
    /// for writes.
    ///
    /// Indirectly, this value aims to represent the Coordinator's desired value
    /// for `upper` for table frontiers, as long as we know it is open.
    last_open_local_ts: Timestamp,
    /// Whether or not we have written at the open timestamp.
    writes_at_open_ts: bool,
    /// Whether or not we have read the writes that have occurred at the open
    /// timestamp. When this is `true`, it signals we need to open a new
    /// timestamp to support future writes.
    read_writes_at_open_ts: bool,

    transient_id_counter: u64,
    /// A map from connection ID to metadata about that connection for all
    /// active connections.
    active_conns: HashMap<u32, ConnMeta>,

    /// Holds pending compaction messages to be sent to the dataflow workers. When
    /// `since_handles` are advanced or `txn_reads` are dropped, this can advance.
    index_since_updates: Rc<RefCell<HashMap<GlobalId, Antichain<Timestamp>>>>,
    /// Holds pending compaction messages to be sent to the dataflow workers. When
    /// `since_handles` are advanced or `txn_reads` are dropped, this can advance.
    source_since_updates: Rc<RefCell<HashMap<GlobalId, Antichain<Timestamp>>>>,
    /// Holds handles to ids that are advanced by update_upper.
    since_handles: HashMap<GlobalId, AntichainToken<Timestamp>>,
    /// Tracks active read transactions so that we don't compact any indexes beyond
    /// an in-progress transaction.
    // TODO(mjibson): Should this live on a Session?
    txn_reads: HashMap<u32, TxnReads>,
    /// Tracks write frontiers for active exactly-once sinks.
    sink_writes: HashMap<GlobalId, SinkWrites<Timestamp>>,

    /// A map from pending peeks to the queue into which responses are sent, and
    /// the IDs of workers who have responded.
    pending_peeks: HashMap<u32, mpsc::UnboundedSender<PeekResponse>>,
    /// A map from pending tails to the tail description.
    pending_tails: HashMap<GlobalId, PendingTail>,

    /// Serializes accesses to write critical sections.
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// Holds plans deferred due to write lock.
    write_lock_wait_group: VecDeque<DeferredPlan>,
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
    timedomain_ids: HashSet<GlobalId>,
    _handles: Vec<AntichainToken<Timestamp>>,
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
                $coord.defer_write($tx, $session, $plan_to_defer);
                return;
            }
        }
    };
}

impl Coordinator {
    /// Assign a timestamp for a read from a local input. Reads following writes
    /// must be at a time >= the write's timestamp; we choose "equal to" for
    /// simplicity's sake and to open as few new timestamps as possible.
    fn get_local_read_ts(&mut self) -> Timestamp {
        if self.writes_at_open_ts {
            // If you have pending writes, you will need to read those writes,
            // which happened at the last known open time. This also means you
            // will need to advance to those writes, i.e. close over
            // `last_open_local_ts`.
            self.read_writes_at_open_ts = true;
            self.last_open_local_ts
        } else {
            // If there are no writes at the open timestamp, we know we can read
            // at one unit of time less than the open time (which will always be
            // closed).
            self.last_open_local_ts - 1
        }
    }

    /// Assign a timestamp for a write to a local input. Writes following reads
    /// must ensure that they are assigned a strictly larger timestamp to ensure
    /// they are not visible to any real-time earlier reads.
    fn get_local_write_ts(&mut self) -> Timestamp {
        // This assert is valid because:
        // - Whenever a write precedes a read, the read sets
        //   `read_writes_at_open_ts = true`, which will advance the
        //   `last_open_local_ts`.
        // - The Coordinator always has the opportunity to check the state of
        //   `read_writes_at_open_ts` after a read, even in the case of
        //   `ReadThenWrite` plans, which dictates when we advance the
        //   timestamp.
        // - Advancing the timestamp sets `read_writes_at_open_ts = false`.
        assert!(
            !self.read_writes_at_open_ts,
            "do not perform writes at time where tables want to read"
        );

        self.writes_at_open_ts = true;

        self.last_open_local_ts
    }

    /// Opens a new timestamp for local inputs at which writes may occur, and
    /// where reads should return quickly at a value 1 less.
    fn open_new_local_ts(&mut self) {
        // This is a hack. In a perfect world we would represent time as having a "real" dimension
        // and a "coordinator" dimension so that clients always observed linearizability from
        // things the coordinator did without being related to the real dimension.
        let ts = (self.catalog.config().now)();

        // We cannot depend on `self.catalog.config().now`'s value to increase
        // (in addition to the normal considerations around clocks in computers,
        // this feature enables us to drive the Coordinator's time when using a
        // test harness). Instead, we must manually increment
        // `last_open_local_ts` if `now` appears non-increasing.
        self.last_open_local_ts = std::cmp::max(ts, self.last_open_local_ts + 1);

        // Opening a new timestamp means that there cannot be new writes at the
        // open timestamp.
        self.writes_at_open_ts = false;
        self.read_writes_at_open_ts = false;
    }

    fn now_datetime(&self) -> DateTime<Utc> {
        to_datetime((self.catalog.config().now)())
    }

    /// Generate a new frontiers object that forwards since changes to `index_since_updates`.
    ///
    /// # Panics
    ///
    /// This function panics if called twice with the same `id`.
    fn new_index_frontiers<I>(
        &mut self,
        id: GlobalId,
        initial: I,
        compaction_window_ms: Option<Timestamp>,
    ) -> Frontiers<Timestamp>
    where
        I: IntoIterator<Item = Timestamp>,
    {
        let index_since_updates = Rc::clone(&self.index_since_updates);
        let (frontier, handle) = Frontiers::new(initial, compaction_window_ms, move |frontier| {
            index_since_updates.borrow_mut().insert(id, frontier);
        });
        let prev = self.since_handles.insert(id, handle);
        // Ensure we don't double-register ids.
        assert!(prev.is_none());
        frontier
    }
    ///
    /// Generate a new frontiers object that forwards since changes to `source_since_updates`.
    ///
    /// # Panics
    ///
    /// This function panics if called twice with the same `id`.
    fn new_source_frontiers<I>(
        &mut self,
        id: GlobalId,
        initial: I,
        compaction_window_ms: Option<Timestamp>,
    ) -> Frontiers<Timestamp>
    where
        I: IntoIterator<Item = Timestamp>,
    {
        let storage_since_updates = Rc::clone(&self.source_since_updates);
        let (frontier, handle) = Frontiers::new(initial, compaction_window_ms, move |frontier| {
            storage_since_updates.borrow_mut().insert(id, frontier);
        });
        let prev = self.since_handles.insert(id, handle);
        // Ensure we don't double-register ids.
        assert!(prev.is_none());
        frontier
    }

    /// Initializes coordinator state based on the contained catalog. Must be
    /// called after creating the coordinator and before calling the
    /// `Coordinator::serve` method.
    async fn bootstrap(
        &mut self,
        builtin_table_updates: Vec<BuiltinTableUpdate>,
    ) -> Result<(), CoordError> {
        let entries: Vec<_> = self.catalog.entries().cloned().collect();

        // Sources and indexes may be depended upon by other catalog items,
        // insert them first.
        for entry in &entries {
            match entry.item() {
                // Currently catalog item rebuild assumes that sinks and
                // indexes are always built individually and does not store information
                // about how it was built. If we start building multiple sinks and/or indexes
                // using a single dataflow, we have to make sure the rebuild process re-runs
                // the same multiple-build dataflow.
                CatalogItem::Source(source) => {
                    let since_ts = self
                        .persister
                        .load_source_persist_desc(&source)
                        .map_err(CoordError::Persistence)?
                        .map(|p| p.since_ts)
                        .unwrap_or(0);

                    let frontiers = self.new_source_frontiers(
                        entry.id(),
                        [since_ts],
                        self.logical_compaction_window_ms,
                    );
                    self.sources.insert(entry.id(), frontiers);
                    // Re-announce the source description.
                    let source_description = self
                        .catalog
                        .state()
                        .source_description_for(entry.id())
                        .unwrap();

                    let ts_bindings = self
                        .catalog
                        .load_timestamp_bindings(entry.id())
                        .expect("loading timestamps from coordinator cannot fail");

                    self.dataflow_client
                        .storage()
                        .create_sources(vec![CreateSourceCommand {
                            id: entry.id(),
                            desc: source_description,
                            since: Antichain::from_elem(since_ts),
                            ts_bindings,
                        }])
                        .await
                        .unwrap();
                }
                CatalogItem::Table(table) => {
                    self.persister
                        .add_table(entry.id(), &table)
                        .map_err(CoordError::Persistence)?;

                    let since_ts = self
                        .persister
                        .table_details
                        .get(&entry.id())
                        .map(|td| td.since_ts)
                        .unwrap_or(0);

                    let frontiers = self.new_source_frontiers(
                        entry.id(),
                        [since_ts],
                        self.logical_compaction_window_ms,
                    );

                    // NOTE: Tables are not sources, but to a large part of the system they look
                    // like they are, e.g. they are rendered as a SourceConnector::Local.
                    self.sources.insert(entry.id(), frontiers);
                    // Re-announce the source description.
                    let source_description = self
                        .catalog
                        .state()
                        .source_description_for(entry.id())
                        .unwrap();
                    self.dataflow_client
                        .storage()
                        .create_sources(vec![CreateSourceCommand {
                            id: entry.id(),
                            desc: source_description,
                            since: Antichain::from_elem(since_ts),
                            ts_bindings: vec![],
                        }])
                        .await
                        .unwrap();
                }
                CatalogItem::Index(_) => {
                    if BUILTINS.logs().any(|log| log.index_id == entry.id()) {
                        // Indexes on logging views are special, as they are
                        // already installed in the dataflow plane via
                        // `dataflow_types::client::Command::EnableLogging`. Just teach the
                        // coordinator of their existence, without creating a
                        // dataflow for the index.
                        //
                        // TODO(benesch): why is this hardcoded to 1000?
                        // Should it not be the same logical compaction window
                        // that everything else uses?
                        let frontiers = self.new_index_frontiers(entry.id(), Some(0), Some(1_000));
                        self.indexes.insert(entry.id(), frontiers);
                    } else {
                        let index_id = entry.id();
                        if let Some((name, description)) =
                            Self::prepare_index_build(self.catalog.state(), &index_id)
                        {
                            let df = self.dataflow_builder().build_index_dataflow(
                                name,
                                index_id,
                                description,
                            )?;
                            self.ship_dataflow(df).await;
                        }
                    }
                }
                _ => (), // Handled in next loop.
            }
        }

        for entry in entries {
            match entry.item() {
                CatalogItem::View(_) => (),
                CatalogItem::Sink(sink) => {
                    let builder = match &sink.connector {
                        SinkConnectorState::Pending(builder) => builder,
                        SinkConnectorState::Ready(_) => {
                            panic!("sink already initialized during catalog boot")
                        }
                    };
                    let connector = sink_connector::build(builder.clone(), entry.id())
                        .await
                        .with_context(|| format!("recreating sink {}", entry.name()))?;
                    self.handle_sink_connector_ready(entry.id(), entry.oid(), connector)
                        .await?;
                }
                _ => (), // Handled in prior loop.
            }
        }

        self.send_builtin_table_updates(builtin_table_updates).await;

        // Announce primary and foreign key relationships.
        if self.logging_enabled {
            for log in BUILTINS.logs() {
                let log_id = &log.id.to_string();
                self.send_builtin_table_updates(
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
                                    id: MZ_VIEW_KEYS.id,
                                    row,
                                    diff: 1,
                                }
                            })
                        })
                        .collect(),
                )
                .await;

                self.send_builtin_table_updates(
                    log.variant
                        .foreign_keys()
                        .into_iter()
                        .enumerate()
                        .flat_map(move |(index, (parent, pairs))| {
                            let parent_id = BUILTINS
                                .logs()
                                .find(|src| src.variant == parent)
                                .unwrap()
                                .id
                                .to_string();
                            pairs.into_iter().map(move |(c, p)| {
                                let row = Row::pack_slice(&[
                                    Datum::String(&log_id),
                                    Datum::Int64(c as i64),
                                    Datum::String(&parent_id),
                                    Datum::Int64(p as i64),
                                    Datum::Int64(index as i64),
                                ]);
                                BuiltinTableUpdate {
                                    id: MZ_VIEW_FOREIGN_KEYS.id,
                                    row,
                                    diff: 1,
                                }
                            })
                        })
                        .collect(),
                )
                .await;
            }
        }

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
        {
            // An explicit SELECT or INSERT on a table will bump the table's timestamps,
            // but there are cases where timestamps are not bumped but we expect the closed
            // timestamps to advance (`AS OF now()`, TAILing views over RT sources and
            // tables). To address these, spawn a task that forces table timestamps to
            // close on a regular interval. This roughly tracks the behaivor of realtime
            // sources that close off timestamps on an interval.
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "coordinator_serve", async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1_000));
                loop {
                    interval.tick().await;
                    // If sending fails, the main thread has shutdown.
                    if internal_cmd_tx.send(Message::AdvanceLocalInputs).is_err() {
                        break;
                    }
                }
            });
        }

        let mut metric_scraper_stream = self.metric_scraper.tick_stream();

        loop {
            let msg = select! {
                // Order matters here. We want to process internal commands
                // before processing external commands.
                biased;

                Some(m) = internal_cmd_rx.recv() => m,
                Some(m) = self.dataflow_client.recv() => Message::Worker(m),
                Some(m) = metric_scraper_stream.next() => m,
                m = cmd_rx.recv() => match m {
                    None => break,
                    Some(m) => Message::Command(m),
                },
            };

            match msg {
                Message::Command(cmd) => self.message_command(cmd).await,
                Message::Worker(worker) => self.message_worker(worker).await,
                Message::StatementReady(ready) => self.message_statement_ready(ready).await,
                Message::SinkConnectorReady(ready) => {
                    self.message_sink_connector_ready(ready).await
                }
                Message::WriteLockGrant(write_lock_guard) => {
                    // It's possible to have more incoming write lock grants
                    // than pending writes because of cancellations.
                    if let Some(mut ready) = self.write_lock_wait_group.pop_front() {
                        ready.session.grant_write_lock(write_lock_guard);
                        self.sequence_plan(ready.tx, ready.session, ready.plan)
                            .await;
                    }
                    // N.B. if no deferred plans, write lock is released by drop
                    // here.
                }
                Message::SendDiffs(diffs) => self.message_send_diffs(diffs),
                Message::ScrapeMetrics => self.message_scrape_metrics().await,
                Message::AdvanceLocalInputs => {
                    // Convince the coordinator it needs to open a new timestamp
                    // and advance inputs.
                    self.read_writes_at_open_ts = true;
                }
            }

            if self.read_writes_at_open_ts {
                self.advance_local_inputs().await;
            }
        }
    }

    // Advance all local inputs (tables) to the current wall clock or at least
    // a time greater than any previous table read (if wall clock has gone
    // backward). This downgrades the capabilities of all tables, which means that
    // all tables can no longer produce new data before this timestamp.
    async fn advance_local_inputs(&mut self) {
        self.open_new_local_ts();

        // Close the stream up to the newly opened timestamp.
        let advance_to = self.last_open_local_ts;

        // Ensure that the persister is aware of exactly the set of tables for
        // which persistence is enabled.
        soft_assert_eq!(
            self.catalog
                .entries()
                .filter(|entry| matches!(
                    entry.item(),
                    CatalogItem::Table(catalog::Table {
                        persist_name: Some(_),
                        ..
                    })
                ))
                .map(|entry| entry.id())
                .collect::<Vec<_>>(),
            self.persister
                .table_details
                .keys()
                .copied()
                .collect::<Vec<_>>(),
        );

        if let Some(table_writer) = &mut self.persister.table_writer {
            // Close out the timestamp for persisted tables.
            //
            // NB: Keep this method call outside the tokio::spawn. We're
            // guaranteed by persist that writes and seals happen in order,
            // but only if we synchronously wait for the (fast) registration
            // of that work to return.
            let seal_fut = table_writer.seal(&self.persister.all_table_ids, advance_to);
            let _ = task::spawn(
                || format!("advance_local_inputs:{advance_to}"),
                async move {
                    if let Err(err) = seal_fut.await {
                        // TODO: Linearizability relies on this, bubble up the
                        // error instead.
                        //
                        // EDIT: On further consideration, I think it doesn't
                        // affect correctness if this fails, just availability
                        // of the table.
                        error!(
                            "failed to seal persisted stream to ts {}: {}",
                            advance_to, err
                        );
                    }
                },
            );
        }

        self.dataflow_client
            .storage()
            .advance_all_table_timestamps(advance_to)
            .await
            .unwrap();
    }

    async fn message_worker(&mut self, message: DataflowResponse) {
        match message {
            DataflowResponse::Compute(
                ComputeResponse::PeekResponse(conn_id, response),
                instance,
            ) => {
                assert_eq!(instance, DEFAULT_COMPUTE_INSTANCE_ID);
                // We expect exactly one peek response, which we forward.
                self.pending_peeks
                    .remove(&conn_id)
                    .expect("no more PeekResponses after closing peek channel")
                    .send(response)
                    .expect("Peek endpoint terminated prematurely");
            }
            DataflowResponse::Compute(
                ComputeResponse::TailResponse(sink_id, response),
                instance,
            ) => {
                assert_eq!(instance, DEFAULT_COMPUTE_INSTANCE_ID);
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
            DataflowResponse::Compute(ComputeResponse::FrontierUppers(updates), instance) => {
                assert_eq!(instance, DEFAULT_COMPUTE_INSTANCE_ID);
                for (name, changes) in updates {
                    self.update_upper(&name, changes);
                }
                self.maintenance().await;
            }
            DataflowResponse::Storage(StorageResponse::TimestampBindings(
                TimestampBindingFeedback { bindings, changes },
            )) => {
                self.catalog
                    .insert_timestamp_bindings(
                        bindings
                            .into_iter()
                            .map(|(id, pid, ts, offset)| (id, pid.to_string(), ts, offset.offset)),
                    )
                    .expect("inserting timestamp bindings cannot fail");

                let mut durability_updates = Vec::new();
                let mut timestamp_compactions = Vec::new();
                for (source_id, mut changes) in changes {
                    if let Some(source_state) = self.sources.get_mut(&source_id) {
                        // Apply the updates the dataflow worker sent over, and check if there
                        // were any changes to the source's upper frontier.
                        let changes: Vec<_> =
                            source_state.upper.update_iter(changes.drain()).collect();

                        if !changes.is_empty() {
                            // The source's durability frontier changed as a result of the updates sent over
                            // by the dataflow workers. Advance the durability frontier known to the dataflow worker
                            // to indicate that these bindings have been persisted.
                            durability_updates
                                .push((source_id, source_state.upper.frontier().to_owned()));

                            // Allow compaction to advance.
                            if let Some(compaction_window_ms) = source_state.compaction_window_ms {
                                if !source_state.upper.frontier().is_empty() {
                                    self.since_handles
                                        .get_mut(&source_id)
                                        .unwrap()
                                        .maybe_advance(source_state.upper.frontier().iter().map(
                                            |time| {
                                                compaction_window_ms
                                                    * (time.saturating_sub(compaction_window_ms)
                                                        / compaction_window_ms)
                                            },
                                        ));
                                }
                            }
                        }

                        // Let's also check to see if we can compact any of the bindings we've received.
                        let compaction_ts = *source_state
                            .since
                            .borrow()
                            .frontier()
                            .first()
                            .expect("known to exist");

                        timestamp_compactions.push((source_id, compaction_ts));
                    }
                }

                self.catalog
                    .compact_timestamp_bindings(&timestamp_compactions)
                    .expect("compacting timestamp bindings cannot fail");

                // Announce the new frontiers that have been durably persisted.
                if !durability_updates.is_empty() {
                    self.dataflow_client
                        .storage()
                        .update_durability_frontiers(durability_updates)
                        .await
                        .unwrap();
                }
            }
        }
    }

    async fn message_statement_ready(
        &mut self,
        StatementReady {
            mut session,
            tx,
            result,
            params,
        }: StatementReady,
    ) {
        match future::ready(result)
            .and_then(|stmt| self.handle_statement(&mut session, stmt, &params))
            .await
        {
            Ok(plan) => self.sequence_plan(tx, session, plan).await,
            Err(e) => tx.send(Err(e), session),
        }
    }

    async fn message_sink_connector_ready(
        &mut self,
        SinkConnectorReady {
            session,
            tx,
            id,
            oid,
            result,
        }: SinkConnectorReady,
    ) {
        match result {
            Ok(connector) => {
                // NOTE: we must not fail from here on out. We have a
                // connector, which means there is external state (like
                // a Kafka topic) that's been created on our behalf. If
                // we fail now, we'll leak that external state.
                if self.catalog.try_get_by_id(id).is_some() {
                    // TODO(benesch): this `expect` here is possibly scary, but
                    // no better solution presents itself. Possibly sinks should
                    // have an error bit, and an error here would set the error
                    // bit on the sink.
                    self.handle_sink_connector_ready(id, oid, connector)
                        .await
                        .expect("sinks should be validated by sequence_create_sink");
                } else {
                    // Another session dropped the sink while we were
                    // creating the connector. Report to the client that
                    // we created the sink, because from their
                    // perspective we did, as there is state (e.g. a
                    // Kafka topic) they need to clean up.
                }
                tx.send(Ok(ExecuteResponse::CreatedSink { existed: false }), session);
            }
            Err(e) => {
                // Drop the placeholder sink if still present.
                if self.catalog.try_get_by_id(id).is_some() {
                    self.catalog_transact(vec![catalog::Op::DropItem(id)], |_builder| Ok(()))
                        .await
                        .expect("deleting placeholder sink cannot fail");
                } else {
                    // Another session may have dropped the placeholder sink while we were
                    // attempting to create the connector, in which case we don't need to do
                    // anything.
                }
                tx.send(Err(e), session);
            }
        }
    }

    fn message_send_diffs(
        &mut self,
        SendDiffs {
            mut session,
            tx,
            id,
            diffs,
            kind,
        }: SendDiffs,
    ) {
        match diffs {
            Ok(diffs) => {
                tx.send(
                    self.sequence_send_diffs(
                        &mut session,
                        SendDiffsPlan {
                            id,
                            updates: diffs,
                            kind,
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

    async fn message_scrape_metrics(&mut self) {
        let scraped_metrics = self.metric_scraper.scrape_once();
        self.send_builtin_table_updates_at_offset(scraped_metrics)
            .await;
    }

    async fn message_command(&mut self, cmd: Command) {
        match cmd {
            Command::Startup {
                session,
                create_user_if_not_exists,
                cancel_tx,
                tx,
            } => {
                if let Err(e) = self.catalog.create_temporary_schema(session.conn_id()) {
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
                            result: Err(CoordError::UnknownLoginRole(session.user().into())),
                            session,
                        });
                        return;
                    }
                    let plan = CreateRolePlan {
                        name: session.user().to_string(),
                    };
                    if let Err(err) = self.sequence_create_role(plan).await {
                        let _ = tx.send(Response {
                            result: Err(err),
                            session,
                        });
                        return;
                    }
                }

                let mut messages = vec![];
                let catalog = self.catalog.for_session(&session);
                if catalog
                    .resolve_database(catalog.default_database())
                    .is_err()
                {
                    messages.push(StartupMessage::UnknownSessionDatabase(
                        catalog.default_database().into(),
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

                ClientTransmitter::new(tx).send(
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
            } => {
                let result = session
                    .get_portal(&portal_name)
                    .ok_or(CoordError::UnknownCursor(portal_name));
                let portal = match result {
                    Ok(portal) => portal,
                    Err(e) => {
                        let _ = tx.send(Response {
                            result: Err(e),
                            session,
                        });
                        return;
                    }
                };
                let stmt = portal.stmt.clone();
                let params = portal.parameters.clone();

                match stmt {
                    Some(stmt) => {
                        // Verify that this statetement type can be executed in the current
                        // transaction state.
                        match session.transaction() {
                            // By this point we should be in a running transaction.
                            &TransactionStatus::Default => unreachable!(),

                            // Started is almost always safe (started means there's a single statement
                            // being executed). Failed transactions have already been checked in pgwire for
                            // a safe statement (COMMIT, ROLLBACK, etc.) and can also proceed.
                            &TransactionStatus::Started(_) | &TransactionStatus::Failed(_) => {
                                if let Statement::Declare(_) = stmt {
                                    // Declare is an exception. Although it's not against any spec to execute
                                    // it, it will always result in nothing happening, since all portals will be
                                    // immediately closed. Users don't know this detail, so this error helps them
                                    // understand what's going wrong. Postgres does this too.
                                    let _ = tx.send(Response {
                                        result: Err(CoordError::OperationRequiresTransaction(
                                            "DECLARE CURSOR".into(),
                                        )),
                                        session,
                                    });
                                    return;
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
                            &TransactionStatus::InTransactionImplicit(_)
                            | &TransactionStatus::InTransaction(_) => match stmt {
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
                                | Statement::ShowDatabases(_)
                                | Statement::ShowIndexes(_)
                                | Statement::ShowObjects(_)
                                | Statement::ShowVariable(_)
                                | Statement::SetVariable(_)
                                | Statement::StartTransaction(_)
                                | Statement::Tail(_)
                                | Statement::Raise(_) => {
                                    // Always safe.
                                }

                                Statement::Insert(ref insert_statment)
                                    if matches!(
                                        insert_statment.source,
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
                                | Statement::AlterObjectRename(_)
                                | Statement::CreateDatabase(_)
                                | Statement::CreateIndex(_)
                                | Statement::CreateRole(_)
                                | Statement::CreateSchema(_)
                                | Statement::CreateSink(_)
                                | Statement::CreateSource(_)
                                | Statement::CreateTable(_)
                                | Statement::CreateType(_)
                                | Statement::CreateView(_)
                                | Statement::CreateViews(_)
                                | Statement::Delete(_)
                                | Statement::DropDatabase(_)
                                | Statement::DropObjects(_)
                                | Statement::Insert(_)
                                | Statement::Update(_) => {
                                    let _ = tx.send(Response {
                                        result: Err(CoordError::OperationProhibitsTransaction(
                                            stmt.to_string(),
                                        )),
                                        session,
                                    });
                                    return;
                                }
                            },
                        }

                        if self.catalog.config().safe_mode {
                            if let Err(e) = check_statement_safety(&stmt) {
                                let _ = tx.send(Response {
                                    result: Err(e),
                                    session,
                                });
                                return;
                            }
                        }

                        let internal_cmd_tx = self.internal_cmd_tx.clone();
                        let catalog = self.catalog.for_session(&session);
                        let purify_fut = mz_sql::pure::purify(&catalog, stmt);
                        let conn_id = session.conn_id();
                        task::spawn(|| format!("purify:{conn_id}"), async move {
                            let result = purify_fut.await.map_err(|e| e.into());
                            internal_cmd_tx
                                .send(Message::StatementReady(StatementReady {
                                    session,
                                    tx: ClientTransmitter::new(tx),
                                    result,
                                    params,
                                }))
                                .expect("sending to internal_cmd_tx cannot fail");
                        });
                    }
                    None => {
                        let _ = tx.send(Response {
                            result: Ok(ExecuteResponse::EmptyQuery),
                            session,
                        });
                    }
                }
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
                    None => session.start_transaction(now, None),
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
                let tx = ClientTransmitter::new(tx);
                self.sequence_end_transaction(tx, session, action).await;
            }

            Command::VerifyPreparedStatement {
                name,
                mut session,
                tx,
            } => {
                let result = self.handle_verify_prepared_statement(&mut session, &name);
                let _ = tx.send(Response { result, session });
            }
        }
    }

    /// Validate that all upper frontier updates obey the following invariants:
    ///
    /// 1. The `upper` frontier for each source, index and sink does not go backwards with
    /// upper updates
    /// 2. `upper` never contains any times with negative multiplicity.
    /// 3. `upper` never contains any times with multiplicity greater than `1`.
    /// 4. No updates increase the sum of all multiplicities in `upper`.
    ///
    /// Note that invariants 2 - 4 require single dimensional time, and a fixed number of
    /// dataflow workers. If we migrate to multidimensional time then 2 no longer holds, and
    /// 3. relaxes to "the longest chain in `upper` has to have <= n_workers elements" and
    /// 4. relaxes to "no comparable updates increase the sum of all multiplicities in `upper`".
    /// If we ever switch to dynamically scaling the number of dataflow workers then 3 and 4 no
    /// longer hold.
    fn validate_update_iter(
        upper: &mut MutableAntichain<Timestamp>,
        mut changes: ChangeBatch<Timestamp>,
    ) -> Vec<(Timestamp, i64)> {
        let old_frontier = upper.frontier().to_owned();

        // Validate that no changes correspond to a net addition in the sum of all multiplicities.
        // All updates have to relinquish a time, and optionally, acquire another time.
        // TODO: generalize this to multidimensional times.
        let total_changes = changes
            .iter()
            .map(|(_, change)| *change)
            .fold(0, |acc, x| acc + x);
        assert!(total_changes <= 0);

        let frontier_changes = upper.update_iter(changes.clone().drain()).collect();

        // Make sure no times in `upper` have a negative multiplicity
        for (t, _) in changes.into_inner() {
            let count = upper.count_for(&t);
            assert!(count >= 0);
            assert!(count as usize <= 1);
        }

        assert!(<_ as PartialOrder>::less_equal(
            &old_frontier.borrow(),
            &upper.frontier(),
        ));

        frontier_changes
    }

    /// Updates the upper frontier of a maintained arrangement or sink.
    fn update_upper(&mut self, name: &GlobalId, changes: ChangeBatch<Timestamp>) {
        if let Some(index_state) = self.indexes.get_mut(name) {
            let changes = Self::validate_update_iter(&mut index_state.upper, changes);

            if !changes.is_empty() {
                // Advance the compaction frontier to trail the new frontier.
                // If the compaction latency is `None` compaction messages are
                // not emitted, and the trace should be broadly useable.
                // TODO: If the frontier advances surprisingly quickly, e.g. in
                // the case of a constant collection, this compaction is actively
                // harmful. We should reconsider compaction policy with an eye
                // towards minimizing unexpected screw-ups.
                if let Some(compaction_window_ms) = index_state.compaction_window_ms {
                    // Decline to compact complete collections. This would have the
                    // effect of making the collection unusable. Instead, we would
                    // prefer to compact collections only when we believe it would
                    // reduce the volume of the collection, but we don't have that
                    // information here.
                    if !index_state.upper.frontier().is_empty() {
                        // The since_handle for this GlobalId should have already been registered with
                        // an AntichainToken. Advance it. Changes to the AntichainToken's frontier
                        // will propagate to the Frontiers' since, and changes to that will propate to
                        // self.since_updates.
                        self.since_handles.get_mut(name).unwrap().maybe_advance(
                            index_state.upper.frontier().iter().map(|time| {
                                compaction_window_ms
                                    * (time.saturating_sub(compaction_window_ms)
                                        / compaction_window_ms)
                            }),
                        );
                    }
                }
            }
        } else if self.sources.get_mut(name).is_some() {
            panic!(
                "expected an update for an index or sink, instead got update for source {}",
                name
            );
        } else if let Some(sink_state) = self.sink_writes.get_mut(name) {
            // Only one dataflow worker should give updates for sinks
            let changes = Self::validate_update_iter(&mut sink_state.frontier, changes);

            if !changes.is_empty() {
                sink_state.advance_source_handles();
            }
        }
    }

    /// Allows compaction of identified collections through the indicated frontiers.
    fn persisted_table_allow_compaction(
        &mut self,
        since_updates: &[(GlobalId, Antichain<Timestamp>)],
    ) {
        // The updates of `since_updates` identified by a persistence-internal `stream_id`.
        let mut persistence_since_updates = vec![];

        for (id, frontier) in since_updates.iter() {
            // HACK: Avoid the "failed to compact persisted tables" error log at
            // restart, by not trying to allow compaction on the minimum
            // timestamp.
            if !frontier
                .elements()
                .iter()
                .any(|x| *x > Timestamp::minimum())
            {
                continue;
            }

            if let Some(persist) = self.persister.table_details.get(id) {
                persistence_since_updates.push((persist.stream_id, frontier.clone()));
            }
        }

        // The persistence source that is backing a table on workers does not send frontier updates
        // back to the coordinator. We update our internal bookkeeping here, because we also
        // forward the compaction frontier here and therefore know that the since advances.
        for (id, frontier) in since_updates {
            let since_handle = self
                .since_handles
                .get_mut(&id)
                .expect("missing since handle");

            since_handle.maybe_advance(frontier.iter().cloned());
        }

        if !persistence_since_updates.is_empty() {
            let persist_multi = match &mut self.persister.table_writer {
                Some(multi) => multi,
                None => {
                    error!("internal error: persist_multi_details invariant violated");
                    return;
                }
            };

            let compaction_fut = persist_multi.allow_compaction(&persistence_since_updates);
            let _ = task::spawn(
                // TODO(guswynn): Add more relevant info here
                || "compaction",
                async move {
                    if let Err(err) = compaction_fut.await {
                        // TODO: Do something smarter here
                        error!("failed to compact persisted tables: {}", err);
                    }
                },
            );
        }
    }

    /// Perform maintenance work associated with the coordinator.
    ///
    /// Primarily, this involves sequencing compaction commands, which should be
    /// issued whenever available.
    async fn maintenance(&mut self) {
        // Take this opportunity to drain `since_update` commands.
        // Don't try to compact to an empty frontier. There may be a good reason to do this
        // in principle, but not in any current Mz use case.
        // (For background, see: https://github.com/MaterializeInc/materialize/pull/1113#issuecomment-559281990)

        let index_since_updates: Vec<_> = self
            .index_since_updates
            .borrow_mut()
            .drain()
            .filter(|(_, frontier)| frontier != &Antichain::new())
            .collect();

        if !index_since_updates.is_empty() {
            // Error value is ignored because this call attempts to modify ids
            // for indexes that have not been installed. See above, presumably.
            self.dataflow_client
                .compute(DEFAULT_COMPUTE_INSTANCE_ID)
                .unwrap()
                .allow_compaction(index_since_updates)
                .await;
        }

        let source_since_updates: Vec<_> = self
            .source_since_updates
            .borrow_mut()
            .drain()
            .filter(|(_, frontier)| frontier != &Antichain::new())
            .collect();

        if !source_since_updates.is_empty() {
            self.persisted_table_allow_compaction(&source_since_updates);
            self.dataflow_client
                .storage()
                .allow_compaction(source_since_updates)
                .await
                .unwrap();
        }
    }

    async fn handle_statement(
        &mut self,
        session: &mut Session,
        stmt: mz_sql::ast::Statement<Raw>,
        params: &mz_sql::plan::Params,
    ) -> Result<mz_sql::plan::Plan, CoordError> {
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
    ) -> Result<(), CoordError> {
        let desc = describe(&self.catalog, stmt.clone(), &param_types, session)?;
        let params = vec![];
        let result_formats = vec![mz_pgrepr::Format::Text; desc.arity()];
        session.set_portal(name, desc, Some(stmt), params, result_formats)?;
        Ok(())
    }

    fn handle_describe(
        &self,
        session: &mut Session,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), CoordError> {
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
    ) -> Result<StatementDesc, CoordError> {
        if let Some(stmt) = stmt {
            describe(&self.catalog, stmt, &param_types, session)
        } else {
            Ok(StatementDesc::new(None))
        }
    }

    /// Verify a prepared statement is still valid.
    fn handle_verify_prepared_statement(
        &self,
        session: &mut Session,
        name: &str,
    ) -> Result<(), CoordError> {
        let ps = match session.get_prepared_statement_unverified(&name) {
            Some(ps) => ps,
            None => return Err(CoordError::UnknownPreparedStatement(name.to_string())),
        };
        if ps.catalog_revision != self.catalog.transient_revision() {
            let desc = self.describe(
                session,
                ps.sql().cloned(),
                ps.desc()
                    .param_types
                    .iter()
                    .map(|ty| Some(ty.clone()))
                    .collect(),
            )?;
            if &desc != ps.desc() {
                Err(CoordError::ChangedPlan)
            } else {
                // If the descs are the same, we can bump our version to declare that ps is
                // correct as of now.
                let ps = session
                    .get_prepared_statement_mut_unverified(name)
                    .expect("known to exist");
                ps.catalog_revision = self.catalog.transient_revision();
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    /// Instruct the dataflow layer to cancel any ongoing, interactive work for
    /// the named `conn_id`.
    async fn handle_cancel(&mut self, conn_id: u32, secret_key: u32) {
        if let Some(conn_meta) = self.active_conns.get(&conn_id) {
            // If the secret key specified by the client doesn't match the
            // actual secret key for the target connection, we treat this as a
            // rogue cancellation request and ignore it.
            if conn_meta.secret_key != secret_key {
                return;
            }

            // Cancel deferred writes. There is at most one pending write per session.
            if let Some(idx) = self
                .write_lock_wait_group
                .iter()
                .position(|ready| ready.session.conn_id() == conn_id)
            {
                let ready = self.write_lock_wait_group.remove(idx).unwrap();
                ready.tx.send(Ok(ExecuteResponse::Canceled), ready.session);
            }

            // Inform the target session (if it asks) about the cancellation.
            let _ = conn_meta.cancel_tx.send(Canceled::Canceled);

            // Allow dataflow to cancel any pending peeks.
            self.dataflow_client
                .compute(DEFAULT_COMPUTE_INSTANCE_ID)
                .unwrap()
                .cancel_peek(conn_id)
                .await
                .unwrap();
        }
    }

    /// Handle termination of a client session.
    ///
    /// This cleans up any state in the coordinator associated with the session.
    async fn handle_terminate(&mut self, session: &mut Session) {
        self.clear_transaction(session).await;

        self.drop_temp_items(session.conn_id()).await;
        self.catalog
            .drop_temporary_schema(session.conn_id())
            .expect("unable to drop temporary schema");
        self.active_conns.remove(&session.conn_id());
    }

    /// Handle removing in-progress transaction state regardless of the end action
    /// of the transaction.
    async fn clear_transaction(&mut self, session: &mut Session) -> TransactionStatus {
        let (drop_sinks, txn) = session.clear_transaction();
        self.drop_sinks(drop_sinks).await;

        // Allow compaction of sources from this transaction.
        self.txn_reads.remove(&session.conn_id());

        txn
    }

    /// Removes all temporary items created by the specified connection, though
    /// not the temporary schema itself.
    async fn drop_temp_items(&mut self, conn_id: u32) {
        let ops = self.catalog.drop_temp_item_ops(conn_id);
        self.catalog_transact(ops, |_builder| Ok(()))
            .await
            .expect("unable to drop temporary items for conn_id");
    }

    async fn handle_sink_connector_ready(
        &mut self,
        id: GlobalId,
        oid: u32,
        connector: SinkConnector,
    ) -> Result<(), CoordError> {
        // Update catalog entry with sink connector.
        let entry = self.catalog.get_by_id(&id);
        let name = entry.name().clone();
        let mut sink = match entry.item() {
            CatalogItem::Sink(sink) => sink.clone(),
            _ => unreachable!(),
        };
        sink.connector = catalog::SinkConnectorState::Ready(connector.clone());
        let as_of = SinkAsOf {
            frontier: self.determine_frontier(&[sink.from]),
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
            .catalog_transact(ops, |mut builder| {
                let sink_description = mz_dataflow_types::sinks::SinkDesc {
                    from: sink.from,
                    from_desc: builder
                        .catalog
                        .get_by_id(&sink.from)
                        .desc()
                        .unwrap()
                        .clone(),
                    connector: connector.clone(),
                    envelope: Some(sink.envelope),
                    as_of,
                };
                Ok(builder.build_sink_dataflow(name.to_string(), id, sink_description)?)
            })
            .await?;

        // For some sinks, we need to block compaction of each timestamp binding
        // until all sinks that depend on a given source have finished writing out that timestamp.
        // To achieve that, each sink will hold a AntichainToken for all of the sources it depends
        // on, and will advance all of its source dependencies' compaction frontiers as it completes
        // writes.
        if connector.requires_source_compaction_holdback() {
            let mut tokens = Vec::new();

            // Collect AntichainTokens from all of the sources that have them.
            for id in connector.transitive_source_dependencies() {
                if let Some(token) = self.since_handles.get(&id) {
                    tokens.push(token.clone());
                }
            }

            let sink_writes = SinkWrites::new(tokens);
            self.sink_writes.insert(id, sink_writes);
        }
        Ok(self.ship_dataflow(df).await)
    }

    async fn sequence_plan(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: Plan,
    ) {
        match plan {
            Plan::CreateDatabase(plan) => {
                tx.send(self.sequence_create_database(plan).await, session);
            }
            Plan::CreateSchema(plan) => {
                tx.send(self.sequence_create_schema(plan).await, session);
            }
            Plan::CreateRole(plan) => {
                tx.send(self.sequence_create_role(plan).await, session);
            }
            Plan::CreateTable(plan) => {
                tx.send(self.sequence_create_table(&session, plan).await, session);
            }
            Plan::CreateSource(plan) => {
                tx.send(
                    self.sequence_create_source(&mut session, plan).await,
                    session,
                );
            }
            Plan::CreateSink(plan) => {
                self.sequence_create_sink(session, plan, tx).await;
            }
            Plan::CreateView(plan) => {
                tx.send(self.sequence_create_view(&session, plan).await, session);
            }
            Plan::CreateViews(plan) => {
                tx.send(
                    self.sequence_create_views(&mut session, plan).await,
                    session,
                );
            }
            Plan::CreateIndex(plan) => {
                tx.send(self.sequence_create_index(plan).await, session);
            }
            Plan::CreateType(plan) => {
                tx.send(self.sequence_create_type(plan).await, session);
            }
            Plan::DropDatabase(plan) => {
                tx.send(self.sequence_drop_database(plan).await, session);
            }
            Plan::DropSchema(plan) => {
                tx.send(self.sequence_drop_schema(plan).await, session);
            }
            Plan::DropRoles(plan) => {
                tx.send(self.sequence_drop_roles(plan).await, session);
            }
            Plan::DropItems(plan) => {
                tx.send(self.sequence_drop_items(plan).await, session);
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
            Plan::StartTransaction(plan) => {
                let duplicated =
                    matches!(session.transaction(), TransactionStatus::InTransaction(_));
                let session = session.start_transaction(self.now_datetime(), plan.access);
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
                tx.send(self.sequence_tail(&mut session, plan).await, session);
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
                tx.send(self.sequence_alter_item_rename(plan).await, session);
            }
            Plan::AlterIndexSetOptions(plan) => {
                tx.send(self.sequence_alter_index_set_options(plan), session);
            }
            Plan::AlterIndexResetOptions(plan) => {
                tx.send(self.sequence_alter_index_reset_options(plan), session);
            }
            Plan::AlterIndexEnable(plan) => {
                tx.send(self.sequence_alter_index_enable(plan).await, session);
            }
            Plan::DiscardTemp => {
                self.drop_temp_items(session.conn_id()).await;
                tx.send(Ok(ExecuteResponse::DiscardedTemp), session);
            }
            Plan::DiscardAll => {
                let ret = if let TransactionStatus::Started(_) = session.transaction() {
                    self.drop_temp_items(session.conn_id()).await;
                    let drop_sinks = session.reset();
                    self.drop_sinks(drop_sinks).await;
                    Ok(ExecuteResponse::DiscardedAll)
                } else {
                    Err(CoordError::OperationProhibitsTransaction(
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
                    tx.send(Err(CoordError::UnknownCursor(plan.name)), session);
                }
            }
            Plan::Prepare(plan) => {
                if session
                    .get_prepared_statement_unverified(&plan.name)
                    .is_some()
                {
                    tx.send(Err(CoordError::PreparedStatementExists(plan.name)), session);
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
                let plan_name = plan.name.clone();
                match self.sequence_execute(&mut session, plan) {
                    Ok(portal_name) => {
                        let internal_cmd_tx = self.internal_cmd_tx.clone();
                        task::spawn(|| format!("execute:{plan_name}"), async move {
                            internal_cmd_tx
                                .send(Message::Command(Command::Execute {
                                    portal_name,
                                    session,
                                    tx: tx.take(),
                                }))
                                .expect("sending to internal_cmd_tx cannot fail");
                        });
                    }
                    Err(err) => tx.send(Err(err), session),
                };
            }
            Plan::Deallocate(plan) => match plan.name {
                Some(name) => {
                    if session.remove_prepared_statement(&name) {
                        tx.send(Ok(ExecuteResponse::Deallocate { all: false }), session);
                    } else {
                        tx.send(Err(CoordError::UnknownPreparedStatement(name)), session);
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
    ) -> Result<String, CoordError> {
        // Verify the stmt is still valid.
        self.handle_verify_prepared_statement(session, &plan.name)?;
        let ps = session.get_prepared_statement_unverified(&plan.name);
        match ps {
            Some(ps) => {
                let sql = ps.sql().cloned();
                let desc = ps.desc().clone();
                session.create_new_portal(sql, desc, plan.params, Vec::new())
            }
            None => Err(CoordError::UnknownPreparedStatement(plan.name)),
        }
    }

    async fn sequence_create_database(
        &mut self,
        plan: CreateDatabasePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let db_oid = self.catalog.allocate_oid()?;
        let schema_oid = self.catalog.allocate_oid()?;
        let ops = vec![
            catalog::Op::CreateDatabase {
                name: plan.name.clone(),
                oid: db_oid,
            },
            catalog::Op::CreateSchema {
                database_name: DatabaseSpecifier::Name(plan.name),
                schema_name: "public".into(),
                oid: schema_oid,
            },
        ];
        match self.catalog_transact(ops, |_builder| Ok(())).await {
            Ok(_) => Ok(ExecuteResponse::CreatedDatabase { existed: false }),
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::DatabaseAlreadyExists(_),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedDatabase { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_schema(
        &mut self,
        plan: CreateSchemaPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateSchema {
            database_name: plan.database_name,
            schema_name: plan.schema_name,
            oid,
        };
        match self.catalog_transact(vec![op], |_builder| Ok(())).await {
            Ok(_) => Ok(ExecuteResponse::CreatedSchema { existed: false }),
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::SchemaAlreadyExists(_),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedSchema { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_role(
        &mut self,
        plan: CreateRolePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateRole {
            name: plan.name,
            oid,
        };
        self.catalog_transact(vec![op], |_builder| Ok(()))
            .await
            .map(|_| ExecuteResponse::CreatedRole)
    }

    async fn sequence_create_table(
        &mut self,
        session: &Session,
        plan: CreateTablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
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
        let table_id = self.catalog.allocate_id()?;
        let mut index_depends_on = table.depends_on.clone();
        index_depends_on.push(table_id);
        let table = catalog::Table {
            create_sql: table.create_sql,
            desc: table.desc,
            defaults: table.defaults,
            conn_id,
            depends_on: table.depends_on,
            persist_name: self
                .persister
                .new_table_persist_name(table_id, &name.to_string()),
        };
        let index_id = self.catalog.allocate_id()?;
        let mut index_name = name.clone();
        index_name.item += "_primary_idx";
        index_name = self
            .catalog
            .for_session(session)
            .find_available_name(index_name);
        let index = auto_generate_primary_idx(
            index_name.item.clone(),
            name.clone(),
            table_id,
            &table.desc,
            conn_id,
            index_depends_on,
            self.catalog.index_enabled_by_default(&index_id),
        );
        let table_oid = self.catalog.allocate_oid()?;
        let index_oid = self.catalog.allocate_oid()?;
        let df = self
            .catalog_transact(
                vec![
                    catalog::Op::CreateItem {
                        id: table_id,
                        oid: table_oid,
                        name,
                        item: CatalogItem::Table(table.clone()),
                    },
                    catalog::Op::CreateItem {
                        id: index_id,
                        oid: index_oid,
                        name: index_name,
                        item: CatalogItem::Index(index),
                    },
                ],
                |mut builder| {
                    if let Some((name, description)) =
                        Self::prepare_index_build(builder.catalog, &index_id)
                    {
                        let df = builder.build_index_dataflow(name, index_id, description)?;
                        Ok(Some(df))
                    } else {
                        Ok(None)
                    }
                },
            )
            .await;
        match df {
            Ok(df) => {
                // Determine the initial validity for the table.
                self.persister
                    .add_table(table_id, &table)
                    .map_err(CoordError::Persistence)?;
                let since_ts = self
                    .persister
                    .table_details
                    .get(&table_id)
                    .map(|td| td.since_ts)
                    .unwrap_or(0);

                // Announce the creation of the table source.
                let source_description = self
                    .catalog
                    .state()
                    .source_description_for(table_id)
                    .unwrap();
                self.dataflow_client
                    .storage()
                    .create_sources(vec![CreateSourceCommand {
                        id: table_id,
                        desc: source_description,
                        since: Antichain::from_elem(since_ts),
                        ts_bindings: vec![],
                    }])
                    .await
                    .unwrap();
                // Install the dataflow if so required.
                if let Some(df) = df {
                    let frontiers = self.new_source_frontiers(
                        table_id,
                        [since_ts],
                        self.logical_compaction_window_ms,
                    );

                    // NOTE: Tables are not sources, but to a large part of the system they look
                    // like they are, e.g. they are rendered as a SourceConnector::Local.
                    self.sources.insert(table_id, frontiers);

                    self.ship_dataflow(df).await;
                }
                Ok(ExecuteResponse::CreatedTable { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
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
    ) -> Result<ExecuteResponse, CoordError> {
        let if_not_exists = plan.if_not_exists;
        let (metadata, ops) = self.generate_create_source_ops(session, vec![plan])?;
        match self
            .catalog_transact(ops, move |mut builder| {
                let mut dfs = Vec::new();
                let mut source_ids = Vec::new();
                for (source_id, idx_id) in metadata {
                    source_ids.push(source_id);
                    if let Some(index_id) = idx_id {
                        if let Some((name, description)) =
                            Self::prepare_index_build(builder.catalog, &index_id)
                        {
                            let df = builder.build_index_dataflow(name, index_id, description)?;
                            dfs.push(df);
                        }
                    }
                }
                Ok((dfs, source_ids))
            })
            .await
        {
            Ok((dfs, source_ids)) => {
                // Do everything to instantiate the source at the coordinator and
                // inform the timestamper and dataflow workers of its existence before
                // shipping any dataflows that depend on its existence.
                let catalog_state = self.catalog.state();

                // Ask persistence if it has a since timestamps for any
                // of the new sources.
                let since_timestamps = source_ids
                    .iter()
                    .map(|id| {
                        let source = catalog_state.get_by_id(&id).source().ok_or_else(|| {
                            CoordError::Internal(format!("ID {} unexpectedly not a source", id))
                        })?;
                        let since_ts = self
                            .persister
                            .load_source_persist_desc(&source)
                            .map_err(CoordError::Persistence)?
                            .map(|p| p.since_ts)
                            .unwrap_or(0);
                        Ok::<_, CoordError>(since_ts)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let descriptions = source_ids
                    .iter()
                    .map(|id| catalog_state.source_description_for(*id).unwrap())
                    .collect::<Vec<_>>();

                // Continue to do those things.
                let mut source_descriptions = Vec::with_capacity(source_ids.len());
                for ((source_id, since_ts), description) in source_ids
                    .into_iter()
                    .zip_eq(since_timestamps)
                    .zip_eq(descriptions)
                {
                    let ts_bindings = self
                        .catalog
                        .load_timestamp_bindings(source_id)
                        .expect("loading timestamps from coordinator cannot fail");

                    let frontiers = self.new_source_frontiers(
                        source_id,
                        [since_ts],
                        self.logical_compaction_window_ms,
                    );
                    self.sources.insert(source_id, frontiers);
                    source_descriptions.push(CreateSourceCommand {
                        id: source_id,
                        desc: description,
                        since: Antichain::from_elem(since_ts),
                        ts_bindings,
                    });
                }

                self.dataflow_client
                    .storage()
                    .create_sources(source_descriptions)
                    .await
                    .unwrap();
                self.ship_dataflows(dfs).await;
                Ok(ExecuteResponse::CreatedSource { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedSource { existed: true }),
            Err(err) => Err(err),
        }
    }

    fn generate_create_source_ops(
        &mut self,
        session: &mut Session,
        plans: Vec<CreateSourcePlan>,
    ) -> Result<(Vec<(GlobalId, Option<GlobalId>)>, Vec<catalog::Op>), CoordError> {
        let mut metadata = vec![];
        let mut ops = vec![];
        for plan in plans {
            let CreateSourcePlan {
                name,
                source,
                materialized,
                ..
            } = plan;
            let source_id = self.catalog.allocate_id()?;
            let source_oid = self.catalog.allocate_oid()?;

            let persist_details = self.persister.new_serialized_source_persist_details(
                source_id,
                &source.connector,
                &name.to_string(),
            );

            let source = catalog::Source {
                create_sql: source.create_sql,
                connector: source.connector,
                persist_details,
                desc: source.desc,
            };
            ops.push(catalog::Op::CreateItem {
                id: source_id,
                oid: source_oid,
                name: name.clone(),
                item: CatalogItem::Source(source.clone()),
            });
            let index_id = if materialized {
                let mut index_name = name.clone();
                index_name.item += "_primary_idx";
                index_name = self
                    .catalog
                    .for_session(session)
                    .find_available_name(index_name);
                let index_id = self.catalog.allocate_id()?;
                let index = auto_generate_primary_idx(
                    index_name.item.clone(),
                    name,
                    source_id,
                    &source.desc,
                    None,
                    vec![source_id],
                    self.catalog.index_enabled_by_default(&index_id),
                );
                let index_oid = self.catalog.allocate_oid()?;
                ops.push(catalog::Op::CreateItem {
                    id: index_id,
                    oid: index_oid,
                    name: index_name,
                    item: CatalogItem::Index(index),
                });
                Some(index_id)
            } else {
                None
            };
            metadata.push((source_id, index_id))
        }
        Ok((metadata, ops))
    }

    async fn sequence_create_sink(
        &mut self,
        session: Session,
        plan: CreateSinkPlan,
        tx: ClientTransmitter<ExecuteResponse>,
    ) {
        let CreateSinkPlan {
            name,
            sink,
            with_snapshot,
            if_not_exists,
        } = plan;

        // First try to allocate an ID and an OID. If either fails, we're done.
        let id = match self.catalog.allocate_id() {
            Ok(id) => id,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };
        let oid = match self.catalog.allocate_oid() {
            Ok(id) => id,
            Err(e) => {
                tx.send(Err(e.into()), session);
                return;
            }
        };

        // Then try to create a placeholder catalog item with an unknown
        // connector. If that fails, we're done, though if the client specified
        // `if_not_exists` we'll tell the client we succeeded.
        //
        // This placeholder catalog item reserves the name while we create
        // the sink connector, which could take an arbitrarily long time.
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Sink(catalog::Sink {
                create_sql: sink.create_sql,
                from: sink.from,
                connector: catalog::SinkConnectorState::Pending(sink.connector_builder.clone()),
                envelope: sink.envelope,
                with_snapshot,
                depends_on: sink.depends_on,
            }),
        };

        let transact_result = self
            .catalog_transact(vec![op], |mut builder| -> Result<(), CoordError> {
                // Insert a dummy dataflow to trigger validation before we try to actually create
                // the external sink resources (e.g. Kafka Topics)
                builder
                    .build_sink_dataflow(
                        "dummy".into(),
                        id,
                        mz_dataflow_types::sinks::SinkDesc {
                            from: sink.from,
                            from_desc: builder
                                .catalog
                                .get_by_id(&sink.from)
                                .desc()
                                .unwrap()
                                .clone(),
                            connector: SinkConnector::Tail(TailSinkConnector {}),
                            envelope: Some(sink.envelope),
                            as_of: SinkAsOf {
                                frontier: Antichain::new(),
                                strict: false,
                            },
                        },
                    )
                    .map(|_ok| ())
            })
            .await;
        match transact_result {
            Ok(()) => (),
            Err(CoordError::Catalog(catalog::Error {
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

        // Now we're ready to create the sink connector. Arrange to notify the
        // main coordinator thread when the future completes.
        let connector_builder = sink.connector_builder;
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(
            || format!("sink_connector_ready:{}", sink.from),
            async move {
                internal_cmd_tx
                    .send(Message::SinkConnectorReady(SinkConnectorReady {
                        session,
                        tx,
                        id,
                        oid,
                        result: sink_connector::build(connector_builder, id).await,
                    }))
                    .expect("sending to internal_cmd_tx cannot fail");
            },
        );
    }

    fn generate_view_ops(
        &mut self,
        session: &Session,
        name: FullName,
        view: View,
        replace: Option<GlobalId>,
        materialize: bool,
    ) -> Result<(Vec<catalog::Op>, Option<GlobalId>), CoordError> {
        self.validate_timeline(view.expr.global_uses())?;

        let mut ops = vec![];

        if let Some(id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[id]));
        }
        let view_id = self.catalog.allocate_id()?;
        let view_oid = self.catalog.allocate_oid()?;
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
            depends_on: view.depends_on,
        };
        ops.push(catalog::Op::CreateItem {
            id: view_id,
            oid: view_oid,
            name: name.clone(),
            item: CatalogItem::View(view.clone()),
        });
        let index_id = if materialize {
            let mut index_name = name.clone();
            index_name.item += "_primary_idx";
            index_name = self
                .catalog
                .for_session(session)
                .find_available_name(index_name);
            let index_id = self.catalog.allocate_id()?;
            let index = auto_generate_primary_idx(
                index_name.item.clone(),
                name,
                view_id,
                &view.desc,
                view.conn_id,
                vec![view_id],
                self.catalog.index_enabled_by_default(&index_id),
            );
            let index_oid = self.catalog.allocate_oid()?;
            ops.push(catalog::Op::CreateItem {
                id: index_id,
                oid: index_oid,
                name: index_name,
                item: CatalogItem::Index(index),
            });
            Some(index_id)
        } else {
            None
        };

        Ok((ops, index_id))
    }

    async fn sequence_create_view(
        &mut self,
        session: &Session,
        plan: CreateViewPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let if_not_exists = plan.if_not_exists;
        let (ops, index_id) = self.generate_view_ops(
            session,
            plan.name,
            plan.view.clone(),
            plan.replace,
            plan.materialize,
        )?;

        match self
            .catalog_transact(ops, |mut builder| {
                if let Some(index_id) = index_id {
                    if let Some((name, description)) =
                        Self::prepare_index_build(&builder.catalog, &index_id)
                    {
                        let df = builder.build_index_dataflow(name, index_id, description)?;
                        return Ok(Some(df));
                    }
                }
                Ok(None)
            })
            .await
        {
            Ok(df) => {
                if let Some(df) = df {
                    self.ship_dataflow(df).await;
                }
                Ok(ExecuteResponse::CreatedView { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
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
    ) -> Result<ExecuteResponse, CoordError> {
        let mut ops = vec![];
        let mut index_ids = vec![];

        for (name, view) in plan.views {
            let (mut view_ops, index_id) =
                self.generate_view_ops(session, name, view, None, plan.materialize)?;
            ops.append(&mut view_ops);
            if let Some(index_id) = index_id {
                index_ids.push(index_id);
            }
        }

        match self
            .catalog_transact(ops, |mut builder| {
                let mut dfs = vec![];
                for index_id in index_ids {
                    if let Some((name, description)) =
                        Self::prepare_index_build(builder.catalog, &index_id)
                    {
                        let df = builder.build_index_dataflow(name, index_id, description)?;
                        dfs.push(df);
                    }
                }
                Ok(dfs)
            })
            .await
        {
            Ok(dfs) => {
                self.ship_dataflows(dfs).await;
                Ok(ExecuteResponse::CreatedView { existed: false })
            }
            Err(_) if plan.if_not_exists => Ok(ExecuteResponse::CreatedView { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_index(
        &mut self,
        plan: CreateIndexPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let CreateIndexPlan {
            name,
            index,
            options,
            if_not_exists,
        } = plan;

        let id = self.catalog.allocate_id()?;
        let index = catalog::Index {
            create_sql: index.create_sql,
            keys: index.keys,
            on: index.on,
            conn_id: None,
            depends_on: index.depends_on,
            enabled: self.catalog.index_enabled_by_default(&id),
        };
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Index(index),
        };
        match self
            .catalog_transact(vec![op], |mut builder| {
                if let Some((name, description)) = Self::prepare_index_build(builder.catalog, &id) {
                    let df = builder.build_index_dataflow(name, id, description)?;
                    Ok(Some(df))
                } else {
                    Ok(None)
                }
            })
            .await
        {
            Ok(df) => {
                if let Some(df) = df {
                    self.ship_dataflow(df).await;
                    self.set_index_options(id, options).expect("index enabled");
                }
                Ok(ExecuteResponse::CreatedIndex { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedIndex { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_type(
        &mut self,
        plan: CreateTypePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let typ = catalog::Type {
            create_sql: plan.typ.create_sql,
            details: CatalogTypeDetails {
                array_id: None,
                typ: plan.typ.inner,
            },
            depends_on: plan.typ.depends_on,
        };
        let id = self.catalog.allocate_id()?;
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: plan.name,
            item: CatalogItem::Type(typ),
        };
        match self.catalog_transact(vec![op], |_builder| Ok(())).await {
            Ok(()) => Ok(ExecuteResponse::CreatedType),
            Err(err) => Err(err),
        }
    }

    async fn sequence_drop_database(
        &mut self,
        plan: DropDatabasePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_database_ops(plan.name);
        self.catalog_transact(ops, |_builder| Ok(())).await?;
        Ok(ExecuteResponse::DroppedDatabase)
    }

    async fn sequence_drop_schema(
        &mut self,
        plan: DropSchemaPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_schema_ops(plan.name);
        self.catalog_transact(ops, |_builder| Ok(())).await?;
        Ok(ExecuteResponse::DroppedSchema)
    }

    async fn sequence_drop_roles(
        &mut self,
        plan: DropRolesPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = plan
            .names
            .into_iter()
            .map(|name| catalog::Op::DropRole { name })
            .collect();
        self.catalog_transact(ops, |_builder| Ok(())).await?;
        Ok(ExecuteResponse::DroppedRole)
    }

    async fn sequence_drop_items(
        &mut self,
        plan: DropItemsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_items_ops(&plan.items);
        self.catalog_transact(ops, |_builder| Ok(())).await?;
        Ok(match plan.ty {
            ObjectType::Schema => unreachable!(),
            ObjectType::Source => ExecuteResponse::DroppedSource,
            ObjectType::View => ExecuteResponse::DroppedView,
            ObjectType::Table => ExecuteResponse::DroppedTable,
            ObjectType::Sink => ExecuteResponse::DroppedSink,
            ObjectType::Index => ExecuteResponse::DroppedIndex,
            ObjectType::Type => ExecuteResponse::DroppedType,
            ObjectType::Role => unreachable!("DROP ROLE not supported"),
            ObjectType::Object => unreachable!("generic OBJECT cannot be dropped"),
        })
    }

    fn sequence_show_all_variables(
        &mut self,
        session: &Session,
    ) -> Result<ExecuteResponse, CoordError> {
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
    ) -> Result<ExecuteResponse, CoordError> {
        let variable = session.vars().get(&plan.name)?;
        let row = Row::pack_slice(&[Datum::String(&variable.value())]);
        Ok(send_immediate_rows(vec![row]))
    }

    fn sequence_set_variable(
        &self,
        session: &mut Session,
        plan: SetVariablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        session
            .vars_mut()
            .set(&plan.name, &plan.value, plan.local)?;
        Ok(ExecuteResponse::SetVariable { name: plan.name })
    }

    async fn sequence_end_transaction(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        mut action: EndTransactionAction,
    ) {
        if EndTransactionAction::Commit == action {
            let txn = session
                .transaction()
                .inner()
                .expect("must be in a transaction");
            if let Transaction {
                ops: TransactionOps::Writes(_),
                ..
            } = txn
            {
                guard_write_critical_section!(self, tx, session, Plan::CommitTransaction);
            }
        }

        // If the transaction has failed, we can only rollback.
        if let (EndTransactionAction::Commit, TransactionStatus::Failed(_)) =
            (&action, session.transaction())
        {
            action = EndTransactionAction::Rollback;
        }
        let response = ExecuteResponse::TransactionExited {
            tag: action.tag(),
            was_implicit: session.transaction().is_implicit(),
        };

        // Immediately do tasks that must be serialized in the coordinator.
        let rx = self
            .sequence_end_transaction_inner(&mut session, action)
            .await;

        // We can now wait for responses or errors and do any session/transaction
        // finalization in a separate task.
        let conn_id = session.conn_id();
        task::spawn(
            || format!("sequence_end_transaction:{conn_id}"),
            async move {
                let result = match rx {
                    // If we have more work to do, do it
                    Ok(fut) => fut.await,
                    Err(e) => Err(e),
                };

                if result.is_err() {
                    action = EndTransactionAction::Rollback;
                }
                session.vars_mut().end_transaction(action);

                match result {
                    Ok(()) => tx.send(Ok(response), session),
                    Err(err) => tx.send(Err(err), session),
                }
            },
        );
    }

    async fn sequence_end_transaction_inner(
        &mut self,
        session: &mut Session,
        action: EndTransactionAction,
    ) -> Result<impl Future<Output = Result<(), CoordError>>, CoordError> {
        let txn = self.clear_transaction(session).await;

        // Although the compaction frontier may have advanced, we do not need to
        // call `maintenance` here because it will soon be called after the next
        // `update_upper`.

        let mut write_fut = None;
        if let EndTransactionAction::Commit = action {
            if let Some(ops) = txn.into_ops() {
                match ops {
                    TransactionOps::Writes(inserts) => {
                        // Although the transaction has a wall_time in its pcx, we use a new
                        // coordinator timestamp here to provide linearizability. The wall_time does
                        // not have to relate to the write time.
                        let timestamp = self.get_local_write_ts();

                        // Separate out which updates were to tables we are
                        // persisting. In practice, we don't enable/disable this
                        // with table-level granularity so it will be all of
                        // them or none of them, which is checked below.
                        let mut persist_updates = Vec::new();
                        let mut volatile_updates = Vec::new();
                        for WriteOp { id, rows } in inserts {
                            // Re-verify this id exists.
                            let _ = self.catalog.try_get_by_id(id).ok_or_else(|| {
                                CoordError::SqlCatalog(CatalogError::UnknownItem(id.to_string()))
                            })?;
                            // This can be empty if, say, a DELETE's WHERE clause had 0 results.
                            if rows.is_empty() {
                                continue;
                            }
                            if let Some(persist) = self.persister.table_details.get(&id) {
                                let updates = rows
                                    .into_iter()
                                    .map(|(row, diff)| ((row, ()), timestamp, diff));
                                persist_updates.push((&persist.write_handle, updates));
                            } else {
                                let updates = rows
                                    .into_iter()
                                    .map(|(row, diff)| Update {
                                        row,
                                        diff,
                                        timestamp,
                                    })
                                    .collect();
                                volatile_updates.push((id, updates));
                            }
                        }

                        // Write all updates, both persistent and volatile.
                        // Persistence takes care of introducing anything it
                        // writes to the dataflow, so we only need a
                        // Command::Insert for the volatile updates.
                        if !persist_updates.is_empty() {
                            if !volatile_updates.is_empty() {
                                coord_bail!("transaction had mixed persistent and volatile writes");
                            }
                            let persist_multi =
                                self.persister.table_writer.as_mut().ok_or_else(|| {
                                    anyhow!(
                                        "internal error: persist_multi_details invariant violated"
                                    )
                                })?;
                            // NB: Keep this method call outside any
                            // tokio::spawns. We're guaranteed by persist that
                            // writes and seals happen in order, but only if we
                            // synchronously wait for the (fast) registration of
                            // that work to return.
                            write_fut = Some(
                                persist_multi
                                    .write_atomic(|builder| {
                                        for (handle, updates) in persist_updates {
                                            builder.add_write(handle, updates)?;
                                        }
                                        Ok(())
                                    })
                                    .map(|res| match res {
                                        Ok(_) => Ok(()),
                                        Err(err) => {
                                            Err(CoordError::Unstructured(anyhow!("{}", err)))
                                        }
                                    }),
                            );
                        } else {
                            for (id, updates) in volatile_updates {
                                self.dataflow_client
                                    .storage()
                                    .table_insert(id, updates)
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(async move {
            if let Some(fut) = write_fut {
                // Because we return an async block here, this await is not executed until
                // the containing async block is executed, so this await doesn't block the
                // coordinator task.
                fut.await
            } else {
                Ok(())
            }
        })
    }

    /// Return the set of ids in a timedomain and verify timeline correctness.
    ///
    /// When a user starts a transaction, we need to prevent compaction of anything
    /// they might read from. We use a heuristic of "anything in the same database
    /// schemas with the same timeline as whatever the first query is".
    fn timedomain_for(
        &self,
        source_ids: &[GlobalId],
        source_timeline: &Option<Timeline>,
        conn_id: u32,
    ) -> Result<Vec<GlobalId>, CoordError> {
        let mut timedomain_ids = self
            .catalog
            .schema_adjacent_indexed_relations(&source_ids, conn_id);

        // Filter out ids from different timelines. The timeline code only verifies
        // that the SELECT doesn't cross timelines. The schema-adjacent code looks
        // for other ids in the same database schema.
        timedomain_ids.retain(|&id| {
            let id_timeline = self
                .validate_timeline(vec![id])
                .expect("single id should never fail");
            match (&id_timeline, &source_timeline) {
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

        Ok(timedomain_ids)
    }

    /// Sequence a peek, determining a timestamp and the most efficient dataflow interaction.
    ///
    /// Peeks are sequenced by assigning a timestamp for evaluation, and then determining and
    /// deploying the most efficient evaluation plan. The peek could evaluate to a constant,
    /// be a simple read out of an existing arrangement, or required a new dataflow to build
    /// the results to return.
    async fn sequence_peek(
        &mut self,
        session: &mut Session,
        plan: PeekPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let PeekPlan {
            mut source,
            when,
            finishing,
            copy_to,
        } = plan;

        let source_ids = source.global_uses();
        let timeline = self.validate_timeline(source_ids.clone())?;
        let conn_id = session.conn_id();
        let in_transaction = matches!(
            session.transaction(),
            &TransactionStatus::InTransaction(_) | &TransactionStatus::InTransactionImplicit(_)
        );
        // For explicit or implicit transactions that do not use AS OF, get the
        // timestamp of the in-progress transaction or create one. If this is an AS OF
        // query, we don't care about any possible transaction timestamp. If this is a
        // single-statement transaction (TransactionStatus::Started), we don't need to
        // worry about preventing compaction or choosing a valid timestamp for future
        // queries.
        let timestamp = if in_transaction && when == PeekWhen::Immediately {
            // Queries are independent of the logical timestamp iff there are no referenced
            // sources or indexes and there is no reference to `mz_logical_timestamp()`.
            let timestamp_independent = source_ids.is_empty() && !source.contains_temporal();

            // If all previous statements were timestamp-independent and the current one is
            // not, clear the transaction ops so it can get a new timestamp and timedomain.
            if let Some(read_txn) = self.txn_reads.get(&conn_id) {
                if read_txn.timestamp_independent && !timestamp_independent {
                    session.clear_transaction_ops();
                }
            }

            let timestamp = session.get_transaction_timestamp(|session| {
                // Determine a timestamp that will be valid for anything in any schema
                // referenced by the first query.
                let mut timedomain_ids = self.timedomain_for(&source_ids, &timeline, conn_id)?;

                // We want to prevent compaction of the indexes consulted by
                // determine_timestamp, not the ones listed in the query.
                let (timestamp, timestamp_ids) =
                    self.determine_timestamp(session, &timedomain_ids, PeekWhen::Immediately)?;
                // Add the used indexes to the recorded ids.
                timedomain_ids.extend(&timestamp_ids);
                let mut handles = vec![];
                for id in timestamp_ids {
                    handles.push(self.indexes.get(&id).unwrap().since_handle(vec![timestamp]));
                }
                self.txn_reads.insert(
                    conn_id,
                    TxnReads {
                        timestamp_independent,
                        timedomain_ids: timedomain_ids.into_iter().collect(),
                        _handles: handles,
                    },
                );

                Ok(timestamp)
            })?;

            // Verify that the references and indexes for this query are in the current
            // read transaction.
            let mut stmt_ids = HashSet::new();
            stmt_ids.extend(source_ids.iter().collect::<HashSet<_>>());
            // Using nearest_indexes here is a hack until #8318 is fixed. It's used because
            // that's what determine_timestamp uses.
            stmt_ids.extend(
                self.catalog
                    .nearest_indexes(&source_ids)
                    .0
                    .into_iter()
                    .collect::<HashSet<_>>(),
            );
            let read_txn = self.txn_reads.get(&conn_id).unwrap();
            // Find the first reference or index (if any) that is not in the transaction. A
            // reference could be caused by a user specifying an object in a different
            // schema than the first query. An index could be caused by a CREATE INDEX
            // after the transaction started.
            let outside: Vec<_> = stmt_ids.difference(&read_txn.timedomain_ids).collect();
            if !outside.is_empty() {
                let mut names: Vec<_> = read_txn
                    .timedomain_ids
                    .iter()
                    // This could filter out a view that has been replaced in another transaction.
                    .filter_map(|id| self.catalog.try_get_by_id(*id))
                    .map(|item| item.name().to_string())
                    .collect();
                let mut outside: Vec<_> = outside
                    .into_iter()
                    .filter_map(|id| self.catalog.try_get_by_id(*id))
                    .map(|item| item.name().to_string())
                    .collect();
                // Sort so error messages are deterministic.
                names.sort();
                outside.sort();
                return Err(CoordError::RelationOutsideTimeDomain {
                    relations: outside,
                    names,
                });
            }

            timestamp
        } else {
            self.determine_timestamp(session, &source_ids, when)?.0
        };

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
        let mut dataflow = DataflowDesc::new(format!("temp-view-{}", view_id), view_id);
        dataflow.set_as_of(Antichain::from_elem(timestamp));
        let mut builder = self.dataflow_builder();
        builder.import_view_into_dataflow(&view_id, &source, &mut dataflow)?;
        for BuildDesc { view, .. } in &mut dataflow.objects_to_build {
            builder.prep_relation_expr(
                view,
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
        mz_transform::optimize_dataflow(&mut dataflow, self.catalog.enabled_indexes())?;

        // Finalization optimizes the dataflow as much as possible.
        let dataflow_plan = self.finalize_dataflow(dataflow);

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

        // Implement the peek, and capture the response.
        let resp = self
            .implement_fast_path_peek(fast_path, timestamp, finishing, conn_id, source.arity())
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
    ) -> Result<ExecuteResponse, CoordError> {
        let TailPlan {
            from,
            with_snapshot,
            ts,
            copy_to,
            emit_progress,
        } = plan;
        // TAIL AS OF, similar to peeks, doesn't need to worry about transaction
        // timestamp semantics.
        if ts.is_none() {
            // If this isn't a TAIL AS OF, the TAIL can be in a transaction if it's the
            // only operation.
            session.add_transaction_ops(TransactionOps::Tail)?;
        }

        let make_sink_desc = |coord: &mut Coordinator, from, from_desc, uses| {
            // Determine the frontier of updates to tail *from*.
            // Updates greater or equal to this frontier will be produced.
            let frontier = if let Some(ts) = ts {
                // If a timestamp was explicitly requested, use that.
                let ts = coord.determine_timestamp(session, uses, PeekWhen::AtTimestamp(ts))?;
                Antichain::from_elem(ts.0)
            } else {
                coord.determine_frontier(uses)
            };

            Ok::<_, CoordError>(SinkDesc {
                from,
                from_desc,
                connector: SinkConnector::Tail(TailSinkConnector::default()),
                envelope: None,
                as_of: SinkAsOf {
                    frontier,
                    strict: !with_snapshot,
                },
            })
        };

        let dataflow = match from {
            TailFrom::Id(from_id) => {
                let from = self.catalog.get_by_id(&from_id);
                let from_desc = from.desc().unwrap().clone();
                let sink_id = self.catalog.allocate_id()?;
                let sink_desc = make_sink_desc(self, from_id, from_desc, &[from_id])?;
                let sink_name = format!("tail-{}", sink_id);
                self.dataflow_builder()
                    .build_sink_dataflow(sink_name, sink_id, sink_desc)?
            }
            TailFrom::Query {
                expr,
                desc,
                depends_on,
            } => {
                let id = self.allocate_transient_id()?;
                let expr = self.view_optimizer.optimize(expr)?;
                let desc = RelationDesc::new(expr.typ(), desc.iter_names());
                let sink_desc = make_sink_desc(self, id, desc, &depends_on)?;
                let mut dataflow = DataflowDesc::new(format!("tail-{}", id), id);
                let mut dataflow_builder = self.dataflow_builder();
                dataflow_builder.import_view_into_dataflow(&id, &expr, &mut dataflow)?;
                dataflow_builder.build_sink_dataflow_into(&mut dataflow, id, sink_desc)?;
                dataflow
            }
        };

        let (sink_id, sink_desc) = &dataflow.sink_exports[0];
        session.add_drop_sink(*sink_id);
        let arity = sink_desc.from_desc.arity();
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending_tails
            .insert(*sink_id, PendingTail::new(tx, emit_progress, arity));
        self.ship_dataflow(dataflow).await;

        let resp = ExecuteResponse::Tailing { rx };
        match copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    /// A policy for determining the timestamp for a peek.
    ///
    /// The Timestamp result may be `None` in the case that the `when` policy
    /// cannot be satisfied, which is possible due to the restricted validity of
    /// traces (each has a `since` and `upper` frontier, and are only valid after
    /// `since` and sure to be available not after `upper`). The set of indexes
    /// used is also returned.
    fn determine_timestamp(
        &mut self,
        session: &Session,
        uses_ids: &[GlobalId],
        when: PeekWhen,
    ) -> Result<(Timestamp, Vec<GlobalId>), CoordError> {
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
        let (index_ids, unmaterialized_source_ids) = self.catalog.nearest_indexes(uses_ids);

        // Determine the valid lower bound of times that can produce correct outputs.
        // This bound is determined by the arrangements contributing to the query,
        // and does not depend on the transitive sources.
        let mut since = self.indexes.least_valid_since(index_ids.iter().cloned());
        since.join_assign(
            &self
                .sources
                .least_valid_since(unmaterialized_source_ids.iter().cloned()),
        );

        // First determine the candidate timestamp, which is either the explicitly requested
        // timestamp, or the latest timestamp known to be immediately available.
        let timestamp = match when {
            // Explicitly requested timestamps should be respected.
            PeekWhen::AtTimestamp(mut timestamp) => {
                let temp_storage = RowArena::new();
                self.dataflow_builder().prep_scalar_expr(
                    &mut timestamp,
                    ExprPrepStyle::OneShot {
                        logical_time: None,
                        session,
                    },
                )?;
                let evaled = timestamp.eval(&[], &temp_storage)?;
                let ty = timestamp.typ(&RelationType::empty());
                match ty.scalar_type {
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
                    ScalarType::Timestamp => {
                        evaled.unwrap_timestamp().timestamp_millis().try_into()?
                    }
                    _ => coord_bail!(
                        "can't use {} as a timestamp for AS OF",
                        self.catalog.for_session(session).humanize_column_type(&ty)
                    ),
                }
            }

            // These two strategies vary in terms of which traces drive the
            // timestamp determination process: either the trace itself or the
            // original sources on which they depend.
            PeekWhen::Immediately => {
                if !unmaterialized_source_ids.is_empty() {
                    let mut unmaterialized = vec![];
                    let mut disabled_indexes = vec![];
                    for id in unmaterialized_source_ids {
                        // Determine which sources are unmaterialized and which have disabled indexes
                        let name = self.catalog.get_by_id(&id).name().to_string();
                        let indexes = self.catalog.get_indexes_on(id);
                        if indexes.is_empty() {
                            unmaterialized.push(name);
                        } else {
                            let disabled_index_names = indexes
                                .iter()
                                .filter_map(|id| {
                                    if !self.catalog.is_index_enabled(id) {
                                        Some(self.catalog.get_by_id(&id).name().to_string())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            disabled_indexes.push((name, disabled_index_names));
                        }
                    }
                    return Err(CoordError::AutomaticTimestampFailure {
                        unmaterialized,
                        disabled_indexes,
                    });
                }

                let mut candidate = if uses_ids.iter().any(|id| self.catalog.uses_tables(*id)) {
                    // If the view depends on any tables, we enforce
                    // linearizability by choosing the latest input time.
                    self.get_local_read_ts()
                } else {
                    let upper = self.indexes.greatest_open_upper(index_ids.iter().copied());
                    // We peek at the largest element not in advance of `upper`, which
                    // involves a subtraction. If `upper` contains a zero timestamp there
                    // is no "prior" answer, and we do not want to peek at it as it risks
                    // hanging awaiting the response to data that may never arrive.
                    //
                    // The .get(0) here breaks the antichain abstraction by assuming this antichain
                    // has 0 or 1 elements in it. It happens to work because we use a timestamp
                    // type that meets that assumption, but would break if we used a more general
                    // timestamp.
                    if let Some(candidate) = upper.elements().get(0) {
                        if *candidate > 0 {
                            candidate.saturating_sub(1)
                        } else {
                            let unstarted = index_ids
                                .into_iter()
                                .filter(|id| {
                                    self.indexes
                                        .upper_of(id)
                                        .expect("id not found")
                                        .less_equal(&0)
                                })
                                .collect::<Vec<_>>();
                            return Err(CoordError::IncompleteTimestamp(unstarted));
                        }
                    } else {
                        // A complete trace can be read in its final form with this time.
                        //
                        // This should only happen for literals that have no sources
                        Timestamp::max_value()
                    }
                };
                // If the candidate is not beyond the valid `since` frontier,
                // force it to become so as best as we can. If `since` is empty
                // this will be a no-op, as there is no valid time, but that should
                // then be caught below.
                if !since.less_equal(&candidate) {
                    candidate.advance_by(since.borrow());
                }
                candidate
            }
        };

        // If the timestamp is greater or equal to some element in `since` we are
        // assured that the answer will be correct.
        if since.less_equal(&timestamp) {
            Ok((timestamp, index_ids))
        } else {
            let invalid_indexes = index_ids.iter().filter_map(|id| {
                let since = self.indexes.since_of(id).expect("id not found");
                if since.less_equal(&timestamp) {
                    None
                } else {
                    Some(since)
                }
            });
            let invalid_sources = unmaterialized_source_ids.iter().filter_map(|id| {
                let since = self.sources.since_of(id).expect("id not found");
                if since.less_equal(&timestamp) {
                    None
                } else {
                    Some(since)
                }
            });
            let invalid = invalid_indexes.chain(invalid_sources).collect::<Vec<_>>();
            coord_bail!(
                "Timestamp ({}) is not valid for all inputs: {:?}",
                timestamp,
                invalid
            );
        }
    }

    /// Determine the frontier of updates to start *from* for a sink based on
    /// `source_id`.
    ///
    /// Updates greater or equal to this frontier will be produced.
    fn determine_frontier(&mut self, source_ids: &[GlobalId]) -> Antichain<Timestamp> {
        // This function differs from determine_timestamp because sinks/tail don't care
        // about indexes existing or timestamps being complete. If data don't exist
        // yet (upper = 0), it is not a problem for the sink to wait for it. If the
        // timestamp we choose isn't as fresh as possible, that's also fine because we
        // produce timestamps describing when the diff occurred, so users can determine
        // if that's fresh enough.

        // If source_id is already indexed, then nearest_indexes will return the
        // same index that default_index_for does, so we can stick with only using
        // nearest_indexes. We don't care about the indexes being incomplete because
        // callers of this function (CREATE SINK and TAIL) are responsible for creating
        // indexes if needed.
        let (index_ids, unmaterialized_source_ids) = self.catalog.nearest_indexes(source_ids);
        let mut since = self.indexes.least_valid_since(index_ids.iter().copied());
        since.join_assign(
            &self
                .sources
                .least_valid_since(unmaterialized_source_ids.iter().copied()),
        );

        let mut candidate = if index_ids.iter().any(|id| self.catalog.uses_tables(*id)) {
            // If the sink depends on any tables, we enforce linearizability by choosing
            // the latest input time.
            self.get_local_read_ts()
        } else if unmaterialized_source_ids.is_empty() && !index_ids.is_empty() {
            // If the sink does not need to create any indexes and requires at least 1
            // index, use the upper. For something like a static view, the indexes are
            // complete but the index count is 0, and we want 0 instead of max for the
            // time, so we should fall through to the else in that case.
            let upper = self.indexes.greatest_open_upper(index_ids);
            if let Some(ts) = upper.elements().get(0) {
                // We don't need to worry about `ts == 0` like determine_timestamp, because
                // it's fine to not have any timestamps completed yet, which will just cause
                // this sink to wait.
                ts.saturating_sub(1)
            } else {
                Timestamp::max_value()
            }
        } else {
            // If the sink does need to create an index, use 0, which will cause the since
            // to be used below.
            Timestamp::min_value()
        };

        // Ensure that the timestamp is >= since. This is necessary because when a
        // Frontiers is created, its upper = 0, but the since is > 0 until update_upper
        // has run.
        if !since.less_equal(&candidate) {
            candidate.advance_by(since.borrow());
        }
        Antichain::from_elem(candidate)
    }

    fn sequence_explain(
        &mut self,
        session: &Session,
        plan: ExplainPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ExplainPlan {
            raw_plan,
            row_set_finishing,
            stage,
            options,
        } = plan;
        use std::time::Instant;

        struct Timings {
            decorrelation: Option<Duration>,
            optimization: Option<Duration>,
        }

        let mut timings = Timings {
            decorrelation: None,
            optimization: None,
        };

        let decorrelate = |timings: &mut Timings, raw_plan: HirRelationExpr| -> MirRelationExpr {
            let start = Instant::now();
            let decorrelated_plan = raw_plan.optimize_and_lower(&OptimizerConfig {
                qgm_optimizations: session.vars().qgm_optimizations(),
            });
            timings.decorrelation = Some(start.elapsed());
            decorrelated_plan
        };

        let optimize =
            |timings: &mut Timings,
             coord: &mut Self,
             decorrelated_plan: MirRelationExpr|
             -> Result<DataflowDescription<OptimizedMirRelationExpr>, CoordError> {
                let start = Instant::now();
                let optimized_plan = coord.view_optimizer.optimize(decorrelated_plan)?;
                let mut dataflow = DataflowDesc::new(format!("explanation"), GlobalId::Explain);
                coord.dataflow_builder().import_view_into_dataflow(
                    // TODO: If explaining a view, pipe the actual id of the view.
                    &GlobalId::Explain,
                    &optimized_plan,
                    &mut dataflow,
                )?;
                mz_transform::optimize_dataflow(&mut dataflow, coord.catalog.enabled_indexes())?;
                timings.optimization = Some(start.elapsed());
                Ok(dataflow)
            };

        let mut explanation_string = match stage {
            ExplainStage::RawPlan => {
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
            ExplainStage::QueryGraph => {
                // TODO add type information to the output graph
                type Model = Result<mz_sql::query_model::Model, mz_sql::query_model::QGMError>;
                let model = Model::from(raw_plan)?;
                model.as_dot("")?
            }
            ExplainStage::OptimizedQueryGraph => {
                // TODO add type information to the output graph
                type Model = Result<mz_sql::query_model::Model, mz_sql::query_model::QGMError>;
                let mut model = Model::from(raw_plan)?;
                model.optimize();
                model.as_dot("")?
            }
            ExplainStage::DecorrelatedPlan => {
                let decorrelated_plan = OptimizedMirRelationExpr::declare_optimized(decorrelate(
                    &mut timings,
                    raw_plan,
                ));
                let catalog = self.catalog.for_session(session);
                let formatter =
                    mz_dataflow_types::DataflowGraphFormatter::new(&catalog, options.typed);
                let mut explanation =
                    mz_dataflow_types::Explanation::new(&decorrelated_plan, &catalog, &formatter);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                explanation.to_string()
            }
            ExplainStage::OptimizedPlan => {
                let decorrelated_plan = decorrelate(&mut timings, raw_plan);
                self.validate_timeline(decorrelated_plan.global_uses())?;
                let dataflow = optimize(&mut timings, self, decorrelated_plan)?;
                let catalog = self.catalog.for_session(session);
                let formatter =
                    mz_dataflow_types::DataflowGraphFormatter::new(&catalog, options.typed);
                let mut explanation = mz_dataflow_types::Explanation::new_from_dataflow(
                    &dataflow, &catalog, &formatter,
                );
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                explanation.to_string()
            }
            ExplainStage::PhysicalPlan => {
                let decorrelated_plan = decorrelate(&mut timings, raw_plan);
                self.validate_timeline(decorrelated_plan.global_uses())?;
                let dataflow = optimize(&mut timings, self, decorrelated_plan)?;
                let dataflow_plan =
                    mz_dataflow_types::Plan::<mz_repr::Timestamp>::finalize_dataflow(dataflow)
                        .expect("Dataflow planning failed; unrecoverable error");
                let catalog = self.catalog.for_session(session);
                let mut explanation = mz_dataflow_types::Explanation::new_from_dataflow(
                    &dataflow_plan,
                    &catalog,
                    &mz_dataflow_types::JsonViewFormatter {},
                );
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                explanation.to_string()
            }
        };
        if options.timing {
            if let Some(decorrelation) = &timings.decorrelation {
                explanation_string.push_str(&format!(
                    "\nDecorrelation time: {}",
                    Interval {
                        months: 0,
                        days: 0,
                        micros: decorrelation.as_micros().try_into().unwrap(),
                    }
                ));
            }
            if let Some(optimization) = &timings.optimization {
                explanation_string.push_str(&format!(
                    "\nOptimization time: {}",
                    Interval {
                        months: 0,
                        days: 0,
                        micros: optimization.as_micros().try_into().unwrap(),
                    }
                ));
            }
            if timings.decorrelation.is_some() || timings.optimization.is_some() {
                explanation_string.push_str("\n");
            }
        }
        let rows = vec![Row::pack_slice(&[Datum::from(&*explanation_string)])];
        Ok(send_immediate_rows(rows))
    }

    fn sequence_send_diffs(
        &mut self,
        session: &mut Session,
        mut plan: SendDiffsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        if self.catalog.config().disable_user_indexes {
            self.catalog.ensure_default_index_enabled(plan.id)?;
        }

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

        session.add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
            id: plan.id,
            rows: plan.updates,
        }]))?;
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
            constants @ MirRelationExpr::Constant { .. } => tx.send(
                self.sequence_insert_constant(&mut session, plan.id, constants),
                session,
            ),
            // All non-constant values must be planned as read-then-writes.
            mut selection => {
                let desc_arity = match self.catalog.try_get_by_id(plan.id) {
                    Some(table) => table.desc().expect("desc called on table").arity(),
                    None => {
                        tx.send(
                            Err(CoordError::SqlCatalog(CatalogError::UnknownItem(
                                plan.id.to_string(),
                            ))),
                            session,
                        );
                        return;
                    }
                };

                if selection.contains_temporal() {
                    tx.send(
                        Err(CoordError::Unsupported(
                            "calls to mz_logical_timestamp in write statements are not supported",
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
    ) -> Result<ExecuteResponse, CoordError> {
        // Insert can be queued, so we need to re-verify the id exists.
        let desc = match self.catalog.try_get_by_id(id) {
            Some(table) => table.desc()?,
            None => {
                return Err(CoordError::SqlCatalog(CatalogError::UnknownItem(
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
    ) -> Result<ExecuteResponse, CoordError> {
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
        } = plan;

        // Read then writes can be queued, so re-verify the id exists.
        let desc = match self.catalog.try_get_by_id(id) {
            Some(table) => table.desc().expect("desc called on table").clone(),
            None => {
                tx.send(
                    Err(CoordError::SqlCatalog(CatalogError::UnknownItem(
                        id.to_string(),
                    ))),
                    session,
                );
                return;
            }
        };

        // Ensure selection targets are valid, i.e. user-defined tables, or
        // objects local to the dataflow.
        for id in selection.global_uses() {
            let valid = match self.catalog.try_get_by_id(id) {
                // TODO: Widen this check when supporting temporary tables.
                Some(entry) if id.is_user() => entry.is_table(),
                _ => false,
            };
            if !valid {
                tx.send(Err(CoordError::InvalidTableMutationSelection), session);
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
                    when: PeekWhen::AtTimestamp(ts),
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

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        task::spawn(|| format!("sequence_read_then_write:{id}"), async move {
            let arena = RowArena::new();
            let diffs = match peek_response {
                ExecuteResponse::SendingRows(batch) => match batch.await {
                    PeekResponseUnary::Rows(rows) => {
                        |rows: Vec<Row>| -> Result<Vec<(Row, Diff)>, CoordError> {
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
                                                return Err(CoordError::Unstructured(anyhow!(e)))
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
                        Err(CoordError::Unstructured(anyhow!("execution canceled")))
                    }
                    PeekResponseUnary::Error(e) => Err(CoordError::Unstructured(anyhow!(e))),
                },
                _ => Err(CoordError::Unstructured(anyhow!("expected SendingRows"))),
            };
            internal_cmd_tx
                .send(Message::SendDiffs(SendDiffs {
                    session,
                    tx,
                    id,
                    diffs,
                    kind,
                }))
                .expect("sending to internal_cmd_tx cannot fail");
        });
    }

    async fn sequence_alter_item_rename(
        &mut self,
        plan: AlterItemRenamePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let op = catalog::Op::RenameItem {
            id: plan.id,
            to_name: plan.to_name,
        };
        match self.catalog_transact(vec![op], |_builder| Ok(())).await {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(plan.object_type)),
            Err(err) => Err(err),
        }
    }

    fn sequence_alter_index_set_options(
        &mut self,
        plan: AlterIndexSetOptionsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        self.set_index_options(plan.id, plan.options)?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    fn sequence_alter_index_reset_options(
        &mut self,
        plan: AlterIndexResetOptionsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let options = plan
            .options
            .into_iter()
            .map(|o| match o {
                IndexOptionName::LogicalCompactionWindow => IndexOption::LogicalCompactionWindow(
                    self.logical_compaction_window_ms.map(Duration::from_millis),
                ),
            })
            .collect();
        self.set_index_options(plan.id, options)?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    async fn sequence_alter_index_enable(
        &mut self,
        plan: AlterIndexEnablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.enable_index_ops(plan.id)?;

        // If ops is not empty, index was disabled.
        if !ops.is_empty() {
            let df = self
                .catalog_transact(ops, |mut builder| {
                    let (name, description) = Self::prepare_index_build(builder.catalog, &plan.id)
                        .expect("index enabled");
                    let df = builder.build_index_dataflow(name, plan.id, description)?;
                    Ok(df)
                })
                .await?;
            self.ship_dataflow(df).await;
        }

        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    /// Perform a catalog transaction. The closure is passed a [`DataflowBuilder`]
    /// made from the prospective [`CatalogState`] (i.e., the `Catalog` with `ops`
    /// applied but before the transaction is committed). The closure can return
    /// an error to abort the transaction, or otherwise return a value that is
    /// returned by this function. This allows callers to error while building
    /// [`DataflowDesc`]s. [`Coordinator::ship_dataflow`] must be called after this
    /// function successfully returns on any built `DataflowDesc`.
    ///
    /// [`CatalogState`]: crate::catalog::CatalogState
    async fn catalog_transact<F, R>(&mut self, ops: Vec<catalog::Op>, f: F) -> Result<R, CoordError>
    where
        F: FnOnce(DataflowBuilder) -> Result<R, CoordError>,
    {
        let mut sources_to_drop = vec![];
        let mut tables_to_drop = vec![];
        let mut sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut replication_slots_to_drop: HashMap<String, Vec<String>> = HashMap::new();

        for op in &ops {
            if let catalog::Op::DropItem(id) = op {
                match self.catalog.get_by_id(id).item() {
                    CatalogItem::Table(_) => {
                        tables_to_drop.push(*id);
                    }
                    CatalogItem::Source(source) => {
                        sources_to_drop.push(*id);
                        if let SourceConnector::External {
                            connector:
                                ExternalSourceConnector::Postgres(PostgresSourceConnector {
                                    conn,
                                    slot_name,
                                    ..
                                }),
                            ..
                        } = &source.connector
                        {
                            replication_slots_to_drop
                                .entry(conn.clone())
                                .or_insert_with(Vec::new)
                                .push(slot_name.clone());
                        }
                    }
                    CatalogItem::Sink(catalog::Sink {
                        connector: SinkConnectorState::Ready(_),
                        ..
                    }) => {
                        sinks_to_drop.push(*id);
                    }
                    CatalogItem::Index(_) => {
                        indexes_to_drop.push(*id);
                    }
                    _ => (),
                }
            }
        }

        let indexes = &self.indexes;
        let persister = &self.persister;
        let storage = &self.dataflow_client;

        let (builtin_table_updates, result) = self.catalog.transact(ops, |catalog| {
            let builder = DataflowBuilder {
                catalog,
                indexes,
                persister,
                storage,
            };
            f(builder)
        })?;

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        let _: () = async {
            self.send_builtin_table_updates(builtin_table_updates).await;

            if !sources_to_drop.is_empty() {
                for id in &sources_to_drop {
                    self.sources.remove(id);
                    self.since_handles.remove(id);
                }
                self.dataflow_client
                    .storage()
                    .drop_sources(sources_to_drop)
                    .await
                    .unwrap();
            }
            if !tables_to_drop.is_empty() {
                // NOTE: When creating a persistent table we insert its compaction frontier (aka since)
                // in `self.sources` to make sure that it is taken into account when rendering
                // dataflows that use it. We must make sure to remove that here.
                for id in &tables_to_drop {
                    self.sources.remove(id);
                    self.persister.remove_table(*id);
                    self.since_handles.remove(id);
                }
                self.dataflow_client
                    .storage()
                    .drop_sources(tables_to_drop)
                    .await
                    .unwrap();
            }
            if !sinks_to_drop.is_empty() {
                for id in sinks_to_drop.iter() {
                    self.sink_writes.remove(id);
                }
                self.dataflow_client
                    .compute(DEFAULT_COMPUTE_INSTANCE_ID)
                    .unwrap()
                    .drop_sinks(sinks_to_drop)
                    .await
                    .unwrap();
            }
            if !indexes_to_drop.is_empty() {
                self.drop_indexes(indexes_to_drop).await;
            }

            // We don't want to block the coordinator on an external postgres server, so
            // move the drop slots to a separate task. This does mean that a failed drop
            // slot won't bubble up to the user as an error message. However, even if it
            // did (and how the code previously worked), mz has already dropped it from our
            // catalog, and so we wouldn't be able to retry anyway.
            if !replication_slots_to_drop.is_empty() {
                // TODO(guswynn): see if there is more relevant info to add to this name
                task::spawn(|| "drop_replication_slots", async move {
                    for (conn, slot_names) in replication_slots_to_drop {
                        // Try to drop the replication slots, but give up after a while.
                        let _ = Retry::default()
                            .retry_async(|_state| {
                                mz_postgres_util::drop_replication_slots(&conn, &slot_names)
                            })
                            .await;
                    }
                });
            }
        }
        .await;

        Ok(result)
    }

    async fn send_builtin_table_updates_at_offset(&mut self, updates: Vec<TimestampedUpdate>) {
        // NB: This makes sure to send all records for the same id in the same
        // message so we can persist a record and its future retraction
        // atomically. Otherwise, we may end up with permanent orphans if a
        // restart/crash happens at the wrong time.
        let timestamp_base = self.get_local_write_ts();
        let mut updates_by_id = HashMap::<GlobalId, Vec<Update>>::new();
        for tu in updates.into_iter() {
            let timestamp = timestamp_base + tu.timestamp_offset;
            for u in tu.updates {
                updates_by_id.entry(u.id).or_default().push(Update {
                    row: u.row,
                    diff: u.diff,
                    timestamp,
                });
            }
        }
        for (id, updates) in updates_by_id {
            // TODO: It'd be nice to unify this with the similar logic in
            // sequence_end_transaction, but it's not initially clear how to do
            // that.
            let persist = self.persister.table_details.get(&id);
            if let Some(persist) = persist {
                let updates: Vec<((Row, ()), Timestamp, Diff)> = updates
                    .into_iter()
                    .map(|u| ((u.row, ()), u.timestamp, u.diff))
                    .collect();
                // Persistence of system table inserts is best effort, so throw
                // away the response and ignore any errors. We do, however,
                // respect the note below so we don't end up with unexpected
                // write and seal reorderings.
                //
                // NB: Keep this method call outside the tokio::spawn. We're
                // guaranteed by persist that writes and seals happen in order,
                // but only if we synchronously wait for the (fast) registration
                // of that work to return.
                let write_fut = persist.write_handle.write(&updates);
                let _ = task::spawn(|| "builtin_table_updates_write_fut:{id}", write_fut);
            } else {
                self.dataflow_client
                    .storage()
                    .table_insert(id, updates)
                    .await
                    .unwrap();
            }
        }
    }

    async fn send_builtin_table_updates(&mut self, updates: Vec<BuiltinTableUpdate>) {
        let timestamped = TimestampedUpdate {
            updates,
            timestamp_offset: 0,
        };
        self.send_builtin_table_updates_at_offset(vec![timestamped])
            .await
    }

    async fn drop_sinks(&mut self, dataflow_names: Vec<GlobalId>) {
        if !dataflow_names.is_empty() {
            self.dataflow_client
                .compute(DEFAULT_COMPUTE_INSTANCE_ID)
                .unwrap()
                .drop_sinks(dataflow_names)
                .await
                .unwrap();
        }
    }

    async fn drop_indexes(&mut self, indexes: Vec<GlobalId>) {
        let mut trace_keys = Vec::new();
        for id in indexes {
            if self.indexes.remove(&id).is_some() {
                trace_keys.push(id);
            }
            self.since_handles.remove(&id);
        }
        if !trace_keys.is_empty() {
            self.dataflow_client
                .compute(DEFAULT_COMPUTE_INSTANCE_ID)
                .unwrap()
                .drop_indexes(trace_keys)
                .await
                .unwrap();
        }
    }

    fn set_index_options(
        &mut self,
        id: GlobalId,
        options: Vec<IndexOption>,
    ) -> Result<(), CoordError> {
        let index = match self.indexes.get_mut(&id) {
            Some(index) => index,
            None => {
                if !self.catalog.is_index_enabled(&id) {
                    return Err(CoordError::InvalidAlterOnDisabledIndex(
                        self.catalog.get_by_id(&id).name().to_string(),
                    ));
                } else {
                    panic!("coord indexes out of sync")
                }
            }
        };

        for o in options {
            match o {
                IndexOption::LogicalCompactionWindow(window) => {
                    let window = window.map(duration_to_timestamp_millis);
                    index.set_compaction_window_ms(window);
                }
            }
        }
        Ok(())
    }

    /// Finalizes a dataflow and then broadcasts it to all workers.
    /// Utility method for the more general [Self::ship_dataflows]
    async fn ship_dataflow(&mut self, dataflow: DataflowDesc) {
        self.ship_dataflows(vec![dataflow]).await
    }

    /// Finalizes a list of dataflows and then broadcasts it to all workers.
    async fn ship_dataflows(&mut self, dataflows: Vec<DataflowDesc>) {
        let mut dataflow_plans = Vec::with_capacity(dataflows.len());
        for dataflow in dataflows.into_iter() {
            dataflow_plans.push(self.finalize_dataflow(dataflow));
        }
        self.dataflow_client
            .compute(DEFAULT_COMPUTE_INSTANCE_ID)
            .unwrap()
            .create_dataflows(dataflow_plans)
            .await
            .unwrap();
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
        &mut self,
        mut dataflow: DataflowDesc,
    ) -> mz_dataflow_types::DataflowDescription<mz_dataflow_types::Plan> {
        // This function must succeed because catalog_transact has generally been run
        // before calling this function. We don't have plumbing yet to rollback catalog
        // operations if this function fails, and materialized will be in an unsafe
        // state if we do not correctly clean up the catalog.

        // The identity for `join` is the minimum element.
        let mut since = Antichain::from_elem(Timestamp::minimum());

        // Populate "valid from" information for each source.
        for (source_id, _description) in dataflow.source_imports.iter() {
            // Extract `since` information about each source and apply here.
            if let Some(source_since) = self.sources.since_of(source_id) {
                since.join_assign(&source_since);
            }
        }

        // For each imported arrangement, lower bound `since` by its own frontier.
        for (global_id, (_description, _typ)) in dataflow.index_imports.iter() {
            since.join_assign(
                &self
                    .indexes
                    .since_of(global_id)
                    .expect("global id missing at coordinator"),
            );
        }

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

        // Capture `as_of` to initialize the `since` frontiers of indexes.
        let as_of = dataflow.as_of.clone().unwrap();

        // For each produced arrangement, start tracking the arrangement with
        // a compaction frontier of at least `since`.
        for (global_id, _description, _typ) in dataflow.index_exports.iter() {
            let frontiers = self.new_index_frontiers(
                *global_id,
                as_of.elements().to_vec(),
                self.logical_compaction_window_ms,
            );
            self.indexes.insert(*global_id, frontiers);
        }

        // TODO: Produce "valid from" information for each sink.
        // For each sink, ... do nothing because we don't yield `since` for sinks.
        // for (global_id, _description) in dataflow.sink_exports.iter() {
        //     unimplemented!()
        // }

        mz_dataflow_types::Plan::finalize_dataflow(dataflow)
            .expect("Dataflow planning failed; unrecoverable error")
    }

    fn allocate_transient_id(&mut self) -> Result<GlobalId, CoordError> {
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
    fn validate_timeline(&self, mut ids: Vec<GlobalId>) -> Result<Option<Timeline>, CoordError> {
        let mut timelines: HashMap<GlobalId, Timeline> = HashMap::new();

        // Recurse through IDs to find all sources and tables, adding new ones to
        // the set until we reach the bottom. Static views will end up with an empty
        // timelines.
        while let Some(id) = ids.pop() {
            // Protect against possible infinite recursion. Not sure if it's possible, but
            // a cheap prevention for the future.
            if timelines.contains_key(&id) {
                continue;
            }
            let entry = self.catalog.get_by_id(&id);
            match entry.item() {
                CatalogItem::Source(source) => {
                    timelines.insert(id, source.connector.timeline());
                }
                CatalogItem::Index(index) => {
                    ids.push(index.on);
                }
                CatalogItem::View(view) => {
                    ids.extend(view.optimized_expr.global_uses());
                }
                CatalogItem::Table(table) => {
                    timelines.insert(id, table.timeline());
                }
                _ => {}
            }
        }

        let timelines: HashSet<Timeline> = timelines
            .into_iter()
            .map(|(_, timeline)| timeline)
            .collect();

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
            return Err(CoordError::Unsupported(
                "multiple timelines within one dataflow",
            ));
        }
        Ok(timelines.into_iter().next())
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

    /// Defers executing `plan` until the write lock becomes available; waiting
    /// occurs in a greenthread, so callers of this function likely want to
    /// return after calling it.
    fn defer_write(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        session: Session,
        plan: Plan,
    ) {
        let conn_id = session.conn_id();
        let plan = DeferredPlan { tx, session, plan };
        self.write_lock_wait_group.push_back(plan);

        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let write_lock = Arc::clone(&self.write_lock);
        // TODO(guswynn): see if there is more relevant info to add to this name
        task::spawn(|| format!("defer_write:{conn_id}"), async move {
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
pub async fn serve(
    Config {
        dataflow_client,
        logging,
        storage,
        timestamp_frequency,
        logical_compaction_window,
        experimental_mode,
        disable_user_indexes,
        safe_mode,
        build_info,
        aws_external_id,
        metrics_registry,
        persister,
        now,
    }: Config,
) -> Result<(Handle, Client), CoordError> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();

    let (catalog, builtin_table_updates) = Catalog::open(catalog::Config {
        storage,
        experimental_mode: Some(experimental_mode),
        safe_mode,
        enable_logging: logging.is_some(),
        build_info,
        aws_external_id,
        timestamp_frequency,
        now: now.clone(),
        skip_migrations: false,
        metrics_registry: &metrics_registry,
        disable_user_indexes,
        persister: &persister,
    })
    .await?;
    let cluster_id = catalog.config().cluster_id;
    let session_id = catalog.config().session_id;
    let start_instant = catalog.config().start_instant;

    let metric_scraper = Scraper::new(logging.as_ref(), metrics_registry.clone())?;

    // In order for the coordinator to support Rc and Refcell types, it cannot be
    // sent across threads. Spawn it in a thread and have this parent thread wait
    // for bootstrap completion before proceeding.
    let (bootstrap_tx, bootstrap_rx) = oneshot::channel();
    let handle = TokioHandle::current();
    let thread = thread::Builder::new()
        .name("coordinator".to_string())
        .spawn(move || {
            let mut coord = Coordinator {
                dataflow_client: mz_dataflow_types::client::Controller::new(dataflow_client),
                view_optimizer: Optimizer::logical_optimizer(),
                catalog,
                persister,
                indexes: ArrangementFrontiers::default(),
                sources: ArrangementFrontiers::default(),
                logical_compaction_window_ms: logical_compaction_window
                    .map(duration_to_timestamp_millis),
                logging_enabled: logging.is_some(),
                internal_cmd_tx,
                metric_scraper,
                last_open_local_ts: 1,
                writes_at_open_ts: false,
                read_writes_at_open_ts: false,
                transient_id_counter: 1,
                active_conns: HashMap::new(),
                txn_reads: HashMap::new(),
                since_handles: HashMap::new(),
                index_since_updates: Rc::new(RefCell::new(HashMap::new())),
                source_since_updates: Rc::new(RefCell::new(HashMap::new())),
                sink_writes: HashMap::new(),
                pending_peeks: HashMap::new(),
                pending_tails: HashMap::new(),
                write_lock: Arc::new(tokio::sync::Mutex::new(())),
                write_lock_wait_group: VecDeque::new(),
            };
            let logging = logging.map(|config| DataflowLoggingConfig {
                granularity_ns: config.granularity.as_nanos(),
                active_logs: BUILTINS
                    .logs()
                    .map(|src| (src.variant.clone(), src.index_id))
                    .collect(),
                log_logging: config.log_logging,
            });
            handle
                .block_on(
                    coord
                        .dataflow_client
                        .create_instance(DEFAULT_COMPUTE_INSTANCE_ID, logging),
                )
                .unwrap();
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
    ExecuteResponse::SendingRows(Box::pin(async { PeekResponseUnary::Rows(rows) }))
}

fn auto_generate_primary_idx(
    index_name: String,
    on_name: FullName,
    on_id: GlobalId,
    on_desc: &RelationDesc,
    conn_id: Option<u32>,
    depends_on: Vec<GlobalId>,
    enabled: bool,
) -> catalog::Index {
    let default_key = on_desc.typ().default_key();
    catalog::Index {
        create_sql: index_sql(index_name, on_name, &on_desc, &default_key),
        on: on_id,
        keys: default_key
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect(),
        conn_id,
        depends_on,
        enabled,
    }
}

// TODO(benesch): constructing the canonical CREATE INDEX statement should be
// the responsibility of the SQL package.
pub fn index_sql(
    index_name: String,
    view_name: FullName,
    view_desc: &RelationDesc,
    keys: &[usize],
) -> String {
    use mz_sql::ast::{Expr, Value};

    CreateIndexStatement::<Raw> {
        name: Some(Ident::new(index_name)),
        on_name: mz_sql::normalize::unresolve(view_name),
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
pub fn describe(
    catalog: &Catalog,
    stmt: Statement<Raw>,
    param_types: &[Option<ScalarType>],
    session: &Session,
) -> Result<StatementDesc, CoordError> {
    match stmt {
        // FETCH's description depends on the current session, which describe_statement
        // doesn't (and shouldn't?) have access to, so intercept it here.
        Statement::Fetch(FetchStatement { ref name, .. }) => {
            match session.get_portal(name.as_str()).map(|p| p.desc.clone()) {
                Some(desc) => Ok(desc),
                None => Err(CoordError::UnknownCursor(name.to_string())),
            }
        }
        _ => {
            let catalog = &catalog.for_session(session);
            Ok(mz_sql::plan::describe(
                &session.pcx(),
                catalog,
                stmt,
                param_types,
            )?)
        }
    }
}

fn check_statement_safety(stmt: &Statement<Raw>) -> Result<(), CoordError> {
    let (source_or_sink, typ, with_options) = match stmt {
        Statement::CreateSource(CreateSourceStatement {
            connector,
            with_options,
            ..
        }) => ("source", ConnectorType::from(connector), with_options),
        Statement::CreateSink(CreateSinkStatement {
            connector,
            with_options,
            ..
        }) => ("sink", ConnectorType::from(connector), with_options),
        _ => return Ok(()),
    };
    match typ {
        // File sources and sinks are prohibited in safe mode because they allow
        // reading ƒrom and writing to arbitrary files on disk.
        ConnectorType::File => {
            return Err(CoordError::SafeModeViolation(format!(
                "file {}",
                source_or_sink
            )));
        }
        ConnectorType::AvroOcf => {
            return Err(CoordError::SafeModeViolation(format!(
                "Avro OCF {}",
                source_or_sink
            )));
        }
        // Kerberos-authenticated Kafka sources and sinks are prohibited in
        // safe mode because librdkafka will blindly execute the string passed
        // as `sasl_kerberos_kinit_cmd`.
        ConnectorType::Kafka => {
            // It's too bad that we have to reinvent so much of librdkafka's
            // option parsing and hardcode some of its defaults here. But there
            // isn't an obvious alternative; asking librdkafka about its =
            // defaults requires constructing a librdkafka client, and at that
            // point it's already too late.
            let mut with_options = mz_sql::normalize::options(with_options);
            let with_options = mz_sql::kafka_util::extract_config(&mut with_options)?;
            let security_protocol = with_options
                .get("security.protocol")
                .map(|v| v.as_str())
                .unwrap_or("plaintext");
            let sasl_mechanism = with_options
                .get("sasl.mechanisms")
                .map(|v| v.as_str())
                .unwrap_or("GSSAPI");
            if (security_protocol.eq_ignore_ascii_case("sasl_plaintext")
                || security_protocol.eq_ignore_ascii_case("sasl_ssl"))
                && sasl_mechanism.eq_ignore_ascii_case("GSSAPI")
            {
                return Err(CoordError::SafeModeViolation(format!(
                    "Kerberos-authenticated Kafka {}",
                    source_or_sink,
                )));
            }
        }
        _ => (),
    }
    Ok(())
}

/// Logic and types for fast-path determination for dataflow execution.
///
/// This module determines if a dataflow can be short-cut, by returning constant values
/// or by reading out of existing arrangements, and implements the appropriate plan.
pub mod fast_path_peek {

    use mz_dataflow_types::client::DEFAULT_COMPUTE_INSTANCE_ID;
    use mz_dataflow_types::PeekResponseUnary;
    use std::{collections::HashMap, num::NonZeroUsize};

    use crate::CoordError;
    use mz_expr::{EvalError, GlobalId, Id, MirScalarExpr};
    use mz_repr::{Diff, Row};

    #[derive(Debug)]
    pub struct PeekDataflowPlan<T> {
        desc: mz_dataflow_types::DataflowDescription<mz_dataflow_types::Plan<T>, T>,
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
        dataflow_plan: mz_dataflow_types::DataflowDescription<mz_dataflow_types::Plan>,
        view_id: GlobalId,
        index_id: GlobalId,
        index_key: Vec<MirScalarExpr>,
        index_permutation: HashMap<usize, usize>,
        index_thinned_arity: usize,
    ) -> Result<Plan, CoordError> {
        // At this point, `dataflow_plan` contains our best optimized dataflow.
        // We will check the plan to see if there is a fast path to escape full dataflow construction.

        // We need to restrict ourselves to settings where the inserted transient view is the first thing
        // to build (no dependent views). There is likely an index to build as well, but we may not be sure.
        if dataflow_plan.objects_to_build.len() >= 1
            && dataflow_plan.objects_to_build[0].id == view_id
        {
            match &dataflow_plan.objects_to_build[0].view {
                // In the case of a constant, we can return the result now.
                mz_dataflow_types::Plan::Constant { rows } => {
                    return Ok(Plan::Constant(rows.clone()));
                }
                // In the case of a bare `Get`, we may be able to directly index an arrangement.
                mz_dataflow_types::Plan::Get {
                    id,
                    keys: _,
                    mfp,
                    key_val,
                } => {
                    // Convert `mfp` to an executable, non-temporal plan.
                    // It should be non-temporal, as OneShot preparation populates `mz_logical_timestamp`.
                    let map_filter_project = mfp
                        .clone()
                        .into_plan()
                        .map_err(|e| crate::error::CoordError::Unstructured(::anyhow::anyhow!(e)))?
                        .into_nontemporal()
                        .map_err(|_e| {
                            crate::error::CoordError::Unstructured(::anyhow::anyhow!(
                                "OneShot plan has temporal constraints"
                            ))
                        })?;
                    // We should only get excited if we can track down an index for `id`.
                    // If `keys` is non-empty, that means we think one exists.
                    for (index_id, (desc, _typ)) in dataflow_plan.index_imports.iter() {
                        if let Some((key, val)) = key_val {
                            if Id::Global(desc.on_id) == *id && &desc.key == key {
                                // Indicate an early exit with a specific index and key_val.
                                return Ok(Plan::PeekExisting(
                                    *index_id,
                                    val.clone(),
                                    map_filter_project,
                                ));
                            }
                        } else if Id::Global(desc.on_id) == *id {
                            // Indicate an early exit with a specific index and no key_val.
                            return Ok(Plan::PeekExisting(*index_id, None, map_filter_project));
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

    impl crate::coord::Coordinator {
        /// Implements a peek plan produced by `create_plan` above.
        pub async fn implement_fast_path_peek(
            &mut self,
            fast_path: Plan,
            timestamp: mz_repr::Timestamp,
            finishing: mz_expr::RowSetFinishing,
            conn_id: u32,
            source_arity: usize,
        ) -> Result<crate::ExecuteResponse, CoordError> {
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
                    (
                        id,
                        key,
                        conn_id,
                        timestamp,
                        finishing.clone(),
                        map_filter_project,
                    ),
                    None,
                ),
                Plan::PeekDataflow(PeekDataflowPlan {
                    desc: dataflow,
                    id: index_id,
                    key: index_key,
                    permutation: index_permutation,
                    thinned_arity: index_thinned_arity,
                }) => {
                    // Very important: actually create the dataflow (here, so we can destructure).
                    self.dataflow_client
                        .compute(DEFAULT_COMPUTE_INSTANCE_ID)
                        .unwrap()
                        .create_dataflows(vec![dataflow])
                        .await
                        .unwrap();
                    // Create an identity MFP operator.
                    let mut map_filter_project = mz_expr::MapFilterProject::new(source_arity);
                    map_filter_project
                        .permute(index_permutation, index_key.len() + index_thinned_arity);
                    let map_filter_project = map_filter_project
                        .into_plan()
                        .map_err(|e| crate::error::CoordError::Unstructured(::anyhow::anyhow!(e)))?
                        .into_nontemporal()
                        .map_err(|_e| {
                            crate::error::CoordError::Unstructured(::anyhow::anyhow!(
                                "OneShot plan has temporal constraints"
                            ))
                        })?;
                    (
                        (
                            index_id, // transient identifier produced by `dataflow_plan`.
                            None,
                            conn_id,
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

            // The peek is ready to go for both cases, fast and non-fast.
            // Stash the response mechanism, and broadcast dataflow construction.
            self.pending_peeks.insert(conn_id, rows_tx);
            let (id, key, conn_id, timestamp, _finishing, map_filter_project) = peek_command;
            self.dataflow_client
                .compute(DEFAULT_COMPUTE_INSTANCE_ID)
                .unwrap()
                .peek(
                    id,
                    key,
                    conn_id,
                    timestamp,
                    finishing.clone(),
                    map_filter_project,
                )
                .await
                .unwrap();

            use futures::FutureExt;
            use futures::StreamExt;
            use mz_dataflow_types::PeekResponse;

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
                self.drop_indexes(vec![index_id]).await;
            }

            Ok(crate::ExecuteResponse::SendingRows(Box::pin(rows_rx)))
        }
    }
}

#[cfg(test)]
impl Coordinator {
    #[allow(dead_code)]
    async fn verify_ship_dataflow_no_error(&mut self) {
        // ship_dataflow, ship_dataflows, and finalize_dataflow are not allowed
        // to have a `Result` return because these functions are called after
        // `catalog_transact`, after which no errors are allowed. This test exists to
        // prevent us from incorrectly teaching those functions how to return errors
        // (which has happened twice and is the motivation for this test).

        let df = DataflowDesc::new("".into(), GlobalId::Explain);
        let _: () = self.ship_dataflow(df.clone()).await;
        let _: () = self.ship_dataflows(vec![df.clone()]).await;
        let _: DataflowDescription<mz_dataflow_types::plan::Plan> = self.finalize_dataflow(df);
    }
}
