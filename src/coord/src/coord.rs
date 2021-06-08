// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordination of installed views, available timestamps, compacted timestamps, and transactions.
//!
//! The command coordinator maintains a view of the installed
//! views, and for each tracks the frontier of available times
//! ([`upper`](arrangement_state::Frontiers::upper)) and the frontier
//! of compacted times ([`since`](arrangement_state::Frontiers::since)).
//! The upper frontier describes times that may not return immediately, as any
//! timestamps in advance of the frontier are still open. The since frontier
//! constrains those times for which the maintained view will be correct,
//! as any timestamps in advance of the frontier must accumulate to the same
//! value as would an un-compacted trace. The since frontier cannot be directly
//! mutated, but instead can have multiple handles to it which forward changes
//! from an internal MutableAntichain to the since.
//!
//! The [`Coordinator`] tracks various compaction frontiers
//! so that indexes, compaction, and transactions can work
//! together. [`determine_timestamp()`](Coordinator::determine_timestamp)
//! returns the least valid since of its sources. Any new transactions
//! should thus always be >= the current compaction frontier
//! and so should never change the frontier when being added to
//! [`txn_reads`](Coordinator::txn_reads). The compaction frontier may
//! change when a transaction ends (if it was the oldest transaction and
//! the index's since was advanced after the transaction started) or when
//! [`update_upper()`](Coordinator::update_upper) is run (if there are no in
//! progress transactions before the new since). When it does, it is added to
//! [`since_updates`](Coordinator::since_updates) and will be processed during
//! the next [`maintenance()`](Coordinator::maintenance) call.

use std::cell::RefCell;
use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryInto;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use self::prometheus::{Scraper, ScraperMessage};
use anyhow::{anyhow, Context};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::future::{self, FutureExt, TryFutureExt};
use futures::stream::{self, StreamExt};
use itertools::Itertools;
use rand::Rng;
use timely::communication::WorkerGuards;
use timely::order::PartialOrder;
use timely::progress::{Antichain, ChangeBatch, Timestamp as _};
use tokio::runtime::Handle as TokioHandle;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;

use build_info::BuildInfo;
use dataflow::{
    CacheMessage, SequencedCommand, TimestampBindingFeedback, WorkerFeedback,
    WorkerFeedbackWithMeta,
};
use dataflow_types::logging::LoggingConfig as DataflowLoggingConfig;
use dataflow_types::{
    DataflowDesc, ExternalSourceConnector, IndexDesc, PeekResponse, PostgresSourceConnector,
    SinkConnector, SourceConnector, TailSinkConnector, TimestampSourceUpdate, Update,
};
use dataflow_types::{SinkAsOf, SinkEnvelope};
use expr::{
    ExprHumanizer, GlobalId, Id, MirRelationExpr, MirScalarExpr, NullaryFunc,
    OptimizedMirRelationExpr,
};
use ore::antichain::AntichainToken;
use ore::str::StrExt;
use ore::thread::{JoinHandleExt, JoinOnDropHandle};
use repr::{ColumnName, Datum, RelationDesc, Row, Timestamp};
use sql::ast::display::AstDisplay;
use sql::ast::{
    Connector, CreateIndexStatement, CreateSchemaStatement, CreateSinkStatement,
    CreateSourceStatement, CreateTableStatement, DropObjectsStatement, ExplainStage,
    FetchStatement, Ident, ObjectType, Raw, Statement,
};
use sql::catalog::{Catalog as _, CatalogError};
use sql::names::{DatabaseSpecifier, FullName};
use sql::plan::StatementDesc;
use sql::plan::{
    AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan, AlterItemRenamePlan, CreateDatabasePlan,
    CreateIndexPlan, CreateRolePlan, CreateSchemaPlan, CreateSinkPlan, CreateSourcePlan,
    CreateTablePlan, CreateTypePlan, CreateViewPlan, CreateViewsPlan, DropDatabasePlan,
    DropItemsPlan, DropRolesPlan, DropSchemaPlan, ExplainPlan, FetchPlan, IndexOption,
    IndexOptionName, InsertPlan, MutationKind, Params, PeekPlan, PeekWhen, Plan, PlanContext,
    SendDiffsPlan, SetVariablePlan, ShowVariablePlan, Source, TailPlan,
};
use storage::Message as PersistedMessage;
use transform::Optimizer;

use self::arrangement_state::{ArrangementFrontiers, Frontiers, SinkWrites};
use crate::cache::{CacheConfig, Cacher};
use crate::catalog::builtin::{BUILTINS, MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS};
use crate::catalog::{self, BuiltinTableUpdate, Catalog, CatalogItem, SinkConnectorState};
use crate::client::{Client, Handle};
use crate::command::{
    Cancelled, Command, ExecuteResponse, Response, StartupMessage, StartupResponse,
};
use crate::error::CoordError;
use crate::persistence::{PersistenceConfig, PersistentTables};
use crate::session::{
    EndTransactionAction, PreparedStatement, Session, TransactionOps, TransactionStatus, WriteOp,
};
use crate::sink_connector;
use crate::timestamp::{TimestampMessage, Timestamper};
use crate::util::ClientTransmitter;

mod arrangement_state;
mod dataflow_builder;
mod prometheus;

#[derive(Debug)]
pub enum Message {
    Command(Command),
    Worker(WorkerFeedbackWithMeta),
    AdvanceSourceTimestamp(AdvanceSourceTimestamp),
    StatementReady(StatementReady),
    SinkConnectorReady(SinkConnectorReady),
    InsertBuiltinTableUpdates(TimestampedUpdate),
    Shutdown,
}

#[derive(Debug)]
pub struct AdvanceSourceTimestamp {
    pub id: GlobalId,
    pub update: TimestampSourceUpdate,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StatementReady {
    pub session: Session,
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub result: Result<sql::ast::Statement<Raw>, CoordError>,
    pub params: Params,
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
}

/// Configures a coordinator.
pub struct Config<'a> {
    pub workers: usize,
    pub timely_worker: timely::WorkerConfig,
    pub symbiosis_url: Option<&'a str>,
    pub logging: Option<LoggingConfig>,
    pub data_directory: &'a Path,
    pub timestamp_frequency: Duration,
    pub cache: Option<CacheConfig>,
    pub persistence: Option<PersistenceConfig>,
    pub logical_compaction_window: Option<Duration>,
    pub experimental_mode: bool,
    pub safe_mode: bool,
    pub build_info: &'static BuildInfo,
}

/// Glues the external world to the Timely workers.
pub struct Coordinator {
    worker_guards: WorkerGuards<()>,
    worker_txs: Vec<crossbeam_channel::Sender<SequencedCommand>>,
    optimizer: Optimizer,
    catalog: Catalog,
    symbiosis: Option<symbiosis::Postgres>,
    /// Maps (global Id of arrangement) -> (frontier information). This tracks the
    /// `upper` and computed `since` of the indexes. The `since` is the time at
    /// which we are willing to compact up to. `determine_timestamp()` uses this as
    /// part of its heuristic when determining a viable timestamp for queries.
    indexes: ArrangementFrontiers<Timestamp>,
    /// Map of frontier information for sources
    sources: ArrangementFrontiers<Timestamp>,
    /// Delta from leading edge of an arrangement from which we allow compaction.
    logical_compaction_window_ms: Option<Timestamp>,
    /// Instance count: number of times sources have been instantiated in views. This is used
    /// to associate each new instance of a source with a unique instance id (iid)
    logging_granularity: Option<u64>,
    /// Channel to manange internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    /// Channel to communicate source status updates to the timestamper thread.
    ts_tx: std::sync::mpsc::Sender<TimestampMessage>,
    metric_scraper_tx: Option<std::sync::mpsc::Sender<ScraperMessage>>,
    /// Channel to communicate source status updates and shutdown notifications to the cacher
    /// thread.
    cache_tx: Option<mpsc::UnboundedSender<CacheMessage>>,
    /// The last timestamp we assigned to a read.
    read_lower_bound: Timestamp,
    /// The timestamp that all local inputs have been advanced up to.
    closed_up_to: Timestamp,
    /// Whether or not the most recent operation was a read.
    last_op_was_read: bool,
    /// Whether we need to advance local inputs (i.e., did someone observe a timestamp).
    // TODO(justin): this is a hack, and does not work right with TAIL.
    need_advance: bool,
    transient_id_counter: u64,
    /// A map from connection ID to metadata about that connection for all
    /// active connections.
    active_conns: HashMap<u32, ConnMeta>,
    /// Map of all persisted tables.
    persisted_tables: Option<PersistentTables>,

    /// Holds pending compaction messages to be sent to the dataflow workers. When
    /// `since_handles` are advanced or `txn_reads` are dropped, this can advance.
    since_updates: Rc<RefCell<HashMap<GlobalId, Antichain<Timestamp>>>>,
    /// Holds handles to ids that are advanced by update_upper.
    since_handles: HashMap<GlobalId, AntichainToken<Timestamp>>,
    /// Tracks active read transactions so that we don't compact any indexes beyond
    /// an in-progress transaction.
    // TODO(mjibson): Should this live on a Session?
    txn_reads: HashMap<u32, TxnReads>,
    /// Tracks write frontiers for active exactly-once sinks.
    sink_writes: HashMap<GlobalId, SinkWrites<Timestamp>>,
}

/// Metadata about an active connection.
struct ConnMeta {
    /// A watch channel shared with the client to inform the client of
    /// cancellation requests. The coordinator sets the contained value to
    /// `Cancelled::Cancelled` whenever it receives a cancellation request that
    /// targets this connection. It is the client's responsibility to check this
    /// value when appropriate and to reset the value to
    /// `Cancelled::NotCancelled` before starting a new operation.
    cancel_tx: Arc<watch::Sender<Cancelled>>,
    /// Pgwire specifies that every connection have a 32-bit secret associated
    /// with it, that is known to both the client and the server. Cancellation
    /// requests are required to authenticate with the secret of the connection
    /// that they are targeting.
    secret_key: u32,
}

struct TxnReads {
    timedomain_ids: HashSet<GlobalId>,
    _handles: Vec<AntichainToken<Timestamp>>,
}

impl Coordinator {
    fn num_workers(&self) -> usize {
        self.worker_txs.len()
    }

    /// Assign a timestamp for a read.
    fn get_read_ts(&mut self) -> Timestamp {
        let ts = self.get_ts();
        self.last_op_was_read = true;
        self.read_lower_bound = ts;
        ts
    }

    /// Assign a timestamp for a write. Writes following reads must ensure that they are assigned a
    /// strictly larger timestamp to ensure they are not visible to any real-time earlier reads.
    fn get_write_ts(&mut self) -> Timestamp {
        let ts = if self.last_op_was_read {
            self.last_op_was_read = false;
            cmp::max(self.get_ts(), self.read_lower_bound + 1)
        } else {
            self.get_ts()
        };
        self.read_lower_bound = cmp::max(ts, self.closed_up_to);
        self.read_lower_bound
    }

    /// Fetch a new timestamp.
    fn get_ts(&mut self) -> Timestamp {
        // Next time we have a chance, we will force all local inputs forward.
        self.need_advance = true;
        // This is a hack. In a perfect world we would represent time as having a "real" dimension
        // and a "coordinator" dimension so that clients always observed linearizability from
        // things the coordinator did without being related to the real dimension.
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to get millis since epoch")
            .as_millis()
            .try_into()
            .expect("current time did not fit into u64");

        if ts < self.read_lower_bound {
            self.read_lower_bound
        } else {
            ts
        }
    }

    /// Generate a new frontiers object that forwards since changes to since_updates.
    ///
    /// # Panics
    ///
    /// This function panics if called twice with the same `id`.
    fn new_frontiers<I>(
        &mut self,
        id: GlobalId,
        initial: I,
        compaction_window_ms: Option<Timestamp>,
    ) -> Frontiers<Timestamp>
    where
        I: IntoIterator<Item = Timestamp>,
    {
        let since_updates = Rc::clone(&self.since_updates);
        let (frontier, handle) = Frontiers::new(
            self.num_workers(),
            initial,
            compaction_window_ms,
            move |frontier| {
                since_updates.borrow_mut().insert(id, frontier);
            },
        );
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
                //currently catalog item rebuild assumes that sinks and
                //indexes are always built individually and does not store information
                //about how it was built. If we start building multiple sinks and/or indexes
                //using a single dataflow, we have to make sure the rebuild process re-runs
                //the same multiple-build dataflow.
                CatalogItem::Source(source) => {
                    // Inform the timestamper about this source.
                    self.update_timestamper(entry.id(), true).await;
                    self.maybe_begin_caching(entry.id(), source.connector.caching_enabled())
                        .await;
                    let frontiers =
                        self.new_frontiers(entry.id(), Some(0), self.logical_compaction_window_ms);
                    self.sources.insert(entry.id(), frontiers);
                }
                CatalogItem::Index(_) => {
                    if BUILTINS.logs().any(|log| log.index_id == entry.id()) {
                        // Indexes on logging views are special, as they are
                        // already installed in the dataflow plane via
                        // `SequencedCommand::EnableLogging`. Just teach the
                        // coordinator of their existence, without creating a
                        // dataflow for the index.
                        //
                        // TODO(benesch): why is this hardcoded to 1000?
                        // Should it not be the same logical compaction window
                        // that everything else uses?
                        let frontiers = self.new_frontiers(entry.id(), Some(0), Some(1_000));
                        self.indexes.insert(entry.id(), frontiers);
                    } else {
                        let df = self.dataflow_builder().build_index_dataflow(entry.id());
                        self.ship_dataflow(df).await;
                    }
                }
                _ => (), // Handled in next loop.
            }
        }

        for entry in entries {
            match entry.item() {
                CatalogItem::View(_) => (),
                CatalogItem::Table(_) => {
                    // TODO gross hack for now
                    if !entry.id().is_system() {
                        if let Some(tables) = &mut self.persisted_tables {
                            if let Some(messages) = tables.resume(entry.id()) {
                                let mut updates = vec![];
                                for persisted_message in messages.into_iter() {
                                    match persisted_message {
                                        PersistedMessage::Progress(time) => {
                                            // Send the messages accumulated so far + update
                                            // progress
                                            // TODO: I think we need to avoid downgrading capabilities until
                                            // all rows have been sent so the table is not visible for reads
                                            // before being fully reloaded.
                                            let updates = std::mem::replace(&mut updates, vec![]);
                                            if !updates.is_empty() {
                                                self.broadcast(SequencedCommand::Insert {
                                                    id: entry.id(),
                                                    updates,
                                                });
                                                self.broadcast(
                                                    SequencedCommand::AdvanceAllLocalInputs {
                                                        advance_to: time,
                                                    },
                                                );
                                            }
                                        }
                                        PersistedMessage::Data(update) => {
                                            updates.push(update);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
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
        if self.logging_granularity.is_some() {
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
        internal_cmd_rx: mpsc::UnboundedReceiver<Message>,
        cmd_rx: mpsc::UnboundedReceiver<Command>,
        feedback_rx: mpsc::UnboundedReceiver<WorkerFeedbackWithMeta>,
        _timestamper_thread_handle: JoinOnDropHandle<()>,
        _metric_thread_handle: Option<JoinOnDropHandle<()>>,
    ) {
        let cmd_stream = UnboundedReceiverStream::new(cmd_rx)
            .map(Message::Command)
            .chain(stream::once(future::ready(Message::Shutdown)));

        let feedback_stream = UnboundedReceiverStream::new(feedback_rx).map(Message::Worker);

        let mut messages = ore::future::select_all_biased(vec![
            // Order matters here. We want to drain internal commands
            // (`internal_cmd_rx` and `feedback_stream`) before processing
            // external commands (`cmd_stream`).
            UnboundedReceiverStream::new(internal_cmd_rx).boxed(),
            feedback_stream.boxed(),
            cmd_stream.boxed(),
        ]);

        while let Some(msg) = messages.next().await {
            match msg {
                Message::Command(cmd) => self.message_command(cmd).await,
                Message::Worker(worker) => self.message_worker(worker).await,
                Message::StatementReady(ready) => self.message_statement_ready(ready).await,
                Message::SinkConnectorReady(ready) => {
                    self.message_sink_connector_ready(ready).await
                }
                Message::AdvanceSourceTimestamp(advance) => {
                    self.message_advance_source_timestamp(advance).await
                }
                Message::InsertBuiltinTableUpdates(update) => {
                    self.send_builtin_table_updates_at_offset(
                        update.timestamp_offset,
                        update.updates,
                    )
                    .await
                }
                Message::Shutdown => {
                    self.message_shutdown().await;
                    break;
                }
            }

            let needed = self.need_advance;
            let mut next_ts = self.get_ts();
            self.need_advance = false;
            if next_ts <= self.read_lower_bound {
                next_ts = self.read_lower_bound + 1;
            }
            // TODO(justin): this is pretty hacky, and works more-or-less because this frequency
            // lines up with that used in the logging views.
            if needed
                || self.logging_granularity.is_some()
                    && next_ts / self.logging_granularity.unwrap()
                        > self.closed_up_to / self.logging_granularity.unwrap()
            {
                if next_ts > self.closed_up_to {
                    if let Some(tables) = &mut self.persisted_tables {
                        tables.write_progress(next_ts);
                    }
                    self.broadcast(SequencedCommand::AdvanceAllLocalInputs {
                        advance_to: next_ts,
                    });
                    self.closed_up_to = next_ts;
                }
            }
        }

        // Cleanly drain any pending messages from the worker before shutting
        // down.
        drop(self.internal_cmd_tx);
        while messages.next().await.is_some() {}
    }

    async fn message_worker(
        &mut self,
        WorkerFeedbackWithMeta {
            worker_id: _,
            message,
        }: WorkerFeedbackWithMeta,
    ) {
        match message {
            WorkerFeedback::FrontierUppers(updates) => {
                for (name, changes) in updates {
                    self.update_upper(&name, changes);
                }
                self.maintenance().await;
            }
            WorkerFeedback::TimestampBindings(TimestampBindingFeedback { bindings, changes }) => {
                self.catalog
                    .insert_timestamp_bindings(
                        bindings
                            .into_iter()
                            .map(|(id, pid, ts, offset)| (id, pid.to_string(), ts, offset.offset)),
                    )
                    .expect("inserting timestamp bindings cannot fail");

                let mut durability_updates = Vec::new();
                for (source_id, mut changes) in changes {
                    if let Some(source_state) = self.sources.get_mut(&source_id) {
                        // Apply the updates the dataflow worker sent over, and check if there
                        // were any changes to the source's upper frontier.
                        let changes: Vec<_> = source_state
                            .durability
                            .update_iter(changes.drain())
                            .collect();

                        if !changes.is_empty() {
                            // The source's durability frontier changed as a result of the updates sent over
                            // by the dataflow workers. Advance the durability frontier known to the dataflow worker
                            // to indicate that these bindings have been persisted.
                            durability_updates
                                .push((source_id, source_state.durability.frontier().to_owned()));
                        }

                        // Let's also check to see if we can compact any of the bindings we've received.
                        let compaction_ts = if <_ as PartialOrder>::less_equal(
                            &source_state.since.borrow().frontier(),
                            &source_state.durability.frontier(),
                        ) {
                            // In this case we have persisted ahead of the compaction frontier and can safely compact
                            // up to it
                            *source_state
                                .since
                                .borrow()
                                .frontier()
                                .first()
                                .expect("known to exist")
                        } else {
                            // Otherwise, the compaction frontier is ahead of what we've persisted so far, but we can
                            // still potentially compact up whatever we have persisted to this point.
                            // Note that we have to subtract from the durability frontier because it functions as the
                            // least upper bound of whats been persisted, and we decline to compact up to the empty
                            // frontier.
                            source_state
                                .durability
                                .frontier()
                                .first()
                                .unwrap_or(&0)
                                .saturating_sub(1)
                        };

                        self.catalog
                            .compact_timestamp_bindings(source_id, compaction_ts)
                            .expect("compacting timestamp bindings cannot fail");
                    }
                }

                // Announce the new frontiers that have been durably persisted.
                if !durability_updates.is_empty() {
                    self.broadcast(SequencedCommand::DurabilityFrontierUpdates(
                        durability_updates,
                    ));
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
            Ok((pcx, plan)) => self.sequence_plan(tx, session, pcx, plan).await,
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
                        .expect("marking sink ready should never fail");
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
                self.catalog_transact(vec![catalog::Op::DropItem(id)])
                    .await
                    .expect("deleting placeholder sink cannot fail");
                tx.send(Err(e), session);
            }
        }
    }

    async fn message_shutdown(&mut self) {
        self.metric_scraper_tx
            .as_ref()
            .map(|tx| tx.send(ScraperMessage::Shutdown).unwrap());
        self.ts_tx.send(TimestampMessage::Shutdown).unwrap();
        self.broadcast(SequencedCommand::Shutdown);
    }

    async fn message_advance_source_timestamp(
        &mut self,
        AdvanceSourceTimestamp { id, update }: AdvanceSourceTimestamp,
    ) {
        self.broadcast(SequencedCommand::AdvanceSourceTimestamp { id, update });
    }

    async fn message_command(&mut self, cmd: Command) {
        match cmd {
            Command::Startup {
                session,
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

                let catalog = self.catalog.for_session(&session);
                if catalog.resolve_role(session.user()).is_err() {
                    let _ = tx.send(Response {
                        result: Err(CoordError::UnknownLoginRole(session.user().into())),
                        session,
                    });
                    return;
                }

                let mut messages = vec![];
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
                            &TransactionStatus::Started(_) | &TransactionStatus::Failed => {
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
                                | Statement::Declare(_)
                                | Statement::Discard(_)
                                | Statement::Explain(_)
                                | Statement::Fetch(_)
                                | Statement::Insert(_)
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
                                | Statement::StartTransaction(_)
                                | Statement::Tail(_) => {
                                    // Always safe.
                                }

                                // Statements below must by run singly (in Started).
                                Statement::AlterIndexOptions(_)
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
                                | Statement::SetVariable(_)
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
                        let purify_fut = sql::pure::purify(&catalog, stmt);
                        tokio::spawn(async move {
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
                let result = self
                    .sequence_copy_rows(&mut session, id, columns, rows)
                    .await;
                let _ = tx.send(Response { result, session });
            }

            Command::Terminate { mut session } => {
                self.handle_terminate(&mut session).await;
            }

            Command::Commit {
                action,
                mut session,
                tx,
            } => {
                let result = self.sequence_end_transaction(&mut session, action).await;
                let _ = tx.send(Response { result, session });
            }
        }
    }

    /// Updates the upper frontier of a named view.
    fn update_upper(&mut self, name: &GlobalId, mut changes: ChangeBatch<Timestamp>) {
        if let Some(index_state) = self.indexes.get_mut(name) {
            let changes: Vec<_> = index_state.upper.update_iter(changes.drain()).collect();
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
        } else if let Some(source_state) = self.sources.get_mut(name) {
            let changes: Vec<_> = source_state.upper.update_iter(changes.drain()).collect();
            if !changes.is_empty() {
                if let Some(compaction_window_ms) = source_state.compaction_window_ms {
                    if !source_state.upper.frontier().is_empty() {
                        self.since_handles.get_mut(name).unwrap().maybe_advance(
                            source_state.upper.frontier().iter().map(|time| {
                                compaction_window_ms
                                    * (time.saturating_sub(compaction_window_ms)
                                        / compaction_window_ms)
                            }),
                        );
                    }
                }
            }
        } else if let Some(sink_state) = self.sink_writes.get_mut(name) {
            let changes: Vec<_> = sink_state.frontier.update_iter(changes.drain()).collect();

            if !changes.is_empty() {
                sink_state.advance_source_handles();
            }
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
        let since_updates: Vec<_> = self
            .since_updates
            .borrow_mut()
            .drain()
            .filter(|(_, frontier)| frontier != &Antichain::new())
            .collect();

        if !since_updates.is_empty() {
            if let Some(tables) = &mut self.persisted_tables {
                tables.allow_compaction(&since_updates);
            }
            self.broadcast(SequencedCommand::AllowCompaction(since_updates));
        }
    }

    async fn handle_statement(
        &mut self,
        session: &mut Session,
        stmt: sql::ast::Statement<Raw>,
        params: &sql::plan::Params,
    ) -> Result<(PlanContext, sql::plan::Plan), CoordError> {
        let pcx = session.pcx();

        // When symbiosis mode is enabled, use symbiosis planning for:
        //  - CREATE TABLE
        //  - CREATE SCHEMA
        //  - DROP TABLE
        //  - INSERT
        // When these statements are routed through symbiosis, table information
        // is created and maintained locally, which is required for other statements
        // to be executed correctly.
        if let Statement::CreateTable(CreateTableStatement { .. })
        | Statement::DropObjects(DropObjectsStatement {
            object_type: ObjectType::Table,
            ..
        })
        | Statement::CreateSchema(CreateSchemaStatement { .. })
        | Statement::Insert { .. } = &stmt
        {
            if let Some(ref mut postgres) = self.symbiosis {
                let plan = postgres
                    .execute(&pcx, &self.catalog.for_session(session), &stmt)
                    .await?;
                return Ok((pcx, plan));
            }
        }

        match sql::plan::plan(
            &pcx,
            &self.catalog.for_session(session),
            stmt.clone(),
            params,
        ) {
            Ok(plan) => Ok((pcx, plan)),
            Err(err) => match self.symbiosis {
                Some(ref mut postgres) if postgres.can_handle(&stmt) => {
                    let plan = postgres
                        .execute(&pcx, &self.catalog.for_session(session), &stmt)
                        .await?;
                    Ok((pcx, plan))
                }
                _ => Err(err.into()),
            },
        }
    }

    fn handle_declare(
        &self,
        session: &mut Session,
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<(), CoordError> {
        // handle_describe cares about symbiosis mode here. Declared cursors are
        // perhaps rare enough we can ignore that worry and just error instead.
        let desc = describe(
            &self.catalog.for_session(session),
            stmt.clone(),
            &param_types,
            Some(session),
        )?;
        let params = vec![];
        let result_formats = vec![pgrepr::Format::Text; desc.arity()];
        session.set_portal(name, desc, Some(stmt), params, result_formats)?;
        Ok(())
    }

    fn handle_describe(
        &self,
        session: &mut Session,
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<pgrepr::Type>>,
    ) -> Result<(), CoordError> {
        let desc = if let Some(stmt) = stmt.clone() {
            match describe(
                &self.catalog.for_session(session),
                stmt.clone(),
                &param_types,
                Some(session),
            ) {
                Ok(desc) => desc,
                // Describing the query failed. If we're running in symbiosis with
                // Postgres, see if Postgres can handle it. Note that Postgres
                // only handles commands that do not return rows, so the
                // `StatementDesc` is constructed accordingly.
                Err(err) => match self.symbiosis {
                    Some(ref postgres) if postgres.can_handle(&stmt) => StatementDesc::new(None),
                    _ => return Err(err),
                },
            }
        } else {
            StatementDesc::new(None)
        };
        session.set_prepared_statement(name, PreparedStatement::new(stmt, desc));
        Ok(())
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

            // Tell dataflow to cancel any pending peeks.
            self.broadcast(SequencedCommand::CancelPeek { conn_id });

            // Inform the target session (if it asks) about the cancellation.
            let _ = conn_meta.cancel_tx.send(Cancelled::Cancelled);
        }
    }

    /// Handle termination of a client session.
    ///
    /// This cleans up any state in the coordinator associated with the session.
    async fn handle_terminate(&mut self, session: &mut Session) {
        let (drop_sinks, _) = session.clear_transaction();
        self.drop_sinks(drop_sinks).await;

        self.drop_temp_items(session.conn_id()).await;
        self.catalog
            .drop_temporary_schema(session.conn_id())
            .expect("unable to drop temporary schema");
        self.active_conns.remove(&session.conn_id());
    }

    /// Removes all temporary items created by the specified connection, though
    /// not the temporary schema itself.
    async fn drop_temp_items(&mut self, conn_id: u32) {
        let ops = self.catalog.drop_temp_item_ops(conn_id);
        self.catalog_transact(ops)
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
        let ops = vec![
            catalog::Op::DropItem(id),
            catalog::Op::CreateItem {
                id,
                oid,
                name: name.clone(),
                item: CatalogItem::Sink(sink.clone()),
            },
        ];
        self.catalog_transact(ops).await?;
        let as_of = SinkAsOf {
            frontier: self.determine_frontier(sink.from),
            strict: !sink.with_snapshot,
        };
        let df = self.dataflow_builder().build_sink_dataflow(
            name.to_string(),
            id,
            sink.from,
            connector.clone(),
            sink.envelope,
            as_of,
        );

        if let SinkConnector::Kafka(kafka_sink) = connector {
            // For exactly-once Kafka sinks, we need to block compaction of each timestamp binding
            // until all sinks that depend on a given source have finished writing out that timestamp.
            // To achieve that, each sink will hold a AntichainToken for all of the sources it depends
            // on, and will advance all of its source dependencies' compaction frontiers as it completes
            // writes.
            if kafka_sink.exactly_once {
                let mut tokens = Vec::new();

                // Collect AntichainTokens from all of the sources that have them.
                for id in &kafka_sink.transitive_source_dependencies {
                    if let Some(token) = self.since_handles.get(&id) {
                        tokens.push(token.clone());
                    }
                }

                let sink_writes = SinkWrites::new(tokens);
                self.sink_writes.insert(id, sink_writes);
            }
        }
        Ok(self.ship_dataflow(df).await)
    }

    async fn sequence_plan(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        pcx: PlanContext,
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
                tx.send(
                    self.sequence_create_table(&mut session, pcx, plan).await,
                    session,
                );
            }
            Plan::CreateSource(plan) => {
                tx.send(
                    self.sequence_create_source(&mut session, pcx, plan).await,
                    session,
                );
            }
            Plan::CreateSink(plan) => {
                self.sequence_create_sink(session, pcx, plan, tx).await;
            }
            Plan::CreateView(plan) => {
                tx.send(
                    self.sequence_create_view(&mut session, pcx, plan).await,
                    session,
                );
            }
            Plan::CreateViews(plan) => {
                tx.send(
                    self.sequence_create_views(&mut session, pcx, plan).await,
                    session,
                );
            }
            Plan::CreateIndex(plan) => {
                tx.send(self.sequence_create_index(pcx, plan).await, session);
            }
            Plan::CreateType(plan) => {
                tx.send(self.sequence_create_type(pcx, plan).await, session);
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
                tx.send(self.sequence_show_all_variables(&session).await, session);
            }
            Plan::ShowVariable(plan) => {
                tx.send(self.sequence_show_variable(&session, plan).await, session);
            }
            Plan::SetVariable(plan) => {
                tx.send(
                    self.sequence_set_variable(&mut session, plan).await,
                    session,
                );
            }
            Plan::StartTransaction => {
                let duplicated =
                    matches!(session.transaction(), TransactionStatus::InTransaction(_));
                session.start_transaction();
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
                tx.send(
                    self.sequence_end_transaction(&mut session, action).await,
                    session,
                );
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
                tx.send(self.sequence_send_diffs(&mut session, plan).await, session);
            }
            Plan::Insert(plan) => {
                tx.send(self.sequence_insert(&mut session, plan).await, session);
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
        match self.catalog_transact(ops).await {
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
        match self.catalog_transact(vec![op]).await {
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
        self.catalog_transact(vec![op])
            .await
            .map(|_| ExecuteResponse::CreatedRole)
    }

    async fn sequence_create_table(
        &mut self,
        session: &Session,
        pcx: PlanContext,
        plan: CreateTablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let CreateTablePlan {
            name,
            table,
            if_not_exists,
            depends_on,
        } = plan;

        let conn_id = if table.temporary {
            Some(session.conn_id())
        } else {
            None
        };
        let table_id = self.catalog.allocate_id()?;
        let mut index_depends_on = depends_on.clone();
        index_depends_on.push(table_id);
        let table = catalog::Table {
            create_sql: table.create_sql,
            plan_cx: pcx,
            desc: table.desc,
            defaults: table.defaults,
            conn_id,
            depends_on,
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
        );
        let table_oid = self.catalog.allocate_oid()?;
        let index_oid = self.catalog.allocate_oid()?;
        match self
            .catalog_transact(vec![
                catalog::Op::CreateItem {
                    id: table_id,
                    oid: table_oid,
                    name,
                    item: CatalogItem::Table(table),
                },
                catalog::Op::CreateItem {
                    id: index_id,
                    oid: index_oid,
                    name: index_name,
                    item: CatalogItem::Index(index),
                },
            ])
            .await
        {
            Ok(_) => {
                if let Some(tables) = &mut self.persisted_tables {
                    tables.create(table_id);
                }
                let df = self.dataflow_builder().build_index_dataflow(index_id);
                self.ship_dataflow(df).await;
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
        pcx: PlanContext,
        plan: CreateSourcePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        // TODO(petrosagg): remove this check once postgres sources are properly supported
        if matches!(
            plan,
            CreateSourcePlan {
                source: Source {
                    connector: SourceConnector::External {
                        connector: ExternalSourceConnector::Postgres(_),
                        ..
                    },
                    ..
                },
                materialized: false,
                ..
            }
        ) {
            coord_bail!("Unmaterialized Postgres sources are not supported yet");
        }

        let if_not_exists = plan.if_not_exists;
        let (metadata, ops) = self.generate_create_source_ops(session, pcx, vec![plan])?;
        match self.catalog_transact(ops).await {
            Ok(()) => {
                self.ship_sources(metadata).await;
                Ok(ExecuteResponse::CreatedSource { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedSource { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn ship_sources(&mut self, metadata: Vec<(GlobalId, Option<GlobalId>, bool)>) {
        for (source_id, idx_id, caching_enabled) in metadata {
            // Do everything to instantiate the source at the coordinator and
            // inform the timestamper and dataflow workers of its existence before
            // shipping any dataflows that depend on its existence.
            self.update_timestamper(source_id, true).await;
            self.maybe_begin_caching(source_id, caching_enabled).await;
            let frontiers =
                self.new_frontiers(source_id, Some(0), self.logical_compaction_window_ms);
            self.sources.insert(source_id, frontiers);
            if let Some(index_id) = idx_id {
                let df = self.dataflow_builder().build_index_dataflow(index_id);
                self.ship_dataflow(df).await;
            }
        }
    }

    fn generate_create_source_ops(
        &mut self,
        session: &mut Session,
        pcx: PlanContext,
        plans: Vec<CreateSourcePlan>,
    ) -> Result<(Vec<(GlobalId, Option<GlobalId>, bool)>, Vec<catalog::Op>), CoordError> {
        let mut metadata = vec![];
        let mut ops = vec![];
        for plan in plans {
            let CreateSourcePlan {
                name,
                source,
                materialized,
                ..
            } = plan;
            let optimized_expr = self
                .optimizer
                .optimize(source.expr, self.catalog.indexes())?;
            let transformed_desc = RelationDesc::new(optimized_expr.0.typ(), source.column_names);
            let source = catalog::Source {
                create_sql: source.create_sql,
                plan_cx: pcx.clone(),
                optimized_expr,
                connector: source.connector,
                bare_desc: source.bare_desc,
                desc: transformed_desc,
            };
            let source_id = self.catalog.allocate_id()?;
            let source_oid = self.catalog.allocate_oid()?;
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
                let index = auto_generate_primary_idx(
                    index_name.item.clone(),
                    name,
                    source_id,
                    &source.desc,
                    None,
                    vec![source_id],
                );
                let index_id = self.catalog.allocate_id()?;
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
            metadata.push((source_id, index_id, source.connector.caching_enabled()))
        }
        Ok((metadata, ops))
    }

    async fn sequence_create_sink(
        &mut self,
        session: Session,
        pcx: PlanContext,
        plan: CreateSinkPlan,
        tx: ClientTransmitter<ExecuteResponse>,
    ) {
        let CreateSinkPlan {
            name,
            sink,
            with_snapshot,
            if_not_exists,
            depends_on,
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
                plan_cx: pcx,
                from: sink.from,
                connector: catalog::SinkConnectorState::Pending(sink.connector_builder.clone()),
                envelope: sink.envelope,
                with_snapshot,
                depends_on,
            }),
        };
        match self.catalog_transact(vec![op]).await {
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
        tokio::spawn(async move {
            internal_cmd_tx
                .send(Message::SinkConnectorReady(SinkConnectorReady {
                    session,
                    tx,
                    id,
                    oid,
                    result: sink_connector::build(connector_builder, id).await,
                }))
                .expect("sending to internal_cmd_tx cannot fail");
        });
    }

    fn generate_view_ops(
        &mut self,
        session: &Session,
        pcx: PlanContext,
        plan: CreateViewPlan,
    ) -> Result<(Vec<catalog::Op>, Option<GlobalId>), CoordError> {
        let CreateViewPlan {
            name,
            view,
            replace,
            materialize,
            if_not_exists: _,
            depends_on,
        } = plan;

        let mut ops = vec![];

        if let Some(id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[id]));
        }
        let view_id = self.catalog.allocate_id()?;
        let view_oid = self.catalog.allocate_oid()?;
        // Optimize the expression so that we can form an accurately typed description.
        let optimized_expr = self.prep_relation_expr(view.expr, ExprPrepStyle::Static)?;
        let desc = RelationDesc::new(optimized_expr.typ(), view.column_names);
        let view = catalog::View {
            create_sql: view.create_sql,
            plan_cx: pcx,
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
            let mut index_name = name.clone();
            index_name.item += "_primary_idx";
            index_name = self
                .catalog
                .for_session(session)
                .find_available_name(index_name);
            let index = auto_generate_primary_idx(
                index_name.item.clone(),
                name,
                view_id,
                &view.desc,
                view.conn_id,
                vec![view_id],
            );
            let index_id = self.catalog.allocate_id()?;
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
        pcx: PlanContext,
        plan: CreateViewPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let if_not_exists = plan.if_not_exists;
        let (ops, index_id) = self.generate_view_ops(session, pcx, plan)?;

        match self.catalog_transact(ops).await {
            Ok(()) => {
                if let Some(index_id) = index_id {
                    let df = self.dataflow_builder().build_index_dataflow(index_id);
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
        pcx: PlanContext,
        plan: CreateViewsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let mut ops = vec![];
        let mut index_ids = vec![];

        for view_plan in plan.views {
            let (mut view_ops, index_id) = self.generate_view_ops(session, pcx, view_plan)?;
            ops.append(&mut view_ops);
            if let Some(index_id) = index_id {
                index_ids.push(index_id);
            }
        }

        match self.catalog_transact(ops).await {
            Ok(()) => {
                let mut dfs = vec![];
                for index_id in index_ids {
                    let df = self.dataflow_builder().build_index_dataflow(index_id);
                    dfs.push(df);
                }
                self.ship_dataflows(dfs).await;
                Ok(ExecuteResponse::CreatedView { existed: false })
            }
            // TODO somehow check this or remove if not exists modifiers
            // Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedView { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_index(
        &mut self,
        pcx: PlanContext,
        plan: CreateIndexPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let CreateIndexPlan {
            name,
            mut index,
            options,
            if_not_exists,
            depends_on,
        } = plan;

        for key in &mut index.keys {
            Self::prep_scalar_expr(key, ExprPrepStyle::Static)?;
        }
        let index = catalog::Index {
            create_sql: index.create_sql,
            plan_cx: pcx,
            keys: index.keys,
            on: index.on,
            conn_id: None,
            depends_on,
        };
        let id = self.catalog.allocate_id()?;
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Index(index),
        };
        match self.catalog_transact(vec![op]).await {
            Ok(()) => {
                let df = self.dataflow_builder().build_index_dataflow(id);
                self.ship_dataflow(df).await;
                self.set_index_options(id, options);
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
        pcx: PlanContext,
        plan: CreateTypePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let typ = catalog::Type {
            create_sql: plan.typ.create_sql,
            plan_cx: pcx,
            inner: plan.typ.inner.into(),
            depends_on: plan.depends_on,
        };
        let id = self.catalog.allocate_id()?;
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: plan.name,
            item: CatalogItem::Type(typ),
        };
        match self.catalog_transact(vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::CreatedType),
            Err(err) => Err(err),
        }
    }

    async fn sequence_drop_database(
        &mut self,
        plan: DropDatabasePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_database_ops(plan.name);
        self.catalog_transact(ops).await?;
        Ok(ExecuteResponse::DroppedDatabase)
    }

    async fn sequence_drop_schema(
        &mut self,
        plan: DropSchemaPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_schema_ops(plan.name);
        self.catalog_transact(ops).await?;
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
        self.catalog_transact(ops).await?;
        Ok(ExecuteResponse::DroppedRole)
    }

    async fn sequence_drop_items(
        &mut self,
        plan: DropItemsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_items_ops(&plan.items);
        self.catalog_transact(ops).await?;
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

    async fn sequence_show_all_variables(
        &mut self,
        session: &Session,
    ) -> Result<ExecuteResponse, CoordError> {
        Ok(send_immediate_rows(
            session
                .vars()
                .iter()
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

    async fn sequence_show_variable(
        &self,
        session: &Session,
        plan: ShowVariablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let variable = session.vars().get(&plan.name)?;
        let row = Row::pack_slice(&[Datum::String(&variable.value())]);
        Ok(send_immediate_rows(vec![row]))
    }

    async fn sequence_set_variable(
        &self,
        session: &mut Session,
        plan: SetVariablePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        session.vars_mut().set(&plan.name, &plan.value)?;
        Ok(ExecuteResponse::SetVariable { name: plan.name })
    }

    async fn sequence_end_transaction(
        &mut self,
        session: &mut Session,
        action: EndTransactionAction,
    ) -> Result<ExecuteResponse, CoordError> {
        let was_implicit = matches!(
            session.transaction(),
            TransactionStatus::InTransactionImplicit(_)
        );

        let (drop_sinks, txn) = session.clear_transaction();
        self.drop_sinks(drop_sinks).await;

        // Allow compaction of sources from this transaction, regardless of the action.
        self.txn_reads.remove(&session.conn_id());
        // Although the compaction frontier may have advanced, we do not need to
        // call `maintenance` here because it will soon be called after the next
        // `update_upper`.

        if let EndTransactionAction::Commit = action {
            match txn {
                TransactionStatus::Default | TransactionStatus::Failed => {}
                TransactionStatus::Started(ops)
                | TransactionStatus::InTransaction(ops)
                | TransactionStatus::InTransactionImplicit(ops) => {
                    match ops {
                        TransactionOps::Writes(inserts) => {
                            let timestamp = self.get_write_ts();
                            for WriteOp { id, rows } in inserts {
                                // Re-verify this id exists.
                                if self.catalog.try_get_by_id(id).is_none() {
                                    return Err(CoordError::SqlCatalog(CatalogError::UnknownItem(
                                        id.to_string(),
                                    )));
                                }

                                let updates: Vec<_> = rows
                                    .into_iter()
                                    .map(|(row, diff)| Update {
                                        row,
                                        diff,
                                        timestamp,
                                    })
                                    .collect();

                                if let Some(tables) = &mut self.persisted_tables {
                                    tables.write(id, &updates);
                                }
                                self.broadcast(SequencedCommand::Insert { id, updates });
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(ExecuteResponse::TransactionExited {
            tag: action.tag(),
            was_implicit,
        })
    }

    async fn sequence_peek(
        &mut self,
        session: &mut Session,
        plan: PeekPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let PeekPlan {
            source,
            when,
            finishing,
            copy_to,
        } = plan;

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
            let timestamp = session.get_transaction_timestamp(|| {
                // Determine a timestamp that will be valid for anything in any schema
                // referenced by the first query. This is a first pass implementation of "time
                // domains".
                let timedomain_ids = self.catalog.timedomain_for(&source, conn_id);

                // We want to prevent compaction of the indexes consulted by
                // determine_timestamp, not the ones listed in the query.
                let (timestamp, timestamp_ids) =
                    self.determine_timestamp(&timedomain_ids, PeekWhen::Immediately)?;
                let mut handles = vec![];
                for id in timestamp_ids {
                    handles.push(self.indexes.get(&id).unwrap().since_handle(vec![timestamp]));
                }
                let mut timedomain_set = HashSet::new();
                for id in timedomain_ids {
                    timedomain_set.insert(id);
                }
                self.txn_reads.insert(
                    conn_id,
                    TxnReads {
                        timedomain_ids: timedomain_set,
                        _handles: handles,
                    },
                );

                Ok(timestamp)
            })?;

            // Verify that the indexes for this query are in the current read transaction.
            let txn_reads = self.txn_reads.get(&conn_id).unwrap();
            for id in source.global_uses() {
                if !txn_reads.timedomain_ids.contains(&id) {
                    let mut names: Vec<_> = txn_reads
                        .timedomain_ids
                        .iter()
                        // This could filter out a view that has been replaced in another transaction.
                        .filter_map(|id| self.catalog.try_get_by_id(*id))
                        .map(|item| item.name().to_string())
                        .collect();
                    // Sort so error messages are deterministic.
                    names.sort();
                    return Err(CoordError::RelationOutsideTimeDomain {
                        relation: self.catalog.get_by_id(&id).name().to_string(),
                        names,
                    });
                }
            }

            timestamp
        } else {
            self.determine_timestamp(&source.global_uses(), when)?.0
        };

        let source = self.prep_relation_expr(
            source,
            ExprPrepStyle::OneShot {
                logical_time: timestamp,
            },
        )?;

        // If this optimizes to a constant expression, we can immediately return the result.
        let resp = if let MirRelationExpr::Constant { rows, typ: _ } = &*source {
            let rows = match rows {
                Ok(rows) => rows,
                Err(e) => return Err(e.clone().into()),
            };
            let mut results = Vec::new();
            for &(ref row, count) in rows {
                assert!(
                    count >= 0,
                    "Negative multiplicity in constant result: {}",
                    count
                );
                for _ in 0..count {
                    results.push(row.clone());
                }
            }
            finishing.finish(&mut results);
            send_immediate_rows(results)
        } else {
            // Peeks describe a source of data and a timestamp at which to view its contents.
            //
            // We need to determine both an appropriate timestamp from the description, and
            // also to ensure that there is a view in place to query, if the source of data
            // for the peek is not a base relation.

            // Choose a timestamp for all workers to use in the peek.
            // We minimize over all participating views, to ensure that the query will not
            // need to block on the arrival of further input data.
            let (rows_tx, rows_rx) = mpsc::unbounded_channel();

            // Extract any surrounding linear operators to determine if we can simply read
            // out the contents from an existing arrangement.
            let (mut map_filter_project, inner) =
                expr::MapFilterProject::extract_from_expression(&source);
            map_filter_project.optimize();

            // We can use a fast path approach if our query corresponds to a read out of
            // an existing materialization. This is the case if the expression is now a
            // `MirRelationExpr::Get` and its target is something we have materialized.
            // Otherwise, we will need to build a new dataflow.
            let mut fast_path: Option<(_, Option<Row>)> = None;
            if let MirRelationExpr::Get {
                id: Id::Global(id),
                typ: _,
            } = inner
            {
                // Here we should check for an index whose keys are constrained to literal
                // values by predicate constraints in `map_filter_project`. If we find such
                // an index, we can use it with the literal to perform look-ups at workers,
                // and in principle avoid even contacting all but one worker (future work).
                if let Some(indexes) = self.catalog.indexes().get(id) {
                    // Determine for each index identifier, an optional row literal as key.
                    // We want to extract the "best" option, where we prefer indexes with
                    // literals and long keys, then indexes at all, then exit correctly.
                    fast_path = indexes
                        .iter()
                        .map(|(id, exprs)| {
                            let literal_row = map_filter_project.literal_constraints(exprs);
                            // Prefer non-trivial literal rows foremost, then long expressions,
                            // then we don't really care at that point.
                            (literal_row.is_some(), exprs.len(), literal_row, *id)
                        })
                        .max()
                        .map(|(_some, _len, literal, id)| (id, literal));
                }
            }

            // Unpack what we have learned with default values if we found nothing.
            let (fast_path, index_id, literal_row) = if let Some((id, row)) = fast_path {
                (true, id, row)
            } else {
                (false, self.allocate_transient_id()?, None)
            };

            if !fast_path {
                // Slow path. We need to perform some computation, so build
                // a new transient dataflow that will be dropped after the
                // peek completes.
                let typ = source.typ();
                map_filter_project = expr::MapFilterProject::new(typ.arity());
                let key: Vec<_> = (0..typ.arity()).map(MirScalarExpr::Column).collect();
                let view_id = self.allocate_transient_id()?;
                let mut dataflow = DataflowDesc::new(format!("temp-view-{}", view_id));
                dataflow.set_as_of(Antichain::from_elem(timestamp));
                self.dataflow_builder()
                    .import_view_into_dataflow(&view_id, &source, &mut dataflow);
                dataflow.add_index_to_build(index_id, view_id, typ.clone(), key.clone());
                dataflow.add_index_export(index_id, view_id, typ, key);
                self.ship_dataflow(dataflow).await;
            }

            let mfp_plan = map_filter_project
                .into_plan()
                .map_err(|e| crate::error::CoordError::Unstructured(::anyhow::anyhow!(e)))?
                .into_nontemporal()
                .map_err(|_e| {
                    crate::error::CoordError::Unstructured(::anyhow::anyhow!(
                        "Extracted plan has temporal constraints"
                    ))
                })?;

            self.broadcast(SequencedCommand::Peek {
                id: index_id,
                key: literal_row,
                conn_id: session.conn_id(),
                tx: rows_tx,
                timestamp,
                finishing: finishing.clone(),
                map_filter_project: mfp_plan,
            });

            if !fast_path {
                self.drop_indexes(vec![index_id]).await;
            }

            let rows_rx = UnboundedReceiverStream::new(rows_rx)
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
                .map(move |mut resp| {
                    if let PeekResponse::Rows(rows) = &mut resp {
                        finishing.finish(rows)
                    }
                    resp
                });

            ExecuteResponse::SendingRows(Box::pin(rows_rx))
        };

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
            id: source_id,
            with_snapshot,
            ts,
            copy_to,
            emit_progress,
            object_columns,
            desc,
        } = plan;
        // TAIL AS OF, similar to peeks, doesn't need to worry about transaction
        // timestamp semantics.
        if ts.is_none() {
            // If this isn't a TAIL AS OF, the TAIL can be in a transaction if it's the
            // only operation.
            session.add_transaction_ops(TransactionOps::Tail)?;
        }

        // Determine the frontier of updates to tail *from*.
        // Updates greater or equal to this frontier will be produced.
        let frontier = if let Some(ts) = ts {
            // If a timestamp was explicitly requested, use that.
            Antichain::from_elem(
                self.determine_timestamp(&[source_id], PeekWhen::AtTimestamp(ts))?
                    .0,
            )
        } else {
            self.determine_frontier(source_id)
        };
        let sink_name = format!(
            "tail-source-{}",
            self.catalog
                .for_session(session)
                .humanize_id(source_id)
                .expect("Source id is known to exist in catalog")
        );
        let sink_id = self.catalog.allocate_id()?;
        session.add_drop_sink(sink_id);
        let (tx, rx) = mpsc::unbounded_channel();

        let df = self.dataflow_builder().build_sink_dataflow(
            sink_name,
            sink_id,
            source_id,
            SinkConnector::Tail(TailSinkConnector {
                tx,
                emit_progress,
                object_columns,
                value_desc: desc,
            }),
            SinkEnvelope::Tail { emit_progress },
            SinkAsOf {
                frontier,
                strict: !with_snapshot,
            },
        );
        self.ship_dataflow(df).await;

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
        let since = self.indexes.least_valid_since(index_ids.iter().cloned());

        // First determine the candidate timestamp, which is either the explicitly requested
        // timestamp, or the latest timestamp known to be immediately available.
        let timestamp = match when {
            // Explicitly requested timestamps should be respected.
            PeekWhen::AtTimestamp(timestamp) => timestamp,

            // These two strategies vary in terms of which traces drive the
            // timestamp determination process: either the trace itself or the
            // original sources on which they depend.
            PeekWhen::Immediately => {
                if !unmaterialized_source_ids.is_empty() {
                    coord_bail!(
                        "Unable to automatically determine a timestamp for your query; \
                        this can happen if your query depends on non-materialized sources.\n\
                        For more details, see https://materialize.com/s/non-materialized-error"
                    );
                }
                let mut candidate = if uses_ids.iter().any(|id| self.catalog.uses_tables(*id)) {
                    // If the view depends on any tables, we enforce
                    // linearizability by choosing the latest input time.
                    self.get_read_ts()
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
                                .iter()
                                .filter(|id| {
                                    self.indexes
                                        .upper_of(id)
                                        .expect("id not found")
                                        .less_equal(&0)
                                })
                                .collect::<Vec<_>>();
                            coord_bail!(
                                "At least one input has no complete timestamps yet: {:?}",
                                unstarted
                            );
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
            let invalid = index_ids
                .iter()
                .filter(|id| {
                    !self
                        .indexes
                        .since_of(id)
                        .expect("id not found")
                        .less_equal(&timestamp)
                })
                .map(|id| (id, self.indexes.since_of(id)))
                .collect::<Vec<_>>();
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
    fn determine_frontier(&mut self, source_id: GlobalId) -> Antichain<Timestamp> {
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
        let (index_ids, unmaterialized_source_ids) = self.catalog.nearest_indexes(&[source_id]);
        let mut since = self.indexes.least_valid_since(index_ids.iter().copied());
        since.join_assign(
            &self
                .sources
                .least_valid_since(unmaterialized_source_ids.iter().copied()),
        );

        let mut candidate = if index_ids.iter().any(|id| self.catalog.uses_tables(*id)) {
            // If the sink depends on any tables, we enforce linearizability by choosing
            // the latest input time.
            self.get_read_ts()
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
            decorrelated_plan,
            row_set_finishing,
            stage,
            options,
        } = plan;

        let explanation_string = match stage {
            ExplainStage::RawPlan => {
                let catalog = self.catalog.for_session(session);
                let mut explanation = sql::plan::Explanation::new(&raw_plan, &catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types(&BTreeMap::new());
                }
                explanation.to_string()
            }
            ExplainStage::DecorrelatedPlan => {
                let catalog = self.catalog.for_session(session);
                let mut explanation =
                    dataflow_types::Explanation::new(&decorrelated_plan, &catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types();
                }
                explanation.to_string()
            }
            ExplainStage::OptimizedPlan => {
                let optimized_plan =
                    self.prep_relation_expr(decorrelated_plan, ExprPrepStyle::Explain)?;
                let mut dataflow = DataflowDesc::new(format!("explanation"));
                self.dataflow_builder().import_view_into_dataflow(
                    // TODO: If explaining a view, pipe the actual id of the view.
                    &GlobalId::Explain,
                    &optimized_plan,
                    &mut dataflow,
                );
                transform::optimize_dataflow(&mut dataflow);
                let catalog = self.catalog.for_session(session);
                let mut explanation =
                    dataflow_types::Explanation::new_from_dataflow(&dataflow, &catalog);
                if let Some(row_set_finishing) = row_set_finishing {
                    explanation.explain_row_set_finishing(row_set_finishing);
                }
                if options.typed {
                    explanation.explain_types();
                }
                explanation.to_string()
            }
        };
        let rows = vec![Row::pack_slice(&[Datum::from(&*explanation_string)])];
        Ok(send_immediate_rows(rows))
    }

    async fn sequence_send_diffs(
        &mut self,
        session: &mut Session,
        plan: SendDiffsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        session.add_transaction_ops(TransactionOps::Writes(vec![WriteOp {
            id: plan.id,
            rows: plan.updates,
        }]))?;
        Ok(match plan.kind {
            MutationKind::Delete => ExecuteResponse::Deleted(plan.affected_rows),
            MutationKind::Insert => ExecuteResponse::Inserted(plan.affected_rows),
            MutationKind::Update => ExecuteResponse::Updated(plan.affected_rows),
        })
    }

    async fn sequence_insert(
        &mut self,
        session: &mut Session,
        plan: InsertPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        match self
            .prep_relation_expr(plan.values, ExprPrepStyle::Write)?
            .into_inner()
        {
            MirRelationExpr::Constant { rows, typ: _ } => {
                let rows = rows?;
                let desc = self.catalog.get_by_id(&plan.id).desc()?;
                for (row, _) in &rows {
                    for (datum, (name, typ)) in row.unpack().iter().zip(desc.iter()) {
                        if datum == &Datum::Null && !typ.nullable {
                            coord_bail!(
                                "null value in column {} violates not-null constraint",
                                name.unwrap_or(&ColumnName::from("unnamed column"))
                                    .as_str()
                                    .quoted()
                            )
                        }
                    }
                }
                let diffs_plan = SendDiffsPlan {
                    id: plan.id,
                    affected_rows: rows.len(),
                    updates: rows,
                    kind: MutationKind::Insert,
                };
                self.sequence_send_diffs(session, diffs_plan).await
            }
            // If we couldn't optimize the INSERT statement to a constant, it
            // must depend on another relation. We're not yet sophisticated
            // enough to handle this.
            _ => coord_bail!("INSERT statements cannot reference other relations"),
        }
    }

    async fn sequence_copy_rows(
        &mut self,
        session: &mut Session,
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
    ) -> Result<ExecuteResponse, CoordError> {
        let catalog = self.catalog.for_session(session);
        let values = sql::plan::plan_copy_from(&catalog, id, columns, rows)?;
        let plan = InsertPlan {
            id,
            values: values.lower(),
        };
        self.sequence_insert(session, plan).await
    }

    async fn sequence_alter_item_rename(
        &mut self,
        plan: AlterItemRenamePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let op = catalog::Op::RenameItem {
            id: plan.id,
            to_name: plan.to_name,
        };
        match self.catalog_transact(vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(plan.object_type)),
            Err(err) => Err(err),
        }
    }

    fn sequence_alter_index_set_options(
        &mut self,
        plan: AlterIndexSetOptionsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        self.set_index_options(plan.id, plan.options);
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
        self.set_index_options(plan.id, options);
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    async fn catalog_transact(&mut self, ops: Vec<catalog::Op>) -> Result<(), CoordError> {
        let mut sources_to_drop = vec![];
        let mut sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut replication_slots_to_drop: HashMap<String, Vec<String>> = HashMap::new();

        for op in &ops {
            if let catalog::Op::DropItem(id) = op {
                match self.catalog.get_by_id(id).item() {
                    CatalogItem::Table(_) => {
                        sources_to_drop.push(*id);
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

        let builtin_table_updates = self.catalog.transact(ops)?;
        self.send_builtin_table_updates(builtin_table_updates).await;

        if !sources_to_drop.is_empty() {
            for &id in &sources_to_drop {
                if let Some(tables) = &mut self.persisted_tables {
                    tables.destroy(id);
                }
                self.update_timestamper(id, false).await;
                self.catalog.delete_timestamp_bindings(id)?;
                if let Some(cache_tx) = &mut self.cache_tx {
                    cache_tx
                        .send(CacheMessage::DropSource(id))
                        .expect("cache receiver should not drop first");
                }
                self.sources.remove(&id);
            }
            self.broadcast(SequencedCommand::DropSources(sources_to_drop));
        }
        if !sinks_to_drop.is_empty() {
            for id in sinks_to_drop.iter() {
                self.sink_writes.remove(id);
            }
            self.broadcast(SequencedCommand::DropSinks(sinks_to_drop));
        }
        if !indexes_to_drop.is_empty() {
            self.drop_indexes(indexes_to_drop).await;
        }
        for (conn, slot_names) in replication_slots_to_drop {
            postgres_util::drop_replication_slots(&conn, &slot_names).await?;
        }

        Ok(())
    }

    async fn send_builtin_table_updates_at_offset(
        &mut self,
        timestamp_offset: u64,
        mut updates: Vec<BuiltinTableUpdate>,
    ) {
        let timestamp = self.get_write_ts() + timestamp_offset;
        updates.sort_by_key(|u| u.id);
        for (id, updates) in &updates.into_iter().group_by(|u| u.id) {
            self.broadcast(SequencedCommand::Insert {
                id,
                updates: updates
                    .into_iter()
                    .map(|u| Update {
                        row: u.row,
                        diff: u.diff,
                        timestamp,
                    })
                    .collect(),
            })
        }
    }

    async fn send_builtin_table_updates(&mut self, updates: Vec<BuiltinTableUpdate>) {
        self.send_builtin_table_updates_at_offset(0, updates).await
    }

    async fn drop_sinks(&mut self, dataflow_names: Vec<GlobalId>) {
        if !dataflow_names.is_empty() {
            self.broadcast(SequencedCommand::DropSinks(dataflow_names));
        }
    }

    async fn drop_indexes(&mut self, indexes: Vec<GlobalId>) {
        let mut trace_keys = Vec::new();
        for id in indexes {
            if self.indexes.remove(&id).is_some() {
                trace_keys.push(id);
            }
        }
        if !trace_keys.is_empty() {
            self.broadcast(SequencedCommand::DropIndexes(trace_keys))
        }
    }

    fn set_index_options(&mut self, id: GlobalId, options: Vec<IndexOption>) {
        let index = self.indexes.get_mut(&id).expect("index known to exist");
        for o in options {
            match o {
                IndexOption::LogicalCompactionWindow(window) => {
                    let window = window.map(duration_to_timestamp_millis);
                    index.set_compaction_window_ms(window);
                }
            }
        }
    }

    /// Prepares a relation expression for execution by preparing all contained
    /// scalar expressions (see `prep_scalar_expr`), then optimizing the
    /// relation expression.
    fn prep_relation_expr(
        &mut self,
        mut expr: MirRelationExpr,
        style: ExprPrepStyle,
    ) -> Result<OptimizedMirRelationExpr, CoordError> {
        if let ExprPrepStyle::Static = style {
            let mut opt_expr = self.optimizer.optimize(expr, self.catalog.indexes())?;
            opt_expr.0.try_visit_mut(&mut |e| {
                // Carefully test filter expressions, which may represent temporal filters.
                if let expr::MirRelationExpr::Filter { input, predicates } = &*e {
                    let mfp = expr::MapFilterProject::new(input.arity())
                        .filter(predicates.iter().cloned());
                    match mfp.into_plan() {
                        Err(e) => coord_bail!("{:?}", e),
                        Ok(_) => Ok(()),
                    }
                } else {
                    e.try_visit_scalars_mut1(&mut |s| Self::prep_scalar_expr(s, style))
                }
            })?;
            Ok(opt_expr)
        } else {
            expr.try_visit_scalars_mut(&mut |s| Self::prep_scalar_expr(s, style))?;
            // TODO (wangandi): Is there anything that optimizes to a
            // constant expression that originally contains a global get? Is
            // there anything not containing a global get that cannot be
            // optimized to a constant expression?
            Ok(self.optimizer.optimize(expr, self.catalog.indexes())?)
        }
    }

    /// Prepares a scalar expression for execution by replacing any placeholders
    /// with their correct values.
    ///
    /// Specifically, calls to the special function `MzLogicalTimestamp` are
    /// replaced if `style` is `OneShot { logical_timestamp }`. Calls are not
    /// replaced for the `Explain` style nor for `Static` which should not
    /// reach this point if we have correctly validated the use of placeholders.
    fn prep_scalar_expr(expr: &mut MirScalarExpr, style: ExprPrepStyle) -> Result<(), CoordError> {
        // Replace calls to `MzLogicalTimestamp` as described above.
        let mut observes_ts = false;
        expr.visit_mut(&mut |e| {
            if let MirScalarExpr::CallNullary(f @ NullaryFunc::MzLogicalTimestamp) = e {
                observes_ts = true;
                if let ExprPrepStyle::OneShot { logical_time } = style {
                    *e = MirScalarExpr::literal_ok(
                        Datum::from(i128::from(logical_time)),
                        f.output_type().scalar_type,
                    );
                }
            }
        });
        if observes_ts && matches!(style, ExprPrepStyle::Static | ExprPrepStyle::Write) {
            coord_bail!("mz_logical_timestamp cannot be used in static or write queries");
        }
        Ok(())
    }

    /// Finalizes a dataflow and then broadcasts it to all workers.
    /// Utility method for the more general [Self::ship_dataflows]
    async fn ship_dataflow(&mut self, dataflow: DataflowDesc) {
        self.ship_dataflows(vec![dataflow]).await
    }

    /// Finalizes a list of dataflows and then broadcasts it to all workers.
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
    async fn ship_dataflows(&mut self, mut dataflows: Vec<DataflowDesc>) {
        // This function must succeed because catalog_transact has generally been run
        // before calling this function. We don't have plumbing yet to rollback catalog
        // operations if this function fails, and materialized will be in an unsafe
        // state if we do not correctly clean up the catalog.

        for dataflow in &mut dataflows {
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

            // For each produced arrangement, start tracking the arrangement with
            // a compaction frontier of at least `since`.
            for (global_id, _description, _typ) in dataflow.index_exports.iter() {
                let frontiers = self.new_frontiers(
                    *global_id,
                    since.elements().to_vec(),
                    self.logical_compaction_window_ms,
                );
                self.indexes.insert(*global_id, frontiers);
            }

            // TODO: Produce "valid from" information for each sink.
            // For each sink, ... do nothing because we don't yield `since` for sinks.
            // for (global_id, _description) in dataflow.sink_exports.iter() {
            //     // TODO: assign `since` to a "valid from" element of the sink. E.g.
            //     self.sink_info[global_id].valid_from(&since);
            // }

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

            // Optimize the dataflow across views, and any other ways that appeal.
            transform::optimize_dataflow(dataflow);
        }

        // Finalize the dataflow by broadcasting its construction to all workers.
        self.broadcast(SequencedCommand::CreateDataflows(dataflows));
    }

    fn broadcast(&self, cmd: SequencedCommand) {
        for tx in &self.worker_txs {
            tx.send(cmd.clone())
                .expect("worker command receiver should not drop first")
        }
        for handle in self.worker_guards.guards() {
            handle.thread().unpark()
        }
    }

    // Notify the timestamper thread that a source has been created or dropped.
    async fn update_timestamper(&mut self, source_id: GlobalId, create: bool) {
        if create {
            let bindings = self
                .catalog
                .load_timestamp_bindings(source_id)
                .expect("loading timestamps from coordinator cannot fail");
            if let Some(entry) = self.catalog.try_get_by_id(source_id) {
                if let CatalogItem::Source(s) = entry.item() {
                    self.ts_tx
                        .send(TimestampMessage::Add(source_id, s.connector.clone()))
                        .expect("Failed to send CREATE Instance notice to timestamper");
                    self.broadcast(SequencedCommand::AddSourceTimestamping {
                        id: source_id,
                        connector: s.connector.clone(),
                        bindings,
                    });
                }
            }
        } else {
            self.ts_tx
                .send(TimestampMessage::Drop(source_id))
                .expect("Failed to send DROP Instance notice to timestamper");
            self.broadcast(SequencedCommand::DropSourceTimestamping { id: source_id });
        }
    }

    // Tell the cacher to start caching data for `id` if that source
    // has caching enabled and Materialize has caching enabled.
    // This function is a no-op if the cacher has already started caching
    // this source.
    async fn maybe_begin_caching(&mut self, id: GlobalId, connector_caching_enabled: bool) {
        if connector_caching_enabled {
            if let Some(cache_tx) = &mut self.cache_tx {
                cache_tx
                    .send(CacheMessage::AddSource(
                        self.catalog.config().cluster_id,
                        id,
                    ))
                    .expect("caching receiver should not drop first");
            } else {
                log::error!(
                    "trying to create a cached source ({}) but caching is disabled.",
                    id
                );
            }
        }
    }

    fn allocate_transient_id(&mut self) -> Result<GlobalId, CoordError> {
        let id = self.transient_id_counter;
        if id == u64::max_value() {
            coord_bail!("id counter overflows i64");
        }
        self.transient_id_counter += 1;
        Ok(GlobalId::Transient(id))
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
        workers,
        timely_worker,
        symbiosis_url,
        logging,
        data_directory,
        timestamp_frequency,
        cache: cache_config,
        persistence: persistence_config,
        logical_compaction_window,
        experimental_mode,
        safe_mode,
        build_info,
    }: Config<'_>,
) -> Result<(Handle, Client), CoordError> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (feedback_tx, feedback_rx) = mpsc::unbounded_channel();
    let cache_tx = if let Some(cache_config) = &cache_config {
        let (cache_tx, cache_rx) = mpsc::unbounded_channel();
        let mut cacher = Cacher::new(cache_rx, cache_config.clone());
        tokio::spawn(async move { cacher.run().await });
        Some(cache_tx)
    } else {
        None
    };

    let persisted_tables = if let Some(persistence_config) = &persistence_config {
        PersistentTables::new(persistence_config)
    } else {
        None
    };
    let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();

    let symbiosis = if let Some(symbiosis_url) = symbiosis_url {
        Some(symbiosis::Postgres::open_and_erase(symbiosis_url).await?)
    } else {
        None
    };

    let path = data_directory.join("catalog");
    let (catalog, builtin_table_updates) = Catalog::open(&catalog::Config {
        path: &path,
        experimental_mode: Some(experimental_mode),
        safe_mode,
        enable_logging: logging.is_some(),
        cache_directory: cache_config.map(|c| c.path),
        build_info,
        num_workers: workers,
        timestamp_frequency,
    })?;
    let cluster_id = catalog.config().cluster_id;
    let session_id = catalog.config().session_id;
    let start_instant = catalog.config().start_instant;

    let (worker_txs, worker_rxs): (Vec<_>, Vec<_>) =
        (0..workers).map(|_| crossbeam_channel::unbounded()).unzip();
    let worker_guards = dataflow::serve(dataflow::Config {
        command_receivers: worker_rxs,
        timely_worker,
        experimental_mode,
    })
    .map_err(|s| CoordError::Unstructured(anyhow!("{}", s)))?;

    let (metric_scraper_handle, metric_scraper_tx) = if let Some(LoggingConfig {
        granularity,
        retain_readings_for,
        ..
    }) = logging
    {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut scraper = Scraper::new(
            granularity,
            retain_readings_for,
            ::prometheus::default_registry(),
            rx,
            internal_cmd_tx.clone(),
        );
        let executor = TokioHandle::current();
        let scraper_thread_handle = thread::Builder::new()
            .name("scraper".to_string())
            .spawn(move || {
                let _executor_guard = executor.enter();
                scraper.run();
            })
            .unwrap()
            .join_on_drop();
        (Some(scraper_thread_handle), Some(tx))
    } else {
        (None, None)
    };

    // Spawn timestamper after any fallible operations so that if bootstrap fails we still
    // tell it to shut down.
    let (ts_tx, ts_rx) = std::sync::mpsc::channel();
    let mut timestamper =
        Timestamper::new(Duration::from_millis(10), internal_cmd_tx.clone(), ts_rx);
    let executor = TokioHandle::current();
    let timestamper_thread_handle = thread::Builder::new()
        .name("timestamper".to_string())
        .spawn(move || {
            let _executor_guard = executor.enter();
            timestamper.run();
        })
        .unwrap()
        .join_on_drop();

    // In order for the coordinator to support Rc and Refcell types, it cannot be
    // sent across threads. Spawn it in a thread and have this parent thread wait
    // for bootstrap completion before proceeding.
    let (bootstrap_tx, bootstrap_rx) = std::sync::mpsc::channel();
    let handle = TokioHandle::current();
    let thread = thread::Builder::new()
        .name("coordinator".to_string())
        .spawn(move || {
            let mut coord = Coordinator {
                worker_guards,
                worker_txs,
                optimizer: Default::default(),
                catalog,
                symbiosis,
                indexes: ArrangementFrontiers::default(),
                sources: ArrangementFrontiers::default(),
                logging_granularity: logging
                    .as_ref()
                    .and_then(|c| c.granularity.as_millis().try_into().ok()),
                logical_compaction_window_ms: logical_compaction_window
                    .map(duration_to_timestamp_millis),
                internal_cmd_tx,
                ts_tx: ts_tx.clone(),
                metric_scraper_tx: metric_scraper_tx.clone(),
                cache_tx,
                closed_up_to: 1,
                read_lower_bound: 1,
                last_op_was_read: false,
                need_advance: true,
                transient_id_counter: 1,
                active_conns: HashMap::new(),
                persisted_tables,
                txn_reads: HashMap::new(),
                since_handles: HashMap::new(),
                since_updates: Rc::new(RefCell::new(HashMap::new())),
                sink_writes: HashMap::new(),
            };
            coord.broadcast(SequencedCommand::EnableFeedback(feedback_tx));
            if let Some(config) = &logging {
                coord.broadcast(SequencedCommand::EnableLogging(DataflowLoggingConfig {
                    granularity_ns: config.granularity.as_nanos(),
                    active_logs: BUILTINS
                        .logs()
                        .map(|src| (src.variant.clone(), src.index_id))
                        .collect(),
                    log_logging: config.log_logging,
                }));
            }
            if let Some(cache_tx) = &coord.cache_tx {
                coord.broadcast(SequencedCommand::EnableCaching(cache_tx.clone()));
            }
            let bootstrap = handle.block_on(coord.bootstrap(builtin_table_updates));
            let ok = bootstrap.is_ok();
            bootstrap_tx.send(bootstrap).unwrap();
            if !ok {
                metric_scraper_tx.map(|tx| tx.send(ScraperMessage::Shutdown).unwrap());
                // Tell the timestamper thread to shut down.
                ts_tx.send(TimestampMessage::Shutdown).unwrap();
                // Explicitly drop the timestamper handle here so we can wait for
                // the thread to return.
                drop(timestamper_thread_handle);
                coord.broadcast(SequencedCommand::Shutdown);
                return;
            }
            handle.block_on(coord.serve(
                internal_cmd_rx,
                cmd_rx,
                feedback_rx,
                timestamper_thread_handle,
                metric_scraper_handle,
            ))
        })
        .unwrap();
    match bootstrap_rx.recv().unwrap() {
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

/// The styles in which an expression can be prepared.
#[derive(Clone, Copy, Debug)]
enum ExprPrepStyle {
    /// The expression is being prepared for output as part of an `EXPLAIN`
    /// query.
    Explain,
    /// The expression is being prepared for installation in a static context,
    /// like in a view.
    Static,
    /// The expression is being prepared to run once at the specified logical
    /// time.
    OneShot { logical_time: u64 },
    /// The expression is being prepared to run in an INSERT or other write.
    Write,
}

/// Constructs an [`ExecuteResponse`] that that will send some rows to the
/// client immediately, as opposed to asking the dataflow layer to send along
/// the rows after some computation.
fn send_immediate_rows(rows: Vec<Row>) -> ExecuteResponse {
    ExecuteResponse::SendingRows(Box::pin(async { PeekResponse::Rows(rows) }))
}

fn auto_generate_primary_idx(
    index_name: String,
    on_name: FullName,
    on_id: GlobalId,
    on_desc: &RelationDesc,
    conn_id: Option<u32>,
    depends_on: Vec<GlobalId>,
) -> catalog::Index {
    let default_key = on_desc.typ().default_key();

    catalog::Index {
        create_sql: index_sql(index_name, on_name, &on_desc, &default_key),
        plan_cx: PlanContext::default(),
        on: on_id,
        keys: default_key
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect(),
        conn_id,
        depends_on,
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
    use sql::ast::{Expr, Value};

    CreateIndexStatement::<Raw> {
        name: Some(Ident::new(index_name)),
        on_name: sql::normalize::unresolve(view_name),
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
    catalog: &dyn sql::catalog::Catalog,
    stmt: Statement<Raw>,
    param_types: &[Option<pgrepr::Type>],
    session: Option<&Session>,
) -> Result<StatementDesc, CoordError> {
    match stmt {
        // FETCH's description depends on the current session, which describe_statement
        // doesn't (and shouldn't?) have access to, so intercept it here.
        Statement::Fetch(FetchStatement { ref name, .. }) => {
            match session
                .map(|session| session.get_portal(name.as_str()).map(|p| p.desc.clone()))
                .flatten()
            {
                Some(desc) => Ok(desc),
                None => Err(CoordError::UnknownCursor(name.to_string())),
            }
        }
        _ => Ok(sql::plan::describe(catalog, stmt, param_types)?),
    }
}

fn check_statement_safety(stmt: &Statement<Raw>) -> Result<(), CoordError> {
    let (ty, connector, with_options) = match stmt {
        Statement::CreateSource(CreateSourceStatement {
            connector,
            with_options,
            ..
        }) => ("source", connector, with_options),
        Statement::CreateSink(CreateSinkStatement {
            connector,
            with_options,
            ..
        }) => ("sink", connector, with_options),
        _ => return Ok(()),
    };
    match connector {
        // File sources and sinks are prohibited in safe mode because they allow
        // reading ƒrom and writing to arbitrary files on disk.
        Connector::File { .. } => {
            return Err(CoordError::SafeModeViolation(format!("file {}", ty)));
        }
        Connector::AvroOcf { .. } => {
            return Err(CoordError::SafeModeViolation(format!("Avro OCF {}", ty)));
        }
        // Kerberos-authenticated Kafka sources and sinks are prohibited in
        // safe mode because librdkafka will blindly execute the string passed
        // as `sasl_kerberos_kinit_cmd`.
        Connector::Kafka { .. } => {
            // It's too bad that we have to reinvent so much of librdkafka's
            // option parsing and hardcode some of its defaults here. But there
            // isn't an obvious alternative; asking librdkafka about its =
            // defaults requires constructing a librdkafka client, and at that
            // point it's already too late.
            let mut with_options = sql::normalize::options(with_options);
            let with_options = sql::kafka_util::extract_config(&mut with_options)?;
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
                    ty,
                )));
            }
        }
        _ => (),
    }
    Ok(())
}
