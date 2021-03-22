// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordination of installed views, available timestamps, and compacted timestamps.
//!
//! The command coordinator maintains a view of the installed views, and for each tracks
//! the frontier of available times (`upper`) and the frontier of compacted times (`since`).
//! The upper frontier describes times that may not return immediately, as any timestamps in
//! advance of the frontier are still open. The since frontier constrains those times for
//! which the maintained view will be correct, as any timestamps in advance of the frontier
//! must accumulate to the same value as would an un-compacted trace.

use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::iter;
use std::mem;
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use self::prometheus::{Scraper, ScraperMessage};
use anyhow::{anyhow, Context};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::future::{self, FutureExt, TryFutureExt};
use futures::stream::{self, StreamExt};
use rand::Rng;
use timely::communication::WorkerGuards;
use timely::progress::{Antichain, ChangeBatch, Timestamp as _};
use tokio::runtime::{Handle as TokioHandle, Runtime};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;

use build_info::BuildInfo;
use dataflow::{CacheMessage, SequencedCommand, WorkerFeedback, WorkerFeedbackWithMeta};
use dataflow_types::logging::LoggingConfig as DataflowLoggingConfig;
use dataflow_types::{
    AvroOcfSinkConnector, DataflowDesc, IndexDesc, KafkaSinkConnector, PeekResponse, SinkConnector,
    SourceConnector, TailSinkConnector, TimestampSourceUpdate, Update,
};
use dataflow_types::{SinkAsOf, SinkEnvelope};
use expr::{
    ExprHumanizer, GlobalId, Id, MirRelationExpr, MirScalarExpr, NullaryFunc,
    OptimizedMirRelationExpr, RowSetFinishing,
};
use ore::collections::CollectionExt;
use ore::str::StrExt;
use ore::thread::{JoinHandleExt, JoinOnDropHandle};
use repr::adt::array::ArrayDimension;
use repr::{ColumnName, Datum, RelationDesc, RelationType, Row, Timestamp};
use sql::ast::display::AstDisplay;
use sql::ast::{
    CreateIndexStatement, CreateTableStatement, DropObjectsStatement, ExplainOptions, ExplainStage,
    FetchStatement, Ident, ObjectType, Raw, Statement,
};
use sql::catalog::{Catalog as _, CatalogError};
use sql::names::{DatabaseSpecifier, FullName, SchemaName};
use sql::plan::StatementDesc;
use sql::plan::{
    CopyFormat, IndexOption, IndexOptionName, MutationKind, Params, PeekWhen, Plan, PlanContext,
};
use storage::Message as PersistedMessage;
use transform::Optimizer;

use self::arrangement_state::{ArrangementFrontiers, Frontiers};
use crate::cache::{CacheConfig, Cacher};
use crate::catalog::builtin::{
    BUILTINS, MZ_ARRAY_TYPES, MZ_AVRO_OCF_SINKS, MZ_BASE_TYPES, MZ_COLUMNS, MZ_DATABASES,
    MZ_FUNCTIONS, MZ_INDEXES, MZ_INDEX_COLUMNS, MZ_KAFKA_SINKS, MZ_LIST_TYPES, MZ_MAP_TYPES,
    MZ_PSEUDO_TYPES, MZ_ROLES, MZ_SCHEMAS, MZ_SINKS, MZ_SOURCES, MZ_TABLES, MZ_TYPES, MZ_VIEWS,
    MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS,
};
use crate::catalog::{
    self, Catalog, CatalogItem, Func, Index, SinkConnectorState, Type, TypeInner,
};
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
mod metrics;
mod prometheus;

#[derive(Debug)]
pub enum Message {
    Command(Command),
    Worker(WorkerFeedbackWithMeta),
    AdvanceSourceTimestamp(AdvanceSourceTimestamp),
    StatementReady(StatementReady),
    SinkConnectorReady(SinkConnectorReady),
    Broadcast(SequencedCommand),
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
    pub build_info: &'static BuildInfo,
}

/// Glues the external world to the Timely workers.
pub struct Coordinator {
    worker_guards: WorkerGuards<()>,
    worker_txs: Vec<crossbeam_channel::Sender<SequencedCommand>>,
    optimizer: Optimizer,
    catalog: Catalog,
    symbiosis: Option<symbiosis::Postgres>,
    /// Maps (global Id of arrangement) -> (frontier information)
    indexes: ArrangementFrontiers<Timestamp>,
    since_updates: Vec<(GlobalId, Antichain<Timestamp>)>,
    /// Delta from leading edge of an arrangement from which we allow compaction.
    logical_compaction_window_ms: Option<Timestamp>,
    /// Instance count: number of times sources have been instantiated in views. This is used
    /// to associate each new instance of a source with a unique instance id (iid)
    logging_granularity: Option<u64>,
    // Channel to manange internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    // Channel to communicate source status updates to the timestamper thread.
    ts_tx: std::sync::mpsc::Sender<TimestampMessage>,
    // Channel to communicate source status updates and shutdown notifications to the cacher
    // thread.
    cache_tx: Option<mpsc::UnboundedSender<CacheMessage>>,
    /// The last timestamp we assigned to a read.
    read_lower_bound: Timestamp,
    /// The timestamp that all local inputs have been advanced up to.
    closed_up_to: Timestamp,
    /// Whether or not the most recent operation was a read.
    last_op_was_read: bool,
    /// Whether we need to advance local inputs (i.e., did someone observe a timestamp).
    /// TODO(justin): this is a hack, and does not work right with TAIL.
    need_advance: bool,
    transient_id_counter: u64,
    /// A map from connection ID to metadata about that connection for all
    // active connections.
    active_conns: HashMap<u32, ConnMeta>,
    /// Map of all persisted tables.
    persisted_tables: Option<PersistentTables>,
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

    /// Initializes coordinator state based on the contained catalog. Must be
    /// called after creating the coordinator and before calling the
    /// `Coordinator::serve` method.
    async fn bootstrap(&mut self, events: Vec<catalog::Event>) -> Result<(), CoordError> {
        let items: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                catalog::Event::CreatedItem {
                    id,
                    oid,
                    name,
                    item,
                    ..
                } => Some((id, oid, name, item)),
                _ => None,
            })
            .collect();

        // Sources and indexes may be depended upon by other catalog items,
        // insert them first.
        for &(id, _, _, item) in &items {
            match item {
                //currently catalog item rebuild assumes that sinks and
                //indexes are always built individually and does not store information
                //about how it was built. If we start building multiple sinks and/or indexes
                //using a single dataflow, we have to make sure the rebuild process re-runs
                //the same multiple-build dataflow.
                CatalogItem::Source(source) => {
                    // Inform the timestamper about this source.
                    self.update_timestamper(*id, true).await;
                    self.maybe_begin_caching(*id, &source.connector).await;
                }
                CatalogItem::Index(_) => {
                    if BUILTINS.logs().any(|log| log.index_id == *id) {
                        // Indexes on logging views are special, as they are
                        // already installed in the dataflow plane via
                        // `SequencedCommand::EnableLogging`. Just teach the
                        // coordinator of their existence, without creating a
                        // dataflow for the index.
                        //
                        // TODO(benesch): why is this hardcoded to 1000?
                        // Should it not be the same logical compaction window
                        // that everything else uses?
                        self.indexes
                            .insert(*id, Frontiers::new(self.num_workers(), Some(1_000)));
                    } else {
                        let df = self.dataflow_builder().build_index_dataflow(*id);
                        self.ship_dataflow(df).await?;
                    }
                }
                _ => (), // Handled in next loop.
            }
        }

        for &(id, oid, name, item) in &items {
            match item {
                CatalogItem::View(_) => (),
                CatalogItem::Table(_) => {
                    // TODO gross hack for now
                    if !id.is_system() {
                        if let Some(tables) = &mut self.persisted_tables {
                            if let Some(messages) = tables.resume(*id) {
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
                                                    id: *id,
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
                    let connector = sink_connector::build(builder.clone(), *id)
                        .await
                        .with_context(|| format!("recreating sink {}", name))?;
                    self.handle_sink_connector_ready(*id, *oid, connector)
                        .await?;
                }
                _ => (), // Handled in prior loop.
            }
        }

        self.process_catalog_events(events).await?;

        // Announce primary and foreign key relationships.
        if self.logging_granularity.is_some() {
            for log in BUILTINS.logs() {
                let log_id = &log.id.to_string();
                self.update_catalog_view(
                    MZ_VIEW_KEYS.id,
                    log.variant.desc().typ().keys.iter().enumerate().flat_map(
                        move |(index, key)| {
                            key.iter().map(move |k| {
                                let row = Row::pack_slice(&[
                                    Datum::String(log_id),
                                    Datum::Int64(*k as i64),
                                    Datum::Int64(index as i64),
                                ]);
                                (row, 1)
                            })
                        },
                    ),
                )
                .await;

                self.update_catalog_view(
                    MZ_VIEW_FOREIGN_KEYS.id,
                    log.variant.foreign_keys().into_iter().enumerate().flat_map(
                        move |(index, (parent, pairs))| {
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
                                (row, 1)
                            })
                        },
                    ),
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
                Message::Broadcast(cmd) => self.broadcast(cmd),
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
        }
    }

    async fn message_statement_ready(
        &mut self,
        StatementReady {
            session,
            tx,
            result,
            params,
        }: StatementReady,
    ) {
        match future::ready(result)
            .and_then(|stmt| self.handle_statement(&session, stmt, &params))
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
                mut session,
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
                                | Statement::Declare(_)
                                | Statement::Discard(_)
                                | Statement::Explain(_)
                                | Statement::Fetch(_)
                                | Statement::Rollback(_)
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
                                | Statement::StartTransaction(_) => {
                                    // Always safe.
                                }

                                Statement::Copy(_) | Statement::Select(_) | Statement::Tail(_) => {
                                    if let Err(e) =
                                        session.add_transaction_ops(TransactionOps::Reads)
                                    {
                                        let _ = tx.send(Response {
                                            result: Err(e),
                                            session,
                                        });
                                        return;
                                    }
                                }

                                Statement::Insert(_) => {
                                    // Insert will add the actual operations later. We can still do a check to
                                    // early exit here before processing it.
                                    if let Err(e) =
                                        session.add_transaction_ops(TransactionOps::Writes(vec![]))
                                    {
                                        let _ = tx.send(Response {
                                            result: Err(e),
                                            session,
                                        });
                                        return;
                                    }
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

                        let internal_cmd_tx = self.internal_cmd_tx.clone();
                        tokio::spawn(async move {
                            let result = sql::pure::purify(stmt).await.map_err(|e| e.into());
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
                        let mut compaction_frontier = Antichain::new();
                        for time in index_state.upper.frontier().iter() {
                            compaction_frontier.insert(
                                compaction_window_ms
                                    * (time.saturating_sub(compaction_window_ms)
                                        / compaction_window_ms),
                            );
                        }
                        if index_state.since != compaction_frontier {
                            index_state.advance_since(&compaction_frontier);
                            self.since_updates
                                .push((name.clone(), index_state.since.clone()));
                        }
                    }
                }
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
        self.since_updates
            .retain(|(_, frontier)| frontier != &Antichain::new());
        if !self.since_updates.is_empty() {
            let since_updates = mem::take(&mut self.since_updates);
            if let Some(tables) = &mut self.persisted_tables {
                tables.allow_compaction(&since_updates);
            }
            self.broadcast(SequencedCommand::AllowCompaction(since_updates));
        }
    }

    async fn handle_statement(
        &mut self,
        session: &Session,
        stmt: sql::ast::Statement<Raw>,
        params: &sql::plan::Params,
    ) -> Result<(PlanContext, sql::plan::Plan), CoordError> {
        let pcx = PlanContext::default();

        // When symbiosis mode is enabled, use symbiosis planning for:
        //  - CREATE TABLE
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
    // This cleans up any state in the coordinator associated with the session.
    async fn handle_terminate(&mut self, session: &mut Session) {
        let (drop_sinks, _) = session.clear_transaction();
        self.drop_sinks(drop_sinks).await;

        self.drop_temp_items(session.conn_id()).await;
        self.catalog
            .drop_temporary_schema(session.conn_id())
            .expect("unable to drop temporary schema");
        self.active_conns.remove(&session.conn_id());
    }

    // Removes all temporary items created by the specified connection, though
    // not the temporary schema itself.
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
            connector,
            sink.envelope,
            as_of,
        );
        self.ship_dataflow(df).await
    }

    /// Insert a single row into a given catalog view.
    async fn update_catalog_view<I>(&mut self, index_id: GlobalId, updates: I)
    where
        I: IntoIterator<Item = (Row, isize)>,
    {
        let timestamp = self.get_write_ts();
        let updates = updates
            .into_iter()
            .map(|(row, diff)| Update {
                row,
                diff,
                timestamp,
            })
            .collect();
        self.broadcast(SequencedCommand::Insert {
            id: index_id,
            updates,
        });
    }

    async fn report_database_update(
        &mut self,
        database_id: i64,
        oid: u32,
        name: &str,
        diff: isize,
    ) {
        self.update_catalog_view(
            MZ_DATABASES.id,
            iter::once((
                Row::pack_slice(&[
                    Datum::Int64(database_id),
                    Datum::Int32(oid as i32),
                    Datum::String(&name),
                ]),
                diff,
            )),
        )
        .await
    }

    async fn report_schema_update(
        &mut self,
        schema_id: i64,
        oid: u32,
        database_id: Option<i64>,
        schema_name: &str,
        diff: isize,
    ) {
        self.update_catalog_view(
            MZ_SCHEMAS.id,
            iter::once((
                Row::pack_slice(&[
                    Datum::Int64(schema_id),
                    Datum::Int32(oid as i32),
                    match database_id {
                        None => Datum::Null,
                        Some(database_id) => Datum::Int64(database_id),
                    },
                    Datum::String(schema_name),
                ]),
                diff,
            )),
        )
        .await
    }

    async fn report_role_update(&mut self, role_id: i64, oid: u32, name: &str, diff: isize) {
        self.update_catalog_view(
            MZ_ROLES.id,
            iter::once((
                Row::pack_slice(&[
                    Datum::Int64(role_id),
                    Datum::Int32(oid as i32),
                    Datum::String(&name),
                ]),
                diff,
            )),
        )
        .await
    }

    async fn report_column_updates(
        &mut self,
        desc: &RelationDesc,
        global_id: GlobalId,
        diff: isize,
    ) {
        for (i, (column_name, column_type)) in desc.iter().enumerate() {
            self.update_catalog_view(
                MZ_COLUMNS.id,
                iter::once((
                    Row::pack_slice(&[
                        Datum::String(&global_id.to_string()),
                        Datum::String(
                            &column_name
                                .map(|n| n.to_string())
                                .unwrap_or_else(|| "?column?".to_owned()),
                        ),
                        Datum::Int64(i as i64 + 1),
                        Datum::from(column_type.nullable),
                        Datum::String(pgrepr::Type::from(&column_type.scalar_type).name()),
                    ]),
                    diff,
                )),
            )
            .await
        }
    }

    async fn report_index_update(
        &mut self,
        global_id: GlobalId,
        oid: u32,
        index: &Index,
        name: &str,
        diff: isize,
    ) {
        self.report_index_update_inner(
            global_id,
            oid,
            index,
            name,
            index
                .keys
                .iter()
                .map(|key| {
                    key.typ(self.catalog.get_by_id(&index.on).desc().unwrap().typ())
                        .nullable
                })
                .collect(),
            diff,
        )
        .await
    }

    // When updating the mz_indexes system table after dropping an index, it may no longer be possible
    // to generate the 'nullable' information for that index. This function allows callers to bypass
    // that computation and provide their own value, instead.
    async fn report_index_update_inner(
        &mut self,
        global_id: GlobalId,
        oid: u32,
        index: &Index,
        name: &str,
        nullable: Vec<bool>,
        diff: isize,
    ) {
        let key_sqls = match sql::parse::parse(&index.create_sql)
            .expect("create_sql cannot be invalid")
            .into_element()
        {
            Statement::CreateIndex(CreateIndexStatement { key_parts, .. }) => key_parts.unwrap(),
            _ => unreachable!(),
        };
        self.update_catalog_view(
            MZ_INDEXES.id,
            iter::once((
                Row::pack_slice(&[
                    Datum::String(&global_id.to_string()),
                    Datum::Int32(oid as i32),
                    Datum::String(name),
                    Datum::String(&index.on.to_string()),
                ]),
                diff,
            )),
        )
        .await;

        for (i, key) in index.keys.iter().enumerate() {
            let nullable = *nullable
                .get(i)
                .expect("missing nullability information for index key");
            let seq_in_index = i64::try_from(i + 1).expect("invalid index sequence number");
            let key_sql = key_sqls
                .get(i)
                .expect("missing sql information for index key")
                .to_string();
            let (field_number, expression) = match key {
                MirScalarExpr::Column(col) => (
                    Datum::Int64(i64::try_from(*col + 1).expect("invalid index column number")),
                    Datum::Null,
                ),
                _ => (Datum::Null, Datum::String(&key_sql)),
            };
            self.update_catalog_view(
                MZ_INDEX_COLUMNS.id,
                iter::once((
                    Row::pack_slice(&[
                        Datum::String(&global_id.to_string()),
                        Datum::Int64(seq_in_index),
                        field_number,
                        expression,
                        Datum::from(nullable),
                    ]),
                    diff,
                )),
            )
            .await
        }
    }

    async fn report_table_update(
        &mut self,
        global_id: GlobalId,
        oid: u32,
        schema_id: i64,
        name: &str,
        diff: isize,
    ) {
        self.update_catalog_view(
            MZ_TABLES.id,
            iter::once((
                Row::pack_slice(&[
                    Datum::String(&global_id.to_string()),
                    Datum::Int32(oid as i32),
                    Datum::Int64(schema_id),
                    Datum::String(name),
                ]),
                diff,
            )),
        )
        .await
    }

    async fn report_source_update(
        &mut self,
        global_id: GlobalId,
        oid: u32,
        schema_id: i64,
        name: &str,
        diff: isize,
    ) {
        self.update_catalog_view(
            MZ_SOURCES.id,
            iter::once((
                Row::pack_slice(&[
                    Datum::String(&global_id.to_string()),
                    Datum::Int32(oid as i32),
                    Datum::Int64(schema_id),
                    Datum::String(name),
                ]),
                diff,
            )),
        )
        .await
    }

    async fn report_view_update(
        &mut self,
        global_id: GlobalId,
        oid: u32,
        schema_id: i64,
        name: &str,
        diff: isize,
    ) {
        self.update_catalog_view(
            MZ_VIEWS.id,
            iter::once((
                Row::pack_slice(&[
                    Datum::String(&global_id.to_string()),
                    Datum::Int32(oid as i32),
                    Datum::Int64(schema_id),
                    Datum::String(name),
                ]),
                diff,
            )),
        )
        .await
    }

    async fn report_sink_update(
        &mut self,
        global_id: GlobalId,
        oid: u32,
        schema_id: i64,
        name: &str,
        diff: isize,
    ) {
        self.update_catalog_view(
            MZ_SINKS.id,
            iter::once((
                Row::pack_slice(&[
                    Datum::String(&global_id.to_string()),
                    Datum::Int32(oid as i32),
                    Datum::Int64(schema_id),
                    Datum::String(name),
                ]),
                diff,
            )),
        )
        .await
    }

    async fn report_type_update(
        &mut self,
        id: GlobalId,
        oid: u32,
        schema_id: i64,
        name: &str,
        typ: &Type,
        diff: isize,
    ) {
        self.update_catalog_view(
            MZ_TYPES.id,
            iter::once((
                Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::Int32(oid as i32),
                    Datum::Int64(schema_id),
                    Datum::String(name),
                ]),
                diff,
            )),
        )
        .await;

        let (index_id, update) = match typ.inner {
            TypeInner::Array { element_id } => (
                MZ_ARRAY_TYPES.id,
                vec![id.to_string(), element_id.to_string()],
            ),
            TypeInner::Base => (MZ_BASE_TYPES.id, vec![id.to_string()]),
            TypeInner::List { element_id } => (
                MZ_LIST_TYPES.id,
                vec![id.to_string(), element_id.to_string()],
            ),
            TypeInner::Map { key_id, value_id } => (
                MZ_MAP_TYPES.id,
                vec![id.to_string(), key_id.to_string(), value_id.to_string()],
            ),
            TypeInner::Pseudo => (MZ_PSEUDO_TYPES.id, vec![id.to_string()]),
        };
        self.update_catalog_view(
            index_id,
            iter::once((
                Row::pack_slice(&update.iter().map(|c| Datum::String(c)).collect::<Vec<_>>()[..]),
                diff,
            )),
        )
        .await
    }

    async fn report_func_update(
        &mut self,
        id: GlobalId,
        schema_id: i64,
        name: &str,
        func: &Func,
        diff: isize,
    ) {
        for func_impl_details in func.inner.func_impls() {
            let arg_ids = func_impl_details
                .arg_oids
                .iter()
                .map(|oid| self.catalog.get_by_oid(oid).id().to_string())
                .collect::<Vec<_>>();
            let mut row = Row::default();
            row.push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: arg_ids.len(),
                }],
                arg_ids.iter().map(|id| Datum::String(&id)),
            )
            .unwrap();
            let arg_ids = row.unpack_first();

            let variadic_id = match func_impl_details.variadic_oid {
                Some(oid) => Some(self.catalog.get_by_oid(&oid).id().to_string()),
                None => None,
            };

            self.update_catalog_view(
                MZ_FUNCTIONS.id,
                iter::once((
                    Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::Int32(func_impl_details.oid as i32),
                        Datum::Int64(schema_id),
                        Datum::String(name),
                        arg_ids,
                        Datum::from(variadic_id.as_deref()),
                    ]),
                    diff,
                )),
            )
            .await
        }
    }

    async fn sequence_plan(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        pcx: PlanContext,
        plan: Plan,
    ) {
        match plan {
            Plan::CreateDatabase {
                name,
                if_not_exists,
            } => tx.send(
                self.sequence_create_database(name, if_not_exists).await,
                session,
            ),

            Plan::CreateSchema {
                database_name,
                schema_name,
                if_not_exists,
            } => tx.send(
                self.sequence_create_schema(database_name, schema_name, if_not_exists)
                    .await,
                session,
            ),

            Plan::CreateRole { name } => tx.send(self.sequence_create_role(name).await, session),

            Plan::CreateTable {
                name,
                table,
                if_not_exists,
                depends_on,
            } => tx.send(
                self.sequence_create_table(
                    pcx,
                    name,
                    table,
                    if_not_exists,
                    depends_on,
                    session.conn_id(),
                )
                .await,
                session,
            ),

            Plan::CreateSource {
                name,
                source,
                if_not_exists,
                materialized,
            } => tx.send(
                self.sequence_create_source(pcx, name, source, if_not_exists, materialized)
                    .await,
                session,
            ),

            Plan::CreateSink {
                name,
                sink,
                with_snapshot,
                if_not_exists,
                depends_on,
            } => {
                self.sequence_create_sink(
                    pcx,
                    tx,
                    session,
                    name,
                    sink,
                    with_snapshot,
                    if_not_exists,
                    depends_on,
                )
                .await
            }

            Plan::CreateView {
                name,
                view,
                replace,
                materialize,
                if_not_exists,
                depends_on,
            } => tx.send(
                self.sequence_create_view(
                    pcx,
                    name,
                    view,
                    replace,
                    session.conn_id(),
                    materialize,
                    if_not_exists,
                    depends_on,
                )
                .await,
                session,
            ),

            Plan::CreateIndex {
                name,
                index,
                options,
                if_not_exists,
                depends_on,
            } => tx.send(
                self.sequence_create_index(pcx, name, index, options, if_not_exists, depends_on)
                    .await,
                session,
            ),

            Plan::CreateType {
                name,
                typ,
                depends_on,
            } => tx.send(
                self.sequence_create_type(pcx, name, typ, depends_on).await,
                session,
            ),

            Plan::DropDatabase { name } => {
                tx.send(self.sequence_drop_database(name).await, session)
            }

            Plan::DropSchema { name } => tx.send(self.sequence_drop_schema(name).await, session),

            Plan::DropRoles { names } => tx.send(self.sequence_drop_roles(names).await, session),

            Plan::DropItems { items, ty } => {
                tx.send(self.sequence_drop_items(items, ty).await, session)
            }

            Plan::EmptyQuery => tx.send(Ok(ExecuteResponse::EmptyQuery), session),

            Plan::ShowAllVariables => {
                tx.send(self.sequence_show_all_variables(&session).await, session)
            }

            Plan::ShowVariable(name) => {
                tx.send(self.sequence_show_variable(&session, name).await, session)
            }

            Plan::SetVariable { name, value } => tx.send(
                self.sequence_set_variable(&mut session, name, value).await,
                session,
            ),

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
                )
            }

            Plan::Peek {
                source,
                when,
                finishing,
                copy_to,
            } => tx.send(
                self.sequence_peek(session.conn_id(), source, when, finishing, copy_to)
                    .await,
                session,
            ),

            Plan::Tail {
                id,
                ts,
                with_snapshot,
                copy_to,
                emit_progress,
                object_columns,
                desc,
            } => tx.send(
                self.sequence_tail(
                    &mut session,
                    id,
                    with_snapshot,
                    ts,
                    copy_to,
                    emit_progress,
                    object_columns,
                    desc,
                )
                .await,
                session,
            ),

            Plan::SendRows(rows) => tx.send(Ok(send_immediate_rows(rows)), session),

            Plan::ExplainPlan {
                raw_plan,
                decorrelated_plan,
                row_set_finishing,
                stage,
                options,
            } => tx.send(
                self.sequence_explain_plan(
                    &session,
                    raw_plan,
                    decorrelated_plan,
                    row_set_finishing,
                    stage,
                    options,
                ),
                session,
            ),

            Plan::SendDiffs {
                id,
                updates,
                affected_rows,
                kind,
            } => tx.send(
                self.sequence_send_diffs(&mut session, id, updates, affected_rows, kind)
                    .await,
                session,
            ),

            Plan::Insert { id, values } => tx.send(
                self.sequence_insert(&mut session, id, values).await,
                session,
            ),

            Plan::AlterNoop { object_type } => {
                tx.send(Ok(ExecuteResponse::AlteredObject(object_type)), session)
            }

            Plan::AlterItemRename {
                id,
                to_name,
                object_type,
            } => tx.send(
                self.sequence_alter_item_rename(id, to_name, object_type)
                    .await,
                session,
            ),

            Plan::AlterIndexSetOptions { id, options } => {
                tx.send(self.sequence_alter_index_set_options(id, options), session)
            }

            Plan::AlterIndexResetOptions { id, options } => tx.send(
                self.sequence_alter_index_reset_options(id, options),
                session,
            ),

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

            Plan::Declare { name, stmt } => {
                let param_types = vec![];
                let res = self
                    .handle_declare(&mut session, name, stmt, param_types)
                    .map(|()| ExecuteResponse::DeclaredCursor);
                tx.send(res, session);
            }

            Plan::Fetch {
                name,
                count,
                timeout,
            } => tx.send(
                Ok(ExecuteResponse::Fetch {
                    name,
                    count,
                    timeout,
                }),
                session,
            ),

            Plan::Close { name } => {
                if session.remove_portal(&name) {
                    tx.send(Ok(ExecuteResponse::ClosedCursor), session)
                } else {
                    tx.send(Err(CoordError::UnknownCursor(name)), session)
                }
            }
        }
    }

    async fn sequence_create_database(
        &mut self,
        name: String,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, CoordError> {
        let db_oid = self.catalog.allocate_oid()?;
        let schema_oid = self.catalog.allocate_oid()?;
        let ops = vec![
            catalog::Op::CreateDatabase {
                name: name.clone(),
                oid: db_oid,
            },
            catalog::Op::CreateSchema {
                database_name: DatabaseSpecifier::Name(name),
                schema_name: "public".into(),
                oid: schema_oid,
            },
        ];
        match self.catalog_transact(ops).await {
            Ok(_) => Ok(ExecuteResponse::CreatedDatabase { existed: false }),
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedDatabase { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_schema(
        &mut self,
        database_name: DatabaseSpecifier,
        schema_name: String,
        if_not_exists: bool,
    ) -> Result<ExecuteResponse, CoordError> {
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateSchema {
            database_name,
            schema_name,
            oid,
        };
        match self.catalog_transact(vec![op]).await {
            Ok(_) => Ok(ExecuteResponse::CreatedSchema { existed: false }),
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedSchema { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_role(&mut self, name: String) -> Result<ExecuteResponse, CoordError> {
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateRole { name, oid };
        self.catalog_transact(vec![op])
            .await
            .map(|_| ExecuteResponse::CreatedRole)
    }

    async fn sequence_create_table(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        table: sql::plan::Table,
        if_not_exists: bool,
        depends_on: Vec<GlobalId>,
        conn_id: u32,
    ) -> Result<ExecuteResponse, CoordError> {
        let conn_id = if table.temporary { Some(conn_id) } else { None };
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
                self.ship_dataflow(df).await?;
                Ok(ExecuteResponse::CreatedTable { existed: false })
            }
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedTable { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_source(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        source: sql::plan::Source,
        if_not_exists: bool,
        materialized: bool,
    ) -> Result<ExecuteResponse, CoordError> {
        let optimized_expr = self
            .optimizer
            .optimize(source.expr, self.catalog.indexes())?;
        let transformed_desc = RelationDesc::new(optimized_expr.0.typ(), source.column_names);
        let source = catalog::Source {
            create_sql: source.create_sql,
            plan_cx: pcx,
            optimized_expr,
            connector: source.connector,
            bare_desc: source.bare_desc,
            desc: transformed_desc,
        };
        let source_id = self.catalog.allocate_id()?;
        let source_oid = self.catalog.allocate_oid()?;
        let mut ops = vec![catalog::Op::CreateItem {
            id: source_id,
            oid: source_oid,
            name: name.clone(),
            item: CatalogItem::Source(source.clone()),
        }];
        let index_id = if materialized {
            let mut index_name = name.clone();
            index_name.item += "_primary_idx";
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
        match self.catalog_transact(ops).await {
            Ok(()) => {
                self.update_timestamper(source_id, true).await;
                if let Some(index_id) = index_id {
                    let df = self.dataflow_builder().build_index_dataflow(index_id);
                    self.ship_dataflow(df).await?;
                }

                self.maybe_begin_caching(source_id, &source.connector).await;
                Ok(ExecuteResponse::CreatedSource { existed: false })
            }
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedSource { existed: true }),
            Err(err) => Err(err),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn sequence_create_sink(
        &mut self,
        pcx: PlanContext,
        tx: ClientTransmitter<ExecuteResponse>,
        session: Session,
        name: FullName,
        sink: sql::plan::Sink,
        with_snapshot: bool,
        if_not_exists: bool,
        depends_on: Vec<GlobalId>,
    ) {
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
            Err(_) if if_not_exists => {
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

    #[allow(clippy::too_many_arguments)]
    async fn sequence_create_view(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        view: sql::plan::View,
        replace: Option<GlobalId>,
        conn_id: u32,
        materialize: bool,
        if_not_exists: bool,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, CoordError> {
        let mut ops = vec![];
        if let Some(id) = replace {
            ops.extend(self.catalog.drop_items_ops(&[id]));
        }
        let view_id = self.catalog.allocate_id()?;
        let view_oid = self.catalog.allocate_oid()?;
        // Optimize the expression so that we can form an accurately typed description.
        let optimized_expr = self.prep_relation_expr(view.expr, ExprPrepStyle::Static)?;
        let desc = RelationDesc::new(optimized_expr.as_ref().typ(), view.column_names);
        let view = catalog::View {
            create_sql: view.create_sql,
            plan_cx: pcx,
            optimized_expr,
            desc,
            conn_id: if view.temporary { Some(conn_id) } else { None },
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
        match self.catalog_transact(ops).await {
            Ok(()) => {
                if let Some(index_id) = index_id {
                    let df = self.dataflow_builder().build_index_dataflow(index_id);
                    self.ship_dataflow(df).await?;
                }
                Ok(ExecuteResponse::CreatedView { existed: false })
            }
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedView { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_index(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        mut index: sql::plan::Index,
        options: Vec<IndexOption>,
        if_not_exists: bool,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, CoordError> {
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
                self.ship_dataflow(df).await?;
                self.set_index_options(id, options);
                Ok(ExecuteResponse::CreatedIndex { existed: false })
            }
            Err(_) if if_not_exists => Ok(ExecuteResponse::CreatedIndex { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_type(
        &mut self,
        pcx: PlanContext,
        name: FullName,
        typ: sql::plan::Type,
        depends_on: Vec<GlobalId>,
    ) -> Result<ExecuteResponse, CoordError> {
        let typ = catalog::Type {
            create_sql: typ.create_sql,
            plan_cx: pcx,
            inner: typ.inner.into(),
            depends_on,
        };
        let id = self.catalog.allocate_id()?;
        let oid = self.catalog.allocate_oid()?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Type(typ),
        };
        match self.catalog_transact(vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::CreatedType),
            Err(err) => Err(err),
        }
    }

    async fn sequence_drop_database(
        &mut self,
        name: String,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_database_ops(name);
        self.catalog_transact(ops).await?;
        Ok(ExecuteResponse::DroppedDatabase)
    }

    async fn sequence_drop_schema(
        &mut self,
        name: SchemaName,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_schema_ops(name);
        self.catalog_transact(ops).await?;
        Ok(ExecuteResponse::DroppedSchema)
    }

    async fn sequence_drop_roles(
        &mut self,
        names: Vec<String>,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = names
            .into_iter()
            .map(|name| catalog::Op::DropRole { name })
            .collect();
        self.catalog_transact(ops).await?;
        Ok(ExecuteResponse::DroppedRole)
    }

    async fn sequence_drop_items(
        &mut self,
        items: Vec<GlobalId>,
        ty: ObjectType,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_items_ops(&items);
        self.catalog_transact(ops).await?;
        Ok(match ty {
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
        name: String,
    ) -> Result<ExecuteResponse, CoordError> {
        let variable = session.vars().get(&name)?;
        let row = Row::pack_slice(&[Datum::String(&variable.value())]);
        Ok(send_immediate_rows(vec![row]))
    }

    async fn sequence_set_variable(
        &self,
        session: &mut Session,
        name: String,
        value: String,
    ) -> Result<ExecuteResponse, CoordError> {
        session.vars_mut().set(&name, &value)?;
        Ok(ExecuteResponse::SetVariable { name })
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

        if let EndTransactionAction::Commit = action {
            match txn {
                TransactionStatus::Default | TransactionStatus::Failed => {}
                TransactionStatus::Started(ops)
                | TransactionStatus::InTransaction(ops)
                | TransactionStatus::InTransactionImplicit(ops) => {
                    if let TransactionOps::Writes(inserts) = ops {
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
        conn_id: u32,
        source: MirRelationExpr,
        when: PeekWhen,
        finishing: RowSetFinishing,
        copy_to: Option<CopyFormat>,
    ) -> Result<ExecuteResponse, CoordError> {
        let timestamp = self.determine_timestamp(&source, when)?;

        let source = self.prep_relation_expr(
            source,
            ExprPrepStyle::OneShot {
                logical_time: timestamp,
            },
        )?;

        // If this optimizes to a constant expression, we can immediately return the result.
        let resp = if let MirRelationExpr::Constant { rows, typ: _ } = source.as_ref() {
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
                expr::MapFilterProject::extract_from_expression(source.as_ref());
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
                let typ = source.as_ref().typ();
                map_filter_project = expr::MapFilterProject::new(typ.arity());
                let key: Vec<_> = (0..typ.arity()).map(MirScalarExpr::Column).collect();
                let view_id = self.allocate_transient_id()?;
                let mut dataflow = DataflowDesc::new(format!("temp-view-{}", view_id));
                dataflow.set_as_of(Antichain::from_elem(timestamp));
                self.dataflow_builder()
                    .import_view_into_dataflow(&view_id, &source, &mut dataflow);
                dataflow.add_index_to_build(index_id, view_id, typ.clone(), key.clone());
                dataflow.add_index_export(index_id, view_id, typ, key);
                self.ship_dataflow(dataflow).await?;
            }

            self.broadcast(SequencedCommand::Peek {
                id: index_id,
                key: literal_row,
                conn_id,
                tx: rows_tx,
                timestamp,
                finishing: finishing.clone(),
                map_filter_project,
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

    #[allow(clippy::too_many_arguments)]
    async fn sequence_tail(
        &mut self,
        session: &mut Session,
        source_id: GlobalId,
        with_snapshot: bool,
        ts: Option<Timestamp>,
        copy_to: Option<CopyFormat>,
        emit_progress: bool,
        object_columns: usize,
        desc: RelationDesc,
    ) -> Result<ExecuteResponse, CoordError> {
        // Determine the frontier of updates to tail *from*.
        // Updates greater or equal to this frontier will be produced.
        let frontier = if let Some(ts) = ts {
            // If a timestamp was explicitly requested, use that.
            Antichain::from_elem(self.determine_timestamp(
                &MirRelationExpr::Get {
                    id: Id::Global(source_id),
                    // TODO(justin): find a way to avoid synthesizing an arbitrary relation type.
                    typ: RelationType::empty(),
                },
                PeekWhen::AtTimestamp(ts),
            )?)
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
        self.ship_dataflow(df).await?;

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
    /// The result may be `None` in the case that the `when` policy cannot be satisfied,
    /// which is possible due to the restricted validity of traces (each has a `since`
    /// and `upper` frontier, and are only valid after `since` and sure to be available
    /// not after `upper`).
    fn determine_timestamp(
        &mut self,
        source: &MirRelationExpr,
        when: PeekWhen,
    ) -> Result<Timestamp, CoordError> {
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
        let uses_ids = &source.global_uses();
        let (index_ids, indexes_complete) = self.catalog.nearest_indexes(&uses_ids);

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
                if !indexes_complete {
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
            Ok(timestamp)
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
    fn determine_frontier(&self, source_id: GlobalId) -> Antichain<Timestamp> {
        // TODO: The logic that follows is at variance from PEEK logic which consults the
        // "queryable" state of its inputs. We might want those to line up, but it is only
        // a "might".
        if let Some(index_id) = self.catalog.default_index_for(source_id) {
            let upper = self
                .indexes
                .upper_of(&index_id)
                .expect("name missing at coordinator");

            if let Some(ts) = upper.get(0) {
                Antichain::from_elem(ts.saturating_sub(1))
            } else {
                Antichain::from_elem(Timestamp::max_value())
            }
        } else {
            // Use the earliest time that is still valid for all sources.
            let (index_ids, _indexes_complete) = self.catalog.nearest_indexes(&[source_id]);
            self.indexes.least_valid_since(index_ids)
        }
    }

    fn sequence_explain_plan(
        &mut self,
        session: &Session,
        raw_plan: sql::plan::HirRelationExpr,
        decorrelated_plan: expr::MirRelationExpr,
        row_set_finishing: Option<RowSetFinishing>,
        stage: ExplainStage,
        options: ExplainOptions,
    ) -> Result<ExecuteResponse, CoordError> {
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
        id: GlobalId,
        rows: Vec<(Row, isize)>,
        affected_rows: usize,
        kind: MutationKind,
    ) -> Result<ExecuteResponse, CoordError> {
        session.add_transaction_ops(TransactionOps::Writes(vec![WriteOp { id, rows }]))?;
        Ok(match kind {
            MutationKind::Delete => ExecuteResponse::Deleted(affected_rows),
            MutationKind::Insert => ExecuteResponse::Inserted(affected_rows),
            MutationKind::Update => ExecuteResponse::Updated(affected_rows),
        })
    }

    async fn sequence_insert(
        &mut self,
        session: &mut Session,
        id: GlobalId,
        values: MirRelationExpr,
    ) -> Result<ExecuteResponse, CoordError> {
        let prep_style = ExprPrepStyle::OneShot {
            logical_time: self.get_write_ts(),
        };
        match self.prep_relation_expr(values, prep_style)?.into_inner() {
            MirRelationExpr::Constant { rows, typ: _ } => {
                let rows = rows?;
                let desc = self.catalog.get_by_id(&id).desc()?;
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
                let affected_rows = rows.len();
                self.sequence_send_diffs(session, id, rows, affected_rows, MutationKind::Insert)
                    .await
            }
            // If we couldn't optimize the INSERT statement to a constant, it
            // must depend on another relation. We're not yet sophisticated
            // enough to handle this.
            _ => coord_bail!("INSERT statements cannot reference other relations"),
        }
    }

    async fn sequence_alter_item_rename(
        &mut self,
        id: GlobalId,
        to_name: String,
        object_type: ObjectType,
    ) -> Result<ExecuteResponse, CoordError> {
        let op = catalog::Op::RenameItem { id, to_name };
        match self.catalog_transact(vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(object_type)),
            Err(err) => Err(err),
        }
    }

    fn sequence_alter_index_set_options(
        &mut self,
        id: GlobalId,
        options: Vec<IndexOption>,
    ) -> Result<ExecuteResponse, CoordError> {
        self.set_index_options(id, options);
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    fn sequence_alter_index_reset_options(
        &mut self,
        id: GlobalId,
        options: Vec<IndexOptionName>,
    ) -> Result<ExecuteResponse, CoordError> {
        let options = options
            .into_iter()
            .map(|o| match o {
                IndexOptionName::LogicalCompactionWindow => IndexOption::LogicalCompactionWindow(
                    self.logical_compaction_window_ms.map(Duration::from_millis),
                ),
            })
            .collect();
        self.set_index_options(id, options);
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    async fn catalog_transact(&mut self, ops: Vec<catalog::Op>) -> Result<(), CoordError> {
        let events = self.catalog.transact(ops)?;
        self.process_catalog_events(events).await
    }

    async fn process_catalog_events(
        &mut self,
        events: Vec<catalog::Event>,
    ) -> Result<(), CoordError> {
        let mut sources_to_drop = vec![];
        let mut sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];

        for event in &events {
            match event {
                catalog::Event::CreatedDatabase { id, oid, name } => {
                    self.report_database_update(*id, *oid, name, 1).await;
                }
                catalog::Event::CreatedSchema {
                    database_id,
                    schema_id,
                    schema_name,
                    oid,
                } => {
                    self.report_schema_update(*schema_id, *oid, *database_id, schema_name, 1)
                        .await;
                }
                catalog::Event::CreatedRole { id, oid, name } => {
                    self.report_role_update(*id, *oid, name, 1).await;
                }
                catalog::Event::CreatedItem {
                    schema_id,
                    id,
                    oid,
                    name,
                    item,
                } => {
                    if let Ok(desc) = item.desc(&name) {
                        self.report_column_updates(desc, *id, 1).await;
                    }
                    metrics::item_created(*id, &item);
                    match item {
                        CatalogItem::Index(index) => {
                            self.report_index_update(*id, *oid, &index, &name.item, 1)
                                .await
                        }
                        CatalogItem::Table(_) => {
                            self.report_table_update(*id, *oid, *schema_id, &name.item, 1)
                                .await
                        }
                        CatalogItem::Source(_) => {
                            self.report_source_update(*id, *oid, *schema_id, &name.item, 1)
                                .await;
                        }
                        CatalogItem::View(_) => {
                            self.report_view_update(*id, *oid, *schema_id, &name.item, 1)
                                .await;
                        }
                        CatalogItem::Sink(sink) => {
                            if let catalog::Sink {
                                connector: SinkConnectorState::Ready(_),
                                ..
                            } = sink
                            {
                                self.report_sink_update(*id, *oid, *schema_id, &name.item, 1)
                                    .await;
                            }
                        }
                        CatalogItem::Type(ty) => {
                            self.report_type_update(*id, *oid, *schema_id, &name.item, ty, 1)
                                .await;
                        }
                        CatalogItem::Func(func) => {
                            self.report_func_update(*id, *schema_id, &name.item, func, 1)
                                .await;
                        }
                    }
                }
                catalog::Event::UpdatedItem {
                    schema_id,
                    id,
                    oid,
                    from_name,
                    to_name,
                    item,
                } => {
                    // Remove old name and add new name to relevant mz system tables.
                    match item {
                        CatalogItem::Source(_) => {
                            self.report_source_update(*id, *oid, *schema_id, &from_name.item, -1)
                                .await;
                            self.report_source_update(*id, *oid, *schema_id, &to_name.item, 1)
                                .await;
                        }
                        CatalogItem::View(_) => {
                            self.report_view_update(*id, *oid, *schema_id, &from_name.item, -1)
                                .await;
                            self.report_view_update(*id, *oid, *schema_id, &to_name.item, 1)
                                .await;
                        }
                        CatalogItem::Sink(sink) => {
                            if let catalog::Sink {
                                connector: SinkConnectorState::Ready(_),
                                ..
                            } = sink
                            {
                                self.report_sink_update(*id, *oid, *schema_id, &from_name.item, -1)
                                    .await;
                                self.report_sink_update(*id, *oid, *schema_id, &to_name.item, 1)
                                    .await;
                            }
                        }
                        CatalogItem::Table(_) => {
                            self.report_table_update(*id, *oid, *schema_id, &from_name.item, -1)
                                .await;
                            self.report_table_update(*id, *oid, *schema_id, &to_name.item, 1)
                                .await;
                        }
                        CatalogItem::Index(index) => {
                            self.report_index_update(*id, *oid, &index, &from_name.item, -1)
                                .await;
                            self.report_index_update(*id, *oid, &index, &to_name.item, 1)
                                .await;
                        }
                        CatalogItem::Type(typ) => {
                            self.report_type_update(
                                *id,
                                *oid,
                                *schema_id,
                                &from_name.item,
                                &typ,
                                -1,
                            )
                            .await;
                            self.report_type_update(*id, *oid, *schema_id, &to_name.item, &typ, 1)
                                .await;
                        }
                        CatalogItem::Func(_) => unreachable!("functions cannot be updated"),
                    }
                }
                catalog::Event::DroppedDatabase { id, oid, name } => {
                    self.report_database_update(*id, *oid, name, -1).await;
                }
                catalog::Event::DroppedSchema {
                    database_id,
                    schema_id,
                    schema_name,
                    oid,
                } => {
                    self.report_schema_update(
                        *schema_id,
                        *oid,
                        Some(*database_id),
                        schema_name,
                        -1,
                    )
                    .await;
                }
                catalog::Event::DroppedRole { id, oid, name } => {
                    self.report_role_update(*id, *oid, name, -1).await;
                }
                catalog::Event::DroppedIndex { entry, nullable } => match entry.item() {
                    CatalogItem::Index(index) => {
                        indexes_to_drop.push(entry.id());
                        self.report_index_update_inner(
                            entry.id(),
                            entry.oid(),
                            index,
                            &entry.name().item,
                            nullable.to_owned(),
                            -1,
                        )
                        .await
                    }
                    _ => unreachable!("DroppedIndex for non-index item"),
                },
                catalog::Event::DroppedItem { schema_id, entry } => {
                    metrics::item_dropped(entry.id(), entry.item());
                    match entry.item() {
                        CatalogItem::Table(_) => {
                            sources_to_drop.push(entry.id());
                            self.report_table_update(
                                entry.id(),
                                entry.oid(),
                                *schema_id,
                                &entry.name().item,
                                -1,
                            )
                            .await;
                            if let Some(tables) = &mut self.persisted_tables {
                                tables.destroy(entry.id());
                            }
                        }
                        CatalogItem::Source(_) => {
                            sources_to_drop.push(entry.id());
                            self.report_source_update(
                                entry.id(),
                                entry.oid(),
                                *schema_id,
                                &entry.name().item,
                                -1,
                            )
                            .await;
                            self.update_timestamper(entry.id(), false).await;
                            if let Some(cache_tx) = &mut self.cache_tx {
                                cache_tx
                                    .send(CacheMessage::DropSource(entry.id()))
                                    .expect("cache receiver should not drop first");
                            }
                        }
                        CatalogItem::View(_) => {
                            self.report_view_update(
                                entry.id(),
                                entry.oid(),
                                *schema_id,
                                &entry.name().item,
                                -1,
                            )
                            .await;
                        }
                        CatalogItem::Sink(catalog::Sink {
                            connector: SinkConnectorState::Ready(connector),
                            ..
                        }) => {
                            sinks_to_drop.push(entry.id());
                            self.report_sink_update(
                                entry.id(),
                                entry.oid(),
                                *schema_id,
                                &entry.name().item,
                                -1,
                            )
                            .await;
                            match connector {
                                SinkConnector::Kafka(KafkaSinkConnector { topic, .. }) => {
                                    let row = Row::pack_slice(&[
                                        Datum::String(entry.id().to_string().as_str()),
                                        Datum::String(topic.as_str()),
                                    ]);
                                    self.update_catalog_view(
                                        MZ_KAFKA_SINKS.id,
                                        iter::once((row, -1)),
                                    )
                                    .await;
                                }
                                SinkConnector::AvroOcf(AvroOcfSinkConnector { path, .. }) => {
                                    let row = Row::pack_slice(&[
                                        Datum::String(entry.id().to_string().as_str()),
                                        Datum::Bytes(&path.clone().into_os_string().into_vec()),
                                    ]);
                                    self.update_catalog_view(
                                        MZ_AVRO_OCF_SINKS.id,
                                        iter::once((row, -1)),
                                    )
                                    .await;
                                }
                                _ => (),
                            }
                        }
                        CatalogItem::Sink(catalog::Sink {
                            connector: SinkConnectorState::Pending(_),
                            ..
                        }) => {
                            // If the sink connector state is pending, the sink
                            // dataflow was never created, so nothing to drop.
                        }
                        CatalogItem::Type(typ) => {
                            self.report_type_update(
                                entry.id(),
                                entry.oid(),
                                *schema_id,
                                &entry.name().item,
                                typ,
                                -1,
                            )
                            .await;
                        }
                        CatalogItem::Index(_) => {
                            unreachable!("dropped indexes should be handled by DroppedIndex");
                        }
                        CatalogItem::Func(_) => {
                            unreachable!("functions cannot be dropped")
                        }
                    }
                    if let Ok(desc) = entry.desc() {
                        self.report_column_updates(desc, entry.id(), -1).await;
                    }
                }
                catalog::Event::NoOp => (),
            }
        }

        if !sources_to_drop.is_empty() {
            self.broadcast(SequencedCommand::DropSources(sources_to_drop));
        }
        if !sinks_to_drop.is_empty() {
            self.broadcast(SequencedCommand::DropSinks(sinks_to_drop));
        }
        if !indexes_to_drop.is_empty() {
            self.drop_indexes(indexes_to_drop).await;
        }

        Ok(())
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
                if let expr::MirRelationExpr::Filter {
                    input: _,
                    predicates,
                } = &*e
                {
                    match dataflow::FilterPlan::create_from(predicates.iter().cloned()) {
                        Err(e) => coord_bail!("{:?}", e),
                        Ok(plan) => {
                            // If we are in experimenal mode permit temporal filters.
                            // TODO(mcsherry): remove this gating eventually.
                            if plan.non_temporal() || self.catalog.config().experimental_mode {
                                Ok(())
                            } else {
                                coord_bail!("temporal filters require the --experimental flag")
                            }
                        }
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
        if observes_ts && matches!(style, ExprPrepStyle::Static) {
            coord_bail!("mz_logical_timestamp cannot be used in static queries");
        }
        Ok(())
    }

    /// Finalizes a dataflow and then broadcasts it to all workers.
    ///
    /// Finalization includes optimization, but also validation of various
    /// invariants such as ensuring that the `as_of` frontier is in advance of
    /// the various `since` frontiers of participating data inputs.
    ///
    /// In particular, there are requirement on the `as_of` field for the dataflow
    /// and the `since` frontiers of created arrangements, as a function of the `since`
    /// frontiers of dataflow inputs (sources and imported arrangements).
    async fn ship_dataflow(&mut self, mut dataflow: DataflowDesc) -> Result<(), CoordError> {
        // The identity for `join` is the minimum element.
        let mut since = Antichain::from_elem(Timestamp::minimum());

        // TODO: Populate "valid from" information for each source.
        // For each source, ... do nothing because we don't track `since` for sources.
        // for (instance_id, _description) in dataflow.source_imports.iter() {
        //     // TODO: Extract `since` information about each source and apply here. E.g.
        //     since.join_assign(&self.source_info[instance_id].since);
        // }

        // For each imported arrangement, lower bound `since` by its own frontier.
        for (global_id, (_description, _typ)) in dataflow.index_imports.iter() {
            since.join_assign(
                self.indexes
                    .since_of(global_id)
                    .expect("global id missing at coordinator"),
            );
        }

        // For each produced arrangement, start tracking the arrangement with
        // a compaction frontier of at least `since`.
        for (global_id, _description, _typ) in dataflow.index_exports.iter() {
            let mut frontiers =
                Frontiers::new(self.num_workers(), self.logical_compaction_window_ms);
            frontiers.advance_since(&since);
            self.indexes.insert(*global_id, frontiers);
        }

        for (id, sink) in &dataflow.sink_exports {
            match &sink.connector {
                SinkConnector::Kafka(KafkaSinkConnector { topic, .. }) => {
                    let row = Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String(topic.as_str()),
                    ]);
                    self.update_catalog_view(MZ_KAFKA_SINKS.id, iter::once((row, 1)))
                        .await;
                }
                SinkConnector::AvroOcf(AvroOcfSinkConnector { path, .. }) => {
                    let row = Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::Bytes(&path.clone().into_os_string().into_vec()),
                    ]);
                    self.update_catalog_view(MZ_AVRO_OCF_SINKS.id, iter::once((row, 1)))
                        .await;
                }
                _ => (),
            }
        }

        // TODO: Produce "valid from" information for each sink.
        // For each sink, ... do nothing because we don't yield `since` for sinks.
        // for (global_id, _description) in dataflow.sink_exports.iter() {
        //     // TODO: assign `since` to a "valid from" element of the sink. E.g.
        //     self.sink_info[global_id].valid_from(&since);
        // }

        // Ensure that the dataflow's `as_of` is at least `since`.
        if let Some(as_of) = &mut dataflow.as_of {
            // If we have requested a specific time that is invalid .. someone errored.
            use timely::order::PartialOrder;
            if !(<_ as PartialOrder>::less_equal(&since, as_of)) {
                coord_bail!(
                    "Dataflow {} requested as_of ({:?}) not >= since ({:?})",
                    dataflow.debug_name,
                    as_of,
                    since
                );
            }
        } else {
            // Bind the since frontier to the dataflow description.
            dataflow.set_as_of(since);
        }

        // Optimize the dataflow across views, and any other ways that appeal.
        transform::optimize_dataflow(&mut dataflow);

        // Finalize the dataflow by broadcasting its construction to all workers.
        self.broadcast(SequencedCommand::CreateDataflows(vec![dataflow]));
        Ok(())
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
            if let Some(entry) = self.catalog.try_get_by_id(source_id) {
                if let CatalogItem::Source(s) = entry.item() {
                    self.ts_tx
                        .send(TimestampMessage::Add(source_id, s.connector.clone()))
                        .expect("Failed to send CREATE Instance notice to timestamper");
                    self.broadcast(SequencedCommand::AddSourceTimestamping {
                        id: source_id,
                        connector: s.connector.clone(),
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
    async fn maybe_begin_caching(&mut self, id: GlobalId, source_connector: &SourceConnector) {
        if let SourceConnector::External { connector, .. } = source_connector {
            if connector.caching_enabled() {
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
        build_info,
    }: Config<'_>,
    // TODO(benesch): Don't pass runtime explicitly when
    // `Handle::current().block_in_place()` lands. See:
    // https://github.com/tokio-rs/tokio/pull/3097.
    runtime: Arc<Runtime>,
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
    let (catalog, initial_catalog_events) = Catalog::open(&catalog::Config {
        path: &path,
        experimental_mode: Some(experimental_mode),
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
        let scraper_thread_handle = thread::spawn(move || {
            let _executor_guard = executor.enter();
            scraper.run();
        })
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
    let timestamper_thread_handle = thread::spawn(move || {
        let _executor_guard = executor.enter();
        timestamper.update()
    })
    .join_on_drop();

    let mut coord = Coordinator {
        worker_guards,
        worker_txs,
        optimizer: Default::default(),
        catalog,
        symbiosis,
        indexes: ArrangementFrontiers::default(),
        since_updates: Vec::new(),
        logging_granularity: logging
            .as_ref()
            .and_then(|c| c.granularity.as_millis().try_into().ok()),
        logical_compaction_window_ms: logical_compaction_window.map(duration_to_timestamp_millis),
        internal_cmd_tx,
        ts_tx: ts_tx.clone(),
        cache_tx,
        closed_up_to: 1,
        read_lower_bound: 1,
        last_op_was_read: false,
        need_advance: true,
        transient_id_counter: 1,
        active_conns: HashMap::new(),
        persisted_tables,
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
    match coord.bootstrap(initial_catalog_events).await {
        Ok(()) => {
            let thread = thread::spawn(move || {
                runtime.block_on(coord.serve(
                    internal_cmd_rx,
                    cmd_rx,
                    feedback_rx,
                    timestamper_thread_handle,
                    metric_scraper_handle,
                ))
            });
            let handle = Handle {
                cluster_id,
                session_id,
                start_instant,
                _thread: thread.join_on_drop(),
            };
            let client = Client::new(cmd_tx);
            Ok((handle, client))
        }
        Err(e) => {
            metric_scraper_tx.map(|tx| tx.send(ScraperMessage::Shutdown).unwrap());
            // Tell the timestamper thread to shut down.
            ts_tx.send(TimestampMessage::Shutdown).unwrap();
            // Explicitly drop the timestamper handle here so we can wait for
            // the thread to return.
            drop(timestamper_thread_handle);
            coord.broadcast(SequencedCommand::Shutdown);
            Err(e)
        }
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

// Convert a Duration to a Timestamp representing the number
// of milliseconds contained in that Duration
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
