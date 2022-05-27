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
use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_stash::Append;
use rand::Rng;
use serde::{Deserialize, Serialize};
use timely::order::PartialOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tokio::runtime::Handle as TokioHandle;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::warn;
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_dataflow_types::client::controller::{
    ClusterReplicaSizeConfig, ClusterReplicaSizeMap, ReadPolicy,
};
use mz_dataflow_types::client::{
    ComputeInstanceId, ConcreteComputeInstanceReplicaConfig, ControllerResponse,
    LinearizedTimestampBindingFeedback, ReplicaId,
};
use mz_dataflow_types::sinks::{SinkAsOf, SinkConnector, SinkDesc, TailSinkConnector};
use mz_dataflow_types::sources::{
    ExternalSourceConnector, IngestionDescription, PostgresSourceConnector, SourceConnector,
    Timeline,
};
use mz_dataflow_types::{
    BuildDesc, ConnectorContext, DataflowDesc, DataflowDescription, IndexDesc, PeekResponse, Update,
};
use mz_expr::{
    permutation_for_arrangement, CollectionPlan, ExprHumanizer, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_ore::thread::JoinHandleExt;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::{Numeric, NumericMaxScale};
use mz_repr::{
    Datum, Diff, GlobalId, RelationDesc, RelationType, Row, RowArena, ScalarType, Timestamp,
};
use mz_secrets::{SecretOp, SecretsController, SecretsReader};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{
    CreateIndexStatement, CreateSourceStatement, ExplainStage, FetchStatement, Ident, InsertSource,
    ObjectType, Query, Raw, RawIdent, SetExpr, Statement,
};
use mz_sql::catalog::{
    CatalogComputeInstance, CatalogError, CatalogItemType, CatalogTypeDetails, SessionCatalog as _,
};
use mz_sql::names::{
    FullObjectName, QualifiedObjectName, ResolvedDatabaseSpecifier, SchemaSpecifier,
};
use mz_sql::plan::{
    AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan, AlterItemRenamePlan, AlterSecretPlan,
    CreateComputeInstancePlan, CreateComputeInstanceReplicaPlan, CreateConnectorPlan,
    CreateDatabasePlan, CreateIndexPlan, CreateRolePlan, CreateSchemaPlan, CreateSecretPlan,
    CreateSinkPlan, CreateSourcePlan, CreateTablePlan, CreateTypePlan, CreateViewPlan,
    CreateViewsPlan, DropComputeInstanceReplicaPlan, DropComputeInstancesPlan, DropDatabasePlan,
    DropItemsPlan, DropRolesPlan, DropSchemaPlan, ExecutePlan, ExplainPlan, FetchPlan,
    HirRelationExpr, IndexOption, IndexOptionName, InsertPlan, MutationKind, OptimizerConfig,
    Params, PeekPlan, Plan, QueryWhen, RaisePlan, ReadThenWritePlan, ReplicaConfig,
    ResetVariablePlan, SendDiffsPlan, SetVariablePlan, ShowVariablePlan, StatementDesc, TailFrom,
    TailPlan, View,
};
use mz_sql_parser::ast::RawObjectName;
use mz_transform::Optimizer;

use crate::catalog::builtin::{BUILTINS, MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS};
use crate::catalog::{
    self, storage, BuiltinTableUpdate, Catalog, CatalogItem, CatalogState, ComputeInstance,
    Connector, SinkConnectorState,
};
use crate::client::{Client, Handle};
use crate::command::{
    Canceled, Command, ExecuteResponse, Response, StartupMessage, StartupResponse,
};
use crate::coord::dataflow_builder::{prep_relation_expr, prep_scalar_expr, ExprPrepStyle};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::error::CoordError;
use crate::session::{
    EndTransactionAction, PreparedStatement, Session, Transaction, TransactionOps,
    TransactionStatus, WriteOp,
};
use crate::sink_connector;
use crate::tail::PendingTail;
use crate::util::ClientTransmitter;

pub mod id_bundle;

mod dataflow_builder;
mod indexes;

#[derive(Debug)]
pub enum Message {
    Command(Command),
    ControllerReady,
    CreateSourceStatementReady(CreateSourceStatementReady),
    SinkConnectorReady(SinkConnectorReady),
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
pub struct CreateSourceStatementReady {
    pub session: Session,
    #[derivative(Debug = "ignore")]
    pub tx: ClientTransmitter<ExecuteResponse>,
    pub result: Result<CreateSourceStatement<Raw>, CoordError>,
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
    pub compute_instance: ComputeInstanceId,
}

/// Configures a coordinator.
pub struct Config<S> {
    pub dataflow_client: mz_dataflow_types::client::Controller,
    pub storage: storage::Connection<S>,
    pub timestamp_frequency: Duration,
    pub logical_compaction_window: Option<Duration>,
    pub experimental_mode: bool,
    pub build_info: &'static BuildInfo,
    pub metrics_registry: MetricsRegistry,
    pub now: NowFn,
    pub secrets_controller: Box<dyn SecretsController>,
    pub secrets_reader: SecretsReader,
    pub availability_zones: Vec<String>,
    pub replica_sizes: ClusterReplicaSizeMap,
    pub connector_context: ConnectorContext,
}

struct PendingPeek {
    sender: mpsc::UnboundedSender<PeekResponse>,
    conn_id: u32,
    otel_ctx: OpenTelemetryContext,
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

/// State provided to a catalog transaction closure.
pub struct CatalogTxn<'a, T> {
    dataflow_client: &'a mz_dataflow_types::client::Controller<T>,
    catalog: &'a CatalogState,
}

fn concretize_replica_config(
    config: ReplicaConfig,
    replica_sizes: &ClusterReplicaSizeMap,
    availability_zones: &[String],
) -> Result<ConcreteComputeInstanceReplicaConfig, CoordError> {
    let config = match config {
        ReplicaConfig::Remote { replicas } => {
            ConcreteComputeInstanceReplicaConfig::Remote { replicas }
        }
        ReplicaConfig::Managed {
            size,
            availability_zone,
        } => {
            let size_config = replica_sizes.0.get(&size).ok_or_else(|| {
                let mut entries = replica_sizes.0.iter().collect::<Vec<_>>();
                entries.sort_by_key(
                    |(
                        _name,
                        ClusterReplicaSizeConfig {
                            scale, cpu_limit, ..
                        },
                    )| (*scale, *cpu_limit),
                );
                let expected = entries.into_iter().map(|(name, _)| name.clone()).collect();
                CoordError::InvalidClusterReplicaSize { size, expected }
            })?;

            if let Some(az) = &availability_zone {
                if !availability_zones.contains(az) {
                    return Err(CoordError::InvalidClusterReplicaAz {
                        az: az.to_string(),
                        expected: availability_zones.to_vec(),
                    });
                }
            }
            ConcreteComputeInstanceReplicaConfig::Managed {
                size_config: *size_config,
                availability_zone,
            }
        }
    };
    Ok(config)
}

/// Glues the external world to the Timely workers.
pub struct Coordinator<S> {
    /// A client to a running dataflow cluster.
    ///
    /// This component offers:
    /// - Sufficient isolation from COMPUTE, so long as communication with
    ///   COMPUTE replicas is non-blocking.
    /// - Insufficient isolation from STORAGE. The ADAPTER cannot tolerate
    ///   failure of STORAGE services.
    dataflow_client: mz_dataflow_types::client::Controller,
    /// Optimizer instance for logical optimization of views.
    view_optimizer: Optimizer,
    catalog: Catalog<S>,

    /// Delta from leading edge of an arrangement from which we allow compaction.
    logical_compaction_window_ms: Option<Timestamp>,
    /// Channel to manage internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,

    /// Mechanism for totally ordering write and read timestamps, so that all reads
    /// reflect exactly the set of writes that precede them, and no writes that follow.
    global_timeline: timeline::TimestampOracle<Timestamp>,

    transient_id_counter: u64,
    /// A map from connection ID to metadata about that connection for all
    /// active connections.
    active_conns: HashMap<u32, ConnMeta>,

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
    txn_reads: HashMap<u32, TxnReads>,

    /// A map from pending peek ids to the queue into which responses are sent, and
    /// the connection id of the client that initiated the peek.
    pending_peeks: HashMap<Uuid, PendingPeek>,
    /// A map from client connection ids to a set of all pending peeks for that client
    client_pending_peeks: HashMap<u32, BTreeMap<Uuid, ComputeInstanceId>>,
    /// A map from pending tails to the tail description.
    pending_tails: HashMap<GlobalId, PendingTail>,

    /// Serializes accesses to write critical sections.
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// Holds plans deferred due to write lock.
    write_lock_wait_group: VecDeque<DeferredPlan>,

    /// Handle to secret manager that can create and delete secrets from
    /// an arbitrary secret storage engine.
    secrets_controller: Box<dyn SecretsController>,
    /// Handle to secrets reader that gives us access to user secrets
    #[allow(dead_code)]
    secrets_reader: SecretsReader,
    /// Map of strings to corresponding compute replica sizes.
    replica_sizes: ClusterReplicaSizeMap,
    /// Valid availability zones for replicas.
    availability_zones: Vec<String>,

    /// Extra context to pass through to connector creation.
    connector_context: ConnectorContext,
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
                $coord.defer_write($tx, $session, $plan_to_defer);
                return;
            }
        }
    };
}

impl<S: Append + 'static> Coordinator<S> {
    /// Assign a timestamp for a read from a local input. Reads following writes
    /// must be at a time >= the write's timestamp; we choose "equal to" for
    /// simplicity's sake and to open as few new timestamps as possible.
    fn get_local_read_ts(&mut self) -> Timestamp {
        self.global_timeline.read_ts()
    }

    /// Assign a timestamp for creating a source. Writes following reads
    /// must ensure that they are assigned a strictly larger timestamp to ensure
    /// they are not visible to any real-time earlier reads.
    fn get_local_write_ts(&mut self) -> Timestamp {
        self.global_timeline.write_ts()
    }

    /// Assign a timestamp for a write to a local input and increase the local ts.
    /// Writes following reads must ensure that they are assigned a strictly larger
    /// timestamp to ensure they are not visible to any real-time earlier reads.
    fn get_and_step_local_write_ts(&mut self) -> (Timestamp, Timestamp) {
        let ts = self.global_timeline.write_ts();
        /* Without an ADAPTER side durable WAL, all writes must increase the timestamp and be made
         * durable via an APPEND command to STORAGE. Calling `read_ts()` here ensures that the
         * timestamp will go up for the next write.
         * The timestamp must be increased for every write because each call to APPEND will close
         * the provided timestamp, meaning no more writes can happen at that timestamp.
         * If we add an ADAPTER side durable WAL, then consecutive writes could all happen at the
         * same timestamp as long as they're written to the WAL first.
         */
        let _ = self.global_timeline.read_ts();
        let advance_to = ts.step_forward();
        (ts, advance_to)
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
        let mut policy_updates = Vec::new();
        for id in ids.into_iter() {
            let policy = match compaction_window_ms {
                Some(time) => ReadPolicy::lag_writes_by(time),
                None => ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum())),
            };
            self.read_capability.insert(id, policy.clone().into());
            policy_updates.push((id, self.read_capability[&id].policy()));
        }
        self.dataflow_client
            .storage_mut()
            .set_read_policy(policy_updates)
            .await
            .unwrap();
    }

    /// Initialize the compute read policies.
    ///
    /// This should be called only after a compute collection is created, and
    /// ideally very soon afterwards. The collection is otherwise initialized
    /// with a read policy that allows no compaction.
    async fn initialize_compute_read_policies(
        &mut self,
        ids: Vec<GlobalId>,
        instance: mz_dataflow_types::client::ComputeInstanceId,
        compaction_window_ms: Option<Timestamp>,
    ) {
        let mut policy_updates = Vec::new();
        for id in ids.into_iter() {
            let policy = match compaction_window_ms {
                Some(time) => ReadPolicy::lag_writes_by(time),
                None => ReadPolicy::ValidFrom(Antichain::from_elem(Timestamp::minimum())),
            };
            self.read_capability.insert(id, policy.clone().into());
            policy_updates.push((id, self.read_capability[&id].policy()));
        }
        self.dataflow_client
            .compute_mut(instance)
            .unwrap()
            .set_read_policy(policy_updates)
            .await
            .unwrap();
    }

    /// Initializes coordinator state based on the contained catalog. Must be
    /// called after creating the coordinator and before calling the
    /// `Coordinator::serve` method.
    async fn bootstrap(
        &mut self,
        builtin_table_updates: Vec<BuiltinTableUpdate>,
    ) -> Result<(), CoordError> {
        for instance in self.catalog.compute_instances() {
            self.dataflow_client
                .create_instance(instance.id, instance.logging.clone())
                .await
                .unwrap();
            for (replica_id, config) in instance.replicas_by_id.clone() {
                self.dataflow_client
                    .add_replica_to_instance(instance.id, replica_id, config)
                    .await
                    .unwrap();
            }
        }

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

        for entry in &entries {
            match entry.item() {
                // Currently catalog item rebuild assumes that sinks and
                // indexes are always built individually and does not store information
                // about how it was built. If we start building multiple sinks and/or indexes
                // using a single dataflow, we have to make sure the rebuild process re-runs
                // the same multiple-build dataflow.
                CatalogItem::Source(_) => {
                    // Re-announce the source description.
                    let source_description = self
                        .catalog
                        .state()
                        .source_description_for(entry.id())
                        .unwrap();

                    let mut ingestion = IngestionDescription {
                        id: entry.id(),
                        desc: source_description,
                        since: Antichain::from_elem(Timestamp::minimum()),
                        source_imports: BTreeMap::new(),
                        storage_metadata: (),
                    };

                    for id in entry.uses() {
                        if self.catalog.state().get_entry(id).source().is_some() {
                            ingestion.source_imports.insert(*id, ());
                        }
                    }

                    self.dataflow_client
                        .storage_mut()
                        .create_sources(vec![ingestion])
                        .await
                        .unwrap();
                    self.initialize_storage_read_policies(
                        vec![entry.id()],
                        self.logical_compaction_window_ms,
                    )
                    .await;
                }
                CatalogItem::Table(_) => {
                    let since_ts = self.get_local_write_ts();

                    // Re-announce the source description.
                    let source_description = self
                        .catalog
                        .state()
                        .source_description_for(entry.id())
                        .unwrap();

                    let mut ingestion = IngestionDescription {
                        id: entry.id(),
                        desc: source_description,
                        since: Antichain::from_elem(since_ts),
                        source_imports: BTreeMap::new(),
                        storage_metadata: (),
                    };

                    for id in entry.uses() {
                        if self.catalog.state().get_entry(id).source().is_some() {
                            ingestion.source_imports.insert(*id, ());
                        }
                    }

                    self.dataflow_client
                        .storage_mut()
                        .create_sources(vec![ingestion])
                        .await
                        .unwrap();
                    self.initialize_storage_read_policies(
                        vec![entry.id()],
                        self.logical_compaction_window_ms,
                    )
                    .await;
                }
                CatalogItem::Index(idx) => {
                    if logs.contains(&idx.on) {
                        // TODO: make this one call, not many.
                        self.initialize_compute_read_policies(
                            vec![entry.id()],
                            idx.compute_instance,
                            self.logical_compaction_window_ms,
                        )
                        .await;
                    } else {
                        let df = self
                            .dataflow_builder(idx.compute_instance)
                            .build_index_dataflow(entry.id())?;
                        self.ship_dataflow(df, idx.compute_instance).await;
                    }
                }
                CatalogItem::View(_) => (),
                CatalogItem::Sink(sink) => {
                    let builder = match &sink.connector {
                        SinkConnectorState::Pending(builder) => builder,
                        SinkConnectorState::Ready(_) => {
                            panic!("sink already initialized during catalog boot")
                        }
                    };
                    let connector = sink_connector::build(
                        builder.clone(),
                        entry.id(),
                        self.connector_context.clone(),
                    )
                    .await
                    .with_context(|| format!("recreating sink {}", entry.name()))?;
                    self.handle_sink_connector_ready(
                        entry.id(),
                        entry.oid(),
                        connector,
                        // The sink should be established on a specific compute instance.
                        sink.compute_instance,
                    )
                    .await?;
                }
                // Nothing to do for these cases
                CatalogItem::Log(_)
                | CatalogItem::Type(_)
                | CatalogItem::Func(_)
                | CatalogItem::Secret(_)
                | CatalogItem::Connector(_) => {}
            }
        }

        self.send_builtin_table_updates(builtin_table_updates).await;

        // Announce primary and foreign key relationships.
        let mz_view_keys = self.catalog.resolve_builtin_table(&MZ_VIEW_KEYS);
        for log in BUILTINS::logs() {
            let log_id = &self.catalog.resolve_builtin_log(log).to_string();
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
                                id: mz_view_keys,
                                row,
                                diff: 1,
                            }
                        })
                    })
                    .collect(),
            )
            .await;

            let mz_foreign_keys = self.catalog.resolve_builtin_table(&MZ_VIEW_FOREIGN_KEYS);
            self.send_builtin_table_updates(
                log.variant
                    .foreign_keys()
                    .into_iter()
                    .enumerate()
                    .flat_map(|(index, (parent, pairs))| {
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
                    })
                    .collect(),
            )
            .await;
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
            // timestamps to advance (`AS OF X`, TAILing views over RT sources and
            // tables). To address these, spawn a task that forces table timestamps to
            // close on a regular interval. This roughly tracks the behavior of realtime
            // sources that close off timestamps on an interval.
            let internal_cmd_tx = self.internal_cmd_tx.clone();
            task::spawn(|| "coordinator_advance_local_inputs", async move {
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

        loop {
            let msg = select! {
                // Order matters here. We want to process internal commands
                // before processing external commands.
                biased;

                Some(m) = internal_cmd_rx.recv() => m,
                m = self.dataflow_client.ready() => {
                    let () = m.unwrap();
                    Message::ControllerReady
                }
                m = cmd_rx.recv() => match m {
                    None => break,
                    Some(m) => Message::Command(m),
                },
            };

            match msg {
                Message::Command(cmd) => self.message_command(cmd).await,
                Message::ControllerReady => {
                    if let Some(m) = self.dataflow_client.process().await.unwrap() {
                        self.message_controller(m).await
                    }
                }
                Message::CreateSourceStatementReady(ready) => {
                    self.message_create_source_statement_ready(ready).await
                }
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
                Message::AdvanceLocalInputs => {
                    // Convince the coordinator it needs to open a new timestamp
                    // and advance inputs.
                    self.global_timeline.fast_forward(self.now());
                }
            }

            if let Some(timestamp) = self.global_timeline.should_advance_to() {
                self.advance_local_inputs(timestamp).await;
            }
        }
    }

    // Advance all local inputs (tables) to the current wall clock or at least
    // a time greater than any previous table read (if wall clock has gone
    // backward). This downgrades the capabilities of all tables, which means that
    // all tables can no longer produce new data before this timestamp.
    async fn advance_local_inputs(&mut self, advance_to: mz_repr::Timestamp) {
        let appends = self
            .catalog
            .entries()
            .filter(|e| e.is_table())
            .map(|table| (table.id(), vec![], advance_to))
            .collect();
        self.dataflow_client
            .storage_mut()
            .append(appends)
            .await
            .unwrap();
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn message_controller(&mut self, message: ControllerResponse) {
        match message {
            ControllerResponse::PeekResponse(uuid, response) => {
                // We expect exactly one peek response, which we forward. Then we clean up the
                // peek's state in the coordinator.
                if let Some(PendingPeek {
                    sender: rows_tx,
                    conn_id,
                    otel_ctx,
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
            ControllerResponse::LinearizedTimestamps(LinearizedTimestampBindingFeedback {
                timestamp: _,
                peek_id: _,
            }) => {
                // TODO(guswynn): communicate `bindings` to `sequence_peek`
            }
        }
    }

    async fn message_create_source_statement_ready(
        &mut self,
        CreateSourceStatementReady {
            mut session,
            tx,
            result,
            params,
        }: CreateSourceStatementReady,
    ) {
        let stmt = match result {
            Ok(stmt) => stmt,
            Err(e) => return tx.send(Err(e), session),
        };

        let plan = match self
            .handle_statement(&mut session, Statement::CreateSource(stmt), &params)
            .await
        {
            Ok(Plan::CreateSource(plan)) => plan,
            Ok(_) => unreachable!("planning CREATE SOURCE must result in a Plan::CreateSource"),
            Err(e) => return tx.send(Err(e), session),
        };

        let result = self.sequence_create_source(&mut session, plan).await;
        tx.send(result, session);
    }

    async fn message_sink_connector_ready(
        &mut self,
        SinkConnectorReady {
            session,
            tx,
            id,
            oid,
            result,
            compute_instance,
        }: SinkConnectorReady,
    ) {
        match result {
            Ok(connector) => {
                // NOTE: we must not fail from here on out. We have a
                // connector, which means there is external state (like
                // a Kafka topic) that's been created on our behalf. If
                // we fail now, we'll leak that external state.
                if self.catalog.try_get_entry(&id).is_some() {
                    // TODO(benesch): this `expect` here is possibly scary, but
                    // no better solution presents itself. Possibly sinks should
                    // have an error bit, and an error here would set the error
                    // bit on the sink.
                    self.handle_sink_connector_ready(id, oid, connector, compute_instance)
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
                if self.catalog.try_get_entry(&id).is_some() {
                    self.catalog_transact(vec![catalog::Op::DropItem(id)], |_| Ok(()))
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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn message_command(&mut self, cmd: Command) {
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
                otel_ctx,
            } => {
                let tx = ClientTransmitter::new(tx, self.internal_cmd_tx.clone());

                otel_ctx.attach_as_parent();
                self.handle_execute(portal_name, session, tx).await;
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

            // Processing this command DOES NOT send a response to the client;
            // in any situation where you use it, you must also have a code
            // path that responds to the client (e.g. reporting an error).
            Command::RemovePendingPeeks { conn_id } => {
                // The peek is present on some specific compute instance.
                // Allow dataflow to cancel any pending peeks.
                if let Some(uuids) = self.client_pending_peeks.remove(&conn_id) {
                    let mut inverse: BTreeMap<ComputeInstanceId, BTreeSet<Uuid>> =
                        Default::default();
                    for (uuid, compute_instance) in &uuids {
                        inverse.entry(*compute_instance).or_default().insert(*uuid);
                    }
                    for (compute_instance, uuids) in inverse {
                        self.dataflow_client
                            .compute_mut(compute_instance)
                            .unwrap()
                            .cancel_peeks(&uuids)
                            .await
                            .unwrap();
                    }
                    for (uuid, _) in uuids {
                        self.pending_peeks.remove(&uuid);
                    }
                }
            }
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
    fn verify_prepared_statement(
        &self,
        session: &mut Session,
        name: &str,
    ) -> Result<(), CoordError> {
        let ps = match session.get_prepared_statement_unverified(&name) {
            Some(ps) => ps,
            None => return Err(CoordError::UnknownPreparedStatement(name.to_string())),
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
    fn verify_portal(&self, session: &mut Session, name: &str) -> Result<(), CoordError> {
        let portal = match session.get_portal_unverified(&name) {
            Some(portal) => portal,
            None => return Err(CoordError::UnknownCursor(name.to_string())),
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
    ) -> Result<Option<u64>, CoordError> {
        let current_revision = self.catalog.transient_revision();
        if catalog_revision != current_revision {
            let current_desc = self.describe(
                session,
                stmt.cloned(),
                desc.param_types.iter().map(|ty| Some(ty.clone())).collect(),
            )?;
            if &current_desc != desc {
                Err(CoordError::ChangedPlan)
            } else {
                Ok(Some(current_revision))
            }
        } else {
            Ok(None)
        }
    }

    /// Handles an execute command.
    #[tracing::instrument(level = "debug", skip(self, tx, session))]
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
            Some(stmt) => stmt,
            None => return tx.send(Ok(ExecuteResponse::EmptyQuery), session),
        };

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
                        Err(CoordError::OperationRequiresTransaction(
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
                    | Statement::ShowCreateConnector(_)
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

                    Statement::AlterSecret(_)
                        if self.secrets_controller.supports_multi_statement_txn() =>
                    {
                        // if the controller supports this, its safe combine
                    }

                    // Statements below must by run singly (in Started).
                    Statement::AlterIndex(_)
                    | Statement::AlterSecret(_)
                    | Statement::AlterObjectRename(_)
                    | Statement::CreateConnector(_)
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
                            Err(CoordError::OperationProhibitsTransaction(stmt.to_string())),
                            session,
                        )
                    }
                }
            }
        }

        let stmt = stmt.clone();
        let params = portal.parameters.clone();
        match stmt {
            // `CREATE SOURCE` statements must be purified off the main
            // coordinator thread of control.
            Statement::CreateSource(stmt) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = session.conn_id();
                let params = portal.parameters.clone();
                let catalog = self.catalog.for_session(&session);
                let purify_fut =
                    match mz_sql::connectors::populate_connectors(stmt, &catalog, &mut vec![]) {
                        Ok(stmt) => mz_sql::pure::purify_create_source(
                            self.now(),
                            stmt,
                            self.connector_context.clone(),
                        ),
                        Err(e) => return tx.send(Err(e.into()), session),
                    };
                task::spawn(|| format!("purify:{conn_id}"), async move {
                    let result = purify_fut.await.map_err(|e| e.into());
                    internal_cmd_tx
                        .send(Message::CreateSourceStatementReady(
                            CreateSourceStatementReady {
                                session,
                                tx,
                                result,
                                params,
                            },
                        ))
                        .expect("sending to internal_cmd_tx cannot fail");
                });
            }

            // All other statements are handled immediately.
            _ => match self.handle_statement(&mut session, stmt, &params).await {
                Ok(plan) => self.sequence_plan(tx, session, plan).await,
                Err(e) => tx.send(Err(e), session),
            },
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

            // The peek is present on some specific compute instance.
            // Allow dataflow to cancel any pending peeks.
            if let Some(uuids) = self.client_pending_peeks.remove(&conn_id) {
                let mut inverse: BTreeMap<ComputeInstanceId, BTreeSet<Uuid>> = Default::default();
                for (uuid, compute_instance) in &uuids {
                    inverse.entry(*compute_instance).or_default().insert(*uuid);
                }
                for (compute_instance, uuids) in inverse {
                    self.dataflow_client
                        .compute_mut(compute_instance)
                        .unwrap()
                        .cancel_peeks(&uuids)
                        .await
                        .unwrap();
                }
                for (uuid, _) in uuids {
                    if let Some(PendingPeek {
                        sender: rows_tx,
                        conn_id: _,
                        otel_ctx: _,
                    }) = self.pending_peeks.remove(&uuid)
                    {
                        rows_tx
                            .send(PeekResponse::Canceled)
                            .expect("Peek endpoint terminated prematurely");
                    } else {
                        warn!("Received a cancel request without a pending peek: {uuid}");
                    }
                }
            }
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
        self.internal_cmd_tx
            .send(Message::Command(Command::RemovePendingPeeks {
                conn_id: session.conn_id(),
            }))
            .expect("sending to internal_cmd_tx cannot fail");
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
            self.release_read_hold(txn_reads.read_holds).await;
        }
        txn
    }

    /// Removes all temporary items created by the specified connection, though
    /// not the temporary schema itself.
    async fn drop_temp_items(&mut self, conn_id: u32) {
        let ops = self.catalog.drop_temp_item_ops(conn_id);
        self.catalog_transact(ops, |_| Ok(()))
            .await
            .expect("unable to drop temporary items for conn_id");
    }

    async fn handle_sink_connector_ready(
        &mut self,
        id: GlobalId,
        oid: u32,
        connector: SinkConnector,
        compute_instance: ComputeInstanceId,
    ) -> Result<(), CoordError> {
        // Update catalog entry with sink connector.
        let entry = self.catalog.get_entry(&id);
        let name = entry.name().clone();
        let mut sink = match entry.item() {
            CatalogItem::Sink(sink) => sink.clone(),
            _ => unreachable!(),
        };
        sink.connector = catalog::SinkConnectorState::Ready(connector.clone());
        // We don't try to linearize the as of for the sink; we just pick the
        // least valid read timestamp. If users want linearizability across
        // Materialize and their sink, they'll need to reason about the
        // timestamps we emit anyway, so might as emit as much historical detail
        // as we possibly can.
        let id_bundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(&[sink.from]);
        let frontier = self.least_valid_read(&id_bundle, compute_instance);
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
            .catalog_transact(ops, |txn| {
                let mut builder = txn.dataflow_builder(compute_instance);
                let from_entry = builder.catalog.get_entry(&sink.from);
                let sink_description = mz_dataflow_types::sinks::SinkDesc {
                    from: sink.from,
                    from_desc: from_entry
                        .desc(
                            &builder
                                .catalog
                                .resolve_full_name(from_entry.name(), from_entry.conn_id()),
                        )
                        .unwrap()
                        .into_owned(),
                    connector: connector.clone(),
                    envelope: Some(sink.envelope),
                    as_of,
                };
                Ok(builder.build_sink_dataflow(name.to_string(), id, sink_description)?)
            })
            .await?;

        Ok(self.ship_dataflow(df, compute_instance).await)
    }

    async fn sequence_plan(
        &mut self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        plan: Plan,
    ) {
        match plan {
            Plan::CreateConnector(plan) => {
                tx.send(self.sequence_create_connector(plan).await, session);
            }
            Plan::CreateDatabase(plan) => {
                tx.send(self.sequence_create_database(plan).await, session);
            }
            Plan::CreateSchema(plan) => {
                tx.send(self.sequence_create_schema(plan).await, session);
            }
            Plan::CreateRole(plan) => {
                tx.send(self.sequence_create_role(plan).await, session);
            }
            Plan::CreateComputeInstance(plan) => {
                tx.send(self.sequence_create_compute_instance(plan).await, session);
            }
            Plan::CreateComputeInstanceReplica(plan) => {
                tx.send(
                    self.sequence_create_compute_instance_replica(plan).await,
                    session,
                );
            }
            Plan::CreateTable(plan) => {
                tx.send(self.sequence_create_table(&session, plan).await, session);
            }
            Plan::CreateSecret(plan) => {
                tx.send(self.sequence_create_secret(&session, plan).await, session);
            }
            Plan::CreateSource(_) => unreachable!("handled separately"),
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
            Plan::DropComputeInstances(plan) => {
                tx.send(self.sequence_drop_compute_instances(plan).await, session);
            }
            Plan::DropComputeInstanceReplica(plan) => {
                tx.send(
                    self.sequence_drop_compute_instance_replica(plan).await,
                    session,
                );
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
            Plan::ResetVariable(plan) => {
                tx.send(self.sequence_reset_variable(&mut session, plan), session);
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
                tx.send(self.sequence_alter_index_set_options(plan).await, session);
            }
            Plan::AlterIndexResetOptions(plan) => {
                tx.send(self.sequence_alter_index_reset_options(plan).await, session);
            }
            Plan::AlterSecret(plan) => {
                tx.send(
                    self.sequence_alter_secret(&mut session, plan).await,
                    session,
                );
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
                                    otel_ctx: OpenTelemetryContext::empty(),
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
    #[tracing::instrument(level = "debug", skip(self))]
    fn sequence_execute(
        &mut self,
        session: &mut Session,
        plan: ExecutePlan,
    ) -> Result<String, CoordError> {
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

    async fn sequence_create_connector(
        &mut self,
        plan: CreateConnectorPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let connector_oid = self.catalog.allocate_oid().await?;
        let connector_gid = self.catalog.allocate_user_id().await?;
        let ops = vec![catalog::Op::CreateItem {
            id: connector_gid,
            oid: connector_oid,
            name: plan.name.clone(),
            item: CatalogItem::Connector(Connector {
                create_sql: plan.connector.create_sql,
                connector: plan.connector.connector,
            }),
        }];
        match self.catalog_transact(ops, |_| Ok(())).await {
            Ok(_) => Ok(ExecuteResponse::CreatedConnector { existed: false }),
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if plan.if_not_exists => Ok(ExecuteResponse::CreatedConnector { existed: true }),
            Err(err) => Err(err),
        }
    }

    async fn sequence_create_database(
        &mut self,
        plan: CreateDatabasePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let db_oid = self.catalog.allocate_oid().await?;
        let schema_oid = self.catalog.allocate_oid().await?;
        let ops = vec![catalog::Op::CreateDatabase {
            name: plan.name.clone(),
            oid: db_oid,
            public_schema_oid: schema_oid,
        }];
        match self.catalog_transact(ops, |_| Ok(())).await {
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
        let oid = self.catalog.allocate_oid().await?;
        let op = catalog::Op::CreateSchema {
            database_id: plan.database_spec,
            schema_name: plan.schema_name,
            oid,
        };
        match self.catalog_transact(vec![op], |_| Ok(())).await {
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
        let oid = self.catalog.allocate_oid().await?;
        let op = catalog::Op::CreateRole {
            name: plan.name,
            oid,
        };
        self.catalog_transact(vec![op], |_| Ok(()))
            .await
            .map(|_| ExecuteResponse::CreatedRole)
    }

    async fn sequence_create_compute_instance(
        &mut self,
        CreateComputeInstancePlan {
            name,
            config,
            replicas,
        }: CreateComputeInstancePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let introspection_sources = if config.is_some() {
            self.catalog.allocate_introspection_source_indexes().await
        } else {
            Vec::new()
        };
        let mut ops = vec![catalog::Op::CreateComputeInstance {
            name: name.clone(),
            config: config.clone(),
            introspection_sources,
        }];

        for (replica_name, config) in replicas {
            let config =
                concretize_replica_config(config, &self.replica_sizes, &self.availability_zones)?;
            ops.push(catalog::Op::CreateComputeInstanceReplica {
                name: replica_name,
                config,
                on_cluster_name: name.clone(),
            });
        }
        self.catalog_transact(ops, |_| Ok(())).await?;
        let instance = self
            .catalog
            .resolve_compute_instance(&name)
            .expect("compute instance must exist after creation");
        self.dataflow_client
            .create_instance(instance.id, instance.logging.clone())
            .await
            .unwrap();
        for (replica_id, config) in instance.replicas_by_id.clone() {
            self.dataflow_client
                .add_replica_to_instance(instance.id, replica_id, config)
                .await
                .unwrap();
        }
        Ok(ExecuteResponse::CreatedComputeInstance { existed: false })
    }

    async fn sequence_create_compute_instance_replica(
        &mut self,
        CreateComputeInstanceReplicaPlan {
            name,
            of_cluster,
            config,
        }: CreateComputeInstanceReplicaPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let config =
            concretize_replica_config(config, &self.replica_sizes, &self.availability_zones)?;
        let op = catalog::Op::CreateComputeInstanceReplica {
            name: name.clone(),
            config: config.clone(),
            on_cluster_name: of_cluster.clone(),
        };

        self.catalog_transact(vec![op], |_| Ok(())).await?;

        let instance = self.catalog.resolve_compute_instance(&of_cluster)?;
        let replica_id = instance.replica_id_by_name[&name];
        self.dataflow_client
            .add_replica_to_instance(instance.id, replica_id, config)
            .await
            .unwrap();
        Ok(ExecuteResponse::CreatedComputeInstanceReplica { existed: false })
    }

    async fn sequence_create_secret(
        &mut self,
        session: &Session,
        plan: CreateSecretPlan,
    ) -> Result<ExecuteResponse, CoordError> {
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

        self.secrets_controller
            .apply(vec![SecretOp::Ensure {
                id,
                contents: payload,
            }])
            .await?;

        let ops = vec![catalog::Op::CreateItem {
            id,
            oid,
            name,
            item: CatalogItem::Secret(secret.clone()),
        }];

        match self.catalog_transact(ops, |_| Ok(())).await {
            Ok(()) => Ok(ExecuteResponse::CreatedSecret { existed: false }),
            Err(CoordError::Catalog(catalog::Error {
                kind: catalog::ErrorKind::ItemAlreadyExists(_),
                ..
            })) if if_not_exists => Ok(ExecuteResponse::CreatedSecret { existed: true }),
            Err(err) => {
                match self
                    .secrets_controller
                    .apply(vec![SecretOp::Delete { id }])
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        warn!(
                            "Dropping newly created secrets has encountered an error: {}",
                            e
                        );
                    }
                }
                Err(err)
            }
        }
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
        let table_id = self.catalog.allocate_user_id().await?;
        let mut index_depends_on = table.depends_on.clone();
        index_depends_on.push(table_id);
        let table = catalog::Table {
            create_sql: table.create_sql,
            desc: table.desc,
            defaults: table.defaults,
            conn_id,
            depends_on: table.depends_on,
        };
        let table_oid = self.catalog.allocate_oid().await?;
        let ops = vec![catalog::Op::CreateItem {
            id: table_id,
            oid: table_oid,
            name,
            item: CatalogItem::Table(table.clone()),
        }];
        match self.catalog_transact(ops, |_| Ok(())).await {
            Ok(()) => {
                // Determine the initial validity for the table.
                let since_ts = self.get_local_write_ts();

                // Announce the creation of the table source.
                let source_description = self
                    .catalog
                    .state()
                    .source_description_for(table_id)
                    .unwrap();

                let ingestion = IngestionDescription {
                    id: table_id,
                    desc: source_description,
                    since: Antichain::from_elem(since_ts),
                    source_imports: BTreeMap::new(),
                    storage_metadata: (),
                };

                self.dataflow_client
                    .storage_mut()
                    .create_sources(vec![ingestion])
                    .await
                    .unwrap();

                self.initialize_storage_read_policies(
                    vec![table_id],
                    self.logical_compaction_window_ms,
                )
                .await;
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
        let mut ops = vec![];
        let source_id = self.catalog.allocate_user_id().await?;
        let source_oid = self.catalog.allocate_oid().await?;
        let source = catalog::Source {
            create_sql: plan.source.create_sql,
            connector: plan.source.connector,
            desc: plan.source.desc,
            depends_on: plan.source.depends_on,
        };
        ops.push(catalog::Op::CreateItem {
            id: source_id,
            oid: source_oid,
            name: plan.name.clone(),
            item: CatalogItem::Source(source.clone()),
        });
        let index = if plan.materialized {
            let compute_instance = self
                .catalog
                .resolve_compute_instance(session.vars().cluster())?
                .id;
            let mut index_name = plan.name.clone();
            index_name.item += "_primary_idx";
            index_name = self
                .catalog
                .for_session(session)
                .find_available_name(index_name);
            let index_id = self.catalog.allocate_user_id().await?;
            let full_name = self
                .catalog
                .resolve_full_name(&plan.name, Some(session.conn_id()));
            let index = auto_generate_primary_idx(
                index_name.item.clone(),
                compute_instance,
                full_name,
                source_id,
                &source.desc,
                None,
                vec![source_id],
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
        match self
            .catalog_transact(ops, move |txn| {
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
                // Do everything to instantiate the source at the coordinator and
                // inform the timestamper and dataflow workers of its existence before
                // shipping any dataflows that depend on its existence.

                let source_description = self
                    .catalog
                    .state()
                    .source_description_for(source_id)
                    .unwrap();

                let mut ingestion = IngestionDescription {
                    id: source_id,
                    desc: source_description,
                    since: Antichain::from_elem(Timestamp::minimum()),
                    source_imports: BTreeMap::new(),
                    storage_metadata: (),
                };

                for id in self.catalog.state().get_entry(&source_id).uses() {
                    if self.catalog.state().get_entry(id).source().is_some() {
                        ingestion.source_imports.insert(*id, ());
                    }
                }

                self.dataflow_client
                    .storage_mut()
                    .create_sources(vec![ingestion])
                    .await
                    .unwrap();

                self.initialize_storage_read_policies(
                    vec![source_id],
                    self.logical_compaction_window_ms,
                )
                .await;
                if let Some((df, compute_instance)) = df {
                    self.ship_dataflow(df, compute_instance).await;
                }
                Ok(ExecuteResponse::CreatedSource { existed: false })
            }
            Err(CoordError::Catalog(catalog::Error {
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
                compute_instance,
            }),
        };

        let transact_result = self
            .catalog_transact(vec![op], |txn| -> Result<(), CoordError> {
                let from_entry = txn.catalog.get_entry(&sink.from);
                // Insert a dummy dataflow to trigger validation before we try to actually create
                // the external sink resources (e.g. Kafka Topics)
                txn.dataflow_builder(sink.compute_instance)
                    .build_sink_dataflow(
                        "dummy".into(),
                        id,
                        mz_dataflow_types::sinks::SinkDesc {
                            from: sink.from,
                            from_desc: from_entry
                                .desc(
                                    &txn.catalog
                                        .resolve_full_name(from_entry.name(), from_entry.conn_id()),
                                )
                                .unwrap()
                                .into_owned(),
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
        let connector_context = self.connector_context.clone();
        task::spawn(
            || format!("sink_connector_ready:{}", sink.from),
            async move {
                internal_cmd_tx
                    .send(Message::SinkConnectorReady(SinkConnectorReady {
                        session,
                        tx,
                        id,
                        oid,
                        result: sink_connector::build(connector_builder, id, connector_context)
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
    ) -> Result<(Vec<catalog::Op>, Option<(GlobalId, ComputeInstanceId)>), CoordError> {
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
            depends_on: view.depends_on,
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
    ) -> Result<ExecuteResponse, CoordError> {
        let if_not_exists = plan.if_not_exists;
        let (ops, index) = self
            .generate_view_ops(
                session,
                plan.name,
                plan.view.clone(),
                plan.replace,
                plan.materialize,
            )
            .await?;
        match self
            .catalog_transact(ops, |txn| {
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
        let mut indexes = vec![];

        for (name, view) in plan.views {
            let (mut view_ops, index) = self
                .generate_view_ops(session, name, view, None, plan.materialize)
                .await?;
            ops.append(&mut view_ops);
            indexes.extend(index);
        }
        match self
            .catalog_transact(ops, |txn| {
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

        // An index must be created on a specific compute instance.
        let compute_instance = index.compute_instance;

        let id = self.catalog.allocate_user_id().await?;
        let index = catalog::Index {
            create_sql: index.create_sql,
            keys: index.keys,
            on: index.on,
            conn_id: None,
            depends_on: index.depends_on,
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
            .catalog_transact(vec![op], |txn| {
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
        let id = self.catalog.allocate_user_id().await?;
        let oid = self.catalog.allocate_oid().await?;
        let op = catalog::Op::CreateItem {
            id,
            oid,
            name: plan.name,
            item: CatalogItem::Type(typ),
        };
        match self.catalog_transact(vec![op], |_| Ok(())).await {
            Ok(()) => Ok(ExecuteResponse::CreatedType),
            Err(err) => Err(err),
        }
    }

    async fn sequence_drop_database(
        &mut self,
        plan: DropDatabasePlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_database_ops(plan.id);
        self.catalog_transact(ops, |_| Ok(())).await?;
        Ok(ExecuteResponse::DroppedDatabase)
    }

    async fn sequence_drop_schema(
        &mut self,
        plan: DropSchemaPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_schema_ops(plan.id);
        self.catalog_transact(ops, |_| Ok(())).await?;
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
        self.catalog_transact(ops, |_| Ok(())).await?;
        Ok(ExecuteResponse::DroppedRole)
    }

    async fn sequence_drop_compute_instances(
        &mut self,
        plan: DropComputeInstancesPlan,
    ) -> Result<ExecuteResponse, CoordError> {
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
            let ids_to_drop: Vec<GlobalId> = instance.indexes().iter().cloned().collect();
            ops.extend(self.catalog.drop_items_ops(&ids_to_drop));
            ops.push(catalog::Op::DropComputeInstance { name });
        }

        self.catalog_transact(ops, |_| Ok(())).await?;
        for (instance_id, replicas) in instance_replica_drop_sets {
            for (replica_id, config) in replicas {
                self.dataflow_client
                    .drop_replica(instance_id, replica_id, config)
                    .await
                    .unwrap();
            }
            self.dataflow_client
                .drop_instance(instance_id)
                .await
                .unwrap();
        }

        Ok(ExecuteResponse::DroppedComputeInstance)
    }

    async fn sequence_drop_compute_instance_replica(
        &mut self,
        DropComputeInstanceReplicaPlan { names }: DropComputeInstanceReplicaPlan,
    ) -> Result<ExecuteResponse, CoordError> {
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

        self.catalog_transact(ops, |_| Ok(())).await?;

        for (compute_id, replica_id, config) in replicas_to_drop {
            self.dataflow_client
                .drop_replica(compute_id, replica_id, config)
                .await
                .unwrap();
        }

        Ok(ExecuteResponse::DroppedComputeInstanceReplicas)
    }

    async fn sequence_drop_items(
        &mut self,
        plan: DropItemsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let ops = self.catalog.drop_items_ops(&plan.items);
        self.catalog_transact(ops, |_| Ok(())).await?;
        Ok(match plan.ty {
            ObjectType::Source => ExecuteResponse::DroppedSource,
            ObjectType::View => ExecuteResponse::DroppedView,
            ObjectType::Table => ExecuteResponse::DroppedTable,
            ObjectType::Sink => ExecuteResponse::DroppedSink,
            ObjectType::Index => ExecuteResponse::DroppedIndex,
            ObjectType::Type => ExecuteResponse::DroppedType,
            ObjectType::Secret => ExecuteResponse::DroppedSecret,
            ObjectType::Connector => ExecuteResponse::DroppedConnector,
            ObjectType::Role | ObjectType::Cluster | ObjectType::ClusterReplica => {
                unreachable!("handled through their respective sequence_drop functions")
            }
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
    ) -> Result<ExecuteResponse, CoordError> {
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

        if let EndTransactionAction::Commit = action {
            if let Some(ops) = txn.into_ops() {
                match ops {
                    TransactionOps::Writes(inserts) => {
                        // Although the transaction has a wall_time in its pcx, we use a new
                        // coordinator timestamp here to provide linearizability. The wall_time does
                        // not have to relate to the write time.
                        let (timestamp, advance_to) = self.get_and_step_local_write_ts();
                        let mut appends: HashMap<GlobalId, Vec<Update<Timestamp>>> = HashMap::new();

                        for WriteOp { id, rows } in inserts {
                            // Re-verify this id exists.
                            let _ = self.catalog.try_get_entry(&id).ok_or_else(|| {
                                CoordError::SqlCatalog(CatalogError::UnknownItem(id.to_string()))
                            })?;
                            // This can be empty if, say, a DELETE's WHERE clause had 0 results.
                            if rows.is_empty() {
                                continue;
                            }
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

                        match appends.len() {
                            1 => {
                                let (id, updates) = appends.into_element();
                                self.dataflow_client
                                    .storage_mut()
                                    .append(vec![(id, updates, advance_to)])
                                    .await
                                    .unwrap();
                            },
                            0 => {},
                            _ => unreachable!("multi-table write transaction should fail immediately when the second table is added to the transaction"),
                        }
                    }
                    TransactionOps::Secrets(secrets) => {
                        self.secrets_controller.apply(secrets).await?
                    }
                    _ => {}
                }
            }
        }
        Ok(async move { Ok(()) })
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
        conn_id: u32,
        compute_instance: mz_dataflow_types::client::ComputeInstanceId,
    ) -> Result<CollectionIdBundle, CoordError>
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

        // Gather the indexes and unmaterialized sources used by those items.
        let mut id_bundle: CollectionIdBundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(item_ids.iter());

        // Filter out ids from different timelines.
        for ids in [&mut id_bundle.storage_ids, &mut id_bundle.compute_ids] {
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
    #[tracing::instrument(level = "debug", skip(self, session))]
    async fn sequence_peek(
        &mut self,
        session: &mut Session,
        plan: PeekPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        // TODO: remove this function when sources are linearizable.
        // See: #11048.
        fn check_no_unmaterialized_sources<S: Append>(
            catalog: &Catalog<S>,
            compute_instance: ComputeInstanceId,
            id_bundle: &CollectionIdBundle,
            session: &Session,
        ) -> Result<(), CoordError> {
            let mut unmaterialized = vec![];
            for id in &id_bundle.storage_ids {
                let entry = catalog.get_entry(id);
                if entry.is_table() {
                    continue;
                }
                let has_indexes = catalog
                    .state()
                    .get_indexes_on(*id, compute_instance)
                    .any(|_| true);
                if !has_indexes {
                    unmaterialized.push(
                        catalog
                            .resolve_full_name(entry.name(), Some(session.conn_id()))
                            .to_string(),
                    );
                }
            }
            if unmaterialized.is_empty() {
                Ok(())
            } else {
                Err(CoordError::AutomaticTimestampFailure { unmaterialized })
            }
        }

        fn check_no_invalid_log_reads<S: Append>(
            catalog: &Catalog<S>,
            compute_instance: &ComputeInstance,
            source_ids: &BTreeSet<GlobalId>,
            target_replica: &mut Option<ReplicaId>,
        ) -> Result<(), CoordError> {
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
                return Err(CoordError::IntrospectionDisabled { log_names });
            }

            // Reading from log sources on replicated compute instances is only
            // allowed if a target replica is selected. Otherwise, we have no
            // way of knowing which replica we read the introspection data from.
            let num_replicas = compute_instance.replicas_by_id.len();
            if target_replica.is_none() {
                if num_replicas == 1 {
                    *target_replica = compute_instance.replicas_by_id.keys().next().copied();
                } else {
                    return Err(CoordError::UntargetedLogRead { log_names });
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
                    .ok_or(CoordError::UnknownClusterReplica {
                        cluster_name: compute_instance.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        if compute_instance.replicas_by_id.is_empty() {
            return Err(CoordError::NoClusterReplicasAvailable(
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
        // For explicit or implicit transactions that do not use AS OF, get the
        // timestamp of the in-progress transaction or create one. If this is an AS OF
        // query, we don't care about any possible transaction timestamp. If this is a
        // single-statement transaction (TransactionStatus::Started), we don't need to
        // worry about preventing compaction or choosing a valid timestamp for future
        // queries.
        let timestamp = if in_transaction && when == QueryWhen::Immediately {
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
                        QueryWhen::Immediately,
                        compute_instance,
                    )?;
                    let read_holds = read_holds::ReadHolds {
                        time: timestamp,
                        id_bundle,
                        compute_instance,
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
            session.add_transaction_ops(TransactionOps::Peeks(timestamp))?;

            // Verify that the references and indexes for this query are in the
            // current read transaction.
            let id_bundle = self
                .index_oracle(compute_instance)
                .sufficient_collections(&source_ids);
            check_no_unmaterialized_sources(&self.catalog, compute_instance, &id_bundle, session)?;
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
                return Err(CoordError::RelationOutsideTimeDomain {
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
            if when == QueryWhen::Immediately {
                check_no_unmaterialized_sources(
                    &self.catalog,
                    compute_instance,
                    &id_bundle,
                    session,
                )?;
            }
            self.determine_timestamp(session, &id_bundle, when, compute_instance)?
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
    ) -> Result<ExecuteResponse, CoordError> {
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
                coord.determine_timestamp(session, &id_bundle, when, compute_instance)?;

            Ok::<_, CoordError>(SinkDesc {
                from,
                from_desc,
                connector: SinkConnector::Tail(TailSinkConnector::default()),
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
            TailFrom::Query {
                expr,
                desc,
                depends_on,
            } => {
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
    fn least_valid_read(
        &self,
        id_bundle: &CollectionIdBundle,
        instance: mz_dataflow_types::client::ComputeInstanceId,
    ) -> Antichain<mz_repr::Timestamp> {
        let mut since = Antichain::from_elem(Timestamp::minimum());
        {
            let storage = self.dataflow_client.storage();
            for id in id_bundle.storage_ids.iter() {
                since.join_assign(&storage.collection(*id).unwrap().implied_capability)
            }
        }
        {
            let compute = self.dataflow_client.compute(instance).unwrap();
            for id in id_bundle.compute_ids.iter() {
                since.join_assign(&compute.collection(*id).unwrap().implied_capability)
            }
        }
        since
    }

    /// The smallest common valid write frontier among the specified collections.
    ///
    /// Times that are not greater or equal to this frontier are complete for all collections
    /// identified as arguments.
    fn least_valid_write(
        &self,
        id_bundle: &CollectionIdBundle,
        instance: mz_dataflow_types::client::ComputeInstanceId,
    ) -> Antichain<mz_repr::Timestamp> {
        let mut since = Antichain::new();
        {
            let storage = self.dataflow_client.storage();
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
            let compute = self.dataflow_client.compute(instance).unwrap();
            for id in id_bundle.compute_ids.iter() {
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
        since
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
        when: QueryWhen,
        compute_instance: ComputeInstanceId,
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

        let since = self.least_valid_read(&id_bundle, compute_instance);

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

        if when.advance_to_since() {
            candidate.advance_by(since.borrow());
        }
        let uses_tables = id_bundle.iter().any(|id| self.catalog.uses_tables(id));
        if when.advance_to_table_ts(uses_tables) {
            candidate.join_assign(&self.get_local_read_ts());
        }
        if when.advance_to_upper(uses_tables) {
            let upper = self.least_valid_write(&id_bundle, compute_instance);

            // We peek at the largest element not in advance of `upper`, which
            // involves a subtraction. If `upper` contains a zero timestamp there
            // is no "prior" answer, and we do not want to peek at it as it risks
            // hanging awaiting the response to data that may never arrive.
            let upper = if let Some(upper) = upper.as_option() {
                upper.step_back().unwrap_or_else(Timestamp::minimum)
            } else {
                // A complete trace can be read in its final form with this time.
                //
                // This should only happen for literals that have no sources or sources that
                // are known to have completed (non-tailed files for example).
                Timestamp::MAX
            };
            candidate.join_assign(&upper);
        }

        // If the timestamp is greater or equal to some element in `since` we are
        // assured that the answer will be correct.
        if since.less_equal(&candidate) {
            Ok(candidate)
        } else {
            let invalid_indexes = id_bundle
                .compute_ids
                .iter()
                .filter_map(|id| {
                    let since = self
                        .dataflow_client
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
                .collect::<Vec<_>>();
            let invalid_sources = id_bundle.storage_ids.iter().filter_map(|id| {
                let since = self
                    .dataflow_client
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
    ) -> Result<ExecuteResponse, CoordError> {
        let compute_instance = self
            .catalog
            .resolve_compute_instance(session.vars().cluster())?
            .id;

        let ExplainPlan {
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
         -> Result<MirRelationExpr, CoordError> {
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
             -> Result<DataflowDescription<OptimizedMirRelationExpr>, CoordError> {
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
                let catalog = self.catalog.for_session(session);
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                model.as_dot("", &catalog, options.typed)?
            }
            ExplainStage::OptimizedQueryGraph => {
                let catalog = self.catalog.for_session(session);
                let mut model = mz_sql::query_model::Model::try_from(raw_plan)?;
                model.optimize();
                model.as_dot("", &catalog, options.typed)?
            }
            ExplainStage::DecorrelatedPlan => {
                let decorrelated_plan = OptimizedMirRelationExpr::declare_optimized(decorrelate(
                    &mut timings,
                    raw_plan,
                )?);
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
                let decorrelated_plan = decorrelate(&mut timings, raw_plan)?;
                self.validate_timeline(decorrelated_plan.depends_on())?;
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
                let decorrelated_plan = decorrelate(&mut timings, raw_plan)?;
                self.validate_timeline(decorrelated_plan.depends_on())?;
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
            ExplainStage::Timestamp => {
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
                    QueryWhen::Immediately,
                    compute_instance,
                )?;
                let since = self
                    .least_valid_read(&id_bundle, compute_instance)
                    .elements()
                    .to_vec();
                let upper = self
                    .least_valid_write(&id_bundle, compute_instance)
                    .elements()
                    .to_vec();
                let has_table = id_bundle.iter().any(|id| self.catalog.uses_tables(id));
                let table_read_ts = if has_table {
                    Some(self.get_local_read_ts())
                } else {
                    None
                };
                let mut sources = Vec::new();
                {
                    let storage = self.dataflow_client.storage();
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
                        sources.push(mz_dataflow_types::TimestampSource {
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
                    let compute = self.dataflow_client.compute(compute_instance).unwrap();
                    for id in id_bundle.compute_ids.iter() {
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
                        sources.push(mz_dataflow_types::TimestampSource {
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
                let explanation = mz_dataflow_types::TimestampExplanation {
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
        let desc = match self.catalog.try_get_entry(&id) {
            Some(table) => table.desc(
                &self
                    .catalog
                    .resolve_full_name(table.name(), Some(session.conn_id())),
            )?,
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
                    Err(CoordError::SqlCatalog(CatalogError::UnknownItem(
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
                    typ @ (Func | View) => {
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
                    Source | Secret | Connector => false,
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
                    otel_ctx: _,
                } => {
                    // TODO: This timeout should be removed once #11782 lands;
                    // we should instead periodically ensure clusters are
                    // healthy and actively cancel any work waiting on unhealthy
                    // clusters.
                    match tokio::time::timeout(timeout_dur, batch).await {
                        Ok(res) => match res {
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
                                                        return Err(CoordError::Unstructured(
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
                                Err(CoordError::Unstructured(anyhow!("execution canceled")))
                            }
                            PeekResponseUnary::Error(e) => {
                                Err(CoordError::Unstructured(anyhow!(e)))
                            }
                        },
                        Err(_) => {
                            // We timed out, so remove the pending peek. This is
                            // best-effort and doesn't guarantee we won't
                            // receive a response.
                            internal_cmd_tx
                                .send(Message::Command(Command::RemovePendingPeeks {
                                    conn_id: session.conn_id(),
                                }))
                                .expect("sending to internal_cmd_tx cannot fail");
                            Err(CoordError::StatementTimeout)
                        }
                    }
                }
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
            current_full_name: plan.current_full_name,
            to_name: plan.to_name,
        };
        match self.catalog_transact(vec![op], |_| Ok(())).await {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(plan.object_type)),
            Err(err) => Err(err),
        }
    }

    async fn sequence_alter_index_set_options(
        &mut self,
        plan: AlterIndexSetOptionsPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        self.set_index_options(plan.id, plan.options).await?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    async fn sequence_alter_index_reset_options(
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
        self.set_index_options(plan.id, options).await?;
        Ok(ExecuteResponse::AlteredObject(ObjectType::Index))
    }

    async fn sequence_alter_secret(
        &mut self,
        session: &mut Session,
        plan: AlterSecretPlan,
    ) -> Result<ExecuteResponse, CoordError> {
        let AlterSecretPlan { id, mut secret_as } = plan;

        let payload = self.extract_secret(session, &mut secret_as)?;

        session.add_transaction_ops(TransactionOps::Secrets(vec![SecretOp::Ensure {
            id,
            contents: payload,
        }]))?;

        Ok(ExecuteResponse::AlteredObject(ObjectType::Secret))
    }

    fn extract_secret(
        &mut self,
        session: &Session,
        mut secret_as: &mut MirScalarExpr,
    ) -> Result<Vec<u8>, CoordError> {
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
    async fn catalog_transact<F, R>(&mut self, ops: Vec<catalog::Op>, f: F) -> Result<R, CoordError>
    where
        F: FnOnce(CatalogTxn<Timestamp>) -> Result<R, CoordError>,
    {
        let mut sources_to_drop = vec![];
        let mut tables_to_drop = vec![];
        let mut sinks_to_drop = vec![];
        let mut indexes_to_drop = vec![];
        let mut replication_slots_to_drop: HashMap<String, Vec<String>> = HashMap::new();
        let mut secrets_to_drop = vec![];

        for op in &ops {
            if let catalog::Op::DropItem(id) = op {
                match self.catalog.get_entry(id).item() {
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
                    CatalogItem::Secret(_) => {
                        secrets_to_drop.push(*id);
                    }
                    _ => (),
                }
            }
        }

        let (builtin_table_updates, result) = self
            .catalog
            .transact(ops, |catalog| {
                f(CatalogTxn {
                    dataflow_client: &self.dataflow_client,
                    catalog,
                })
            })
            .await?;

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        let _: () = async {
            self.send_builtin_table_updates(builtin_table_updates).await;

            if !sources_to_drop.is_empty() {
                for id in &sources_to_drop {
                    self.read_capability.remove(id);
                }
                self.dataflow_client
                    .storage_mut()
                    .drop_sources(sources_to_drop)
                    .await
                    .unwrap();
            }
            if !tables_to_drop.is_empty() {
                for id in &tables_to_drop {
                    self.read_capability.remove(id);
                }
                self.dataflow_client
                    .storage_mut()
                    .drop_sources(tables_to_drop)
                    .await
                    .unwrap();
            }
            if !sinks_to_drop.is_empty() {
                self.drop_sinks(sinks_to_drop).await;
            }
            if !indexes_to_drop.is_empty() {
                self.drop_indexes(indexes_to_drop).await;
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
                    for (conn, slot_names) in replication_slots_to_drop {
                        // Try to drop the replication slots, but give up after a while.
                        let _ = Retry::default()
                            .max_duration(Duration::from_secs(30))
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

    async fn send_builtin_table_updates(&mut self, updates: Vec<BuiltinTableUpdate>) {
        let (timestamp, advance_to) = self.get_and_step_local_write_ts();
        let mut appends: HashMap<GlobalId, Vec<Update<Timestamp>>> = HashMap::new();
        for u in updates {
            appends.entry(u.id).or_default().push(Update {
                row: u.row,
                diff: u.diff,
                timestamp,
            });
        }
        let appends = appends
            .into_iter()
            .map(|(id, updates)| (id, updates, advance_to))
            .collect();
        self.dataflow_client
            .storage_mut()
            .append(appends)
            .await
            .unwrap();
    }

    async fn drop_sinks(&mut self, sinks: Vec<(ComputeInstanceId, GlobalId)>) {
        let mut by_compute_instance = HashMap::new();
        for (compute_instance, id) in sinks {
            by_compute_instance
                .entry(compute_instance)
                .or_insert(vec![])
                .push(id);
        }
        for (compute_instance, ids) in by_compute_instance {
            self.dataflow_client
                .compute_mut(compute_instance)
                .unwrap()
                .drop_sinks(ids)
                .await
                .unwrap();
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
            self.dataflow_client
                .compute_mut(compute_instance)
                .unwrap()
                .drop_sinks(ids)
                .await
                .unwrap();
        }
    }

    async fn set_index_options(
        &mut self,
        id: GlobalId,
        options: Vec<IndexOption>,
    ) -> Result<(), CoordError> {
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
                    self.dataflow_client
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
        let ops = secrets
            .into_iter()
            .map(|id| SecretOp::Delete { id })
            .collect_vec();

        match self.secrets_controller.apply(ops).await {
            Ok(_) => {}
            Err(e) => {
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
        self.dataflow_client
            .compute_mut(instance)
            .unwrap()
            .create_dataflows(dataflow_plans)
            .await
            .unwrap();
        self.initialize_compute_read_policies(
            output_ids,
            instance,
            self.logical_compaction_window_ms,
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
    ) -> mz_dataflow_types::DataflowDescription<mz_dataflow_types::Plan> {
        // This function must succeed because catalog_transact has generally been run
        // before calling this function. We don't have plumbing yet to rollback catalog
        // operations if this function fails, and materialized will be in an unsafe
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

        let since = self.least_valid_read(
            &CollectionIdBundle {
                storage_ids,
                compute_ids,
            },
            compute_instance,
        );

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
    fn validate_timeline<I>(&self, ids: I) -> Result<Option<Timeline>, CoordError>
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
            let entry = self.catalog.get_entry(&id);
            match entry.item() {
                CatalogItem::Source(source) => {
                    timelines.insert(id, source.connector.timeline());
                }
                CatalogItem::Index(index) => {
                    ids.push(index.on);
                }
                CatalogItem::View(view) => {
                    ids.extend(view.optimized_expr.depends_on());
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
pub async fn serve<S: Append + 'static>(
    Config {
        dataflow_client,
        storage,
        timestamp_frequency,
        logical_compaction_window,
        experimental_mode,
        build_info,
        metrics_registry,
        now,
        secrets_controller,
        secrets_reader,
        replica_sizes,
        availability_zones,
        connector_context,
    }: Config<S>,
) -> Result<(Handle, Client), CoordError> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();

    let (catalog, builtin_table_updates) = Catalog::open(catalog::Config {
        storage,
        experimental_mode: Some(experimental_mode),
        build_info,
        timestamp_frequency,
        now: now.clone(),
        skip_migrations: false,
        metrics_registry: &metrics_registry,
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

    let thread = thread::Builder::new()
        .name("coordinator".to_string())
        .spawn(move || {
            let mut coord = Coordinator {
                dataflow_client,
                view_optimizer: Optimizer::logical_optimizer(),
                catalog,
                logical_compaction_window_ms: logical_compaction_window
                    .map(duration_to_timestamp_millis),
                internal_cmd_tx,
                global_timeline: timeline::TimestampOracle::new(now(), move || (&*now)()),
                transient_id_counter: 1,
                active_conns: HashMap::new(),
                read_capability: Default::default(),
                txn_reads: Default::default(),
                pending_peeks: HashMap::new(),
                client_pending_peeks: HashMap::new(),
                pending_tails: HashMap::new(),
                write_lock: Arc::new(tokio::sync::Mutex::new(())),
                write_lock_wait_group: VecDeque::new(),
                secrets_controller,
                secrets_reader,
                replica_sizes,
                availability_zones,
                connector_context,
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
        otel_ctx: OpenTelemetryContext::empty(),
    }
}

fn auto_generate_primary_idx(
    index_name: String,
    compute_instance: ComputeInstanceId,
    on_name: FullObjectName,
    on_id: GlobalId,
    on_desc: &RelationDesc,
    conn_id: Option<u32>,
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
        in_cluster: Some(RawIdent::Resolved(compute_instance.to_string())),
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
) -> Result<StatementDesc, CoordError> {
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

/// Logic and types for fast-path determination for dataflow execution.
///
/// This module determines if a dataflow can be short-cut, by returning constant values
/// or by reading out of existing arrangements, and implements the appropriate plan.
pub mod fast_path_peek {
    use mz_dataflow_types::client::{ComputeInstanceId, ReplicaId};
    use mz_ore::tracing::OpenTelemetryContext;
    use mz_stash::Append;
    use std::{collections::HashMap, num::NonZeroUsize};
    use uuid::Uuid;

    use crate::coord::{PeekResponseUnary, PendingPeek};
    use crate::CoordError;
    use mz_expr::{EvalError, Id, MirScalarExpr};
    use mz_repr::{Diff, GlobalId, Row};

    #[derive(Debug)]
    pub struct PeekDataflowPlan<T> {
        desc: mz_dataflow_types::DataflowDescription<mz_dataflow_types::Plan<T>, (), T>,
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
            match &dataflow_plan.objects_to_build[0].plan {
                // In the case of a constant, we can return the result now.
                mz_dataflow_types::Plan::Constant { rows } => {
                    return Ok(Plan::Constant(rows.clone()));
                }
                // In the case of a bare `Get`, we may be able to directly index an arrangement.
                mz_dataflow_types::Plan::Get { id, keys, plan } => {
                    match plan {
                        mz_dataflow_types::plan::GetPlan::PassArrangements => {
                            // An arrangement may or may not exist. If not, nothing to be done.
                            if let Some((key, permute, thinning)) = keys.arbitrary_arrangement() {
                                // Just grab any arrangement, but be sure to de-permute the results.
                                for (index_id, (desc, _typ)) in dataflow_plan.index_imports.iter() {
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
                        mz_dataflow_types::plan::GetPlan::Arrangement(key, val, mfp) => {
                            // Convert `mfp` to an executable, non-temporal plan.
                            // It should be non-temporal, as OneShot preparation populates `mz_logical_timestamp`.
                            let map_filter_project = mfp
                                .clone()
                                .into_plan()
                                .map_err(|e| {
                                    crate::error::CoordError::Unstructured(::anyhow::anyhow!(e))
                                })?
                                .into_nontemporal()
                                .map_err(|_e| {
                                    crate::error::CoordError::Unstructured(::anyhow::anyhow!(
                                        "OneShot plan has temporal constraints"
                                    ))
                                })?;
                            // We should only get excited if we can track down an index for `id`.
                            // If `keys` is non-empty, that means we think one exists.
                            for (index_id, (desc, _typ)) in dataflow_plan.index_imports.iter() {
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
                        mz_dataflow_types::plan::GetPlan::Collection(_) => {
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
            conn_id: u32,
            source_arity: usize,
            compute_instance: ComputeInstanceId,
            target_replica: Option<ReplicaId>,
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
                    self.dataflow_client
                        .compute_mut(compute_instance)
                        .unwrap()
                        .create_dataflows(vec![dataflow])
                        .await
                        .unwrap();
                    self.initialize_compute_read_policies(
                        output_ids,
                        compute_instance,
                        self.logical_compaction_window_ms,
                    )
                    .await;

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

            let otel_ctx = OpenTelemetryContext::obtain();
            // The peek is ready to go for both cases, fast and non-fast.
            // Stash the response mechanism, and broadcast dataflow construction.
            self.pending_peeks.insert(
                uuid,
                PendingPeek {
                    sender: rows_tx,
                    conn_id,
                    otel_ctx: otel_ctx.clone(),
                },
            );
            self.client_pending_peeks
                .entry(conn_id)
                .or_default()
                .insert(uuid, compute_instance);
            let (id, key, timestamp, _finishing, map_filter_project) = peek_command;

            self.dataflow_client
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
                self.drop_indexes(vec![(compute_instance, index_id)]).await;
            }

            Ok(crate::ExecuteResponse::SendingRows {
                future: Box::pin(rows_rx),
                otel_ctx,
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

    use mz_dataflow_types::client::ComputeInstanceId;

    use crate::coord::id_bundle::CollectionIdBundle;

    /// Relevant information for acquiring or releasing a bundle of read holds.
    pub(super) struct ReadHolds<T> {
        pub(super) time: T,
        pub(super) id_bundle: CollectionIdBundle,
        pub(super) compute_instance: ComputeInstanceId,
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
            let storage = self.dataflow_client.storage_mut();
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
            let mut policy_changes = Vec::new();
            let mut compute = self
                .dataflow_client
                .compute_mut(read_holds.compute_instance)
                .unwrap();
            for id in read_holds.id_bundle.compute_ids.iter() {
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
        /// Release read holds on the indicated collections at the indicated time.
        ///
        /// This method relies on a previous call to `acquire_read_holds` with the same
        /// argument, and its behavior will be erratic if called on anything else, or if
        /// called more than once on the same bundle of read holds.
        pub(super) async fn release_read_hold(
            &mut self,
            read_holds: ReadHolds<mz_repr::Timestamp>,
        ) {
            let ReadHolds {
                time,
                id_bundle:
                    CollectionIdBundle {
                        storage_ids,
                        compute_ids,
                    },
                compute_instance,
            } = read_holds;

            // Update STORAGE read policies.
            let mut policy_changes = Vec::new();
            for id in storage_ids.iter() {
                // It's possible that a concurrent DDL statement has already dropped this GlobalId
                if let Some(read_needs) = self.read_capability.get_mut(id) {
                    read_needs.holds.update_iter(Some((time, -1)));
                    policy_changes.push((*id, read_needs.policy()));
                }
            }
            self.dataflow_client
                .storage_mut()
                .set_read_policy(policy_changes)
                .await
                .unwrap();
            // Update COMPUTE read policies
            let mut policy_changes = Vec::new();
            for id in compute_ids.iter() {
                // It's possible that a concurrent DDL statement has already dropped this GlobalId
                if let Some(read_needs) = self.read_capability.get_mut(id) {
                    read_needs.holds.update_iter(Some((time, -1)));
                    policy_changes.push((*id, read_needs.policy()));
                }
            }
            self.dataflow_client
                .compute_mut(compute_instance)
                .expect("Reference to absent compute instance")
                .set_read_policy(policy_changes)
                .await
                .unwrap();
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
        let _: DataflowDescription<mz_dataflow_types::plan::Plan> =
            self.finalize_dataflow(df, compute_instance);
    }
}

/// A mechanism to ensure that a sequence of writes and reads proceed correctly through timestamps.
mod timeline {

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

    /// Retreat a timestamp by the least amount possible such that
    /// `ts.step_back().unwrap().less_than(ts)` is true. Return `None` if unable,
    /// which must only happen if the timestamp is `Timestamp::minimum()`.
    fn step_back(&self) -> Option<Self>;
}

impl CoordTimestamp for mz_repr::Timestamp {
    fn step_forward(&self) -> Self {
        match self.checked_add(1) {
            Some(ts) => ts,
            None => panic!("could not step forward"),
        }
    }

    fn step_back(&self) -> Option<Self> {
        self.checked_sub(1)
    }
}
