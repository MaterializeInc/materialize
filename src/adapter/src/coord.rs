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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::ops::Neg;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use derivative::Derivative;
use fail::fail_point;
use futures::StreamExt;
use itertools::Itertools;
use rand::seq::SliceRandom;
use tokio::runtime::Handle as TokioHandle;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch, OwnedMutexGuard};
use tracing::{info, span, warn, Instrument, Level};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_cloud_resources::{CloudResourceController, VpcEndpointConfig};
use mz_controller::clusters::{ClusterConfig, ClusterEvent, ClusterId, ReplicaId};
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr, RowSetFinishing};
use mz_orchestrator::ServiceProcessMetrics;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::retry::Retry;
use mz_ore::task::spawn;
use mz_ore::thread::JoinHandleExt;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{stack, task};
use mz_persist_client::usage::{ShardsUsage, StorageUsageClient};
use mz_repr::explain::ExplainFormat;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_secrets::SecretsController;
use mz_sql::ast::{CreateSourceStatement, CreateSubsourceStatement, Raw, Statement};
use mz_sql::catalog::EnvironmentId;
use mz_sql::names::Aug;
use mz_sql::plan::{CopyFormat, MutationKind, Params, QueryWhen};
use mz_storage_client::controller::{
    CollectionDescription, CreateExportToken, DataSource, StorageError,
};
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::sinks::StorageSinkConnection;
use mz_storage_client::types::sources::{IngestionDescription, SourceExport, Timeline};
use mz_transform::Optimizer;

use crate::catalog::builtin::{BUILTINS, MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS};
use crate::catalog::{
    self, storage, AwsPrincipalContext, BuiltinMigrationMetadata, BuiltinTableUpdate, Catalog,
    CatalogItem, ClusterReplicaSizeMap, DataSourceDesc, Source, StorageSinkConnectionState,
};
use crate::client::{Client, ConnectionId, Handle};
use crate::command::{Canceled, Command, ExecuteResponse};
use crate::config::SystemParameterFrontend;
use crate::coord::appends::{BuiltinTableUpdateSource, Deferred, PendingWriteTxn};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::peek::PendingPeek;
use crate::coord::read_policy::ReadCapability;
use crate::coord::timeline::{TimelineContext, TimelineState, WriteTimestamp};
use crate::coord::timestamp_selection::TimestampContext;
use crate::error::AdapterError;
use crate::metrics::Metrics;
use crate::session::{EndTransactionAction, Session};
use crate::subscribe::ActiveSubscribe;
use crate::util::{ClientTransmitter, CompletedClientTransmitter, ComputeSinkId, ResultExt};
use crate::AdapterNotice;

pub(crate) mod id_bundle;
pub(crate) mod peek;
pub(crate) mod timeline;
pub(crate) mod timestamp_selection;

mod appends;
mod command_handler;
mod dataflows;
mod ddl;
mod indexes;
mod message_handler;
mod read_policy;
mod sequencer;
mod sql;

// TODO: We can have only two consts here, instead of three, once there exists a `const` way to
// convert between a `Timestamp` and a `Duration`, and unwrap a result in const contexts. Currently
// unstable compiler features that would allow this are:
// * `const_option`: https://github.com/rust-lang/rust/issues/67441
// * `const_result`: https://github.com/rust-lang/rust/issues/82814
// * `const_num_from_num`: https://github.com/rust-lang/rust/issues/87852
// * `const_precise_live_drops`: https://github.com/rust-lang/rust/issues/73255

/// `DEFAULT_LOGICAL_COMPACTION_WINDOW`, in milliseconds.
/// The default is set to a second to track the default timestamp frequency for sources.
const DEFAULT_LOGICAL_COMPACTION_WINDOW_MILLIS: u64 = 1000;

/// The default logical compaction window for new objects
pub const DEFAULT_LOGICAL_COMPACTION_WINDOW: Duration =
    Duration::from_millis(DEFAULT_LOGICAL_COMPACTION_WINDOW_MILLIS);

/// `DEFAULT_LOGICAL_COMPACTION_WINDOW` as an `EpochMillis` timestamp
pub const DEFAULT_LOGICAL_COMPACTION_WINDOW_TS: mz_repr::Timestamp =
    Timestamp::new(DEFAULT_LOGICAL_COMPACTION_WINDOW_MILLIS);

/// A dummy availability zone to use when no availability zones are explicitly
/// specified.
pub const DUMMY_AVAILABILITY_ZONE: &str = "";

#[derive(Debug)]
pub enum Message<T = mz_repr::Timestamp> {
    Command(Command),
    ControllerReady,
    CreateSourceStatementReady(CreateSourceStatementReady),
    SinkConnectionReady(SinkConnectionReady),
    SendDiffs(SendDiffs),
    WriteLockGrant(tokio::sync::OwnedMutexGuard<()>),
    /// Initiates a group commit.
    GroupCommitInitiate,
    /// Makes a group commit visible to all clients.
    GroupCommitApply(
        /// Timestamp of the writes in the group commit.
        T,
        /// Clients waiting on responses from the group commit.
        Vec<CompletedClientTransmitter<ExecuteResponse>>,
        /// Optional lock if the group commit contained writes to user tables.
        Option<OwnedMutexGuard<()>>,
    ),
    AdvanceTimelines,
    ClusterEvent(ClusterEvent),
    RemovePendingPeeks {
        conn_id: ConnectionId,
    },
    LinearizeReads(Vec<PendingReadTxn>),
    StorageUsageFetch,
    StorageUsageUpdate(ShardsUsage),
    RealTimeRecencyTimestamp {
        conn_id: ConnectionId,
        transient_revision: u64,
        real_time_recency_ts: Timestamp,
    },
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
    pub result: Result<
        (
            Vec<(GlobalId, CreateSubsourceStatement<Aug>)>,
            CreateSourceStatement<Aug>,
        ),
        AdapterError,
    >,
    pub params: Params,
    pub depends_on: Vec<GlobalId>,
    pub original_stmt: Statement<Raw>,
    pub otel_ctx: OpenTelemetryContext,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SinkConnectionReady {
    #[derivative(Debug = "ignore")]
    pub session_and_tx: Option<(Session, ClientTransmitter<ExecuteResponse>)>,
    pub id: GlobalId,
    pub oid: u32,
    pub create_export_token: CreateExportToken,
    pub result: Result<StorageSinkConnection, AdapterError>,
}

#[derive(Debug)]
pub enum RealTimeRecencyContext {
    ExplainTimestamp {
        tx: ClientTransmitter<ExecuteResponse>,
        session: Session,
        format: ExplainFormat,
        cluster_id: ClusterId,
        optimized_plan: OptimizedMirRelationExpr,
        id_bundle: CollectionIdBundle,
    },
    Peek {
        tx: ClientTransmitter<ExecuteResponse>,
        finishing: RowSetFinishing,
        copy_to: Option<CopyFormat>,
        source: MirRelationExpr,
        session: Session,
        cluster_id: ClusterId,
        when: QueryWhen,
        target_replica: Option<ReplicaId>,
        view_id: GlobalId,
        index_id: GlobalId,
        timeline_context: TimelineContext,
        source_ids: BTreeSet<GlobalId>,
        id_bundle: CollectionIdBundle,
        in_immediate_multi_stmt_txn: bool,
    },
}

impl RealTimeRecencyContext {
    pub(crate) fn take_tx_and_session(self) -> (ClientTransmitter<ExecuteResponse>, Session) {
        match self {
            RealTimeRecencyContext::ExplainTimestamp { tx, session, .. }
            | RealTimeRecencyContext::Peek { tx, session, .. } => (tx, session),
        }
    }
}

/// Configures a coordinator.
pub struct Config {
    pub dataflow_client: mz_controller::Controller,
    pub storage: storage::Connection,
    pub unsafe_mode: bool,
    pub persisted_introspection: bool,
    pub build_info: &'static BuildInfo,
    pub environment_id: EnvironmentId,
    pub metrics_registry: MetricsRegistry,
    pub now: NowFn,
    pub secrets_controller: Arc<dyn SecretsController>,
    pub cloud_resource_controller: Option<Arc<dyn CloudResourceController>>,
    pub availability_zones: Vec<String>,
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    pub default_storage_cluster_size: Option<String>,
    pub bootstrap_system_parameters: BTreeMap<String, String>,
    pub connection_context: ConnectionContext,
    pub storage_usage_client: StorageUsageClient,
    pub storage_usage_collection_interval: Duration,
    pub storage_usage_retention_period: Option<Duration>,
    pub segment_client: Option<mz_segment::Client>,
    pub egress_ips: Vec<Ipv4Addr>,
    pub system_parameter_frontend: Option<Arc<SystemParameterFrontend>>,
    pub aws_account_id: Option<String>,
    pub aws_privatelink_availability_zones: Option<Vec<String>>,
}

/// Soft-state metadata about a compute replica
#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct ReplicaMetadata {
    /// The last time we heard from this replica (possibly rounded)
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// The last known CPU and memory metrics
    pub metrics: Option<Vec<ServiceProcessMetrics>>,
    /// Write frontiers of that replica.
    pub write_frontiers: Vec<(GlobalId, mz_repr::Timestamp)>,
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

    /// Sinks that will need to be dropped when the current transaction, if
    /// any, is cleared.
    drop_sinks: Vec<ComputeSinkId>,

    /// Channel on which to send notices to a session.
    notice_tx: mpsc::UnboundedSender<AdapterNotice>,
}

#[derive(Debug)]
/// A pending transaction waiting to be committed.
pub struct PendingTxn {
    /// Transmitter used to send a response back to the client.
    client_transmitter: ClientTransmitter<ExecuteResponse>,
    /// Client response for transaction.
    response: Result<ExecuteResponse, AdapterError>,
    /// Session of the client who initiated the transaction.
    session: Session,
    /// The action to take at the end of the transaction.
    action: EndTransactionAction,
}

#[derive(Debug)]
/// A pending read transaction waiting to be linearized.
pub enum PendingReadTxn {
    Read {
        /// The inner transaction.
        txn: PendingTxn,
        /// The timestamp context of the transaction.
        timestamp_context: TimestampContext<mz_repr::Timestamp>,
    },
    ReadThenWrite {
        /// Channel used to alert the transaction that the read has been linearized.
        tx: oneshot::Sender<()>,
        /// Timestamp and timeline of the read.
        timestamp: (mz_repr::Timestamp, Timeline),
    },
}

impl PendingReadTxn {
    /// Return the timestamp context of the pending read transaction.
    pub fn timestamp_context(&self) -> TimestampContext<mz_repr::Timestamp> {
        match &self {
            PendingReadTxn::Read {
                timestamp_context, ..
            } => timestamp_context.clone(),
            PendingReadTxn::ReadThenWrite {
                timestamp: (timestamp, timeline),
                ..
            } => TimestampContext::TimelineTimestamp(timeline.clone(), timestamp.clone()),
        }
    }

    /// Alert the client that the read has been linearized.
    pub fn finish(self) {
        match self {
            PendingReadTxn::Read {
                txn:
                    PendingTxn {
                        client_transmitter,
                        response,
                        mut session,
                        action,
                    },
                ..
            } => {
                session.vars_mut().end_transaction(action);
                client_transmitter.send(response, session);
            }
            PendingReadTxn::ReadThenWrite { tx, .. } => {
                // Ignore errors if the caller has hung up.
                let _ = tx.send(());
            }
        }
    }
}

/// Glues the external world to the Timely workers.
pub struct Coordinator {
    /// The controller for the storage and compute layers.
    controller: mz_controller::Controller,
    /// Optimizer instance for logical optimization of views.
    view_optimizer: Optimizer,
    catalog: Catalog,

    /// Channel to manage internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,

    /// Channel for strict serializable reads ready to commit.
    strict_serializable_reads_tx: mpsc::UnboundedSender<PendingReadTxn>,

    /// Mechanism for totally ordering write and read timestamps, so that all reads
    /// reflect exactly the set of writes that precede them, and no writes that follow.
    global_timelines: BTreeMap<Timeline, TimelineState<Timestamp>>,

    transient_id_counter: u64,
    /// A map from connection ID to metadata about that connection for all
    /// active connections.
    active_conns: BTreeMap<ConnectionId, ConnMeta>,

    /// For each identifier in STORAGE, its read policy and any read holds on time.
    ///
    /// Transactions should introduce and remove constraints through the methods
    /// `acquire_read_holds` and `release_read_holds`, respectively. The base
    /// policy can also be updated, though one should be sure to communicate this
    /// to the controller for it to have an effect.
    ///
    /// Access to this field should be restricted to methods in the [`read_policy`] API.
    storage_read_capabilities: BTreeMap<GlobalId, ReadCapability<mz_repr::Timestamp>>,
    /// For each identifier in COMPUTE, its read policy and any read holds on time.
    ///
    /// Transactions should introduce and remove constraints through the methods
    /// `acquire_read_holds` and `release_read_holds`, respectively. The base
    /// policy can also be updated, though one should be sure to communicate this
    /// to the controller for it to have an effect.
    ///
    /// Access to this field should be restricted to methods in the [`read_policy`] API.
    compute_read_capabilities: BTreeMap<GlobalId, ReadCapability<mz_repr::Timestamp>>,

    /// For each transaction, the pinned storage and compute identifiers and time at
    /// which they are pinned.
    ///
    /// Upon completing a transaction, this timestamp should be removed from the holds
    /// in `self.read_capability[id]`, using the `release_read_holds` method.
    txn_reads: BTreeMap<ConnectionId, crate::coord::read_policy::ReadHolds<mz_repr::Timestamp>>,

    /// Access to the peek fields should be restricted to methods in the [`peek`] API.
    /// A map from pending peek ids to the queue into which responses are sent, and
    /// the connection id of the client that initiated the peek.
    pending_peeks: BTreeMap<Uuid, PendingPeek>,
    /// A map from client connection ids to a set of all pending peeks for that client.
    client_pending_peeks: BTreeMap<ConnectionId, BTreeMap<Uuid, ClusterId>>,

    /// A map from client connection ids to a pending real time recency timestamps.
    pending_real_time_recency_timestamp: BTreeMap<ConnectionId, RealTimeRecencyContext>,

    /// A map from active subscribes to the subscribe description.
    active_subscribes: BTreeMap<GlobalId, ActiveSubscribe>,

    /// Serializes accesses to write critical sections.
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// Holds plans deferred due to write lock.
    write_lock_wait_group: VecDeque<Deferred>,
    /// Pending writes waiting for a group commit.
    pending_writes: Vec<PendingWriteTxn>,

    /// Handle to secret manager that can create and delete secrets from
    /// an arbitrary secret storage engine.
    secrets_controller: Arc<dyn SecretsController>,

    /// Handle to a manager that can create and delete kubernetes resources
    /// (ie: VpcEndpoint objects)
    cloud_resource_controller: Option<Arc<dyn CloudResourceController>>,

    /// Extra context to pass through to connection creation.
    connection_context: ConnectionContext,

    /// Metadata about replicas that doesn't need to be persisted.
    /// Intended for inclusion in system tables.
    ///
    /// `None` is used as a tombstone value for replicas that have been
    /// dropped and for which no further updates should be recorded.
    transient_replica_metadata: BTreeMap<ReplicaId, Option<ReplicaMetadata>>,

    /// Persist client for fetching storage metadata such as size metrics.
    storage_usage_client: StorageUsageClient,
    /// The interval at which to collect storage usage information.
    storage_usage_collection_interval: Duration,

    /// Segment analytics client.
    segment_client: Option<mz_segment::Client>,

    /// Coordinator metrics.
    metrics: Metrics,
}

impl Coordinator {
    /// Initializes coordinator state based on the contained catalog. Must be
    /// called after creating the coordinator and before calling the
    /// `Coordinator::serve` method.
    #[tracing::instrument(level = "info", skip_all)]
    pub(crate) async fn bootstrap(
        &mut self,
        builtin_migration_metadata: BuiltinMigrationMetadata,
        mut builtin_table_updates: Vec<BuiltinTableUpdate>,
    ) -> Result<(), AdapterError> {
        info!("coordinator init: beginning bootstrap");

        // Inform the controllers about their initial configuration.
        let compute_config = self.catalog.compute_config();
        self.controller.compute.update_configuration(compute_config);
        let storage_config = self.catalog.storage_config();
        self.controller.storage.update_configuration(storage_config);

        // Capture identifiers that need to have their read holds relaxed once the bootstrap completes.
        //
        // TODO[btv] -- This is of type `Timestamp` because that's what `initialize_read_policies`
        // takes, but it's not clear that that type makes sense. Read policies are logically
        // durations, not instants.
        //
        // Ultimately, it doesn't concretely matter today, because the type ends up just being
        // u64 anyway.
        let mut policies_to_set: BTreeMap<Timestamp, CollectionIdBundle> = Default::default();
        policies_to_set.insert(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS, Default::default());

        info!("coordinator init: creating compute replicas");
        let mut replicas_to_start = vec![];
        for instance in self.catalog.clusters() {
            self.controller.create_cluster(
                instance.id,
                ClusterConfig {
                    arranged_logs: instance.log_indexes.clone(),
                },
            )?;
            for (replica_id, replica) in instance.replicas_by_id.clone() {
                let introspection_collections: Vec<_> = replica
                    .config
                    .compute
                    .logging
                    .sources
                    .iter()
                    .map(|(variant, id)| (*id, variant.desc().into()))
                    .collect();

                self.controller
                    .storage
                    .migrate_collections(introspection_collections.clone())
                    .await?;

                // Create collections does not recreate existing collections, so it is safe to
                // always call it.
                self.controller
                    .storage
                    .create_collections(introspection_collections)
                    .await
                    .unwrap_or_terminate("cannot fail to create collections");

                // TODO - Should these windows be configurable?
                policies_to_set
                    .get_mut(&DEFAULT_LOGICAL_COMPACTION_WINDOW_TS)
                    .expect(
                        "Default policy map was inserted just after `policies_to_set` was created.",
                    )
                    .compute_ids
                    .entry(instance.id)
                    .or_insert_with(BTreeSet::new)
                    .extend(replica.config.compute.logging.source_ids());
                policies_to_set
                    .get_mut(&DEFAULT_LOGICAL_COMPACTION_WINDOW_TS)
                    .expect(
                        "Default policy map was inserted just after `policies_to_set` was created.",
                    )
                    .storage_ids
                    .extend(replica.config.compute.logging.source_ids());

                let role = instance.role();
                replicas_to_start.push((instance.id, replica_id, role, replica.config));
            }
        }
        self.controller.create_replicas(replicas_to_start).await?;

        info!("coordinator init: migrating builtin objects");
        // Migrate builtin objects.
        self.controller
            .storage
            .drop_sources_unvalidated(builtin_migration_metadata.previous_materialized_view_ids);

        self.controller
            .storage
            .drop_sources_unvalidated(builtin_migration_metadata.previous_source_ids);
        self.controller
            .storage
            .drop_sinks_unvalidated(builtin_migration_metadata.previous_sink_ids);

        // Load catalog entries based on topological dependency sorting. We do
        // this to reinforce that `GlobalId`'s `Ord` implementation does not
        // express the entries' dependency graph.
        let mut entries_awaiting_dependencies: BTreeMap<
            GlobalId,
            Vec<(catalog::CatalogEntry, Vec<GlobalId>)>,
        > = BTreeMap::new();
        let mut loaded_items = BTreeSet::new();
        let mut unsorted_entries: VecDeque<_> = self
            .catalog
            .entries()
            .cloned()
            .map(|entry| {
                let remaining_deps = entry.uses().to_vec();
                (entry, remaining_deps)
            })
            .collect();
        let mut entries = Vec::with_capacity(unsorted_entries.len());

        while let Some((entry, mut remaining_deps)) = unsorted_entries.pop_front() {
            remaining_deps.retain(|dep| !loaded_items.contains(dep));
            // While you cannot assume anything about the ordering of
            // dependencies based on their GlobalId, it is not secret knowledge
            // that the most likely final dependency is that with the greatest
            // ID.
            match remaining_deps.last() {
                Some(dep) => {
                    entries_awaiting_dependencies
                        .entry(*dep)
                        .or_default()
                        .push((entry, remaining_deps));
                }
                None => {
                    let id = entry.id();
                    if let Some(waiting_on_this_dep) = entries_awaiting_dependencies.remove(&id) {
                        unsorted_entries.extend(waiting_on_this_dep);
                    }
                    loaded_items.insert(id);
                    entries.push(entry);
                }
            }
        }

        let logs: BTreeSet<_> = BUILTINS::logs()
            .map(|log| self.catalog.resolve_builtin_log(log))
            .collect();

        // This is disabled for the moment because it has unusual upper
        // advancement behavior.
        // See: https://materializeinc.slack.com/archives/C01CFKM1QRF/p1660726837927649
        let source_status_collection_id = Some(self.catalog.resolve_builtin_storage_collection(
            &crate::catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
        ));

        let mut collections_to_create = Vec::new();

        fn source_desc<T>(
            id: GlobalId,
            source_status_collection_id: Option<GlobalId>,
            source: &Source,
        ) -> CollectionDescription<T> {
            let (data_source, status_collection_id) = match &source.data_source {
                // Re-announce the source description.
                DataSourceDesc::Ingestion(ingestion) => {
                    let mut source_imports = BTreeMap::new();
                    for source_import in &ingestion.source_imports {
                        source_imports.insert(*source_import, ());
                    }

                    let mut source_exports = BTreeMap::new();
                    // By convention the first output corresponds to the main source object
                    let main_export = SourceExport {
                        output_index: 0,
                        storage_metadata: (),
                    };
                    source_exports.insert(id, main_export);
                    for (subsource, output_index) in ingestion.subsource_exports.clone() {
                        let export = SourceExport {
                            output_index,
                            storage_metadata: (),
                        };
                        source_exports.insert(subsource, export);
                    }
                    (
                        DataSource::Ingestion(IngestionDescription {
                            desc: ingestion.desc.clone(),
                            ingestion_metadata: (),
                            source_imports,
                            source_exports,
                            instance_id: ingestion.cluster_id,
                            remap_collection_id: ingestion.remap_collection_id.expect(
                                "ingestion-based collection must name remap collection before going to storage",
                            ),
                        }),
                        source_status_collection_id,
                    )
                }
                DataSourceDesc::Progress => (DataSource::Progress, None),
                DataSourceDesc::Source => (DataSource::Other, None),
                DataSourceDesc::Introspection(introspection) => {
                    (DataSource::Introspection(*introspection), None)
                }
            };
            CollectionDescription {
                desc: source.desc.clone(),
                data_source,
                since: None,
                status_collection_id,
            }
        }

        self.controller
            .storage
            .migrate_collections(
                entries
                    .iter()
                    .filter_map(|entry| match entry.item() {
                        CatalogItem::Source(source) => Some((
                            entry.id(),
                            source_desc(entry.id(), source_status_collection_id, source),
                        )),
                        CatalogItem::Table(table) => {
                            let collection_desc = table.desc.clone().into();
                            Some((entry.id(), collection_desc))
                        }
                        CatalogItem::MaterializedView(mview) => {
                            let collection_desc = mview.desc.clone().into();
                            Some((entry.id(), collection_desc))
                        }
                        _ => None,
                    })
                    .collect(),
            )
            .await?;

        // Do a first pass looking for collections to create so we can call
        // create_collections only once with a large batch. This batches only
        // tables and system sources. Replicas above, user sources, and
        // materialized views below could be added here, but they take some
        // additional work due to their dependencies.
        for entry in &entries {
            match entry.item() {
                CatalogItem::Table(table) => {
                    let collection_desc = table.desc.clone().into();
                    collections_to_create.push((entry.id(), collection_desc));
                }
                // User sources can have dependencies, so do avoid them in the
                // batch.
                CatalogItem::Source(source) if entry.id().is_system() => {
                    collections_to_create.push((
                        entry.id(),
                        source_desc(entry.id(), source_status_collection_id, source),
                    ));
                }
                _ => {
                    // No collections to create.
                }
            }
        }

        self.controller
            .storage
            .create_collections(collections_to_create)
            .await
            .unwrap_or_terminate("cannot fail to create collections");

        info!("coordinator init: installing existing objects in catalog");
        let mut privatelink_connections = BTreeMap::new();
        for entry in &entries {
            info!(
                "coordinator init: installing {} {}",
                entry.item().typ(),
                entry.id()
            );
            let policy = entry
                .item()
                .initial_logical_compaction_window()
                .map(|duration| {
                    let ts = Timestamp::from(
                        u64::try_from(duration.as_millis())
                            .expect("Timestamp millis must fit in u64"),
                    );
                    ts
                });
            match entry.item() {
                // Currently catalog item rebuild assumes that sinks and
                // indexes are always built individually and does not store information
                // about how it was built. If we start building multiple sinks and/or indexes
                // using a single dataflow, we have to make sure the rebuild process re-runs
                // the same multiple-build dataflow.
                CatalogItem::Source(source) => {
                    // System sources were created above, add others here.
                    if !entry.id().is_system() {
                        let source_desc =
                            source_desc(entry.id(), source_status_collection_id, source);
                        self.controller
                            .storage
                            .create_collections(vec![(entry.id(), source_desc)])
                            .await
                            .unwrap_or_terminate("cannot fail to create collections");
                    }
                    policies_to_set
                        .entry(policy.expect("sources have a compaction window"))
                        .or_insert_with(Default::default)
                        .storage_ids
                        .insert(entry.id());
                }
                CatalogItem::Table(_) => {
                    policies_to_set
                        .entry(policy.expect("tables have a compaction window"))
                        .or_insert_with(Default::default)
                        .storage_ids
                        .insert(entry.id());
                }
                CatalogItem::Index(idx) => {
                    let policy_entry = policies_to_set
                        .entry(policy.expect("indexes have a compaction window"))
                        .or_insert_with(Default::default);

                    if logs.contains(&idx.on) {
                        policy_entry
                            .compute_ids
                            .entry(idx.cluster_id)
                            .or_insert_with(BTreeSet::new)
                            .insert(entry.id());
                    } else {
                        let dataflow = self
                            .dataflow_builder(idx.cluster_id)
                            .build_index_dataflow(entry.id())?;
                        // What follows is morally equivalent to `self.ship_dataflow(df, idx.cluster_id)`,
                        // but we cannot call that as it will also downgrade the read hold on the index.
                        policy_entry
                            .compute_ids
                            .entry(idx.cluster_id)
                            .or_insert_with(Default::default)
                            .extend(dataflow.export_ids());
                        let dataflow_plan =
                            vec![self.must_finalize_dataflow(dataflow, idx.cluster_id)];
                        self.controller
                            .active_compute()
                            .create_dataflows(idx.cluster_id, dataflow_plan)
                            .unwrap_or_terminate("cannot fail to create dataflows");
                    }
                }
                CatalogItem::View(_) => (),
                CatalogItem::MaterializedView(mview) => {
                    // Re-create the storage collection.
                    let collection_desc = mview.desc.clone().into();
                    self.controller
                        .storage
                        .create_collections(vec![(entry.id(), collection_desc)])
                        .await
                        .unwrap_or_terminate("cannot fail to create collections");

                    policies_to_set
                        .entry(policy.expect("materialized views have a compaction window"))
                        .or_insert_with(Default::default)
                        .storage_ids
                        .insert(entry.id());

                    // Re-create the sink on the compute instance.
                    let id_bundle = self
                        .index_oracle(mview.cluster_id)
                        .sufficient_collections(&mview.depends_on);
                    let as_of = self.least_valid_read(&id_bundle);
                    let internal_view_id = self.allocate_transient_id()?;
                    let df = self
                        .dataflow_builder(mview.cluster_id)
                        .build_materialized_view_dataflow(entry.id(), as_of, internal_view_id)?;
                    self.must_ship_dataflow(df, mview.cluster_id).await;
                }
                CatalogItem::Sink(sink) => {
                    // Re-create the sink.
                    let builder = match &sink.connection {
                        StorageSinkConnectionState::Pending(builder) => builder.clone(),
                        StorageSinkConnectionState::Ready(_) => {
                            panic!("sink already initialized during catalog boot")
                        }
                    };
                    // Now we're ready to create the sink connection. Arrange to notify the
                    // main coordinator thread when the future completes.
                    let internal_cmd_tx = self.internal_cmd_tx.clone();
                    let connection_context = self.connection_context.clone();
                    let id = entry.id();
                    let oid = entry.oid();

                    let create_export_token = self
                        .controller
                        .storage
                        .prepare_export(id, sink.from)
                        .unwrap_or_terminate("cannot fail to prepare export");

                    task::spawn(
                        || format!("sink_connection_ready:{}", sink.from),
                        async move {
                            let conn_result = Retry::default()
                                .max_tries(usize::MAX)
                                .clamp_backoff(Duration::from_secs(60 * 10))
                                .retry_async(|_| async {
                                    let builder = builder.clone();
                                    let connection_context = connection_context.clone();
                                    mz_storage_client::sink::build_sink_connection(
                                        builder,
                                        connection_context,
                                    )
                                    .await
                                })
                                .await
                                .map_err(StorageError::from)
                                .map_err(AdapterError::from);
                            // It is not an error for sink connections to become ready after `internal_cmd_rx` is dropped.
                            let result = internal_cmd_tx.send(Message::SinkConnectionReady(
                                SinkConnectionReady {
                                    session_and_tx: None,
                                    id,
                                    oid,
                                    create_export_token,
                                    result: conn_result,
                                },
                            ));
                            if let Err(e) = result {
                                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                            }
                        },
                    );
                }
                CatalogItem::Connection(catalog_connection) => {
                    if let mz_storage_client::types::connections::Connection::AwsPrivatelink(conn) =
                        &catalog_connection.connection
                    {
                        privatelink_connections.insert(
                            entry.id(),
                            VpcEndpointConfig {
                                aws_service_name: conn.service_name.clone(),
                                availability_zone_ids: conn.availability_zones.clone(),
                            },
                        );
                    }
                }
                // Nothing to do for these cases
                CatalogItem::Log(_)
                | CatalogItem::Type(_)
                | CatalogItem::Func(_)
                | CatalogItem::Secret(_) => {}
            }
        }

        if let Some(cloud_resource_controller) = &self.cloud_resource_controller {
            // Clean up any extraneous VpcEndpoints that shouldn't exist.
            let existing_vpc_endpoints = cloud_resource_controller.list_vpc_endpoints().await?;
            let desired_vpc_endpoints = privatelink_connections.keys().cloned().collect();
            let vpc_endpoints_to_remove = existing_vpc_endpoints.difference(&desired_vpc_endpoints);
            for id in vpc_endpoints_to_remove {
                cloud_resource_controller.delete_vpc_endpoint(*id).await?;
            }

            // Ensure desired VpcEndpoints are up to date.
            for (id, spec) in privatelink_connections {
                cloud_resource_controller
                    .ensure_vpc_endpoint(id, spec)
                    .await?;
            }
        }

        // Having installed all entries, creating all constraints, we can now relax read policies.
        //
        // TODO -- Improve `initialize_read_policies` API so we can avoid calling this in a loop.
        //
        // As of this writing, there can only be at most two keys in `policies_to_set`,
        // so the extra load isn't crazy, but that might not be true in general if we
        // open up custom compaction windows to users.
        for (ts, policies) in policies_to_set {
            self.initialize_read_policies(&policies, Some(ts)).await;
        }

        info!("coordinator init: announcing completion of initialization to controller");
        // Announce the completion of initialization.
        self.controller.initialization_complete();

        // Announce primary and foreign key relationships.
        info!("coordinator init: announcing primary and foreign key relationships");
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
                                Datum::UInt64(u64::cast_from(*k)),
                                Datum::UInt64(u64::cast_from(index)),
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
                        let parent_log = BUILTINS::logs()
                            .find(|src| src.variant == parent)
                            .expect("log foreign key variant is invalid");
                        let parent_id = self.catalog.resolve_builtin_log(parent_log).to_string();
                        pairs.into_iter().map(move |(c, p)| {
                            let row = Row::pack_slice(&[
                                Datum::String(log_id),
                                Datum::UInt64(u64::cast_from(c)),
                                Datum::String(&parent_id),
                                Datum::UInt64(u64::cast_from(p)),
                                Datum::UInt64(u64::cast_from(index)),
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

        // Expose mapping from T-shirt sizes to actual sizes
        builtin_table_updates.extend(self.catalog.state().pack_all_replica_size_updates());

        // Advance all tables to the current timestamp
        info!("coordinator init: advancing all tables to current timestamp");
        let WriteTimestamp {
            timestamp: _,
            advance_to,
        } = self.get_local_write_ts().await;
        let appends = entries
            .iter()
            .filter(|entry| entry.is_table())
            .map(|entry| (entry.id(), Vec::new(), advance_to))
            .collect();
        self.controller
            .storage
            .append(appends)
            .expect("invalid updates")
            .await
            .expect("One-shot shouldn't be dropped during bootstrap")
            .unwrap_or_terminate("cannot fail to append");

        // Add builtin table updates the clear the contents of all system tables
        info!("coordinator init: resetting system tables");
        let read_ts = self.get_local_read_ts();
        for system_table in entries
            .iter()
            .filter(|entry| entry.is_table() && entry.id().is_system())
        {
            info!(
                "coordinator init: resetting system table {} ({})",
                self.catalog.resolve_full_name(system_table.name(), None),
                system_table.id()
            );
            let current_contents = self
                .controller
                .storage
                .snapshot(system_table.id(), read_ts)
                .await
                .unwrap_or_terminate("cannot fail to fetch snapshot");
            info!("coordinator init: table size {}", current_contents.len());
            let retractions = current_contents
                .into_iter()
                .map(|(row, diff)| BuiltinTableUpdate {
                    id: system_table.id(),
                    row,
                    diff: diff.neg(),
                });
            builtin_table_updates.extend(retractions);
        }

        info!("coordinator init: sending builtin table updates");
        self.send_builtin_table_updates(builtin_table_updates, BuiltinTableUpdateSource::DDL)
            .await;

        // Signal to the storage controller that it is now free to reconcile its
        // state with what it has learned from the adapter.
        self.controller.storage.reconcile_state().await;

        // Cleanup orphaned secrets. Errors during list() or delete() do not
        // need to prevent bootstrap from succeeding; we will retry next
        // startup.
        if let Ok(controller_secrets) = self.secrets_controller.list().await {
            // Fetch all IDs from the catalog to future-proof against other
            // things using secrets. Today, SECRET and CONNECTION objects use
            // secrets_controller.ensure, but more things could in the future
            // that would be easy to miss adding here.
            let catalog_ids: BTreeSet<GlobalId> =
                self.catalog.entries().map(|entry| entry.id()).collect();
            let controller_secrets: BTreeSet<GlobalId> = controller_secrets.into_iter().collect();
            let orphaned = controller_secrets.difference(&catalog_ids);
            for id in orphaned {
                info!("coordinator init: deleting orphaned secret {id}");
                fail_point!("orphan_secrets");
                if let Err(e) = self.secrets_controller.delete(*id).await {
                    warn!("Dropping orphaned secret has encountered an error: {}", e);
                }
            }
        }

        info!("coordinator init: bootstrap complete");
        Ok(())
    }

    /// Serves the coordinator, receiving commands from users over `cmd_rx`
    /// and feedback from dataflow workers over `feedback_rx`.
    ///
    /// You must call `bootstrap` before calling this method.
    async fn serve(
        mut self,
        mut internal_cmd_rx: mpsc::UnboundedReceiver<Message>,
        mut strict_serializable_reads_rx: mpsc::UnboundedReceiver<PendingReadTxn>,
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
            tokio::time::interval(self.catalog.config().timestamp_interval);
        // // Watcher that listens for and reports cluster service status changes.
        let mut cluster_events = self.controller.events_stream();
        let (idle_tx, mut idle_rx) = tokio::sync::mpsc::channel(1);
        let idle_metric = self.metrics.queue_busy_seconds.with_label_values(&[]);
        spawn(|| "coord idle metric", async move {
            // Every 5 seconds, attempt to measure how long it takes for the
            // coord select loop to be empty, because this message is the last
            // processed. If it is idle, this will result in some microseconds
            // of measurement.
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                // If the buffer is full (or the channel is closed), ignore and
                // try again later.
                let _ = idle_tx.try_send(idle_metric.start_timer());
            }
        });

        self.schedule_storage_usage_collection();

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
                Some(event) = cluster_events.next() => Message::ClusterEvent(event),
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
                // `recv()` on `UnboundedReceiver` is cancellation safe:
                // https://docs.rs/tokio/1.8.0/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety
                Some(pending_read_txn) = strict_serializable_reads_rx.recv() => {
                    let mut pending_read_txns = vec![pending_read_txn];
                    while let Ok(pending_read_txn) = strict_serializable_reads_rx.try_recv() {
                        pending_read_txns.push(pending_read_txn);
                    }
                    Message::LinearizeReads(pending_read_txns)
                }
                // `tick()` on `Interval` is cancel-safe:
                // https://docs.rs/tokio/1.19.2/tokio/time/struct.Interval.html#cancel-safety
                _ = advance_timelines_interval.tick() => Message::GroupCommitInitiate,

                // Process the idle metric at the lowest priority to sample queue non-idle time.
                // `recv()` on `Receiver` is cancellation safe:
                // https://docs.rs/tokio/1.8.0/tokio/sync/mpsc/struct.Receiver.html#cancel-safety
                timer = idle_rx.recv() => {
                    timer.expect("does not drop").observe_duration();
                    continue;
                }
            };

            // All message processing functions trace. Start a parent span for them to make
            // it easy to find slow messages.
            let span = span!(Level::DEBUG, "coordinator message processing");
            let _enter = span.enter();

            self.handle_message(msg).await;
        }
    }
}

/// Serves the coordinator based on the provided configuration.
///
/// For a high-level description of the coordinator, see the [crate
/// documentation](crate).
///
/// Returns a handle to the coordinator and a client to communicate with the
/// coordinator.
// TODO: This causes stack overflows during tests which can be fixed by setting
// RUST_MIN_STACK=8388608, but we'd like to come up with a better solution, so
// don't enable serve tracing for now.
//#[tracing::instrument(name = "coord::serve", level = "info", skip_all)]
pub async fn serve(
    Config {
        dataflow_client,
        storage,
        unsafe_mode,
        persisted_introspection,
        build_info,
        environment_id,
        metrics_registry,
        now,
        secrets_controller,
        cloud_resource_controller,
        cluster_replica_sizes,
        default_storage_cluster_size,
        bootstrap_system_parameters,
        mut availability_zones,
        connection_context,
        storage_usage_client,
        storage_usage_collection_interval,
        storage_usage_retention_period,
        segment_client,
        egress_ips,
        aws_account_id,
        aws_privatelink_availability_zones,
        system_parameter_frontend,
    }: Config,
) -> Result<(Handle, Client), AdapterError> {
    info!("coordinator init: beginning");

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();
    let (strict_serializable_reads_tx, strict_serializable_reads_rx) = mpsc::unbounded_channel();

    // Validate and process availability zones.
    if !availability_zones.iter().all_unique() {
        coord_bail!("availability zones must be unique");
    }
    // Later on, we choose an AZ for every replica, so we need to have at least
    // one. If we're using an orchestrator that doesn't have the notion of AZs,
    // just create a fake, blank one.
    if availability_zones.is_empty() {
        availability_zones.push(DUMMY_AVAILABILITY_ZONE.into());
    }
    // Shuffle availability zones for unbiased selection in
    // Coordinator::sequence_create_compute_replica.
    availability_zones.shuffle(&mut rand::thread_rng());

    let aws_principal_context = if aws_account_id.is_some()
        && connection_context.aws_external_id_prefix.is_some()
    {
        Some(AwsPrincipalContext {
            aws_account_id: aws_account_id.expect("known to be `Some` from `is_some()` call above"),
            aws_external_id_prefix: connection_context
                .aws_external_id_prefix
                .clone()
                .expect("known to be `Some` from `is_some()` call above"),
        })
    } else {
        None
    };

    let aws_privatelink_availability_zones = aws_privatelink_availability_zones
        .map(|azs_vec| BTreeSet::from_iter(azs_vec.iter().cloned()));

    info!("coordinator init: opening catalog");
    let (mut catalog, builtin_migration_metadata, builtin_table_updates, _last_catalog_version) =
        Catalog::open(catalog::Config {
            storage,
            unsafe_mode,
            persisted_introspection,
            build_info,
            environment_id,
            now: now.clone(),
            skip_migrations: false,
            metrics_registry: &metrics_registry,
            cluster_replica_sizes,
            default_storage_cluster_size,
            bootstrap_system_parameters,
            availability_zones,
            secrets_reader: secrets_controller.reader(),
            egress_ips,
            aws_principal_context,
            aws_privatelink_availability_zones,
            system_parameter_frontend,
            storage_usage_retention_period,
        })
        .await?;
    let session_id = catalog.config().session_id;
    let start_instant = catalog.config().start_instant;

    // In order for the coordinator to support Rc and Refcell types, it cannot be
    // sent across threads. Spawn it in a thread and have this parent thread wait
    // for bootstrap completion before proceeding.
    let (bootstrap_tx, bootstrap_rx) = oneshot::channel();
    let handle = TokioHandle::current();

    let initial_timestamps = catalog.get_all_persisted_timestamps().await?;
    let metrics = Metrics::register_into(&metrics_registry);
    let metrics_clone = metrics.clone();
    let span = tracing::Span::current();
    let thread = thread::Builder::new()
        // The Coordinator thread tends to keep a lot of data on its stack. To
        // prevent a stack overflow we allocate a stack twice as big as the default
        // stack.
        .stack_size(2 * stack::STACK_SIZE)
        .name("coordinator".to_string())
        .spawn(move || {
            let mut timestamp_oracles = BTreeMap::new();
            for (timeline, initial_timestamp) in initial_timestamps {
                handle.block_on(Coordinator::ensure_timeline_state_with_initial_time(
                    &timeline,
                    initial_timestamp,
                    now.clone(),
                    |ts| catalog.persist_timestamp(&timeline, ts),
                    &mut timestamp_oracles,
                ));
            }

            let mut coord = Coordinator {
                controller: dataflow_client,
                view_optimizer: Optimizer::logical_optimizer(),
                catalog,
                internal_cmd_tx,
                strict_serializable_reads_tx,
                global_timelines: timestamp_oracles,
                transient_id_counter: 1,
                active_conns: BTreeMap::new(),
                storage_read_capabilities: Default::default(),
                compute_read_capabilities: Default::default(),
                txn_reads: Default::default(),
                pending_peeks: BTreeMap::new(),
                client_pending_peeks: BTreeMap::new(),
                pending_real_time_recency_timestamp: BTreeMap::new(),
                active_subscribes: BTreeMap::new(),
                write_lock: Arc::new(tokio::sync::Mutex::new(())),
                write_lock_wait_group: VecDeque::new(),
                pending_writes: Vec::new(),
                secrets_controller,
                cloud_resource_controller,
                connection_context,
                transient_replica_metadata: BTreeMap::new(),
                storage_usage_client,
                storage_usage_collection_interval,
                segment_client,
                metrics,
            };
            let bootstrap = handle.block_on(async {
                coord
                    .bootstrap(builtin_migration_metadata, builtin_table_updates)
                    .instrument(span)
                    .await?;
                coord
                    .controller
                    .remove_orphaned_replicas(coord.catalog.get_next_replica_id().await?)
                    .await
                    .map_err(AdapterError::Orchestrator)?;
                Ok(())
            });
            let ok = bootstrap.is_ok();
            bootstrap_tx
                .send(bootstrap)
                .expect("bootstrap_rx is not dropped until it receives this message");
            if ok {
                handle.block_on(coord.serve(internal_cmd_rx, strict_serializable_reads_rx, cmd_rx));
            }
        })
        .expect("failed to create coordinator thread");
    match bootstrap_rx
        .await
        .expect("bootstrap_tx always sends a message or panics/halts")
    {
        Ok(()) => {
            info!("coordinator init: complete");
            let handle = Handle {
                session_id,
                start_instant,
                _thread: thread.join_on_drop(),
            };
            let client = Client::new(build_info, cmd_tx.clone(), metrics_clone);
            Ok((handle, client))
        }
        Err(e) => Err(e),
    }
}
