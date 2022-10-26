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
use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::ops::Neg;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use derivative::Derivative;
use futures::StreamExt;
use itertools::Itertools;
use mz_ore::retry::Retry;
use rand::seq::SliceRandom;
use timely::progress::Timestamp as _;
use tokio::runtime::Handle as TokioHandle;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch, OwnedMutexGuard};
use tracing::{info, span, warn, Level};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_compute_client::command::ReplicaId;
use mz_compute_client::controller::{ComputeInstanceEvent, ComputeInstanceId};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::thread::JoinHandleExt;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{stack, task};
use mz_persist_client::usage::StorageUsageClient;
use mz_persist_client::ShardId;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_secrets::SecretsController;
use mz_sql::ast::{CreateSourceStatement, CreateSubsourceStatement, Raw, Statement};
use mz_sql::names::Aug;
use mz_sql::plan::{MutationKind, Params};
use mz_stash::Append;
use mz_storage::controller::{CollectionDescription, CreateExportToken, DataSource, StorageError};
use mz_storage::types::connections::ConnectionContext;
use mz_storage::types::sinks::StorageSinkConnection;
use mz_storage::types::sources::{IngestionDescription, SourceExport, Timeline};
use mz_transform::Optimizer;

use crate::catalog::builtin::{BUILTINS, MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS};
use crate::catalog::{
    self, storage, BuiltinMigrationMetadata, BuiltinTableUpdate, Catalog, CatalogItem,
    ClusterReplicaSizeMap, DataSourceDesc, StorageHostSizeMap, StorageSinkConnectionState,
};
use crate::client::{Client, ConnectionId, Handle};
use crate::command::{Canceled, Command, ExecuteResponse};
use crate::coord::appends::{BuiltinTableUpdateSource, Deferred, PendingWriteTxn};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::metrics::Metrics;
use crate::coord::peek::PendingPeek;
use crate::coord::read_policy::{ReadCapability, ReadHolds};
use crate::coord::timeline::{TimelineState, WriteTimestamp};
use crate::error::AdapterError;
use crate::session::{EndTransactionAction, Session};
use crate::subscribe::PendingSubscribe;
use crate::util::{ClientTransmitter, CompletedClientTransmitter};
use crate::AdapterNotice;

pub(crate) mod id_bundle;
pub(crate) mod peek;

mod appends;
mod command_handler;
mod dataflows;
mod ddl;
mod indexes;
mod message_handler;
mod metrics;
mod read_policy;
mod sequencer;
mod sql;
mod timeline;
mod timestamp_selection;

/// The default is set to a second to track the default timestamp frequency for sources.
pub const DEFAULT_LOGICAL_COMPACTION_WINDOW_MS: Option<mz_repr::Timestamp> =
    Some(Timestamp::new(1_000));

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
    ComputeInstanceStatus(ComputeInstanceEvent),
    RemovePendingPeeks {
        conn_id: ConnectionId,
    },
    LinearizeReads(Vec<PendingTxn>),
    StorageUsageFetch,
    StorageUsageUpdate(HashMap<Option<ShardId>, u64>),
    Consolidate(Vec<mz_stash::Id>),
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

/// Configures a coordinator.
pub struct Config<S> {
    pub dataflow_client: mz_controller::Controller,
    pub storage: storage::Connection<S>,
    pub unsafe_mode: bool,
    pub persisted_introspection: bool,
    pub build_info: &'static BuildInfo,
    pub environment_id: String,
    pub metrics_registry: MetricsRegistry,
    pub now: NowFn,
    pub secrets_controller: Arc<dyn SecretsController>,
    pub availability_zones: Vec<String>,
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    pub storage_host_sizes: StorageHostSizeMap,
    pub default_storage_host_size: Option<String>,
    pub bootstrap_system_vars: Option<String>,
    pub connection_context: ConnectionContext,
    pub storage_usage_client: StorageUsageClient,
    pub storage_usage_collection_interval: Duration,
    pub segment_api_key: Option<String>,
    pub egress_ips: Vec<Ipv4Addr>,
    pub consolidations_tx: mpsc::UnboundedSender<Vec<mz_stash::Id>>,
    pub consolidations_rx: mpsc::UnboundedReceiver<Vec<mz_stash::Id>>,
}

/// Soft-state metadata about a compute replica
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaMetadata {
    /// The last time we heard from this replica (possibly rounded)
    pub last_heartbeat: DateTime<Utc>,
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

    /// Channel on which to send notices to a session.
    notice_tx: mpsc::UnboundedSender<AdapterNotice>,
}

struct TxnReads {
    // True iff all statements run so far in the transaction are independent
    // of the chosen logical timestamp (not the PlanContext walltime). This
    // happens if both 1) there are no referenced sources or indexes and 2)
    // `mz_now()` is not present.
    timestamp_independent: bool,
    read_holds: crate::coord::read_policy::ReadHolds<mz_repr::Timestamp>,
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

/// Glues the external world to the Timely workers.
pub struct Coordinator<S> {
    /// The controller for the storage and compute layers.
    controller: mz_controller::Controller,
    /// Optimizer instance for logical optimization of views.
    view_optimizer: Optimizer,
    catalog: Catalog<S>,

    /// Channel to manage internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,

    /// Channel for strict serializable reads ready to commit.
    strict_serializable_reads_tx: mpsc::UnboundedSender<PendingTxn>,

    /// Channel for catalog stash consolidations.
    consolidations_tx: mpsc::UnboundedSender<Vec<mz_stash::Id>>,

    /// Mechanism for totally ordering write and read timestamps, so that all reads
    /// reflect exactly the set of writes that precede them, and no writes that follow.
    global_timelines: BTreeMap<Timeline, TimelineState<Timestamp>>,

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
    ///
    /// Access to this field should be restricted to methods in the [`read_policy`] API.
    read_capability: HashMap<GlobalId, ReadCapability<mz_repr::Timestamp>>,
    /// For each transaction, the pinned storage and compute identifiers and time at
    /// which they are pinned.
    ///
    /// Upon completing a transaction, this timestamp should be removed from the holds
    /// in `self.read_capability[id]`, using the `release_read_holds` method.
    txn_reads: HashMap<ConnectionId, TxnReads>,

    /// Access to the peek fields should be restricted to methods in the [`peek`] API.
    /// A map from pending peek ids to the queue into which responses are sent, and
    /// the connection id of the client that initiated the peek.
    pending_peeks: HashMap<Uuid, PendingPeek>,
    /// A map from client connection ids to a set of all pending peeks for that client
    client_pending_peeks: HashMap<ConnectionId, BTreeMap<Uuid, ComputeInstanceId>>,

    /// A map from pending subscribes to the subscribe description.
    pending_subscribes: HashMap<GlobalId, PendingSubscribe>,

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

    /// Persist client for fetching storage metadata such as size metrics.
    storage_usage_client: StorageUsageClient,
    /// The interval at which to collect storage usage information.
    storage_usage_collection_interval: Duration,

    /// Segment analytics client.
    segment_client: Option<mz_segment::Client>,

    /// Coordinator metrics.
    metrics: Metrics,
}

impl<S: Append + 'static> Coordinator<S> {
    /// Initializes coordinator state based on the contained catalog. Must be
    /// called after creating the coordinator and before calling the
    /// `Coordinator::serve` method.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn bootstrap(
        &mut self,
        builtin_migration_metadata: BuiltinMigrationMetadata,
        mut builtin_table_updates: Vec<BuiltinTableUpdate>,
    ) -> Result<(), AdapterError> {
        info!("coordinator init: beginning bootstrap");

        // Capture identifiers that need to have their read holds relaxed once the bootstrap completes.
        let mut policies_to_set: CollectionIdBundle = Default::default();

        info!("coordinator init: creating compute replicas");
        for instance in self.catalog.compute_instances() {
            self.controller.compute.create_instance(
                instance.id,
                instance.log_indexes.clone(),
                self.catalog.system_config().max_result_size(),
            )?;
            for (replica_id, replica) in instance.replicas_by_id.clone() {
                let introspection_collections = replica
                    .config
                    .logging
                    .sources
                    .iter()
                    .map(|(variant, id)| (*id, variant.desc().into()))
                    .collect();

                // Create collections does not recreate existing collections, so it is safe to
                // always call it.
                self.controller
                    .storage
                    .create_collections(introspection_collections)
                    .await
                    .unwrap();

                policies_to_set
                    .compute_ids
                    .entry(instance.id)
                    .or_insert_with(BTreeSet::new)
                    .extend(replica.config.logging.source_ids());
                policies_to_set
                    .storage_ids
                    .extend(replica.config.logging.source_ids());

                self.controller
                    .active_compute()
                    .add_replica_to_instance(instance.id, replica_id, replica.config)
                    .await
                    .unwrap();
            }
        }

        info!("coordinator init: migrating builtin objects");
        // Migrate builtin objects.
        self.controller
            .storage
            .drop_sources_unvalidated(builtin_migration_metadata.previous_materialized_view_ids)
            .await?;
        self.controller
            .storage
            .drop_sources_unvalidated(builtin_migration_metadata.previous_source_ids)
            .await?;
        self.controller
            .storage
            .drop_sinks_unvalidated(builtin_migration_metadata.previous_sink_ids)
            .await?;

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

        // This is disabled for the moment because it has unusual upper
        // advancement behavior.
        // See: https://materializeinc.slack.com/archives/C01CFKM1QRF/p1660726837927649
        let status_collection_id = if false {
            Some(self.catalog.resolve_builtin_storage_collection(
                &crate::catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
            ))
        } else {
            None
        };

        info!("coordinator init: installing existing objects in catalog");
        for entry in &entries {
            info!(
                "coordinator init: installing {} {}",
                entry.item().typ(),
                entry.id()
            );
            match entry.item() {
                // Currently catalog item rebuild assumes that sinks and
                // indexes are always built individually and does not store information
                // about how it was built. If we start building multiple sinks and/or indexes
                // using a single dataflow, we have to make sure the rebuild process re-runs
                // the same multiple-build dataflow.
                CatalogItem::Source(source) => {
                    let data_source = match &source.data_source {
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
                            source_exports.insert(entry.id(), main_export);
                            for (subsource, output_index) in ingestion.subsource_exports.clone() {
                                let export = SourceExport {
                                    output_index,
                                    storage_metadata: (),
                                };
                                source_exports.insert(subsource, export);
                            }

                            DataSource::Ingestion(IngestionDescription {
                                desc: ingestion.desc.clone(),
                                ingestion_metadata: (),
                                source_imports,
                                source_exports,
                                host_config: ingestion.host_config.clone(),
                            })
                        }
                        DataSourceDesc::Source => DataSource::Other,
                        DataSourceDesc::Introspection(introspection) => {
                            DataSource::Introspection(*introspection)
                        }
                    };

                    self.controller
                        .storage
                        .create_collections(vec![(
                            entry.id(),
                            CollectionDescription {
                                desc: source.desc.clone(),
                                data_source,
                                since: None,
                                status_collection_id,
                            },
                        )])
                        .await
                        .unwrap();

                    policies_to_set.storage_ids.insert(entry.id());
                }
                CatalogItem::Table(table) => {
                    let collection_desc = table.desc.clone().into();
                    self.controller
                        .storage
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
                            .active_compute()
                            .create_dataflows(idx.compute_instance, dataflow_plan)
                            .await
                            .unwrap();
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
                        .unwrap();

                    policies_to_set.storage_ids.insert(entry.id());

                    // Re-create the sink on the compute instance.
                    let id_bundle = self
                        .index_oracle(mview.compute_instance)
                        .sufficient_collections(&mview.depends_on);
                    let as_of = self.least_valid_read(&id_bundle);
                    let internal_view_id = self.allocate_transient_id()?;
                    let df = self
                        .dataflow_builder(mview.compute_instance)
                        .build_materialized_view_dataflow(entry.id(), as_of, internal_view_id)?;
                    self.ship_dataflow(df, mview.compute_instance).await;
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
                        .await
                        .unwrap();

                    task::spawn(
                        || format!("sink_connection_ready:{}", sink.from),
                        async move {
                            let conn_result = Retry::default()
                                .max_tries(usize::MAX)
                                .clamp_backoff(Duration::from_secs(60 * 10))
                                .retry_async(|_| async {
                                    let builder = builder.clone();
                                    let connection_context = connection_context.clone();
                                    mz_storage::sink::build_sink_connection(
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
                        let parent_log =
                            BUILTINS::logs().find(|src| src.variant == parent).unwrap();
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
            .unwrap();

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
                .unwrap();
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
        mut strict_serializable_reads_rx: mpsc::UnboundedReceiver<PendingTxn>,
        mut cmd_rx: mpsc::UnboundedReceiver<Command>,
        mut consolidations_rx: mpsc::UnboundedReceiver<Vec<mz_stash::Id>>,
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
        // Watcher that listens for and reports compute service status changes.
        let mut compute_events = self.controller.compute.watch_services();

        self.schedule_storage_usage_collection().await;

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
                // `recv()` on `UnboundedReceiver` is cancellation safe:
                // https://docs.rs/tokio/1.8.0/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety
                Some(collections) = consolidations_rx.recv() => {
                    let mut ids:HashSet<mz_stash::Id> = HashSet::from_iter(collections);
                    while let Ok(collections) = consolidations_rx.try_recv() {
                        ids.extend(collections);
                    }
                    Message::Consolidate(ids.into_iter().collect())
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
pub async fn serve<S: Append + 'static>(
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
        cluster_replica_sizes,
        storage_host_sizes,
        default_storage_host_size,
        bootstrap_system_vars,
        mut availability_zones,
        connection_context,
        storage_usage_client,
        storage_usage_collection_interval,
        segment_api_key,
        egress_ips,
        consolidations_tx,
        consolidations_rx,
    }: Config<S>,
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

    info!("coordinator init: opening catalog");
    let (mut catalog, builtin_migration_metadata, builtin_table_updates) =
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
            storage_host_sizes,
            default_storage_host_size,
            bootstrap_system_vars,
            availability_zones,
            secrets_reader: secrets_controller.reader(),
            egress_ips,
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
                        move || (now)().into(),
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

            let segment_client =
                handle.block_on(async { segment_api_key.map(mz_segment::Client::new) });

            let mut coord = Coordinator {
                controller: dataflow_client,
                view_optimizer: Optimizer::logical_optimizer(),
                catalog,
                internal_cmd_tx,
                strict_serializable_reads_tx,
                consolidations_tx,
                global_timelines: timestamp_oracles,
                transient_id_counter: 1,
                active_conns: HashMap::new(),
                read_capability: Default::default(),
                txn_reads: Default::default(),
                pending_peeks: HashMap::new(),
                client_pending_peeks: HashMap::new(),
                pending_subscribes: HashMap::new(),
                write_lock: Arc::new(tokio::sync::Mutex::new(())),
                write_lock_wait_group: VecDeque::new(),
                pending_writes: Vec::new(),
                secrets_controller,
                connection_context,
                transient_replica_metadata: HashMap::new(),
                storage_usage_client,
                storage_usage_collection_interval,
                segment_client,
                metrics: Metrics::register_with(&metrics_registry),
            };
            let bootstrap =
                handle.block_on(coord.bootstrap(builtin_migration_metadata, builtin_table_updates));
            let ok = bootstrap.is_ok();
            bootstrap_tx.send(bootstrap).unwrap();
            if ok {
                handle.block_on(coord.serve(
                    internal_cmd_rx,
                    strict_serializable_reads_rx,
                    cmd_rx,
                    consolidations_rx,
                ));
            }
        })
        .unwrap();
    match bootstrap_rx.await.unwrap() {
        Ok(()) => {
            info!("coordinator init: complete");
            let handle = Handle {
                session_id,
                start_instant,
                _thread: thread.join_on_drop(),
            };
            let client = Client::new(cmd_tx.clone());
            Ok((handle, client))
        }
        Err(e) => Err(e),
    }
}
