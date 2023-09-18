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
use std::ops::Neg;
use std::sync::{atomic, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use fail::fail_point;
use futures::StreamExt;
use itertools::Itertools;
use mz_build_info::BuildInfo;
use mz_cloud_resources::{CloudResourceController, VpcEndpointConfig};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::ComputeInstanceId;
use mz_controller::clusters::{ClusterConfig, ClusterEvent, CreateReplicaConfig};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::{MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, RowSetFinishing};
use mz_orchestrator::ServiceProcessMetrics;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{MetricsFutureExt, MetricsRegistry};
use mz_ore::notify::{notifier, NotifyPermit, NotifyReceiver, NotifySender};
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::retry::Retry;
use mz_ore::task::spawn;
use mz_ore::thread::JoinHandleExt;
use mz_ore::tracing::{OpenTelemetryContext, TracingHandle};
use mz_ore::{soft_assert_or_log, stack, task};
use mz_persist_client::usage::{ShardsUsageReferenced, StorageUsageClient};
use mz_repr::explain::ExplainFormat;
use mz_repr::role_id::RoleId;
use mz_repr::{Datum, GlobalId, RelationType, Row, Timestamp};
use mz_secrets::cache::CachingSecretsReader;
use mz_secrets::SecretsController;
use mz_sql::ast::{CreateSubsourceStatement, Raw, Statement};
use mz_sql::catalog::EnvironmentId;
use mz_sql::names::{Aug, ResolvedIds};
use mz_sql::plan::{CopyFormat, CreateConnectionPlan, Params, QueryWhen};
use mz_sql::rbac::UnauthorizedError;
use mz_sql::session::user::{RoleMetadata, User};
use mz_sql::session::vars::ConnectionCounter;
use mz_storage_client::controller::{
    CollectionDescription, CreateExportToken, DataSource, DataSourceOther,
};
use mz_storage_types::connections::inline::{IntoInlineConnection, ReferencedConnection};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::StorageError;
use mz_storage_types::sinks::StorageSinkConnection;
use mz_storage_types::sources::Timeline;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::Optimizer;
use timely::progress::Antichain;
use tokio::runtime::Handle as TokioHandle;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch, OwnedMutexGuard};
use tracing::{debug, info, info_span, span, warn, Instrument, Level, Span};
use uuid::Uuid;

use crate::catalog::builtin::{BUILTINS, MZ_VIEW_FOREIGN_KEYS, MZ_VIEW_KEYS};
use crate::catalog::{
    self, AwsPrincipalContext, BuiltinMigrationMetadata, BuiltinTableUpdate, Catalog, CatalogItem,
    ClusterReplicaSizeMap, DataSourceDesc, Source, StorageSinkConnectionState,
};
use crate::client::{Client, ConnectionId, Handle};
use crate::command::{Canceled, Command, ExecuteResponse};
use crate::config::SystemParameterSyncConfig;
use crate::coord::appends::{Deferred, PendingWriteTxn};
use crate::coord::dataflows::dataflow_import_id_bundle;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::peek::PendingPeek;
use crate::coord::read_policy::ReadCapability;
use crate::coord::timeline::{TimelineContext, TimelineState, WriteTimestamp};
use crate::coord::timestamp_selection::TimestampContext;
use crate::error::AdapterError;
use crate::metrics::Metrics;
use crate::session::{EndTransactionAction, Session};
use crate::statement_logging::StatementEndedExecutionReason;
use crate::subscribe::ActiveSubscribe;
use crate::util::{ClientTransmitter, CompletedClientTransmitter, ComputeSinkId, ResultExt};
use crate::{flags, AdapterNotice, TimestampProvider};

pub(crate) mod dataflows;
use self::statement_logging::{StatementLogging, StatementLoggingId};

pub(crate) mod id_bundle;
pub(crate) mod peek;
pub(crate) mod statement_logging;
pub(crate) mod timeline;
pub(crate) mod timestamp_selection;

mod appends;
mod command_handler;
mod ddl;
mod indexes;
mod introspection;
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

#[derive(Debug)]
pub enum Message<T = mz_repr::Timestamp> {
    Command(Command),
    ControllerReady,
    PurifiedStatementReady(PurifiedStatementReady),
    CreateConnectionValidationReady(CreateConnectionValidationReady),
    SinkConnectionReady(SinkConnectionReady),
    WriteLockGrant(tokio::sync::OwnedMutexGuard<()>),
    /// Initiates a group commit.
    GroupCommitInitiate(Span, Option<NotifyPermit>),
    /// Makes a group commit visible to all clients.
    GroupCommitApply(
        /// Timestamp of the writes in the group commit.
        T,
        /// Clients waiting on responses from the group commit.
        Vec<CompletedClientTransmitter>,
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
    StorageUsageUpdate(ShardsUsageReferenced),
    RealTimeRecencyTimestamp {
        conn_id: ConnectionId,
        real_time_recency_ts: Timestamp,
        validity: PlanValidity,
    },

    /// Performs any cleanup and logging actions necessary for
    /// finalizing a statement execution.
    RetireExecute {
        data: ExecuteContextExtra,
        reason: StatementEndedExecutionReason,
    },
    ExecuteSingleStatementTransaction {
        ctx: ExecuteContext,
        stmt: Statement<Raw>,
        params: mz_sql::plan::Params,
    },
    PeekStageReady {
        ctx: ExecuteContext,
        stage: PeekStage,
    },
    DrainStatementLog,
}

impl Message {
    /// Returns a string to identify the kind of [`Message`], useful for logging.
    pub const fn kind(&self) -> &'static str {
        use Message::*;

        match self {
            Command(_) => "command",
            ControllerReady => "controller_ready",
            PurifiedStatementReady(_) => "purified_statement_ready",
            CreateConnectionValidationReady(_) => "create_connection_validation_ready",
            SinkConnectionReady(_) => "sink_connection_ready",
            WriteLockGrant(_) => "write_lock_grant",
            GroupCommitInitiate(..) => "group_commit_initiate",
            GroupCommitApply(..) => "group_commit_apply",
            AdvanceTimelines => "advance_timelines",
            ClusterEvent(_) => "cluster_event",
            RemovePendingPeeks { .. } => "remove_pending_peeks",
            LinearizeReads(_) => "linearize_reads",
            StorageUsageFetch => "storage_usage_fetch",
            StorageUsageUpdate(_) => "storage_usage_update",
            RealTimeRecencyTimestamp { .. } => "real_time_recency_timestamp",
            RetireExecute { .. } => "retire_execute",
            ExecuteSingleStatementTransaction { .. } => "execute_single_statement_transaction",
            PeekStageReady { .. } => "peek_stage_ready",
            DrainStatementLog => "drain_statement_log",
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct BackgroundWorkResult<T> {
    #[derivative(Debug = "ignore")]
    pub ctx: ExecuteContext,
    pub result: Result<T, AdapterError>,
    pub params: Params,
    pub resolved_ids: ResolvedIds,
    pub original_stmt: Statement<Raw>,
    pub otel_ctx: OpenTelemetryContext,
}

pub type PurifiedStatementReady = BackgroundWorkResult<(
    Vec<(GlobalId, CreateSubsourceStatement<Aug>)>,
    Statement<Aug>,
)>;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct CreateConnectionValidationReady {
    #[derivative(Debug = "ignore")]
    pub ctx: ExecuteContext,
    pub result: Result<CreateConnectionPlan, AdapterError>,
    pub connection_gid: GlobalId,
    pub plan_validity: PlanValidity,
    pub otel_ctx: OpenTelemetryContext,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SinkConnectionReady {
    #[derivative(Debug = "ignore")]
    pub ctx: Option<ExecuteContext>,
    pub id: GlobalId,
    pub oid: u32,
    pub create_export_token: CreateExportToken,
    pub result: Result<StorageSinkConnection<ReferencedConnection>, AdapterError>,
}

#[derive(Debug)]
pub enum RealTimeRecencyContext {
    ExplainTimestamp {
        ctx: ExecuteContext,
        format: ExplainFormat,
        cluster_id: ClusterId,
        optimized_plan: OptimizedMirRelationExpr,
        id_bundle: CollectionIdBundle,
    },
    Peek {
        ctx: ExecuteContext,
        finishing: RowSetFinishing,
        copy_to: Option<CopyFormat>,
        dataflow: DataflowDescription<OptimizedMirRelationExpr>,
        cluster_id: ClusterId,
        when: QueryWhen,
        target_replica: Option<ReplicaId>,
        view_id: GlobalId,
        index_id: GlobalId,
        timeline_context: TimelineContext,
        source_ids: BTreeSet<GlobalId>,
        in_immediate_multi_stmt_txn: bool,
        key: Vec<MirScalarExpr>,
        typ: RelationType,
        dataflow_metainfo: DataflowMetainfo,
    },
}

impl RealTimeRecencyContext {
    pub(crate) fn take_context(self) -> ExecuteContext {
        match self {
            RealTimeRecencyContext::ExplainTimestamp { ctx, .. }
            | RealTimeRecencyContext::Peek { ctx, .. } => ctx,
        }
    }
}

#[derive(Debug)]
pub enum PeekStage {
    Validate(PeekStageValidate),
    Optimize(PeekStageOptimize),
    Timestamp(PeekStageTimestamp),
    Finish(PeekStageFinish),
}

impl PeekStage {
    fn validity(&mut self) -> Option<&mut PlanValidity> {
        match self {
            PeekStage::Validate(_) => None,
            PeekStage::Optimize(PeekStageOptimize { validity, .. })
            | PeekStage::Timestamp(PeekStageTimestamp { validity, .. })
            | PeekStage::Finish(PeekStageFinish { validity, .. }) => Some(validity),
        }
    }
}

#[derive(Debug)]
pub struct PeekStageValidate {
    plan: mz_sql::plan::SelectPlan,
    target_cluster: TargetCluster,
}

#[derive(Debug)]
pub struct PeekStageOptimize {
    validity: PlanValidity,
    source: MirRelationExpr,
    finishing: RowSetFinishing,
    copy_to: Option<CopyFormat>,
    view_id: GlobalId,
    index_id: GlobalId,
    source_ids: BTreeSet<GlobalId>,
    cluster_id: ClusterId,
    when: QueryWhen,
    target_replica: Option<ReplicaId>,
    timeline_context: TimelineContext,
    in_immediate_multi_stmt_txn: bool,
}

#[derive(Debug)]
pub struct PeekStageTimestamp {
    validity: PlanValidity,
    dataflow: DataflowDescription<OptimizedMirRelationExpr>,
    finishing: RowSetFinishing,
    copy_to: Option<CopyFormat>,
    view_id: GlobalId,
    index_id: GlobalId,
    source_ids: BTreeSet<GlobalId>,
    cluster_id: ClusterId,
    id_bundle: CollectionIdBundle,
    when: QueryWhen,
    target_replica: Option<ReplicaId>,
    timeline_context: TimelineContext,
    in_immediate_multi_stmt_txn: bool,
    key: Vec<MirScalarExpr>,
    typ: RelationType,
    dataflow_metainfo: DataflowMetainfo,
}

#[derive(Debug)]
pub struct PeekStageFinish {
    validity: PlanValidity,
    finishing: RowSetFinishing,
    copy_to: Option<CopyFormat>,
    dataflow: DataflowDescription<OptimizedMirRelationExpr>,
    cluster_id: ClusterId,
    id_bundle: Option<CollectionIdBundle>,
    when: QueryWhen,
    target_replica: Option<ReplicaId>,
    view_id: GlobalId,
    index_id: GlobalId,
    timeline_context: TimelineContext,
    source_ids: BTreeSet<GlobalId>,
    real_time_recency_ts: Option<mz_repr::Timestamp>,
    key: Vec<MirScalarExpr>,
    typ: RelationType,
    dataflow_metainfo: DataflowMetainfo,
}

/// An enum describing which cluster to run a statement on.
///
/// One example usage would be that if a query depends only on system tables, we might
/// automatically run it on the introspection cluster to benefit from indexes that exist there.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TargetCluster {
    /// The introspection cluster.
    Introspection,
    /// The current user's active cluster.
    Active,
}

/// A struct to hold information about the validity of plans and if they should be abandoned after
/// doing work off of the Coordinator thread.
#[derive(Debug)]
pub struct PlanValidity {
    /// The most recent revision at which this plan was verified as valid.
    transient_revision: u64,
    /// Objects on which the plan depends.
    dependency_ids: BTreeSet<GlobalId>,
    cluster_id: Option<ComputeInstanceId>,
    replica_id: Option<ReplicaId>,
    role_metadata: RoleMetadata,
}

impl PlanValidity {
    /// Returns an error if the current catalog no longer has all dependencies.
    fn check(&mut self, catalog: &Catalog) -> Result<(), AdapterError> {
        if self.transient_revision == catalog.transient_revision() {
            return Ok(());
        }
        // If the transient revision changed, we have to recheck. If successful, bump the revision
        // so next check uses the above fast path.
        if let Some(cluster_id) = self.cluster_id {
            let Some(cluster) = catalog.try_get_cluster(cluster_id) else {
                return Err(AdapterError::ChangedPlan);
            };

            if let Some(replica_id) = self.replica_id {
                if !cluster.replicas_by_id.contains_key(&replica_id) {
                    return Err(AdapterError::ChangedPlan);
                }
            }
        }
        // It is sufficient to check that all the source_ids still exist because we assume:
        // - Ids do not mutate.
        // - Ids are not reused.
        // - If an id was dropped, this will detect it and error.
        for id in &self.dependency_ids {
            if catalog.try_get_entry(id).is_none() {
                return Err(AdapterError::ChangedPlan);
            }
        }
        if catalog
            .try_get_role(&self.role_metadata.current_role)
            .is_none()
        {
            return Err(AdapterError::Unauthorized(
                UnauthorizedError::ConcurrentRoleDrop(self.role_metadata.current_role.clone()),
            ));
        }
        if catalog
            .try_get_role(&self.role_metadata.session_role)
            .is_none()
        {
            return Err(AdapterError::Unauthorized(
                UnauthorizedError::ConcurrentRoleDrop(self.role_metadata.session_role.clone()),
            ));
        }

        if catalog
            .try_get_role(&self.role_metadata.authenticated_role)
            .is_none()
        {
            return Err(AdapterError::Unauthorized(
                UnauthorizedError::ConcurrentRoleDrop(
                    self.role_metadata.authenticated_role.clone(),
                ),
            ));
        }
        self.transient_revision = catalog.transient_revision();
        Ok(())
    }
}

/// Configures a coordinator.
pub struct Config {
    pub dataflow_client: mz_controller::Controller,
    pub storage: mz_catalog::Connection,
    pub unsafe_mode: bool,
    pub all_features: bool,
    pub build_info: &'static BuildInfo,
    pub environment_id: EnvironmentId,
    pub metrics_registry: MetricsRegistry,
    pub now: NowFn,
    pub secrets_controller: Arc<dyn SecretsController>,
    pub cloud_resource_controller: Option<Arc<dyn CloudResourceController>>,
    pub availability_zones: Vec<String>,
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    pub default_storage_cluster_size: Option<String>,
    pub system_parameter_defaults: BTreeMap<String, String>,
    pub connection_context: ConnectionContext,
    pub storage_usage_client: StorageUsageClient,
    pub storage_usage_collection_interval: Duration,
    pub storage_usage_retention_period: Option<Duration>,
    pub segment_client: Option<mz_segment::Client>,
    pub egress_ips: Vec<Ipv4Addr>,
    pub system_parameter_sync_config: Option<SystemParameterSyncConfig>,
    pub aws_account_id: Option<String>,
    pub aws_privatelink_availability_zones: Option<Vec<String>>,
    pub active_connection_count: Arc<Mutex<ConnectionCounter>>,
    pub http_host_name: Option<String>,
    pub tracing_handle: TracingHandle,
}

/// Soft-state metadata about a compute replica
#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct ReplicaMetadata {
    /// The last time we heard from this replica (possibly rounded)
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// The last known CPU and memory metrics
    pub metrics: Option<Vec<ServiceProcessMetrics>>,
}

/// Metadata about an active connection.
#[derive(Debug)]
pub struct ConnMeta {
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
    /// The time when the session's connection was initiated.
    connected_at: EpochMillis,
    user: User,
    application_name: String,
    uuid: Uuid,
    conn_id: ConnectionId,

    /// Sinks that will need to be dropped when the current transaction, if
    /// any, is cleared.
    drop_sinks: Vec<ComputeSinkId>,

    /// Channel on which to send notices to a session.
    notice_tx: mpsc::UnboundedSender<AdapterNotice>,

    /// The role that initiated the database context. Fixed for the duration of the connection.
    /// WARNING: This role reference is not updated when the role is dropped.
    /// Consumers should not assume that this role exist.
    authenticated_role: RoleId,
}

impl ConnMeta {
    pub fn conn_id(&self) -> &ConnectionId {
        &self.conn_id
    }

    pub fn user(&self) -> &User {
        &self.user
    }

    pub fn application_name(&self) -> &str {
        &self.application_name
    }

    pub fn authenticated_role_id(&self) -> &RoleId {
        &self.authenticated_role
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn connected_at(&self) -> EpochMillis {
        self.connected_at
    }
}

#[derive(Debug)]
/// A pending transaction waiting to be committed.
pub struct PendingTxn {
    /// Context used to send a response back to the client.
    ctx: ExecuteContext,
    /// Client response for transaction.
    response: Result<PendingTxnResponse, AdapterError>,
    /// The action to take at the end of the transaction.
    action: EndTransactionAction,
}

#[derive(Debug)]
/// The response we'll send for a [`PendingTxn`].
pub enum PendingTxnResponse {
    /// The transaction will be committed.
    Committed {
        /// Parameters that will change, and their values, once this transaction is complete.
        params: BTreeMap<&'static str, String>,
    },
    /// The transaction will be rolled back.
    Rolledback {
        /// Parameters that will change, and their values, once this transaction is complete.
        params: BTreeMap<&'static str, String>,
    },
}

impl PendingTxnResponse {
    pub fn extend_params(&mut self, p: impl IntoIterator<Item = (&'static str, String)>) {
        match self {
            PendingTxnResponse::Committed { params }
            | PendingTxnResponse::Rolledback { params } => params.extend(p),
        }
    }
}

impl From<PendingTxnResponse> for ExecuteResponse {
    fn from(value: PendingTxnResponse) -> Self {
        match value {
            PendingTxnResponse::Committed { params } => {
                ExecuteResponse::TransactionCommitted { params }
            }
            PendingTxnResponse::Rolledback { params } => {
                ExecuteResponse::TransactionRolledBack { params }
            }
        }
    }
}

#[derive(Debug)]
/// A pending read transaction waiting to be linearized along with metadata about it's state
pub struct PendingReadTxn {
    /// The transaction type
    txn: PendingRead,
    /// When we created this pending txn, when the transaction ends. Only used for metrics.
    created: Instant,
    /// Number of times we requeued the processing of this pending read txn.
    /// Requeueing is necessary if the time we executed the query is after the current oracle time;
    /// see [`Coordinator::message_linearize_reads`] for more details.
    num_requeues: u64,
}

#[derive(Debug)]
/// A pending read transaction waiting to be linearized.
enum PendingRead {
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

impl PendingRead {
    /// Return the timestamp context of the pending read transaction.
    pub fn timestamp_context(&self) -> TimestampContext<mz_repr::Timestamp> {
        match &self {
            PendingRead::Read {
                timestamp_context, ..
            } => timestamp_context.clone(),
            PendingRead::ReadThenWrite {
                timestamp: (timestamp, timeline),
                ..
            } => TimestampContext::TimelineTimestamp(timeline.clone(), timestamp.clone()),
        }
    }

    /// Alert the client that the read has been linearized.
    ///
    /// If it is necessary to finalize an execute, return the state necessary to do so
    /// (execution context and result)
    pub fn finish(self) -> Option<(ExecuteContext, Result<ExecuteResponse, AdapterError>)> {
        match self {
            PendingRead::Read {
                txn:
                    PendingTxn {
                        mut ctx,
                        response,
                        action,
                    },
                ..
            } => {
                let changed = ctx.session_mut().vars_mut().end_transaction(action);
                // Append any parameters that changed to the response.
                let response = response.map(|mut r| {
                    r.extend_params(changed);
                    ExecuteResponse::from(r)
                });

                Some((ctx, response))
            }
            PendingRead::ReadThenWrite { tx, .. } => {
                // Ignore errors if the caller has hung up.
                let _ = tx.send(());
                None
            }
        }
    }

    fn label(&self) -> &'static str {
        match self {
            PendingRead::Read { .. } => "read",
            PendingRead::ReadThenWrite { .. } => "read_then_write",
        }
    }
}

/// State that the coordinator must process as part of retiring
/// command execution.  `ExecuteContextExtra::Default` is guaranteed
/// to produce a value that will cause the coordinator to do nothing, and
/// is intended for use by code that invokes the execution processing flow
/// (i.e., `sequence_plan`) without actually being a statement execution.
///
/// This struct must not be dropped if it contains non-trivial
/// state. The only valid way to get rid of it is to pass it to the
/// coordinator for retirement. To enforce this, we assert in the
/// `Drop` implementation.
#[derive(Debug, Default)]
#[must_use]
pub struct ExecuteContextExtra {
    statement_uuid: Option<StatementLoggingId>,
}

impl ExecuteContextExtra {
    pub(crate) fn new(statement_uuid: Option<StatementLoggingId>) -> Self {
        Self { statement_uuid }
    }
    pub fn is_trivial(&self) -> bool {
        let Self { statement_uuid } = self;
        statement_uuid.is_none()
    }
    /// Take responsibility for the contents.  This should only be
    /// called from code that knows what to do to finish up logging
    /// based on the inner value.
    #[must_use]
    fn retire(mut self) -> Option<StatementLoggingId> {
        let Self { statement_uuid } = &mut self;
        statement_uuid.take()
    }
}

impl Drop for ExecuteContextExtra {
    fn drop(&mut self) {
        let Self { statement_uuid } = &*self;
        soft_assert_or_log!(
            statement_uuid.is_none(),
            "execute context dropped without being properly retired."
        )
    }
}

/// Bundle of state related to statement execution.
///
/// This struct collects a bundle of state that needs to be threaded
/// through various functions as part of statement execution.
/// Currently, it is only used to finalize execution, by calling one
/// of the methods `retire` or `retire_aysnc`. Finalizing execution
/// involves sending the session back to the pgwire layer so that it
/// may be used to process further commands. In the future, it will
/// also involve performing some work on the main coordinator thread
/// (e.g., recording the time at which the statement finished
/// executing) the state necessary to perform this work is bundled in
/// the `ExecuteContextExtra` object (today, it is simply empty).
#[derive(Debug)]
pub struct ExecuteContext {
    tx: ClientTransmitter<ExecuteResponse>,
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    session: Session,
    extra: ExecuteContextExtra,
}

impl ExecuteContext {
    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut Session {
        &mut self.session
    }

    pub fn tx(&self) -> &ClientTransmitter<ExecuteResponse> {
        &self.tx
    }

    pub fn tx_mut(&mut self) -> &mut ClientTransmitter<ExecuteResponse> {
        &mut self.tx
    }

    pub fn from_parts(
        tx: ClientTransmitter<ExecuteResponse>,
        internal_cmd_tx: mpsc::UnboundedSender<Message>,
        session: Session,
        extra: ExecuteContextExtra,
    ) -> Self {
        Self {
            tx,
            session,
            extra,
            internal_cmd_tx,
        }
    }

    /// By calling this function, the caller takes responsibility for
    /// dealing with the instance of `ExecuteContextExtra`. This is
    /// intended to support protocols (like `COPY FROM`) that involve
    /// multiple passes of sending the session back and forth between
    /// the coordinator and the pgwire layer. As part of any such
    /// protocol, we must ensure that the `ExecuteContextExtra`
    /// (possibly wrapped in a new `ExecuteContext`) is passed back to the coordinator for
    /// eventual retirement.
    pub fn into_parts(
        self,
    ) -> (
        ClientTransmitter<ExecuteResponse>,
        mpsc::UnboundedSender<Message>,
        Session,
        ExecuteContextExtra,
    ) {
        let Self {
            tx,
            internal_cmd_tx,
            session,
            extra,
        } = self;
        (tx, internal_cmd_tx, session, extra)
    }

    /// Retire the execution, by sending a message to the coordinator.
    pub fn retire(self, result: Result<ExecuteResponse, AdapterError>) {
        let Self {
            tx,
            internal_cmd_tx,
            session,
            extra,
        } = self;
        let reason = if extra.is_trivial() {
            None
        } else {
            Some((&result).into())
        };
        tx.send(result, session);
        if let Some(reason) = reason {
            if let Err(e) = internal_cmd_tx.send(Message::RetireExecute {
                data: extra,
                reason,
            }) {
                warn!("internal_cmd_rx dropped before we could send: {:?}", e);
            }
        }
    }

    pub fn extra(&self) -> &ExecuteContextExtra {
        &self.extra
    }

    pub fn extra_mut(&mut self) -> &mut ExecuteContextExtra {
        &mut self.extra
    }
}

/// Glues the external world to the Timely workers.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Coordinator {
    /// The controller for the storage and compute layers.
    #[derivative(Debug = "ignore")]
    controller: mz_controller::Controller,
    /// Optimizer instance for logical optimization of views.
    view_optimizer: Optimizer,
    /// The catalog in an Arc suitable for readonly references. The Arc allows
    /// us to hand out cheap copies of the catalog to functions that can use it
    /// off of the main coordinator thread. If the coordinator needs to mutate
    /// the catalog, call catalog_mut(), which will clone this struct member,
    /// allowing it to be mutated here while the other off-thread references can
    /// read their catalog as long as needed. In the future we would like this
    /// to be a pTVC, but for now this is sufficient.
    catalog: Arc<Catalog>,

    /// Channel to manage internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    /// Notification that triggers a group commit.
    group_commit_tx: NotifySender,

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
    /// For the realtime timeline, an explicit SELECT or INSERT on a table will bump the
    /// table's timestamps, but there are cases where timestamps are not bumped but
    /// we expect the closed timestamps to advance (`AS OF X`, SUBSCRIBing views over
    /// RT sources and tables). To address these, spawn a task that forces table
    /// timestamps to close on a regular interval. This roughly tracks the behavior
    /// of realtime sources that close off timestamps on an interval.
    ///
    /// For non-realtime timelines, nothing pushes the timestamps forward, so we must do
    /// it manually.
    advance_timelines_interval: tokio::time::Interval,

    /// Handle to secret manager that can create and delete secrets from
    /// an arbitrary secret storage engine.
    secrets_controller: Arc<dyn SecretsController>,
    /// A secrets reader than maintains an in-memory cache, where values have a set TTL.
    caching_secrets_reader: CachingSecretsReader,

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
    #[derivative(Debug = "ignore")]
    segment_client: Option<mz_segment::Client>,

    /// Coordinator metrics.
    metrics: Metrics,

    /// Tracing handle.
    tracing_handle: TracingHandle,

    /// Data used by the statement logging feature.
    statement_logging: StatementLogging,

    /// Whether to start replicas with the new variable-length row encoding scheme.
    variable_length_row_encoding: bool,
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
        let compute_config = flags::compute_config(self.catalog().system_config());
        self.controller.compute.update_configuration(compute_config);
        let storage_config = flags::storage_config(self.catalog().system_config());
        self.controller.storage.update_configuration(storage_config);
        let orchestrator_scheduling_config =
            flags::orchestrator_scheduling_config(self.catalog().system_config());
        self.controller
            .update_orchestrator_scheduling_config(orchestrator_scheduling_config);

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

        debug!("coordinator init: creating compute replicas");
        let mut replicas_to_start = vec![];
        for instance in self.catalog.clusters() {
            self.controller.create_cluster(
                instance.id,
                ClusterConfig {
                    arranged_logs: instance.log_indexes.clone(),
                },
                self.variable_length_row_encoding,
            )?;
            for (replica_id, replica) in instance.replicas_by_id.clone() {
                let role = instance.role();
                replicas_to_start.push(CreateReplicaConfig {
                    cluster_id: instance.id,
                    replica_id,
                    role,
                    config: replica.config,
                });
            }
        }
        self.controller.create_replicas(replicas_to_start).await?;

        debug!("coordinator init: migrating builtin objects");
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

        // Subsources must be created immediately before their primary source
        // without any intermediary collections between them. This version of MZ
        // needs this because it sets tighter bounds on collections' sinces.
        // Without this, dependent collections can place read holds on the
        // subsource, which can result in panics if we adjust the subsource's
        // since.
        //
        // This can likely be removed in the next version of Materialize
        // (v0.46).
        let mut entries_awaiting_dependent: BTreeMap<GlobalId, Vec<catalog::CatalogEntry>> =
            BTreeMap::new();
        let mut awaited_dependent_seen = BTreeSet::new();

        let mut unsorted_entries: VecDeque<_> = self
            .catalog()
            .entries()
            .cloned()
            .map(|entry| {
                let remaining_deps = entry.uses().0.iter().copied().collect::<Vec<_>>();
                (entry, remaining_deps)
            })
            .collect();
        let mut entries = Vec::with_capacity(unsorted_entries.len());

        while let Some((entry, mut remaining_deps)) = unsorted_entries.pop_front() {
            let awaiting_this_dep = entries_awaiting_dependent.get(&entry.id());
            remaining_deps.retain(|dep| {
                // Consider dependency filled if item is loaded or if this
                // dependency is waiting on this entry.
                !loaded_items.contains(dep)
                    && awaiting_this_dep
                        .map(|awaiting| awaiting.iter().all(|e| e.id() != *dep))
                        .unwrap_or(true)
            });

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

                    if let Some(waiting_on_this_dependent) = entries_awaiting_dependent.remove(&id)
                    {
                        mz_ore::soft_assert! {{
                            let subsources =  entry.subsources();
                            let w: Vec<_> = waiting_on_this_dependent.iter().map(|e| e.id()).collect();
                            w.iter().all(|w| subsources.contains(w))
                        }, "expect that items are exactly source's subsources"}

                        // Re-enqueue objects and continue.
                        for entry in
                            std::iter::once(entry).chain(waiting_on_this_dependent.into_iter())
                        {
                            awaited_dependent_seen.insert(entry.id());
                            unsorted_entries.push_front((entry, vec![]));
                        }
                        continue;
                    }

                    // Subsources wait on their primary source before being
                    // added.
                    if entry.is_subsource() && !awaited_dependent_seen.contains(&id) {
                        let min = entry
                            .used_by()
                            .into_iter()
                            .min()
                            .expect("subsource always used");

                        entries_awaiting_dependent
                            .entry(*min)
                            .or_default()
                            .push(entry);
                        continue;
                    }

                    awaited_dependent_seen.remove(&id);
                    loaded_items.insert(id);
                    entries.push(entry);
                }
            }
        }

        assert!(
            entries_awaiting_dependent.is_empty() && entries_awaiting_dependencies.is_empty(),
            "items not cleared from queue {entries_awaiting_dependent:?}, {entries_awaiting_dependencies:?}"
        );

        let logs: BTreeSet<_> = BUILTINS::logs()
            .map(|log| self.catalog().resolve_builtin_log(log))
            .collect();

        // This is disabled for the moment because it has unusual upper
        // advancement behavior.
        // See: https://materializeinc.slack.com/archives/C01CFKM1QRF/p1660726837927649
        let source_status_collection_id = Some(self.catalog().resolve_builtin_storage_collection(
            &crate::catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
        ));

        let mut collections_to_create = Vec::new();

        fn source_desc<T>(
            catalog: &Catalog,
            source_status_collection_id: Option<GlobalId>,
            source: &Source,
        ) -> CollectionDescription<T> {
            let (data_source, status_collection_id) = match &source.data_source {
                // Re-announce the source description.
                DataSourceDesc::Ingestion(ingestion) => {
                    let ingestion = ingestion.clone().into_inline_connection(catalog.state());

                    (
                        DataSource::Ingestion(ingestion.clone()),
                        source_status_collection_id,
                    )
                }
                // Subsources use source statuses.
                DataSourceDesc::Source => (
                    DataSource::Other(DataSourceOther::Source),
                    source_status_collection_id,
                ),
                DataSourceDesc::Webhook { .. } => {
                    (DataSource::Webhook, source_status_collection_id)
                }
                DataSourceDesc::Progress => (DataSource::Progress, None),
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

        let migratable_collections = entries
            .iter()
            .filter_map(|entry| match entry.item() {
                CatalogItem::Source(source) => Some((
                    entry.id(),
                    source_desc(self.catalog(), source_status_collection_id, source),
                )),
                CatalogItem::Table(table) => {
                    let collection_desc = CollectionDescription::from_desc(
                        table.desc.clone(),
                        DataSourceOther::TableWrites,
                    );
                    Some((entry.id(), collection_desc))
                }
                CatalogItem::MaterializedView(mview) => {
                    let collection_desc = CollectionDescription::from_desc(
                        mview.desc.clone(),
                        DataSourceOther::Compute,
                    );
                    Some((entry.id(), collection_desc))
                }
                _ => None,
            })
            .collect();

        self.controller
            .storage
            .migrate_collections(migratable_collections)
            .await?;

        // Do a first pass looking for collections to create so we can call
        // create_collections only once with a large batch. This batches only
        // tables and system sources. Replicas above, user sources, and
        // materialized views below could be added here, but they take some
        // additional work due to their dependencies.
        for entry in &entries {
            match entry.item() {
                CatalogItem::Table(table) => {
                    let collection_desc = CollectionDescription::from_desc(
                        table.desc.clone(),
                        DataSourceOther::TableWrites,
                    );
                    collections_to_create.push((entry.id(), collection_desc));
                }
                // User sources can have dependencies, so do avoid them in the
                // batch.
                CatalogItem::Source(source) if entry.id().is_system() => {
                    collections_to_create.push((
                        entry.id(),
                        source_desc(self.catalog(), source_status_collection_id, source),
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

        debug!("coordinator init: installing existing objects in catalog");
        let mut privatelink_connections = BTreeMap::new();
        for entry in &entries {
            debug!(
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
                            source_desc(self.catalog(), source_status_collection_id, source);
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
                        let (mut df, df_metainfo) = self
                            .dataflow_builder(idx.cluster_id)
                            .build_index_dataflow(entry.id())?;

                        // Note: ideally, the optimized_plan should be computed and
                        // set when the CatalogItem is re-constructed (in
                        // parse_item).
                        //
                        // However, it's not clear how exactly to change
                        // `load_catalog_items` to accommodate for the
                        // `build_index_dataflow` call above.
                        self.catalog_mut()
                            .set_optimized_plan(entry.id(), df.clone());
                        self.catalog_mut()
                            .set_dataflow_metainfo(entry.id(), df_metainfo);

                        let as_of = self.bootstrap_index_as_of(
                            &df,
                            idx.cluster_id,
                            idx.is_retained_metrics_object,
                        );
                        df.set_as_of(as_of);

                        // What follows is morally equivalent to `self.ship_dataflow(df, idx.cluster_id)`,
                        // but we cannot call that as it will also downgrade the read hold on the index.
                        policy_entry
                            .compute_ids
                            .entry(idx.cluster_id)
                            .or_insert_with(Default::default)
                            .extend(df.export_ids());

                        let df = self.must_finalize_dataflow(df, idx.cluster_id);
                        self.catalog_mut().set_physical_plan(entry.id(), df.clone());

                        self.controller
                            .active_compute()
                            .create_dataflow(idx.cluster_id, df)
                            .unwrap_or_terminate("cannot fail to create dataflows");
                    }
                }
                CatalogItem::View(_) => (),
                CatalogItem::MaterializedView(mview) => {
                    // Re-create the storage collection.
                    let collection_desc = CollectionDescription::from_desc(
                        mview.desc.clone(),
                        DataSourceOther::Compute,
                    );
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
                    let internal_view_id = self.allocate_transient_id()?;
                    let debug_name = self
                        .catalog()
                        .resolve_full_name(entry.name(), entry.conn_id())
                        .to_string();

                    let mut builder = self.dataflow_builder(mview.cluster_id);
                    let (mut df, df_metainfo) = builder.build_materialized_view(
                        entry.id(),
                        internal_view_id,
                        debug_name,
                        &mview.optimized_expr,
                        &mview.desc,
                    )?;

                    // Note: ideally, the optimized_plan should be computed and
                    // set when the CatalogItem is re-constructed (in
                    // parse_item).
                    //
                    // However, it's not clear how exactly to change
                    // `load_catalog_items` to accommodate for the
                    // `build_materialized_view` call above.
                    self.catalog_mut()
                        .set_optimized_plan(entry.id(), df.clone());
                    self.catalog_mut()
                        .set_dataflow_metainfo(entry.id(), df_metainfo);

                    // The 'as_of' field of the dataflow changes after restart
                    let as_of = self.bootstrap_materialized_view_as_of(&df, mview.cluster_id);
                    df.set_as_of(as_of);

                    let df = self.must_ship_dataflow(df, mview.cluster_id).await;
                    self.catalog_mut().set_physical_plan(entry.id(), df);
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

                    let referenced_builder = builder.clone();
                    let builder = builder.into_inline_connection(self.catalog().state());

                    task::spawn(
                        || format!("sink_connection_ready:{}", sink.from),
                        async move {
                            let conn_result = Retry::default()
                                .max_tries(usize::MAX)
                                .clamp_backoff(Duration::from_secs(60 * 10))
                                .retry_async(|_| async {
                                    let referenced_builder = referenced_builder.clone();
                                    let builder = builder.clone();
                                    let connection_context = connection_context.clone();
                                    mz_storage_client::sink::build_sink_connection(
                                        builder,
                                        referenced_builder,
                                        connection_context,
                                    )
                                    .await
                                })
                                .await
                                .map_err(StorageError::Generic)
                                .map_err(AdapterError::from);
                            // It is not an error for sink connections to become ready after `internal_cmd_rx` is dropped.
                            let result = internal_cmd_tx.send(Message::SinkConnectionReady(
                                SinkConnectionReady {
                                    ctx: None,
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
                    if let mz_storage_types::connections::Connection::AwsPrivatelink(conn) =
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
            let existing_vpc_endpoints = BTreeSet::from_iter(existing_vpc_endpoints.into_keys());
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

        debug!("coordinator init: announcing completion of initialization to controller");
        // Announce the completion of initialization.
        self.controller.initialization_complete();

        // Announce primary and foreign key relationships.
        debug!("coordinator init: announcing primary and foreign key relationships");
        let mz_view_keys = self.catalog().resolve_builtin_table(&MZ_VIEW_KEYS);
        for log in BUILTINS::logs() {
            let log_id = &self.catalog().resolve_builtin_log(log).to_string();
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

            let mz_foreign_keys = self.catalog().resolve_builtin_table(&MZ_VIEW_FOREIGN_KEYS);
            builtin_table_updates.extend(
                log.variant.foreign_keys().into_iter().enumerate().flat_map(
                    |(index, (parent, pairs))| {
                        let parent_log = BUILTINS::logs()
                            .find(|src| src.variant == parent)
                            .expect("log foreign key variant is invalid");
                        let parent_id = self.catalog().resolve_builtin_log(parent_log).to_string();
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
        builtin_table_updates.extend(self.catalog().state().pack_all_replica_size_updates());

        // Advance all tables to the current timestamp
        debug!("coordinator init: advancing all tables to current timestamp");
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
            .append_table(appends)
            .expect("invalid updates")
            .await
            .expect("One-shot shouldn't be dropped during bootstrap")
            .unwrap_or_terminate("cannot fail to append");

        // Add builtin table updates the clear the contents of all system tables
        debug!("coordinator init: resetting system tables");
        let read_ts = self.get_local_read_ts();
        for system_table in entries
            .iter()
            .filter(|entry| entry.is_table() && entry.id().is_system())
        {
            debug!(
                "coordinator init: resetting system table {} ({})",
                self.catalog().resolve_full_name(system_table.name(), None),
                system_table.id()
            );
            let current_contents = self
                .controller
                .storage
                .snapshot(system_table.id(), read_ts)
                .await
                .unwrap_or_terminate("cannot fail to fetch snapshot");
            debug!("coordinator init: table size {}", current_contents.len());
            let retractions = current_contents
                .into_iter()
                .map(|(row, diff)| BuiltinTableUpdate {
                    id: system_table.id(),
                    row,
                    diff: diff.neg(),
                });
            builtin_table_updates.extend(retractions);
        }

        debug!("coordinator init: sending builtin table updates");
        self.send_builtin_table_updates(builtin_table_updates).await;

        // Signal to the storage controller that it is now free to reconcile its
        // state with what it has learned from the adapter.
        self.controller.storage.reconcile_state().await;

        // Cleanup orphaned secrets. Errors during list() or delete() do not
        // need to prevent bootstrap from succeeding; we will retry next
        // startup.
        match self.secrets_controller.list().await {
            Ok(controller_secrets) => {
                // Fetch all IDs from the catalog to future-proof against other
                // things using secrets. Today, SECRET and CONNECTION objects use
                // secrets_controller.ensure, but more things could in the future
                // that would be easy to miss adding here.
                let catalog_ids: BTreeSet<GlobalId> =
                    self.catalog().entries().map(|entry| entry.id()).collect();
                let controller_secrets: BTreeSet<GlobalId> =
                    controller_secrets.into_iter().collect();
                let orphaned = controller_secrets.difference(&catalog_ids);
                for id in orphaned {
                    info!("coordinator init: deleting orphaned secret {id}");
                    fail_point!("orphan_secrets");
                    if let Err(e) = self.secrets_controller.delete(*id).await {
                        warn!("Dropping orphaned secret has encountered an error: {}", e);
                    }
                }
            }
            Err(e) => warn!("Failed to list secrets during orphan cleanup: {:?}", e),
        }

        info!("coordinator init: bootstrap complete");
        Ok(())
    }

    /// Returns an `as_of` suitable for bootstrapping the given index dataflow.
    fn bootstrap_index_as_of(
        &self,
        dataflow: &DataflowDescription<OptimizedMirRelationExpr>,
        cluster_id: ComputeInstanceId,
        is_retained_metrics_index: bool,
    ) -> Antichain<Timestamp> {
        // All inputs must be readable at the chosen `as_of`, so it must be at least the join of
        // the `since`s of all dependencies.
        let id_bundle = dataflow_import_id_bundle(dataflow, cluster_id);
        let min_as_of = self.least_valid_read(&id_bundle);

        // For compute reconciliation to recognize that an existing dataflow can be reused, we want
        // to advance the `as_of` far enough that it is beyond the `as_of`s of all dataflows that
        // might still be installed on replicas, but ideally not much farther as that would just
        // increase the wait time until the index becomes readable. We advance the `as_of` to the
        // meet of the `upper`s of all dependencies, as we know that no replica can have produced
        // output for that time, so we can assume that no replica `as_of` has been adanced beyond
        // this time either.
        let write_frontier = self.least_valid_write(&id_bundle);
        // Things go wrong if we try to create a dataflow with `as_of = []`, so avoid that.
        if write_frontier.is_empty() {
            return min_as_of;
        }

        // Advancing the `as_of` to the write frontier means that we lose some historical data.
        // That might be acceptable for the default 1-second index compaction window, but not for
        // retained-metrics indexes. So we need to regress the write frontier by the retention
        // duration of the index.
        //
        // NOTE: If we ever allow custom index compaction windows, we'll need to apply those here
        // as well.
        let lag = if is_retained_metrics_index {
            let retention = self.catalog().state().system_config().metrics_retention();
            Timestamp::new(u64::try_from(retention.as_millis()).unwrap_or_else(|_| {
                tracing::error!("absurd metrics retention duration: {retention:?}");
                u64::MAX
            }))
        } else {
            DEFAULT_LOGICAL_COMPACTION_WINDOW_TS
        };

        let time = write_frontier.into_option().expect("checked above");
        let time = time.saturating_sub(lag);
        let max_as_of = Antichain::from_elem(time);

        min_as_of.join(&max_as_of)
    }

    /// Returns an `as_of` suitable for bootstrapping the given materialized view dataflow.
    fn bootstrap_materialized_view_as_of(
        &self,
        dataflow: &DataflowDescription<OptimizedMirRelationExpr>,
        cluster_id: ComputeInstanceId,
    ) -> Antichain<Timestamp> {
        // All inputs must be readable at the chosen `as_of`, so it must be at least the join of
        // the `since`s of all dependencies.
        let id_bundle = dataflow_import_id_bundle(dataflow, cluster_id);
        let mut as_of = self.least_valid_read(&id_bundle);

        // For compute reconciliation to recognize that an existing dataflow can be reused, we want
        // to advance the `as_of` as far as possible. If a storage collection for the MV already
        // exists, we can advance to that collection's upper. This is the most we can advance the
        // `as_of` without skipping times in the MV output.
        let sink_id = dataflow
            .sink_exports
            .keys()
            .exactly_one()
            .expect("MV dataflow must export a sink");
        let write_frontier = self.storage_write_frontier(*sink_id);
        // Things go wrong if we try to create a dataflow with `as_of = []`, so avoid that.
        if !write_frontier.is_empty() {
            as_of.join_assign(write_frontier);
        }

        as_of
    }

    /// Serves the coordinator, receiving commands from users over `cmd_rx`
    /// and feedback from dataflow workers over `feedback_rx`.
    ///
    /// You must call `bootstrap` before calling this method.
    async fn serve(
        mut self,
        mut internal_cmd_rx: mpsc::UnboundedReceiver<Message>,
        group_commit_rx: NotifyReceiver,
        mut strict_serializable_reads_rx: mpsc::UnboundedReceiver<PendingReadTxn>,
        mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    ) {
        // Watcher that listens for and reports cluster service status changes.
        let mut cluster_events = self.controller.events_stream();

        let (idle_tx, mut idle_rx) = tokio::sync::mpsc::channel(1);
        let idle_metric = self.metrics.queue_busy_seconds.with_label_values(&[]);
        spawn(|| "coord watchdog", async move {
            // Every 5 seconds, attempt to measure how long it takes for the
            // coord select loop to be empty, because this message is the last
            // processed. If it is idle, this will result in some microseconds
            // of measurement.
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            // If we end up having to wait more than 5 seconds for the coord to respond, then the
            // behavior of Delay results in the interval "restarting" from whenever we yield
            // instead of trying to catch up.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            // Track if we become stuck to de-dupe error reporting.
            let mut coord_stuck = false;

            loop {
                interval.tick().await;

                // Wait for space in the channel, if we timeout then the coordinator is stuck!
                let duration = tokio::time::Duration::from_secs(60);
                let timeout = tokio::time::timeout(duration, idle_tx.reserve()).await;
                let Ok(maybe_permit) = timeout else {
                    // Only log the error if we're newly stuck, to prevent logging repeatedly.
                    if !coord_stuck {
                        tracing::error!("Coordinator is stuck, did not respond after {duration:?}");
                    }
                    coord_stuck = true;

                    continue;
                };

                // We got a permit, we're not stuck!
                if coord_stuck {
                    tracing::info!("Coordinator became unstuck");
                }
                coord_stuck = false;

                // If we failed to acquire a permit it's because we're shutting down.
                let Ok(permit) = maybe_permit else {
                    break;
                };

                permit.send(idle_metric.start_timer());
            }
        });

        self.schedule_storage_usage_collection();
        self.spawn_statement_logging_task();
        flags::tracing_config(self.catalog.system_config()).apply(&self.tracing_handle);

        // Report if the handling of a single message takes longer than this threshold.
        let reporting_threshold = self
            .catalog
            .system_config()
            .coord_slow_message_reporting_threshold_ms();

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
                // TODO
                permit = group_commit_rx.queued_and_ready() => {
                    let span = info_span!(parent: None, "group_commit_notify");
                    span.follows_from(Span::current());
                    Message::GroupCommitInitiate(span, Some(permit))
                },
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
                _ = self.advance_timelines_interval.tick() => {
                    let span = info_span!(parent: None, "advance_timelines_interval");
                    span.follows_from(Span::current());
                    Message::GroupCommitInitiate(span, None)
                },

                // Process the idle metric at the lowest priority to sample queue non-idle time.
                // `recv()` on `Receiver` is cancellation safe:
                // https://docs.rs/tokio/1.8.0/tokio/sync/mpsc/struct.Receiver.html#cancel-safety
                timer = idle_rx.recv() => {
                    timer.expect("does not drop").observe_duration();
                    continue;
                }
            };

            // Track the wall time for each message for reporting.
            let histogram_metric = self
                .metrics
                .slow_message_handling
                .with_label_values(&[msg.kind()]);

            self.handle_message(msg)
                // All message processing functions trace. Start a parent span for them to make
                // it easy to find slow messages.
                .instrument(span!(Level::DEBUG, "coordinator message processing"))
                .wall_time()
                .observe(histogram_metric)
                .with_filter(move |wall_time| wall_time >= reporting_threshold)
                .await;
        }
    }

    /// Obtain a read-only Catalog reference.
    fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Obtain a read-only Catalog snapshot, suitable for giving out to
    /// non-Coordinator thread tasks.
    fn owned_catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Obtain a writeable Catalog reference.
    fn catalog_mut(&mut self) -> &mut Catalog {
        // make_mut will cause any other Arc references (from owned_catalog) to
        // continue to be valid by cloning the catalog, putting it in a new Arc,
        // which lives at self._catalog. If there are no other Arc references,
        // then no clone is made, and it returns a reference to the existing
        // object. This makes this method and owned_catalog both very cheap: at
        // most one clone per catalog mutation, but only if there's a read-only
        // reference to it.
        Arc::make_mut(&mut self.catalog)
    }

    /// Publishes a notice message to all sessions.
    pub(crate) fn broadcast_notice(&mut self, notice: AdapterNotice) {
        for meta in self.active_conns.values() {
            let _ = meta.notice_tx.send(notice.clone());
        }
    }

    pub(crate) fn active_conns(&self) -> &BTreeMap<ConnectionId, ConnMeta> {
        &self.active_conns
    }

    pub(crate) fn retire_execution(
        &mut self,
        reason: StatementEndedExecutionReason,
        ctx_extra: ExecuteContextExtra,
    ) {
        if let Some(uuid) = ctx_extra.retire() {
            self.end_statement_execution(uuid, reason);
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
        all_features,
        build_info,
        environment_id,
        metrics_registry,
        now,
        secrets_controller,
        cloud_resource_controller,
        cluster_replica_sizes,
        default_storage_cluster_size,
        system_parameter_defaults,
        availability_zones,
        connection_context,
        storage_usage_client,
        storage_usage_collection_interval,
        storage_usage_retention_period,
        segment_client,
        egress_ips,
        aws_account_id,
        aws_privatelink_availability_zones,
        system_parameter_sync_config,
        active_connection_count,
        http_host_name,
        tracing_handle,
    }: Config,
) -> Result<(Handle, Client), AdapterError> {
    info!("coordinator init: beginning");

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();
    let (group_commit_tx, group_commit_rx) = notifier();
    let (strict_serializable_reads_tx, strict_serializable_reads_rx) = mpsc::unbounded_channel();

    // Validate and process availability zones.
    if !availability_zones.iter().all_unique() {
        coord_bail!("availability zones must be unique");
    }

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
    let (catalog, builtin_migration_metadata, builtin_table_updates, _last_catalog_version) =
        Catalog::open(catalog::Config {
            storage,
            unsafe_mode,
            all_features,
            build_info,
            environment_id: environment_id.clone(),
            now: now.clone(),
            skip_migrations: false,
            metrics_registry: &metrics_registry,
            cluster_replica_sizes,
            default_storage_cluster_size,
            system_parameter_defaults,
            availability_zones,
            secrets_reader: secrets_controller.reader(),
            egress_ips,
            aws_principal_context,
            aws_privatelink_availability_zones,
            system_parameter_sync_config,
            storage_usage_retention_period,
            connection_context: Some(connection_context.clone()),
            active_connection_count,
            http_host_name,
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
    let segment_client_clone = segment_client.clone();
    let span = tracing::Span::current();
    let coord_now = now.clone();
    let advance_timelines_interval = tokio::time::interval(catalog.config().timestamp_interval);
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
                    coord_now.clone(),
                    |ts| catalog.persist_timestamp(&timeline, ts),
                    &mut timestamp_oracles,
                ));
            }

            let caching_secrets_reader = CachingSecretsReader::new(secrets_controller.reader());
            let variable_length_row_encoding = catalog
                .system_config()
                .variable_length_row_encoding_DANGEROUS();
            mz_repr::VARIABLE_LENGTH_ROW_ENCODING
                .store(variable_length_row_encoding, atomic::Ordering::SeqCst);
            let mut coord = Coordinator {
                controller: dataflow_client,
                view_optimizer: Optimizer::logical_optimizer(
                    &mz_transform::typecheck::empty_context(),
                ),
                catalog: Arc::new(catalog),
                internal_cmd_tx,
                group_commit_tx,
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
                advance_timelines_interval,
                secrets_controller,
                caching_secrets_reader,
                cloud_resource_controller,
                connection_context,
                transient_replica_metadata: BTreeMap::new(),
                storage_usage_client,
                storage_usage_collection_interval,
                segment_client,
                metrics,
                tracing_handle,
                statement_logging: StatementLogging::new(),
                variable_length_row_encoding,
            };
            let bootstrap = handle.block_on(async {
                coord
                    .bootstrap(builtin_migration_metadata, builtin_table_updates)
                    .instrument(span)
                    .await?;
                coord
                    .controller
                    .remove_orphaned_replicas(
                        coord.catalog().get_next_user_replica_id().await?,
                        coord.catalog().get_next_system_replica_id().await?,
                    )
                    .await
                    .map_err(AdapterError::Orchestrator)?;
                Ok(())
            });
            let ok = bootstrap.is_ok();
            bootstrap_tx
                .send(bootstrap)
                .expect("bootstrap_rx is not dropped until it receives this message");
            if ok {
                handle.block_on(coord.serve(
                    internal_cmd_rx,
                    group_commit_rx,
                    strict_serializable_reads_rx,
                    cmd_rx,
                ));
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
            let client = Client::new(
                build_info,
                cmd_tx.clone(),
                metrics_clone,
                now,
                environment_id,
                segment_client_clone,
            );
            Ok((handle, client))
        }
        Err(e) => Err(e),
    }
}
