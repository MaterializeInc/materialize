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
use std::num::NonZeroI64;
use std::ops::Neg;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use fail::fail_point;
use futures::future::{BoxFuture, FutureExt, LocalBoxFuture};
use futures::StreamExt;
use itertools::Itertools;
use mz_adapter_types::compaction::{
    CompactionWindow, ReadCapability, DEFAULT_LOGICAL_COMPACTION_WINDOW_TS,
};
use mz_adapter_types::connection::ConnectionId;
use mz_build_info::BuildInfo;
use mz_catalog::config::{AwsPrincipalContext, ClusterReplicaSizeMap};
use mz_catalog::memory::objects::{CatalogEntry, CatalogItem, Connection, DataSourceDesc, Source};
use mz_cloud_resources::{CloudResourceController, VpcEndpointConfig, VpcEndpointEvent};
use mz_compute_client::controller::error::InstanceMissing;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_compute_types::ComputeInstanceId;
use mz_controller::clusters::{ClusterConfig, ClusterEvent, CreateReplicaConfig};
use mz_controller::ControllerConfig;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_orchestrator::ServiceProcessMetrics;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::option::OptionExt;
use mz_ore::task::spawn;
use mz_ore::thread::JoinHandleExt;
use mz_ore::tracing::{OpenTelemetryContext, TracingHandle};
use mz_ore::{soft_panic_or_log, stack};
use mz_persist_client::usage::{ShardsUsageReferenced, StorageUsageClient};
use mz_repr::explain::ExplainFormat;
use mz_repr::role_id::RoleId;
use mz_repr::{GlobalId, Timestamp};
use mz_secrets::cache::CachingSecretsReader;
use mz_secrets::{SecretsController, SecretsReader};
use mz_sql::ast::{CreateSubsourceStatement, Raw, Statement};
use mz_sql::catalog::EnvironmentId;
use mz_sql::names::{Aug, ResolvedIds};
use mz_sql::plan::{self, CopyFormat, CreateConnectionPlan, Params, QueryWhen};
use mz_sql::rbac::UnauthorizedError;
use mz_sql::session::user::{RoleMetadata, User};
use mz_sql::session::vars::{self, ConnectionCounter, OwnedVarInput, SystemVars};
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::controller::PersistTxnTablesImpl;
use mz_storage_types::sources::Timeline;
use opentelemetry::trace::TraceContextExt;
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio::runtime::Handle as TokioHandle;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch, OwnedMutexGuard};
use tracing::{debug, info, info_span, span, warn, Instrument, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::catalog::{BuiltinMigrationMetadata, BuiltinTableUpdate, Catalog};
use crate::client::{Client, Handle};
use crate::command::{Canceled, Command, ExecuteResponse};
use crate::config::{SynchronizedParameters, SystemParameterFrontend, SystemParameterSyncConfig};
use crate::coord::appends::{Deferred, GroupCommitPermit, PendingWriteTxn};
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::peek::PendingPeek;
use crate::coord::timeline::{TimelineContext, TimelineState, WriteTimestamp};
use crate::coord::timestamp_oracle::catalog_oracle::CatalogTimestampPersistence;
use crate::coord::timestamp_oracle::postgres_oracle::{
    PostgresTimestampOracle, PostgresTimestampOracleConfig,
};
use crate::coord::timestamp_selection::TimestampContext;
use crate::error::AdapterError;
use crate::metrics::Metrics;
use crate::optimize::dataflows::{
    dataflow_import_id_bundle, ComputeInstanceSnapshot, DataflowBuilder,
};
use crate::optimize::{self, Optimize, OptimizerConfig};
use crate::session::{EndTransactionAction, Session};
use crate::statement_logging::StatementEndedExecutionReason;
use crate::subscribe::ActiveSubscribe;
use crate::util::{ClientTransmitter, CompletedClientTransmitter, ComputeSinkId, ResultExt};
use crate::webhook::{WebhookAppenderInvalidator, WebhookConcurrencyLimiter};
use crate::{flags, AdapterNotice, TimestampProvider};
use mz_catalog::builtin::BUILTINS;
use mz_catalog::durable::DurableCatalogState;

use self::statement_logging::{StatementLogging, StatementLoggingId};

pub(crate) mod id_bundle;
pub(crate) mod peek;
pub(crate) mod statement_logging;
pub(crate) mod timeline;
pub(crate) mod timestamp_oracle;
pub(crate) mod timestamp_selection;

mod appends;
mod command_handler;
pub mod consistency;
mod ddl;
mod indexes;
mod introspection;
mod message_handler;
mod privatelink_status;
mod read_policy;
mod sequencer;
mod sql;

#[derive(Debug)]
pub enum Message<T = mz_repr::Timestamp> {
    Command(OpenTelemetryContext, Command),
    ControllerReady,
    PurifiedStatementReady(PurifiedStatementReady),
    CreateConnectionValidationReady(CreateConnectionValidationReady),
    AlterConnectionValidationReady(AlterConnectionValidationReady),
    WriteLockGrant(tokio::sync::OwnedMutexGuard<()>),
    /// Initiates a group commit.
    GroupCommitInitiate(Span, Option<GroupCommitPermit>),
    /// Makes a group commit visible to all clients.
    GroupCommitApply(
        /// Timestamp of the writes in the group commit.
        T,
        /// Clients waiting on responses from the group commit.
        Vec<CompletedClientTransmitter>,
        /// Optional lock if the group commit contained writes to user tables.
        Option<OwnedMutexGuard<()>>,
        /// Permit which limits how many group commits we run at once.
        Option<GroupCommitPermit>,
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
        otel_ctx: OpenTelemetryContext,
        reason: StatementEndedExecutionReason,
    },
    ExecuteSingleStatementTransaction {
        ctx: ExecuteContext,
        otel_ctx: OpenTelemetryContext,
        stmt: Arc<Statement<Raw>>,
        params: mz_sql::plan::Params,
    },
    PeekStageReady {
        ctx: ExecuteContext,
        otel_ctx: OpenTelemetryContext,
        stage: PeekStage,
    },
    CreateIndexStageReady {
        ctx: ExecuteContext,
        otel_ctx: OpenTelemetryContext,
        stage: CreateIndexStage,
    },
    CreateViewStageReady {
        ctx: ExecuteContext,
        otel_ctx: OpenTelemetryContext,
        stage: CreateViewStage,
    },
    CreateMaterializedViewStageReady {
        ctx: ExecuteContext,
        otel_ctx: OpenTelemetryContext,
        stage: CreateMaterializedViewStage,
    },
    SubscribeStageReady {
        ctx: ExecuteContext,
        otel_ctx: OpenTelemetryContext,
        stage: SubscribeStage,
    },
    DrainStatementLog,
    PrivateLinkVpcEndpointEvents(Vec<VpcEndpointEvent>),
}

impl Message {
    /// Returns a string to identify the kind of [`Message`], useful for logging.
    pub const fn kind(&self) -> &'static str {
        match self {
            Message::Command(_, msg) => match msg {
                Command::CatalogSnapshot { .. } => "command-catalog_snapshot",
                Command::Startup { .. } => "command-startup",
                Command::Execute { .. } => "command-execute",
                Command::Commit { .. } => "command-commit",
                Command::CancelRequest { .. } => "command-cancel_request",
                Command::PrivilegedCancelRequest { .. } => "command-privileged_cancel_request",
                Command::GetWebhook { .. } => "command-get_webhook",
                Command::GetSystemVars { .. } => "command-get_system_vars",
                Command::SetSystemVars { .. } => "command-set_system_vars",
                Command::Terminate { .. } => "command-terminate",
                Command::RetireExecute { .. } => "command-retire_execute",
                Command::CheckConsistency { .. } => "command-check_consistency",
            },
            Message::ControllerReady => "controller_ready",
            Message::PurifiedStatementReady(_) => "purified_statement_ready",
            Message::CreateConnectionValidationReady(_) => "create_connection_validation_ready",
            Message::WriteLockGrant(_) => "write_lock_grant",
            Message::GroupCommitInitiate(..) => "group_commit_initiate",
            Message::GroupCommitApply(..) => "group_commit_apply",
            Message::AdvanceTimelines => "advance_timelines",
            Message::ClusterEvent(_) => "cluster_event",
            Message::RemovePendingPeeks { .. } => "remove_pending_peeks",
            Message::LinearizeReads(_) => "linearize_reads",
            Message::StorageUsageFetch => "storage_usage_fetch",
            Message::StorageUsageUpdate(_) => "storage_usage_update",
            Message::RealTimeRecencyTimestamp { .. } => "real_time_recency_timestamp",
            Message::RetireExecute { .. } => "retire_execute",
            Message::ExecuteSingleStatementTransaction { .. } => {
                "execute_single_statement_transaction"
            }
            Message::PeekStageReady { .. } => "peek_stage_ready",
            Message::CreateIndexStageReady { .. } => "create_index_stage_ready",
            Message::CreateViewStageReady { .. } => "create_view_stage_ready",
            Message::CreateMaterializedViewStageReady { .. } => {
                "create_materialized_view_stage_ready"
            }
            Message::SubscribeStageReady { .. } => "subscribe_stage_ready",
            Message::DrainStatementLog => "drain_statement_log",
            Message::AlterConnectionValidationReady(..) => "alter_connection_validation_ready",
            Message::PrivateLinkVpcEndpointEvents(_) => "private_link_vpc_endpoint_events",
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
    pub original_stmt: Arc<Statement<Raw>>,
    pub otel_ctx: OpenTelemetryContext,
}

pub type PurifiedStatementReady = BackgroundWorkResult<(
    Vec<(GlobalId, CreateSubsourceStatement<Aug>)>,
    Statement<Aug>,
)>;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ValidationReady<T> {
    #[derivative(Debug = "ignore")]
    pub ctx: ExecuteContext,
    pub result: Result<T, AdapterError>,
    pub connection_gid: GlobalId,
    pub plan_validity: PlanValidity,
    pub otel_ctx: OpenTelemetryContext,
}

pub type CreateConnectionValidationReady = ValidationReady<CreateConnectionPlan>;
pub type AlterConnectionValidationReady = ValidationReady<Connection>;

#[derive(Debug)]
pub enum RealTimeRecencyContext {
    ExplainTimestamp {
        ctx: ExecuteContext,
        format: ExplainFormat,
        cluster_id: ClusterId,
        optimized_plan: OptimizedMirRelationExpr,
        id_bundle: CollectionIdBundle,
        when: QueryWhen,
    },
    Peek {
        ctx: ExecuteContext,
        root_otel_ctx: OpenTelemetryContext,
        copy_to: Option<CopyFormat>,
        when: QueryWhen,
        target_replica: Option<ReplicaId>,
        timeline_context: TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        source_ids: BTreeSet<GlobalId>,
        in_immediate_multi_stmt_txn: bool,
        optimizer: optimize::peek::Optimizer,
        global_mir_plan: optimize::peek::GlobalMirPlan,
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
    Timestamp(PeekStageTimestamp),
    Optimize(PeekStageOptimize),
    RealTimeRecency(PeekStageRealTimeRecency),
    Finish(PeekStageFinish),
}

impl PeekStage {
    fn validity(&mut self) -> Option<&mut PlanValidity> {
        match self {
            PeekStage::Validate(_) => None,
            PeekStage::Timestamp(PeekStageTimestamp { validity, .. })
            | PeekStage::Optimize(PeekStageOptimize { validity, .. })
            | PeekStage::RealTimeRecency(PeekStageRealTimeRecency { validity, .. })
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
pub struct PeekStageTimestamp {
    validity: PlanValidity,
    source: MirRelationExpr,
    copy_to: Option<CopyFormat>,
    source_ids: BTreeSet<GlobalId>,
    when: QueryWhen,
    target_replica: Option<ReplicaId>,
    timeline_context: TimelineContext,
    in_immediate_multi_stmt_txn: bool,
    optimizer: optimize::peek::Optimizer,
}

#[derive(Debug)]
pub struct PeekStageOptimize {
    validity: PlanValidity,
    source: MirRelationExpr,
    copy_to: Option<CopyFormat>,
    source_ids: BTreeSet<GlobalId>,
    when: QueryWhen,
    target_replica: Option<ReplicaId>,
    timeline_context: TimelineContext,
    oracle_read_ts: Option<Timestamp>,
    in_immediate_multi_stmt_txn: bool,
    optimizer: optimize::peek::Optimizer,
}

#[derive(Debug)]
pub struct PeekStageRealTimeRecency {
    validity: PlanValidity,
    copy_to: Option<CopyFormat>,
    source_ids: BTreeSet<GlobalId>,
    id_bundle: CollectionIdBundle,
    when: QueryWhen,
    target_replica: Option<ReplicaId>,
    timeline_context: TimelineContext,
    oracle_read_ts: Option<Timestamp>,
    in_immediate_multi_stmt_txn: bool,
    optimizer: optimize::peek::Optimizer,
    global_mir_plan: optimize::peek::GlobalMirPlan,
}

#[derive(Debug)]
pub struct PeekStageFinish {
    validity: PlanValidity,
    copy_to: Option<CopyFormat>,
    id_bundle: Option<CollectionIdBundle>,
    when: QueryWhen,
    target_replica: Option<ReplicaId>,
    timeline_context: TimelineContext,
    oracle_read_ts: Option<Timestamp>,
    source_ids: BTreeSet<GlobalId>,
    real_time_recency_ts: Option<mz_repr::Timestamp>,
    optimizer: optimize::peek::Optimizer,
    global_mir_plan: optimize::peek::GlobalMirPlan,
}

#[derive(Debug)]
pub enum CreateIndexStage {
    Validate(CreateIndexValidate),
    Optimize(CreateIndexOptimize),
    Finish(CreateIndexFinish),
}

impl CreateIndexStage {
    fn validity(&mut self) -> Option<&mut PlanValidity> {
        match self {
            Self::Validate(_) => None,
            Self::Optimize(stage) => Some(&mut stage.validity),
            Self::Finish(stage) => Some(&mut stage.validity),
        }
    }
}

#[derive(Debug)]
pub struct CreateIndexValidate {
    plan: plan::CreateIndexPlan,
    resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct CreateIndexOptimize {
    validity: PlanValidity,
    plan: plan::CreateIndexPlan,
    resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct CreateIndexFinish {
    validity: PlanValidity,
    id: GlobalId,
    plan: plan::CreateIndexPlan,
    resolved_ids: ResolvedIds,
    global_mir_plan: optimize::index::GlobalMirPlan,
    global_lir_plan: optimize::index::GlobalLirPlan,
}

#[derive(Debug)]
pub enum CreateViewStage {
    Validate(CreateViewValidate),
    Optimize(CreateViewOptimize),
    Finish(CreateViewFinish),
}

impl CreateViewStage {
    fn validity(&mut self) -> Option<&mut PlanValidity> {
        match self {
            Self::Validate(_) => None,
            Self::Optimize(stage) => Some(&mut stage.validity),
            Self::Finish(stage) => Some(&mut stage.validity),
        }
    }
}

#[derive(Debug)]
pub struct CreateViewValidate {
    plan: plan::CreateViewPlan,
    resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct CreateViewOptimize {
    validity: PlanValidity,
    plan: plan::CreateViewPlan,
    resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct CreateViewFinish {
    validity: PlanValidity,
    id: GlobalId,
    plan: plan::CreateViewPlan,
    resolved_ids: ResolvedIds,
    optimized_expr: OptimizedMirRelationExpr,
}

#[derive(Debug)]
pub enum CreateMaterializedViewStage {
    Validate(CreateMaterializedViewValidate),
    Optimize(CreateMaterializedViewOptimize),
    Finish(CreateMaterializedViewFinish),
}

impl CreateMaterializedViewStage {
    fn validity(&mut self) -> Option<&mut PlanValidity> {
        match self {
            Self::Validate(_) => None,
            Self::Optimize(stage) => Some(&mut stage.validity),
            Self::Finish(stage) => Some(&mut stage.validity),
        }
    }
}

#[derive(Debug)]
pub struct CreateMaterializedViewValidate {
    plan: plan::CreateMaterializedViewPlan,
    resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct CreateMaterializedViewOptimize {
    validity: PlanValidity,
    plan: plan::CreateMaterializedViewPlan,
    resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct CreateMaterializedViewFinish {
    validity: PlanValidity,
    id: GlobalId,
    plan: plan::CreateMaterializedViewPlan,
    resolved_ids: ResolvedIds,
    local_mir_plan: optimize::materialized_view::LocalMirPlan,
    global_mir_plan: optimize::materialized_view::GlobalMirPlan,
    global_lir_plan: optimize::materialized_view::GlobalLirPlan,
}

#[derive(Debug)]
pub enum SubscribeStage {
    Validate(SubscribeValidate),
    OptimizeMir(SubscribeOptimizeMir),
    Timestamp(SubscribeTimestamp),
    OptimizeLir(SubscribeOptimizeLir),
    Finish(SubscribeFinish),
}

impl SubscribeStage {
    fn validity(&mut self) -> Option<&mut PlanValidity> {
        match self {
            Self::Validate(_) => None,
            Self::OptimizeMir(stage) => Some(&mut stage.validity),
            Self::Timestamp(stage) => Some(&mut stage.validity),
            Self::OptimizeLir(stage) => Some(&mut stage.validity),
            Self::Finish(stage) => Some(&mut stage.validity),
        }
    }
}

#[derive(Debug)]
pub struct SubscribeValidate {
    plan: plan::SubscribePlan,
    target_cluster: TargetCluster,
}

#[derive(Debug)]
pub struct SubscribeOptimizeMir {
    validity: PlanValidity,
    plan: plan::SubscribePlan,
    timeline: TimelineContext,
}

#[derive(Debug)]
pub struct SubscribeTimestamp {
    validity: PlanValidity,
    plan: plan::SubscribePlan,
    timeline: TimelineContext,
    optimizer: optimize::subscribe::Optimizer,
    global_mir_plan: optimize::subscribe::GlobalMirPlan<optimize::subscribe::Unresolved>,
}

#[derive(Debug)]
pub struct SubscribeOptimizeLir {
    validity: PlanValidity,
    plan: plan::SubscribePlan,
    optimizer: optimize::subscribe::Optimizer,
    global_mir_plan: optimize::subscribe::GlobalMirPlan<optimize::subscribe::Resolved>,
}

#[derive(Debug)]
pub struct SubscribeFinish {
    validity: PlanValidity,
    cluster_id: ComputeInstanceId,
    plan: plan::SubscribePlan,
    global_lir_plan: optimize::subscribe::GlobalLirPlan,
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
    /// The cluster selected at the start of a transaction.
    Transaction(ClusterId),
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
                if cluster.replica(replica_id).is_none() {
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
    pub controller_config: ControllerConfig,
    pub controller_envd_epoch: NonZeroI64,
    pub controller_persist_txn_tables: PersistTxnTablesImpl,
    pub storage: Box<dyn mz_catalog::durable::DurableCatalogState>,
    pub timestamp_oracle_url: Option<String>,
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
    pub builtin_cluster_replica_size: String,
    pub system_parameter_defaults: BTreeMap<String, String>,
    pub storage_usage_client: StorageUsageClient,
    pub storage_usage_collection_interval: Duration,
    pub storage_usage_retention_period: Option<Duration>,
    pub segment_client: Option<mz_segment::Client>,
    pub egress_ips: Vec<Ipv4Addr>,
    pub system_parameter_sync_config: Option<SystemParameterSyncConfig>,
    pub aws_account_id: Option<String>,
    pub aws_privatelink_availability_zones: Option<Vec<String>>,
    pub connection_context: ConnectionContext,
    pub active_connection_count: Arc<Mutex<ConnectionCounter>>,
    pub webhook_concurrency_limit: WebhookConcurrencyLimiter,
    pub http_host_name: Option<String>,
    pub tracing_handle: TracingHandle,
}

/// Soft-state metadata about a compute replica
#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct ReplicaMetadata {
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
    /// Telemetry context.
    otel_ctx: OpenTelemetryContext,
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
            } => TimestampContext::TimelineTimestamp {
                timeline: timeline.clone(),
                chosen_ts: timestamp.clone(),
                oracle_ts: None, // For writes, we always pick the oracle
                                 // timestamp!
            },
        }
    }

    /// Alert the client that the read has been linearized.
    ///
    /// If it is necessary to finalize an execute, return the state necessary to do so
    /// (execution context and result)
    #[tracing::instrument(level = "debug", skip_all)]
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
    pub fn contents(&self) -> Option<StatementLoggingId> {
        let Self { statement_uuid } = self;
        *statement_uuid
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
        if let Some(statement_uuid) = statement_uuid {
            // Note: the impact when this error hits
            // is that the statement will never be marked
            // as finished in the statement log.
            soft_panic_or_log!("execute context for statement {statement_uuid:?} dropped without being properly retired.");
        }
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
    #[tracing::instrument(level = "debug", skip_all)]
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
                otel_ctx: OpenTelemetryContext::obtain(),
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
    /// The catalog in an Arc suitable for readonly references. The Arc allows
    /// us to hand out cheap copies of the catalog to functions that can use it
    /// off of the main coordinator thread. If the coordinator needs to mutate
    /// the catalog, call [`Self::catalog_mut`], which will clone this struct member,
    /// allowing it to be mutated here while the other off-thread references can
    /// read their catalog as long as needed. In the future we would like this
    /// to be a pTVC, but for now this is sufficient.
    catalog: Arc<Catalog>,

    /// Channel to manage internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    /// Notification that triggers a group commit.
    group_commit_tx: appends::GroupCommitNotifier,

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
    /// A map from active webhooks to their invalidation handle.
    active_webhooks: BTreeMap<GlobalId, WebhookAppenderInvalidator>,

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

    /// Limit for how many conncurrent webhook requests we allow.
    webhook_concurrency_limit: WebhookConcurrencyLimiter,

    /// Implementation of
    /// [`TimestampOracle`](crate::coord::timestamp_oracle::TimestampOracle) to
    /// use.
    timestamp_oracle_impl: vars::TimestampOracleImpl,

    /// Optional config for the Postgres-backed timestamp oracle. This is
    /// _required_ when `postgres` is configured using the `timestamp_oracle`
    /// system variable.
    pg_timestamp_oracle_config: Option<PostgresTimestampOracleConfig>,
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
        let system_config = self.catalog().system_config();
        let compute_config = flags::compute_config(system_config);
        let storage_config = flags::storage_config(system_config);
        let scheduling_config = flags::orchestrator_scheduling_config(system_config);
        let merge_effort = system_config.default_idle_arrangement_merge_effort();
        let exert_prop = system_config.default_arrangement_exert_proportionality();
        self.controller.compute.update_configuration(compute_config);
        self.controller.storage.update_parameters(storage_config);
        self.controller
            .update_orchestrator_scheduling_config(scheduling_config);
        self.controller
            .set_default_idle_arrangement_merge_effort(merge_effort);
        self.controller
            .set_default_arrangement_exert_proportionality(exert_prop);

        let mut policies_to_set: BTreeMap<CompactionWindow, CollectionIdBundle> =
            Default::default();

        debug!("coordinator init: creating compute replicas");
        let mut replicas_to_start = vec![];
        for instance in self.catalog.clusters() {
            self.controller.create_cluster(
                instance.id,
                ClusterConfig {
                    arranged_logs: instance.log_indexes.clone(),
                },
            )?;
            for replica in instance.replicas() {
                let role = instance.role();
                replicas_to_start.push(CreateReplicaConfig {
                    cluster_id: instance.id,
                    replica_id: replica.replica_id,
                    role,
                    config: replica.config.clone(),
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

        debug!("coordinator init: initializing storage collections");
        self.bootstrap_storage_collections().await;

        // Load catalog entries based on topological dependency sorting. We do
        // this to reinforce that `GlobalId`'s `Ord` implementation does not
        // express the entries' dependency graph.
        let mut entries_awaiting_dependencies: BTreeMap<
            GlobalId,
            Vec<(CatalogEntry, Vec<GlobalId>)>,
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
        let mut entries_awaiting_dependent: BTreeMap<GlobalId, Vec<CatalogEntry>> = BTreeMap::new();
        let mut awaited_dependent_seen = BTreeSet::new();

        let mut unsorted_entries: VecDeque<_> = self
            .catalog()
            .entries()
            .cloned()
            .map(|entry| {
                let remaining_deps = entry.uses().into_iter().collect::<Vec<_>>();
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
                        mz_ore::soft_assert_no_log! {{
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

        debug!("coordinator init: optimizing dataflow plans");
        self.bootstrap_dataflow_plans(&entries)?;

        // Discover what indexes MVs depend on. Needed for as-of selection below.
        // This step relies on the dataflow plans created by `bootstrap_dataflow_plans`.
        let mut index_dependent_matviews = self.collect_index_dependent_matviews();

        let logs: BTreeSet<_> = BUILTINS::logs()
            .map(|log| self.catalog().resolve_builtin_log(log))
            .collect();

        debug!("coordinator init: installing existing objects in catalog");
        let mut privatelink_connections = BTreeMap::new();

        for entry in &entries {
            debug!(
                "coordinator init: installing {} {}",
                entry.item().typ(),
                entry.id()
            );
            let policy = entry.item().initial_logical_compaction_window();
            match entry.item() {
                // Currently catalog item rebuild assumes that sinks and
                // indexes are always built individually and does not store information
                // about how it was built. If we start building multiple sinks and/or indexes
                // using a single dataflow, we have to make sure the rebuild process re-runs
                // the same multiple-build dataflow.
                CatalogItem::Source(_) => {
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
                        let mut df_desc = self
                            .catalog()
                            .try_get_physical_plan(&entry.id())
                            .expect("added in `bootstrap_dataflow_plans`")
                            .clone();

                        // Timestamp selection
                        let dependent_matviews = index_dependent_matviews
                            .remove(&entry.id())
                            .expect("all index dependants were collected");
                        let as_of = self.bootstrap_index_as_of(
                            &df_desc,
                            idx.cluster_id,
                            idx.is_retained_metrics_object,
                            dependent_matviews,
                        );
                        df_desc.set_as_of(as_of);

                        let df_meta = self
                            .catalog()
                            .try_get_dataflow_metainfo(&entry.id())
                            .expect("added in `bootstrap_dataflow_plans`");

                        if self.catalog().state().system_config().enable_mz_notices() {
                            // Collect optimization hint updates.
                            self.catalog().pack_optimizer_notices(
                                &mut builtin_table_updates,
                                df_meta.optimizer_notices.iter(),
                                1,
                            );
                        }

                        // What follows is morally equivalent to `self.ship_dataflow(df, idx.cluster_id)`,
                        // but we cannot call that as it will also downgrade the read hold on the index.
                        policy_entry
                            .compute_ids
                            .entry(idx.cluster_id)
                            .or_insert_with(Default::default)
                            .extend(df_desc.export_ids());

                        self.controller
                            .active_compute()
                            .create_dataflow(idx.cluster_id, df_desc)
                            .unwrap_or_terminate("cannot fail to create dataflows");
                    }
                }
                CatalogItem::View(_) => (),
                CatalogItem::MaterializedView(mview) => {
                    policies_to_set
                        .entry(policy.expect("materialized views have a compaction window"))
                        .or_insert_with(Default::default)
                        .storage_ids
                        .insert(entry.id());

                    let mut df_desc = self
                        .catalog()
                        .try_get_physical_plan(&entry.id())
                        .expect("added in `bootstrap_dataflow_plans`")
                        .clone();

                    // Timestamp selection
                    let as_of = self.bootstrap_materialized_view_as_of(&df_desc, mview.cluster_id);
                    df_desc.set_as_of(as_of);

                    let df_meta = self
                        .catalog()
                        .try_get_dataflow_metainfo(&entry.id())
                        .expect("added in `bootstrap_dataflow_plans`");

                    if self.catalog().state().system_config().enable_mz_notices() {
                        // Collect optimization hint updates.
                        self.catalog().pack_optimizer_notices(
                            &mut builtin_table_updates,
                            df_meta.optimizer_notices.iter(),
                            1,
                        );
                    }

                    self.ship_dataflow(df_desc, mview.cluster_id).await;
                }
                CatalogItem::Sink(sink) => {
                    let id = entry.id();
                    self.create_storage_export(id, sink)
                        .await
                        .unwrap_or_terminate("cannot fail to create exports");
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
        for (cw, policies) in policies_to_set {
            self.initialize_read_policies(&policies, cw).await;
        }

        debug!("coordinator init: announcing completion of initialization to controller");
        // Announce the completion of initialization.
        self.controller.initialization_complete();

        // Expose mapping from T-shirt sizes to actual sizes
        builtin_table_updates.extend(self.catalog().state().pack_all_replica_size_updates());

        // Advance all tables to the current timestamp
        debug!("coordinator init: advancing all tables to current timestamp");
        let WriteTimestamp {
            timestamp: write_ts,
            advance_to,
        } = self.get_local_write_ts().await;
        let appends = entries
            .iter()
            .filter(|entry| entry.is_table())
            .map(|entry| (entry.id(), Vec::new()))
            .collect();
        self.controller
            .storage
            .append_table(write_ts.clone(), advance_to, appends)
            .expect("invalid updates")
            .await
            .expect("One-shot shouldn't be dropped during bootstrap")
            .unwrap_or_terminate("cannot fail to append");
        self.apply_local_write(write_ts).await;

        // Add builtin table updates the clear the contents of all system tables
        debug!("coordinator init: resetting system tables");
        let read_ts = self.get_local_read_ts().await;
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
        let builtin_updates_fut = self
            .builtin_table_update()
            .execute(builtin_table_updates)
            .await;

        // Destructure Self so we can do some concurrent work.
        let Self {
            controller,
            secrets_controller,
            catalog,
            ..
        } = self;

        // Signal to the storage controller that it is now free to reconcile its
        // state with what it has learned from the adapter.
        let storage_reconcile_fut = controller.storage.reconcile_state();

        // Cleanup orphaned secrets. Errors during list() or delete() do not
        // need to prevent bootstrap from succeeding; we will retry next
        // startup.
        let secrets_cleanup_fut = async move {
            match secrets_controller.list().await {
                Ok(controller_secrets) => {
                    // Fetch all IDs from the catalog to future-proof against other
                    // things using secrets. Today, SECRET and CONNECTION objects use
                    // secrets_controller.ensure, but more things could in the future
                    // that would be easy to miss adding here.
                    let catalog_ids: BTreeSet<GlobalId> =
                        catalog.entries().map(|entry| entry.id()).collect();
                    let controller_secrets: BTreeSet<GlobalId> =
                        controller_secrets.into_iter().collect();
                    let orphaned = controller_secrets.difference(&catalog_ids);
                    for id in orphaned {
                        info!("coordinator init: deleting orphaned secret {id}");
                        fail_point!("orphan_secrets");
                        if let Err(e) = secrets_controller.delete(*id).await {
                            warn!("Dropping orphaned secret has encountered an error: {}", e);
                        }
                    }
                }
                Err(e) => warn!("Failed to list secrets during orphan cleanup: {:?}", e),
            }
        };

        // Run all of our final steps concurrently.
        futures::future::join_all([
            storage_reconcile_fut,
            builtin_updates_fut,
            Box::pin(secrets_cleanup_fut),
        ])
        .await;

        info!("coordinator init: bootstrap complete");
        Ok(())
    }

    /// Initializes all storage collections required by catalog objects in the storage controller.
    ///
    /// This method takes care of collection creation, as well as migration of existing
    /// collections.
    ///
    /// Creating all storage collections in a single `create_collections` call, rather than on
    /// demand, is more efficient as it reduces the number of writes to durable storage. It also
    /// allows subsequent bootstrap logic to fetch metadata (such as frontiers) of arbitrary
    /// storage collections, without needing to worry about dependency order.
    async fn bootstrap_storage_collections(&mut self) {
        // Reset the txns and table shards to a known set of invariants.
        //
        // TODO: This can be removed once we've flipped to the new txns system
        // for good and there is no possibility of the old code running
        // concurrently with the new code.
        let init_ts = self.get_local_write_ts().await.timestamp;
        self.controller
            .storage
            .init_txns(init_ts)
            .await
            .unwrap_or_terminate("init_txns");
        self.apply_local_write(init_ts).await;

        let catalog = self.catalog();
        let source_status_collection_id = catalog
            .resolve_builtin_storage_collection(&mz_catalog::builtin::MZ_SOURCE_STATUS_HISTORY);

        let source_desc = |source: &Source| {
            let (data_source, status_collection_id) = match &source.data_source {
                // Re-announce the source description.
                DataSourceDesc::Ingestion(ingestion) => {
                    let ingestion = ingestion.clone().into_inline_connection(catalog.state());

                    (
                        DataSource::Ingestion(ingestion.clone()),
                        Some(source_status_collection_id),
                    )
                }
                // Subsources use source statuses.
                DataSourceDesc::Source => (
                    DataSource::Other(DataSourceOther::Source),
                    Some(source_status_collection_id),
                ),
                DataSourceDesc::Webhook { .. } => {
                    (DataSource::Webhook, Some(source_status_collection_id))
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
        };

        let collections: Vec<_> = catalog
            .entries()
            .filter_map(|entry| {
                let id = entry.id();
                match entry.item() {
                    CatalogItem::Source(source) => Some((id, source_desc(source))),
                    CatalogItem::Table(table) => {
                        let collection_desc = CollectionDescription::from_desc(
                            table.desc.clone(),
                            DataSourceOther::TableWrites,
                        );
                        Some((id, collection_desc))
                    }
                    CatalogItem::MaterializedView(mv) => {
                        let collection_desc = CollectionDescription::from_desc(
                            mv.desc.clone(),
                            DataSourceOther::Compute,
                        );
                        Some((id, collection_desc))
                    }
                    _ => None,
                }
            })
            .collect();

        self.controller
            .storage
            .migrate_collections(collections.clone())
            .await
            .unwrap_or_terminate("cannot fail to migrate collections");

        let register_ts = self.get_local_write_ts().await.timestamp;

        self.controller
            .storage
            .create_collections(Some(register_ts), collections)
            .await
            .unwrap_or_terminate("cannot fail to create collections");

        self.apply_local_write(register_ts).await;
    }

    /// Invokes the optimizer on all indexes and materialized views in the catalog and inserts the
    /// resulting dataflow plans into the catalog state.
    ///
    /// `ordered_catalog_entries` must by sorted in dependency order, with dependencies ordered
    /// before their dependants.
    ///
    /// This method does not perform timestamp selection for the dataflows, nor does it create them
    /// in the compute controller. Both of these steps happen later during bootstrapping.
    fn bootstrap_dataflow_plans(
        &mut self,
        ordered_catalog_entries: &[CatalogEntry],
    ) -> Result<(), AdapterError> {
        // The optimizer expects to be able to query its `ComputeInstanceSnapshot` for
        // collections the current dataflow can depend on. But since we don't yet install anything
        // on compute instances, the snapshot information is incomplete. We fix that by manually
        // updating `ComputeInstanceSnapshot` objects to ensure they contain collections previously
        // optimized.
        let mut instance_snapshots = BTreeMap::new();

        let optimizer_config = OptimizerConfig::from(self.catalog().system_config());

        for entry in ordered_catalog_entries {
            let id = entry.id();
            match entry.item() {
                CatalogItem::Index(idx) => {
                    // Collect optimizer parameters.
                    let compute_instance =
                        instance_snapshots.entry(idx.cluster_id).or_insert_with(|| {
                            self.instance_snapshot(idx.cluster_id)
                                .expect("compute instance exists")
                        });

                    // The index may already be installed on the compute instance. For example,
                    // this is the case for introspection indexes.
                    if compute_instance.contains_collection(&id) {
                        continue;
                    }

                    // Build an optimizer for this INDEX.
                    let mut optimizer = optimize::index::Optimizer::new(
                        self.owned_catalog(),
                        compute_instance.clone(),
                        entry.id(),
                        optimizer_config.clone(),
                    );

                    // MIR ⇒ MIR optimization (global)
                    let index_plan = optimize::index::Index::new(entry.name(), &idx.on, &idx.keys);
                    let global_mir_plan = optimizer.optimize(index_plan)?;
                    let optimized_plan = global_mir_plan.df_desc().clone();

                    // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                    let global_lir_plan = optimizer.optimize(global_mir_plan)?;

                    let (physical_plan, metainfo) = global_lir_plan.unapply();
                    let metainfo = self.catalog().render_notices(metainfo, Some(entry.id()));

                    let catalog = self.catalog_mut();
                    catalog.set_optimized_plan(id, optimized_plan);
                    catalog.set_physical_plan(id, physical_plan);
                    catalog.set_dataflow_metainfo(id, metainfo);

                    compute_instance.insert_collection(id);
                }
                CatalogItem::MaterializedView(mv) => {
                    // Collect optimizer parameters.
                    let compute_instance =
                        instance_snapshots.entry(mv.cluster_id).or_insert_with(|| {
                            self.instance_snapshot(mv.cluster_id)
                                .expect("compute instance exists")
                        });
                    let internal_view_id = self.allocate_transient_id()?;
                    let debug_name = self
                        .catalog()
                        .resolve_full_name(entry.name(), None)
                        .to_string();

                    // Build an optimizer for this MATERIALIZED VIEW.
                    let mut optimizer = optimize::materialized_view::Optimizer::new(
                        self.owned_catalog(),
                        compute_instance.clone(),
                        entry.id(),
                        internal_view_id,
                        mv.desc.iter_names().cloned().collect(),
                        mv.non_null_assertions.clone(),
                        debug_name,
                        optimizer_config.clone(),
                    );

                    // MIR ⇒ MIR optimization (global)
                    let global_mir_plan = optimizer.optimize(mv.optimized_expr.clone())?;
                    let optimized_plan = global_mir_plan.df_desc().clone();

                    // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                    let global_lir_plan = optimizer.optimize(global_mir_plan)?;

                    let (physical_plan, metainfo) = global_lir_plan.unapply();
                    let metainfo = self.catalog().render_notices(metainfo, Some(entry.id()));

                    let catalog = self.catalog_mut();
                    catalog.set_optimized_plan(id, optimized_plan);
                    catalog.set_physical_plan(id, physical_plan);
                    catalog.set_dataflow_metainfo(id, metainfo);

                    compute_instance.insert_collection(id);
                }
                _ => (),
            }
        }

        Ok(())
    }

    /// Collects for each index the materialized views that depend on it, either directly or
    /// transitively through other indexes (but not through other MVs).
    ///
    /// The returned information is required during coordinator bootstrap for index as-of
    /// selection, to ensure that selected as-ofs satisfy the requirements of downstream MVs.
    ///
    /// This method expects all dataflow plans to be available, so it must run after
    /// [`Coordinator::bootstrap_dataflow_plans`].
    fn collect_index_dependent_matviews(&self) -> BTreeMap<GlobalId, BTreeSet<GlobalId>> {
        // Collect imports of all indexes and MVs in the catalog.
        let mut index_imports = BTreeMap::new();
        let mut mv_imports = BTreeMap::new();
        let catalog = self.catalog();
        for entry in catalog.entries() {
            let id = entry.id();
            if let Some(plan) = catalog.try_get_physical_plan(&id) {
                let imports: Vec<_> = plan.import_ids().collect();
                if entry.is_index() {
                    index_imports.insert(id, imports);
                } else if entry.is_materialized_view() {
                    mv_imports.insert(id, imports);
                }
            }
        }

        // Start with an empty set of dependants for each index.
        let mut dependants: BTreeMap<_, _> = index_imports
            .keys()
            .map(|id| (*id, BTreeSet::new()))
            .collect();

        // Collect direct dependants first.
        for (mv_id, mv_deps) in &mv_imports {
            for dep_id in mv_deps {
                if let Some(ids) = dependants.get_mut(dep_id) {
                    ids.insert(*mv_id);
                } else {
                    // `dep_id` references a source import.
                    // We ignore it since we only want to collect dependants on indexes.
                }
            }
        }

        // Collect transitive dependants.
        loop {
            let mut changed = false;

            // Objects with larger IDs tend to depend on objects with smaller IDs. We don't want to
            // rely on that being true, but we can use it to be more efficient.
            for (idx_id, idx_deps) in index_imports.iter().rev() {
                // For each dependency of this index, and each MV depending on this index, add the
                // transitive dependency to `dependants`.
                //
                // I.e., if `dep_id <- idx_id` and `idx_id <- mv_id`, then we add `dep_id <- mv_id`
                // to `dependants`.

                let mv_ids = dependants.get(idx_id).expect("inserted above").clone();
                if mv_ids.is_empty() {
                    continue;
                }

                for dep_id in idx_deps {
                    if let Some(ids) = dependants.get_mut(dep_id) {
                        changed |= mv_ids.iter().any(|mv_id| ids.insert(*mv_id));
                    } else {
                        // `dep_id` references a source import.
                    }
                }
            }

            if !changed {
                break;
            }
        }

        dependants
    }

    /// Returns an `as_of` suitable for bootstrapping the given index dataflow.
    fn bootstrap_index_as_of(
        &self,
        dataflow: &DataflowDescription<Plan>,
        cluster_id: ComputeInstanceId,
        is_retained_metrics_index: bool,
        dependent_matviews: BTreeSet<GlobalId>,
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
            tracing::info!(
                export_ids = %dataflow.display_export_ids(),
                %cluster_id,
                min_as_of = ?min_as_of.elements(),
                write_frontier = ?write_frontier.elements(),
                "selecting index `as_of` as {:?}",
                min_as_of.elements(),
            );

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

        let time = write_frontier.clone().into_option().expect("checked above");
        let time = time.saturating_sub(lag);
        let max_compaction_frontier = Antichain::from_elem(time);

        // We must not select an `as_of` that is beyond any times that have not yet been written to
        // downstream materialized views. If we would, we might skip times in the output of these
        // materialized views, violating correctness. So our chosen `as_of` must be at most the
        // meet of the `upper`s of all dependent materialized views.
        //
        // An exception are materialized views that have an `upper` that's less than their `since`
        // (most likely because they have not yet produced their snapshot). For these views we only
        // need to provide output starting from their `since`s, so these serve as upper bounds for
        // our `as_of`.
        let mut max_as_of = Antichain::new();
        for mv_id in &dependent_matviews {
            let since = self.storage_implied_capability(*mv_id);
            let upper = self.storage_write_frontier(*mv_id);
            max_as_of.meet_assign(&since.join(upper));
        }

        let as_of = if PartialOrder::less_equal(&min_as_of, &max_as_of) {
            min_as_of.join(&max_compaction_frontier).meet(&max_as_of)
        } else {
            // This should not happen. If we get here that means we _will_ skip times in some of
            // the dependent materialized views, which is a correctness bug. However, skipping
            // times in materialized views is probably preferable to panicking and thus making the
            // entire environment unavailable. So we chose to handle this case gracefully and only
            // log an error, unless soft-asserts are enabled, and continue with the `min_as_of` to
            // make the dependent materialized views skip as few times as possible.

            mz_ore::soft_panic_or_log!(
                "error bootstrapping index `as_of`: \
                 `min_as_of` {:?} greater than `max_as_of` {:?} \
                 (import_ids={}, export_ids={}, dependent_matviews={:?})",
                min_as_of.elements(),
                max_as_of.elements(),
                dataflow.display_import_ids(),
                dataflow.display_export_ids(),
                dependent_matviews,
            );

            min_as_of.clone()
        };

        tracing::info!(
            export_ids = %dataflow.display_export_ids(),
            %cluster_id,
            as_of = ?as_of.elements(),
            min_as_of = ?min_as_of.elements(),
            max_as_of = ?max_as_of.elements(),
            write_frontier = ?write_frontier.elements(),
            %lag,
            max_compaction_frontier = ?max_compaction_frontier.elements(),
            "bootstrapping index `as_of`",
        );

        as_of
    }

    /// Returns an `as_of` suitable for bootstrapping the given materialized view dataflow.
    fn bootstrap_materialized_view_as_of(
        &self,
        dataflow: &DataflowDescription<Plan>,
        cluster_id: ComputeInstanceId,
    ) -> Antichain<Timestamp> {
        // All inputs must be readable at the chosen `as_of`, so it must be at least the join of
        // the `since`s of all dependencies.
        let id_bundle = dataflow_import_id_bundle(dataflow, cluster_id);
        let min_as_of = self.least_valid_read(&id_bundle);

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
        let as_of = if write_frontier.is_empty() {
            min_as_of.clone()
        } else {
            min_as_of.join(write_frontier)
        };

        tracing::info!(
            export_ids = %dataflow.display_export_ids(),
            %cluster_id,
            as_of = ?as_of.elements(),
            min_as_of = ?min_as_of.elements(),
            write_frontier = ?write_frontier.elements(),
            "bootstrapping materialized view `as_of`",
        );

        as_of
    }

    /// Serves the coordinator, receiving commands from users over `cmd_rx`
    /// and feedback from dataflow workers over `feedback_rx`.
    ///
    /// You must call `bootstrap` before calling this method.
    ///
    /// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 92KB. This would
    /// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
    /// Because of that we purposefully move this Future onto the heap (i.e. Box it).
    fn serve(
        mut self,
        mut internal_cmd_rx: mpsc::UnboundedReceiver<Message>,
        mut strict_serializable_reads_rx: mpsc::UnboundedReceiver<PendingReadTxn>,
        mut cmd_rx: mpsc::UnboundedReceiver<(OpenTelemetryContext, Command)>,
        group_commit_rx: appends::GroupCommitWaiter,
    ) -> LocalBoxFuture<'static, ()> {
        struct LastMessage {
            kind: &'static str,
            stmt: Option<Arc<Statement<Raw>>>,
        }

        async move {
            // Watcher that listens for and reports cluster service status changes.
            let mut cluster_events = self.controller.events_stream();
            let last_message = Arc::new(Mutex::new(LastMessage {
                kind: "none",
                stmt: None,
            }));

            let (idle_tx, mut idle_rx) = tokio::sync::mpsc::channel(1);
            let idle_metric = self.metrics.queue_busy_seconds.with_label_values(&[]);
            let last_message_watchdog = Arc::clone(&last_message);

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
                            let last_message = last_message_watchdog.lock().expect("poisoned");
                            let last_message_sql = last_message
                                .stmt
                                .as_ref()
                                .map(|stmt| stmt.to_ast_string_redacted())
                                .display_or("<none>");
                            tracing::error!(
                                last_message_kind = %last_message.kind,
                                %last_message_sql,
                                "coordinator stuck for {duration:?}",
                            );
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

            self.schedule_storage_usage_collection().await;
            self.spawn_privatelink_vpc_endpoints_watch_task();
            self.spawn_statement_logging_task();
            flags::tracing_config(self.catalog.system_config()).apply(&self.tracing_handle);

            // Report if the handling of a single message takes longer than this threshold.
            let prometheus_threshold = self
                .catalog
                .system_config()
                .coord_slow_message_reporting_threshold();

            loop {
                // Before adding a branch to this select loop, please ensure that the branch is
                // cancellation safe and add a comment explaining why. You can refer here for more
                // info: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
                let msg = select! {
                    // Order matters here. Some correctness properties rely on us processing
                    // internal commands before processing external commands.
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
                    // See [`appends::GroupCommitWaiter`] for notes on why this is cancel safe.
                    permit = group_commit_rx.ready() => {
                        // If we happen to have batched exactly one user write, use
                        // that span so the `emit_trace_id_notice` hooks up.
                        // Otherwise, the best we can do is invent a new root span
                        // and make it follow from all the Spans in the pending
                        // writes.
                        let user_write_spans = self.pending_writes.iter().flat_map(|x| match x {
                            PendingWriteTxn::User{span, ..} => Some(span),
                            PendingWriteTxn::System{..} => None,
                        });
                        let span = match user_write_spans.exactly_one() {
                            Ok(span) => span.clone(),
                            Err(user_write_spans) => {
                                let span = info_span!(parent: None, "group_commit_notify");
                                for s in user_write_spans {
                                    span.follows_from(s);
                                }
                                span
                            }
                        };
                        Message::GroupCommitInitiate(span, Some(permit))
                    },
                    // `recv()` on `UnboundedReceiver` is cancellation safe:
                    // https://docs.rs/tokio/1.8.0/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety
                    m = cmd_rx.recv() => match m {
                        None => break,
                        Some((otel_ctx, m)) => {
                            Message::Command(otel_ctx, m)

                        }
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

                // All message processing functions trace. Start a parent span
                // for them to make it easy to find slow messages.
                let msg_kind = msg.kind();
                let span = span!(
                    Level::DEBUG,
                    "coordinator processing (handle_message)",
                    kind = msg_kind
                );
                let otel_context = span.context().span().span_context().clone();

                // Record the last kind of message in case we get stuck. For
                // execute commands, we additionally stash the user's SQL,
                // statement, so we can log it in case we get stuck.
                *last_message.lock().expect("poisoned") = LastMessage {
                    kind: msg_kind,
                    stmt: match &msg {
                        Message::Command(
                            _,
                            Command::Execute {
                                portal_name,
                                session,
                                ..
                            },
                        ) => session
                            .get_portal_unverified(portal_name)
                            .and_then(|p| p.stmt.as_ref().map(Arc::clone)),
                        _ => None,
                    },
                };

                let start = Instant::now();
                self.handle_message(span, msg).await;
                let duration = start.elapsed();

                // Report slow messages to Prometheus.
                if duration > prometheus_threshold {
                    self.metrics
                        .slow_message_handling
                        .with_label_values(&[msg_kind])
                        .observe(duration.as_secs_f64());
                }

                // If something is _really_ slow, print a trace id for debugging, if OTEL is enabled.
                let trace_id_threshold = Duration::from_secs(5).min(prometheus_threshold * 25);
                if duration > trace_id_threshold && otel_context.is_valid() {
                    let trace_id = otel_context.trace_id();
                    tracing::warn!(
                        ?msg_kind,
                        ?trace_id,
                        ?duration,
                        "very slow coordinator message"
                    );
                }
            }
            // Try and cleanup as a best effort. There may be some async tasks out there holding a
            // reference that prevents us from cleaning up.
            if let Some(catalog) = Arc::into_inner(self.catalog) {
                catalog.expire().await;
            }
        }
        .boxed_local()
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

    /// Obtain a reference to the coordinator's connection context.
    fn connection_context(&self) -> &ConnectionContext {
        self.controller.connection_context()
    }

    /// Obtain a reference to the coordinator's secret reader.
    fn secrets_reader(&self) -> &dyn SecretsReader {
        &*self.connection_context().secrets_reader
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

    #[tracing::instrument(level = "debug", skip(self, ctx_extra))]
    pub(crate) fn retire_execution(
        &mut self,
        reason: StatementEndedExecutionReason,
        ctx_extra: ExecuteContextExtra,
    ) {
        if let Some(uuid) = ctx_extra.retire() {
            self.end_statement_execution(uuid, reason);
        }
    }

    /// Creates a new dataflow builder from the catalog and indexes in `self`.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn dataflow_builder(&self, instance: ComputeInstanceId) -> DataflowBuilder {
        let compute = self
            .instance_snapshot(instance)
            .expect("compute instance does not exist");
        DataflowBuilder::new(self.catalog().state(), compute)
    }

    /// Return a reference-less snapshot to the indicated compute instance.
    pub fn instance_snapshot(
        &self,
        id: ComputeInstanceId,
    ) -> Result<ComputeInstanceSnapshot, InstanceMissing> {
        ComputeInstanceSnapshot::new(&self.controller, id)
    }

    /// Call into the compute controller to install a finalized dataflow, and
    /// initialize the read policies for its exported objects.
    pub(crate) async fn ship_dataflow(
        &mut self,
        dataflow: DataflowDescription<Plan>,
        instance: ComputeInstanceId,
    ) {
        let export_ids = dataflow.export_ids().collect();

        self.controller
            .active_compute()
            .create_dataflow(instance, dataflow)
            .unwrap_or_terminate("dataflow creation cannot fail");

        self.initialize_compute_read_policies(export_ids, instance, CompactionWindow::Default)
            .await;
    }
}

#[cfg(test)]
impl Coordinator {
    #[allow(dead_code)]
    async fn verify_ship_dataflow_no_error(&mut self, dataflow: DataflowDescription<Plan>) {
        // `ship_dataflow_new` is not allowed to have a `Result` return because this function is
        // called after `catalog_transact`, after which no errors are allowed. This test exists to
        // prevent us from incorrectly teaching those functions how to return errors (which has
        // happened twice and is the motivation for this test).

        // An arbitrary compute instance ID to satisfy the function calls below. Note that
        // this only works because this function will never run.
        let compute_instance = ComputeInstanceId::User(1);

        let _: () = self.ship_dataflow(dataflow, compute_instance).await;
    }
}

/// Serves the coordinator based on the provided configuration.
///
/// For a high-level description of the coordinator, see the [crate
/// documentation](crate).
///
/// Returns a handle to the coordinator and a client to communicate with the
/// coordinator.
///
/// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 42KB. This would
/// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
/// Because of that we purposefully move this Future onto the heap (i.e. Box it).
pub fn serve(
    Config {
        controller_config,
        controller_envd_epoch,
        controller_persist_txn_tables,
        mut storage,
        timestamp_oracle_url,
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
        builtin_cluster_replica_size,
        system_parameter_defaults,
        availability_zones,
        storage_usage_client,
        storage_usage_collection_interval,
        storage_usage_retention_period,
        segment_client,
        egress_ips,
        aws_account_id,
        aws_privatelink_availability_zones,
        connection_context,
        system_parameter_sync_config,
        active_connection_count,
        webhook_concurrency_limit,
        http_host_name,
        tracing_handle,
    }: Config,
) -> BoxFuture<'static, Result<(Handle, Client), AdapterError>> {
    async move {
        info!("coordinator init: beginning");

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();
        let (group_commit_tx, group_commit_rx) = appends::notifier();
        let (strict_serializable_reads_tx, strict_serializable_reads_rx) =
            mpsc::unbounded_channel();

        // Validate and process availability zones.
        if !availability_zones.iter().all_unique() {
            coord_bail!("availability zones must be unique");
        }

        let aws_principal_context = match (
            aws_account_id,
            connection_context.aws_external_id_prefix.clone(),
        ) {
            (Some(aws_account_id), Some(aws_external_id_prefix)) => Some(AwsPrincipalContext {
                aws_account_id,
                aws_external_id_prefix,
            }),
            _ => None,
        };

        let aws_privatelink_availability_zones = aws_privatelink_availability_zones
            .map(|azs_vec| BTreeSet::from_iter(azs_vec.iter().cloned()));

        info!("coordinator init: opening catalog");
        let remote_system_parameters =
            load_remote_system_parameters(&mut storage, system_parameter_sync_config).await?;
        let (catalog, builtin_migration_metadata, builtin_table_updates, _last_catalog_version) =
            Catalog::open(mz_catalog::config::Config {
                storage,
                metrics_registry: &metrics_registry,
                storage_usage_retention_period,
                state: mz_catalog::config::StateConfig {
                    unsafe_mode,
                    all_features,
                    build_info,
                    environment_id: environment_id.clone(),
                    now: now.clone(),
                    skip_migrations: false,
                    cluster_replica_sizes,
                    default_storage_cluster_size,
                    builtin_cluster_replica_size,
                    system_parameter_defaults,
                    remote_system_parameters,
                    availability_zones,
                    egress_ips,
                    aws_principal_context,
                    aws_privatelink_availability_zones,
                    connection_context,
                    active_connection_count,
                    http_host_name,
                },
            })
            .await?;
        let session_id = catalog.config().session_id;
        let start_instant = catalog.config().start_instant;

        // In order for the coordinator to support Rc and Refcell types, it cannot be
        // sent across threads. Spawn it in a thread and have this parent thread wait
        // for bootstrap completion before proceeding.
        let (bootstrap_tx, bootstrap_rx) = oneshot::channel();
        let handle = TokioHandle::current();

        let metrics = Metrics::register_into(&metrics_registry);
        let metrics_clone = metrics.clone();
        let segment_client_clone = segment_client.clone();
        let span = tracing::Span::current();
        let coord_now = now.clone();
        let advance_timelines_interval = tokio::time::interval(catalog.config().timestamp_interval);

        // We get the timestamp oracle impl once on startup, to ensure that it
        // doesn't change in between when the system var changes: all oracles must
        // use the same impl!
        let timestamp_oracle_impl = catalog.system_config().timestamp_oracle_impl();

        let pg_timestamp_oracle_config = timestamp_oracle_url
            .map(|pg_url| PostgresTimestampOracleConfig::new(&pg_url, &metrics_registry));
        if let Some(config) = pg_timestamp_oracle_config.as_ref() {
            // Apply settings from system vars as early as possible because some
            // of them are locked in right when an oracle is first opened!
            let pg_timestamp_oracle_params =
                flags::pg_timstamp_oracle_config(catalog.system_config());
            pg_timestamp_oracle_params.apply(config);
        }

        let initial_timestamps =
            get_initial_oracle_timestamps(&catalog, &pg_timestamp_oracle_config).await?;

        let thread = thread::Builder::new()
            // The Coordinator thread tends to keep a lot of data on its stack. To
            // prevent a stack overflow we allocate a stack three times as big as the default
            // stack.
            .stack_size(3 * stack::STACK_SIZE)
            .name("coordinator".to_string())
            .spawn(move || {
                let catalog = Arc::new(catalog);

                let mut timestamp_oracles = BTreeMap::new();
                for (timeline, initial_timestamp) in initial_timestamps {
                    let persistence =
                        CatalogTimestampPersistence::new(timeline.clone(), Arc::clone(&catalog));

                    handle.block_on(Coordinator::ensure_timeline_state_with_initial_time(
                        &timeline,
                        initial_timestamp,
                        coord_now.clone(),
                        timestamp_oracle_impl,
                        persistence,
                        pg_timestamp_oracle_config.clone(),
                        &mut timestamp_oracles,
                    ));
                }

                let controller = handle.block_on(
                    mz_controller::Controller::new(
                        controller_config,
                        controller_envd_epoch,
                        controller_persist_txn_tables,
                    )
                    .boxed(),
                );

                let caching_secrets_reader = CachingSecretsReader::new(secrets_controller.reader());
                let mut coord = Coordinator {
                    controller,
                    catalog,
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
                    active_webhooks: BTreeMap::new(),
                    write_lock: Arc::new(tokio::sync::Mutex::new(())),
                    write_lock_wait_group: VecDeque::new(),
                    pending_writes: Vec::new(),
                    advance_timelines_interval,
                    secrets_controller,
                    caching_secrets_reader,
                    cloud_resource_controller,
                    transient_replica_metadata: BTreeMap::new(),
                    storage_usage_client,
                    storage_usage_collection_interval,
                    segment_client,
                    metrics,
                    tracing_handle,
                    statement_logging: StatementLogging::new(),
                    webhook_concurrency_limit,
                    timestamp_oracle_impl,
                    pg_timestamp_oracle_config,
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
                        strict_serializable_reads_rx,
                        cmd_rx,
                        group_commit_rx,
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
    .boxed()
}

// While we have two implementations of TimestampOracle (catalog-backed and
// postgres/crdb-backed), we determine the highest timestamp for each timeline
// on bootstrap, to initialize the currently-configured oracle. This mostly
// works, but there can be linearizability violations, because there is no
// central moment where do distributed coordination for both oracle types.
// Working around this seems prohibitively hard, maybe even impossible so we
// have to live with this window of potential violations during the upgrade
// window (which is the only point where we should switch oracle
// implementations).
//
// NOTE: We can remove all this code, including the pre-existing code below that
// initializes oracles on bootstrap, once we have fully migrated to the new
// postgres/crdb-backed oracle.
async fn get_initial_oracle_timestamps(
    catalog: &Catalog,
    pg_timestamp_oracle_config: &Option<PostgresTimestampOracleConfig>,
) -> Result<BTreeMap<Timeline, Timestamp>, AdapterError> {
    let catalog_oracle_timestamps = catalog.get_all_persisted_timestamps().await?;
    let debug_msg = || {
        catalog_oracle_timestamps
            .iter()
            .map(|(timeline, ts)| format!("{:?} -> {}", timeline, ts))
            .join(", ")
    };
    info!(
        "current timestamps from the catalog-backed timestamp oracle: {}",
        debug_msg()
    );

    let mut initial_timestamps = catalog_oracle_timestamps;
    if let Some(pg_timestamp_oracle_config) = pg_timestamp_oracle_config {
        let postgres_oracle_timestamps =
            PostgresTimestampOracle::<NowFn>::get_all_timelines(pg_timestamp_oracle_config.clone())
                .await?;

        let debug_msg = || {
            postgres_oracle_timestamps
                .iter()
                .map(|(timeline, ts)| format!("{:?} -> {}", timeline, ts))
                .join(", ")
        };
        info!(
            "current timestamps from the postgres-backed timestamp oracle: {}",
            debug_msg()
        );

        for (timeline, ts) in postgres_oracle_timestamps {
            let entry = initial_timestamps
                .entry(Timeline::from_str(&timeline).expect("could not parse timeline"));

            entry
                .and_modify(|current_ts| *current_ts = std::cmp::max(*current_ts, ts))
                .or_insert(ts);
        }
    } else {
        info!("no postgres url for postgres-backed timestamp oracle configured!");
    };

    let debug_msg = || {
        initial_timestamps
            .iter()
            .map(|(timeline, ts)| format!("{:?}: {}", timeline, ts))
            .join(", ")
    };
    info!("initial oracle timestamps: {}", debug_msg());

    Ok(initial_timestamps)
}

/// Potentially load system parameters from a remote frontend server.
#[tracing::instrument(level = "info", skip_all)]
async fn load_remote_system_parameters(
    storage: &mut Box<dyn DurableCatalogState>,
    system_parameter_sync_config: Option<SystemParameterSyncConfig>,
) -> Result<Option<BTreeMap<String, OwnedVarInput>>, AdapterError> {
    if let Some(system_parameter_sync_config) = system_parameter_sync_config {
        tracing::info!("parameter sync on boot: start sync");

        // We intentionally block initial startup, potentially forever,
        // on initializing LaunchDarkly. This may seem scary, but the
        // alternative is even scarier. Over time, we expect that the
        // compiled-in default values for the system parameters will
        // drift substantially from the defaults configured in
        // LaunchDarkly, to the point that starting an environment
        // without loading the latest values from LaunchDarkly will
        // result in running an untested configuration.
        //
        // Note this only applies during initial startup. Restarting
        // after we've synced once doesn't block on LaunchDarkly, as it
        // seems reasonable to assume that the last-synced configuration
        // was valid enough.
        //
        // This philosophy appears to provide a good balance between not
        // running untested configurations in production while also not
        // making LaunchDarkly a "tier 1" dependency for existing
        // environments.
        //
        // If this proves to be an issue, we could seek to address the
        // configuration drift in a different way--for example, by
        // writing a script that runs in CI nightly and checks for
        // deviation between the compiled Rust code and LaunchDarkly.
        //
        // If it is absolutely necessary to bring up a new environment
        // while LaunchDarkly is down, the following manual mitigation
        // can be performed:
        //
        //    1. Edit the environmentd startup parameters to omit the
        //       LaunchDarkly configuration.
        //    2. Boot environmentd.
        //    3. Use the catalog-debug tool to run `edit config "{\"key\":\"system_config_synced\"}" "{\"value\": 1}"`.
        //    4. Adjust any other parameters as necessary to avoid
        //       running a nonstandard configuration in production.
        //    5. Edit the environmentd startup parameters to restore the
        //       LaunchDarkly configuration, for when LaunchDarkly comes
        //       back online.
        //    6. Reboot environmentd.
        if storage.is_read_only() {
            tracing::info!("parameter sync on boot: skipping sync as catalog is read-only");
        } else if !storage.has_system_config_synced_once().await? {
            let mut params = SynchronizedParameters::new(SystemVars::default());
            let frontend = SystemParameterFrontend::from(&system_parameter_sync_config).await?;
            frontend.pull(&mut params);
            let ops = params
                .modified()
                .into_iter()
                .map(|param| {
                    let name = param.name;
                    let value = param.value;
                    tracing::info!(name, value, initial = true, "sync parameter");
                    (name, OwnedVarInput::Flat(value))
                })
                .collect();
            tracing::info!("parameter sync on boot: end sync");
            return Ok(Some(ops));
        } else {
            tracing::info!("parameter sync on boot: skipping sync as config has synced once");
        }
    }

    Ok(None)
}
