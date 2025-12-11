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

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::net::IpAddr;
use std::num::NonZeroI64;
use std::ops::Neg;
use std::str::FromStr;
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::{DateTime, Utc};
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use fail::fail_point;
use futures::StreamExt;
use futures::future::{BoxFuture, FutureExt, LocalBoxFuture};
use http::Uri;
use ipnet::IpNet;
use itertools::{Either, Itertools};
use mz_adapter_types::bootstrap_builtin_cluster_config::BootstrapBuiltinClusterConfig;
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_adapter_types::dyncfgs::WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL;
use mz_auth::password::Password;
use mz_build_info::BuildInfo;
use mz_catalog::builtin::{BUILTINS, BUILTINS_STATIC, MZ_AUDIT_EVENTS, MZ_STORAGE_USAGE_BY_SHARD};
use mz_catalog::config::{AwsPrincipalContext, BuiltinItemMigrationConfig, ClusterReplicaSizeMap};
use mz_catalog::durable::{AuditLogIterator, OpenableDurableCatalogState};
use mz_catalog::expr_cache::{GlobalExpressions, LocalExpressions};
use mz_catalog::memory::objects::{
    CatalogEntry, CatalogItem, ClusterReplicaProcessStatus, ClusterVariantManaged, Connection,
    DataSourceDesc, StateDiff, StateUpdate, StateUpdateKind, Table, TableDataSource,
};
use mz_cloud_resources::{CloudResourceController, VpcEndpointConfig, VpcEndpointEvent};
use mz_compute_client::as_of_selection;
use mz_compute_client::controller::error::{DataflowCreationError, InstanceMissing};
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_controller::clusters::{
    ClusterConfig, ClusterEvent, ClusterStatus, ProcessId, ReplicaLocation,
};
use mz_controller::{ControllerConfig, Readiness};
use mz_controller_types::{ClusterId, ReplicaId, WatchSetId};
use mz_expr::{MapFilterProject, OptimizedMirRelationExpr, RowSetFinishing};
use mz_license_keys::ValidatedLicenseKey;
use mz_orchestrator::OfflineReason;
use mz_ore::cast::{CastFrom, CastInto, CastLossy};
use mz_ore::channel::trigger::Trigger;
use mz_ore::future::TimeoutError;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::task::{JoinHandle, spawn};
use mz_ore::thread::JoinHandleExt;
use mz_ore::tracing::{OpenTelemetryContext, TracingHandle};
use mz_ore::url::SensitiveUrl;
use mz_ore::{
    assert_none, instrument, soft_assert_eq_or_log, soft_assert_or_log, soft_panic_or_log, stack,
};
use mz_persist_client::PersistClient;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::usage::{ShardsUsageReferenced, StorageUsageClient};
use mz_repr::adt::numeric::Numeric;
use mz_repr::explain::{ExplainConfig, ExplainFormat};
use mz_repr::global_id::TransientIdGen;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationDesc, Timestamp};
use mz_secrets::cache::CachingSecretsReader;
use mz_secrets::{SecretsController, SecretsReader};
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::{CatalogCluster, EnvironmentId};
use mz_sql::names::{QualifiedItemName, ResolvedIds, SchemaSpecifier};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::{
    self, AlterSinkPlan, ConnectionDetails, CreateConnectionPlan, NetworkPolicyRule,
    OnTimeoutAction, Params, QueryWhen,
};
use mz_sql::session::user::User;
use mz_sql::session::vars::{MAX_CREDIT_CONSUMPTION_RATE, SystemVars, Var};
use mz_sql_parser::ast::ExplainStage;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::client::TableData;
use mz_storage_client::controller::{CollectionDescription, DataSource, ExportDescription};
use mz_storage_types::connections::Connection as StorageConnection;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::connections::inline::{IntoInlineConnection, ReferencedConnection};
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::sinks::{S3SinkFormat, StorageSinkDesc};
use mz_storage_types::sources::kafka::KAFKA_PROGRESS_DESC;
use mz_storage_types::sources::{IngestionDescription, SourceExport, Timeline};
use mz_timestamp_oracle::WriteTimestamp;
use mz_timestamp_oracle::postgres_oracle::{
    PostgresTimestampOracle, PostgresTimestampOracleConfig,
};
use mz_transform::dataflow::DataflowMetainfo;
use opentelemetry::trace::TraceContextExt;
use serde::Serialize;
use thiserror::Error;
use timely::progress::{Antichain, Timestamp as _};
use tokio::runtime::Handle as TokioHandle;
use tokio::select;
use tokio::sync::{OwnedMutexGuard, mpsc, oneshot, watch};
use tokio::time::{Interval, MissedTickBehavior};
use tracing::{Instrument, Level, Span, debug, info, info_span, span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::active_compute_sink::{ActiveComputeSink, ActiveCopyFrom};
use crate::catalog::{BuiltinTableUpdate, Catalog, OpenCatalogResult};
use crate::client::{Client, Handle};
use crate::command::{Command, ExecuteResponse};
use crate::config::{SynchronizedParameters, SystemParameterFrontend, SystemParameterSyncConfig};
use crate::coord::appends::{
    BuiltinTableAppendNotify, DeferredOp, GroupCommitPermit, PendingWriteTxn,
};
use crate::coord::caught_up::CaughtUpCheckContext;
use crate::coord::cluster_scheduling::SchedulingDecision;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::introspection::IntrospectionSubscribe;
use crate::coord::peek::PendingPeek;
use crate::coord::statement_logging::{StatementLogging, StatementLoggingId};
use crate::coord::timeline::{TimelineContext, TimelineState};
use crate::coord::timestamp_selection::{TimestampContext, TimestampDetermination};
use crate::coord::validity::PlanValidity;
use crate::error::AdapterError;
use crate::explain::insights::PlanInsightsContext;
use crate::explain::optimizer_trace::{DispatchGuard, OptimizerTrace};
use crate::metrics::Metrics;
use crate::optimize::dataflows::{
    ComputeInstanceSnapshot, DataflowBuilder, dataflow_import_id_bundle,
};
use crate::optimize::{self, Optimize, OptimizerConfig};
use crate::session::{EndTransactionAction, Session};
use crate::statement_logging::{StatementEndedExecutionReason, StatementLifecycleEvent};
use crate::util::{ClientTransmitter, ResultExt};
use crate::webhook::{WebhookAppenderInvalidator, WebhookConcurrencyLimiter};
use crate::{AdapterNotice, ReadHolds, flags};

pub(crate) mod appends;
pub(crate) mod catalog_serving;
pub(crate) mod cluster_scheduling;
pub(crate) mod consistency;
pub(crate) mod id_bundle;
pub(crate) mod in_memory_oracle;
pub(crate) mod peek;
pub(crate) mod read_policy;
pub(crate) mod sequencer;
pub(crate) mod statement_logging;
pub(crate) mod timeline;
pub(crate) mod timestamp_selection;

pub mod catalog_implications;
mod caught_up;
mod command_handler;
mod ddl;
mod indexes;
mod introspection;
mod message_handler;
mod privatelink_status;
mod sql;
mod validity;

#[derive(Debug)]
pub enum Message {
    Command(OpenTelemetryContext, Command),
    ControllerReady {
        controller: ControllerReadiness,
    },
    PurifiedStatementReady(PurifiedStatementReady),
    CreateConnectionValidationReady(CreateConnectionValidationReady),
    AlterConnectionValidationReady(AlterConnectionValidationReady),
    TryDeferred {
        /// The connection that created this op.
        conn_id: ConnectionId,
        /// The write lock that notified us our deferred op might be able to run.
        ///
        /// Note: While we never want to hold a partial set of locks, it can be important to hold
        /// onto the _one_ that notified us our op might be ready. If there are multiple operations
        /// waiting on a single collection, and we don't hold this lock through retyring the op,
        /// then everything waiting on this collection will get retried causing traffic in the
        /// Coordinator's message queue.
        ///
        /// See [`DeferredOp::can_be_optimistically_retried`] for more detail.
        acquired_lock: Option<(CatalogItemId, tokio::sync::OwnedMutexGuard<()>)>,
    },
    /// Initiates a group commit.
    GroupCommitInitiate(Span, Option<GroupCommitPermit>),
    DeferredStatementReady,
    AdvanceTimelines,
    ClusterEvent(ClusterEvent),
    CancelPendingPeeks {
        conn_id: ConnectionId,
    },
    LinearizeReads,
    StagedBatches {
        conn_id: ConnectionId,
        table_id: CatalogItemId,
        batches: Vec<Result<ProtoBatch, String>>,
    },
    StorageUsageSchedule,
    StorageUsageFetch,
    StorageUsageUpdate(ShardsUsageReferenced),
    StorageUsagePrune(Vec<BuiltinTableUpdate>),
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
        span: Span,
        stage: PeekStage,
    },
    CreateIndexStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: CreateIndexStage,
    },
    CreateViewStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: CreateViewStage,
    },
    CreateMaterializedViewStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: CreateMaterializedViewStage,
    },
    SubscribeStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: SubscribeStage,
    },
    IntrospectionSubscribeStageReady {
        span: Span,
        stage: IntrospectionSubscribeStage,
    },
    SecretStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: SecretStage,
    },
    ClusterStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: ClusterStage,
    },
    ExplainTimestampStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: ExplainTimestampStage,
    },
    DrainStatementLog,
    PrivateLinkVpcEndpointEvents(Vec<VpcEndpointEvent>),
    CheckSchedulingPolicies,

    /// Scheduling policy decisions about turning clusters On/Off.
    /// `Vec<(policy name, Vec of decisions by the policy)>`
    /// A cluster will be On if and only if there is at least one On decision for it.
    /// Scheduling decisions for clusters that have `SCHEDULE = MANUAL` are ignored.
    SchedulingDecisions(Vec<(&'static str, Vec<(ClusterId, SchedulingDecision)>)>),
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
                Command::Dump { .. } => "command-dump",
                Command::AuthenticatePassword { .. } => "command-auth_check",
                Command::AuthenticateGetSASLChallenge { .. } => "command-auth_get_sasl_challenge",
                Command::AuthenticateVerifySASLProof { .. } => "command-auth_verify_sasl_proof",
                Command::GetComputeInstanceClient { .. } => "get-compute-instance-client",
                Command::GetOracle { .. } => "get-oracle",
                Command::DetermineRealTimeRecentTimestamp { .. } => {
                    "determine-real-time-recent-timestamp"
                }
                Command::GetTransactionReadHoldsBundle { .. } => {
                    "get-transaction-read-holds-bundle"
                }
                Command::StoreTransactionReadHolds { .. } => "store-transaction-read-holds",
                Command::ExecuteSlowPathPeek { .. } => "execute-slow-path-peek",
                Command::ExecuteCopyTo { .. } => "execute-copy-to",
            },
            Message::ControllerReady {
                controller: ControllerReadiness::Compute,
            } => "controller_ready(compute)",
            Message::ControllerReady {
                controller: ControllerReadiness::Storage,
            } => "controller_ready(storage)",
            Message::ControllerReady {
                controller: ControllerReadiness::Metrics,
            } => "controller_ready(metrics)",
            Message::ControllerReady {
                controller: ControllerReadiness::Internal,
            } => "controller_ready(internal)",
            Message::PurifiedStatementReady(_) => "purified_statement_ready",
            Message::CreateConnectionValidationReady(_) => "create_connection_validation_ready",
            Message::TryDeferred { .. } => "try_deferred",
            Message::GroupCommitInitiate(..) => "group_commit_initiate",
            Message::AdvanceTimelines => "advance_timelines",
            Message::ClusterEvent(_) => "cluster_event",
            Message::CancelPendingPeeks { .. } => "cancel_pending_peeks",
            Message::LinearizeReads => "linearize_reads",
            Message::StagedBatches { .. } => "staged_batches",
            Message::StorageUsageSchedule => "storage_usage_schedule",
            Message::StorageUsageFetch => "storage_usage_fetch",
            Message::StorageUsageUpdate(_) => "storage_usage_update",
            Message::StorageUsagePrune(_) => "storage_usage_prune",
            Message::RetireExecute { .. } => "retire_execute",
            Message::ExecuteSingleStatementTransaction { .. } => {
                "execute_single_statement_transaction"
            }
            Message::PeekStageReady { .. } => "peek_stage_ready",
            Message::ExplainTimestampStageReady { .. } => "explain_timestamp_stage_ready",
            Message::CreateIndexStageReady { .. } => "create_index_stage_ready",
            Message::CreateViewStageReady { .. } => "create_view_stage_ready",
            Message::CreateMaterializedViewStageReady { .. } => {
                "create_materialized_view_stage_ready"
            }
            Message::SubscribeStageReady { .. } => "subscribe_stage_ready",
            Message::IntrospectionSubscribeStageReady { .. } => {
                "introspection_subscribe_stage_ready"
            }
            Message::SecretStageReady { .. } => "secret_stage_ready",
            Message::ClusterStageReady { .. } => "cluster_stage_ready",
            Message::DrainStatementLog => "drain_statement_log",
            Message::AlterConnectionValidationReady(..) => "alter_connection_validation_ready",
            Message::PrivateLinkVpcEndpointEvents(_) => "private_link_vpc_endpoint_events",
            Message::CheckSchedulingPolicies => "check_scheduling_policies",
            Message::SchedulingDecisions { .. } => "scheduling_decision",
            Message::DeferredStatementReady => "deferred_statement_ready",
        }
    }
}

/// The reason for why a controller needs processing on the main loop.
#[derive(Debug)]
pub enum ControllerReadiness {
    /// The storage controller is ready.
    Storage,
    /// The compute controller is ready.
    Compute,
    /// A batch of metric data is ready.
    Metrics,
    /// An internally-generated message is ready to be returned.
    Internal,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct BackgroundWorkResult<T> {
    #[derivative(Debug = "ignore")]
    pub ctx: ExecuteContext,
    pub result: Result<T, AdapterError>,
    pub params: Params,
    pub plan_validity: PlanValidity,
    pub original_stmt: Arc<Statement<Raw>>,
    pub otel_ctx: OpenTelemetryContext,
}

pub type PurifiedStatementReady = BackgroundWorkResult<mz_sql::pure::PurifiedStatement>;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ValidationReady<T> {
    #[derivative(Debug = "ignore")]
    pub ctx: ExecuteContext,
    pub result: Result<T, AdapterError>,
    pub resolved_ids: ResolvedIds,
    pub connection_id: CatalogItemId,
    pub connection_gid: GlobalId,
    pub plan_validity: PlanValidity,
    pub otel_ctx: OpenTelemetryContext,
}

pub type CreateConnectionValidationReady = ValidationReady<CreateConnectionPlan>;
pub type AlterConnectionValidationReady = ValidationReady<Connection>;

#[derive(Debug)]
pub enum PeekStage {
    /// Common stages across SELECT, EXPLAIN and COPY TO queries.
    LinearizeTimestamp(PeekStageLinearizeTimestamp),
    RealTimeRecency(PeekStageRealTimeRecency),
    TimestampReadHold(PeekStageTimestampReadHold),
    Optimize(PeekStageOptimize),
    /// Final stage for a peek.
    Finish(PeekStageFinish),
    /// Final stage for an explain.
    ExplainPlan(PeekStageExplainPlan),
    ExplainPushdown(PeekStageExplainPushdown),
    /// Preflight checks for a copy to operation.
    CopyToPreflight(PeekStageCopyTo),
    /// Final stage for a copy to which involves shipping the dataflow.
    CopyToDataflow(PeekStageCopyTo),
}

#[derive(Debug)]
pub struct CopyToContext {
    /// The `RelationDesc` of the data to be copied.
    pub desc: RelationDesc,
    /// The destination uri of the external service where the data will be copied.
    pub uri: Uri,
    /// Connection information required to connect to the external service to copy the data.
    pub connection: StorageConnection<ReferencedConnection>,
    /// The ID of the CONNECTION object to be used for copying the data.
    pub connection_id: CatalogItemId,
    /// Format params to format the data.
    pub format: S3SinkFormat,
    /// Approximate max file size of each uploaded file.
    pub max_file_size: u64,
    /// Number of batches the output of the COPY TO will be partitioned into
    /// to distribute the load across workers deterministically.
    /// This is only an option since it's not set when CopyToContext is instantiated
    /// but immediately after in the PeekStageValidate stage.
    pub output_batch_count: Option<u64>,
}

#[derive(Debug)]
pub struct PeekStageLinearizeTimestamp {
    validity: PlanValidity,
    plan: mz_sql::plan::SelectPlan,
    max_query_result_size: Option<u64>,
    source_ids: BTreeSet<GlobalId>,
    target_replica: Option<ReplicaId>,
    timeline_context: TimelineContext,
    optimizer: Either<optimize::peek::Optimizer, optimize::copy_to::Optimizer>,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
}

#[derive(Debug)]
pub struct PeekStageRealTimeRecency {
    validity: PlanValidity,
    plan: mz_sql::plan::SelectPlan,
    max_query_result_size: Option<u64>,
    source_ids: BTreeSet<GlobalId>,
    target_replica: Option<ReplicaId>,
    timeline_context: TimelineContext,
    oracle_read_ts: Option<Timestamp>,
    optimizer: Either<optimize::peek::Optimizer, optimize::copy_to::Optimizer>,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
}

#[derive(Debug)]
pub struct PeekStageTimestampReadHold {
    validity: PlanValidity,
    plan: mz_sql::plan::SelectPlan,
    max_query_result_size: Option<u64>,
    source_ids: BTreeSet<GlobalId>,
    target_replica: Option<ReplicaId>,
    timeline_context: TimelineContext,
    oracle_read_ts: Option<Timestamp>,
    real_time_recency_ts: Option<mz_repr::Timestamp>,
    optimizer: Either<optimize::peek::Optimizer, optimize::copy_to::Optimizer>,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
}

#[derive(Debug)]
pub struct PeekStageOptimize {
    validity: PlanValidity,
    plan: mz_sql::plan::SelectPlan,
    max_query_result_size: Option<u64>,
    source_ids: BTreeSet<GlobalId>,
    id_bundle: CollectionIdBundle,
    target_replica: Option<ReplicaId>,
    determination: TimestampDetermination<mz_repr::Timestamp>,
    optimizer: Either<optimize::peek::Optimizer, optimize::copy_to::Optimizer>,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
}

#[derive(Debug)]
pub struct PeekStageFinish {
    validity: PlanValidity,
    plan: mz_sql::plan::SelectPlan,
    max_query_result_size: Option<u64>,
    id_bundle: CollectionIdBundle,
    target_replica: Option<ReplicaId>,
    source_ids: BTreeSet<GlobalId>,
    determination: TimestampDetermination<mz_repr::Timestamp>,
    cluster_id: ComputeInstanceId,
    finishing: RowSetFinishing,
    /// When present, an optimizer trace to be used for emitting a plan insights
    /// notice.
    plan_insights_optimizer_trace: Option<OptimizerTrace>,
    insights_ctx: Option<Box<PlanInsightsContext>>,
    global_lir_plan: optimize::peek::GlobalLirPlan,
    optimization_finished_at: EpochMillis,
}

#[derive(Debug)]
pub struct PeekStageCopyTo {
    validity: PlanValidity,
    optimizer: optimize::copy_to::Optimizer,
    global_lir_plan: optimize::copy_to::GlobalLirPlan,
    optimization_finished_at: EpochMillis,
    source_ids: BTreeSet<GlobalId>,
}

#[derive(Debug)]
pub struct PeekStageExplainPlan {
    validity: PlanValidity,
    optimizer: optimize::peek::Optimizer,
    df_meta: DataflowMetainfo,
    explain_ctx: ExplainPlanContext,
    insights_ctx: Option<Box<PlanInsightsContext>>,
}

#[derive(Debug)]
pub struct PeekStageExplainPushdown {
    validity: PlanValidity,
    determination: TimestampDetermination<mz_repr::Timestamp>,
    imports: BTreeMap<GlobalId, MapFilterProject>,
}

#[derive(Debug)]
pub enum CreateIndexStage {
    Optimize(CreateIndexOptimize),
    Finish(CreateIndexFinish),
    Explain(CreateIndexExplain),
}

#[derive(Debug)]
pub struct CreateIndexOptimize {
    validity: PlanValidity,
    plan: plan::CreateIndexPlan,
    resolved_ids: ResolvedIds,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
}

#[derive(Debug)]
pub struct CreateIndexFinish {
    validity: PlanValidity,
    item_id: CatalogItemId,
    global_id: GlobalId,
    plan: plan::CreateIndexPlan,
    resolved_ids: ResolvedIds,
    global_mir_plan: optimize::index::GlobalMirPlan,
    global_lir_plan: optimize::index::GlobalLirPlan,
}

#[derive(Debug)]
pub struct CreateIndexExplain {
    validity: PlanValidity,
    exported_index_id: GlobalId,
    plan: plan::CreateIndexPlan,
    df_meta: DataflowMetainfo,
    explain_ctx: ExplainPlanContext,
}

#[derive(Debug)]
pub enum CreateViewStage {
    Optimize(CreateViewOptimize),
    Finish(CreateViewFinish),
    Explain(CreateViewExplain),
}

#[derive(Debug)]
pub struct CreateViewOptimize {
    validity: PlanValidity,
    plan: plan::CreateViewPlan,
    resolved_ids: ResolvedIds,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
}

#[derive(Debug)]
pub struct CreateViewFinish {
    validity: PlanValidity,
    /// ID of this item in the Catalog.
    item_id: CatalogItemId,
    /// ID by with Compute will reference this View.
    global_id: GlobalId,
    plan: plan::CreateViewPlan,
    /// IDs of objects resolved during name resolution.
    resolved_ids: ResolvedIds,
    optimized_expr: OptimizedMirRelationExpr,
}

#[derive(Debug)]
pub struct CreateViewExplain {
    validity: PlanValidity,
    id: GlobalId,
    plan: plan::CreateViewPlan,
    explain_ctx: ExplainPlanContext,
}

#[derive(Debug)]
pub enum ExplainTimestampStage {
    Optimize(ExplainTimestampOptimize),
    RealTimeRecency(ExplainTimestampRealTimeRecency),
    Finish(ExplainTimestampFinish),
}

#[derive(Debug)]
pub struct ExplainTimestampOptimize {
    validity: PlanValidity,
    plan: plan::ExplainTimestampPlan,
    cluster_id: ClusterId,
}

#[derive(Debug)]
pub struct ExplainTimestampRealTimeRecency {
    validity: PlanValidity,
    format: ExplainFormat,
    optimized_plan: OptimizedMirRelationExpr,
    cluster_id: ClusterId,
    when: QueryWhen,
}

#[derive(Debug)]
pub struct ExplainTimestampFinish {
    validity: PlanValidity,
    format: ExplainFormat,
    optimized_plan: OptimizedMirRelationExpr,
    cluster_id: ClusterId,
    source_ids: BTreeSet<GlobalId>,
    when: QueryWhen,
    real_time_recency_ts: Option<Timestamp>,
}

#[derive(Debug)]
pub enum ClusterStage {
    Alter(AlterCluster),
    WaitForHydrated(AlterClusterWaitForHydrated),
    Finalize(AlterClusterFinalize),
}

#[derive(Debug)]
pub struct AlterCluster {
    validity: PlanValidity,
    plan: plan::AlterClusterPlan,
}

#[derive(Debug)]
pub struct AlterClusterWaitForHydrated {
    validity: PlanValidity,
    plan: plan::AlterClusterPlan,
    new_config: ClusterVariantManaged,
    timeout_time: Instant,
    on_timeout: OnTimeoutAction,
}

#[derive(Debug)]
pub struct AlterClusterFinalize {
    validity: PlanValidity,
    plan: plan::AlterClusterPlan,
    new_config: ClusterVariantManaged,
}

#[derive(Debug)]
pub enum ExplainContext {
    /// The ordinary, non-explain variant of the statement.
    None,
    /// The `EXPLAIN <level> PLAN FOR <explainee>` version of the statement.
    Plan(ExplainPlanContext),
    /// Generate a notice containing the `EXPLAIN PLAN INSIGHTS` output
    /// alongside the query's normal output.
    PlanInsightsNotice(OptimizerTrace),
    /// `EXPLAIN FILTER PUSHDOWN`
    Pushdown,
}

impl ExplainContext {
    /// If available for this context, wrap the [`OptimizerTrace`] into a
    /// [`tracing::Dispatch`] and set it as default, returning the resulting
    /// guard in a `Some(guard)` option.
    pub(crate) fn dispatch_guard(&self) -> Option<DispatchGuard<'_>> {
        let optimizer_trace = match self {
            ExplainContext::Plan(explain_ctx) => Some(&explain_ctx.optimizer_trace),
            ExplainContext::PlanInsightsNotice(optimizer_trace) => Some(optimizer_trace),
            _ => None,
        };
        optimizer_trace.map(|optimizer_trace| optimizer_trace.as_guard())
    }

    pub(crate) fn needs_cluster(&self) -> bool {
        match self {
            ExplainContext::None => true,
            ExplainContext::Plan(..) => false,
            ExplainContext::PlanInsightsNotice(..) => true,
            ExplainContext::Pushdown => false,
        }
    }

    pub(crate) fn needs_plan_insights(&self) -> bool {
        matches!(
            self,
            ExplainContext::Plan(ExplainPlanContext {
                stage: ExplainStage::PlanInsights,
                ..
            }) | ExplainContext::PlanInsightsNotice(_)
        )
    }
}

#[derive(Debug)]
pub struct ExplainPlanContext {
    /// EXPLAIN BROKEN is internal syntax for showing EXPLAIN output despite an internal error in
    /// the optimizer: we don't immediately bail out from peek sequencing when an internal optimizer
    /// error happens, but go on with trying to show the requested EXPLAIN stage. This can still
    /// succeed if the requested EXPLAIN stage is before the point where the error happened.
    pub broken: bool,
    pub config: ExplainConfig,
    pub format: ExplainFormat,
    pub stage: ExplainStage,
    pub replan: Option<GlobalId>,
    pub desc: Option<RelationDesc>,
    pub optimizer_trace: OptimizerTrace,
}

#[derive(Debug)]
pub enum CreateMaterializedViewStage {
    Optimize(CreateMaterializedViewOptimize),
    Finish(CreateMaterializedViewFinish),
    Explain(CreateMaterializedViewExplain),
}

#[derive(Debug)]
pub struct CreateMaterializedViewOptimize {
    validity: PlanValidity,
    plan: plan::CreateMaterializedViewPlan,
    resolved_ids: ResolvedIds,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
}

#[derive(Debug)]
pub struct CreateMaterializedViewFinish {
    /// The ID of this Materialized View in the Catalog.
    item_id: CatalogItemId,
    /// The ID of the durable pTVC backing this Materialized View.
    global_id: GlobalId,
    validity: PlanValidity,
    plan: plan::CreateMaterializedViewPlan,
    resolved_ids: ResolvedIds,
    local_mir_plan: optimize::materialized_view::LocalMirPlan,
    global_mir_plan: optimize::materialized_view::GlobalMirPlan,
    global_lir_plan: optimize::materialized_view::GlobalLirPlan,
}

#[derive(Debug)]
pub struct CreateMaterializedViewExplain {
    global_id: GlobalId,
    validity: PlanValidity,
    plan: plan::CreateMaterializedViewPlan,
    df_meta: DataflowMetainfo,
    explain_ctx: ExplainPlanContext,
}

#[derive(Debug)]
pub enum SubscribeStage {
    OptimizeMir(SubscribeOptimizeMir),
    TimestampOptimizeLir(SubscribeTimestampOptimizeLir),
    Finish(SubscribeFinish),
}

#[derive(Debug)]
pub struct SubscribeOptimizeMir {
    validity: PlanValidity,
    plan: plan::SubscribePlan,
    timeline: TimelineContext,
    dependency_ids: BTreeSet<GlobalId>,
    cluster_id: ComputeInstanceId,
    replica_id: Option<ReplicaId>,
}

#[derive(Debug)]
pub struct SubscribeTimestampOptimizeLir {
    validity: PlanValidity,
    plan: plan::SubscribePlan,
    timeline: TimelineContext,
    optimizer: optimize::subscribe::Optimizer,
    global_mir_plan: optimize::subscribe::GlobalMirPlan<optimize::subscribe::Unresolved>,
    dependency_ids: BTreeSet<GlobalId>,
    replica_id: Option<ReplicaId>,
}

#[derive(Debug)]
pub struct SubscribeFinish {
    validity: PlanValidity,
    cluster_id: ComputeInstanceId,
    replica_id: Option<ReplicaId>,
    plan: plan::SubscribePlan,
    global_lir_plan: optimize::subscribe::GlobalLirPlan,
    dependency_ids: BTreeSet<GlobalId>,
}

#[derive(Debug)]
pub enum IntrospectionSubscribeStage {
    OptimizeMir(IntrospectionSubscribeOptimizeMir),
    TimestampOptimizeLir(IntrospectionSubscribeTimestampOptimizeLir),
    Finish(IntrospectionSubscribeFinish),
}

#[derive(Debug)]
pub struct IntrospectionSubscribeOptimizeMir {
    validity: PlanValidity,
    plan: plan::SubscribePlan,
    subscribe_id: GlobalId,
    cluster_id: ComputeInstanceId,
    replica_id: ReplicaId,
}

#[derive(Debug)]
pub struct IntrospectionSubscribeTimestampOptimizeLir {
    validity: PlanValidity,
    optimizer: optimize::subscribe::Optimizer,
    global_mir_plan: optimize::subscribe::GlobalMirPlan<optimize::subscribe::Unresolved>,
    cluster_id: ComputeInstanceId,
    replica_id: ReplicaId,
}

#[derive(Debug)]
pub struct IntrospectionSubscribeFinish {
    validity: PlanValidity,
    global_lir_plan: optimize::subscribe::GlobalLirPlan,
    read_holds: ReadHolds<Timestamp>,
    cluster_id: ComputeInstanceId,
    replica_id: ReplicaId,
}

#[derive(Debug)]
pub enum SecretStage {
    CreateEnsure(CreateSecretEnsure),
    CreateFinish(CreateSecretFinish),
    RotateKeysEnsure(RotateKeysSecretEnsure),
    RotateKeysFinish(RotateKeysSecretFinish),
    Alter(AlterSecret),
}

#[derive(Debug)]
pub struct CreateSecretEnsure {
    validity: PlanValidity,
    plan: plan::CreateSecretPlan,
}

#[derive(Debug)]
pub struct CreateSecretFinish {
    validity: PlanValidity,
    item_id: CatalogItemId,
    global_id: GlobalId,
    plan: plan::CreateSecretPlan,
}

#[derive(Debug)]
pub struct RotateKeysSecretEnsure {
    validity: PlanValidity,
    id: CatalogItemId,
}

#[derive(Debug)]
pub struct RotateKeysSecretFinish {
    validity: PlanValidity,
    ops: Vec<crate::catalog::Op>,
}

#[derive(Debug)]
pub struct AlterSecret {
    validity: PlanValidity,
    plan: plan::AlterSecretPlan,
}

/// An enum describing which cluster to run a statement on.
///
/// One example usage would be that if a query depends only on system tables, we might
/// automatically run it on the catalog server cluster to benefit from indexes that exist there.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TargetCluster {
    /// The catalog server cluster.
    CatalogServer,
    /// The current user's active cluster.
    Active,
    /// The cluster selected at the start of a transaction.
    Transaction(ClusterId),
}

/// Result types for each stage of a sequence.
pub(crate) enum StageResult<T> {
    /// A task was spawned that will return the next stage.
    Handle(JoinHandle<Result<T, AdapterError>>),
    /// A task was spawned that will return a response for the client.
    HandleRetire(JoinHandle<Result<ExecuteResponse, AdapterError>>),
    /// The next stage is immediately ready and will execute.
    Immediate(T),
    /// The final stage was executed and is ready to respond to the client.
    Response(ExecuteResponse),
}

/// Common functionality for [Coordinator::sequence_staged].
pub(crate) trait Staged: Send {
    type Ctx: StagedContext;

    fn validity(&mut self) -> &mut PlanValidity;

    /// Returns the next stage or final result.
    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut Self::Ctx,
    ) -> Result<StageResult<Box<Self>>, AdapterError>;

    /// Prepares a message for the Coordinator.
    fn message(self, ctx: Self::Ctx, span: Span) -> Message;

    /// Whether it is safe to SQL cancel this stage.
    fn cancel_enabled(&self) -> bool;
}

pub trait StagedContext {
    fn retire(self, result: Result<ExecuteResponse, AdapterError>);
    fn session(&self) -> Option<&Session>;
}

impl StagedContext for ExecuteContext {
    fn retire(self, result: Result<ExecuteResponse, AdapterError>) {
        self.retire(result);
    }

    fn session(&self) -> Option<&Session> {
        Some(self.session())
    }
}

impl StagedContext for () {
    fn retire(self, _result: Result<ExecuteResponse, AdapterError>) {}

    fn session(&self) -> Option<&Session> {
        None
    }
}

/// Configures a coordinator.
pub struct Config {
    pub controller_config: ControllerConfig,
    pub controller_envd_epoch: NonZeroI64,
    pub storage: Box<dyn mz_catalog::durable::DurableCatalogState>,
    pub audit_logs_iterator: AuditLogIterator,
    pub timestamp_oracle_url: Option<SensitiveUrl>,
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
    pub builtin_system_cluster_config: BootstrapBuiltinClusterConfig,
    pub builtin_catalog_server_cluster_config: BootstrapBuiltinClusterConfig,
    pub builtin_probe_cluster_config: BootstrapBuiltinClusterConfig,
    pub builtin_support_cluster_config: BootstrapBuiltinClusterConfig,
    pub builtin_analytics_cluster_config: BootstrapBuiltinClusterConfig,
    pub system_parameter_defaults: BTreeMap<String, String>,
    pub storage_usage_client: StorageUsageClient,
    pub storage_usage_collection_interval: Duration,
    pub storage_usage_retention_period: Option<Duration>,
    pub segment_client: Option<mz_segment::Client>,
    pub egress_addresses: Vec<IpNet>,
    pub remote_system_parameters: Option<BTreeMap<String, String>>,
    pub aws_account_id: Option<String>,
    pub aws_privatelink_availability_zones: Option<Vec<String>>,
    pub connection_context: ConnectionContext,
    pub connection_limit_callback: Box<dyn Fn(u64, u64) -> () + Send + Sync + 'static>,
    pub webhook_concurrency_limit: WebhookConcurrencyLimiter,
    pub http_host_name: Option<String>,
    pub tracing_handle: TracingHandle,
    /// Whether or not to start controllers in read-only mode. This is only
    /// meant for use during development of read-only clusters and 0dt upgrades
    /// and should go away once we have proper orchestration during upgrades.
    pub read_only_controllers: bool,

    /// A trigger that signals that the current deployment has caught up with a
    /// previous deployment. Only used during 0dt deployment, while in read-only
    /// mode.
    pub caught_up_trigger: Option<Trigger>,

    pub helm_chart_version: Option<String>,
    pub license_key: ValidatedLicenseKey,
    pub external_login_password_mz_system: Option<Password>,
    pub force_builtin_schema_migration: Option<String>,
}

/// Metadata about an active connection.
#[derive(Debug, Serialize)]
pub struct ConnMeta {
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
    client_ip: Option<IpAddr>,

    /// Sinks that will need to be dropped when the current transaction, if
    /// any, is cleared.
    drop_sinks: BTreeSet<GlobalId>,

    /// Lock for the Coordinator's deferred statements that is dropped on transaction clear.
    #[serde(skip)]
    deferred_lock: Option<OwnedMutexGuard<()>>,

    /// Cluster reconfigurations that will need to be
    /// cleaned up when the current transaction is cleared
    pending_cluster_alters: BTreeSet<ClusterId>,

    /// Channel on which to send notices to a session.
    #[serde(skip)]
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

    pub fn client_ip(&self) -> Option<IpAddr> {
        self.client_ip
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
    /// The timestamp context of the transaction.
    timestamp_context: TimestampContext<mz_repr::Timestamp>,
    /// When we created this pending txn, when the transaction ends. Only used for metrics.
    created: Instant,
    /// Number of times we requeued the processing of this pending read txn.
    /// Requeueing is necessary if the time we executed the query is after the current oracle time;
    /// see [`Coordinator::message_linearize_reads`] for more details.
    num_requeues: u64,
    /// Telemetry context.
    otel_ctx: OpenTelemetryContext,
}

impl PendingReadTxn {
    /// Return the timestamp context of the pending read transaction.
    pub fn timestamp_context(&self) -> &TimestampContext<mz_repr::Timestamp> {
        &self.timestamp_context
    }

    pub(crate) fn take_context(self) -> ExecuteContext {
        self.txn.take_context()
    }
}

#[derive(Debug)]
/// A pending read transaction waiting to be linearized.
enum PendingRead {
    Read {
        /// The inner transaction.
        txn: PendingTxn,
    },
    ReadThenWrite {
        /// Context used to send a response back to the client.
        ctx: ExecuteContext,
        /// Channel used to alert the transaction that the read has been linearized and send back
        /// `ctx`.
        tx: oneshot::Sender<Option<ExecuteContext>>,
    },
}

impl PendingRead {
    /// Alert the client that the read has been linearized.
    ///
    /// If it is necessary to finalize an execute, return the state necessary to do so
    /// (execution context and result)
    #[instrument(level = "debug")]
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
            PendingRead::ReadThenWrite { ctx, tx, .. } => {
                // Ignore errors if the caller has hung up.
                let _ = tx.send(Some(ctx));
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

    pub(crate) fn take_context(self) -> ExecuteContext {
        match self {
            PendingRead::Read { txn, .. } => txn.ctx,
            PendingRead::ReadThenWrite { ctx, tx, .. } => {
                // Inform the transaction that we've taken their context.
                // Ignore errors if the caller has hung up.
                let _ = tx.send(None);
                ctx
            }
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
            soft_panic_or_log!(
                "execute context for statement {statement_uuid:?} dropped without being properly retired."
            );
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
    inner: Box<ExecuteContextInner>,
}

impl std::ops::Deref for ExecuteContext {
    type Target = ExecuteContextInner;
    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl std::ops::DerefMut for ExecuteContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.inner
    }
}

#[derive(Debug)]
pub struct ExecuteContextInner {
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
            inner: ExecuteContextInner {
                tx,
                session,
                extra,
                internal_cmd_tx,
            }
            .into(),
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
        let ExecuteContextInner {
            tx,
            internal_cmd_tx,
            session,
            extra,
        } = *self.inner;
        (tx, internal_cmd_tx, session, extra)
    }

    /// Retire the execution, by sending a message to the coordinator.
    #[instrument(level = "debug")]
    pub fn retire(self, result: Result<ExecuteResponse, AdapterError>) {
        let ExecuteContextInner {
            tx,
            internal_cmd_tx,
            session,
            extra,
        } = *self.inner;
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

#[derive(Debug)]
struct ClusterReplicaStatuses(
    BTreeMap<ClusterId, BTreeMap<ReplicaId, BTreeMap<ProcessId, ClusterReplicaProcessStatus>>>,
);

impl ClusterReplicaStatuses {
    pub(crate) fn new() -> ClusterReplicaStatuses {
        ClusterReplicaStatuses(BTreeMap::new())
    }

    /// Initializes the statuses of the specified cluster.
    ///
    /// Panics if the cluster statuses are already initialized.
    pub(crate) fn initialize_cluster_statuses(&mut self, cluster_id: ClusterId) {
        let prev = self.0.insert(cluster_id, BTreeMap::new());
        assert_eq!(
            prev, None,
            "cluster {cluster_id} statuses already initialized"
        );
    }

    /// Initializes the statuses of the specified cluster replica.
    ///
    /// Panics if the cluster replica statuses are already initialized.
    pub(crate) fn initialize_cluster_replica_statuses(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        num_processes: usize,
        time: DateTime<Utc>,
    ) {
        tracing::info!(
            ?cluster_id,
            ?replica_id,
            ?time,
            "initializing cluster replica status"
        );
        let replica_statuses = self.0.entry(cluster_id).or_default();
        let process_statuses = (0..num_processes)
            .map(|process_id| {
                let status = ClusterReplicaProcessStatus {
                    status: ClusterStatus::Offline(Some(OfflineReason::Initializing)),
                    time: time.clone(),
                };
                (u64::cast_from(process_id), status)
            })
            .collect();
        let prev = replica_statuses.insert(replica_id, process_statuses);
        assert_none!(
            prev,
            "cluster replica {cluster_id}.{replica_id} statuses already initialized"
        );
    }

    /// Removes the statuses of the specified cluster.
    ///
    /// Panics if the cluster does not exist.
    pub(crate) fn remove_cluster_statuses(
        &mut self,
        cluster_id: &ClusterId,
    ) -> BTreeMap<ReplicaId, BTreeMap<ProcessId, ClusterReplicaProcessStatus>> {
        let prev = self.0.remove(cluster_id);
        prev.unwrap_or_else(|| panic!("unknown cluster: {cluster_id}"))
    }

    /// Removes the statuses of the specified cluster replica.
    ///
    /// Panics if the cluster or replica does not exist.
    pub(crate) fn remove_cluster_replica_statuses(
        &mut self,
        cluster_id: &ClusterId,
        replica_id: &ReplicaId,
    ) -> BTreeMap<ProcessId, ClusterReplicaProcessStatus> {
        let replica_statuses = self
            .0
            .get_mut(cluster_id)
            .unwrap_or_else(|| panic!("unknown cluster: {cluster_id}"));
        let prev = replica_statuses.remove(replica_id);
        prev.unwrap_or_else(|| panic!("unknown cluster replica: {cluster_id}.{replica_id}"))
    }

    /// Inserts or updates the status of the specified cluster replica process.
    ///
    /// Panics if the cluster or replica does not exist.
    pub(crate) fn ensure_cluster_status(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process_id: ProcessId,
        status: ClusterReplicaProcessStatus,
    ) {
        let replica_statuses = self
            .0
            .get_mut(&cluster_id)
            .unwrap_or_else(|| panic!("unknown cluster: {cluster_id}"))
            .get_mut(&replica_id)
            .unwrap_or_else(|| panic!("unknown cluster replica: {cluster_id}.{replica_id}"));
        replica_statuses.insert(process_id, status);
    }

    /// Computes the status of the cluster replica as a whole.
    ///
    /// Panics if `cluster_id` or `replica_id` don't exist.
    pub fn get_cluster_replica_status(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> ClusterStatus {
        let process_status = self.get_cluster_replica_statuses(cluster_id, replica_id);
        Self::cluster_replica_status(process_status)
    }

    /// Computes the status of the cluster replica as a whole.
    pub fn cluster_replica_status(
        process_status: &BTreeMap<ProcessId, ClusterReplicaProcessStatus>,
    ) -> ClusterStatus {
        process_status
            .values()
            .fold(ClusterStatus::Online, |s, p| match (s, p.status) {
                (ClusterStatus::Online, ClusterStatus::Online) => ClusterStatus::Online,
                (x, y) => {
                    let reason_x = match x {
                        ClusterStatus::Offline(reason) => reason,
                        ClusterStatus::Online => None,
                    };
                    let reason_y = match y {
                        ClusterStatus::Offline(reason) => reason,
                        ClusterStatus::Online => None,
                    };
                    // Arbitrarily pick the first known not-ready reason.
                    ClusterStatus::Offline(reason_x.or(reason_y))
                }
            })
    }

    /// Gets the statuses of the given cluster replica.
    ///
    /// Panics if the cluster or replica does not exist
    pub(crate) fn get_cluster_replica_statuses(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &BTreeMap<ProcessId, ClusterReplicaProcessStatus> {
        self.try_get_cluster_replica_statuses(cluster_id, replica_id)
            .unwrap_or_else(|| panic!("unknown cluster replica: {cluster_id}.{replica_id}"))
    }

    /// Gets the statuses of the given cluster replica.
    pub(crate) fn try_get_cluster_replica_statuses(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> Option<&BTreeMap<ProcessId, ClusterReplicaProcessStatus>> {
        self.try_get_cluster_statuses(cluster_id)
            .and_then(|statuses| statuses.get(&replica_id))
    }

    /// Gets the statuses of the given cluster.
    pub(crate) fn try_get_cluster_statuses(
        &self,
        cluster_id: ClusterId,
    ) -> Option<&BTreeMap<ReplicaId, BTreeMap<ProcessId, ClusterReplicaProcessStatus>>> {
        self.0.get(&cluster_id)
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

    /// A client for persist. Initially, this is only used for reading stashed
    /// peek responses out of batches.
    persist_client: PersistClient,

    /// Channel to manage internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    /// Notification that triggers a group commit.
    group_commit_tx: appends::GroupCommitNotifier,

    /// Channel for strict serializable reads ready to commit.
    strict_serializable_reads_tx: mpsc::UnboundedSender<(ConnectionId, PendingReadTxn)>,

    /// Mechanism for totally ordering write and read timestamps, so that all reads
    /// reflect exactly the set of writes that precede them, and no writes that follow.
    global_timelines: BTreeMap<Timeline, TimelineState<Timestamp>>,

    /// A generator for transient [`GlobalId`]s, shareable with other threads.
    transient_id_gen: Arc<TransientIdGen>,
    /// A map from connection ID to metadata about that connection for all
    /// active connections.
    active_conns: BTreeMap<ConnectionId, ConnMeta>,

    /// For each transaction, the read holds taken to support any performed reads.
    ///
    /// Upon completing a transaction, these read holds should be dropped.
    txn_read_holds: BTreeMap<ConnectionId, read_policy::ReadHolds<Timestamp>>,

    /// Access to the peek fields should be restricted to methods in the [`peek`] API.
    /// A map from pending peek ids to the queue into which responses are sent, and
    /// the connection id of the client that initiated the peek.
    pending_peeks: BTreeMap<Uuid, PendingPeek>,
    /// A map from client connection ids to a set of all pending peeks for that client.
    client_pending_peeks: BTreeMap<ConnectionId, BTreeMap<Uuid, ClusterId>>,

    /// A map from client connection ids to pending linearize read transaction.
    pending_linearize_read_txns: BTreeMap<ConnectionId, PendingReadTxn>,

    /// A map from the compute sink ID to it's state description.
    active_compute_sinks: BTreeMap<GlobalId, ActiveComputeSink>,
    /// A map from active webhooks to their invalidation handle.
    active_webhooks: BTreeMap<CatalogItemId, WebhookAppenderInvalidator>,
    /// A map of active `COPY FROM` statements. The Coordinator waits for `clusterd`
    /// to stage Batches in Persist that we will then link into the shard.
    active_copies: BTreeMap<ConnectionId, ActiveCopyFrom>,

    /// A map from connection ids to a watch channel that is set to `true` if the connection
    /// received a cancel request.
    staged_cancellation: BTreeMap<ConnectionId, (watch::Sender<bool>, watch::Receiver<bool>)>,
    /// Active introspection subscribes.
    introspection_subscribes: BTreeMap<GlobalId, IntrospectionSubscribe>,

    /// Locks that grant access to a specific object, populated lazily as objects are written to.
    write_locks: BTreeMap<CatalogItemId, Arc<tokio::sync::Mutex<()>>>,
    /// Plans that are currently deferred and waiting on a write lock.
    deferred_write_ops: BTreeMap<ConnectionId, DeferredOp>,

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
    advance_timelines_interval: Interval,

    /// Serialized DDL. DDL must be serialized because:
    /// - Many of them do off-thread work and need to verify the catalog is in a valid state, but
    ///   [`PlanValidity`] does not currently support tracking all changes. Doing that correctly
    ///   seems to be more difficult than it's worth, so we would instead re-plan and re-sequence
    ///   the statements.
    /// - Re-planning a statement is hard because Coordinator and Session state is mutated at
    ///   various points, and we would need to correctly reset those changes before re-planning and
    ///   re-sequencing.
    serialized_ddl: LockedVecDeque<DeferredPlanStatement>,

    /// Handle to secret manager that can create and delete secrets from
    /// an arbitrary secret storage engine.
    secrets_controller: Arc<dyn SecretsController>,
    /// A secrets reader than maintains an in-memory cache, where values have a set TTL.
    caching_secrets_reader: CachingSecretsReader,

    /// Handle to a manager that can create and delete kubernetes resources
    /// (ie: VpcEndpoint objects)
    cloud_resource_controller: Option<Arc<dyn CloudResourceController>>,

    /// Persist client for fetching storage metadata such as size metrics.
    storage_usage_client: StorageUsageClient,
    /// The interval at which to collect storage usage information.
    storage_usage_collection_interval: Duration,

    /// Segment analytics client.
    #[derivative(Debug = "ignore")]
    segment_client: Option<mz_segment::Client>,

    /// Coordinator metrics.
    metrics: Metrics,
    /// Optimizer metrics.
    optimizer_metrics: OptimizerMetrics,

    /// Tracing handle.
    tracing_handle: TracingHandle,

    /// Data used by the statement logging feature.
    statement_logging: StatementLogging,

    /// Limit for how many concurrent webhook requests we allow.
    webhook_concurrency_limit: WebhookConcurrencyLimiter,

    /// Optional config for the Postgres-backed timestamp oracle. This is
    /// _required_ when `postgres` is configured using the `timestamp_oracle`
    /// system variable.
    pg_timestamp_oracle_config: Option<PostgresTimestampOracleConfig>,

    /// Periodically asks cluster scheduling policies to make their decisions.
    check_cluster_scheduling_policies_interval: Interval,

    /// This keeps the last On/Off decision for each cluster and each scheduling policy.
    /// (Clusters that have been dropped or are otherwise out of scope for automatic scheduling are
    /// periodically cleaned up from this Map.)
    cluster_scheduling_decisions: BTreeMap<ClusterId, BTreeMap<&'static str, SchedulingDecision>>,

    /// When doing 0dt upgrades/in read-only mode, periodically ask all known
    /// clusters/collections whether they are caught up.
    caught_up_check_interval: Interval,

    /// Context needed to check whether all clusters/collections have caught up.
    /// Only used during 0dt deployment, while in read-only mode.
    caught_up_check: Option<CaughtUpCheckContext>,

    /// Tracks the state associated with the currently installed watchsets.
    installed_watch_sets: BTreeMap<WatchSetId, (ConnectionId, WatchSetResponse)>,

    /// Tracks the currently installed watchsets for each connection.
    connection_watch_sets: BTreeMap<ConnectionId, BTreeSet<WatchSetId>>,

    /// Tracks the statuses of all cluster replicas.
    cluster_replica_statuses: ClusterReplicaStatuses,

    /// Whether or not to start controllers in read-only mode. This is only
    /// meant for use during development of read-only clusters and 0dt upgrades
    /// and should go away once we have proper orchestration during upgrades.
    read_only_controllers: bool,

    /// Updates to builtin tables that are being buffered while we are in
    /// read-only mode. We apply these all at once when coming out of read-only
    /// mode.
    ///
    /// This is a `Some` while in read-only mode and will be replaced by a
    /// `None` when we transition out of read-only mode and write out any
    /// buffered updates.
    buffered_builtin_table_updates: Option<Vec<BuiltinTableUpdate>>,

    license_key: ValidatedLicenseKey,
}

impl Coordinator {
    /// Initializes coordinator state based on the contained catalog. Must be
    /// called after creating the coordinator and before calling the
    /// `Coordinator::serve` method.
    #[instrument(name = "coord::bootstrap")]
    pub(crate) async fn bootstrap(
        &mut self,
        boot_ts: Timestamp,
        migrated_storage_collections_0dt: BTreeSet<CatalogItemId>,
        mut builtin_table_updates: Vec<BuiltinTableUpdate>,
        cached_global_exprs: BTreeMap<GlobalId, GlobalExpressions>,
        uncached_local_exprs: BTreeMap<GlobalId, LocalExpressions>,
        audit_logs_iterator: AuditLogIterator,
    ) -> Result<(), AdapterError> {
        let bootstrap_start = Instant::now();
        info!("startup: coordinator init: bootstrap beginning");
        info!("startup: coordinator init: bootstrap: preamble beginning");

        // Initialize cluster replica statuses.
        // Gross iterator is to avoid partial borrow issues.
        let cluster_statuses: Vec<(_, Vec<_>)> = self
            .catalog()
            .clusters()
            .map(|cluster| {
                (
                    cluster.id(),
                    cluster
                        .replicas()
                        .map(|replica| {
                            (replica.replica_id, replica.config.location.num_processes())
                        })
                        .collect(),
                )
            })
            .collect();
        let now = self.now_datetime();
        for (cluster_id, replica_statuses) in cluster_statuses {
            self.cluster_replica_statuses
                .initialize_cluster_statuses(cluster_id);
            for (replica_id, num_processes) in replica_statuses {
                self.cluster_replica_statuses
                    .initialize_cluster_replica_statuses(
                        cluster_id,
                        replica_id,
                        num_processes,
                        now,
                    );
            }
        }

        let system_config = self.catalog().system_config();

        // Inform metrics about the initial system configuration.
        mz_metrics::update_dyncfg(&system_config.dyncfg_updates());

        // Inform the controllers about their initial configuration.
        let compute_config = flags::compute_config(system_config);
        let storage_config = flags::storage_config(system_config);
        let scheduling_config = flags::orchestrator_scheduling_config(system_config);
        let dyncfg_updates = system_config.dyncfg_updates();
        self.controller.compute.update_configuration(compute_config);
        self.controller.storage.update_parameters(storage_config);
        self.controller
            .update_orchestrator_scheduling_config(scheduling_config);
        self.controller.update_configuration(dyncfg_updates);

        self.validate_resource_limit_numeric(
            Numeric::zero(),
            self.current_credit_consumption_rate(),
            |system_vars| {
                self.license_key
                    .max_credit_consumption_rate()
                    .map_or_else(|| system_vars.max_credit_consumption_rate(), Numeric::from)
            },
            "cluster replica",
            MAX_CREDIT_CONSUMPTION_RATE.name(),
        )?;

        let mut policies_to_set: BTreeMap<CompactionWindow, CollectionIdBundle> =
            Default::default();

        let enable_worker_core_affinity =
            self.catalog().system_config().enable_worker_core_affinity();
        for instance in self.catalog.clusters() {
            self.controller.create_cluster(
                instance.id,
                ClusterConfig {
                    arranged_logs: instance.log_indexes.clone(),
                    workload_class: instance.config.workload_class.clone(),
                },
            )?;
            for replica in instance.replicas() {
                let role = instance.role();
                self.controller.create_replica(
                    instance.id,
                    replica.replica_id,
                    instance.name.clone(),
                    replica.name.clone(),
                    role,
                    replica.config.clone(),
                    enable_worker_core_affinity,
                )?;
            }
        }

        info!(
            "startup: coordinator init: bootstrap: preamble complete in {:?}",
            bootstrap_start.elapsed()
        );

        let init_storage_collections_start = Instant::now();
        info!("startup: coordinator init: bootstrap: storage collections init beginning");
        self.bootstrap_storage_collections(&migrated_storage_collections_0dt)
            .await;
        info!(
            "startup: coordinator init: bootstrap: storage collections init complete in {:?}",
            init_storage_collections_start.elapsed()
        );

        // The storage controller knows about the introspection collections now, so we can start
        // sinking introspection updates in the compute controller. It makes sense to do that as
        // soon as possible, to avoid updates piling up in the compute controller's internal
        // buffers.
        self.controller.start_compute_introspection_sink();

        let optimize_dataflows_start = Instant::now();
        info!("startup: coordinator init: bootstrap: optimize dataflow plans beginning");
        let entries: Vec<_> = self.catalog().entries().cloned().collect();
        let uncached_global_exps = self.bootstrap_dataflow_plans(&entries, cached_global_exprs)?;
        info!(
            "startup: coordinator init: bootstrap: optimize dataflow plans complete in {:?}",
            optimize_dataflows_start.elapsed()
        );

        // We don't need to wait for the cache to update.
        let _fut = self.catalog().update_expression_cache(
            uncached_local_exprs.into_iter().collect(),
            uncached_global_exps.into_iter().collect(),
        );

        // Select dataflow as-ofs. This step relies on the storage collections created by
        // `bootstrap_storage_collections` and the dataflow plans created by
        // `bootstrap_dataflow_plans`.
        let bootstrap_as_ofs_start = Instant::now();
        info!("startup: coordinator init: bootstrap: dataflow as-of bootstrapping beginning");
        let dataflow_read_holds = self.bootstrap_dataflow_as_ofs().await;
        info!(
            "startup: coordinator init: bootstrap: dataflow as-of bootstrapping complete in {:?}",
            bootstrap_as_ofs_start.elapsed()
        );

        let postamble_start = Instant::now();
        info!("startup: coordinator init: bootstrap: postamble beginning");

        let logs: BTreeSet<_> = BUILTINS::logs()
            .map(|log| self.catalog().resolve_builtin_log(log))
            .flat_map(|item_id| self.catalog().get_global_ids(&item_id))
            .collect();

        let mut privatelink_connections = BTreeMap::new();

        for entry in &entries {
            debug!(
                "coordinator init: installing {} {}",
                entry.item().typ(),
                entry.id()
            );
            let mut policy = entry.item().initial_logical_compaction_window();
            match entry.item() {
                // Currently catalog item rebuild assumes that sinks and
                // indexes are always built individually and does not store information
                // about how it was built. If we start building multiple sinks and/or indexes
                // using a single dataflow, we have to make sure the rebuild process re-runs
                // the same multiple-build dataflow.
                CatalogItem::Source(source) => {
                    // Propagate source compaction windows to subsources if needed.
                    if source.custom_logical_compaction_window.is_none() {
                        if let DataSourceDesc::IngestionExport { ingestion_id, .. } =
                            source.data_source
                        {
                            policy = Some(
                                self.catalog()
                                    .get_entry(&ingestion_id)
                                    .source()
                                    .expect("must be source")
                                    .custom_logical_compaction_window
                                    .unwrap_or_default(),
                            );
                        }
                    }
                    policies_to_set
                        .entry(policy.expect("sources have a compaction window"))
                        .or_insert_with(Default::default)
                        .storage_ids
                        .insert(source.global_id());
                }
                CatalogItem::Table(table) => {
                    policies_to_set
                        .entry(policy.expect("tables have a compaction window"))
                        .or_insert_with(Default::default)
                        .storage_ids
                        .extend(table.global_ids());
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
                            .insert(idx.global_id());
                    } else {
                        let df_desc = self
                            .catalog()
                            .try_get_physical_plan(&idx.global_id())
                            .expect("added in `bootstrap_dataflow_plans`")
                            .clone();

                        let df_meta = self
                            .catalog()
                            .try_get_dataflow_metainfo(&idx.global_id())
                            .expect("added in `bootstrap_dataflow_plans`");

                        if self.catalog().state().system_config().enable_mz_notices() {
                            // Collect optimization hint updates.
                            self.catalog().state().pack_optimizer_notices(
                                &mut builtin_table_updates,
                                df_meta.optimizer_notices.iter(),
                                Diff::ONE,
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
                            .compute
                            .create_dataflow(idx.cluster_id, df_desc, None)
                            .unwrap_or_terminate("cannot fail to create dataflows");
                    }
                }
                CatalogItem::View(_) => (),
                CatalogItem::MaterializedView(mview) => {
                    policies_to_set
                        .entry(policy.expect("materialized views have a compaction window"))
                        .or_insert_with(Default::default)
                        .storage_ids
                        .insert(mview.global_id_writes());

                    let mut df_desc = self
                        .catalog()
                        .try_get_physical_plan(&mview.global_id_writes())
                        .expect("added in `bootstrap_dataflow_plans`")
                        .clone();

                    if let Some(initial_as_of) = mview.initial_as_of.clone() {
                        df_desc.set_initial_as_of(initial_as_of);
                    }

                    // If we have a refresh schedule that has a last refresh, then set the `until` to the last refresh.
                    let until = mview
                        .refresh_schedule
                        .as_ref()
                        .and_then(|s| s.last_refresh())
                        .and_then(|r| r.try_step_forward());
                    if let Some(until) = until {
                        df_desc.until.meet_assign(&Antichain::from_elem(until));
                    }

                    let df_meta = self
                        .catalog()
                        .try_get_dataflow_metainfo(&mview.global_id_writes())
                        .expect("added in `bootstrap_dataflow_plans`");

                    if self.catalog().state().system_config().enable_mz_notices() {
                        // Collect optimization hint updates.
                        self.catalog().state().pack_optimizer_notices(
                            &mut builtin_table_updates,
                            df_meta.optimizer_notices.iter(),
                            Diff::ONE,
                        );
                    }

                    self.ship_dataflow(df_desc, mview.cluster_id, None).await;

                    // If this is a replacement MV, it must remain read-only until the replacement
                    // gets applied.
                    if mview.replacement_target.is_none() {
                        self.allow_writes(mview.cluster_id, mview.global_id_writes());
                    }
                }
                CatalogItem::Sink(sink) => {
                    policies_to_set
                        .entry(CompactionWindow::Default)
                        .or_insert_with(Default::default)
                        .storage_ids
                        .insert(sink.global_id());
                }
                CatalogItem::Connection(catalog_connection) => {
                    if let ConnectionDetails::AwsPrivatelink(conn) = &catalog_connection.details {
                        privatelink_connections.insert(
                            entry.id(),
                            VpcEndpointConfig {
                                aws_service_name: conn.service_name.clone(),
                                availability_zone_ids: conn.availability_zones.clone(),
                            },
                        );
                    }
                }
                CatalogItem::ContinualTask(ct) => {
                    policies_to_set
                        .entry(policy.expect("continual tasks have a compaction window"))
                        .or_insert_with(Default::default)
                        .storage_ids
                        .insert(ct.global_id());

                    let mut df_desc = self
                        .catalog()
                        .try_get_physical_plan(&ct.global_id())
                        .expect("added in `bootstrap_dataflow_plans`")
                        .clone();

                    if let Some(initial_as_of) = ct.initial_as_of.clone() {
                        df_desc.set_initial_as_of(initial_as_of);
                    }

                    let df_meta = self
                        .catalog()
                        .try_get_dataflow_metainfo(&ct.global_id())
                        .expect("added in `bootstrap_dataflow_plans`");

                    if self.catalog().state().system_config().enable_mz_notices() {
                        // Collect optimization hint updates.
                        self.catalog().state().pack_optimizer_notices(
                            &mut builtin_table_updates,
                            df_meta.optimizer_notices.iter(),
                            Diff::ONE,
                        );
                    }

                    self.ship_dataflow(df_desc, ct.cluster_id, None).await;
                    self.allow_writes(ct.cluster_id, ct.global_id());
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
            let existing_vpc_endpoints = cloud_resource_controller
                .list_vpc_endpoints()
                .await
                .context("list vpc endpoints")?;
            let existing_vpc_endpoints = BTreeSet::from_iter(existing_vpc_endpoints.into_keys());
            let desired_vpc_endpoints = privatelink_connections.keys().cloned().collect();
            let vpc_endpoints_to_remove = existing_vpc_endpoints.difference(&desired_vpc_endpoints);
            for id in vpc_endpoints_to_remove {
                cloud_resource_controller
                    .delete_vpc_endpoint(*id)
                    .await
                    .context("deleting extraneous vpc endpoint")?;
            }

            // Ensure desired VpcEndpoints are up to date.
            for (id, spec) in privatelink_connections {
                cloud_resource_controller
                    .ensure_vpc_endpoint(id, spec)
                    .await
                    .context("ensuring vpc endpoint")?;
            }
        }

        // Having installed all entries, creating all constraints, we can now drop read holds and
        // relax read policies.
        drop(dataflow_read_holds);
        // TODO -- Improve `initialize_read_policies` API so we can avoid calling this in a loop.
        for (cw, policies) in policies_to_set {
            self.initialize_read_policies(&policies, cw).await;
        }

        // Expose mapping from T-shirt sizes to actual sizes
        builtin_table_updates.extend(
            self.catalog().state().resolve_builtin_table_updates(
                self.catalog().state().pack_all_replica_size_updates(),
            ),
        );

        debug!("startup: coordinator init: bootstrap: initializing migrated builtin tables");
        // When 0dt is enabled, we create new shards for any migrated builtin storage collections.
        // In read-only mode, the migrated builtin tables (which are a subset of migrated builtin
        // storage collections) need to be back-filled so that any dependent dataflow can be
        // hydrated. Additionally, these shards are not registered with the txn-shard, and cannot
        // be registered while in read-only, so they are written to directly.
        let migrated_updates_fut = if self.controller.read_only() {
            let min_timestamp = Timestamp::minimum();
            let migrated_builtin_table_updates: Vec<_> = builtin_table_updates
                .extract_if(.., |update| {
                    let gid = self.catalog().get_entry(&update.id).latest_global_id();
                    migrated_storage_collections_0dt.contains(&update.id)
                        && self
                            .controller
                            .storage_collections
                            .collection_frontiers(gid)
                            .expect("all tables are registered")
                            .write_frontier
                            .elements()
                            == &[min_timestamp]
                })
                .collect();
            if migrated_builtin_table_updates.is_empty() {
                futures::future::ready(()).boxed()
            } else {
                // Group all updates per-table.
                let mut grouped_appends: BTreeMap<GlobalId, Vec<TableData>> = BTreeMap::new();
                for update in migrated_builtin_table_updates {
                    let gid = self.catalog().get_entry(&update.id).latest_global_id();
                    grouped_appends.entry(gid).or_default().push(update.data);
                }
                info!(
                    "coordinator init: rehydrating migrated builtin tables in read-only mode: {:?}",
                    grouped_appends.keys().collect::<Vec<_>>()
                );

                // Consolidate Row data, staged batches must already be consolidated.
                let mut all_appends = Vec::with_capacity(grouped_appends.len());
                for (item_id, table_data) in grouped_appends.into_iter() {
                    let mut all_rows = Vec::new();
                    let mut all_data = Vec::new();
                    for data in table_data {
                        match data {
                            TableData::Rows(rows) => all_rows.extend(rows),
                            TableData::Batches(_) => all_data.push(data),
                        }
                    }
                    differential_dataflow::consolidation::consolidate(&mut all_rows);
                    all_data.push(TableData::Rows(all_rows));

                    // TODO(parkmycar): Use SmallVec throughout.
                    all_appends.push((item_id, all_data));
                }

                let fut = self
                    .controller
                    .storage
                    .append_table(min_timestamp, boot_ts.step_forward(), all_appends)
                    .expect("cannot fail to append");
                async {
                    fut.await
                        .expect("One-shot shouldn't be dropped during bootstrap")
                        .unwrap_or_terminate("cannot fail to append")
                }
                .boxed()
            }
        } else {
            futures::future::ready(()).boxed()
        };

        info!(
            "startup: coordinator init: bootstrap: postamble complete in {:?}",
            postamble_start.elapsed()
        );

        let builtin_update_start = Instant::now();
        info!("startup: coordinator init: bootstrap: generate builtin updates beginning");

        if self.controller.read_only() {
            info!(
                "coordinator init: bootstrap: stashing builtin table updates while in read-only mode"
            );

            // TODO(jkosh44) Optimize deserializing the audit log in read-only mode.
            let audit_join_start = Instant::now();
            info!("startup: coordinator init: bootstrap: audit log deserialization beginning");
            let audit_log_updates: Vec<_> = audit_logs_iterator
                .map(|(audit_log, ts)| StateUpdate {
                    kind: StateUpdateKind::AuditLog(audit_log),
                    ts,
                    diff: StateDiff::Addition,
                })
                .collect();
            let audit_log_builtin_table_updates = self
                .catalog()
                .state()
                .generate_builtin_table_updates(audit_log_updates);
            builtin_table_updates.extend(audit_log_builtin_table_updates);
            info!(
                "startup: coordinator init: bootstrap: audit log deserialization complete in {:?}",
                audit_join_start.elapsed()
            );
            self.buffered_builtin_table_updates
                .as_mut()
                .expect("in read-only mode")
                .append(&mut builtin_table_updates);
        } else {
            self.bootstrap_tables(&entries, builtin_table_updates, audit_logs_iterator)
                .await;
        };
        info!(
            "startup: coordinator init: bootstrap: generate builtin updates complete in {:?}",
            builtin_update_start.elapsed()
        );

        let cleanup_secrets_start = Instant::now();
        info!("startup: coordinator init: bootstrap: generate secret cleanup beginning");
        // Cleanup orphaned secrets. Errors during list() or delete() do not
        // need to prevent bootstrap from succeeding; we will retry next
        // startup.
        {
            // Destructure Self so we can selectively move fields into the async
            // task.
            let Self {
                secrets_controller,
                catalog,
                ..
            } = self;

            let next_user_item_id = catalog.get_next_user_item_id().await?;
            let next_system_item_id = catalog.get_next_system_item_id().await?;
            let read_only = self.controller.read_only();
            // Fetch all IDs from the catalog to future-proof against other
            // things using secrets. Today, SECRET and CONNECTION objects use
            // secrets_controller.ensure, but more things could in the future
            // that would be easy to miss adding here.
            let catalog_ids: BTreeSet<CatalogItemId> =
                catalog.entries().map(|entry| entry.id()).collect();
            let secrets_controller = Arc::clone(secrets_controller);

            spawn(|| "cleanup-orphaned-secrets", async move {
                if read_only {
                    info!(
                        "coordinator init: not cleaning up orphaned secrets while in read-only mode"
                    );
                    return;
                }
                info!("coordinator init: cleaning up orphaned secrets");

                match secrets_controller.list().await {
                    Ok(controller_secrets) => {
                        let controller_secrets: BTreeSet<CatalogItemId> =
                            controller_secrets.into_iter().collect();
                        let orphaned = controller_secrets.difference(&catalog_ids);
                        for id in orphaned {
                            let id_too_large = match id {
                                CatalogItemId::System(id) => *id >= next_system_item_id,
                                CatalogItemId::User(id) => *id >= next_user_item_id,
                                CatalogItemId::IntrospectionSourceIndex(_)
                                | CatalogItemId::Transient(_) => false,
                            };
                            if id_too_large {
                                info!(
                                    %next_user_item_id, %next_system_item_id,
                                    "coordinator init: not deleting orphaned secret {id} that was likely created by a newer deploy generation"
                                );
                            } else {
                                info!("coordinator init: deleting orphaned secret {id}");
                                fail_point!("orphan_secrets");
                                if let Err(e) = secrets_controller.delete(*id).await {
                                    warn!(
                                        "Dropping orphaned secret has encountered an error: {}",
                                        e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => warn!("Failed to list secrets during orphan cleanup: {:?}", e),
                }
            });
        }
        info!(
            "startup: coordinator init: bootstrap: generate secret cleanup complete in {:?}",
            cleanup_secrets_start.elapsed()
        );

        // Run all of our final steps concurrently.
        let final_steps_start = Instant::now();
        info!(
            "startup: coordinator init: bootstrap: migrate builtin tables in read-only mode beginning"
        );
        migrated_updates_fut
            .instrument(info_span!("coord::bootstrap::final"))
            .await;

        debug!(
            "startup: coordinator init: bootstrap: announcing completion of initialization to controller"
        );
        // Announce the completion of initialization.
        self.controller.initialization_complete();

        // Initialize unified introspection.
        self.bootstrap_introspection_subscribes().await;

        info!(
            "startup: coordinator init: bootstrap: migrate builtin tables in read-only mode complete in {:?}",
            final_steps_start.elapsed()
        );

        info!(
            "startup: coordinator init: bootstrap complete in {:?}",
            bootstrap_start.elapsed()
        );
        Ok(())
    }

    /// Prepares tables for writing by resetting them to a known state and
    /// appending the given builtin table updates. The timestamp oracle
    /// will be advanced to the write timestamp of the append when this
    /// method returns.
    #[allow(clippy::async_yields_async)]
    #[instrument]
    async fn bootstrap_tables(
        &mut self,
        entries: &[CatalogEntry],
        mut builtin_table_updates: Vec<BuiltinTableUpdate>,
        audit_logs_iterator: AuditLogIterator,
    ) {
        /// Smaller helper struct of metadata for bootstrapping tables.
        struct TableMetadata<'a> {
            id: CatalogItemId,
            name: &'a QualifiedItemName,
            table: &'a Table,
        }

        // Filter our entries down to just tables.
        let table_metas: Vec<_> = entries
            .into_iter()
            .filter_map(|entry| {
                entry.table().map(|table| TableMetadata {
                    id: entry.id(),
                    name: entry.name(),
                    table,
                })
            })
            .collect();

        // Append empty batches to advance the timestamp of all tables.
        debug!("coordinator init: advancing all tables to current timestamp");
        let WriteTimestamp {
            timestamp: write_ts,
            advance_to,
        } = self.get_local_write_ts().await;
        let appends = table_metas
            .iter()
            .map(|meta| (meta.table.global_id_writes(), Vec::new()))
            .collect();
        // Append the tables in the background. We apply the write timestamp before getting a read
        // timestamp and reading a snapshot of each table, so the snapshots will block on their own
        // until the appends are complete.
        let table_fence_rx = self
            .controller
            .storage
            .append_table(write_ts.clone(), advance_to, appends)
            .expect("invalid updates");

        self.apply_local_write(write_ts).await;

        // Add builtin table updates the clear the contents of all system tables
        debug!("coordinator init: resetting system tables");
        let read_ts = self.get_local_read_ts().await;

        // Filter out the 'mz_storage_usage_by_shard' table since we need to retain that info for
        // billing purposes.
        let mz_storage_usage_by_shard_schema: SchemaSpecifier = self
            .catalog()
            .resolve_system_schema(MZ_STORAGE_USAGE_BY_SHARD.schema)
            .into();
        let is_storage_usage_by_shard = |meta: &TableMetadata| -> bool {
            meta.name.item == MZ_STORAGE_USAGE_BY_SHARD.name
                && meta.name.qualifiers.schema_spec == mz_storage_usage_by_shard_schema
        };

        let mut retraction_tasks = Vec::new();
        let mut system_tables: Vec<_> = table_metas
            .iter()
            .filter(|meta| meta.id.is_system() && !is_storage_usage_by_shard(meta))
            .collect();

        // Special case audit events because it's append only.
        let (audit_events_idx, _) = system_tables
            .iter()
            .find_position(|table| {
                table.id == self.catalog().resolve_builtin_table(&MZ_AUDIT_EVENTS)
            })
            .expect("mz_audit_events must exist");
        let audit_events = system_tables.remove(audit_events_idx);
        let audit_log_task = self.bootstrap_audit_log_table(
            audit_events.id,
            audit_events.name,
            audit_events.table,
            audit_logs_iterator,
            read_ts,
        );

        for system_table in system_tables {
            let table_id = system_table.id;
            let full_name = self.catalog().resolve_full_name(system_table.name, None);
            debug!("coordinator init: resetting system table {full_name} ({table_id})");

            // Fetch the current contents of the table for retraction.
            let snapshot_fut = self
                .controller
                .storage_collections
                .snapshot_cursor(system_table.table.global_id_writes(), read_ts);
            let batch_fut = self
                .controller
                .storage_collections
                .create_update_builder(system_table.table.global_id_writes());

            let task = spawn(|| format!("snapshot-{table_id}"), async move {
                // Create a TimestamplessUpdateBuilder.
                let mut batch = batch_fut
                    .await
                    .unwrap_or_terminate("cannot fail to create a batch for a BuiltinTable");
                tracing::info!(?table_id, "starting snapshot");
                // Get a cursor which will emit a consolidated snapshot.
                let mut snapshot_cursor = snapshot_fut
                    .await
                    .unwrap_or_terminate("cannot fail to snapshot");

                // Retract the current contents, spilling into our builder.
                while let Some(values) = snapshot_cursor.next().await {
                    for ((key, _val), _t, d) in values {
                        let key = key.expect("builtin table had errors");
                        let d_invert = d.neg();
                        batch.add(&key, &(), &d_invert).await;
                    }
                }
                tracing::info!(?table_id, "finished snapshot");

                let batch = batch.finish().await;
                BuiltinTableUpdate::batch(table_id, batch)
            });
            retraction_tasks.push(task);
        }

        let retractions_res = futures::future::join_all(retraction_tasks).await;
        for retractions in retractions_res {
            builtin_table_updates.push(retractions);
        }

        let audit_join_start = Instant::now();
        info!("startup: coordinator init: bootstrap: join audit log deserialization beginning");
        let audit_log_updates = audit_log_task.await;
        let audit_log_builtin_table_updates = self
            .catalog()
            .state()
            .generate_builtin_table_updates(audit_log_updates);
        builtin_table_updates.extend(audit_log_builtin_table_updates);
        info!(
            "startup: coordinator init: bootstrap: join audit log deserialization complete in {:?}",
            audit_join_start.elapsed()
        );

        // Now that the snapshots are complete, the appends must also be complete.
        table_fence_rx
            .await
            .expect("One-shot shouldn't be dropped during bootstrap")
            .unwrap_or_terminate("cannot fail to append");

        info!("coordinator init: sending builtin table updates");
        let (_builtin_updates_fut, write_ts) = self
            .builtin_table_update()
            .execute(builtin_table_updates)
            .await;
        info!(?write_ts, "our write ts");
        if let Some(write_ts) = write_ts {
            self.apply_local_write(write_ts).await;
        }
    }

    /// Prepare updates to the audit log table. The audit log table append only and very large, so
    /// we only need to find the events present in `audit_logs_iterator` but not in the audit log
    /// table.
    #[instrument]
    fn bootstrap_audit_log_table<'a>(
        &self,
        table_id: CatalogItemId,
        name: &'a QualifiedItemName,
        table: &'a Table,
        audit_logs_iterator: AuditLogIterator,
        read_ts: Timestamp,
    ) -> JoinHandle<Vec<StateUpdate>> {
        let full_name = self.catalog().resolve_full_name(name, None);
        debug!("coordinator init: reconciling audit log: {full_name} ({table_id})");
        let current_contents_fut = self
            .controller
            .storage_collections
            .snapshot(table.global_id_writes(), read_ts);
        spawn(|| format!("snapshot-audit-log-{table_id}"), async move {
            let current_contents = current_contents_fut
                .await
                .unwrap_or_terminate("cannot fail to fetch snapshot");
            let contents_len = current_contents.len();
            debug!("coordinator init: audit log table ({table_id}) size {contents_len}");

            // Fetch the largest audit log event ID that has been written to the table.
            let max_table_id = current_contents
                .into_iter()
                .filter(|(_, diff)| *diff == 1)
                .map(|(row, _diff)| row.unpack_first().unwrap_uint64())
                .sorted()
                .rev()
                .next();

            // Filter audit log catalog updates to those that are not present in the table.
            audit_logs_iterator
                .take_while(|(audit_log, _)| match max_table_id {
                    Some(id) => audit_log.event.sortable_id() > id,
                    None => true,
                })
                .map(|(audit_log, ts)| StateUpdate {
                    kind: StateUpdateKind::AuditLog(audit_log),
                    ts,
                    diff: StateDiff::Addition,
                })
                .collect::<Vec<_>>()
        })
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
    ///
    /// `migrated_storage_collections` is a set of builtin storage collections that have been
    /// migrated and should be handled specially.
    #[instrument]
    async fn bootstrap_storage_collections(
        &mut self,
        migrated_storage_collections: &BTreeSet<CatalogItemId>,
    ) {
        let catalog = self.catalog();
        let source_status_collection_id = catalog
            .resolve_builtin_storage_collection(&mz_catalog::builtin::MZ_SOURCE_STATUS_HISTORY);
        let source_status_collection_id = catalog
            .get_entry(&source_status_collection_id)
            .latest_global_id();

        let source_desc = |object_id: GlobalId,
                           data_source: &DataSourceDesc,
                           desc: &RelationDesc,
                           timeline: &Timeline| {
            let (data_source, status_collection_id) = match data_source.clone() {
                // Re-announce the source description.
                DataSourceDesc::Ingestion { desc, cluster_id } => {
                    let desc = desc.into_inline_connection(catalog.state());
                    let ingestion = IngestionDescription::new(desc, cluster_id, object_id);

                    (
                        DataSource::Ingestion(ingestion),
                        Some(source_status_collection_id),
                    )
                }
                DataSourceDesc::OldSyntaxIngestion {
                    desc,
                    progress_subsource,
                    data_config,
                    details,
                    cluster_id,
                } => {
                    let desc = desc.into_inline_connection(catalog.state());
                    let data_config = data_config.into_inline_connection(catalog.state());
                    // TODO(parkmycar): We should probably check the type here, but I'm not sure if
                    // this will always be a Source or a Table.
                    let progress_subsource =
                        catalog.get_entry(&progress_subsource).latest_global_id();
                    let mut ingestion =
                        IngestionDescription::new(desc, cluster_id, progress_subsource);
                    let legacy_export = SourceExport {
                        storage_metadata: (),
                        data_config,
                        details,
                    };
                    ingestion.source_exports.insert(object_id, legacy_export);

                    (
                        DataSource::Ingestion(ingestion),
                        Some(source_status_collection_id),
                    )
                }
                DataSourceDesc::IngestionExport {
                    ingestion_id,
                    external_reference: _,
                    details,
                    data_config,
                } => {
                    // TODO(parkmycar): We should probably check the type here, but I'm not sure if
                    // this will always be a Source or a Table.
                    let ingestion_id = catalog.get_entry(&ingestion_id).latest_global_id();
                    (
                        DataSource::IngestionExport {
                            ingestion_id,
                            details,
                            data_config: data_config.into_inline_connection(catalog.state()),
                        },
                        Some(source_status_collection_id),
                    )
                }
                DataSourceDesc::Webhook { .. } => {
                    (DataSource::Webhook, Some(source_status_collection_id))
                }
                DataSourceDesc::Progress => (DataSource::Progress, None),
                DataSourceDesc::Introspection(introspection) => {
                    (DataSource::Introspection(introspection), None)
                }
            };
            CollectionDescription {
                desc: desc.clone(),
                data_source,
                since: None,
                status_collection_id,
                timeline: Some(timeline.clone()),
                primary: None,
            }
        };

        let mut compute_collections = vec![];
        let mut collections = vec![];
        let mut new_builtin_continual_tasks = vec![];
        for entry in catalog.entries() {
            match entry.item() {
                CatalogItem::Source(source) => {
                    collections.push((
                        source.global_id(),
                        source_desc(
                            source.global_id(),
                            &source.data_source,
                            &source.desc,
                            &source.timeline,
                        ),
                    ));
                }
                CatalogItem::Table(table) => {
                    match &table.data_source {
                        TableDataSource::TableWrites { defaults: _ } => {
                            let versions: BTreeMap<_, _> = table
                                .collection_descs()
                                .map(|(gid, version, desc)| (version, (gid, desc)))
                                .collect();
                            let collection_descs = versions.iter().map(|(version, (gid, desc))| {
                                let next_version = version.bump();
                                let primary_collection =
                                    versions.get(&next_version).map(|(gid, _desc)| gid).copied();
                                let mut collection_desc =
                                    CollectionDescription::for_table(desc.clone());
                                collection_desc.primary = primary_collection;

                                (*gid, collection_desc)
                            });
                            collections.extend(collection_descs);
                        }
                        TableDataSource::DataSource {
                            desc: data_source_desc,
                            timeline,
                        } => {
                            // TODO(alter_table): Support versioning tables that read from sources.
                            soft_assert_eq_or_log!(table.collections.len(), 1);
                            let collection_descs =
                                table.collection_descs().map(|(gid, _version, desc)| {
                                    (
                                        gid,
                                        source_desc(
                                            entry.latest_global_id(),
                                            data_source_desc,
                                            &desc,
                                            timeline,
                                        ),
                                    )
                                });
                            collections.extend(collection_descs);
                        }
                    };
                }
                CatalogItem::MaterializedView(mv) => {
                    let collection_descs = mv.collection_descs().map(|(gid, _version, desc)| {
                        let collection_desc =
                            CollectionDescription::for_other(desc, mv.initial_as_of.clone());
                        (gid, collection_desc)
                    });

                    collections.extend(collection_descs);
                    compute_collections.push((mv.global_id_writes(), mv.desc.latest()));
                }
                CatalogItem::ContinualTask(ct) => {
                    let collection_desc =
                        CollectionDescription::for_other(ct.desc.clone(), ct.initial_as_of.clone());
                    if ct.global_id().is_system() && collection_desc.since.is_none() {
                        // We need a non-0 since to make as_of selection work. Fill it in below with
                        // the `bootstrap_builtin_continual_tasks` call, which can only be run after
                        // `create_collections_for_bootstrap`.
                        new_builtin_continual_tasks.push((ct.global_id(), collection_desc));
                    } else {
                        compute_collections.push((ct.global_id(), ct.desc.clone()));
                        collections.push((ct.global_id(), collection_desc));
                    }
                }
                CatalogItem::Sink(sink) => {
                    let storage_sink_from_entry = self.catalog().get_entry_by_global_id(&sink.from);
                    let from_desc = storage_sink_from_entry
                        .relation_desc()
                        .expect("sinks can only be built on items with descs")
                        .into_owned();
                    let collection_desc = CollectionDescription {
                        // TODO(sinks): make generic once we have more than one sink type.
                        desc: KAFKA_PROGRESS_DESC.clone(),
                        data_source: DataSource::Sink {
                            desc: ExportDescription {
                                sink: StorageSinkDesc {
                                    from: sink.from,
                                    from_desc,
                                    connection: sink
                                        .connection
                                        .clone()
                                        .into_inline_connection(self.catalog().state()),
                                    envelope: sink.envelope,
                                    as_of: Antichain::from_elem(Timestamp::minimum()),
                                    with_snapshot: sink.with_snapshot,
                                    version: sink.version,
                                    from_storage_metadata: (),
                                    to_storage_metadata: (),
                                    commit_interval: sink.commit_interval,
                                },
                                instance_id: sink.cluster_id,
                            },
                        },
                        since: None,
                        status_collection_id: None,
                        timeline: None,
                        primary: None,
                    };
                    collections.push((sink.global_id, collection_desc));
                }
                _ => (),
            }
        }

        let register_ts = if self.controller.read_only() {
            self.get_local_read_ts().await
        } else {
            // Getting a write timestamp bumps the write timestamp in the
            // oracle, which we're not allowed in read-only mode.
            self.get_local_write_ts().await.timestamp
        };

        let storage_metadata = self.catalog.state().storage_metadata();
        let migrated_storage_collections = migrated_storage_collections
            .into_iter()
            .flat_map(|item_id| self.catalog.get_entry(item_id).global_ids())
            .collect();

        // Before possibly creating collections, make sure their schemas are correct.
        //
        // Across different versions of Materialize the nullability of columns can change based on
        // updates to our optimizer.
        self.controller
            .storage
            .evolve_nullability_for_bootstrap(storage_metadata, compute_collections)
            .await
            .unwrap_or_terminate("cannot fail to evolve collections");

        self.controller
            .storage
            .create_collections_for_bootstrap(
                storage_metadata,
                Some(register_ts),
                collections,
                &migrated_storage_collections,
            )
            .await
            .unwrap_or_terminate("cannot fail to create collections");

        self.bootstrap_builtin_continual_tasks(new_builtin_continual_tasks)
            .await;

        if !self.controller.read_only() {
            self.apply_local_write(register_ts).await;
        }
    }

    /// Make as_of selection happy for builtin CTs. Ideally we'd write the
    /// initial as_of down in the durable catalog, but that's hard because of
    /// boot ordering. Instead, we set the since of the storage collection to
    /// something that's a reasonable lower bound for the as_of. Then, if the
    /// upper is 0, the as_of selection code will allow us to jump it forward to
    /// this since.
    async fn bootstrap_builtin_continual_tasks(
        &mut self,
        // TODO(alter_table): Switch to CatalogItemId.
        mut collections: Vec<(GlobalId, CollectionDescription<Timestamp>)>,
    ) {
        for (id, collection) in &mut collections {
            let entry = self.catalog.get_entry_by_global_id(id);
            let ct = match &entry.item {
                CatalogItem::ContinualTask(ct) => ct.clone(),
                _ => unreachable!("only called with continual task builtins"),
            };
            let debug_name = self
                .catalog()
                .resolve_full_name(entry.name(), None)
                .to_string();
            let (_optimized_plan, physical_plan, _metainfo) = self
                .optimize_create_continual_task(&ct, *id, self.owned_catalog(), debug_name)
                .expect("builtin CT should optimize successfully");

            // Determine an as of for the new continual task.
            let mut id_bundle = dataflow_import_id_bundle(&physical_plan, ct.cluster_id);
            // Can't acquire a read hold on ourselves because we don't exist yet.
            id_bundle.storage_ids.remove(id);
            let read_holds = self.acquire_read_holds(&id_bundle);
            let as_of = read_holds.least_valid_read();

            collection.since = Some(as_of.clone());
        }
        self.controller
            .storage
            .create_collections(self.catalog.state().storage_metadata(), None, collections)
            .await
            .unwrap_or_terminate("cannot fail to create collections");
    }

    /// Invokes the optimizer on all indexes and materialized views in the catalog and inserts the
    /// resulting dataflow plans into the catalog state.
    ///
    /// `ordered_catalog_entries` must be sorted in dependency order, with dependencies ordered
    /// before their dependants.
    ///
    /// This method does not perform timestamp selection for the dataflows, nor does it create them
    /// in the compute controller. Both of these steps happen later during bootstrapping.
    ///
    /// Returns a map of expressions that were not cached.
    #[instrument]
    fn bootstrap_dataflow_plans(
        &mut self,
        ordered_catalog_entries: &[CatalogEntry],
        mut cached_global_exprs: BTreeMap<GlobalId, GlobalExpressions>,
    ) -> Result<BTreeMap<GlobalId, GlobalExpressions>, AdapterError> {
        // The optimizer expects to be able to query its `ComputeInstanceSnapshot` for
        // collections the current dataflow can depend on. But since we don't yet install anything
        // on compute instances, the snapshot information is incomplete. We fix that by manually
        // updating `ComputeInstanceSnapshot` objects to ensure they contain collections previously
        // optimized.
        let mut instance_snapshots = BTreeMap::new();
        let mut uncached_expressions = BTreeMap::new();

        let optimizer_config = OptimizerConfig::from(self.catalog().system_config());

        for entry in ordered_catalog_entries {
            match entry.item() {
                CatalogItem::Index(idx) => {
                    // Collect optimizer parameters.
                    let compute_instance =
                        instance_snapshots.entry(idx.cluster_id).or_insert_with(|| {
                            self.instance_snapshot(idx.cluster_id)
                                .expect("compute instance exists")
                        });
                    let global_id = idx.global_id();

                    // The index may already be installed on the compute instance. For example,
                    // this is the case for introspection indexes.
                    if compute_instance.contains_collection(&global_id) {
                        continue;
                    }

                    let (optimized_plan, physical_plan, metainfo) =
                        match cached_global_exprs.remove(&global_id) {
                            Some(global_expressions)
                                if global_expressions.optimizer_features
                                    == optimizer_config.features =>
                            {
                                debug!("global expression cache hit for {global_id:?}");
                                (
                                    global_expressions.global_mir,
                                    global_expressions.physical_plan,
                                    global_expressions.dataflow_metainfos,
                                )
                            }
                            Some(_) | None => {
                                let (optimized_plan, global_lir_plan) = {
                                    // Build an optimizer for this INDEX.
                                    let mut optimizer = optimize::index::Optimizer::new(
                                        self.owned_catalog(),
                                        compute_instance.clone(),
                                        global_id,
                                        optimizer_config.clone(),
                                        self.optimizer_metrics(),
                                    );

                                    // MIR ⇒ MIR optimization (global)
                                    let index_plan = optimize::index::Index::new(
                                        entry.name().clone(),
                                        idx.on,
                                        idx.keys.to_vec(),
                                    );
                                    let global_mir_plan = optimizer.optimize(index_plan)?;
                                    let optimized_plan = global_mir_plan.df_desc().clone();

                                    // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                                    let global_lir_plan = optimizer.optimize(global_mir_plan)?;

                                    (optimized_plan, global_lir_plan)
                                };

                                let (physical_plan, metainfo) = global_lir_plan.unapply();
                                let metainfo = {
                                    // Pre-allocate a vector of transient GlobalIds for each notice.
                                    let notice_ids =
                                        std::iter::repeat_with(|| self.allocate_transient_id())
                                            .map(|(_item_id, gid)| gid)
                                            .take(metainfo.optimizer_notices.len())
                                            .collect::<Vec<_>>();
                                    // Return a metainfo with rendered notices.
                                    self.catalog().render_notices(
                                        metainfo,
                                        notice_ids,
                                        Some(idx.global_id()),
                                    )
                                };
                                uncached_expressions.insert(
                                    global_id,
                                    GlobalExpressions {
                                        global_mir: optimized_plan.clone(),
                                        physical_plan: physical_plan.clone(),
                                        dataflow_metainfos: metainfo.clone(),
                                        optimizer_features: OptimizerFeatures::from(
                                            self.catalog().system_config(),
                                        ),
                                    },
                                );
                                (optimized_plan, physical_plan, metainfo)
                            }
                        };

                    let catalog = self.catalog_mut();
                    catalog.set_optimized_plan(idx.global_id(), optimized_plan);
                    catalog.set_physical_plan(idx.global_id(), physical_plan);
                    catalog.set_dataflow_metainfo(idx.global_id(), metainfo);

                    compute_instance.insert_collection(idx.global_id());
                }
                CatalogItem::MaterializedView(mv) => {
                    // Collect optimizer parameters.
                    let compute_instance =
                        instance_snapshots.entry(mv.cluster_id).or_insert_with(|| {
                            self.instance_snapshot(mv.cluster_id)
                                .expect("compute instance exists")
                        });
                    let global_id = mv.global_id_writes();

                    let (optimized_plan, physical_plan, metainfo) =
                        match cached_global_exprs.remove(&global_id) {
                            Some(global_expressions)
                                if global_expressions.optimizer_features
                                    == optimizer_config.features =>
                            {
                                debug!("global expression cache hit for {global_id:?}");
                                (
                                    global_expressions.global_mir,
                                    global_expressions.physical_plan,
                                    global_expressions.dataflow_metainfos,
                                )
                            }
                            Some(_) | None => {
                                let (_, internal_view_id) = self.allocate_transient_id();
                                let debug_name = self
                                    .catalog()
                                    .resolve_full_name(entry.name(), None)
                                    .to_string();
                                let force_non_monotonic = Default::default();

                                let (optimized_plan, global_lir_plan) = {
                                    // Build an optimizer for this MATERIALIZED VIEW.
                                    let mut optimizer = optimize::materialized_view::Optimizer::new(
                                        self.owned_catalog().as_optimizer_catalog(),
                                        compute_instance.clone(),
                                        global_id,
                                        internal_view_id,
                                        mv.desc.latest().iter_names().cloned().collect(),
                                        mv.non_null_assertions.clone(),
                                        mv.refresh_schedule.clone(),
                                        debug_name,
                                        optimizer_config.clone(),
                                        self.optimizer_metrics(),
                                        force_non_monotonic,
                                    );

                                    // MIR ⇒ MIR optimization (global)
                                    let global_mir_plan =
                                        optimizer.optimize(mv.optimized_expr.as_ref().clone())?;
                                    let optimized_plan = global_mir_plan.df_desc().clone();

                                    // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                                    let global_lir_plan = optimizer.optimize(global_mir_plan)?;

                                    (optimized_plan, global_lir_plan)
                                };

                                let (physical_plan, metainfo) = global_lir_plan.unapply();
                                let metainfo = {
                                    // Pre-allocate a vector of transient GlobalIds for each notice.
                                    let notice_ids =
                                        std::iter::repeat_with(|| self.allocate_transient_id())
                                            .map(|(_item_id, global_id)| global_id)
                                            .take(metainfo.optimizer_notices.len())
                                            .collect::<Vec<_>>();
                                    // Return a metainfo with rendered notices.
                                    self.catalog().render_notices(
                                        metainfo,
                                        notice_ids,
                                        Some(mv.global_id_writes()),
                                    )
                                };
                                uncached_expressions.insert(
                                    global_id,
                                    GlobalExpressions {
                                        global_mir: optimized_plan.clone(),
                                        physical_plan: physical_plan.clone(),
                                        dataflow_metainfos: metainfo.clone(),
                                        optimizer_features: OptimizerFeatures::from(
                                            self.catalog().system_config(),
                                        ),
                                    },
                                );
                                (optimized_plan, physical_plan, metainfo)
                            }
                        };

                    let catalog = self.catalog_mut();
                    catalog.set_optimized_plan(mv.global_id_writes(), optimized_plan);
                    catalog.set_physical_plan(mv.global_id_writes(), physical_plan);
                    catalog.set_dataflow_metainfo(mv.global_id_writes(), metainfo);

                    compute_instance.insert_collection(mv.global_id_writes());
                }
                CatalogItem::ContinualTask(ct) => {
                    let compute_instance =
                        instance_snapshots.entry(ct.cluster_id).or_insert_with(|| {
                            self.instance_snapshot(ct.cluster_id)
                                .expect("compute instance exists")
                        });
                    let global_id = ct.global_id();

                    let (optimized_plan, physical_plan, metainfo) =
                        match cached_global_exprs.remove(&global_id) {
                            Some(global_expressions)
                                if global_expressions.optimizer_features
                                    == optimizer_config.features =>
                            {
                                debug!("global expression cache hit for {global_id:?}");
                                (
                                    global_expressions.global_mir,
                                    global_expressions.physical_plan,
                                    global_expressions.dataflow_metainfos,
                                )
                            }
                            Some(_) | None => {
                                let debug_name = self
                                    .catalog()
                                    .resolve_full_name(entry.name(), None)
                                    .to_string();
                                let (optimized_plan, physical_plan, metainfo) = self
                                    .optimize_create_continual_task(
                                        ct,
                                        global_id,
                                        self.owned_catalog(),
                                        debug_name,
                                    )?;
                                uncached_expressions.insert(
                                    global_id,
                                    GlobalExpressions {
                                        global_mir: optimized_plan.clone(),
                                        physical_plan: physical_plan.clone(),
                                        dataflow_metainfos: metainfo.clone(),
                                        optimizer_features: OptimizerFeatures::from(
                                            self.catalog().system_config(),
                                        ),
                                    },
                                );
                                (optimized_plan, physical_plan, metainfo)
                            }
                        };

                    let catalog = self.catalog_mut();
                    catalog.set_optimized_plan(ct.global_id(), optimized_plan);
                    catalog.set_physical_plan(ct.global_id(), physical_plan);
                    catalog.set_dataflow_metainfo(ct.global_id(), metainfo);

                    compute_instance.insert_collection(ct.global_id());
                }
                _ => (),
            }
        }

        Ok(uncached_expressions)
    }

    /// Selects for each compute dataflow an as-of suitable for bootstrapping it.
    ///
    /// Returns a set of [`ReadHold`]s that ensures the read frontiers of involved collections stay
    /// in place and that must not be dropped before all compute dataflows have been created with
    /// the compute controller.
    ///
    /// This method expects all storage collections and dataflow plans to be available, so it must
    /// run after [`Coordinator::bootstrap_storage_collections`] and
    /// [`Coordinator::bootstrap_dataflow_plans`].
    async fn bootstrap_dataflow_as_ofs(&mut self) -> BTreeMap<GlobalId, ReadHold<Timestamp>> {
        let mut catalog_ids = Vec::new();
        let mut dataflows = Vec::new();
        let mut read_policies = BTreeMap::new();
        for entry in self.catalog.entries() {
            let gid = match entry.item() {
                CatalogItem::Index(idx) => idx.global_id(),
                CatalogItem::MaterializedView(mv) => mv.global_id_writes(),
                CatalogItem::ContinualTask(ct) => ct.global_id(),
                CatalogItem::Table(_)
                | CatalogItem::Source(_)
                | CatalogItem::Log(_)
                | CatalogItem::View(_)
                | CatalogItem::Sink(_)
                | CatalogItem::Type(_)
                | CatalogItem::Func(_)
                | CatalogItem::Secret(_)
                | CatalogItem::Connection(_) => continue,
            };
            if let Some(plan) = self.catalog.try_get_physical_plan(&gid) {
                catalog_ids.push(gid);
                dataflows.push(plan.clone());

                if let Some(compaction_window) = entry.item().initial_logical_compaction_window() {
                    read_policies.insert(gid, compaction_window.into());
                }
            }
        }

        let read_ts = self.get_local_read_ts().await;
        let read_holds = as_of_selection::run(
            &mut dataflows,
            &read_policies,
            &*self.controller.storage_collections,
            read_ts,
            self.controller.read_only(),
        );

        let catalog = self.catalog_mut();
        for (id, plan) in catalog_ids.into_iter().zip_eq(dataflows) {
            catalog.set_physical_plan(id, plan);
        }

        read_holds
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
        mut strict_serializable_reads_rx: mpsc::UnboundedReceiver<(ConnectionId, PendingReadTxn)>,
        mut cmd_rx: mpsc::UnboundedReceiver<(OpenTelemetryContext, Command)>,
        group_commit_rx: appends::GroupCommitWaiter,
    ) -> LocalBoxFuture<'static, ()> {
        async move {
            // Watcher that listens for and reports cluster service status changes.
            let mut cluster_events = self.controller.events_stream();
            let last_message = Arc::new(Mutex::new(LastMessage {
                kind: "none",
                stmt: None,
            }));

            let (idle_tx, mut idle_rx) = tokio::sync::mpsc::channel(1);
            let idle_metric = self.metrics.queue_busy_seconds.clone();
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
                    let duration = tokio::time::Duration::from_secs(30);
                    let timeout = tokio::time::timeout(duration, idle_tx.reserve()).await;
                    let Ok(maybe_permit) = timeout else {
                        // Only log if we're newly stuck, to prevent logging repeatedly.
                        if !coord_stuck {
                            let last_message = last_message_watchdog.lock().expect("poisoned");
                            tracing::warn!(
                                last_message_kind = %last_message.kind,
                                last_message_sql = %last_message.stmt_to_string(),
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
            let warn_threshold = self
                .catalog()
                .system_config()
                .coord_slow_message_warn_threshold();

            // How many messages we'd like to batch up before processing them. Must be > 0.
            const MESSAGE_BATCH: usize = 64;
            let mut messages = Vec::with_capacity(MESSAGE_BATCH);
            let mut cmd_messages = Vec::with_capacity(MESSAGE_BATCH);

            let message_batch = self.metrics.message_batch.clone();

            loop {
                // Before adding a branch to this select loop, please ensure that the branch is
                // cancellation safe and add a comment explaining why. You can refer here for more
                // info: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
                select! {
                    // We prioritize internal commands over other commands. However, we work through
                    // batches of commands in some branches of this select, which means that even if
                    // a command generates internal commands, we will work through the current batch
                    // before receiving a new batch of commands.
                    biased;

                    // `recv_many()` on `UnboundedReceiver` is cancellation safe:
                    // https://docs.rs/tokio/1.38.0/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety-1
                    // Receive a batch of commands.
                    _ = internal_cmd_rx.recv_many(&mut messages, MESSAGE_BATCH) => {},
                    // `next()` on any stream is cancel-safe:
                    // https://docs.rs/tokio-stream/0.1.9/tokio_stream/trait.StreamExt.html#cancel-safety
                    // Receive a single command.
                    Some(event) = cluster_events.next() => messages.push(Message::ClusterEvent(event)),
                    // See [`mz_controller::Controller::Controller::ready`] for notes
                    // on why this is cancel-safe.
                    // Receive a single command.
                    () = self.controller.ready() => {
                        // NOTE: We don't get a `Readiness` back from `ready()`
                        // because the controller wants to keep it and it's not
                        // trivially `Clone` or `Copy`. Hence this accessor.
                        let controller = match self.controller.get_readiness() {
                            Readiness::Storage => ControllerReadiness::Storage,
                            Readiness::Compute => ControllerReadiness::Compute,
                            Readiness::Metrics(_) => ControllerReadiness::Metrics,
                            Readiness::Internal(_) => ControllerReadiness::Internal,
                            Readiness::NotReady => unreachable!("just signaled as ready"),
                        };
                        messages.push(Message::ControllerReady { controller });
                    }
                    // See [`appends::GroupCommitWaiter`] for notes on why this is cancel safe.
                    // Receive a single command.
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
                        messages.push(Message::GroupCommitInitiate(span, Some(permit)));
                    },
                    // `recv_many()` on `UnboundedReceiver` is cancellation safe:
                    // https://docs.rs/tokio/1.38.0/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety-1
                    // Receive a batch of commands.
                    count = cmd_rx.recv_many(&mut cmd_messages, MESSAGE_BATCH) => {
                        if count == 0 {
                            break;
                        } else {
                            messages.extend(cmd_messages.drain(..).map(|(otel_ctx, cmd)| Message::Command(otel_ctx, cmd)));
                        }
                    },
                    // `recv()` on `UnboundedReceiver` is cancellation safe:
                    // https://docs.rs/tokio/1.38.0/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety
                    // Receive a single command.
                    Some(pending_read_txn) = strict_serializable_reads_rx.recv() => {
                        let mut pending_read_txns = vec![pending_read_txn];
                        while let Ok(pending_read_txn) = strict_serializable_reads_rx.try_recv() {
                            pending_read_txns.push(pending_read_txn);
                        }
                        for (conn_id, pending_read_txn) in pending_read_txns {
                            let prev = self.pending_linearize_read_txns.insert(conn_id, pending_read_txn);
                            soft_assert_or_log!(
                                prev.is_none(),
                                "connections can not have multiple concurrent reads, prev: {prev:?}"
                            )
                        }
                        messages.push(Message::LinearizeReads);
                    }
                    // `tick()` on `Interval` is cancel-safe:
                    // https://docs.rs/tokio/1.19.2/tokio/time/struct.Interval.html#cancel-safety
                    // Receive a single command.
                    _ = self.advance_timelines_interval.tick() => {
                        let span = info_span!(parent: None, "coord::advance_timelines_interval");
                        span.follows_from(Span::current());

                        // Group commit sends an `AdvanceTimelines` message when
                        // done, which is what downgrades read holds. In
                        // read-only mode we send this message directly because
                        // we're not doing group commits.
                        if self.controller.read_only() {
                            messages.push(Message::AdvanceTimelines);
                        } else {
                            messages.push(Message::GroupCommitInitiate(span, None));
                        }
                    },
                    // `tick()` on `Interval` is cancel-safe:
                    // https://docs.rs/tokio/1.19.2/tokio/time/struct.Interval.html#cancel-safety
                    // Receive a single command.
                    _ = self.check_cluster_scheduling_policies_interval.tick() => {
                        messages.push(Message::CheckSchedulingPolicies);
                    },

                    // `tick()` on `Interval` is cancel-safe:
                    // https://docs.rs/tokio/1.19.2/tokio/time/struct.Interval.html#cancel-safety
                    // Receive a single command.
                    _ = self.caught_up_check_interval.tick() => {
                        // We do this directly on the main loop instead of
                        // firing off a message. We are still in read-only mode,
                        // so optimizing for latency, not blocking the main loop
                        // is not that important.
                        self.maybe_check_caught_up().await;

                        continue;
                    },

                    // Process the idle metric at the lowest priority to sample queue non-idle time.
                    // `recv()` on `Receiver` is cancellation safe:
                    // https://docs.rs/tokio/1.8.0/tokio/sync/mpsc/struct.Receiver.html#cancel-safety
                    // Receive a single command.
                    timer = idle_rx.recv() => {
                        timer.expect("does not drop").observe_duration();
                        self.metrics
                            .message_handling
                            .with_label_values(&["watchdog"])
                            .observe(0.0);
                        continue;
                    }
                };

                // Observe the number of messages we're processing at once.
                message_batch.observe(f64::cast_lossy(messages.len()));

                for msg in messages.drain(..) {
                    // All message processing functions trace. Start a parent span
                    // for them to make it easy to find slow messages.
                    let msg_kind = msg.kind();
                    let span = span!(
                        target: "mz_adapter::coord::handle_message_loop",
                        Level::INFO,
                        "coord::handle_message",
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
                    self.handle_message(msg).instrument(span).await;
                    let duration = start.elapsed();

                    self.metrics
                        .message_handling
                        .with_label_values(&[msg_kind])
                        .observe(duration.as_secs_f64());

                    // If something is _really_ slow, print a trace id for debugging, if OTEL is enabled.
                    if duration > warn_threshold {
                        let trace_id = otel_context.is_valid().then(|| otel_context.trace_id());
                        tracing::error!(
                            ?msg_kind,
                            ?trace_id,
                            ?duration,
                            "very slow coordinator message"
                        );
                    }
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

    /// Obtain a handle to the optimizer metrics, suitable for giving
    /// out to non-Coordinator thread tasks.
    fn optimizer_metrics(&self) -> OptimizerMetrics {
        self.optimizer_metrics.clone()
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

    /// Obtain a reference to the coordinator's secret reader, in an `Arc`.
    fn secrets_reader(&self) -> &Arc<dyn SecretsReader> {
        &self.connection_context().secrets_reader
    }

    /// Publishes a notice message to all sessions.
    ///
    /// TODO(parkmycar): This code is dead, but is a nice parallel to [`Coordinator::broadcast_notice_tx`]
    /// so we keep it around.
    #[allow(dead_code)]
    pub(crate) fn broadcast_notice(&self, notice: AdapterNotice) {
        for meta in self.active_conns.values() {
            let _ = meta.notice_tx.send(notice.clone());
        }
    }

    /// Returns a closure that will publish a notice to all sessions that were active at the time
    /// this method was called.
    pub(crate) fn broadcast_notice_tx(
        &self,
    ) -> Box<dyn FnOnce(AdapterNotice) -> () + Send + 'static> {
        let senders: Vec<_> = self
            .active_conns
            .values()
            .map(|meta| meta.notice_tx.clone())
            .collect();
        Box::new(move |notice| {
            for tx in senders {
                let _ = tx.send(notice.clone());
            }
        })
    }

    pub(crate) fn active_conns(&self) -> &BTreeMap<ConnectionId, ConnMeta> {
        &self.active_conns
    }

    #[instrument(level = "debug")]
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
    #[instrument(level = "debug")]
    pub fn dataflow_builder(&self, instance: ComputeInstanceId) -> DataflowBuilder<'_> {
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
    /// initialize the read policies for its exported readable objects.
    ///
    /// # Panics
    ///
    /// Panics if dataflow creation fails.
    pub(crate) async fn ship_dataflow(
        &mut self,
        dataflow: DataflowDescription<Plan>,
        instance: ComputeInstanceId,
        subscribe_target_replica: Option<ReplicaId>,
    ) {
        self.try_ship_dataflow(dataflow, instance, subscribe_target_replica)
            .await
            .unwrap_or_terminate("dataflow creation cannot fail");
    }

    /// Call into the compute controller to install a finalized dataflow, and
    /// initialize the read policies for its exported readable objects.
    pub(crate) async fn try_ship_dataflow(
        &mut self,
        dataflow: DataflowDescription<Plan>,
        instance: ComputeInstanceId,
        subscribe_target_replica: Option<ReplicaId>,
    ) -> Result<(), DataflowCreationError> {
        // We must only install read policies for indexes, not for sinks.
        // Sinks are write-only compute collections that don't have read policies.
        let export_ids = dataflow.exported_index_ids().collect();

        self.controller
            .compute
            .create_dataflow(instance, dataflow, subscribe_target_replica)?;

        self.initialize_compute_read_policies(export_ids, instance, CompactionWindow::Default)
            .await;

        Ok(())
    }

    /// Call into the compute controller to allow writes to the specified IDs
    /// from the specified instance. Calling this function multiple times and
    /// calling it on a read-only instance has no effect.
    pub(crate) fn allow_writes(&mut self, instance: ComputeInstanceId, id: GlobalId) {
        self.controller
            .compute
            .allow_writes(instance, id)
            .unwrap_or_terminate("allow_writes cannot fail");
    }

    /// Like `ship_dataflow`, but also await on builtin table updates.
    pub(crate) async fn ship_dataflow_and_notice_builtin_table_updates(
        &mut self,
        dataflow: DataflowDescription<Plan>,
        instance: ComputeInstanceId,
        notice_builtin_updates_fut: Option<BuiltinTableAppendNotify>,
    ) {
        if let Some(notice_builtin_updates_fut) = notice_builtin_updates_fut {
            let ship_dataflow_fut = self.ship_dataflow(dataflow, instance, None);
            let ((), ()) =
                futures::future::join(notice_builtin_updates_fut, ship_dataflow_fut).await;
        } else {
            self.ship_dataflow(dataflow, instance, None).await;
        }
    }

    /// Install a _watch set_ in the controller that is automatically associated with the given
    /// connection id. The watchset will be automatically cleared if the connection terminates
    /// before the watchset completes.
    pub fn install_compute_watch_set(
        &mut self,
        conn_id: ConnectionId,
        objects: BTreeSet<GlobalId>,
        t: Timestamp,
        state: WatchSetResponse,
    ) {
        let ws_id = self.controller.install_compute_watch_set(objects, t);
        self.connection_watch_sets
            .entry(conn_id.clone())
            .or_default()
            .insert(ws_id);
        self.installed_watch_sets.insert(ws_id, (conn_id, state));
    }

    /// Install a _watch set_ in the controller that is automatically associated with the given
    /// connection id. The watchset will be automatically cleared if the connection terminates
    /// before the watchset completes.
    pub fn install_storage_watch_set(
        &mut self,
        conn_id: ConnectionId,
        objects: BTreeSet<GlobalId>,
        t: Timestamp,
        state: WatchSetResponse,
    ) {
        let ws_id = self.controller.install_storage_watch_set(objects, t);
        self.connection_watch_sets
            .entry(conn_id.clone())
            .or_default()
            .insert(ws_id);
        self.installed_watch_sets.insert(ws_id, (conn_id, state));
    }

    /// Cancels pending watchsets associated with the provided connection id.
    pub fn cancel_pending_watchsets(&mut self, conn_id: &ConnectionId) {
        if let Some(ws_ids) = self.connection_watch_sets.remove(conn_id) {
            for ws_id in ws_ids {
                self.installed_watch_sets.remove(&ws_id);
            }
        }
    }

    /// Returns the state of the [`Coordinator`] formatted as JSON.
    ///
    /// The returned value is not guaranteed to be stable and may change at any point in time.
    pub async fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Note: We purposefully use the `Debug` formatting for the value of all fields in the
        // returned object as a tradeoff between usability and stability. `serde_json` will fail
        // to serialize an object if the keys aren't strings, so `Debug` formatting the values
        // prevents a future unrelated change from silently breaking this method.

        let global_timelines: BTreeMap<_, _> = self
            .global_timelines
            .iter()
            .map(|(timeline, state)| (timeline.to_string(), format!("{state:?}")))
            .collect();
        let active_conns: BTreeMap<_, _> = self
            .active_conns
            .iter()
            .map(|(id, meta)| (id.unhandled().to_string(), format!("{meta:?}")))
            .collect();
        let txn_read_holds: BTreeMap<_, _> = self
            .txn_read_holds
            .iter()
            .map(|(id, capability)| (id.unhandled().to_string(), format!("{capability:?}")))
            .collect();
        let pending_peeks: BTreeMap<_, _> = self
            .pending_peeks
            .iter()
            .map(|(id, peek)| (id.to_string(), format!("{peek:?}")))
            .collect();
        let client_pending_peeks: BTreeMap<_, _> = self
            .client_pending_peeks
            .iter()
            .map(|(id, peek)| {
                let peek: BTreeMap<_, _> = peek
                    .iter()
                    .map(|(uuid, storage_id)| (uuid.to_string(), storage_id))
                    .collect();
                (id.to_string(), peek)
            })
            .collect();
        let pending_linearize_read_txns: BTreeMap<_, _> = self
            .pending_linearize_read_txns
            .iter()
            .map(|(id, read_txn)| (id.unhandled().to_string(), format!("{read_txn:?}")))
            .collect();

        Ok(serde_json::json!({
            "global_timelines": global_timelines,
            "active_conns": active_conns,
            "txn_read_holds": txn_read_holds,
            "pending_peeks": pending_peeks,
            "client_pending_peeks": client_pending_peeks,
            "pending_linearize_read_txns": pending_linearize_read_txns,
            "controller": self.controller.dump().await?,
        }))
    }

    /// Prune all storage usage events from the [`MZ_STORAGE_USAGE_BY_SHARD`] table that are older
    /// than `retention_period`.
    ///
    /// This method will read the entire contents of [`MZ_STORAGE_USAGE_BY_SHARD`] into memory
    /// which can be expensive.
    ///
    /// DO NOT call this method outside of startup. The safety of reading at the current oracle read
    /// timestamp and then writing at whatever the current write timestamp is (instead of
    /// `read_ts + 1`) relies on the fact that there are no outstanding writes during startup.
    ///
    /// Group commit, which this method uses to write the retractions, has builtin fencing, and we
    /// never commit retractions to [`MZ_STORAGE_USAGE_BY_SHARD`] outside of this method, which is
    /// only called once during startup. So we don't have to worry about double/invalid retractions.
    async fn prune_storage_usage_events_on_startup(&self, retention_period: Duration) {
        let item_id = self
            .catalog()
            .resolve_builtin_table(&MZ_STORAGE_USAGE_BY_SHARD);
        let global_id = self.catalog.get_entry(&item_id).latest_global_id();
        let read_ts = self.get_local_read_ts().await;
        let current_contents_fut = self
            .controller
            .storage_collections
            .snapshot(global_id, read_ts);
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        spawn(|| "storage_usage_prune", async move {
            let mut current_contents = current_contents_fut
                .await
                .unwrap_or_terminate("cannot fail to fetch snapshot");
            differential_dataflow::consolidation::consolidate(&mut current_contents);

            let cutoff_ts = u128::from(read_ts).saturating_sub(retention_period.as_millis());
            let mut expired = Vec::new();
            for (row, diff) in current_contents {
                assert_eq!(
                    diff, 1,
                    "consolidated contents should not contain retractions: ({row:#?}, {diff:#?})"
                );
                // This logic relies on the definition of `mz_storage_usage_by_shard` not changing.
                let collection_timestamp = row
                    .unpack()
                    .get(3)
                    .expect("definition of mz_storage_by_shard changed")
                    .unwrap_timestamptz();
                let collection_timestamp = collection_timestamp.timestamp_millis();
                let collection_timestamp: u128 = collection_timestamp
                    .try_into()
                    .expect("all collections happen after Jan 1 1970");
                if collection_timestamp < cutoff_ts {
                    debug!("pruning storage event {row:?}");
                    let builtin_update = BuiltinTableUpdate::row(item_id, row, Diff::MINUS_ONE);
                    expired.push(builtin_update);
                }
            }

            // main thread has shut down.
            let _ = internal_cmd_tx.send(Message::StorageUsagePrune(expired));
        });
    }

    fn current_credit_consumption_rate(&self) -> Numeric {
        self.catalog()
            .user_cluster_replicas()
            .filter_map(|replica| match &replica.config.location {
                ReplicaLocation::Managed(location) => Some(location.size_for_billing()),
                ReplicaLocation::Unmanaged(_) => None,
            })
            .map(|size| {
                self.catalog()
                    .cluster_replica_sizes()
                    .0
                    .get(size)
                    .expect("location size is validated against the cluster replica sizes")
                    .credits_per_hour
            })
            .sum()
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
        let compute_instance = ComputeInstanceId::user(1).expect("1 is a valid ID");

        let _: () = self.ship_dataflow(dataflow, compute_instance, None).await;
    }
}

/// Contains information about the last message the [`Coordinator`] processed.
struct LastMessage {
    kind: &'static str,
    stmt: Option<Arc<Statement<Raw>>>,
}

impl LastMessage {
    /// Returns a redacted version of the statement that is safe for logs.
    fn stmt_to_string(&self) -> Cow<'static, str> {
        self.stmt
            .as_ref()
            .map(|stmt| stmt.to_ast_string_redacted().into())
            .unwrap_or(Cow::Borrowed("<none>"))
    }
}

impl fmt::Debug for LastMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LastMessage")
            .field("kind", &self.kind)
            .field("stmt", &self.stmt_to_string())
            .finish()
    }
}

impl Drop for LastMessage {
    fn drop(&mut self) {
        // Only print the last message if we're currently panicking, otherwise we'd spam our logs.
        if std::thread::panicking() {
            // If we're panicking theres no guarantee `tracing` still works, so print to stderr.
            eprintln!("Coordinator panicking, dumping last message\n{self:?}",);
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
///
/// BOXED FUTURE: As of Nov 2023 the returned Future from this function was 42KB. This would
/// get stored on the stack which is bad for runtime performance, and blow up our stack usage.
/// Because of that we purposefully move this Future onto the heap (i.e. Box it).
pub fn serve(
    Config {
        controller_config,
        controller_envd_epoch,
        mut storage,
        audit_logs_iterator,
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
        builtin_system_cluster_config,
        builtin_catalog_server_cluster_config,
        builtin_probe_cluster_config,
        builtin_support_cluster_config,
        builtin_analytics_cluster_config,
        system_parameter_defaults,
        availability_zones,
        storage_usage_client,
        storage_usage_collection_interval,
        storage_usage_retention_period,
        segment_client,
        egress_addresses,
        aws_account_id,
        aws_privatelink_availability_zones,
        connection_context,
        connection_limit_callback,
        remote_system_parameters,
        webhook_concurrency_limit,
        http_host_name,
        tracing_handle,
        read_only_controllers,
        caught_up_trigger: clusters_caught_up_trigger,
        helm_chart_version,
        license_key,
        external_login_password_mz_system,
        force_builtin_schema_migration,
    }: Config,
) -> BoxFuture<'static, Result<(Handle, Client), AdapterError>> {
    async move {
        let coord_start = Instant::now();
        info!("startup: coordinator init: beginning");
        info!("startup: coordinator init: preamble beginning");

        // Initializing the builtins can be an expensive process and consume a lot of memory. We
        // forcibly initialize it early while the stack is relatively empty to avoid stack
        // overflows later.
        let _builtins = LazyLock::force(&BUILTINS_STATIC);

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (internal_cmd_tx, internal_cmd_rx) = mpsc::unbounded_channel();
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

        info!(
            "startup: coordinator init: preamble complete in {:?}",
            coord_start.elapsed()
        );
        let oracle_init_start = Instant::now();
        info!("startup: coordinator init: timestamp oracle init beginning");

        let pg_timestamp_oracle_config = timestamp_oracle_url
            .map(|pg_url| PostgresTimestampOracleConfig::new(&pg_url, &metrics_registry));
        let mut initial_timestamps =
            get_initial_oracle_timestamps(&pg_timestamp_oracle_config).await?;

        // Insert an entry for the `EpochMilliseconds` timeline if one doesn't exist,
        // which will ensure that the timeline is initialized since it's required
        // by the system.
        initial_timestamps
            .entry(Timeline::EpochMilliseconds)
            .or_insert_with(mz_repr::Timestamp::minimum);
        let mut timestamp_oracles = BTreeMap::new();
        for (timeline, initial_timestamp) in initial_timestamps {
            Coordinator::ensure_timeline_state_with_initial_time(
                &timeline,
                initial_timestamp,
                now.clone(),
                pg_timestamp_oracle_config.clone(),
                &mut timestamp_oracles,
                read_only_controllers,
            )
            .await;
        }

        // Opening the durable catalog uses one or more timestamps without communicating with
        // the timestamp oracle. Here we make sure to apply the catalog upper with the timestamp
        // oracle to linearize future operations with opening the catalog.
        let catalog_upper = storage.current_upper().await;
        // Choose a time at which to boot. This is used, for example, to prune
        // old storage usage data or migrate audit log entries.
        //
        // This time is usually the current system time, but with protection
        // against backwards time jumps, even across restarts.
        let epoch_millis_oracle = &timestamp_oracles
            .get(&Timeline::EpochMilliseconds)
            .expect("inserted above")
            .oracle;

        let mut boot_ts = if read_only_controllers {
            let read_ts = epoch_millis_oracle.read_ts().await;
            std::cmp::max(read_ts, catalog_upper)
        } else {
            // Getting/applying a write timestamp bumps the write timestamp in the
            // oracle, which we're not allowed in read-only mode.
            epoch_millis_oracle.apply_write(catalog_upper).await;
            epoch_millis_oracle.write_ts().await.timestamp
        };

        info!(
            "startup: coordinator init: timestamp oracle init complete in {:?}",
            oracle_init_start.elapsed()
        );

        let catalog_open_start = Instant::now();
        info!("startup: coordinator init: catalog open beginning");
        let persist_client = controller_config
            .persist_clients
            .open(controller_config.persist_location.clone())
            .await
            .context("opening persist client")?;
        let builtin_item_migration_config =
            BuiltinItemMigrationConfig {
                persist_client: persist_client.clone(),
                read_only: read_only_controllers,
                force_migration: force_builtin_schema_migration,
            }
        ;
        let OpenCatalogResult {
            mut catalog,
            migrated_storage_collections_0dt,
            new_builtin_collections,
            builtin_table_updates,
            cached_global_exprs,
            uncached_local_exprs,
        } = Catalog::open(mz_catalog::config::Config {
            storage,
            metrics_registry: &metrics_registry,
            state: mz_catalog::config::StateConfig {
                unsafe_mode,
                all_features,
                build_info,
                deploy_generation: controller_config.deploy_generation,
                environment_id: environment_id.clone(),
                read_only: read_only_controllers,
                now: now.clone(),
                boot_ts: boot_ts.clone(),
                skip_migrations: false,
                cluster_replica_sizes,
                builtin_system_cluster_config,
                builtin_catalog_server_cluster_config,
                builtin_probe_cluster_config,
                builtin_support_cluster_config,
                builtin_analytics_cluster_config,
                system_parameter_defaults,
                remote_system_parameters,
                availability_zones,
                egress_addresses,
                aws_principal_context,
                aws_privatelink_availability_zones,
                connection_context,
                http_host_name,
                builtin_item_migration_config,
                persist_client: persist_client.clone(),
                enable_expression_cache_override: None,
                helm_chart_version,
                external_login_password_mz_system,
                license_key: license_key.clone(),
            },
        })
        .await?;

        // Opening the catalog uses one or more timestamps, so push the boot timestamp up to the
        // current catalog upper.
        let catalog_upper = catalog.current_upper().await;
        boot_ts = std::cmp::max(boot_ts, catalog_upper);

        if !read_only_controllers {
            epoch_millis_oracle.apply_write(boot_ts).await;
        }

        info!(
            "startup: coordinator init: catalog open complete in {:?}",
            catalog_open_start.elapsed()
        );

        let coord_thread_start = Instant::now();
        info!("startup: coordinator init: coordinator thread start beginning");

        let session_id = catalog.config().session_id;
        let start_instant = catalog.config().start_instant;

        // In order for the coordinator to support Rc and Refcell types, it cannot be
        // sent across threads. Spawn it in a thread and have this parent thread wait
        // for bootstrap completion before proceeding.
        let (bootstrap_tx, bootstrap_rx) = oneshot::channel();
        let handle = TokioHandle::current();

        let metrics = Metrics::register_into(&metrics_registry);
        let metrics_clone = metrics.clone();
        let optimizer_metrics = OptimizerMetrics::register_into(
            &metrics_registry,
            catalog.system_config().optimizer_e2e_latency_warning_threshold(),
        );
        let segment_client_clone = segment_client.clone();
        let coord_now = now.clone();
        let advance_timelines_interval = tokio::time::interval(catalog.config().timestamp_interval);
        let mut check_scheduling_policies_interval = tokio::time::interval(
            catalog
                .system_config()
                .cluster_check_scheduling_policies_interval(),
        );
        check_scheduling_policies_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let clusters_caught_up_check_interval = if read_only_controllers {
            let dyncfgs = catalog.system_config().dyncfgs();
            let interval = WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL.get(dyncfgs);

            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            interval
        } else {
            // When not in read-only mode, we don't do hydration checks. But we
            // still have to provide _some_ interval. This is large enough that
            // it doesn't matter.
            //
            // TODO(aljoscha): We cannot use Duration::MAX right now because of
            // https://github.com/tokio-rs/tokio/issues/6634. Use that once it's
            // fixed for good.
            let mut interval = tokio::time::interval(Duration::from_secs(60 * 60));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            interval
        };

        let clusters_caught_up_check =
            clusters_caught_up_trigger.map(|trigger| CaughtUpCheckContext {
                trigger,
                exclude_collections: new_builtin_collections.into_iter().collect(),
            });

        if let Some(config) = pg_timestamp_oracle_config.as_ref() {
            // Apply settings from system vars as early as possible because some
            // of them are locked in right when an oracle is first opened!
            let pg_timestamp_oracle_params =
                flags::pg_timstamp_oracle_config(catalog.system_config());
            pg_timestamp_oracle_params.apply(config);
        }

        // Register a callback so whenever the MAX_CONNECTIONS or SUPERUSER_RESERVED_CONNECTIONS
        // system variables change, we update our connection limits.
        let connection_limit_callback: Arc<dyn Fn(&SystemVars) + Send + Sync> =
            Arc::new(move |system_vars: &SystemVars| {
                let limit: u64 = system_vars.max_connections().cast_into();
                let superuser_reserved: u64 =
                    system_vars.superuser_reserved_connections().cast_into();

                // If superuser_reserved > max_connections, prefer max_connections.
                //
                // In this scenario all normal users would be locked out because all connections
                // would be reserved for superusers so complain if this is the case.
                let superuser_reserved = if superuser_reserved >= limit {
                    tracing::warn!(
                        "superuser_reserved ({superuser_reserved}) is greater than max connections ({limit})!"
                    );
                    limit
                } else {
                    superuser_reserved
                };

                (connection_limit_callback)(limit, superuser_reserved);
            });
        catalog.system_config_mut().register_callback(
            &mz_sql::session::vars::MAX_CONNECTIONS,
            Arc::clone(&connection_limit_callback),
        );
        catalog.system_config_mut().register_callback(
            &mz_sql::session::vars::SUPERUSER_RESERVED_CONNECTIONS,
            connection_limit_callback,
        );

        let (group_commit_tx, group_commit_rx) = appends::notifier();

        let parent_span = tracing::Span::current();
        let thread = thread::Builder::new()
            // The Coordinator thread tends to keep a lot of data on its stack. To
            // prevent a stack overflow we allocate a stack three times as big as the default
            // stack.
            .stack_size(3 * stack::STACK_SIZE)
            .name("coordinator".to_string())
            .spawn(move || {
                let span = info_span!(parent: parent_span, "coord::coordinator").entered();

                let controller = handle
                    .block_on({
                        catalog.initialize_controller(
                            controller_config,
                            controller_envd_epoch,
                            read_only_controllers,
                        )
                    })
                    .unwrap_or_terminate("failed to initialize storage_controller");
                // Initializing the controller uses one or more timestamps, so push the boot timestamp up to the
                // current catalog upper.
                let catalog_upper = handle.block_on(catalog.current_upper());
                boot_ts = std::cmp::max(boot_ts, catalog_upper);
                if !read_only_controllers {
                    let epoch_millis_oracle = &timestamp_oracles
                        .get(&Timeline::EpochMilliseconds)
                        .expect("inserted above")
                        .oracle;
                    handle.block_on(epoch_millis_oracle.apply_write(boot_ts));
                }

                let catalog = Arc::new(catalog);

                let caching_secrets_reader = CachingSecretsReader::new(secrets_controller.reader());
                let mut coord = Coordinator {
                    controller,
                    catalog,
                    internal_cmd_tx,
                    group_commit_tx,
                    strict_serializable_reads_tx,
                    global_timelines: timestamp_oracles,
                    transient_id_gen: Arc::new(TransientIdGen::new()),
                    active_conns: BTreeMap::new(),
                    txn_read_holds: Default::default(),
                    pending_peeks: BTreeMap::new(),
                    client_pending_peeks: BTreeMap::new(),
                    pending_linearize_read_txns: BTreeMap::new(),
                    serialized_ddl: LockedVecDeque::new(),
                    active_compute_sinks: BTreeMap::new(),
                    active_webhooks: BTreeMap::new(),
                    active_copies: BTreeMap::new(),
                    staged_cancellation: BTreeMap::new(),
                    introspection_subscribes: BTreeMap::new(),
                    write_locks: BTreeMap::new(),
                    deferred_write_ops: BTreeMap::new(),
                    pending_writes: Vec::new(),
                    advance_timelines_interval,
                    secrets_controller,
                    caching_secrets_reader,
                    cloud_resource_controller,
                    storage_usage_client,
                    storage_usage_collection_interval,
                    segment_client,
                    metrics,
                    optimizer_metrics,
                    tracing_handle,
                    statement_logging: StatementLogging::new(coord_now.clone()),
                    webhook_concurrency_limit,
                    pg_timestamp_oracle_config,
                    check_cluster_scheduling_policies_interval: check_scheduling_policies_interval,
                    cluster_scheduling_decisions: BTreeMap::new(),
                    caught_up_check_interval: clusters_caught_up_check_interval,
                    caught_up_check: clusters_caught_up_check,
                    installed_watch_sets: BTreeMap::new(),
                    connection_watch_sets: BTreeMap::new(),
                    cluster_replica_statuses: ClusterReplicaStatuses::new(),
                    read_only_controllers,
                    buffered_builtin_table_updates: Some(Vec::new()),
                    license_key,
                    persist_client,
                };
                let bootstrap = handle.block_on(async {
                    coord
                        .bootstrap(
                            boot_ts,
                            migrated_storage_collections_0dt,
                            builtin_table_updates,
                            cached_global_exprs,
                            uncached_local_exprs,
                            audit_logs_iterator,
                        )
                        .await?;
                    coord
                        .controller
                        .remove_orphaned_replicas(
                            coord.catalog().get_next_user_replica_id().await?,
                            coord.catalog().get_next_system_replica_id().await?,
                        )
                        .await
                        .map_err(AdapterError::Orchestrator)?;

                    if let Some(retention_period) = storage_usage_retention_period {
                        coord
                            .prune_storage_usage_events_on_startup(retention_period)
                            .await;
                    }

                    Ok(())
                });
                let ok = bootstrap.is_ok();
                drop(span);
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
                info!(
                    "startup: coordinator init: coordinator thread start complete in {:?}",
                    coord_thread_start.elapsed()
                );
                info!(
                    "startup: coordinator init: complete in {:?}",
                    coord_start.elapsed()
                );
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

// Determines and returns the highest timestamp for each timeline, for all known
// timestamp oracle implementations.
//
// Initially, we did this so that we can switch between implementations of
// timestamp oracle, but now we also do this to determine a monotonic boot
// timestamp, a timestamp that does not regress across reboots.
//
// This mostly works, but there can be linearizability violations, because there
// is no central moment where we do distributed coordination for all oracle
// types. Working around this seems prohibitively hard, maybe even impossible so
// we have to live with this window of potential violations during the upgrade
// window (which is the only point where we should switch oracle
// implementations).
async fn get_initial_oracle_timestamps(
    pg_timestamp_oracle_config: &Option<PostgresTimestampOracleConfig>,
) -> Result<BTreeMap<Timeline, Timestamp>, AdapterError> {
    let mut initial_timestamps = BTreeMap::new();

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

#[instrument]
pub async fn load_remote_system_parameters(
    storage: &mut Box<dyn OpenableDurableCatalogState>,
    system_parameter_sync_config: Option<SystemParameterSyncConfig>,
    system_parameter_sync_timeout: Duration,
) -> Result<Option<BTreeMap<String, String>>, AdapterError> {
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
        // after we've synced once only blocks for a maximum of
        // `FRONTEND_SYNC_TIMEOUT` on LaunchDarkly, as it seems
        // reasonable to assume that the last-synced configuration was
        // valid enough.
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
        let mut params = SynchronizedParameters::new(SystemVars::default());
        let frontend_sync = async {
            let frontend = SystemParameterFrontend::from(&system_parameter_sync_config).await?;
            frontend.pull(&mut params);
            let ops = params
                .modified()
                .into_iter()
                .map(|param| {
                    let name = param.name;
                    let value = param.value;
                    tracing::info!(name, value, initial = true, "sync parameter");
                    (name, value)
                })
                .collect();
            tracing::info!("parameter sync on boot: end sync");
            Ok(Some(ops))
        };
        if !storage.has_system_config_synced_once().await? {
            frontend_sync.await
        } else {
            match mz_ore::future::timeout(system_parameter_sync_timeout, frontend_sync).await {
                Ok(ops) => Ok(ops),
                Err(TimeoutError::Inner(e)) => Err(e),
                Err(TimeoutError::DeadlineElapsed) => {
                    tracing::info!("parameter sync on boot: sync has timed out");
                    Ok(None)
                }
            }
        }
    } else {
        Ok(None)
    }
}

#[derive(Debug)]
pub enum WatchSetResponse {
    StatementDependenciesReady(StatementLoggingId, StatementLifecycleEvent),
    AlterSinkReady(AlterSinkReadyContext),
}

#[derive(Debug)]
pub struct AlterSinkReadyContext {
    ctx: Option<ExecuteContext>,
    otel_ctx: OpenTelemetryContext,
    plan: AlterSinkPlan,
    plan_validity: PlanValidity,
    read_hold: ReadHolds<Timestamp>,
}

impl AlterSinkReadyContext {
    fn ctx(&mut self) -> &mut ExecuteContext {
        self.ctx.as_mut().expect("only cleared on drop")
    }

    fn retire(mut self, result: Result<ExecuteResponse, AdapterError>) {
        self.ctx
            .take()
            .expect("only cleared on drop")
            .retire(result);
    }
}

impl Drop for AlterSinkReadyContext {
    fn drop(&mut self) {
        if let Some(ctx) = self.ctx.take() {
            ctx.retire(Err(AdapterError::Canceled));
        }
    }
}

/// A struct for tracking the ownership of a lock and a VecDeque to store to-be-done work after the
/// lock is freed.
#[derive(Debug)]
struct LockedVecDeque<T> {
    items: VecDeque<T>,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl<T> LockedVecDeque<T> {
    pub fn new() -> Self {
        Self {
            items: VecDeque::new(),
            lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn try_lock_owned(&self) -> Result<OwnedMutexGuard<()>, tokio::sync::TryLockError> {
        Arc::clone(&self.lock).try_lock_owned()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn push_back(&mut self, value: T) {
        self.items.push_back(value)
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    pub fn remove(&mut self, index: usize) -> Option<T> {
        self.items.remove(index)
    }

    pub fn iter(&self) -> std::collections::vec_deque::Iter<'_, T> {
        self.items.iter()
    }
}

#[derive(Debug)]
struct DeferredPlanStatement {
    ctx: ExecuteContext,
    ps: PlanStatement,
}

#[derive(Debug)]
enum PlanStatement {
    Statement {
        stmt: Arc<Statement<Raw>>,
        params: Params,
    },
    Plan {
        plan: mz_sql::plan::Plan,
        resolved_ids: ResolvedIds,
    },
}

#[derive(Debug, Error)]
pub enum NetworkPolicyError {
    #[error("Access denied for address {0}")]
    AddressDenied(IpAddr),
    #[error("Access denied missing IP address")]
    MissingIp,
}

pub(crate) fn validate_ip_with_policy_rules(
    ip: &IpAddr,
    rules: &Vec<NetworkPolicyRule>,
) -> Result<(), NetworkPolicyError> {
    // At the moment we're not handling action or direction
    // as those are only able to be "allow" and "ingress" respectively
    if rules.iter().any(|r| r.address.0.contains(ip)) {
        Ok(())
    } else {
        Err(NetworkPolicyError::AddressDenied(ip.clone()))
    }
}
