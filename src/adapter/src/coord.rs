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
use itertools::Itertools;
use mz_adapter_types::bootstrap_builtin_cluster_config::BootstrapBuiltinClusterConfig;
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_adapter_types::dyncfgs::{ENABLE_0DT_HYDRATE_MIGRATED_BUILTIN_MVS, USER_ID_POOL_BATCH_SIZE};
use mz_auth::password::Password;
use mz_build_info::BuildInfo;
use mz_catalog::builtin::{
    BUILTINS, BUILTINS_STATIC, MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY, MZ_OBJECT_HYDRATION_HISTORY,
    MZ_REPLICA_HYDRATION_HISTORY, MZ_STORAGE_USAGE_BY_SHARD,
};
use mz_catalog::config::{AwsPrincipalContext, BuiltinItemMigrationConfig, ClusterReplicaSizeMap};
use mz_catalog::durable::OpenableDurableCatalogState;
use mz_catalog::expr_cache::{GlobalExpressions, LocalExpressions, latest_item_version};
use mz_catalog::memory::objects::{
    CatalogEntry, CatalogItem, ClusterReplicaProcessStatus, Connection, DataSourceDesc, Index,
    MaterializedView, MetricSink, ReconfigurationTarget, Table, TableDataSource,
};
use mz_cloud_resources::{CloudResourceController, VpcEndpointConfig, VpcEndpointEvent};
use mz_compute_client::as_of_selection;
use mz_compute_client::controller::error::{
    CollectionLookupError, CollectionMissing, DataflowCreationError, InstanceMissing,
};
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::LirRelationExpr;
use mz_controller::clusters::{
    ClusterConfig, ClusterEvent, ClusterStatus, ManagedReplicaLocation, ProcessId, ReplicaLocation,
};
use mz_controller::{ControllerConfig, Readiness};
use mz_controller_types::{ClusterId, ReplicaId, WatchSetId};
use mz_dyncfg::{ConfigUpdates, ParameterScope};
use mz_expr::{MapFilterProject, OptimizedMirRelationExpr, RowSetFinishing};
use mz_license_keys::{ExpirationBehavior, ValidatedLicenseKey};
use mz_orchestrator::OfflineReason;
use mz_ore::cast::{CastFrom, CastInto, CastLossy};
use mz_ore::channel::trigger::Trigger;
use mz_ore::future::TimeoutError;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::task::{AbortOnDropHandle, JoinHandle, spawn};
use mz_ore::thread::JoinHandleExt;
use mz_ore::tracing::{OpenTelemetryContext, TracingHandle};
use mz_ore::{
    assert_none, instrument, soft_assert_eq_or_log, soft_assert_or_log, soft_panic_or_log, stack,
};
use mz_persist_client::PersistClient;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::usage::{ShardsUsageReferenced, StorageUsageClient};
use mz_repr::adt::numeric::Numeric;
use mz_repr::explain::{ExplainConfig, ExplainFormat};
use mz_repr::global_id::TransientIdGen;
use mz_repr::optimize::{OptimizerFeatureOverrides, OptimizerFeatures, OverrideFrom};
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationDesc, RelationVersion, Timestamp};
use mz_secrets::cache::CachingSecretsReader;
use mz_secrets::{SecretsController, SecretsReader};
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::{CatalogCluster, EnvironmentId};
use mz_sql::names::{QualifiedItemName, ResolvedIds};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::{
    self, AlterSinkPlan, ConnectionDetails, CreateConnectionPlan, HirRelationExpr,
    NetworkPolicyRule, Params, QueryWhen,
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
use mz_storage_types::read_policy::ReadPolicy;
use mz_storage_types::sinks::{S3SinkFormat, StorageSinkDesc};
use mz_storage_types::sources::kafka::KAFKA_PROGRESS_DESC;
use mz_storage_types::sources::{IngestionDescription, SourceExport, Timeline};
use mz_timestamp_oracle::{TimestampOracleConfig, WriteTimestamp};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;
use opentelemetry::trace::TraceContextExt;
use semver::Version;
use serde::Serialize;
use thiserror::Error;
use timely::progress::{Antichain, Timestamp as _};
use tokio::runtime::Handle as TokioHandle;
use tokio::select;
use tokio::sync::{Notify, OwnedMutexGuard, Semaphore, mpsc, oneshot, watch};
use tokio::time::Interval;
use tracing::{Instrument, Level, Span, debug, info, info_span, span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::active_compute_sink::{ActiveComputeSink, ActiveCopyFrom};
use crate::catalog::{BuiltinTableUpdate, Catalog, CatalogState, OpenCatalogResult};
use crate::client::{Client, Handle, truncate_sql_for_logging};
use crate::command::{Command, ExecuteResponse};
use crate::config::{
    ClusterEvalContext, ClusterScopeContext, ReplicaEvalContext, ReplicaScopeContext,
    ScopedParameters, ScopedParametersScope, SynchronizedParameters, SystemParameterFrontend,
    SystemParameterSyncConfig,
};
use crate::coord::appends::{
    BuiltinTableAppendCompletion, BuiltinTableAppendNotify, DeferredPlan, GroupCommitPermit,
    PendingWriteTxn,
};
use crate::coord::caught_up::CaughtUpCheckContext;
use crate::coord::compaction_bound_subscriber::CompactionBoundSubscriber;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::introspection::IntrospectionSubscribe;
use crate::coord::metric_sink::{CuratedMetricSink, InstalledMetricSink, PlannedMetricSink};
use crate::coord::peek::PendingPeek;
use crate::coord::read_protection::CATALOG_SUBSCRIPTION_INTERVAL;
use crate::coord::statement_logging::StatementLogging;
use crate::coord::timeline::{TimelineContext, TimelineState};
use crate::coord::timestamp_selection::{TimestampContext, TimestampDetermination};
use crate::coord::validity::PlanValidity;
use crate::error::AdapterError;
use crate::explain::insights::PlanInsightsContext;
use crate::explain::optimizer_trace::{DispatchGuard, OptimizerTrace};
use crate::metrics::Metrics;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
use crate::optimize::{self, Optimize, OptimizerConfig};
use crate::session::{EndTransactionAction, Session};
use crate::statement_logging::{
    StatementEndedExecutionReason, StatementLifecycleEvent, StatementLoggingId,
};
use crate::util::{ClientTransmitter, ResultExt, sort_topological};
use crate::webhook::{WebhookAppenderInvalidator, WebhookConcurrencyLimiter};
use crate::{AdapterNotice, ReadHolds, flags};

pub(crate) mod appends;
pub(crate) mod catalog_serving;
pub(crate) mod cluster_controller;
pub(crate) mod consistency;
pub(crate) mod id_bundle;
pub(crate) mod in_memory_oracle;
pub(crate) mod peek;
pub(crate) mod read_policy;
mod read_protection;
pub(crate) mod read_then_write;
pub(crate) mod sequencer;
pub(crate) mod statement_logging;
pub(crate) mod timeline;
pub(crate) mod timestamp_selection;

pub mod catalog_implications;
mod catalog_reads;
mod caught_up;
mod command_handler;
mod compaction_bound_subscriber;
mod ddl;
pub(crate) mod group_sync;
mod hydration_history;
mod indexes;
mod info_metrics;
mod introspection;
mod message_handler;
mod metric_sink;
mod privatelink_status;
mod query_execution;
mod sql;
mod storage_bootstrap;
mod validity;

/// The oldest leader version against which a replacement-migrated builtin materialized view may
/// write its new persist shard while this environment is still read-only.
///
/// Every builtin materialized view reads `mz_internal.mz_catalog_raw`, so its dataflow only makes
/// progress up to the catalog shard's frontier. Holding that frontier at the current time is the
/// leader's job, and leaders only started doing it in v26.17 (PR #35402). Write-enable such an MV
/// against an older leader and it sits at a stale frontier and never reports caught up, which
/// blocks promotion outright instead of merely leaving the collection cold at cut-over. We still
/// support upgrading from before v26.17, so that leader is a real case, not a hypothetical.
const MIN_LEADER_VERSION_FOR_MIGRATED_MV_WRITES: Version = Version::new(26, 17, 0);

/// A pool of pre-allocated user IDs to avoid per-DDL persist writes.
///
/// IDs in the range `[next, upper)` are available for allocation.
/// When exhausted, the pool must be refilled via the catalog.
///
/// # Correctness
///
/// The pool is owned by [`Coordinator`], which processes all requests
/// on a single-threaded event loop. Because every access requires
/// `&mut self` on the coordinator, there is no concurrent access to the
/// pool — no additional synchronization is needed.
///
/// Global ID uniqueness is guaranteed because each refill calls
/// [`mz_catalog::catalog::Catalog::allocate_user_ids`], which performs a durable persist
/// write that atomically reserves the entire batch before any IDs from
/// it are handed out. If the process crashes after a refill but before
/// all pre-allocated IDs are consumed, the unused IDs form harmless
/// gaps in the sequence — user IDs are not required to be contiguous.
///
/// This guarantee holds even if multiple `environmentd` processes run
/// concurrently. Each process has its own independent pool,
/// but every refill goes through the shared persist-backed catalog,
/// which serializes allocations across all callers. Two processes
/// will therefore never receive overlapping ID ranges,
/// for the same reason they could not before this pool existed.
#[derive(Debug)]
pub(crate) struct IdPool {
    next: u64,
    upper: u64,
}

impl IdPool {
    /// Creates an empty pool.
    pub fn empty() -> Self {
        IdPool { next: 0, upper: 0 }
    }

    /// Allocates a single ID from the pool, returning `None` if exhausted.
    pub fn allocate(&mut self) -> Option<u64> {
        if self.next < self.upper {
            let id = self.next;
            self.next += 1;
            Some(id)
        } else {
            None
        }
    }

    /// Allocates `n` consecutive IDs from the pool, returning `None` if
    /// insufficient IDs remain.
    pub fn allocate_many(&mut self, n: u64) -> Option<Vec<u64>> {
        if self.remaining() >= n {
            let ids = (self.next..self.next + n).collect();
            self.next += n;
            Some(ids)
        } else {
            None
        }
    }

    /// Returns the number of IDs remaining in the pool.
    pub fn remaining(&self) -> u64 {
        self.upper - self.next
    }

    /// Refills the pool with the given range `[next, upper)`.
    pub fn refill(&mut self, next: u64, upper: u64) {
        assert!(next <= upper, "invalid pool range: {next}..{upper}");
        self.next = next;
        self.upper = upper;
    }
}

/// A row for `mz_object_arrangement_size_history`, prepared off-thread by the
/// arrangement sizes snapshot task and stamped with a collection timestamp at
/// write time.
#[derive(Debug)]
pub struct ArrangementSizeRecord {
    pub replica_id: String,
    pub object_id: String,
    pub size: i64,
    pub hydration_complete: bool,
}

#[derive(Debug)]
pub enum Message {
    Command(OpenTelemetryContext, Command),
    QueryDataflowResponse(crate::query_client::compute::DataflowResponse),
    QueryWatchSetReady(WatchSetId, Result<(), AdapterError>),
    ClientReadProtectionReady(Box<read_protection::PendingReadProtection>),
    ControllerReady {
        controller: ControllerReadiness,
    },
    ExecuteCatalogReady {
        ctx: ExecuteContext,
        continuation: catalog_reads::ExecuteCatalogContinuation,
        otel_ctx: OpenTelemetryContext,
    },
    ExecuteReplan(ExecuteContext),
    PurifiedStatementReady(PurifiedStatementReady),
    CreateConnectionValidationReady(CreateConnectionValidationReady),
    AlterConnectionValidationReady(AlterConnectionValidationReady),
    DeferredPlanReady {
        /// The connection whose session-startup appends completed.
        conn_id: ConnectionId,
    },
    /// Initiates a group commit.
    GroupCommitInitiate(Span, Option<GroupCommitPermit>),
    /// Finalizes an applied group commit.
    ///
    /// Statement timestamps precede response retirement because retirement ends statement logging.
    GroupCommitApplied {
        /// Responses to retire after recording statement timestamps.
        responses: Vec<crate::util::CompletedClientTransmitter>,
        /// Statement executions associated with this commit.
        statement_logging_ids: Vec<StatementLoggingId>,
        /// Frontend-sequenced writes to complete after local timestamp bookkeeping.
        internal_results: Vec<crate::coord::appends::InternalWriteResponder>,
        /// The applied write timestamp.
        write_ts: Timestamp,
    },
    DeferredStatementReady,
    AdvanceTimelines,
    ClusterEvent(ClusterEvent),
    LinearizeReads,
    StagedBatches {
        conn_id: ConnectionId,
        ingestion_id: Uuid,
        table_id: CatalogItemId,
        batches: Vec<Result<ProtoBatch, String>>,
    },
    StorageUsageSchedule,
    StorageUsageFetch,
    StorageUsageUpdate(ShardsUsageReferenced),
    StorageUsagePrune(Vec<BuiltinTableUpdate>),
    ArrangementSizesSchedule,
    ArrangementSizesSnapshot,
    ArrangementSizesWrite(Vec<ArrangementSizeRecord>),
    ArrangementSizesPrune(Vec<BuiltinTableUpdate>),
    HydrationHistorySchedule,
    HydrationHistoryRun,
    CaughtUpCheck(caught_up::CaughtUpCheckRequest),
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
    DdlCommitStageReady {
        ctx: DdlCommitContext,
        span: Span,
        stage: DdlCommitStage,
    },
    CatalogCommitStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: CatalogCommitStage,
    },
    DropObjectsStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: DropObjectsStage,
    },
    CreateIndexStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: CreateIndexStage,
    },
    CreateMetricSinkStageReady {
        ctx: ExecuteContext,
        span: Span,
        stage: CreateMetricSinkStage,
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
    MetricSinkStageReady {
        span: Span,
        stage: MetricSinkStage,
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

    /// One pull/apply call from the cluster controller task, answered on the main
    /// coordinator message loop from the catalog and live controller signals.
    /// See [`cluster_controller`].
    ClusterControllerRequest(cluster_controller::ClusterControllerRequest),
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
                Command::UpdateScopedSystemParameters { .. } => {
                    "command-update_scoped_system_parameters"
                }
                Command::InstallScopedSystemParameterFrontend { .. } => {
                    "command-install_scoped_system_parameter_frontend"
                }
                Command::Terminate { .. } => "command-terminate",
                Command::RetireExecute { .. } => "command-retire_execute",
                Command::CheckConsistency { .. } => "command-check_consistency",
                Command::Dump { .. } => "command-dump",
                Command::AuthenticatePassword { .. } => "command-auth_check",
                Command::AuthenticateGetSASLChallenge { .. } => "command-auth_get_sasl_challenge",
                Command::AuthenticateVerifySASLProof { .. } => "command-auth_verify_sasl_proof",
                Command::CheckRoleCanLogin { .. } => "command-check_role_can_login",
                Command::GetComputeInstanceClient { .. } => "get-compute-instance-client",
                Command::AcquireClientReadProtection { .. } => "acquire-client-read-protection",
                Command::GetOracle { .. } => "get-oracle",
                Command::DetermineRealTimeRecentTimestamp { .. } => {
                    "determine-real-time-recent-timestamp"
                }
                Command::GetTransactionReadHoldsBundle { .. } => {
                    "get-transaction-read-holds-bundle"
                }
                Command::StoreTransactionReadHolds { .. } => "store-transaction-read-holds",
                Command::ExecuteSlowPathPeek { .. } => "execute-slow-path-peek",
                Command::ExecuteSubscribe { .. } => "execute-subscribe",
                Command::CopyToPreflight { .. } => "copy-to-preflight",
                Command::ExecuteCopyTo { .. } => "execute-copy-to",
                Command::ExecuteSideEffectingFunc { .. } => "execute-side-effecting-func",
                Command::LookupConnection { .. } => "lookup-connection",
                Command::RegisterFrontendPeek { .. } => "register-frontend-peek",
                Command::UnregisterFrontendPeek { .. } => "unregister-frontend-peek",
                Command::ExplainTimestamp { .. } => "explain-timestamp",
                Command::FrontendStatementLogging(..) => "frontend-statement-logging",
                Command::StartCopyFromStdin { .. } => "start-copy-from-stdin",
                Command::InjectAuditEvents { .. } => "inject-audit-events",
                Command::RegisterConnectionCancelWatch { .. } => "register-connection-cancel-watch",
                Command::CreateInternalSubscribe { .. } => "create-internal-subscribe",
                Command::AttemptWrite { .. } => "attempt-write",
                Command::DropInternalSubscribe { .. } => "drop-internal-subscribe",
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
            Message::QueryDataflowResponse(_) => "query_dataflow_response",
            Message::QueryWatchSetReady(..) => "query_watch_set_ready",
            Message::ClientReadProtectionReady(..) => "client_read_protection_ready",
            Message::ExecuteCatalogReady { .. } => "execute_catalog_ready",
            Message::ExecuteReplan(_) => "execute_replan",
            Message::PurifiedStatementReady(_) => "purified_statement_ready",
            Message::CreateConnectionValidationReady(_) => "create_connection_validation_ready",
            Message::DeferredPlanReady { .. } => "deferred_plan_ready",
            Message::GroupCommitInitiate(..) => "group_commit_initiate",
            Message::GroupCommitApplied { .. } => "group_commit_applied",
            Message::AdvanceTimelines => "advance_timelines",
            Message::ClusterEvent(_) => "cluster_event",
            Message::LinearizeReads => "linearize_reads",
            Message::StagedBatches { .. } => "staged_batches",
            Message::StorageUsageSchedule => "storage_usage_schedule",
            Message::StorageUsageFetch => "storage_usage_fetch",
            Message::StorageUsageUpdate(_) => "storage_usage_update",
            Message::StorageUsagePrune(_) => "storage_usage_prune",
            Message::ArrangementSizesSchedule => "arrangement_sizes_schedule",
            Message::ArrangementSizesSnapshot => "arrangement_sizes_snapshot",
            Message::ArrangementSizesWrite(_) => "arrangement_sizes_write",
            Message::ArrangementSizesPrune(_) => "arrangement_sizes_prune",
            Message::HydrationHistorySchedule => "hydration_history_schedule",
            Message::HydrationHistoryRun => "hydration_history_run",
            Message::CaughtUpCheck(_) => "caught_up_check",
            Message::RetireExecute { .. } => "retire_execute",
            Message::ExecuteSingleStatementTransaction { .. } => {
                "execute_single_statement_transaction"
            }
            Message::PeekStageReady { .. } => "peek_stage_ready",
            Message::ExplainTimestampStageReady { .. } => "explain_timestamp_stage_ready",
            Message::DdlCommitStageReady { .. } => "ddl_commit_stage_ready",
            Message::CatalogCommitStageReady { .. } => "catalog_commit_stage_ready",
            Message::DropObjectsStageReady { .. } => "drop_objects_stage_ready",
            Message::CreateIndexStageReady { .. } => "create_index_stage_ready",
            Message::CreateMetricSinkStageReady { .. } => "create_metric_sink_stage_ready",
            Message::CreateViewStageReady { .. } => "create_view_stage_ready",
            Message::CreateMaterializedViewStageReady { .. } => {
                "create_materialized_view_stage_ready"
            }
            Message::SubscribeStageReady { .. } => "subscribe_stage_ready",
            Message::IntrospectionSubscribeStageReady { .. } => {
                "introspection_subscribe_stage_ready"
            }
            Message::MetricSinkStageReady { .. } => "metric_sink_stage_ready",
            Message::SecretStageReady { .. } => "secret_stage_ready",
            Message::ClusterStageReady { .. } => "cluster_stage_ready",
            Message::DrainStatementLog => "drain_statement_log",
            Message::AlterConnectionValidationReady(..) => "alter_connection_validation_ready",
            Message::PrivateLinkVpcEndpointEvents(_) => "private_link_vpc_endpoint_events",
            Message::ClusterControllerRequest(_) => "cluster_controller_request",
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
    TimestampValidated(PeekStageTimestampValidated),
    Optimize(PeekStageOptimize),
    /// Final stage for a peek.
    Finish(PeekStageFinish),
    /// Requested diagnostic observations, obtained before dispatching the peek.
    FinishWithTimestampNotice(PeekStageFinish, crate::TimestampExplanation),
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
    optimizer: optimize::PeekOptimizer,
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
    optimizer: optimize::PeekOptimizer,
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
    optimizer: optimize::PeekOptimizer,
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
    determination: TimestampDetermination,
    optimizer: optimize::PeekOptimizer,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
}

#[derive(Debug)]
pub struct PeekStageTimestampValidated {
    stage: PeekStageOptimize,
    read_holds: Option<ReadHolds>,
}

#[derive(Debug)]
pub struct PeekStageFinish {
    validity: PlanValidity,
    plan: mz_sql::plan::SelectPlan,
    max_query_result_size: Option<u64>,
    id_bundle: CollectionIdBundle,
    target_replica: Option<ReplicaId>,
    source_ids: BTreeSet<GlobalId>,
    determination: TimestampDetermination,
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
    target_replica: Option<ReplicaId>,
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
    determination: TimestampDetermination,
    imports: BTreeMap<GlobalId, MapFilterProject>,
}

/// Owns COMMIT after the session's accumulated transaction has been extracted.
#[derive(Debug)]
pub struct DdlCommitContext(ExecuteContext);

pub struct DdlCommitStage {
    validity: PlanValidity,
    planning_revision: u64,
    ops: Vec<crate::catalog::Op>,
    prepared: Option<ddl::PreparedCatalogTransaction>,
    side_effects: Vec<crate::session::DdlSideEffect>,
}

impl std::fmt::Debug for DdlCommitStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DdlCommitStage")
            .field("planning_revision", &self.planning_revision)
            .finish_non_exhaustive()
    }
}

/// SQL work whose only remaining effect is a catalog commit and response.
#[derive(Debug)]
pub struct CatalogCommitStage {
    validity: PlanValidity,
    planning_revision: u64,
    ops: Vec<crate::catalog::Op>,
    prepared: Option<ddl::PreparedCatalogTransaction>,
    response: ExecuteResponse,
}

#[derive(Debug)]
pub struct DropObjectsStage {
    validity: PlanValidity,
    // A DROP cascade depends on the full structural snapshot, even when its
    // explicit dependencies still exist after an intervening local DDL.
    planning_revision: u64,
    ops: Vec<crate::catalog::Op>,
    prepared: Option<ddl::PreparedCatalogTransaction>,
    object_type: mz_sql::catalog::ObjectType,
    dropped_active_db: bool,
    dropped_active_cluster: bool,
    expr_cache_invalidate_ids: BTreeSet<GlobalId>,
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
    optimizer_features: OptimizerFeatures,
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
pub enum CreateMetricSinkStage {
    Optimize(CreateMetricSinkOptimize),
    Finish(CreateMetricSinkFinish),
}

#[derive(Debug)]
pub struct CreateMetricSinkOptimize {
    validity: PlanValidity,
    plan: plan::CreateMetricSinkPlan,
    resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct CreateMetricSinkFinish {
    validity: PlanValidity,
    item_id: CatalogItemId,
    global_id: GlobalId,
    plan: plan::CreateMetricSinkPlan,
    resolved_ids: ResolvedIds,
    global_mir_plan: optimize::metric_sink::GlobalMirPlan,
    global_lir_plan: optimize::metric_sink::GlobalLirPlan,
    optimizer_features: OptimizerFeatures,
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
    LinearizeTimestamp(ExplainTimestampLinearizeTimestamp),
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
pub struct ExplainTimestampLinearizeTimestamp {
    validity: PlanValidity,
    format: ExplainFormat,
    optimized_plan: OptimizedMirRelationExpr,
    cluster_id: ClusterId,
    source_ids: BTreeSet<GlobalId>,
    when: QueryWhen,
    real_time_recency_ts: Option<Timestamp>,
}

#[derive(Debug)]
pub struct ExplainTimestampFinish {
    validity: PlanValidity,
    format: ExplainFormat,
    cluster_id: ClusterId,
    source_ids: BTreeSet<GlobalId>,
    when: QueryWhen,
    real_time_recency_ts: Option<Timestamp>,
    /// The timeline context derived in the preceding `LinearizeTimestamp`
    /// stage, carried forward so it stays consistent with `oracle_read_ts`.
    timeline_context: TimelineContext,
    /// The linearized read timestamp, read off the coordinator loop in the
    /// preceding `LinearizeTimestamp` stage. `None` when no linearized read is
    /// needed.
    oracle_read_ts: Option<Timestamp>,
}

#[derive(Debug)]
pub enum ClusterStage {
    Alter(AlterCluster),
    /// The foreground wait-shim over a controller-driven background
    /// reconfiguration: poll the durable `reconfiguration` record until it
    /// clears, then report success or timeout depending on whether the realized
    /// config reached the target.
    AwaitReconfiguration(AlterClusterAwaitReconfiguration),
}

#[derive(Debug)]
pub struct AlterCluster {
    validity: PlanValidity,
    plan: plan::AlterClusterPlan,
}

#[derive(Debug)]
pub struct AlterClusterAwaitReconfiguration {
    validity: PlanValidity,
    cluster_id: ClusterId,
    /// The target shape the awaited `ALTER` wrote. Once the record becomes
    /// terminal, the realized config matching this is what distinguishes a
    /// cut-over from a failure. See `await_reconfiguration_stage`.
    target: ReconfigurationTarget,
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
    optimizer_features: OptimizerFeatures,
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
    LinearizeTimestamp(SubscribeLinearizeTimestamp),
    TimestampOptimizeLir(SubscribeTimestampOptimizeLir),
    TimestampValidated(SubscribeTimestampValidated),
    Finish(SubscribeFinish),
    Explain(SubscribeExplain),
}

#[derive(Debug)]
pub struct SubscribeOptimizeMir {
    validity: PlanValidity,
    plan: plan::SubscribePlan,
    timeline: TimelineContext,
    dependency_ids: BTreeSet<GlobalId>,
    cluster_id: ComputeInstanceId,
    replica_id: Option<ReplicaId>,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
}

#[derive(Debug)]
pub struct SubscribeLinearizeTimestamp {
    validity: PlanValidity,
    plan: plan::SubscribePlan,
    timeline: TimelineContext,
    optimizer: optimize::subscribe::Optimizer,
    global_mir_plan: optimize::subscribe::GlobalMirPlan<optimize::subscribe::Unresolved>,
    dependency_ids: BTreeSet<GlobalId>,
    replica_id: Option<ReplicaId>,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
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
    /// The linearized read timestamp, read off the coordinator loop in the
    /// preceding `LinearizeTimestamp` stage. `None` when no linearized read is
    /// needed.
    oracle_read_ts: Option<Timestamp>,
    /// An optional context set iff the state machine is initiated from
    /// sequencing an EXPLAIN for this statement.
    explain_ctx: ExplainContext,
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
pub struct SubscribeTimestampValidated {
    stage: SubscribeTimestampOptimizeLir,
    determination: TimestampDetermination,
    read_holds: ReadHolds,
}

#[derive(Debug)]
pub struct SubscribeExplain {
    validity: PlanValidity,
    optimizer: optimize::subscribe::Optimizer,
    df_meta: DataflowMetainfo,
    cluster_id: ComputeInstanceId,
    explain_ctx: ExplainPlanContext,
}

#[derive(Debug)]
pub enum IntrospectionSubscribeStage {
    OptimizeMir(IntrospectionSubscribeOptimizeMir),
    TimestampOptimizeLir(IntrospectionSubscribeTimestampOptimizeLir),
    Finish(IntrospectionSubscribeFinish),
}

#[derive(Debug)]
pub struct IntrospectionSubscribeOptimizeMir {
    catalog: Arc<Catalog>,
    validity: PlanValidity,
    plan: plan::SubscribePlan,
    subscribe_id: GlobalId,
    cluster_id: ComputeInstanceId,
    replica_id: ReplicaId,
}

#[derive(Debug)]
pub struct IntrospectionSubscribeTimestampOptimizeLir {
    catalog: Arc<Catalog>,
    validity: PlanValidity,
    optimizer: optimize::subscribe::Optimizer,
    global_mir_plan: optimize::subscribe::GlobalMirPlan<optimize::subscribe::Unresolved>,
    cluster_id: ComputeInstanceId,
    replica_id: ReplicaId,
}

#[derive(Debug)]
pub struct IntrospectionSubscribeFinish {
    catalog: Arc<Catalog>,
    validity: PlanValidity,
    global_lir_plan: optimize::subscribe::GlobalLirPlan,
    read_holds: ReadHolds,
    cluster_id: ComputeInstanceId,
    replica_id: ReplicaId,
}

#[derive(Debug)]
pub enum MetricSinkStage {
    Optimize(MetricSinkOptimize),
    Finish(MetricSinkFinish),
}

#[derive(Debug)]
pub struct MetricSinkOptimize {
    validity: PlanValidity,
    definition: &'static CuratedMetricSink,
    /// The transient id of the sink's compute export. Recorded in
    /// [`Coordinator::metric_sinks`] once the finish stage ships the dataflow.
    sink_id: GlobalId,
    /// The planned `source_sql`, and the shape it produces.
    expr: HirRelationExpr,
    desc: RelationDesc,
    cluster_id: ComputeInstanceId,
    replica_id: ReplicaId,
}

#[derive(Debug)]
pub struct MetricSinkFinish {
    validity: PlanValidity,
    definition: &'static CuratedMetricSink,
    sink_id: GlobalId,
    global_lir_plan: optimize::metric_sink::GlobalLirPlan,
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
    /// Async admission work whose resources are released on cancellation.
    Await(futures::future::BoxFuture<'static, Result<T, AdapterError>>),
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

    /// Validates this continuation before execution. Explicit transactions may
    /// require a stricter structural check than individual statement plans.
    fn check_validity(&mut self, catalog: &Catalog) -> Result<(), AdapterError> {
        self.validity().check(catalog)
    }

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

    /// Handle an error before a stage installs execution. Statement contexts
    /// can retry catalog invalidation without retiring their logging obligation.
    fn handle_error(self, error: AdapterError)
    where
        Self: Sized,
    {
        self.retire(Err(error));
    }
}

impl StagedContext for ExecuteContext {
    fn handle_error(self, error: AdapterError) {
        if matches!(&error, AdapterError::CatalogSnapshotChanged) && self.query_replan.is_some() {
            let sender = self.internal_cmd_tx.clone();
            let _ = sender.send(Message::ExecuteReplan(self));
        } else {
            self.retire(Err(error));
        }
    }

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
    pub controller_envd_epoch: std::num::NonZeroI64,
    pub storage: Box<dyn mz_catalog::durable::DurableCatalogState>,
    pub client_protection_storage: Option<Box<dyn mz_catalog::durable::DurableCatalogState>>,
    pub compaction_bound_subscriber: Option<Box<dyn mz_catalog::durable::DurableCatalogState>>,
    pub timestamp_oracle_config: Option<TimestampOracleConfig>,
    /// Clock for timestamp allocation and policy checks, shared with catalog writers.
    pub timestamp_oracle_now: NowFn,
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
    txn: PendingTxn,
    /// The timestamp context of the transaction.
    timestamp_context: TimestampContext,
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
    pub fn timestamp_context(&self) -> &TimestampContext {
        &self.timestamp_context
    }

    pub(crate) fn take_context(self) -> ExecuteContext {
        self.txn.ctx
    }

    /// Alert the client that the read has been linearized.
    #[instrument(level = "debug")]
    pub fn finish(self) {
        let PendingTxn {
            mut ctx,
            response,
            action,
        } = self.txn;
        let changed = ctx.session_mut().vars_mut().end_transaction(action);
        let response = response.map(|mut r| {
            r.extend_params(changed);
            ExecuteResponse::from(r)
        });
        ctx.retire(response);
    }
}

/// State that the coordinator must process as part of retiring
/// command execution.  `ExecuteContextExtra::Default` is guaranteed
/// to produce a value that will cause the coordinator to do nothing, and
/// is intended for use by code that invokes the execution processing flow
/// (i.e., `sequence_plan`) without actually being a statement execution.
///
/// This is a pure data struct containing only the statement logging ID.
/// For auto-retire-on-drop behavior, use `ExecuteContextGuard` which wraps
/// this struct and owns the channel for sending retirement messages.
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
        self.statement_uuid.is_none()
    }
    pub fn contents(&self) -> Option<StatementLoggingId> {
        self.statement_uuid
    }
    /// Consume this extra and return the statement UUID for retirement.
    /// This should only be called from code that knows what to do to finish
    /// up logging based on the inner value.
    #[must_use]
    pub(crate) fn retire(self) -> Option<StatementLoggingId> {
        self.statement_uuid
    }
}

/// A guard that wraps `ExecuteContextExtra` and owns a channel for sending
/// retirement messages to the coordinator.
///
/// If this guard is dropped with a `Some` `statement_uuid` in its inner
/// `ExecuteContextExtra`, the `Drop` implementation will automatically send a
/// `Message::RetireExecute` to log the statement ending.
/// This handles cases like connection drops where the context cannot be
/// explicitly retired.
/// See <https://github.com/MaterializeInc/database-issues/issues/7304>
#[derive(Debug)]
#[must_use]
pub struct ExecuteContextGuard {
    extra: ExecuteContextExtra,
    /// Channel for sending messages to the coordinator. Used for auto-retiring on drop.
    /// For `Default` instances, this is a dummy sender (receiver already dropped), so
    /// sends will fail silently - which is the desired behavior since Default instances
    /// should only be used for non-logged statements.
    coordinator_tx: mpsc::UnboundedSender<Message>,
}

impl Default for ExecuteContextGuard {
    fn default() -> Self {
        // Create a dummy sender by immediately dropping the receiver.
        // Any send on this channel will fail silently, which is the desired
        // behavior for Default instances (non-logged statements).
        let (tx, _rx) = mpsc::unbounded_channel();
        Self {
            extra: ExecuteContextExtra::default(),
            coordinator_tx: tx,
        }
    }
}

impl ExecuteContextGuard {
    pub(crate) fn new(
        statement_uuid: Option<StatementLoggingId>,
        coordinator_tx: mpsc::UnboundedSender<Message>,
    ) -> Self {
        Self {
            extra: ExecuteContextExtra::new(statement_uuid),
            coordinator_tx,
        }
    }
    pub fn is_trivial(&self) -> bool {
        self.extra.is_trivial()
    }
    pub fn contents(&self) -> Option<StatementLoggingId> {
        self.extra.contents()
    }
    /// Take responsibility for the contents.  This should only be
    /// called from code that knows what to do to finish up logging
    /// based on the inner value.
    ///
    /// Returns the inner `ExecuteContextExtra`, consuming the guard without
    /// triggering the auto-retire behavior.
    pub(crate) fn defuse(mut self) -> ExecuteContextExtra {
        // Taking statement_uuid prevents the Drop impl from sending a retire message
        std::mem::take(&mut self.extra)
    }
}

impl Drop for ExecuteContextGuard {
    fn drop(&mut self) {
        if let Some(statement_uuid) = self.extra.statement_uuid.take() {
            // Auto-retire since the guard was dropped without explicit retirement (likely due
            // to connection drop).
            let msg = Message::RetireExecute {
                data: ExecuteContextExtra {
                    statement_uuid: Some(statement_uuid),
                },
                otel_ctx: OpenTelemetryContext::obtain(),
                reason: StatementEndedExecutionReason::Aborted,
            };
            // Send may fail for Default instances (dummy sender), which is fine since
            // Default instances should only be used for non-logged statements.
            let _ = self.coordinator_tx.send(msg);
        }
    }
}

/// Carries the session and statement state needed to retire an execution.
///
/// Dropping an unretired context fails the client synchronously. Shutdown can drop contexts from
/// task queues, where spawning response-barrier work is no longer safe.
#[derive(Debug)]
pub struct ExecuteContext {
    // `None` only after `retire`/`into_parts` consumed the context.
    inner: Option<Box<ExecuteContextInner>>,
}

impl std::ops::Deref for ExecuteContext {
    type Target = ExecuteContextInner;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("only consumed by value")
    }
}

impl std::ops::DerefMut for ExecuteContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().expect("only consumed by value")
    }
}

impl Drop for ExecuteContext {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        // Destructors cannot spawn response-barrier tasks during runtime shutdown. Send the error
        // synchronously and let the statement guard report retirement.
        tracing::warn!("execute context dropped without retirement, failing the client");
        let ExecuteContextInner { tx, session, .. } = *inner;
        tx.send(
            Err(AdapterError::Internal(
                "statement execution abandoned, outcome unknown (server shutting down)".into(),
            )),
            session,
        );
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ExecuteContextInner {
    tx: ClientTransmitter<ExecuteResponse>,
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    session: Session,
    extra: ExecuteContextGuard,
    /// Fixed when execution enters the coordinator, not renewed by diagnostic stages.
    statement_deadline: Option<Instant>,
    #[derivative(Debug = "ignore")]
    query_catalog: Option<(Arc<Catalog>, Option<Timestamp>)>,
    #[derivative(Debug = "ignore")]
    query_replan: Option<Arc<(Arc<Statement<Raw>>, Params)>>,
    query_portal: Option<String>,
    query_replanned: bool,
    subscribe_admitted: bool,
    #[derivative(Debug = "ignore")]
    response_barriers: Vec<BuiltinTableAppendNotify>,
}

impl ExecuteContext {
    pub(crate) fn admit_subscribe(&mut self) -> Result<(), AdapterError> {
        if !self.subscribe_admitted {
            self.session_mut()
                .add_transaction_ops(crate::session::TransactionOps::Subscribe)?;
            self.subscribe_admitted = true;
        }
        Ok(())
    }

    /// The immutable catalog captured for this execution's preplanning.
    /// Contexts that bypass statement preplanning need not have a snapshot.
    pub(crate) fn query_catalog(&self) -> Option<&Arc<Catalog>> {
        self.query_catalog.as_ref().map(|(catalog, _)| catalog)
    }

    /// The EpochMilliseconds certification timestamp, absent for frozen savepoints
    /// or contexts that bypass statement preplanning. This is not a data timestamp.
    pub(crate) fn query_catalog_timestamp(&self) -> Option<Timestamp> {
        self.query_catalog
            .as_ref()
            .and_then(|(_, timestamp)| *timestamp)
    }

    pub(crate) fn set_query_catalog(
        &mut self,
        catalog: Arc<Catalog>,
        timestamp: Option<Timestamp>,
    ) {
        self.query_catalog = Some((catalog, timestamp));
    }

    pub(crate) fn statement_deadline(&self) -> Option<Instant> {
        self.statement_deadline
    }

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
        extra: ExecuteContextGuard,
    ) -> Self {
        Self::from_parts_with_response_barriers(tx, internal_cmd_tx, session, extra, Vec::new())
    }

    pub fn from_parts_with_response_barriers(
        tx: ClientTransmitter<ExecuteResponse>,
        internal_cmd_tx: mpsc::UnboundedSender<Message>,
        session: Session,
        extra: ExecuteContextGuard,
        response_barriers: Vec<BuiltinTableAppendNotify>,
    ) -> Self {
        let timeout = *session.vars().statement_timeout();
        let statement_deadline = (!timeout.is_zero())
            .then_some(timeout)
            .and_then(|timeout| Instant::now().checked_add(timeout));
        Self {
            inner: Some(
                ExecuteContextInner {
                    tx,
                    session,
                    extra,
                    statement_deadline,
                    response_barriers,
                    internal_cmd_tx,
                    query_catalog: None,
                    query_replan: None,
                    query_portal: None,
                    query_replanned: false,
                    subscribe_admitted: false,
                }
                .into(),
            ),
        }
    }

    /// By calling this function, the caller takes responsibility for
    /// dealing with the instance of `ExecuteContextGuard`. This is
    /// intended to support protocols (like `COPY FROM`) that involve
    /// multiple passes of sending the session back and forth between
    /// the coordinator and the pgwire layer. As part of any such
    /// protocol, we must ensure that the `ExecuteContextGuard`
    /// (possibly wrapped in a new `ExecuteContext`) is passed back to the coordinator for
    /// eventual retirement. The returned response barriers must stay attached
    /// to the user-visible response path.
    /// Catalog certification is not part of these returned parts. A continuing
    /// execution must preserve it explicitly or reenter through catalog freshness.
    ///
    /// The returned parts lose the `Drop` backstop that answers the client on shutdown, so they
    /// must not be held across an await point. A bare `ClientTransmitter` panics when dropped
    /// unsent.
    pub fn into_parts(
        mut self,
    ) -> (
        ClientTransmitter<ExecuteResponse>,
        mpsc::UnboundedSender<Message>,
        Session,
        ExecuteContextGuard,
        Vec<BuiltinTableAppendNotify>,
    ) {
        let ExecuteContextInner {
            tx,
            internal_cmd_tx,
            session,
            extra,
            response_barriers,
            statement_deadline: _,
            query_catalog: _,
            query_replan: _,
            query_portal: _,
            query_replanned: _,
            subscribe_admitted: _,
        } = *self.inner.take().expect("only consumed by value");
        (tx, internal_cmd_tx, session, extra, response_barriers)
    }

    /// Retire the execution, by sending a message to the coordinator.
    #[instrument(level = "debug")]
    pub fn retire(mut self, result: Result<ExecuteResponse, AdapterError>) {
        let response_barriers = std::mem::take(&mut self.response_barriers);
        if response_barriers.is_empty() {
            let (tx, internal_cmd_tx, session, extra, _) = self.into_parts();
            retire_execution_context(tx, internal_cmd_tx, session, extra, result);
            return;
        }
        // Keep `self` intact across the wait: if shutdown drops this task, the context's `Drop`
        // backstop answers the client. Barriers are empty on re-entry, so this terminates.
        spawn(
            || "execute_context::retire_after_response_barriers",
            async move {
                for barrier in response_barriers {
                    barrier.await;
                }
                self.retire(result);
            },
        );
    }

    /// Delays sending this statement's response until `barrier` resolves.
    pub(crate) fn delay_response_until(&mut self, barrier: BuiltinTableAppendCompletion) {
        self.response_barriers.push(barrier.into_notify());
    }

    pub fn extra(&self) -> &ExecuteContextGuard {
        &self.extra
    }

    pub fn extra_mut(&mut self) -> &mut ExecuteContextGuard {
        &mut self.extra
    }
}

fn retire_execution_context(
    tx: ClientTransmitter<ExecuteResponse>,
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    session: Session,
    extra: ExecuteContextGuard,
    result: Result<ExecuteResponse, AdapterError>,
) {
    let reason = if extra.is_trivial() {
        None
    } else {
        Some((&result).into())
    };
    tx.send(result, session);
    if let Some(reason) = reason {
        let extra = extra.defuse();
        if let Err(e) = internal_cmd_tx.send(Message::RetireExecute {
            otel_ctx: OpenTelemetryContext::obtain(),
            data: extra,
            reason,
        }) {
            warn!("internal_cmd_rx dropped before we could send: {:?}", e);
        }
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
                    restart_count: 0,
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
    /// Adapter-owned table writes. Runtime operations use the group committer's FIFO.
    table_write_handle: Arc<dyn crate::table_writer::TableWriteHandle>,
    /// Adapter-owned webhook and statement-history writes and idle frontier advancement.
    adapter_storage: mz_controller::AdapterStorageWriter,
    /// Request-side connection configuration projected from the catalog and CLI.
    storage_configuration: mz_storage_types::configuration::StorageConfiguration,
    /// The catalog in an Arc suitable for readonly references. The Arc allows
    /// us to hand out cheap copies of the catalog to functions that can use it
    /// off of the main coordinator thread. If the coordinator needs to mutate
    /// the catalog, call [`Self::catalog_mut`], which will clone this struct member,
    /// allowing it to be mutated here while the other off-thread references can
    /// read their catalog as long as needed. In the future we would like this
    /// to be a pTVC, but for now this is sufficient.
    catalog: Arc<Catalog>,
    compaction_bound_subscriber: Option<CompactionBoundSubscriber>,
    /// Changed records retained until publication succeeds, including failed attempts.
    read_protection_pending: BTreeSet<GlobalId>,
    query_client: Option<Arc<crate::query_client::QueryClient>>,
    client_protection_catalog: Option<Catalog>,
    client_protection_reclaimer: crate::query_client::read_protection::ClientProtectionReclaimer,
    query_persist_location: mz_persist_types::PersistLocation,
    query_orchestrator: Arc<dyn mz_orchestrator::NamespacedOrchestrator>,
    query_deploy_generation: u64,

    /// A client for persist. Initially, this is only used for reading stashed
    /// peek responses out of batches.
    persist_client: PersistClient,

    /// Channel to manage internal commands from the coordinator to itself.
    internal_cmd_tx: mpsc::UnboundedSender<Message>,
    /// Notification that triggers a group commit.
    group_commit_tx: appends::GroupCommitNotifier,
    /// Wakes the cluster controller task to reconcile immediately instead of
    /// waiting out its tick interval. Notified after catalog transactions that
    /// change durable cluster state.
    reconcile_now: Arc<Notify>,
    group_committer_tx: mpsc::UnboundedSender<appends::TableWriteCmd>,

    /// Channel for strict serializable reads ready to commit.
    strict_serializable_reads_tx: mpsc::UnboundedSender<(ConnectionId, PendingReadTxn)>,

    /// Signals that pending strict serializable reads should be re-checked
    /// because the timestamp oracle may have advanced. Awaited below group commit
    /// in [`Coordinator::serve`]; see that branch for the ordering rationale.
    linearize_reads_notify: Arc<Notify>,

    /// Mechanism for totally ordering write and read timestamps, so that all reads
    /// reflect exactly the set of writes that precede them, and no writes that follow.
    global_timelines: BTreeMap<Timeline, TimelineState>,

    /// A generator for transient [`GlobalId`]s, shareable with other threads.
    transient_id_gen: Arc<TransientIdGen>,
    /// A map from connection ID to metadata about that connection for all
    /// active connections.
    active_conns: BTreeMap<ConnectionId, ConnMeta>,

    /// For each transaction, the read holds taken to support any performed reads.
    ///
    /// Upon completing a transaction, these read holds should be dropped.
    txn_read_holds: BTreeMap<ConnectionId, read_policy::ReadHolds>,

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

    /// Connection-scoped cancellation watches.
    ///
    /// Each entry is a watch channel whose value is `false` until cancellation
    /// is requested for that connection, at which point it is set to `true`.
    ///
    /// Consumers install these watches while they have cancellable work in
    /// flight, always as a fresh channel, so nobody can observe a cancellation
    /// aimed at an earlier statement. An entry is removed when a statement
    /// starts, when a stage runs uncancelable, and when the connection's state
    /// is cleared.
    connection_cancel_watches: BTreeMap<ConnectionId, (watch::Sender<bool>, watch::Receiver<bool>)>,
    /// Active introspection subscribes.
    introspection_subscribes: BTreeMap<GlobalId, IntrospectionSubscribe>,
    /// The last replica visited by the sequential hydration-history sweep.
    hydration_history_replica_cursor: Option<ReplicaId>,
    /// Hydration-history sweep owned by the coordinator while one is in flight.
    hydration_history_sweep: Option<AbortOnDropHandle<()>>,
    /// The curated metric sinks installed on each replica.
    ///
    /// Keyed replica-first so a replica's installs form one contiguous range: teardown on replica
    /// drop is the only lookup that is not by exact key.
    metric_sinks: BTreeMap<(ReplicaId, &'static str), InstalledMetricSink>,
    /// Curated metric-sink plans, cached per definition so each is planned once rather than once
    /// per replica. See [`Coordinator::plan_metric_sink`].
    metric_sink_plans: BTreeMap<&'static str, PlannedMetricSink>,

    /// Committed maintained exports waiting for selected plans or local imports.
    /// Publication and reclamation cannot overtake their installation.
    pending_compute_installations: BTreeSet<GlobalId>,
    /// Next installation attempt and its backoff, reset by new relevant catalog state.
    pending_compute_installation_retry: Option<(Instant, Duration)>,

    /// Plans waiting for session-startup builtin table appends.
    deferred_plans: BTreeMap<ConnectionId, DeferredPlan>,

    /// Pending writes waiting for a group commit.
    pending_writes: Vec<PendingWriteTxn>,

    /// Semaphore to limit concurrent OCC (optimistic concurrency control)
    /// read-then-write operations.
    ///
    /// Each operation maintains a subscribe that continually receives and
    /// consolidates updates. With N concurrent loops, every successful write
    /// forces the other N-1 to redo work, so total work scales as `O(n^2)`.
    /// The semaphore caps concurrency to keep that bounded.
    ///
    /// NOTE: The number of permits is read from `max_concurrent_occ_writes` at
    /// coordinator startup. Runtime changes require an `environmentd` restart.
    occ_write_semaphore: Arc<Semaphore>,

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

    /// Last reported native index observations, never execution capabilities.
    native_frontiers: introspection::frontiers::NativeFrontiers,

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

    /// Optional config for the timestamp oracle. This is _required_ when
    /// a timestamp oracle backend is configured.
    timestamp_oracle_config: Option<TimestampOracleConfig>,
    /// Clock for timestamp allocation and policy checks, not other adapter consumers.
    timestamp_oracle_now: NowFn,

    /// Context needed to check whether all clusters/collections have caught up.
    /// Only present during 0dt deployment, while in read-only mode, and taken
    /// by [`Coordinator::spawn_caught_up_check_task`].
    caught_up_check: Option<CaughtUpCheckContext>,

    /// The metrics registry, handed to the catalog info-metrics background task
    /// so it can register and own its `*_info` series.
    catalog_info_metrics_registry: MetricsRegistry,

    /// The shared system-parameter frontend, installed by the sync loop once it
    /// initializes (and re-installed on reconnect). `None` until then, for
    /// example before LaunchDarkly connects, where a newly-created object
    /// resolves to the environment-wide value (the cold-cache fallback). Used to
    /// resolve a new cluster's or replica's scoped overrides synchronously at
    /// create time, so its first plan or first controller configuration is
    /// correct rather than waiting for the next sync tick. See the scoped
    /// feature flags design.
    scoped_frontend: Option<Arc<SystemParameterFrontend>>,

    /// Tracks the state associated with the currently installed watchsets.
    installed_watch_sets: BTreeMap<WatchSetId, InstalledWatchSet>,
    query_watch_set_ids: mz_ore::id_gen::Gen<WatchSetId>,

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

    /// Pre-allocated pool of user IDs to amortize persist writes across DDL operations.
    user_id_pool: IdPool,
}

impl Coordinator {
    /// Persists the scoped system-parameter working copy and reconciles it into
    /// the per-scope resolution boundaries.
    ///
    /// The system-parameter sync loop and the create-time fold
    /// (`scoped_overrides_create_op`, folded into the create transaction) are the
    /// only writers, both serialized on the coordinator loop. The diff is
    /// persisted to the
    /// durable cache (so values survive an `environmentd` restart and an LD
    /// outage) via `Op::UpdateScopedSystemParameters`, which also updates the
    /// in-memory working copy in [`CatalogState`] and the
    /// `mz_cluster_system_parameters` / `mz_replica_system_parameters`
    /// introspection relations. The `replica`-scoped overrides reach the compute
    /// controller's per-replica dyncfg layer through the catalog implication for
    /// the persisted change. The `cluster`-scoped layer is resolved at plan time
    /// via [`CatalogState::cluster_scoped_optimizer_overrides`].
    ///
    /// [`CatalogState`]: crate::catalog::CatalogState
    /// [`CatalogState::cluster_scoped_optimizer_overrides`]: crate::catalog::CatalogState::cluster_scoped_optimizer_overrides
    pub(crate) async fn reconcile_scoped_system_parameters(
        &mut self,
        scoped: ScopedParameters,
        prune_scope: ScopedParametersScope,
    ) {
        // Nothing changed: skip the durable write. This is the common case on
        // most sync ticks.
        if self.catalog().state().scoped_system_parameters() == &scoped {
            return;
        }

        // Persist the diff and update the in-memory working copy + introspection
        // through the catalog transaction, serialized on the coordinator loop
        // with the create-time fold. The replica-scoped
        // controller push is derived from this transaction's diff by the catalog
        // implication. `prune_scope` bounds removals to the evaluated objects, so
        // a concurrently-created object's override is not wiped. Best-effort: a
        // failure here is logged and retried on the next sync tick.
        if let Err(e) = self
            .catalog_transact(
                None,
                vec![crate::catalog::Op::UpdateScopedSystemParameters {
                    scoped,
                    prune_scope,
                }],
            )
            .await
        {
            tracing::warn!("failed to persist scoped system parameters: {e}");
        }
    }

    /// Evaluates scoped overrides for objects created by `ops` and returns an
    /// [`Op::UpdateScopedSystemParameters`] to fold into the same transaction.
    ///
    /// The objects are not yet in the catalog, so this derives their contexts
    /// from concrete create ops and pre-allocated ids. Centralizing the fold
    /// here makes create-time configuration an invariant of coordinator-applied
    /// catalog ops, independent of which component produced them. The committed
    /// diff drives the replica-scoped controller push before `create_replica`.
    /// Render-frozen flags make a later push too late.
    ///
    /// Returns `None` when no scoped object is created or the shared frontend is
    /// not yet installed. An installed frontend produces an op even when no
    /// override applies, so a final DDL-transaction evaluation can clear a value
    /// staged by an earlier statement. The periodic sync loop remains the
    /// authoritative full-state reconciler.
    ///
    /// [`Op::UpdateScopedSystemParameters`]: crate::catalog::Op::UpdateScopedSystemParameters
    fn scoped_overrides_create_op(&self, ops: &[crate::catalog::Op]) -> Option<crate::catalog::Op> {
        let mut created_clusters = BTreeMap::new();
        let mut clusters = Vec::new();
        for op in ops {
            let crate::catalog::Op::CreateCluster { id, name, .. } = op else {
                continue;
            };
            let cluster = ClusterScopeContext {
                id: id.to_string(),
                name: name.clone(),
                is_builtin: id.is_system(),
            };
            created_clusters.insert(*id, cluster.clone());
            clusters.push(ClusterEvalContext {
                cluster_id: *id,
                cluster,
            });
        }

        let mut replicas = Vec::new();
        for op in ops {
            let (crate::catalog::Op::CreateClusterReplica {
                cluster_id,
                replica_id,
                name,
                config,
                ..
            }
            | crate::catalog::Op::CreateClusterReplicaRealization {
                cluster_id,
                replica_id,
                name,
                config,
                ..
            }) = op
            else {
                continue;
            };
            let ReplicaLocation::Managed(location) = &config.location else {
                continue;
            };
            let Some(cluster) = created_clusters.get(cluster_id).cloned().or_else(|| {
                self.catalog()
                    .try_get_cluster(*cluster_id)
                    .map(|cluster| ClusterScopeContext {
                        id: cluster_id.to_string(),
                        name: cluster.name.clone(),
                        is_builtin: cluster_id.is_system(),
                    })
            }) else {
                continue;
            };
            replicas.push(ReplicaEvalContext {
                cluster_id: *cluster_id,
                replica_id: *replica_id,
                replica: ReplicaScopeContext {
                    id: replica_id.to_string(),
                    name: name.clone(),
                    is_builtin: cluster_id.is_system(),
                    size: location.size.clone(),
                    size_family: location.allocation.family().to_string(),
                    cluster_id: cluster_id.to_string(),
                    cluster_name: cluster.name.clone(),
                },
                cluster,
            });
        }

        if clusters.is_empty() && replicas.is_empty() {
            return None;
        }
        let frontend = self.scoped_frontend.clone()?;
        let catalog = self.catalog();
        let system_config = catalog.system_config();

        // Partition the synced parameters by scope class, as the sync loop does,
        // so we evaluate exactly the flags in use at each scope.
        let replica_param_names: Vec<&'static str> = system_config
            .iter_synced()
            .filter(|var| var.scope() == ParameterScope::Replica)
            .map(|var| var.name())
            .collect();
        let cluster_param_names: Vec<&'static str> = system_config
            .iter_synced()
            .filter(|var| var.scope() == ParameterScope::Cluster)
            .map(|var| var.name())
            .collect();

        let params = SynchronizedParameters::new(system_config.clone());
        let mut evaluated = ScopedParameters::default();
        if !cluster_param_names.is_empty() && !clusters.is_empty() {
            evaluated.cluster =
                frontend.pull_cluster_overrides(&params, &cluster_param_names, &clusters);
        }
        if !replica_param_names.is_empty() && !replicas.is_empty() {
            evaluated.replica =
                frontend.pull_replica_overrides(&params, &replica_param_names, &replicas);
        }
        // Prune only within the objects this transaction creates. A later
        // statement in a DDL transaction can replace an earlier folded value,
        // but this never touches an unrelated object's override.
        let prune_scope = ScopedParametersScope {
            clusters: clusters.iter().map(|cluster| cluster.cluster_id).collect(),
            replicas: replicas.iter().map(|replica| replica.replica_id).collect(),
        };
        Some(crate::catalog::Op::UpdateScopedSystemParameters {
            scoped: evaluated,
            prune_scope,
        })
    }

    /// Renders the replica-local scoped overrides in the catalog working copy as
    /// per-replica [`ConfigUpdates`], grouped by cluster.
    ///
    /// Sparse: only replicas with an override are present. Parameters that are
    /// not dyncfgs are skipped, as are values that fail to parse.
    pub(crate) fn replica_dyncfg_overrides(
        &self,
    ) -> BTreeMap<ComputeInstanceId, BTreeMap<ReplicaId, ConfigUpdates>> {
        mz_catalog::compute_config::replica_dyncfg_overrides(self.catalog())
    }

    /// Resolves the replica-local scoped overrides from the catalog working copy
    /// into the controllers' per-replica dyncfg layers, then re-pushes the
    /// environment-wide configuration so replicas observe the new values.
    /// Driven by the catalog implication for replica-scoped configuration
    /// changes, and called once on bootstrap.
    pub(crate) fn push_replica_dyncfg_overrides(&mut self) {
        let instance_overrides = self.replica_dyncfg_overrides();

        // Both controllers carry a per-replica dyncfg layer, because the two
        // protocols realize configs in different worker `ConfigSet`s on
        // `clusterd`. The compute worker's `handle_update_configuration`
        // applies the pushed dyncfg updates to compute's own worker
        // `ConfigSet`, to the shared persist client `ConfigSet`
        // (`persist_clients.cfg()`) that the co-located storage server reads
        // from the same `Arc`, and to `mz_metrics`, which covers
        // persist-backed and process-global configs such as persist client
        // tuning and `lgalloc`. Configs realized from the storage worker's own
        // `ConfigSet` (read in its `UpdateConfiguration` handler) are reached
        // only by the storage controller's layer. A third class is not pushed
        // to a running replica at all but baked into its process configuration
        // when the controller provisions it, which is why the overrides also go
        // to the outer controller.
        self.controller
            .update_replica_dyncfg_overrides(instance_overrides);
        // Re-push the env-wide configs so existing replicas pick up their
        // (possibly changed) overrides. This also reverts a removed override:
        // the per-replica layer no longer carries the key, so the replica
        // falls back to the env-wide value, which both configs always include
        // because they render the full dyncfg set.
        let compute_config = crate::flags::compute_config(self.catalog().system_config());
        self.controller.compute.update_configuration(compute_config);
        let storage_config = crate::flags::storage_config(self.catalog().system_config());
        self.controller.storage.update_parameters(storage_config);
    }

    /// Returns the cluster-coherent scoped optimizer-feature overrides for
    /// `cluster_id`. See
    /// [`CatalogState::cluster_scoped_optimizer_overrides`](crate::catalog::CatalogState::cluster_scoped_optimizer_overrides).
    pub(crate) fn cluster_scoped_optimizer_overrides(
        &self,
        cluster_id: ClusterId,
    ) -> OptimizerFeatureOverrides {
        self.catalog()
            .state()
            .cluster_scoped_optimizer_overrides(cluster_id)
    }

    /// Commits bootstrap catalog changes, retaining refreshed rows for the
    /// initial system-table reset instead of sending them to the live writer.
    async fn bootstrap_catalog_transact(
        &mut self,
        ops: Vec<crate::catalog::Op>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
    ) -> Result<Vec<u64>, AdapterError> {
        let revision = self.catalog().transient_revision();
        loop {
            let write_ts = self.get_catalog_write_ts().await;
            match self
                .catalog_mut()
                .transact(None, write_ts, None, ops.clone())
                .await
            {
                Ok(result) => {
                    builtin_table_updates.extend(result.builtin_table_updates);
                    return Ok(result.created_client_incarnations);
                }
                Err(error) => {
                    self.refresh_bootstrap_catalog_after_conflict(
                        error,
                        revision,
                        builtin_table_updates,
                    )
                    .await?;
                }
            }
        }
    }

    /// Refreshes a stale bootstrap prefix without accepting a changed planning
    /// context. Preview and commit both require this check before retrying.
    async fn refresh_bootstrap_catalog_after_conflict(
        &mut self,
        error: AdapterError,
        revision: u64,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
    ) -> Result<(), AdapterError> {
        if !matches!(&error,
        AdapterError::Catalog(error) if matches!(&error.kind,
            mz_catalog::memory::error::ErrorKind::Durable(
                mz_catalog::durable::DurableCatalogError::CatalogOutOfSync { .. }
            )))
        {
            return Err(error);
        }
        info!(%error, "refreshing bootstrap selections after catalog contention");
        let (builtin, updates) = self.catalog_mut().sync_to_current_updates().await?;
        builtin_table_updates.extend(
            self.catalog()
                .state()
                .resolve_builtin_table_updates(builtin),
        );
        if self.catalog().transient_revision() != revision {
            return Err(error);
        }
        Box::pin(self.apply_catalog_implications(None, updates)).await
    }

    /// Initializes coordinator state based on the contained catalog. Must be
    /// called after creating the coordinator and before calling the
    /// `Coordinator::serve` method.
    #[instrument(name = "coord::bootstrap")]
    pub(crate) async fn bootstrap(
        &mut self,
        boot_ts: Timestamp,
        migrated_storage_collections_0dt: BTreeSet<CatalogItemId>,
        hydrate_migrated_mvs: bool,
        mut builtin_table_updates: Vec<BuiltinTableUpdate>,
        cached_global_exprs: BTreeMap<GlobalId, GlobalExpressions>,
        uncached_local_exprs: BTreeMap<GlobalId, LocalExpressions>,
    ) -> Result<Option<Arc<crate::query_client::QueryClient>>, AdapterError> {
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

        // Install the replica-local scoped overrides before creating any
        // replica below. Parts of a replica's configuration (its `TimelyConfig`,
        // its expiration offset) are resolved once, when the controller
        // provisions the replica, and must see its overrides at that point. The
        // push after the creation loop cannot serve this purpose, because those
        // values are frozen by then.
        let replica_dyncfg_overrides = self.replica_dyncfg_overrides();
        self.controller
            .update_replica_dyncfg_overrides(replica_dyncfg_overrides);

        // Skip the credit consumption check at bootstrap under DisableClusterCreation behavior:
        // this codepath validates existing replicas at startup, not cluster creation, so it
        // must not block startup. New cluster creation is still gated by the DDL-time check.
        // The Disable case is already handled by a bail! in main.rs before we reach here.
        let enforce_credit_limit_at_bootstrap = !matches!(
            self.license_key.expiration_behavior,
            ExpirationBehavior::DisableClusterCreation,
        );
        if enforce_credit_limit_at_bootstrap {
            self.validate_resource_limit_numeric(
                Numeric::zero(),
                self.current_credit_consumption_rate(None),
                |system_vars| {
                    self.license_key
                        .max_credit_consumption_rate()
                        .map_or_else(|| system_vars.max_credit_consumption_rate(), Numeric::from)
                },
                "cluster replica",
                MAX_CREDIT_CONSUMPTION_RATE.name(),
            )?;
        }

        let mut policies_to_set: BTreeMap<CompactionWindow, CollectionIdBundle> =
            Default::default();

        let enable_worker_core_affinity =
            self.catalog().system_config().enable_worker_core_affinity();
        self.restore_compute_read_protection().await?;
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

        // Now that the compute instances and their replicas exist, push the
        // replica-local scoped overrides into the controllers so existing
        // replicas observe them at startup. The scoped (per-cluster and
        // per-replica) working copy was restored from the durable cache into
        // `CatalogState` while opening the catalog, so the last-known values are
        // in effect before the first parameter sync and through a sync outage.
        // This must run after the creation loop above: the push iterates the
        // controller's instances, so before they exist it is a no-op. It also
        // runs before dataflows are rendered later in bootstrap, so render-frozen
        // replica flags take effect. The cluster-coherent layer is read at plan
        // time.
        self.push_replica_dyncfg_overrides();

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

        let sorting_start = Instant::now();
        info!("startup: coordinator init: bootstrap: sorting catalog entries");
        let entries = self.bootstrap_sort_catalog_entries();
        info!(
            "startup: coordinator init: bootstrap: sorting catalog entries complete in {:?}",
            sorting_start.elapsed()
        );

        let optimize_dataflows_start = Instant::now();
        info!("startup: coordinator init: bootstrap: optimize dataflow plans beginning");
        let protected_plans = self.catalog().state().catalog_read_protection_enabled();
        let write_plans = protected_plans
            && (!self.read_only_controllers || self.controller.replica_owned_compute());
        let mut index_timeline_holds = ReadHolds::new();
        // Keep construction separate from activation. Bootstrap read policies
        // still use their initialization holds until the late client handoff.
        let bootstrap_client = if write_plans {
            let incarnations = self
                .bootstrap_catalog_transact(
                    vec![crate::catalog::Op::CreateClientIncarnation { replica_id: None }],
                    &mut builtin_table_updates,
                )
                .await?;
            let incarnation = incarnations
                .into_iter()
                .next()
                .expect("created bootstrap client");
            Some(self.build_query_client(incarnation).await?)
        } else {
            None
        };
        if self.controller.replica_owned_compute()
            && let Some(client) = &bootstrap_client
        {
            let indexes: BTreeSet<_> = entries
                .iter()
                .filter_map(|entry| match entry.item() {
                    CatalogItem::Index(index)
                        if self
                            .catalog()
                            .state()
                            .collection_compaction_bounds()
                            .contains_key(&index.global_id()) =>
                    {
                        Some(index.global_id())
                    }
                    _ => None,
                })
                .collect();
            if !indexes.is_empty() {
                // Reconstruction can outlive the previous adapter's lease. Own
                // the serving window throughout bootstrap, including plan reuse.
                let state = self.catalog().state().clone();
                let publication = self
                    .prepare_index_timeline_publication(Arc::clone(client), &state, indexes)
                    .await?;
                let result = self
                    .bootstrap_catalog_transact(vec![publication.op()], &mut builtin_table_updates)
                    .await;
                index_timeline_holds.extend(publication.finish(result.is_ok()));
                result?;
            }
        }
        let selections = Box::pin(self.bootstrap_replica_metric_sink_selections()).await?;
        if !selections.is_empty() {
            self.bootstrap_catalog_transact(selections, &mut builtin_table_updates)
                .await?;
        }
        let mut candidates = cached_global_exprs;
        let mut written_ids = BTreeSet::new();
        if protected_plans {
            let build =
                Catalog::expression_build_version(self.catalog().config().build_info).to_string();
            let revisions: Vec<_> = self
                .catalog()
                .state()
                .written_plans()
                .iter()
                .filter(|((id, version), _)| {
                    version == &build && self.catalog().try_get_entry_by_global_id(id).is_some()
                })
                .map(|((id, _), selection)| (*id, selection.revision))
                .collect();
            let written = self.catalog().read_written_plans(revisions.clone()).await?;
            if written.len() != revisions.len() {
                return Err(AdapterError::internal(
                    "bootstrap written plans",
                    "selected plan is missing",
                ));
            }
            written_ids.extend(written.iter().filter_map(|(id, plan)| {
                (!write_plans
                    || plan
                        .collection_imports()
                        .all(|input| self.catalog().try_get_entry_by_global_id(input).is_some()))
                .then_some(*id)
            }));
            candidates.extend(written);
        }
        let mut prepared = if write_plans {
            candidates.clone()
        } else {
            BTreeMap::new()
        };
        let planning_revision = self.catalog().transient_revision();
        let mut plan_holds = Vec::new();
        let uncached_global_exps = self
            .bootstrap_dataflow_plans(
                &entries,
                candidates,
                &written_ids,
                bootstrap_client.as_ref(),
                &mut builtin_table_updates,
                &mut plan_holds,
            )
            .await?;
        if write_plans {
            prepared.extend(uncached_global_exps.clone());
            prepared.retain(|id, _| {
                !written_ids.contains(id)
                    && self.catalog().try_get_physical_plan(id).is_some()
                    && self.catalog().try_get_entry_by_global_id(id).is_some_and(
                        |entry| match entry.item() {
                            CatalogItem::Index(index) => index.global_id() == *id,
                            CatalogItem::MaterializedView(mv) => mv.global_id_writes() == *id,
                            CatalogItem::MetricSink(sink) => sink.global_id == *id,
                            _ => false,
                        },
                    )
            });
            if !prepared.is_empty() {
                let mut selections = self.catalog().write_plans(prepared).await?;
                self.check_bootstrap_planning_revision(planning_revision)?;
                let client = bootstrap_client
                    .as_ref()
                    .expect("writable protected bootstrap");
                // Selection and issuer liveness must be checked atomically. The
                // aggregate includes every temporary plan-import hold.
                let publication = if self.controller.replica_owned_compute() {
                    let indexes = selections
                        .iter()
                        .filter_map(|op| match op {
                            crate::catalog::Op::SetWrittenPlan { id, .. }
                                if !self
                                    .catalog()
                                    .state()
                                    .collection_compaction_bounds()
                                    .contains_key(id)
                                    && self
                                        .catalog()
                                        .try_get_entry_by_global_id(id)
                                        .is_some_and(|entry| {
                                            matches!(entry.item(), CatalogItem::Index(_))
                                        }) =>
                            {
                                Some(*id)
                            }
                            _ => None,
                        })
                        .collect();
                    // The preview opens a durable transaction too. A publisher
                    // can invalidate its prefix without invalidating the held
                    // plans, so rebuild the preview after the same checked
                    // refresh used by the commit path.
                    let (candidate, _) = loop {
                        let write_ts = self.get_catalog_write_ts().await;
                        let result = self
                            .catalog()
                            .transact_incremental_dry_run(
                                self.catalog().state(),
                                selections.clone(),
                                None,
                                None,
                                write_ts,
                            )
                            .await;
                        match result {
                            Ok(candidate) => break candidate,
                            Err(error) => {
                                self.refresh_bootstrap_catalog_after_conflict(
                                    error,
                                    planning_revision,
                                    &mut builtin_table_updates,
                                )
                                .await?;
                            }
                        }
                    };
                    let publication = self
                        .prepare_index_timeline_publication(Arc::clone(client), &candidate, indexes)
                        .await?;
                    selections.push(publication.op());
                    Some(publication)
                } else {
                    let requirements = client.protection.prepare_publication(BTreeMap::new());
                    selections.push(crate::catalog::Op::PublishClientReadRequirements {
                        incarnation: client.protection.incarnation(),
                        requirements,
                    });
                    None
                };
                let result = self
                    .bootstrap_catalog_transact(selections, &mut builtin_table_updates)
                    .await;
                if let Some(publication) = publication {
                    index_timeline_holds.extend(publication.finish(result.is_ok()));
                } else {
                    client.protection.finish_publication(result.is_ok());
                }
                if !self
                    .catalog()
                    .state()
                    .client_incarnations()
                    .contains_key(&client.protection.incarnation())
                {
                    client.protection.mark_closed();
                }
                result?;
                client.published();
            }
        }
        drop(plan_holds);
        self.publish_bootstrap_read_protection(
            bootstrap_client.as_ref(),
            &mut builtin_table_updates,
        )
        .await?;
        info!(
            "startup: coordinator init: bootstrap: optimize dataflow plans complete in {:?}",
            optimize_dataflows_start.elapsed()
        );

        // We don't need to wait for the cache to update.
        let _fut = self.catalog().update_expression_cache(
            uncached_local_exprs.into_iter().collect(),
            uncached_global_exps.into_iter().collect(),
            Default::default(),
        );

        // Select dataflow as-ofs. This step relies on the storage collections created by
        // `bootstrap_storage_collections` and the dataflow plans created by
        // `bootstrap_dataflow_plans`.
        let bootstrap_as_ofs_start = Instant::now();
        info!("startup: coordinator init: bootstrap: dataflow as-of bootstrapping beginning");
        let dataflow_read_holds = if self.controller.replica_owned_compute() {
            BTreeMap::new()
        } else {
            self.bootstrap_dataflow_as_ofs().await?
        };
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
            self.publish_bootstrap_read_protection(
                bootstrap_client.as_ref(),
                &mut builtin_table_updates,
            )
            .await?;
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

                        if !self.controller.replica_owned_compute() {
                            self.controller
                                .compute
                                .create_dataflow(idx.cluster_id, df_desc, None)
                                .unwrap_or_terminate("cannot fail to create dataflows");
                        }
                    }
                }
                CatalogItem::View(_) => (),
                CatalogItem::MaterializedView(mview) => {
                    // Each version receives a read policy when it is created. Bootstrap
                    // must restore every policy because the oldest version owns the shared
                    // Persist shard and capability changes reach it through each newer
                    // version's primary link. A `NoPolicy` version would block that
                    // propagation and pin compaction.
                    policies_to_set
                        .entry(policy.expect("materialized views have a compaction window"))
                        .or_insert_with(Default::default)
                        .storage_ids
                        .extend(mview.global_ids());

                    let mut df_desc = self
                        .catalog()
                        .try_get_physical_plan(&mview.global_id_writes())
                        .expect("added in `bootstrap_dataflow_plans`")
                        .clone();

                    mview.apply_execution_bounds(&mut df_desc);

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

                    if !self.controller.replica_owned_compute() {
                        let target = self
                            .materialized_view_physical_target(
                                mview.cluster_id,
                                mview.target_replica,
                            )
                            .unwrap_or_terminate("dataflow target unavailable");
                        self.ship_dataflow(df_desc, mview.cluster_id, target).await;
                    }

                    // A pending `REPLACEMENT FOR` MV must stay read-only until
                    // `ALTER ... APPLY REPLACEMENT` swaps it in. Unrelated to the
                    // builtin-migration `Replacement` mechanism below.
                    if mview.replacement_target.is_none()
                        && !self.controller.replica_owned_compute()
                    {
                        let gid = mview.global_id_writes();
                        if hydrate_migrated_mvs
                            && migrated_storage_collections_0dt.contains(&entry.id())
                        {
                            // `migrated_storage_collections_0dt` is `Replacement`-migrated items
                            // only, so this is a fresh shard we own: nothing else writes it, and
                            // writing it while read-only hydrates the MV and its dependents before
                            // cut-over. An `Evolution`-migrated MV reuses the leader's live shard
                            // and must never reach here.
                            //
                            // A *new* builtin MV gets no such treatment: its shard allocation
                            // lives only in this read-only savepoint, so the promoted leader
                            // allocates a different shard and discards whatever we wrote.
                            self.controller
                                .compute
                                .allow_writes_in_read_only(mview.cluster_id, gid)
                                .unwrap_or_terminate("allow_writes cannot fail");
                        } else {
                            self.allow_writes(mview.cluster_id, gid);
                        }
                    }
                }
                CatalogItem::MetricSink(metric_sink) => {
                    let df_desc = self
                        .catalog()
                        .try_get_physical_plan(&metric_sink.global_id)
                        .expect("added in `bootstrap_dataflow_plans`")
                        .clone();

                    let df_meta = self
                        .catalog()
                        .try_get_dataflow_metainfo(&metric_sink.global_id)
                        .expect("added in `bootstrap_dataflow_plans`");

                    if self.catalog().state().system_config().enable_mz_notices() {
                        // Collect optimization hint updates.
                        self.catalog().state().pack_optimizer_notices(
                            &mut builtin_table_updates,
                            df_meta.optimizer_notices.iter(),
                            Diff::ONE,
                        );
                    }

                    // No read policy to set: the export is a sink, not a readable collection, so
                    // `ship_dataflow` has no index export to initialize a policy for.
                    if !self.controller.replica_owned_compute() {
                        self.ship_dataflow(df_desc, metric_sink.cluster_id, None)
                            .await;
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
        self.adopt_index_timeline_holds(index_timeline_holds);

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
                    assert!(
                        gid.is_system() && migrated_storage_collections_0dt.contains(&update.id)
                    );
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

                let fut = self.table_write_handle.append(
                    min_timestamp,
                    boot_ts.step_forward(),
                    all_appends,
                );
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

        self.publish_bootstrap_read_protection(
            bootstrap_client.as_ref(),
            &mut builtin_table_updates,
        )
        .await?;
        if self.controller.read_only() {
            info!(
                "coordinator init: bootstrap: stashing builtin table updates while in read-only mode"
            );

            self.buffered_builtin_table_updates
                .as_mut()
                .expect("in read-only mode")
                .append(&mut builtin_table_updates);
        } else {
            self.bootstrap_tables(&entries, builtin_table_updates).await;
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

        if !self.controller.replica_owned_compute() {
            self.bootstrap_introspection_subscribes().await;
            self.bootstrap_metric_sinks().await;
        }

        info!(
            "startup: coordinator init: bootstrap: migrate builtin tables in read-only mode complete in {:?}",
            final_steps_start.elapsed()
        );

        info!(
            "startup: coordinator init: bootstrap complete in {:?}",
            bootstrap_start.elapsed()
        );
        Ok(bootstrap_client)
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
        // Append the tables in the background. Snapshots at the fence timestamp
        // block until the WAL passes that timestamp.
        let table_fence_rx = self
            .table_write_handle
            .append(write_ts.clone(), advance_to, appends);

        self.apply_local_write(write_ts).await;

        // Add builtin table updates the clear the contents of all system tables
        debug!("coordinator init: resetting system tables");
        // Catalog publishers can advance the shared oracle without advancing the
        // table WAL. Reading that newer time here could wait for table progress
        // that only starts after bootstrap finishes.
        let read_ts = write_ts;

        let retained_across_restarts = BTreeSet::from([
            self.catalog()
                .resolve_builtin_table(&MZ_STORAGE_USAGE_BY_SHARD),
            self.catalog()
                .resolve_builtin_table(&MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY),
            self.catalog()
                .resolve_builtin_table(&MZ_OBJECT_HYDRATION_HISTORY),
            self.catalog()
                .resolve_builtin_table(&MZ_REPLICA_HYDRATION_HISTORY),
        ]);

        let mut retraction_tasks = Vec::new();
        let system_tables: Vec<_> = table_metas
            .iter()
            .filter(|meta| meta.id.is_system() && !retained_across_restarts.contains(&meta.id))
            .collect();
        let txns_shard = self
            .catalog()
            .txn_wal_shard()
            .await
            .unwrap_or_terminate("table writer has committed WAL metadata");
        let reader = mz_storage_client::collection_reader::CollectionReader::new(
            self.persist_client.clone(),
            mz_txn_wal::txn_read::TxnsRead::start::<mz_storage_types::controller::TxnsCodecRow>(
                self.persist_client.clone(),
                txns_shard,
            )
            .await,
        );

        for system_table in system_tables {
            let table_id = system_table.id;
            let full_name = self.catalog().resolve_full_name(system_table.name, None);
            debug!("coordinator init: resetting system table {full_name} ({table_id})");

            // Fetch the current contents of the table for retraction.
            let global_id = system_table.table.global_id_writes();
            let metadata = mz_storage_types::controller::CollectionMetadata {
                persist_location: self.query_persist_location.clone(),
                data_shard: self
                    .catalog()
                    .state()
                    .storage_metadata()
                    .get_collection_shard(global_id)
                    .expect("system table has committed shard metadata"),
                relation_desc: system_table.table.desc.latest(),
                txns_shard: Some(txns_shard),
            };
            let snapshot_fut = reader.snapshot_cursor(global_id, metadata.clone(), read_ts);
            let persist = self.persist_client.clone();

            let task = spawn(|| format!("snapshot-{table_id}"), async move {
                use mz_storage_types::{StorageDiff, sources::SourceData};
                let writer = persist
                    .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                        metadata.data_shard,
                        Arc::new(metadata.relation_desc),
                        Arc::new(mz_persist_types::codec_impls::UnitSchema),
                        mz_persist_client::Diagnostics {
                            shard_name: global_id.to_string(),
                            handle_purpose: "system table reset batch".into(),
                        },
                    )
                    .await
                    .expect("system table schema matches committed description");
                let mut batch = mz_storage_client::client::TimestamplessUpdateBuilder::new(&writer);
                tracing::info!(?table_id, "starting snapshot");
                // Get a cursor which will emit a consolidated snapshot.
                let mut snapshot_cursor = snapshot_fut
                    .await
                    .unwrap_or_terminate("cannot fail to snapshot");

                // Retract the current contents, spilling into our builder.
                while let Some(values) = snapshot_cursor.next().await {
                    for (key, _t, d) in values {
                        let d_invert = d.neg();
                        batch.add(&key, &(), &d_invert).await;
                    }
                }
                tracing::info!(?table_id, "finished snapshot");

                let batch = batch.finish().await;
                writer.expire().await;
                BuiltinTableUpdate::batch(table_id, batch)
            });
            retraction_tasks.push(task);
        }

        let retractions_res = futures::future::join_all(retraction_tasks).await;
        for retractions in retractions_res {
            builtin_table_updates.push(retractions);
        }

        // Snapshot completion establishes WAL progress past the fence timestamp.
        // Check that our fence append itself succeeded before resetting the tables.
        table_fence_rx
            .await
            .expect("One-shot shouldn't be dropped during bootstrap")
            .unwrap_or_terminate("cannot fail to append");

        info!("coordinator init: sending builtin table updates");
        let builtin_updates_fut = self.builtin_table_update().execute(builtin_table_updates);
        // Wait for the committer to apply the write, so the builtin tables are readable before
        // we start serving. The committer allocates the timestamp and advances the oracle.
        builtin_updates_fut.await;
    }

    /// Initializes all storage collections required by catalog objects in the storage controller.
    ///
    /// This method takes care of collection creation, as well as migration of existing
    /// collections.
    ///
    /// Registers shared-shard aliases together, with prerequisites installed first wherever
    /// possible. Subsequent bootstrap logic can fetch metadata of arbitrary storage collections.
    ///
    /// `migrated_storage_collections` is a set of builtin storage collections that have been
    /// migrated and should be handled specially.
    #[instrument]
    async fn bootstrap_storage_collections(
        &mut self,
        migrated_storage_collections: &BTreeSet<CatalogItemId>,
    ) {
        let catalog = self.catalog();

        let source_desc = |object_id: GlobalId,
                           data_source: &DataSourceDesc,
                           desc: &RelationDesc,
                           timeline: &Timeline| {
            let data_source = match data_source.clone() {
                // Re-announce the source description.
                DataSourceDesc::Ingestion { desc, cluster_id } => {
                    let desc = desc.into_inline_connection(catalog.state());
                    let ingestion = IngestionDescription::new(desc, cluster_id, object_id);
                    DataSource::Ingestion(ingestion)
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

                    DataSource::Ingestion(ingestion)
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

                    DataSource::IngestionExport {
                        ingestion_id,
                        details,
                        data_config: data_config.into_inline_connection(catalog.state()),
                    }
                }
                DataSourceDesc::Webhook { .. } => DataSource::Webhook,
                DataSourceDesc::Progress => DataSource::Progress,
                DataSourceDesc::Introspection(introspection) => {
                    DataSource::Introspection(introspection)
                }
                DataSourceDesc::Catalog => DataSource::Other,
            };
            CollectionDescription {
                desc: desc.clone(),
                data_source,
                since: None,
                timeline: Some(timeline.clone()),
                primary: None,
            }
        };

        let mut compute_collections = vec![];
        let mut collections = vec![];
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
                    collections.extend(self.materialized_view_storage_collections(mv));
                    compute_collections.push((mv.global_id_writes(), mv.desc.latest()));
                }
                CatalogItem::Sink(sink) => {
                    let storage_sink_from_entry = self.catalog().get_entry_by_global_id(&sink.from);
                    let from_desc = storage_sink_from_entry
                        .relation_desc()
                        .expect("sinks can only be built on items with descs")
                        .into_owned();
                    let as_of = if self.catalog().state().catalog_read_protection_enabled() {
                        self.catalog().state().maintained_read_requirements()[&sink.global_id()]
                            .frontier
                            .into_iter()
                            .collect()
                    } else {
                        Antichain::from_elem(Timestamp::minimum())
                    };
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
                                    as_of,
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
                        timeline: None,
                        primary: None,
                    };
                    collections.push((sink.global_id, collection_desc));
                }
                CatalogItem::Log(_)
                | CatalogItem::View(_)
                | CatalogItem::Index(_)
                | CatalogItem::Type(_)
                | CatalogItem::Func(_)
                | CatalogItem::Secret(_)
                | CatalogItem::Connection(_)
                // Nothing to bootstrap: a metric sink has no storage collection, it publishes
                // into the replica's metrics registry.
                | CatalogItem::MetricSink(_) => (),
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
        let migrated_storage_collections: BTreeSet<_> = migrated_storage_collections
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

        // New builtin storage collections are by default created with [0] since/upper frontiers.
        // For collections that have dependencies on other collections (MVs, CTs), this can violate
        // the frontier invariants assumed by as-of selection. For example, as-of selection expects
        // to be able to pick up computing a materialized view from its most recent upper, but if
        // that upper is [0] it's likely that the required times are not available anymore in the
        // MV inputs.
        //
        // To avoid violating frontier invariants, we need to bump their sinces to times greater
        // than all of their upstream storage inputs. To know the since of a storage input, it has
        // to be registered with the storage controller first.
        let mut pending: BTreeMap<_, _> = collections.into_iter().collect();

        // Only ungoverned builtins need transitive frontiers. Registration ordering uses direct
        // catalog edges, including non-storage objects, rather than expanding user reachability.
        let builtin_dep_gids: BTreeMap<_, _> = pending
            .iter()
            .filter(|(gid, collection)| {
                gid.is_system()
                    && collection.since.is_none()
                    && !storage_metadata.compaction_bounds.contains_key(*gid)
            })
            .map(|(gid, _)| {
                let entry = self.catalog.get_entry_by_global_id(gid);
                let item_id = entry.id();
                let deps = self.catalog.state().transitive_uses(item_id);
                let dep_gids: BTreeSet<_> = deps
                    // Ignore self-dependencies. For example, `transitive_uses` includes the input ID,
                    // and CTs can depend on themselves.
                    .filter(|dep_id| *dep_id != item_id)
                    .map(|dep_id| self.catalog.get_entry(&dep_id).latest_global_id())
                    // Ignore dependencies on objects that are not storage collections.
                    .filter(|dep_gid| pending.contains_key(dep_gid))
                    .collect();
                (*gid, dep_gids)
            })
            .collect();

        let dependencies = self
            .catalog
            .entries()
            .map(|entry| (entry.id(), entry.uses()))
            .collect();
        let batches = storage_bootstrap::registration_batches(
            &dependencies,
            pending.keys().map(|gid| {
                (
                    *gid,
                    self.catalog.get_entry_by_global_id(gid).id(),
                    storage_metadata.collection_metadata[gid],
                )
            }),
        );
        let mut table_registrations = Vec::new();

        for batch in batches {
            let mut ready: Vec<_> = batch
                .into_iter()
                .map(|gid| {
                    let collection = pending.remove(&gid).expect("registered exactly once");
                    (gid, collection)
                })
                .collect();

            // Bump sinces of builtin collections.
            for (gid, collection) in &mut ready {
                // Governed collections recover their persisted readability. Pristine
                // builtin MVs initialize from committed birth permission in storage.
                if storage_metadata.compaction_bounds.contains_key(gid) {
                    continue;
                }
                // Don't silently overwrite an explicitly specified `since`.
                if !gid.is_system() || collection.since.is_some() {
                    continue;
                }

                let mut derived_since = Antichain::from_elem(Timestamp::MIN);
                // Builtins cannot depend on user objects or be replacement targets, so their
                // prerequisites cannot be co-registered in a replacement-induced cycle.
                for dep_gid in &builtin_dep_gids[gid] {
                    let (since, _) = self
                        .controller
                        .storage
                        .collection_frontiers(*dep_gid)
                        .expect("previously registered");
                    derived_since.join_assign(&since);
                }
                collection.since = Some(derived_since);
            }

            table_registrations.extend(ready.iter().filter_map(|(gid, collection)| {
                (matches!(collection.data_source, DataSource::Table)
                    && (!self.controller.read_only() || migrated_storage_collections.contains(gid)))
                .then(|| self.table_registration(*gid, collection.desc.clone()))
            }));

            self.register_adapter_storage_collections(&ready, &migrated_storage_collections)
                .await;
            self.controller
                .storage
                .create_collections_for_bootstrap(
                    storage_metadata,
                    Some(register_ts),
                    ready,
                    &migrated_storage_collections,
                )
                .await
                .unwrap_or_terminate("cannot fail to create collections");
        }

        let statistics_item = self
            .catalog
            .resolve_builtin_storage_collection(&mz_catalog::builtin::MZ_SOURCE_STATISTICS_RAW);
        let statistics_id = self.catalog.get_entry(&statistics_item).latest_global_id();
        self.adapter_storage
            .initialize_statistics(
                self.persist_client.clone(),
                statistics_id,
                mz_storage_types::controller::CollectionMetadata {
                    persist_location: self.query_persist_location.clone(),
                    data_shard: storage_metadata
                        .get_collection_shard(statistics_id)
                        .expect("statistics have committed shard metadata"),
                    relation_desc: mz_storage_client::statistics::MZ_SOURCE_STATISTICS_RAW_DESC
                        .clone(),
                    txns_shard: None,
                },
                mz_storage_types::dyncfgs::STATISTICS_RETENTION_DURATION
                    .get(self.storage_configuration.config_set()),
                migrated_storage_collections.contains(&statistics_id),
            )
            .await;

        // Register txn-wal tables before the later system-table snapshot.
        if !table_registrations.is_empty() {
            self.table_write_handle
                .register(register_ts, table_registrations)
                .await
                .expect("table worker is alive during bootstrap")
                .unwrap_or_terminate("cannot fail to register tables");
        }

        if !self.controller.read_only() {
            self.apply_local_write(register_ts).await;
        }
    }

    /// Returns the current list of catalog entries, sorted into an appropriate order for
    /// bootstrapping.
    ///
    /// The returned entries are in dependency order. Indexes are sorted immediately after the
    /// objects they index, to ensure that all dependants of these indexed objects can make use of
    /// the respective indexes.
    fn bootstrap_sort_catalog_entries(&self) -> Vec<CatalogEntry> {
        self.sort_catalog_entries(self.catalog().entries().cloned())
    }

    /// Orders a dependency-closed set of entries, placing indexes directly after their inputs.
    fn sort_catalog_entries(
        &self,
        entries: impl IntoIterator<Item = CatalogEntry>,
    ) -> Vec<CatalogEntry> {
        let mut indexes_on = BTreeMap::<_, Vec<_>>::new();
        let mut non_indexes = Vec::new();
        for entry in entries {
            if let Some(index) = entry.index() {
                let on = self.catalog().get_entry_by_global_id(&index.on);
                indexes_on.entry(on.id()).or_default().push(entry);
            } else {
                non_indexes.push(entry);
            }
        }

        let key_fn = |entry: &CatalogEntry| entry.id;
        let dependencies_fn = |entry: &CatalogEntry| entry.uses();
        sort_topological(&mut non_indexes, key_fn, dependencies_fn);

        let mut result = Vec::new();
        for entry in non_indexes {
            let id = entry.id();
            result.push(entry);
            if let Some(mut indexes) = indexes_on.remove(&id) {
                result.append(&mut indexes);
            }
        }

        soft_assert_or_log!(
            indexes_on.is_empty(),
            "indexes with missing dependencies: {indexes_on:?}",
        );

        result
    }

    /// Builds an index plan and rendered notices from its catalog definition.
    ///
    /// All catalog reads, including notice rendering, use `catalog`. The compute
    /// snapshot must contain the collections available for imports. The coordinator
    /// supplies only optimizer metrics and transient IDs.
    /// This does not select an `as_of`, install the dataflow, or cache the result.
    fn build_index_dataflow_plan(
        &self,
        catalog: Arc<CatalogState>,
        name: &QualifiedItemName,
        index: &Index,
        compute_instance: ComputeInstanceSnapshot,
        optimizer_config: OptimizerConfig,
    ) -> Result<GlobalExpressions, AdapterError> {
        let global_id = index.global_id();
        let mut optimizer = optimize::index::Optimizer::new(
            Arc::<CatalogState>::clone(&catalog),
            compute_instance,
            global_id,
            optimizer_config.clone(),
            self.optimizer_metrics(),
        );
        let index_plan = optimize::index::Index::new(name.clone(), index.on, index.keys.to_vec());
        let global_mir_plan = optimizer.optimize(index_plan)?;
        let global_mir = global_mir_plan.df_desc().clone();
        let global_lir_plan = optimizer.optimize(global_mir_plan)?;
        let (physical_plan, metainfo) = global_lir_plan.unapply();
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .map(|(_item_id, gid)| gid)
            .take(metainfo.optimizer_notices.len())
            .collect::<Vec<_>>();
        let dataflow_metainfos = CatalogState::render_notices_core(
            &catalog.for_system_session(),
            (catalog.config().now)(),
            &metainfo,
            notice_ids,
            Some(global_id),
        );
        Ok(GlobalExpressions {
            global_mir,
            physical_plan,
            dataflow_metainfos,
            optimizer_features: optimizer_config.features,
            item_version: RelationVersion::root(),
        })
    }

    /// Builds a materialized view plan and rendered notices from its catalog definition.
    ///
    /// All catalog reads, including notice rendering, use `catalog`. The compute
    /// snapshot must contain the collections available for imports. The coordinator
    /// supplies only optimizer metrics and transient IDs.
    /// This does not select an `as_of`, install the dataflow, or cache the result.
    fn build_materialized_view_dataflow_plan(
        &self,
        catalog: Arc<CatalogState>,
        name: &QualifiedItemName,
        mv: &MaterializedView,
        compute_instance: ComputeInstanceSnapshot,
        optimizer_config: OptimizerConfig,
    ) -> Result<GlobalExpressions, AdapterError> {
        let global_id = mv.global_id_writes();
        let (_, internal_view_id) = self.allocate_transient_id();
        let debug_name = catalog.resolve_full_name(name, None).to_string();
        let mut optimizer = optimize::materialized_view::Optimizer::new(
            Arc::<CatalogState>::clone(&catalog),
            compute_instance,
            global_id,
            internal_view_id,
            mv.desc.latest().iter_names().cloned().collect(),
            mv.non_null_assertions.clone(),
            mv.refresh_schedule.clone(),
            debug_name,
            optimizer_config.clone(),
            self.optimizer_metrics(),
        );

        // Use the HIR SQL type because MIR SQL types may not be coherent.
        let typ =
            infer_sql_type_for_catalog(&mv.raw_expr, &mv.locally_optimized_expr.as_ref().clone());
        let global_mir_plan =
            optimizer.optimize((mv.locally_optimized_expr.as_ref().clone(), typ))?;
        let global_mir = global_mir_plan.df_desc().clone();
        let global_lir_plan = optimizer.optimize(global_mir_plan)?;
        let (physical_plan, metainfo) = global_lir_plan.unapply();
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .map(|(_item_id, gid)| gid)
            .take(metainfo.optimizer_notices.len())
            .collect::<Vec<_>>();
        let dataflow_metainfos = CatalogState::render_notices_core(
            &catalog.for_system_session(),
            (catalog.config().now)(),
            &metainfo,
            notice_ids,
            Some(global_id),
        );
        Ok(GlobalExpressions {
            global_mir,
            physical_plan,
            dataflow_metainfos,
            optimizer_features: optimizer_config.features,
            item_version: latest_item_version(&mv.collections),
        })
    }

    /// Builds a metric sink plan and rendered notices from its catalog definition.
    ///
    /// All catalog reads, including notice rendering, use `catalog`. The compute
    /// snapshot must contain the collections available for imports. The coordinator
    /// supplies only optimizer metrics and transient IDs.
    /// This does not select an `as_of`, install the dataflow, or cache the result.
    fn build_metric_sink_dataflow_plan(
        &self,
        catalog: Arc<CatalogState>,
        name: &QualifiedItemName,
        metric_sink: &MetricSink,
        compute_instance: ComputeInstanceSnapshot,
        optimizer_config: OptimizerConfig,
    ) -> Result<GlobalExpressions, AdapterError> {
        let global_id = metric_sink.global_id;
        // A transient id for the view the optimizer builds over `from` to
        // shape its rows (see `optimize::metric_sink::shape_metric_sink_source`).
        // The id only needs to be unique within this dataflow, so a cached plan
        // reusing a transient id from a previous boot is safe: build ids are
        // dataflow-local on the worker and never registered in the controller's
        // instance-global collections (only export ids are).
        let (_, view_id) = self.allocate_transient_id();

        let (optimized_plan, global_lir_plan) = {
            let mut optimizer = optimize::metric_sink::Optimizer::new(
                Arc::<CatalogState>::clone(&catalog),
                compute_instance,
                view_id,
                global_id,
                optimizer_config.clone(),
                self.optimizer_metrics(),
            );

            // MIR ⇒ MIR optimization (global)
            let metric_sink_plan = optimize::metric_sink::MetricSink::new(
                catalog.resolve_full_name(name, None).to_string(),
                optimize::metric_sink::MetricSinkFrom::Id(metric_sink.from),
                metric_sink.prefix.clone(),
                None,
            );
            let global_mir_plan = optimizer.optimize(metric_sink_plan)?;
            let optimized_plan = global_mir_plan.df_desc().clone();

            // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
            let global_lir_plan = optimizer.optimize(global_mir_plan)?;

            (optimized_plan, global_lir_plan)
        };

        let (physical_plan, metainfo) = global_lir_plan.unapply();
        let metainfo = {
            // Pre-allocate a vector of transient GlobalIds for each notice.
            let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
                .map(|(_item_id, gid)| gid)
                .take(metainfo.optimizer_notices.len())
                .collect::<Vec<_>>();
            // Return a metainfo with rendered notices.
            CatalogState::render_notices_core(
                &catalog.for_system_session(),
                (catalog.config().now)(),
                &metainfo,
                notice_ids,
                Some(global_id),
            )
        };
        Ok(GlobalExpressions {
            global_mir: optimized_plan,
            physical_plan,
            dataflow_metainfos: metainfo,
            optimizer_features: optimizer_config.features,
            item_version: RelationVersion::root(),
        })
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
    async fn bootstrap_dataflow_plans(
        &mut self,
        ordered_catalog_entries: &[CatalogEntry],
        mut cached_global_exprs: BTreeMap<GlobalId, GlobalExpressions>,
        written_ids: &BTreeSet<GlobalId>,
        bootstrap_client: Option<&Arc<crate::query_client::QueryClient>>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        plan_holds: &mut Vec<ReadHolds>,
    ) -> Result<BTreeMap<GlobalId, GlobalExpressions>, AdapterError> {
        let revision = self.catalog().transient_revision();
        let mut instance_snapshots: BTreeMap<_, _> = self
            .catalog()
            .clusters()
            .map(|cluster| {
                let snapshot = if self.catalog().state().catalog_read_protection_enabled() {
                    ComputeInstanceSnapshot::new_from_parts(
                        cluster.id,
                        cluster.log_indexes.values().copied().collect(),
                    )
                } else {
                    self.instance_snapshot(cluster.id)
                        .expect("compute instance exists")
                };
                (cluster.id, snapshot)
            })
            .collect();
        let mut uncached_expressions = BTreeMap::new();
        for entry in ordered_catalog_entries {
            self.publish_bootstrap_read_protection(bootstrap_client, builtin_table_updates)
                .await?;
            self.check_bootstrap_planning_revision(revision)?;
            let (global_id, cluster_id) = match entry.item() {
                CatalogItem::Index(index) => (index.global_id(), index.cluster_id),
                CatalogItem::MaterializedView(mv) => (mv.global_id_writes(), mv.cluster_id),
                CatalogItem::MetricSink(sink) => (sink.global_id, sink.cluster_id),
                _ => continue,
            };
            let compute_instance = instance_snapshots
                .get_mut(&cluster_id)
                .expect("dataflow cluster is declared");
            if matches!(entry.item(), CatalogItem::Index(_))
                && compute_instance.contains_collection(&global_id)
            {
                continue;
            }
            let config = OptimizerConfig::from(self.catalog().system_config())
                .override_from(&self.catalog().get_cluster(cluster_id).config.features())
                .override_from(
                    &self
                        .catalog()
                        .state()
                        .cluster_scoped_optimizer_overrides(cluster_id),
                );
            let prepare = bootstrap_client.is_some() && !written_ids.contains(&global_id);
            // Absence is initial admission, not permission to read at MIN. The
            // selection transaction computes its bound across same-batch imports.
            let mut required = if prepare {
                self.bootstrap_plan_requirement(entry)?
            } else {
                None
            };
            let mut excluded = BTreeSet::new();
            let mut cached = cached_global_exprs.remove(&global_id);
            let expressions = loop {
                let snapshot = if let Some(required) = required {
                    // Keep dependency order, but only offer indexes with justified
                    // permission. Replica installation is deliberately irrelevant.
                    let candidates = self
                        .catalog()
                        .get_cluster(cluster_id)
                        .log_indexes
                        .values()
                        .copied()
                        .chain(ordered_catalog_entries.iter().filter_map(
                            |entry| match entry.item() {
                                CatalogItem::Index(index) if index.cluster_id == cluster_id => {
                                    Some(index.global_id())
                                }
                                CatalogItem::MaterializedView(mv)
                                    if mv.cluster_id == cluster_id =>
                                {
                                    Some(mv.global_id_writes())
                                }
                                _ => None,
                            },
                        ))
                        .filter(|id| {
                            compute_instance.contains_collection(id) && !excluded.contains(id)
                        })
                        .filter(|id| {
                            self.catalog()
                                .state()
                                .collection_compaction_bounds()
                                .get(id)
                                .map_or_else(
                                    || {
                                        self.catalog()
                                            .get_cluster(cluster_id)
                                            .log_indexes
                                            .values()
                                            .any(|log| log == id)
                                    },
                                    |bound| bound.less_equal(&required),
                                )
                        })
                        .collect();
                    ComputeInstanceSnapshot::new_from_parts(cluster_id, candidates)
                } else {
                    compute_instance.clone()
                };
                let cache_hit = cached.take().filter(|expressions| {
                    written_ids.contains(&global_id)
                        || (expressions.optimizer_features == config.features
                            && (!prepare
                                || (expressions.collection_imports().all(|input| {
                                    self.catalog().try_get_entry_by_global_id(input).is_some()
                                }) && expressions
                                    .global_mir
                                    .index_imports
                                    .keys()
                                    .chain(expressions.physical_plan.index_imports.keys())
                                    .all(|id| snapshot.contains_collection(id)))))
                });
                let (expressions, built) = match cache_hit {
                    Some(expressions) => {
                        debug!("global expression cache hit for {global_id:?}");
                        (expressions, false)
                    }
                    None => {
                        let catalog = Arc::new(self.catalog().state().clone());
                        let expressions = match entry.item() {
                            CatalogItem::Index(index) => self.build_index_dataflow_plan(
                                catalog,
                                entry.name(),
                                index,
                                snapshot,
                                config.clone(),
                            )?,
                            CatalogItem::MaterializedView(mv) => self
                                .build_materialized_view_dataflow_plan(
                                    catalog,
                                    entry.name(),
                                    mv,
                                    snapshot,
                                    config.clone(),
                                )?,
                            CatalogItem::MetricSink(sink) => self.build_metric_sink_dataflow_plan(
                                catalog,
                                entry.name(),
                                sink,
                                snapshot,
                                config.clone(),
                            )?,
                            _ => unreachable!(),
                        };
                        (expressions, true)
                    }
                };
                if let (Some(client), Some(requested)) = (bootstrap_client, required) {
                    let bundle = CollectionIdBundle {
                        storage_ids: expressions
                            .global_mir
                            .source_imports
                            .keys()
                            .chain(expressions.physical_plan.source_imports.keys())
                            .copied()
                            .collect(),
                        compute_ids: BTreeMap::from([(
                            cluster_id,
                            expressions
                                .global_mir
                                .index_imports
                                .keys()
                                .chain(expressions.physical_plan.index_imports.keys())
                                .copied()
                                .collect(),
                        )]),
                    };
                    let acquired = self
                        .acquire_bootstrap_read_protection(
                            Arc::clone(client),
                            bundle.clone(),
                            Some(requested),
                            builtin_table_updates,
                        )
                        .await;
                    self.check_bootstrap_planning_revision(revision)?;
                    // Recovery progress can retire an old input requirement while
                    // preparation awaits. Use only the newly committed requirement,
                    // never advance one merely to accommodate an unsuitable import.
                    let current = self.bootstrap_plan_requirement(entry)?;
                    required = current;
                    let Some(required) = current else {
                        continue;
                    };
                    if required < requested {
                        continue;
                    }
                    let (holds, _) = match acquired {
                        Ok(acquired) => acquired,
                        Err(error) => {
                            if !self
                                .catalog()
                                .state()
                                .client_incarnations()
                                .contains_key(&client.protection.incarnation())
                            {
                                return Err(error);
                            }
                            // A refreshed permission can become empty while the
                            // grant is being acquired. Replan only if an actual
                            // index import is now demonstrably ineligible.
                            let before = excluded.len();
                            excluded.extend(
                                bundle
                                    .compute_ids
                                    .values()
                                    .flatten()
                                    .filter(|id| {
                                        self.catalog()
                                            .state()
                                            .collection_compaction_bounds()
                                            .get(*id)
                                            .is_some_and(|bound| !bound.less_equal(&required))
                                    })
                                    .copied(),
                            );
                            if excluded.len() == before {
                                return Err(error);
                            }
                            continue;
                        }
                    };
                    if !holds.least_valid_read().less_equal(&required) {
                        if bundle
                            .storage_ids
                            .iter()
                            .any(|id| !holds.since(id).less_equal(&required))
                        {
                            return Err(AdapterError::internal(
                                "bootstrap plan preparation",
                                "storage cannot support required history",
                            ));
                        }
                        let before = excluded.len();
                        excluded.extend(
                            bundle
                                .compute_ids
                                .values()
                                .flatten()
                                .filter(|id| !holds.since(id).less_equal(&required))
                                .copied(),
                        );
                        if excluded.len() == before {
                            return Err(AdapterError::internal(
                                "bootstrap plan preparation",
                                "imports cannot support required history",
                            ));
                        }
                        // Only imports proven ineligible are removed. A metadata
                        // conflict does not move the owner's fixed frontier.
                        continue;
                    }
                    plan_holds.push(holds);
                }
                if built {
                    uncached_expressions.insert(global_id, expressions.clone());
                }
                break expressions;
            };
            let catalog = self.catalog_mut();
            catalog.set_optimized_plan(global_id, expressions.global_mir);
            catalog.set_physical_plan(global_id, expressions.physical_plan);
            catalog.set_dataflow_metainfo(global_id, expressions.dataflow_metainfos);
            if !matches!(entry.item(), CatalogItem::MetricSink(_)) {
                compute_instance.insert_collection(global_id);
            }
        }
        Ok(uncached_expressions)
    }

    fn bootstrap_plan_requirement(
        &self,
        entry: &CatalogEntry,
    ) -> Result<Option<Timestamp>, AdapterError> {
        let requirement = match entry.item() {
            CatalogItem::Index(index) => self
                .catalog()
                .state()
                .collection_compaction_bounds()
                .get(&index.global_id())
                .map(|bound| bound.as_option().copied()),
            CatalogItem::MaterializedView(mv) => self
                .catalog()
                .state()
                .maintained_read_requirements()
                .get(&mv.global_id_writes())
                .map(|requirement| requirement.frontier),
            _ => None,
        };
        match requirement {
            Some(Some(timestamp)) => Ok(Some(timestamp)),
            Some(None) if matches!(entry.item(), CatalogItem::MaterializedView(_)) => Ok(None),
            Some(None) => Err(AdapterError::internal(
                "bootstrap plan preparation",
                "required history is empty",
            )),
            None => Ok(None),
        }
    }

    fn check_bootstrap_planning_revision(&self, revision: u64) -> Result<(), AdapterError> {
        if self.catalog().transient_revision() != revision {
            return Err(AdapterError::internal(
                "bootstrap plan preparation",
                "catalog planning context changed",
            ));
        }
        Ok(())
    }

    async fn publish_bootstrap_read_protection(
        &mut self,
        client: Option<&Arc<crate::query_client::QueryClient>>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
    ) -> Result<(), AdapterError> {
        let Some(client) = client else { return Ok(()) };
        let Some(requirements) = client
            .protection
            .prepare_publication_if_needed(client.last_publication().elapsed())
        else {
            return Ok(());
        };
        let result = self
            .bootstrap_catalog_transact(
                vec![crate::catalog::Op::PublishClientReadRequirements {
                    incarnation: client.protection.incarnation(),
                    requirements,
                }],
                builtin_table_updates,
            )
            .await;
        client.protection.finish_publication(result.is_ok());
        if !self
            .catalog()
            .state()
            .client_incarnations()
            .contains_key(&client.protection.incarnation())
        {
            client.protection.mark_closed();
        }
        result?;
        client.published();
        Ok(())
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
    async fn bootstrap_dataflow_as_ofs(
        &mut self,
    ) -> Result<BTreeMap<GlobalId, ReadHold>, AdapterError> {
        let mut catalog_ids = Vec::new();
        let mut dataflows = Vec::new();
        let mut read_policies: BTreeMap<GlobalId, ReadPolicy> = BTreeMap::new();
        let mut pending_replacements = BTreeMap::new();
        for entry in self.catalog.entries() {
            let gid = match entry.item() {
                CatalogItem::Index(idx) => idx.global_id(),
                CatalogItem::MaterializedView(mv) => mv.global_id_writes(),
                CatalogItem::MetricSink(metric_sink) => metric_sink.global_id,
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

                if let CatalogItem::MaterializedView(mv) = entry.item()
                    && mv.replacement_target.is_some()
                {
                    pending_replacements.insert(
                        gid,
                        mv.initial_as_of
                            .clone()
                            .expect("pending replacement has an initial visibility frontier"),
                    );
                }
                if let Some(compaction_window) = entry.item().initial_logical_compaction_window() {
                    read_policies.insert(gid, compaction_window.into());
                }
            }
        }

        self.sync_compute_read_protection().await?;
        let mut index_bounds: BTreeMap<_, _> = self
            .catalog()
            .state()
            .collection_compaction_bounds()
            .iter()
            .map(|(&id, bound)| (id, bound.clone()))
            .collect();
        if let Some(subscriber) = &self.compaction_bound_subscriber {
            index_bounds.extend(
                subscriber
                    .bounds()
                    .iter()
                    .map(|(&id, bound)| (id, bound.clone())),
            );
        }
        let read_ts = self.get_local_read_ts().await;
        let read_holds = as_of_selection::run(
            &mut dataflows,
            &read_policies,
            &index_bounds,
            &pending_replacements,
            &*self.controller.storage_collections,
            read_ts,
            self.controller.read_only(),
            self.catalog().state().catalog_read_protection_enabled(),
        );

        let catalog = self.catalog_mut();
        for (id, plan) in catalog_ids.into_iter().zip_eq(dataflows) {
            catalog.set_physical_plan(id, plan);
        }

        Ok(read_holds)
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
            self.schedule_arrangement_sizes_collection().await;
            self.schedule_hydration_history_collection();
            self.spawn_privatelink_vpc_endpoints_watch_task();
            self.spawn_statement_logging_task();
            self.spawn_catalog_info_metrics_task();
            self.spawn_cluster_controller_task();
            self.spawn_caught_up_check_task();
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

            // A persisted `Notified` future for the linearize re-check signal.
            // It must outlive a single loop iteration and be re-`set` only after
            // it completes: a fresh `notified()` per iteration could drop a
            // wakeup that arrives while a higher-priority branch wins the same
            // poll, stranding pending reads. Keeping it pinned across iterations
            // leaves it registered, so no wakeup is lost.
            let linearize_reads_notify = Arc::clone(&self.linearize_reads_notify);
            let linearize_reads_notified = linearize_reads_notify.notified();
            tokio::pin!(linearize_reads_notified);

            let mut publication_delay = self
                .catalog()
                .system_config()
                .catalog_read_protection_publish_interval();
            let publication_timer = tokio::time::sleep(mz_catalog::retry::sample_duration(
                Duration::ZERO,
                publication_delay,
            ));
            tokio::pin!(publication_timer);
            let subscription_timer = tokio::time::sleep(mz_catalog::retry::sample_duration(
                Duration::ZERO,
                CATALOG_SUBSCRIPTION_INTERVAL,
            ));
            tokio::pin!(subscription_timer);
            let client_heartbeat_delay =
                crate::query_client::read_protection::client_protection_heartbeat_interval();
            let client_heartbeat_timer = tokio::time::sleep(client_heartbeat_delay);
            tokio::pin!(client_heartbeat_timer);
            // Match storage frontier introspection's maintenance cadence. This
            // is independent of permission publication and installation waits.
            let frontier_timer = tokio::time::sleep(Duration::from_secs(1));
            tokio::pin!(frontier_timer);

            async fn trace_maintenance<T>(
                phase: &'static str,
                operation: impl std::future::Future<Output = T>,
            ) -> T {
                let start = Instant::now();
                tracing::debug!(
                    target: "mz_adapter::frontend_read_then_write",
                    phase, "coordinator maintenance await started"
                );
                let result = operation.await;
                tracing::debug!(
                    target: "mz_adapter::frontend_read_then_write",
                    phase, elapsed = ?start.elapsed(), "coordinator maintenance await completed"
                );
                result
            }

            loop {
                let delay = self
                    .catalog()
                    .system_config()
                    .catalog_read_protection_publish_interval();
                if delay != publication_delay {
                    publication_delay = delay;
                    publication_timer.set(tokio::time::sleep(mz_catalog::retry::sample_duration(
                        Duration::ZERO,
                        delay,
                    )));
                }
                // Before adding a branch to this select loop, please ensure that the branch is
                // cancellation safe and add a comment explaining why. You can refer here for more
                // info: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
                select! {
                    // Maintenance retains priority at each poll. Each message-bearing round also
                    // admits a bounded client batch below, after its selected messages, so continuous
                    // internal responses cannot exclude waiting client commands.
                    biased;

                    // Polling the pinned timer is cancel-safe. Renewal and requirement
                    // publication share one transaction before checking abandoned clients.
                    _ = client_heartbeat_timer.as_mut() => {
                        let mut conflict = false;
                        if self.query_client.as_ref().is_some_and(|client| {
                            client.last_publication().elapsed() >= client_heartbeat_delay
                        }) && let Err(error) = trace_maintenance(
                            "heartbeat_publish_client", self.publish_client_read_protection()
                        ).await {
                            conflict |= read_protection::is_read_protection_conflict(&error);
                            warn!(%error, "unable to publish query client protection");
                        }
                        if let Err(error) = trace_maintenance(
                            "heartbeat_reclaim", self.reclaim_client_read_protection()
                        ).await {
                            conflict |= read_protection::is_read_protection_conflict(&error);
                            warn!(%error, "unable to reclaim query client protection");
                        }
                        if conflict {
                            read_protection::defer_protection_retry(
                                client_heartbeat_timer.as_mut(),
                                publication_timer.as_mut(),
                                self.read_protection_conflict_delay(),
                            );
                        } else {
                            client_heartbeat_timer.set(tokio::time::sleep(client_heartbeat_delay));
                        }
                    }

                    // Polling a pinned Sleep is cancellation-safe. Following committed permission
                    // does not depend on the savepoint's publication setting.
                    _ = subscription_timer.as_mut(),
                        if self.compaction_bound_subscriber.is_some()
                            || self.controller.replica_owned_compute()
                            || !self.pending_compute_installations.is_empty() => {
                        if self.controller.replica_owned_compute() {
                            if let Err(error) = trace_maintenance(
                                "subscription_refresh", self.refresh_catalog(None)
                            ).await {
                                warn!(%error, "unable to follow committed native catalog state");
                            }
                            if let Err(error) = trace_maintenance(
                                "subscription_reconcile", self.reconcile_declared_replicas()
                            ).await {
                                warn!(%error, "unable to realize declared replicas");
                            }
                        }
                        trace_maintenance(
                            "subscription_install", self.install_pending_compute_collections()
                        ).await;
                        if let Err(error) = trace_maintenance(
                            "subscription_protection", self.sync_compute_read_protection()
                        ).await {
                            warn!(%error, "unable to follow catalog read protection");
                        }
                        subscription_timer.set(tokio::time::sleep(mz_catalog::retry::periodic_delay(
                            CATALOG_SUBSCRIPTION_INTERVAL,
                        )));
                    }

                    // Polling a pinned Sleep is cancellation-safe. Bootstrap restores execution holds
                    // before this runs. Give publication a turn even under continuous load,
                    // but schedule from completion so a slow commit cannot monopolize us.
                    _ = publication_timer.as_mut(),
                        if self.query_client.is_some()
                            || (self.catalog().state().catalog_read_protection_enabled()
                                && !self.controller.read_only()) => {
                        let mut conflict = false;
                        if let Err(error) = trace_maintenance(
                            "publication_client", self.publish_client_read_protection()
                        ).await {
                            conflict |= read_protection::is_read_protection_conflict(&error);
                            warn!(%error, "unable to publish query client protection");
                        }
                        if let Err(error) = trace_maintenance(
                            "publication_bounds", self.publish_read_protection()
                        ).await {
                            conflict |= read_protection::is_read_protection_conflict(&error);
                            warn!(%error, "unable to publish catalog read protection");
                        }
                        // Only definitive outcomes reach here. Rebuild proposals on
                        // the next turn, retaining committed protection meanwhile.
                        if conflict {
                            read_protection::defer_protection_retry(
                                publication_timer.as_mut(),
                                client_heartbeat_timer.as_mut(),
                                self.read_protection_conflict_delay(),
                            );
                        } else {
                            let delay = mz_catalog::retry::periodic_delay(publication_delay);
                            publication_timer.set(tokio::time::sleep(delay));
                        }
                    }
                    // Polling the pinned Sleep is cancellation-safe. Snapshot
                    // observations, not permissions or controller installation.
                    _ = frontier_timer.as_mut(), if self.controller.replica_owned_compute() => {
                        self.update_native_frontier_introspection();
                        frontier_timer.set(tokio::time::sleep(Duration::from_secs(1)));
                    },
                    // `recv_many()` on `UnboundedReceiver` is cancellation safe:
                    // https://docs.rs/tokio/1.38.0/tokio/sync/mpsc/struct.UnboundedReceiver.html#cancel-safety-1
                    // Receive a batch of commands.
                    _ = internal_cmd_rx.recv_many(&mut messages, MESSAGE_BATCH) => {},
                    // `next()` on any stream is cancel-safe:
                    // https://docs.rs/tokio-stream/0.1.9/tokio_stream/trait.StreamExt.html#cancel-safety
                    // Receive a single command.
                    Some(event) = cluster_events.next() => {
                        messages.push(Message::ClusterEvent(event))
                    },
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
                            PendingWriteTxn::User { span, .. } => Some(span),
                            PendingWriteTxn::System { .. } => None,
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
                            let prev = self
                                .pending_linearize_read_txns
                                .insert(conn_id, pending_read_txn);
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
                        // Writable keepalives use the committer to advance tables and read holds.
                        // Its permit coalesces ticks behind a slow oracle. Read-only mode advances
                        // timelines directly.
                        if self.controller.read_only() {
                            messages.push(Message::AdvanceTimelines);
                        } else {
                            self.group_commit_tx.notify();
                        }
                    },
                    // Re-check pending strict serializable reads. Deliberately
                    // placed below the group commit branches above: a re-check
                    // only makes a read ready if the timestamp oracle has
                    // advanced, and the oracle only advances via group commit, so
                    // this must never win over (and thereby starve) group commit.
                    // `Notify` coalesces re-arms into a single wakeup, so even
                    // when a pending read sits just behind the oracle (re-armed
                    // sub-millisecond), the lower branches (including the idle
                    // watchdog) stay reachable. See the pin above for why the
                    // future is persisted rather than recreated per iteration.
                    () = linearize_reads_notified.as_mut() => {
                        linearize_reads_notified.set(linearize_reads_notify.notified());
                        messages.push(Message::LinearizeReads);
                    }

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

                // Preserve client FIFO and the batch limit. Maintenance-only rounds re-poll without
                // client work, which could consume the re-armed timers' delays and exclude message
                // processing again. Service is bounded in message-bearing rounds, not time: a handler
                // can still await slow work. The select remains the idle wakeup/shutdown path.
                if !messages.is_empty() {
                    while cmd_messages.len() < MESSAGE_BATCH {
                        let Ok(command) = cmd_rx.try_recv() else {
                            break;
                        };
                        cmd_messages.push(command);
                    }
                }
                messages.extend(
                    cmd_messages
                        .drain(..)
                        .map(|(otel_ctx, cmd)| Message::Command(otel_ctx, cmd)),
                );

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

            // The sweep can own timestamp-oracle senders through its background
            // client. Release them before the coordinator runtime starts shutting
            // down the oracle workers.
            if let Some(sweep) = self.hydration_history_sweep.take() {
                sweep.abort_and_wait().await;
            }
            if let Some(subscriber) = self.compaction_bound_subscriber.take() {
                subscriber.expire().await;
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

    /// Refills the user ID pool by allocating IDs from the catalog.
    ///
    /// Requests `max(min_count, batch_size)` IDs so the pool is never
    /// under-filled relative to the configured batch size.
    async fn refill_user_id_pool(&mut self, min_count: u64) -> Result<(), AdapterError> {
        let batch_size = USER_ID_POOL_BATCH_SIZE.get(self.catalog().system_config().dyncfgs());
        let to_allocate = min_count.max(u64::from(batch_size));
        let id_ts = self.get_catalog_write_ts().await;
        let ids = self.catalog().allocate_user_ids(to_allocate, id_ts).await?;
        if let (Some((first_id, _)), Some((last_id, _))) = (ids.first(), ids.last()) {
            let start = match first_id {
                CatalogItemId::User(id) => *id,
                other => {
                    return Err(AdapterError::Internal(format!(
                        "expected User CatalogItemId, got {other:?}"
                    )));
                }
            };
            let end = match last_id {
                CatalogItemId::User(id) => *id + 1, // exclusive upper bound
                other => {
                    return Err(AdapterError::Internal(format!(
                        "expected User CatalogItemId, got {other:?}"
                    )));
                }
            };
            self.user_id_pool.refill(start, end);
        } else {
            return Err(AdapterError::Internal(
                "catalog returned no user IDs".into(),
            ));
        }
        Ok(())
    }

    /// Allocates a single user ID, refilling the pool from the catalog if needed.
    async fn allocate_user_id(&mut self) -> Result<(CatalogItemId, GlobalId), AdapterError> {
        if let Some(id) = self.user_id_pool.allocate() {
            return Ok((CatalogItemId::User(id), GlobalId::User(id)));
        }
        self.refill_user_id_pool(1).await?;
        let id = self.user_id_pool.allocate().expect("ID pool just refilled");
        Ok((CatalogItemId::User(id), GlobalId::User(id)))
    }

    /// Allocates `count` user IDs, refilling the pool from the catalog if needed.
    async fn allocate_user_ids(
        &mut self,
        count: u64,
    ) -> Result<Vec<(CatalogItemId, GlobalId)>, AdapterError> {
        if self.user_id_pool.remaining() < count {
            self.refill_user_id_pool(count).await?;
        }
        let raw_ids = self
            .user_id_pool
            .allocate_many(count)
            .expect("pool has enough IDs after refill");
        Ok(raw_ids
            .into_iter()
            .map(|id| (CatalogItemId::User(id), GlobalId::User(id)))
            .collect())
    }

    /// Obtain a reference to the coordinator's connection context.
    fn connection_context(&self) -> &ConnectionContext {
        &self.storage_configuration.connection_context
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
            let ended_at = self.now();
            self.end_statement_execution(uuid, reason, ended_at);
        }
    }

    /// Creates a new dataflow builder from the catalog and indexes in `self`.
    #[instrument(level = "debug")]
    pub fn dataflow_builder(&self, instance: ComputeInstanceId) -> DataflowBuilder<'_> {
        let compute = self
            .query_instance_snapshot(instance)
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

    /// Query planning offers catalog-declared indexes independently of readiness.
    fn query_instance_snapshot(
        &self,
        id: ComputeInstanceId,
    ) -> Result<ComputeInstanceSnapshot, InstanceMissing> {
        if let Some(client) = self.query_client.as_ref() {
            self.catalog()
                .try_get_cluster(id)
                .ok_or(InstanceMissing(id))?;
            Ok(client.instance_snapshot(self.catalog(), id))
        } else {
            self.instance_snapshot(id)
        }
    }

    /// Maintained DDL produces a planning candidate from catalog declarations.
    /// Installation separately validates available paths and their readability.
    fn candidate_instance_snapshot(
        &self,
        id: ComputeInstanceId,
    ) -> Result<ComputeInstanceSnapshot, InstanceMissing> {
        if !self.catalog().state().catalog_read_protection_enabled() {
            return self.instance_snapshot(id);
        }
        let cluster = self
            .catalog()
            .try_get_cluster(id)
            .ok_or(InstanceMissing(id))?;
        let mut indexes: BTreeSet<_> = cluster.log_indexes.values().copied().collect();
        indexes.extend(cluster.bound_objects.iter().filter_map(|item| {
            match self.catalog().get_entry(item).item() {
                CatalogItem::Index(index) => Some(index.global_id()),
                _ => None,
            }
        }));
        Ok(ComputeInstanceSnapshot::new_from_parts(id, indexes))
    }

    /// Validate an MV pin for legacy controller installation. An explicit pin
    /// without a local realization is an error, never an untargeted dataflow.
    fn materialized_view_physical_target(
        &self,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
    ) -> Result<Option<ReplicaId>, DataflowCreationError> {
        target
            .map(|target| {
                self.catalog()
                    .state()
                    .physical_replica_for_target(cluster, target)
                    .ok_or(DataflowCreationError::ReplicaMissing(target))
            })
            .transpose()
    }

    /// Call into the compute controller to install a finalized dataflow, and
    /// initialize the read policies for its exported readable objects.
    ///
    /// # Panics
    ///
    /// Panics if dataflow creation fails.
    pub(crate) async fn ship_dataflow(
        &mut self,
        dataflow: DataflowDescription<LirRelationExpr>,
        instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
    ) {
        self.try_ship_dataflow(dataflow, instance, target_replica)
            .await
            .unwrap_or_terminate("dataflow creation cannot fail");
    }

    /// Call into the compute controller to install a finalized dataflow, and
    /// initialize the read policies for its exported readable objects.
    pub(crate) async fn try_ship_dataflow(
        &mut self,
        dataflow: DataflowDescription<LirRelationExpr>,
        instance: ComputeInstanceId,
        target_replica: Option<ReplicaId>,
    ) -> Result<(), DataflowCreationError> {
        // We must only install read policies for indexes, not for sinks.
        // Sinks are write-only compute collections that don't have read policies.
        let export_ids = dataflow.exported_index_ids().collect();

        self.controller
            .compute
            .create_dataflow(instance, dataflow, target_replica)?;

        self.initialize_compute_read_policies(export_ids, instance, CompactionWindow::Default)
            .await;

        Ok(())
    }

    /// Call into the compute controller to allow writes to the specified IDs
    /// from the specified instance. Calling this function multiple times and
    /// calling it on a read-only instance has no effect.
    pub(crate) fn allow_writes(&mut self, instance: ComputeInstanceId, id: GlobalId) {
        if self.controller.replica_owned_compute() {
            return;
        }
        self.controller
            .compute
            .allow_writes(instance, id)
            .unwrap_or_terminate("allow_writes cannot fail");
    }

    /// Sets `df_desc`'s as-of from a read hold on `id_bundle`, ships the dataflow, and drops the
    /// hold once compute has taken its own (compute puts in its own read holds during
    /// `create_dataflow`, so it is safe to release this one right after shipping).
    ///
    /// The read hold across shipping keeps the since of `id_bundle` from advancing underneath the
    /// as-of just picked.
    async fn ship_new_dataflow(
        &mut self,
        id_bundle: &CollectionIdBundle,
        mut df_desc: DataflowDescription<LirRelationExpr>,
        instance: ComputeInstanceId,
        notice_builtin_updates_fut: Option<BuiltinTableAppendNotify>,
    ) {
        let read_holds = self.acquire_read_holds(id_bundle);
        let since = read_holds.least_valid_read();
        df_desc.set_as_of(since.clone());
        self.ship_dataflow_and_notice_builtin_table_updates(
            df_desc,
            instance,
            notice_builtin_updates_fut,
            None,
        )
        .await;

        drop(read_holds);
    }

    /// Persist already-rendered optimizer notices for a newly created
    /// non-transient dataflow.
    ///
    /// Protected writers publish notices from committed plan selections instead.
    /// Installation must not overwrite that metadata or append those notices again.
    ///
    /// This:
    /// - packs builtin-table updates for `mz_optimizer_notices` (if enabled),
    /// - stores the rendered metainfo on the catalog object via
    ///   `set_dataflow_metainfo`,
    /// - and returns a future that resolves once the builtin-table append
    ///   has been observed, or `None` if nothing was appended.
    fn persist_dataflow_metainfo(
        &mut self,
        df_meta: DataflowMetainfo<Arc<OptimizerNotice>>,
        export_id: GlobalId,
    ) -> Option<BuiltinTableAppendNotify> {
        if self.catalog().state().catalog_read_protection_enabled() {
            return None;
        }
        // Attend to optimization notice builtin tables and save the metainfo in the catalog's
        // in-memory state.
        if self.catalog().state().system_config().enable_mz_notices()
            && !df_meta.optimizer_notices.is_empty()
        {
            let mut builtin_table_updates = Vec::with_capacity(df_meta.optimizer_notices.len());
            self.catalog().state().pack_optimizer_notices(
                &mut builtin_table_updates,
                df_meta.optimizer_notices.iter(),
                Diff::ONE,
            );

            // Save the metainfo.
            self.catalog_mut().set_dataflow_metainfo(export_id, df_meta);

            Some(self.builtin_table_update().execute(builtin_table_updates))
        } else {
            // Save the metainfo.
            self.catalog_mut().set_dataflow_metainfo(export_id, df_meta);

            None
        }
    }

    /// Like `ship_dataflow`, but also await on builtin table updates.
    pub(crate) async fn ship_dataflow_and_notice_builtin_table_updates(
        &mut self,
        dataflow: DataflowDescription<LirRelationExpr>,
        instance: ComputeInstanceId,
        notice_builtin_updates_fut: Option<BuiltinTableAppendNotify>,
        target_replica: Option<ReplicaId>,
    ) {
        if let Some(notice_builtin_updates_fut) = notice_builtin_updates_fut {
            let ship_dataflow_fut = self.ship_dataflow(dataflow, instance, target_replica);
            let ((), ()) =
                futures::future::join(notice_builtin_updates_fut, ship_dataflow_fut).await;
        } else {
            self.ship_dataflow(dataflow, instance, target_replica).await;
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
    ) -> Result<(), CollectionLookupError> {
        if let Some(client) = self.query_client.clone() {
            let catalog = self.owned_catalog();
            let mut compute_ids: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
            for id in objects {
                let cluster = catalog
                    .try_get_entry_by_global_id(&id)
                    .and_then(|entry| entry.item().cluster_id())
                    .ok_or(CollectionLookupError::CollectionMissing(id))?;
                compute_ids.entry(cluster).or_default().insert(id);
            }
            self.install_query_watch_set(conn_id, state, async move {
                client
                    .wait_for_progress(
                        &catalog,
                        &CollectionIdBundle {
                            storage_ids: BTreeSet::new(),
                            compute_ids,
                        },
                        t,
                    )
                    .await
            });
            return Ok(());
        }
        let ws_id = self.controller.install_compute_watch_set(objects, t)?;
        self.connection_watch_sets
            .entry(conn_id.clone())
            .or_default()
            .insert(ws_id);
        self.installed_watch_sets.insert(
            ws_id,
            InstalledWatchSet {
                conn_id,
                response: state,
                _execution: None,
            },
        );
        Ok(())
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
    ) -> Result<(), CollectionMissing> {
        if let Some(client) = self.query_client.clone() {
            let catalog = self.owned_catalog();
            for id in &objects {
                client
                    .collection_metadata(&catalog, *id)
                    .map_err(|_| CollectionMissing(*id))?;
            }
            self.install_query_watch_set(conn_id, state, async move {
                client
                    .wait_for_progress(
                        &catalog,
                        &CollectionIdBundle {
                            storage_ids: objects,
                            compute_ids: BTreeMap::new(),
                        },
                        t,
                    )
                    .await
            });
            return Ok(());
        }
        let ws_id = self.controller.install_storage_watch_set(objects, t)?;
        self.connection_watch_sets
            .entry(conn_id.clone())
            .or_default()
            .insert(ws_id);
        self.installed_watch_sets.insert(
            ws_id,
            InstalledWatchSet {
                conn_id,
                response: state,
                _execution: None,
            },
        );
        Ok(())
    }

    /// Owns a cancellable progress wait, including waits for initial observations.
    fn install_query_watch_set(
        &mut self,
        conn_id: ConnectionId,
        response: WatchSetResponse,
        future: impl std::future::Future<Output = Result<(), AdapterError>> + Send + 'static,
    ) {
        let id = self.query_watch_set_ids.allocate_id();
        let tx = self.internal_cmd_tx.clone();
        let execution = mz_ore::task::spawn(|| "query progress watch", async move {
            let result = future.await;
            let _ = tx.send(Message::QueryWatchSetReady(id, result));
        })
        .abort_on_drop();
        self.connection_watch_sets
            .entry(conn_id.clone())
            .or_default()
            .insert(id);
        self.installed_watch_sets.insert(
            id,
            InstalledWatchSet {
                conn_id,
                response,
                _execution: Some(execution),
            },
        );
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

    /// Retracts `mz_object_arrangement_size_history` rows older than the
    /// `arrangement_size_history_retention_period` dyncfg.
    ///
    /// Must only run at startup: it reads at the oracle read timestamp and
    /// writes retractions at the current write timestamp, which is only safe
    /// when no other writes are in flight. See [the equivalent storage-usage
    /// pruner](Self::prune_storage_usage_events_on_startup) for the same
    /// reasoning.
    async fn prune_arrangement_sizes_history_on_startup(&self) {
        // The catalog server is not writable in read-only mode.
        if self.controller.read_only() {
            return;
        }

        let retention_period = mz_adapter_types::dyncfgs::ARRANGEMENT_SIZE_HISTORY_RETENTION_PERIOD
            .get(self.catalog().system_config().dyncfgs());
        let item_id = self
            .catalog()
            .resolve_builtin_table(&mz_catalog::builtin::MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY);
        let global_id = self.catalog.get_entry(&item_id).latest_global_id();
        let read_ts = self.get_local_read_ts().await;
        let current_contents_fut = self
            .controller
            .storage_collections
            .snapshot(global_id, read_ts);
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        spawn(|| "arrangement_sizes_history_prune", async move {
            let mut current_contents = current_contents_fut
                .await
                .unwrap_or_terminate("cannot fail to fetch snapshot");
            differential_dataflow::consolidation::consolidate(&mut current_contents);

            let cutoff_ts = u128::from(read_ts).saturating_sub(retention_period.as_millis());
            let expired =
                arrangement_sizes_expired_retractions(current_contents, cutoff_ts, item_id);

            // TODO(arrangement-sizes): when the writeable-catalog-server
            // plumbing in https://github.com/MaterializeInc/materialize/pull/35436
            // lands, retract directly on `mz_catalog_server`.
            let _ = internal_cmd_tx.send(Message::ArrangementSizesPrune(expired));
        });
    }

    /// The environment's current credit consumption rate, summed over all user
    /// cluster replicas except those of `exclude_cluster`.
    fn current_credit_consumption_rate(&self, exclude_cluster: Option<ClusterId>) -> Numeric {
        self.catalog()
            .user_cluster_replicas()
            .filter(|replica| Some(replica.cluster_id) != exclude_cluster)
            .filter_map(|replica| match &replica.config.location {
                ReplicaLocation::Managed(location) => Some(self.replica_credits_per_hour(location)),
                ReplicaLocation::Unmanaged(_) => None,
            })
            .sum()
    }

    /// The credit rate of a managed replica, read from the size map by its billing size.
    ///
    /// An unknown billing size counts as free. DDL validates `SIZE` and `BILLED AS` against
    /// the map at replica creation, but the map is external configuration and can lose a
    /// size later. That case is a soft panic rather than a hard one, so that in production
    /// such a replica can still be dropped from SQL.
    fn replica_credits_per_hour(&self, location: &ManagedReplicaLocation) -> Numeric {
        let size = location.size_for_billing();
        match self.catalog().cluster_replica_sizes().0.get(size) {
            Some(allocation) => allocation.credits_per_hour,
            None => {
                soft_panic_or_log!(
                    "replica of size {:?} bills as unknown replica size {:?}, counting it as free",
                    location.size,
                    size,
                );
                Numeric::zero()
            }
        }
    }
}

/// Returns retraction updates for rows in a consolidated
/// `mz_object_arrangement_size_history` snapshot whose `collection_timestamp`
/// (column 3) is strictly before `cutoff_ts`.
///
/// Panics if any input row has `diff != 1`: the caller must consolidate first,
/// and a consolidated history table should never contain retractions because
/// the only source of retractions is this function itself.
fn arrangement_sizes_expired_retractions(
    rows: impl IntoIterator<Item = (mz_repr::Row, i64)>,
    cutoff_ts: u128,
    item_id: CatalogItemId,
) -> Vec<BuiltinTableUpdate> {
    let mut expired = Vec::new();
    for (row, diff) in rows {
        assert_eq!(
            diff, 1,
            "consolidated contents should not contain retractions: ({row:#?}, {diff:#?})"
        );
        let collection_timestamp = row
            .unpack()
            .get(3)
            .expect("definition of mz_object_arrangement_size_history changed")
            .unwrap_timestamptz()
            .timestamp_millis();
        let collection_timestamp: u128 = collection_timestamp
            .try_into()
            .expect("all collections happen after Jan 1 1970");
        if collection_timestamp < cutoff_ts {
            expired.push(BuiltinTableUpdate::row(item_id, row, Diff::MINUS_ONE));
        }
    }
    expired
}

#[cfg(test)]
impl Coordinator {
    #[allow(dead_code)]
    async fn verify_ship_dataflow_no_error(
        &mut self,
        dataflow: DataflowDescription<LirRelationExpr>,
    ) {
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
            .map(|stmt| truncate_sql_for_logging(stmt.to_ast_string_redacted()).into())
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
        client_protection_storage,
        compaction_bound_subscriber,
        timestamp_oracle_config,
        timestamp_oracle_now,
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

        let mut initial_timestamps =
            get_initial_oracle_timestamps(&timestamp_oracle_config).await?;

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
                timestamp_oracle_now.clone(),
                timestamp_oracle_config.clone(),
                &mut timestamp_oracles,
                read_only_controllers,
            )
            .await;
        }

        // Bootstrap also orders its table and builtin initialization beyond the
        // observed catalog prefix. Durable catalog writers complete their own
        // commits through the shared oracle.
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

        // The catalog shard's upper is durable, so a write that once landed far ahead of the
        // clock is re-applied to the oracle here on every boot and cannot be waited out. We
        // report it rather than refusing to start: the timeline is stalled either way, and a
        // process that will not boot turns that into a total outage plus a crash loop.
        let boot_now: mz_repr::Timestamp = (timestamp_oracle_now)().into();
        if catalog_upper > timeline::write_ts_upper_bound(&boot_now) {
            tracing::error!(
                %catalog_upper, %boot_now,
                "catalog upper is far ahead of the wall clock, so writes and \
                strict-serializable reads on the EpochMilliseconds timeline will block \
                until the clock catches up",
            );
        }

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
            last_seen_version,
            migrated_storage_collections_0dt,
            new_builtin_collections,
            mut builtin_table_updates,
            cached_global_exprs,
            uncached_local_exprs,
        } = Catalog::open(mz_catalog::config::Config {
            storage,
            metrics_registry: &metrics_registry,
            state: mz_catalog::config::StateConfig {
                unsafe_mode,
                all_features,
                build_info,
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

        // Whether replacement-migrated builtin MVs may write their new shards before cut-over.
        // Both `bootstrap` and the readiness gate below read this, and they have to agree.
        // `MIN_LEADER_VERSION_FOR_MIGRATED_MV_WRITES` explains why the leader's version settles it.
        //
        // While we are read-only, `last_seen_version` is that leader's version: our catalog
        // transaction is a savepoint, so our own bump of the setting never lands. `None` means a
        // freshly initialized catalog, with nothing migrated and no leader to be compatible with.
        //
        // `ENABLE_0DT_HYDRATE_MIGRATED_BUILTIN_MVS` is the break-glass revert: off falls back to
        // excluding migrated MVs from the caught-up gate, no redeploy needed.
        let hydrate_migrated_mvs = ENABLE_0DT_HYDRATE_MIGRATED_BUILTIN_MVS
            .get(catalog.system_config().dyncfgs())
            && last_seen_version
                .as_ref()
                .is_none_or(|version| *version >= MIN_LEADER_VERSION_FOR_MIGRATED_MV_WRITES);

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
        let advance_timelines_interval =
            tokio::time::interval(catalog.system_config().default_timestamp_interval());

        let clusters_caught_up_check =
            clusters_caught_up_trigger.map(|trigger| {
                let mut exclude_collections: BTreeSet<GlobalId> =
                    new_builtin_collections.iter().copied().collect();

                // A collection that can't advance its write frontier in read-only mode
                // stalls its transitive dependents too, so exclude those from the caught-up
                // check as well. That's every *new* builtin collection, whose fresh shard has no
                // writer until this deployment promotes, plus migrated MVs whenever the leader is
                // too old for them to write. An excluded dependent may still be hydrating right
                // after promotion, a brief blip we accept because these collections are small and
                // get a writer at cut-over.
                //
                // Seeded from all of `new_builtin_collections`, not just the MVs: a new builtin
                // table or source has no read-only writer either (bootstrap registration
                // retains only *migrated* tables), so an MV reading one never advances past its
                // empty frontier. A *migrated* table is the opposite case, even though a builtin
                // MV can read one (`mz_clusters` joins `mz_cluster_replica_size_internal`):
                // `read_only_mode_table_worker` keeps advancing migrated tables' uppers.
                let new_builtin_items = new_builtin_collections.iter().map(|global_id| {
                    catalog
                        .state()
                        .try_get_entry_by_global_id(global_id)
                        .expect("new builtin collections have catalog entries")
                        .id()
                });
                let frozen_migrated_mvs = migrated_storage_collections_0dt
                    .iter()
                    .copied()
                    .filter(|_| !hydrate_migrated_mvs)
                    .filter(|id| catalog.state().get_entry(id).is_materialized_view());
                let mut todo: Vec<_> = new_builtin_items.chain(frozen_migrated_mvs).collect();
                while let Some(item_id) = todo.pop() {
                    let entry = catalog.state().get_entry(&item_id);
                    exclude_collections.extend(entry.global_ids());
                    todo.extend_from_slice(entry.used_by());
                }

                CaughtUpCheckContext {
                    trigger,
                    exclude_collections,
                }
            });

        if let Some(TimestampOracleConfig::Postgres(pg_config)) =
            timestamp_oracle_config.as_ref()
        {
            // Apply settings from system vars as early as possible because some
            // of them are locked in right when an oracle is first opened!
            let pg_timestamp_oracle_params =
                flags::timestamp_oracle_config(catalog.system_config());
            pg_timestamp_oracle_params.apply(pg_config);
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
        let query_orchestrator = controller_config.orchestrator.namespace("cluster");
        let query_deploy_generation = controller_config.deploy_generation;
        let query_persist_location = controller_config.persist_location.clone();
        let adapter_storage =
            mz_controller::AdapterStorageWriter::new(read_only_controllers, now.clone());
        let storage_configuration = mz_storage_types::configuration::StorageConfiguration::new(
            controller_config.connection_context.clone(),
            catalog.system_config().dyncfgs().clone(),
        );

        let parent_span = tracing::Span::current();
        let thread = thread::Builder::new()
            // The Coordinator thread tends to keep a lot of data on its stack. To
            // prevent a stack overflow we allocate a stack three times as big as the default
            // stack.
            .stack_size(3 * stack::STACK_SIZE)
            .name("coordinator".to_string())
            .spawn(move || {
                let span = info_span!(parent: parent_span, "coord::coordinator").entered();

                let client_protection_catalog = client_protection_storage.map(|storage| {
                    handle.block_on(catalog.writer_projection(storage))
                        .unwrap_or_terminate("failed to acquire client protection projection")
                });
                let (table_write_handle, txns_metrics) = handle.block_on(
                    catalog.initialize_table_writer(
                        persist_client.clone(), &controller_config.metrics_registry,
                        read_only_controllers,
                    ),
                ).unwrap_or_terminate("failed to initialize adapter table writer");
                let (controller, storage_builtin_updates) = handle
                    .block_on({
                        catalog.initialize_controller(
                            controller_config,
                            controller_envd_epoch,
                            read_only_controllers,
                            txns_metrics,
                        )
                    })
                    .unwrap_or_terminate("failed to initialize storage_controller");
                builtin_table_updates.extend(storage_builtin_updates);
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
                let max_concurrent_occ_writes =
                    usize::cast_from(catalog.system_config().max_concurrent_occ_writes());

                let caching_secrets_reader = CachingSecretsReader::new(secrets_controller.reader());
                let (group_committer_tx, group_committer_rx) = mpsc::unbounded_channel();
                let mut coord = Coordinator {
                    controller,
                    table_write_handle,
                    adapter_storage,
                    storage_configuration,
                    catalog,
                    compaction_bound_subscriber: compaction_bound_subscriber
                        .map(CompactionBoundSubscriber::new),
                    read_protection_pending: BTreeSet::new(),
                    query_client: None,
                    client_protection_catalog,
                    client_protection_reclaimer: Default::default(),
                    query_persist_location,
                    query_orchestrator,
                    query_deploy_generation,
                    internal_cmd_tx,
                    group_commit_tx,
                    reconcile_now: Arc::new(Notify::new()),
                    group_committer_tx,
                    strict_serializable_reads_tx,
                    linearize_reads_notify: Arc::new(Notify::new()),
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
                    connection_cancel_watches: BTreeMap::new(),
                    introspection_subscribes: BTreeMap::new(),
                    hydration_history_replica_cursor: None,
                    hydration_history_sweep: None,
                    metric_sinks: BTreeMap::new(),
                    metric_sink_plans: BTreeMap::new(),
                    pending_compute_installations: BTreeSet::new(),
                    pending_compute_installation_retry: None,
                    deferred_plans: BTreeMap::new(),
                    pending_writes: Vec::new(),
                    occ_write_semaphore: Arc::new(Semaphore::new(max_concurrent_occ_writes)),
                    advance_timelines_interval,
                    native_frontiers: Default::default(),
                    secrets_controller,
                    caching_secrets_reader,
                    cloud_resource_controller,
                    storage_usage_client,
                    storage_usage_collection_interval,
                    segment_client,
                    metrics,
                    catalog_info_metrics_registry: metrics_registry.clone(),
                    scoped_frontend: None,
                    optimizer_metrics,
                    tracing_handle,
                    statement_logging: StatementLogging::new(coord_now.clone()),
                    webhook_concurrency_limit,
                    timestamp_oracle_config,
                    timestamp_oracle_now,
                    caught_up_check: clusters_caught_up_check,
                    installed_watch_sets: BTreeMap::new(),
                    query_watch_set_ids: Default::default(),
                    connection_watch_sets: BTreeMap::new(),
                    cluster_replica_statuses: ClusterReplicaStatuses::new(),
                    read_only_controllers,
                    buffered_builtin_table_updates: Some(Vec::new()),
                    license_key,
                    user_id_pool: IdPool::empty(),
                    persist_client,
                };

                // Read-only promotion restarts the process and creates a fresh committer.
                handle.block_on(async {
                    appends::spawn_group_committer(
                        group_committer_rx,
                        coord.get_local_timestamp_oracle(),
                        Arc::clone(&coord.table_write_handle),
                        coord.catalog().upper_handle(),
                        coord.internal_cmd_tx.clone(),
                        coord.catalog().config().now.clone(),
                        coord.timestamp_oracle_now.clone(),
                        coord.metrics.clone(),
                        coord.catalog().system_config().dyncfgs(),
                    );
                });

                let bootstrap = handle.block_on(async {
                    let prepared_client = coord
                        .bootstrap(
                            boot_ts,
                            migrated_storage_collections_0dt,
                            hydrate_migrated_mvs,
                            builtin_table_updates,
                            cached_global_exprs,
                            uncached_local_exprs,
                        )
                        .await?;
                    if coord.catalog().state().catalog_read_protection_enabled() {
                        if !read_only_controllers {
                            let observed = coord.controller.list_replica_services().await
                                .map_err(AdapterError::Orchestrator)?;
                            // Writable creation commits before provisioning. Read
                            // membership after listing, not from bootstrap inventory.
                            let live = coord.catalog().committed_cluster_replicas().await?;
                            coord.controller
                                .remove_orphaned_replicas_from_snapshot(observed, live)
                                .map_err(AdapterError::Orchestrator)?;
                        }
                        // A prewarming savepoint can provision private replicas.
                        // Durable membership is not authority to delete them.
                    } else {
                        coord.controller.remove_orphaned_replicas(
                            coord.catalog().get_next_user_replica_id().await?,
                            coord.catalog().get_next_system_replica_id().await?,
                        ).await.map_err(AdapterError::Orchestrator)?;
                    }

                    if let Some(retention_period) = storage_usage_retention_period {
                        coord
                            .prune_storage_usage_events_on_startup(retention_period)
                            .await;
                    }

                    coord.prune_arrangement_sizes_history_on_startup().await;
                    coord.initialize_query_client(prepared_client).await?;
                    if coord.controller.replica_owned_compute() {
                        // These observations execute through the query client.
                        // Their admission must not delay storage/WAL bootstrap.
                        coord.bootstrap_introspection_subscribes().await;
                        coord.bootstrap_metric_sinks().await;
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
                    cmd_tx,
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
    timestamp_oracle_config: &Option<TimestampOracleConfig>,
) -> Result<BTreeMap<Timeline, Timestamp>, AdapterError> {
    let mut initial_timestamps = BTreeMap::new();

    if let Some(config) = timestamp_oracle_config {
        let oracle_timestamps = config.get_all_timelines().await?;

        let debug_msg = || {
            oracle_timestamps
                .iter()
                .map(|(timeline, ts)| format!("{:?} -> {}", timeline, ts))
                .join(", ")
        };
        info!(
            "current timestamps from the timestamp oracle: {}",
            debug_msg()
        );

        for (timeline, ts) in oracle_timestamps {
            let entry = initial_timestamps
                .entry(Timeline::from_str(&timeline).expect("could not parse timeline"));

            entry
                .and_modify(|current_ts| *current_ts = std::cmp::max(*current_ts, ts))
                .or_insert(ts);
        }
    } else {
        info!("no timestamp oracle configured!");
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
struct InstalledWatchSet {
    conn_id: ConnectionId,
    response: WatchSetResponse,
    _execution: Option<AbortOnDropHandle<()>>,
}

#[derive(Debug)]
pub enum WatchSetResponse {
    StatementDependenciesReady(StatementLoggingId, StatementLifecycleEvent),
    AlterSinkReady(AlterSinkReadyContext),
    AlterMaterializedViewReady(AlterMaterializedViewReadyContext),
}

#[derive(Debug)]
pub struct AlterSinkReadyContext {
    ctx: Option<ExecuteContext>,
    otel_ctx: OpenTelemetryContext,
    plan: AlterSinkPlan,
    plan_validity: PlanValidity,
    read_hold: ReadHolds,
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

#[derive(Debug)]
pub struct AlterMaterializedViewReadyContext {
    ctx: Option<ExecuteContext>,
    otel_ctx: OpenTelemetryContext,
    plan: plan::AlterMaterializedViewApplyReplacementPlan,
    plan_validity: PlanValidity,
    prepared: Option<ddl::PreparedCatalogTransaction>,
}

impl AlterMaterializedViewReadyContext {
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

impl Drop for AlterMaterializedViewReadyContext {
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
        sql_impl_resolved_ids: ResolvedIds,
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

pub(crate) use mz_catalog::optimize::infer_sql_type_for_catalog;

#[cfg(test)]
mod execute_context_tests {
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::session::Session;
    use crate::util::ClientTransmitter;

    #[mz_ore::test(tokio::test)]
    async fn replan_preserves_admission_logging_and_response_barriers() {
        let (client_tx, mut client_rx) = oneshot::channel();
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel();
        let (release, barrier) = oneshot::channel::<()>();
        let logging_id = StatementLoggingId(Uuid::new_v4());
        let mut session = Session::dummy();
        session.start_transaction_single_stmt(mz_ore::now::to_datetime(0));
        let session_id = session.uuid();
        let mut ctx = ExecuteContext::from_parts_with_response_barriers(
            ClientTransmitter::new(client_tx, internal_tx.clone()),
            internal_tx.clone(),
            session,
            ExecuteContextGuard::new(Some(logging_id), internal_tx),
            vec![Box::pin(async move {
                barrier.await.expect("release barrier");
            })],
        );
        ctx.statement_deadline = Some(Instant::now() + Duration::from_secs(60));
        let statement = mz_sql_parser::parser::parse_statements("SUBSCRIBE (SELECT 1)")
            .expect("valid subscribe")
            .remove(0)
            .ast;
        ctx.query_replan = Some(Arc::new((Arc::new(statement), Params::empty())));
        ctx.admit_subscribe()
            .expect("initial transaction admission");
        let deadline = ctx.statement_deadline();
        StagedContext::handle_error(ctx, AdapterError::CatalogSnapshotChanged);

        let Message::ExecuteReplan(mut ctx) = internal_rx.try_recv().expect("replan request")
        else {
            panic!("planning invalidation must retain the execution");
        };
        assert_eq!(ctx.session().uuid(), session_id);
        assert_eq!(ctx.extra().contents(), Some(logging_id));
        assert_eq!(ctx.statement_deadline(), deadline);
        ctx.admit_subscribe()
            .expect("same statement must not repeat transaction admission");
        assert!(
            internal_rx.try_recv().is_err(),
            "replanning must not end logging"
        );
        assert!(matches!(
            client_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        ctx.retire(Err(AdapterError::Canceled));
        tokio::task::yield_now().await;
        assert!(matches!(
            client_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        release.send(()).expect("barrier retained until retirement");
        let response = client_rx.await.expect("final client response");
        assert!(matches!(response.result, Err(AdapterError::Canceled)));
        let Message::RetireExecute { data, .. } =
            internal_rx.recv().await.expect("retirement event")
        else {
            panic!("final retirement must end the original log entry");
        };
        assert_eq!(data.contents(), Some(logging_id));
        assert!(internal_rx.try_recv().is_err());
    }

    /// Runtime shutdown drops the barrier-waiting task that `retire` spawns. The context's `Drop`
    /// backstop must answer the client, rather than panicking on an unsent `ClientTransmitter`.
    #[mz_ore::test]
    fn test_retire_answers_client_when_runtime_shuts_down() {
        let runtime = tokio::runtime::Runtime::new().expect("can build runtime");

        let (client_tx, mut client_rx) = oneshot::channel();
        let (internal_cmd_tx, _internal_cmd_rx) = mpsc::unbounded_channel();

        runtime.block_on(async {
            let ctx = ExecuteContext::from_parts_with_response_barriers(
                ClientTransmitter::new(client_tx, internal_cmd_tx.clone()),
                internal_cmd_tx,
                Session::dummy(),
                ExecuteContextGuard::default(),
                // Stands in for a group commit that shutdown will never apply.
                vec![Box::pin(std::future::pending())],
            );
            ctx.retire(Ok(ExecuteResponse::StartedTransaction));
        });

        drop(runtime);

        let response = client_rx.try_recv().expect("client must be answered");
        assert!(
            matches!(response.result, Err(AdapterError::Internal(_))),
            "expected an internal error, got {:?}",
            response.result
        );
    }
}

#[cfg(test)]
mod id_pool_tests {
    use super::IdPool;

    #[mz_ore::test]
    fn test_empty_pool() {
        let mut pool = IdPool::empty();
        assert_eq!(pool.remaining(), 0);
        assert_eq!(pool.allocate(), None);
        assert_eq!(pool.allocate_many(1), None);
    }

    #[mz_ore::test]
    fn test_allocate_single() {
        let mut pool = IdPool::empty();
        pool.refill(10, 13);
        assert_eq!(pool.remaining(), 3);
        assert_eq!(pool.allocate(), Some(10));
        assert_eq!(pool.allocate(), Some(11));
        assert_eq!(pool.allocate(), Some(12));
        assert_eq!(pool.remaining(), 0);
        assert_eq!(pool.allocate(), None);
    }

    #[mz_ore::test]
    fn test_allocate_many() {
        let mut pool = IdPool::empty();
        pool.refill(100, 105);
        assert_eq!(pool.allocate_many(3), Some(vec![100, 101, 102]));
        assert_eq!(pool.remaining(), 2);
        // Not enough remaining for 3 more.
        assert_eq!(pool.allocate_many(3), None);
        // But 2 works.
        assert_eq!(pool.allocate_many(2), Some(vec![103, 104]));
        assert_eq!(pool.remaining(), 0);
    }

    #[mz_ore::test]
    fn test_allocate_many_zero() {
        let mut pool = IdPool::empty();
        pool.refill(1, 5);
        assert_eq!(pool.allocate_many(0), Some(vec![]));
        assert_eq!(pool.remaining(), 4);
    }

    #[mz_ore::test]
    fn test_refill_resets_pool() {
        let mut pool = IdPool::empty();
        pool.refill(0, 2);
        assert_eq!(pool.allocate(), Some(0));
        // Refill before exhaustion replaces the range.
        pool.refill(50, 52);
        assert_eq!(pool.allocate(), Some(50));
        assert_eq!(pool.allocate(), Some(51));
        assert_eq!(pool.allocate(), None);
    }

    #[mz_ore::test]
    fn test_mixed_allocate_and_allocate_many() {
        let mut pool = IdPool::empty();
        pool.refill(0, 10);
        assert_eq!(pool.allocate(), Some(0));
        assert_eq!(pool.allocate_many(3), Some(vec![1, 2, 3]));
        assert_eq!(pool.allocate(), Some(4));
        assert_eq!(pool.remaining(), 5);
    }

    #[mz_ore::test]
    #[should_panic(expected = "invalid pool range")]
    fn test_refill_invalid_range_panics() {
        let mut pool = IdPool::empty();
        pool.refill(10, 5);
    }
}

#[cfg(test)]
mod arrangement_sizes_pruner_tests {
    use itertools::Itertools;
    use mz_repr::catalog_item_id::CatalogItemId;
    use mz_repr::{Datum, Row};

    use super::arrangement_sizes_expired_retractions;

    // History cleanup must preserve the full stored row, including provenance.
    fn history_row(ts_ms: i64) -> Row {
        let dt = mz_ore::now::to_datetime(ts_ms.try_into().expect("non-negative"));
        Row::pack_slice(&[
            Datum::String("r1"),
            Datum::String("u1"),
            Datum::Int64(123),
            Datum::TimestampTz(dt.try_into().expect("fits in TimestampTz")),
            Datum::True,
            Datum::UInt64(8),
        ])
    }

    fn item_id() -> CatalogItemId {
        // Any CatalogItemId will do; tests don't dispatch on it.
        CatalogItemId::User(42)
    }

    #[mz_ore::test]
    fn empty_input_produces_no_retractions() {
        let out = arrangement_sizes_expired_retractions(Vec::new(), 1_000, item_id());
        assert!(out.is_empty());
    }

    #[mz_ore::test]
    fn retracts_only_rows_strictly_before_cutoff() {
        // Mixes both sides of the filter and includes a row at exactly
        // the cutoff timestamp to pin down the strict-less-than boundary.
        let rows = vec![
            (history_row(100), 1),
            (history_row(500), 1),
            (history_row(1_000), 1), // at cutoff: kept (strict <)
            (history_row(5_000), 1),
        ];
        let out = arrangement_sizes_expired_retractions(rows, 1_000, item_id());
        assert_eq!(out.len(), 2);
        for (update, timestamp) in out.into_iter().zip_eq([100, 500]) {
            let mz_storage_client::client::TableData::Rows(rows) = update.data else {
                panic!("history cleanup must retract stored rows");
            };
            assert_eq!(
                rows,
                vec![(history_row(timestamp), mz_repr::Diff::MINUS_ONE)]
            );
        }
    }

    #[mz_ore::test]
    #[should_panic(expected = "consolidated contents should not contain retractions")]
    fn retraction_in_input_panics() {
        let rows = vec![(history_row(100), -1)];
        let _ = arrangement_sizes_expired_retractions(rows, 1_000, item_id());
    }
}
