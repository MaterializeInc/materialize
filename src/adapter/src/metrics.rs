// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use prometheus::{HistogramVec, IntCounterVec, IntGaugeVec};

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_sql::ast::{AstInfo, Statement, StatementKind};

use crate::session::Session;

#[derive(Debug, Clone)]
pub struct Metrics {
    pub query_total: IntCounterVec,
    pub active_sessions: IntGaugeVec,
    pub active_subscribes: IntGaugeVec,
    pub queue_busy_seconds: HistogramVec,
    pub determine_timestamp: IntCounterVec,
    pub commands: IntCounterVec,
}

impl Metrics {
    pub(crate) fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            query_total: registry.register(metric!(
                name: "mz_query_total",
                help: "The total number of queries issued of the given type since process start.",
                var_labels: ["session_type", "statement_type"],
            )),
            active_sessions: registry.register(metric!(
                name: "mz_active_sessions",
                help: "The number of active coordinator sessions.",
                var_labels: ["session_type"],
            )),
            active_subscribes: registry.register(metric!(
                name: "mz_active_subscribes",
                help: "The number of active SUBSCRIBE queries.",
                var_labels: ["session_type"],
            )),
            queue_busy_seconds: registry.register(metric!(
                name: "mz_coord_queue_busy_seconds",
                help: "The number of seconds the coord queue was processing before it was empty. This is a sampled metric and does not measure the full coord queue wait/idle times.",
            )),
            determine_timestamp: registry.register(metric!(
                name: "mz_determine_timestamp",
                help: "The total number of calls to determine_timestamp.",
                var_labels:["respond_immediately", "isolation_level", "compute_instance"],
            )),
            commands: registry.register(metric!(
                name: "mz_adapter_commands",
                help: "The total number of adapter commands issued of the given type since process start.",
                var_labels: ["command_type", "status"],
            )),
        }
    }
}

pub(crate) fn session_type_label_value(session: &Session) -> &'static str {
    match session.is_system() {
        true => "system",
        false => "user",
    }
}

pub(crate) fn statement_type_label_value<T>(stmt: &Statement<T>) -> &'static str
where
    T: AstInfo,
{
    let kind = StatementKind::from(stmt);
    match kind {
        StatementKind::Select => "select",
        StatementKind::Insert => "insert",
        StatementKind::Copy => "copy",
        StatementKind::Update => "update",
        StatementKind::Delete => "delete",
        StatementKind::CreateConnection => "create_connection",
        StatementKind::CreateDatabase => "create_database",
        StatementKind::CreateSchema => "create_schema",
        StatementKind::CreateSource => "create_source",
        StatementKind::CreateSubsource => "create_subsource",
        StatementKind::CreateSink => "create_sink",
        StatementKind::CreateView => "create_view",
        StatementKind::CreateMaterializedView => "create_materialized_view",
        StatementKind::CreateTable => "create_table",
        StatementKind::CreateIndex => "create_index",
        StatementKind::CreateType => "create_type",
        StatementKind::CreateRole => "create_role",
        StatementKind::CreateCluster => "create_cluster",
        StatementKind::CreateClusterReplica => "create_cluster_replica",
        StatementKind::CreateSecret => "create_secret",
        StatementKind::AlterObjectRename => "alter_object_rename",
        StatementKind::AlterIndex => "alter_index",
        StatementKind::AlterSecret => "alter_secret",
        StatementKind::AlterSink => "alter_sink",
        StatementKind::AlterSource => "alter_source",
        StatementKind::AlterSystemSet => "alter_system_set",
        StatementKind::AlterSystemReset => "alter_system_reset",
        StatementKind::AlterSystemResetAll => "alter_system_reset_all",
        StatementKind::AlterConnection => "alter_connection",
        StatementKind::Discard => "discard",
        StatementKind::DropDatabase => "drop_database",
        StatementKind::DropSchema => "drop_schema",
        StatementKind::DropObjects => "drop_objects",
        StatementKind::DropRoles => "drop_roles",
        StatementKind::DropClusters => "drop_clusters",
        StatementKind::DropClusterReplicas => "drop_cluster_replicas",
        StatementKind::SetVariable => "set_variable",
        StatementKind::ResetVariable => "reset_variable",
        StatementKind::Show => "show",
        StatementKind::StartTransaction => "start_transaction",
        StatementKind::SetTransaction => "set_transaction",
        StatementKind::Commit => "commit",
        StatementKind::Rollback => "rollback",
        StatementKind::Subscribe => "subscribe",
        StatementKind::Explain => "explain",
        StatementKind::Declare => "declare",
        StatementKind::Fetch => "fetch",
        StatementKind::Close => "close",
        StatementKind::Prepare => "prepare",
        StatementKind::Execute => "execute",
        StatementKind::Deallocate => "deallocate",
        StatementKind::Raise => "raise",
    }
}
