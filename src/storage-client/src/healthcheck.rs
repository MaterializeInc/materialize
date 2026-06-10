// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{RelationDesc, SqlScalarType};
use std::sync::LazyLock;

pub static MZ_PREPARED_STATEMENT_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("id", SqlScalarType::Uuid.nullable(false))
        .with_column("session_id", SqlScalarType::Uuid.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("sql_hash", SqlScalarType::Bytes.nullable(false))
        .with_column(
            "prepared_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("statement_type", SqlScalarType::String.nullable(true))
        .with_column("throttled_count", SqlScalarType::UInt64.nullable(false))
        .finish()
});

pub static MZ_SQL_TEXT_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column(
            "prepared_day",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("sql_hash", SqlScalarType::Bytes.nullable(false))
        .with_column("sql", SqlScalarType::String.nullable(false))
        .with_column("redacted_sql", SqlScalarType::String.nullable(false))
        .finish()
});

pub static MZ_SESSION_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("session_id", SqlScalarType::Uuid.nullable(false))
        .with_column(
            "connected_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "initial_application_name",
            SqlScalarType::String.nullable(false),
        )
        .with_column("authenticated_user", SqlScalarType::String.nullable(false))
        .finish()
});

// NOTE: Update the views `mz_statement_execution_history_redacted`
// and `mz_activity_log`, and `mz_activity_log_redacted` whenever this
// is updated, to include the new columns where appropriate.
//
// The `redacted` views should contain only those columns that should
// be queryable by support.
pub static MZ_STATEMENT_EXECUTION_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("id", SqlScalarType::Uuid.nullable(false))
        .with_column("prepared_statement_id", SqlScalarType::Uuid.nullable(false))
        .with_column("sample_rate", SqlScalarType::Float64.nullable(false))
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
        .with_column("application_name", SqlScalarType::String.nullable(false))
        .with_column("cluster_name", SqlScalarType::String.nullable(true))
        .with_column("database_name", SqlScalarType::String.nullable(false))
        .with_column(
            "search_path",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::String),
                custom_id: None,
            }
            .nullable(false),
        )
        .with_column(
            "transaction_isolation",
            SqlScalarType::String.nullable(false),
        )
        // Note that this can't be a timestamp, as it might be u64::max,
        // which is out of range.
        .with_column("execution_timestamp", SqlScalarType::UInt64.nullable(true))
        .with_column("transaction_id", SqlScalarType::UInt64.nullable(false))
        .with_column("transient_index_id", SqlScalarType::String.nullable(true))
        .with_column(
            "params",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_column("mz_version", SqlScalarType::String.nullable(false))
        .with_column(
            "began_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "finished_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column("finished_status", SqlScalarType::String.nullable(true))
        .with_column("error_message", SqlScalarType::String.nullable(true))
        .with_column("result_size", SqlScalarType::Int64.nullable(true))
        .with_column("rows_returned", SqlScalarType::Int64.nullable(true))
        .with_column("execution_strategy", SqlScalarType::String.nullable(true))
        .finish()
});

pub static MZ_SOURCE_STATUS_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column(
            "occurred_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("source_id", SqlScalarType::String.nullable(false))
        .with_column("status", SqlScalarType::String.nullable(false))
        .with_column("error", SqlScalarType::String.nullable(true))
        .with_column("details", SqlScalarType::Jsonb.nullable(true))
        .with_column("replica_id", SqlScalarType::String.nullable(true))
        .finish()
});

pub static MZ_SINK_STATUS_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column(
            "occurred_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("sink_id", SqlScalarType::String.nullable(false))
        .with_column("status", SqlScalarType::String.nullable(false))
        .with_column("error", SqlScalarType::String.nullable(true))
        .with_column("details", SqlScalarType::Jsonb.nullable(true))
        .with_column("replica_id", SqlScalarType::String.nullable(true))
        .finish()
});

pub static MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC: LazyLock<RelationDesc> =
    LazyLock::new(|| {
        RelationDesc::builder()
            .with_column(
                "occurred_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("connection_id", SqlScalarType::String.nullable(false))
            .with_column("status", SqlScalarType::String.nullable(false))
            .finish()
    });

pub static REPLICA_STATUS_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("replica_id", SqlScalarType::String.nullable(false))
        .with_column("process_id", SqlScalarType::UInt64.nullable(false))
        .with_column("status", SqlScalarType::String.nullable(false))
        .with_column("reason", SqlScalarType::String.nullable(true))
        .with_column(
            "occurred_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish()
});

/// NOTE: We want to avoid breaking schema changes, as those would cause the builtin migrations to
/// drop all existing data. For details on what changes are compatible, see
/// [`mz_persist_types::schema::backward_compatible`].
pub static REPLICA_METRICS_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("replica_id", SqlScalarType::String.nullable(false))
        .with_column("process_id", SqlScalarType::UInt64.nullable(false))
        .with_column("cpu_nano_cores", SqlScalarType::UInt64.nullable(true))
        .with_column("memory_bytes", SqlScalarType::UInt64.nullable(true))
        .with_column("disk_bytes", SqlScalarType::UInt64.nullable(true))
        .with_column(
            "occurred_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("heap_bytes", SqlScalarType::UInt64.nullable(true))
        .with_column("heap_limit", SqlScalarType::UInt64.nullable(true))
        .finish()
});

pub static WALLCLOCK_LAG_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("replica_id", SqlScalarType::String.nullable(true))
        .with_column("lag", SqlScalarType::Interval.nullable(true))
        .with_column(
            "occurred_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish()
});

pub static WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column(
            "period_start",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "period_end",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("lag_seconds", SqlScalarType::UInt64.nullable(true))
        .with_column("labels", SqlScalarType::Jsonb.nullable(false))
        .finish()
});
