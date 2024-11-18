// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{RelationDesc, ScalarType};
use std::sync::LazyLock;

pub static MZ_PREPARED_STATEMENT_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("id", ScalarType::Uuid.nullable(false))
        .with_column("session_id", ScalarType::Uuid.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("sql_hash", ScalarType::Bytes.nullable(false))
        .with_column(
            "prepared_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("statement_type", ScalarType::String.nullable(true))
        .with_column("throttled_count", ScalarType::UInt64.nullable(false))
        .finish()
});

pub static MZ_SQL_TEXT_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column(
            "prepared_day",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("sql_hash", ScalarType::Bytes.nullable(false))
        .with_column("sql", ScalarType::String.nullable(false))
        .with_column("redacted_sql", ScalarType::String.nullable(false))
        .finish()
});

pub static MZ_SESSION_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("session_id", ScalarType::Uuid.nullable(false))
        .with_column(
            "connected_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "initial_application_name",
            ScalarType::String.nullable(false),
        )
        .with_column("authenticated_user", ScalarType::String.nullable(false))
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
        .with_column("id", ScalarType::Uuid.nullable(false))
        .with_column("prepared_statement_id", ScalarType::Uuid.nullable(false))
        .with_column("sample_rate", ScalarType::Float64.nullable(false))
        .with_column("cluster_id", ScalarType::String.nullable(true))
        .with_column("application_name", ScalarType::String.nullable(false))
        .with_column("cluster_name", ScalarType::String.nullable(true))
        .with_column("database_name", ScalarType::String.nullable(false))
        .with_column(
            "search_path",
            ScalarType::List {
                element_type: Box::new(ScalarType::String),
                custom_id: None,
            }
            .nullable(false),
        )
        .with_column("transaction_isolation", ScalarType::String.nullable(false))
        // Note that this can't be a timestamp, as it might be u64::max,
        // which is out of range.
        .with_column("execution_timestamp", ScalarType::UInt64.nullable(true))
        .with_column("transaction_id", ScalarType::UInt64.nullable(false))
        .with_column("transient_index_id", ScalarType::String.nullable(true))
        .with_column(
            "params",
            ScalarType::Array(Box::new(ScalarType::String)).nullable(false),
        )
        .with_column("mz_version", ScalarType::String.nullable(false))
        .with_column(
            "began_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "finished_at",
            ScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column("finished_status", ScalarType::String.nullable(true))
        .with_column("error_message", ScalarType::String.nullable(true))
        .with_column("result_size", ScalarType::Int64.nullable(true))
        .with_column("rows_returned", ScalarType::Int64.nullable(true))
        .with_column("execution_strategy", ScalarType::String.nullable(true))
        .finish()
});

pub static MZ_SOURCE_STATUS_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column(
            "occurred_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("source_id", ScalarType::String.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("error", ScalarType::String.nullable(true))
        .with_column("details", ScalarType::Jsonb.nullable(true))
        .finish()
});

pub static MZ_SINK_STATUS_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column(
            "occurred_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("sink_id", ScalarType::String.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("error", ScalarType::String.nullable(true))
        .with_column("details", ScalarType::Jsonb.nullable(true))
        .finish()
});

pub static MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC: LazyLock<RelationDesc> =
    LazyLock::new(|| {
        RelationDesc::builder()
            .with_column(
                "occurred_at",
                ScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("connection_id", ScalarType::String.nullable(false))
            .with_column("status", ScalarType::String.nullable(false))
            .finish()
    });

pub static REPLICA_STATUS_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("replica_id", ScalarType::String.nullable(false))
        .with_column("process_id", ScalarType::UInt64.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("reason", ScalarType::String.nullable(true))
        .with_column(
            "occurred_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish()
});

pub static REPLICA_METRICS_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("replica_id", ScalarType::String.nullable(false))
        .with_column("process_id", ScalarType::UInt64.nullable(false))
        .with_column("cpu_nano_cores", ScalarType::UInt64.nullable(true))
        .with_column("memory_bytes", ScalarType::UInt64.nullable(true))
        .with_column("disk_bytes", ScalarType::UInt64.nullable(true))
        .with_column(
            "occurred_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish()
});

pub static WALLCLOCK_LAG_HISTORY_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("object_id", ScalarType::String.nullable(false))
        .with_column("replica_id", ScalarType::String.nullable(true))
        .with_column("lag", ScalarType::Interval.nullable(false))
        .with_column(
            "occurred_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish()
});
