// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{RelationDesc, ScalarType};
use once_cell::sync::Lazy;

pub static MZ_PREPARED_STATEMENT_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
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
});

pub static MZ_SQL_TEXT_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(
            "prepared_day",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("sql_hash", ScalarType::Bytes.nullable(false))
        .with_column("sql", ScalarType::String.nullable(false))
        .with_column("redacted_sql", ScalarType::String.nullable(false))
});

pub static MZ_SESSION_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column("id", ScalarType::Uuid.nullable(false))
        .with_column(
            "connected_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "initial_application_name",
            ScalarType::String.nullable(false),
        )
        .with_column("authenticated_user", ScalarType::String.nullable(false))
});

// NOTE: Update the views `mz_statement_execution_history_redacted`
// and `mz_activity_log`, and `mz_activity_log_redacted` whenever this
// is updated, to include the new columns where appropriate.
//
// The `redacted` views should contain only those columns that should
// be queryable by support.
pub static MZ_STATEMENT_EXECUTION_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column("id", ScalarType::Uuid.nullable(false))
        .with_column("prepared_statement_id", ScalarType::Uuid.nullable(false))
        .with_column("sample_rate", ScalarType::Float64.nullable(false))
        .with_column("cluster_id", ScalarType::String.nullable(true))
        .with_column("application_name", ScalarType::String.nullable(false))
        .with_column("cluster_name", ScalarType::String.nullable(true))
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
        .with_column("rows_returned", ScalarType::Int64.nullable(true))
        .with_column("execution_strategy", ScalarType::String.nullable(true))
});

pub static MZ_SOURCE_STATUS_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(
            "occurred_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("source_id", ScalarType::String.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("error", ScalarType::String.nullable(true))
        .with_column("details", ScalarType::Jsonb.nullable(true))
});

pub static MZ_SINK_STATUS_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(
            "occurred_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("sink_id", ScalarType::String.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("error", ScalarType::String.nullable(true))
        .with_column("details", ScalarType::Jsonb.nullable(true))
});

pub static MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC: Lazy<RelationDesc> =
    Lazy::new(|| {
        RelationDesc::empty()
            .with_column(
                "occurred_at",
                ScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("connection_id", ScalarType::String.nullable(false))
            .with_column("status", ScalarType::String.nullable(false))
    });
