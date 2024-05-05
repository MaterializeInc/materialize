// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{RelationDesc, ScalarType};
use mz_sql_parser::ident;
use once_cell::sync::Lazy;

pub static MZ_PREPARED_STATEMENT_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(ident!("id"), ScalarType::Uuid.nullable(false))
        .with_column(ident!("session_id"), ScalarType::Uuid.nullable(false))
        .with_column(ident!("name"), ScalarType::String.nullable(false))
        .with_column(ident!("sql_hash"), ScalarType::Bytes.nullable(false))
        .with_column(
            ident!("prepared_at"),
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(ident!("statement_type"), ScalarType::String.nullable(true))
        .with_column(
            ident!("throttled_count"),
            ScalarType::UInt64.nullable(false),
        )
});

pub static MZ_SQL_TEXT_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(
            ident!("prepared_day"),
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(ident!("sql_hash"), ScalarType::Bytes.nullable(false))
        .with_column(ident!("sql"), ScalarType::String.nullable(false))
        .with_column(ident!("redacted_sql"), ScalarType::String.nullable(false))
});

pub static MZ_SESSION_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(ident!("id"), ScalarType::Uuid.nullable(false))
        .with_column(
            ident!("connected_at"),
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            ident!("initial_application_name"),
            ScalarType::String.nullable(false),
        )
        .with_column(
            ident!("authenticated_user"),
            ScalarType::String.nullable(false),
        )
});

// NOTE: Update the views `mz_statement_execution_history_redacted`
// and `mz_activity_log`, and `mz_activity_log_redacted` whenever this
// is updated, to include the new columns where appropriate.
//
// The `redacted` views should contain only those columns that should
// be queryable by support.
pub static MZ_STATEMENT_EXECUTION_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(ident!("id"), ScalarType::Uuid.nullable(false))
        .with_column(
            ident!("prepared_statement_id"),
            ScalarType::Uuid.nullable(false),
        )
        .with_column(ident!("sample_rate"), ScalarType::Float64.nullable(false))
        .with_column(ident!("cluster_id"), ScalarType::String.nullable(true))
        .with_column(
            ident!("application_name"),
            ScalarType::String.nullable(false),
        )
        .with_column(ident!("cluster_name"), ScalarType::String.nullable(true))
        .with_column(
            ident!("transaction_isolation"),
            ScalarType::String.nullable(false),
        )
        // Note that this can't be a timestamp, as it might be u64::max,
        // which is out of range.
        .with_column(
            ident!("execution_timestamp"),
            ScalarType::UInt64.nullable(true),
        )
        .with_column(ident!("transaction_id"), ScalarType::UInt64.nullable(false))
        .with_column(
            ident!("transient_index_id"),
            ScalarType::String.nullable(true),
        )
        .with_column(
            ident!("params"),
            ScalarType::Array(Box::new(ScalarType::String)).nullable(false),
        )
        .with_column(ident!("mz_version"), ScalarType::String.nullable(false))
        .with_column(
            ident!("began_at"),
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            ident!("finished_at"),
            ScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column(ident!("finished_status"), ScalarType::String.nullable(true))
        .with_column(ident!("error_message"), ScalarType::String.nullable(true))
        .with_column(ident!("rows_returned"), ScalarType::Int64.nullable(true))
        .with_column(
            ident!("execution_strategy"),
            ScalarType::String.nullable(true),
        )
});

pub static MZ_SOURCE_STATUS_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(
            ident!("occurred_at"),
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(ident!("source_id"), ScalarType::String.nullable(false))
        .with_column(ident!("status"), ScalarType::String.nullable(false))
        .with_column(ident!("error"), ScalarType::String.nullable(true))
        .with_column(ident!("details"), ScalarType::Jsonb.nullable(true))
});

pub static MZ_SINK_STATUS_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(
            ident!("occurred_at"),
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(ident!("sink_id"), ScalarType::String.nullable(false))
        .with_column(ident!("status"), ScalarType::String.nullable(false))
        .with_column(ident!("error"), ScalarType::String.nullable(true))
        .with_column(ident!("details"), ScalarType::Jsonb.nullable(true))
});

pub static MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC: Lazy<RelationDesc> =
    Lazy::new(|| {
        RelationDesc::empty()
            .with_column(
                ident!("occurred_at"),
                ScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column(ident!("connection_id"), ScalarType::String.nullable(false))
            .with_column(ident!("status"), ScalarType::String.nullable(false))
    });
