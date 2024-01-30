// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! MySQL utility library.

mod tunnel;
pub use tunnel::{Config, TunnelConfig};

mod desc;
pub use desc::{
    MySqlColumnDesc, MySqlKeyDesc, MySqlTableDesc, ProtoMySqlColumnDesc, ProtoMySqlKeyDesc,
    ProtoMySqlTableDesc,
};

mod replication;
pub use replication::{
    ensure_full_row_binlog_format, ensure_gtid_consistency, ensure_replication_commit_order,
    query_sys_var,
};

pub mod schemas;
pub use schemas::{schema_info, SchemaRequest};

#[derive(Debug, thiserror::Error)]
pub enum MySqlError {
    #[error("unsupported data type: '{column_type}' for '{qualified_table_name}.{column_name}'.")]
    UnsupportedDataType {
        column_type: String,
        qualified_table_name: String,
        column_name: String,
    },
    #[error("invalid mysql system setting '{setting}'. Expected '{expected}'. Got '{actual}'.")]
    InvalidSystemSetting {
        setting: String,
        expected: String,
        actual: String,
    },
    /// Any other error we bail on.
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
    /// A mysql_async error.
    #[error(transparent)]
    MySql(#[from] mysql_async::Error),
}

// NOTE: this error was renamed between MySQL 5.7 and 8.0
// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_source_fatal_error_reading_binlog
// https://dev.mysql.com/doc/mysql-errors/5.7/en/server-error-reference.html#error_er_master_fatal_error_reading_binlog
pub const ER_SOURCE_FATAL_ERROR_READING_BINLOG_CODE: u16 = 1236;
