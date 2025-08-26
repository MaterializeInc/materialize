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
use std::time::Duration;

use aws_rds::RdsTokenError;
pub use tunnel::{
    Config, DEFAULT_CONNECT_TIMEOUT, DEFAULT_SNAPSHOT_LOCK_WAIT_TIMEOUT,
    DEFAULT_SNAPSHOT_MAX_EXECUTION_TIME, DEFAULT_TCP_KEEPALIVE, MySqlConn, TimeoutConfig,
    TunnelConfig,
};

mod desc;
pub use desc::{
    MySqlColumnDesc, MySqlColumnMeta, MySqlKeyDesc, MySqlTableDesc, ProtoMySqlColumnDesc,
    ProtoMySqlKeyDesc, ProtoMySqlTableDesc,
};

mod replication;
pub use replication::{
    ensure_full_row_binlog_format, ensure_gtid_consistency, ensure_replication_commit_order,
    query_sys_var,
};

pub mod schemas;
pub use schemas::{
    MySqlTableSchema, QualifiedTableRef, SYSTEM_SCHEMAS, SchemaRequest, schema_info,
};

pub mod privileges;
pub use privileges::validate_source_privileges;

pub mod decoding;
pub use decoding::pack_mysql_row;
mod aws_rds;

#[derive(Debug, Clone)]
pub struct UnsupportedDataType {
    pub column_type: String,
    pub qualified_table_name: String,
    pub column_name: String,
    pub intended_type: Option<String>,
}

impl std::fmt::Display for UnsupportedDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.intended_type {
            Some(intended_type) => write!(
                f,
                "'{}.{}' of type '{}' represented as: '{}'",
                self.qualified_table_name, self.column_name, self.column_type, intended_type
            ),
            None => write!(
                f,
                "'{}.{}' of type '{}'",
                self.qualified_table_name, self.column_name, self.column_type
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MissingPrivilege {
    pub privilege: String,
    pub qualified_table_name: String,
}

impl std::fmt::Display for MissingPrivilege {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Missing privilege '{}' for '{}'",
            self.privilege, self.qualified_table_name
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MySqlError {
    #[error("error validating privileges: {0:?}")]
    MissingPrivileges(Vec<MissingPrivilege>),
    #[error("error creating mysql connection with config: {0}")]
    InvalidClientConfig(String),
    #[error("error setting up ssh: {0}")]
    Ssh(#[source] anyhow::Error),
    #[error("error decoding value for '{qualified_table_name}' column '{column_name}': {error}")]
    ValueDecodeError {
        column_name: String,
        qualified_table_name: String,
        error: String,
    },
    #[error("unsupported data types: {columns:?}")]
    UnsupportedDataTypes { columns: Vec<UnsupportedDataType> },
    #[error("duplicated column names in table '{qualified_table_name}': {columns:?}")]
    DuplicatedColumnNames {
        qualified_table_name: String,
        columns: Vec<String>,
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
    #[error("connection attempt timed out after {0:?}")]
    ConnectionTimeout(Duration),
    /// Error retrieving AWS authorization token
    #[error(transparent)]
    AwsTokenError(#[from] RdsTokenError),
}

/// Quotes MySQL identifiers. [See MySQL quote_identifier()](https://github.com/mysql/mysql-sys/blob/master/functions/quote_identifier.sql)
pub fn quote_identifier(identifier: &str) -> String {
    let mut escaped = identifier.replace("`", "``");
    escaped.insert(0, '`');
    escaped.push('`');
    escaped
}

// NOTE: this error was renamed between MySQL 5.7 and 8.0
// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_source_fatal_error_reading_binlog
// https://dev.mysql.com/doc/mysql-errors/5.7/en/server-error-reference.html#error_er_master_fatal_error_reading_binlog
pub const ER_SOURCE_FATAL_ERROR_READING_BINLOG_CODE: u16 = 1236;

// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_no_such_table
pub const ER_NO_SUCH_TABLE: u16 = 1146;

#[cfg(test)]
mod tests {

    use super::quote_identifier;
    #[mz_ore::test]
    fn test_identifier_quoting() {
        let expected = vec!["`a`", "`naughty``sql`", "```;naughty;sql;```"];
        let input = ["a", "naughty`sql", "`;naughty;sql;`"]
            .iter()
            .map(|raw_str| quote_identifier(raw_str))
            .collect::<Vec<_>>();
        assert_eq!(expected, input);
    }
}
