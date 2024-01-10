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
pub use desc::{MySqlColumnDesc, MySqlDataType, MySqlTableDesc};

mod replication;
pub use replication::{ensure_full_row_binlog_format, ensure_gtid_consistency, query_sys_var};

pub mod schemas;
pub use schemas::schema_info;

#[derive(Debug, thiserror::Error)]
pub enum MySqlError {
    #[error("unsupported data type: {0}")]
    UnsupportedDataType(String),
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
