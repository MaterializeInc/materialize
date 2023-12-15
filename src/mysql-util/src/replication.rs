// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mysql_async::prelude::Queryable;
use mysql_async::Conn;

use crate::MySqlError;

/// Query a MySQL System Variable
pub async fn query_sys_var(conn: &mut Conn, name: &str) -> Result<String, MySqlError> {
    let value: String = conn
        .query_first(format!("SELECT @@{}", name))
        .await?
        .unwrap();
    Ok(value)
}

/// Verify a MySQL System Variable matches the expected value
async fn verify_sys_setting(
    conn: &mut Conn,
    setting: &str,
    expected: &str,
) -> Result<(), MySqlError> {
    match query_sys_var(conn, setting).await?.as_str() {
        actual if actual == expected => Ok(()),
        actual => Err(MySqlError::InvalidSystemSetting {
            setting: setting.to_string(),
            expected: expected.to_string(),
            actual: actual.to_string(),
        }),
    }
}

pub async fn ensure_full_row_binlog_format(conn: &mut Conn) -> Result<(), MySqlError> {
    verify_sys_setting(conn, "log_bin", "1").await?;
    verify_sys_setting(conn, "binlog_format", "ROW").await?;
    verify_sys_setting(conn, "binlog_row_image", "FULL").await?;
    Ok(())
}

pub async fn ensure_gtid_consistency(conn: &mut Conn) -> Result<(), MySqlError> {
    verify_sys_setting(conn, "gtid_mode", "ON").await?;
    verify_sys_setting(conn, "enforce_gtid_consistency", "ON").await?;
    Ok(())
}
