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
    verify_sys_setting(conn, "gtid_next", "AUTOMATIC").await?;
    Ok(())
}

/// In case this is a MySQL replica, we ensure that the replication settings are such that
/// the replica would commit all transactions in the order they were committed on the primary.
/// We don't really know that this is a replica, but if the settings indicate multi-threaded
/// replication and the preserve-commit-order setting is not on, then it _could_ be a replica
/// with correctness issues.
/// We used to check `performance_schema.replication_connection_configuration` to determine if
/// this was in-fact a replica but that requires non-standard privileges.
/// Before MySQL 8.0.27, single-threaded was default and preserve-commit-order was not, and after
/// 8.0.27 multi-threaded is default and preserve-commit-order is default on. So both of those
/// default scenarios are fine. Unfortunately on some versions of MySQL on RDS, the default
/// parameters use multi-threading without the preserve-commit-order setting on.
pub async fn ensure_replication_commit_order(conn: &mut Conn) -> Result<(), MySqlError> {
    // This system variables were renamed between MySQL 5.7 and 8.0
    let is_multi_threaded = match query_sys_var(conn, "replica_parallel_workers").await {
        Ok(val) => val != "0" && val != "1",
        Err(_) => match query_sys_var(conn, "slave_parallel_workers").await {
            Ok(val) => val != "0" && val != "1",
            Err(err) => return Err(err),
        },
    };

    if is_multi_threaded {
        match verify_sys_setting(conn, "replica_preserve_commit_order", "1").await {
            Ok(_) => Ok(()),
            Err(_) => verify_sys_setting(conn, "slave_preserve_commit_order", "1").await,
        }
    } else {
        Ok(())
    }
}
