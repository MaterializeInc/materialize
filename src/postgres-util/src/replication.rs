// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use tokio_postgres::Client;

use mz_ssh_util::tunnel_manager::SshTunnelManager;

use crate::{simple_query_opt, Config, PostgresError};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum WalLevel {
    Minimal,
    Replica,
    Logical,
}

impl std::str::FromStr for WalLevel {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "minimal" => Ok(Self::Minimal),
            "replica" => Ok(Self::Replica),
            "logical" => Ok(Self::Logical),
            o => Err(anyhow::anyhow!("unknown wal_level {}", o)),
        }
    }
}

impl std::fmt::Display for WalLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            WalLevel::Minimal => "minimal",
            WalLevel::Replica => "replica",
            WalLevel::Logical => "logical",
        };

        f.write_str(s)
    }
}

#[mz_ore::test]
fn test_wal_level_max() {
    // Ensure `WalLevel::Logical` is the max among all levels.
    for o in [WalLevel::Minimal, WalLevel::Replica, WalLevel::Logical] {
        assert_eq!(WalLevel::Logical, WalLevel::Logical.max(o))
    }
}

pub async fn get_wal_level(
    ssh_tunnel_manager: &SshTunnelManager,
    config: &Config,
) -> Result<WalLevel, PostgresError> {
    let client = config
        .connect("wal_level_check", ssh_tunnel_manager)
        .await?;
    let wal_level = client.query_one("SHOW wal_level", &[]).await?;
    let wal_level: String = wal_level.get("wal_level");
    Ok(WalLevel::from_str(&wal_level)?)
}

pub async fn get_max_wal_senders(
    ssh_tunnel_manager: &SshTunnelManager,
    config: &Config,
) -> Result<i64, PostgresError> {
    let client = config
        .connect("max_wal_senders_check", ssh_tunnel_manager)
        .await?;
    let max_wal_senders = client
        .query_one(
            "SELECT CAST(current_setting('max_wal_senders') AS int8) AS max_wal_senders",
            &[],
        )
        .await?;
    Ok(max_wal_senders.get("max_wal_senders"))
}

pub async fn available_replication_slots(
    ssh_tunnel_manager: &SshTunnelManager,
    config: &Config,
) -> Result<i64, PostgresError> {
    let client = config
        .connect("postgres_check_replication_slots", ssh_tunnel_manager)
        .await?;

    let available_replication_slots = client
        .query_one(
            "SELECT
            CAST(current_setting('max_replication_slots') AS int8)
              - (SELECT count(*) FROM pg_catalog.pg_replication_slots)
              AS available_replication_slots;",
            &[],
        )
        .await?;

    let available_replication_slots: i64 =
        available_replication_slots.get("available_replication_slots");

    Ok(available_replication_slots)
}

pub async fn drop_replication_slots(
    ssh_tunnel_manager: &SshTunnelManager,
    config: Config,
    slots: &[&str],
) -> Result<(), PostgresError> {
    let client = config
        .connect("postgres_drop_replication_slots", ssh_tunnel_manager)
        .await?;
    let replication_client = config.connect_replication(ssh_tunnel_manager).await?;
    for slot in slots {
        let rows = client
            .query(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1::TEXT",
                &[&slot],
            )
            .await?;
        match rows.len() {
            0 => {
                // DROP_REPLICATION_SLOT will error if the slot does not exist
                tracing::info!(
                    "drop_replication_slots called on non-existent slot {}",
                    slot
                );
                continue;
            }
            1 => {
                replication_client
                    .simple_query(&format!("DROP_REPLICATION_SLOT {} WAIT", slot))
                    .await?;
            }
            _ => {
                return Err(PostgresError::Generic(anyhow::anyhow!(
                    "multiple pg_replication_slots entries for slot {}",
                    &slot
                )))
            }
        }
    }
    Ok(())
}

pub async fn get_timeline_id(replication_client: &Client) -> Result<u64, PostgresError> {
    if let Some(r) = simple_query_opt(replication_client, "IDENTIFY_SYSTEM").await? {
        r.get("timeline")
            .expect("Returns a row with a timeline ID")
            .parse::<u64>()
            .map_err(|err| {
                PostgresError::Generic(anyhow::anyhow!(
                    "Failed to parse timeline ID from IDENTIFY_SYSTEM: {}",
                    err
                ))
            })
    } else {
        Err(PostgresError::Generic(anyhow::anyhow!(
            "IDENTIFY_SYSTEM did not return a result row"
        )))
    }
}
