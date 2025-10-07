// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use tokio_postgres::{
    Client,
    types::{Oid, PgLsn},
};

use mz_ssh_util::tunnel_manager::SshTunnelManager;

use crate::{Config, PostgresError, simple_query_opt};

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

pub async fn get_wal_level(client: &Client) -> Result<WalLevel, PostgresError> {
    let wal_level = client.query_one("SHOW wal_level", &[]).await?;
    let wal_level: String = wal_level.get("wal_level");
    Ok(WalLevel::from_str(&wal_level)?)
}

pub async fn get_max_wal_senders(client: &Client) -> Result<i64, PostgresError> {
    let max_wal_senders = client
        .query_one(
            "SELECT CAST(current_setting('max_wal_senders') AS int8) AS max_wal_senders",
            &[],
        )
        .await?;
    Ok(max_wal_senders.get("max_wal_senders"))
}

pub async fn available_replication_slots(client: &Client) -> Result<i64, PostgresError> {
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

/// Returns true if BYPASSRLS is set for the current user, false otherwise.
///
/// See <https://www.postgresql.org/docs/current/ddl-rowsecurity.html>
pub async fn bypass_rls_attribute(client: &Client) -> Result<bool, PostgresError> {
    let rls_attribute = client
        .query_one(
            "SELECT rolbypassrls FROM pg_roles WHERE rolname = CURRENT_USER;",
            &[],
        )
        .await?;
    Ok(rls_attribute.get("rolbypassrls"))
}

/// Returns an error if the tables identified by the oid's have RLS policies which
/// affect the current user. Two checks are made:
///
/// 1. Identify which tables, from the provided oid's, have RLS policies that affecct the user or
///    public.
/// 2. If there are policies that affect the user, check if the BYPASSRLS attribute is set. If set,
///    the role is unaffected by the policies.
pub async fn validate_no_rls_policies(
    client: &Client,
    table_oids: &[Oid],
) -> Result<(), PostgresError> {
    if table_oids.is_empty() {
        return Ok(());
    }
    let tables_with_rls_for_user = client
        .query(
            "SELECT
                    format('%I.%I', pc.relnamespace::regnamespace, pc.relname) AS qualified_name
                FROM pg_policy pp
                JOIN pg_class pc ON pc.oid = polrelid
                WHERE
                    polrelid = ANY($1::oid[])
                    AND
                    (0 = ANY(polroles) OR CURRENT_USER::regrole::oid = ANY(polroles));",
            &[&table_oids],
        )
        .await
        .map_err(PostgresError::from)?;

    let mut tables_with_rls_for_user = tables_with_rls_for_user
        .into_iter()
        .map(|row| row.get("qualified_name"))
        .collect::<Vec<String>>();

    // If the user has the BYPASSRLS flag set, then the policies don't apply, so we can
    // return success.
    if tables_with_rls_for_user.is_empty() || bypass_rls_attribute(client).await? {
        Ok(())
    } else {
        tables_with_rls_for_user.sort();
        Err(PostgresError::BypassRLSRequired(tables_with_rls_for_user))
    }
}

pub async fn drop_replication_slots(
    ssh_tunnel_manager: &SshTunnelManager,
    config: Config,
    slots: &[(&str, bool)],
) -> Result<(), PostgresError> {
    let client = config
        .connect("postgres_drop_replication_slots", ssh_tunnel_manager)
        .await?;
    let replication_client = config.connect_replication(ssh_tunnel_manager).await?;
    for (slot, should_wait) in slots {
        let rows = client
            .query(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1::TEXT",
                &[&slot],
            )
            .await?;
        match &*rows {
            [] => {
                // DROP_REPLICATION_SLOT will error if the slot does not exist
                tracing::info!(
                    "drop_replication_slots called on non-existent slot {}",
                    slot
                );
                continue;
            }
            [row] => {
                // The drop of a replication slot happens concurrently with an ingestion dataflow
                // shutting down, therefore there is the possibility that the slot is still in use.
                // We really don't want to leak the slot and not forcefully terminating the
                // dataflow's connection risks timing out. For this reason we always kill the
                // active backend and drop the slot.
                let active_pid: Option<i32> = row.get("active_pid");
                if let Some(active_pid) = active_pid {
                    client
                        .simple_query(&format!("SELECT pg_terminate_backend({active_pid})"))
                        .await?;
                }

                let wait_str = if *should_wait { " WAIT" } else { "" };
                replication_client
                    .simple_query(&format!("DROP_REPLICATION_SLOT {slot}{wait_str}"))
                    .await?;
            }
            _ => {
                return Err(PostgresError::Generic(anyhow::anyhow!(
                    "multiple pg_replication_slots entries for slot {}",
                    &slot
                )));
            }
        }
    }
    Ok(())
}

pub async fn get_timeline_id(client: &Client) -> Result<u64, PostgresError> {
    if let Some(r) =
        simple_query_opt(client, "SELECT timeline_id FROM pg_control_checkpoint()").await?
    {
        r.get("timeline_id")
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

pub async fn get_current_wal_lsn(client: &Client) -> Result<PgLsn, PostgresError> {
    let row = client.query_one("SELECT pg_current_wal_lsn()", &[]).await?;
    let lsn: PgLsn = row.get(0);

    Ok(lsn)
}
