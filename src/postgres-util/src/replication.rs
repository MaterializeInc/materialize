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
    Client, SimpleQueryRow,
    types::{Oid, PgLsn},
};

use mz_ssh_util::tunnel_manager::SshTunnelManager;

use crate::{Config, PG_FIRST_TIMELINE_ID, PostgresError, simple_query_opt};

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

#[derive(Debug, PartialEq, Eq)]
pub struct MzPgTimelineHistoryEntry {
    pub timeline_id: u64, // TODO (maz) should declare a type of timeline id
    pub switchpoint_lsn: Option<PgLsn>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct MzPgTimelineHistory {
    pub history: Vec<MzPgTimelineHistoryEntry>,
}

impl MzPgTimelineHistory {
    // returns the timelime id of the resume_lsn or None if the lsn does not occur in the history
    // MZ has seen all LSNs up to, but not including, the resume_lsn.  This function
    // determins the timeline using lsn <= switchpoint_lsn
    pub fn timeline_of_resume_lsn(&self, lsn: &PgLsn) -> Option<u64> {
        let mut timeline_id = None;
        for entry in self.history.iter().rev() {
            if entry
                .switchpoint_lsn
                .is_some_and(|switchpoint| switchpoint < *lsn)
            {
                break;
            }
            timeline_id = Some(entry.timeline_id);
        }
        timeline_id
    }

    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }
}

impl FromIterator<MzPgTimelineHistoryEntry> for MzPgTimelineHistory {
    fn from_iter<T: IntoIterator<Item = MzPgTimelineHistoryEntry>>(iter: T) -> Self {
        MzPgTimelineHistory {
            history: iter.into_iter().collect(),
        }
    }
}

impl TryFrom<SimpleQueryRow> for MzPgTimelineHistory {
    type Error = PostgresError;

    fn try_from(value: SimpleQueryRow) -> Result<Self, Self::Error> {
        let [filename_col, content_col] = value.columns() else {
            return Err(PostgresError::Generic(anyhow::anyhow!(
                "Timeline history row should only contain 2 columns"
            )));
        };

        if (filename_col.name(), content_col.name()) != ("filename", "content") {
            return Err(PostgresError::Generic(anyhow::anyhow!(
                "Timeline history expected columns [filename, content], got [{col1},{col2}]",
                col1 = filename_col.name(),
                col2 = content_col.name()
            )));
        }

        let current_timeline_id = parse_timeline_filename(value.get(0).expect("filename"))?;
        let mut history = parse_timeline_history(value.get(1).expect("content"))?;

        // append the current timeline to maintain the ordering
        history.push(MzPgTimelineHistoryEntry {
            timeline_id: current_timeline_id,
            switchpoint_lsn: None,
        });
        Ok(MzPgTimelineHistory { history })
    }
}

fn parse_timeline_filename(filename: &str) -> Result<u64, PostgresError> {
    // valid name is <hex_timeline_id>.history, including zero padding
    let Some(ext_idx) = filename.find(".history") else {
        return Err(PostgresError::Generic(anyhow::anyhow!(
            "Invalid timeline history filename: {filename}"
        )));
    };
    // extract the current timeline from the filename
    u64::from_str_radix(&filename[..ext_idx], 16).map_err(|_| {
        PostgresError::Generic(anyhow::anyhow!(
            "Invalid timeline history filename: {filename}"
        ))
    })
}

fn parse_timeline_history(content: &str) -> Result<Vec<MzPgTimelineHistoryEntry>, PostgresError> {
    let mut history = vec![];
    for line in content.lines() {
        // skip whitespace
        let line = line.trim();

        // skip comments and blank lines
        if line.starts_with("#") || line.is_empty() {
            continue;
        }

        // the remaining lines must be a in a tab separated format
        // <timeline_id>\t<switchpoint_hi>/<switchpoint_lo>\t<reason_string>
        //
        // The reason string is ignored. It is meant to give humans context as to why the
        // switch happened, but is not relevant in making decisions about timeline
        // continuity.
        let mut parts = line.split('\t');

        let timeline_id_str = parts
            .next()
            .ok_or_else(|| PostgresError::Generic(anyhow::anyhow!("Missing timeline_id")))?;
        let timeline_id = timeline_id_str.parse::<u64>().map_err(|_| {
            PostgresError::Generic(anyhow::anyhow!(
                "Invalid timeline_id '{timeline_id_str}': must be u64"
            ))
        })?;

        let switchpoint_str = parts
            .next()
            .ok_or_else(|| PostgresError::Generic(anyhow::anyhow!("Missing switchpoint")))?;
        let switchpoint_lsn = switchpoint_str.parse::<PgLsn>().map_err(|_| {
            PostgresError::Generic(anyhow::anyhow!(
                "Invalid switchpoint '{switchpoint_str}': must be LSN"
            ))
        })?;

        history.push(MzPgTimelineHistoryEntry {
            timeline_id,
            switchpoint_lsn: Some(switchpoint_lsn),
        });
    }
    Ok(history)
}

/// Retrieve the timeline history from PostgreSQL via TIMELINE_HISTORY command.
///
/// The resulting [`MzPgTimelineHistory`] will have the current timeline as the last entry
/// with a switchpoint LSN of [`None`].
///
/// If the current timeline is 1, PostgreSQL is not queried for history.
pub async fn get_timeline_history(
    replication_client: &Client,
    current_timeline: u64,
) -> Result<MzPgTimelineHistory, PostgresError> {
    // Special case the first timeline, as there is no history file.
    if current_timeline == PG_FIRST_TIMELINE_ID {
        return Ok(MzPgTimelineHistory {
            history: vec![MzPgTimelineHistoryEntry {
                timeline_id: PG_FIRST_TIMELINE_ID,
                switchpoint_lsn: None,
            }],
        });
    }
    let cmd = format!("TIMELINE_HISTORY {current_timeline}");
    let Some(raw_history) = simple_query_opt(replication_client, &cmd).await? else {
        return Err(PostgresError::Generic(anyhow::anyhow!(
            "Failed to retrieve timeline history for timeline {current_timeline}"
        )));
    };
    MzPgTimelineHistory::try_from(raw_history)
}

pub async fn get_current_wal_lsn(client: &Client) -> Result<PgLsn, PostgresError> {
    let row = client.query_one("SELECT pg_current_wal_lsn()", &[]).await?;
    let lsn: PgLsn = row.get(0);

    Ok(lsn)
}

#[cfg(test)]
mod test {
    use std::u64;

    use super::*;

    // Tests for parse_timeline_filename

    #[mz_ore::test]
    fn test_parse_timeline_filename_standard() {
        // Standard PostgreSQL format: 8-digit zero-padded hex
        assert_eq!(parse_timeline_filename("00000002.history").unwrap(), 2);
        // Non-zero-padded and lowercase also work
        assert_eq!(parse_timeline_filename("a.history").unwrap(), 10);
    }

    #[mz_ore::test]
    fn test_parse_timeline_filename_max_value() {
        let result = parse_timeline_filename("FFFFFFFFFFFFFFFF.history").unwrap();
        assert_eq!(result, u64::MAX);
    }

    #[mz_ore::test]
    fn test_parse_timeline_filename_missing_extension() {
        let result = parse_timeline_filename("00000002");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid timeline history filename: 00000002"),
            "got: {}",
            err
        );
    }

    #[mz_ore::test]
    fn test_parse_timeline_filename_empty_timeline_id() {
        let result = parse_timeline_filename(".history");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid timeline history filename: .history"),
            "got: {}",
            err
        );
    }

    #[mz_ore::test]
    fn test_parse_timeline_filename_invalid_hex() {
        let result = parse_timeline_filename("notahex.history");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid timeline history filename: notahex.history"),
            "got: {}",
            err
        );
    }

    #[mz_ore::test]
    fn test_parse_timeline_filename_overflow() {
        // One more than max u64 in hex
        let result = parse_timeline_filename("10000000000000000.history");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid timeline history filename: 10000000000000000.history"),
            "got: {}",
            err
        );
    }

    // Tests for parse_timeline_history

    #[mz_ore::test]
    fn test_parse_timeline_history_single_entry() {
        let content = "1\t0/411F520\tno recovery target specified\n";
        let result = parse_timeline_history(content).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timeline_id, 1);
        assert_eq!(result[0].switchpoint_lsn, Some(PgLsn::from(0x411F520)));
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_multiple_entries() {
        let content = "1\t0/411F520\tno recovery target specified\n\
                       2\t0/5000000\tafter failover\n\
                       3\t0/6000000\tpoint in time recovery\n";
        let result = parse_timeline_history(content).unwrap();
        assert_eq!(result.len(), 3);

        assert_eq!(result[0].timeline_id, 1);
        assert_eq!(result[0].switchpoint_lsn, Some(PgLsn::from(0x411F520)));

        assert_eq!(result[1].timeline_id, 2);
        assert_eq!(result[1].switchpoint_lsn, Some(PgLsn::from(0x5000000)));

        assert_eq!(result[2].timeline_id, 3);
        assert_eq!(result[2].switchpoint_lsn, Some(PgLsn::from(0x6000000)));
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_with_comments() {
        let content = "# This is a comment\n\
                       1\t0/411F520\tno recovery target specified\n\
                       # Another comment\n\
                       2\t0/5000000\tafter failover\n";
        let result = parse_timeline_history(content).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timeline_id, 1);
        assert_eq!(result[1].timeline_id, 2);
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_with_blank_lines() {
        let content = "\n\
                       1\t0/411F520\tno recovery target specified\n\
                       \n\
                       \n\
                       2\t0/5000000\tafter failover\n\
                       \n";
        let result = parse_timeline_history(content).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timeline_id, 1);
        assert_eq!(result[1].timeline_id, 2);
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_with_whitespace() {
        let content = "   1\t0/411F520\tno recovery target specified\n\
                          2\t0/5000000\tafter failover   \n";
        let result = parse_timeline_history(content).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timeline_id, 1);
        assert_eq!(result[1].timeline_id, 2);
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_empty_content() {
        let content = "";
        let result = parse_timeline_history(content).unwrap();
        assert!(result.is_empty());
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_only_comments_and_blanks() {
        let content = "# Comment line 1\n\
                       # Comment line 2\n\
                       \n\
                       # More comments\n";
        let result = parse_timeline_history(content).unwrap();
        assert!(result.is_empty());
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_large_lsn() {
        // Test with large LSN values (multi-segment)
        let content = "1\tFFFFFFFF/FFFFFFFF\tmax lsn test\n";
        let result = parse_timeline_history(content).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timeline_id, 1);
        assert_eq!(result[0].switchpoint_lsn, Some(PgLsn::from(u64::MAX)));
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_no_reason_field() {
        // The reason field is optional - line with just timeline_id and switchpoint
        let content = "1\t0/411F520\n";
        let result = parse_timeline_history(content).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timeline_id, 1);
        assert_eq!(result[0].switchpoint_lsn, Some(PgLsn::from(0x411F520)));
    }

    // Negative test cases

    #[mz_ore::test]
    fn test_parse_timeline_history_invalid_timeline_id() {
        let content = "notanumber\t0/411F520\tsome reason\n";
        let result = parse_timeline_history(content);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid timeline_id 'notanumber': must be u64"),
            "got: {}",
            err
        );
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_missing_switchpoint() {
        // Line with timeline_id but no switchpoint (no tab separator)
        let content = "1\n";
        let result = parse_timeline_history(content);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Missing switchpoint"), "got: {}", err);
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_empty_switchpoint() {
        // Line with timeline_id and empty switchpoint but with reason field
        // to prevent trim from collapsing it
        let content = "1\t\tsome reason\n";
        let result = parse_timeline_history(content);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid switchpoint '': must be LSN"),
            "got: {}",
            err
        );
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_invalid_switchpoint_format() {
        let content = "1\tnotanlsn\tsome reason\n";
        let result = parse_timeline_history(content);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid switchpoint 'notanlsn': must be LSN"),
            "got: {}",
            err
        );
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_negative_timeline_id() {
        let content = "-1\t0/411F520\tsome reason\n";
        let result = parse_timeline_history(content);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid timeline_id '-1': must be u64"),
            "got: {}",
            err
        );
    }

    #[mz_ore::test]
    fn test_parse_timeline_history_switchpoint_missing_slash() {
        let content = "1\t0411F520\tsome reason\n";
        let result = parse_timeline_history(content);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid switchpoint '0411F520': must be LSN"),
            "got: {}",
            err
        );
    }

    #[mz_ore::test]
    fn test_timeline_of_resume_lsn() {
        let hist = MzPgTimelineHistory {
            history: vec![
                MzPgTimelineHistoryEntry {
                    timeline_id: 1,
                    switchpoint_lsn: Some(PgLsn::from(16667)),
                },
                MzPgTimelineHistoryEntry {
                    timeline_id: 2,
                    switchpoint_lsn: Some(PgLsn::from(166667)),
                },
                MzPgTimelineHistoryEntry {
                    timeline_id: 3,
                    switchpoint_lsn: None,
                },
            ],
        };

        assert_eq!(Some(1), hist.timeline_of_resume_lsn(&PgLsn::from(u64::MIN)));
        assert_eq!(Some(1), hist.timeline_of_resume_lsn(&PgLsn::from(16666)));
        assert_eq!(Some(1), hist.timeline_of_resume_lsn(&PgLsn::from(16667)));

        assert_eq!(Some(2), hist.timeline_of_resume_lsn(&PgLsn::from(16668)));
        assert_eq!(Some(2), hist.timeline_of_resume_lsn(&PgLsn::from(166666)));
        assert_eq!(Some(2), hist.timeline_of_resume_lsn(&PgLsn::from(166667)));

        assert_eq!(Some(3), hist.timeline_of_resume_lsn(&PgLsn::from(166668)));
        assert_eq!(
            Some(3),
            hist.timeline_of_resume_lsn(&PgLsn::from(1666666666667))
        );
        assert_eq!(Some(3), hist.timeline_of_resume_lsn(&PgLsn::from(u64::MAX)));
    }
}
