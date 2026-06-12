// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent JSON-line command reader for scripting the driver.
//!
//! Instead of recompiling a Rust scenario, a test (or an agent) streams JSON
//! commands on stdin; the driver executes each against `clusterd` and writes one
//! JSON response per line on stdout. This is the first cut of the scripting layer
//! described in the design doc: the coarse orchestration verbs map almost directly
//! to [`Driver`] calls. Authoring arbitrary MIR — the full `define` command and its
//! literal shim — is a later step; `define_index` here covers the common shape via
//! the existing [`index_dataflow`] sugar.
//!
//! # Protocol
//!
//! One JSON object per line. Blank lines and lines beginning with `#` or `//` are
//! ignored, so scripts can carry comments. Each command produces exactly one
//! response line. A parse or execution error yields an `error` response and the
//! loop continues, but [`run`] returns `Err` at end-of-input if any command failed,
//! so a scripted run fails the process (and CI) on the first bad command.
//!
//! Shards are referenced by a string alias; the first command naming an alias
//! allocates a fresh [`ShardId`] for it. Object ids are raw `u64`s mapped to
//! [`GlobalId::User`].

use std::collections::BTreeMap;
use std::time::Duration;

use mz_compute_client::protocol::command::ComputeCommand;
use mz_ore::cast::CastFrom;
use mz_persist_client::PersistClient;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::data::{sample_desc, sample_rows, write_rows_single_ts, write_rows_spread};
use crate::dataflow::index_dataflow;
use crate::driver::Driver;

/// The default payload padding (bytes) for synthetic rows when a command omits it.
const DEFAULT_ROW_BYTES: usize = 64;
/// The default timeout (seconds) for `await_frontier` when a command omits it.
const DEFAULT_TIMEOUT_SECS: u64 = 600;

/// A command read from the script stream.
///
/// Tagged on `"cmd"`, snake_case, e.g.
/// `{"cmd":"write_single_ts","shard":"s1","ts":0,"rows":1000}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    /// Write `rows` synthetic rows to `shard` at a single timestamp `ts`.
    WriteSingleTs {
        /// Shard alias; allocated on first use.
        shard: String,
        /// The timestamp to write at.
        ts: u64,
        /// Number of rows to write.
        rows: u64,
        /// Payload padding per row; defaults to [`DEFAULT_ROW_BYTES`].
        #[serde(default)]
        row_bytes: Option<usize>,
    },
    /// Write `rows` synthetic rows to `shard`, spread across `n_ts` timestamps in a
    /// single append.
    WriteSpread {
        /// Shard alias; allocated on first use.
        shard: String,
        /// Number of rows to write.
        rows: u64,
        /// Number of distinct timestamps to spread the rows across.
        n_ts: u64,
        /// Payload padding per row; defaults to [`DEFAULT_ROW_BYTES`].
        #[serde(default)]
        row_bytes: Option<usize>,
    },
    /// Submit (without scheduling) an index dataflow over `shard`.
    DefineIndex {
        /// The imported source's global id.
        source_id: u64,
        /// The exported index's global id.
        index_id: u64,
        /// Shard alias to import; must already exist.
        shard: String,
        /// Columns to arrange by.
        key: Vec<usize>,
        /// The dataflow's `as_of`.
        as_of: u64,
        /// The shard's exclusive write upper (see `PersistSource::upper`).
        upper: u64,
    },
    /// Schedule a previously-submitted collection so it makes progress.
    Schedule {
        /// The collection's global id.
        id: u64,
    },
    /// Advance an index's read frontier (`since`) via `AllowCompaction`.
    AllowCompaction {
        /// The index's global id.
        id: u64,
        /// The new read frontier.
        frontier: u64,
    },
    /// Wait until `id`'s output frontier reaches `ts`, or fail after the timeout.
    AwaitFrontier {
        /// The collection's global id.
        id: u64,
        /// The target output-frontier timestamp.
        ts: u64,
        /// Timeout in seconds; defaults to [`DEFAULT_TIMEOUT_SECS`].
        #[serde(default)]
        timeout_secs: Option<u64>,
    },
    /// Peek `id` at `ts` and return the row count.
    PeekCount {
        /// The index's global id.
        id: u64,
        /// The timestamp to peek at.
        ts: u64,
    },
}

/// A response written for each command. Tagged on `"status"`, snake_case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    /// A command with no return value succeeded.
    Ok,
    /// A write succeeded; reports the number of rows written.
    Wrote {
        /// Rows written.
        rows: u64,
    },
    /// A peek succeeded; reports the row count.
    Count {
        /// Row count.
        count: u64,
    },
    /// A command failed.
    Error {
        /// Human-readable failure description.
        message: String,
    },
}

/// Mutable state threaded through a script run.
pub struct ScriptState {
    driver: Driver,
    client: PersistClient,
    loc: PersistLocation,
    desc: RelationDesc,
    /// Alias-to-shard map; aliases are allocated lazily on first use.
    shards: BTreeMap<String, ShardId>,
}

impl ScriptState {
    /// Build the state from a connected driver and its persist location, opening a
    /// persist client and fixing the synthetic schema.
    pub async fn new(driver: Driver, loc: PersistLocation) -> anyhow::Result<Self> {
        let client = driver.host.client().await?;
        Ok(ScriptState {
            driver,
            client,
            loc,
            desc: sample_desc(),
            shards: BTreeMap::new(),
        })
    }

    /// Resolve a shard alias, allocating a fresh [`ShardId`] on first use.
    fn shard_id(&mut self, alias: &str) -> ShardId {
        *self
            .shards
            .entry(alias.to_string())
            .or_insert_with(ShardId::new)
    }

    /// Execute a single command.
    pub async fn execute(&mut self, cmd: Command) -> anyhow::Result<Response> {
        match cmd {
            Command::WriteSingleTs {
                shard,
                ts,
                rows,
                row_bytes,
            } => {
                let shard = self.shard_id(&shard);
                let pad = row_bytes.unwrap_or(DEFAULT_ROW_BYTES);
                let batch = sample_rows(rows, pad);
                write_rows_single_ts(&self.client, shard, &self.desc, &batch, Timestamp::from(ts))
                    .await?;
                Ok(Response::Wrote { rows })
            }
            Command::WriteSpread {
                shard,
                rows,
                n_ts,
                row_bytes,
            } => {
                let shard = self.shard_id(&shard);
                let pad = row_bytes.unwrap_or(DEFAULT_ROW_BYTES);
                let batch = sample_rows(rows, pad);
                write_rows_spread(&self.client, shard, &self.desc, &batch, n_ts).await?;
                Ok(Response::Wrote { rows })
            }
            Command::DefineIndex {
                source_id,
                index_id,
                shard,
                key,
                as_of,
                upper,
            } => {
                let shard = self.shard_id(&shard);
                let df = index_dataflow(
                    GlobalId::User(source_id),
                    GlobalId::User(index_id),
                    shard,
                    self.loc.clone(),
                    self.desc.clone(),
                    key,
                    Timestamp::from(as_of),
                    Timestamp::from(upper),
                );
                self.driver.submit_dataflow(df)?;
                Ok(Response::Ok)
            }
            Command::Schedule { id } => {
                self.driver.schedule(GlobalId::User(id))?;
                Ok(Response::Ok)
            }
            Command::AllowCompaction { id, frontier } => {
                self.driver.send(ComputeCommand::AllowCompaction {
                    id: GlobalId::User(id),
                    frontier: Antichain::from_elem(Timestamp::from(frontier)),
                })?;
                Ok(Response::Ok)
            }
            Command::AwaitFrontier {
                id,
                ts,
                timeout_secs,
            } => {
                let timeout = Duration::from_secs(timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
                self.driver
                    .expect_frontier(GlobalId::User(id), Timestamp::from(ts), timeout)
                    .await?;
                Ok(Response::Ok)
            }
            Command::PeekCount { id, ts } => {
                let count = self
                    .driver
                    .peek_count(GlobalId::User(id), self.desc.clone(), Timestamp::from(ts))
                    .await?;
                Ok(Response::Count {
                    count: u64::cast_from(count),
                })
            }
        }
    }
}

/// Run the command loop: read JSON-line commands from stdin, execute each, and
/// write one JSON response per line to stdout.
///
/// Returns `Err` if any command failed, so a scripted run exits non-zero on the
/// first bad command while still reporting per-command results.
pub async fn run(driver: Driver, loc: PersistLocation) -> anyhow::Result<()> {
    let mut state = ScriptState::new(driver, loc).await?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    let mut failed = false;

    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("//") {
            continue;
        }
        let response = match serde_json::from_str::<Command>(trimmed) {
            Ok(cmd) => match state.execute(cmd).await {
                Ok(response) => response,
                Err(e) => {
                    failed = true;
                    Response::Error {
                        message: e.to_string(),
                    }
                }
            },
            Err(e) => {
                failed = true;
                Response::Error {
                    message: format!("parse error: {e}"),
                }
            }
        };
        let mut json = serde_json::to_string(&response)?;
        json.push('\n');
        stdout.write_all(json.as_bytes()).await?;
        stdout.flush().await?;
    }

    if failed {
        anyhow::bail!("one or more script commands failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Commands deserialize from their documented JSON form, and responses
    /// serialize to the documented tagged form.
    #[mz_ore::test]
    fn command_and_response_serde() {
        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"write_single_ts","shard":"s1","ts":0,"rows":1000}"#)
                .unwrap();
        assert_eq!(
            cmd,
            Command::WriteSingleTs {
                shard: "s1".to_string(),
                ts: 0,
                rows: 1000,
                row_bytes: None,
            }
        );

        let cmd: Command = serde_json::from_str(
            r#"{"cmd":"define_index","source_id":1000,"index_id":1001,"shard":"s1","key":[0],"as_of":0,"upper":1}"#,
        )
        .unwrap();
        assert_eq!(
            cmd,
            Command::DefineIndex {
                source_id: 1000,
                index_id: 1001,
                shard: "s1".to_string(),
                key: vec![0],
                as_of: 0,
                upper: 1,
            }
        );

        // Optional field present.
        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"await_frontier","id":1001,"ts":1,"timeout_secs":30}"#)
                .unwrap();
        assert_eq!(
            cmd,
            Command::AwaitFrontier {
                id: 1001,
                ts: 1,
                timeout_secs: Some(30),
            }
        );

        assert_eq!(
            serde_json::to_string(&Response::Count { count: 5 }).unwrap(),
            r#"{"status":"count","count":5}"#
        );
        assert_eq!(
            serde_json::to_string(&Response::Ok).unwrap(),
            r#"{"status":"ok"}"#
        );
    }
}
