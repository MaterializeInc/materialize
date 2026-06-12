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
use mz_repr::{GlobalId, RelationDesc, Row, SqlColumnType, SqlScalarType, Timestamp};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use timely::progress::Antichain;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt};

use crate::data::{
    Cell, pack_cells, sample_desc, synth_rows, write_rows_single_ts, write_rows_spread,
};
use crate::dataflow::index_dataflow;
use crate::driver::Driver;

/// The default payload padding (bytes) for synthetic rows when a command omits it.
const DEFAULT_ROW_BYTES: usize = 64;
/// The default timeout (seconds) for `await_frontier` when a command omits it.
const DEFAULT_TIMEOUT_SECS: u64 = 600;

/// A column declaration in a `define_schema` command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    /// Column name.
    pub name: String,
    /// Scalar type name; see [`scalar_type_from_str`].
    #[serde(rename = "type")]
    pub ty: String,
    /// Whether the column admits `NULL`.
    #[serde(default)]
    pub nullable: bool,
}

/// Map a JSON type name to a [`SqlScalarType`]. The supported set is intentionally
/// small and matches [`crate::data::Cell`]; extend both together.
fn scalar_type_from_str(s: &str) -> anyhow::Result<SqlScalarType> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "int16" | "smallint" => SqlScalarType::Int16,
        "int32" | "int" | "integer" => SqlScalarType::Int32,
        "int64" | "bigint" => SqlScalarType::Int64,
        "bool" | "boolean" => SqlScalarType::Bool,
        "string" | "text" => SqlScalarType::String,
        "bytes" | "bytea" => SqlScalarType::Bytes,
        other => anyhow::bail!("unsupported column type {other:?}"),
    })
}

/// Build a [`RelationDesc`] from column specs.
fn relation_desc(columns: &[ColumnSpec]) -> anyhow::Result<RelationDesc> {
    let mut builder = RelationDesc::builder();
    for col in columns {
        builder = builder.with_column(
            col.name.as_str(),
            SqlColumnType {
                scalar_type: scalar_type_from_str(&col.ty)?,
                nullable: col.nullable,
            },
        );
    }
    Ok(builder.finish())
}

/// Parse a JSON value into an owned [`Cell`] for `col`. This is the reusable
/// literal shim: the same `JSON value + ColumnType -> Datum` mapping that a later
/// full-MIR `define` command will use for literal constants. `Bytes` columns take
/// a string whose UTF-8 encoding is the value.
fn cell_from_json(value: &Value, col: &SqlColumnType) -> anyhow::Result<Cell> {
    if value.is_null() {
        anyhow::ensure!(col.nullable, "null value in non-nullable column");
        return Ok(Cell::Null);
    }
    let cell = match col.scalar_type {
        SqlScalarType::Int16 => Cell::Int16(i16::try_from(json_i64(value)?)?),
        SqlScalarType::Int32 => Cell::Int32(i32::try_from(json_i64(value)?)?),
        SqlScalarType::Int64 => Cell::Int64(json_i64(value)?),
        SqlScalarType::Bool => Cell::Bool(
            value
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("expected bool, got {value}"))?,
        ),
        SqlScalarType::String => Cell::Str(
            value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("expected string, got {value}"))?
                .to_string(),
        ),
        SqlScalarType::Bytes => Cell::Bytes(
            value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("expected string (utf8 bytes), got {value}"))?
                .as_bytes()
                .to_vec(),
        ),
        ref other => anyhow::bail!("unsupported column type {other:?}"),
    };
    Ok(cell)
}

/// Extract an `i64` from a JSON number, erroring on non-integers.
fn json_i64(value: &Value) -> anyhow::Result<i64> {
    value
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("expected integer, got {value}"))
}

/// Pack explicit JSON rows against `desc`, validating arity per row.
fn rows_from_json(desc: &RelationDesc, rows: &[Vec<Value>]) -> anyhow::Result<Vec<Row>> {
    let cols: Vec<&SqlColumnType> = desc.iter_types().collect();
    let mut out = Vec::with_capacity(rows.len());
    for (r, row) in rows.iter().enumerate() {
        anyhow::ensure!(
            row.len() == cols.len(),
            "row {r} has {} values but schema has {} columns",
            row.len(),
            cols.len()
        );
        // Arity validated above, so indexing `cols` by position is in bounds.
        let cells = row
            .iter()
            .enumerate()
            .map(|(c, v)| cell_from_json(v, cols[c]))
            .collect::<anyhow::Result<Vec<Cell>>>()?;
        out.push(pack_cells(&cells));
    }
    Ok(out)
}

/// A command read from the script stream.
///
/// Tagged on `"cmd"`, snake_case, e.g.
/// `{"cmd":"write_single_ts","shard":"s1","ts":0,"rows":1000}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    /// Declare a named relation schema for later `schema` references.
    DefineSchema {
        /// Schema name, referenced by `schema` fields on other commands.
        name: String,
        /// Ordered column declarations.
        columns: Vec<ColumnSpec>,
    },
    /// Write `count` synthetic rows to `shard` at a single timestamp `ts`.
    WriteSingleTs {
        /// Shard alias; allocated on first use.
        shard: String,
        /// Schema name; defaults to the built-in `(bigint, text)` sample schema.
        #[serde(default)]
        schema: Option<String>,
        /// The timestamp to write at.
        ts: u64,
        /// Number of synthetic rows to write.
        count: u64,
        /// First synthetic row index, so successive batches can use disjoint id
        /// ranges (`start..start + count`) that never consolidate. Defaults to 0.
        #[serde(default)]
        start: u64,
        /// Payload padding per row; defaults to [`DEFAULT_ROW_BYTES`].
        #[serde(default)]
        row_bytes: Option<usize>,
    },
    /// Write `count` synthetic rows to `shard`, spread across `n_ts` timestamps in a
    /// single append.
    WriteSpread {
        /// Shard alias; allocated on first use.
        shard: String,
        /// Schema name; defaults to the built-in sample schema.
        #[serde(default)]
        schema: Option<String>,
        /// Number of synthetic rows to write.
        count: u64,
        /// Number of distinct timestamps to spread the rows across.
        n_ts: u64,
        /// First synthetic row index (see [`Command::WriteSingleTs`]). Defaults to 0.
        #[serde(default)]
        start: u64,
        /// Payload padding per row; defaults to [`DEFAULT_ROW_BYTES`].
        #[serde(default)]
        row_bytes: Option<usize>,
    },
    /// Write explicit rows to `shard` at a single timestamp `ts`. Each row is an
    /// array of JSON values matching the schema's columns in order.
    WriteRows {
        /// Shard alias; allocated on first use.
        shard: String,
        /// Schema name; defaults to the built-in sample schema.
        #[serde(default)]
        schema: Option<String>,
        /// The timestamp to write at.
        ts: u64,
        /// Rows as arrays of column values.
        rows: Vec<Vec<Value>>,
    },
    /// Submit (without scheduling) an index dataflow over `shard`.
    DefineIndex {
        /// The imported source's global id.
        source_id: u64,
        /// The exported index's global id.
        index_id: u64,
        /// Shard alias to import; must already exist.
        shard: String,
        /// Schema name; defaults to the built-in sample schema. Must match what was
        /// written to `shard`.
        #[serde(default)]
        schema: Option<String>,
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
        /// If true, a timeout is reported (`status: timeout`) without failing the
        /// run. Used by reproductions where not reaching the frontier is an
        /// expected outcome, not an assertion failure.
        #[serde(default)]
        allow_timeout: bool,
    },
    /// Peek `id` at `ts` and return the row count.
    PeekCount {
        /// The index's global id.
        id: u64,
        /// Schema name; defaults to the built-in sample schema. Must match the
        /// index's output relation.
        #[serde(default)]
        schema: Option<String>,
        /// The timestamp to peek at.
        ts: u64,
    },
    /// Peek `id` at `ts` and assert the row count equals `count`, failing the run
    /// otherwise. The scripted equivalent of a scenario's count assertion.
    ExpectCount {
        /// The index's global id.
        id: u64,
        /// Schema name; defaults to the built-in sample schema.
        #[serde(default)]
        schema: Option<String>,
        /// The timestamp to peek at.
        ts: u64,
        /// The expected row count.
        count: u64,
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
    /// An `await_frontier` with `allow_timeout` timed out without reaching the
    /// target. Reported, not a failure.
    Timeout,
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
    /// Named schemas declared via `define_schema`.
    schemas: BTreeMap<String, RelationDesc>,
    /// Alias-to-shard map; aliases are allocated lazily on first use.
    shards: BTreeMap<String, ShardId>,
}

impl ScriptState {
    /// Build the state from a connected driver and its persist location, opening a
    /// persist client.
    pub async fn new(driver: Driver, loc: PersistLocation) -> anyhow::Result<Self> {
        let client = driver.host.client().await?;
        Ok(ScriptState {
            driver,
            client,
            loc,
            schemas: BTreeMap::new(),
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

    /// Resolve a schema name, defaulting to the built-in sample schema when absent.
    fn resolve_schema(&self, name: &Option<String>) -> anyhow::Result<RelationDesc> {
        match name {
            None => Ok(sample_desc()),
            Some(name) => self.schemas.get(name).cloned().ok_or_else(|| {
                anyhow::anyhow!("unknown schema {name:?}; declare it with define_schema first")
            }),
        }
    }

    /// Execute a single command.
    pub async fn execute(&mut self, cmd: Command) -> anyhow::Result<Response> {
        match cmd {
            Command::DefineSchema { name, columns } => {
                let desc = relation_desc(&columns)?;
                self.schemas.insert(name, desc);
                Ok(Response::Ok)
            }
            Command::WriteSingleTs {
                shard,
                schema,
                ts,
                count,
                start,
                row_bytes,
            } => {
                let desc = self.resolve_schema(&schema)?;
                let shard = self.shard_id(&shard);
                let pad = row_bytes.unwrap_or(DEFAULT_ROW_BYTES);
                let batch = synth_rows(&desc, start, count, pad);
                write_rows_single_ts(&self.client, shard, &desc, &batch, Timestamp::from(ts))
                    .await?;
                Ok(Response::Wrote { rows: count })
            }
            Command::WriteSpread {
                shard,
                schema,
                count,
                n_ts,
                start,
                row_bytes,
            } => {
                let desc = self.resolve_schema(&schema)?;
                let shard = self.shard_id(&shard);
                let pad = row_bytes.unwrap_or(DEFAULT_ROW_BYTES);
                let batch = synth_rows(&desc, start, count, pad);
                write_rows_spread(&self.client, shard, &desc, &batch, n_ts).await?;
                Ok(Response::Wrote { rows: count })
            }
            Command::WriteRows {
                shard,
                schema,
                ts,
                rows,
            } => {
                let desc = self.resolve_schema(&schema)?;
                let batch = rows_from_json(&desc, &rows)?;
                let written = u64::cast_from(batch.len());
                let shard = self.shard_id(&shard);
                write_rows_single_ts(&self.client, shard, &desc, &batch, Timestamp::from(ts))
                    .await?;
                Ok(Response::Wrote { rows: written })
            }
            Command::DefineIndex {
                source_id,
                index_id,
                shard,
                schema,
                key,
                as_of,
                upper,
            } => {
                let desc = self.resolve_schema(&schema)?;
                let shard = self.shard_id(&shard);
                let df = index_dataflow(
                    GlobalId::User(source_id),
                    GlobalId::User(index_id),
                    shard,
                    self.loc.clone(),
                    desc,
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
                allow_timeout,
            } => {
                let timeout = Duration::from_secs(timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
                let result = self
                    .driver
                    .expect_frontier(GlobalId::User(id), Timestamp::from(ts), timeout)
                    .await;
                match result {
                    Ok(()) => Ok(Response::Ok),
                    // `expect_frontier` only errors on timeout. When tolerated,
                    // report it instead of failing the run.
                    Err(_) if allow_timeout => Ok(Response::Timeout),
                    Err(e) => Err(e),
                }
            }
            Command::PeekCount { id, schema, ts } => {
                let desc = self.resolve_schema(&schema)?;
                let count = self
                    .driver
                    .peek_count(GlobalId::User(id), desc, Timestamp::from(ts))
                    .await?;
                Ok(Response::Count {
                    count: u64::cast_from(count),
                })
            }
            Command::ExpectCount {
                id,
                schema,
                ts,
                count,
            } => {
                let desc = self.resolve_schema(&schema)?;
                let actual = u64::cast_from(
                    self.driver
                        .peek_count(GlobalId::User(id), desc, Timestamp::from(ts))
                        .await?,
                );
                anyhow::ensure!(
                    actual == count,
                    "expected {count} rows at ts {ts}, got {actual}"
                );
                Ok(Response::Count { count: actual })
            }
        }
    }
}

/// Run the command loop: read JSON-line commands from `reader`, execute each, and
/// write one JSON response per line to stdout.
///
/// `reader` is any buffered async source — a file (`DRIVER_SCRIPT`) or stdin.
/// Returns `Err` if any command failed, so a scripted run exits non-zero on the
/// first bad command while still reporting per-command results.
pub async fn run<R: AsyncBufRead + Unpin>(
    driver: Driver,
    loc: PersistLocation,
    reader: R,
) -> anyhow::Result<()> {
    let mut state = ScriptState::new(driver, loc).await?;
    let mut lines = reader.lines();
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

    /// Commands deserialize from their documented JSON form (optional `schema`
    /// defaults to `None`), and responses serialize to the documented tagged form.
    #[mz_ore::test]
    fn command_and_response_serde() {
        let cmd: Command =
            serde_json::from_str(r#"{"cmd":"write_single_ts","shard":"s1","ts":0,"count":1000}"#)
                .unwrap();
        assert_eq!(
            cmd,
            Command::WriteSingleTs {
                shard: "s1".to_string(),
                schema: None,
                ts: 0,
                count: 1000,
                start: 0,
                row_bytes: None,
            }
        );

        let cmd: Command = serde_json::from_str(
            r#"{"cmd":"define_schema","name":"kv","columns":[{"name":"k","type":"bigint"},{"name":"v","type":"text","nullable":true}]}"#,
        )
        .unwrap();
        assert_eq!(
            cmd,
            Command::DefineSchema {
                name: "kv".to_string(),
                columns: vec![
                    ColumnSpec {
                        name: "k".to_string(),
                        ty: "bigint".to_string(),
                        nullable: false,
                    },
                    ColumnSpec {
                        name: "v".to_string(),
                        ty: "text".to_string(),
                        nullable: true,
                    },
                ],
            }
        );

        let cmd: Command = serde_json::from_str(
            r#"{"cmd":"write_rows","shard":"s1","schema":"kv","ts":0,"rows":[[5,"x"],[6,null]]}"#,
        )
        .unwrap();
        assert_eq!(
            cmd,
            Command::WriteRows {
                shard: "s1".to_string(),
                schema: Some("kv".to_string()),
                ts: 0,
                rows: vec![
                    vec![Value::from(5), Value::from("x")],
                    vec![Value::from(6), Value::Null],
                ],
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

    /// `define_schema` types map to a `RelationDesc` with matching arity and
    /// nullability, and `synth_rows` fills it.
    #[mz_ore::test]
    fn schema_parse_and_synth() {
        let columns = vec![
            ColumnSpec {
                name: "k".to_string(),
                ty: "bigint".to_string(),
                nullable: false,
            },
            ColumnSpec {
                name: "flag".to_string(),
                ty: "boolean".to_string(),
                nullable: false,
            },
            ColumnSpec {
                name: "v".to_string(),
                ty: "text".to_string(),
                nullable: true,
            },
        ];
        let desc = relation_desc(&columns).unwrap();
        assert_eq!(desc.arity(), 3);
        let types: Vec<_> = desc.iter_types().collect();
        assert_eq!(types[0].scalar_type, SqlScalarType::Int64);
        assert_eq!(types[1].scalar_type, SqlScalarType::Bool);
        assert!(types[2].nullable);

        let rows = synth_rows(&desc, 0, 4, 8);
        assert_eq!(rows.len(), 4);

        assert!(scalar_type_from_str("nope").is_err());
    }

    /// JSON values parse into the right `Cell`s, and `null` is rejected for
    /// non-nullable columns.
    #[mz_ore::test]
    fn cell_from_json_maps_values() {
        let int_col = SqlColumnType {
            scalar_type: SqlScalarType::Int64,
            nullable: false,
        };
        let str_col = SqlColumnType {
            scalar_type: SqlScalarType::String,
            nullable: true,
        };
        assert_eq!(
            cell_from_json(&Value::from(7), &int_col).unwrap(),
            Cell::Int64(7)
        );
        assert_eq!(
            cell_from_json(&Value::from("hi"), &str_col).unwrap(),
            Cell::Str("hi".to_string())
        );
        assert_eq!(cell_from_json(&Value::Null, &str_col).unwrap(), Cell::Null);
        // null into a non-nullable column is an error.
        assert!(cell_from_json(&Value::Null, &int_col).is_err());
        // wrong JSON type is an error.
        assert!(cell_from_json(&Value::from("x"), &int_col).is_err());
    }
}
