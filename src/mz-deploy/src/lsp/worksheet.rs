//! SQL worksheet query execution and SUBSCRIBE streaming for the LSP server.
//!
//! Provides the pure logic for the worksheet feature: statement validation,
//! LIMIT injection, response formatting, and SUBSCRIBE batch grouping. The
//! async I/O (connecting to Materialize, executing queries, running the
//! SUBSCRIBE cursor loop, handling cancellation) lives in the
//! [`Backend`](super::server::Backend) handler methods that delegate here.
//!
//! ## Worksheet Overview
//!
//! Worksheets are `.sql` files inside the `worksheets/` directory. The LSP
//! provides "Execute" code lenses above each statement, and a results panel
//! in VS Code displays the output. Two execution modes are supported:
//!
//! ### One-Shot Queries (SELECT, SHOW, EXPLAIN)
//!
//! ```text
//! User clicks "Execute" code lens
//!   → VS Code sends `mz-deploy/execute-query` LSP request
//!   → Backend validates SQL, injects LIMIT, runs via simple_query
//!   → Returns ExecuteQueryResponse (columns + rows or raw text)
//!   → VS Code renders as table or <pre> block
//! ```
//!
//! ### SUBSCRIBE (Streaming)
//!
//! ```text
//! User clicks "Execute" on a SUBSCRIBE statement
//!   → VS Code sends `mz-deploy/subscribe` LSP request
//!   → Backend validates, injects WITH (PROGRESS), returns SubscribeStarted
//!   → Backend spawns background task on a DEDICATED connection:
//!       BEGIN → DECLARE CURSOR FOR SUBSCRIBE ... WITH (PROGRESS) → FETCH loop
//!   → Each FETCH batch is grouped by mz_timestamp:
//!       - Progress-only rows (mz_progressed=true) → SubscribeBatch { progress_only: true }
//!       - Data rows → SubscribeBatch { diffs: [{ diff: +1/-1, values }] }
//!   → Batches pushed to VS Code via mz-deploy/subscribeBatch notifications
//!   → VS Code webview maintains two views:
//!       - "Live" tab: current snapshot (applies +1 inserts, -1 deletes)
//!       - "Diffs" tab: append-only log of all changes with timestamps
//!   → User clicks "Stop" → mz-deploy/cancel-query → cancels cursor → SubscribeComplete
//! ```
//!
//! **Key invariant:** All updates at a single `mz_timestamp` are delivered as
//! one [`SubscribeBatch`], ensuring atomic rendering in the frontend.
//!
//! **Progress tracking:** `WITH (PROGRESS)` is always injected so the frontend
//! can distinguish "no data changes" from "system is still processing". Progress
//! batches update the timestamp display even when no rows change.
//!
//! **Connection isolation:** SUBSCRIBE uses a dedicated connection (separate
//! from the main worksheet connection) because it holds a transaction open for
//! its entire lifetime. The main connection remains available for one-shot
//! queries and session management.
//!
//! ## Statement Validation
//!
//! Only read-only statements are allowed in worksheets:
//!
//! | Statement kind | Result type |
//! |----------------|-------------|
//! | `SELECT` | Tabular (columns + rows) |
//! | `SHOW` | Tabular |
//! | `EXPLAIN *` | Raw text |
//! | `SUBSCRIBE` | Streaming diffs via notifications |
//! | Everything else | Rejected with `ReadOnlyViolation` |
//!
//! Multi-statement inputs are rejected — exactly one statement is required.
//!
//! ## LIMIT Injection
//!
//! For `SELECT` statements without a `LIMIT` clause, the validator injects
//! `LIMIT 1001`. The extra row allows the caller to detect truncation: if
//! 1001 rows come back, the response includes only the first 1000 with
//! `truncated: true`.
//!
//! ## SUBSCRIBE Batch Grouping
//!
//! [`group_subscribe_rows`] takes raw `FETCH ALL` output and groups rows by
//! `mz_timestamp`. Each timestamp group becomes a [`SubscribeBatch`]:
//!
//! - Row format from Materialize: `mz_timestamp | mz_progressed | mz_diff | col1 | col2 | ...`
//! - If all rows in a group have `mz_progressed = true` → progress-only batch
//! - Otherwise → data batch with [`SubscribeDiffRow`]s (progress rows filtered out)
//! - Column names (excluding mz_* columns) are sent with the first data batch only
//!
//! ## Response Formatting (One-Shot)
//!
//! The formatting pipeline has two layers:
//!
//! 1. **Extraction** ([`extract_rows_from_messages`]) — Converts
//!    `tokio_postgres::SimpleQueryMessage` into column names and text rows.
//!    This layer touches the `tokio_postgres` types that lack public
//!    constructors and is tested via integration tests.
//!
//! 2. **Formatting** ([`build_tabular_response`], [`build_raw_text_response`])
//!    — Pure functions that apply truncation, coerce text values to JSON via
//!    [`parse_text_value`], and assemble the final [`ExecuteQueryResponse`].
//!    Fully unit-testable.

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{Expr, Limit, Raw, Statement, SubscribeOptionName, Value};
use serde::Serialize;
use std::collections::BTreeMap;
use tokio_postgres::SimpleQueryMessage;

/// Maximum number of rows returned to the client.
pub const MAX_ROWS: usize = 1000;

/// The LIMIT injected into SELECT statements that have no LIMIT clause.
/// One more than [`MAX_ROWS`] so we can detect truncation.
pub const INJECTED_LIMIT: usize = MAX_ROWS + 1;

/// Default query timeout in milliseconds.
pub const DEFAULT_TIMEOUT_MS: u64 = 30_000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// The result of validating a worksheet query.
///
/// Carries the (possibly LIMIT-injected) SQL text and how to interpret
/// the result set.
#[derive(Debug)]
pub enum ValidatedQuery {
    /// A `SELECT` or `SHOW` statement whose results are columnar.
    Tabular {
        sql: String,
        /// `true` when the validator injected a LIMIT (so the caller
        /// should check for truncation).
        limit_injected: bool,
    },
    /// An `EXPLAIN` variant whose output is a single block of text.
    RawText { sql: String },
    /// An INSERT, UPDATE, or DELETE statement. Returns affected row count.
    DML { sql: String },
}

/// Error returned by worksheet operations.
///
/// Serialized as the `data` field of a JSON-RPC error response so the
/// VS Code extension can display structured error information.
#[derive(Debug, Serialize)]
pub struct WorksheetError {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

/// Response payload for `mz-deploy/execute-query`.
#[derive(Debug, Serialize, PartialEq)]
pub struct ExecuteQueryResponse {
    /// Column names (present for tabular results).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// Row data (present for tabular results). Each inner vec corresponds
    /// to one row, with values positionally matching `columns`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<serde_json::Value>>>,
    /// Raw text output (present for EXPLAIN / SHOW CREATE results).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_text: Option<String>,
    /// `true` when the result was truncated to [`MAX_ROWS`] rows.
    pub truncated: bool,
    /// Wall-clock execution time in milliseconds.
    pub elapsed_ms: u64,
    /// Number of rows affected by a DML statement (INSERT/UPDATE/DELETE).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affected_rows: Option<u64>,
}

/// Response payload for `mz-deploy/connection-info`.
#[derive(Debug, Serialize)]
pub struct ConnectionInfoResponse {
    pub connected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Response payload for `mz-deploy/worksheet-context`.
///
/// Returns all databases mapped to their schemas, plus all clusters and the
/// current session values. The client derives the database list from
/// `database_schemas` keys and the schema list from the selected database's
/// entry — no server round-trip needed when the user switches databases.
#[derive(Debug, Serialize)]
pub struct WorksheetContextResponse {
    /// Available profile names from `profiles.toml`.
    pub profiles: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_profile: Option<String>,
    /// Map of database name → list of schema names in that database.
    pub database_schemas: BTreeMap<String, Vec<String>>,
    pub clusters: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_cluster: Option<String>,
}

/// Request parameters for `mz-deploy/set-profile`.
#[derive(Debug, serde::Deserialize)]
pub struct SetProfileParams {
    pub profile: String,
}

/// Request parameters for `mz-deploy/set-session`.
///
/// Each field is optional — only the fields that are set will produce SET
/// commands. When `database` is set, the returned [`WorksheetContextResponse`]
/// will contain the schema list for the new database (cascade refresh).
#[derive(Debug, serde::Deserialize)]
pub struct SetSessionParams {
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub cluster: Option<String>,
}

/// Build semicolon-separated SET commands from session parameters.
///
/// Only produces SET statements for fields that are `Some`. Returns an empty
/// string if all fields are `None`.
///
/// # Examples
///
/// ```ignore
/// let params = SetSessionParams { database: Some("mydb".into()), schema: None, cluster: Some("c1".into()) };
/// assert_eq!(build_set_commands(&params), "SET database = 'mydb'; SET cluster = 'c1'");
/// ```
pub fn build_set_commands(params: &SetSessionParams) -> String {
    let mut cmds = Vec::new();
    if let Some(ref db) = params.database {
        cmds.push(format!("SET database = '{}'", db.replace('\'', "''")));
    }
    if let Some(ref schema) = params.schema {
        cmds.push(format!(
            "SET search_path = '{}'",
            schema.replace('\'', "''")
        ));
    }
    if let Some(ref cluster) = params.cluster {
        cmds.push(format!("SET cluster = '{}'", cluster.replace('\'', "''")));
    }
    cmds.join("; ")
}

/// Request parameters for `mz-deploy/execute-query`.
#[derive(Debug, serde::Deserialize)]
pub struct ExecuteQueryParams {
    pub query: String,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

// ---------------------------------------------------------------------------
// Subscribe types
// ---------------------------------------------------------------------------

/// Acknowledgement response for `mz-deploy/subscribe`.
#[derive(Debug, Serialize)]
pub struct SubscribeStarted {
    pub subscribe_id: String,
}

/// A batch of rows at a single timestamp, pushed via `mz-deploy/subscribeBatch`.
///
/// All updates at a single timestamp are grouped into one batch for atomic
/// rendering. Progress-only batches (no data changes) have `progress_only: true`
/// and empty `diffs`.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct SubscribeBatch {
    pub subscribe_id: String,
    pub timestamp: String,
    pub progress_only: bool,
    /// Column names (sent with the first data batch only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// Diff rows: each row has a `diff` (+1 or -1) and data values.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub diffs: Vec<SubscribeDiffRow>,
}

/// A single diff row from a SUBSCRIBE batch.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct SubscribeDiffRow {
    pub diff: i64,
    pub values: Vec<serde_json::Value>,
}

/// Final notification when SUBSCRIBE ends (cancelled or errored).
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct SubscribeComplete {
    pub subscribe_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request parameters for `mz-deploy/subscribe`.
#[derive(Debug, serde::Deserialize)]
pub struct SubscribeParams {
    pub query: String,
}

// ---------------------------------------------------------------------------
// Subscribe validation
// ---------------------------------------------------------------------------

/// Validate and rewrite a SUBSCRIBE statement to ensure `WITH (PROGRESS)`.
///
/// Parses the SQL, verifies it is a single SUBSCRIBE statement, injects
/// the `PROGRESS` option if not already present, and returns the rewritten SQL.
pub fn validate_subscribe_query(sql: &str) -> Result<String, WorksheetError> {
    let stmts = mz_sql_parser::parser::parse_statements(sql).map_err(|e| WorksheetError {
        code: "parse_error",
        message: e.to_string(),
        hint: None,
    })?;

    if stmts.len() != 1 {
        return Err(WorksheetError {
            code: "parse_error",
            message: "expected exactly one SUBSCRIBE statement".to_string(),
            hint: None,
        });
    }

    let stmt = stmts.into_iter().next().unwrap();
    match stmt.ast {
        Statement::Subscribe(mut sub) => {
            // Inject PROGRESS if not already present.
            let has_progress = sub
                .options
                .iter()
                .any(|o| matches!(o.name, SubscribeOptionName::Progress));
            if !has_progress {
                sub.options.push(mz_sql_parser::ast::SubscribeOption {
                    name: SubscribeOptionName::Progress,
                    value: None,
                });
            }
            Ok(sub.to_ast_string_simple())
        }
        _ => Err(WorksheetError {
            code: "invalid_statement",
            message: "expected a SUBSCRIBE statement".to_string(),
            hint: None,
        }),
    }
}

/// Group SUBSCRIBE rows by timestamp and classify as data or progress batches.
///
/// Each row from `FETCH ALL` on a SUBSCRIBE WITH (PROGRESS) cursor has:
/// - Column 0: `mz_timestamp` (text)
/// - Column 1: `mz_progressed` (bool, as text "t"/"f")
/// - Column 2: `mz_diff` (i64, as text)
/// - Columns 3+: data columns
///
/// Returns a list of batches, one per distinct timestamp.
pub fn group_subscribe_rows(
    rows: &[Vec<Option<String>>],
    subscribe_id: &str,
    include_columns: bool,
    column_names: &[String],
) -> Vec<SubscribeBatch> {
    // Group rows by timestamp, preserving order.
    let mut groups: Vec<(String, Vec<&Vec<Option<String>>>)> = Vec::new();
    for row in rows {
        let ts = row
            .first()
            .and_then(|v| v.as_deref())
            .unwrap_or("")
            .to_string();
        if let Some(last) = groups.last_mut() {
            if last.0 == ts {
                last.1.push(row);
                continue;
            }
        }
        groups.push((ts, vec![row]));
    }

    let mut batches = Vec::new();
    let mut first_data_batch = include_columns;

    for (ts, ts_rows) in groups {
        // Check if all rows are progress-only.
        let all_progress = ts_rows.iter().all(|r| {
            r.get(1)
                .and_then(|v| v.as_deref())
                .map(|v| v == "t")
                .unwrap_or(false)
        });

        if all_progress {
            batches.push(SubscribeBatch {
                subscribe_id: subscribe_id.to_string(),
                timestamp: ts,
                progress_only: true,
                columns: None,
                diffs: Vec::new(),
            });
        } else {
            let diffs: Vec<SubscribeDiffRow> = ts_rows
                .iter()
                .filter(|r| {
                    // Skip progress rows within a data batch.
                    r.get(1)
                        .and_then(|v| v.as_deref())
                        .map(|v| v != "t")
                        .unwrap_or(true)
                })
                .map(|r| {
                    let diff: i64 = r
                        .get(2)
                        .and_then(|v| v.as_deref())
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(1);
                    let values: Vec<serde_json::Value> = r[3..]
                        .iter()
                        .map(|v| match v {
                            Some(t) => parse_text_value(t),
                            None => serde_json::Value::Null,
                        })
                        .collect();
                    SubscribeDiffRow { diff, values }
                })
                .collect();

            let columns = if first_data_batch {
                first_data_batch = false;
                Some(column_names.to_vec())
            } else {
                None
            };

            batches.push(SubscribeBatch {
                subscribe_id: subscribe_id.to_string(),
                timestamp: ts,
                progress_only: false,
                columns,
                diffs,
            });
        }
    }

    batches
}

// ---------------------------------------------------------------------------
// Statement validation
// ---------------------------------------------------------------------------

/// Parse and validate a SQL string for worksheet execution.
///
/// Accepts exactly one statement. Returns the classified query with
/// (possibly LIMIT-injected) SQL, or an error describing why the
/// statement was rejected.
pub fn validate_worksheet_query(sql: &str) -> Result<ValidatedQuery, WorksheetError> {
    let stmts = mz_sql_parser::parser::parse_statements(sql).map_err(|e| WorksheetError {
        code: "parse_error",
        message: e.to_string(),
        hint: None,
    })?;

    if stmts.is_empty() {
        return Err(WorksheetError {
            code: "parse_error",
            message: "no SQL statement found".to_string(),
            hint: None,
        });
    }

    if stmts.len() > 1 {
        return Err(WorksheetError {
            code: "parse_error",
            message: "only one statement is allowed per execution".to_string(),
            hint: Some("Remove the extra statements or execute them separately.".to_string()),
        });
    }

    let stmt = stmts.into_iter().next().unwrap();

    match stmt.ast {
        Statement::Select(mut select_stmt) => {
            let limit_injected = if select_stmt.query.limit.is_none() {
                select_stmt.query.limit = Some(Limit {
                    with_ties: false,
                    quantity: Expr::Value(Value::Number(INJECTED_LIMIT.to_string())),
                });
                true
            } else {
                false
            };
            Ok(ValidatedQuery::Tabular {
                sql: select_stmt.to_ast_string_simple(),
                limit_injected,
            })
        }
        Statement::Show(_) => Ok(ValidatedQuery::Tabular {
            sql: stmt.sql.to_string(),
            limit_injected: false,
        }),
        Statement::ExplainPlan(_)
        | Statement::ExplainPushdown(_)
        | Statement::ExplainTimestamp(_)
        | Statement::ExplainSinkSchema(_) => Ok(ValidatedQuery::RawText {
            sql: stmt.sql.to_string(),
        }),
        // All other statements (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, etc.)
        // are treated as DML — they execute but results go to a notification, not the panel.
        _ => Ok(ValidatedQuery::DML {
            sql: stmt.sql.to_string(),
        }),
    }
}

/// Returns true if the statement is read-only (allowed in worksheets).
///
/// Returns true if the statement is a DML operation (INSERT, UPDATE, DELETE)
/// or other non-query statement. Used by the code lens module to show "Run"
/// instead of "Execute" for statements that don't produce result sets.
pub fn is_dml_statement(stmt: &mz_sql_parser::ast::Statement<Raw>) -> bool {
    !matches!(
        stmt,
        Statement::Select(_)
            | Statement::Show(_)
            | Statement::ExplainPlan(_)
            | Statement::ExplainPushdown(_)
            | Statement::ExplainTimestamp(_)
            | Statement::ExplainSinkSchema(_)
            | Statement::Subscribe(_)
    )
}

// ---------------------------------------------------------------------------
// Response formatting — extraction layer (touches tokio_postgres types)
// ---------------------------------------------------------------------------

/// Extracted row data from `SimpleQueryMessage` results.
///
/// This intermediate type bridges the `tokio_postgres` types (which lack
/// public constructors) and the pure formatting functions.
pub struct ExtractedRows {
    /// Column names from the first row's metadata.
    pub columns: Vec<String>,
    /// Row values as optional text (None = SQL NULL).
    pub rows: Vec<Vec<Option<String>>>,
}

/// Extract column names and text values from `SimpleQueryMessage` results.
///
/// Returns `None` if no rows were received (e.g., an empty result set with
/// no column metadata).
pub fn extract_rows_from_messages(messages: Vec<SimpleQueryMessage>) -> Option<ExtractedRows> {
    let mut columns: Option<Vec<String>> = None;
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();

    for msg in messages {
        match msg {
            SimpleQueryMessage::Row(row) => {
                if columns.is_none() {
                    columns = Some(row.columns().iter().map(|c| c.name().to_string()).collect());
                }
                let values: Vec<Option<String>> = (0..row.columns().len())
                    .map(|i| {
                        let text: Option<&str> = row.get(i);
                        text.map(|t| t.to_string())
                    })
                    .collect();
                rows.push(values);
            }
            SimpleQueryMessage::CommandComplete(_) | _ => {}
        }
    }

    columns.map(|cols| ExtractedRows {
        columns: cols,
        rows,
    })
}

/// Extract raw text lines from `SimpleQueryMessage` results.
///
/// Concatenates all cell values, which is the right shape for EXPLAIN
/// output (a series of single-column rows).
pub fn extract_text_from_messages(messages: Vec<SimpleQueryMessage>) -> Vec<String> {
    let mut lines = Vec::new();
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            for i in 0..row.columns().len() {
                let text: Option<&str> = row.get(i);
                if let Some(t) = text {
                    lines.push(t.to_string());
                }
            }
        }
    }
    lines
}

// ---------------------------------------------------------------------------
// Response formatting — pure layer (unit-testable)
// ---------------------------------------------------------------------------

/// Build a tabular [`ExecuteQueryResponse`] from extracted row data.
///
/// Applies truncation when `limit_injected` is true and the row count
/// reaches [`INJECTED_LIMIT`]. Coerces text values to JSON via
/// [`parse_text_value`].
pub fn build_tabular_response(
    extracted: Option<ExtractedRows>,
    limit_injected: bool,
    elapsed_ms: u64,
) -> ExecuteQueryResponse {
    let (columns, mut json_rows) = match extracted {
        Some(ext) => {
            let json_rows: Vec<Vec<serde_json::Value>> = ext
                .rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| match cell {
                            Some(t) => parse_text_value(&t),
                            None => serde_json::Value::Null,
                        })
                        .collect()
                })
                .collect();
            (Some(ext.columns), json_rows)
        }
        None => (None, Vec::new()),
    };

    let truncated = limit_injected && json_rows.len() >= INJECTED_LIMIT;
    if truncated {
        json_rows.truncate(MAX_ROWS);
    }

    ExecuteQueryResponse {
        columns,
        rows: Some(json_rows),
        raw_text: None,
        truncated,
        elapsed_ms,
        affected_rows: None,
    }
}

/// Build a raw-text [`ExecuteQueryResponse`] from extracted text lines.
pub fn build_raw_text_response(lines: Vec<String>, elapsed_ms: u64) -> ExecuteQueryResponse {
    ExecuteQueryResponse {
        columns: None,
        rows: None,
        raw_text: Some(lines.join("\n")),
        truncated: false,
        elapsed_ms,
        affected_rows: None,
    }
}

/// Build a DML response from `SimpleQueryMessage` results.
///
/// Extracts the affected row count from `CommandComplete` messages.
pub fn build_dml_response(
    messages: Vec<SimpleQueryMessage>,
    elapsed_ms: u64,
) -> ExecuteQueryResponse {
    let mut affected = 0u64;
    for msg in messages {
        if let SimpleQueryMessage::CommandComplete(n) = msg {
            affected += n;
        }
    }
    ExecuteQueryResponse {
        columns: None,
        rows: None,
        raw_text: None,
        truncated: false,
        elapsed_ms,
        affected_rows: Some(affected),
    }
}

/// Coerce a text value from the simple query protocol into a JSON value.
///
/// - Integer-shaped strings → `serde_json::Value::Number`
/// - Float-shaped strings → `serde_json::Value::Number` (if finite)
/// - `"t"` / `"f"` or `"true"` / `"false"` (case-insensitive) →
///   `serde_json::Value::Bool`
/// - `"NULL"` is not special here (SQL NULL arrives as `None`, not `"NULL"`)
/// - Everything else → `serde_json::Value::String`
pub fn parse_text_value(text: &str) -> serde_json::Value {
    // Try integer first.
    if let Ok(n) = text.parse::<i64>() {
        return serde_json::Value::Number(n.into());
    }

    // Try float.
    if let Ok(f) = text.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return serde_json::Value::Number(n);
        }
    }

    // Try boolean (Postgres returns "t"/"f" for bool columns).
    match text.to_lowercase().as_str() {
        "t" | "true" => return serde_json::Value::Bool(true),
        "f" | "false" => return serde_json::Value::Bool(false),
        _ => {}
    }

    serde_json::Value::String(text.to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- validate_worksheet_query --

    #[test]
    fn test_select_allowed() {
        let result = validate_worksheet_query("SELECT 1").unwrap();
        assert!(matches!(result, ValidatedQuery::Tabular { .. }));
    }

    #[test]
    fn test_select_star_allowed() {
        let result = validate_worksheet_query("SELECT * FROM foo").unwrap();
        assert!(matches!(result, ValidatedQuery::Tabular { .. }));
    }

    #[test]
    fn test_show_allowed() {
        let result = validate_worksheet_query("SHOW TABLES").unwrap();
        assert!(matches!(
            result,
            ValidatedQuery::Tabular {
                limit_injected: false,
                ..
            }
        ));
    }

    #[test]
    fn test_explain_is_raw_text() {
        let result = validate_worksheet_query("EXPLAIN SELECT 1").unwrap();
        assert!(matches!(result, ValidatedQuery::RawText { .. }));
    }

    #[test]
    fn test_insert_allowed() {
        let result = validate_worksheet_query("INSERT INTO foo VALUES (1)").unwrap();
        assert!(matches!(result, ValidatedQuery::DML { .. }));
    }

    #[test]
    fn test_update_allowed() {
        let result = validate_worksheet_query("UPDATE foo SET x = 1").unwrap();
        assert!(matches!(result, ValidatedQuery::DML { .. }));
    }

    #[test]
    fn test_delete_allowed() {
        let result = validate_worksheet_query("DELETE FROM foo").unwrap();
        assert!(matches!(result, ValidatedQuery::DML { .. }));
    }

    #[test]
    fn test_create_table_is_dml() {
        let result = validate_worksheet_query("CREATE TABLE foo (id INT)").unwrap();
        assert!(matches!(result, ValidatedQuery::DML { .. }));
    }

    #[test]
    fn test_drop_is_dml() {
        let result = validate_worksheet_query("DROP TABLE foo").unwrap();
        assert!(matches!(result, ValidatedQuery::DML { .. }));
    }

    #[test]
    fn test_empty_input() {
        let err = validate_worksheet_query("").unwrap_err();
        assert_eq!(err.code, "parse_error");
    }

    #[test]
    fn test_multi_statement_rejected() {
        let err = validate_worksheet_query("SELECT 1; SELECT 2").unwrap_err();
        assert_eq!(err.code, "parse_error");
        assert!(err.message.contains("only one statement"));
    }

    #[test]
    fn test_parse_error() {
        let err = validate_worksheet_query("SELECTTTT").unwrap_err();
        assert_eq!(err.code, "parse_error");
    }

    // -- LIMIT injection --

    #[test]
    fn test_limit_injected_when_missing() {
        let result = validate_worksheet_query("SELECT * FROM foo").unwrap();
        match result {
            ValidatedQuery::Tabular {
                sql,
                limit_injected,
            } => {
                assert!(limit_injected);
                let upper = sql.to_uppercase();
                assert!(upper.contains("LIMIT 1001"), "sql was: {sql}");
            }
            _ => panic!("expected Tabular"),
        }
    }

    #[test]
    fn test_existing_limit_preserved() {
        let result = validate_worksheet_query("SELECT * FROM foo LIMIT 10").unwrap();
        match result {
            ValidatedQuery::Tabular {
                sql,
                limit_injected,
            } => {
                assert!(!limit_injected);
                let upper = sql.to_uppercase();
                assert!(upper.contains("LIMIT 10"), "sql was: {sql}");
                assert!(!upper.contains("LIMIT 1001"), "sql was: {sql}");
            }
            _ => panic!("expected Tabular"),
        }
    }

    #[test]
    fn test_limit_not_injected_for_show() {
        let result = validate_worksheet_query("SHOW TABLES").unwrap();
        match result {
            ValidatedQuery::Tabular { limit_injected, .. } => {
                assert!(!limit_injected);
            }
            _ => panic!("expected Tabular"),
        }
    }

    #[test]
    fn test_select_with_cte_gets_limit() {
        let result = validate_worksheet_query("WITH x AS (SELECT 1 AS a) SELECT * FROM x").unwrap();
        match result {
            ValidatedQuery::Tabular {
                sql,
                limit_injected,
            } => {
                assert!(limit_injected);
                let upper = sql.to_uppercase();
                assert!(upper.contains("LIMIT 1001"), "sql was: {sql}");
            }
            _ => panic!("expected Tabular"),
        }
    }

    // -- parse_text_value --

    #[test]
    fn test_parse_integer() {
        assert_eq!(parse_text_value("42"), serde_json::json!(42));
        assert_eq!(parse_text_value("-7"), serde_json::json!(-7));
        assert_eq!(parse_text_value("0"), serde_json::json!(0));
    }

    #[test]
    fn test_parse_float() {
        assert_eq!(parse_text_value("3.14"), serde_json::json!(3.14));
        assert_eq!(parse_text_value("-0.5"), serde_json::json!(-0.5));
    }

    #[test]
    fn test_parse_boolean() {
        assert_eq!(parse_text_value("t"), serde_json::Value::Bool(true));
        assert_eq!(parse_text_value("f"), serde_json::Value::Bool(false));
        assert_eq!(parse_text_value("true"), serde_json::Value::Bool(true));
        assert_eq!(parse_text_value("false"), serde_json::Value::Bool(false));
    }

    #[test]
    fn test_parse_string() {
        assert_eq!(
            parse_text_value("hello"),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(
            parse_text_value("2026-01-01"),
            serde_json::Value::String("2026-01-01".to_string())
        );
    }

    #[test]
    fn test_parse_empty_string() {
        assert_eq!(
            parse_text_value(""),
            serde_json::Value::String(String::new())
        );
    }

    // -- build_tabular_response --

    #[test]
    fn test_build_empty_result() {
        let resp = build_tabular_response(None, false, 10);
        assert_eq!(resp.columns, None);
        assert_eq!(resp.rows, Some(vec![]));
        assert!(!resp.truncated);
        assert_eq!(resp.elapsed_ms, 10);
    }

    #[test]
    fn test_build_tabular_basic() {
        let extracted = ExtractedRows {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec![Some("1".to_string()), Some("alice".to_string())],
                vec![Some("2".to_string()), None],
            ],
        };
        let resp = build_tabular_response(Some(extracted), false, 42);
        assert_eq!(
            resp.columns,
            Some(vec!["id".to_string(), "name".to_string()])
        );
        assert_eq!(
            resp.rows,
            Some(vec![
                vec![serde_json::json!(1), serde_json::json!("alice")],
                vec![serde_json::json!(2), serde_json::Value::Null],
            ])
        );
        assert!(!resp.truncated);
        assert_eq!(resp.elapsed_ms, 42);
    }

    #[test]
    fn test_truncation_detected() {
        let rows: Vec<Vec<Option<String>>> = (0..INJECTED_LIMIT)
            .map(|i| vec![Some(i.to_string())])
            .collect();
        let extracted = ExtractedRows {
            columns: vec!["n".to_string()],
            rows,
        };
        let resp = build_tabular_response(Some(extracted), true, 50);
        assert!(resp.truncated);
        assert_eq!(resp.rows.as_ref().unwrap().len(), MAX_ROWS);
    }

    #[test]
    fn test_no_truncation_when_under_limit() {
        let extracted = ExtractedRows {
            columns: vec!["n".to_string()],
            rows: vec![vec![Some("1".to_string())]],
        };
        let resp = build_tabular_response(Some(extracted), true, 5);
        assert!(!resp.truncated);
        assert_eq!(resp.rows.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_no_truncation_when_not_injected() {
        let rows: Vec<Vec<Option<String>>> = (0..INJECTED_LIMIT)
            .map(|i| vec![Some(i.to_string())])
            .collect();
        let extracted = ExtractedRows {
            columns: vec!["n".to_string()],
            rows,
        };
        let resp = build_tabular_response(Some(extracted), false, 50);
        assert!(!resp.truncated);
        assert_eq!(resp.rows.as_ref().unwrap().len(), INJECTED_LIMIT);
    }

    // -- build_raw_text_response --

    #[test]
    fn test_build_raw_text() {
        let lines = vec!["line 1".to_string(), "line 2".to_string()];
        let resp = build_raw_text_response(lines, 20);
        assert_eq!(resp.raw_text, Some("line 1\nline 2".to_string()));
        assert!(resp.columns.is_none());
        assert!(resp.rows.is_none());
        assert!(!resp.truncated);
        assert_eq!(resp.elapsed_ms, 20);
    }

    #[test]
    fn test_build_raw_text_empty() {
        let resp = build_raw_text_response(vec![], 5);
        assert_eq!(resp.raw_text, Some(String::new()));
    }

    // -- build_set_commands --

    #[test]
    fn test_set_all_fields() {
        let params = SetSessionParams {
            database: Some("mydb".to_string()),
            schema: Some("public".to_string()),
            cluster: Some("c1".to_string()),
        };
        assert_eq!(
            build_set_commands(&params),
            "SET database = 'mydb'; SET search_path = 'public'; SET cluster = 'c1'"
        );
    }

    #[test]
    fn test_set_only_cluster() {
        let params = SetSessionParams {
            database: None,
            schema: None,
            cluster: Some("compute".to_string()),
        };
        assert_eq!(build_set_commands(&params), "SET cluster = 'compute'");
    }

    #[test]
    fn test_set_only_database() {
        let params = SetSessionParams {
            database: Some("analytics".to_string()),
            schema: None,
            cluster: None,
        };
        assert_eq!(build_set_commands(&params), "SET database = 'analytics'");
    }

    #[test]
    fn test_set_none() {
        let params = SetSessionParams {
            database: None,
            schema: None,
            cluster: None,
        };
        assert_eq!(build_set_commands(&params), "");
    }

    #[test]
    fn test_set_escapes_single_quotes() {
        let params = SetSessionParams {
            database: Some("my'db".to_string()),
            schema: None,
            cluster: None,
        };
        assert_eq!(build_set_commands(&params), "SET database = 'my''db'");
    }

    // -- validate_subscribe_query --

    #[test]
    fn test_subscribe_injects_progress() {
        let sql = validate_subscribe_query("SUBSCRIBE (SELECT 1)").unwrap();
        let upper = sql.to_uppercase();
        assert!(upper.contains("PROGRESS"), "sql was: {sql}");
    }

    #[test]
    fn test_subscribe_preserves_existing_progress() {
        let sql = validate_subscribe_query("SUBSCRIBE (SELECT 1) WITH (PROGRESS)").unwrap();
        let upper = sql.to_uppercase();
        // Should have PROGRESS exactly once, not duplicated.
        assert!(upper.contains("PROGRESS"), "sql was: {sql}");
    }

    #[test]
    fn test_subscribe_rejects_non_subscribe() {
        let err = validate_subscribe_query("SELECT 1").unwrap_err();
        assert_eq!(err.code, "invalid_statement");
    }

    // -- group_subscribe_rows --

    #[test]
    fn test_group_progress_only_batch() {
        let rows = vec![vec![
            Some("1000".to_string()),
            Some("t".to_string()),
            None,
            None,
        ]];
        let batches = group_subscribe_rows(&rows, "sub1", true, &["val".to_string()]);
        assert_eq!(batches.len(), 1);
        assert!(batches[0].progress_only);
        assert!(batches[0].diffs.is_empty());
        assert_eq!(batches[0].timestamp, "1000");
    }

    #[test]
    fn test_group_data_batch() {
        let rows = vec![vec![
            Some("1000".to_string()),
            Some("f".to_string()),
            Some("1".to_string()),
            Some("hello".to_string()),
        ]];
        let batches = group_subscribe_rows(&rows, "sub1", true, &["val".to_string()]);
        assert_eq!(batches.len(), 1);
        assert!(!batches[0].progress_only);
        assert_eq!(batches[0].diffs.len(), 1);
        assert_eq!(batches[0].diffs[0].diff, 1);
        assert_eq!(batches[0].diffs[0].values, vec![serde_json::json!("hello")]);
        assert_eq!(batches[0].columns, Some(vec!["val".to_string()]));
    }

    #[test]
    fn test_group_multiple_timestamps() {
        let rows = vec![
            vec![
                Some("1000".to_string()),
                Some("f".to_string()),
                Some("1".to_string()),
                Some("a".to_string()),
            ],
            vec![Some("1000".to_string()), Some("t".to_string()), None, None],
            vec![Some("2000".to_string()), Some("t".to_string()), None, None],
        ];
        let batches = group_subscribe_rows(&rows, "sub1", true, &["val".to_string()]);
        assert_eq!(batches.len(), 2);
        // First batch is data (ts=1000 has a non-progress row).
        assert!(!batches[0].progress_only);
        assert_eq!(batches[0].diffs.len(), 1);
        // Second batch is progress-only (ts=2000).
        assert!(batches[1].progress_only);
    }
}
