// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Many things here only compile in the wasm32 target. We don't need to annotate it that way because
// this should only ever be built by wasm-build which specifies that.

#[cfg(target_arch = "wasm32")]
use lol_alloc::{FreeListAllocator, LockedAllocator};
use mz_sql_parser::ast::display::AstDisplay;
#[cfg(target_arch = "wasm32")]
use mz_sql_parser::ast::{statement_kind_label_value, StatementKind};
use mz_sql_parser::ast::{Raw, ShowStatement, Statement, SubscribeOption, SubscribeOptionName};
use mz_sql_parser::parser;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[global_allocator]
static ALLOCATOR: LockedAllocator<FreeListAllocator> =
    LockedAllocator::new(FreeListAllocator::new());

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(typescript_custom_section)]
const TS_TYPES: &str = r#"
export interface ParsedStatement {
    kind: string;
    sql: string;
    /** Byte offset into the original UTF-8 SQL input. */
    offset: number;
    in_cluster: string | undefined;
}

export interface ParseError {
    message: string;
    /** Byte offset into the original UTF-8 SQL input where parsing failed. */
    offset: number;
}

export interface ParseResult {
    /** Successfully parsed statements, including any recovered before a parse error. */
    statements: ParsedStatement[];
    error: ParseError | null;
}

/** Parses zero or more SQL statements. */
export function parse(sql: string): ParseResult;

/**
 * Adds `WITH (PROGRESS)` to a single valid SUBSCRIBE statement.
 *
 * Throws if `sql` is not exactly one valid SUBSCRIBE statement.
 * Returns normalized SQL and may not preserve original formatting,
 * comments, or a trailing semicolon.
 */
export function inject_progress(sql: string): string;
"#;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = ParsedStatement, getter_with_clone, inspectable)]
pub struct JsParsedStatement {
    pub kind: String,
    pub sql: String,
    pub offset: usize,
    pub in_cluster: Option<String>,
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = ParseError, getter_with_clone, inspectable)]
pub struct JsParseError {
    pub message: String,
    pub offset: usize,
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = ParseResult, getter_with_clone, inspectable)]
pub struct JsParseResult {
    pub statements: Vec<JsValue>,
    pub error: JsValue,
}

/// Refines the kind label for `Show` statements. The top-level `StatementKind`
/// collapses all `SHOW` variants into a single `"show"` label; this function
/// inspects the inner `ShowStatement` to distinguish sub-types.
fn refine_show_kind(stmt: &ShowStatement<Raw>) -> &'static str {
    match stmt {
        ShowStatement::ShowCreateView(_) => "show_create_view",
        ShowStatement::ShowCreateMaterializedView(_) => "show_create_materialized_view",
        ShowStatement::ShowCreateSource(_) => "show_create_source",
        ShowStatement::ShowCreateTable(_) => "show_create_table",
        ShowStatement::ShowCreateSink(_) => "show_create_sink",
        ShowStatement::ShowCreateIndex(_) => "show_create_index",
        ShowStatement::ShowCreateConnection(_) => "show_create_connection",
        ShowStatement::ShowCreateCluster(_) => "show_create_cluster",
        ShowStatement::ShowCreateType(_) => "show_create_type",
        ShowStatement::ShowObjects(_) => "show_objects",
        ShowStatement::ShowColumns(_) => "show_columns",
        ShowStatement::ShowVariable(_) => "show_variable",
        ShowStatement::InspectShard(_) => "inspect_shard",
    }
}

#[cfg(target_arch = "wasm32")]
fn convert_stmts(base: &str, stmts: Vec<parser::StatementParseResult<'_>>) -> Vec<JsValue> {
    stmts
        .into_iter()
        .map(|s| {
            let kind: StatementKind = (&s.ast).into();
            let kind_str = match &s.ast {
                Statement::Show(show) => refine_show_kind(show),
                _ => statement_kind_label_value(kind),
            };
            let offset = s.sql.as_ptr().addr() - base.as_ptr().addr();
            JsValue::from(JsParsedStatement {
                kind: kind_str.to_string(),
                sql: s.sql.to_string(),
                offset,
                in_cluster: in_cluster(&s.ast),
            })
        })
        .collect()
}

/// Returns the cluster this statement targets if set.
fn in_cluster(stmt: &Statement<mz_sql_parser::ast::Raw>) -> Option<String> {
    let cluster = match stmt {
        Statement::CreateMaterializedView(mv) => mv.in_cluster.clone(),
        Statement::CreateIndex(index) => index.in_cluster.clone(),
        Statement::CreateSource(src) => src.in_cluster.clone(),
        Statement::CreateWebhookSource(src) => src.in_cluster.clone(),
        Statement::CreateSink(src) => src.in_cluster.clone(),
        _ => None,
    };

    cluster.map(|cluster| cluster.to_string())
}

/// Try to recover successfully-parsed statements from the text before the
/// error position. Finds the last semicolon before `error_pos` and re-parses
/// the prefix up to (and including) that semicolon.
#[cfg(target_arch = "wasm32")]
fn recover_statements_before(sql: &str, error_pos: usize) -> Vec<JsValue> {
    let prefix = sql.get(..error_pos).unwrap_or_else(|| sql);
    let last_semi = match prefix.rfind(';') {
        Some(i) => i + 1,
        None => return Vec::new(),
    };
    let clean = &sql[..last_semi];
    match parser::parse_statements(clean) {
        Ok(stmts) => convert_stmts(sql, stmts),
        Err(_) => Vec::new(),
    }
}

/// Parses a SQL string into statements with their kinds and byte offsets.
///
/// Returns a `ParseResult` containing an array of parsed statements (each with
/// a `kind` string, the `sql` text, and a byte `offset` into the input), and
/// an `error` field that is non-null when parsing fails.
///
/// When parsing fails partway through, any statements that were successfully
/// parsed before the error are still returned alongside the error. Offsets are
/// byte offsets into the original UTF-8 input string, not JavaScript character
/// or UTF-16 code unit offsets.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(skip_typescript)]
pub fn parse(sql: &str) -> JsParseResult {
    match parser::parse_statements(sql) {
        Ok(stmts) => JsParseResult {
            statements: convert_stmts(sql, stmts),
            error: JsValue::NULL,
        },
        Err(e) => JsParseResult {
            statements: recover_statements_before(sql, e.error.pos),
            error: JsValue::from(JsParseError {
                message: e.error.message,
                offset: e.error.pos,
            }),
        },
    }
}

/// Parses a single SUBSCRIBE statement and ensures it includes `WITH (PROGRESS)`.
///
/// Returns normalized SQL for the modified statement. The returned string may
/// not preserve the input formatting, comments, or a trailing semicolon.
///
/// Returns an error if the input is not exactly one valid SUBSCRIBE statement.
/// In JavaScript, this error is surfaced as a thrown exception.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(skip_typescript)]
pub fn inject_progress(sql: &str) -> Result<String, JsError> {
    inject_progress_impl(sql).map_err(|msg| JsError::new(&msg))
}

fn inject_progress_impl(sql: &str) -> Result<String, String> {
    let stmts = parser::parse_statements(sql).map_err(|e| e.error.message)?;

    if stmts.len() != 1 {
        return Err(format!(
            "expected exactly one statement, found {}",
            stmts.len()
        ));
    }

    let stmt = stmts.into_iter().next().unwrap();
    match stmt.ast {
        Statement::Subscribe(mut sub) => {
            let already = sub
                .options
                .iter()
                .any(|o| o.name == SubscribeOptionName::Progress);
            if !already {
                sub.options.push(SubscribeOption {
                    name: SubscribeOptionName::Progress,
                    value: None,
                });
            }
            Ok(Statement::Subscribe(sub).to_ast_string_simple())
        }
        _ => Err("statement is not SUBSCRIBE".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use mz_sql_parser::ast::{Statement, SubscribeOptionName};
    use mz_sql_parser::parser;

    use super::{in_cluster, inject_progress_impl};

    #[mz_ore::test]
    fn test_partial_parse_recovery() {
        let sql = "SELECT 1; SELEC";
        assert!(parser::parse_statements(sql).is_err());
        let err = parser::parse_statements(sql).unwrap_err();
        let prefix = &sql[..sql[..err.error.pos].rfind(';').unwrap() + 1];
        let stmts = parser::parse_statements(prefix).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0].sql.trim(), "SELECT 1");
    }

    #[mz_ore::test]
    fn test_partial_parse_no_valid_prefix() {
        let sql = "SELEC";
        let err = parser::parse_statements(sql).unwrap_err();
        assert!(sql[..err.error.pos].rfind(';').is_none());
    }

    #[mz_ore::test]
    fn test_partial_parse_multiple_valid_then_error() {
        let sql = "SELECT 1; SELECT 2; BAD";
        let err = parser::parse_statements(sql).unwrap_err();
        let prefix = &sql[..sql[..err.error.pos].rfind(';').unwrap() + 1];
        let stmts = parser::parse_statements(prefix).unwrap();
        assert_eq!(stmts.len(), 2);
    }

    #[mz_ore::test]
    fn test_inject_progress_on_subscribe() {
        let result = inject_progress_impl("SUBSCRIBE (SELECT 1)").unwrap();
        assert!(
            result.contains("WITH (PROGRESS)"),
            "expected WITH (PROGRESS), got: {result}"
        );
    }

    #[mz_ore::test]
    fn test_inject_progress_idempotent() {
        let sql = "SUBSCRIBE (SELECT 1) WITH (PROGRESS)";
        let stmts = parser::parse_statements(sql).unwrap();
        match &stmts[0].ast {
            Statement::Subscribe(sub) => {
                assert!(sub
                    .options
                    .iter()
                    .any(|o| o.name == SubscribeOptionName::Progress));
            }
            _ => panic!("expected subscribe"),
        }
        // inject_progress should not add a duplicate
        let result = inject_progress_impl(sql).unwrap();
        let count = result.matches("PROGRESS").count();
        assert_eq!(count, 1, "should have exactly one PROGRESS, got: {result}");
    }

    #[mz_ore::test]
    fn test_inject_progress_rejects_non_subscribe() {
        let result = inject_progress_impl("SELECT 1");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "statement is not SUBSCRIBE");
    }

    #[mz_ore::test]
    fn test_inject_progress_rejects_multiple_statements() {
        let result = inject_progress_impl("SELECT 1; SUBSCRIBE (SELECT 1)");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "expected exactly one statement, found 2"
        );
    }

    #[mz_ore::test]
    fn test_offset_calculation_after_recovery() {
        let sql = "SELECT 1;\n  SELECT 2;\n  BAD";
        let err = parser::parse_statements(sql).unwrap_err();
        let prefix = &sql[..sql[..err.error.pos].rfind(';').unwrap() + 1];
        let stmts = parser::parse_statements(prefix).unwrap();
        for s in &stmts {
            let offset = s.sql.as_ptr().addr() - sql.as_ptr().addr();
            assert!(offset < sql.len(), "offset {offset} out of range");
            assert_eq!(&sql[offset..offset + s.sql.len()], s.sql);
        }
    }

    #[mz_ore::test]
    fn test_in_cluster_create_materialized_view() {
        let sql = "CREATE MATERIALIZED VIEW mv IN CLUSTER foo AS SELECT 1";
        let stmts = parser::parse_statements(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(in_cluster(&stmts[0].ast), Some("foo".to_string()));
    }

    #[mz_ore::test]
    fn test_in_cluster_select_returns_none() {
        let sql = "SELECT 1";
        let stmts = parser::parse_statements(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(in_cluster(&stmts[0].ast), None);
    }
}
