// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! LSP-specific diagnostic emission.
//!
//! Each producer in this module first builds a
//! `PositionalDiagnostic` using the shared locator
//! helpers, then converts it to a [`tower_lsp::lsp_types::Diagnostic`] via
//! `to_lsp` using a [`Rope`] for byte-offset → line/column conversion.
//!
//! Three tiers of diagnostics:
//!
//! - **Per-keystroke diagnostics** ([`diagnose()`]) — Resolves psql-style
//!   variables before parsing. Unresolved variables produce positioned
//!   diagnostics (ERROR or WARNING depending on the warn pragma). The resolved
//!   SQL is then parsed with [`mz_sql_parser::parser::parse_statements()`] and
//!   any parse error positions are mapped back to original-text offsets via
//!   `resolved_to_original`.
//!
//! - **On-save validation errors** (`validation_diagnostics()`) — Converts
//!   project-level `ValidationError`s into LSP diagnostics grouped by file.
//!   When an error carries a byte offset (most statement-level errors), the
//!   diagnostic is positioned at the correct line/column. File-level errors
//!   (e.g., missing CREATE statement) fall back to `(0, 0)`.
//!
//! - **On-save typecheck errors** (`typecheck_diagnostics()`) — Inspects
//!   the structured upstream error to position the diagnostic. See
//!   `locate_typecheck` for the dispatch.

use crate::diagnostics::{PositionalDiagnostic, Severity, Suggestion, locate_typecheck};
use crate::fs::FileSystem;
use crate::project::compiler::typecheck::{ObjectTypeCheckError, TypeCheckError};
use crate::project::error::ValidationError;
use crate::project::syntax::variables::{resolve_variables, resolved_to_original};
use ropey::Rope;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tower_lsp::lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};

/// Parse `text` as SQL and return diagnostics for any parse errors and variable issues.
///
/// Resolves psql-style variables before parsing. Unresolved variables produce
/// diagnostics at their position in the original text. Parse errors from the
/// resolved SQL are mapped back to original-text positions via the substitution log.
///
/// # Arguments
/// * `text` — The original SQL source text (as the editor shows it).
/// * `rope` — A [`Rope`] built from the same `text`, used for byte-offset to
///   line/column conversion.
/// * `variables` — Variable definitions from the project config.
/// * `profile_name` — Active profile name (if any), shown in undefined-variable
///   messages. When `None`, the diagnostic suggests setting a profile.
///
/// # Returns
/// A (possibly empty) vec of LSP diagnostics. Variable diagnostics are
/// `WARNING` if the file has the warn pragma, `ERROR` otherwise. Parse errors
/// are always `ERROR`.
pub fn diagnose(
    text: &str,
    rope: &Rope,
    variables: &BTreeMap<String, String>,
    profile_name: Option<&str>,
) -> Vec<Diagnostic> {
    if text.trim().is_empty() {
        return Vec::new();
    }

    parse_positional(text, variables, profile_name)
        .iter()
        .map(|pd| to_lsp(pd, rope))
        .collect()
}

/// Build [`PositionalDiagnostic`]s for parse errors and unresolved variables
/// in `text`. All positions are in original-text byte space (post-substitution
/// parser offsets are mapped back via `resolved_to_original`).
fn parse_positional(
    text: &str,
    variables: &BTreeMap<String, String>,
    profile_name: Option<&str>,
) -> Vec<PositionalDiagnostic> {
    let resolved = resolve_variables(text, variables);
    let mut pds = Vec::new();

    let var_severity = if resolved.has_warn_pragma {
        Severity::Warning
    } else {
        Severity::Error
    };
    for uv in &resolved.unresolved {
        let message = match profile_name {
            Some(name) => format!(
                "undefined variable ':{}'  — define in [{}.variables] in project.toml",
                uv.name, name
            ),
            None => format!(
                "undefined variable ':{}'  — no profile is selected; run `mz-deploy profile set <name>` and define in [<profile>.variables] in project.toml",
                uv.name
            ),
        };
        pds.push(PositionalDiagnostic {
            severity: var_severity,
            file: PathBuf::new(),
            source: text.to_string(),
            byte_range: uv.byte_offset..(uv.byte_offset + uv.byte_len),
            message,
            footers: Vec::new(),
            suggestions: Vec::new(),
        });
    }

    if let Err(e) = mz_sql_parser::parser::parse_statements(&resolved.sql) {
        let original_offset = resolved_to_original(e.error.pos, &resolved.substitutions);
        pds.push(PositionalDiagnostic {
            severity: Severity::Error,
            file: PathBuf::new(),
            source: text.to_string(),
            byte_range: original_offset..original_offset,
            message: e.error.message.clone(),
            footers: Vec::new(),
            suggestions: Vec::new(),
        });
    }

    pds
}

/// Convert `ValidationError`s into LSP diagnostics grouped by file path.
///
/// When an error carries a `byte_offset`, the file is read and a [`Rope`] is
/// built so the offset can be converted to a precise line/column position.
/// Errors without an offset (file-level) fall back to `(0, 0)`.
///
/// Returns an empty map when `errors` is empty.
pub(crate) fn validation_diagnostics(
    fs: &FileSystem,
    errors: &[ValidationError],
) -> BTreeMap<PathBuf, Vec<Diagnostic>> {
    let mut map: BTreeMap<PathBuf, Vec<Diagnostic>> = BTreeMap::new();
    let mut source_cache: BTreeMap<PathBuf, Option<(String, Rope)>> = BTreeMap::new();
    let zero = Position::new(0, 0);

    for error in errors {
        let entry = source_cache
            .entry(error.context.file.clone())
            .or_insert_with(|| read_source(fs, &error.context.file));

        let diag = match (entry.as_ref(), error.context.byte_offset) {
            (Some((source, rope)), Some(offset)) => {
                let primary_range =
                    crate::diagnostics::locate_validation(&error.kind, source, Some(offset))
                        .unwrap_or(offset..offset);
                let (body, footers, suggestions) =
                    crate::diagnostics::format_validation_kind(&error.kind, source, &primary_range);
                let mut message = body;
                append_detail_and_hints(&mut message, None, &footers);
                let mut diag = build_error_diagnostic(primary_range, message, rope);
                attach_quickfix_data(&mut diag, &suggestions, rope);
                diag
            }
            _ => Diagnostic {
                range: Range::new(zero, zero),
                severity: Some(DiagnosticSeverity::ERROR),
                source: Some("mz-deploy".to_string()),
                message: error.kind.message(),
                ..Default::default()
            },
        };

        map.entry(error.context.file.clone())
            .or_default()
            .push(diag);
    }

    map
}

/// Convert a [`TypeCheckError`] into LSP diagnostics grouped by file path.
///
/// Per-object errors (`TypeCheckFailed`, `Multiple`) are positioned by
/// inspecting the underlying error via [`locate_typecheck`]. The on-disk
/// source file is read once per file and cached. If the read fails, all
/// diagnostics for that file fall back to `(0, 0)`.
///
/// Non-object variants (`DatabaseSetupError`, `SortError`,
/// `TypesCacheWriteFailed`) have no per-file context and return an empty map;
/// callers should log them to the client message stream instead.
pub(crate) fn typecheck_diagnostics(
    fs: &FileSystem,
    error: &TypeCheckError,
    candidates: &crate::lsp::code_action::Candidates,
) -> BTreeMap<PathBuf, Vec<Diagnostic>> {
    let errors: &[ObjectTypeCheckError] = match error {
        TypeCheckError::Multiple(errs) => errs.as_slice(),
        TypeCheckError::DatabaseSetupError(_)
        | TypeCheckError::SortError(_)
        | TypeCheckError::TypesCacheWriteFailed(_) => &[],
    };

    let mut map: BTreeMap<PathBuf, Vec<Diagnostic>> = BTreeMap::new();
    let mut source_cache: BTreeMap<PathBuf, Option<(String, Rope)>> = BTreeMap::new();
    let zero = Position::new(0, 0);

    for e in errors {
        let entry = source_cache
            .entry(e.file_path.clone())
            .or_insert_with(|| read_source(fs, &e.file_path));

        let diag = match entry.as_ref() {
            Some((source, rope)) => {
                let byte_range = locate_typecheck(&e.kind, source).unwrap_or(0..0);
                let (body, footers, format_suggestions) =
                    crate::diagnostics::format_typecheck_kind(&e.kind, source, &byte_range);
                let suggestions = if format_suggestions.is_empty() {
                    crate::lsp::code_action::fuzzy_suggestions(
                        &e.kind,
                        source,
                        &byte_range,
                        candidates,
                    )
                } else {
                    format_suggestions
                };

                let mut message = body;
                append_detail_and_hints(&mut message, e.detail().as_deref(), &footers);
                let mut diag = build_error_diagnostic(byte_range, message, rope);
                attach_quickfix_data(&mut diag, &suggestions, rope);
                diag
            }
            None => {
                // No source available — fall back to the upstream Display.
                let mut message = e.error_message();
                let footers: Vec<String> = e.hint().into_iter().collect();
                append_detail_and_hints(&mut message, e.detail().as_deref(), &footers);
                Diagnostic {
                    range: Range::new(zero, zero),
                    severity: Some(DiagnosticSeverity::ERROR),
                    source: Some("mz-deploy".to_string()),
                    message,
                    ..Default::default()
                }
            }
        };

        map.entry(e.file_path.clone()).or_default().push(diag);
    }

    map
}

fn read_source(fs: &FileSystem, path: &Path) -> Option<(String, Rope)> {
    let text = fs.read_to_string(path).ok()?;
    let rope = Rope::from_str(&text);
    Some((text, rope))
}

/// Build an error-severity LSP `Diagnostic` for `byte_range` in `rope`.
///
/// Both LSP diagnostic flows always emit `ERROR`; warnings come from the
/// per-keystroke parse path via `to_lsp` below.
fn build_error_diagnostic(
    byte_range: std::ops::Range<usize>,
    message: String,
    rope: &Rope,
) -> Diagnostic {
    let zero = Position::new(0, 0);
    let start = offset_to_position(byte_range.start, rope).unwrap_or(zero);
    let end = offset_to_position(byte_range.end, rope).unwrap_or(start);
    Diagnostic {
        range: Range::new(start, end),
        severity: Some(DiagnosticSeverity::ERROR),
        source: Some("mz-deploy".to_string()),
        message,
        ..Default::default()
    }
}

/// Append `\ndetail: <detail>` (when present) and one `\nhint: <footer>`
/// line per footer to `message`. Preserves the human-readable advice for
/// editors that don't render code actions.
fn append_detail_and_hints(message: &mut String, detail: Option<&str>, footers: &[String]) {
    if let Some(detail) = detail {
        message.push_str("\ndetail: ");
        message.push_str(detail);
    }
    for footer in footers {
        message.push_str("\nhint: ");
        message.push_str(footer);
    }
}

/// Encode `suggestions` as the `QuickFixData` JSON payload on `diag.data`
/// for the LSP code-action handler. No-op when `suggestions` is empty.
fn attach_quickfix_data(diag: &mut Diagnostic, suggestions: &[Suggestion], rope: &Rope) {
    if let Some(qf) = crate::lsp::code_action::suggestions_to_data(suggestions, rope) {
        diag.data = Some(serde_json::to_value(qf).expect("serializable"));
    }
}

/// Convert a [`PositionalDiagnostic`] to an LSP [`Diagnostic`].
///
/// Used by the per-keystroke parse path which builds `PositionalDiagnostic`s
/// with both error and warning severity. The validation / typecheck flows
/// build `Diagnostic`s directly via [`build_error_diagnostic`].
fn to_lsp(pd: &PositionalDiagnostic, rope: &Rope) -> Diagnostic {
    let zero = Position::new(0, 0);
    let start = offset_to_position(pd.byte_range.start, rope).unwrap_or(zero);
    let end = offset_to_position(pd.byte_range.end, rope).unwrap_or(start);
    let severity = match pd.severity {
        Severity::Error => DiagnosticSeverity::ERROR,
        Severity::Warning => DiagnosticSeverity::WARNING,
    };
    Diagnostic {
        range: Range::new(start, end),
        severity: Some(severity),
        source: Some("mz-deploy".to_string()),
        message: pd.message.clone(),
        ..Default::default()
    }
}

/// Convert a byte offset to an LSP [`Position`] (line, column) using a [`Rope`].
pub(crate) fn offset_to_position(offset: usize, rope: &Rope) -> Option<Position> {
    let char_offset = rope.try_byte_to_char(offset).ok()?;
    let line = rope.try_char_to_line(char_offset).ok()?;
    let first_char_of_line = rope.try_line_to_char(line).ok()?;
    let line_start_byte = rope.try_char_to_byte(first_char_of_line).ok()?;
    let column = utf16_len(rope.byte_slice(line_start_byte..offset).as_str()?);

    let line_u32 = line.try_into().ok()?;
    let column_u32 = column.try_into().ok()?;

    Some(Position::new(line_u32, column_u32))
}

/// Convert an LSP [`Position`] into a byte offset using a [`Rope`].
pub(crate) fn position_to_offset(position: Position, rope: &Rope) -> Option<usize> {
    let line = usize::try_from(position.line).ok()?;
    let target_col = usize::try_from(position.character).ok()?;
    let line_start_char = rope.try_line_to_char(line).ok()?;
    let line_text = rope.line(line);

    let mut utf16_col = 0usize;
    let mut char_delta = 0usize;
    for ch in line_text.chars() {
        if utf16_col >= target_col {
            break;
        }
        let next = utf16_col + ch.len_utf16();
        if next > target_col {
            break;
        }
        utf16_col = next;
        char_delta += 1;
    }

    let char_offset = line_start_char + char_delta;
    rope.try_char_to_byte(char_offset).ok()
}

fn utf16_len(text: &str) -> usize {
    text.chars().map(char::len_utf16).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn valid_sql_produces_no_diagnostics() {
        let text = "CREATE VIEW foo AS SELECT 1;";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), None).is_empty());
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn syntax_error_produces_diagnostic_at_correct_position() {
        let text = "CREATE VIEW foo AS SELECTT 1;";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        // Error should be on line 0 (first line)
        assert_eq!(diags[0].range.start.line, 0);
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn multiline_error_position() {
        let text = "CREATE VIEW foo AS\nSELECT 1;\nCREATE VIEW bar AS SELECTT 2;";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        assert_eq!(diags.len(), 1);
        // Error should be on line 2 (third line, zero-indexed)
        assert_eq!(diags[0].range.start.line, 2);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn empty_file_produces_no_diagnostics() {
        let text = "";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), None).is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn offset_to_position_uses_utf16_columns() {
        let text = "SELECT 😀FROM";
        let rope = Rope::from_str(text);
        // `FROM` starts after 7 ASCII code units plus 2 UTF-16 code units for 😀.
        assert_eq!(offset_to_position(11, &rope), Some(Position::new(0, 9)));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn position_to_offset_uses_utf16_columns() {
        let text = "SELECT 😀foo";
        let rope = Rope::from_str(text);
        // `foo` begins after 7 ASCII code units plus 2 UTF-16 code units for 😀.
        assert_eq!(position_to_offset(Position::new(0, 9), &rope), Some(11));
        assert_eq!(
            position_to_offset(Position::new(0, 12), &rope),
            Some(text.len())
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn whitespace_only_file_produces_no_diagnostics() {
        let text = "   \n  \n  ";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), None).is_empty());
    }

    // --- Variable-aware diagnose tests ---

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn resolved_variable_no_diagnostics() {
        let text = "CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        assert!(diags.is_empty());
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn resolved_variable_produces_clean_parse() {
        let v = vars(&[("cluster", "quickstart")]);
        let text = "CREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &v, None);
        assert!(diags.is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn unresolved_variable_produces_error() {
        let text = "CREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        // Should have at least the variable error
        let var_diags: Vec<_> = diags
            .iter()
            .filter(|d| d.message.contains("undefined variable"))
            .collect();
        assert_eq!(var_diags.len(), 1);
        assert_eq!(var_diags[0].severity, Some(DiagnosticSeverity::ERROR));
        // `:cluster` starts at byte 39
        assert!(var_diags[0].message.contains(":cluster"));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn unresolved_variable_with_pragma_produces_warning() {
        let text = "-- PRAGMA WARN_ON_MISSING_VARIABLES;\nCREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), None);
        let var_diags: Vec<_> = diags
            .iter()
            .filter(|d| d.message.contains("undefined variable"))
            .collect();
        assert_eq!(var_diags.len(), 1);
        assert_eq!(var_diags[0].severity, Some(DiagnosticSeverity::WARNING));
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn parse_error_maps_back_to_original_position() {
        // After resolving :x → "ab", the parse error in resolved text
        // should map back to the original text position.
        let v = vars(&[("x", "ab")]);
        // "CREATE VIEW :x AS SELECTT 1" → "CREATE VIEW ab AS SELECTT 1"
        let text = "CREATE VIEW :x AS SELECTT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &v, None);
        // Should have exactly one parse error diagnostic.
        let parse_diags: Vec<_> = diags
            .iter()
            .filter(|d| !d.message.contains("undefined variable"))
            .collect();
        assert_eq!(parse_diags.len(), 1);
        assert_eq!(parse_diags[0].severity, Some(DiagnosticSeverity::ERROR));
        assert_eq!(parse_diags[0].range.start.line, 0);
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn no_variables_unchanged_behavior() {
        let text = "CREATE VIEW foo AS SELECT 1;";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), None).is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn typecheck_unknown_column_attaches_quickfix_data() {
        use crate::lsp::code_action::{Candidates, QuickFixData};
        use crate::project::compiler::typecheck::{ObjectTypeCheckError, ObjectTypeCheckErrorKind};
        use crate::project::ir::object_id::ObjectId;
        use mz_repr::ColumnName;
        use mz_sql::plan::PlanError;
        use std::sync::Arc;

        let source = "SELECT custoser_name FROM users";
        let path = std::env::temp_dir().join("typecheck_qf_test.sql");
        std::fs::write(&path, source).unwrap();

        let plan_err = PlanError::UnknownColumn {
            table: None,
            column: ColumnName::from("custoser_name"),
            similar: Box::new([ColumnName::from("customer_name")]),
        };
        let err = ObjectTypeCheckError {
            object_id: ObjectId::new(
                "materialize".to_string(),
                "public".to_string(),
                "v".to_string(),
            ),
            file_path: path.clone(),
            kind: ObjectTypeCheckErrorKind::Plan(Arc::new(plan_err)),
        };
        let tc = TypeCheckError::Multiple(vec![err]);

        let fs = FileSystem::default();
        let candidates = Candidates::default();
        let map = typecheck_diagnostics(&fs, &tc, &candidates);
        let diags = map.get(&path).expect("diags for file");
        assert_eq!(diags.len(), 1);

        let data = diags[0]
            .data
            .as_ref()
            .expect("Diagnostic.data should be set when suggestions exist");
        let qf: QuickFixData = serde_json::from_value(data.clone()).expect("decodes");
        assert_eq!(qf.suggestions.len(), 1);
        assert_eq!(qf.suggestions[0].alternatives.len(), 1);
        assert_eq!(qf.suggestions[0].alternatives[0].new_text, "customer_name");
        assert!(
            diags[0]
                .message
                .contains("column custoser_name does not exist")
        );
        let _ = std::fs::remove_file(&path);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn typecheck_unknown_item_attaches_fuzzy_quickfix_data() {
        use crate::lsp::code_action::{Candidates, QuickFixData};
        use crate::project::compiler::typecheck::{ObjectTypeCheckError, ObjectTypeCheckErrorKind};
        use crate::project::ir::object_id::ObjectId;
        use mz_sql::catalog::CatalogError;

        let source = "SELECT * FROM cusotmers";
        let path = std::env::temp_dir().join("typecheck_fuzzy_test.sql");
        std::fs::write(&path, source).unwrap();

        let err = ObjectTypeCheckError {
            object_id: ObjectId::new(
                "materialize".to_string(),
                "public".to_string(),
                "v".to_string(),
            ),
            file_path: path.clone(),
            kind: ObjectTypeCheckErrorKind::Catalog(CatalogError::UnknownItem(
                "cusotmers".to_string(),
            )),
        };
        let tc = TypeCheckError::Multiple(vec![err]);

        let fs = FileSystem::default();
        let candidates = Candidates {
            items: vec!["customers".to_string()],
            ..Default::default()
        };
        let map = typecheck_diagnostics(&fs, &tc, &candidates);
        let diags = map.get(&path).expect("diags for file");
        assert_eq!(diags.len(), 1);

        let data = diags[0]
            .data
            .as_ref()
            .expect("Diagnostic.data should be set");
        let qf: QuickFixData = serde_json::from_value(data.clone()).expect("decodes");
        assert_eq!(qf.suggestions.len(), 1);
        assert_eq!(qf.suggestions[0].alternatives.len(), 1);
        assert_eq!(qf.suggestions[0].alternatives[0].new_text, "customers");
        let _ = std::fs::remove_file(&path);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn validation_object_name_mismatch_attaches_quickfix_data() {
        use crate::lsp::code_action::QuickFixData;
        use crate::project::error::validation::ErrorContext;
        use crate::project::error::{ValidationError, ValidationErrorKind};

        let source = "CREATE TABLE customers (id INT);";
        let path = std::env::temp_dir().join("validation_qf_test.sql");
        std::fs::write(&path, source).unwrap();

        let err = ValidationError {
            kind: ValidationErrorKind::ObjectNameMismatch {
                declared: "customers".to_string(),
                expected: "users".to_string(),
            },
            context: ErrorContext {
                file: path.clone(),
                sql_statement: Some(source.to_string()),
                byte_offset: Some(0),
            },
        };

        let fs = FileSystem::default();
        let map = validation_diagnostics(&fs, &[err]);
        let diags = map.get(&path).expect("diags for file");
        assert_eq!(diags.len(), 1);

        let data = diags[0]
            .data
            .as_ref()
            .expect("Diagnostic.data should be set");
        let qf: QuickFixData = serde_json::from_value(data.clone()).expect("decodes");
        assert_eq!(qf.suggestions[0].alternatives[0].new_text, "users");
        let _ = std::fs::remove_file(&path);
    }
}
