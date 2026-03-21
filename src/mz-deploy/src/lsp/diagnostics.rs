//! SQL diagnostic conversion for the LSP server.
//!
//! Provides two tiers of diagnostics:
//!
//! - **Per-keystroke parse errors** ([`diagnose()`]) — Parses SQL text with
//!   [`mz_sql_parser::parser::parse_statements()`] and converts any
//!   [`ParserStatementError`] into LSP [`Diagnostic`]s with correct
//!   line/column positions. The byte offset from [`ParserError::pos`] is mapped
//!   to an LSP [`Position`] using a ropey [`Rope`].
//!
//! - **On-save validation errors** ([`validation_diagnostics()`]) — Converts
//!   project-level [`ValidationError`]s into LSP diagnostics grouped by file.
//!   All validation diagnostics are positioned at line 0, col 0 since the
//!   errors don't carry byte offsets.

use crate::project::error::ValidationError;
use ropey::Rope;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tower_lsp::lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};

/// Parse `text` as SQL and return diagnostics for any parse errors.
///
/// # Arguments
/// * `text` — The SQL source text to parse.
/// * `rope` — A [`Rope`] built from the same `text`, used for byte-offset to
///   line/column conversion.
///
/// # Returns
/// A (possibly empty) vec of LSP diagnostics. At most one diagnostic is
/// returned per parse invocation since the parser stops at the first error.
pub fn diagnose(text: &str, rope: &Rope) -> Vec<Diagnostic> {
    if text.trim().is_empty() {
        return Vec::new();
    }

    match mz_sql_parser::parser::parse_statements(text) {
        Ok(_) => Vec::new(),
        Err(e) => {
            let position =
                offset_to_position(e.error.pos, rope).unwrap_or_else(|| Position::new(0, 0));
            vec![Diagnostic {
                range: Range::new(position, position),
                severity: Some(DiagnosticSeverity::ERROR),
                source: Some("mz-deploy".to_string()),
                message: e.error.message.clone(),
                ..Default::default()
            }]
        }
    }
}

/// Convert [`ValidationError`]s into LSP diagnostics grouped by file path.
///
/// Each error becomes a [`Diagnostic`] at position `(0, 0)` with `ERROR` severity
/// and the error kind's short message. Errors are grouped by the file in their
/// [`ErrorContext`](crate::project::error::ErrorContext).
///
/// Returns an empty map when `errors` is empty.
pub fn validation_diagnostics(errors: &[ValidationError]) -> BTreeMap<PathBuf, Vec<Diagnostic>> {
    let mut map: BTreeMap<PathBuf, Vec<Diagnostic>> = BTreeMap::new();
    let zero = Position::new(0, 0);

    for error in errors {
        map.entry(error.context.file.clone())
            .or_default()
            .push(Diagnostic {
                range: Range::new(zero, zero),
                severity: Some(DiagnosticSeverity::ERROR),
                source: Some("mz-deploy".to_string()),
                message: error.kind.message(),
                ..Default::default()
            });
    }

    map
}

/// Convert a byte offset to an LSP [`Position`] (line, column) using a [`Rope`].
fn offset_to_position(offset: usize, rope: &Rope) -> Option<Position> {
    let line = rope.try_char_to_line(offset).ok()?;
    let first_char_of_line = rope.try_line_to_char(line).ok()?;
    let column = offset - first_char_of_line;

    let line_u32 = line.try_into().ok()?;
    let column_u32 = column.try_into().ok()?;

    Some(Position::new(line_u32, column_u32))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::error::{ErrorContext, ValidationErrorKind};

    #[test]
    fn valid_sql_produces_no_diagnostics() {
        let text = "CREATE VIEW foo AS SELECT 1;";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope).is_empty());
    }

    #[test]
    fn syntax_error_produces_diagnostic_at_correct_position() {
        let text = "CREATE VIEW foo AS SELECTT 1;";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        // Error should be on line 0 (first line)
        assert_eq!(diags[0].range.start.line, 0);
    }

    #[test]
    fn multiline_error_position() {
        let text = "CREATE VIEW foo AS\nSELECT 1;\nCREATE VIEW bar AS SELECTT 2;";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope);
        assert_eq!(diags.len(), 1);
        // Error should be on line 2 (third line, zero-indexed)
        assert_eq!(diags[0].range.start.line, 2);
    }

    #[test]
    fn empty_file_produces_no_diagnostics() {
        let text = "";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope).is_empty());
    }

    #[test]
    fn whitespace_only_file_produces_no_diagnostics() {
        let text = "   \n  \n  ";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope).is_empty());
    }

    fn make_validation_error(file: &str, object_name: &str) -> ValidationError {
        ValidationError {
            kind: ValidationErrorKind::NoMainStatement {
                object_name: object_name.to_string(),
            },
            context: ErrorContext {
                file: PathBuf::from(file),
                sql_statement: None,
            },
        }
    }

    #[test]
    fn single_validation_error_produces_one_diagnostic() {
        let errors = vec![make_validation_error("db/schema/view.sql", "view")];
        let result = validation_diagnostics(&errors);
        assert_eq!(result.len(), 1);
        let diags = &result[&PathBuf::from("db/schema/view.sql")];
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        assert_eq!(diags[0].source.as_deref(), Some("mz-deploy"));
        assert_eq!(diags[0].range.start.line, 0);
        assert_eq!(diags[0].range.start.character, 0);
    }

    #[test]
    fn multiple_errors_same_file_grouped() {
        let errors = vec![
            make_validation_error("db/schema/view.sql", "view"),
            make_validation_error("db/schema/view.sql", "view2"),
        ];
        let result = validation_diagnostics(&errors);
        assert_eq!(result.len(), 1);
        assert_eq!(result[&PathBuf::from("db/schema/view.sql")].len(), 2);
    }

    #[test]
    fn multiple_errors_different_files_separate_entries() {
        let errors = vec![
            make_validation_error("db/schema/a.sql", "a"),
            make_validation_error("db/schema/b.sql", "b"),
        ];
        let result = validation_diagnostics(&errors);
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&PathBuf::from("db/schema/a.sql")));
        assert!(result.contains_key(&PathBuf::from("db/schema/b.sql")));
    }

    #[test]
    fn empty_errors_returns_empty_map() {
        let result = validation_diagnostics(&[]);
        assert!(result.is_empty());
    }
}
