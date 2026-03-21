//! SQL diagnostic conversion for the LSP server.
//!
//! Provides two tiers of diagnostics:
//!
//! - **Per-keystroke diagnostics** ([`diagnose()`]) — Resolves psql-style
//!   variables before parsing. Unresolved variables produce positioned
//!   diagnostics (ERROR or WARNING depending on the warn pragma). The resolved
//!   SQL is then parsed with [`mz_sql_parser::parser::parse_statements()`] and
//!   any parse error positions are mapped back to original-text offsets via
//!   [`resolved_to_original`]. The [`Rope`] is always built from the original
//!   text (what the editor shows), since all emitted byte offsets are in
//!   original-text space.
//!
//! - **On-save validation errors** ([`validation_diagnostics()`]) — Converts
//!   project-level [`ValidationError`]s into LSP diagnostics grouped by file.
//!   When an error carries a byte offset (most statement-level errors), the
//!   diagnostic is positioned at the correct line/column. File-level errors
//!   (e.g., missing CREATE statement) fall back to `(0, 0)`.

use crate::project::error::ValidationError;
use crate::project::variables::{resolve_variables, resolved_to_original};
use ropey::Rope;
use std::collections::BTreeMap;
use std::path::PathBuf;
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
/// * `profile_name` — Active profile name, shown in undefined-variable messages.
///
/// # Returns
/// A (possibly empty) vec of LSP diagnostics. Variable diagnostics are
/// `WARNING` if the file has the warn pragma, `ERROR` otherwise. Parse errors
/// are always `ERROR`.
pub fn diagnose(
    text: &str,
    rope: &Rope,
    variables: &BTreeMap<String, String>,
    profile_name: &str,
) -> Vec<Diagnostic> {
    if text.trim().is_empty() {
        return Vec::new();
    }

    let resolved = resolve_variables(text, variables);
    let mut diags = Vec::new();

    // Unresolved variable diagnostics — positioned in original text.
    let var_severity = if resolved.has_warn_pragma {
        DiagnosticSeverity::WARNING
    } else {
        DiagnosticSeverity::ERROR
    };
    for uv in &resolved.unresolved {
        let position =
            offset_to_position(uv.byte_offset, rope).unwrap_or_else(|| Position::new(0, 0));
        let end_position = offset_to_position(uv.byte_offset + uv.byte_len, rope)
            .unwrap_or_else(|| Position::new(0, 0));
        diags.push(Diagnostic {
            range: Range::new(position, end_position),
            severity: Some(var_severity),
            source: Some("mz-deploy".to_string()),
            message: format!(
                "undefined variable ':{}'  — define in [profiles.{}.variables] in project.toml",
                uv.name, profile_name
            ),
            ..Default::default()
        });
    }

    // Parse the resolved SQL; map errors back to original-text positions.
    if let Err(e) = mz_sql_parser::parser::parse_statements(&resolved.sql) {
        let original_offset = resolved_to_original(e.error.pos, &resolved.substitutions);
        let position =
            offset_to_position(original_offset, rope).unwrap_or_else(|| Position::new(0, 0));
        diags.push(Diagnostic {
            range: Range::new(position, position),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            message: e.error.message.clone(),
            ..Default::default()
        });
    }

    diags
}

/// Convert [`ValidationError`]s into LSP diagnostics grouped by file path.
///
/// When an error carries a `byte_offset`, the file is read and a [`Rope`] is
/// built so the offset can be converted to a precise line/column position.
/// Errors without an offset (file-level) fall back to `(0, 0)`.
///
/// Returns an empty map when `errors` is empty.
pub fn validation_diagnostics(errors: &[ValidationError]) -> BTreeMap<PathBuf, Vec<Diagnostic>> {
    let mut map: BTreeMap<PathBuf, Vec<Diagnostic>> = BTreeMap::new();
    // Cache ropes per file so we only read each file once.
    let mut rope_cache: BTreeMap<PathBuf, Option<Rope>> = BTreeMap::new();
    let zero = Position::new(0, 0);

    for error in errors {
        let position = if let Some(offset) = error.context.byte_offset {
            let rope = rope_cache
                .entry(error.context.file.clone())
                .or_insert_with(|| {
                    std::fs::read_to_string(&error.context.file)
                        .ok()
                        .map(|s| Rope::from_str(&s))
                });
            rope.as_ref()
                .and_then(|r| offset_to_position(offset, r))
                .unwrap_or(zero)
        } else {
            zero
        };

        map.entry(error.context.file.clone())
            .or_default()
            .push(Diagnostic {
                range: Range::new(position, position),
                severity: Some(DiagnosticSeverity::ERROR),
                source: Some("mz-deploy".to_string()),
                message: error.kind.message(),
                ..Default::default()
            });
    }

    map
}

/// Convert a byte offset to an LSP [`Position`] (line, column) using a [`Rope`].
pub(crate) fn offset_to_position(offset: usize, rope: &Rope) -> Option<Position> {
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
        assert!(diagnose(text, &rope, &BTreeMap::new(), "default").is_empty());
    }

    #[test]
    fn syntax_error_produces_diagnostic_at_correct_position() {
        let text = "CREATE VIEW foo AS SELECTT 1;";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), "default");
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Some(DiagnosticSeverity::ERROR));
        // Error should be on line 0 (first line)
        assert_eq!(diags[0].range.start.line, 0);
    }

    #[test]
    fn multiline_error_position() {
        let text = "CREATE VIEW foo AS\nSELECT 1;\nCREATE VIEW bar AS SELECTT 2;";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), "default");
        assert_eq!(diags.len(), 1);
        // Error should be on line 2 (third line, zero-indexed)
        assert_eq!(diags[0].range.start.line, 2);
    }

    #[test]
    fn empty_file_produces_no_diagnostics() {
        let text = "";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), "default").is_empty());
    }

    #[test]
    fn whitespace_only_file_produces_no_diagnostics() {
        let text = "   \n  \n  ";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), "default").is_empty());
    }

    fn make_validation_error(file: &str, object_name: &str) -> ValidationError {
        ValidationError {
            kind: ValidationErrorKind::NoMainStatement {
                object_name: object_name.to_string(),
            },
            context: ErrorContext {
                file: PathBuf::from(file),
                sql_statement: None,
                byte_offset: None,
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

    // --- Variable-aware diagnose tests ---

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn resolved_variable_no_diagnostics() {
        let text = "CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), "default");
        assert!(diags.is_empty());
    }

    #[test]
    fn resolved_variable_produces_clean_parse() {
        let v = vars(&[("cluster", "quickstart")]);
        let text = "CREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &v, "default");
        assert!(diags.is_empty());
    }

    #[test]
    fn unresolved_variable_produces_error() {
        let text = "CREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), "default");
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

    #[test]
    fn unresolved_variable_with_pragma_produces_warning() {
        let text = "-- PRAGMA WARN_ON_MISSING_VARIABLES;\nCREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &BTreeMap::new(), "default");
        let var_diags: Vec<_> = diags
            .iter()
            .filter(|d| d.message.contains("undefined variable"))
            .collect();
        assert_eq!(var_diags.len(), 1);
        assert_eq!(var_diags[0].severity, Some(DiagnosticSeverity::WARNING));
    }

    #[test]
    fn parse_error_maps_back_to_original_position() {
        // After resolving :x → "ab", the parse error in resolved text
        // should map back to the original text position.
        let v = vars(&[("x", "ab")]);
        // "CREATE VIEW :x AS SELECTT 1" → "CREATE VIEW ab AS SELECTT 1"
        let text = "CREATE VIEW :x AS SELECTT 1";
        let rope = Rope::from_str(text);
        let diags = diagnose(text, &rope, &v, "default");
        // Should have exactly one parse error diagnostic.
        let parse_diags: Vec<_> = diags
            .iter()
            .filter(|d| !d.message.contains("undefined variable"))
            .collect();
        assert_eq!(parse_diags.len(), 1);
        assert_eq!(parse_diags[0].severity, Some(DiagnosticSeverity::ERROR));
        assert_eq!(parse_diags[0].range.start.line, 0);
    }

    #[test]
    fn no_variables_unchanged_behavior() {
        let text = "CREATE VIEW foo AS SELECT 1;";
        let rope = Rope::from_str(text);
        assert!(diagnose(text, &rope, &BTreeMap::new(), "default").is_empty());
    }
}
