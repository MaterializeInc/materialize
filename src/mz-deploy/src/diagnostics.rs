// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Source-positioned diagnostics decoupled from any output format.
//!
//! [`PositionalDiagnostic`] is the neutral intermediate representation: a
//! severity, a file path, a source string, and a byte range within that
//! source. The LSP server wraps it into [`tower_lsp::lsp_types::Diagnostic`]
//! by converting the byte range to line/column via a [`ropey::Rope`]; the CLI
//! wraps it into an [`annotate_snippets`] snippet for terminal output.
//!
//! Both consumers share the locator helpers in this module
//! ([`find_identifier`], [`find_identifier_after`], [`locate_plan`],
//! [`locate_catalog`], [`locate_typecheck`], [`locate_validation`]) which
//! derive byte ranges from `mz_sql` and validation errors that carry only an
//! identifier name (no offset). The module also exposes
//! [`format_typecheck_kind`] and [`format_validation_kind`], the shared
//! formatters that turn an error kind into a `(message, footers, suggestions)`
//! triple — `footers` carry class-level advice, `suggestions` carry
//! mechanical edits encoded as byte-range replacements.

use std::ops::Range;
use std::path::PathBuf;

use mz_repr::ColumnName;
use mz_sql::catalog::CatalogError;
use mz_sql::names::PartialItemName;
use mz_sql::plan::PlanError;

use crate::project::compiler::typecheck::ObjectTypeCheckErrorKind;
use crate::project::error::ValidationErrorKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Severity {
    Error,
    Warning,
}

/// A diagnostic anchored to a byte range within a single source file.
///
/// Output-format-neutral: the LSP wraps it into `tower_lsp::Diagnostic`,
/// the CLI wraps it into an `annotate_snippets` snippet.
#[derive(Debug, Clone)]
pub(crate) struct PositionalDiagnostic {
    pub severity: Severity,
    pub file: PathBuf,
    /// The source text the byte range refers into. Owned so renderers
    /// don't need separate filesystem access.
    pub source: String,
    /// Half-open byte range within `source`. May be empty (`start == end`)
    /// for caret-style positions.
    pub byte_range: Range<usize>,
    pub message: String,
    /// Plain help lines shown beneath the snippet.
    pub footers: Vec<String>,
    /// Structured replacement suggestions. Each renders as its own help
    /// section with the patch shown inline (rustc-style "did you mean").
    pub suggestions: Vec<Suggestion>,
}

/// One or more interchangeable replacements offered under a single help
/// title. Renders as a rustc-style multi-suggestion block:
///
/// ```text
/// help: did you mean one of these?
///   |
/// 4 -     custoser_name,
/// 4 +     customer_id,
///   |
/// 4 -     custoser_name,
/// 4 +     customer_name,
///   |
/// ```
#[derive(Debug, Clone)]
pub(crate) struct Suggestion {
    pub label: String,
    pub alternatives: Vec<Replacement>,
}

/// A single replacement: substitute `byte_range` of the source with
/// `replacement`.
#[derive(Debug, Clone)]
pub(crate) struct Replacement {
    pub byte_range: Range<usize>,
    pub replacement: String,
}

/// Locate the byte range a typecheck error points at within `source`.
///
/// Dispatches to [`locate_plan`] / [`locate_catalog`] for the wrapped
/// `mz_sql` error, or returns the parser's byte offset directly.
/// `Internal` variants have no locatable position.
pub(crate) fn locate_typecheck(
    kind: &ObjectTypeCheckErrorKind,
    source: &str,
) -> Option<Range<usize>> {
    match kind {
        ObjectTypeCheckErrorKind::Parser(e) => {
            let pos = e.error.pos;
            Some(pos..pos)
        }
        ObjectTypeCheckErrorKind::Plan(e) => locate_plan(e, source),
        ObjectTypeCheckErrorKind::Catalog(e) => locate_catalog(e, source),
        ObjectTypeCheckErrorKind::Internal(_) => None,
    }
}

/// Locate the byte range a [`PlanError`] points at within `source`.
///
/// For variants that carry a column or function name, finds the first
/// whole-word occurrence in `source`. For variants that wrap a
/// `ParserError`, returns its byte offset directly.
pub(crate) fn locate_plan(e: &PlanError, source: &str) -> Option<Range<usize>> {
    use PlanError::*;
    match e {
        UnknownColumn { column, .. }
        | UngroupedColumn { column, .. }
        | UnknownColumnInUsingClause { column, .. }
        | AmbiguousColumnInUsingClause { column, .. }
        | WrongJoinTypeForLateralColumn { column, .. } => find_identifier(source, column.as_str()),
        AmbiguousColumn(column) => find_identifier(source, column.as_str()),
        AmbiguousTable(name) => find_identifier(source, name.item.as_str()),
        UnknownFunction { name, .. }
        | IndistinctFunction { name, .. }
        | UnknownOperator { name, .. }
        | IndistinctOperator { name, .. } => find_identifier(source, last_component(name)),
        Parser(p) => Some(p.pos..p.pos),
        ParserStatement(p) => Some(p.error.pos..p.error.pos),
        Catalog(c) => locate_catalog(c, source),
        _ => None,
    }
}

/// Locate the byte range a [`CatalogError`] points at within `source`.
pub(crate) fn locate_catalog(e: &CatalogError, source: &str) -> Option<Range<usize>> {
    use CatalogError::*;
    match e {
        UnknownDatabase(name)
        | UnknownSchema(name)
        | UnknownRole(name)
        | UnknownCluster(name)
        | UnknownClusterReplica(name)
        | UnknownConnection(name)
        | UnknownNetworkPolicy(name)
        | UnknownItem(name) => find_identifier(source, last_component(name)),
        UnknownFunction { name, .. } | UnknownType { name, .. } => {
            find_identifier(source, last_component(name))
        }
        _ => None,
    }
}

/// Strip qualifying prefixes from a dotted identifier, returning the final
/// component. `schema.table` → `table`; `t` → `t`.
pub(crate) fn last_component(s: &str) -> &str {
    s.rsplit_once('.').map(|(_, last)| last).unwrap_or(s)
}

/// Find the first whole-word occurrence of `name` in `source`.
///
/// "Whole word" means the bytes adjacent to the match are not identifier
/// characters (`[A-Za-z0-9_]`). Returns the half-open byte range of the
/// match, or `None` if `name` does not appear as a standalone token.
pub(crate) fn find_identifier(source: &str, name: &str) -> Option<Range<usize>> {
    if name.is_empty() {
        return None;
    }
    let bytes = source.as_bytes();
    let needle = name.as_bytes();
    if needle.len() > bytes.len() {
        return None;
    }
    for start in 0..=(bytes.len() - needle.len()) {
        if &bytes[start..start + needle.len()] != needle {
            continue;
        }
        let before_ok = start == 0 || !is_ident_byte(bytes[start - 1]);
        let end = start + needle.len();
        let after_ok = end == bytes.len() || !is_ident_byte(bytes[end]);
        if before_ok && after_ok {
            return Some(start..end);
        }
    }
    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Same as [`find_identifier`] but starts the search at `start_byte`.
/// Returns absolute byte ranges into `source`.
pub(crate) fn find_identifier_after(
    source: &str,
    name: &str,
    start_byte: usize,
) -> Option<Range<usize>> {
    let slice = source.get(start_byte..)?;
    let local = find_identifier(slice, name)?;
    Some((start_byte + local.start)..(start_byte + local.end))
}

/// Build the (message, footers, suggestions) triple for one typecheck kind.
///
/// Variants that carry alternatives (`UnknownColumn::similar`,
/// `UnknownFunction::alternative`) are formatted directly so we control
/// identifier quoting and can emit structured patches. Other variants fall
/// back to `Display` + the upstream `hint()`.
pub(crate) fn format_typecheck_kind(
    kind: &ObjectTypeCheckErrorKind,
    source: &str,
    primary_range: &Range<usize>,
) -> (String, Vec<String>, Vec<Suggestion>) {
    match kind {
        ObjectTypeCheckErrorKind::Plan(e) => format_plan(e, source, primary_range),
        ObjectTypeCheckErrorKind::Catalog(e) => format_catalog(e, source, primary_range),
        ObjectTypeCheckErrorKind::Parser(e) => (e.to_string(), Vec::new(), Vec::new()),
        ObjectTypeCheckErrorKind::Internal(msg) => (msg.clone(), Vec::new(), Vec::new()),
    }
}

fn format_plan(
    e: &PlanError,
    source: &str,
    primary_range: &Range<usize>,
) -> (String, Vec<String>, Vec<Suggestion>) {
    if let PlanError::UnknownColumn {
        table,
        column,
        similar,
    } = e
    {
        let qualified = column_display(table.as_ref(), column);
        let message = format!("column {qualified} does not exist");
        if similar.is_empty() {
            return (message, Vec::new(), Vec::new());
        }
        let span = locate_replacement(source, primary_range, column.as_str());
        let label = match similar.as_ref() {
            [single] => format!("did you mean `{}`?", column_display(table.as_ref(), single)),
            _ => "did you mean one of these?".to_string(),
        };
        let alternatives = similar
            .iter()
            .map(|alt| Replacement {
                byte_range: span.clone(),
                replacement: alt.as_str().to_string(),
            })
            .collect();
        return (
            message,
            Vec::new(),
            vec![Suggestion {
                label,
                alternatives,
            }],
        );
    }
    fallback_plan(e)
}

fn fallback_plan(e: &PlanError) -> (String, Vec<String>, Vec<Suggestion>) {
    let footers = e.hint().into_iter().collect();
    (e.to_string(), footers, Vec::new())
}

fn format_catalog(
    e: &CatalogError,
    source: &str,
    primary_range: &Range<usize>,
) -> (String, Vec<String>, Vec<Suggestion>) {
    match e {
        CatalogError::UnknownFunction {
            name,
            alternative: Some(alt),
        } => {
            let message = format!("function {name} does not exist");
            let suggestion = Suggestion {
                label: format!("did you mean `{alt}`?"),
                alternatives: vec![Replacement {
                    byte_range: locate_replacement(source, primary_range, last_component(name)),
                    replacement: alt.clone(),
                }],
            };
            (message, Vec::new(), vec![suggestion])
        }
        other => fallback_catalog(other),
    }
}

fn fallback_catalog(e: &CatalogError) -> (String, Vec<String>, Vec<Suggestion>) {
    let footers = e.hint().into_iter().collect();
    (e.to_string(), footers, Vec::new())
}

/// Format `table.column` as a dotted PostgreSQL reference (relation +
/// column). Each component is rendered as its raw identifier — no outer
/// quotes — so a reader interprets the dot as a separator rather than as
/// part of a single quoted identifier.
fn column_display(table: Option<&PartialItemName>, column: &ColumnName) -> String {
    match table {
        Some(t) => format!("{}.{}", t.item, column),
        None => column.as_str().to_string(),
    }
}

/// Find the byte range of `needle` to replace.
///
/// Prefer the primary annotation range when its content matches `needle`;
/// otherwise fall back to a whole-word search of the source so the patch
/// still lands somewhere reasonable for variants whose locator returned a
/// less specific span.
pub(crate) fn locate_replacement(
    source: &str,
    primary_range: &Range<usize>,
    needle: &str,
) -> Range<usize> {
    let in_bounds = primary_range.end <= source.len() && primary_range.start <= primary_range.end;
    if in_bounds && &source[primary_range.clone()] == needle {
        return primary_range.clone();
    }
    find_identifier(source, needle).unwrap_or_else(|| primary_range.clone())
}

/// Locate the byte range of the declared identifier in a *Mismatch
/// validation error. Returns `None` for variants that don't carry a
/// `declared` name we can rewrite.
pub(crate) fn locate_validation(
    kind: &ValidationErrorKind,
    source: &str,
    statement_offset: Option<usize>,
) -> Option<Range<usize>> {
    let (needle, _) = mismatch_pair(kind)?;
    find_identifier_after(source, needle, statement_offset.unwrap_or(0))
}

/// Build the (message, footers, suggestions) triple for a validation kind.
///
/// For *Mismatch variants the suggestion is a single replacement that
/// rewrites the declared identifier to the expected one. Other variants
/// surface only the message and any upstream `help()` text.
pub(crate) fn format_validation_kind(
    kind: &ValidationErrorKind,
    source: &str,
    primary_range: &Range<usize>,
) -> (String, Vec<String>, Vec<Suggestion>) {
    let message = kind.message();
    let footers: Vec<String> = kind.help().into_iter().collect();
    let suggestions = mismatch_suggestion(kind, source, primary_range);
    (message, footers, suggestions)
}

/// `Some((declared, expected))` if `kind` is a rewritable *Mismatch variant
/// — the trailing identifier the user wrote, plus the one their file path
/// requires.
fn mismatch_pair(kind: &ValidationErrorKind) -> Option<(&str, &str)> {
    use ValidationErrorKind::*;
    match kind {
        ObjectNameMismatch { declared, expected }
        | SchemaMismatch { declared, expected }
        | DatabaseMismatch { declared, expected }
        | ClusterNameMismatch { declared, expected }
        | RoleNameMismatch { declared, expected }
        | NetworkPolicyNameMismatch { declared, expected } => {
            Some((declared.as_str(), expected.as_str()))
        }
        _ => None,
    }
}

fn mismatch_suggestion(
    kind: &ValidationErrorKind,
    source: &str,
    primary_range: &Range<usize>,
) -> Vec<Suggestion> {
    let Some((declared, expected)) = mismatch_pair(kind) else {
        return Vec::new();
    };
    let span = locate_replacement(source, primary_range, declared);
    vec![Suggestion {
        label: format!("rename to `{expected}`"),
        alternatives: vec![Replacement {
            byte_range: span,
            replacement: expected.to_string(),
        }],
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::ColumnName;
    use std::sync::Arc;

    #[test]
    fn find_identifier_skips_substrings() {
        let source = "SELECT customer_id, id FROM t";
        let r = find_identifier(source, "id").unwrap();
        assert_eq!(&source[r.clone()], "id");
        assert_eq!(r.start, 20);
    }

    #[test]
    fn find_identifier_empty_needle() {
        assert!(find_identifier("anything", "").is_none());
    }

    #[test]
    fn find_identifier_absent() {
        assert!(find_identifier("SELECT 1", "missing").is_none());
    }

    #[test]
    fn find_identifier_at_start() {
        let r = find_identifier("foo bar", "foo").unwrap();
        assert_eq!(r, 0..3);
    }

    #[test]
    fn find_identifier_at_end() {
        let r = find_identifier("foo bar", "bar").unwrap();
        assert_eq!(r, 4..7);
    }

    #[test]
    fn find_identifier_needle_longer_than_haystack() {
        assert!(find_identifier("ab", "abcd").is_none());
    }

    #[test]
    fn last_component_strips_qualifier() {
        assert_eq!(last_component("foo"), "foo");
        assert_eq!(last_component("schema.table"), "table");
        assert_eq!(last_component("db.schema.table"), "table");
    }

    #[test]
    fn locate_plan_unknown_column() {
        let source = "CREATE VIEW v AS SELECT bogus FROM t";
        let e = PlanError::UnknownColumn {
            table: None,
            column: ColumnName::from("bogus"),
            similar: Box::new([]),
        };
        let r = locate_plan(&e, source).unwrap();
        assert_eq!(&source[r.clone()], "bogus");
        assert_eq!(r, 24..29);
    }

    #[test]
    fn locate_plan_unknown_function() {
        let source = "SELECT bogus_fn(1) FROM t";
        let e = PlanError::UnknownFunction {
            name: "bogus_fn".to_string(),
            arg_types: vec!["int4".to_string()],
        };
        let r = locate_plan(&e, source).unwrap();
        assert_eq!(&source[r], "bogus_fn");
    }

    #[test]
    fn locate_plan_unhandled_variant_returns_none() {
        let e = PlanError::Unstructured("anything".into());
        assert!(locate_plan(&e, "SELECT 1").is_none());
    }

    #[test]
    fn locate_catalog_unknown_item_strips_qualifier() {
        let source = "SELECT * FROM bogus_table";
        let e = CatalogError::UnknownItem("schema.bogus_table".to_string());
        let r = locate_catalog(&e, source).unwrap();
        assert_eq!(&source[r], "bogus_table");
    }

    #[test]
    fn locate_typecheck_internal_returns_none() {
        let kind = ObjectTypeCheckErrorKind::Internal("boom".into());
        assert!(locate_typecheck(&kind, "anything").is_none());
    }

    #[test]
    fn locate_typecheck_dispatches_to_plan() {
        let source = "CREATE VIEW v AS SELECT bogus FROM t";
        let kind = ObjectTypeCheckErrorKind::Plan(Arc::new(PlanError::UnknownColumn {
            table: None,
            column: ColumnName::from("bogus"),
            similar: Box::new([]),
        }));
        let r = locate_typecheck(&kind, source).unwrap();
        assert_eq!(&source[r], "bogus");
    }

    #[test]
    fn locate_typecheck_dispatches_to_catalog() {
        let source = "SELECT * FROM bogus_table";
        let kind =
            ObjectTypeCheckErrorKind::Catalog(CatalogError::UnknownItem("bogus_table".into()));
        let r = locate_typecheck(&kind, source).unwrap();
        assert_eq!(&source[r], "bogus_table");
    }

    #[test]
    fn locate_replacement_prefers_primary_range_when_matches() {
        let r = locate_replacement("SELECT emails FROM t", &(7..13), "emails");
        assert_eq!(r, 7..13);
    }

    #[test]
    fn locate_replacement_falls_back_to_search() {
        let r = locate_replacement("SELECT emails FROM t", &(0..0), "emails");
        assert_eq!(r, 7..13);
    }

    #[test]
    fn find_identifier_after_skips_earlier_occurrence() {
        let source = "CREATE TABLE foo (...);\nCREATE VIEW v AS SELECT * FROM foo;";
        let r = find_identifier_after(source, "foo", 24).unwrap();
        // Match should be the second `foo`, not the first.
        assert!(r.start > 24);
        assert_eq!(&source[r.clone()], "foo");
    }

    #[test]
    fn locate_validation_object_name_mismatch_finds_declared_token() {
        use crate::project::error::ValidationErrorKind;
        let source = "CREATE TABLE customers (id INT);";
        let kind = ValidationErrorKind::ObjectNameMismatch {
            declared: "customers".to_string(),
            expected: "users".to_string(),
        };
        let r = locate_validation(&kind, source, Some(0)).unwrap();
        assert_eq!(&source[r], "customers");
    }

    #[test]
    fn format_validation_kind_object_name_mismatch_yields_rename_suggestion() {
        use crate::project::error::ValidationErrorKind;
        let source = "CREATE TABLE customers (id INT);";
        let kind = ValidationErrorKind::ObjectNameMismatch {
            declared: "customers".to_string(),
            expected: "users".to_string(),
        };
        let primary = locate_validation(&kind, source, Some(0)).unwrap();
        let (msg, footers, suggestions) = format_validation_kind(&kind, source, &primary);
        assert!(msg.contains("declared 'customers'"));
        assert!(msg.contains("expected 'users'"));
        // Footer carries the class-level rule (the "why").
        assert!(
            footers
                .iter()
                .any(|f| f.contains("must match the .sql file name"))
        );
        // Suggestion carries the mechanical edit (the "what").
        assert!(suggestions[0].label.contains("users"));
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].alternatives.len(), 1);
        assert_eq!(suggestions[0].alternatives[0].replacement, "users");
        assert_eq!(
            &source[suggestions[0].alternatives[0].byte_range.clone()],
            "customers"
        );
    }

    #[test]
    fn format_validation_kind_unhandled_returns_no_suggestions() {
        use crate::project::error::ValidationErrorKind;
        let kind = ValidationErrorKind::NoMainStatement {
            object_name: "x".to_string(),
        };
        let (_msg, _footers, sugg) = format_validation_kind(&kind, "", &(0..0));
        assert!(sugg.is_empty());
    }
}
