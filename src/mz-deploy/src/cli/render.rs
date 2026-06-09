// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rich CLI rendering for [`PositionalDiagnostic`]s.
//!
//! [`render`] turns one diagnostic into a styled [`annotate_snippets`] string.
//! [`to_positional`] inspects a [`CliError`] and pulls out any positional
//! diagnostics it carries so `display_error` can render rustc-quality output:
//! caret under the offending token, file/line origin, plain `help:` footers,
//! and `did you mean` patches that show the suggested replacement inline.
//!
//! Errors that don't carry source positions (configuration errors, network
//! failures, etc.) return an empty `Vec`; the caller falls back to the plain
//! [`std::fmt::Display`] path.

use crate::cli::CliError;
use crate::diagnostics::{PositionalDiagnostic, Severity};
use crate::log::color_enabled;
use crate::project::compiler::typecheck::{ObjectTypeCheckError, TypeCheckError};
use crate::project::error::{ParseError, ProjectError, ValidationError, ValidationErrors};
use annotate_snippets::{AnnotationKind, Group, Level, Patch, Renderer, Snippet, Title};

/// Render a single [`PositionalDiagnostic`] to a styled string.
///
/// Includes the primary annotated snippet, plain footers, and any
/// structured replacement suggestions as inline `did you mean` patches.
pub(crate) fn render(pd: &PositionalDiagnostic) -> String {
    let level = match pd.severity {
        Severity::Error => Level::ERROR,
        Severity::Warning => Level::WARNING,
    };
    let origin = origin_string(&pd.file);

    let mut groups: Vec<Group<'_>> = Vec::new();
    let primary_title: Title<'_> = level.primary_title(pd.message.as_str());
    let primary_group = if pd.source.is_empty() {
        Group::with_title(primary_title)
    } else {
        primary_title.element(
            Snippet::source(&pd.source)
                .path(origin.as_str())
                .annotation(AnnotationKind::Primary.span(clamped_range(pd))),
        )
    };
    groups.push(primary_group);

    for footer in &pd.footers {
        groups.push(Group::with_title(
            Level::HELP.secondary_title(footer.as_str()),
        ));
    }

    for s in &pd.suggestions {
        if s.alternatives.is_empty() {
            continue;
        }
        let mut group = Group::with_title(Level::HELP.secondary_title(s.label.as_str()));
        for alt in &s.alternatives {
            group = group.element(Snippet::source(&pd.source).path(origin.as_str()).patch(
                Patch::new(clamp(&pd.source, &alt.byte_range), alt.replacement.as_str()),
            ));
        }
        groups.push(group);
    }

    let renderer = if color_enabled() {
        Renderer::styled()
    } else {
        Renderer::plain()
    };
    renderer.render(&groups[..]).to_string()
}

/// Render `path` as a snippet origin, dropping redundant `./` components
/// so paths like `././models/foo.sql` print as `models/foo.sql`.
fn origin_string(path: &std::path::Path) -> String {
    let trimmed: std::path::PathBuf = path
        .components()
        .filter(|c| !matches!(c, std::path::Component::CurDir))
        .collect();
    if trimmed.as_os_str().is_empty() {
        path.display().to_string()
    } else {
        trimmed.display().to_string()
    }
}

/// Clamp the byte range to `[0, source.len()]` so an out-of-bounds offset
/// (e.g. a parser pos past EOF) doesn't panic inside annotate-snippets.
fn clamped_range(pd: &PositionalDiagnostic) -> std::ops::Range<usize> {
    clamp(&pd.source, &pd.byte_range)
}

fn clamp(source: &str, range: &std::ops::Range<usize>) -> std::ops::Range<usize> {
    let len = source.len();
    let start = range.start.min(len);
    let end = range.end.min(len).max(start);
    start..end
}

/// Extract any positional diagnostics carried by `error`.
///
/// Returns an empty `Vec` for errors that don't reference SQL source — those
/// fall back to plain [`std::fmt::Display`] rendering at the call site.
pub(crate) fn to_positional(error: &CliError) -> Vec<PositionalDiagnostic> {
    match error {
        CliError::Project(ProjectError::Parse(pe)) => parse_to_positional(pe),
        CliError::Project(ProjectError::Validation(ves)) => validation_to_positional(ves),
        CliError::TypeCheckFailed(tce) => typecheck_to_positional(tce),
        _ => Vec::new(),
    }
}

fn parse_to_positional(error: &ParseError) -> Vec<PositionalDiagnostic> {
    match error {
        ParseError::SqlParseFailed { path, sql, source } => vec![PositionalDiagnostic {
            severity: Severity::Error,
            file: path.clone(),
            source: sql.clone(),
            byte_range: source.error.pos..source.error.pos,
            message: source.error.message.clone(),
            footers: Vec::new(),
            suggestions: Vec::new(),
        }],
        ParseError::UnresolvedVariables(ve) => unresolved_variables_to_positional(ve),
        ParseError::StatementsParseFailed { .. } => Vec::new(),
    }
}

/// One [`PositionalDiagnostic`] per unresolved variable, pointed at its
/// reference in the source. The hint footer differs based on whether a
/// profile is active: with no profile, it directs the user to set one;
/// otherwise, it points at the profile's `[variables]` table.
fn unresolved_variables_to_positional(
    error: &crate::project::syntax::variables::VariableError,
) -> Vec<PositionalDiagnostic> {
    let source = std::fs::read_to_string(&error.path).unwrap_or_default();
    let footer = if error.profile_set {
        "define this variable in [<profile>.variables] in project.toml".to_string()
    } else {
        "no profile is selected; run `mz-deploy profile set <name>` and define \
         this variable in [<profile>.variables] in project.toml"
            .to_string()
    };
    error
        .unresolved
        .iter()
        .map(|uv| PositionalDiagnostic {
            severity: Severity::Error,
            file: error.path.clone(),
            source: source.clone(),
            byte_range: uv.byte_offset..(uv.byte_offset + uv.byte_len),
            message: format!("undefined variable ':{}'", uv.name),
            footers: vec![footer.clone()],
            suggestions: Vec::new(),
        })
        .collect()
}

fn validation_to_positional(errors: &ValidationErrors) -> Vec<PositionalDiagnostic> {
    errors
        .errors
        .iter()
        .map(validation_error_to_positional)
        .collect()
}

fn validation_error_to_positional(error: &ValidationError) -> PositionalDiagnostic {
    let file = error.context.file.clone();

    if let Ok(source) = std::fs::read_to_string(&file) {
        let offset = error.context.byte_offset.unwrap_or(0);
        let primary_range =
            crate::diagnostics::locate_validation(&error.kind, &source, Some(offset))
                .unwrap_or(offset..offset);
        let (message, footers, suggestions) =
            crate::diagnostics::format_validation_kind(&error.kind, &source, &primary_range);
        return PositionalDiagnostic {
            severity: Severity::Error,
            file,
            source,
            byte_range: primary_range,
            message,
            footers,
            suggestions,
        };
    }

    PositionalDiagnostic {
        severity: Severity::Error,
        file,
        source: error.context.sql_statement.clone().unwrap_or_default(),
        byte_range: 0..0,
        message: error.kind.message(),
        footers: error.kind.help().into_iter().collect(),
        suggestions: Vec::new(),
    }
}

fn typecheck_to_positional(error: &TypeCheckError) -> Vec<PositionalDiagnostic> {
    let errors: Vec<&ObjectTypeCheckError> = match error {
        TypeCheckError::Multiple(es) => es.iter().collect(),
        TypeCheckError::DatabaseSetupError(_)
        | TypeCheckError::SortError(_)
        | TypeCheckError::TypesCacheWriteFailed(_) => return Vec::new(),
    };

    errors
        .iter()
        .map(|e| object_typecheck_to_positional(e))
        .collect()
}

fn object_typecheck_to_positional(error: &ObjectTypeCheckError) -> PositionalDiagnostic {
    let source = std::fs::read_to_string(&error.file_path).unwrap_or_default();
    let primary_range = crate::diagnostics::locate_typecheck(&error.kind, &source).unwrap_or(0..0);

    let (message, footers, suggestions) =
        crate::diagnostics::format_typecheck_kind(&error.kind, &source, &primary_range);

    let mut full_message = message;
    if let Some(detail) = error.detail() {
        full_message.push_str("\ndetail: ");
        full_message.push_str(&detail);
    }

    PositionalDiagnostic {
        severity: Severity::Error,
        file: error.file_path.clone(),
        source,
        byte_range: primary_range,
        message: full_message,
        footers,
        suggestions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn pd(source: &str, range: std::ops::Range<usize>, message: &str) -> PositionalDiagnostic {
        PositionalDiagnostic {
            severity: Severity::Error,
            file: PathBuf::from("test.sql"),
            source: source.to_string(),
            byte_range: range,
            message: message.to_string(),
            footers: Vec::new(),
            suggestions: Vec::new(),
        }
    }

    #[mz_ore::test]
    fn render_includes_message_and_origin() {
        let out = render(&pd("SELECT bogus", 7..12, "unknown column"));
        assert!(out.contains("unknown column"));
        assert!(out.contains("test.sql"));
    }

    #[mz_ore::test]
    fn render_message_only_when_source_empty() {
        let out = render(&pd("", 0..0, "missing CREATE statement"));
        assert!(out.contains("missing CREATE statement"));
        // No snippet block → no origin pointer.
        assert!(!out.contains("test.sql"));
    }

    #[mz_ore::test]
    fn render_with_footer() {
        let mut diag = pd("SELECT 1", 7..8, "type mismatch");
        diag.footers.push("convert with CAST".to_string());
        let out = render(&diag);
        assert!(out.contains("type mismatch"));
        assert!(out.contains("convert with CAST"));
    }

    #[mz_ore::test]
    fn clamped_range_caps_at_source_len() {
        let diag = pd("abc", 100..200, "out of range");
        assert_eq!(clamped_range(&diag), 3..3);
    }

    #[mz_ore::test]
    fn clamped_range_preserves_in_bounds() {
        let diag = pd("abcdef", 1..4, "ok");
        assert_eq!(clamped_range(&diag), 1..4);
    }

    #[mz_ore::test]
    fn origin_string_strips_curdir() {
        assert_eq!(
            origin_string(std::path::Path::new("././models/app/foo.sql")),
            "models/app/foo.sql"
        );
    }

    #[mz_ore::test]
    fn origin_string_preserves_absolute() {
        assert_eq!(
            origin_string(std::path::Path::new("/abs/models/foo.sql")),
            "/abs/models/foo.sql"
        );
    }

    #[mz_ore::test]
    fn origin_string_preserves_bare_curdir() {
        assert_eq!(origin_string(std::path::Path::new(".")), ".");
    }
}
