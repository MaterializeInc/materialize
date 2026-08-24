// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared helpers for comment reconciliation across apply commands.
//!
//! Mirrors [`super::grants`]: read the comments recorded in the catalog, then
//! emit only the `COMMENT ON` statements that close the gap between them and
//! what the project declares. A comment the project no longer declares is
//! cleared with `COMMENT ON ... IS NULL`, so the project is the source of truth
//! for comments on the objects it manages.

use crate::cli::CliError;
use crate::cli::commands::reconcile::ReconcileTarget;
use crate::cli::executor::DeploymentExecutor;
use crate::client::ObjectComment;
use crate::info;
use mz_sql_parser::ast::{CommentObjectType, CommentStatement, Raw};
use owo_colors::{OwoColorize, Stream, Style};
use std::collections::BTreeMap;

/// What a comment attaches to within one object.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommentTarget {
    /// The object itself.
    Object,
    /// One of the object's columns, by name.
    ///
    /// Column names are compared verbatim: the parser has already folded
    /// unquoted identifiers to the casing the catalog stores.
    Column(String),
}

/// Reconcile comments on one object: set what differs, clear what the project
/// no longer declares.
pub async fn reconcile(
    executor: &DeploymentExecutor<'_>,
    object: &ReconcileTarget<'_>,
    comments: &[CommentStatement<Raw>],
    current: &[ObjectComment],
) -> Result<(), CliError> {
    let desired = desired_comments(comments);
    let changes = comment_changes(&desired, current, object);

    let dash_style = Style::new().red().bold();
    for stmt in &changes {
        if stmt.comment.is_none() && !executor.is_dry_run() {
            info!(
                "  {} Clearing stale comment on {} '{}'",
                "-".if_supports_color(Stream::Stderr, |t| dash_style.style(t)),
                object.kind().label(),
                object.display_name(),
            );
        }
        executor.execute_sql(stmt).await?;
    }
    Ok(())
}

/// Extract the comment text each target should carry.
///
/// An authored `COMMENT ON ... IS NULL` declares the absence of a comment, so it
/// is left out here and handled by the same path that clears stale comments.
/// When a project declares the same target twice, the last statement wins, which
/// matches the order the server would have applied them in.
pub fn desired_comments(comments: &[CommentStatement<Raw>]) -> BTreeMap<CommentTarget, String> {
    let mut desired = BTreeMap::new();
    for stmt in comments {
        let target = match &stmt.object {
            CommentObjectType::Column { name } => {
                CommentTarget::Column(name.column.as_str().to_string())
            }
            _ => CommentTarget::Object,
        };
        match &stmt.comment {
            Some(comment) => {
                desired.insert(target, comment.clone());
            }
            None => {
                desired.remove(&target);
            }
        }
    }
    desired
}

/// Compute the `COMMENT ON` statements that close the gap between `desired` and
/// `current`.
///
/// Emits a statement for every target whose text differs from what the catalog
/// holds, then clears every target the catalog holds that `desired` does not
/// declare.
pub fn comment_changes(
    desired: &BTreeMap<CommentTarget, String>,
    current: &[ObjectComment],
    object: &ReconcileTarget<'_>,
) -> Vec<CommentStatement<Raw>> {
    let stored: BTreeMap<CommentTarget, &str> = current
        .iter()
        .map(|c| {
            let target = match &c.column {
                Some(column) => CommentTarget::Column(column.clone()),
                None => CommentTarget::Object,
            };
            (target, c.comment.as_str())
        })
        .collect();

    let mut changes = Vec::new();
    for (target, comment) in desired {
        if stored.get(target).is_some_and(|held| *held == comment) {
            continue;
        }
        changes.push(object.comment_statement(target, Some(comment.clone())));
    }
    for target in stored.keys() {
        if !desired.contains_key(target) {
            changes.push(object.comment_statement(target, None));
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::commands::reconcile::ObjectKind;
    use crate::project::ir::object_id::ObjectId;
    use mz_sql_parser::ast::Statement;
    use mz_sql_parser::parser::parse_statements;

    /// Parse a COMMENT SQL string into a `CommentStatement`.
    fn parse_comment(sql: &str) -> CommentStatement<Raw> {
        let stmts = parse_statements(sql).unwrap();
        match stmts.into_iter().next().unwrap().ast {
            Statement::Comment(c) => c,
            other => panic!("expected COMMENT, got: {}", other),
        }
    }

    fn stored(column: Option<&str>, comment: &str) -> ObjectComment {
        ObjectComment {
            column: column.map(str::to_string),
            comment: comment.to_string(),
        }
    }

    fn obj_id() -> ObjectId {
        ObjectId::new("db".to_string(), "public".to_string(), "t".to_string())
    }

    fn changes(desired: &[CommentStatement<Raw>], current: &[ObjectComment]) -> Vec<String> {
        let id = obj_id();
        let object = ReconcileTarget::item(ObjectKind::Table, &id);
        comment_changes(&desired_comments(desired), current, &object)
            .iter()
            .map(|c| c.to_string())
            .collect()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_desired_comments_object_and_column() {
        let desired = desired_comments(&[
            parse_comment("COMMENT ON TABLE db.public.t IS 'the table'"),
            parse_comment("COMMENT ON COLUMN db.public.t.id IS 'the key'"),
        ]);
        assert_eq!(desired.len(), 2);
        assert_eq!(desired[&CommentTarget::Object], "the table");
        assert_eq!(desired[&CommentTarget::Column("id".to_string())], "the key");
    }

    /// An authored `IS NULL` declares absence, so it drops out of the desired
    /// set and is handled by the stale-clearing path instead.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_desired_comments_explicit_null_declares_absence() {
        let desired = desired_comments(&[
            parse_comment("COMMENT ON TABLE db.public.t IS 'the table'"),
            parse_comment("COMMENT ON TABLE db.public.t IS NULL"),
        ]);
        assert!(desired.is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_desired_comments_last_statement_wins() {
        let desired = desired_comments(&[
            parse_comment("COMMENT ON TABLE db.public.t IS 'first'"),
            parse_comment("COMMENT ON TABLE db.public.t IS 'second'"),
        ]);
        assert_eq!(desired[&CommentTarget::Object], "second");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_comment_changes_already_matching() {
        let desired = vec![
            parse_comment("COMMENT ON TABLE db.public.t IS 'the table'"),
            parse_comment("COMMENT ON COLUMN db.public.t.id IS 'the key'"),
        ];
        let current = vec![stored(None, "the table"), stored(Some("id"), "the key")];
        assert!(changes(&desired, &current).is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_comment_changes_missing_comment_is_set() {
        let desired = vec![parse_comment("COMMENT ON TABLE db.public.t IS 'the table'")];
        assert_eq!(
            changes(&desired, &[]),
            vec!["COMMENT ON TABLE db.public.t IS 'the table'"]
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_comment_changes_only_the_drifted_target() {
        let desired = vec![
            parse_comment("COMMENT ON TABLE db.public.t IS 'the table'"),
            parse_comment("COMMENT ON COLUMN db.public.t.id IS 'the new key'"),
        ];
        let current = vec![stored(None, "the table"), stored(Some("id"), "the old key")];
        assert_eq!(
            changes(&desired, &current),
            vec!["COMMENT ON COLUMN db.public.t.id IS 'the new key'"]
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_comment_changes_clears_stale_object_comment() {
        assert_eq!(
            changes(&[], &[stored(None, "left over")]),
            vec!["COMMENT ON TABLE db.public.t IS NULL"]
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_comment_changes_clears_stale_column_comment() {
        let desired = vec![parse_comment("COMMENT ON TABLE db.public.t IS 'the table'")];
        let current = vec![stored(None, "the table"), stored(Some("id"), "left over")];
        assert_eq!(
            changes(&desired, &current),
            vec!["COMMENT ON COLUMN db.public.t.id IS NULL"]
        );
    }

    /// Sets come before clears, and each group is ordered by target.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_comment_changes_sets_then_clears() {
        let desired = vec![parse_comment(
            "COMMENT ON COLUMN db.public.t.id IS 'the key'",
        )];
        let current = vec![stored(None, "left over")];
        assert_eq!(
            changes(&desired, &current),
            vec![
                "COMMENT ON COLUMN db.public.t.id IS 'the key'",
                "COMMENT ON TABLE db.public.t IS NULL",
            ]
        );
    }

    /// Named objects render their own keyword, not `TABLE`.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_comment_changes_named_object_keywords() {
        let desired = desired_comments(&[parse_comment("COMMENT ON CLUSTER c IS 'the cluster'")]);
        let cluster = ReconcileTarget::named(ObjectKind::Cluster, "c");
        let rendered: Vec<String> = comment_changes(&desired, &[], &cluster)
            .iter()
            .map(|c| c.to_string())
            .collect();
        assert_eq!(rendered, vec!["COMMENT ON CLUSTER c IS 'the cluster'"]);

        let cleared: Vec<String> = comment_changes(
            &BTreeMap::new(),
            &[stored(None, "left over")],
            &ReconcileTarget::schema("db", "public"),
        )
        .iter()
        .map(|c| c.to_string())
        .collect();
        assert_eq!(cleared, vec!["COMMENT ON SCHEMA db.public IS NULL"]);
    }
}
