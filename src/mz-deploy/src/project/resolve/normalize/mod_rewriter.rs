// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AST-based rewriting of database and schema names in mod statements.
//!
//! When a profile suffix is active, database names in mod files must be
//! suffixed (e.g., `app` -> `app_dev`). When a staging suffix is active,
//! schema names must be suffixed (e.g., `public` -> `public_staging`).
//!
//! This module provides [`rewrite_database_names`] and [`rewrite_schema_names`],
//! which apply these transformations at the AST level using the auto-generated
//! [`VisitMut`] traversal. This is safer than raw text substitution because it
//! only touches actual identifier nodes, not string literals or comments.
//!
//! ## Supported Statement Types
//!
//! The visitor handles all statement types permitted in mod files:
//! - `COMMENT ON DATABASE/SCHEMA`
//! - `GRANT ... ON DATABASE/SCHEMA`
//! - `ALTER DEFAULT PRIVILEGES IN DATABASE/SCHEMA`
//!
//! The auto-generated traversal uses two separate hooks for database/schema
//! names depending on AST position:
//!
//! - `visit_database_name_mut` / `visit_schema_name_mut` — associated type
//!   hooks used by COMMENT ON DATABASE/SCHEMA and ALTER DEFAULT PRIVILEGES
//! - `visit_unresolved_database_name_mut` / `visit_unresolved_schema_name_mut`
//!   — concrete struct hooks used by GRANT ON DATABASE/SCHEMA (via
//!   `UnresolvedObjectName::Database/Schema`)
//!
//! Both hooks receive `&mut UnresolvedDatabaseName` / `&mut UnresolvedSchemaName`,
//! so the rewriting logic is shared via a helper function. Both hooks must be
//! overridden to cover all mod statement types.

use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    Ident, Raw, Statement, UnresolvedDatabaseName, UnresolvedObjectName, UnresolvedSchemaName,
};

/// Append `suffix` to the database name if it matches `database_name`.
fn rewrite_database_name(node: &mut UnresolvedDatabaseName, database_name: &str, suffix: &str) {
    if node.0.as_str() == database_name {
        node.0 =
            Ident::new(format!("{}{}", database_name, suffix)).expect("valid database identifier");
    }
}

/// Append `suffix` to the schema part (last ident) if it matches `schema_name`.
fn rewrite_schema_name(node: &mut UnresolvedSchemaName, schema_name: &str, suffix: &str) {
    if let Some(last) = node.0.last() {
        if last.as_str() == schema_name {
            let idx = node.0.len() - 1;
            node.0[idx] =
                Ident::new(format!("{}{}", schema_name, suffix)).expect("valid schema identifier");
        }
    }
}

/// Visitor that rewrites database name identifiers by appending a suffix.
///
/// Overrides both `visit_database_name_mut` (associated type hook, used by
/// COMMENT and ALTER DEFAULT PRIVILEGES) and `visit_unresolved_database_name_mut`
/// (concrete struct hook, used by GRANT via `UnresolvedObjectName::Database`).
struct DatabaseNameRewriter<'a> {
    database_name: &'a str,
    suffix: &'a str,
}

impl<'a> VisitMut<'_, Raw> for DatabaseNameRewriter<'a> {
    fn visit_database_name_mut(&mut self, node: &mut UnresolvedDatabaseName) {
        rewrite_database_name(node, self.database_name, self.suffix);
    }

    fn visit_object_name_mut(&mut self, node: &mut UnresolvedObjectName) {
        if let UnresolvedObjectName::Database(db_name) = node {
            rewrite_database_name(db_name, self.database_name, self.suffix);
        }
    }

    fn visit_unresolved_database_name_mut(&mut self, node: &mut UnresolvedDatabaseName) {
        rewrite_database_name(node, self.database_name, self.suffix);
    }
}

/// Visitor that rewrites schema name identifiers by appending a suffix.
///
/// Overrides both `visit_schema_name_mut` (associated type hook, used by
/// COMMENT and ALTER DEFAULT PRIVILEGES) and `visit_unresolved_schema_name_mut`
/// (concrete struct hook, used by GRANT via `UnresolvedObjectName::Schema`).
struct SchemaNameRewriter<'a> {
    schema_name: &'a str,
    suffix: &'a str,
}

impl<'a> VisitMut<'_, Raw> for SchemaNameRewriter<'a> {
    fn visit_object_name_mut(&mut self, node: &mut UnresolvedObjectName) {
        if let UnresolvedObjectName::Schema(schema_name) = node {
            rewrite_schema_name(schema_name, self.schema_name, self.suffix);
        }
    }

    fn visit_schema_name_mut(&mut self, node: &mut UnresolvedSchemaName) {
        rewrite_schema_name(node, self.schema_name, self.suffix);
    }

    fn visit_unresolved_schema_name_mut(&mut self, node: &mut UnresolvedSchemaName) {
        rewrite_schema_name(node, self.schema_name, self.suffix);
    }
}

/// Rewrite database names in parsed mod statements by appending a suffix.
///
/// Applies suffix-based renaming to all [`UnresolvedDatabaseName`] nodes
/// in the given statements using the auto-generated [`VisitMut`] traversal.
/// Only identifiers that exactly match `database_name` are rewritten;
/// string literals, comments, and other identifiers are untouched.
///
/// # Arguments
/// * `statements` - Parsed mod statements to rewrite (mutated in place)
/// * `database_name` - The original database name to match
/// * `suffix` - The suffix to append (e.g., `"_dev"`)
pub(crate) fn rewrite_database_names(
    statements: &mut [Statement<Raw>],
    database_name: &str,
    suffix: &str,
) {
    let mut rewriter = DatabaseNameRewriter {
        database_name,
        suffix,
    };
    for stmt in statements {
        visit_mut::visit_statement_mut(&mut rewriter, stmt);
    }
}

/// Rewrite schema names in parsed mod statements by appending a suffix.
///
/// Applies suffix-based renaming to all [`UnresolvedSchemaName`] nodes
/// in the given statements using the auto-generated [`VisitMut`] traversal.
/// Matches the last identifier in each schema name (the schema part),
/// so both `schema` and `db.schema` forms are handled.
///
/// # Arguments
/// * `statements` - Parsed mod statements to rewrite (mutated in place)
/// * `schema_name` - The original schema name to match
/// * `suffix` - The suffix to append (e.g., `"_staging"`)
pub(crate) fn rewrite_schema_names(
    statements: &mut [Statement<Raw>],
    schema_name: &str,
    suffix: &str,
) {
    let mut rewriter = SchemaNameRewriter {
        schema_name,
        suffix,
    };
    for stmt in statements {
        visit_mut::visit_statement_mut(&mut rewriter, stmt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_sql_parser::parser::parse_statements;

    fn parse_one(sql: &str) -> Statement<Raw> {
        let stmts = parse_statements(sql).expect("valid SQL");
        assert_eq!(stmts.len(), 1, "expected exactly one statement");
        stmts.into_iter().next().unwrap().ast
    }

    fn rewrite_db(sql: &str, db_name: &str, suffix: &str) -> String {
        let mut stmts = vec![parse_one(sql)];
        rewrite_database_names(&mut stmts, db_name, suffix);
        format!("{}", stmts[0])
    }

    fn rewrite_schema(sql: &str, schema_name: &str, suffix: &str) -> String {
        let mut stmts = vec![parse_one(sql)];
        rewrite_schema_names(&mut stmts, schema_name, suffix);
        format!("{}", stmts[0])
    }

    // --- Database name rewriting ---

    #[test]
    fn test_comment_on_database() {
        let result = rewrite_db(
            "COMMENT ON DATABASE app IS 'app description'",
            "app",
            "_dev",
        );
        assert!(
            result.contains("app_dev"),
            "database name should be rewritten: {result}"
        );
        assert!(
            result.contains("app description"),
            "string literal should be untouched: {result}"
        );
    }

    #[test]
    fn test_grant_on_database() {
        let result = rewrite_db("GRANT ALL ON DATABASE app TO role1", "app", "_dev");
        assert!(
            result.contains("app_dev"),
            "database name should be rewritten: {result}"
        );
    }

    #[test]
    fn test_alter_default_privileges_database() {
        let result = rewrite_db(
            "ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN DATABASE app GRANT SELECT ON TABLES TO role1",
            "app",
            "_dev",
        );
        assert!(
            result.contains("app_dev"),
            "database name should be rewritten: {result}"
        );
    }

    #[test]
    fn test_database_no_match_passthrough() {
        let original = "COMMENT ON DATABASE other IS 'untouched'";
        let result = rewrite_db(original, "app", "_dev");
        assert!(
            !result.contains("_dev"),
            "non-matching database should be untouched: {result}"
        );
    }

    // --- Schema name rewriting ---

    #[test]
    fn test_comment_on_schema_qualified() {
        let result = rewrite_schema(
            "COMMENT ON SCHEMA app.public IS 'schema description'",
            "public",
            "_staging",
        );
        assert!(
            result.contains("public_staging"),
            "schema name should be rewritten: {result}"
        );
        assert!(
            result.contains("schema description"),
            "string literal should be untouched: {result}"
        );
    }

    #[test]
    fn test_grant_on_schema() {
        let result = rewrite_schema(
            "GRANT USAGE ON SCHEMA app.public TO role1",
            "public",
            "_staging",
        );
        assert!(
            result.contains("public_staging"),
            "schema name should be rewritten: {result}"
        );
    }

    #[test]
    fn test_alter_default_privileges_schema() {
        let result = rewrite_schema(
            "ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA app.public GRANT SELECT ON TABLES TO role1",
            "public",
            "_staging",
        );
        assert!(
            result.contains("public_staging"),
            "schema name should be rewritten: {result}"
        );
    }

    #[test]
    fn test_schema_no_match_passthrough() {
        let original = "COMMENT ON SCHEMA app.other IS 'untouched'";
        let result = rewrite_schema(original, "public", "_staging");
        assert!(
            !result.contains("_staging"),
            "non-matching schema should be untouched: {result}"
        );
    }
}
