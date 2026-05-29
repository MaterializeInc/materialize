// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Anonymizes SQL using Materialize's own parser.
//!
//! This is the SQL-rewriting half of `bin/mz-workload-anonymize`. Doing the
//! work on the parsed AST — rather than with text substitution — is what makes
//! it correct: identifiers are renamed as whole tokens (no substring or
//! in-string corruption, no word-boundary or case guesswork), and literals are
//! redacted in every position the dialect allows, including option values like
//! connection hosts and sink topics that the engine's redacted Display leaves
//! intact.
//!
//! Protocol: reads a JSON request on stdin and writes a JSON array on stdout,
//! one element per input statement — the rewritten SQL, or `null` when the
//! statement could not be parsed (the caller falls back to its regex for those).
//!
//! ```json
//! {
//!   "mapping": {"orders": "table_1", "auction_house": "db_0"},
//!   "rename_identifiers": true,
//!   "redact_literals": true,
//!   "statements": ["SELECT * FROM orders WHERE id = 7", "..."]
//! }
//! ```

use std::collections::BTreeMap;
use std::io::{self, Read, Write};

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    Ident, Raw, RawClusterName, RawDataType, RawItemName, RawNetworkPolicyName, Statement,
    UnresolvedDatabaseName, UnresolvedItemName, UnresolvedObjectName, UnresolvedSchemaName, Value,
};
use mz_sql_parser::parser;
use serde::Deserialize;

#[derive(Deserialize)]
struct Request {
    /// Original identifier -> anonymized identifier. Entries that map a name to
    /// itself (e.g. preserved keywords) are no-ops.
    #[serde(default)]
    mapping: BTreeMap<String, String>,
    #[serde(default)]
    rename_identifiers: bool,
    #[serde(default)]
    redact_literals: bool,
    statements: Vec<String>,
}

struct Anonymizer<'a> {
    mapping: &'a BTreeMap<String, String>,
    rename: bool,
    redact: bool,
}

impl Anonymizer<'_> {
    fn rename_ident(&self, ident: &mut Ident) {
        if self.rename {
            if let Some(new) = self.mapping.get(ident.as_str()) {
                *ident = Ident::new_unchecked(new.clone());
            }
        }
    }

    fn rename_idents(&self, idents: &mut [Ident]) {
        for ident in idents {
            self.rename_ident(ident);
        }
    }
}

impl<'ast> VisitMut<'ast, Raw> for Anonymizer<'_> {
    fn visit_ident_mut(&mut self, node: &'ast mut Ident) {
        self.rename_ident(node);
    }

    fn visit_value_mut(&mut self, node: &'ast mut Value) {
        if self.redact {
            match node {
                // Collapse every data-bearing literal to a single string
                // placeholder. Using a string (rather than redacting in place)
                // keeps the output reparseable — `x = '<REDACTED>'` is valid
                // even where `x` is numeric — and matches the `'<REDACTED>'`
                // sentinel the replay tooling already recognizes.
                Value::Number(_) | Value::String(_) | Value::HexString(_) | Value::Interval(_) => {
                    *node = Value::String("<REDACTED>".to_string());
                }
                // Booleans and NULL are not sensitive.
                Value::Boolean(_) | Value::Null => {}
            }
        }
    }

    // The `Raw` AstInfo associated types below have no-op default visitors (the
    // generic visitor cannot see into an associated type), yet they hold the
    // identifiers for object/cluster/type references. Override each to descend
    // to its `Ident`s so renaming reaches them.

    fn visit_item_name_mut(&mut self, node: &'ast mut RawItemName) {
        match node {
            RawItemName::Name(n) | RawItemName::Id(_, n, _) => self.rename_idents(&mut n.0),
        }
    }

    fn visit_unresolved_item_name_mut(&mut self, node: &'ast mut UnresolvedItemName) {
        self.rename_idents(&mut node.0);
    }

    fn visit_column_reference_mut(&mut self, node: &'ast mut Ident) {
        self.rename_ident(node);
    }

    fn visit_schema_name_mut(&mut self, node: &'ast mut UnresolvedSchemaName) {
        self.rename_idents(&mut node.0);
    }

    fn visit_database_name_mut(&mut self, node: &'ast mut UnresolvedDatabaseName) {
        self.rename_ident(&mut node.0);
    }

    fn visit_cluster_name_mut(&mut self, node: &'ast mut RawClusterName) {
        if let RawClusterName::Unresolved(ident) = node {
            self.rename_ident(ident);
        }
    }

    fn visit_network_policy_name_mut(&mut self, node: &'ast mut RawNetworkPolicyName) {
        if let RawNetworkPolicyName::Unresolved(ident) = node {
            self.rename_ident(ident);
        }
    }

    fn visit_data_type_mut(&mut self, node: &'ast mut RawDataType) {
        match node {
            RawDataType::Array(inner) | RawDataType::List(inner) => self.visit_data_type_mut(inner),
            RawDataType::Map {
                key_type,
                value_type,
            } => {
                self.visit_data_type_mut(key_type);
                self.visit_data_type_mut(value_type);
            }
            RawDataType::Other { name, .. } => self.visit_item_name_mut(name),
        }
    }

    fn visit_object_name_mut(&mut self, node: &'ast mut UnresolvedObjectName) {
        match node {
            UnresolvedObjectName::Cluster(i)
            | UnresolvedObjectName::Role(i)
            | UnresolvedObjectName::NetworkPolicy(i) => self.rename_ident(i),
            UnresolvedObjectName::Database(n) => self.rename_ident(&mut n.0),
            UnresolvedObjectName::Schema(n) => self.rename_idents(&mut n.0),
            UnresolvedObjectName::Item(n) => self.rename_idents(&mut n.0),
            UnresolvedObjectName::ClusterReplica(_) => visit_mut::visit_object_name_mut(self, node),
        }
    }
}

/// Statement kinds whose literals are non-sensitive configuration — cluster
/// sizing/replication and session/system settings (timeouts, isolation, feature
/// flags) — which replay needs preserved verbatim. Identifiers in these
/// statements are still renamed; only literal redaction is skipped.
fn preserves_literals(stmt: &Statement<Raw>) -> bool {
    matches!(
        stmt,
        Statement::CreateCluster(_)
            | Statement::CreateClusterReplica(_)
            | Statement::AlterCluster(_)
            | Statement::SetVariable(_)
            | Statement::ResetVariable(_)
            | Statement::SetTransaction(_)
            | Statement::AlterSystemSet(_)
            | Statement::AlterSystemReset(_)
    )
}

/// Anonymizes one workload entry (which may hold more than one statement),
/// returning `None` if it does not parse.
fn anonymize(
    sql: &str,
    mapping: &BTreeMap<String, String>,
    rename: bool,
    redact: bool,
) -> Option<String> {
    let stmts = parser::parse_statements(sql).ok()?;
    let rewritten: Vec<String> = stmts
        .into_iter()
        .map(|parsed| {
            let mut ast = parsed.ast;
            let mut visitor = Anonymizer {
                mapping,
                rename,
                redact: redact && !preserves_literals(&ast),
            };
            visitor.visit_statement_mut(&mut ast);
            ast.to_ast_string_simple()
        })
        .collect();
    Some(rewritten.join("; "))
}

fn main() -> io::Result<()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;

    let req: Request =
        serde_json::from_str(&input).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let result: Vec<Option<String>> = req
        .statements
        .iter()
        .map(|sql| {
            anonymize(
                sql,
                &req.mapping,
                req.rename_identifiers,
                req.redact_literals,
            )
        })
        .collect();

    let out = serde_json::to_string(&result)?;
    io::stdout().write_all(out.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::anonymize;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[mz_ore::test]
    fn renames_table_reference() {
        // The whole point of the AST approach: an object reference in FROM is an
        // item name (an AstInfo associated type), which the old regex mangled
        // and a naive visitor skips. It must be renamed as a whole token.
        let m = map(&[("orders", "table_1")]);
        let out = anonymize("SELECT id FROM orders", &m, true, false).expect("parses");
        assert!(out.contains("table_1"), "{out}");
        assert!(out.contains("id"), "id is unmapped, keep it: {out}");
        assert!(!out.contains("orders"), "{out}");
    }

    #[mz_ore::test]
    fn renames_qualified_reference() {
        let m = map(&[("mydb", "db_0"), ("myschema", "schema_1"), ("t", "table_1")]);
        let out = anonymize("SELECT * FROM mydb.myschema.t", &m, true, false).expect("parses");
        assert!(out.contains("db_0.schema_1.table_1"), "{out}");
    }

    #[mz_ore::test]
    fn does_not_rename_inside_other_identifiers() {
        // The old regex would rewrite `id` inside `user_id`; the AST does not.
        let m = map(&[("id", "column_1")]);
        let out = anonymize("SELECT user_id FROM t", &m, true, false).expect("parses");
        assert!(out.contains("user_id"), "{out}");
    }

    #[mz_ore::test]
    fn redacts_query_literals_including_numbers() {
        let m = map(&[]);
        let out =
            anonymize("SELECT 'secret', 42 FROM t WHERE x = 'a'", &m, false, true).expect("parses");
        assert!(!out.contains("secret"), "{out}");
        assert!(!out.contains("42"), "{out}");
        assert!(out.contains("<REDACTED>"), "{out}");
    }

    #[mz_ore::test]
    fn does_not_rename_inside_string_literals() {
        // A literal containing a word that matches a renamed identifier must not
        // be touched by renaming (it is data).
        let m = map(&[("orders", "table_1")]);
        let out = anonymize("SELECT 'orders' FROM orders", &m, true, false).expect("parses");
        assert!(out.contains("'orders'"), "{out}");
        assert!(out.contains("table_1"), "{out}");
    }

    #[mz_ore::test]
    fn preserves_cluster_config_literals() {
        // Cluster sizing is config replay needs; rename the name, keep the size.
        let m = map(&[("prod", "cluster_0")]);
        let out =
            anonymize("CREATE CLUSTER prod (SIZE = '100cc')", &m, true, true).expect("parses");
        assert!(out.contains("'100cc'"), "size must be preserved: {out}");
        assert!(out.contains("cluster_0"), "{out}");
    }

    #[mz_ore::test]
    fn preserves_set_config_literals() {
        let m = map(&[]);
        let out = anonymize("SET statement_timeout = '5s'", &m, true, true).expect("parses");
        assert!(out.contains("'5s'"), "timeout must be preserved: {out}");
    }

    #[mz_ore::test]
    fn returns_none_on_parse_error() {
        assert_eq!(anonymize("SELEC not valid", &map(&[]), true, true), None);
    }
}
