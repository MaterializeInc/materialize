// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software shall be governed
// by the Apache License, Version 2.0.

//! Extraction of catalog item references from a raw SQL AST.

use std::collections::BTreeSet;

use crate::ast::visit::Visit;
use crate::ast::{
    CteBlock, Function, Query, Raw, RawDataType, RawItemName, Statement, UnresolvedItemName,
};

/// The catalog item references appearing in a statement, bucketed by how they
/// are named.
///
/// References that carry a catalog id (`[u1 AS db.schema.name]`) land in
/// `ids` regardless of position. Name-only references are bucketed by
/// syntactic position, because position determines the catalog namespace they
/// resolve in: function names resolve to functions, type names to types, and
/// everything else to relations. Type position splits further, because `T[]`
/// and a bare `T` resolve to different catalog items.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ItemReferences {
    /// Catalog ids from `RawItemName::Id` references, in any position.
    pub ids: BTreeSet<String>,
    /// Name-only references in relation position. References that resolve to
    /// a CTE are excluded, using the same lexical scoping rules as name
    /// resolution: a plain `WITH` binds each CTE name only after its own
    /// definition, `WITH MUTUALLY RECURSIVE` binds all names up front, and
    /// bindings are visible in the query body and any nested scopes.
    pub relation_names: BTreeSet<Vec<String>>,
    /// Name-only references in function-call position.
    pub func_names: BTreeSet<Vec<String>>,
    /// Name-only references in data-type position written without `[]`.
    pub type_names: BTreeSet<Vec<String>>,
    /// Element names of data-type references written `T[]`.
    ///
    /// Such a reference resolves to the array type paired with `T`, not to `T`
    /// itself, so these are reported apart from `type_names` rather than as a
    /// subset of it. Mapping an element name to its array type requires the
    /// catalog, which the parser does not have, so the name is reported as
    /// written.
    pub array_element_names: BTreeSet<Vec<String>>,
}

/// Collects all catalog item references from a statement.
pub fn collect_item_references(stmt: &Statement<Raw>) -> ItemReferences {
    let mut collector = ReferenceCollector {
        refs: ItemReferences::default(),
        cte_names: Vec::new(),
    };
    collector.visit_statement(stmt);
    collector.refs
}

struct ReferenceCollector {
    refs: ItemReferences,
    /// Stack of CTE names currently in scope. Entries may repeat when an
    /// inner scope shadows an outer binding.
    cte_names: Vec<String>,
}

impl ReferenceCollector {
    fn record(&mut self, name: &RawItemName, position: Position) {
        match name {
            RawItemName::Id(id, _, _) => {
                self.refs.ids.insert(id.clone());
            }
            RawItemName::Name(UnresolvedItemName(parts)) => {
                let parts: Vec<String> = parts.iter().map(|p| p.as_str().to_string()).collect();
                match position {
                    Position::Relation => {
                        // Only 1-part names can refer to a CTE.
                        if let [only] = &parts[..] {
                            if self.cte_names.iter().any(|c| c == only) {
                                return;
                            }
                        }
                        self.refs.relation_names.insert(parts);
                    }
                    Position::Func => {
                        self.refs.func_names.insert(parts);
                    }
                    Position::Type { in_array } => {
                        if in_array {
                            self.refs.array_element_names.insert(parts);
                        } else {
                            self.refs.type_names.insert(parts);
                        }
                    }
                }
            }
        }
    }

    fn visit_raw_data_type(&mut self, node: &RawDataType, in_array: bool) {
        match node {
            RawDataType::Array(ty) => self.visit_raw_data_type(ty, true),
            RawDataType::List(ty) => self.visit_raw_data_type(ty, false),
            RawDataType::Map {
                key_type,
                value_type,
            } => {
                self.visit_raw_data_type(key_type, false);
                self.visit_raw_data_type(value_type, false);
            }
            RawDataType::Other { name, typ_mod: _ } => {
                self.record(name, Position::Type { in_array })
            }
        }
    }
}

enum Position {
    Relation,
    Func,
    Type { in_array: bool },
}

impl<'ast> Visit<'ast, Raw> for ReferenceCollector {
    fn visit_item_name(&mut self, node: &'ast RawItemName) {
        self.record(node, Position::Relation);
    }

    fn visit_function(&mut self, node: &'ast Function<Raw>) {
        self.record(&node.name, Position::Func);
        // Visit everything `visit::visit_function` visits except the name,
        // which would otherwise reach `visit_item_name` and be misclassified
        // as a relation reference.
        self.visit_function_args(&node.args);
        if let Some(filter) = &node.filter {
            self.visit_expr(filter);
        }
        if let Some(over) = &node.over {
            self.visit_window_spec(over);
        }
    }

    fn visit_data_type(&mut self, node: &'ast RawDataType) {
        // The generated `visit_data_type` is an empty leaf hook, so recurse
        // through the nested type structure by hand.
        self.visit_raw_data_type(node, false);
    }

    fn visit_query(&mut self, node: &'ast Query<Raw>) {
        // Track CTE bindings with the same scoping rules as the name
        // resolver's `fold_query`, so that CTE references are excluded from
        // `relation_names` exactly when resolution would resolve them to a
        // CTE rather than a catalog item.
        let scope_depth = self.cte_names.len();
        match &node.ctes {
            CteBlock::Simple(ctes) => {
                // Each CTE's definition sees only the CTEs bound before it.
                for cte in ctes {
                    self.visit_cte(cte);
                    self.cte_names.push(cte.alias.name.as_str().to_string());
                }
            }
            CteBlock::MutuallyRecursive(block) => {
                // All bindings go into scope before any definition is
                // walked, so that the definitions can refer to each other.
                for cte in &block.ctes {
                    self.cte_names.push(cte.name.as_str().to_string());
                }
                self.visit_mut_rec_block(block);
            }
        }
        // The remaining fields of `visit::visit_query`, with the CTE
        // bindings in scope.
        self.visit_set_expr(&node.body);
        for order_by in &node.order_by {
            self.visit_order_by_expr(order_by);
        }
        if let Some(limit) = &node.limit {
            self.visit_limit(limit);
        }
        if let Some(offset) = &node.offset {
            self.visit_expr(offset);
        }
        self.cte_names.truncate(scope_depth);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_statements;

    #[track_caller]
    fn collect(sql: &str) -> ItemReferences {
        let stmts = parse_statements(sql).expect("valid sql");
        assert_eq!(stmts.len(), 1, "expected a single statement");
        collect_item_references(&stmts.into_iter().next().unwrap().ast)
    }

    fn names(parts: &[&[&str]]) -> BTreeSet<Vec<String>> {
        parts
            .iter()
            .map(|name| name.iter().map(|p| p.to_string()).collect())
            .collect()
    }

    fn strings(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[mz_ore::test]
    fn ids_from_bracketed_references() {
        let refs = collect(
            r#"CREATE VIEW v AS
               SELECT a::[s20 AS "pg_catalog"."int4"] FROM [u1 AS "materialize"."public"."t"]"#,
        );
        assert_eq!(refs.ids, strings(&["s20", "u1"]));
        assert!(refs.relation_names.is_empty());
        assert!(refs.type_names.is_empty());
    }

    #[mz_ore::test]
    fn versioned_id_reference() {
        let refs = collect(
            r#"CREATE VIEW v AS SELECT * FROM [u3 AS "materialize"."public"."t" VERSION 2]"#,
        );
        assert_eq!(refs.ids, strings(&["u3"]));
    }

    #[mz_ore::test]
    fn function_names_classified() {
        let refs = collect(
            r#"CREATE VIEW v AS SELECT "pg_catalog"."abs"(1), count(*) FROM "mz_catalog"."mz_tables""#,
        );
        assert_eq!(
            refs.func_names,
            names(&[&["pg_catalog", "abs"], &["count"]])
        );
        assert_eq!(refs.relation_names, names(&[&["mz_catalog", "mz_tables"]]));
    }

    #[mz_ore::test]
    fn function_args_still_visited() {
        let refs = collect(
            r#"CREATE VIEW v AS SELECT "pg_catalog"."abs"(a::"pg_catalog"."int8")
               FROM "mz_catalog"."mz_tables""#,
        );
        assert_eq!(refs.func_names, names(&[&["pg_catalog", "abs"]]));
        assert_eq!(refs.type_names, names(&[&["pg_catalog", "int8"]]));
    }

    #[mz_ore::test]
    fn nested_data_types() {
        let refs = collect(
            r#"CREATE TABLE t (a "pg_catalog"."int4"[], b "pg_catalog"."text" list,
               c map["pg_catalog"."text" => "pg_catalog"."int8" list])"#,
        );
        // int4 sits under `[]`, so it is reported as an array element rather
        // than as a type reference in its own right.
        assert_eq!(
            refs.type_names,
            names(&[&["pg_catalog", "text"], &["pg_catalog", "int8"]])
        );
        assert_eq!(refs.array_element_names, names(&[&["pg_catalog", "int4"]]));
    }

    #[mz_ore::test]
    fn cte_names_excluded() {
        let refs = collect(
            r#"CREATE VIEW v AS
               WITH c AS (SELECT * FROM "mz_catalog"."mz_tables")
               SELECT * FROM c JOIN "mz_catalog"."mz_views" ON true"#,
        );
        assert_eq!(
            refs.relation_names,
            names(&[&["mz_catalog", "mz_tables"], &["mz_catalog", "mz_views"]])
        );
    }

    #[mz_ore::test]
    fn mut_rec_cte_names_excluded() {
        let refs = collect(
            r#"CREATE VIEW v AS
               WITH MUTUALLY RECURSIVE
                   reach (a int) AS (SELECT a FROM "mz_catalog"."mz_tables", reach)
               SELECT * FROM reach"#,
        );
        assert_eq!(refs.relation_names, names(&[&["mz_catalog", "mz_tables"]]));
        // The int column type on the WMR CTE is still a type reference.
        assert_eq!(refs.type_names, names(&[&["int4"]]));
    }

    #[mz_ore::test]
    fn qualified_cte_lookalike_not_excluded() {
        // Only 1-part references can refer to a CTE.
        let refs = collect(
            r#"CREATE VIEW v AS
               WITH mz_tables AS (SELECT 1 AS a)
               SELECT * FROM mz_tables JOIN "mz_catalog"."mz_tables" ON true"#,
        );
        assert_eq!(refs.relation_names, names(&[&["mz_catalog", "mz_tables"]]));
    }

    #[mz_ore::test]
    fn cte_scoping_is_lexical() {
        // The CTE binding is scoped to the subquery that declares it: the
        // same name in a sibling scope is a real relation reference.
        let refs = collect(
            r#"CREATE VIEW v AS
               SELECT * FROM (WITH tbl AS (SELECT 1 AS a) SELECT * FROM tbl) x
               JOIN tbl ON true"#,
        );
        assert_eq!(refs.relation_names, names(&[&["tbl"]]));
    }

    #[mz_ore::test]
    fn simple_cte_definition_does_not_see_itself() {
        // Mirroring the name resolver: a plain WITH binds a CTE's name only
        // after its own definition, so the inner `tbl` is a real relation.
        let refs = collect(
            r#"CREATE VIEW v AS
               WITH tbl AS (SELECT * FROM tbl) SELECT * FROM tbl"#,
        );
        assert_eq!(refs.relation_names, names(&[&["tbl"]]));
    }

    #[mz_ore::test]
    fn later_simple_cte_sees_earlier() {
        let refs = collect(
            r#"CREATE VIEW v AS
               WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a)
               SELECT * FROM b"#,
        );
        assert!(refs.relation_names.is_empty());
    }

    #[mz_ore::test]
    fn connection_and_secret_references() {
        let refs = collect(
            r#"CREATE CONNECTION kc TO KAFKA (
                BROKER 'kafka:9092',
                SSH TUNNEL [u5 AS "materialize"."public"."ssh"],
                SASL PASSWORD = SECRET [u7 AS "materialize"."public"."pw"]
            )"#,
        );
        assert_eq!(refs.ids, strings(&["u5", "u7"]));
    }

    #[mz_ore::test]
    fn index_references() {
        let refs = collect(
            r#"CREATE INDEX i IN CLUSTER [u1] ON [u2 AS "materialize"."public"."t"] ("pg_catalog"."abs"(a))"#,
        );
        assert_eq!(refs.ids, strings(&["u2"]));
        assert_eq!(refs.func_names, names(&[&["pg_catalog", "abs"]]));
    }
}
