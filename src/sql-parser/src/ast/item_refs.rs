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
    CteBlock, Function, Ident, Query, Raw, RawDataType, RawItemName, Statement, UnresolvedItemName,
};

/// The catalog item references appearing in a statement.
///
/// User statements are normalized and have IDs in its relation and type references (e.g. `[u1 AS
/// db.schema.name]`). For references that don't have IDs, we create separate `named_*` buckets that
/// contain their qualified names.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ItemReferences {
    /// Catalog IDs from `RawItemName::Id` references
    pub ids: BTreeSet<String>,
    /// Named relations that don't carry an ID. Most commonly seen in builtin statements since they
    /// are not normalized. CTE names are excluded: a name bound by a WITH clause refers to the
    /// query-local binding rather than a catalog item, so recording it would fabricate an edge to
    /// any catalog object that happens to share the name.
    pub named_relations: BTreeSet<UnresolvedItemName>,
    pub named_funcs: BTreeSet<UnresolvedItemName>,
    pub named_types: BTreeSet<UnresolvedItemName>,
    /// Data-type references written `T[]`. These are
    /// reported separately from `named_types` since `T[]` maps to the named type `_T`.
    pub named_array_elements: BTreeSet<UnresolvedItemName>,
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
    /// Stack of CTE names currently in scope.
    cte_names: Vec<Ident>,
}

impl ReferenceCollector {
    fn record(&mut self, name: &RawItemName, position: Position) {
        match name {
            RawItemName::Id(id, _, _) => {
                self.refs.ids.insert(id.clone());
            }
            RawItemName::Name(name) => match position {
                Position::Relation => {
                    // Filter out CTEs.
                    if let [only] = &name.0[..] {
                        if self.cte_names.contains(only) {
                            return;
                        }
                    }
                    self.refs.named_relations.insert(name.clone());
                }
                Position::Func => {
                    self.refs.named_funcs.insert(name.clone());
                }
                Position::Type => {
                    self.refs.named_types.insert(name.clone());
                }
            },
        }
    }
}

enum Position {
    Relation,
    Func,
    Type,
}

impl<'ast> Visit<'ast, Raw> for ReferenceCollector {
    fn visit_item_name(&mut self, node: &'ast RawItemName) {
        self.record(node, Position::Relation);
    }

    fn visit_function(&mut self, node: &'ast Function<Raw>) {
        let Function {
            name,
            args,
            filter,
            over,
            distinct: _,
        } = node;
        self.record(name, Position::Func);
        self.visit_function_args(args);
        if let Some(filter) = filter {
            self.visit_expr(filter);
        }
        if let Some(over) = over {
            self.visit_window_spec(over);
        }
    }

    fn visit_data_type(&mut self, node: &'ast RawDataType) {
        match node {
            RawDataType::Array(elem_type) => match &**elem_type {
                // `T[]` references the array type paired with `T`, not `T`
                // itself. That pairing lives in `T`'s catalog details, which
                // the parser cannot read, so the element is reported on its
                // own field for the caller to map.
                RawDataType::Other {
                    name: RawItemName::Name(name),
                    typ_mod: _,
                } => {
                    self.refs.named_array_elements.insert(name.clone());
                }
                // `[<id> AS <name>][]`. The id in hand names the element, and
                // mapping it to the array type needs the catalog, so nothing
                // is reported rather than an edge to the wrong type. Stored
                // SQL never carries this spelling: name resolution prints a
                // resolved array type as a single id reference.
                RawDataType::Other {
                    name: RawItemName::Id(..),
                    typ_mod: _,
                } => {}
                // An array of a list, map, or array. Name resolution rejects
                // the first two and the parser collapses `T[][]` into a 1D
                // array, so none reach stored SQL. Recurse anyway so that no
                // reference is dropped unreported.
                elem_type => self.visit_data_type(elem_type),
            },
            RawDataType::List(elem_type) => self.visit_data_type(elem_type),
            RawDataType::Map {
                key_type,
                value_type,
            } => {
                self.visit_data_type(key_type);
                self.visit_data_type(value_type);
            }
            RawDataType::Other { name, typ_mod: _ } => self.record(name, Position::Type),
        }
    }

    fn visit_query(&mut self, node: &'ast Query<Raw>) {
        let Query {
            ctes,
            body,
            order_by,
            limit,
            offset,
        } = node;

        let scope_depth = self.cte_names.len();
        // Keep track of CTEs such that we don't record them
        // in `named_relations`
        match ctes {
            CteBlock::Simple(ctes) => {
                for cte in ctes {
                    self.visit_cte(cte);
                    self.cte_names.push(cte.alias.name.clone());
                }
            }
            CteBlock::MutuallyRecursive(block) => {
                for cte in &block.ctes {
                    self.cte_names.push(cte.name.clone());
                }
                self.visit_mut_rec_block(block);
            }
        }
        self.visit_set_expr(body);
        for order_by in order_by {
            self.visit_order_by_expr(order_by);
        }
        if let Some(limit) = limit {
            self.visit_limit(limit);
        }
        if let Some(offset) = offset {
            self.visit_expr(offset);
        }
        // Pop CTEs once visited.
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

    fn names(parts: &[&[&str]]) -> BTreeSet<UnresolvedItemName> {
        parts
            .iter()
            .map(|name| UnresolvedItemName(name.iter().map(|p| Ident::new_unchecked(*p)).collect()))
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
        assert!(refs.named_relations.is_empty());
        assert!(refs.named_types.is_empty());
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
            refs.named_funcs,
            names(&[&["pg_catalog", "abs"], &["count"]])
        );
        assert_eq!(refs.named_relations, names(&[&["mz_catalog", "mz_tables"]]));
    }

    #[mz_ore::test]
    fn function_args_still_visited() {
        let refs = collect(
            r#"CREATE VIEW v AS SELECT "pg_catalog"."abs"(a::"pg_catalog"."int8")
               FROM "mz_catalog"."mz_tables""#,
        );
        assert_eq!(refs.named_funcs, names(&[&["pg_catalog", "abs"]]));
        assert_eq!(refs.named_types, names(&[&["pg_catalog", "int8"]]));
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
            refs.named_types,
            names(&[&["pg_catalog", "text"], &["pg_catalog", "int8"]])
        );
        assert_eq!(refs.named_array_elements, names(&[&["pg_catalog", "int4"]]));
    }

    #[mz_ore::test]
    fn array_nested_in_list() {
        // A list of arrays still references the array type.
        let refs = collect(r#"CREATE TABLE t (a "pg_catalog"."int4"[] list)"#);
        assert_eq!(refs.named_array_elements, names(&[&["pg_catalog", "int4"]]));
        assert!(refs.named_types.is_empty());
    }

    #[mz_ore::test]
    fn array_of_id_reference_reports_nothing() {
        // Mapping the element id to its array type needs the catalog, so
        // nothing is reported rather than an edge to the element.
        let refs = collect(r#"CREATE TABLE t (a [s20 AS "pg_catalog"."int4"][])"#);
        assert!(refs.named_array_elements.is_empty());
        assert!(refs.named_types.is_empty());
        assert!(refs.ids.is_empty());
    }

    #[mz_ore::test]
    fn array_of_list_still_visits_the_inner_type() {
        // Name resolution rejects `T list[]`, but the collector must not drop
        // the inner reference unreported.
        let refs = collect(r#"CREATE TABLE t (a "pg_catalog"."int4" list[])"#);
        assert_eq!(refs.named_types, names(&[&["pg_catalog", "int4"]]));
        assert!(refs.named_array_elements.is_empty());
    }

    #[mz_ore::test]
    fn cte_names_excluded() {
        let refs = collect(
            r#"CREATE VIEW v AS
               WITH c AS (SELECT * FROM "mz_catalog"."mz_tables")
               SELECT * FROM c JOIN "mz_catalog"."mz_views" ON true"#,
        );
        assert_eq!(
            refs.named_relations,
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
        assert_eq!(refs.named_relations, names(&[&["mz_catalog", "mz_tables"]]));
        // The int column type on the WMR CTE is still a type reference.
        assert_eq!(refs.named_types, names(&[&["int4"]]));
    }

    #[mz_ore::test]
    fn qualified_cte_lookalike_not_excluded() {
        // Only 1-part references can refer to a CTE.
        let refs = collect(
            r#"CREATE VIEW v AS
               WITH mz_tables AS (SELECT 1 AS a)
               SELECT * FROM mz_tables JOIN "mz_catalog"."mz_tables" ON true"#,
        );
        assert_eq!(refs.named_relations, names(&[&["mz_catalog", "mz_tables"]]));
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
        assert_eq!(refs.named_relations, names(&[&["tbl"]]));
    }

    #[mz_ore::test]
    fn simple_cte_definition_does_not_see_itself() {
        // Mirroring the name resolver: a plain WITH binds a CTE's name only
        // after its own definition, so the inner `tbl` is a real relation.
        let refs = collect(
            r#"CREATE VIEW v AS
               WITH tbl AS (SELECT * FROM tbl) SELECT * FROM tbl"#,
        );
        assert_eq!(refs.named_relations, names(&[&["tbl"]]));
    }

    #[mz_ore::test]
    fn later_simple_cte_sees_earlier() {
        let refs = collect(
            r#"CREATE VIEW v AS
               WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a)
               SELECT * FROM b"#,
        );
        assert!(refs.named_relations.is_empty());
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
        assert_eq!(refs.named_funcs, names(&[&["pg_catalog", "abs"]]));
    }
}
