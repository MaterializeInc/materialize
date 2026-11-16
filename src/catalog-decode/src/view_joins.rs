// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The joins and passed-through columns of a view definition, which
//! `mz_internal.mz_inferred_relationships` reads relationships from.
//!
//! The input is a persisted `create_sql`, whose relation references are
//! resolved to catalog ids (`[u4 AS "db"."schema"."name"]`). A column is
//! attributed to a relation only when its qualifier is bound to exactly that
//! relation in an enclosing scope. Any other binding, such as a CTE, a
//! subquery, a table function, or an alias that renames columns, is opaque,
//! so a column read through it is never attributed. This errs toward missing
//! a relationship over reporting a wrong one.

use std::collections::BTreeMap;

use mz_sql_parser::ast::{
    CteBlock, Expr, Ident, JoinConstraint, JoinOperator, Query, Raw, RawItemName, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use serde_json::json;

/// A column of a catalog relation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ColumnRef {
    id: String,
    column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Binding {
    Relation(String),
    /// Bound to something that is not a catalog relation's columns.
    Opaque,
}

#[derive(Default)]
struct Extracted {
    joins: Vec<(ColumnRef, ColumnRef)>,
    usings: Vec<(String, String, Vec<String>)>,
    outputs: Vec<(String, ColumnRef)>,
}

/// The scopes enclosing the expression being read, innermost last.
type Scopes = Vec<BTreeMap<String, Binding>>;

/// Returns, as JSON, the column pairs the view equates (`joins`), its
/// `JOIN ... USING` clauses with the relations joined before them
/// (`using`), and its output columns that are input columns unchanged
/// (`outputs`). Statements other than views and materialized views yield
/// empty lists.
pub fn view_joins(sql: &str) -> Result<serde_json::Value, String> {
    let stmt = crate::create_sql::parse_single_statement(sql)?;
    let (query, renames) = match &stmt {
        Statement::CreateView(view) => (&view.definition.query, &view.definition.columns),
        Statement::CreateMaterializedView(mv) => (&mv.query, &mv.columns),
        _ => return Ok(json!({"joins": [], "using": [], "outputs": []})),
    };
    let mut out = Extracted::default();
    let mut scopes = Scopes::new();
    walk_query(query, &mut scopes, &mut out, true);

    // `CREATE VIEW v (a, b) AS ...` renames the outputs by position.
    if !renames.is_empty() {
        out.outputs = out
            .outputs
            .into_iter()
            .enumerate()
            .filter_map(|(i, (_, source))| {
                renames
                    .get(i)
                    .map(|name| (name.as_str().to_string(), source))
            })
            .collect();
    }
    out.outputs.retain(|(_, source)| !source.id.is_empty());

    Ok(json!({
        "joins": out.joins.iter().map(|(l, r)| json!({
            "left_id": l.id, "left_column": l.column,
            "right_id": r.id, "right_column": r.column,
        })).collect::<Vec<_>>(),
        "using": out.usings.iter().map(|(right, column, candidates)| json!({
            "right_id": right, "column": column, "candidates": candidates,
        })).collect::<Vec<_>>(),
        "outputs": out.outputs.iter().map(|(column, source)| json!({
            "column": column, "source_id": source.id, "source_column": source.column,
        })).collect::<Vec<_>>(),
    }))
}

fn walk_query(query: &Query<Raw>, scopes: &mut Scopes, out: &mut Extracted, is_top: bool) {
    match &query.ctes {
        CteBlock::Simple(ctes) => {
            for cte in ctes {
                walk_query(&cte.query, scopes, out, false);
            }
        }
        CteBlock::MutuallyRecursive(block) => {
            for cte in &block.ctes {
                walk_query(&cte.query, scopes, out, false);
            }
        }
    }
    walk_set_expr(&query.body, scopes, out, is_top);
}

fn walk_set_expr(expr: &SetExpr<Raw>, scopes: &mut Scopes, out: &mut Extracted, is_top: bool) {
    match expr {
        SetExpr::Select(select) => walk_select(select, scopes, out, is_top),
        SetExpr::Query(query) => walk_query(query, scopes, out, is_top),
        // Outputs mix several SELECT lists, so none is a single input column.
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left, scopes, out, false);
            walk_set_expr(right, scopes, out, false);
        }
        SetExpr::Values(_) | SetExpr::Show(_) | SetExpr::Table(_) => {}
    }
}

fn walk_select(select: &Select<Raw>, scopes: &mut Scopes, out: &mut Extracted, is_top: bool) {
    scopes.push(BTreeMap::new());
    let mut conditions = Vec::new();
    let mut relations = Vec::new();
    for item in &select.from {
        bind_table_with_joins(item, scopes, out, &mut conditions, &mut relations);
    }
    if let Some(selection) = &select.selection {
        conditions.push(selection);
    }
    for condition in conditions {
        read_condition(condition, scopes, out);
    }

    if is_top {
        // An unqualified column belongs to the FROM clause's only relation.
        let single = match (select.from.as_slice(), relations.as_slice()) {
            ([only], [relation]) if only.joins.is_empty() => Some(relation.clone()),
            _ => None,
        };
        for item in &select.projection {
            let SelectItem::Expr { expr, alias } = item else {
                // Columns after a wildcard have no position or name to read.
                break;
            };
            match column_ref(expr, scopes, single.as_deref()) {
                Some(source) => {
                    let name = alias
                        .as_ref()
                        .map(|a| a.as_str().to_string())
                        .unwrap_or_else(|| source.column.clone());
                    out.outputs.push((name, source));
                }
                // Keeps positions aligned for a renaming column list, and is
                // dropped after renaming.
                None => out.outputs.push((String::new(), ColumnRef::placeholder())),
            }
        }
    }
    scopes.pop();
}

impl ColumnRef {
    fn placeholder() -> Self {
        ColumnRef {
            id: String::new(),
            column: String::new(),
        }
    }
}

/// Binds the FROM item's relations in the innermost scope, and collects its
/// join conditions and the relations it names, in order.
fn bind_table_with_joins<'a>(
    item: &'a TableWithJoins<Raw>,
    scopes: &mut Scopes,
    out: &mut Extracted,
    conditions: &mut Vec<&'a Expr<Raw>>,
    relations: &mut Vec<String>,
) {
    let mut joined: Vec<String> = Vec::new();
    bind_factor(&item.relation, scopes, out, conditions, &mut joined);
    for join in &item.joins {
        let before = joined.clone();
        let right = bind_factor(&join.relation, scopes, out, conditions, &mut joined);
        let constraint = match &join.join_operator {
            JoinOperator::Inner(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c) => Some(c),
            JoinOperator::CrossJoin => None,
        };
        match constraint {
            Some(JoinConstraint::On(expr)) => conditions.push(expr),
            Some(JoinConstraint::Using { columns, .. }) => {
                if let Some(right) = right {
                    let mut candidates = before;
                    candidates.dedup();
                    if !candidates.is_empty() {
                        for column in columns {
                            out.usings.push((
                                right.clone(),
                                column.as_str().to_string(),
                                candidates.clone(),
                            ));
                        }
                    }
                }
            }
            Some(JoinConstraint::Natural) | None => {}
        }
    }
    relations.extend(joined);
}

/// Binds one FROM item. Returns the catalog relation it names, if any.
fn bind_factor<'a>(
    factor: &'a TableFactor<Raw>,
    scopes: &mut Scopes,
    out: &mut Extracted,
    conditions: &mut Vec<&'a Expr<Raw>>,
    joined: &mut Vec<String>,
) -> Option<String> {
    let scope = scopes.last_mut().expect("a SELECT pushed its scope");
    match factor {
        TableFactor::Table { name, alias } => {
            let (binding, default_name) = match name {
                RawItemName::Id(id, name, _) => (Binding::Relation(id.clone()), name),
                // A CTE.
                RawItemName::Name(name) => (Binding::Opaque, name),
            };
            let renames = alias.as_ref().is_some_and(|a| !a.columns.is_empty());
            let binding = if renames { Binding::Opaque } else { binding };
            let bound_name = match alias {
                Some(alias) => alias.name.as_str().to_string(),
                None => match default_name.0.last() {
                    Some(ident) => ident.as_str().to_string(),
                    None => return None,
                },
            };
            bind(scope, bound_name, binding.clone());
            match binding {
                Binding::Relation(id) => {
                    joined.push(id.clone());
                    Some(id)
                }
                Binding::Opaque => None,
            }
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            if let Some(alias) = alias {
                bind(scope, alias.name.as_str().to_string(), Binding::Opaque);
            }
            walk_query(subquery, scopes, out, false);
            None
        }
        TableFactor::Function { alias, .. } | TableFactor::RowsFrom { alias, .. } => {
            if let Some(alias) = alias {
                bind(scope, alias.name.as_str().to_string(), Binding::Opaque);
            }
            None
        }
        TableFactor::NestedJoin { join, .. } => {
            let mut relations = Vec::new();
            bind_table_with_joins(join, scopes, out, conditions, &mut relations);
            joined.extend(relations);
            None
        }
    }
}

/// Binding a name twice in one scope makes it ambiguous.
fn bind(scope: &mut BTreeMap<String, Binding>, name: String, binding: Binding) {
    match scope.get(&name) {
        Some(existing) if *existing != binding => {
            scope.insert(name, Binding::Opaque);
        }
        _ => {
            scope.insert(name, binding);
        }
    }
}

fn lookup(scopes: &Scopes, name: &str) -> Option<Binding> {
    scopes
        .iter()
        .rev()
        .find_map(|scope| scope.get(name).cloned())
}

/// The relation column an expression is, if it is a bare column reference.
fn column_ref(expr: &Expr<Raw>, scopes: &Scopes, single: Option<&str>) -> Option<ColumnRef> {
    match expr {
        Expr::Nested(inner) => column_ref(inner, scopes, single),
        Expr::Identifier(parts) => match parts.as_slice() {
            [qualifier, column] => match lookup(scopes, qualifier.as_str())? {
                Binding::Relation(id) => Some(ColumnRef {
                    id,
                    column: column.as_str().to_string(),
                }),
                Binding::Opaque => None,
            },
            [column] => single.map(|id| ColumnRef {
                id: id.to_string(),
                column: column.as_str().to_string(),
            }),
            _ => None,
        },
        _ => None,
    }
}

fn is_equals(op: &str, namespace: &Option<Vec<Ident>>) -> bool {
    op == "=" && namespace.is_none()
}

/// Reads the column equalities among a condition's top-level conjuncts, and
/// walks the subqueries they hold.
fn read_condition(expr: &Expr<Raw>, scopes: &mut Scopes, out: &mut Extracted) {
    match expr {
        Expr::Nested(inner) => read_condition(inner, scopes, out),
        Expr::And { left, right } => {
            read_condition(left, scopes, out);
            read_condition(right, scopes, out);
        }
        Expr::Op {
            op,
            expr1,
            expr2: Some(expr2),
        } if is_equals(&op.op, &op.namespace) => {
            let left = column_ref(expr1, scopes, None);
            let right = column_ref(expr2, scopes, None);
            if let (Some(left), Some(right)) = (left, right) {
                if left.id != right.id {
                    out.joins.push((left, right));
                }
            }
        }
        // `a.x IN (SELECT b.y FROM b ...)` is a semijoin on `a.x = b.y`.
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            if let (Some(left), SetExpr::Select(select)) =
                (column_ref(expr, scopes, None), &subquery.body)
            {
                if let [SelectItem::Expr { expr: inner, .. }] = select.projection.as_slice() {
                    scopes.push(BTreeMap::new());
                    let mut relations = Vec::new();
                    let mut ignored = Vec::new();
                    for item in &select.from {
                        bind_table_with_joins(item, scopes, out, &mut ignored, &mut relations);
                    }
                    let single = match (select.from.as_slice(), relations.as_slice()) {
                        ([only], [relation]) if only.joins.is_empty() => Some(relation.clone()),
                        _ => None,
                    };
                    let right = column_ref(inner, scopes, single.as_deref());
                    scopes.pop();
                    if let Some(right) = right {
                        if left.id != right.id {
                            out.joins.push((left, right));
                        }
                    }
                }
            }
            walk_query(subquery, scopes, out, false);
        }
        Expr::Exists(subquery) | Expr::Subquery(subquery) => {
            walk_query(subquery, scopes, out, false);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::view_joins;

    fn joins(sql: &str) -> Vec<String> {
        let out = view_joins(sql).expect("parses");
        out["joins"]
            .as_array()
            .unwrap()
            .iter()
            .map(|j| {
                format!(
                    "{}.{}={}.{}",
                    j["left_id"].as_str().unwrap(),
                    j["left_column"].as_str().unwrap(),
                    j["right_id"].as_str().unwrap(),
                    j["right_column"].as_str().unwrap()
                )
            })
            .collect()
    }

    fn outputs(sql: &str) -> Vec<String> {
        let out = view_joins(sql).expect("parses");
        out["outputs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| {
                format!(
                    "{}<-{}.{}",
                    o["column"].as_str().unwrap(),
                    o["source_id"].as_str().unwrap(),
                    o["source_column"].as_str().unwrap()
                )
            })
            .collect()
    }

    // Fixtures are `create_sql` as v26.50 stores it, for an auction schema:
    // u3 auctions, u4 bids, u5 organizations, u6 users, u7 winning_bids.

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn aliased_joins_and_passed_columns() {
        let sql = r#"CREATE VIEW "materialize"."agents"."org_spend" AS SELECT "o"."id" AS "org_id", "o"."name", "pg_catalog"."sum"("w"."amount") AS "spend" FROM [u5 AS "materialize"."auction"."organizations"] AS "o" JOIN [u6 AS "materialize"."auction"."users"] AS "u" ON "u"."org_id" = "o"."id" JOIN [u7 AS "materialize"."auction"."winning_bids"] AS "w" ON "w"."buyer" = "u"."id" GROUP BY "o"."id", "o"."name""#;
        assert_eq!(joins(sql), ["u6.org_id=u5.id", "u7.buyer=u6.id"]);
        assert_eq!(outputs(sql), ["org_id<-u5.id", "name<-u5.name"]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn materialized_view_with_distinct_on() {
        let sql = r#"CREATE MATERIALIZED VIEW "materialize"."auction"."winning_bids" IN CLUSTER [u2] WITH (REFRESH = ON COMMIT) AS SELECT DISTINCT ON ("a"."id") "b"."id" AS "bid_id", "a"."id" AS "auction_id", "b"."buyer" FROM [u3 AS "materialize"."auction"."auctions"] AS "a" JOIN [u4 AS "materialize"."auction"."bids"] AS "b" ON "a"."id" = "b"."auction_id" WHERE "b"."bid_time" < "a"."end_time""#;
        assert_eq!(joins(sql), ["u3.id=u4.auction_id"]);
        assert_eq!(
            outputs(sql),
            ["bid_id<-u4.id", "auction_id<-u3.id", "buyer<-u4.buyer"]
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn semijoins_and_correlated_conditions() {
        let sql = r#"CREATE VIEW "v" AS SELECT * FROM [u4 AS "materialize"."auction"."bids"] AS "b" WHERE "b"."auction_id" IN (SELECT "a"."id" FROM [u3 AS "materialize"."auction"."auctions"] AS "a" WHERE "a"."seller" = "b"."buyer")"#;
        assert_eq!(joins(sql), ["u4.auction_id=u3.id", "u3.seller=u4.buyer"]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn opaque_bindings_resolve_nothing() {
        // A CTE named like nothing in the catalog.
        let cte = r#"CREATE VIEW "v" AS WITH "x" AS (SELECT "seller" FROM [u3 AS "materialize"."auction"."auctions"]) SELECT "u"."name" FROM "x", [u6 AS "materialize"."auction"."users"] AS "u" WHERE "u"."id" = "x"."seller""#;
        assert!(joins(cte).is_empty());
        // An inner alias shadows the outer one inside the subquery only.
        let shadow = r#"CREATE VIEW "v" AS SELECT "a"."id" FROM [u3 AS "materialize"."auction"."auctions"] AS "a" JOIN (SELECT "a"."id" FROM [u4 AS "materialize"."auction"."bids"] AS "a") AS "s" ON "s"."id" = "a"."id""#;
        assert!(joins(shadow).is_empty());
        // An alias that renames columns.
        let renamed = r#"CREATE VIEW "v" AS SELECT 1 FROM [u4 AS "materialize"."auction"."bids"] AS "b" ("x", "auction_id") JOIN [u3 AS "materialize"."auction"."auctions"] AS "a" ON "a"."id" = "b"."auction_id""#;
        assert!(joins(renamed).is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn expressions_are_not_columns() {
        let sql = r#"CREATE VIEW "v" AS SELECT 1 FROM [u4 AS "materialize"."auction"."bids"] AS "b" JOIN [u3 AS "materialize"."auction"."auctions"] AS "a" ON "a"."id" + 0 = "b"."auction_id" AND "a"."seller"::[s46 AS "pg_catalog"."text"] = "b"."buyer"::[s46 AS "pg_catalog"."text"]"#;
        assert_eq!(view_joins(sql).unwrap()["joins"], json!([]));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn using_joins_list_earlier_relations() {
        let sql = r#"CREATE VIEW "v" AS SELECT "users"."name" FROM [u6 AS "materialize"."auction"."users"] JOIN [u2 AS "materialize"."auction"."accounts"] AS "a" USING ("org_id")"#;
        assert_eq!(
            view_joins(sql).unwrap()["using"],
            json!([{"right_id": "u2", "column": "org_id", "candidates": ["u6"]}])
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn outputs_follow_view_column_renames_and_skip_set_operations() {
        let renamed = r#"CREATE VIEW "v" ("x", "y") AS SELECT 1, "s"."seller" FROM [u3 AS "materialize"."auction"."auctions"] AS "s""#;
        assert_eq!(outputs(renamed), ["y<-u3.seller"]);
        let union = r#"CREATE VIEW "v" AS SELECT "a"."id" FROM [u3 AS "materialize"."auction"."auctions"] AS "a" UNION ALL SELECT "b"."id" FROM [u4 AS "materialize"."auction"."bids"] AS "b""#;
        assert!(outputs(union).is_empty());
        let single = r#"CREATE VIEW "v" AS SELECT "seller" FROM [u3 AS "materialize"."auction"."auctions"] GROUP BY "seller""#;
        assert_eq!(outputs(single), ["seller<-u3.seller"]);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn other_statements_are_empty() {
        let sql = r#"CREATE TABLE "materialize"."public"."t" ("a" [s20 AS "pg_catalog"."int4"])"#;
        assert_eq!(
            view_joins(sql).unwrap(),
            json!({"joins": [], "using": [], "outputs": []})
        );
    }
}
