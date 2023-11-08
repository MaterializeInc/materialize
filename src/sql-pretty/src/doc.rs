// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functions that convert SQL AST nodes to pretty Docs.

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::*;
use pretty::{Doc, RcDoc};

use crate::util::{
    bracket, bracket_doc, comma_separate, comma_separated, nest, nest_title, title_comma_separate,
};
use crate::TAB;

// Use when we don't know what to do.
pub(crate) fn doc_display<'a, T: AstDisplay>(v: &T, _debug: &str) -> RcDoc<'a, ()> {
    #[cfg(test)]
    eprintln!(
        "UNKNOWN PRETTY TYPE in {}: {}, {}",
        _debug,
        std::any::type_name::<T>(),
        v.to_ast_string()
    );
    doc_display_pass(v)
}

// Use when the AstDisplay trait is what we want.
fn doc_display_pass<'a, T: AstDisplay>(v: &T) -> RcDoc<'a, ()> {
    RcDoc::text(v.to_ast_string())
}

pub(crate) fn doc_create_view<T: AstInfo>(v: &CreateViewStatement<T>) -> RcDoc {
    let mut docs = vec![];
    docs.push(RcDoc::text(format!(
        "CREATE{}{} VIEW{}",
        if v.if_exists == IfExistsBehavior::Replace {
            " OR REPLACE"
        } else {
            ""
        },
        if v.temporary { " TEMPORARY" } else { "" },
        if v.if_exists == IfExistsBehavior::Skip {
            " IF NOT EXISTS"
        } else {
            ""
        },
    )));
    docs.push(doc_view_definition(&v.definition));
    RcDoc::intersperse(docs, Doc::line()).nest(TAB).group()
}

pub(crate) fn doc_create_materialized_view<T: AstInfo>(
    v: &CreateMaterializedViewStatement<T>,
) -> RcDoc {
    let mut docs = vec![];
    docs.push(RcDoc::text(format!(
        "CREATE{} MATERIALIZED VIEW{} {}",
        if v.if_exists == IfExistsBehavior::Replace {
            " OR REPLACE"
        } else {
            ""
        },
        if v.if_exists == IfExistsBehavior::Skip {
            " IF NOT EXISTS"
        } else {
            ""
        },
        v.name,
    )));
    if !v.columns.is_empty() {
        docs.push(bracket(
            "(",
            comma_separate(doc_display_pass, &v.columns),
            ")",
        ));
    }
    if let Some(cluster) = &v.in_cluster {
        docs.push(RcDoc::text(format!(
            "IN CLUSTER {}",
            cluster.to_ast_string()
        )));
    }
    if !v.with_options.is_empty() {
        docs.push(bracket(
            "WITH (",
            comma_separate(doc_display_pass, &v.with_options),
            ")",
        ));
    }
    docs.push(nest_title("AS", doc_query(&v.query)));
    RcDoc::intersperse(docs, Doc::line()).nest(TAB).group()
}

fn doc_view_definition<T: AstInfo>(v: &ViewDefinition<T>) -> RcDoc {
    let mut docs = vec![RcDoc::text(v.name.to_string())];
    if !v.columns.is_empty() {
        docs.push(bracket(
            "(",
            comma_separate(doc_display_pass, &v.columns),
            ")",
        ));
    }
    docs.push(nest_title("AS", doc_query(&v.query)));
    RcDoc::intersperse(docs, Doc::line()).group()
}

pub(crate) fn doc_insert<T: AstInfo>(v: &InsertStatement<T>) -> RcDoc {
    let mut first = vec![RcDoc::text(format!(
        "INSERT INTO {}",
        v.table_name.to_ast_string()
    ))];
    if !v.columns.is_empty() {
        first.push(bracket(
            "(",
            comma_separate(doc_display_pass, &v.columns),
            ")",
        ));
    }
    let sources = match &v.source {
        InsertSource::Query(query) => doc_query(query),
        _ => doc_display(&v.source, "insert source"),
    };
    let mut doc = RcDoc::intersperse(
        [
            RcDoc::intersperse(first, Doc::line()).nest(TAB).group(),
            sources,
        ],
        Doc::line(),
    )
    .nest(TAB)
    .group();
    if !v.returning.is_empty() {
        doc = nest(
            doc,
            nest_title("RETURNING", comma_separate(doc_select_item, &v.returning)),
        )
    }
    doc
}

pub(crate) fn doc_select_statement<T: AstInfo>(v: &SelectStatement<T>) -> RcDoc {
    let mut doc = doc_query(&v.query);
    if let Some(as_of) = &v.as_of {
        doc = RcDoc::intersperse([doc, doc_display_pass(as_of)], Doc::line())
            .nest(TAB)
            .group();
    }
    doc.group()
}

fn doc_order_by<T: AstInfo>(v: &[OrderByExpr<T>]) -> RcDoc {
    title_comma_separate("ORDER BY", doc_order_by_expr, v)
}

fn doc_order_by_expr<T: AstInfo>(v: &OrderByExpr<T>) -> RcDoc {
    let doc = doc_expr(&v.expr);
    let doc = match v.asc {
        Some(true) => nest(doc, RcDoc::text("ASC")),
        Some(false) => nest(doc, RcDoc::text("DESC")),
        None => doc,
    };
    match v.nulls_last {
        Some(true) => nest(doc, RcDoc::text("NULLS LAST")),
        Some(false) => nest(doc, RcDoc::text("NULLS FIRST")),
        None => doc,
    }
}

fn doc_query<T: AstInfo>(v: &Query<T>) -> RcDoc {
    let mut docs = vec![];
    if !v.ctes.is_empty() {
        match &v.ctes {
            CteBlock::Simple(ctes) => docs.push(title_comma_separate("WITH", doc_cte, ctes)),
            CteBlock::MutuallyRecursive(mutrec) => {
                let mut doc = RcDoc::text("WITH MUTUALLY RECURSIVE");
                if !mutrec.options.is_empty() {
                    doc = nest(
                        doc,
                        bracket("(", comma_separate(doc_display_pass, &mutrec.options), ")"),
                    );
                }
                docs.push(nest(
                    doc,
                    comma_separate(doc_mutually_recursive, &mutrec.ctes),
                ));
            }
        }
    }
    docs.push(doc_set_expr(&v.body));
    if !v.order_by.is_empty() {
        docs.push(doc_order_by(&v.order_by));
    }

    let offset = if let Some(offset) = &v.offset {
        vec![RcDoc::concat([nest_title("OFFSET", doc_expr(offset))])]
    } else {
        vec![]
    };

    if let Some(limit) = &v.limit {
        if limit.with_ties {
            docs.extend(offset);
            docs.push(RcDoc::concat([
                RcDoc::text("FETCH FIRST "),
                doc_expr(&limit.quantity),
                RcDoc::text(" ROWS WITH TIES"),
            ]));
        } else {
            docs.push(nest_title("LIMIT", doc_expr(&limit.quantity)));
            docs.extend(offset);
        }
    } else {
        docs.extend(offset);
    }

    RcDoc::intersperse(docs, Doc::line()).group()
}

fn doc_cte<T: AstInfo>(v: &Cte<T>) -> RcDoc {
    RcDoc::concat([
        RcDoc::text(format!("{} AS", v.alias)),
        RcDoc::line(),
        bracket("(", doc_query(&v.query), ")"),
    ])
}

fn doc_mutually_recursive<T: AstInfo>(v: &CteMutRec<T>) -> RcDoc {
    let mut docs = Vec::new();
    if !v.columns.is_empty() {
        docs.push(bracket(
            "(",
            comma_separate(doc_display_pass, &v.columns),
            ")",
        ));
    }
    docs.push(bracket("AS (", doc_query(&v.query), ")"));
    nest(
        doc_display_pass(&v.name),
        RcDoc::intersperse(docs, Doc::line()).group(),
    )
}

fn doc_set_expr<T: AstInfo>(v: &SetExpr<T>) -> RcDoc {
    match v {
        SetExpr::Select(v) => doc_select(v),
        SetExpr::Query(v) => bracket("(", doc_query(v), ")"),
        SetExpr::SetOperation {
            op,
            all,
            left,
            right,
        } => {
            let all_str = if *all { " ALL" } else { "" };
            RcDoc::concat([
                doc_set_expr(left),
                RcDoc::line(),
                RcDoc::concat([
                    RcDoc::text(format!("{}{}", op, all_str)),
                    RcDoc::line(),
                    doc_set_expr(right),
                ])
                .nest(TAB)
                .group(),
            ])
        }
        SetExpr::Values(v) => doc_values(v),
        SetExpr::Show(v) => doc_display(v, "SHOW"),
        SetExpr::Table(v) => nest(RcDoc::text("TABLE"), doc_display_pass(v)),
    }
    .group()
}

fn doc_values<T: AstInfo>(v: &Values<T>) -> RcDoc {
    let rows =
        v.0.iter()
            .map(|row| bracket("(", comma_separate(doc_expr, row), ")"))
            .collect();
    RcDoc::concat([RcDoc::text("VALUES"), RcDoc::line(), comma_separated(rows)])
        .nest(TAB)
        .group()
}

fn doc_table_with_joins<T: AstInfo>(v: &TableWithJoins<T>) -> RcDoc {
    let mut docs = vec![doc_table_factor(&v.relation)];
    for j in &v.joins {
        docs.push(doc_join(j));
    }
    RcDoc::intersperse(docs, Doc::line()).nest(TAB).group()
}

fn doc_join<T: AstInfo>(v: &Join<T>) -> RcDoc {
    let (constraint, name) = match &v.join_operator {
        JoinOperator::Inner(constraint) => (constraint, "JOIN"),
        JoinOperator::FullOuter(constraint) => (constraint, "FULL JOIN"),
        JoinOperator::LeftOuter(constraint) => (constraint, "LEFT JOIN"),
        JoinOperator::RightOuter(constraint) => (constraint, "RIGHT JOIN"),
        _ => return doc_display(v, "join operator"),
    };
    let constraint = match constraint {
        JoinConstraint::On(expr) => nest_title("ON", doc_expr(expr)),
        JoinConstraint::Using { columns, alias } => {
            let mut doc = bracket("USING(", comma_separate(doc_display_pass, columns), ")");
            if let Some(alias) = alias {
                doc = nest(doc, nest_title("AS", doc_display_pass(alias)));
            }
            doc
        }
        _ => return doc_display(v, "join constrant"),
    };
    RcDoc::intersperse(
        [RcDoc::text(name), doc_table_factor(&v.relation), constraint],
        Doc::line(),
    )
    .nest(TAB)
    .group()
}

fn doc_table_factor<T: AstInfo>(v: &TableFactor<T>) -> RcDoc {
    match v {
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            if *lateral {
                return doc_display(v, "table factor lateral");
            }
            let mut docs = vec![bracket("(", doc_query(subquery), ")")];
            if let Some(alias) = alias {
                docs.push(RcDoc::text(format!("AS {}", alias)));
            }
            RcDoc::intersperse(docs, Doc::line()).nest(TAB).group()
        }
        TableFactor::NestedJoin { join, alias } => {
            let mut doc = bracket("(", doc_table_with_joins(join), ")");
            if let Some(alias) = alias {
                doc = RcDoc::intersperse([doc, RcDoc::text(format!("AS {}", alias))], Doc::line())
                    .nest(TAB)
                    .group()
            }
            doc
        }
        TableFactor::Table { .. } => doc_display_pass(v),
        _ => doc_display(v, "table factor variant"),
    }
}

fn doc_select<T: AstInfo>(v: &Select<T>) -> RcDoc {
    let mut docs = vec![];
    docs.push(title_comma_separate(
        format!(
            "SELECT{}",
            if let Some(distinct) = &v.distinct {
                format!(" {}", distinct.to_ast_string())
            } else {
                "".into()
            }
        ),
        doc_select_item,
        &v.projection,
    ));
    if !v.from.is_empty() {
        docs.push(title_comma_separate("FROM", doc_table_with_joins, &v.from));
    }
    if let Some(selection) = &v.selection {
        docs.push(nest_title("WHERE", doc_expr(selection)));
    }
    if !v.group_by.is_empty() {
        docs.push(title_comma_separate("GROUP BY", doc_expr, &v.group_by));
    }
    if let Some(having) = &v.having {
        docs.push(nest_title("HAVING", doc_expr(having)));
    }
    if !v.options.is_empty() {
        docs.push(bracket(
            "OPTIONS (",
            comma_separate(doc_display_pass, &v.options),
            ")",
        ));
    }
    RcDoc::intersperse(docs, Doc::line()).group()
}

fn doc_select_item<T: AstInfo>(v: &SelectItem<T>) -> RcDoc {
    match v {
        SelectItem::Expr { expr, alias } => {
            let mut doc = doc_expr(expr);
            if let Some(alias) = alias {
                doc = nest(
                    doc,
                    RcDoc::concat([RcDoc::text("AS "), doc_display_pass(alias)]),
                );
            }
            doc
        }
        SelectItem::Wildcard => doc_display_pass(v),
    }
}

fn doc_expr<T: AstInfo>(v: &Expr<T>) -> RcDoc {
    match v {
        Expr::Op { op, expr1, expr2 } => {
            if let Some(expr2) = expr2 {
                RcDoc::concat([
                    doc_expr(expr1),
                    RcDoc::line(),
                    RcDoc::text(format!("{} ", op)),
                    doc_expr(expr2).nest(TAB),
                ])
            } else {
                RcDoc::concat([RcDoc::text(format!("{} ", op)), doc_expr(expr1)])
            }
        }
        Expr::Cast { expr, data_type } => bracket(
            "CAST(",
            RcDoc::concat([
                doc_expr(expr),
                RcDoc::line(),
                RcDoc::text(format!("AS {}", data_type.to_ast_string())),
            ])
            .nest(TAB),
            ")",
        ),
        Expr::Nested(ast) => bracket("(", doc_expr(ast), ")"),
        Expr::Function(fun) => doc_function(fun),
        Expr::Subquery(ast) => bracket("(", doc_query(ast), ")"),
        Expr::Identifier(_)
        | Expr::Value(_)
        | Expr::QualifiedWildcard(_)
        | Expr::WildcardAccess(_)
        | Expr::FieldAccess { .. } => doc_display_pass(v),
        Expr::And { left, right } => bracket_doc(
            doc_expr(left),
            RcDoc::text("AND"),
            doc_expr(right),
            RcDoc::line(),
        ),
        Expr::Or { left, right } => bracket_doc(
            doc_expr(left),
            RcDoc::text("OR"),
            doc_expr(right),
            RcDoc::line(),
        ),
        Expr::Exists(s) => bracket("EXISTS (", doc_query(s), ")"),
        Expr::IsExpr {
            expr,
            negated,
            construct,
        } => bracket_doc(
            doc_expr(expr),
            RcDoc::text(if *negated { "IS NOT" } else { "IS" }),
            doc_display_pass(construct),
            RcDoc::line(),
        ),
        Expr::Not { expr } => RcDoc::concat([RcDoc::text("NOT"), RcDoc::line(), doc_expr(expr)]),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => RcDoc::intersperse(
            [
                doc_expr(expr),
                RcDoc::text(if *negated { "NOT BETWEEN" } else { "BETWEEN" }),
                RcDoc::intersperse(
                    [doc_expr(low), RcDoc::text("AND"), doc_expr(high)],
                    RcDoc::line(),
                )
                .group(),
            ],
            RcDoc::line(),
        ),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => RcDoc::intersperse(
            [
                doc_expr(expr),
                RcDoc::text(if *negated { "NOT IN (" } else { "IN (" }),
                doc_query(subquery),
                RcDoc::text(")"),
            ],
            RcDoc::line(),
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => RcDoc::intersperse(
            [
                doc_expr(expr),
                RcDoc::text(if *negated { "NOT IN (" } else { "IN (" }),
                comma_separate(doc_expr, list),
                RcDoc::text(")"),
            ],
            RcDoc::line(),
        ),
        Expr::Row { exprs } => bracket("ROW(", comma_separate(doc_expr, exprs), ")"),
        _ => doc_display(v, "expr variant"),
    }
    .group()
}

fn doc_function<T: AstInfo>(v: &Function<T>) -> RcDoc {
    match &v.args {
        FunctionArgs::Star => doc_display_pass(v),
        FunctionArgs::Args { args, order_by } => {
            if args.is_empty() {
                // Nullary, don't allow newline between parens, so just delegate.
                doc_display_pass(v)
            } else {
                if v.filter.is_some() || v.over.is_some() || !order_by.is_empty() {
                    return doc_display(v, "function filter or over or order by");
                }
                let mut name = format!("{}(", v.name.to_ast_string());
                if v.distinct {
                    name.push_str("DISTINCT");
                }
                bracket(name, comma_separate(doc_expr, args), ")")
            }
        }
    }
}
