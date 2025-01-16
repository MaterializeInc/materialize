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
    bracket, bracket_doc, comma_separate, comma_separated, intersperse_line_nest, nest,
    nest_comma_separate, nest_title, title_comma_separate,
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

pub(crate) fn doc_create_source<T: AstInfo>(v: &CreateSourceStatement<T>) -> RcDoc {
    let mut docs = Vec::new();
    let title = format!(
        "CREATE SOURCE{}",
        if v.if_not_exists {
            " IF NOT EXISTS"
        } else {
            ""
        }
    );
    let mut doc = doc_display_pass(&v.name);
    let mut names = Vec::new();
    names.extend(v.col_names.iter().map(doc_display_pass));
    names.extend(v.key_constraint.iter().map(doc_display_pass));
    if !names.is_empty() {
        doc = nest(doc, bracket("(", comma_separated(names), ")"));
    }
    docs.push(nest_title(title, doc));
    if let Some(cluster) = &v.in_cluster {
        docs.push(nest_title("IN CLUSTER", doc_display_pass(cluster)));
    }
    docs.push(nest_title("FROM", doc_display_pass(&v.connection)));
    if let Some(format) = &v.format {
        docs.push(doc_format_specifier(format));
    }
    if !v.include_metadata.is_empty() {
        docs.push(nest_title(
            "INCLUDE",
            comma_separate(doc_display_pass, &v.include_metadata),
        ));
    }
    if let Some(envelope) = &v.envelope {
        docs.push(nest_title("ENVELOPE", doc_display_pass(envelope)));
    }
    if let Some(references) = &v.external_references {
        docs.push(doc_external_references(references));
    }
    if let Some(progress) = &v.progress_subsource {
        docs.push(nest_title("EXPOSE PROGRESS AS", doc_display_pass(progress)));
    }
    if !v.with_options.is_empty() {
        docs.push(bracket(
            "WITH (",
            comma_separate(doc_display_pass, &v.with_options),
            ")",
        ));
    }
    RcDoc::intersperse(docs, Doc::line()).group()
}

fn doc_format_specifier<T: AstInfo>(v: &FormatSpecifier<T>) -> RcDoc {
    match v {
        FormatSpecifier::Bare(format) => nest_title("FORMAT", doc_display_pass(format)),
        FormatSpecifier::KeyValue { key, value } => {
            let docs = vec![
                nest_title("KEY FORMAT", doc_display_pass(key)),
                nest_title("VALUE FORMAT", doc_display_pass(value)),
            ];
            RcDoc::intersperse(docs, Doc::line()).group()
        }
    }
}

fn doc_external_references(v: &ExternalReferences) -> RcDoc {
    match v {
        ExternalReferences::SubsetTables(subsources) => bracket(
            "FOR TABLES (",
            comma_separate(doc_display_pass, subsources),
            ")",
        ),
        ExternalReferences::SubsetSchemas(schemas) => bracket(
            "FOR SCHEMAS (",
            comma_separate(doc_display_pass, schemas),
            ")",
        ),
        ExternalReferences::All => RcDoc::text("FOR ALL TABLES"),
    }
}

pub(crate) fn doc_copy<T: AstInfo>(v: &CopyStatement<T>) -> RcDoc {
    let relation = match &v.relation {
        CopyRelation::Named { name, columns } => {
            let mut relation = doc_display_pass(name);
            if !columns.is_empty() {
                relation = bracket_doc(
                    nest(relation, RcDoc::text("(")),
                    comma_separate(doc_display_pass, columns),
                    RcDoc::text(")"),
                    RcDoc::line_(),
                );
            }
            RcDoc::concat([RcDoc::text("COPY "), relation])
        }
        CopyRelation::Select(query) => bracket("COPY (", doc_select_statement(query), ")"),
        CopyRelation::Subscribe(query) => bracket("COPY (", doc_subscribe(query), ")"),
    };
    let mut docs = vec![
        relation,
        RcDoc::concat([
            doc_display_pass(&v.direction),
            RcDoc::text(" "),
            doc_display_pass(&v.target),
        ]),
    ];
    if !v.options.is_empty() {
        docs.push(bracket(
            "WITH (",
            comma_separate(doc_display_pass, &v.options),
            ")",
        ));
    }
    RcDoc::intersperse(docs, Doc::line()).group()
}

pub(crate) fn doc_subscribe<T: AstInfo>(v: &SubscribeStatement<T>) -> RcDoc {
    let doc = match &v.relation {
        SubscribeRelation::Name(name) => nest_title("SUBSCRIBE", doc_display_pass(name)),
        SubscribeRelation::Query(query) => bracket("SUBSCRIBE (", doc_query(query), ")"),
    };
    let mut docs = vec![doc];
    if !v.options.is_empty() {
        docs.push(bracket(
            "WITH (",
            comma_separate(doc_display_pass, &v.options),
            ")",
        ));
    }
    if let Some(as_of) = &v.as_of {
        docs.push(doc_as_of(as_of));
    }
    if let Some(up_to) = &v.up_to {
        docs.push(nest_title("UP TO", doc_expr(up_to)));
    }
    match &v.output {
        SubscribeOutput::Diffs => {}
        SubscribeOutput::WithinTimestampOrderBy { order_by } => {
            docs.push(nest_title(
                "WITHIN TIMESTAMP ORDER BY ",
                comma_separate(doc_order_by_expr, order_by),
            ));
        }
        SubscribeOutput::EnvelopeUpsert { key_columns } => {
            docs.push(bracket(
                "ENVELOPE UPSERT (KEY (",
                comma_separate(doc_display_pass, key_columns),
                "))",
            ));
        }
        SubscribeOutput::EnvelopeDebezium { key_columns } => {
            docs.push(bracket(
                "ENVELOPE DEBEZIUM (KEY (",
                comma_separate(doc_display_pass, key_columns),
                "))",
            ));
        }
    }
    RcDoc::intersperse(docs, Doc::line()).group()
}

fn doc_as_of<T: AstInfo>(v: &AsOf<T>) -> RcDoc {
    let (title, expr) = match v {
        AsOf::At(expr) => ("AS OF", expr),
        AsOf::AtLeast(expr) => ("AS OF AT LEAST", expr),
    };
    nest_title(title, doc_expr(expr))
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
    intersperse_line_nest(docs)
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
    intersperse_line_nest(docs)
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
    let mut doc = intersperse_line_nest([intersperse_line_nest(first), sources]);
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
        doc = intersperse_line_nest([doc, doc_as_of(as_of)]);
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
            .map(|row| bracket("(", comma_separate(doc_expr, row), ")"));
    RcDoc::concat([RcDoc::text("VALUES"), RcDoc::line(), comma_separated(rows)])
        .nest(TAB)
        .group()
}

fn doc_table_with_joins<T: AstInfo>(v: &TableWithJoins<T>) -> RcDoc {
    let mut docs = vec![doc_table_factor(&v.relation)];
    for j in &v.joins {
        docs.push(doc_join(j));
    }
    intersperse_line_nest(docs)
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
    intersperse_line_nest([RcDoc::text(name), doc_table_factor(&v.relation), constraint])
}

fn doc_table_factor<T: AstInfo>(v: &TableFactor<T>) -> RcDoc {
    match v {
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            let prefix = if *lateral { "LATERAL (" } else { "(" };
            let mut docs = vec![bracket(prefix, doc_query(subquery), ")")];
            if let Some(alias) = alias {
                docs.push(RcDoc::text(format!("AS {}", alias)));
            }
            intersperse_line_nest(docs)
        }
        TableFactor::NestedJoin { join, alias } => {
            let mut doc = bracket("(", doc_table_with_joins(join), ")");
            if let Some(alias) = alias {
                doc = nest(doc, RcDoc::text(format!("AS {}", alias)));
            }
            doc
        }
        TableFactor::Table { name, alias } => {
            let mut doc = doc_display_pass(name);
            if let Some(alias) = alias {
                doc = nest(doc, RcDoc::text(format!("AS {}", alias)));
            }
            doc
        }
        _ => doc_display(v, "table factor variant"),
    }
}

fn doc_distinct<T: AstInfo>(v: &Distinct<T>) -> RcDoc {
    match v {
        Distinct::EntireRow => RcDoc::text("DISTINCT"),
        Distinct::On(cols) => bracket("DISTINCT ON (", comma_separate(doc_expr, cols), ")"),
    }
}

fn doc_select<T: AstInfo>(v: &Select<T>) -> RcDoc {
    let mut docs = vec![];
    let mut select = RcDoc::text("SELECT");
    if let Some(distinct) = &v.distinct {
        select = nest(select, doc_distinct(distinct));
    }
    docs.push(nest_comma_separate(select, doc_select_item, &v.projection));
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
    if let Some(qualify) = &v.qualify {
        docs.push(nest_title("QUALIFY", doc_expr(qualify)));
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

pub fn doc_expr<T: AstInfo>(v: &Expr<T>) -> RcDoc {
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
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let mut docs = Vec::new();
            if let Some(operand) = operand {
                docs.push(doc_expr(operand));
            }
            for (c, r) in conditions.iter().zip(results) {
                let when = nest_title("WHEN", doc_expr(c));
                let then = nest_title("THEN", doc_expr(r));
                docs.push(nest(when, then));
            }
            if let Some(else_result) = else_result {
                docs.push(nest_title("ELSE", doc_expr(else_result)));
            }
            let doc = intersperse_line_nest(docs);
            bracket_doc(RcDoc::text("CASE"), doc, RcDoc::text("END"), RcDoc::line())
        }
        Expr::Cast { expr, data_type } => {
            // See AstDisplay for Expr for an explanation of this.
            let needs_wrap = !matches!(
                **expr,
                Expr::Nested(_)
                    | Expr::Value(_)
                    | Expr::Cast { .. }
                    | Expr::Function { .. }
                    | Expr::Identifier { .. }
                    | Expr::Collate { .. }
                    | Expr::HomogenizingFunction { .. }
                    | Expr::NullIf { .. }
            );
            let mut doc = doc_expr(expr);
            if needs_wrap {
                doc = bracket("(", doc, ")");
            }
            RcDoc::concat([doc, RcDoc::text(format!("::{}", data_type.to_ast_string()))])
        }
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
        Expr::NullIf { l_expr, r_expr } => bracket(
            "NULLIF (",
            comma_separate(doc_expr, [&**l_expr, &**r_expr]),
            ")",
        ),
        Expr::HomogenizingFunction { function, exprs } => {
            bracket(format!("{function}("), comma_separate(doc_expr, exprs), ")")
        }
        Expr::ArraySubquery(s) => bracket("ARRAY(", doc_query(s), ")"),
        Expr::ListSubquery(s) => bracket("LIST(", doc_query(s), ")"),
        Expr::Array(exprs) => bracket("ARRAY[", comma_separate(doc_expr, exprs), "]"),
        Expr::List(exprs) => bracket("LIST[", comma_separate(doc_expr, exprs), "]"),
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
                return doc_display_pass(v);
            }
            if v.filter.is_some() || v.over.is_some() || !order_by.is_empty() {
                return doc_display(v, "function filter or over or order by");
            }
            let special = match v.name.to_ast_string_stable().as_str() {
                r#""extract""# if v.args.len() == Some(2) => true,
                r#""position""# if v.args.len() == Some(2) => true,
                _ => false,
            };
            if special {
                return doc_display(v, "special function");
            }
            let name = format!(
                "{}({}",
                v.name.to_ast_string(),
                if v.distinct { "DISTINCT " } else { "" }
            );
            bracket(name, comma_separate(doc_expr, args), ")")
        }
    }
}
