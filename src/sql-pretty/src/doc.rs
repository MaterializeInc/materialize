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
use crate::{PrettyConfig, TAB};

// Use when we don't know what to do.
pub(crate) fn doc_display<'a, T: AstDisplay>(
    v: &T,
    config: PrettyConfig,
    _debug: &str,
) -> RcDoc<'a, ()> {
    #[cfg(test)]
    eprintln!(
        "UNKNOWN PRETTY TYPE in {}: {}, {}",
        _debug,
        std::any::type_name::<T>(),
        v.to_ast_string_simple()
    );
    doc_display_pass(v, config)
}

// Use when the AstDisplay trait is what we want.
fn doc_display_pass<'a, T: AstDisplay>(v: &T, config: PrettyConfig) -> RcDoc<'a, ()> {
    RcDoc::text(v.to_ast_string(config.format_mode))
}

fn doc_display_pass_mapper<T: AstDisplay>(
    config: PrettyConfig,
) -> impl for<'b> FnMut(&'b T) -> RcDoc<'b, ()> {
    move |v| doc_display_pass(v, config)
}

pub(crate) fn doc_create_source<T: AstInfo>(
    v: &CreateSourceStatement<T>,
    config: PrettyConfig,
) -> RcDoc {
    let mut docs = Vec::new();
    let title = format!(
        "CREATE SOURCE{}",
        if v.if_not_exists {
            " IF NOT EXISTS"
        } else {
            ""
        }
    );
    let mut doc = doc_display_pass(&v.name, config);
    let mut names = Vec::new();
    names.extend(v.col_names.iter().map(doc_display_pass_mapper(config)));
    names.extend(v.key_constraint.iter().map(doc_display_pass_mapper(config)));
    if !names.is_empty() {
        doc = nest(doc, bracket("(", comma_separated(names), ")"));
    }
    docs.push(nest_title(title, doc));
    if let Some(cluster) = &v.in_cluster {
        docs.push(nest_title("IN CLUSTER", doc_display_pass(cluster, config)));
    }
    docs.push(nest_title("FROM", doc_display_pass(&v.connection, config)));
    if let Some(format) = &v.format {
        docs.push(doc_format_specifier(format, config));
    }
    if !v.include_metadata.is_empty() {
        docs.push(nest_title(
            "INCLUDE",
            comma_separate(doc_display_pass_mapper(config), &v.include_metadata),
        ));
    }
    if let Some(envelope) = &v.envelope {
        docs.push(nest_title("ENVELOPE", doc_display_pass(envelope, config)));
    }
    if let Some(references) = &v.external_references {
        docs.push(doc_external_references(references, config));
    }
    if let Some(progress) = &v.progress_subsource {
        docs.push(nest_title(
            "EXPOSE PROGRESS AS",
            doc_display_pass(progress, config),
        ));
    }
    if !v.with_options.is_empty() {
        docs.push(bracket(
            "WITH (",
            comma_separate(doc_display_pass_mapper(config), &v.with_options),
            ")",
        ));
    }
    RcDoc::intersperse(docs, Doc::line()).group()
}

fn doc_format_specifier<T: AstInfo>(v: &FormatSpecifier<T>, config: PrettyConfig) -> RcDoc {
    match v {
        FormatSpecifier::Bare(format) => nest_title("FORMAT", doc_display_pass(format, config)),
        FormatSpecifier::KeyValue { key, value } => {
            let docs = vec![
                nest_title("KEY FORMAT", doc_display_pass(key, config)),
                nest_title("VALUE FORMAT", doc_display_pass(value, config)),
            ];
            RcDoc::intersperse(docs, Doc::line()).group()
        }
    }
}

fn doc_external_references(v: &ExternalReferences, config: PrettyConfig) -> RcDoc {
    match v {
        ExternalReferences::SubsetTables(subsources) => bracket(
            "FOR TABLES (",
            comma_separate(doc_display_pass_mapper(config), subsources),
            ")",
        ),
        ExternalReferences::SubsetSchemas(schemas) => bracket(
            "FOR SCHEMAS (",
            comma_separate(doc_display_pass_mapper(config), schemas),
            ")",
        ),
        ExternalReferences::All => RcDoc::text("FOR ALL TABLES"),
    }
}

pub(crate) fn doc_copy<T: AstInfo>(v: &CopyStatement<T>, config: PrettyConfig) -> RcDoc {
    let relation = match &v.relation {
        CopyRelation::Named { name, columns } => {
            let mut relation = doc_display_pass(name, config);
            if !columns.is_empty() {
                relation = bracket_doc(
                    nest(relation, RcDoc::text("(")),
                    comma_separate(doc_display_pass_mapper(config), columns),
                    RcDoc::text(")"),
                    RcDoc::line_(),
                );
            }
            RcDoc::concat([RcDoc::text("COPY "), relation])
        }
        CopyRelation::Select(query) => bracket("COPY (", doc_select_statement(query, config), ")"),
        CopyRelation::Subscribe(query) => bracket("COPY (", doc_subscribe(query, config), ")"),
    };
    let mut docs = vec![
        relation,
        RcDoc::concat([
            doc_display_pass(&v.direction, config),
            RcDoc::text(" "),
            doc_display_pass(&v.target, config),
        ]),
    ];
    if !v.options.is_empty() {
        docs.push(bracket(
            "WITH (",
            comma_separate(doc_display_pass_mapper(config), &v.options),
            ")",
        ));
    }
    RcDoc::intersperse(docs, Doc::line()).group()
}

pub(crate) fn doc_subscribe<T: AstInfo>(v: &SubscribeStatement<T>, config: PrettyConfig) -> RcDoc {
    let doc = match &v.relation {
        SubscribeRelation::Name(name) => nest_title("SUBSCRIBE", doc_display_pass(name, config)),
        SubscribeRelation::Query(query) => bracket("SUBSCRIBE (", doc_query(query, config), ")"),
    };
    let mut docs = vec![doc];
    if !v.options.is_empty() {
        docs.push(bracket(
            "WITH (",
            comma_separate(doc_display_pass_mapper(config), &v.options),
            ")",
        ));
    }
    if let Some(as_of) = &v.as_of {
        docs.push(doc_as_of(as_of, config));
    }
    if let Some(up_to) = &v.up_to {
        docs.push(nest_title("UP TO", doc_expr(up_to, config)));
    }
    match &v.output {
        SubscribeOutput::Diffs => {}
        SubscribeOutput::WithinTimestampOrderBy { order_by } => {
            docs.push(nest_title(
                "WITHIN TIMESTAMP ORDER BY ",
                comma_separate(doc_order_by_expr_mapper(config), order_by),
            ));
        }
        SubscribeOutput::EnvelopeUpsert { key_columns } => {
            docs.push(bracket(
                "ENVELOPE UPSERT (KEY (",
                comma_separate(doc_display_pass_mapper(config), key_columns),
                "))",
            ));
        }
        SubscribeOutput::EnvelopeDebezium { key_columns } => {
            docs.push(bracket(
                "ENVELOPE DEBEZIUM (KEY (",
                comma_separate(doc_display_pass_mapper(config), key_columns),
                "))",
            ));
        }
    }
    RcDoc::intersperse(docs, Doc::line()).group()
}

fn doc_as_of<T: AstInfo>(v: &AsOf<T>, config: PrettyConfig) -> RcDoc {
    let (title, expr) = match v {
        AsOf::At(expr) => ("AS OF", expr),
        AsOf::AtLeast(expr) => ("AS OF AT LEAST", expr),
    };
    nest_title(title, doc_expr(expr, config))
}

pub(crate) fn doc_create_view<T: AstInfo>(
    v: &CreateViewStatement<T>,
    config: PrettyConfig,
) -> RcDoc {
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
    docs.push(doc_view_definition(&v.definition, config));
    intersperse_line_nest(docs)
}

pub(crate) fn doc_create_materialized_view<T: AstInfo>(
    v: &CreateMaterializedViewStatement<T>,
    config: PrettyConfig,
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
            comma_separate(doc_display_pass_mapper(config), &v.columns),
            ")",
        ));
    }
    if let Some(cluster) = &v.in_cluster {
        docs.push(RcDoc::text(format!(
            "IN CLUSTER {}",
            cluster.to_ast_string_simple()
        )));
    }
    if !v.with_options.is_empty() {
        docs.push(bracket(
            "WITH (",
            comma_separate(doc_display_pass_mapper(config), &v.with_options),
            ")",
        ));
    }
    docs.push(nest_title("AS", doc_query(&v.query, config)));
    intersperse_line_nest(docs)
}

fn doc_view_definition<T: AstInfo>(v: &ViewDefinition<T>, config: PrettyConfig) -> RcDoc {
    let mut docs = vec![RcDoc::text(v.name.to_string())];
    if !v.columns.is_empty() {
        docs.push(bracket(
            "(",
            comma_separate(doc_display_pass_mapper(config), &v.columns),
            ")",
        ));
    }
    docs.push(nest_title("AS", doc_query(&v.query, config)));
    RcDoc::intersperse(docs, Doc::line()).group()
}

pub(crate) fn doc_insert<T: AstInfo>(v: &InsertStatement<T>, config: PrettyConfig) -> RcDoc {
    let mut first = vec![RcDoc::text(format!(
        "INSERT INTO {}",
        v.table_name.to_ast_string_simple()
    ))];
    if !v.columns.is_empty() {
        first.push(bracket(
            "(",
            comma_separate(doc_display_pass_mapper(config), &v.columns),
            ")",
        ));
    }
    let sources = match &v.source {
        InsertSource::Query(query) => doc_query(query, config),
        _ => doc_display(&v.source, config, "insert source"),
    };
    let mut doc = intersperse_line_nest([intersperse_line_nest(first), sources]);
    if !v.returning.is_empty() {
        doc = nest(
            doc,
            nest_title(
                "RETURNING",
                comma_separate(doc_display_pass_mapper(config), &v.returning),
            ),
        )
    }
    doc
}

pub(crate) fn doc_select_statement<T: AstInfo>(
    v: &SelectStatement<T>,
    config: PrettyConfig,
) -> RcDoc {
    let mut doc = doc_query(&v.query, config);
    if let Some(as_of) = &v.as_of {
        doc = intersperse_line_nest([doc, doc_as_of(as_of, config)]);
    }
    doc.group()
}

fn doc_order_by<T: AstInfo>(v: &[OrderByExpr<T>], config: PrettyConfig) -> RcDoc {
    title_comma_separate("ORDER BY", doc_order_by_expr_mapper(config), v)
}

fn doc_order_by_expr<T: AstInfo>(v: &OrderByExpr<T>, config: PrettyConfig) -> RcDoc {
    let doc = doc_expr(&v.expr, config);
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

fn doc_order_by_expr_mapper<T: AstInfo>(
    config: PrettyConfig,
) -> impl for<'b> FnMut(&'b OrderByExpr<T>) -> RcDoc<'b, ()> {
    move |v| doc_order_by_expr(v, config)
}

fn doc_query<T: AstInfo>(v: &Query<T>, config: PrettyConfig) -> RcDoc {
    let mut docs = vec![];
    if !v.ctes.is_empty() {
        match &v.ctes {
            CteBlock::Simple(ctes) => {
                docs.push(title_comma_separate("WITH", doc_cte_mapper(config), ctes))
            }
            CteBlock::MutuallyRecursive(mutrec) => {
                let mut doc = RcDoc::text("WITH MUTUALLY RECURSIVE");
                if !mutrec.options.is_empty() {
                    doc = nest(
                        doc,
                        bracket(
                            "(",
                            comma_separate(doc_display_pass_mapper(config), &mutrec.options),
                            ")",
                        ),
                    );
                }
                docs.push(nest(
                    doc,
                    comma_separate(doc_mutually_recursive_mapper(config), &mutrec.ctes),
                ));
            }
        }
    }
    docs.push(doc_set_expr(&v.body, config));
    if !v.order_by.is_empty() {
        docs.push(doc_order_by(&v.order_by, config));
    }

    let offset = if let Some(offset) = &v.offset {
        vec![RcDoc::concat([nest_title(
            "OFFSET",
            doc_expr(offset, config),
        )])]
    } else {
        vec![]
    };

    if let Some(limit) = &v.limit {
        if limit.with_ties {
            docs.extend(offset);
            docs.push(RcDoc::concat([
                RcDoc::text("FETCH FIRST "),
                doc_expr(&limit.quantity, config),
                RcDoc::text(" ROWS WITH TIES"),
            ]));
        } else {
            docs.push(nest_title("LIMIT", doc_expr(&limit.quantity, config)));
            docs.extend(offset);
        }
    } else {
        docs.extend(offset);
    }

    RcDoc::intersperse(docs, Doc::line()).group()
}

fn doc_cte<T: AstInfo>(v: &Cte<T>, config: PrettyConfig) -> RcDoc {
    RcDoc::concat([
        RcDoc::text(format!("{} AS", v.alias)),
        RcDoc::line(),
        bracket("(", doc_query(&v.query, config), ")"),
    ])
}

fn doc_cte_mapper<T: AstInfo>(
    config: PrettyConfig,
) -> impl for<'b> FnMut(&'b Cte<T>) -> RcDoc<'b, ()> {
    move |v| doc_cte(v, config)
}

fn doc_mutually_recursive<T: AstInfo>(v: &CteMutRec<T>, config: PrettyConfig) -> RcDoc {
    let mut docs = Vec::new();
    if !v.columns.is_empty() {
        docs.push(bracket(
            "(",
            comma_separate(doc_display_pass_mapper(config), &v.columns),
            ")",
        ));
    }
    docs.push(bracket("AS (", doc_query(&v.query, config), ")"));
    nest(
        doc_display_pass(&v.name, config),
        RcDoc::intersperse(docs, Doc::line()).group(),
    )
}

fn doc_mutually_recursive_mapper<T: AstInfo>(
    config: PrettyConfig,
) -> impl for<'b> FnMut(&'b CteMutRec<T>) -> RcDoc<'b, ()> {
    move |v| doc_mutually_recursive(v, config)
}

fn doc_set_expr<T: AstInfo>(v: &SetExpr<T>, config: PrettyConfig) -> RcDoc {
    match v {
        SetExpr::Select(v) => doc_select(v, config),
        SetExpr::Query(v) => bracket("(", doc_query(v, config), ")"),
        SetExpr::SetOperation {
            op,
            all,
            left,
            right,
        } => {
            let all_str = if *all { " ALL" } else { "" };
            RcDoc::concat([
                doc_set_expr(left, config),
                RcDoc::line(),
                RcDoc::concat([
                    RcDoc::text(format!("{}{}", op, all_str)),
                    RcDoc::line(),
                    doc_set_expr(right, config),
                ])
                .nest(TAB)
                .group(),
            ])
        }
        SetExpr::Values(v) => doc_values(v, config),
        SetExpr::Show(v) => doc_display(v, config, "SHOW"),
        SetExpr::Table(v) => nest(RcDoc::text("TABLE"), doc_display_pass(v, config)),
    }
    .group()
}

fn doc_values<T: AstInfo>(v: &Values<T>, config: PrettyConfig) -> RcDoc {
    let rows =
        v.0.iter()
            .map(|row| bracket("(", comma_separate(doc_expr_mapper(config), row), ")"));
    RcDoc::concat([RcDoc::text("VALUES"), RcDoc::line(), comma_separated(rows)])
        .nest(TAB)
        .group()
}

fn doc_table_with_joins<T: AstInfo>(v: &TableWithJoins<T>, config: PrettyConfig) -> RcDoc {
    let mut docs = vec![doc_table_factor(&v.relation, config)];
    for j in &v.joins {
        docs.push(doc_join(j, config));
    }
    intersperse_line_nest(docs)
}

fn doc_table_with_joins_mapper<T: AstInfo>(
    config: PrettyConfig,
) -> impl for<'b> FnMut(&'b TableWithJoins<T>) -> RcDoc<'b, ()> {
    move |v| doc_table_with_joins(v, config)
}

fn doc_join<T: AstInfo>(v: &Join<T>, config: PrettyConfig) -> RcDoc {
    let (constraint, name) = match &v.join_operator {
        JoinOperator::Inner(constraint) => (constraint, "JOIN"),
        JoinOperator::FullOuter(constraint) => (constraint, "FULL JOIN"),
        JoinOperator::LeftOuter(constraint) => (constraint, "LEFT JOIN"),
        JoinOperator::RightOuter(constraint) => (constraint, "RIGHT JOIN"),
        _ => return doc_display(v, config, "join operator"),
    };
    let constraint = match constraint {
        JoinConstraint::On(expr) => nest_title("ON", doc_expr(expr, config)),
        JoinConstraint::Using { columns, alias } => {
            let mut doc = bracket(
                "USING(",
                comma_separate(doc_display_pass_mapper(config), columns),
                ")",
            );
            if let Some(alias) = alias {
                doc = nest(doc, nest_title("AS", doc_display_pass(alias, config)));
            }
            doc
        }
        _ => return doc_display(v, config, "join constraint"),
    };
    intersperse_line_nest([
        RcDoc::text(name),
        doc_table_factor(&v.relation, config),
        constraint,
    ])
}

fn doc_table_factor<T: AstInfo>(v: &TableFactor<T>, config: PrettyConfig) -> RcDoc {
    match v {
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            let prefix = if *lateral { "LATERAL (" } else { "(" };
            let mut docs = vec![bracket(prefix, doc_query(subquery, config), ")")];
            if let Some(alias) = alias {
                docs.push(RcDoc::text(format!("AS {}", alias)));
            }
            intersperse_line_nest(docs)
        }
        TableFactor::NestedJoin { join, alias } => {
            let mut doc = bracket("(", doc_table_with_joins(join, config), ")");
            if let Some(alias) = alias {
                doc = nest(doc, RcDoc::text(format!("AS {}", alias)));
            }
            doc
        }
        TableFactor::Table { name, alias } => {
            let mut doc = doc_display_pass(name, config);
            if let Some(alias) = alias {
                doc = nest(doc, RcDoc::text(format!("AS {}", alias)));
            }
            doc
        }
        _ => doc_display(v, config, "table factor variant"),
    }
}

fn doc_distinct<T: AstInfo>(v: &Distinct<T>, config: PrettyConfig) -> RcDoc {
    match v {
        Distinct::EntireRow => RcDoc::text("DISTINCT"),
        Distinct::On(cols) => bracket(
            "DISTINCT ON (",
            comma_separate(doc_expr_mapper(config), cols),
            ")",
        ),
    }
}

fn doc_select<T: AstInfo>(v: &Select<T>, config: PrettyConfig) -> RcDoc {
    let mut docs = vec![];
    let mut select = RcDoc::text("SELECT");
    if let Some(distinct) = &v.distinct {
        select = nest(select, doc_distinct(distinct, config));
    }
    docs.push(nest_comma_separate(
        select,
        doc_select_item_mapper(config),
        &v.projection,
    ));
    if !v.from.is_empty() {
        docs.push(title_comma_separate(
            "FROM",
            doc_table_with_joins_mapper(config),
            &v.from,
        ));
    }
    if let Some(selection) = &v.selection {
        docs.push(nest_title("WHERE", doc_expr(selection, config)));
    }
    if !v.group_by.is_empty() {
        docs.push(title_comma_separate(
            "GROUP BY",
            doc_expr_mapper(config),
            &v.group_by,
        ));
    }
    if let Some(having) = &v.having {
        docs.push(nest_title("HAVING", doc_expr(having, config)));
    }
    if let Some(qualify) = &v.qualify {
        docs.push(nest_title("QUALIFY", doc_expr(qualify, config)));
    }
    if !v.options.is_empty() {
        docs.push(bracket(
            "OPTIONS (",
            comma_separate(doc_display_pass_mapper(config), &v.options),
            ")",
        ));
    }
    RcDoc::intersperse(docs, Doc::line()).group()
}

fn doc_select_item<T: AstInfo>(v: &SelectItem<T>, config: PrettyConfig) -> RcDoc {
    match v {
        SelectItem::Expr { expr, alias } => {
            let mut doc = doc_expr(expr, config);
            if let Some(alias) = alias {
                doc = nest(
                    doc,
                    RcDoc::concat([RcDoc::text("AS "), doc_display_pass(alias, config)]),
                );
            }
            doc
        }
        SelectItem::Wildcard => doc_display_pass(v, config),
    }
}

fn doc_select_item_mapper<T: AstInfo>(
    config: PrettyConfig,
) -> impl for<'b> FnMut(&'b SelectItem<T>) -> RcDoc<'b, ()> {
    move |v| doc_select_item(v, config)
}

pub fn doc_expr<T: AstInfo>(v: &Expr<T>, config: PrettyConfig) -> RcDoc {
    match v {
        Expr::Op { op, expr1, expr2 } => {
            if let Some(expr2) = expr2 {
                RcDoc::concat([
                    doc_expr(expr1, config),
                    RcDoc::line(),
                    RcDoc::text(format!("{} ", op)),
                    doc_expr(expr2, config).nest(TAB),
                ])
            } else {
                RcDoc::concat([RcDoc::text(format!("{} ", op)), doc_expr(expr1, config)])
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
                docs.push(doc_expr(operand, config));
            }
            for (c, r) in conditions.iter().zip(results) {
                let when = nest_title("WHEN", doc_expr(c, config));
                let then = nest_title("THEN", doc_expr(r, config));
                docs.push(nest(when, then));
            }
            if let Some(else_result) = else_result {
                docs.push(nest_title("ELSE", doc_expr(else_result, config)));
            }
            let doc = intersperse_line_nest(docs);
            bracket_doc(RcDoc::text("CASE"), doc, RcDoc::text("END"), RcDoc::line())
        }
        Expr::Cast { expr, data_type } => {
            let doc = doc_expr(expr, config);
            RcDoc::concat([
                doc,
                RcDoc::text(format!("::{}", data_type.to_ast_string_simple())),
            ])
        }
        Expr::Nested(ast) => bracket("(", doc_expr(ast, config), ")"),
        Expr::Function(fun) => doc_function(fun, config),
        Expr::Subquery(ast) => bracket("(", doc_query(ast, config), ")"),
        Expr::Identifier(_)
        | Expr::Value(_)
        | Expr::QualifiedWildcard(_)
        | Expr::WildcardAccess(_)
        | Expr::FieldAccess { .. } => doc_display_pass(v, config),
        Expr::And { left, right } => bracket_doc(
            doc_expr(left, config),
            RcDoc::text("AND"),
            doc_expr(right, config),
            RcDoc::line(),
        ),
        Expr::Or { left, right } => bracket_doc(
            doc_expr(left, config),
            RcDoc::text("OR"),
            doc_expr(right, config),
            RcDoc::line(),
        ),
        Expr::Exists(s) => bracket("EXISTS (", doc_query(s, config), ")"),
        Expr::IsExpr {
            expr,
            negated,
            construct,
        } => bracket_doc(
            doc_expr(expr, config),
            RcDoc::text(if *negated { "IS NOT" } else { "IS" }),
            doc_display_pass(construct, config),
            RcDoc::line(),
        ),
        Expr::Not { expr } => {
            RcDoc::concat([RcDoc::text("NOT"), RcDoc::line(), doc_expr(expr, config)])
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => RcDoc::intersperse(
            [
                doc_expr(expr, config),
                RcDoc::text(if *negated { "NOT BETWEEN" } else { "BETWEEN" }),
                RcDoc::intersperse(
                    [
                        doc_expr(low, config),
                        RcDoc::text("AND"),
                        doc_expr(high, config),
                    ],
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
                doc_expr(expr, config),
                RcDoc::text(if *negated { "NOT IN (" } else { "IN (" }),
                doc_query(subquery, config),
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
                doc_expr(expr, config),
                RcDoc::text(if *negated { "NOT IN (" } else { "IN (" }),
                comma_separate(doc_expr_mapper(config), list),
                RcDoc::text(")"),
            ],
            RcDoc::line(),
        ),
        Expr::Row { exprs } => bracket("ROW(", comma_separate(doc_expr_mapper(config), exprs), ")"),
        Expr::NullIf { l_expr, r_expr } => bracket(
            "NULLIF (",
            comma_separate(doc_expr_mapper(config), [&**l_expr, &**r_expr]),
            ")",
        ),
        Expr::HomogenizingFunction { function, exprs } => bracket(
            format!("{function}("),
            comma_separate(doc_expr_mapper(config), exprs),
            ")",
        ),
        Expr::ArraySubquery(s) => bracket("ARRAY(", doc_query(s, config), ")"),
        Expr::ListSubquery(s) => bracket("LIST(", doc_query(s, config), ")"),
        Expr::Array(exprs) => bracket(
            "ARRAY[",
            comma_separate(doc_expr_mapper(config), exprs),
            "]",
        ),
        Expr::List(exprs) => bracket("LIST[", comma_separate(doc_expr_mapper(config), exprs), "]"),
        _ => doc_display(v, config, "expr variant"),
    }
    .group()
}

fn doc_expr_mapper<T: AstInfo>(
    config: PrettyConfig,
) -> impl for<'b> FnMut(&'b Expr<T>) -> RcDoc<'b, ()> {
    move |v| doc_expr(v, config)
}

fn doc_function<T: AstInfo>(v: &Function<T>, config: PrettyConfig) -> RcDoc {
    match &v.args {
        FunctionArgs::Star => doc_display_pass(v, config),
        FunctionArgs::Args { args, order_by } => {
            if args.is_empty() {
                // Nullary, don't allow newline between parens, so just delegate.
                return doc_display_pass(v, config);
            }
            if v.filter.is_some() || v.over.is_some() || !order_by.is_empty() {
                return doc_display(v, config, "function filter or over or order by");
            }
            let special = match v.name.to_ast_string_stable().as_str() {
                r#""extract""# if v.args.len() == Some(2) => true,
                r#""position""# if v.args.len() == Some(2) => true,
                _ => false,
            };
            if special {
                return doc_display(v, config, "special function");
            }
            let name = format!(
                "{}({}",
                v.name.to_ast_string_simple(),
                if v.distinct { "DISTINCT " } else { "" }
            );
            bracket(name, comma_separate(doc_expr_mapper(config), args), ")")
        }
    }
}
