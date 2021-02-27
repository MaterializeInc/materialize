// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data manipulation language (DML).
//!
//! This module houses the handlers for statements that manipulate data, like
//! `INSERT`, `SELECT`, `TAIL`, and `COPY`.

use std::collections::HashSet;
use std::convert::TryFrom;

use anyhow::bail;

use expr::MirRelationExpr;
use ore::collections::CollectionExt;
use repr::{RelationDesc, ScalarType};

use crate::ast::{
    CopyDirection, CopyRelation, CopyStatement, CopyTarget, CreateViewStatement, DeleteStatement,
    ExplainStage, ExplainStatement, Explainee, InsertStatement, Query, Raw, SelectStatement,
    Statement, TailStatement, UpdateStatement,
};
use crate::catalog::CatalogItemType;
use crate::plan::query;
use crate::plan::query::QueryLifetime;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{CopyFormat, Params, PeekWhen, Plan};

// TODO(benesch): currently, describing a `SELECT` or `INSERT` query
// plans the whole query to determine its shape and parameter types,
// and then throws away that plan. If we were smarter, we'd stash that
// plan somewhere so we don't have to recompute it when the query is
// executed.

pub fn describe_insert(
    scx: &StatementContext,
    InsertStatement {
        table_name,
        columns,
        source,
        ..
    }: InsertStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    query::plan_insert_query(scx, table_name, columns, source)?;
    Ok(StatementDesc::new(None))
}

pub fn plan_insert(
    scx: &StatementContext,
    InsertStatement {
        table_name,
        columns,
        source,
    }: InsertStatement<Raw>,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let (id, mut expr) = query::plan_insert_query(scx, table_name, columns, source)?;
    expr.bind_parameters(&params)?;
    let expr = expr.lower();

    Ok(Plan::Insert { id, values: expr })
}

pub fn describe_update(
    _: &StatementContext,
    _: UpdateStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    unsupported!("UPDATE statements")
}

pub fn plan_update(
    _: &StatementContext,
    _: UpdateStatement<Raw>,
    _: &Params,
) -> Result<Plan, anyhow::Error> {
    unsupported!("UPDATE statements")
}

pub fn describe_delete(
    _: &StatementContext,
    _: DeleteStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    unsupported!("DELETE statements")
}

pub fn plan_delete(
    _: &StatementContext,
    _: DeleteStatement<Raw>,
    _: &Params,
) -> Result<Plan, anyhow::Error> {
    unsupported!("DELETE statements")
}

pub fn describe_select(
    scx: &StatementContext,
    SelectStatement { query, .. }: SelectStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    let query::PlannedQuery { desc, .. } =
        query::plan_root_query(scx, query, QueryLifetime::OneShot)?;
    Ok(StatementDesc::new(Some(desc)))
}

pub fn plan_select(
    scx: &StatementContext,
    SelectStatement { query, as_of }: SelectStatement<Raw>,
    params: &Params,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, anyhow::Error> {
    let query::PlannedQuery {
        expr, finishing, ..
    } = plan_query(scx, query, params, QueryLifetime::OneShot)?;
    let when = match as_of.map(|e| query::eval_as_of(scx, e)).transpose()? {
        Some(ts) => PeekWhen::AtTimestamp(ts),
        None => PeekWhen::Immediately,
    };

    Ok(Plan::Peek {
        source: expr,
        when,
        finishing,
        copy_to,
    })
}

pub fn describe_explain(
    scx: &StatementContext,
    ExplainStatement {
        stage, explainee, ..
    }: ExplainStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(Some(RelationDesc::empty().with_column(
        match stage {
            ExplainStage::RawPlan => "Raw Plan",
            ExplainStage::DecorrelatedPlan => "Decorrelated Plan",
            ExplainStage::OptimizedPlan { .. } => "Optimized Plan",
        },
        ScalarType::String.nullable(false),
    )))
    .with_pgrepr_params(match explainee {
        Explainee::Query(q) => {
            describe_select(
                scx,
                SelectStatement {
                    query: q,
                    as_of: None,
                },
            )?
            .param_types
        }
        _ => vec![],
    }))
}

pub fn plan_explain(
    scx: &StatementContext,
    ExplainStatement {
        stage,
        explainee,
        options,
    }: ExplainStatement<Raw>,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let is_view = matches!(explainee, Explainee::View(_));
    let (scx, query) = match explainee {
        Explainee::View(name) => {
            let view = scx.resolve_item(name.clone())?;
            if view.item_type() != CatalogItemType::View {
                bail!("Expected {} to be a view, not a {}", name, view.item_type());
            }
            let parsed = crate::parse::parse(view.create_sql())
                .expect("Sql for existing view should be valid sql");
            let query = match parsed.into_last() {
                Statement::CreateView(CreateViewStatement { query, .. }) => query,
                _ => panic!("Sql for existing view should parse as a view"),
            };
            let scx = StatementContext {
                pcx: view.plan_cx(),
                catalog: scx.catalog,
                ids: HashSet::new(),
                param_types: scx.param_types.clone(),
            };
            (scx, query)
        }
        Explainee::Query(query) => (scx.clone(), query),
    };
    // Previouly we would bail here for ORDER BY and LIMIT; this has been relaxed to silently
    // report the plan without the ORDER BY and LIMIT decorations (which are done in post).
    let query::PlannedQuery {
        mut expr,
        desc,
        finishing,
        ..
    } = query::plan_root_query(&scx, query, QueryLifetime::OneShot)?;
    let finishing = if is_view {
        // views don't use a separate finishing
        expr.finish(finishing);
        None
    } else if finishing.is_trivial(desc.arity()) {
        None
    } else {
        Some(finishing)
    };
    expr.bind_parameters(&params)?;
    let decorrelated_expr = expr.clone().lower();
    Ok(Plan::ExplainPlan {
        raw_plan: expr,
        decorrelated_plan: decorrelated_expr,
        row_set_finishing: finishing,
        stage,
        options,
    })
}

/// Plans and decorrelates a `Query`. Like `query::plan_root_query`, but returns
/// an `::expr::MirRelationExpr`, which cannot include correlated expressions.
pub fn plan_query(
    scx: &StatementContext,
    query: Query<Raw>,
    params: &Params,
    lifetime: QueryLifetime,
) -> Result<query::PlannedQuery<MirRelationExpr>, anyhow::Error> {
    let query::PlannedQuery {
        mut expr,
        desc,
        finishing,
        depends_on,
    } = query::plan_root_query(scx, query, lifetime)?;
    expr.bind_parameters(&params)?;
    Ok(query::PlannedQuery {
        expr: expr.lower(),
        desc,
        finishing,
        depends_on,
    })
}

with_options! {
    struct TailOptions {
        snapshot: bool,
        progress: bool,
     }
}

pub fn describe_tail(
    scx: &StatementContext,
    TailStatement { name, options, .. }: TailStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    let sql_object = scx.resolve_item(name)?;
    let options = TailOptions::try_from(options)?;
    const MAX_U64_DIGITS: u8 = 20;
    let mut desc = RelationDesc::empty().with_column(
        "timestamp",
        ScalarType::Decimal(MAX_U64_DIGITS, 0).nullable(false),
    );
    if options.progress.unwrap_or(false) {
        desc = desc.with_column("progressed", ScalarType::Bool.nullable(false));
    }
    let desc = desc
        .with_column("diff", ScalarType::Int64.nullable(true))
        .concat(sql_object.desc()?.clone());
    Ok(StatementDesc::new(Some(desc)))
}

pub fn plan_tail(
    scx: &StatementContext,
    TailStatement {
        name,
        options,
        as_of,
    }: TailStatement<Raw>,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, anyhow::Error> {
    let entry = scx.resolve_item(name)?;
    let ts = as_of.map(|e| query::eval_as_of(scx, e)).transpose()?;
    let options = TailOptions::try_from(options)?;
    let desc = entry.desc()?.clone();

    match entry.item_type() {
        CatalogItemType::Table | CatalogItemType::Source | CatalogItemType::View => {
            Ok(Plan::Tail {
                id: entry.id(),
                ts,
                with_snapshot: options.snapshot.unwrap_or(true),
                copy_to,
                emit_progress: options.progress.unwrap_or(false),
                object_columns: entry.desc()?.arity(),
                desc,
            })
        }
        CatalogItemType::Func
        | CatalogItemType::Index
        | CatalogItemType::Sink
        | CatalogItemType::Type => bail!(
            "'{}' cannot be tailed because it is a {}",
            entry.name(),
            entry.item_type(),
        ),
    }
}

with_options! {
    struct CopyOptions {
        format: String,
    }
}

pub fn describe_copy(
    scx: &StatementContext,
    CopyStatement { relation, .. }: CopyStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(match relation {
        CopyRelation::Table { .. } => bail!("unsupported COPY relation {:?}", relation),
        CopyRelation::Select(stmt) => describe_select(scx, stmt)?,
        CopyRelation::Tail(stmt) => describe_tail(scx, stmt)?,
    }
    .with_is_copy())
}

pub fn plan_copy(
    scx: &StatementContext,
    CopyStatement {
        relation,
        direction,
        target,
        options,
    }: CopyStatement<Raw>,
) -> Result<Plan, anyhow::Error> {
    let options = CopyOptions::try_from(options)?;
    let format = if let Some(format) = options.format {
        match format.to_lowercase().as_str() {
            "text" => CopyFormat::Text,
            "csv" => CopyFormat::Csv,
            "binary" => CopyFormat::Binary,
            _ => bail!("unknown FORMAT: {}", format),
        }
    } else {
        CopyFormat::Text
    };
    match (&direction, &target) {
        (CopyDirection::To, CopyTarget::Stdout) => match relation {
            CopyRelation::Table { .. } => bail!("table with COPY TO unsupported"),
            CopyRelation::Select(stmt) => {
                Ok(plan_select(scx, stmt, &Params::empty(), Some(format))?)
            }
            CopyRelation::Tail(stmt) => Ok(plan_tail(scx, stmt, Some(format))?),
        },
        _ => bail!("COPY {} {} not supported", direction, target),
    }
}
