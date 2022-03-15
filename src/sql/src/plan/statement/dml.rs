// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use std::collections::HashMap;

use anyhow::bail;

use mz_expr::MirRelationExpr;
use mz_ore::collections::CollectionExt;
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::{RelationDesc, ScalarType};
use mz_sql_parser::ast::AstInfo;

use crate::ast::{
    CopyDirection, CopyRelation, CopyStatement, CopyTarget, CreateViewStatement, DeleteStatement,
    ExplainStage, ExplainStatement, Explainee, Ident, InsertStatement, Query, SelectStatement,
    Statement, TailRelation, TailStatement, UpdateStatement, ViewDefinition,
};
use crate::catalog::CatalogItemType;
use crate::names::{resolve_names, Aug, ResolvedObjectName};
use crate::plan::query::QueryLifetime;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{query, QueryContext};
use crate::plan::{
    CopyFormat, CopyFromPlan, CopyParams, ExplainPlan, InsertPlan, MutationKind, Params, PeekPlan,
    Plan, ReadThenWritePlan, TailFrom, TailPlan,
};

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
    }: InsertStatement<Aug>,
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
    }: InsertStatement<Aug>,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let (id, mut expr) = query::plan_insert_query(scx, table_name, columns, source)?;
    expr.bind_parameters(&params)?;
    let expr = expr.optimize_and_lower(&scx.into())?;

    Ok(Plan::Insert(InsertPlan { id, values: expr }))
}

pub fn describe_delete(
    scx: &StatementContext,
    stmt: DeleteStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    query::plan_delete_query(scx, stmt)?;
    Ok(StatementDesc::new(None))
}

pub fn plan_delete(
    scx: &StatementContext,
    stmt: DeleteStatement<Aug>,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let rtw_plan = query::plan_delete_query(scx, stmt)?;
    plan_read_then_write(MutationKind::Delete, scx, params, rtw_plan)
}

pub fn describe_update(
    scx: &StatementContext,
    stmt: UpdateStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    query::plan_update_query(scx, stmt)?;
    Ok(StatementDesc::new(None))
}

pub fn plan_update(
    scx: &StatementContext,
    stmt: UpdateStatement<Aug>,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let rtw_plan = query::plan_update_query(scx, stmt)?;
    plan_read_then_write(MutationKind::Update, scx, params, rtw_plan)
}

pub fn plan_read_then_write(
    kind: MutationKind,
    scx: &StatementContext,
    params: &Params,
    query::ReadThenWritePlan {
        id,
        mut selection,
        finishing,
        assignments,
    }: query::ReadThenWritePlan,
) -> Result<Plan, anyhow::Error> {
    selection.bind_parameters(&params)?;
    let selection = selection.optimize_and_lower(&scx.into())?;
    let mut assignments_outer = HashMap::new();
    for (idx, mut set) in assignments {
        set.bind_parameters(&params)?;
        let set = set.lower_uncorrelated()?;
        assignments_outer.insert(idx, set);
    }

    Ok(Plan::ReadThenWrite(ReadThenWritePlan {
        id,
        selection,
        finishing,
        assignments: assignments_outer,
        kind,
    }))
}

pub fn describe_select(
    scx: &StatementContext,
    stmt: SelectStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    let query::PlannedQuery { desc, .. } =
        query::plan_root_query(scx, stmt.query, QueryLifetime::OneShot(scx.pcx()?))?;
    Ok(StatementDesc::new(Some(desc)))
}

pub fn plan_select(
    scx: &StatementContext,
    SelectStatement { query, as_of }: SelectStatement<Aug>,
    params: &Params,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, anyhow::Error> {
    let query::PlannedQuery {
        expr, finishing, ..
    } = plan_query(scx, query, params, QueryLifetime::OneShot(scx.pcx()?))?;
    let when = query::plan_as_of(scx, as_of)?;
    Ok(Plan::Peek(PeekPlan {
        source: expr,
        when,
        finishing,
        copy_to,
    }))
}

pub fn describe_explain(
    scx: &StatementContext,
    ExplainStatement {
        stage, explainee, ..
    }: ExplainStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(Some(RelationDesc::empty().with_column(
        match stage {
            ExplainStage::RawPlan => "Raw Plan",
            ExplainStage::QueryGraph => "Query Graph",
            ExplainStage::OptimizedQueryGraph => "Optimized Query Graph",
            ExplainStage::DecorrelatedPlan => "Decorrelated Plan",
            ExplainStage::OptimizedPlan { .. } => "Optimized Plan",
            ExplainStage::PhysicalPlan => "Physical Plan",
            ExplainStage::Timestamp => "Timestamp",
        },
        ScalarType::String.nullable(false),
    )))
    .with_params(match explainee {
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
    }: ExplainStatement<Aug>,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let is_view = matches!(explainee, Explainee::View(_));
    let query = match explainee {
        Explainee::View(name) => {
            let view = scx.get_item_by_name(&name)?;
            if view.item_type() != CatalogItemType::View {
                bail!("Expected {} to be a view, not a {}", name, view.item_type());
            }
            let parsed = crate::parse::parse(view.create_sql())
                .expect("Sql for existing view should be valid sql");
            let query = match parsed.into_last() {
                Statement::CreateView(CreateViewStatement {
                    definition: ViewDefinition { query, .. },
                    ..
                }) => query,
                _ => panic!("Sql for existing view should parse as a view"),
            };
            let mut qcx = QueryContext::root(&scx, QueryLifetime::OneShot(scx.pcx().unwrap()));
            resolve_names(&mut qcx, query)?
        }
        Explainee::Query(query) => query,
    };
    // Previously we would bail here for ORDER BY and LIMIT; this has been relaxed to silently
    // report the plan without the ORDER BY and LIMIT decorations (which are done in post).
    let query::PlannedQuery {
        mut expr,
        desc,
        finishing,
        ..
    } = query::plan_root_query(&scx, query, QueryLifetime::OneShot(scx.pcx()?))?;
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
    Ok(Plan::Explain(ExplainPlan {
        raw_plan: expr,
        row_set_finishing: finishing,
        stage,
        options,
    }))
}

/// Plans and decorrelates a `Query`. Like `query::plan_root_query`, but returns
/// an `mz_expr::MirRelationExpr`, which cannot include correlated expressions.
pub fn plan_query(
    scx: &StatementContext,
    query: Query<Aug>,
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
        expr: expr.optimize_and_lower(&scx.into())?,
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
    stmt: TailStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    let relation_desc = match stmt.relation {
        TailRelation::Name(name) => scx.get_item_by_name(&name)?.desc()?.clone(),
        TailRelation::Query(query) => {
            let query::PlannedQuery { desc, .. } =
                query::plan_root_query(scx, query, QueryLifetime::OneShot(scx.pcx()?))?;
            desc
        }
    };
    let options = TailOptions::try_from(stmt.options)?;
    let progress = options.progress.unwrap_or(false);
    let mut desc = RelationDesc::empty().with_column(
        "mz_timestamp",
        ScalarType::Numeric {
            max_scale: Some(NumericMaxScale::ZERO),
        }
        .nullable(false),
    );
    if progress {
        desc = desc.with_column("mz_progressed", ScalarType::Bool.nullable(false));
    }
    desc = desc.with_column("mz_diff", ScalarType::Int64.nullable(true));
    for (name, mut ty) in relation_desc.into_iter() {
        if progress {
            ty.nullable = true;
        }
        desc = desc.with_column(name, ty);
    }
    return Ok(StatementDesc::new(Some(desc)));
}

pub fn plan_tail(
    scx: &StatementContext,
    TailStatement {
        relation,
        options,
        as_of,
    }: TailStatement<Aug>,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, anyhow::Error> {
    let from = match relation {
        TailRelation::Name(name) => {
            let entry = scx.get_item_by_name(&name)?;
            match entry.item_type() {
                CatalogItemType::Table | CatalogItemType::Source | CatalogItemType::View => {
                    TailFrom::Id(entry.id())
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
        TailRelation::Query(query) => {
            // There's no way to apply finishing operations to a `TAIL`
            // directly. So we wrap the query in another query so that the
            // user-supplied query is planned as a subquery whose `ORDER
            // BY`/`LIMIT`/`OFFSET` clauses turn into a TopK operator.
            let query = Query::query(query);
            let query = plan_query(
                scx,
                query,
                &Params::empty(),
                QueryLifetime::OneShot(scx.pcx()?),
            )?;
            assert!(query.finishing.is_trivial(query.desc.arity()));
            TailFrom::Query {
                expr: query.expr,
                desc: query.desc,
                depends_on: query.depends_on,
            }
        }
    };

    let when = query::plan_as_of(scx, as_of)?;
    let options = TailOptions::try_from(options)?;
    Ok(Plan::Tail(TailPlan {
        from,
        when,
        with_snapshot: options.snapshot.unwrap_or(true),
        copy_to,
        emit_progress: options.progress.unwrap_or(false),
    }))
}

pub fn describe_table(
    scx: &StatementContext,
    table_name: <Aug as AstInfo>::ObjectName,
    columns: Vec<Ident>,
) -> Result<StatementDesc, anyhow::Error> {
    let (_, desc, _) = query::plan_copy_from(scx, table_name, columns)?;
    Ok(StatementDesc::new(Some(desc)))
}

with_options! {
    struct CopyOptions {
        format: String,
        delimiter: String,
        null: String,
        escape: String,
        quote: String,
        header: bool,
    }
}

pub fn describe_copy(
    scx: &StatementContext,
    CopyStatement { relation, .. }: CopyStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(match relation {
        CopyRelation::Table { name, columns } => describe_table(scx, name, columns)?,
        CopyRelation::Select(stmt) => describe_select(scx, stmt)?,
        CopyRelation::Tail(stmt) => describe_tail(scx, stmt)?,
    }
    .with_is_copy())
}

fn plan_copy_from(
    scx: &StatementContext,
    table_name: ResolvedObjectName,
    columns: Vec<Ident>,
    params: CopyParams,
) -> Result<Plan, anyhow::Error> {
    let (id, _, columns) = query::plan_copy_from(scx, table_name, columns)?;
    Ok(Plan::CopyFrom(CopyFromPlan {
        id,
        columns,
        params,
    }))
}

pub fn plan_copy(
    scx: &StatementContext,
    CopyStatement {
        relation,
        direction,
        target,
        options,
    }: CopyStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let options = CopyOptions::try_from(options)?;
    let mut copy_params = CopyParams {
        format: CopyFormat::Text,
        delimiter: options.delimiter,
        null: options.null,
        escape: options.escape,
        quote: options.quote,
        header: options.header,
    };
    if let Some(format) = options.format {
        copy_params.format = match format.to_lowercase().as_str() {
            "text" => CopyFormat::Text,
            "csv" => CopyFormat::Csv,
            "binary" => CopyFormat::Binary,
            _ => bail!("unknown FORMAT: {}", format),
        };
    }
    if let CopyDirection::To = direction {
        if copy_params.delimiter.is_some() {
            bail!("COPY TO does not support DELIMITER option yet");
        }
        if copy_params.null.is_some() {
            bail!("COPY TO does not support NULL option yet");
        }
    }
    match (&direction, &target) {
        (CopyDirection::To, CopyTarget::Stdout) => match relation {
            CopyRelation::Table { .. } => bail!("table with COPY TO unsupported"),
            CopyRelation::Select(stmt) => Ok(plan_select(
                scx,
                stmt,
                &Params::empty(),
                Some(copy_params.format),
            )?),
            CopyRelation::Tail(stmt) => Ok(plan_tail(scx, stmt, Some(copy_params.format))?),
        },
        (CopyDirection::From, CopyTarget::Stdin) => match relation {
            CopyRelation::Table { name, columns } => {
                plan_copy_from(scx, name, columns, copy_params)
            }
            _ => bail!("COPY FROM {} not supported", target),
        },
        _ => bail!("COPY {} {} not supported", direction, target),
    }
}
