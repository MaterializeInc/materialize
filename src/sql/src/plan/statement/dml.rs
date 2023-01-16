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
//! `INSERT`, `SELECT`, `SUBSCRIBE`, and `COPY`.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};

use mz_expr::MirRelationExpr;
use mz_ore::collections::CollectionExt;
use mz_pgcopy::{CopyCsvFormatParams, CopyFormatParams, CopyTextFormatParams};
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::explain_new::{ExplainConfig, ExplainFormat};
use mz_repr::{RelationDesc, ScalarType};

use crate::ast::display::AstDisplay;
use crate::ast::{
    AstInfo, CopyDirection, CopyOption, CopyOptionName, CopyRelation, CopyStatement, CopyTarget,
    CreateMaterializedViewStatement, CreateViewStatement, DeleteStatement, ExplainStage,
    ExplainStatement, Explainee, Ident, InsertStatement, Query, SelectStatement, Statement,
    SubscribeOption, SubscribeOptionName, SubscribeRelation, SubscribeStatement, UpdateStatement,
    ViewDefinition,
};
use crate::catalog::CatalogItemType;
use crate::names::{self, Aug, ResolvedObjectName};
use crate::plan::query::{plan_up_to, QueryLifetime};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::with_options::TryFromValue;
use crate::plan::{
    query, CopyFormat, CopyFromPlan, ExplainPlan, InsertPlan, MutationKind, Params, PeekPlan, Plan,
    PlanError, QueryContext, ReadThenWritePlan, SubscribeFrom, SubscribePlan,
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
        returning,
    }: InsertStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let (_, _, returning) = query::plan_insert_query(scx, table_name, columns, source, returning)?;
    let desc = if returning.expr.is_empty() {
        None
    } else {
        Some(returning.desc)
    };
    Ok(StatementDesc::new(desc))
}

pub fn plan_insert(
    scx: &StatementContext,
    InsertStatement {
        table_name,
        columns,
        source,
        returning,
    }: InsertStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let (id, mut expr, returning) =
        query::plan_insert_query(scx, table_name, columns, source, returning)?;
    expr.bind_parameters(params)?;
    let expr = expr.optimize_and_lower(&scx.into())?;
    let returning = returning
        .expr
        .into_iter()
        .map(|expr| expr.lower_uncorrelated())
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Plan::Insert(InsertPlan {
        id,
        values: expr,
        returning,
    }))
}

pub fn describe_delete(
    scx: &StatementContext,
    stmt: DeleteStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    query::plan_delete_query(scx, stmt)?;
    Ok(StatementDesc::new(None))
}

pub fn plan_delete(
    scx: &StatementContext,
    stmt: DeleteStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let rtw_plan = query::plan_delete_query(scx, stmt)?;
    plan_read_then_write(MutationKind::Delete, scx, params, rtw_plan)
}

pub fn describe_update(
    scx: &StatementContext,
    stmt: UpdateStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    query::plan_update_query(scx, stmt)?;
    Ok(StatementDesc::new(None))
}

pub fn plan_update(
    scx: &StatementContext,
    stmt: UpdateStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
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
) -> Result<Plan, PlanError> {
    selection.bind_parameters(params)?;
    let selection = selection.optimize_and_lower(&scx.into())?;
    let mut assignments_outer = HashMap::new();
    for (idx, mut set) in assignments {
        set.bind_parameters(params)?;
        let set = set.lower_uncorrelated()?;
        assignments_outer.insert(idx, set);
    }

    Ok(Plan::ReadThenWrite(ReadThenWritePlan {
        id,
        selection,
        finishing,
        assignments: assignments_outer,
        kind,
        returning: Vec::new(),
    }))
}

pub fn describe_select(
    scx: &StatementContext,
    stmt: SelectStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let query::PlannedQuery { desc, .. } =
        query::plan_root_query(scx, stmt.query, QueryLifetime::OneShot(scx.pcx()?))?;
    Ok(StatementDesc::new(Some(desc)))
}

pub fn plan_select(
    scx: &StatementContext,
    SelectStatement { query, as_of }: SelectStatement<Aug>,
    params: &Params,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, PlanError> {
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
) -> Result<StatementDesc, PlanError> {
    let mut relation_desc = RelationDesc::empty();

    match stage {
        ExplainStage::RawPlan => {
            relation_desc =
                relation_desc.with_column("Raw Plan", ScalarType::String.nullable(false));
        }
        ExplainStage::QueryGraph => {
            relation_desc =
                relation_desc.with_column("Query Graph", ScalarType::String.nullable(false));
        }
        ExplainStage::OptimizedQueryGraph => {
            relation_desc = relation_desc
                .with_column("Optimized Query Graph", ScalarType::String.nullable(false));
        }
        ExplainStage::DecorrelatedPlan => {
            relation_desc =
                relation_desc.with_column("Decorrelated Plan", ScalarType::String.nullable(false));
        }
        ExplainStage::OptimizedPlan => {
            relation_desc =
                relation_desc.with_column("Optimized Plan", ScalarType::String.nullable(false));
        }
        ExplainStage::PhysicalPlan => {
            relation_desc =
                relation_desc.with_column("Physical Plan", ScalarType::String.nullable(false));
        }
        ExplainStage::Timestamp => {
            relation_desc =
                relation_desc.with_column("Timestamp", ScalarType::String.nullable(false));
        }
        ExplainStage::Trace => {
            relation_desc = relation_desc
                .with_column("Time", ScalarType::UInt64.nullable(false))
                .with_column("Path", ScalarType::String.nullable(false))
                .with_column("Plan", ScalarType::String.nullable(false));
        }
    };

    Ok(
        StatementDesc::new(Some(relation_desc)).with_params(match explainee {
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
        }),
    )
}

pub fn plan_explain(
    scx: &StatementContext,
    ExplainStatement {
        stage,
        config_flags,
        format,
        explainee,
    }: ExplainStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let is_view = matches!(explainee, Explainee::View(_));
    let (explainee, query) = match explainee {
        Explainee::View(name) => {
            let view = scx.get_item_by_resolved_name(&name)?;
            let item_type = view.item_type();
            // Return a more helpful error on `EXPLAIN [...] VIEW <materialized-view>`.
            if item_type == CatalogItemType::MaterializedView {
                return Err(PlanError::ExplainViewOnMaterializedView(
                    name.full_name_str(),
                ));
            } else if item_type != CatalogItemType::View {
                sql_bail!(
                    "Expected {} to be a view, not a {}",
                    name.full_name_str(),
                    item_type
                );
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
            let qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx().unwrap()));
            (
                mz_repr::explain_new::Explainee::Dataflow(view.id()),
                names::resolve(qcx.scx.catalog, query)?.0,
            )
        }
        Explainee::MaterializedView(name) => {
            let mview = scx.get_item_by_resolved_name(&name)?;
            let item_type = mview.item_type();
            if item_type != CatalogItemType::MaterializedView {
                sql_bail!(
                    "Expected {} to be a materialized view, not a {}",
                    name,
                    item_type
                );
            }
            let parsed = crate::parse::parse(mview.create_sql())
                .expect("Sql for existing materialized view should be valid sql");
            let query = match parsed.into_last() {
                Statement::CreateMaterializedView(CreateMaterializedViewStatement {
                    query,
                    ..
                }) => query,
                _ => {
                    panic!("Sql for existing materialized view should parse as a materialized view")
                }
            };
            let qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx().unwrap()));
            (
                mz_repr::explain_new::Explainee::Dataflow(mview.id()),
                names::resolve(qcx.scx.catalog, query)?.0,
            )
        }
        Explainee::Query(query) => (mz_repr::explain_new::Explainee::Query, query),
    };
    // Previously we would bail here for ORDER BY and LIMIT; this has been relaxed to silently
    // report the plan without the ORDER BY and LIMIT decorations (which are done in post).
    let query::PlannedQuery {
        mut expr,
        desc,
        finishing,
    } = query::plan_root_query(scx, query, QueryLifetime::OneShot(scx.pcx()?))?;
    let finishing = if is_view {
        // views don't use a separate finishing
        expr.finish(finishing);
        None
    } else if finishing.is_trivial(desc.arity()) {
        None
    } else {
        Some(finishing)
    };
    expr.bind_parameters(params)?;

    let config_flags = config_flags
        .iter()
        .map(|ident| ident.to_string().to_lowercase())
        .collect::<BTreeSet<_>>();
    let config = ExplainConfig::try_from(config_flags)?;

    let format = match format {
        mz_sql_parser::ast::ExplainFormat::Text => ExplainFormat::Text,
        mz_sql_parser::ast::ExplainFormat::Json => ExplainFormat::Json,
        mz_sql_parser::ast::ExplainFormat::Dot => ExplainFormat::Dot,
    };

    Ok(Plan::Explain(ExplainPlan {
        raw_plan: expr,
        row_set_finishing: finishing,
        stage,
        format,
        config,
        explainee,
    }))
}

/// Plans and decorrelates a `Query`. Like `query::plan_root_query`, but returns
/// an `mz_expr::MirRelationExpr`, which cannot include correlated expressions.
pub fn plan_query(
    scx: &StatementContext,
    query: Query<Aug>,
    params: &Params,
    lifetime: QueryLifetime,
) -> Result<query::PlannedQuery<MirRelationExpr>, PlanError> {
    let query::PlannedQuery {
        mut expr,
        desc,
        finishing,
    } = query::plan_root_query(scx, query, lifetime)?;
    expr.bind_parameters(params)?;
    Ok(query::PlannedQuery {
        expr: expr.optimize_and_lower(&scx.into())?,
        desc,
        finishing,
    })
}

generate_extracted_config!(SubscribeOption, (Snapshot, bool), (Progress, bool));

pub fn describe_subscribe(
    scx: &StatementContext,
    stmt: SubscribeStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let relation_desc = match stmt.relation {
        SubscribeRelation::Name(name) => {
            let item = scx.get_item_by_resolved_name(&name)?;
            item.desc(&scx.catalog.resolve_full_name(item.name()))?
                .into_owned()
        }
        SubscribeRelation::Query(query) => {
            let query::PlannedQuery { desc, .. } =
                query::plan_root_query(scx, query, QueryLifetime::OneShot(scx.pcx()?))?;
            desc
        }
    };
    let SubscribeOptionExtracted { progress, .. } = stmt.options.try_into()?;
    let progress = progress.unwrap_or(false);
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
    Ok(StatementDesc::new(Some(desc)))
}

pub fn plan_subscribe(
    scx: &StatementContext,
    SubscribeStatement {
        relation,
        options,
        as_of,
        up_to,
    }: SubscribeStatement<Aug>,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, PlanError> {
    let from = match relation {
        SubscribeRelation::Name(name) => {
            let entry = scx.get_item_by_resolved_name(&name)?;
            match entry.item_type() {
                CatalogItemType::Table
                | CatalogItemType::Source
                | CatalogItemType::View
                | CatalogItemType::MaterializedView => SubscribeFrom::Id(entry.id()),
                CatalogItemType::Func
                | CatalogItemType::Index
                | CatalogItemType::Sink
                | CatalogItemType::Type
                | CatalogItemType::Secret
                | CatalogItemType::Connection => sql_bail!(
                    "'{}' cannot be subscribed to because it is a {}",
                    name.full_name_str(),
                    entry.item_type(),
                ),
            }
        }
        SubscribeRelation::Query(query) => {
            // There's no way to apply finishing operations to a `SUBSCRIBE`
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
            SubscribeFrom::Query {
                expr: query.expr,
                desc: query.desc,
            }
        }
    };

    let when = query::plan_as_of(scx, as_of)?;
    let up_to = up_to.map(|up_to| plan_up_to(scx, up_to)).transpose()?;

    let SubscribeOptionExtracted {
        progress, snapshot, ..
    } = options.try_into()?;
    Ok(Plan::Subscribe(SubscribePlan {
        from,
        when,
        up_to,
        with_snapshot: snapshot.unwrap_or(true),
        copy_to,
        emit_progress: progress.unwrap_or(false),
    }))
}

pub fn describe_table(
    scx: &StatementContext,
    table_name: <Aug as AstInfo>::ObjectName,
    columns: Vec<Ident>,
) -> Result<StatementDesc, PlanError> {
    let (_, desc, _) = query::plan_copy_from(scx, table_name, columns)?;
    Ok(StatementDesc::new(Some(desc)))
}

pub fn describe_copy(
    scx: &StatementContext,
    CopyStatement { relation, .. }: CopyStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(match relation {
        CopyRelation::Table { name, columns } => describe_table(scx, name, columns)?,
        CopyRelation::Select(stmt) => describe_select(scx, stmt)?,
        CopyRelation::Subscribe(stmt) => describe_subscribe(scx, stmt)?,
    }
    .with_is_copy())
}

fn plan_copy_from(
    scx: &StatementContext,
    table_name: ResolvedObjectName,
    columns: Vec<Ident>,
    format: CopyFormat,
    options: CopyOptionExtracted,
) -> Result<Plan, PlanError> {
    fn only_available_with_csv<T>(option: Option<T>, param: &str) -> Result<(), PlanError> {
        match option {
            Some(_) => sql_bail!("COPY {} available only in CSV mode", param),
            None => Ok(()),
        }
    }

    fn extract_byte_param_value(
        v: Option<String>,
        default: u8,
        param_name: &str,
    ) -> Result<u8, PlanError> {
        match v {
            Some(v) if v.len() == 1 => Ok(v.as_bytes()[0]),
            Some(..) => sql_bail!("COPY {} must be a single one-byte character", param_name),
            None => Ok(default),
        }
    }

    let params = match format {
        CopyFormat::Text => {
            only_available_with_csv(options.quote, "quote")?;
            only_available_with_csv(options.escape, "escape")?;
            only_available_with_csv(options.header, "HEADER")?;
            let delimiter = match options.delimiter {
                Some(delimiter) if delimiter.len() > 1 => {
                    sql_bail!("COPY delimiter must be a single one-byte character");
                }
                Some(delimiter) => Cow::from(delimiter),
                None => Cow::from("\t"),
            };
            let null = match options.null {
                Some(null) => Cow::from(null),
                None => Cow::from("\\N"),
            };
            CopyFormatParams::Text(CopyTextFormatParams { null, delimiter })
        }
        CopyFormat::Csv => {
            let quote = extract_byte_param_value(options.quote, b'"', "quote")?;
            let escape = extract_byte_param_value(options.escape, quote, "escape")?;
            let header = options.header.unwrap_or(false);
            let delimiter = extract_byte_param_value(options.delimiter, b',', "delimiter")?;
            if delimiter == quote {
                sql_bail!("COPY delimiter and quote must be different");
            }
            let null = match options.null {
                Some(null) => Cow::from(null),
                None => Cow::from(""),
            };
            CopyFormatParams::Csv(CopyCsvFormatParams {
                delimiter,
                quote,
                escape,
                null,
                header,
            })
        }
        CopyFormat::Binary => bail_unsupported!("FORMAT BINARY"),
    };

    let (id, _, columns) = query::plan_copy_from(scx, table_name, columns)?;
    Ok(Plan::CopyFrom(CopyFromPlan {
        id,
        columns,
        params,
    }))
}

generate_extracted_config!(
    CopyOption,
    (Format, String, Default("text")),
    (Delimiter, String),
    (Null, String),
    (Escape, String),
    (Quote, String),
    (Header, bool)
);

pub fn plan_copy(
    scx: &StatementContext,
    CopyStatement {
        relation,
        direction,
        target,
        options,
    }: CopyStatement<Aug>,
) -> Result<Plan, PlanError> {
    let options = CopyOptionExtracted::try_from(options)?;
    let format = match options.format.to_lowercase().as_str() {
        "text" => CopyFormat::Text,
        "csv" => CopyFormat::Csv,
        "binary" => CopyFormat::Binary,
        _ => sql_bail!("unknown FORMAT: {}", options.format),
    };
    if let CopyDirection::To = direction {
        if options.delimiter.is_some() {
            sql_bail!("COPY TO does not support DELIMITER option yet");
        }
        if options.null.is_some() {
            sql_bail!("COPY TO does not support NULL option yet");
        }
    }
    match (&direction, &target) {
        (CopyDirection::To, CopyTarget::Stdout) => match relation {
            CopyRelation::Table { .. } => sql_bail!("table with COPY TO unsupported"),
            CopyRelation::Select(stmt) => {
                Ok(plan_select(scx, stmt, &Params::empty(), Some(format))?)
            }
            CopyRelation::Subscribe(stmt) => Ok(plan_subscribe(scx, stmt, Some(format))?),
        },
        (CopyDirection::From, CopyTarget::Stdin) => match relation {
            CopyRelation::Table { name, columns } => {
                plan_copy_from(scx, name, columns, format, options)
            }
            _ => sql_bail!("COPY FROM {} not supported", target),
        },
        _ => sql_bail!("COPY {} {} not supported", direction, target),
    }
}
