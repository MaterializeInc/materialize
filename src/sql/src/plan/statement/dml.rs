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
use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;

use mz_expr::MirRelationExpr;
use mz_pgcopy::{CopyCsvFormatParams, CopyFormatParams, CopyTextFormatParams};
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::bytes::ByteSize;
use mz_repr::explain::{ExplainConfig, ExplainFormat};
use mz_repr::{GlobalId, RelationDesc, ScalarType};
use mz_sql_parser::ast::{
    ExplainSinkSchemaFor, ExplainSinkSchemaStatement, ExplainTimestampStatement, Expr,
    IfExistsBehavior, OrderByExpr, SubscribeOutput, UnresolvedItemName,
};
use mz_sql_parser::ident;
use mz_storage_types::sinks::{KafkaSinkConnection, KafkaSinkFormat, StorageSinkConnection};

use crate::ast::display::AstDisplay;
use crate::ast::{
    AstInfo, CopyDirection, CopyOption, CopyOptionName, CopyRelation, CopyStatement, CopyTarget,
    DeleteStatement, ExplainPlanStatement, ExplainStage, Explainee, Ident, InsertStatement, Query,
    SelectStatement, SubscribeOption, SubscribeOptionName, SubscribeRelation, SubscribeStatement,
    UpdateStatement,
};
use crate::catalog::CatalogItemType;
use crate::names::{Aug, ResolvedItemName};
use crate::normalize;
use crate::plan::query::{plan_expr, plan_up_to, ExprContext, PlannedRootQuery, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::{ddl, StatementContext, StatementDesc};
use crate::plan::with_options::{self, TryFromValue};
use crate::plan::{
    self, side_effecting_func, transform_ast, CopyToFrom, CopyToPlan, CreateSinkPlan,
    ExplainSinkSchemaPlan, ExplainTimestampPlan,
};
use crate::plan::{
    query, CopyFormat, CopyFromPlan, ExplainPlanPlan, InsertPlan, MutationKind, Params, Plan,
    PlanError, QueryContext, ReadThenWritePlan, SelectPlan, SubscribeFrom, SubscribePlan,
};
use crate::session::vars;

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
    plan_read_then_write(MutationKind::Delete, params, rtw_plan)
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
    plan_read_then_write(MutationKind::Update, params, rtw_plan)
}

pub fn plan_read_then_write(
    kind: MutationKind,
    params: &Params,
    query::ReadThenWritePlan {
        id,
        mut selection,
        finishing,
        assignments,
    }: query::ReadThenWritePlan,
) -> Result<Plan, PlanError> {
    selection.bind_parameters(params)?;
    let mut assignments_outer = BTreeMap::new();
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
    if let Some(desc) = side_effecting_func::describe_select_if_side_effecting(scx, &stmt)? {
        return Ok(StatementDesc::new(Some(desc)));
    }

    let query::PlannedRootQuery { desc, .. } =
        query::plan_root_query(scx, stmt.query, QueryLifetime::OneShot)?;
    Ok(StatementDesc::new(Some(desc)))
}

pub fn plan_select(
    scx: &StatementContext,
    select: SelectStatement<Aug>,
    params: &Params,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, PlanError> {
    if let Some(f) = side_effecting_func::plan_select_if_side_effecting(scx, &select, params)? {
        return Ok(Plan::SideEffectingFunc(f));
    }

    let when = query::plan_as_of(scx, select.as_of)?;
    let query::PlannedRootQuery {
        mut expr,
        desc: _,
        finishing,
        scope: _,
    } = query::plan_root_query(scx, select.query, QueryLifetime::OneShot)?;
    expr.bind_parameters(params)?;

    Ok(Plan::Select(SelectPlan {
        source: expr,
        when,
        finishing,
        copy_to,
    }))
}

pub fn describe_explain_plan(
    scx: &StatementContext,
    ExplainPlanStatement {
        stage, explainee, ..
    }: ExplainPlanStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let mut relation_desc = RelationDesc::empty();

    match stage {
        ExplainStage::RawPlan => {
            relation_desc =
                relation_desc.with_column("Raw Plan", ScalarType::String.nullable(false));
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
        ExplainStage::Trace => {
            relation_desc = relation_desc
                .with_column("Time", ScalarType::UInt64.nullable(false))
                .with_column("Path", ScalarType::String.nullable(false))
                .with_column("Plan", ScalarType::String.nullable(false));
        }
    };

    Ok(
        StatementDesc::new(Some(relation_desc)).with_params(match explainee {
            Explainee::Select(select, _) => describe_select(scx, *select)?.param_types,
            _ => vec![],
        }),
    )
}

pub fn describe_explain_timestamp(
    scx: &StatementContext,
    ExplainTimestampStatement { select, .. }: ExplainTimestampStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let mut relation_desc = RelationDesc::empty();
    relation_desc = relation_desc.with_column("Timestamp", ScalarType::String.nullable(false));

    Ok(StatementDesc::new(Some(relation_desc))
        .with_params(describe_select(scx, select)?.param_types))
}

pub fn describe_explain_schema(
    _: &StatementContext,
    ExplainSinkSchemaStatement { .. }: ExplainSinkSchemaStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let mut relation_desc = RelationDesc::empty();
    relation_desc = relation_desc.with_column("Schema", ScalarType::String.nullable(false));
    Ok(StatementDesc::new(Some(relation_desc)))
}

pub fn plan_explain_plan(
    scx: &StatementContext,
    ExplainPlanStatement {
        stage,
        config_flags,
        format,
        explainee,
    }: ExplainPlanStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    use crate::plan::ExplaineeStatement;

    let format = match format {
        mz_sql_parser::ast::ExplainFormat::Text => ExplainFormat::Text,
        mz_sql_parser::ast::ExplainFormat::Json => ExplainFormat::Json,
        mz_sql_parser::ast::ExplainFormat::Dot => ExplainFormat::Dot,
    };

    let config = {
        let config_flags = config_flags
            .iter()
            .map(|ident| ident.to_string().to_lowercase())
            .collect::<BTreeSet<_>>();

        let mut config = ExplainConfig::try_from(config_flags)?;

        if config.filter_pushdown {
            scx.require_feature_flag(&vars::ENABLE_MFP_PUSHDOWN_EXPLAIN)?;
            // If filtering is disabled, explain plans should not include pushdown info.
            config.filter_pushdown = scx.catalog.system_vars().persist_stats_filter_enabled();
        }

        config
    };

    let explainee = match explainee {
        Explainee::View(_) => {
            bail_never_supported!(
                "EXPLAIN ... VIEW <view_name>",
                "sql/explain-plan",
                "Use `EXPLAIN ... SELECT * FROM <view_name>` (if the view is not indexed) or `EXPLAIN ... INDEX <idx_name>` (if the view is indexed) instead."
            );
        }
        Explainee::MaterializedView(name) => {
            let item = scx.get_item_by_resolved_name(&name)?;
            let item_type = item.item_type();
            if item_type != CatalogItemType::MaterializedView {
                sql_bail!("Expected {name} to be a materialized view, not a {item_type}");
            }
            crate::plan::Explainee::MaterializedView(item.id())
        }
        Explainee::Index(name) => {
            let item = scx.get_item_by_resolved_name(&name)?;
            let item_type = item.item_type();
            if item_type != CatalogItemType::Index {
                sql_bail!("Expected {name} to be an index, not a {item_type}");
            }
            crate::plan::Explainee::Index(item.id())
        }
        Explainee::Select(select, broken) => {
            // The following code should align with `dml::plan_select`.
            let when = query::plan_as_of(scx, select.as_of)?;
            let query::PlannedRootQuery {
                mut expr,
                desc,
                finishing,
                scope: _,
            } = query::plan_root_query(scx, select.query, QueryLifetime::OneShot)?;
            expr.bind_parameters(params)?;

            let plan = SelectPlan {
                source: expr,
                when,
                finishing,
                copy_to: None,
            };

            if broken {
                scx.require_feature_flag(&vars::ENABLE_EXPLAIN_BROKEN)?;
            }

            crate::plan::Explainee::Statement(ExplaineeStatement::Select { broken, plan, desc })
        }
        Explainee::CreateMaterializedView(mut stmt, broken) => {
            if stmt.if_exists != IfExistsBehavior::Skip {
                // If we don't force this parameter to Skip planning will
                // fail for names that already exist in the catalog. This
                // can happen even in `Replace` mode if the existing item
                // has dependencies.
                stmt.if_exists = IfExistsBehavior::Skip;
            } else {
                sql_bail!(
                    "Cannot EXPLAIN a CREATE MATERIALIZED VIEW that explictly sets IF NOT EXISTS \
                     (the behavior is implied within the scope of an enclosing EXPLAIN)"
                );
            }

            let Plan::CreateMaterializedView(plan) =
                ddl::plan_create_materialized_view(scx, *stmt, params)?
            else {
                sql_bail!("expected CreateMaterializedViewPlan plan");
            };

            crate::plan::Explainee::Statement(ExplaineeStatement::CreateMaterializedView {
                broken,
                plan,
            })
        }
        Explainee::CreateIndex(mut stmt, broken) => {
            if !stmt.if_not_exists {
                // If we don't force this parameter to true planning will
                // fail for index items that already exist in the catalog.
                stmt.if_not_exists = true;
            } else {
                sql_bail!(
                    "Cannot EXPLAIN a CREATE INDEX that explictly sets IF NOT EXISTS \
                     (the behavior is implied within the scope of an enclosing EXPLAIN)"
                );
            }

            let Plan::CreateIndex(plan) = ddl::plan_create_index(scx, *stmt)? else {
                sql_bail!("expected CreateIndexPlan plan");
            };

            crate::plan::Explainee::Statement(ExplaineeStatement::CreateIndex { broken, plan })
        }
    };

    Ok(Plan::ExplainPlan(ExplainPlanPlan {
        stage,
        format,
        config,
        explainee,
    }))
}

pub fn plan_explain_schema(
    scx: &StatementContext,
    explain_schema: ExplainSinkSchemaStatement<Aug>,
) -> Result<Plan, PlanError> {
    let ExplainSinkSchemaStatement {
        schema_for,
        mut statement,
    } = explain_schema;

    // Force the sink's name to one that's guaranteed not to exist, by virtue of
    // being a non-existent item in a schema under the system's control, so that
    // `plan_create_sink` doesn't complain about the name already existing.
    statement.name = Some(UnresolvedItemName::qualified(&[
        ident!("mz_catalog"),
        ident!("mz_explain_schema"),
    ]));

    crate::pure::add_materialize_comments(scx.catalog, &mut statement)?;

    match ddl::plan_create_sink(scx, statement)? {
        Plan::CreateSink(CreateSinkPlan { sink, .. }) => match sink.connection {
            StorageSinkConnection::Kafka(KafkaSinkConnection {
                format:
                    KafkaSinkFormat::Avro {
                        key_schema,
                        value_schema,
                        ..
                    },
                ..
            }) => {
                let schema = match schema_for {
                    ExplainSinkSchemaFor::Key => {
                        key_schema.ok_or_else(|| sql_err!("CREATE SINK does not have a key"))?
                    }
                    ExplainSinkSchemaFor::Value => value_schema,
                };

                Ok(Plan::ExplainSinkSchema(ExplainSinkSchemaPlan {
                    sink_from: sink.from,
                    json_schema: schema,
                }))
            }
            _ => bail_unsupported!(
                "EXPLAIN SCHEMA is only available for Kafka sinks with Avro schemas"
            ),
        },
        _ => unreachable!("plan_create_sink returns a CreateSinkPlan"),
    }
}

pub fn plan_explain_timestamp(
    scx: &StatementContext,
    ExplainTimestampStatement { format, select }: ExplainTimestampStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let format = match format {
        mz_sql_parser::ast::ExplainFormat::Text => ExplainFormat::Text,
        mz_sql_parser::ast::ExplainFormat::Json => ExplainFormat::Json,
        mz_sql_parser::ast::ExplainFormat::Dot => ExplainFormat::Dot,
    };

    let raw_plan = {
        let query::PlannedRootQuery {
            expr: mut raw_plan,
            desc: _,
            finishing: _,
            scope: _,
        } = query::plan_root_query(scx, select.query, QueryLifetime::OneShot)?;
        raw_plan.bind_parameters(params)?;

        raw_plan
    };
    let when = query::plan_as_of(scx, select.as_of)?;

    Ok(Plan::ExplainTimestamp(ExplainTimestampPlan {
        format,
        raw_plan,
        when,
    }))
}

/// Plans and decorrelates a [`Query`]. Like [`query::plan_root_query`], but
/// returns an [`MirRelationExpr`], which cannot include correlated expressions.
#[deprecated = "Use `query::plan_root_query` and use `HirRelationExpr` in `~Plan` structs."]
pub fn plan_query(
    scx: &StatementContext,
    query: Query<Aug>,
    params: &Params,
    lifetime: QueryLifetime,
) -> Result<query::PlannedRootQuery<MirRelationExpr>, PlanError> {
    let query::PlannedRootQuery {
        mut expr,
        desc,
        finishing,
        scope,
    } = query::plan_root_query(scx, query, lifetime)?;
    expr.bind_parameters(params)?;

    Ok(query::PlannedRootQuery {
        expr: expr.lower(scx.catalog.system_vars())?,
        desc,
        finishing,
        scope,
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
            let query::PlannedRootQuery { desc, .. } =
                query::plan_root_query(scx, query, QueryLifetime::Subscribe)?;
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

    let debezium = matches!(stmt.output, SubscribeOutput::EnvelopeDebezium { .. });
    match stmt.output {
        SubscribeOutput::Diffs | SubscribeOutput::WithinTimestampOrderBy { .. } => {
            desc = desc.with_column("mz_diff", ScalarType::Int64.nullable(true));
            for (name, mut ty) in relation_desc.into_iter() {
                if progress {
                    ty.nullable = true;
                }
                desc = desc.with_column(name, ty);
            }
        }
        SubscribeOutput::EnvelopeUpsert { key_columns }
        | SubscribeOutput::EnvelopeDebezium { key_columns } => {
            desc = desc.with_column("mz_state", ScalarType::String.nullable(true));
            let key_columns = key_columns
                .into_iter()
                .map(normalize::column_name)
                .collect_vec();
            let mut before_values_desc = RelationDesc::empty();
            let mut after_values_desc = RelationDesc::empty();
            for (mut name, mut ty) in relation_desc.into_iter() {
                if key_columns.contains(&name) {
                    if progress {
                        ty.nullable = true;
                    }
                    desc = desc.with_column(name, ty);
                } else {
                    ty.nullable = true;
                    before_values_desc =
                        before_values_desc.with_column(format!("before_{}", name), ty.clone());
                    if debezium {
                        name = format!("after_{}", name).into();
                    }
                    after_values_desc = after_values_desc.with_column(name, ty);
                }
            }
            if debezium {
                desc = desc.concat(before_values_desc);
            }
            desc = desc.concat(after_values_desc);
        }
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
        output,
    }: SubscribeStatement<Aug>,
    params: &Params,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, PlanError> {
    let (from, desc, scope) = match relation {
        SubscribeRelation::Name(name) => {
            let entry = scx.get_item_by_resolved_name(&name)?;
            let desc = match entry.desc(&scx.catalog.resolve_full_name(entry.name())) {
                Ok(desc) => desc,
                Err(..) => sql_bail!(
                    "'{}' cannot be subscribed to because it is a {}",
                    name.full_name_str(),
                    entry.item_type(),
                ),
            };
            let item_name = match name {
                ResolvedItemName::Item { full_name, .. } => Some(full_name.into()),
                _ => None,
            };
            let scope = Scope::from_source(item_name, desc.iter().map(|(name, _type)| name));
            (SubscribeFrom::Id(entry.id()), desc.into_owned(), scope)
        }
        SubscribeRelation::Query(query) => {
            #[allow(deprecated)] // TODO(aalexandrov): Use HirRelationExpr in Subscribe
            let query = plan_query(scx, query, params, QueryLifetime::Subscribe)?;
            // There's no way to apply finishing operations to a `SUBSCRIBE` directly, so the
            // finishing should have already been turned into a `TopK` by
            // `plan_query` / `plan_root_query`, upon seeing the `QueryLifetime::Subscribe`.
            assert!(query.finishing.is_trivial(query.desc.arity()));
            let desc = query.desc.clone();
            (
                SubscribeFrom::Query {
                    expr: query.expr,
                    desc: query.desc,
                },
                desc,
                query.scope,
            )
        }
    };

    let when = query::plan_as_of(scx, as_of)?;
    let up_to = up_to.map(|up_to| plan_up_to(scx, up_to)).transpose()?;

    let qcx = QueryContext::root(scx, QueryLifetime::Subscribe);
    let ecx = ExprContext {
        qcx: &qcx,
        name: "",
        scope: &scope,
        relation_type: desc.typ(),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_parameters: true,
        allow_windows: false,
    };

    let output_columns: Vec<_> = scope.column_names().enumerate().collect();
    let output = match output {
        SubscribeOutput::Diffs => plan::SubscribeOutput::Diffs,
        SubscribeOutput::EnvelopeUpsert { key_columns } => {
            let order_by = key_columns
                .iter()
                .map(|ident| OrderByExpr {
                    expr: Expr::Identifier(vec![ident.clone()]),
                    asc: None,
                    nulls_last: None,
                })
                .collect_vec();
            let (order_by, map_exprs) = query::plan_order_by_exprs(
                &ExprContext {
                    name: "ENVELOPE UPSERT KEY clause",
                    ..ecx
                },
                &order_by[..],
                &output_columns[..],
            )?;
            if !map_exprs.is_empty() {
                return Err(PlanError::InvalidKeysInSubscribeEnvelopeUpsert);
            }
            plan::SubscribeOutput::EnvelopeUpsert {
                order_by_keys: order_by,
            }
        }
        SubscribeOutput::EnvelopeDebezium { key_columns } => {
            scx.require_feature_flag(&vars::ENABLE_ENVELOPE_DEBEZIUM_IN_SUBSCRIBE)?;
            let order_by = key_columns
                .iter()
                .map(|ident| OrderByExpr {
                    expr: Expr::Identifier(vec![ident.clone()]),
                    asc: None,
                    nulls_last: None,
                })
                .collect_vec();
            let (order_by, map_exprs) = query::plan_order_by_exprs(
                &ExprContext {
                    name: "ENVELOPE DEBEZIUM KEY clause",
                    ..ecx
                },
                &order_by[..],
                &output_columns[..],
            )?;
            if !map_exprs.is_empty() {
                return Err(PlanError::InvalidKeysInSubscribeEnvelopeDebezium);
            }
            plan::SubscribeOutput::EnvelopeDebezium {
                order_by_keys: order_by,
            }
        }
        SubscribeOutput::WithinTimestampOrderBy { order_by } => {
            scx.require_feature_flag(&vars::ENABLE_WITHIN_TIMESTAMP_ORDER_BY_IN_SUBSCRIBE)?;
            let mz_diff = "mz_diff".into();
            let output_columns = std::iter::once((0, &mz_diff))
                .chain(output_columns.into_iter().map(|(i, c)| (i + 1, c)))
                .collect_vec();
            match query::plan_order_by_exprs(
                &ExprContext {
                    name: "WITHIN TIMESTAMP ORDER BY clause",
                    ..ecx
                },
                &order_by[..],
                &output_columns[..],
            ) {
                Err(PlanError::UnknownColumn {
                    table: None,
                    column,
                    similar: _,
                }) if &column == &mz_diff => {
                    // mz_diff is being used in an expression. Since mz_diff isn't part of the table
                    // it looks like an unknown column. Instead, return a better error
                    return Err(PlanError::InvalidOrderByInSubscribeWithinTimestampOrderBy);
                }
                Err(e) => return Err(e),
                Ok((order_by, map_exprs)) => {
                    if !map_exprs.is_empty() {
                        return Err(PlanError::InvalidOrderByInSubscribeWithinTimestampOrderBy);
                    }

                    plan::SubscribeOutput::WithinTimestampOrderBy { order_by }
                }
            }
        }
    };

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
        output,
    }))
}

pub fn describe_table(
    scx: &StatementContext,
    table_name: <Aug as AstInfo>::ItemName,
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

fn plan_copy_to(
    scx: &StatementContext,
    from: CopyToFrom,
    to: &Expr<Aug>,
    format: CopyFormat,
    options: CopyOptionExtracted,
) -> Result<Plan, PlanError> {
    let conn_id = match options.aws_connection {
        Some(conn_id) => GlobalId::from(conn_id),
        None => sql_bail!("AWS CONNECTION is required for COPY ... TO <expr>"),
    };
    let connection = scx.get_item(&conn_id).connection()?;

    match connection {
        mz_storage_types::connections::Connection::Aws(_) => {}
        _ => sql_bail!("only AWS CONNECTION is supported for COPY ... TO <expr>"),
    }

    if format != CopyFormat::Csv {
        sql_bail!("only CSV format is supported for COPY ... TO <expr>");
    }

    // TODO(mouli): Get these from sql options
    let format_params = CopyFormatParams::Csv(
        CopyCsvFormatParams::try_new(None, None, None, None, None)
            .map_err(|e| sql_err!("{}", e))?,
    );

    // Converting the to expr to a MirScalarExpr
    let mut to_expr = to.clone();
    transform_ast::transform(scx, &mut to_expr)?;
    let relation_type = RelationDesc::empty();
    let ecx = &ExprContext {
        qcx: &QueryContext::root(scx, QueryLifetime::OneShot),
        name: "COPY TO target",
        scope: &Scope::empty(),
        relation_type: relation_type.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    };

    let to = plan_expr(ecx, &to_expr)?
        .type_as(ecx, &ScalarType::String)?
        .lower_uncorrelated()?;

    Ok(Plan::CopyTo(CopyToPlan {
        from,
        to,
        connection: connection.to_owned(),
        format_params,
    }))
}

fn plan_copy_from(
    scx: &StatementContext,
    table_name: ResolvedItemName,
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
        param_name: &str,
    ) -> Result<Option<u8>, PlanError> {
        match v {
            Some(v) if v.len() == 1 => Ok(Some(v.as_bytes()[0])),
            Some(..) => sql_bail!("COPY {} must be a single one-byte character", param_name),
            None => Ok(None),
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
            let quote = extract_byte_param_value(options.quote, "quote")?;
            let escape = extract_byte_param_value(options.escape, "escape")?;
            let delimiter = extract_byte_param_value(options.delimiter, "delimiter")?;
            CopyFormatParams::Csv(
                CopyCsvFormatParams::try_new(
                    delimiter,
                    quote,
                    escape,
                    options.header,
                    options.null,
                )
                .map_err(|e| sql_err!("{}", e))?,
            )
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
    (Header, bool),
    (AwsConnection, with_options::Object),
    (MaxFileSize, ByteSize)
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
            CopyRelation::Subscribe(stmt) => {
                Ok(plan_subscribe(scx, stmt, &Params::empty(), Some(format))?)
            }
        },
        (CopyDirection::From, CopyTarget::Stdin) => match relation {
            CopyRelation::Table { name, columns } => {
                plan_copy_from(scx, name, columns, format, options)
            }
            _ => sql_bail!("COPY FROM {} not supported", target),
        },
        (CopyDirection::To, CopyTarget::Expr(to_expr)) => {
            scx.require_feature_flag(&vars::ENABLE_COPY_TO_EXPR)?;

            let from = match relation {
                CopyRelation::Table { name, columns } => {
                    if !columns.is_empty() {
                        // TODO(mouli): Add support for this
                        sql_bail!("specifying columns for COPY <table_name> TO commands not yet supported; use COPY (SELECT...) TO ... instead");
                    }
                    CopyToFrom::Id {
                        id: *name.item_id(),
                    }
                }
                CopyRelation::Select(stmt) => {
                    if !stmt.query.order_by.is_empty() {
                        sql_bail!("ORDER BY is not supported in SELECT query for COPY statements")
                    }
                    #[allow(deprecated)] // TODO(aalexandrov): Use HirRelationExpr in CopyToFrom
                    let PlannedRootQuery {
                        expr,
                        finishing,
                        desc,
                        scope: _,
                    } = plan_query(scx, stmt.query, &Params::empty(), QueryLifetime::OneShot)?;
                    CopyToFrom::Query {
                        expr,
                        desc,
                        finishing,
                    }
                }
                _ => sql_bail!("COPY {} {} not supported", direction, target),
            };

            plan_copy_to(scx, from, to_expr, format, options)
        }
        _ => sql_bail!("COPY {} {} not supported", direction, target),
    }
}
