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
use std::collections::BTreeMap;

use itertools::Itertools;

use mz_adapter_types::dyncfgs::DEFAULT_SINK_PARTITION_STRATEGY;
use mz_arrow_util::builder::ArrowBuilder;
use mz_expr::{MirRelationExpr, RowSetFinishing};
use mz_ore::num::NonNeg;
use mz_ore::soft_panic_or_log;
use mz_pgcopy::{CopyCsvFormatParams, CopyFormatParams, CopyTextFormatParams};
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::bytes::ByteSize;
use mz_repr::explain::{ExplainConfig, ExplainFormat};
use mz_repr::optimize::OptimizerFeatureOverrides;
use mz_repr::{Datum, GlobalId, RelationDesc, ScalarType};
use mz_sql_parser::ast::{
    CreateSinkOption, CreateSinkOptionName, CteBlock, ExplainPlanOption, ExplainPlanOptionName,
    ExplainPushdownStatement, ExplainSinkSchemaFor, ExplainSinkSchemaStatement,
    ExplainTimestampStatement, Expr, IfExistsBehavior, OrderByExpr, SetExpr, SubscribeOutput,
    UnresolvedItemName, Value, WithOptionValue,
};
use mz_sql_parser::ident;
use mz_storage_types::sinks::{
    KafkaSinkConnection, KafkaSinkFormat, KafkaSinkFormatType, S3SinkFormat, StorageSinkConnection,
    MAX_S3_SINK_FILE_SIZE, MIN_S3_SINK_FILE_SIZE,
};

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
use crate::plan::query::{plan_expr, plan_up_to, ExprContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::{ddl, StatementContext, StatementDesc};
use crate::plan::with_options::{self, TryFromValue};
use crate::plan::{
    self, side_effecting_func, transform_ast, CopyToPlan, CreateSinkPlan, ExplainPushdownPlan,
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

    let (plan, _desc) = plan_select_inner(scx, select, params, copy_to)?;
    Ok(Plan::Select(plan))
}

fn plan_select_inner(
    scx: &StatementContext,
    select: SelectStatement<Aug>,
    params: &Params,
    copy_to: Option<CopyFormat>,
) -> Result<(SelectPlan, RelationDesc), PlanError> {
    let when = query::plan_as_of(scx, select.as_of.clone())?;
    let query::PlannedRootQuery {
        mut expr,
        desc,
        finishing,
        scope: _,
    } = query::plan_root_query(scx, select.query.clone(), QueryLifetime::OneShot)?;
    expr.bind_parameters(params)?;

    // A top-level limit cannot be data dependent so eagerly evaluate it.
    let limit = match finishing.limit {
        None => None,
        Some(mut limit) => {
            limit.bind_parameters(params)?;
            let Some(limit) = limit.as_literal() else {
                sql_bail!("Top-level LIMIT must be a constant expression")
            };
            match limit {
                Datum::Null => None,
                Datum::Int64(v) if v >= 0 => NonNeg::<i64>::try_from(v).ok(),
                _ => {
                    soft_panic_or_log!("Valid literal limit must be asserted in `plan_select`");
                    sql_bail!("LIMIT must be a non negative INT or NULL")
                }
            }
        }
    };

    let plan = SelectPlan {
        source: expr,
        when,
        finishing: RowSetFinishing {
            limit,
            offset: finishing.offset,
            project: finishing.project,
            order_by: finishing.order_by,
        },
        copy_to,
        select: Some(select),
    };

    Ok((plan, desc))
}

pub fn describe_explain_plan(
    scx: &StatementContext,
    explain: ExplainPlanStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let mut relation_desc = RelationDesc::empty();

    match explain.stage() {
        ExplainStage::RawPlan => {
            let name = "Raw Plan";
            relation_desc = relation_desc.with_column(name, ScalarType::String.nullable(false));
        }
        ExplainStage::DecorrelatedPlan => {
            let name = "Decorrelated Plan";
            relation_desc = relation_desc.with_column(name, ScalarType::String.nullable(false));
        }
        ExplainStage::LocalPlan => {
            let name = "Locally Optimized Plan";
            relation_desc = relation_desc.with_column(name, ScalarType::String.nullable(false));
        }
        ExplainStage::GlobalPlan => {
            let name = "Optimized Plan";
            relation_desc = relation_desc.with_column(name, ScalarType::String.nullable(false));
        }
        ExplainStage::PhysicalPlan => {
            let name = "Physical Plan";
            relation_desc = relation_desc.with_column(name, ScalarType::String.nullable(false));
        }
        ExplainStage::Trace => {
            relation_desc = relation_desc
                .with_column("Time", ScalarType::UInt64.nullable(false))
                .with_column("Path", ScalarType::String.nullable(false))
                .with_column("Plan", ScalarType::String.nullable(false));
        }
        ExplainStage::PlanInsights => {
            let name = "Plan Insights";
            relation_desc = relation_desc.with_column(name, ScalarType::String.nullable(false));
        }
    };

    Ok(
        StatementDesc::new(Some(relation_desc)).with_params(match explain.explainee {
            Explainee::Select(select, _) => describe_select(scx, *select)?.param_types,
            _ => vec![],
        }),
    )
}

pub fn describe_explain_pushdown(
    scx: &StatementContext,
    statement: ExplainPushdownStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let relation_desc = RelationDesc::empty()
        .with_column("Source", ScalarType::String.nullable(false))
        .with_column("Total Bytes", ScalarType::UInt64.nullable(false))
        .with_column("Selected Bytes", ScalarType::UInt64.nullable(false))
        .with_column("Total Parts", ScalarType::UInt64.nullable(false))
        .with_column("Selected Parts", ScalarType::UInt64.nullable(false));

    Ok(
        StatementDesc::new(Some(relation_desc)).with_params(match statement.explainee {
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

generate_extracted_config!(
    ExplainPlanOption,
    (Arity, bool, Default(false)),
    (Cardinality, bool, Default(false)),
    (ColumnNames, bool, Default(false)),
    (FilterPushdown, bool, Default(false)),
    (HumanizedExpressions, bool, Default(false)),
    (JoinImplementations, bool, Default(false)),
    (Keys, bool, Default(false)),
    (LinearChains, bool, Default(false)),
    (NoFastPath, bool, Default(false)),
    (NonNegative, bool, Default(false)),
    (NoNotices, bool, Default(false)),
    (NodeIdentifiers, bool, Default(false)),
    (Raw, bool, Default(false)),
    (RawPlans, bool, Default(false)),
    (RawSyntax, bool, Default(false)),
    (Redacted, bool, Default(false)),
    (SubtreeSize, bool, Default(false)),
    (Timing, bool, Default(false)),
    (Types, bool, Default(false)),
    (ReoptimizeImportedViews, Option<bool>, Default(None)),
    (EnableNewOuterJoinLowering, Option<bool>, Default(None)),
    (EnableEagerDeltaJoins, Option<bool>, Default(None)),
    (EnableVariadicLeftJoinLowering, Option<bool>, Default(None)),
    (EnableLetrecFixpointAnalysis, Option<bool>, Default(None)),
    (EnableOuterJoinNullFilter, Option<bool>, Default(None)),
    (EnableValueWindowFunctionFusion, Option<bool>, Default(None))
);

impl TryFrom<ExplainPlanOptionExtracted> for ExplainConfig {
    type Error = PlanError;

    fn try_from(mut v: ExplainPlanOptionExtracted) -> Result<Self, Self::Error> {
        use mz_ore::assert::SOFT_ASSERTIONS;
        use std::sync::atomic::Ordering;

        // If `WITH(raw)` is specified, ensure that the config will be as
        // representative for the original plan as possible.
        if v.raw {
            v.raw_plans = true;
            v.raw_syntax = true;
        }

        // Certain config should always be enabled in release builds running on
        // staging or prod (where SOFT_ASSERTIONS are turned off).
        let enable_on_prod = !SOFT_ASSERTIONS.load(Ordering::Relaxed);

        Ok(ExplainConfig {
            arity: v.arity || enable_on_prod,
            cardinality: v.cardinality,
            column_names: v.column_names,
            filter_pushdown: v.filter_pushdown || enable_on_prod,
            humanized_exprs: !v.raw_plans && (v.humanized_expressions || enable_on_prod),
            join_impls: v.join_implementations,
            keys: v.keys,
            linear_chains: !v.raw_plans && v.linear_chains,
            no_fast_path: v.no_fast_path,
            no_notices: v.no_notices,
            node_ids: v.node_identifiers,
            non_negative: v.non_negative,
            raw_plans: v.raw_plans,
            raw_syntax: v.raw_syntax,
            redacted: v.redacted,
            subtree_size: v.subtree_size,
            timing: v.timing,
            types: v.types,
            // The ones that are initialized with `Default::default()` are not wired up to EXPLAIN.
            features: OptimizerFeatureOverrides {
                enable_eager_delta_joins: v.enable_eager_delta_joins,
                enable_new_outer_join_lowering: v.enable_new_outer_join_lowering,
                enable_variadic_left_join_lowering: v.enable_variadic_left_join_lowering,
                enable_letrec_fixpoint_analysis: v.enable_letrec_fixpoint_analysis,
                enable_consolidate_after_union_negate: Default::default(),
                enable_reduce_mfp_fusion: Default::default(),
                enable_outer_join_null_filter: v.enable_outer_join_null_filter,
                enable_cardinality_estimates: Default::default(),
                persist_fast_path_limit: Default::default(),
                reoptimize_imported_views: v.reoptimize_imported_views,
                enable_value_window_function_fusion: v.enable_value_window_function_fusion,
            },
        })
    }
}

fn plan_explainee(
    scx: &StatementContext,
    explainee: Explainee<Aug>,
    params: &Params,
) -> Result<plan::Explainee, PlanError> {
    use crate::plan::ExplaineeStatement;

    let is_replan = matches!(
        explainee,
        Explainee::ReplanView(_) | Explainee::ReplanMaterializedView(_) | Explainee::ReplanIndex(_)
    );

    let explainee = match explainee {
        Explainee::View(name) | Explainee::ReplanView(name) => {
            let item = scx.get_item_by_resolved_name(&name)?;
            let item_type = item.item_type();
            if item_type != CatalogItemType::View {
                sql_bail!("Expected {name} to be a view, not a {item_type}");
            }
            match is_replan {
                true => crate::plan::Explainee::ReplanView(item.id()),
                false => crate::plan::Explainee::View(item.id()),
            }
        }
        Explainee::MaterializedView(name) | Explainee::ReplanMaterializedView(name) => {
            let item = scx.get_item_by_resolved_name(&name)?;
            let item_type = item.item_type();
            if item_type != CatalogItemType::MaterializedView {
                sql_bail!("Expected {name} to be a materialized view, not a {item_type}");
            }
            match is_replan {
                true => crate::plan::Explainee::ReplanMaterializedView(item.id()),
                false => crate::plan::Explainee::MaterializedView(item.id()),
            }
        }
        Explainee::Index(name) | Explainee::ReplanIndex(name) => {
            let item = scx.get_item_by_resolved_name(&name)?;
            let item_type = item.item_type();
            if item_type != CatalogItemType::Index {
                sql_bail!("Expected {name} to be an index, not a {item_type}");
            }
            match is_replan {
                true => crate::plan::Explainee::ReplanIndex(item.id()),
                false => crate::plan::Explainee::Index(item.id()),
            }
        }
        Explainee::Select(select, broken) => {
            let (plan, desc) = plan_select_inner(scx, *select, params, None)?;
            crate::plan::Explainee::Statement(ExplaineeStatement::Select { broken, plan, desc })
        }
        Explainee::CreateView(mut stmt, broken) => {
            if stmt.if_exists != IfExistsBehavior::Skip {
                // If we don't force this parameter to Skip planning will
                // fail for names that already exist in the catalog. This
                // can happen even in `Replace` mode if the existing item
                // has dependencies.
                stmt.if_exists = IfExistsBehavior::Skip;
            } else {
                sql_bail!(
                    "Cannot EXPLAIN a CREATE VIEW that explictly sets IF NOT EXISTS \
                     (the behavior is implied within the scope of an enclosing EXPLAIN)"
                );
            }

            let Plan::CreateView(plan) = ddl::plan_create_view(scx, *stmt, params)? else {
                sql_bail!("expected CreateViewPlan plan");
            };

            crate::plan::Explainee::Statement(ExplaineeStatement::CreateView { broken, plan })
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

    Ok(explainee)
}

pub fn plan_explain_plan(
    scx: &StatementContext,
    explain: ExplainPlanStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let format = match explain.format() {
        mz_sql_parser::ast::ExplainFormat::Text => ExplainFormat::Text,
        mz_sql_parser::ast::ExplainFormat::Json => ExplainFormat::Json,
        mz_sql_parser::ast::ExplainFormat::Dot => ExplainFormat::Dot,
    };
    let stage = explain.stage();

    // Plan ExplainConfig.
    let config = {
        let mut with_options = ExplainPlanOptionExtracted::try_from(explain.with_options)?;

        if with_options.filter_pushdown {
            // If filtering is disabled, explain plans should not include pushdown info.
            with_options.filter_pushdown = scx.catalog.system_vars().persist_stats_filter_enabled();
        }

        ExplainConfig::try_from(with_options)?
    };

    let explainee = plan_explainee(scx, explain.explainee, params)?;

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
        // Parser limits to JSON.
        format: _,
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
    let default_strategy = DEFAULT_SINK_PARTITION_STRATEGY.get(scx.catalog.system_vars().dyncfgs());
    statement.with_options.push(CreateSinkOption {
        name: CreateSinkOptionName::PartitionStrategy,
        value: Some(WithOptionValue::Value(Value::String(default_strategy))),
    });

    match ddl::plan_create_sink(scx, statement)? {
        Plan::CreateSink(CreateSinkPlan { sink, .. }) => match sink.connection {
            StorageSinkConnection::Kafka(KafkaSinkConnection {
                format:
                    KafkaSinkFormat {
                        key_format,
                        value_format:
                            KafkaSinkFormatType::Avro {
                                schema: value_schema,
                                ..
                            },
                        ..
                    },
                ..
            }) => {
                let schema = match schema_for {
                    ExplainSinkSchemaFor::Key => key_format
                        .and_then(|f| match f {
                            KafkaSinkFormatType::Avro { schema, .. } => Some(schema),
                            _ => None,
                        })
                        .ok_or_else(|| sql_err!("CREATE SINK does not have a key"))?,
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

pub fn plan_explain_pushdown(
    scx: &StatementContext,
    statement: ExplainPushdownStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_EXPLAIN_PUSHDOWN)?;
    let explainee = plan_explainee(scx, statement.explainee, params)?;
    Ok(Plan::ExplainPushdown(ExplainPushdownPlan { explainee }))
}

pub fn plan_explain_timestamp(
    scx: &StatementContext,
    explain: ExplainTimestampStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let format = match explain.format() {
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
        } = query::plan_root_query(scx, explain.select.query, QueryLifetime::OneShot)?;
        raw_plan.bind_parameters(params)?;

        raw_plan
    };
    let when = query::plan_as_of(scx, explain.select.as_of)?;

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
        // No metrics passed! One more reason not to use this deprecated function.
        expr: expr.lower(scx.catalog.system_vars(), None)?,
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

            // Add the key columns in the order that they're specified.
            for column_name in &key_columns {
                let mut column_ty = relation_desc
                    .get_by_name(column_name)
                    .map(|(_pos, ty)| ty.clone())
                    .ok_or_else(|| PlanError::UnknownColumn {
                        table: None,
                        column: column_name.clone(),
                        similar: Box::new([]),
                    })?;
                if progress {
                    column_ty.nullable = true;
                }
                desc = desc.with_column(column_name, column_ty);
            }

            // Then add the remaining columns in the order from the original
            // table, filtering out the key columns since we added those above.
            for (mut name, mut ty) in relation_desc
                .into_iter()
                .filter(|(name, _ty)| !key_columns.contains(name))
            {
                ty.nullable = true;
                before_values_desc =
                    before_values_desc.with_column(format!("before_{}", name), ty.clone());
                if debezium {
                    name = format!("after_{}", name).into();
                }
                after_values_desc = after_values_desc.with_column(name, ty);
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

pub fn describe_copy_from_table(
    scx: &StatementContext,
    table_name: <Aug as AstInfo>::ItemName,
    columns: Vec<Ident>,
) -> Result<StatementDesc, PlanError> {
    let (_, desc, _) = query::plan_copy_from(scx, table_name, columns)?;
    Ok(StatementDesc::new(Some(desc)))
}

pub fn describe_copy_item(
    scx: &StatementContext,
    object_name: <Aug as AstInfo>::ItemName,
    columns: Vec<Ident>,
) -> Result<StatementDesc, PlanError> {
    let (_, desc, _) = query::plan_copy_item(scx, object_name, columns)?;
    Ok(StatementDesc::new(Some(desc)))
}

pub fn describe_copy(
    scx: &StatementContext,
    CopyStatement {
        relation,
        direction,
        ..
    }: CopyStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(match (relation, direction) {
        (CopyRelation::Named { name, columns }, CopyDirection::To) => {
            describe_copy_item(scx, name, columns)?
        }
        (CopyRelation::Named { name, columns }, CopyDirection::From) => {
            describe_copy_from_table(scx, name, columns)?
        }
        (CopyRelation::Select(stmt), _) => describe_select(scx, stmt)?,
        (CopyRelation::Subscribe(stmt), _) => describe_subscribe(scx, stmt)?,
    }
    .with_is_copy())
}

fn plan_copy_to_expr(
    scx: &StatementContext,
    select_plan: SelectPlan,
    desc: RelationDesc,
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

    let format = match format {
        CopyFormat::Csv => {
            let quote = extract_byte_param_value(options.quote, "quote")?;
            let escape = extract_byte_param_value(options.escape, "escape")?;
            let delimiter = extract_byte_param_value(options.delimiter, "delimiter")?;
            S3SinkFormat::PgCopy(CopyFormatParams::Csv(
                CopyCsvFormatParams::try_new(
                    delimiter,
                    quote,
                    escape,
                    options.header,
                    options.null,
                )
                .map_err(|e| sql_err!("{}", e))?,
            ))
        }
        CopyFormat::Parquet => {
            // Validate that the output desc can be formatted as parquet
            ArrowBuilder::validate_desc(&desc).map_err(|e| sql_err!("{}", e))?;
            S3SinkFormat::Parquet
        }
        CopyFormat::Binary => bail_unsupported!("FORMAT BINARY"),
        CopyFormat::Text => bail_unsupported!("FORMAT TEXT"),
    };

    // Converting the to expr to a HirScalarExpr
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

    let to = plan_expr(ecx, &to_expr)?.type_as(ecx, &ScalarType::String)?;

    if options.max_file_size.as_bytes() < MIN_S3_SINK_FILE_SIZE.as_bytes() {
        sql_bail!(
            "MAX FILE SIZE cannot be less than {}",
            MIN_S3_SINK_FILE_SIZE
        );
    }
    if options.max_file_size.as_bytes() > MAX_S3_SINK_FILE_SIZE.as_bytes() {
        sql_bail!(
            "MAX FILE SIZE cannot be greater than {}",
            MAX_S3_SINK_FILE_SIZE
        );
    }

    Ok(Plan::CopyTo(CopyToPlan {
        select_plan,
        desc,
        to,
        connection: connection.to_owned(),
        connection_id: conn_id,
        format,
        max_file_size: options.max_file_size.as_bytes(),
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

    let params = match format {
        CopyFormat::Text => {
            only_available_with_csv(options.quote, "quote")?;
            only_available_with_csv(options.escape, "escape")?;
            only_available_with_csv(options.header, "HEADER")?;
            let delimiter =
                extract_byte_param_value(options.delimiter, "delimiter")?.unwrap_or(b'\t');
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
        CopyFormat::Parquet => bail_unsupported!("FORMAT PARQUET"),
    };

    let (id, _, columns) = query::plan_copy_from(scx, table_name, columns)?;
    Ok(Plan::CopyFrom(CopyFromPlan {
        id,
        columns,
        params,
    }))
}

fn extract_byte_param_value(v: Option<String>, param_name: &str) -> Result<Option<u8>, PlanError> {
    match v {
        Some(v) if v.len() == 1 => Ok(Some(v.as_bytes()[0])),
        Some(..) => sql_bail!("COPY {} must be a single one-byte character", param_name),
        None => Ok(None),
    }
}

generate_extracted_config!(
    CopyOption,
    (Format, String),
    (Delimiter, String),
    (Null, String),
    (Escape, String),
    (Quote, String),
    (Header, bool),
    (AwsConnection, with_options::Object),
    (MaxFileSize, ByteSize, Default(ByteSize::mb(256)))
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
    // Parse any user-provided FORMAT option. If not provided, will default to
    // Text for COPY TO STDOUT and COPY FROM STDIN, but will error for COPY TO <expr>.
    let format = options
        .format
        .as_ref()
        .map(|format| match format.to_lowercase().as_str() {
            "text" => Ok(CopyFormat::Text),
            "csv" => Ok(CopyFormat::Csv),
            "binary" => Ok(CopyFormat::Binary),
            "parquet" => Ok(CopyFormat::Parquet),
            _ => sql_bail!("unknown FORMAT: {}", format),
        })
        .transpose()?;

    match (&direction, &target) {
        (CopyDirection::To, CopyTarget::Stdout) => {
            if options.delimiter.is_some() {
                sql_bail!("COPY TO does not support DELIMITER option yet");
            }
            if options.quote.is_some() {
                sql_bail!("COPY TO does not support QUOTE option yet");
            }
            if options.null.is_some() {
                sql_bail!("COPY TO does not support NULL option yet");
            }
            match relation {
                CopyRelation::Named { .. } => sql_bail!("named with COPY TO STDOUT unsupported"),
                CopyRelation::Select(stmt) => Ok(plan_select(
                    scx,
                    stmt,
                    &Params::empty(),
                    Some(format.unwrap_or(CopyFormat::Text)),
                )?),
                CopyRelation::Subscribe(stmt) => Ok(plan_subscribe(
                    scx,
                    stmt,
                    &Params::empty(),
                    Some(format.unwrap_or(CopyFormat::Text)),
                )?),
            }
        }
        (CopyDirection::From, CopyTarget::Stdin) => match relation {
            CopyRelation::Named { name, columns } => plan_copy_from(
                scx,
                name,
                columns,
                format.unwrap_or(CopyFormat::Text),
                options,
            ),
            _ => sql_bail!("COPY FROM {} not supported", target),
        },
        (CopyDirection::To, CopyTarget::Expr(to_expr)) => {
            // System users are always allowed to use this feature, even when
            // the flag is disabled, so that we can dogfood for analytics in
            // production environments. The feature is stable enough that we're
            // not worried about it crashing.
            if !scx.catalog.active_role_id().is_system() {
                scx.require_feature_flag(&vars::ENABLE_COPY_TO_EXPR)?;
            }

            let format = match format {
                Some(inner) => inner,
                _ => sql_bail!("COPY TO <expr> requires a FORMAT option"),
            };

            let stmt = match relation {
                CopyRelation::Named { name, columns } => {
                    if !columns.is_empty() {
                        // TODO(mouli): Add support for this
                        sql_bail!("specifying columns for COPY <table_name> TO commands not yet supported; use COPY (SELECT...) TO ... instead");
                    }
                    // Generate a synthetic SELECT query that just gets the table
                    let query = Query {
                        ctes: CteBlock::empty(),
                        body: SetExpr::Table(name),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    };
                    SelectStatement { query, as_of: None }
                }
                CopyRelation::Select(stmt) => {
                    if !stmt.query.order_by.is_empty() {
                        sql_bail!("ORDER BY is not supported in SELECT query for COPY statements")
                    }
                    stmt
                }
                _ => sql_bail!("COPY {} {} not supported", direction, target),
            };

            let (plan, desc) = plan_select_inner(scx, stmt, &Params::empty(), None)?;
            plan_copy_to_expr(scx, plan, desc, to_expr, format, options)
        }
        _ => sql_bail!("COPY {} {} not supported", direction, target),
    }
}
