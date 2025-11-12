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

use mz_arrow_util::builder::ArrowBuilder;
use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, RowSetFinishing};
use mz_ore::num::NonNeg;
use mz_ore::soft_panic_or_log;
use mz_ore::str::separated;
use mz_pgcopy::{CopyCsvFormatParams, CopyFormatParams, CopyTextFormatParams};
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::bytes::ByteSize;
use mz_repr::explain::{ExplainConfig, ExplainFormat};
use mz_repr::optimize::OptimizerFeatureOverrides;
use mz_repr::{CatalogItemId, Datum, RelationDesc, Row, SqlRelationType, SqlScalarType};
use mz_sql_parser::ast::{
    CteBlock, ExplainAnalyzeClusterStatement, ExplainAnalyzeComputationProperties,
    ExplainAnalyzeComputationProperty, ExplainAnalyzeObjectStatement, ExplainAnalyzeProperty,
    ExplainPlanOption, ExplainPlanOptionName, ExplainPushdownStatement, ExplainSinkSchemaFor,
    ExplainSinkSchemaStatement, ExplainTimestampStatement, Expr, IfExistsBehavior, OrderByExpr,
    SetExpr, SubscribeOutput, UnresolvedItemName,
};
use mz_sql_parser::ident;
use mz_storage_types::sinks::{
    KafkaSinkConnection, KafkaSinkFormat, KafkaSinkFormatType, MAX_S3_SINK_FILE_SIZE,
    MIN_S3_SINK_FILE_SIZE, S3SinkFormat, StorageSinkConnection,
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
use crate::plan::query::{
    ExprContext, QueryLifetime, offset_into_value, plan_as_of_or_up_to, plan_expr,
};
use crate::plan::scope::Scope;
use crate::plan::statement::show::ShowSelect;
use crate::plan::statement::{StatementContext, StatementDesc, ddl};
use crate::plan::{
    self, CopyFromFilter, CopyToPlan, CreateSinkPlan, ExplainPushdownPlan, ExplainSinkSchemaPlan,
    ExplainTimestampPlan, HirRelationExpr, HirScalarExpr, side_effecting_func, transform_ast,
};
use crate::plan::{
    CopyFormat, CopyFromPlan, ExplainPlanPlan, InsertPlan, MutationKind, Params, Plan, PlanError,
    QueryContext, ReadThenWritePlan, SelectPlan, SubscribeFrom, SubscribePlan, query,
};
use crate::plan::{CopyFromSource, with_options};
use crate::session::vars::{self, ENABLE_COPY_FROM_REMOTE};

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
    expr.bind_parameters(scx, QueryLifetime::OneShot, params)?;
    let returning = returning
        .expr
        .into_iter()
        .map(|mut expr| {
            expr.bind_parameters(scx, QueryLifetime::OneShot, params)?;
            expr.lower_uncorrelated()
        })
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
    plan_read_then_write(scx, MutationKind::Delete, params, rtw_plan)
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
    plan_read_then_write(scx, MutationKind::Update, params, rtw_plan)
}

pub fn plan_read_then_write(
    scx: &StatementContext,
    kind: MutationKind,
    params: &Params,
    query::ReadThenWritePlan {
        id,
        mut selection,
        finishing,
        assignments,
    }: query::ReadThenWritePlan,
) -> Result<Plan, PlanError> {
    selection.bind_parameters(scx, QueryLifetime::OneShot, params)?;
    let mut assignments_outer = BTreeMap::new();
    for (idx, mut set) in assignments {
        set.bind_parameters(scx, QueryLifetime::OneShot, params)?;
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
    let lifetime = QueryLifetime::OneShot;
    let query::PlannedRootQuery {
        mut expr,
        desc,
        finishing,
        scope: _,
    } = query::plan_root_query(scx, select.query.clone(), lifetime)?;
    expr.bind_parameters(scx, lifetime, params)?;

    // OFFSET clauses in `expr` should become constants with the above binding of parameters.
    // Let's check this and simplify them to literals.
    expr.try_visit_mut_pre(&mut |expr| {
        if let HirRelationExpr::TopK { offset, .. } = expr {
            let offset_value = offset_into_value(offset.take())?;
            *offset = HirScalarExpr::literal(Datum::Int64(offset_value), SqlScalarType::Int64);
        }
        Ok::<(), PlanError>(())
    })?;
    // (We don't need to simplify LIMIT clauses in `expr`, because we can handle non-constant
    // expressions there. If they happen to be simplifiable to literals, then the optimizer will do
    // so later.)

    // We need to concretize the `limit` and `offset` of the RowSetFinishing, so that we go from
    // `RowSetFinishing<HirScalarExpr, HirScalarExpr>` to `RowSetFinishing`.
    // This involves binding parameters and evaluating each expression to a number.
    // (This should be possible even for `limit` here, because we are at the top level of a SELECT,
    // so this `limit` has to be a constant.)
    let limit = match finishing.limit {
        None => None,
        Some(mut limit) => {
            limit.bind_parameters(scx, lifetime, params)?;
            // TODO: Call `try_into_literal_int64` instead of `as_literal`.
            let Some(limit) = limit.as_literal() else {
                sql_bail!(
                    "Top-level LIMIT must be a constant expression, got {}",
                    limit
                )
            };
            match limit {
                Datum::Null => None,
                Datum::Int64(v) if v >= 0 => NonNeg::<i64>::try_from(v).ok(),
                _ => {
                    soft_panic_or_log!("Valid literal limit must be asserted in `plan_select`");
                    sql_bail!("LIMIT must be a non-negative INT or NULL")
                }
            }
        }
    };
    let offset = {
        let mut offset = finishing.offset.clone();
        offset.bind_parameters(scx, lifetime, params)?;
        let offset = offset_into_value(offset.take())?;
        offset
            .try_into()
            .expect("checked in offset_into_value that it is not negative")
    };

    let plan = SelectPlan {
        source: expr,
        when,
        finishing: RowSetFinishing {
            limit,
            offset,
            project: finishing.project,
            order_by: finishing.order_by,
        },
        copy_to,
        select: Some(Box::new(select)),
    };

    Ok((plan, desc))
}

pub fn describe_explain_plan(
    scx: &StatementContext,
    explain: ExplainPlanStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let mut relation_desc = RelationDesc::builder();

    match explain.stage() {
        ExplainStage::RawPlan => {
            let name = "Raw Plan";
            relation_desc = relation_desc.with_column(name, SqlScalarType::String.nullable(false));
        }
        ExplainStage::DecorrelatedPlan => {
            let name = "Decorrelated Plan";
            relation_desc = relation_desc.with_column(name, SqlScalarType::String.nullable(false));
        }
        ExplainStage::LocalPlan => {
            let name = "Locally Optimized Plan";
            relation_desc = relation_desc.with_column(name, SqlScalarType::String.nullable(false));
        }
        ExplainStage::GlobalPlan => {
            let name = "Optimized Plan";
            relation_desc = relation_desc.with_column(name, SqlScalarType::String.nullable(false));
        }
        ExplainStage::PhysicalPlan => {
            let name = "Physical Plan";
            relation_desc = relation_desc.with_column(name, SqlScalarType::String.nullable(false));
        }
        ExplainStage::Trace => {
            relation_desc = relation_desc
                .with_column("Time", SqlScalarType::UInt64.nullable(false))
                .with_column("Path", SqlScalarType::String.nullable(false))
                .with_column("Plan", SqlScalarType::String.nullable(false));
        }
        ExplainStage::PlanInsights => {
            let name = "Plan Insights";
            relation_desc = relation_desc.with_column(name, SqlScalarType::String.nullable(false));
        }
    };
    let relation_desc = relation_desc.finish();

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
    let relation_desc = RelationDesc::builder()
        .with_column("Source", SqlScalarType::String.nullable(false))
        .with_column("Total Bytes", SqlScalarType::UInt64.nullable(false))
        .with_column("Selected Bytes", SqlScalarType::UInt64.nullable(false))
        .with_column("Total Parts", SqlScalarType::UInt64.nullable(false))
        .with_column("Selected Parts", SqlScalarType::UInt64.nullable(false))
        .finish();

    Ok(
        StatementDesc::new(Some(relation_desc)).with_params(match statement.explainee {
            Explainee::Select(select, _) => describe_select(scx, *select)?.param_types,
            _ => vec![],
        }),
    )
}

pub fn describe_explain_analyze_object(
    _scx: &StatementContext,
    statement: ExplainAnalyzeObjectStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    if statement.as_sql {
        let relation_desc = RelationDesc::builder()
            .with_column("SQL", SqlScalarType::String.nullable(false))
            .finish();
        return Ok(StatementDesc::new(Some(relation_desc)));
    }

    match statement.properties {
        ExplainAnalyzeProperty::Computation(ExplainAnalyzeComputationProperties {
            properties,
            skew,
        }) => {
            let mut relation_desc = RelationDesc::builder()
                .with_column("operator", SqlScalarType::String.nullable(false));

            if skew {
                relation_desc =
                    relation_desc.with_column("worker_id", SqlScalarType::UInt64.nullable(true));
            }

            let mut seen_properties = BTreeSet::new();
            for property in properties {
                // handle each property only once (belt and suspenders)
                if !seen_properties.insert(property) {
                    continue;
                }

                match property {
                    ExplainAnalyzeComputationProperty::Memory if skew => {
                        let numeric = SqlScalarType::Numeric { max_scale: None }.nullable(true);
                        relation_desc = relation_desc
                            .with_column("memory_ratio", numeric.clone())
                            .with_column("worker_memory", SqlScalarType::String.nullable(true))
                            .with_column("avg_memory", SqlScalarType::String.nullable(true))
                            .with_column("total_memory", SqlScalarType::String.nullable(true))
                            .with_column("records_ratio", numeric.clone())
                            .with_column("worker_records", numeric.clone())
                            .with_column("avg_records", numeric.clone())
                            .with_column("total_records", numeric);
                    }
                    ExplainAnalyzeComputationProperty::Memory => {
                        relation_desc = relation_desc
                            .with_column("total_memory", SqlScalarType::String.nullable(true))
                            .with_column(
                                "total_records",
                                SqlScalarType::Numeric { max_scale: None }.nullable(true),
                            );
                    }
                    ExplainAnalyzeComputationProperty::Cpu => {
                        if skew {
                            relation_desc = relation_desc
                                .with_column(
                                    "cpu_ratio",
                                    SqlScalarType::Numeric { max_scale: None }.nullable(true),
                                )
                                .with_column(
                                    "worker_elapsed",
                                    SqlScalarType::Interval.nullable(true),
                                )
                                .with_column("avg_elapsed", SqlScalarType::Interval.nullable(true));
                        }
                        relation_desc = relation_desc
                            .with_column("total_elapsed", SqlScalarType::Interval.nullable(true));
                    }
                }
            }

            let relation_desc = relation_desc.finish();
            Ok(StatementDesc::new(Some(relation_desc)))
        }
        ExplainAnalyzeProperty::Hints => {
            let relation_desc = RelationDesc::builder()
                .with_column("operator", SqlScalarType::String.nullable(true))
                .with_column("levels", SqlScalarType::Int64.nullable(true))
                .with_column("to_cut", SqlScalarType::Int64.nullable(true))
                .with_column("hint", SqlScalarType::Float64.nullable(true))
                .with_column("savings", SqlScalarType::String.nullable(true))
                .finish();
            Ok(StatementDesc::new(Some(relation_desc)))
        }
    }
}

pub fn describe_explain_analyze_cluster(
    _scx: &StatementContext,
    statement: ExplainAnalyzeClusterStatement,
) -> Result<StatementDesc, PlanError> {
    if statement.as_sql {
        let relation_desc = RelationDesc::builder()
            .with_column("SQL", SqlScalarType::String.nullable(false))
            .finish();
        return Ok(StatementDesc::new(Some(relation_desc)));
    }

    let ExplainAnalyzeComputationProperties { properties, skew } = statement.properties;

    let mut relation_desc = RelationDesc::builder()
        .with_column("object", SqlScalarType::String.nullable(false))
        .with_column("global_id", SqlScalarType::String.nullable(false));

    if skew {
        relation_desc =
            relation_desc.with_column("worker_id", SqlScalarType::UInt64.nullable(true));
    }

    let mut seen_properties = BTreeSet::new();
    for property in properties {
        // handle each property only once (belt and suspenders)
        if !seen_properties.insert(property) {
            continue;
        }

        match property {
            ExplainAnalyzeComputationProperty::Memory if skew => {
                let numeric = SqlScalarType::Numeric { max_scale: None }.nullable(true);
                relation_desc = relation_desc
                    .with_column("max_operator_memory_ratio", numeric.clone())
                    .with_column("worker_memory", SqlScalarType::String.nullable(true))
                    .with_column("avg_memory", SqlScalarType::String.nullable(true))
                    .with_column("total_memory", SqlScalarType::String.nullable(true))
                    .with_column("max_operator_records_ratio", numeric.clone())
                    .with_column("worker_records", numeric.clone())
                    .with_column("avg_records", numeric.clone())
                    .with_column("total_records", numeric);
            }
            ExplainAnalyzeComputationProperty::Memory => {
                relation_desc = relation_desc
                    .with_column("total_memory", SqlScalarType::String.nullable(true))
                    .with_column(
                        "total_records",
                        SqlScalarType::Numeric { max_scale: None }.nullable(true),
                    );
            }
            ExplainAnalyzeComputationProperty::Cpu => {
                if skew {
                    relation_desc = relation_desc
                        .with_column(
                            "max_operator_cpu_ratio",
                            SqlScalarType::Numeric { max_scale: None }.nullable(true),
                        )
                        .with_column("worker_elapsed", SqlScalarType::Interval.nullable(true))
                        .with_column("avg_elapsed", SqlScalarType::Interval.nullable(true));
                }
                relation_desc = relation_desc
                    .with_column("total_elapsed", SqlScalarType::Interval.nullable(true));
            }
        }
    }

    Ok(StatementDesc::new(Some(relation_desc.finish())))
}

pub fn describe_explain_timestamp(
    scx: &StatementContext,
    ExplainTimestampStatement { select, .. }: ExplainTimestampStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let relation_desc = RelationDesc::builder()
        .with_column("Timestamp", SqlScalarType::String.nullable(false))
        .finish();

    Ok(StatementDesc::new(Some(relation_desc))
        .with_params(describe_select(scx, select)?.param_types))
}

pub fn describe_explain_schema(
    _: &StatementContext,
    ExplainSinkSchemaStatement { .. }: ExplainSinkSchemaStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    let relation_desc = RelationDesc::builder()
        .with_column("Schema", SqlScalarType::String.nullable(false))
        .finish();
    Ok(StatementDesc::new(Some(relation_desc)))
}

// Currently, there are two reasons for why a flag should be `Option<bool>` instead of simply
// `bool`:
// - When it's an override of a global feature flag, for example optimizer feature flags. In this
//   case, we need not just false and true, but also None to say "take the value of the global
//   flag".
// - When it's an override of whether SOFT_ASSERTIONS are enabled. For example, when `Arity` is not
//   explicitly given in the EXPLAIN command, then we'd like staging and prod to default to true,
//   but otherwise we'd like to default to false.
generate_extracted_config!(
    ExplainPlanOption,
    (Arity, Option<bool>, Default(None)),
    (Cardinality, bool, Default(false)),
    (ColumnNames, bool, Default(false)),
    (FilterPushdown, Option<bool>, Default(None)),
    (HumanizedExpressions, Option<bool>, Default(None)),
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
    (Equivalences, bool, Default(false)),
    (ReoptimizeImportedViews, Option<bool>, Default(None)),
    (EnableNewOuterJoinLowering, Option<bool>, Default(None)),
    (EnableEagerDeltaJoins, Option<bool>, Default(None)),
    (EnableVariadicLeftJoinLowering, Option<bool>, Default(None)),
    (EnableLetrecFixpointAnalysis, Option<bool>, Default(None)),
    (EnableJoinPrioritizeArranged, Option<bool>, Default(None)),
    (
        EnableProjectionPushdownAfterRelationCse,
        Option<bool>,
        Default(None)
    )
);

impl TryFrom<ExplainPlanOptionExtracted> for ExplainConfig {
    type Error = PlanError;

    fn try_from(mut v: ExplainPlanOptionExtracted) -> Result<Self, Self::Error> {
        // If `WITH(raw)` is specified, ensure that the config will be as
        // representative for the original plan as possible.
        if v.raw {
            v.raw_plans = true;
            v.raw_syntax = true;
        }

        // Certain config should default to be enabled in release builds running on
        // staging or prod (where SOFT_ASSERTIONS are turned off).
        let enable_on_prod = !mz_ore::assert::soft_assertions_enabled();

        Ok(ExplainConfig {
            arity: v.arity.unwrap_or(enable_on_prod),
            cardinality: v.cardinality,
            column_names: v.column_names,
            filter_pushdown: v.filter_pushdown.unwrap_or(enable_on_prod),
            humanized_exprs: !v.raw_plans && (v.humanized_expressions.unwrap_or(enable_on_prod)),
            join_impls: v.join_implementations,
            keys: v.keys,
            linear_chains: !v.raw_plans && v.linear_chains,
            no_fast_path: v.no_fast_path,
            no_notices: v.no_notices,
            node_ids: v.node_identifiers,
            non_negative: v.non_negative,
            raw_plans: v.raw_plans,
            raw_syntax: v.raw_syntax,
            verbose_syntax: false,
            redacted: v.redacted,
            subtree_size: v.subtree_size,
            equivalences: v.equivalences,
            timing: v.timing,
            types: v.types,
            // The ones that are initialized with `Default::default()` are not wired up to EXPLAIN.
            features: OptimizerFeatureOverrides {
                enable_guard_subquery_tablefunc: Default::default(),
                enable_eager_delta_joins: v.enable_eager_delta_joins,
                enable_new_outer_join_lowering: v.enable_new_outer_join_lowering,
                enable_variadic_left_join_lowering: v.enable_variadic_left_join_lowering,
                enable_letrec_fixpoint_analysis: v.enable_letrec_fixpoint_analysis,
                enable_consolidate_after_union_negate: Default::default(),
                enable_reduce_mfp_fusion: Default::default(),
                enable_cardinality_estimates: Default::default(),
                persist_fast_path_limit: Default::default(),
                reoptimize_imported_views: v.reoptimize_imported_views,
                enable_reduce_reduction: Default::default(),
                enable_join_prioritize_arranged: v.enable_join_prioritize_arranged,
                enable_projection_pushdown_after_relation_cse: v
                    .enable_projection_pushdown_after_relation_cse,
                enable_less_reduce_in_eqprop: Default::default(),
                enable_dequadratic_eqprop_map: Default::default(),
                enable_eq_classes_withholding_errors: Default::default(),
                enable_fast_path_plan_insights: Default::default(),
                enable_repr_typecheck: Default::default(),
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

            let Plan::CreateView(plan) = ddl::plan_create_view(scx, *stmt)? else {
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
                ddl::plan_create_materialized_view(scx, *stmt)?
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
    let (format, verbose_syntax) = match explain.format() {
        mz_sql_parser::ast::ExplainFormat::Text => (ExplainFormat::Text, false),
        mz_sql_parser::ast::ExplainFormat::VerboseText => (ExplainFormat::Text, true),
        mz_sql_parser::ast::ExplainFormat::Json => (ExplainFormat::Json, false),
        mz_sql_parser::ast::ExplainFormat::Dot => (ExplainFormat::Dot, false),
    };
    let stage = explain.stage();

    // Plan ExplainConfig.
    let mut config = {
        let mut with_options = ExplainPlanOptionExtracted::try_from(explain.with_options)?;

        if !scx.catalog.system_vars().persist_stats_filter_enabled() {
            // If filtering is disabled, explain plans should not include pushdown info.
            with_options.filter_pushdown = Some(false);
        }

        ExplainConfig::try_from(with_options)?
    };
    config.verbose_syntax = verbose_syntax;

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

    crate::pure::purify_create_sink_avro_doc_on_options(
        scx.catalog,
        *statement.from.item_id(),
        &mut statement.format,
    )?;

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

pub fn plan_explain_analyze_object(
    scx: &StatementContext,
    statement: ExplainAnalyzeObjectStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let explainee_name = statement
        .explainee
        .name()
        .ok_or_else(|| sql_err!("EXPLAIN ANALYZE on anonymous dataflows",))?
        .full_name_str();
    let explainee = plan_explainee(scx, statement.explainee, params)?;

    match explainee {
        plan::Explainee::Index(_index_id) => (),
        plan::Explainee::MaterializedView(_item_id) => (),
        _ => {
            return Err(sql_err!("EXPLAIN ANALYZE queries for this explainee type",));
        }
    };

    // generate SQL query

    /* WITH {CTEs}
       SELECT REPEAT(' ', nesting * 2) || operator AS operator
             {columns}
        FROM      mz_introspection.mz_lir_mapping mlm
             JOIN {from} USING (lir_id)
             JOIN mz_introspection.mz_mappable_objects mo
               ON (mlm.global_id = mo.global_id)
       WHERE     mo.name = '{plan.explainee_name}'
             AND {predicates}
       ORDER BY lir_id DESC
    */
    let mut ctes = Vec::with_capacity(4); // max 2 per ExplainAnalyzeComputationProperty
    let mut columns = vec!["REPEAT(' ', nesting * 2) || operator AS operator"];
    let mut from = vec!["mz_introspection.mz_lir_mapping mlm"];
    let mut predicates = vec![format!("mo.name = '{}'", explainee_name)];
    let mut order_by = vec!["mlm.lir_id DESC"];

    match statement.properties {
        ExplainAnalyzeProperty::Computation(ExplainAnalyzeComputationProperties {
            properties,
            skew,
        }) => {
            let mut worker_id = None;
            let mut seen_properties = BTreeSet::new();
            for property in properties {
                // handle each property only once (belt and suspenders)
                if !seen_properties.insert(property) {
                    continue;
                }

                match property {
                    ExplainAnalyzeComputationProperty::Memory => {
                        ctes.push((
                            "summary_memory",
                            r#"
  SELECT mlm.global_id AS global_id,
         mlm.lir_id AS lir_id,
         SUM(mas.size) AS total_memory,
         SUM(mas.records) AS total_records,
         CASE WHEN COUNT(DISTINCT mas.worker_id) <> 0 THEN SUM(mas.size) / COUNT(DISTINCT mas.worker_id) ELSE NULL END AS avg_memory,
         CASE WHEN COUNT(DISTINCT mas.worker_id) <> 0 THEN SUM(mas.records) / COUNT(DISTINCT mas.worker_id) ELSE NULL END AS avg_records
    FROM            mz_introspection.mz_lir_mapping mlm
         CROSS JOIN generate_series((mlm.operator_id_start) :: int8, (mlm.operator_id_end - 1) :: int8) AS valid_id
               JOIN mz_introspection.mz_arrangement_sizes_per_worker mas
                 ON (mas.operator_id = valid_id)
GROUP BY mlm.global_id, mlm.lir_id"#,
                        ));
                        from.push("LEFT JOIN summary_memory sm USING (global_id, lir_id)");

                        if skew {
                            ctes.push((
                                "per_worker_memory",
                                r#"
  SELECT mlm.global_id AS global_id,
         mlm.lir_id AS lir_id,
         mas.worker_id AS worker_id,
         SUM(mas.size) AS worker_memory,
         SUM(mas.records) AS worker_records
    FROM            mz_introspection.mz_lir_mapping mlm
         CROSS JOIN generate_series((mlm.operator_id_start) :: int8, (mlm.operator_id_end - 1) :: int8) AS valid_id
               JOIN mz_introspection.mz_arrangement_sizes_per_worker mas
                 ON (mas.operator_id = valid_id)
GROUP BY mlm.global_id, mlm.lir_id, mas.worker_id"#,
                            ));
                            from.push("LEFT JOIN per_worker_memory pwm USING (global_id, lir_id)");

                            if let Some(worker_id) = worker_id {
                                predicates.push(format!("pwm.worker_id = {worker_id}"));
                            } else {
                                worker_id = Some("pwm.worker_id");
                                columns.push("pwm.worker_id AS worker_id");
                                order_by.push("worker_id");
                            }

                            columns.extend([
                                "CASE WHEN pwm.worker_id IS NOT NULL AND sm.avg_memory <> 0 THEN ROUND(pwm.worker_memory / sm.avg_memory, 2) ELSE NULL END AS memory_ratio",
                                "pg_size_pretty(pwm.worker_memory) AS worker_memory",
                                "pg_size_pretty(sm.avg_memory) AS avg_memory",
                                "pg_size_pretty(sm.total_memory) AS total_memory",
                                "CASE WHEN pwm.worker_id IS NOT NULL AND sm.avg_records <> 0 THEN ROUND(pwm.worker_records / sm.avg_records, 2) ELSE NULL END AS records_ratio",
                                "pwm.worker_records AS worker_records",
                                "sm.avg_records AS avg_records",
                                "sm.total_records AS total_records",
                            ]);
                        } else {
                            columns.extend([
                                "pg_size_pretty(sm.total_memory) AS total_memory",
                                "sm.total_records AS total_records",
                            ]);
                        }
                    }
                    ExplainAnalyzeComputationProperty::Cpu => {
                        ctes.push((
                            "summary_cpu",
                            r#"
  SELECT mlm.global_id AS global_id,
         mlm.lir_id AS lir_id,
         SUM(mse.elapsed_ns) AS total_ns,
         CASE WHEN COUNT(DISTINCT mse.worker_id) <> 0 THEN SUM(mse.elapsed_ns) / COUNT(DISTINCT mse.worker_id) ELSE NULL END AS avg_ns
    FROM            mz_introspection.mz_lir_mapping mlm
         CROSS JOIN generate_series((mlm.operator_id_start) :: int8, (mlm.operator_id_end - 1) :: int8) AS valid_id
               JOIN mz_introspection.mz_scheduling_elapsed_per_worker mse
                 ON (mse.id = valid_id)
GROUP BY mlm.global_id, mlm.lir_id"#,
                        ));
                        from.push("LEFT JOIN summary_cpu sc USING (global_id, lir_id)");

                        if skew {
                            ctes.push((
                                "per_worker_cpu",
                                r#"
  SELECT mlm.global_id AS global_id,
         mlm.lir_id AS lir_id,
         mse.worker_id AS worker_id,
         SUM(mse.elapsed_ns) AS worker_ns
    FROM            mz_introspection.mz_lir_mapping mlm
         CROSS JOIN generate_series((mlm.operator_id_start) :: int8, (mlm.operator_id_end - 1) :: int8) AS valid_id
               JOIN mz_introspection.mz_scheduling_elapsed_per_worker mse
                 ON (mse.id = valid_id)
GROUP BY mlm.global_id, mlm.lir_id, mse.worker_id"#,
                            ));
                            from.push("LEFT JOIN per_worker_cpu pwc USING (global_id, lir_id)");

                            if let Some(worker_id) = worker_id {
                                predicates.push(format!("pwc.worker_id = {worker_id}"));
                            } else {
                                worker_id = Some("pwc.worker_id");
                                columns.push("pwc.worker_id AS worker_id");
                                order_by.push("worker_id");
                            }

                            columns.extend([
                                "CASE WHEN pwc.worker_id IS NOT NULL AND sc.avg_ns <> 0 THEN ROUND(pwc.worker_ns / sc.avg_ns, 2) ELSE NULL END AS cpu_ratio",
                                "pwc.worker_ns / 1000 * '1 microsecond'::INTERVAL AS worker_elapsed",
                                "sc.avg_ns / 1000 * '1 microsecond'::INTERVAL AS avg_elapsed",
                            ]);
                        }
                        columns.push(
                            "sc.total_ns / 1000 * '1 microsecond'::INTERVAL AS total_elapsed",
                        );
                    }
                }
            }
        }
        ExplainAnalyzeProperty::Hints => {
            columns.extend([
                "megsa.levels AS levels",
                "megsa.to_cut AS to_cut",
                "megsa.hint AS hint",
                "pg_size_pretty(megsa.savings) AS savings",
            ]);
            from.extend(["JOIN mz_introspection.mz_dataflow_global_ids mdgi ON (mlm.global_id = mdgi.global_id)",
            "LEFT JOIN (generate_series((mlm.operator_id_start) :: int8, (mlm.operator_id_end - 1) :: int8) AS valid_id JOIN \
             mz_introspection.mz_expected_group_size_advice megsa ON (megsa.region_id = valid_id)) ON (megsa.dataflow_id = mdgi.id)"]);
        }
    }

    from.push("JOIN mz_introspection.mz_mappable_objects mo ON (mlm.global_id = mo.global_id)");

    let ctes = if !ctes.is_empty() {
        format!(
            "WITH {}",
            separated(
                ",\n",
                ctes.iter()
                    .map(|(name, defn)| format!("{name} AS ({defn})"))
            )
        )
    } else {
        String::new()
    };
    let columns = separated(", ", columns);
    let from = separated(" ", from);
    let predicates = separated(" AND ", predicates);
    let order_by = separated(", ", order_by);
    let query = format!(
        r#"{ctes}
SELECT {columns}
FROM {from}
WHERE {predicates}
ORDER BY {order_by}"#
    );

    if statement.as_sql {
        let rows = vec![Row::pack_slice(&[Datum::String(
            &mz_sql_pretty::pretty_str_simple(&query, 80).map_err(|e| {
                PlanError::Unstructured(format!("internal error parsing our own SQL: {e}"))
            })?,
        )])];
        let typ = SqlRelationType::new(vec![SqlScalarType::String.nullable(false)]);

        Ok(Plan::Select(SelectPlan::immediate(rows, typ)))
    } else {
        let (show_select, _resolved_ids) = ShowSelect::new_from_bare_query(scx, query)?;
        show_select.plan()
    }
}

pub fn plan_explain_analyze_cluster(
    scx: &StatementContext,
    statement: ExplainAnalyzeClusterStatement,
    _params: &Params,
) -> Result<Plan, PlanError> {
    // object string
    // worker_id uint64        (if           skew)
    // memory_ratio numeric    (if memory && skew)
    // worker_memory string    (if memory && skew)
    // avg_memory string       (if memory && skew)
    // total_memory string     (if memory)
    // records_ratio numeric   (if memory && skew)
    // worker_records          (if memory && skew)
    // avg_records numeric     (if memory && skew)
    // total_records numeric   (if memory)
    // cpu_ratio numeric       (if cpu    && skew)
    // worker_elapsed interval (if cpu    && skew)
    // avg_elapsed interval    (if cpu    && skew)
    // total_elapsed interval  (if cpu)

    /* WITH {CTEs}
       SELECT mo.name AS object
             {columns}
        FROM mz_introspection.mz_mappable_objects mo
             {from}
       WHERE {predicates}
       ORDER BY {order_by}, mo.name DESC
    */
    let mut ctes = Vec::with_capacity(4); // max 2 per ExplainAnalyzeComputationProperty
    let mut columns = vec!["mo.name AS object", "mo.global_id AS global_id"];
    let mut from = vec!["mz_introspection.mz_mappable_objects mo"];
    let mut predicates = vec![];
    let mut order_by = vec![];

    let ExplainAnalyzeComputationProperties { properties, skew } = statement.properties;
    let mut worker_id = None;
    let mut seen_properties = BTreeSet::new();
    for property in properties {
        // handle each property only once (belt and suspenders)
        if !seen_properties.insert(property) {
            continue;
        }

        match property {
            ExplainAnalyzeComputationProperty::Memory => {
                if skew {
                    let mut set_worker_id = false;
                    if let Some(worker_id) = worker_id {
                        // join condition if we're showing skew for more than one property
                        predicates.push(format!("om.worker_id = {worker_id}"));
                    } else {
                        worker_id = Some("om.worker_id");
                        columns.push("om.worker_id AS worker_id");
                        set_worker_id = true; // we'll add ourselves to `order_by` later
                    };

                    // computes the average memory per LIR operator (for per operator ratios)
                    ctes.push((
                    "per_operator_memory_summary",
                    r#"
SELECT mlm.global_id AS global_id,
       mlm.lir_id AS lir_id,
       SUM(mas.size) AS total_memory,
       SUM(mas.records) AS total_records,
       CASE WHEN COUNT(DISTINCT mas.worker_id) <> 0 THEN SUM(mas.size) / COUNT(DISTINCT mas.worker_id) ELSE NULL END AS avg_memory,
       CASE WHEN COUNT(DISTINCT mas.worker_id) <> 0 THEN SUM(mas.records) / COUNT(DISTINCT mas.worker_id) ELSE NULL END AS avg_records
FROM        mz_introspection.mz_lir_mapping mlm
 CROSS JOIN generate_series((mlm.operator_id_start) :: int8, (mlm.operator_id_end - 1) :: int8) AS valid_id
       JOIN mz_introspection.mz_arrangement_sizes_per_worker mas
         ON (mas.operator_id = valid_id)
GROUP BY mlm.global_id, mlm.lir_id"#,
                ));

                    // computes the memory per worker in a per operator way
                    ctes.push((
                    "per_operator_memory_per_worker",
                    r#"
SELECT mlm.global_id AS global_id,
       mlm.lir_id AS lir_id,
       mas.worker_id AS worker_id,
       SUM(mas.size) AS worker_memory,
       SUM(mas.records) AS worker_records
FROM        mz_introspection.mz_lir_mapping mlm
 CROSS JOIN generate_series((mlm.operator_id_start) :: int8, (mlm.operator_id_end - 1) :: int8) AS valid_id
       JOIN mz_introspection.mz_arrangement_sizes_per_worker mas
         ON (mas.operator_id = valid_id)
GROUP BY mlm.global_id, mlm.lir_id, mas.worker_id"#,
                    ));

                    // computes memory ratios per worker per operator
                    ctes.push((
                    "per_operator_memory_ratios",
                    r#"
SELECT pompw.global_id AS global_id,
       pompw.lir_id AS lir_id,
       pompw.worker_id AS worker_id,
       CASE WHEN pompw.worker_id IS NOT NULL AND poms.avg_memory <> 0 THEN ROUND(pompw.worker_memory / poms.avg_memory, 2) ELSE NULL END AS memory_ratio,
       CASE WHEN pompw.worker_id IS NOT NULL AND poms.avg_records <> 0 THEN ROUND(pompw.worker_records / poms.avg_records, 2) ELSE NULL END AS records_ratio
  FROM      per_operator_memory_per_worker pompw
       JOIN per_operator_memory_summary poms
         USING (global_id, lir_id)
"#,
                    ));

                    // summarizes each object, per worker
                    ctes.push((
                        "object_memory",
                        r#"
SELECT pompw.global_id AS global_id,
       pompw.worker_id AS worker_id,
       MAX(pomr.memory_ratio) AS max_operator_memory_ratio,
       MAX(pomr.records_ratio) AS max_operator_records_ratio,
       SUM(pompw.worker_memory) AS worker_memory,
       SUM(pompw.worker_records) AS worker_records
FROM        per_operator_memory_per_worker pompw
     JOIN   per_operator_memory_ratios pomr
     USING (global_id, worker_id, lir_id)
GROUP BY pompw.global_id, pompw.worker_id
"#,
                    ));

                    // summarizes each worker
                    ctes.push(("object_average_memory", r#"
SELECT om.global_id AS global_id,
       SUM(om.worker_memory) AS total_memory,
       CASE WHEN COUNT(DISTINCT om.worker_id) <> 0 THEN SUM(om.worker_memory) / COUNT(DISTINCT om.worker_id) ELSE NULL END AS avg_memory,
       SUM(om.worker_records) AS total_records,
       CASE WHEN COUNT(DISTINCT om.worker_id) <> 0 THEN SUM(om.worker_records) / COUNT(DISTINCT om.worker_id) ELSE NULL END AS avg_records
  FROM object_memory om
GROUP BY om.global_id"#));

                    from.push("LEFT JOIN object_memory om USING (global_id)");
                    from.push("LEFT JOIN object_average_memory oam USING (global_id)");

                    columns.extend([
                        "om.max_operator_memory_ratio AS max_operator_memory_ratio",
                        "pg_size_pretty(om.worker_memory) AS worker_memory",
                        "pg_size_pretty(oam.avg_memory) AS avg_memory",
                        "pg_size_pretty(oam.total_memory) AS total_memory",
                        "om.max_operator_records_ratio AS max_operator_records_ratio",
                        "om.worker_records AS worker_records",
                        "oam.avg_records AS avg_records",
                        "oam.total_records AS total_records",
                    ]);

                    order_by.extend([
                        "max_operator_memory_ratio DESC",
                        "max_operator_records_ratio DESC",
                        "worker_memory DESC",
                        "worker_records DESC",
                    ]);

                    if set_worker_id {
                        order_by.push("worker_id");
                    }
                } else {
                    // no skew, so just compute totals
                    ctes.push((
                        "per_operator_memory_totals",
                        r#"
    SELECT mlm.global_id AS global_id,
           mlm.lir_id AS lir_id,
           SUM(mas.size) AS total_memory,
           SUM(mas.records) AS total_records
    FROM        mz_introspection.mz_lir_mapping mlm
     CROSS JOIN generate_series((mlm.operator_id_start) :: int8, (mlm.operator_id_end - 1) :: int8) AS valid_id
           JOIN mz_introspection.mz_arrangement_sizes_per_worker mas
             ON (mas.operator_id = valid_id)
    GROUP BY mlm.global_id, mlm.lir_id"#,
                    ));

                    ctes.push((
                        "object_totals",
                        r#"
SELECT pomt.global_id AS global_id,
       SUM(pomt.total_memory) AS total_memory,
       SUM(pomt.total_records) AS total_records
FROM per_operator_memory_totals pomt
GROUP BY pomt.global_id
"#,
                    ));

                    from.push("LEFT JOIN object_totals ot USING (global_id)");
                    columns.extend([
                        "pg_size_pretty(ot.total_memory) AS total_memory",
                        "ot.total_records AS total_records",
                    ]);
                    order_by.extend(["total_memory DESC", "total_records DESC"]);
                }
            }
            ExplainAnalyzeComputationProperty::Cpu => unimplemented!("!!! TODO mgree"),
        }
    }

    // generate SQL query text
    let ctes = if !ctes.is_empty() {
        format!(
            "WITH {}",
            separated(
                ",\n",
                ctes.iter()
                    .map(|(name, defn)| format!("{name} AS ({defn})"))
            )
        )
    } else {
        String::new()
    };
    let columns = separated(", ", columns);
    let from = separated(" ", from);
    let predicates = if !predicates.is_empty() {
        format!("WHERE {}", separated(" AND ", predicates))
    } else {
        String::new()
    };
    // add mo.name last, to break ties only
    order_by.push("mo.name DESC");
    let order_by = separated(", ", order_by);
    let query = format!(
        r#"{ctes}
SELECT {columns}
FROM {from}
{predicates}
ORDER BY {order_by}"#
    );

    if statement.as_sql {
        let rows = vec![Row::pack_slice(&[Datum::String(
            &mz_sql_pretty::pretty_str_simple(&query, 80).map_err(|e| {
                PlanError::Unstructured(format!("internal error parsing our own SQL: {e}"))
            })?,
        )])];
        let typ = SqlRelationType::new(vec![SqlScalarType::String.nullable(false)]);

        Ok(Plan::Select(SelectPlan::immediate(rows, typ)))
    } else {
        let (show_select, _resolved_ids) = ShowSelect::new_from_bare_query(scx, query)?;
        show_select.plan()
    }
}

pub fn plan_explain_timestamp(
    scx: &StatementContext,
    explain: ExplainTimestampStatement<Aug>,
) -> Result<Plan, PlanError> {
    let (format, _verbose_syntax) = match explain.format() {
        mz_sql_parser::ast::ExplainFormat::Text => (ExplainFormat::Text, false),
        mz_sql_parser::ast::ExplainFormat::VerboseText => (ExplainFormat::Text, true),
        mz_sql_parser::ast::ExplainFormat::Json => (ExplainFormat::Json, false),
        mz_sql_parser::ast::ExplainFormat::Dot => (ExplainFormat::Dot, false),
    };

    let raw_plan = {
        let query::PlannedRootQuery {
            expr: raw_plan,
            desc: _,
            finishing: _,
            scope: _,
        } = query::plan_root_query(scx, explain.select.query, QueryLifetime::OneShot)?;
        if raw_plan.contains_parameters()? {
            return Err(PlanError::ParameterNotAllowed(
                "EXPLAIN TIMESTAMP".to_string(),
            ));
        }

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
    expr.bind_parameters(scx, lifetime, params)?;

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
    let mut desc = RelationDesc::builder().with_column(
        "mz_timestamp",
        SqlScalarType::Numeric {
            max_scale: Some(NumericMaxScale::ZERO),
        }
        .nullable(false),
    );
    if progress {
        desc = desc.with_column("mz_progressed", SqlScalarType::Bool.nullable(false));
    }

    let debezium = matches!(stmt.output, SubscribeOutput::EnvelopeDebezium { .. });
    match stmt.output {
        SubscribeOutput::Diffs | SubscribeOutput::WithinTimestampOrderBy { .. } => {
            desc = desc.with_column("mz_diff", SqlScalarType::Int64.nullable(true));
            for (name, mut ty) in relation_desc.into_iter() {
                if progress {
                    ty.nullable = true;
                }
                desc = desc.with_column(name, ty);
            }
        }
        SubscribeOutput::EnvelopeUpsert { key_columns }
        | SubscribeOutput::EnvelopeDebezium { key_columns } => {
            desc = desc.with_column("mz_state", SqlScalarType::String.nullable(true));
            let key_columns = key_columns
                .into_iter()
                .map(normalize::column_name)
                .collect_vec();
            let mut before_values_desc = RelationDesc::builder();
            let mut after_values_desc = RelationDesc::builder();

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
    Ok(StatementDesc::new(Some(desc.finish())))
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
            (
                SubscribeFrom::Id(entry.global_id()),
                desc.into_owned(),
                scope,
            )
        }
        SubscribeRelation::Query(query) => {
            #[allow(deprecated)] // TODO(aalexandrov): Use HirRelationExpr in Subscribe
            let query = plan_query(scx, query, params, QueryLifetime::Subscribe)?;
            // There's no way to apply finishing operations to a `SUBSCRIBE` directly, so the
            // finishing should have already been turned into a `TopK` by
            // `plan_query` / `plan_root_query`, upon seeing the `QueryLifetime::Subscribe`.
            assert!(HirRelationExpr::is_trivial_row_set_finishing_hir(
                &query.finishing,
                query.desc.arity()
            ));
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
    let up_to = up_to
        .map(|up_to| plan_as_of_or_up_to(scx, up_to))
        .transpose()?;

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
    let (_, desc, _, _) = query::plan_copy_from(scx, table_name, columns)?;
    Ok(StatementDesc::new(Some(desc)))
}

pub fn describe_copy_item(
    scx: &StatementContext,
    object_name: <Aug as AstInfo>::ItemName,
    columns: Vec<Ident>,
) -> Result<StatementDesc, PlanError> {
    let (_, desc, _, _) = query::plan_copy_item(scx, object_name, columns)?;
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
        Some(conn_id) => CatalogItemId::from(conn_id),
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

    let to = plan_expr(ecx, &to_expr)?.type_as(ecx, &SqlScalarType::String)?;

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
    target: &CopyTarget<Aug>,
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

    let source = match target {
        CopyTarget::Stdin => CopyFromSource::Stdin,
        CopyTarget::Expr(from) => {
            scx.require_feature_flag(&ENABLE_COPY_FROM_REMOTE)?;

            // Converting the expr to an HirScalarExpr
            let mut from_expr = from.clone();
            transform_ast::transform(scx, &mut from_expr)?;
            let relation_type = RelationDesc::empty();
            let ecx = &ExprContext {
                qcx: &QueryContext::root(scx, QueryLifetime::OneShot),
                name: "COPY FROM target",
                scope: &Scope::empty(),
                relation_type: relation_type.typ(),
                allow_aggregates: false,
                allow_subqueries: false,
                allow_parameters: false,
                allow_windows: false,
            };
            let from = plan_expr(ecx, &from_expr)?.type_as(ecx, &SqlScalarType::String)?;

            match options.aws_connection {
                Some(conn_id) => {
                    let conn_id = CatalogItemId::from(conn_id);

                    // Validate the connection type is one we expect.
                    let connection = match scx.get_item(&conn_id).connection()? {
                        mz_storage_types::connections::Connection::Aws(conn) => conn,
                        _ => sql_bail!("only AWS CONNECTION is supported in COPY ... FROM"),
                    };

                    CopyFromSource::AwsS3 {
                        uri: from,
                        connection,
                        connection_id: conn_id,
                    }
                }
                None => CopyFromSource::Url(from),
            }
        }
        CopyTarget::Stdout => bail_never_supported!("COPY FROM {} not supported", target),
    };

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
        CopyFormat::Parquet => CopyFormatParams::Parquet,
    };

    let filter = match (options.files, options.pattern) {
        (Some(_), Some(_)) => bail_unsupported!("must specify one of FILES or PATTERN"),
        (Some(files), None) => Some(CopyFromFilter::Files(files)),
        (None, Some(pattern)) => Some(CopyFromFilter::Pattern(pattern)),
        (None, None) => None,
    };

    if filter.is_some() && matches!(source, CopyFromSource::Stdin) {
        bail_unsupported!("COPY FROM ... WITH (FILES ...) only supported from a URL")
    }

    let table_name_string = table_name.full_name_str();

    let (id, source_desc, columns, maybe_mfp) = query::plan_copy_from(scx, table_name, columns)?;

    let Some(mfp) = maybe_mfp else {
        sql_bail!("[internal error] COPY FROM ... expects an MFP to be produced");
    };

    Ok(Plan::CopyFrom(CopyFromPlan {
        target_id: id,
        target_name: table_name_string,
        source,
        columns,
        source_desc,
        mfp,
        params,
        filter,
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
    (MaxFileSize, ByteSize, Default(ByteSize::mb(256))),
    (Files, Vec<String>),
    (Pattern, String)
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
        (CopyDirection::From, target) => match relation {
            CopyRelation::Named { name, columns } => plan_copy_from(
                scx,
                target,
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
                        sql_bail!(
                            "specifying columns for COPY <table_name> TO commands not yet supported; use COPY (SELECT...) TO ... instead"
                        );
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
