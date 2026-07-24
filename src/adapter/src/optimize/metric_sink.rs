// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Row-wise shaping for `MetricSink` sources.
//!
//! A metric sink exports the rows of an existing collection into the in-process Prometheus metrics
//! registry. The compute-side operator (`mz_compute::sink::metric_sink`) reads a canonical row
//! shape and two planner-computed classification columns rather than parsing `metric_type` strings
//! or validating metric names on its hot path. `shape_metric_sink_source` produces that shape.
//!
//! Only the pure per-row shaping lives here. The cross-row logic (dedup, collision detection,
//! family-conflict counting) stays in the operator, because it needs the frontier-gated fold that
//! a per-row `Map` in MIR can't express.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_compute_types::plan::LirRelationExpr;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, MetricSinkConnection};
use mz_expr::func::variadic::Coalesce;
use mz_expr::{MirRelationExpr, MirScalarExpr, func};
use mz_repr::explain::trace_plan;
use mz_repr::{
    ColumnName, Datum, GlobalId, RelationDesc, ReprRelationType, ReprScalarType, Row, SqlScalarType,
};
use mz_sql::names::QualifiedItemName;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_transform::TransformCtx;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{SharedTypecheckingContext, empty_typechecking_context};
use timely::progress::Antichain;

use crate::optimize::dataflows::{
    ComputeInstanceSnapshot, DataflowBuilder, ExprPrep, ExprPrepMaintained,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizerCatalog, OptimizerConfig,
    OptimizerError, optimize_mir_local,
};

/// Matches Prometheus's metric name grammar: `[a-zA-Z_:][a-zA-Z0-9_:]*`.
///
/// Expressed in MIR (see `shape_metric_sink_source`) rather than parsed from a `&str` on the
/// operator's hot path.
const METRIC_NAME_PATTERN: &str = "^[a-zA-Z_:][a-zA-Z0-9_:]*$";

/// Optimizer for `CREATE METRIC SINK`.
///
/// Like `CREATE INDEX`, the pipeline starts directly from the `GlobalId` of the collection to
/// export rather than lowering a new relational expression from HIR. Unlike a materialized view
/// sink, there is no persist shard, so there is no storage-metadata stage.
pub struct Optimizer {
    /// A representation typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: SharedTypecheckingContext,
    /// A snapshot of the catalog state.
    catalog: Arc<dyn OptimizerCatalog>,
    /// A snapshot of the cluster that will run the dataflow.
    compute_instance: ComputeInstanceSnapshot,
    /// A transient GlobalId for the shaped view built over the sink's source relation (see
    /// `shape_metric_sink_source`).
    view_id: GlobalId,
    /// A durable GlobalId to be used with the exported metric sink.
    sink_id: GlobalId,
    /// Optimizer config.
    config: OptimizerConfig,
    /// Optimizer metrics.
    metrics: OptimizerMetrics,
    /// The time spent performing optimization so far.
    duration: Duration,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<dyn OptimizerCatalog>,
        compute_instance: ComputeInstanceSnapshot,
        view_id: GlobalId,
        sink_id: GlobalId,
        config: OptimizerConfig,
        metrics: OptimizerMetrics,
    ) -> Self {
        Self {
            typecheck_ctx: empty_typechecking_context(),
            catalog,
            compute_instance,
            view_id,
            sink_id,
            config,
            metrics,
            duration: Default::default(),
        }
    }
}

/// A wrapper of metric sink parts needed to start the optimization process.
pub struct MetricSink {
    name: QualifiedItemName,
    from: GlobalId,
}

impl MetricSink {
    /// Construct a new [`MetricSink`]. Arguments are recorded as-is.
    pub fn new(name: QualifiedItemName, from: GlobalId) -> Self {
        Self { name, from }
    }
}

/// The (sealed intermediate) result after embedding a [`MetricSink`] into a
/// [`MirDataflowDescription`], inlining referenced views, and jointly optimizing the `MIR` plans.
#[derive(Clone, Debug)]
pub struct GlobalMirPlan {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalMirPlan {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Clone, Debug)]
pub struct GlobalLirPlan {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalLirPlan {
    pub fn df_desc(&self) -> &LirDataflowDescription {
        &self.df_desc
    }
}

impl Optimize<MetricSink> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, metric_sink: MetricSink) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let from_entry = self.catalog.get_entry(&metric_sink.from);
        let full_name = self
            .catalog
            .resolve_full_name(&metric_sink.name, from_entry.conn_id());
        let from_desc = from_entry
            .relation_desc()
            .expect("can only create a metric sink on items with a valid description")
            .into_owned();

        let mut df_builder = {
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(&*self.catalog, compute).with_config(&self.config)
        };
        let mut df_desc = MirDataflowDescription::new(full_name.to_string());
        let mut df_meta = DataflowMetainfo::default();

        df_builder.import_into_dataflow(&metric_sink.from, &mut df_desc, &self.config.features)?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        // Push the pure row-wise shaping (coalesce identity elements, classify the metric kind,
        // validate the metric name) into MIR, so the operator only does the cross-row logic
        // (dedup/collision/family-conflict) that needs the fold. See `shape_metric_sink_source`.
        let (shaped_expr, shaped_desc) = shape_metric_sink_source(metric_sink.from, &from_desc);
        let mut local_ctx = TransformCtx::local(
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
            Some(&mut self.metrics),
            Some(self.view_id),
        );
        let shaped_expr = optimize_mir_local(shaped_expr, &mut local_ctx)?;

        df_builder.import_view_into_dataflow(
            &self.view_id,
            &shaped_expr,
            &mut df_desc,
            &self.config.features,
        )?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        let sink_description = ComputeSinkDesc {
            from: self.view_id,
            from_desc: shaped_desc,
            connection: ComputeSinkConnection::MetricSink(MetricSinkConnection {}),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: Vec::new(),
            refresh_schedule: None,
        };
        df_desc.export_sink(self.sink_id, sink_description);

        // Prepare expressions in the assembled dataflow.
        let style = ExprPrepMaintained;
        df_desc.visit_children(
            |r| style.prep_relation_expr(r),
            |s| style.prep_scalar_expr(s),
        )?;

        // Construct TransformCtx for global optimization.
        let mut transform_ctx = TransformCtx::global(
            &df_builder,
            &mz_transform::EmptyStatisticsOracle,
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
            Some(&mut self.metrics),
        );
        // Run global optimization.
        mz_transform::optimize_dataflow(&mut df_desc, &mut transform_ctx, false)?;

        self.duration += time.elapsed();

        Ok(GlobalMirPlan { df_desc, df_meta })
    }
}

impl Optimize<GlobalMirPlan> for Optimizer {
    type To = GlobalLirPlan;

    fn optimize(&mut self, plan: GlobalMirPlan) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let GlobalMirPlan {
            mut df_desc,
            df_meta,
        } = plan;

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0, &self.config.features)?
        }

        // Finalize the dataflow: MIR ⇒ LIR lowering and LIR ⇒ LIR transforms.
        let df_desc = LirRelationExpr::finalize_dataflow(
            df_desc,
            &self.config.features,
            Some(self.metrics.lowering()),
        )?;

        // Trace the pipeline output under `optimize`.
        trace_plan(&df_desc);

        self.duration += time.elapsed();
        self.metrics
            .observe_e2e_optimization_time("metric_sink", self.duration);

        Ok(GlobalLirPlan { df_desc, df_meta })
    }
}

impl GlobalLirPlan {
    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }
}

/// Extends the metric sink's imported relation with the row-wise shaping the operator otherwise
/// has to do in Rust: coalesces `labels`/`help` to their identity element, and adds two columns
/// the operator reads instead of parsing strings on its hot path:
///
/// * `metric_kind` (`Int32`, nullable): `0` for `gauge`, `1` for `counter`, `NULL` for any other
///   `metric_type`.
/// * `name_valid` (`Bool`, nullable): whether `metric_name` matches the Prometheus metric-name
///   grammar (see `METRIC_NAME_PATTERN`). The operator treats a `NULL` the same as `false`.
///
/// No row is dropped or filtered here: the operator still needs every row, including the ones
/// this marks invalid, to count `skipped`/`null_values`. Only the pure per-row shaping moves to
/// MIR. Dedup, collision detection, and family-conflict counting stay in the operator, because
/// they need cross-row state (the frontier-gated fold) that a `Map` can't express.
///
/// TODO: A full move would also express the dedup/collision/family-conflict logic in MIR (e.g.
/// via `Reduce` + `FirstValue`), collapsing the operator to a plain fold over the live set. That
/// full move is deferred: the tiebreak fidelity that logic needs is easier to keep correct
/// hand-written and unit-tested for now.
fn shape_metric_sink_source(
    from_id: GlobalId,
    from_desc: &RelationDesc,
) -> (MirRelationExpr, RelationDesc) {
    // Precondition: the source relation exposes the canonical metric-sink columns (`metric_name`,
    // `metric_type`, `labels`, `value`, `help`). No in-tree caller enforces this yet (see the NOTE
    // on `METRIC_NAME_PATTERN`); the SQL planner will, once the CREATE METRIC SINK planning path
    // lands.
    let get_idx = |name: &str| {
        from_desc
            .get_by_name(&ColumnName::from(name))
            .expect("metric-sink source relation must expose the canonical columns")
    };
    let (metric_name_idx, metric_name_ct) = get_idx("metric_name");
    let (metric_type_idx, metric_type_ct) = get_idx("metric_type");
    let (labels_idx, labels_ct) = get_idx("labels");
    let (value_idx, value_ct) = get_idx("value");
    let (help_idx, help_ct) = get_idx("help");

    let repr_typ = ReprRelationType::from(from_desc.typ());
    let arity = repr_typ.column_types.len();
    let labels_repr_type = ReprScalarType::from(&labels_ct.scalar_type);

    let empty_map_row = {
        let mut row = Row::default();
        row.packer().push_dict_with(|_| {});
        row
    };
    let labels_coalesced = MirScalarExpr::call_variadic(
        Coalesce,
        vec![
            MirScalarExpr::column(labels_idx),
            MirScalarExpr::literal_from_single_element_row(empty_map_row, labels_repr_type),
        ],
    );
    let help_coalesced = MirScalarExpr::call_variadic(
        Coalesce,
        vec![
            MirScalarExpr::column(help_idx),
            MirScalarExpr::literal_ok(Datum::String(""), ReprScalarType::String),
        ],
    );

    let metric_type_literal = |s: &'static str| {
        MirScalarExpr::column(metric_type_idx).call_binary(
            MirScalarExpr::literal_ok(Datum::String(s), ReprScalarType::String),
            func::Eq,
        )
    };
    let metric_kind = metric_type_literal("gauge").if_then_else(
        MirScalarExpr::literal_ok(Datum::Int32(0), ReprScalarType::Int32),
        metric_type_literal("counter").if_then_else(
            MirScalarExpr::literal_ok(Datum::Int32(1), ReprScalarType::Int32),
            MirScalarExpr::literal_null(ReprScalarType::Int32),
        ),
    );

    // The regexp requires at least one leading character, so an empty name fails it without a
    // separate `!= ""` check. `is_null().not()` keeps a null name concretely `false`, not `NULL`.
    let name_valid = MirScalarExpr::column(metric_name_idx)
        .call_is_null()
        .not()
        .and(MirScalarExpr::column(metric_name_idx).call_binary(
            MirScalarExpr::literal_ok(Datum::String(METRIC_NAME_PATTERN), ReprScalarType::String),
            func::IsRegexpMatchCaseSensitive,
        ));

    let shaped_expr = MirRelationExpr::global_get(from_id, repr_typ)
        .map(vec![
            labels_coalesced,
            help_coalesced,
            metric_kind,
            name_valid,
        ])
        .project(vec![
            metric_name_idx,
            metric_type_idx,
            arity, // coalesced labels
            value_idx,
            arity + 1, // coalesced help
            arity + 2, // metric_kind
            arity + 3, // name_valid
        ]);

    let mut labels_shaped_ct = labels_ct.clone();
    labels_shaped_ct.nullable = false;
    let mut help_shaped_ct = help_ct.clone();
    help_shaped_ct.nullable = false;
    let shaped_desc = RelationDesc::from_names_and_types([
        ("metric_name", metric_name_ct.clone()),
        ("metric_type", metric_type_ct.clone()),
        ("labels", labels_shaped_ct),
        ("value", value_ct.clone()),
        ("help", help_shaped_ct),
        ("metric_kind", SqlScalarType::Int32.nullable(true)),
        ("name_valid", SqlScalarType::Bool.nullable(true)),
    ]);

    (shaped_expr, shaped_desc)
}

#[cfg(test)]
mod tests {
    use mz_expr::Eval;
    use mz_repr::{RowArena, SqlColumnType};

    use super::*;

    /// The canonical metric-sink source shape, with `labels`/`help` nullable so the shaping's
    /// coalesce is observable and an extra trailing column so column resolution is exercised by
    /// name, not position.
    fn source_desc() -> RelationDesc {
        RelationDesc::builder()
            .with_column("metric_name", SqlScalarType::String.nullable(true))
            .with_column("metric_type", SqlScalarType::String.nullable(false))
            .with_column(
                "labels",
                SqlScalarType::Map {
                    value_type: Box::new(SqlScalarType::String),
                    custom_id: None,
                }
                .nullable(true),
            )
            .with_column("value", SqlScalarType::Float64.nullable(true))
            .with_column("help", SqlScalarType::String.nullable(true))
            .with_column("extra", SqlScalarType::String.nullable(true))
            .finish()
    }

    #[mz_ore::test]
    fn shaped_desc_column_contract() {
        let (_expr, desc) = shape_metric_sink_source(GlobalId::Transient(0), &source_desc());

        let cols: Vec<(String, SqlColumnType)> = desc
            .iter()
            .map(|(name, ty)| (name.as_str().to_string(), ty.clone()))
            .collect();

        // Exactly the seven canonical columns, in order. The trailing `extra` source column is
        // projected away.
        let names: Vec<&str> = cols.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "metric_name",
                "metric_type",
                "labels",
                "value",
                "help",
                "metric_kind",
                "name_valid",
            ]
        );

        let by_name = |name: &str| {
            cols.iter()
                .find(|(n, _)| n == name)
                .expect("column present in shaped desc")
                .1
                .clone()
        };

        // `labels`/`help` are coalesced to their identity element, so they are non-null.
        assert!(!by_name("labels").nullable);
        assert!(!by_name("help").nullable);

        // `metric_name`/`value` stay nullable (no identity element).
        assert!(by_name("metric_name").nullable);
        assert!(by_name("value").nullable);

        // The two classification columns the operator reads.
        assert_eq!(by_name("metric_kind"), SqlScalarType::Int32.nullable(true));
        assert_eq!(by_name("name_valid"), SqlScalarType::Bool.nullable(true));
    }

    #[mz_ore::test]
    fn shaped_expr_projects_seven_columns() {
        let (expr, _desc) = shape_metric_sink_source(GlobalId::Transient(0), &source_desc());

        // The shaping is a `Map` of four new columns followed by a `Project` down to the seven
        // canonical columns.
        match &expr {
            MirRelationExpr::Project { outputs, .. } => {
                assert_eq!(outputs.len(), 7);
            }
            other => panic!("expected a Project at the root of the shaped expr, got {other:?}"),
        }
    }

    /// The four scalars the shaping `Map` appends, in order:
    /// `[labels_coalesced, help_coalesced, metric_kind, name_valid]`. Lets the two classification
    /// scalars be evaluated directly against an input row.
    fn shaped_map_scalars(desc: &RelationDesc) -> Vec<MirScalarExpr> {
        let (expr, _desc) = shape_metric_sink_source(GlobalId::Transient(0), desc);
        match expr {
            MirRelationExpr::Project { input, .. } => match *input {
                MirRelationExpr::Map { scalars, .. } => scalars,
                other => panic!("expected a Map under the Project, got {other:?}"),
            },
            other => panic!("expected a Project at the root, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn metric_kind_classifies_type() {
        let scalars = shaped_map_scalars(&source_desc());
        let metric_kind = &scalars[2];
        let arena = RowArena::new();
        // Row layout matches `source_desc`: [metric_name, metric_type, labels, value, help, extra].
        for (metric_type, expected) in [
            ("gauge", Datum::Int32(0)),
            ("counter", Datum::Int32(1)),
            ("histogram", Datum::Null),
            ("summary", Datum::Null),
        ] {
            let row = [
                Datum::Null,
                Datum::String(metric_type),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ];
            assert_eq!(
                metric_kind
                    .eval(&row, &arena)
                    .expect("metric_kind eval succeeds"),
                expected,
                "metric_type = {metric_type}",
            );
        }
    }

    #[mz_ore::test]
    fn name_valid_matches_prometheus_grammar() {
        let scalars = shaped_map_scalars(&source_desc());
        let name_valid = &scalars[3];
        let arena = RowArena::new();
        for (metric_name, expected) in [
            (Datum::String("http_requests_total"), Datum::True),
            (Datum::String("with:colons_and_1_digit"), Datum::True),
            (Datum::String("1_leading_digit"), Datum::False),
            (Datum::String("has-a-dash"), Datum::False),
            (Datum::String(""), Datum::False),
            (Datum::Null, Datum::False),
        ] {
            let row = [
                metric_name,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ];
            assert_eq!(
                name_valid
                    .eval(&row, &arena)
                    .expect("name_valid eval succeeds"),
                expected,
                "metric_name = {metric_name:?}",
            );
        }
    }
}
