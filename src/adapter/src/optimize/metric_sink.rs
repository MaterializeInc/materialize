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
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::{HirRelationExpr, HirToMirConfig};
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

/// Optimizer for metric sinks, both `CREATE METRIC SINK` and the coordinator-installed curated
/// sinks.
///
/// The source is either an existing collection (like `CREATE INDEX`, no HIR to lower) or a planned
/// query (like a materialized view), see [`MetricSinkFrom`]. Either way the row-wise shaping is
/// appended in MIR and the dataflow exports a single `MetricSink`. Unlike a materialized view sink
/// there is no persist shard, so there is no storage-metadata stage.
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
    /// Names the assembled dataflow, for debugging.
    debug_name: String,
    /// The collection whose rows the sink exports.
    from: MetricSinkFrom,
    /// Prepended to every row's `metric_name` to form the published name. Validated as a valid start
    /// of a Prometheus metric name before it reaches here (plan time for a user sink, install time
    /// for a curated one).
    prefix: String,
    /// Value for the `sink` label on the sink's health gauges. `None` defaults to the sink's
    /// `GlobalId`, which is what a user sink wants. A curated sink passes its stable name.
    label: Option<String>,
}

impl MetricSink {
    /// Construct a new [`MetricSink`]. Arguments are recorded as-is.
    pub fn new(
        debug_name: String,
        from: MetricSinkFrom,
        prefix: String,
        label: Option<String>,
    ) -> Self {
        Self {
            debug_name,
            from,
            prefix,
            label,
        }
    }
}

/// Where a metric sink's rows come from.
///
/// Either way the source must expose the canonical metric-sink columns (see
/// [`shape_metric_sink_source`]).
pub enum MetricSinkFrom {
    /// An existing catalog collection, as `CREATE METRIC SINK ... FROM <relation>` resolves to.
    Id(GlobalId),
    /// A planned query, as a coordinator-installed sink built from curated SQL uses. The query is
    /// not a catalog item, so it is lowered and locally optimized here rather than imported.
    Query {
        expr: HirRelationExpr,
        desc: RelationDesc,
    },
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

        let mut df_builder = {
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(&*self.catalog, compute).with_config(&self.config)
        };
        let mut df_desc = MirDataflowDescription::new(metric_sink.debug_name);
        let mut df_meta = DataflowMetainfo::default();

        let (source_expr, source_desc) = match metric_sink.from {
            MetricSinkFrom::Id(from) => {
                let from_desc = self
                    .catalog
                    .get_entry(&from)
                    .relation_desc()
                    .expect("can only create a metric sink on items with a valid description")
                    .into_owned();
                let repr_typ = ReprRelationType::from(from_desc.typ());
                (MirRelationExpr::global_get(from, repr_typ), from_desc)
            }
            MetricSinkFrom::Query { expr, desc } => {
                // HIR ⇒ MIR lowering and decorrelation. The result is inlined under the shaping
                // below rather than becoming its own build, so the whole source is one view.
                let expr = expr.lower(HirToMirConfig::from(&self.config), Some(&self.metrics))?;
                (expr, desc)
            }
        };

        // Push the pure row-wise shaping (coalesce identity elements, classify the metric kind,
        // validate the metric name) into MIR, so the operator only does the cross-row logic
        // (dedup/collision/family-conflict) that needs the fold. See `shape_metric_sink_source`.
        let (shaped_expr, shaped_desc) =
            shape_metric_sink_source(source_expr, &source_desc, &metric_sink.prefix);
        let mut local_ctx = TransformCtx::local(
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
            Some(&mut self.metrics),
            Some(self.view_id),
        );
        let shaped_expr = optimize_mir_local(shaped_expr, &mut local_ctx)?;

        // Imports the source's dependencies (the `Id` variant's collection, or the query's leaf
        // collections) before inserting the shaped view that reads them.
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
            connection: ComputeSinkConnection::MetricSink(MetricSinkConnection {
                label: metric_sink
                    .label
                    .unwrap_or_else(|| self.sink_id.to_string()),
            }),
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

/// Extends the metric sink's source expression with the row-wise shaping the operator otherwise
/// has to do in Rust: prepends the configured `prefix` to `metric_name` to form the published name,
/// coalesces `labels`/`help` to their identity element, and adds two columns the operator reads
/// instead of parsing strings on its hot path:
///
/// * `metric_kind` (`Int32`, nullable): `0` for `gauge`, `1` for `counter`, `NULL` for any other
///   `metric_type`.
/// * `name_valid` (`Bool`, nullable): whether the published name (`prefix + metric_name`) matches
///   the Prometheus metric-name grammar (see `METRIC_NAME_PATTERN`). The operator treats a `NULL`
///   the same as `false`. Validating the published name, not the bare `metric_name`, is what lets a
///   row name start with a digit: the prefix supplies the valid leading character.
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
    source: MirRelationExpr,
    source_desc: &RelationDesc,
    prefix: &str,
) -> (MirRelationExpr, RelationDesc) {
    // Precondition: `source_desc` describes `source` and exposes the canonical metric-sink columns
    // (`metric_name`, `metric_type`, `labels`, `value`, `help`).
    // `mz_sql::plan::validate_metric_sink_desc` enforces this for both `CREATE METRIC SINK` and
    // the coordinator-installed curated sinks, so a missing column here is a caller bug.
    let get_idx = |name: &str| {
        source_desc
            .get_by_name(&ColumnName::from(name))
            .expect("metric-sink source relation must expose the canonical columns")
    };
    let (metric_name_idx, metric_name_ct) = get_idx("metric_name");
    let (metric_type_idx, metric_type_ct) = get_idx("metric_type");
    let (labels_idx, labels_ct) = get_idx("labels");
    let (value_idx, value_ct) = get_idx("value");
    let (help_idx, help_ct) = get_idx("help");

    let arity = source_desc.typ().columns().len();
    // The mapped columns are appended at `arity + N` and the `Project` indexes into `source` by
    // position, so `source` must have exactly the arity `source_desc` describes. Guaranteed by the
    // callers (a trivial finishing over the planned query, or a direct `Get` of the source), but a
    // mismatch would silently read the wrong columns, so assert it here.
    mz_ore::soft_assert_eq_or_log!(source.arity(), arity);
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

    // The published name is `prefix + metric_name`. The prefix is validated to start with the
    // reserved marker (see `validate_metric_sink_prefix`, run at plan time for a user sink and at
    // install time for a curated one), so every published family lands in the
    // `mz_metric_sink_` lane nothing else in the process registry writes. `TextConcat` (the `||`
    // operator) propagates nulls, so a null `metric_name` stays null and is skipped, never
    // published as the bare prefix.
    let prefixed_name = MirScalarExpr::literal_ok(Datum::String(prefix), ReprScalarType::String)
        .call_binary(
            MirScalarExpr::column(metric_name_idx),
            func::TextConcatBinary,
        );

    // The regexp requires at least one leading character, so an empty published name fails it
    // without a separate `!= ""` check. `is_null().not()` keeps a null name concretely `false`,
    // not `NULL`. The shaping map appends `prefixed_name` at `arity + 2`, and a map scalar may
    // reference earlier-appended columns, so this reads that column instead of rebuilding the
    // concat.
    let published_name = MirScalarExpr::column(arity + 2);
    let name_valid = published_name
        .clone()
        .call_is_null()
        .not()
        .and(published_name.call_binary(
            MirScalarExpr::literal_ok(Datum::String(METRIC_NAME_PATTERN), ReprScalarType::String),
            func::IsRegexpMatchCaseSensitive,
        ));

    let shaped_expr = source
        .map(vec![
            labels_coalesced,
            help_coalesced,
            prefixed_name,
            metric_kind,
            name_valid,
        ])
        .project(vec![
            arity + 2, // prefixed metric_name
            metric_type_idx,
            arity, // coalesced labels
            value_idx,
            arity + 1, // coalesced help
            arity + 3, // metric_kind
            arity + 4, // name_valid
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
    use std::collections::BTreeMap;

    use mz_catalog::memory::objects::{CatalogEntry, CatalogItem, Table, TableDataSource};
    use mz_controller_types::ClusterId;
    use mz_expr::Eval;
    use mz_ore::metrics::MetricsRegistry;
    use mz_repr::adt::mz_acl_item::PrivilegeMap;
    use mz_repr::role_id::RoleId;
    use mz_repr::{
        CatalogItemId, RelationVersion, RelationVersionSelector, RowArena, SqlColumnType,
        VersionedRelationDesc,
    };
    use mz_sql::names::{
        FullItemName, ItemQualifiers, QualifiedItemName, RawDatabaseSpecifier,
        ResolvedDatabaseSpecifier, ResolvedIds, SchemaId, SchemaSpecifier,
    };
    use mz_sql::session::vars::SystemVars;

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

    /// A bare `Get` of `TABLE_GID`, the source expression the `MetricSinkFrom::Id` path shapes.
    fn source_get(desc: &RelationDesc) -> MirRelationExpr {
        MirRelationExpr::global_get(TABLE_GID, ReprRelationType::from(desc.typ()))
    }

    #[mz_ore::test]
    fn shaped_desc_column_contract() {
        let (_expr, desc) =
            shape_metric_sink_source(source_get(&source_desc()), &source_desc(), "app_");

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
        let (expr, _desc) =
            shape_metric_sink_source(source_get(&source_desc()), &source_desc(), "app_");

        // The shaping is a `Map` of five new columns followed by a `Project` down to the seven
        // canonical columns.
        match &expr {
            MirRelationExpr::Project { outputs, .. } => {
                assert_eq!(outputs.len(), 7);
            }
            other => panic!("expected a Project at the root of the shaped expr, got {other:?}"),
        }
    }

    /// The five scalars the shaping `Map` appends, in order:
    /// `[labels_coalesced, help_coalesced, prefixed_name, metric_kind, name_valid]`.
    fn shaped_map_scalars(desc: &RelationDesc, prefix: &str) -> Vec<MirScalarExpr> {
        let (expr, _desc) = shape_metric_sink_source(source_get(desc), desc, prefix);
        match expr {
            MirRelationExpr::Project { input, .. } => match *input {
                MirRelationExpr::Map { scalars, .. } => scalars,
                other => panic!("expected a Map under the Project, got {other:?}"),
            },
            other => panic!("expected a Project at the root, got {other:?}"),
        }
    }

    /// Evaluate the shaping `Map`'s appended scalars in order, extending the row with each result.
    /// A later scalar may reference an earlier appended column (`name_valid` reads the published-name
    /// column), so they must be evaluated cumulatively rather than in isolation.
    fn eval_shaped_row<'a>(
        scalars: &'a [MirScalarExpr],
        input: &[Datum<'a>],
        arena: &'a RowArena,
    ) -> Vec<Datum<'a>> {
        let mut row = input.to_vec();
        for scalar in scalars {
            let datum = scalar.eval(&row, arena).expect("scalar eval succeeds");
            row.push(datum);
        }
        row
    }

    #[mz_ore::test]
    fn metric_kind_classifies_type() {
        let scalars = shaped_map_scalars(&source_desc(), "app_");
        let metric_kind = &scalars[3];
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
        // Validation is on the published name `prefix + metric_name`, not the bare `metric_name`.
        // The plan-time-validated prefix supplies the valid leading character, so a row name may
        // start with a digit (or be empty, publishing the bare prefix). A dash is invalid anywhere,
        // and a null name stays null.
        let scalars = shaped_map_scalars(&source_desc(), "app_");
        let arena = RowArena::new();
        for (metric_name, expected) in [
            (Datum::String("http_requests_total"), Datum::True),
            (Datum::String("with:colons_and_1_digit"), Datum::True),
            (Datum::String("1_leading_digit"), Datum::True),
            (Datum::String("has-a-dash"), Datum::False),
            (Datum::String(""), Datum::True),
            (Datum::Null, Datum::False),
        ] {
            let input = [
                metric_name,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ];
            // `name_valid` is the last appended scalar and reads the published-name column, so
            // evaluate the whole appended row and read its final column.
            let row = eval_shaped_row(&scalars, &input, &arena);
            assert_eq!(
                *row.last().expect("row has appended columns"),
                expected,
                "metric_name = {metric_name:?}",
            );
        }
    }

    /// The smallest catalog the optimizer needs: one table, at `TABLE_GID`, exposing the canonical
    /// metric-sink columns.
    #[derive(Debug)]
    struct SingleTableCatalog {
        entry: CatalogEntry,
    }

    const TABLE_ITEM_ID: CatalogItemId = CatalogItemId::User(1);
    const TABLE_GID: GlobalId = GlobalId::User(1);
    const SINK_GID: GlobalId = GlobalId::User(2);

    impl SingleTableCatalog {
        fn new() -> Self {
            let table = Table {
                create_sql: None,
                desc: VersionedRelationDesc::new(source_desc()),
                collections: BTreeMap::from([(RelationVersion::root(), TABLE_GID)]),
                conn_id: None,
                resolved_ids: ResolvedIds::empty(),
                custom_logical_compaction_window: None,
                is_retained_metrics_object: false,
                data_source: TableDataSource::TableWrites {
                    defaults: Vec::new(),
                },
            };
            let entry = CatalogEntry {
                item: CatalogItem::Table(table),
                referenced_by: Vec::new(),
                used_by: Vec::new(),
                id: TABLE_ITEM_ID,
                oid: 20_000,
                name: QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(SchemaId::User(1)),
                    },
                    item: "t".to_string(),
                },
                owner_id: RoleId::User(1),
                privileges: PrivilegeMap::default(),
            };
            Self { entry }
        }
    }

    impl OptimizerCatalog for SingleTableCatalog {
        fn get_entry(&self, _id: &GlobalId) -> mz_catalog::memory::objects::CatalogCollectionEntry {
            mz_catalog::memory::objects::CatalogCollectionEntry {
                entry: self.entry.clone(),
                version: RelationVersionSelector::Latest,
            }
        }

        fn get_entry_by_item_id(&self, _id: &CatalogItemId) -> &CatalogEntry {
            &self.entry
        }

        fn resolve_full_name(
            &self,
            name: &QualifiedItemName,
            _conn_id: Option<&mz_adapter_types::connection::ConnectionId>,
        ) -> FullItemName {
            FullItemName {
                database: RawDatabaseSpecifier::Ambient,
                schema: "public".to_string(),
                item: name.item.clone(),
            }
        }

        fn get_indexes_on(
            &self,
            _id: GlobalId,
            _cluster: ClusterId,
        ) -> Box<dyn Iterator<Item = (GlobalId, &mz_catalog::memory::objects::Index)> + '_>
        {
            Box::new(std::iter::empty())
        }
    }

    const VIEW_GID: GlobalId = GlobalId::Transient(1);

    /// Runs the whole pipeline over `from` and returns the assembled dataflow.
    fn optimize_from(from: MetricSinkFrom, metric_label: Option<String>) -> LirDataflowDescription {
        let catalog = Arc::new(SingleTableCatalog::new());
        let cluster_id = ClusterId::user(1).expect("valid cluster id");
        let compute_instance = ComputeInstanceSnapshot::new_without_collections(cluster_id);
        let config = OptimizerConfig::from(&SystemVars::default());
        let metrics = OptimizerMetrics::register_into(&MetricsRegistry::new(), Duration::MAX);

        let mut optimizer = Optimizer::new(
            catalog,
            compute_instance,
            VIEW_GID,
            SINK_GID,
            config,
            metrics,
        );

        let global_mir_plan = optimizer
            .optimize(MetricSink::new(
                "metric-sink-test".to_string(),
                from,
                "app_".to_string(),
                metric_label,
            ))
            .expect("MIR optimization succeeds");
        let global_lir_plan = optimizer
            .optimize(global_mir_plan)
            .expect("LIR optimization succeeds");
        let (df_desc, _df_meta) = global_lir_plan.unapply();
        df_desc
    }

    /// Asserts the dataflow exports exactly one `MetricSink` over the shaped view, whose desc
    /// carries the operator's column contract.
    fn assert_one_shaped_metric_sink_export(df_desc: &LirDataflowDescription) {
        assert!(df_desc.index_exports.is_empty());
        let sink_exports: Vec<_> = df_desc.sink_exports.iter().collect();
        assert_eq!(sink_exports.len(), 1);
        let (sink_id, sink_desc) = sink_exports[0];
        assert_eq!(*sink_id, SINK_GID);
        assert!(matches!(
            sink_desc.connection,
            ComputeSinkConnection::MetricSink(_)
        ));
        assert_eq!(sink_desc.from, VIEW_GID);
        let shaped_names: Vec<&str> = sink_desc
            .from_desc
            .iter_names()
            .map(|n| n.as_str())
            .collect();
        assert_eq!(
            shaped_names,
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
    }

    /// The `sink` label carried by the export's connection.
    fn sink_label(df_desc: &LirDataflowDescription) -> &str {
        match &df_desc
            .sink_exports
            .values()
            .next()
            .expect("one export")
            .connection
        {
            ComputeSinkConnection::MetricSink(conn) => &conn.label,
            other => panic!("expected a metric sink connection, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn optimizer_exports_one_metric_sink() {
        let df_desc = optimize_from(MetricSinkFrom::Id(TABLE_GID), None);
        assert_one_shaped_metric_sink_export(&df_desc);
        // The source collection is imported, not rebuilt: the only build is the shaped view.
        assert!(df_desc.source_imports.contains_key(&TABLE_GID));
        let build_ids: Vec<_> = df_desc.objects_to_build.iter().map(|b| b.id).collect();
        assert_eq!(build_ids, vec![VIEW_GID]);
    }

    /// The `Query` source path (what a coordinator-installed curated sink takes) assembles the
    /// same shape, with the query lowered under the shaping instead of a `Get` of a catalog item.
    #[mz_ore::test]
    fn optimizer_shapes_a_query_source() {
        let desc = source_desc();
        // The simplest query over the canonical columns. Building richer HIR by hand buys nothing:
        // what is under test is that a query source is lowered and shaped, not the lowering itself.
        let expr = HirRelationExpr::Get {
            id: mz_expr::Id::Global(TABLE_GID),
            typ: desc.typ().clone(),
        };

        let df_desc = optimize_from(
            MetricSinkFrom::Query {
                expr,
                desc: desc.clone(),
            },
            None,
        );
        assert_one_shaped_metric_sink_export(&df_desc);
        // The query's leaf collection is imported by the shaped view's dependency walk.
        assert!(df_desc.source_imports.contains_key(&TABLE_GID));
        let build_ids: Vec<_> = df_desc.objects_to_build.iter().map(|b| b.id).collect();
        assert_eq!(build_ids, vec![VIEW_GID]);
    }

    /// With no explicit label a sink is tagged by its `GlobalId`, what a user's `CREATE METRIC
    /// SINK` relies on. An explicit label (a curated sink's stable name) is used verbatim.
    #[mz_ore::test]
    fn metric_sink_label_defaults_to_sink_id_else_override() {
        let df_desc = optimize_from(MetricSinkFrom::Id(TABLE_GID), None);
        assert_eq!(sink_label(&df_desc), SINK_GID.to_string());

        let df_desc = optimize_from(
            MetricSinkFrom::Id(TABLE_GID),
            Some("mz_curated".to_string()),
        );
        assert_eq!(sink_label(&df_desc), "mz_curated");
    }
}
