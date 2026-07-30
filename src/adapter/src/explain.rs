// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for various intermediate representations.
//!
//! Ideally, the `EXPLAIN` support for each IR should be in the crate where this
//! IR is defined. However, we need to resort to an [`Explainable`] newtype
//! struct in order to provide alternate [`mz_repr::explain::Explain`]
//! implementations for some structs (see the [`mir`]) module for details.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::IndexKeyBound;
use mz_expr::explain::ExplainContext;
use mz_repr::GlobalId;
use mz_repr::explain::{Explain, ExplainConfig, ExplainError, ExplainFormat, ExprHumanizer};
use mz_repr::optimize::OptimizerFeatures;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;

use crate::AdapterError;
use crate::index_arrangement_stats::ArrangementStats;

pub(crate) mod fast_path;
pub(crate) mod hir;
pub(crate) mod insights;
pub(crate) mod lir;
pub(crate) mod mir;
pub(crate) mod optimizer_trace;

/// Newtype struct for wrapping types that should
/// implement the [`mz_repr::explain::Explain`] trait.
pub(crate) struct Explainable<'a, T>(&'a mut T);

impl<'a, T> Explainable<'a, T> {
    pub(crate) fn new(t: &'a mut T) -> Explainable<'a, T> {
        Explainable(t)
    }
}

/// Convenience method to derive an `ExplainContext` from the `index_imports` in
/// the given `plan` and all other input parameters, wrap the `plan` in an
/// `Explainable`, and finally compute and return the `explain(...)` result.
pub(crate) fn explain_dataflow<T>(
    mut plan: DataflowDescription<T>,
    format: ExplainFormat,
    config: &ExplainConfig,
    features: &OptimizerFeatures,
    humanizer: &dyn ExprHumanizer,
    cardinality_stats: BTreeMap<GlobalId, usize>,
    target_cluster: Option<&str>,
    dataflow_metainfo: &DataflowMetainfo<Arc<OptimizerNotice>>,
) -> Result<String, AdapterError>
where
    for<'a> Explainable<'a, DataflowDescription<T>>: Explain<'a, Context = ExplainContext<'a>>,
{
    // Collect the list of indexes used by the dataflow at this point.
    let used_indexes = dataflow_metainfo.used_indexes(&plan);

    let optimizer_notices = OptimizerNotice::explain(
        &dataflow_metainfo.optimizer_notices,
        humanizer,
        config.redacted,
    )
    .map_err(ExplainError::FormatError)?;

    let context = ExplainContext {
        config,
        features,
        humanizer,
        cardinality_stats,
        used_indexes,
        finishing: Default::default(),
        duration: Default::default(),
        target_cluster,
        optimizer_notices,
    };

    Ok(Explainable::new(&mut plan).explain(&format, &context)?)
}

/// Convenience method to explain a single plan.
///
/// In the long term, this method and [`explain_dataflow`] should be unified. In
/// order to do that, however, we first need to generalize the role
/// [`DataflowMetainfo`] as a carrier of metainformation for the optimization
/// pass in general, and not for a specific structure representing an
/// intermediate result.
pub(crate) fn explain_plan<T>(
    mut plan: T,
    format: ExplainFormat,
    config: &ExplainConfig,
    features: &OptimizerFeatures,
    humanizer: &dyn ExprHumanizer,
    cardinality_stats: BTreeMap<GlobalId, usize>,
    target_cluster: Option<&str>,
) -> Result<String, AdapterError>
where
    for<'a> Explainable<'a, T>: Explain<'a, Context = ExplainContext<'a>>,
{
    let context = ExplainContext {
        config,
        features,
        humanizer,
        cardinality_stats,
        used_indexes: Default::default(),
        finishing: Default::default(),
        duration: Default::default(),
        target_cluster,
        optimizer_notices: Default::default(),
    };

    Ok(Explainable::new(&mut plan).explain(&format, &context)?)
}

/// Statistics `EXPLAIN MEMORY BOUND` needs to populate its row and byte columns.
///
/// Empty for every other stage, and empty is always sound: it leaves those columns unknown
/// rather than guessing.
#[derive(Debug, Default, Clone)]
pub struct MemoryBoundStats {
    /// Row counts, keyed by the relation they describe.
    pub rows: BTreeMap<GlobalId, usize>,
    /// Arrangement statistics, keyed by the index that maintains them.
    pub arrangements: BTreeMap<GlobalId, ArrangementStats>,
    /// Indexes the compute controller reports hydrated on the plan's cluster.
    ///
    /// Empty where the caller cannot ask, which only costs coverage: an index still
    /// qualifies by having caught up with its relation's row count.
    pub hydrated_indexes: BTreeSet<GlobalId>,
}

/// Distinct-key bounds per relation, from the dataflow's own index imports.
///
/// A relation's key columns take at most as many distinct values as an index over them
/// reports keys, so this tightens a `Reduce` whose group key those columns cover. Lowering
/// matches the columns against plan shape; deciding which reports to trust is this
/// function's job.
///
/// Two reports are declined rather than approximated:
///
/// * An index whose key is not a list of plain columns. Matching an expression key would
///   mean comparing it against the group key's expressions, which lowering does not attempt.
/// * An index that may not have caught up. **A hydrating index reports fewer keys than its
///   collection holds**, and an under-count would understate a memory bound, which is the
///   direction that ends in an out-of-memory kill. An index qualifies either by the
///   controller reporting it hydrated, or by holding at least as many records as its
///   relation has rows. The second is only a proxy for the first, and it is kept because
///   the peek paths cannot ask the controller; where neither is available the index is
///   declined.
fn index_key_bounds(
    dataflow: &mz_compute_types::dataflows::DataflowDescription<mz_expr::OptimizedMirRelationExpr>,
    stats: &std::collections::BTreeMap<mz_repr::GlobalId, usize>,
    index_stats: &std::collections::BTreeMap<mz_repr::GlobalId, ArrangementStats>,
    hydrated: &BTreeSet<mz_repr::GlobalId>,
) -> BTreeMap<mz_repr::GlobalId, Vec<IndexKeyBound>> {
    use mz_ore::cast::CastFrom;

    let mut bounds: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (index_id, import) in &dataflow.index_imports {
        let Some(arrangement) = index_stats.get(index_id) else {
            continue;
        };
        let caught_up = hydrated.contains(index_id)
            || stats
                .get(&import.desc.on_id)
                .copied()
                .map(u64::cast_from)
                .is_some_and(|rows| arrangement.records >= rows);
        if !caught_up {
            continue;
        }
        let mut key_columns = BTreeSet::new();
        for key in &import.desc.key {
            match key {
                mz_expr::MirScalarExpr::Column(col, _) => {
                    key_columns.insert(*col);
                }
                _ => {
                    key_columns.clear();
                    break;
                }
            }
        }
        if key_columns.is_empty() {
            continue;
        }
        bounds
            .entry(import.desc.on_id)
            .or_default()
            .push(IndexKeyBound {
                key_columns,
                distinct_keys: arrangement.distinct_keys,
            });
    }
    bounds
}

/// Renders the static memory bound for each node of a physical plan.
///
/// Takes the optimized MIR rather than the lowered plan, because the widths need each node's
/// output type and LIR is type-erased. Lowering again here is what makes those types available;
/// it also means the reported plan is the one current optimizer features produce, which can
/// differ from a plan lowered before a feature flag moved.
pub(crate) fn memory_bound_rows(
    dataflow: mz_compute_types::dataflows::DataflowDescription<mz_expr::OptimizedMirRelationExpr>,
    features: &mz_repr::optimize::OptimizerFeatures,
    stats: MemoryBoundStats,
) -> Result<Vec<mz_repr::Row>, AdapterError> {
    use mz_compute_types::plan::LirRelationExpr;
    use mz_compute_types::plan::arrangement_count::Caveat;
    use mz_compute_types::plan::memory_bound::{bytes_per_row, node_label, nodes_by_id};
    use mz_ore::cast::CastFrom;
    use mz_repr::{Datum, Row};

    let index_key_bounds = index_key_bounds(
        &dataflow,
        &stats.rows,
        &stats.arrangements,
        &stats.hydrated_indexes,
    );
    let stats = stats.rows;

    // Sound upper bound rather than the heuristic estimate: an underestimate here would
    // understate the memory a plan needs, which is the dangerous direction.
    //
    // Invoked once per lowered node on that node's subtree, so this is quadratic in plan size.
    // Acceptable on an EXPLAIN, which is not on any hot path.
    let bound_features = features.clone();
    let row_bound: mz_compute_types::plan::RowBoundFn =
        Box::new(move |expr: &mz_expr::MirRelationExpr| {
            let mut builder = mz_transform::analysis::DerivedBuilder::new(&bound_features);
            builder.require(mz_transform::analysis::Cardinality::upper_bound(
                stats.clone(),
            ));
            let derived = builder.visit(expr);
            let estimate = *derived
                .as_view()
                .value::<mz_transform::analysis::Cardinality>()?;
            estimate.rounded().map(u64::cast_from)
        });

    let (dataflow, node_types, row_bounds) = LirRelationExpr::finalize_dataflow_with_node_types(
        dataflow,
        features,
        None,
        Some(row_bound),
        index_key_bounds,
    )
    .map_err(|e| AdapterError::Internal(format!("cannot lower plan: {e}")))?;

    let mut rows = Vec::new();
    for build in &dataflow.objects_to_build {
        let nodes = nodes_by_id(&build.plan);
        for (lir_id, entry) in bytes_per_row(&build.plan, &node_types) {
            let label = nodes
                .get(&lir_id)
                .map_or_else(|| "<unknown>".to_string(), |node| node_label(node));
            // A caveat means the plan alone does not settle the count, so name it rather than
            // presenting a guess as exact.
            let note = entry.caveat.map(|caveat| match caveat {
                Caveat::ArrangeByMayReuse => "may reuse an already-available arrangement",
                Caveat::ThresholdFlavorUnknown => {
                    "one more error arrangement if the input is an imported trace"
                }
                Caveat::JoinSourceMayReuse => "assumes the source arrangement is available",
            });
            // A width of `None` means some column has no static ceiling. Reporting a number
            // there would claim a bound that does not hold.
            let width = entry
                .row_width
                .map_or(Datum::Null, |w| Datum::UInt64(u64::cast_from(w)));
            let bytes = entry
                .bytes_per_row
                .map_or(Datum::Null, |b| Datum::UInt64(u64::cast_from(b)));
            // Total bytes needs every factor: unknown width or unknown rows means unknown
            // total, not a smaller one.
            let node_rows = row_bounds.get(&lir_id).copied();
            let total = match (entry.bytes_per_row, node_rows) {
                (Some(per_row), Some(n)) => u64::try_from(per_row)
                    .ok()
                    .and_then(|per_row| per_row.checked_mul(n)),
                _ => None,
            };
            rows.push(Row::pack_slice(&[
                Datum::UInt64(lir_id.into()),
                Datum::String(&label),
                Datum::UInt64(u64::cast_from(entry.arrangements)),
                width,
                bytes,
                node_rows.map_or(Datum::Null, Datum::UInt64),
                total.map_or(Datum::Null, Datum::UInt64),
                note.map_or(Datum::Null, Datum::String),
            ]));
        }
    }
    Ok(rows)
}
