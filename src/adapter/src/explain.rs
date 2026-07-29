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

use std::collections::BTreeMap;
use std::sync::Arc;

use mz_compute_types::dataflows::DataflowDescription;
use mz_expr::explain::ExplainContext;
use mz_repr::GlobalId;
use mz_repr::explain::{Explain, ExplainConfig, ExplainError, ExplainFormat, ExprHumanizer};
use mz_repr::optimize::OptimizerFeatures;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::OptimizerNotice;

use crate::AdapterError;

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

/// Renders the static memory bound for each node of a physical plan.
///
/// Takes the optimized MIR rather than the lowered plan, because the widths need each node's
/// output type and LIR is type-erased. Lowering again here is what makes those types available;
/// it also means the reported plan is the one current optimizer features produce, which can
/// differ from a plan lowered before a feature flag moved.
pub(crate) fn memory_bound_rows(
    dataflow: mz_compute_types::dataflows::DataflowDescription<mz_expr::OptimizedMirRelationExpr>,
    features: &mz_repr::optimize::OptimizerFeatures,
) -> Result<Vec<mz_repr::Row>, AdapterError> {
    use mz_compute_types::plan::LirRelationExpr;
    use mz_compute_types::plan::arrangement_count::Caveat;
    use mz_compute_types::plan::memory_bound::{bytes_per_row, node_label, nodes_by_id};
    use mz_ore::cast::CastFrom;
    use mz_repr::{Datum, Row};

    let (dataflow, node_types) =
        LirRelationExpr::finalize_dataflow_with_node_types(dataflow, features, None)
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
            rows.push(Row::pack_slice(&[
                Datum::UInt64(lir_id.into()),
                Datum::String(&label),
                Datum::UInt64(u64::cast_from(entry.arrangements)),
                width,
                bytes,
                note.map_or(Datum::Null, Datum::String),
            ]));
        }
    }
    Ok(rows)
}
