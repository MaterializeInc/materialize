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
use mz_repr::explain::{Explain, ExplainConfig, ExplainError, ExplainFormat, ExprHumanizer};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::GlobalId;
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
