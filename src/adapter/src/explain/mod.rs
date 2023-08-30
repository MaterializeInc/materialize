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

use std::time::Duration;

use mz_compute_client::types::dataflows::DataflowDescription;
use mz_expr::explain::ExplainContext;
use mz_repr::explain::{
    Explain, ExplainConfig, ExplainError, ExplainFormat, ExprHumanizer, UsedIndexes,
};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::optimizer_notices::OptimizerNotice;

use crate::AdapterError;

pub(crate) mod fast_path;
pub(crate) mod hir;
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
    humanizer: &dyn ExprHumanizer,
    dataflow_metainfo: &DataflowMetainfo,
) -> Result<String, AdapterError>
where
    for<'a> Explainable<'a, DataflowDescription<T>>: Explain<'a, Context = ExplainContext<'a>>,
{
    // Collect the list of indexes used by the dataflow at this point.
    let used_indexes = UsedIndexes::new(
        plan.index_imports
            .iter()
            .map(|(id, index_import)| {
                (
                    *id,
                    index_import.usage_types.clone().expect(
                        "prune_and_annotate_dataflow_index_imports should have been called already",
                    ),
                )
            })
            .collect(),
    );

    let optimizer_notices =
        OptimizerNotice::explain(&dataflow_metainfo.optimizer_notices, humanizer)
            .map_err(ExplainError::FormatError)?;

    let context = ExplainContext {
        config,
        humanizer,
        used_indexes,
        finishing: None,
        duration: Duration::default(),
        optimizer_notices,
    };

    Ok(Explainable::new(&mut plan).explain(&format, &context)?)
}
