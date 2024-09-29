// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for structures defined in this crate.

pub(crate) mod text;

use std::collections::BTreeMap;

use mz_expr::explain::{enforce_linear_chains, ExplainContext, ExplainMultiPlan, ExplainSource};
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::explain::{AnnotatedPlan, Explain, ExplainError, UnsupportedFormat};
use mz_repr::GlobalId;

use crate::dataflows::DataflowDescription;
use crate::plan::Plan;

impl<'a> Explain<'a> for DataflowDescription<Plan> {
    type Context = ExplainContext<'a>;

    type Text = ExplainMultiPlan<'a, Plan>;

    type Json = ExplainMultiPlan<'a, Plan>;

    type Dot = UnsupportedFormat;

    fn explain_text(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }

    fn explain_json(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }
}

impl<'a> DataflowDescription<Plan> {
    fn as_explain_multi_plan(
        &'a mut self,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainMultiPlan<'a, Plan>, ExplainError> {
        let export_ids = export_ids_for(self);
        let plans = self
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| {
                let public_id = export_ids
                    .get(&build_desc.id)
                    .unwrap_or(&build_desc.id)
                    .clone();
                let id = context
                    .humanizer
                    .humanize_id(public_id)
                    .unwrap_or_else(|| public_id.to_string());
                let plan = AnnotatedPlan {
                    plan: &build_desc.plan,
                    annotations: BTreeMap::default(),
                };
                (id, plan)
            })
            .collect::<Vec<_>>();

        let sources = self
            .source_imports
            .iter_mut()
            .map(|(id, (source_desc, _))| {
                let op = source_desc.arguments.operators.as_ref();
                ExplainSource::new(*id, op, context.config.filter_pushdown)
            })
            .collect::<Vec<_>>();

        Ok(ExplainMultiPlan {
            context,
            sources,
            plans,
        })
    }
}

impl<'a> Explain<'a> for DataflowDescription<OptimizedMirRelationExpr> {
    type Context = ExplainContext<'a>;

    type Text = ExplainMultiPlan<'a, MirRelationExpr>;

    type Json = ExplainMultiPlan<'a, MirRelationExpr>;

    type Dot = UnsupportedFormat;

    fn explain_text(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }

    fn explain_json(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }
}

impl<'a> DataflowDescription<OptimizedMirRelationExpr> {
    fn as_explain_multi_plan(
        &'a mut self,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainMultiPlan<'a, MirRelationExpr>, ExplainError> {
        let export_ids = export_ids_for(self);
        let plans = self
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| {
                // normalize the representation as linear chains
                // (this implies !context.config.raw_plans by construction)
                if context.config.linear_chains {
                    enforce_linear_chains(build_desc.plan.as_inner_mut())?;
                };

                let id = export_ids
                    .get(&build_desc.id)
                    .unwrap_or(&build_desc.id)
                    .clone();
                let id = context
                    .humanizer
                    .humanize_id(id)
                    .unwrap_or_else(|| id.to_string());
                let plan = AnnotatedPlan {
                    plan: build_desc.plan.as_inner(),
                    annotations: BTreeMap::default(),
                };
                Ok((id, plan))
            })
            .collect::<Result<Vec<_>, ExplainError>>()?;

        let sources = self
            .source_imports
            .iter_mut()
            .map(|(id, (source_desc, _))| {
                let op = source_desc.arguments.operators.as_ref();
                ExplainSource::new(*id, op, context.config.filter_pushdown)
            })
            .collect::<Vec<_>>();

        Ok(ExplainMultiPlan {
            context,
            sources,
            plans,
        })
    }
}

/// TODO(database-issues#7533): Add documentation.
pub fn export_ids_for<P, S, T>(dd: &DataflowDescription<P, S, T>) -> BTreeMap<GlobalId, GlobalId> {
    let mut map = BTreeMap::<GlobalId, GlobalId>::default();

    // Dataflows created from a `CREATE MATERIALIZED VIEW` have:
    //
    // 1. Exactly one entry in `objects_to_build` representing the dataflow to
    //    be installed. This entry has a transient ID that changes whenever the
    //    dataflow is re-installed.
    // 2. Exactly one entry in `sink_exports` referencing the `objects_to_build`
    //    entry.
    // 3. No enties in index_exports.
    //
    // Because the user-facing ID is for the `sink_exports` entry in (2), we
    // create a mapping.
    if dd.sink_exports.len() == 1 && dd.objects_to_build.len() == 1 && dd.index_exports.is_empty() {
        for (public_id, export) in dd.sink_exports.iter() {
            map.insert(export.from, *public_id);
        }
    }

    // Dataflows created from a `CREATE INDEX` adhere to the following
    // constraints.
    //
    // 1. One or more entries in `objects_to_build`. The last entry arranges a
    //    `Get $id` where $id might be:
    //    1. The previous `objects_to_build` entry that corresponds to the
    //       dataflow of the indexed view (if we index a VIEW).
    //    2. A `source_imports` entry (if we index a SOURCE, TABLE, or
    //       MATERIALIZED VIEW).
    //    3. An `index_imports` entry (if we index a SOURCE, TABLE, or
    //       MATERIALIZED VIEW that is already indexed).
    // 2. Exactly one entry in `index_exports` identified by the same ID as the
    //    last `objects_to_build` entry.
    //
    // Because there are no transient IDs involved in the above configurations,
    // we don't need to add further entries to the `map` to account for these
    // cases.

    map
}
