// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lookup join execution dataflow construction.
//!
//! Consult [LookupJoinPlan] documentation for details.

use std::collections::BTreeSet;
use std::rc::Rc;

use differential_dataflow::VecCollection;
use mz_compute_types::plan::join::LookupJoinPlan;
use mz_dyncfg::ConfigSet;
use mz_expr::ErrorScope;
use mz_repr::{Diff, Row};

use crate::render::RenderTimestamp;
use crate::render::columnar::{columnar_consolidate, vec_to_columnar};
use crate::render::context::{CollectionBundle, Context};
use crate::render::errors::DataflowErrorSer;
use crate::render::join::delta_join::{
    build_path_stages, build_update_stream_raw, bundle_errs, prune_bundle, record_path_arrangements,
};

impl<'scope, T: RenderTimestamp> Context<'scope, T> {
    /// Renders `MirRelationExpr::Join` as a lookup join, a single delta join path that responds
    /// only to the positive updates of its source relation.
    pub fn render_lookup_join(
        &self,
        inputs: Vec<CollectionBundle<'scope, T>>,
        join_plan: LookupJoinPlan,
    ) -> CollectionBundle<'scope, T> {
        let (oks, errs) = self.scope.clone().region_named("Join(Lookup)", |inner| {
            // Entering a collection into a region does work proportional to its data, so we prune
            // each bundle to what the join reads before bringing it into `inner`.
            let mut arrangements = vec![BTreeSet::new(); inputs.len()];
            let mut raw = vec![false; inputs.len()];
            record_path_arrangements(
                &mut arrangements,
                &mut raw,
                join_plan.source_relation,
                None,
                &join_plan.stage_plans,
            );
            let bundles = inputs
                .iter()
                .enumerate()
                .map(|(index, cb)| {
                    prune_bundle(cb, raw[index], &arrangements[index]).enter_region(inner)
                })
                .collect::<Vec<_>>();

            let mut inner_errs: Vec<_> = bundles.iter().flat_map(bundle_errs).collect();
            let update_stream = build_lookup_join(
                &bundles,
                join_plan,
                &self.config_set,
                self.error_scope(),
                &mut inner_errs,
            );

            (
                update_stream.leave_region(self.scope),
                differential_dataflow::collection::concatenate(inner, inner_errs)
                    .leave_region(self.scope),
            )
        });
        CollectionBundle::from_edge(vec_to_columnar(oks), errs)
    }
}

/// Builds the lookup join `join_plan` over `bundles`, which must hold the raw source collection and
/// the lookup arrangements the plan reads.
///
/// The errors the join produces are pushed to `errs_out`. The errors `bundles` carry are the
/// caller's to propagate.
fn build_lookup_join<'scope, T: RenderTimestamp>(
    bundles: &[CollectionBundle<'scope, T>],
    join_plan: LookupJoinPlan,
    config_set: &Rc<ConfigSet>,
    scope: ErrorScope,
    errs_out: &mut Vec<VecCollection<'scope, T, DataflowErrorSer, Diff>>,
) -> VecCollection<'scope, T, Row, Diff> {
    let LookupJoinPlan {
        source_relation,
        initial_closure,
        stage_plans,
        final_closure,
    } = join_plan;

    let (source, _source_errs) = bundles[source_relation]
        .collection
        .clone()
        .expect("The unarranged collection doesn't exist.");
    // Deciding whether an update is positive requires the source's updates at each time to
    // be consolidated. A persist snapshot need not be: a row inserted and retracted before
    // the as-of can appear as a `+1` and a `-1` in different parts, and dropping the `-1`
    // alone would resurrect the row. Nor need a temporal filter's output be: it presents a
    // row whose validity ends before the as-of as a `+1` and a `-1` at the as-of. The
    // consolidation holds each update back only until the source frontier passes its time.
    let source = columnar_consolidate(source, "LookupJoinSourceConsolidation");
    // A non-positive update produces no output. Without an arrangement of the source,
    // changes to the lookup relations produce none either.
    let (update_stream, err_stream) = build_update_stream_raw(
        source,
        "LookupJoinSource",
        |diff| diff.is_positive(),
        initial_closure,
        scope,
    );
    errs_out.push(err_stream);

    // Each stage presents the lookup relation's updates at times less or equal the source
    // update's time. The half join holds its output for a time until the lookup
    // arrangement's frontier passes that time, so each result reflects every lookup update
    // at the source update's time, and nothing later. It also holds back compaction of the
    // lookup arrangement to the source frontier, as a delta join does.
    build_path_stages(
        update_stream,
        bundles,
        stage_plans,
        |_lookup_relation| true,
        final_closure,
        "LookupJoinFinalization",
        config_set,
        scope,
        errs_out,
    )
}

#[cfg(test)]
mod tests;
