// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use mz_catalog::builtin::{Builtin, MZ_CATALOG_RAW, MZ_COMPUTE_HYDRATION_TIMES_IND};
use mz_expr::SafeMfpPlan;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::{Row, RowArena};
use mz_sql::catalog::IdReference;
use mz_sql::optimizer_metrics::OptimizerMetrics;

use super::{Index, Optimizer};
use crate::catalog::{Catalog, CatalogState};
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::optimize::{Optimize, OptimizerConfig};

#[mz_ore::test(tokio::test)]
async fn hydration_times_filters_catalog_protection_before_backpressure() {
    Catalog::with_debug(|catalog| async move {
        let state = Arc::new(catalog.state().clone());
        let index_id = state.resolve_builtin_object(&Builtin::<IdReference>::Index(
            &MZ_COMPUTE_HYDRATION_TIMES_IND,
        ));
        let entry = state.get_entry(&index_id);
        let index = entry.index().expect("builtin hydration-times index");
        let mut optimizer = Optimizer::new(
            Arc::<CatalogState>::clone(&state),
            ComputeInstanceSnapshot::new_from_parts(index.cluster_id, BTreeSet::new()),
            index.global_id,
            OptimizerConfig::from(state.system_config()),
            OptimizerMetrics::register_into(&MetricsRegistry::new(), Duration::ZERO),
        );
        let mir = optimizer
            .optimize(Index::new(
                entry.name().clone(),
                index.on,
                index.keys.to_vec(),
            ))
            .expect("optimize builtin index MIR");
        let (dataflow, _) = optimizer
            .optimize(mir)
            .expect("optimize builtin index LIR")
            .unapply();
        let raw_id = state
            .get_entry(&state.resolve_builtin_source(&MZ_CATALOG_RAW))
            .source()
            .expect("raw catalog source")
            .global_id;
        // Compute evaluates these source operators before LimitProgress. Filtering
        // later in the dataflow would still let unrelated catalog updates create
        // pending timestamps in retained metrics' logical backpressure.
        let operators = dataflow
            .source_imports
            .get(&raw_id)
            .expect("builtin index imports raw catalog")
            .desc
            .arguments
            .operators
            .clone()
            .expect("raw catalog import must filter before LimitProgress");
        let plan = SafeMfpPlan::from_mfp(operators);
        for (json, retained) in [
            (
                r#"{"kind":"CollectionCompactionBound","key":{"id":{"User":1}},"value":{"frontier":1000}}"#,
                false,
            ),
            (
                r#"{"kind":"ClientReadRequirement","key":{"incarnation":1,"id":{"User":1}},"value":{"frontier":1000}}"#,
                false,
            ),
            (
                r#"{"kind":"FenceToken","deploy_generation":42,"epoch":17}"#,
                true,
            ),
            (
                r#"{"kind":"Config","key":{"key":"catalog_read_protection_enabled"},"value":{"value":1}}"#,
                true,
            ),
        ] {
            let row = json.parse::<Jsonb>().expect("valid catalog JSON").into_row();
            let arena = RowArena::new();
            let mut output = Row::default();
            let actual = plan
                .evaluate_into(&mut row.iter().collect(), &arena, &mut output)
                .expect("evaluate raw catalog source operators")
                .is_some();
            assert_eq!(actual, retained, "source import retention for {json}");
        }
    })
    .await;
}
