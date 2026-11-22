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

use mz_catalog::builtin::{
    Builtin, MZ_CATALOG_DEPLOYMENT_AUTHORITY, MZ_CATALOG_RAW, MZ_COMPUTE_HYDRATION_TIMES_IND,
};
use mz_ore::metrics::MetricsRegistry;
use mz_sql::catalog::IdReference;
use mz_sql::optimizer_metrics::OptimizerMetrics;

use super::{Index, Optimizer};
use crate::catalog::{Catalog, CatalogState};
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::optimize::{Optimize, OptimizerConfig};

#[mz_ore::test(tokio::test)]
async fn hydration_times_isolates_catalog_history() {
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
        let authority_id =
            state
                .get_entry(&state.resolve_builtin_object(
                    &Builtin::<IdReference>::MaterializedView(&MZ_CATALOG_DEPLOYMENT_AUTHORITY),
                ))
                .latest_global_id();
        // Retained metrics may retain the authority output's history, but must
        // not pin the catalog journal through their reconstruction window.
        assert!(dataflow.source_imports.contains_key(&authority_id));
        assert!(!dataflow.source_imports.contains_key(&raw_id));
    })
    .await;
}
