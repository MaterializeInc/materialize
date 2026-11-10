// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for the storage controller components

use mz_cluster_client::metrics::{ControllerMetrics, WallclockLagMetrics};
use mz_ore::metric;
use mz_ore::metrics::{DeleteOnDropCounter, IntCounterVec, MetricsRegistry};
use mz_repr::GlobalId;
use mz_storage_types::instances::StorageInstanceId;

/// Storage controller metrics
#[derive(Debug, Clone)]
pub struct StorageControllerMetrics {
    regressed_offset_known: IntCounterVec,
    /// Metrics shared with the compute controller.
    shared: ControllerMetrics,
}

impl StorageControllerMetrics {
    pub fn new(metrics_registry: &MetricsRegistry, shared: ControllerMetrics) -> Self {
        Self {
            regressed_offset_known: metrics_registry.register(metric!(
                name: "mz_storage_regressed_offset_known",
                help: "number of regressed offset_known stats for this id",
                var_labels: ["id"],
            )),
            shared,
        }
    }

    pub fn regressed_offset_known(
        &self,
        id: mz_repr::GlobalId,
    ) -> DeleteOnDropCounter<prometheus::core::AtomicU64, Vec<String>> {
        self.regressed_offset_known
            .get_delete_on_drop_metric(vec![id.to_string()])
    }

    pub fn wallclock_lag_metrics(
        &self,
        id: GlobalId,
        instance_id: Option<StorageInstanceId>,
    ) -> WallclockLagMetrics {
        self.shared
            .wallclock_lag_metrics(id.to_string(), instance_id.map(|x| x.to_string()), None)
    }
}
