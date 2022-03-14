// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use prometheus::core::AtomicI64;

use mz_expr::SourceInstanceId;
use mz_ore::iter::IteratorExt;
use mz_ore::metrics::{DeleteOnDropGauge, GaugeVecExt, IntGaugeVec};

use crate::source::metrics::PartitionSpecificMetrics;

pub(super) struct KafkaPartitionMetrics {
    labels: Vec<String>,
    // gauge_vec: &'a IntGaugeVec,
    partition_offset_map: HashMap<i32, DeleteOnDropGauge<'static, AtomicI64, Vec<String>>>,
}

impl KafkaPartitionMetrics {
    pub fn new(
        metrics: &PartitionSpecificMetrics,
        ids: Vec<i32>,
        topic: String,
        source_name: String,
        source_id: SourceInstanceId,
    ) -> Self {
        Self {
            partition_offset_map: HashMap::from_iter(ids.iter().map(|id| {
                let labels = &[
                    topic.clone(),
                    source_name.clone(),
                    source_id.to_string(),
                    format!("{}", id),
                ];
                (
                    *id,
                    metrics
                        .partition_offset_max
                        .get_delete_on_drop_gauge(labels.to_vec()),
                )
            })),
            labels: vec![topic.clone(), source_name.clone(), source_id.to_string()],
            // gauge_vec: &metrics.partition_offset_max,
        }
    }

    pub fn set_offset_max(&mut self, id: i32, offset: i64) {
        self.partition_offset_map
            .entry(id)
            // .or_insert_with_key(|id| {
            //     let labels = self.labels.iter().cloned().chain_one(format!("{}", id)).collect();
            //     self.metrics.partition_offset_max.get_delete_on_drop_gauge(labels)
            // })
            .and_modify(|gauge| gauge.set(offset));
    }
}
