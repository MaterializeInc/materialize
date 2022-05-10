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
use tracing::debug;

use mz_ore::iter::IteratorExt;
use mz_ore::metrics::{DeleteOnDropGauge, GaugeVecExt};
use mz_repr::GlobalId;

use crate::source::metrics::SourceBaseMetrics;
pub(super) struct KafkaPartitionMetrics {
    labels: Vec<String>,
    base_metrics: SourceBaseMetrics,
    partition_offset_map: HashMap<i32, DeleteOnDropGauge<'static, AtomicI64, Vec<String>>>,
}

impl KafkaPartitionMetrics {
    pub fn new(
        base_metrics: SourceBaseMetrics,
        ids: Vec<i32>,
        topic: String,
        source_id: GlobalId,
    ) -> Self {
        let metrics = &base_metrics.partition_specific;
        Self {
            partition_offset_map: HashMap::from_iter(ids.iter().map(|id| {
                let labels = &[topic.clone(), source_id.to_string(), format!("{}", id)];
                (
                    *id,
                    metrics
                        .partition_offset_max
                        .get_delete_on_drop_gauge(labels.to_vec()),
                )
            })),
            labels: vec![topic.clone(), source_id.to_string()],
            base_metrics,
        }
    }

    pub fn set_offset_max(&mut self, id: i32, offset: i64) {
        // Valid partition ids start at 0, librdkafka uses -1 as a sentinel for unassigned partitions
        if id < 0 {
            return;
        }
        // This offset value is another librdkafka sentinel indicating it got an invalid high watermark from the broker
        if offset == -1001 {
            // TODO(nharring-adjacent): This is potentially spammy so its at debug but it would be better as info with sampling
            debug!("Got invalid high watermark for partition {}", id);
            return;
        }
        self.partition_offset_map
            .entry(id)
            .or_insert_with_key(|id| {
                self.base_metrics
                    .partition_specific
                    .partition_offset_max
                    .get_delete_on_drop_gauge(
                        self.labels
                            .iter()
                            .cloned()
                            .chain_one(format!("{}", id))
                            .collect(),
                    )
            })
            .set(offset);
    }
}
