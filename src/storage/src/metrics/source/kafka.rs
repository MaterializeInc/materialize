// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for Kafka consumption.

use std::collections::BTreeMap;

use mz_ore::iter::IteratorExt;
use mz_ore::metric;
use mz_ore::metrics::{DeleteOnDropGauge, IntGaugeVec, MetricsRegistry};
use mz_repr::GlobalId;
use prometheus::core::AtomicI64;
use tracing::debug;

/// Definitions for kafka-specific per-partition metrics.
#[derive(Clone, Debug)]
pub(crate) struct KafkaSourceMetricDefs {
    pub(crate) partition_offset_max: IntGaugeVec,
}

impl KafkaSourceMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            partition_offset_max: registry.register(metric!(
                name: "mz_kafka_partition_offset_max",
                help: "High watermark offset on broker for partition",
                var_labels: ["topic", "source_id", "partition_id"],
            )),
        }
    }
}

/// Kafka-specific per-partition metrics.
pub(crate) struct KafkaSourceMetrics {
    labels: Vec<String>,
    defs: KafkaSourceMetricDefs,
    partition_offset_map: BTreeMap<i32, DeleteOnDropGauge<'static, AtomicI64, Vec<String>>>,
}

impl KafkaSourceMetrics {
    /// Create a `KafkaSourceMetrics` from the `KafkaSourceMetricDefs`.
    pub(crate) fn new(
        defs: &KafkaSourceMetricDefs,
        ids: Vec<i32>,
        topic: String,
        source_id: GlobalId,
    ) -> Self {
        Self {
            partition_offset_map: BTreeMap::from_iter(ids.iter().map(|id| {
                let labels = &[topic.clone(), source_id.to_string(), format!("{}", id)];
                (
                    *id,
                    defs.partition_offset_max
                        .get_delete_on_drop_metric(labels.to_vec()),
                )
            })),
            labels: vec![topic.clone(), source_id.to_string()],
            defs: defs.clone(),
        }
    }

    pub(crate) fn set_offset_max(&mut self, id: i32, offset: i64) {
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
                self.defs.partition_offset_max.get_delete_on_drop_metric(
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
