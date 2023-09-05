// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! "Base" metrics used by all dataflow sources.
//!
//! We label metrics by the concrete source that they get emitted from (which makes these metrics
//! in-eligible for ingestion by third parties), so that means we have to register the metric
//! vectors to the registry once, and then generate concrete instantiations of them for the
//! appropriate source.

use mz_ore::metric;
use mz_ore::metrics::{CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt};
use mz_ore::metrics::{IntCounterVec, MetricsRegistry};
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_client::types::sources::SourceDesc;
use prometheus::core::{AtomicF64, AtomicI64, AtomicU64};

/// A set of base metrics that hang off a central metrics registry, labeled by the object
/// they belong to.
#[derive(Debug, Clone)]
pub struct StorageBaseMetrics {
    pub(crate) active_objects: IntCounterVec,
}

impl StorageBaseMetrics {
    /// TODO(undocumented)
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            active_objects: registry.register(metric!(
                name: "mz_storage_active_objects",
                help: "All active objects (sources and sinks)",
                var_labels: ["id", "type", "connnection_type", "connection_networking", "encoding", "encoding_networking", "envelope"],
            )),
        }
    }
}

pub struct StorageMetrics {
    pub(crate) active_objects: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

use mz_storage_client::types::sources::StorageDescription;
impl StorageMetrics {
    pub fn new_source(
        base: &StorageBaseMetrics,
        worker_id: usize,
        id: GlobalId,
        source_desc: &SourceDesc,
    ) -> Option<Self> {
        Self::new(
            base,
            worker_id,
            id,
            source_desc.type_(),
            source_desc.connection_type(),
            source_desc.connection_networking(),
            source_desc.encoding(),
            source_desc.encoding_networking(),
            source_desc.envelope_type(),
        )
    }
    /// Initializes source metrics used in the `persist_sink`.
    fn new(
        base: &StorageBaseMetrics,
        worker_id: usize,
        id: GlobalId,
        type_: String,
        connnection_type: String,
        connnection_networking: String,
        encoding_type: String,
        encoding_networking: String,
        envelope: String,
    ) -> Option<Self> {
        if worker_id != 0 {
            return None;
        }

        Some(Self {
            active_objects: base.active_objects.get_delete_on_drop_counter(vec![
                id.to_string(),
                type_,
                connnection_type,
                connnection_networking,
                encoding_type,
                encoding_networking,
                envelope,
            ]),
        })
    }
}
