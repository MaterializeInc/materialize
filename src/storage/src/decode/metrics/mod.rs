// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for tracking metrics related to decoding.

use mz_ore::metric;
use mz_ore::metrics::raw::IntCounterVec;
use mz_ore::metrics::MetricsRegistry;

use crate::decode::{DataDecoderInner, PreDelimitedFormat};

/// Metrics specific to a single worker.
#[derive(Clone, Debug)]
pub struct DecodeMetrics {
    events_read: IntCounterVec,
}

impl DecodeMetrics {
    /// TODO(undocumented)
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            events_read: registry.register(metric!(
                name: "mz_dataflow_events_read_total",
                help: "Count of events we have read from the wire",
                var_labels: ["format", "status"],
            )),
        }
    }

    fn counter_inc(&self, decoder: &DataDecoderInner, success: bool, n: usize) {
        let format_label = match decoder {
            DataDecoderInner::Avro(_) => "avro",
            DataDecoderInner::Csv(_) => "csv",
            DataDecoderInner::DelimitedBytes { format, .. }
            | DataDecoderInner::PreDelimited(format) => match format {
                PreDelimitedFormat::Bytes => "raw",
                PreDelimitedFormat::Text => "text",
                PreDelimitedFormat::Regex(..) => "regex",
                PreDelimitedFormat::Protobuf(..) => "protobuf",
            },
        };
        let success_label = if success { "success" } else { "error" };
        self.events_read
            .with_label_values(&[format_label, success_label])
            .inc_by(n as u64);
    }

    pub(crate) fn count_successes(&self, decoder: &DataDecoderInner, n: usize) {
        self.counter_inc(decoder, true, n);
    }

    pub(crate) fn count_errors(&self, decoder: &DataDecoderInner, n: usize) {
        self.counter_inc(decoder, true, n);
    }
}
