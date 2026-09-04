// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for storage dataflow operators

use std::sync::atomic::{AtomicU64, Ordering};

use mz_ore::metrics::{IntCounter, UIntGauge};

/// Metric handles for one instance of the `backpressure` operator.
///
/// The series behind the handles belong to whoever constructed them and stay
/// registered for as long as that owner keeps them: the persist client's
/// process-level backpressure series for `persist_source`, or a source's own
/// per-worker series for upsert. Several operator instances may share one
/// gauge, which is why the gauge handle is a [GaugeContribution].
#[derive(Debug)]
pub struct BackpressureOperatorMetrics {
    /// Bytes this operator emitted.
    pub emitted_bytes: IntCounter,
    /// This operator's share of the gauge: the inflight bytes it most recently
    /// stalled on.
    pub last_backpressured_bytes: GaugeContribution,
    /// Bytes retired by processing downstream of this operator.
    pub retired_bytes: IntCounter,
}

impl BackpressureOperatorMetrics {
    pub fn new(
        emitted_bytes: IntCounter,
        last_backpressured_bytes: UIntGauge,
        retired_bytes: IntCounter,
    ) -> Self {
        BackpressureOperatorMetrics {
            emitted_bytes,
            last_backpressured_bytes: GaugeContribution::new(last_backpressured_bytes),
            retired_bytes,
        }
    }
}

/// One contributor's share of a gauge that reads as the sum over all
/// contributors. Dropping the contribution withdraws it.
#[derive(Debug)]
pub struct GaugeContribution {
    gauge: UIntGauge,
    contributed: AtomicU64,
}

impl GaugeContribution {
    pub fn new(gauge: UIntGauge) -> Self {
        GaugeContribution {
            gauge,
            contributed: AtomicU64::new(0),
        }
    }

    /// Replaces this contributor's share with `value`.
    pub fn set(&self, value: u64) {
        let previous = self.contributed.swap(value, Ordering::AcqRel);
        // Add before subtracting so the shared total never dips below the sum
        // of the other contributions.
        self.gauge.add(value);
        self.gauge.sub(previous);
    }
}

impl Drop for GaugeContribution {
    fn drop(&mut self) {
        self.gauge.sub(self.contributed.load(Ordering::Acquire));
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::metrics::UIntGauge;

    use super::GaugeContribution;

    #[mz_ore::test]
    fn gauge_contribution_sums_live_shares_and_withdraws_on_drop() {
        let gauge = UIntGauge::new("gauge", "help").expect("valid metric");
        let a = GaugeContribution::new(gauge.clone());
        let b = GaugeContribution::new(gauge.clone());

        a.set(5);
        b.set(3);
        assert_eq!(gauge.get(), 8);

        a.set(2);
        assert_eq!(gauge.get(), 5);

        drop(a);
        assert_eq!(gauge.get(), 3);

        b.set(0);
        assert_eq!(gauge.get(), 0);
    }
}
