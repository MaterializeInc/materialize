// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use ore::metrics::ThirdPartyMetric;
use prometheus::IntCounterVec;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// A tracing [`Layer`] that allows hooking into the reporting/filtering chain
/// for log messages, incrementing a counter for the severity of messages
/// reported.
pub struct MetricsRecorderLayer<S> {
    counter: ThirdPartyMetric<IntCounterVec>,
    _inner: PhantomData<S>,
}

impl<S> MetricsRecorderLayer<S> {
    /// Construct a metrics-recording layer.
    pub fn new(counter: ThirdPartyMetric<IntCounterVec>) -> Self {
        Self {
            counter,
            _inner: PhantomData,
        }
    }
}

impl<S> Layer<S> for MetricsRecorderLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, ev: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = ev.metadata();
        self.counter
            .third_party_metric_with_label_values(&[&metadata.level().to_string()])
            .inc();
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::MetricsRecorderLayer;
    use log::{error, info, warn};
    use ore::metric;
    use ore::metrics::raw::IntCounterVec;
    use ore::metrics::{MetricsRegistry, ThirdPartyMetric};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[test]
    fn increments_per_sev_counter() {
        let r = MetricsRegistry::new();
        let counter: ThirdPartyMetric<IntCounterVec> = r.register_third_party_visible(metric!(
            name: "test_counter",
            help: "a test counter",
            var_labels: ["severity"],
        ));
        tracing_subscriber::registry()
            .with(MetricsRecorderLayer::new(counter))
            .init();

        info!("test message");
        (0..5).for_each(|_| warn!("a warning"));
        error!("test error");
        error!("test error");

        println!("gathered: {:?}", r.gather());

        let metric = r
            .gather()
            .into_iter()
            .find(|fam| fam.get_name() == "test_counter")
            .expect("Didn't find the counter we set up");
        let mut sevs: HashMap<&str, u32> = HashMap::new();
        for counter in metric.get_metric() {
            let sev = counter.get_label()[0].get_value();
            sevs.insert(sev, counter.get_counter().get_value() as u32);
        }
        let mut sevs: Vec<(&str, u32)> = sevs.into_iter().collect();
        sevs.sort_by_key(|(name, _)| name.to_string());
        assert_eq!(&[("ERROR", 2), ("INFO", 1), ("WARN", 5)][..], &sevs[..]);
    }
}
