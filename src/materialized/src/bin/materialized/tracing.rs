// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use prometheus::IntCounterVec;
use tracing::span::{Attributes, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Id, Metadata, Subscriber};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// A tracing [`Layer`] that applies a [`LevelFilter`] to one layer only.
///
/// By default, tracing filter layers apply *globally*. See the ["Filtering with
/// `Layers`"][layer-filtering] section of the tracing-subscriber docs for
/// details. This means that you can't apply different filters to different
/// output streams—e.g., logging all messages to a file but only warnings to
/// stderr—as the most restrictive filter will win. This behavior is
/// unintuitive and generally not what anyone wants.
///
/// A `FilterLayer` is a workaround for the desired behavior until per-layer
/// filters are supported upstream (see [tokio-rs/tracing#508]). The idea is
/// to wrap an inner layer and apply a level filter manually, only notifying the
/// inner layer if the event matches the filter.
///
/// [layer-filtering]: https://docs.rs/tracing-subscriber/0.2.14/tracing_subscriber/layer/trait.Layer.html#filtering-with-layers
/// [tokio-rs/tracing#508]: https://github.com/tokio-rs/tracing/pull/508
pub struct FilterLayer<L, S> {
    layer: L,
    filter: LevelFilter,
    _inner: PhantomData<S>,
}

impl<L, S> FilterLayer<L, S>
where
    L: Layer<S> + 'static,
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    /// Constructs a new filter layer.
    ///
    /// The wrapped `layer` will be notified of only those events that are
    /// enabled by `filter`.
    pub fn new(layer: L, filter: LevelFilter) -> FilterLayer<L, S> {
        FilterLayer {
            layer,
            filter,
            _inner: PhantomData,
        }
    }
}

impl<S, L> Layer<S> for FilterLayer<L, S>
where
    L: Layer<S> + 'static,
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        self.layer.register_callsite(metadata)
    }

    fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, S>) -> bool {
        self.layer.enabled(metadata, ctx)
    }

    fn new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if self.filter.enabled(ctx.metadata(id).unwrap(), ctx.clone()) {
            self.layer.new_span(attrs, id, ctx)
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if self.filter.enabled(ctx.metadata(id).unwrap(), ctx.clone()) {
            self.layer.on_record(id, values, ctx)
        }
    }

    fn on_follows_from(&self, id: &Id, follows: &Id, ctx: Context<'_, S>) {
        if self.filter.enabled(ctx.metadata(id).unwrap(), ctx.clone()) {
            self.layer.on_follows_from(id, follows, ctx)
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if self.filter.enabled(event.metadata(), ctx.clone()) {
            self.layer.on_event(event, ctx)
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if self.filter.enabled(ctx.metadata(id).unwrap(), ctx.clone()) {
            self.layer.on_enter(id, ctx)
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        if self.filter.enabled(ctx.metadata(id).unwrap(), ctx.clone()) {
            self.layer.on_exit(id, ctx)
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if self.filter.enabled(ctx.metadata(&id).unwrap(), ctx.clone()) {
            self.layer.on_close(id, ctx)
        }
    }

    fn on_id_change(&self, old: &Id, new: &Id, ctx: Context<'_, S>) {
        if self.filter.enabled(ctx.metadata(old).unwrap(), ctx.clone()) {
            self.layer.on_id_change(old, new, ctx)
        }
    }
}

/// A tracing `Layer` that allows hooking into the reporting/filtering chain for log messages,
/// incrementing a counter for the severity of messages reported.
pub struct MetricsRecorderLayer<S> {
    counter: IntCounterVec,
    _inner: PhantomData<S>,
}

impl<S> MetricsRecorderLayer<S> {
    /// Construct a metrics-recording layer.
    pub fn new(counter: IntCounterVec) -> Self {
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
            .with_label_values(&[&metadata.level().to_string()])
            .inc();
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::MetricsRecorderLayer;
    use log::{error, info, warn};
    use prometheus::{IntCounterVec, Opts, Registry};
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[test]
    fn increments_per_sev_counter() {
        let counter_opts = Opts::new("test_counter", "test counter help");
        let counter = IntCounterVec::new(counter_opts, &["severity"]).unwrap();
        let r = Registry::new();
        r.register(Box::new(counter.clone())).unwrap();
        tracing_subscriber::registry()
            .with(MetricsRecorderLayer::new(counter))
            .init();

        info!("test message");
        (0..5).for_each(|_| warn!("a warning"));
        error!("test error");
        error!("test error");

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
