// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Report lgalloc metrics.

use std::collections::BTreeMap;
use std::time::Duration;

use lgalloc::SizeClassStats;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{raw, MetricsRegistry};
use paste::paste;
use prometheus::core::{AtomicU64, GenericGauge};

macro_rules! metrics {
    ($namespace:ident $(($name:ident, $desc:expr, $suffix:expr, $conv:expr)),*) => {
        metrics! { @define $namespace $(($name, $desc, $suffix, $conv)),*}
    };
    (@define $namespace:ident $(($name:ident, $desc:expr, $suffix:expr, $conv:expr)),*) => {
        paste! {
            struct LgMetrics {
                stats: lgalloc::LgAllocStats,
                size_class: BTreeMap<usize, LgMetricsSC>,
                $([<$name $suffix>]: raw::UIntGaugeVec,)*
            }
            struct LgMetricsSC {
                $([<$name $suffix>]: GenericGauge<AtomicU64>,)*
            }
            impl LgMetrics {
                fn new(registry: &MetricsRegistry) -> Self {
                    Self {
                        size_class: BTreeMap::default(),
                        stats:  lgalloc::LgAllocStats::default(),
                        $([<$name $suffix>]: registry.register(mz_ore::metric!(
                            name: concat!(stringify!($namespace), "_", stringify!($name), $suffix),
                            help: $desc,
                            var_labels: ["size_class"],
                        )),)*
                    }
                }
                fn update(&mut self) {
                    lgalloc::lgalloc_stats(&mut self.stats);
                    for sc in &self.stats.size_class {
                        let sc_stats = self.size_class.entry(sc.size_class).or_insert_with(|| {
                            LgMetricsSC {
                                $([<$name $suffix>]: self.[<$name $suffix>].with_label_values(&[&sc.size_class.to_string()]),)*
                            }
                        });
                        $(sc_stats.[<$name $suffix>].set(($conv)(u64::cast_from(sc.$name), sc));)*
                    }
                }
            }
        }
    };
}

fn normalize_by_size_class(value: u64, stats: &SizeClassStats) -> u64 {
    value * u64::cast_from(stats.size_class)
}

fn id(value: u64, _stats: &SizeClassStats) -> u64 {
    value
}

metrics! {
    mz_metrics_lgalloc
    (allocations, "Number of region allocations in size class", "_total", id),
    (area_total_bytes, "Number of bytes in all areas in size class", "", id),
    (areas, "Number of areas backing size class", "_total", id),
    (clean_regions, "Number of clean regions in size class", "_total", id),
    (clean_regions, "Number of clean regions in size class", "_total_bytes", normalize_by_size_class),
    (deallocations, "Number of region deallocations for size class", "_total", id),
    (free_regions, "Number of free regions in size class", "_total", id),
    (free_regions, "Number of free regions in size class", "_total_bytes", normalize_by_size_class),
    (global_regions, "Number of global regions in size class", "_total", id),
    (global_regions, "Number of global regions in size class", "_total_bytes", normalize_by_size_class),
    (refill, "Number of area refills for size class", "_total", id),
    (slow_path, "Number of slow path region allocations for size class", "_total", id),
    (thread_regions, "Number of thread regions in size class", "_total", id),
    (thread_regions, "Number of thread regions in size class", "_total_bytes", normalize_by_size_class)
}

/// Register a task to read lgalloc stats.
#[allow(clippy::unused_async)]
pub async fn register_metrics_into(metrics_registry: &MetricsRegistry) {
    let mut lgmetrics = LgMetrics::new(metrics_registry);

    mz_ore::task::spawn(|| "lgalloc_stats_update", async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            lgmetrics.update();
        }
    });
}
