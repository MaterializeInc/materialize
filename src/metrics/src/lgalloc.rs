// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Report lgalloc metrics.

use std::collections::BTreeMap;

use mz_ore::cast::CastFrom;
use mz_ore::metrics::{MetricsRegistry, raw};
use paste::paste;
use prometheus::core::{AtomicU64, GenericGauge};

use crate::MetricsUpdate;

macro_rules! metrics_size_class {
    ($namespace:ident $(($name:ident, $desc:expr, $metric:expr, $conv:expr)),*) => {
        paste! {
            pub(crate) struct LgMetrics {
                size_class: BTreeMap<usize, LgMetricsSC>,
                $($metric: raw::UIntGaugeVec,)*
            }
            struct LgMetricsSC {
                $($metric: GenericGauge<AtomicU64>,)*
            }
            impl LgMetrics {
                fn new(registry: &MetricsRegistry) -> Self {
                    Self {
                        size_class: BTreeMap::default(),
                        $($metric: registry.register(mz_ore::metric!(
                            name: concat!(stringify!($namespace), "_", stringify!($metric)),
                            help: $desc,
                            var_labels: ["size_class"],
                        )),)*
                    }
                }
                fn get_size_class(&mut self, size_class: usize) -> &LgMetricsSC {
                    self.size_class.entry(size_class).or_insert_with(|| {
                        let labels: &[&str] = &[&size_class.to_string()];
                        LgMetricsSC {
                            $($metric: self.$metric.with_label_values(labels),)*
                        }
                    })
                }
                fn update(&mut self) {
                    let stats = lgalloc::lgalloc_stats();
                    for (size_class, sc) in &stats.size_class {
                        let sc_stats = self.get_size_class(*size_class);
                        $(sc_stats.$metric.set(($conv)(u64::cast_from(sc.$name), *size_class));)*
                    }
                }
            }
        }
    };
}

fn normalize_by_size_class(value: u64, size_class: usize) -> u64 {
    value * u64::cast_from(size_class)
}

fn id(value: u64, _size_class: usize) -> u64 {
    value
}

metrics_size_class! {
    mz_metrics_lgalloc
    (allocations, "Number of region allocations in size class", allocations_total, id),
    (area_total_bytes, "Number of bytes in all areas in size class", area_total_bytes, id),
    (areas, "Number of areas backing size class", areas_total, id),
    (clean_regions, "Number of clean regions in size class", clean_regions_total, id),
    (clean_regions, "Number of clean regions in size class", clean_regions_bytes_total, normalize_by_size_class),
    (deallocations, "Number of region deallocations for size class", deallocations_total, id),
    (free_regions, "Number of free regions in size class", free_regions_total, id),
    (free_regions, "Number of free regions in size class", free_regions_bytes_total, normalize_by_size_class),
    (global_regions, "Number of global regions in size class", global_regions_total, id),
    (global_regions, "Number of global regions in size class", global_regions_bytes_total, normalize_by_size_class),
    (refill, "Number of area refills for size class", refill_total, id),
    (slow_path, "Number of slow path region allocations for size class", slow_path_total, id),
    (thread_regions, "Number of thread regions in size class", thread_regions_total, id),
    (thread_regions, "Number of thread regions in size class", thread_regions_bytes_total, normalize_by_size_class),
    (clear_eager_total, "Number of eager clears for size class", clear_eager_total, id),
    (clear_eager_total, "Number of eager clears for size class", clear_eager_bytes_total, normalize_by_size_class),
    (clear_slow_total, "Number of slow clears for size class", clear_slow_total, id),
    (clear_slow_total, "Number of slow clears for size class", clear_slow_bytes_total, normalize_by_size_class)
}

/// Register a task to read lgalloc stats.
pub(crate) fn register_metrics_into(metrics_registry: &MetricsRegistry) -> LgMetrics {
    LgMetrics::new(metrics_registry)
}

impl MetricsUpdate for LgMetrics {
    type Error = std::convert::Infallible;
    const NAME: &'static str = "lgalloc";
    fn update(&mut self) -> Result<(), Self::Error> {
        self.update();
        Ok(())
    }
}
