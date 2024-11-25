// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Report lgalloc metrics.

use std::collections::BTreeMap;
use std::ops::AddAssign;
use std::time::Duration;
use tracing::error;

use lgalloc::{FileStats, SizeClassStats};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{raw, MetricsRegistry};
use paste::paste;
use prometheus::core::{AtomicU64, GenericGauge};

/// Error during FileStats
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error {
    /// Kind of error
    #[from]
    pub kind: ErrorKind,
}

/// Kind of error during FileStats
#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    /// Failed to get file stats
    #[error("Failed to get file stats: {0}")]
    FileStatsFailed(String),
}

impl Error {
    /// Create a new error
    pub fn new(kind: ErrorKind) -> Error {
        Error { kind }
    }
}

/// An accumulator for [`FileStats`].
#[derive(Default)]
struct FileStatsAccum {
    /// The size of the file in bytes.
    pub file_size: usize,
    /// Size of the file on disk in bytes.
    pub allocated_size: usize,
    /// Number of mapped bytes, if different from `dirty`. Consult `man 7 numa` for details.
    pub mapped: usize,
    /// Number of active bytes. Consult `man 7 numa` for details.
    pub active: usize,
    /// Number of dirty bytes. Consult `man 7 numa` for details.
    pub dirty: usize,
}

impl AddAssign<&FileStats> for FileStatsAccum {
    fn add_assign(&mut self, rhs: &FileStats) {
        self.file_size += rhs.file_size;
        self.allocated_size += rhs.allocated_size;
        self.mapped += rhs.mapped;
        self.active += rhs.active;
        self.dirty += rhs.dirty;
    }
}

macro_rules! metrics_size_class {
    ($namespace:ident
        @size_class ($(($name:ident, $desc:expr, $metric:expr, $conv:expr)),*)
        @file ($(($f_name:ident, $f_metric:ident, $f_desc:expr)),*)
    ) => {
        metrics_size_class! {
            @define $namespace
            @size_class $(($name, $desc, $metric, $conv)),*
            @file $(($f_name, $f_metric, $f_desc)),*
        }
    };
    (@define $namespace:ident
        @size_class $(($name:ident, $desc:expr, $metric:expr, $conv:expr)),*
        @file $(($f_name:ident, $f_metric:ident, $f_desc:expr)),*
    ) => {
        paste! {
            struct LgMetrics {
                size_class: BTreeMap<usize, LgMetricsSC>,
                $($metric: raw::UIntGaugeVec,)*
                $($f_metric: raw::UIntGaugeVec,)*
            }
            struct LgMetricsSC {
                $($metric: GenericGauge<AtomicU64>,)*
                $($f_metric: GenericGauge<AtomicU64>,)*
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
                        $($f_metric: registry.register(mz_ore::metric!(
                            name: concat!(stringify!($namespace), "_", stringify!($f_metric)),
                            help: $f_desc,
                            var_labels: ["size_class"],
                        )),)*
                    }
                }
                fn get_size_class(&mut self, size_class: usize) -> &LgMetricsSC {
                    self.size_class.entry(size_class).or_insert_with(|| {
                        let labels: &[&str] = &[&size_class.to_string()];
                        LgMetricsSC {
                            $($metric: self.$metric.with_label_values(labels),)*
                            $($f_metric: self.$f_metric.with_label_values(labels),)*
                        }
                    })
                }
                fn update(&mut self) -> Result<(), Error> {
                    let stats = lgalloc::lgalloc_stats();
                    for sc in &stats.size_class {
                        let sc_stats = self.get_size_class(sc.size_class);
                        $(sc_stats.$metric.set(($conv)(u64::cast_from(sc.$name), sc));)*
                    }
                    let mut accums = BTreeMap::new();
                    match &stats.file_stats {
                        Ok(file_stats) => {
                            for file_stat in file_stats {
                                let accum: &mut FileStatsAccum = accums.entry(file_stat.size_class).or_default();
                                accum.add_assign(file_stat);
                            }
                        }
                        Err(err) => {
                            return Err(Error::new(ErrorKind::FileStatsFailed(err.to_string())));
                        }
                    }
                    for (size_class, accum) in accums {
                        let sc_stats = self.get_size_class(size_class);
                        $(sc_stats.$f_metric.set(u64::cast_from(accum.$f_name));)*
                    }
                    Ok(())
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

metrics_size_class! {
    mz_metrics_lgalloc
    @size_class (
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
        (clear_eager_total, "Number of thread regions in size class", clear_eager_total, id),
        (clear_eager_total, "Number of thread regions in size class", clear_eager_bytes_total, normalize_by_size_class),
        (clear_slow_total, "Number of thread regions in size class", clear_slow_total, id),
        (clear_slow_total, "Number of thread regions in size class", clear_slow_bytes_total, normalize_by_size_class)
    )
    @file (
        (file_size, file_size_bytes, "Sum of file sizes in size class"),
        (allocated_size, file_allocated_size_bytes, "Sum of allocated sizes in size class"),
        (mapped, vm_mapped_bytes, "Sum of mapped sizes in size class"),
        (active, vm_active_bytes, "Sum of active sizes in size class"),
        (dirty, vm_dirty_bytes, "Sum of dirty sizes in size class")
    )
}

/// Register a task to read lgalloc stats.
#[allow(clippy::unused_async)]
pub async fn register_metrics_into(metrics_registry: &MetricsRegistry) {
    let mut lgmetrics = LgMetrics::new(metrics_registry);

    mz_ore::task::spawn(|| "lgalloc_stats_update", async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(err) = lgmetrics.update() {
                error!("lgalloc stats update failed: {err}");
                break;
            }
        }
    });
}
