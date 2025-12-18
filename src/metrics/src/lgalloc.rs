// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Report lgalloc metrics.

use std::collections::BTreeMap;
use std::ops::AddAssign;

use lgalloc::{FileStats, MapStats};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::{MetricsRegistry, raw};
use paste::paste;
use prometheus::core::{AtomicU64, GenericGauge};

use crate::MetricsUpdate;

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
}

/// An accumulator for [`MapStats`].
#[derive(Default)]
struct MapStatsAccum {
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
    }
}

impl AddAssign<&MapStats> for MapStatsAccum {
    fn add_assign(&mut self, rhs: &MapStats) {
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
            pub(crate) struct LgMetrics {
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
                    for (size_class, sc) in &stats.size_class {
                        let sc_stats = self.get_size_class(*size_class);
                        $(sc_stats.$metric.set(($conv)(u64::cast_from(sc.$name), *size_class));)*
                    }
                    let mut f_accums = BTreeMap::new();
                    for (size_class, file_stat) in &stats.file {
                        let accum: &mut FileStatsAccum = f_accums.entry(*size_class).or_default();
                        match file_stat {
                            Ok(file_stat) => {
                                accum.add_assign(file_stat);
                            }
                            Err(err) => {
                                return Err(ErrorKind::FileStatsFailed(err.to_string()).into());
                            }
                        }
                    }
                    for (size_class, f_accum) in f_accums {
                        let sc_stats = self.get_size_class(size_class);
                        $(sc_stats.$f_metric.set(u64::cast_from(f_accum.$f_name));)*
                    }
                    Ok(())
                }
            }
        }
    };
}

macro_rules! map_metrics {
    ($namespace:ident
        @mem ($(($m_name:ident, $m_metric:ident, $m_desc:expr)),*)
    ) => {
        map_metrics! {
            @define $namespace
            @mem $(($m_name, $m_metric, $m_desc)),*
        }
    };
    (@define $namespace:ident
        @mem $(($m_name:ident, $m_metric:ident, $m_desc:expr)),*
    ) => {
        paste! {
            pub(crate) struct LgMapMetrics {
                size_class: BTreeMap<usize, LgMapMetricsSC>,
                $($m_metric: raw::UIntGaugeVec,)*
            }
            struct LgMapMetricsSC {
                $($m_metric: GenericGauge<AtomicU64>,)*
            }
            impl LgMapMetrics {
                fn new(registry: &MetricsRegistry) -> Self {
                    Self {
                        size_class: BTreeMap::default(),
                        $($m_metric: registry.register(mz_ore::metric!(
                            name: concat!(stringify!($namespace), "_", stringify!($m_metric)),
                            help: $m_desc,
                            var_labels: ["size_class"],
                        )),)*
                    }
                }
                fn get_size_class(&mut self, size_class: usize) -> &LgMapMetricsSC {
                    self.size_class.entry(size_class).or_insert_with(|| {
                        let labels: &[&str] = &[&size_class.to_string()];
                        LgMapMetricsSC {
                            $($m_metric: self.$m_metric.with_label_values(labels),)*
                        }
                    })
                }
                fn update(&mut self) -> std::io::Result<()> {
                    #[cfg(target_os = "linux")]
                    let stats = lgalloc::lgalloc_stats_with_mapping()?;
                    // We don't have `numa_maps` on non-Linux platforms, so we use the regular
                    // stats instead. It won't extract anything, but avoids unused variable
                    // warnings.
                    #[cfg(not(target_os = "linux"))]
                    let stats = lgalloc::lgalloc_stats();
                    let mut m_accums = BTreeMap::new();
                    for (size_class, map_stat) in stats.map.iter().flatten() {
                        let accum: &mut MapStatsAccum = m_accums.entry(*size_class).or_default();
                        accum.add_assign(map_stat);
                    }
                    for (size_class, m_accum) in m_accums {
                        let sc_stats = self.get_size_class(size_class);
                        $(sc_stats.$m_metric.set(u64::cast_from(m_accum.$m_name));)*
                    }
                    Ok(())
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
        (allocated_size, file_allocated_size_bytes, "Sum of allocated sizes in size class")
    )
}

map_metrics! {
    mz_metrics_lgalloc
    @mem (
        (mapped, vm_mapped_bytes, "Sum of mapped sizes in size class"),
        (active, vm_active_bytes, "Sum of active sizes in size class"),
        (dirty, vm_dirty_bytes, "Sum of dirty sizes in size class")
    )
}

/// Register a task to read lgalloc stats.
pub(crate) fn register_metrics_into(metrics_registry: &MetricsRegistry) -> LgMetrics {
    LgMetrics::new(metrics_registry)
}

/// Register a task to read mapping-related lgalloc stats.
pub(crate) fn register_map_metrics_into(metrics_registry: &MetricsRegistry) -> LgMapMetrics {
    LgMapMetrics::new(metrics_registry)
}

impl MetricsUpdate for LgMetrics {
    type Error = Error;
    const NAME: &'static str = "lgalloc";
    fn update(&mut self) -> Result<(), Self::Error> {
        self.update()
    }
}

impl MetricsUpdate for LgMapMetrics {
    type Error = std::io::Error;
    const NAME: &'static str = "lgalloc_map";
    fn update(&mut self) -> Result<(), Self::Error> {
        self.update()
    }
}
