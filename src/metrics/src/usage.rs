// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Process resource usage: instantaneous readings and peak tracking.
//!
//! "Memory" here means resident set size and "heap" means resident set size plus swap, matching
//! the vocabulary of `mz_cluster_replica_metrics`. The memory limiter kills on the heap figure.

use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, UIntGauge};
use tracing::debug;

use crate::MetricsUpdate;

/// Sentinel published for a peak that has never been measured.
///
/// A process cannot use `u64::MAX` bytes of anything, so the sentinel cannot collide with a real
/// measurement.
const UNKNOWN: u64 = u64::MAX;

static PEAK_MEMORY_BYTES: AtomicU64 = AtomicU64::new(UNKNOWN);
static PEAK_HEAP_BYTES: AtomicU64 = AtomicU64::new(UNKNOWN);
static PEAK_DISK_BYTES: AtomicU64 = AtomicU64::new(UNKNOWN);

/// Peak resource usage of the current process, in bytes.
///
/// Each field is a high-water mark measured since process start. Peaks never decrease and are
/// never reset. A peak that resets is not composable: a consumer reading it at some later point
/// cannot tell which window the value covers, and two consumers reading at different times
/// disagree about the same episode.
///
/// A `None` field means the value cannot be measured in this configuration, not that usage was
/// zero. Disk is only measured when the process was given a scratch directory.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PeakUsage {
    /// Peak resident set size.
    ///
    /// Exact: the kernel maintains this high-water mark itself, so no spike can pass between two
    /// samples unseen.
    pub memory_bytes: Option<u64>,
    /// Peak resident set size plus swap.
    ///
    /// A lower bound, being a maximum over samples: a spike shorter than the sampling interval
    /// can be missed. Always at least `memory_bytes`.
    pub heap_bytes: Option<u64>,
    /// Peak disk usage of the scratch directory's filesystem.
    ///
    /// A lower bound, for the same reason as `heap_bytes`.
    pub disk_bytes: Option<u64>,
}

/// Return the current [`PeakUsage`] of this process.
///
/// Returns all-`None` until the peak tracker has taken its first sample, and forever in a process
/// that never registered one.
pub fn peak_usage() -> PeakUsage {
    fn load(cell: &AtomicU64) -> Option<u64> {
        match cell.load(Ordering::Relaxed) {
            UNKNOWN => None,
            value => Some(value),
        }
    }

    PeakUsage {
        memory_bytes: load(&PEAK_MEMORY_BYTES),
        heap_bytes: load(&PEAK_HEAP_BYTES),
        disk_bytes: load(&PEAK_DISK_BYTES),
    }
}

/// Tracker of [`PeakUsage`], driven by the metrics update task.
///
/// Peaks are folded in this struct, which the update task owns exclusively, and published to
/// process-global state. Sampling deliberately happens here rather than in a compute logging
/// operator: a saturated timely worker stops scheduling its logging operators exactly during the
/// episodes whose peak we want to catch.
///
/// The memory peak comes from `getrusage`'s `ru_maxrss` and so carries no sampling error. The
/// heap and disk peaks have no kernel-side equivalent and are maxima over samples.
pub(crate) struct PeakUsageMetrics {
    /// Directory whose filesystem usage is tracked, if disk is in use.
    disk_root: Option<PathBuf>,
    /// The peaks observed so far.
    peaks: PeakUsage,
    memory: UIntGauge,
    heap: UIntGauge,
    disk: UIntGauge,
}

impl PeakUsageMetrics {
    fn new(registry: &MetricsRegistry, disk_root: Option<PathBuf>) -> Self {
        Self {
            disk_root,
            peaks: PeakUsage::default(),
            memory: registry.register(metric!(
                name: "mz_metrics_peak_memory_bytes",
                help: "Peak memory (RAM) usage since process start.",
            )),
            heap: registry.register(metric!(
                name: "mz_metrics_peak_heap_bytes",
                help: "Peak heap (RAM + swap) usage since process start.",
            )),
            disk: registry.register(metric!(
                name: "mz_metrics_peak_disk_bytes",
                help: "Peak disk usage since process start.",
            )),
        }
    }
}

impl MetricsUpdate for PeakUsageMetrics {
    type Error = Infallible;
    const NAME: &'static str = "peak_usage";

    fn update(&mut self) -> Result<(), Self::Error> {
        /// Fold a new observation into a peak.
        fn fold(peak: &mut Option<u64>, observation: Option<u64>) {
            if let Some(observation) = observation {
                *peak = Some(peak.map_or(observation, |peak| peak.max(observation)));
            }
        }

        let status = match ProcStatus::from_proc() {
            Ok(status) => Some(status),
            Err(err) => {
                debug!("failed to read /proc/self/status: {err}");
                None
            }
        };

        fold(&mut self.peaks.memory_bytes, max_rss_bytes());
        fold(
            &mut self.peaks.memory_bytes,
            status.as_ref().map(|s| s.rss()),
        );
        fold(
            &mut self.peaks.heap_bytes,
            status.as_ref().map(|s| s.heap()),
        );
        // Peak heap is at least peak memory. Folding the memory peak in preserves that ordering
        // when `ru_maxrss` catches a spike that sampling missed.
        fold(&mut self.peaks.heap_bytes, self.peaks.memory_bytes);
        let disk = self
            .disk_root
            .as_deref()
            .and_then(|root| match disk_usage(root) {
                Ok(bytes) => Some(bytes),
                Err(err) => {
                    debug!("statvfs on {} failed: {err}", root.display());
                    None
                }
            });
        fold(&mut self.peaks.disk_bytes, disk);

        for (gauge, peak) in [
            (&self.memory, self.peaks.memory_bytes),
            (&self.heap, self.peaks.heap_bytes),
            (&self.disk, self.peaks.disk_bytes),
        ] {
            if let Some(peak) = peak {
                gauge.set(peak);
            }
        }

        PEAK_MEMORY_BYTES.store(
            self.peaks.memory_bytes.unwrap_or(UNKNOWN),
            Ordering::Relaxed,
        );
        PEAK_HEAP_BYTES.store(self.peaks.heap_bytes.unwrap_or(UNKNOWN), Ordering::Relaxed);
        PEAK_DISK_BYTES.store(self.peaks.disk_bytes.unwrap_or(UNKNOWN), Ordering::Relaxed);

        Ok(())
    }
}

/// Register the peak usage tracker.
///
/// `disk_root` is a directory on the filesystem whose usage should be tracked, or `None` if this
/// process does not use disk.
pub(crate) fn register_metrics_into(
    registry: &MetricsRegistry,
    disk_root: Option<PathBuf>,
) -> PeakUsageMetrics {
    PeakUsageMetrics::new(registry, disk_root)
}

/// Return the used bytes of the filesystem containing `root`.
///
/// Callers decide how to report a failure. The peak tracker polls this on a short interval, so
/// logging an error here would repeat for as long as the directory is unavailable.
pub fn disk_usage(root: &Path) -> Result<u64, nix::Error> {
    let stat = nix::sys::statvfs::statvfs(root)?;

    // `fsblkcnt_t` is a `u32` on macOS but a `u64` on Linux.
    #[allow(clippy::useless_conversion)]
    let used_blocks = u64::from(stat.blocks() - stat.blocks_available());
    let used_bytes = used_blocks * stat.fragment_size();

    debug!("disk usage: {used_bytes}");

    Ok(used_bytes)
}

/// Return this process's peak resident set size, in bytes.
///
/// This is the kernel's own high-water mark, so unlike a sampled maximum it cannot miss a
/// short-lived spike.
fn max_rss_bytes() -> Option<u64> {
    match crate::rusage::max_rss_bytes() {
        Ok(bytes) => u64::try_from(bytes).ok(),
        Err(err) => {
            debug!("getrusage failed: {err}");
            None
        }
    }
}

/// Memory usage of the current process, read from `/proc/self/status`.
#[derive(Clone, Copy, Debug)]
pub struct ProcStatus {
    /// Resident Set Size (RSS) in bytes.
    pub vm_rss: usize,
    /// Swap memory in bytes.
    pub vm_swap: usize,
}

impl ProcStatus {
    /// Read a new `ProcStatus` from `/proc/self/status`.
    ///
    /// Fails on platforms without a Linux-style procfs.
    pub fn from_proc() -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string("/proc/self/status")?;
        let mut vm_rss = 0;
        let mut vm_swap = 0;

        for line in contents.lines() {
            if line.starts_with("VmRSS:") {
                vm_rss = parse_kib_line(line).context("failed to parse VmRSS")?;
            } else if line.starts_with("VmSwap:") {
                vm_swap = parse_kib_line(line).context("failed to parse VmSwap")?;
            }
        }

        Ok(Self { vm_rss, vm_swap })
    }

    /// Memory (RAM) usage, in bytes.
    pub fn rss(&self) -> u64 {
        u64::cast_from(self.vm_rss)
    }

    /// Heap (RAM + swap) usage, in bytes.
    pub fn heap(&self) -> u64 {
        self.rss().saturating_add(u64::cast_from(self.vm_swap))
    }
}

/// Parse the value of a `/proc/self/status` line reporting a size in KiB, returning bytes.
fn parse_kib_line(line: &str) -> anyhow::Result<usize> {
    let kib: usize = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("missing value: {line}"))?
        .parse()?;
    Ok(kib * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics_for_test() -> PeakUsageMetrics {
        PeakUsageMetrics::new(&MetricsRegistry::new(), None)
    }

    /// Peaks must only ever grow, and must survive an observation that drops back down.
    #[mz_ore::test]
    fn peaks_are_monotonic() {
        let mut metrics = metrics_for_test();

        metrics.update().unwrap();
        let first = metrics.peaks;
        // Something must be measurable on the platforms we run tests on, otherwise the rest of
        // this test asserts nothing.
        assert!(first.memory_bytes.is_some() || first.heap_bytes.is_some());

        // Simulate a spike, then a drop back to a lower reading.
        metrics.peaks.memory_bytes = Some(u64::MAX / 4);
        metrics.peaks.heap_bytes = Some(u64::MAX / 4);
        let spike = metrics.peaks;
        metrics.update().unwrap();

        assert_eq!(
            metrics.peaks, spike,
            "a lower reading must not lower a peak"
        );
    }

    /// The heap peak includes the memory peak, even when the memory peak comes from `ru_maxrss`
    /// and sampling never observed a heap reading that large.
    #[mz_ore::test]
    fn heap_peak_covers_memory_peak() {
        let mut metrics = metrics_for_test();
        metrics.update().unwrap();

        let PeakUsage {
            memory_bytes,
            heap_bytes,
            ..
        } = metrics.peaks;
        if let (Some(memory), Some(heap)) = (memory_bytes, heap_bytes) {
            assert!(
                heap >= memory,
                "heap peak {heap} below memory peak {memory}"
            );
        }
    }

    /// Disk is not measured without a scratch directory, and an unmeasured peak reads as unknown
    /// rather than as zero.
    ///
    /// NOTE: reads the process-global peaks, which every test in this module publishes to. This
    /// stays deterministic only because no test here configures a `disk_root`.
    #[mz_ore::test]
    fn unmeasured_peak_is_unknown() {
        let mut metrics = metrics_for_test();
        metrics.update().unwrap();

        assert_eq!(metrics.peaks.disk_bytes, None);
        assert_eq!(peak_usage().disk_bytes, None);
    }
}
