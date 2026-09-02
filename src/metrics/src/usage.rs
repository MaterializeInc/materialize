// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Resource usage observations for the current process.
//!
//! This module is a mechanism, not a policy. It reports what each source says, under that
//! source's own name, and never combines two sources into a third figure. The discrepancies
//! between sources carry information: cgroup memory far above `VmRSS` means page cache or kernel
//! memory is charged to the replica, which is exactly the case a single fused "memory" number
//! hides. Deciding which source answers "how much memory is this replica using" belongs to the
//! SQL views built on top.
//!
//! Peaks are observations too. `cgroup memory.peak` and `getrusage`'s `ru_maxrss` are high-water
//! marks the kernel maintains itself, so they carry no sampling error and outlive anything short
//! of the process exiting. Only a source with no kernel-side peak gets one folded here, reported
//! under a distinct metric name so a caller can tell an exact peak from a sampled one.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::Context;
use mz_ore::cast::CastFrom;
use mz_ore::cgroup::CgroupV2;
use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, raw};
use tracing::{debug, info};

use crate::MetricsUpdate;

/// Identifies one observation: which reader produced it, and what it measures.
///
/// Static strings rather than an enum, so adding a metric touches only the sampler.
pub type MetricKey = (&'static str, &'static str);

/// The most recent observations, keyed by [`MetricKey`].
///
/// Published as a whole map rather than as independent cells so a reader always sees one
/// self-consistent sample. Reading a set of independent atomics could mix two samples into a
/// combination that never existed, for instance a peak below the current value it bounds.
static OBSERVATIONS: Mutex<Option<BTreeMap<MetricKey, u64>>> = Mutex::new(None);

/// Source names, as reported alongside each observation.
pub mod source {
    /// This process's cgroup v2 interface files.
    pub const CGROUP: &str = "cgroup";
    /// `getrusage(RUSAGE_SELF)`.
    pub const RUSAGE: &str = "rusage";
    /// `/proc/self/status`.
    pub const PROC_STATUS: &str = "proc_status";
    /// `statvfs` on the scratch directory's filesystem.
    pub const STATVFS: &str = "statvfs";
}

/// Return the most recent resource usage observations of this process.
///
/// `None` until the sampler has taken its first sample, and forever in a process that never
/// registered one. An observation the sampler could not read is absent from the map, never
/// present as zero.
pub fn observations() -> Option<BTreeMap<MetricKey, u64>> {
    OBSERVATIONS.lock().expect("poisoned").clone()
}

/// Observations that get a peak folded in this process, because their source has no kernel-side
/// high-water mark. Each is a maximum over samples and therefore a lower bound on the true peak.
const DERIVED_PEAKS: &[(MetricKey, &str)] = &[
    ((source::STATVFS, "fs_used"), "fs_used_peak"),
    // Load-bearing only below the kernel version that provides `memory.swap.peak`.
    ((source::PROC_STATUS, "vm_swap"), "vm_swap_peak"),
    // A lower bound on the peak of the quantity the memory limiter enforces. No upper bound is
    // available: the cgroup peaks describe a smaller quantity, excluding resident file-backed
    // pages, and `ru_maxrss` is refreshed at kernel checkpoints and has been seen reading below
    // the concurrent `vm_rss`.
    ((source::PROC_STATUS, "heap"), "heap_peak"),
];

/// Sampler of resource usage observations, driven by the metrics update task.
///
/// Sampling happens here rather than in a compute logging operator because a saturated timely
/// worker stops scheduling its logging operators during exactly the episodes we most want
/// sampled. Folding the derived peaks here also keeps them alive across a teardown and rebuild of
/// the logging dataflow, which an operator-local fold would lose.
pub(crate) struct UsageMetrics {
    /// This process's cgroup, if it has a v2 one with the memory controller enabled.
    cgroup: Option<CgroupV2>,
    /// Directory whose filesystem usage is tracked, if disk is in use.
    disk_root: Option<PathBuf>,
    /// Peaks folded here, for the sources listed in [`DERIVED_PEAKS`].
    derived_peaks: BTreeMap<MetricKey, u64>,
    gauges: raw::UIntGaugeVec,
}

impl UsageMetrics {
    fn new(registry: &MetricsRegistry, disk_root: Option<PathBuf>) -> Self {
        // Readings taken from the wrong cgroup look plausible rather than absent, and diagnosing
        // that otherwise takes access to the container. Reported once, at registration.
        let cgroup = CgroupV2::detect();
        match &cgroup {
            Some(cgroup) => info!(
                dir = %cgroup.path().display(),
                "reading resource usage from cgroup v2",
            ),
            None => info!("no cgroup v2 with a memory controller; cgroup usage unavailable"),
        }

        Self {
            cgroup,
            disk_root,
            derived_peaks: BTreeMap::new(),
            gauges: registry.register(metric!(
                name: "mz_metrics_resource_usage",
                help: "Resource usage observations, by source and metric.",
                var_labels: ["source", "metric"],
            )),
        }
    }

    /// Read every source, without interpreting any of them.
    fn sample(&self) -> BTreeMap<MetricKey, u64> {
        let mut out = BTreeMap::new();
        let mut put = |source: &'static str, metric: &'static str, value: Option<u64>| {
            if let Some(value) = value {
                out.insert((source, metric), value);
            }
        };

        if let Some(cgroup) = &self.cgroup {
            // `memory.peak` and `memory.swap.peak` are absent on kernels too old to provide them
            // and read as `None` there. `memory.current` is the accounting that limit enforcement
            // and the OOM killer act on, which is why it is worth reporting next to `vm_rss`.
            let files: &[(&'static str, &str)] = &[
                ("memory_current", "memory.current"),
                ("memory_peak", "memory.peak"),
                ("memory_max", "memory.max"),
                ("swap_current", "memory.swap.current"),
                ("swap_peak", "memory.swap.peak"),
                ("swap_max", "memory.swap.max"),
            ];
            for &(metric, file) in files {
                put(source::CGROUP, metric, cgroup.read_u64(file));
            }

            // `oom_kill` counts kills inside this cgroup and `max` counts times the limit was
            // hit, which together answer whether a replica died of memory pressure.
            let keyed: &[(&'static str, &str, &str)] = &[
                ("anon", "memory.stat", "anon"),
                ("file", "memory.stat", "file"),
                ("shmem", "memory.stat", "shmem"),
                // Pages held in memory with their swap slot still allocated. They are charged
                // twice, once as `anon` here and once in `memory.swap.current`, and they are the
                // whole of the difference between that and `proc_status vm_swap`.
                ("swapcached", "memory.stat", "swapcached"),
                ("kernel", "memory.stat", "kernel"),
                ("slab", "memory.stat", "slab"),
                ("sock", "memory.stat", "sock"),
                ("events_max", "memory.events", "max"),
                ("events_oom_kill", "memory.events", "oom_kill"),
            ];
            for &(metric, file, key) in keyed {
                put(source::CGROUP, metric, cgroup.read_keyed_u64(file, key));
            }
        }

        put(source::RUSAGE, "max_rss", max_rss_bytes());

        match ProcStatus::from_proc() {
            Ok(status) => {
                put(source::PROC_STATUS, "vm_rss", Some(status.rss()));
                put(source::PROC_STATUS, "vm_swap", Some(status.swap()));
                // The quantity the memory limiter enforces against `--heap-limit`. Reported as
                // its own observation because the kernel maintains no combined memory-plus-swap
                // peak, so a peak of the sum has to be folded from samples of the sum. It is one
                // source added to itself, not two sources fused.
                put(source::PROC_STATUS, "heap", Some(status.heap()));
                // Decomposes `vm_rss`. `rss_file` is the part charged to another cgroup, so it
                // explains the gap between `vm_rss` and `cgroup memory_current`.
                put(source::PROC_STATUS, "rss_anon", Some(status.rss_anon()));
                put(source::PROC_STATUS, "rss_file", Some(status.rss_file()));
                put(source::PROC_STATUS, "rss_shmem", Some(status.rss_shmem()));
            }
            Err(err) => debug!("failed to read /proc/self/status: {err}"),
        }

        if let Some(root) = self.disk_root.as_deref() {
            // NOTE: filesystem-wide used bytes, not this process's usage. Named for what it is,
            // since on a shared filesystem it counts writes this replica never made.
            match disk_usage(root) {
                Ok(bytes) => put(source::STATVFS, "fs_used", Some(bytes)),
                Err(err) => debug!("statvfs on {} failed: {err}", root.display()),
            }
        }

        out
    }

    /// Fold the derived peaks over `sample`, adding them to it.
    fn fold_derived_peaks(&mut self, sample: &mut BTreeMap<MetricKey, u64>) {
        for ((source, metric), peak_metric) in DERIVED_PEAKS {
            let Some(&value) = sample.get(&(*source, *metric)) else {
                continue;
            };
            let peak = self
                .derived_peaks
                .entry((source, peak_metric))
                .and_modify(|peak| *peak = (*peak).max(value))
                .or_insert(value);
            sample.insert((source, peak_metric), *peak);
        }
    }
}

impl MetricsUpdate for UsageMetrics {
    type Error = Infallible;
    const NAME: &'static str = "usage";

    fn update(&mut self) -> Result<(), Self::Error> {
        let mut sample = self.sample();
        self.fold_derived_peaks(&mut sample);

        for ((source, metric), value) in &sample {
            self.gauges.with_label_values(&[source, metric]).set(*value);
        }

        *OBSERVATIONS.lock().expect("poisoned") = Some(sample);

        Ok(())
    }
}

/// Register the resource usage sampler.
///
/// `disk_root` is a directory on the filesystem whose usage should be tracked, or `None` if this
/// process does not use disk.
pub(crate) fn register_metrics_into(
    registry: &MetricsRegistry,
    disk_root: Option<PathBuf>,
) -> UsageMetrics {
    UsageMetrics::new(registry, disk_root)
}

/// Return the used bytes of the filesystem containing `root`.
///
/// Callers decide how to report a failure. The sampler polls this on a short interval, so logging
/// an error here would repeat for as long as the directory is unavailable.
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
///
/// The `rss_*` fields decompose `vm_rss`. The decomposition is load-bearing rather than
/// decorative: `rss_file` counts pages of file-backed mappings, most of it this binary's own text,
/// and those pages are charged to whichever cgroup first faulted them in. On a Kubernetes node
/// that is the runtime that unpacked the image, not the replica, so `vm_rss` runs a roughly
/// constant amount above the replica's own cgroup charge.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcStatus {
    /// Resident Set Size (RSS) in bytes.
    pub vm_rss: usize,
    /// Swap memory in bytes.
    pub vm_swap: usize,
    /// Resident anonymous memory in bytes.
    pub rss_anon: usize,
    /// Resident file-backed memory in bytes.
    pub rss_file: usize,
    /// Resident shared memory in bytes.
    pub rss_shmem: usize,
}

impl ProcStatus {
    /// Read a new `ProcStatus` from `/proc/self/status`.
    ///
    /// Fails on platforms without a Linux-style procfs.
    pub fn from_proc() -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string("/proc/self/status")?;
        let mut status = Self::default();

        for line in contents.lines() {
            let (field, target) = match line.split_once(':') {
                Some(("VmRSS", rest)) => ("VmRSS", (&mut status.vm_rss, rest)),
                Some(("VmSwap", rest)) => ("VmSwap", (&mut status.vm_swap, rest)),
                Some(("RssAnon", rest)) => ("RssAnon", (&mut status.rss_anon, rest)),
                Some(("RssFile", rest)) => ("RssFile", (&mut status.rss_file, rest)),
                Some(("RssShmem", rest)) => ("RssShmem", (&mut status.rss_shmem, rest)),
                _ => continue,
            };
            let (slot, rest) = target;
            *slot = parse_kib(rest).with_context(|| format!("failed to parse {field}"))?;
        }

        Ok(status)
    }

    /// Memory (RAM) usage, in bytes.
    pub fn rss(&self) -> u64 {
        u64::cast_from(self.vm_rss)
    }

    /// Swap usage, in bytes.
    pub fn swap(&self) -> u64 {
        u64::cast_from(self.vm_swap)
    }

    /// Heap (RAM + swap) usage, in bytes.
    pub fn heap(&self) -> u64 {
        self.rss().saturating_add(self.swap())
    }

    /// Resident anonymous memory, in bytes.
    pub fn rss_anon(&self) -> u64 {
        u64::cast_from(self.rss_anon)
    }

    /// Resident file-backed memory, in bytes.
    pub fn rss_file(&self) -> u64 {
        u64::cast_from(self.rss_file)
    }

    /// Resident shared memory, in bytes.
    pub fn rss_shmem(&self) -> u64 {
        u64::cast_from(self.rss_shmem)
    }
}

/// Parse the value part of a `/proc/self/status` line reporting a size in KiB, returning bytes.
fn parse_kib(rest: &str) -> anyhow::Result<usize> {
    let kib: usize = rest
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing value: {rest}"))?
        .parse()?;
    Ok(kib * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics_for_test() -> UsageMetrics {
        UsageMetrics::new(&MetricsRegistry::new(), None)
    }

    /// Something must be measurable on the platforms we test on, otherwise the other tests here
    /// assert nothing.
    #[mz_ore::test]
    fn sample_is_not_empty() {
        let metrics = metrics_for_test();
        assert!(!metrics.sample().is_empty());
    }

    /// A derived peak must rise with a higher observation and survive a lower one.
    ///
    /// Drives the fold over a synthetic sample rather than through `update`, so the assertion does
    /// not depend on this machine's disk usage actually moving.
    #[mz_ore::test]
    fn derived_peaks_are_monotonic() {
        let mut metrics = metrics_for_test();
        let key = (source::STATVFS, "fs_used");
        let peak_key = (source::STATVFS, "fs_used_peak");

        let mut fold = |value| {
            let mut sample = BTreeMap::from_iter([(key, value)]);
            metrics.fold_derived_peaks(&mut sample);
            sample[&peak_key]
        };

        assert_eq!(fold(100), 100, "first observation sets the peak");
        assert_eq!(fold(200), 200, "a higher observation raises the peak");
        assert_eq!(fold(50), 200, "a lower observation must not lower the peak");
    }

    /// `vm_rss` must decompose exactly into its three parts, since a caller comparing `rss_file`
    /// against a cgroup charge relies on the decomposition being complete.
    #[mz_ore::test]
    #[cfg_attr(not(target_os = "linux"), ignore = "requires a Linux procfs")]
    fn vm_rss_decomposes() {
        let status = ProcStatus::from_proc().expect("procfs available");
        assert_eq!(
            status.rss(),
            status.rss_anon() + status.rss_file() + status.rss_shmem(),
            "vm_rss {} != anon {} + file {} + shmem {}",
            status.rss(),
            status.rss_anon(),
            status.rss_file(),
            status.rss_shmem(),
        );
    }

    /// `heap` must be exactly the sum the memory limiter compares against its limit, since a
    /// caller reading `heap_peak` to ask how close a replica came to a kill relies on it.
    #[mz_ore::test]
    #[cfg_attr(not(target_os = "linux"), ignore = "requires a Linux procfs")]
    fn heap_is_rss_plus_swap() {
        let metrics = metrics_for_test();
        let sample = metrics.sample();

        let get = |metric| sample[&(source::PROC_STATUS, metric)];
        assert_eq!(get("heap"), get("vm_rss") + get("vm_swap"));
    }

    /// A source that cannot be read is absent, never zero.
    #[mz_ore::test]
    fn unmeasured_observation_is_absent() {
        let metrics = metrics_for_test();
        let sample = metrics.sample();

        // No `disk_root` was configured, so nothing from `statvfs` may appear.
        assert!(sample.keys().all(|(source, _)| *source != source::STATVFS));
    }

    /// A derived peak is only published for a source that was actually read.
    #[mz_ore::test]
    fn derived_peak_needs_an_observation() {
        let mut metrics = metrics_for_test();
        let mut sample = BTreeMap::new();
        metrics.fold_derived_peaks(&mut sample);
        assert!(sample.is_empty());
    }
}
