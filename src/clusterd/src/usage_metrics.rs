// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for collecting system usage metrics.
//!
//! Currently only disk and swap usage is supported.
//! We may want to add CPU and memory usage in the future.

use std::path::PathBuf;

use serde::Serialize;
use tracing::{debug, error};

/// A system usage metrics collector.
pub(crate) struct Collector {
    pub disk_root: Option<PathBuf>,
}

impl Collector {
    /// Collect current system usage metrics.
    pub fn collect(&self) -> Usage {
        let disk_bytes = self.collect_disk_usage();
        let (memory_bytes, swap_bytes) = collect_heap_usage();
        let heap_limit = collect_heap_limit();

        Usage {
            disk_bytes,
            memory_bytes,
            swap_bytes,
            heap_limit,
        }
    }

    fn collect_disk_usage(&self) -> Option<u64> {
        let Some(root) = &self.disk_root else {
            return None;
        };

        let stat = match nix::sys::statvfs::statvfs(root) {
            Ok(stat) => stat,
            Err(err) => {
                error!("statvfs error: {err}");
                return None;
            }
        };

        // `fsblkcnt_t` is a `u32` on macOS but a `u64` on Linux.
        #[allow(clippy::useless_conversion)]
        let used_blocks = u64::from(stat.blocks() - stat.blocks_available());
        let used_bytes = used_blocks * stat.fragment_size();

        debug!("disk usage: {used_bytes}");

        Some(used_bytes)
    }
}

/// A system usage measurement.
#[derive(Serialize)]
pub(crate) struct Usage {
    disk_bytes: Option<u64>,
    memory_bytes: Option<u64>,
    swap_bytes: Option<u64>,
    heap_limit: Option<u64>,
}

#[cfg(target_os = "linux")]
mod linux {
    use std::fs;
    use std::path::Path;

    use anyhow::{anyhow, bail};
    use mz_compute::memory_limiter;
    use mz_ore::cast::CastInto;
    use tracing::{debug, error};

    /// Collect memory and swap usage.
    pub fn collect_heap_usage() -> (Option<u64>, Option<u64>) {
        use mz_ore::cast::CastInto;

        match memory_limiter::ProcStatus::from_proc() {
            Ok(status) => {
                let memory_bytes = status.vm_rss.cast_into();
                let swap_bytes = status.vm_swap.cast_into();

                debug!("memory usage: {memory_bytes}");
                debug!("swap usage: {swap_bytes}");

                (Some(memory_bytes), Some(swap_bytes))
            }
            Err(err) => {
                error!("error reading /proc/self/status: {err}");
                (None, None)
            }
        }
    }

    /// Collect the heap limit, i.e. memory + swap limit.
    pub fn collect_heap_limit() -> Option<u64> {
        // If we don't know the physical limits, we can't know the heap limit.
        let (phys_mem_limit, phys_swap_limit) = get_physical_limits()?;

        // Limits might be reduced by the cgroup.
        let (cgroup_mem_limit, cgroup_swap_limit) = get_cgroup_limits();
        let mem_limit = cgroup_mem_limit.unwrap_or(u64::MAX).min(phys_mem_limit);
        let swap_limit = cgroup_swap_limit.unwrap_or(u64::MAX).min(phys_swap_limit);

        let heap_limit = mem_limit + swap_limit;

        // Heap limit might be reduced by the memory limiter.
        let limiter_limit = memory_limiter::get_memory_limit().map(CastInto::cast_into);
        let heap_limit = limiter_limit.unwrap_or(u64::MAX).min(heap_limit);

        debug!("memory limit: {mem_limit} (phys={phys_mem_limit}, cgroup={cgroup_mem_limit:?})");
        debug!("swap limit: {swap_limit} (phys={phys_swap_limit}, cgroup={cgroup_swap_limit:?})");
        debug!("heap limit: {heap_limit} (limiter={limiter_limit:?})");

        Some(heap_limit)
    }

    /// Helper for parsing `/proc/meminfo`.
    struct ProcMemInfo {
        mem_total: u64,
        swap_total: u64,
    }

    impl ProcMemInfo {
        fn from_proc() -> anyhow::Result<Self> {
            let contents = fs::read_to_string("/proc/meminfo")?;

            fn parse_kib_line(line: &str) -> anyhow::Result<u64> {
                if let Some(kib) = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|x| x.parse::<u64>().ok())
                {
                    Ok(kib * 1024)
                } else {
                    bail!("invalid meminfo line: {line}");
                }
            }

            let mut memory = None;
            let mut swap = None;
            for line in contents.lines() {
                if line.starts_with("MemTotal:") {
                    memory = Some(parse_kib_line(line)?);
                } else if line.starts_with("SwapTotal:") {
                    swap = Some(parse_kib_line(line)?);
                }
            }

            let mem_total = memory.ok_or_else(|| anyhow!("MemTotal not found"))?;
            let swap_total = swap.ok_or_else(|| anyhow!("SwapTotal not found"))?;

            Ok(Self {
                mem_total,
                swap_total,
            })
        }
    }

    /// Collect the physical memory and swap limits.
    fn get_physical_limits() -> Option<(u64, u64)> {
        let meminfo = match ProcMemInfo::from_proc() {
            Ok(meminfo) => meminfo,
            Err(error) => {
                error!("reading `/proc/meminfo`: {error}");
                return None;
            }
        };

        Some((meminfo.mem_total, meminfo.swap_total))
    }

    /// Collect the memory and swap limits enforced by the current cgroup.
    ///
    /// We make the following simplifying assumptions that hold for a standard Kubernetes
    /// environment:
    //  * The current process is a member of exactly one cgroups v2 hierarchy.
    //  * The cgroups hierarchy is mounted at `/sys/fs/cgroup`.
    //  * The limits are applied to the current cgroup directly (and not one of its ancestors).
    fn get_cgroup_limits() -> (Option<u64>, Option<u64>) {
        let Ok(proc_cgroup) = fs::read_to_string("/proc/self/cgroup") else {
            return (None, None);
        };
        let Some(cgroup_path) = proc_cgroup.split(':').nth(2) else {
            error!("invalid `/proc/self/cgroup` format: {proc_cgroup}");
            return (None, None);
        };

        let root = Path::new("/sys/fs/cgroup").join(cgroup_path);
        let memory_file = root.join("memory.max");
        let swap_file = root.join("memory.swap.max");

        let memory = fs::read_to_string(memory_file)
            .ok()
            .and_then(|s| s.parse().ok());
        let swap = fs::read_to_string(swap_file)
            .ok()
            .and_then(|s| s.parse().ok());

        (memory, swap)
    }
}

#[cfg(not(target_os = "linux"))]
mod macos {
    use mz_compute::memory_limiter;
    use mz_ore::cast::CastInto;

    pub fn collect_heap_usage() -> (Option<u64>, Option<u64>) {
        (None, None)
    }

    pub fn collect_heap_limit() -> Option<u64> {
        memory_limiter::get_memory_limit().map(CastInto::cast_into)
    }
}

#[cfg(target_os = "linux")]
use linux::*;
#[cfg(not(target_os = "linux"))]
use macos::*;
