// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the rust-prometheus project.
// The original source code was retrieved on November 10th, 2020 from:
//
//     https://github.com/tikv/rust-prometheus/blob/200c362e5e58442230334d6126817fcceed47c25/src/process_collector.rs
//
// The original source code is licensed under the Apache 2.0 license, a copy of
// which can be found in the LICENSE file at the root of this repository.

//! Monitor a process.
//!
//! This module only supports **Linux** platform.

use std::collections::HashMap;
use std::sync::Mutex;

use lazy_static::lazy_static;

use prometheus::core::{Collector, Desc};
use prometheus::proto;
use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Opts};

/// Six metrics per ProcessCollector.
const METRICS_NUMBER: usize = 9;
/// The `pid_t` data type represents process IDs.
use libc::pid_t;

/// A collector which exports the current state of
/// process metrics including cpu, memory and file descriptor usage as well as
/// the process start time for the given process id.
#[derive(Debug)]
pub struct ProcessCollector {
    pid: pid_t,
    descs: Vec<Desc>,
    cpu_total: Mutex<Counter>,
    open_fds: Gauge,
    max_fds: Gauge,
    vsize: Gauge,
    rss: Gauge,
    swap: Gauge,
    threads: ThreadsCollector,
    start_time: Gauge,
    system_swap: Gauge,
    system_swap_free: Gauge,
}

impl ProcessCollector {
    /// Create a `ProcessCollector` with the given process id and namespace.
    pub fn new<S: Into<String>>(pid: pid_t, namespace: S) -> ProcessCollector {
        let namespace = namespace.into();
        let mut descs = Vec::new();

        let cpu_total = Counter::with_opts(
            Opts::new(
                "process_cpu_seconds_total",
                "Total user and system CPU time spent in \
                 seconds.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(cpu_total.desc().into_iter().cloned());

        let open_fds = Gauge::with_opts(
            Opts::new("process_open_fds", "Number of open file descriptors.")
                .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(open_fds.desc().into_iter().cloned());

        let max_fds = Gauge::with_opts(
            Opts::new(
                "process_max_fds",
                "Maximum number of open file descriptors.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(max_fds.desc().into_iter().cloned());

        let vsize = Gauge::with_opts(
            Opts::new(
                "process_virtual_memory_bytes",
                "Virtual memory size in bytes.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(vsize.desc().into_iter().cloned());

        let rss = Gauge::with_opts(
            Opts::new(
                "process_resident_memory_bytes",
                "Resident memory size in bytes.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(rss.desc().into_iter().cloned());

        let swap = Gauge::with_opts(
            Opts::new(
                "process_swap_memory_bytes",
                "Swapped out memory size in bytes.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(swap.desc().into_iter().cloned());

        let start_time = Gauge::with_opts(
            Opts::new(
                "process_start_time_seconds",
                "Start time of the process since unix epoch \
                 in seconds.",
            )
            .namespace(namespace),
        )
        .unwrap();
        descs.extend(start_time.desc().into_iter().cloned());

        let system_swap = Gauge::with_opts(Opts::new(
            "system_swap_memory_bytes",
            "Total amount of swap configured on the system",
        ))
        .unwrap();
        descs.extend(system_swap.desc().into_iter().cloned());
        let system_swap_free = Gauge::with_opts(Opts::new(
            "system_swap_memory_free_bytes",
            "Amount of swap available for use on the system",
        ))
        .unwrap();
        descs.extend(system_swap_free.desc().into_iter().cloned());

        let threads = ThreadsCollector::new(pid);
        descs.extend(threads.desc().into_iter().cloned());

        ProcessCollector {
            pid,
            descs,
            cpu_total: Mutex::new(cpu_total),
            open_fds,
            max_fds,
            vsize,
            rss,
            swap,
            threads,
            start_time,
            system_swap,
            system_swap_free,
        }
    }

    /// Return a `ProcessCollector` of the calling process.
    pub fn for_self() -> ProcessCollector {
        let pid = unsafe { libc::getpid() };
        ProcessCollector::new(pid, "")
    }
}

impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let p = match procfs::process::Process::new(self.pid) {
            Ok(p) => p,
            Err(..) => {
                // we can't construct a Process object, so there's no stats to gather
                return Vec::new();
            }
        };

        // file descriptors
        if let Ok(fd_list) = p.fd() {
            self.open_fds.set(fd_list.len() as f64);
        }
        if let Ok(limits) = p.limits() {
            if let procfs::process::LimitValue::Value(max) = limits.max_open_files.soft_limit {
                self.max_fds.set(max as f64)
            }
        }

        // memory
        self.vsize.set(p.stat.vsize as f64);
        self.rss.set(p.stat.rss as f64 * *PAGESIZE);
        if let Ok(status) = p.status() {
            if let Some(swap) = status.vmswap {
                self.swap.set(swap as f64);
            }
        }

        // swap
        if let Ok(status) = p.status() {
            if let Some(vmswap_kib) = status.vmswap {
                let vmswap = vmswap_kib * 1024;
                self.swap.set(vmswap as f64);
            }
        }

        if let Ok(s) = procfs::Meminfo::new() {
            self.system_swap.set(s.swap_total as f64);
            self.system_swap_free.set(s.swap_free as f64);
        }

        // proc_start_time
        if let Some(boot_time) = *BOOT_TIME {
            self.start_time
                .set(p.stat.starttime as f64 / *CLK_TCK + boot_time);
        }

        // cpu
        let cpu_total_mfs = {
            let cpu_total = self.cpu_total.lock().unwrap();
            let delta = collect_cpu_stat(&cpu_total, &p.stat);
            if delta > 0.0 {
                cpu_total.inc_by(delta);
            }
            cpu_total.collect()
        };

        // collect MetricFamilys.
        let threads = self.threads.collect();
        let mut mfs = Vec::with_capacity(METRICS_NUMBER + threads.len());
        mfs.extend(cpu_total_mfs);
        mfs.extend(self.open_fds.collect());
        mfs.extend(self.max_fds.collect());
        mfs.extend(self.vsize.collect());
        mfs.extend(self.rss.collect());
        mfs.extend(self.swap.collect());
        mfs.extend(self.start_time.collect());
        mfs.extend(self.system_swap.collect());
        mfs.extend(self.system_swap_free.collect());
        mfs.extend(threads);
        mfs
    }
}

#[derive(Debug)]
struct ThreadsCollector {
    inner: Mutex<TcInner>,

    descs: Vec<Desc>,
}

#[derive(Debug)]
struct TcInner {
    known_threads: HashMap<String, ThreadStats>,
    pid: pid_t,

    total_cpu_vec: CounterVec,
    thread_count_vec: GaugeVec,
}

impl ThreadsCollector {
    fn new(pid: pid_t) -> ThreadsCollector {
        let known_threads = HashMap::new();
        let total_cpu_vec = CounterVec::new(
            Opts::new(
                "process_thread_cpu_seconds_total",
                "The total time spent by all threads with this name.",
            ),
            &["thread_name"],
        )
        .unwrap();
        let thread_count_vec = GaugeVec::new(
            Opts::new("process_thread_count", "Number of threads with this name"),
            &["thread_name"],
        )
        .unwrap();

        let mut descs = Vec::new();
        descs.extend(thread_count_vec.desc().into_iter().cloned());
        descs.extend(total_cpu_vec.desc().into_iter().cloned());

        ThreadsCollector {
            inner: Mutex::new(TcInner {
                known_threads,
                pid,
                total_cpu_vec,
                thread_count_vec,
            }),
            descs,
        }
    }
}

impl Collector for ThreadsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut tc = self.inner.lock().unwrap();
        let p = match procfs::process::Process::new(tc.pid) {
            Ok(p) => p,
            Err(..) => {
                // we can't construct a Process object, so there's no stats to gather
                return Vec::new();
            }
        };

        if let Ok(threads) = p.tasks() {
            // Thread errors can happen when the thread is completed while we're inspecting
            for thread in threads.flat_map(|t_result| t_result) {
                let name = match thread.stat() {
                    Ok(stat) => stat.comm,
                    Err(_) => continue,
                };
                let stats = match tc.known_threads.get_mut(&name) {
                    Some(thread) => thread,
                    None => {
                        let total_cpu = tc.total_cpu_vec.with_label_values(&[&name]);
                        let thread_count = tc.thread_count_vec.with_label_values(&[&name]);
                        tc.known_threads
                            // use entry instead of insert to get a reference to
                            // the newly inserted stats
                            .entry(name.clone())
                            .or_insert_with(|| ThreadStats::new(total_cpu, thread_count))
                    }
                };
                stats.update(&thread);
            }
        }

        let mut mfs = Vec::new();
        for (_, ts) in tc.known_threads.iter_mut() {
            ts.finish(&mut mfs);
        }
        mfs
    }
}

#[derive(Debug)]
struct ThreadStats {
    total_cpu: Counter,
    count: Gauge,
    local_total: u64,
    local_count: f64,
}

impl ThreadStats {
    fn new(total_cpu: Counter, count: Gauge) -> ThreadStats {
        ThreadStats {
            total_cpu,
            count,
            local_total: 0,
            local_count: 0.0,
        }
    }

    fn update(&mut self, thread: &procfs::process::Task) {
        self.local_count += 1.0;
        if let Ok(stat) = thread.stat() {
            self.local_total += stat.utime + stat.stime;
        }
    }

    /// Extend metric families with data for threads that were found on this last iteration
    fn finish(&mut self, metric_families: &mut Vec<proto::MetricFamily>) {
        if self.local_count != 0.0 {
            self.count.set(self.local_count);
            let past = self.total_cpu.get();
            let total = self.local_total as f64 / *CLK_TCK;
            let delta = total - past;
            if delta > 0.0 {
                self.total_cpu.inc_by(delta);
            }
            self.local_count = 0.0;
            self.local_total = 0;

            metric_families.extend(self.count.collect());
            metric_families.extend(self.total_cpu.collect());
        }
    }
}

fn collect_cpu_stat(cpu_total: &Counter, stat: &procfs::process::Stat) -> f64 {
    let total = (stat.utime + stat.stime) as f64 / *CLK_TCK;
    let past = cpu_total.get();
    total - past
}

lazy_static! {
    // getconf CLK_TCK
    static ref CLK_TCK: f64 = {
        unsafe {
            libc::sysconf(libc::_SC_CLK_TCK) as f64
        }
    };

    // getconf PAGESIZE
    static ref PAGESIZE: f64 = {
        unsafe {
            libc::sysconf(libc::_SC_PAGESIZE) as f64
        }
    };
}

lazy_static! {
    static ref BOOT_TIME: Option<f64> = procfs::boot_time_secs().ok().map(|i| i as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_process_collector() {
        let pc = ProcessCollector::for_self();
        {
            // Ensure that we have at least the right number of metrics
            let descs = pc.desc();
            assert!(
                descs.len() >= super::METRICS_NUMBER,
                "{} >= {}",
                descs.len(),
                super::METRICS_NUMBER,
            );
            let mfs = pc.collect();
            assert!(
                mfs.len() >= super::METRICS_NUMBER,
                "{} >= {}",
                mfs.len(),
                super::METRICS_NUMBER,
            );
        }

        let r = Registry::new();
        let res = r.register(Box::new(pc));
        assert!(res.is_ok());
        r.gather();
    }
}
