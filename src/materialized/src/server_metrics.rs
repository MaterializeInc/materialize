// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tools for interacting with Prometheus metrics

use std::time::Instant;

use sysinfo::{ProcessorExt, SystemExt};

use mz_ore::cast::CastFrom;
use mz_ore::cgroup::{self, MemoryLimit};
use mz_ore::metric;
use mz_ore::metrics::{ComputedGauge, MetricsRegistry, UIntGauge};
use mz_ore::option::OptionExt;

use crate::BUILD_INFO;

/// Global metrics for the materialized server.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// The number of workers active in the system.
    pub worker_count: UIntGauge,
    /// The number of seconds that the system has been running.
    pub uptime: ComputedGauge,
}

impl Metrics {
    pub fn register_with(
        registry: &mut MetricsRegistry,
        workers: usize,
        start_instant: Instant,
    ) -> Self {
        let worker_count: UIntGauge = registry.register(metric!(
            name: "mz_server_metadata_timely_worker_threads",
            help: "number of timely worker threads",
            const_labels: {"count" => workers},
        ));
        worker_count.set(u64::cast_from(workers));

        let uptime = {
            let mut system = sysinfo::System::new();
            system.refresh_system();

            let memory_limit = cgroup::detect_memory_limit().unwrap_or(MemoryLimit {
                max: None,
                swap_max: None,
            });

            registry.register_computed_gauge(
                metric!(
                    name: "mz_server_metadata_seconds",
                    help: "server metadata, value is uptime",
                    const_labels: {
                        "build_time" => BUILD_INFO.time,
                        "version" => BUILD_INFO.version,
                        "build_sha" => BUILD_INFO.sha,
                        "os" => &os_info::get().to_string(),
                        "ncpus_logical" => &num_cpus::get().to_string(),
                        "ncpus_physical" => &num_cpus::get_physical().to_string(),
                        "cpu0" => &{
                            match &system.processors().get(0) {
                                None => "<unknown>".to_string(),
                                Some(cpu0) => format!("{} {}MHz", cpu0.brand(), cpu0.frequency()),
                            }
                        },
                        "memory_total" => &system.total_memory().to_string(),
                        "memory_limit" => memory_limit.max.map(|l| l / 1024).display_or("none"),
                        "swap_limit" => memory_limit.swap_max.map(|l| l / 1024).display_or("none")
                    },
                ),
                move || start_instant.elapsed().as_secs_f64(),
            )
        };

        Self {
            worker_count,
            uptime,
        }
    }
}
