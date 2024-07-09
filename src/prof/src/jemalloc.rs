// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Utilities for getting jemalloc statistics, as well as exporting them as metrics.

use std::time::Duration;

use jemalloc_pprof::JemallocProfCtl;
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, UIntGauge};
use pprof_util::ProfStartTime;
use tikv_jemalloc_ctl::{epoch, stats};

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

#[derive(Copy, Clone, Debug)]
pub struct JemallocProfMetadata {
    pub start_time: Option<ProfStartTime>,
}

// See stats.{allocated, active, ...} in http://jemalloc.net/jemalloc.3.html for details
pub struct JemallocStats {
    pub active: usize,
    pub allocated: usize,
    pub metadata: usize,
    pub resident: usize,
    pub retained: usize,
}

pub trait JemallocProfCtlExt {
    fn dump_stats(&mut self, json_format: bool) -> anyhow::Result<String>;
    fn stats(&self) -> anyhow::Result<JemallocStats>;
}

impl JemallocProfCtlExt for JemallocProfCtl {
    fn dump_stats(&mut self, json_format: bool) -> anyhow::Result<String> {
        // Try to avoid allocations within `stats_print`
        let mut buf = Vec::with_capacity(1 << 22);
        let mut options = tikv_jemalloc_ctl::stats_print::Options::default();
        options.json_format = json_format;
        tikv_jemalloc_ctl::stats_print::stats_print(&mut buf, options)?;
        Ok(String::from_utf8(buf)?)
    }

    fn stats(&self) -> anyhow::Result<JemallocStats> {
        JemallocStats::get()
    }
}

impl JemallocStats {
    pub fn get() -> anyhow::Result<JemallocStats> {
        epoch::advance()?;
        Ok(JemallocStats {
            active: stats::active::read()?,
            allocated: stats::allocated::read()?,
            metadata: stats::metadata::read()?,
            resident: stats::resident::read()?,
            retained: stats::retained::read()?,
        })
    }
}

/// Metrics for jemalloc.
pub struct JemallocMetrics {
    pub active: UIntGauge,
    pub allocated: UIntGauge,
    pub metadata: UIntGauge,
    pub resident: UIntGauge,
    pub retained: UIntGauge,
}

impl JemallocMetrics {
    /// Registers the metrics into the provided metrics registry, and spawns
    /// a task to keep the metrics up to date.
    // `async` indicates that the Tokio runtime context is required.
    #[allow(clippy::unused_async)]
    pub async fn register_into(registry: &MetricsRegistry) {
        let m = JemallocMetrics {
            active: registry.register(metric!(
                name: "jemalloc_active",
                help: "Total number of bytes in active pages allocated by the application",
            )),
            allocated: registry.register(metric!(
                name: "jemalloc_allocated",
                help: "Total number of bytes allocated by the application",
            )),
            metadata: registry.register(metric!(
                name: "jemalloc_metadata",
                help: "Total number of bytes dedicated to metadata.",
            )),
            resident: registry.register(metric!(
                name: "jemalloc_resident",
                help: "Maximum number of bytes in physically resident data pages mapped",
            )),
            retained: registry.register(metric!(
                name: "jemalloc_retained",
                help: "Total number of bytes in virtual memory mappings",
            )),
        };

        mz_ore::task::spawn(|| "jemalloc_stats_update", async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(e) = m.update() {
                    tracing::warn!("Error while updating jemalloc stats: {}", e);
                }
            }
        });
    }

    fn update(&self) -> anyhow::Result<()> {
        let s = JemallocStats::get()?;
        self.active.set(u64::cast_from(s.active));
        self.allocated.set(u64::cast_from(s.allocated));
        self.metadata.set(u64::cast_from(s.metadata));
        self.resident.set(u64::cast_from(s.resident));
        self.retained.set(u64::cast_from(s.retained));
        Ok(())
    }
}
