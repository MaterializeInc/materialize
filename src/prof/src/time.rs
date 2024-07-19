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

use std::os::raw::c_int;

use anyhow::bail;
use pprof::ProfilerGuard;
use pprof_util::{StackProfile, WeightedStack};
use tokio::time::{self, Duration};

/// # Safety
///
/// Nothing else must be attempting to unwind backtraces while this is called.
/// In particular, jemalloc memory profiling must be off.
pub async unsafe fn prof_time(
    total_time: Duration,
    sample_freq: u32,
    merge_threads: bool,
) -> anyhow::Result<StackProfile> {
    if sample_freq > 1_000_000 {
        bail!("Sub-microsecond intervals are not supported.");
    }
    let sample_freq = c_int::try_from(sample_freq)?;
    let pg = ProfilerGuard::new(sample_freq)?;
    time::sleep(total_time).await;
    let builder = pg.report();
    let report = builder.build_unresolved()?;
    let mut profile = StackProfile::default();
    for (f, weight) in report.data {
        let thread_name;
        // No other known way to convert `*mut c_void` to `usize`.
        #[allow(clippy::as_conversions)]
        let mut addrs: Vec<_> = f.frames.iter().map(|f| f.ip() as usize).collect();
        addrs.reverse();
        // No other known way to convert `isize` to `f64`.
        #[allow(clippy::as_conversions)]
        let weight = weight as f64;
        let anno = if merge_threads {
            None
        } else {
            thread_name = String::from_utf8_lossy(&f.thread_name[0..f.thread_name_length]);
            Some(thread_name.as_ref())
        };
        let stack = WeightedStack { addrs, weight };
        profile.push_stack(stack, anno);
    }

    Ok(profile)
}
