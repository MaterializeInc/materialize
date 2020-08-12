// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{StackProfile, WeightedStack};
use anyhow::bail;
use pprof::ProfilerGuard;
use std::{os::raw::c_int, time::Duration};
use tokio::time::delay_for;

// SAFETY - nothing else must be attempting to unwind backtraces while this is called.
// In particular, jemalloc memory profiling must be off.
pub async unsafe fn prof_time(
    total_time: Duration,
    sample_freq: u32,
    merge_threads: bool,
) -> anyhow::Result<StackProfile> {
    if sample_freq > (1e6 as u32) {
        bail!("Sub-microsecond intervals are not supported.");
    }
    let pg = ProfilerGuard::new(sample_freq as c_int)?;
    delay_for(total_time).await;
    let builder = pg.report();
    let report = builder.build_unresolved()?;
    let mut profile = <StackProfile as Default>::default();
    for (f, weight) in report.data {
        let thread_name;
        let mut addrs: Vec<_> = f.frames[0..f.depth]
            .iter()
            .map(|f| f.ip() as usize)
            .collect();
        addrs.reverse();
        let weight = weight as f64;
        let anno = if merge_threads {
            None
        } else {
            thread_name = String::from_utf8_lossy(&f.thread_name[0..f.thread_name_length]);
            Some(thread_name.as_ref())
        };
        let stack = WeightedStack { addrs, weight };
        profile.push(stack, anno);
    }

    Ok(profile)
}
