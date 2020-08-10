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

pub async fn prof_time(
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
    let mut annotations = vec![];
    let stacks = report
        .data
        .into_iter()
        .map(|(f, weight)| {
            let mut addrs: Vec<_> = f.frames[0..f.depth]
                .iter()
                .map(|f| f.ip() as usize)
                .collect();
            addrs.reverse();
            let weight = weight as usize;
            let anno_idx = if merge_threads {
                None
            } else {
                let thread_name = String::from_utf8_lossy(&f.thread_name[0..f.thread_name_length]);
                let anno_idx = annotations
                    .iter()
                    .position(|anno| thread_name.as_ref() == anno)
                    .unwrap_or_else(|| {
                        annotations.push(thread_name.to_string());
                        annotations.len() - 1
                    });
                Some(anno_idx)
            };
            (WeightedStack { addrs, weight }, anno_idx)
        })
        .collect();
    Ok(StackProfile {
        annotations,
        stacks,
    })
}
