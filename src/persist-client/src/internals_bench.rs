// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks of internal code not exposed through the public API.

#![allow(missing_docs)]

use std::hint::black_box;
use std::time::Instant;

use differential_dataflow::trace::Description;
use timely::progress::Antichain;
use tracing::info;

use crate::internal::state::HollowBatch;
use crate::internal::trace::Trace;

pub fn trace_push_batch_one_iter(num_batches: usize) {
    let mut trace = Trace::<usize>::default();
    let mut start = Instant::now();
    for ts in 0..num_batches {
        if ts % 1000 == 0 {
            info!("{} {:?}", ts, start.elapsed());
            start = Instant::now();
        }
        // A single non-empty batch followed by a large number of empty batches
        // and no compaction. This is a particularly problematic workload for
        // our fork of Spine which came up during deserialization of State in
        // database-issues#4985.
        //
        // Other, much better handled, workloads include all empty or all
        // non-empty.
        let len = if ts == 0 { 1 } else { 0 };
        let _ = trace.push_batch(HollowBatch::new_run(
            Description::new(
                Antichain::from_elem(ts),
                Antichain::from_elem(ts + 1),
                Antichain::from_elem(0),
            ),
            vec![],
            len,
        ));
    }
    black_box(trace);
}
