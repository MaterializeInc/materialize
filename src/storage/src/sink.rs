// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Moving data to external systems

use mz_repr::Timestamp;
use timely::progress::Antichain;

mod iceberg;
mod kafka;
mod postgres;

/// Formats the error reported when a sink's input has been compacted past the
/// upper the sink last committed, so the sink can never resume. `remedy` tells
/// the user how to recover, which differs by sink type.
fn overcompacted_error(
    name: &str,
    as_of: &Antichain<Timestamp>,
    resume_upper: &Antichain<Timestamp>,
    remedy: &str,
) -> String {
    let fmt = |f: &Antichain<Timestamp>| match f.as_option() {
        Some(t) => t.to_string(),
        None => "[]".to_string(),
    };
    let gap = match (as_of.as_option(), resume_upper.as_option()) {
        (Some(as_of), Some(upper)) => {
            let secs = u64::from(*as_of).saturating_sub(u64::from(*upper)) / 1000;
            let (h, m, s) = (secs / 3600, secs % 3600 / 60, secs % 60);
            format!(" (gap {h}h{m:02}m{s:02}s)")
        }
        _ => String::new(),
    };
    format!(
        "sink {name} cannot resume: input compacted to {}, past last committed upper {}{gap}; \
         data in that range is unrecoverable, {remedy}",
        fmt(as_of),
        fmt(resume_upper),
    )
}
