// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hydration-style update streams for exercising the MV sink correction buffers.
//!
//! The scenario these model: an MV sink restarts with an old as-of and the desired input replays
//! through `T` distinct timestamps while persist writes (and thus `advance_since` and
//! `updates_before` calls) trail behind. Reads and since advancement must only do work
//! proportional to the drained slice, otherwise the catch-up degenerates into quadratic behavior
//! in `T`.
//!
//! Shared between the `correction` criterion bench, which compares wall-clock time of the buffer
//! implementations, and the deterministic complexity tests in `correction_v2`, so both measure
//! the same workloads.

use mz_repr::{Datum, Diff, Row, Timestamp};

/// Number of updates inserted per distinct timestamp.
pub const UPDATES_PER_TS: u64 = 16;

/// Time offset of far-future retractions in the temporal-filter pattern.
const TEMPORAL_OFFSET: u64 = 1 << 40;

/// The shape of the update stream fed into the correction buffer.
#[derive(Clone, Copy, Debug)]
pub enum Pattern {
    /// Every timestamp appends new, distinct rows. Nothing consolidates away.
    Append,
    /// Every timestamp updates the same set of keys: an addition for the new value and a
    /// retraction of the previous one. Retraction-heavy, consolidates down to a small set.
    Upsert,
    /// Every timestamp appends new rows accompanied by their far-future retractions, and deletes
    /// the previous timestamp's rows, retracting now and re-adding the far-future retraction at a
    /// slightly different future time. Models an MV behind a temporal filter (e.g. a last-30-days
    /// view): an ever-growing mass of far-future updates that never participates in reads.
    TemporalFilter,
}

impl Pattern {
    /// All patterns, for iterating over the workloads.
    pub const ALL: [Pattern; 3] = [Self::Append, Self::Upsert, Self::TemporalFilter];

    /// Name for use in benchmark IDs and assertion messages.
    pub fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Upsert => "upsert",
            Self::TemporalFilter => "temporal_filter",
        }
    }
}

fn row(key: u64, value: u64) -> Row {
    let payload = format!("payload-{value:016}");
    Row::pack_slice(&[Datum::UInt64(key), Datum::String(&payload)])
}

/// Generate one batch of updates per distinct timestamp `0..num_ts`.
pub fn make_batches(num_ts: u64, pattern: Pattern) -> Vec<Vec<(Row, Timestamp, Diff)>> {
    (0..num_ts)
        .map(|t| {
            let time = Timestamp::from(t);
            match pattern {
                Pattern::Append => (0..UPDATES_PER_TS)
                    .map(|i| (row(t * UPDATES_PER_TS + i, t), time, Diff::ONE))
                    .collect(),
                Pattern::Upsert => (0..UPDATES_PER_TS / 2)
                    .flat_map(|key| {
                        let addition = (row(key, t), time, Diff::ONE);
                        let retraction = t
                            .checked_sub(1)
                            .map(|prev| (row(key, prev), time, -Diff::ONE));
                        std::iter::once(addition).chain(retraction)
                    })
                    .collect(),
                Pattern::TemporalFilter => (0..UPDATES_PER_TS / 4)
                    .flat_map(|i| {
                        let key = t * (UPDATES_PER_TS / 4) + i;
                        // New row, plus its retraction when the temporal filter window closes.
                        let this = [
                            (row(key, t), time, Diff::ONE),
                            (
                                row(key, t),
                                Timestamp::from(t + TEMPORAL_OFFSET),
                                -Diff::ONE,
                            ),
                        ];
                        // Delete the previous timestamp's row: retract it now and cancel its
                        // window-close retraction. The cancellation lands at a different future
                        // time than the original retraction, so the far-future mass grows.
                        let prev = t.checked_sub(1).map(|p| {
                            let key = p * (UPDATES_PER_TS / 4) + i;
                            [
                                (row(key, p), time, -Diff::ONE),
                                (row(key, p), Timestamp::from(t + TEMPORAL_OFFSET), Diff::ONE),
                            ]
                        });
                        this.into_iter().chain(prev.into_iter().flatten())
                    })
                    .collect(),
            }
        })
        .collect()
}
