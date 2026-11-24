// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wall-clock staleness histograms of collections, bucketed exponentially.
//!
//! A collection's staleness at wall-clock time `w` is how far `w` is ahead of its reflected
//! time, the latest wall-clock time whose data the collection contains in full, see
//! [`reflected_time`]. Between frontier advances the reflected time stays put while the wall
//! clock moves, so staleness rises linearly and drops at each advance.
//!
//! [`StalenessRecorder`] charges each sample, the span between two events, to the bucket of
//! its staleness at its end, weighted by its length in milliseconds, and emits a
//! [`PeriodRow`] per non-empty bucket of each closed period. [`StalenessReporter`] tracks the
//! worst staleness since its last read, for the lag history and metrics.
//!
//! ```
//! # use std::time::Duration;
//! # use mz_repr::Timestamp;
//! # use mz_storage_types::freshness::{BucketScheme, RecorderConfig, StalenessRecorder};
//! let config = RecorderConfig {
//!     scheme: BucketScheme::new(4).expect("power of two"),
//!     period: Duration::from_secs(60),
//! };
//! let mut recorder = StalenessRecorder::new(Timestamp::from(0), Timestamp::from(0));
//! // The frontier advances at 200 ms to reflect 190 ms. The sample [0, 200) is charged
//! // 200 ms at staleness 200 ms. No period closed yet.
//! assert!(recorder.observe(Timestamp::from(200), Timestamp::from(190), &config).is_empty());
//! // The period [0, 60000) closes. Its counts sum to its length.
//! let rows = recorder.tick(Timestamp::from(60_000), &config);
//! assert_eq!(rows.iter().map(|r| r.count_ms).sum::<u64>(), 60_000);
//! ```

mod histogram;
mod recorder;
mod reflected;
mod reporter;

pub use histogram::BucketScheme;
pub use recorder::{PeriodRow, RecorderConfig, StalenessRecorder};
pub use reflected::reflected_time;
pub use reporter::StalenessReporter;

#[cfg(test)]
mod tests;
