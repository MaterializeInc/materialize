// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reporting of the current wall-clock staleness of a collection.

use std::time::Duration;

use mz_repr::Timestamp;

use super::recorder::staleness;

/// Tracks the worst-case staleness of the samples of one collection for periodic reads.
#[derive(Debug)]
pub struct StalenessReporter {
    /// The start of the open sample. It never decreases.
    sample_start: Timestamp,
    /// The reflected time in effect over the open sample.
    reflected: Timestamp,
    /// The largest worst case of the samples closed since the last read.
    peak: Duration,
    /// The worst case of the last closed sample.
    last: Duration,
}

impl StalenessReporter {
    /// Starts the first sample at `start`, against the reflected time `reflected`.
    pub fn new(start: Timestamp, reflected: Timestamp) -> Self {
        StalenessReporter {
            sample_start: start,
            reflected,
            peak: Duration::ZERO,
            last: Duration::ZERO,
        }
    }

    /// Closes the open sample at `now` and starts the next one against `reflected`.
    ///
    /// A `now` at or before the open sample's start drops the sample, and the open sample
    /// continues against `reflected`.
    pub fn observe(&mut self, now: Timestamp, reflected: Timestamp) {
        if self.sample_start < now {
            let worst = staleness(now, self.reflected);
            self.peak = self.peak.max(worst);
            self.last = worst;
            self.sample_start = now;
        }
        self.reflected = reflected;
    }

    /// The largest of the worst cases of the samples closed since the last read, the worst case
    /// of the last closed sample, and the current staleness at `now`.
    pub fn read(&mut self, now: Timestamp) -> Duration {
        std::mem::take(&mut self.peak)
            .max(self.last)
            .max(staleness(now, self.reflected))
    }
}
