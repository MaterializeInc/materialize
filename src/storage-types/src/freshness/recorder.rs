// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Recording of wall-clock staleness into period histograms.
//!
//! A sample is the wall-clock interval `[a, b)` between two consecutive events of a recording,
//! frontier advances and period boundaries. It is charged its worst case: `b - a` milliseconds,
//! all in the bucket of the staleness `b - r` just before `b`, where `r` is the reflected time
//! in effect over the sample.

use std::collections::BTreeMap;
use std::time::Duration;

use mz_repr::Timestamp;

use super::histogram::BucketScheme;

/// Parameters of one recorder call. They may change between calls.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RecorderConfig {
    /// The bucket scheme of the periods opened from this call on.
    pub scheme: BucketScheme,
    /// The period length. Lengths below 1 ms count as 1 ms.
    pub period: Duration,
    /// How long the histograms keep periods. The recorder does not use it.
    pub retention: Duration,
}

impl RecorderConfig {
    fn period_ms(&self) -> u64 {
        u64::try_from(self.period.as_millis())
            .unwrap_or(u64::MAX)
            .max(1)
    }
}

/// One non-empty bucket of a closed period `[period_start, period_end)`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PeriodRow {
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    pub buckets_per_octave: u32,
    pub bucket: i32,
    /// Milliseconds of wall-clock time in the bucket.
    pub count_ms: u64,
}

/// The staleness at `now` of a collection that reflects `reflected`, 0 if it reflects a later
/// time.
pub(super) fn staleness(now: Timestamp, reflected: Timestamp) -> Duration {
    Duration::from_millis(u64::from(now.saturating_sub(reflected)))
}

/// The open period `[start, end)` with the counts of the samples charged to it so far.
#[derive(Debug)]
struct OpenPeriod {
    start: Timestamp,
    end: Timestamp,
    /// The configuration the period was opened with. Its rows must not mix bucket schemes.
    config: RecorderConfig,
    counts: BTreeMap<i32, u64>,
}

/// Records the wall-clock staleness of one collection into period histograms.
///
/// Periods are half-open and lie on the grid of the period length from the epoch, except that
/// the first one starts at the recording start and a change of the period length or bucket
/// scheme cuts the open period at the end of the last closed sample. The emitted periods tile
/// wall-clock time from the start without gaps or overlaps, and each one's counts sum to its
/// length. A period's rows are returned once, by the first call whose `now` reaches its end, or
/// by [`Self::finish`].
#[derive(Debug)]
pub struct StalenessRecorder {
    /// The start of the open sample. It never decreases, so no wall-clock time counts twice.
    sample_start: Timestamp,
    /// The reflected time in effect over the open sample.
    reflected: Timestamp,
    /// `None` until a call supplies the configuration to open it with, and after a period
    /// closed until the next sample reaches into the next one.
    period: Option<OpenPeriod>,
    /// The end of the last emitted period.
    emitted_until: Timestamp,
    finished: bool,
}

impl StalenessRecorder {
    /// Starts recording at `start`, against the reflected time `reflected`.
    pub fn new(start: Timestamp, reflected: Timestamp) -> Self {
        StalenessRecorder {
            sample_start: start,
            reflected,
            period: None,
            emitted_until: start,
            finished: false,
        }
    }

    /// Closes the open sample at `now` and starts the next one against `reflected`, returning
    /// the rows of periods that closed.
    ///
    /// A `now` at or before the open sample's start drops the sample, since the wall clock
    /// stepped back, and the open sample continues against `reflected`.
    pub fn observe(
        &mut self,
        now: Timestamp,
        reflected: Timestamp,
        config: &RecorderConfig,
    ) -> Vec<PeriodRow> {
        if self.finished {
            return Vec::new();
        }
        let mut rows = self.apply_config(config);
        self.close_sample(now, config, &mut rows);
        self.reflected = reflected;
        rows
    }

    /// Closes the open sample at the last period boundary at or before `now`, if it passed one,
    /// and returns the rows of periods that closed. The sample continues from the boundary
    /// against the same reflected time.
    pub fn tick(&mut self, now: Timestamp, config: &RecorderConfig) -> Vec<PeriodRow> {
        if self.finished {
            return Vec::new();
        }
        let mut rows = self.apply_config(config);
        let now = u64::from(now);
        let boundary = Timestamp::from(now - now % config.period_ms());
        self.close_sample(boundary, config, &mut rows);
        rows
    }

    /// Closes the open sample at `now`, returns the rows of the remaining partial period, and
    /// ends the recording. Every later call returns nothing.
    pub fn finish(&mut self, now: Timestamp, config: &RecorderConfig) -> Vec<PeriodRow> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let mut rows = self.apply_config(config);
        self.close_sample(now, config, &mut rows);
        self.cut(&mut rows);
        rows
    }

    /// Cuts the open period if `config` changed its period length or bucket scheme.
    fn apply_config(&mut self, config: &RecorderConfig) -> Vec<PeriodRow> {
        let mut rows = Vec::new();
        // A changed retention concerns only the consumers of the rows, so it does not cut.
        let changed = |p: &OpenPeriod| {
            p.config.scheme != config.scheme || p.config.period_ms() != config.period_ms()
        };
        if self.period.as_ref().is_some_and(changed) {
            self.cut(&mut rows);
        }
        rows
    }

    /// Ends the open period at the start of the open sample, everything counted so far, and
    /// emits it unless it is empty.
    fn cut(&mut self, rows: &mut Vec<PeriodRow>) {
        if let Some(mut period) = self.period.take() {
            period.end = self.sample_start;
            if period.start < period.end {
                self.emit(period, rows);
            }
        }
    }

    /// Charges the open sample up to `to` to the periods it overlaps, and starts the next
    /// sample at `to`. Does nothing unless `to` is after the open sample's start.
    fn close_sample(&mut self, to: Timestamp, config: &RecorderConfig, rows: &mut Vec<PeriodRow>) {
        while self.sample_start < to {
            let start = self.sample_start;
            let period = self.period.get_or_insert_with(|| {
                let start_ms = u64::from(start);
                let period_ms = config.period_ms();
                OpenPeriod {
                    start,
                    end: Timestamp::from(
                        (start_ms - start_ms % period_ms).saturating_add(period_ms),
                    ),
                    config: *config,
                    counts: BTreeMap::new(),
                }
            });
            // The sample is split at the period end, and each part is charged the staleness at
            // its own end.
            let end = to.min(period.end);
            let bucket = period.config.scheme.bucket(staleness(end, self.reflected));
            *period.counts.entry(bucket).or_default() += u64::from(end) - u64::from(start);
            self.sample_start = end;
            if end == period.end {
                let period = self.period.take().expect("open");
                self.emit(period, rows);
            }
        }
    }

    fn emit(&mut self, period: OpenPeriod, rows: &mut Vec<PeriodRow>) {
        let in_order = period.start >= self.emitted_until && period.end > period.start;
        debug_assert!(
            in_order,
            "period [{}, {}) after one ending at {}",
            period.start, period.end, self.emitted_until
        );
        self.emitted_until = period.end;
        let buckets_per_octave = period.config.scheme.per_octave();
        rows.extend(
            period
                .counts
                .into_iter()
                .map(|(bucket, count_ms)| PeriodRow {
                    period_start: period.start,
                    period_end: period.end,
                    buckets_per_octave,
                    bucket,
                    count_ms,
                }),
        );
    }
}
