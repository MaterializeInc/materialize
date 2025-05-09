// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for sending frontier probes to upstream systems.

use std::time::Duration;

use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::Timestamp;
use tracing::trace;

/// A ticker to drive source upstream probing.
///
/// This type works similar to [`tokio::time::Interval`] but returns timestamps from its
/// [`Ticker::tick`] method that can be used as probe timestamps. These timestamps are rounded down
/// to the nearest multiple of the tick interval, to reduce the amount of unique timestamps emitted
/// by sources, thereby reducing churn in downstream dataflows.
///
/// The ticker also supports usage in non-async contexts, using [`Ticker::tick_blocking`].
///
/// The tick interval is determined by the result of the `get_interval` closure. It is updated
/// after each tick, allowing it to be changed dynamically during the operation of the ticker.
pub(super) struct Ticker<G> {
    interval: EpochMillis,
    now: NowFn,
    last_tick: Option<EpochMillis>,
    get_interval: G,
}

impl<G: Fn() -> Duration> Ticker<G> {
    pub fn new(get_interval: G, now: NowFn) -> Self {
        let interval = get_interval().as_millis().try_into().unwrap();
        Self {
            interval,
            now,
            last_tick: None,
            get_interval,
        }
    }

    /// Wait until it is time for the next probe, returning a suitable probe timestamp.
    ///
    /// This method tries to resolve as close as possible to the returned probe timestamp, though
    /// it is not guaranteed to always succeed. If a tick is missed, it is skipped entirely.
    pub async fn tick(&mut self) -> Timestamp {
        let target = self.next_tick_target();

        let mut now = (self.now)();
        while now < target {
            let wait = Duration::from_millis(target - now);
            tokio::time::sleep(wait).await;
            now = (self.now)();
        }

        trace!(target, now, "probe ticker skew: {}ms", now - target);
        self.apply_tick(now)
    }

    /// Wait until it is time for the next probe, returning a suitable probe timestamp.
    ///
    /// Blocking version of [`Ticker::tick`].
    pub fn tick_blocking(&mut self) -> Timestamp {
        let target = self.next_tick_target();

        let mut now = (self.now)();
        while now < target {
            let wait = Duration::from_millis(target - now);
            std::thread::sleep(wait);
            now = (self.now)();
        }

        trace!(target, now, "probe ticker skew: {}ms", now - target);
        self.apply_tick(now)
    }

    /// Return the desired time of the next tick.
    fn next_tick_target(&self) -> EpochMillis {
        let target = match self.last_tick {
            Some(ms) => ms + self.interval,
            None => (self.now)(),
        };
        self.round_to_interval(target)
    }

    /// Apply a tick at the given time, returning the probe timestamp.
    fn apply_tick(&mut self, time: EpochMillis) -> Timestamp {
        let time = self.round_to_interval(time);
        self.last_tick = Some(time);

        // Refresh the interval for the next tick.
        self.interval = (self.get_interval)().as_millis().try_into().unwrap();
        trace!("probe ticker interval: {}ms", self.interval);

        time.into()
    }

    fn round_to_interval(&self, ms: EpochMillis) -> EpochMillis {
        ms - (ms % self.interval)
    }
}
