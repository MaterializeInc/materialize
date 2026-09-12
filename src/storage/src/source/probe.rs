// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for sending frontier probes to upstream systems.

use std::convert::Infallible;
use std::time::Duration;

use differential_dataflow::Hashable;
use futures::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::{GlobalId, Timestamp};
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::StreamVec;
use timely::dataflow::channels::pact::Pipeline;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::trace;

use crate::source::types::Probe;

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
        let mut ticker = Self {
            interval: Default::default(),
            now,
            last_tick: None,
            get_interval,
        };
        ticker.refresh_interval();
        ticker
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

    fn refresh_interval(&mut self) {
        let ms = (self.get_interval)().as_millis().try_into().unwrap();
        self.interval = std::cmp::max(ms, 1);
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
        self.refresh_interval();
        trace!("probe ticker interval: {}ms", self.interval);

        time.into()
    }

    fn round_to_interval(&self, ms: EpochMillis) -> EpochMillis {
        ms - (ms % self.interval)
    }
}

/// Floors `ts` to the largest multiple of `grid` that is not greater than it.
///
/// A zero grid leaves `ts` unchanged.
pub(super) fn floor_to_grid(ts: Timestamp, grid: Duration) -> Timestamp {
    let grid_ms = u64::try_from(grid.as_millis()).unwrap_or(u64::MAX);
    if grid_ms == 0 {
        return ts;
    }
    let ms = u64::from(ts);
    Timestamp::from(ms - (ms % grid_ms))
}

/// Emits a probe whenever the frontier of `progress` advances, at most once per `min_interval`.
///
/// Runs on one worker chosen by `source_id`. Never emits the minimum frontier, so the first
/// binding of a source stays the snapshot binding, and never emits an empty frontier, which the
/// remap operator treats as source shutdown.
pub(super) fn arrival_probes<'scope, T: TimelyTimestamp>(
    source_id: GlobalId,
    progress: StreamVec<'scope, T, Infallible>,
    min_interval: Duration,
    now_fn: NowFn,
) -> StreamVec<'scope, T, Probe<T>> {
    let scope = progress.scope();

    let active_worker = usize::cast_from(source_id.hashed()) % scope.peers();
    let is_active_worker = active_worker == scope.index();

    let mut op = AsyncOperatorBuilder::new("arrival_probes".into(), scope);
    let (output, output_stream) = op.new_output::<CapacityContainerBuilder<_>>();
    let mut input = op.new_input_for(progress, Pipeline, &output);

    op.build(|caps| async move {
        if !is_active_worker {
            return;
        }

        let [cap] = caps.try_into().expect("one capability per output");

        let min_ms = u64::try_from(min_interval.as_millis()).unwrap_or(u64::MAX);
        let minimum_frontier = Antichain::from_elem(T::minimum());
        let mut frontier = minimum_frontier.clone();
        let mut last_emit: Option<EpochMillis> = None;
        let mut pending = false;

        loop {
            // The sleep is only armed while a frontier advance waits for the rate limit to
            // expire, so the `Duration::MAX` fallback is never slept on.
            let wait = match (pending, last_emit) {
                (true, Some(last)) => {
                    Duration::from_millis(last.saturating_add(min_ms).saturating_sub(now_fn()))
                }
                _ => Duration::MAX,
            };

            tokio::select! {
                event = input.next() => match event {
                    Some(AsyncEvent::Progress(new_frontier)) => {
                        if new_frontier == frontier
                            || new_frontier == minimum_frontier
                            || new_frontier.is_empty()
                        {
                            continue;
                        }
                        frontier = new_frontier;

                        let now = now_fn();
                        if last_emit.is_none_or(|last| now >= last.saturating_add(min_ms)) {
                            trace!(
                                "arrival_probes({source_id}) probing at {now}: \
                                frontier advanced"
                            );
                            output.give(&cap, Probe {
                                probe_ts: now.into(),
                                upstream_frontier: frontier.clone(),
                            });
                            last_emit = Some(now);
                            pending = false;
                        } else {
                            pending = true;
                        }
                    }
                    Some(AsyncEvent::Data(..)) => unreachable!("progress-only stream"),
                    None => return,
                },
                _ = tokio::time::sleep(wait), if pending => {
                    let now = now_fn();
                    trace!(
                        "arrival_probes({source_id}) probing at {now}: rate limit expired"
                    );
                    output.give(&cap, Probe {
                        probe_ts: now.into(),
                        upstream_frontier: frontier.clone(),
                    });
                    last_emit = Some(now);
                    pending = false;
                }
            }
        }
    });

    output_stream
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mz_repr::Timestamp;

    use super::floor_to_grid;

    #[mz_ore::test]
    fn floor_to_grid_rounds_down_to_multiples() {
        let grid = Duration::from_millis(250);
        let ts = Timestamp::from;
        assert_eq!(floor_to_grid(ts(1000), grid), ts(1000));
        assert_eq!(floor_to_grid(ts(1249), grid), ts(1000));
        assert_eq!(floor_to_grid(ts(1250), grid), ts(1250));
        assert_eq!(floor_to_grid(ts(7), grid), ts(0));
        assert_eq!(floor_to_grid(ts(7), Duration::ZERO), ts(7));
    }
}
