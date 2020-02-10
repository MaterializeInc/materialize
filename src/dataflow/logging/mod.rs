// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod differential;
pub mod materialized;
pub mod timely;

use ::timely::dataflow::operators::capture::{Event, EventPusher};
use dataflow_types::logging::{DifferentialLog, LogVariant, MaterializedLog, TimelyLog};
use dataflow_types::Timestamp;
use std::time::Duration;

/// Logs events as a timely stream, with progress statements.
pub struct BatchLogger<T, E, P>
where
    P: EventPusher<Timestamp, (Duration, E, T)>,
{
    /// Time in milliseconds of the current expressed capability.
    time_ms: Timestamp,
    event_pusher: P,
    _phantom: ::std::marker::PhantomData<(E, T)>,
    /// Each time is advanced to the strictly next millisecond that is a multiple of this granularity.
    /// This means we should be able to perform the same action on timestamp capabilities, and only
    /// flush buffers when this timestamp advances.
    granularity_ms: u64,
    /// A stash for data that does not yet need to be sent.
    buffer: Vec<(Duration, E, T)>,
}

impl<T, E, P> BatchLogger<T, E, P>
where
    P: EventPusher<Timestamp, (Duration, E, T)>,
{
    /// Creates a new batch logger.
    pub fn new(event_pusher: P, granularity_ms: u64) -> Self {
        BatchLogger {
            time_ms: 0,
            event_pusher,
            _phantom: ::std::marker::PhantomData,
            granularity_ms,
            buffer: Vec::with_capacity(1024),
        }
    }

    /// Publishes a batch of logged events and advances the capability.
    pub fn publish_batch(&mut self, time: &Duration, data: &mut Vec<(Duration, E, T)>) {
        let new_time_ms =
            (((time.as_millis() as Timestamp) / self.granularity_ms) + 1) * self.granularity_ms;
        if !data.is_empty() {
            // If we don't need to grow our buffer, move
            if data.len() > self.buffer.capacity() - self.buffer.len() {
                self.event_pusher.push(Event::Messages(
                    self.time_ms as Timestamp,
                    self.buffer.drain(..).collect(),
                ));
            }

            self.buffer.extend(data.drain(..));
        }
        if self.time_ms < new_time_ms {
            // Flush buffered events that may need to advance.
            self.event_pusher.push(Event::Messages(
                self.time_ms as Timestamp,
                self.buffer.drain(..).collect(),
            ));

            // In principle we can buffer up until this point, if that is appealing to us.
            // We could buffer more aggressively if the logging granularity were exposed
            // here, as the forward ticks would be that much less frequent.
            self.event_pusher
                .push(Event::Progress(vec![(new_time_ms, 1), (self.time_ms, -1)]));
        }
        self.time_ms = new_time_ms;
    }
}
impl<T, E, P> Drop for BatchLogger<T, E, P>
where
    P: EventPusher<Timestamp, (Duration, E, T)>,
{
    fn drop(&mut self) {
        self.event_pusher
            .push(Event::Progress(vec![(self.time_ms as Timestamp, -1)]));
    }
}
