// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by various subsystems.

pub mod differential;
pub mod materialized;
pub mod reachability;
pub mod timely;

use std::time::Duration;

use ::timely::communication::Push;
use ::timely::dataflow::channels::Bundle;
use ::timely::dataflow::operators::capture::{Event, EventPusher};
use ::timely::dataflow::operators::generic::OutputHandle;
use ::timely::dataflow::operators::Capability;
use ::timely::dataflow::operators::CapabilityRef;
use ::timely::progress::Timestamp as TimelyTimestamp;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::ExchangeData;

use dataflow_types::logging::{DifferentialLog, LogVariant, MaterializedLog, TimelyLog};
use repr::Timestamp;

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
    /// Batch size in bytes for batches
    const BATCH_SIZE_BYTES: usize = 1 << 13;

    /// Calculate the default buffer size based on `(Duration, E, T)` tuples.
    fn buffer_capacity() -> usize {
        let size = ::std::mem::size_of::<(Duration, E, T)>();
        if size == 0 {
            Self::BATCH_SIZE_BYTES
        } else if size <= Self::BATCH_SIZE_BYTES {
            Self::BATCH_SIZE_BYTES / size
        } else {
            1
        }
    }

    /// Creates a new batch logger.
    pub fn new(event_pusher: P, granularity_ms: u64) -> Self {
        BatchLogger {
            time_ms: 0,
            event_pusher,
            _phantom: ::std::marker::PhantomData,
            granularity_ms,
            buffer: Vec::with_capacity(Self::buffer_capacity()),
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

            self.buffer.append(data);
        }
        if self.time_ms < new_time_ms {
            // Flush buffered events that may need to advance.
            self.event_pusher.push(Event::Messages(
                self.time_ms as Timestamp,
                self.buffer.drain(..).collect(),
            ));
            if self.buffer.capacity() > Self::buffer_capacity() {
                self.buffer = Vec::with_capacity(Self::buffer_capacity())
            }

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

/// A buffer that consolidates updates
///
/// The buffer implements a wrapper around [OutputHandle] consolidating elements pushed to it. It is
/// backed by a capacity-limited buffer, which means that compaction only occurs within the
/// dimensions of the buffer, i.e. the number of unique keys is less than half of the buffer's
/// capacity.
///
/// A cap is retained whenever the current time changes to be able to flush on drop or when the time
/// changes again.
///
/// The buffer is filled with updates until it reaches its capacity. At this point, the updates are
/// consolidated to free up space. This process repeats until the consolidation recovered less than
/// half of the buffer's capacity, at which point the buffer will be shipped.
///
/// The buffer retains a capability to send data on flush. It will flush all data once dropped, if
/// time changes, or if the buffer capacity is reached.
pub struct ConsolidateBuffer<'a, T, D: ExchangeData, R: Semigroup, P>
where
    P: Push<Bundle<T, (D, T, R)>> + 'a,
    T: Clone + Lattice + Ord + TimelyTimestamp + 'a,
    D: 'a,
{
    // a buffer for records, to send at self.cap
    // Invariant: Buffer only contains data if cap is Some.
    buffer: Vec<(D, T, R)>,
    output_handle: OutputHandle<'a, T, (D, T, R), P>,
    cap: Option<Capability<T>>,
    port: usize,
}

impl<'a, T, D: ExchangeData, R: Semigroup, P> ConsolidateBuffer<'a, T, D, R, P>
where
    T: Clone + Lattice + Ord + TimelyTimestamp + 'a,
    P: Push<Bundle<T, (D, T, R)>> + 'a,
{
    /// Create a new [ConsolidateBuffer], wrapping the provided session.
    ///
    /// * `output_handle`: The output to send data to.
    /// * 'port': The output port to retain capabilities for.
    pub fn new(output_handle: OutputHandle<'a, T, (D, T, R), P>, port: usize) -> Self {
        Self {
            output_handle,
            port,
            cap: None,
            buffer: Vec::with_capacity(::timely::container::buffer::default_capacity::<(D, T, R)>()),
        }
    }

    /// Give an element to the buffer
    pub fn give(&mut self, cap: &CapabilityRef<T>, data: (D, T, R)) {
        // Retain a cap for the current time, which will be used on flush.
        if self.cap.as_ref().map_or(true, |t| t.time() != cap.time()) {
            // Flush on capability change
            self.flush();
            // Retain capability for the specified output port.
            self.cap = Some(cap.delayed_for_output(cap.time(), self.port));
        }
        self.buffer.push(data);
        if self.buffer.len() == self.buffer.capacity() {
            // Consolidate while the consolidation frees at least half the buffer
            consolidate_updates(&mut self.buffer);
            if self.buffer.len() > self.buffer.capacity() / 2 {
                self.flush();
            }
        }
    }

    /// Flush the internal buffer to the underlying session
    pub fn flush(&mut self) {
        if let Some(cap) = &self.cap {
            self.output_handle.session(cap).give_vec(&mut self.buffer);
        }
    }
}

impl<'a, T, D: ExchangeData, R: Semigroup, P> Drop for ConsolidateBuffer<'a, T, D, R, P>
where
    P: Push<Bundle<T, (D, T, R)>> + 'a,
    T: Clone + Lattice + Ord + TimelyTimestamp + 'a,
    D: 'a,
{
    fn drop(&mut self) {
        self.flush();
    }
}
