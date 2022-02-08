// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits and types for replaying captured timely dataflow streams.
//!
//! This is roughly based on [timely::dataflow::operators::capture::Replay], which
//! provides the protocol and semantics of the [MzReplay] operator.

use crate::activator::RcActivator;
use std::time::{Duration, Instant};
use timely::dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use timely::dataflow::channels::pushers::Counter as PushCounter;
use timely::dataflow::operators::capture::event::EventIterator;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::generic::builder_raw::OperatorBuilder;
use timely::dataflow::{Scope, Stream};
use timely::progress::Timestamp;
use timely::Data;

/// Replay a capture stream into a scope with the same timestamp.
pub trait MzReplay<T: Timestamp, D: Data>: Sized {
    /// Replays `self` into the provided scope, as a `Stream<S, D>'.
    ///
    /// The `period` argument allows the specification of a re-activation period, where the operator
    /// will re-activate itself every so often.
    fn mz_replay<S: Scope<Timestamp = T>>(
        self,
        scope: &mut S,
        name: &str,
        perid: Duration,
        rc_activator: RcActivator,
    ) -> Stream<S, D>;
}

impl<T: Timestamp, D: Data, I> MzReplay<T, D> for I
where
    I: IntoIterator,
    <I as IntoIterator>::Item: EventIterator<T, D> + 'static,
{
    fn mz_replay<S: Scope<Timestamp = T>>(
        self,
        scope: &mut S,
        name: &str,
        period: Duration,
        rc_activator: RcActivator,
    ) -> Stream<S, D> {
        let name = format!("Replay {}", name);
        let mut builder = OperatorBuilder::new(name, scope.clone());

        let address = builder.operator_info().address;
        let activator = scope.activator_for(&address[..]);

        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let mut started = false;

        let mut last_active = Instant::now();

        rc_activator.register(scope.activator_for(&address[..]));

        builder.build(move |progress| {
            rc_activator.ack();
            if last_active + period <= Instant::now() || !started {
                last_active = Instant::now();
                activator.activate_after(period);
            }

            if !started {
                // The first thing we do is modify our capabilities to match the number of streams we manage.
                // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
                // our very first action.
                progress.internals[0]
                    .update(S::Timestamp::minimum(), (event_streams.len() as i64) - 1);
                started = true;
            }

            let mut buffer = Vec::new();

            for event_stream in event_streams.iter_mut() {
                while let Some(event) = event_stream.next() {
                    match &event {
                        Event::Progress(vec) => {
                            progress.internals[0].extend(vec.iter().cloned());
                        }
                        Event::Messages(time, data) => {
                            buffer.extend_from_slice(data);
                            output.session(time).give_vec(&mut buffer);
                        }
                    }
                }
            }

            output.cease();
            output
                .inner()
                .produced()
                .borrow_mut()
                .drain_into(&mut progress.produceds[0]);

            false
        });

        stream
    }
}
