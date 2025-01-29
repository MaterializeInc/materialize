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

use std::any::Any;
use std::borrow::Cow;
use std::rc::Rc;
use std::time::{Duration, Instant};
use timely::container::ContainerBuilder;

use timely::dataflow::channels::pushers::buffer::{Buffer as PushBuffer, Session};
use timely::dataflow::channels::pushers::{Counter as PushCounter, Tee};
use timely::dataflow::operators::capture::event::EventIterator;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::generic::builder_raw::OperatorBuilder;
use timely::dataflow::{Scope, StreamCore};
use timely::progress::Timestamp;
use timely::scheduling::ActivateOnDrop;
use timely::Container;

use crate::activator::ActivatorTrait;

/// Replay a capture stream into a scope with the same timestamp.
pub trait MzReplay<T, C, A>: Sized
where
    T: Timestamp,
    A: ActivatorTrait,
{
    /// Replays `self` into the provided scope, as a `StreamCore<S, CB::Container>` and provides
    /// a cancellation token. Uses the supplied container builder `CB` to form containers.
    ///
    /// The `period` argument allows the specification of a re-activation period, where the operator
    /// will re-activate itself every so often.
    ///
    /// * `scope`: The [Scope] to replay into.
    /// * `name`: Human-readable debug name of the Timely operator.
    /// * `period`: Reschedule the operator once the period has elapsed.
    ///    Provide [Duration::MAX] to disable periodic scheduling.
    /// * `activator`: An activator to trigger the operator.
    fn mz_replay<S: Scope<Timestamp = T>, CB, L>(
        self,
        scope: &mut S,
        name: &str,
        period: Duration,
        activator: A,
        logic: L,
    ) -> (StreamCore<S, CB::Container>, Rc<dyn Any>)
    where
        CB: ContainerBuilder,
        L: FnMut(Session<T, CB, PushCounter<T, CB::Container, Tee<T, CB::Container>>>, Cow<C>)
            + 'static;
}

impl<T, C, I, A> MzReplay<T, C, A> for I
where
    T: Timestamp,
    C: Container + Clone,
    I: IntoIterator,
    I::Item: EventIterator<T, C> + 'static,
    A: ActivatorTrait + 'static,
{
    fn mz_replay<S: Scope<Timestamp = T>, CB, L>(
        self,
        scope: &mut S,
        name: &str,
        period: Duration,
        activator: A,
        mut logic: L,
    ) -> (StreamCore<S, CB::Container>, Rc<dyn Any>)
    where
        for<'a> CB: ContainerBuilder,
        L: FnMut(Session<T, CB, PushCounter<T, CB::Container, Tee<T, CB::Container>>>, Cow<C>)
            + 'static,
    {
        let name = format!("Replay {}", name);
        let mut builder = OperatorBuilder::new(name, scope.clone());

        let address = builder.operator_info().address;
        let periodic_activator = scope.activator_for(Rc::clone(&address));

        let (targets, stream) = builder.new_output();

        let mut output: PushBuffer<_, CB, _> = PushBuffer::new(PushCounter::new(targets));
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let mut started = false;

        let mut last_active = Instant::now();

        let mut progress_sofar =
            <timely::progress::ChangeBatch<_>>::new_from(S::Timestamp::minimum(), 1);
        let token = Rc::new(ActivateOnDrop::new(
            (),
            Rc::clone(&address),
            scope.activations(),
        ));
        let weak_token = Rc::downgrade(&token);

        activator.register(scope, address);

        builder.build(move |progress| {
            activator.ack();
            if last_active
                .checked_add(period)
                .map_or(false, |next_active| next_active <= Instant::now())
                || !started
            {
                last_active = Instant::now();
                if period < Duration::MAX {
                    periodic_activator.activate_after(period);
                }
            }

            if !started {
                // The first thing we do is modify our capabilities to match the number of streams we manage.
                // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
                // our very first action.
                let len: i64 = event_streams
                    .len()
                    .try_into()
                    .expect("Implausibly large vector");
                progress.internals[0].update(S::Timestamp::minimum(), len - 1);
                progress_sofar.update(S::Timestamp::minimum(), len);
                started = true;
            }

            if weak_token.upgrade().is_some() {
                for event_stream in event_streams.iter_mut() {
                    while let Some(event) = event_stream.next() {
                        use Cow::*;
                        match event {
                            Owned(Event::Progress(vec)) => {
                                progress.internals[0].extend(vec.iter().cloned());
                                progress_sofar.extend(vec.into_iter());
                            }
                            Owned(Event::Messages(time, data)) => {
                                logic(output.session_with_builder(&time), Owned(data));
                            }
                            Borrowed(Event::Progress(vec)) => {
                                progress.internals[0].extend(vec.iter().cloned());
                                progress_sofar.extend(vec.iter().cloned());
                            }
                            Borrowed(Event::Messages(time, data)) => {
                                logic(output.session_with_builder(time), Borrowed(data));
                            }
                        }
                    }
                }
            } else {
                // Negate the accumulated progress contents emitted so far.
                progress.internals[0]
                    .extend(progress_sofar.drain().map(|(time, diff)| (time, -diff)));
            }

            output.cease();
            output
                .inner()
                .produced()
                .borrow_mut()
                .drain_into(&mut progress.produceds[0]);

            false
        });

        (stream, token)
    }
}
