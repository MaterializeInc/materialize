// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Extensions for `OperatorBuilder` to create async operators.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::future::FusedFuture;
use futures_util::FutureExt;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Capability;
use timely::dataflow::Scope;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

/// A type that is not inhabited by any value. Should be redefined as the
/// [never](https://doc.rust-lang.org/std/primitive.never.html) type once it stabilizes
pub enum Never {}

/// A helper type to integrate timely notifications with async futures. Its intended to be used
/// with a top level while loop. Timely will automatically make the futures returned by
/// `scheduler.notified.await()` resolve when there are progress or data updates to be processed by
/// the operator
///
/// ```ignore
/// async fn example(s: Scheduler) {
///     while scheduler.notified().await {
///     }
/// }
/// ```
#[derive(Debug, Default, Clone)]
pub struct Scheduler {
    inner: Rc<Cell<bool>>,
}

impl Scheduler {
    /// Notifies a waiting task, returning a boolean indicating whether or not there was a pending
    /// notification already stored in the scheduler.
    ///
    /// If a task is currently waiting, that task is notified. Otherwise, a permit is stored in
    /// this `Scheduler` and the next call to `notified().await` will complete immediately
    /// consuming the permit made available by this call to `notify()`.
    fn notify(&self) -> bool {
        self.inner.replace(true)
    }

    /// Returns a boolean indicating whether a notification is pending
    fn is_notified(&self) -> bool {
        self.inner.get()
    }

    /// Wait for a notification.
    #[allow(dead_code)]
    pub fn notified(&self) -> Notified<'_> {
        Notified {
            notified: &*self.inner,
        }
    }
}

/// The future returned by [Scheduler::notified]
#[derive(Debug)]
pub struct Notified<'a> {
    notified: &'a Cell<bool>,
}

impl Future for Notified<'_> {
    type Output = bool;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
        if self.notified.replace(false) {
            Poll::Ready(true)
        } else {
            Poll::Pending
        }
    }
}

/// Extension trait for `OperatorBuilder`.
pub trait OperatorBuilderExt<G: Scope> {
    /// Creates an operator implementation from supplied async logic constructor.
    ///
    /// The logic constructror is expected to return a future that will never return and that
    /// follows the following pattern:
    ///
    /// ```ignore
    /// op.build_async(scope, move |capabilities, frontier, scheduler| async move {
    ///     while scheduler.yield_now().await {
    ///         // async operator logic here
    ///     }
    /// });
    /// ```
    ///
    /// Since timely's input handles and frontier notifications are not integrated with the async
    /// ecosystem the only way to yield control back to timely is by awaiting on
    /// `scheduler.yield_now()`. The operator will ensure that this call resolves when there is
    /// more work to do.
    fn build_async<B, Fut>(self, scope: G, constructor: B)
    where
        B: FnOnce(
            Vec<Capability<G::Timestamp>>,
            Rc<RefCell<Vec<Antichain<G::Timestamp>>>>,
            Scheduler,
        ) -> Fut,
        Fut: Future + 'static;
}

impl<G: Scope> OperatorBuilderExt<G> for OperatorBuilder<G> {
    fn build_async<B, Fut>(self, scope: G, constructor: B)
    where
        B: FnOnce(
            Vec<Capability<G::Timestamp>>,
            Rc<RefCell<Vec<Antichain<G::Timestamp>>>>,
            Scheduler,
        ) -> Fut,
        Fut: Future + 'static,
    {
        let activator = scope.sync_activator_for(&self.operator_info().address[..]);
        let waker = futures_util::task::waker(Arc::new(activator));
        let shared_frontiers = Rc::new(RefCell::new(vec![
            Antichain::from_elem(Timestamp::minimum());
            self.shape().inputs()
        ]));

        self.build_reschedule(move |capabilities| {
            let scheduler = Scheduler::default();
            let mut logic_fut = Box::pin(
                constructor(
                    capabilities,
                    Rc::clone(&shared_frontiers),
                    scheduler.clone(),
                )
                .fuse(),
            );
            move |frontiers| {
                // Attempt to update the shared frontier before polling the future.  This operation
                // can fail if the future also borrowed the frontier and kept the borrow active
                // across an await point. It is fine for this operation to fail because we will
                // poll the future right afterwards and will come back to this point once the
                // operator gets rescheduled. At that future moment we will be able to update the
                // frontier and poll the future with fresh data in the handle.
                if let Ok(mut shared_frontiers) = shared_frontiers.try_borrow_mut() {
                    for (shared, new) in shared_frontiers.iter_mut().zip(frontiers) {
                        if !PartialOrder::less_equal(&new.frontier(), &shared.borrow()) {
                            *shared = new.frontier().to_owned();
                        }
                    }
                }

                if logic_fut.is_terminated() {
                    return false;
                }

                let had_pending_notify = scheduler.notify();
                let operator_incomplete = Pin::new(&mut logic_fut)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending();
                // Here we check that:
                //   1. the scheduler had been notified in some previous run of the closure
                //   2. the future just went past a `scheduler.notified().await` point
                //
                // These can be true at the same time only if the logic future awaited on something
                // other then `scheduler.notified()`. We care about this case because while the
                // logic future is awaiting on some other future, more data might become available
                // in the input handles or the frontier might have progressed. Therefore this check
                // will ensure that `scheduler.notified()` will complete at least one more time
                // which will give a chance to the logic future to check its input and frontier.
                if had_pending_notify && !scheduler.is_notified() {
                    waker.wake_by_ref();
                }

                operator_incomplete
            }
        });
    }
}

/// Convenience macro used to wrap what might otherwise be the argument to `build_reschedule`.
#[macro_export]
macro_rules! async_op {
    (|$capabilities:ident, $frontiers:ident| $body:block) => {
        move |mut capabilities, frontiers, scheduler| async move {
            loop {
                scheduler.notified().await;

                // rebind to mutable references to make sure they can't be accidentally dropped
                #[allow(unused_mut)]
                let mut $capabilities = &mut capabilities;
                let $frontiers = (*frontiers.borrow()).clone();

                if !async { $body }.await && $frontiers.iter().all(|f| f.is_empty()) {
                    break;
                }
            }
        }
    };
}

#[cfg(test)]
mod test {
    use super::*;

    use std::time::Duration;

    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, ToStream};

    #[tokio::test]
    async fn async_operator() {
        // Run timely in a separate thread
        #[allow(clippy::disallowed_methods)]
        let extracted = tokio::task::spawn_blocking(|| {
            let capture = timely::example(|scope| {
                let input = (0..10).to_stream(scope);

                let mut op = OperatorBuilder::new("async_passthru".to_string(), input.scope());
                let mut input_handle = op.new_input(&input, Pipeline);
                let (mut output, output_stream) = op.new_output();

                op.build_async(
                    input.scope(),
                    async_op!(|capabilities, _frontiers| {
                        // Drop initial capabilities
                        capabilities.clear();
                        let mut output_handle = output.activate();
                        while let Some((cap, data)) = input_handle.next() {
                            let cap = cap.retain();

                            let mut session = output_handle.session(&cap);
                            for item in data.iter().copied() {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                session.give(item);
                            }
                        }
                        false
                    }),
                );

                output_stream.capture()
            });
            capture.extract()
        })
        .await
        .expect("timely panicked");

        assert_eq!(extracted, vec![(0, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]);
    }
}
