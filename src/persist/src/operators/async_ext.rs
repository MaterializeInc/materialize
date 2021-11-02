// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Capability;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::progress::Timestamp;
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
        Fut: Future<Output = Never> + 'static;
}

impl<G: Scope> OperatorBuilderExt<G> for OperatorBuilder<G> {
    fn build_async<B, Fut>(self, scope: G, constructor: B)
    where
        B: FnOnce(
            Vec<Capability<G::Timestamp>>,
            Rc<RefCell<Vec<Antichain<G::Timestamp>>>>,
            Scheduler,
        ) -> Fut,
        Fut: Future<Output = Never> + 'static,
    {
        let activator = scope.sync_activator_for(&self.operator_info().address[..]);
        let waker = futures_util::task::waker(Arc::new(activator));
        let shared_frontiers = Rc::new(RefCell::new(vec![
            Antichain::from_elem(Timestamp::minimum());
            self.shape().inputs()
        ]));

        self.build(move |capabilities| {
            let scheduler = Scheduler::default();
            let mut logic_fut = Box::pin(constructor(
                capabilities,
                Rc::clone(&shared_frontiers),
                scheduler.clone(),
            ));
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

                let had_pending_notify = scheduler.notify();
                let _ = Pin::new(&mut logic_fut).poll(&mut Context::from_waker(&waker));
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
            }
        });
    }
}
