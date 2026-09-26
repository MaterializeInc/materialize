// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Request attribution independent of internal tracing spans.
//!
//! Context is explicit at ownership boundaries. Capture [`current`] when queuing
//! work, then use [`scope`] when executing it. A background task or a batch with
//! multiple request owners must use an empty context, not an arbitrary caller's.
//! The thread-local slot is populated only during a synchronous scope, future
//! poll, or future destruction. It never remains entered across suspension.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::{pin_project, pinned_drop};
use uuid::Uuid;

/// Identity of a frontend request, independent of statement logging or tracing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct RequestContext {
    /// The existing session UUID. This does not contain credentials or SQL text.
    pub session_id: Uuid,
    /// A session-local sequence, with zero reserved for session lifecycle work.
    /// This is not a statement-logging identifier.
    pub request_id: u64,
}

thread_local! {
    static CURRENT: Cell<Option<RequestContext>> = const { Cell::new(None) };
}

/// Returns the context of the work currently executing on this thread.
pub fn current() -> Option<RequestContext> {
    CURRENT.with(Cell::get)
}

/// Captures request identity only when the startup diagnostic mode uses it.
pub fn capture() -> Option<RequestContext> {
    if crate::tracing::QpsTracingMode::current().request_context() {
        current()
    } else {
        None
    }
}

/// Scopes diagnostic work without TLS updates in the baseline/filter-only modes.
pub fn scope_if_enabled<F: Future>(
    context: Option<RequestContext>,
    future: F,
) -> impl Future<Output = F::Output> {
    // Construct the scope eagerly so cancellation before the first poll is scoped.
    if crate::tracing::QpsTracingMode::current().request_context() {
        either::Either::Right(scope(context, future))
    } else {
        either::Either::Left(future)
    }
}

/// Runs synchronous diagnostic work without baseline/filter-only TLS updates.
pub fn in_scope_if_enabled<T>(context: Option<RequestContext>, f: impl FnOnce() -> T) -> T {
    if crate::tracing::QpsTracingMode::current().request_context() {
        in_scope(context, f)
    } else {
        f()
    }
}

/// Executes synchronous work in a context, restoring the caller on panic too.
///
/// `None` explicitly clears an enclosing context. For async work use [`scope`].
pub fn in_scope<T>(context: Option<RequestContext>, f: impl FnOnce() -> T) -> T {
    struct Restore(Option<RequestContext>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(CURRENT.with(|slot| slot.replace(context)));
    f()
}

/// Scopes every poll and destruction of a future, including unpolled cancellation.
///
/// This does not propagate context to separately spawned tasks or queued work.
pub fn scope<F: Future>(context: Option<RequestContext>, future: F) -> Scoped<F> {
    Scoped {
        context,
        future: Some(future),
    }
}

/// A future that restores its request context while executing or being dropped.
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub struct Scoped<F> {
    context: Option<RequestContext>,
    #[pin]
    future: Option<F>,
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        in_scope(*this.context, || {
            this.future
                .as_pin_mut()
                .expect("future present until destruction")
                .poll(cx)
        })
    }
}

#[pinned_drop]
impl<F> PinnedDrop for Scoped<F> {
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        in_scope(*this.context, || {
            // Drop in place under the original request even if no poll occurred.
            this.future.set(None);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomPinned;
    use std::panic::AssertUnwindSafe;
    #[cfg(not(feature = "panic"))]
    use std::panic::catch_unwind;
    use std::sync::{Arc, Mutex};
    use std::task::Waker;

    use super::*;
    #[cfg(feature = "panic")]
    use crate::panic::catch_unwind;

    fn request(n: u64) -> Option<RequestContext> {
        Some(RequestContext {
            session_id: Uuid::from_u128(u128::from(n)),
            request_id: n,
        })
    }

    #[crate::test]
    fn nested_and_empty_scopes_restore_after_panic() {
        assert_eq!(current(), None);
        in_scope(request(1), || {
            assert_eq!(current(), request(1));
            in_scope(None, || assert_eq!(current(), None));
            let result = catch_unwind(|| {
                in_scope(request(2), || {
                    assert_eq!(current(), request(2));
                    panic!("scope panic");
                });
            });
            assert!(result.is_err());
            assert_eq!(current(), request(1));
        });
        assert_eq!(current(), None);
    }

    #[derive(Debug)]
    struct CheckedFuture {
        expected: Option<RequestContext>,
        dropped: Arc<Mutex<Vec<Option<RequestContext>>>>,
        panic_on_poll: bool,
        _pin: PhantomPinned,
    }

    impl Future for CheckedFuture {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            assert_eq!(current(), self.expected);
            assert!(!self.panic_on_poll, "poll panic");
            Poll::Pending
        }
    }

    impl Drop for CheckedFuture {
        fn drop(&mut self) {
            assert_eq!(current(), self.expected);
            self.dropped.lock().unwrap().push(current());
        }
    }

    fn checked(
        expected: Option<RequestContext>,
        dropped: &Arc<Mutex<Vec<Option<RequestContext>>>>,
        panic_on_poll: bool,
    ) -> Pin<Box<Scoped<CheckedFuture>>> {
        Box::pin(scope(
            expected,
            CheckedFuture {
                expected,
                dropped: Arc::clone(dropped),
                panic_on_poll,
                _pin: PhantomPinned,
            },
        ))
    }

    #[crate::test]
    fn pending_requests_migrate_and_cancel_with_their_own_context() {
        let dropped = Arc::new(Mutex::new(Vec::new()));
        let mut first = checked(request(1), &dropped, false);
        let mut second = checked(request(2), &dropped, false);
        let mut empty = checked(None, &dropped, false);
        in_scope(request(3), || {
            let mut cx = Context::from_waker(Waker::noop());
            assert!(first.as_mut().poll(&mut cx).is_pending());
            assert!(second.as_mut().poll(&mut cx).is_pending());
            assert!(empty.as_mut().poll(&mut cx).is_pending());
            assert_eq!(current(), request(3));
        });
        std::thread::spawn(move || {
            in_scope(request(4), || {
                let mut cx = Context::from_waker(Waker::noop());
                assert!(second.as_mut().poll(&mut cx).is_pending());
                assert!(first.as_mut().poll(&mut cx).is_pending());
                drop(first);
                drop(second);
                drop(empty);
                assert_eq!(current(), request(4));
            });
            assert_eq!(current(), None);
        })
        .join()
        .unwrap();
        assert_eq!(*dropped.lock().unwrap(), vec![request(1), request(2), None]);
        assert_eq!(current(), None);
    }

    #[crate::test]
    fn unpolled_cancellation_and_poll_panic_restore_caller() {
        let dropped = Arc::new(Mutex::new(Vec::new()));
        in_scope(request(3), || {
            drop(checked(request(1), &dropped, false));
            let mut second = checked(request(2), &dropped, true);
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = second
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
            }));
            assert!(result.is_err());
            assert_eq!(current(), request(3));
            drop(second);
            assert_eq!(current(), request(3));
        });
        assert_eq!(*dropped.lock().unwrap(), vec![request(1), request(2)]);
        assert_eq!(current(), None);
    }

    #[crate::test]
    fn queue_handoff_is_explicit_not_receiver_ambient_context() {
        let (tx, rx) = std::sync::mpsc::channel();
        in_scope(request(1), || tx.send(current()).unwrap());
        in_scope(request(2), || {
            assert_eq!(current(), request(2));
            in_scope(rx.recv().unwrap(), || assert_eq!(current(), request(1)));
            assert_eq!(current(), request(2));
        });
        assert_eq!(current(), None);
    }
}
