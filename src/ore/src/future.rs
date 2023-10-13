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

//! Future and stream utilities.
//!
//! This module provides future and stream combinators that are missing from
//! the [`futures`](futures) crate.

use std::any::Any;
use std::error::Error;
use std::fmt::{self, Debug};
use std::future::Future;
use std::marker::PhantomData;
use std::panic::UnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::{CatchUnwind, FutureExt};
use futures::sink::Sink;
use pin_project::pin_project;
use tokio::task::futures::TaskLocalFuture;
use tokio::time::{self, Duration, Instant};

use crate::panic::CATCHING_UNWIND_ASYNC;
use crate::task;

/// Extension methods for futures.
pub trait OreFutureExt {
    /// Wraps a future in a [`SpawnIfCanceled`] future, which will spawn a
    /// task to poll the inner future to completion if it is dropped.
    fn spawn_if_canceled<Name, NameClosure>(
        self,
        nc: NameClosure,
    ) -> SpawnIfCanceled<Self::Output, Name, NameClosure>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name + Unpin,
        Self: Future + Send + 'static,
        Self::Output: Send + 'static;

    /// Like [`FutureExt::catch_unwind`], but can unwind panics even if
    /// [`set_abort_on_panic`] has been called.
    ///
    /// [`set_abort_on_panic`]: crate::panic::set_abort_on_panic
    fn ore_catch_unwind(self) -> OreCatchUnwind<Self>
    where
        Self: Sized + UnwindSafe;
}

impl<T> OreFutureExt for T
where
    T: Future,
{
    fn spawn_if_canceled<Name, NameClosure>(
        self,
        nc: NameClosure,
    ) -> SpawnIfCanceled<T::Output, Name, NameClosure>
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name + Unpin,
        T: Send + 'static,
        T::Output: Send + 'static,
    {
        SpawnIfCanceled {
            inner: Some(Box::pin(self)),
            nc: Some(nc),
        }
    }

    fn ore_catch_unwind(self) -> OreCatchUnwind<Self>
    where
        Self: UnwindSafe,
    {
        OreCatchUnwind {
            #[allow(clippy::disallowed_methods)]
            inner: CATCHING_UNWIND_ASYNC.scope(true, FutureExt::catch_unwind(self)),
        }
    }
}

/// The future returned by [`OreFutureExt::spawn_if_canceled`].
pub struct SpawnIfCanceled<T, Name, NameClosure>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name + Unpin,
    T: Send + 'static,
{
    inner: Option<Pin<Box<dyn Future<Output = T> + Send>>>,
    nc: Option<NameClosure>,
}

impl<T, Name, NameClosure> Future for SpawnIfCanceled<T, Name, NameClosure>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name + Unpin,
    T: Send + 'static,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<T> {
        match &mut self.inner {
            None => panic!("SpawnIfCanceled polled after completion"),
            Some(f) => match f.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(res) => {
                    self.inner = None;
                    Poll::Ready(res)
                }
            },
        }
    }
}

impl<T, Name, NameClosure> Drop for SpawnIfCanceled<T, Name, NameClosure>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name + Unpin,
    T: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(f) = self.inner.take() {
            task::spawn(
                || format!("spawn_if_canceled:{}", (self.nc).take().unwrap()().as_ref()),
                f,
            );
        }
    }
}

impl<T, Name, NameClosure> fmt::Debug for SpawnIfCanceled<T, Name, NameClosure>
where
    Name: AsRef<str>,
    NameClosure: FnOnce() -> Name + Unpin,
    T: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SpawnIfCanceled")
            .field(
                "inner",
                match &self.inner {
                    None => &"None",
                    Some(_) => &"Some(<future>)",
                },
            )
            .finish()
    }
}

/// The future returned by [`OreFutureExt::ore_catch_unwind`].
#[derive(Debug)]
#[pin_project]
pub struct OreCatchUnwind<Fut> {
    #[pin]
    inner: TaskLocalFuture<bool, CatchUnwind<Fut>>,
}

impl<Fut> Future for OreCatchUnwind<Fut>
where
    Fut: Future + UnwindSafe,
{
    type Output = Result<Fut::Output, Box<dyn Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// The error returned by [`timeout`] and [`timeout_at`].
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TimeoutError<E> {
    /// The timeout deadline has elapsed.
    DeadlineElapsed,
    /// The underlying operation failed.
    Inner(E),
}

impl<E> fmt::Display for TimeoutError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeoutError::DeadlineElapsed => f.write_str("deadline has elapsed"),
            e => e.fmt(f),
        }
    }
}

impl<E> Error for TimeoutError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TimeoutError::DeadlineElapsed => None,
            TimeoutError::Inner(e) => Some(e),
        }
    }
}

/// Applies a maximum duration to a [`Result`]-returning future.
///
/// Whether the maximum duration was reached is indicated via the error type.
/// Specifically:
///
///   * If `future` does not complete within `duration`, returns
///     [`TimeoutError::DeadlineElapsed`].
///   * If `future` completes with `Ok(t)`, returns `Ok(t)`.
///   * If `future` completes with `Err(e)`, returns
///     [`TimeoutError::Inner(e)`](TimeoutError::Inner).
///
/// Using this function can be considerably more readable than
/// [`tokio::time::timeout`] when the inner future returns a `Result`.
///
/// # Examples
///
/// ```
/// # use tokio::time::Duration;
/// use mz_ore::future::TimeoutError;
/// # tokio_test::block_on(async {
/// let slow_op = async {
///     tokio::time::sleep(Duration::from_secs(1)).await;
///     Ok::<_, String>(())
/// };
/// let res = mz_ore::future::timeout(Duration::from_millis(1), slow_op).await;
/// assert_eq!(res, Err(TimeoutError::DeadlineElapsed));
/// # });
/// ```
pub async fn timeout<F, T, E>(duration: Duration, future: F) -> Result<T, TimeoutError<E>>
where
    F: Future<Output = Result<T, E>>,
{
    match time::timeout(duration, future).await {
        Ok(Ok(t)) => Ok(t),
        Ok(Err(e)) => Err(TimeoutError::Inner(e)),
        Err(_) => Err(TimeoutError::DeadlineElapsed),
    }
}

/// Applies a deadline to a [`Result`]-returning future.
///
/// Whether the deadline elapsed is indicated via the error type. Specifically:
///
///   * If `future` does not complete by `deadline`, returns
///     [`TimeoutError::DeadlineElapsed`].
///   * If `future` completes with `Ok(t)`, returns `Ok(t)`.
///   * If `future` completes with `Err(e)`, returns
///     [`TimeoutError::Inner(e)`](TimeoutError::Inner).
///
/// Using this function can be considerably more readable than
/// [`tokio::time::timeout_at`] when the inner future returns a `Result`.
///
/// # Examples
///
/// ```
/// # use tokio::time::{Duration, Instant};
/// use mz_ore::future::TimeoutError;
/// # tokio_test::block_on(async {
/// let slow_op = async {
///     tokio::time::sleep(Duration::from_secs(1)).await;
///     Ok::<_, String>(())
/// };
/// let deadline = Instant::now() + Duration::from_millis(1);
/// let res = mz_ore::future::timeout_at(deadline, slow_op).await;
/// assert_eq!(res, Err(TimeoutError::DeadlineElapsed));
/// # });
/// ```
pub async fn timeout_at<F, T, E>(deadline: Instant, future: F) -> Result<T, TimeoutError<E>>
where
    F: Future<Output = Result<T, E>>,
{
    match time::timeout_at(deadline, future).await {
        Ok(Ok(t)) => Ok(t),
        Ok(Err(e)) => Err(TimeoutError::Inner(e)),
        Err(_) => Err(TimeoutError::DeadlineElapsed),
    }
}

/// Extension methods for sinks.
pub trait OreSinkExt<T>: Sink<T> {
    /// Boxes this sink.
    fn boxed(self) -> Box<dyn Sink<T, Error = Self::Error> + Send>
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }

    /// Like [`futures::sink::SinkExt::send`], but does not flush the sink after enqueuing
    /// `item`.
    fn enqueue(&mut self, item: T) -> Enqueue<Self, T> {
        Enqueue {
            sink: self,
            item: Some(item),
        }
    }
}

impl<S, T> OreSinkExt<T> for S where S: Sink<T> {}

/// Future for the [`enqueue`](OreSinkExt::enqueue) method.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Enqueue<'a, Si, Item>
where
    Si: ?Sized,
{
    sink: &'a mut Si,
    item: Option<Item>,
}

impl<Si, Item> Future for Enqueue<'_, Si, Item>
where
    Si: Sink<Item> + Unpin + ?Sized,
    Item: Unpin,
{
    type Output = Result<(), Si::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = &mut *self;
        if let Some(item) = this.item.take() {
            let mut sink = Pin::new(&mut this.sink);
            match sink.as_mut().poll_ready(cx)? {
                Poll::Ready(()) => sink.as_mut().start_send(item)?,
                Poll::Pending => {
                    this.item = Some(item);
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

/// Constructs a sink that consumes its input and sends it nowhere.
pub fn dev_null<T, E>() -> DevNull<T, E> {
    DevNull(PhantomData, PhantomData)
}

/// A sink that consumes its input and sends it nowhere.
///
/// Primarily useful as a base sink when folding multiple sinks into one using
/// [`futures::sink::SinkExt::fanout`].
#[derive(Debug)]
pub struct DevNull<T, E>(PhantomData<T>, PhantomData<E>);

impl<T, E> Sink<T> for DevNull<T, E> {
    type Error = E;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _: T) -> Result<(), Self::Error> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
