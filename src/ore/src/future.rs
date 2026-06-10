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
//! the [`futures`] crate.

use std::any::Any;
use std::error::Error;
use std::fmt::{self, Debug};
use std::future::Future;
use std::marker::PhantomData;
use std::panic::UnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::Stream;
use futures::future::{CatchUnwind, FutureExt};
use futures::sink::Sink;
use pin_project::pin_project;
use tokio::task::futures::TaskLocalFuture;
use tokio::time::{self, Duration, Instant};

use crate::task;

/// Whether or not to run the future in `run_in_task_if` in a task.
#[derive(Clone, Copy, Debug)]
pub enum InTask {
    /// Run it in a task.
    Yes,
    /// Poll it normally.
    No,
}

/// Extension methods for futures.
#[async_trait::async_trait]
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

    /// Run a `'static` future in a Tokio task, naming that task, using a convenient
    /// postfix call notation.
    ///
    /// Useful in contexts where futures may be starved and cause inadvertent
    /// failures in I/O-sensitive operations, such as when called within timely
    /// operators.
    async fn run_in_task<Name, NameClosure>(self, nc: NameClosure) -> Self::Output
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name + Unpin + Send,
        Self: Future + Send + 'static,
        Self::Output: Send + 'static;

    /// The same as `run_in_task`, but allows the callee to dynamically choose whether or
    /// not the future is polled into a Tokio task.
    // This is not currently a provided method because rust-analyzer fails inference if it is :(.
    async fn run_in_task_if<Name, NameClosure>(
        self,
        in_task: InTask,
        nc: NameClosure,
    ) -> Self::Output
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name + Unpin + Send,
        Self: Future + Send + 'static,
        Self::Output: Send + 'static;

    /// Like [`FutureExt::catch_unwind`], but can unwind panics even if
    /// [`panic::install_enhanced_handler`] has been called.
    ///
    /// [`panic::install_enhanced_handler`]: crate::panic::install_enhanced_handler
    #[cfg(feature = "panic")]
    fn ore_catch_unwind(self) -> OreCatchUnwind<Self>
    where
        Self: Sized + UnwindSafe;
}

#[async_trait::async_trait]
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

    async fn run_in_task<Name, NameClosure>(self, nc: NameClosure) -> T::Output
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name + Unpin + Send,
        T: Send + 'static,
        T::Output: Send + 'static,
    {
        task::spawn(nc, self).await
    }

    async fn run_in_task_if<Name, NameClosure>(self, in_task: InTask, nc: NameClosure) -> T::Output
    where
        Name: AsRef<str>,
        NameClosure: FnOnce() -> Name + Unpin + Send,
        Self: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        if let InTask::Yes = in_task {
            self.run_in_task(nc).await
        } else {
            self.await
        }
    }

    #[cfg(feature = "panic")]
    fn ore_catch_unwind(self) -> OreCatchUnwind<Self>
    where
        Self: UnwindSafe,
    {
        use crate::panic::CATCHING_UNWIND_ASYNC;

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
            TimeoutError::Inner(e) => e.fmt(f),
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
    fn enqueue(&mut self, item: T) -> Enqueue<'_, Self, T> {
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

/// Extension methods for streams.
#[async_trait]
pub trait OreStreamExt: Stream {
    /// Awaits the stream for an event to be available and returns all currently buffered
    /// events on the stream up to some `max`.
    ///
    /// This method returns `None` if the stream has ended.
    ///
    /// If there are no events ready on the stream this method will sleep until an event is
    /// sent or the stream is closed. When woken it will return up to `max` currently buffered
    /// events.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv_many` is used as the event in a `select!` statement
    /// and some other branch completes first, it is guaranteed that no messages were received on
    /// this channel.
    async fn recv_many(&mut self, max: usize) -> Option<Vec<Self::Item>>;
}

#[async_trait]
impl<T> OreStreamExt for T
where
    T: futures::stream::Stream + futures::StreamExt + Send + Unpin,
{
    async fn recv_many(&mut self, max: usize) -> Option<Vec<Self::Item>> {
        // Wait for an event to be ready on the stream
        let first = self.next().await?;
        let mut buffer = Vec::from([first]);

        // Note(parkmycar): It's very important for cancelation safety that we don't add any more
        // .await points other than the initial one.

        // Pull all other ready events off the stream, up to the max
        while let Some(v) = self.next().now_or_never().and_then(|e| e) {
            buffer.push(v);

            // Break so we don't loop here continuously.
            if buffer.len() >= max {
                break;
            }
        }

        Some(buffer)
    }
}

/// Yield execution back to the runtime.
///
/// A snapshot of the old `tokio::task::yield_now` implementation, from before it
/// had sneaky TLS shenangans.
pub async fn yield_now() {
    struct YieldNow {
        yielded: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                return Poll::Ready(());
            }

            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    YieldNow { yielded: false }.await
}
