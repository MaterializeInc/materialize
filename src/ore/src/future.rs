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

use std::fmt::{self, Debug};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::sink::Sink;

/// Extension methods for futures.
pub trait OreFutureExt {
    /// Wraps a future in a [`SpawnIfCanceled`] future, which will spawn a
    /// task to poll the inner future to completion if it is dropped.
    fn spawn_if_canceled(self) -> SpawnIfCanceled<Self::Output>
    where
        Self: Future + Send + 'static,
        Self::Output: Send + 'static;
}

impl<T> OreFutureExt for T
where
    T: Future,
{
    fn spawn_if_canceled(self) -> SpawnIfCanceled<T::Output>
    where
        T: Send + 'static,
        T::Output: Send + 'static,
    {
        SpawnIfCanceled {
            inner: Some(Box::pin(self)),
        }
    }
}

/// The future returned by [`OreFutureExt::spawn_if_canceled`].
pub struct SpawnIfCanceled<T>
where
    T: Send + 'static,
{
    inner: Option<Pin<Box<dyn Future<Output = T> + Send>>>,
}

impl<T> Future for SpawnIfCanceled<T>
where
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

impl<T> Drop for SpawnIfCanceled<T>
where
    T: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(f) = self.inner.take() {
            tokio::spawn(f);
        }
    }
}

impl<T> fmt::Debug for SpawnIfCanceled<T>
where
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
