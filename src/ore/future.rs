// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Future and stream utilities.
//!
//! This module provides future and stream combinators that are missing from
//! the [`futures`](futures) crate.

use std::fmt::{self, Debug};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::{Either, FutureExt, MapOk, TryFuture, TryFutureExt};
use futures::sink::Sink;
use futures::stream::{
    Fuse, FuturesUnordered, Stream, StreamExt, StreamFuture, TryStream, TryStreamExt,
};
use futures::{io, ready};

pub mod channel;

/// Extension methods for futures.
pub trait OreFutureExt {
    /// Wraps this future an [`Either`] future, with this future becoming the
    /// left variant.
    fn left<U>(self) -> Either<Self, U>
    where
        Self: Sized;

    /// Wraps this future in an [`Either`] future, with this future becoming the
    /// right variant.
    fn right<U>(self) -> Either<U, Self>
    where
        Self: Sized;

    /// Wrap this future in an [`Either3`] future, with this future becoming the
    /// [`Either3::A`] variant.
    fn either_a<U, V>(self) -> Either3<Self, U, V>
    where
        Self: Sized;

    /// Wraps this future in an [`Either3`] future, with this future becoming
    /// the [`Either3::B`] variant.
    fn either_b<U, V>(self) -> Either3<U, Self, V>
    where
        Self: Sized;

    /// Wraps this future in an [`Either3`] future, with this future becoming
    /// the [`Either3::C`] variant.
    fn either_c<U, V>(self) -> Either3<U, V, Self>
    where
        Self: Sized;
}

impl<T> OreFutureExt for T
where
    T: Future,
{
    fn left<U>(self) -> Either<T, U> {
        Either::Left(self)
    }

    fn right<U>(self) -> Either<U, T> {
        Either::Right(self)
    }

    fn either_a<U, V>(self) -> Either3<T, U, V> {
        Either3::A(self)
    }

    fn either_b<U, V>(self) -> Either3<U, T, V> {
        Either3::B(self)
    }

    fn either_c<U, V>(self) -> Either3<U, V, T> {
        Either3::C(self)
    }
}

/// Extension methods for [`Result`]-returning futures.
pub trait OreTryFutureExt: TryFuture {
    /// Discards the successful result of this future by producing unit instead.
    /// Errors are passed through.
    fn discard(self) -> MapOk<Self, fn(Self::Ok) -> ()>
    where
        Self: Sized + Future;
}

impl<T> OreTryFutureExt for T
where
    T: TryFuture,
{
    fn discard(self) -> MapOk<Self, fn(T::Ok) -> ()> {
        self.map_ok(discard)
    }
}

fn discard<T>(_: T) {}

/// Combines three different futures yielding the same item and error types into
/// a single concrete type.
///
/// Like [`futures::future::Either`], but for three types instead of two.
#[derive(Debug)]
pub enum Either3<A, B, C> {
    /// The first variant of the type.
    A(A),
    /// The second variant of the type.
    B(B),
    /// The third variant of the type.
    C(C),
}

impl<A, B, C> Future for Either3<A, B, C>
where
    A: Future,
    B: Future<Output = A::Output>,
    C: Future<Output = A::Output>,
{
    type Output = A::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<A::Output> {
        // It is safe to project enum variants here because we promise not to
        // move out of any of the variants. Based on the `Either` type in the
        // futures crate.
        // See: https://github.com/rust-lang/futures-rs/blob/06098e452/futures-util/src/future/either.rs#L59-L67
        unsafe {
            match self.get_unchecked_mut() {
                Either3::A(a) => Pin::new_unchecked(a).poll(cx),
                Either3::B(b) => Pin::new_unchecked(b).poll(cx),
                Either3::C(c) => Pin::new_unchecked(c).poll(cx),
            }
        }
    }
}

/// Extension methods for streams.
pub trait OreStreamExt: Stream {
    /// Discards all items produced by the stream.
    ///
    /// The returned future will resolve successfully when the entire stream is
    /// exhausted.
    fn drain(self) -> Drain<Self>
    where
        Self: Sized,
    {
        Drain(self)
    }

    /// Flattens a stream of streams into one continuous stream, but does not
    /// exhaust each incoming stream before moving on to the next.
    ///
    /// In other words, this is a combination of [`Stream::flatten`] and
    /// [`Stream::select`]. The streams may be interleaved in any order, but
    /// the ordering within one of the underlying streams is preserved.
    fn select_flatten(self) -> SelectFlatten<Self>
    where
        Self: Stream + Sized,
        Self::Item: Stream + Unpin,
    {
        SelectFlatten {
            incoming_streams: self.fuse(),
            active_streams: FuturesUnordered::new(),
        }
    }
}

impl<S: Stream> OreStreamExt for S {}

/// Extension methods for [`Result`]-producing streams.
pub trait OreTryStreamExt: TryStream {
    /// Returns the next element of the stream or EOF.
    ///
    /// This is like [`Stream::try_next`], but `try_recv` treats EOF as an
    /// error, and so does not need to wrap the next item in an option type.
    fn try_recv(&mut self) -> TryRecv<'_, Self>
    where
        Self: TryStream + Unpin + Sized,
        Self::Error: From<io::Error>,
    {
        TryRecv(self)
    }
}

impl<S: TryStream> OreTryStreamExt for S {}

/// The stream returned by [`OreStreamExt::drain`].
#[derive(Debug)]
pub struct Drain<S>(S);

impl<S> Future for Drain<S>
where
    S: Stream + Unpin,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        while let Some(_) = ready!(Pin::new(&mut self.0).poll_next(cx)) {}
        Poll::Ready(())
    }
}

/// The stream returned by [`StreamExt::select_flatten`].
#[derive(Debug)]
pub struct SelectFlatten<S>
where
    S: Stream,
{
    /// The stream of incoming streams.
    incoming_streams: Fuse<S>,
    /// The set of currently active streams that have been received from
    /// `incoming_streams`. Streams are removed from the set when they are
    /// closed.
    active_streams: FuturesUnordered<StreamFuture<S::Item>>,
}

impl<S> Stream for SelectFlatten<S>
where
    S: Stream + Unpin,
    S::Item: Stream + Unpin,
{
    type Item = <S::Item as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // First, drain the incoming stream queue.
        while let Poll::Ready(Some(stream)) = self.incoming_streams.poll_next_unpin(cx) {
            // New stream available. Add it to the set of active streams.
            self.active_streams.push(stream.into_future())
        }

        // Second, try to find an item from a ready stream.
        loop {
            match self.active_streams.poll_next_unpin(cx) {
                Poll::Ready(Some((Some(item), stream))) => {
                    // An active stream yielded an item. Arrange to receive the
                    // next item from the stream, then propagate the received
                    // item.
                    self.active_streams.push(stream.into_future());
                    return Poll::Ready(Some(item));
                }
                Poll::Ready(Some((None, _stream))) => {
                    // An active stream yielded a `None`, which means it has
                    // terminated. Drop it on the floor. Then go around the loop
                    // to see if another stream is ready.
                }
                Poll::Ready(None) => {
                    if self.incoming_streams.is_done() {
                        // There are no remaining active streams, and our
                        // incoming stream queue is done too. We're good and
                        // truly finished, so propagate the termination event.
                        return Poll::Ready(None);
                    } else {
                        // There are no remaining active streams, but we might
                        // yet get another stream from the incoming stream
                        // queue. Indicate that we're not yet ready.
                        return Poll::Pending;
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// The future returned by [`StreamExt::try_recv`].
#[derive(Debug)]
pub struct TryRecv<'a, S>(&'a mut S);

impl<'a, S> Future for TryRecv<'a, S>
where
    S: TryStream + Unpin,
    S::Error: From<io::Error>,
{
    type Output = Result<S::Ok, S::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match ready!(self.0.try_poll_next_unpin(cx)) {
            Some(Ok(r)) => Poll::Ready(Ok(r)),
            Some(Err(err)) => Poll::Ready(Err(err)),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            )
            .into())),
        }
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

    /// Like [`SinkExt::send`], but does not flush the sink after enqueuing
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
/// [`futures::Sink::fanout`].
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

/// Either a future or an immediately available value
pub enum MaybeFuture<'a, T: Unpin + Debug> {
    /// An immediately available value. Will be `Some` unless
    /// `poll` has been called.
    Immediate(Option<T>),
    /// A computation producing the value.
    Future(Pin<Box<dyn Future<Output = T> + 'a + Send>>),
}

impl<'a, T: Unpin + fmt::Debug> fmt::Debug for MaybeFuture<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Immediate(op) => write!(f, "Immediate({:?})", op),
            Self::Future(_) => write!(f, "Future(...)"),
        }
    }
}

impl<'a, T: Unpin + Debug> From<T> for MaybeFuture<'a, T> {
    fn from(t: T) -> Self {
        Self::Immediate(Some(t))
    }
}

impl<'a, T: Unpin + Debug + 'a> MaybeFuture<'a, T> {
    /// Apply a function to the underlying value
    /// (possibly after the future completes)
    pub fn map<F, R: Unpin + Debug>(self, f: F) -> MaybeFuture<'a, R>
    where
        F: FnOnce(T) -> R + 'static + Send,
    {
        match self {
            MaybeFuture::Immediate(t) => MaybeFuture::Immediate(t.map(f)),
            MaybeFuture::Future(fut) => {
                let fut = Box::pin(fut.map(f));
                MaybeFuture::Future(fut)
            }
        }
    }
}

impl<'a, T: Unpin + Debug> Future for MaybeFuture<'a, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::into_inner(self) {
            Self::Immediate(t) => Poll::Ready(t.take().unwrap()),
            Self::Future(fut) => Pin::new(fut).poll(cx),
        }
    }
}
