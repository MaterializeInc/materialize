// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Future and stream utilities.
//!
//! This module provides future and stream combinators that are missing from
//! the [`futures`](futures) crate.

use futures::future::{Either, Map};
use futures::try_ready;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend, Stream};
use std::io;
use std::marker::PhantomData;

pub mod sync;

/// Extension methods for futures.
pub trait FutureExt {
    /// Boxes this future.
    fn boxed(self) -> Box<dyn Future<Item = Self::Item, Error = Self::Error> + Send>
    where
        Self: Future + Send + 'static;

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

    /// Discards the successful result of this future by producing unit instead.
    /// Errors are passed through.
    fn discard(self) -> Map<Self, fn(Self::Item) -> ()>
    where
        Self: Sized + Future;

    /// Wraps this future in a future that will abort the underlying future if
    /// `signal` completes. In other words, allows the underlying future to
    /// be canceled.
    fn watch_for_cancel<S>(self, signal: S) -> Cancelable<Self, S>
    where
        Self: Sized + Future<Item = ()>,
        S: Future<Item = ()>;
}

impl<T> FutureExt for T
where
    T: Future,
{
    fn boxed(self) -> Box<dyn Future<Item = T::Item, Error = T::Error> + Send>
    where
        T: Send + 'static,
    {
        Box::new(self)
    }

    fn left<U>(self) -> Either<T, U> {
        Either::A(self)
    }

    fn right<U>(self) -> Either<U, T> {
        Either::B(self)
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

    fn discard(self) -> Map<Self, fn(T::Item) -> ()> {
        self.map(discard)
    }

    fn watch_for_cancel<S>(self, signal: S) -> Cancelable<Self, S> {
        Cancelable {
            future: self,
            signal,
        }
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
    B: Future<Item = A::Item, Error = A::Error>,
    C: Future<Item = A::Item, Error = A::Error>,
{
    type Item = A::Item;
    type Error = A::Error;

    fn poll(&mut self) -> Poll<A::Item, A::Error> {
        match *self {
            Either3::A(ref mut a) => a.poll(),
            Either3::B(ref mut b) => b.poll(),
            Either3::C(ref mut c) => c.poll(),
        }
    }
}

/// The future returned by [`FutureExt::watch_for_cancel`].
#[derive(Debug)]
pub struct Cancelable<F, S> {
    future: F,
    signal: S,
}

impl<F, S> Future for Cancelable<F, S>
where
    F: Future<Item = ()>,
    S: Future<Item = ()>,
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.signal.poll() {
            Ok(Async::Ready(())) | Err(_) => Ok(Async::Ready(())),
            Ok(Async::NotReady) => self.future.poll(),
        }
    }
}

/// Extension methods for streams.
pub trait StreamExt: Stream {
    /// Discards all items produced by the stream.
    ///
    /// The returned future will resolve successfully when the entire stream is
    /// exhausted, or resolve with an error, if the stream returns an error.
    fn drain(self) -> Drain<Self>
    where
        Self: Sized,
    {
        Drain(self)
    }

    /// Consumes this stream, returning an future that resolves with the pair
    /// of the next element of the stream and the remaining stream.
    ///
    /// This is like [`Stream::into_future`]. There are two reasons to prefer
    /// this method:
    ///
    ///   1. `into_future` is a terrible name. `recv` is far more descriptive
    ///      and discoverable, and is symmetric with
    ///      [`Sink::send`](futures::sink::Sink::send).
    ///
    ///   2. `recv` treats EOF as an error, and so does not need to wrap the
    ///      next item in an option type. Specifically, `into_future` has an
    ///      item type of `(Option<S::Item>, S)`, while `recv` has an item type
    ///      of `(S::Item, S)`. If EOF will not be handled differently than
    ///      any other exceptional condition, callers of `into_future` will need
    ///      to write more boilerplate.
    fn recv(self) -> Recv<Self>
    where
        Self: Stream<Error = io::Error>,
        Self: Sized,
    {
        Recv { inner: Some(self) }
    }
}

impl<S: Stream> StreamExt for S {}

/// The stream returned by [`StreamExt::drain`].
#[derive(Debug)]
pub struct Drain<S>(S);

impl<S> Future for Drain<S>
where
    S: Stream,
{
    type Item = ();
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Some(_) = try_ready!(self.0.poll()) {}
        Ok(Async::Ready(()))
    }
}

/// The future returned by [`StreamExt::recv`].
#[derive(Debug)]
pub struct Recv<S> {
    inner: Option<S>,
}

impl<S: Stream<Error = io::Error>> Future for Recv<S> {
    type Item = (S::Item, S);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let item = {
            let s = self.inner.as_mut().expect("polling Recv twice");
            match s.poll() {
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(Some(r))) => Ok(r),
                Ok(Async::Ready(None)) => Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected eof",
                )),
                Err(e) => Err(e),
            }
        };
        let stream = self.inner.take().unwrap();
        item.map(|v| Async::Ready((v, stream)))
    }
}

/// Constructs a sink that consumes its input and sends it nowhere.
pub fn dev_null<T, E>() -> DevNull<T, E> {
    DevNull(PhantomData, PhantomData)
}

/// A sink that consumes its input and sends it nowhere.
///
/// Primarily useful as a base sink when folding multiple sinks into one using
/// [`futures::Stream::fanout`].
#[derive(Debug)]
pub struct DevNull<T, E>(PhantomData<T>, PhantomData<E>);

impl<T, E> Sink for DevNull<T, E> {
    type SinkItem = T;
    type SinkError = E;

    fn start_send(&mut self, _: T) -> StartSend<Self::SinkItem, Self::SinkError> {
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}
