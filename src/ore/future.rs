// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! Future and stream utilities.
//!
//! This module provides future and stream combinators that are missing from
//! the [`futures`](futures) crate.

use futures::future::{Either, Map};
use futures::{Async, Future, Poll, Stream};
use std::io;

/// Extension methods for futures.
pub trait FutureExt {
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

    /// Creates a future that will call [`into`](std::convert::Into::into) on
    /// the wrapped future's error, if the wrapped future produces an error.
    ///
    /// It is roughly equivalent to:
    ///
    /// ```ignore
    /// future.map_err(|e| e.into())
    /// ```
    fn err_into<E>(self) -> ErrInto<Self, E>
    where
        Self: Sized + Future,
        E: From<Self::Error>;

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

    fn err_into<E>(self) -> ErrInto<Self, E> {
        ErrInto(self, std::marker::PhantomData)
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

/// The future returned by [`FutureExt::err_into`].
pub struct ErrInto<T, E>(T, std::marker::PhantomData<E>);

impl<T, E> Future for ErrInto<T, E>
where
    T: Future,
    E: From<T::Error>,
{
    type Item = T::Item;
    type Error = E;

    fn poll(&mut self) -> Poll<T::Item, E> {
        match self.0.poll() {
            Ok(v) => Ok(v),
            Err(e) => Err(e.into()),
        }
    }
}

/// The future returned by [`FutureExt::watch_for_cancel`].
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
    /// Consumes this stream, returning an future that resolves with the pair
    /// of the next element of the stream and the remaining stream.
    ///
    /// This is like [`Stream::into_future`]. There are two reasons to prefer
    /// this method:
    ///
    ///   1. `into_future` is a terrible name. `recv` is far more descriptive
    ///      and discoverabl, and is symmetric with
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
        Self: Sized;
}

impl<S: Stream<Error = io::Error>> StreamExt for S {
    fn recv(self) -> Recv<Self> {
        Recv { inner: Some(self) }
    }
}

/// The future returned by [`StreamExt::recv`].
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
