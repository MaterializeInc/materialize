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

//! Retry utilities.
//!
//! This module provides an API for retrying fallible asynchronous operations
//! until they succeed or until some criteria for giving up has been reached,
//! using exponential backoff between retries.
//!
//! # Examples
//!
//! Retry a contrived fallible operation until it succeeds:
//!
//! ```
//! use std::time::Duration;
//! use mz_ore::retry::Retry;
//!
//! let res = Retry::default().retry(|state| {
//!    if state.i == 3 {
//!        Ok(())
//!    } else {
//!        Err("contrived failure")
//!    }
//! });
//! assert_eq!(res, Ok(()));
//! ```
//!
//! Limit the number of retries such that success is never observed:
//!
//! ```
//! use std::time::Duration;
//! use mz_ore::retry::Retry;
//!
//! let res = Retry::default().max_tries(2).retry(|state| {
//!    if state.i == 3 {
//!        Ok(())
//!    } else {
//!        Err("contrived failure")
//!    }
//! });
//! assert_eq!(res, Err("contrived failure"));
//! ```

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{cmp, thread};

use futures::{ready, Stream, StreamExt};
use pin_project::pin_project;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::error::Elapsed;
use tokio::time::{self, Duration, Instant, Sleep};

/// The state of a retry operation constructed with [`Retry`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryState {
    /// The retry counter, starting from zero on the first try.
    pub i: usize,
    /// The duration that the retry operation will sleep for before the next
    /// retry if this try fails.
    ///
    /// If this is the last attempt, then this field will be `None`.
    pub next_backoff: Option<Duration>,
}

/// The result of a retryable operation.
#[derive(Debug)]
pub enum RetryResult<T, E> {
    /// The operation was successful and does not need to be retried.
    Ok(T),
    /// The operation was unsuccessful but can be retried.
    RetryableErr(E),
    /// The operation was unsuccessful but cannot be retried.
    FatalErr(E),
}

impl<T, E> From<Result<T, E>> for RetryResult<T, E> {
    fn from(res: Result<T, E>) -> RetryResult<T, E> {
        match res {
            Ok(t) => RetryResult::Ok(t),
            Err(e) => RetryResult::RetryableErr(e),
        }
    }
}

/// Configures a retry operation.
///
/// See the [module documentation](self) for usage examples.
#[pin_project]
#[derive(Debug)]
pub struct Retry {
    initial_backoff: Duration,
    factor: f64,
    clamp_backoff: Duration,
    max_duration: Duration,
    max_tries: usize,
}

impl Retry {
    /// Sets the initial backoff for the retry operation.
    ///
    /// The initial backoff is the amount of time to wait if the first try
    /// fails.
    pub fn initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }

    /// Clamps the maximum backoff for the retry operation.
    ///
    /// The maximum backoff is the maximum amount of time to wait between tries.
    pub fn clamp_backoff(mut self, clamp_backoff: Duration) -> Self {
        self.clamp_backoff = clamp_backoff;
        self
    }

    /// Sets the exponential backoff factor for the retry operation.
    ///
    /// The time to wait is multiplied by this factor after each failed try. The
    /// default factor is two.
    pub fn factor(mut self, factor: f64) -> Self {
        self.factor = factor;
        self
    }

    /// Sets the maximum number of tries.
    ///
    /// If the operation is still failing after `max_tries`, then
    /// [`retry`](Retry::retry) will return the last error.
    ///
    /// Calls to `max_tries` will override any previous calls to `max_tries`.
    ////
    /// # Panics
    ///
    /// Panics if `max_tries` is zero.
    pub fn max_tries(mut self, max_tries: usize) -> Self {
        if max_tries == 0 {
            panic!("max tries must be greater than zero");
        }
        self.max_tries = max_tries;
        self
    }

    /// Sets the maximum duration.
    ///
    /// If the operation is still failing after the specified `duration`, then
    /// the operation will be retried once more and [`retry`](Retry::retry) will
    /// return the last error.
    ///
    /// Calls to `max_duration` will override any previous calls to
    /// `max_duration`.
    pub fn max_duration(mut self, duration: Duration) -> Self {
        self.max_duration = duration;
        self
    }

    /// Retries the fallible operation `f` according to the configured policy.
    ///
    /// The `retry` method invokes `f` repeatedly until it returns either
    /// [`RetryResult::Ok`] or [`RetryResult::FatalErr`], or until the maximum
    /// duration or tries have been reached, as configured via
    /// [`max_duration`](Retry::max_duration) or
    /// [`max_tries`](Retry::max_tries). If the last invocation of `f` returns
    /// `RetryResult::Ok(t)`, then `retry` returns `Ok(t)`. If the last
    /// invocation of `f` returns `RetryResult::RetriableErr(e)` or
    /// `RetryResult::FatalErr(e)`, then `retry` returns `Err(e)`.
    ///
    /// As a convenience, `f` can return any type that is convertible to a
    /// `RetryResult`. The conversion from [`Result`] to `RetryResult` converts
    /// `Err(e)` to `RetryResult::RetryableErr(e)`, that is, it considers all
    /// errors retryable.
    ///
    /// After the first failure, `retry` sleeps for the initial backoff
    /// configured via [`initial_backoff`](Retry::initial_backoff). After each
    /// successive failure, `retry` sleeps for twice the last backoff. If the
    /// backoff would ever exceed the maximum backoff configured viq
    /// [`Retry::clamp_backoff`], then the backoff is clamped to the specified
    /// maximum.
    ///
    /// The operation does not attempt to forcibly time out `f`, even if there
    /// is a maximum duration. If there is the possibility of `f` blocking
    /// forever, consider adding a timeout internally.
    pub fn retry<F, R, T, E>(self, mut f: F) -> Result<T, E>
    where
        F: FnMut(RetryState) -> R,
        R: Into<RetryResult<T, E>>,
    {
        let start = Instant::now();
        let mut i = 0;
        let mut next_backoff = Some(cmp::min(self.initial_backoff, self.clamp_backoff));
        loop {
            let elapsed = start.elapsed();
            if elapsed > self.max_duration || i + 1 >= self.max_tries {
                next_backoff = None;
            } else if elapsed + next_backoff.unwrap() > self.max_duration {
                next_backoff = Some(self.max_duration - elapsed);
            }
            let state = RetryState { i, next_backoff };
            match f(state).into() {
                RetryResult::Ok(t) => return Ok(t),
                RetryResult::FatalErr(e) => return Err(e),
                RetryResult::RetryableErr(e) => match &mut next_backoff {
                    None => return Err(e),
                    Some(next_backoff) => {
                        thread::sleep(*next_backoff);
                        *next_backoff =
                            cmp::min(next_backoff.mul_f64(self.factor), self.clamp_backoff);
                    }
                },
            }
            i += 1;
        }
    }

    /// Like [`Retry::retry`] but for asynchronous operations.
    pub async fn retry_async<F, U, R, T, E>(self, mut f: F) -> Result<T, E>
    where
        F: FnMut(RetryState) -> U,
        U: Future<Output = R>,
        R: Into<RetryResult<T, E>>,
    {
        let stream = self.into_retry_stream();
        tokio::pin!(stream);
        let mut err = None;
        while let Some(state) = stream.next().await {
            match f(state).await.into() {
                RetryResult::Ok(v) => return Ok(v),
                RetryResult::FatalErr(e) => return Err(e),
                RetryResult::RetryableErr(e) => err = Some(e),
            }
        }
        Err(err.expect("retry produces at least one element"))
    }

    /// Like [`Retry::retry_async`] but the operation will be canceled if the
    /// maximum duration is reached.
    ///
    /// Specifically, if the maximum duration is reached, the operation `f` will
    /// be forcibly canceled by dropping it. Canceling `f` can be surprising if
    /// the operation is not programmed to expect the possibility of not
    /// resuming from an `await` point; if you wish to always run `f` to
    /// completion, use [`Retry::retry_async`] instead.
    ///
    /// If `f` is forcibly canceled, the error returned will be the error
    /// returned by the prior invocation of `f`. If there is no prior invocation
    /// of `f`, then an `Elapsed` error is returned. The idea is that if `f`
    /// fails three times in a row with a useful error message, and then the
    /// fourth attempt is canceled because the timeout is reached, the caller
    /// would rather see the useful error message from the third attempt, rather
    /// than the "deadline exceeded" message from the fourth attempt.
    pub async fn retry_async_canceling<F, U, T, E>(self, mut f: F) -> Result<T, E>
    where
        F: FnMut(RetryState) -> U,
        U: Future<Output = Result<T, E>>,
        E: From<Elapsed> + std::fmt::Debug,
    {
        let start = Instant::now();
        let max_duration = self.max_duration;
        let stream = self.into_retry_stream();
        tokio::pin!(stream);
        let mut err = None;
        while let Some(state) = stream.next().await {
            let fut = time::timeout(max_duration.saturating_sub(start.elapsed()), f(state));
            match fut.await {
                Ok(Ok(t)) => return Ok(t),
                Ok(Err(e)) => err = Some(e),
                Err(e) => return Err(err.unwrap_or_else(|| e.into())),
            }
        }
        Err(err.expect("retry produces at least one element"))
    }

    /// Convert into [`RetryStream`]
    pub fn into_retry_stream(self) -> RetryStream {
        RetryStream {
            retry: self,
            start: Instant::now(),
            i: 0,
            next_backoff: None,
            sleep: time::sleep(Duration::default()),
        }
    }
}

impl Default for Retry {
    /// Constructs a retry operation that will retry forever with backoff
    /// defaults that are reasonable for a fallible network operation.
    fn default() -> Self {
        Retry {
            initial_backoff: Duration::from_millis(125),
            factor: 2.0,
            clamp_backoff: Duration::MAX,
            max_tries: usize::MAX,
            max_duration: Duration::MAX,
        }
    }
}

/// Opaque type representing the stream of retries that continues to back off.
#[pin_project]
#[derive(Debug)]
pub struct RetryStream {
    retry: Retry,
    start: Instant,
    i: usize,
    next_backoff: Option<Duration>,
    #[pin]
    sleep: Sleep,
}

impl RetryStream {
    fn reset(self: Pin<&mut Self>) {
        let this = self.project();
        *this.start = Instant::now();
        *this.i = 0;
        *this.next_backoff = None;
    }
}

impl Stream for RetryStream {
    type Item = RetryState;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let retry = this.retry;

        match this.next_backoff {
            None if *this.i == 0 => {
                *this.next_backoff = Some(cmp::min(retry.initial_backoff, retry.clamp_backoff));
            }
            None => return Poll::Ready(None),
            Some(next_backoff) => {
                ready!(this.sleep.as_mut().poll(cx));
                *next_backoff = cmp::min(next_backoff.mul_f64(retry.factor), retry.clamp_backoff);
            }
        }

        let elapsed = this.start.elapsed();
        if elapsed > retry.max_duration || *this.i + 1 >= retry.max_tries {
            *this.next_backoff = None;
        } else if elapsed + this.next_backoff.unwrap() > retry.max_duration {
            *this.next_backoff = Some(retry.max_duration - elapsed);
        }

        let state = RetryState {
            i: *this.i,
            next_backoff: *this.next_backoff,
        };
        if let Some(d) = *this.next_backoff {
            this.sleep.reset(Instant::now() + d);
        }
        *this.i += 1;
        Poll::Ready(Some(state))
    }
}

/// Wrapper of a `Reader` factory that will automatically retry and resume reading an underlying
/// resource in the events of errors according to a retry schedule.
#[pin_project]
#[derive(Debug)]
pub struct RetryReader<F, U, R> {
    factory: F,
    offset: usize,
    error: Option<std::io::Error>,
    #[pin]
    retry: RetryStream,
    #[pin]
    state: RetryReaderState<U, R>,
}

#[pin_project(project = RetryReaderStateProj)]
#[derive(Debug)]
enum RetryReaderState<U, R> {
    Waiting,
    Creating(#[pin] U),
    Reading(#[pin] R),
}

impl<F, U, R> RetryReader<F, U, R>
where
    F: FnMut(RetryState, usize) -> U,
    U: Future<Output = Result<R, std::io::Error>>,
    R: AsyncRead,
{
    /// Uses the provided `Reader` factory to construct a `RetryReader` with the default `Retry`
    /// settings.
    ///
    /// The factory will be called once at the beginning and subsequently every time a retry
    /// attempt is made. The factory will be called with a single `usize` argument representing the
    /// offset at which the returned `Reader` should resume reading from.
    pub fn new(factory: F) -> Self {
        Self::with_retry(factory, Retry::default())
    }

    /// Uses the provided `Reader` factory to construct a `RetryReader` with the passed `Retry`
    /// settings. See the documentation of [RetryReader::new] for more details.
    pub fn with_retry(factory: F, retry: Retry) -> Self {
        Self {
            factory,
            offset: 0,
            error: None,
            retry: retry.into_retry_stream(),
            state: RetryReaderState::Waiting,
        }
    }
}

impl<F, U, R> AsyncRead for RetryReader<F, U, R>
where
    F: FnMut(RetryState, usize) -> U,
    U: Future<Output = Result<R, std::io::Error>>,
    R: AsyncRead,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        loop {
            let mut this = self.as_mut().project();
            use RetryReaderState::*;
            match this.state.as_mut().project() {
                RetryReaderStateProj::Waiting => match ready!(this.retry.as_mut().poll_next(cx)) {
                    None => {
                        return Poll::Ready(Err(this
                            .error
                            .take()
                            .expect("retry produces at least one element")))
                    }
                    Some(state) => {
                        this.state
                            .set(Creating((*this.factory)(state, *this.offset)));
                    }
                },
                RetryReaderStateProj::Creating(reader_fut) => match ready!(reader_fut.poll(cx)) {
                    Ok(reader) => {
                        this.state.set(Reading(reader));
                    }
                    Err(err) => {
                        *this.error = Some(err);
                        this.state.set(Waiting);
                    }
                },
                RetryReaderStateProj::Reading(reader) => {
                    let filled_end = buf.filled().len();
                    match ready!(reader.poll_read(cx, buf)) {
                        Ok(()) => {
                            if let Some(_) = this.error.take() {
                                this.retry.reset();
                            }
                            *this.offset += buf.filled().len() - filled_end;
                            return Poll::Ready(Ok(()));
                        }
                        Err(err) => {
                            *this.error = Some(err);
                            this.state.set(Waiting);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, bail};

    use super::*;

    #[mz_test_macro::test]
    fn test_retry_success() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .retry(|state| {
                states.push(state);
                if state.i == 2 {
                    Ok(())
                } else {
                    Err::<(), _>("injected")
                }
            });
        assert_eq!(res, Ok(()));
        assert_eq!(
            states,
            &[
                RetryState {
                    i: 0,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 1,
                    next_backoff: Some(Duration::from_millis(2))
                },
                RetryState {
                    i: 2,
                    next_backoff: Some(Duration::from_millis(4))
                },
            ]
        );
    }

    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_async_success() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .retry_async(|state| {
                states.push(state);
                async move {
                    if state.i == 2 {
                        Ok(())
                    } else {
                        Err::<(), _>("injected")
                    }
                }
            })
            .await;
        assert_eq!(res, Ok(()));
        assert_eq!(
            states,
            &[
                RetryState {
                    i: 0,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 1,
                    next_backoff: Some(Duration::from_millis(2))
                },
                RetryState {
                    i: 2,
                    next_backoff: Some(Duration::from_millis(4))
                },
            ]
        );
    }

    #[mz_test_macro::test(tokio::test)]
    async fn test_retry_fatal() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .retry(|state| {
                states.push(state);
                if state.i == 0 {
                    RetryResult::RetryableErr::<(), _>("retry me")
                } else {
                    RetryResult::FatalErr("injected")
                }
            });
        assert_eq!(res, Err("injected"));
        assert_eq!(
            states,
            &[
                RetryState {
                    i: 0,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 1,
                    next_backoff: Some(Duration::from_millis(2))
                },
            ]
        );
    }

    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_async_fatal() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .retry_async(|state| {
                states.push(state);
                async move {
                    if state.i == 0 {
                        RetryResult::RetryableErr::<(), _>("retry me")
                    } else {
                        RetryResult::FatalErr("injected")
                    }
                }
            })
            .await;
        assert_eq!(res, Err("injected"));
        assert_eq!(
            states,
            &[
                RetryState {
                    i: 0,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 1,
                    next_backoff: Some(Duration::from_millis(2))
                },
            ]
        );
    }

    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_fail_max_tries() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .max_tries(3)
            .retry(|state| {
                states.push(state);
                Err::<(), _>("injected")
            });
        assert_eq!(res, Err("injected"));
        assert_eq!(
            states,
            &[
                RetryState {
                    i: 0,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 1,
                    next_backoff: Some(Duration::from_millis(2))
                },
                RetryState {
                    i: 2,
                    next_backoff: None
                },
            ]
        );
    }

    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_async_fail_max_tries() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .max_tries(3)
            .retry_async(|state| {
                states.push(state);
                async { Err::<(), _>("injected") }
            })
            .await;
        assert_eq!(res, Err("injected"));
        assert_eq!(
            states,
            &[
                RetryState {
                    i: 0,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 1,
                    next_backoff: Some(Duration::from_millis(2))
                },
                RetryState {
                    i: 2,
                    next_backoff: None
                },
            ]
        );
    }

    #[mz_test_macro::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    fn test_retry_fail_max_duration() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(5))
            .max_duration(Duration::from_millis(10))
            .retry(|state| {
                states.push(state);
                Err::<(), _>("injected")
            });
        assert_eq!(res, Err("injected"));

        // The first try should indicate a next backoff of exactly 5ms.
        assert_eq!(
            states[0],
            RetryState {
                i: 0,
                next_backoff: Some(Duration::from_millis(5))
            },
        );

        // The next try should indicate a next backoff of between 0 and 5ms. The
        // exact value depends on how long it took for the first try itself to
        // execute.
        assert_eq!(states[1].i, 1);
        let backoff = states[1].next_backoff.unwrap();
        assert!(backoff > Duration::from_millis(0) && backoff < Duration::from_millis(5));

        // The final try should indicate that the operation is complete with
        // a next backoff of None.
        assert_eq!(
            states[2],
            RetryState {
                i: 2,
                next_backoff: None,
            },
        );
    }

    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_async_fail_max_duration() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(5))
            .max_duration(Duration::from_millis(10))
            .retry_async(|state| {
                states.push(state);
                async { Err::<(), _>("injected") }
            })
            .await;
        assert_eq!(res, Err("injected"));

        // The first try should indicate a next backoff of exactly 5ms.
        assert_eq!(
            states[0],
            RetryState {
                i: 0,
                next_backoff: Some(Duration::from_millis(5))
            },
        );

        // The next try should indicate a next backoff of between 0 (None) and 5ms. The
        // exact value depends on how long it took for the first try itself to
        // execute.
        assert_eq!(states[1].i, 1);
        assert!(match states[1].next_backoff {
            None => true,
            Some(backoff) =>
                backoff > Duration::from_millis(0) && backoff < Duration::from_millis(5),
        });

        // The final try should indicate that the operation is complete with
        // a next backoff of None.
        assert_eq!(
            states[2],
            RetryState {
                i: 2,
                next_backoff: None,
            },
        );
    }

    #[mz_test_macro::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    fn test_retry_fail_clamp_backoff() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .clamp_backoff(Duration::from_millis(1))
            .max_tries(4)
            .retry(|state| {
                states.push(state);
                Err::<(), _>("injected")
            });
        assert_eq!(res, Err("injected"));
        assert_eq!(
            states,
            &[
                RetryState {
                    i: 0,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 1,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 2,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 3,
                    next_backoff: None
                },
            ]
        );
    }

    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_async_fail_clamp_backoff() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .clamp_backoff(Duration::from_millis(1))
            .max_tries(4)
            .retry_async(|state| {
                states.push(state);
                async { Err::<(), _>("injected") }
            })
            .await;
        assert_eq!(res, Err("injected"));
        assert_eq!(
            states,
            &[
                RetryState {
                    i: 0,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 1,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 2,
                    next_backoff: Some(Duration::from_millis(1))
                },
                RetryState {
                    i: 3,
                    next_backoff: None
                },
            ]
        );
    }

    /// Test that canceling retry operations surface the last error when the
    /// underlying future is not explicitly timed out.
    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_async_canceling_uncanceled_failure() {
        let res = Retry::default()
            .max_duration(Duration::from_millis(100))
            .retry_async_canceling(|_| async move { Err::<(), _>(anyhow!("injected")) })
            .await;
        assert_eq!(res.unwrap_err().to_string(), "injected");
    }

    /// Test that canceling retry operations surface the last error when the
    /// underlying future *is* not explicitly timed out.
    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_async_canceling_canceled_failure() {
        let res = Retry::default()
            .max_duration(Duration::from_millis(100))
            .retry_async_canceling(|state| async move {
                if state.i == 0 {
                    bail!("injected")
                } else {
                    time::sleep(Duration::MAX).await;
                    Ok(())
                }
            })
            .await;
        assert_eq!(res.unwrap_err().to_string(), "injected");
    }

    /// Test that the "deadline has elapsed" error is surfaced when there is
    /// no other error to surface.
    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_async_canceling_canceled_first_failure() {
        let res = Retry::default()
            .max_duration(Duration::from_millis(100))
            .retry_async_canceling(|_| async move {
                time::sleep(Duration::MAX).await;
                Ok::<_, anyhow::Error>(())
            })
            .await;
        assert_eq!(res.unwrap_err().to_string(), "deadline has elapsed");
    }

    #[mz_test_macro::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: cannot write to event
    async fn test_retry_reader() {
        use tokio::io::AsyncReadExt;

        /// Reader that errors out after the first read
        struct FlakyReader {
            offset: usize,
            should_error: bool,
        }

        impl AsyncRead for FlakyReader {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<Result<(), std::io::Error>> {
                if self.should_error {
                    Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()))
                } else if self.offset < 256 {
                    buf.put_slice(&[b'A']);
                    self.should_error = true;
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Ok(()))
                }
            }
        }

        let reader = RetryReader::new(|_state, offset| async move {
            Ok(FlakyReader {
                offset,
                should_error: false,
            })
        });
        tokio::pin!(reader);

        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.unwrap();
        assert_eq!(data, vec![b'A'; 256]);
    }
}
