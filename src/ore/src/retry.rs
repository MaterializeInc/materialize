// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

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
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use std::time::Duration;
//! use ore::retry::Retry;
//!
//! let res = Retry::default().retry(|state| async move {
//!    if state.i == 3 {
//!        Ok(())
//!    } else {
//!        Err("contrived failure")
//!    }
//! }).await;
//! assert_eq!(res, Ok(()));
//! # });
//! ```
//!
//! Limit the number of retries such that success is never observed:
//!
//! ```
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use std::time::Duration;
//! use ore::retry::Retry;
//!
//! let res = Retry::default().max_tries(2).retry(|state| async move {
//!    if state.i == 3 {
//!        Ok(())
//!    } else {
//!        Err("contrived failure")
//!    }
//! }).await;
//! assert_eq!(res, Err("contrived failure"));
//! # });

use std::cmp;
use std::future::Future;
use std::time::Instant;

use tokio::time::{self, Duration};

// TODO(benesch): remove this if the `duration_constants` feature stabilizes.
// See: https://github.com/rust-lang/rust/issues/57391
const MAX_DURATION: Duration = Duration::from_secs(u64::MAX);

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

/// Configures a retry operation.
///
/// See the [module documentation](self) for usage examples.
#[derive(Debug)]
pub struct Retry {
    initial_backoff: Duration,
    factor: f64,
    clamp_backoff: Duration,
    limit: RetryLimit,
}

impl Retry {
    /// Sets the initial backoff for the retry operation.
    ///
    /// The initial backoff is the amount of time to wait if the first try
    /// fails.
    pub fn initial_backoff(mut self, initial_backoff: Duration) -> Self {
        if initial_backoff == Duration::from_secs(0) {
            panic!("initial backoff duration must be greater than zero");
        }
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
    /// Maximum durations and maximum tries are mutually exclusive within a
    /// given `Retry` operation. Calls to `max_tries` will override any
    /// previous calls to `max_tries` or [`max_duration`](Retry::max_duration).
    ////
    /// # Panics
    ///
    /// Panics if `max_tries` is zero.
    pub fn max_tries(mut self, max_tries: usize) -> Self {
        if max_tries == 0 {
            panic!("max tries must be greater than zero");
        }
        self.limit = RetryLimit::Tries(max_tries);
        self
    }

    /// Sets the maximum duration.
    ///
    /// If the operation is still failing after the specified `duration`, then
    /// the operation will be retried once more and [`retry`](Retry::retry) will
    /// return the last error.
    ///
    /// Maximum durations and maximum tries are mutually exclusive within a
    /// given `Retry` operation. Calls to `max_duration` will override any
    /// previous calls to `max_duration` or [`max_tries`](Retry::max_tries).
    pub fn max_duration(mut self, duration: Duration) -> Self {
        self.limit = RetryLimit::Duration(duration);
        self
    }

    /// Retries the asynchronous, fallible operation `f` according to the
    /// configured policy.
    ///
    /// The `retry` method invokes `f` repeatedly until it succeeds or until the
    /// maximum duration or tries have been reached, as configured via
    /// [`max_duration`](Retry::max_duration) or
    /// [`max_tries`](Retry::max_tries). If `f` never succeeds, then `retry`
    /// returns `f`'s return value from its last invocation.
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
    pub async fn retry<F, U, T, E>(self, mut f: F) -> Result<T, E>
    where
        F: FnMut(RetryState) -> U,
        U: Future<Output = Result<T, E>>,
    {
        let start = Instant::now();
        let mut i = 0;
        let mut next_backoff = Some(cmp::min(self.initial_backoff, self.clamp_backoff));
        loop {
            match self.limit {
                RetryLimit::Tries(max_tries) if i + 1 >= max_tries => next_backoff = None,
                RetryLimit::Duration(max_duration) => {
                    let elapsed = start.elapsed();
                    if elapsed > max_duration {
                        next_backoff = None;
                    } else if elapsed + next_backoff.unwrap() > max_duration {
                        next_backoff = Some(max_duration - elapsed);
                    }
                }
                _ => (),
            }
            let state = RetryState { i, next_backoff };
            match f(state).await {
                Ok(t) => return Ok(t),
                Err(e) => match &mut next_backoff {
                    None => return Err(e),
                    Some(next_backoff) => {
                        time::sleep(*next_backoff).await;
                        *next_backoff =
                            cmp::min(next_backoff.mul_f64(self.factor), self.clamp_backoff);
                    }
                },
            }
            i += 1;
        }
    }
}

impl Default for Retry {
    /// Constructs a retry operation with defaults that are reasonable for a
    /// fallible network operation.
    fn default() -> Self {
        Retry {
            initial_backoff: Duration::from_millis(125),
            factor: 2.0,
            clamp_backoff: MAX_DURATION,
            limit: RetryLimit::Duration(Duration::from_secs(30)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RetryLimit {
    Duration(Duration),
    Tries(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_retry_success() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .retry(|state| {
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

    #[tokio::test]
    async fn test_retry_fail_max_tries() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .max_tries(3)
            .retry(|state| {
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

    #[tokio::test]
    async fn test_retry_fail_max_duration() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(5))
            .max_duration(Duration::from_millis(10))
            .retry(|state| {
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

    #[tokio::test]
    async fn test_retry_fail_clamp_backoff() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .clamp_backoff(Duration::from_millis(1))
            .max_tries(4)
            .retry(|state| {
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
}
