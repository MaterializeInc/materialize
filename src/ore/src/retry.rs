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
    max_backoff: Duration,
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

    /// Sets the maximum backoff for the retry operation.
    ///
    /// The maximum backoff is the maximum amount of time to wait between tries.
    pub fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Sets the maximum number of tries.
    ///
    /// If the operation is still failing after `max_tries`, then
    /// [`Retry::retry`] will return the last error.
    ///
    /// # Panics
    ///
    /// Panics if `max_tries` is zero.
    pub fn max_tries(mut self, max_tries: usize) -> Self {
        if max_tries == 0 {
            panic!("Retry::max_tries must be greater than zero");
        }
        self.max_tries = max_tries;
        self
    }

    /// Retries the asynchronous, fallible operation `f` according to the
    /// configured policy.
    ///
    /// The `retry` method invokes `f` repeatedly until it succeeds or the
    /// maximum number of retries configured with [`Retry::max_tries`]
    /// is reached, at which point it propagates `f`'s return value from its
    /// last invocation.
    ///
    /// After the first failure, `retry` sleeps for the initial backoff
    /// configured via [`Retry::initial_backoff`]. After each successive
    /// failure, `retry` sleeps for twice the last backoff. If the backoff would
    /// ever exceed the maximum backoff configured viq
    /// [`Retry::max_backoff`], then the backoff is clamped to the
    /// maximum.
    pub async fn retry<F, U, T, E>(self, mut f: F) -> Result<T, E>
    where
        F: FnMut(RetryState) -> U,
        U: Future<Output = Result<T, E>>,
    {
        let mut i = 0;
        let mut next_backoff = Some(cmp::min(self.initial_backoff, self.max_backoff));
        loop {
            if i + 1 >= self.max_tries {
                next_backoff = None;
            }
            let state = RetryState { i, next_backoff };
            match f(state).await {
                Ok(t) => return Ok(t),
                Err(e) => match &mut next_backoff {
                    None => return Err(e),
                    Some(next_backoff) => {
                        time::sleep(*next_backoff).await;
                        *next_backoff = cmp::min(*next_backoff * 2, self.max_backoff);
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
            max_backoff: MAX_DURATION,
            max_tries: 8,
        }
    }
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
    async fn test_retry_fail_max_backoff() {
        let mut states = vec![];
        let res = Retry::default()
            .initial_backoff(Duration::from_millis(1))
            .max_backoff(Duration::from_millis(1))
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
