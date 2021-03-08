// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! Retry utilities.

use std::cmp;
use std::future::Future;

use tokio::time::{self, Duration};

const ZERO_DURATION: Duration = Duration::from_secs(0);

/// The state of a retry opreation managed by [`retry_for`].
#[derive(Clone, Debug)]
pub struct RetryState {
    /// The retry counter, starting from zero on the first try.
    pub i: usize,
    /// If this try fails, the amount of time that `retry_for` will sleep
    /// before the next attempt. If this is the last attempt, then this
    /// field will be `None`.
    pub next_backoff: Option<Duration>,
    /// If this try fails, the maximum amount of time that `retry_for` will sleep
    /// before the next attempt.
    ///
    /// If this is none, there is no max backoff.
    pub max_backoff: Option<Duration>,
    /// If the operation is still failing after a cumulative delay of `max_sleep`,
    /// its last error is returned. Set to `None` to retry forever.
    pub max_sleep: Option<Duration>,
}

impl RetryState {
    /// Retries a fallible operation `f` with exponential backoff.
    ///
    /// If the operation is still failing after a cumulative delay of `max_sleep`,
    /// its last error is returned.
    pub async fn retry<F, U, T, E>(mut self, mut f: F) -> Result<T, E>
    where
        F: FnMut(RetryState) -> U,
        U: Future<Output = Result<T, E>>,
    {
        let mut total_backoff = ZERO_DURATION;
        loop {
            match f(self.clone()).await {
                Ok(t) => return Ok(t),
                Err(e) => match self.next_backoff {
                    None => return Err(e),
                    Some(mut backoff) => {
                        total_backoff += backoff;
                        time::sleep(backoff).await;
                        self.i += 1;
                        backoff *= 2;
                        let mut next_backoff = match self.max_sleep {
                            None => Some(backoff),
                            Some(max_sleep) => match cmp::min(backoff, max_sleep - total_backoff) {
                                ZERO_DURATION => None,
                                b => Some(b),
                            },
                        };
                        if let (Some(b), Some(max_backoff)) = (next_backoff, self.max_backoff) {
                            next_backoff = Some(cmp::min(b, max_backoff));
                        }

                        self.next_backoff = next_backoff;
                    }
                },
            }
        }
    }
}

/// Retries a fallible operation `f` with exponential backoff.
///
/// If the operation is still failing after a cumulative delay of `max_sleep`,
/// its last error is returned.
pub async fn retry_for<F, U, T, E>(max_sleep: Duration, f: F) -> Result<T, E>
where
    F: FnMut(RetryState) -> U,
    U: Future<Output = Result<T, E>>,
{
    RetryBuilder::new()
        .max_sleep(Some(max_sleep))
        .build()
        .retry(f)
        .await
}

/// A builder for a RetryState.
#[derive(Debug)]
pub struct RetryBuilder {
    initial_backoff: Duration,
    max_backoff: Option<Duration>,
    max_sleep: Option<Duration>,
}

impl RetryBuilder {
    /// Creates the default RetryBuilder.
    pub fn new() -> Self {
        RetryBuilder {
            initial_backoff: Duration::from_millis(125),
            max_backoff: None,
            max_sleep: None,
        }
    }
    /// Constructs the RetryState.
    pub fn build(self) -> RetryState {
        RetryState {
            i: 0,
            next_backoff: Some(self.initial_backoff),
            max_backoff: self.max_backoff,
            max_sleep: self.max_sleep,
        }
    }
    /// If the operation is still failing after a cumulative delay of `max`, its
    /// last error is returned. Set to `None` to retry forever.
    pub fn max_sleep(mut self, max: Option<Duration>) -> Self {
        self.max_sleep = max;
        self
    }
    /// If a try fails, the amount of time that `retry_for` will sleep
    /// before the next attempt.
    pub fn initial_backoff(mut self, duration: Duration) -> Self {
        self.initial_backoff = duration;
        self
    }

    /// If a try fails repeateadly, the maximum time that `retry_for` will
    /// sleep between attempts.
    ///
    /// Default is to allow infinite sleep between retries.
    pub fn max_backoff(mut self, duration: Duration) -> Self {
        self.max_backoff = Some(duration);
        self
    }
}

impl Default for RetryBuilder {
    fn default() -> Self {
        Self::new()
    }
}
