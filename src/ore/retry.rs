// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Retry utilities.

use std::cmp;
use std::future::Future;

use tokio::time::{self, Duration};

const ZERO_DURATION: Duration = Duration::from_secs(0);

/// Convenience struct for holding retry information.
#[derive(Clone, Debug)]
pub struct RetryState {
    /// Current retry iteration.
    pub i: usize,

    /// Duration of the next retry backoff.
    pub next_backoff: Option<Duration>,
}

/// Retries a fallible operation `f` with exponential backoff.
///
/// If the operation is still failing after a cumulative delay of `max_sleep`,
/// its last error is returned.
pub async fn retry_for<F, U, T>(max_sleep: Duration, mut f: F) -> Result<T, String>
where
    F: FnMut(RetryState) -> U,
    U: Future<Output = Result<T, String>>,
{
    let mut state = RetryState {
        i: 0,
        next_backoff: Some(Duration::from_millis(125)),
    };
    let mut total_backoff = ZERO_DURATION;
    loop {
        match f(state.clone()).await {
            Ok(t) => return Ok(t),
            Err(e) => match state.next_backoff {
                None => return Err(e),
                Some(backoff) => {
                    total_backoff += backoff;
                    time::delay_for(backoff).await;
                    state.i += 1;
                    state.next_backoff = match cmp::min(backoff * 2, max_sleep - total_backoff) {
                        ZERO_DURATION => None,
                        b => Some(b),
                    }
                }
            },
        }
    }
}
