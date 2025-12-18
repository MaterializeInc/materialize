// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Retry utilities.
//!
//! TODO: The structure of this intentionally mirrors [mz_ore::retry], so that
//! it could be merged in if anyone were so inclined.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};

/// Configures a retry operation.
#[derive(Debug, Clone, PartialEq)]
pub struct Retry {
    /// An initial fixed sleep, before the exponential backoff.
    ///
    /// This is skipped if set to [Duration::ZERO].
    pub fixed_sleep: Duration,
    /// The initial backoff for the exponential backoff retries.
    pub initial_backoff: Duration,
    /// The backoff multiplier.
    pub multiplier: u32,
    /// Clamps the maximum backoff for the retry operation.
    pub clamp_backoff: Duration,
    /// A seed for the random jitter.
    pub seed: u64,
}

impl Retry {
    /// The default retry configuration for persist.
    ///
    /// Uses the given SystemTime to initialize the seed for random jitter.
    pub fn persist_defaults(now: SystemTime) -> Self {
        Retry {
            fixed_sleep: Duration::ZERO,
            // Chosen to meet the following arbitrary criteria: a power of two
            // that's close to the AWS Aurora latency of 6ms.
            initial_backoff: Duration::from_millis(4),
            multiplier: 2,
            // Chosen to meet the following arbitrary criteria: between 10s and
            // 60s.
            clamp_backoff: Duration::from_secs(16),
            seed: now
                .duration_since(UNIX_EPOCH)
                .map_or(0, |x| u64::from(x.subsec_nanos())),
        }
    }

    /// Convert into [`RetryStream`]
    pub fn into_retry_stream(self) -> RetryStream {
        let rng = SmallRng::seed_from_u64(self.seed);
        let backoff = (self.fixed_sleep == Duration::ZERO).then_some(self.initial_backoff);
        RetryStream {
            cfg: self,
            rng,
            attempt: 0,
            backoff,
        }
    }
}

/// A series of exponential, jittered, clamped sleeps.
#[derive(Debug)]
pub struct RetryStream {
    cfg: Retry,
    rng: SmallRng,
    attempt: usize,
    // None if the next sleep is `cfg.fixed_sleep`.
    backoff: Option<Duration>,
}

impl RetryStream {
    /// How many times [Self::sleep] has been called.
    pub fn attempt(&self) -> usize {
        self.attempt
    }

    /// The next sleep (without jitter for easy printing in logs).
    pub fn next_sleep(&self) -> Duration {
        self.backoff.unwrap_or(self.cfg.fixed_sleep)
    }

    /// Executes the next sleep in the series.
    ///
    /// This isn't cancel-safe, so it consumes and returns self, to prevent
    /// accidental mis-use.
    pub async fn sleep(mut self) -> Self {
        // Should the jitter be configurable?
        let jitter = self.rng.random_range(0.9..=1.1);
        let sleep = self.next_sleep().mul_f64(jitter);
        tokio::time::sleep(sleep).await;
        self.advance()
    }

    // Only exposed for testing.
    fn advance(mut self) -> Self {
        self.attempt += 1;
        self.backoff = Some(match self.backoff {
            None => self.cfg.initial_backoff,
            Some(x) => std::cmp::min(x * self.cfg.multiplier, self.cfg.clamp_backoff),
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn retry_stream() {
        #[track_caller]
        fn testcase(r: Retry, expected_sleep_ms: Vec<u64>) {
            let mut r = r.into_retry_stream();
            for expected_sleep_ms in expected_sleep_ms {
                let expected = Duration::from_millis(expected_sleep_ms);
                let actual = r.next_sleep();
                assert_eq!(actual, expected);
                r = r.advance();
            }
        }

        testcase(
            Retry {
                fixed_sleep: Duration::ZERO,
                initial_backoff: Duration::from_millis(1_200),
                multiplier: 2,
                clamp_backoff: Duration::from_secs(16),
                seed: 0,
            },
            vec![1_200, 2_400, 4_800, 9_600, 16_000, 16_000],
        );
        testcase(
            Retry {
                fixed_sleep: Duration::from_millis(1_200),
                initial_backoff: Duration::from_millis(100),
                multiplier: 2,
                clamp_backoff: Duration::from_secs(16),
                seed: 0,
            },
            vec![
                1_200, 100, 200, 400, 800, 1_600, 3_200, 6_400, 12_800, 16_000, 16_000,
            ],
        );
    }
}
