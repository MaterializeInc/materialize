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
//! TODO: This is a copy of [mz_persist::retry], which we include here so we
//! don't have that as a dependency, once we move the oracle out into it's own
//! crate. If one were so inclined, both the persist version and [mz_ore::retry]
//! (on which that is based), could be merged.
//!
//! The main difference between the [mz_ore::retry] version and this is that we
//! don't limit the retry duration or the number of retries. This is because
//! callers can't do anything but retry themselves or panic when timestamp
//! operations fail.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};

/// Configures a retry operation.
#[derive(Debug, Clone, PartialEq)]
pub struct Retry {
    /// The initial backoff for the retry operation.
    pub initial_backoff: Duration,
    /// The backoff multiplier.
    pub multiplier: u32,
    /// Clamps the maximum backoff for the retry operation.
    pub clamp_backoff: Duration,
    /// A seed for the random jitter.
    pub seed: u64,
}

#[allow(dead_code)]
impl Retry {
    /// The default retry configuration for timestamp oracles.
    ///
    /// Uses the given SystemTime to initialize the seed for random jitter.
    pub fn oracle_defaults(now: SystemTime) -> Self {
        Retry {
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
        let backoff = self.initial_backoff;
        RetryStream {
            cfg: self,
            rng,
            attempt: 0,
            backoff,
        }
    }
}

/// A series of exponential, jittered, clamped sleeps.
#[allow(dead_code)]
#[derive(Debug)]
pub struct RetryStream {
    cfg: Retry,
    rng: SmallRng,
    attempt: usize,
    backoff: Duration,
}

#[allow(dead_code)]
impl RetryStream {
    /// How many times [Self::sleep] has been called.
    pub fn attempt(&self) -> usize {
        self.attempt
    }

    /// The next sleep (without jitter for easy printing in logs).
    pub fn next_sleep(&self) -> Duration {
        self.backoff
    }

    /// Executes the next sleep in the series.
    ///
    /// This isn't cancel-safe, so it consumes and returns self, to prevent
    /// accidental mis-use.
    pub async fn sleep(mut self) -> Self {
        // Should the jitter be configurable?
        let jitter = self.rng.gen_range(0.9..=1.1);
        let sleep = self.backoff.mul_f64(jitter);
        tokio::time::sleep(sleep).await;
        self.attempt += 1;
        self.backoff = std::cmp::min(self.backoff * self.cfg.multiplier, self.cfg.clamp_backoff);
        self
    }
}
