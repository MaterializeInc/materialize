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
    /// The initial backoff for the retry operation.
    pub initial_backoff: Duration,
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
            // Chosen to meet the following arbitrary criteria: a power of two
            // that's close to the AWS Aurora latency of 6ms.
            initial_backoff: Duration::from_millis(4),
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
            backoff,
        }
    }
}

/// A series of exponential, jittered, clamped sleeps.
#[derive(Debug)]
pub struct RetryStream {
    cfg: Retry,
    rng: SmallRng,
    backoff: Duration,
}

impl RetryStream {
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
        self.backoff = std::cmp::min(self.backoff * 2, self.cfg.clamp_backoff);
        self
    }
}
