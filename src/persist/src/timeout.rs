// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timeout utilities

use std::future::Future;
use std::time::Duration;

pub trait TimeoutExt: Future {
    fn timeout(self, duration: Duration) -> tokio::time::Timeout<Self>
    where
        Self: Sized;
}

impl<F: Future + Sized + Send> TimeoutExt for F {
    fn timeout(self, duration: Duration) -> tokio::time::Timeout<Self> {
        tokio::time::timeout(duration, self)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mz_ore::metric;
    use mz_ore::metrics::{IntCounter, MetricsRegistry};

    use crate::timeout::TimeoutExt;

    #[tokio::test(start_paused = true)]
    async fn times_out() {
        let registry = MetricsRegistry::new();
        let counter: IntCounter = registry.register(metric!(
            name: "timeouts",
            help: "timeouts",
        ));
        let fut = async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            1234
        };
        let fut = fut.timeout(Duration::from_secs(30));

        tokio::time::advance(Duration::from_secs(31)).await;

        assert!(fut.await.is_err());
        assert_eq!(counter.get(), 1);
    }
}
