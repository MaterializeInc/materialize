// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Randomized delays for cooperating catalog writers.

use std::time::Duration;

use rand::RngExt;

/// Samples uniformly from the inclusive duration range. The caller owns retry
/// state, deadline limits, and sleeping. Panics if `minimum > maximum`.
pub fn sample_duration(minimum: Duration, maximum: Duration) -> Duration {
    sample_with(&mut rand::rng(), minimum, maximum)
}

fn sample_with(rng: &mut impl rand::Rng, minimum: Duration, maximum: Duration) -> Duration {
    rng.random_range(minimum..=maximum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn duration_range() {
        use rand::SeedableRng;

        let mut rng = rand::rngs::SmallRng::seed_from_u64(1);
        let minimum = Duration::from_millis(10);
        let maximum = Duration::from_millis(100);
        for _ in 0..100 {
            assert!((minimum..=maximum).contains(&sample_with(&mut rng, minimum, maximum)));
        }
        let phases: std::collections::BTreeSet<_> = (0..16)
            .map(|_| sample_with(&mut rng, Duration::ZERO, Duration::from_secs(1)))
            .collect();
        assert!(phases.len() > 1);
        assert_eq!(sample_duration(minimum, minimum), minimum);
        assert_eq!(
            sample_duration(Duration::ZERO, Duration::ZERO),
            Duration::ZERO
        );
    }
}
