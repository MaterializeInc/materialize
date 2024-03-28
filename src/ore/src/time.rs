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

//! Timing related extensions.

use std::time::Duration;

use num::Zero;

/// Generic Error type returned from methods on [`DurationExt`].
#[derive(Copy, Clone, Debug)]
pub struct DurationError(#[allow(dead_code)] &'static str);

/// Extensions for [`std::time::Duration`].
pub trait DurationExt {
    /// Creates a [`Duration`] from the specified number of seconds represented as an `i64`.
    ///
    /// Returns an error if the provided number of seconds is negative.
    ///
    /// ```
    /// use std::time::Duration;
    /// use mz_ore::time::DurationExt;
    ///
    /// // Negative numbers will fail.
    /// assert!(Duration::try_from_secs_i64(-100).is_err());
    ///
    /// // i64 max works.
    /// assert!(Duration::try_from_secs_i64(i64::MAX).is_ok());
    /// ```
    fn try_from_secs_i64(secs: i64) -> Result<Duration, DurationError>;

    /// Saturating `Duration` multiplication. Computes `self * rhs`, saturating at the numeric
    /// bounds instead of overflowing.
    ///
    /// ```
    /// use std::time::Duration;
    /// use mz_ore::time::DurationExt;
    ///
    /// let one = Duration::from_secs(1);
    /// assert_eq!(one.saturating_mul_f64(f64::INFINITY), Duration::from_secs(u64::MAX));
    ///
    /// assert_eq!(one.saturating_mul_f64(f64::NEG_INFINITY), Duration::from_secs(0));
    /// assert_eq!(one.saturating_mul_f64(f64::NAN), Duration::from_secs(0));
    /// assert_eq!(one.saturating_mul_f64(-0.0), Duration::from_secs(0));
    /// assert_eq!(one.saturating_mul_f64(0.0), Duration::from_secs(0));
    ///
    /// assert_eq!(Duration::from_secs(20).saturating_mul_f64(0.1), Duration::from_secs(2));
    /// ```
    fn saturating_mul_f64(&self, rhs: f64) -> Duration;
}

impl DurationExt for Duration {
    fn try_from_secs_i64(secs: i64) -> Result<Duration, DurationError> {
        let secs: u64 = secs
            .try_into()
            .map_err(|_| DurationError("negative number of seconds"))?;
        Ok(Duration::from_secs(secs))
    }

    fn saturating_mul_f64(&self, rhs: f64) -> Duration {
        let x = self.as_secs_f64() * rhs;
        let bound = if x.is_sign_negative() || x.is_nan() || x.is_zero() {
            u64::MIN
        } else {
            u64::MAX
        };
        Duration::try_from_secs_f64(x).unwrap_or(Duration::from_secs(bound))
    }
}

#[cfg(test)]
mod tests {
    use super::DurationExt;
    use proptest::prelude::*;
    use std::time::Duration;

    const ONE: Duration = Duration::from_secs(1);

    proptest! {
        #[crate::test]
        fn proptest_saturating_mul_f64(rhs: f64) {
            // Saturating multiplication should never panic.
            let _ = ONE.saturating_mul_f64(rhs);
        }
    }
}
