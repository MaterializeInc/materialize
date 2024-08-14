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

//! Now utilities.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::SystemTime;

#[cfg(feature = "chrono")]
use chrono::{DateTime, TimeZone, Utc};

/// A type representing the number of milliseconds since the Unix epoch.
pub type EpochMillis = u64;

/// Converts epoch milliseconds to a DateTime.
#[cfg(feature = "chrono")]
// TODO(benesch): rewrite to avoid dangerous use of `as`.
#[allow(clippy::as_conversions)]
pub fn to_datetime(millis: EpochMillis) -> DateTime<Utc> {
    let dur = std::time::Duration::from_millis(millis);
    match Utc
        .timestamp_opt(dur.as_secs() as i64, dur.subsec_nanos())
        .single()
    {
        Some(single) => single,
        None => {
            panic!("Ambiguous timestamp: {millis} millis")
        }
    }
}

/// A function that returns system or mocked time.
// This is a newtype so that it can implement `Debug`, as closures don't
// implement `Debug` by default. It derefs to a callable so that it is
// ergonomically equivalent to a closure.
#[derive(Clone)]
pub struct NowFn<T = EpochMillis>(Arc<dyn Fn() -> T + Send + Sync>);

impl NowFn<EpochMillis> {
    /// Returns now in seconds.
    pub fn as_secs(&self) -> i64 {
        let millis: u64 = (self)();
        // Justification for `unwrap`:
        // Any u64, when divided by 1000, is a valid i64.
        i64::try_from(millis / 1_000).unwrap()
    }
}

impl<T> fmt::Debug for NowFn<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("<now_fn>")
    }
}

impl<T> Deref for NowFn<T> {
    type Target = dyn Fn() -> T + Send + Sync;

    fn deref(&self) -> &Self::Target {
        &(*self.0)
    }
}

impl<F, T> From<F> for NowFn<T>
where
    F: Fn() -> T + Send + Sync + 'static,
{
    fn from(f: F) -> NowFn<T> {
        NowFn(Arc::new(f))
    }
}

fn system_time() -> EpochMillis {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("failed to get millis since epoch")
        .as_millis()
        .try_into()
        .expect("current time did not fit into u64")
}
fn now_zero() -> EpochMillis {
    0
}

/// A [`NowFn`] that returns the actual system time.
pub static SYSTEM_TIME: LazyLock<NowFn> = LazyLock::new(|| NowFn::from(system_time));

/// A [`NowFn`] that always returns zero.
///
/// For use in tests.
pub static NOW_ZERO: LazyLock<NowFn> = LazyLock::new(|| NowFn::from(now_zero));

#[cfg(feature = "chrono")]
#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::to_datetime;

    #[crate::test]
    fn test_to_datetime() {
        let test_cases = [
            (
                0,
                NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_nano_opt(0, 0, 0, 0)
                    .unwrap(),
            ),
            (
                1600000000000,
                NaiveDate::from_ymd_opt(2020, 9, 13)
                    .unwrap()
                    .and_hms_nano_opt(12, 26, 40, 0)
                    .unwrap(),
            ),
            (
                1658323270293,
                NaiveDate::from_ymd_opt(2022, 7, 20)
                    .unwrap()
                    .and_hms_nano_opt(13, 21, 10, 293_000_000)
                    .unwrap(),
            ),
        ];
        // to_datetime works properly and roundtrips
        for (millis, datetime) in test_cases.into_iter() {
            let converted_datetime = to_datetime(millis).naive_utc();
            assert_eq!(datetime, converted_datetime);
            assert_eq!(
                millis,
                u64::try_from(converted_datetime.and_utc().timestamp_millis()).unwrap()
            )
        }
    }
}
