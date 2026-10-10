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

//! Integration with the Antithesis testing platform.
//!
//! Every function here is a no-op unless the `antithesis` feature is enabled.
//! Only the Antithesis build flavor enables it, so production binaries do not
//! link the SDK. With the feature on, process panics and logged soft
//! assertions are reported to Antithesis as failed properties, which turns the
//! existing assertion corpus into test properties without per-site changes.

use std::cell::Cell;

#[cfg(all(feature = "antithesis", target_os = "linux"))]
use antithesis_instrumentation as _;

/// Re-export of the SDK for the assertion macros below.
#[doc(hidden)]
#[cfg(feature = "antithesis")]
pub use antithesis_sdk as sdk;

std::thread_local! {
    static PANIC_EXPECTED: Cell<bool> = const { Cell::new(false) };
}

#[doc(hidden)]
#[cfg(feature = "antithesis")]
#[macro_export]
macro_rules! __antithesis_assert {
    ($assert:ident, $($arg:expr),+) => {
        $crate::antithesis::sdk::$assert!($($arg),+)
    };
}

#[doc(hidden)]
#[cfg(not(feature = "antithesis"))]
#[macro_export]
macro_rules! __antithesis_assert {
    ($assert:ident, $($arg:expr),+) => {{
        let _ = ($($arg),+);
    }};
}

/// Asserts that `$cond` holds every time this is evaluated.
///
/// Without the `antithesis` feature this only evaluates its arguments.
/// `$message` must be a string literal unique to the call site: Antithesis
/// catalogs assertions by message at build time. `$details` is a reference to
/// a `serde::Serialize` value.
#[macro_export]
macro_rules! antithesis_always {
    ($cond:expr, $message:expr, $details:expr) => {
        $crate::__antithesis_assert!(assert_always, $cond, $message, $details)
    };
}

/// Asserts that `$cond` holds at least once during a test run. See
/// [`antithesis_always`] for the argument contract.
#[macro_export]
macro_rules! antithesis_sometimes {
    ($cond:expr, $message:expr, $details:expr) => {
        $crate::__antithesis_assert!(assert_sometimes, $cond, $message, $details)
    };
}

/// Asserts that this point is reached at least once during a test run. See
/// [`antithesis_always`] for the argument contract.
#[macro_export]
macro_rules! antithesis_reachable {
    ($message:expr, $details:expr) => {
        $crate::__antithesis_assert!(assert_reachable, $message, $details)
    };
}

/// Asserts that this point is never reached. See [`antithesis_always`] for
/// the argument contract.
#[macro_export]
macro_rules! antithesis_unreachable {
    ($message:expr, $details:expr) => {
        $crate::__antithesis_assert!(assert_unreachable, $message, $details)
    };
}

/// Marks the next panic on this thread as an outcome the system produces by
/// design, so it is reported as reached rather than as a failure.
///
/// Call immediately before panicking on a path whose cause is outside the
/// process's control, such as giving up after an expired lease. Every other
/// panic is reported as a failed property.
pub fn expect_panic() {
    PANIC_EXPECTED.with(|expected| expected.set(true));
}

/// Initializes the SDK and registers the assertion catalog.
///
/// Call once at the top of `main`. Assertions registered here are reported as
/// unreached if a run never evaluates them, which is what makes a missing
/// `Sometimes` visible.
pub fn init() {
    #[cfg(feature = "antithesis")]
    antithesis_sdk::antithesis_init();
}

/// Reports a panic that is about to take down the process.
pub fn panicked(location: &str, message: &str) {
    let expected = PANIC_EXPECTED.with(|expected| expected.replace(false));
    #[cfg(feature = "antithesis")]
    {
        #[derive(serde::Serialize)]
        struct Details<'a> {
            location: &'a str,
            message: &'a str,
        }
        let details = Details { location, message };
        if expected {
            antithesis_sdk::assert_reachable!("Materialize process panicked by design", &details);
        } else {
            antithesis_sdk::assert_unreachable!("Materialize process panicked", &details);
        }
    }
    #[cfg(not(feature = "antithesis"))]
    let _ = (location, message, expected);
}
