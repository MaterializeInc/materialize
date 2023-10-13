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

//! Assertion utilities.
//!
//! # Soft assertions
//!
//! Soft assertions are like debug assertions, but they can be toggled on and
//! off at runtime in a release build.
//!
//! They are useful in two scenarios:
//!
//!   * When a failed assertion should result in a log message rather a process
//!     crash.
//!
//!   * When evaluating the condition is too expensive to evaluate in production
//!     deployments.
//!
//! When soft assertions are disabled, the performance cost of each assertion is
//! one branch on an atomic.
//!
//! Ore provides the following macros to make soft assertions:
//!
//!   * [`soft_assert`](crate::soft_assert)
//!   * [`soft_assert_eq`](crate::soft_assert_eq)
//!   * [`soft_assert_or_log`](crate::soft_assert_or_log)
//!   * [`soft_panic_or_log`](crate::soft_panic_or_log)
//!
//! Due to limitations in Rust, these macros are exported at the crate root.

use std::sync::atomic::AtomicBool;

use crate::env;

/// Whether to enable soft assertions.
///
/// This value defaults to `true` when `debug_assertions` are enabled, or to the
/// value of the `MZ_SOFT_ASSERTIONS` environment variable otherwise.
// The rules about what you can do in a `ctor` function are somewhat fuzzy,
// because Rust does not explicitly support constructors. But a scan of the
// stdlib suggests that reading environment variables is safe enough.
#[cfg(not(miri))]
#[ctor::ctor]
pub static SOFT_ASSERTIONS: AtomicBool = {
    let default = cfg!(debug_assertions) || env::is_var_truthy("MZ_SOFT_ASSERTIONS");
    AtomicBool::new(default)
};

/// Always enable soft assertions when running [Miri].
///
/// Note: Miri also doesn't support global constructors, aka [`ctor`], if it ever does we could
/// get rid of this second definition. See <https://github.com/rust-lang/miri/issues/450> for
/// more details.
///
/// [Miri]: https://github.com/rust-lang/miri
#[cfg(miri)]
pub static SOFT_ASSERTIONS: AtomicBool = AtomicBool::new(true);

/// Asserts that a condition is true if soft assertions are enabled.
///
/// Soft assertions have a small runtime cost even when disabled. See
/// [`ore::assert`](crate::assert#Soft-assertions) for details.
#[macro_export]
macro_rules! soft_assert {
    ($cond:expr $(, $($arg:tt)+)?) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            assert!($cond$(, $($arg)+)?);
        }
    }}
}

/// Asserts that two values are equal if soft assertions are enabled.
///
/// Soft assertions have a small runtime cost even when disabled. See
/// [`ore::assert`](crate::assert#Soft-assertions) for details.
#[macro_export]
macro_rules! soft_assert_eq {
    ($cond:expr, $($arg:tt)+) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            assert_eq!($cond, $($arg)+);
        }
    }}
}

/// Asserts that a condition is true if soft assertions are enabled, or logs
/// an error if soft assertions are disabled and the condition is false.
#[macro_export]
macro_rules! soft_assert_or_log {
    ($cond:expr, $($arg:tt)+) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            assert!($cond, $($arg)+);
        } else if !$cond {
            ::tracing::error!($($arg)+)
        }
    }}
}

/// Panics if soft assertions are enabled, or logs an error if soft
/// assertions are disabled.
#[macro_export]
macro_rules! soft_panic_or_log {
    ($($arg:tt)+) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            panic!($($arg)+);
        } else {
            ::tracing::error!($($arg)+)
        }
    }}
}

/// Asserts that the left expression contains the right expression.
///
/// Containment is determined by the `contains` method on the left type. If the
/// left expression does not contain the right expression, the macro will panic
/// with a descriptive message that includes both the left and right
/// expressions.
///
/// # Motivation
///
/// The standard pattern for asserting containment uses the [`assert!`] macro
///
/// ```
/// # let left = &[()];
/// # let right = ();
/// assert!(left.contains(&right))
/// ```
///
/// but this pattern panics with a message that only displays `false` as the
/// cause. This hampers determination of the true cause of the assertion
/// failure.
///
/// # Examples
///
/// Check whether a string contains a substring:
///
/// ```
/// use mz_ore::assert_contains;
/// assert_contains!("hello", "ello");
/// ```
///
/// Check whether a slice contains an element:
///
/// ```
/// use mz_ore::assert_contains;
/// assert_contains!(&[1, 2, 3], 2);
/// ```
///
/// Failed assertions panic:
///
/// ```should_panic
/// use mz_ore::assert_contains;
/// assert_contains!("hello", "yellow");
/// ```
#[macro_export]
macro_rules! assert_contains {
    ($left:expr, $right:expr $(,)?) => {{
        let left = $left;
        let right = $right;
        if !left.contains(&$right) {
            panic!(
                r#"assertion failed: `left.contains(right)`:
  left: `{:?}`
 right: `{:?}`"#,
                left, right
            );
        }
    }};
}

#[cfg(test)]
mod tests {
    #[mz_test_macro::test]
    fn test_assert_contains_str() {
        assert_contains!("hello", "ello");
    }

    #[mz_test_macro::test]
    fn test_assert_contains_slice() {
        assert_contains!(&[1, 2, 3], 2);
    }

    #[mz_test_macro::test]
    #[should_panic(expected = "assertion failed: `left.contains(right)`:
  left: `\"hello\"`
 right: `\"yellow\"`")]
    fn test_assert_contains_fail() {
        assert_contains!("hello", "yellow");
    }
}
