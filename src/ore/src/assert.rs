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
//!   * When evaluating the condition is too expensive to evaluate in production
//!     deployments.
//!
//! When soft assertions are disabled, the performance cost of each assertion is
//! one branch on an atomic.
//!
//! Ore provides the following macros to make soft assertions:
//!
//!   * [`soft_assert_or_log`](crate::soft_assert_or_log)
//!   * [`soft_assert_eq_or_log`](crate::soft_assert_eq_or_log)
//!   * [`soft_assert_ne_or_log`](crate::soft_assert_ne_or_log)
//!   * [`soft_panic_or_log`](crate::soft_panic_or_log)
//!   * [`soft_assert_no_log`](crate::soft_assert_no_log)
//!   * [`soft_assert_eq_no_log`](crate::soft_assert_eq_no_log)
//!   * [`soft_assert_ne_no_log`](crate::soft_assert_ne_no_log)
//!
//! The `_or_log` variants should be used by default, as they allow us to find
//! failed condition checks in production. The `_no_log` variants are silent
//! in production and should only be used when performance considerations
//! prohibit the use of the logging variants.
//!
//! Due to limitations in Rust, these macros are exported at the crate root.

use std::sync::atomic::AtomicBool;

/// Whether to enable soft assertions.
///
/// This value defaults to `true` when `debug_assertions` are enabled, or to the
/// value of the `MZ_SOFT_ASSERTIONS` environment variable otherwise.
// The rules about what you can do in a `ctor` function are somewhat fuzzy,
// because Rust does not explicitly support constructors. But a scan of the
// stdlib suggests that reading environment variables is safe enough.
#[cfg(not(any(miri, target_arch = "wasm32")))]
#[ctor::ctor]
pub static SOFT_ASSERTIONS: AtomicBool = {
    let default = cfg!(debug_assertions) || crate::env::is_var_truthy("MZ_SOFT_ASSERTIONS");
    AtomicBool::new(default)
};

/// Always enable soft assertions when running [Miri] or wasm.
///
/// Note: Miri also doesn't support global constructors, aka [`ctor`], if it ever does we could
/// get rid of this second definition. See <https://github.com/rust-lang/miri/issues/450> for
/// more details.
///
/// [Miri]: https://github.com/rust-lang/miri
#[cfg(any(miri, target_arch = "wasm32"))]
pub static SOFT_ASSERTIONS: AtomicBool = AtomicBool::new(true);

/// Asserts that a condition is true if soft assertions are enabled.
///
/// Soft assertions have a small runtime cost even when disabled. See
/// [`ore::assert`](crate::assert#Soft-assertions) for details.
#[macro_export]
macro_rules! soft_assert_no_log {
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
macro_rules! soft_assert_eq_no_log {
    ($cond:expr, $($arg:tt)+) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            assert_eq!($cond, $($arg)+);
        }
    }}
}

/// Asserts that two values are not equal if soft assertions are enabled.
///
/// Soft assertions have a small runtime cost even when disabled. See
/// [`ore::assert`](crate::assert#Soft-assertions) for details.
#[macro_export]
macro_rules! soft_assert_ne_no_log {
    ($cond:expr, $($arg:tt)+) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            assert_ne!($cond, $($arg)+);
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

/// Asserts that two expressions are equal to each other if soft assertions
/// are enabled, or logs an error if soft assertions are disabled and the
/// two expressions are not equal.
#[macro_export]
macro_rules! soft_assert_eq_or_log {
    ($left:expr, $right:expr) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            assert_eq!($left, $right);
        } else {
            // Borrowed from [`std::assert_eq`].
            match (&$left, &$right) {
                (left_val, right_val) => {
                    if !(*left_val == *right_val) {
                        ::tracing::error!(
                            "assertion {:?} == {:?} failed",
                            left_val, right_val
                        );
                    }
                }
            }
        }
    }};
    ($left:expr, $right:expr, $($arg:tt)+) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            assert_eq!($left, $right, $($arg)+);
        } else {
            // Borrowed from [`std::assert_eq`].
            match (&$left, &$right) {
                (left, right) => {
                    if !(*left == *right) {
                        ::tracing::error!(
                            "assertion {:?} == {:?} failed: {}",
                            left, right, format!($($arg)+)
                        );
                    }
                }
            }
        }
    }};
}

/// Asserts that two expressions are not equal to each other if soft assertions
/// are enabled, or logs an error if soft assertions are disabled and the
/// two expressions are not equal.
#[macro_export]
macro_rules! soft_assert_ne_or_log {
    ($left:expr, $right:expr) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            assert_ne!($left, $right);
        } else {
            // Borrowed from [`std::assert_ne`].
            match (&$left, &$right) {
                (left_val, right_val) => {
                    if *left_val == *right_val {
                        ::tracing::error!(
                            "assertion {:?} != {:?} failed",
                            left_val, right_val
                        );
                    }
                }
            }
        }
    }};
    ($left:expr, $right:expr, $($arg:tt)+) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            assert_ne!($left, $right, $($arg)+);
        } else {
            // Borrowed from [`std::assert_ne`].
            match (&$left, &$right) {
                (left_val, right_val) => {
                    if *left_val == *right_val {
                        ::tracing::error!(
                            "assertion {:?} != {:?} failed: {}",
                            $left, $right, format!($($arg)+)
                        );
                    }
                }
            }
        }
    }};
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

/// Panics if soft assertions are enabled.
#[macro_export]
macro_rules! soft_panic_no_log {
    ($($arg:tt)+) => {{
        if $crate::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            panic!($($arg)+);
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

/// Asserts that the provided expression, that returns an `Option`, is `None`.
///
/// # Motivation
///
/// The standard pattern for asserting a value is `None` using the `assert!` macro is:
///
/// ```
/// # let x: Option<usize> = None;
/// assert!(x.is_none());
/// ```
///
/// The issue with this pattern is when the assertion fails it only prints `false`
/// and not the value contained in the `Some(_)` variant which makes debugging difficult.
///
/// # Examples
///
/// ### Basic Use
///
/// ```should_panic
/// use mz_ore::assert_none;
/// assert_none!(Some(42));
/// ```
///
/// ### With extra message
///
/// ```should_panic
/// use mz_ore::assert_none;
/// let other_val = 100;
/// assert_none!(Some(42), "ohh noo! x {other_val}");
/// ```
///
#[macro_export]
macro_rules! assert_none {
    ($val:expr, $($msg:tt)+) => {{
        if let Some(y) = &$val {
            panic!("assertion failed: expected None found Some({y:?}), {}", format!($($msg)+));
        }
    }};
    ($val:expr) => {{
        if let Some(y) = &$val {
            panic!("assertion failed: expected None found Some({y:?})");
        }
    }}
}

/// Asserts that the provided expression, that returns a `Result`, is `Ok`.
///
/// # Motivation
///
/// The standard pattern for asserting a value is `Ok` using the `assert!` macro is:
///
/// ```
/// # let x: Result<usize, usize> = Ok(42);
/// assert!(x.is_ok());
/// ```
///
/// The issue with this pattern is when the assertion fails it only prints `false`
/// and not the value contained in the `Err(_)` variant which makes debugging difficult.
///
/// # Examples
///
/// ### Basic Use
///
/// ```should_panic
/// use mz_ore::assert_ok;
/// let error: Result<usize, usize> = Err(42);
/// assert_ok!(error);
/// ```
///
/// ### With extra message
///
/// ```should_panic
/// use mz_ore::assert_ok;
/// let other_val = 100;
/// let error: Result<usize, usize> = Err(42);
/// assert_ok!(error, "ohh noo! x {other_val}");
/// ```
///
#[macro_export]
macro_rules! assert_ok {
    ($val:expr, $($msg:tt)+) => {{
        if let Err(y) = &$val {
            panic!("assertion failed: expected Ok found Err({y:?}), {}", format!($($msg)+));
        }
    }};
    ($val:expr) => {{
        if let Err(y) = &$val {
            panic!("assertion failed: expected Ok found Err({y:?})");
        }
    }}
}

/// Asserts that the provided expression, that returns a `Result`, is `Err`.
///
/// # Motivation
///
/// The standard pattern for asserting a value is `Err` using the `assert!` macro is:
///
/// ```
/// # let x: Result<usize, usize> = Err(42);
/// assert!(x.is_err());
/// ```
///
/// The issue with this pattern is when the assertion fails it only prints `false`
/// and not the value contained in the `Ok(_)` variant which makes debugging difficult.
///
/// # Examples
///
/// ### Basic Use
///
/// ```should_panic
/// use mz_ore::assert_err;
/// let error: Result<usize, usize> = Ok(42);
/// assert_err!(error);
/// ```
///
/// ### With extra message
///
/// ```should_panic
/// use mz_ore::assert_err;
/// let other_val = 100;
/// let error: Result<usize, usize> = Ok(42);
/// assert_err!(error, "ohh noo! x {other_val}");
/// ```
///
#[macro_export]
macro_rules! assert_err {
    ($val:expr, $($msg:tt)+) => {{
        if let Ok(y) = &$val {
            panic!("assertion failed: expected Err found Ok({y:?}), {}", format!($($msg)+));
        }
    }};
    ($val:expr) => {{
        if let Ok(y) = &$val {
            panic!("assertion failed: expected Err found Ok({y:?})");
        }
    }}
}

#[cfg(test)]
mod tests {
    #[crate::test]
    fn test_assert_contains_str() {
        assert_contains!("hello", "ello");
    }

    #[crate::test]
    fn test_assert_contains_slice() {
        assert_contains!(&[1, 2, 3], 2);
    }

    #[crate::test]
    #[should_panic(expected = "assertion failed: `left.contains(right)`:
  left: `\"hello\"`
 right: `\"yellow\"`")]
    fn test_assert_contains_fail() {
        assert_contains!("hello", "yellow");
    }

    #[crate::test]
    #[should_panic(expected = "assertion failed: expected None found Some(42)")]
    fn test_assert_none_fail() {
        assert_none!(Some(42));
    }

    #[crate::test]
    #[should_panic(expected = "assertion failed: expected None found Some(42), ohh no!")]
    fn test_assert_none_fail_with_msg() {
        assert_none!(Some(42), "ohh no!");
    }

    #[crate::test]
    fn test_assert_ok() {
        assert_ok!(Ok::<_, usize>(42));
    }

    #[crate::test]
    #[should_panic(expected = "assertion failed: expected Ok found Err(42)")]
    fn test_assert_ok_fail() {
        assert_ok!(Err::<usize, _>(42));
    }

    #[crate::test]
    #[should_panic(expected = "assertion failed: expected Ok found Err(42), ohh no!")]
    fn test_assert_ok_fail_with_msg() {
        assert_ok!(Err::<usize, _>(42), "ohh no!");
    }

    #[crate::test]
    fn test_assert_err() {
        assert_err!(Err::<usize, _>(42));
    }

    #[crate::test]
    #[should_panic(expected = "assertion failed: expected Err found Ok(42)")]
    fn test_assert_err_fail() {
        assert_err!(Ok::<_, usize>(42));
    }

    #[crate::test]
    #[should_panic(expected = "assertion failed: expected Err found Ok(42), ohh no!")]
    fn test_assert_err_fail_with_msg() {
        assert_err!(Ok::<_, usize>(42), "ohh no!");
    }
}
