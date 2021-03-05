// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! Assertion utilities.

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
/// use ore::assert_contains;
/// assert_contains!("hello", "ello");
/// ```
///
/// Check whether a slice contains an element:
///
/// ```
/// use ore::assert_contains;
/// assert_contains!(&[1, 2, 3], 2);
/// ```
///
/// Failed assertions panic:
///
/// ```should_panic
/// use ore::assert_contains;
/// assert_contains!("hello", "yellow");
/// ```
#[macro_export]
macro_rules! assert_contains {
    ($left:expr, $right:expr $(,)?) => {{
        if !$left.contains(&$right) {
            panic!(
                r#"assertion failed: `left.contains(right)`:
  left: `{:?}`
 right: `{:?}`"#,
                $left, $right
            );
        }
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_assert_contains_str() {
        assert_contains!("hello", "ello");
    }

    #[test]
    fn test_assert_contains_slice() {
        assert_contains!(&[1, 2, 3], 2);
    }

    #[test]
    #[should_panic(expected = "assertion failed: `left.contains(right)`:
  left: `\"hello\"`
 right: `\"yellow\"`")]
    fn test_assert_contains_fail() {
        assert_contains!("hello", "yellow");
    }
}
