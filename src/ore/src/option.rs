// Copyright 2019 The Rust Project Contributors
// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the Option implementation in the
// libcore crate distributed as part of the Rust project. The original source
// code was retrieved on April 18, 2019 from:
//
//     https://github.com/rust-lang/rust/blob/e928e9441157f63a776ba1f8773818838e0912ea/src/libcore/option.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Option utilities.

use std::fmt::Display;
use std::ops::Deref;

/// Extension methods for [`std::option::Option`].
pub trait OptionExt<T> {
    /// Converts from `Option<&T>` to `Option<T::Owned>` when `T` implements
    /// [`ToOwned`].
    ///
    /// The canonical use case is converting from an `Option<&str>` to an
    /// `Option<String>`.
    ///
    /// The name is symmetric with [`Option::cloned`].
    fn owned(&self) -> Option<<<T as Deref>::Target as ToOwned>::Owned>
    where
        T: Deref,
        T::Target: ToOwned;
}

impl<T> OptionExt<T> for Option<T> {
    fn owned(&self) -> Option<<<T as Deref>::Target as ToOwned>::Owned>
    where
        T: Deref,
        T::Target: ToOwned,
    {
        self.as_ref().map(|x| x.deref().to_owned())
    }
}

/// Extension methods for things, like `[Option]`, that are displayable if a default value is supplied.
pub trait OrDisplay<T, D> {
    /// The type returned by the [`or_display`] function
    type Displayable: Display;
    /// Wrap the object in a type that can be displayed, by providing it with a default display value.
    fn or_display(self, default: D) -> Self::Displayable;
}

#[derive(Debug)]
/// A wrapper for `[Option]`, which is displayable if
/// a default display value is provided.
pub struct DisplayableOption<T, D> {
    inner: Option<T>,
    default: D,
}

impl<T, D> DisplayableOption<T, D> {
    /// Consume the `DisplayableOption` and return its constituent parts
    pub fn into_parts(self) -> (Option<T>, D) {
        (self.inner, self.default)
    }
}

impl<T, D> Display for DisplayableOption<T, D>
where
    D: Display,
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Some(t) => write!(f, "{}", t),
            None => write!(f, "{}", self.default),
        }
    }
}

impl<T, D> OrDisplay<T, D> for Option<T>
where
    D: Display,
    T: Display,
{
    type Displayable = DisplayableOption<T, D>;

    fn or_display(self, default: D) -> Self::Displayable {
        DisplayableOption {
            inner: self,
            default,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::option::OrDisplay;
    #[test]
    fn test_displayable_option() {
        let mut maybe = Some(42);
        let s = format!("Value is {}", maybe.or_display("unknown"));
        assert_eq!(&s, "Value is 42");

        maybe = None;
        let s = format!("Value is {}", maybe.or_display("unknown"));
        assert_eq!(&s, "Value is unknown");
    }
}
