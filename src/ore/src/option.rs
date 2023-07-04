// Copyright 2019 The Rust Project Contributors
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

use std::fmt;
use std::ops::Deref;

use either::Either;

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

    /// Returns a type that displays the option's value if it is present, or
    /// the provided default otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use mz_ore::option::OptionExt;
    ///
    /// fn render(number: Option<i32>) -> String {
    ///     format!("Your lucky number is {}.", number.display_or("unknown"))
    /// }
    ///
    /// assert_eq!(render(Some(42)), "Your lucky number is 42.");
    /// assert_eq!(render(None), "Your lucky number is unknown.");
    /// ```
    fn display_or<D>(self, default: D) -> Either<T, D>
    where
        T: fmt::Display,
        D: fmt::Display;

    /// Like [`OptionExt::display_or`], but the default value is computed
    /// only if the option is `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use mz_ore::option::OptionExt;
    ///
    /// fn render(number: Option<i32>, guess: i32) -> String {
    ///     format!(
    ///         "Your lucky number is {}.",
    ///         number.display_or_else(|| format!("unknown (best guess: {})", guess)),
    ///     )
    /// }
    ///
    /// assert_eq!(render(Some(42), 7), "Your lucky number is 42.");
    /// assert_eq!(render(None, 7), "Your lucky number is unknown (best guess: 7).");
    /// ```
    fn display_or_else<D, R>(self, default: D) -> Either<T, R>
    where
        T: fmt::Display,
        D: FnOnce() -> R,
        R: fmt::Display;
}

impl<T> OptionExt<T> for Option<T> {
    fn owned(&self) -> Option<<<T as Deref>::Target as ToOwned>::Owned>
    where
        T: Deref,
        T::Target: ToOwned,
    {
        self.as_ref().map(|x| x.deref().to_owned())
    }

    fn display_or<D>(self, default: D) -> Either<T, D>
    where
        T: fmt::Display,
        D: fmt::Display,
    {
        match self {
            Some(t) => Either::Left(t),
            None => Either::Right(default),
        }
    }

    fn display_or_else<D, R>(self, default: D) -> Either<T, R>
    where
        T: fmt::Display,
        D: FnOnce() -> R,
        R: fmt::Display,
    {
        match self {
            Some(t) => Either::Left(t),
            None => Either::Right(default()),
        }
    }
}

/// From <https://github.com/rust-lang/rust/issues/38282#issuecomment-266275785>
///
/// Extend `Option` with a fallible map method.
///
/// (Note that the usual collect trick can't be used with Option, unless I'm missing something.)
///
/// # Type parameters
///
/// - `T`: The input `Option`'s value type
/// - `U`: The outputs `Option`'s value type
/// - `E`: The possible error during the mapping
pub trait FallibleMapExt<T, U, E> {
    /// Try to apply a fallible map function to the option
    fn try_map<F>(&self, f: F) -> Result<Option<U>, E>
    where
        F: FnOnce(&T) -> Result<U, E>;
}

impl<T, U, E> FallibleMapExt<T, U, E> for Option<T> {
    fn try_map<F>(&self, f: F) -> Result<Option<U>, E>
    where
        F: FnOnce(&T) -> Result<U, E>,
    {
        match self {
            &Some(ref x) => f(x).map(Some),
            &None => Ok(None),
        }
    }
}
