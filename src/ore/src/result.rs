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

//! Result utilities.

use crate::display::DisplayExt;

/// Error wrapper that conveys severity information
#[derive(Debug)]
pub enum Severity<E> {
    /// A recoverable error
    Recoverable(E),
    /// A fatal error
    Fatal(E),
}

/// Extension methods for [`std::result::Result`].
pub trait ResultExt<T, E> {
    /// Applies [`Into::into`] to a contained [`Err`] value, leaving an [`Ok`]
    /// value untouched.
    fn err_into<E2>(self) -> Result<T, E2>
    where
        E: Into<E2>;

    /// Formats an [`Err`] value as a detailed error message, preserving any context information.
    ///
    /// This is equivalent to `format!("{:#}", err)`, except that it's easier to type.
    fn err_to_string(&self) -> Option<String>
    where
        E: std::fmt::Display;

    /// Maps a `Result<T, E>` to `Result<T, String>` by converting the [`Err`] result into a string
    /// using the "alternate" formatting.
    fn map_err_to_string(self) -> Result<T, String>
    where
        E: std::fmt::Display;

    /// Mark an error as recoverable
    fn recoverable(self) -> Result<T, Severity<E>>;

    /// Mark an error as fatal
    fn fatal(self) -> Result<T, Severity<E>>;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn err_into<E2>(self) -> Result<T, E2>
    where
        E: Into<E2>,
    {
        self.map_err(|e| e.into())
    }

    fn err_to_string(&self) -> Option<String>
    where
        E: std::fmt::Display,
    {
        self.as_ref().err().map(DisplayExt::to_string_alt)
    }

    fn map_err_to_string(self) -> Result<T, String>
    where
        E: std::fmt::Display,
    {
        self.map_err(|e| DisplayExt::to_string_alt(&e))
    }

    fn recoverable(self) -> Result<T, Severity<E>> {
        self.map_err(Severity::Recoverable)
    }

    fn fatal(self) -> Result<T, Severity<E>> {
        self.map_err(Severity::Fatal)
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Display;

    use super::*;

    #[test]
    fn prints_err_alternate_repr() {
        struct Foo;
        impl Display for Foo {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                if f.alternate() {
                    write!(f, "success")
                } else {
                    write!(f, "fail")
                }
            }
        }

        let res: Result<(), Foo> = Err(Foo);
        assert_eq!(Some("success".to_string()), res.err_to_string());

        let res: Result<(), Foo> = Err(Foo);
        assert_eq!(Err("success".to_string()), res.map_err_to_string());
    }
}
