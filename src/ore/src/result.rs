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

    /// Safely unwraps a `Result<T, Infallible>`, where [`Infallible`] is a type that represents when
    /// an error cannot occur.
    ///
    /// [`Infallible`]: core::convert::Infallible
    fn infallible_unwrap(self) -> T
    where
        E: Into<core::convert::Infallible>;
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

    fn infallible_unwrap(self) -> T
    where
        E: Into<core::convert::Infallible>,
    {
        match self {
            Ok(t) => t,
            Err(e) => {
                let _infallible = e.into();

                // This code will forever be unreachable because Infallible is an enum
                // with no variants, so it's impossible to consturct. If it ever does
                // become possible to construct this will become a compile time error
                // since there will be a variant we're not matching on.
                #[allow(unreachable_code)]
                match _infallible {}
            }
        }
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
