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

use std::convert::Infallible;

use crate::error::ErrorExt;

/// Extension methods for [`std::result::Result`].
pub trait ResultExt<T, E> {
    /// Applies [`Into::into`] to a contained [`Err`] value, leaving an [`Ok`]
    /// value untouched.
    fn err_into<E2>(self) -> Result<T, E2>
    where
        E: Into<E2>;

    /// Formats an [`Err`] value as a detailed error message, preserving any context information.
    ///
    /// This is equivalent to `format!("{}", err.display_with_causes())`, except that it's easier to
    /// type.
    fn err_to_string_with_causes(&self) -> Option<String>
    where
        E: std::error::Error;

    /// Maps a `Result<T, E>` to `Result<T, String>` by converting the [`Err`] result into a string,
    /// along with the chain of source errors, if any.
    fn map_err_to_string_with_causes(self) -> Result<T, String>
    where
        E: std::error::Error;

    /// Safely unwraps a `Result<T, Infallible>`, where [`Infallible`] is a type that represents when
    /// an error cannot occur.
    fn infallible_unwrap(self) -> T
    where
        E: Into<Infallible>;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn err_into<E2>(self) -> Result<T, E2>
    where
        E: Into<E2>,
    {
        self.map_err(|e| e.into())
    }

    fn err_to_string_with_causes(&self) -> Option<String>
    where
        E: std::error::Error,
    {
        self.as_ref().err().map(ErrorExt::to_string_with_causes)
    }

    fn map_err_to_string_with_causes(self) -> Result<T, String>
    where
        E: std::error::Error,
    {
        self.map_err(|e| ErrorExt::to_string_with_causes(&e))
    }

    fn infallible_unwrap(self) -> T
    where
        E: Into<Infallible>,
    {
        match self {
            Ok(t) => t,
            Err(e) => {
                let _infallible = e.into();

                // This code will forever be unreachable because Infallible is an enum
                // with no variants, so it's impossible to construct. If it ever does
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

    use anyhow::anyhow;

    use super::*;

    #[mz_test_macro::test]
    fn prints_error_chain() {
        let error = anyhow!("root");
        let error = error.context("context");
        let error = TestError { inner: error };
        let res: Result<(), _> = Err(error);

        assert_eq!(
            res.err_to_string_with_causes(),
            Some("test error: context: root".to_string())
        );

        let error = anyhow!("root");
        let error = error.context("context");
        let error = TestError { inner: error };
        let res: Result<(), _> = Err(error);

        assert_eq!(
            res.map_err_to_string_with_causes(),
            Err("test error: context: root".to_string())
        );
    }

    #[derive(Debug)]
    struct TestError {
        inner: anyhow::Error,
    }

    impl Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            // We don't print our causes.
            write!(f, "test error")?;
            Ok(())
        }
    }

    impl std::error::Error for TestError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.inner.as_ref())
        }
    }
}
