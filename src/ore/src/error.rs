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

//! Error utilities.

use std::error::Error;
use std::fmt::{self, Debug, Display};

/// Extension methods for [`std::error::Error`].
pub trait ErrorExt: Error {
    /// Returns a type that displays the error, along with the chain of _source_ errors or
    /// causes, if there are any.
    ///
    /// # Examples
    ///
    /// ```
    /// use anyhow::anyhow;
    /// use mz_ore::error::ErrorExt;
    ///
    /// let error = anyhow!("inner");
    /// let error = error.context("context");
    /// assert_eq!(format!("error: ({})", error.display_with_causes()), "error: (context: inner)");
    /// ```
    fn display_with_causes(&self) -> ErrorChainFormatter<&Self> {
        ErrorChainFormatter(self)
    }

    /// Converts `self` to a string `String`, along with the chain of _source_ errors or
    /// causes, if there are any.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use anyhow::anyhow;
    /// use mz_ore::error::ErrorExt;
    ///
    /// let error = anyhow!("inner");
    /// let error = error.context("context");
    /// assert_eq!(error.to_string_with_causes(), "context: inner");
    /// ```
    fn to_string_with_causes(&self) -> String {
        format!("{}", self.display_with_causes())
    }
}

impl<E: Error + ?Sized> ErrorExt for E {}

/// Formats an error with its full chain of causes.
pub struct ErrorChainFormatter<E>(E);

impl<E: Error> Display for ErrorChainFormatter<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)?;

        let mut maybe_cause = self.0.source();

        while let Some(cause) = maybe_cause {
            write!(f, ": {}", cause)?;
            maybe_cause = cause.source();
        }

        Ok(())
    }
}

impl<E: Error> Debug for ErrorChainFormatter<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::anyhow;

    use super::*;

    #[mz_test_macro::test]
    fn basic_usage() {
        let error = anyhow!("root");
        let error = error.context("context");
        assert_eq!(error.to_string_with_causes(), "context: root");
    }

    #[mz_test_macro::test]
    fn basic_usage_with_arc() {
        // The reason for this signature is that our Plan errors have a `cause` field like this:
        // ```
        // cause: Arc<dyn Error + Send + Sync>
        // ```
        fn verify(dyn_error: Arc<dyn Error + Send + Sync>) {
            assert_eq!(
                dyn_error.to_string_with_causes(),
                "test error: context: root"
            );
        }

        let error = anyhow!("root");
        let error = error.context("context");
        let error = TestError { inner: error };
        let arc_error = Arc::new(error);
        verify(arc_error);
    }

    #[derive(Debug)]
    struct TestError {
        inner: anyhow::Error,
    }

    impl Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // We don't print our causes.
            write!(f, "test error")?;
            Ok(())
        }
    }

    impl std::error::Error for TestError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self.inner.as_ref())
        }
    }
}
