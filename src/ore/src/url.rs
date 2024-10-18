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

//! URL utilities.

use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

#[cfg(feature = "proptest")]
use proptest::prelude::{Arbitrary, BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};
use url::Url;

/// A URL that redacts its password when formatted.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SensitiveUrl(pub Url);

impl SensitiveUrl {
    /// Converts into the underlying URL with the password redacted if it
    /// exists.
    pub fn into_redacted(mut self) -> Url {
        if self.0.password().is_some() {
            self.0.set_password(Some("<redacted>")).unwrap();
        }
        self.0
    }

    /// Formats as a string without redacting the password.
    pub fn to_string_unredacted(&self) -> String {
        self.0.to_string()
    }
}

impl fmt::Display for SensitiveUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.clone().into_redacted().fmt(f)
    }
}

impl fmt::Debug for SensitiveUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.clone().into_redacted().fmt(f)
    }
}

impl FromStr for SensitiveUrl {
    type Err = url::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Url::from_str(s)?))
    }
}

impl Deref for SensitiveUrl {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "proptest")]
impl Arbitrary for SensitiveUrl {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        proptest::sample::select(vec![
            SensitiveUrl::from_str("http://user:pass@example.com").unwrap(),
            SensitiveUrl::from_str("http://user@example.com").unwrap(),
            SensitiveUrl::from_str("http://example.com").unwrap(),
        ])
        .boxed()
    }
}
