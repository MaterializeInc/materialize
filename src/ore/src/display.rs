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

//! Display utilities.

use std::fmt::Display;

/// Extension methods for [`std::fmt::Display`].
pub trait DisplayExt {
    /// Formats an object with the "alternative" format (`{:#}`) and returns it.
    fn to_string_alt(&self) -> String;
}

impl<T: Display> DisplayExt for T {
    fn to_string_alt(&self) -> String {
        format!("{:#}", self)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn prints_alternate_repr() {
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

        assert_eq!(Foo.to_string_alt(), "success");
    }
}
