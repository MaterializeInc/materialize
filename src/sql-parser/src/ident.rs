// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
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

/// A macro that creates an [`Ident`] from a string literal, validating all of our invariants at
/// compile time.
///
/// # Examples
///
/// ```
/// use mz_sql_parser::ident;
///
/// // Checks invariants at compile time, returning an `Ident`.
/// let id = ident!("my_postgres_source");
///
/// assert_eq!(id.as_str(), "my_postgres_source");
/// ```
///
/// ```compile_fail
/// use mz_sql_parser::ident;
///
/// const TOO_LONG: &str = "I am a very long identifier that is more than 255 characters long which is the max length for idents.\
/// I am a very long identifier that is more than 255 characters long which is the max length for idents.\
/// Use that sentance twice since 255 characters is actually a lot.";
///
/// // This fails to build since the string is too long.
/// let _id = ident!(TOO_LONG);
/// ```
///
/// ```compile_fail
/// use mz_sql_parser::ident;
/// const FORBIDDEN: &str = ".";
/// ident!(FORBIDDEN);
/// ```
///
/// ```ignore
/// use mz_sql_parser::ident;
/// const FORBIDDEN: &str = "..";
/// ident!(FORBIDDEN);
/// ```
///
/// [`Ident`]: crate::ast::Ident
///
#[macro_export]
macro_rules! ident {
    ($val:expr) => {{
        let _x: &'static str = $val;
        $crate::ident!(@internal_check 255, $val);

        $crate::ast::Ident::new_unchecked($val)
    }};

    // Internal helper macro to assert the length of the provided string literal is less than our
    // maximum.
    (@internal_check $max_len:literal, $val:expr) => {{
        #[allow(dead_code)]
        const fn check_len<const MAX: usize, const LEN: usize>() {
            if LEN > MAX {
                panic!(stringify!(length of provided string literal, $val, is greater than specified max of $max_len));
            }
        }

        #[allow(dead_code)]
        const fn check_value(val: &str) {
            if equal(val, ".") || equal(val, "..") {
                panic!(stringify!(provided string literal, $val, is an invalid identifier));
            }
        }

        // `str` does not implement `const PartialEq`, which is why we need to
        // hand roll this equals function.
        const fn equal(lhs: &str, rhs: &str) -> bool {
            let lhs = lhs.as_bytes();
            let rhs = rhs.as_bytes();
            if lhs.len() != rhs.len() {
                return false;
            }
            let mut i = 0;
            while i < lhs.len() {
                if lhs[i] != rhs[i] {
                    return false;
                }
                i += 1;
            }
            true
        }

        const _X_VAL: &str = $val;
        const _X_LEN: usize = _X_VAL.len();
        const _X_CHECK_LEN: () = check_len::<$max_len, _X_LEN>();
        const _X_CHECK_VALUE: () = check_value(_X_VAL);
    }}
}
