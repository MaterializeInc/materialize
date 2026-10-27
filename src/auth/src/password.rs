// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::secure::Zeroize;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};

use static_assertions::assert_not_impl_all;

///Password is a String wrapper type that does not implement Display or Debug
#[derive(
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
    Arbitrary
)]
pub struct Password(pub String);

assert_not_impl_all!(Password: Display);

impl From<String> for Password {
    fn from(password: String) -> Self {
        Password(password)
    }
}

impl From<&str> for Password {
    fn from(password: &str) -> Self {
        Password(password.to_string())
    }
}

impl Password {
    /// Returns the password as a byte slice, suitable for hashing.
    ///
    /// Unlike the former `to_string().as_bytes()` pattern, this does not
    /// create an unzeroed `String` copy on the heap.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns the password as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Drop for Password {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl Debug for Password {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Password(****)")
    }
}
