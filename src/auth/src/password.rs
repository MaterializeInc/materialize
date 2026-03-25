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

impl ToString for Password {
    fn to_string(&self) -> String {
        self.0.clone()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_password_implements_drop() {
        assert!(std::mem::needs_drop::<Password>());
    }

    #[mz_ore::test]
    fn test_password_round_trip() {
        let p = Password::from("hunter2");
        assert_eq!(p.to_string(), "hunter2");
    }
}
