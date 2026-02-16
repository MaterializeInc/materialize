// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

impl Debug for Password {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Password(****)")
    }
}
