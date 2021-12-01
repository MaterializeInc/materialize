// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use serde::{Deserialize, Serialize};

use lowertest::MzStructReflect;
use repr::adt::char::{format_str_pad, Char};
use repr::{ColumnType, ScalarType};

use crate::scalar::func::EagerUnaryFunc;

/// All Char data is stored in Datum::String with its blank padding removed
/// (i.e. trimmed), so this function provides a means of restoring any
/// removed padding.
#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect,
)]
pub struct PadChar {
    pub length: Option<usize>,
}

impl<'a> EagerUnaryFunc<'a> for PadChar {
    type Input = &'a str;
    type Output = Char<String>;

    fn call(&self, a: &'a str) -> Char<String> {
        Char(format_str_pad(a, self.length))
    }

    fn output_type(&self, input: ColumnType) -> ColumnType {
        ScalarType::Char {
            length: self.length,
        }
        .nullable(input.nullable)
    }
}

impl fmt::Display for PadChar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("padchar")
    }
}
