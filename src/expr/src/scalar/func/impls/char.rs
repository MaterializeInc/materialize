// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr_derive::sqlfunc;
use mz_repr::SqlScalarType;
use mz_repr::adt::char::{Char, CharLength, format_str_pad};
use serde::{Deserialize, Serialize};

/// All Char data is stored in Datum::String with its blank padding removed
/// (i.e. trimmed), so this function provides a means of restoring any
/// removed padding.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
pub struct PadChar {
    pub length: Option<CharLength>,
}

#[sqlfunc(
    PadChar,
    sqlname = "padchar",
    output_type_expr = SqlScalarType::Char { length: self.length }.nullable(input_type.nullable)
)]
fn pad_char<'a>(&self, a: &'a str) -> Char<String> {
    Char(format_str_pad(a, self.length))
}

// This function simply allows the expression of changing a's type from char to
// string
#[sqlfunc(
    sqlname = "char_to_text",
    preserves_uniqueness = true,
    is_eliminable_cast = true,
    inverse = super::CastStringToChar{
        length: None,
        fail_on_len: false,
    }
)]
fn cast_char_to_string<'a>(a: Char<&'a str>) -> &'a str {
    a.0
}
