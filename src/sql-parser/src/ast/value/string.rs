// Copyright Materialize, Inc. All rights reserved.
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

use std::fmt;

use super::ValueError;
use crate::ast::display::{AstDisplay, AstFormatter};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TrimSide {
    Both,
    Leading,
    Trailing,
}

impl AstDisplay for TrimSide {
    fn fmt(&self, f: &mut AstFormatter) {
        match self {
            TrimSide::Both => f.write_str("btrim"),
            TrimSide::Leading => f.write_str("ltrim"),
            TrimSide::Trailing => f.write_str("rtrim"),
        }
    }
}
impl_display!(TrimSide);

use std::str::FromStr;

impl FromStr for TrimSide {
    type Err = ValueError;
    fn from_str(s: &str) -> Result<TrimSide, Self::Err> {
        Ok(match &*s.to_uppercase() {
            "BOTH" => TrimSide::Both,
            "LEADING" => TrimSide::Leading,
            "TRAILING" => TrimSide::Trailing,
            _ => return Err(ValueError(format!("invalid TRIM specifier: {}", s))),
        })
    }
}
