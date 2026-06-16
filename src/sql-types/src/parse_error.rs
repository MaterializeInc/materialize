// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

/// A leaf error type for the `FromStr`/`TryFrom` impls on the types in this crate.
///
/// `mz-sql` converts this into its richer `PlanError` via a `From` impl, so callers
/// using `?` in a planner context keep working. This crate must not depend on
/// `PlanError`, which lives high in the dependency graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// An unstructured parse error with a human-readable message.
    Unstructured(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseError::Unstructured(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for ParseError {}

impl From<std::num::ParseIntError> for ParseError {
    fn from(e: std::num::ParseIntError) -> Self {
        ParseError::Unstructured(e.to_string())
    }
}
