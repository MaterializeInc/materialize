// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helper code used throughout the planner.

use std::fmt;

use anyhow::bail;

use mz_repr::RelationDesc;

use crate::ast::Ident;
use crate::normalize;

/// Renames the columns in `desc` with the names in `column_names` if
/// `column_names` is non-empty.
///
/// Returns an error if the length of `column_names` is not either zero or the
/// arity of `desc`.
pub fn maybe_rename_columns(
    context: impl fmt::Display,
    desc: RelationDesc,
    column_names: &[Ident],
) -> Result<RelationDesc, anyhow::Error> {
    if column_names.is_empty() {
        return Ok(desc);
    }

    if column_names.len() != desc.typ().column_types.len() {
        bail!(
            "{0} definition names {1} column{2}, but {0} has {3} column{4}",
            context,
            column_names.len(),
            if column_names.len() == 1 { "" } else { "s" },
            desc.typ().column_types.len(),
            if desc.typ().column_types.len() == 1 {
                ""
            } else {
                "s"
            },
        )
    }

    let new_names = column_names
        .iter()
        .map(|n| normalize::column_name(n.clone()));

    Ok(desc.with_names(new_names))
}

/// Specifies the side of a join.
///
/// Intended for use in error messages.
#[derive(Debug, Clone, Copy)]
pub enum JoinSide {
    /// The left side.
    Left,
    /// The right side.
    Right,
}

impl fmt::Display for JoinSide {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JoinSide::Left => f.write_str("left"),
            JoinSide::Right => f.write_str("right"),
        }
    }
}
