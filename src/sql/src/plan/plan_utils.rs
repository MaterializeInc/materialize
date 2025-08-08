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

use mz_repr::RelationDesc;

use crate::ast::Ident;
use crate::normalize;
use crate::plan::PlanError;
use crate::plan::query::SelectOptionExtracted;

/// Renames the columns in `desc` with the names in `column_names` if
/// `column_names` is non-empty.
///
/// Returns an error if the length of `column_names` is greater than the arity
/// of `desc`.
pub fn maybe_rename_columns(
    context: impl fmt::Display,
    desc: &mut RelationDesc,
    column_names: &[Ident],
) -> Result<(), PlanError> {
    if column_names.len() > desc.typ().column_types.len() {
        sql_bail!(
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

    for (i, name) in column_names.iter().enumerate() {
        *desc.get_name_mut(i) = normalize::column_name(name.clone());
    }

    Ok(())
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

/// Specifies a bundle of group size query hints.
///
/// This struct bridges from old to new syntax for group size query hints,
/// making it easier to pass these hints along and make use of a group size
/// hint configuration.
#[derive(Debug, Default, Clone, Copy)]
pub struct GroupSizeHints {
    pub aggregate_input_group_size: Option<u64>,
    pub distinct_on_input_group_size: Option<u64>,
    pub limit_input_group_size: Option<u64>,
}

impl TryFrom<SelectOptionExtracted> for GroupSizeHints {
    type Error = PlanError;

    /// Creates group size hints from extracted `SELECT` `OPTIONS` validating that
    /// either the old `EXPECTED GROUP SIZE` syntax was used or alternatively the
    /// new syntax with `AGGREGATE INPUT GROUP SIZE`, `DISTINCT ON INPUT GROUP SIZE`,
    /// and `LIMIT INPUT GROUP SIZE`. If the two syntax versions are mixed in the
    /// same `OPTIONS` clause, an error is returned.[^1]
    /// [^1]: <https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20230829_topk_size_hint.md>
    fn try_from(select_option_extracted: SelectOptionExtracted) -> Result<Self, Self::Error> {
        let SelectOptionExtracted {
            expected_group_size,
            aggregate_input_group_size,
            distinct_on_input_group_size,
            limit_input_group_size,
            ..
        } = select_option_extracted;
        if expected_group_size.is_some()
            && (aggregate_input_group_size.is_some()
                || distinct_on_input_group_size.is_some()
                || limit_input_group_size.is_some())
        {
            Err(PlanError::InvalidGroupSizeHints)
        } else {
            let aggregate_input_group_size = aggregate_input_group_size.or(expected_group_size);
            let distinct_on_input_group_size = distinct_on_input_group_size.or(expected_group_size);
            let limit_input_group_size = limit_input_group_size.or(expected_group_size);
            Ok(GroupSizeHints {
                aggregate_input_group_size,
                distinct_on_input_group_size,
                limit_input_group_size,
            })
        }
    }
}
