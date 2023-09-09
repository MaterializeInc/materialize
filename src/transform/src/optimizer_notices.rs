// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Notices that the optimizer wants to show to users.

use std::fmt::{Error, Write};

use itertools::Itertools;

use mz_expr::explain::display_singleton_row;
use mz_expr::MirScalarExpr;
use mz_ore::str::separated;
use mz_repr::explain::ExprHumanizer;
use mz_repr::{GlobalId, Row};

/// Notices that the optimizer wants to show to users.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(missing_docs)]
pub enum OptimizerNotice {
    IndexTooWideForLiteralConstraints(IndexTooWideForLiteralConstraints),
    /// An index with an empty key is maximally skewed (all of the data goes to a single worker),
    /// and is almost never really useful. It's slightly useful for a cross join, because a cross
    /// join also has an empty key, so we avoid rearranging the input. However, this is still
    /// not very useful, because
    ///  - Rearranging the input shouldn't take too much memory, because if a cross join has a big
    ///    input, then we have a serious problem anyway.
    ///  - Even with the arrangement already there, the cross join will read every input record, so
    ///    the orders of magnitude performance improvements that can happen with other joins when an
    ///    input arrangement exists can't happen with a cross join.
    /// Also note that skew is hard to debug, so it's good to avoid this problem in the first place.
    IndexKeyEmpty,
}

impl OptimizerNotice {
    /// Turns the `OptimizerNotice` into a string, using the given `ExprHumanizer`.
    pub fn to_string(&self, humanizer: &dyn ExprHumanizer) -> (String, Option<String>) {
        match self {
            OptimizerNotice::IndexTooWideForLiteralConstraints(
                IndexTooWideForLiteralConstraints {
                    index_id,
                    index_key,
                    usable_subset,
                    literal_values,
                    index_on_id,
                    recommended_key,
                },
            ) => {
                let index_name = humanizer
                    .humanize_id(*index_id)
                    .unwrap_or_else(|| index_id.to_string());
                let index_on_id_name = humanizer
                    .humanize_id_unqualified(*index_on_id)
                    .unwrap_or_else(|| index_on_id.to_string());
                let col_names = humanizer.column_names_for_id(*index_on_id);
                let display_exprs = |exprs: &Vec<MirScalarExpr>| {
                    separated(
                        ", ",
                        exprs
                            .clone()
                            .into_iter()
                            .map(|expr| expr.to_string_with_col_names(&col_names)),
                    )
                };
                let index_key_display = display_exprs(index_key);
                let usable_subset_display = display_exprs(usable_subset);
                let recommended_cols_display = display_exprs(recommended_key);

                let usable_subset = usable_subset
                    .into_iter()
                    .map(|expr| expr.clone().to_string_with_col_names(&col_names))
                    .collect_vec();
                let usable_literal_constraints_display = if usable_subset.len() == 1 {
                    if literal_values.len() == 1 {
                        format!(
                            "{} = {}",
                            usable_subset[0],
                            display_singleton_row(literal_values[0].clone())
                        )
                    } else {
                        format!(
                            "{} IN ({})",
                            usable_subset[0],
                            separated(
                                ", ",
                                literal_values
                                    .iter()
                                    .map(|r| display_singleton_row(r.clone()))
                            )
                        )
                    }
                } else {
                    if literal_values.len() == 1 {
                        format!(
                            "{}",
                            separated(
                                " AND ",
                                usable_subset
                                    .into_iter()
                                    .zip_eq(literal_values[0].into_iter())
                                    .map(|(key_field, value)| {
                                        format!("{} = {}", key_field, value)
                                    })
                            )
                        )
                    } else {
                        format!(
                            "({}) IN ({})",
                            usable_subset_display,
                            separated(", ", literal_values)
                        )
                    }
                };

                // TODO: Also print whether the index is used elsewhere (for something that is not a
                // full scan), so that the user knows whether to delete the old index.
                (
                    format!("Index {index_name} on {index_on_id_name}({index_key_display}) is too wide to use for literal equalities `{usable_literal_constraints_display}`."),
                    Some(format!("If your literal equalities filter out many rows, create an index whose key exactly matches your literal equalities: ({recommended_cols_display}).")),
                )
            }
            OptimizerNotice::IndexKeyEmpty =>
                (
                    "Empty index key. The index will be completely skewed to one worker thread, which can lead to performance problems.".to_string(),
                    Some("CREATE DEFAULT INDEX is almost always better than an index with an empty key. (Except for cross joins with big inputs, which are better to avoid anyway.)".to_string()),
                )
        }
    }

    /// Turns a `Vec<OptimizerNotice>` into a String that can be used in EXPLAIN.
    pub fn explain(
        notices: &Vec<OptimizerNotice>,
        humanizer: &dyn ExprHumanizer,
    ) -> Result<Vec<String>, Error> {
        let mut notice_strings = Vec::new();
        for notice in notices {
            if notice.is_valid(humanizer) {
                let (notice, hint) = notice.to_string(humanizer);
                let mut s = String::new();
                write!(s, "  - Notice: {}", notice)?;
                match hint {
                    Some(hint) => write!(s, "\n    Hint: {}", hint)?,
                    None => {}
                }
                notice_strings.push(s);
            }
        }
        Ok(notice_strings)
    }

    /// Returns whether the ids mentioned in the notice still exist.
    pub fn is_valid(&self, humanizer: &dyn ExprHumanizer) -> bool {
        match self {
            OptimizerNotice::IndexTooWideForLiteralConstraints(
                IndexTooWideForLiteralConstraints {
                    index_id,
                    index_on_id,
                    ..
                },
            ) => humanizer.id_exists(*index_id) && humanizer.id_exists(*index_on_id),
            OptimizerNotice::IndexKeyEmpty => true,
        }
    }
}

/// An index could be used for some literal constraints if the index included only a subset of its
/// columns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexTooWideForLiteralConstraints {
    /// The id of the index.
    pub index_id: GlobalId,
    /// The key of the index.
    pub index_key: Vec<MirScalarExpr>,
    /// A subset of the index keys. If the index were only on these keys, then it could have been
    /// used for a lookup.
    pub usable_subset: Vec<MirScalarExpr>,
    /// Literal values that we would have looked up in the index, if it were on `usable_subset`.
    pub literal_values: Vec<Row>,
    /// The id of the object that the index is on.
    pub index_on_id: GlobalId,
    /// Our recommendation for what key should a new index have. Note that this might include more
    /// columns than `usable_subset`.
    pub recommended_key: Vec<MirScalarExpr>,
}
