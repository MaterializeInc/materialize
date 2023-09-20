// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Notices that the optimizer wants to show to users.

use std::fmt::{self, Error, Write};

use itertools::zip_eq;

use mz_expr::explain::{display_singleton_row, HumanizedExpr};
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

impl OptimizerNotice {
    /// Turns the `OptimizerNotice` into a string, using the given `ExprHumanizer`.
    pub fn to_string(&self, humanizer: &dyn ExprHumanizer) -> (String, String) {
        (
            HumanizedNoticeMsg::new(self, humanizer).to_string(),
            HumanizedNoticeHint::new(self, humanizer).to_string(),
        )
    }

    /// Turns a `Vec<OptimizerNotice>` into a String that can be used in EXPLAIN.
    pub fn explain(
        notices: &Vec<OptimizerNotice>,
        humanizer: &dyn ExprHumanizer,
    ) -> Result<Vec<String>, Error> {
        let mut notice_strings = Vec::new();
        for notice in notices {
            if notice.is_valid(humanizer) {
                let mut s = String::new();
                let msg = HumanizedNoticeMsg::new(notice, humanizer);
                let hint = HumanizedNoticeHint::new(notice, humanizer);
                write!(s, "  - Notice: {}\n", msg)?;
                write!(s, "    Hint: {}", hint)?;
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

#[derive(Debug)]
struct HumanizedNoticeMsg<'a> {
    notice: &'a OptimizerNotice,
    humanizer: &'a dyn ExprHumanizer,
}

impl<'a> HumanizedNoticeMsg<'a> {
    fn new(notice: &'a OptimizerNotice, humanizer: &'a dyn ExprHumanizer) -> Self {
        Self { notice, humanizer }
    }
}

impl<'a> fmt::Display for HumanizedNoticeMsg<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.notice {
            OptimizerNotice::IndexTooWideForLiteralConstraints(
                IndexTooWideForLiteralConstraints {
                    index_id,
                    index_key,
                    usable_subset,
                    literal_values,
                    index_on_id,
                    ..
                },
            ) => {
                let col_names = self.humanizer.column_names_for_id(*index_on_id);
                let col_names = col_names.as_ref();

                let index_name = self
                    .humanizer
                    .humanize_id(*index_id)
                    .unwrap_or_else(|| index_id.to_string());

                let index_on_id_name = self
                    .humanizer
                    .humanize_id_unqualified(*index_on_id)
                    .unwrap_or_else(|| index_on_id.to_string());

                let index_key = separated(", ", HumanizedExpr::seq(index_key, col_names));

                write!(f, "Index {index_name} on {index_on_id_name}({index_key}) is too wide to use for literal equalities `")?;
                {
                    if usable_subset.len() == 1 {
                        if literal_values.len() == 1 {
                            write!(
                                f,
                                "{} = {}",
                                HumanizedExpr::new(&usable_subset[0], col_names),
                                display_singleton_row(literal_values[0].clone())
                            )?;
                        } else {
                            write!(
                                f,
                                "{} IN ({})",
                                HumanizedExpr::new(&usable_subset[0], col_names),
                                separated(
                                    ", ",
                                    literal_values
                                        .iter()
                                        .map(|r| display_singleton_row(r.clone()))
                                )
                            )?;
                        }
                    } else {
                        if literal_values.len() == 1 {
                            write!(
                                f,
                                "{}",
                                separated(
                                    " AND ",
                                    zip_eq(
                                        usable_subset.into_iter(),
                                        literal_values[0].into_iter()
                                    )
                                    .map(
                                        |(key_field, value)| {
                                            format!("{} = {}", key_field, value)
                                        }
                                    )
                                )
                            )?;
                        } else {
                            write!(
                                f,
                                "({}) IN ({})",
                                separated(", ", HumanizedExpr::seq(usable_subset, col_names)),
                                separated(", ", literal_values)
                            )?;
                        }
                    };
                }
                write!(f, "`.")
            }
            OptimizerNotice::IndexKeyEmpty => {
                write!(f, "Empty index key. The index will be completely skewed to one worker thread, which can lead to performance problems.")
            }
        }
    }
}

#[derive(Debug)]
struct HumanizedNoticeHint<'a> {
    notice: &'a OptimizerNotice,
    humanizer: &'a dyn ExprHumanizer,
}

impl<'a> HumanizedNoticeHint<'a> {
    fn new(notice: &'a OptimizerNotice, humanizer: &'a dyn ExprHumanizer) -> Self {
        Self { notice, humanizer }
    }
}

impl<'a> fmt::Display for HumanizedNoticeHint<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.notice {
            OptimizerNotice::IndexTooWideForLiteralConstraints(
                IndexTooWideForLiteralConstraints {
                    index_on_id,
                    recommended_key,
                    ..
                },
            ) => {
                let col_names = self.humanizer.column_names_for_id(*index_on_id);
                let col_names = col_names.as_ref();
                let recommended_key = {
                    separated(
                        ", ",
                        recommended_key
                            .iter()
                            .map(|expr| HumanizedExpr::new(expr, col_names)),
                    )
                };

                // TODO: Also print whether the index is used elsewhere (for something that is not a
                // full scan), so that the user knows whether to delete the old index.
                write!(f, "If your literal equalities filter out many rows, create an index whose key exactly matches your literal equalities: ({recommended_key}).")
            }
            OptimizerNotice::IndexKeyEmpty => {
                write!(f, "CREATE DEFAULT INDEX is almost always better than an index with an empty key. (Except for cross joins with big inputs, which are better to avoid anyway.)")
            }
        }
    }
}
