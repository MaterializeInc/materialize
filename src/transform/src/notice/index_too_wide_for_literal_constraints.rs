// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hosts [`IndexTooWideForLiteralConstraints`].

use std::collections::BTreeSet;
use std::fmt;

use itertools::zip_eq;
use mz_expr::explain::{HumanizedNotice, HumanizerMode};
use mz_expr::MirScalarExpr;
use mz_ore::str::separated;
use mz_repr::explain::ExprHumanizer;
use mz_repr::GlobalId;
use mz_repr::Row;

use crate::notice::{ActionKind, OptimizerNoticeApi};

/// An index could be used for some literal constraints if the index included only a subset of its
/// columns.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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

impl OptimizerNoticeApi for IndexTooWideForLiteralConstraints {
    fn dependencies(&self) -> BTreeSet<GlobalId> {
        BTreeSet::from([self.index_id, self.index_on_id])
    }

    fn fmt_message(
        &self,
        f: &mut fmt::Formatter<'_>,
        humanizer: &dyn ExprHumanizer,
        redacted: bool,
    ) -> fmt::Result {
        let col_names = humanizer.column_names_for_id(self.index_on_id);
        let col_names = col_names.as_ref();

        let index_name = humanizer
            .humanize_id(self.index_id)
            .unwrap_or_else(|| self.index_id.to_string());
        let index_on_id_name = humanizer
            .humanize_id_unqualified(self.index_on_id)
            .unwrap_or_else(|| self.index_on_id.to_string());

        let mode = HumanizedNotice::new(redacted);

        let index_key = separated(", ", mode.seq(&self.index_key, col_names));
        write!(
            f,
            "Index {index_name} on {index_on_id_name}({index_key}) \
             is too wide to use for literal equalities "
        )?;

        write!(f, "`")?;
        {
            if self.usable_subset.len() == 1 {
                let exprs = mode.expr(&self.usable_subset[0], col_names);
                let lits = self
                    .literal_values
                    .iter()
                    .map(|l| l.unpack_first())
                    .collect::<Vec<_>>();
                let mut lits = mode.seq(&lits, col_names);
                if self.literal_values.len() == 1 {
                    write!(f, "{} = {}", exprs, lits.next().unwrap())?;
                } else {
                    write!(f, "{} IN ({})", exprs, separated(", ", lits))?;
                }
            } else {
                if self.literal_values.len() == 1 {
                    let exprs = mode.seq(&self.usable_subset, col_names);
                    let lits = self.literal_values[0].unpack();
                    let lits = mode.seq(&lits, col_names);
                    let eqs = zip_eq(exprs, lits).map(|(expr, lit)| format!("{} = {}", expr, lit));
                    write!(f, "{}", separated(" AND ", eqs))?;
                } else {
                    let exprs = mode.seq(&self.usable_subset, col_names);
                    let lits = mode.seq(&self.literal_values, col_names);
                    write!(
                        f,
                        "({}) IN ({})",
                        separated(", ", exprs),
                        separated(", ", lits)
                    )?;
                }
            };
        }
        write!(f, "`.")
    }

    fn fmt_hint(
        &self,
        f: &mut fmt::Formatter<'_>,
        humanizer: &dyn ExprHumanizer,
        redacted: bool,
    ) -> fmt::Result {
        let col_names = humanizer.column_names_for_id(self.index_on_id);

        let mode = HumanizedNotice::new(redacted);

        let recommended_key = mode.seq(&self.recommended_key, col_names.as_ref());
        let recommended_key = separated(", ", recommended_key);

        // TODO: Also print whether the index is used elsewhere (for something that is not a
        // full scan), so that the user knows whether to delete the old index.
        write!(
            f,
            "If your literal equalities filter out many rows, \
             create an index whose key exactly matches your literal equalities: \
             ({recommended_key})."
        )
    }

    fn fmt_action(
        &self,
        f: &mut fmt::Formatter<'_>,
        humanizer: &dyn ExprHumanizer,
        redacted: bool,
    ) -> fmt::Result {
        let Some(index_on_id_name) = humanizer.humanize_id_unqualified(self.index_on_id) else {
            return Ok(());
        };

        let mode = HumanizedNotice::new(redacted);
        let col_names = humanizer.column_names_for_id(self.index_on_id);

        let recommended_key = mode.seq(&self.recommended_key, col_names.as_ref());
        let recommended_key = separated(", ", recommended_key);

        write!(f, "CREATE INDEX ON {index_on_id_name}({recommended_key});")
    }

    fn action_kind(&self, humanizer: &dyn ExprHumanizer) -> ActionKind {
        match humanizer.humanize_id_unqualified(self.index_on_id) {
            Some(_) => ActionKind::SqlStatements,
            None => ActionKind::None,
        }
    }
}
