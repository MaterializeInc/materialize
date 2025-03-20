// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hosts [`IndexAlreadyExists`].

use std::collections::BTreeSet;
use std::fmt;

use mz_expr::explain::{HumanizedNotice, HumanizerMode};
use mz_expr::MirScalarExpr;
use mz_ore::str::separated;
use mz_repr::explain::ExprHumanizer;
use mz_repr::GlobalId;

use crate::notice::{ActionKind, OptimizerNoticeApi};

/// Trying to re-create an index that already exists.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct IndexAlreadyExists {
    /// The id of the identical index.
    pub index_id: GlobalId,
    /// The key of the index.
    pub index_key: Vec<MirScalarExpr>,
    /// The id of the object that the index is on.
    pub index_on_id: GlobalId,
    /// The id the index that duplicates existing indexes.
    pub exported_index_id: GlobalId,
}

impl OptimizerNoticeApi for IndexAlreadyExists {
    fn dependencies(&self) -> BTreeSet<GlobalId> {
        BTreeSet::from([self.index_id, self.index_on_id])
    }

    fn fmt_message(
        &self,
        f: &mut fmt::Formatter<'_>,
        humanizer: &dyn ExprHumanizer,
        redacted: bool,
    ) -> fmt::Result {
        let exported_index_name = humanizer
            .humanize_id(self.exported_index_id)
            .unwrap_or_else(|| self.exported_index_id.to_string());
        let index_name = humanizer
            .humanize_id(self.index_id)
            .unwrap_or_else(|| self.index_id.to_string());
        let index_on_id_name = humanizer
            .humanize_id_unqualified(self.index_on_id)
            .unwrap_or_else(|| self.index_on_id.to_string());

        let mode = HumanizedNotice::new(redacted);
        let col_names = humanizer.column_names_for_id(self.index_on_id);
        let col_names = col_names.as_ref();
        let index_key = separated(", ", mode.seq(&self.index_key, col_names));

        write!(
            f,
            "Index {exported_index_name} is identical to {index_name}, which \
             is also defined on {index_on_id_name}({index_key})."
        )
    }

    fn fmt_hint(
        &self,
        f: &mut fmt::Formatter<'_>,
        humanizer: &dyn ExprHumanizer,
        redacted: bool,
    ) -> fmt::Result {
        let index_on_id_name = humanizer
            .humanize_id_unqualified(self.index_on_id)
            .unwrap_or_else(|| self.index_on_id.to_string());

        let mode = HumanizedNotice::new(redacted);
        let col_names = humanizer.column_names_for_id(self.index_on_id);
        let col_names = col_names.as_ref();
        let index_key = separated(", ", mode.seq(&self.index_key, col_names));

        write!(
            f,
            "Please drop all indexes except the first index created on \
             {index_on_id_name}({index_key}) and recreate all dependent objects."
        )
    }

    fn fmt_action(
        &self,
        _f: &mut fmt::Formatter<'_>,
        _humanizer: &dyn ExprHumanizer,
        _redacted: bool,
    ) -> fmt::Result {
        Ok(())
    }

    fn action_kind(&self, _humanizer: &dyn ExprHumanizer) -> ActionKind {
        ActionKind::None
    }
}
