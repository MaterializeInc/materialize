// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_catalog::builtin::notice::MZ_OPTIMIZER_NOTICES;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::{
    Action, ActionKind, OptimizerNotice, OptimizerNoticeApi, OptimizerNoticeKind,
    RawOptimizerNotice,
};

use crate::catalog::{BuiltinTableUpdate, Catalog, CatalogState};

impl Catalog {
    /// Transform the [`DataflowMetainfo`] by rendering an [`OptimizerNotice`]
    /// for each [`RawOptimizerNotice`].
    ///
    /// Delegates to [`CatalogState::render_notices`].
    pub fn render_notices(
        &self,
        df_meta: DataflowMetainfo<RawOptimizerNotice>,
        item_id: Option<GlobalId>,
    ) -> DataflowMetainfo<OptimizerNotice> {
        self.state.render_notices(df_meta, item_id)
    }

    /// Pack a [`BuiltinTableUpdate`] with the given `diff` for each
    /// [`OptimizerNotice`] in `notices` into `updates`.
    ///
    /// Delegates to [`CatalogState::pack_optimizer_notices`].
    pub fn pack_optimizer_notices<'a>(
        &self,
        updates: &mut Vec<BuiltinTableUpdate>,
        notices: impl Iterator<Item = &'a OptimizerNotice>,
        diff: Diff,
    ) {
        self.state.pack_optimizer_notices(updates, notices, diff);
    }
}

impl CatalogState {
    /// Transform the [`DataflowMetainfo`] by rendering an [`OptimizerNotice`]
    /// for each [`RawOptimizerNotice`].
    pub fn render_notices(
        &self,
        df_meta: DataflowMetainfo<RawOptimizerNotice>,
        item_id: Option<GlobalId>,
    ) -> DataflowMetainfo<OptimizerNotice> {
        let optimizer_notices = df_meta
            .optimizer_notices
            .into_iter()
            .map(|notice| OptimizerNotice {
                kind: OptimizerNoticeKind::from(&notice),
                item_id,
                dependencies: notice.dependencies(),
                message: notice.message(self, false).to_string(),
                hint: notice.hint(self, false).to_string(),
                action: match notice.action_kind(self) {
                    ActionKind::SqlStatements => {
                        Action::SqlStatements(notice.action(self, false).to_string())
                    }
                    ActionKind::PlainText => {
                        Action::PlainText(notice.action(self, false).to_string())
                    }
                    ActionKind::None => {
                        Action::None // No concrete action.
                    }
                },
                message_redacted: notice.message(self, true).to_string(),
                hint_redacted: notice.hint(self, true).to_string(),
                action_redacted: match notice.action_kind(self) {
                    ActionKind::SqlStatements => {
                        Action::SqlStatements(notice.action(self, true).to_string())
                    }
                    ActionKind::PlainText => {
                        Action::PlainText(notice.action(self, true).to_string())
                    }
                    ActionKind::None => {
                        Action::None // No concrete action.
                    }
                },
                created_at: (self.config().now)(),
            })
            .collect();

        DataflowMetainfo {
            optimizer_notices,
            index_usage_types: df_meta.index_usage_types,
        }
    }

    /// Pack a [`BuiltinTableUpdate`] with the given `diff` for each
    /// [`OptimizerNotice`] in `notices` into `updates`.
    pub fn pack_optimizer_notices<'a>(
        &self,
        updates: &mut Vec<BuiltinTableUpdate>,
        notices: impl Iterator<Item = &'a OptimizerNotice>,
        diff: Diff,
    ) {
        let mut row = Row::default();

        for notice in notices {
            let mut packer = row.packer();

            // Pre-convert some fields into a type that can be wrapped into a
            // Datum.
            let item_id = notice.item_id.as_ref().map(ToString::to_string);
            let deps = notice
                .dependencies
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let created_at = mz_ore::now::to_datetime(notice.created_at)
                .try_into()
                .expect("must fit");

            // push `notice_type` column (TODO(21513): encode as int?)
            packer.push(Datum::String(notice.kind.as_str()));
            // push `message` column
            packer.push(Datum::String(&notice.message));
            // push `hint` column
            packer.push(Datum::String(&notice.hint));
            // push `action` column
            packer.push(match &notice.action {
                Action::None => Datum::Null,
                Action::PlainText(text) => Datum::String(text),
                Action::SqlStatements(text) => Datum::String(text),
            });
            // push `message_redacted` column
            packer.push(Datum::String(&notice.message_redacted));
            // push `hint_redacted` column
            packer.push(Datum::String(&notice.hint_redacted));
            // push `action_redacted` column
            packer.push(match &notice.action_redacted {
                Action::None => Datum::Null,
                Action::PlainText(text) => Datum::String(text),
                Action::SqlStatements(text) => Datum::String(text),
            });
            // push `action_type` column (TODO(21513): encode as int?)
            packer.push(match &notice.action {
                Action::None => Datum::Null,
                action => Datum::String(action.kind().as_str()),
            });
            // push `object_id` column
            packer.push(match item_id.as_ref() {
                Some(item_id) => Datum::String(item_id),
                None => Datum::Null,
            });
            // push `dependency_ids` column
            packer.push_list(deps.iter().map(|d| Datum::String(d)));
            // push `created_at` column
            packer.push(Datum::TimestampTz(created_at));

            updates.push(BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_OPTIMIZER_NOTICES),
                row: row.clone(),
                diff,
            });
        }
    }
}
