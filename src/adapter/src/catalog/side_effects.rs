// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Side effects derived from catalog changes that can be applied to a
//! controller.

use itertools::Itertools;
use mz_catalog::memory::objects::{CatalogItem, StateDiff, StateUpdateKind, Table};
use mz_ore::instrument;
use mz_repr::{CatalogItemId, GlobalId};

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::CatalogState;

/// An update that needs to be applied to a controller.
#[derive(Debug, Clone)]
pub enum CatalogSideEffect {
    CreateTable(CatalogItemId, GlobalId, Table),
    DropTable(CatalogItemId, GlobalId),
}

impl CatalogState {
    /// Generate a list of [CatalogSideEffects](CatalogSideEffect) that
    /// correspond to a single update made to the durable catalog.
    #[instrument(level = "debug")]
    pub(crate) fn generate_side_effects(
        &self,
        kind: StateUpdateKind,
        diff: StateDiff,
    ) -> Vec<CatalogSideEffect> {
        // WIP: Exhaustive match?
        match kind {
            StateUpdateKind::Item(item) => self.generate_item_update(item.id, diff),
            _ => Vec::new(),
        }
    }

    fn generate_item_update(&self, id: CatalogItemId, diff: StateDiff) -> Vec<CatalogSideEffect> {
        let entry = self.get_entry(&id);
        let id = entry.id();
        let global_ids = entry.global_ids();

        // WIP: Exhaustive match?
        let updates = match entry.item() {
            CatalogItem::Table(table) => match diff {
                StateDiff::Addition => {
                    // TODO: Correctly handle multiple global IDs.
                    global_ids
                        .map(|gid| CatalogSideEffect::CreateTable(id, gid, table.clone()))
                        .collect_vec()
                }
                StateDiff::Retraction => global_ids
                    .map(|gid| CatalogSideEffect::DropTable(id, gid))
                    .collect_vec(),
            },
            _ => {
                // WIP!
                Vec::new()
            }
        };

        updates
    }
}
