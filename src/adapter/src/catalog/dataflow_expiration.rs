// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Helper function for dataflow expiration checks.

use mz_repr::{CatalogItemId, GlobalId};

use crate::catalog::Catalog;

impl Catalog {
    /// Whether the catalog entry `id` or any of its transitive dependencies is a materialized view
    /// with a refresh schedule. Used to disable dataflow expiration if found.
    pub(crate) fn item_has_transitive_refresh_schedule(&self, id: GlobalId) -> bool {
        let test_has_transitive_refresh_schedule = |dep: CatalogItemId| -> bool {
            if let Some(mv) = self.get_entry(&dep).materialized_view() {
                return mv.refresh_schedule.is_some();
            }
            false
        };

        let item_id = self.resolve_item_id(&id);
        test_has_transitive_refresh_schedule(item_id)
            || self
                .state()
                .transitive_uses(item_id)
                .any(test_has_transitive_refresh_schedule)
    }
}
