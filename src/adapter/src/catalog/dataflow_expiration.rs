// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Helper function for dataflow expiration checks.

use crate::catalog::Catalog;
use differential_dataflow::lattice::Lattice;
use mz_catalog::memory::objects::CatalogItem;
use mz_expr::CollectionPlan;
use mz_repr::{GlobalId, Timestamp};

impl Catalog {
    /// Whether the catalog entry `id` or any of its transitive dependencies is a materialized view
    /// with a refresh schedule. Used to disable dataflow expiration if found.
    pub(crate) fn item_has_transitive_refresh_schedule(&self, id: GlobalId) -> bool {
        let test_has_transitive_refresh_schedule = |dep: GlobalId| -> bool {
            if let Some(mv) = self.get_entry(&dep).materialized_view() {
                return mv.refresh_schedule.is_some();
            }
            false
        };
        test_has_transitive_refresh_schedule(id)
            || self
                .state()
                .transitive_uses(id)
                .any(test_has_transitive_refresh_schedule)
    }
    /// Recursive function.
    pub(crate) fn compute_expiration_with_refresh(
        &self,
        id: GlobalId,
        replica_expiration: Timestamp,
    ) -> Timestamp {
        let entry = self.get_entry(&id);
        // TODO: use a queue to deduplicate work.
        // TODO: don't use Timestamp::MAX as the initial value.
        match &entry.item {
            CatalogItem::MaterializedView(mv) => {
                let mut new_expiration = mv.raw_expr.depends_on().into_iter().fold(
                    Timestamp::MAX,
                    |new_expiration, dep| {
                        // Pick the minimum refresh time of all dependencies.
                        new_expiration
                            .meet(&self.compute_expiration_with_refresh(dep, replica_expiration))
                    },
                );
                if let Some(refresh_schedule) = &mv.refresh_schedule {
                    if let Some(next_refresh) = refresh_schedule.round_up_timestamp(new_expiration)
                    {
                        new_expiration = next_refresh;
                    }
                }
                new_expiration
            }
            CatalogItem::View(view) => view.raw_expr.depends_on().into_iter().fold(
                Timestamp::MAX,
                |new_expiration, dep| {
                    new_expiration
                        .meet(&self.compute_expiration_with_refresh(dep, replica_expiration))
                },
            ),
            CatalogItem::Index(index) => {
                self.compute_expiration_with_refresh(index.on, replica_expiration)
            }
            CatalogItem::Func(_) => {
                todo!()
            }
            CatalogItem::ContinualTask(_) => {
                todo!()
            }
            // These will not have a transitive dependency on REFRESH.
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Log(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => replica_expiration,
        }
    }
}
