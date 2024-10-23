// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Helper function for dataflow expiration checks.

use crate::catalog::Catalog;
use mz_catalog::memory::objects::DataSourceDesc;
use mz_repr::definity::Indefiniteness;
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogItem, CatalogItemType};
use mz_storage_types::sources::{GenericSourceConnection, Timeline};
use std::collections::BTreeMap;

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
}

pub(crate) struct IndefinitenessHelper<'a> {
    seen: BTreeMap<GlobalId, Indefiniteness>,
    catalog: &'a Catalog,
    level: usize,
}

impl<'a> IndefinitenessHelper<'a> {
    pub(crate) fn new(catalog: &'a Catalog) -> Self {
        Self {
            seen: BTreeMap::new(),
            catalog,
            level: 0,
        }
    }

    pub(crate) fn indefinite_up_to(&mut self, id: GlobalId) -> Indefiniteness {
        use Indefiniteness::*;

        if let Some(indefiniteness) = self.seen.get(&id).cloned() {
            return indefiniteness;
        }
        let indent = "  ".repeat(self.level);

        self.level += 1;
        let entry = self.catalog.get_entry(&id);
        let mut indefiniteness = match entry.item_type() {
            CatalogItemType::Table => {
                println!("{indent}table: {id:?}");
                // Indefinite until expiration time.
                Wallclock
            }
            CatalogItemType::Source => {
                println!("{indent}source: {id:?}");
                // Indefinite until expiration time, only supports epoch timeline.
                let Some(source) = entry.source() else {
                    return Definite;
                };
                let timeline = source.timeline.clone();
                if !matches!(timeline, Timeline::EpochMilliseconds) {
                    Definite
                } else {
                    match &source.data_source {
                        DataSourceDesc::Ingestion { ingestion_desc, .. } => {
                            match ingestion_desc.desc.connection {
                                GenericSourceConnection::Kafka(_)
                                | GenericSourceConnection::Postgres(_)
                                | GenericSourceConnection::MySql(_) => Wallclock,
                                GenericSourceConnection::LoadGenerator(_) => Definite,
                            }
                        }
                        DataSourceDesc::IngestionExport { ingestion_id, .. } => {
                            self.indefinite_up_to(*ingestion_id)
                        }
                        DataSourceDesc::Introspection(_)
                        | DataSourceDesc::Progress
                        | DataSourceDesc::Webhook { .. } => Wallclock,
                    }
                }
            }
            CatalogItemType::MaterializedView => {
                println!("{indent}mv: {id:?}");
                // Indefinite until the meet of the dependencies, rounded to the next refresh.
                let materialized_view = entry.materialized_view().unwrap();
                let mut indefiniteness = Definite;
                for dep in entry.uses() {
                    indefiniteness.unify(&self.indefinite_up_to(dep));
                }
                if let Some(refresh_schedule) = &materialized_view.refresh_schedule {
                    println!("{indent}mv refresh_schedule: {refresh_schedule:?}");
                    match indefiniteness {
                        Definite => Definite,
                        RefreshSchedule(_, existing) => {
                            RefreshSchedule(Some(refresh_schedule.clone()), existing)
                        }
                        Wallclock => {
                            RefreshSchedule(Some(refresh_schedule.clone()), vec![Wallclock])
                        }
                    }
                } else {
                    indefiniteness
                }
            }
            CatalogItemType::Index => {
                println!("{indent}index: {id:?}");
                // Indefinite until the meet of the dependencies.
                let mut indefiniteness = Definite;
                for dep in entry.uses() {
                    indefiniteness.unify(&self.indefinite_up_to(dep));
                }
                indefiniteness
            }
            CatalogItemType::Connection
            | CatalogItemType::ContinualTask
            | CatalogItemType::Func
            | CatalogItemType::Secret
            | CatalogItemType::Sink
            | CatalogItemType::Type
            | CatalogItemType::View => Definite,
        };
        self.level -= 1;

        indefiniteness.normalize();

        println!("{indent}-> indefiniteness: {id:?} {indefiniteness:?}");
        self.seen.insert(id, indefiniteness.clone());
        indefiniteness
    }
}
