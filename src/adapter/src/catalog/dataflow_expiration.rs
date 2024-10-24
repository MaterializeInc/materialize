// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Helper function for dataflow expiration checks.

use std::collections::BTreeMap;

use mz_catalog::memory::objects::DataSourceDesc;
use mz_repr::time_dependence::TimeDependence;
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogItem, CatalogItemType};
use mz_storage_types::sources::{GenericSourceConnection, Timeline};

use crate::catalog::Catalog;
use crate::optimize::dataflows::dataflow_import_id_bundle;

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

pub(crate) struct TimeDependenceHelper<'a> {
    seen: BTreeMap<GlobalId, TimeDependence>,
    catalog: &'a Catalog,
}

impl<'a> TimeDependenceHelper<'a> {
    pub(crate) fn new(catalog: &'a Catalog) -> Self {
        Self {
            seen: BTreeMap::new(),
            catalog,
        }
    }

    pub(crate) fn determine_dependence(&mut self, id: GlobalId) -> TimeDependence {
        use TimeDependence::*;

        if let Some(dependence) = self.seen.get(&id).cloned() {
            return dependence;
        }

        let entry = self.catalog.get_entry(&id);
        let mut time_dependence = match entry.item_type() {
            CatalogItemType::Table => {
                // Follows wall clock.
                Wallclock
            }
            CatalogItemType::Source => {
                // Some sources don't have a `Source` entry, so we can't determine their time
                // dependence.
                let Some(source) = entry.source() else {
                    return Indeterminate;
                };
                // We only know how to handle the epoch timeline.
                let timeline = source.timeline.clone();
                if !matches!(timeline, Timeline::EpochMilliseconds) {
                    Indeterminate
                } else {
                    match &source.data_source {
                        DataSourceDesc::Ingestion { ingestion_desc, .. } => {
                            match ingestion_desc.desc.connection {
                                // Kafka, Postgres, MySql sources follow wall clock.
                                GenericSourceConnection::Kafka(_)
                                | GenericSourceConnection::Postgres(_)
                                | GenericSourceConnection::MySql(_) => Wallclock,
                                // Load generators not further specified.
                                GenericSourceConnection::LoadGenerator(_) => Indeterminate,
                            }
                        }
                        DataSourceDesc::IngestionExport { ingestion_id, .. } => {
                            self.determine_dependence(*ingestion_id)
                        }
                        // Introspection, progress and webhook sources follow wall clock.
                        DataSourceDesc::Introspection(_)
                        | DataSourceDesc::Progress
                        | DataSourceDesc::Webhook { .. } => Wallclock,
                    }
                }
            }
            CatalogItemType::MaterializedView => {
                // Follow dependencies, rounded to the next refresh.
                let materialized_view = entry.materialized_view().unwrap();
                let mut dependence = Indeterminate;
                if let Some(plan) = self.catalog.try_get_physical_plan(&id) {
                    let id_bundle =
                        dataflow_import_id_bundle(plan, entry.cluster_id().expect("must exist"));
                    for dep in id_bundle.iter() {
                        dependence.unify(&self.determine_dependence(dep));
                    }
                    if let Some(refresh_schedule) = &materialized_view.refresh_schedule {
                        dependence = match dependence {
                            Indeterminate => Indeterminate,
                            RefreshSchedule(_, existing) => {
                                RefreshSchedule(Some(refresh_schedule.clone()), existing)
                            }
                            Wallclock => {
                                RefreshSchedule(Some(refresh_schedule.clone()), vec![Wallclock])
                            }
                        };
                    }
                }
                dependence
            }
            CatalogItemType::Index => {
                // Follow dependencies, if any.
                let mut dependence = Indeterminate;
                if let Some(plan) = self.catalog.try_get_physical_plan(&id) {
                    let id_bundle =
                        dataflow_import_id_bundle(plan, entry.cluster_id().expect("must exist"));
                    for dep in id_bundle.iter() {
                        dependence.unify(&self.determine_dependence(dep));
                    }
                }
                dependence
            }
            // All others are indeterminate.
            CatalogItemType::Connection
            | CatalogItemType::ContinualTask
            | CatalogItemType::Func
            | CatalogItemType::Secret
            | CatalogItemType::Sink
            | CatalogItemType::Type
            | CatalogItemType::View => Indeterminate,
        };

        time_dependence.normalize();

        self.seen.insert(id, time_dependence.clone());
        time_dependence
    }
}
