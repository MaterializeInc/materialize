// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Helper function for dataflow expiration checks.

use std::collections::BTreeMap;

use mz_catalog::memory::objects::DataSourceDesc;
use mz_compute_types::dataflows::DataflowDescription;
use mz_controller_types::ClusterId;
use mz_ore::soft_panic_or_log;
use mz_repr::time_dependence::TimeDependence;
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogItem, CatalogItemType};
use mz_storage_types::sources::{GenericSourceConnection, Timeline};

use crate::catalog::Catalog;
use crate::optimize::dataflows::dataflow_import_id_bundle;

pub(crate) struct TimeDependenceHelper<'a> {
    seen: BTreeMap<GlobalId, TimeDependence>,
    catalog: &'a Catalog,
    level: usize,
}

impl<'a> TimeDependenceHelper<'a> {
    pub(crate) fn new(catalog: &'a Catalog) -> Self {
        Self {
            seen: BTreeMap::new(),
            catalog,
            level: 0,
        }
    }

    pub(crate) fn determine_time_dependence_plan<P>(
        &mut self,
        plan: &DataflowDescription<P>,
        cluster: ClusterId,
    ) -> TimeDependence {
        let id_bundle = dataflow_import_id_bundle(plan, cluster);
        self.determine_time_dependence_ids(id_bundle.iter())
    }

    pub(crate) fn determine_time_dependence_ids(
        &mut self,
        ids: impl IntoIterator<Item = GlobalId>,
    ) -> TimeDependence {
        use TimeDependence::*;
        let mut time_dependence = Indeterminate;
        for id in ids {
            time_dependence.unify(&self.determine_dependence_inner(id));
        }
        time_dependence
    }

    fn determine_dependence_inner(&mut self, id: GlobalId) -> TimeDependence {
        use TimeDependence::*;

        if let Some(dependence) = self.seen.get(&id).cloned() {
            return dependence;
        }
        let indent = "  ".repeat(self.level);

        self.level += 1;
        let entry = self.catalog.get_entry(&id);
        println!("{indent}{}: {id:?} {:?}", entry.item_type(), entry.name());
        let mut time_dependence = match entry.item_type() {
            CatalogItemType::Table => {
                // Follows wall clock.
                Wallclock
            }
            CatalogItemType::Source => {
                // Some sources don't have a `Source` entry, so we can't determine their time
                // dependence.
                if let Some(source) = entry.source() {
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
                                self.determine_dependence_inner(*ingestion_id)
                            }
                            // Introspection, progress and webhook sources follow wall clock.
                            DataSourceDesc::Introspection(_)
                            | DataSourceDesc::Progress
                            | DataSourceDesc::Webhook { .. } => Wallclock,
                        }
                    }
                } else {
                    Indeterminate
                }
            }
            CatalogItemType::MaterializedView
            | CatalogItemType::Index
            | CatalogItemType::ContinualTask => {
                // Follow dependencies, if any.
                if let Some(time_dependence) = self
                    .catalog
                    .try_get_physical_plan(&id)
                    .and_then(|plan| plan.time_dependence.as_ref())
                    .cloned()
                {
                    println!("{indent}mv|index|CT cached value: {time_dependence:?}");
                    time_dependence
                } else {
                    soft_panic_or_log!("{indent}mv|index|CT physical plan absent");
                    Indeterminate
                }
            }
            // All others are indeterminate.
            CatalogItemType::Connection
            | CatalogItemType::Func
            | CatalogItemType::Secret
            | CatalogItemType::Sink
            | CatalogItemType::Type
            | CatalogItemType::View => Indeterminate,
        };
        self.level -= 1;

        time_dependence.normalize();

        println!("{indent}-> time dependence: {id:?} {time_dependence:?}");
        self.seen.insert(id, time_dependence.clone());
        time_dependence
    }
}
