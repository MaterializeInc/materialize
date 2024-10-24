// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Helper function for dataflow expiration checks.

use std::collections::BTreeMap;

use mz_catalog::memory::objects::DataSourceDesc;
use mz_compute_types::dataflows::DataflowDescription;
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

    pub(crate) fn determine_dependence<P>(
        &mut self,
        id: GlobalId,
        plan: Option<&DataflowDescription<P>>,
    ) -> TimeDependence {
        println!("determine_dependence({id})");
        use TimeDependence::*;

        if let Some(dependence) = self.seen.get(&id).cloned() {
            return dependence;
        }
        let indent = "  ".repeat(self.level);

        self.level += 1;
        let entry = self.catalog.get_entry(&id);
        let mut time_dependence = match entry.item_type() {
            CatalogItemType::Table => {
                println!("{indent}table: {id:?}");
                // Follows wall clock.
                Wallclock
            }
            CatalogItemType::Source => {
                println!("{indent}source: {id:?}");
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
                            self.determine_dependence::<()>(*ingestion_id, None)
                        }
                        // Introspection, progress and webhook sources follow wall clock.
                        DataSourceDesc::Introspection(_)
                        | DataSourceDesc::Progress
                        | DataSourceDesc::Webhook { .. } => Wallclock,
                    }
                }
            }
            CatalogItemType::MaterializedView => {
                println!("{indent}mv: {id:?}");
                // Follow dependencies, rounded to the next refresh.
                let materialized_view = entry.materialized_view().unwrap();
                let mut dependence = Indeterminate;
                if let Some(plan) = self.catalog.try_get_physical_plan(&id) {
                    if let Some(time_dependence) = &plan.time_dependence {
                        println!("{indent}mv cached value: {time_dependence:?}");
                        dependence = time_dependence.clone();
                    } else {
                        let id_bundle = dataflow_import_id_bundle(
                            plan,
                            entry.cluster_id().expect("must exist"),
                        );
                        for dep in id_bundle.iter() {
                            dependence.unify(&self.determine_dependence::<()>(dep, None));
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
                        // panic!("Dependency on materialized view without time dependence");
                    }
                } else {
                    println!("{indent}mv physical plan absent");
                }
                dependence
            }
            CatalogItemType::Index | CatalogItemType::ContinualTask => {
                println!("{indent}index|CT: {id:?}");
                // Follow dependencies, if any.
                let mut dependence = Indeterminate;
                if let Some(time_dependence) = self
                    .catalog
                    .try_get_physical_plan(&id)
                    .and_then(|plan| plan.time_dependence.as_ref())
                    .cloned()
                {
                    // if let Some(time_dependence) = &plan.time_dependence {
                    println!("{indent}index|CT cached value: {time_dependence:?}");
                    dependence = time_dependence;
                } else if let Some(plan) = plan {
                    let id_bundle =
                        dataflow_import_id_bundle(plan, entry.cluster_id().expect("must exist"));
                    for dep in id_bundle.iter() {
                        dependence.unify(&self.determine_dependence::<()>(dep, None));
                    }
                    // panic!("Dependency on view without time dependence");
                } else {
                    println!("{indent}index|CT physical plan absent");
                }
                dependence
            }
            // All others are indeterminate.
            CatalogItemType::Connection
            | CatalogItemType::Func
            | CatalogItemType::Secret
            | CatalogItemType::Sink
            | CatalogItemType::Type
            | CatalogItemType::View => Indeterminate,
        };

        time_dependence.normalize();

        println!("{indent}-> time dependence: {id:?} {time_dependence:?}");
        self.seen.insert(id, time_dependence.clone());
        time_dependence
    }
}
