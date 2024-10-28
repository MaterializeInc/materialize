// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Helper function for dataflow expiration checks.
//!
//! A [`TimeDependence`] describes how a dataflow follows wall-clock time, and the
//! [`TimeDependenceHelper]` type offers functions to compute the time dependence based on the
//! current state of the catalog.
//!
//! For a set of global IDs, the helper determines how a dataflow that depends on the set of IDs
//! follows wall-clock time. Within the catalog, we remember how all installed objects follow
//! wall-clock time, which turns this step into gathering and combining different time dependencies:
//! * A meet of anything with wall-clock time results in wall-clock time.
//! * A meet of anything but wall-clock time and a refresh schedule results in a refresh schedule
//!   that depends on the deduplicated collection of dependencies.
//! * Otherwise, a dataflow is indeterminate, which expresses that we either don't know how it
//!   follows wall-clock time, or is a constant collection.
//!
//! The time dependence needs to be computed on the actual dependencies, and not on catalog
//! uses. An optimized dataflow depends on concrete indexes, and has unnecessary dependencies
//! pruned. Additionally, transitive dependencies can depend on indexes that do not exist anymore,
//! which makes combining run-time information with catalog-based information inconclusive.

use std::collections::BTreeMap;

use mz_catalog::memory::objects::{DataSourceDesc, TableDataSource};
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::time_dependence::TimeDependence;
use mz_controller_types::ClusterId;
use mz_ore::soft_panic_or_log;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogItem, CatalogItemType};
use mz_storage_types::sources::{GenericSourceConnection, Timeline};

use crate::catalog::Catalog;
use crate::optimize::dataflows::dataflow_import_id_bundle;

/// Helper type to determine the time dependence of a dataflow. See module-level documentation
/// for more details.
pub(crate) struct TimeDependenceHelper<'a> {
    /// Map of seen objects to their time dependence.
    seen: BTreeMap<GlobalId, TimeDependence>,
    /// Current catalog.
    catalog: &'a Catalog,
}

impl<'a> TimeDependenceHelper<'a> {
    /// Construct a new helper.
    pub(crate) fn new(catalog: &'a Catalog) -> Self {
        Self {
            seen: BTreeMap::new(),
            catalog,
        }
    }

    /// Determine a time dependence based on a plan. The result can have an optional
    /// refresh schedule.
    ///
    /// Note that the cluster IDs is only required to make [`dataflow_import_id_bundle`] happy.
    pub(crate) fn determine_time_dependence_plan<P>(
        &mut self,
        plan: &DataflowDescription<P>,
        cluster: ClusterId,
        schedule: Option<RefreshSchedule>,
    ) -> TimeDependence {
        let id_bundle = dataflow_import_id_bundle(plan, cluster);
        self.determine_time_dependence_ids(id_bundle.iter(), schedule)
    }

    /// Determine a time dependence based on a set of global IDs. The result can have an optional
    /// refresh schedule.
    pub(crate) fn determine_time_dependence_ids(
        &mut self,
        ids: impl IntoIterator<Item = GlobalId>,
        schedule: Option<RefreshSchedule>,
    ) -> TimeDependence {
        use TimeDependence::*;

        // Collect all time dependencies of our dependencies.
        let mut time_dependencies = ids
            .into_iter()
            .map(|id| self.determine_dependence_inner(id))
            .collect::<Vec<_>>();

        // Sort and dedupe to remove redundancy.
        time_dependencies.sort();
        time_dependencies.dedup();

        let mut time_dependence = if time_dependencies.iter().any(|dep| matches!(dep, Wallclock)) {
            // Wall-clock dependency is dominant.
            RefreshSchedule(schedule, vec![Wallclock])
        } else if time_dependencies
            .iter()
            .any(|dep| matches!(dep, RefreshSchedule(_, _)))
        {
            // No immediate wall-clock dependency, found some dependency with a refresh schedule.
            // Remove remaining Indeterminate dependencies.
            time_dependencies.retain(|dep| matches!(dep, RefreshSchedule(_, _)));
            RefreshSchedule(schedule, time_dependencies)
        } else {
            // No wall-clock dependence, no refresh schedule
            Indeterminate
        };
        time_dependence.normalize();
        time_dependence
    }

    /// Determine the time dependence for a [`DataSourceDesc`].
    ///
    /// Non-epoch timelines are indeterminate, and load generators.
    fn for_data_source_desc(
        &mut self,
        desc: &DataSourceDesc,
        timeline: &Timeline,
    ) -> TimeDependence {
        use TimeDependence::*;
        // We only know how to handle the epoch timeline.
        if !matches!(timeline, Timeline::EpochMilliseconds) {
            return Indeterminate;
        }
        match desc {
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

    /// Determine the time dependence for a single global ID.
    fn determine_dependence_inner(&mut self, id: GlobalId) -> TimeDependence {
        use TimeDependence::*;

        if let Some(dependence) = self.seen.get(&id).cloned() {
            return dependence;
        }

        let entry = self.catalog.get_entry(&id);
        let mut time_dependence = match entry.item_type() {
            CatalogItemType::Table => {
                if let Some(data_source) = entry.table().map(|table| &table.data_source) {
                    match data_source {
                        // Tables follow wall clock.
                        TableDataSource::TableWrites { .. } => Wallclock,
                        TableDataSource::DataSource { desc, timeline } => {
                            self.for_data_source_desc(desc, timeline)
                        }
                    }
                } else {
                    Indeterminate
                }
            }
            CatalogItemType::Source => {
                // Some sources don't have a `Source` entry, so we can't determine their time
                // dependence.
                match entry.source() {
                    Some(source) => {
                        self.for_data_source_desc(&source.data_source, &source.timeline)
                    }
                    None => Indeterminate,
                }
            }
            CatalogItemType::MaterializedView
            | CatalogItemType::Index
            | CatalogItemType::ContinualTask => {
                // Return cached information
                if let Some(time_dependence) = self
                    .catalog
                    .try_get_physical_plan(&id)
                    .and_then(|plan| plan.time_dependence.as_ref())
                    .cloned()
                {
                    time_dependence
                } else {
                    soft_panic_or_log!(
                        "Physical plan alarmingly absent for {} {:?}",
                        entry.item_type(),
                        entry.id
                    );
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

        time_dependence.normalize();

        self.seen.insert(id, time_dependence.clone());
        time_dependence
    }
}
