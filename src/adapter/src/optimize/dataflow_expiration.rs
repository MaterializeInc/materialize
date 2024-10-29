// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Helper function for dataflow expiration checks.
//!
//! A [`TimeDependence`] describes how a dataflow follows wall-clock time, and this module offers
//! functions to compute the time dependence based on the current state of the catalog.
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

use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc, TableDataSource};
use mz_compute_types::time_dependence::TimeDependence;
use mz_ore::soft_panic_or_log;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::GlobalId;
use mz_sql::catalog::CatalogItem as _;
use mz_storage_types::sources::{GenericSourceConnection, Timeline};

use crate::catalog::Catalog;

/// Determine a time dependence based on a set of global IDs. The result includes the schedule if
/// supplied and needed. Returns `None` if called with an empty iterator, or if all dependencies
/// are indeterminate.
pub(crate) fn time_dependence(
    catalog: &Catalog,
    ids: impl IntoIterator<Item = GlobalId>,
    schedule: Option<RefreshSchedule>,
) -> Option<TimeDependence> {
    // Collect all time dependencies of our dependencies.
    let mut time_dependencies = ids
        .into_iter()
        .filter_map(|id| time_dependence_for_id(catalog, id))
        .collect::<Vec<_>>();

    // Sort and dedupe to remove redundancy.
    time_dependencies.sort();
    time_dependencies.dedup();

    let time_dependence = if time_dependencies
        .iter()
        .any(|dep| *dep == TimeDependence::default())
    {
        // Wall-clock dependency is dominant.
        Some(TimeDependence::default())
    } else if !time_dependencies.is_empty() {
        // No immediate wall-clock dependency, found some dependency with a refresh schedule.
        Some(TimeDependence::new(schedule, time_dependencies))
    } else {
        // No wall-clock dependence, no refresh schedule
        None
    };
    TimeDependence::normalize(time_dependence)
}

/// Determine the time dependence for a [`DataSourceDesc`].
///
/// Non-epoch timelines are indeterminate, and load generators.
fn time_dependence_for_source_desc(
    catalog: &Catalog,
    desc: &DataSourceDesc,
    timeline: &Timeline,
) -> Option<TimeDependence> {
    // We only know how to handle the epoch timeline.
    if !matches!(timeline, Timeline::EpochMilliseconds) {
        return None;
    }
    match desc {
        DataSourceDesc::Ingestion { ingestion_desc, .. } => {
            match ingestion_desc.desc.connection {
                // Kafka, Postgres, MySql sources follow wall clock.
                GenericSourceConnection::Kafka(_)
                | GenericSourceConnection::Postgres(_)
                | GenericSourceConnection::MySql(_) => Some(TimeDependence::default()),
                // Load generators not further specified.
                GenericSourceConnection::LoadGenerator(_) => None,
            }
        }
        DataSourceDesc::IngestionExport { ingestion_id, .. } => {
            time_dependence_for_id(catalog, *ingestion_id)
        }
        // Introspection, progress and webhook sources follow wall clock.
        DataSourceDesc::Introspection(_)
        | DataSourceDesc::Progress
        | DataSourceDesc::Webhook { .. } => Some(TimeDependence::default()),
    }
}

/// Determine the time dependence for a single global ID.
fn time_dependence_for_id(catalog: &Catalog, id: GlobalId) -> Option<TimeDependence> {
    let entry = catalog.get_entry(&id);
    match entry.item() {
        // Introspection sources follow wall-clock.
        CatalogItem::Log(_) => Some(TimeDependence::default()),
        CatalogItem::Table(table) => {
            match &table.data_source {
                // Tables follow wall clock.
                TableDataSource::TableWrites { .. } => Some(TimeDependence::default()),
                TableDataSource::DataSource { desc, timeline } => {
                    time_dependence_for_source_desc(catalog, desc, timeline)
                }
            }
        }
        CatalogItem::Source(source) => {
            time_dependence_for_source_desc(catalog, &source.data_source, &source.timeline)
        }
        CatalogItem::MaterializedView(_)
        | CatalogItem::Index(_)
        | CatalogItem::ContinualTask(_) => {
            // Return cached information
            if let Some(time_dependence) = catalog
                .try_get_physical_plan(&id)
                .map(|plan| plan.time_dependence.clone())
            {
                time_dependence
            } else {
                soft_panic_or_log!(
                    "Physical plan alarmingly absent for {} {:?}",
                    entry.item_type(),
                    entry.id
                );
                None
            }
        }
        // All others are indeterminate.
        CatalogItem::Connection(_)
        | CatalogItem::Func(_)
        | CatalogItem::Secret(_)
        | CatalogItem::Sink(_)
        | CatalogItem::Type(_)
        | CatalogItem::View(_) => None,
    }
}
