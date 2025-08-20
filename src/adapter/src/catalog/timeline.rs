// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to timelines.

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_catalog::memory::objects::{CatalogItem, ContinualTask, MaterializedView, View};
use mz_expr::CollectionPlan;
use mz_ore::collections::CollectionExt;
use mz_repr::{CatalogItemId, GlobalId};
use mz_storage_types::sources::Timeline;

use crate::catalog::Catalog;
use crate::{AdapterError, CollectionIdBundle, TimelineContext};

impl Catalog {
    /// Return the [`TimelineContext`] belonging to a [`CatalogItemId`], if one exists.
    pub(crate) fn get_timeline_context(&self, id: CatalogItemId) -> TimelineContext {
        let entry = self.get_entry(&id);
        self.validate_timeline_context(entry.global_ids())
            .expect("impossible for a single object to belong to incompatible timeline contexts")
    }

    /// Return the [`TimelineContext`] belonging to a [`GlobalId`], if one exists.
    pub(crate) fn get_timeline_context_for_global_id(&self, id: GlobalId) -> TimelineContext {
        self.validate_timeline_context(vec![id])
            .expect("impossible for a single object to belong to incompatible timeline contexts")
    }

    /// Returns an iterator that partitions an id bundle by the [`TimelineContext`] that each id
    /// belongs to.
    pub fn partition_ids_by_timeline_context(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> impl Iterator<Item = (TimelineContext, CollectionIdBundle)> + use<> {
        let mut res: BTreeMap<TimelineContext, CollectionIdBundle> = BTreeMap::new();

        for gid in &id_bundle.storage_ids {
            let timeline_context = self.get_timeline_context_for_global_id(*gid);
            res.entry(timeline_context)
                .or_default()
                .storage_ids
                .insert(*gid);
        }

        for (compute_instance, ids) in &id_bundle.compute_ids {
            for gid in ids {
                let timeline_context = self.get_timeline_context_for_global_id(*gid);
                res.entry(timeline_context)
                    .or_default()
                    .compute_ids
                    .entry(*compute_instance)
                    .or_default()
                    .insert(*gid);
            }
        }

        res.into_iter()
    }

    /// Returns an id bundle containing all the ids in the give timeline.
    pub(crate) fn ids_in_timeline(&self, timeline: &Timeline) -> CollectionIdBundle {
        let mut id_bundle = CollectionIdBundle::default();
        for entry in self.entries() {
            if let TimelineContext::TimelineDependent(entry_timeline) =
                self.get_timeline_context(entry.id())
            {
                if timeline == &entry_timeline {
                    match entry.item() {
                        CatalogItem::Table(table) => {
                            id_bundle.storage_ids.extend(table.global_ids());
                        }
                        CatalogItem::Source(source) => {
                            id_bundle.storage_ids.insert(source.global_id());
                        }
                        CatalogItem::MaterializedView(mv) => {
                            id_bundle.storage_ids.insert(mv.global_id());
                        }
                        CatalogItem::ContinualTask(ct) => {
                            id_bundle.storage_ids.insert(ct.global_id());
                        }
                        CatalogItem::Index(index) => {
                            id_bundle
                                .compute_ids
                                .entry(index.cluster_id)
                                .or_default()
                                .insert(index.global_id());
                        }
                        CatalogItem::View(_)
                        | CatalogItem::Sink(_)
                        | CatalogItem::Type(_)
                        | CatalogItem::Func(_)
                        | CatalogItem::Secret(_)
                        | CatalogItem::Connection(_)
                        | CatalogItem::Log(_) => {}
                    }
                }
            }
        }
        id_bundle
    }

    /// Return an error if the ids are from incompatible [`TimelineContext`]s. This should
    /// be used to prevent users from doing things that are either meaningless
    /// (joining data from timelines that have similar numbers with different
    /// meanings like two separate debezium topics) or will never complete (joining
    /// cdcv2 and realtime data).
    pub(crate) fn validate_timeline_context<I>(
        &self,
        ids: I,
    ) -> Result<TimelineContext, AdapterError>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let items_ids = ids
            .into_iter()
            .filter_map(|gid| self.try_resolve_item_id(&gid));
        let mut timeline_contexts: Vec<_> =
            self.get_timeline_contexts(items_ids).into_iter().collect();
        // If there's more than one timeline, we will not produce meaningful
        // data to a user. Take, for example, some realtime source and a debezium
        // consistency topic source. The realtime source uses something close to now
        // for its timestamps. The debezium source starts at 1 and increments per
        // transaction. We don't want to choose some timestamp that is valid for both
        // of these because the debezium source will never get to the same value as the
        // realtime source's "milliseconds since Unix epoch" value. And even if it did,
        // it's not meaningful to join just because those two numbers happen to be the
        // same now.
        //
        // Another example: assume two separate debezium consistency topics. Both
        // start counting at 1 and thus have similarish numbers that probably overlap
        // a lot. However it's still not meaningful to join those two at a specific
        // transaction counter number because those counters are unrelated to the
        // other.
        let timelines: Vec<_> = timeline_contexts
            .extract_if(.., |timeline_context| timeline_context.contains_timeline())
            .collect();

        // A single or group of objects may contain multiple compatible timeline
        // contexts. For example `SELECT *, 1, mz_now() FROM t` will contain all
        // types of contexts. We choose the strongest context level to return back.
        if timelines.len() > 1 {
            Err(AdapterError::Unsupported(
                "multiple timelines within one dataflow",
            ))
        } else if timelines.len() == 1 {
            Ok(timelines.into_element())
        } else if timeline_contexts
            .iter()
            .contains(&TimelineContext::TimestampDependent)
        {
            Ok(TimelineContext::TimestampDependent)
        } else {
            Ok(TimelineContext::TimestampIndependent)
        }
    }

    /// Return the [`TimelineContext`]s belonging to a list of [`CatalogItemId`]s, if any exist.
    fn get_timeline_contexts<I>(&self, ids: I) -> BTreeSet<TimelineContext>
    where
        I: IntoIterator<Item = CatalogItemId>,
    {
        let mut seen: BTreeSet<CatalogItemId> = BTreeSet::new();
        let mut timelines: BTreeSet<TimelineContext> = BTreeSet::new();

        // Recurse through IDs to find all sources and tables, adding new ones to
        // the set until we reach the bottom.
        let mut ids: Vec<_> = ids.into_iter().collect();
        while let Some(id) = ids.pop() {
            // Protect against possible infinite recursion. Not sure if it's possible, but
            // a cheap prevention for the future.
            if !seen.insert(id) {
                continue;
            }
            if let Some(entry) = self.try_get_entry(&id) {
                match entry.item() {
                    CatalogItem::Source(source) => {
                        timelines
                            .insert(TimelineContext::TimelineDependent(source.timeline.clone()));
                    }
                    CatalogItem::Index(index) => {
                        let on_id = self.resolve_item_id(&index.on);
                        ids.push(on_id);
                    }
                    CatalogItem::View(View { optimized_expr, .. }) => {
                        // If the definition contains a temporal function, the timeline must
                        // be timestamp dependent.
                        if optimized_expr.contains_temporal() {
                            timelines.insert(TimelineContext::TimestampDependent);
                        } else {
                            timelines.insert(TimelineContext::TimestampIndependent);
                        }
                        let item_ids = optimized_expr
                            .depends_on()
                            .into_iter()
                            .map(|gid| self.resolve_item_id(&gid));
                        ids.extend(item_ids);
                    }
                    CatalogItem::MaterializedView(MaterializedView { optimized_expr, .. }) => {
                        // In some cases the timestamp selected may not affect the answer to a
                        // query, but it may affect our ability to query the materialized view.
                        // Materialized views must durably materialize the result of a query, even
                        // for constant queries. If we choose a timestamp larger than the upper,
                        // which represents the current progress of the view, then the query will
                        // need to block and wait for the materialized view to advance.
                        timelines.insert(TimelineContext::TimestampDependent);
                        let item_ids = optimized_expr
                            .depends_on()
                            .into_iter()
                            .map(|gid| self.resolve_item_id(&gid));
                        ids.extend(item_ids);
                    }
                    CatalogItem::ContinualTask(ContinualTask { raw_expr, .. }) => {
                        // See comment in MaterializedView
                        timelines.insert(TimelineContext::TimestampDependent);
                        let item_ids = raw_expr
                            .depends_on()
                            .into_iter()
                            .map(|gid| self.resolve_item_id(&gid));
                        ids.extend(item_ids);
                    }
                    CatalogItem::Table(table) => {
                        timelines.insert(TimelineContext::TimelineDependent(table.timeline()));
                    }
                    CatalogItem::Log(_) => {
                        timelines.insert(TimelineContext::TimelineDependent(
                            Timeline::EpochMilliseconds,
                        ));
                    }
                    CatalogItem::Sink(_)
                    | CatalogItem::Type(_)
                    | CatalogItem::Func(_)
                    | CatalogItem::Secret(_)
                    | CatalogItem::Connection(_) => {}
                }
            }
        }

        timelines
    }
}
