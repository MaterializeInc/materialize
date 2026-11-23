// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A pinned LIR plan is a durable record of a dataflow's plan;
//! we must _instantiate_ that plan with other runtime information
//! to actually run the dataflow.
//!
//! [`CatalogInstantiationContext`] is the [`InstantiationContext`] for
//! `PinnedDataflow::instantiate`, supplying the export descriptors not
//! recorded in the pinned plan and the current shape of each import. We look
//! this information up in the catalog items; each lookup reproduces what we
//! would have built in the first place.
//!
//! [`CatalogState`] is the [`PinningContext`] for `PinnedDataflow::pin`.

use std::collections::BTreeSet;

use mz_catalog::memory::objects::CatalogItem;
use mz_compute_types::plan::pinned::{
    IndexInfo, InlinedBuiltin, InlinedBuiltinKind, InstantiationContext, MaterializedViewInfo,
    MetricSinkInfo, PinError, PinningContext,
};
use mz_expr::MirRelationExpr;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{
    CatalogItemId, GlobalId, RelationVersionSelector, ReprRelationType, SqlRelationType,
};

use crate::catalog::CatalogState;
use crate::optimize::metric_sink::shape_metric_sink_source;

/// The catalog and the coordinator's transient id generator, which together
/// answer everything instantiation asks for.
// TODO(mgree): construct this where the coordinator loads pinned plans.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy)]
pub struct CatalogInstantiationContext<'a> {
    pub catalog: &'a CatalogState,
    pub transient_ids: &'a TransientIdGen,
}

impl InstantiationContext for CatalogInstantiationContext<'_> {
    fn collection_type(&self, id: GlobalId) -> Option<SqlRelationType> {
        let entry = self.catalog.try_get_entry_by_global_id(&id)?;
        // Tables and materialized views are imported at the version `id`
        // names, as `DataflowBuilder::import_into_dataflow` does.
        let typ = match entry.item() {
            CatalogItem::Table(table) => table.desc_for(&id).into_typ(),
            CatalogItem::MaterializedView(mv) => mv.desc_for(&id).into_typ(),
            other => other
                .relation_desc(RelationVersionSelector::Latest)?
                .typ()
                .clone(),
        };
        Some(typ)
    }

    fn index(&self, id: GlobalId) -> Option<IndexInfo> {
        let index = self.catalog.try_get_entry_by_global_id(&id)?.index()?;
        let on_desc = self.catalog.try_get_desc_by_global_id(&index.on)?;
        Some(IndexInfo {
            on: index.on,
            keys: index.keys.to_vec(),
            typ: ReprRelationType::from(on_desc.typ()),
        })
    }

    fn materialized_view(&self, id: GlobalId) -> Option<MaterializedViewInfo> {
        let mv = self
            .catalog
            .try_get_entry_by_global_id(&id)?
            .materialized_view()?;
        // Only the writing collection is fed by a sink. An older version's id
        // resolves to the same item but names no sink.
        if mv.global_id_writes() != id {
            return None;
        }
        Some(MaterializedViewInfo {
            desc: mv.desc.latest(),
            non_null_assertions: mv.non_null_assertions.clone(),
            refresh_schedule: mv.refresh_schedule.clone(),
        })
    }

    fn metric_sink(&self, id: GlobalId) -> Option<MetricSinkInfo> {
        let CatalogItem::MetricSink(sink) = self.catalog.try_get_entry_by_global_id(&id)?.item()
        else {
            return None;
        };
        let from_desc = self.catalog.try_get_desc_by_global_id(&sink.from)?;
        // The sink reads the shaped view the optimizer builds over `from`,
        // whose description depends only on the source description and the
        // prefix. The expression the shaping also returns is not needed here.
        let source =
            MirRelationExpr::global_get(sink.from, ReprRelationType::from(from_desc.typ()));
        let (_shaped_expr, shaped_desc) =
            shape_metric_sink_source(source, &from_desc, &sink.prefix);
        Some(MetricSinkInfo {
            // A user sink passes no label to the optimizer, which then labels
            // the sink with its id.
            label: id.to_string(),
            from_desc: shaped_desc,
        })
    }

    fn allocate_transient_id(&self) -> GlobalId {
        self.transient_ids.allocate_id().1
    }
}

impl PinningContext for CatalogState {
    fn inlined_builtins(&self, id: GlobalId) -> Result<BTreeSet<InlinedBuiltin>, PinError> {
        let exported = self
            .try_get_entry_by_global_id(&id)
            .ok_or(PinError::UnknownExport(id))?;
        let mut builtins = BTreeSet::new();
        let mut seen = BTreeSet::new();
        // Each item to visit, with whether a user item references it.
        let mut todo: Vec<(CatalogItemId, bool)> = exported
            .uses()
            .into_iter()
            .map(|use_id| (use_id, true))
            .collect();
        while let Some((item_id, user_referenced)) = todo.pop() {
            let entry = self.get_entry(&item_id);
            // The builtin registry leaves out unstable builtins, see its module
            // docs. A user item can reference one only with
            // `unsafe_enable_unstable_dependencies`, see
            // `CatalogState::check_unstable_dependencies`.
            if user_referenced
                && self.is_unstable_schema_specifier(entry.name().qualifiers.schema_spec)
            {
                return Err(PinError::UnstableDependency {
                    export: id,
                    dependency: self.resolve_full_name(entry.name(), None).to_string(),
                });
            }
            // Must happen after unstable check: an earlier system dependency
            // should not mask a later user one.
            if !seen.insert(item_id) {
                continue;
            }
            // Planning inlines the views a plan reads and the functions it
            // calls. Anything else it reads, including another index or
            // materialized view, is an import with its own plan, so the walk
            // stops there.
            let kind = match entry.item() {
                CatalogItem::View(_) => {
                    let user = !item_id.is_system();
                    todo.extend(entry.uses().into_iter().map(|use_id| (use_id, user)));
                    InlinedBuiltinKind::View
                }
                CatalogItem::Func(func)
                    if func.inner.func_impls().iter().any(|imp| imp.sql.is_some()) =>
                {
                    InlinedBuiltinKind::Func
                }
                _ => continue,
            };
            if item_id.is_system() {
                let name = self.resolve_full_name(entry.name(), None);
                builtins.insert(InlinedBuiltin {
                    kind,
                    schema: name.schema,
                    name: name.item,
                });
            }
        }
        Ok(builtins)
    }
}

#[cfg(test)]
#[path = "pinned_tests.rs"]
mod tests;
