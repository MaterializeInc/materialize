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

use mz_catalog::memory::objects::{CatalogEntry, CatalogItem};
use mz_compute_types::plan::pinned::{
    IndexInfo, InlinedBuiltin, InlinedBuiltinKind, InstantiationContext, MaterializedViewInfo,
    MetricSinkInfo, PinError, PinningContext,
};
use mz_expr::MirRelationExpr;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{GlobalId, RelationVersionSelector, ReprRelationType, SqlRelationType};

use crate::catalog::CatalogState;
use crate::optimize::metric_sink::shape_metric_sink_source;

/// The catalog and the coordinator's transient id generator, which together
/// answer everything instantiation asks for.
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
            .ok_or(PinError::UnknownItem(id))?;
        let mut builtins = BTreeSet::new();
        let mut seen = BTreeSet::new();
        // The export and the user views it reads, directly or through other
        // user views, are what planning inlines. Every other item reached is
        // referenced by one of them.
        let mut todo = vec![exported.id()];
        while let Some(item_id) = todo.pop() {
            if !seen.insert(item_id) {
                continue;
            }
            let entry = self.get_entry(&item_id);
            // Mirrors `DataflowBuilder::import_into_dataflow`, which inlines
            // user views and pending replacements, whose collections cannot
            // be read yet, and imports everything else.
            let inlined = item_id == exported.id()
                || match entry.item() {
                    CatalogItem::View(_) => !item_id.is_system(),
                    CatalogItem::MaterializedView(mv) => mv.replacement_target.is_some(),
                    _ => false,
                };
            if inlined {
                todo.extend(entry.uses());
                self.inlined_function_bodies(id, entry, &mut builtins)?;
                continue;
            }
            self.check_referenced_builtin(id, entry, &mut builtins)?;
            if item_id.is_system() && entry.is_relation() {
                return Err(PinError::BuiltinRelation {
                    item: id,
                    relation: self.full_name(entry),
                });
            }
        }
        Ok(builtins)
    }

    fn index_key_builtins(&self, id: GlobalId) -> Result<BTreeSet<InlinedBuiltin>, PinError> {
        let entry = self
            .try_get_entry_by_global_id(&id)
            .filter(|entry| entry.index().is_some())
            .ok_or(PinError::UnknownItem(id))?;
        let mut builtins = BTreeSet::new();
        // The index's references are the indexed object, which the reader
        // imports as an arrangement and does not inline, and the functions in
        // its key, which the reader does inline.
        for item_id in entry.references().items() {
            let referenced = self.get_entry(item_id);
            if !referenced.is_relation() {
                self.check_referenced_builtin(id, referenced, &mut builtins)?;
            }
        }
        self.inlined_function_bodies(id, entry, &mut builtins)?;
        Ok(builtins)
    }
}

impl CatalogState {
    fn full_name(&self, entry: &CatalogEntry) -> String {
        self.resolve_full_name(entry.name(), None).to_string()
    }

    /// Rejects `referenced` if it is unstable, and records it if it is a
    /// SQL-defined builtin function.
    fn check_referenced_builtin(
        &self,
        item: GlobalId,
        referenced: &CatalogEntry,
        builtins: &mut BTreeSet<InlinedBuiltin>,
    ) -> Result<(), PinError> {
        // The builtin registry leaves out unstable builtins, see its module
        // docs. A user item can reference one directly only with
        // `unsafe_enable_unstable_dependencies`, see
        // `CatalogState::check_unstable_dependencies`, but a function body can
        // reference one regardless.
        if self.is_unstable_schema_specifier(referenced.name().qualifiers.schema_spec) {
            return Err(PinError::UnstableDependency {
                item,
                dependency: self.full_name(referenced),
            });
        }
        if let CatalogItem::Func(func) = referenced.item() {
            if referenced.id().is_system()
                && func.inner.func_impls().iter().any(|imp| imp.sql.is_some())
            {
                let name = self.resolve_full_name(referenced.name(), None);
                builtins.insert(InlinedBuiltin {
                    kind: InlinedBuiltinKind::Func,
                    schema: name.schema,
                    name: name.item,
                });
            }
        }
        Ok(())
    }

    /// Checks what planning resolved inside the bodies of the SQL-defined
    /// functions `entry` calls, transitively.
    fn inlined_function_bodies(
        &self,
        item: GlobalId,
        entry: &CatalogEntry,
        builtins: &mut BTreeSet<InlinedBuiltin>,
    ) -> Result<(), PinError> {
        for item_id in entry.item().function_body_references().items() {
            let referenced = self.get_entry(item_id);
            self.check_referenced_builtin(item, referenced, builtins)?;
            // A builtin relation read in a body usually makes the plan import
            // it, and `PinnedDataflow::pin` refuses builtin imports anyway.
            // Constant folding can remove the read, though, so refuse on the
            // reference, which keeps the rule that nothing depending on a
            // builtin relation is pinned.
            if referenced.is_relation() {
                return Err(PinError::BuiltinRelation {
                    item,
                    relation: self.full_name(referenced),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "pinned_tests.rs"]
mod tests;
