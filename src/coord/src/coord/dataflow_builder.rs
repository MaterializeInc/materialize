// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and methods for building dataflow descriptions.
//!
//! Dataflows are buildable from the coordinator's `catalog` and `indexes`
//! members, which respectively describe the collection backing identifiers
//! and indicate which identifiers have arrangements available. This module
//! isolates that logic from the rest of the somewhat complicated coordinator.

use dataflow_types::SinkAsOf;

use super::*;

/// Borrows of catalog and indexes sufficient to build dataflow descriptions.
pub struct DataflowBuilder<'a> {
    catalog: &'a Catalog,
    indexes: &'a ArrangementFrontiers<Timestamp>,
    transient_id_counter: &'a mut u64,
}

impl Coordinator {
    /// Creates a new dataflow builder from the catalog and indexes in `self`.
    pub fn dataflow_builder(&mut self) -> DataflowBuilder {
        DataflowBuilder {
            catalog: &self.catalog,
            indexes: &self.indexes,
            transient_id_counter: &mut self.transient_id_counter,
        }
    }
}

impl<'a> DataflowBuilder<'a> {
    /// Imports the view, source, or table with `id` into the provided
    /// dataflow description.
    fn import_into_dataflow(&mut self, id: &GlobalId, dataflow: &mut DataflowDesc) {
        // Avoid importing the item redundantly.
        if dataflow.is_imported(id) {
            return;
        }

        // A valid index is any index on `id` that is known to the dataflow
        // layer, as indicated by its presence in `self.indexes`.
        let valid_index = self.catalog.indexes()[id]
            .iter()
            .find(|(id, _keys)| self.indexes.contains_key(*id));
        if let Some((index_id, keys)) = valid_index {
            let index_desc = IndexDesc {
                on_id: *id,
                keys: keys.to_vec(),
            };
            let desc = self
                .catalog
                .get_by_id(id)
                .desc()
                .expect("indexes can only be built on items with descs");
            dataflow.import_index(*index_id, index_desc, desc.typ().clone(), *id);
        } else {
            // This is only needed in the case of a source with a transformation, but we generate it now to
            // get around borrow checker issues.
            let transient_id = *self.transient_id_counter;
            *self.transient_id_counter = transient_id
                .checked_add(1)
                .expect("id counter overflows i64");
            let entry = self.catalog.get_by_id(id);
            match entry.item() {
                CatalogItem::Table(table) => {
                    dataflow.import_source(
                        entry.name().to_string(),
                        *id,
                        SourceConnector::Local(table.timeline()),
                        table.desc.clone(),
                        *id,
                    );
                }
                CatalogItem::Source(source) => {
                    if source.optimized_expr.0.is_trivial_source() {
                        dataflow.import_source(
                            entry.name().to_string(),
                            *id,
                            source.connector.clone(),
                            source.bare_desc.clone(),
                            *id,
                        );
                    } else {
                        // From the dataflow layer's perspective, the source transformation is just a view (across which it should be able to do whole-dataflow optimizations).
                        // Install it as such (giving the source a global transient ID by which the view/transformation can refer to it)
                        let bare_source_id = GlobalId::Transient(transient_id);
                        dataflow.import_source(
                            entry.name().to_string(),
                            bare_source_id,
                            source.connector.clone(),
                            source.bare_desc.clone(),
                            *id,
                        );
                        let mut transformation = source.optimized_expr.clone();
                        transformation.0.visit_mut(&mut |node| {
                            match node {
                                MirRelationExpr::Get { id, .. } if *id == Id::LocalBareSource => {
                                    *id = Id::Global(bare_source_id);
                                }
                                _ => {}
                            };
                        });
                        self.import_view_into_dataflow(id, &transformation, dataflow);
                    }
                }
                CatalogItem::View(view) => {
                    let expr = view.optimized_expr.clone();
                    self.import_view_into_dataflow(id, &expr, dataflow);
                }
                _ => unreachable!(),
            }
        }
    }

    /// Imports the view with the specified ID and expression into the provided
    /// dataflow description.
    pub fn import_view_into_dataflow(
        &mut self,
        view_id: &GlobalId,
        view: &OptimizedMirRelationExpr,
        dataflow: &mut DataflowDesc,
    ) {
        // TODO: We only need to import Get arguments for which we cannot find arrangements.
        for get_id in view.global_uses() {
            self.import_into_dataflow(&get_id, dataflow);

            // TODO: indexes should be imported after the optimization process, and only those
            // actually used by the optimized plan
            let indexes = &self.catalog.indexes()[&get_id];
            for (id, keys) in indexes.iter() {
                let on_entry = self.catalog.get_by_id(&get_id);
                let on_type = on_entry.desc().unwrap().typ().clone();
                let index_desc = IndexDesc {
                    on_id: get_id,
                    keys: keys.clone(),
                };
                dataflow.import_index(*id, index_desc, on_type, *view_id);
            }
        }
        dataflow.insert_view(*view_id, view.clone());
    }

    /// Builds a dataflow description for the index with the specified ID.
    pub fn build_index_dataflow(&mut self, id: GlobalId) -> DataflowDesc {
        let index_entry = self.catalog.get_by_id(&id);
        let index = match index_entry.item() {
            CatalogItem::Index(index) => index,
            _ => unreachable!("cannot create index dataflow on non-index"),
        };
        let on_entry = self.catalog.get_by_id(&index.on);
        let on_type = on_entry.desc().unwrap().typ().clone();
        let mut dataflow = DataflowDesc::new(index_entry.name().to_string());
        let on_id = index.on;
        let keys = index.keys.clone();
        self.import_into_dataflow(&on_id, &mut dataflow);
        dataflow.export_index(id, on_id, on_type, keys);
        dataflow
    }

    /// Builds a dataflow description for the sink with the specified name,
    /// ID, source, and output connector.
    pub fn build_sink_dataflow(
        &mut self,
        name: String,
        id: GlobalId,
        from: GlobalId,
        connector: SinkConnector,
        envelope: SinkEnvelope,
        as_of: SinkAsOf,
    ) -> DataflowDesc {
        let mut dataflow = DataflowDesc::new(name);
        dataflow.set_as_of(as_of.frontier.clone());
        self.import_into_dataflow(&from, &mut dataflow);
        let from_type = self.catalog.get_by_id(&from).desc().unwrap().clone();
        dataflow.export_sink(id, from, from_type, connector, envelope, as_of);
        dataflow
    }
}
