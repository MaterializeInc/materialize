// Copyright Materialize, Inc. All rights reserved.
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
            dataflow.add_index_import(*index_id, index_desc, desc.typ().clone(), *id);
        } else {
            // This is only needed in the case of a source with a transformation, but we generate it now to
            // get around borrow checker issues.
            let transient_id = *self.transient_id_counter;
            *self.transient_id_counter = transient_id
                .checked_add(1)
                .expect("id counter overflows i64");
            match self.catalog.get_by_id(id).item() {
                CatalogItem::Table(table) => {
                    dataflow.add_source_import(
                        *id,
                        SourceConnector::Local,
                        table.desc.clone(),
                        *id,
                    );
                }
                CatalogItem::Source(source) => {
                    // If Materialize has caching enabled, check to see if the source has any
                    // already cached data that can be reused, and if so, augment the source
                    // connector to use that data before importing it into the dataflow.
                    let connector = if let Some(path) =
                        self.catalog.config().cache_directory.as_deref()
                    {
                        match crate::cache::augment_connector(
                            source.connector.clone(),
                            &path,
                            self.catalog.config().cluster_id,
                            *id,
                        ) {
                            Ok(Some(connector)) => Some(connector),
                            Ok(None) => None,
                            Err(e) => {
                                log::error!("encountered error while trying to reuse cached data for source {}: {}", id.to_string(), e);
                                log::trace!(
                                    "continuing without cached data for source {}",
                                    id.to_string()
                                );
                                None
                            }
                        }
                    } else {
                        None
                    };

                    // Default back to the regular connector if we didn't get a augmented one.
                    let connector = connector.unwrap_or_else(|| source.connector.clone());

                    if source.optimized_expr.0.is_trivial_source() {
                        dataflow.add_source_import(*id, connector, source.bare_desc.clone(), *id);
                    } else {
                        // From the dataflow layer's perspective, the source transformation is just a view (across which it should be able to do whole-dataflow optimizations).
                        // Install it as such (giving the source a global transient ID by which the view/transformation can refer to it)
                        let bare_source_id = GlobalId::Transient(transient_id);
                        dataflow.add_source_import(
                            bare_source_id,
                            connector,
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
        for get_id in view.as_ref().global_uses() {
            self.import_into_dataflow(&get_id, dataflow);
        }
        // Collect sources, views, and indexes used.
        view.as_ref().visit(&mut |e| {
            if let MirRelationExpr::ArrangeBy { input, keys } = e {
                if let MirRelationExpr::Get {
                    id: Id::Global(on_id),
                    typ,
                } = &**input
                {
                    for key_set in keys {
                        let index_desc = IndexDesc {
                            on_id: *on_id,
                            keys: key_set.to_vec(),
                        };
                        // If the arrangement exists, import it. It may not exist, in which
                        // case we should import the source to be sure that we have access
                        // to the collection to arrange it ourselves.
                        let indexes = &self.catalog.indexes()[on_id];
                        if let Some((id, _)) = indexes.iter().find(|(_id, keys)| keys == key_set) {
                            dataflow.add_index_import(*id, index_desc, typ.clone(), *view_id);
                        }
                    }
                }
            }
        });
        dataflow.add_view_to_build(*view_id, view.clone(), view.as_ref().typ());
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
        dataflow.add_index_to_build(id, on_id.clone(), on_type.clone(), keys.clone());
        dataflow.add_index_export(id, on_id, on_type, keys);
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
        dataflow.add_sink_export(id, from, from_type, connector, envelope, as_of);
        dataflow
    }
}
