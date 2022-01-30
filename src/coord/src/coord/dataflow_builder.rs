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

use super::*;
use crate::error::RematerializedSourceType;
use ore::stack::maybe_grow;

/// Borrows of catalog and indexes sufficient to build dataflow descriptions.
pub struct DataflowBuilder<'a> {
    pub catalog: &'a CatalogState,
    pub indexes: &'a ArrangementFrontiers<Timestamp>,
}

impl<C> Coordinator<C>
where
    C: dataflow_types::client::Client,
{
    /// Creates a new dataflow builder from the catalog and indexes in `self`.
    pub fn dataflow_builder<'a>(&'a mut self) -> DataflowBuilder {
        DataflowBuilder {
            catalog: self.catalog.state(),
            indexes: &self.indexes,
        }
    }

    /// Prepares the arguments to an index build dataflow, by interrogating the catalog.
    ///
    /// Returns `None` if the index entry in the catalog in not enabled.
    pub fn prepare_index_build(
        catalog: &CatalogState,
        index_id: &GlobalId,
    ) -> Option<(String, IndexDesc)> {
        let index_entry = catalog.get_by_id(&index_id);
        let index = match index_entry.item() {
            CatalogItem::Index(index) => index,
            _ => unreachable!("cannot create index dataflow on non-index"),
        };
        if !index.enabled {
            None
        } else {
            Some((
                index_entry.name().to_string(),
                dataflow_types::IndexDesc {
                    on_id: index.on,
                    key: index.keys.clone(),
                },
            ))
        }
    }
}

impl<'a> DataflowBuilder<'a> {
    /// Imports the view, source, or table with `id` into the provided
    /// dataflow description.
    fn import_into_dataflow(
        &mut self,
        id: &GlobalId,
        dataflow: &mut DataflowDesc,
    ) -> Result<(), CoordError> {
        maybe_grow(|| {
            // Avoid importing the item redundantly.
            if dataflow.is_imported(id) {
                return Ok(());
            }

            // A valid index is any index on `id` that is known to the dataflow
            // layer, as indicated by its presence in `self.indexes`.
            let valid_index = self.catalog.enabled_indexes()[id]
                .iter()
                .find(|(id, _keys)| self.indexes.contains_key(*id));
            if let Some((index_id, keys)) = valid_index {
                let index_desc = IndexDesc {
                    on_id: *id,
                    key: keys.to_vec(),
                };
                let desc = self
                    .catalog
                    .get_by_id(id)
                    .desc()
                    .expect("indexes can only be built on items with descs");
                dataflow.import_index(*index_id, index_desc, desc.typ().clone(), *id);
            } else {
                let entry = self.catalog.get_by_id(id);
                match entry.item() {
                    CatalogItem::Table(table) => {
                        let connector = SourceConnector::Local {
                            timeline: table.timeline(),
                            persisted_name: table.persist.as_ref().map(|p| p.stream_name.clone()),
                        };
                        dataflow.import_source(
                            *id,
                            dataflow_types::sources::SourceDesc {
                                name: entry.name().to_string(),
                                connector,
                                operators: None,
                                desc: table.desc.clone(),
                            },
                        );
                    }
                    CatalogItem::Source(source) => {
                        if source.connector.requires_single_materialization() {
                            let source_type =
                                RematerializedSourceType::for_connector(&source.connector);
                            let dependent_indexes = self.catalog.dependent_indexes(*id);
                            // If this source relies on any pre-existing indexes (i.e., indexes
                            // that we're not building as part of this `DataflowBuilder`), we're
                            // attempting to reinstantiate a single-use source.
                            let intersection = self.indexes.intersection(dependent_indexes);
                            if !intersection.is_empty() {
                                let existing_indexes = intersection
                                    .iter()
                                    .map(|id| self.catalog.get_by_id(id).name().item.clone())
                                    .collect();
                                return Err(CoordError::InvalidRematerialization {
                                    base_source: entry.name().item.clone(),
                                    existing_indexes,
                                    source_type,
                                });
                            }
                        }

                        let mut connector = source.connector.clone();

                        let persist_details = self
                            .catalog
                            .persist()
                            .source_persist_desc_from_serialized(
                                &source.connector,
                                source.persist_details.clone(),
                            )
                            .map_err(CoordError::Persistence)?;

                        // TODO: I don't like that we're injecting this into the otherwise "pristine"
                        // immutable SourceConnector. We should clean this up once we have an
                        // ingestd/dataflowd split, where we probably want to send SourceConnector only to
                        // ingestd (and always with persistence details) and dataflowd will never see the
                        // current style of SourceConnector.
                        match &mut connector {
                            SourceConnector::External { persist, .. } => {
                                assert!(persist.is_none());
                                *persist = persist_details;
                            }
                            SourceConnector::Local { .. } => unreachable!(),
                        }

                        let source_connector = dataflow_types::sources::SourceDesc {
                            name: entry.name().to_string(),
                            connector,
                            operators: None,
                            desc: source.desc.clone(),
                        };

                        dataflow.import_source(*id, source_connector);
                    }
                    CatalogItem::View(view) => {
                        let expr = view.optimized_expr.clone();
                        self.import_view_into_dataflow(id, &expr, dataflow)?;
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        })
    }

    /// Imports the view with the specified ID and expression into the provided
    /// dataflow description.
    pub fn import_view_into_dataflow(
        &mut self,
        view_id: &GlobalId,
        view: &OptimizedMirRelationExpr,
        dataflow: &mut DataflowDesc,
    ) -> Result<(), CoordError> {
        // TODO: We only need to import Get arguments for which we cannot find arrangements.
        for get_id in view.global_uses() {
            self.import_into_dataflow(&get_id, dataflow)?;

            // TODO: indexes should be imported after the optimization process, and only those
            // actually used by the optimized plan
            if let Some(indexes) = self.catalog.enabled_indexes().get(&get_id) {
                for (id, keys) in indexes.iter() {
                    // Ensure only valid indexes (i.e. those in self.indexes) are imported.
                    // TODO(#8318): Ensure this logic is accounted for.
                    if !self.indexes.contains_key(*id) {
                        continue;
                    }
                    let on_entry = self.catalog.get_by_id(&get_id);
                    let on_type = on_entry.desc().unwrap().typ().clone();
                    let index_desc = IndexDesc {
                        on_id: get_id,
                        key: keys.clone(),
                    };
                    dataflow.import_index(*id, index_desc, on_type, *view_id);
                }
            }
        }
        dataflow.insert_view(*view_id, view.clone());
        Ok(())
    }

    /// Builds a dataflow description for the index with the specified ID.
    pub fn build_index_dataflow(
        &mut self,
        name: String,
        id: GlobalId,
        index_description: dataflow_types::IndexDesc,
    ) -> Result<DataflowDesc, CoordError> {
        let on_entry = self.catalog.get_by_id(&index_description.on_id);
        let on_type = on_entry.desc().unwrap().typ().clone();
        let mut dataflow = DataflowDesc::new(name);
        self.import_into_dataflow(&index_description.on_id, &mut dataflow)?;
        dataflow.export_index(id, index_description, on_type);
        Ok(dataflow)
    }

    /// Builds a dataflow description for the sink with the specified name,
    /// ID, source, and output connector.
    ///
    /// For as long as this dataflow is active, `id` can be used to reference
    /// the sink (primarily to drop it, at the moment).
    pub fn build_sink_dataflow(
        &mut self,
        name: String,
        id: GlobalId,
        sink_description: dataflow_types::sinks::SinkDesc,
    ) -> Result<DataflowDesc, CoordError> {
        let mut dataflow = DataflowDesc::new(name);
        dataflow.set_as_of(sink_description.as_of.frontier.clone());
        self.import_into_dataflow(&sink_description.from, &mut dataflow)?;
        dataflow.export_sink(id, sink_description);
        Ok(dataflow)
    }
}
