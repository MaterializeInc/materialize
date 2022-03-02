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

use mz_dataflow_types::sinks::SinkDesc;
use mz_dataflow_types::{BuildDesc, DataflowDesc, IndexDesc};
use mz_expr::{
    GlobalId, MapFilterProject, MirRelationExpr, MirScalarExpr, NullaryFunc,
    OptimizedMirRelationExpr,
};
use mz_ore::stack::maybe_grow;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{Datum, Row, Timestamp};

use crate::catalog::{CatalogItem, CatalogState};
use crate::coord::ArrangementFrontiers;
use crate::coord::Coordinator;
use crate::error::RematerializedSourceType;
use crate::session::{Session, SERVER_MAJOR_VERSION, SERVER_MINOR_VERSION};
use crate::{CoordError, PersisterWithConfig};

/// Borrows of catalog and indexes sufficient to build dataflow descriptions.
pub struct DataflowBuilder<'a> {
    pub catalog: &'a CatalogState,
    pub indexes: &'a ArrangementFrontiers<Timestamp>,
    pub persister: &'a PersisterWithConfig,
    /// A handle to the storage abstraction, which describe sources from their identifier.
    pub storage:
        &'a mz_dataflow_types::client::Controller<Box<dyn mz_dataflow_types::client::Client>>,
}

/// The styles in which an expression can be prepared for use in a dataflow.
#[derive(Clone, Copy, Debug)]
pub enum ExprPrepStyle<'a> {
    /// The expression is being prepared for installation as a maintained index.
    Index,
    /// The expression is being prepared to run once at the specified logical
    /// time in the specified session.
    OneShot {
        logical_time: Option<u64>,
        session: &'a Session,
    },
}

impl Coordinator {
    /// Creates a new dataflow builder from the catalog and indexes in `self`.
    pub fn dataflow_builder<'a>(&'a mut self) -> DataflowBuilder {
        DataflowBuilder {
            catalog: self.catalog.state(),
            indexes: &self.indexes,
            persister: &self.persister,
            storage: &self.dataflow_client,
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
                mz_dataflow_types::IndexDesc {
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
                    CatalogItem::Table(_) => {
                        let source_description = self.catalog.source_description_for(*id).unwrap();
                        let persist_details = None;
                        dataflow.import_source(*id, source_description, persist_details);
                    }
                    CatalogItem::Source(source) => {
                        if source.requires_single_materialization() {
                            let source_type = RematerializedSourceType::for_source(source);
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

                        let source_description = self.catalog.source_description_for(*id).unwrap();

                        let persist_desc = self
                            .persister
                            .load_source_persist_desc(&source)
                            .map_err(CoordError::Persistence)?;

                        dataflow.import_source(*id, source_description, persist_desc);
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
        mut index_description: IndexDesc,
    ) -> Result<DataflowDesc, CoordError> {
        let on_entry = self.catalog.get_by_id(&index_description.on_id);
        let on_type = on_entry.desc().unwrap().typ().clone();
        let mut dataflow = DataflowDesc::new(name, id);
        self.import_into_dataflow(&index_description.on_id, &mut dataflow)?;
        for BuildDesc { view, .. } in &mut dataflow.objects_to_build {
            self.prep_relation_expr(view, ExprPrepStyle::Index)?;
        }
        for key in &mut index_description.key {
            self.prep_scalar_expr(key, ExprPrepStyle::Index)?;
        }
        dataflow.export_index(id, index_description, on_type);

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(&mut dataflow, self.catalog.enabled_indexes())?;

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
        sink_description: SinkDesc,
    ) -> Result<DataflowDesc, CoordError> {
        let mut dataflow = DataflowDesc::new(name, id);
        self.build_sink_dataflow_into(&mut dataflow, id, sink_description)?;
        Ok(dataflow)
    }

    /// Like `build_sink_dataflow`, but builds the sink dataflow into the
    /// existing dataflow description instead of creating one from scratch.
    pub fn build_sink_dataflow_into(
        &mut self,
        dataflow: &mut DataflowDesc,
        id: GlobalId,
        sink_description: SinkDesc,
    ) -> Result<(), CoordError> {
        dataflow.set_as_of(sink_description.as_of.frontier.clone());
        self.import_into_dataflow(&sink_description.from, dataflow)?;
        for BuildDesc { view, .. } in &mut dataflow.objects_to_build {
            self.prep_relation_expr(view, ExprPrepStyle::Index)?;
        }
        dataflow.export_sink(id, sink_description);

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(dataflow, self.catalog.enabled_indexes())?;

        Ok(())
    }

    /// Prepares a relation expression for dataflow execution by preparing all
    /// contained scalar expressions (see `prep_scalar_expr`) in the specified
    /// style.
    pub fn prep_relation_expr(
        &self,
        expr: &mut OptimizedMirRelationExpr,
        style: ExprPrepStyle,
    ) -> Result<(), CoordError> {
        match style {
            ExprPrepStyle::Index => {
                expr.0.try_visit_mut_post(&mut |e| {
                    // Carefully test filter expressions, which may represent temporal filters.
                    if let MirRelationExpr::Filter { input, predicates } = &*e {
                        let mfp =
                            MapFilterProject::new(input.arity()).filter(predicates.iter().cloned());
                        match mfp.into_plan() {
                            Err(e) => coord_bail!("{:?}", e),
                            Ok(_) => Ok(()),
                        }
                    } else {
                        e.try_visit_scalars_mut1(&mut |s| self.prep_scalar_expr(s, style))
                    }
                })
            }
            ExprPrepStyle::OneShot { .. } => expr
                .0
                .try_visit_scalars_mut(&mut |s| self.prep_scalar_expr(s, style)),
        }
    }

    /// Prepares a scalar expression for execution by replacing any placeholders
    /// with their correct values.
    ///
    /// Specifically, calls to nullary functions replaced if `style` is
    /// `OneShot`. If `style` is `Index`, then an error is produced if a call
    /// to a nullary function is encountered.
    pub fn prep_scalar_expr(
        &self,
        expr: &mut MirScalarExpr,
        style: ExprPrepStyle,
    ) -> Result<(), CoordError> {
        match style {
            // Evaluate each nullary function and replace the invocation with
            // the result.
            ExprPrepStyle::OneShot {
                logical_time,
                session,
            } => {
                let mut res = Ok(());
                expr.visit_mut_post(&mut |e| {
                    if let MirScalarExpr::CallNullary(f) = e {
                        match self.eval_nullary_func(f, logical_time, session) {
                            Ok(evaled) => *e = evaled,
                            Err(e) => res = Err(e),
                        }
                    }
                });
                res
            }

            // Reject the query if it contains any nullary function calls.
            ExprPrepStyle::Index => {
                let mut last_observed_nullary_func = None;
                expr.visit_mut_post(&mut |e| {
                    if let MirScalarExpr::CallNullary(f) = e {
                        last_observed_nullary_func = Some(f.clone());
                    }
                });
                if let Some(f) = last_observed_nullary_func {
                    return Err(CoordError::UnmaterializableFunction(f));
                }
                Ok(())
            }
        }
    }

    fn eval_nullary_func(
        &self,
        f: &NullaryFunc,
        logical_time: Option<u64>,
        session: &Session,
    ) -> Result<MirScalarExpr, CoordError> {
        let pack_1d_array = |datums: Vec<Datum>| {
            let mut row = Row::default();
            row.packer()
                .push_array(
                    &[ArrayDimension {
                        lower_bound: 1,
                        length: datums.len(),
                    }],
                    datums,
                )
                .expect("known to be a valid array");
            Ok(MirScalarExpr::Literal(Ok(row), f.output_type()))
        };
        let pack = |datum| {
            Ok(MirScalarExpr::literal_ok(
                datum,
                f.output_type().scalar_type,
            ))
        };

        match f {
            NullaryFunc::CurrentDatabase => pack(Datum::from(session.vars().database())),
            NullaryFunc::CurrentSchemasWithSystem => pack_1d_array(
                session
                    .vars()
                    .search_path()
                    .iter()
                    .map(|s| Datum::String(s))
                    .collect(),
            ),
            NullaryFunc::CurrentSchemasWithoutSystem => {
                use crate::catalog::builtin::{
                    INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_TEMP_SCHEMA,
                    PG_CATALOG_SCHEMA,
                };
                pack_1d_array(
                    session
                        .vars()
                        .search_path()
                        .iter()
                        .filter(|s| {
                            (**s != PG_CATALOG_SCHEMA)
                                && (**s != INFORMATION_SCHEMA)
                                && (**s != MZ_CATALOG_SCHEMA)
                                && (**s != MZ_TEMP_SCHEMA)
                                && (**s != MZ_INTERNAL_SCHEMA)
                        })
                        .map(|s| Datum::String(s))
                        .collect(),
                )
            }
            NullaryFunc::CurrentTimestamp => pack(Datum::from(session.pcx().wall_time)),
            NullaryFunc::CurrentUser => pack(Datum::from(session.user())),
            NullaryFunc::MzClusterId => pack(Datum::from(self.catalog.config().cluster_id)),
            NullaryFunc::MzLogicalTimestamp => match logical_time {
                None => coord_bail!("cannot call mz_logical_timestamp in this context"),
                Some(logical_time) => pack(Datum::from(Numeric::from(logical_time))),
            },
            NullaryFunc::MzSessionId => pack(Datum::from(self.catalog.config().session_id)),
            NullaryFunc::MzUptime => {
                let uptime = self.catalog.config().start_instant.elapsed();
                let uptime = chrono::Duration::from_std(uptime).map_or(Datum::Null, Datum::from);
                pack(uptime)
            }
            NullaryFunc::MzVersion => pack(Datum::from(
                &*self.catalog.config().build_info.human_version(),
            )),
            NullaryFunc::PgBackendPid => pack(Datum::Int32(session.conn_id() as i32)),
            NullaryFunc::PgPostmasterStartTime => {
                pack(Datum::from(self.catalog.config().start_time))
            }
            NullaryFunc::Version => {
                let build_info = self.catalog.config().build_info;
                let version = format!(
                    "PostgreSQL {}.{} on {} (materialized {})",
                    SERVER_MAJOR_VERSION,
                    SERVER_MINOR_VERSION,
                    build_info.target_triple,
                    build_info.version,
                );
                pack(Datum::from(&*version))
            }
        }
    }
}
