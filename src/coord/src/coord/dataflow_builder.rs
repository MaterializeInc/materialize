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

use mz_dataflow_types::client::controller::ComputeController;
use mz_dataflow_types::client::ComputeInstanceId;
use mz_dataflow_types::sinks::SinkDesc;
use mz_dataflow_types::{BuildDesc, DataflowDesc, IndexDesc};
use mz_expr::{
    CollectionPlan, GlobalId, MapFilterProject, MirRelationExpr, MirScalarExpr,
    OptimizedMirRelationExpr, UnmaterializableFunc,
};
use mz_ore::stack::maybe_grow;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{Datum, Row};

use crate::catalog::{CatalogItem, CatalogState};
use crate::coord::{CatalogTxn, Coordinator};
use crate::error::RematerializedSourceType;
use crate::session::{Session, SERVER_MAJOR_VERSION, SERVER_MINOR_VERSION};
use crate::{CoordError, PersisterWithConfig};

/// Borrows of catalog and indexes sufficient to build dataflow descriptions.
pub struct DataflowBuilder<'a, T> {
    pub catalog: &'a CatalogState,
    pub persister: &'a PersisterWithConfig,
    /// A handle to the compute abstraction, which describes indexes by identifier.
    ///
    /// This can also be used to grab a handle to the storage abstraction, through
    /// its `storage_mut()` method.
    pub compute: ComputeController<'a, T>,
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
    pub fn dataflow_builder(
        &self,
        instance: ComputeInstanceId,
    ) -> DataflowBuilder<mz_repr::Timestamp> {
        let compute = self.dataflow_client.compute(instance).unwrap();
        DataflowBuilder {
            catalog: self.catalog.state(),
            persister: &self.persister,
            compute,
        }
    }
}

impl CatalogTxn<'_, mz_repr::Timestamp> {
    /// Creates a new dataflow builder from an ongoing catalog transaction.
    pub fn dataflow_builder(
        &self,
        instance: ComputeInstanceId,
    ) -> DataflowBuilder<mz_repr::Timestamp> {
        let compute = self.dataflow_client.compute(instance).unwrap();
        DataflowBuilder {
            catalog: self.catalog,
            persister: &self.persister,
            compute,
        }
    }
}

impl<'a> DataflowBuilder<'a, mz_repr::Timestamp> {
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

            // A valid index is any index on `id` that is known to index oracle.
            //
            // TODO: indexes should be imported after the optimization process,
            // and only those actually used by the optimized plan
            //
            // NOTE(benesch): is the above TODO still true? The dataflow layer
            // has gotten increasingly smart about index selection. Maybe it's
            // now fine to present all indexes?
            let index_oracle = self.index_oracle();
            let mut valid_indexes = index_oracle.indexes_on(*id).peekable();
            if valid_indexes.peek().is_some() {
                // Deduplicate indexes by keys, in case we have redundant indexes.
                let mut valid_indexes = valid_indexes.collect::<Vec<_>>();
                valid_indexes.sort_by_key(|(_, idx)| &idx.keys);
                valid_indexes.dedup_by_key(|(_, idx)| &idx.keys);
                for (index_id, idx) in valid_indexes {
                    let index_desc = IndexDesc {
                        on_id: *id,
                        key: idx.keys.to_vec(),
                    };
                    let desc = self
                        .catalog
                        .get_by_id(id)
                        .desc()
                        .expect("indexes can only be built on items with descs");
                    dataflow.import_index(index_id, index_desc, desc.typ().clone());
                }
            } else {
                drop(valid_indexes);
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
                            let intersection = dependent_indexes
                                .into_iter()
                                .filter(|id| self.compute.collection(*id).is_ok())
                                .collect::<Vec<_>>();

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
    ///
    /// You should generally prefer calling
    /// [`DataflowBuilder::import_into_dataflow`], which can handle objects of
    /// any type as long as they exist in the catalog. This method exists for
    /// when the view does not exist in the catalog, e.g., because it is
    /// identified by a [`GlobalId::Transient`].
    pub fn import_view_into_dataflow(
        &mut self,
        view_id: &GlobalId,
        view: &OptimizedMirRelationExpr,
        dataflow: &mut DataflowDesc,
    ) -> Result<(), CoordError> {
        for get_id in view.depends_on() {
            self.import_into_dataflow(&get_id, dataflow)?;
        }
        dataflow.insert_plan(*view_id, view.clone());
        Ok(())
    }

    /// Builds a dataflow description for the index with the specified ID.
    ///
    /// Returns `None` if the index is not enabled.
    ///
    /// TODO(benesch): The `DataflowBuilder` shouldn't be in charge of checking
    /// whether the index is enabled, but it will be easier to clean that up
    /// when the concept of a "cluster" has been plumbed a bit further.
    pub fn build_index_dataflow(
        &mut self,
        id: GlobalId,
    ) -> Result<Option<DataflowDesc>, CoordError> {
        let index_entry = self.catalog.get_by_id(&id);
        let index = match index_entry.item() {
            CatalogItem::Index(index) => index,
            _ => unreachable!("cannot create index dataflow on non-index"),
        };
        if !index.enabled {
            return Ok(None);
        }
        let on_entry = self.catalog.get_by_id(&index.on);
        let on_type = on_entry.desc().unwrap().typ().clone();
        let name = index_entry.name().to_string();
        let mut dataflow = DataflowDesc::new(name);
        self.import_into_dataflow(&index.on, &mut dataflow)?;
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            self.prep_relation_expr(plan, ExprPrepStyle::Index)?;
        }
        let mut index_description = mz_dataflow_types::IndexDesc {
            on_id: index.on,
            key: index.keys.clone(),
        };
        for key in &mut index_description.key {
            self.prep_scalar_expr(key, ExprPrepStyle::Index)?;
        }
        dataflow.export_index(id, index_description, on_type);

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(&mut dataflow, &self.index_oracle())?;

        Ok(Some(dataflow))
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
        let mut dataflow = DataflowDesc::new(name);
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
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            self.prep_relation_expr(plan, ExprPrepStyle::Index)?;
        }
        dataflow.export_sink(id, sink_description);

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(dataflow, &self.index_oracle())?;

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

    /// Prepares a scalar expression for execution by handling unmaterializable
    /// functions.
    ///
    /// Specifically, calls to unmaterializable functions are replaced if
    /// `style` is `OneShot`. If `style` is `Index`, then an error is produced
    /// if a call to a unmaterializable function is encountered.
    pub fn prep_scalar_expr(
        &self,
        expr: &mut MirScalarExpr,
        style: ExprPrepStyle,
    ) -> Result<(), CoordError> {
        match style {
            // Evaluate each unmaterializable function and replace the
            // invocation with the result.
            ExprPrepStyle::OneShot {
                logical_time,
                session,
            } => {
                let mut res = Ok(());
                expr.visit_mut_post(&mut |e| {
                    if let MirScalarExpr::CallUnmaterializable(f) = e {
                        match self.eval_unmaterializable_func(f, logical_time, session) {
                            Ok(evaled) => *e = evaled,
                            Err(e) => res = Err(e),
                        }
                    }
                });
                res
            }

            // Reject the query if it contains any unmaterializable function calls.
            ExprPrepStyle::Index => {
                let mut last_observed_unmaterializable_func = None;
                expr.visit_mut_post(&mut |e| {
                    if let MirScalarExpr::CallUnmaterializable(f) = e {
                        last_observed_unmaterializable_func = Some(f.clone());
                    }
                });
                if let Some(f) = last_observed_unmaterializable_func {
                    return Err(CoordError::UnmaterializableFunction(f));
                }
                Ok(())
            }
        }
    }

    fn eval_unmaterializable_func(
        &self,
        f: &UnmaterializableFunc,
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
            UnmaterializableFunc::CurrentDatabase => pack(Datum::from(session.vars().database())),
            UnmaterializableFunc::CurrentSchemasWithSystem => pack_1d_array(
                session
                    .vars()
                    .search_path()
                    .iter()
                    .map(|s| Datum::String(s))
                    .collect(),
            ),
            UnmaterializableFunc::CurrentSchemasWithoutSystem => {
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
            UnmaterializableFunc::CurrentTimestamp => pack(Datum::from(session.pcx().wall_time)),
            UnmaterializableFunc::CurrentUser => pack(Datum::from(session.user())),
            UnmaterializableFunc::MzClusterId => {
                pack(Datum::from(self.catalog.config().cluster_id))
            }
            UnmaterializableFunc::MzLogicalTimestamp => match logical_time {
                None => coord_bail!("cannot call mz_logical_timestamp in this context"),
                Some(logical_time) => pack(Datum::from(Numeric::from(logical_time))),
            },
            UnmaterializableFunc::MzSessionId => {
                pack(Datum::from(self.catalog.config().session_id))
            }
            UnmaterializableFunc::MzUptime => {
                let uptime = self.catalog.config().start_instant.elapsed();
                let uptime = chrono::Duration::from_std(uptime).map_or(Datum::Null, Datum::from);
                pack(uptime)
            }
            UnmaterializableFunc::MzVersion => pack(Datum::from(
                &*self.catalog.config().build_info.human_version(),
            )),
            UnmaterializableFunc::PgBackendPid => pack(Datum::Int32(session.conn_id() as i32)),
            UnmaterializableFunc::PgPostmasterStartTime => {
                pack(Datum::from(self.catalog.config().start_time))
            }
            UnmaterializableFunc::Version => {
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
