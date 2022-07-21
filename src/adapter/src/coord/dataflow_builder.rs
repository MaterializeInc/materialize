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

use timely::progress::Antichain;

use mz_compute_client::command::{BuildDesc, DataflowDesc, IndexDesc};
use mz_compute_client::controller::{ComputeController, ComputeInstanceId};
use mz_expr::visit::Visit;
use mz_expr::{
    CollectionPlan, Id, MapFilterProject, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr,
    UnmaterializableFunc, RECURSION_LIMIT,
};
use mz_ore::stack::{maybe_grow, CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{Datum, GlobalId, Row, Timestamp};
use mz_stash::Append;
use mz_storage::types::sinks::{PersistSinkConnection, SinkAsOf, SinkConnection, SinkDesc};
use std::collections::{HashMap, HashSet};
use tracing::warn;

use crate::catalog::{CatalogItem, CatalogState, RecordedView, View};
use crate::coord::{CatalogTxn, Coordinator};
use crate::session::{Session, SERVER_MAJOR_VERSION, SERVER_MINOR_VERSION};
use crate::AdapterError;

/// Borrows of catalog and indexes sufficient to build dataflow descriptions.
pub struct DataflowBuilder<'a, T> {
    pub catalog: &'a CatalogState,
    /// A handle to the compute abstraction, which describes indexes by identifier.
    ///
    /// This can also be used to grab a handle to the storage abstraction, through
    /// its `storage_mut()` method.
    pub compute: ComputeController<'a, T>,
    recursion_guard: RecursionGuard,
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
    /// The expression is being prepared for evaluation in an AS OF clause.
    AsOf,
}

impl<S: Append> Coordinator<S> {
    /// Creates a new dataflow builder from the catalog and indexes in `self`.
    pub fn dataflow_builder(
        &self,
        instance: ComputeInstanceId,
    ) -> DataflowBuilder<mz_repr::Timestamp> {
        let compute = self.controller.compute(instance).unwrap();
        DataflowBuilder {
            catalog: self.catalog.state(),
            compute,
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
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
            compute,
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
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
    ) -> Result<(), AdapterError> {
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
                    let entry = self.catalog.get_entry(id);
                    let desc = entry
                        .desc(
                            &self
                                .catalog
                                .resolve_full_name(entry.name(), entry.conn_id()),
                        )
                        .expect("indexes can only be built on items with descs");
                    let monotonic = self.monotonic_view(*id);
                    dataflow.import_index(index_id, index_desc, desc.typ().clone(), monotonic);
                }
            } else {
                drop(valid_indexes);
                let entry = self.catalog.get_entry(id);
                match entry.item() {
                    CatalogItem::Table(table) => {
                        dataflow.import_source(*id, table.desc.typ().clone(), false);
                    }
                    CatalogItem::Source(source) => {
                        dataflow.import_source(
                            *id,
                            source.desc.typ().clone(),
                            source.source_desc.monotonic(),
                        );
                    }
                    CatalogItem::View(view) => {
                        let expr = view.optimized_expr.clone();
                        self.import_view_into_dataflow(id, &expr, dataflow)?;
                    }
                    CatalogItem::RecordedView(rview) => {
                        let monotonic = self.monotonic_view(*id);
                        dataflow.import_source(*id, rview.desc.typ().clone(), monotonic);
                    }
                    CatalogItem::Log(log) => {
                        dataflow.import_source(*id, log.variant.desc().typ().clone(), false);
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
    ) -> Result<(), AdapterError> {
        for get_id in view.depends_on() {
            self.import_into_dataflow(&get_id, dataflow)?;
        }
        dataflow.insert_plan(*view_id, view.clone());
        Ok(())
    }

    /// Builds a dataflow description for the index with the specified ID.
    pub fn build_index_dataflow(&mut self, id: GlobalId) -> Result<DataflowDesc, AdapterError> {
        let index_entry = self.catalog.get_entry(&id);
        let index = match index_entry.item() {
            CatalogItem::Index(index) => index,
            _ => unreachable!("cannot create index dataflow on non-index"),
        };
        let on_entry = self.catalog.get_entry(&index.on);
        let on_type = on_entry
            .desc(
                &self
                    .catalog
                    .resolve_full_name(on_entry.name(), on_entry.conn_id()),
            )
            .unwrap()
            .typ()
            .clone();
        let name = index_entry.name().to_string();
        let mut dataflow = DataflowDesc::new(name);
        self.import_into_dataflow(&index.on, &mut dataflow)?;
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            prep_relation_expr(self.catalog, plan, ExprPrepStyle::Index)?;
        }
        let mut index_description = mz_compute_client::command::IndexDesc {
            on_id: index.on,
            key: index.keys.clone(),
        };
        for key in &mut index_description.key {
            prep_scalar_expr(self.catalog, key, ExprPrepStyle::Index)?;
        }
        dataflow.export_index(id, index_description, on_type);

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(&mut dataflow, &self.index_oracle())?;

        Ok(dataflow)
    }

    /// Builds a dataflow description for the sink with the specified name,
    /// ID, source, and output connection.
    ///
    /// For as long as this dataflow is active, `id` can be used to reference
    /// the sink (primarily to drop it, at the moment).
    pub fn build_sink_dataflow(
        &mut self,
        name: String,
        id: GlobalId,
        sink_description: SinkDesc,
    ) -> Result<DataflowDesc, AdapterError> {
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
    ) -> Result<(), AdapterError> {
        dataflow.set_as_of(sink_description.as_of.frontier.clone());
        self.import_into_dataflow(&sink_description.from, dataflow)?;
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            prep_relation_expr(self.catalog, plan, ExprPrepStyle::Index)?;
        }
        dataflow.export_sink(id, sink_description);

        // Optimize the dataflow across views, and any other ways that appeal.
        mz_transform::optimize_dataflow(dataflow, &self.index_oracle())?;

        Ok(())
    }

    /// Builds a dataflow description for the recorded view specified by `id`.
    ///
    /// For this, we first build a dataflow for the view expression, then we
    /// add a sink that writes that dataflow's output to storage.
    /// `internal_view_id` is the ID we assign to the view dataflow internally,
    /// so we can connect the sink to it.
    pub fn build_recorded_view_dataflow(
        &mut self,
        id: GlobalId,
        as_of: Antichain<Timestamp>,
        internal_view_id: GlobalId,
    ) -> Result<DataflowDesc, AdapterError> {
        let rview_entry = self.catalog.get_entry(&id);
        let rview = match rview_entry.item() {
            CatalogItem::RecordedView(rv) => rv,
            _ => unreachable!(
                "cannot create recorded view dataflow on something that is not a recorded view"
            ),
        };

        let name = rview_entry.name().to_string();
        let mut dataflow = DataflowDesc::new(name);

        self.import_view_into_dataflow(&internal_view_id, &rview.optimized_expr, &mut dataflow)?;
        for BuildDesc { plan, .. } in &mut dataflow.objects_to_build {
            prep_relation_expr(self.catalog, plan, ExprPrepStyle::Index)?;
        }

        let sink_description = SinkDesc {
            from: internal_view_id,
            from_desc: rview.desc.clone(),
            connection: SinkConnection::Persist(PersistSinkConnection {
                value_desc: rview.desc.clone(),
                storage_metadata: (),
            }),
            envelope: None,
            as_of: SinkAsOf {
                frontier: as_of,
                strict: false,
            },
        };
        self.build_sink_dataflow_into(&mut dataflow, id, sink_description)?;

        Ok(dataflow)
    }

    /// Determine the given view's monotonicity.
    ///
    /// This recursively traverses the expressions of all (recorded) views involved in the given
    /// view's query expression. If this becomes a performance problem, we could add the
    /// monotonicity information of views into the catalog instead.
    fn monotonic_view(&self, id: GlobalId) -> bool {
        self.monotonic_view_inner(id, &mut HashMap::new())
            .unwrap_or_else(|e| {
                warn!("Error inspecting view {id} for monotonicity: {e}");
                false
            })
    }

    fn monotonic_view_inner(
        &self,
        id: GlobalId,
        memo: &mut HashMap<GlobalId, bool>,
    ) -> Result<bool, RecursionLimitError> {
        self.checked_recur(|_| {
            match self.catalog.get_entry(&id).item() {
                CatalogItem::Source(source) => Ok(source.source_desc.monotonic()),
                CatalogItem::View(View { optimized_expr, .. })
                | CatalogItem::RecordedView(RecordedView { optimized_expr, .. }) => {
                    let mut view_expr = optimized_expr.clone().into_inner();

                    // Inspect global ids that occur in the Gets in view_expr, and collect the ids
                    // of monotonic (recorded) views and sources (but not indexes).
                    let mut monotonic_ids = HashSet::new();
                    let recursion_result: Result<(), RecursionLimitError> = view_expr
                        .try_visit_post(&mut |e| {
                            if let MirRelationExpr::Get {
                                id: Id::Global(got_id),
                                ..
                            } = e
                            {
                                let got_id = *got_id;

                                // A view might be reached multiple times. If we already computed
                                // the monotonicity of the gid, then use that. If not, then compute
                                // it now.
                                let monotonic = match memo.get(&got_id) {
                                    Some(monotonic) => *monotonic,
                                    None => {
                                        let monotonic = self.monotonic_view_inner(got_id, memo)?;
                                        memo.insert(got_id, monotonic);
                                        monotonic
                                    }
                                };
                                if monotonic {
                                    monotonic_ids.insert(got_id);
                                }
                            }
                            Ok(())
                        });
                    if let Err(error) = recursion_result {
                        // We still might have got some of the IDs, so just log and continue. Now
                        // the subsequent monotonicity analysis can have false negatives.
                        warn!("Error inspecting view {id} for monotonicity: {error}");
                    }

                    // Use `monotonic_ids` as a starting point for propagating monotonicity info.
                    mz_transform::monotonic::MonotonicFlag::default().apply(
                        &mut view_expr,
                        &monotonic_ids,
                        &mut HashSet::new(),
                    )
                }
                _ => Ok(false),
            }
        })
    }
}

impl<'a, T> CheckedRecursion for DataflowBuilder<'a, T> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

/// Prepares a relation expression for dataflow execution by preparing all
/// contained scalar expressions (see `prep_scalar_expr`) in the specified
/// style.
pub fn prep_relation_expr(
    catalog: &CatalogState,
    expr: &mut OptimizedMirRelationExpr,
    style: ExprPrepStyle,
) -> Result<(), AdapterError> {
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
                    e.try_visit_scalars_mut1(&mut |s| prep_scalar_expr(catalog, s, style))
                }
            })
        }
        ExprPrepStyle::OneShot { .. } | ExprPrepStyle::AsOf => expr
            .0
            .try_visit_scalars_mut(&mut |s| prep_scalar_expr(catalog, s, style)),
    }
}

/// Prepares a scalar expression for execution by handling unmaterializable
/// functions.
///
/// Specifically, calls to unmaterializable functions are replaced if
/// `style` is `OneShot`. If `style` is `Index`, then an error is produced
/// if a call to a unmaterializable function is encountered.
pub fn prep_scalar_expr(
    state: &CatalogState,
    expr: &mut MirScalarExpr,
    style: ExprPrepStyle,
) -> Result<(), AdapterError> {
    match style {
        // Evaluate each unmaterializable function and replace the
        // invocation with the result.
        ExprPrepStyle::OneShot {
            logical_time,
            session,
        } => expr.try_visit_mut_post(&mut |e| {
            if let MirScalarExpr::CallUnmaterializable(f) = e {
                *e = eval_unmaterializable_func(state, f, logical_time, session)?;
            }
            Ok(())
        }),

        // Reject the query if it contains any unmaterializable function calls.
        ExprPrepStyle::Index | ExprPrepStyle::AsOf => {
            let mut last_observed_unmaterializable_func = None;
            expr.visit_mut_post(&mut |e| {
                if let MirScalarExpr::CallUnmaterializable(f) = e {
                    last_observed_unmaterializable_func = Some(f.clone());
                }
            })?;

            if let Some(f) = last_observed_unmaterializable_func {
                let err = match style {
                    ExprPrepStyle::Index => AdapterError::UnmaterializableFunction(f),
                    ExprPrepStyle::AsOf => AdapterError::UncallableFunction {
                        func: f,
                        context: "AS OF",
                    },
                    _ => unreachable!(),
                };
                return Err(err);
            }
            Ok(())
        }
    }
}

fn eval_unmaterializable_func(
    state: &CatalogState,
    f: &UnmaterializableFunc,
    logical_time: Option<u64>,
    session: &Session,
) -> Result<MirScalarExpr, AdapterError> {
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
        UnmaterializableFunc::CurrentSchemasWithSystem => {
            let search_path = session.vars().search_path();
            let mut v = Vec::with_capacity(search_path.len() + 2);
            let default_schemas = ["mz_catalog", "pg_catalog"];
            for schema in default_schemas {
                if !search_path.contains(&schema) {
                    v.push(Datum::String(schema))
                }
            }
            for schema in search_path {
                v.push(Datum::String(schema))
            }
            pack_1d_array(v)
        }
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
        UnmaterializableFunc::MzClusterId => pack(Datum::from(state.config().cluster_id)),
        UnmaterializableFunc::MzLogicalTimestamp => match logical_time {
            None => coord_bail!("cannot call mz_logical_timestamp in this context"),
            Some(logical_time) => pack(Datum::from(Numeric::from(logical_time))),
        },
        UnmaterializableFunc::MzSessionId => pack(Datum::from(state.config().session_id)),
        UnmaterializableFunc::MzUptime => {
            let uptime = state.config().start_instant.elapsed();
            let uptime = chrono::Duration::from_std(uptime).map_or(Datum::Null, Datum::from);
            pack(uptime)
        }
        UnmaterializableFunc::MzVersion => {
            pack(Datum::from(&*state.config().build_info.human_version()))
        }
        UnmaterializableFunc::MzVersionNum => {
            pack(Datum::Int32(state.config().build_info.version_num()))
        }
        UnmaterializableFunc::PgBackendPid => pack(Datum::Int32(session.conn_id() as i32)),
        UnmaterializableFunc::PgPostmasterStartTime => pack(Datum::from(state.config().start_time)),
        UnmaterializableFunc::Version => {
            let build_info = state.config().build_info;
            let version = format!(
                "PostgreSQL {}.{} on {} (Materialize {})",
                SERVER_MAJOR_VERSION,
                SERVER_MINOR_VERSION,
                mz_build_info::TARGET_TRIPLE,
                build_info.version,
            );
            pack(Datum::from(&*version))
        }
    }
}
