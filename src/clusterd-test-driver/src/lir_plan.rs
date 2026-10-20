// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dataflows of user-provided LIR plans, as `CREATE MATERIALIZED VIEW ... FROM PLAN` builds
//! them.
//!
//! The steps mirror `mz_adapter::optimize::from_plan::build_dataflow_from_plan`: the
//! envelope's imports are bound to collections by name, its objects are built with the
//! import placeholders substituted, the output is exported to a materialized-view sink, and
//! the dataflow is validated in user plan mode before anything renders it. The driver has no
//! catalog, so the caller binds each import name to a persist collection or an index the
//! replica already maintains.

use std::collections::BTreeMap;

use itertools::Itertools;
use mz_compute_types::dataflows::{
    BuildDesc, IndexDesc, LirDataflowDescription, RenderDataflowDescription,
};
use mz_compute_types::plan::envelope::{ImportSource, LirPlanEnvelope, substitute_global_ids};
use mz_compute_types::plan::validate::{
    Rule, ValidateConfig, ValidationError, validate_dataflow_choosing_sink_forms,
};
use mz_compute_types::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, MaterializedViewSinkConnection,
};
use mz_expr::MirScalarExpr;
use mz_repr::{
    GlobalId, RelationDesc, ReprRelationType, ReprScalarType, SqlRelationType, Timestamp,
};
use mz_storage_types::controller::CollectionMetadata;
use timely::progress::Antichain;

use crate::dataflow::{PersistSink, PersistSource, augment};

/// A relation an envelope imports, bound to the collection `id`.
#[derive(Clone, Debug)]
pub struct LirRelation {
    /// The collection the import placeholder stands for.
    pub id: GlobalId,
    /// The collection's type.
    pub typ: ReprRelationType,
    /// The persist shard backing the collection, required if the envelope reads it from
    /// persist.
    pub persist: Option<PersistSource>,
    /// Whether the collection is monotonic.
    pub monotonic: bool,
}

/// An index an envelope imports, which the replica maintains as `index_id`.
#[derive(Clone, Debug)]
pub struct LirIndex {
    /// The exported index.
    pub index_id: GlobalId,
    /// The collection the index arranges.
    pub on_id: GlobalId,
    /// The key columns.
    pub key: Vec<usize>,
}

/// The import bindings of an envelope, by the names the envelope uses.
#[derive(Clone, Debug, Default)]
pub struct LirCatalog {
    /// Relations by name.
    pub relations: BTreeMap<String, LirRelation>,
    /// Indexes by name.
    pub indexes: BTreeMap<String, LirIndex>,
}

/// A dataflow built from an [`LirPlanEnvelope`], not yet validated.
pub struct LirPlanDataflow {
    df: LirDataflowDescription,
    output: GlobalId,
    sources: BTreeMap<GlobalId, PersistSource>,
    sinks: BTreeMap<GlobalId, CollectionMetadata>,
}

impl LirPlanDataflow {
    /// Imports the relations and indexes `envelope` names from `catalog`, and builds its
    /// objects.
    ///
    /// Every import must be an [`ImportSource::Name`] that `catalog` binds. Fails if an import
    /// reads from persist a relation without a shard, or names an index on another relation.
    pub fn new(
        name: impl Into<String>,
        envelope: &LirPlanEnvelope,
        catalog: &LirCatalog,
    ) -> anyhow::Result<Self> {
        let mut df = LirDataflowDescription::new(name.into());
        let mut sources = BTreeMap::new();
        let mut placeholders = BTreeMap::new();
        for import in &envelope.imports {
            let ImportSource::Name(name) = &import.source else {
                anyhow::bail!("import {} is not named", import.id);
            };
            let relation = catalog
                .relations
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("no relation bound to {name}"))?;
            if import.persist {
                let source = relation
                    .persist
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("{name} has no persist shard"))?;
                df.import_source(relation.id, source.desc.typ().clone(), relation.monotonic);
                sources.insert(relation.id, source.clone());
            }
            for index in &import.indexes {
                let ImportSource::Name(index_name) = index else {
                    anyhow::bail!("an index of import {} is not named", import.id);
                };
                let index = catalog
                    .indexes
                    .get(index_name)
                    .ok_or_else(|| anyhow::anyhow!("no index bound to {index_name}"))?;
                anyhow::ensure!(
                    index.on_id == relation.id,
                    "{index_name} is not an index on {name}"
                );
                let key = index
                    .key
                    .iter()
                    .copied()
                    .map(MirScalarExpr::column)
                    .collect();
                df.import_index(
                    index.index_id,
                    IndexDesc {
                        on_id: relation.id,
                        key,
                    },
                    relation.typ.clone(),
                    relation.monotonic,
                );
            }
            placeholders.insert(import.id, relation.id);
        }
        for object in &envelope.objects {
            let mut plan = object.plan.clone();
            substitute_global_ids(&mut plan, &placeholders);
            df.objects_to_build.push(BuildDesc {
                id: object.id,
                plan,
            });
        }
        Ok(LirPlanDataflow {
            df,
            output: envelope.output,
            sources,
            sinks: BTreeMap::new(),
        })
    }

    /// Sets the dataflow's `as_of`.
    pub fn as_of(&mut self, t: Timestamp) -> &mut Self {
        self.df.set_as_of(Antichain::from_elem(t));
        self
    }

    /// Validates the dataflow without exports, and returns the type of the envelope's output.
    ///
    /// Stands in for the schema a `CREATE MATERIALIZED VIEW` statement declares.
    pub fn output_type(&self) -> Result<ReprRelationType, Vec<ValidationError>> {
        let mut df = self.df.clone();
        df.sink_exports.clear();
        df.index_exports.clear();
        let mut types = validate_dataflow_choosing_sink_forms(&mut df, &user_config())?;
        types
            .remove(&self.output)
            .ok_or_else(|| vec![output_error("cannot infer the type of the plan's output")])
    }

    /// Exports the envelope's output to a materialized-view sink `sink_id` writing `target`,
    /// with the declared schema `desc`.
    ///
    /// As for a `CREATE MATERIALIZED VIEW` statement, every non-nullable column of `desc` gets
    /// a non-null assertion.
    pub fn export_materialized_view(
        &mut self,
        sink_id: GlobalId,
        desc: RelationDesc,
        target: PersistSink,
    ) -> &mut Self {
        let non_null_assertions = desc
            .typ()
            .column_types
            .iter()
            .positions(|c| !c.nullable)
            .collect();
        self.sinks.insert(
            sink_id,
            CollectionMetadata {
                persist_location: target.location,
                data_shard: target.shard,
                relation_desc: desc.clone(),
                txns_shard: None,
            },
        );
        self.df.export_sink(
            sink_id,
            ComputeSinkDesc {
                from: self.output,
                from_desc: desc.clone(),
                connection: ComputeSinkConnection::MaterializedView(
                    MaterializedViewSinkConnection {
                        value_desc: desc,
                        storage_metadata: (),
                    },
                ),
                with_snapshot: true,
                up_to: Antichain::new(),
                non_null_assertions,
                refresh_schedule: None,
                from_arrangement: None,
            },
        );
        self
    }

    /// Validates the dataflow in user plan mode, chooses the sinks' arrangements, and checks
    /// the output against the declared schema of every materialized-view sink.
    ///
    /// Returns every violation. Never panics.
    pub fn validate(&mut self) -> Result<(), Vec<ValidationError>> {
        let types = validate_dataflow_choosing_sink_forms(&mut self.df, &user_config())?;
        let errors: Vec<_> = self
            .df
            .sink_exports
            .values()
            .flat_map(|sink| output_errors(types.get(&sink.from), &sink.from_desc))
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// The LIR dataflow, for inspection.
    pub fn lir(&self) -> &LirDataflowDescription {
        &self.df
    }

    /// Converts the dataflow into the form `CreateDataflow` carries.
    ///
    /// Call [`Self::validate`] first, rendering trusts the plan.
    pub fn finish(self) -> anyhow::Result<RenderDataflowDescription<CollectionMetadata>> {
        augment(self.df, &self.sources, &self.sinks)
    }
}

/// A schema of type `typ` with columns named `c0`, `c1`, ..., as a statement would declare
/// for a plan whose output has that type.
pub fn declared_desc(typ: &ReprRelationType) -> RelationDesc {
    let names = (0..typ.arity()).map(|i| format!("c{i}"));
    RelationDesc::new(SqlRelationType::from_repr(typ), names)
}

/// The validator configuration for user plans.
fn user_config() -> ValidateConfig {
    ValidateConfig { user_plan: true }
}

fn output_error(message: impl Into<String>) -> ValidationError {
    ValidationError {
        lir_id: None,
        operator: "dataflow",
        rule: Rule::Output,
        message: message.into(),
    }
}

/// Checks the output type `typ` against the declared schema `desc`, as the adapter does.
fn output_errors(typ: Option<&ReprRelationType>, desc: &RelationDesc) -> Vec<ValidationError> {
    let Some(typ) = typ else {
        return vec![output_error("cannot infer the type of the plan's output")];
    };
    if typ.arity() != desc.arity() {
        return vec![output_error(format!(
            "the plan's output has {} columns, but the declared schema has {}",
            typ.arity(),
            desc.arity()
        ))];
    }
    desc.iter()
        .zip_eq(typ.column_types.iter())
        .filter_map(|((name, declared), inferred)| {
            let declared = ReprScalarType::from(&declared.scalar_type);
            (declared != inferred.scalar_type).then(|| {
                output_error(format!(
                    "column {name} has type {:?} in the plan, but {declared:?} in the declared \
                     schema",
                    inferred.scalar_type
                ))
            })
        })
        .collect()
}
