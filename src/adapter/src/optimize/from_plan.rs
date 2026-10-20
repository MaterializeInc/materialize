// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dataflows of materialized views defined by user-provided LIR plans, as in
//! `CREATE MATERIALIZED VIEW ... FROM PLAN '<json>'`.
//!
//! The plan supplies only the objects to build and how to read each import. The import
//! descriptions, their types and monotonicity, come from the catalog, through the same
//! [`DataflowBuilder`] functions that import relations into optimizer dataflows. The result is
//! validated before anything renders it.

use std::collections::BTreeMap;

use anyhow::anyhow;
use itertools::Itertools;
use mz_catalog::memory::objects::CatalogItem;
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::{BuildDesc, LirDataflowDescription};
use mz_compute_types::plan::envelope::{
    ImportSource, LirPlanEnvelope, parse_envelope, substitute_global_ids,
};
use mz_compute_types::plan::validate::{
    Rule, ValidateConfig, ValidationError, validate_dataflow_choosing_sink_forms,
};
use mz_ore::collections::CollectionExt;
use mz_repr::explain::trace_plan;
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{GlobalId, RelationDesc, ReprRelationType, ReprScalarType};
use mz_sql::plan::PlanError;
use mz_sql::plan::lir_plan::version_selector;
use mz_sql_parser::ast::{MaterializedViewBody, Statement};
use mz_transform::dataflow::DataflowMetainfo;

use crate::catalog::CatalogState;
use crate::error::AdapterError;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
use crate::optimize::materialized_view::{GlobalLirPlan, materialized_view_sink};

/// Builds and validates the dataflow of a materialized view defined by `envelope`.
///
/// Every import of `envelope` must be an [`ImportSource::Item`], as purification leaves it.
/// The dataflow exports the view's sink as `sink_id`, with schema `desc` and the columns in
/// `non_null_assertions` non-nullable, which is the schema the catalog records for the view.
/// The plan's output must have the arity of `desc`, and each of its columns the
/// [`ReprScalarType`] of the declared column.
///
/// Returns every validation error, one per line, as a [`PlanError::InvalidLirPlan`] if the
/// plan is invalid.
pub fn build_dataflow_from_plan(
    catalog: &CatalogState,
    cluster_id: ComputeInstanceId,
    envelope: &LirPlanEnvelope,
    sink_id: GlobalId,
    desc: &RelationDesc,
    non_null_assertions: Vec<usize>,
    refresh_schedule: Option<RefreshSchedule>,
    debug_name: String,
) -> Result<GlobalLirPlan, AdapterError> {
    let features = OptimizerFeatures::from(catalog.system_config())
        .override_from(&catalog.get_cluster(cluster_id).config.features())
        .override_from(&catalog.cluster_scoped_optimizer_overrides(cluster_id));
    // The snapshot only serves `DataflowBuilder::indexes_on`, which plans do not consult, since
    // they name the indexes they read.
    let builder = DataflowBuilder::new(
        catalog,
        ComputeInstanceSnapshot::new_without_collections(cluster_id),
    );

    let mut df = LirDataflowDescription::new(debug_name);
    df.refresh_schedule.clone_from(&refresh_schedule);

    let mut placeholders = BTreeMap::new();
    for import in &envelope.imports {
        let ImportSource::Item { item_id, version } = &import.source else {
            return Err(AdapterError::Internal(format!(
                "LIR plan import {} is not resolved to a catalog item",
                import.id
            )));
        };
        let entry = catalog.get_entry(item_id);
        let name = catalog.resolve_full_name(entry.name(), None);
        let selector = version_selector(entry, *version)?;
        let id = entry
            .item()
            .global_id_for_version(selector)
            .ok_or_else(|| invalid(format!("{name} has no version {version}")))?;
        let monotonic = builder.monotonic_object(id, &features);

        // NOTE: Planning rejects imports of replacements, whose collections queries do not read
        // either ("replacement materialized view ... cannot be depended upon").
        if import.persist {
            if !builder.import_persist_collection(&id, &mut df, monotonic) {
                return Err(invalid(format!("LIR plan cannot read {name} from persist")));
            }
        }

        for index in &import.indexes {
            let ImportSource::Item { item_id, .. } = index else {
                return Err(AdapterError::Internal(format!(
                    "LIR plan import {} has an index that is not resolved to a catalog item",
                    import.id
                )));
            };
            let index_entry = catalog.get_entry(item_id);
            let index_name = catalog.resolve_full_name(index_entry.name(), None);
            let CatalogItem::Index(index) = index_entry.item() else {
                return Err(invalid(format!("{index_name} is not an index")));
            };
            if index.on != id || index.cluster_id != cluster_id {
                return Err(invalid(format!(
                    "{index_name} is not an index on {name} on the cluster of the materialized view"
                )));
            }
            builder.import_index_into_dataflow(
                index.global_id(),
                id,
                index.keys.to_vec(),
                &mut df,
                monotonic,
            );
        }

        placeholders.insert(import.id, id);
    }

    for object in &envelope.objects {
        let mut plan = object.plan.clone();
        substitute_global_ids(&mut plan, &placeholders);
        df.objects_to_build.push(BuildDesc {
            id: object.id,
            plan,
        });
    }

    let mut typ = desc.typ().clone();
    for &c in &non_null_assertions {
        if let Some(column) = typ.column_types.get_mut(c) {
            column.nullable = false;
        }
    }
    let sink_desc = RelationDesc::new(typ, desc.iter_names().cloned());
    df.export_sink(
        sink_id,
        materialized_view_sink(
            envelope.output,
            sink_desc,
            non_null_assertions,
            refresh_schedule,
        ),
    );

    let config = ValidateConfig {
        enforce_function_allowlist: true,
    };
    let errors = match validate_dataflow_choosing_sink_forms(&mut df, &config) {
        Ok(types) => output_errors(types.get(&envelope.output), desc),
        Err(errors) => errors,
    };
    if !errors.is_empty() {
        let lines: Vec<_> = errors.iter().map(ToString::to_string).collect();
        return Err(invalid(lines.join("\n")));
    }

    // Traced where the optimizer traces its physical plans, which `EXPLAIN` reads.
    tracing::debug_span!(target: "optimizer", "optimize").in_scope(|| {
        tracing::debug_span!(target: "optimizer", "finalize_dataflow").in_scope(|| trace_plan(&df));
        trace_plan(&df);
    });

    Ok(GlobalLirPlan::from_parts(df, DataflowMetainfo::default()))
}

/// A user error for an invalid plan.
fn invalid(message: String) -> AdapterError {
    PlanError::InvalidLirPlan(message).into()
}

/// Checks the type of a plan's output against the declared schema `desc`.
fn output_errors(typ: Option<&ReprRelationType>, desc: &RelationDesc) -> Vec<ValidationError> {
    let error = |message| ValidationError {
        lir_id: None,
        operator: "dataflow",
        rule: Rule::Output,
        message,
    };
    let Some(typ) = typ else {
        return vec![error(
            "cannot infer the type of the plan's output".to_string(),
        )];
    };
    if typ.arity() != desc.arity() {
        return vec![error(format!(
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
                error(format!(
                    "column {name} has type {:?} in the plan, but {declared:?} in the declared schema",
                    inferred.scalar_type
                ))
            })
        })
        .collect()
}

/// The plan of a materialized view defined by `CREATE MATERIALIZED VIEW ... FROM PLAN`, read
/// from its `create_sql`, or `None` for a view defined by a query.
pub fn stored_envelope(create_sql: &str) -> Result<Option<LirPlanEnvelope>, AdapterError> {
    let stmt = mz_sql::parse::parse(create_sql)
        .map_err(|e| anyhow!("cannot parse stored statement: {e}"))?
        .into_element()
        .ast;
    let Statement::CreateMaterializedView(stmt) = stmt else {
        return Err(anyhow!("stored statement is not CREATE MATERIALIZED VIEW").into());
    };
    match stmt.body {
        MaterializedViewBody::Query(_) => Ok(None),
        MaterializedViewBody::Plan(json) => {
            Ok(Some(parse_envelope(&json).map_err(|e| anyhow!("{e}"))?))
        }
    }
}
