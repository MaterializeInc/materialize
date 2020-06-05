// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Whole-dataflow optimization
//!
//! A dataflow may contain multiple views, each of which may only be
//! optimized locally. However, information like demand and predicate
//! pushdown can be applied across views once we understand the context
//! in which the views will be executed.

use dataflow_types::{DataflowDesc, LinearOperator};
use expr::Id;
use std::collections::{HashMap, HashSet};

/// Optimizes the implementation of each dataflow.
///
/// This method is currently limited in scope to propagating filtering and
/// projection information, though it could certainly generalize beyond.
pub fn optimize_dataflow(dataflow: &mut DataflowDesc) {
    optimize_dataflow_demand(dataflow);
    optimize_dataflow_filters(dataflow);
}

/// Pushes demand information from published outputs to dataflow inputs.
fn optimize_dataflow_demand(dataflow: &mut DataflowDesc) {

        let mut demand = HashMap::new();

        // Demand all columns of inputs to sinks.
        for (_id, sink) in dataflow.sink_exports.iter() {
            let input_id = sink.from.0;
            demand
                .entry(Id::Global(input_id))
                .or_insert_with(HashSet::new)
                .extend(0..dataflow.arity_of(&input_id));
        }

        // Demand all columns of inputs to exported indexes.
        for (_id, desc, _typ) in dataflow.index_exports.iter() {
            let input_id = desc.on_id;
            demand
                .entry(Id::Global(input_id))
                .or_insert_with(HashSet::new)
                .extend(0..dataflow.arity_of(&input_id));
        }

        // Propagate demand information from outputs to inputs.
        for build_desc in dataflow.objects_to_build.iter_mut().rev() {
            let transform = crate::demand::Demand;
            if let Some(columns) = demand.get(&Id::Global(build_desc.id)).clone() {
                transform.action(
                    build_desc.relation_expr.as_mut(),
                    columns.clone(),
                    &mut demand,
                );
            }
        }

        // Push demand information into the SourceDesc.
        for (source_id, source_desc) in dataflow.source_imports.iter_mut() {
            if let Some(columns) = demand.get(&Id::Global(source_id.sid)).clone() {
                // Install no-op demand information if none exists.
                if source_desc.operators.is_none() {
                    source_desc.operators = Some(LinearOperator {
                        predicates: Vec::new(),
                        projection: (0..source_desc.desc.typ().arity()).collect(),
                    })
                }
                // Restrict required columns by those identified as demanded.
                if let Some(operator) = &mut source_desc.operators {
                    operator.projection.retain(|col| columns.contains(col));
                }
            }
        }

}


/// Pushes predicate to dataflow inputs.
fn optimize_dataflow_filters(dataflow: &mut DataflowDesc) {

    // Contains id -> predicates map, describing those predicates that
    // can (but need not) be applied to the collection named by `id`.
    let mut predicates = HashMap::<Id, HashSet<expr::ScalarExpr>>::new();

    // Propagate predicate information from outputs to inputs.
    for build_desc in dataflow.objects_to_build.iter_mut().rev() {
        let transform = crate::predicate_pushdown::PredicatePushdown;
        if let Some(list) = predicates.get(&Id::Global(build_desc.id)).clone() {
            *build_desc.relation_expr.as_mut() = build_desc.relation_expr.as_mut().take_dangerous().filter(list.iter().cloned());
        }
        transform.action(
            build_desc.relation_expr.as_mut(),
            &mut predicates,
        )
    }

    // Push predicate information into the SourceDesc.
    for (source_id, source_desc) in dataflow.source_imports.iter_mut() {
        if let Some(list) = predicates.get(&Id::Global(source_id.sid)).clone() {
            // Install no-op predicate information if none exists.
            if source_desc.operators.is_none() {
                source_desc.operators = Some(LinearOperator {
                    predicates: Vec::new(),
                    projection: (0..source_desc.desc.typ().arity()).collect(),
                })
            }
            // Add any predicates that can be pushed to the source.
            if let Some(operator) = &mut source_desc.operators {
                operator.predicates.extend(list.iter().cloned());
            }
        }
    }

}