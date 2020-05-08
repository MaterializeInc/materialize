// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations for relation expressions.
//!
//! This crate contains traits, types, and methods suitable for transforming
//! `RelationExpr` types in ways that preserve semantics and improve performance.
//! The core trait is `Transform`, and many implementors of this trait can be
//! boxed and iterated over. Some common transformation patterns are wrapped
//! as `Transform` implementors themselves.
//!
//! The crate also contains the beginnings of whole-dataflow optimization,
//! which uses the same analyses but spanning multiple dataflow elements.

#![forbid(missing_docs)]
#![deny(missing_debug_implementations)]

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use expr::{EvalError, GlobalId, IdGen, OptimizedRelationExpr, RelationExpr, ScalarExpr};

// pub mod binding;
pub mod column_knowledge;
pub mod demand;
pub mod empty_map;
pub mod filter_lets;
pub mod fusion;
pub mod inline_let;
pub mod join_elision;
pub mod join_implementation;
pub mod map_lifting;
pub mod nonnull_requirements;
pub mod nonnullable;
pub mod predicate_pushdown;
pub mod projection_extraction;
pub mod projection_lifting;
pub mod reduce_elision;
pub mod reduction;
pub mod reduction_pushdown;
pub mod redundant_join;
pub mod split_predicates;
pub mod topk_elision;
pub mod update_let;
// pub mod use_indexes;

// TODO(justin): this kind of state should be "global" to the entire planning of a given query, so
// that we can thread through things like unique ID generation.
/// State that gets threaded through all transforms.
#[derive(Debug)]
pub struct TransformArgs<'a> {
    /// A shared instance of IdGen to allow constructing new Let expressions.
    pub id_gen: &'a mut IdGen,
    /// The indexes accessible.
    pub indexes: &'a HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
}

/// Types capable of transforming relation expressions.
pub trait Transform: std::fmt::Debug {
    /// Transform a relation into a functionally equivalent relation.
    fn transform(
        &self,
        relation: &mut RelationExpr,
        state: TransformArgs,
    ) -> Result<(), TransformError>;
}

/// Errors that can occur during a transformation.
#[derive(Debug, Clone)]
pub enum TransformError {
    /// An error resulting from evaluation of a `ScalarExpr`.
    Eval(EvalError),
    /// An unstructured error.
    Internal(String),
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TransformError::Eval(e) => write!(f, "{}", e),
            TransformError::Internal(msg) => write!(f, "internal transform error: {}", msg),
        }
    }
}

impl Error for TransformError {}

impl From<EvalError> for TransformError {
    fn from(e: EvalError) -> TransformError {
        TransformError::Eval(e)
    }
}

/// A sequence of transformations iterated some number of times.
#[derive(Debug)]
pub struct Fixpoint {
    transforms: Vec<Box<dyn crate::Transform + Send>>,
    limit: usize,
}

impl Transform for Fixpoint {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        state: TransformArgs,
    ) -> Result<(), TransformError> {
        for _ in 0..self.limit {
            let original = relation.clone();
            for transform in self.transforms.iter() {
                transform.transform(
                    relation,
                    TransformArgs {
                        id_gen: state.id_gen,
                        indexes: state.indexes,
                    },
                )?;
            }
            if *relation == original {
                return Ok(());
            }
        }
        let original = relation.clone();
        for transform in self.transforms.iter() {
            transform.transform(
                relation,
                TransformArgs {
                    id_gen: state.id_gen,
                    indexes: state.indexes,
                },
            )?;
        }
        Err(TransformError::Internal(format!(
            "fixpoint looped too many times {:#?} {}\n{}",
            self,
            original.pretty(),
            relation.pretty()
        )))
    }
}

/// A naive optimizer for relation expressions.
///
/// The optimizer currently applies only peep-hole optimizations, from a limited
/// set that were sufficient to get some of TPC-H up and working. It is worth a
/// review at some point to improve the quality, coverage, and architecture of
/// the optimizations.
#[derive(Debug)]
pub struct Optimizer {
    transforms: Vec<Box<dyn crate::Transform + Send>>,
}

impl Optimizer {
    /// Optimizes the supplied relation expression.
    fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) -> Result<(), TransformError> {
        let mut id_gen = Default::default();
        for transform in self.transforms.iter() {
            transform.transform(
                relation,
                TransformArgs {
                    id_gen: &mut id_gen,
                    indexes,
                },
            )?;
        }
        Ok(())
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        let transforms: Vec<Box<dyn crate::Transform + Send>> = vec![
            // The first block are peep-hole optimizations that simplify
            // the representation of the query and are largely uncontentious.
            Box::new(crate::join_elision::JoinElision),
            Box::new(crate::inline_let::InlineLet),
            Box::new(crate::reduction::FoldConstants),
            Box::new(crate::split_predicates::SplitPredicates),
            Box::new(crate::fusion::filter::Filter),
            Box::new(crate::fusion::map::Map),
            Box::new(crate::projection_extraction::ProjectionExtraction),
            Box::new(crate::fusion::project::Project),
            Box::new(crate::fusion::join::Join),
            Box::new(crate::join_elision::JoinElision),
            Box::new(crate::empty_map::EmptyMap),
            // Unbinding risks exponential blow-up on queries with deep let bindings.
            // Box::new(crate::binding::Unbind),
            // Box::new(crate::binding::Deduplicate),
            // Early actions include "no-brainer" transformations that reduce complexity in linear passes.
            Box::new(crate::join_elision::JoinElision),
            Box::new(crate::reduction::FoldConstants),
            Box::new(crate::fusion::filter::Filter),
            Box::new(crate::fusion::map::Map),
            Box::new(crate::reduction::FoldConstants),
            Box::new(crate::reduction::NegatePredicate),
            Box::new(crate::reduction::DeMorgans),
            Box::new(crate::reduction::UndistributeAnd),
            Box::new(crate::split_predicates::SplitPredicates),
            Box::new(crate::Fixpoint {
                limit: 100,
                transforms: vec![
                    Box::new(crate::nonnullable::NonNullable),
                    Box::new(crate::reduction::FoldConstants),
                    Box::new(crate::predicate_pushdown::PredicatePushdown),
                    Box::new(crate::fusion::join::Join),
                    Box::new(crate::fusion::filter::Filter),
                    Box::new(crate::fusion::project::Project),
                    Box::new(crate::fusion::map::Map),
                    Box::new(crate::empty_map::EmptyMap),
                    Box::new(crate::join_elision::JoinElision),
                    Box::new(crate::reduce_elision::ReduceElision),
                    Box::new(crate::inline_let::InlineLet),
                    Box::new(crate::update_let::UpdateLet),
                    Box::new(crate::projection_extraction::ProjectionExtraction),
                    Box::new(crate::projection_lifting::ProjectionLifting),
                    Box::new(crate::map_lifting::LiteralLifting),
                    Box::new(crate::filter_lets::FilterLets),
                    Box::new(crate::nonnull_requirements::NonNullRequirements),
                    Box::new(crate::column_knowledge::ColumnKnowledge),
                    Box::new(crate::reduction_pushdown::ReductionPushdown),
                    Box::new(crate::redundant_join::RedundantJoin),
                    Box::new(crate::topk_elision::TopKElision),
                ],
            }),
            // As a final logical action, convert any constant expression to a constant.
            // Some optimizations fight against this, and we want to be sure to end as a
            // `RelationExpr::Constant` if that is the case, so that subsequent use can
            // clearly see this.
            Box::new(crate::reduction::FoldConstants),
            // TODO (wangandi): materialize#616 the FilterEqualLiteral transform
            // exists but is currently unevaluated with the new join implementations.

            // Implementation transformations
            Box::new(crate::Fixpoint {
                limit: 100,
                transforms: vec![
                    Box::new(crate::projection_lifting::ProjectionLifting),
                    Box::new(crate::join_implementation::JoinImplementation),
                    Box::new(crate::fusion::filter::Filter),
                    Box::new(crate::demand::Demand),
                ],
            }),
            Box::new(crate::fusion::project::Project),
            Box::new(crate::reduction_pushdown::ReductionPushdown),
        ];
        Self { transforms }
    }
}

impl Optimizer {
    /// Optimizes the supplied relation expression.
    pub fn optimize(
        &mut self,
        mut relation: RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) -> Result<OptimizedRelationExpr, TransformError> {
        self.transform(&mut relation, &indexes)?;
        Ok(expr::OptimizedRelationExpr(relation))
    }

    /// Simple fusion and elision transformations to render the query readable.
    pub fn pre_optimization() -> Self {
        let transforms: Vec<Box<dyn crate::Transform + Send>> = vec![
            Box::new(crate::join_elision::JoinElision),
            Box::new(crate::inline_let::InlineLet),
            Box::new(crate::reduction::FoldConstants),
            Box::new(crate::split_predicates::SplitPredicates),
            Box::new(crate::fusion::filter::Filter),
            Box::new(crate::fusion::map::Map),
            Box::new(crate::projection_extraction::ProjectionExtraction),
            Box::new(crate::fusion::project::Project),
            Box::new(crate::fusion::join::Join),
            Box::new(crate::join_elision::JoinElision),
            Box::new(crate::empty_map::EmptyMap),
        ];
        Self { transforms }
    }
}

use dataflow_types::{DataflowDesc, LinearOperator};
use expr::Id;
use std::collections::HashSet;

/// Optimizes the implementation of each dataflow.
pub fn optimize_dataflow(dataflow: &mut DataflowDesc) {
    // This method is currently limited in scope to propagating filtering and
    // projection information, though it could certainly generalize beyond.

    // 1. Propagate demand information from outputs back to sources.
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
