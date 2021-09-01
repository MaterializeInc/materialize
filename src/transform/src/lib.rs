// Copyright Materialize, Inc. and contributors. All rights reserved.
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
//! `MirRelationExpr` types in ways that preserve semantics and improve performance.
//! The core trait is `Transform`, and many implementors of this trait can be
//! boxed and iterated over. Some common transformation patterns are wrapped
//! as `Transform` implementors themselves.
//!
//! The crate also contains the beginnings of whole-dataflow optimization,
//! which uses the same analyses but spanning multiple dataflow elements.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use expr::MirRelationExpr;
use expr::MirScalarExpr;
use expr::{GlobalId, IdGen};

pub mod column_knowledge;
pub mod cse;
pub mod demand;
pub mod fusion;
pub mod inline_let;
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
pub mod topk_elision;
pub mod union_cancel;
pub mod update_let;

pub mod dataflow;
pub use dataflow::optimize_dataflow;

/// Arguments that get threaded through all transforms.
#[derive(Debug)]
pub struct TransformArgs<'a> {
    /// A shared instance of IdGen to allow constructing new Let expressions.
    pub id_gen: &'a mut IdGen,
    /// The indexes accessible.
    pub indexes: &'a HashMap<GlobalId, Vec<(GlobalId, Vec<MirScalarExpr>)>>,
}

/// Types capable of transforming relation expressions.
pub trait Transform: std::fmt::Debug {
    /// Transform a relation into a functionally equivalent relation.
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), TransformError>;
    /// A string describing the transform.
    ///
    /// This is useful mainly when iterating through many `Box<Tranform>`
    /// and one wants to judge progress before some defect occurs.
    fn debug(&self) -> String {
        format!("{:?}", self)
    }
}

/// Errors that can occur during a transformation.
#[derive(Debug, Clone)]
pub enum TransformError {
    /// An unstructured error.
    Internal(String),
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TransformError::Internal(msg) => write!(f, "internal transform error: {}", msg),
        }
    }
}

impl Error for TransformError {}

/// Tree-structure containing the type of information of a sub-tree within a `MirRelationExpr`
/// graph.
#[derive(Debug)]
pub struct TypeInfo {
    typ: repr::RelationType,
    inputs: InputTypeInfo,
}

/// Type information of the input expressions
#[derive(Debug)]
pub struct InputTypeInfo {
    input_types: Vec<Box<TypeInfo>>,
}

impl InputTypeInfo {
    fn new() -> Self {
        Self {
            input_types: Vec::new(),
        }
    }

    fn push(&mut self, input: TypeInfo) {
        self.input_types.push(Box::new(input));
    }

    fn take_first(&mut self) {
        let first = self.input_types.drain(0..1).next().unwrap();
        *self = first.inputs;
    }

    fn prepare_to_prepend(&mut self, relation: &MirRelationExpr) {
        let typ = relation.typ();
        let mut inputs = Self::new();
        std::mem::swap(self, &mut inputs);
        let single_input = TypeInfo { typ, inputs };
        self.input_types.push(Box::new(single_input));
    }

    fn first<'a>(&'a self) -> &'a TypeInfo {
        &*self.input_types.first().as_ref().unwrap()
    }
}

/// Transformations that can be applied in post-order.
pub trait LocalTransform: std::fmt::Debug {
    /// doc me
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        inputs: &mut InputTypeInfo,
    ) -> Result<(), TransformError>;

    /// A string describing the transform.
    fn debug(&self) -> String {
        format!("{:?}", self)
    }
}

/// A group of post-order transformations applied in a single traversal of the graph.
#[derive(Debug)]
pub struct PostOrderTransforms {
    transforms: Vec<Box<dyn LocalTransform + Send>>,
}

impl PostOrderTransforms {
    fn new(transforms: Vec<Box<dyn LocalTransform + Send>>) -> Self {
        Self { transforms }
    }
}

impl Transform for PostOrderTransforms {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        let mut visitor = PostOrderTransformsVisitor::new(&self.transforms);
        // note: MirRelationExpr::try_visit_mut now takes an instance of MirRelationExprMutVisitor
        relation.try_mut_visitor(&mut visitor)?;
        Ok(())
    }
}

macro_rules! local_transforms {
    ( $( $x:expr ),* ) => {
        Box::new(PostOrderTransforms::new(vec![$(
            Box::new($x),
        )*]))
    };
}

/// Visitor type that applies a given list of transforms in post-order.
struct PostOrderTransformsVisitor<'a> {
    transforms: &'a Vec<Box<dyn LocalTransform + Send>>,
    stack: Vec<InputTypeInfo>,
}

impl<'a> PostOrderTransformsVisitor<'a> {
    fn new(transforms: &'a Vec<Box<dyn LocalTransform + Send>>) -> Self {
        Self {
            transforms,
            stack: vec![InputTypeInfo::new()],
        }
    }
}

impl<'a> expr::MirRelationExprMutVisitor<TransformError> for PostOrderTransformsVisitor<'a> {
    fn pre(&mut self, _relation: &mut MirRelationExpr) -> Result<(), TransformError> {
        self.stack.push(InputTypeInfo::new());
        Ok(())
    }
    fn post(&mut self, relation: &mut MirRelationExpr) -> Result<(), TransformError> {
        let mut inputs = self.stack.pop().unwrap();
        for transform in self.transforms.iter() {
            // the transform is responsible for updating `input_types` if the layout
            // of the current relation changes
            transform.transform(relation, &mut inputs)?;
        }
        // note: MirRelationExpr::typ now takes the type of its inputs as a parameter
        // let typ = relation.typ(input_types.iter().map(|e| e.typ));
        let typ = relation.typ();
        self.stack
            .last_mut()
            .unwrap()
            .push(TypeInfo { typ, inputs });
        Ok(())
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
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), TransformError> {
        for _ in 0..self.limit {
            let original = relation.clone();
            for transform in self.transforms.iter() {
                transform.transform(
                    relation,
                    TransformArgs {
                        id_gen: args.id_gen,
                        indexes: args.indexes,
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
                    id_gen: args.id_gen,
                    indexes: args.indexes,
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

/// A sequence of transformations that simplify the `MirRelationExpr`
#[derive(Debug)]
pub struct FuseAndCollapse {
    transforms: Vec<Box<dyn crate::Transform + Send>>,
}

impl Default for FuseAndCollapse {
    fn default() -> Self {
        Self {
            // TODO: The relative orders of the transforms have not been
            // determined except where there are comments.
            // TODO (#6542): All the transforms here except for
            // `ProjectionLifting`, `InlineLet`, `UpdateLet`, and
            // `RedundantJoin` can be implemented as free functions. Note that
            // (#716) proposes the removal of `InlineLet` and `UpdateLet` as a
            // transforms.
            transforms: vec![
                local_transforms!(crate::projection_extraction::ProjectionExtraction),
                Box::new(crate::projection_lifting::ProjectionLifting),
                local_transforms!(crate::fusion::map::Map, crate::fusion::filter::Filter),
                Box::new(crate::fusion::negate::Negate),
                Box::new(crate::fusion::project::Project),
                local_transforms!(crate::fusion::join::Join),
                Box::new(crate::inline_let::InlineLet),
                Box::new(crate::fusion::reduce::Reduce),
                Box::new(crate::fusion::union::Union),
                // This goes after union fusion so we can cancel out
                // more branches at a time.
                Box::new(crate::union_cancel::UnionBranchCancellation),
                // This should run before redundant join to ensure that key info
                // is correct.
                Box::new(crate::update_let::UpdateLet),
                // Removes redundant inputs from joins.
                // Note that this eliminates one redundant input per join,
                // so it is necessary to run this section in a loop.
                // TODO: (#6748) Predicate pushdown unlocks the ability to
                // remove some redundant joins but also prevents other
                // redundant joins from being removed. When predicate pushdown
                // no longer works against redundant join, check if it is still
                // necessary to run RedundantJoin here.
                Box::new(crate::redundant_join::RedundantJoin),
                // As a final logical action, convert any constant expression to a constant.
                // Some optimizations fight against this, and we want to be sure to end as a
                // `MirRelationExpr::Constant` if that is the case, so that subsequent use can
                // clearly see this.
                Box::new(crate::reduction::FoldConstants { limit: Some(10000) }),
            ],
        }
    }
}

impl Transform for FuseAndCollapse {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), TransformError> {
        for transform in self.transforms.iter() {
            transform.transform(
                relation,
                TransformArgs {
                    id_gen: args.id_gen,
                    indexes: args.indexes,
                },
            )?;
        }
        Ok(())
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
    /// The list of transforms to apply to an input relation.
    pub transforms: Vec<Box<dyn crate::Transform + Send>>,
}

impl Optimizer {
    /// Builds a logical optimizer that only performs logical transformations.
    pub fn for_view() -> Self {
        let transforms: Vec<Box<dyn crate::Transform + Send>> = vec![
            // 1. Structure-agnostic cleanup
            Box::new(crate::topk_elision::TopKElision),
            Box::new(crate::nonnull_requirements::NonNullRequirements),
            // 2. Collapse constants, joins, unions, and lets as much as possible.
            // TODO: lift filters/maps to maximize ability to collapse
            // things down?
            Box::new(crate::Fixpoint {
                limit: 100,
                transforms: vec![Box::new(crate::FuseAndCollapse::default())],
            }),
            // 3. Move predicate information up and down the tree.
            //    This also fixes the shape of joins in the plan.
            Box::new(crate::Fixpoint {
                limit: 100,
                transforms: vec![
                    // Predicate pushdown sets the equivalence classes of joins.
                    Box::new(crate::predicate_pushdown::PredicatePushdown),
                    // Lifts the information `!isnull(col)`
                    Box::new(crate::nonnullable::NonNullable),
                    // Lifts the information `col = literal`
                    // TODO (#6613): this also tries to lift `!isnull(col)` but
                    // less well than the previous transform. Eliminate
                    // redundancy between the two transforms.
                    Box::new(crate::column_knowledge::ColumnKnowledge),
                    // Lifts the information `col1 = col2`
                    Box::new(crate::demand::Demand),
                    Box::new(crate::FuseAndCollapse::default()),
                ],
            }),
            // 4. Reduce/Join simplifications.
            Box::new(crate::Fixpoint {
                limit: 100,
                transforms: vec![
                    // Pushes aggregations down
                    Box::new(crate::reduction_pushdown::ReductionPushdown),
                    // Replaces reduces with maps when the group keys are
                    // unique with maps
                    Box::new(crate::reduce_elision::ReduceElision),
                    // Converts `Cross Join {Constant(Literal) + Input}` to
                    // `Map {Cross Join (Input, Constant()), Literal}`.
                    // Join fusion will clean this up to `Map{Input, Literal}`
                    Box::new(crate::map_lifting::LiteralLifting),
                    // Identifies common relation subexpressions.
                    // Must be followed by let inlining, to keep under control.
                    Box::new(crate::cse::relation_cse::RelationCSE),
                    Box::new(crate::inline_let::InlineLet),
                    Box::new(crate::update_let::UpdateLet),
                    Box::new(crate::FuseAndCollapse::default()),
                ],
            }),
        ];
        Self { transforms }
    }

    /// Builds a physical optimizer.
    ///
    /// Performs logical transformations followed by all physical ones.
    /// This is meant to be used for optimizing each view within a dataflow
    /// once view inlining has already happened, right before dataflow
    /// rendering.
    pub fn for_dataflow() -> Self {
        // Implementation transformations
        let transforms: Vec<Box<dyn crate::Transform + Send>> = vec![
            Box::new(crate::Fixpoint {
                limit: 100,
                transforms: vec![
                    Box::new(crate::projection_lifting::ProjectionLifting),
                    Box::new(crate::join_implementation::JoinImplementation),
                    Box::new(crate::column_knowledge::ColumnKnowledge),
                    Box::new(crate::reduction::FoldConstants { limit: Some(10000) }),
                    local_transforms!(crate::fusion::filter::Filter),
                    // fill in the new demand after maps have been shifted
                    // around.
                    Box::new(crate::demand::Demand),
                    Box::new(crate::map_lifting::LiteralLifting),
                    local_transforms!(crate::fusion::map::Map),
                ],
            }),
            Box::new(crate::reduction_pushdown::ReductionPushdown),
            local_transforms!(crate::cse::map::Map),
            Box::new(crate::projection_lifting::ProjectionLifting),
            Box::new(crate::join_implementation::JoinImplementation),
            Box::new(crate::fusion::project::Project),
            // Identifies common relation subexpressions.
            // Must be followed by let inlining, to keep under control.
            Box::new(crate::cse::relation_cse::RelationCSE),
            Box::new(crate::inline_let::InlineLet),
            Box::new(crate::update_let::UpdateLet),
            Box::new(crate::reduction::FoldConstants { limit: Some(10000) }),
        ];
        let mut optimizer = Self::for_view();
        optimizer.transforms.extend(transforms);
        optimizer
    }

    /// Simple fusion and elision transformations to render the query readable.
    pub fn pre_optimization() -> Self {
        let transforms: Vec<Box<dyn crate::Transform + Send>> = vec![
            local_transforms!(crate::fusion::join::Join),
            Box::new(crate::inline_let::InlineLet),
            Box::new(crate::reduction::FoldConstants { limit: Some(10000) }),
            local_transforms!(crate::fusion::filter::Filter, crate::fusion::map::Map),
            Box::new(crate::fusion::negate::Negate),
            local_transforms!(crate::projection_extraction::ProjectionExtraction),
            Box::new(crate::fusion::project::Project),
            local_transforms!(crate::fusion::join::Join),
        ];
        Self { transforms }
    }

    /// Optimizes the supplied relation expression returning an optimized relation.
    pub fn optimize(
        &mut self,
        mut relation: MirRelationExpr,
        indexes: &HashMap<GlobalId, Vec<(GlobalId, Vec<MirScalarExpr>)>>,
    ) -> Result<expr::OptimizedMirRelationExpr, TransformError> {
        self.transform(&mut relation, indexes)?;
        Ok(expr::OptimizedMirRelationExpr(relation))
    }

    /// Optimizes the supplied relation expression in place.
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        indexes: &HashMap<GlobalId, Vec<(GlobalId, Vec<MirScalarExpr>)>>,
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
