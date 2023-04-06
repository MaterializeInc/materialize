// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::collapsible_if)]
#![warn(clippy::collapsible_else_if)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

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

use std::error::Error;
use std::fmt;
use std::iter;
use tracing::error;

use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_ore::id_gen::IdGen;
use mz_repr::GlobalId;

pub mod attribute;
pub mod canonicalization;
pub mod canonicalize_mfp;
pub mod column_knowledge;
pub mod compound;
pub mod cse;
pub mod demand;
pub mod fold_constants;
pub mod fusion;
pub mod join_implementation;
pub mod literal_constraints;
pub mod literal_lifting;
pub mod monotonic;
pub mod movement;
pub mod nonnull_requirements;
pub mod nonnullable;
pub mod normalize_lets;
pub mod normalize_ops;
pub mod ordering;
pub mod predicate_pushdown;
pub mod reduce_elision;
pub mod reduction_pushdown;
pub mod redundant_join;
pub mod semijoin_idempotence;
pub mod threshold_elision;
pub mod union_cancel;

pub mod dataflow;
pub use dataflow::optimize_dataflow;
use mz_ore::stack::RecursionLimitError;

#[macro_use]
extern crate num_derive;

/// Compute the conjunction of a variadic number of expressions.
#[macro_export]
macro_rules! all {
    ($x:expr) => ($x);
    ($($x:expr,)+) => ( $($x)&&+ )
}

/// Compute the disjunction of a variadic number of expressions.
#[macro_export]
macro_rules! any {
    ($x:expr) => ($x);
    ($($x:expr,)+) => ( $($x)||+ )
}

/// Arguments that get threaded through all transforms.
#[derive(Debug)]
pub struct TransformArgs<'a> {
    /// The indexes accessible.
    pub indexes: &'a dyn IndexOracle,
}

/// Types capable of transforming relation expressions.
pub trait Transform: std::fmt::Debug {
    /// Indicates if the transform can be safely applied to expressions containing
    /// `LetRec` AST nodes.
    fn recursion_safe(&self) -> bool {
        false
    }

    /// Transform a relation into a functionally equivalent relation.
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), TransformError>;
    /// A string describing the transform.
    ///
    /// This is useful mainly when iterating through many `Box<Transform>`
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
    /// An ideally temporary error indicating transforms that do not support the `MirRelationExpr::LetRec` variant.
    LetRecUnsupported,
    /// A reference to an apparently unbound identifier.
    IdentifierMissing(mz_expr::LocalId),
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TransformError::Internal(msg) => write!(f, "internal transform error: {}", msg),
            TransformError::LetRecUnsupported => write!(f, "LetRec AST node not supported"),
            TransformError::IdentifierMissing(i) => {
                write!(f, "apparently unbound identifier: {:?}", i)
            }
        }
    }
}

impl Error for TransformError {}

impl From<RecursionLimitError> for TransformError {
    fn from(error: RecursionLimitError) -> Self {
        TransformError::Internal(error.to_string())
    }
}

/// A trait for a type that can answer questions about what indexes exist.
pub trait IndexOracle: fmt::Debug {
    /// Returns an iterator over the indexes that exist on the identified
    /// collection.
    ///
    /// Each index is described by the list of key expressions. If no indexes
    /// exist for the identified collection, or if the identified collection
    /// is unknown, the returned iterator will be empty.
    ///
    // NOTE(benesch): The allocation here is unfortunate, but on the other hand
    // you need only allocate when you actually look for an index. Can we do
    // better somehow? Making the entire optimizer generic over this iterator
    // type doesn't presently seem worthwhile.
    fn indexes_on(&self, id: GlobalId) -> Box<dyn Iterator<Item = &[MirScalarExpr]> + '_>;
}

/// An [`IndexOracle`] that knows about no indexes.
#[derive(Debug)]
pub struct EmptyIndexOracle;

impl IndexOracle for EmptyIndexOracle {
    fn indexes_on(&self, _: GlobalId) -> Box<dyn Iterator<Item = &[MirScalarExpr]> + '_> {
        Box::new(iter::empty())
    }
}

/// A sequence of transformations iterated some number of times.
#[derive(Debug)]
pub struct Fixpoint {
    name: &'static str,
    transforms: Vec<Box<dyn crate::Transform>>,
    limit: usize,
}

impl Transform for Fixpoint {
    fn recursion_safe(&self) -> bool {
        true
    }

    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = self.name)
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), TransformError> {
        let recursive = relation.has_let_rec();

        // The number of iterations for a relation to settle depends on the
        // number of nodes in the relation. Instead of picking an arbitrary
        // hard limit on the number of iterations, we use a soft limit and
        // check whether the relation has become simpler after reaching it.
        // If so, we perform another pass of transforms. Otherwise, there is
        // a bug somewhere that prevents the relation from settling on a
        // stable shape.
        loop {
            let mut original_count = 0;
            relation.try_visit_post::<_, TransformError>(&mut |_| Ok(original_count += 1))?;
            for i in 0..self.limit {
                let original = relation.clone();

                let span = tracing::span!(
                    tracing::Level::TRACE,
                    "iteration",
                    target = "optimizer",
                    path.segment = format!("{:04}", i)
                );
                span.in_scope(|| -> Result<(), TransformError> {
                    for transform in self.transforms.iter() {
                        if transform.recursion_safe() || !recursive {
                            transform.transform(
                                relation,
                                TransformArgs {
                                    indexes: args.indexes,
                                },
                            )?;
                        }
                    }
                    mz_repr::explain::trace_plan(relation);
                    Ok(())
                })?;

                if *relation == original {
                    mz_repr::explain::trace_plan(relation);
                    return Ok(());
                }
            }
            let mut final_count = 0;
            relation.try_visit_post::<_, TransformError>(&mut |_| Ok(final_count += 1))?;
            if final_count >= original_count {
                break;
            }
        }
        for transform in self.transforms.iter() {
            if transform.recursion_safe() || !recursive {
                transform.transform(
                    relation,
                    TransformArgs {
                        indexes: args.indexes,
                    },
                )?;
            }
        }
        Err(TransformError::Internal(format!(
            "fixpoint looped too many times {:#?}; transformed relation: {}",
            self,
            relation.pretty()
        )))
    }
}

/// A sequence of transformations that simplify the `MirRelationExpr`
#[derive(Debug)]
pub struct FuseAndCollapse {
    transforms: Vec<Box<dyn crate::Transform>>,
}

impl Default for FuseAndCollapse {
    fn default() -> Self {
        Self {
            // TODO: The relative orders of the transforms have not been
            // determined except where there are comments.
            // TODO (#6542): All the transforms here except for `ProjectionLifting`
            //  and `RedundantJoin` can be implemented as free functions.
            transforms: vec![
                Box::new(crate::canonicalization::ProjectionExtraction),
                Box::new(crate::movement::ProjectionLifting::default()),
                Box::new(crate::fusion::Fusion),
                Box::new(crate::canonicalization::FlatMapToMap),
                Box::new(crate::fusion::join::Join),
                Box::new(crate::normalize_lets::NormalizeLets::new(false)),
                Box::new(crate::fusion::reduce::Reduce),
                Box::new(crate::compound::UnionNegateFusion),
                // This goes after union fusion so we can cancel out
                // more branches at a time.
                Box::new(crate::union_cancel::UnionBranchCancellation),
                // This should run before redundant join to ensure that key info
                // is correct.
                Box::new(crate::normalize_lets::NormalizeLets::new(false)),
                // Removes redundant inputs from joins.
                // Note that this eliminates one redundant input per join,
                // so it is necessary to run this section in a loop.
                // TODO: (#6748) Predicate pushdown unlocks the ability to
                // remove some redundant joins but also prevents other
                // redundant joins from being removed. When predicate pushdown
                // no longer works against redundant join, check if it is still
                // necessary to run RedundantJoin here.
                Box::new(crate::redundant_join::RedundantJoin::default()),
                // As a final logical action, convert any constant expression to a constant.
                // Some optimizations fight against this, and we want to be sure to end as a
                // `MirRelationExpr::Constant` if that is the case, so that subsequent use can
                // clearly see this.
                Box::new(crate::fold_constants::FoldConstants { limit: Some(10000) }),
            ],
        }
    }
}

impl Transform for FuseAndCollapse {
    fn recursion_safe(&self) -> bool {
        true
    }

    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "fuse_and_collapse")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), TransformError> {
        let recursive = relation.has_let_rec();

        for transform in self.transforms.iter() {
            if transform.recursion_safe() || !recursive {
                transform.transform(
                    relation,
                    TransformArgs {
                        indexes: args.indexes,
                    },
                )?;
            }
        }
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

/// Construct a normalizing transform that runs transforms that normalize the
/// structure of the tree until a fixpoint.
///
/// Care needs to be taken to ensure that the fixpoint converges for every
/// possible input tree. If this is not the case, there are two possibilities:
/// 1. The rewrite loop runs enters an oscillating cycle.
/// 2. The expression grows without bound.
pub fn normalize() -> crate::Fixpoint {
    crate::Fixpoint {
        name: "normalize",
        limit: 100,
        transforms: vec![
            Box::new(crate::normalize_lets::NormalizeLets::new(false)),
            Box::new(crate::normalize_ops::NormalizeOps),
        ],
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
    /// A logical name identifying this optimizer instance.
    pub name: &'static str,
    /// The list of transforms to apply to an input relation.
    pub transforms: Vec<Box<dyn crate::Transform>>,
}

impl Optimizer {
    /// Builds a logical optimizer that only performs logical transformations.
    pub fn logical_optimizer() -> Self {
        let transforms: Vec<Box<dyn crate::Transform>> = vec![
            // 1. Structure-agnostic cleanup
            Box::new(normalize()),
            Box::new(crate::nonnull_requirements::NonNullRequirements::default()),
            // 2. Collapse constants, joins, unions, and lets as much as possible.
            // TODO: lift filters/maps to maximize ability to collapse
            // things down?
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![Box::new(crate::FuseAndCollapse::default())],
            }),
            // 3. Structure-aware cleanup that needs to happen before ColumnKnowledge
            Box::new(crate::threshold_elision::ThresholdElision),
            // 4. Move predicate information up and down the tree.
            //    This also fixes the shape of joins in the plan.
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![
                    // Predicate pushdown sets the equivalence classes of joins.
                    Box::new(crate::predicate_pushdown::PredicatePushdown::default()),
                    // Lifts the information `!isnull(col)`
                    Box::new(crate::nonnullable::NonNullable),
                    // Lifts the information `col = literal`
                    // TODO (#6613): this also tries to lift `!isnull(col)` but
                    // less well than the previous transform. Eliminate
                    // redundancy between the two transforms.
                    Box::new(crate::column_knowledge::ColumnKnowledge::default()),
                    // Lifts the information `col1 = col2`
                    Box::new(crate::demand::Demand::default()),
                    Box::new(crate::FuseAndCollapse::default()),
                ],
            }),
            // 5. Reduce/Join simplifications.
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![
                    Box::new(crate::semijoin_idempotence::SemijoinIdempotence),
                    // Pushes aggregations down
                    Box::new(crate::reduction_pushdown::ReductionPushdown),
                    // Replaces reduces with maps when the group keys are
                    // unique with maps
                    Box::new(crate::reduce_elision::ReduceElision),
                    // Converts `Cross Join {Constant(Literal) + Input}` to
                    // `Map {Cross Join (Input, Constant()), Literal}`.
                    // Join fusion will clean this up to `Map{Input, Literal}`
                    Box::new(crate::literal_lifting::LiteralLifting::default()),
                    // Identifies common relation subexpressions.
                    Box::new(crate::cse::relation_cse::RelationCSE::new(false)),
                    Box::new(crate::FuseAndCollapse::default()),
                ],
            }),
        ];
        Self {
            name: "logical",
            transforms,
        }
    }

    /// Builds a physical optimizer.
    ///
    /// Performs logical transformations followed by all physical ones.
    /// This is meant to be used for optimizing each view within a dataflow
    /// once view inlining has already happened, right before dataflow
    /// rendering.
    pub fn physical_optimizer() -> Self {
        // Implementation transformations
        let transforms: Vec<Box<dyn crate::Transform>> = vec![
            // Considerations for the relationship between JoinImplementation and other transforms:
            // - there should be a run of LiteralConstraints before JoinImplementation lifts away
            //   the Filters from the Gets;
            // - there should be no RelationCSE between this LiteralConstraints and
            //   JoinImplementation, because that could move an IndexedFilter behind a Get.
            // - The last RelationCSE before JoinImplementation should be with inline_mfp = true.
            // - Currently, JoinImplementation can't be before LiteralLifting because the latter
            //   sometimes creates `Unimplemented` joins (despite LiteralLifting already having been
            //   run in the logical optimizer).
            // - Not running ColumnKnowledge in the same fixpoint loop with JoinImplementation
            //   is slightly hurting our plans. However, I'd say we should fix these problems by
            //   making ColumnKnowledge (and/or JoinImplementation) smarter (#18051), rather than
            //   having them in the same fixpoint loop. If they would be in the same fixpoint loop,
            //   then we either run the risk of ColumnKnowledge invalidating a join plan (#17993),
            //   or we would have to run JoinImplementation an unbounded number of times, which is
            //   also not good #16076.
            //   (The same is true for FoldConstants, Demand, and LiteralLifting to a lesser
            //   extent.)
            //
            // Also note that FoldConstants and LiteralLifting are not confluent. They can
            // oscillate between e.g.:
            //         Constant
            //           - (4)
            // and
            //         Map (4)
            //           Constant
            //             - ()
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![
                    Box::new(crate::column_knowledge::ColumnKnowledge::default()),
                    Box::new(crate::fold_constants::FoldConstants { limit: Some(10000) }),
                    Box::new(crate::demand::Demand::default()),
                    Box::new(crate::literal_lifting::LiteralLifting::default()),
                ],
            }),
            Box::new(crate::literal_constraints::LiteralConstraints),
            Box::new(crate::Fixpoint {
                name: "fix_joins",
                limit: 100,
                transforms: vec![Box::new(
                    crate::join_implementation::JoinImplementation::default(),
                )],
            }),
            Box::new(crate::canonicalize_mfp::CanonicalizeMfp),
            // Identifies common relation subexpressions.
            Box::new(crate::cse::relation_cse::RelationCSE::new(false)),
            Box::new(crate::fold_constants::FoldConstants { limit: Some(10000) }),
            // Remove threshold operators which have no effect.
            // Must be done at the very end of the physical pass, because before
            // that (at least at the moment) we cannot be sure that all trees
            // are simplified equally well so they are structurally almost
            // identical. Check the `threshold_elision.slt` tests that fail if
            // you remove this transform for examples.
            Box::new(crate::threshold_elision::ThresholdElision),
        ];
        Self {
            name: "physical",
            transforms,
        }
    }

    /// Contains the logical optimizations that should run after cross-view
    /// transformations run.
    pub fn logical_cleanup_pass() -> Self {
        let transforms: Vec<Box<dyn crate::Transform>> = vec![
            // Delete unnecessary maps.
            Box::new(crate::fusion::Fusion),
            Box::new(crate::Fixpoint {
                name: "fixpoint",
                limit: 100,
                transforms: vec![
                    Box::new(crate::canonicalize_mfp::CanonicalizeMfp),
                    // Remove threshold operators which have no effect.
                    Box::new(crate::threshold_elision::ThresholdElision),
                    // Projection pushdown may unblock fusing joins and unions.
                    Box::new(crate::fusion::join::Join),
                    Box::new(crate::redundant_join::RedundantJoin::default()),
                    // Redundant join produces projects that need to be fused.
                    Box::new(crate::fusion::Fusion),
                    Box::new(crate::compound::UnionNegateFusion),
                    // This goes after union fusion so we can cancel out
                    // more branches at a time.
                    Box::new(crate::union_cancel::UnionBranchCancellation),
                    // The last RelationCSE before JoinImplementation should be with
                    // inline_mfp = true.
                    Box::new(crate::cse::relation_cse::RelationCSE::new(true)),
                    Box::new(crate::fold_constants::FoldConstants { limit: Some(10000) }),
                ],
            }),
        ];
        Self {
            name: "logical_cleanup",
            transforms,
        }
    }

    /// Optimizes the supplied relation expression.
    ///
    /// These optimizations are performed with no information about available arrangements,
    /// which makes them suitable for pre-optimization before dataflow deployment.
    #[tracing::instrument(
        target = "optimizer",
        level = "debug",
        skip_all,
        fields(path.segment = self.name)
    )]
    pub fn optimize(
        &self,
        mut relation: MirRelationExpr,
    ) -> Result<mz_expr::OptimizedMirRelationExpr, TransformError> {
        let transform_result = self.transform(&mut relation, &EmptyIndexOracle);
        match transform_result {
            Ok(_) => {
                mz_repr::explain::trace_plan(&relation);
                Ok(mz_expr::OptimizedMirRelationExpr(relation))
            }
            Err(e) => {
                // Without this, the dropping of `relation` (which happens automatically when
                // returning from this function) might run into a stack overflow, see
                // https://github.com/MaterializeInc/materialize/issues/14141
                relation.destroy_carefully();
                error!("Optimizer::optimize(): {}", e);
                Err(e)
            }
        }
    }

    /// Optimizes the supplied relation expression in place, using available arrangements.
    ///
    /// This method should only be called with non-empty `indexes` when optimizing a dataflow,
    /// as the optimizations may lock in the use of arrangements that may cease to exist.
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        indexes: &dyn IndexOracle,
    ) -> Result<(), TransformError> {
        let recursive = relation.has_let_rec();
        for transform in self.transforms.iter() {
            if transform.recursion_safe() || !recursive {
                transform.transform(relation, TransformArgs { indexes })?;
            }
        }

        Ok(())
    }
}
