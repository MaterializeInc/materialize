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

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::{fmt, iter};

use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_ore::id_gen::IdGen;
use mz_ore::stack::RecursionLimitError;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::GlobalId;
use tracing::error;

pub mod analysis;
pub mod canonicalization;
pub mod canonicalize_mfp;
pub mod column_knowledge;
pub mod compound;
pub mod cse;
pub mod dataflow;
pub mod demand;
pub mod equivalence_propagation;
pub mod fold_constants;
pub mod fusion;
pub mod join_implementation;
pub mod literal_constraints;
pub mod literal_lifting;
pub mod monotonic;
pub mod movement;
pub mod non_null_requirements;
pub mod nonnullable;
pub mod normalize_lets;
pub mod normalize_ops;
pub mod notice;
pub mod ordering;
pub mod predicate_pushdown;
pub mod reduce_elision;
pub mod reduction_pushdown;
pub mod redundant_join;
pub mod semijoin_idempotence;
pub mod threshold_elision;
pub mod typecheck;
pub mod union_cancel;

use crate::dataflow::DataflowMetainfo;
use crate::typecheck::SharedContext;
pub use dataflow::optimize_dataflow;
use mz_ore::{soft_assert_or_log, soft_panic_or_log};

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

/// Arguments that get threaded through all transforms, plus a `DataflowMetainfo` that can be
/// manipulated by the transforms.
#[derive(Debug)]
pub struct TransformCtx<'a> {
    /// The global ID for this query (if it exists).
    pub global_id: Option<GlobalId>,
    /// The indexes accessible.
    pub indexes: &'a dyn IndexOracle,
    /// Statistical estimates.
    pub stats: &'a dyn StatisticsOracle,
    /// Features passed to the enclosing `Optimizer`.
    pub features: &'a OptimizerFeatures,
    /// Typechecking context.
    pub typecheck_ctx: &'a SharedContext,
    /// Transforms can use this field to communicate information outside the result plans.
    pub df_meta: &'a mut DataflowMetainfo,
}

const FOLD_CONSTANTS_LIMIT: usize = 10000;

impl<'a> TransformCtx<'a> {
    /// Generates a [`TransformCtx`] instance for the local MIR optimization
    /// stage.
    ///
    /// Used to call [`Optimizer::optimize`] on a
    /// [`Optimizer::logical_optimizer`] in order to transform a stand-alone
    /// [`MirRelationExpr`].
    pub fn local(
        features: &'a OptimizerFeatures,
        typecheck_ctx: &'a typecheck::SharedContext,
        df_meta: &'a mut DataflowMetainfo,
    ) -> Self {
        Self {
            indexes: &EmptyIndexOracle,
            stats: &EmptyStatisticsOracle,
            global_id: None,
            features,
            typecheck_ctx,
            df_meta,
        }
    }

    /// Generates a [`TransformCtx`] instance for the global MIR optimization
    /// stage.
    ///
    /// Used to call [`dataflow::optimize_dataflow`].
    pub fn global(
        indexes: &'a dyn IndexOracle,
        stats: &'a dyn StatisticsOracle,
        features: &'a OptimizerFeatures,
        typecheck_ctx: &'a SharedContext,
        df_meta: &'a mut DataflowMetainfo,
    ) -> Self {
        Self {
            indexes,
            stats,
            global_id: None,
            features,
            df_meta,
            typecheck_ctx,
        }
    }

    fn typecheck(&self) -> SharedContext {
        Arc::clone(self.typecheck_ctx)
    }

    fn set_global_id(&mut self, global_id: GlobalId) {
        self.global_id = Some(global_id);
    }

    fn reset_global_id(&mut self) {
        self.global_id = None;
    }
}

/// Types capable of transforming relation expressions.
pub trait Transform: std::fmt::Debug {
    /// Transform a relation into a functionally equivalent relation.
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
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
    /// A reference to an apparently unbound identifier.
    IdentifierMissing(mz_expr::LocalId),
    /// Notify the caller to panic with the given message.
    ///
    /// This is used to bypass catch_unwind-wrapped calls of the optimizer and
    /// support `SELECT mz_unsafe.mz_panic(<literal>)` statements as a mechanism to kill
    /// environmentd in various tests.
    CallerShouldPanic(String),
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TransformError::Internal(msg) => write!(f, "internal transform error: {}", msg),
            TransformError::IdentifierMissing(i) => {
                write!(f, "apparently unbound identifier: {:?}", i)
            }
            TransformError::CallerShouldPanic(msg) => {
                write!(f, "caller should panic with message: {}", msg)
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
    fn indexes_on(
        &self,
        id: GlobalId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &[MirScalarExpr])> + '_>;
}

/// An [`IndexOracle`] that knows about no indexes.
#[derive(Debug)]
pub struct EmptyIndexOracle;

impl IndexOracle for EmptyIndexOracle {
    fn indexes_on(
        &self,
        _id: GlobalId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &[MirScalarExpr])> + '_> {
        Box::new(iter::empty())
    }
}

/// A trait for a type that can estimate statistics about a given `GlobalId`
pub trait StatisticsOracle: fmt::Debug + Send {
    /// Returns a cardinality estimate for the given identifier
    ///
    /// Returning `None` means "no estimate"; returning `Some(0)` means estimating that the shard backing `id` is empty
    fn cardinality_estimate(&self, id: GlobalId) -> Option<usize>;

    /// Returns a map from identifiers to sizes
    fn as_map(&self) -> BTreeMap<GlobalId, usize>;
}

/// A [`StatisticsOracle`] that knows nothing and can give no estimates.
#[derive(Debug)]
pub struct EmptyStatisticsOracle;

impl StatisticsOracle for EmptyStatisticsOracle {
    fn cardinality_estimate(&self, _: GlobalId) -> Option<usize> {
        None
    }

    fn as_map(&self) -> BTreeMap<GlobalId, usize> {
        BTreeMap::new()
    }
}

/// A sequence of transformations iterated some number of times.
#[derive(Debug)]
pub struct Fixpoint {
    name: &'static str,
    transforms: Vec<Box<dyn Transform>>,
    limit: usize,
}

impl Fixpoint {
    /// Run a single iteration of the [`Fixpoint`] transform by iterating
    /// through all transforms.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = iter_name)
    )]
    fn apply_transforms(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
        iter_name: String,
    ) -> Result<(), TransformError> {
        for transform in self.transforms.iter() {
            transform.transform(relation, ctx)?;
        }
        mz_repr::explain::trace_plan(relation);
        Ok(())
    }
}

impl Transform for Fixpoint {
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = self.name)
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // The number of iterations for a relation to settle depends on the
        // number of nodes in the relation. Instead of picking an arbitrary
        // hard limit on the number of iterations, we use a soft limit and
        // check whether the relation has become simpler after reaching it.
        // If so, we perform another pass of transforms. Otherwise, there is
        // a bug somewhere that prevents the relation from settling on a
        // stable shape.
        let mut iter_no = 0;
        let mut seen = BTreeMap::new();
        seen.insert(relation.hash_to_u64(), iter_no);
        let original = relation.clone();
        loop {
            let prev_size = relation.size();
            for i in iter_no..iter_no + self.limit {
                let prev = relation.clone();
                self.apply_transforms(relation, ctx, format!("{i:04}"))?;
                if *relation == prev {
                    if prev_size > 100000 {
                        tracing::warn!(%prev_size, "Very big MIR plan");
                    }
                    mz_repr::explain::trace_plan(relation);
                    return Ok(());
                }
                let seen_i = seen.insert(relation.hash_to_u64(), i);
                if let Some(seen_i) = seen_i {
                    // Let's see whether this is just a hash collision, or a real loop: Run the
                    // whole thing from the beginning up until `seen_i`, and compare all the plans
                    // to the current plan from the outer `for`.
                    // (It would not be enough to compare only the plan at `seen_i`, because
                    // then we could miss a real loop if there is also a hash collision somewhere
                    // in the middle of the loop, because then we'd compare the last plan of the
                    // loop not with its actual match, but with the colliding plan.)
                    let mut again = original.clone();
                    // The `+2` is because:
                    // - one `+1` is to finally get to the plan at `seen_i`,
                    // - another `+1` is because we are comparing to `relation` only _before_
                    //   calling `apply_transforms`.
                    for j in 0..(seen_i + 2) {
                        if again == *relation {
                            // We really got into an infinite loop (e.g., we are oscillating between
                            // two plans). This is not catastrophic, because we can just say we are
                            // done now, but it would be great to eventually find a way to prevent
                            // these loops from happening in the first place. We have several
                            // relevant issues, see
                            // https://github.com/MaterializeInc/database-issues/issues/8197#issuecomment-2200172227
                            mz_repr::explain::trace_plan(relation);
                            soft_panic_or_log!(
                                "Fixpoint `{}` detected a loop of length {} after {} iterations",
                                self.name,
                                i - seen_i,
                                i
                            );
                            return Ok(());
                        }
                        self.apply_transforms(
                            &mut again,
                            ctx,
                            format!("collision detection {j:04}"),
                        )?;
                    }
                    // If we got here, then this was just a hash collision! Just continue as if
                    // nothing happened.
                }
            }
            let current_size = relation.size();

            iter_no += self.limit;

            if current_size < prev_size {
                tracing::warn!(
                    "Fixpoint {} ran for {} iterations \
                     without reaching a fixpoint but reduced the relation size; \
                     current_size ({}) < prev_size ({}); \
                     continuing for {} more iterations",
                    self.name,
                    iter_no,
                    current_size,
                    prev_size,
                    self.limit
                );
            } else {
                // We failed to reach a fixed point, or find a sufficiently short cycle.
                // This is not catastrophic, because we can just say we are done now,
                // but it would be great to eventually find a way to prevent these loops from
                // happening in the first place. We have several relevant issues, see
                // https://github.com/MaterializeInc/database-issues/issues/8197#issuecomment-2200172227
                mz_repr::explain::trace_plan(relation);
                soft_panic_or_log!(
                    "Fixpoint {} failed to reach a fixed point, or cycle of length at most {}",
                    self.name,
                    self.limit,
                );
                return Ok(());
            }
        }
    }
}

/// Convenience macro for guarding transforms behind a feature flag.
///
/// If you have a code block like
///
/// ```ignore
/// vec![
///     Box::new(Foo::default()),
///     Box::new(Bar::default()),
///     Box::new(Baz::default()),
/// ]
/// ```
///
/// and you want to guard `Bar` behind a feature flag `enable_bar`, you can
/// write
///
/// ```ignore
/// transforms![
///     Box::new(Foo::default()),
///     Box::new(Bar::default()); if ctx.features.enable_bar,
///     Box::new(Baz::default()),
/// ]
/// ```
///
/// as a shorthand and in order to minimize your code diff.
#[allow(unused_macros)]
macro_rules! transforms {
    // Internal rule. Matches lines with a guard: `$transform; if $cond`.
    (@op fill $buf:ident with $transform:expr; if $cond:expr, $($transforms:tt)*) => {
        if $cond {
            $buf.push($transform);
        }
        transforms!(@op fill $buf with $($transforms)*);
    };
    // Internal rule. Matchesl lines without a guard: `$transform`.
    (@op fill $buf:ident with $transform:expr, $($transforms:tt)*) => {
        $buf.push($transform);
        transforms!(@op fill $buf with $($transforms)*);
    };
    // Internal rule: matches the empty $transforms TokenTree (terminal case).
    (@op fill $buf:ident with) => {
        // do nothing
    };
    ($($transforms:tt)*) => {{
        let mut __buf = Vec::<Box<dyn Transform>>::new();
        transforms!(@op fill __buf with $($transforms)*);
        __buf
    }};
}

/// A sequence of transformations that simplify the `MirRelationExpr`
#[derive(Debug)]
pub struct FuseAndCollapse {
    transforms: Vec<Box<dyn Transform>>,
}

impl Default for FuseAndCollapse {
    fn default() -> Self {
        Self {
            // TODO: The relative orders of the transforms have not been
            // determined except where there are comments.
            // TODO (database-issues#2036): All the transforms here except for `ProjectionLifting`
            //  and `RedundantJoin` can be implemented as free functions.
            transforms: vec![
                Box::new(canonicalization::ProjectionExtraction),
                Box::new(movement::ProjectionLifting::default()),
                Box::new(fusion::Fusion),
                Box::new(canonicalization::FlatMapToMap),
                Box::new(fusion::join::Join),
                Box::new(normalize_lets::NormalizeLets::new(false)),
                Box::new(fusion::reduce::Reduce),
                Box::new(compound::UnionNegateFusion),
                // This goes after union fusion so we can cancel out
                // more branches at a time.
                Box::new(union_cancel::UnionBranchCancellation),
                // This should run before redundant join to ensure that key info
                // is correct.
                Box::new(normalize_lets::NormalizeLets::new(false)),
                // Removes redundant inputs from joins.
                // Note that this eliminates one redundant input per join,
                // so it is necessary to run this section in a loop.
                Box::new(redundant_join::RedundantJoin::default()),
                // As a final logical action, convert any constant expression to a constant.
                // Some optimizations fight against this, and we want to be sure to end as a
                // `MirRelationExpr::Constant` if that is the case, so that subsequent use can
                // clearly see this.
                Box::new(fold_constants::FoldConstants {
                    limit: Some(FOLD_CONSTANTS_LIMIT),
                }),
                Box::new(canonicalization::ReduceScalars),
            ],
        }
    }
}

impl Transform for FuseAndCollapse {
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "fuse_and_collapse")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        for transform in self.transforms.iter() {
            transform.transform(relation, ctx)?;
        }
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

/// Run the [`FuseAndCollapse`] transforms in a fixpoint.
pub fn fuse_and_collapse() -> Fixpoint {
    Fixpoint {
        name: "fuse_and_collapse",
        limit: 100,
        transforms: FuseAndCollapse::default().transforms,
    }
}

/// Construct a normalizing transform that runs transforms that normalize the
/// structure of the tree until a fixpoint.
///
/// Care needs to be taken to ensure that the fixpoint converges for every
/// possible input tree. If this is not the case, there are two possibilities:
/// 1. The rewrite loop runs enters an oscillating cycle.
/// 2. The expression grows without bound.
pub fn normalize() -> Fixpoint {
    Fixpoint {
        name: "normalize",
        limit: 100,
        transforms: vec![
            Box::new(normalize_lets::NormalizeLets::new(false)),
            Box::new(normalize_ops::NormalizeOps),
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
    pub transforms: Vec<Box<dyn Transform>>,
}

impl Optimizer {
    /// Builds a logical optimizer that only performs logical transformations.
    #[deprecated = "Create an Optimize instance and call `optimize` instead."]
    pub fn logical_optimizer(ctx: &mut TransformCtx) -> Self {
        let transforms: Vec<Box<dyn Transform>> = vec![
            Box::new(typecheck::Typecheck::new(ctx.typecheck()).strict_join_equivalences()),
            // 1. Structure-agnostic cleanup
            Box::new(normalize()),
            Box::new(non_null_requirements::NonNullRequirements::default()),
            // 2. Collapse constants, joins, unions, and lets as much as possible.
            // TODO: lift filters/maps to maximize ability to collapse
            // things down?
            Box::new(fuse_and_collapse()),
            // 3. Structure-aware cleanup that needs to happen before ColumnKnowledge
            Box::new(threshold_elision::ThresholdElision),
            // 4. Move predicate information up and down the tree.
            //    This also fixes the shape of joins in the plan.
            Box::new(Fixpoint {
                name: "fixpoint_logical_01",
                limit: 100,
                transforms: vec![
                    // Predicate pushdown sets the equivalence classes of joins.
                    Box::new(predicate_pushdown::PredicatePushdown::default()),
                    Box::new(equivalence_propagation::EquivalencePropagation::default()),
                    // Lifts the information `!isnull(col)`
                    Box::new(nonnullable::NonNullable),
                    // Lifts the information `col = literal`
                    // TODO (database-issues#2062): this also tries to lift `!isnull(col)` but
                    // less well than the previous transform. Eliminate
                    // redundancy between the two transforms.
                    Box::new(column_knowledge::ColumnKnowledge::default()),
                    // Lifts the information `col1 = col2`
                    Box::new(demand::Demand::default()),
                    Box::new(FuseAndCollapse::default()),
                ],
            }),
            // 5. Reduce/Join simplifications.
            Box::new(Fixpoint {
                name: "fixpoint_logical_02",
                limit: 100,
                transforms: vec![
                    Box::new(semijoin_idempotence::SemijoinIdempotence::default()),
                    // Pushes aggregations down
                    Box::new(reduction_pushdown::ReductionPushdown),
                    // Replaces reduces with maps when the group keys are
                    // unique with maps
                    Box::new(reduce_elision::ReduceElision),
                    // Converts `Cross Join {Constant(Literal) + Input}` to
                    // `Map {Cross Join (Input, Constant()), Literal}`.
                    // Join fusion will clean this up to `Map{Input, Literal}`
                    Box::new(literal_lifting::LiteralLifting::default()),
                    // Identifies common relation subexpressions.
                    Box::new(cse::relation_cse::RelationCSE::new(false)),
                    Box::new(FuseAndCollapse::default()),
                ],
            }),
            Box::new(
                typecheck::Typecheck::new(ctx.typecheck())
                    .disallow_new_globals()
                    .strict_join_equivalences(),
            ),
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
    pub fn physical_optimizer(ctx: &mut TransformCtx) -> Self {
        // Implementation transformations
        let transforms: Vec<Box<dyn Transform>> = vec![
            Box::new(
                typecheck::Typecheck::new(ctx.typecheck())
                    .disallow_new_globals()
                    .strict_join_equivalences(),
            ),
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
            //   making ColumnKnowledge (and/or JoinImplementation) smarter (database-issues#5289), rather than
            //   having them in the same fixpoint loop. If they would be in the same fixpoint loop,
            //   then we either run the risk of ColumnKnowledge invalidating a join plan (database-issues#5260),
            //   or we would have to run JoinImplementation an unbounded number of times, which is
            //   also not good database-issues#4639.
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
            Box::new(Fixpoint {
                name: "fixpoint_physical_01",
                limit: 100,
                transforms: vec![
                    Box::new(column_knowledge::ColumnKnowledge::default()),
                    Box::new(fold_constants::FoldConstants {
                        limit: Some(FOLD_CONSTANTS_LIMIT),
                    }),
                    Box::new(canonicalization::ReduceScalars),
                    Box::new(demand::Demand::default()),
                    Box::new(literal_lifting::LiteralLifting::default()),
                ],
            }),
            Box::new(literal_constraints::LiteralConstraints),
            Box::new(Fixpoint {
                name: "fixpoint_join_impl",
                limit: 100,
                transforms: vec![Box::new(join_implementation::JoinImplementation::default())],
            }),
            Box::new(canonicalize_mfp::CanonicalizeMfp),
            // Identifies common relation subexpressions.
            Box::new(cse::relation_cse::RelationCSE::new(false)),
            Box::new(fold_constants::FoldConstants {
                limit: Some(FOLD_CONSTANTS_LIMIT),
            }),
            Box::new(canonicalization::ReduceScalars),
            // Remove threshold operators which have no effect.
            // Must be done at the very end of the physical pass, because before
            // that (at least at the moment) we cannot be sure that all trees
            // are simplified equally well so they are structurally almost
            // identical. Check the `threshold_elision.slt` tests that fail if
            // you remove this transform for examples.
            Box::new(threshold_elision::ThresholdElision),
            // We need this to ensure that `CollectIndexRequests` gets a normalized plan.
            // (For example, `FoldConstants` can break the normalized form by removing all
            // references to a Let, see https://github.com/MaterializeInc/database-issues/issues/6371)
            Box::new(normalize_lets::NormalizeLets::new(false)),
            Box::new(typecheck::Typecheck::new(ctx.typecheck()).disallow_new_globals()),
        ];
        Self {
            name: "physical",
            transforms,
        }
    }

    /// Contains the logical optimizations that should run after cross-view
    /// transformations run.
    ///
    /// Set `allow_new_globals` when you will use these as the first passes.
    /// The first instance of the typechecker in an optimizer pipeline should
    /// allow new globals (or it will crash when it encounters them).
    pub fn logical_cleanup_pass(ctx: &mut TransformCtx, allow_new_globals: bool) -> Self {
        let mut typechecker = typecheck::Typecheck::new(ctx.typecheck()).strict_join_equivalences();

        if !allow_new_globals {
            typechecker = typechecker.disallow_new_globals();
        }

        let transforms: Vec<Box<dyn Transform>> = vec![
            Box::new(typechecker),
            // Delete unnecessary maps.
            Box::new(fusion::Fusion),
            Box::new(Fixpoint {
                name: "fixpoint_logical_cleanup_pass_01",
                limit: 100,
                transforms: vec![
                    Box::new(canonicalize_mfp::CanonicalizeMfp),
                    // Remove threshold operators which have no effect.
                    Box::new(threshold_elision::ThresholdElision),
                    // Projection pushdown may unblock fusing joins and unions.
                    Box::new(fusion::join::Join),
                    // Predicate pushdown required to tidy after join fusion.
                    Box::new(predicate_pushdown::PredicatePushdown::default()),
                    Box::new(redundant_join::RedundantJoin::default()),
                    // Redundant join produces projects that need to be fused.
                    Box::new(fusion::Fusion),
                    Box::new(compound::UnionNegateFusion),
                    // This goes after union fusion so we can cancel out
                    // more branches at a time.
                    Box::new(union_cancel::UnionBranchCancellation),
                    // The last RelationCSE before JoinImplementation should be with
                    // inline_mfp = true.
                    Box::new(cse::relation_cse::RelationCSE::new(true)),
                    Box::new(fold_constants::FoldConstants {
                        limit: Some(FOLD_CONSTANTS_LIMIT),
                    }),
                    Box::new(canonicalization::ReduceScalars),
                ],
            }),
            Box::new(
                typecheck::Typecheck::new(ctx.typecheck())
                    .disallow_new_globals()
                    .strict_join_equivalences(),
            ),
        ];
        Self {
            name: "logical_cleanup",
            transforms,
        }
    }

    /// Builds a tiny optimizer, which is only suitable for optimizing fast-path queries.
    pub fn fast_path_optimizer(_ctx: &mut TransformCtx) -> Self {
        let transforms: Vec<Box<dyn Transform>> = vec![
            Box::new(canonicalization::ReduceScalars),
            Box::new(literal_constraints::LiteralConstraints),
            Box::new(canonicalize_mfp::CanonicalizeMfp),
            // We might have arrived at a constant, e.g., due to contradicting literal constraints.
            Box::new(fold_constants::FoldConstants {
                limit: Some(FOLD_CONSTANTS_LIMIT),
            }),
        ];
        Self {
            name: "fast_path_optimizer",
            transforms,
        }
    }

    /// Optimizes the supplied relation expression.
    ///
    /// These optimizations are performed with no information about available arrangements,
    /// which makes them suitable for pre-optimization before dataflow deployment.
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = self.name)
    )]
    pub fn optimize(
        &self,
        mut relation: MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<mz_expr::OptimizedMirRelationExpr, TransformError> {
        let transform_result = self.transform(&mut relation, ctx);

        // Make sure we are not swallowing any notice.
        // TODO: we should actually wire up notices that come from here. This is not urgent, because
        // currently notices can only come from the physical MIR optimizer (specifically,
        // `LiteralConstraints`), and callers of this method are running the logical MIR optimizer.
        soft_assert_or_log!(
            ctx.df_meta.optimizer_notices.is_empty(),
            "logical MIR optimization unexpectedly produced notices"
        );

        match transform_result {
            Ok(_) => {
                mz_repr::explain::trace_plan(&relation);
                Ok(mz_expr::OptimizedMirRelationExpr(relation))
            }
            Err(e) => {
                // Without this, the dropping of `relation` (which happens automatically when
                // returning from this function) might run into a stack overflow, see
                // https://github.com/MaterializeInc/database-issues/issues/4043
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
        args: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        for transform in self.transforms.iter() {
            transform.transform(relation, args)?;
        }

        Ok(())
    }
}
