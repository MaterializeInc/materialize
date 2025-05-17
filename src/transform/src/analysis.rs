// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits and types for reusable expression analysis

pub mod equivalences;
pub mod monotonic;

use mz_expr::MirRelationExpr;

pub use arity::Arity;
pub use cardinality::Cardinality;
pub use column_names::{ColumnName, ColumnNames};
pub use common::{Derived, DerivedBuilder, DerivedView};
pub use explain::annotate_plan;
pub use non_negative::NonNegative;
pub use subtree::SubtreeSize;
pub use types::RelationType;
pub use unique_keys::UniqueKeys;

/// An analysis that can be applied bottom-up to a `MirRelationExpr`.
pub trait Analysis: 'static {
    /// The type of value this analysis associates with an expression.
    type Value: std::fmt::Debug;
    /// Announce any dependencies this analysis has on other analyses.
    ///
    /// The method should invoke `builder.require::<Foo>()` for each other
    /// analysis `Foo` this analysis depends upon.
    fn announce_dependencies(_builder: &mut DerivedBuilder) {}
    /// The analysis value derived for an expression, given other analysis results.
    ///
    /// The other analysis results include the results of this analysis for all children,
    /// in `results`, and the results of other analyses this analysis has expressed a
    /// dependence upon, in `depends`, for children and the expression itself.
    /// The analysis results for `Self` can only be found in `results`, and are not
    /// available in `depends`.
    ///
    /// Implementors of this method must defensively check references into `results`, as
    /// it may be invoked on `LetRec` bindings that have not yet been populated. It is up
    /// to the analysis what to do in that case, but conservative behavior is recommended.
    ///
    /// The `index` indicates the post-order index for the expression, for use in finding
    /// the corresponding information in `results` and `depends`.
    ///
    /// The returned result will be associated with this expression for this analysis,
    /// and the analyses will continue.
    fn derive(
        &self,
        expr: &MirRelationExpr,
        index: usize,
        results: &[Self::Value],
        depends: &Derived,
    ) -> Self::Value;

    /// When available, provide a lattice interface to allow optimistic recursion.
    ///
    /// Providing a non-`None` output indicates that the analysis intends re-iteration.
    fn lattice() -> Option<Box<dyn Lattice<Self::Value>>> {
        None
    }
}

/// Lattice methods for repeated analysis
pub trait Lattice<T> {
    /// An element greater than all other elements.
    fn top(&self) -> T;
    /// Set `a` to the greatest lower bound of `a` and `b`, and indicate if `a` changed as a result.
    fn meet_assign(&self, a: &mut T, b: T) -> bool;
}

/// Types common across multiple analyses
pub mod common {

    use std::any::{Any, TypeId};
    use std::collections::BTreeMap;

    use mz_expr::LocalId;
    use mz_expr::MirRelationExpr;
    use mz_ore::assert_none;
    use mz_repr::optimize::OptimizerFeatures;

    use super::Analysis;
    use super::subtree::SubtreeSize;

    /// Container for analysis state and binding context.
    #[derive(Default)]
    #[allow(missing_debug_implementations)]
    pub struct Derived {
        /// A record of active analyses and their results, indexed by their type id.
        analyses: BTreeMap<TypeId, Box<dyn AnalysisBundle>>,
        /// Analyses ordered where each depends only on strictly prior analyses.
        order: Vec<TypeId>,
        /// Map from local identifier to result offset for analysis values.
        bindings: BTreeMap<LocalId, usize>,
    }

    impl Derived {
        /// Return the analysis results derived so far.
        ///
        /// # Panics
        ///
        /// This method panics if `A` was not installed as a required analysis.
        pub fn results<A: Analysis>(&self) -> &[A::Value] {
            let type_id = TypeId::of::<Bundle<A>>();
            if let Some(bundle) = self.analyses.get(&type_id) {
                if let Some(bundle) = bundle.as_any().downcast_ref::<Bundle<A>>() {
                    return &bundle.results[..];
                }
            }
            panic!("Analysis {:?} missing", std::any::type_name::<A>());
        }
        /// Bindings from local identifiers to result offsets for analysis values.
        pub fn bindings(&self) -> &BTreeMap<LocalId, usize> {
            &self.bindings
        }
        /// Result offsets for the state of a various number of children of the current expression.
        ///
        /// The integers are the zero-offset locations in the `SubtreeSize` analysis. The order of
        /// the children is descending, from last child to first, because of how the information is
        /// laid out, and the non-reversibility of the look-ups.
        ///
        /// It is an error to call this method with more children than expression has.
        pub fn children_of_rev<'a>(
            &'a self,
            start: usize,
            count: usize,
        ) -> impl Iterator<Item = usize> + 'a {
            let sizes = self.results::<SubtreeSize>();
            let offset = 1;
            (0..count).scan(offset, move |offset, _| {
                let result = start - *offset;
                *offset += sizes[result];
                Some(result)
            })
        }

        /// Recast the derived data as a view that can be subdivided into views over child state.
        pub fn as_view<'a>(&'a self) -> DerivedView<'a> {
            DerivedView {
                derived: self,
                lower: 0,
                upper: self.results::<SubtreeSize>().len(),
            }
        }
    }

    /// The subset of a `Derived` corresponding to an expression and its children.
    ///
    /// Specifically, bounds an interval `[lower, upper)` that ends with the state
    /// of an expression, at `upper-1`, and is preceded by the state of descendents.
    ///
    /// This is best thought of as a node in a tree rather
    #[allow(missing_debug_implementations)]
    #[derive(Copy, Clone)]
    pub struct DerivedView<'a> {
        derived: &'a Derived,
        lower: usize,
        upper: usize,
    }

    impl<'a> DerivedView<'a> {
        /// The value associated with the expression.
        pub fn value<A: Analysis>(&self) -> Option<&'a A::Value> {
            self.results::<A>().last()
        }

        /// The post-order traversal index for the expression.
        ///
        /// This can be used to index into the full set of results, as provided by an
        /// instance of `Derived`.
        pub fn index(&self) -> usize {
            self.upper - 1
        }

        /// The value bound to an identifier, if it has been derived.
        ///
        /// There are several reasons the value could not be derived, and this method
        /// does not distinguish between them.
        pub fn bound<A: Analysis>(&self, id: LocalId) -> Option<&'a A::Value> {
            self.derived
                .bindings
                .get(&id)
                .and_then(|index| self.derived.results::<A>().get(*index))
        }

        /// The results for expression and its children.
        ///
        /// The results for the expression itself will be the last element.
        ///
        /// # Panics
        ///
        /// This method panics if `A` was not installed as a required analysis.
        pub fn results<A: Analysis>(&self) -> &'a [A::Value] {
            &self.derived.results::<A>()[self.lower..self.upper]
        }

        /// Bindings from local identifiers to result offsets for analysis values.
        ///
        /// This method returns all bindings, which may include bindings not in scope for
        /// the expression and its children; they should be ignored.
        pub fn bindings(&self) -> &'a BTreeMap<LocalId, usize> {
            self.derived.bindings()
        }

        /// Subviews over `self` corresponding to the children of the expression, in reverse order.
        ///
        /// These views should disjointly cover the same interval as `self`, except for the last element
        /// which corresponds to the expression itself.
        ///
        /// The number of produced items should exactly match the number of children, which need not
        /// be provided as an argument. This relies on the well-formedness of the view, which should
        /// exhaust itself just as it enumerates its last (the first) child view.
        pub fn children_rev(&self) -> impl Iterator<Item = DerivedView<'a>> + 'a {
            // This logic is copy/paste from `Derived::children_of_rev` but it was annoying to layer
            // it over the output of that function, and perhaps clearer to rewrite in any case.

            // Discard the last element (the size of the expression's subtree).
            // Repeatedly read out the last element, then peel off that many elements.
            // Each extracted slice corresponds to a child of the current expression.
            // We should end cleanly with an empty slice, otherwise there is an issue.
            let sizes = self.results::<SubtreeSize>();
            let sizes = &sizes[..sizes.len() - 1];

            let offset = self.lower;
            let derived = self.derived;
            (0..).scan(sizes, move |sizes, _| {
                if let Some(size) = sizes.last() {
                    *sizes = &sizes[..sizes.len() - size];
                    Some(Self {
                        derived,
                        lower: offset + sizes.len(),
                        upper: offset + sizes.len() + size,
                    })
                } else {
                    None
                }
            })
        }

        /// A convenience method for the view over the expressions last child.
        ///
        /// This method is appropriate to call on expressions with multiple children,
        /// and in particular for `Let` and `LetRecv` variants where the body is the
        /// last child.
        ///
        /// It is an error to call this on a view for an expression with no children.
        pub fn last_child(&self) -> DerivedView<'a> {
            self.children_rev().next().unwrap()
        }
    }

    /// A builder wrapper to accumulate announced dependencies and construct default state.
    #[allow(missing_debug_implementations)]
    pub struct DerivedBuilder<'a> {
        result: Derived,
        features: &'a OptimizerFeatures,
    }

    impl<'a> DerivedBuilder<'a> {
        /// Create a new [`DerivedBuilder`] parameterized by [`OptimizerFeatures`].
        pub fn new(features: &'a OptimizerFeatures) -> Self {
            // The default builder should include `SubtreeSize` to facilitate navigation.
            let mut builder = DerivedBuilder {
                result: Derived::default(),
                features,
            };
            builder.require(SubtreeSize);
            builder
        }
    }

    impl<'a> DerivedBuilder<'a> {
        /// Announces a dependence on an analysis `A`.
        ///
        /// This ensures that `A` will be performed, and before any analysis that
        /// invokes this method.
        pub fn require<A: Analysis>(&mut self, analysis: A) {
            // The method recursively descends through required analyses, first
            // installing each in `result.analyses` and second in `result.order`.
            // The first is an obligation, and serves as an indication that we have
            // found a cycle in dependencies.
            let type_id = TypeId::of::<Bundle<A>>();
            if !self.result.order.contains(&type_id) {
                // If we have not sequenced `type_id` but have a bundle, it means
                // we are in the process of fulfilling its requirements: a cycle.
                if self.result.analyses.contains_key(&type_id) {
                    panic!("Cyclic dependency detected: {}", std::any::type_name::<A>());
                }
                // Insert the analysis bundle first, so that we can detect cycles.
                self.result.analyses.insert(
                    type_id,
                    Box::new(Bundle::<A> {
                        analysis,
                        results: Vec::new(),
                        fuel: 100,
                        allow_optimistic: self.features.enable_letrec_fixpoint_analysis,
                    }),
                );
                A::announce_dependencies(self);
                // All dependencies are successfully sequenced; sequence `type_id`.
                self.result.order.push(type_id);
            }
        }
        /// Complete the building: perform analyses and return the resulting `Derivation`.
        pub fn visit(mut self, expr: &MirRelationExpr) -> Derived {
            // A stack of expressions to process (`Ok`) and let bindings to fill (`Err`).
            let mut todo = vec![Ok(expr)];
            // Expressions in reverse post-order: each expression, followed by its children in reverse order.
            // We will reverse this to get the post order, but must form it in reverse.
            let mut rev_post_order = Vec::new();
            while let Some(command) = todo.pop() {
                match command {
                    // An expression to visit.
                    Ok(expr) => {
                        match expr {
                            MirRelationExpr::Let { id, value, body } => {
                                todo.push(Ok(value));
                                todo.push(Err(*id));
                                todo.push(Ok(body));
                            }
                            MirRelationExpr::LetRec {
                                ids, values, body, ..
                            } => {
                                for (id, value) in ids.iter().zip(values) {
                                    todo.push(Ok(value));
                                    todo.push(Err(*id));
                                }
                                todo.push(Ok(body));
                            }
                            _ => {
                                todo.extend(expr.children().map(Ok));
                            }
                        }
                        rev_post_order.push(expr);
                    }
                    // A local id to install
                    Err(local_id) => {
                        // Capture the *remaining* work, which we'll need to flip around.
                        let prior = self.result.bindings.insert(local_id, rev_post_order.len());
                        assert_none!(prior, "Shadowing not allowed");
                    }
                }
            }
            // Flip the offsets now that we know a length.
            for value in self.result.bindings.values_mut() {
                *value = rev_post_order.len() - *value - 1;
            }
            // Visit the pre-order in reverse order: post-order.
            rev_post_order.reverse();

            // Apply each analysis to `expr` in order.
            for id in self.result.order.iter() {
                if let Some(mut bundle) = self.result.analyses.remove(id) {
                    bundle.analyse(&rev_post_order[..], &self.result);
                    self.result.analyses.insert(*id, bundle);
                }
            }

            self.result
        }
    }

    /// An abstraction for an analysis and associated state.
    trait AnalysisBundle: Any {
        /// Populates internal state for all of `exprs`.
        ///
        /// Result indicates whether new information was produced for `exprs.last()`.
        fn analyse(&mut self, exprs: &[&MirRelationExpr], depends: &Derived) -> bool;
        /// Upcasts `self` to a `&dyn Any`.
        ///
        /// NOTE: This is required until <https://github.com/rust-lang/rfcs/issues/2765> is fixed
        fn as_any(&self) -> &dyn std::any::Any;
    }

    /// A wrapper for analysis state.
    struct Bundle<A: Analysis> {
        /// The algorithm instance used to derive the results.
        analysis: A,
        /// A vector of results.
        results: Vec<A::Value>,
        /// Counts down with each `LetRec` re-iteration, to avoid unbounded effort.
        /// Should it reach zero, the analysis should discard its results and restart as if pessimistic.
        fuel: usize,
        /// Allow optimistic analysis for `A` (otherwise we always do pesimistic
        /// analysis, even if a [`crate::analysis::Lattice`] is available for `A`).
        allow_optimistic: bool,
    }

    impl<A: Analysis> AnalysisBundle for Bundle<A> {
        fn analyse(&mut self, exprs: &[&MirRelationExpr], depends: &Derived) -> bool {
            self.results.clear();
            // Attempt optimistic analysis, and if that fails go pessimistic.
            let update = A::lattice()
                .filter(|_| self.allow_optimistic)
                .and_then(|lattice| {
                    for _ in exprs.iter() {
                        self.results.push(lattice.top());
                    }
                    self.analyse_optimistic(exprs, 0, exprs.len(), depends, &*lattice)
                        .ok()
                })
                .unwrap_or_else(|| {
                    self.results.clear();
                    self.analyse_pessimistic(exprs, depends)
                });
            assert_eq!(self.results.len(), exprs.len());
            update
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    impl<A: Analysis> Bundle<A> {
        /// Analysis that starts optimistically but is only correct at a fixed point.
        ///
        /// Will fail out to `analyse_pessimistic` if the lattice is missing, or `self.fuel` is exhausted.
        /// When successful, the result indicates whether new information was produced for `exprs.last()`.
        fn analyse_optimistic(
            &mut self,
            exprs: &[&MirRelationExpr],
            lower: usize,
            upper: usize,
            depends: &Derived,
            lattice: &dyn crate::analysis::Lattice<A::Value>,
        ) -> Result<bool, ()> {
            // Repeatedly re-evaluate the whole tree bottom up, until no changes of fuel spent.
            let mut changed = true;
            while changed {
                changed = false;

                // Bail out if we have done too many `LetRec` passes in this analysis.
                self.fuel -= 1;
                if self.fuel == 0 {
                    return Err(());
                }

                // Track if repetitions may be required, to avoid iteration if they are not.
                let mut is_recursive = false;
                // Update each derived value and track if any have changed.
                for index in lower..upper {
                    let value = self.derive(exprs[index], index, depends);
                    changed = lattice.meet_assign(&mut self.results[index], value) || changed;
                    if let MirRelationExpr::LetRec { .. } = &exprs[index] {
                        is_recursive = true;
                    }
                }

                // Un-set the potential loop if there was no recursion.
                if !is_recursive {
                    changed = false;
                }
            }
            Ok(true)
        }

        /// Analysis that starts conservatively and can be stopped at any point.
        ///
        /// Result indicates whether new information was produced for `exprs.last()`.
        fn analyse_pessimistic(&mut self, exprs: &[&MirRelationExpr], depends: &Derived) -> bool {
            // TODO: consider making iterative, from some `bottom()` up using `join_assign()`.
            self.results.clear();
            for (index, expr) in exprs.iter().enumerate() {
                self.results.push(self.derive(expr, index, depends));
            }
            true
        }

        #[inline]
        fn derive(&self, expr: &MirRelationExpr, index: usize, depends: &Derived) -> A::Value {
            self.analysis
                .derive(expr, index, &self.results[..], depends)
        }
    }
}

/// Expression subtree sizes
///
/// This analysis counts the number of expressions in each subtree, and is most useful
/// for navigating the results of other analyses that are offset by subtree sizes.
pub mod subtree {

    use super::{Analysis, Derived};
    use mz_expr::MirRelationExpr;

    /// Analysis that determines the size in child expressions of relation expressions.
    #[derive(Debug)]
    pub struct SubtreeSize;

    impl Analysis for SubtreeSize {
        type Value = usize;

        fn derive(
            &self,
            expr: &MirRelationExpr,
            index: usize,
            results: &[Self::Value],
            _depends: &Derived,
        ) -> Self::Value {
            match expr {
                MirRelationExpr::Constant { .. } | MirRelationExpr::Get { .. } => 1,
                _ => {
                    let mut offset = 1;
                    for _ in expr.children() {
                        offset += results[index - offset];
                    }
                    offset
                }
            }
        }
    }
}

/// Expression arities
mod arity {

    use super::{Analysis, Derived};
    use mz_expr::MirRelationExpr;

    /// Analysis that determines the number of columns of relation expressions.
    #[derive(Debug)]
    pub struct Arity;

    impl Analysis for Arity {
        type Value = usize;

        fn derive(
            &self,
            expr: &MirRelationExpr,
            index: usize,
            results: &[Self::Value],
            depends: &Derived,
        ) -> Self::Value {
            let mut offsets = depends
                .children_of_rev(index, expr.children().count())
                .map(|child| results[child])
                .collect::<Vec<_>>();
            offsets.reverse();
            expr.arity_with_input_arities(offsets.into_iter())
        }
    }
}

/// Expression types
mod types {

    use super::{Analysis, Derived, Lattice};
    use mz_expr::MirRelationExpr;
    use mz_repr::ColumnType;

    /// Analysis that determines the type of relation expressions.
    ///
    /// The value is `Some` when it discovers column types, and `None` in the case that
    /// it has discovered no constraining information on the column types. The `None`
    /// variant should only occur in the course of iteration, and should not be revealed
    /// as an output of the analysis. One can `unwrap()` the result, and if it errors then
    /// either the expression is malformed or the analysis has a bug.
    ///
    /// The analysis will panic if an expression is not well typed (i.e. if `try_col_with_input_cols`
    /// returns an error).
    #[derive(Debug)]
    pub struct RelationType;

    impl Analysis for RelationType {
        type Value = Option<Vec<ColumnType>>;

        fn derive(
            &self,
            expr: &MirRelationExpr,
            index: usize,
            results: &[Self::Value],
            depends: &Derived,
        ) -> Self::Value {
            let offsets = depends
                .children_of_rev(index, expr.children().count())
                .map(|child| &results[child])
                .collect::<Vec<_>>();

            // For most expressions we'll apply `try_col_with_input_cols`, but for `Get` expressions
            // we'll want to combine what we know (iteratively) with the stated `Get::typ`.
            match expr {
                MirRelationExpr::Get {
                    id: mz_expr::Id::Local(i),
                    typ,
                    ..
                } => {
                    let mut result = typ.column_types.clone();
                    if let Some(o) = depends.bindings().get(i) {
                        if let Some(t) = results.get(*o) {
                            if let Some(rec_typ) = t {
                                // Reconcile nullability statements.
                                // Unclear if we should trust `typ`.
                                assert_eq!(result.len(), rec_typ.len());
                                result.clone_from(rec_typ);
                                for (res, col) in result.iter_mut().zip(typ.column_types.iter()) {
                                    if !col.nullable {
                                        res.nullable = false;
                                    }
                                }
                            } else {
                                // Our `None` information indicates that we are optimistically
                                // assuming the best, including that all columns are non-null.
                                // This should only happen in the first visit to a `Get` expr.
                                // Use `typ`, but flatten nullability.
                                for col in result.iter_mut() {
                                    col.nullable = false;
                                }
                            }
                        }
                    }
                    Some(result)
                }
                _ => {
                    // Every expression with inputs should have non-`None` inputs at this point.
                    let input_cols = offsets.into_iter().rev().map(|o| {
                        o.as_ref()
                            .expect("RelationType analysis discovered type-less expression")
                    });
                    Some(expr.try_col_with_input_cols(input_cols).unwrap())
                }
            }
        }

        fn lattice() -> Option<Box<dyn Lattice<Self::Value>>> {
            Some(Box::new(RTLattice))
        }
    }

    struct RTLattice;

    impl Lattice<Option<Vec<ColumnType>>> for RTLattice {
        fn top(&self) -> Option<Vec<ColumnType>> {
            None
        }
        fn meet_assign(&self, a: &mut Option<Vec<ColumnType>>, b: Option<Vec<ColumnType>>) -> bool {
            match (a, b) {
                (_, None) => false,
                (Some(a), Some(b)) => {
                    let mut changed = false;
                    assert_eq!(a.len(), b.len());
                    for (at, bt) in a.iter_mut().zip(b.iter()) {
                        assert_eq!(at.scalar_type, bt.scalar_type);
                        if !at.nullable && bt.nullable {
                            at.nullable = true;
                            changed = true;
                        }
                    }
                    changed
                }
                (a, b) => {
                    *a = b;
                    true
                }
            }
        }
    }
}

/// Expression unique keys
mod unique_keys {

    use super::arity::Arity;
    use super::{Analysis, Derived, DerivedBuilder, Lattice};
    use mz_expr::MirRelationExpr;

    /// Analysis that determines the unique keys of relation expressions.
    ///
    /// The analysis value is a `Vec<Vec<usize>>`, which should be interpreted as a list
    /// of sets of column identifiers, each set of which has the property that there is at
    /// most one instance of each assignment of values to those columns.
    ///
    /// The sets are minimal, in that any superset of another set is removed from the list.
    /// Any superset of unique key columns are also unique key columns.
    #[derive(Debug)]
    pub struct UniqueKeys;

    impl Analysis for UniqueKeys {
        type Value = Vec<Vec<usize>>;

        fn announce_dependencies(builder: &mut DerivedBuilder) {
            builder.require(Arity);
        }

        fn derive(
            &self,
            expr: &MirRelationExpr,
            index: usize,
            results: &[Self::Value],
            depends: &Derived,
        ) -> Self::Value {
            let mut offsets = depends
                .children_of_rev(index, expr.children().count())
                .collect::<Vec<_>>();
            offsets.reverse();

            match expr {
                MirRelationExpr::Get {
                    id: mz_expr::Id::Local(i),
                    typ,
                    ..
                } => {
                    // We have information from `typ` and from the analysis.
                    // We should "join" them, unioning and reducing the keys.
                    let mut keys = typ.keys.clone();
                    if let Some(o) = depends.bindings().get(i) {
                        if let Some(ks) = results.get(*o) {
                            for k in ks.iter() {
                                antichain_insert(&mut keys, k.clone());
                            }
                            keys.extend(ks.iter().cloned());
                            keys.sort();
                            keys.dedup();
                        }
                    }
                    keys
                }
                _ => {
                    let arity = depends.results::<Arity>();
                    expr.keys_with_input_keys(
                        offsets.iter().map(|o| arity[*o]),
                        offsets.iter().map(|o| &results[*o]),
                    )
                }
            }
        }

        fn lattice() -> Option<Box<dyn Lattice<Self::Value>>> {
            Some(Box::new(UKLattice))
        }
    }

    fn antichain_insert(into: &mut Vec<Vec<usize>>, item: Vec<usize>) {
        // Insert only if there is not a dominating element of `into`.
        if into.iter().all(|key| !key.iter().all(|k| item.contains(k))) {
            into.retain(|key| !key.iter().all(|k| item.contains(k)));
            into.push(item);
        }
    }

    /// Lattice for sets of columns that define a unique key.
    ///
    /// An element `Vec<Vec<usize>>` describes all sets of columns `Vec<usize>` that are a
    /// superset of some set of columns in the lattice element.
    struct UKLattice;

    impl Lattice<Vec<Vec<usize>>> for UKLattice {
        fn top(&self) -> Vec<Vec<usize>> {
            vec![vec![]]
        }
        fn meet_assign(&self, a: &mut Vec<Vec<usize>>, b: Vec<Vec<usize>>) -> bool {
            a.sort();
            a.dedup();
            let mut c = Vec::new();
            for cols_a in a.iter_mut() {
                cols_a.sort();
                cols_a.dedup();
                for cols_b in b.iter() {
                    let mut cols_c = cols_a.iter().chain(cols_b).cloned().collect::<Vec<_>>();
                    cols_c.sort();
                    cols_c.dedup();
                    antichain_insert(&mut c, cols_c);
                }
            }
            c.sort();
            c.dedup();
            std::mem::swap(a, &mut c);
            a != &mut c
        }
    }
}

/// Determines if accumulated frequences can be negative.
///
/// This analysis assumes that globally identified collection have the property, and it is
/// incorrect to apply it to expressions that reference external collections that may have
/// negative accumulations.
mod non_negative {

    use super::{Analysis, Derived, Lattice};
    use crate::analysis::common_lattice::BoolLattice;
    use mz_expr::{Id, MirRelationExpr};

    /// Analysis that determines if all accumulations at all times are non-negative.
    ///
    /// The analysis assumes that `Id::Global` references only refer to non-negative collections.
    #[derive(Debug)]
    pub struct NonNegative;

    impl Analysis for NonNegative {
        type Value = bool;

        fn derive(
            &self,
            expr: &MirRelationExpr,
            index: usize,
            results: &[Self::Value],
            depends: &Derived,
        ) -> Self::Value {
            match expr {
                MirRelationExpr::Constant { rows, .. } => rows
                    .as_ref()
                    .map(|r| r.iter().all(|(_, diff)| *diff >= mz_repr::Diff::ZERO))
                    .unwrap_or(true),
                MirRelationExpr::Get { id, .. } => match id {
                    Id::Local(id) => {
                        let index = *depends
                            .bindings()
                            .get(id)
                            .expect("Dependency info not found");
                        *results.get(index).unwrap_or(&false)
                    }
                    Id::Global(_) => true,
                },
                // Negate must be false unless input is "non-positive".
                MirRelationExpr::Negate { .. } => false,
                // Threshold ensures non-negativity.
                MirRelationExpr::Threshold { .. } => true,
                // Reduce errors on negative input.
                MirRelationExpr::Reduce { .. } => true,
                MirRelationExpr::Join { .. } => {
                    // If all inputs are non-negative, the join is non-negative.
                    depends
                        .children_of_rev(index, expr.children().count())
                        .all(|off| results[off])
                }
                MirRelationExpr::Union { base, inputs } => {
                    // If all inputs are non-negative, the union is non-negative.
                    let all_non_negative = depends
                        .children_of_rev(index, expr.children().count())
                        .all(|off| results[off]);

                    if all_non_negative {
                        return true;
                    }

                    // We look for the pattern `Union { base, Negate(Subset(base)) }`.
                    // TODO: take some care to ensure that union fusion does not introduce a regression.
                    if inputs.len() == 1 {
                        if let MirRelationExpr::Negate { input } = &inputs[0] {
                            // If `base` is non-negative, and `is_superset_of(base, input)`, return true.
                            // TODO: this is not correct until we have `is_superset_of` validate non-negativity
                            // as it goes, but it matches the current implementation.
                            let mut children = depends.children_of_rev(index, 2);
                            let _negate = children.next().unwrap();
                            let base_id = children.next().unwrap();
                            debug_assert_eq!(children.next(), None);
                            if results[base_id] && is_superset_of(&*base, &*input) {
                                return true;
                            }
                        }
                    }

                    false
                }
                _ => results[index - 1],
            }
        }

        fn lattice() -> Option<Box<dyn Lattice<Self::Value>>> {
            Some(Box::new(BoolLattice))
        }
    }

    /// Returns true only if `rhs.negate().union(lhs)` contains only non-negative multiplicities
    /// once consolidated.
    ///
    /// Informally, this happens when `rhs` is a multiset subset of `lhs`, meaning the multiplicity
    /// of any record in `rhs` is at most the multiplicity of the same record in `lhs`.
    ///
    /// This method recursively descends each of `lhs` and `rhs` and performs a great many equality
    /// tests, which has the potential to be quadratic. We should consider restricting its attention
    /// to `Get` identifiers, under the premise that equal AST nodes would necessarily be identified
    /// by common subexpression elimination. This requires care around recursively bound identifiers.
    ///
    /// These rules are .. somewhat arbitrary, and likely reflect observed opportunities. For example,
    /// while we do relate `distinct(filter(A)) <= distinct(A)`, we do not relate `distinct(A) <= A`.
    /// Further thoughts about the class of optimizations, and whether there should be more or fewer,
    /// can be found here: <https://github.com/MaterializeInc/database-issues/issues/4044>.
    fn is_superset_of(mut lhs: &MirRelationExpr, mut rhs: &MirRelationExpr) -> bool {
        // This implementation is iterative.
        // Before converting this implementation to recursive (e.g. to improve its accuracy)
        // make sure to use the `CheckedRecursion` struct to avoid blowing the stack.
        while lhs != rhs {
            match rhs {
                MirRelationExpr::Filter { input, .. } => rhs = &**input,
                MirRelationExpr::TopK { input, .. } => rhs = &**input,
                // Descend in both sides if the current roots are
                // projections with the same `outputs` vector.
                MirRelationExpr::Project {
                    input: rhs_input,
                    outputs: rhs_outputs,
                } => match lhs {
                    MirRelationExpr::Project {
                        input: lhs_input,
                        outputs: lhs_outputs,
                    } if lhs_outputs == rhs_outputs => {
                        rhs = &**rhs_input;
                        lhs = &**lhs_input;
                    }
                    _ => return false,
                },
                // Descend in both sides if the current roots are reduces with empty aggregates
                // on the same set of keys (that is, a distinct operation on those keys).
                MirRelationExpr::Reduce {
                    input: rhs_input,
                    group_key: rhs_group_key,
                    aggregates: rhs_aggregates,
                    monotonic: _,
                    expected_group_size: _,
                } if rhs_aggregates.is_empty() => match lhs {
                    MirRelationExpr::Reduce {
                        input: lhs_input,
                        group_key: lhs_group_key,
                        aggregates: lhs_aggregates,
                        monotonic: _,
                        expected_group_size: _,
                    } if lhs_aggregates.is_empty() && lhs_group_key == rhs_group_key => {
                        rhs = &**rhs_input;
                        lhs = &**lhs_input;
                    }
                    _ => return false,
                },
                _ => {
                    // TODO: Imagine more complex reasoning here!
                    return false;
                }
            }
        }
        true
    }
}

mod column_names {
    use std::ops::Range;
    use std::sync::Arc;

    use super::Analysis;
    use mz_expr::{AggregateFunc, Id, MirRelationExpr, MirScalarExpr, TableFunc};
    use mz_repr::GlobalId;
    use mz_repr::explain::ExprHumanizer;
    use mz_sql::ORDINALITY_COL_NAME;

    /// An abstract type denoting an inferred column name.
    #[derive(Debug, Clone)]
    pub enum ColumnName {
        /// A column with name inferred to be equal to a GlobalId schema column.
        Global(GlobalId, usize),
        /// An anonymous expression named after the top-level function name.
        Aggregate(AggregateFunc, Box<ColumnName>),
        /// A column with a name that has been saved from the original SQL query.
        Annotated(Arc<str>),
        /// An column with an unknown name.
        Unknown,
    }

    impl ColumnName {
        /// Return `true` iff the variant has an inferred name.
        pub fn is_known(&self) -> bool {
            match self {
                Self::Global(..) | Self::Aggregate(..) => true,
                // We treat annotated columns as unknown because we would rather
                // override them with inferred names, if we can.
                Self::Annotated(..) | Self::Unknown => false,
            }
        }

        /// Humanize the column to a [`String`], returns an empty [`String`] for
        /// unknown columns (or columns named `"?column?"`).
        pub fn humanize(&self, humanizer: &dyn ExprHumanizer) -> String {
            match self {
                Self::Global(id, c) => humanizer.humanize_column(*id, *c).unwrap_or_default(),
                Self::Aggregate(func, expr) => {
                    let func = func.name();
                    let expr = expr.humanize(humanizer);
                    if expr.is_empty() {
                        String::from(func)
                    } else {
                        format!("{func}_{expr}")
                    }
                }
                Self::Annotated(name) => name.to_string(),
                Self::Unknown => String::new(),
            }
        }

        /// Clone this column name if it is known, otherwise try to use the provided
        /// name if it is available.
        pub fn cloned_or_annotated(&self, name: &Option<Arc<str>>) -> Self {
            match self {
                Self::Global(..) | Self::Aggregate(..) | Self::Annotated(..) => self.clone(),
                Self::Unknown => name
                    .as_ref()
                    .filter(|name| name.as_ref() != "\"?column?\"")
                    .map_or_else(|| Self::Unknown, |name| Self::Annotated(Arc::clone(name))),
            }
        }
    }

    /// Compute the column types of each subtree of a [MirRelationExpr] from the
    /// bottom-up.
    #[derive(Debug)]
    pub struct ColumnNames;

    impl ColumnNames {
        /// fallback schema consisting of ordinal column names: #0, #1, ...
        fn anonymous(range: Range<usize>) -> impl Iterator<Item = ColumnName> {
            range.map(|_| ColumnName::Unknown)
        }

        /// fallback schema consisting of ordinal column names: #0, #1, ...
        fn extend_with_scalars(column_names: &mut Vec<ColumnName>, scalars: &Vec<MirScalarExpr>) {
            for scalar in scalars {
                column_names.push(match scalar {
                    MirScalarExpr::Column(c, name) => column_names[*c].cloned_or_annotated(&name.0),
                    _ => ColumnName::Unknown,
                });
            }
        }
    }

    impl Analysis for ColumnNames {
        type Value = Vec<ColumnName>;

        fn derive(
            &self,
            expr: &MirRelationExpr,
            index: usize,
            results: &[Self::Value],
            depends: &crate::analysis::Derived,
        ) -> Self::Value {
            use MirRelationExpr::*;

            match expr {
                Constant { rows: _, typ } => {
                    // Fallback to an anonymous schema for constants.
                    ColumnNames::anonymous(0..typ.arity()).collect()
                }
                Get {
                    id: Id::Global(id),
                    typ,
                    access_strategy: _,
                } => {
                    // Emit ColumnName::Global instances for each column in the
                    // `Get` type. Those can be resolved to real names later when an
                    // ExpressionHumanizer is available.
                    (0..typ.columns().len())
                        .map(|c| ColumnName::Global(*id, c))
                        .collect()
                }
                Get {
                    id: Id::Local(id),
                    typ,
                    access_strategy: _,
                } => {
                    let index_child = *depends.bindings().get(id).expect("id in scope");
                    if index_child < results.len() {
                        results[index_child].clone()
                    } else {
                        // Possible because we infer LetRec bindings in order. This
                        // can be improved by introducing a fixpoint loop in the
                        // Env<A>::schedule_tasks LetRec handling block.
                        ColumnNames::anonymous(0..typ.arity()).collect()
                    }
                }
                Let {
                    id: _,
                    value: _,
                    body: _,
                } => {
                    // Return the column names of the `body`.
                    results[index - 1].clone()
                }
                LetRec {
                    ids: _,
                    values: _,
                    limits: _,
                    body: _,
                } => {
                    // Return the column names of the `body`.
                    results[index - 1].clone()
                }
                Project { input: _, outputs } => {
                    // Permute the column names of the input.
                    let input_column_names = &results[index - 1];
                    let mut column_names = vec![];
                    for col in outputs {
                        column_names.push(input_column_names[*col].clone());
                    }
                    column_names
                }
                Map { input: _, scalars } => {
                    // Extend the column names of the input with anonymous columns.
                    let mut column_names = results[index - 1].clone();
                    Self::extend_with_scalars(&mut column_names, scalars);
                    column_names
                }
                FlatMap {
                    input: _,
                    func,
                    exprs: _,
                } => {
                    // Extend the column names of the input with anonymous columns.
                    let mut column_names = results[index - 1].clone();
                    let func_output_start = column_names.len();
                    let func_output_end = column_names.len() + func.output_arity();
                    column_names.extend(Self::anonymous(func_output_start..func_output_end));
                    if let TableFunc::WithOrdinality { .. } = func {
                        // We know the name of the last column
                        // TODO(ggevay): generalize this to meaningful col names for all table functions
                        **column_names.last_mut().as_mut().expect(
                            "there is at least one output column, from the WITH ORDINALITY",
                        ) = ColumnName::Annotated(ORDINALITY_COL_NAME.into());
                    }
                    column_names
                }
                Filter {
                    input: _,
                    predicates: _,
                } => {
                    // Return the column names of the `input`.
                    results[index - 1].clone()
                }
                Join {
                    inputs: _,
                    equivalences: _,
                    implementation: _,
                } => {
                    let mut input_results = depends
                        .children_of_rev(index, expr.children().count())
                        .map(|child| &results[child])
                        .collect::<Vec<_>>();
                    input_results.reverse();

                    let mut column_names = vec![];
                    for input_column_names in input_results {
                        column_names.extend(input_column_names.iter().cloned());
                    }
                    column_names
                }
                Reduce {
                    input: _,
                    group_key,
                    aggregates,
                    monotonic: _,
                    expected_group_size: _,
                } => {
                    // We clone and extend the input vector and then remove the part
                    // associated with the input at the end.
                    let mut column_names = results[index - 1].clone();
                    let input_arity = column_names.len();

                    // Infer the group key part.
                    Self::extend_with_scalars(&mut column_names, group_key);
                    // Infer the aggregates part.
                    for aggregate in aggregates.iter() {
                        // The inferred name will consist of (1) the aggregate
                        // function name and (2) the aggregate expression (iff
                        // it is a simple column reference).
                        let func = aggregate.func.clone();
                        let expr = match aggregate.expr.as_column() {
                            Some(c) => column_names.get(c).unwrap_or(&ColumnName::Unknown).clone(),
                            None => ColumnName::Unknown,
                        };
                        column_names.push(ColumnName::Aggregate(func, Box::new(expr)));
                    }
                    // Remove the prefix associated with the input
                    column_names.drain(0..input_arity);

                    column_names
                }
                TopK {
                    input: _,
                    group_key: _,
                    order_key: _,
                    limit: _,
                    offset: _,
                    monotonic: _,
                    expected_group_size: _,
                } => {
                    // Return the column names of the `input`.
                    results[index - 1].clone()
                }
                Negate { input: _ } => {
                    // Return the column names of the `input`.
                    results[index - 1].clone()
                }
                Threshold { input: _ } => {
                    // Return the column names of the `input`.
                    results[index - 1].clone()
                }
                Union { base: _, inputs: _ } => {
                    // Use the first non-empty column across all inputs.
                    let mut column_names = vec![];

                    let mut inputs_results = depends
                        .children_of_rev(index, expr.children().count())
                        .map(|child| &results[child])
                        .collect::<Vec<_>>();

                    let base_results = inputs_results.pop().unwrap();
                    inputs_results.reverse();

                    for (i, mut column_name) in base_results.iter().cloned().enumerate() {
                        for input_results in inputs_results.iter() {
                            if !column_name.is_known() && input_results[i].is_known() {
                                column_name = input_results[i].clone();
                                break;
                            }
                        }
                        column_names.push(column_name);
                    }

                    column_names
                }
                ArrangeBy { input: _, keys: _ } => {
                    // Return the column names of the `input`.
                    results[index - 1].clone()
                }
            }
        }
    }
}

mod explain {
    //! Derived Analysis framework and definitions.

    use std::collections::BTreeMap;

    use mz_expr::MirRelationExpr;
    use mz_expr::explain::{ExplainContext, HumanizedExplain, HumanizerMode};
    use mz_ore::stack::RecursionLimitError;
    use mz_repr::explain::{Analyses, AnnotatedPlan};

    use crate::analysis::equivalences::{Equivalences, HumanizedEquivalenceClasses};

    // Analyses should have shortened paths when exported.
    use super::DerivedBuilder;

    impl<'c> From<&ExplainContext<'c>> for DerivedBuilder<'c> {
        fn from(context: &ExplainContext<'c>) -> DerivedBuilder<'c> {
            let mut builder = DerivedBuilder::new(context.features);
            if context.config.subtree_size {
                builder.require(super::SubtreeSize);
            }
            if context.config.non_negative {
                builder.require(super::NonNegative);
            }
            if context.config.types {
                builder.require(super::RelationType);
            }
            if context.config.arity {
                builder.require(super::Arity);
            }
            if context.config.keys {
                builder.require(super::UniqueKeys);
            }
            if context.config.cardinality {
                builder.require(super::Cardinality::with_stats(
                    context.cardinality_stats.clone(),
                ));
            }
            if context.config.column_names || context.config.humanized_exprs {
                builder.require(super::ColumnNames);
            }
            if context.config.equivalences {
                builder.require(Equivalences);
            }
            builder
        }
    }

    /// Produce an [`AnnotatedPlan`] wrapping the given [`MirRelationExpr`] along
    /// with [`Analyses`] derived from the given context configuration.
    pub fn annotate_plan<'a>(
        plan: &'a MirRelationExpr,
        context: &'a ExplainContext,
    ) -> Result<AnnotatedPlan<'a, MirRelationExpr>, RecursionLimitError> {
        let mut annotations = BTreeMap::<&MirRelationExpr, Analyses>::default();
        let config = context.config;

        // We want to annotate the plan with analyses in the following cases:
        // 1. An Analysis was explicitly requested in the ExplainConfig.
        // 2. Humanized expressions were requested in the ExplainConfig (in which
        //    case we need to derive the ColumnNames Analysis).
        if config.requires_analyses() || config.humanized_exprs {
            // get the annotation keys
            let subtree_refs = plan.post_order_vec();
            // get the annotation values
            let builder = DerivedBuilder::from(context);
            let derived = builder.visit(plan);

            if config.subtree_size {
                for (expr, subtree_size) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::SubtreeSize>().into_iter(),
                ) {
                    let analyses = annotations.entry(expr).or_default();
                    analyses.subtree_size = Some(*subtree_size);
                }
            }
            if config.non_negative {
                for (expr, non_negative) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::NonNegative>().into_iter(),
                ) {
                    let analyses = annotations.entry(expr).or_default();
                    analyses.non_negative = Some(*non_negative);
                }
            }

            if config.arity {
                for (expr, arity) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::Arity>().into_iter(),
                ) {
                    let analyses = annotations.entry(expr).or_default();
                    analyses.arity = Some(*arity);
                }
            }

            if config.types {
                for (expr, types) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::RelationType>().into_iter(),
                ) {
                    let analyses = annotations.entry(expr).or_default();
                    analyses.types = Some(types.clone());
                }
            }

            if config.keys {
                for (expr, keys) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::UniqueKeys>().into_iter(),
                ) {
                    let analyses = annotations.entry(expr).or_default();
                    analyses.keys = Some(keys.clone());
                }
            }

            if config.cardinality {
                for (expr, card) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::Cardinality>().into_iter(),
                ) {
                    let analyses = annotations.entry(expr).or_default();
                    analyses.cardinality = Some(card.to_string());
                }
            }

            if config.column_names || config.humanized_exprs {
                for (expr, column_names) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::ColumnNames>().into_iter(),
                ) {
                    let analyses = annotations.entry(expr).or_default();
                    let value = column_names
                        .iter()
                        .map(|column_name| column_name.humanize(context.humanizer))
                        .collect();
                    analyses.column_names = Some(value);
                }
            }

            if config.equivalences {
                for (expr, equivs) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<Equivalences>().into_iter(),
                ) {
                    let analyses = annotations.entry(expr).or_default();
                    analyses.equivalences = Some(match equivs.as_ref() {
                        Some(equivs) => HumanizedEquivalenceClasses {
                            equivalence_classes: equivs,
                            cols: analyses.column_names.as_ref(),
                            mode: HumanizedExplain::new(config.redacted),
                        }
                        .to_string(),
                        None => "<empty collection>".to_string(),
                    });
                }
            }
        }

        Ok(AnnotatedPlan { plan, annotations })
    }
}

/// Definition and helper structs for the [`Cardinality`] Analysis.
mod cardinality {
    use std::collections::{BTreeMap, BTreeSet};

    use mz_expr::{
        BinaryFunc, Id, JoinImplementation, MirRelationExpr, MirScalarExpr, TableFunc, UnaryFunc,
        VariadicFunc,
    };
    use mz_ore::cast::{CastFrom, CastLossy, TryCastFrom};
    use mz_repr::GlobalId;

    use ordered_float::OrderedFloat;

    use super::{Analysis, Arity, SubtreeSize, UniqueKeys};

    /// Compute the estimated cardinality of each subtree of a [MirRelationExpr] from the bottom up.
    #[allow(missing_debug_implementations)]
    pub struct Cardinality {
        /// Cardinalities for globally named entities
        pub stats: BTreeMap<GlobalId, usize>,
    }

    impl Cardinality {
        /// A cardinality estimator with provided statistics for the given global identifiers
        pub fn with_stats(stats: BTreeMap<GlobalId, usize>) -> Self {
            Cardinality { stats }
        }
    }

    impl Default for Cardinality {
        fn default() -> Self {
            Cardinality {
                stats: BTreeMap::new(),
            }
        }
    }

    /// Cardinality estimates
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub enum CardinalityEstimate {
        Unknown,
        Estimate(OrderedFloat<f64>),
    }

    impl CardinalityEstimate {
        pub fn max(lhs: CardinalityEstimate, rhs: CardinalityEstimate) -> CardinalityEstimate {
            use CardinalityEstimate::*;
            match (lhs, rhs) {
                (Estimate(lhs), Estimate(rhs)) => Estimate(std::cmp::max(lhs, rhs)),
                _ => Unknown,
            }
        }

        pub fn rounded(&self) -> Option<usize> {
            match self {
                CardinalityEstimate::Estimate(OrderedFloat(f)) => {
                    let rounded = f.ceil();
                    let flattened = usize::cast_from(
                        u64::try_cast_from(rounded)
                            .expect("positive and representable cardinality estimate"),
                    );

                    Some(flattened)
                }
                CardinalityEstimate::Unknown => None,
            }
        }
    }

    impl std::ops::Add for CardinalityEstimate {
        type Output = CardinalityEstimate;

        fn add(self, rhs: Self) -> Self::Output {
            use CardinalityEstimate::*;
            match (self, rhs) {
                (Estimate(lhs), Estimate(rhs)) => Estimate(lhs + rhs),
                _ => Unknown,
            }
        }
    }

    impl std::ops::Sub for CardinalityEstimate {
        type Output = CardinalityEstimate;

        fn sub(self, rhs: Self) -> Self::Output {
            use CardinalityEstimate::*;
            match (self, rhs) {
                (Estimate(lhs), Estimate(rhs)) => Estimate(lhs - rhs),
                _ => Unknown,
            }
        }
    }

    impl std::ops::Sub<CardinalityEstimate> for f64 {
        type Output = CardinalityEstimate;

        fn sub(self, rhs: CardinalityEstimate) -> Self::Output {
            use CardinalityEstimate::*;
            if let Estimate(OrderedFloat(rhs)) = rhs {
                Estimate(OrderedFloat(self - rhs))
            } else {
                Unknown
            }
        }
    }

    impl std::ops::Mul for CardinalityEstimate {
        type Output = CardinalityEstimate;

        fn mul(self, rhs: Self) -> Self::Output {
            use CardinalityEstimate::*;
            match (self, rhs) {
                (Estimate(lhs), Estimate(rhs)) => Estimate(lhs * rhs),
                _ => Unknown,
            }
        }
    }

    impl std::ops::Mul<f64> for CardinalityEstimate {
        type Output = CardinalityEstimate;

        fn mul(self, rhs: f64) -> Self::Output {
            if let CardinalityEstimate::Estimate(OrderedFloat(lhs)) = self {
                CardinalityEstimate::Estimate(OrderedFloat(lhs * rhs))
            } else {
                CardinalityEstimate::Unknown
            }
        }
    }

    impl std::ops::Div for CardinalityEstimate {
        type Output = CardinalityEstimate;

        fn div(self, rhs: Self) -> Self::Output {
            use CardinalityEstimate::*;
            match (self, rhs) {
                (Estimate(lhs), Estimate(rhs)) => Estimate(lhs / rhs),
                _ => Unknown,
            }
        }
    }

    impl std::ops::Div<f64> for CardinalityEstimate {
        type Output = CardinalityEstimate;

        fn div(self, rhs: f64) -> Self::Output {
            use CardinalityEstimate::*;
            if let Estimate(lhs) = self {
                Estimate(lhs / OrderedFloat(rhs))
            } else {
                Unknown
            }
        }
    }

    impl std::iter::Sum for CardinalityEstimate {
        fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
            iter.fold(CardinalityEstimate::from(0.0), |acc, elt| acc + elt)
        }
    }

    impl std::iter::Product for CardinalityEstimate {
        fn product<I: Iterator<Item = Self>>(iter: I) -> Self {
            iter.fold(CardinalityEstimate::from(1.0), |acc, elt| acc * elt)
        }
    }

    impl From<usize> for CardinalityEstimate {
        fn from(value: usize) -> Self {
            Self::Estimate(OrderedFloat(f64::cast_lossy(value)))
        }
    }

    impl From<f64> for CardinalityEstimate {
        fn from(value: f64) -> Self {
            Self::Estimate(OrderedFloat(value))
        }
    }

    /// The default selectivity for predicates we know nothing about.
    ///
    /// But see also expr/src/scalar.rs for `FilterCharacteristics::worst_case_scaling_factor()` for a more nuanced take.
    pub const WORST_CASE_SELECTIVITY: OrderedFloat<f64> = OrderedFloat(0.1);

    // This section defines how we estimate cardinality for each syntactic construct.
    //
    // We split it up into functions to make it all a bit more tractable to work with.
    impl Cardinality {
        fn flat_map(&self, tf: &TableFunc, input: CardinalityEstimate) -> CardinalityEstimate {
            match tf {
                TableFunc::Wrap { types, width } => {
                    input * (f64::cast_lossy(types.len()) / f64::cast_lossy(*width))
                }
                _ => {
                    // TODO(mgree) what explosion factor should we make up?
                    input * CardinalityEstimate::from(4.0)
                }
            }
        }

        fn predicate(
            &self,
            predicate_expr: &MirScalarExpr,
            unique_columns: &BTreeSet<usize>,
        ) -> OrderedFloat<f64> {
            let index_selectivity = |expr: &MirScalarExpr| -> Option<OrderedFloat<f64>> {
                match expr {
                    MirScalarExpr::Column(col, _) => {
                        if unique_columns.contains(col) {
                            // TODO(mgree): when we have index cardinality statistics, they should go here when `expr` is a `MirScalarExpr::Column` that's in `unique_columns`
                            None
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            };

            match predicate_expr {
                MirScalarExpr::Column(_, _)
                | MirScalarExpr::Literal(_, _)
                | MirScalarExpr::CallUnmaterializable(_) => OrderedFloat(1.0),
                MirScalarExpr::CallUnary { func, expr } => match func {
                    UnaryFunc::Not(_) => OrderedFloat(1.0) - self.predicate(expr, unique_columns),
                    UnaryFunc::IsTrue(_) | UnaryFunc::IsFalse(_) => OrderedFloat(0.5),
                    UnaryFunc::IsNull(_) => {
                        if let Some(icard) = index_selectivity(expr) {
                            icard
                        } else {
                            WORST_CASE_SELECTIVITY
                        }
                    }
                    _ => WORST_CASE_SELECTIVITY,
                },
                MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                    match func {
                        BinaryFunc::Eq => {
                            match (index_selectivity(expr1), index_selectivity(expr2)) {
                                (Some(isel1), Some(isel2)) => std::cmp::max(isel1, isel2),
                                (Some(isel), None) | (None, Some(isel)) => isel,
                                (None, None) => WORST_CASE_SELECTIVITY,
                            }
                        }
                        // 1.0 - the Eq case
                        BinaryFunc::NotEq => {
                            match (index_selectivity(expr1), index_selectivity(expr2)) {
                                (Some(isel1), Some(isel2)) => {
                                    OrderedFloat(1.0) - std::cmp::max(isel1, isel2)
                                }
                                (Some(isel), None) | (None, Some(isel)) => OrderedFloat(1.0) - isel,
                                (None, None) => OrderedFloat(1.0) - WORST_CASE_SELECTIVITY,
                            }
                        }
                        BinaryFunc::Lt | BinaryFunc::Lte | BinaryFunc::Gt | BinaryFunc::Gte => {
                            // TODO(mgree) if we have high/low key values and one of the columns is an index, we can do better
                            OrderedFloat(0.33)
                        }
                        _ => OrderedFloat(1.0), // TOOD(mgree): are there other interesting cases?
                    }
                }
                MirScalarExpr::CallVariadic { func, exprs } => match func {
                    VariadicFunc::And => exprs
                        .iter()
                        .map(|expr| self.predicate(expr, unique_columns))
                        .product(),
                    VariadicFunc::Or => {
                        // TODO(mgree): BETWEEN will get compiled down to an AND of appropriate bounds---we could try to detect it and be clever

                        // F(expr1 OR expr2) = F(expr1) + F(expr2) - F(expr1) * F(expr2), but generalized
                        let mut exprs = exprs.into_iter();

                        let mut expr1;

                        if let Some(first) = exprs.next() {
                            expr1 = self.predicate(first, unique_columns);
                        } else {
                            return OrderedFloat(1.0);
                        }

                        for expr2 in exprs {
                            let expr2 = self.predicate(expr2, unique_columns);
                            expr1 = expr1 + expr2 - expr1 * expr2;
                        }
                        expr1
                    }
                    _ => OrderedFloat(1.0),
                },
                MirScalarExpr::If { cond: _, then, els } => std::cmp::max(
                    self.predicate(then, unique_columns),
                    self.predicate(els, unique_columns),
                ),
            }
        }

        fn filter(
            &self,
            predicates: &Vec<MirScalarExpr>,
            keys: &Vec<Vec<usize>>,
            input: CardinalityEstimate,
        ) -> CardinalityEstimate {
            // TODO(mgree): should we try to do something for indices built on multiple columns?
            let mut unique_columns = BTreeSet::new();
            for key in keys {
                if key.len() == 1 {
                    unique_columns.insert(key[0]);
                }
            }

            let mut estimate = input;
            for expr in predicates {
                let selectivity = self.predicate(expr, &unique_columns);
                debug_assert!(
                    OrderedFloat(0.0) <= selectivity && selectivity <= OrderedFloat(1.0),
                    "predicate selectivity {selectivity} should be in the range [0,1]"
                );
                estimate = estimate * selectivity.0;
            }

            estimate
        }

        fn join(
            &self,
            equivalences: &Vec<Vec<MirScalarExpr>>,
            _implementation: &JoinImplementation,
            unique_columns: BTreeMap<usize, usize>,
            mut inputs: Vec<CardinalityEstimate>,
        ) -> CardinalityEstimate {
            if inputs.is_empty() {
                return CardinalityEstimate::from(0.0);
            }

            for equiv in equivalences {
                // those sources which have a unique key
                let mut unique_sources = BTreeSet::new();
                let mut all_unique = true;

                for expr in equiv {
                    if let MirScalarExpr::Column(col, _) = expr {
                        if let Some(idx) = unique_columns.get(col) {
                            unique_sources.insert(*idx);
                        } else {
                            all_unique = false;
                        }
                    } else {
                        all_unique = false;
                    }
                }

                // no unique columns in this equivalence
                if unique_sources.is_empty() {
                    continue;
                }

                // ALL unique columns in this equivalence
                if all_unique {
                    // these inputs have unique keys for _all_ of the equivalence, so they're a bound on how many rows we'll get from those sources
                    // we'll find the leftmost such input and use it to hold the minimum; the other sources we set to 1.0 (so they have no effect)
                    let mut sources = unique_sources.iter();

                    let lhs_idx = *sources.next().unwrap();
                    let mut lhs =
                        std::mem::replace(&mut inputs[lhs_idx], CardinalityEstimate::from(1.0));
                    for &rhs_idx in sources {
                        let rhs =
                            std::mem::replace(&mut inputs[rhs_idx], CardinalityEstimate::from(1.0));
                        lhs = CardinalityEstimate::min(lhs, rhs);
                    }

                    inputs[lhs_idx] = lhs;

                    // best option! go look at the next equivalence
                    continue;
                }

                // some unique columns in this equivalence
                for idx in unique_sources {
                    // when joining R and S on R.x = S.x, if R.x is unique and S.x is not, we're bounded above by the cardinality of S
                    inputs[idx] = CardinalityEstimate::from(1.0);
                }
            }

            let mut product = CardinalityEstimate::from(1.0);
            for input in inputs {
                product = product * input;
            }
            product
        }

        fn reduce(
            &self,
            group_key: &Vec<MirScalarExpr>,
            expected_group_size: &Option<u64>,
            input: CardinalityEstimate,
        ) -> CardinalityEstimate {
            // TODO(mgree): if no `group_key` is present, we can do way better

            if let Some(group_size) = expected_group_size {
                input / f64::cast_lossy(*group_size)
            } else if group_key.is_empty() {
                CardinalityEstimate::from(1.0)
            } else {
                // in the worst case, every row is its own group
                input
            }
        }

        fn topk(
            &self,
            group_key: &Vec<usize>,
            limit: &Option<MirScalarExpr>,
            expected_group_size: &Option<u64>,
            input: CardinalityEstimate,
        ) -> CardinalityEstimate {
            // TODO: support simple arithmetic expressions
            let k = limit
                .as_ref()
                .and_then(|l| l.as_literal_int64())
                .map_or(1, |l| std::cmp::max(0, l));

            if let Some(group_size) = expected_group_size {
                input * (f64::cast_lossy(k) / f64::cast_lossy(*group_size))
            } else if group_key.is_empty() {
                CardinalityEstimate::from(f64::cast_lossy(k))
            } else {
                // in the worst case, every row is its own group
                input.clone()
            }
        }

        fn threshold(&self, input: CardinalityEstimate) -> CardinalityEstimate {
            // worst case scaling factor is 1
            input.clone()
        }
    }

    impl Analysis for Cardinality {
        type Value = CardinalityEstimate;

        fn announce_dependencies(builder: &mut crate::analysis::DerivedBuilder) {
            builder.require(crate::analysis::Arity);
            builder.require(crate::analysis::UniqueKeys);
        }

        fn derive(
            &self,
            expr: &MirRelationExpr,
            index: usize,
            results: &[Self::Value],
            depends: &crate::analysis::Derived,
        ) -> Self::Value {
            use MirRelationExpr::*;

            let sizes = depends.as_view().results::<SubtreeSize>();
            let arity = depends.as_view().results::<Arity>();
            let keys = depends.as_view().results::<UniqueKeys>();

            match expr {
                Constant { rows, .. } => {
                    CardinalityEstimate::from(rows.as_ref().map_or_else(|_| 0, |v| v.len()))
                }
                Get { id, .. } => match id {
                    Id::Local(id) => depends
                        .bindings()
                        .get(id)
                        .and_then(|id| results.get(*id))
                        .copied()
                        .unwrap_or(CardinalityEstimate::Unknown),
                    Id::Global(id) => self
                        .stats
                        .get(id)
                        .copied()
                        .map(CardinalityEstimate::from)
                        .unwrap_or(CardinalityEstimate::Unknown),
                },
                Let { .. } | Project { .. } | Map { .. } | ArrangeBy { .. } | Negate { .. } => {
                    results[index - 1].clone()
                }
                LetRec { .. } =>
                // TODO(mgree): implement a recurrence-based approach (or at least identify common idioms, e.g. transitive closure)
                {
                    CardinalityEstimate::Unknown
                }
                Union { base: _, inputs: _ } => depends
                    .children_of_rev(index, expr.children().count())
                    .map(|off| results[off].clone())
                    .sum(),
                FlatMap { func, .. } => {
                    let input = results[index - 1];
                    self.flat_map(func, input)
                }
                Filter { predicates, .. } => {
                    let input = results[index - 1];
                    let keys = depends.results::<UniqueKeys>();
                    let keys = &keys[index - 1];
                    self.filter(predicates, keys, input)
                }
                Join {
                    equivalences,
                    implementation,
                    inputs,
                    ..
                } => {
                    let mut input_results = Vec::with_capacity(inputs.len());

                    // maps a column to the index in `inputs` that it belongs to
                    let mut unique_columns = BTreeMap::new();
                    let mut key_offset = 0;

                    let mut offset = 1;
                    for idx in 0..inputs.len() {
                        let input = results[index - offset];
                        input_results.push(input);

                        let arity = arity[index - offset];
                        let keys = &keys[index - offset];
                        for key in keys {
                            if key.len() == 1 {
                                unique_columns.insert(key_offset + key[0], idx);
                            }
                        }
                        key_offset += arity;

                        offset += &sizes[index - offset];
                    }

                    self.join(equivalences, implementation, unique_columns, input_results)
                }
                Reduce {
                    group_key,
                    expected_group_size,
                    ..
                } => {
                    let input = results[index - 1];
                    self.reduce(group_key, expected_group_size, input)
                }
                TopK {
                    group_key,
                    limit,
                    expected_group_size,
                    ..
                } => {
                    let input = results[index - 1];
                    self.topk(group_key, limit, expected_group_size, input)
                }
                Threshold { .. } => {
                    let input = results[index - 1];
                    self.threshold(input)
                }
            }
        }
    }

    impl std::fmt::Display for CardinalityEstimate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                CardinalityEstimate::Estimate(OrderedFloat(estimate)) => write!(f, "{estimate}"),
                CardinalityEstimate::Unknown => write!(f, "<UNKNOWN>"),
            }
        }
    }
}

mod common_lattice {
    use crate::analysis::Lattice;

    pub struct BoolLattice;

    impl Lattice<bool> for BoolLattice {
        // `true` > `false`.
        fn top(&self) -> bool {
            true
        }
        // `false` is the greatest lower bound. `into` changes if it's true and `item` is false.
        fn meet_assign(&self, into: &mut bool, item: bool) -> bool {
            let changed = *into && !item;
            *into = *into && item;
            changed
        }
    }
}
