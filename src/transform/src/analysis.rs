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

use mz_expr::MirRelationExpr;

pub use common::{Derived, DerivedBuilder, DerivedView};

pub use arity::Arity;
pub use cardinality::Cardinality;
pub use column_names::{ColumnName, ColumnNames};
pub use explain::annotate_plan;
pub use non_negative::NonNegative;
pub use subtree::SubtreeSize;
pub use types::RelationType;
pub use unique_keys::UniqueKeys;

/// An analysis that can be applied bottom-up to a `MirRelationExpr`.
pub trait Analysis: 'static {
    /// The type of value this analysis associates with an expression.
    type Value: std::fmt::Debug;
    /// Announce any depencies this analysis has on other analyses.
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
    /// The return result will be associated with this expression for this analysis,
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
    use mz_repr::optimize::OptimizerFeatures;

    use super::subtree::SubtreeSize;
    use super::Analysis;

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
        pub fn results<A: Analysis>(&self) -> Option<&[A::Value]> {
            let type_id = TypeId::of::<Bundle<A>>();
            if let Some(bundle) = self.analyses.get(&type_id) {
                if let Some(bundle) = bundle.as_any().downcast_ref::<Bundle<A>>() {
                    return Some(&bundle.results[..]);
                }
            }
            None
        }
        /// Return an owned version of analysis results derived so far,
        /// replacing them with an empty vector.
        pub fn take_results<A: Analysis>(&mut self) -> Option<Vec<A::Value>> {
            let type_id = TypeId::of::<Bundle<A>>();
            if let Some(bundle) = self.analyses.get_mut(&type_id) {
                if let Some(bundle) = bundle.as_any_mut().downcast_mut::<Bundle<A>>() {
                    return Some(std::mem::take(&mut bundle.results));
                }
            }
            None
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
            let sizes = self.results::<SubtreeSize>().expect("SubtreeSize missing");
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
                upper: self
                    .results::<SubtreeSize>()
                    .expect("SubtreeSize missing")
                    .len(),
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
            self.results::<A>().and_then(|slice| slice.last())
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
                .and_then(|index| self.derived.results::<A>().and_then(|r| r.get(*index)))
        }

        /// The results for expression and its children.
        ///
        /// The results for the expression itself will be the last element.
        pub fn results<A: Analysis>(&self) -> Option<&'a [A::Value]> {
            self.derived
                .results::<A>()
                .map(|slice| &slice[self.lower..self.upper])
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
            let sizes = self.results::<SubtreeSize>().expect("SubtreeSize missing");
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
                    // TODO: Find a better way to identify `A`.
                    panic!("Cyclic dependency detected: {:?}", type_id);
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
                        assert!(prior.is_none(), "Shadowing not allowed");
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
        /// Upcasts `self` to a `&mut dyn Any`.
        ///
        /// NOTE: This is required until <https://github.com/rust-lang/rfcs/issues/2765> is fixed
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
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
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    impl<A: Analysis> Bundle<A> {
        /// Analysis that starts optimistically but is only correct at a fixed point.
        ///
        /// Will fail out to `analyse_pessimistic` if the lattice is missing, or `self.fuel` is exhausted.
        fn analyse_optimistic(
            &mut self,
            exprs: &[&MirRelationExpr],
            lower: usize,
            upper: usize,
            depends: &Derived,
            lattice: &dyn crate::analysis::Lattice<A::Value>,
        ) -> Result<bool, ()> {
            if let MirRelationExpr::LetRec { .. } = &exprs[upper - 1] {
                let sizes = depends
                    .results::<SubtreeSize>()
                    .expect("SubtreeSize required");
                let mut values = depends
                    .children_of_rev(upper - 1, exprs[upper - 1].children().count())
                    .skip(1)
                    .collect::<Vec<_>>();
                values.reverse();

                // Visit each child, and track whether any new information emerges.
                // Repeat, as long as new information continues to emerge.
                let mut new_information = true;
                while new_information {
                    // Bail out if we have done too many `LetRec` passes in this analysis.
                    self.fuel -= 1;
                    if self.fuel == 0 {
                        return Err(());
                    }

                    new_information = false;
                    // Visit non-body children (values).
                    for child in values.iter() {
                        new_information = self.analyse_optimistic(
                            exprs,
                            *child + 1 - sizes[*child],
                            *child + 1,
                            depends,
                            lattice,
                        )? || new_information;
                    }
                }
                // Visit `body` and then the `LetRec` and return whether it evolved.
                let body = upper - 2;
                self.analyse_optimistic(exprs, body + 1 - sizes[body], body + 1, depends, lattice)?;
                let value = self.derive(exprs[upper - 1], upper - 1, depends);
                Ok(lattice.meet_assign(&mut self.results[upper - 1], value))
            } else {
                // If not a `LetRec`, we still want to revisit results and update them with meet.
                let mut changed = false;
                for index in lower..upper {
                    let value = self.derive(exprs[index], index, depends);
                    changed = lattice.meet_assign(&mut self.results[index], value);
                }
                Ok(changed)
            }
        }

        /// Analysis that starts conservatively and can be stopped at any point.
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

    use super::{Analysis, Derived};
    use mz_expr::MirRelationExpr;
    use mz_repr::ColumnType;

    /// Analysis that determines the type of relation expressions.
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

            if offsets.iter().all(|o| o.is_some()) {
                let input_cols = offsets.into_iter().rev().map(|o| o.as_ref().unwrap());
                let subtree_column_types = expr.try_col_with_input_cols(input_cols);
                subtree_column_types.ok()
            } else {
                None
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
                    let arity = depends.results::<Arity>().unwrap();
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
                    .map(|r| r.iter().all(|(_, diff)| diff >= &0))
                    .unwrap_or(true),
                MirRelationExpr::Get { id, .. } => match id {
                    Id::Local(id) => depends
                        .bindings()
                        .get(id)
                        .map(|off| results[*off])
                        .unwrap_or(false),
                    Id::Global(_) => true,
                },
                // Negate must be false unless input is "non-positive".
                MirRelationExpr::Negate { .. } => false,
                // Threshold ensures non-negativity.
                MirRelationExpr::Threshold { .. } => true,
                MirRelationExpr::Join { .. } | MirRelationExpr::Union { .. } => {
                    // These two cases require all of their inputs to be non-negative.
                    depends
                        .children_of_rev(index, expr.children().count())
                        .all(|off| results[off])
                }
                _ => results[index - 1],
            }
        }

        fn lattice() -> Option<Box<dyn Lattice<Self::Value>>> {
            Some(Box::new(NNLattice))
        }
    }

    struct NNLattice;

    impl Lattice<bool> for NNLattice {
        fn top(&self) -> bool {
            true
        }
        fn meet_assign(&self, into: &mut bool, item: bool) -> bool {
            let changed = *into && !item;
            *into = *into && item;
            changed
        }
    }
}

mod column_names {
    use std::ops::Range;

    use super::Analysis;
    use mz_expr::{AggregateFunc, Id, MirRelationExpr, MirScalarExpr};
    use mz_repr::explain::ExprHumanizer;
    use mz_repr::GlobalId;

    /// An abstract type denoting an inferred column name.
    #[derive(Debug, Clone)]
    pub enum ColumnName {
        /// A column with name inferred to be equal to a GlobalId schema column.
        Global(GlobalId, usize),
        /// An anonymous expression named after the top-level function name.
        Aggregate(AggregateFunc, Box<ColumnName>),
        /// An column with an unknown name.
        Unknown,
    }

    impl ColumnName {
        /// Return `true` iff this the variant is not unknown.
        pub fn is_known(&self) -> bool {
            matches!(self, Self::Global(..) | Self::Aggregate(..))
        }

        /// Humanize the column to a [`String`], returns an empty [`String`] for
        /// unknown columns.
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
                Self::Unknown => String::new(),
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
                    MirScalarExpr::Column(c) => column_names[*c].clone(),
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
                    // Emit ColumnName::Global instanceds for each column in the
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
    //! Derived attributes framework and definitions.

    use std::collections::BTreeMap;

    use mz_expr::explain::ExplainContext;
    use mz_expr::MirRelationExpr;
    use mz_ore::stack::RecursionLimitError;
    use mz_repr::explain::{AnnotatedPlan, Attributes};

    // Attributes should have shortened paths when exported.
    use super::cardinality::HumanizedSymbolicExpression;
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
                builder.require(super::Cardinality::default());
            }
            if context.config.column_names || context.config.humanized_exprs {
                builder.require(super::ColumnNames);
            }
            builder
        }
    }

    /// Produce an [`AnnotatedPlan`] wrapping the given [`MirRelationExpr`] along
    /// with [`Attributes`] derived from the given context configuration.
    pub fn annotate_plan<'a>(
        plan: &'a MirRelationExpr,
        context: &'a ExplainContext,
    ) -> Result<AnnotatedPlan<'a, MirRelationExpr>, RecursionLimitError> {
        let mut annotations = BTreeMap::<&MirRelationExpr, Attributes>::default();
        let config = context.config;

        // We want to annotate the plan with attributes in the following cases:
        // 1. An attribute was explicitly requested in the ExplainConfig.
        // 2. Humanized expressions were requested in the ExplainConfig (in which
        //    case we need to derive the ColumnNames attribute).
        if config.requires_attributes() || config.humanized_exprs {
            // get the annotation keys
            let subtree_refs = plan.post_order_vec();
            // get the annotation values
            let builder = DerivedBuilder::from(context);
            let derived = builder.visit(plan);

            if config.subtree_size {
                for (expr, subtree_size) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::SubtreeSize>().unwrap().into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.subtree_size = Some(*subtree_size);
                }
            }
            if config.non_negative {
                for (expr, non_negative) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::NonNegative>().unwrap().into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.non_negative = Some(*non_negative);
                }
            }

            if config.arity {
                for (expr, arity) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::Arity>().unwrap().into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.arity = Some(*arity);
                }
            }

            if config.types {
                for (expr, types) in std::iter::zip(
                    subtree_refs.iter(),
                    derived
                        .results::<super::RelationType>()
                        .unwrap()
                        .into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.types = Some(types.clone());
                }
            }

            if config.keys {
                for (expr, keys) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::UniqueKeys>().unwrap().into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    attrs.keys = Some(keys.clone());
                }
            }

            if config.cardinality {
                for (expr, card) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::Cardinality>().unwrap().into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    let value = HumanizedSymbolicExpression::new(card, context.humanizer);
                    attrs.cardinality = Some(value.to_string());
                }
            }

            if config.column_names || config.humanized_exprs {
                for (expr, column_names) in std::iter::zip(
                    subtree_refs.iter(),
                    derived.results::<super::ColumnNames>().unwrap().into_iter(),
                ) {
                    let attrs = annotations.entry(expr).or_default();
                    let value = column_names
                        .iter()
                        .map(|column_name| column_name.humanize(context.humanizer))
                        .collect();
                    attrs.column_names = Some(value);
                }
            }
        }

        Ok(AnnotatedPlan { plan, annotations })
    }
}

/// Definition and helper structs for the [`Cardinality`] attribute.
mod cardinality {
    use std::collections::{BTreeMap, BTreeSet};

    use mz_expr::{
        BinaryFunc, Id, JoinImplementation, MirRelationExpr, MirScalarExpr, TableFunc, UnaryFunc,
        VariadicFunc,
    };
    use mz_ore::cast::CastLossy;
    use mz_repr::GlobalId;

    use mz_repr::explain::ExprHumanizer;
    use ordered_float::OrderedFloat;

    use super::{Analysis, Arity, SubtreeSize, UniqueKeys};
    use crate::symbolic::SymbolicExpression;

    /// Compute the estimated cardinality of each subtree of a [MirRelationExpr] from the bottom up.
    #[allow(missing_debug_implementations)]
    pub struct Cardinality {
        /// A factorizer for generating appropriating scaling factors
        pub factorize: Box<dyn Factorizer + Send + Sync>,
    }

    impl Default for Cardinality {
        fn default() -> Self {
            Cardinality {
                factorize: Box::new(WorstCaseFactorizer {
                    cardinalities: BTreeMap::new(),
                }),
            }
        }
    }

    /// The variables used in symbolic expressions representing cardinality
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub enum FactorizerVariable {
        /// The total cardinality of a given global id
        Id(GlobalId),
        /// The inverse of the number of distinct keys in the index held in the given column, i.e., 1/|# of distinct keys|
        ///
        /// TODO(mgree): need to correlate this back to a given global table, to feed in statistics (or have a sub-attribute for collecting this)
        Index(usize),
        /// An unbound local or other unknown quantity
        Unknown,
    }

    /// SymbolicExpressions specialized to factorizer variables
    pub type SymExp = SymbolicExpression<FactorizerVariable>;

    /// A `Factorizer` computes selectivity factors
    pub trait Factorizer {
        /// Compute selectivity for the flat map of `tf`
        fn flat_map(&self, tf: &TableFunc, input: &SymExp) -> SymExp;
        /// Computes selectivity of the predicate `expr`, given that `unique_columns` are indexed/unique
        ///
        /// The result should be in the range [0, 1.0]
        fn predicate(&self, expr: &MirScalarExpr, unique_columns: &BTreeSet<usize>) -> SymExp;
        /// Computes selectivity for a filter
        fn filter(
            &self,
            predicates: &Vec<MirScalarExpr>,
            keys: &Vec<Vec<usize>>,
            input: &SymExp,
        ) -> SymExp;
        /// Computes selectivity for a join; the cardinality estimate for each input is paired with the keys on that input
        ///
        /// `unique_columns` maps column references (that are indexed/unique) to their relation's index in `inputs`
        fn join(
            &self,
            equivalences: &Vec<Vec<MirScalarExpr>>,
            implementation: &JoinImplementation,
            unique_columns: BTreeMap<usize, usize>,
            inputs: Vec<&SymExp>,
        ) -> SymExp;
        /// Computes selectivity for a reduce
        fn reduce(
            &self,
            group_key: &Vec<MirScalarExpr>,
            expected_group_size: &Option<u64>,
            input: &SymExp,
        ) -> SymExp;
        /// Computes selectivity for a topk
        fn topk(
            &self,
            group_key: &Vec<usize>,
            limit: &Option<MirScalarExpr>,
            expected_group_size: &Option<u64>,
            input: &SymExp,
        ) -> SymExp;
        /// Computes slectivity for a threshold
        fn threshold(&self, input: &SymExp) -> SymExp;
    }

    /// The simplest possible `Factorizer` that aims to generate worst-case, upper-bound cardinalities
    #[derive(Debug)]
    pub struct WorstCaseFactorizer {
        /// cardinalities for each `GlobalId` and its unique values
        pub cardinalities: BTreeMap<FactorizerVariable, usize>,
    }

    /// The default selectivity for predicates we know nothing about.
    ///
    /// It is safe to use this instead of `FactorizerVariable::Index(col)`.
    pub const WORST_CASE_SELECTIVITY: f64 = 0.1;

    impl Factorizer for WorstCaseFactorizer {
        fn flat_map(&self, tf: &TableFunc, input: &SymExp) -> SymExp {
            match tf {
                TableFunc::Wrap { types, width } => {
                    input * (f64::cast_lossy(types.len()) / f64::cast_lossy(*width))
                }
                _ => {
                    // TODO(mgree) what explosion factor should we make up?
                    input * &SymExp::from(4.0)
                }
            }
        }

        fn predicate(&self, expr: &MirScalarExpr, unique_columns: &BTreeSet<usize>) -> SymExp {
            let index_cardinality = |expr: &MirScalarExpr| -> Option<SymExp> {
                match expr {
                    MirScalarExpr::Column(col) => {
                        if unique_columns.contains(col) {
                            Some(SymbolicExpression::symbolic(FactorizerVariable::Index(
                                *col,
                            )))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            };

            match expr {
                MirScalarExpr::Column(_)
                | MirScalarExpr::Literal(_, _)
                | MirScalarExpr::CallUnmaterializable(_) => SymExp::from(1.0),
                MirScalarExpr::CallUnary { func, expr } => match func {
                    UnaryFunc::Not(_) => 1.0 - self.predicate(expr, unique_columns),
                    UnaryFunc::IsTrue(_) | UnaryFunc::IsFalse(_) => SymExp::from(0.5),
                    UnaryFunc::IsNull(_) => {
                        if let Some(icard) = index_cardinality(expr) {
                            icard
                        } else {
                            SymExp::from(WORST_CASE_SELECTIVITY)
                        }
                    }
                    _ => SymExp::from(WORST_CASE_SELECTIVITY),
                },
                MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                    match func {
                        BinaryFunc::Eq => {
                            match (index_cardinality(expr1), index_cardinality(expr2)) {
                                (Some(icard1), Some(icard2)) => {
                                    SymbolicExpression::max(icard1, icard2)
                                }
                                (Some(icard), None) | (None, Some(icard)) => icard,
                                (None, None) => SymExp::from(WORST_CASE_SELECTIVITY),
                            }
                        }
                        // 1.0 - the Eq case
                        BinaryFunc::NotEq => {
                            match (index_cardinality(expr1), index_cardinality(expr2)) {
                                (Some(icard1), Some(icard2)) => {
                                    1.0 - SymbolicExpression::max(icard1, icard2)
                                }
                                (Some(icard), None) | (None, Some(icard)) => 1.0 - icard,
                                (None, None) => SymExp::from(1.0 - WORST_CASE_SELECTIVITY),
                            }
                        }
                        BinaryFunc::Lt | BinaryFunc::Lte | BinaryFunc::Gt | BinaryFunc::Gte => {
                            // TODO(mgree) if we have high/low key values and one of the columns is an index, we can do better
                            SymExp::from(0.33)
                        }
                        _ => SymExp::from(1.0), // TOOD(mgree): are there other interesting cases?
                    }
                }
                MirScalarExpr::CallVariadic { func, exprs } => match func {
                    VariadicFunc::And => {
                        // can't use SymExp::product because it expects a vector of references :/
                        let mut factor = SymExp::from(1.0);

                        for expr in exprs {
                            factor = factor * self.predicate(expr, unique_columns);
                        }

                        factor
                    }
                    VariadicFunc::Or => {
                        // TODO(mgree): BETWEEN will get compiled down to an OR of appropriate bounds---we could try to detect it and be clever

                        // F(expr1 OR expr2) = F(expr1) + F(expr2) - F(expr1) * F(expr2), but generalized
                        let mut exprs = exprs.into_iter();

                        let mut expr1;

                        if let Some(first) = exprs.next() {
                            expr1 = self.predicate(first, unique_columns);
                        } else {
                            return SymExp::from(1.0);
                        }

                        for expr2 in exprs {
                            let expr2 = self.predicate(expr2, unique_columns);

                            let intersection = expr1.clone() * expr2.clone();
                            // TODO(mgree) a big expression! two things could help: hash-consing and simplification
                            expr1 = expr1 + expr2 - intersection;
                        }
                        expr1
                    }
                    _ => SymExp::from(1.0),
                },
                MirScalarExpr::If { cond: _, then, els } => SymExp::max(
                    self.predicate(then, unique_columns),
                    self.predicate(els, unique_columns),
                ),
            }
        }

        fn filter(
            &self,
            predicates: &Vec<MirScalarExpr>,
            keys: &Vec<Vec<usize>>,
            input: &SymExp,
        ) -> SymExp {
            // TODO(mgree): should we try to do something for indices built on multiple columns?
            let mut unique_columns = BTreeSet::new();
            for key in keys {
                if key.len() == 1 {
                    unique_columns.insert(key[0]);
                }
            }

            // worst case scaling factor is 1
            let mut factor = SymExp::from(1.0);

            for expr in predicates {
                let predicate_scaling_factor = self.predicate(expr, &unique_columns);

                // constant scaling factors should be in [0,1]
                debug_assert!(
                match predicate_scaling_factor {
                    SymExp::Constant(OrderedFloat(n)) => 0.0 <= n && n <= 1.0,
                    _ => true,
                },
                "predicate scaling factor {predicate_scaling_factor} should be in the range [0,1]"
            );

                factor = factor * predicate_scaling_factor;
            }

            input.clone() * factor
        }

        fn join(
            &self,
            equivalences: &Vec<Vec<MirScalarExpr>>,
            _implementation: &JoinImplementation,
            unique_columns: BTreeMap<usize, usize>,
            inputs: Vec<&SymExp>,
        ) -> SymExp {
            let mut inputs = inputs.into_iter().cloned().collect::<Vec<_>>();

            for equiv in equivalences {
                // those sources which have a unique key
                let mut unique_sources = BTreeSet::new();
                let mut all_unique = true;

                for expr in equiv {
                    if let MirScalarExpr::Column(col) = expr {
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
                    let mut lhs = std::mem::replace(&mut inputs[lhs_idx], SymExp::f64(1.0));
                    for &rhs_idx in sources {
                        let rhs = std::mem::replace(&mut inputs[rhs_idx], SymExp::f64(1.0));
                        lhs = SymExp::min(lhs, rhs);
                    }

                    inputs[lhs_idx] = lhs;

                    // best option! go look at the next equivalence
                    continue;
                }

                // some unique columns in this equivalence
                for idx in unique_sources {
                    // when joining R and S on R.x = S.x, if R.x is unique and S.x is not, we're bounded above by the cardinality of S
                    inputs[idx] = SymExp::f64(1.0);
                }
            }

            SymbolicExpression::product(inputs)
        }

        fn reduce(
            &self,
            group_key: &Vec<MirScalarExpr>,
            expected_group_size: &Option<u64>,
            input: &SymExp,
        ) -> SymExp {
            // TODO(mgree): if no `group_key` is present, we can do way better

            if let Some(group_size) = expected_group_size {
                input / f64::cast_lossy(*group_size)
            } else if group_key.is_empty() {
                SymExp::from(1.0)
            } else {
                // in the worst case, every row is its own group
                input.clone()
            }
        }

        fn topk(
            &self,
            group_key: &Vec<usize>,
            limit: &Option<MirScalarExpr>,
            expected_group_size: &Option<u64>,
            input: &SymExp,
        ) -> SymExp {
            // TODO: support simple arithmetic expressions
            let k = limit
                .as_ref()
                .and_then(|l| l.as_literal_int64())
                .map_or(1, |l| std::cmp::max(0, l));

            if let Some(group_size) = expected_group_size {
                input * (f64::cast_lossy(k) / f64::cast_lossy(*group_size))
            } else if group_key.is_empty() {
                SymExp::from(k)
            } else {
                // in the worst case, every row is its own group
                input.clone()
            }
        }

        fn threshold(&self, input: &SymExp) -> SymExp {
            // worst case scaling factor is 1
            input.clone()
        }
    }

    impl Analysis for Cardinality {
        type Value = SymExp;

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

            let sizes = depends
                .as_view()
                .results::<SubtreeSize>()
                .expect("SubtreeSize analysis results missing");
            let arity = depends
                .as_view()
                .results::<Arity>()
                .expect("Arity analysis results missing");
            let keys = depends
                .as_view()
                .results::<UniqueKeys>()
                .expect("UniqueKeys analysis results missing");

            match expr {
                Constant { rows, .. } => {
                    SymExp::from(rows.as_ref().map_or_else(|_| 0, |v| v.len()))
                }
                Get { id, .. } => match id {
                    Id::Local(id) => depends
                        .bindings()
                        .get(id)
                        .and_then(|id| results.get(*id))
                        .cloned()
                        .unwrap_or(SymExp::symbolic(FactorizerVariable::Unknown)),
                    Id::Global(id) => SymbolicExpression::symbolic(FactorizerVariable::Id(*id)),
                },
                Let { .. } | Project { .. } | Map { .. } | ArrangeBy { .. } | Negate { .. } => {
                    results[index - 1].clone()
                }
                LetRec { .. } =>
                // TODO(mgree): implement a recurrence-based approach (or at least identify common idioms, e.g. transitive closure)
                {
                    SymbolicExpression::symbolic(FactorizerVariable::Unknown)
                }
                Union { base: _, inputs: _ } => {
                    let inputs = depends
                        .children_of_rev(index, expr.children().count())
                        .map(|off| results[off].clone())
                        .collect();
                    SymbolicExpression::sum(inputs)
                }
                FlatMap { func, .. } => {
                    let input = &results[index - 1];
                    self.factorize.flat_map(func, input)
                }
                Filter { predicates, .. } => {
                    let input = &results[index - 1];
                    let keys = depends.results::<UniqueKeys>().expect("UniqueKeys missing");
                    let keys = &keys[index - 1];
                    self.factorize.filter(predicates, keys, input)
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
                        let input = &results[index - offset];
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

                    self.factorize
                        .join(equivalences, implementation, unique_columns, input_results)
                }
                Reduce {
                    group_key,
                    expected_group_size,
                    ..
                } => {
                    let input = &results[index - 1];
                    self.factorize.reduce(group_key, expected_group_size, input)
                }
                TopK {
                    group_key,
                    limit,
                    expected_group_size,
                    ..
                } => {
                    let input = &results[index - 1];
                    self.factorize
                        .topk(group_key, limit, expected_group_size, input)
                }
                Threshold { .. } => {
                    let input = &results[index - 1];
                    self.factorize.threshold(input)
                }
            }
        }
    }

    impl SymExp {
        /// Render a symbolic expression nicely
        pub fn humanize(
            &self,
            h: &dyn ExprHumanizer,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            self.humanize_factor(h, f)
        }

        fn humanize_factor(
            &self,
            h: &dyn ExprHumanizer,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            use SymbolicExpression::*;
            match self {
                Sum(ss) => {
                    assert!(ss.len() >= 2);

                    let mut ss = ss.iter();
                    ss.next().unwrap().humanize_factor(h, f)?;
                    for s in ss {
                        write!(f, " + ")?;
                        s.humanize_factor(h, f)?;
                    }
                    Ok(())
                }
                _ => self.humanize_term(h, f),
            }
        }

        fn humanize_term(
            &self,
            h: &dyn ExprHumanizer,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            use SymbolicExpression::*;
            match self {
                Product(ps) => {
                    assert!(ps.len() >= 2);

                    let mut ps = ps.iter();
                    ps.next().unwrap().humanize_term(h, f)?;
                    for p in ps {
                        write!(f, " * ")?;
                        p.humanize_term(h, f)?;
                    }
                    Ok(())
                }
                _ => self.humanize_atom(h, f),
            }
        }

        fn humanize_atom(
            &self,
            h: &dyn ExprHumanizer,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            use SymbolicExpression::*;
            match self {
                Constant(OrderedFloat::<f64>(n)) => write!(f, "{n}"),
                Symbolic(FactorizerVariable::Id(v), n) => {
                    let id = h.humanize_id(*v).unwrap_or_else(|| format!("{v:?}"));
                    write!(f, "{id}")?;

                    if *n > 1 {
                        write!(f, "^{n}")?;
                    }

                    Ok(())
                }
                Symbolic(FactorizerVariable::Index(col), n) => {
                    write!(f, "icard(#{col})^{n}")
                }
                Symbolic(FactorizerVariable::Unknown, n) => {
                    write!(f, "unknown^{n}")
                }
                Max(e1, e2) => {
                    write!(f, "max(")?;
                    e1.humanize_factor(h, f)?;
                    write!(f, ", ")?;
                    e2.humanize_factor(h, f)?;
                    write!(f, ")")
                }
                Min(e1, e2) => {
                    write!(f, "min(")?;
                    e1.humanize_factor(h, f)?;
                    write!(f, ", ")?;
                    e2.humanize_factor(h, f)?;
                    write!(f, ")")
                }
                Sum(_) | Product(_) => {
                    write!(f, "(")?;
                    self.humanize_factor(h, f)?;
                    write!(f, ")")
                }
            }
        }
    }

    impl std::fmt::Display for SymExp {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.humanize(&mz_repr::explain::DummyHumanizer, f)
        }
    }

    /// Wrapping struct for pretty printing of symbolic expressions
    #[allow(missing_debug_implementations)]
    pub struct HumanizedSymbolicExpression<'a, 'b> {
        expr: &'a SymExp,
        humanizer: &'b dyn ExprHumanizer,
    }

    impl<'a, 'b> HumanizedSymbolicExpression<'a, 'b> {
        /// Pairs a symbolic expression with a way to render GlobalIds
        pub fn new(expr: &'a SymExp, humanizer: &'b dyn ExprHumanizer) -> Self {
            Self { expr, humanizer }
        }
    }

    impl<'a, 'b> std::fmt::Display for HumanizedSymbolicExpression<'a, 'b> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.expr.normalize().humanize(self.humanizer, f)
        }
    }
}
