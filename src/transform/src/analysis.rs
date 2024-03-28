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
    /// The `index` indicates the post-order index for the expression, for use in finding
    /// the corresponding information in `results` and `depends`.
    ///
    /// The return result will be associated with this expression for this analysis,
    /// and the analyses will continue.
    fn derive(
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
    pub struct DerivedBuilder {
        result: Derived,
    }

    impl Default for DerivedBuilder {
        fn default() -> Self {
            // The default builder should include `SubtreeSize` to facilitate navigation.
            let mut builder = DerivedBuilder {
                result: Derived::default(),
            };
            builder.require::<SubtreeSize>();
            builder
        }
    }

    impl DerivedBuilder {
        /// Announces a dependence on an analysis `A`.
        ///
        /// This ensures that `A` will be performed, and before any analysis that
        /// invokes this method.
        pub fn require<A: Analysis>(&mut self) {
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
                        results: Vec::new(),
                        fuel: 100,
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
    }

    /// A wrapper for analysis state.
    struct Bundle<A: Analysis> {
        results: Vec<A::Value>,
        /// Counts down with each `LetRec` re-iteration, to avoid unbounded effort.
        /// Should it reach zero, the analysis should discard its results and restart as if pessimistic.
        fuel: usize,
    }

    impl<A: Analysis> AnalysisBundle for Bundle<A> {
        fn analyse(&mut self, exprs: &[&MirRelationExpr], depends: &Derived) -> bool {
            self.results.clear();
            // Attempt optimistic analysis, and if that fails go pessimistic.
            let update = A::lattice()
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
                let value = A::derive(exprs[upper - 1], upper - 1, &self.results[..], depends);
                Ok(lattice.meet_assign(&mut self.results[upper - 1], value))
            } else {
                // If not a `LetRec`, we still want to revisit results and update them with meet.
                let mut changed = false;
                for index in lower..upper {
                    let value = A::derive(exprs[index], index, &self.results[..], depends);
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
                self.results
                    .push(A::derive(expr, index, &self.results[..], depends));
            }
            true
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
            builder.require::<Arity>();
        }

        fn derive(
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
