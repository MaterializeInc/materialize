// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits and types for reusable expression analysis

use mz_expr::MirRelationExpr;

pub use common::{Derived, DerivedBuilder};

pub use arity::Arity;
pub use subtree::SubtreeSize;
pub use types::RelationType;

/// An analysis that can be applied bottom-up to a `MirRelationExpr`.
pub trait Analysis: 'static {
    /// The type of value this analysis associates with an expression.
    type Value;
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
    /// The return result will be associated with this expression for this analysis,
    /// and the analyses will continue.
    fn derive(expr: &MirRelationExpr, results: &[Self::Value], depends: &Derived) -> Self::Value;
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
        /// The integers are the zero-offset locations in the `SubtreeSize` analysis,
        /// which if absent will cause the method to return `None`. The order of the children
        /// is descending, from last child to first, because of how the information is laid out,
        /// and the non-reversibility of the look-ups.
        ///
        /// It is an error to call this method with more children than expression has
        pub fn children_of_rev<'a>(
            &'a self,
            start: usize,
            count: usize,
        ) -> Option<impl Iterator<Item = usize> + 'a> {
            if let Some(sizes) = self.results::<SubtreeSize>() {
                let offset = 1;
                Some((0..count).scan(offset, move |offset, _| {
                    let result = start - *offset;
                    *offset += sizes[result];
                    Some(result)
                }))
            } else {
                None
            }
        }
    }

    /// A builder wrapper to accumulate announced dependencies and construct default state.
    #[allow(missing_debug_implementations)]
    #[derive(Default)]
    pub struct DerivedBuilder {
        result: Derived,
    }

    impl DerivedBuilder {
        /// Announces a dependence on an analysis `A`.
        ///
        /// This ensures that `A` will be performed, and before any analysis that
        /// invokes this method.
        pub fn require<A: Analysis>(&mut self) {
            let type_id = TypeId::of::<Bundle<A>>();
            if !self.result.analyses.contains_key(&type_id) {
                A::announce_dependencies(self);
                if self.result.analyses.contains_key(&type_id) {
                    // panic!("Cyclic dependency detected: {:?}");
                }
                self.result.order.push(type_id);
                self.result.analyses.insert(
                    type_id,
                    Box::new(Bundle::<A> {
                        results: Vec::new(),
                    }),
                );
            }
        }
        /// Complete the building, and return a usable `Derivation`.
        pub fn visit(self, expr: &MirRelationExpr) -> Derived {
            let mut result = self.result;

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
                                for (id, value) in ids.iter().zip(values).rev() {
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
                        let prior = result.bindings.insert(local_id, rev_post_order.len());
                        assert!(prior.is_none());
                    }
                }
            }
            // Flip the offsets now that we know a length.
            for value in result.bindings.values_mut() {
                *value = rev_post_order.len() - *value - 1;
            }
            // Visit the pre-order in reverse order: post-order.
            while let Some(expr) = rev_post_order.pop() {
                // Apply each analysis to `expr` in order.
                for id in result.order.iter() {
                    if let Some(mut bundle) = result.analyses.remove(id) {
                        bundle.analyse(expr, &result);
                        result.analyses.insert(*id, bundle);
                    }
                }
            }

            result
        }
    }

    /// An abstraction for an analysis and associated state.
    trait AnalysisBundle: Any {
        fn analyse(&mut self, expr: &MirRelationExpr, depends: &Derived);
        /// Upcasts `self` to a `&dyn Any`.
        ///
        /// NOTE: This is required until <https://github.com/rust-lang/rfcs/issues/2765> is fixed
        fn as_any(&self) -> &dyn std::any::Any;
    }

    /// A wrapper for analysis state.
    struct Bundle<A: Analysis> {
        results: Vec<A::Value>,
    }

    impl<A: Analysis> AnalysisBundle for Bundle<A> {
        fn analyse(&mut self, expr: &MirRelationExpr, depends: &Derived) {
            let value = A::derive(expr, &self.results[..], depends);
            self.results.push(value);
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
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
            results: &[Self::Value],
            _depends: &Derived,
        ) -> Self::Value {
            match expr {
                MirRelationExpr::Constant { .. } | MirRelationExpr::Get { .. } => 1,
                _ => {
                    let n = results.len();
                    let mut offset = 1;
                    for _ in expr.children() {
                        offset += results[n - offset];
                    }
                    offset
                }
            }
        }
    }
}

/// Expression arities
mod arity {

    use super::subtree::SubtreeSize;
    use super::{Analysis, Derived, DerivedBuilder};
    use mz_expr::MirRelationExpr;

    /// Analysis that determines the number of columns of relation expressions.
    #[derive(Debug)]
    pub struct Arity;

    impl Analysis for Arity {
        type Value = usize;

        fn announce_dependencies(builder: &mut DerivedBuilder) {
            builder.require::<SubtreeSize>();
        }

        fn derive(
            expr: &MirRelationExpr,
            results: &[Self::Value],
            depends: &Derived,
        ) -> Self::Value {
            let position = results.len();
            let mut offsets = depends
                .children_of_rev(position, expr.children().count())
                .expect("SubtreeSize missing")
                .map(|child| results[child])
                .collect::<Vec<_>>();
            offsets.reverse();
            expr.arity_with_input_arities(offsets.iter())
        }
    }
}

/// Expression types
mod types {

    use super::subtree::SubtreeSize;
    use super::{Analysis, Derived, DerivedBuilder};
    use mz_expr::MirRelationExpr;
    use mz_repr::ColumnType;

    /// Analysis that determines the type of relation expressions.
    #[derive(Debug)]
    pub struct RelationType;

    impl Analysis for RelationType {
        type Value = Option<Vec<ColumnType>>;

        fn announce_dependencies(builder: &mut DerivedBuilder) {
            builder.require::<SubtreeSize>();
        }

        fn derive(
            expr: &MirRelationExpr,
            results: &[Self::Value],
            depends: &Derived,
        ) -> Self::Value {
            let position = results.len();
            let offsets = depends
                .children_of_rev(position, expr.children().count())
                .expect("SubtreeSize missing")
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
    use super::subtree::SubtreeSize;
    use super::{Analysis, Derived, DerivedBuilder};
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
            builder.require::<SubtreeSize>();
            builder.require::<Arity>();
        }

        fn derive(
            expr: &MirRelationExpr,
            results: &[Self::Value],
            depends: &Derived,
        ) -> Self::Value {
            let position = results.len();
            let mut offsets = depends
                .children_of_rev(position, expr.children().count())
                .expect("SubtreeSize missing")
                .collect::<Vec<_>>();
            offsets.reverse();

            let arity = depends.results::<Arity>().unwrap();

            expr.keys_with_input_keys(
                offsets.iter().map(|o| arity[*o]),
                offsets.iter().map(|o| &results[*o]),
            )
        }
    }
}

/// Determines if accumulated frequences can be negative.
///
/// This analysis assumes that globally identified collection have the property, and it is
/// incorrect to apply it to expressions that reference external collections that may have
/// negative accumulations.
mod non_negative {

    use super::subtree::SubtreeSize;
    use super::{Analysis, Derived, DerivedBuilder};
    use mz_expr::{Id, MirRelationExpr};

    /// Analysis that determines if all accumulations at all times are non-negative.
    ///
    /// The analysis assumes that `Id::Global` references only refer to non-negative collections.
    #[derive(Debug)]
    pub struct NonNegative;

    impl Analysis for NonNegative {
        type Value = bool;

        fn announce_dependencies(builder: &mut DerivedBuilder) {
            builder.require::<SubtreeSize>();
        }

        fn derive(
            expr: &MirRelationExpr,
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
                    let position = results.len();
                    depends
                        .children_of_rev(position, expr.children().count())
                        .expect("SubtreeSize missing")
                        .all(|off| results[off])
                }
                _ => *results.last().unwrap(),
            }
        }
    }
}
