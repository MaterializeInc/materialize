// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! # Bindings
//!
//! This module implements the functionality for processing bindings, including
//! hoisting bindings to the top of a dataflow graph and for adding new bindings
//! to eliminate common subgraphs.
//!
//! ## Functionality
//!
//! Notably, this module defines [`Binding`]. That structure captures only the
//! name and value of a binding but not its body, as [`RelationExpr::Let`] does.
//! This module also defines [`Environment`] as an ordered collection of bindings.
//! The latter is the main data structure necessary for traversing a dataflow
//! graph and manipulating its bindings.
//!
//! ## Invariants
//!
//! Correctly handling bindings during such a graph traversal requires that the
//! corresponding code preserve a few invariants. In particular, assuming a
//! depth-first traversal implemented by calling [`RelationExpr::visit1`] or
//! [`RelationExpr::visit1_mut`], the following orderings _must_ be preserved:
//!
//!  1. A [`RelationExpr::Let`]'s value must be processed before its body. All
//!     methods for visiting [`RelationExpr`] already preserve that invariant.
//!
//!  2. When generating new bindings via [`Environment::add`], a binding for
//!     a smaller subgraph must be generated before the binding for a larger
//!     subgraph containing the smaller one. Or, to put this differently, a
//!     binding for a deeper subgraph must be generated before the binding
//!     for a shallower subgraph containing the deeper one. This corresponds
//!     to generating bindings in _post-order_ during the depth-first traversal.
//!
//!  3. When collecting existing bindings via [`Environment::extract`], the
//!     bindings contained in a [`RelationExpr::Let`]'s value must be collected
//!     first, then the binding for the [`RelationExpr::Let`] itself, and then
//!     the bindings for its body. This corresponds to collecting bindings
//!     _in-order_ during the depth-first traversal.

use crate::RelationExpr;
use repr::RelationType;

/// Create a fresh name that is guaranteed not to be bound.
pub fn fresh_name() -> String {
    format!("bdg-{}", uuid::Uuid::new_v4())
}

/// A binding from a value to a name. Unlike [`RelationExpr::Let`],
/// a binding does not have a body, only a name and value.
#[derive(Debug)]
pub struct Binding {
    name: String,
    value: RelationExpr,
}

impl Binding {
    /// Create a new binding for the given name and value.
    pub fn new(name: String, value: RelationExpr) -> Self {
        Self { name, value }
    }

    /// Apply the binding to the body, yielding a [`RelationExpr::Let`].
    pub fn use_in(self, body: RelationExpr) -> RelationExpr {
        RelationExpr::Let {
            name: self.name,
            value: Box::new(self.value),
            body: Box::new(body),
        }
    }
}

/// An environment, i.e., an ordered collection of bindings.
#[derive(Debug, Default)]
pub struct Environment {
    bindings: Vec<Binding>,
}

impl Environment {
    /// Add a new binding for the given name and value to this environment.
    pub fn bind(&mut self, name: String, value: RelationExpr) -> &mut Self {
        self.bindings.push(Binding::new(name, value));
        self
    }

    /// Extract a binding from the given dataflow graph. If the dataflow graph is not a
    /// [`RelationExpr::Let`], this method does nothing. Otherwise, it adds a binding for
    /// the name and value to this environment and leaves the body in the let's place.
    pub fn extract(&mut self, expr: &mut RelationExpr) -> &mut Self {
        if let RelationExpr::Let { .. } = expr {
            if let RelationExpr::Let { name, value, body } = expr.take() {
                self.bindings.push(Binding::new(name, *value));
                *expr = *body;
            } else {
                unreachable!();
            }
        }
        self
    }

    /// Use this environment's bindings in the given body. This method returns
    /// a [`RelationExpr::Let`] nested within each other for each binding with
    /// innermost body being the given body. It consumes all bindings.
    pub fn use_in(self, body: RelationExpr) -> RelationExpr {
        self.bindings
            .into_iter()
            .rev()
            .fold(body, |acc, b| b.use_in(acc))
    }

    /// Inject this environment's bindings into the given dataflow graph. This method
    /// makes the bindings available to the dataflow graph. It consumes all bindings.
    pub fn inject(self, expr: &mut RelationExpr) {
        *expr = self.use_in(expr.take());
    }
}

/// The hoist optimization.
#[derive(Debug)]
pub struct Hoist;

impl super::Transform for Hoist {
    // Hoist all bindings to the top of a dataflow graph.
    fn transform(&self, expr: &mut RelationExpr, _meta: &RelationType) {
        Hoist::hoist(expr);
    }
}

impl Hoist {
    // Hoist all bindings to the top of a dataflow graph.
    fn hoist(expr: &mut RelationExpr) {
        let mut env = Environment::default();

        pub fn extract(expr: &mut RelationExpr, env: &mut Environment) {
            // NB: visit1_mut() invokes the callback on the children of a RelationExpr.
            // That is not good enough for RelationExpr::Let since the value and body
            // might just be RelationExpr::Let's, too. Hence we recurse on extract().
            if let RelationExpr::Let { value, .. } = expr {
                extract(value, env);
                env.extract(expr);
                // By the magic vested in env.extract(), expr now is let's body.
                extract(expr, env);
            } else {
                expr.visit1_mut(|e| extract(e, env));
            }
        }

        extract(expr, &mut env);
        env.inject(expr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use repr::Datum;

    fn not_my_type() -> RelationType {
        RelationType::new(vec![])
    }

    fn n(i: i32) -> RelationExpr {
        RelationExpr::constant(vec![vec![Datum::Int32(i)]], not_my_type())
    }

    fn b<T>(name: T, value: RelationExpr, body: RelationExpr) -> RelationExpr
    where
        T: Into<String>,
    {
        RelationExpr::Let {
            name: name.into(),
            value: Box::new(value),
            body: Box::new(body),
        }
    }

    #[test]
    fn test_hoist() {
        let mut expr = b(
            "h",
            b(
                "d",
                b("b", b("a", n(1), n(2)), b("c", n(3), n(4))),
                b("f", b("e", n(5), n(6)), b("g", n(7), n(8))),
            ),
            b("i", n(9), b("j", n(10), n(11)).distinct().negate()),
        );

        print!(">>IN\n{}\n<<IN\n\n", expr.pretty());
        Hoist::hoist(&mut expr);
        print!(">>OUT\n{}\n<<OUT\n", expr.pretty());

        assert_eq!(
            expr,
            b(
                "a",
                n(1),
                b(
                    "b",
                    n(2),
                    b(
                        "c",
                        n(3),
                        b(
                            "d",
                            n(4),
                            b(
                                "e",
                                n(5),
                                b(
                                    "f",
                                    n(6),
                                    b(
                                        "g",
                                        n(7),
                                        b(
                                            "h",
                                            n(8),
                                            b("i", n(9), b("j", n(10), n(11).distinct().negate()))
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        );
    }
}
