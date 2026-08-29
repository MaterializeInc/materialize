// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Predicates and the security levels that order them.
//!
//! A predicate is an expression that must hold for a row to be produced. Most
//! predicates may be evaluated wherever the optimizer finds convenient. A
//! predicate that a security barrier constrains may not: it must not observe a
//! row that a predicate at a lower level would have rejected, because a
//! fallible expression reveals the row through the error it raises.
//!
//! [`Predicate`] pairs an expression with the level it was introduced at, so
//! that constraint travels with the predicate through the optimizer rather than
//! being re-derived from plan shape. See
//! `doc/developer/design/20260828_security_barrier_views.md`.

use serde::{Deserialize, Serialize};

use crate::MirScalarExpr;

/// The security level a predicate was introduced at.
///
/// A predicate at a lower level must be evaluated before one at a higher level,
/// unless the higher one is leakproof. Level `0` means unconstrained, which is
/// what a predicate written directly against its own collection is.
pub type SecurityLevel = u8;

/// A predicate together with the security level it was introduced at.
///
/// `level` is declared first so that the derived `Ord` orders by it, which is
/// what makes sorting a predicate list put lower levels first by construction.
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize
)]
pub struct Predicate<E = MirScalarExpr> {
    /// The security level the predicate was introduced at.
    pub level: SecurityLevel,
    /// The predicate expression.
    pub expr: E,
}

impl<E> Predicate<E> {
    /// A predicate subject to no ordering constraint.
    ///
    /// This is the right constructor for a predicate written directly against
    /// the collection it filters, which is nearly all of them. Reach for
    /// [`Predicate::at_level`] only when the predicate crossed a security
    /// barrier to get where it is.
    pub fn unconstrained(expr: E) -> Self {
        Predicate { level: 0, expr }
    }

    /// A predicate that must be evaluated after every predicate below `level`.
    pub fn at_level(expr: E, level: SecurityLevel) -> Self {
        Predicate { level, expr }
    }

    /// Whether an ordering constraint applies to this predicate.
    pub fn is_constrained(&self) -> bool {
        self.level > 0
    }

    /// Raises the predicate one level.
    ///
    /// Applied to a consumer's predicates when a security barrier is inlined
    /// into it. Raising rather than assigning a fixed level is what makes
    /// nested barriers compose: each inlining lifts everything already merged,
    /// so a barrier's own predicates stay below every predicate written above
    /// it.
    pub fn raise(&mut self) {
        self.level = self.level.saturating_add(1);
    }

    /// Applies `f` to the expression, preserving the level.
    pub fn map_expr<F, T>(self, f: F) -> Predicate<T>
    where
        F: FnOnce(E) -> T,
    {
        Predicate {
            level: self.level,
            expr: f(self.expr),
        }
    }

    /// Borrows the expression, preserving the level.
    pub fn as_ref(&self) -> Predicate<&E> {
        Predicate {
            level: self.level,
            expr: &self.expr,
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for Predicate<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.level > 0 {
            write!(f, "[L{}] ", self.level)?;
        }
        self.expr.fmt(f)
    }
}

impl<E> std::ops::Deref for Predicate<E> {
    type Target = E;
    fn deref(&self) -> &E {
        &self.expr
    }
}

impl<E> std::ops::DerefMut for Predicate<E> {
    fn deref_mut(&mut self) -> &mut E {
        &mut self.expr
    }
}
