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
//!
//! # Contract for a transform that handles predicates
//!
//! Two things a transform cannot get wrong. A predicate cannot be built without
//! naming a level, because no conversion from a bare expression exists: use
//! [`Predicate::unconstrained`] for one written against the collection it
//! filters, which is nearly all of them, and [`Predicate::at_level`] otherwise.
//! And a level cannot be lowered, because the field is private and
//! [`Predicate::raise`] is its only mutator.
//!
//! Reordering predicates within a plan is also safe. `MapFilterProject` sorts
//! by level, so however much a transform shuffles them, the order is
//! re-established when they fuse into one operator. That sort is where the
//! constraint is discharged, not a step a caller has to remember.
//!
//! Three things a transform must get right.
//!
//! **Relocating a predicate across operators.** The sort only orders predicates
//! that end up in the same `MapFilterProject`. Moving a levelled predicate into
//! an operator evaluated earlier than the one holding a lower-level predicate
//! defeats the constraint. `PredicatePushdown` splits levelled predicates into a
//! `Filter` it then leaves alone for this reason, and a new relocating transform
//! does not inherit that.
//!
//! Stated as a plan invariant: for any `Filter` D that is a descendant of a
//! `Filter` A, `max_level(D) <= min_level(A)`. A descendant is evaluated first
//! on any given row, so lower levels below higher ones is the correct direction
//! and the reverse is the violation.
//!
//! TODO: assert that invariant in `mz_transform::typecheck`, which already
//! validates plan invariants between transforms, so this shape stops resting on
//! review.
//!
//! **Deriving a predicate from a levelled one.** A derived predicate inherits
//! the *minimum* level of the predicates it came from. Nothing checks this, and
//! [`Predicate::unconstrained`] is the natural thing to reach for, so it is the
//! shape most likely to go wrong. The derivation sites in
//! `analysis::equivalences` and `equivalence_propagation` avoid the question
//! entirely by declining to seed an equivalence class from a levelled predicate
//! that is not leakproof.
//!
//! **Adding a new home for predicates.** A new `MirRelationExpr` variant that
//! holds predicates, or a new fused-operator representation, is not reached by
//! the pass that assigns levels at a security barrier, and its predicates are
//! not covered by the `MapFilterProject` sort. Both have to be extended.

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
    ///
    /// Private, and deliberately so. A predicate's level may be read, and may
    /// be [raised](Predicate::raise), but there is no operation that lowers it.
    /// Losing a constraint therefore requires constructing a fresh predicate
    /// through [`Predicate::unconstrained`], which is a named call a reviewer
    /// can grep for, rather than an assignment that reads like nothing.
    level: SecurityLevel,
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

    /// The security level this predicate must be evaluated at.
    pub fn level(&self) -> SecurityLevel {
        self.level
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
