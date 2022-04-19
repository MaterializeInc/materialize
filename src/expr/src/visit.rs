// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Visitor support for recursive data types.
//!
//! Recursive types can implement the [`VisitChildren`] trait, to
//! specify how their recursive entries can be accessed. The extension
//! trait [`Visit`] then adds support for recursively traversing
//! instances of those types.
//!
//! # Naming
//!
//! Visitor methods follow this naming pattern:
//!
//! ```text
//! [try_]visit_[mut_]{children,post,pre}
//! ```
//!
//! * The `try`-prefix specifies whether the visitor callback is
//!   fallible (prefix present) or infallible (prefix omitted).
//! * The `mut`-suffix specifies whether the visitor callback gets
//!   access to mutable (prefix present) or immutable (prefix omitted)
//!   child references.
//! * The final suffix determines the nature of the traversal:
//!   * `children`: only visit direct children
//!   * `post`: recursively visit children in post-order
//!   * `pre`: recursively visit children in pre-order

use std::marker::PhantomData;

use mz_ore::stack::{maybe_grow, CheckedRecursion, RecursionGuard, RecursionLimitError};

use crate::RECURSION_LIMIT;

/// A trait for types that can visit their direct children of the same
/// type.
///
/// Implementing this trait for a type automatically also implements
/// the [`Visit`] trait, which enables recursive traversal.
pub trait VisitChildren {
    /// Apply an infallible immutable function `f` to each direct child.
    fn visit_children<'a, F>(&'a self, f: F)
    where
        F: FnMut(&'a Self);

    /// Apply an infallible mutable function `f` to each direct child.
    fn visit_mut_children<'a, F>(&'a mut self, f: F)
    where
        F: FnMut(&'a mut Self);

    /// Apply a fallible immutable function `f` to each direct child.
    fn try_visit_children<'a, F, E>(&'a self, f: F) -> Result<(), E>
    where
        F: FnMut(&'a Self) -> Result<(), E>;

    /// Apply a fallible mutable function `f` to each direct child.
    fn try_visit_mut_children<'a, F, E>(&'a mut self, f: F) -> Result<(), E>
    where
        F: FnMut(&'a mut Self) -> Result<(), E>;
}

/// A trait for types that can recursively visit their children of the
/// same type.
///
/// This trait is automatically implemented for all implementors of
/// [`VisitChildren`].
///
/// All methods provided by this trait ensure that the stack is grown
/// as needed, to avoid stack overflows when traversing deeply
/// recursive objects. The `try_*` methods also enforce a recursion
/// limit of [`RECURSION_LIMIT`] by returning an error when that limit
/// is exceeded. The infallible visitor methods don't enforce a limit,
/// as they have no way of reporting errors.
pub trait Visit {
    /// Post-order immutable infallible visitor for `self`.
    fn visit_post<F>(&self, f: &mut F)
    where
        F: FnMut(&Self);

    /// Post-order mutable infallible visitor for `self`.
    fn visit_mut_post<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self);

    /// Post-order immutable fallible visitor for `self`.
    fn try_visit_post<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
        E: From<RecursionLimitError>;

    /// Post-order mutable fallible visitor for `self`.
    fn try_visit_mut_post<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
        E: From<RecursionLimitError>;

    /// Pre-order immutable infallible visitor for `self`.
    fn visit_pre<F>(&self, f: &mut F)
    where
        F: FnMut(&Self);

    /// Pre-order mutable infallible visitor for `self`.
    fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self);

    /// Pre-order immutable fallible visitor for `self`.
    fn try_visit_pre<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
        E: From<RecursionLimitError>;

    /// Pre-order mutable fallible visitor for `self`.
    fn try_visit_mut_pre<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
        E: From<RecursionLimitError>;

    /// A generalization of [`Visit::visit_pre`] and [`Visit::visit_post`].
    ///
    /// The function `pre` runs on `self` before it runs on any of the children.
    /// The function `post` runs on children first before the parent.
    ///
    /// Optionally, `pre` can return which children, if any, should be visited
    /// (default is to visit all children).
    fn visit_pre_post<F1, F2>(&self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self);

    /// A generalization of [`Visit::visit_mut_pre`] and [`Visit::visit_mut_post`].
    ///
    /// The function `pre` runs on `self` before it runs on any of the children.
    /// The function `post` runs on children first before the parent.
    ///
    /// Optionally, `pre` can return which children, if any, should be visited
    /// (default is to visit all children).
    fn visit_mut_pre_post<F1, F2>(&mut self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut Self>>,
        F2: FnMut(&mut Self);
}

impl<T: VisitChildren> Visit for T {
    fn visit_post<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        Visitor::new().visit_post(self, f)
    }

    fn visit_mut_post<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        Visitor::new().visit_mut_post(self, f)
    }

    fn try_visit_post<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        Visitor::new().try_visit_post(self, f)
    }

    fn try_visit_mut_post<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        Visitor::new().try_visit_mut_post(self, f)
    }

    fn visit_pre<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        Visitor::new().visit_pre(self, f)
    }

    fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        Visitor::new().visit_mut_pre(self, f)
    }

    fn try_visit_pre<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        Visitor::new().try_visit_pre(self, f)
    }

    fn try_visit_mut_pre<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        Visitor::new().try_visit_mut_pre(self, f)
    }

    fn visit_pre_post<F1, F2>(&self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self),
    {
        Visitor::new().visit_pre_post(self, pre, post)
    }

    fn visit_mut_pre_post<F1, F2>(&mut self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut Self>>,
        F2: FnMut(&mut Self),
    {
        Visitor::new().visit_mut_pre_post(self, pre, post)
    }
}

struct Visitor<T> {
    recursion_guard: RecursionGuard,
    _type: PhantomData<T>,
}

impl<T> CheckedRecursion for Visitor<T> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl<T: VisitChildren> Visitor<T> {
    fn new() -> Self {
        Self {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
            _type: PhantomData,
        }
    }

    fn visit_post<F>(&self, value: &T, f: &mut F)
    where
        F: FnMut(&T),
    {
        maybe_grow(|| {
            value.visit_children(|child| self.visit_post(child, f));
            f(value)
        })
    }

    fn visit_mut_post<F>(&self, value: &mut T, f: &mut F)
    where
        F: FnMut(&mut T),
    {
        maybe_grow(|| {
            value.visit_mut_children(|child| self.visit_mut_post(child, f));
            f(value)
        })
    }

    fn try_visit_post<F, E>(&self, value: &T, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&T) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        self.checked_recur(move |_| {
            value.try_visit_children(|child| self.try_visit_post(child, f))?;
            f(value)
        })
    }

    fn try_visit_mut_post<F, E>(&self, value: &mut T, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut T) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        self.checked_recur(move |_| {
            value.try_visit_mut_children(|child| self.try_visit_mut_post(child, f))?;
            f(value)
        })
    }

    fn visit_pre<F>(&self, value: &T, f: &mut F)
    where
        F: FnMut(&T),
    {
        maybe_grow(|| {
            f(value);
            value.visit_children(|child| self.visit_pre(child, f))
        })
    }

    fn visit_mut_pre<F>(&self, value: &mut T, f: &mut F)
    where
        F: FnMut(&mut T),
    {
        maybe_grow(|| {
            f(value);
            value.visit_mut_children(|child| self.visit_mut_pre(child, f))
        })
    }

    fn try_visit_pre<F, E>(&self, value: &T, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&T) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        self.checked_recur(move |_| {
            f(value)?;
            value.try_visit_children(|child| self.try_visit_pre(child, f))
        })
    }

    fn try_visit_mut_pre<F, E>(&self, value: &mut T, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut T) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        self.checked_recur(move |_| {
            f(value)?;
            value.try_visit_mut_children(|child| self.try_visit_mut_pre(child, f))
        })
    }

    fn visit_pre_post<F1, F2>(&self, value: &T, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&T) -> Option<Vec<&T>>,
        F2: FnMut(&T),
    {
        maybe_grow(|| {
            if let Some(to_visit) = pre(value) {
                for child in to_visit {
                    self.visit_pre_post(child, pre, post);
                }
            } else {
                value.visit_children(|child| self.visit_pre_post(child, pre, post));
            }
            post(value);
        })
    }

    fn visit_mut_pre_post<F1, F2>(&self, value: &mut T, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&mut T) -> Option<Vec<&mut T>>,
        F2: FnMut(&mut T),
    {
        maybe_grow(|| {
            if let Some(to_visit) = pre(value) {
                for child in to_visit {
                    self.visit_mut_pre_post(child, pre, post);
                }
            } else {
                value.visit_mut_children(|child| self.visit_mut_pre_post(child, pre, post));
            }
            post(value);
        })
    }
}
