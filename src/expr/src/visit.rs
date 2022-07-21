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

/// A trait for types that can visit their direct children of type `T`.
///
/// Implementing [`VisitChildren<Self>`] automatically also implements
/// the [`Visit`] trait, which enables recursive traversal.
///
/// Note that care needs to be taken when implementing this trait for
/// mutually recursive types (such as a type A where A has children
/// of type B and vice versa). More specifically, at the moment it is
/// not possible to implement versions of `VisitChildren<A> for A` such
/// that A considers as its children all A-nodes occurring at leaf
/// positions of B-children and vice versa for `VisitChildren<B> for B`.
/// Doing this will result in recusion limit violations as indicated
/// in the accompanying `test_recursive_types_b` test.
pub trait VisitChildren<T> {
    /// Apply an infallible immutable function `f` to each direct child.
    fn visit_children<F>(&self, f: F)
    where
        F: FnMut(&T);

    /// Apply an infallible mutable function `f` to each direct child.
    fn visit_mut_children<F>(&mut self, f: F)
    where
        F: FnMut(&mut T);

    /// Apply a fallible immutable function `f` to each direct child.
    ///
    /// For mutually recursive implementations (say consisting of two
    /// types `A` and `B`), recursing through `B` in order to find all
    /// `A`-children of a node of type `A` might cause lead to a
    /// [`RecursionLimitError`], hence the bound on `E`.
    fn try_visit_children<F, E>(&self, f: F) -> Result<(), E>
    where
        F: FnMut(&T) -> Result<(), E>,
        E: From<RecursionLimitError>;

    /// Apply a fallible mutable function `f` to each direct child.
    ///
    /// For mutually recursive implementations (say consisting of two
    /// types `A` and `B`), recursing through `B` in order to find all
    /// `A`-children of a node of type `A` might cause lead to a
    /// [`RecursionLimitError`], hence the bound on `E`.
    fn try_visit_mut_children<F, E>(&mut self, f: F) -> Result<(), E>
    where
        F: FnMut(&mut T) -> Result<(), E>,
        E: From<RecursionLimitError>;
}

/// A trait for types that can recursively visit their children of the
/// same type.
///
/// This trait is automatically implemented for all implementors of
/// [`VisitChildren`].
///
/// All methods provided by this trait ensure that the stack is grown
/// as needed, to avoid stack overflows when traversing deeply
/// recursive objects. They also enforce a recursion limit of
/// [`RECURSION_LIMIT`] by returning an error when that limit
/// is exceeded.
///
/// There are also `*_nolimit` methods that don't enforce a recursion
/// limit. Those methods are deprecated and should not be used.
pub trait Visit {
    /// Post-order immutable infallible visitor for `self`.
    fn visit_post<F>(&self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&Self);

    /// Post-order immutable infallible visitor for `self`.
    /// Does not enforce a recursion limit.
    #[deprecated = "Use `visit_post` instead."]
    fn visit_post_nolimit<F>(&self, f: &mut F)
    where
        F: FnMut(&Self);

    /// Post-order mutable infallible visitor for `self`.
    fn visit_mut_post<F>(&mut self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&mut Self);

    /// Post-order mutable infallible visitor for `self`.
    /// Does not enforce a recursion limit.
    #[deprecated = "Use `visit_mut_post` instead."]
    fn visit_mut_post_nolimit<F>(&mut self, f: &mut F)
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
    fn visit_pre<F>(&self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&Self);

    /// Pre-order immutable infallible visitor for `self`.
    /// Does not enforce a recursion limit.
    #[deprecated = "Use `visit_pre` instead."]
    fn visit_pre_nolimit<F>(&self, f: &mut F)
    where
        F: FnMut(&Self);

    /// Pre-order mutable infallible visitor for `self`.
    fn visit_mut_pre<F>(&mut self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&mut Self);

    /// Pre-order mutable infallible visitor for `self`.
    /// Does not enforce a recursion limit.
    #[deprecated = "Use `visit_mut_pre` instead."]
    fn visit_mut_pre_nolimit<F>(&mut self, f: &mut F)
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
    fn visit_pre_post<F1, F2>(
        &self,
        pre: &mut F1,
        post: &mut F2,
    ) -> Result<(), RecursionLimitError>
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self);

    /// A generalization of [`Visit::visit_pre`] and [`Visit::visit_post`].
    /// Does not enforce a recursion limit.
    ///
    /// The function `pre` runs on `self` before it runs on any of the children.
    /// The function `post` runs on children first before the parent.
    ///
    /// Optionally, `pre` can return which children, if any, should be visited
    /// (default is to visit all children).
    #[deprecated = "Use `visit_pre_post` instead."]
    fn visit_pre_post_nolimit<F1, F2>(&self, pre: &mut F1, post: &mut F2)
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
    fn visit_mut_pre_post<F1, F2>(
        &mut self,
        pre: &mut F1,
        post: &mut F2,
    ) -> Result<(), RecursionLimitError>
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut Self>>,
        F2: FnMut(&mut Self);

    /// A generalization of [`Visit::visit_mut_pre`] and [`Visit::visit_mut_post`].
    /// Does not enforce a recursion limit.
    ///
    /// The function `pre` runs on `self` before it runs on any of the children.
    /// The function `post` runs on children first before the parent.
    ///
    /// Optionally, `pre` can return which children, if any, should be visited
    /// (default is to visit all children).
    #[deprecated = "Use `visit_mut_pre_post` instead."]
    fn visit_mut_pre_post_nolimit<F1, F2>(&mut self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut Self>>,
        F2: FnMut(&mut Self);
}

impl<T: VisitChildren<T>> Visit for T {
    fn visit_post<F>(&self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&Self),
    {
        Visitor::new().visit_post(self, f)
    }

    fn visit_post_nolimit<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        Visitor::new().visit_post_nolimit(self, f)
    }

    fn visit_mut_post<F>(&mut self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&mut Self),
    {
        Visitor::new().visit_mut_post(self, f)
    }

    fn visit_mut_post_nolimit<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        Visitor::new().visit_mut_post_nolimit(self, f)
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

    fn visit_pre<F>(&self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&Self),
    {
        Visitor::new().visit_pre(self, f)
    }

    fn visit_pre_nolimit<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        Visitor::new().visit_pre_nolimit(self, f)
    }

    fn visit_mut_pre<F>(&mut self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&mut Self),
    {
        Visitor::new().visit_mut_pre(self, f)
    }

    fn visit_mut_pre_nolimit<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        Visitor::new().visit_mut_pre_nolimit(self, f)
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

    fn visit_pre_post<F1, F2>(&self, pre: &mut F1, post: &mut F2) -> Result<(), RecursionLimitError>
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self),
    {
        Visitor::new().visit_pre_post(self, pre, post)
    }

    fn visit_pre_post_nolimit<F1, F2>(&self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self),
    {
        Visitor::new().visit_pre_post_nolimit(self, pre, post)
    }

    fn visit_mut_pre_post<F1, F2>(
        &mut self,
        pre: &mut F1,
        post: &mut F2,
    ) -> Result<(), RecursionLimitError>
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut Self>>,
        F2: FnMut(&mut Self),
    {
        Visitor::new().visit_mut_pre_post(self, pre, post)
    }

    fn visit_mut_pre_post_nolimit<F1, F2>(&mut self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut Self>>,
        F2: FnMut(&mut Self),
    {
        Visitor::new().visit_mut_pre_post_nolimit(self, pre, post)
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

impl<T: VisitChildren<T>> Visitor<T> {
    fn new() -> Self {
        Self {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
            _type: PhantomData,
        }
    }

    fn visit_post<F>(&self, value: &T, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&T),
    {
        self.checked_recur(move |_| {
            value.try_visit_children(|child| self.visit_post(child, f))?;
            f(value);
            Ok(())
        })
    }

    fn visit_post_nolimit<F>(&self, value: &T, f: &mut F)
    where
        F: FnMut(&T),
    {
        maybe_grow(|| {
            value.visit_children(|child| self.visit_post_nolimit(child, f));
            f(value)
        })
    }

    fn visit_mut_post<F>(&self, value: &mut T, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&mut T),
    {
        self.checked_recur(move |_| {
            value.try_visit_mut_children(|child| self.visit_mut_post(child, f))?;
            f(value);
            Ok(())
        })
    }

    fn visit_mut_post_nolimit<F>(&self, value: &mut T, f: &mut F)
    where
        F: FnMut(&mut T),
    {
        maybe_grow(|| {
            value.visit_mut_children(|child| self.visit_mut_post_nolimit(child, f));
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

    fn visit_pre<F>(&self, value: &T, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&T),
    {
        self.checked_recur(move |_| {
            f(value);
            value.try_visit_children(|child| self.visit_pre(child, f))
        })
    }

    fn visit_pre_nolimit<F>(&self, value: &T, f: &mut F)
    where
        F: FnMut(&T),
    {
        maybe_grow(|| {
            f(value);
            value.visit_children(|child| self.visit_pre_nolimit(child, f))
        })
    }

    fn visit_mut_pre<F>(&self, value: &mut T, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&mut T),
    {
        self.checked_recur(move |_| {
            f(value);
            value.try_visit_mut_children(|child| self.visit_mut_pre(child, f))
        })
    }

    fn visit_mut_pre_nolimit<F>(&self, value: &mut T, f: &mut F)
    where
        F: FnMut(&mut T),
    {
        maybe_grow(|| {
            f(value);
            value.visit_mut_children(|child| self.visit_mut_pre_nolimit(child, f))
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

    fn visit_pre_post<F1, F2>(
        &self,
        value: &T,
        pre: &mut F1,
        post: &mut F2,
    ) -> Result<(), RecursionLimitError>
    where
        F1: FnMut(&T) -> Option<Vec<&T>>,
        F2: FnMut(&T),
    {
        self.checked_recur(move |_| {
            if let Some(to_visit) = pre(value) {
                for child in to_visit {
                    self.visit_pre_post(child, pre, post)?;
                }
            } else {
                value.try_visit_children(|child| self.visit_pre_post(child, pre, post))?;
            }
            post(value);
            Ok(())
        })
    }

    fn visit_pre_post_nolimit<F1, F2>(&self, value: &T, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&T) -> Option<Vec<&T>>,
        F2: FnMut(&T),
    {
        maybe_grow(|| {
            if let Some(to_visit) = pre(value) {
                for child in to_visit {
                    self.visit_pre_post_nolimit(child, pre, post);
                }
            } else {
                value.visit_children(|child| self.visit_pre_post_nolimit(child, pre, post));
            }
            post(value);
        })
    }

    fn visit_mut_pre_post<F1, F2>(
        &self,
        value: &mut T,
        pre: &mut F1,
        post: &mut F2,
    ) -> Result<(), RecursionLimitError>
    where
        F1: FnMut(&mut T) -> Option<Vec<&mut T>>,
        F2: FnMut(&mut T),
    {
        self.checked_recur(move |_| {
            if let Some(to_visit) = pre(value) {
                for child in to_visit {
                    self.visit_mut_pre_post(child, pre, post)?;
                }
            } else {
                value.try_visit_mut_children(|child| self.visit_mut_pre_post(child, pre, post))?;
            }
            post(value);
            Ok(())
        })
    }

    fn visit_mut_pre_post_nolimit<F1, F2>(&self, value: &mut T, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&mut T) -> Option<Vec<&mut T>>,
        F2: FnMut(&mut T),
    {
        maybe_grow(|| {
            if let Some(to_visit) = pre(value) {
                for child in to_visit {
                    self.visit_mut_pre_post_nolimit(child, pre, post);
                }
            } else {
                value.visit_mut_children(|child| self.visit_mut_pre_post_nolimit(child, pre, post));
            }
            post(value);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    enum A {
        Add(Box<A>, Box<A>),
        Lit(u64),
        FrB(Box<B>),
    }

    #[derive(Debug, Eq, PartialEq)]
    enum B {
        Mul(Box<B>, Box<B>),
        Lit(u64),
        FrA(Box<A>),
    }

    impl VisitChildren<A> for A {
        fn visit_children<F>(&self, mut f: F)
        where
            F: FnMut(&A),
        {
            VisitChildren::visit_children(self, |expr: &B| {
                #[allow(deprecated)]
                Visit::visit_post_nolimit(expr, &mut |expr| match expr {
                    B::FrA(expr) => f(expr.as_ref()),
                    _ => (),
                });
            });

            match self {
                A::Add(lhs, rhs) => {
                    f(lhs);
                    f(rhs);
                }
                A::Lit(_) => (),
                A::FrB(_) => (),
            }
        }

        fn visit_mut_children<F>(&mut self, mut f: F)
        where
            F: FnMut(&mut A),
        {
            VisitChildren::visit_mut_children(self, |expr: &mut B| {
                #[allow(deprecated)]
                Visit::visit_mut_post_nolimit(expr, &mut |expr| match expr {
                    B::FrA(expr) => f(expr.as_mut()),
                    _ => (),
                });
            });

            match self {
                A::Add(lhs, rhs) => {
                    f(lhs);
                    f(rhs);
                }
                A::Lit(_) => (),
                A::FrB(_) => (),
            }
        }

        fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
        where
            F: FnMut(&A) -> Result<(), E>,
            E: From<RecursionLimitError>,
        {
            VisitChildren::try_visit_children(self, |expr: &B| {
                Visit::try_visit_post(expr, &mut |expr| match expr {
                    B::FrA(expr) => f(expr.as_ref()),
                    _ => Ok(()),
                })
            })?;

            match self {
                A::Add(lhs, rhs) => {
                    f(lhs)?;
                    f(rhs)?;
                }
                A::Lit(_) => (),
                A::FrB(_) => (),
            }
            Ok(())
        }

        fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
        where
            F: FnMut(&mut A) -> Result<(), E>,
            E: From<RecursionLimitError>,
        {
            VisitChildren::try_visit_mut_children(self, |expr: &mut B| {
                Visit::try_visit_mut_post(expr, &mut |expr| match expr {
                    B::FrA(expr) => f(expr.as_mut()),
                    _ => Ok(()),
                })
            })?;

            match self {
                A::Add(lhs, rhs) => {
                    f(lhs)?;
                    f(rhs)?;
                }
                A::Lit(_) => (),
                A::FrB(_) => (),
            }
            Ok(())
        }
    }

    impl VisitChildren<B> for A {
        fn visit_children<F>(&self, mut f: F)
        where
            F: FnMut(&B),
        {
            match self {
                A::Add(_, _) => (),
                A::Lit(_) => (),
                A::FrB(expr) => f(expr),
            }
        }

        fn visit_mut_children<F>(&mut self, mut f: F)
        where
            F: FnMut(&mut B),
        {
            match self {
                A::Add(_, _) => (),
                A::Lit(_) => (),
                A::FrB(expr) => f(expr),
            }
        }

        fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
        where
            F: FnMut(&B) -> Result<(), E>,
            E: From<RecursionLimitError>,
        {
            match self {
                A::Add(_, _) => Ok(()),
                A::Lit(_) => Ok(()),
                A::FrB(expr) => f(expr),
            }
        }

        fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
        where
            F: FnMut(&mut B) -> Result<(), E>,
            E: From<RecursionLimitError>,
        {
            match self {
                A::Add(_, _) => Ok(()),
                A::Lit(_) => Ok(()),
                A::FrB(expr) => f(expr),
            }
        }
    }

    impl VisitChildren<B> for B {
        fn visit_children<F>(&self, mut f: F)
        where
            F: FnMut(&B),
        {
            // VisitChildren::visit_children(self, |expr: &A| {
            //     #[allow(deprecated)]
            //     Visit::visit_post_nolimit(expr, &mut |expr| match expr {
            //         A::FrB(expr) => f(expr.as_ref()),
            //         _ => (),
            //     });
            // });

            match self {
                B::Mul(lhs, rhs) => {
                    f(lhs);
                    f(rhs);
                }
                B::Lit(_) => (),
                B::FrA(_) => (),
            }
        }

        fn visit_mut_children<F>(&mut self, mut f: F)
        where
            F: FnMut(&mut B),
        {
            // VisitChildren::visit_mut_children(self, |expr: &mut A| {
            //     #[allow(deprecated)]
            //     Visit::visit_mut_post_nolimit(expr, &mut |expr| match expr {
            //         A::FrB(expr) => f(expr.as_mut()),
            //         _ => (),
            //     });
            // });

            match self {
                B::Mul(lhs, rhs) => {
                    f(lhs);
                    f(rhs);
                }
                B::Lit(_) => (),
                B::FrA(_) => (),
            }
        }

        fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
        where
            F: FnMut(&B) -> Result<(), E>,
            E: From<RecursionLimitError>,
        {
            // VisitChildren::try_visit_children(self, |expr: &A| {
            //     Visit::try_visit_post(expr, &mut |expr| match expr {
            //         A::FrB(expr) => f(expr.as_ref()),
            //         _ => Ok(()),
            //     })
            // })?;

            match self {
                B::Mul(lhs, rhs) => {
                    f(lhs)?;
                    f(rhs)?;
                }
                B::Lit(_) => (),
                B::FrA(_) => (),
            }
            Ok(())
        }

        fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
        where
            F: FnMut(&mut B) -> Result<(), E>,
            E: From<RecursionLimitError>,
        {
            // VisitChildren::try_visit_mut_children(self, |expr: &mut A| {
            //     Visit::try_visit_mut_post(expr, &mut |expr| match expr {
            //         A::FrB(expr) => f(expr.as_mut()),
            //         _ => Ok(()),
            //     })
            // })?;

            match self {
                B::Mul(lhs, rhs) => {
                    f(lhs)?;
                    f(rhs)?;
                }
                B::Lit(_) => (),
                B::FrA(_) => (),
            }
            Ok(())
        }
    }

    impl VisitChildren<A> for B {
        fn visit_children<F>(&self, mut f: F)
        where
            F: FnMut(&A),
        {
            match self {
                B::Mul(_, _) => (),
                B::Lit(_) => (),
                B::FrA(expr) => f(expr),
            }
        }

        fn visit_mut_children<F>(&mut self, mut f: F)
        where
            F: FnMut(&mut A),
        {
            match self {
                B::Mul(_, _) => (),
                B::Lit(_) => (),
                B::FrA(expr) => f(expr),
            }
        }

        fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
        where
            F: FnMut(&A) -> Result<(), E>,
            E: From<RecursionLimitError>,
        {
            match self {
                B::Mul(_, _) => Ok(()),
                B::Lit(_) => Ok(()),
                B::FrA(expr) => f(expr),
            }
        }

        fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
        where
            F: FnMut(&mut A) -> Result<(), E>,
            E: From<RecursionLimitError>,
        {
            match self {
                B::Mul(_, _) => Ok(()),
                B::Lit(_) => Ok(()),
                B::FrA(expr) => f(expr),
            }
        }
    }

    /// x + (y + z)
    fn test_term_a(x: A, y: A, z: A) -> A {
        let x = Box::new(x);
        let y = Box::new(y);
        let z = Box::new(z);
        A::Add(x, Box::new(A::Add(y, z)))
    }

    /// u + (v + w)
    fn test_term_b(u: B, v: B, w: B) -> B {
        let u = Box::new(u);
        let v = Box::new(v);
        let w = Box::new(w);
        B::Mul(u, Box::new(B::Mul(v, w)))
    }

    fn a_to_b(x: A) -> B {
        B::FrA(Box::new(x))
    }

    fn b_to_a(x: B) -> A {
        A::FrB(Box::new(x))
    }

    fn test_term_rec_b(b: u64) -> B {
        test_term_b(
            a_to_b(test_term_a(
                b_to_a(test_term_b(B::Lit(b + 11), B::Lit(b + 12), B::Lit(b + 13))),
                b_to_a(test_term_b(B::Lit(b + 14), B::Lit(b + 15), B::Lit(b + 16))),
                b_to_a(test_term_b(B::Lit(b + 17), B::Lit(b + 18), B::Lit(b + 19))),
            )),
            a_to_b(test_term_a(
                b_to_a(test_term_b(B::Lit(b + 21), B::Lit(b + 22), B::Lit(b + 23))),
                b_to_a(test_term_b(B::Lit(b + 24), B::Lit(b + 25), B::Lit(b + 26))),
                b_to_a(test_term_b(B::Lit(b + 27), B::Lit(b + 28), B::Lit(b + 29))),
            )),
            a_to_b(test_term_a(
                b_to_a(test_term_b(B::Lit(b + 31), B::Lit(b + 32), B::Lit(b + 33))),
                b_to_a(test_term_b(B::Lit(b + 34), B::Lit(b + 35), B::Lit(b + 36))),
                b_to_a(test_term_b(B::Lit(b + 37), B::Lit(b + 38), B::Lit(b + 39))),
            )),
        )
    }

    fn test_term_rec_a(b: u64) -> A {
        test_term_a(
            b_to_a(test_term_b(
                a_to_b(test_term_a(A::Lit(b + 11), A::Lit(b + 12), A::Lit(b + 13))),
                a_to_b(test_term_a(A::Lit(b + 14), A::Lit(b + 15), A::Lit(b + 16))),
                a_to_b(test_term_a(A::Lit(b + 17), A::Lit(b + 18), A::Lit(b + 19))),
            )),
            b_to_a(test_term_b(
                a_to_b(test_term_a(A::Lit(b + 21), A::Lit(b + 22), A::Lit(b + 23))),
                a_to_b(test_term_a(A::Lit(b + 24), A::Lit(b + 25), A::Lit(b + 26))),
                a_to_b(test_term_a(A::Lit(b + 27), A::Lit(b + 28), A::Lit(b + 29))),
            )),
            b_to_a(test_term_b(
                a_to_b(test_term_a(A::Lit(b + 31), A::Lit(b + 32), A::Lit(b + 33))),
                a_to_b(test_term_a(A::Lit(b + 34), A::Lit(b + 35), A::Lit(b + 36))),
                a_to_b(test_term_a(A::Lit(b + 37), A::Lit(b + 38), A::Lit(b + 39))),
            )),
        )
    }

    #[test]
    fn test_recursive_types_a() {
        let mut act = test_term_rec_a(0);
        let exp = test_term_rec_a(20);

        let res = act.visit_mut_pre(&mut |expr| match expr {
            A::Lit(x) => *x = *x + 20,
            _ => (),
        });

        assert!(res.is_ok());
        assert_eq!(act, exp);
    }

    /// This test currently fails with the following error:
    ///
    ///   reached the recursion limit while instantiating
    ///   `<visit::Visitor<B> as CheckedRec...ore::stack::RecursionLimitError>`
    ///
    /// The problem (I think) is in the fact that the lambdas passed in the
    /// VisitChildren<A> for A and the VisitChildren<B> for B definitions end
    /// up in an infinite loop.
    ///
    /// More specifically, we run into the following cycle:
    ///
    /// - `<A as VisitChildren<A>>::visit_children`
    ///   - <A as VisitChildren<B>>::visit_children`
    ///     - <B as Visit>::visit_post_nolimit`
    ///       - <B as VisitChildren<B>>::visit_children`
    ///         - <B as VisitChildren<A>>::visit_children`
    ///           - <A as Visit>::visit_post_nolimit`
    ///             - <A as VisitChildren<A>>::visit_children`
    #[test]
    #[ignore = "making the VisitChildren definitions symmetric breaks the compiler"]
    fn test_recursive_types_b() {
        let mut act = test_term_rec_b(0);
        let exp = test_term_rec_b(30);

        let res = act.visit_mut_pre(&mut |expr| match expr {
            B::Lit(x) => *x = *x + 30,
            _ => (),
        });

        assert!(res.is_ok());
        assert_eq!(act, exp);
    }
}
