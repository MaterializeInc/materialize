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
//!   * no suffix: recursively visit children in pre- and post-order
//!     using a ~Visitor~` that encapsulates the shared context.

use std::marker::PhantomData;

use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError, maybe_grow};

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
/// Doing this will result in recursion limit violations as indicated
/// in the accompanying `test_recursive_types_b` test.
pub trait VisitChildren<T> {
    /// Apply an infallible immutable function `f` to each direct child.
    fn visit_children<F>(&self, f: F)
    where
        F: FnMut(&T),
    {
        self.children().into_iter().for_each(f);
    }

    /// Apply an infallible mutable function `f` to each direct child.
    fn visit_mut_children<F>(&mut self, f: F)
    where
        F: FnMut(&mut T),
    {
        self.children_mut().into_iter().for_each(f);
    }

    /// Apply a fallible immutable function `f` to each direct child.
    ///
    /// For mutually recursive implementations (say consisting of two
    /// types `A` and `B`), recursing through `B` in order to find all
    /// `A`-children of a node of type `A` might cause lead to a
    /// [`RecursionLimitError`], hence the bound on `E`.
    fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&T) -> Result<(), E>,
    {
        for child in self.children() {
            f(child)?;
        }

        Ok(())
    }

    /// Apply a fallible mutable function `f` to each direct child.
    ///
    /// For mutually recursive implementations (say consisting of two
    /// types `A` and `B`), recursing through `B` in order to find all
    /// `A`-children of a node of type `A` might cause lead to a
    /// [`RecursionLimitError`], hence the bound on `E`.
    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut T) -> Result<(), E>,
    {
        for child in self.children_mut() {
            f(child)?;
        }

        Ok(())
    }

    // The `T`-typed children of this element.
    fn children(&self) -> Vec<&T>;

    // The `&mut T`-typed children of this element.
    fn children_mut(&mut self) -> Vec<&mut T>;
}

/// A trait for types that can recursively visit their children of the
/// same type.
///
/// This trait is automatically implemented for all implementors of
/// [`VisitChildren`].
///
/// All methods provided by this trait are iterative.
///
/// NB that any trait with post-traversal uses unsafe code---any shenanigans
/// in the visitor could result in unsoundness. Ordinary rust code in the visitor
/// that does not move pointers in the structure are fine.
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
        F: FnMut(&Self) -> Result<(), E>;

    /// Post-order mutable fallible visitor for `self`.
    fn try_visit_mut_post<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>;

    /// Pre-order immutable infallible visitor for `self`.
    fn visit_pre<F>(&self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&Self);

    /// Pre-order immutable infallible visitor for `self`, which also accumulates context
    /// information along the path from the root to the current node's parent.
    /// `acc_fun` is a similar closure as in `fold`. The accumulated context is passed to the
    /// visitor, along with the current node.
    ///
    /// For example, one can use this on a `MirScalarExpr` to tell the visitor whether the current
    /// subexpression has a negation somewhere above it.
    ///
    /// When using it on a `MirRelationExpr`, one has to be mindful that `Let` bindings are not
    /// followed, i.e., the context won't include what happens with a `Let` binding in some other
    /// `MirRelationExpr` where the binding occurs in a `Get`.
    fn visit_pre_with_context<Context, AccFun, Visitor>(
        &self,
        init: Context,
        acc_fun: &mut AccFun,
        visitor: &mut Visitor,
    ) -> Result<(), RecursionLimitError>
    where
        Context: Clone,
        AccFun: FnMut(Context, &Self) -> Context,
        Visitor: FnMut(&Context, &Self);

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
        F: FnMut(&Self) -> Result<(), E>;

    /// Pre-order mutable fallible visitor for `self`.
    fn try_visit_mut_pre<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>;

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
    #[deprecated = "Use `visit` instead."]
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
    #[deprecated = "Use `visit_mut` instead."]
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

enum VisitAction<'a, T> {
    Enter(&'a T),
    Leave(&'a T),
}
enum VisitMutAction<T> {
    Enter(*mut T),
    Leave(*mut T),
}

impl<T: VisitChildren<T>> Visit for T {
    fn visit_post<F>(&self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&Self),
    {
        use VisitAction::*;
        let mut stack = vec![Enter(self)];
        while let Some(action) = stack.pop() {
            match action {
                Enter(elt) => {
                    stack.push(Leave(elt));
                    // Push children in reverse so they pop (and are visited) left-to-right.
                    stack.extend(elt.children().into_iter().rev().map(Enter));
                }
                Leave(elt) => f(elt),
            }
        }

        Ok(())
    }

    fn visit_post_nolimit<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        StackSafeVisit::new().visit_post_nolimit(self, f)
    }

    #[allow(clippy::as_conversions)]
    fn visit_mut_post<F>(&mut self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&mut Self),
    {
        // This code uses `unsafe`. The core safety argument is that:
        //
        // - `children_mut()` produces disjoint children
        // - no aliasing means each `Enter` is processed separately, and we `Leave` each node exactly once
        //
        // Put another way, our `stack` mirrors the function call stack, which allows multiple `&mut` refs at once,
        // since only one stack frame can be active at a time.

        use VisitMutAction::*;
        let mut stack = vec![Enter(self as *mut T)];
        while let Some(action) = stack.pop() {
            match action {
                Enter(ptr) => {
                    stack.push(Leave(ptr));
                    let elt = unsafe { &mut *ptr };
                    // Push children in reverse so they pop (and are visited) left-to-right.
                    stack.extend(
                        elt.children_mut()
                            .into_iter()
                            .rev()
                            .map(|child| Enter(child as *mut T)),
                    );
                }
                Leave(elt) => f(unsafe { &mut *elt }),
            }
        }

        Ok(())
    }

    #[allow(clippy::as_conversions)]
    fn visit_mut_post_nolimit<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        StackSafeVisit::new().visit_mut_post_nolimit(self, f)
    }

    fn try_visit_post<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
    {
        use VisitAction::*;
        let mut stack = vec![Enter(self)];
        while let Some(action) = stack.pop() {
            match action {
                Enter(elt) => {
                    stack.push(Leave(elt));
                    // Push children in reverse so they pop (and are visited) left-to-right.
                    stack.extend(elt.children().into_iter().rev().map(Enter));
                }
                Leave(elt) => f(elt)?,
            }
        }

        Ok(())
    }

    #[allow(clippy::as_conversions)]
    fn try_visit_mut_post<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
    {
        // This code uses `unsafe`. The core safety argument is that:
        //
        // - `children_mut()` produces disjoint children
        // - no aliasing means each `Enter` is processed separately, and we `Leave` each node exactly once
        //
        // Put another way, our `stack` mirrors the function call stack, which allows multiple `&mut` refs at once,
        // since only one stack frame can be active at a time.

        use VisitMutAction::*;
        let mut stack = vec![Enter(self as *mut T)];
        while let Some(action) = stack.pop() {
            match action {
                Enter(ptr) => {
                    stack.push(Leave(ptr));
                    let elt = unsafe { &mut *ptr };
                    // Push children in reverse so they pop (and are visited) left-to-right.
                    stack.extend(
                        elt.children_mut()
                            .into_iter()
                            .rev()
                            .map(|child| Enter(child as *mut T)),
                    );
                }
                Leave(ptr) => f(unsafe { &mut *ptr })?,
            }
        }

        Ok(())
    }

    fn visit_pre<F>(&self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&Self),
    {
        let mut stack = vec![self];
        while let Some(elt) = stack.pop() {
            f(elt);
            // Push children in reverse so they pop (and are visited) left-to-right.
            stack.extend(elt.children().into_iter().rev());
        }

        Ok(())
    }

    fn visit_pre_with_context<Context, AccFun, Visitor>(
        &self,
        init: Context,
        acc_fun: &mut AccFun,
        visitor: &mut Visitor,
    ) -> Result<(), RecursionLimitError>
    where
        Context: Clone,
        AccFun: FnMut(Context, &Self) -> Context,
        Visitor: FnMut(&Context, &Self),
    {
        let mut stack = vec![(self, init)];
        while let Some((elt, ctx)) = stack.pop() {
            visitor(&ctx, elt);
            let ctx = acc_fun(ctx, elt);
            // Push children in reverse so they pop (and are visited) left-to-right.
            stack.extend(
                elt.children()
                    .into_iter()
                    .rev()
                    .map(|child| (child, ctx.clone())),
            );
        }

        Ok(())
    }

    fn visit_pre_nolimit<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        StackSafeVisit::new().visit_pre_nolimit(self, f)
    }

    fn visit_mut_pre<F>(&mut self, f: &mut F) -> Result<(), RecursionLimitError>
    where
        F: FnMut(&mut Self),
    {
        let mut stack = vec![self];
        while let Some(elt) = stack.pop() {
            f(elt);
            // Push children in reverse so they pop (and are visited) left-to-right.
            stack.extend(elt.children_mut().into_iter().rev());
        }

        Ok(())
    }

    fn visit_mut_pre_nolimit<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        StackSafeVisit::new().visit_mut_pre_nolimit(self, f)
    }

    fn try_visit_pre<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
    {
        let mut stack = vec![self];
        while let Some(elt) = stack.pop() {
            f(elt)?;
            // Push children in reverse so they pop (and are visited) left-to-right.
            stack.extend(elt.children().into_iter().rev());
        }

        Ok(())
    }

    fn try_visit_mut_pre<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
    {
        let mut stack = vec![self];
        while let Some(elt) = stack.pop() {
            f(elt)?;
            // Push children in reverse so they pop (and are visited) left-to-right.
            stack.extend(elt.children_mut().into_iter().rev());
        }

        Ok(())
    }
    fn visit_pre_post<F1, F2>(&self, pre: &mut F1, post: &mut F2) -> Result<(), RecursionLimitError>
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self),
    {
        use VisitAction::*;
        let mut stack = vec![Enter(self)];
        while let Some(action) = stack.pop() {
            match action {
                Enter(elt) => {
                    let children = pre(elt).unwrap_or_else(|| elt.children());
                    stack.push(Leave(elt));
                    for child in children.into_iter().rev() {
                        stack.push(Enter(child));
                    }
                }
                Leave(elt) => {
                    post(elt);
                }
            }
        }

        Ok(())
    }

    fn visit_pre_post_nolimit<F1, F2>(&self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self),
    {
        StackSafeVisit::new().visit_pre_post_nolimit(self, pre, post)
    }

    #[allow(clippy::as_conversions)]
    fn visit_mut_pre_post<F1, F2>(
        &mut self,
        pre: &mut F1,
        post: &mut F2,
    ) -> Result<(), RecursionLimitError>
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut Self>>,
        F2: FnMut(&mut Self),
    {
        // This code uses `unsafe`. The core safety argument is that:
        //
        // - `children_mut()` produces disjoint children
        // - no aliasing means each `Enter` is processed separately, and we `Leave` each node exactly once
        // - even if `pre` modifies the pointer, we retake it before computing children
        //
        // Put another way, our `stack` mirrors the function call stack, which allows multiple `&mut` refs at once,
        // since only one stack frame can be active at a time.

        use VisitMutAction::*;
        let mut stack = vec![Enter(self as *mut T)];
        while let Some(action) = stack.pop() {
            match action {
                Enter(ptr) => {
                    let elt = unsafe { &mut *ptr };
                    let children: Vec<&mut T> = match pre(elt) {
                        Some(explicit) => explicit,
                        None => {
                            let elt = unsafe { &mut *ptr };
                            elt.children_mut()
                        }
                    };
                    stack.push(Leave(ptr));
                    for child in children.into_iter().rev() {
                        stack.push(Enter(child));
                    }
                }
                Leave(ptr) => {
                    post(unsafe { &mut *ptr });
                }
            }
        }

        Ok(())
    }

    fn visit_mut_pre_post_nolimit<F1, F2>(&mut self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut Self>>,
        F2: FnMut(&mut Self),
    {
        StackSafeVisit::new().visit_mut_pre_post_nolimit(self, pre, post)
    }
}

struct StackSafeVisit<T> {
    recursion_guard: RecursionGuard,
    _type: PhantomData<T>,
}

impl<T> CheckedRecursion for StackSafeVisit<T> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl<T: VisitChildren<T>> StackSafeVisit<T> {
    fn new() -> Self {
        Self {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
            _type: PhantomData,
        }
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

    fn visit_mut_post_nolimit<F>(&self, value: &mut T, f: &mut F)
    where
        F: FnMut(&mut T),
    {
        maybe_grow(|| {
            value.visit_mut_children(|child| self.visit_mut_post_nolimit(child, f));
            f(value)
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

    fn visit_mut_pre_nolimit<F>(&self, value: &mut T, f: &mut F)
    where
        F: FnMut(&mut T),
    {
        maybe_grow(|| {
            f(value);
            value.visit_mut_children(|child| self.visit_mut_pre_nolimit(child, f))
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
    use mz_ore::assert_ok;

    use super::*;

    // This test demonstrates how to build visitors for mutually recursive definitions.
    // The key move here is the `direct_sub_*` methods, which are worklist-based traversals
    // that find children of appropriate type.

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

    impl A {
        fn direct_sub_b(&self) -> Vec<&B> {
            let mut subs: Vec<&B> = vec![];

            let mut worklist = vec![self];
            while let Some(a) = worklist.pop() {
                match a {
                    A::Add(lhs, rhs) => {
                        worklist.push(&*lhs);
                        worklist.push(&*rhs);
                    }
                    A::Lit(_) => (),
                    A::FrB(b) => subs.push(&*b),
                }
            }

            subs
        }

        fn direct_sub_b_mut(&mut self) -> Vec<&mut B> {
            let mut subs: Vec<&mut B> = vec![];

            let mut worklist = vec![self];
            while let Some(a) = worklist.pop() {
                match a {
                    A::Add(lhs, rhs) => {
                        worklist.push(&mut **lhs);
                        worklist.push(&mut **rhs);
                    }
                    A::Lit(_) => (),
                    A::FrB(b) => subs.push(&mut **b),
                }
            }

            subs
        }
    }

    impl B {
        fn direct_sub_a(&self) -> Vec<&A> {
            let mut subs: Vec<&A> = vec![];

            let mut worklist = vec![self];
            while let Some(b) = worklist.pop() {
                match b {
                    B::Mul(lhs, rhs) => {
                        worklist.push(&*lhs);
                        worklist.push(&*rhs);
                    }
                    B::Lit(_) => (),
                    B::FrA(a) => subs.push(&*a),
                }
            }

            subs
        }

        fn direct_sub_a_mut(&mut self) -> Vec<&mut A> {
            let mut subs: Vec<&mut A> = vec![];

            let mut worklist = vec![self];
            while let Some(b) = worklist.pop() {
                match b {
                    B::Mul(lhs, rhs) => {
                        worklist.push(&mut **lhs);
                        worklist.push(&mut **rhs);
                    }
                    B::Lit(_) => (),
                    B::FrA(a) => subs.push(&mut **a),
                }
            }

            subs
        }
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

        fn children(&self) -> Vec<&A> {
            let mut v: Vec<&A> = vec![];

            match self {
                A::Add(lhs, rhs) => {
                    v.push(&*lhs);
                    v.push(&*rhs)
                }
                A::Lit(_) => (),
                A::FrB(b) => {
                    v.append(&mut b.direct_sub_a());
                }
            }

            v
        }

        fn children_mut(&mut self) -> Vec<&mut A> {
            let mut v: Vec<&mut A> = vec![];

            match self {
                A::Add(lhs, rhs) => {
                    v.push(&mut **lhs);
                    v.push(&mut **rhs)
                }
                A::Lit(_) => (),
                A::FrB(b) => {
                    v.append(&mut b.direct_sub_a_mut());
                }
            }

            v
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
        {
            match self {
                A::Add(_, _) => Ok(()),
                A::Lit(_) => Ok(()),
                A::FrB(expr) => f(expr),
            }
        }

        fn children(&self) -> Vec<&B> {
            match self {
                A::Add(_, _) | A::Lit(_) => vec![],
                A::FrB(b) => vec![&*b],
            }
        }

        fn children_mut(&mut self) -> Vec<&mut B> {
            match self {
                A::Add(_, _) | A::Lit(_) => vec![],
                A::FrB(b) => vec![&mut **b],
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

        fn children(&self) -> Vec<&B> {
            let mut v: Vec<&B> = vec![];
            match self {
                B::Mul(lhs, rhs) => {
                    v.push(&*lhs);
                    v.push(&*rhs);
                }
                B::Lit(_) => (),
                B::FrA(a) => v.append(&mut a.direct_sub_b()),
            }
            v
        }

        fn children_mut(&mut self) -> Vec<&mut B> {
            let mut v: Vec<&mut B> = vec![];
            match self {
                B::Mul(lhs, rhs) => {
                    v.push(&mut **lhs);
                    v.push(&mut **rhs);
                }
                B::Lit(_) => (),
                B::FrA(a) => v.append(&mut a.direct_sub_b_mut()),
            }
            v
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
        {
            match self {
                B::Mul(_, _) => Ok(()),
                B::Lit(_) => Ok(()),
                B::FrA(expr) => f(expr),
            }
        }

        fn children(&self) -> Vec<&A> {
            match self {
                B::Mul(_, _) | B::Lit(_) => vec![],
                B::FrA(a) => vec![&*a],
            }
        }

        fn children_mut(&mut self) -> Vec<&mut A> {
            match self {
                B::Mul(_, _) | B::Lit(_) => vec![],
                B::FrA(a) => vec![&mut **a],
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

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn test_recursive_types_a() {
        let mut act = test_term_rec_a(0);
        let exp = test_term_rec_a(20);

        let res = act.visit_mut_pre(&mut |expr| match expr {
            A::Lit(x) => *x = *x + 20,
            _ => (),
        });

        assert_ok!(res);
        assert_eq!(act, exp);
    }

    #[mz_ore::test]
    fn test_recursive_types_b() {
        let mut act = test_term_rec_b(0);
        let exp = test_term_rec_b(30);

        let res = act.visit_mut_pre(&mut |expr| match expr {
            B::Lit(x) => *x = *x + 30,
            _ => (),
        });

        assert_ok!(res);
        assert_eq!(act, exp);
    }
}
