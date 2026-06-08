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
//! trait [`Visit`] then adds support for iteratively traversing
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
        self.children().for_each(f);
    }

    /// Apply an infallible mutable function `f` to each direct child.
    fn visit_mut_children<F>(&mut self, f: F)
    where
        F: FnMut(&mut T),
    {
        self.children_mut().for_each(f);
    }

    /// Apply a fallible immutable function `f` to each direct child.
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
    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut T) -> Result<(), E>,
    {
        for child in self.children_mut() {
            f(child)?;
        }

        Ok(())
    }

    /// The `T`-typed children of this element.
    fn children<'a>(&'a self) -> impl DoubleEndedIterator<Item = &'a T>
    where
        T: 'a;

    /// The `&mut T`-typed children of this element.
    ///
    /// It is critical for the safety of mutable post-order traversals that this
    /// function be written using safe code.
    fn children_mut<'a>(&'a mut self) -> impl DoubleEndedIterator<Item = &'a mut T>
    where
        T: 'a;
}

/// A trait for types that can recursively visit their children of the
/// same type.
///
/// This trait is automatically implemented for all implementors of
/// [`VisitChildren`].
///
/// All methods provided by this trait are iterative.
///
/// NB that any visitor with mutable post-traversal uses unsafe code. It is critical
/// that `VisitChildren::children_mut` be written using safe code, i.e., no aliasing
/// of children or access to parents.
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
        F: FnMut(&Self) -> Result<(), E>;

    /// Post-order mutable fallible visitor for `self`.
    fn try_visit_mut_post<F, E>(&mut self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>;

    /// Pre-order immutable infallible visitor for `self`.
    fn visit_pre<F>(&self, f: &mut F)
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
    ) where
        Context: Clone,
        AccFun: FnMut(Context, &Self) -> Context,
        Visitor: FnMut(&Context, &Self);

    /// Pre-order mutable infallible visitor for `self`.
    fn visit_mut_pre<F>(&mut self, f: &mut F)
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
    ///
    /// It is important for safety that `pre` is (a) safe code and (b) returns children only.
    fn visit_mut_pre_post<F1, F2>(&mut self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut Self>>,
        F2: FnMut(&mut Self);
}

/// Frames for immutable post-traversals, will be kept in a stack.
enum VisitAction<'a, T> {
    /// Put on the stack when entering a node.
    ///
    /// Causes us to push children.
    Enter(&'a T),
    /// Put on the stack when leaving a node, all children visited.
    ///
    /// Causes us to do the post-traversal visit of the parent.
    Leave(&'a T),
}

/// Frames for mutable post-traversals, will be kept in a stack.
///
/// Notice that we use mutable pointers, because mutable post-traversal is unsafe in rust.
///
/// The core argument for correctness mirrors the tree-borrow correctness argument Rust's
/// borrow checker uses for the ordinary function call stack. Loosely:
///
///  - We split a mutable parent node into its children, and put them on the stack.
///  - We keep a mutable pointer to the parent node, but won't touch it until we're done with all children.
///    + In the function call stack, this mutable pointer is the stack frame, safely inaccessible.
///    + In our `unsafe` action stack, this mutable pointer is below all of the `Enter` actions,
///      and we promise not to touch it until they complete.
///  - When we have processed all children, we can reassemble access to the parent from its parts.
///    + In the function call stack, we do this on return from recursive calls.
///    - In our `unsafe` action stack, we do this after popping all children.
enum VisitMutAction<T> {
    /// Put on the stack when entering a node.
    ///
    /// Causes us to push children.
    Enter(*mut T),
    /// Put on the stack when leaving a node, all children visited.
    ///
    /// Causes us to do the post-traversal visit of the parent.
    Leave(*mut T),
}

impl<T: VisitChildren<T>> Visit for T {
    fn visit_post<F>(&self, f: &mut F)
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
                    stack.extend(elt.children().rev().map(Enter));
                }
                Leave(elt) => f(elt),
            }
        }
    }

    #[allow(clippy::as_conversions)]
    fn visit_mut_post<F>(&mut self, f: &mut F)
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
                    stack.extend(elt.children_mut().rev().map(|child| Enter(child as *mut T)));
                }
                Leave(elt) => f(unsafe { &mut *elt }),
            }
        }
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
                    stack.extend(elt.children().rev().map(Enter));
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
                    stack.extend(elt.children_mut().rev().map(|child| Enter(child as *mut T)));
                }
                Leave(ptr) => f(unsafe { &mut *ptr })?,
            }
        }

        Ok(())
    }

    fn visit_pre<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        let mut stack = vec![self];
        while let Some(elt) = stack.pop() {
            f(elt);
            // Push children in reverse so they pop (and are visited) left-to-right.
            stack.extend(elt.children().rev());
        }
    }

    fn visit_pre_with_context<Context, AccFun, Visitor>(
        &self,
        init: Context,
        acc_fun: &mut AccFun,
        visitor: &mut Visitor,
    ) where
        Context: Clone,
        AccFun: FnMut(Context, &Self) -> Context,
        Visitor: FnMut(&Context, &Self),
    {
        let mut stack = vec![(self, init)];
        while let Some((elt, ctx)) = stack.pop() {
            visitor(&ctx, elt);
            let ctx = acc_fun(ctx, elt);
            // Push children in reverse so they pop (and are visited) left-to-right.
            stack.extend(elt.children().rev().map(|child| (child, ctx.clone())));
        }
    }

    fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        let mut stack = vec![self];
        while let Some(elt) = stack.pop() {
            f(elt);
            // Push children in reverse so they pop (and are visited) left-to-right.
            stack.extend(elt.children_mut().rev())
        }
    }

    fn try_visit_pre<F, E>(&self, f: &mut F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
    {
        let mut stack = vec![self];
        while let Some(elt) = stack.pop() {
            f(elt)?;
            // Push children in reverse so they pop (and are visited) left-to-right.
            stack.extend(elt.children().rev());
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
            stack.extend(elt.children_mut().rev());
        }

        Ok(())
    }
    fn visit_pre_post<F1, F2>(&self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&Self) -> Option<Vec<&Self>>,
        F2: FnMut(&Self),
    {
        use VisitAction::*;
        let mut stack = vec![Enter(self)];
        while let Some(action) = stack.pop() {
            match action {
                Enter(elt) => {
                    stack.push(Leave(elt));
                    if let Some(children) = pre(elt) {
                        for child in children.into_iter().rev() {
                            stack.push(Enter(child));
                        }
                    } else {
                        for child in elt.children().rev() {
                            stack.push(Enter(child));
                        }
                    }
                }
                Leave(elt) => {
                    post(elt);
                }
            }
        }
    }

    #[allow(clippy::as_conversions)]
    fn visit_mut_pre_post<F1, F2>(&mut self, pre: &mut F1, post: &mut F2)
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
                    stack.push(Leave(ptr));

                    if let Some(children) = pre(elt) {
                        for child in children.into_iter().rev() {
                            stack.push(Enter(child));
                        }
                    } else {
                        let elt = unsafe { &mut *ptr };
                        for child in elt.children_mut().rev() {
                            stack.push(Enter(child));
                        }
                    }
                }
                Leave(ptr) => {
                    post(unsafe { &mut *ptr });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
                Visit::visit_post(expr, &mut |expr| match expr {
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
                Visit::visit_mut_post(expr, &mut |expr| match expr {
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

        fn children<'a>(&'a self) -> impl DoubleEndedIterator<Item = &'a A>
        where
            A: 'a,
        {
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

            v.into_iter()
        }

        fn children_mut<'a>(&'a mut self) -> impl DoubleEndedIterator<Item = &'a mut A>
        where
            A: 'a,
        {
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

            v.into_iter()
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

        fn children<'a>(&'a self) -> impl DoubleEndedIterator<Item = &'a B>
        where
            B: 'a,
        {
            let mut child: Option<&B> = None;
            match self {
                A::Add(_, _) | A::Lit(_) => (),
                A::FrB(b) => child = Some(&*b),
            }
            child.into_iter()
        }

        fn children_mut<'a>(&'a mut self) -> impl DoubleEndedIterator<Item = &'a mut B>
        where
            B: 'a,
        {
            let mut child: Option<&mut B> = None;
            match self {
                A::Add(_, _) | A::Lit(_) => (),
                A::FrB(b) => child = Some(&mut **b),
            }
            child.into_iter()
        }
    }

    impl VisitChildren<B> for B {
        fn visit_children<F>(&self, mut f: F)
        where
            F: FnMut(&B),
        {
            // VisitChildren::visit_children(self, |expr: &A| {
            //     #[allow(deprecated)]
            //     Visit::visit_post(expr, &mut |expr| match expr {
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

        fn children<'a>(&'a self) -> impl DoubleEndedIterator<Item = &'a B>
        where
            B: 'a,
        {
            let mut v: Vec<&B> = vec![];
            match self {
                B::Mul(lhs, rhs) => {
                    v.push(&*lhs);
                    v.push(&*rhs);
                }
                B::Lit(_) => (),
                B::FrA(a) => v.append(&mut a.direct_sub_b()),
            }
            v.into_iter()
        }

        fn children_mut<'a>(&'a mut self) -> impl DoubleEndedIterator<Item = &'a mut B>
        where
            B: 'a,
        {
            let mut v: Vec<&mut B> = vec![];
            match self {
                B::Mul(lhs, rhs) => {
                    v.push(&mut **lhs);
                    v.push(&mut **rhs);
                }
                B::Lit(_) => (),
                B::FrA(a) => v.append(&mut a.direct_sub_b_mut()),
            }
            v.into_iter()
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

        fn children<'a>(&'a self) -> impl DoubleEndedIterator<Item = &'a A>
        where
            A: 'a,
        {
            let mut child: Option<&A> = None;
            match self {
                B::Mul(_, _) | B::Lit(_) => (),
                B::FrA(a) => child = Some(&*a),
            }
            child.into_iter()
        }

        fn children_mut<'a>(&'a mut self) -> impl DoubleEndedIterator<Item = &'a mut A>
        where
            A: 'a,
        {
            let mut child: Option<&mut A> = None;
            match self {
                B::Mul(_, _) | B::Lit(_) => (),
                B::FrA(a) => child = Some(&mut **a),
            }
            child.into_iter()
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

        act.visit_mut_pre(&mut |expr| match expr {
            A::Lit(x) => *x = *x + 20,
            _ => (),
        });

        assert_eq!(act, exp);
    }

    #[mz_ore::test]
    fn test_recursive_types_b() {
        let mut act = test_term_rec_b(0);
        let exp = test_term_rec_b(30);

        act.visit_mut_pre(&mut |expr| match expr {
            B::Lit(x) => *x = *x + 30,
            _ => (),
        });

        assert_eq!(act, exp);
    }
}
