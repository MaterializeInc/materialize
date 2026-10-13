//! Iterative application of a differential dataflow fragment.
//!
//! The `iterate` operator takes as an argument a closure from a differential dataflow collection
//! to a collection of the same type. The output collection is the result of applying this closure
//! an unbounded number of times.
//!
//! The implementation of `iterate` does not directly apply the closure, but rather establishes an
//! iterative timely dataflow subcomputation, in which differences circulate until they dissipate
//! (indicating that the computation has reached fixed point), or until some number of iterations
//! have passed.
//!
//! **Note**: The dataflow assembled by `iterate` does not automatically insert `consolidate` for
//! you. This means that either (i) you should insert one yourself, (ii) you should be certain that
//! all paths from the input to the output of the loop involve consolidation, or (iii) you should
//! be worried that logically cancelable differences may circulate indefinitely.
//!
//! # Details
//!
//! The `iterate` method is written using a `Variable`, which lets you define your own iterative
//! computations when `iterate` itself is not sufficient. This can happen when you have two
//! collections that should evolve simultaneously, or when you would like to rotate your loop and
//! return an intermediate result.
//!
//! Using `Variable` requires more explicit arrangement of your computation, but isn't much more
//! complicated. You must define a new variable from an existing stream (its initial value), and
//! then set it to be a function of this variable (and perhaps other collections and variables).
//!
//! A `Variable` dereferences to a `Collection`, the one corresponding to its value in each iteration,
//! and it can be used in most situations where a collection can be used. The act of setting a
//! `Variable` consumes it and returns the corresponding `Collection`, preventing you from setting
//! it multiple times.

use std::fmt::Debug;

use timely::Container;
use timely::progress::Timestamp;
use timely::order::Product;

use timely::dataflow::*;
use timely::dataflow::scope::Iterative;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::feedback::Handle;

use crate::{Data, VecCollection, Collection};
use crate::difference::{Semigroup, Abelian};
use crate::lattice::Lattice;

/// An extension trait for the `iterate` method.
pub trait Iterate<'scope, T: Timestamp + Lattice, D: Data, R: Semigroup> {
    /// Iteratively apply `logic` to the source collection until convergence.
    ///
    /// Importantly, this method does not automatically consolidate results.
    /// It may be important to conclude the closure you supply with `consolidate()` to ensure that
    /// logically empty collections that contain cancelling records do not result in non-termination.
    /// Operators like `reduce`, `distinct`, and `count` also perform consolidation, and are safe to conclude with.
    ///
    /// The closure is also passed a copy of the inner scope, to facilitate importing external collections.
    /// It can also be acquired by calling `.scope()` on the closure's collection argument, but the code
    /// can be awkward to write fluently.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Iterate;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     scope.new_collection_from(1 .. 10u32).1
    ///          .iterate(|_scope, values| {
    ///              values.map(|x| if x % 2 == 0 { x/2 } else { x })
    ///                    .consolidate()
    ///          });
    /// });
    /// ```
    fn iterate<F>(self, logic: F) -> VecCollection<'scope, T, D, R>
    where
        for<'inner> F: FnOnce(Iterative<'inner, T, u64>, VecCollection<'inner, Product<T, u64>, D, R>)->VecCollection<'inner, Product<T, u64>, D, R>;
}

impl<'scope, T: Timestamp + Lattice, D: Ord+Data+Debug, R: Abelian+'static> Iterate<'scope, T, D, R> for VecCollection<'scope, T, D, R> {
    fn iterate<F>(self, logic: F) -> VecCollection<'scope, T, D, R>
    where
        for<'inner> F: FnOnce(Iterative<'inner, T, u64>, VecCollection<'inner, Product<T, u64>, D, R>)->VecCollection<'inner, Product<T, u64>, D, R>,
    {
        let outer = self.inner.scope();
        outer.scoped("Iterate", |subgraph| {
            // create a new variable, apply logic, bind variable, return.
            //
            // this could be much more succinct if we returned the collection
            // wrapped by `variable`, but it also results in substantially more
            // diffs produced; `result` is post-consolidation, and means fewer
            // records are yielded out of the loop.
            let (variable, collection) = Variable::new_from(self.enter(subgraph), Product::new(Default::default(), 1));
            let result = logic(subgraph, collection);
            variable.set(result.clone());
            result.leave(outer)
        })
    }
}

impl<'scope, T: Timestamp + Lattice, D: Ord+Data+Debug, R: Semigroup+'static> Iterate<'scope, T, D, R> for Scope<'scope, T> {
    fn iterate<F>(self, logic: F) -> VecCollection<'scope, T, D, R>
    where
        for<'inner> F: FnOnce(Iterative<'inner, T, u64>, VecCollection<'inner, Product<T, u64>, D, R>)->VecCollection<'inner, Product<T, u64>, D, R>,
    {
        let outer = self;
        self.scoped("Iterate", |subgraph| {
                // create a new variable, apply logic, bind variable, return.
                //
                // this could be much more succinct if we returned the collection
                // wrapped by `variable`, but it also results in substantially more
                // diffs produced; `result` is post-consolidation, and means fewer
                // records are yielded out of the loop.
                let (variable, collection) = Variable::new(subgraph, Product::new(Default::default(), 1));
                let result = logic(subgraph, collection);
                variable.set(result.clone());
                result.leave(outer)
            }
        )
    }
}

/// A recursively defined collection.
///
/// The `Variable` struct allows differential dataflow programs requiring more sophisticated
/// iterative patterns than singly recursive iteration. For example: in mutual recursion two
/// collections evolve simultaneously.
///
/// # Examples
///
/// The following example is equivalent to the example for the `Iterate` trait.
///
/// ```
/// use timely::order::Product;
/// use timely::dataflow::Scope;
///
/// use differential_dataflow::input::Input;
/// use differential_dataflow::operators::iterate::Variable;
///
/// ::timely::example(|scope| {
///
///     let numbers = scope.new_collection_from(1 .. 10u32).1;
///
///     scope.iterative::<u64,_,_>(|nested| {
///         let summary = Product::new(Default::default(), 1);
///         let (variable, collection) = Variable::new_from(numbers.enter(nested), summary);
///         let result = collection.map(|x| if x % 2 == 0 { x/2 } else { x })
///                                .consolidate();
///         variable.set(result.clone());
///         result.leave(scope)
///     });
/// })
/// ```
///
/// Variables support iterative patterns that can be both more flexible, and more efficient.
///
/// Mutual recursion is when one defines multiple variables in the same iterative context,
/// and their definitions are not independent. For example, odd numbers and even numbers
/// can be determined from each other, iteratively.
/// ```
/// use timely::order::Product;
/// use timely::dataflow::Scope;
///
/// use differential_dataflow::input::Input;
/// use differential_dataflow::operators::iterate::Variable;
///
/// ::timely::example(|scope| {
///
///     let numbers = scope.new_collection_from(10 .. 20u32).1;
///
///     scope.iterative::<u64,_,_>(|nested| {
///         let summary = Product::new(Default::default(), 1);
///         let (even_v, even) = Variable::new_from(numbers.clone().enter(nested).filter(|x| x % 2 == 0), summary);
///         let (odds_v, odds) = Variable::new_from(numbers.clone().enter(nested).filter(|x| x % 2 == 1), summary);
///         odds_v.set(even.clone().filter(|x| *x > 0).map(|x| x-1).concat(odds.clone()).distinct());
///         even_v.set(odds.clone().filter(|x| *x > 0).map(|x| x-1).concat(even.clone()).distinct());
///     });
/// })
/// ```
///
/// Direct construction can be more efficient than `iterate` when you know a way to directly
/// determine the changes to make to the initial collection, rather than simply adding that
/// collection, running your intended logic, and then subtracting the collection.
///
/// An an example, the logic in `identifiers.rs` looks for hash collisions, and tweaks the salt
/// for all but one element in each group of collisions. Most elements do not collide, and we
/// we don't need to circulate the non-colliding elements to confirm that they subtract away.
/// By iteratively developing a variable of the *edits* to the input, we can produce and circulate
/// a smaller volume of updates. This can be especially impactful when the initial collection is
/// large, and the edits to perform are relatively smaller.
pub struct Variable<'scope, T, C>
where
    T: Timestamp + Lattice,
    C: Container,
{
    feedback: Handle<'scope, T, C>,
    source: Option<Collection<'scope, T, C>>,
    step: T::Summary,
}

/// A `Variable` specialized to a vector container of update triples (data, time, diff).
pub type VecVariable<'scope, T, D, R> = Variable<'scope, T, Vec<(D, T, R)>>;

impl<'scope, T, C: Container> Variable<'scope, T, C>
where
    T: Timestamp + Lattice,
    C: crate::collection::containers::ResultsIn<T::Summary>,
{
    /// Creates a new initially empty `Variable` and its associated `Collection`.
    ///
    /// The collection should be used, along with other potentially recursive collections,
    /// to define a output collection to which the variable is then `set`.
    /// In an iterative context, each collection starts empty and are repeatedly updated by
    /// the logic used to produce the collection their variable is bound to. This process
    /// continues until no changes occur, at which point we have reached a fixed point (or
    /// the range of timestamps have been exhausted). Calling `leave()` on any collection
    /// will produce its fixed point in the outer scope.
    ///
    /// In a non-iterative scope the mechanics are the same, but the interpretation varies.
    pub fn new(scope: Scope<'scope, T>, step: T::Summary) -> (Self, Collection<'scope, T, C>) {
        let (feedback, updates) = scope.feedback(step.clone());
        let collection = Collection::<T, C>::new(updates);
        (Self { feedback, source: None, step }, collection)
    }

    /// Creates a new `Variable` and its associated `Collection`, initially `source`.
    ///
    /// This method is a short-cut for a pattern that one can write manually with `new()`,
    /// but which is easy enough to get wrong that the help is valuable.
    ///
    /// This pattern uses a variable `x` to develop `x = logic(x + source) - source`,
    /// which finds a fixed point `x` that satisfies `x + source = logic(x + source)`.
    /// The fixed point equals the repeated application of `logic` to `source` plus the
    ///
    /// To implement the pattern one would create a new initially empty variable with `new()`,
    /// then concatenate `source` into that collection, and use it as `logic` dictates.
    /// Just before the variable is set to the result collection, `source` is subtracted.
    ///
    /// If using this pattern manually, it is important to bear in mind that the collection
    /// that result from `logic` converges to its fixed point, but that once `source` is
    /// subtracted the collection converges to this limit minus `source`, a collection that
    /// may have records that accumulate to negative multiplicities, and for which the model
    /// of them as "data sets" may break down. Be careful when applying non-linear operations
    /// like `reduce` that they make sense when updates may have non-positive differences.
    ///
    /// Finally, implementing this pattern manually has the ability to more directly implement
    /// the logic `x = logic(x + source) - source`. If there is a different mechanism than just
    /// adding the source, doing the logic, then subtracting the source, it is appropriate to do.
    /// For example, if the logic modifies a few records it is possible to produce this update
    /// directly without using the backstop implementation this method provides.
    pub fn new_from(source: Collection<'scope, T, C>, step: T::Summary) -> (Self, Collection<'scope, T, C>) where C: Clone + crate::collection::containers::Negate {
        let (feedback, updates) = source.inner.scope().feedback(step.clone());
        let collection = Collection::<T, C>::new(updates).concat(source.clone());
        (Variable { feedback, source: Some(source.negate()), step }, collection)
    }

    /// Set the definition of the `Variable` to a collection.
    ///
    /// This method binds the `Variable` to be equal to the supplied collection,
    /// which may be recursively defined in terms of the variable itself.
    pub fn set(mut self, mut result: Collection<'scope, T, C>) {
        if let Some(source) = self.source.take() {
            result = result.concat(source);
        }
        result
            .results_in(self.step)
            .inner
            .connect_loop(self.feedback);
    }
}
