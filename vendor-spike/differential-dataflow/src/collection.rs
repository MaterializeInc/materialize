//! Types and traits associated with collections of data.
//!
//! The `Collection` type is differential dataflow's core abstraction for an updatable pile of data.
//!
//! Most differential dataflow programs are "collection-oriented", in the sense that they transform
//! one collection into another, using operators defined on collections. This contrasts with a more
//! imperative programming style, in which one might iterate through the contents of a collection
//! manually. The higher-level of programming allows differential dataflow to provide efficient
//! implementations, and to support efficient incremental updates to the collections.

use timely::Container;
use timely::progress::Timestamp;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::*;

use crate::difference::Abelian;

/// An evolving collection represented by a stream of abstract containers.
///
/// The containers purport to reperesent changes to a collection, and they must implement various traits
/// in order to expose some of this functionality (e.g. negation, timestamp manipulation). Other actions
/// on the containers, and streams of containers, are left to the container implementor to describe.
#[derive(Clone)]
pub struct Collection<'scope, T: Timestamp, C: 'static> {
    /// The underlying timely dataflow stream.
    ///
    /// This field is exposed to support direct timely dataflow manipulation when required, but it is
    /// not intended to be the idiomatic way to work with the collection.
    ///
    /// The timestamp in the data is required to always be at least the timestamp _of_ the data, in
    /// the timely-dataflow sense. If this invariant is not upheld, differential operators may behave
    /// unexpectedly.
    pub inner: Stream<'scope, T, C>,
}

impl<'scope, T: Timestamp, C> Collection<'scope, T, C> {
    /// Creates a new Collection from a timely dataflow stream.
    ///
    /// This method seems to be rarely used, with the `as_collection` method on streams being a more
    /// idiomatic approach to convert timely streams to collections. Also, the `input::Input` trait
    /// provides a `new_collection` method which will create a new collection for you without exposing
    /// the underlying timely stream at all.
    ///
    /// This stream should satisfy the timestamp invariant as documented on [Collection]; this
    /// method does not check it.
    pub fn new(stream: Stream<'scope, T, C>) -> Self { Self { inner: stream } }
}
impl<'scope, T: Timestamp, C: Container> Collection<'scope, T, C> {
    /// Creates a new collection accumulating the contents of the two collections.
    ///
    /// Despite the name, differential dataflow collections are unordered. This method is so named because the
    /// implementation is the concatenation of the stream of updates, but it corresponds to the addition of the
    /// two collections.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let data = scope.new_collection_from(1 .. 10).1;
    ///
    ///     let odds = data.clone().filter(|x| x % 2 == 1);
    ///     let evens = data.clone().filter(|x| x % 2 == 0);
    ///
    ///     odds.concat(evens)
    ///         .assert_eq(data);
    /// });
    /// ```
    pub fn concat(self, other: Self) -> Self {
        self.inner
            .concat(other.inner)
            .as_collection()
    }
    /// Creates a new collection accumulating the contents of the two collections.
    ///
    /// Despite the name, differential dataflow collections are unordered. This method is so named because the
    /// implementation is the concatenation of the stream of updates, but it corresponds to the addition of the
    /// two collections.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let data = scope.new_collection_from(1 .. 10).1;
    ///
    ///     let odds = data.clone().filter(|x| x % 2 == 1);
    ///     let evens = data.clone().filter(|x| x % 2 == 0);
    ///
    ///     odds.concatenate(Some(evens))
    ///         .assert_eq(data);
    /// });
    /// ```
    pub fn concatenate<I>(self, sources: I) -> Self
    where
        I: IntoIterator<Item=Self>
    {
        self.inner
            .scope()
            .concatenate(sources.into_iter().map(|x| x.inner).chain([self.inner]))
            .as_collection()
    }
    // Brings a Collection into a nested region.
    ///
    /// This method is a specialization of `enter` to the case where the nested scope is a region.
    /// It removes the need for an operator that adjusts the timestamp.
    pub fn enter_region<'inner>(self, child: Scope<'inner, T>) -> Collection<'inner, T, C> {
        self.inner
            .enter(child)
            .as_collection()
    }
    /// Applies a supplied function to each batch of updates.
    ///
    /// This method is analogous to `inspect`, but operates on batches and reveals the timestamp of the
    /// timely dataflow capability associated with the batch of updates. The observed batching depends
    /// on how the system executes, and may vary run to run.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///     scope.new_collection_from(1 .. 10).1
    ///          .map_in_place(|x| *x *= 2)
    ///          .filter(|x| x % 2 == 1)
    ///          .inspect_container(|event| println!("event: {:?}", event));
    /// });
    /// ```
    pub fn inspect_container<F>(self, func: F) -> Self
    where
        F: FnMut(Result<(&T, &C), &[T]>)+'static,
    {
        self.inner
            .inspect_container(func)
            .as_collection()
    }
    /// Attaches a timely dataflow probe to the output of a Collection.
    ///
    /// This probe is used to determine when the state of the Collection has stabilized and can
    /// be read out.
    pub fn probe(self) -> (probe::Handle<T>, Self) {
        let (handle, stream) = self.inner.probe();
        (handle, stream.as_collection())
    }
    /// Attaches a timely dataflow probe to the output of a Collection.
    ///
    /// This probe is used to determine when the state of the Collection has stabilized and all updates observed.
    /// In addition, a probe is also often use to limit the number of rounds of input in flight at any moment; a
    /// computation can wait until the probe has caught up to the input before introducing more rounds of data, to
    /// avoid swamping the system.
    pub fn probe_with(self, handle: &probe::Handle<T>) -> Self {
        Self::new(self.inner.probe_with(handle))
    }
    /// The scope containing the underlying timely dataflow stream.
    pub fn scope(&self) -> Scope<'scope, T> {
        self.inner.scope()
    }

    /// Creates a new collection whose counts are the negation of those in the input.
    ///
    /// This method is most commonly used with `concat` to get those element in one collection but not another.
    /// However, differential dataflow computations are still defined for all values of the difference type `R`,
    /// including negative counts.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let data = scope.new_collection_from(1 .. 10).1;
    ///
    ///     let odds = data.clone().filter(|x| x % 2 == 1);
    ///     let evens = data.clone().filter(|x| x % 2 == 0);
    ///
    ///     odds.negate()
    ///         .concat(data)
    ///         .assert_eq(evens);
    /// });
    /// ```
    pub fn negate(self) -> Self where C: containers::Negate {
        use timely::dataflow::channels::pact::Pipeline;
        self.inner
            .unary(Pipeline, "Negate", move |_,_| move |input, output| {
                input.for_each(|time, data| output.session(&time).give_container(&mut std::mem::take(data).negate()));
            })
            .as_collection()
    }

    /// Brings a Collection into a nested scope.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::Scope;
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let data = scope.new_collection_from(1 .. 10).1;
    ///
    ///     let result = scope.region(|child| {
    ///         data.clone()
    ///             .enter(child)
    ///             .leave(scope)
    ///     });
    ///
    ///     data.assert_eq(result);
    /// });
    /// ```
    pub fn enter<'inner, TInner>(self, child: Scope<'inner, TInner>) -> Collection<'inner, TInner, <C as containers::Enter<T, TInner>>::InnerContainer>
    where
        C: containers::Enter<T, TInner, InnerContainer: Container>,
        TInner: Refines<T>,
    {
        use timely::dataflow::channels::pact::Pipeline;
        self.inner
            .enter(child)
            .unary(Pipeline, "Enter", move |_,_| move |input, output| {
                input.for_each(|time, data| output.session(&time).give_container(&mut std::mem::take(data).enter()));
            })
            .as_collection()
    }

    /// Advances a timestamp in the stream according to the timestamp actions on the path.
    ///
    /// The path may advance the timestamp sufficiently that it is no longer valid, for example if
    /// incrementing fields would result in integer overflow. In this case, the record is dropped.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{ToStream, Concat, Inspect, vec::BranchWhen};
    ///
    /// use differential_dataflow::input::Input;
    ///
    /// timely::example(|scope| {
    ///     let summary1 = 5;
    ///
    ///     let data = scope.new_collection_from(1 .. 10).1;
    ///     /// Applies `results_in` on every timestamp in the collection.
    ///     data.results_in(summary1);
    /// });
    /// ```
    pub fn results_in(self, step: T::Summary) -> Self
    where
        C: containers::ResultsIn<T::Summary>,
    {
        use timely::dataflow::channels::pact::Pipeline;
        self.inner
            .unary(Pipeline, "ResultsIn", move |_,_| move |input, output| {
                input.for_each(|time, data| output.session(&time).give_container(&mut std::mem::take(data).results_in(&step)));
            })
            .as_collection()
    }
}

use timely::progress::timestamp::Refines;

/// Methods requiring a nested scope.
impl<'scope, T: Timestamp, C: Container> Collection<'scope, T, C>
{
    /// Returns the final value of a Collection from a nested scope to its containing scope.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::Scope;
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///    let data = scope.new_collection_from(1 .. 10).1;
    ///
    ///    let result = scope.region(|child| {
    ///         data.clone()
    ///             .enter(child)
    ///             .leave(scope)
    ///     });
    ///
    ///     data.assert_eq(result);
    /// });
    /// ```
    pub fn leave<'outer, TOuter>(self, outer: Scope<'outer, TOuter>) -> Collection<'outer, TOuter, <C as containers::Leave<T, TOuter>>::OuterContainer>
    where
        TOuter: Timestamp,
        T: Refines<TOuter>,
        C: containers::Leave<T, TOuter, OuterContainer: Container>,
    {
        use timely::dataflow::channels::pact::Pipeline;
        self.inner
            .leave(outer)
            .unary(Pipeline, "Leave", move |_,_| move |input, output| {
                input.for_each(|time, data| output.session(&time).give_container(&mut std::mem::take(data).leave()));
            })
            .as_collection()
    }

    /// Returns the value of a Collection from a nested region to its containing scope.
    ///
    /// This method is a specialization of `leave` to the case that of a nested region.
    /// It removes the need for an operator that adjusts the timestamp.
    pub fn leave_region<'outer>(self, outer: Scope<'outer, T>) -> Collection<'outer, T, C> {
        self.inner
            .leave(outer)
            .as_collection()
    }
}

pub use vec::Collection as VecCollection;
/// Specializations of `Collection` that use `Vec` as the container.
pub mod vec {

    use std::hash::Hash;

    use timely::progress::Timestamp;
    use timely::order::Product;
    use timely::dataflow::scope::Iterative;
    use timely::dataflow::operators::*;
    use timely::dataflow::operators::vec::*;

    use crate::collection::AsCollection;
    use crate::difference::{Semigroup, Abelian, Multiply};
    use crate::lattice::Lattice;
    use crate::hashable::Hashable;
    use crate::trace::{BatchCursor, BatchDiff, BatchKey, BatchVal, Navigable};
    use crate::trace::Cursor;

    /// An evolving collection of values of type `D`, backed by Rust `Vec` types as containers.
    ///
    /// The `Collection` type is the core abstraction in differential dataflow programs. As you write your
    /// differential dataflow computation, you write as if the collection is a static dataset to which you
    /// apply functional transformations, creating new collections. Once your computation is written, you
    /// are able to mutate the collection (by inserting and removing elements); differential dataflow will
    /// propagate changes through your functional computation and report the corresponding changes to the
    /// output collections.
    ///
    /// Each vec collection has three generic parameters. The parameter `T` is the timestamp type of the
    /// scope in which the collection exists; as you write more complicated programs you may wish to
    /// introduce nested scopes (e.g. for iteration), and this parameter tracks the scope's timestamp
    /// (for timely dataflow's benefit). The `D` parameter is the type of data in your collection, for
    /// example `String`, or `(u32, Vec<Option<()>>)`.
    /// The `R` parameter represents the types of changes that the data undergo, and is most commonly (and
    /// defaults to) `isize`, representing changes to the occurrence count of each record.
    ///
    /// This type definition instantiates the [`Collection`] type with a `Vec<(D, T, R)>`.
    pub type Collection<'scope, T, D, R = isize> = super::Collection<'scope, T, Vec<(D, T, R)>>;


    impl<'scope, T: Timestamp, D: Clone+'static, R: Clone+'static> Collection<'scope, T, D, R> {
        /// Creates a new collection by applying the supplied function to each input element.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map(|x| x * 2)
        ///          .filter(|x| x % 2 == 1)
        ///          .assert_empty();
        /// });
        /// ```
        pub fn map<D2, L>(self, mut logic: L) -> Collection<'scope, T, D2, R>
        where
            D2: Clone+'static,
            L: FnMut(D) -> D2 + 'static,
        {
            self.inner
                .map(move |(data, time, delta)| (logic(data), time, delta))
                .as_collection()
        }
        /// Creates a new collection by applying the supplied function to each input element.
        ///
        /// Although the name suggests in-place mutation, this function does not change the source collection,
        /// but rather re-uses the underlying allocations in its implementation. The method is semantically
        /// equivalent to `map`, but can be more efficient.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map_in_place(|x| *x *= 2)
        ///          .filter(|x| x % 2 == 1)
        ///          .assert_empty();
        /// });
        /// ```
        pub fn map_in_place<L>(self, mut logic: L) -> Collection<'scope, T, D, R>
        where
            L: FnMut(&mut D) + 'static,
        {
            self.inner
                .map_in_place(move |&mut (ref mut data, _, _)| logic(data))
                .as_collection()
        }
        /// Creates a new collection by applying the supplied function to each input element and accumulating the results.
        ///
        /// This method extracts an iterator from each input element, and extracts the full contents of the iterator. Be
        /// warned that if the iterators produce substantial amounts of data, they are currently fully drained before
        /// attempting to consolidate the results.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     scope.new_collection_from(1 .. 10).1
        ///          .flat_map(|x| 0 .. x);
        /// });
        /// ```
        pub fn flat_map<I, L>(self, mut logic: L) -> Collection<'scope, T, I::Item, R>
        where
            T: Clone,
            I: IntoIterator<Item: Clone+'static>,
            L: FnMut(D) -> I + 'static,
        {
            self.inner
                .flat_map(move |(data, time, delta)| logic(data).into_iter().map(move |x| (x, time.clone(), delta.clone())))
                .as_collection()
        }
        /// Creates a new collection containing those input records satisfying the supplied predicate.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map(|x| x * 2)
        ///          .filter(|x| x % 2 == 1)
        ///          .assert_empty();
        /// });
        /// ```
        pub fn filter<L>(self, mut logic: L) -> Collection<'scope, T, D, R>
        where
            L: FnMut(&D) -> bool + 'static,
        {
            self.inner
                .filter(move |(data, _, _)| logic(data))
                .as_collection()
        }
        /// Replaces each record with another, with a new difference type.
        ///
        /// This method is most commonly used to take records containing aggregatable data (e.g. numbers to be summed)
        /// and move the data into the difference component. This will allow differential dataflow to update in-place.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let nums = scope.new_collection_from(0 .. 10).1;
        ///     let x1 = nums.clone().flat_map(|x| 0 .. x);
        ///     let x2 = nums.map(|x| (x, 9 - x))
        ///                  .explode(|(x,y)| Some((x,y)));
        ///
        ///     x1.assert_eq(x2);
        /// });
        /// ```
        pub fn explode<D2, R2, I, L>(self, mut logic: L) -> Collection<'scope, T, D2, <R2 as Multiply<R>>::Output>
        where
            D2: Clone+'static,
            R2: Semigroup+Multiply<R, Output: Semigroup+'static>,
            I: IntoIterator<Item=(D2,R2)>,
            L: FnMut(D)->I+'static,
        {
            self.inner
                .flat_map(move |(x, t, d)| logic(x).into_iter().map(move |(x,d2)| (x, t.clone(), d2.multiply(&d))))
                .as_collection()
        }

        /// Joins each record against a collection defined by the function `logic`.
        ///
        /// This method performs what is essentially a join with the collection of records `(x, logic(x))`.
        /// Rather than materialize this second relation, `logic` is applied to each record and the appropriate
        /// modifications made to the results, namely joining timestamps and multiplying differences.
        ///
        /// #Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     // creates `x` copies of `2*x` from time `3*x` until `4*x`,
        ///     // for x from 0 through 9.
        ///     scope.new_collection_from(0 .. 10isize).1
        ///          .join_function(|x|
        ///              //   data      time      diff
        ///              vec![(2*x, (3*x) as u64,  x),
        ///                   (2*x, (4*x) as u64, -x)]
        ///           );
        /// });
        /// ```
        pub fn join_function<D2, R2, I, L>(self, mut logic: L) -> Collection<'scope, T, D2, <R2 as Multiply<R>>::Output>
        where
            T: Lattice,
            D2: Clone+'static,
            R2: Semigroup+Multiply<R, Output: Semigroup+'static>,
            I: IntoIterator<Item=(D2,T,R2)>,
            L: FnMut(D)->I+'static,
        {
            self.inner
                .flat_map(move |(x, t, d)| logic(x).into_iter().map(move |(x,t2,d2)| (x, t.join(&t2), d2.multiply(&d))))
                .as_collection()
        }

        /// Brings a Collection into a nested scope, at varying times.
        ///
        /// The `initial` function indicates the time at which each element of the Collection should appear.
        ///
        /// # Examples
        ///
        /// ```
        /// use timely::dataflow::Scope;
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let data = scope.new_collection_from(1 .. 10).1;
        ///
        ///     let result = scope.iterative::<u64,_,_>(|child| {
        ///         data.clone()
        ///             .enter_at(child, |x| *x)
        ///             .leave(scope)
        ///     });
        ///
        ///     data.assert_eq(result);
        /// });
        /// ```
        pub fn enter_at<'inner, TInner, F>(self, child: Iterative<'inner, T, TInner>, mut initial: F) -> Collection<'inner, Product<T, TInner>, D, R>
        where
            TInner: Timestamp+Hash,
            F: FnMut(&D) -> TInner + Clone + 'static,
        {
            self.inner
                .enter(child)
                .map(move |(data, time, diff)| {
                    let new_time = Product::new(time, initial(&data));
                    (data, new_time, diff)
                })
                .as_collection()
        }

        /// Delays each difference by a supplied function.
        ///
        /// It is assumed that `func` only advances timestamps; this is not verified, and things may go horribly
        /// wrong if that assumption is incorrect. It is also critical that `func` be monotonic: if two times are
        /// ordered, they should have the same order or compare equal once `func` is applied to them (this
        /// is because we advance the timely capability with the same logic, and it must remain `less_equal`
        /// to all of the data timestamps).
        pub fn delay<F>(self, func: F) -> Collection<'scope, T, D, R>
        where
            T: Hash,
            F: FnMut(&T) -> T + Clone + 'static,
        {
            let mut func1 = func.clone();
            let mut func2 = func.clone();

            self.inner
                .delay_batch(move |x| func1(x))
                .map_in_place(move |x| x.1 = func2(&x.1))
                .as_collection()
        }

        /// Applies a supplied function to each update.
        ///
        /// This method is most commonly used to report information back to the user, often for debugging purposes.
        /// Any function can be used here, but be warned that the incremental nature of differential dataflow does
        /// not guarantee that it will be called as many times as you might expect.
        ///
        /// The `(data, time, diff)` triples indicate a change `diff` to the frequency of `data` which takes effect
        /// at the logical time `time`. When times are totally ordered (for example, `usize`), these updates reflect
        /// the changes along the sequence of collections. For partially ordered times, the mathematics are more
        /// interesting and less intuitive, unfortunately.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map_in_place(|x| *x *= 2)
        ///          .filter(|x| x % 2 == 1)
        ///          .inspect(|x| println!("error: {:?}", x));
        /// });
        /// ```
        pub fn inspect<F>(self, func: F) -> Collection<'scope, T, D, R>
        where
            F: FnMut(&(D, T, R))+'static,
        {
            self.inner
                .inspect(func)
                .as_collection()
        }
        /// Applies a supplied function to each batch of updates.
        ///
        /// This method is analogous to `inspect`, but operates on batches and reveals the timestamp of the
        /// timely dataflow capability associated with the batch of updates. The observed batching depends
        /// on how the system executes, and may vary run to run.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map_in_place(|x| *x *= 2)
        ///          .filter(|x| x % 2 == 1)
        ///          .inspect_batch(|t,xs| println!("errors @ {:?}: {:?}", t, xs));
        /// });
        /// ```
        pub fn inspect_batch<F>(self, mut func: F) -> Collection<'scope, T, D, R>
        where
            F: FnMut(&T, &[(D, T, R)])+'static,
        {
            self.inner
                .inspect_batch(move |time, data| func(time, data))
                .as_collection()
        }

        /// Assert if the collection is ever non-empty.
        ///
        /// Because this is a dataflow fragment, the test is only applied as the computation is run. If the computation
        /// is not run, or not run to completion, there may be un-exercised times at which the collection could be
        /// non-empty. Typically, a timely dataflow computation runs to completion on drop, and so clean exit from a
        /// program should indicate that this assertion never found cause to complain.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map(|x| x * 2)
        ///          .filter(|x| x % 2 == 1)
        ///          .assert_empty();
        /// });
        /// ```
        pub fn assert_empty(self)
        where
            D: crate::ExchangeData+Hashable,
            R: crate::ExchangeData+Hashable + Semigroup,
            T: Lattice+Ord,
        {
            self.consolidate()
                .inspect(|x| panic!("Assertion failed: non-empty collection: {:?}", x));
        }
    }

    /// Methods requiring an Abelian difference, to support negation.
    impl<'scope, T: Timestamp + Clone + 'static, D: Clone+'static, R: Abelian+'static> Collection<'scope, T, D, R> {
        /// Assert if the collections are ever different.
        ///
        /// Because this is a dataflow fragment, the test is only applied as the computation is run. If the computation
        /// is not run, or not run to completion, there may be un-exercised times at which the collections could vary.
        /// Typically, a timely dataflow computation runs to completion on drop, and so clean exit from a program should
        /// indicate that this assertion never found cause to complain.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let data = scope.new_collection_from(1 .. 10).1;
        ///
        ///     let odds = data.clone().filter(|x| x % 2 == 1);
        ///     let evens = data.clone().filter(|x| x % 2 == 0);
        ///
        ///     odds.concat(evens)
        ///         .assert_eq(data);
        /// });
        /// ```
        pub fn assert_eq(self, other: Self)
        where
            D: crate::ExchangeData+Hashable,
            R: crate::ExchangeData+Hashable,
            T: Lattice+Ord,
        {
            self.negate()
                .concat(other)
                .assert_empty();
        }
    }

    use crate::trace::{Trace, Builder};
    use crate::operators::arrange::{Arranged, TraceAgent};

    impl <'scope, T, K, V, R> Collection<'scope, T, (K, V), R>
    where
        T: Timestamp + Lattice + Ord,
        K: crate::ExchangeData+Hashable,
        V: crate::ExchangeData,
        R: crate::ExchangeData+Semigroup,
    {
        /// Applies a reduction function on records grouped by key.
        ///
        /// Input data must be structured as `(key, val)` pairs.
        /// The user-supplied reduction function takes as arguments
        ///
        /// 1. a reference to the key,
        /// 2. a reference to the slice of values and their accumulated updates,
        /// 3. a mutuable reference to a vector to populate with output values and accumulated updates.
        ///
        /// The user logic is only invoked for non-empty input collections, and it is safe to assume that the
        /// slice of input values is non-empty. The values are presented in sorted order, as defined by their
        /// `Ord` implementations.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     // report the smallest value for each group
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map(|x| (x / 3, x))
        ///          .reduce(|_key, input, output| {
        ///              output.push((*input[0].0, 1))
        ///          });
        /// });
        /// ```
        pub fn reduce<L, V2: crate::Data, R2: Ord+Abelian+'static>(self, logic: L) -> Collection<'scope, T, (K, V2), R2>
        where L: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>)+'static {
            self.reduce_named("Reduce", logic)
        }

        /// As `reduce` with the ability to name the operator.
        pub fn reduce_named<L, V2: crate::Data, R2: Ord+Abelian+'static>(self, name: &str, logic: L) -> Collection<'scope, T, (K, V2), R2>
        where L: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>)+'static {
            use crate::trace::implementations::{ValBuilder, ValSpine};

            self.arrange_by_key_named(&format!("Arrange: {}", name))
                .reduce_abelian::<_,ValBuilder<_,_,_,_>,ValSpine<K,V2,_,_>,_,_>(
                    name,
                    logic,
                    |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
                )
                .as_collection(|k,v| (k.clone(), v.clone()))
        }

        /// Applies `reduce` to arranged data, and returns an arrangement of output data.
        ///
        /// This method is used by the more ergonomic `reduce`, `distinct`, and `count` methods, although
        /// it can be very useful if one needs to manually attach and re-use existing arranged collections.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        /// use differential_dataflow::trace::Trace;
        /// use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let trace =
        ///     scope.new_collection_from(1 .. 10u32).1
        ///          .map(|x| (x, x))
        ///          .reduce_abelian::<_,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>>(
        ///             "Example",
        ///              move |_key, src, dst| dst.push((*src[0].0, 1))
        ///          )
        ///          .trace;
        /// });
        /// ```
        pub fn reduce_abelian<L, Bu, T2>(self, name: &str, mut logic: L) -> Arranged<'scope, TraceAgent<T2>>
        where
            T2: Trace<Batch: Navigable, Time=T>+'static,
            for<'a> BatchCursor<T2>: Cursor<Key<'a>= &'a K, ValOwn = V, Time = T2::Time, Diff: Abelian>,
            Bu: Builder<Time=T2::Time, Input = Vec<((K, V), T2::Time, BatchDiff<T2>)>, Output = T2::Batch> + 'static,
            L: FnMut(&K, &[(&V, R)], &mut Vec<(V, BatchDiff<T2>)>)+'static,
        {
            self.reduce_core::<_,Bu,T2>(name, move |key, input, output, change| {
                if !input.is_empty() { logic(key, input, change); }
                change.extend(output.drain(..).map(|(x,mut d)| { d.negate(); (x, d) }));
                crate::consolidation::consolidate(change);
            })
        }

        /// Solves for output updates when presented with inputs and would-be outputs.
        ///
        /// Unlike `reduce_arranged`, this method may be called with an empty `input`,
        /// and it may not be safe to index into the first element.
        /// At least one of the two collections will be non-empty.
        pub fn reduce_core<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<'scope, TraceAgent<T2>>
        where
            V: Clone+'static,
            T2: Trace<Batch: Navigable, Time=T>+'static,
            for<'a> BatchCursor<T2>: Cursor<Key<'a>=&'a K, ValOwn = V, Time = T2::Time>,
            Bu: Builder<Time=T2::Time, Input = Vec<((K, V), T2::Time, BatchDiff<T2>)>, Output = T2::Batch> + 'static,
            L: FnMut(&K, &[(&V, R)], &mut Vec<(V,BatchDiff<T2>)>, &mut Vec<(V, BatchDiff<T2>)>)+'static,
        {
            self.arrange_by_key_named(&format!("Arrange: {}", name))
                .reduce_core::<_,Bu,_,_,_>(
                    name,
                    logic,
                    |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
                )
        }
    }

    impl<'scope, T, K, R1> Collection<'scope, T, K, R1>
    where
        T: Timestamp + Lattice + Ord,
        K: crate::ExchangeData+Hashable,
        R1: crate::ExchangeData+Semigroup
    {

        /// Reduces the collection to one occurrence of each distinct element.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     // report at most one of each key.
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map(|x| x / 3)
        ///          .distinct();
        /// });
        /// ```
        pub fn distinct(self) -> Collection<'scope, T, K, isize> {
            self.distinct_core()
        }

        /// Distinct for general integer differences.
        ///
        /// This method allows `distinct` to produce collections whose difference
        /// type is something other than an `isize` integer, for example perhaps an
        /// `i32`.
        pub fn distinct_core<R2: Ord+Abelian+'static+From<i8>>(self) -> Collection<'scope, T, K, R2> {
            self.threshold_named("Distinct", |_,_| R2::from(1i8))
        }

        /// Transforms the multiplicity of records.
        ///
        /// The `threshold` function is obliged to map `R1::zero` to `R2::zero`, or at
        /// least the computation may behave as if it does. Otherwise, the transformation
        /// can be nearly arbitrary: the code does not assume any properties of `threshold`.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     // report at most one of each key.
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map(|x| x / 3)
        ///          .threshold(|_,c| c % 2);
        /// });
        /// ```
        pub fn threshold<R2: Ord+Abelian+'static, F: FnMut(&K, &R1)->R2+'static>(self, thresh: F) -> Collection<'scope, T, K, R2> {
            self.threshold_named("Threshold", thresh)
        }

        /// A `threshold` with the ability to name the operator.
        pub fn threshold_named<R2: Ord+Abelian+'static, F: FnMut(&K,&R1)->R2+'static>(self, name: &str, mut thresh: F) -> Collection<'scope, T, K, R2> {
            use crate::trace::implementations::{KeyBuilder, KeySpine};

            self.arrange_by_self_named(&format!("Arrange: {}", name))
                .reduce_abelian::<_,KeyBuilder<K,T,R2>,KeySpine<K,T,R2>,_,_>(
                    name,
                    move |k,s,t| t.push(((), thresh(k, &s[0].1))),
                    |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
                )
                .as_collection(|k,_| k.clone())
        }

    }

    impl<'scope, T, K, R> Collection<'scope, T, K, R>
    where
        T: Timestamp + Lattice + Ord,
        K: crate::ExchangeData+Hashable,
        R: crate::ExchangeData+Semigroup
    {

        /// Counts the number of occurrences of each element.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///     // report the number of occurrences of each key
        ///     scope.new_collection_from(1 .. 10).1
        ///          .map(|x| x / 3)
        ///          .count();
        /// });
        /// ```
        pub fn count(self) -> Collection<'scope, T, (K, R), isize> { self.count_core() }

        /// Count for general integer differences.
        ///
        /// This method allows `count` to produce collections whose difference
        /// type is something other than an `isize` integer, for example perhaps an
        /// `i32`.
        pub fn count_core<R2: Ord + Abelian + From<i8> + 'static>(self) -> Collection<'scope, T, (K, R), R2> {
            use crate::trace::implementations::{ValBuilder, ValSpine};
            self.arrange_by_self_named("Arrange: Count")
                .reduce_abelian::<_,ValBuilder<K,R,T,R2>,ValSpine<K,R,T,R2>,_,_>(
                    "Count",
                    |_k,s,t| t.push((s[0].1.clone(), R2::from(1i8))),
                    |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v,t,r)| ((key.clone(), v),t,r))); },
                )
                .as_collection(|k,c| (k.clone(), c.clone()))
        }
    }

    /// Methods which require data be arrangeable.
    impl<'scope, T, D, R> Collection<'scope, T, D, R>
    where
        T: Timestamp + Clone + 'static + Lattice,
        D: crate::ExchangeData+Hashable,
        R: crate::ExchangeData+Semigroup,
    {
        /// Aggregates the weights of equal records into at most one record.
        ///
        /// This method uses the type `D`'s `hashed()` method to partition the data. The data are
        /// accumulated in place, each held back until their timestamp has completed.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let x = scope.new_collection_from(1 .. 10u32).1;
        ///
        ///     x.clone()
        ///      .negate()
        ///      .concat(x)
        ///      .consolidate() // <-- ensures cancellation occurs
        ///      .assert_empty();
        /// });
        /// ```
        pub fn consolidate(self) -> Self {
            use crate::trace::implementations::{KeyBatcher, KeyBuilder, KeySpine};
            self.consolidate_named::<KeyBatcher<_, _, _>,KeyBuilder<_,_,_>, KeySpine<_,_,_>,_>("Consolidate", |key,&()| key.clone())
        }

        /// As `consolidate` but with the ability to name the operator, specify the trace type,
        /// and provide the function `reify` to produce owned keys and values..
        pub fn consolidate_named<Ba, Bu, Tr, F>(self, name: &str, reify: F) -> Self
        where
            Ba: crate::trace::Batcher<Output=Vec<((D, ()), T, R)>, Time=T> + 'static,
            Tr: crate::trace::Trace<Batch: Navigable, Time=T>+'static,
            for<'a> BatchCursor<Tr>: Cursor<Time=Tr::Time, Diff=R>,
            Bu: crate::trace::Builder<Time=Tr::Time, Input=Vec<((D, ()), T, R)>, Output=Tr::Batch>,
            F: Fn(BatchKey<'_, Tr>, BatchVal<'_, Tr>) -> D + 'static,
        {
            use crate::operators::arrange::arrangement::Arrange;
            self.map(|k| (k, ()))
                .arrange_named::<Ba, Bu, Tr>(name)
                .as_collection(reify)
        }

        /// Aggregates the weights of equal records.
        ///
        /// Unlike `consolidate`, this method does not exchange data and does not
        /// ensure that at most one copy of each `(data, time)` pair exists in the
        /// results. Instead, it acts on each batch of data and collapses equivalent
        /// `(data, time)` pairs found therein, suppressing any that accumulate to
        /// zero.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let x = scope.new_collection_from(1 .. 10u32).1;
        ///
        ///     // nothing to assert, as no particular guarantees.
        ///     x.clone()
        ///      .negate()
        ///      .concat(x)
        ///      .consolidate_stream();
        /// });
        /// ```
        pub fn consolidate_stream(self) -> Self {

            use timely::dataflow::channels::pact::Pipeline;
            use timely::dataflow::operators::Operator;
            use crate::collection::AsCollection;
            use crate::consolidation::ConsolidatingContainerBuilder;

            self.inner
                .unary::<ConsolidatingContainerBuilder<_>, _, _, _>(Pipeline, "ConsolidateStream", |_cap, _info| {

                    move |input, output| {
                        input.for_each(|time, data| {
                            output.session_with_builder(&time).give_iterator(data.drain(..));
                        })
                    }
                })
                .as_collection()
        }
    }

    use crate::trace::implementations::{ValSpine, ValBatcher, ValBuilder};
    use crate::trace::implementations::{KeySpine, KeyBatcher, KeyBuilder};
    use crate::trace::implementations::ContainerChunker;
    use crate::operators::arrange::Arrange;

    impl<'scope, T, K, V, R> Arrange<'scope, T, Vec<((K, V), T, R)>> for Collection<'scope, T, (K, V), R>
    where
        T: Timestamp + Lattice,
        K: crate::ExchangeData + Hashable,
        V: crate::ExchangeData,
        R: crate::ExchangeData + Semigroup,
    {
        fn arrange_named<Ba, Bu, Tr>(self, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
        where
            Ba: crate::trace::Batcher<Output=Vec<((K, V), T, R)>, Time=T> + 'static,
            Bu: crate::trace::Builder<Time=T, Input=Vec<((K, V), T, R)>, Output = Tr::Batch>,
            Tr: crate::trace::Trace<Time=T> + 'static,
        {
            let exchange = timely::dataflow::channels::pact::Exchange::new(move |update: &((K,V),T,R)| (update.0).0.hashed().into());
            crate::operators::arrange::arrangement::arrange_core::<_, _, ContainerChunker<Vec<((K, V), T, R)>>, Ba, Bu, _>(self.inner, exchange, name)
        }
    }

    impl<'scope, T, K: crate::ExchangeData+Hashable, R: crate::ExchangeData+Semigroup> Arrange<'scope, T, Vec<((K, ()), T, R)>> for Collection<'scope, T, K, R>
    where
        T: Timestamp + Lattice + Ord,
    {
        fn arrange_named<Ba, Bu, Tr>(self, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
        where
            Ba: crate::trace::Batcher<Output=Vec<((K, ()), T, R)>, Time=T> + 'static,
            Bu: crate::trace::Builder<Time=T, Input=Vec<((K, ()), T, R)>, Output = Tr::Batch>,
            Tr: crate::trace::Trace<Time=T> + 'static,
        {
            let exchange = timely::dataflow::channels::pact::Exchange::new(move |update: &((K,()),T,R)| (update.0).0.hashed().into());
            crate::operators::arrange::arrangement::arrange_core::<_, _, ContainerChunker<Vec<((K, ()), T, R)>>, Ba, Bu, _>(self.map(|k| (k, ())).inner, exchange, name)
        }
    }


    impl<'scope, T, K: crate::ExchangeData+Hashable, V: crate::ExchangeData, R: crate::ExchangeData+Semigroup> Collection<'scope, T, (K,V), R>
    where
        T: Timestamp + Lattice + Ord,
    {
        /// Arranges a collection of `(Key, Val)` records by `Key`.
        ///
        /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
        /// This trace is current for all times completed by the output stream, which can be used to
        /// safely identify the stable times and values in the trace.
        pub fn arrange_by_key(self) -> Arranged<'scope, TraceAgent<ValSpine<K, V, T, R>>> {
            self.arrange_by_key_named("ArrangeByKey")
        }

        /// As `arrange_by_key` but with the ability to name the arrangement.
        pub fn arrange_by_key_named(self, name: &str) -> Arranged<'scope, TraceAgent<ValSpine<K, V, T, R>>> {
            self.arrange_named::<ValBatcher<_,_,_,_>,ValBuilder<_,_,_,_>,_>(name)
        }
    }

    impl<'scope, T, K: crate::ExchangeData+Hashable, R: crate::ExchangeData+Semigroup> Collection<'scope, T, K, R>
    where
        T: Timestamp + Lattice + Ord,
    {
        /// Arranges a collection of `Key` records by `Key`.
        ///
        /// This operator arranges a collection of records into a shared trace, whose contents it maintains.
        /// This trace is current for all times complete in the output stream, which can be used to safely
        /// identify the stable times and values in the trace.
        pub fn arrange_by_self(self) -> Arranged<'scope, TraceAgent<KeySpine<K, T, R>>> {
            self.arrange_by_self_named("ArrangeBySelf")
        }

        /// As `arrange_by_self` but with the ability to name the arrangement.
        pub fn arrange_by_self_named(self, name: &str) -> Arranged<'scope, TraceAgent<KeySpine<K, T, R>>> {
            self.map(|k| (k, ()))
                .arrange_named::<KeyBatcher<_,_,_>,KeyBuilder<_,_,_>,_>(name)
        }
    }

    impl<'scope, T, K, V, R> Collection<'scope, T, (K, V), R>
    where
        T: Timestamp + Lattice + Ord,
        K: crate::ExchangeData+Hashable,
        V: crate::ExchangeData,
        R: crate::ExchangeData+Semigroup,
    {
        /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and yields pairs `(key, (val1, val2))`.
        ///
        /// The [`join_map`](Join::join_map) method may be more convenient for non-trivial processing pipelines.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
        ///     let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
        ///     let z = scope.new_collection_from(vec![(0, (1, 'a')), (1, (3, 'b'))]).1;
        ///
        ///     x.join(y)
        ///      .assert_eq(z);
        /// });
        /// ```
        pub fn join<V2, R2>(self, other: Collection<'scope, T, (K,V2), R2>) -> Collection<'scope, T, (K,(V,V2)), <R as Multiply<R2>>::Output>
        where
            K:  crate::ExchangeData,
            V2: crate::ExchangeData,
            R2: crate::ExchangeData+Semigroup,
            R: Multiply<R2, Output: Semigroup+'static>,
        {
            self.join_map(other, |k,v,v2| (k.clone(),(v.clone(),v2.clone())))
        }

        /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and then applies a function.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
        ///     let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
        ///     let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
        ///
        ///     x.join_map(y, |_key, &a, &b| (a,b))
        ///      .assert_eq(z);
        /// });
        /// ```
        pub fn join_map<V2: crate::ExchangeData, R2: crate::ExchangeData+Semigroup, D: crate::Data, L>(self, other: Collection<'scope, T, (K, V2), R2>, mut logic: L) -> Collection<'scope, T, D, <R as Multiply<R2>>::Output>
        where R: Multiply<R2, Output: Semigroup+'static>, L: FnMut(&K, &V, &V2)->D+'static {
            let arranged1 = self.arrange_by_key();
            let arranged2 = other.arrange_by_key();
            arranged1.join_core(arranged2, move |k,v1,v2| Some(logic(k,v1,v2)))
        }

        /// Matches pairs `(key, val)` and `key` based on `key`, producing the former with frequencies multiplied.
        ///
        /// When the second collection contains frequencies that are either zero or one this is the more traditional
        /// relational semijoin. When the second collection may contain multiplicities, this operation may scale up
        /// the counts of the records in the first input.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
        ///     let y = scope.new_collection_from(vec![0, 2]).1;
        ///     let z = scope.new_collection_from(vec![(0, 1)]).1;
        ///
        ///     x.semijoin(y)
        ///      .assert_eq(z);
        /// });
        /// ```
        pub fn semijoin<R2: crate::ExchangeData+Semigroup>(self, other: Collection<'scope, T, K, R2>) -> Collection<'scope, T, (K, V), <R as Multiply<R2>>::Output>
        where R: Multiply<R2, Output: Semigroup+'static> {
            let arranged1 = self.arrange_by_key();
            let arranged2 = other.arrange_by_self();
            arranged1.join_core(arranged2, |k,v,_| Some((k.clone(), v.clone())))
        }

        /// Subtracts the semijoin with `other` from `self`.
        ///
        /// In the case that `other` has multiplicities zero or one this results
        /// in a relational antijoin, in which we discard input records whose key
        /// is present in `other`. If the multiplicities could be other than zero
        /// or one, the semantic interpretation of this operator is less clear.
        ///
        /// In almost all cases, you should ensure that `other` has multiplicities
        /// that are zero or one, perhaps by using the `distinct` operator.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
        ///     let y = scope.new_collection_from(vec![0, 2]).1;
        ///     let z = scope.new_collection_from(vec![(1, 3)]).1;
        ///
        ///     x.antijoin(y)
        ///      .assert_eq(z);
        /// });
        /// ```
        pub fn antijoin<R2: crate::ExchangeData+Semigroup>(self, other: Collection<'scope, T, K, R2>) -> Collection<'scope, T, (K, V), R>
        where R: Multiply<R2, Output=R>, R: Abelian+'static {
            self.clone().concat(self.semijoin(other).negate())
        }

        /// Joins two arranged collections with the same key type.
        ///
        /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function,
        /// which produces something implementing `IntoIterator`, where the output collection will have an entry for
        /// every value returned by the iterator.
        ///
        /// # Examples
        ///
        /// ```
        /// use differential_dataflow::input::Input;
        /// use differential_dataflow::trace::Trace;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let x = scope.new_collection_from(vec![(0u32, 1), (1, 3)]).1
        ///                  .arrange_by_key();
        ///     let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1
        ///                  .arrange_by_key();
        ///
        ///     let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
        ///
        ///     x.join_core(y, |_key, &a, &b| Some((a, b)))
        ///      .assert_eq(z);
        /// });
        /// ```
        pub fn join_core<Tr2,I,L,R2> (self, stream2: Arranged<'scope, Tr2>, result: L) -> Collection<'scope, T,I::Item,<R as Multiply<R2>>::Output>
        where
            Tr2: crate::trace::TraceReader<Batch: Navigable, Time=T>+Clone+'static,
            for<'a> BatchCursor<Tr2>: Cursor<Key<'a>=&'a K>,
            // Pin the cursor diff to a named param `R2`: a `Multiply` bound on a projection does not
            // connect to its use-site (the solver normalizes the use but not the bound's subject).
            BatchCursor<Tr2>: Cursor<Diff = R2, Time = T>,
            R: Multiply<R2, Output: Semigroup+'static>,
            I: IntoIterator<Item: crate::Data>,
            L: FnMut(&K,&V,BatchVal<'_, Tr2>)->I+'static,
        {
            self.arrange_by_key()
                .join_core(stream2, result)
        }
    }
}

/// Conversion to a differential dataflow Collection.
pub trait AsCollection<'scope, T: Timestamp, C> {
    /// Converts the type to a differential dataflow collection.
    fn as_collection(self) -> Collection<'scope, T, C>;
}

impl<'scope, T: Timestamp, C> AsCollection<'scope, T, C> for Stream<'scope, T, C> {
    /// Converts the type to a differential dataflow collection.
    ///
    /// By calling this method, you guarantee that the timestamp invariant (as documented on
    /// [Collection]) is upheld. This method will not check it.
    fn as_collection(self) -> Collection<'scope, T, C> {
        Collection::<T,C>::new(self)
    }
}

/// Concatenates multiple collections.
///
/// This method has the effect of a sequence of calls to `concat`, but it does
/// so in one operator rather than a chain of many operators.
///
/// # Examples
///
/// ```
/// use differential_dataflow::input::Input;
///
/// ::timely::example(|scope| {
///
///     let data = scope.new_collection_from(1 .. 10).1;
///
///     let odds = data.clone().filter(|x| x % 2 == 1);
///     let evens = data.clone().filter(|x| x % 2 == 0);
///
///     differential_dataflow::collection::concatenate(scope, vec![odds, evens])
///         .assert_eq(data);
/// });
/// ```
pub fn concatenate<'scope, T, C, I>(scope: Scope<'scope, T>, iterator: I) -> Collection<'scope, T, C>
where
    T: Timestamp,
    C: Container,
    I: IntoIterator<Item=Collection<'scope, T, C>>,
{
    scope
        .concatenate(iterator.into_iter().map(|x| x.inner))
        .as_collection()
}

/// Traits that can be implemented by containers to provide functionality to collections based on them.
pub mod containers {

    /// A container that can negate its updates.
    pub trait Negate {
        /// Negates Abelian differences of each update.
        fn negate(self) -> Self;
    }

    /// A container that can enter from timestamp `T1` into timestamp `T2`.
    pub trait Enter<T1, T2> {
        /// The resulting container type.
        type InnerContainer;
        /// Update timestamps from `T1` to `T2`.
        fn enter(self) -> Self::InnerContainer;
    }

    /// A container that can leave from timestamp `T1` into timestamp `T2`.
    pub trait Leave<T1, T2> {
        /// The resulting container type.
        type OuterContainer;
        /// Update timestamps from `T1` to `T2`.
        fn leave(self) -> Self::OuterContainer;
    }

    /// A container that can advance timestamps by a summary `TS`.
    pub trait ResultsIn<TS> {
        /// Advance times in the container by `step`.
        fn results_in(self, step: &TS) -> Self;
    }


    /// Implementations of container traits for the `Vec` container.
    mod vec {

        use timely::progress::{Timestamp, timestamp::Refines};
        use crate::collection::Abelian;

        use super::{Negate, Enter, Leave, ResultsIn};

        impl<D, T, R: Abelian> Negate for Vec<(D, T, R)> {
            fn negate(mut self) -> Self {
                for (_data, _time, diff) in self.iter_mut() { diff.negate(); }
                self
            }
        }

        impl<D, T1: Timestamp, T2: Refines<T1>, R> Enter<T1, T2> for Vec<(D, T1, R)> {
            type InnerContainer = Vec<(D, T2, R)>;
            fn enter(self) -> Self::InnerContainer {
                self.into_iter().map(|(d,t1,r)| (d,T2::to_inner(t1),r)).collect()
            }
        }

        impl<D, T1: Refines<T2>, T2: Timestamp, R> Leave<T1, T2> for Vec<(D, T1, R)> {
            type OuterContainer = Vec<(D, T2, R)>;
            fn leave(self) -> Self::OuterContainer {
                self.into_iter().map(|(d,t1,r)| (d,t1.to_outer(),r)).collect()
            }
        }

        impl<D, T: Timestamp, R> ResultsIn<T::Summary> for Vec<(D, T, R)> {
            fn results_in(self, step: &T::Summary) -> Self {
                use timely::progress::PathSummary;
                self.into_iter().filter_map(move |(d,t,r)| step.results_in(&t).map(|t| (d,t,r))).collect()
            }
        }
    }

    /// Implementations of container traits for the `Rc` container.
    mod rc {
        use std::rc::Rc;

        use timely::progress::{Timestamp, timestamp::Refines};

        use super::{Negate, Enter, Leave, ResultsIn};

        impl<C: Negate+Clone+Default> Negate for Rc<C> {
            fn negate(mut self) -> Self {
                std::mem::take(Rc::make_mut(&mut self)).negate().into()
            }
        }

        impl<C: Enter<T1, T2>+Clone+Default, T1: Timestamp, T2: Refines<T1>> Enter<T1, T2> for Rc<C> {
            type InnerContainer = Rc<C::InnerContainer>;
            fn enter(mut self) -> Self::InnerContainer {
                std::mem::take(Rc::make_mut(&mut self)).enter().into()
            }
        }

        impl<C: Leave<T1, T2>+Clone+Default, T1: Refines<T2>, T2: Timestamp> Leave<T1, T2> for Rc<C> {
            type OuterContainer = Rc<C::OuterContainer>;
            fn leave(mut self) -> Self::OuterContainer {
                std::mem::take(Rc::make_mut(&mut self)).leave().into()
            }
        }

        impl<C: ResultsIn<TS>+Clone+Default, TS> ResultsIn<TS> for Rc<C> {
            fn results_in(mut self, step: &TS) -> Self {
                std::mem::take(Rc::make_mut(&mut self)).results_in(step).into()
            }
        }
    }
}
