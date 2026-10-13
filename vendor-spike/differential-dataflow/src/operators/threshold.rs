//! Reduce the collection to one occurrence of each distinct element.
//!
//! The `distinct_total` and `distinct_total_u` operators are optimizations of the more general
//! `distinct` and `distinct_u` operators for the case in which time is totally ordered.

use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Pipeline;

use crate::lattice::Lattice;
use crate::{ExchangeData, VecCollection};
use crate::difference::{Semigroup, Abelian};
use crate::hashable::Hashable;
use crate::collection::AsCollection;
use crate::operators::arrange::Arranged;
use crate::trace::{BatchCursor, BatchDiff, BatchDiffGat, BatchKey, BatchReader, Cursor, Navigable, TraceReader};

/// Extension trait for the `distinct` differential dataflow method.
pub trait ThresholdTotal<'scope, T: Timestamp + TotalOrder + Lattice, K: ExchangeData, R: ExchangeData+Semigroup> : Sized {
    /// Reduces the collection to one occurrence of each distinct element.
    fn threshold_semigroup<R2, F>(self, thresh: F) -> VecCollection<'scope, T, K, R2>
    where
        R2: Semigroup+'static,
        F: FnMut(&K,&R,Option<&R>)->Option<R2>+'static,
        ;
    /// Reduces the collection to one occurrence of each distinct element.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::ThresholdTotal;
    ///
    /// ::timely::example(|scope| {
    ///     // report the number of occurrences of each key
    ///     scope.new_collection_from(1 .. 10).1
    ///          .map(|x| x / 3)
    ///          .threshold_total(|_,c| c % 2);
    /// });
    /// ```
    fn threshold_total<R2: Abelian+'static, F: FnMut(&K,&R)->R2+'static>(self, mut thresh: F) -> VecCollection<'scope, T, K, R2> {
        self.threshold_semigroup(move |key, new, old| {
            let mut new = thresh(key, new);
            if let Some(old) = old {
                let mut add = thresh(key, old);
                add.negate();
                new.plus_equals(&add);
            }
            if !new.is_zero() { Some(new) } else { None }
        })
    }
    /// Reduces the collection to one occurrence of each distinct element.
    ///
    /// This reduction only tests whether the weight associated with a record is non-zero, and otherwise
    /// ignores its specific value. To take more general actions based on the accumulated weight, consider
    /// the `threshold` method.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::ThresholdTotal;
    ///
    /// ::timely::example(|scope| {
    ///     // report the number of occurrences of each key
    ///     scope.new_collection_from(1 .. 10).1
    ///          .map(|x| x / 3)
    ///          .distinct_total();
    /// });
    /// ```
    fn distinct_total(self) -> VecCollection<'scope, T, K, isize> {
        self.distinct_total_core()
    }

    /// Distinct for general integer differences.
    ///
    /// This method allows `distinct` to produce collections whose difference
    /// type is something other than an `isize` integer, for example perhaps an
    /// `i32`.
    fn distinct_total_core<R2: Abelian+From<i8>+'static>(self) -> VecCollection<'scope, T, K, R2> {
        self.threshold_total(|_,_| R2::from(1i8))
    }

}

impl<'scope, T, K: ExchangeData+Hashable, R: ExchangeData+Semigroup> ThresholdTotal<'scope, T, K, R> for VecCollection<'scope, T, K, R>
where
    T: Timestamp + TotalOrder + Lattice,
{
    fn threshold_semigroup<R2, F>(self, thresh: F) -> VecCollection<'scope, T, K, R2>
    where
        R2: Semigroup+'static,
        F: FnMut(&K,&R,Option<&R>)->Option<R2>+'static,
    {
        self.arrange_by_self_named("Arrange: ThresholdTotal")
            .threshold_semigroup(thresh)
    }
}

impl<'scope, K, Tr> ThresholdTotal<'scope, Tr::Time, K, BatchDiff<Tr>> for Arranged<'scope, Tr>
where
    Tr: TraceReader<Batch: Navigable, Time: TotalOrder> + Clone + 'static,
    for<'a> BatchCursor<Tr>: Cursor<
        Key<'a>=&'a K,
        Val<'a>=&'a (),
        Time = Tr::Time,
        Diff : ExchangeData + Semigroup<BatchDiffGat<'a, Tr>>,
    >,
    K: ExchangeData,
{
    fn threshold_semigroup<R2, F>(self, mut thresh: F) -> VecCollection<'scope, Tr::Time, K, R2>
    where
        R2: Semigroup+'static,
        F: for<'a> FnMut(BatchKey<'a, Tr>,&BatchDiff<Tr>,Option<&BatchDiff<Tr>>)->Option<R2>+'static,
    {

        let mut trace = self.trace.clone();

        self.stream.unary_frontier(Pipeline, "ThresholdTotal", move |_,_| {

            // tracks the lower and upper limit of received batches.
            let mut lower_limit = timely::progress::frontier::Antichain::from_elem(Tr::Time::minimum());
            let mut upper_limit = timely::progress::frontier::Antichain::from_elem(Tr::Time::minimum());

            move |(input, _frontier), output| {

                let mut batch_storage = Vec::new();

                // Downgrde previous upper limit to be current lower limit.
                lower_limit.clear();
                lower_limit.extend(upper_limit.borrow().iter().cloned());

                let mut cap = None;
                input.for_each(|capability, batches| {
                    if cap.is_none() {                          // NB: Assumes batches are in-order
                        cap = Some(capability.retain(0));
                    }
                    for batch in batches.drain(..) {
                        upper_limit.clone_from(batch.upper());  // NB: Assumes batches are in-order
                        batch_storage.push(batch);
                    }
                });

                if let Some(capability) = cap {

                    let mut session = output.session(&capability);

                    let (mut batch_cursor, batch_storage) = crate::trace::cursor::cursor_list(batch_storage);
                    let (mut trace_cursor, trace_storage) = trace.cursor_through(lower_limit.borrow()).unwrap();

                    while let Some(key) = batch_cursor.get_key(&batch_storage) {
                        let mut count: Option<BatchDiff<Tr>> = None;

                        // Compute the multiplicity of this key before the current batch.
                        trace_cursor.seek_key(&trace_storage, key);
                        if trace_cursor.get_key(&trace_storage) == Some(key) {
                            trace_cursor.map_times(&trace_storage, |_, diff| {
                                count.as_mut().map(|c| c.plus_equals(&diff));
                                if count.is_none() { count = Some(<BatchCursor<Tr> as Cursor>::owned_diff(diff)); }
                            });
                        }

                        // Apply `thresh` both before and after `diff` is applied to `count`.
                        // If the result is non-zero, send it along.
                        batch_cursor.map_times(&batch_storage, |time, diff| {

                            let difference =
                            match &count {
                                Some(old) => {
                                    let mut temp = old.clone();
                                    temp.plus_equals(&diff);
                                    thresh(key, &temp, Some(old))
                                },
                                None => { thresh(key, &<BatchCursor<Tr> as Cursor>::owned_diff(diff), None) },
                            };

                            // Either add or assign `diff` to `count`.
                            if let Some(count) = &mut count {
                                count.plus_equals(&diff);
                            }
                            else {
                                count = Some(<BatchCursor<Tr> as Cursor>::owned_diff(diff));
                            }

                            if let Some(difference) = difference {
                                if !difference.is_zero() {
                                    session.give((key.clone(), <BatchCursor<Tr> as Cursor>::owned_time(time), difference));
                                }
                            }
                        });

                        batch_cursor.step_key(&batch_storage);
                    }
                }

                // tidy up the shared input trace.
                trace.advance_upper(&mut upper_limit);
                trace.set_logical_compaction(upper_limit.borrow());
                trace.set_physical_compaction(upper_limit.borrow());
            }
        })
        .as_collection()
    }
}
