//! Types and operators for dynamically scoped iterative dataflows.
//!
//! Scopes in timely dataflow are expressed statically, as part of the type system.
//! This affords many efficiencies, as well as type-driven reassurance of correctness.
//! However, there are times you need scopes whose organization is discovered only at runtime.
//! Naiad and Materialize are examples: the latter taking arbitrary SQL into iterative dataflows.
//!
//! This module provides a timestamp type `Pointstamp` that can represent an update with an
//! unboundedly long sequence of some `T: Timestamp`, ordered by the product order by which times
//! in iterative dataflows are ordered. The module also provides methods for manipulating these
//! timestamps to emulate the movement of update streams in to, within, and out of iterative scopes.
//!

pub mod pointstamp;

use timely::order::Product;
use timely::progress::Timestamp;
use timely::dataflow::operators::generic::{OutputBuilder, builder_rc::OperatorBuilder};
use timely::dataflow::channels::pact::Pipeline;
use timely::progress::Antichain;

use crate::difference::Semigroup;
use crate::{VecCollection, Data};
use crate::collection::AsCollection;
use crate::dynamic::pointstamp::PointStamp;
use crate::dynamic::pointstamp::PointStampSummary;

impl<'scope, D, R, T, TOuter> VecCollection<'scope, Product<TOuter, PointStamp<T>>, D, R>
where
    D: Data,
    R: Semigroup+'static,
    T: Timestamp+Default,
    TOuter: Timestamp,
{
    /// Enters a dynamically created scope which has `level` timestamp coordinates.
    pub fn enter_dynamic(self, _level: usize) -> Self {
        self
    }
    /// Leaves a dynamically created scope which has `level` timestamp coordinates.
    pub fn leave_dynamic(self, level: usize) -> Self {
        // Create a unary operator that will strip all but `level-1` timestamp coordinates.
        let mut builder = OperatorBuilder::new("LeaveDynamic".to_string(), self.scope());
        let (output, stream) = builder.new_output();
        let mut output = OutputBuilder::from(output);
        let mut input = builder.new_input_connection(self.inner, Pipeline, [(0, Antichain::from_elem(Product { outer: Default::default(), inner: PointStampSummary { retain: Some(level - 1), actions: Vec::new() } }))]);

        builder.build(move |_capability| move |_frontier| {
            let mut output = output.activate();
            input.for_each(|cap, data| {
                let mut new_time = cap.time().clone();
                let mut vec = std::mem::take(&mut new_time.inner).into_inner();
                vec.truncate(level - 1);
                new_time.inner = PointStamp::new(vec);
                let new_cap = cap.delayed(&new_time, 0);
                for (_data, time, _diff) in data.iter_mut() {
                    let mut vec = std::mem::take(&mut time.inner).into_inner();
                    vec.truncate(level - 1);
                    time.inner = PointStamp::new(vec);
                }
                output.session(&new_cap).give_container(data);
            });
        });

        stream.as_collection()
    }
}

/// Produces the summary for a feedback operator at `level`, applying `summary` to that coordinate.
pub fn feedback_summary<T>(level: usize, summary: T::Summary) -> PointStampSummary<T::Summary>
where
    T: Timestamp+Default,
{
    PointStampSummary {
        retain: None,
        actions: std::iter::repeat(Default::default()).take(level-1).chain(std::iter::once(summary)).collect(),
    }
}
