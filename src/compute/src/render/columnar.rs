// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conversion utilities between Vec-based and columnar collections.

use columnar::{Columnar, Index};
use differential_dataflow::{AsCollection, VecCollection};
use mz_repr::{Diff, Row};
use mz_timely_util::columnar::builder::ColumnBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;

use crate::typedefs::{ColumnarCollection, MzTimestamp};

/// Convert a `VecCollection<S, Row, Diff>` to a `ColumnarCollection<S, Row, Diff>`.
///
/// This operator batches rows into columnar containers using `ColumnBuilder`.
/// The `ColumnBuilder` automatically determines batch sizes based on memory alignment
/// (approximately 2MB per container).
pub fn vec_to_columnar<S>(
    vec_collection: VecCollection<S, Row, Diff>,
) -> ColumnarCollection<S, Row, Diff>
where
    S: Scope,
    S::Timestamp: MzTimestamp,
{
    vec_collection
        .inner
        .unary::<ColumnBuilder<(Row, S::Timestamp, Diff)>, _, _, _>(
            Pipeline,
            "VecToColumnar",
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for (row, time, diff) in data.iter() {
                            session.give((row, time, diff));
                        }
                    });
                }
            },
        )
        .as_collection()
}

/// Convert a `ColumnarCollection<S, Row, Diff>` to a `VecCollection<S, Row, Diff>`.
///
/// This operator iterates columnar containers and emits individual `(Row, T, Diff)` tuples
/// into Vec-based containers.
pub fn columnar_to_vec<S>(
    columnar_collection: ColumnarCollection<S, Row, Diff>,
) -> VecCollection<S, Row, Diff>
where
    S: Scope,
    S::Timestamp: MzTimestamp,
{
    columnar_collection
        .inner
        .unary::<CapacityContainerBuilder<Vec<(Row, S::Timestamp, Diff)>>, _, _, _>(
            Pipeline,
            "ColumnarToVec",
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for (d, t, r) in data.borrow().into_index_iter() {
                            session.give((
                                Columnar::into_owned(d),
                                Columnar::into_owned(t),
                                Columnar::into_owned(r),
                            ));
                        }
                    });
                }
            },
        )
        .as_collection()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::input::Input;
    use mz_repr::{Datum, Diff, Row};
    use timely::dataflow::operators::probe::Probe;
    use timely::dataflow::operators::Inspect;

    /// Round-trip data through vec_to_columnar and then columnar_to_vec,
    /// verifying that all rows survive the conversion unchanged.
    #[mz_ore::test]
    fn round_trip_vec_columnar_vec() {
        timely::execute_directly(|worker| {
            let results: Rc<RefCell<Vec<(Row, u64, Diff)>>> =
                Rc::new(RefCell::new(Vec::new()));
            let results_capture = results.clone();

            let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, collection) = scope.new_collection::<Row, Diff>();

                // Convert Vec -> Columnar -> Vec
                let columnar = vec_to_columnar(collection);
                let round_tripped = columnar_to_vec(columnar);

                let (probe, _stream) = round_tripped
                    .inner
                    .inspect(move |item: &(Row, u64, Diff)| {
                        results_capture
                            .borrow_mut()
                            .push((item.0.clone(), item.1, item.2));
                    })
                    .probe();

                (input, probe)
            });

            let row1 = Row::pack_slice(&[Datum::Int32(42), Datum::String("hello")]);
            let row2 = Row::pack_slice(&[Datum::Int64(100)]);
            let row3 = Row::pack_slice(&[Datum::True, Datum::False, Datum::Null]);
            let empty_row = Row::default();

            let one = Diff::from(1);
            input.update(row1.clone(), one);
            input.update(row2.clone(), one);
            input.update(row3.clone(), one);
            input.update(empty_row.clone(), one);
            input.advance_to(1);
            input.flush();

            worker.step_while(|| probe.less_than(&1));

            let mut actual = results.borrow().clone();
            actual.sort_by(|a, b| a.0.cmp(&b.0));

            let mut expected = vec![
                (row1, 0u64, one),
                (row2, 0u64, one),
                (row3, 0u64, one),
                (empty_row, 0u64, one),
            ];
            expected.sort_by(|a, b| a.0.cmp(&b.0));

            assert_eq!(actual.len(), expected.len(), "Row count mismatch");
            for (a, e) in actual.iter().zip(expected.iter()) {
                assert_eq!(a.0, e.0, "Row data mismatch");
                assert_eq!(a.1, e.1, "Timestamp mismatch");
                assert_eq!(a.2, e.2, "Diff mismatch");
            }
        });
    }

    /// Verify that vec_to_columnar and then back works with multiple timestamps.
    #[mz_ore::test]
    fn round_trip_multiple_timestamps() {
        timely::execute_directly(|worker| {
            let results: Rc<RefCell<Vec<(Row, u64, Diff)>>> =
                Rc::new(RefCell::new(Vec::new()));
            let results_capture = results.clone();

            let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, collection) = scope.new_collection::<Row, Diff>();

                let columnar = vec_to_columnar(collection);
                let round_tripped = columnar_to_vec(columnar);

                let (probe, _stream) = round_tripped
                    .inner
                    .inspect(move |item: &(Row, u64, Diff)| {
                        results_capture
                            .borrow_mut()
                            .push((item.0.clone(), item.1, item.2));
                    })
                    .probe();

                (input, probe)
            });

            let row1 = Row::pack_slice(&[Datum::Int32(1)]);
            let row2 = Row::pack_slice(&[Datum::Int32(2)]);

            let one = Diff::from(1);
            input.update(row1.clone(), one);
            input.advance_to(1);
            input.update(row2.clone(), one);
            input.advance_to(2);
            input.flush();

            worker.step_while(|| probe.less_than(&2));

            let actual = results.borrow().clone();
            assert_eq!(actual.len(), 2);
            assert!(actual.iter().any(|(r, t, _)| *r == row1 && *t == 0));
            assert!(actual.iter().any(|(r, t, _)| *r == row2 && *t == 1));
        });
    }
}
