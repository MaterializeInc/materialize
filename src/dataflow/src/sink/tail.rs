// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::rc::Rc;

use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::BatchReader;
use itertools::Itertools;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::progress::frontier::AntichainRef;
use timely::progress::timestamp::Timestamp as TimelyTimestamp;
use timely::progress::Antichain;
use timely::PartialOrder;

use dataflow_types::{SinkAsOf, TailSinkConnector};
use expr::GlobalId;
use repr::adt::apd::Apd;
use repr::{Datum, Diff, Row, Timestamp};

pub fn tail<G>(
    stream: Stream<G, Rc<OrdValBatch<GlobalId, Row, Timestamp, Diff>>>,
    id: GlobalId,
    connector: TailSinkConnector,
    as_of: SinkAsOf,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let mut errored = false;
    let mut packer = Row::default();
    let mut received_data = false;

    // Initialize to the minimal input frontier.
    let mut input_frontier = Antichain::from_elem(<G::Timestamp as TimelyTimestamp>::minimum());

    stream.sink(Pipeline, &format!("tail-{}", id), move |input| {
        input.for_each(|_, batches| {
            if errored {
                // TODO(benesch): we should actually drop the sink if the
                // receiver has gone away.
                return;
            }
            received_data = true;
            let mut results = vec![];
            for batch in batches.iter() {
                let mut cursor = batch.cursor();
                while cursor.key_valid(&batch) {
                    while cursor.val_valid(&batch) {
                        let row = cursor.val(&batch);
                        cursor.map_times(&batch, |time, diff| {
                            assert!(*diff >= 0, "negative multiplicities sinked in tail");
                            let diff = *diff as usize;
                            let should_emit = if as_of.strict {
                                as_of.frontier.less_than(time)
                            } else {
                                as_of.frontier.less_equal(time)
                            };
                            if should_emit {
                                for _ in 0..diff {
                                    // Add the unpacked timestamp so we can sort by them later.
                                    results.push((*time, row.clone()));
                                }
                            }
                        });
                        cursor.step_val(&batch);
                    }
                    cursor.step_key(&batch);
                }
            }

            // Sort results by time and convert to Vec<Row>. We use stable sort here even
            // though it is slower because it will produce deterministic results since the
            // cursor will always produce rows in the same order.
            results.sort_by_key(|(time, _)| *time);
            let mut results: Vec<Row> = results.into_iter().map(|(_, row)| row).collect();

            if let Some(batch) = batches.last() {
                let progress_row = update_progress(
                    &mut input_frontier,
                    batch.desc.upper().borrow(),
                    &mut packer,
                    connector.object_columns + 1,
                );
                if connector.emit_progress {
                    if let Some(progress_row) = progress_row {
                        results.push(progress_row);
                    }
                }
            }

            // TODO(benesch): the lack of backpressure here can result in
            // unbounded memory usage.
            if connector.tx.send(results).is_err() {
                errored = true;
                return;
            }
        });

        let progress_row = update_progress(
            &mut input_frontier,
            input.frontier().frontier(),
            &mut packer,
            connector.object_columns + 1,
        );

        // Only emit updates if this operator/worker received actual
        // data for emission. For TAIL, data is exchanged to one worker,
        // which forwards all the data to the client process. If we
        // blindly forwarded the frontier from all workers we would get
        // multiple progress updates in the client.
        if connector.emit_progress && received_data {
            if let Some(progress_row) = progress_row {
                let results = vec![progress_row];

                // TODO(benesch): the lack of backpressure here can result in
                // unbounded memory usage.
                if connector.tx.send(results).is_err() {
                    errored = true;
                    return;
                }
            }
        }
    })
}

// Checks if there is progress between `current_input_frontier` and
// `new_input_frontier`. If yes, updates the `current_input_frontier` to
// `new_input_frontier` and returns a [`Row`] that encodes this progress. If
// there is no progress, returns [`None`].
fn update_progress(
    current_input_frontier: &mut Antichain<Timestamp>,
    new_input_frontier: AntichainRef<Timestamp>,
    packer: &mut Row,
    empty_columns: usize,
) -> Option<Row> {
    // Test to see if strict progress has occurred. This is true if the new
    // frontier is not less or equal to the old frontier.
    let progress = !PartialOrder::less_equal(&new_input_frontier, &current_input_frontier.borrow());

    if progress {
        current_input_frontier.clear();
        current_input_frontier.extend(new_input_frontier.iter().cloned());

        // This only looks at the first entry of the antichain.
        // If we ever have multi-dimensional time, this is not correct
        // anymore. There might not even be progress in the first dimension.
        // We panic, so that future developers introducing multi-dimensional
        // time in Materialize will notice.
        let upper = new_input_frontier
            .iter()
            .at_most_one()
            .expect("more than one element in the frontier")
            .cloned();

        // the input frontier might be empty
        upper.map(|upper| {
            packer.push(Datum::from(Apd::from(upper)));
            packer.push(Datum::True);
            // Fill in the diff column and all table columns with NULL.
            for _ in 0..empty_columns {
                packer.push(Datum::Null);
            }
            packer.finish_and_reuse()
        })
    } else {
        None
    }
}
