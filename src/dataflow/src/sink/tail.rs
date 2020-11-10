// Copyright Materialize, Inc. All rights reserved.
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
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use futures::executor::block_on;
use futures::sink::SinkExt;

use dataflow_types::TailSinkConnector;
use expr::GlobalId;
use ore::cast::CastFrom;
use repr::adt::decimal::Significand;
use repr::{Datum, Diff, Row, RowPacker, Timestamp};

pub fn tail<G>(
    stream: Stream<G, Rc<OrdValBatch<GlobalId, Row, Timestamp, Diff>>>,
    id: GlobalId,
    connector: TailSinkConnector,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let mut tx = block_on(connector.tx.connect()).expect("tail transmitter failed");
    let mut packer = RowPacker::new();
    let mut cols = 0;
    stream.sink(Pipeline, &format!("tail-{}", id), move |input| {
        input.for_each(|_, batches| {
            let mut results = vec![];
            for batch in batches.iter() {
                let mut cursor = batch.cursor();
                while cursor.key_valid(&batch) {
                    while cursor.val_valid(&batch) {
                        let row = cursor.val(&batch);
                        cursor.map_times(&batch, |time, diff| {
                            let should_emit = if connector.strict {
                                connector.frontier.less_than(time)
                            } else {
                                connector.frontier.less_equal(time)
                            };
                            if should_emit {
                                packer.push(Datum::Decimal(Significand::new(i128::from(*time))));
                                if connector.emit_timestamps {
                                    packer.push(Datum::False);
                                }
                                packer.push(Datum::Int64(i64::cast_from(*diff)));
                                packer.extend_by_row(row);
                                // Add the unpacked timestamp so we can sort by them later.
                                results.push((*time, packer.finish_and_reuse()));
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

            // Due to timely dataflow constraints, differential dataflow cannot ship empty
            // batches, so we are guaranteed that if there's atleast one batch results will
            // be non-empty.
            if connector.emit_timestamps && !batches.is_empty() {
                // The user has request timestamp done messages. We've already sorted the
                // results by timestamp so we can duplicate the last row's timestamp which is
                // somewhat easier than using a batch.upper() antichain. This timestamp won't
                // be reused any other batch because they are disjoint and strictly increasing.
                let (time, _) = results.last().unwrap().clone();
                packer.push(Datum::Decimal(Significand::new(i128::from(time))));
                packer.push(Datum::True);
                // Fill in all table columns with NULL. Determine the number of columns
                // by iterating through a row and skipping the first two that we add by
                // hand. Remember this value and only iterate once because the iteration
                // involves decoding the row datum bytes which is slow.
                if cols == 0 {
                    let (_, row) = results.last().unwrap();
                    // The skip(2) skips the timestamp and done columns, leaving diff + all columns
                    // in thing being tailed.
                    cols = row.iter().skip(2).count();
                }
                for _ in 0..cols {
                    packer.push(Datum::Null);
                }
                results.push((time, packer.finish_and_reuse()));
            }

            let results: Vec<Row> = results.into_iter().map(|(_, row)| row).collect();

            // TODO(benesch): this blocks the Timely thread until the send
            // completes. Hopefully it's just a quick write to a kernel buffer,
            // but perhaps not if the batch gets too large? We may need to do
            // something smarter, like offloading to a networking thread.
            block_on(tx.send(results)).expect("tail send failed");
        });
    })
}
