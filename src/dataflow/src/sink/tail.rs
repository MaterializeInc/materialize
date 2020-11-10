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
                                if connector.emit_progress {
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
            let mut results: Vec<Row> = results.into_iter().map(|(_, row)| row).collect();

            if connector.emit_progress {
                if let Some(upper) = batch_upper(batches.last()) {
                    // The user has requested progress messages and there's at least one
                    // batch. All of the batches might have zero rows, so we do not depend on
                    // results at all. Another benefit of using upper (instead of the largest row
                    // time) is that the batch's upper may be larger than the row time.
                    packer.push(Datum::Decimal(Significand::new(i128::from(upper))));
                    packer.push(Datum::True);
                    // Fill in the diff column and all table columns with NULL.
                    for _ in 0..(connector.object_columns + 1) {
                        packer.push(Datum::Null);
                    }
                    results.push(packer.finish_and_reuse());
                }
            }

            // TODO(benesch): this blocks the Timely thread until the send
            // completes. Hopefully it's just a quick write to a kernel buffer,
            // but perhaps not if the batch gets too large? We may need to do
            // something smarter, like offloading to a networking thread.
            block_on(tx.send(results)).expect("tail send failed");
        });
    })
}

fn batch_upper(
    batch: Option<&Rc<OrdValBatch<GlobalId, Row, u64, isize, usize>>>,
) -> Option<Timestamp> {
    batch
        .map(|b| b.desc.upper().elements().get(0))
        .flatten()
        .copied()
}
