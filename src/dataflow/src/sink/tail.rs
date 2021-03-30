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

use dataflow_types::{SinkAsOf, TailSinkConnector};
use expr::GlobalId;
use repr::adt::decimal::Significand;
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
    stream.sink(Pipeline, &format!("tail-{}", id), move |input| {
        input.for_each(|_, batches| {
            if errored {
                // TODO(benesch): we should actually drop the sink if the
                // receiver has gone away.
                return;
            }
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

            // TODO(benesch): the lack of backpressure here can result in
            // unbounded memory usage.
            if connector.tx.send(results).is_err() {
                errored = true;
                return;
            }
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
