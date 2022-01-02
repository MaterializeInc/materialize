// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations around supporting the TAIL protocol with the dataflow layer

use dataflow_types::TailResponse;
use ore::cast::CastFrom;
use repr::adt::numeric;
use repr::{Datum, Row};
use tokio::sync::mpsc;

/// A description of a pending tail from coord's perspective
pub(crate) struct PendingTail {
    /// Channel to send responses to the client
    ///
    /// The responses have the form `Vec<Row>` but should perhaps become `TailResponse`.
    channel: mpsc::UnboundedSender<Vec<Row>>,
    /// Whether progress information should be emitted
    emit_progress: bool,
    /// Number of columns in the output
    object_columns: usize,
}

impl PendingTail {
    /// Create a new [PendingTail].
    /// * The `channel` receives batches of finalized rows.
    /// * If `emit_progress` is true, the finalized rows are either data or progress updates
    /// * `object_columns` is the arity of the sink relation.
    pub(crate) fn new(
        channel: mpsc::UnboundedSender<Vec<Row>>,
        emit_progress: bool,
        object_columns: usize,
    ) -> Self {
        Self {
            channel,
            emit_progress,
            object_columns,
        }
    }

    /// Process a tail response
    ///
    /// Returns `true` if the sink should be removed.
    pub(crate) fn process_response(&mut self, response: TailResponse) -> bool {
        let mut packer = Row::default();
        match response {
            TailResponse::Progress(upper) => {
                if self.emit_progress && !upper.is_empty() {
                    assert_eq!(
                        upper.len(),
                        1,
                        "TAIL only supports single-dimensional timestamps"
                    );
                    packer.push(Datum::from(numeric::Numeric::from(*&upper[0])));
                    packer.push(Datum::True);
                    // Fill in the diff column and all table columns with NULL.
                    for _ in 0..(self.object_columns + 1) {
                        packer.push(Datum::Null);
                    }
                    let row = packer.finish_and_reuse();

                    let result = self.channel.send(vec![row]);
                    if result.is_err() {
                        // TODO(benesch): we should actually drop the sink if the
                        // receiver has gone away. E.g. form a DROP SINK command?
                    }
                }
                upper.is_empty()
            }
            TailResponse::Rows(mut rows) => {
                // Sort results by time. We use stable sort here because it will produce deterministic
                // results since the cursor will always produce rows in the same order.
                // TODO: Is sorting necessary?
                rows.sort_by_key(|(_, time, _)| *time);

                let rows = rows
                    .into_iter()
                    .map(|(row, time, diff)| {
                        packer.push(Datum::from(numeric::Numeric::from(time)));
                        if self.emit_progress {
                            // When sinking with PROGRESS, the output
                            // includes an additional column that
                            // indicates whether a timestamp is
                            // complete. For regular "data" upates this
                            // is always `false`.
                            packer.push(Datum::False);
                        }

                        packer.push(Datum::Int64(i64::cast_from(diff)));

                        packer.extend_by_row(&row);

                        packer.finish_and_reuse()
                    })
                    .collect();
                // TODO(benesch): the lack of backpressure here can result in
                // unbounded memory usage.
                let result = self.channel.send(rows);
                if result.is_err() {
                    // TODO(benesch): we should actually drop the sink if the
                    // receiver has gone away. E.g. form a DROP SINK command?
                }
                false
            }
        }
    }
}
