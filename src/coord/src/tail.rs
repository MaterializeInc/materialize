// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations around supporting the TAIL protocol with the dataflow layer

use mz_compute_client::TailResponse;
use mz_repr::adt::numeric;
use mz_repr::{Datum, Row};
use tokio::sync::mpsc;

use crate::coord::PeekResponseUnary;

/// A description of a pending tail from coord's perspective
pub(crate) struct PendingTail {
    /// Channel to send responses to the client
    ///
    /// The responses have the form `PeekResponseUnary` but should perhaps become `TailResponse`.
    channel: mpsc::UnboundedSender<PeekResponseUnary>,
    /// Whether progress information should be emitted
    emit_progress: bool,
    /// Number of columns in the output
    arity: usize,
}

impl PendingTail {
    /// Create a new [PendingTail].
    /// * The `channel` receives batches of finalized PeekResponses.
    /// * If `emit_progress` is true, the finalized rows are either data or progress updates
    /// * `arity` is the arity of the sink relation.
    pub(crate) fn new(
        channel: mpsc::UnboundedSender<PeekResponseUnary>,
        emit_progress: bool,
        arity: usize,
    ) -> Self {
        Self {
            channel,
            emit_progress,
            arity,
        }
    }

    /// Process a tail response
    ///
    /// Returns `true` if the sink should be removed.
    pub(crate) fn process_response(&mut self, response: TailResponse) -> bool {
        let mut row_buf = Row::default();
        match response {
            TailResponse::Batch(mz_compute_client::TailBatch {
                lower: _,
                upper,
                updates: mut rows,
            }) => {
                // Sort results by time. We use stable sort here because it will produce deterministic
                // results since the cursor will always produce rows in the same order.
                // TODO: Is sorting necessary?
                rows.sort_by_key(|(time, _, _)| *time);

                let rows = rows
                    .into_iter()
                    .map(|(time, row, diff)| {
                        let mut packer = row_buf.packer();
                        packer.push(Datum::from(numeric::Numeric::from(time)));
                        if self.emit_progress {
                            // When sinking with PROGRESS, the output
                            // includes an additional column that
                            // indicates whether a timestamp is
                            // complete. For regular "data" updates this
                            // is always `false`.
                            packer.push(Datum::False);
                        }

                        packer.push(Datum::Int64(diff));

                        packer.extend_by_row(&row);

                        row_buf.clone()
                    })
                    .collect();
                // TODO(benesch): the lack of backpressure here can result in
                // unbounded memory usage.
                let result = self.channel.send(PeekResponseUnary::Rows(rows));
                if result.is_err() {
                    // TODO(benesch): we should actually drop the sink if the
                    // receiver has gone away. E.g. form a DROP SINK command?
                }

                if self.emit_progress && !upper.is_empty() {
                    assert_eq!(
                        upper.len(),
                        1,
                        "TAIL only supports single-dimensional timestamps"
                    );
                    let mut packer = row_buf.packer();
                    packer.push(Datum::from(numeric::Numeric::from(*&upper[0])));
                    packer.push(Datum::True);
                    // Fill in the diff column and all table columns with NULL.
                    for _ in 0..(self.arity + 1) {
                        packer.push(Datum::Null);
                    }

                    let result = self.channel.send(PeekResponseUnary::Rows(vec![row_buf]));
                    if result.is_err() {
                        // TODO(benesch): we should actually drop the sink if the
                        // receiver has gone away. E.g. form a DROP SINK command?
                    }
                }
                upper.is_empty()
            }
            TailResponse::DroppedAt(_frontier) => {
                // TODO: Could perhaps do this earlier, in response to DROP SINK.
                true
            }
        }
    }
}
