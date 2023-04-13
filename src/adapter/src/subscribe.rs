// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations around supporting the SUBSCRIBE protocol with the dataflow layer

use std::cmp::Ordering;
use std::collections::BTreeSet;

use itertools::Itertools;
use mz_expr::{compare_columns, ColumnOrder};
use mz_ore::now::EpochMillis;
use mz_sql::plan::SubscribeOutput;
use timely::progress::Antichain;
use tokio::sync::mpsc;

use mz_compute_client::protocol::response::{SubscribeBatch, SubscribeResponse};
use mz_controller::clusters::ClusterId;
use mz_repr::adt::numeric;
use mz_repr::{Datum, GlobalId, Row, Timestamp};
use mz_sql::session::user::User;

use crate::client::ConnectionId;
use crate::coord::peek::PeekResponseUnary;

/// A description of an active subscribe from coord's perspective
#[derive(Debug)]
pub struct ActiveSubscribe {
    /// The user of the session that created the subscribe.
    pub user: User,
    /// The connection id of the session that created the subscribe.
    pub conn_id: ConnectionId,
    /// Channel to send responses to the client.
    ///
    /// The responses have the form `PeekResponseUnary` but should perhaps become `TailResponse`.
    pub channel: mpsc::UnboundedSender<PeekResponseUnary>,
    /// Whether progress information should be emitted.
    pub emit_progress: bool,
    /// As of of subscribe
    pub as_of: Timestamp,
    /// Number of columns in the output.
    pub arity: usize,
    /// The cluster that the subscribe is running on.
    pub cluster_id: ClusterId,
    /// All `GlobalId`s that the subscribe depend on.
    pub depends_on: BTreeSet<GlobalId>,
    /// The time when the subscribe was started.
    pub start_time: EpochMillis,
    /// Whether we are already in the process of dropping the resources related to this subscribe.
    pub dropping: bool,
    /// How to modify output
    pub output: Option<SubscribeOutput>,
}

impl ActiveSubscribe {
    pub(crate) fn initialize(&self) {
        // Always emit progress message indicating snapshot timestamp.
        self.send_progress_message(&Antichain::from_elem(self.as_of));
    }

    fn send_progress_message(&self, upper: &Antichain<Timestamp>) {
        if !self.emit_progress {
            return;
        }
        if let Some(upper) = upper.as_option() {
            let mut row_buf = Row::default();
            let mut packer = row_buf.packer();
            packer.push(Datum::from(numeric::Numeric::from(*upper)));
            packer.push(Datum::True);

            // Fill in the diff column
            if !matches!(self.output, Some(SubscribeOutput::EnvelopeUpsert {..})) {
                packer.push(Datum::Null);
            }
            // Fill all table columns with NULL.
            for _ in 0..self.arity {
                packer.push(Datum::Null);
            }

            let result = self.channel.send(PeekResponseUnary::Rows(vec![row_buf]));
            if result.is_err() {
                // TODO(benesch): we should actually drop the sink if the
                // receiver has gone away. E.g. form a DROP SINK command?
            }
        }
    }

    /// Process a subscribe response
    ///
    /// Returns `true` if the sink should be removed.
    pub(crate) fn process_response(&mut self, response: SubscribeResponse) -> bool {
        let mut row_buf = Row::default();
        match response {
            SubscribeResponse::Batch(SubscribeBatch {
                lower: _,
                upper,
                updates,
            }) => {
                match updates {
                    Ok(mut rows) => {
                        // Sort results by time. We use stable sort here because it will produce deterministic
                        // results since the cursor will always produce rows in the same order.
                        // TODO: Is sorting by time necessary?
                        match &self.output {
                            Some(SubscribeOutput::WithinTimestampOrderBy { order_by }) => {
                                tracing::info!(?order_by);
                                let mut left_datum_vec = mz_repr::DatumVec::new();
                                let mut right_datum_vec = mz_repr::DatumVec::new();
                                rows.sort_by(|(left_time, left_row, left_diff), (right_time, right_row, right_diff)| {
                                    left_time.cmp(right_time).then_with(|| {
                                        let left_datums = left_datum_vec.borrow_with(left_row);
                                        let right_datums = right_datum_vec.borrow_with(right_row);
                                        compare_columns(order_by, &left_datums, &right_datums, || {
                                            left_row.cmp(right_row).then(left_diff.cmp(right_diff))
                                        })
                                    })
                                });
                            },
                            Some(SubscribeOutput::EnvelopeUpsert { key_indices }) => {
                                let mut left_datum_vec = mz_repr::DatumVec::new();
                                let mut right_datum_vec = mz_repr::DatumVec::new();
                                let order_by = &key_indices
                                .iter()
                                .map(|idx| ColumnOrder {
                                    column: *idx,
                                    desc: false,
                                    nulls_last: true,
                                })
                                .collect_vec();
                                rows.sort_by(|(left_time, left_row, left_diff), (right_time, right_row, right_diff)| {
                                    left_time.cmp(right_time).then_with(|| {
                                        let left_datums = left_datum_vec.borrow_with(left_row);
                                        let right_datums = right_datum_vec.borrow_with(right_row);
                                        compare_columns(order_by, &left_datums, &right_datums, || {
                                            left_row.cmp(right_row).then(left_diff.cmp(right_diff))
                                        })
                                    })
                                });

                                let mut new_rows = Vec::new();
                                let mut it = rows.iter();
                                let mut datum_vec = mz_repr::DatumVec::new();
                                while let Some(start) = it.next() {
                                    let group = std::iter::once(start).chain(it.take_while_ref(|row| {
                                        let left_datums = left_datum_vec.borrow_with(&start.1);
                                        let right_datums = right_datum_vec.borrow_with(&row.1);
                                        start.0 == row.0 && compare_columns(order_by, &left_datums, &right_datums, || Ordering::Equal) == Ordering::Equal
                                    }
                                    ));
                                    let mut saw_new_row = false;
                                    for (t, r, d) in group {
                                        if *d < 0 { continue }
                                        let mut packer = row_buf.packer();
                                        let datums = datum_vec.borrow_with(r);
                                        for idx in key_indices {
                                            packer.push(datums[*idx]);
                                        }
                                        for idx in 0..self.arity {
                                            if !key_indices.contains(&idx) {
                                                packer.push(datums[idx]);
                                            }
                                        }

                                        saw_new_row = true;
                                        new_rows.push((*t, row_buf.clone(), 1));
                                    }

                                    if !saw_new_row {  // emit deletion
                                        let mut packer = row_buf.packer();
                                        let datums = datum_vec.borrow_with(&start.1);
                                        for idx in key_indices {
                                            packer.push(datums[*idx]);
                                        }
                                        for _ in 0..self.arity - key_indices.len() {
                                            packer.push(Datum::Null);
                                        }
                                        new_rows.push((start.0, row_buf.clone(), 1));
                                    }
                                }
                                rows = new_rows;
                            },
                            None => rows.sort_by_key(|(time, _, _)| *time),
                        }

                        let rows = rows
                            .into_iter()
                            .map(|(time, row, diff)| {
                                assert!(self.as_of <= time);
                                let mut packer = row_buf.packer();
                                // TODO: Change to MzTimestamp.
                                packer.push(Datum::from(numeric::Numeric::from(time)));
                                if self.emit_progress {
                                    // When sinking with PROGRESS, the output
                                    // includes an additional column that
                                    // indicates whether a timestamp is
                                    // complete. For regular "data" updates this
                                    // is always `false`.
                                    packer.push(Datum::False);
                                }

                                if !matches!(self.output, Some(SubscribeOutput::EnvelopeUpsert {..})) {
                                    packer.push(Datum::Int64(diff));
                                }

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
                    }
                    Err(text) => {
                        let result = self.channel.send(PeekResponseUnary::Error(text));
                        if result.is_err() {
                            // TODO(benesch): we should actually drop the sink if the
                            // receiver has gone away. E.g. form a DROP SINK command?
                        }
                    }
                }
                // Emit progress message if requested. Don't emit progress for the first batch if the upper
                // is exactly `as_of` (we're guaranteed it is not less than `as_of`, but it might be exactly
                // `as_of`) as we've already emitted that progress message in `initialize`.
                if !upper.less_equal(&self.as_of) {
                    self.send_progress_message(&upper);
                }
                upper.is_empty()
            }
            SubscribeResponse::DroppedAt(_frontier) => {
                // TODO: Could perhaps do this earlier, in response to DROP SINK.
                true
            }
        }
    }
}
