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
use std::iter;

use itertools::Itertools;
use mz_expr::compare_columns;
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
    pub output: SubscribeOutput,
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

            // Fill in the mz_diff or mz_state column
            packer.push(Datum::Null);

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
                            SubscribeOutput::WithinTimestampOrderBy { order_by } => {
                                let mut left_datum_vec = mz_repr::DatumVec::new();
                                let mut right_datum_vec = mz_repr::DatumVec::new();
                                rows.sort_by(|(left_time, left_row, left_diff), (right_time, right_row, right_diff)| {
                                    left_time.cmp(right_time).then_with(|| {
                                        let mut left_datums = left_datum_vec.borrow();
                                        left_datums.extend(&[Datum::Int64(*left_diff)]);
                                        left_datums.extend(left_row.iter());
                                        let mut right_datums = right_datum_vec.borrow();
                                        right_datums.extend(&[Datum::Int64(*right_diff)]);
                                        right_datums.extend(right_row.iter());
                                        compare_columns(order_by, &left_datums, &right_datums, || {
                                            left_row.cmp(right_row).then(left_diff.cmp(right_diff))
                                        })
                                    })
                                });
                            }
                            SubscribeOutput::EnvelopeUpsert { order_by_keys } => {
                                let mut left_datum_vec = mz_repr::DatumVec::new();
                                let mut right_datum_vec = mz_repr::DatumVec::new();
                                rows.sort_by(|(left_time, left_row, left_diff), (right_time, right_row, right_diff)| {
                                    left_time.cmp(right_time).then_with(|| {
                                        let left_datums = left_datum_vec.borrow_with(left_row);
                                        let right_datums = right_datum_vec.borrow_with(right_row);
                                        compare_columns(order_by_keys, &left_datums, &right_datums, || left_diff.cmp(right_diff))
                                    })
                                });

                                let mut new_rows = Vec::new();
                                let mut it = rows.iter();
                                let mut datum_vec = mz_repr::DatumVec::new();
                                while let Some(start) = it.next() {
                                    let group =
                                        iter::once(start).chain(it.take_while_ref(|row| {
                                            let left_datums = left_datum_vec.borrow_with(&start.1);
                                            let right_datums = right_datum_vec.borrow_with(&row.1);
                                            start.0 == row.0
                                                && compare_columns(
                                                    order_by_keys,
                                                    &left_datums,
                                                    &right_datums,
                                                    || Ordering::Equal,
                                                ) == Ordering::Equal
                                        })).collect_vec();

                                    // Four cases:
                                    // [(key, value, +1)] => ("upsert", key, value
                                    // [(key, v1, -1), (key, v2, +1)] => ("upsert", key, v2)
                                    // [(key, value, -1)] => ("delete", key, NULL)
                                    // everything else => ("key violation", key, NULL)
                                    let mut packer = row_buf.packer();
                                    new_rows.push(match &group[..] {
                                        [(_, row, 1)] | [(_, _, -1), (_, row, 1)] => {
                                            // upsert
                                            let datums = datum_vec.borrow_with(row);
                                            for column_order in order_by_keys {
                                                packer.push(datums[column_order.column]);
                                            }
                                            for idx in 0..self.arity {
                                                if !order_by_keys.iter().any(|co| co.column == idx) {
                                                    packer.push(datums[idx]);
                                                }
                                            }
                                            (start.0, row_buf.clone(), 1)
                                        }
                                        [(_, _, -1)] => {
                                            // delete
                                            let datums = datum_vec.borrow_with(&start.1);
                                            for column_order in order_by_keys {
                                                packer.push(datums[column_order.column]);
                                            }
                                            for _ in 0..self.arity - order_by_keys.len() {
                                                packer.push(Datum::Null);
                                            }
                                            (start.0, row_buf.clone(), -1)
                                        }
                                        _ => {
                                            // key violation
                                            let datums = datum_vec.borrow_with(&start.1);
                                            for column_order in order_by_keys {
                                                packer.push(datums[column_order.column]);
                                            }
                                            for _ in 0..self.arity - order_by_keys.len() {
                                                packer.push(Datum::Null);
                                            }
                                            (start.0, row_buf.clone(), 0)
                                        }
                                    });
                                }
                                rows = new_rows;
                            }
                            SubscribeOutput::Diffs => rows.sort_by_key(|(time, _, _)| *time),
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

                                if matches!(self.output, SubscribeOutput::EnvelopeUpsert { .. }) {
                                    packer.push(match diff {
                                        -1 => Datum::String("delete"),
                                        0 => Datum::String("key violation"),
                                        1 => Datum::String("upsert"),
                                        _ => panic!("envelope upsert can only generate -1..1 diffs"),
                                    });
                                } else {
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
