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
use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::protocol::response::SubscribeBatch;
use mz_controller_types::ClusterId;
use mz_expr::compare_columns;
use mz_ore::now::EpochMillis;
use mz_repr::adt::numeric;
use mz_repr::{Datum, GlobalId, Row, Timestamp};
use mz_sql::plan::SubscribeOutput;
use mz_sql::session::user::User;
use timely::progress::Antichain;
use tokio::sync::mpsc;

use crate::coord::peek::PeekResponseUnary;
use crate::{AdapterError, ExecuteContext, ExecuteResponse};

#[derive(Debug)]
/// A description of an active compute sink from coord's perspective.
/// This is either a subscribe sink or a copy to sink.
pub enum ActiveComputeSink {
    Subscribe(ActiveSubscribe),
    CopyTo(ActiveCopyTo),
}

impl ActiveComputeSink {
    pub fn cluster_id(&self) -> ClusterId {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => subscribe.cluster_id,
            ActiveComputeSink::CopyTo(copy_to) => copy_to.cluster_id,
        }
    }

    pub fn connection_id(&self) -> &ConnectionId {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => &subscribe.conn_id,
            ActiveComputeSink::CopyTo(copy_to) => &copy_to.conn_id,
        }
    }

    pub fn user(&self) -> &User {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => &subscribe.user,
            ActiveComputeSink::CopyTo(copy_to) => &copy_to.user,
        }
    }

    pub fn depends_on(&self) -> &BTreeSet<GlobalId> {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => &subscribe.depends_on,
            ActiveComputeSink::CopyTo(copy_to) => &copy_to.depends_on,
        }
    }
}

/// A description of an active subscribe from coord's perspective
#[derive(Debug)]
pub struct ActiveSubscribe {
    /// The user of the session that created the subscribe.
    pub user: User,
    /// The connection id of the session that created the subscribe.
    pub conn_id: ConnectionId,
    /// Channel to send responses to the client.
    ///
    /// The responses have the form `PeekResponseUnary` but should perhaps
    /// become `SubscribeResponse`.
    pub channel: mpsc::UnboundedSender<PeekResponseUnary>,
    /// Whether progress information should be emitted.
    pub emit_progress: bool,
    /// As of of subscribe
    pub as_of: Timestamp,
    /// Number of columns in the output.
    pub arity: usize,
    /// The cluster that the subscribe is running on.
    pub cluster_id: ClusterId,
    /// All `GlobalId`s that the subscribe's expression depends on.
    pub depends_on: BTreeSet<GlobalId>,
    /// The time when the subscribe was started.
    pub start_time: EpochMillis,
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

            if let SubscribeOutput::EnvelopeDebezium { order_by_keys } = &self.output {
                for _ in 0..(self.arity - order_by_keys.len()) {
                    packer.push(Datum::Null);
                }
            }

            self.send(PeekResponseUnary::Rows(vec![row_buf]));
        }
    }

    /// Process a subscribe response.
    ///
    /// Returns `true` if the subscribe is finished.
    pub(crate) fn process_response(&mut self, batch: SubscribeBatch) -> bool {
        let mut row_buf = Row::default();
        match batch.updates {
            Ok(mut rows) => {
                // Sort results by time. We use stable sort here because it will produce deterministic
                // results since the cursor will always produce rows in the same order.
                // Compute doesn't guarantee that the results are sorted (#18936)
                match &self.output {
                    SubscribeOutput::WithinTimestampOrderBy { order_by } => {
                        let mut left_datum_vec = mz_repr::DatumVec::new();
                        let mut right_datum_vec = mz_repr::DatumVec::new();
                        rows.sort_by(
                            |(left_time, left_row, left_diff),
                             (right_time, right_row, right_diff)| {
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
                            },
                        );
                    }
                    SubscribeOutput::EnvelopeUpsert { order_by_keys }
                    | SubscribeOutput::EnvelopeDebezium { order_by_keys } => {
                        let debezium =
                            matches!(self.output, SubscribeOutput::EnvelopeDebezium { .. });
                        let mut left_datum_vec = mz_repr::DatumVec::new();
                        let mut right_datum_vec = mz_repr::DatumVec::new();
                        rows.sort_by(
                            |(left_time, left_row, left_diff),
                             (right_time, right_row, right_diff)| {
                                left_time.cmp(right_time).then_with(|| {
                                    let left_datums = left_datum_vec.borrow_with(left_row);
                                    let right_datums = right_datum_vec.borrow_with(right_row);
                                    compare_columns(
                                        order_by_keys,
                                        &left_datums,
                                        &right_datums,
                                        || left_diff.cmp(right_diff),
                                    )
                                })
                            },
                        );

                        let mut new_rows = Vec::new();
                        let mut it = rows.iter();
                        let mut datum_vec = mz_repr::DatumVec::new();
                        let mut old_datum_vec = mz_repr::DatumVec::new();
                        while let Some(start) = it.next() {
                            let group = iter::once(start)
                                .chain(it.take_while_ref(|row| {
                                    let left_datums = left_datum_vec.borrow_with(&start.1);
                                    let right_datums = right_datum_vec.borrow_with(&row.1);
                                    start.0 == row.0
                                        && compare_columns(
                                            order_by_keys,
                                            &left_datums,
                                            &right_datums,
                                            || Ordering::Equal,
                                        ) == Ordering::Equal
                                }))
                                .collect_vec();

                            // Four cases:
                            // [(key, value, +1)] => ("insert", key, NULL, value)
                            // [(key, v1, -1), (key, v2, +1)] => ("upsert", key, v1, v2)
                            // [(key, value, -1)] => ("delete", key, value, NULL)
                            // everything else => ("key_violation", key, NULL, NULL)
                            let value_columns = self.arity - order_by_keys.len();
                            let mut packer = row_buf.packer();
                            new_rows.push(match &group[..] {
                                [(_, row, 1)] => {
                                    packer.push(if debezium {
                                        Datum::String("insert")
                                    } else {
                                        Datum::String("upsert")
                                    });
                                    let datums = datum_vec.borrow_with(row);
                                    for column_order in order_by_keys {
                                        packer.push(datums[column_order.column]);
                                    }
                                    if debezium {
                                        for _ in 0..value_columns {
                                            packer.push(Datum::Null);
                                        }
                                    }
                                    for idx in 0..self.arity {
                                        if !order_by_keys.iter().any(|co| co.column == idx) {
                                            packer.push(datums[idx]);
                                        }
                                    }
                                    (start.0, row_buf.clone(), 0)
                                }
                                [(_, _, -1)] => {
                                    packer.push(Datum::String("delete"));
                                    let datums = datum_vec.borrow_with(&start.1);
                                    for column_order in order_by_keys {
                                        packer.push(datums[column_order.column]);
                                    }
                                    if debezium {
                                        for idx in 0..self.arity {
                                            if !order_by_keys.iter().any(|co| co.column == idx) {
                                                packer.push(datums[idx]);
                                            }
                                        }
                                    }
                                    for _ in 0..self.arity - order_by_keys.len() {
                                        packer.push(Datum::Null);
                                    }
                                    (start.0, row_buf.clone(), 0)
                                }
                                [(_, old_row, -1), (_, row, 1)] => {
                                    packer.push(Datum::String("upsert"));
                                    let datums = datum_vec.borrow_with(row);
                                    let old_datums = old_datum_vec.borrow_with(old_row);

                                    for column_order in order_by_keys {
                                        packer.push(datums[column_order.column]);
                                    }
                                    if debezium {
                                        for idx in 0..self.arity {
                                            if !order_by_keys.iter().any(|co| co.column == idx) {
                                                packer.push(old_datums[idx]);
                                            }
                                        }
                                    }
                                    for idx in 0..self.arity {
                                        if !order_by_keys.iter().any(|co| co.column == idx) {
                                            packer.push(datums[idx]);
                                        }
                                    }
                                    (start.0, row_buf.clone(), 0)
                                }
                                _ => {
                                    packer.push(Datum::String("key_violation"));
                                    let datums = datum_vec.borrow_with(&start.1);
                                    for column_order in order_by_keys {
                                        packer.push(datums[column_order.column]);
                                    }
                                    if debezium {
                                        for _ in 0..(self.arity - order_by_keys.len()) {
                                            packer.push(Datum::Null);
                                        }
                                    }
                                    for _ in 0..(self.arity - order_by_keys.len()) {
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

                        match &self.output {
                            SubscribeOutput::EnvelopeUpsert { .. }
                            | SubscribeOutput::EnvelopeDebezium { .. } => {}
                            SubscribeOutput::Diffs
                            | SubscribeOutput::WithinTimestampOrderBy { .. } => {
                                packer.push(Datum::Int64(diff));
                            }
                        }

                        packer.extend_by_row(&row);

                        row_buf.clone()
                    })
                    .collect();
                self.send(PeekResponseUnary::Rows(rows));
            }
            Err(text) => {
                self.send(PeekResponseUnary::Error(text));
            }
        }
        // Emit progress message if requested. Don't emit progress for the first batch if the upper
        // is exactly `as_of` (we're guaranteed it is not less than `as_of`, but it might be exactly
        // `as_of`) as we've already emitted that progress message in `initialize`.
        if !batch.upper.less_equal(&self.as_of) {
            self.send_progress_message(&batch.upper);
        }
        batch.upper.is_empty()
    }

    /// Sends a message to the client if the subscribe has not already completed
    /// and if the client has not already gone away.
    pub fn send(&self, response: PeekResponseUnary) {
        // TODO(benesch): the lack of backpressure here can result in
        // unbounded memory usage.
        let _ = self.channel.send(response);
    }
}

/// The reason for removing an [`ActiveComputeSink`].
#[derive(Debug, Clone)]
pub enum ComputeSinkRemovalReason {
    /// The compute sink completed successfully.
    Finished,
    /// The compute sink was canceled due to a user request.
    Canceled,
    /// The compute sink was forcibly terminated because an object it depended on
    /// was dropped.
    DependencyDropped(String),
}

/// A description of an active copy to from coord's perspective.
#[derive(Debug)]
pub struct ActiveCopyTo {
    /// The user of the session that created the subscribe.
    pub user: User,
    /// The connection id of the session that created the subscribe.
    pub conn_id: ConnectionId,
    /// The context about the COPY TO statement getting executed.
    /// Used to eventually call `ctx.retire` on.
    pub ctx: Option<ExecuteContext>,
    /// The cluster that the copy to is running on.
    pub cluster_id: ClusterId,
    /// All `GlobalId`s that the copy to's expression depends on.
    pub depends_on: BTreeSet<GlobalId>,
}

impl ActiveCopyTo {
    pub(crate) fn process_response(&mut self, response: Result<ExecuteResponse, AdapterError>) {
        // TODO(mouli): refactor so that process_response takes `self` instead of `&mut self`. Then,
        // we can make `ctx` not optional, and get rid of the `user` and `conn_id` as well.
        // Using an option to get an owned value after take to call `retire` on it.
        if let Some(ctx) = self.ctx.take() {
            let _ = ctx.retire(response);
        }
    }
}
