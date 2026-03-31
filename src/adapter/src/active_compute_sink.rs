// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator bookkeeping for active compute sinks.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use anyhow::anyhow;
use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::protocol::response::SubscribeBatch;
use mz_controller_types::ClusterId;
use mz_expr::row::{RowCollection, RowCollectionBuilder};
use mz_expr::{RowComparator, compare_columns};
use mz_ore::cast::CastFrom;
use mz_ore::now::EpochMillis;
use mz_repr::adt::numeric;
use mz_repr::{CatalogItemId, Datum, Diff, GlobalId, IntoRowIterator, Row, RowRef, Timestamp};
use mz_sql::plan::SubscribeOutput;
use mz_storage_types::instances::StorageInstanceId;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::coord::peek::PeekResponseUnary;
use crate::{AdapterError, ExecuteContext, ExecuteResponse};

#[derive(Debug)]
/// A description of an active compute sink from the coordinator's perspective.
pub enum ActiveComputeSink {
    /// An active subscribe sink.
    Subscribe(ActiveSubscribe),
    /// An active copy to sink.
    CopyTo(ActiveCopyTo),
}

impl ActiveComputeSink {
    /// Reports the ID of the cluster on which the sink is running.
    pub fn cluster_id(&self) -> ClusterId {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => subscribe.cluster_id,
            ActiveComputeSink::CopyTo(copy_to) => copy_to.cluster_id,
        }
    }

    /// Reports the ID of the connection which created the sink.
    pub fn connection_id(&self) -> &ConnectionId {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => &subscribe.conn_id,
            ActiveComputeSink::CopyTo(copy_to) => &copy_to.conn_id,
        }
    }

    /// Reports the IDs of the objects on which the sink depends.
    pub fn depends_on(&self) -> &BTreeSet<GlobalId> {
        match &self {
            ActiveComputeSink::Subscribe(subscribe) => &subscribe.depends_on,
            ActiveComputeSink::CopyTo(copy_to) => &copy_to.depends_on,
        }
    }

    /// Retires the sink with the specified reason.
    ///
    /// This method must be called on every sink before it is dropped. It
    /// informs the end client that the sink is finished for the specified
    /// reason.
    pub fn retire(self, reason: ActiveComputeSinkRetireReason) {
        match self {
            ActiveComputeSink::Subscribe(subscribe) => subscribe.retire(reason),
            ActiveComputeSink::CopyTo(copy_to) => copy_to.retire(reason),
        }
    }
}

/// The reason for removing an [`ActiveComputeSink`].
#[derive(Debug, Clone)]
pub enum ActiveComputeSinkRetireReason {
    /// The compute sink completed successfully.
    Finished,
    /// The compute sink was canceled due to a user request.
    Canceled,
    /// The compute sink was forcibly terminated because an object it depended on
    /// was dropped.
    DependencyDropped(String),
}

/// A description of an active subscribe from coord's perspective
#[derive(Debug)]
pub struct ActiveSubscribe {
    /// The ID of the connection which created the subscribe.
    pub conn_id: ConnectionId,
    /// The UUID of the session which created the subscribe.
    pub session_uuid: Uuid,
    /// The ID of the cluster on which the subscribe is running.
    pub cluster_id: ClusterId,
    /// The IDs of the objects on which the subscribe depends.
    pub depends_on: BTreeSet<GlobalId>,
    /// Channel on which to send responses to the client.
    // The responses have the form `PeekResponseUnary` but should perhaps
    // become `SubscribeResponse`.
    pub channel: mpsc::UnboundedSender<PeekResponseUnary>,
    /// Whether progress information should be emitted.
    pub emit_progress: bool,
    /// The logical timestamp at which the subscribe began execution.
    pub as_of: Timestamp,
    /// The number of columns in the relation that was subscribed to.
    pub arity: usize,
    /// The time when the subscribe started.
    pub start_time: EpochMillis,
    /// How to present the subscribe's output.
    pub output: SubscribeOutput,
}

fn format_progress(
    builder: &mut RowCollectionBuilder,
    arity: usize,
    output: &SubscribeOutput,
    upper: Timestamp,
) {
    let mut row_buf = Row::default();
    let mut packer = row_buf.packer();
    packer.push(Datum::from(numeric::Numeric::from(upper)));
    packer.push(Datum::True);

    // Fill in the mz_diff or mz_state column
    packer.push(Datum::Null);

    // Fill all table columns with NULL.
    for _ in 0..arity {
        packer.push(Datum::Null);
    }

    if let SubscribeOutput::EnvelopeDebezium { order_by_keys } = output {
        for _ in 0..(arity - order_by_keys.len()) {
            packer.push(Datum::Null);
        }
    }
    builder.push(&row_buf, NonZeroUsize::MIN);
}

/// Generate the appropriate unary peek response for the given subscribe batch.
pub fn format_subscribe_response(
    batch: SubscribeBatch,
    arity: usize,
    as_of: Timestamp,
    output: &SubscribeOutput,
    emit_progress: bool,
) -> PeekResponseUnary {
    let comparator = RowComparator::new(output.row_order());
    let rows = match batch.updates {
        Ok(ref rows) => {
            let iters = rows.iter().map(|r| r.iter());
            let merged = mz_ore::iter::merge_iters_by(
                iters,
                |(left_row, left_time, _), (right_row, right_time, _)| {
                    left_time.cmp(right_time).then_with(|| {
                        comparator.compare_rows(left_row, right_row, || left_row.cmp(right_row))
                    })
                },
            );
            mz_ore::iter::consolidate_update_iter(merged)
        }
        Err(s) => {
            return PeekResponseUnary::Error(s);
        }
    };

    // Sort results by time. We use stable sort here because it will produce
    // deterministic results since the cursor will always produce rows in
    // the same order. Compute doesn't guarantee that the results are sorted
    // (materialize#18936)
    let mut output_buf = Row::default();
    let mut output_builder = RowCollection::builder(0, 0);
    let mut left_datum_vec = mz_repr::DatumVec::new();
    let mut right_datum_vec = mz_repr::DatumVec::new();
    let mut push_row = |row: &RowRef, time: Timestamp, diff: Diff| {
        assert!(as_of <= time);
        let mut packer = output_buf.packer();
        // TODO: Change to MzTimestamp.
        packer.push(Datum::from(numeric::Numeric::from(time)));
        if emit_progress {
            // When sinking with PROGRESS, the output includes an
            // additional column that indicates whether a timestamp is
            // complete. For regular "data" updates this is always
            // `false`.
            packer.push(Datum::False);
        }

        match output {
            SubscribeOutput::EnvelopeUpsert { .. } | SubscribeOutput::EnvelopeDebezium { .. } => {}
            SubscribeOutput::Diffs | SubscribeOutput::WithinTimestampOrderBy { .. } => {
                packer.push(Datum::Int64(diff.into_inner()));
            }
        }

        packer.extend_by_row_ref(row);

        output_builder.push(output_buf.as_row_ref(), NonZeroUsize::MIN);
    };

    match output {
        SubscribeOutput::WithinTimestampOrderBy { order_by } => {
            let mut rows: Vec<_> = rows.collect();
            // Since the diff is inserted as the first column, we can't take advantage of the
            // known ordering. (Aside from timestamp, I suppose.)
            rows.sort_by(
                |(left_row, left_time, left_diff), (right_row, right_time, right_diff)| {
                    left_time.cmp(right_time).then_with(|| {
                        let mut left_datums = left_datum_vec.borrow();
                        left_datums.extend(&[Datum::Int64(left_diff.into_inner())]);
                        left_datums.extend(left_row.iter());
                        let mut right_datums = right_datum_vec.borrow();
                        right_datums.extend(&[Datum::Int64(right_diff.into_inner())]);
                        right_datums.extend(right_row.iter());
                        compare_columns(order_by, &left_datums, &right_datums, || {
                            left_row.cmp(right_row).then(left_diff.cmp(right_diff))
                        })
                    })
                },
            );
            for (row, time, diff) in rows {
                push_row(row, *time, diff);
            }
        }
        SubscribeOutput::EnvelopeUpsert { order_by_keys }
        | SubscribeOutput::EnvelopeDebezium { order_by_keys } => {
            let debezium = matches!(output, SubscribeOutput::EnvelopeDebezium { .. });
            let mut it = rows.peekable();
            let mut datum_vec = mz_repr::DatumVec::new();
            let mut old_datum_vec = mz_repr::DatumVec::new();
            let comparator = RowComparator::new(order_by_keys.as_slice());
            let mut group = Vec::with_capacity(2);
            let mut row_buf = Row::default();
            // The iterator is sorted by time and key, so elements in the same group should be
            // adjacent already.
            while let Some(start) = it.next() {
                group.clear();
                group.push(start);
                while let Some(row) = it.peek()
                    && start.1 == row.1
                    && {
                        comparator
                            .compare_rows(start.0, row.0, || Ordering::Equal)
                            .is_eq()
                    }
                {
                    group.extend(it.next());
                }
                group.sort_by_key(|(_, _, d)| *d);

                // Four cases:
                // [(key, value, +1)] => ("insert", key, NULL, value)
                // [(key, v1, -1), (key, v2, +1)] => ("upsert", key, v1, v2)
                // [(key, value, -1)] => ("delete", key, value, NULL)
                // everything else => ("key_violation", key, NULL, NULL)
                let value_columns = arity - order_by_keys.len();
                let mut packer = row_buf.packer();
                match &group[..] {
                    [(row, _, Diff::ONE)] => {
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
                        for idx in 0..arity {
                            if !order_by_keys.iter().any(|co| co.column == idx) {
                                packer.push(datums[idx]);
                            }
                        }
                        push_row(row_buf.as_row_ref(), *start.1, Diff::ZERO)
                    }
                    [(_, _, Diff::MINUS_ONE)] => {
                        packer.push(Datum::String("delete"));
                        let datums = datum_vec.borrow_with(start.0);
                        for column_order in order_by_keys {
                            packer.push(datums[column_order.column]);
                        }
                        if debezium {
                            for idx in 0..arity {
                                if !order_by_keys.iter().any(|co| co.column == idx) {
                                    packer.push(datums[idx]);
                                }
                            }
                        }
                        for _ in 0..arity - order_by_keys.len() {
                            packer.push(Datum::Null);
                        }
                        push_row(row_buf.as_row_ref(), *start.1, Diff::ZERO)
                    }
                    [(old_row, _, Diff::MINUS_ONE), (row, _, Diff::ONE)] => {
                        packer.push(Datum::String("upsert"));
                        let datums = datum_vec.borrow_with(row);
                        let old_datums = old_datum_vec.borrow_with(old_row);

                        for column_order in order_by_keys {
                            packer.push(datums[column_order.column]);
                        }
                        if debezium {
                            for idx in 0..arity {
                                if !order_by_keys.iter().any(|co| co.column == idx) {
                                    packer.push(old_datums[idx]);
                                }
                            }
                        }
                        for idx in 0..arity {
                            if !order_by_keys.iter().any(|co| co.column == idx) {
                                packer.push(datums[idx]);
                            }
                        }
                        push_row(row_buf.as_row_ref(), *start.1, Diff::ZERO)
                    }
                    _ => {
                        packer.push(Datum::String("key_violation"));
                        let datums = datum_vec.borrow_with(start.0);
                        for column_order in order_by_keys {
                            packer.push(datums[column_order.column]);
                        }
                        if debezium {
                            for _ in 0..(arity - order_by_keys.len()) {
                                packer.push(Datum::Null);
                            }
                        }
                        for _ in 0..(arity - order_by_keys.len()) {
                            packer.push(Datum::Null);
                        }
                        push_row(row_buf.as_row_ref(), *start.1, Diff::ZERO)
                    }
                };
            }
        }
        SubscribeOutput::Diffs => {
            // Diffs output is sorted by time and row, so it can be pushed directly.
            for (row, time, diff) in rows {
                push_row(row, *time, diff)
            }
        }
    };

    if emit_progress && !batch.upper.less_equal(&as_of) {
        if let Some(upper) = batch.upper.as_option() {
            format_progress(&mut output_builder, arity, output, *upper);
        }
    }

    let rows = output_builder.build();
    let rows = Box::new(rows.into_row_iter());

    PeekResponseUnary::Rows(rows)
}

impl ActiveSubscribe {
    /// Initializes the subscription.
    ///
    /// This method must be called exactly once, after constructing an
    /// `ActiveSubscribe` and before calling `process_response`.
    pub fn initialize(&self) {
        if self.emit_progress {
            // A single, all-null row except for the progress timestamp.
            let mut output_builder = RowCollection::builder(8, 1);
            format_progress(&mut output_builder, self.arity, &self.output, self.as_of);
            self.send(PeekResponseUnary::Rows(Box::new(
                output_builder.build().into_row_iter(),
            )));
        }
    }

    /// Processes a subscribe response from the controller.
    ///
    /// Returns `true` if the subscribe is finished.
    pub fn process_response(&self, batch: SubscribeBatch) -> bool {
        let is_finished = match &batch.updates {
            Ok(_) => batch.upper.is_empty(),
            Err(_) => true,
        };
        let response = format_subscribe_response(
            batch,
            self.arity,
            self.as_of,
            &self.output,
            self.emit_progress,
        );

        self.send(response);

        is_finished
    }

    /// Retires the subscribe with the specified reason.
    ///
    /// This method must be called on every subscribe before it is dropped. It
    /// informs the end client that the subscribe is finished for the specified
    /// reason.
    pub fn retire(self, reason: ActiveComputeSinkRetireReason) {
        let message = match reason {
            ActiveComputeSinkRetireReason::Finished => return,
            ActiveComputeSinkRetireReason::Canceled => PeekResponseUnary::Canceled,
            ActiveComputeSinkRetireReason::DependencyDropped(d) => PeekResponseUnary::Error(
                format!("subscribe has been terminated because underlying {d} was dropped"),
            ),
        };
        self.send(message);
    }

    /// Sends a message to the client if the subscribe has not already completed
    /// and if the client has not already gone away.
    fn send(&self, response: PeekResponseUnary) {
        // TODO(benesch): the lack of backpressure here can result in
        // unbounded memory usage.
        let _ = self.channel.send(response);
    }
}

/// A description of an active copy to sink from the coordinator's perspective.
#[derive(Debug)]
pub struct ActiveCopyTo {
    /// The ID of the connection which created the subscribe.
    pub conn_id: ConnectionId,
    /// The result channel for the `COPY ... TO` statement that created the copy to sink.
    pub tx: oneshot::Sender<Result<ExecuteResponse, AdapterError>>,
    /// The ID of the cluster on which the copy to is running.
    pub cluster_id: ClusterId,
    /// The IDs of the objects on which the copy to depends.
    pub depends_on: BTreeSet<GlobalId>,
}

impl ActiveCopyTo {
    /// Retires the copy to with a response from the controller.
    ///
    /// Unlike subscribes, copy tos only expect a single response from the
    /// controller, so `process_response` and `retire` are unified into a single
    /// operation.
    ///
    /// Either this method or `retire` must be called on every copy to before it
    /// is dropped.
    pub fn retire_with_response(self, response: Result<u64, anyhow::Error>) {
        let response = match response {
            Ok(n) => Ok(ExecuteResponse::Copied(usize::cast_from(n))),
            Err(error) => Err(AdapterError::Unstructured(error)),
        };
        let _ = self.tx.send(response);
    }

    /// Retires the copy to with the specified reason.
    ///
    /// Either this method or `retire_with_response` must be called on every
    /// copy to before it is dropped.
    pub fn retire(self, reason: ActiveComputeSinkRetireReason) {
        let message = match reason {
            ActiveComputeSinkRetireReason::Finished => return,
            ActiveComputeSinkRetireReason::Canceled => Err(AdapterError::Canceled),
            ActiveComputeSinkRetireReason::DependencyDropped(d) => Err(AdapterError::Unstructured(
                anyhow!("copy has been terminated because underlying {d} was dropped"),
            )),
        };
        let _ = self.tx.send(message);
    }
}

/// State we keep in the `Coordinator` to track active `COPY FROM` statements.
#[derive(Debug)]
pub(crate) struct ActiveCopyFrom {
    /// ID of the ingestion running in clusterd.
    pub ingestion_id: uuid::Uuid,
    /// The cluster this is currently running on.
    pub cluster_id: StorageInstanceId,
    /// The table we're currently copying into.
    pub table_id: CatalogItemId,
    /// Context of the SQL session that ran the statement.
    pub ctx: ExecuteContext,
}
