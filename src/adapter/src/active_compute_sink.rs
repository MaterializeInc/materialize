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
use std::iter;
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::protocol::response::{SubscribeBatch, SubscribeBatchContents};
use mz_compute_types::sinks::SubscribeOutput;
use mz_controller_types::ClusterId;
use mz_expr::compare_columns;
use mz_ore::cast::CastFrom;
use mz_ore::now::EpochMillis;
use mz_persist_client::{PersistClient, Schemas};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::adt::numeric;
use mz_repr::{CatalogItemId, Datum, Diff, GlobalId, IntoRowIterator, Row, Timestamp};
use mz_storage_types::instances::StorageInstanceId;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
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

impl ActiveSubscribe {
    /// Initializes the subscription.
    ///
    /// This method must be called exactly once, after constructing an
    /// `ActiveSubscribe` and before calling `process_response`.
    pub fn initialize(&self) {
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

            let row_iter = Box::new(row_buf.into_row_iter());
            self.send(PeekResponseUnary::Rows(row_iter));
        }
    }

    /// Processes a subscribe response from the controller.
    ///
    /// Returns `true` if the subscribe is finished.
    pub async fn process_response(
        &self,
        batch: SubscribeBatch,
        mut persist_client: PersistClient,
    ) -> bool {
        let mut rows = match batch.updates {
            SubscribeBatchContents::Updates(rows) => rows,
            SubscribeBatchContents::Error(s) => {
                self.send(PeekResponseUnary::Error(s));
                return true;
            }
            SubscribeBatchContents::Stashed(stashed) => {
                // Read the stashed batches from persist and consolidate them
                let mut all_updates = Vec::new();

                // First add any inline updates
                all_updates.extend(stashed.inline_updates.clone());

                // Read the stashed batches
                if !stashed.batches.is_empty() {
                    let mut batches = Vec::new();
                    for proto_batch in stashed.batches.iter() {
                        let batch = persist_client
                            .batch_from_transmittable_batch(&stashed.shard_id, proto_batch.clone());
                        batches.push(batch);
                    }

                    // We want all the updates in the batch, so read from the
                    // beginning.
                    let as_of = Antichain::from_elem(Timestamp::MIN);

                    let read_schemas: Schemas<SourceData, ()> = Schemas {
                        id: None,
                        key: Arc::new(stashed.relation_desc.clone()),
                        val: Arc::new(UnitSchema),
                    };

                    let mut cursor = persist_client
                        .read_batches_consolidated::<_, _, _, i64>(
                            stashed.shard_id,
                            as_of.clone(),
                            read_schemas,
                            batches,
                            |_stats| true,
                            usize::MAX, // Memory budget - use max for now
                        )
                        .await
                        .expect("invalid usage");

                    while let Some(rows) = cursor.next().await {
                        for ((key, _val), ts, diff) in rows {
                            if let Ok(source_data) = key {
                                if let Ok(row) = source_data.0 {
                                    all_updates.push((ts.clone(), row, Diff::from(diff)));
                                }
                            }
                        }
                    }

                    // Clean up the batches
                    let batches = cursor.into_lease();
                    for batch in batches {
                        batch.delete().await;
                    }
                }

                all_updates
            }
        };

        // Sort results by time. We use stable sort here because it will produce
        // deterministic results since the cursor will always produce rows in
        // the same order. Compute doesn't guarantee that the results are sorted
        // (materialize#18936)
        let mut row_buf = Row::default();
        match &self.output {
            SubscribeOutput::WithinTimestampOrderBy { order_by } => {
                let mut left_datum_vec = mz_repr::DatumVec::new();
                let mut right_datum_vec = mz_repr::DatumVec::new();
                rows.sort_by(
                    |(left_time, left_row, left_diff), (right_time, right_row, right_diff)| {
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
            }
            SubscribeOutput::EnvelopeUpsert { order_by_keys }
            | SubscribeOutput::EnvelopeDebezium { order_by_keys } => {
                let debezium = matches!(self.output, SubscribeOutput::EnvelopeDebezium { .. });
                let mut left_datum_vec = mz_repr::DatumVec::new();
                let mut right_datum_vec = mz_repr::DatumVec::new();
                rows.sort_by(
                    |(left_time, left_row, left_diff), (right_time, right_row, right_diff)| {
                        left_time.cmp(right_time).then_with(|| {
                            let left_datums = left_datum_vec.borrow_with(left_row);
                            let right_datums = right_datum_vec.borrow_with(right_row);
                            compare_columns(order_by_keys, &left_datums, &right_datums, || {
                                left_diff.cmp(right_diff)
                            })
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
                        [(_, row, Diff::ONE)] => {
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
                            (start.0, row_buf.clone(), Diff::ZERO)
                        }
                        [(_, _, Diff::MINUS_ONE)] => {
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
                            (start.0, row_buf.clone(), Diff::ZERO)
                        }
                        [(_, old_row, Diff::MINUS_ONE), (_, row, Diff::ONE)] => {
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
                            (start.0, row_buf.clone(), Diff::ZERO)
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
                            (start.0, row_buf.clone(), Diff::ZERO)
                        }
                    });
                }
                rows = new_rows;
            }
            SubscribeOutput::Diffs => rows.sort_by_key(|(time, _, _)| *time),
        }

        let rows: Vec<Row> = rows
            .into_iter()
            .map(|(time, row, diff)| {
                assert!(self.as_of <= time);
                let mut packer = row_buf.packer();
                // TODO: Change to MzTimestamp.
                packer.push(Datum::from(numeric::Numeric::from(time)));
                if self.emit_progress {
                    // When sinking with PROGRESS, the output includes an
                    // additional column that indicates whether a timestamp is
                    // complete. For regular "data" updates this is always
                    // `false`.
                    packer.push(Datum::False);
                }

                match &self.output {
                    SubscribeOutput::EnvelopeUpsert { .. }
                    | SubscribeOutput::EnvelopeDebezium { .. } => {}
                    SubscribeOutput::Diffs | SubscribeOutput::WithinTimestampOrderBy { .. } => {
                        packer.push(Datum::Int64(diff.into_inner()));
                    }
                }

                packer.extend_by_row(&row);

                row_buf.clone()
            })
            .collect();
        let rows = Box::new(rows.into_row_iter());

        self.send(PeekResponseUnary::Rows(rows));

        // Emit progress message if requested. Don't emit progress for the first
        // batch if the upper is exactly `as_of` (we're guaranteed it is not
        // less than `as_of`, but it might be exactly `as_of`) as we've already
        // emitted that progress message in `initialize`.
        if !batch.upper.less_equal(&self.as_of) {
            self.send_progress_message(&batch.upper);
        }

        batch.upper.is_empty()
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
