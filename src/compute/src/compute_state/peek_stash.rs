// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! For eligible peeks, we send the result back via the peek stash (aka persist
//! blob), instead of inline in `ComputeResponse`.

use std::num::{NonZeroI64, NonZeroU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::{PeekResponse, StashedPeekResponse};
use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::Schemas;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{Diff, RelationDesc, Row, Timestamp};
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tracing::debug;
use uuid::Uuid;

use crate::arrangement::manager::{PaddedTrace, TraceBundle};
use crate::compute_state::peek_result_iterator;
use crate::compute_state::peek_result_iterator::PeekResultIterator;
use crate::typedefs::RowRowAgent;

/// An async task that stashes a peek response in persist and yield a handle to
/// the batch in a [PeekResponse::Stashed].
///
/// Note that `PeekResponseTask` intentionally does not implement or derive
/// `Clone`, as each `PeekResponseTask` is meant to be dropped after it's
/// done or no longer needed.
pub struct StashPeekResponse {
    pub peek: Peek,
    /// Iterator for the results. The worker thread has to continually pump
    /// results from this to the `rows_tx` channel.
    peek_iterator: Option<PeekResultIterator<PaddedTrace<RowRowAgent<Timestamp, Diff>>>>,
    /// We can't give a PeekResultIterator to our async upload task because the
    /// underlying trace reader is not Send/Sync. So we need to use a channel to
    /// send result rows from the worker thread to the async background task.
    rows_tx: Option<tokio::sync::mpsc::Sender<Result<Vec<(Row, NonZeroI64)>, String>>>,
    /// When the async task is busy it cannot work of rows, and we don't want to
    /// use an unbounded channel. We stash rows that we cannot send and retry
    /// later.
    stashed_rows: Option<Result<Vec<(Row, NonZeroI64)>, String>>,
    /// The result of the background task, eventually.
    pub result: oneshot::Receiver<(PeekResponse, Duration)>,
    /// A background task that's responsible for producing the peek results.
    /// If we're no longer interested in the results, we abort the task.
    _abort_handle: AbortOnDropHandle<()>,
}

impl StashPeekResponse {
    pub fn start_upload(
        persist_clients: Arc<PersistClientCache>,
        persist_location: &PersistLocation,
        peek: Peek,
        mut trace_bundle: TraceBundle,
    ) -> Self {
        let (rows_tx, rows_rx) = tokio::sync::mpsc::channel(10);
        let (result_tx, result_rx) = oneshot::channel::<(PeekResponse, Duration)>();

        let persist_clients = Arc::clone(&persist_clients);
        let persist_location = persist_location.clone();

        let peek_uuid = peek.uuid;
        let relation_desc = peek.result_desc.clone();

        let oks_handle = trace_bundle.oks_mut();

        let peek_iterator = peek_result_iterator::PeekResultIterator::new(
            peek.target.id().clone(),
            peek.map_filter_project.clone(),
            peek.timestamp,
            peek.literal_constraints.clone(),
            oks_handle,
        );

        let task_handle = mz_ore::task::spawn(|| "compute::stash_peek_response", async move {
            let start = Instant::now();

            let result = Self::do_upload(
                &persist_clients,
                persist_location,
                peek.uuid,
                relation_desc,
                rows_rx,
            )
            .await;

            let result = match result {
                Ok(peek_response) => peek_response,
                Err(e) => PeekResponse::Error(e.to_string()),
            };
            match result_tx.send((result, start.elapsed())) {
                Ok(()) => {}
                Err((_result, elapsed)) => {
                    debug!(duration = ?elapsed, "dropping result for cancelled peek {}", peek_uuid)
                }
            }
        });

        Self {
            peek,
            peek_iterator: Some(peek_iterator),
            rows_tx: Some(rows_tx),
            stashed_rows: None,
            result: result_rx,
            _abort_handle: task_handle.abort_on_drop(),
        }
    }

    async fn do_upload(
        persist_clients: &PersistClientCache,
        persist_location: PersistLocation,
        peek_uuid: Uuid,
        relation_desc: RelationDesc,
        mut rows_rx: tokio::sync::mpsc::Receiver<Result<Vec<(Row, NonZeroI64)>, String>>,
    ) -> Result<PeekResponse, String> {
        let client = persist_clients
            .open(persist_location)
            .await
            .map_err(|e| e.to_string())?;

        let shard_id = format!("s{}", peek_uuid);
        let shard_id = ShardId::try_from(shard_id).expect("can parse");
        let write_schemas: Schemas<SourceData, ()> = Schemas {
            id: None,
            key: Arc::new(relation_desc.clone()),
            val: Arc::new(UnitSchema),
        };

        let result_ts = Timestamp::default();
        let lower = Antichain::from_elem(result_ts);
        let upper = Antichain::from_elem(result_ts.step_forward());

        // We have to use SourceData, which is a wrapper around a Result<Row,
        // DataflowError>, because the bare columnar Row encoder doesn't support
        // encoding rows with zero columns.
        //
        // TODO: We _could_ work around the above by teaching the bare columnar
        // Row encoder about zero-column rows.
        let mut batch_builder = client
            .batch_builder::<SourceData, (), Timestamp, i64>(shard_id, write_schemas, lower)
            .await;

        let mut num_rows: u64 = 0;

        loop {
            let row = rows_rx.recv().await;
            match row {
                Some(Ok(rows)) => {
                    assert!(
                        rows.len() > 0,
                        "the contract is that we only get non-empty batches"
                    );

                    for (row, diff) in rows {
                        num_rows +=
                            u64::from(NonZeroU64::try_from(diff).expect("diff fits into u64"));
                        let diff: i64 = diff.into();

                        batch_builder
                            .add(&SourceData(Ok(row)), &(), &Timestamp::default(), &diff)
                            .await
                            .expect("invalid usage");
                    }
                }
                Some(Err(err)) => return Ok(PeekResponse::Error(err)),
                None => {
                    break;
                }
            }
        }

        let batch = batch_builder.finish(upper).await.expect("invalid usage");

        let stashed_response = StashedPeekResponse {
            num_rows: u64::cast_from(num_rows),
            encoded_size_bytes: batch.encoded_size_bytes(),
            relation_desc,
            shard_id,
            batches: vec![batch.into_transmittable_batch()],
            inline_rows: RowCollection::new(vec![], &[]),
        };
        let result = PeekResponse::Stashed(stashed_response);
        Ok(result)
    }

    /// Pumps rows from the [PeekResultIterator] to the async task, via our
    /// `rows_tx`. Will pump at most `batch_size` rows in one batch, and at most
    /// the given `num_batches` batches.
    pub fn pump_rows(&mut self, mut num_batches: usize, batch_size: usize) {
        while self.peek_iterator.is_some() || self.stashed_rows.is_some() {
            let rows = if let Some(rows) = self.stashed_rows.take() {
                Some(rows)
            } else {
                match self.peek_iterator.as_mut() {
                    Some(row_iter) => {
                        let rows: Result<Vec<_>, _> = row_iter.take(batch_size).collect();
                        let rows = rows.map(|rows| if rows.is_empty() { None } else { Some(rows) });
                        rows.transpose()
                    }
                    None => None,
                }
            };

            if let Some(rows) = rows {
                match self
                    .rows_tx
                    .as_mut()
                    .expect("missing rows_tx")
                    .try_send(rows)
                {
                    Ok(_) => {
                        // All good!
                    }
                    Err(TrySendError::Full(rows)) => {
                        // Need to stash the row and try again later
                        let prev = self.stashed_rows.replace(rows);
                        assert!(prev.is_none());
                    }
                    _ => {}
                }
            } else {
                // We are done, yank our iterator and the row_tx.
                self.peek_iterator.take();
                self.rows_tx.take();
            }

            num_batches -= 1;
            if num_batches == 0 {
                break;
            }
        }
    }
}
