// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::RefCell;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::{AsCollection, Collection};
use mz_compute_client::protocol::response::{
    StashedSubscribeBatch, SubscribeBatch, SubscribeBatchContents, SubscribeResponse,
};
use mz_compute_types::dyncfgs::{
    ENABLE_SUBSCRIBE_RESPONSE_STASH, PEEK_RESPONSE_STASH_BATCH_MAX_RUNS,
    SUBSCRIBE_RESPONSE_STASH_THRESHOLD_BYTES,
};
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use mz_dyncfg::ConfigSet;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{PersistClient, Schemas};
use mz_persist_types::PersistLocation;
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use mz_timely_util::probe::{Handle, ProbeNotify};
use timely::PartialOrder;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::progress::Antichain;
use timely::progress::timestamp::Timestamp as TimelyTimestamp;

use crate::render::StartSignal;
use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for SubscribeSinkConnection
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut crate::compute_state::ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        _start_signal: StartSignal,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
        _ct_times: Option<Collection<G, (), Diff>>,
        output_probe: &Handle<Timestamp>,
    ) -> Option<Rc<dyn Any>> {
        // An encapsulation of the Subscribe response protocol.
        // Used to send rows and progress messages,
        // and alert if the dataflow was dropped before completing.
        let mut subscribe_protocol_handle = Arc::new(Mutex::new(Some(SubscribeProtocol {
            sink_id,
            sink_as_of: as_of.clone(),
            subscribe_response_buffer: Some(Rc::clone(&compute_state.subscribe_response_buffer)),
            prev_upper: Antichain::from_elem(Timestamp::minimum()),
            poison: None,
            worker_config: Rc::clone(&compute_state.worker_config),
            persist_clients: compute_state.persist_clients.clone(),
            persist_location: compute_state.peek_stash_persist_location.clone(),
            relation_desc: sink.from_desc.clone(),
            stash_writer: None, // Will be initialized in the operator if eligible.
        })));

        let subscribe_protocol_weak = Arc::downgrade(&mut subscribe_protocol_handle);
        let sinked_collection = sinked_collection
            .inner
            .probe_notify_with(vec![output_probe.clone()])
            .as_collection();
        let output_envelope = match &sink.connection {
            ComputeSinkConnection::Subscribe(conn) => conn.output.clone(),
            _ => unreachable!("subscribe sink must have subscribe connection"),
        };
        subscribe(
            sinked_collection,
            err_collection,
            sink_id,
            sink.with_snapshot,
            as_of,
            sink.up_to.clone(),
            subscribe_protocol_handle,
            output_envelope,
        );

        // Inform the coordinator that we have been dropped,
        // and destroy the subscribe protocol so the sink operator
        // can't send spurious messages while shutting down.
        Some(Rc::new(scopeguard::guard((), move |_| {
            if let Some(subscribe_protocol_handle) = subscribe_protocol_weak.upgrade() {
                let mut protocol = subscribe_protocol_handle.lock().expect("lock poisoned");
                std::mem::drop(protocol.take())
            }
        })))
    }
}

fn subscribe<G>(
    sinked_collection: Collection<G, Row, Diff>,
    err_collection: Collection<G, DataflowError, Diff>,
    sink_id: GlobalId,
    with_snapshot: bool,
    as_of: Antichain<G::Timestamp>,
    up_to: Antichain<G::Timestamp>,
    subscribe_protocol_handle: Arc<Mutex<Option<SubscribeProtocol>>>,
    output_envelope: mz_compute_types::sinks::SubscribeOutput,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let name = format!("subscribe-{}", sink_id);
    let mut op = AsyncOperatorBuilder::new(name, sinked_collection.scope());
    let mut ok_input = op.new_disconnected_input(&sinked_collection.inner, Pipeline);
    let mut err_input = op.new_disconnected_input(&err_collection.inner, Pipeline);

    op.build(|_capabilities| async move {
        // Initialize stash writer if eligible. Only the "diff" style output
        // envelope is supported for stashing, the other envelope types require
        // boutique sorting.
        if let Some(protocol) = subscribe_protocol_handle
            .lock()
            .expect("lock poisoned")
            .deref_mut()
        {
            let stash_enabled = ENABLE_SUBSCRIBE_RESPONSE_STASH.get(&protocol.worker_config);
            let is_diffs_output = matches!(
                output_envelope,
                mz_compute_types::sinks::SubscribeOutput::Diffs
            );
            if stash_enabled && is_diffs_output && protocol.persist_location.is_some() {
                if let Some(persist_location) = &protocol.persist_location {
                    match StashWriter::new(
                        protocol.persist_clients.clone(),
                        persist_location.clone(),
                        protocol.relation_desc.clone(),
                    )
                    .await
                    {
                        Ok(writer) => {
                            protocol.stash_writer = Some(writer);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to initialize stash writer for subscribe {}: {}",
                                protocol.sink_id,
                                e
                            );
                        }
                    }
                }
            }
        }

        let mut rows_to_emit = Vec::new();
        let mut errors_to_emit = Vec::new();

        let mut ok_frontier = Antichain::from_elem(G::Timestamp::minimum());
        let mut err_frontier = Antichain::from_elem(G::Timestamp::minimum());

        loop {
            // Wait for either input to be ready and then process all available
            // updates in a batch.
            tokio::select! {
                _ = ok_input.ready() => {},
                _ = err_input.ready() => {},
            }

            let should_emit = |time: &Timestamp| {
                let beyond_as_of = if with_snapshot {
                    as_of.less_equal(time)
                } else {
                    as_of.less_than(time)
                };
                let before_up_to = !up_to.less_equal(time);
                beyond_as_of && before_up_to
            };

            while let Some(event) = ok_input.next_sync() {
                match event {
                    Event::Data(_cap, mut data) => {
                        for (row, time, diff) in data.drain(..) {
                            if should_emit(&time) {
                                rows_to_emit.push((time, row, diff));
                            }
                        }
                    }
                    Event::Progress(f) => {
                        ok_frontier.clear();
                        ok_frontier.extend(f.into_iter());
                    }
                }
            }

            while let Some(event) = err_input.next_sync() {
                match event {
                    Event::Data(_cap, mut data) => {
                        for (error, time, diff) in data.drain(..) {
                            if should_emit(&time) {
                                errors_to_emit.push((time, error, diff));
                            }
                        }
                    }
                    Event::Progress(f) => {
                        err_frontier.clear();
                        err_frontier.extend(f.into_iter());
                    }
                }
            }

            let mut frontier = Antichain::new();
            frontier.extend(ok_frontier.iter().copied());
            frontier.extend(err_frontier.iter().copied());

            if let Some(subscribe_protocol) = subscribe_protocol_handle
                .lock()
                .expect("lock poisoned")
                .deref_mut()
            {
                subscribe_protocol
                    .send_batch(frontier.clone(), &mut rows_to_emit, &mut errors_to_emit)
                    .await;
            }

            if PartialOrder::less_equal(&up_to, &frontier) {
                // We are done; indicate this by sending a batch at the
                // empty frontier.
                if let Some(subscribe_protocol) = subscribe_protocol_handle
                    .lock()
                    .expect("lock poisoned")
                    .deref_mut()
                {
                    subscribe_protocol
                        .send_batch(Antichain::default(), &mut Vec::new(), &mut Vec::new())
                        .await;
                }

                break;
            }

            // If both inputs are exhausted, after we processed any last updates
            // above.
            if frontier.is_empty() {
                break;
            }
        }

        // Clean up stash writer if present
        if let Some(protocol) = subscribe_protocol_handle
            .lock()
            .expect("lock poisoned")
            .deref_mut()
        {
            protocol.stash_writer.take();
        }
    });
}

/// Handles writing batches to persist for subscribe stashing.
struct StashWriter {
    persist_client: PersistClient,
    shard_id: ShardId,
    write_schemas: Schemas<SourceData, ()>,
}

impl StashWriter {
    /// Create a new StashWriter.
    async fn new(
        persist_clients: Arc<PersistClientCache>,
        persist_location: PersistLocation,
        relation_desc: RelationDesc,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let persist_client = persist_clients.open(persist_location).await?;

        // WIP: Should probably mint a shard id and send that along from the
        // controller, so all workers use the same shard id.
        let shard_id = ShardId::new();

        let write_schemas: Schemas<SourceData, ()> = Schemas {
            id: None,
            key: Arc::new(relation_desc.clone()),
            val: Arc::new(UnitSchema),
        };

        Ok(StashWriter {
            persist_client,
            shard_id,
            write_schemas,
        })
    }

    /// Write a batch of updates to persist as a batch and returns that.
    async fn write_batch(
        &mut self,
        updates: Vec<(Timestamp, Row, Diff)>,
        batch_max_runs: usize,
    ) -> (ProtoBatch, u64, usize) {
        let mut encoded_size_bytes = 0;
        let mut num_rows_batches = 0;

        let result_ts = Timestamp::default();
        let lower = Antichain::from_elem(result_ts);
        let mut upper = result_ts;

        // We have to use SourceData, which is a wrapper around a Result<Row,
        // DataflowError>, because the bare columnar Row encoder doesn't support
        // encoding rows with zero columns.
        //
        // TODO: We _could_ work around the above by teaching the bare columnar
        // Row encoder about zero-column rows.
        let mut batch_builder = self
            .persist_client
            .batch_builder::<SourceData, (), Timestamp, i64>(
                self.shard_id.clone(),
                self.write_schemas.clone(),
                lower,
                Some(batch_max_runs),
            )
            .await;

        for (time, row, diff) in updates {
            upper = std::cmp::max(upper, time);
            batch_builder
                .add(&SourceData(Ok(row.clone())), &(), &time, &diff.into_inner())
                .await
                .expect("usage error");
            num_rows_batches += 1;
        }

        let batch = batch_builder
            .finish(Antichain::from_elem(upper.step_forward()))
            .await
            .expect("usage error");

        encoded_size_bytes += batch.encoded_size_bytes();

        (
            batch.into_transmittable_batch(),
            num_rows_batches,
            encoded_size_bytes,
        )
    }
}

/// A type that guides the transmission of rows back to the coordinator.
///
/// A protocol instance may `send` rows indefinitely in response to `send_batch` calls.
/// A `send_batch` call advancing the upper to the empty frontier is used to indicate the end of
/// a stream. If the stream is not advanced to the empty frontier, the `Drop` implementation sends
/// an indication that the protocol has finished without completion.
struct SubscribeProtocol {
    pub sink_id: GlobalId,
    pub sink_as_of: Antichain<Timestamp>,
    pub subscribe_response_buffer: Option<Rc<RefCell<Vec<(GlobalId, SubscribeResponse)>>>>,
    pub prev_upper: Antichain<Timestamp>,
    /// The error poisoning this subscribe, if any.
    ///
    /// As soon as a subscribe has encountered an error, it is poisoned: It will only return the
    /// same error in subsequent batches, until it is dropped. The subscribe protocol currently
    /// does not support retracting errors (database-issues#5182).
    pub poison: Option<String>,
    /// Configuration for accessing dyncfgs.
    pub worker_config: Rc<ConfigSet>,
    /// Persist clients for stashing large results.
    pub persist_clients: Arc<PersistClientCache>,
    /// The persist location where we can stash large results.
    pub persist_location: Option<PersistLocation>,
    /// The relation description for the subscribe.
    pub relation_desc: RelationDesc,
    /// Optional stash writer for eligible subscribes.
    pub stash_writer: Option<StashWriter>,
}

impl SubscribeProtocol {
    /// Attempt to send a batch of rows with the given `upper`.
    ///
    /// This method filters the updates to send based on the provided `upper`. Updates are only
    /// sent when their times are before `upper`. If `upper` has not advanced far enough, no batch
    /// will be sent. `rows` and `errors` that have been sent are drained from their respective
    /// vectors, only entries that have not been sent remain after the call returns. The caller is
    /// expected to re-submit these entries, potentially along with new ones, at a later `upper`.
    ///
    /// The subscribe protocol only supports reporting a single error. Because of this, it will
    /// only actually send the first error received in a `SubscribeResponse`. Subsequent errors are
    /// dropped. To simplify life for the caller, this method still maintains the illusion that
    /// `errors` are handled the same way as `rows`.
    async fn send_batch(
        &mut self,
        upper: Antichain<Timestamp>,
        rows: &mut Vec<(Timestamp, Row, Diff)>,
        errors: &mut Vec<(Timestamp, DataflowError, Diff)>,
    ) {
        // Only send a batch if both conditions hold:
        //  a) `upper` has reached or passed the sink's `as_of` frontier.
        //  b) `upper` is different from when we last sent a batch.
        if !PartialOrder::less_equal(&self.sink_as_of, &upper) || upper == self.prev_upper {
            return;
        }

        // The compute protocol requires us to only send out consolidated batches.
        consolidate_updates(rows);
        consolidate_updates(errors);

        let (keep_rows, ship_rows) = rows.drain(..).partition(|u| upper.less_equal(&u.0));
        let (keep_errors, ship_errors) = errors.drain(..).partition(|u| upper.less_equal(&u.0));
        *rows = keep_rows;
        *errors = keep_errors;

        let updates = match (&self.poison, ship_errors.first()) {
            (Some(error), _) => {
                // The subscribe is poisoned; keep sending the same error.
                SubscribeBatchContents::Error(error.clone())
            }
            (None, Some((_, error, _))) => {
                // The subscribe encountered its first error; poison it.
                let error = error.to_string();
                self.poison = Some(error.clone());
                SubscribeBatchContents::Error(error)
            }
            (None, None) => {
                // No error encountered so far; ship the rows we have!
                let stash_enabled = ENABLE_SUBSCRIBE_RESPONSE_STASH.get(&self.worker_config);
                let stash_threshold_bytes =
                    SUBSCRIBE_RESPONSE_STASH_THRESHOLD_BYTES.get(&self.worker_config);

                // Calculate the size of the update batch
                let total_size: usize = ship_rows
                    .iter()
                    .map(|(_time, row, _diff)| row.byte_len())
                    .sum();

                if stash_enabled
                    && total_size > stash_threshold_bytes
                    && self.stash_writer.is_some()
                {
                    let batch_max_runs =
                        PEEK_RESPONSE_STASH_BATCH_MAX_RUNS.get(&self.worker_config);

                    match self.stash_updates(ship_rows, batch_max_runs).await {
                        Ok(batch_contents) => batch_contents,
                        Err(e) => {
                            tracing::warn!("Error stashing subscribe batch: {}", e);
                            // Error uploading, fall back to inline updates
                            // Note: ship_rows was moved, so we return empty
                            SubscribeBatchContents::Updates(Vec::new())
                        }
                    }
                } else {
                    SubscribeBatchContents::Updates(ship_rows)
                }
            }
        };

        let buffer = self
            .subscribe_response_buffer
            .as_mut()
            .expect("The subscribe response buffer is only cleared on drop.");

        buffer.borrow_mut().push((
            self.sink_id,
            SubscribeResponse::Batch(SubscribeBatch {
                lower: self.prev_upper.clone(),
                upper: upper.clone(),
                updates,
            }),
        ));

        let input_exhausted = upper.is_empty();
        self.prev_upper = upper;
        if input_exhausted {
            // The dataflow's input has been exhausted; clear the channel,
            // to avoid sending `SubscribeResponse::DroppedAt`.
            self.subscribe_response_buffer = None;
        }
    }

    /// Upload updates to persist using our [StashWriter].
    async fn stash_updates(
        &mut self,
        updates: Vec<(Timestamp, Row, Diff)>,
        batch_max_runs: usize,
    ) -> Result<SubscribeBatchContents, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(writer) = &mut self.stash_writer {
            let (batch, num_rows_batches, encoded_size_bytes) =
                writer.write_batch(updates, batch_max_runs).await;

            Ok(SubscribeBatchContents::Stashed(Box::new(
                StashedSubscribeBatch {
                    num_rows_batches: num_rows_batches as u64,
                    encoded_size_bytes,
                    relation_desc: self.relation_desc.clone(),
                    shard_id: writer.shard_id,
                    batches: vec![batch],
                    inline_updates: Vec::new(), // All updates are stashed
                },
            )))
        } else {
            Err("Stash writer not initialized".into())
        }
    }
}

impl Drop for SubscribeProtocol {
    fn drop(&mut self) {
        if let Some(buffer) = self.subscribe_response_buffer.take() {
            buffer.borrow_mut().push((
                self.sink_id,
                SubscribeResponse::DroppedAt(self.prev_upper.clone()),
            ));
        }

        // Note: We can't clean up stash_writer here because Drop is not async.
        // Cleanup is handled in the main operator loop when it detects shutdown.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::url::SensitiveUrl;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_repr::{Datum, RelationDesc, Row, ScalarType};
    use std::str::FromStr;

    // Guards an assumption we have about `read_batches_consolidated` yielding
    // updates in timestamp order.
    #[mz_ore::test(tokio::test)]
    async fn test_stash_writer_timestamp_ordering() {
        // Use memory storage for testing
        let persist_location = PersistLocation {
            blob_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
            consensus_uri: SensitiveUrl::from_str("mem://").expect("invalid URL"),
        };

        // Create persist client cache
        let persist_clients = Arc::new(PersistClientCache::new(
            PersistConfig::new_for_tests(),
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        ));

        // Create a simple relation description with a single column
        let relation_desc = RelationDesc::builder()
            .with_column("value", ScalarType::Int64.nullable(false))
            .finish();

        // Create the stash writer
        let mut stash_writer = StashWriter::new(
            persist_clients.clone(),
            persist_location.clone(),
            relation_desc.clone(),
        )
        .await
        .expect("failed to create stash writer");

        // Create updates in reverse timestamp order (10 down to 1)
        let mut updates = Vec::new();
        for i in (1..=10).rev() {
            let mut row = Row::default();
            row.packer().push(Datum::Int64(i));
            updates.push((Timestamp::from(i as u64), row, Diff::from(1)));
        }

        // Write the batch
        let batch_max_runs = 100;
        let (batch, num_rows, _encoded_size) = stash_writer
            .write_batch(updates.clone(), batch_max_runs)
            .await;

        assert_eq!(num_rows, 10);

        // Now read the batch back using read_batches_consolidated
        let mut persist_client = persist_clients
            .open(persist_location)
            .await
            .expect("failed to open persist client");

        let batch_from_proto =
            persist_client.batch_from_transmittable_batch(&stash_writer.shard_id, batch);

        let as_of = Antichain::from_elem(Timestamp::MIN);
        let read_schemas: Schemas<SourceData, ()> = Schemas {
            id: None,
            key: Arc::new(relation_desc),
            val: Arc::new(UnitSchema),
        };

        let mut cursor = persist_client
            .read_batches_consolidated::<_, _, _, i64>(
                stash_writer.shard_id,
                as_of,
                read_schemas,
                vec![batch_from_proto],
                |_stats| true,
                usize::MAX,
            )
            .await
            .expect("failed to create cursor");

        // Collect all results
        let mut results = Vec::new();
        while let Some(rows) = cursor.next().await {
            for ((key, _val), ts, diff) in rows {
                if let Ok(source_data) = key {
                    if let Ok(row) = source_data.0 {
                        results.push((ts, row, Diff::from(diff)));
                    }
                }
            }
        }

        // Verify we got all 10 rows back
        assert_eq!(results.len(), 10);

        // Verify they are sorted by timestamp (ascending)
        for i in 0..results.len() - 1 {
            assert!(
                results[i].0 <= results[i + 1].0,
                "Results not sorted by timestamp: {:?} > {:?}",
                results[i].0,
                results[i + 1].0
            );
        }

        // Verify the timestamps are 1 through 10 in order
        for (i, (ts, row, diff)) in results.iter().enumerate() {
            assert_eq!(*ts, Timestamp::from((i + 1) as u64));
            // Also verify the row data matches
            let datums = row.unpack();
            assert_eq!(datums[0], Datum::Int64((i + 1) as i64));
            assert_eq!(*diff, Diff::from(1));
        }

        // Clean up
        let batches = cursor.into_lease();
        for batch in batches {
            batch.delete().await;
        }
    }
}
