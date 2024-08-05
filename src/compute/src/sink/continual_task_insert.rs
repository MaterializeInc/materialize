// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{Collection, Hashable};
use futures::StreamExt;
use itertools::Itertools;
use mz_compute_types::sinks::{ComputeSinkDesc, ContinualTaskInsertConnection};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::controller::{CollectionMetadata, TxnsCodecRow};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_storage_types::PersistEpoch;
use mz_timely_util::builder_async::{Event, OperatorBuilder as AsyncOperatorBuilder};
use mz_txn_wal::metrics::Metrics as TxnMetrics;
use mz_txn_wal::txns::{Tidy, TxnsHandle};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Concat, Map};
use timely::dataflow::Scope;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;

impl<G> SinkRender<G> for ContinualTaskInsertConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        start_signal: StartSignal,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>> {
        let mut scope = sinked_collection.inner.scope();
        tracing::info!("render: {:#?}", sink);

        let (to_retract_ok_stream, to_retract_err_stream, to_retract_token) =
            mz_storage_operators::persist_source::persist_source(
                &mut scope,
                sink_id,
                Arc::clone(&compute_state.persist_clients),
                &compute_state.txns_ctx,
                &compute_state.worker_config,
                self.retract_from_metadata.clone(),
                Some(as_of.clone()),
                SnapshotMode::Include,
                Antichain::new(), // we want all updates
                None,             // no MFP
                compute_state.dataflow_max_inflight_bytes(),
                start_signal,
                |error| panic!("continual_task: {error}"),
            );

        // Massage everything into SourceData.

        let to_retract_ok_source_data_stream =
            to_retract_ok_stream.map(|(x, ts, diff)| (SourceData(Ok(x)), ts, diff));

        let to_retract_err_source_data_stream =
            to_retract_err_stream.map(|(x, ts, diff)| (SourceData(Err(x)), ts, diff));

        let to_retract_stream =
            to_retract_ok_source_data_stream.concat(&to_retract_err_source_data_stream);

        let to_insert_ok_source_data_stream = sinked_collection
            .inner
            .map(|(x, ts, diff)| (SourceData(Ok(x)), ts, diff));

        let to_insert_err_source_data_stream = err_collection
            .inner
            .map(|(x, ts, diff)| (SourceData(Err(x)), ts, diff));

        let to_insert_stream =
            to_insert_ok_source_data_stream.concat(&to_insert_err_source_data_stream);

        let operator_name = format!("ContinualTaskInsert({})", sink_id);
        let mut task_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

        let hashed_id = sink_id.hashed();
        let active_worker = usize::cast_from(hashed_id) % scope.peers() == scope.index();

        let mut to_insert_input =
            task_op.new_disconnected_input(&to_insert_stream, Exchange::new(move |_| hashed_id));
        let mut to_retract_input =
            task_op.new_disconnected_input(&to_retract_stream, Exchange::new(move |_| hashed_id));

        let persist_clients = Arc::clone(&compute_state.persist_clients);
        let target_metadata = self.target_metadata.clone();
        let retract_from_metadata = self.retract_from_metadata.clone();

        let task_button = task_op.build(move |_capabilities| async move {
            if !active_worker {
                return;
            }

            let txns_id = target_metadata.txns_shard.expect("missing txns shard id");

            // Get ourselves a TxnsHandle
            let dummy_metrics_registry = MetricsRegistry::new();
            let txns_client = persist_clients
                .open(target_metadata.persist_location.clone())
                .await
                .expect("location should be valid");
            let txns_metrics = Arc::new(TxnMetrics::new(&dummy_metrics_registry));
            let mut txns: TxnsHandle<SourceData, (), Timestamp, i64, PersistEpoch, TxnsCodecRow> =
                TxnsHandle::open(
                    mz_repr::Timestamp::minimum(),
                    txns_client.clone(),
                    Arc::clone(&txns_metrics),
                    txns_id,
                    Arc::new(RelationDesc::empty()),
                    Arc::new(UnitSchema),
                )
                .await;
            // Keep track of any work we have to do.
            let mut tidy = Tidy::default();

            let diagnostics = Diagnostics {
                shard_name: target_metadata.data_shard.to_string(),
                handle_purpose: format!("continual task write for {}", sink_id),
            };

            let mut register_ts = 0.into();
            loop {
                let insert_write = txns_client
                    .open_writer(
                        target_metadata.data_shard,
                        Arc::new(target_metadata.relation_desc.clone()),
                        Arc::new(UnitSchema),
                        diagnostics.clone(),
                    )
                    .await
                    .expect("invalid persist usage");
                let retract_write = txns_client
                    .open_writer(
                        retract_from_metadata.data_shard,
                        Arc::new(retract_from_metadata.relation_desc.clone()),
                        Arc::new(UnitSchema),
                        diagnostics.clone(),
                    )
                    .await
                    .expect("invalid persist usage");
                let handles = vec![insert_write, retract_write];
                let res = txns.register(register_ts, handles).await;
                match res {
                    Ok(_) => {
                        tracing::info!("registered our table shards!");
                        break;
                    }
                    Err(ts) => {
                        tracing::error!(%ts, "could not register table shards!");
                        register_ts = ts;
                    }
                }
            }

            let mut to_insert_upper = Antichain::from_elem(mz_repr::Timestamp::minimum());
            let mut to_retract_upper = Antichain::from_elem(mz_repr::Timestamp::minimum());

            // Correction buffers for things that want to make it into the next
            // transaction.
            let mut to_insert_buffer = Vec::new();
            let mut to_retract_buffer = Vec::new();

            loop {
                tokio::select! {
                    Some(event) = to_insert_input.next() => {
                        match event {
                            Event::Data(_ts, mut data) => {
                                tracing::info!("to_insert: {:?}", data);
                                to_insert_buffer.append(&mut data);
                            }
                            Event::Progress(upper) => {
                                // Trying not to be cute with join_assign and
                                // the like, for now.
                                to_insert_upper = upper;
                                // tracing::info!("to_insert, upper: {:?}", upper);
                            }
                        }
                    }
                    Some(event) = to_retract_input.next() => {
                        match event {
                            Event::Data(_ts, data) => {
                                tracing::info!("to_retract: {:?}", data);
                                let mut retract_data = data.into_iter().map(|(update, ts, diff)| (update, ts, -diff)).collect_vec();
                                to_retract_buffer.append(&mut retract_data);
                            }
                            Event::Progress(upper) => {
                                // Trying not to be cute with join_assign and
                                // the like, for now.
                                to_retract_upper = upper;
                                // tracing::info!("to_retract, upper: {:?}", upper);
                            }
                        }
                    }
                    else => {
                        // All inputs are exhausted, so we can shut down.
                        return;
                    }
                };

                let mut upper = to_insert_upper.clone();
                upper.extend(to_retract_upper.iter().cloned());

                // Advance all timestamps in our buffers to the upper.
                for (_data, ts, _diff) in to_insert_buffer.iter_mut() {
                    ts.advance_by(upper.borrow());
                }
                for (_data, ts, _diff) in to_retract_buffer.iter_mut() {
                    ts.advance_by(upper.borrow());
                }

                differential_dataflow::consolidation::consolidate_updates(&mut to_insert_buffer);
                differential_dataflow::consolidation::consolidate_updates(&mut to_retract_buffer);

                tracing::info!(
                    ?upper,
                    ?to_insert_buffer,
                    ?to_retract_buffer,
                    "should transactionally append!"
                );

                let write_ts = upper.into_option().expect("must have single-element upper");

                // Can only start writing after the time at which our shards
                // have been registered.
                if !(write_ts > register_ts) {
                    continue;
                }

                if to_insert_buffer.is_empty() && to_retract_buffer.is_empty() {
                    continue;
                }

                let mut txn = txns.begin();
                for (update, _ts, diff) in to_insert_buffer.iter() {
                    txn.write(&target_metadata.data_shard, update.clone(), (), *diff)
                        .await;
                }
                for (update, _ts, diff) in to_retract_buffer.iter() {
                    txn.write(&retract_from_metadata.data_shard, update.clone(), (), *diff)
                        .await;
                }
                // Sneak in any txns shard tidying from previous commits.
                txn.tidy(std::mem::take(&mut tidy));

                let txn_res = txn.commit_at(&mut txns, write_ts.clone()).await;

                match txn_res {
                    Ok(apply) => {
                        tracing::info!("applying {:?}", apply);
                        let new_tidy = apply.apply(&mut txns).await;

                        tidy.merge(new_tidy);

                        // We don't serve any reads out of this TxnsHandle, so go ahead
                        // and compact as aggressively as we can (i.e. to the time we
                        // just wrote).
                        let () = txns.compact_to(write_ts).await;
                    }
                    Err(current) => {
                        tidy.merge(txn.take_tidy());
                        tracing::info!(
                            "unable to commit txn at {:?} current={:?}",
                            write_ts,
                            current
                        );
                        // We'll retry next time the uppers move.
                    }
                };
            }
        });

        let task_token = Rc::new(task_button.press_on_drop());
        Some(Rc::new((to_retract_token, task_token)))
    }
}
