// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! THROWAWAY spike: a storage sink that hands consolidated updates to a WASM
//! guest writing to Valkey. Enabled per clusterd process by the
//! `MZ_SPIKE_WASM_SINK` environment variable (JSON), which replaces the render
//! of every Kafka sink in that process. Never merge.
//!
//! Progress protocol, identical to the Kafka sink: the guest commits frontier
//! `f` in the target, then this operator advances the sink's own persist shard
//! to `f` and publishes `f` as the sink's write frontier. The controller holds
//! the sink input at `f - 1`, and a restart derives its as-of from the shard.

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use differential_dataflow::Hashable;
use futures::StreamExt;
use differential_dataflow::VecCollection;
use mz_interchange::envelopes::for_each_diff_pair;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{KafkaSinkConnection, StorageSinkDesc};
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{
    Event, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_wasm_sink_spike::encode::Encoder;
use mz_wasm_sink_spike::guest::{self, GuestThread};
use timely::PartialOrder;
use timely::dataflow::StreamVec;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::vec::Map;
use timely::progress::Antichain;
use tracing::info;

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::{SinkBatchStream, SinkRender};
use crate::storage_state::StorageState;

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct SpikeConfig {
    wasm: String,
    valkey: SocketAddr,
    #[serde(default = "default_deadline")]
    call_deadline_ms: u64,
    #[serde(default)]
    fuel_base: u64,
    #[serde(default)]
    fuel_per_byte: u64,
    #[serde(default = "default_memory")]
    memory_bytes: usize,
    #[serde(default = "default_chunk")]
    chunk_rows: usize,
}

fn default_deadline() -> u64 {
    30_000
}
fn default_memory() -> usize {
    64 << 20
}
fn default_chunk() -> usize {
    50_000
}

/// Returns the spike config if `MZ_SPIKE_WASM_SINK` is set.
pub(crate) fn spike_config() -> Option<SpikeConfig> {
    let s = std::env::var("MZ_SPIKE_WASM_SINK").ok()?;
    Some(serde_json::from_str(&s).expect("valid MZ_SPIKE_WASM_SINK"))
}

pub(crate) struct WasmSinkRender {
    pub(crate) kafka: KafkaSinkConnection,
    pub(crate) cfg: SpikeConfig,
}

impl<'scope> SinkRender<'scope> for WasmSinkRender {
    fn get_key_indices(&self) -> Option<&[usize]> {
        self.kafka
            .key_desc_and_indices
            .as_ref()
            .map(|(_desc, indices)| indices.as_slice())
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        self.kafka.relation_key_indices.as_deref()
    }

    fn render_sink(
        &self,
        storage_state: &mut StorageState,
        sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
        sink_id: GlobalId,
        batches: SinkBatchStream<'scope>,
        key_is_synthetic: bool,
        _err_collection: VecCollection<'scope, Timestamp, DataflowError, Diff>,
    ) -> (
        StreamVec<'scope, Timestamp, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    ) {
        let scope = batches.scope();
        let cfg = self.cfg.clone();

        let write_handle = {
            let persist = Arc::clone(&storage_state.persist_clients);
            let shard_meta = sink.to_storage_metadata.clone();
            async move {
                let client = persist.open(shard_meta.persist_location).await?;
                let handle = client
                    .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                        shard_meta.data_shard,
                        Arc::new(shard_meta.relation_desc),
                        Arc::new(UnitSchema),
                        Diagnostics::from_purpose("wasm sink handle"),
                    )
                    .await?;
                Ok::<_, anyhow::Error>(handle)
            }
        };

        let write_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::MIN)));
        storage_state
            .sink_write_frontiers
            .insert(sink_id, Rc::clone(&write_frontier));

        let name = format!("wasm-{sink_id}");
        let mut builder = AsyncOperatorBuilder::new(name.clone(), scope.clone());
        let hashed_id = sink_id.hashed();
        let is_active_worker =
            usize::try_from(hashed_id % u64::try_from(scope.peers()).unwrap()).unwrap()
                == scope.index();
        // Each worker flattens its share of the arranged batches into diff
        // pairs, keyed updates stay in time order per key, and one worker
        // talks to the guest.
        let pairs = batches.flat_map(|batch| {
            let mut v = Vec::new();
            for_each_diff_pair(&batch, |_key, time, pair| {
                v.push(((pair.before, pair.after), time));
            });
            v
        });
        let mut input = builder.new_disconnected_input(pairs, Exchange::new(move |_| hashed_id));

        let desc = sink.from_desc.clone();
        let as_of = sink.as_of.clone();
        let with_snapshot = sink.with_snapshot;

        let (button, errors) = builder.build_fallible(move |_caps| {
            Box::pin(async move {
                if !is_active_worker {
                    write_frontier.borrow_mut().clear();
                    return Ok(());
                }
                if key_is_synthetic {
                    bail!("wasm sink needs KEY (...) so updates of one key share a group");
                }
                let mut write_handle = write_handle.await?;

                let engine = guest::engine(cfg.fuel_base > 0)?;
                let component = guest::load_component(&engine, cfg.wasm.as_ref())?;
                let allowed = cfg.valkey;
                let limits = guest::WasmLimits {
                    memory_bytes: cfg.memory_bytes,
                    call_ticks: cfg.call_deadline_ms * guest::TICKS_PER_SEC / 1000,
                    fuel: (cfg.fuel_base > 0).then_some((cfg.fuel_base, cfg.fuel_per_byte)),
                };
                let guest = GuestThread::spawn(
                    move || {
                        Ok(
                            Box::new(guest::Wasm::new(&engine, &component, allowed, limits)?)
                                as Box<dyn guest::Guest>,
                        )
                    },
                    Duration::from_millis(cfg.call_deadline_ms),
                );
                let config = serde_json::json!({
                    "addr": cfg.valkey.to_string(),
                    "prefix": format!("{{{sink_id}}}"),
                });
                let resume = guest.open(config.to_string()).await?;
                let as_of_t = as_of.as_option().copied().unwrap_or(Timestamp::MAX);
                info!(%sink_id, ?resume, ?as_of, with_snapshot, "wasm sink: opened");

                // The target has applied every update at a time < `resume`.
                let resume = match resume {
                    None => {
                        if !with_snapshot {
                            bail!("target has no progress and the sink starts without snapshot");
                        }
                        guest.reset().await?;
                        None
                    }
                    Some(r) => {
                        let r = Timestamp::from(r);
                        // Without a snapshot the input supplies only times > as_of.
                        if !with_snapshot && r <= as_of_t {
                            bail!("gap: target progress {r} <= as_of {as_of_t} without snapshot");
                        }
                        Some(r)
                    }
                };

                let encoder = Encoder::new(&desc);
                let mut rows: Vec<(Row, Timestamp, i64)> = Vec::new();
                let mut committed = as_of.clone();
                while let Some(event) = input.next().await {
                    match event {
                        Event::Data(_cap, mut data) => {
                            for ((before, after), time) in data.drain(..) {
                                if resume.is_some_and(|r| time < r) {
                                    continue;
                                }
                                if let Some(b) = before {
                                    rows.push((b, time, -1));
                                }
                                if let Some(a) = after {
                                    rows.push((a, time, 1));
                                }
                            }
                            if rows.len() >= cfg.chunk_rows {
                                guest.write(encoder.encode(&rows)?, None).await?;
                                rows.clear();
                            }
                        }
                        Event::Progress(upper) => {
                            if !PartialOrder::less_than(&committed, &upper) {
                                continue;
                            }
                            let Some(commit) = upper.as_option().map(|t| u64::from(*t)) else {
                                break;
                            };
                            let bytes = if rows.is_empty() {
                                Vec::new()
                            } else {
                                encoder.encode(&rows)?
                            };
                            guest.write(bytes, Some(commit)).await?;
                            rows.clear();

                            // Record progress only after the target committed it.
                            let mut expect = write_handle.shared_upper();
                            loop {
                                if PartialOrder::less_equal(&upper, &expect) {
                                    break;
                                }
                                const EMPTY: &[((SourceData, ()), Timestamp, StorageDiff)] = &[];
                                match write_handle
                                    .compare_and_append(EMPTY, expect, upper.clone())
                                    .await
                                    .map_err(|e| anyhow!("invalid usage: {e:?}"))?
                                {
                                    Ok(()) => break,
                                    Err(mismatch) => expect = mismatch.current,
                                }
                            }
                            write_frontier.borrow_mut().clone_from(&upper);
                            committed = upper;
                        }
                    }
                }
                Ok(())
            })
        });

        let statuses = errors.map(|error: Rc<anyhow::Error>| HealthStatusMessage {
            id: None,
            update: HealthStatusUpdate::halting(format!("{error:#}"), None),
            namespace: StatusNamespace::Kafka,
        });
        (statuses, vec![button.press_on_drop()])
    }
}
