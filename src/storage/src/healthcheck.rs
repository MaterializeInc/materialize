// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Healthcheck common

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Hashable;
use mz_ore::cast::CastFrom;
use mz_ore::now::NowFn;
use mz_persist_client::PersistLocation;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use mz_storage_client::healthcheck::MZ_SOURCE_STATUS_HISTORY_DESC;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Enter, Map};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tracing::{info, trace, warn};

use crate::internal_control::InternalStorageCommand;
use crate::render::sources::OutputIndex;
use crate::source::types::{HealthStatus, HealthStatusUpdate};

pub async fn write_to_persist(
    collection_id: GlobalId,
    new_status: &str,
    new_error: Option<&str>,
    now: NowFn,
    client: &PersistClient,
    status_shard: ShardId,
    relation_desc: &RelationDesc,
    hint: Option<&str>,
) {
    let now_ms = now();
    let row = mz_storage_client::healthcheck::pack_status_row(
        collection_id,
        new_status,
        new_error,
        now_ms,
        hint,
    );

    let mut handle = client
        .open_writer(
            status_shard,
            Arc::new(relation_desc.clone()),
            Arc::new(UnitSchema),
            Diagnostics {
                // TODO: plumb through the GlobalId of the status collection
                shard_name: format!("status({})", collection_id),
                handle_purpose: format!("healthcheck::write_to_persist {}", collection_id),
            },
        )
        .await
        .expect(
            "Invalid usage of the persist \
            client for collection {collection_id} status history shard",
        );

    let mut recent_upper = handle.upper().clone();
    let mut append_ts = Timestamp::from(now_ms);
    'retry_loop: loop {
        // We don't actually care so much about the timestamp we append at; it's best-effort.
        // Ensure that the append timestamp is not less than the current upper, and the new upper
        // is past that timestamp. (Unless we've advanced to the empty antichain, but in
        // that case we're already in trouble.)
        for t in recent_upper.elements() {
            append_ts.join_assign(t);
        }
        let mut new_upper = Antichain::from_elem(append_ts.step_forward());
        new_upper.join_assign(&recent_upper);

        let updates = vec![((SourceData(Ok(row.clone())), ()), append_ts, 1i64)];
        let cas_result = handle
            .compare_and_append(updates, recent_upper.clone(), new_upper)
            .await;
        match cas_result {
            Ok(Ok(())) => break 'retry_loop,
            Ok(Err(mismatch)) => {
                recent_upper = mismatch.current;
            }
            Err(e) => {
                panic!("Invalid usage of the persist client for collection {collection_id} status history shard: {e:?}");
            }
        }
    }

    handle.expire().await
}

/// How long to wait before initiating a `SuspendAndRestart` command, to
/// prevent hot restart loops.
const SUSPEND_AND_RESTART_DELAY: Duration = Duration::from_secs(30);


struct HealthState<'a> {
    source_id: GlobalId,
    persist_details: Option<(ShardId, &'a PersistClient)>,
    healths: Vec<Option<HealthStatus>>,
    last_reported_status: Option<HealthStatus>,
    halt_with: Option<HealthStatus>,
}

impl<'a> HealthState<'a> {
    fn new(
        source_id: GlobalId,
        metadata: CollectionMetadata,
        persist_clients: &'a BTreeMap<PersistLocation, PersistClient>,
        worker_count: usize,
    ) -> HealthState<'a> {
        let persist_details = match (
            metadata.status_shard,
            persist_clients.get(&metadata.persist_location),
        ) {
            (Some(shard), Some(persist_client)) => Some((shard, persist_client)),
            _ => None,
        };

        HealthState {
            source_id,
            persist_details,
            healths: vec![None; worker_count],
            last_reported_status: None,
            halt_with: None,
        }
    }
}

/// Writes updates that come across `health_stream` to the collection's status shards, as identified
/// by their `CollectionMetadata`.
///
/// Only one worker will be active and write to the status shard.
///
/// The `OutputIndex` values that come across `health_stream` must be a strict subset of those in
/// `configs`'s keys.
pub(crate) fn health_operator<'g, G: Scope<Timestamp = ()>>(
    scope: &mut Child<'g, G, mz_repr::Timestamp>,
    storage_state: &crate::storage_state::StorageState,
    resume_uppers: BTreeMap<GlobalId, Antichain<mz_repr::Timestamp>>,
    primary_source_id: GlobalId,
    health_stream: &Stream<G, (OutputIndex, HealthStatusUpdate)>,
    configs: BTreeMap<OutputIndex, (GlobalId, CollectionMetadata)>,
) -> Rc<dyn Any> {
    // Derived config options
    let healthcheck_worker_id = scope.index();
    let worker_count = scope.peers();
    let now = storage_state.now.clone();
    let persist_clients = Arc::clone(&storage_state.persist_clients);
    let internal_cmd_tx = Rc::clone(&storage_state.internal_cmd_tx);

    // Inject the originating worker id to each item before exchanging to the chosen worker
    let health_stream = health_stream.map(move |status| (healthcheck_worker_id, status));

    // We'll route all the work to a single arbitrary worker;
    // there's not much to do, and we need a global view.
    let chosen_worker_id = usize::cast_from(configs.keys().next().hashed()) % worker_count;
    let is_active_worker = chosen_worker_id == healthcheck_worker_id;

    let operator_name = format!("healthcheck({})", healthcheck_worker_id);
    let mut health_op = AsyncOperatorBuilder::new(operator_name, scope.clone());

    let health = health_stream.enter(&scope);

    let mut input = health_op.new_input(
        &health,
        Exchange::new(move |_| u64::cast_from(chosen_worker_id)),
    );

    // Construct a minimal number of persist clients
    let persist_locations: BTreeSet<_> = configs
        .values()
        .filter_map(|(source_id, metadata)| {
            if is_active_worker {
                match &metadata.status_shard {
                    Some(status_shard) => {
                        info!("Health for source {source_id} being written to {status_shard}");
                        Some(metadata.persist_location.clone())
                    }
                    None => {
                        trace!("Health for source {source_id} not being written to status shard");
                        None
                    }
                }
            } else {
                None
            }
        })
        .collect();

    let button = health_op.build(move |mut _capabilities| async move {
        // Convert the persist locations into persist clients
        let mut persist_clients_per_location = BTreeMap::new();
        for persist_location in persist_locations {
            persist_clients_per_location.insert(
                persist_location.clone(),
                persist_clients
                    .open(persist_location)
                    .await
                    .expect("error creating persist client for Healthchecker"),
            );
        }

        let mut health_states: BTreeMap<_, _> = configs
            .into_iter()
            .map(|(output_idx, (id, metadata))| {
                (
                    output_idx,
                    HealthState::new(id, metadata, &persist_clients_per_location, worker_count),
                )
            })
            .collect();

        // Write the initial starting state to the status shard for all managed sources
        if is_active_worker {
            for state in health_states.values() {
                if !resume_uppers[&state.source_id].is_empty() {
                    if let Some((status_shard, persist_client)) = state.persist_details {
                        let status = HealthStatus::Starting;
                        write_to_persist(
                            state.source_id,
                            status.name(),
                            status.error(),
                            now.clone(),
                            persist_client,
                            status_shard,
                            &*MZ_SOURCE_STATUS_HISTORY_DESC,
                            status.hint(),
                        )
                        .await;
                    }
                }
            }
        }

        let mut outputs_seen = BTreeSet::new();
        while let Some(event) = input.next_mut().await {
            if let AsyncEvent::Data(_cap, rows) = event {
                for (worker_id, (output_index, health_event)) in rows.drain(..) {
                    let HealthState {
                        source_id,
                        healths,
                        halt_with,
                        ..
                    } = match health_states.get_mut(&output_index) {
                        Some(health) => health,
                        // This is a health status update for a subsource that we did not request to
                        // be generated, which means it doesn't have a GlobalId and should not be
                        // propagated to the shard.
                        None => continue,
                    };

                    let new_round = outputs_seen.insert(output_index);

                    if !is_active_worker {
                        warn!(
                            "Health messages for source {source_id} passed to \
                              an unexpected worker id: {healthcheck_worker_id}"
                        )
                    }

                    let HealthStatusUpdate {
                        update,
                        should_halt,
                    } = health_event;

                    if should_halt {
                        *halt_with = Some(update.clone());
                    }

                    let update = Some(update);
                    // Keep the max of the messages in each round; this ensures that errors don't
                    // get lost while also letting us frequently update to the newest status.
                    if new_round || &healths[worker_id] < &update {
                        healths[worker_id] = update;
                    }
                }

                let mut halt_with_outer = None;

                while let Some(output_index) = outputs_seen.pop_first() {
                    let HealthState {
                        source_id,
                        healths,
                        persist_details,
                        last_reported_status,
                        halt_with,
                    } = health_states
                        .get_mut(&output_index)
                        .expect("known to exist");

                    let overall_status = healths.iter().filter_map(Option::as_ref).max();

                    if let Some(new_status) = overall_status {
                        if last_reported_status.as_ref() != Some(&new_status) {
                            info!(
                                "Health transition for source {source_id}: \
                                  {last_reported_status:?} -> {new_status:?}"
                            );
                            if let Some((status_shard, persist_client)) = persist_details {
                                write_to_persist(
                                    *source_id,
                                    new_status.name(),
                                    new_status.error(),
                                    now.clone(),
                                    persist_client,
                                    *status_shard,
                                    &*MZ_SOURCE_STATUS_HISTORY_DESC,
                                    new_status.hint(),
                                )
                                .await;
                            }

                            *last_reported_status = Some(new_status.clone());
                        }
                    }

                    // Set halt with if None.
                    if halt_with_outer.is_none() && halt_with.is_some() {
                        halt_with_outer = Some((*source_id, halt_with.clone()));
                    }
                }

                // TODO(aljoscha): Instead of threading through the
                // `should_halt` bit, we can give an internal command sender
                // directly to the places where `should_halt = true` originates.
                // We should definitely do that, but this is okay for a PoC.
                if let Some((id, halt_with)) = halt_with_outer {
                    mz_ore::soft_assert!(
                        id == primary_source_id,
                        "subsources should not produce halting errors, however {:?} halted while primary \
                                            source is {:?}",
                        id,
                        primary_source_id
                    );

                    info!(
                        "Broadcasting suspend-and-restart command because of {:?} after {:?} delay",
                        halt_with, SUSPEND_AND_RESTART_DELAY
                    );
                    tokio::time::sleep(SUSPEND_AND_RESTART_DELAY).await;
                    internal_cmd_tx.borrow_mut().broadcast(
                        InternalStorageCommand::SuspendAndRestart {
                            // Suspend and restart is expected to operate on the primary source and
                            // not any of the subsources.
                            id,
                            reason: format!("{:?}", halt_with),
                        },
                    );
                }
            }
        }
    });

    Rc::new(button.press_on_drop())
}
