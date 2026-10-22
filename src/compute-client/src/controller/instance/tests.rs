// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_compute_types::dyncfgs::{ENABLE_COLUMN_PAGED_BATCHER, ENABLE_MZ_JOIN_CORE};
use mz_dyncfg::{ConfigSet, ConfigUpdates, ConfigVal};
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{GlobalId, RelationDesc, Timestamp};
use timely::progress::Antichain;
use tokio::sync::mpsc;

use crate::protocol::command::{ComputeCommand, InstanceConfig};
use crate::protocol::response::{PeekResponse, StashedPeekResponse};
use crate::stash::tests::CountedClient;

use super::{
    CollectionReadiness, Instance, ReplicaCollectionIntrospection, ReplicaCollectionState,
    ReplicaId, classify_collection_readiness, discard_peek_response,
};

fn ac(ts: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::new(ts))
}

fn state(id: ReplicaId, as_of: u64, write: u64, output: u64) -> ReplicaCollectionState {
    let (tx, _rx) = mpsc::unbounded_channel();
    let introspection = ReplicaCollectionIntrospection::new(id, GlobalId::User(1), tx, ac(as_of));
    let mut state = ReplicaCollectionState::new(None, ac(as_of), introspection, Vec::new());
    state.update_write_frontier(ac(write));
    state.update_output_frontier(ac(output));
    state
}

fn classify(
    replicas: &[(ReplicaId, ReplicaCollectionState)],
    targets: &[ReplicaId],
    references: &[ReplicaId],
    lag: Option<Timestamp>,
) -> CollectionReadiness {
    classify_collection_readiness(
        replicas.iter().map(|(id, state)| (*id, state)),
        &targets.iter().copied().collect::<BTreeSet<_>>(),
        &references.iter().copied().collect::<BTreeSet<_>>(),
        lag,
    )
}

const LAG: Option<Timestamp> = Some(Timestamp::new(60));

/// The regression the lag gate exists for: a pending replica whose dataflow
/// has produced its first output past the as-of, and so reports hydrated,
/// while its output frontier is still far behind the outgoing replica.
#[mz_ore::test]
fn shared_write_does_not_hide_output_lag() {
    let reference = ReplicaId::User(1);
    let target = ReplicaId::User(2);
    let mut replicas = vec![
        (reference, state(reference, 100, 10_000, 10_000)),
        // The shared MV write frontier is caught up, but this replica has
        // only just produced its first output past the as-of.
        (target, state(target, 100, 10_000, 101)),
    ];
    assert_eq!(
        classify(&replicas, &[target], &[reference], LAG),
        CollectionReadiness::Lagging { lag: Some(9_899) },
    );
    replicas[1].1.update_output_frontier(ac(9_940));
    assert_eq!(
        classify(&replicas, &[target], &[reference], LAG),
        CollectionReadiness::Ready,
    );
}

#[mz_ore::test]
fn refresh_writes_ahead_of_outputs_is_ready() {
    let reference = ReplicaId::User(1);
    let target = ReplicaId::User(2);
    let replicas = vec![
        (reference, state(reference, 100, 20_000, 10_000)),
        (target, state(target, 100, 20_000, 10_000)),
    ];
    assert_eq!(
        classify(&replicas, &[target], &[reference], LAG),
        CollectionReadiness::Ready,
    );
}

#[mz_ore::test]
fn furthest_reference_and_bystander_exclusion() {
    let trailing = ReplicaId::User(1);
    let ahead = ReplicaId::User(2);
    let target = ReplicaId::User(3);
    let bystander = ReplicaId::User(4);
    let replicas = vec![
        (trailing, state(trailing, 100, 9_000, 9_000)),
        (ahead, state(ahead, 100, 10_000, 10_000)),
        (target, state(target, 100, 8_990, 8_990)),
        (bystander, state(bystander, 100, 1_000_000, 1_000_000)),
    ];
    assert_eq!(
        classify(&replicas, &[target], &[trailing], LAG),
        CollectionReadiness::Ready,
    );
    assert_eq!(
        classify(&replicas, &[target], &[trailing, ahead], LAG),
        CollectionReadiness::Lagging { lag: Some(1_010) },
    );
}

#[mz_ore::test]
fn one_ready_target_suffices() {
    let reference = ReplicaId::User(1);
    let unhydrated = ReplicaId::User(2);
    let lagging = ReplicaId::User(3);
    let ready = ReplicaId::User(4);
    let replicas = vec![
        (reference, state(reference, 100, 10_000, 10_000)),
        // This replica is within the lag allowance but has not advanced
        // past its own as-of. It cannot supply progress for the hydrated,
        // lagging replica below.
        (unhydrated, state(unhydrated, 10_000, 10_000, 10_000)),
        (lagging, state(lagging, 100, 1_000, 1_000)),
        (ready, state(ready, 100, 9_990, 9_990)),
    ];
    assert_eq!(
        classify(&replicas, &[unhydrated, lagging, ready], &[reference], LAG),
        CollectionReadiness::Ready,
    );
    assert_eq!(
        classify(&replicas, &[unhydrated, lagging], &[reference], LAG),
        CollectionReadiness::Lagging { lag: Some(9_000) },
    );
}

#[mz_ore::test]
fn no_lag_gate_is_hydration_only() {
    let reference = ReplicaId::User(1);
    let target = ReplicaId::User(2);
    let replicas = vec![
        (reference, state(reference, 100, 10_000, 10_000)),
        (target, state(target, 100, 101, 101)),
    ];
    assert_eq!(
        classify(&replicas, &[target], &[reference], None),
        CollectionReadiness::Ready,
    );
    let unhydrated = vec![(target, state(target, 100, 100, 100))];
    assert_eq!(
        classify(&unhydrated, &[target], &[], None),
        CollectionReadiness::Unhydrated,
    );
}

#[mz_ore::test]
fn lag_reports_the_closest_hydrated_target() {
    let reference = ReplicaId::User(1);
    let far = ReplicaId::User(2);
    let close = ReplicaId::User(3);
    let mut replicas = vec![
        (reference, state(reference, 100, 10_000, 10_000)),
        (far, state(far, 100, 1_000, 1_000)),
        (close, state(close, 100, 9_000, 9_000)),
    ];
    for _ in 0..2 {
        assert_eq!(
            classify(&replicas, &[far, close], &[reference], LAG),
            CollectionReadiness::Lagging { lag: Some(1_000) },
        );
        replicas.reverse();
    }
}

#[mz_ore::test]
fn completed_reference_requires_completed_target() {
    let reference = ReplicaId::User(1);
    let target = ReplicaId::User(2);
    let mut replicas = vec![
        (reference, state(reference, 100, 1_000, 1_000)),
        (target, state(target, 100, 1_000, 1_000)),
    ];
    replicas[0].1.update_output_frontier(Antichain::new());
    assert_eq!(
        classify(&replicas, &[target], &[reference], Some(Timestamp::new(0))),
        CollectionReadiness::Lagging { lag: None },
    );
    replicas[1].1.update_output_frontier(Antichain::new());
    assert_eq!(
        classify(&replicas, &[target], &[reference], Some(Timestamp::new(0))),
        CollectionReadiness::Ready,
    );
}

#[mz_ore::test]
fn no_reference_means_nothing_to_regress() {
    let target = ReplicaId::User(1);
    let replicas = vec![(target, state(target, 100, 5_000, 5_000))];
    assert_eq!(
        classify(&replicas, &[target], &[], Some(Timestamp::new(0))),
        CollectionReadiness::Ready,
    );
    assert_eq!(
        classify(&replicas, &[target], &[target], Some(Timestamp::new(0))),
        CollectionReadiness::Ready,
    );
}

#[mz_ore::test]
fn no_targets_is_unhydrated() {
    let reference = ReplicaId::User(1);
    let replicas = vec![(reference, state(reference, 100, 10_000, 10_000))];
    assert_eq!(
        classify(&replicas, &[], &[reference], LAG),
        CollectionReadiness::Unhydrated,
    );
}

fn create_instance_command() -> ComputeCommand {
    ComputeCommand::CreateInstance(Box::new(InstanceConfig {
        logging: Default::default(),
        expiration_offset: None,
        peek_stash_persist_location: PersistLocation::new_in_mem(),
        arrangement_dictionary_compression: false,
        initial_config: Default::default(),
    }))
}

fn initial_config(cmd: &ComputeCommand) -> &ConfigUpdates {
    match cmd {
        ComputeCommand::CreateInstance(config) => &config.initial_config,
        other => panic!("expected CreateInstance, got {other:?}"),
    }
}

/// `CreateInstance` is specialized with a full snapshot of the instance-wide dyncfg, so the
/// replica seeds its worker config at create time rather than waiting for the first
/// `UpdateConfiguration`. This is the regression guard for create-time setup observing dyncfg
/// defaults.
#[mz_ore::test]
fn create_instance_snapshots_instance_wide_dyncfg() {
    let dyncfg = ConfigSet::default()
        .add(&ENABLE_COLUMN_PAGED_BATCHER)
        .add(&ENABLE_MZ_JOIN_CORE);
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_COLUMN_PAGED_BATCHER, true);
    updates.add(&ENABLE_MZ_JOIN_CORE, false);
    updates.apply(&dyncfg);

    // A replica without an override sees exactly the instance-wide values.
    let cmd = Instance::specialize_command_for_replica(
        create_instance_command(),
        &ConfigUpdates::default(),
        &dyncfg,
    );
    let snapshot = initial_config(&cmd);
    assert_eq!(
        snapshot.updates.get(ENABLE_COLUMN_PAGED_BATCHER.name()),
        Some(&ConfigVal::Bool(true)),
    );
    assert_eq!(
        snapshot.updates.get(ENABLE_MZ_JOIN_CORE.name()),
        Some(&ConfigVal::Bool(false)),
    );
}

/// A replica-scoped override beats the instance-wide value in the create-time snapshot, so a
/// create-time-frozen scoped flag reaches the replica with its override applied.
#[mz_ore::test]
fn create_instance_snapshot_applies_replica_override() {
    let dyncfg = ConfigSet::default().add(&ENABLE_COLUMN_PAGED_BATCHER);
    let mut updates = ConfigUpdates::default();
    updates.add(&ENABLE_COLUMN_PAGED_BATCHER, true);
    updates.apply(&dyncfg);

    let mut override_updates = ConfigUpdates::default();
    override_updates.add(&ENABLE_COLUMN_PAGED_BATCHER, false);

    let cmd = Instance::specialize_command_for_replica(
        create_instance_command(),
        &override_updates,
        &dyncfg,
    );
    assert_eq!(
        initial_config(&cmd)
            .updates
            .get(ENABLE_COLUMN_PAGED_BATCHER.name()),
        Some(&ConfigVal::Bool(false)),
        "replica override should win over the instance-wide value",
    );
}

/// `UpdateConfiguration` continues to merge the replica's override into the update.
#[mz_ore::test]
fn update_configuration_merges_replica_override() {
    let dyncfg = ConfigSet::default().add(&ENABLE_COLUMN_PAGED_BATCHER);

    let mut override_updates = ConfigUpdates::default();
    override_updates.add(&ENABLE_COLUMN_PAGED_BATCHER, true);

    let cmd = Instance::specialize_command_for_replica(
        ComputeCommand::UpdateConfiguration(Box::new(Default::default())),
        &override_updates,
        &dyncfg,
    );
    match cmd {
        ComputeCommand::UpdateConfiguration(params) => assert_eq!(
            params
                .dyncfg_updates
                .updates
                .get(ENABLE_COLUMN_PAGED_BATCHER.name()),
            Some(&ConfigVal::Bool(true)),
        ),
        other => panic!("expected UpdateConfiguration, got {other:?}"),
    }
}

#[mz_ore::test(tokio::test)]
async fn discarding_a_stashed_peek_response_deletes_its_batches() {
    let persist = CountedClient::new().await;
    let shard_id = ShardId::new();
    let batches = vec![
        persist.write_batch(shard_id).await,
        persist.write_batch(shard_id).await,
    ];
    assert!(persist.succeeded("blob_set") > 0);

    let response = PeekResponse::Stashed(Box::new(StashedPeekResponse {
        num_rows_batches: 8,
        encoded_size_bytes: 0,
        relation_desc: RelationDesc::empty(),
        shard_id,
        batches,
        inline_rows: Vec::new(),
    }));
    discard_peek_response(Some(&persist.client), response);

    persist.wait_until_nothing_is_left().await;
}
