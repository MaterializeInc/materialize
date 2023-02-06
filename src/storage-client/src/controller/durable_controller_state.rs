// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstraction for reading and updating durable storage controller state
//! to/from a log (a persist shard, in our case).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_ore::now::EpochMillis;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::read::Listen;
use mz_persist_types::Codec64;
use mz_repr::TimestampManipulation;
use serde::Deserialize;
use serde::Serialize;

use mz_persist_client::read::ListenEvent;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::PersistClient;
use mz_persist_client::ShardId;
use mz_persist_types::codec_impls::TodoSchema;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::Codec;
use mz_repr::GlobalId;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp;

use crate::types::sinks::SinkAsOf;

#[derive(Debug)]
pub struct DurableControllerState<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    // TODO(aljoscha): We should keep these collections as a _trace_, with the
    // full diffs. And only collapse them down to a map when needed. And also
    // ensure that there is only one entry for a given `GlobalId` then.
    data_shards: BTreeMap<GlobalId, ShardId>,
    remap_shards: BTreeMap<GlobalId, ShardId>,
    export_as_ofs: BTreeMap<GlobalId, (T, bool)>,
    shard_finalization_wal: BTreeSet<ShardId>,

    trace: Vec<(StateUpdate<T>, u64, i64)>,

    upper: u64,
    listen: Listen<StateUpdate<T>, (), u64, i64>,
    write_handle: WriteHandle<StateUpdate<T>, (), u64, i64>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StateUpdate<T> {
    DataShard(GlobalId, ShardId),
    RemapShard(GlobalId, ShardId),
    ExportAsOf(GlobalId, (T, bool)),
    FinalizedShard(ShardId),
}

impl<T> Codec for StateUpdate<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    type Schema = TodoSchema<StateUpdate<T>>;

    fn codec_name() -> String {
        "StateUpdateSerde".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        let bytes = serde_json::to_vec(&self).expect("failed to encode StateUpdate");
        buf.put(bytes.as_slice());
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        serde_json::from_slice(buf).map_err(|err| err.to_string())
    }
}

impl<T> DurableControllerState<T>
where
    T: Timestamp + Lattice + TotalOrder + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    pub async fn new(
        shard_id: ShardId,
        persist_client: PersistClient,
    ) -> DurableControllerState<T> {
        let purpose = "DurableControllerState".to_string();
        let (mut write_handle, mut read_handle) = persist_client
            .open(
                shard_id,
                &purpose,
                Arc::new(TodoSchema::default()),
                Arc::new(UnitSchema),
            )
            .await
            .expect("invalid usage");

        let since = read_handle.since().clone();
        let upper: u64 = write_handle
            .upper()
            .as_option()
            .cloned()
            .expect("we use a totally ordered time and never finalize the shard");

        let upper = if upper == 0 {
            // Get around the fact that we can't get a snapshot and listen when
            // the upper is 0.
            //
            // And yes, we should probably not do this in a production impl.
            // const EMPTY_UPDATES: &[((StateUpdate<T>, ()), u64, i64)] = &[];
            let batch_builder = write_handle.builder(Antichain::from_elem(0));
            let mut batch = batch_builder
                .finish(Antichain::from_elem(1))
                .await
                .expect("invalid usage");

            let _res = write_handle
                .compare_and_append_batch(
                    &mut [&mut batch],
                    Antichain::from_elem(0),
                    Antichain::from_elem(1),
                )
                .await
                .expect("unvalid usage");
            1
        } else {
            upper
        };

        let mut restart_as_of = upper.saturating_sub(1);
        restart_as_of.advance_by(since.borrow());

        // Always true, currently!
        let initial_snapshot = if restart_as_of > 0 {
            read_handle
                .snapshot_and_fetch(Antichain::from_elem(restart_as_of))
                .await
                .expect("we have advanced the restart_as_of by the since")
        } else {
            Vec::new()
        };

        let initial_snapshot = initial_snapshot
            .into_iter()
            .map(|((key, _unit), ts, diff)| (key.expect("key decoding error"), ts, diff))
            .collect_vec();

        println!("initial snapshot: {:?}", initial_snapshot);

        println!("creating listen at {:?}", restart_as_of);
        let listen = read_handle
            .listen(Antichain::from_elem(restart_as_of))
            .await
            .expect("invalid usage");

        let mut this = Self {
            data_shards: BTreeMap::new(),
            remap_shards: BTreeMap::new(),
            export_as_ofs: BTreeMap::new(),
            shard_finalization_wal: BTreeSet::new(),
            trace: Vec::new(),
            upper: restart_as_of,
            listen,
            write_handle,
        };

        this.apply_updates(initial_snapshot);

        println!("syncing to {:?}", upper);
        this.sync(upper).await;

        this
    }

    pub fn get_data_shard(&self, global_id: &GlobalId) -> Option<ShardId> {
        self.data_shards.get(global_id).cloned()
    }

    pub fn get_all_data_shards(&self) -> Vec<(GlobalId, ShardId)> {
        self.data_shards
            .iter()
            .map(|(id, shard)| (id.clone(), shard.clone()))
            .collect_vec()
    }

    pub fn get_remap_shard(&self, global_id: &GlobalId) -> Option<ShardId> {
        self.remap_shards.get(global_id).cloned()
    }

    pub fn get_all_remap_shards(&self) -> Vec<(GlobalId, ShardId)> {
        self.remap_shards
            .iter()
            .map(|(id, shard)| (id.clone(), shard.clone()))
            .collect_vec()
    }

    pub fn get_export_as_of(&self, global_id: &GlobalId) -> Option<SinkAsOf<T>> {
        self.export_as_ofs
            .get(global_id)
            .map(|(as_of, strict)| SinkAsOf {
                frontier: Antichain::from_elem(as_of.clone()),
                strict: *strict,
            })
    }

    pub fn is_in_finalization_wal(&self, shard_id: &ShardId) -> bool {
        self.shard_finalization_wal.contains(shard_id)
    }

    pub fn get_finalization_wal(&self) -> Vec<ShardId> {
        self.shard_finalization_wal.iter().cloned().collect_vec()
    }

    // TODO(aljoscha): These `prepare_*` updates are very inefficient. Also, we
    // can probably factor out quite a bit of common functionality.
    //
    // Also, the API for these is currently a big footgun! If we do two
    // `prepare_*` calls for the same global ID, both will emit updates because
    // we don't "remember" an update that we already emitted.

    pub fn prepare_upsert_data_shard(
        &self,
        global_id: GlobalId,
        shard_id: ShardId,
    ) -> Vec<(StateUpdate<T>, i64)> {
        let mut updates = Vec::new();

        let current_shard_id = self.data_shards.get(&global_id);

        match current_shard_id {
            Some(current_shard_id) if current_shard_id == &shard_id => {
                // No need to change anything!
            }
            Some(current_shard_id) => {
                // Need to retract the old mapping and insert a new mapping.
                updates.push((
                    StateUpdate::DataShard(global_id.clone(), current_shard_id.clone()),
                    -1,
                ));
                updates.push((StateUpdate::DataShard(global_id.clone(), shard_id), 1));
            }
            None => {
                // Only need to add the new mapping.
                updates.push((StateUpdate::DataShard(global_id, shard_id), 1));
            }
        }

        updates
    }

    pub fn prepare_upsert_remap_shard(
        &self,
        global_id: GlobalId,
        shard_id: ShardId,
    ) -> Vec<(StateUpdate<T>, i64)> {
        let mut updates = Vec::new();

        let current_shard_id = self.remap_shards.get(&global_id);

        match current_shard_id {
            Some(current_shard_id) if current_shard_id == &shard_id => {
                // No need to change anything!
            }
            Some(current_shard_id) => {
                // Need to retract the old mapping and insert a new mapping.
                updates.push((
                    StateUpdate::RemapShard(global_id.clone(), current_shard_id.clone()),
                    -1,
                ));
                updates.push((StateUpdate::RemapShard(global_id.clone(), shard_id), 1));
            }
            None => {
                // Only need to add the new mapping.
                updates.push((StateUpdate::RemapShard(global_id, shard_id), 1));
            }
        }

        updates
    }

    pub fn prepare_upsert_export_as_of(
        &self,
        global_id: GlobalId,
        as_of: SinkAsOf<T>,
    ) -> Vec<(StateUpdate<T>, i64)> {
        let (as_of, strict) = (
            as_of.frontier.as_option().expect("empty antichain"),
            as_of.strict,
        );

        let mut updates = Vec::new();

        let current_as_of = self.export_as_ofs.get(&global_id);

        match current_as_of {
            Some((current_as_of, current_strict))
                if current_as_of == as_of && current_strict == &strict =>
            {
                // No need to change anything!
            }
            Some((current_as_of, current_strict)) => {
                // Need to retract the old mapping and insert a new mapping.
                updates.push((
                    StateUpdate::ExportAsOf(
                        global_id.clone(),
                        (current_as_of.clone(), current_strict.clone()),
                    ),
                    -1,
                ));
                updates.push((
                    StateUpdate::ExportAsOf(global_id.clone(), (as_of.clone(), strict)),
                    1,
                ));
            }
            None => {
                // Only need to add the new mapping.
                updates.push((
                    StateUpdate::ExportAsOf(global_id.clone(), (as_of.clone(), strict)),
                    1,
                ));
            }
        }

        updates
    }

    pub fn prepare_insert_finalized_shard(&self, shard_id: ShardId) -> Vec<(StateUpdate<T>, i64)> {
        let mut updates = Vec::new();

        let exists = self.shard_finalization_wal.contains(&shard_id);

        if exists {
            // No need to change anything!
        } else {
            // Need to insert.
            updates.push((StateUpdate::FinalizedShard(shard_id), 1));
        }

        updates
    }

    // We currently never remove old data shards from durable controller state.
    #[allow(unused)]
    pub fn prepare_remove_data_shard(&self, global_id: GlobalId) -> Vec<(StateUpdate<T>, i64)> {
        let mut updates = Vec::new();

        let current_shard_id = self.data_shards.get(&global_id);

        match current_shard_id {
            Some(current_shard_id) => {
                // Need to retract the mapping.
                updates.push((
                    StateUpdate::DataShard(global_id.clone(), current_shard_id.clone()),
                    -1,
                ));
            }
            None => {
                // Nothing to do!
            }
        }

        updates
    }

    // We currently never remove old remap shards from durable controller state.
    #[allow(unused)]
    pub fn prepare_remove_remap_shard(&self, global_id: GlobalId) -> Vec<(StateUpdate<T>, i64)> {
        let mut updates = Vec::new();

        let current_shard_id = self.remap_shards.get(&global_id);

        match current_shard_id {
            Some(current_shard_id) => {
                // Need to retract the mapping.
                updates.push((
                    StateUpdate::RemapShard(global_id.clone(), current_shard_id.clone()),
                    -1,
                ));
            }
            None => {
                // Nothing to do!
            }
        }

        updates
    }

    // We currently never remove old export state from durable controller state.
    #[allow(unused)]
    pub fn prepare_remove_export_as_of(&self, global_id: GlobalId) -> Vec<(StateUpdate<T>, i64)> {
        let mut updates = Vec::new();

        let current_as_of = self.export_as_ofs.get(&global_id);

        match current_as_of {
            Some((current_as_of, current_strict)) => {
                // Need to retract the mapping.
                updates.push((
                    StateUpdate::ExportAsOf(
                        global_id.clone(),
                        (current_as_of.clone(), current_strict.clone()),
                    ),
                    -1,
                ));
            }
            None => {
                // Nothing to do!
            }
        }

        updates
    }

    pub fn prepare_remove_finalized_shard(&self, shard_id: ShardId) -> Vec<(StateUpdate<T>, i64)> {
        let mut updates = Vec::new();

        let exists = self.shard_finalization_wal.contains(&shard_id);

        if exists {
            // Need to retract the mapping.
            updates.push((StateUpdate::FinalizedShard(shard_id.clone()), -1));
        } else {
            // Nothing to do!
        }

        updates
    }

    pub async fn commit_updates(
        &mut self,
        updates: Vec<(StateUpdate<T>, i64)>,
    ) -> Result<(), UpperMismatch<u64>> {
        println!("committing {:?}", updates);

        let current_upper = self.upper.clone();
        // Yeah yeah, need to make this better...
        let next_upper = current_upper + 1;

        let mut batch_builder = self
            .write_handle
            .builder(Antichain::from_elem(current_upper));

        for (state_update, diff) in updates.into_iter() {
            batch_builder
                .add(&state_update, &(), &current_upper, &diff)
                .await
                .expect("invalid usage");
        }

        let mut batch = batch_builder
            .finish(Antichain::from_elem(next_upper))
            .await
            .expect("invalid usage");

        let res = self
            .write_handle
            .compare_and_append_batch(
                &mut [&mut batch],
                Antichain::from_elem(current_upper),
                Antichain::from_elem(next_upper),
            )
            .await
            .expect("invalid usage");

        match res {
            Ok(()) => {
                // All's well! We can sync back our updates to update our
                // in-memory maps/traces.
                self.sync(next_upper).await;
            }
            Err(upper_mismatch) => return Err(upper_mismatch),
        }

        Ok(())
    }

    async fn sync(&mut self, target_upper: u64) {
        let mut updates: Vec<(StateUpdate<T>, u64, i64)> = Vec::new();

        // Tail the log until we reach the target upper. Note that, in the
        // common case, we are also the writer, so we are waiting to read back
        // what we wrote.
        while self.upper < target_upper {
            let listen_events = self.listen.fetch_next().await;

            for listen_event in listen_events.into_iter() {
                match listen_event {
                    ListenEvent::Progress(upper) => {
                        println!("progress: {:?}", upper);
                        self.upper = upper
                            .as_option()
                            .cloned()
                            .expect("we use a totally ordered time and never finalize the shard");
                    }
                    ListenEvent::Updates(batch_updates) => {
                        println!("updates: {:?}", batch_updates);
                        let mut batch_updates = batch_updates
                            .into_iter()
                            .map(|((key, _unit), ts, diff)| {
                                (key.expect("key decoding error"), ts, diff)
                            })
                            .collect_vec();

                        updates.append(&mut batch_updates);
                    }
                }
            }
        }

        self.apply_updates(updates);
    }

    // TODO(aljoscha): We should keep the collections as a _trace_, with the
    // full diffs. And only collapse them down to a map when needed. And also
    // ensure that there is only one entry for a given `GlobalId` then.
    fn apply_updates(&mut self, mut updates: Vec<(StateUpdate<T>, u64, i64)>) {
        self.trace.append(&mut updates);

        let upper_frontier = Antichain::from_elem(self.upper);
        for (_update, ts, _diff) in self.trace.iter_mut() {
            ts.advance_by(upper_frontier.borrow());
        }

        // TODO(aljoscha): Make sure we reduce the capacity of `self.trace` if
        // it grows too much.
        consolidation::consolidate_updates(&mut self.trace);

        // TODO(aljoscha): This is very inefficient, we're rebuilding the
        // in-memory cash of every type of collection every time we're applying
        // updates. It's easy and correct, though.
        self.data_shards = BTreeMap::new();
        self.remap_shards = BTreeMap::new();
        self.export_as_ofs = BTreeMap::new();
        self.shard_finalization_wal = BTreeSet::new();

        for update in self.trace.iter() {
            match update {
                (state_update, _ts, 1) => match state_update {
                    StateUpdate::DataShard(global_id, shard_id) => {
                        self.data_shards.insert(global_id.clone(), shard_id.clone());
                    }
                    StateUpdate::RemapShard(global_id, shard_id) => {
                        self.remap_shards
                            .insert(global_id.clone(), shard_id.clone());
                    }
                    StateUpdate::ExportAsOf(global_id, as_of) => {
                        self.export_as_ofs.insert(global_id.clone(), as_of.clone());
                    }
                    StateUpdate::FinalizedShard(shard_id) => {
                        let is_new = self.shard_finalization_wal.insert(shard_id.clone());
                        if !is_new {
                            panic!(
                                "invalid state, already had shard {:?} in the finalization WAL: {:?}",
                                shard_id, self.shard_finalization_wal
                            );
                        }
                    }
                },
                invalid_update => {
                    panic!("invalid update in consolidated trace: {:?}", invalid_update);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::Duration;

    use once_cell::sync::Lazy;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::{PersistLocation, ShardId};
    use mz_repr::GlobalId;

    // 15 minutes
    static PERSIST_READER_LEASE_TIMEOUT_MS: Duration = Duration::from_secs(60 * 15);

    static PERSIST_CACHE: Lazy<Arc<PersistClientCache>> = Lazy::new(|| {
        let mut persistcfg = PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
        persistcfg.reader_lease_duration = PERSIST_READER_LEASE_TIMEOUT_MS;
        Arc::new(PersistClientCache::new(persistcfg, &MetricsRegistry::new()))
    });

    async fn make_test_state(shard_id: ShardId) -> DurableControllerState<mz_repr::Timestamp> {
        let persist_location = PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        };

        let persist_client = PERSIST_CACHE.open(persist_location).await.unwrap();

        let durable_storage_state = DurableControllerState::new(shard_id, persist_client).await;

        durable_storage_state
    }

    #[tokio::test]
    async fn test_basic_usage() {
        let log_shard_id = ShardId::new();

        let mut durable_state = make_test_state(log_shard_id).await;

        let global_id = GlobalId::User(0);
        let data_shard = ShardId::new();
        let remap_shard = ShardId::new();
        let export_as_of = SinkAsOf {
            frontier: Antichain::from_elem(mz_repr::Timestamp::new(13)),
            strict: true,
        };

        let mut updates = Vec::new();

        let mut new_updates = durable_state.prepare_upsert_data_shard(global_id, data_shard);
        updates.append(&mut new_updates);
        let mut new_updates = durable_state.prepare_upsert_remap_shard(global_id, remap_shard);
        updates.append(&mut new_updates);
        let mut new_updates =
            durable_state.prepare_upsert_export_as_of(global_id, export_as_of.clone());
        updates.append(&mut new_updates);

        let res = durable_state.commit_updates(updates).await;
        assert!(matches!(res, Ok(())));

        let stored_data_shard = durable_state.get_data_shard(&global_id);
        assert_eq!(stored_data_shard, Some(data_shard));

        let stored_remap_shard = durable_state.get_remap_shard(&global_id);
        assert_eq!(stored_remap_shard, Some(remap_shard));

        let stored_export_as_of = durable_state.get_export_as_of(&global_id);
        assert_eq!(stored_export_as_of, Some(export_as_of.clone()));

        println!("re-creating!");

        // Re-create our durable state.
        let mut durable_state = make_test_state(log_shard_id).await;

        let stored_data_shard = durable_state.get_data_shard(&global_id);
        assert_eq!(stored_data_shard, Some(data_shard));

        let stored_remap_shard = durable_state.get_remap_shard(&global_id);
        assert_eq!(stored_remap_shard, Some(remap_shard));

        let stored_export_as_of = durable_state.get_export_as_of(&global_id);
        assert_eq!(stored_export_as_of, Some(export_as_of));

        // Do something that causes a retraction and update.
        let mut updates = Vec::new();

        let new_data_shard = ShardId::new();

        let mut new_updates = durable_state.prepare_upsert_data_shard(global_id, new_data_shard);
        updates.append(&mut new_updates);

        let res = durable_state.commit_updates(updates).await;
        assert!(matches!(res, Ok(())));

        // Re-create our durable state.
        let durable_state = make_test_state(log_shard_id).await;

        let stored_data_shard = durable_state.get_data_shard(&global_id);
        assert_eq!(stored_data_shard, Some(new_data_shard));
    }

    #[tokio::test]
    async fn test_finalization_wal() {
        let log_shard_id = ShardId::new();

        let mut durable_state = make_test_state(log_shard_id).await;

        let shard1 = ShardId::new();
        let shard2 = ShardId::new();

        let mut updates = Vec::new();

        let mut new_updates = durable_state.prepare_insert_finalized_shard(shard1);
        updates.append(&mut new_updates);
        let mut new_updates = durable_state.prepare_insert_finalized_shard(shard2);
        updates.append(&mut new_updates);

        let res = durable_state.commit_updates(updates).await;
        assert!(matches!(res, Ok(())));

        assert!(durable_state.is_in_finalization_wal(&shard1));
        assert!(durable_state.is_in_finalization_wal(&shard2));

        println!("re-creating!");

        // Re-create our durable state.
        let mut durable_state = make_test_state(log_shard_id).await;

        assert!(durable_state.is_in_finalization_wal(&shard1));
        assert!(durable_state.is_in_finalization_wal(&shard2));

        let mut updates = Vec::new();

        let mut new_updates = durable_state.prepare_remove_finalized_shard(shard1);
        updates.append(&mut new_updates);

        let res = durable_state.commit_updates(updates).await;
        assert!(matches!(res, Ok(())));

        assert!(!durable_state.is_in_finalization_wal(&shard1));
        assert!(durable_state.is_in_finalization_wal(&shard2));
    }
}
