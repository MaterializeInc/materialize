// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Thin client for fast-path peek sequencing. This intentionally carries
// minimal state: just the handles necessary to talk directly to compute
// instances and to the storage collections view.

use std::collections::BTreeMap;
use std::sync::Arc;

use mz_compute_types::ComputeInstanceId;
use mz_repr::Timestamp;
use timely::progress::Antichain;

/// Storage collections trait alias we need to consult for since/frontiers.
pub type StorageCollectionsHandle = Arc<
    dyn mz_storage_client::storage_collections::StorageCollections<Timestamp = Timestamp>
        + Send
        + Sync,
>;

/// A thin client to the compute and storage controllers for sequencing
/// fast-path peeks from the session task.
///
/// Note: The compute instance client type is generic over timestamp, but in
/// the adapter we operate with the default system timestamp `mz_repr::Timestamp`.
#[derive(Debug)]
pub struct PeekClient {
    /// Channels to talk to each compute Instance task directly.
    /// ////////////// todo: we'll need to update this somehow when instances come and go
    pub compute_instances: BTreeMap<
        ComputeInstanceId,
        mz_compute_client::controller::instance::Client<Timestamp>,
    >,
    /// Handle to storage collections for reading frontiers and policies.
    pub storage_collections: StorageCollectionsHandle,
}

impl PeekClient {
    /// Acquire read holds on the required compute/storage collections.
    /// Similar to Coordinator::acquire_read_holds.
    pub async fn acquire_read_holds(
        &self,
        id_bundle: &crate::CollectionIdBundle,
    ) -> crate::ReadHolds<Timestamp> {
        use tracing::debug;

        let mut read_holds = crate::ReadHolds::new();

        // Acquire storage read holds via StorageCollections.
        let desired_storage: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
        let storage_read_holds = self
            .storage_collections
            .acquire_read_holds(desired_storage)
            .expect("missing storage collections");
        read_holds.storage_holds = storage_read_holds
            .into_iter()
            .map(|hold| (hold.id(), hold))
            .collect();

        // Acquire compute read holds by calling into the appropriate instance tasks.
        for (&instance_id, collection_ids) in &id_bundle.compute_ids {
            let client = self
                .compute_instances
                .get(&instance_id)
                .expect("missing compute instance client");
            for &id in collection_ids {
                let hold = client
                    .acquire_read_hold(id)
                    .await
                    .expect("missing compute collection");
                let prev = read_holds.compute_holds.insert((instance_id, id), hold);
                assert!(prev.is_none(), "duplicate compute ID in id_bundle {id_bundle:?}");
            }
        }

        debug!(?read_holds, "acquire_read_holds (fast path)");
        read_holds
    }

    /// Determine the smallest common valid write frontier among the specified collections.
    ///
    /// Note: Unlike the old Coordinator/StorageController `least_valid_write` that treated sinks
    /// specially when fetching storage frontiers, we intentionally do not special‑case sinks here
    /// because peeks never read from sinks. Therefore, using
    /// `StorageCollections::collections_frontiers` is sufficient. //////// todo: verify
    pub async fn least_valid_write(
        &self,
        id_bundle: &crate::CollectionIdBundle,
    ) -> timely::progress::Antichain<Timestamp> {
        let mut upper = Antichain::new();
        if !id_bundle.storage_ids.is_empty() {
            let storage_ids: Vec<_> = id_bundle.storage_ids.iter().copied().collect();
            for f in self.storage_collections.collections_frontiers(storage_ids).expect("missing collections") {
                upper.extend(f.write_frontier);
            }
        }
        for (instance_id, ids) in &id_bundle.compute_ids {
            let client = self.compute_instances.get(instance_id).expect("PeekClient is missing a compute instance client");
            for id in ids {
                let wf = client.collection_write_frontier(*id).await;
                upper.extend(wf);
            }
        }
        upper
    }

    /// Stub: implement the provided peek plan directly against a compute instance.
    pub async fn implement_peek_plan(&self) {
        // stub
    }
}
