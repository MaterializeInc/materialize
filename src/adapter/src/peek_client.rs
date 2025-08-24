// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Thin client for fast-path peek sequencing. This intentionally carries
// minimal state: just the handles necessary to talk directly to compute
// instances and to the storage collections view.

use std::collections::BTreeMap;
use std::sync::Arc;

use mz_compute_types::ComputeInstanceId;
use mz_repr::Timestamp;

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
#[derive(Debug, Default)]
pub struct PeekClient {
    /// Channels to talk to each compute Instance task directly.
    pub compute_instances: BTreeMap<
        ComputeInstanceId,
        mz_compute_client::controller::instance::Client<Timestamp>,
    >,
    /// Handle to storage collections for reading frontiers and policies.
    pub storage_collections: Option<StorageCollectionsHandle>,
}

impl PeekClient {
    /// Creates an empty PeekClient with no attached controllers. These will be
    /// populated later when wiring fast-path peeks.
    pub fn new() -> Self {
        Self {
            compute_instances: BTreeMap::new(),
            storage_collections: None,
        }
    }

    /// Stub: acquire read holds on the required compute/storage collections.
    ///
    /// This will be implemented to coordinate read holds across the relevant
    /// controllers.
    pub async fn acquire_read_holds(&self) {
        // stub
    }

    /// Stub: determine the least valid write frontier to pick a safe read timestamp.
    pub async fn least_valid_write(&self) {
        // stub
    }

    /// Stub: implement the provided peek plan directly against a compute instance.
    pub async fn implement_peek_plan(&self) {
        // stub
    }
}
