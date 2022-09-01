// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Healthchecks for sources
use anyhow::Context;
use chrono::NaiveDateTime;
use std::fmt::Display;
use std::sync::Arc;
use timely::progress::{Antichain, Timestamp as _};
use timely::PartialOrder;
use tokio::sync::Mutex;
use tracing::trace;

use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::{Listen, ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_repr::{Datum, GlobalId, Row, Timestamp};

use crate::source::{CollectionMetadata, NowFn};
use crate::types::sources::SourceData;

/// The Healthchecker is responsible for tracking the current state
/// of a Timely worker for a source, as well as updating the relevant
/// state collection based on it.
pub struct Healthchecker {
    /// Name of the source (e.g. kafka-s1)
    source_name: String,
    /// Name of the upstream resource, if any (e.g. Kafka topic, file path)
    upstream_name: Option<String>,
    /// Internal ID of the source (e.g. s1)
    source_id: GlobalId,
    /// Type of the source, from SourceConnection::name
    source_type: &'static str,
    /// The ID of the Timely worker on which this operator is executing
    worker_id: usize,
    /// The total count of Timely workers
    worker_count: usize,
    /// Whether this is an active Timely worker or not
    active: bool,
    /// Current status of this source
    current_status: SourceStatus,
    /// Last observed upper
    upper: Antichain<Timestamp>,
    /// Write handle of the Healthchecker persist shard
    ///
    /// The schema used matches the one used in regular sources and tables.
    write_handle: WriteHandle<SourceData, (), Timestamp, i64>,
    /// A listener to tail the Healthchecker shard for new updates
    listener: Listen<SourceData, (), Timestamp, i64>,
    /// The function that should be used to get the current time when updating upper
    now: NowFn,
}

impl Healthchecker {
    pub async fn new(
        source_name: String,
        upstream_name: Option<String>,
        source_id: GlobalId,
        source_type: &'static str,
        worker_id: usize,
        worker_count: usize,
        active: bool,
        persist_clients: &Arc<Mutex<PersistClientCache>>,
        storage_metadata: &CollectionMetadata,
        now: NowFn,
    ) -> anyhow::Result<Self> {
        trace!("Initializing healthchecker for source {source_name}");
        let mut persist_clients = persist_clients.lock().await;
        let persist_client = persist_clients
            .open(storage_metadata.persist_location.clone())
            .await
            .context("error creating persist client for Healthchecker")?;
        drop(persist_clients);

        let (write_handle, read_handle) = persist_client
            .open(storage_metadata.status_shard.unwrap())
            .await
            .context("error opening Healthchecker persist shard")?;

        let (since, upper) = (read_handle.since().clone(), write_handle.upper().clone());

        // More details on why the listener starts at `since` instead of `upper` in the docstring for [`bootstrap_state`]
        let listener = read_handle
            .clone()
            .await
            .listen(since.clone())
            .await
            .expect("since <= as_of asserted");

        let mut healthchecker = Self {
            worker_id,
            worker_count,
            source_type,
            source_name,
            source_id,
            upstream_name,
            current_status: SourceStatus::Starting,
            active,
            upper: Antichain::from_elem(Timestamp::minimum()),
            write_handle,
            listener,
            now,
        };

        // Bootstrap should reload the previous state of the source, if any
        healthchecker.bootstrap_state(read_handle, &upper).await;
        tracing::trace!(
            "Healthchecker for source {} at status {} finished bootstrapping!",
            &healthchecker.source_name,
            &healthchecker.current_status
        );

        Ok(healthchecker)
    }

    /// Process a [`SourceStatusUpdate`] emitted by a source
    pub async fn update_status(&mut self, status_update: SourceStatusUpdate) {
        trace!(
            "Processing status update: {status_update:?}, current status is {current_status}",
            current_status = &self.current_status
        );
        // Only update status if it is a valid transition
        if self.active && self.current_status.can_transition(&status_update.status) {
            loop {
                let next_ts = (self.now)();
                let new_upper = Antichain::from_elem(next_ts + 1);

                let updates = self.prepare_row_update(&status_update, next_ts);
                match self
                    .write_handle
                    .compare_and_append(updates.iter(), self.upper.clone(), new_upper.clone())
                    .await
                {
                    Ok(Ok(Ok(()))) => {
                        self.upper = new_upper;
                        // Update internal status only after a successful append
                        self.current_status = status_update.status;
                        break;
                    }
                    Ok(Ok(Err(actual_upper))) => {
                        trace!(
                            "Had to retry updating status, old upper {:?}, new upper {:?}",
                            &self.upper,
                            &actual_upper
                        );
                        // Sync to the new upper, go to the loop again
                        self.sync(&actual_upper.0).await;
                        // If we can't transition to the new status after the sync, no need to do anything else
                        if !self.current_status.can_transition(&status_update.status) {
                            break;
                        }
                    }
                    Ok(Err(invalid_use)) => panic!("compare_and_append failed: {invalid_use}"),
                    // An external error means that the operation might have suceeded or failed but we
                    // don't know. In either case it is safe to retry because:
                    // * If it succeeded, then on retry we'll get an `Upper(_)` error as if some other
                    //   process raced us. This is safe and will just cause the healthchecker to sync
                    //   again, and on retry it will notice that the new state was already processed and
                    //   finish successfully.
                    // * If it failed, then we'll succeed on retry and proceed normally.
                    Err(external_err) => {
                        trace!("compare_and_append in update_status failed: {external_err}");
                        continue;
                    }
                };
            }
        }
    }

    /// Synchronizes internal state with state in the storage collection up until a given timestamp
    async fn sync(&mut self, target_upper: &Antichain<Timestamp>) {
        while PartialOrder::less_than(&self.upper, target_upper) {
            for event in self.listener.next().await {
                match event {
                    ListenEvent::Progress(new_upper) => {
                        self.upper = new_upper;
                    }
                    ListenEvent::Updates(updates) => {
                        self.process_collection_updates(updates);
                    }
                }
            }
        }
    }

    /// Bootstraps the state of this Healthchecker instance by reading data from the
    /// underlying storage collection
    ///
    /// This function works by first reading a snapshot of the collection at its `since`,
    /// and then using the listener to read all updates from `since` up until (but not including)
    /// `upper`. This is done as a way to read all data in the collection, but without
    /// having to assume that the `upper` is a single `u64`.
    async fn bootstrap_state(
        &mut self,
        mut read_handle: ReadHandle<SourceData, (), Timestamp, i64>,
        upper: &Antichain<Timestamp>,
    ) {
        let since = read_handle.since().clone();
        trace!("Bootstrapping state as of {:?}!", since);
        // Ensure the collection is readable at `since`
        if PartialOrder::less_than(&since, &self.upper) {
            let updates = read_handle
                .snapshot_and_fetch(since.clone())
                .await
                .expect("local since is not beyond read handle's since");
            self.process_collection_updates(updates);
        };
        self.sync(upper).await;
        trace!("State bootstrapped as of {since:?}!");
    }

    /// Process any updates that might be in the collection to update current status
    /// Currently assumes that the collection only contains assertions (rows with diff = 1)
    fn process_collection_updates(
        &mut self,
        mut updates: Vec<(
            (Result<SourceData, String>, Result<(), String>),
            Timestamp,
            i64,
        )>,
    ) {
        // Sort by timestamp and diff
        updates.sort_by(|(_, t1, d1), (_, t2, d2)| (t1, d1).cmp(&(t2, d2)));
        for ((source_data, _), ts, _diff) in updates {
            trace!("Reading from snapshot at time {}: {:?}", ts, &source_data);
            let row = source_data
                .expect("failed to deserialize row")
                .0
                .expect("status collection should not have errors");
            let row_vec = row.unpack();
            let row_source_id = row_vec[2].unwrap_str();
            let row_worker_id = row_vec[5].unwrap_int64();
            let row_status = row_vec[7].unwrap_str();

            if self.source_id.to_string() == row_source_id
                && self.worker_id == row_worker_id as usize
            {
                self.current_status = SourceStatus::try_from(row_status).expect("invalid status");
            }
        }
    }

    fn prepare_row_update(
        &self,
        status_update: &SourceStatusUpdate,
        ts: u64,
    ) -> Vec<((SourceData, ()), Timestamp, i64)> {
        let timestamp = NaiveDateTime::from_timestamp(
            (ts / 1000)
                .try_into()
                .expect("timestamp seconds does not fit into i64"),
            (ts % 1000 * 1_000_000)
                .try_into()
                .expect("timestamp millis does not fit into a u32"),
        )
        .into();
        let source_id = self.source_id.to_string();
        let source_name = Datum::String(&self.source_name);
        let source_id = Datum::String(&source_id);
        let source_type: Datum = self.source_type.into();
        let upstream_name: Datum = self.upstream_name.as_deref().into();
        let worker_id =
            Datum::Int64(i64::try_from(self.worker_id).expect("worker_id does not fit into i64"));
        let worker_count = Datum::Int64(
            i64::try_from(self.worker_count).expect("worker_count does not fit into i64"),
        );
        let status = Datum::String(status_update.status.name());
        let error = status_update.error.as_deref().into();
        let metadata = Datum::Null;
        let row = Row::pack_slice(&[
            timestamp,
            source_name,
            source_id,
            source_type,
            upstream_name,
            worker_id,
            worker_count,
            status,
            error,
            metadata,
        ]);

        vec![((SourceData(Ok(row)), ()), ts, 1)]
    }
}

/// Identify the state a worker for a given source can be at a point in time
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceStatus {
    /// Initial state of a Source during initialization.
    Setup,
    /// Intended to be the state while the `storaged` process is initializing itself
    /// Pushed by the Healthchecker on creation.
    Starting,
    /// State indicating the source is running fine. Pushed automatically as long
    /// as rows are being consumed.
    Running,
    /// Represents a stall in the ingestion process that might get resolved.
    /// Existing data is still available and queryable.
    Stalled,
    /// Represents a irrecoverable failure in the pipeline. Data from this collection
    /// is not queryable any longer. The only valid transition from Failed is Dropped.
    Failed,
    /// Represents a source that was dropped.
    /// TODO(andrioni): make the controller push this update, as `Drop` is not called for
    /// the Healthchecker.
    Dropped,
}

impl SourceStatus {
    fn name(&self) -> &'static str {
        match self {
            SourceStatus::Setup => "setup",
            SourceStatus::Starting => "starting",
            SourceStatus::Running => "running",
            SourceStatus::Stalled => "stalled",
            SourceStatus::Failed => "failed",
            SourceStatus::Dropped => "dropped",
        }
    }

    fn can_transition(&self, new_status: &SourceStatus) -> bool {
        match self {
            // Failed can only transition to Dropped
            SourceStatus::Failed => matches!(new_status, SourceStatus::Dropped),
            // Dropped is a terminal state
            SourceStatus::Dropped => false,
            // All other states can transition freely to any other state
            SourceStatus::Setup
            | SourceStatus::Starting
            | SourceStatus::Running
            | SourceStatus::Stalled => self != new_status,
        }
    }
}

impl Display for SourceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<&str> for SourceStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "setup" => Ok(SourceStatus::Setup),
            "starting" => Ok(SourceStatus::Starting),
            "running" => Ok(SourceStatus::Running),
            "stalled" => Ok(SourceStatus::Stalled),
            "failed" => Ok(SourceStatus::Failed),
            "dropped" => Ok(SourceStatus::Dropped),
            _ => Err(format!("{value} is not a valid SourceStatus")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStatusUpdate {
    status: SourceStatus,
    error: Option<String>,
    // TODO(andrioni): figure out later how to accept a JSON as metadata
}

impl SourceStatusUpdate {
    pub fn new(status: SourceStatus) -> Self {
        Self {
            status,
            error: None,
        }
    }

    pub fn failed(error_message: &str) -> Self {
        Self {
            status: SourceStatus::Failed,
            error: Some(error_message.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use itertools::Itertools;
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::now::SYSTEM_TIME;
    use once_cell::sync::Lazy;

    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::{PersistConfig, PersistLocation, ShardId};

    // Test suite
    #[tokio::test(start_paused = true)]
    async fn test_startup() {
        let persist_cache = persist_cache();
        let healthchecker = simple_healthchecker(ShardId::new(), 1, &persist_cache).await;

        assert_eq!(healthchecker.current_status, SourceStatus::Starting);
    }

    #[tokio::test(start_paused = true)]
    async fn test_simple_bootstrap() {
        let shard_id = ShardId::new();
        let persist_cache = persist_cache();

        let mut healthchecker = simple_healthchecker(shard_id, 1, &persist_cache).await;

        tokio::time::advance(Duration::from_millis(1)).await;

        // Update status to Running
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Running))
            .await;

        // Check that the status is indeed Running
        assert_eq!(healthchecker.current_status, SourceStatus::Running);

        // Start new healthchecker on the same shard
        let healthchecker = simple_healthchecker(shard_id, 1, &persist_cache).await;

        // Ensure that we loaded the previous state
        assert_eq!(healthchecker.current_status, SourceStatus::Running);
    }

    #[tokio::test(start_paused = true)]
    async fn test_bootstrap_last_state() {
        let shard_id = ShardId::new();
        let persist_cache = persist_cache();

        let mut healthchecker = simple_healthchecker(shard_id, 1, &persist_cache).await;

        tokio::time::advance(Duration::from_millis(1)).await;

        // Update status to Running
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Running))
            .await;

        // Now update status to Stalled
        tokio::time::advance(Duration::from_millis(1)).await;
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Stalled))
            .await;

        // Check that the status is indeed Stalled
        assert_eq!(healthchecker.current_status, SourceStatus::Stalled);

        // Start new healthchecker on the same shard
        let healthchecker = simple_healthchecker(shard_id, 1, &persist_cache).await;

        // Ensure that it is at the latest state, Stalled, not Running or Starting
        assert_eq!(healthchecker.current_status, SourceStatus::Stalled);
    }

    #[tokio::test(start_paused = true)]
    async fn test_bootstrap_different_sources() {
        let shard_id = ShardId::new();
        let persist_cache = persist_cache();

        // First healthchecker is for source u1
        let mut healthchecker = simple_healthchecker(shard_id, 1, &persist_cache).await;

        tokio::time::advance(Duration::from_millis(1)).await;

        // Update status to Running
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Running))
            .await;

        // Start new healthchecker on the same shard for source u2
        let healthchecker = simple_healthchecker(shard_id, 2, &persist_cache).await;

        // It should ignore the state for source u1, and be at the Starting state
        assert_eq!(healthchecker.current_status, SourceStatus::Starting);
    }

    #[tokio::test(start_paused = true)]
    async fn test_repeated_update() {
        let shard_id = ShardId::new();
        let persist_cache = persist_cache();
        let mut healthchecker = simple_healthchecker(shard_id, 1, &persist_cache).await;
        tokio::time::advance(Duration::from_millis(1)).await;

        // Update status to Running
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Running))
            .await;

        // Now update status to Running multiple times, which is a no-op
        tokio::time::advance(Duration::from_millis(1)).await;
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Running))
            .await;
        tokio::time::advance(Duration::from_millis(1)).await;
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Running))
            .await;

        // Check in the storage collection that there is just a single row
        assert_eq!(
            dump_storage_collection(shard_id, &persist_cache)
                .await
                .len(),
            1
        );

        // Create another healthchecker with a different id, and also set it to Running
        let mut healthchecker = simple_healthchecker(shard_id, 2, &persist_cache).await;
        // Advance past the previous update, since each healthchecker has its own notion of time
        tokio::time::advance(Duration::from_millis(2)).await;
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Running))
            .await;

        // Now we should have two rows in the storage collection, one for each source_id
        assert_eq!(
            dump_storage_collection(shard_id, &persist_cache)
                .await
                .len(),
            2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_forbidden_transition() {
        let shard_id = ShardId::new();
        let persist_cache = persist_cache();
        let mut healthchecker = simple_healthchecker(shard_id, 1, &persist_cache).await;
        tokio::time::advance(Duration::from_millis(1)).await;

        // Update status to Running
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Running))
            .await;

        // Now update status to Failed
        tokio::time::advance(Duration::from_millis(1)).await;
        healthchecker
            .update_status(SourceStatusUpdate::failed("some error here"))
            .await;
        assert_eq!(healthchecker.current_status, SourceStatus::Failed);

        // Validate that we can't transition back to Running
        tokio::time::advance(Duration::from_millis(1)).await;
        healthchecker
            .update_status(SourceStatusUpdate::new(SourceStatus::Running))
            .await;
        assert_eq!(healthchecker.current_status, SourceStatus::Failed);

        // Check that the error message is persisted
        let error_message = dump_storage_collection(shard_id, &persist_cache)
            .await
            .into_iter()
            .find_map(|row| {
                let error = row.unpack()[8];
                if !error.is_null() {
                    Some(error.unwrap_str().to_string())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(error_message, "some error here")
    }

    #[test]
    fn test_can_transition() {
        let test_cases = [
            // Allowed transitions
            (
                SourceStatus::Setup,
                vec![
                    SourceStatus::Starting,
                    SourceStatus::Running,
                    SourceStatus::Stalled,
                    SourceStatus::Failed,
                    SourceStatus::Dropped,
                ],
                true,
            ),
            (
                SourceStatus::Starting,
                vec![
                    SourceStatus::Setup,
                    SourceStatus::Running,
                    SourceStatus::Stalled,
                    SourceStatus::Failed,
                    SourceStatus::Dropped,
                ],
                true,
            ),
            (
                SourceStatus::Running,
                vec![
                    SourceStatus::Setup,
                    SourceStatus::Starting,
                    SourceStatus::Stalled,
                    SourceStatus::Failed,
                    SourceStatus::Dropped,
                ],
                true,
            ),
            (
                SourceStatus::Stalled,
                vec![
                    SourceStatus::Setup,
                    SourceStatus::Starting,
                    SourceStatus::Running,
                    SourceStatus::Failed,
                    SourceStatus::Dropped,
                ],
                true,
            ),
            (SourceStatus::Failed, vec![SourceStatus::Dropped], true),
            // Forbidden transitions
            (SourceStatus::Setup, vec![SourceStatus::Setup], false),
            (SourceStatus::Starting, vec![SourceStatus::Starting], false),
            (SourceStatus::Running, vec![SourceStatus::Running], false),
            (SourceStatus::Stalled, vec![SourceStatus::Stalled], false),
            (
                SourceStatus::Failed,
                vec![
                    SourceStatus::Setup,
                    SourceStatus::Starting,
                    SourceStatus::Running,
                    SourceStatus::Stalled,
                    SourceStatus::Failed,
                ],
                false,
            ),
            (
                SourceStatus::Dropped,
                vec![
                    SourceStatus::Setup,
                    SourceStatus::Starting,
                    SourceStatus::Running,
                    SourceStatus::Stalled,
                    SourceStatus::Failed,
                    SourceStatus::Dropped,
                ],
                false,
            ),
        ];

        for test_case in test_cases {
            run_test(test_case)
        }

        fn run_test(test_case: (SourceStatus, Vec<SourceStatus>, bool)) {
            let (from_status, to_status, allowed) = test_case;
            for status in to_status {
                assert_eq!(allowed, from_status.can_transition(&status))
            }
        }
    }

    // Auxiliary functions
    fn persist_cache() -> Arc<Mutex<PersistClientCache>> {
        Arc::new(Mutex::new(PersistClientCache::new(
            PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
        )))
    }

    static PERSIST_LOCATION: Lazy<PersistLocation> = Lazy::new(|| PersistLocation {
        blob_uri: "mem://".to_owned(),
        consensus_uri: "mem://".to_owned(),
    });

    async fn new_healthchecker(
        status_shard_id: ShardId,
        source_name: String,
        source_id: GlobalId,
        source_type: &'static str,
        upstream_name: Option<String>,
        worker_id: usize,
        worker_count: usize,
        active: bool,
        persist_clients: &Arc<Mutex<PersistClientCache>>,
    ) -> Healthchecker {
        let start = tokio::time::Instant::now();
        let now_fn = NowFn::from(move || start.elapsed().as_millis() as u64);

        let storage_metadata = CollectionMetadata {
            persist_location: (*PERSIST_LOCATION).clone(),
            remap_shard: ShardId::new(),
            data_shard: ShardId::new(),
            status_shard: Some(status_shard_id),
        };

        Healthchecker::new(
            source_name,
            upstream_name,
            source_id,
            source_type,
            worker_id,
            worker_count,
            active,
            persist_clients,
            &storage_metadata,
            now_fn,
        )
        .await
        .unwrap()
    }

    async fn simple_healthchecker(
        status_shard_id: ShardId,
        source_id: u64,
        persist_clients: &Arc<Mutex<PersistClientCache>>,
    ) -> Healthchecker {
        new_healthchecker(
            status_shard_id,
            "source".to_string(),
            GlobalId::User(source_id),
            "kafka",
            Some("sample-topic".to_string()),
            1,
            1,
            true,
            &Arc::clone(persist_clients),
        )
        .await
    }

    async fn dump_storage_collection(
        shard_id: ShardId,
        persist_clients: &Arc<Mutex<PersistClientCache>>,
    ) -> Vec<Row> {
        let persist_client = persist_clients
            .lock()
            .await
            .open((*PERSIST_LOCATION).clone())
            .await
            .unwrap();

        let (write_handle, mut read_handle) = persist_client.open(shard_id).await.unwrap();

        let upper = write_handle.upper();
        let readable_upper = Antichain::from_elem(upper.elements()[0] - 1);

        read_handle
            .snapshot_and_fetch(readable_upper)
            .await
            .unwrap()
            .into_iter()
            .map(
                |((v, _), _, _): ((Result<SourceData, String>, Result<(), String>), u64, i64)| {
                    v.unwrap().0.unwrap()
                },
            )
            .collect_vec()
    }
}
