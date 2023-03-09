// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Healthchecks for sinks
use std::fmt::Display;
use std::sync::Arc;

use anyhow::Context;
use tracing::trace;

use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{PersistClient, PersistLocation, ShardId};
use mz_repr::GlobalId;
use mz_storage_client::healthcheck::MZ_SINK_STATUS_HISTORY_DESC;

use crate::healthcheck::write_to_persist;

/// The Healthchecker is responsible for tracking the current state
/// of a Timely worker for a source, as well as updating the relevant
/// state collection based on it.
#[derive(Debug)]
pub struct Healthchecker {
    /// Internal ID of the source (e.g. s1)
    sink_id: GlobalId,
    /// Current status of this source
    current_status: Option<SinkStatus>,
    /// PersistClient of the Healthchecker persist location
    persist_client: PersistClient,
    /// Status shard for the healthchecker
    status_shard: ShardId,
    /// The function that should be used to get the current time when updating upper
    now: NowFn,
}

impl Healthchecker {
    /// Create healthchecker for sink, recorded on `status_shard_id` at `persist_location`.
    ///
    /// This function initializes the Healthchecker in the `SinkStatus::Setup` state without writing to persistent
    /// storage.
    pub async fn new(
        sink_id: GlobalId,
        persist_clients: &Arc<PersistClientCache>,
        persist_location: PersistLocation,
        status_shard: ShardId,
        now: NowFn,
    ) -> anyhow::Result<Self> {
        trace!("Initializing healthchecker for sink {sink_id}");
        let persist_client = persist_clients
            .open(persist_location)
            .await
            .context("error creating persist client for Healthchecker")?;

        Ok(Self {
            sink_id,
            current_status: None,
            persist_client,
            status_shard,
            now,
        })
    }

    /// Process a [`SinkStatus`] emitted by a sink
    pub async fn update_status(&mut self, status_update: SinkStatus) {
        trace!(
            "Processing status update: {status_update:?}, current status is {current_status:?}",
            current_status = &self.current_status
        );

        // Only update status if it is a valid transition
        if SinkStatus::can_transition(self.current_status.as_ref(), &status_update) {
            write_to_persist(
                self.sink_id,
                status_update.name(),
                status_update.error(),
                self.now.clone(),
                &self.persist_client,
                self.status_shard,
                &*MZ_SINK_STATUS_HISTORY_DESC,
                status_update.hint(),
            )
            .await;

            self.current_status = Some(status_update);
        }
    }
}

/// Identify the state a worker for a given source can be at a point in time
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkStatus {
    /// Initial state of a Sink during initialization.
    Setup,
    /// Intended to be the state while the `clusterd` process is initializing itself
    /// Pushed by the Healthchecker on creation.
    Starting,
    /// State indicating the sink is running fine. Pushed automatically as long
    /// as rows are being consumed.
    Running,
    /// Represents a stall in the export process that might get resolved.
    /// Existing data is still available and queryable.
    Stalled { error: String, hint: Option<String> },
    /// Represents a irrecoverable failure in the pipeline. Data from this collection
    /// is not queryable any longer. The only valid transition from Failed is Dropped.
    Failed { error: String, hint: Option<String> },
    /// Represents a sink that was dropped.
    Dropped,
}

impl SinkStatus {
    fn name(&self) -> &'static str {
        match self {
            SinkStatus::Setup => "setup",
            SinkStatus::Starting => "starting",
            SinkStatus::Running => "running",
            SinkStatus::Stalled { .. } => "stalled",
            SinkStatus::Failed { .. } => "failed",
            SinkStatus::Dropped => "dropped",
        }
    }

    fn error(&self) -> Option<&str> {
        match self {
            SinkStatus::Stalled { error, .. } => Some(&*error),
            SinkStatus::Failed { error, .. } => Some(&*error),
            SinkStatus::Setup => None,
            SinkStatus::Starting => None,
            SinkStatus::Running => None,
            SinkStatus::Dropped => None,
        }
    }

    fn hint(&self) -> Option<&str> {
        match self {
            SinkStatus::Stalled { error: _, hint } => hint.as_deref(),
            SinkStatus::Failed { error: _, hint } => hint.as_deref(),
            SinkStatus::Setup => None,
            SinkStatus::Starting => None,
            SinkStatus::Running => None,
            SinkStatus::Dropped => None,
        }
    }

    fn can_transition(old_status: Option<&SinkStatus>, new_status: &SinkStatus) -> bool {
        match old_status {
            None => true,
            // Failed can only transition to Dropped
            Some(SinkStatus::Failed { .. }) => matches!(new_status, SinkStatus::Dropped),
            // Dropped is a terminal state
            Some(SinkStatus::Dropped) => false,
            // All other states can transition freely to any other state
            Some(
                old @ SinkStatus::Setup
                | old @ SinkStatus::Starting
                | old @ SinkStatus::Running
                | old @ SinkStatus::Stalled { .. },
            ) => old != new_status,
        }
    }
}

impl Display for SinkStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use itertools::Itertools;
    use once_cell::sync::Lazy;
    use timely::progress::Antichain;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::{PersistLocation, ShardId};
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_repr::Row;
    use mz_storage_client::types::sources::SourceData;

    // Test suite
    #[tokio::test(start_paused = true)]
    async fn test_startup() {
        let persist_cache = persist_cache();
        let healthchecker = simple_healthchecker(ShardId::new(), 1, &persist_cache).await;

        assert_eq!(healthchecker.current_status, None);
    }

    fn stalled() -> SinkStatus {
        SinkStatus::Stalled {
            error: "".into(),
            hint: None,
        }
    }

    fn failed() -> SinkStatus {
        SinkStatus::Failed {
            error: "".into(),
            hint: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_bootstrap_different_sources() {
        let shard_id = ShardId::new();
        let persist_cache = persist_cache();

        // First healthchecker is for source u1
        let mut healthchecker = simple_healthchecker(shard_id, 1, &persist_cache).await;

        tokio::time::advance(Duration::from_millis(1)).await;

        // Update status to Running
        healthchecker.update_status(SinkStatus::Running).await;

        // Start new healthchecker on the same shard for source u2
        let healthchecker = simple_healthchecker(shard_id, 2, &persist_cache).await;

        // It should ignore the state for source u1, and be at the Setup state
        assert_eq!(healthchecker.current_status, None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_repeated_update() {
        let shard_id = ShardId::new();
        let persist_cache = persist_cache();
        let mut healthchecker = simple_healthchecker(shard_id, 1, &persist_cache).await;
        tokio::time::advance(Duration::from_millis(1)).await;

        // Update status to Running
        healthchecker.update_status(SinkStatus::Running).await;

        // Now update status to Running multiple times, which is a no-op
        tokio::time::advance(Duration::from_millis(1)).await;
        healthchecker.update_status(SinkStatus::Running).await;
        tokio::time::advance(Duration::from_millis(1)).await;
        healthchecker.update_status(SinkStatus::Running).await;

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
        healthchecker.update_status(SinkStatus::Running).await;

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
        healthchecker.update_status(SinkStatus::Running).await;

        // Now update status to Failed
        tokio::time::advance(Duration::from_millis(1)).await;
        let error = String::from("some error here");
        healthchecker
            .update_status(SinkStatus::Failed {
                error: error.clone(),
                hint: None,
            })
            .await;
        assert_eq!(
            healthchecker.current_status,
            Some(SinkStatus::Failed {
                error: error.clone(),
                hint: None,
            })
        );

        // Validate that we can't transition back to Running
        tokio::time::advance(Duration::from_millis(1)).await;
        healthchecker.update_status(SinkStatus::Running).await;
        assert_eq!(
            healthchecker.current_status,
            Some(SinkStatus::Failed { error, hint: None })
        );

        // Check that the error message is persisted
        let error_message = dump_storage_collection(shard_id, &persist_cache)
            .await
            .into_iter()
            .find_map(|row| {
                let error = row.unpack()[3];
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
                Some(SinkStatus::Setup),
                vec![
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            (
                Some(SinkStatus::Starting),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            (
                Some(SinkStatus::Running),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            (
                Some(stalled()),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            (Some(failed()), vec![SinkStatus::Dropped], true),
            (
                None,
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                true,
            ),
            // Forbidden transitions
            (Some(SinkStatus::Setup), vec![SinkStatus::Setup], false),
            (
                Some(SinkStatus::Starting),
                vec![SinkStatus::Starting],
                false,
            ),
            (Some(SinkStatus::Running), vec![SinkStatus::Running], false),
            (Some(stalled()), vec![stalled()], false),
            (
                Some(failed()),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                ],
                false,
            ),
            (
                Some(SinkStatus::Dropped),
                vec![
                    SinkStatus::Setup,
                    SinkStatus::Starting,
                    SinkStatus::Running,
                    stalled(),
                    failed(),
                    SinkStatus::Dropped,
                ],
                false,
            ),
        ];

        for test_case in test_cases {
            run_test(test_case)
        }

        fn run_test(test_case: (Option<SinkStatus>, Vec<SinkStatus>, bool)) {
            let (from_status, to_status, allowed) = test_case;
            for status in to_status {
                assert_eq!(
                    allowed,
                    SinkStatus::can_transition(from_status.as_ref(), &status),
                    "Bad can_transition: {from_status:?} -> {status:?}; expected allowed: {allowed:?}"
                );
            }
        }
    }

    // Auxiliary functions
    fn persist_cache() -> Arc<PersistClientCache> {
        Arc::new(PersistClientCache::new(
            PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
        ))
    }

    static PERSIST_LOCATION: Lazy<PersistLocation> = Lazy::new(|| PersistLocation {
        blob_uri: "mem://".to_owned(),
        consensus_uri: "mem://".to_owned(),
    });

    async fn new_healthchecker(
        status_shard_id: ShardId,
        source_id: GlobalId,
        persist_clients: &Arc<PersistClientCache>,
    ) -> Healthchecker {
        let start = tokio::time::Instant::now();
        let now_fn = NowFn::from(move || u64::try_from(start.elapsed().as_millis()).unwrap());

        Healthchecker::new(
            source_id,
            persist_clients,
            (*PERSIST_LOCATION).clone(),
            status_shard_id,
            now_fn,
        )
        .await
        .expect("error creating healthchecker")
    }

    async fn simple_healthchecker(
        status_shard_id: ShardId,
        source_id: u64,
        persist_clients: &Arc<PersistClientCache>,
    ) -> Healthchecker {
        new_healthchecker(
            status_shard_id,
            GlobalId::User(source_id),
            &Arc::clone(persist_clients),
        )
        .await
    }

    async fn dump_storage_collection(
        shard_id: ShardId,
        persist_clients: &Arc<PersistClientCache>,
    ) -> Vec<Row> {
        let persist_client = persist_clients
            .open((*PERSIST_LOCATION).clone())
            .await
            .unwrap();

        let (write_handle, mut read_handle) = persist_client
            .open(
                shard_id,
                "tests::dump_storage_collection",
                Arc::new(MZ_SINK_STATUS_HISTORY_DESC.clone()),
                Arc::new(UnitSchema),
            )
            .await
            .unwrap();

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
