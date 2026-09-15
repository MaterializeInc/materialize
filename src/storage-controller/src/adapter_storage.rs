// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adapter-owned writers for webhook and statement-history collections.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use mz_ore::now::NowFn;
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::{IntrospectionType, MonotonicAppender};
use mz_storage_client::statistics::{ControllerSourceStatistics, WebhookStatistics};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use tokio::sync::watch;

use crate::StorageError;
use crate::collection_mgmt::CollectionManager;
use crate::statistics::{self, WebhookStatisticsState};

/// Appends request-side data and advances idle collection uppers while the adapter runs.
///
/// The adapter registers handles from committed metadata. Idle progress pauses
/// when no adapter owns a writer. Read-only writers neither append nor tick,
/// except for explicitly writable migrated system collections.
#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub struct AdapterStorageWriter {
    collections: CollectionManager,
    history_ids: Mutex<BTreeMap<IntrospectionType, GlobalId>>,
    statistics: Arc<Mutex<WebhookStatisticsState>>,
    statistics_interval: watch::Sender<Duration>,
    #[derivative(Debug = "ignore")]
    statistics_tokens: Arc<Mutex<BTreeMap<GlobalId, Box<dyn Any + Send + Sync>>>>,
}

impl AdapterStorageWriter {
    /// Creates a writer with a fixed read-only mode and the supplied clock.
    pub fn new(read_only: bool, now: NowFn) -> Self {
        Self {
            collections: CollectionManager::new(read_only, now),
            history_ids: Mutex::new(BTreeMap::new()),
            statistics: Arc::new(Mutex::new(WebhookStatisticsState {
                source_statistics: BTreeMap::new(),
                webhook_statistics: BTreeMap::new(),
            })),
            statistics_interval: watch::channel(StorageParameters::default().statistics_interval).0,
            statistics_tokens: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Registers a webhook collection for appends and automatic idle progress.
    pub fn register_webhook(
        &self,
        id: GlobalId,
        handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    ) {
        self.statistics
            .lock()
            .expect("poisoned")
            .webhook_statistics
            .entry(id)
            .or_default();
        self.collections
            .register_append_only_collection(id, handle, false, None);
    }

    /// Registers a statement-history collection. Migrated system shards may
    /// advance even when the adapter is read-only.
    pub fn register_history(
        &self,
        typ: IntrospectionType,
        id: GlobalId,
        handle: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        force_writable: bool,
    ) {
        assert!(typ.is_statement_history());
        assert!(!force_writable || id.is_system());
        let mut ids = self.history_ids.lock().expect("poisoned");
        assert!(
            ids.insert(typ, id).is_none(),
            "history type already registered"
        );
        // Statement histories have no bootstrap pruning or status deduplication.
        self.collections
            .register_append_only_collection(id, handle, force_writable, None);
    }

    /// Enqueues history updates with the shared append-only writer's batching.
    /// The adapter must be writable and the type must have been registered from
    /// committed collection metadata, otherwise this panics.
    pub fn append_history(&self, typ: IntrospectionType, updates: Vec<(Row, Diff)>) {
        let ids = self.history_ids.lock().expect("poisoned");
        let id = ids[&typ];
        self.collections
            .blind_write(id, updates.into_iter().map(Into::into).collect());
    }

    /// Returns an appender, or an identifier-missing error for an unregistered ID.
    pub fn monotonic_appender(&self, id: GlobalId) -> Result<MonotonicAppender, StorageError> {
        self.collections.monotonic_appender(id)
    }

    /// Removes the collection and returns a future that waits for its worker to stop.
    ///
    /// Await completion before releasing resources that require the writer to have
    /// stopped. Queued appends may be refused during shutdown.
    pub fn unregister(&self, id: GlobalId) -> BoxFuture<'static, ()> {
        self.history_ids
            .lock()
            .expect("poisoned")
            .retain(|_, gid| *gid != id);
        let mut statistics = self.statistics.lock().expect("poisoned");
        statistics.webhook_statistics.remove(&id);
        statistics.source_statistics.remove(&(id, None));
        self.statistics_tokens.lock().expect("poisoned").remove(&id);
        self.collections.unregister_collection(id)
    }

    /// Updates the batching duration for user collections, including registered ones.
    pub fn update_user_batch_duration(&self, duration: Duration) {
        self.collections.update_user_batch_duration(duration);
    }

    /// Applies adapter-local batching and statistics cadence.
    pub fn update_parameters(&self, parameters: &StorageParameters) {
        self.update_user_batch_duration(parameters.user_storage_managed_collections_batch_duration);
        self.statistics_interval.send_if_modified(|interval| {
            if *interval == parameters.statistics_interval {
                return false;
            }
            *interval = parameters.statistics_interval;
            true
        });
    }

    /// Returns request counters for a committed, registered webhook.
    pub fn statistics(&self, id: GlobalId) -> Result<Arc<WebhookStatistics>, StorageError> {
        self.statistics
            .lock()
            .expect("poisoned")
            .webhook_statistics
            .get(&id)
            .cloned()
            .ok_or(StorageError::IdentifierMissing(id))
    }

    /// Owns the webhook partition of the persisted source statistics relation.
    ///
    /// Call after registering the complete committed webhook inventory, even if
    /// empty. Restoration keeps only live IDs, so it cannot resurrect a concurrent
    /// drop. Replica statistics have a non-NULL replica ID and remain independently
    /// owned by lifecycle publication.
    pub async fn initialize_statistics(
        &self,
        persist: PersistClient,
        id: GlobalId,
        metadata: CollectionMetadata,
        retention: Duration,
        force_writable: bool,
    ) {
        assert!(
            metadata.txns_shard.is_none(),
            "statistics are not WAL tables"
        );
        self.statistics_tokens
            .lock()
            .expect("poisoned")
            .insert(id, Box::new(()));
        let desc = Arc::new(metadata.relation_desc);
        let shard = metadata.data_shard;
        let writer = persist
            .open_writer(
                shard,
                Arc::clone(&desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: id.to_string(),
                    handle_purpose: "webhook statistics".into(),
                },
            )
            .await
            .expect("statistics schema matches committed metadata");
        let read_handle = move || {
            let persist = persist.clone();
            let desc = Arc::clone(&desc);
            async move {
                persist
                    .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
                        shard,
                        desc,
                        Arc::new(UnitSchema),
                        Diagnostics {
                            shard_name: id.to_string(),
                            handle_purpose: "webhook statistics recovery".into(),
                        },
                        USE_CRITICAL_SINCE_SNAPSHOT.get(persist.dyncfgs()),
                    )
                    .await
                    .expect("statistics schema matches committed metadata")
            }
            .boxed()
        };
        let initial_read = read_handle();
        let state = Arc::clone(&self.statistics);
        let tokens = Arc::clone(&self.statistics_tokens);
        let collections = self.collections.clone();
        let interval = *self.statistics_interval.borrow();
        let updates = self.statistics_interval.subscribe();
        let initialize = async move {
            let mut reader = initial_read.await;
            let upper = reader.shared_upper();
            let rows = match upper.as_option().and_then(|ts| ts.step_back()) {
                Some(as_of) => reader
                    .snapshot_and_fetch(Antichain::from_elem(as_of))
                    .await
                    .expect("statistics snapshot is readable"),
                None => Vec::new(),
            };
            reader.expire().await;
            {
                let mut state = state.lock().expect("poisoned");
                for ((row, ()), _, diff) in rows {
                    let row = row.0.expect("statistics contain no error rows");
                    if !owns_statistics(&row) {
                        continue;
                    }
                    assert_eq!(diff, 1, "statistics have unit multiplicity");
                    let (source, replica, stats) = ControllerSourceStatistics::from_row(row);
                    assert!(replica.is_none());
                    if state.webhook_statistics.contains_key(&source) {
                        state.source_statistics.insert((source, None), stats);
                    }
                }
            }
            let scraper = statistics::spawn_prepared_statistics_scraper(
                id,
                collections,
                Arc::clone(&state),
                Vec::new(),
                interval,
                updates.clone(),
                retention,
            );
            let drain = statistics::spawn_webhook_statistics_scraper(state, interval, updates);
            tokens
                .lock()
                .expect("poisoned")
                .insert(id, Box::new((scraper, drain)));
        }
        .boxed();
        self.collections.register_differential_writer(
            id,
            writer,
            read_handle,
            force_writable,
            owns_statistics,
            initialize,
        );
    }
}

impl Drop for AdapterStorageWriter {
    fn drop(&mut self) {
        // Preparation and scrapers hold manager clones. Explicitly unregister
        // tasks so dropping this owner also stops unfinished initialization.
        let statistics_ids = std::mem::take(&mut *self.statistics_tokens.lock().expect("poisoned"));
        let state = self.statistics.lock().expect("poisoned");
        let history_ids = self.history_ids.lock().expect("poisoned");
        for id in state
            .webhook_statistics
            .keys()
            .chain(statistics_ids.keys())
            .chain(history_ids.values())
        {
            drop(self.collections.unregister_collection(*id));
        }
    }
}

fn owns_statistics(row: &Row) -> bool {
    row.iter()
        .nth(1)
        .expect("statistics have a replica column")
        .is_null()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use mz_persist_client::read::ReadHandle;
    use mz_persist_client::{Diagnostics, PersistClient, ShardId};
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_repr::{Diff, RelationDesc, Row};
    use timely::progress::Antichain;

    use super::*;

    async fn handles() -> (
        WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        ReadHandle<SourceData, (), Timestamp, StorageDiff>,
    ) {
        PersistClient::new_for_tests()
            .await
            .open(
                ShardId::new(),
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
                false,
            )
            .await
            .unwrap()
    }

    #[mz_ore::test(tokio::test)]
    async fn statistics_restore_counters_and_reconcile_only_webhooks() {
        use mz_cluster_client::ReplicaId;
        use mz_storage_client::statistics::{MZ_SOURCE_STATISTICS_RAW_DESC, PackableStats};

        fn row(id: GlobalId, replica: Option<ReplicaId>, count: u64) -> Row {
            let counters = WebhookStatistics::default();
            counters.messages_received.store(count, Ordering::SeqCst);
            let mut stats = ControllerSourceStatistics::new(id, replica);
            stats.incorporate(counters.drain_into_update(id));
            let mut row = Row::default();
            stats.pack(row.packer());
            row
        }

        async fn expect_rows(
            reader: &mut ReadHandle<SourceData, (), Timestamp, StorageDiff>,
            expected: Vec<(String, bool, u64)>,
        ) {
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let upper = reader.shared_upper().into_option().unwrap();
                    let snapshot = reader
                        .snapshot_and_fetch(Antichain::from_elem(upper.step_back().unwrap()))
                        .await
                        .unwrap();
                    let mut actual = Vec::new();
                    for ((row, ()), _, diff) in snapshot {
                        assert_eq!(diff, 1);
                        let row = row.0.unwrap();
                        let mut fields = row.iter();
                        actual.push((
                            fields.next().unwrap().unwrap_str().to_owned(),
                            fields.next().unwrap().is_null(),
                            fields.next().unwrap().unwrap_uint64(),
                        ));
                    }
                    actual.sort();
                    if actual == expected {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        }

        let persist = PersistClient::new_for_tests().await;
        let stats_id = GlobalId::System(1);
        let live = GlobalId::User(1);
        let replica = GlobalId::User(2);
        let orphan = GlobalId::User(3);
        let shard = ShardId::new();
        let webhook_shard = ShardId::new();
        let (mut seed, mut reader) = persist
            .open::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Arc::new(MZ_SOURCE_STATISTICS_RAW_DESC.clone()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
                false,
            )
            .await
            .unwrap();
        let rows = [
            row(live, None, 7),
            row(replica, Some(ReplicaId::User(1)), 19),
            row(orphan, None, 11),
        ];
        let updates = rows
            .into_iter()
            .map(|row| ((SourceData(Ok(row)), ()), Timestamp::from(100), 1));
        seed.compare_and_append(
            updates,
            Antichain::from_elem(Timestamp::MIN),
            Antichain::from_elem(Timestamp::from(101)),
        )
        .await
        .unwrap()
        .unwrap();
        seed.expire().await;
        let metadata = CollectionMetadata {
            persist_location: mz_persist_client::PersistLocation::new_in_mem(),
            data_shard: shard,
            relation_desc: MZ_SOURCE_STATISTICS_RAW_DESC.clone(),
            txns_shard: None,
        };

        let read_only = AdapterStorageWriter::new(true, NowFn::from(|| 200));
        read_only
            .initialize_statistics(
                persist.clone(),
                stats_id,
                metadata.clone(),
                Duration::from_secs(60),
                false,
            )
            .await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(
            reader.shared_upper(),
            Antichain::from_elem(Timestamp::from(101))
        );
        drop(read_only);

        // Independent restarts retain cumulative counters. An empty committed
        // inventory still owns cleanup of the persisted webhook partition.
        for (increment, total) in [(Some(3), 10), (Some(2), 12), (None, 0)] {
            let writer = AdapterStorageWriter::new(false, NowFn::from(|| 200));
            writer.update_parameters(&StorageParameters {
                statistics_interval: Duration::from_millis(10),
                ..Default::default()
            });
            if let Some(increment) = increment {
                let handle = persist
                    .open_writer(
                        webhook_shard,
                        Arc::new(RelationDesc::empty()),
                        Arc::new(UnitSchema),
                        Diagnostics::for_tests(),
                    )
                    .await
                    .unwrap();
                writer.register_webhook(live, handle);
                writer
                    .statistics(live)
                    .unwrap()
                    .messages_received
                    .fetch_add(increment, Ordering::SeqCst);
            }
            writer
                .initialize_statistics(
                    persist.clone(),
                    stats_id,
                    metadata.clone(),
                    Duration::from_secs(60),
                    false,
                )
                .await;
            let mut expected = vec![(replica.to_string(), false, 19)];
            if increment.is_some() {
                expected.push((live.to_string(), true, total));
            }
            expected.sort();
            expect_rows(&mut reader, expected).await;
            drop(writer);
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn history_routing_and_migrated_read_only_ownership() {
        use IntrospectionType::*;

        for (read_only, force_writable) in [(false, false), (true, false), (true, true)] {
            let writer = AdapterStorageWriter::new(read_only, NowFn::from(|| 100));
            // Statistics preparation can keep the kernel alive beyond the owner.
            let _manager = writer.collections.clone();
            let mut readers = Vec::new();
            let mut appenders = Vec::new();
            for (n, typ) in [
                SessionHistory,
                PreparedStatementHistory,
                StatementExecutionHistory,
                StatementLifecycleHistory,
                SqlText,
            ]
            .into_iter()
            .enumerate()
            {
                let (handle, reader) = handles().await;
                let id = GlobalId::System(u64::try_from(n).unwrap() + 1);
                writer.register_history(typ, id, handle, force_writable);
                if !read_only {
                    writer.append_history(
                        typ,
                        vec![(Row::default(), Diff::from(i64::try_from(n).unwrap() + 1))],
                    );
                }
                readers.push(reader);
                appenders.push(writer.monotonic_appender(id).unwrap());
            }
            if !read_only || force_writable {
                for (n, reader) in readers.iter_mut().enumerate() {
                    let rows = tokio::time::timeout(
                        Duration::from_secs(10),
                        reader.snapshot_and_fetch(Antichain::from_elem(Timestamp::from(100))),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                    if read_only {
                        // Migrated shards advance while statement logging stays disabled.
                        assert!(rows.is_empty());
                    } else {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].0.0, SourceData(Ok(Row::default())));
                        assert_eq!(rows[0].2, i64::try_from(n).unwrap() + 1);
                    }
                }
            } else {
                for appender in &appenders {
                    assert!(matches!(
                        appender
                            .append(vec![(Row::default(), Diff::ONE).into()])
                            .await,
                        Err(StorageError::ReadOnly)
                    ));
                }
                for reader in &readers {
                    assert_eq!(reader.shared_upper(), Antichain::from_elem(Timestamp::MIN));
                }
            }
            drop(writer);
            for appender in appenders {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        if matches!(
                            appender.append(Vec::new()).await,
                            Err(StorageError::ShuttingDown(_))
                        ) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn append_and_idle_progress() {
        let (handle, mut reader) = handles().await;
        let clock = Arc::new(AtomicU64::new(100));
        let now = Arc::clone(&clock);
        let writer =
            AdapterStorageWriter::new(false, NowFn::from(move || now.load(Ordering::SeqCst)));
        let id = GlobalId::User(1);
        writer.register_webhook(id, handle);
        writer.update_user_batch_duration(Duration::ZERO);
        tokio::time::timeout(
            Duration::from_secs(10),
            writer
                .monotonic_appender(id)
                .unwrap()
                .append(vec![(Row::default(), Diff::ONE).into()]),
        )
        .await
        .unwrap()
        .unwrap();

        // Moving the clock without another append must make this snapshot readable.
        clock.store(200, Ordering::SeqCst);
        let snapshot = tokio::time::timeout(
            Duration::from_secs(10),
            reader.snapshot_and_fetch(Antichain::from_elem(Timestamp::from(200))),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(snapshot.len(), 1);
        let ((data, ()), _, diff) = &snapshot[0];
        assert_eq!(data, &SourceData(Ok(Row::default())));
        assert_eq!(*diff, 1);
        writer.unregister(id).await;
    }

    #[mz_ore::test(tokio::test)]
    async fn read_only_refuses_appends_and_idle_progress() {
        let (handle, reader) = handles().await;
        let writer = AdapterStorageWriter::new(true, NowFn::from(|| 100));
        let id = GlobalId::User(1);
        writer.register_webhook(id, handle);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            writer
                .monotonic_appender(id)
                .unwrap()
                .append(vec![(Row::default(), Diff::ONE).into()]),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(StorageError::ReadOnly)));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(reader.shared_upper(), Antichain::from_elem(Timestamp::MIN));
        writer.unregister(id).await;
    }

    #[mz_ore::test(tokio::test)]
    async fn unregister_stops_existing_appenders() {
        let (handle, reader) = handles().await;
        let writer = AdapterStorageWriter::new(false, NowFn::from(|| 100));
        let id = GlobalId::User(1);
        writer.register_webhook(id, handle);
        let appender = writer.monotonic_appender(id).unwrap();
        // Enqueue a request without yielding to the worker, so shutdown must
        // resolve it rather than leaving its caller waiting.
        let append = appender.append(vec![(Row::default(), Diff::ONE).into()]);
        tokio::pin!(append);
        assert!(futures::poll!(&mut append).is_pending());
        tokio::time::timeout(Duration::from_secs(10), writer.unregister(id))
            .await
            .unwrap();
        assert!(matches!(
            append.await,
            Err(StorageError::IdentifierInvalid(actual)) if actual == id
        ));
        assert!(matches!(
            writer.monotonic_appender(id),
            Err(StorageError::IdentifierMissing(actual)) if actual == id
        ));
        assert!(matches!(
            appender
                .append(vec![(Row::default(), Diff::ONE).into()])
                .await,
            Err(StorageError::ShuttingDown(_))
        ));
        let upper = reader.shared_upper();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(reader.shared_upper(), upper);
        writer.unregister(id).await;
    }
}
