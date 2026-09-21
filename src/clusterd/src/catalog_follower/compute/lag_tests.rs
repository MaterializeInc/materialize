// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Native live-window coverage with a lagging Persist input, not full M2 acceptance.
//!
//! Times are milliseconds, so 50/100-second uppers exercise a ten-second policy.
//! This module uses only the normal follower, catalog, query, and Persist APIs,
//! apart from the explicitly test-gated blob-cache interception helper.

#![cfg(test)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use mz_catalog::catalog::{Catalog, CatalogError, Op};
use mz_catalog::durable::DurableCatalogError;
use mz_catalog::memory::error::ErrorKind;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::ComputeClient;
use mz_ore::bytes::SegmentedBytes;
use mz_persist::location::{Blob, BlobMetadata, ExternalError};
use mz_persist_client::ShardId;
use mz_repr::{Datum, GlobalId, Row, Timestamp};
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use tokio::sync::{Notify, watch};
use tokio::time::timeout;
use uuid::Uuid;

use super::tests::{Fixture, assert_rows, disable_inline_parts, in_child, publish, start_runtime};

#[mz_ore::test(tokio::test)]
async fn lagging_index_admits_new_historical_reader() {
    const CHILD: &str = "MZ_CLUSTERD_LAGGING_INDEX_TEST_CHILD";
    const TEST: &str =
        "catalog_follower::compute::lag_tests::lagging_index_admits_new_historical_reader";
    if !in_child(CHILD, TEST, Duration::from_secs(180)).await {
        return;
    }

    let Fixture {
        clients,
        persist: _persist,
        writer: _writer,
        mut observer,
        store: _store,
        config,
        source,
        index,
        desc,
        shard,
        mut input,
    } = Fixture::new((1, 45_000), 50_000, true).await;
    // The writer and catalog observer already hold the unwrapped backend. Only
    // clients opened for the runtime below see the gate, so write maintenance
    // cannot be the operation that satisfies the blocked-GET notification.
    let gate = Arc::new(SourceGate::new(shard));
    clients
        .intercept_blob_for_tests(config.persist_location.blob_uri.clone(), {
            let gate = Arc::clone(&gate);
            move |blob| Arc::new(GatedBlob { blob, gate })
        })
        .await
        .unwrap();

    // Snapshot the non-inline batch configuration before runtime configuration
    // updates can change the shared dyncfg set. No data is added or PUT yet.
    disable_inline_parts(&clients);
    let mut next_batch = input.builder(Antichain::from_elem(Timestamp::new(50_000)));
    let factory = start_runtime(config, Arc::clone(&clients)).await;

    let replica = wait_window(&mut observer, source, index, 50_000).await;
    assert_only_replica_grants(&observer, replica, source, index);

    // HelloQuery reports a frontier snapshot without acquiring a peek hold.
    // Initial window publication alone does not establish hydration.
    let mut query = timeout(Duration::from_secs(30), async {
        loop {
            let mut query = factory();
            let upper = query_upper(&mut *query, index).await;
            if upper == Antichain::from_elem(Timestamp::new(50_000)) {
                break query;
            }
            drop(query);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("index must hydrate to 50 without query holds");

    gate.closed.send_replace(true);
    next_batch
        .add(
            &SourceData(Ok(Row::pack_slice(&[Datum::Int64(2)]))),
            &(),
            &Timestamp::new(95_000),
            &1,
        )
        .await
        .unwrap();
    let mut next_batch = next_batch
        .finish(Antichain::from_elem(Timestamp::new(100_000)))
        .await
        .unwrap();
    input
        .compare_and_append_batch(
            &mut [&mut next_batch],
            Antichain::from_elem(Timestamp::new(50_000)),
            Antichain::from_elem(Timestamp::new(100_000)),
            true,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        input.fetch_recent_upper().await,
        &Antichain::from_elem(Timestamp::new(100_000)),
    );
    timeout(Duration::from_secs(30), gate.blocked.notified())
        .await
        .expect("runtime must attempt a source batch GET");

    // Durable recovery progress proves the follower has observed upper 100.
    // The source's object requirement no longer protects the old window, and
    // no reader exists yet. Only the live replica window can preserve it.
    timeout(Duration::from_secs(30), async {
        let mut poll = tokio::time::interval(Duration::from_millis(20));
        loop {
            poll.tick().await;
            observer.sync_to_current_updates().await.unwrap();
            assert_window(&observer, replica, source, index, 50_000);
            assert_only_replica_grants(&observer, replica, source, index);
            if observer.state().maintained_read_requirements()[&source].frontier
                == Some(Timestamp::new(99_999))
            {
                assert_eq!(
                    observer.state().collection_compaction_bounds()[&source],
                    Antichain::from_elem(Timestamp::new(40_000))
                );
                break;
            }
        }
    })
    .await
    .expect("follower must publish durable recovery progress with source GET blocked");

    let mut probe = factory();
    assert_eq!(
        query_upper(&mut *probe, index).await,
        Antichain::from_elem(Timestamp::new(50_000)),
        "running index must still lag durable source upper 100",
    );
    drop(probe);

    // This incarnation and its first grant are created only after lag is
    // established. Neither hydration nor the stalled interval has query holds.
    let reader = create_reader(&mut observer).await;
    publish(
        &mut observer,
        reader,
        BTreeMap::from([(index, Timestamp::new(45_000))]),
    )
    .await
    .unwrap();
    for id in [index, source] {
        assert_eq!(
            observer.state().client_read_requirements()[&(reader, id)],
            Timestamp::new(45_000),
        );
    }
    assert!(*gate.closed.borrow());
    assert_rows(&mut *query, index, &desc, 45_000, &[1]).await;
    assert!(*gate.closed.borrow());

    gate.closed.send_replace(false);
    // Completion at 95 proves that the same running index consumes the released
    // input. The outstanding 45 grant still protects old rows across catch-up.
    assert_rows(&mut *query, index, &desc, 95_000, &[1, 2]).await;
    assert_rows(&mut *query, index, &desc, 45_000, &[1]).await;
    publish(&mut observer, reader, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        wait_window(&mut observer, source, index, 100_000).await,
        replica,
    );
    assert_only_replica_grants(&observer, replica, source, index);
    let rejected = publish(
        &mut observer,
        reader,
        BTreeMap::from([(index, Timestamp::new(45_000))]),
    )
    .await;
    assert!(
        matches!(rejected, Err(CatalogError::Catalog(ref error))
        if matches!(error.kind, ErrorKind::Durable(DurableCatalogError::InvalidReadProtection(_)))),
        "released history below the advanced window must be rejected: {rejected:?}",
    );
    std::process::exit(0);
}

// Batch keys are <shard>/<writer-or-version>/<part>. Rollup keys use a sequence
// number instead of a writer (v rather than w/n). Match only this shard's batch
// namespace, leaving catalog, expression cache, rollups, and all PUTs untouched.
#[derive(Debug)]
struct SourceGate {
    prefix: String,
    closed: watch::Sender<bool>,
    blocked: Notify,
}

impl SourceGate {
    fn new(shard: ShardId) -> Self {
        Self {
            prefix: format!("{shard}/"),
            closed: watch::channel(false).0,
            blocked: Notify::new(),
        }
    }

    async fn before_get(&self, key: &str) {
        let Some(suffix) = key.strip_prefix(&self.prefix) else {
            return;
        };
        if !suffix.starts_with(['w', 'n']) {
            return;
        }
        let mut closed = self.closed.subscribe();
        let is_closed = *closed.borrow_and_update();
        if is_closed {
            // notify_one retains a permit if the test has not started waiting.
            // watch retains release state and wakes every concurrent GET.
            self.blocked.notify_one();
            closed.wait_for(|closed| !closed).await.unwrap();
        }
    }
}

#[derive(Debug)]
struct GatedBlob {
    blob: Arc<dyn Blob>,
    gate: Arc<SourceGate>,
}

#[async_trait]
impl Blob for GatedBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        self.gate.before_get(key).await;
        self.blob.get(key).await
    }

    async fn list_keys_and_metadata(
        &self,
        prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.blob.list_keys_and_metadata(prefix, f).await
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        self.blob.set(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        self.blob.delete(key).await
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        self.blob.restore(key).await
    }
}

fn assert_only_replica_grants(catalog: &Catalog, replica: u64, source: GlobalId, index: GlobalId) {
    for (incarnation, id) in catalog.state().client_read_requirements().keys() {
        if *id == source || *id == index {
            assert_eq!(
                *incarnation, replica,
                "unexpected pre-existing reader grant"
            );
        }
    }
}

fn assert_window(catalog: &Catalog, replica: u64, source: GlobalId, index: GlobalId, upper: u64) {
    let state = catalog.state();
    let expected = state
        .index_read_policy(index)
        .unwrap()
        .frontier(Antichain::from_elem(Timestamp::new(upper)).borrow());
    assert_eq!(
        state.collection_compaction_bounds().get(&index),
        Some(&expected)
    );
    let floor = *expected.as_option().unwrap();
    for id in [index, source] {
        assert_eq!(
            state.client_read_requirements().get(&(replica, id)),
            Some(&floor)
        );
    }
}

async fn wait_window(catalog: &mut Catalog, source: GlobalId, index: GlobalId, upper: u64) -> u64 {
    timeout(Duration::from_secs(30), async {
        let mut poll = tokio::time::interval(Duration::from_millis(20));
        loop {
            poll.tick().await;
            catalog.sync_to_current_updates().await.unwrap();
            let state = catalog.state();
            let expected = state
                .index_read_policy(index)
                .unwrap()
                .frontier(Antichain::from_elem(Timestamp::new(upper)).borrow());
            let floor = *expected.as_option().unwrap();
            if state.collection_compaction_bounds().get(&index) == Some(&expected)
                && let Some((&incarnation, _)) =
                    state.client_incarnations().iter().find(|(id, _)| {
                        state
                            .client_read_requirements()
                            .contains_key(&(**id, index))
                    })
                && state.client_read_requirements().get(&(incarnation, index)) == Some(&floor)
                && state.client_read_requirements().get(&(incarnation, source)) == Some(&floor)
            {
                return incarnation;
            }
        }
    })
    .await
    .expect("replica must publish its live window and logical input grant")
}

async fn create_reader(catalog: &mut Catalog) -> u64 {
    timeout(Duration::from_secs(10), async {
        loop {
            catalog.sync_to_current_updates().await.unwrap();
            let ts = catalog.current_upper().await;
            match catalog
                .transact(None, ts, None, vec![Op::CreateClientIncarnation])
                .await
            {
                Err(CatalogError::Catalog(error))
                    if matches!(
                        error.kind,
                        ErrorKind::Durable(DurableCatalogError::CatalogOutOfSync { .. })
                    ) =>
                {
                    continue;
                }
                result => return result.unwrap().created_client_incarnations[0],
            }
        }
    })
    .await
    .expect("create fresh historical reader")
}

async fn query_upper(query: &mut dyn ComputeClient, index: GlobalId) -> Antichain<Timestamp> {
    timeout(Duration::from_secs(15), async {
        query
            .send(ComputeCommand::HelloQuery {
                nonce: Uuid::new_v4(),
            })
            .await
            .unwrap();
        assert!(matches!(
            query.recv().await.unwrap(),
            Some(ComputeResponse::QueryReady)
        ));
        loop {
            let response = query.recv().await.unwrap().expect("query connection");
            match response {
                ComputeResponse::Frontiers(id, frontiers) if id == index => {
                    return frontiers.write_frontier.expect("initial frontier snapshot");
                }
                ComputeResponse::Frontiers(..) => (),
                response => panic!("expected frontier snapshot: {response:?}"),
            }
        }
    })
    .await
    .expect("query handshake and index frontier snapshot")
}
