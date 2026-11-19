// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Competing startup pruners against a shared WAL. One test owns the process-global hooks.

#![recursion_limit = "256"]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use mz_catalog::builtin::{
    BuiltinTable, MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY, MZ_STORAGE_USAGE_BY_SHARD,
};
use mz_catalog::durable::{CatalogError, DurableCatalogError, persist_backed_catalog_join_active};
use mz_environmentd::test_util::TestHarness;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_persist_client::critical::Opaque;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::{Diagnostics, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, Row, Timestamp};
use mz_storage_client::controller::StorageTxn;
use mz_storage_types::controller::TxnsCodecRow;
use mz_storage_types::sources::SourceData;
use mz_txn_wal::txn_read::TxnsRead;
use mz_txn_wal::txn_write::CommitError;
use mz_txn_wal::txns::TxnsHandle;

const STORAGE: &str = "storage_usage_prune_before_submit";
const ARRANGEMENT: &str = "arrangement_sizes_prune_before_submit";
const COMPLETED: &str = "builtin_history_prune_completed";
const DEADLINE: Duration = Duration::from_secs(180);

struct Hooks {
    resume: Option<mpsc::Sender<()>>,
}

impl Drop for Hooks {
    fn drop(&mut self) {
        for hook in [STORAGE, ARRANGEMENT, COMPLETED] {
            fail::remove(hook);
        }
        if let Some(resume) = self.resume.take() {
            let _ = resume.send(());
        }
    }
}

fn seed_row(storage: bool, expired: bool) -> Row {
    let timestamp = chrono::DateTime::from_timestamp_millis(if expired { 1 } else { 10_000_000 })
        .unwrap()
        .try_into()
        .unwrap();
    let marker = if expired {
        "prune-expired"
    } else {
        "prune-control"
    };
    if storage {
        Row::pack([
            Datum::UInt64(if expired { u64::MAX - 1 } else { u64::MAX }),
            Datum::String(marker),
            Datum::UInt64(123),
            Datum::TimestampTz(timestamp),
        ])
    } else {
        Row::pack([
            Datum::String("prune-replica"),
            Datum::String(marker),
            Datum::Int64(123),
            Datum::TimestampTz(timestamp),
            Datum::True,
            Datum::Null,
        ])
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
#[allow(clippy::disallowed_methods)]
async fn test_competing_builtin_history_pruners() {
    // Run serially because the hooks have no adapter/table argument.
    for (table, hook, storage) in [
        (&*MZ_STORAGE_USAGE_BY_SHARD, STORAGE, true),
        (&*MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY, ARRANGEMENT, false),
    ] {
        tokio::time::timeout(DEADLINE, competing_pruners(table, hook, storage))
            .await
            .expect("history pruning rendezvous timed out");
    }
}

#[allow(clippy::disallowed_methods)]
async fn competing_pruners(table: &BuiltinTable, hook: &'static str, storage: bool) {
    let harness = TestHarness::default()
        .with_now(NowFn::from(|| 1_000_000))
        .with_storage_usage_collection_interval(Duration::from_secs(3600))
        .with_storage_usage_retention_period(if storage {
            Duration::from_secs(100)
        } else {
            Duration::MAX
        })
        .with_system_parameter_default(
            "arrangement_size_history_retention_period".into(),
            "100s".into(),
        );
    // The non-target table has no expired rows: arrangement collection is off,
    // and storage retention is unlimited when testing arrangement pruning.
    // Only the target therefore produces a write-completion notification.
    let mut hooks = Hooks { resume: None };
    let (initial_tx, mut initial_rx) = tokio::sync::mpsc::unbounded_channel();
    for hook in [STORAGE, ARRANGEMENT] {
        let initial_tx = initial_tx.clone();
        fail::cfg_callback(hook, move || {
            let _ = initial_tx.send(());
        })
        .unwrap();
    }
    let seed_server = harness.clone().start().await;
    // Both initial snapshots precede seeding. Do not let a delayed initial
    // task consume A's rendezvous or a later completion notification.
    for _ in 0..2 {
        initial_rx.recv().await.unwrap();
    }
    fail::remove(STORAGE);
    fail::remove(ARRANGEMENT);
    let deployment = seed_server.deployment();
    let client = seed_server.connect().internal().await.unwrap();
    client
        .batch_execute("CREATE ADAPTER prune_peer (SIZE '1')")
        .await
        .unwrap();
    let oracle = seed_server.timestamp_oracle().await;
    let persist = seed_server
        .persist_clients
        .open(seed_server.persist_location.clone())
        .await
        .unwrap();
    let mut catalog = persist_backed_catalog_join_active(
        persist.clone(),
        seed_server.environment_id.organization_id(),
        mz_environmentd::BUILD_INFO.semver_version(),
        Arc::new(mz_catalog::durable::Metrics::new(&MetricsRegistry::new())),
        Some(seed_server.catalog_timestamp_oracle().await),
    )
    .await
    .unwrap();
    let (wal_id, shard, peer_id) = loop {
        catalog.sync_to_current_updates().await.unwrap();
        let txn = match catalog.transaction().await {
            Ok(txn) => txn,
            Err(CatalogError::Durable(DurableCatalogError::CatalogOutOfSync { .. })) => continue,
            Err(error) => panic!("history fixture catalog: {error}"),
        };
        let id = txn
            .get_system_object_mappings()
            .find(|mapping| {
                mapping.description.schema_name == table.schema
                    && mapping.description.object_name == table.name
            })
            .unwrap()
            .unique_identifier
            .global_id;
        let peer_id = txn
            .get_adapter_declarations()
            .find(|a| a.name == "prune_peer")
            .unwrap()
            .id;
        break (
            txn.get_txn_wal_shard().unwrap(),
            txn.get_collection_metadata()[&id],
            peer_id,
        );
    };
    let peer_harness = harness.clone().with_adapter_id(peer_id);
    let (writer, mut reader) = persist
        .open::<SourceData, (), Timestamp, i64>(
            shard,
            Arc::new(table.desc.clone()),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
            false,
        )
        .await
        .unwrap();
    // Keep this leased reader for the entire race, protecting the snapshot cut
    // even when another adapter advances collection compaction.
    let mut wal: TxnsHandle<SourceData, (), Timestamp, i64, TxnsCodecRow> = TxnsHandle::open(
        oracle.read_ts().await,
        persist.clone(),
        persist.dyncfgs().clone(),
        Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new())),
        wal_id,
        Opaque::encode(&0u64),
    )
    .await;
    wal.adopt([writer]);
    let expired = seed_row(storage, true);
    let control = seed_row(storage, false);
    let mut txn = wal.begin();
    for row in [&expired, &control] {
        txn.write(&shard, SourceData(Ok(row.clone())), (), 1).await;
    }
    loop {
        let ts = oracle.write_ts().await.timestamp;
        catalog.advance_upper(ts.step_forward()).await.unwrap();
        match txn.commit_at(&mut wal, ts).await {
            Ok(apply) => {
                apply.apply(&mut wal).await;
                oracle.apply_write(ts).await;
                break;
            }
            Err(CommitError::UpperMismatch(_)) => tokio::task::yield_now().await,
            Err(error) => panic!("seeding registered history table: {error}"),
        }
    }
    drop(wal);
    drop(catalog);
    let txns = TxnsRead::start::<TxnsCodecRow>(persist, wal_id).await;
    assert_eq!(
        snapshot(&txns, shard, &mut reader, oracle.read_ts().await, &control)
            .await
            .get(&expired),
        Some(&1)
    );
    drop(client);
    drop(seed_server);

    let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    hooks.resume = Some(resume_tx);
    let rendezvous = Mutex::new(Some((parked_tx, resume_rx)));
    fail::cfg_callback(hook, move || {
        let Some((parked, resume)) = rendezvous.lock().unwrap().take() else {
            return;
        };
        tokio::task::block_in_place(|| {
            let _ = parked.send(());
            let _ = resume.recv_timeout(DEADLINE * 2);
        });
    })
    .unwrap();
    let (completed_tx, mut completed_rx) = tokio::sync::mpsc::unbounded_channel();
    fail::cfg_callback(COMPLETED, move || {
        // Never block the completion path.
        let _ = completed_tx.send(());
    })
    .unwrap();
    let a = harness.clone().start_in(Arc::clone(&deployment)).await;
    // Drop order matters on panic: release the blocked task before its server.
    let mut hooks = hooks;
    parked_rx.await.expect("A must prepare its stale candidate");
    assert_eq!(
        snapshot(&txns, shard, &mut reader, oracle.read_ts().await, &control)
            .await
            .get(&expired),
        Some(&1)
    );

    // Startup/status/keepalive writes may invalidate B's candidate. Retry B's
    // actual startup, not its retractions, while retaining A's original snapshot.
    let mut b = None;
    for attempt in 0..20 {
        let peer = peer_harness.clone().start_in(Arc::clone(&deployment)).await;
        completed_rx.recv().await.expect("B pruning completion");
        if !snapshot(&txns, shard, &mut reader, oracle.read_ts().await, &control)
            .await
            .contains_key(&expired)
        {
            b = Some(peer);
            break;
        }
        drop(peer);
        assert!(
            attempt < 19,
            "B never pruned the expired row after 20 startup attempts"
        );
    }
    let b = b.unwrap();
    hooks.resume.take().unwrap().send(()).unwrap();
    completed_rx
        .recv()
        .await
        .expect("A pruning completion, including rejected candidates");
    assert!(
        !snapshot(&txns, shard, &mut reader, oracle.read_ts().await, &control)
            .await
            .contains_key(&expired)
    );

    for server in [&a, &b] {
        let client = server.connect().internal().await.unwrap();
        let column = if storage { "shard_id" } else { "object_id" };
        let query = format!(
            "SELECT {column} FROM mz_internal.{} WHERE {column} IN ('prune-expired', 'prune-control') ORDER BY 1",
            table.name
        );
        let rows = client.query(&query, &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, String>(0), "prune-control");
    }
    drop(b);
    drop(a);
    // An empty candidate has no write completion callback. On restart, fence
    // the snapshot validation itself and then verify public reads still work.
    let (restart_tx, mut restart_rx) = tokio::sync::mpsc::unbounded_channel();
    fail::cfg_callback(hook, move || {
        let _ = restart_tx.send(());
    })
    .unwrap();
    let restarted = harness.start_in(Arc::clone(&deployment)).await;
    restart_rx
        .recv()
        .await
        .expect("restart snapshot validation");
    let client = restarted.connect().internal().await.unwrap();
    let column = if storage { "shard_id" } else { "object_id" };
    let query = format!(
        "SELECT {column} FROM mz_internal.{} WHERE {column} IN ('prune-expired', 'prune-control')",
        table.name
    );
    let rows = client.query(&query, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "prune-control");
    drop(client);
    assert!(
        !snapshot(&txns, shard, &mut reader, oracle.read_ts().await, &control)
            .await
            .contains_key(&expired)
    );
    reader.expire().await;
    drop(restarted);
    drop(hooks);
    drop(deployment);
}

async fn snapshot(
    txns: &TxnsRead<Timestamp>,
    shard: ShardId,
    reader: &mut ReadHandle<SourceData, (), Timestamp, i64>,
    ts: Timestamp,
    control: &Row,
) -> BTreeMap<Row, i64> {
    txns.update_gt(ts).await;
    let rows = txns
        .data_snapshot(shard, ts)
        .await
        .snapshot_and_fetch(reader)
        .await
        .unwrap();
    let mut consolidated = BTreeMap::<Row, i64>::new();
    for ((data, ()), _, diff) in rows {
        *consolidated.entry(data.0.unwrap()).or_default() += diff;
    }
    consolidated.retain(|_, diff| *diff != 0);
    assert!(
        consolidated.values().all(|diff| *diff > 0),
        "negative history multiplicity: {consolidated:?}"
    );
    assert_eq!(
        consolidated.get(control),
        Some(&1),
        "control row must survive"
    );
    consolidated
}
