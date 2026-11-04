// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Retirement races with an applier whose Persist state cache is still live.

use std::time::Duration;

use mz_persist_client::PersistLocation;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::RetryParameters;
use timely::progress::Antichain;

use crate::operator::DataSubscribeTask;
use crate::tests::{reader, writer};
use crate::txn_cache::TxnsCache;
use crate::txn_write::CommitError;
use crate::txns::TxnsHandle;

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // too slow
async fn register_forget_apply_discovers_retirement_at_cas() {
    stale_apply_after_retirement(false).await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // too slow
async fn batch_apply_discovers_retirement_at_cas() {
    stale_apply_after_retirement(true).await;
}

async fn stale_apply_after_retirement(write_retired: bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut clients = PersistClientCache::new_no_metrics();
        clients
            .cfg()
            .set_next_listen_batch_retryer(RetryParameters {
                fixed_sleep: Duration::ZERO,
                initial_backoff: Duration::from_millis(1),
                multiplier: 1,
                clamp: Duration::from_millis(1),
            });
        let location = PersistLocation::new_in_mem();
        let peer_client = clients.open(location.clone()).await.unwrap();
        let mut peer = TxnsHandle::expect_open(peer_client.clone()).await;
        let txns_id = peer.txns_id();
        let ids = peer.expect_registers(1, 2).await;
        let (retired, live) = (ids[0], ids[1]);
        let mut retired_read = reader(&peer_client, retired).await;
        let _live_read = reader(&peer_client, live).await;

        // Reuse the blob/consensus backends, but not the Persist state cache.
        // Cloning peer_client would share uppers and miss the CAS error path.
        // new_no_metrics also disables pubsub between these two clients.
        clients.clear_state_cache();
        let stale_client = clients.open(location).await.unwrap();
        let mut stale = TxnsHandle::expect_open_id(stale_client.clone(), txns_id).await;
        stale.adopt([
            writer(&stale_client, retired).await,
            writer(&stale_client, live).await,
        ]);
        // Cache apply handles and consume the initial Register. Neither data
        // shard is written through this client before the retirement race.
        let _ = stale.apply_le(&1).await;

        let mut forbidden = stale.begin();
        forbidden.write(&retired, "forbidden".into(), (), 1).await;
        forbidden
            .write(&live, "must-not-commit".into(), (), 1)
            .await;

        let mut txn = peer.begin();
        txn.write(&live, "live-before".into(), (), 1).await;
        if write_retired {
            txn.write(&retired, "retired-before".into(), (), 1).await;
        }
        let _ = txn.commit_at(&mut peer, 2).await.unwrap();
        // Forget must apply committed batches before physical closure is legal.
        // Keep its tidy uncommitted so the stale applier still has work to do.
        let peer_tidy = peer.forget(3, [retired]).await.unwrap();
        let _ = stale.txns_cache.update_gt(&3).await;
        let mut listen = DataSubscribeTask::new(peer_client.clone(), txns_id, live, 2).await;
        assert!(listen.step_past(3).await > 3);

        let mut closer = writer(&peer_client, retired).await;
        assert_eq!(closer.shared_upper(), Antichain::from_elem(4));
        closer
            .compare_and_append_batch(&mut [], Antichain::from_elem(4), Antichain::new(), true)
            .await
            .unwrap()
            .unwrap();
        assert!(closer.shared_upper().is_empty());

        // This is the already-cached apply handle, not a newly opened handle
        // that could discover closure before entering empty_caa/apply_caa.
        // With no pubsub or data reads on stale_client, only its CAS can learn
        // the empty upper. No scheduler timing or production hook is needed.
        let data_write = stale.datas.take_write_for_apply(&retired).await;
        assert_eq!(data_write.shared_upper(), Antichain::from_elem(2));
        stale.datas.put_write_for_apply(data_write);
        // Without a retired batch, the first attempted CAS fills Forget at 3.
        // Otherwise it applies the batch at 2 before visiting Forget at 3.
        let stale_tidy = stale.apply_le(&3).await;
        let data_write = stale.datas.take_write_for_apply(&retired).await;
        assert!(data_write.shared_upper().is_empty());
        stale.datas.put_write_for_apply(data_write);

        // A retained local writer and a transaction buffered before Forget do
        // not confer write authority. Reject the whole cross-shard transaction.
        assert_eq!(
            forbidden.commit_at(&mut stale, 4).await.unwrap_err(),
            CommitError::ShardNotRegistered(retired)
        );
        // Reusing 4 also proves rejection did not advance the WAL.
        let mut txn = stale.begin();
        txn.write(&live, "live-after".into(), (), 1).await;
        txn.tidy(stale_tidy);
        let _ = txn
            .commit_at(&mut stale, 4)
            .await
            .unwrap()
            .apply(&mut stale)
            .await;
        peer.tidy_at(5, peer_tidy).await.unwrap();

        let mut observer = TxnsCache::expect_open(0, &peer).await;
        assert_eq!(
            observer.expect_snapshot(&peer_client, live, 5).await,
            vec!["live-after", "live-before"]
        );
        assert!(listen.step_past(5).await > 5);
        let mut updates = listen.finish().await;
        updates.sort();
        assert_eq!(
            updates,
            vec![("live-after".into(), 4, 1), ("live-before".into(), 2, 1)]
        );

        // A redundant apply must neither duplicate nor delete the retired
        // batch's data. Keep a since hold while closing its write frontier.
        let snapshot = retired_read
            .snapshot_and_fetch(Antichain::from_elem(2))
            .await
            .unwrap();
        let expected = if write_retired {
            vec![(("retired-before".into(), ()), 2, 1)]
        } else {
            vec![]
        };
        assert_eq!(snapshot, expected);
    })
    .await
    .expect("retirement, stale application, and live reads make progress");
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // too slow
#[should_panic(expected = "txns shard should not be closed")]
async fn initialization_rejects_wal_closure_discovered_at_cas() {
    use crate::TxnsCodecDefault;
    use mz_persist_client::{Diagnostics, ShardId};
    use mz_persist_types::txn::TxnsCodec;
    use std::sync::Arc;

    let mut clients = PersistClientCache::new_no_metrics();
    let location = PersistLocation::new_in_mem();
    let peer_client = clients.open(location.clone()).await.unwrap();
    let mut peer = TxnsHandle::expect_open(peer_client).await;
    let txns_id = peer.txns_id();
    clients.clear_state_cache();
    let stale_client = clients.open(location).await.unwrap();
    let (key, val) = TxnsCodecDefault::schemas();
    let (mut stale_write, stale_read) = stale_client
        .open::<ShardId, Vec<u8>, u64, i64>(
            txns_id,
            Arc::new(key),
            Arc::new(val),
            Diagnostics::for_tests(),
            true,
        )
        .await
        .unwrap();
    peer.txns_write
        .compare_and_append_batch(&mut [], Antichain::from_elem(1), Antichain::new(), true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stale_write.shared_upper(), Antichain::from_elem(1));
    let _ = TxnsCache::<u64, TxnsCodecDefault>::init(1, stale_read, &mut stale_write).await;
}
