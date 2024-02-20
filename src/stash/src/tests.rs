// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::time::Duration;

use crate::transaction::MAX_INSERT_ARGUMENTS;
use crate::{
    AppendBatch, Data, DebugStashFactory, Stash, StashCollection, StashError, StashFactory,
    TableTransaction, Timestamp, TypedCollection, INSERT_BATCH_SPLIT_SIZE,
};
use futures::Future;
use mz_ore::assert_contains;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::task::spawn;
use timely::progress::Antichain;
use tokio::sync::oneshot;
use tokio_postgres::Config;

static C1: TypedCollection<i64, i64> = TypedCollection::new("c1");
static C_SAVEPOINT: TypedCollection<i64, i64> = TypedCollection::new("c_savepoint");

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_invalid_url() {
    let tls = mz_tls_util::make_tls(&Config::new()).unwrap();
    let factory = StashFactory::new(&MetricsRegistry::new());

    // Verify invalid URLs fail on connect.
    assert!(factory
        .open("host=invalid".into(), None, tls, None)
        .await
        .unwrap_err()
        .to_string()
        .contains("stash error: postgres: error connecting to server"));
}

async fn test_append<F, O>(f: F) -> Stash
where
    O: Future<Output = Stash>,
    F: Fn() -> O,
{
    const TYPED: TypedCollection<String, String> = TypedCollection::new("typed");

    let mut stash = f().await;

    // Can't peek if since == upper.
    assert!(TYPED
        .peek_one(&mut stash)
        .await
        .unwrap_err()
        .to_string()
        .contains("since {-9223372036854775808} is not less than upper {-9223372036854775808}"));
    TYPED
        .upsert_key(&mut stash, "k1".to_string(), |_| {
            Ok::<_, Infallible>("v1".to_string())
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        TYPED.peek_one(&mut stash).await.unwrap(),
        BTreeMap::from([("k1".to_string(), "v1".to_string())])
    );
    TYPED
        .upsert_key(&mut stash, "k1".to_string(), |_| {
            Ok::<_, Infallible>("v2".to_string())
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        TYPED.peek_one(&mut stash).await.unwrap(),
        BTreeMap::from([("k1".to_string(), "v2".to_string())])
    );
    assert_eq!(
        TYPED
            .peek_key_one(&mut stash, "k1".to_string())
            .await
            .unwrap(),
        Some("v2".to_string()),
    );
    assert_eq!(
        TYPED
            .peek_key_one(&mut stash, "k2".to_string())
            .await
            .unwrap(),
        None
    );
    TYPED
        .upsert(
            &mut stash,
            vec![
                ("k1".to_string(), "v3".to_string()),
                ("k2".to_string(), "v4".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        TYPED.peek_one(&mut stash).await.unwrap(),
        BTreeMap::from([
            ("k1".to_string(), "v3".to_string()),
            ("k2".to_string(), "v4".to_string())
        ])
    );

    // Test append across collections.
    let orders = collection::<String, String>(&mut stash, "orders")
        .await
        .unwrap();
    let other = collection::<String, String>(&mut stash, "other")
        .await
        .unwrap();

    stash
        .with_transaction(move |tx| {
            Box::pin(async move {
                // Seal so we can invalidate the upper below.
                tx.seal(other.id, Antichain::from_elem(1), None)
                    .await
                    .unwrap();
                let mut orders_batch = orders.make_batch_tx(&tx).await.unwrap();
                orders.append_to_batch(&mut orders_batch, &"k1".to_string(), &"v1".to_string(), 1);
                let mut other_batch = other.make_batch_tx(&tx).await.unwrap();
                other.append_to_batch(&mut other_batch, &"k2".to_string(), &"v2".to_string(), 1);

                // Invalidate one upper and ensure append doesn't commit partial batches.
                let other_upper = other_batch.upper;
                other_batch.upper = Antichain::from_elem(Timestamp::MIN);
                let result = tx
                    .append(vec![orders_batch.clone(), other_batch.clone()])
                    .await;
                let Err(err) = result else {
                    panic!("unexpected success!")
                };
                assert_contains!(err.to_string(), "{-9223372036854775808}",);

                // Test batches in the other direction too.
                let result = tx
                    .append(vec![other_batch.clone(), orders_batch.clone()])
                    .await;
                let Err(err) = result else {
                    panic!("unexpected success!")
                };
                assert_contains!(err.to_string(), "{-9223372036854775808}",);

                // Fix the upper, append should work now.
                other_batch.upper = other_upper;
                drop(tx.append(vec![other_batch, orders_batch]).await.unwrap());
                assert_eq!(
                    tx.iter(orders).await.unwrap(),
                    &[(("k1".into(), "v1".into()), -9223372036854775808, 1),]
                );
                assert_eq!(
                    tx.iter(other).await.unwrap(),
                    &[(("k2".into(), "v2".into()), 1, 1),]
                );
                assert_eq!(
                    tx.peek_one(orders).await.unwrap(),
                    BTreeMap::from([("k1".to_string(), "v1".to_string())])
                );
                assert_eq!(
                    tx.peek_one(other).await.unwrap(),
                    BTreeMap::from([("k2".to_string(), "v2".to_string())])
                );

                // Verify the upper got bumped.
                assert_eq!(
                    tx.since(orders.id).await.unwrap().into_option().unwrap(),
                    tx.upper(orders.id).await.unwrap().into_option().unwrap() - 1
                );
                // Multiple empty batches should bump the upper and the since because append
                // must also compact and consolidate.
                for _ in 0..5 {
                    let orders_batch = orders.make_batch_tx(&tx).await.unwrap();
                    drop(tx.append(vec![orders_batch]).await.unwrap());
                    assert_eq!(
                        tx.since(orders.id).await.unwrap().into_option().unwrap(),
                        tx.upper(orders.id).await.unwrap().into_option().unwrap() - 1
                    );
                }
                Ok(())
            })
        })
        .await
        .unwrap();

    // Remake the stash and ensure data remains.
    let mut stash = f().await;
    stash
        .with_transaction(move |tx| {
            Box::pin(async move {
                assert_eq!(
                    tx.peek_one(orders).await.unwrap(),
                    BTreeMap::from([("k1".to_string(), "v1".to_string())])
                );
                assert_eq!(
                    tx.peek_one(other).await.unwrap(),
                    BTreeMap::from([("k2".to_string(), "v2".to_string())])
                );
                Ok(())
            })
        })
        .await
        .unwrap();

    // Remake again, mutate before reading, then read.
    let mut stash = f().await;
    stash
        .with_transaction(move |tx| {
            Box::pin(async move {
                tx.update_savepoint(
                    orders.id,
                    &[(("k3".to_string(), "v3".to_string()), 1, 1)],
                    None,
                )
                .await
                .unwrap();
                tx.seal(orders.id, Antichain::from_elem(2), None)
                    .await
                    .unwrap();

                assert_eq!(
                    tx.peek_one(orders).await.unwrap(),
                    BTreeMap::from([
                        ("k1".to_string(), "v1".to_string()),
                        ("k3".to_string(), "v3".to_string())
                    ])
                );
                Ok(())
            })
        })
        .await
        .unwrap();

    // Remake the stash, mutate, then read.
    let mut stash = f().await;
    stash
        .with_transaction(move |tx| {
            Box::pin(async move {
                let mut orders_batch = orders.make_batch_tx(&tx).await.unwrap();
                orders.append_to_batch(&mut orders_batch, &"k4".to_string(), &"v4".to_string(), 1);
                drop(tx.append(vec![orders_batch]).await.unwrap());
                assert_eq!(
                    tx.peek_one(orders).await.unwrap(),
                    BTreeMap::from([
                        ("k1".to_string(), "v1".to_string()),
                        ("k3".to_string(), "v3".to_string()),
                        ("k4".to_string(), "v4".to_string())
                    ])
                );
                Ok(())
            })
        })
        .await
        .unwrap();

    // Remake and read again.
    let mut stash = f().await;
    stash
        .with_transaction(move |tx| {
            Box::pin(async move {
                assert_eq!(
                    tx.peek_one(orders).await.unwrap(),
                    BTreeMap::from([
                        ("k1".to_string(), "v1".to_string()),
                        ("k3".to_string(), "v3".to_string()),
                        ("k4".to_string(), "v4".to_string())
                    ])
                );
                Ok(())
            })
        })
        .await
        .unwrap();

    test_stash_table(&mut stash).await;

    stash
}

async fn test_stash<F, O>(f: F) -> Stash
where
    O: Future<Output = Stash>,
    F: Fn() -> O,
{
    let mut stash = f().await;
    stash
        .with_transaction(move |tx| {
            Box::pin(async move {
                // Create an arrangement, write some data into it, then read it back.
                let orders = tx.collection::<String, String>("orders").await.unwrap();
                tx.update_savepoint(orders.id, &[(("widgets".to_string(), "1".to_string()), 1, 1)], None)
                    .await
                    .unwrap();
                tx.update_savepoint(orders.id, &[(("wombats".to_string(), "2".to_string()), 1, 2)], None)
                    .await
                    .unwrap();

                let collections = tx.collections().await;
                tracing::info!("{collections:?}");
                let data: Vec<_> = tx.iter_raw(orders.id).await.unwrap().collect();
                tracing::info!("{data:?}");

                // Move this before iter to better test the memory tx's iter_key.
                assert_eq!(
                    tx.iter_key(orders, &"widgets".to_string()).await.unwrap(),
                    &[("1".into(), 1, 1)]
                );
                assert_eq!(
                    tx.iter(orders).await.unwrap(),
                    &[
                        (("widgets".into(), "1".into()), 1, 1),
                        (("wombats".into(), "2".into()), 1, 2),
                    ]
                );
                assert_eq!(
                    tx.iter_key(orders, &"wombats".to_string()).await.unwrap(),
                    &[("2".into(), 1, 2)]
                );

                // Write to another arrangement and ensure the data stays separate.
                let other = tx.collection::<String, String>("other").await.unwrap();
                tx.update_savepoint(other.id, &[(("foo".to_string(), "bar".to_string()), 1, 1)], None)
                    .await
                    .unwrap();
                assert_eq!(
                    tx.iter(other).await.unwrap(),
                    &[(("foo".into(), "bar".into()), 1, 1)],
                );
                assert_eq!(
                    tx.iter(orders).await.unwrap(),
                    &[
                        (("widgets".into(), "1".into()), 1, 1),
                        (("wombats".into(), "2".into()), 1, 2),
                    ]
                );

                // Check that consolidation happens immediately...
                tx.update_savepoint(orders.id, &[(("wombats".to_string(), "2".to_string()), 1, -1)], None)
                    .await
                    .unwrap();
                assert_eq!(
                    tx.iter(orders).await.unwrap(),
                    &[
                        (("widgets".into(), "1".into()), 1, 1),
                        (("wombats".into(), "2".into()), 1, 1),
                    ]
                );

                // ...even when it results in a entry's removal.
                tx.update_savepoint(orders.id, &[(("wombats".to_string(), "2".to_string()), 1, -1)], None)
                    .await
                    .unwrap();
                assert_eq!(
                    tx.iter(orders).await.unwrap(),
                    &[(("widgets".into(), "1".into()), 1, 1),]
                );

                // Check that logical compaction applies immediately.
                tx.update_savepoint(
                    orders.id,
                    &[
                        (("widgets".to_string(), "1".to_string()), 2, 1),
                        (("widgets".to_string(), "1".to_string()), 3, 1),
                        (("widgets".to_string(), "1".to_string()), 4, 1),
                    ],
                    None,
                )
                    .await
                    .unwrap();
                tx.seal(orders.id, Antichain::from_elem(3), None)
                    .await
                    .unwrap();
                // Peek should not observe widgets from timestamps 3 or 4.
                assert_eq!(tx.peek_timestamp(orders).await.unwrap(), 2);
                assert_eq!(
                    tx.peek(orders).await.unwrap(),
                    vec![("widgets".into(), "1".into(), 2)]
                );
                assert_eq!(
                    tx.peek_one(orders).await.unwrap_err().to_string(),
                    "stash error: unexpected peek multiplicity of 2"
                );
                tx.compact(orders.id, &Antichain::from_elem(3), None)
                    .await
                    .unwrap();
                assert_eq!(
                    tx.iter(orders).await.unwrap(),
                    &[
                        (("widgets".into(), "1".into()), 3, 3),
                        (("widgets".into(), "1".into()), 4, 1),
                    ]
                );

                // Check that physical compaction does not change the collection's contents.
                tx.consolidate(orders.id).await.unwrap();
                assert_eq!(
                    tx.iter(orders).await.unwrap(),
                    &[
                        (("widgets".into(), "1".into()), 3, 3),
                        (("widgets".into(), "1".into()), 4, 1),
                    ]
                );

                // Test invalid seals, compactions, and updates.
                assert_eq!(
                    tx.seal(orders.id, Antichain::from_elem(2), None)
                        .await
                        .unwrap_err()
                        .to_string(),
                    "stash error: seal request {2} is less than the current upper frontier {3}",
                );
                assert_eq!(
                    tx.compact(orders.id, &Antichain::from_elem(2), None)
                        .await
                        .unwrap_err()
                        .to_string(),
                    "stash error: compact request {2} is less than the current since frontier {3}",
                );
                assert_eq!(
                    tx.compact(orders.id, &Antichain::from_elem(4), None)
                        .await
                        .unwrap_err()
                        .to_string(),
                    "stash error: compact request {4} is greater than the current upper frontier {3}",
                );
                assert_eq!(
                    tx.update_savepoint(orders.id, &[(("wodgets".to_string(), "1".to_string()), 2, 1)], None)
                        .await
                        .unwrap_err()
                        .to_string(),
                    "stash error: entry time 2 is less than the current upper frontier {3}",
                );

                // Test advancing since and upper to the empty frontier.
                tx.seal(orders.id, Antichain::new(), None).await.unwrap();
                tx.compact(orders.id, &Antichain::new(), None)
                    .await
                    .unwrap();
                assert_eq!(
                    match tx.iter(orders).await {
                        Ok(_) => panic!("call to iter unexpectedly succeeded"),
                        Err(e) => e.to_string(),
                    },
                    "stash error: cannot iterate collection with empty since frontier",
                );
                assert_eq!(
                    match tx.iter_key(orders, &"wombats".to_string()).await {
                        Ok(_) => panic!("call to iter_key unexpectedly succeeded"),
                        Err(e) => e.to_string(),
                    },
                    "stash error: cannot iterate collection with empty since frontier",
                );
                tx.consolidate(orders.id).await.unwrap();

                // Double check that the other collection is still untouched.
                assert_eq!(
                    tx.iter(other).await.unwrap(),
                    &[(("foo".into(), "bar".into()), 1, 1)],
                );
                assert_eq!(
                    tx.since(other.id).await.unwrap(),
                    Antichain::from_elem(Timestamp::MIN)
                );
                assert_eq!(
                    tx.upper(other.id).await.unwrap(),
                    Antichain::from_elem(Timestamp::MIN)
                );

                // Test peek_one.
                tx.seal(other.id, Antichain::from_elem(2), None)
                    .await
                    .unwrap();
                assert_eq!(
                    tx.peek_one(other).await.unwrap(),
                    BTreeMap::from([("foo".to_string(), "bar".to_string())])
                );
                assert_eq!(
                    tx.peek_key_one(other, &"foo".to_string()).await.unwrap(),
                    Some("bar".to_string())
                );
                Ok(())
            })
        })
        .await
        .unwrap();

    stash
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_fail_after_commit() {
    Stash::with_debug_stash(|mut stash| async move {
        let col = collection::<i64, i64>(&mut stash, "c1").await.unwrap();
        let mut batch = make_batch(&col, &mut stash).await.unwrap();
        col.append_to_batch(&mut batch, &1, &2, 1);
        append(&mut stash, vec![batch]).await.unwrap();
        assert_eq!(
            C1.peek_one(&mut stash).await.unwrap(),
            BTreeMap::from([(1, 2)])
        );
        let mut batch = make_batch(&col, &mut stash).await.unwrap();
        col.append_to_batch(&mut batch, &1, &2, -1);

        fail::cfg("stash_commit_pre", "return(commit failpoint)").unwrap();
        fail::cfg("stash_commit_post", "return(commit failpoint)").unwrap();
        // Because the commit error will either retry or discover it succeeded,
        // it never returns an error. Thus, we need to re-enable the failpoint
        // in another thread. Use both a pre and post commit error to test both
        // commit success and fail paths. Use a channel to check that we haven't
        // succeeded unexpectedly.
        let (tx, mut rx) = oneshot::channel();
        let handle = spawn(|| "stash_commit_enable", async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            // Assert no success yet.
            rx.try_recv().unwrap_err();
            fail::cfg("stash_commit_post", "off").unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            // Assert no success yet.
            rx.try_recv().unwrap_err();
            fail::cfg("stash_commit_pre", "off").unwrap();
            rx.await.unwrap();
        });
        append(&mut stash, vec![batch.clone()]).await.unwrap();
        assert_eq!(C1.peek_one(&mut stash).await.unwrap(), BTreeMap::new());
        tx.send(()).unwrap();
        handle.await.unwrap();
    })
    .await
    .expect("must succeed");
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_batch_large_serialized_size() {
    Stash::with_debug_stash(|mut stash| async move {
        static S: TypedCollection<i64, String> = TypedCollection::new("s");
        let _16mb = "0".repeat(1 << 24);
        // A too large update will always fail.
        assert_contains!(
            S.upsert(&mut stash, vec![(1, _16mb)])
                .await
                .unwrap_err()
                .to_string(),
            "message size 16 MiB bigger than maximum allowed message size"
        );
        // An large but reasonable update will be split into batches.
        let large = "0".repeat(INSERT_BATCH_SPLIT_SIZE);
        S.upsert(&mut stash, vec![(1, large.clone()), (2, large)])
            .await
            .unwrap();
        assert_eq!(S.iter(&mut stash).await.unwrap().len(), 2);
    })
    .await
    .expect("must succeed");
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_batch_large_number_updates() {
    Stash::with_debug_stash(|mut stash| async move {
        let col = collection::<i64, i64>(&mut stash, "c1").await.unwrap();
        let mut batch = make_batch(&col, &mut stash).await.unwrap();
        // Back of the envelope math would produce 12 batches of updates.
        //
        // Each update statement takes 4 arguments, so we have a total of
        // MAX_INSERT_ARGUMENTS * 4 * 3 arguments, leading to 12 batches of updates.
        for i in 0..(MAX_INSERT_ARGUMENTS * 3) {
            let i = i.into();
            col.append_to_batch(&mut batch, &i, &(i + 1), 1);
        }
        append(&mut stash, vec![batch]).await.unwrap();
    })
    .await
    .expect("must succeed");
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_readonly() {
    let factory = DebugStashFactory::try_new().await.expect("must succeed");
    let mut stash_rw = factory.open().await;
    let col_rw = collection::<i64, i64>(&mut stash_rw, "c1").await.unwrap();
    let mut batch = make_batch(&col_rw, &mut stash_rw).await.unwrap();
    col_rw.append_to_batch(&mut batch, &1, &2, 1);
    append(&mut stash_rw, vec![batch]).await.unwrap();

    // Now make a readonly stash. We should fail to create new collections,
    // but be able to read existing collections.
    let mut stash_ro = factory.open_readonly().await;
    let res = collection::<i64, i64>(&mut stash_ro, "c2").await;
    assert_contains!(
        res.unwrap_err().to_string(),
        "cannot execute INSERT in a read-only transaction"
    );
    assert_eq!(
        C1.peek_one(&mut stash_ro).await.unwrap(),
        BTreeMap::from([(1, 2)])
    );

    // The previous stash should still be the leader.
    assert!(stash_rw.confirm_leadership().await.is_ok());
    stash_rw.verify().await.unwrap();
    factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_savepoint() {
    let factory = DebugStashFactory::try_new().await.expect("must succeed");
    let mut stash_rw = factory.open().await;
    // Data still present from previous test.

    // Now make a savepoint stash. We should be allowed to create anything
    // we want, but it shouldn't be viewable to other stashes.
    let mut stash_sp = factory.open_savepoint().await;
    let c1 = collection::<i64, i64>(&mut stash_rw, "c1").await.unwrap();
    let mut batch = make_batch(&c1, &mut stash_rw).await.unwrap();
    c1.append_to_batch(&mut batch, &1, &2, 1);
    append(&mut stash_rw, vec![batch]).await.unwrap();
    let mut batch = make_batch(&c1, &mut stash_sp).await.unwrap();
    c1.append_to_batch(&mut batch, &5, &6, 1);
    append(&mut stash_sp, vec![batch]).await.unwrap();
    assert_eq!(
        C1.peek_one(&mut stash_sp).await.unwrap(),
        BTreeMap::from([(1, 2), (5, 6)]),
    );
    // RW collection can't see the new row.
    assert_eq!(
        C1.peek_one(&mut stash_rw).await.unwrap(),
        BTreeMap::from([(1, 2)])
    );

    // SP stash can create a new collection, append to it, peek it.
    let c_savepoint = collection::<i64, i64>(&mut stash_sp, "c_savepoint")
        .await
        .unwrap();
    let mut batch = make_batch(&c_savepoint, &mut stash_sp).await.unwrap();
    c_savepoint.append_to_batch(&mut batch, &3, &4, 1);
    append(&mut stash_sp, vec![batch]).await.unwrap();
    assert_eq!(
        C_SAVEPOINT.peek_one(&mut stash_sp).await.unwrap(),
        BTreeMap::from([(3, 4)])
    );
    // But the RW collection can't see it.
    assert_eq!(
        BTreeSet::from_iter(stash_rw.collections().await.unwrap().into_values()),
        BTreeSet::from(["c1".to_string()])
    );

    drop(stash_sp);

    // The previous stash should still be the leader.
    assert!(stash_rw.confirm_leadership().await.is_ok());
    // Verify c1 didn't change.
    assert_eq!(
        C1.peek_one(&mut stash_rw).await.unwrap(),
        BTreeMap::from([(1, 2)])
    );
    stash_rw.verify().await.unwrap();
    factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_fence() {
    let factory = DebugStashFactory::try_new().await.expect("must succeed");
    let mut conn1 = factory.open().await;
    let mut conn2 = factory.open().await;
    assert!(match collection::<String, String>(&mut conn1, "c").await {
        Err(e) => e.is_unrecoverable(),
        _ => panic!("expected error"),
    });
    let _: StashCollection<String, String> = collection(&mut conn2, "c").await.unwrap();
    factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_append() {
    let factory = DebugStashFactory::try_new().await.expect("must succeed");
    test_append(|| async { factory.open().await }).await;
    factory.open().await.verify().await.unwrap();
    factory.drop().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_stash_stash() {
    let factory = DebugStashFactory::try_new().await.expect("must succeed");
    test_stash(|| async { factory.open().await }).await;
    factory.open().await.verify().await.unwrap();
    factory.drop().await;
}

async fn make_batch<K, V>(
    collection: &StashCollection<K, V>,
    stash: &mut Stash,
) -> Result<AppendBatch, StashError> {
    let id = collection.id;
    let lower = stash
        .with_transaction(move |tx| Box::pin(async move { tx.upper(id).await }))
        .await?;
    collection.make_batch_lower(lower)
}

async fn collection<K, V>(
    stash: &mut Stash,
    name: &str,
) -> Result<StashCollection<K, V>, StashError>
where
    K: Data,
    V: Data,
{
    let name = name.to_string();
    stash
        .with_transaction(move |tx| Box::pin(async move { tx.collection(&name).await }))
        .await
}

/// Atomically adds entries, seals, compacts, and consolidates multiple
/// collections.
///
/// The `lower` of each `AppendBatch` is checked to be the existing `upper` of the collection.
/// The `upper` of the `AppendBatch` will be the new `upper` of the collection.
/// The `compact` of each `AppendBatch` will be the new `since` of the collection.
///
/// If this method returns `Ok`, the entries have been made durable and uppers
/// advanced, otherwise no changes were committed.
async fn append(stash: &mut Stash, batches: Vec<AppendBatch>) -> Result<(), StashError> {
    if batches.is_empty() {
        return Ok(());
    }
    stash
        .with_transaction(move |tx| {
            Box::pin(async move {
                let batches = batches.clone();
                drop(tx.append(batches).await?);
                for id in tx.collections().await?.keys() {
                    tx.consolidate(*id).await?;
                }
                Ok(())
            })
        })
        .await
}

async fn get<K, V>(
    typed_collection: &TypedCollection<K, V>,
    stash: &mut Stash,
) -> Result<StashCollection<K, V>, StashError>
where
    K: Data,
    V: Data,
{
    collection(stash, typed_collection.name()).await
}

async fn test_stash_table(stash: &mut Stash) {
    const TABLE: TypedCollection<Vec<u8>, String> = TypedCollection::new("table");
    fn uniqueness_violation(a: &String, b: &String) -> bool {
        a == b
    }
    let collection = get(&TABLE, stash).await.unwrap();

    async fn commit(
        stash: &mut Stash,
        collection: StashCollection<Vec<u8>, String>,
        pending: Vec<(Vec<u8>, String, i64)>,
    ) -> Result<(), StashError> {
        let mut batch = make_batch(&collection, stash).await.unwrap();
        for (k, v, diff) in pending {
            collection.append_to_batch(&mut batch, &k, &v, diff);
        }
        append(stash, vec![batch]).await.unwrap();
        Ok(())
    }

    TABLE
        .upsert_key(stash, 1i64.to_le_bytes().to_vec(), |_| {
            Ok::<_, Infallible>("v1".to_string())
        })
        .await
        .unwrap()
        .unwrap();
    TABLE
        .upsert(stash, vec![(2i64.to_le_bytes().to_vec(), "v2".to_string())])
        .await
        .unwrap();
    let mut table =
        TableTransaction::new(TABLE.peek_one(stash).await.unwrap(), uniqueness_violation).unwrap();
    assert_eq!(
        table.items(),
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v1".to_string()),
            (2i64.to_le_bytes().to_vec(), "v2".to_string())
        ])
    );
    assert_eq!(table.delete(|_k, _v| false).len(), 0);
    assert_eq!(table.delete(|_k, v| v == "v2").len(), 1);
    assert_eq!(
        table.items(),
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string())])
    );
    assert_eq!(table.update(|_k, _v| Some("v3".to_string())).unwrap(), 1);

    // Uniqueness violation.
    table
        .insert(3i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap_err();

    table
        .insert(3i64.to_le_bytes().to_vec(), "v4".to_string())
        .unwrap();
    assert_eq!(
        table.items(),
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v3".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string()),
        ])
    );
    assert_eq!(
        table
            .update(|_k, _v| Some("v1".to_string()))
            .unwrap_err()
            .to_string(),
        "stash error: uniqueness violation"
    );
    let pending = table.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v1".to_string(), -1),
            (1i64.to_le_bytes().to_vec(), "v3".to_string(), 1),
            (2i64.to_le_bytes().to_vec(), "v2".to_string(), -1),
            (3i64.to_le_bytes().to_vec(), "v4".to_string(), 1),
        ]
    );
    commit(stash, collection, pending).await.unwrap();
    let items = TABLE.peek_one(stash).await.unwrap();
    assert_eq!(
        items,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v3".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string())
        ])
    );

    let mut table = TableTransaction::new(items, uniqueness_violation).unwrap();
    // Deleting then creating an item that has a uniqueness violation should work.
    assert_eq!(table.delete(|k, _v| k == &1i64.to_le_bytes()).len(), 1);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    // Uniqueness violation in value.
    table
        .insert(5i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap_err();
    // Key already exists, expect error.
    table
        .insert(1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap_err();
    assert_eq!(table.delete(|k, _v| k == &1i64.to_le_bytes()).len(), 1);
    // Both the inserts work now because the key and uniqueness violation are gone.
    table
        .insert(5i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    table
        .insert(1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap();
    let pending = table.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v3".to_string(), -1),
            (1i64.to_le_bytes().to_vec(), "v5".to_string(), 1),
            (5i64.to_le_bytes().to_vec(), "v3".to_string(), 1),
        ]
    );
    commit(stash, collection, pending).await.unwrap();
    let items = TABLE.peek_one(stash).await.unwrap();
    assert_eq!(
        items,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v5".to_string()),
            (3i64.to_le_bytes().to_vec(), "v4".to_string()),
            (5i64.to_le_bytes().to_vec(), "v3".to_string()),
        ])
    );

    let mut table = TableTransaction::new(items, uniqueness_violation).unwrap();
    assert_eq!(table.delete(|_k, _v| true).len(), 3);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v1".to_string())
        .unwrap();

    commit(stash, collection, table.pending()).await.unwrap();
    let items = TABLE.peek_one(stash).await.unwrap();
    assert_eq!(
        items,
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v1".to_string()),])
    );

    let mut table = TableTransaction::new(items, uniqueness_violation).unwrap();
    assert_eq!(table.delete(|_k, _v| true).len(), 1);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v2".to_string())
        .unwrap();
    commit(stash, collection, table.pending()).await.unwrap();
    let items = TABLE.peek_one(stash).await.unwrap();
    assert_eq!(
        items,
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "v2".to_string()),])
    );

    // Verify we don't try to delete v3 or v4 during commit.
    let mut table = TableTransaction::new(items, uniqueness_violation).unwrap();
    assert_eq!(table.delete(|_k, _v| true).len(), 1);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v3".to_string())
        .unwrap();
    table
        .insert(1i64.to_le_bytes().to_vec(), "v4".to_string())
        .unwrap_err();
    assert_eq!(table.delete(|_k, _v| true).len(), 1);
    table
        .insert(1i64.to_le_bytes().to_vec(), "v5".to_string())
        .unwrap();
    commit(stash, collection, table.pending()).await.unwrap();
    let items = TABLE.peek_one(stash).await.unwrap();
    assert_eq!(
        items.into_iter().collect::<Vec<_>>(),
        vec![(1i64.to_le_bytes().to_vec(), "v5".to_string())]
    );

    // Test `set`.
    let items = TABLE.peek_one(stash).await.unwrap();
    let mut table = TableTransaction::new(items, uniqueness_violation).unwrap();
    // Uniqueness violation.
    table
        .set(2i64.to_le_bytes().to_vec(), Some("v5".to_string()))
        .unwrap_err();
    table
        .set(3i64.to_le_bytes().to_vec(), Some("v6".to_string()))
        .unwrap();
    table.set(2i64.to_le_bytes().to_vec(), None).unwrap();
    table.set(1i64.to_le_bytes().to_vec(), None).unwrap();
    let pending = table.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v5".to_string(), -1),
            (3i64.to_le_bytes().to_vec(), "v6".to_string(), 1),
        ]
    );
    commit(stash, collection, pending).await.unwrap();
    let items = TABLE.peek_one(stash).await.unwrap();
    assert_eq!(
        items,
        BTreeMap::from([(3i64.to_le_bytes().to_vec(), "v6".to_string())])
    );

    // Duplicate `set`.
    let items = TABLE.peek_one(stash).await.unwrap();
    let mut table = TableTransaction::new(items, uniqueness_violation).unwrap();
    table
        .set(3i64.to_le_bytes().to_vec(), Some("v6".to_string()))
        .unwrap();
    let pending = table.pending::<Vec<u8>, String>();
    assert!(pending.is_empty());

    // Test `set_many`.
    let items = TABLE.peek_one(stash).await.unwrap();
    let mut table = TableTransaction::new(items, uniqueness_violation).unwrap();
    // Uniqueness violation.
    table
        .set_many(BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), Some("v6".to_string())),
            (42i64.to_le_bytes().to_vec(), Some("v1".to_string())),
        ]))
        .unwrap_err();
    table
        .set_many(BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), Some("v6".to_string())),
            (3i64.to_le_bytes().to_vec(), Some("v1".to_string())),
        ]))
        .unwrap();
    table
        .set_many(BTreeMap::from([
            (42i64.to_le_bytes().to_vec(), Some("v7".to_string())),
            (3i64.to_le_bytes().to_vec(), None),
        ]))
        .unwrap();
    let pending = table.pending();
    assert_eq!(
        pending,
        vec![
            (1i64.to_le_bytes().to_vec(), "v6".to_string(), 1),
            (3i64.to_le_bytes().to_vec(), "v6".to_string(), -1),
            (42i64.to_le_bytes().to_vec(), "v7".to_string(), 1),
        ]
    );
    commit(stash, collection, pending).await.unwrap();
    let items = TABLE.peek_one(stash).await.unwrap();
    assert_eq!(
        items,
        BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), "v6".to_string()),
            (42i64.to_le_bytes().to_vec(), "v7".to_string())
        ])
    );

    // Duplicate `set_many`.
    let items = TABLE.peek_one(stash).await.unwrap();
    let mut table = TableTransaction::new(items, uniqueness_violation).unwrap();
    table
        .set_many(BTreeMap::from([
            (1i64.to_le_bytes().to_vec(), Some("v6".to_string())),
            (42i64.to_le_bytes().to_vec(), Some("v7".to_string())),
        ]))
        .unwrap();
    let pending = table.pending::<Vec<u8>, String>();
    assert!(pending.is_empty());
}

#[mz_ore::test]
fn test_table() {
    fn uniqueness_violation(a: &String, b: &String) -> bool {
        a == b
    }
    let mut table = TableTransaction::new(
        BTreeMap::from([(1i64.to_le_bytes().to_vec(), "a".to_string())]),
        uniqueness_violation,
    )
    .unwrap();

    table
        .insert(2i64.to_le_bytes().to_vec(), "b".to_string())
        .unwrap();
    table
        .insert(3i64.to_le_bytes().to_vec(), "c".to_string())
        .unwrap();
}
