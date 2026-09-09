// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use mz_ore::pool::{ExtentCodec, Pool};
use proptest::prelude::*;
use timely::order::Product;

use super::*;
use crate::columnar::chunk::LZ4_CODEC;

fn store(block: usize) -> (Pool, Store) {
    let pool = Pool::new().unwrap();
    pool.set_budget(0);
    // Tests exercise compressed extents, without relying on host swap support.
    pool.set_rss_target(1 << 20);
    let store = Store::new(pool.clone(), block, block * 2, 2, &LZ4_CODEC).unwrap();
    (pool, store)
}

fn batch<K: Ord, M>(
    store: &Store,
    records: impl IntoIterator<Item = (K, M, Option<Vec<u8>>)>,
) -> Batch<K, M> {
    let mut builder = store.builder();
    let records = records
        .into_iter()
        .map(|(key, metadata, row)| Record {
            key,
            metadata,
            row: row.map(|row| builder.push(&row).unwrap()),
        })
        .collect();
    Batch::new(records, &[&builder.finish()]).unwrap()
}

#[mz_ore::test(tokio::test)]
async fn selection_reuses_blocks_after_input_drop() {
    let (pool, store) = store(128);
    let mut input = batch(
        &store,
        (0..50).flat_map(|key| {
            (0..4).map(move |order| {
                (
                    (key, 7u64),
                    order,
                    (order != 3 || key % 2 == 0).then(|| vec![u8::try_from(key).unwrap(), order]),
                )
            })
        }),
    );
    let inserted = pool.stats().inserts;
    assert_eq!(pool.stats().resident_bytes, 0);
    for _ in 0..8 {
        let mut cursor = LatestCursor::new(&input);
        let mut selected = Vec::new();
        loop {
            match cursor.step(1) {
                SelectionStep::Selected(index) => selected.push(index),
                SelectionStep::Yield => {}
                SelectionStep::Done => break,
            }
        }
        input = input.select(selected).unwrap();
    }
    assert_eq!(input.records().len(), 50);
    assert_eq!(
        pool.stats().inserts,
        inserted,
        "metadata rebuilds must not rewrite payloads"
    );
    assert_eq!(
        store.stats().decoded_bytes,
        0,
        "selection must not inspect payloads"
    );
    let record = &input.records()[0];
    let row = record.row.unwrap();
    let request = store.prepare_read([(input.payloads(), row)]).unwrap();
    drop(input);
    assert!(
        pool.stats().live_chunks > 0,
        "prepared reads retain ownership"
    );
    let lease = request.read().await;
    assert_eq!(lease.get(row).unwrap(), &[0, 3]);
    assert!(store.stats().charged_bytes > 0);
    drop(lease);
    assert_eq!(store.stats().charged_bytes, 0);
    assert_eq!(pool.stats().live_chunks, 0);
}

#[mz_ore::test(tokio::test)]
async fn bulk_reads_coalesce_blocks_and_preserve_equal_rows_at_distinct_handles() {
    let (pool, store) = store(64);
    let mut builder = store.builder();
    let empty = builder.push(b"").unwrap();
    let first = builder.push(b"same row").unwrap();
    let second = builder.push(b"same row").unwrap();
    let other = builder.push(b"other").unwrap();
    let manifest = builder.finish();
    assert_ne!(first, second);
    let lease = store
        .prepare_read([empty, first, second, other, first].map(|r| (&manifest, r)))
        .unwrap()
        .read()
        .await;
    assert_eq!(manifest.block_count(), 1);
    assert_eq!(store.stats().decoded_bytes, 56);
    assert_eq!(lease.get(first).unwrap(), lease.get(second).unwrap());
    assert_ne!(lease.get(first).unwrap(), lease.get(other).unwrap());
    assert_eq!(lease.get(empty).unwrap(), b"");
    drop(manifest);
    assert_eq!(pool.stats().live_chunks, 1);
    drop(lease);
    assert_eq!(pool.stats().live_chunks, 0);
}

#[mz_ore::test(tokio::test)]
async fn oversized_rows_and_read_sets_fail_without_consuming_admission() {
    let (_, store) = store(64);
    let mut builder = store.builder();
    assert_eq!(builder.push(&[0; 57]), Err(StoreError::RowTooLarge));
    let rows: Vec<_> = (0..3)
        .map(|value| builder.push(&[value; 56]).unwrap())
        .collect();
    let manifest = builder.finish();
    assert_eq!(manifest.block_count(), 3);
    assert!(matches!(
        store.prepare_read(rows.iter().map(|r| (&manifest, *r))),
        Err(StoreError::ReadTooLarge)
    ));
    assert_eq!(store.stats().charged_bytes, 0);
    let lease = store
        .prepare_read([(&manifest, rows[0]), (&manifest, rows[1])])
        .unwrap()
        .read()
        .await;
    assert_eq!(store.stats().charged_bytes, 128);
    assert_eq!(lease.get(rows[1]).unwrap(), &[1; 56]);
}

#[mz_ore::test]
fn independent_stores_do_not_alias_handles() {
    let (_, a) = store(64);
    let (_, b) = store(64);
    let mut a_builder = a.builder();
    let a_row = a_builder.push(b"a").unwrap();
    let a_manifest = a_builder.finish();
    let mut b_builder = b.builder();
    let b_row = b_builder.push(b"b").unwrap();
    let b_manifest = b_builder.finish();
    assert_ne!(a_row, b_row);
    assert!(matches!(
        a.prepare_read([(&b_manifest, a_row)]),
        Err(StoreError::UnownedRow)
    ));
    assert!(matches!(
        Manifest::retain([b_row], [&a_manifest]),
        Err(StoreError::UnownedRow)
    ));
}

#[mz_ore::test(tokio::test)]
async fn canceled_admission_does_not_leak_a_reservation() {
    let (_, store) = store(64);
    let mut builder = store.builder();
    let a = builder.push(&[0; 56]).unwrap();
    let b = builder.push(&[1; 56]).unwrap();
    let manifest = builder.finish();
    let first = store
        .prepare_read([(&manifest, a), (&manifest, b)])
        .unwrap()
        .read()
        .await;
    let mut second = Box::pin(
        store
            .prepare_read([(&manifest, a), (&manifest, b)])
            .unwrap()
            .read(),
    );
    assert!(futures_util::poll!(&mut second).is_pending());
    assert_eq!(store.stats().charged_bytes, 128);
    drop(second);
    drop(first);
    let third = store
        .prepare_read([(&manifest, a), (&manifest, b)])
        .unwrap()
        .read();
    let third = tokio::time::timeout(Duration::from_secs(10), third)
        .await
        .unwrap();
    assert!(store.stats().peak_charged_bytes <= 128);
    drop(third);
    assert_eq!(store.stats().charged_bytes, 0);
}

#[derive(Debug)]
struct GateCodec {
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl ExtentCodec for GateCodec {
    fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
        out.clear();
        out.extend_from_slice(body);
    }

    fn decode(&self, stored: &[u8], body: &mut [u8]) {
        let entered = self.entered.lock().unwrap().take();
        if let Some(entered) = entered {
            let _ = entered.send(());
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
        body.copy_from_slice(stored);
    }
}

#[mz_ore::test(tokio::test)]
async fn canceled_decode_owns_its_bytes_and_blocks_until_completion() {
    let pool = Pool::new().unwrap();
    pool.set_budget(0);
    pool.set_rss_target(1 << 20);
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = mpsc::channel();
    let codec = Box::leak(Box::new(GateCodec {
        entered: Mutex::new(Some(entered)),
        release: Mutex::new(released),
    }));
    let store = Store::new(pool.clone(), 64, 128, 1, codec).unwrap();
    let mut builder = store.builder();
    let row = builder.push(&[7; 56]).unwrap();
    let manifest = builder.finish();
    let read = store.prepare_read([(&manifest, row)]).unwrap();
    let task = mz_ore::task::spawn(|| "canceled_payload_read", read.read());
    tokio::time::timeout(Duration::from_secs(10), started)
        .await
        .unwrap()
        .unwrap();
    task.abort_and_wait().await;
    drop(manifest);
    assert_eq!(
        store.stats().charged_bytes,
        64,
        "cancellation cannot admit replacement bytes during decode"
    );
    assert_eq!(pool.stats().live_chunks, 1);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while store.stats().charged_bytes != 0 || pool.stats().live_chunks != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[mz_ore::test(tokio::test)]
async fn join_streams_a_group_larger_than_the_read_budget() {
    let (_, store) = store(64);
    let left = batch(
        &store,
        (0..20).map(|i| {
            (
                "shared",
                (Product::new(1u64, 4u64), -2i64),
                Some(vec![i; 56]),
            )
        }),
    );
    let right = batch(
        &store,
        (0..17).map(|i| {
            (
                "shared",
                (Product::new(3u64, 2u64), 3i64),
                Some(vec![i; 56]),
            )
        }),
    );
    let mut cursor = JoinCursor::new(&left, &right);
    let mut matches = 0;
    let mut yields = 0;
    loop {
        match cursor.step(1) {
            JoinStep::Match(m) => {
                assert_eq!(m.time, Product::new(3, 4));
                assert_eq!(m.diff, -6);
                let l = m.left.unwrap();
                let r = m.right.unwrap();
                let lease = store
                    .prepare_read([(left.payloads(), l), (right.payloads(), r)])
                    .unwrap()
                    .read()
                    .await;
                assert_eq!(
                    lease.get(l).unwrap(),
                    &[u8::try_from(matches / 17).unwrap(); 56]
                );
                assert_eq!(
                    lease.get(r).unwrap(),
                    &[u8::try_from(matches % 17).unwrap(); 56]
                );
                matches += 1;
            }
            JoinStep::Yield => yields += 1,
            JoinStep::Done => break,
        }
    }
    assert_eq!(matches, 20 * 17);
    assert_eq!(store.stats().read_jobs, 340);
    assert_eq!(store.stats().decoded_bytes, 340 * 128);
    assert!(yields > 0);
    assert_eq!(store.stats().peak_charged_bytes, 128);
    assert_eq!(store.stats().charged_bytes, 0);
}

#[mz_ore::test]
fn unmatched_keys_yield_and_zero_fuel_does_no_work() {
    let left = Batch::new(
        (0..100)
            .map(|key| Record {
                key,
                metadata: (0u64, 1i64),
                row: None,
            })
            .collect(),
        &[],
    )
    .unwrap();
    let right = Batch::new(
        vec![Record {
            key: 100,
            metadata: (0u64, 1i64),
            row: None,
        }],
        &[],
    )
    .unwrap();
    let mut cursor = JoinCursor::new(&left, &right);
    assert_eq!(cursor.step(0), JoinStep::Yield);
    for _ in 0..100 {
        assert_eq!(cursor.step(1), JoinStep::Yield);
    }
    assert_eq!(cursor.step(1), JoinStep::Done);
    let mut latest = LatestCursor::new(&left);
    assert_eq!(latest.step(0), SelectionStep::Yield);
    assert_eq!(latest.step(2), SelectionStep::Selected(0));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(80))]

    #[mz_ore::test]
    fn join_matches_reference_with_partial_times_and_retractions(
        left in prop::collection::vec((0u8..8, 0u64..5, 0u64..5, -3i64..4), 0..35),
        right in prop::collection::vec((0u8..8, 0u64..5, 0u64..5, -3i64..4), 0..35),
        fuel in 1usize..9,
    ) {
        let make = |rows: &[(u8,u64,u64,i64)]| Batch::new(rows.iter().map(|&(key,x,y,diff)| Record {
            key: format!("key-{key}"), metadata: (Product::new(x,y), diff), row: None,
        }).collect(), &[]).unwrap();
        let l = make(&left);
        let r = make(&right);
        let mut expected = BTreeMap::new();
        for &(lk,lx,ly,ld) in &left {
            for &(rk,rx,ry,rd) in &right {
                if lk == rk {
                    let time = Product::new(lx, ly).join(&Product::new(rx, ry));
                    *expected.entry((format!("key-{lk}"), time)).or_insert(0) += ld * rd;
                }
            }
        }
        let mut actual = BTreeMap::new();
        let mut cursor = JoinCursor::new(&l, &r);
        loop {
            match cursor.step(fuel) {
                JoinStep::Match(m) => {
                    let key = l.records()[m.left_index].key.clone();
                    prop_assert_eq!(&key, &r.records()[m.right_index].key);
                    *actual.entry((key, m.time)).or_insert(0) += m.diff;
                },
                JoinStep::Yield => {},
                JoinStep::Done => break,
            }
        }
        expected.retain(|_,d| *d != 0);
        actual.retain(|_,d| *d != 0);
        prop_assert_eq!(actual, expected);
    }

    #[mz_ore::test]
    fn latest_matches_reference_across_yields(
        updates in prop::collection::vec((0u8..20, 0u64..100), 0..200),
        fuel in 1usize..12,
    ) {
        let records = updates.iter().map(|&(key, metadata)| Record {
            key, metadata, row: None,
        }).collect();
        let input = Batch::new(records, &[]).unwrap();
        let mut expected = BTreeMap::new();
        for &(key, order) in &updates {
            expected.entry(key)
                .and_modify(|best| *best = std::cmp::max(*best, order))
                .or_insert(order);
        }
        let mut cursor = LatestCursor::new(&input);
        let mut actual = BTreeMap::new();
        loop {
            match cursor.step(fuel) {
                SelectionStep::Selected(index) => {
                    let r = &input.records()[index];
                    actual.insert(r.key, r.metadata);
                }
                SelectionStep::Yield => {},
                SelectionStep::Done => break,
            }
        }
        prop_assert_eq!(actual, expected);
    }
}

#[mz_ore::test]
fn single_block_consumers_do_not_require_a_join_sized_budget() {
    let pool = Pool::new().unwrap();
    pool.set_budget(0);
    pool.set_rss_target(1 << 20);
    assert!(matches!(
        Store::new(pool.clone(), 63, 128, 1, &LZ4_CODEC),
        Err(StoreError::BlockSize)
    ));
    assert!(matches!(
        Store::new(pool.clone(), 64, 63, 1, &LZ4_CODEC),
        Err(StoreError::ReadBudget)
    ));
    assert!(matches!(
        Store::new(pool.clone(), 64, 128, 0, &LZ4_CODEC),
        Err(StoreError::ReadBudget)
    ));
    let store = Store::new(pool, 64, 64, 1, &LZ4_CODEC).unwrap();
    let mut builder = store.builder();
    let a = builder.push(&[0; 56]).unwrap();
    let b = builder.push(&[1; 56]).unwrap();
    let manifest = builder.finish();
    assert!(store.prepare_read([(&manifest, a)]).is_ok());
    assert!(matches!(
        store.prepare_read([(&manifest, a), (&manifest, b)]),
        Err(StoreError::ReadTooLarge)
    ));
}

#[mz_ore::test(tokio::test)]
async fn consumers_share_admission_through_ready_output() {
    let (_, store) = store(64);
    let mut builder = store.builder();
    let a = builder.push(&[0; 56]).unwrap();
    let b = builder.push(&[1; 56]).unwrap();
    let manifest = builder.finish();
    let reads = (0..12).map(|_| {
        let request = store
            .prepare_read([(&manifest, a), (&manifest, b)])
            .unwrap();
        async move {
            let lease = request.read().await;
            tokio::task::yield_now().await;
            assert_eq!(lease.get(a).unwrap(), &[0; 56]);
            assert_eq!(lease.get(b).unwrap(), &[1; 56]);
        }
    });
    tokio::time::timeout(
        Duration::from_secs(10),
        futures_util::future::join_all(reads),
    )
    .await
    .unwrap();
    assert_eq!(store.stats().charged_bytes, 0);
    assert_eq!(store.stats().peak_charged_bytes, 128);
    assert_eq!(store.stats().read_jobs, 12);
}

#[mz_ore::test(tokio::test)]
async fn borrowed_output_survives_all_batch_owners_and_does_not_alias_empty_payloads() {
    let (pool, store) = store(64);
    let source = batch(&store, [("a", (), Some(Vec::new())), ("b", (), None)]);
    let selected = source.select([0, 1]).unwrap();
    drop(source);
    let row = selected.records()[0].row.unwrap();
    assert!(selected.records()[1].row.is_none());
    assert!(matches!(
        selected.select([2]),
        Err(StoreError::InvalidIndex)
    ));
    let read = store.prepare_read([(selected.payloads(), row)]).unwrap();
    drop(selected);
    let lease = read.read().await;
    assert_eq!(lease.get(row).unwrap(), b"");
    assert_eq!(pool.stats().live_chunks, 1);
    drop(lease);
    assert_eq!(pool.stats().live_chunks, 0);
}
