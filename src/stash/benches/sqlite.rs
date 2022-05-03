// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Make this work with the async API.
fn main() {}
/*
use std::iter::{repeat, repeat_with};

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::NamedTempFile;
use timely::progress::Antichain;

use mz_stash::{Sqlite, Stash};

fn bench_update(c: &mut Criterion) {
    let file = NamedTempFile::new().unwrap();
    let mut stash = Sqlite::open(file.path()).unwrap();

    let orders = stash.collection::<String, String>("orders").unwrap();
    let mut ts = 1;
    c.bench_function("update", |b| {
        b.iter(|| {
            let data = ("widgets".into(), "1".into());
            stash.update(orders, data, ts, 1).unwrap();
            ts += 1;
        })
    });
}

fn bench_update_many(c: &mut Criterion) {
    let file = NamedTempFile::new().unwrap();
    let mut stash = Sqlite::open(file.path()).unwrap();

    let orders = stash.collection::<String, String>("orders").unwrap();
    let mut ts = 1;
    c.bench_function("update_many", |b| {
        b.iter(|| {
            let data = ("widgets".into(), "1".into());
            stash
                .update_many(orders, repeat((data, ts, 1)).take(10))
                .unwrap();
            ts += 1;
        })
    });
}

fn bench_consolidation(c: &mut Criterion) {
    let file = NamedTempFile::new().unwrap();
    let mut stash = Sqlite::open(file.path()).unwrap();

    let orders = stash.collection::<String, String>("orders").unwrap();
    let mut ts = 1;
    c.bench_function("consolidation", |b| {
        b.iter(|| {
            let data = ("widgets".into(), "1".into());
            stash.update(orders, data.clone(), ts, 1).unwrap();
            stash.update(orders, data, ts + 1, -1).unwrap();
            let frontier = Antichain::from_elem(ts + 2);
            stash.seal(orders, frontier.borrow()).unwrap();
            stash.compact(orders, frontier.borrow()).unwrap();
            stash.consolidate(orders).unwrap();
            ts += 2;
        })
    });
}

fn bench_consolidation_large(c: &mut Criterion) {
    let file = NamedTempFile::new().unwrap();
    let mut stash = Sqlite::open(file.path()).unwrap();

    let orders = stash.collection::<String, String>("orders").unwrap();
    let mut ts = 0;

    // Prepopulate the database with 100k records
    let kv = ("widgets".into(), "1".into());
    stash
        .update_many(
            orders,
            repeat_with(|| {
                let update = (kv.clone(), ts, 1);
                ts += 1;
                update
            })
            .take(100_000),
        )
        .unwrap();
    let frontier = Antichain::from_elem(ts);
    stash.seal(orders, frontier.borrow()).unwrap();

    let mut compact_ts = 0;
    c.bench_function("consolidation large", |b| {
        b.iter(|| {
            ts += 1;
            // add 10k records
            stash
                .update_many(
                    orders,
                    repeat_with(|| {
                        let update = (kv.clone(), ts, 1);
                        ts += 1;
                        update
                    })
                    .take(10_000),
                )
                .unwrap();
            let frontier = Antichain::from_elem(ts);
            stash.seal(orders, frontier.borrow()).unwrap();
            // compact + consolidate
            compact_ts += 10_000;
            let compact_frontier = Antichain::from_elem(compact_ts);
            stash.compact(orders, compact_frontier.borrow()).unwrap();
            stash.consolidate(orders).unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_update,
    bench_update_many,
    bench_consolidation,
    bench_consolidation_large
);
criterion_main!(benches);
*/
