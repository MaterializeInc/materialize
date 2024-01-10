// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use criterion::{criterion_group, criterion_main, Criterion};
use mz_ore::metrics::MetricsRegistry;
use mz_stash::{Stash, StashFactory, TypedCollection};
use mz_stash_types::StashError;
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

pub static FACTORY: Lazy<StashFactory> = Lazy::new(|| StashFactory::new(&MetricsRegistry::new()));

fn init_bench() -> (Runtime, Stash) {
    let runtime = Runtime::new().unwrap();
    let connstr = std::env::var("POSTGRES_URL").unwrap();
    let tls = mz_tls_util::make_tls(
        &tokio_postgres::config::Config::from_str(&connstr)
            .expect("invalid postgres url for storage stash"),
    )
    .unwrap();
    runtime
        .block_on(Stash::clear(&connstr, tls.clone()))
        .unwrap();
    let stash = runtime
        .block_on((*FACTORY).open(connstr, None, tls, None))
        .unwrap();
    (runtime, stash)
}

pub static COLLECTION_ORDER: TypedCollection<i64, i64> = TypedCollection::new("orders");

fn bench_update(c: &mut Criterion) {
    c.bench_function("upsert", |b| {
        let (runtime, mut stash) = init_bench();
        let mut i = 1;
        b.iter(|| {
            i += 1;
            runtime.block_on(async {
                COLLECTION_ORDER.upsert(&mut stash, [(i, i)]).await.unwrap();
            })
        })
    });
}

fn bench_update_many(c: &mut Criterion) {
    c.bench_function("upsert_key", |b| {
        let (runtime, mut stash) = init_bench();
        let mut i = 1;
        b.iter(move || {
            runtime.block_on(async {
                i += 1;
                COLLECTION_ORDER
                    .upsert_key(&mut stash, 1, move |_| Ok::<_, StashError>(i))
                    .await
                    .unwrap()
                    .unwrap();
            })
        })
    });
}

criterion_group!(benches, bench_update, bench_update_many);
criterion_main!(benches);
