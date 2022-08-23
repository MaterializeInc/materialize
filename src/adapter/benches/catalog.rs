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

use mz_adapter::catalog::{Catalog, Op};
use mz_ore::{now::SYSTEM_TIME, task::spawn};

use tokio::runtime::Runtime;

fn bench_transact(c: &mut Criterion) {
    c.bench_function("transact", |b| {
        let runtime = Runtime::new().unwrap();

        let postgres_url = std::env::var("CATALOG_POSTGRES_BENCH").unwrap();
        let tls = mz_postgres_util::make_tls(
            &tokio_postgres::config::Config::from_str(&postgres_url)
                .expect("invalid postgres url for storage stash"),
        )
        .unwrap();
        let mut catalog = runtime.block_on(async {
            let schema = "catalog_bench";

            let (client, connection) = tokio_postgres::connect(&postgres_url, tls.clone())
                .await
                .unwrap();
            spawn(|| format!("postgres connection"), async move {
                connection.await.unwrap();
            });
            client
                .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                .await
                .unwrap();
            client
                .batch_execute(&format!("CREATE SCHEMA {schema}"))
                .await
                .unwrap();

            Catalog::open_debug_postgres(postgres_url, Some(schema.into()), SYSTEM_TIME.clone())
                .await
                .unwrap()
        });
        let mut id = 0;
        b.iter(|| {
            runtime.block_on(async {
                id += 1;
                let ops = vec![Op::CreateDatabase {
                    name: id.to_string(),
                    oid: id,
                    public_schema_oid: id,
                }];
                catalog.transact(None, ops, |_| Ok(())).await.unwrap();
            })
        })
    });
}

criterion_group!(benches, bench_transact);
criterion_main!(benches);
