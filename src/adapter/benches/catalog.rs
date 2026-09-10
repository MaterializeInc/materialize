// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mz_adapter::catalog::{Catalog, Op};
use mz_catalog::durable::objects::{CollectionCompactionBound, MaintainedReadRequirement};
use mz_catalog::durable::{TestCatalogStateBuilder, test_bootstrap_args};
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::{PersistClient, ShardId};
use mz_repr::{GlobalId, Timestamp};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_storage_client::controller::StorageTxn;
use timely::progress::Antichain;
use tokio::runtime::Runtime;
use uuid::Uuid;

fn bench_transact(c: &mut Criterion) {
    c.bench_function("transact", |b| {
        let runtime = Runtime::new().unwrap();

        let bootstrap_args = test_bootstrap_args();
        let mut catalog = runtime.block_on(async {
            Catalog::open_debug_catalog(
                PersistClient::new_for_tests().await,
                Uuid::new_v4(),
                &bootstrap_args,
            )
            .await
            .unwrap()
        });
        let mut id = 0;
        b.iter(|| {
            runtime.block_on(async {
                id += 1;
                let ops = vec![Op::CreateDatabase {
                    name: id.to_string(),
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }];
                let commit_ts = catalog.current_upper().await;
                catalog.transact(None, commit_ts, None, ops).await.unwrap();
            })
        });
        runtime.block_on(async {
            catalog.expire().await;
        });
    });
}

fn bench_read_protection(c: &mut Criterion) {
    let mut group = c.benchmark_group("catalog_read_protection");
    for count in [100_u64, 1_000, 10_000] {
        group.throughput(Throughput::Elements(count + count / 2));
        group.bench_with_input(BenchmarkId::new("batch", count), &count, |b, &count| {
            let runtime = Runtime::new().unwrap();
            // Count is total collections, split into disjoint input/output pairs.
            let pairs: Vec<_> = (0..count / 2)
                .map(|i| (GlobalId::User(2 * i + 1), GlobalId::User(2 * i + 2)))
                .collect();
            let records = |frontier| {
                let requirements = pairs
                    .iter()
                    .map(|&(input, output)| MaintainedReadRequirement {
                        id: output,
                        inputs: BTreeSet::from([input]),
                        frontier: Some(frontier),
                    })
                    .collect();
                let bounds = pairs
                    .iter()
                    .flat_map(|&(input, output)| [input, output])
                    .map(|id| CollectionCompactionBound {
                        id,
                        frontier: Some(frontier),
                    })
                    .collect();
                (requirements, bounds)
            };
            // Seed catalog metadata only. This isolates transaction processing
            // from controller work, physical retention, and concurrent DDL.
            let mut catalog = runtime.block_on(async {
                let persist_client = PersistClient::new_for_tests().await;
                let organization_id = Uuid::new_v4();
                let bootstrap_args = test_bootstrap_args();
                let mut storage = TestCatalogStateBuilder::new(persist_client.clone())
                    .with_organization_id(organization_id)
                    .with_default_deploy_generation()
                    .build()
                    .await
                    .unwrap()
                    .open(SYSTEM_TIME().into(), &bootstrap_args)
                    .await
                    .unwrap();
                // Acknowledge setup updates before transacting. The debug catalog
                // reconstructs in-memory state from the committed seed below.
                let _ = storage.sync_to_current_updates().await.unwrap();
                let mut tx = storage.transaction().await.unwrap();
                tx.insert_collection_metadata(
                    pairs
                        .iter()
                        .flat_map(|&(input, output)| [input, output])
                        .map(|id| (id, ShardId::new()))
                        .collect(),
                )
                .unwrap();
                let (requirements, bounds) = records(Timestamp::new(0));
                tx.set_read_protection(requirements, bounds).unwrap();
                let _ = tx.get_and_commit_op_updates();
                let commit_ts = tx.upper();
                tx.commit(commit_ts).await.unwrap();
                storage.expire().await;
                Catalog::open_debug_catalog(persist_client, organization_id, &bootstrap_args)
                    .await
                    .unwrap()
            });
            let mut frontier = 0_u64;
            // Only Catalog::transact is timed, not record construction or upper lookup.
            // Updating the same keys keeps cardinality fixed across warmup and samples.
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        frontier = frontier.checked_add(1).unwrap();
                        let (requirements, bounds) = records(Timestamp::new(frontier));
                        let ops = vec![Op::SetReadProtection {
                            requirements,
                            bounds,
                        }];
                        let commit_ts = catalog.current_upper().await;
                        let start = Instant::now();
                        let result = catalog.transact(None, commit_ts, None, ops).await;
                        elapsed += start.elapsed();
                        result.unwrap();
                    }
                    elapsed
                })
            });
            let state = catalog.state();
            assert_eq!(state.maintained_read_requirements().len(), pairs.len());
            assert_eq!(
                state.storage_metadata().compaction_bounds.len(),
                2 * pairs.len()
            );
            for &(input, output) in &pairs {
                assert_eq!(
                    state.maintained_read_requirements()[&output].frontier,
                    Some(Timestamp::new(frontier))
                );
                for id in [input, output] {
                    assert_eq!(
                        state.storage_metadata().compaction_bounds[&id],
                        Antichain::from_elem(Timestamp::new(frontier))
                    );
                }
            }
            runtime.block_on(catalog.expire());
        });
    }
    group.finish();
}

criterion_group!(benches, bench_transact, bench_read_protection);
criterion_main!(benches);
