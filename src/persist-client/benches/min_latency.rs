// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId};
use mz_ore::task;
use mz_persist::location::ExternalError;
use mz_persist::workload::DataGenerator;
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{PersistClient, ShardId};
use timely::progress::Antichain;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tracing::debug;

use crate::{create_file_pg_client, create_mem_mem_client, create_s3_pg_client};

pub fn bench_writes(
    g: &mut BenchmarkGroup<'_, WallTime>,
    runtime: Arc<Runtime>,
    data: &DataGenerator,
) -> Result<(), ExternalError> {
    {
        let client = runtime.block_on(create_mem_mem_client())?;
        let runtime = Arc::clone(&runtime);
        g.bench_function(
            BenchmarkId::new("mem_mem", data.goodput_pretty()),
            move |b| {
                b.iter(|| {
                    runtime
                        .block_on(bench_writes_one_iter(&client, data))
                        .expect("failed to run writes iter")
                })
            },
        );
    }

    if let Some((client, tempdir)) = runtime.block_on(create_file_pg_client())? {
        let runtime = Arc::clone(&runtime);
        g.bench_function(
            BenchmarkId::new("file_pg", data.goodput_pretty()),
            move |b| {
                b.iter(|| {
                    runtime
                        .block_on(bench_writes_one_iter(&client, data))
                        .expect("failed to run writes iter")
                })
            },
        );
        drop(tempdir);
    }

    if let Some(client) = runtime.block_on(create_s3_pg_client())? {
        let runtime = Arc::clone(&runtime);
        g.bench_function(BenchmarkId::new("s3_pg", data.goodput_pretty()), move |b| {
            b.iter(|| {
                runtime
                    .block_on(bench_writes_one_iter(&client, data))
                    .expect("failed to run writes iter")
            })
        });
    }

    Ok(())
}

async fn bench_writes_one_iter(
    client: &PersistClient,
    data: &DataGenerator,
) -> Result<(), anyhow::Error> {
    let (mut write, _read) = client
        .open::<Vec<u8>, Vec<u8>, u64, i64>(ShardId::new())
        .await?;

    // Write the data as fast as we can while keeping each batch in its own
    // write.
    for batch in data.batches() {
        let max_ts = match batch.get(batch.len() - 1) {
            Some((_, max_ts, _)) => max_ts,
            None => continue,
        };
        let updates = batch
            .iter()
            .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), t, d));
        write
            .compare_and_append(
                updates,
                write.upper().clone(),
                Antichain::from_elem(max_ts + 1),
            )
            .await
            .expect("external durability failure")
            .expect("invalid usage")
            .expect("unexpected upper");
    }

    Ok(())
}

pub fn bench_write_to_listen(
    g: &mut BenchmarkGroup<'_, WallTime>,
    runtime: Arc<Runtime>,
    data: &DataGenerator,
) -> Result<(), ExternalError> {
    {
        let client = runtime.block_on(create_mem_mem_client())?;
        let runtime = Arc::clone(&runtime);
        g.bench_function(
            BenchmarkId::new("mem_mem", data.goodput_pretty()),
            move |b| {
                b.iter(|| {
                    runtime
                        .block_on(bench_write_to_listen_one_iter(&client, data))
                        .expect("failed to run write_to_listen iter")
                })
            },
        );
    }

    if let Some((client, tempdir)) = runtime.block_on(create_file_pg_client())? {
        let runtime = Arc::clone(&runtime);
        g.bench_function(
            BenchmarkId::new("file_pg", data.goodput_pretty()),
            move |b| {
                b.iter(|| {
                    runtime
                        .block_on(bench_write_to_listen_one_iter(&client, data))
                        .expect("failed to run write_to_listen iter")
                })
            },
        );
        drop(tempdir);
    }

    if let Some(client) = runtime.block_on(create_s3_pg_client())? {
        let runtime = Arc::clone(&runtime);
        g.bench_function(BenchmarkId::new("s3_pg", data.goodput_pretty()), move |b| {
            b.iter(|| {
                runtime
                    .block_on(bench_write_to_listen_one_iter(&client, data))
                    .expect("failed to run write_to_listen iter")
            })
        });
    }

    Ok(())
}

async fn bench_write_to_listen_one_iter(
    client: &PersistClient,
    data: &DataGenerator,
) -> Result<(), anyhow::Error> {
    let (mut write, read) = client
        .open::<Vec<u8>, Vec<u8>, u64, i64>(ShardId::new())
        .await?;

    // Start the listener in a task before the write so it can't "cheat" by
    // fetching state right as we grab it.
    let mut listen = read
        .listen(Antichain::from_elem(0))
        .await
        .expect("cannot serve requested as_of");
    let (tx, mut rx) = mpsc::unbounded_channel();
    let listen = task::spawn(|| "listen", async move {
        loop {
            let events = tokio::select! {
                x = listen.next() => x,
                _ = tx.closed() => return,
            };
            for event in events {
                debug!("listener got event {:?}", event);
                match event {
                    ListenEvent::Progress(x) => match tx.send(x) {
                        Ok(()) => {}
                        Err(mpsc::error::SendError(_)) => return,
                    },
                    _ => {}
                }
            }
        }
    });

    // Now write the data, waiting for the listener to catch up after each
    // batch.
    for batch in data.batches() {
        let max_ts = match batch.get(batch.len() - 1) {
            Some((_, max_ts, _)) => max_ts,
            None => continue,
        };
        let updates = batch
            .iter()
            .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), t, d));
        write
            .compare_and_append(
                updates,
                write.upper().clone(),
                Antichain::from_elem(max_ts + 1),
            )
            .await
            .expect("external durability failure")
            .expect("invalid usage")
            .expect("unexpected upper");

        loop {
            match rx.recv().await {
                Some(x) if !x.less_than(&max_ts) => break,
                Some(_) => {}
                None => panic!("listener exited unexpectedly"),
            }
        }
    }
    drop(rx);

    // Now wait for the listener task to clean up so it doesn't leak into other
    // benchmarks.
    listen.await.expect("listener task failed");

    Ok(())
}
