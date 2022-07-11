// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Instant;

use criterion::{black_box, Criterion, Throughput};
use timely::progress::Antichain;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tracing::debug;

use mz_ore::task;
use mz_persist::workload::DataGenerator;
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{PersistClient, ShardId};
use mz_persist_types::Codec64;

use crate::{bench_all_clients, load};

pub fn bench_writes(
    name: &str,
    throughput: bool,
    c: &mut Criterion,
    runtime: &Runtime,
    data: &DataGenerator,
) {
    let mut g = c.benchmark_group(name);
    if throughput {
        g.throughput(Throughput::Bytes(data.goodput_bytes()));
    }
    bench_all_clients(&mut g, runtime, data, |b, client| {
        // This writes a set of batches to a single client to suss out any
        // issues from repeated use. However, use iter_custom so we can divide
        // out the number of batches to get a per-write number as the benchmark
        // output.
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut batches = None;
            for _ in 0..iters {
                let b = runtime
                    .block_on(bench_writes_one_iter(&client, &data))
                    .expect("failed to run iter");
                assert_eq!(batches.get_or_insert(b), &b);
            }
            let batches = batches.expect("didn't have at least one iter");
            start.elapsed().div_f64(batches as f64)
        })
    });
}

async fn bench_writes_one_iter(
    client: &PersistClient,
    data: &DataGenerator,
) -> Result<usize, anyhow::Error> {
    let mut write = client
        .open_writer::<Vec<u8>, Vec<u8>, u64, i64>(ShardId::new())
        .await?;

    // Write the data as fast as we can while keeping each batch in its own
    // write.
    let (batch_count, _) = load(&mut write, data).await;
    Ok(batch_count)
}

pub fn bench_write_to_listen(
    name: &str,
    throughput: bool,
    c: &mut Criterion,
    runtime: &Runtime,
    data: &DataGenerator,
) {
    let mut g = c.benchmark_group(name);
    if throughput {
        g.throughput(Throughput::Bytes(data.goodput_bytes()));
    }
    bench_all_clients(&mut g, runtime, data, |b, client| {
        // This write_to_listens a set of batches to a single client to suss out
        // any issues from repeated use. However, use iter_custom so we can
        // divide out the number of batches to get a per-write_to_listen number
        // as the benchmark output.
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut batches = None;
            for _ in 0..iters {
                let b = runtime
                    .block_on(bench_write_to_listen_one_iter(&client, &data))
                    .expect("failed to run iter");
                assert_eq!(batches.get_or_insert(b), &b);
            }
            let batches = batches.expect("didn't have at least one iter");
            start.elapsed().div_f64(batches as f64)
        })
    });
}

async fn bench_write_to_listen_one_iter(
    client: &PersistClient,
    data: &DataGenerator,
) -> Result<usize, anyhow::Error> {
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
                _ = tx.closed() => break,
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

        // Gracefully expire the Listen.
        listen.expire().await;
    });

    // Now write the data, waiting for the listener to catch up after each
    // batch.
    let mut batch_count = 0;
    for batch in data.batches() {
        batch_count += 1;
        let max_ts = match batch.get(batch.len() - 1) {
            Some((_, max_ts, _)) => u64::decode(max_ts),
            None => continue,
        };
        let updates = batch
            .iter()
            .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), u64::decode(t), i64::decode(d)));
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

    Ok(batch_count)
}

pub fn bench_snapshot(
    name: &str,
    throughput: bool,
    c: &mut Criterion,
    runtime: &Runtime,
    data: &DataGenerator,
) {
    let mut g = c.benchmark_group(name);
    if throughput {
        g.throughput(Throughput::Bytes(data.goodput_bytes()));
    }
    bench_all_clients(&mut g, runtime, data, |b, client| {
        let shard_id = ShardId::new();
        let (_, as_of) = runtime.block_on(async {
            let mut write = client
                .open_writer::<Vec<u8>, Vec<u8>, u64, i64>(shard_id)
                .await
                .expect("failed to open shard");
            load(&mut write, data).await
        });

        b.iter(|| {
            runtime
                .block_on(bench_snapshot_one_iter(&client, &shard_id, &as_of))
                .expect("failed to run iter")
        })
    });
}

async fn bench_snapshot_one_iter(
    client: &PersistClient,
    shard_id: &ShardId,
    as_of: &Antichain<u64>,
) -> Result<(), anyhow::Error> {
    let read = client
        .open_reader::<Vec<u8>, Vec<u8>, u64, i64>(*shard_id)
        .await?;

    let mut snap = read
        .snapshot(as_of.clone())
        .await
        .expect("cannot serve requested as_of");
    while let Some(x) = snap.next().await {
        black_box(x);
    }

    // Gracefully expire the ReadHandle.
    read.expire().await;

    Ok(())
}
