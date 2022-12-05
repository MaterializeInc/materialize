// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::cast_precision_loss)]

use std::fs::File;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cache::PersistClientCache;
use prometheus::Encoder;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, info_span, trace, Instrument};

use mz_ore::cast::CastFrom;
use mz_persist::workload::DataGenerator;
use mz_persist_client::{Metrics, PersistConfig, PersistLocation, ShardId};

use crate::open_loop::api::{BenchmarkReader, BenchmarkWriter};
use crate::BUILD_INFO;

/// Different benchmark configurations.
#[derive(clap::ArgEnum, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum BenchmarkType {
    /// Data is written straight into persistence from the data generator.
    RawWriter,
    /// A simulated materialize source pipeline where data is first timestamped using a consensus
    /// persist shard and then written to persistence.
    MzSourceModel,
}

/// Open-loop benchmark for persistence.
#[derive(Debug, clap::Parser)]
pub struct Args {
    /// Number of writer instances.
    #[clap(long, value_name = "W", default_value_t = 1)]
    num_writers: usize,

    /// Number of reader instances.
    #[clap(long, value_name = "R", default_value_t = 1)]
    num_readers: usize,

    /// Handle to the persist consensus system.
    #[clap(long, value_name = "CONSENSUS_URI")]
    consensus_uri: String,

    /// Handle to the persist blob storage.
    #[clap(long, value_name = "BLOB_URI")]
    blob_uri: String,

    /// The type of benchmark to run
    #[clap(arg_enum, long, default_value_t = BenchmarkType::RawWriter)]
    benchmark_type: BenchmarkType,

    /// Runtime in a whole number of seconds
    #[clap(long, parse(try_from_str = humantime::parse_duration), value_name = "S", default_value = "60s")]
    runtime: Duration,

    /// How many records writers should emit per second.
    #[clap(long, value_name = "R", default_value_t = 100)]
    records_per_second: usize,

    /// Size of records (goodbytes) in bytes.
    #[clap(long, value_name = "B", default_value_t = 64)]
    record_size_bytes: usize,

    /// Batch size in number of records (if applicable).
    #[clap(long, env = "", value_name = "R", default_value_t = 100)]
    batch_size: usize,

    /// Duration between subsequent informational log outputs.
    #[clap(long, parse(try_from_str = humantime::parse_duration), value_name = "L", default_value = "1s")]
    logging_granularity: Duration,

    /// Id of the persist shard (for use in multi-process runs).
    #[clap(short, long, value_name = "I")]
    shard_id: Option<String>,

    /// The address of the internal HTTP server.
    #[clap(long, value_name = "HOST:PORT", default_value = "127.0.0.1:6878")]
    internal_http_listen_addr: SocketAddr,

    /// Path of a file to write metrics at the end of the run.
    #[clap(long)]
    metrics_file: Option<String>,
}

const MIB: u64 = 1024 * 1024;

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    {
        let metrics_registry = metrics_registry.clone();
        info!(
            "serving internal HTTP server on {}",
            args.internal_http_listen_addr
        );
        mz_ore::task::spawn(
            || "http_server",
            axum::Server::bind(&args.internal_http_listen_addr).serve(
                axum::Router::new()
                    .route(
                        "/metrics",
                        axum::routing::get(move || async move {
                            mz_http_util::handle_prometheus(&metrics_registry).await
                        }),
                    )
                    .into_make_service(),
            ),
        );
    }

    let location = PersistLocation {
        blob_uri: args.blob_uri.clone(),
        consensus_uri: args.consensus_uri.clone(),
    };
    let persist = PersistClientCache::new(
        PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone()),
        &metrics_registry,
    )
    .open(location)
    .await?;

    let shard_id = match args.shard_id.clone() {
        Some(shard_id) => ShardId::from_str(&shard_id).map_err(anyhow::Error::msg)?,
        None => ShardId::new(),
    };

    let metrics = Arc::clone(persist.metrics());
    let (writers, readers) = match args.benchmark_type.clone() {
        BenchmarkType::RawWriter => {
            raw_persist_benchmark::setup_raw_persist(
                persist,
                shard_id,
                args.num_writers,
                args.num_readers,
            )
            .await?
        }
        BenchmarkType::MzSourceModel => panic!("source model"),
    };

    run_benchmark(args, metrics_registry, metrics, writers, readers).await
}

async fn run_benchmark<W, R>(
    args: Args,
    metrics_registry: MetricsRegistry,
    metrics: Arc<Metrics>,
    writers: Vec<W>,
    readers: Vec<R>,
) -> Result<(), anyhow::Error>
where
    W: BenchmarkWriter + Send + Sync + 'static,
    R: BenchmarkReader + Send + Sync + 'static,
{
    let num_records_total = args.records_per_second * usize::cast_from(args.runtime.as_secs());
    let data_generator =
        DataGenerator::new(num_records_total, args.record_size_bytes, args.batch_size);

    let benchmark_description = format!(
        "num-readers={} num-writers={} runtime={:?} num_records_total={} records-per-second={} record-size-bytes={} batch-size={}",
        args.num_readers, args.num_writers, args.runtime, num_records_total, args.records_per_second,
        args.record_size_bytes, args.batch_size);

    info!("starting benchmark: {}", benchmark_description);
    let mut generator_handles: Vec<JoinHandle<Result<String, anyhow::Error>>> = vec![];
    let mut write_handles: Vec<JoinHandle<Result<String, anyhow::Error>>> = vec![];
    let mut read_handles: Vec<JoinHandle<Result<(String, R), anyhow::Error>>> = vec![];

    // All workers should have the starting time (so they can consistently track progress
    // and reason about lag independently).
    let start = Instant::now();
    // Use a barrier to start all threads at the same time. We need 2x the number of
    // writers because we start 2 distinct tasks per writer.
    let barrier = Arc::new(Barrier::new(2 * args.num_writers + args.num_readers));

    // The batch interarrival time. We'll use this quantity to rate limit the
    // data generation.
    let time_per_batch = {
        let records_per_second_f64 = args.records_per_second as f64;
        let batch_size_f64 = args.batch_size as f64;

        let batches_per_second = records_per_second_f64 / batch_size_f64;
        Duration::from_secs(1).div_f64(batches_per_second)
    };

    for (idx, mut writer) in writers.into_iter().enumerate() {
        let b = Arc::clone(&barrier);
        let data_generator = data_generator.clone();
        let start = start.clone();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // Intentionally create the span outside the task to set the parent.
        let generator_span = info_span!("generator", idx);
        let data_generator_handle = mz_ore::task::spawn(
            || format!("data-generator-{}", idx),
            async move {
                trace!("data generator {} waiting for barrier", idx);
                b.wait().await;
                info!("starting data generator {}", idx);

                // The number of batches this data generator has sent over to the
                // corresponding writer task.
                let mut batch_idx = 0;
                // The last time we emitted progress information to stdout, expressed
                // as a relative duration from start.
                let mut prev_log = Duration::from_millis(0);
                loop {
                    // Data generation can be CPU expensive, so generate it
                    // in a spawn_blocking to play nicely with the rest of
                    // the async code.
                    let mut data_generator = data_generator.clone();
                    // Intentionally create the span outside the task to set the
                    // parent.
                    let batch_span = info_span!("batch", batch_idx);
                    let batch = mz_ore::task::spawn_blocking(
                        || "data_generator-batch",
                        move || {
                            batch_span
                                .in_scope(|| data_generator.gen_batch(usize::cast_from(batch_idx)))
                        },
                    )
                    .await
                    .expect("task failed");
                    trace!("data generator {} made a batch", idx);
                    let batch = match batch {
                        Some(x) => x,
                        None => {
                            let records_sent = usize::cast_from(batch_idx) * args.batch_size;
                            let finished = format!(
                                "Data generator {} finished after {} ms and sent {} records",
                                idx,
                                start.elapsed().as_millis(),
                                records_sent
                            );
                            return Ok(finished);
                        }
                    };
                    batch_idx += 1;

                    // Sleep so this doesn't busy wait if it's ahead of
                    // schedule.
                    let elapsed = start.elapsed();
                    let next_batch_time = time_per_batch * (batch_idx);
                    let sleep = next_batch_time.saturating_sub(elapsed);
                    if sleep > Duration::ZERO {
                        async {
                            debug!("Data generator ahead of schedule, sleeping for {:?}", sleep);
                            tokio::time::sleep(sleep).await
                        }
                        .instrument(info_span!("throttle"))
                        .await;
                    }

                    // send will only error if the matching receiver has been dropped.
                    if let Err(SendError(_)) = tx.send(batch) {
                        bail!("receiver unexpectedly dropped");
                    }
                    trace!("data generator {} wrote a batch", idx);

                    if elapsed - prev_log > args.logging_granularity {
                        let records_sent = usize::cast_from(batch_idx) * args.batch_size;
                        debug!(
                            "After {} ms data generator {} has sent {} records.",
                            start.elapsed().as_millis(),
                            idx,
                            records_sent
                        );
                        prev_log = elapsed;
                    }
                }
            }
            .instrument(generator_span),
        );

        generator_handles.push(data_generator_handle);
        let b = Arc::clone(&barrier);

        // Intentionally create the span outside the task to set the parent.
        let writer_span = info_span!("writer", idx);
        let writer_handle = mz_ore::task::spawn(|| format!("writer-{}", idx), async move {
            trace!("writer {} waiting for barrier", idx);
            b.wait().await;
            info!("starting writer {}", idx);

            // Max observed latency for BenchmarkWriter::write.
            let mut max_write_latency = Duration::from_millis(0);

            // The last time we emitted progress information to stdout, expressed
            // as a relative duration from start.
            let mut prev_log = Duration::from_millis(0);
            let mut records_written = 0;

            loop {
                let batch = match rx.recv().await {
                    Some(batch) => batch,
                    None => break,
                };

                trace!("writer {} received a batch. writing", idx);
                let write_start = Instant::now();
                writer.write(batch).await?;

                records_written += args.batch_size;
                let write_latency = write_start.elapsed();
                if write_latency > max_write_latency {
                    max_write_latency = write_latency;
                }

                let elapsed = start.elapsed();

                if elapsed - prev_log > args.logging_granularity {
                    info!("After {} ms writer {} has written {} records. Max write latency {} ms most recent write latency {} ms.",
                          elapsed.as_millis(), idx, records_written, max_write_latency.as_millis(), write_latency.as_millis());
                    prev_log = elapsed;
                }

                if records_written >= num_records_total {
                    break;
                }
            }
            let elapsed = start.elapsed();
            let finished = format!(
                "Writer {} finished after {} ms and wrote {} records. Max write latency {} ms.",
                idx,
                elapsed.as_millis(),
                records_written,
                max_write_latency.as_millis()
            );

            writer.finish().await.unwrap();

            Ok(finished)
        }.instrument(writer_span));

        write_handles.push(writer_handle);
    }

    for (idx, mut reader) in readers.into_iter().enumerate() {
        let b = Arc::clone(&barrier);
        // Intentionally create the span outside the task to set the parent.
        let reader_span = info_span!("reader", idx);
        let reader_handle = mz_ore::task::spawn(|| format!("reader-{}", idx), async move {
            trace!("reader {} waiting for barrier", idx);
            b.wait().await;
            info!("starting reader {}", idx);

            // Max observed latency for BenchmarkReader::num_records.
            let mut max_read_latency = Duration::from_millis(0);

            // Max observed delay between the number of records expected to be read at any
            // point in time vs the number of records actually ingested by that point.
            let mut max_lag = 0;

            // The last time we emitted progress information to stdout, expressed
            // as a relative duration from start.
            let mut prev_log = Duration::from_millis(0);
            loop {
                let elapsed = start.elapsed();
                let expected_sent = elapsed.as_millis() as usize
                    / (time_per_batch.as_millis() as usize)
                    * args.batch_size;
                let read_start = Instant::now();
                let num_records_read = reader.num_records().await?;
                let read_latency = read_start.elapsed();
                let lag = if expected_sent > num_records_read {
                    expected_sent - num_records_read
                } else {
                    0
                };
                if lag > max_lag {
                    max_lag = lag;
                }

                if read_latency > max_read_latency {
                    max_read_latency = read_latency;
                }

                if elapsed - prev_log > args.logging_granularity {
                    let elapsed_seconds = elapsed.as_secs();
                    let mb_read = (num_records_read * args.record_size_bytes) as f64 / MIB as f64;
                    let throughput = mb_read / elapsed_seconds as f64;
                    info!("After {} ms reader {} has read {} records (throughput {:.3} MiB/s). Max read lag {} records, most recent read lag {} records. Max read latency {} ms, most recent read latency {} ms",
                          elapsed.as_millis(), idx, num_records_read, throughput, max_lag, lag, max_read_latency.as_millis(), read_latency.as_millis());
                    prev_log = elapsed;
                }
                if num_records_read == num_records_total {
                    let elapsed_seconds = elapsed.as_secs();
                    let mb_read = (num_records_read * args.record_size_bytes) as f64 / MIB as f64;
                    let throughput = mb_read / elapsed_seconds as f64;
                    let finished = format!("Reader {} finished after {} ms and read {} records (throughput {:.3} MiB/s). Max read lag {} records. Max read latency {} ms.",
                          idx, elapsed.as_millis(), num_records_read, throughput, max_lag, max_read_latency.as_millis());
                    return Ok((finished, reader));
                }
            }
        }.instrument(reader_span));
        read_handles.push(reader_handle);
    }

    for handle in generator_handles {
        match handle.await? {
            Ok(finished) => info!("{}", finished),
            Err(e) => error!("error: {:?}", e),
        }
    }
    for handle in write_handles {
        match handle.await? {
            Ok(finished) => info!("{}", finished),
            Err(e) => error!("error: {:?}", e),
        }
    }
    for handle in read_handles {
        match handle.await? {
            Ok((finished, _)) => info!("{}", finished),
            Err(e) => error!("error: {:?}", e),
        }
    }

    if let Some(metrics_file) = args.metrics_file {
        let mut file = File::create(metrics_file)?;
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&metrics_registry.gather(), &mut file)?;
        file.sync_all()?;
    }
    eprintln!("write amp: {}", metrics.write_amplification());

    Ok(())
}

mod api {
    use async_trait::async_trait;

    use mz_persist::indexed::columnar::ColumnarRecords;

    /// An interface to write a batch of data into a persistent system.
    #[async_trait]
    pub trait BenchmarkWriter {
        /// Writes the given batch to this writer.
        async fn write(&mut self, batch: ColumnarRecords) -> Result<(), anyhow::Error>;

        /// Signals that we are finished writing to this [BenchmarkWriter]. This
        /// will join any async tasks that might have been spawned for this
        /// [BenchmarkWriter].
        async fn finish(self) -> Result<(), anyhow::Error>;
    }

    /// An abstraction over a reader of data, which can report the number
    /// of distinct records its read so far.
    #[async_trait]
    pub trait BenchmarkReader {
        async fn num_records(&mut self) -> Result<usize, anyhow::Error>;
    }
}

mod raw_persist_benchmark {
    use async_trait::async_trait;
    use timely::progress::Antichain;
    use tokio::sync::mpsc::Sender;

    use mz_ore::cast::CastFrom;
    use mz_persist::indexed::columnar::ColumnarRecords;
    use mz_persist_client::read::{Listen, ListenEvent};
    use mz_persist_client::{PersistClient, ShardId};
    use mz_persist_types::Codec64;
    use tokio::task::JoinHandle;
    use tracing::{info_span, Instrument};

    use crate::open_loop::api::{BenchmarkReader, BenchmarkWriter};

    pub async fn setup_raw_persist(
        persist: PersistClient,
        id: ShardId,
        num_writers: usize,
        num_readers: usize,
    ) -> Result<
        (
            Vec<RawBenchmarkWriter>,
            Vec<Listen<Vec<u8>, Vec<u8>, u64, i64>>,
        ),
        anyhow::Error,
    > {
        let mut writers = vec![];
        for idx in 0..num_writers {
            let writer = RawBenchmarkWriter::new(&persist, id, idx).await?;

            writers.push(writer);
        }

        let mut readers = vec![];
        for _ in 0..num_readers {
            let (_writer, reader) = persist
                .open::<Vec<u8>, Vec<u8>, u64, i64>(id, "open loop")
                .await?;

            let listen = reader
                .listen(Antichain::from_elem(0))
                .await
                .expect("cannot serve requested as_of");
            readers.push(listen);
        }

        Ok((writers, readers))
    }

    pub struct RawBenchmarkWriter {
        tx: Option<Sender<ColumnarRecords>>,
        #[allow(dead_code)]
        handles: Vec<JoinHandle<()>>,
    }

    impl RawBenchmarkWriter {
        async fn new(
            persist: &PersistClient,
            id: ShardId,
            idx: usize,
        ) -> Result<Self, anyhow::Error> {
            let mut handles = Vec::<JoinHandle<()>>::new();
            let (records_tx, mut records_rx) = tokio::sync::mpsc::channel::<ColumnarRecords>(2);
            let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel(10);

            let mut write = persist
                .open_writer::<Vec<u8>, Vec<u8>, u64, i64>(id, "open loop")
                .await?;

            // Intentionally create the span outside the task to set the parent.
            let batch_writer_span = info_span!("batch-writer", idx);
            let handle = mz_ore::task::spawn(
                || format!("batch-writer-{}", idx),
                async move {
                    let mut current_upper = timely::progress::Timestamp::minimum();
                    while let Some(records) = records_rx.recv().await {
                        let mut max_ts = 0;
                        let current_upper_chain = Antichain::from_elem(current_upper);

                        let mut builder = write.builder(records.len(), current_upper_chain);

                        for ((k, v), t, d) in records.iter() {
                            builder
                                .add(&k.to_vec(), &v.to_vec(), &u64::decode(t), &i64::decode(d))
                                .await
                                .expect("invalid usage");

                            max_ts = std::cmp::max(max_ts, u64::decode(t));
                        }

                        max_ts = max_ts + 1;
                        let new_upper_chain = Antichain::from_elem(max_ts);
                        current_upper = max_ts;

                        let batch = builder
                            .finish(new_upper_chain)
                            .await
                            .expect("invalid usage");

                        match batch_tx.send(batch).await {
                            Ok(_) => (),
                            Err(e) => panic!("send error: {}", e),
                        }
                    }
                }
                .instrument(batch_writer_span),
            );
            handles.push(handle);

            let mut write = persist
                .open_writer::<Vec<u8>, Vec<u8>, u64, i64>(id, "open loop")
                .await?;

            // Intentionally create the span outside the task to set the parent.
            let appender_span = info_span!("appender", idx);
            let handle = mz_ore::task::spawn(
                || format!("appender-{}", idx),
                async move {
                    while let Some(batch) = batch_rx.recv().await {
                        let lower = batch.lower().clone();
                        let upper = batch.upper().clone();
                        write
                            .append_batch(batch, lower, upper)
                            .await
                            .expect("invalid usage")
                            .expect("unexpected upper");
                    }
                }
                .instrument(appender_span),
            );
            handles.push(handle);

            let writer = RawBenchmarkWriter {
                tx: Some(records_tx),
                handles,
            };

            Ok(writer)
        }
    }

    #[async_trait]
    impl BenchmarkWriter for RawBenchmarkWriter {
        async fn write(&mut self, batch: ColumnarRecords) -> Result<(), anyhow::Error> {
            self.tx
                .as_mut()
                .expect("writer was already finished")
                .send(batch)
                .await
                .expect("writer send error");
            Ok(())
        }

        async fn finish(mut self) -> Result<(), anyhow::Error> {
            self.tx.take().expect("already finished");

            for handle in self.handles.drain(..) {
                let () = handle.await?;
            }

            Ok(())
        }
    }

    #[async_trait]
    impl BenchmarkReader for Listen<Vec<u8>, Vec<u8>, u64, i64> {
        async fn num_records(&mut self) -> Result<usize, anyhow::Error> {
            // This impl abuses the fact that DataGenerator timestamps each
            // record with the record count to avoid having to actually count
            // the number of records..
            let mut count = 0;
            let events = self.next().await;

            for event in events {
                if let ListenEvent::Progress(t) = event {
                    count = usize::cast_from(t.elements()[0]);
                }
            }

            Ok(count)
        }
    }
}
