// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace};

use mz_ore::cast::CastFrom;
use mz_persist::workload::DataGenerator;
use mz_persist_client::{PersistLocation, ShardId};

use crate::api::{BenchmarkReader, BenchmarkWriter};

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
#[derive(clap::Parser)]
struct Args {
    /// Number of tokio worker threads.
    #[clap(short, long, value_name = "T")]
    tokio_worker_threads: Option<usize>,

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
    #[clap(arg_enum, long, required = true)]
    benchmark_type: BenchmarkType,

    /// Runtime in seconds
    #[clap(long, value_name = "S", default_value_t = 60)]
    runtime_seconds: usize,

    /// How many records writers should emit per second.
    #[clap(long, value_name = "R", default_value_t = 100)]
    records_per_second: usize,

    /// Size of records (goodbytes) in bytes.
    #[clap(long, value_name = "B", default_value_t = 64)]
    record_size_bytes: usize,

    /// Batch size in number of records (if applicable).
    #[clap(long, env = "", value_name = "R", default_value_t = 10)]
    batch_size: usize,

    /// Duration in seconds between subsequent informational log outputs.
    #[clap(long, value_name = "L", default_value_t = 1)]
    logging_granularity_seconds: u64,

    /// Id of the persist shard (for use in multi-process runs).
    #[clap(short, long, value_name = "I")]
    shard_id: Option<String>,
}

const MIB: u64 = 1024 * 1024;

fn main() {
    mz_ore::test::init_logging();

    let args: Args = mz_ore::cli::parse_args();

    let mut tokio_builder = tokio::runtime::Builder::new_multi_thread();

    tokio_builder.enable_all();

    if let Some(tokio_worker_threads) = args.tokio_worker_threads {
        info!("Tokio worker threads: {tokio_worker_threads}");
        tokio_builder.worker_threads(tokio_worker_threads);
    }

    tokio_builder.build().unwrap().block_on(async {
        if let Err(err) = run(mz_ore::cli::parse_args()).await {
            eprintln!("persist_open_loop_benchmark: {:#}", err);
            process::exit(1);
        }
    });
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let location = PersistLocation {
        blob_uri: args.blob_uri.clone(),
        consensus_uri: args.consensus_uri.clone(),
    };
    let persist = location.open().await?;

    let num_records_total = args.records_per_second * args.runtime_seconds;
    let data_generator =
        DataGenerator::new(num_records_total, args.record_size_bytes, args.batch_size);
    let shard_id = match args.shard_id {
        Some(shard_id) => ShardId::from_str(&shard_id).map_err(anyhow::Error::msg)?,
        None => ShardId::new(),
    };
    let (writers, readers) = match args.benchmark_type {
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

    let benchmark_description = format!("num-readers={} num-writers={} runtime-seconds={} records-per-second={} record-size-bytes={} batch-size={}",
                                        args.num_readers, args.num_writers, args.runtime_seconds, args.records_per_second,
                                        args.record_size_bytes, args.batch_size);

    info!("starting benchmark: {}", benchmark_description);
    let mut generator_handles: Vec<JoinHandle<Result<String, anyhow::Error>>> = vec![];
    let mut write_handles: Vec<
        JoinHandle<Result<(String, Box<dyn BenchmarkWriter + Send + Sync>), anyhow::Error>>,
    > = vec![];
    let mut read_handles: Vec<
        JoinHandle<Result<(String, Box<dyn BenchmarkReader + Send + Sync>), anyhow::Error>>,
    > = vec![];

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

    let logging_granularity = Duration::from_secs(args.logging_granularity_seconds);

    for (idx, mut writer) in writers.into_iter().enumerate() {
        let b = Arc::clone(&barrier);
        let data_generator = data_generator.clone();
        let start = start.clone();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let data_generator_handle =
            mz_ore::task::spawn(|| format!("data-generator-{}", idx), async move {
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
                    // Sleep so this doesn't busy wait if it's ahead of
                    // schedule.
                    let elapsed = start.elapsed();
                    let next_batch_time = time_per_batch * (batch_idx + 1);
                    let sleep = next_batch_time.saturating_sub(start.elapsed());
                    if sleep > Duration::ZERO {
                        trace!("Data generator ahead of schedule, sleeping for {:?}", sleep);
                        tokio::time::sleep(sleep).await;
                    }

                    // Write down any batches we were supposed to have already sent
                    // according to the desired batch writing rate.
                    while start.elapsed() > time_per_batch * batch_idx {
                        // Data generation can be CPU expensive, so generate it
                        // in a spawn_blocking to play nicely with the rest of
                        // the async code.
                        let mut data_generator = data_generator.clone();
                        let batch = mz_ore::task::spawn_blocking(
                            || "data_generator-batch",
                            move || data_generator.gen_batch(usize::cast_from(batch_idx)),
                        )
                        .await
                        .expect("task failed");
                        batch_idx += 1;

                        let batch = match batch {
                            Some(batch) => batch,
                            None => {
                                let records_sent = usize::cast_from(batch_idx) * args.batch_size;
                                let finished =
                                    format!(
                                    "Data generator {} finished after {} ms and sent {} records",
                                    idx, start.elapsed().as_millis(), records_sent
                                );
                                return Ok(finished);
                            }
                        };
                        trace!("data generator {} wrote a batch", idx);

                        // send will only error if the matching receiver has been dropped.
                        if let Err(SendError(_)) = tx.send(batch) {
                            bail!("receiver unexpectedly dropped");
                        }
                    }

                    if elapsed - prev_log > logging_granularity {
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
            });

        generator_handles.push(data_generator_handle);
        let b = Arc::clone(&barrier);

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
                writer.write(&batch).await?;
                records_written += args.batch_size;
                let write_latency = write_start.elapsed();
                if write_latency > max_write_latency {
                    max_write_latency = write_latency;
                }

                let elapsed = start.elapsed();

                if elapsed - prev_log > logging_granularity {
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
            Ok((finished, writer))
        });

        write_handles.push(writer_handle);
    }

    for (idx, mut reader) in readers.into_iter().enumerate() {
        let b = Arc::clone(&barrier);
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

                if elapsed - prev_log > logging_granularity {
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
        });
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
            Ok((finished, _)) => info!("{}", finished),
            Err(e) => error!("error: {:?}", e),
        }
    }
    for handle in read_handles {
        match handle.await? {
            Ok((finished, _)) => info!("{}", finished),
            Err(e) => error!("error: {:?}", e),
        }
    }
    Ok(())
}

mod api {
    use async_trait::async_trait;

    use mz_persist::indexed::columnar::ColumnarRecords;

    /// An interface to write a batch of data into a persistent system.
    #[async_trait]
    pub trait BenchmarkWriter {
        async fn write(&mut self, batch: &ColumnarRecords) -> Result<(), anyhow::Error>;
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
    use mz_persist::indexed::columnar::ColumnarRecords;
    use mz_persist_client::read::{Listen, ListenEvent};
    use mz_persist_client::write::WriteHandle;
    use mz_persist_client::{PersistClient, ShardId};
    use timely::progress::Antichain;

    use crate::api::{BenchmarkReader, BenchmarkWriter};

    pub async fn setup_raw_persist(
        persist: PersistClient,
        id: ShardId,
        num_writers: usize,
        num_readers: usize,
    ) -> Result<
        (
            Vec<Box<dyn BenchmarkWriter + Send + Sync>>,
            Vec<Box<dyn BenchmarkReader + Send + Sync>>,
        ),
        anyhow::Error,
    > {
        let mut writers = vec![];
        for _ in 0..num_writers {
            let (writer, reader) = persist.open::<Vec<u8>, Vec<u8>, u64, i64>(id).await?;
            reader.expire().await;

            writers.push(Box::new(writer) as Box<dyn BenchmarkWriter + Send + Sync>);
        }

        let mut readers = vec![];
        for _ in 0..num_readers {
            let (writer, reader) = persist.open::<Vec<u8>, Vec<u8>, u64, i64>(id).await?;
            writer.expire().await;

            let listen = reader
                .listen(Antichain::from_elem(0))
                .await
                .expect("cannot serve requested as_of");
            readers.push(Box::new(listen) as Box<dyn BenchmarkReader + Send + Sync>);
        }

        Ok((writers, readers))
    }

    #[async_trait]
    impl BenchmarkWriter for WriteHandle<Vec<u8>, Vec<u8>, u64, i64> {
        async fn write(&mut self, batch: &ColumnarRecords) -> Result<(), anyhow::Error> {
            let max_ts = match batch.iter().last() {
                Some((_, t, _)) => t,
                None => return Ok(()),
            };
            let new_upper = Antichain::from_elem(max_ts + 1);

            // TODO: figure out the right way to do this without the extra allocations.
            let batch = batch
                .iter()
                .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), t, d));
            self.append(batch, self.upper().clone(), new_upper)
                .await??
                .expect("invalid current upper");

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
                    count = t.elements()[0] as usize;
                }
            }

            Ok(count)
        }
    }
}
