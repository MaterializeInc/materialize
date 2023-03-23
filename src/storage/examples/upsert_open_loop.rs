// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Open-loop benchmark for measuring UPSERT performance. Run using
//!
//! ```
//! $ cargo run --example upsert_open_loop -- --runtime 10sec
//! ```

#![allow(clippy::cast_precision_loss)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::bail;
use differential_dataflow::Hashable;
use mz_ore::task;
use mz_timely_util::probe::{Handle, ProbeNotify};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, info_span, trace, Instrument};

use mz_build_info::{build_info, BuildInfo};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cast::CastFrom;
use mz_ore::cli::{self, CliConfig};
use mz_ore::metrics::MetricsRegistry;
use mz_persist::indexed::columnar::ColumnarRecords;
use mz_persist::workload::DataGenerator;
use mz_timely_util::builder_async::OperatorBuilder as AsyncOperatorBuilder;

// TODO(aljoscha): Make workload configurable: cardinality of keyspace, hot vs. cold keys, the
// works.
mod workload;

const BUILD_INFO: BuildInfo = build_info!();

/// Open-loop benchmark for persistence.
#[derive(Debug, clap::Parser)]
pub struct Args {
    /// Number of sources.
    #[clap(long, value_name = "W", default_value_t = 1)]
    num_sources: usize,

    /// Number of (timely) workers/threads.
    #[clap(long, value_name = "R", default_value_t = 1)]
    num_workers: usize,

    /// Runtime in a whole number of seconds
    #[clap(long, parse(try_from_str = humantime::parse_duration), value_name = "S", default_value = "60s")]
    runtime: Duration,

    /// How many records writers should emit per second.
    #[clap(long, value_name = "R", default_value_t = 10000)]
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

    /// The address of the internal HTTP server.
    #[clap(long, value_name = "HOST:PORT", default_value = "127.0.0.1:6878")]
    internal_http_listen_addr: SocketAddr,

    /// Path of a file to write metrics at the end of the run.
    #[clap(long)]
    metrics_file: Option<String>,

    #[clap(flatten)]
    tracing: TracingCliArgs,
}

fn main() {
    let args: Args = cli::parse_args(CliConfig::default());

    // Mirror the tokio Runtime configuration in our production binaries.
    let ncpus_useful = usize::max(1, std::cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ncpus_useful)
        .enable_all()
        .build()
        .expect("Failed building the Runtime");

    let _ = runtime
        .block_on(args.tracing.configure_tracing(StaticTracingConfig {
            service_name: "persist-open-loop",
            build_info: BUILD_INFO,
        }))
        .expect("failed to init tracing");

    let root_span = info_span!("upsert_open_loop");
    let res = runtime.block_on(run(args).instrument(root_span));

    if let Err(err) = res {
        eprintln!("error: {:#}", err);
        std::process::exit(1);
    }
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    {
        let metrics_registry = metrics_registry.clone();
        info!(
            "serving internal HTTP server on http://{}/metrics",
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

    let num_sources = args.num_sources;
    let num_workers = args.num_workers;
    run_benchmark(args, metrics_registry, num_sources, num_workers).await
}

async fn run_benchmark(
    args: Args,
    _metrics_registry: MetricsRegistry,
    num_sources: usize,
    num_workers: usize,
) -> Result<(), anyhow::Error> {
    let num_records_total = args.records_per_second * usize::cast_from(args.runtime.as_secs());
    let data_generator =
        DataGenerator::new(num_records_total, args.record_size_bytes, args.batch_size);

    let benchmark_description = format!(
        "num-sources={} num-workers={} runtime={:?} num_records_total={} records-per-second={} record-size-bytes={} batch-size={}",
        args.num_sources, args.num_workers, args.runtime, num_records_total, args.records_per_second,
        args.record_size_bytes, args.batch_size);

    info!("starting benchmark: {}", benchmark_description);

    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let progress_tx = Arc::new(Mutex::new(Some(progress_tx)));

    let mut generator_handles: Vec<JoinHandle<Result<String, anyhow::Error>>> = vec![];
    let mut source_rxs = BTreeMap::new();

    // All workers should have the starting time (so they can consistently track progress
    // and reason about lag independently).
    let start = Instant::now();

    // This controls the time that data generators (and in turn sources) downgrade to. We know this,
    // and probe the time at the end of the pipeline to figure out the lag.
    let shared_source_time = Arc::new(AtomicU64::new(start.elapsed().as_millis() as u64));

    // The batch interarrival time. We'll use this quantity to rate limit the
    // data generation.
    // No other known way to convert `usize` to `f64`.
    #[allow(clippy::as_conversions)]
    let time_per_batch = {
        let records_per_second_f64 = args.records_per_second as f64;
        let batch_size_f64 = args.batch_size as f64;

        let batches_per_second = records_per_second_f64 / batch_size_f64;
        Duration::from_secs(1).div_f64(batches_per_second)
    };

    for source_id in 0..num_sources {
        let data_generator = data_generator.clone();
        let start = start.clone();

        let (generator_tx, generator_rx) = tokio::sync::mpsc::unbounded_channel();
        source_rxs.insert(source_id, generator_rx);

        let shared_source_time = Arc::clone(&shared_source_time);

        // Intentionally create the span outside the task to set the parent.
        let generator_span = info_span!("generator", source_id);
        let data_generator_handle = mz_ore::task::spawn(
            || format!("data-generator-{}", source_id),
            async move {
                info!("starting data generator {}", source_id);

                // The number of batches this data generator has sent over to the
                // corresponding writer task.
                let mut batch_idx = 0;
                // The last time we emitted progress information to stdout, expressed
                // as a relative duration from start.
                let mut prev_log = Duration::from_millis(0);

                let mut current_source_time = shared_source_time.load(Ordering::SeqCst);

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
                    trace!("data generator {} made a batch", source_id);
                    let batch = match batch {
                        Some(x) => x,
                        None => {
                            let records_sent = usize::cast_from(batch_idx) * args.batch_size;
                            let finished = format!(
                                "Data generator {} finished after {} ms and sent {} records",
                                source_id,
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
                    if let Err(SendError(_)) = generator_tx.send(GeneratorEvent::Data(batch)) {
                        bail!("receiver unexpectedly dropped");
                    }
                    trace!("data generator {} wrote a batch", source_id);

                    let new_source_time =
                        shared_source_time.load(std::sync::atomic::Ordering::SeqCst);
                    if new_source_time > current_source_time {
                        current_source_time = new_source_time;
                        if let Err(SendError(_)) =
                            generator_tx.send(GeneratorEvent::Progress(current_source_time))
                        {
                            bail!("receiver unexpectedly dropped");
                        }
                        trace!("data generator {source_id} downgraded to {current_source_time}");
                    }

                    if elapsed - prev_log > args.logging_granularity {
                        let records_sent = usize::cast_from(batch_idx) * args.batch_size;
                        debug!(
                            "After {} ms data generator {} has sent {} records.",
                            start.elapsed().as_millis(),
                            source_id,
                            records_sent
                        );
                        prev_log = elapsed;
                    }
                }
            }
            .instrument(generator_span),
        );

        generator_handles.push(data_generator_handle);
    }

    let source_rxs = Arc::new(Mutex::new(source_rxs));

    let timely_config = timely::Config::process(num_workers);
    let timely_guards = timely::execute::execute(timely_config, move |timely_worker| {
        let progress_tx = Arc::clone(&progress_tx);
        let source_rxs = Arc::clone(&source_rxs);

        let probe = timely_worker.dataflow::<u64, _, _>(move |scope| {
            let mut source_streams = Vec::new();

            for source_id in 0..num_sources {
                let source_rxs = Arc::clone(&source_rxs);

                let source_stream = generator_source(scope, source_id, source_rxs);

                // TODO(gus): Plug in upsert here.

                // Choose a different worker for the counting.
                // TODO(aljoscha): Factor out into function.
                let worker_id = scope.index();
                let worker_count = scope.peers();
                let chosen_worker =
                    usize::cast_from((source_id + 1).hashed() % u64::cast_from(worker_count));
                let active_worker = chosen_worker == worker_id;

                let _output: Stream<_, ()> = source_stream.unary_frontier(
                    Exchange::new(move |_| u64::cast_from(chosen_worker)),
                    &format!("source-{source_id}-counter"),
                    move |_caps, _info| {
                        let mut count = 0;
                        let mut buffer = Vec::new();
                        move |input, _output| {
                            input.for_each(|_time, data| {
                                data.swap(&mut buffer);
                                for _record in buffer.drain(..) {
                                    count += 1;
                                }
                            });

                            if input.frontier().is_empty() && active_worker {
                                assert!(count == num_records_total);
                                info!("final count for source {source_id}: {count}");
                            }
                        }
                    },
                );

                source_streams.push(source_stream);
            }

            let probe = Handle::default();

            for source_stream in source_streams {
                source_stream.probe_notify_with(vec![probe.clone()]);
            }

            let worker_id = scope.index();

            let active_worker = 0 == worker_id;

            let progress_op = AsyncOperatorBuilder::new(format!("progress-bridge"), scope.clone());

            let probe_clone = probe.clone();
            let _shutdown_button = progress_op.build(move |_capabilities| async move {
                if !active_worker {
                    return;
                }

                let progress_tx = progress_tx
                    .lock()
                    .expect("lock poisoned")
                    .take()
                    .expect("someone took our progress_tx");

                loop {
                    let _progressed = probe_clone.progressed().await;
                    let mut frontier = Antichain::new();
                    probe_clone.with_frontier(|new_frontier| {
                        frontier.clone_from(&new_frontier.to_owned())
                    });
                    if !frontier.is_empty() {
                        let progress_ts = frontier.into_option().unwrap();
                        if let Err(SendError(_)) = progress_tx.send(progress_ts) {
                            return;
                        }
                    } else {
                        // We're done!
                        return;
                    }
                }
            });

            probe
        });

        // Step until our sources shut down.
        while probe.less_than(&u64::MAX) {
            timely_worker.step();
        }
    })
    .unwrap();

    let start_clone = start.clone();
    task::spawn(|| "lag-observer", async move {
        while let Some(observed_time) = progress_rx.recv().await {
            // TODO(aljoscha): The lag here also depends on when the generator task downgrades the
            // time, which it only does _after_ it creates a new batch. We have to change it to
            // immediately downgrade when we change the shared source timestamp.
            // TODO(aljoscha): Make the output similar to the persist open-loop benchmark, where we
            // output throughput, num records, etc.
            let now = start_clone.elapsed();
            let diff = now.as_millis() as u64 - observed_time;
            info!("lag: {diff}ms");
        }
        trace!("progress channel closed!");
    });

    while start.elapsed() < args.runtime {
        let current_time = shared_source_time.load(Ordering::SeqCst);
        let new_time = start.elapsed().as_millis() as u64;

        if new_time > current_time + 1000 {
            shared_source_time.store(new_time, Ordering::SeqCst);
        }

        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    for handle in generator_handles {
        match handle.await? {
            Ok(finished) => info!("{}", finished),
            Err(e) => error!("error: {:?}", e),
        }
    }

    for g in timely_guards.join() {
        g.expect("timely process failed");
    }

    Ok(())
}

enum GeneratorEvent {
    Progress(u64),
    Data(ColumnarRecords),
}

/// A source that reads from it's unbounded channel and returns a `Stream` of the contained records.
///
/// Only one worker is expected to read from the channel that the associated generator is writing
/// to.
fn generator_source<G>(
    scope: &G,
    source_id: usize,
    generator_rxs: Arc<Mutex<BTreeMap<usize, UnboundedReceiver<GeneratorEvent>>>>,
) -> Stream<G, (Vec<u8>, Vec<u8>)>
where
    G: Scope<Timestamp = u64>,
{
    let generator_rxs = Arc::clone(&generator_rxs);

    let scope = scope.clone();
    let worker_id = scope.index();
    let worker_count = scope.peers();

    let chosen_worker = usize::cast_from(source_id.hashed() % u64::cast_from(worker_count));
    let active_worker = chosen_worker == worker_id;

    let mut source_op = AsyncOperatorBuilder::new(format!("source-{source_id}"), scope.clone());

    let (mut output, output_stream) = source_op.new_output();

    let _shutdown_button = source_op.build(move |mut capabilities| async move {
        if !active_worker {
            return;
        }

        let mut cap = capabilities.pop().expect("missing capability");

        let mut generator_rxs = generator_rxs.lock().expect("lock poisoned");

        let mut generator_rx = generator_rxs
            .remove(&source_id)
            .expect("someone else took our channel");
        drop(generator_rxs);

        while let Some(event) = generator_rx.recv().await {
            match event {
                GeneratorEvent::Progress(ts) => {
                    trace!("source {source_id}, progress: {ts}");
                    cap.downgrade(&ts);
                }
                GeneratorEvent::Data(batch) => {
                    // TODO(aljoscha): Work with the diff field.
                    for record in batch
                        .iter()
                        .map(|((key, value), _ts, _diff)| (key.to_vec(), value.to_vec()))
                    {
                        // TODO(aljoscha): Is this the most efficient way of emitting all those
                        // records?
                        output.give(&cap, record).await;
                    }
                }
            }
        }
    });

    output_stream
}
