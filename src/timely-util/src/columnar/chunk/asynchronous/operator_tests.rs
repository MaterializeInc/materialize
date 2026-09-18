// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Live-operator checks and an opt-in comparison against the synchronous arranger.
//!
//! The benchmark uses deterministic 1,900-byte values with a repeated 475-byte
//! pattern, overlapping key ranges, and independently advancing sources.
//! Sources share a pool and decoded-input budget across the configured workers.
//! Input generation is incremental and included in the reported times. The fixture
//! does not retain the entire input history. It reports output frontier latency
//! separately from final compaction. After ingestion, the benchmark policy requests
//! one non-empty batch per source. The drain waits for that shape, released read
//! admission, and pool counters that remain quiet for 20 ms.
//! `MZ_BENCH_IDLE_MS` gives maintenance time between completed input bursts while
//! continuing to schedule the worker. It applies to the coordinated-input mode.
//! This exercises pool eviction and compression, not forced device contention.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use columnar::{Columnar, Index, Len};
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::chunk::{ChunkBatch, ChunkBatcher, ChunkBuilder, ChunkSpine};
use differential_dataflow::trace::{BatchReader, ExertionLogic, TraceReader};
use itertools::Itertools;
use mz_ore::pool::{Pool, PoolStats};
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Input, Inspect, Probe};
use timely::dataflow::{InputHandle, Scope};

use super::super::merge::ReadBudget;
use super::super::{ChunkChunker, Column, ColumnChunk, with_spill_override};

type Update = ((u64, Vec<u8>), u64, i64);
type TestChunk = ColumnChunk<(u64, Vec<u8>), u64, i64>;
type TestBatch = Rc<ChunkBatch<TestChunk>>;
type InputPort = InputHandle<u64, CapacityContainerBuilder<Column<Update>>>;
type ProbeHandle = timely::dataflow::operators::probe::Handle<u64>;

#[derive(Default)]
struct Observed {
    batches: Vec<usize>,
    rows: Option<Vec<Update>>,
    traces: Vec<Box<dyn Fn() -> (usize, usize)>>,
    wake_traces: Vec<Box<dyn Fn()>>,
    /// Async spines' layer shapes, for `MZ_BENCH_DUMP_SHAPE`.
    shapes: Vec<Box<dyn Fn() -> String>>,
}

fn observe<'scope, Tr>(
    arranged: Arranged<'scope, TraceAgent<Tr>>,
    observed: Rc<RefCell<Observed>>,
    probe: &ProbeHandle,
) where
    Tr: TraceReader<Time = u64, Batch = TestBatch> + 'static,
{
    // Share this reader so sampling does not introduce another compaction hold.
    let trace = Rc::new(RefCell::new(arranged.trace));
    let inspected = Rc::clone(&trace);
    observed.borrow_mut().traces.push(Box::new(move || {
        let mut batches = 0;
        let mut rows = 0;
        inspected.borrow().map_batches(|batch| {
            batches += usize::from(!batch.is_empty());
            rows += batch.len();
        });
        (batches, rows)
    }));
    arranged
        .stream
        .inspect_batch(move |_, batches| {
            for batch in batches {
                let mut observed = observed.borrow_mut();
                observed.batches.push(batch.len());
                super::super::metrics::record_batch(batch.len());
                if let Some(rows) = &mut observed.rows {
                    for chunk in &batch.chunks {
                        let column = chunk.clone().into_column();
                        let view = column.borrow();
                        rows.extend((0..view.len()).map(|i| Update::into_owned(view.get(i))));
                    }
                }
                let mut trace = trace.borrow_mut();
                trace.set_logical_compaction(batch.upper().borrow());
                trace.set_physical_compaction(batch.upper().borrow());
            }
        })
        .probe_with(probe);
}

fn install(
    scope: Scope<'_, u64>,
    asynchronous: bool,
    budget: ReadBudget,
    observed: Rc<RefCell<Observed>>,
    probe: &ProbeHandle,
) -> (InputPort, Option<super::PressOnDropButton>) {
    let (input, stream) = scope.new_input::<Column<Update>>();
    let token = if asynchronous {
        let (arranged, token) = super::arrange(stream, budget, "Async");
        let notify = Arc::clone(&arranged.trace.trace_box_unstable().borrow().trace().notify);
        let state = Rc::clone(&arranged.trace.trace_box_unstable().borrow().trace().state);
        observed
            .borrow_mut()
            .wake_traces
            .push(Box::new(move || notify.notify_one()));
        observed.borrow_mut().shapes.push(Box::new(move || {
            let (layers, pending) = state.borrow().shape();
            format!("pending={pending} layers={layers:?}")
        }));
        observe(arranged, observed, probe);
        Some(token)
    } else {
        #[allow(clippy::disallowed_methods)]
        let arranged = arrange_core::<
            _,
            _,
            ChunkChunker<(u64, Vec<u8>), u64, i64>,
            ChunkBatcher<TestChunk>,
            ChunkBuilder<TestChunk>,
            ChunkSpine<TestChunk>,
        >(stream, Pipeline, "Sync");
        let activator = scope.activator_for(Rc::clone(&arranged.trace.operator().address));
        observed
            .borrow_mut()
            .wake_traces
            .push(Box::new(move || activator.activate()));
        observe(arranged, observed, probe);
        None
    };
    (input, token)
}

/// One source's update generator.
///
/// `MZ_BENCH_KEY_SPACE` bounds the keys. A repeated key retracts its previous
/// value alongside the new insert, the shape an upsert operator's feedback
/// takes, so the trace only shrinks to the live keys once merges consolidate.
struct Generator {
    key_space: u64,
    live: std::collections::BTreeMap<u64, u64>,
    emitted: usize,
}

impl Generator {
    fn new() -> Self {
        Generator {
            key_space: u64::try_from(parameter("MZ_BENCH_KEY_SPACE", 0)).unwrap(),
            live: Default::default(),
            emitted: 0,
        }
    }

    /// One round's updates at `time`, consolidated the way the batcher publishes them.
    fn column(&mut self, time: u64, round: u64, rows: u64) -> Column<Update> {
        let mut updates = Vec::new();
        for row in 0..rows {
            let seed = mix(round * rows + row);
            let key = if self.key_space == 0 {
                seed
            } else {
                seed % self.key_space
            };
            if self.key_space != 0 {
                if let Some(previous) = self.live.insert(key, seed) {
                    updates.push(((key, value(previous)), time, -1i64));
                }
            }
            updates.push(((key, value(seed)), time, 1i64));
        }
        if self.key_space != 0 {
            differential_dataflow::consolidation::consolidate_updates(&mut updates);
        }
        self.emitted += updates.len();
        let mut column = Column::default();
        for update in &updates {
            column.push_into(update);
        }
        column
    }

    /// Rows a fully consolidated trace retains.
    fn live_keys(&self) -> usize {
        if self.key_space == 0 {
            self.emitted
        } else {
            self.live.len()
        }
    }
}

fn mix(mut key: u64) -> u64 {
    key = key.wrapping_add(0x9e3779b97f4a7c15);
    key = (key ^ (key >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    key = (key ^ (key >> 27)).wrapping_mul(0x94d049bb133111eb);
    key ^ (key >> 31)
}

fn value(mut random: u64) -> Vec<u8> {
    let pattern: Vec<u8> = (0..475)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random.to_le_bytes()[0]
        })
        .collect();
    pattern.repeat(4)
}

/// Send one round, as a single message or split into `message_rows`-row messages.
fn send_round(
    input: &mut InputPort,
    generator: &mut Generator,
    time: u64,
    round: u64,
    rows: u64,
    message_rows: usize,
) {
    let mut column = generator.column(time, round, rows);
    if message_rows == 0 || message_rows >= usize::try_from(rows).unwrap() {
        input.send_batch(&mut column);
        return;
    }
    let view = column.borrow();
    let len = view.len();
    let mut start = 0;
    while start < len {
        let end = (start + message_rows).min(len);
        let mut part: Column<Update> = Column::default();
        for i in start..end {
            part.push_into(&Update::into_owned(view.get(i)));
        }
        input.send_batch(&mut part);
        start = end;
    }
}

#[derive(Clone, Copy)]
struct Config {
    rounds: u64,
    rows: u64,
    sources: usize,
    pool_bytes: usize,
    burst: usize,
    asynchronous: bool,
    direct_output: bool,
}

struct Measurement {
    hydrated: Duration,
    elapsed: Duration,
    batches: usize,
    trace_batches: usize,
    /// Non-empty trace batches and retained rows when the last input round published.
    hydrated_batches: usize,
    hydrated_rows: usize,
    peaks: Peaks,
    /// Per-round publication latency of the probe sources: worst and 99th percentile.
    probe_max: Duration,
    probe_p99: Duration,
    grants: usize,
    stats: PoolStats,
}

/// Maxima observed at worker steps during ingestion.
#[derive(Default, Clone, Copy)]
struct Peaks {
    /// Non-empty trace batches across the worker's traces.
    batches: usize,
    /// Pool-resident bytes.
    resident: u64,
    /// Decoded bytes admitted under the shared read budget.
    reserved: usize,
}

fn sample(observed: &Observed, pool: &Pool, budget: &ReadBudget, peaks: &mut Peaks) {
    let batches = observed.traces.iter().map(|trace| trace().0).sum();
    peaks.batches = peaks.batches.max(batches);
    peaks.resident = peaks.resident.max(pool.stats().resident_bytes);
    peaks.reserved = peaks.reserved.max(budget.reserved_bytes());
}

fn run(config: Config) -> Measurement {
    let timeout =
        Duration::from_secs(u64::try_from(parameter("MZ_BENCH_TIMEOUT_S", 1200)).unwrap());
    let workers = parameter("MZ_BENCH_WORKERS", 1);
    let idle = Duration::from_millis(u64::try_from(parameter("MZ_BENCH_IDLE_MS", 0)).unwrap());
    assert!(idle.is_zero() || parameter("MZ_BENCH_INDEPENDENT", 0) == 0);
    // Small sources sharing the worker with the configured ones. Their
    // per-round publication latency shows how much a large source's
    // maintenance stalls its neighbours.
    let probe_sources = parameter("MZ_BENCH_PROBE_SOURCES", 0);
    let probe_rows = u64::try_from(parameter("MZ_BENCH_PROBE_ROWS", 64)).unwrap();
    assert!(probe_sources == 0 || parameter("MZ_BENCH_INDEPENDENT", 0) == 0);
    assert!(workers > 0 && config.sources > 0 && config.burst > 0);
    assert!(config.rounds > 0 && config.rows > 0);
    let mut timely_config = timely::Config::process(workers);
    let draining = Arc::new(AtomicBool::new(false));
    let hydrated_workers = Arc::new(AtomicUsize::new(0));
    let policy_draining = Arc::clone(&draining);
    // Optional grants during ingestion, counted identically for both spines.
    let grants = Arc::new(AtomicUsize::new(0));
    let policy_grants = Arc::clone(&grants);
    // Match the storage TimelyConfig in mz_controller::clusters.
    let exert_proportionality = parameter("MZ_BENCH_EXERT_PROPORTIONALITY", 1337);
    let logic: ExertionLogic = Arc::new(move |levels| {
        if policy_draining.load(Ordering::Relaxed) {
            let batches = levels
                .iter()
                .filter(|(_, _, rows)| *rows > 0)
                .map(|(_, count, _)| count)
                .sum::<usize>();
            return (batches > 1).then_some(1000);
        }
        if exert_proportionality == 0 {
            return None;
        }
        let mut proportionality = exert_proportionality;
        let mut first = true;
        for (_, count, len) in levels
            .iter()
            .copied()
            .skip_while(|(_, count, _)| *count == 0)
        {
            if count > 1 || (!first && proportionality > 0 && len > 0) {
                policy_grants.fetch_add(1, Ordering::Relaxed);
                return Some(1000);
            }
            first = false;
            proportionality /= 2;
        }
        None
    });
    timely_config
        .worker
        .set("differential/default_exert_logic".into(), logic);
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers.clamp(2, 4))
            .enable_all()
            .build()
            .unwrap(),
    );
    let pool = Pool::new().unwrap();
    pool.set_spill_threads(parameter("MZ_BENCH_SPILL_THREADS", 0));
    pool.set_runtime_reads(parameter("MZ_BENCH_TOKIO_READS", 0) != 0);
    pool.set_budget(config.pool_bytes);
    pool.set_read_delay(Duration::from_micros(
        u64::try_from(parameter("MZ_BENCH_READ_DELAY_US", 0)).unwrap(),
    ));
    let worker_pool = pool.clone();
    let budget =
        ReadBudget::new(u32::try_from(parameter("MZ_BENCH_READ_BYTES", 256 << 20)).unwrap());
    let barrier = Arc::new(std::sync::Barrier::new(workers));
    let worker_runtime = Arc::clone(&runtime);
    let worker_budget = budget.clone();
    let results = timely::execute(timely_config, move |worker| {
        let _entered = worker_runtime.enter();
        let budget = worker_budget.clone();
        let pool = worker_pool.clone();
        super::super::set_direct_compressed_output_override(Some(config.direct_output));
        let mut dataflows = Vec::new();
        let measure = || {
            with_spill_override(pool.clone(), || {
                let mut probes = Vec::new();
                let observed = Rc::new(RefCell::new(Observed::default()));
                let mut inputs = Vec::new();
                let mut tokens = Vec::new();
                for _ in 0..config.sources + probe_sources {
                    let probe = ProbeHandle::new();
                    dataflows.push(worker.next_dataflow_index());
                    let (input, token) = worker.dataflow(|scope| {
                        install(
                            scope,
                            config.asynchronous,
                            budget.clone(),
                            Rc::clone(&observed),
                            &probe,
                        )
                    });
                    inputs.push(input);
                    tokens.push(token);
                    probes.push(probe);
                }
                barrier.wait();
                let mut generators: Vec<Generator> = (0..config.sources + probe_sources)
                    .map(|_| Generator::new())
                    .collect();
                let mut peaks = Peaks::default();
                let mut probe_latencies: Vec<Duration> = Vec::new();
                let start = Instant::now();
                if parameter("MZ_BENCH_INDEPENDENT", 0) != 0 {
                    let mut rounds = vec![0; config.sources];
                    while rounds.iter().any(|round| *round < config.rounds) {
                        for (((input, probe), round), generator) in inputs
                            .iter_mut()
                            .zip_eq(&probes)
                            .zip_eq(&mut rounds)
                            .zip_eq(&mut generators)
                        {
                            while *round < config.rounds {
                                let required =
                                    round.saturating_sub(u64::try_from(config.burst - 1).unwrap());
                                if probe.less_than(&required) {
                                    break;
                                }
                                input.send_batch(&mut generator.column(
                                    *round,
                                    *round,
                                    config.rows,
                                ));
                                *round += 1;
                                input.advance_to(*round);
                            }
                        }
                        worker.step();
                        std::thread::yield_now();
                        sample(&observed.borrow(), &pool, &budget, &mut peaks);
                        check_timeout(start, timeout, &budget, &observed.borrow(), &pool);
                    }
                } else {
                    let message_rows = parameter("MZ_BENCH_MESSAGE_ROWS", 0);
                    let tick_steps = parameter("MZ_BENCH_TICK_STEPS", 0);
                    // The first `snapshot_rounds` rounds share timestamp zero and
                    // no frontier advance separates them, the shape a source's
                    // initial snapshot presents to the arranger.
                    let snapshot_rounds =
                        u64::try_from(parameter("MZ_BENCH_SNAPSHOT_ROUNDS", 0)).unwrap();
                    for round in 0..config.rounds {
                        let time = if round < snapshot_rounds { 0 } else { round };
                        let advance = round + 1 > snapshot_rounds;
                        for (index, (input, generator)) in
                            inputs.iter_mut().zip_eq(&mut generators).enumerate()
                        {
                            let rows = if index < config.sources {
                                config.rows
                            } else {
                                probe_rows
                            };
                            send_round(input, generator, time, round, rows, message_rows);
                            if advance {
                                input.advance_to(round + 1);
                            }
                        }
                        // Activations per input tick, independent of wall-clock
                        // speed. Both spines receive one exertion turn per step.
                        for _ in 0..tick_steps {
                            worker.step();
                            sample(&observed.borrow(), &pool, &budget, &mut peaks);
                            check_timeout(start, timeout, &budget, &observed.borrow(), &pool);
                        }
                        if advance && usize::try_from(round + 1).unwrap() % config.burst == 0 {
                            let round_start = Instant::now();
                            let mut probe_latency = None;
                            worker.step();
                            while probes.iter().any(|probe| probe.less_than(&(round + 1))) {
                                if probe_latency.is_none()
                                    && probe_sources > 0
                                    && probes[config.sources..]
                                        .iter()
                                        .all(|probe| !probe.less_than(&(round + 1)))
                                {
                                    probe_latency = Some(round_start.elapsed());
                                }
                                worker.step();
                                std::thread::yield_now();
                                sample(&observed.borrow(), &pool, &budget, &mut peaks);
                                check_timeout(start, timeout, &budget, &observed.borrow(), &pool);
                            }
                            if probe_sources > 0 {
                                probe_latencies
                                    .push(probe_latency.unwrap_or_else(|| round_start.elapsed()));
                            }
                            let idle_start = Instant::now();
                            while idle_start.elapsed() < idle {
                                worker.step();
                                std::thread::yield_now();
                                check_timeout(start, timeout, &budget, &observed.borrow(), &pool);
                            }
                        }
                    }
                }
                while probes.iter().any(|probe| probe.less_than(&config.rounds)) {
                    worker.step();
                    std::thread::yield_now();
                    sample(&observed.borrow(), &pool, &budget, &mut peaks);
                    check_timeout(start, timeout, &budget, &observed.borrow(), &pool);
                }
                let hydrated = start.elapsed();
                if parameter("MZ_BENCH_DUMP_SHAPE", 0) != 0 {
                    for shape in &observed.borrow().shapes {
                        eprintln!("SHAPE at hydration: {}", shape());
                    }
                }
                let (hydrated_batches, hydrated_rows) = observed
                    .borrow()
                    .traces
                    .iter()
                    .map(|trace| trace())
                    .fold((0, 0), |sum, next| (sum.0 + next.0, sum.1 + next.1));
                let ingestion_grants = grants.load(Ordering::Relaxed);
                // Optional consolidation is funded by input frontier advances, so
                // the drain keeps ticking the inputs the way a live source does.
                // Closing them instead would tear the synchronous operator down.
                let mut tick = config.rounds + 1;
                // Normalize the terminal trace shape without changing the ingestion policy.
                if hydrated_workers.fetch_add(1, Ordering::Relaxed) + 1 == workers {
                    draining.store(true, Ordering::Relaxed);
                }
                let mut woke_drain = false;
                let mut previous = pool.stats();
                let mut stable_since = Instant::now();
                loop {
                    for input in &mut inputs {
                        input.advance_to(tick);
                    }
                    tick += 1;
                    if !woke_drain && draining.load(Ordering::Relaxed) {
                        for wake in &observed.borrow().wake_traces {
                            wake();
                        }
                        woke_drain = true;
                    }
                    worker.step();
                    std::thread::yield_now();
                    let current = pool.stats();
                    if current.inserts != previous.inserts
                        || current.async_reads != previous.async_reads
                        || current.async_reads_in_flight != 0
                    {
                        stable_since = Instant::now();
                    }
                    previous = current;
                    if woke_drain
                        && budget.reserved_bytes() == 0
                        && stable_since.elapsed().as_millis() >= 20
                        && observed.borrow().traces.iter().all(|trace| trace().0 == 1)
                    {
                        break;
                    }
                    check_timeout(start, timeout, &budget, &observed.borrow(), &pool);
                }
                let elapsed = start.elapsed();
                let observed = observed.borrow();
                assert_eq!(
                    observed.batches.iter().sum::<usize>(),
                    generators.iter().map(|g| g.emitted).sum::<usize>()
                );
                let mut trace_batches = 0;
                for (trace, generator) in observed.traces.iter().zip_eq(&generators) {
                    let (batches, rows) = trace();
                    trace_batches += batches;
                    assert_eq!(rows, generator.live_keys());
                }
                probe_latencies.sort();
                let probe_max: Duration =
                    probe_latencies.iter().max().map_or(Duration::ZERO, |d| *d);
                let p99_index = probe_latencies.len().saturating_sub(1) * 99 / 100;
                let probe_p99: Duration = probe_latencies
                    .iter()
                    .nth(p99_index)
                    .map_or(Duration::ZERO, |d| *d);
                let result = Measurement {
                    hydrated,
                    elapsed,
                    batches: observed.batches.len(),
                    trace_batches,
                    hydrated_batches,
                    hydrated_rows,
                    peaks,
                    probe_max,
                    probe_p99,
                    grants: ingestion_grants,
                    stats: pool.stats(),
                };
                drop(tokens);
                result
            })
        };
        let result = mz_ore::panic::catch_unwind(std::panic::AssertUnwindSafe(measure));
        // A failed assertion must also destroy dataflows while Tokio is entered.
        for index in dataflows {
            worker.drop_dataflow(index);
        }
        result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
    .unwrap()
    .join();
    // Cancelled operator reads retain admission until their owned tasks finish.
    // Keep Tokio alive for those tasks before propagating a worker panic.
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            while budget.reserved_bytes() > 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("cancelled reads release admission");
    });
    let mut results = results.into_iter().map(Result::unwrap);
    let mut result = results.next().unwrap();
    for next in results {
        result.elapsed = result.elapsed.max(next.elapsed);
        result.hydrated = result.hydrated.max(next.hydrated);
        result.batches += next.batches;
        result.trace_batches += next.trace_batches;
        result.hydrated_batches += next.hydrated_batches;
        result.hydrated_rows += next.hydrated_rows;
        result.peaks.batches += next.peaks.batches;
        result.peaks.resident = result.peaks.resident.max(next.peaks.resident);
        result.peaks.reserved = result.peaks.reserved.max(next.peaks.reserved);
        result.probe_max = result.probe_max.max(next.probe_max);
        result.probe_p99 = result.probe_p99.max(next.probe_p99);
        result.grants = result.grants.max(next.grants);
    }
    result.stats = pool.stats();
    result
}

fn check_timeout(
    start: Instant,
    timeout: Duration,
    budget: &ReadBudget,
    observed: &Observed,
    pool: &Pool,
) {
    if start.elapsed() >= timeout {
        let stats = pool.stats();
        panic!(
            "operator benchmark timed out: reserved={} published={} reads={} in_flight={}",
            budget.reserved_bytes(),
            observed.batches.iter().sum::<usize>(),
            stats.async_reads,
            stats.async_reads_in_flight,
        );
    }
}

fn parameter(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |s| s.parse().unwrap())
}

#[mz_ore::test]
#[ignore = "local operator performance comparison"]
fn operator_microbench() {
    let selected = std::env::var("MZ_BENCH_ASYNC")
        .ok()
        .map(|value| value.parse::<bool>().unwrap());
    let bursts = std::env::var("MZ_BENCH_BURST")
        .map_or_else(|_| vec![1, 8, 32], |s| vec![s.parse().unwrap()]);
    for sample in 0..parameter("MZ_BENCH_SAMPLES", 1) {
        for &burst in &bursts {
            for asynchronous in if sample % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                if selected.is_some_and(|value| value != asynchronous) {
                    continue;
                }
                let config = Config {
                    rounds: u64::try_from(parameter("MZ_BENCH_ROUNDS", 32)).unwrap(),
                    rows: u64::try_from(parameter("MZ_BENCH_ROWS", 512)).unwrap(),
                    sources: parameter("MZ_BENCH_SOURCES", 1),
                    pool_bytes: parameter("MZ_BENCH_POOL_BYTES", 0),
                    burst,
                    asynchronous,
                    direct_output: parameter("MZ_BENCH_DIRECT", 0) != 0,
                };
                let registry = mz_ore::metrics::MetricsRegistry::new();
                super::super::metrics::register(&registry);
                let before = registry.gather();
                let m = run(config);
                let after = registry.gather();
                for (before, after) in before.iter().zip_eq(&after) {
                    let family = after.name();
                    if after.name().ends_with("size_total") {
                        continue;
                    }
                    for (before, after) in before.get_metric().iter().zip_eq(after.get_metric()) {
                        eprintln!(
                            "WORK async={asynchronous} family={family} stage={} value={}",
                            after.get_label()[0].value(),
                            after.get_gauge().as_ref().unwrap().value()
                                - before.get_gauge().as_ref().unwrap().value(),
                        );
                    }
                }
                eprintln!(
                    "OPERATOR spill_threads={} tokio_reads={} spill_reads={} probe_sources={} probe_rows={} read_delay_us={} probe_max_ms={} probe_p99_ms={} key_space={} snapshot_rounds={} unfunded={} hydrated_batches={} hydrated_rows={} peak_batches={} peak_resident={} peak_reserved={} trace_batches={} workers={} idle_ms={} tick_steps={} message_rows={} grants={} sample={sample} async={asynchronous} burst={burst} sources={} rows={} pool={} direct={} ms={} hydrated_ms={} batches={} inserts={} bytes={} reads={}",
                    parameter("MZ_BENCH_SPILL_THREADS", 0),
                    parameter("MZ_BENCH_TOKIO_READS", 0),
                    m.stats.spill_reads,
                    parameter("MZ_BENCH_PROBE_SOURCES", 0),
                    parameter("MZ_BENCH_PROBE_ROWS", 64),
                    parameter("MZ_BENCH_READ_DELAY_US", 0),
                    m.probe_max.as_millis(),
                    m.probe_p99.as_millis(),
                    parameter("MZ_BENCH_KEY_SPACE", 0),
                    parameter("MZ_BENCH_SNAPSHOT_ROUNDS", 0),
                    std::env::var_os("MZ_BENCH_UNFUNDED").is_some(),
                    m.hydrated_batches,
                    m.hydrated_rows,
                    m.peaks.batches,
                    m.peaks.resident,
                    m.peaks.reserved,
                    m.trace_batches,
                    parameter("MZ_BENCH_WORKERS", 1),
                    parameter("MZ_BENCH_IDLE_MS", 0),
                    parameter("MZ_BENCH_TICK_STEPS", 0),
                    parameter("MZ_BENCH_MESSAGE_ROWS", 0),
                    m.grants,
                    config.sources,
                    config.rounds * config.rows,
                    config.pool_bytes,
                    config.direct_output,
                    m.elapsed.as_millis(),
                    m.hydrated.as_millis(),
                    m.batches,
                    m.stats.inserts,
                    m.stats.extent_bytes_written,
                    m.stats.async_reads
                );
            }
        }
    }
}

#[mz_ore::test]
fn queued_input_preserves_updates_and_completes_progress() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _entered = runtime.enter();
    for burst in [1, 80] {
        timely::execute_directly(move |worker| {
            let pool = Pool::new().unwrap();
            pool.set_spill_threads(0);
            pool.set_budget(0);
            with_spill_override(pool, || {
                let observed = Rc::new(RefCell::new(Observed {
                    rows: Some(Vec::new()),
                    ..Observed::default()
                }));
                let probe = ProbeHandle::new();
                let (mut input, token) = worker.dataflow(|scope| {
                    install(
                        scope,
                        true,
                        ReadBudget::new(1 << 20),
                        Rc::clone(&observed),
                        &probe,
                    )
                });
                let mut expected = Vec::new();
                let start = Instant::now();
                // Exceed the drain limit, with future updates and cancellation.
                for round in 0..80 {
                    let rows = [
                        ((round % 7, vec![42; 128]), round + 2, 1),
                        ((round % 7, vec![42; 128]), round + 2, -1),
                        ((round, vec![17; 128]), round + 3, 1),
                    ];
                    let mut column = Column::default();
                    for row in &rows {
                        column.push_into(row);
                    }
                    expected.extend(rows);
                    input.send_batch(&mut column);
                    input.advance_to(round + 1);
                    if (round + 1) % burst == 0 {
                        while probe.less_than(input.time()) {
                            worker.step();
                            std::thread::yield_now();
                            assert!(start.elapsed().as_secs() < 10);
                        }
                    }
                }
                drop(input);
                while !probe.done() {
                    worker.step();
                    std::thread::yield_now();
                    assert!(start.elapsed().as_secs() < 10);
                }
                let mut actual = observed.borrow_mut().rows.take().unwrap();
                differential_dataflow::consolidation::consolidate_updates(&mut actual);
                differential_dataflow::consolidation::consolidate_updates(&mut expected);
                assert_eq!(actual, expected);
                drop(token);
            });
        });
    }
}
