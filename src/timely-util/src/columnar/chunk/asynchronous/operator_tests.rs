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
        observed
            .borrow_mut()
            .wake_traces
            .push(Box::new(move || notify.notify_one()));
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

fn column(round: u64, rows: u64) -> Column<Update> {
    let mut column = Column::default();
    for row in 0..rows {
        let mut key = (round * rows + row).wrapping_add(0x9e3779b97f4a7c15);
        key = (key ^ (key >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        key = (key ^ (key >> 27)).wrapping_mul(0x94d049bb133111eb);
        key ^= key >> 31;
        let mut random = key;
        let pattern: Vec<u8> = (0..475)
            .map(|_| {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                random.to_le_bytes()[0]
            })
            .collect();
        column.push_into(((key, pattern.repeat(4)), round, 1i64));
    }
    column
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
    stats: PoolStats,
}

fn run(config: Config) -> Measurement {
    let timeout =
        Duration::from_secs(u64::try_from(parameter("MZ_BENCH_TIMEOUT_S", 1200)).unwrap());
    let workers = parameter("MZ_BENCH_WORKERS", 1);
    let idle = Duration::from_millis(u64::try_from(parameter("MZ_BENCH_IDLE_MS", 0)).unwrap());
    assert!(idle.is_zero() || parameter("MZ_BENCH_INDEPENDENT", 0) == 0);
    assert!(workers > 0 && config.sources > 0 && config.burst > 0);
    assert!(config.rounds > 0 && config.rows > 0);
    let mut timely_config = timely::Config::process(workers);
    let draining = Arc::new(AtomicBool::new(false));
    let hydrated_workers = Arc::new(AtomicUsize::new(0));
    let policy_draining = Arc::clone(&draining);
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
    pool.set_budget(config.pool_bytes);
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
                for _ in 0..config.sources {
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
                let start = Instant::now();
                if parameter("MZ_BENCH_INDEPENDENT", 0) != 0 {
                    let mut rounds = vec![0; config.sources];
                    while rounds.iter().any(|round| *round < config.rounds) {
                        for ((input, probe), round) in
                            inputs.iter_mut().zip_eq(&probes).zip_eq(&mut rounds)
                        {
                            while *round < config.rounds {
                                let required =
                                    round.saturating_sub(u64::try_from(config.burst - 1).unwrap());
                                if probe.less_than(&required) {
                                    break;
                                }
                                input.send_batch(&mut column(*round, config.rows));
                                *round += 1;
                                input.advance_to(*round);
                            }
                        }
                        worker.step();
                        std::thread::yield_now();
                        check_timeout(start, timeout, &budget, &observed.borrow(), &pool);
                    }
                } else {
                    for round in 0..config.rounds {
                        for input in &mut inputs {
                            input.send_batch(&mut column(round, config.rows));
                            input.advance_to(round + 1);
                        }
                        if usize::try_from(round + 1).unwrap() % config.burst == 0 {
                            worker.step();
                            while probes.iter().any(|probe| probe.less_than(&(round + 1))) {
                                worker.step();
                                std::thread::yield_now();
                                check_timeout(start, timeout, &budget, &observed.borrow(), &pool);
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
                    check_timeout(start, timeout, &budget, &observed.borrow(), &pool);
                }
                let hydrated = start.elapsed();
                // Normalize the terminal trace shape without changing the ingestion policy.
                if hydrated_workers.fetch_add(1, Ordering::Relaxed) + 1 == workers {
                    draining.store(true, Ordering::Relaxed);
                }
                let mut woke_drain = false;
                let mut previous = pool.stats();
                let mut stable_since = Instant::now();
                loop {
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
                    usize::try_from(config.rounds * config.rows).unwrap() * config.sources
                );
                let mut trace_batches = 0;
                for trace in &observed.traces {
                    let (batches, rows) = trace();
                    trace_batches += batches;
                    assert_eq!(rows, usize::try_from(config.rounds * config.rows).unwrap());
                }
                let result = Measurement {
                    hydrated,
                    elapsed,
                    batches: observed.batches.len(),
                    trace_batches,
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
                    "OPERATOR trace_batches={} workers={} idle_ms={} sample={sample} async={asynchronous} burst={burst} sources={} rows={} pool={} direct={} ms={} hydrated_ms={} batches={} inserts={} bytes={} reads={}",
                    m.trace_batches,
                    parameter("MZ_BENCH_WORKERS", 1),
                    parameter("MZ_BENCH_IDLE_MS", 0),
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
