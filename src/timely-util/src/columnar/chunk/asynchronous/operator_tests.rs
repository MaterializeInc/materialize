// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Live-operator checks and an opt-in comparison against the synchronous arranger.
//!
//! The benchmark uses deterministic 1,900-byte values with a repeated 475-byte
//! pattern, overlapping key ranges, and independently advancing sources. All
//! sources share one worker, pool, and decoded-input budget. It reports output
//! frontier latency separately from the subsequent maintenance drain. The latter
//! ends after admission is released and pool counters stay quiet for 20 ms.
//! This exercises pool eviction and compression, not forced device contention.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
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
}

fn observe<'scope, Tr>(
    arranged: Arranged<'scope, TraceAgent<Tr>>,
    observed: Rc<RefCell<Observed>>,
    probe: &ProbeHandle,
) where
    Tr: TraceReader<Time = u64, Batch = TestBatch> + 'static,
{
    let mut trace = arranged.trace;
    arranged
        .stream
        .inspect_batch(move |_, batches| {
            for batch in batches {
                let mut observed = observed.borrow_mut();
                observed.batches.push(batch.len());
                if let Some(rows) = &mut observed.rows {
                    for chunk in &batch.chunks {
                        let column = chunk.clone().into_column();
                        let view = column.borrow();
                        rows.extend((0..view.len()).map(|i| Update::into_owned(view.get(i))));
                    }
                }
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
        observe(arranged, observed, probe);
        None
    };
    (input, token)
}

fn columns(rounds: u64, rows: u64) -> Vec<Column<Update>> {
    (0..rounds)
        .map(|round| {
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
        })
        .collect()
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
    stats: PoolStats,
}

fn run(config: Config) -> Measurement {
    let mut timely_config = timely::Config::thread();
    let logic: ExertionLogic = Arc::new(|levels| {
        let mut proportionality = 16;
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
    timely::execute(timely_config, move |worker| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        let pool = Pool::new().unwrap();
        pool.set_spill_threads(0);
        pool.set_budget(config.pool_bytes);
        super::super::set_direct_compressed_output_override(Some(config.direct_output));
        with_spill_override(pool.clone(), || {
            let probe = ProbeHandle::new();
            let observed = Rc::new(RefCell::new(Observed::default()));
            let budget = ReadBudget::new(32 << 20);
            let mut inputs = Vec::new();
            let mut tokens = Vec::new();
            for _ in 0..config.sources {
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
            }
            let mut fixtures: Vec<_> = (0..config.sources)
                .map(|_| columns(config.rounds, config.rows).into_iter())
                .collect();
            let start = Instant::now();
            for round in 0..config.rounds {
                for (input, fixture) in inputs.iter_mut().zip_eq(&mut fixtures) {
                    input.send_batch(&mut fixture.next().unwrap());
                    input.advance_to(round + 1);
                }
                worker.step();
                if usize::try_from(round + 1).unwrap() % config.burst == 0 {
                    while probe.less_than(&(round + 1)) {
                        worker.step();
                        std::thread::yield_now();
                        assert!(start.elapsed().as_secs() < 120);
                    }
                }
            }
            while probe.less_than(&config.rounds) {
                worker.step();
                std::thread::yield_now();
                assert!(start.elapsed().as_secs() < 120);
            }
            let hydrated = start.elapsed();
            let mut previous = pool.stats();
            let mut stable_since = Instant::now();
            loop {
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
                if budget.reserved_bytes() == 0 && stable_since.elapsed().as_millis() >= 20 {
                    break;
                }
                assert!(start.elapsed().as_secs() < 120);
            }
            let elapsed = start.elapsed();
            let observed = observed.borrow();
            assert_eq!(
                observed.batches.iter().sum::<usize>(),
                usize::try_from(config.rounds * config.rows).unwrap() * config.sources
            );
            let result = Measurement {
                hydrated,
                elapsed,
                batches: observed.batches.len(),
                stats: pool.stats(),
            };
            drop(tokens);
            result
        })
    })
    .unwrap()
    .join()
    .pop()
    .unwrap()
    .unwrap()
}

fn parameter(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |s| s.parse().unwrap())
}

#[mz_ore::test]
#[ignore = "local operator performance comparison"]
fn operator_microbench() {
    let bursts = std::env::var("MZ_BENCH_BURST")
        .map_or_else(|_| vec![1, 8, 32], |s| vec![s.parse().unwrap()]);
    for sample in 0..parameter("MZ_BENCH_SAMPLES", 1) {
        for &burst in &bursts {
            for asynchronous in if sample % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let config = Config {
                    rounds: u64::try_from(parameter("MZ_BENCH_ROUNDS", 32)).unwrap(),
                    rows: u64::try_from(parameter("MZ_BENCH_ROWS", 512)).unwrap(),
                    sources: parameter("MZ_BENCH_SOURCES", 1),
                    pool_bytes: parameter("MZ_BENCH_POOL_BYTES", 0),
                    burst,
                    asynchronous,
                    direct_output: parameter("MZ_BENCH_DIRECT", 0) != 0,
                };
                let m = run(config);
                eprintln!(
                    "OPERATOR sample={sample} async={asynchronous} burst={burst} sources={} rows={} pool={} direct={} ms={} hydrated_ms={} batches={} inserts={} bytes={} reads={}",
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
