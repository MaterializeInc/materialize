// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Measures the cost of driving a frequently woken logic future from the timely worker thread,
//! as `builder_async` does, against driving it from a tokio task.
//!
//! # Why
//!
//! An async operator built with `builder_async` runs its logic future on the timely worker. Every
//! wake of the future is a timely activation: the worker unparks if it was parked, steps,
//! schedules the dataflow, and polls the future. The premise under test is that this is far more
//! expensive than a tokio task wake, so an operator whose future wakes at a high rate, for
//! example awaiting a channel or socket that yields per item, would be cheaper to run as a tokio
//! task that ships its output back to the worker through channels. Such a builder would split an
//! operator in two halves: the task half owns `Send` proxies for capabilities and outputs and
//! runs the logic on the tokio runtime, the worker half owns the timely resources and is
//! activated only to apply the capability updates and records the task sends, or when the task
//! terminates.
//!
//! # What
//!
//! The logic future awaits `items` items from a bounded channel and does nothing else. In the
//! `timely` variant it is the logic of a `builder_async` operator. In the `tokio` variant it is
//! spawned as a tokio task, and a `builder_async` operator with a pending logic stands in for the
//! worker half described above: it is never scheduled during the run and is activated once when
//! the task terminates, which is all the worker half would do for a logic that sends nothing.
//! In both variants the operator holds the capability of its single output for the whole run, so
//! nothing sends data or capability updates and only scheduling is measured. The data path a
//! real tokio backed builder would add, one channel transfer per record, is deliberately left
//! out.
//!
//! Reported per item are wall clock time, process CPU time across all threads, timely worker
//! steps, polls of the logic future, and logged timely events, as medians over the repetitions.
//! CPU below wall time means a thread was waiting rather than working, and `polls/item` shows
//! how many items each wake drained.
//!
//! # Knobs
//!
//! The items arrive through a bounded tokio channel from either a plain OS thread
//! (`thread-producer`) or a tokio task (`task-producer`). The origin matters more than anything
//! else: a wake from a foreign thread is a remote wake for both variants, while a wake from a
//! tokio task lets tokio keep producer and consumer on one worker and skip the park entirely.
//!
//! With a channel capacity of one every item is a wake. With a larger capacity the producer runs
//! ahead while the consumer is being woken, so one wake drains several items and the park is
//! amortized, which is how real producers behave. The capacity sweep finds where the two
//! variants meet.
//!
//! With more than one timely worker only worker 0 consumes items. The others build the same
//! dataflow, hold their capabilities, and never wake, so any change in worker 0's cost comes from
//! timely's per step handling of its peers. Every worker parks with production's maintenance
//! timeout between activations unless `--busy` steps worker 0 without parking, which separates
//! the park and unpark from the rest of the timely cost.
//!
//! Run with `cargo bench -p mz-timely-util --bench async_builder_scheduling -- --help`.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread::Thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use mz_ore::cast::CastLossy;
use mz_timely_util::builder_async;
use timely::WorkerConfig;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::probe::Probe;
use timely::logging::{TimelyEvent, TimelyEventBuilder};
use timely::logging_core::Logger;
use timely::scheduling::SyncActivator;
use timely::worker::Worker;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
struct Options {
    /// Items the logic future awaits per run.
    #[arg(long, default_value_t = 20_000)]
    items: usize,
    #[arg(long, default_value_t = 1)]
    channel_capacity: usize,
    /// Measured runs per case after one warmup run, of which medians are reported.
    #[arg(long, default_value_t = 5)]
    reps: usize,
    #[arg(long, default_value_t = 1)]
    workers: usize,
    #[arg(long, default_value_t = 2)]
    tokio_threads: usize,
    /// Park timeout of every worker, production's maintenance interval by default.
    #[arg(long, default_value_t = 10)]
    park_ms: u64,
    /// Step worker 0 without ever parking.
    #[arg(long)]
    busy: bool,
    /// Scenarios to run, all by default.
    #[arg(long)]
    scenario: Vec<Scenario>,
    /// Passed by `cargo bench`.
    #[arg(long, hide = true)]
    bench: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Scenario {
    ThreadProducer,
    TaskProducer,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Impl {
    Timely,
    Tokio,
}

fn name(value: impl ValueEnum) -> String {
    value
        .to_possible_value()
        .expect("named")
        .get_name()
        .to_string()
}

/// User plus system CPU time of the whole process.
fn cpu_time() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` fully initializes `usage` when it returns zero, which is asserted
    // before the value is read.
    let usage = unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        usage.assume_init()
    };
    let duration = |tv: libc::timeval| {
        let (secs, micros) = (u64::try_from(tv.tv_sec), u64::try_from(tv.tv_usec));
        Duration::from_secs(secs.expect("non-negative"))
            + Duration::from_micros(micros.expect("non-negative"))
    };
    duration(usage.ru_utime) + duration(usage.ru_stime)
}

/// Waits for the producer to finish.
type Join = Box<dyn FnOnce()>;

/// Spawns the producer of a scenario, returning the consumer end of the channel and the producer.
fn spawn_producer(
    handle: &Handle,
    scenario: Scenario,
    opts: &Options,
) -> (mpsc::Receiver<()>, Join) {
    let (items, capacity) = (opts.items, opts.channel_capacity);
    let (tx, rx) = mpsc::channel(capacity);
    let join: Join = match scenario {
        Scenario::ThreadProducer => {
            let thread = std::thread::spawn(move || {
                for _ in 0..items {
                    tx.blocking_send(()).expect("consumer alive");
                }
            });
            Box::new(move || thread.join().expect("producer panicked"))
        }
        Scenario::TaskProducer => {
            let task = mz_ore::task::spawn(|| "producer", async move {
                for _ in 0..items {
                    tx.send(()).await.expect("consumer alive");
                }
            });
            let handle = handle.clone();
            Box::new(move || handle.block_on(task))
        }
    };
    (rx, join)
}

/// The measured logic: await `items` items and nothing else.
async fn consume(mut rx: mpsc::Receiver<()>, items: usize) {
    for _ in 0..items {
        rx.recv().await.expect("producer sends every item");
    }
}

type Logic = Pin<Box<dyn Future<Output = ()> + Send>>;
type LogBatch = Vec<(Duration, TimelyEvent)>;

/// Registers timely's logger on this worker with a sink that only counts events, returning the
/// counter.
fn install_logging(worker: &Worker) -> Rc<Cell<usize>> {
    let events = Rc::new(Cell::new(0));
    let counter = Rc::clone(&events);
    let sink = move |_: &Duration, data: &mut Option<LogBatch>| {
        if let Some(data) = data.take() {
            counter.set(counter.get() + data.len());
        }
    };
    let logger = Logger::<TimelyEventBuilder>::new(Instant::now(), Duration::ZERO, sink);
    worker
        .log_register()
        .expect("timely logging is enabled")
        .insert_logger("timely", logger);
    events
}

/// Activates an operator when dropped. Held by the tokio task driving the logic so that the
/// worker notices the task terminating without polling for it.
struct ActivateOnDrop(SyncActivator);

impl Drop for ActivateOnDrop {
    fn drop(&mut self) {
        let _ = self.0.activate();
    }
}

/// State shared by the worker threads.
struct Shared {
    /// Aligns the workers at the start of every run.
    barrier: Barrier,
    /// Completed runs, advanced by worker 0. The idle workers wait for it to pass their own run
    /// count before tearing their dataflow down.
    completed: AtomicUsize,
    /// The worker threads by index, so that worker 0 can unpark the idle ones.
    threads: Mutex<Vec<Option<Thread>>>,
}

/// The measurements of one run, all per item.
#[derive(Clone, Copy)]
struct Row {
    wall_ns: f64,
    cpu_ns: f64,
    steps: f64,
    polls: f64,
    events: f64,
}

fn run_case(
    worker: &mut Worker,
    handle: &Handle,
    shared: &Shared,
    events: &Cell<usize>,
    run: usize,
    imp: Impl,
    scenario: Scenario,
    opts: &Options,
) -> Row {
    let index = worker.index();
    let items = opts.items;
    let polls = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicBool::new(false));
    // Only worker 0 consumes the items and reports completion through the flag. The other
    // workers run a pending logic that holds its capability and never wakes.
    let (mut logic, join): (Logic, Option<Join>) = if index == 0 {
        let (rx, join) = spawn_producer(handle, scenario, opts);
        let (polls, finished) = (Arc::clone(&polls), Arc::clone(&finished));
        let mut logic = Box::pin(async move {
            consume(rx, items).await;
            finished.store(true, Ordering::Release);
        });
        let counted = std::future::poll_fn(move |cx| {
            polls.fetch_add(1, Ordering::Relaxed);
            logic.as_mut().poll(cx)
        });
        (Box::pin(counted), Some(join))
    } else {
        (Box::pin(std::future::pending()), None)
    };

    // The output's capability is held until the logic completes, so its release is the only
    // capability update of the run, and the probe observes the release of all peers at teardown.
    let (probe, token, driver) = worker.dataflow::<u64, _, _>(|scope| {
        let mut op = builder_async::OperatorBuilder::new("measured".to_string(), scope.clone());
        let (_output, stream) = op.new_output::<CapacityContainerBuilder<Vec<u64>>>();
        let (probe, _stream) = stream.probe();
        let driver = match imp {
            Impl::Tokio if index == 0 => {
                let address = op.operator_info().address.to_vec();
                let activate = ActivateOnDrop(scope.worker().sync_activator_for(address));
                let logic = std::mem::replace(&mut logic, Box::pin(std::future::pending()));
                Some(mz_ore::task::spawn(|| "driver", async move {
                    let _activate = activate;
                    logic.await
                }))
            }
            _ => None,
        };
        let button = op.build(move |caps| async move {
            let _caps = caps;
            logic.await
        });
        (probe, button.press_on_drop(), driver)
    });

    let park = Some(Duration::from_millis(opts.park_ms));
    let (events_start, cpu_start, start) = (events.get(), cpu_time(), Instant::now());
    let mut steps = 0;
    if index == 0 {
        while !finished.load(Ordering::Acquire) {
            if opts.busy {
                worker.step();
            } else {
                worker.step_or_park(park);
            }
            steps += 1;
        }
        shared.completed.fetch_add(1, Ordering::Release);
        for thread in shared.threads.lock().expect("poisoned").iter().flatten() {
            thread.unpark();
        }
    } else {
        while shared.completed.load(Ordering::Acquire) <= run {
            worker.step_or_park(park);
        }
    }
    let per_item = |count: usize| f64::cast_lossy(count) / f64::cast_lossy(items);
    let row = Row {
        wall_ns: start.elapsed().as_secs_f64() * 1e9 / f64::cast_lossy(items),
        cpu_ns: (cpu_time() - cpu_start).as_secs_f64() * 1e9 / f64::cast_lossy(items),
        steps: per_item(steps),
        polls: per_item(polls.load(Ordering::Relaxed)),
        events: per_item(events.get() - events_start),
    };

    if let Some(join) = join {
        join();
    }
    if let Some(driver) = driver {
        handle.block_on(driver);
    }
    // Tear the dataflow down before the next run. The frontier empties only once the presses and
    // capability releases of all peers have arrived, so keep stepping until then.
    drop(token);
    while !probe.done() {
        worker.step();
    }
    row
}

/// The field wise median of the rows.
fn median(rows: &[Row]) -> Row {
    let column = |field: fn(&Row) -> f64| {
        let mut values: Vec<f64> = rows.iter().map(field).collect();
        values.sort_by(f64::total_cmp);
        values[values.len() / 2]
    };
    Row {
        wall_ns: column(|row| row.wall_ns),
        cpu_ns: column(|row| row.cpu_ns),
        steps: column(|row| row.steps),
        polls: column(|row| row.polls),
        events: column(|row| row.events),
    }
}

fn main() {
    let opts = Options::parse();
    assert!(opts.workers > 0 && opts.channel_capacity > 0);
    let scenarios = if opts.scenario.is_empty() {
        vec![Scenario::ThreadProducer, Scenario::TaskProducer]
    } else {
        opts.scenario.clone()
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(opts.tokio_threads)
        .enable_all()
        .build()
        .expect("runtime");
    let handle = runtime.handle().clone();
    let (builders, other) = timely::CommunicationConfig::Process(opts.workers)
        .try_build()
        .expect("communication");
    let shared = Arc::new(Shared {
        barrier: Barrier::new(opts.workers),
        completed: AtomicUsize::new(0),
        threads: Mutex::new(vec![None; opts.workers]),
    });

    println!("{opts:?}");
    println!(
        "{:<15} | {:<6} | {:>12} {:>12} {:>10} {:>10} {:>11}",
        "scenario", "impl", "wall/item", "cpu/item", "steps/item", "polls/item", "events/item"
    );

    timely::execute::execute_from(builders, other, WorkerConfig::default(), move |worker| {
        let _guard = handle.enter();
        let index = worker.index();
        shared.threads.lock().expect("poisoned")[index] = Some(std::thread::current());
        let events = install_logging(worker);
        shared.barrier.wait();

        let mut run = 0;
        for &scenario in &scenarios {
            for imp in [Impl::Timely, Impl::Tokio] {
                let mut rows = Vec::new();
                for rep in 0..=opts.reps {
                    shared.barrier.wait();
                    let row =
                        run_case(worker, &handle, &shared, &events, run, imp, scenario, &opts);
                    run += 1;
                    if rep > 0 {
                        rows.push(row);
                    }
                }
                if index == 0 {
                    let row = median(&rows);
                    println!(
                        "{:<15} | {:<6} | {:>9.0} ns {:>9.0} ns {:>10.2} {:>10.2} {:>11.2}",
                        name(scenario),
                        name(imp),
                        row.wall_ns,
                        row.cpu_ns,
                        row.steps,
                        row.polls,
                        row.events,
                    );
                }
            }
        }
    })
    .expect("timely")
    .join();
}
