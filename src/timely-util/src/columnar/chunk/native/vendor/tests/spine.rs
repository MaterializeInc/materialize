// Copyright (c) 2015 Frank McSherry
// SPDX-License-Identifier: MIT
// See ../LICENSE.

use super::super::spine::{Exertion, Spine};
use differential_dataflow_next::trace::asynchronous::{Batch as SpineBatch, MergeStatus, Merger};
use differential_dataflow_next::trace::{Description, Span};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use timely_next::dataflow::operators::generic::OperatorInfo;
use timely_next::progress::{Antichain, frontier::AntichainRef};

#[derive(Default)]
struct Gate {
    permits: usize,
    waker: Option<Waker>,
    polls: usize,
    worked: usize,
}

struct WakeCount(std::sync::atomic::AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct Batch {
    rows: Vec<(u64, u64, i64)>,
    gate: Arc<Mutex<Gate>>,
}

impl SpineBatch for Batch {
    type Time = u64;
    type Merger = Merge;

    fn len(&self) -> usize {
        self.rows.len()
    }
}

struct Merge {
    rows: Vec<(u64, u64, i64)>,
    position: usize,
    since: u64,
    gate: Arc<Mutex<Gate>>,
}

impl Merger<Batch> for Merge {
    fn new(a: &Batch, b: &Batch, since: AntichainRef<u64>) -> Self {
        Self {
            rows: a.rows.iter().chain(&b.rows).cloned().collect(),
            position: 0,
            since: since.first().copied().unwrap_or(0),
            gate: Arc::clone(&a.gate),
        }
    }

    fn poll_work(
        &mut self,
        _: &Batch,
        _: &Batch,
        cx: &mut Context<'_>,
        fuel: &mut isize,
    ) -> Poll<MergeStatus> {
        while self.position < self.rows.len() && *fuel > 0 {
            let mut gate = self.gate.lock().unwrap();
            gate.polls += 1;
            if gate.permits == 0 {
                gate.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            gate.permits -= 1;
            gate.worked += 1;
            self.rows[self.position].1 = self.rows[self.position].1.max(self.since);
            self.position += 1;
            *fuel -= 1;
        }
        Poll::Ready(if self.position == self.rows.len() {
            MergeStatus::Complete
        } else {
            MergeStatus::InProgress
        })
    }

    fn done(mut self) -> Option<Batch> {
        assert_eq!(
            self.position,
            self.rows.len(),
            "extracted an incomplete merge"
        );
        differential_dataflow_next::consolidation::consolidate_updates(&mut self.rows);
        (!self.rows.is_empty()).then_some(Batch {
            rows: self.rows,
            gate: self.gate,
        })
    }
}

fn contents(trace: &Spine<Batch>) -> Vec<(u64, u64, i64)> {
    let mut rows = Vec::new();
    trace.map_spans(|span| {
        if let Some(batch) = &span.inner {
            rows.extend_from_slice(&batch.rows);
        }
    });
    differential_dataflow_next::consolidation::consolidate_updates(&mut rows);
    rows
}

fn drive(trace: &mut Spine<Batch>, gate: &Mutex<Gate>, expected: &[(u64, u64, i64)]) {
    for _ in 0..200_000 {
        assert_eq!(
            contents(trace),
            expected,
            "published contents changed while suspended"
        );
        if !trace.maintenance_pending() {
            return;
        }
        let wake = {
            let mut g = gate.lock().unwrap();
            g.permits += 3;
            g.waker.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        trace.exert(Exertion::Idle);
    }
    panic!("maintenance failed to resume");
}

#[mz_ore::test]
fn pending_reads_preserve_rollup_and_every_published_update() {
    let gate = Arc::new(Mutex::new(Gate::default()));
    let wake = Arc::new(WakeCount(std::sync::atomic::AtomicUsize::new(0)));
    let mut trace = Spine::new(OperatorInfo::new(0, 0, [].into()), None, None);
    trace.set_waker(Arc::clone(&wake).into());
    let mut expected = Vec::new();
    for time in 0..70u64 {
        let count = [1, 1, 2, 1, 32, 3, 1, 128, 2, 7][usize::try_from(time % 10).unwrap()];
        let rows = (0..count).map(|key| (key, time, 1)).collect::<Vec<_>>();
        expected.extend_from_slice(&rows);
        differential_dataflow_next::consolidation::consolidate_updates(&mut expected);
        trace.insert(Span::new(
            Description::new(
                Antichain::from_elem(time),
                Antichain::from_elem(time + 1),
                Antichain::from_elem(0),
            ),
            Some(Batch {
                rows,
                gate: Arc::clone(&gate),
            }),
        ));
        trace.set_physical_compaction(Antichain::from_elem(time + 1).borrow());
        assert_eq!(contents(&trace), expected);
        // Some arrivals deliberately queue behind an earlier blocked introduction.
        if time % 5 == 4 {
            drive(&mut trace, &gate, &expected);
        }
    }
    drive(&mut trace, &gate, &expected);
    assert!(wake.0.load(std::sync::atomic::Ordering::Relaxed) > 0);
}

struct MaintenanceSample {
    worked: usize,
    completions: usize,
    elapsed: std::time::Duration,
}

// Two equal batches start a merge without doing any merge work during setup.
// Each permit represents one row made available by a completed read. The same
// maintenance future used by the arranger decides when input can resume.
fn maintenance_sample(
    rows: u64,
    fuel: usize,
    read_rows: Option<usize>,
    exertion: Exertion,
) -> MaintenanceSample {
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::pin;
    use std::time::Instant;

    use crate::columnar::chunk::native::{NotifyWake, maintain};
    use tokio::sync::Notify;

    let gate = Arc::new(Mutex::new(Gate::default()));
    let notify = Arc::new(Notify::new());
    let mut trace = Spine::new(OperatorInfo::new(0, 0, [].into()), None, None);
    trace.set_waker(Arc::new(NotifyWake(Arc::clone(&notify))).into());
    for time in 0..2 {
        trace.insert(Span::new(
            Description::new(
                Antichain::from_elem(time),
                Antichain::from_elem(time + 1),
                Antichain::from_elem(0),
            ),
            Some(Batch {
                rows: (0..rows).map(|key| (key, time, 1)).collect(),
                gate: Arc::clone(&gate),
            }),
        ));
    }
    trace.set_physical_compaction(Antichain::from_elem(2).borrow());
    assert!(!trace.maintenance_pending());
    assert_eq!(gate.lock().unwrap().worked, 0);
    trace.set_exert_logic(Arc::new(move |levels| {
        levels
            .iter()
            .any(|(_, count, _)| *count > 1)
            .then_some(fuel)
    }));
    if read_rows.is_none() {
        gate.lock().unwrap().permits = usize::MAX / 2;
    }
    let expected = contents(&trace);
    let state = RefCell::new(trace);
    let mut completions = 0;
    let start = Instant::now();
    {
        let mut future = pin!(maintain(&state, &notify, exertion));
        let wake_count = Arc::new(WakeCount(std::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_count));
        let mut cx = Context::from_waker(&waker);
        while future.as_mut().poll(&mut cx).is_pending() {
            assert!(completions < 4 * rows);
            let wake = {
                let mut gate = gate.lock().unwrap();
                gate.permits += read_rows.expect("resident maintenance must not suspend");
                gate.waker
                    .take()
                    .expect("pending read must install a waker")
            };
            let before = wake_count.0.load(std::sync::atomic::Ordering::Relaxed);
            wake.wake();
            assert!(wake_count.0.load(std::sync::atomic::Ordering::Relaxed) > before);
            completions += 1;
        }
    }
    let elapsed = start.elapsed();
    let worked = gate.lock().unwrap().worked;
    assert_eq!(contents(&state.borrow()), expected);
    assert!(!state.borrow().maintenance_pending());

    // Returning to input must leave a wakeup for remaining background work.
    if worked < usize::try_from(2 * rows).unwrap() {
        let mut notified = std::pin::pin!(notify.notified());
        assert!(
            notified
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
    }
    gate.lock().unwrap().permits = usize::MAX / 2;
    for _ in 0..=2 * rows {
        state.borrow_mut().exert(Exertion::Idle);
        if gate.lock().unwrap().worked == usize::try_from(2 * rows).unwrap() {
            assert_eq!(contents(&state.borrow()), expected);
            return MaintenanceSample {
                worked,
                completions: usize::try_from(completions).unwrap(),
                elapsed,
            };
        }
    }
    panic!("remaining merge work did not finish");
}

#[mz_ore::test]
fn maintenance_readiness_preserves_work_allowance() {
    for exertion in [Exertion::Merges, Exertion::Funded, Exertion::Idle] {
        for fuel in [1, 1000] {
            let ready = maintenance_sample(4096, fuel, None, exertion);
            assert_eq!(ready.worked, fuel);
            for read_rows in [1, 64, 1024] {
                let pending = maintenance_sample(4096, fuel, Some(read_rows), exertion);
                assert_eq!(
                    pending.worked, ready.worked,
                    "fuel={fuel}, read_rows={read_rows}, exertion={exertion:?}"
                );
            }
        }
    }
}

#[mz_ore::test]
fn merge_only_maintenance_defers_consolidation_until_idle() {
    let gate = Arc::new(Mutex::new(Gate {
        permits: usize::MAX,
        ..Gate::default()
    }));
    let mut trace = Spine::new(OperatorInfo::new(0, 0, [].into()), None, None);
    // Descending sizes place the batches in separate layers without starting a merge.
    for (time, count) in [(0, 32), (1, 1)] {
        trace.insert(Span::new(
            Description::new(
                Antichain::from_elem(time),
                Antichain::from_elem(time + 1),
                Antichain::from_elem(0),
            ),
            Some(Batch {
                rows: (0..count).map(|key| (key, time, 1)).collect(),
                gate: Arc::clone(&gate),
            }),
        ));
    }
    trace.set_physical_compaction(Antichain::from_elem(2).borrow());
    let expected = contents(&trace);
    trace.set_exert_logic(Arc::new(|levels| {
        let batches: usize = levels.iter().map(|(_, count, _)| count).sum();
        (batches > 1).then_some(1000)
    }));
    for _ in 0..100 {
        assert!(trace.exert(Exertion::Merges), "policy work is deferred");
        assert_eq!(gate.lock().unwrap().worked, 0);
        assert_eq!(contents(&trace), expected);
    }
    for _ in 0..100 {
        trace.exert(Exertion::Idle);
    }
    assert_eq!(gate.lock().unwrap().worked, 33);
    let mut batches = 0;
    trace.map_spans(|span| batches += usize::from(span.inner.is_some()));
    assert_eq!(batches, 1, "idle maintenance must complete consolidation");
    assert_eq!(contents(&trace), expected);
}

// One large batch followed by small batches, each of which is followed by far more
// exertion turns than inserted updates. Returns merge rows worked and the number of
// non-empty batches left.
fn many_turns_per_insert(exertion: Exertion) -> (usize, usize) {
    let gate = Arc::new(Mutex::new(Gate {
        permits: usize::MAX,
        ..Gate::default()
    }));
    let mut trace = Spine::new(OperatorInfo::new(0, 0, [].into()), None, None);
    // Active merges and any second non-empty layer request effort, as the storage
    // policy does for layers near the largest one.
    trace.set_exert_logic(Arc::new(|levels| {
        let active = levels.iter().any(|(_, count, _)| *count > 1);
        let separate = levels.iter().filter(|(_, _, len)| *len > 0).count() > 1;
        (active || separate).then_some(1000)
    }));
    let mut expected = Vec::new();
    for time in 0..=16u64 {
        let count = if time == 0 { 4096 } else { 16 };
        let rows: Vec<_> = (0..count)
            .map(|key| (key + 100_000 * time, time, 1))
            .collect();
        expected.extend_from_slice(&rows);
        trace.insert(Span::new(
            Description::new(
                Antichain::from_elem(time),
                Antichain::from_elem(time + 1),
                Antichain::from_elem(0),
            ),
            Some(Batch {
                rows,
                gate: Arc::clone(&gate),
            }),
        ));
        trace.set_physical_compaction(Antichain::from_elem(time + 1).borrow());
        for _ in 0..10_000 {
            trace.exert(exertion);
        }
        assert!(!trace.maintenance_pending());
    }
    differential_dataflow_next::consolidation::consolidate_updates(&mut expected);
    assert_eq!(contents(&trace), expected);
    let mut batches = 0;
    trace.map_spans(|span| batches += usize::from(span.inner.is_some()));
    (gate.lock().unwrap().worked, batches)
}

#[mz_ore::test]
fn funded_consolidation_is_bounded_by_inserted_updates() {
    let inserted = 4096 + 16 * 16;
    let (idle_worked, idle_batches) = many_turns_per_insert(Exertion::Idle);
    assert_eq!(idle_batches, 1);
    assert!(
        idle_worked > 12 * 4096,
        "idle turns should lift most small batches into the large one: {idle_worked}"
    );
    let (funded_worked, _) = many_turns_per_insert(Exertion::Funded);
    // Insertion-funded merges of the small batches plus at most the credit's worth
    // of policy-requested effort.
    assert!(
        funded_worked <= 8 * inserted + 16 * 16 * 4,
        "funded exertion exceeded its credit: {funded_worked} of {idle_worked}"
    );
    let (merges_worked, _) = many_turns_per_insert(Exertion::Merges);
    assert!(
        merges_worked <= 16 * 16 * 4,
        "merge-only turns forced consolidation: {merges_worked}"
    );
}

// One large batch and one small batch in separate layers, then only frontier
// progress and funded exertion turns. The policy asks for more effort per grant
// than the insertions credit, so only progress can pay for consolidation. Returns
// merge rows worked and the number of non-empty batches left.
fn progress_without_updates(advances: usize) -> (usize, usize) {
    let gate = Arc::new(Mutex::new(Gate {
        permits: usize::MAX,
        ..Gate::default()
    }));
    let mut trace = Spine::new(OperatorInfo::new(0, 0, [].into()), None, None);
    trace.set_exert_logic(Arc::new(|levels| {
        let active = levels.iter().any(|(_, count, _)| *count > 1);
        let separate = levels.iter().filter(|(_, _, len)| *len > 0).count() > 1;
        (active || separate).then_some(1_000_000)
    }));
    for (time, count) in [(0u64, 4096u64), (1, 16)] {
        trace.insert(Span::new(
            Description::new(
                Antichain::from_elem(time),
                Antichain::from_elem(time + 1),
                Antichain::from_elem(0),
            ),
            Some(Batch {
                rows: (0..count)
                    .map(|key| (key + 100_000 * time, time, 1))
                    .collect(),
                gate: Arc::clone(&gate),
            }),
        ));
    }
    trace.set_physical_compaction(Antichain::from_elem(2).borrow());
    for _ in 0..advances {
        trace.fund_progress();
    }
    for _ in 0..10_000 {
        trace.exert(Exertion::Funded);
    }
    assert!(!trace.maintenance_pending());
    let mut batches = 0;
    trace.map_spans(|span| batches += usize::from(span.inner.is_some()));
    (gate.lock().unwrap().worked, batches)
}

#[mz_ore::test]
fn frontier_progress_funds_bounded_consolidation() {
    let (worked, batches) = progress_without_updates(0);
    assert_eq!(worked, 0, "turns alone must not fund consolidation");
    assert_eq!(batches, 2);
    let (worked, batches) = progress_without_updates(1);
    assert_eq!(batches, 1, "one advance funds the pending consolidation");
    assert_eq!(worked, 4096 + 16);
    let (worked, batches) = progress_without_updates(1_000);
    assert_eq!(batches, 1);
    assert_eq!(worked, 4096 + 16, "banked allowances must not redo work");
}

/// Run with `cargo test -p mz-timely-util maintenance_microbench -- --ignored --nocapture`.
/// Reports work and read completions before the arranger can next accept input.
/// Reads complete immediately when requested: timings measure scheduling and mock
/// merge work, excluding setup and verification, and do not model device latency.
#[mz_ore::test]
#[ignore = "local scheduling microbenchmark"]
fn maintenance_microbench() {
    println!("rows_per_batch,fuel,read_rows,sample,worked,read_completions,elapsed_us");
    for rows in [64, 4096, 65536] {
        for fuel in [1, 1000] {
            for read_rows in [None, Some(1), Some(64), Some(1024)] {
                for sample in 0..5 {
                    let result = maintenance_sample(rows, fuel, read_rows, Exertion::Idle);
                    println!(
                        "{rows},{fuel},{},{sample},{},{},{}",
                        read_rows.map_or_else(|| "ready".to_owned(), |n| n.to_string()),
                        result.worked,
                        result.completions,
                        result.elapsed.as_micros()
                    );
                }
            }
        }
    }
}
