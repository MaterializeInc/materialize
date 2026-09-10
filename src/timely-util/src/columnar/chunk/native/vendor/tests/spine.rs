// Copyright (c) 2015 Frank McSherry
// SPDX-License-Identifier: MIT
// See ../LICENSE.

use super::super::spine::Spine;
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
        trace.exert();
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
fn maintenance_sample(rows: u64, fuel: usize, read_rows: Option<usize>) -> MaintenanceSample {
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
        let mut future = pin!(maintain(&state, &notify));
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
        state.borrow_mut().exert();
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
    for fuel in [1, 1000] {
        let ready = maintenance_sample(4096, fuel, None);
        assert_eq!(ready.worked, fuel);
        for read_rows in [1, 64, 1024] {
            let pending = maintenance_sample(4096, fuel, Some(read_rows));
            assert_eq!(
                pending.worked, ready.worked,
                "fuel={fuel}, read_rows={read_rows}"
            );
        }
    }
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
                    let result = maintenance_sample(rows, fuel, read_rows);
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
