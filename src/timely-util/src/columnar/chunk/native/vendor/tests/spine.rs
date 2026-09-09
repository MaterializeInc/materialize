// Copyright (c) 2015 Frank McSherry
// SPDX-License-Identifier: MIT
// See ../LICENSE.

use super::super::spine::Span;
use super::super::spine::{MergeStatus, Merger, Spine, SpineBatch};
use differential_dataflow::trace::Description;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{Antichain, frontier::AntichainRef};

#[derive(Default)]
struct Gate {
    permits: usize,
    waker: Option<Waker>,
    polls: usize,
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
        differential_dataflow::consolidation::consolidate_updates(&mut self.rows);
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
    differential_dataflow::consolidation::consolidate_updates(&mut rows);
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
        differential_dataflow::consolidation::consolidate_updates(&mut expected);
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
