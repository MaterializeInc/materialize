// Copyright (c) 2015 Frank McSherry
// SPDX-License-Identifier: MIT
// See ../LICENSE.

use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

use super::super::batcher::Merger as ChainMerger;
use super::super::chunk::{Chunk, ChunkBatch, ChunkBatchMerger, ChunkMerger};
use super::super::spine::{MergeStatus, Merger};
use differential_dataflow::trace::chunk::{Chunk as MzChunk, vec::VecChunk};
use differential_dataflow::trace::{Cursor, Navigable};
use timely::container::PushInto;
use timely::progress::{Antichain, frontier::AntichainRef};

type Inner = VecChunk<u64, (), u64, i64>;
#[derive(Clone, Default)]
struct Delayed(Inner);
#[derive(Default)]
struct Pending {
    operation: Option<&'static str>,
}
impl Pending {
    // Complete the primitive's mutation before suspending. Its input may now be
    // empty, but the driver must still poll the same primitive to completion.
    fn poll(&mut self, operation: &'static str, cx: &Context<'_>, work: impl FnOnce()) -> Poll<()> {
        if let Some(saved) = self.operation.take() {
            assert_eq!(saved, operation, "resumed a different primitive");
            Poll::Ready(())
        } else {
            work();
            self.operation = Some(operation);
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
fn raw(queue: &mut VecDeque<Delayed>) -> VecDeque<Inner> {
    queue.drain(..).map(|c| c.0).collect()
}
fn restore(queue: &mut VecDeque<Delayed>, chunks: VecDeque<Inner>) {
    queue.extend(chunks.into_iter().map(Delayed));
}
impl Chunk for Delayed {
    type Time = u64;
    type Pending = Pending;
    const TARGET: usize = Inner::TARGET;
    fn len(&self) -> usize {
        self.0.len()
    }
    fn merge(a: &mut VecDeque<Self>, b: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let (mut aa, mut bb, mut result) = (raw(a), raw(b), VecDeque::new());
        Inner::merge(&mut aa, &mut bb, &mut result);
        restore(a, aa);
        restore(b, bb);
        restore(out, result);
    }
    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<u64>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) {
        let (mut aa, mut result) = (raw(input), VecDeque::new());
        Inner::advance(&mut aa, frontier, done, &mut result);
        restore(input, aa);
        restore(out, result);
    }
    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<u64>,
        residual: &mut Antichain<u64>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        let (mut aa, mut kept, mut shipped) = (raw(input), VecDeque::new(), VecDeque::new());
        Inner::extract(&mut aa, frontier, residual, &mut kept, &mut shipped);
        restore(input, aa);
        restore(keep, kept);
        restore(ship, shipped);
    }
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        let (mut aa, mut result) = (raw(input), VecDeque::new());
        Inner::settle(&mut aa, done, &mut result);
        restore(input, aa);
        restore(out, result);
    }
    fn poll_merge(
        state: &mut Pending,
        cx: &mut Context<'_>,
        a: &mut VecDeque<Self>,
        b: &mut VecDeque<Self>,
        out: &mut VecDeque<Self>,
    ) -> Poll<()> {
        state.poll("merge", cx, || Self::merge(a, b, out))
    }
    fn poll_advance(
        state: &mut Pending,
        cx: &mut Context<'_>,
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<u64>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) -> Poll<()> {
        state.poll("advance", cx, || Self::advance(input, frontier, done, out))
    }
    fn poll_extract(
        state: &mut Pending,
        cx: &mut Context<'_>,
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<u64>,
        residual: &mut Antichain<u64>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) -> Poll<()> {
        state.poll("extract", cx, || {
            Self::extract(input, frontier, residual, keep, ship)
        })
    }
    fn poll_settle(
        state: &mut Pending,
        cx: &mut Context<'_>,
        input: &mut VecDeque<Self>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) -> Poll<()> {
        state.poll("settle", cx, || Self::settle(input, done, out))
    }
}
fn chunk(rows: &[(u64, u64, i64)]) -> Delayed {
    let mut chunk = Inner::default();
    for &(key, time, diff) in rows {
        chunk.push_into(((key, ()), time, diff));
    }
    Delayed(chunk)
}
fn rows(chunks: &[Delayed]) -> Vec<((u64, ()), u64, i64)> {
    let mut rows = Vec::new();
    for Delayed(chunk) in chunks {
        let mut cursor = chunk.cursor();
        while cursor.key_valid(chunk) {
            let key = *cursor.key(chunk);
            cursor.map_times(chunk, |time, diff| rows.push(((key, ()), *time, *diff)));
            cursor.step_key(chunk);
        }
    }
    rows
}

fn finish(mut poll: impl FnMut(&mut Context<'_>) -> Poll<()>) {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1000 {
        if poll(&mut cx).is_ready() {
            return;
        }
    }
    panic!("chunk operation did not complete");
}
#[mz_ore::test]
fn batch_merge_resumes_each_phase_after_consuming_input() {
    let a = ChunkBatch {
        chunks: vec![chunk(&[(0, 0, 1), (1, 0, 2)]), chunk(&[(2, 0, 1)])],
    };
    let b = ChunkBatch {
        chunks: vec![chunk(&[(0, 1, -1), (1, 1, 3)]), chunk(&[(3, 1, 1)])],
    };
    let mut merge = ChunkBatchMerger::new(&a, &b, Antichain::from_elem(2).borrow());
    let mut fuel = 1;
    let mut yields = 0;
    finish(|cx| match merge.poll_work(&a, &b, cx, &mut fuel) {
        Poll::Pending => {
            yields += 1;
            Poll::Pending
        }
        Poll::Ready(MergeStatus::InProgress) => {
            assert!(fuel <= 0);
            fuel = 1;
            Poll::Pending
        }
        Poll::Ready(MergeStatus::Complete) => Poll::Ready(()),
    });
    assert!(yields >= 3);
    assert_eq!(
        rows(&merge.done().unwrap().chunks),
        vec![((1, ()), 2, 5), ((2, ()), 2, 1), ((3, ()), 2, 1)]
    );
}
#[mz_ore::test]
fn chain_merge_and_extraction_resume_before_reusing_state() {
    let mut a = vec![chunk(&[(0, 0, 1), (1, 3, 1)])];
    let mut b = vec![chunk(&[(0, 0, -1), (2, 1, 1)])];
    let mut merger = ChunkMerger::<Delayed>::default();
    let (mut merged, mut stash) = (Vec::new(), Vec::new());
    finish(|cx| merger.poll_merge(&mut a, &mut b, &mut merged, &mut stash, cx));
    assert_eq!(rows(&merged), vec![((1, ()), 3, 1), ((2, ()), 1, 1)]);
    let (mut ship, mut kept, mut frontier) = (Vec::new(), Vec::new(), Antichain::new());
    finish(|cx| {
        merger.poll_extract(
            &mut merged,
            Antichain::from_elem(2).borrow(),
            &mut frontier,
            &mut ship,
            &mut kept,
            &mut stash,
            cx,
        )
    });
    assert_eq!(rows(&ship), vec![((2, ()), 1, 1)]);
    assert_eq!(rows(&kept), vec![((1, ()), 3, 1)]);
    assert_eq!(frontier, Antichain::from_elem(3));
}
