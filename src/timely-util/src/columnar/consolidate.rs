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

//! A `ContainerBuilder` that consolidates `(D, T, R)` updates and emits columnar containers.
//!
//! Two-level buffering:
//!
//! 1. AoS staging `Vec<(D, T, R)>` with a small cap so `consolidate_updates`' `n log n` cost
//!    stays bounded. Cancellations and same-key updates collapse here before reaching the
//!    column-shaped storage. A drain-multiple-of-half-cap trick keeps the leftover in staging,
//!    so cross-batch keys with the same `(D, T)` continue consolidating on the next sort.
//! 2. SoA accumulator: one sub-container per column (`D::Container`, `T::Container`,
//!    `R::Container`). Drains from staging do three sequential passes (one per column), so
//!    each pass writes a single cache-line stream. When the accumulator reaches the output
//!    target byte size, it serializes into an aligned `Vec<u64>` (no zero-fill — written
//!    exactly once by `indexed::write`) and ships as `Column::Align`. The trailing partial on
//!    `finish` ships as `Column::Typed` to avoid one final serialize copy.
//!
//! Generic over `(D, T, R): Columnar` via the columnar tuple decomposition
//! `<(D, T, R) as Columnar>::Container = (D::Container, T::Container, R::Container)`.

use std::collections::VecDeque;

use columnar::bytes::indexed;
use columnar::{Borrow, Columnar, Push};
use differential_dataflow::Data;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use timely::container::{ContainerBuilder, PushInto};

use crate::columnar::Column;

/// Items per staging buffer. Small enough that O(n log n) sort stays cheap, large enough to
/// amortize the sort + drain overhead across many pushes.
const STAGING_CAP_ITEMS: usize = 16 * 1024;
/// Drain a multiple of this on consolidate. Remainder stays in staging so cross-batch keys
/// keep consolidating on the next sort.
const DRAIN_GRAIN: usize = STAGING_CAP_ITEMS / 2;
/// Target serialized size for output `Column::Align` chunks, in u64 words.
/// 2 MiB matches the existing `ColumnBuilder` heuristic.
const OUTPUT_TARGET_WORDS: usize = (2 << 20) / 8;

/// A container builder that consolidates `(D, T, R)` updates and emits `Column<(D, T, R)>`.
///
/// Stages updates in an AoS `Vec` for in-place consolidation, then drains consolidated rows
/// in three sequential passes into SoA sub-containers. When the SoA accumulator reaches
/// `OUTPUT_TARGET_WORDS` worth of serialized bytes, it is written into an aligned `Vec<u64>`
/// (using `indexed::write` over uninitialized memory — no zero-fill) and queued as
/// `Column::Align`. The trailing partial on `finish` ships as `Column::Typed`.
///
/// Does **not** maintain FIFO ordering (consolidation reorders updates).
pub struct ConsolidatingColumnBuilder<D, T, R>
where
    D: Columnar,
    T: Columnar,
    R: Columnar,
{
    /// AoS staging buffer for in-place consolidation. Cap = [`STAGING_CAP_ITEMS`].
    staging: Vec<(D, T, R)>,
    /// SoA accumulator, one sub-container per column.
    cur_d: D::Container,
    cur_t: T::Container,
    cur_r: R::Container,
    /// Number of `(D, T, R)` tuples currently in `cur_*`.
    cur_len: usize,
    /// Finished columns ready to ship.
    pending: VecDeque<Column<(D, T, R)>>,
    /// The currently extracted/finished column.
    finished: Option<Column<(D, T, R)>>,
}

impl<D, T, R> Default for ConsolidatingColumnBuilder<D, T, R>
where
    D: Columnar,
    T: Columnar,
    R: Columnar,
{
    fn default() -> Self {
        Self {
            // Pre-allocate so `push` is unconditional (no per-push capacity check or lazy
            // reserve branch).
            staging: Vec::with_capacity(STAGING_CAP_ITEMS),
            cur_d: D::Container::default(),
            cur_t: T::Container::default(),
            cur_r: R::Container::default(),
            cur_len: 0,
            pending: VecDeque::new(),
            finished: None,
        }
    }
}

impl<D, T, R> ConsolidatingColumnBuilder<D, T, R>
where
    D: Data + Columnar,
    T: Data + Columnar,
    R: Semigroup + Columnar + 'static,
    (D, T, R): Columnar<Container = (D::Container, T::Container, R::Container)>,
{
    /// Sort and consolidate `staging`, then drain a multiple-of-`grain` prefix into the SoA
    /// accumulator. Pass `1` to drain everything (used by `finish`).
    #[cold]
    fn consolidate_and_drain(&mut self, grain: usize) {
        consolidate_updates(&mut self.staging);
        let drain_n = (self.staging.len() / grain) * grain;
        if drain_n == 0 {
            return;
        }

        // Three sequential passes — one per column. Each pass writes a single cache-line stream
        // and is branch-free (the sub-containers' `push` only checks per-call capacity).
        let head = &self.staging[..drain_n];
        for (d, _, _) in head {
            self.cur_d.push(d);
        }
        for (_, t, _) in head {
            self.cur_t.push(t);
        }
        for (_, _, r) in head {
            self.cur_r.push(r);
        }
        self.cur_len += drain_n;
        self.staging.drain(..drain_n);

        // Check serialized size; if past the target, flush as aligned bytes. Using
        // `length_in_words` rather than item count gives bounded output Columns regardless of
        // variable-width `D` (e.g. `Row` rows of varying length).
        let view = (
            self.cur_d.borrow(),
            self.cur_t.borrow(),
            self.cur_r.borrow(),
        );
        if indexed::length_in_words(&view) >= OUTPUT_TARGET_WORDS {
            self.flush_aligned();
        }
    }

    /// Serialize the SoA accumulator into a `Column::Align` via `indexed::encode`, which
    /// builds the buffer with `Vec::push`/`extend_from_slice` so no memory is initialized
    /// twice.
    #[cold]
    fn flush_aligned(&mut self) {
        if self.cur_len == 0 {
            return;
        }
        let cur: <(D, T, R) as Columnar>::Container = (
            std::mem::take(&mut self.cur_d),
            std::mem::take(&mut self.cur_t),
            std::mem::take(&mut self.cur_r),
        );
        self.cur_len = 0;

        let mut buf: Vec<u64> = Vec::with_capacity(indexed::length_in_words(&cur.borrow()));
        indexed::encode(&mut buf, &cur.borrow());
        self.pending.push_back(Column::Align(buf));
    }
}

impl<D, T, R> PushInto<(D, T, R)> for ConsolidatingColumnBuilder<D, T, R>
where
    D: Data + Columnar,
    T: Data + Columnar,
    R: Semigroup + Columnar + 'static,
    (D, T, R): Columnar<Container = (D::Container, T::Container, R::Container)>,
{
    /// Push an element into the staging buffer; consolidate + drain when full.
    #[inline]
    fn push_into(&mut self, item: (D, T, R)) {
        self.staging.push(item);
        if self.staging.len() == STAGING_CAP_ITEMS {
            self.consolidate_and_drain(DRAIN_GRAIN);
        }
    }
}

impl<D, T, R> ContainerBuilder for ConsolidatingColumnBuilder<D, T, R>
where
    D: Data + Columnar,
    T: Data + Columnar,
    R: Semigroup + Columnar + 'static,
    (D, T, R): Columnar<Container = (D::Container, T::Container, R::Container)>,
    <(D, T, R) as Columnar>::Container: Clone,
{
    type Container = Column<(D, T, R)>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(c) = self.pending.pop_front() {
            self.finished = Some(c);
            self.finished.as_mut()
        } else {
            None
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.staging.is_empty() {
            // `multiple = 1` so any remainder also leaves staging.
            self.consolidate_and_drain(1);
        }
        // Trailing partial: ship as `Column::Typed` (no extra serialize copy).
        if self.cur_len > 0 {
            let cur: <(D, T, R) as Columnar>::Container = (
                std::mem::take(&mut self.cur_d),
                std::mem::take(&mut self.cur_t),
                std::mem::take(&mut self.cur_r),
            );
            self.cur_len = 0;
            self.pending.push_back(Column::Typed(cur));
        }
        self.extract()
    }
}

#[cfg(test)]
mod tests {
    use columnar::Index;
    use columnar::Len;
    use timely::container::{ContainerBuilder, PushInto};

    use super::*;

    /// Collect every `(D, T, R)` row from a `Column<(u64, u64, i64)>`.
    fn rows(column: &Column<(u64, u64, i64)>) -> Vec<(u64, u64, i64)> {
        let borrow = column.borrow();
        (0..borrow.len())
            .map(|i| {
                let r = borrow.get(i);
                (*r.0, *r.1, *r.2)
            })
            .collect()
    }

    /// Drain a builder by repeatedly calling `extract` then `finish`.
    fn drain(mut builder: ConsolidatingColumnBuilder<u64, u64, i64>) -> Vec<(u64, u64, i64)> {
        let mut out: Vec<(u64, u64, i64)> = Vec::new();
        while let Some(c) = builder.extract() {
            for r in rows(c) {
                out.push(r);
            }
        }
        if let Some(c) = builder.finish() {
            for r in rows(c) {
                out.push(r);
            }
        }
        out
    }

    #[mz_ore::test]
    fn empty_finish_yields_none() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        assert!(builder.extract().is_none());
        assert!(builder.finish().is_none());
    }

    #[mz_ore::test]
    fn single_push_finish_yields_one() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        builder.push_into((1u64, 0u64, 1i64));
        let column = builder.finish().expect("one container");
        assert_eq!(rows(column), vec![(1, 0, 1)]);
        assert!(builder.finish().is_none());
    }

    #[mz_ore::test]
    fn consolidates_on_threshold() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        // Push enough +1/-1 pairs to exceed `STAGING_CAP_ITEMS * 2` and trigger several
        // consolidation cycles. Everything cancels in the staging buffer before ever reaching
        // the SoA accumulator.
        for _ in 0..(STAGING_CAP_ITEMS * 4) {
            builder.push_into((7u64, 0u64, 1i64));
            builder.push_into((7u64, 0u64, -1i64));
        }
        assert!(drain(builder).is_empty());
    }

    #[mz_ore::test]
    fn cross_batch_consolidation() {
        // A single key pushed many times must collapse to one row even across many staging
        // refills. The drain-multiple-of-grain trick keeps the in-progress consolidated row
        // in staging so subsequent pushes merge into it.
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        let n: i64 = 100_000;
        for _ in 0..n {
            builder.push_into((42u64, 0u64, 1i64));
        }
        let out = drain(builder);
        assert_eq!(out, vec![(42, 0, n)]);
    }

    #[mz_ore::test]
    fn multiple_distinct_keys() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        builder.push_into((1u64, 0u64, 1i64));
        builder.push_into((2u64, 0u64, 1i64));
        builder.push_into((1u64, 0u64, 1i64));
        let mut out = drain(builder);
        out.sort();
        assert_eq!(out, vec![(1, 0, 2), (2, 0, 1)]);
    }

    #[mz_ore::test]
    fn emits_multiple_containers() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        // Enough distinct rows to fill the SoA accumulator past the output target multiple
        // times. Each row is 24 bytes; ~87k rows ≈ 2 MiB, so 300k pushes should mint at least
        // 2-3 aligned containers plus a typed partial on `finish`.
        let n: u64 = 300_000;
        for d in 0..n {
            builder.push_into((d, 0u64, 1i64));
        }

        let mut containers = 0;
        let mut out: Vec<(u64, u64, i64)> = Vec::new();
        while let Some(c) = builder.extract() {
            containers += 1;
            for r in rows(c) {
                out.push(r);
            }
        }
        if let Some(c) = builder.finish() {
            containers += 1;
            for r in rows(c) {
                out.push(r);
            }
        }
        assert!(
            containers > 1,
            "expected multiple containers, got {containers}"
        );
        out.sort();
        let expected: Vec<(u64, u64, i64)> = (0..n).map(|d| (d, 0u64, 1i64)).collect();
        assert_eq!(out, expected);
    }
}
