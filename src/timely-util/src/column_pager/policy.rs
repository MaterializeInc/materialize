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

//! Concrete [`PagingPolicy`] implementations.
//!
//! [`TieredPolicy`] is a single process-wide byte budget for resident
//! columns. Resident columns can move between Timely workers, so the
//! accounting cannot be thread-local; budget is held in a single
//! [`AtomicUsize`] and credited back from whichever thread happens to drop
//! the column.
//!
//! [`PoolPolicy`] routes every column into an [`mz_ore::pool::Pool`], which
//! enforces its own resident-bytes budget by evicting cold chunks.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use mz_ore::pager::Backend;
use mz_ore::pool::Pool;

use crate::column_pager::{Codec, PageDecision, PageEvent, PageHint, PagingPolicy};

const BACKEND_SWAP: u8 = 0;
const BACKEND_FILE: u8 = 1;

const CODEC_NONE: u8 = 0;
const CODEC_LZ4: u8 = 1;

fn encode_backend(b: Backend) -> u8 {
    match b {
        Backend::Swap => BACKEND_SWAP,
        Backend::File => BACKEND_FILE,
    }
}

fn decode_backend(v: u8) -> Backend {
    match v {
        BACKEND_FILE => Backend::File,
        _ => Backend::Swap,
    }
}

fn encode_codec(c: Option<Codec>) -> u8 {
    match c {
        None => CODEC_NONE,
        Some(Codec::Lz4) => CODEC_LZ4,
    }
}

fn decode_codec(v: u8) -> Option<Codec> {
    match v {
        CODEC_LZ4 => Some(Codec::Lz4),
        _ => None,
    }
}

/// A single-pool byte budget for resident columns.
///
/// Each call to [`PagingPolicy::decide`] tries to reserve `len_bytes` from a
/// process-wide [`AtomicUsize`] pool. Success ⇒ [`PageDecision::Skip`]
/// (column kept resident); failure ⇒ [`PageDecision::Page`] (column spills
/// via the configured `backend` + `codec`). [`PagingPolicy::record`] credits
/// the bytes back on [`PageEvent::ResidentReleased`].
///
/// ## Why a single global pool
///
/// Earlier iterations used `thread_local!` per-worker accounting plus a
/// shared spill pool. That layout assumed each `PagedColumn::Resident`
/// would drop on the thread that received the grant — which is **not**
/// true in Materialize: columns move between Timely workers freely. A
/// cross-thread drop would credit the wrong worker's local pool and drift
/// both budgets silently.
///
/// The single-atomic design sidesteps the issue entirely: credit goes back
/// to the same pool regardless of where the drop runs. A future
/// thread-aware policy is welcome to revive the tiered layout — it would
/// need either a `SendColumn` wrapper that pins the column to its
/// originating thread, or an explicit cross-thread credit channel keyed by
/// origin [`std::thread::ThreadId`].
///
/// ## Contention
///
/// Hot paths (`decide`, `record`) touch a single atomic via a
/// compare-exchange loop. Per-byte CAS is fine at current column
/// granularity; if profiles show contention we can switch to chunk-sized
/// reservations.
pub struct TieredPolicy {
    /// Remaining budget, in bytes, available for resident columns. Drains
    /// on `decide` (Skip), refills on `record(ResidentReleased)`.
    budget: AtomicUsize,
    /// Last-configured total. `reconfigure` adjusts `budget` by the delta
    /// against this value so existing `ResidentTicket`s stay coherent with
    /// the running budget after an operator-driven tune.
    configured: AtomicUsize,
    backend: AtomicU8,
    codec: AtomicU8,
}

impl TieredPolicy {
    /// Constructs a policy with `total_budget` bytes available for resident
    /// columns. `backend` and `codec` are used for the
    /// [`PageDecision::Page`] outcome when the pool is exhausted.
    pub fn new(total_budget: usize, backend: Backend, codec: Option<Codec>) -> Self {
        Self {
            budget: AtomicUsize::new(total_budget),
            configured: AtomicUsize::new(total_budget),
            backend: AtomicU8::new(encode_backend(backend)),
            codec: AtomicU8::new(encode_codec(codec)),
        }
    }

    /// Adjust this policy in place. Budget moves by `new_total - prev_total`
    /// so in-flight `ResidentTicket`s — which still credit this same atomic
    /// when they drop — stay coherent with the resized pool. Backend and
    /// codec selection take effect on the next [`PagingPolicy::decide`]
    /// call.
    ///
    /// Shrinking the configured total below the in-flight resident set
    /// saturates the available budget at zero; subsequent `decide` calls
    /// page out until releases bring the pool back above zero.
    pub fn reconfigure(&self, new_total: usize, backend: Backend, codec: Option<Codec>) {
        let prev = self.configured.swap(new_total, Ordering::Relaxed);
        if new_total > prev {
            self.budget.fetch_add(new_total - prev, Ordering::Relaxed);
        } else if prev > new_total {
            let shrink = prev - new_total;
            let _ = self
                .budget
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_sub(shrink))
                });
        }
        self.backend
            .store(encode_backend(backend), Ordering::Relaxed);
        self.codec.store(encode_codec(codec), Ordering::Relaxed);
    }

    /// Returns the current remaining budget in bytes. Useful for metrics or
    /// tests.
    pub fn budget_remaining(&self) -> usize {
        self.budget.load(Ordering::Relaxed)
    }

    /// Returns the most-recently-configured total. Useful for tests.
    pub fn configured_total(&self) -> usize {
        self.configured.load(Ordering::Relaxed)
    }
}

impl PagingPolicy for TieredPolicy {
    fn decide(&self, hint: PageHint) -> PageDecision {
        if try_consume(&self.budget, hint.len_bytes) {
            PageDecision::Skip
        } else {
            PageDecision::Page {
                backend: decode_backend(self.backend.load(Ordering::Relaxed)),
                codec: decode_codec(self.codec.load(Ordering::Relaxed)),
            }
        }
    }

    fn record(&self, event: PageEvent) {
        let PageEvent::ResidentReleased { bytes } = event else {
            return;
        };
        self.budget.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Routes every non-empty column into a buffer [`Pool`].
///
/// The pool owns the budget: it evicts cold chunks into swap-backed extents
/// when its resident bytes exceed its configured bound, so the policy never
/// gates a decision on size. [`PagingPolicy::decide`] answers
/// [`PageDecision::Pool`] for every non-empty hint and keeps empty columns
/// resident; [`PagingPolicy::record`] is a no-op, since the column pager's
/// metrics observers already count page traffic and the pool tracks its own
/// residency in [`mz_ore::pool::PoolStats`].
#[derive(Debug, Clone)]
pub struct PoolPolicy {
    pool: Pool,
}

impl PoolPolicy {
    /// Constructs a policy backed by `pool`.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl PagingPolicy for PoolPolicy {
    fn decide(&self, hint: PageHint) -> PageDecision {
        if hint.len_bytes == 0 {
            PageDecision::Skip
        } else {
            PageDecision::Pool(self.pool.clone())
        }
    }

    fn record(&self, _event: PageEvent) {}
}

/// Atomically subtracts `want` from `atomic` if at least `want` is available.
/// Returns `true` on success.
fn try_consume(atomic: &AtomicUsize, want: usize) -> bool {
    let mut cur = atomic.load(Ordering::Relaxed);
    loop {
        if cur < want {
            return false;
        }
        // Relaxed: the budget is a pure counter; no memory is published or
        // acquired through it.
        match atomic.compare_exchange_weak(cur, cur - want, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use timely::container::PushInto;

    use crate::column_pager::{ColumnPager, PagedColumn};
    use crate::columnar::Column;

    use super::*;

    fn sample(n: i64) -> Column<i64> {
        let mut c: Column<i64> = Default::default();
        for v in 0..n {
            c.push_into(v);
        }
        c
    }

    /// Promotes a typed policy `Arc` to `Arc<dyn PagingPolicy>` without
    /// triggering `clippy::clone_on_ref_ptr` or `clippy::as_conversions`.
    fn as_dyn(p: &Arc<impl PagingPolicy + 'static>) -> Arc<dyn PagingPolicy> {
        #[allow(clippy::clone_on_ref_ptr)]
        p.clone()
    }

    /// Allocations within the budget stay resident; release returns budget.
    #[mz_ore::test]
    fn fits_in_budget() {
        let policy = Arc::new(TieredPolicy::new(64 * 1024, Backend::Swap, None));
        let cp = ColumnPager::new(as_dyn(&policy));
        let before = policy.budget_remaining();
        let mut col = sample(256);
        let p = cp.page(&mut col);
        assert!(matches!(p, PagedColumn::Resident(_, _)));
        assert!(policy.budget_remaining() < before);
        drop(p); // Drop fires ResidentReleased; budget returns.
        assert_eq!(policy.budget_remaining(), before);
    }

    /// An exhausted budget forces pageout.
    #[mz_ore::test]
    fn exhausted_pages_out() {
        let policy = Arc::new(TieredPolicy::new(0, Backend::Swap, None));
        let cp = ColumnPager::new(as_dyn(&policy));
        let mut col = sample(256);
        let p = cp.page(&mut col);
        assert!(matches!(p, PagedColumn::Paged { .. }));
    }

    /// Holding one Resident exhausts the budget; releasing it makes room.
    #[mz_ore::test]
    fn release_refills_budget() {
        let policy = Arc::new(TieredPolicy::new(4 * 1024, Backend::Swap, None));
        let cp = ColumnPager::new(as_dyn(&policy));

        // First allocation fits.
        let mut col = sample(256);
        let p1 = cp.page(&mut col);
        assert!(matches!(p1, PagedColumn::Resident(_, _)));

        // Second allocation overflows the budget → page out.
        let mut col2 = sample(256);
        let p2 = cp.page(&mut col2);
        assert!(matches!(p2, PagedColumn::Paged { .. }));

        // Releasing the first refills the budget; a third allocation now
        // fits resident again.
        drop(p1);
        drop(p2);
        let mut col3 = sample(256);
        let p3 = cp.page(&mut col3);
        assert!(matches!(p3, PagedColumn::Resident(_, _)));
    }

    /// Releasing a Resident on a different thread must still credit the same
    /// global budget pool.
    #[mz_ore::test]
    fn release_crosses_threads() {
        let policy = Arc::new(TieredPolicy::new(64 * 1024, Backend::Swap, None));
        let cp = ColumnPager::new(as_dyn(&policy));
        let before = policy.budget_remaining();
        let mut col = sample(256);
        let p = cp.page(&mut col);
        assert!(matches!(p, PagedColumn::Resident(_, _)));
        assert!(policy.budget_remaining() < before);

        // Move ownership to another thread; drop happens there.
        std::thread::spawn(move || drop(p)).join().unwrap();
        assert_eq!(
            policy.budget_remaining(),
            before,
            "cross-thread drop must credit the same global pool",
        );
    }

    /// Reconfigure preserves the in-flight resident set: tickets minted
    /// against the old configured total still credit the same atomic when
    /// they drop. Growing the configured total adds the delta; shrinking
    /// subtracts saturating at zero.
    #[mz_ore::test]
    fn reconfigure_preserves_in_flight() {
        let policy = Arc::new(TieredPolicy::new(4 * 1024, Backend::Swap, None));
        let cp = ColumnPager::new(as_dyn(&policy));

        // Hold one resident, consuming some budget.
        let mut col = sample(256);
        let p = cp.page(&mut col);
        assert!(matches!(p, PagedColumn::Resident(_, _)));
        let consumed = 4 * 1024 - policy.budget_remaining();
        assert!(consumed > 0);

        // Grow the pool by 8 KiB. Available budget should rise by the delta;
        // configured total reflects the new size.
        let before = policy.budget_remaining();
        policy.reconfigure(4 * 1024 + 8 * 1024, Backend::Swap, None);
        assert_eq!(policy.budget_remaining(), before + 8 * 1024);
        assert_eq!(policy.configured_total(), 4 * 1024 + 8 * 1024);

        // Drop the resident; the ticket credits the same atomic, even
        // though the pool was resized in between.
        drop(p);
        assert_eq!(policy.budget_remaining(), 4 * 1024 + 8 * 1024);
    }

    /// Shrinking the configured total below available budget saturates at
    /// zero rather than wrapping.
    #[mz_ore::test]
    fn reconfigure_shrink_saturates() {
        let policy = Arc::new(TieredPolicy::new(1024, Backend::Swap, None));
        // Shrink by more than the current available budget.
        policy.reconfigure(0, Backend::Swap, None);
        assert_eq!(policy.budget_remaining(), 0);
        assert_eq!(policy.configured_total(), 0);
    }

    /// Backend / codec selection takes effect on the next decide.
    #[mz_ore::test]
    fn reconfigure_swaps_backend_and_codec() {
        let policy = Arc::new(TieredPolicy::new(0, Backend::Swap, None));
        let initial = policy.decide(PageHint { len_bytes: 1 });
        assert!(matches!(
            initial,
            PageDecision::Page {
                backend: Backend::Swap,
                codec: None
            }
        ));
        policy.reconfigure(0, Backend::File, Some(Codec::Lz4));
        let updated = policy.decide(PageHint { len_bytes: 1 });
        assert!(matches!(
            updated,
            PageDecision::Page {
                backend: Backend::File,
                codec: Some(Codec::Lz4),
            }
        ));
    }

    #[mz_ore::test]
    fn try_consume_atomicity() {
        let a = AtomicUsize::new(10);
        assert!(try_consume(&a, 4));
        assert_eq!(a.load(Ordering::Relaxed), 6);
        assert!(!try_consume(&a, 7));
        assert_eq!(a.load(Ordering::Relaxed), 6);
        assert!(try_consume(&a, 6));
        assert_eq!(a.load(Ordering::Relaxed), 0);
        assert!(!try_consume(&a, 1));
    }
}
