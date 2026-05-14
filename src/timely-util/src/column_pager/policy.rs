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
//! Today: [`TieredPolicy`], a single process-wide byte budget for resident
//! columns. Resident columns can move between Timely workers, so the
//! accounting cannot be thread-local; budget is held in a single
//! [`AtomicUsize`] and credited back from whichever thread happens to drop
//! the column.

use std::sync::atomic::{AtomicUsize, Ordering};

use mz_ore::pager::Backend;

use crate::column_pager::{Codec, PageDecision, PageEvent, PageHint, PagingPolicy};

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
    /// Remaining budget, in bytes, available for resident columns.
    budget: AtomicUsize,
    backend: Backend,
    codec: Option<Codec>,
}

impl TieredPolicy {
    /// Constructs a policy with `total_budget` bytes available for resident
    /// columns. `backend` and `codec` are used for the
    /// [`PageDecision::Page`] outcome when the pool is exhausted.
    pub fn new(total_budget: usize, backend: Backend, codec: Option<Codec>) -> Self {
        Self {
            budget: AtomicUsize::new(total_budget),
            backend,
            codec,
        }
    }

    /// Returns the current remaining budget in bytes. Useful for metrics or
    /// tests.
    pub fn budget_remaining(&self) -> usize {
        self.budget.load(Ordering::Relaxed)
    }
}

impl PagingPolicy for TieredPolicy {
    fn decide(&self, hint: PageHint) -> PageDecision {
        if try_consume(&self.budget, hint.len_bytes) {
            PageDecision::Skip
        } else {
            PageDecision::Page {
                backend: self.backend,
                codec: self.codec,
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

/// Atomically subtracts `want` from `atomic` if at least `want` is available.
/// Returns `true` on success.
fn try_consume(atomic: &AtomicUsize, want: usize) -> bool {
    let mut cur = atomic.load(Ordering::Relaxed);
    loop {
        if cur < want {
            return false;
        }
        match atomic.compare_exchange_weak(cur, cur - want, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
