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
//! Today: [`TieredPolicy`], a two-tier byte budget where each Timely worker
//! gets a fixed local budget and falls back to a process-wide shared pool when
//! the local pool is exhausted.

use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use mz_ore::pager::Backend;

use crate::column_pager::{Codec, PageDecision, PageEvent, PageHint, PagingPolicy};

/// A two-tier byte budget for resident columns.
///
/// Each Timely worker thread draws first from a fixed per-worker pool of
/// `per_worker_budget` bytes. When a worker's pool is exhausted, it consults
/// the shared process-wide pool of `shared_budget` bytes (set at construction
/// via [`TieredPolicy::new`]). If both are full, [`PagingPolicy::decide`]
/// returns [`PageDecision::Page`] and the column is paged out via the
/// configured `backend` + `codec`.
///
/// ## Per-worker state via thread-local storage
///
/// Worker state lives in a `thread_local!` static, so each OS thread (= each
/// Timely worker, in current Materialize deployments) sees its own
/// `WorkerState`. This means **at most one `TieredPolicy` instance per
/// process** — a second instance would share the same `LOCAL` static and
/// corrupt the first instance's accounting.
///
/// ## Release order
///
/// On [`PageEvent::ResidentReleased`], the policy returns budget to the shared
/// pool first (so other workers unblock sooner), then to the local pool.
///
/// ## Contention
///
/// The shared pool is a single [`AtomicUsize`]. Only the cold path (local
/// exhausted) touches it. Per-byte CAS is fine at current page granularity;
/// if profiles show contention we can switch to chunk reservations.
pub struct TieredPolicy {
    per_worker_budget: usize,
    shared: AtomicUsize,
    backend: Backend,
    codec: Option<Codec>,
}

thread_local! {
    static LOCAL: RefCell<Option<WorkerState>> = const { RefCell::new(None) };
}

/// Per-worker state. Initialized lazily on the first `with_local` call so the
/// `thread_local!` static doesn't need to know `per_worker_budget` up front.
#[derive(Debug)]
struct WorkerState {
    /// Remaining bytes in the local pool.
    remaining: usize,
    /// Bytes the worker currently owes back to its local pool.
    locally_owed: usize,
    /// Bytes the worker currently owes back to the shared pool.
    shared_owed: usize,
}

impl TieredPolicy {
    /// Constructs a tiered policy. Total budget is
    /// `per_worker_budget * workers + shared_budget`. The first `decide` call
    /// from each worker initializes that worker's local pool.
    ///
    /// `backend` and `codec` are used for the [`PageDecision::Page`] outcome
    /// when both pools are exhausted.
    pub fn new(
        per_worker_budget: usize,
        shared_budget: usize,
        backend: Backend,
        codec: Option<Codec>,
    ) -> Self {
        Self {
            per_worker_budget,
            shared: AtomicUsize::new(shared_budget),
            backend,
            codec,
        }
    }

    /// Returns the current shared-pool remaining size in bytes. Useful for
    /// metrics or tests.
    pub fn shared_remaining(&self) -> usize {
        self.shared.load(Ordering::Relaxed)
    }

    fn with_local<R>(&self, f: impl FnOnce(&mut WorkerState) -> R) -> R {
        LOCAL.with(|cell| {
            let mut borrow = cell.borrow_mut();
            let state = borrow.get_or_insert_with(|| WorkerState {
                remaining: self.per_worker_budget,
                locally_owed: 0,
                shared_owed: 0,
            });
            f(state)
        })
    }
}

impl PagingPolicy for TieredPolicy {
    fn decide(&self, hint: PageHint) -> PageDecision {
        self.with_local(|s| {
            // Local pool first.
            if s.remaining >= hint.len_bytes {
                s.remaining -= hint.len_bytes;
                s.locally_owed += hint.len_bytes;
                return PageDecision::Skip;
            }
            // Shared pool fallback.
            if try_consume(&self.shared, hint.len_bytes) {
                s.shared_owed += hint.len_bytes;
                return PageDecision::Skip;
            }
            // Both exhausted — page out.
            PageDecision::Page {
                backend: self.backend,
                codec: self.codec,
            }
        })
    }

    fn record(&self, event: PageEvent) {
        let PageEvent::ResidentReleased { bytes } = event else {
            return;
        };
        self.with_local(|s| {
            // Return to shared first so other workers unblock sooner.
            let from_shared = bytes.min(s.shared_owed);
            if from_shared > 0 {
                s.shared_owed -= from_shared;
                self.shared.fetch_add(from_shared, Ordering::Relaxed);
            }
            let to_local = bytes - from_shared;
            if to_local > 0 {
                debug_assert!(
                    s.locally_owed >= to_local,
                    "release exceeds locally_owed (releasing {to_local}, owed {})",
                    s.locally_owed,
                );
                s.locally_owed -= to_local;
                s.remaining += to_local;
            }
        });
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

    /// All allocations fit in the per-worker pool.
    #[mz_ore::test]
    fn fits_in_local() {
        let policy = Arc::new(TieredPolicy::new(64 * 1024, 0, Backend::Swap, None));
        let cp = ColumnPager::new(as_dyn(&policy));
        let mut col = sample(256);
        let p = cp.page(&mut col);
        assert!(matches!(p, PagedColumn::Resident(_, _)));
        drop(p); // Drop fires ResidentReleased; budget returns.
    }

    /// Local pool exhausted, shared pool covers the rest.
    #[mz_ore::test]
    fn spills_to_shared() {
        let policy = Arc::new(TieredPolicy::new(0, 64 * 1024, Backend::Swap, None));
        let cp = ColumnPager::new(as_dyn(&policy));
        let mut col = sample(256);
        let before = policy.shared_remaining();
        let p = cp.page(&mut col);
        assert!(matches!(p, PagedColumn::Resident(_, _)));
        let after = policy.shared_remaining();
        assert!(after < before, "shared pool should be consumed");
        drop(p);
        assert_eq!(
            policy.shared_remaining(),
            before,
            "release should refund the shared pool",
        );
    }

    /// Both pools exhausted: pageout is forced.
    #[mz_ore::test]
    fn exhausted_pages_out() {
        let policy = Arc::new(TieredPolicy::new(0, 0, Backend::Swap, None));
        let cp = ColumnPager::new(as_dyn(&policy));
        let mut col = sample(256);
        let p = cp.page(&mut col);
        assert!(matches!(p, PagedColumn::Paged { .. }));
    }

    /// Local refill: a held Resident locks budget; dropping it frees space
    /// for the next allocation.
    #[mz_ore::test]
    fn release_refills_local() {
        let policy = Arc::new(TieredPolicy::new(4 * 1024, 0, Backend::Swap, None));
        let cp = ColumnPager::new(as_dyn(&policy));

        // First allocation fits.
        let mut col = sample(256);
        let p1 = cp.page(&mut col);
        assert!(matches!(p1, PagedColumn::Resident(_, _)));

        // Second allocation overflows local (no shared) -> page out.
        let mut col2 = sample(256);
        let p2 = cp.page(&mut col2);
        assert!(matches!(p2, PagedColumn::Paged { .. }));

        // Releasing the first should refill local; a third allocation now
        // fits resident again.
        drop(p1);
        drop(p2);
        let mut col3 = sample(256);
        let p3 = cp.page(&mut col3);
        assert!(matches!(p3, PagedColumn::Resident(_, _)));
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
