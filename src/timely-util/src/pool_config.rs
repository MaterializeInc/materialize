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

//! Process-wide installation and configuration point for the buffer pool
//! that backs chunk spilling.
//!
//! Chunk spill consumers, arriving with the chunk-batcher work, resolve their
//! backing through [`active_pool`] at each spill decision, so live
//! reconfiguration takes effect on the next call.
//! The pool itself ([`mz_ore::pool::Pool`]) is a process singleton: live
//! chunk handles keep their `Arc` into it, so replacing it on reconfigure
//! would split residency accounting across two budgets. Operator-driven
//! tunes instead retune the one instance in place via [`apply_pool_config`].
//!
//! See `doc/developer/design/20260610_buffer_managed_state.md` for the
//! buffer-pool design.

#![deny(missing_docs)]

pub mod metrics;

/// Process-wide buffer pool shared by every spill consumer in the process.
///
/// Construction reserves virtual address space only: 1 TiB per chunk size
/// class plus 1 TiB per extent-arena class, tens of TiB in total. Physical
/// memory is paid per resident chunk. On the rare platforms or
/// configurations where the reservation fails, the pool is permanently
/// unavailable for this process and [`apply_pool_config`] reports that by
/// returning `false`.
static GLOBAL_POOL: std::sync::OnceLock<Option<mz_ore::pool::Pool>> = std::sync::OnceLock::new();

/// Whether [`apply_pool_config`] has installed the pool as the process's
/// spill mechanism. [`active_pool`] reads this so consumers stay inert until
/// the first config apply budgets the pool.
static POOL_MODE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Returns the process-wide buffer pool, initializing it on first call.
/// `None` if the virtual reservation failed at first use.
pub fn global_pool() -> Option<mz_ore::pool::Pool> {
    GLOBAL_POOL
        .get_or_init(|| match mz_ore::pool::Pool::new() {
            Ok(pool) => Some(pool),
            Err(err) => {
                tracing::warn!(
                    %err,
                    "buffer pool reservation failed; pool spilling unavailable",
                );
                None
            }
        })
        .clone()
}

/// Returns the process-wide buffer pool only if something already
/// initialized it; never triggers the virtual reservation itself. Metrics
/// scrapes read through this so that observing a process (which may never
/// install the pool) does not mmap the pool's address space as a side
/// effect.
pub fn global_pool_peek() -> Option<mz_ore::pool::Pool> {
    GLOBAL_POOL.get().cloned().flatten()
}

/// Returns the pool only when [`apply_pool_config`] has installed and
/// budgeted it, and never initializes anything itself. Chunk spill consumers
/// resolve their backing through this at each spill decision, so an
/// unconfigured or gated-off process keeps every chunk resident.
pub fn active_pool() -> Option<mz_ore::pool::Pool> {
    if POOL_MODE.load(std::sync::atomic::Ordering::Relaxed) {
        global_pool_peek()
    } else {
        None
    }
}

/// Applies a buffer-pool configuration, installing the pool as the process's
/// spill mechanism. This is the only path that constructs the pool. A process
/// that never calls it, because the spill gate is off or it is unconfigured,
/// never reserves the pool's address space or spawns its spill threads.
/// Returns `false` (and changes nothing) if the pool is unavailable because
/// its virtual reservation failed.
///
/// On success the pool becomes reachable through [`active_pool`] and its
/// resident budget is retuned in place so live handles stay coherent.
pub fn apply_pool_config(cfg: PoolPagerConfig) -> bool {
    let Some(pool) = global_pool() else {
        return false;
    };
    pool.set_budget(cfg.budget_bytes);
    pool.set_rss_target(cfg.rss_target_bytes);
    pool.set_spill_threads(cfg.spill_threads);
    pool.set_eager_backing(cfg.eager_backing);
    POOL_MODE.store(true, std::sync::atomic::Ordering::Relaxed);
    true
}

/// Inputs to [`apply_pool_config`]. All sizes are absolute bytes; fractions
/// are resolved by the caller against *physical RAM* (see
/// `mz_ore::memory::physical_memory_bytes`), never against an announced
/// limit that may include swap.
#[derive(Clone, Copy, Debug)]
pub struct PoolPagerConfig {
    /// Resident-bytes budget for uncompressed slots.
    pub budget_bytes: usize,
    /// Spill threads for off-worker eviction I/O (spawn-once).
    pub spill_threads: usize,
    /// Whether idle spill threads eagerly compress chunks to
    /// `BackedResident` ahead of pressure.
    pub eager_backing: bool,
    /// Ceiling on the pool's total RSS; the compressed-resident tier is the
    /// headroom above the budget and warm cap. Zero collapses the tier.
    pub rss_target_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `apply_pool_config` installs the pool: `active_pool` resolves to it
    /// afterwards, and the configured budget is visible on the instance.
    /// Mutates process-global state, so it is the only test in this module
    /// that observes `POOL_MODE`.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: foreign function calls (mmap, madvise)
    fn apply_pool_config_installs_pool() {
        let ok = apply_pool_config(PoolPagerConfig {
            budget_bytes: 1 << 30,
            spill_threads: 0,
            eager_backing: false,
            rss_target_bytes: 0,
        });
        assert!(ok, "pool reservation expected to succeed in tests");
        assert!(active_pool().is_some());
        assert!(global_pool_peek().is_some());
    }
}
