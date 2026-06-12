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

//! Column-aware pager. Pages [`Column`] instances out via [`mz_ore::pager`],
//! optionally compressing with lz4.
//!
//! The pager (`mz_ore::pager`) deals in `Vec<u64>` blobs and two backends. This
//! module adds:
//!
//! 1. A [`PagingPolicy`] trait that decides _whether_ to page out, _which
//!    backend_, and _whether to compress_. Decisions live in the policy
//!    implementation, not in the global atomic the pager exposes.
//! 2. A [`ColumnPager`] that drains a `Column<C>` into a [`PagedColumn`] and
//!    rehydrates it on demand.
//! 3. Lz4 frame-format compression as an optional codec.
//! 4. A pooled path ([`PageDecision::Pool`]) that hands the body to an
//!    [`mz_ore::pool::Pool`] instead of a pager backend. Residency becomes a
//!    state of the pool's chunk handle rather than a property baked in at
//!    pageout time — the prototype seam for
//!    `doc/developer/design/20260610_buffer_managed_state.md`.
//!
//! The serialization uses the existing [`ContainerBytes`] protocol on
//! `Column<C>`, so we get a single byte layout that both raw and compressed
//! paths share. See `doc/developer/design/20260504_pager.md` for background.

#![deny(missing_docs)]

pub mod metrics;
pub mod policy;

use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};

use columnar::Columnar;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use mz_ore::pager::{self, Backend, Handle};
use timely::bytes::arc::BytesMut;
use timely::dataflow::channels::ContainerBytes;

use crate::columnar::Column;

/// Compression codec applied to a paged-out column.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Codec {
    /// lz4 frame format (`lz4_flex::frame`). Self-delimiting, streams via
    /// `io::Read`/`io::Write`, no random access.
    Lz4,
}

/// Inputs to a pageout decision.
#[derive(Copy, Clone, Debug)]
pub struct PageHint {
    /// Uncompressed body size in bytes (matches [`ContainerBytes::length_in_bytes`]).
    pub len_bytes: usize,
}

/// Outcome of a policy decision.
#[derive(Clone, Debug)]
pub enum PageDecision {
    /// Keep the column resident; no I/O, no compression.
    Skip,
    /// Page out using the given backend and (optionally) codec.
    Page {
        /// Pager backend to use.
        backend: Backend,
        /// Compression codec, or `None` for raw bytes.
        codec: Option<Codec>,
    },
    /// Hand the body to the given buffer pool. The pool owns residency from
    /// here on: it enforces its own resident-bytes budget and compresses
    /// into swap-backed extents at eviction time, so neither a backend nor a
    /// codec choice applies.
    Pool(mz_ore::pool::Pool),
}

/// Notifications the column-pager sends back to the policy. Implementations
/// typically forward to metrics counters.
#[derive(Debug)]
pub enum PageEvent {
    /// A successful pageout. `bytes_in` is the uncompressed body size,
    /// `bytes_out` is the on-storage payload size (after compression).
    PagedOut {
        /// Uncompressed body size handed to the pager.
        bytes_in: usize,
        /// On-storage payload size after compression and padding.
        bytes_out: usize,
        /// Backend selected by the policy.
        backend: Backend,
        /// Codec selected by the policy.
        codec: Option<Codec>,
    },
    /// A successful page-in. `bytes` is the uncompressed body size delivered to
    /// the caller.
    PagedIn {
        /// Uncompressed body size delivered to the caller.
        bytes: usize,
    },
    /// A pageout failure surfaced via the underlying pager.
    Failed {
        /// Backend that produced the error.
        backend: Backend,
        /// Underlying I/O error.
        err: io::Error,
    },
    /// A resident column has been dropped. Fires from [`ResidentTicket::drop`]
    /// when the [`PagedColumn::Resident`] holding the ticket is consumed by
    /// [`ColumnPager::take`] or dropped without being taken. Policies use this
    /// to return budget allocated when [`PagingPolicy::decide`] answered
    /// [`PageDecision::Skip`].
    ResidentReleased {
        /// Uncompressed body size returned to the policy.
        bytes: usize,
    },
}

/// Decides whether/how to page a column out, and records page events.
///
/// Implementations carry their own state (counters, atomics, configuration)
/// via interior mutability. Methods take `&self` so a single policy can be
/// shared across operator threads.
pub trait PagingPolicy: Send + Sync {
    /// Returns the action to take for a column with the given hint.
    fn decide(&self, hint: PageHint) -> PageDecision;
    /// Records a pageout/pagein/failure event for metrics or adaptive decisions.
    fn record(&self, event: PageEvent);
}

/// Sizing metadata captured at pageout time. Stored alongside the payload so
/// `take` can size buffers.
#[derive(Clone, Debug)]
pub struct Meta {
    /// Uncompressed body size in bytes.
    pub len_bytes: usize,
}

/// A column whose body may be resident, paged out, or paged out and compressed.
///
/// Each variant corresponds to one of the [`PageDecision`] outcomes.
///
/// All variants are `Send`. The [`Resident`](PagedColumn::Resident) variant's
/// drop credit goes back to the policy via [`PageEvent::ResidentReleased`];
/// concrete policies that ship with this crate (notably
/// [`policy::TieredPolicy`]) credit a single process-wide atomic pool, so
/// dropping a `Resident` on a different thread than the one that called
/// [`ColumnPager::page`] is safe. Custom policies that introduce
/// thread-local accounting must take care to either pin the column to its
/// origin thread (e.g. via a `SendColumn` wrapper) or carry the origin in
/// [`PageEvent::ResidentReleased`] so credit can be routed correctly.
pub enum PagedColumn<C: Columnar> {
    /// Body kept resident. Returned when the policy answered
    /// [`PageDecision::Skip`]. The accompanying [`ResidentTicket`] fires a
    /// [`PageEvent::ResidentReleased`] when the variant is dropped or
    /// consumed by [`ColumnPager::take`], so the policy can reclaim the
    /// budget it granted in [`PagingPolicy::decide`].
    Resident(Column<C>, ResidentTicket),
    /// Raw `ContainerBytes` payload stored via [`pager::Handle`]. The backend
    /// (Swap or File) is baked into the handle.
    Paged {
        /// Pager handle owning the raw payload.
        handle: Handle,
        /// Sizing metadata.
        meta: Meta,
    },
    /// Lz4-framed serialized form. The framed bytes themselves may live in
    /// memory or in the pager (see [`CompressedInner`]).
    Compressed {
        /// Where the framed bytes live.
        inner: CompressedInner,
        /// Sizing metadata.
        meta: Meta,
    },
    /// Body held as a buffer-pool chunk. Residency is a state of the handle,
    /// not of this variant: the pool keeps the chunk resident or evicts it
    /// to a swap-backed extent under its own budget, and
    /// [`ColumnPager::take`] reads it back from wherever it currently lives.
    Pooled {
        /// Pool handle owning the chunk.
        handle: mz_ore::pool::ChunkHandle,
        /// Sizing metadata.
        meta: Meta,
    },
}

/// Drop guard that returns budget to a [`PagingPolicy`] when a
/// [`PagedColumn::Resident`] is destroyed.
///
/// The ticket holds an `Arc` to the policy and the byte count it was charged
/// for at [`PagingPolicy::decide`] time. On drop it fires a
/// [`PageEvent::ResidentReleased`] event; the policy implementation decides
/// what to credit and where (local pool, shared pool, both).
pub struct ResidentTicket {
    bytes: usize,
    policy: Arc<dyn PagingPolicy>,
}

impl Drop for ResidentTicket {
    fn drop(&mut self) {
        // Zero-byte tickets were never charged (see
        // `PagedColumn::resident_untracked`); releasing them would only add
        // metric noise.
        if self.bytes == 0 {
            return;
        }
        metrics::observe_resident_released(self.bytes);
        self.policy
            .record(PageEvent::ResidentReleased { bytes: self.bytes });
    }
}

impl<C: Columnar> PagedColumn<C> {
    /// Wraps `col` as a resident paged column without consulting any policy
    /// or charging any budget; the ticket is inert. For callers that keep a
    /// column resident by construction — deciding before any pager is
    /// involved — rather than by a policy's grant.
    pub fn resident_untracked(col: Column<C>) -> Self {
        static INERT: LazyLock<Arc<dyn PagingPolicy>> =
            LazyLock::new(|| Arc::new(AlwaysResidentPolicy));
        let ticket = ResidentTicket {
            bytes: 0,
            policy: Arc::clone(&INERT),
        };
        PagedColumn::Resident(col, ticket)
    }
}

/// Storage location for the lz4-framed bytes inside a compressed paged column.
pub enum CompressedInner {
    /// Owned `Vec<u8>` held resident in the caller's address space.
    Memory(Vec<u8>),
    /// Framed bytes padded to a `u64` boundary and handed to the pager. The
    /// frame trailer self-delimits, so the trailing pad is ignored on read.
    Paged(Handle),
}

/// Pages typed [`Column`]s out and back in, driven by a [`PagingPolicy`].
///
/// Cheap to clone (it's an `Arc`). Hold one per operator if you want per-site
/// policy state, or share globally if you want one policy.
#[derive(Clone)]
pub struct ColumnPager {
    policy: Arc<dyn PagingPolicy>,
}

impl ColumnPager {
    /// Constructs a column pager driven by `policy`.
    pub fn new(policy: Arc<dyn PagingPolicy>) -> Self {
        Self { policy }
    }

    /// Constructs a pager that never pages out: every [`page`] returns a
    /// [`PagedColumn::Resident`] whose ticket discards release events. Useful
    /// as a default when callers want a placeholder pager before injecting a
    /// real policy.
    ///
    /// [`page`]: ColumnPager::page
    pub fn disabled() -> Self {
        Self::new(Arc::new(AlwaysResidentPolicy))
    }

    /// Constructs a pager backed by `pool`: every non-empty [`page`] routes
    /// the body into the pool, which enforces its own resident-bytes budget
    /// (see [`policy::PoolPolicy`]). A prototype seam, opt-in via callers'
    /// pager injection points rather than the global pager plumbing.
    ///
    /// [`page`]: ColumnPager::page
    pub fn pooled(pool: mz_ore::pool::Pool) -> Self {
        Self::new(Arc::new(policy::PoolPolicy::new(pool)))
    }
}

/// Policy that keeps every column resident and discards events. Backs
/// [`ColumnPager::disabled`].
struct AlwaysResidentPolicy;

impl PagingPolicy for AlwaysResidentPolicy {
    fn decide(&self, _hint: PageHint) -> PageDecision {
        PageDecision::Skip
    }
    fn record(&self, _event: PageEvent) {}
}

//
// Following the pager design doc's spirit (`doc/developer/design/20260504_pager.md`):
// "the cluster runs on swap or file, not both at once; a global atomic
// encodes that operational reality directly. A per-pager design would
// either duplicate the global flag at the struct level or invite confusion
// about which configuration wins."
//
// The configuration state is exactly two bits — which shared mechanism is
// installed ([`POOL_MODE`]) and whether compute's own batchers are enabled
// ([`COMPUTE_ENABLED`]) — plus the two mechanism singletons and the
// [`SWAP_PAGEOUT`] toggle. Every pager a consumer sees is *derived* from
// those bits at the moment it asks ([`global_pager`] for compute,
// [`shared_pager`] for per-consumer opt-ins), so there is one resolution
// path and nothing cached to fall out of sync. Consumers that capture a
// pager (at render, say) keep it until they next ask; live reconfiguration
// takes effect on the next call.

/// Whether compute's own batchers page through the shared mechanism.
/// [`global_pager`] derives from this; set by the `apply_*_config` calls.
static COMPUTE_ENABLED: AtomicBool = AtomicBool::new(false);

/// Process-global toggle for `MADV_PAGEOUT` on the lz4 + swap spill path.
///
/// When set, [`ColumnPager::page`] issues `MADV_PAGEOUT` over the compressed
/// bytes it keeps resident in [`CompressedInner::Memory`], proactively evicting
/// them at spill time instead of leaving them for lazy kernel reclaim. A single
/// process-global flag (set by [`apply_tiered_config`]) mirrors the backend /
/// codec selection: it is a process-wide operational choice, not a per-column
/// one, and every consumer of the shared pager reads the same value. Defaults
/// to off; the eager-reclaim syscall stays gated until proven.
static SWAP_PAGEOUT: AtomicBool = AtomicBool::new(false);

/// Process-wide [`policy::TieredPolicy`] singleton.
///
/// Why a singleton: every `ResidentTicket` keeps an `Arc<dyn PagingPolicy>`
/// pointing at the policy that decided to keep the column resident.
/// Replacing the global `TieredPolicy` would orphan in-flight tickets onto
/// the previous instance — they would credit a budget atomic that the new
/// policy can no longer see, draining the new pool monotonically until it
/// locks up on Page decisions. A persistent singleton with in-place
/// [`policy::TieredPolicy::reconfigure`] sidesteps the issue: all tickets,
/// past and present, share the same atomic.
///
/// Initialized eagerly with zero budget so [`metrics::register`] can read
/// it during compute startup, before any [`apply_tiered_config`] call. The
/// first config apply resizes the pool via `reconfigure`, which is the same
/// path operator-driven tunes take.
static TIERED_POLICY: LazyLock<Arc<policy::TieredPolicy>> =
    LazyLock::new(|| Arc::new(policy::TieredPolicy::new(0, Backend::Swap, None)));

/// Returns a reference to the process-wide [`policy::TieredPolicy`] singleton.
pub fn tiered_policy() -> &'static policy::TieredPolicy {
    &TIERED_POLICY
}

/// Apply a tiered-pager configuration. Reuses the singleton
/// [`policy::TieredPolicy`] so in-flight `ResidentTicket`s remain coherent
/// with the running budget after the operator tunes any of the inputs.
///
/// Makes the tiered policy the shared mechanism; [`global_pager`] resolves
/// to it when `enabled` and to the disabled pager otherwise. With paging
/// disabled, in-flight tickets still credit the singleton, which is
/// harmless: the budget grows above the configured total until the next
/// enable reconciles it via `reconfigure`.
///
/// `swap_pageout` toggles `MADV_PAGEOUT` on the lz4 + swap spill path (see
/// `SWAP_PAGEOUT`); it is stored unconditionally so the next `page` call
/// observes it regardless of `enabled`.
pub fn apply_tiered_config(
    enabled: bool,
    total_budget: usize,
    backend: Backend,
    codec: Option<Codec>,
    swap_pageout: bool,
) {
    SWAP_PAGEOUT.store(swap_pageout, Ordering::Relaxed);
    TIERED_POLICY.reconfigure(total_budget, backend, codec);
    POOL_MODE.store(false, std::sync::atomic::Ordering::Relaxed);
    COMPUTE_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

/// Process-wide buffer pool shared by every pooled pager in the process.
///
/// A singleton for the same reason [`TIERED_POLICY`] is one: live
/// [`PagedColumn::Pooled`] handles keep their `Arc` into the pool, so
/// replacing it on reconfigure would split residency accounting across two
/// budgets. Operator-driven tunes go through
/// [`mz_ore::pool::Pool::set_budget`] on the one instance instead.
///
/// Construction reserves virtual address space only (a few GiB per size
/// class); physical memory is paid per resident chunk. On the rare platforms
/// or configurations where the reservation fails, the pool is permanently
/// unavailable for this process and [`apply_pool_config`] reports that by
/// returning `false` so callers can fall back to the tiered path.
static GLOBAL_POOL: std::sync::OnceLock<Option<mz_ore::pool::Pool>> = std::sync::OnceLock::new();

/// Whether the pool is the active shared spill mechanism (set by
/// [`apply_pool_config`], cleared by [`apply_tiered_config`]). Read by
/// [`shared_pager`] so per-consumer opt-ins follow whichever mechanism the
/// last config apply installed.
static POOL_MODE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Returns the process-wide buffer pool, initializing it on first call.
/// `None` if the virtual reservation failed at first use.
pub fn global_pool() -> Option<mz_ore::pool::Pool> {
    GLOBAL_POOL
        .get_or_init(
            || match mz_ore::pool::Pool::new(mz_ore::pool::PoolConfig::default()) {
                Ok(pool) => Some(pool),
                Err(err) => {
                    tracing::warn!(
                        %err,
                        "column pager: buffer pool reservation failed; pool mode unavailable",
                    );
                    None
                }
            },
        )
        .clone()
}

/// Returns the process-wide buffer pool only if something already
/// initialized it; never triggers the virtual reservation itself. Metrics
/// scrapes read through this so that observing a process (which may never
/// enable pool mode) does not mmap the pool's address space as a side
/// effect.
pub fn global_pool_peek() -> Option<mz_ore::pool::Pool> {
    GLOBAL_POOL.get().cloned().flatten()
}

/// Apply a pool-backed pager configuration. Returns `false` (and changes
/// nothing) if the pool is unavailable, so the caller can fall back to
/// [`apply_tiered_config`].
///
/// On success the pool becomes the active shared mechanism — [`global_pager`]
/// resolves to it when `enabled`, and per-consumer opt-ins via
/// [`shared_pager`] reach it either way — and the pool's resident budget is
/// retuned in place so live handles stay coherent.
pub fn apply_pool_config(cfg: PoolPagerConfig) -> bool {
    let Some(pool) = global_pool() else {
        return false;
    };
    pool.set_budget(cfg.budget_bytes);
    pool.set_rss_target(cfg.rss_target_bytes);
    pool.set_spill_threads(cfg.spill_threads);
    pool.set_eager_backing(cfg.eager_backing);
    POOL_MODE.store(true, std::sync::atomic::Ordering::Relaxed);
    COMPUTE_ENABLED.store(cfg.enabled, std::sync::atomic::Ordering::Relaxed);
    true
}

/// Inputs to [`apply_pool_config`]. All sizes are absolute bytes; fractions
/// are resolved by the caller against *physical RAM* (see
/// `mz_ore::memory::physical_memory_bytes`), never against an announced
/// limit that may include swap.
#[derive(Clone, Copy, Debug)]
pub struct PoolPagerConfig {
    /// Whether compute's own batchers page through the pool.
    pub enabled: bool,
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

/// The pager for compute's own batchers: [`shared_pager`] resolved against
/// the compute enable bit the last `apply_*_config` call stored. Cheap (one
/// `Arc` clone); called per chunk, so unlike [`shared_pager`] it does not
/// log its resolution.
pub fn global_pager() -> ColumnPager {
    resolve_shared(COMPUTE_ENABLED.load(std::sync::atomic::Ordering::Relaxed)).0
}

/// A pager that, when `enabled`, draws from the process-wide shared spill
/// mechanism — the buffer pool when [`apply_pool_config`] installed it, else
/// the [`tiered_policy`] budget `apply_tiered_config` sizes — and otherwise is
/// a disabled (always-resident) pager.
///
/// This lets a second consumer (e.g. the storage upsert source stash) opt into
/// the one shared budget independently of whether the config apply enabled
/// the process-global pager for its own (compute) batchers. There is still a
/// single budget; only the enable decision is per-consumer. Which mechanism
/// is shared follows the most recent config apply ([`apply_pool_config`] vs
/// [`apply_tiered_config`]), so a consumer that captured a pager before a
/// mechanism flip keeps its old one until it next calls here.
pub fn shared_pager(enabled: bool) -> ColumnPager {
    let (pager, resolved) = resolve_shared(enabled);
    tracing::info!(
        enabled,
        pool_mode = POOL_MODE.load(std::sync::atomic::Ordering::Relaxed),
        "shared column pager resolved: {resolved}",
    );
    pager
}

/// The one resolution path from the two configuration bits to a pager.
/// Returns the pager and a label naming the resolution for logs.
fn resolve_shared(enabled: bool) -> (ColumnPager, &'static str) {
    if !enabled {
        return (ColumnPager::disabled(), "disabled");
    }
    if POOL_MODE.load(std::sync::atomic::Ordering::Relaxed) {
        if let Some(pool) = global_pool() {
            return (ColumnPager::pooled(pool), "pool");
        }
    }
    #[allow(clippy::clone_on_ref_ptr)]
    let dyn_policy: Arc<dyn PagingPolicy> = TIERED_POLICY.clone();
    (ColumnPager::new(dyn_policy), "tiered")
}

impl ColumnPager {
    /// Drains `col` into a [`PagedColumn`]. After return `col` is left as a
    /// fresh `Column::default()` (typed, empty), ready to be refilled by the
    /// caller on the next loop iteration.
    ///
    /// Backend / codec semantics:
    ///
    /// * Uncompressed, [`Column::Align`]: the inner `Vec<u64>` is moved into
    ///   the pager handle with no copies. Swap backend keeps the allocation
    ///   resident; file backend writes it out and drops it.
    /// * Uncompressed, other variants: the column is serialized via
    ///   [`ContainerBytes::into_bytes`] into a `Vec<u8>`, copied into a
    ///   u64-aligned `Vec<u64>`, then handed to the pager.
    /// * Compressed: the column is serialized through an [`FrameEncoder`]
    ///   directly into the output buffer. No intermediate uncompressed
    ///   `Vec<u8>` is materialized.
    pub fn page<C: Columnar>(&self, col: &mut Column<C>) -> PagedColumn<C> {
        let len_bytes = col.length_in_bytes();
        let hint = PageHint { len_bytes };

        let (backend, codec) = match self.policy.decide(hint) {
            PageDecision::Skip => {
                metrics::observe_skip(len_bytes);
                let ticket = ResidentTicket {
                    bytes: len_bytes,
                    policy: Arc::clone(&self.policy),
                };
                // A resident chunk joins a merge chain and may live there
                // across many merge rounds. A `Column::Typed` body arrives
                // carrying `Column::merge_from`'s worst-case `reserve_for`
                // capacity — sized for the unconsolidated union of both merge
                // inputs — and parking that slack in the chain is the dominant
                // source of merge-batcher resident memory. Serialize the body
                // into a `Vec<u64>` sized exactly to its content and store the
                // fitting `Column::Align` instead, clearing `col` in place (as
                // the codec paths below do) so the high-capacity typed buffer
                // stays with the caller for recycling. `Align` / `Bytes`
                // bodies are already fitting (or refcounted), so move them
                // through unchanged.
                let resident = if matches!(col, Column::Typed(_)) {
                    debug_assert_eq!(len_bytes % 8, 0);
                    let mut buf = Vec::with_capacity(len_bytes);
                    col.into_bytes(&mut buf);
                    debug_assert_eq!(buf.len() % 8, 0);
                    col.clear();
                    Column::Align(bytemuck::allocation::pod_collect_to_vec::<u8, u64>(&buf))
                } else {
                    std::mem::take(col)
                };
                return PagedColumn::Resident(resident, ticket);
            }
            PageDecision::Pool(pool) => {
                debug_assert_eq!(len_bytes % 8, 0);
                // Serialize straight into the pool slot: one page population,
                // no staging buffers. The `Align` variant is already the
                // serialized form and copies in directly; other variants
                // write their `ContainerBytes` encoding through a cursor over
                // the slot memory. Sizing is exact, so a short or overlong
                // write is a `ContainerBytes` contract violation and panics
                // via the cursor's bounds.
                let handle = match std::mem::take(col) {
                    Column::Align(v) => {
                        pool.insert_with(v.len(), |dst| dst.copy_from_slice(v.as_slice()))
                    }
                    mut other => {
                        let handle = pool.insert_with(len_bytes / 8, |dst| {
                            let bytes: &mut [u8] = bytemuck::cast_slice_mut(dst);
                            let mut cursor = std::io::Cursor::new(bytes);
                            other.into_bytes(&mut cursor);
                            assert_eq!(
                                usize::try_from(cursor.position()).expect("usize position"),
                                len_bytes,
                                "serialized body must fill the chunk exactly",
                            );
                        });
                        // `into_bytes` only borrowed `other`; clear it in
                        // place and hand it back so the caller keeps the
                        // `Typed` allocation for its next refill.
                        other.clear();
                        *col = other;
                        handle
                    }
                };
                // The pool compresses internally at eviction time, so the
                // policy-visible size is the uncompressed body on both sides.
                // The pool's extent store is swap-backed.
                metrics::observe_pageout(len_bytes, len_bytes);
                self.policy.record(PageEvent::PagedOut {
                    bytes_in: len_bytes,
                    bytes_out: len_bytes,
                    backend: Backend::Swap,
                    codec: None,
                });
                return PagedColumn::Pooled {
                    handle,
                    meta: Meta { len_bytes },
                };
            }
            PageDecision::Page { backend, codec } => (backend, codec),
        };
        let meta = Meta { len_bytes };

        match codec {
            None => {
                // Raw path: the body must end up as u64-aligned bytes for the
                // pager. `Column::Align` already is; other variants are
                // serialized and copied.
                debug_assert_eq!(len_bytes % 8, 0);
                let body = drain_to_aligned(col, len_bytes);
                let handle = pager::pageout_with(backend, &mut [body]);
                let bytes_out = handle.len_bytes();
                metrics::observe_pageout(len_bytes, bytes_out);
                self.policy.record(PageEvent::PagedOut {
                    bytes_in: len_bytes,
                    bytes_out,
                    backend,
                    codec: None,
                });
                PagedColumn::Paged { handle, meta }
            }
            Some(Codec::Lz4) => {
                // Stream serialized bytes straight into lz4 — no intermediate
                // uncompressed `Vec<u8>`.
                let mut out = Vec::with_capacity(len_bytes / 4);
                {
                    let mut enc = FrameEncoder::new(&mut out);
                    col.into_bytes(&mut enc);
                    enc.finish().expect("lz4 finish into Vec is infallible");
                }
                // `into_bytes` borrows `col`, so empty it explicitly now that
                // its bytes live (compressed) in `out`. `clear` retains the
                // `Typed` allocation so the caller can refill it, rather than
                // dropping a buffer it may want to reuse.
                col.clear();
                metrics::observe_pageout(len_bytes, out.len());
                self.policy.record(PageEvent::PagedOut {
                    bytes_in: len_bytes,
                    bytes_out: out.len(),
                    backend,
                    codec: Some(Codec::Lz4),
                });
                let inner = match backend {
                    Backend::Swap => {
                        // The compressed bytes stay resident in our own address
                        // space (we read them back in `take` via `FrameDecoder`),
                        // so the pager's ownership-transferring `pageout` does not
                        // fit. When `SWAP_PAGEOUT` is set, hint `MADV_PAGEOUT`
                        // instead: it proactively swaps the pages out now, holding
                        // RSS at the budget rather than leaving them as unmanaged
                        // anonymous memory the kernel only reclaims lazily at the
                        // pressure cliff. A later read re-faults them back in —
                        // cheap, since lz4 shrank the byte volume.
                        if SWAP_PAGEOUT.load(Ordering::Relaxed) {
                            pager::advise_pageout(&out);
                        }
                        CompressedInner::Memory(out)
                    }
                    Backend::File => {
                        // The pager deals in `Vec<u64>`, so the framed bytes
                        // must be widened. `out` is already compressed (~4x
                        // smaller than the source), so this copy is over the
                        // small form; avoiding it would mean a byte-oriented
                        // pager entry point, not worth widening that surface.
                        let padded = pad_u8_to_u64(out);
                        let handle = pager::pageout_with(Backend::File, &mut [padded]);
                        CompressedInner::Paged(handle)
                    }
                };
                PagedColumn::Compressed { inner, meta }
            }
        }
    }

    /// Rehydrates `paged` into a [`Column<C>`]. Consumes the handle and
    /// reclaims its storage (file backend unlinks; swap backend drops the
    /// `Vec`).
    pub fn take<C: Columnar>(&self, paged: PagedColumn<C>) -> Column<C> {
        match paged {
            // `_ticket` drops here and fires `PageEvent::ResidentReleased`.
            PagedColumn::Resident(c, _ticket) => c,
            PagedColumn::Paged { handle, meta } => {
                let mut body: Vec<u64> = Vec::with_capacity(handle.len());
                pager::take(handle, &mut body);
                debug_assert_eq!(body.len() * 8, meta.len_bytes);
                metrics::observe_pagein(meta.len_bytes);
                self.policy.record(PageEvent::PagedIn {
                    bytes: meta.len_bytes,
                });
                Column::Align(body)
            }
            PagedColumn::Compressed { inner, meta } => {
                let mut decoded = Vec::with_capacity(meta.len_bytes);
                match inner {
                    CompressedInner::Memory(v) => {
                        FrameDecoder::new(&v[..])
                            .read_to_end(&mut decoded)
                            .expect("lz4 decode from memory");
                    }
                    CompressedInner::Paged(h) => {
                        let mut padded = Vec::with_capacity(h.len());
                        pager::take(h, &mut padded);
                        let src: &[u8] = bytemuck::cast_slice(&padded);
                        FrameDecoder::new(src)
                            .read_to_end(&mut decoded)
                            .expect("lz4 decode from pager");
                    }
                }
                debug_assert_eq!(decoded.len(), meta.len_bytes);
                metrics::observe_pagein(decoded.len());
                self.policy.record(PageEvent::PagedIn {
                    bytes: decoded.len(),
                });
                // `BytesMut::from` wraps the `Vec<u8>` without copying; `freeze`
                // produces the refcounted `Bytes` that `ContainerBytes` expects.
                Column::from_bytes(BytesMut::from(decoded).freeze())
            }
            PagedColumn::Pooled { handle, meta } => {
                let mut body: Vec<u64> = Vec::with_capacity(handle.len());
                handle.take(&mut body);
                debug_assert_eq!(body.len() * 8, meta.len_bytes);
                metrics::observe_pagein(meta.len_bytes);
                self.policy.record(PageEvent::PagedIn {
                    bytes: meta.len_bytes,
                });
                Column::Align(body)
            }
        }
    }
}

/// Drains `col` into the u64-aligned raw body shared by the uncompressed
/// pageout paths: a [`Column::Align`] moves its buffer out with no copy
/// (leaving `col` a refill-ready `Typed` default), while other variants
/// serialize via [`ContainerBytes::into_bytes`] and widen the bytes, handing
/// the cleared `Typed` allocation back to `col` for reuse.
fn drain_to_aligned<C: Columnar>(col: &mut Column<C>, len_bytes: usize) -> Vec<u64> {
    match std::mem::take(col) {
        Column::Align(v) => v,
        mut other => {
            let mut buf = Vec::with_capacity(len_bytes);
            other.into_bytes(&mut buf);
            debug_assert_eq!(buf.len() % 8, 0);
            // `into_bytes` only borrowed `other`; clear it in place and hand
            // it back so the caller keeps the `Typed` allocation instead of
            // us dropping a reusable buffer.
            other.clear();
            *col = other;
            bytemuck::allocation::pod_collect_to_vec::<u8, u64>(&buf)
        }
    }
}

/// Reinterprets `bytes` as a `Vec<u64>` by trailing-zero padding to a multiple
/// of 8 and copying. The lz4 frame trailer self-delimits so the trailing pad is
/// invisible to [`FrameDecoder`].
fn pad_u8_to_u64(mut bytes: Vec<u8>) -> Vec<u64> {
    let pad = bytes.len().next_multiple_of(8) - bytes.len();
    if pad != 0 {
        bytes.resize(bytes.len() + pad, 0);
    }
    debug_assert_eq!(bytes.len() % 8, 0);
    // `Vec<u8>` and `Vec<u64>` have different layouts (size + align), so we
    // can't transmute the allocation. Copy into a fresh, properly aligned
    // `Vec<u64>`. The cost is one `len_bytes/8`-word memcpy per pageout.
    let len_u64s = bytes.len() / 8;
    let mut out = vec![0u64; len_u64s];
    let dst: &mut [u8] = bytemuck::cast_slice_mut(&mut out);
    dst.copy_from_slice(&bytes);
    out
}

#[cfg(test)]
#[allow(clippy::clone_on_ref_ptr)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use columnar::Index;
    use timely::container::PushInto;

    use super::*;

    /// Shared scratch directory for all tests in this module. `set_scratch_dir`
    /// is idempotent and only honors the first path it sees, so individual
    /// tests cannot bring their own tempdir without races when run in parallel
    /// (a tempdir dropped at test end would invalidate `SUBDIR` for any peer
    /// still running).
    fn ensure_scratch() {
        static DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
        let dir = DIR.get_or_init(|| tempfile::tempdir().expect("tempdir"));
        pager::set_scratch_dir(dir.path().to_path_buf());
    }

    /// Promotes a typed policy `Arc` to `Arc<dyn PagingPolicy>`. Hides the
    /// unsize coercion behind a `clone()` so the trait object is constructed
    /// without the now-discouraged `as` cast.
    fn as_dyn(p: &Arc<impl PagingPolicy + 'static>) -> Arc<dyn PagingPolicy> {
        p.clone()
    }

    /// Recording policy: configurable decision, counts events.
    struct TestPolicy {
        decision: PageDecision,
        out: AtomicUsize,
        r#in: AtomicUsize,
    }

    impl TestPolicy {
        fn new(decision: PageDecision) -> Arc<Self> {
            Arc::new(Self {
                decision,
                out: AtomicUsize::new(0),
                r#in: AtomicUsize::new(0),
            })
        }
    }

    impl PagingPolicy for TestPolicy {
        fn decide(&self, _hint: PageHint) -> PageDecision {
            self.decision.clone()
        }
        fn record(&self, event: PageEvent) {
            match event {
                PageEvent::PagedOut { .. } => {
                    self.out.fetch_add(1, Ordering::Relaxed);
                }
                PageEvent::PagedIn { .. } => {
                    self.r#in.fetch_add(1, Ordering::Relaxed);
                }
                PageEvent::ResidentReleased { .. } | PageEvent::Failed { .. } => {}
            }
        }
    }

    /// Builds a sample typed column of `i64`s.
    fn sample_typed() -> Column<i64> {
        let mut col: Column<i64> = Default::default();
        for v in 0i64..1024 {
            col.push_into(v);
        }
        col
    }

    /// Drains a column into a `Vec<i64>` for comparison via `borrow`.
    fn collect_i64(col: &Column<i64>) -> Vec<i64> {
        col.borrow().into_index_iter().copied().collect()
    }

    #[mz_ore::test]
    fn skip_policy_keeps_resident() {
        let policy = TestPolicy::new(PageDecision::Skip);
        let cp = ColumnPager::new(as_dyn(&policy));
        let mut col = sample_typed();
        let paged = cp.page(&mut col);
        assert!(matches!(paged, PagedColumn::Resident(_, _)));
        let rt = cp.take(paged);
        assert_eq!(collect_i64(&rt), (0i64..1024).collect::<Vec<_>>());
        assert_eq!(policy.out.load(Ordering::Relaxed), 0);
        assert_eq!(policy.r#in.load(Ordering::Relaxed), 0);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `madvise` on OS `linux`
    fn round_trip_swap_uncompressed() {
        let policy = TestPolicy::new(PageDecision::Page {
            backend: Backend::Swap,
            codec: None,
        });
        let cp = ColumnPager::new(as_dyn(&policy));
        let mut col = sample_typed();
        let paged = cp.page(&mut col);
        assert!(matches!(paged, PagedColumn::Paged { .. }));
        let rt = cp.take(paged);
        assert_eq!(collect_i64(&rt), (0i64..1024).collect::<Vec<_>>());
        assert_eq!(policy.out.load(Ordering::Relaxed), 1);
        assert_eq!(policy.r#in.load(Ordering::Relaxed), 1);
    }

    #[mz_ore::test]
    fn round_trip_swap_lz4() {
        let policy = TestPolicy::new(PageDecision::Page {
            backend: Backend::Swap,
            codec: Some(Codec::Lz4),
        });
        let cp = ColumnPager::new(as_dyn(&policy));
        let mut col = sample_typed();
        let paged = cp.page(&mut col);
        assert!(matches!(
            paged,
            PagedColumn::Compressed {
                inner: CompressedInner::Memory(_),
                ..
            }
        ));
        let rt = cp.take(paged);
        assert_eq!(collect_i64(&rt), (0i64..1024).collect::<Vec<_>>());
    }

    /// Serializes tests that mutate the process-global pager configuration
    /// (`POOL_MODE` / `COMPUTE_ENABLED` / `SWAP_PAGEOUT` / the singletons);
    /// concurrent mutation makes their assertions race. Poison is recovered:
    /// a prior test's panic doesn't invalidate the globals contract here.
    fn global_config_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// With the swap-pageout flag on, the lz4 + swap path issues `MADV_PAGEOUT`
    /// over the compressed bytes; the round-trip must still reproduce the input
    /// (the advice is a non-destructive reclaim hint). Drives the global pager
    /// through `apply_tiered_config` — the only path that sets the flag — with a
    /// zero budget so every column spills. Resets the globals on the way out so
    /// peer tests see the default disabled pager.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `madvise` on OS `linux`
    fn round_trip_swap_lz4_pageout() {
        let _guard = global_config_lock();
        apply_tiered_config(true, 0, Backend::Swap, Some(Codec::Lz4), true);
        let cp = global_pager();
        let mut col = sample_typed();
        let paged = cp.page(&mut col);
        assert!(matches!(
            paged,
            PagedColumn::Compressed {
                inner: CompressedInner::Memory(_),
                ..
            }
        ));
        let rt = cp.take(paged);
        assert_eq!(collect_i64(&rt), (0i64..1024).collect::<Vec<_>>());
        apply_tiered_config(false, 0, Backend::Swap, None, false);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn round_trip_file_uncompressed() {
        ensure_scratch();
        let policy = TestPolicy::new(PageDecision::Page {
            backend: Backend::File,
            codec: None,
        });
        let cp = ColumnPager::new(as_dyn(&policy));
        let mut col = sample_typed();
        let paged = cp.page(&mut col);
        assert!(matches!(paged, PagedColumn::Paged { .. }));
        let rt = cp.take(paged);
        assert_eq!(collect_i64(&rt), (0i64..1024).collect::<Vec<_>>());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `writev` on OS `linux`
    fn round_trip_file_lz4() {
        ensure_scratch();
        let policy = TestPolicy::new(PageDecision::Page {
            backend: Backend::File,
            codec: Some(Codec::Lz4),
        });
        let cp = ColumnPager::new(as_dyn(&policy));
        let mut col = sample_typed();
        let paged = cp.page(&mut col);
        assert!(matches!(
            paged,
            PagedColumn::Compressed {
                inner: CompressedInner::Paged(_),
                ..
            }
        ));
        let rt = cp.take(paged);
        assert_eq!(collect_i64(&rt), (0i64..1024).collect::<Vec<_>>());
    }

    /// Builds a small pool with a modest virtual reservation per size class.
    fn test_pool(budget_bytes: usize) -> mz_ore::pool::Pool {
        let pool = mz_ore::pool::Pool::new(mz_ore::pool::PoolConfig {
            class_capacity_bytes: 64 << 20,
        })
        .expect("pool creation");
        pool.set_budget(budget_bytes);
        pool
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn round_trip_pooled() {
        let pool = test_pool(256 << 20);
        let cp = ColumnPager::pooled(pool.clone());
        let mut col = sample_typed();
        let paged = cp.page(&mut col);
        let PagedColumn::Pooled { handle, meta } = &paged else {
            panic!("expected Pooled");
        };
        assert_eq!(handle.len_bytes(), meta.len_bytes);
        // Push the chunk out to its extent and poison the freed slots, so a
        // `take` that read stale slot memory (the macOS `MADV_DONTNEED`
        // hazard via free-list reuse) would fail the content check below.
        pool.evict(handle);
        pool.poison_free_slots();
        let rt = cp.take(paged);
        assert_eq!(collect_i64(&rt), (0i64..1024).collect::<Vec<_>>());
        let stats = pool.stats();
        assert_eq!(stats.inserts, 1);
        assert_eq!(stats.faults, 1);
        assert_eq!(stats.frees, 1);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // mmap and madvise are foreign calls
    fn pooled_align_fast_path() {
        let pool = test_pool(256 << 20);
        let cp = ColumnPager::pooled(pool);
        let body: Vec<u64> = (1u64..=512).collect();
        let mut col: Column<i64> = Column::Align(body.clone());
        let paged = cp.page(&mut col);
        assert!(matches!(paged, PagedColumn::Pooled { .. }));
        // After paging an Align variant, `col` is reset to the typed default.
        assert!(matches!(col, Column::Typed(_)));
        let rt = cp.take(paged);
        match rt {
            Column::Align(v) => assert_eq!(v, body),
            other => panic!("expected Align, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[mz_ore::test]
    fn align_variant_fast_path() {
        // Construct an Align column directly to exercise the move-only raw path.
        let policy = TestPolicy::new(PageDecision::Page {
            backend: Backend::Swap,
            codec: None,
        });
        let cp = ColumnPager::new(as_dyn(&policy));
        let body: Vec<u64> = (1u64..=512).collect();
        let mut col: Column<i64> = Column::Align(body.clone());
        let paged = cp.page(&mut col);
        assert!(matches!(paged, PagedColumn::Paged { .. }));
        // After paging an Align variant, `col` is reset to the typed default.
        assert!(matches!(col, Column::Typed(_)));
        let rt = cp.take(paged);
        // Round-tripped column should produce identical bytes.
        match rt {
            Column::Align(v) => assert_eq!(v, body),
            other => panic!("expected Align, got {:?}", std::mem::discriminant(&other)),
        }
    }

    /// Exercises the process-global mechanism switch end to end. Runs as one
    /// test because it mutates the process-wide `POOL_MODE` / global pager;
    /// serialized against the other global-mutating tests via
    /// [`global_config_lock`].
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: foreign function calls (mmap, madvise)
    fn pool_mode_routing() {
        let _guard = global_config_lock();
        // Pool mode on: the global pager and shared pager both go pooled.
        let ok = apply_pool_config(PoolPagerConfig {
            enabled: true,
            budget_bytes: 1 << 30,
            spill_threads: 0,
            eager_backing: false,
            rss_target_bytes: 0,
        });
        assert!(ok, "pool reservation expected to succeed in tests");
        let mut col = sample_typed();
        let paged = global_pager().page(&mut col);
        assert!(matches!(paged, PagedColumn::Pooled { .. }));
        let rt = global_pager().take(paged);
        assert_eq!(collect_i64(&rt), (0i64..1024).collect::<Vec<_>>());

        let mut col = sample_typed();
        let paged = shared_pager(true).page(&mut col);
        assert!(matches!(paged, PagedColumn::Pooled { .. }));
        drop(shared_pager(true).take(paged));

        // Disabled consumers stay resident regardless of mechanism.
        let mut col = sample_typed();
        let paged = shared_pager(false).page(&mut col);
        assert!(matches!(paged, PagedColumn::Resident(_, _)));
        drop(paged);

        // Tiered config flips the mechanism back: shared pager no longer pools.
        apply_tiered_config(true, usize::MAX, Backend::Swap, None, false);
        let mut col = sample_typed();
        let paged = shared_pager(true).page(&mut col);
        assert!(
            !matches!(paged, PagedColumn::Pooled { .. }),
            "tiered mode must not hand out pooled columns",
        );
        drop(paged);

        // Leave the globals in the disabled state for any future test runs.
        apply_tiered_config(false, 0, Backend::Swap, None, false);
    }
}
