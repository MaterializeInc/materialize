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
//!
//! The serialization uses the existing [`ContainerBytes`] protocol on
//! `Column<C>`, so we get a single byte layout that both raw and compressed
//! paths share. See `doc/developer/design/20260504_pager.md` for background.

#![deny(missing_docs)]

pub mod policy;

use std::io::{self, Read};
use std::sync::{Arc, LazyLock, RwLock};

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
#[derive(Copy, Clone, Debug)]
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
        self.policy
            .record(PageEvent::ResidentReleased { bytes: self.bytes });
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
// The lower-level `mz_ore::pager` already uses a global atomic for backend
// selection. This module's policy/budget layer mirrors that shape: one
// `ColumnPager` per process, swapped atomically when the controller changes
// the configuration. Merge batchers clone the `Arc` inside on use; live
// reinstalls take effect on the next call without per-thread coordination.

/// Process-global active pager. Defaults to [`ColumnPager::disabled`]
/// until worker init calls [`set_global_pager`].
static GLOBAL_PAGER: LazyLock<RwLock<ColumnPager>> =
    LazyLock::new(|| RwLock::new(ColumnPager::disabled()));

/// Install `pager` as the process-wide active pager. Subsequent
/// [`global_pager`] calls return a clone of this value across all threads.
///
/// Prefer [`apply_tiered_config`] for the production path so the
/// `TieredPolicy` budget atomic stays stable across reconfigures. Direct
/// `set_global_pager` use is appropriate for tests, the disabled pager, or
/// callers that intentionally want a fresh policy.
pub fn set_global_pager(pager: ColumnPager) {
    *GLOBAL_PAGER.write().expect("global pager poisoned") = pager;
}

/// Process-wide [`policy::TieredPolicy`] singleton. Lazily initialized on
/// the first [`apply_tiered_config`] call.
///
/// Why a singleton: every `ResidentTicket` keeps an `Arc<dyn PagingPolicy>`
/// pointing at the policy that decided to keep the column resident.
/// Replacing the global `TieredPolicy` would orphan in-flight tickets onto
/// the previous instance — they would credit a budget atomic that the new
/// policy can no longer see, draining the new pool monotonically until it
/// locks up on Page decisions. A persistent singleton with in-place
/// [`policy::TieredPolicy::reconfigure`] sidesteps the issue: all tickets,
/// past and present, share the same atomic.
static TIERED_POLICY: std::sync::OnceLock<Arc<policy::TieredPolicy>> = std::sync::OnceLock::new();

/// Apply a tiered-pager configuration. Reuses the singleton
/// [`policy::TieredPolicy`] so in-flight `ResidentTicket`s remain coherent
/// with the running budget after the operator tunes any of the inputs.
///
/// When `enabled` is true, installs a [`ColumnPager`] backed by the
/// singleton policy. When false, installs [`ColumnPager::disabled`] —
/// in-flight tickets still credit the singleton, which is harmless: the
/// budget grows above the configured total until the next enable reconciles
/// it via `reconfigure`.
pub fn apply_tiered_config(
    enabled: bool,
    total_budget: usize,
    backend: Backend,
    codec: Option<Codec>,
) {
    let p = TIERED_POLICY
        .get_or_init(|| Arc::new(policy::TieredPolicy::new(total_budget, backend, codec)));
    p.reconfigure(total_budget, backend, codec);
    if enabled {
        #[allow(clippy::clone_on_ref_ptr)]
        let dyn_policy: Arc<dyn PagingPolicy> = p.clone();
        set_global_pager(ColumnPager::new(dyn_policy));
    } else {
        set_global_pager(ColumnPager::disabled());
    }
}

/// Process-wide decision counters. Diagnostic only — log a summary every
/// `DECISION_LOG_INTERVAL` Page decisions so we can tell whether the
/// pager is actually engaging without per-call log spam.
static SKIP_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static PAGE_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static SKIP_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static PAGE_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
const DECISION_LOG_INTERVAL: usize = 1024;

fn record_decision(paged: bool, bytes: usize) {
    use std::sync::atomic::Ordering;
    if paged {
        let n = PAGE_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        PAGE_BYTES.fetch_add(bytes, Ordering::Relaxed);
        if n.is_multiple_of(DECISION_LOG_INTERVAL) {
            let s = SKIP_COUNT.load(Ordering::Relaxed);
            let sb = SKIP_BYTES.load(Ordering::Relaxed);
            let pb = PAGE_BYTES.load(Ordering::Relaxed);
            tracing::info!(
                skip_calls = s,
                skip_bytes = sb,
                page_calls = n,
                page_bytes = pb,
                "column-pager: decision rate"
            );
        }
    } else {
        SKIP_COUNT.fetch_add(1, Ordering::Relaxed);
        SKIP_BYTES.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Returns the current global pager. Cheap: clones the inner `Arc<dyn
/// PagingPolicy>`.
pub fn global_pager() -> ColumnPager {
    GLOBAL_PAGER.read().expect("global pager poisoned").clone()
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
                record_decision(false, len_bytes);
                let ticket = ResidentTicket {
                    bytes: len_bytes,
                    policy: Arc::clone(&self.policy),
                };
                return PagedColumn::Resident(std::mem::take(col), ticket);
            }
            PageDecision::Page { backend, codec } => {
                record_decision(true, len_bytes);
                (backend, codec)
            }
        };
        let meta = Meta { len_bytes };

        match codec {
            None => {
                // Raw path: the body must end up as u64-aligned bytes for the
                // pager. `Column::Align` already is; other variants are
                // serialized and copied.
                debug_assert_eq!(len_bytes % 8, 0);
                let body: Vec<u64> = match std::mem::take(col) {
                    // Move the aligned buffer straight into the pager: the
                    // allocation transfers with no copy. `take` already left
                    // `col` as a refill-ready `Typed` default.
                    Column::Align(v) => v,
                    mut other => {
                        let mut buf = Vec::with_capacity(len_bytes);
                        other.into_bytes(&mut buf);
                        debug_assert_eq!(buf.len() % 8, 0);
                        // `into_bytes` only borrowed `other`; clear it in place
                        // and hand it back so the caller keeps the `Typed`
                        // allocation instead of us dropping a reusable buffer.
                        other.clear();
                        *col = other;
                        bytemuck::allocation::pod_collect_to_vec::<u8, u64>(&buf)
                    }
                };
                let handle = pager::pageout_with(backend, &mut [body]);
                self.policy.record(PageEvent::PagedOut {
                    bytes_in: len_bytes,
                    bytes_out: handle.len_bytes(),
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
                self.policy.record(PageEvent::PagedOut {
                    bytes_in: len_bytes,
                    bytes_out: out.len(),
                    backend,
                    codec: Some(Codec::Lz4),
                });
                let inner = match backend {
                    Backend::Swap => CompressedInner::Memory(out),
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
                self.policy.record(PageEvent::PagedIn {
                    bytes: decoded.len(),
                });
                // `BytesMut::from` wraps the `Vec<u8>` without copying; `freeze`
                // produces the refcounted `Bytes` that `ContainerBytes` expects.
                Column::from_bytes(BytesMut::from(decoded).freeze())
            }
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
            self.decision
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
}
