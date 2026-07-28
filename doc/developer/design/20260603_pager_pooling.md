# Pager file + buffer pooling

* Associated: [CLU-65](https://linear.app/materializeinc/issue/CLU-65/pager) (follow-up to the original pager design, `20260504_pager.md`).

## The problem

The original pager design parked cross-handle file pooling as out of scope:

> Cross-handle file pooling (one shared scratch file with a free-list). Each
> handle owns one named file in the scratch directory; pooling is a follow-up
> if inode pressure shows up.

Inode pressure shows up. A 15-minute on-CPU profile of a production `u6`
clusterd dominated by columnar merge-batcher work puts
`mz_ore::pager::file::try_take_file` second across the entire process at
**1m17.8s cumulative**. The cost splits four ways:

| Cost | Time | Share | What it is |
|---|---|---|---|
| `FileInner::drop` → `remove_file` (unlink) | 35.6s | 46% | Deleting the scratch file after read |
| `read_at` (pread + `copy_to_user`) | 22.8s | 29% | The read itself |
| `Vec<u64>::resize` (zero-fill) | 13.4s | 17% | Zeroing `dst` before reading into it |
| `File::open` | 4.3s | 5% | Path walk + open syscall |

Two findings drive this design:

1. **Unlink is the dominant cost, and it is not the directory operation.** Of
   the 35.6s, ~24.8s is `iput_final → evict → ext4_evict_inode`, and ~18.4s of
   that is `truncate_inode_pages_final` — the kernel synchronously tearing down
   every page-cache page that the immediately-preceding read populated, on the
   worker thread, in the hot loop. For a write-once / read-once / delete temp
   file the entire ext4 inode lifecycle is pure overhead: we create an inode,
   populate its page cache, then destroy both, per spill.

2. **An open-fd cache would not help the hot path.** The merge batcher drains
   chains via `FetchIter::next`, which calls `ColumnPager::take` exactly once
   per entry: open, read fully, delete. Each scratch file is touched by exactly
   one open and one read in its lifetime. There is no second access to amortize
   an open against, so retaining file descriptors keyed by handle buys nothing
   here and reintroduces the fd-exhaustion risk the original design eliminated.
   `read_at_many` (the write-once / read-many random-access API) is the only
   shape an fd cache would help, and it is absent from this workload.

The redundant zero-fill (row 3) is mostly irreducible in isolation: skipping
`dst.resize(.., 0)` only saves the ~2.7s userspace memset. The
`clear_page`/`alloc_anon_folio` faulting (~10s) is the kernel zeroing fresh
anonymous pages on first write-fault, which `read_at`'s `copy_to_user` triggers
regardless of whether we pre-zeroed. That cost is recoverable only by keeping
the destination memory warm across takes (buffer reuse), not by reordering the
zero.

## Success criteria

* The common merge-batcher churn (pageout and take interleaved within a merge)
  stops paying inode create + unlink + page-cache evict per spill.
* Peak open file descriptors stay bounded by a small constant, independent of
  how many handles are simultaneously paged out. The original design's core
  invariant — fd count decoupled from handle count — is preserved.
* The destination-buffer faulting cost is addressed for the steady state
  without holding unbounded resident memory off the pager's budget.
* No change to existing `mz_ore::pager` signatures or the
  handle-survives-backend-flip contract. The buffer-warmth change adds one
  accessor — `Handle::is_file` — so the `ColumnPager` layer can route a warm
  buffer only for file-backed takes (the swap backend keeps its allocation
  resident, so a warm buffer there is wasted). The routing lives in the caller
  rather than inside the pager because a pager-internal pool can't see the
  caller's recycling chokepoint — the merge batcher's per-chunk reuse — where
  the warm buffer has to come from and return to.
* Crash-safety story is unchanged: stale scratch is reclaimed with the pod's
  ephemeral volume; pooling adds no new persistent state.

## Out of scope

* Async / `io_uring`. Still sync syscalls.
* The swap backend. Pooling is a file-backend concern only.
* Compression interaction. The pool stores opaque `Vec<u64>` payloads exactly
  as today; the `ColumnPager` codec layer is unaffected.

## Solution proposal

Two independent, separately-shippable changes. Tier 1 attacks the 35.6s unlink;
Tier 2 attacks the 4.3s open; the buffer story attacks the ~13s faulting. They
compose but do not depend on each other.

### Tier 1 — inode free-list (reuse files instead of unlinking)

Today `FileInner::drop` calls `remove_file`, which unlinks the dirent and, as
the last link drops, evicts the inode and truncates its page cache. Instead,
maintain a per-process free-list of **scratch slots** — named files that exist
on disk but back no live handle.

* `take` / drop, instead of `remove_file`, returns the slot id to the
  free-list. The file stays linked; its inode and (warm) page cache survive.
* `pageout` pops a free slot if one is available and writes the new payload at
  offset 0. Reads are already bounded by `len_u64s`, so stale bytes past the
  new length are never observed — no `ftruncate` needed.
* The free-list is capped (à la the merge batcher's `STASH_CAP` /
  `MAX_RECYCLE_BYTES`). Beyond the cap, or for payloads larger than a recycle
  threshold, fall back to today's create-new / unlink-on-drop path so we don't
  hoard inodes or pin oversized files.

This preserves the no-fd-retained invariant exactly: a slot on the free-list
holds no descriptor, and a slot backing a live handle holds no descriptor. It
removes the unlink, the inode eviction, and the synchronous page-cache
truncate — the entire 35.6s — for every spill that lands a recycled slot.
Because pageout and take interleave during merges (take frees a slot, the next
pageout immediately reuses it), steady-state reuse is high with a tiny cap.

Reuse count being bounded by interleaving (not by the cap alone) is the same
dynamic that makes `STASH_CAP = 2` sufficient for chunk recycling: during a
phase that pages out N chunks before taking any, the free-list drains and the
overflow falls back to fresh files; that long-tail keeps today's behavior.

### Tier 2 — open-fd cache keyed by slot id

Tier 1 still pays `File::open` on both pageout and take (4.3s on take in the
profile; roughly the same again on pageout, so an ~8s ceiling). The fd cannot
live in the handle: the spilled working set is thousands of cold chunks, each a
live handle, and the no-fd-retained invariant exists precisely so fd count does
not track handle count. The fd also cannot live only in the free-list slot,
because it is needed at pageout *and* at the later take, with the cold in-use
period in between — during which the slot is off the free-list.

The reuse unit is therefore the recycled physical *slot id*, not the logical
handle. Tier 1's free-list is LIFO, so the hot churn cycles through a small set
of ids at the top of the stack while the large cold set holds ids that are off
the free-list entirely. That gives strong temporal locality on a handful of
ids, which a bounded LRU **fd cache keyed by slot id** turns into hits. The
cache is a pure `open()` memoization over "the file at `scratch_path(id)`
exists"; the handle stays `(id, len)`.

* **pageout**: cache lookup the popped id; hit → pwrite via the cached fd, miss
  → `open(O_RDWR | O_CREAT)`, insert (evict + close LRU if full), pwrite. Do not
  close — leave the fd cached.
* **take / read**: cache lookup; hit → pread (the open we are removing), miss →
  open, insert, pread.
* **recycle**: push the id to the free-list as in Tier 1; the fd stays cached.

It self-regulates: a short-residency chunk (the merge frontier — paged out then
taken almost immediately) keeps its fd across pageout→take, so both sides are
open-free; a long-residency cold chunk's fd is evicted and closed by `N`
intervening slots, so fds are never pinned for data that is actually cold. Open
fds stay at `min(N, distinct slots in the recent window)`, bounded by `N`
regardless of handle count.

Correctness falls out cleanly: `O_RDWR` fds serve both pwrite and pread; recycle
overwrites the *same inode* in place (pwrite at offset 0), so a cached fd never
goes stale; fresh ids come from a monotonic counter and unlinked ids never
reissue, so there is no inode-reuse-under-stale-fd hazard. The single
invalidation rule: when Tier 1 unlinks a slot (oversize, cap overflow, or the
write-error path), `remove` + close its cached fd first, or the open fd pins the
inode and defeats the unlink. The cache incidentally also speeds the
write-once / read-many `read_at_many` path, which re-opens per call today.

**Concurrency (decided): process-global free-list, and what that implies for
the fd cache.** Tier 1's free-list is a single process-global
`Mutex<Vec<FileId>>`, not thread-local. `Handle` is `Send`, so a handle paged
out by one worker can drop on another; a thread-local free-list would strand
that slot on the dropping thread — away from the worker whose pageouts could
reuse it — and the recycling would drift. A process-global list lets any thread
recycle any slot. Contention is not a concern here: spills amortize to roughly
one file operation per 2 MiB shipped, so the lock is taken far too rarely to
register against the I/O it guards.

This constrains the Tier 2 fd cache. A thread-local fd cache misses by
construction whenever a slot recycled on thread A is popped on thread B — the
cached fd lives on A — so an fd cache must either be process-global (one lock,
like the free-list) or accept those cross-thread misses. The work-stealing
layout lgalloc uses — small thread-local pools that refill from a global pool
once drained, keeping the local pool small while bounding the worst case — is
the alternative to reach for if and only if the global lock ever measurably
contends. It is not justified up front; decide it on a profile, not a priori.

Tier 2 is the smaller remaining lever (~8s) than buffer warmth (~13s) and adds
eviction + invalidation complexity, so it sequences after buffer warmth.

### Buffer warmth (orthogonal, `ColumnPager` layer)

`ColumnPager::take` did `Vec::with_capacity(handle.len())` per call, so every
file-backed take read into cold, freshly-faulted memory — the ~13s under
`Vec::resize` in the profile, of which ~10.7s is the kernel faulting in and
zeroing cold anonymous pages (`alloc_anon_folio`/`clear_page`) on the read's
`copy_to_user`, and ~2.7s the redundant userspace memset. Reusing a buffer
whose pages are already resident removes the faulting; the memset remains
(removing it needs an uninitialized read, not worth an `unsafe` for ~2.7s).

The existing chunk-recycling stash is the *wrong* pool: `recycle_chunk` only
keeps `Column::Typed` and deliberately drops `Align`/`Bytes`, and `take`
produces `Column::Align`. So buffer warmth gets its own pool: a per-worker
(thread-local) free-list of warm `Vec<u64>` read buffers. File-backed take pulls
one (gated on `Handle::is_file` so the swap backend, which fills from resident
memory, is untouched); consumed `Align` chunks return their backing through the
merge batcher's single `recycle_capped` chokepoint, which already sees every
drained merge head. Capture is therefore the merge-internal head churn — shipped
chunks flow on to the spine builder and are not pooled. The pool is bounded like
the chunk stash (cap 4, ≤ 4 MiB each); its bytes are resident but not charged to
the pager's `ResidentTicket` budget. Thread-local placement means no lock and
per-worker locality, matching the one-batcher-per-worker structure.

## Correctness and operational notes

* **Crash cleanup is unchanged.** Free-list slots are ordinary named files in
  the per-process scratch subdir, reclaimed with the ephemeral volume exactly
  like today's per-handle files. No new persistent state, no orphan-tracking.
* **Concurrency.** The free-list is process-global state behind the same
  `SUBDIR`/atomic machinery the backend already uses, or sharded per worker to
  avoid contention. Per-worker sharding matches the one-batcher-per-arrange-
  per-worker structure and avoids a lock on the hot path; the cap is then
  per-shard.
* **Stale data safety.** Reuse writes at offset 0 and reads are length-bounded,
  so a recycled slot never leaks a previous handle's bytes. A debug assertion
  can confirm `len_u64s` never exceeds the bytes written.
* **No API change.** `Handle` stays `(scratch_id, len)`; the id now names a
  poolable slot rather than a single-use file. `pageout`/`take`/`read_at`
  signatures are untouched.

## Alternatives considered

* **`O_TMPFILE`.** Unnamed temp files remove the dirent and the unlink syscall
  and auto-reclaim on close. But they force fd retention for the file's whole
  lifetime (no path to reopen), so fds track live-handle count — the exact
  pressure we avoid — and they still pay the inode evict on close. Weaker than
  the free-list on the dominant cost.
* **`posix_fadvise(DONTNEED)` / `O_DIRECT`.** Drops or bypasses the page cache
  to cut the evict, but does not remove the inode lifecycle and complicates the
  read path. The free-list removes the evict by never destroying the inode.
* **Open-fd LRU keyed by handle.** Addresses only the 4.3s open and only for
  repeated reads of one handle, which the merge workload does not do. Tier 2
  captures the open saving without per-handle fd accounting.
