---
source: src/ore/src/pager.rs
revision: 68b35c5959
---

# mz-ore::pager

Explicit pager for cold data. Moves `Vec<u64>` chunks out of active memory into one of two backends — `Swap` or `File` — and provides scatter-read and whole-take operations to bring the data back.

## Key types

* `Handle` — opaque handle to paged-out data. The backend variant is fixed at `pageout` time; changing the global backend after the fact does not affect existing handles.
* `Backend` — selects the storage backend: `Swap` (keeps allocations resident and hints `MADV_COLD` to the kernel) or `File` (writes to a named scratch file with no retained file descriptor).

## Functions

* `pageout(chunks)` / `pageout_with(backend, chunks)` — scatter pageout: empties each `Vec` in `chunks` and returns a `Handle`. The file backend preserves `Vec` capacity; the swap backend moves the allocation into the handle.
* `try_pageout` / `try_pageout_with` — fallible counterparts that return `io::Result<Handle>` instead of panicking on file I/O errors. The swap backend cannot fail at I/O so these are equivalent to `Ok(pageout(...))` when the backend is `Swap`.
* `read_at_many(handle, ranges, dst)` — scatter read: appends the requested `(offset, len)` ranges (in `u64` units) from the handle into `dst` in request order. Ranges must be pairwise non-overlapping.
* `read_at(handle, offset, len, dst)` — single-range convenience wrapper around `read_at_many`.
* `try_read_at_many` / `try_read_at` — fallible counterparts; bounds violations still panic.
* `take(handle, dst)` — consumes the handle, writes the entire payload into `dst` (cleared first), then reclaims storage. The swap backend has a zero-copy fast path for single-chunk handles into an empty `dst`.
* `try_take` — fallible counterpart; on `Err`, `dst` may hold partial data.
* `backend()` / `set_backend(b)` — read or set the process-global active backend. The selection is stored in an `AtomicU8`.
* `set_scratch_dir(root)` (re-exported from `file`) — configure the scratch directory for the file backend before using `Backend::File`.

## Submodules

* `file` — file backend: scratch-file creation, `pread`-based scatter reads, and drop-time unlink.
* `swap` — swap backend: in-memory chunk storage with `MADV_COLD` hints and prefix-sum range indexing.
