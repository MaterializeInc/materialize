---
source: src/ore/src/pager/file.rs
revision: 6b2261cbfe
---

# mz-ore::pager::file

File backend for `mz_ore::pager`. Stores paged-out data in named scratch files under a process-specific subdirectory, using vectored I/O (`write_vectored`) for writes and `pread` for reads.

## Configuration

`set_scratch_dir(root)` configures the scratch root directory. It is idempotent across multiple calls with the same path; a different path on a subsequent call is logged and ignored. If the first call fails to create the process subdirectory (named `mz-pager-{pid}-{nonce:016x}`), the failure is logged and the directory is left unset so the next call can retry.

## FileInner

`FileInner` (crate-internal) stores the scratch file identifier (`id: u64`) and logical length in `u64`s (`len_u64s`). The scratch file path is `{subdir}/{id}.bin`. On drop, `FileInner` unlinks the scratch file; empty handles (`len_u64s == 0`) skip unlink since no file was created.

## Operations

* `pageout_file` / `try_pageout_file` — writes all non-empty chunks to a new scratch file via `write_vectored`, then clears each chunk (preserving capacity). Empty input produces a file-variant handle without creating any file on disk.
* `read_at_file` / `try_read_at_file` — opens the scratch file and performs `pread` calls for each requested `(offset, len)` range. Adjacent ranges are coalesced into a single `pread` before reading. Bounds violations panic; I/O errors return `Err` in the fallible variant.
* `take_file` / `try_take_file` — reads the entire payload into `dst` (cleared first) then drops `FileInner`, which unlinks the scratch file. On `Err`, `dst` may hold partial data and the scratch file is unlinked on `FileInner` drop.
