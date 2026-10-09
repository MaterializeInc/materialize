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

//! The Linux syscalls behind the file store.

use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cast::CastFrom;

fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))
}

/// Names the unlinked fallback file tries before `EEXIST` fails the
/// open.
const OPEN_NAMED_ATTEMPTS: usize = 8;

fn off_t(offset: u64) -> io::Result<libc::off_t> {
    libc::off_t::try_from(offset).map_err(|_| io::Error::from_raw_os_error(libc::EFBIG))
}

/// Whether `path` is on tmpfs or ramfs.
///
/// NOTE: this checks the filesystem type of `path` alone. An overlay
/// whose upper layer is tmpfs reports the overlay type and passes.
pub(in crate::pool::file) fn is_memory_backed(path: &Path) -> io::Result<bool> {
    let path = c_path(path)?;
    let mut st = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `st` is valid for writes of
    // one `statfs`.
    if unsafe { libc::statfs(path.as_ptr(), st.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `statfs` succeeded, so it initialized `st`.
    let f_type = unsafe { st.assume_init() }.f_type;
    // From `linux/magic.h`. `f_type`'s width and signedness differ
    // across targets, and `libc` lacks `RAMFS_MAGIC`.
    const TMPFS_MAGIC: u32 = 0x0102_1994;
    const RAMFS_MAGIC: u32 = 0x8584_58f6;
    let f_type = i128::from(f_type);
    Ok(f_type == i128::from(TMPFS_MAGIC) || f_type == i128::from(RAMFS_MAGIC))
}

/// Bytes available to unprivileged writers on the volume holding `path`.
pub(in crate::pool::file) fn available_bytes(path: &Path) -> io::Result<u64> {
    let path = c_path(path)?;
    let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `st` is valid for writes of
    // one `statvfs`.
    if unsafe { libc::statvfs(path.as_ptr(), st.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `statvfs` succeeded, so it initialized `st`.
    let st = unsafe { st.assume_init() };
    // The field widths differ across targets.
    #[allow(clippy::useless_conversion)]
    let (bavail, frsize) = (u64::from(st.f_bavail), u64::from(st.f_frsize));
    Ok(bavail.saturating_mul(frsize))
}

/// Opens a nameless read-write file in `dir`, close-on-exec, with
/// `O_DIRECT` if `direct`.
pub(in crate::pool::file) fn open_anonymous(
    dir: &Path,
    class: usize,
    direct: bool,
) -> io::Result<File> {
    let direct = if direct { libc::O_DIRECT } else { 0 };
    // `std` opens every file `O_CLOEXEC`.
    let tmpfile = match tmpfile_fault() {
        Some(err) => Err(err),
        None => OpenOptions::new()
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_TMPFILE | direct)
            .open(dir),
    };
    match tmpfile {
        Err(err) if matches!(err.raw_os_error(), Some(libc::EOPNOTSUPP | libc::EISDIR)) => {
            // The filesystem lacks `O_TMPFILE`: create a unique name and
            // unlink it at once, leaving the same lifetime as an
            // anonymous file. A crash between the two leaves the name
            // behind, and a restarted container can reuse the pid, so
            // the name carries the clock too and a collision retries.
            static NONCE: AtomicU64 = AtomicU64::new(0);
            let mut attempts = 0;
            let (file, path) = loop {
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos());
                let path = dir.join(format!(
                    ".mz-pool-extents-{}-{nanos}-{class}-{}",
                    std::process::id(),
                    NONCE.fetch_add(1, Ordering::Relaxed),
                ));
                match OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(direct)
                    .open(&path)
                {
                    Ok(file) => break (file, path),
                    Err(err)
                        if err.kind() == io::ErrorKind::AlreadyExists
                            && attempts + 1 < OPEN_NAMED_ATTEMPTS =>
                    {
                        attempts += 1;
                    }
                    Err(err) => return Err(err),
                }
            };
            std::fs::remove_file(&path)?;
            Ok(file)
        }
        result => result,
    }
}

/// The injected failure of the next `O_TMPFILE` open, if any.
fn tmpfile_fault() -> Option<io::Error> {
    #[cfg(test)]
    if let Some(result) =
        crate::pool::file::fault::inject(crate::pool::file::fault::Op::OpenTmpfile)
    {
        return Some(result.expect_err("an OpenTmpfile fault carries an errno"));
    }
    None
}

/// Allocates blocks for `[offset, offset + len)`.
pub(in crate::pool::file) fn fallocate(file: &File, offset: u64, len: usize) -> io::Result<()> {
    #[cfg(test)]
    if let Some(result) = crate::pool::file::fault::inject(crate::pool::file::fault::Op::Fallocate)
    {
        return result.map(|_| ());
    }
    fallocate_mode(file, 0, offset, len)
}

/// Deallocates the blocks of `[offset, offset + len)`, keeping the file
/// size.
pub(in crate::pool::file) fn punch_hole(file: &File, offset: u64, len: usize) -> io::Result<()> {
    #[cfg(test)]
    if let Some(result) = crate::pool::file::fault::inject(crate::pool::file::fault::Op::Punch) {
        return result.map(|_| ());
    }
    fallocate_mode(
        file,
        libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
        offset,
        len,
    )
}

fn fallocate_mode(file: &File, mode: libc::c_int, offset: u64, len: usize) -> io::Result<()> {
    let offset = off_t(offset)?;
    let len = off_t(u64::cast_from(len))?;
    loop {
        // SAFETY: plain syscall on an owned descriptor, no memory
        // arguments.
        if unsafe { libc::fallocate(file.as_raw_fd(), mode, offset, len) } == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// One `pwrite` of `buf` at `offset`.
pub(in crate::pool::file) fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    #[cfg(test)]
    if let Some(result) = crate::pool::file::fault::inject(crate::pool::file::fault::Op::Write) {
        return result;
    }
    file.write_at(buf, offset)
}

/// One `pread` into `buf` at `offset`.
pub(in crate::pool::file) fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(test)]
    if let Some(result) = crate::pool::file::fault::inject(crate::pool::file::fault::Op::Read) {
        return result;
    }
    file.read_at(buf, offset)
}

/// Writes back `[offset, offset + len)` synchronously and drops it from
/// the page cache.
pub(in crate::pool::file) fn writeback_and_drop(
    file: &File,
    offset: u64,
    len: usize,
) -> io::Result<()> {
    let range_offset = off_t(offset)?;
    let range_len = off_t(u64::cast_from(len))?;
    // SAFETY: plain syscall on an owned descriptor, no memory arguments.
    let synced = unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            range_offset,
            range_len,
            libc::SYNC_FILE_RANGE_WAIT_BEFORE
                | libc::SYNC_FILE_RANGE_WRITE
                | libc::SYNC_FILE_RANGE_WAIT_AFTER,
        )
    };
    if synced != 0 {
        return Err(io::Error::last_os_error());
    }
    drop_cache(file, offset, len)
}

/// Drops the clean pages of `[offset, offset + len)` from the page cache.
pub(in crate::pool::file) fn drop_cache(file: &File, offset: u64, len: usize) -> io::Result<()> {
    let offset = off_t(offset)?;
    let len = off_t(u64::cast_from(len))?;
    // SAFETY: plain syscall on an owned descriptor, no memory arguments.
    let errno =
        unsafe { libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED) };
    match errno {
        0 => Ok(()),
        errno => Err(io::Error::from_raw_os_error(errno)),
    }
}
