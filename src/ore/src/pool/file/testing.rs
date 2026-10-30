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

//! Test-only helpers of the file store: the temporary-directory picker, the
//! syscall fault seam, and store accessors.

// The file-mode and file-store tests are Linux-only, so their helpers are
// unused in non-Linux test builds.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use super::*;

/// A temporary directory on a disk-backed filesystem, for tests: the store
/// rejects tmpfs, which is a common `/tmp`. Uses `$MZ_POOL_TEST_DIR` if set,
/// else the system temporary directory if it is disk-backed, else
/// `pool-test-tmp` in the build profile directory holding the test binary.
pub(crate) fn disk_tempdir() -> tempfile::TempDir {
    if let Some(dir) = std::env::var_os("MZ_POOL_TEST_DIR") {
        return tempfile::tempdir_in(dir).expect("tempdir in $MZ_POOL_TEST_DIR");
    }
    let tmp = std::env::temp_dir();
    if !sys::is_memory_backed(&tmp).unwrap_or(true) {
        return tempfile::tempdir_in(tmp).expect("tempdir");
    }
    // Cargo places test binaries in `<target>/<profile>/deps`, honoring
    // `CARGO_TARGET_DIR`.
    let exe = std::env::current_exe().expect("test binary path");
    let profile = exe
        .ancestors()
        .find(|dir| dir.file_name().is_some_and(|name| name == "deps"))
        .and_then(Path::parent)
        .expect("test binary runs from a cargo deps directory");
    let dir = profile.join("pool-test-tmp");
    std::fs::create_dir_all(&dir).expect("create pool-test-tmp");
    tempfile::tempdir_in(dir).expect("tempdir in the target directory")
}

/// Test seam over the store's syscalls. The queue is thread-local, so a
/// test injects faults only into the I/O it drives on its own thread.
pub(crate) mod fault {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::io;

    /// A syscall the seam can fail.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Op {
        /// `fallocate` growing a slot, not a hole punch.
        Fallocate,
        Write,
        Read,
        /// The `O_TMPFILE` open of a class file. Its fault needs a nonzero
        /// errno.
        OpenTmpfile,
        /// A hole punch. Its fault needs a nonzero errno.
        Punch,
    }

    thread_local! {
        static QUEUE: RefCell<VecDeque<(Op, i32)>> = const { RefCell::new(VecDeque::new()) };
    }

    /// Makes the next `op` on this thread fail with `errno`. Errno 0 makes
    /// it succeed with 0 bytes transferred.
    pub(crate) fn fail_next(op: Op, errno: i32) {
        QUEUE.with(|q| q.borrow_mut().push_back((op, errno)));
    }

    /// Drops every fault queued on this thread.
    pub(crate) fn clear() {
        QUEUE.with(|q| q.borrow_mut().clear());
    }

    /// Consumes the oldest fault queued for `op`, as the syscall's result.
    pub(in crate::pool::file) fn inject(op: Op) -> Option<io::Result<usize>> {
        QUEUE.with(|q| {
            let mut q = q.borrow_mut();
            let pos = q.iter().position(|&(o, _)| o == op)?;
            let (_, errno) = q.remove(pos).expect("position is in range");
            Some(match errno {
                0 => Ok(0),
                errno => Err(io::Error::from_raw_os_error(errno)),
            })
        })
    }
}

impl FileStore {
    pub(crate) fn allocated_bytes(&self) -> u64 {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Test hook: the number of slots allocated across classes and not
    /// free.
    pub(crate) fn slots_in_use(&self) -> usize {
        self.classes.iter().map(|c| c.slots().in_use()).sum()
    }

    /// Overwrites the slot's first page with `0xFF`, bypassing the fault
    /// seam.
    pub(crate) fn corrupt(&self, slot: FileSlot) {
        use std::os::unix::fs::FileExt;
        let garbage = AlignedBuf::from_bytes(&vec![0xFF; self.page]);
        self.classes[slot.class]
            .file
            .write_all_at(garbage.as_slice(), self.offset(slot))
            .expect("corrupt slot");
    }
}
