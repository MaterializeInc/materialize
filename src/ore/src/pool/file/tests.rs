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

use super::*;

fn store(capacity: u64) -> (tempfile::TempDir, FileStore) {
    let dir = disk_tempdir();
    let store = FileStore::open(dir.path(), Some(capacity), &classes()).expect("open");
    (dir, store)
}

/// A page-aligned copy of `bytes`, zero-padded to a page multiple as
/// `FileStore::write` requires.
fn aligned(bytes: &[u8]) -> AlignedBuf {
    let mut padded = bytes.to_vec();
    padded.resize(
        bytes
            .len()
            .next_multiple_of(crate::pool::region::page_size()),
        0,
    );
    AlignedBuf::from_bytes(&padded)
}

fn pattern(len: usize, seed: u32) -> Vec<u8> {
    (0..u32::try_from(len).expect("fits"))
        .map(|i| u8::try_from((i + seed) % 251).expect("fits"))
        .collect()
}

fn write_bytes(s: &FileStore, slot: FileSlot, data: &[u8]) -> Result<usize, WriteError> {
    let buf = aligned(data);
    s.write(slot, buf.as_slice(), data.len())
}

fn assert_reads_back(s: &FileStore, slot: FileSlot, data: &[u8]) {
    let mut out = AlignedBuf::new();
    s.read(slot, data.len(), crc(data), &mut out);
    assert_eq!(out.as_slice(), data);
}

/// The ladder tests open stores with, the one the pool passes.
fn classes() -> Vec<usize> {
    crate::pool::extent::extent_classes(crate::pool::region::page_size())
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn round_trip() {
    let (_dir, s) = store(64 << 20);
    let data: Vec<u8> = (0..300_000u32)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    let class = s.class_for(data.len()).unwrap();
    let slot = s.alloc(class).unwrap();
    let buf = aligned(&data);
    let rounded = data
        .len()
        .next_multiple_of(crate::pool::region::page_size());
    let written = s.write(slot, buf.as_slice(), data.len()).unwrap();
    assert_eq!(written, rounded, "writes transfer page-rounded bytes");
    let mut out = AlignedBuf::new();
    let read = s.read(slot, data.len(), crc(&data), &mut out);
    assert_eq!(read, rounded, "reads transfer page-rounded bytes");
    assert_eq!(out.as_slice(), &data[..]);
    s.free(slot);
    let stats = s.stats();
    assert_eq!(stats.writes, 1);
    assert_eq!(stats.reads, 1);
    assert_eq!(stats.read_bytes, u64::cast_from(read));
    assert_eq!(stats.allocated_bytes, s.allocated_bytes());
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn freed_slots_are_reused_without_new_allocation() {
    let (_dir, s) = store(64 << 20);
    let class = s.class_for(10_000).unwrap();
    let a = s.alloc(class).unwrap();
    let allocated = s.allocated_bytes();
    assert_eq!(allocated, u64::cast_from(s.class_size(class)));
    s.free(a);
    let b = s.alloc(class).unwrap();
    assert_eq!(a, b, "a warm free slot is reused first");
    assert_eq!(
        s.allocated_bytes(),
        allocated,
        "warm reuse allocates nothing"
    );
    let data = pattern(10_000, 7);
    write_bytes(&s, b, &data).unwrap();
    assert_reads_back(&s, b, &data);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn capacity_limits_allocation() {
    let classes = classes();
    let class = classes
        .iter()
        .position(|&c| c >= 300_000)
        .expect("a class fits");
    let class_size = u64::cast_from(classes[class]);
    let (_dir, s) = store(2 * class_size);
    assert_eq!(s.capacity_bytes(), 2 * class_size);
    let _a = s.alloc(class).unwrap();
    assert!(s.can_alloc(class));
    let _b = s.alloc(class).unwrap();
    assert!(!s.can_alloc(class), "no warm slot and no capacity room");
    assert_eq!(s.alloc(class), Err(AllocError::Full));
    assert_eq!(s.allocated_bytes(), 2 * class_size);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn full_store_punches_other_class_on_demand() {
    let classes = classes();
    let big = classes.len() - 1;
    let small = 0;
    let (_dir, s) = store(u64::cast_from(classes[big]));
    let slot = s.alloc(big).unwrap();
    s.free(slot);
    assert_eq!(
        s.allocated_bytes(),
        s.capacity_bytes(),
        "a warm slot keeps its blocks"
    );
    let small_slot = s.alloc(small).expect("punching the big slot makes room");
    assert_eq!(s.allocated_bytes(), u64::cast_from(s.class_size(small)));
    assert_eq!(
        s.stats().holes_punched_bytes,
        u64::cast_from(s.class_size(big))
    );
    let data = pattern(100, 3);
    write_bytes(&s, small_slot, &data).unwrap();
    assert_reads_back(&s, small_slot, &data);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn enospc_lowers_capacity() {
    let (_dir, s) = store(64 << 20);
    let warm_class = s.class_for(10_000).unwrap();
    let other_class = s.class_for(300_000).unwrap();
    assert_ne!(warm_class, other_class);
    let warm = s.alloc(warm_class).unwrap();
    s.free(warm);

    fault::fail_next(fault::Op::Fallocate, libc::ENOSPC);
    assert_eq!(s.alloc(other_class), Err(AllocError::Full));
    assert_eq!(s.capacity_bytes(), s.allocated_bytes());
    assert_eq!(
        s.allocated_bytes(),
        u64::cast_from(s.class_size(warm_class))
    );

    // The lowered capacity is permanent, and a warm slot still serves.
    assert_eq!(s.alloc(other_class), Err(AllocError::Full));
    let reused = s.alloc(warm_class).unwrap();
    assert_eq!(reused, warm);
    let data = pattern(10_000, 11);
    write_bytes(&s, reused, &data).unwrap();
    assert_reads_back(&s, reused, &data);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn write_enospc_returns_slot_and_lowers_capacity() {
    let (_dir, s) = store(64 << 20);
    let data = pattern(50_000, 7);
    let class = s.class_for(data.len()).unwrap();
    let good = s.alloc(class).unwrap();
    write_bytes(&s, good, &data).unwrap();
    let bad = s.alloc(class).unwrap();
    let size = u64::cast_from(s.class_size(class));
    assert_eq!(s.allocated_bytes(), 2 * size);

    fault::fail_next(fault::Op::Write, libc::ENOSPC);
    assert!(matches!(write_bytes(&s, bad, &data), Err(WriteError::Full)));
    assert!(!s.writes_disabled(), "ENOSPC is a capacity condition");
    let stats = s.stats();
    assert_eq!(stats.write_errors, 0);
    assert_eq!(stats.holes_punched_bytes, size, "the slot was punched");
    assert_eq!(s.allocated_bytes(), size, "the slot was taken back cold");
    assert_eq!(s.capacity_bytes(), size, "capacity latched");
    assert_eq!(s.classes[class].slots().in_use(), 1);
    assert_eq!(s.alloc(class), Err(AllocError::Full));
    assert_reads_back(&s, good, &data);

    // A freed slot keeps serving under the lowered capacity.
    s.free(good);
    let reused = s.alloc(class).unwrap();
    assert_eq!(reused, good);
    write_bytes(&s, reused, &data).unwrap();
    assert_reads_back(&s, reused, &data);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn short_direct_write_retries_whole_buffer() {
    let (_dir, s) = store(64 << 20);
    if s.io_mode() != IoMode::Direct {
        // Buffered writes resume a short write instead.
        return;
    }
    let data = pattern(50_000, 3);
    let slot = s.alloc(s.class_for(data.len()).unwrap()).unwrap();
    // Errno 0 makes the seam transfer 0 bytes, a short write.
    fault::fail_next(fault::Op::Write, 0);
    write_bytes(&s, slot, &data).expect("the retry writes the whole buffer");
    assert_reads_back(&s, slot, &data);
    assert_eq!(s.stats().write_errors, 0);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn short_direct_write_then_enospc_is_full() {
    let (_dir, s) = store(64 << 20);
    if s.io_mode() != IoMode::Direct {
        return;
    }
    let data = pattern(50_000, 4);
    let class = s.class_for(data.len()).unwrap();
    let slot = s.alloc(class).unwrap();
    fault::fail_next(fault::Op::Write, 0);
    fault::fail_next(fault::Op::Write, libc::ENOSPC);
    assert!(matches!(
        write_bytes(&s, slot, &data),
        Err(WriteError::Full)
    ));
    assert!(
        !s.writes_disabled(),
        "ENOSPC after a short write stays a capacity condition"
    );
    assert_eq!(s.stats().write_errors, 0);
    assert_eq!(s.allocated_bytes(), 0, "the slot was taken back cold");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn punched_slot_is_free_and_cold() {
    let classes = classes();
    let big = classes.len() - 1;
    let (_dir, s) = store(u64::cast_from(classes[big]));
    let slot = s.alloc(big).unwrap();
    s.free(slot);
    s.alloc(0).expect("punching the big slot makes room");
    assert_eq!(s.classes[big].warm_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(
        s.classes[big].slots().in_use(),
        0,
        "the slot is free and cold"
    );
    let again = s.alloc(big);
    assert_eq!(again, Err(AllocError::Full), "no room for a cold big slot");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn write_error_disables_writes() {
    let (_dir, s) = store(64 << 20);
    let class = s.class_for(50_000).unwrap();
    let good = s.alloc(class).unwrap();
    let data = pattern(50_000, 5);
    write_bytes(&s, good, &data).unwrap();

    let bad = s.alloc(class).unwrap();
    fault::fail_next(fault::Op::Write, libc::EIO);
    assert!(matches!(
        write_bytes(&s, bad, &data),
        Err(WriteError::Disabled)
    ));
    assert!(s.writes_disabled());
    assert_eq!(s.stats().write_errors, 1);
    assert_eq!(s.alloc(class), Err(AllocError::WritesDisabled));
    assert!(!s.can_alloc(class));
    s.free(bad);

    assert_reads_back(&s, good, &data);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn write_after_disable_fails_without_io() {
    let (_dir, s) = store(64 << 20);
    let data = pattern(50_000, 17);
    let class = s.class_for(data.len()).unwrap();
    let first = s.alloc(class).unwrap();
    let second = s.alloc(class).unwrap();
    fault::fail_next(fault::Op::Write, libc::EIO);
    write_bytes(&s, first, &data).unwrap_err();
    let before = s.stats();
    assert!(write_bytes(&s, second, &data).is_err());
    let after = s.stats();
    assert_eq!(after.writes, before.writes);
    assert_eq!(after.write_errors, before.write_errors);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn can_alloc_counts_punchable_warm_slots() {
    let classes = classes();
    let big = classes.len() - 1;
    let small = 0;
    let (_dir, s) = store(u64::cast_from(classes[big]));
    let slot = s.alloc(big).unwrap();
    s.free(slot);
    assert_eq!(s.allocated_bytes(), s.capacity_bytes());
    assert!(
        s.can_alloc(small),
        "another class's warm slot can be punched"
    );
    s.alloc(small).expect("punching the big slot makes room");
    assert_eq!(
        s.stats().holes_punched_bytes,
        u64::cast_from(s.class_size(big))
    );
    assert!(
        !s.can_alloc(big),
        "the store holds no warm slot and no room"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn can_alloc_any_tracks_room_warm_slots_and_disabled_writes() {
    let classes = classes();
    let big = classes.len() - 1;
    let (_dir, s) = store(u64::cast_from(classes[1]));
    assert!(s.can_alloc_any(), "an empty store has room");
    let slot = s.alloc(1).unwrap();
    assert!(!s.can_alloc_any(), "full with nothing warm");
    s.free(slot);
    assert!(!s.can_alloc(big), "the big class does not fit");
    assert!(s.can_alloc_any(), "the warm slot serves another class");
    let slot = s.alloc(1).expect("the warm slot");
    fault::fail_next(fault::Op::Write, libc::EIO);
    write_bytes(&s, slot, &pattern(100, 0)).expect_err("injected");
    s.free(slot);
    assert!(!s.can_alloc_any(), "writes are disabled");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
#[should_panic(expected = "checksum")]
fn corrupt_slot_panics_on_read() {
    let (_dir, s) = store(64 << 20);
    let data = pattern(20_000, 1);
    let slot = s.alloc(s.class_for(data.len()).unwrap()).unwrap();
    write_bytes(&s, slot, &data).unwrap();
    s.corrupt(slot);
    let mut out = AlignedBuf::new();
    s.read(slot, data.len(), crc(&data), &mut out);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
#[should_panic(expected = "short read")]
fn short_read_panics() {
    let (_dir, s) = store(64 << 20);
    let len = 20_000;
    let slot = s.alloc(s.class_for(len).unwrap()).unwrap();
    // `fallocate` extends the file over the slot, so a real read would see
    // zeros. Errno 0 makes the seam return 0 bytes, as at end of file.
    fault::fail_next(fault::Op::Read, 0);
    let mut out = AlignedBuf::new();
    s.read(slot, len, 0, &mut out);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn tmpfs_is_rejected() {
    let shm = Path::new("/dev/shm");
    if !sys::is_memory_backed(shm).unwrap_or(false) {
        eprintln!("skipping tmpfs_is_rejected: /dev/shm is not tmpfs");
        return;
    }
    let err = FileStore::open(shm, Some(1 << 20), &classes()).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Unsupported);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn read_latency_is_bucketed() {
    let (_dir, s) = store(64 << 20);
    let data = pattern(4096, 9);
    let slot = s.alloc(s.class_for(data.len()).unwrap()).unwrap();
    write_bytes(&s, slot, &data).unwrap();
    assert_reads_back(&s, slot, &data);
    let buckets = s.stats().read_latency_buckets;
    assert_eq!(buckets.iter().sum::<u64>(), 1);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn probe_einval_falls_back_to_buffered() {
    let dir = disk_tempdir();
    let direct = FileStore::open(dir.path(), Some(64 << 20), &classes()).expect("open");
    if direct.io_mode() != IoMode::Direct {
        // The host filesystem lacks direct I/O, so injecting the probe
        // failure would not exercise the fallback.
        return;
    }
    drop(direct);

    fault::fail_next(fault::Op::Write, libc::EINVAL);
    let s = FileStore::open(dir.path(), Some(64 << 20), &classes()).expect("open");
    assert_eq!(s.io_mode(), IoMode::Buffered);
    let data = pattern(300_000, 13);
    let slot = s.alloc(s.class_for(data.len()).unwrap()).unwrap();
    write_bytes(&s, slot, &data).unwrap();
    assert_reads_back(&s, slot, &data);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn probe_error_fails_open() {
    let dir = disk_tempdir();
    fault::fail_next(fault::Op::Read, libc::EIO);
    let err = FileStore::open(dir.path(), Some(64 << 20), &classes()).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EIO));
}

#[mz_ore::test]
fn read_latency_bucket_bounds() {
    assert_eq!(read_latency_bucket(0), 0);
    assert_eq!(read_latency_bucket(31), 0);
    assert_eq!(read_latency_bucket(32), 1);
    assert_eq!(read_latency_bucket(32_767), 10);
    assert_eq!(read_latency_bucket(32_768), 11);
    assert_eq!(read_latency_bucket(65_535), 11);
    assert_eq!(read_latency_bucket(65_536), READ_LATENCY_BUCKETS - 1);
    assert_eq!(read_latency_bucket(u64::MAX), READ_LATENCY_BUCKETS - 1);
}

#[mz_ore::test]
fn aligned_buf_is_page_aligned_and_shrinks() {
    let page = crate::pool::region::page_size();
    let data = pattern(3 * page + 1, 0);
    let mut buf = AlignedBuf::from_bytes(&data);
    assert_eq!(buf.as_slice().as_ptr().addr() % page, 0);
    assert_eq!(buf.as_slice(), &data[..]);
    buf.shrink_above(8 * page);
    assert_eq!(buf.as_slice(), &data[..], "capacity below max is kept");
    buf.shrink_above(page);
    assert!(buf.as_slice().is_empty(), "capacity above max is released");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[cfg(target_os = "linux")]
fn tmpfile_unsupported_falls_back_to_unlinked_file() {
    use std::os::fd::AsRawFd;

    let dir = disk_tempdir();
    // Enough faults for every class file of both the direct and the
    // buffered open attempts.
    for _ in 0..2 * classes().len() {
        fault::fail_next(fault::Op::OpenTmpfile, libc::EOPNOTSUPP);
    }
    let s = FileStore::open(dir.path(), Some(64 << 20), &classes()).expect("open");
    fault::clear();
    for class in &s.classes {
        let link = std::fs::read_link(format!("/proc/self/fd/{}", class.file.as_raw_fd()))
            .expect("descriptor link");
        let link = link.to_string_lossy();
        assert!(
            link.contains(".mz-pool-extents-") && link.ends_with(" (deleted)"),
            "a named file, unlinked: {link}"
        );
    }
    let visible = || std::fs::read_dir(dir.path()).expect("read dir").count();
    assert_eq!(visible(), 0, "no file is visible");

    let data = pattern(100_000, 19);
    let slot = s.alloc(s.class_for(data.len()).unwrap()).unwrap();
    write_bytes(&s, slot, &data).unwrap();
    assert_reads_back(&s, slot, &data);
    drop(s);
    assert_eq!(visible(), 0, "no file is visible");
}
