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

//! One bytes type to rule them all!
//!
//! TODO(parkertimmerman): Ideally we don't implement this "bytes type" on our own
//! and use something else, e.g. `SegmentedBuf` from the `bytes-utils` crate. Currently
//! that type, nor anything else, implement std::io::Read and std::io::Seek, which
//! we need. We have an open issue with the `bytes-utils` crate, <https://github.com/vorner/bytes-utils/issues/16>
//! to add these trait impls.
//!

use bytes::{Buf, Bytes};
use internal::SegmentedReader;
use smallvec::SmallVec;

/// A cheaply clonable collection of possibly non-contiguous bytes.
///
/// `Vec<u8>` or `Bytes` are contiguous chunks of memory, which are fast (e.g. better cache
/// locality) and easier to work with (e.g. can take a slice), but can cause problems (e.g.
/// memory fragmentation) if you try to allocate a single very large chunk. Depending on the
/// application, you probably don't need a contiguous chunk of memory, just a way to store and
/// iterate over a collection of bytes.
///
/// Note: [`SegmentedBytes`] is generic over a `const N: usize`. Internally we use a
/// [`smallvec::SmallVec`] to store our [`Bytes`] segments, and `N` is how many `Bytes` we'll
/// store inline before spilling to the heap. We default `N = 1`, so in the case of a single
/// `Bytes` segment, we avoid one layer of indirection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentedBytes<const N: usize = 1> {
    /// Collection of non-contiguous segments.
    segments: SmallVec<[Bytes; N]>,
    /// Pre-computed length of all the segments.
    len: usize,
}

impl Default for SegmentedBytes {
    fn default() -> Self {
        SegmentedBytes {
            segments: SmallVec::new(),
            len: 0,
        }
    }
}

impl SegmentedBytes {
    /// Creates a new empty [`SegmentedBytes`], reserving space inline for `N` **segments**.
    ///
    /// Note: If you don't know how many segments you have, you should use [`SegmentedBytes::default`].
    pub fn new<const N: usize>() -> SegmentedBytes<N> {
        SegmentedBytes {
            segments: SmallVec::new(),
            len: 0,
        }
    }
}

impl<const N: usize> SegmentedBytes<N> {
    /// Returns the number of bytes contained in this [`SegmentedBytes`].
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns if this [`SegmentedBytes`] is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Consumes `self` returning an [`Iterator`] over all of the non-contiguous segments
    /// that make up this buffer.
    pub fn into_segments(self) -> impl Iterator<Item = Bytes> {
        self.segments.into_iter()
    }

    /// Copies all of the bytes from `self` returning one contiguous blob.
    pub fn into_contiguous(mut self) -> Vec<u8> {
        self.copy_to_bytes(self.remaining()).into()
    }

    /// Extends the buffer by one more segment of [`Bytes`]
    pub fn push(&mut self, b: Bytes) {
        self.len += b.len();
        self.segments.push(b);
    }

    /// Consumes `self` returning a type that implements [`io::Read`] and [`io::Seek`].
    ///
    /// Note: [`Clone`]-ing a [`SegmentedBytes`] is cheap, so if you need to retain the original
    /// [`SegmentedBytes`] you should clone it.
    ///
    /// [`io::Read`]: std::io::Read
    /// [`io::Seek`]: std::io::Seek
    pub fn reader(self) -> SegmentedReader {
        SegmentedReader::new(self.segments)
    }
}

impl<const N: usize> Buf for SegmentedBytes<N> {
    fn remaining(&self) -> usize {
        self.len()
    }

    fn chunk(&self) -> &[u8] {
        // Return the first non-empty segment.
        self.segments
            .iter()
            .filter(|c| !c.is_empty())
            .map(Buf::chunk)
            .next()
            .unwrap_or_default()
    }

    fn advance(&mut self, mut cnt: usize) {
        assert!(cnt <= self.len, "Advance past the end of buffer");
        self.len -= cnt;

        while cnt > 0 {
            if let Some(seg) = self.segments.first_mut() {
                if seg.remaining() > cnt {
                    seg.advance(cnt);
                    // We advanced `cnt` bytes, so no more need to advance.
                    cnt = 0;
                } else {
                    // Remove the whole first buffer.
                    cnt = cnt.saturating_sub(seg.remaining());
                    self.segments.remove(0);
                }
            }
        }
    }
}

impl From<Bytes> for SegmentedBytes {
    fn from(value: Bytes) -> Self {
        let len = value.len();
        let mut segments = SmallVec::new();
        segments.push(value);

        SegmentedBytes { segments, len }
    }
}

impl From<Vec<Bytes>> for SegmentedBytes {
    fn from(value: Vec<Bytes>) -> Self {
        let mut len = 0;
        let mut segments = SmallVec::with_capacity(value.len());

        for segment in value {
            len += segment.len();
            segments.push(segment);
        }

        SegmentedBytes { segments, len }
    }
}

impl From<Vec<u8>> for SegmentedBytes {
    fn from(value: Vec<u8>) -> Self {
        let bytes = Bytes::from(value);
        SegmentedBytes::from(bytes)
    }
}

impl<const N: usize> FromIterator<Bytes> for SegmentedBytes<N> {
    fn from_iter<T: IntoIterator<Item = Bytes>>(iter: T) -> Self {
        let mut len = 0;
        let mut segments = SmallVec::new();

        for segment in iter {
            len += segment.len();
            segments.push(segment);
        }

        SegmentedBytes { segments, len }
    }
}

impl<const N: usize> FromIterator<Vec<u8>> for SegmentedBytes<N> {
    fn from_iter<T: IntoIterator<Item = Vec<u8>>>(iter: T) -> Self {
        iter.into_iter().map(Bytes::from).collect()
    }
}

mod internal {
    use std::collections::BTreeMap;
    use std::io;
    use std::ops::Bound;

    use bytes::Bytes;

    use crate::cast::CastFrom;

    /// Provides efficient reading and seeking across a collection of segmented bytes.
    #[derive(Debug)]
    pub struct SegmentedReader {
        segments: BTreeMap<usize, Bytes>,
        len: usize,
        pointer: u64,
    }

    impl SegmentedReader {
        pub fn new(segments: impl IntoIterator<Item = Bytes>) -> Self {
            let mut map = BTreeMap::new();
            let mut total_len = 0;

            let non_empty_segments = segments.into_iter().filter(|s| !s.is_empty());
            for segment in non_empty_segments {
                total_len += segment.len();
                map.insert(total_len, segment);
            }

            SegmentedReader {
                segments: map,
                len: total_len,
                pointer: 0,
            }
        }

        /// The total number of bytes this [`SegmentedReader`] is mapped over.
        pub fn len(&self) -> usize {
            self.len
        }

        /// The current position of the internal cursor for this [`SegmentedReader`].
        ///
        /// Note: It's possible for the current position to be greater than the length,
        /// as [`std::io::Seek`] allows you to seek past the end of the stream.
        pub fn position(&self) -> u64 {
            self.pointer
        }
    }

    impl io::Read for SegmentedReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let pointer = usize::cast_from(self.pointer);

            // We've seeked past the end, return nothing.
            if pointer > self.len {
                return Ok(0);
            }

            if let Some((accum_len, segment)) = self
                .segments
                .range((Bound::Excluded(&pointer), Bound::Included(&self.len)))
                .next()
            {
                // How many bytes we have left in this segment.
                let remaining_len = accum_len - pointer;
                // Position within the segment to being reading.
                let segment_pos = segment.len() - remaining_len;

                // How many bytes we'll read.
                let len = core::cmp::min(remaining_len, buf.len());
                // Copy bytes from the current segment into the buffer.
                buf[..len].copy_from_slice(&segment[segment_pos..segment_pos + len]);
                // Advance our pointer.
                self.pointer += u64::cast_from(len);

                Ok(len)
            } else {
                Ok(0)
            }
        }
    }

    impl io::Seek for SegmentedReader {
        fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
            use io::SeekFrom;

            // Get a base to seek from, and an offset to seek to.
            let (base, offset) = match pos {
                SeekFrom::Start(n) => {
                    self.pointer = n;
                    return Ok(n);
                }
                SeekFrom::End(n) => (u64::cast_from(self.len), n),
                SeekFrom::Current(n) => (self.pointer, n),
            };

            // Check for integer overflow, but we don't check our bounds!
            //
            // The contract for io::Seek denotes that seeking beyond the end
            // of the stream is allowed. If we're beyond the end of the stream
            // then we won't read back any bytes, but it won't be an error.
            match base.checked_add_signed(offset) {
                Some(n) => {
                    self.pointer = n;
                    Ok(self.pointer)
                }
                None => {
                    let err = io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Invalid seek to an overflowing position",
                    );
                    Err(err)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom};

    use bytes::{Buf, Bytes};
    use proptest::prelude::*;

    use super::SegmentedBytes;

    #[test]
    fn test_empty() {
        let s = SegmentedBytes::default();

        // We should report as empty.
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);

        // An iterator of segments should return nothing.
        let mut i = s.clone().into_segments();
        assert_eq!(i.next(), None);

        // bytes::Buf shouldn't report anything as remaining.
        assert_eq!(s.remaining(), 0);
        // We should get back an empty chunk if we try to read anything.
        assert!(s.chunk().is_empty());

        // Turn ourselves into a type that impls io::Read.
        let mut reader = s.reader();

        // We shouldn't panic, but we shouldn't get back any bytes.
        let mut buf = Vec::new();
        let bytes_read = reader.read(&mut buf[..]).unwrap();
        assert_eq!(bytes_read, 0);

        // We should be able to seek past the end without panicking.
        reader.seek(SeekFrom::Current(20)).unwrap();
        let bytes_read = reader.read(&mut buf[..]).unwrap();
        assert_eq!(bytes_read, 0);
    }

    #[test]
    fn test_bytes_buf() {
        let mut s = SegmentedBytes::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);

        assert_eq!(s.len(), 8);
        assert_eq!(s.len(), s.remaining());
        assert_eq!(s.chunk(), &[0, 1, 2, 3, 4, 5, 6, 7]);

        // Advance far into the buffer.
        s.advance(6);
        assert_eq!(s.len(), 2);
        assert_eq!(s.len(), s.remaining());
        assert_eq!(s.chunk(), &[6, 7]);
    }

    #[test]
    fn test_bytes_buf_multi() {
        let segments = vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]];
        let mut s: SegmentedBytes<2> = segments.into_iter().collect();

        assert_eq!(s.len(), 12);
        assert_eq!(s.len(), s.remaining());

        // Chunk should return the entirety of the first segment.
        assert_eq!(s.chunk(), &[0, 1, 2, 3]);

        // Advance into the middle segment
        s.advance(6);
        assert_eq!(s.len(), 6);
        assert_eq!(s.len(), s.remaining());

        // Chunk should return the rest of the second segment.
        assert_eq!(s.chunk(), &[6, 7]);

        // Read across two segments.
        let x = s.get_u32();
        assert_eq!(x, u32::from_be_bytes([6, 7, 8, 9]));

        // Only two bytes remaining.
        assert_eq!(s.len(), 2);
        assert_eq!(s.len(), s.remaining());

        let mut s = s.chain(&[12, 13, 14, 15][..]);

        // Should have 6 bytes total now.
        assert_eq!(s.remaining(), 6);
        // We'll read out the last two bytes from the last segment.
        assert_eq!(s.chunk(), &[10, 11]);

        // Advance into the chained segment.
        s.advance(3);
        // Read the remaining bytes.
        assert_eq!(s.chunk(), &[13, 14, 15]);
    }

    #[test]
    fn test_io_read() {
        let s = SegmentedBytes::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let mut reader = s.reader();

        assert_eq!(reader.len(), 8);
        assert_eq!(reader.position(), 0);

        // Read a small amount.
        let mut buf = [0; 4];
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 4);
        assert_eq!(buf, [0, 1, 2, 3]);

        // We should still report our original length.
        assert_eq!(reader.len(), 8);
        // But our position has moved.
        assert_eq!(reader.position(), 4);

        // We can seek forwards and read bytes.
        reader.seek(SeekFrom::Current(1)).unwrap();
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 3);
        assert_eq!(buf, [5, 6, 7, 3]);

        assert_eq!(reader.len(), 8);
        // We've read to the end!
        assert_eq!(reader.position(), 8);

        // We shouldn't read any bytes
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);
        // Buffer shouldn't change from what it was last.
        assert_eq!(buf, [5, 6, 7, 3]);

        // Seek backwards and re-read bytes.
        reader.seek(SeekFrom::Start(2)).unwrap();
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 4);
        assert_eq!(buf, [2, 3, 4, 5]);
    }

    #[test]
    fn test_io_read_multi() {
        let segments = vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7, 8, 9], vec![10, 11]];
        let s: SegmentedBytes<2> = segments.into_iter().collect();
        let mut reader = s.reader();

        assert_eq!(reader.len(), 12);
        assert_eq!(reader.position(), 0);

        // Read up to the first segment.
        let mut buf = [0; 6];
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 4);
        assert_eq!(buf, [0, 1, 2, 3, 0, 0]);

        // Read 5 bytes, which should come from the second segment.
        let bytes_read = reader.read(&mut buf[1..]).unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(buf, [0, 4, 5, 6, 7, 8]);

        // Seek backwards to the middle of the first segment.
        reader.seek(SeekFrom::Start(2)).unwrap();
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 2);
        assert_eq!(buf, [2, 3, 5, 6, 7, 8]);

        // Seek past the end.
        reader.seek(SeekFrom::Start(1000)).unwrap();
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);

        assert_eq!(reader.len(), 12);
        assert_eq!(reader.position(), 1000);

        // Seek back to the middle.
        reader.seek(SeekFrom::Start(6)).unwrap();
        // Read the entire bufffer.
        let mut buf = Vec::new();
        let bytes_read = reader.read_to_end(&mut buf).unwrap();
        assert_eq!(bytes_read, 6);
        assert_eq!(buf, &[6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn test_multi() {
        let segments = vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7, 8, 9], vec![10, 11]];
        let mut s: SegmentedBytes<2> = segments.into_iter().collect();

        assert_eq!(s.len(), 12);
        assert_eq!(s.remaining(), 12);

        // Read the first chunk.
        assert_eq!(s.chunk(), [0, 1, 2, 3]);

        // Advance to the middle.
        s.advance(6);
        assert_eq!(s.remaining(), 6);

        // Convert to a reader.
        let mut reader = s.reader();
        // We should be at the beginning, and only see the remaining 6 bytes.
        assert_eq!(reader.len(), 6);
        assert_eq!(reader.position(), 0);

        // Read to the end of the second segment.
        let mut buf = [0; 8];
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 4);
        assert_eq!(buf, [6, 7, 8, 9, 0, 0, 0, 0]);

        // Read again to get the final segment.
        let bytes_read = reader.read(&mut buf[4..]).unwrap();
        assert_eq!(bytes_read, 2);
        assert_eq!(buf, [6, 7, 8, 9, 10, 11, 0, 0]);

        // Seek back to the beginning.
        reader.seek(SeekFrom::Start(0)).unwrap();
        // Read everything.
        reader.read_exact(&mut buf[..6]).unwrap();
        assert_eq!(buf, [6, 7, 8, 9, 10, 11, 0, 0]);
    }

    #[test]
    fn test_single_empty_segment() {
        let s = SegmentedBytes::from(Vec::<u8>::new());

        // Everything should comeback empty.
        assert_eq!(s.len(), 0);
        assert_eq!(s.remaining(), 0);
        assert!(s.chunk().is_empty());

        let mut reader = s.reader();

        // Reading shouldn't fail, but it also shouldn't yield any bytes.
        let mut buf = [0; 4];
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);
        assert_eq!(buf, [0, 0, 0, 0]);
    }

    #[test]
    fn test_middle_segment_empty() {
        let segments = vec![vec![1, 2], vec![], vec![3, 4, 5, 6]];
        let mut s: SegmentedBytes = segments.clone().into_iter().collect();

        assert_eq!(s.len(), 6);
        assert_eq!(s.remaining(), 6);

        // Read and advanced past the first chunk.
        let first_chunk = s.chunk();
        assert_eq!(first_chunk, [1, 2]);
        s.advance(first_chunk.len());

        assert_eq!(s.remaining(), 4);

        // We should skip the empty segment and continue to the next.
        let second_chunk = s.chunk();
        assert_eq!(second_chunk, [3, 4, 5, 6]);

        // Recreate SegmentedBytes.
        let s: SegmentedBytes = segments.into_iter().collect();
        let mut reader = s.reader();

        // We should be able to read the first chunk.
        let mut buf = [0; 4];
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 2);
        assert_eq!(buf, [1, 2, 0, 0]);

        // And we should be able to read the second chunk without issue.
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 4);
        assert_eq!(buf, [3, 4, 5, 6]);

        // Seek backwards and read again.
        reader.seek(SeekFrom::Current(-2)).unwrap();
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 2);
        assert_eq!(buf, [5, 6, 5, 6]);
    }

    #[test]
    fn test_last_segment_empty() {
        let segments = vec![vec![1, 2], vec![3, 4, 5, 6], vec![]];
        let mut s: SegmentedBytes = segments.clone().into_iter().collect();

        assert_eq!(s.len(), 6);
        assert_eq!(s.remaining(), 6);

        // Read and advanced past the first chunk.
        let first_chunk = s.chunk();
        assert_eq!(first_chunk, [1, 2]);
        s.advance(first_chunk.len());

        assert_eq!(s.remaining(), 4);

        // Read and advance past the second chunk.
        let second_chunk = s.chunk();
        assert_eq!(second_chunk, [3, 4, 5, 6]);
        s.advance(second_chunk.len());

        // No bytes should remain.
        assert_eq!(s.remaining(), 0);
        assert!(s.chunk().is_empty());

        // Recreate SegmentedBytes.
        let s: SegmentedBytes = segments.into_iter().collect();
        let mut reader = s.reader();

        // We should be able to read the first chunk.
        let mut buf = [0; 4];
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 2);
        assert_eq!(buf, [1, 2, 0, 0]);

        // And we should be able to read the second chunk without issue.
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 4);
        assert_eq!(buf, [3, 4, 5, 6]);

        // Reading again shouldn't provide any bytes.
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);
        // Buffer shouldn't change.
        assert_eq!(buf, [3, 4, 5, 6]);

        // Seek backwards and read again.
        reader.seek(SeekFrom::Current(-2)).unwrap();
        let bytes_read = reader.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 2);
        assert_eq!(buf, [5, 6, 5, 6]);
    }

    proptest! {
        #[test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_copy_to_bytes(segments: Vec<Vec<u8>>, num_bytes: usize) {
            let contiguous: Vec<u8> = segments.clone().into_iter().flatten().collect();
            let mut contiguous = Bytes::from(contiguous);
            let mut segmented: SegmentedBytes = segments.into_iter().map(Bytes::from).collect();

            // Cap num_bytes at the size of contiguous.
            let num_bytes = contiguous.len() % num_bytes;

            let copied_c = contiguous.copy_to_bytes(num_bytes);
            let copied_s = segmented.copy_to_bytes(num_bytes);

            prop_assert_eq!(copied_c, copied_s);
        }

        #[test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_read_to_end(segments: Vec<Vec<u8>>) {
            let contiguous: Vec<u8> = segments.clone().into_iter().flatten().collect();
            let contiguous = Bytes::from(contiguous);
            let segmented: SegmentedBytes = segments.into_iter().map(Bytes::from).collect();

            let mut reader_c = contiguous.reader();
            let mut reader_s = segmented.reader();

            let mut buf_c = Vec::new();
            reader_c.read_to_end(&mut buf_c).unwrap();

            let mut buf_s = Vec::new();
            reader_s.read_to_end(&mut buf_s).unwrap();

            assert_eq!(buf_s, buf_s);
        }
    }
}
