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

#[cfg(feature = "parquet")]
use std::io::Seek;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use internal::SegmentedReader;
#[cfg(feature = "parquet")]
use parquet::errors::ParquetError;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[cfg(feature = "parquet")]
use crate::cast::CastFrom;

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentedBytes<const N: usize = 1> {
    /// Collection of non-contiguous segments, each segment is guaranteed to be non-empty.
    segments: SmallVec<[(Bytes, Padding); N]>,
    /// Pre-computed length of all the segments.
    len: usize,
}

/// We add [`Padding`] to segments to prevent needing to re-allocate our
/// collection when creating a [`SegmentedReader`].
type Padding = usize;

/// Default value used for [`Padding`].
const PADDING_DEFAULT: usize = 0;

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
    /// Creates a new empty [`SegmentedBytes`] with space for `capacity` **segments**.
    pub fn with_capacity(capacity: usize) -> SegmentedBytes<N> {
        SegmentedBytes {
            segments: SmallVec::with_capacity(capacity),
            len: 0,
        }
    }

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
        self.segments.into_iter().map(|(bytes, _len)| bytes)
    }

    /// Copies all of the bytes from `self` returning one contiguous blob.
    pub fn into_contiguous(mut self) -> Vec<u8> {
        self.copy_to_bytes(self.remaining()).into()
    }

    /// Extends the buffer by one more segment of [`Bytes`].
    ///
    /// If the provided [`Bytes`] is empty, we skip appending it.
    #[inline]
    pub fn push<B: Into<Bytes>>(&mut self, b: B) {
        let b: Bytes = b.into();
        if !b.is_empty() {
            self.len += b.len();
            self.segments.push((b, PADDING_DEFAULT));
        }
    }

    /// Consumes `self` returning a type that implements [`io::Read`] and [`io::Seek`].
    ///
    /// Note: [`Clone`]-ing a [`SegmentedBytes`] is cheap, so if you need to retain the original
    /// [`SegmentedBytes`] you should clone it.
    ///
    /// [`io::Read`]: std::io::Read
    /// [`io::Seek`]: std::io::Seek
    pub fn reader(self) -> SegmentedReader<N> {
        SegmentedReader::new(self)
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
            .filter(|(c, _len)| !c.is_empty())
            .map(|(c, _len)| Buf::chunk(c))
            .next()
            .unwrap_or_default()
    }

    fn advance(&mut self, mut cnt: usize) {
        assert!(cnt <= self.len, "Advance past the end of buffer");
        self.len -= cnt;

        while cnt > 0 {
            if let Some((seg, _len)) = self.segments.first_mut() {
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

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        // If possible, use the zero-copy implementation on individual segments.
        if let Some((seg, _len)) = self.segments.first_mut() {
            if len <= seg.len() {
                self.len -= len;
                return seg.copy_to_bytes(len);
            }
        }
        // Otherwise, fall back to a generic implementation.
        assert!(
            len <= self.len(),
            "tried to copy {len} bytes with {} remaining",
            self.len()
        );
        let mut out = BytesMut::with_capacity(len);
        out.put(self.take(len));
        out.freeze()
    }
}

#[cfg(feature = "parquet")]
impl parquet::file::reader::Length for SegmentedBytes {
    fn len(&self) -> u64 {
        u64::cast_from(self.len)
    }
}

#[cfg(feature = "parquet")]
impl parquet::file::reader::ChunkReader for SegmentedBytes {
    type T = internal::SegmentedReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut reader = self.clone().reader();
        reader.seek(std::io::SeekFrom::Start(start))?;
        Ok(reader)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let start = usize::cast_from(start);
        let mut buf = self.clone();
        if start > buf.remaining() {
            return Err(ParquetError::EOF(format!(
                "seeking {start} bytes ahead, but only {} remaining",
                buf.remaining()
            )));
        }
        buf.advance(start);
        if length > buf.remaining() {
            return Err(ParquetError::EOF(format!(
                "copying {length} bytes, but only {} remaining",
                buf.remaining()
            )));
        }
        let bytes = buf.copy_to_bytes(length);
        Ok(bytes)
    }
}

impl From<Bytes> for SegmentedBytes {
    fn from(value: Bytes) -> Self {
        let mut s = SegmentedBytes::default();
        s.push(value);
        s
    }
}

impl From<Vec<u8>> for SegmentedBytes {
    fn from(value: Vec<u8>) -> Self {
        let b = Bytes::from(value);
        SegmentedBytes::from(b)
    }
}

impl From<Vec<Bytes>> for SegmentedBytes {
    fn from(value: Vec<Bytes>) -> Self {
        let mut s = SegmentedBytes::with_capacity(value.len());
        for segment in value {
            s.push(segment);
        }
        s
    }
}

impl<const N: usize> FromIterator<Bytes> for SegmentedBytes<N> {
    fn from_iter<T: IntoIterator<Item = Bytes>>(iter: T) -> Self {
        let mut s = SegmentedBytes::new();
        for segment in iter {
            s.push(segment);
        }
        s
    }
}

impl<const N: usize> FromIterator<Vec<u8>> for SegmentedBytes<N> {
    fn from_iter<T: IntoIterator<Item = Vec<u8>>>(iter: T) -> Self {
        iter.into_iter().map(Bytes::from).collect()
    }
}

mod internal {
    use std::io;

    use smallvec::SmallVec;

    use crate::bytes::{Bytes, SegmentedBytes};
    use crate::cast::CastFrom;

    /// Provides efficient reading and seeking across a collection of segmented bytes.
    #[derive(Debug)]
    pub struct SegmentedReader<const N: usize = 1> {
        segments: SmallVec<[(Bytes, usize); N]>,
        /// Total length of all segments.
        len: usize,
        // Overall byte position we're currently pointing at.
        overall_ptr: usize,
        /// Current segement that we'd read from.
        segment_ptr: usize,
    }

    impl<const N: usize> SegmentedReader<N> {
        pub fn new(mut bytes: SegmentedBytes<N>) -> Self {
            // Re-adjust our accumlated lengths.
            //
            // Note: `SegmentedBytes` could track the accumulated lengths, but
            // it's complicated by the impl of `Buf::advance`.
            let mut accum_length = 0;
            for (segment, len) in &mut bytes.segments {
                accum_length += segment.len();
                *len = accum_length;
            }

            SegmentedReader {
                segments: bytes.segments,
                len: bytes.len,
                overall_ptr: 0,
                segment_ptr: 0,
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
        pub fn position(&self) -> usize {
            self.overall_ptr
        }
    }

    impl<const N: usize> io::Read for SegmentedReader<N> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            // We've seeked past the end, return nothing.
            if self.overall_ptr >= self.len {
                return Ok(0);
            }

            let (segment, accum_length) = &self.segments[self.segment_ptr];

            // How many bytes we have left in this segment.
            let remaining_len = accum_length.checked_sub(self.overall_ptr).unwrap();
            // Position within the segment to begin reading.
            let segment_pos = segment.len().checked_sub(remaining_len).unwrap();

            // How many bytes we'll read.
            let len = core::cmp::min(remaining_len, buf.len());
            // Copy bytes from the current segment into the buffer.
            let segment_buf = &segment[..];
            buf[..len].copy_from_slice(&segment_buf[segment_pos..segment_pos + len]);

            // Advance our pointers.
            self.overall_ptr += len;

            // Advance to the next segment if we've reached the end of the current.
            if self.overall_ptr == *accum_length {
                self.segment_ptr += 1;
            }

            Ok(len)
        }
    }

    impl<const N: usize> io::Seek for SegmentedReader<N> {
        fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
            use io::SeekFrom;

            // Get an offset from the start.
            let maybe_offset = match pos {
                SeekFrom::Start(n) => Some(usize::cast_from(n)),
                SeekFrom::End(n) => {
                    let n = isize::cast_from(n);
                    self.len().checked_add_signed(n)
                }
                SeekFrom::Current(n) => {
                    let n = isize::cast_from(n);
                    self.overall_ptr.checked_add_signed(n)
                }
            };

            // Check for integer overflow, but we don't check our bounds!
            //
            // The contract for io::Seek denotes that seeking beyond the end
            // of the stream is allowed. If we're beyond the end of the stream
            // then we won't read back any bytes, but it won't be an error.
            let offset = maybe_offset.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid seek to an overflowing position",
                )
            })?;

            // Special case we want to be fast, seeking back to the beginning.
            if offset == 0 {
                self.overall_ptr = 0;
                self.segment_ptr = 0;

                return Ok(u64::cast_from(offset));
            }

            // Seek through our segments until we get to the correct offset.
            let result = self
                .segments
                .binary_search_by(|(_s, accum_len)| accum_len.cmp(&offset));

            self.segment_ptr = match result {
                Ok(segment_ptr) => segment_ptr + 1,
                Err(segment_ptr) => segment_ptr,
            };
            self.overall_ptr = offset;

            Ok(u64::cast_from(offset))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom};

    use bytes::{Buf, Bytes};
    use proptest::prelude::*;

    use super::SegmentedBytes;
    use crate::cast::CastFrom;

    #[crate::test]
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

    #[crate::test]
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

    #[crate::test]
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

    #[crate::test]
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

    #[crate::test]
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

    #[crate::test]
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

    #[crate::test]
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

    #[crate::test]
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

    #[crate::test]
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

    #[crate::test]
    #[cfg_attr(miri, ignore)] // slow
    fn proptest_copy_to_bytes() {
        fn test(segments: Vec<Vec<u8>>, num_bytes: usize) {
            let contiguous: Vec<u8> = segments.clone().into_iter().flatten().collect();
            let mut contiguous = Bytes::from(contiguous);
            let mut segmented: SegmentedBytes = segments.into_iter().map(Bytes::from).collect();

            // Cap num_bytes at the size of contiguous.
            let num_bytes = contiguous.len() % num_bytes;
            let remaining = contiguous.len() - num_bytes;

            let copied_c = contiguous.copy_to_bytes(num_bytes);
            let copied_s = segmented.copy_to_bytes(num_bytes);

            assert_eq!(copied_c, copied_s);

            let copied_c = contiguous.copy_to_bytes(remaining);
            let copied_s = segmented.copy_to_bytes(remaining);

            assert_eq!(copied_c, copied_s);
        }

        proptest!(|(segments in any::<Vec<Vec<u8>>>(), num_bytes in any::<usize>())| {
            test(segments, num_bytes);
        })
    }

    #[crate::test]
    #[cfg_attr(miri, ignore)] // slow
    fn proptest_read_to_end() {
        fn test(segments: Vec<Vec<u8>>) {
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

        proptest!(|(segments in any::<Vec<Vec<u8>>>())| {
            test(segments);
        })
    }

    #[crate::test]
    #[cfg_attr(miri, ignore)] // slow
    fn proptest_read_and_seek() {
        fn test(segments: Vec<Vec<u8>>, from_start: u64, from_current: i64, from_end: i64) {
            let contiguous: Vec<u8> = segments.clone().into_iter().flatten().collect();
            let total_len = contiguous.len();
            let contiguous = std::io::Cursor::new(&contiguous[..]);
            let segmented: SegmentedBytes = segments.into_iter().map(Bytes::from).collect();

            let mut reader_c = contiguous;
            let mut reader_s = segmented.reader();

            let mut buf_c = Vec::new();
            let mut buf_s = Vec::new();

            // Seek from the start.

            let from_start = from_start % (u64::cast_from(total_len).max(1));
            reader_c.seek(SeekFrom::Start(from_start)).unwrap();
            reader_s.seek(SeekFrom::Start(from_start)).unwrap();

            reader_c.read_to_end(&mut buf_c).unwrap();
            reader_s.read_to_end(&mut buf_s).unwrap();

            assert_eq!(&buf_c, &buf_s);
            buf_c.clear();
            buf_s.clear();

            // Seek from the current position.

            let from_current = from_current % i64::try_from(total_len).unwrap().max(1);
            reader_c.seek(SeekFrom::Current(from_current)).unwrap();
            reader_s.seek(SeekFrom::Current(from_current)).unwrap();

            reader_c.read_to_end(&mut buf_c).unwrap();
            reader_s.read_to_end(&mut buf_s).unwrap();

            assert_eq!(&buf_c, &buf_s);
            buf_c.clear();
            buf_s.clear();

            // Seek from the end.

            let from_end = from_end % i64::try_from(total_len).unwrap().max(1);
            reader_c.seek(SeekFrom::End(from_end)).unwrap();
            reader_s.seek(SeekFrom::End(from_end)).unwrap();

            reader_c.read_to_end(&mut buf_c).unwrap();
            reader_s.read_to_end(&mut buf_s).unwrap();

            assert_eq!(&buf_c, &buf_s);
            buf_c.clear();
            buf_s.clear();
        }

        proptest!(|(segments in any::<Vec<Vec<u8>>>(), s in any::<u64>(), c in any::<i64>(), e in any::<i64>())| {
            test(segments, s, c, e);
        })
    }

    #[crate::test]
    #[cfg_attr(miri, ignore)] // slow
    fn proptest_non_empty_segments() {
        fn test(segments: Vec<Vec<u8>>) {
            // Vec
            let segment = segments.first().cloned().unwrap_or_default();
            let s = SegmentedBytes::from(segment.clone());
            assert!(s.into_segments().all(|segment| !segment.is_empty()));

            // Bytes
            let bytes = Bytes::from(segment.clone());
            let s = SegmentedBytes::from(bytes);
            assert!(s.into_segments().all(|segment| !segment.is_empty()));

            // SegmentedBytes::push
            let mut s = SegmentedBytes::default();
            s.push(Bytes::from(segment));
            assert!(s.into_segments().all(|segment| !segment.is_empty()));

            // Vec<Vec<u8>>
            let s: SegmentedBytes = segments.clone().into_iter().collect();
            assert!(s.into_segments().all(|segment| !segment.is_empty()));

            // Vec<Bytes>
            let s: SegmentedBytes = segments.clone().into_iter().map(Bytes::from).collect();
            assert!(s.into_segments().all(|segment| !segment.is_empty()));

            // Vec<Bytes>
            let segments: Vec<_> = segments.into_iter().map(Bytes::from).collect();
            let s = SegmentedBytes::from(segments);
            assert!(s.into_segments().all(|segment| !segment.is_empty()));
        }

        proptest!(|(segments in any::<Vec<Vec<u8>>>())| {
            test(segments);
        })
    }
}
