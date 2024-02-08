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

//! The [bytes] crate but backed by [lgalloc].

use std::sync::Arc;

use bytes::Buf;
use lgalloc::AllocError;

use crate::region::Region;

/// [bytes::Bytes] but backed by [lgalloc].
#[derive(Clone)]
pub struct LgBytes {
    offset: usize,
    region: Arc<Region<u8>>,
}

impl std::fmt::Debug for LgBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_slice(), f)
    }
}

impl LgBytes {
    /// Copies the given buf into an lgalloc managed file-based mapped region,
    /// returning it as a [LgBytes].
    pub fn new_mmap<T: AsRef<[u8]>>(buf: T) -> Result<Self, AllocError> {
        let buf = buf.as_ref();
        // Round the capacity up to the minimum lgalloc mmap size.
        let capacity = std::cmp::max(buf.len(), 1 << lgalloc::VALID_SIZE_CLASS.start);
        let mut region = Region::new_mmap(capacity)?;
        region.extend_from_slice(buf);
        Ok(LgBytes {
            offset: 0,
            region: Arc::new(region),
        })
    }

    /// Presents this buf as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        // This implementation of [bytes::Buf] chooses to panic instead of
        // allowing the offset to advance past remaining, which means this
        // invariant should always hold and we shouldn't need the std::cmp::min.
        // Be defensive anyway.
        debug_assert!(self.offset <= self.region.len());
        let offset = std::cmp::min(self.offset, self.region.len());
        &self.region[offset..]
    }
}

impl From<Region<u8>> for LgBytes {
    fn from(region: Region<u8>) -> Self {
        LgBytes {
            offset: 0,
            region: Arc::new(region),
        }
    }
}

impl Buf for LgBytes {
    /// Returns the number of bytes between the current position and the end of
    /// the buffer.
    ///
    /// This value is greater than or equal to the length of the slice returned
    /// by `chunk()`.
    ///
    /// # Implementer notes
    ///
    /// Implementations of `remaining` should ensure that the return value does
    /// not change unless a call is made to `advance` or any other function that
    /// is documented to change the `Buf`'s current position.
    fn remaining(&self) -> usize {
        self.as_slice().len()
    }

    /// Returns a slice starting at the current position and of length between 0
    /// and `Buf::remaining()`. Note that this *can* return shorter slice (this
    /// allows non-continuous internal representation).
    ///
    /// This is a lower level function. Most operations are done with other
    /// functions.
    ///
    /// # Implementer notes
    ///
    /// This function should never panic. Once the end of the buffer is reached,
    /// i.e., `Buf::remaining` returns 0, calls to `chunk()` should return an
    /// empty slice.
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    /// Advance the internal cursor of the Buf
    ///
    /// The next call to `chunk()` will return a slice starting `cnt` bytes
    /// further into the underlying buffer.
    ///
    /// # Panics
    ///
    /// This function panics if `cnt > self.remaining()`.
    ///
    /// # Implementer notes
    ///
    /// It is recommended for implementations of `advance` to panic if `cnt >
    /// self.remaining()`. If the implementation does not panic, the call must
    /// behave as if `cnt == self.remaining()`.
    ///
    /// A call with `cnt == 0` should never panic and be a no-op.
    fn advance(&mut self, cnt: usize) {
        if cnt > self.remaining() {
            panic!(
                "cannot advance by {} only {} remaining",
                cnt,
                self.remaining()
            )
        };
        self.offset += cnt;
    }
}
