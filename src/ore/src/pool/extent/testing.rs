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

//! Test-only codec and accessors of the extent layer.

// The file-mode and file-store tests are Linux-only, so their helpers are
// unused in non-Linux test builds.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use super::*;

/// lz4 codec for the pool's own tests, mirroring the production codec that
/// lives with the chunk implementation: a little-endian `u32` body-length
/// prefix followed by one lz4 block.
#[derive(Debug)]
pub(crate) struct TestLz4Codec;

pub(crate) static TEST_CODEC: TestLz4Codec = TestLz4Codec;

impl ExtentCodec for TestLz4Codec {
    fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
        let max_out = lz4_flex::block::get_maximum_output_size(body.len());
        out.resize(4 + max_out, 0);
        let len = u32::try_from(body.len()).expect("chunk payloads fit u32");
        out[..4].copy_from_slice(&len.to_le_bytes());
        let compressed = lz4_flex::block::compress_into(body, &mut out[4..])
            .expect("output sized to the maximum");
        out.truncate(4 + compressed);
    }

    fn decode(&self, stored: &[u8], body: &mut [u8]) {
        let prefix: [u8; 4] = stored[..4].try_into().expect("prefix length");
        let len = usize::try_from(u32::from_le_bytes(prefix)).expect("length fits usize");
        assert_eq!(
            len,
            body.len(),
            "destination must match the encoded body length"
        );
        let written = lz4_flex::block::decompress_into(&stored[4..], body)
            .expect("stored bytes hold a valid lz4 block");
        assert_eq!(written, body.len(), "decoded length mismatch");
    }
}

impl ExtentArena {
    /// Test hook: the number of slots currently allocated across classes.
    pub(crate) fn slots_in_use(&self) -> usize {
        self.regions.iter().map(Region::slots_in_use).sum()
    }
}

impl Extent {
    /// The store and slot of a `File` extent, `None` otherwise.
    pub(crate) fn file_slot(&self) -> Option<(Arc<FileStore>, FileSlot)> {
        self.file_location()
            .map(|location| (location.store, location.slot))
    }
}
