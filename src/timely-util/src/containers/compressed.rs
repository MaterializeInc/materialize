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

/// A compressed column that stores data in a compressed format using LZ4.
///
/// The column's type needs to be remembered outside of this struct.
#[derive(Debug, Clone, Default)]
pub struct CompressedColumn {
    uncompressed_size: usize,
    elements: usize,
    data: Vec<u8>,
    valid: usize,
}

impl CompressedColumn {
    pub fn uncompressed_size(&self) -> usize {
        self.uncompressed_size
    }

    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    pub fn elements(&self) -> usize {
        self.elements
    }

    pub fn compress_aligned(
        column_len: usize,
        align: &[u64],
        buffer: &mut Vec<u8>,
        empty: &mut Self,
    ) -> Self {
        let max_size = lz4_flex::block::get_maximum_output_size(align.len() * 8);
        buffer.resize(max_size, 0);
        let len = lz4_flex::block::compress_into(bytemuck::cast_slice(&*align), buffer).unwrap();
        let mut region = std::mem::take(&mut empty.data);
        region.clear();
        region.extend_from_slice(&buffer[..len]);
        Self {
            uncompressed_size: align.len() * 8,
            elements: column_len,
            data: region,
            valid: len,
        }
    }

    pub fn decompress(&self, aligned: &mut [u64]) {
        assert_eq!(self.uncompressed_size, aligned.len() * 8);
        lz4_flex::block::decompress_into(
            &self.data[..self.valid],
            bytemuck::cast_slice_mut(aligned),
        )
        .expect("Failed to decompress block");
    }
}
