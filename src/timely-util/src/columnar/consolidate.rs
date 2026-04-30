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

//! A `ContainerBuilder` that consolidates `(D, T, R)` updates and emits columnar containers.
//!
//! Vendored from `differential_dataflow::consolidation::ConsolidatingContainerBuilder` for the
//! staging path; the inner [`ColumnBuilder`] mints aligned columnar containers from the
//! consolidated tuples.

use columnar::{Columnar, Push};
use differential_dataflow::Data;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use timely::container::{ContainerBuilder, PushInto};

use crate::columnar::Column;
use crate::columnar::builder::ColumnBuilder;

/// Target size in bytes for the staging buffer chunk.
///
/// Larger than `timely::container::buffer::default_capacity` (8 KiB) to amortize the cost of
/// minting aligned columnar containers. Matches `ColumnationChunker` in `crate::columnation`.
const BUFFER_SIZE_BYTES: usize = 64 << 10;

/// Number of `(D, T, R)` items that fit in [`BUFFER_SIZE_BYTES`], or 1 for oversized tuples.
const fn chunk_capacity<C>() -> usize {
    let size = std::mem::size_of::<C>();
    if size == 0 {
        BUFFER_SIZE_BYTES
    } else if size <= BUFFER_SIZE_BYTES {
        BUFFER_SIZE_BYTES / size
    } else {
        1
    }
}

/// A container builder that consolidates `(D, T, R)` updates and emits `Column<(D, T, R)>`.
///
/// Buffers updates in an internal `Vec`, calls
/// [`consolidate_updates`] when the buffer fills, and pushes the surviving updates into an inner
/// [`ColumnBuilder`]. The inner builder chunks output into ~2MB-aligned columnar containers
/// (see [`ColumnBuilder`] for details).
///
/// Does **not** maintain FIFO ordering (consolidation reorders updates).
pub struct ConsolidatingColumnBuilder<D, T, R>
where
    (D, T, R): Columnar,
{
    /// Pre-consolidation staging buffer.
    current: Vec<(D, T, R)>,
    /// Inner columnar builder; receives consolidated tuples and emits `Column<(D, T, R)>`.
    column: ColumnBuilder<(D, T, R)>,
}

impl<D, T, R> Default for ConsolidatingColumnBuilder<D, T, R>
where
    (D, T, R): Columnar,
{
    fn default() -> Self {
        Self {
            current: Vec::new(),
            column: ColumnBuilder::default(),
        }
    }
}

impl<D, T, R> ConsolidatingColumnBuilder<D, T, R>
where
    D: Data,
    T: Data,
    R: Semigroup + 'static,
    (D, T, R): Columnar,
    <(D, T, R) as Columnar>::Container: Push<(D, T, R)>,
{
    /// Consolidate `current` and drain the largest multiple-of-`multiple` prefix into the inner
    /// columnar builder. Pass `1` to drain everything.
    #[cold]
    fn consolidate_and_drain(&mut self, multiple: usize) {
        consolidate_updates(&mut self.current);
        let drain_len = (self.current.len() / multiple) * multiple;
        for item in self.current.drain(..drain_len) {
            self.column.push_into(item);
        }
    }
}

impl<D, T, R> PushInto<(D, T, R)> for ConsolidatingColumnBuilder<D, T, R>
where
    D: Data,
    T: Data,
    R: Semigroup + 'static,
    (D, T, R): Columnar,
    <(D, T, R) as Columnar>::Container: Push<(D, T, R)>,
{
    /// Push an element.
    ///
    /// Precondition: `current` is not allocated or has space for at least one element.
    #[inline]
    fn push_into(&mut self, item: (D, T, R)) {
        let chunk = chunk_capacity::<(D, T, R)>();
        if self.current.capacity() < chunk * 2 {
            self.current.reserve(chunk * 2 - self.current.capacity());
        }
        self.current.push(item);
        if self.current.len() == self.current.capacity() {
            self.consolidate_and_drain(chunk);
        }
    }
}

impl<D, T, R> ContainerBuilder for ConsolidatingColumnBuilder<D, T, R>
where
    D: Data,
    T: Data,
    R: Semigroup + 'static,
    (D, T, R): Columnar,
    <(D, T, R) as Columnar>::Container: Push<(D, T, R)> + Clone,
{
    type Container = Column<(D, T, R)>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.column.extract()
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.current.is_empty() {
            self.consolidate_and_drain(1);
        }
        self.column.finish()
    }
}

#[cfg(test)]
mod tests {
    use columnar::Index;
    use columnar::Len;
    use timely::container::{ContainerBuilder, PushInto};

    use super::*;

    /// Collect every `(D, T, R)` row from a `Column<(u64, u64, i64)>`.
    fn rows(column: &Column<(u64, u64, i64)>) -> Vec<(u64, u64, i64)> {
        let borrow = column.borrow();
        (0..borrow.len())
            .map(|i| {
                let r = borrow.get(i);
                (*r.0, *r.1, *r.2)
            })
            .collect()
    }

    /// Drain a builder by repeatedly calling `extract` then `finish`.
    fn drain(mut builder: ConsolidatingColumnBuilder<u64, u64, i64>) -> Vec<(u64, u64, i64)> {
        let mut out: Vec<(u64, u64, i64)> = Vec::new();
        while let Some(c) = builder.extract() {
            for r in rows(c) {
                out.push(r);
            }
        }
        if let Some(c) = builder.finish() {
            for r in rows(c) {
                out.push(r);
            }
        }
        out
    }

    #[mz_ore::test]
    fn empty_finish_yields_none() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        assert!(builder.extract().is_none());
        assert!(builder.finish().is_none());
    }

    #[mz_ore::test]
    fn single_push_finish_yields_one() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        builder.push_into((1u64, 0u64, 1i64));
        let column = builder.finish().expect("one container");
        assert_eq!(rows(column), vec![(1, 0, 1)]);
        assert!(builder.finish().is_none());
    }

    #[mz_ore::test]
    fn consolidates_on_threshold() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        let chunk = chunk_capacity::<(u64, u64, i64)>();
        // Push enough +1/-1 pairs to exceed `chunk * 2` and trigger several consolidation
        // cycles. Everything cancels.
        for _ in 0..(chunk * 4) {
            builder.push_into((7u64, 0u64, 1i64));
            builder.push_into((7u64, 0u64, -1i64));
        }
        assert!(drain(builder).is_empty());
    }

    #[mz_ore::test]
    fn multiple_distinct_keys() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        builder.push_into((1u64, 0u64, 1i64));
        builder.push_into((2u64, 0u64, 1i64));
        builder.push_into((1u64, 0u64, 1i64));
        let mut out = drain(builder);
        out.sort();
        assert_eq!(out, vec![(1, 0, 2), (2, 0, 1)]);
    }

    #[mz_ore::test]
    fn emits_multiple_containers() {
        let mut builder: ConsolidatingColumnBuilder<u64, u64, i64> = Default::default();
        // Enough distinct rows to force the inner ColumnBuilder past its ~2MB threshold and mint
        // multiple containers. Each row is 24 bytes; 200_000 rows ≈ 4.8MB raw.
        let n: u64 = 200_000;
        for d in 0..n {
            builder.push_into((d, 0u64, 1i64));
        }

        let mut containers = 0;
        let mut out: Vec<(u64, u64, i64)> = Vec::new();
        while let Some(c) = builder.extract() {
            containers += 1;
            for r in rows(c) {
                out.push(r);
            }
        }
        if let Some(c) = builder.finish() {
            containers += 1;
            for r in rows(c) {
                out.push(r);
            }
        }
        assert!(
            containers > 1,
            "expected multiple containers, got {containers}"
        );
        out.sort();
        let expected: Vec<(u64, u64, i64)> = (0..n).map(|d| (d, 0u64, 1i64)).collect();
        assert_eq!(out, expected);
    }
}
