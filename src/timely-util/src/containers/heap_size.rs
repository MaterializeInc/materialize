// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Heap size accounting for the containers that back arrangement batches.

use columnar::{AsBytes, Borrow, Columnar};
use columnation::Columnation;
use differential_dataflow::columnar::layout::Coltainer;

use crate::columnation::ColumnationStack;

/// A container that can report the heap allocations backing it.
pub trait HeapSize {
    /// Calls `callback(size, capacity)`, in bytes, once per allocation backing `self`.
    fn heap_size(&self, callback: impl FnMut(usize, usize));
}

impl<T: Columnation> HeapSize for ColumnationStack<T> {
    fn heap_size(&self, callback: impl FnMut(usize, usize)) {
        ColumnationStack::heap_size(self, callback)
    }
}

impl<C: Columnar> HeapSize for Coltainer<C> {
    fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
        // Columnar containers expose their contents as byte slices but not their spare
        // capacity, so each slice reports its length as both size and capacity. The
        // capacity is therefore a lower bound.
        for (_align, bytes) in self.container.borrow().as_bytes() {
            callback(bytes.len(), bytes.len());
        }
    }
}
