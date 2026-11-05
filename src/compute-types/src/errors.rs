// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serialized dataflow errors.
//!
//! # `DataflowErrorSer`
//!
//! [`DataflowErrorSer`] is a serialized byte representation of
//! [`DataflowError`] used on compute-internal dataflow edges instead of
//! `DataflowError` directly.
//!
//! It is backed by proto-encoded [`ProtoDataflowError`] bytes. Because proto3 +
//! prost + no map fields = deterministic encoding, byte-equality implies
//! semantic equality, which lets us use `Ord`, `Hash`, etc. directly on the
//! bytes.
//!
//! **Invariant**: NEVER add `map` fields to `ProtoDataflowError` or any of its
//! transitive message types, as map fields have non-deterministic encoding
//! order in protobuf.

use columnar::Columnar;
use columnation::{Columnation, Region};
use mz_expr::EvalError;
use mz_proto::{ProtoType, RustType};
use mz_storage_types::errors::{DataflowError, ProtoDataflowError};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Serialized representation of a [`DataflowError`], backed by proto-encoded bytes.
///
/// This type is used on compute-internal dataflow edges to avoid the cost of
/// carrying a full `DataflowError` enum through the dataflow graph. Because the
/// proto encoding is canonical (proto3 + prost + no map fields), byte-equality
/// implies semantic equality.
#[derive(
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    Deserialize,
    Columnar
)]
#[columnar(derive(Eq, PartialEq, Ord, PartialOrd))]
pub struct DataflowErrorSer(Vec<u8>);

impl DataflowErrorSer {
    /// Decode the serialized bytes back into a [`DataflowError`].
    ///
    /// # Panics
    ///
    /// Panics if the bytes do not represent a valid `ProtoDataflowError`.
    pub fn deserialize(&self) -> DataflowError {
        let proto = ProtoDataflowError::decode(self.0.as_slice())
            .expect("DataflowErrorSer: invalid proto bytes");
        proto
            .into_rust()
            .expect("DataflowErrorSer: failed to convert proto to DataflowError")
    }

    /// The length of the serialized bytes.
    pub fn byte_len(&self) -> usize {
        self.0.len()
    }
}

impl From<DataflowError> for DataflowErrorSer {
    fn from(err: DataflowError) -> Self {
        DataflowErrorSer(err.into_proto().encode_to_vec())
    }
}

impl From<EvalError> for DataflowErrorSer {
    fn from(err: EvalError) -> Self {
        // Note: this allocates a Box via DataflowError::EvalError(Box::new(e)).
        // Acceptable in v1.
        DataflowErrorSer::from(DataflowError::from(err))
    }
}

impl fmt::Display for DataflowErrorSer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deserialize().fmt(f)
    }
}

impl fmt::Debug for DataflowErrorSer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DataflowErrorSer")
            .field(&format_args!("{} bytes", self.0.len()))
            .finish()
    }
}

impl Columnation for DataflowErrorSer {
    type InnerRegion = DataflowErrorSerRegion;
}

/// A [`Region`] for [`DataflowErrorSer`], delegating to the region for `Vec<u8>`.
#[derive(Default)]
pub struct DataflowErrorSerRegion {
    inner: <Vec<u8> as Columnation>::InnerRegion,
}

impl Region for DataflowErrorSerRegion {
    type Item = DataflowErrorSer;

    unsafe fn copy(&mut self, item: &Self::Item) -> Self::Item {
        // SAFETY: delegating to the inner Vec<u8> region which handles the allocation.
        DataflowErrorSer(unsafe { self.inner.copy(&item.0) })
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn reserve_items<'a, I>(&mut self, items: I)
    where
        I: Iterator<Item = &'a Self::Item> + Clone,
    {
        self.inner.reserve_items(items.map(|item| &item.0));
    }

    fn reserve_regions<'a, I>(&mut self, regions: I)
    where
        I: Iterator<Item = &'a Self> + Clone,
    {
        self.inner.reserve_regions(regions.map(|r| &r.inner));
    }

    fn heap_size(&self, callback: impl FnMut(usize, usize)) {
        self.inner.heap_size(callback);
    }
}
