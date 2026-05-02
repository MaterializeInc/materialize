// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for handling errors encountered by operators.
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
use mz_repr::Row;
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

/// Used to make possibly-validating code generic: think of this as a kind of `MaybeResult`,
/// specialized for use in compute.  Validation code will only run when the error constructor is
/// Some.
pub(super) trait MaybeValidatingRow<T, E> {
    fn ok(t: T) -> Self;
    fn into_error() -> Option<fn(E) -> Self>;
}

impl<E> MaybeValidatingRow<Row, E> for Row {
    fn ok(t: Row) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<E> MaybeValidatingRow<(), E> for () {
    fn ok(t: ()) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<E, R> MaybeValidatingRow<Vec<R>, E> for Vec<R> {
    fn ok(t: Vec<R>) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<T, E> MaybeValidatingRow<T, E> for Result<T, E> {
    fn ok(row: T) -> Self {
        Ok(row)
    }

    fn into_error() -> Option<fn(E) -> Self> {
        Some(Err)
    }
}

/// Error logger to be used by rendering code.
// TODO: Consider removing this struct.
#[derive(Clone)]
pub(super) struct ErrorLogger {
    dataflow_name: String,
}

impl ErrorLogger {
    pub fn new(dataflow_name: String) -> Self {
        Self { dataflow_name }
    }

    /// Log the given error.
    ///
    /// The logging format is optimized for surfacing errors with Sentry:
    ///  * `error` is logged at ERROR level and will appear as the error title in Sentry.
    ///    We require it to be a static string, to ensure that Sentry always merges instances of
    ///    the same error together.
    ///  * `details` is logged at WARN level and will appear in the breadcrumbs.
    ///    Put relevant dynamic information here.
    ///
    /// The message that's logged at WARN level has the format
    ///   "[customer-data] {message} ({details})"
    /// We include the [customer-data] tag out of the expectation that `details` will always
    /// contain some sensitive customer data. We include the `message` to make it possible to match
    /// the breadcrumbs to their associated error in Sentry.
    ///
    // TODO(database-issues#5362): Rethink or justify our error logging strategy.
    pub fn log(&self, message: &'static str, details: &str) {
        tracing::warn!(
            dataflow = self.dataflow_name,
            "[customer-data] {message} ({details})"
        );
        tracing::error!(message);
    }

    /// Like [`Self::log`], but panics in debug mode.
    ///
    /// Use this method to notify about errors that are certainly caused by bugs in Materialize.
    pub fn soft_panic_or_log(&self, message: &'static str, details: &str) {
        tracing::warn!(
            dataflow = self.dataflow_name,
            "[customer-data] {message} ({details})"
        );
        mz_ore::soft_panic_or_log!("{}", message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_storage_types::errors::DataflowError;
    use proptest::prelude::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn proptest_roundtrip_canonical() {
        proptest!(|(err in any::<DataflowError>())| {
            let ser = DataflowErrorSer::from(err.clone());

            // Round-trip: ser -> deser -> ser must produce identical bytes.
            let deserialized = ser.deserialize();
            let re_serialized = DataflowErrorSer::from(deserialized);
            prop_assert_eq!(&ser, &re_serialized,
                "Canonicality violation: round-trip produced different bytes");

            // Equality: equal errors must produce equal bytes.
            let ser2 = DataflowErrorSer::from(err);
            prop_assert_eq!(&ser, &ser2,
                "Canonicality violation: same error produced different bytes");
        });
    }

    #[mz_ore::test]
    fn display_roundtrip() {
        let eval_err = EvalError::DivisionByZero;
        let dfe = DataflowError::from(eval_err.clone());
        let ser = DataflowErrorSer::from(eval_err);

        assert_eq!(dfe.to_string(), ser.to_string());
    }
}
