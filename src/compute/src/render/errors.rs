// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for handling errors encountered by operators.

use mz_repr::Row;

pub use mz_compute_types::errors::DataflowErrorSer;

impl crate::sink::correction_v2::DataBytes for DataflowErrorSer {
    fn data_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.byte_len()
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
    use mz_expr::EvalError;
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
