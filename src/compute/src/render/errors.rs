// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for handling errors encountered by operators.

use std::hash::Hash;
use std::rc::Weak;

use differential_dataflow::ExchangeData;
use mz_repr::Row;
use timely::container::columnation::Columnation;

/// Used to make possibly-validating code generic: think of this as a kind of `MaybeResult`,
/// specialized for use in compute.  Validation code will only run when the error constructor is
/// Some.
pub(super) trait MaybeValidatingRow<T, E>: ExchangeData + Columnation + Hash {
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

impl<E, R> MaybeValidatingRow<Vec<R>, E> for Vec<R>
where
    R: ExchangeData + Columnation + Hash,
{
    fn ok(t: Vec<R>) -> Self {
        t
    }

    fn into_error() -> Option<fn(E) -> Self> {
        None
    }
}

impl<T, E> MaybeValidatingRow<T, E> for Result<T, E>
where
    T: ExchangeData + Columnation + Hash,
    E: ExchangeData + Columnation + Hash,
{
    fn ok(row: T) -> Self {
        Ok(row)
    }

    fn into_error() -> Option<fn(E) -> Self> {
        Some(Err)
    }
}

/// Error logger to be used by rendering code.
///
/// Holds onto a token to ensure that no false-positive errors are logged while the dataflow is in
/// the process of shutting down.
#[derive(Clone)]
pub(super) struct ErrorLogger {
    token: Option<Weak<()>>,
}

impl ErrorLogger {
    pub fn new(token: Option<Weak<()>>) -> Self {
        Self { token }
    }

    fn token_alive(&self) -> bool {
        match &self.token {
            Some(t) => t.upgrade().is_some(),
            None => true,
        }
    }

    /// Log the given error, unless the dataflow is shutting down.
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
    // TODO(#18214): Rethink or justify our error logging strategy.
    pub fn log(&self, message: &'static str, details: &str) {
        if self.token_alive() {
            self.log_always(message, details);
        }
    }

    /// Like [`Self::log`], but also logs errors when the dataflow is shutting down.
    ///
    /// Use this method to notify about errors that cannot be caused by dataflow shutdown.
    pub fn log_always(&self, message: &'static str, details: &str) {
        tracing::warn!("[customer-data] {message} ({details})");
        tracing::error!(message);
    }

    /// Like [`Self::log_always`], but panics in debug mode.
    ///
    /// Use this method to notify about errors that are certainly caused by bugs in Materialize.
    pub fn soft_panic_or_log(&self, message: &'static str, details: &str) {
        tracing::warn!("[customer-data] {message} ({details})");
        mz_ore::soft_panic_or_log!("{}", message);
    }
}
