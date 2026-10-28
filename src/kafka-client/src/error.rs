// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error types.
//!
//! `Display` and `Debug` output is byte-identical to rust-rdkafka's, because
//! these strings reach users (SQL errors, source and sink statuses) and tests.

use std::error::Error;
use std::ffi;
use std::fmt;
use std::sync::Arc;

use crate::types::RDKafkaConfRes;
pub use crate::types::RDKafkaErrorCode;

/// Kafka result.
pub type KafkaResult<T> = Result<T, KafkaError>;

/// Verify if the value represents an error condition.
///
/// Some librdkafka codes are informational, rather than true errors.
pub trait IsError {
    /// Reports whether the value represents an error.
    fn is_error(&self) -> bool;
}

impl IsError for RDKafkaErrorCode {
    fn is_error(&self) -> bool {
        *self != RDKafkaErrorCode::NoError
    }
}

impl IsError for RDKafkaConfRes {
    fn is_error(&self) -> bool {
        *self != RDKafkaConfRes::RD_KAFKA_CONF_OK
    }
}

impl IsError for RDKafkaError {
    fn is_error(&self) -> bool {
        self.code() != RDKafkaErrorCode::NoError
    }
}

/// An error with a code, a human readable message and classification flags.
///
/// The equivalent of librdkafka's `rd_kafka_error_t`, used for transactional
/// producer and OAuth errors. Equality compares codes only, as in rust-rdkafka.
#[derive(Clone)]
pub struct RDKafkaError(Arc<RDKafkaErrorInner>);

#[derive(Clone)]
struct RDKafkaErrorInner {
    code: RDKafkaErrorCode,
    string: String,
    fatal: bool,
    retriable: bool,
    txn_requires_abort: bool,
}

impl RDKafkaError {
    /// Creates an error with no classification flags set.
    pub(crate) fn new(code: RDKafkaErrorCode, string: impl Into<String>) -> RDKafkaError {
        RDKafkaError(Arc::new(RDKafkaErrorInner {
            code,
            string: string.into(),
            fatal: false,
            retriable: false,
            txn_requires_abort: false,
        }))
    }

    /// Creates an error whose message is librdkafka's description of `code`.
    pub(crate) fn from_code(code: RDKafkaErrorCode) -> RDKafkaError {
        RDKafkaError::new(code, code.err2str())
    }

    fn with_flags(self, f: impl FnOnce(&mut RDKafkaErrorInner)) -> RDKafkaError {
        let mut inner = Arc::unwrap_or_clone(self.0);
        f(&mut inner);
        RDKafkaError(Arc::new(inner))
    }

    /// Marks the error as fatal: the client instance is no longer usable.
    pub(crate) fn mark_fatal(self) -> RDKafkaError {
        self.with_flags(|e| e.fatal = true)
    }

    /// Marks the operation that failed as retriable.
    pub(crate) fn mark_retriable(self) -> RDKafkaError {
        self.with_flags(|e| e.retriable = true)
    }

    /// Marks the error as abortable: the current transaction must be aborted.
    pub(crate) fn mark_txn_requires_abort(self) -> RDKafkaError {
        self.with_flags(|e| e.txn_requires_abort = true)
    }

    /// Returns the error code.
    pub fn code(&self) -> RDKafkaErrorCode {
        self.0.code
    }

    /// Returns librdkafka's name for the error code, e.g. `UNKNOWN_MEMBER_ID`
    /// or `_TIMED_OUT`.
    pub fn name(&self) -> String {
        self.0.code.name().to_string()
    }

    /// Returns a human readable error string.
    pub fn string(&self) -> String {
        self.0.string.clone()
    }

    /// Reports whether the error is a fatal error.
    ///
    /// A fatal error indicates that the client instance is no longer usable.
    pub fn is_fatal(&self) -> bool {
        self.0.fatal
    }

    /// Reports whether the operation that encountered the error can be retried.
    pub fn is_retriable(&self) -> bool {
        self.0.retriable
    }

    /// Reports whether the error is an abortable transaction error.
    pub fn txn_requires_abort(&self) -> bool {
        self.0.txn_requires_abort
    }
}

impl PartialEq for RDKafkaError {
    fn eq(&self, other: &RDKafkaError) -> bool {
        self.code() == other.code()
    }
}

impl Eq for RDKafkaError {}

impl fmt::Debug for RDKafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RDKafkaError({})", self)
    }
}

impl fmt::Display for RDKafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.string)
    }
}

impl Error for RDKafkaError {}

/// Represents all possible Kafka errors.
///
/// If applicable, check the underlying [`RDKafkaErrorCode`] to get details.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KafkaError {
    /// Creation of admin operation failed.
    AdminOpCreation(String),
    /// The admin operation itself failed.
    AdminOp(RDKafkaErrorCode),
    /// The client was dropped before the operation completed.
    Canceled,
    /// Invalid client configuration: result, error description, key, value.
    ClientConfig(RDKafkaConfRes, String, String, String),
    /// Client creation failed.
    ClientCreation(String),
    /// Consumer commit failed.
    ConsumerCommit(RDKafkaErrorCode),
    /// Flushing failed
    Flush(RDKafkaErrorCode),
    /// Global error.
    Global(RDKafkaErrorCode),
    /// Group list fetch failed.
    GroupListFetch(RDKafkaErrorCode),
    /// Message consumption failed.
    MessageConsumption(RDKafkaErrorCode),
    /// Message consumption failed with fatal error.
    MessageConsumptionFatal(RDKafkaErrorCode),
    /// Message production error.
    MessageProduction(RDKafkaErrorCode),
    /// Metadata fetch error.
    MetadataFetch(RDKafkaErrorCode),
    /// No message was received.
    NoMessageReceived,
    /// Unexpected null pointer
    Nul(ffi::NulError),
    /// OAuth configuration failed.
    OAuthConfig(RDKafkaError),
    /// Offset fetch failed.
    OffsetFetch(RDKafkaErrorCode),
    /// End of partition reached.
    PartitionEOF(i32),
    /// Pause/Resume failed.
    PauseResume(String),
    /// Rebalance failed.
    Rebalance(RDKafkaErrorCode),
    /// Seeking a partition failed.
    Seek(String),
    /// Setting partition offset failed.
    SetPartitionOffset(RDKafkaErrorCode),
    /// Offset store failed.
    StoreOffset(RDKafkaErrorCode),
    /// Subscription creation failed.
    Subscription(String),
    /// Transaction error.
    Transaction(RDKafkaError),
}

impl fmt::Debug for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaError::AdminOp(err) => write!(f, "KafkaError (Admin operation error: {})", err),
            KafkaError::AdminOpCreation(err) => {
                write!(f, "KafkaError (Admin operation creation error: {})", err)
            }
            KafkaError::Canceled => write!(f, "KafkaError (Client dropped)"),
            KafkaError::ClientConfig(_, desc, key, value) => write!(
                f,
                "KafkaError (Client config error: {} {} {})",
                desc, key, value
            ),
            KafkaError::ClientCreation(err) => {
                write!(f, "KafkaError (Client creation error: {})", err)
            }
            KafkaError::ConsumerCommit(err) => {
                write!(f, "KafkaError (Consumer commit error: {})", err)
            }
            KafkaError::Flush(err) => write!(f, "KafkaError (Flush error: {})", err),
            KafkaError::Global(err) => write!(f, "KafkaError (Global error: {})", err),
            KafkaError::GroupListFetch(err) => {
                write!(f, "KafkaError (Group list fetch error: {})", err)
            }
            KafkaError::MessageConsumption(err) => {
                write!(f, "KafkaError (Message consumption error: {})", err)
            }
            KafkaError::MessageConsumptionFatal(err) => {
                write!(f, "KafkaError (Message consumption fatal error: {})", err)
            }
            KafkaError::MessageProduction(err) => {
                write!(f, "KafkaError (Message production error: {})", err)
            }
            KafkaError::MetadataFetch(err) => {
                write!(f, "KafkaError (Metadata fetch error: {})", err)
            }
            KafkaError::NoMessageReceived => {
                write!(f, "No message received within the given poll interval")
            }
            KafkaError::Nul(_) => write!(f, "FFI null error"),
            KafkaError::OAuthConfig(err) => write!(f, "KafkaError (OAuth config error: {})", err),
            KafkaError::OffsetFetch(err) => write!(f, "KafkaError (Offset fetch error: {})", err),
            KafkaError::PartitionEOF(part_n) => write!(f, "KafkaError (Partition EOF: {})", part_n),
            KafkaError::PauseResume(err) => {
                write!(f, "KafkaError (Pause/resume error: {})", err)
            }
            KafkaError::Rebalance(err) => write!(f, "KafkaError (Rebalance error: {})", err),
            KafkaError::Seek(err) => write!(f, "KafkaError (Seek error: {})", err),
            KafkaError::SetPartitionOffset(err) => {
                write!(f, "KafkaError (Set partition offset error: {})", err)
            }
            KafkaError::StoreOffset(err) => write!(f, "KafkaError (Store offset error: {})", err),
            KafkaError::Subscription(err) => {
                write!(f, "KafkaError (Subscription error: {})", err)
            }
            KafkaError::Transaction(err) => write!(f, "KafkaError (Transaction error: {})", err),
        }
    }
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaError::AdminOp(err) => write!(f, "Admin operation error: {}", err),
            KafkaError::AdminOpCreation(err) => {
                write!(f, "Admin operation creation error: {}", err)
            }
            KafkaError::Canceled => write!(f, "KafkaError (Client dropped)"),
            KafkaError::ClientConfig(_, desc, key, value) => {
                write!(f, "Client config error: {} {} {}", desc, key, value)
            }
            KafkaError::ClientCreation(err) => write!(f, "Client creation error: {}", err),
            KafkaError::ConsumerCommit(err) => write!(f, "Consumer commit error: {}", err),
            KafkaError::Flush(err) => write!(f, "Flush error: {}", err),
            KafkaError::Global(err) => write!(f, "Global error: {}", err),
            KafkaError::GroupListFetch(err) => write!(f, "Group list fetch error: {}", err),
            KafkaError::MessageConsumption(err) => write!(f, "Message consumption error: {}", err),
            KafkaError::MessageConsumptionFatal(err) => {
                write!(f, "Message consumption fatal error: {}", err)
            }
            KafkaError::MessageProduction(err) => write!(f, "Message production error: {}", err),
            KafkaError::MetadataFetch(err) => write!(f, "Meta data fetch error: {}", err),
            KafkaError::NoMessageReceived => {
                write!(f, "No message received within the given poll interval")
            }
            KafkaError::Nul(_) => write!(f, "FFI nul error"),
            KafkaError::OAuthConfig(err) => write!(f, "OAuth config error: {}", err),
            KafkaError::OffsetFetch(err) => write!(f, "Offset fetch error: {}", err),
            KafkaError::PartitionEOF(part_n) => write!(f, "Partition EOF: {}", part_n),
            KafkaError::PauseResume(err) => write!(f, "Pause/resume error: {}", err),
            KafkaError::Rebalance(err) => write!(f, "Rebalance error: {}", err),
            KafkaError::Seek(err) => write!(f, "Seek error: {}", err),
            KafkaError::SetPartitionOffset(err) => write!(f, "Set partition offset error: {}", err),
            KafkaError::StoreOffset(err) => write!(f, "Store offset error: {}", err),
            KafkaError::Subscription(err) => write!(f, "Subscription error: {}", err),
            KafkaError::Transaction(err) => write!(f, "Transaction error: {}", err),
        }
    }
}

impl Error for KafkaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            KafkaError::AdminOp(_)
            | KafkaError::AdminOpCreation(_)
            | KafkaError::Canceled
            | KafkaError::ClientConfig(..)
            | KafkaError::ClientCreation(_)
            | KafkaError::NoMessageReceived
            | KafkaError::Nul(_)
            | KafkaError::PartitionEOF(_)
            | KafkaError::PauseResume(_)
            | KafkaError::Seek(_)
            | KafkaError::Subscription(_) => None,
            KafkaError::ConsumerCommit(err)
            | KafkaError::Flush(err)
            | KafkaError::Global(err)
            | KafkaError::GroupListFetch(err)
            | KafkaError::MessageConsumption(err)
            | KafkaError::MessageConsumptionFatal(err)
            | KafkaError::MessageProduction(err)
            | KafkaError::MetadataFetch(err)
            | KafkaError::OffsetFetch(err)
            | KafkaError::Rebalance(err)
            | KafkaError::SetPartitionOffset(err)
            | KafkaError::StoreOffset(err) => Some(err),
            KafkaError::OAuthConfig(err) | KafkaError::Transaction(err) => Some(err),
        }
    }
}

impl From<ffi::NulError> for KafkaError {
    fn from(err: ffi::NulError) -> KafkaError {
        KafkaError::Nul(err)
    }
}

impl KafkaError {
    /// Returns the [`RDKafkaErrorCode`] underlying this error, if any.
    pub fn rdkafka_error_code(&self) -> Option<RDKafkaErrorCode> {
        match self {
            KafkaError::AdminOp(_)
            | KafkaError::AdminOpCreation(_)
            | KafkaError::Canceled
            | KafkaError::ClientConfig(..)
            | KafkaError::ClientCreation(_)
            | KafkaError::NoMessageReceived
            | KafkaError::Nul(_)
            | KafkaError::PartitionEOF(_)
            | KafkaError::PauseResume(_)
            | KafkaError::Seek(_)
            | KafkaError::Subscription(_) => None,
            KafkaError::ConsumerCommit(err)
            | KafkaError::Flush(err)
            | KafkaError::Global(err)
            | KafkaError::GroupListFetch(err)
            | KafkaError::MessageConsumption(err)
            | KafkaError::MessageConsumptionFatal(err)
            | KafkaError::MessageProduction(err)
            | KafkaError::MetadataFetch(err)
            | KafkaError::OffsetFetch(err)
            | KafkaError::Rebalance(err)
            | KafkaError::SetPartitionOffset(err)
            | KafkaError::StoreOffset(err) => Some(*err),
            KafkaError::OAuthConfig(err) | KafkaError::Transaction(err) => Some(err.code()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn display_matches_rdkafka() {
        let cases = [
            (
                KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure),
                "Meta data fetch error: BrokerTransportFailure (Local: Broker transport failure)",
                "KafkaError (Metadata fetch error: BrokerTransportFailure (Local: Broker \
                 transport failure))",
            ),
            (
                KafkaError::AdminOp(RDKafkaErrorCode::InvalidConfig),
                "Admin operation error: InvalidConfig (Broker: Configuration is invalid)",
                "KafkaError (Admin operation error: InvalidConfig (Broker: Configuration is \
                 invalid))",
            ),
            (
                KafkaError::ClientConfig(
                    RDKafkaConfRes::RD_KAFKA_CONF_UNKNOWN,
                    "No such configuration property: \"foo\"".into(),
                    "foo".into(),
                    "bar".into(),
                ),
                "Client config error: No such configuration property: \"foo\" foo bar",
                "KafkaError (Client config error: No such configuration property: \"foo\" foo \
                 bar)",
            ),
            (
                KafkaError::ClientCreation("Unsupported SASL mechanism: GARBAGE".into()),
                "Client creation error: Unsupported SASL mechanism: GARBAGE",
                "KafkaError (Client creation error: Unsupported SASL mechanism: GARBAGE)",
            ),
            (
                KafkaError::PartitionEOF(3),
                "Partition EOF: 3",
                "KafkaError (Partition EOF: 3)",
            ),
            (
                KafkaError::Canceled,
                "KafkaError (Client dropped)",
                "KafkaError (Client dropped)",
            ),
            (
                KafkaError::Transaction(
                    RDKafkaError::new(
                        RDKafkaErrorCode::OperationTimedOut,
                        "Timed out waiting for operation to finish, retry call to resume",
                    )
                    .mark_retriable(),
                ),
                "Transaction error: Timed out waiting for operation to finish, retry call to \
                 resume",
                "KafkaError (Transaction error: Timed out waiting for operation to finish, \
                 retry call to resume)",
            ),
        ];
        for (err, display, debug) in cases {
            assert_eq!(err.to_string(), display);
            assert_eq!(format!("{err:?}"), debug);
        }
    }

    #[mz_ore::test]
    fn rdkafka_error_flags() {
        let err = RDKafkaError::from_code(RDKafkaErrorCode::ProducerFenced).mark_fatal();
        assert!(err.is_fatal());
        assert!(!err.is_retriable());
        assert!(!err.txn_requires_abort());
        assert_eq!(err.code(), RDKafkaErrorCode::ProducerFenced);
        assert_eq!(err.name(), "PRODUCER_FENCED");
        assert_eq!(
            err.string(),
            "Broker: There is a newer producer with the same transactionalId which fences the \
             current one"
        );
        let shared = err.clone();
        let abortable = err.mark_txn_requires_abort();
        assert!(abortable.is_fatal() && abortable.txn_requires_abort());
        assert!(!shared.txn_requires_abort());

        let txn =
            KafkaError::Transaction(RDKafkaError::new(RDKafkaErrorCode::OperationTimedOut, "x"));
        assert_eq!(
            txn.rdkafka_error_code(),
            Some(RDKafkaErrorCode::OperationTimedOut)
        );
        assert_eq!(KafkaError::PartitionEOF(1).rdkafka_error_code(), None);
    }
}
