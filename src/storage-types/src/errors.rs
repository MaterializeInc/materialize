// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use std::error::Error;
use std::fmt::Display;

use bytes::BufMut;
use mz_expr::EvalError;
use mz_kafka_util::client::TunnelingClientContext;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, Row};
use mz_ssh_util::tunnel::SshTunnelStatus;
#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use prost::Message;
use rdkafka::error::KafkaError;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.errors.rs"));

/// The underlying data was not decodable in the format we expected: eg.
/// invalid JSON or Avro data that doesn't match a schema.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct DecodeError {
    pub kind: DecodeErrorKind,
    pub raw: Vec<u8>,
}

impl RustType<ProtoDecodeError> for DecodeError {
    fn into_proto(&self) -> ProtoDecodeError {
        ProtoDecodeError {
            kind: Some(RustType::into_proto(&self.kind)),
            raw: Some(self.raw.clone()),
        }
    }

    fn from_proto(proto: ProtoDecodeError) -> Result<Self, TryFromProtoError> {
        let kind = match proto.kind {
            Some(kind) => RustType::from_proto(kind)?,
            None => return Err(TryFromProtoError::missing_field("ProtoDecodeError::kind")),
        };
        let raw = proto.raw.into_rust_if_some("raw")?;
        Ok(Self { kind, raw })
    }
}

impl DecodeError {
    pub fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors")
    }

    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoDecodeError::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // See if we can output the bytes that failed to decode as a string.
        let str_repr = std::str::from_utf8(&self.raw).ok();
        // strip any NUL characters from the str_repr to prevent an error
        // in the postgres protocol decoding
        let str_repr = str_repr.map(|s| s.replace('\0', "NULL"));
        let bytes_repr = hex::encode(&self.raw);
        match str_repr {
            Some(s) => write!(
                f,
                "{} (original text: {}, original bytes: {:x?})",
                self.kind, s, bytes_repr
            ),
            None => write!(f, "{} (original bytes: {:x?})", self.kind, bytes_repr),
        }
    }
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum DecodeErrorKind {
    Text(Box<str>),
    Bytes(Box<str>),
}

impl RustType<ProtoDecodeErrorKind> for DecodeErrorKind {
    fn into_proto(&self) -> ProtoDecodeErrorKind {
        use proto_decode_error_kind::Kind::*;
        ProtoDecodeErrorKind {
            kind: Some(match self {
                DecodeErrorKind::Text(v) => Text(v.into_proto()),
                DecodeErrorKind::Bytes(v) => Bytes(v.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoDecodeErrorKind) -> Result<Self, TryFromProtoError> {
        use proto_decode_error_kind::Kind::*;
        match proto.kind {
            Some(Text(v)) => Ok(DecodeErrorKind::Text(v.into())),
            Some(Bytes(v)) => Ok(DecodeErrorKind::Bytes(v.into())),
            None => Err(TryFromProtoError::missing_field("ProtoDecodeError::kind")),
        }
    }
}

impl Display for DecodeErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeErrorKind::Text(e) => f.write_str(e),
            DecodeErrorKind::Bytes(e) => f.write_str(e),
        }
    }
}

/// Errors arising during envelope processing.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    Deserialize,
    Serialize,
    PartialEq,
    Hash
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum EnvelopeError {
    /// An error that can be retracted by a future message using upsert logic.
    Upsert(UpsertError),
    /// Errors corresponding to `ENVELOPE NONE`. Naming this
    /// `None`, though, would have been too confusing.
    Flat(Box<str>),
}

impl RustType<ProtoEnvelopeErrorV1> for EnvelopeError {
    fn into_proto(&self) -> ProtoEnvelopeErrorV1 {
        use proto_envelope_error_v1::Kind;
        ProtoEnvelopeErrorV1 {
            kind: Some(match self {
                EnvelopeError::Upsert(rust) => Kind::Upsert(rust.into_proto()),
                EnvelopeError::Flat(text) => Kind::Flat(text.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoEnvelopeErrorV1) -> Result<Self, TryFromProtoError> {
        use proto_envelope_error_v1::Kind;
        match proto.kind {
            Some(Kind::Upsert(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::Upsert(rust))
            }
            Some(Kind::Flat(text)) => Ok(Self::Flat(text.into())),
            None => Err(TryFromProtoError::missing_field(
                "ProtoEnvelopeErrorV1::kind",
            )),
        }
    }
}

impl Display for EnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvelopeError::Upsert(err) => write!(f, "Upsert: {err}"),
            EnvelopeError::Flat(err) => write!(f, "Flat: {err}"),
        }
    }
}

/// An error from a value in an upsert source. The corresponding key is included, allowing
/// us to reconstruct their entry in the upsert map upon restart.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct UpsertValueError {
    /// The underlying error.
    pub inner: DecodeError,
    /// The (good) key associated with the errored value.
    pub for_key: Row,
}

impl RustType<ProtoUpsertValueError> for UpsertValueError {
    fn into_proto(&self) -> ProtoUpsertValueError {
        ProtoUpsertValueError {
            inner: Some(self.inner.into_proto()),
            for_key: Some(self.for_key.into_proto()),
        }
    }

    fn from_proto(proto: ProtoUpsertValueError) -> Result<Self, TryFromProtoError> {
        Ok(UpsertValueError {
            inner: proto
                .inner
                .into_rust_if_some("ProtoUpsertValueError::inner")?,
            for_key: proto
                .for_key
                .into_rust_if_some("ProtoUpsertValueError::for_key")?,
        })
    }
}

impl Display for UpsertValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let UpsertValueError { inner, for_key } = self;
        write!(f, "{inner}, decoded key: {for_key:?}")?;
        Ok(())
    }
}

/// A source contained a record with a NULL key, which we don't support.
#[derive(
    Ord,
    PartialOrd,
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct UpsertNullKeyError;

impl RustType<ProtoUpsertNullKeyError> for UpsertNullKeyError {
    fn into_proto(&self) -> ProtoUpsertNullKeyError {
        ProtoUpsertNullKeyError {}
    }

    fn from_proto(_proto: ProtoUpsertNullKeyError) -> Result<Self, TryFromProtoError> {
        Ok(UpsertNullKeyError)
    }
}

impl Display for UpsertNullKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "record with NULL key in UPSERT source; to retract this error, "
        )?;
        write!(
            f,
            "produce a record upstream with a NULL key and NULL value"
        )
    }
}

/// An error that can be retracted by a future message using upsert logic.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum UpsertError {
    /// Wrapper around a key decoding error.
    /// We use this instead of emitting the underlying `DataflowError::DecodeError` because with only
    /// the underlying error, we can't distinguish between an error with the key and an error
    /// with the value.
    ///
    /// It is necessary to distinguish them because the necessary record to retract them is different.
    /// `(K, <errored V>)` is retracted by `(K, null)`, whereas `(<errored K>, anything)` is retracted by
    /// `("bytes", null)`, where "bytes" is the string that failed to correctly decode as a key.
    KeyDecode(DecodeError),
    /// Wrapper around an error related to the value.
    Value(UpsertValueError),
    NullKey(UpsertNullKeyError),
}

impl RustType<ProtoUpsertError> for UpsertError {
    fn into_proto(&self) -> ProtoUpsertError {
        use proto_upsert_error::Kind;
        ProtoUpsertError {
            kind: Some(match self {
                UpsertError::KeyDecode(err) => Kind::KeyDecode(err.into_proto()),
                UpsertError::Value(err) => Kind::Value(err.into_proto()),
                UpsertError::NullKey(err) => Kind::NullKey(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoUpsertError) -> Result<Self, TryFromProtoError> {
        use proto_upsert_error::Kind;
        match proto.kind {
            Some(Kind::KeyDecode(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::KeyDecode(rust))
            }
            Some(Kind::Value(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::Value(rust))
            }
            Some(Kind::NullKey(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::NullKey(rust))
            }
            None => Err(TryFromProtoError::missing_field("ProtoUpsertError::kind")),
        }
    }
}

impl Display for UpsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpsertError::KeyDecode(err) => write!(f, "Key decode: {err}"),
            UpsertError::Value(err) => write!(f, "Value error: {err}"),
            UpsertError::NullKey(err) => write!(f, "Null key: {err}"),
        }
    }
}

/// Source-wide durable errors; for example, a replication log being meaningless or corrupted.
/// This should _not_ include transient source errors, like connection issues or misconfigurations.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct SourceError {
    pub error: SourceErrorDetails,
    pub hint: Option<Box<str>>,
}

impl RustType<ProtoSourceError> for SourceError {
    fn into_proto(&self) -> ProtoSourceError {
        ProtoSourceError {
            error: Some(self.error.into_proto()),
            hint: self.hint.as_ref().map(|hint| hint.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceError) -> Result<Self, TryFromProtoError> {
        Ok(SourceError {
            error: proto.error.into_rust_if_some("ProtoSourceError::error")?,
            hint: proto.hint.map(Into::into),
        })
    }
}

impl Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum SourceErrorDetails {
    Initialization(Box<str>),
    Other(Box<str>),
}

impl RustType<ProtoSourceErrorDetails> for SourceErrorDetails {
    fn into_proto(&self) -> ProtoSourceErrorDetails {
        use proto_source_error_details::Kind;
        ProtoSourceErrorDetails {
            kind: Some(match self {
                SourceErrorDetails::Initialization(s) => Kind::Initialization(s.into_proto()),
                SourceErrorDetails::Other(s) => Kind::Other(s.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceErrorDetails) -> Result<Self, TryFromProtoError> {
        use proto_source_error_details::Kind;
        match proto.kind {
            Some(kind) => match kind {
                Kind::Initialization(s) => Ok(SourceErrorDetails::Initialization(s.into())),
                Kind::DeprecatedFileIo(s) | Kind::DeprecatedPersistence(s) => {
                    warn!("Deprecated source error kind: {s}");
                    Ok(SourceErrorDetails::Other(s.into()))
                }
                Kind::Other(s) => Ok(SourceErrorDetails::Other(s.into())),
            },
            None => Err(TryFromProtoError::missing_field(
                "ProtoSourceErrorDetails::kind",
            )),
        }
    }
}

impl Display for SourceErrorDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceErrorDetails::Initialization(e) => {
                write!(
                    f,
                    "failed during initialization, source must be dropped and recreated: {}",
                    e
                )
            }
            SourceErrorDetails::Other(e) => {
                write!(
                    f,
                    "source must be dropped and recreated due to failure: {}",
                    e
                )
            }
        }
    }
}

/// An error that's destined to be presented to the user in a differential dataflow collection.
/// For example, a divide by zero will be visible in the error collection for a particular row.
///
/// All of the variants are boxed to minimize the memory size of `DataflowError`. This type is
/// likely to appear in `Result<Row, DataflowError>`s on high-throughput code paths, so keeping its
/// size less than or equal to that of `Row` is important to ensure we are not wasting memory.
#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    Deserialize,
    Serialize,
    PartialEq,
    Hash
)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum DataflowError {
    DecodeError(Box<DecodeError>),
    EvalError(Box<EvalError>),
    SourceError(Box<SourceError>),
    EnvelopeError(Box<EnvelopeError>),
}

impl Error for DataflowError {}

impl RustType<ProtoDataflowError> for DataflowError {
    fn into_proto(&self) -> ProtoDataflowError {
        use proto_dataflow_error::Kind::*;
        ProtoDataflowError {
            kind: Some(match self {
                DataflowError::DecodeError(err) => DecodeError(*err.into_proto()),
                DataflowError::EvalError(err) => EvalError(*err.into_proto()),
                DataflowError::SourceError(err) => SourceError(*err.into_proto()),
                DataflowError::EnvelopeError(err) => EnvelopeErrorV1(*err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoDataflowError) -> Result<Self, TryFromProtoError> {
        use proto_dataflow_error::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                DecodeError(err) => Ok(DataflowError::DecodeError(Box::new(err.into_rust()?))),
                EvalError(err) => Ok(DataflowError::EvalError(Box::new(err.into_rust()?))),
                SourceError(err) => Ok(DataflowError::SourceError(Box::new(err.into_rust()?))),
                EnvelopeErrorV1(err) => {
                    Ok(DataflowError::EnvelopeError(Box::new(err.into_rust()?)))
                }
            },
            None => Err(TryFromProtoError::missing_field("ProtoDataflowError::kind")),
        }
    }
}

impl Display for DataflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataflowError::DecodeError(e) => write!(f, "Decode error: {}", e),
            DataflowError::EvalError(e) => write!(
                f,
                "{}{}",
                match **e {
                    EvalError::IfNullError(_) => "",
                    _ => "Evaluation error: ",
                },
                e
            ),
            DataflowError::SourceError(e) => write!(f, "Source error: {}", e),
            DataflowError::EnvelopeError(e) => write!(f, "Envelope error: {}", e),
        }
    }
}

impl From<DecodeError> for DataflowError {
    fn from(e: DecodeError) -> Self {
        Self::DecodeError(Box::new(e))
    }
}

impl From<EvalError> for DataflowError {
    fn from(e: EvalError) -> Self {
        Self::EvalError(Box::new(e))
    }
}

impl From<SourceError> for DataflowError {
    fn from(e: SourceError) -> Self {
        Self::SourceError(Box::new(e))
    }
}

impl From<EnvelopeError> for DataflowError {
    fn from(e: EnvelopeError) -> Self {
        Self::EnvelopeError(Box::new(e))
    }
}

/// An error returned by `KafkaConnection::create_with_context`.
#[derive(thiserror::Error, Debug)]
pub enum ContextCreationError {
    // This ends up double-printing `ssh:` in status tables, but makes for
    // a better experience during ddl.
    #[error("ssh: {0}")]
    Ssh(#[source] anyhow::Error),
    #[error(transparent)]
    KafkaError(#[from] KafkaError),
    #[error(transparent)]
    Dns(#[from] mz_ore::netio::DnsResolutionError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// An extension trait for `Result<T, E>` that makes producing `ContextCreationError`s easier.
pub trait ContextCreationErrorExt<T> {
    /// Override the error case with an ssh error from `cx`, if there is one.
    fn check_ssh_status<C>(self, cx: &TunnelingClientContext<C>)
    -> Result<T, ContextCreationError>;
    /// Add context to the errors within the variants of `ContextCreationError`, without
    /// altering the `Ssh` variant.
    fn add_context(self, msg: &'static str) -> Result<T, ContextCreationError>;
}

impl<T, E> ContextCreationErrorExt<T> for Result<T, E>
where
    ContextCreationError: From<E>,
{
    fn check_ssh_status<C>(
        self,
        cx: &TunnelingClientContext<C>,
    ) -> Result<T, ContextCreationError> {
        self.map_err(|e| {
            if let SshTunnelStatus::Errored(e) = cx.tunnel_status() {
                ContextCreationError::Ssh(anyhow!(e))
            } else {
                ContextCreationError::from(e)
            }
        })
    }

    fn add_context(self, msg: &'static str) -> Result<T, ContextCreationError> {
        self.map_err(|e| {
            let e = ContextCreationError::from(e);
            match e {
                // We need to preserve the `Ssh` variant here, so we add the context inside of it.
                ContextCreationError::Ssh(e) => ContextCreationError::Ssh(anyhow!(e.context(msg))),
                ContextCreationError::Other(e) => {
                    ContextCreationError::Other(anyhow!(e.context(msg)))
                }
                ContextCreationError::KafkaError(e) => {
                    ContextCreationError::Other(anyhow!(anyhow!(e).context(msg)))
                }
                ContextCreationError::Io(e) => {
                    ContextCreationError::Other(anyhow!(anyhow!(e).context(msg)))
                }
                ContextCreationError::Dns(e) => {
                    ContextCreationError::Other(anyhow!(e).context(msg))
                }
            }
        })
    }
}

impl From<CsrConnectError> for ContextCreationError {
    fn from(csr: CsrConnectError) -> ContextCreationError {
        use ContextCreationError::*;

        match csr {
            CsrConnectError::Ssh(e) => Ssh(e),
            other => Other(anyhow::anyhow!(other)),
        }
    }
}

/// An error returned by `CsrConnection::connect`.
#[derive(thiserror::Error, Debug)]
pub enum CsrConnectError {
    // This ends up double-printing `ssh:` in status tables, but makes for
    // a better experience during ddl.
    #[error("ssh: {0}")]
    Ssh(#[source] anyhow::Error),
    #[error(transparent)]
    Tls(#[from] mz_ccsr::tls::TlsError),
    #[error(transparent)]
    Dns(#[from] mz_ore::netio::DnsResolutionError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Error returned in response to a reference to an unknown collection.
#[derive(Error, Debug)]
#[error("collection does not exist: {0}")]
pub struct CollectionMissing(pub GlobalId);

#[cfg(test)]
mod tests {
    use crate::errors::DecodeErrorKind;

    use super::DecodeError;

    #[mz_ore::test]
    fn test_decode_error_codec_roundtrip() -> Result<(), String> {
        let original = DecodeError {
            kind: DecodeErrorKind::Text("ciao".into()),
            raw: b"oaic".to_vec(),
        };
        let mut encoded = Vec::new();
        original.encode(&mut encoded);
        let decoded = DecodeError::decode(&encoded)?;

        assert_eq!(decoded, original);

        Ok(())
    }
}
