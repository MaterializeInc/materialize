// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for the persist crate.

#![warn(missing_docs)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use bytes::{BufMut, Bytes};
use mz_ore::url::SensitiveUrl;
use mz_proto::{RustType, TryFromProtoError};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::columnar::Schema2;

pub mod arrow;
pub mod codec_impls;
pub mod columnar;
pub mod parquet;
pub mod part;
pub mod schema;
pub mod stats;
pub mod stats2;
pub mod timestamp;
pub mod txn;

/// Encoding and decoding operations for a type usable as a persisted key or
/// value.
pub trait Codec: Default + Sized + PartialEq + 'static {
    /// The type of the associated schema for [Self].
    ///
    /// This is a separate type because Row is not self-describing. For Row, you
    /// need a RelationDesc to determine the types of any columns that are
    /// Datum::Null.
    type Schema: Schema2<Self> + PartialEq;

    /// Name of the codec.
    ///
    /// This name is stored for the key and value when a stream is first created
    /// and the same key and value codec must be used for that stream afterward.
    fn codec_name() -> String;
    /// Encode a key or value for permanent storage.
    ///
    /// This must perfectly round-trip Self through [Codec::decode]. If the
    /// encode function for this codec ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut;

    /// Encode a key or value to a Vec.
    ///
    /// This is a convenience function for calling [Self::encode] with a fresh
    /// `Vec` each time. Reuse an allocation when performance matters!
    fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = vec![];
        self.encode(&mut buf);
        buf
    }

    /// Decode a key or value previous encoded with this codec's
    /// [Codec::encode].
    ///
    /// This must perfectly round-trip Self through [Codec::encode]. If the
    /// encode function for this codec ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    ///
    /// It should also gracefully handle data encoded by future versions of
    /// encode (likely with an error).
    //
    // TODO: Mechanically, this could return a ref to the original bytes
    // without any copies, see if we can make the types work out for that.
    fn decode<'a>(buf: &'a [u8], schema: &Self::Schema) -> Result<Self, String>;

    /// A type used with [Self::decode_from] for allocation reuse. Set to `()`
    /// if unnecessary.
    type Storage: Default;
    /// An alternate form of [Self::decode] which enables amortizing allocs.
    ///
    /// First, instead of returning `Self`, it takes `&mut Self` as a parameter,
    /// allowing for reuse of the internal `Row`/`SourceData` allocations.
    ///
    /// Second, it adds a `type Storage` to `Codec` and also passes it in as
    /// `&mut Option<Self::Storage>`, allowing for reuse of
    /// `ProtoRow`/`ProtoSourceData` allocations. If `Some`, this impl may
    /// attempt to reuse allocations with it. It may also leave allocations in
    /// it for use in future calls to `decode_from`.
    ///
    /// A default implementation is provided for `Codec` impls that can't take
    /// advantage of this.
    fn decode_from<'a>(
        &mut self,
        buf: &'a [u8],
        _storage: &mut Option<Self::Storage>,
        schema: &Self::Schema,
    ) -> Result<(), String> {
        *self = Self::decode(buf, schema)?;
        Ok(())
    }

    /// Checks that the given value matches the provided schema.
    ///
    /// A no-op default implementation is provided for convenience.
    fn validate(_val: &Self, _schema: &Self::Schema) -> Result<(), String> {
        Ok(())
    }

    /// Encode a schema for permanent storage.
    ///
    /// This must perfectly round-trip the schema through [Self::decode_schema].
    /// If the encode_schema function ever changes, decode_schema must be able
    /// to handle bytes output by all previous versions of encode_schema.
    ///
    /// TODO: Move this to instead be a new trait that is required by
    /// Self::Schema?
    fn encode_schema(schema: &Self::Schema) -> Bytes;

    /// Decode a schema previous encoded with this codec's
    /// [Self::encode_schema].
    ///
    /// This must perfectly round-trip the schema through [Self::encode_schema].
    /// If the encode_schema function ever changes, decode_schema must be able
    /// to handle bytes output by all previous versions of encode_schema.
    fn decode_schema(buf: &Bytes) -> Self::Schema;
}

/// Encoding and decoding operations for a type usable as a persisted timestamp
/// or diff.
pub trait Codec64: Sized + Clone + 'static {
    /// Name of the codec.
    ///
    /// This name is stored for the timestamp and diff when a stream is first
    /// created and the same timestamp and diff codec must be used for that
    /// stream afterward.
    fn codec_name() -> String;

    /// Encode a timestamp or diff for permanent storage.
    ///
    /// This must perfectly round-trip Self through [Codec64::decode]. If the
    /// encode function for this codec ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    fn encode(&self) -> [u8; 8];

    /// Decode a timestamp or diff previous encoded with this codec's
    /// [Codec64::encode].
    ///
    /// This must perfectly round-trip Self through [Codec64::encode]. If the
    /// encode function for this codec ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    fn decode(buf: [u8; 8]) -> Self;
}

/// A location in s3, other cloud storage, or otherwise "durable storage" used
/// by persist.
///
/// This structure can be durably written down or transmitted for use by other
/// processes. This location can contain any number of persist shards.
#[derive(Arbitrary, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct PersistLocation {
    /// Uri string that identifies the blob store.
    pub blob_uri: SensitiveUrl,

    /// Uri string that identifies the consensus system.
    pub consensus_uri: SensitiveUrl,
}

impl PersistLocation {
    /// Returns a PersistLocation indicating in-mem blob and consensus.
    pub fn new_in_mem() -> Self {
        PersistLocation {
            blob_uri: "mem://".parse().unwrap(),
            consensus_uri: "mem://".parse().unwrap(),
        }
    }
}

/// An opaque identifier for a persist durable TVC (aka shard).
///
/// The [std::string::ToString::to_string] format of this may be stored durably
/// or otherwise used as an interchange format. It can be parsed back using
/// [str::parse] or [std::str::FromStr::from_str].
#[derive(
    Arbitrary, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct ShardId([u8; 16]);

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShardId({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for ShardId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uuid_encoded = match s.strip_prefix('s') {
            Some(x) => x,
            None => return Err(format!("invalid ShardId {}: incorrect prefix", s)),
        };
        let uuid = Uuid::parse_str(uuid_encoded)
            .map_err(|err| format!("invalid ShardId {}: {}", s, err))?;
        Ok(ShardId(*uuid.as_bytes()))
    }
}

impl From<ShardId> for String {
    fn from(shard_id: ShardId) -> Self {
        shard_id.to_string()
    }
}

impl TryFrom<String> for ShardId {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl ShardId {
    /// Returns a random [ShardId] that is reasonably likely to have never been
    /// generated before.
    pub fn new() -> Self {
        ShardId(Uuid::new_v4().as_bytes().to_owned())
    }
}

impl RustType<String> for ShardId {
    fn into_proto(&self) -> String {
        self.to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        match proto.parse() {
            Ok(x) => Ok(x),
            Err(_) => Err(TryFromProtoError::InvalidShardId(proto)),
        }
    }
}

/// An opaque fencing token used in compare_and_downgrade_since.
pub trait Opaque: PartialEq + Clone + Sized + 'static {
    /// The value of the opaque token when no compare_and_downgrade_since calls
    /// have yet been successful.
    fn initial() -> Self;
}

/// Advance a timestamp by the least amount possible such that
/// `ts.less_than(ts.step_forward())` is true.
///
/// TODO: Unify this with repr's TimestampManipulation. Or, ideally, get rid of
/// it entirely by making txn-wal methods take an `advance_to` argument.
pub trait StepForward {
    /// Advance a timestamp by the least amount possible such that
    /// `ts.less_than(ts.step_forward())` is true. Panic if unable to do so.
    fn step_forward(&self) -> Self;
}

impl StepForward for u64 {
    fn step_forward(&self) -> Self {
        self.checked_add(1).unwrap()
    }
}
