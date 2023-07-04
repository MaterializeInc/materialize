// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for describing dataflow sources.

use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::RelationType;
use mz_storage_client::controller::CollectionMetadata;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_compute_client.types.sources.rs"
));

/// A description of an instantiation of a source.
///
/// This includes a description of the source, but additionally any
/// context-dependent options like the ability to apply filtering and
/// projection to the records as they emerge.
#[derive(Arbitrary, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceDesc<M> {
    /// Arguments for this instantiation of the source.
    pub arguments: SourceInstanceArguments,
    /// Additional metadata used by the storage client of a compute instance to read it.
    pub storage_metadata: M,
    /// The relation type of this source
    pub typ: RelationType,
}

impl RustType<ProtoSourceInstanceDesc> for SourceInstanceDesc<CollectionMetadata> {
    fn into_proto(&self) -> ProtoSourceInstanceDesc {
        ProtoSourceInstanceDesc {
            arguments: Some(self.arguments.into_proto()),
            storage_metadata: Some(self.storage_metadata.into_proto()),
            typ: Some(self.typ.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceInstanceDesc) -> Result<Self, TryFromProtoError> {
        Ok(SourceInstanceDesc {
            arguments: proto
                .arguments
                .into_rust_if_some("ProtoSourceInstanceDesc::arguments")?,
            storage_metadata: proto
                .storage_metadata
                .into_rust_if_some("ProtoSourceInstanceDesc::storage_metadata")?,
            typ: proto
                .typ
                .into_rust_if_some("ProtoSourceInstanceDesc::typ")?,
        })
    }
}

/// Per-source construction arguments.
#[derive(Arbitrary, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceArguments {
    /// Linear operators to be applied record-by-record.
    pub operators: Option<mz_expr::MapFilterProject>,
}

impl RustType<ProtoSourceInstanceArguments> for SourceInstanceArguments {
    fn into_proto(&self) -> ProtoSourceInstanceArguments {
        ProtoSourceInstanceArguments {
            operators: self.operators.into_proto(),
        }
    }

    fn from_proto(proto: ProtoSourceInstanceArguments) -> Result<Self, TryFromProtoError> {
        Ok(SourceInstanceArguments {
            operators: proto.operators.into_rust()?,
        })
    }
}
