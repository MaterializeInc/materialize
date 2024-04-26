// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for describing dataflow sinks.

use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc};
use mz_storage_types::controller::CollectionMetadata;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_compute_types.sinks.rs"));

/// A continual task that does insertions and deletions.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContinualTaskInsertDesc<S: 'static = ()> {
    /// The object we're sinking into the target table.
    pub from: GlobalId,
    /// Description for the sinked object.
    pub from_desc: RelationDesc,
    /// The table that we're ingesting into.
    pub target_table: PersistTableConnection<S>,
    /// The table that we're retracting from.
    pub retract_from_table: PersistTableConnection<S>,
}

impl Arbitrary for ContinualTaskInsertDesc<CollectionMetadata> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<RelationDesc>(),
            any::<PersistTableConnection<CollectionMetadata>>(),
            any::<PersistTableConnection<CollectionMetadata>>(),
        )
            .prop_map(|(from, from_desc, target_table, retract_from_table)| {
                ContinualTaskInsertDesc {
                    from,
                    from_desc,
                    target_table,
                    retract_from_table,
                }
            })
            .boxed()
    }
}

impl RustType<ProtoContinualTaskInsertDesc> for ContinualTaskInsertDesc<CollectionMetadata> {
    fn into_proto(&self) -> ProtoContinualTaskInsertDesc {
        ProtoContinualTaskInsertDesc {
            from: Some(self.from.into_proto()),
            from_desc: Some(self.from_desc.into_proto()),
            target_table: Some(self.target_table.into_proto()),
            retract_from_table: Some(self.retract_from_table.into_proto()),
        }
    }

    fn from_proto(proto: ProtoContinualTaskInsertDesc) -> Result<Self, TryFromProtoError> {
        Ok(ContinualTaskInsertDesc {
            from: proto
                .from
                .into_rust_if_some("ProtoContinualTaskInsertDesc::from")?,
            from_desc: proto
                .from_desc
                .into_rust_if_some("ProtoContinualTaskInsertDesc::from_desc")?,
            target_table: proto
                .target_table
                .into_rust_if_some("ProtoContinualTaskInsertDesc::target_table")?,
            retract_from_table: proto
                .retract_from_table
                .into_rust_if_some("ProtoContinualTaskInsertDesc::retract_from_table")?,
        })
    }
}

/// Reference to a table (in persist).
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistTableConnection<S> {
    /// TODO(#25239): Add documentation.
    pub value_desc: RelationDesc,
    /// TODO(#25239): Add documentation.
    pub storage_metadata: S,
}

impl RustType<ProtoPersistTableConnection> for PersistTableConnection<CollectionMetadata> {
    fn into_proto(&self) -> ProtoPersistTableConnection {
        ProtoPersistTableConnection {
            value_desc: Some(self.value_desc.into_proto()),
            storage_metadata: Some(self.storage_metadata.into_proto()),
        }
    }

    fn from_proto(proto: ProtoPersistTableConnection) -> Result<Self, TryFromProtoError> {
        Ok(PersistTableConnection {
            value_desc: proto
                .value_desc
                .into_rust_if_some("ProtoPersistTableConnection::value_desc")?,
            storage_metadata: proto
                .storage_metadata
                .into_rust_if_some("ProtoPersistTableConnection::storage_metadata")?,
        })
    }
}
