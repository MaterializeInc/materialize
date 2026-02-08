// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_proto::{RustType, TryFromProtoError};

// Re-export CatalogItemId from mz-catalog-types
pub use mz_catalog_types::CatalogItemId;

include!(concat!(env!("OUT_DIR"), "/mz_repr.catalog_item_id.rs"));

impl RustType<ProtoCatalogItemId> for CatalogItemId {
    fn into_proto(&self) -> ProtoCatalogItemId {
        use proto_catalog_item_id::Kind::*;
        ProtoCatalogItemId {
            kind: Some(match self {
                CatalogItemId::System(x) => System(*x),
                CatalogItemId::IntrospectionSourceIndex(x) => IntrospectionSourceIndex(*x),
                CatalogItemId::User(x) => User(*x),
                CatalogItemId::Transient(x) => Transient(*x),
            }),
        }
    }

    fn from_proto(proto: ProtoCatalogItemId) -> Result<Self, TryFromProtoError> {
        use proto_catalog_item_id::Kind::*;
        match proto.kind {
            Some(System(x)) => Ok(CatalogItemId::System(x)),
            Some(IntrospectionSourceIndex(x)) => Ok(CatalogItemId::IntrospectionSourceIndex(x)),
            Some(User(x)) => Ok(CatalogItemId::User(x)),
            Some(Transient(x)) => Ok(CatalogItemId::Transient(x)),
            None => Err(TryFromProtoError::missing_field("ProtoCatalogItemId::kind")),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[mz_ore::test]
    fn proptest_catalog_item_id_roundtrips() {
        fn testcase(og: CatalogItemId) {
            let s = og.to_string();
            let rnd: CatalogItemId = s.parse().unwrap();
            assert_eq!(og, rnd);
        }

        proptest!(|(id in any::<CatalogItemId>())| {
            testcase(id);
        })
    }
}
