// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the `parse_catalog_*` SQL functions defined in `mz-expr`.
//!
//! These tests live here rather than in `mz-expr` because they need access to the types in
//! `mz-catalog-protos`, which depends on `mz-expr`.

use mz_catalog_protos::objects;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::func::{EagerUnaryFunc, ParseCatalogId, ParseCatalogPrivileges};
use mz_proto::ProtoType;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::names::{DatabaseId, SchemaId};
use proptest::prelude::*;

fn serialize_and_parse_catalog_id(val: &impl serde::Serialize) -> String {
    let json = serde_json::to_value(val).unwrap();
    let jsonb = Jsonb::from_serde_json(json).unwrap();
    ParseCatalogId.call(jsonb.as_ref()).unwrap()
}

fn serialize_and_parse_catalog_privileges(val: &impl serde::Serialize) -> Vec<MzAclItem> {
    let json = serde_json::to_value(val).unwrap();
    let jsonb = Jsonb::from_serde_json(json).unwrap();
    ParseCatalogPrivileges.call(jsonb.as_ref()).unwrap().0
}

proptest! {
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn parse_catalog_id_global_id(catalog_id in any::<objects::GlobalId>()) {
        let id: GlobalId = catalog_id.into_rust().unwrap();
        let expected = match id {
            GlobalId::Explain => "e".to_string(),
            _ => id.to_string(),
        };
        prop_assert_eq!(serialize_and_parse_catalog_id(&catalog_id), expected);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn parse_catalog_id_catalog_item_id(catalog_id in any::<objects::CatalogItemId>()) {
        let id: CatalogItemId = catalog_id.into_rust().unwrap();
        prop_assert_eq!(serialize_and_parse_catalog_id(&catalog_id), id.to_string());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn parse_catalog_id_role_id(catalog_id in any::<objects::RoleId>()) {
        let id: RoleId = catalog_id.into_rust().unwrap();
        prop_assert_eq!(serialize_and_parse_catalog_id(&catalog_id), id.to_string());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn parse_catalog_id_network_policy_id(catalog_id in any::<objects::NetworkPolicyId>()) {
        let id: NetworkPolicyId = catalog_id.into_rust().unwrap();
        prop_assert_eq!(serialize_and_parse_catalog_id(&catalog_id), id.to_string());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn parse_catalog_id_database_id(catalog_id in any::<objects::DatabaseId>()) {
        let id: DatabaseId = catalog_id.into_rust().unwrap();
        prop_assert_eq!(serialize_and_parse_catalog_id(&catalog_id), id.to_string());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn parse_catalog_id_schema_id(catalog_id in any::<objects::SchemaId>()) {
        let id: SchemaId = catalog_id.into_rust().unwrap();
        prop_assert_eq!(serialize_and_parse_catalog_id(&catalog_id), id.to_string());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn parse_catalog_id_cluster_id(
        // `StorageInstanceId` requires the top 16 bits to be 0.
        catalog_id in (any::<bool>(), 0..=(u64::MAX >> 16))
            .prop_map(|(system, n)| match system {
                true => objects::ClusterId::System(n),
                false => objects::ClusterId::User(n),
            })
    ) {
        let id: ClusterId = catalog_id.into_rust().unwrap();
        prop_assert_eq!(serialize_and_parse_catalog_id(&catalog_id), id.to_string());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn parse_catalog_id_replica_id(catalog_id in any::<objects::ReplicaId>()) {
        let id: ReplicaId = catalog_id.into_rust().unwrap();
        prop_assert_eq!(serialize_and_parse_catalog_id(&catalog_id), id.to_string());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn parse_catalog_privileges(
        catalog_items in proptest::collection::vec(
            (
                any::<objects::RoleId>(),
                any::<objects::RoleId>(),
                proptest::bits::u64::masked(AclMode::all().bits()),
            ).prop_map(|(grantee, grantor, bitflags)| objects::MzAclItem {
                    grantee,
                    grantor,
                    acl_mode: objects::AclMode { bitflags },
            }),
            0..5,
        )
    ) {
        let items: Vec<MzAclItem> = catalog_items
            .iter()
            .map(|i| (*i).into_rust().unwrap())
            .collect();
        prop_assert_eq!(serialize_and_parse_catalog_privileges(&catalog_items), items);
    }
}
