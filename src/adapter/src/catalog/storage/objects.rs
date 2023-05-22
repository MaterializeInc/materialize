// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_controller::clusters::ClusterId;
use mz_ore::now::EpochMillis;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_sql::catalog::RoleAttributes;
use mz_sql::names::{DatabaseId, SchemaId};
use mz_stash::objects::proto;

/// [`StashType`] is a trait that is generally used for converting catalog types, e.g.
/// [`DatabaseId`] into the corresponding type that we serialize into the stash, e.g.
/// [`proto::DatabaseId`].
///
/// While we store serialized protobufs in the Stash, that is more of an implementation detail than
/// a public interface, which is why we don't use the helpers from the `mz_proto` crate.
pub trait StashType: Sized {
    type Stash: ::prost::Message;

    fn into_stash(self) -> Self::Stash;
    fn from_stash(stash: Self::Stash) -> Result<Self, anyhow::Error>;
}

impl StashType for AclMode {
    type Stash = proto::AclMode;

    fn into_stash(self) -> Self::Stash {
        proto::AclMode {
            bitflags: self.bits(),
        }
    }

    fn from_stash(stash: Self::Stash) -> Result<Self, anyhow::Error> {
        AclMode::from_bits(stash.bitflags)
            .ok_or_else(|| anyhow::anyhow!("Invalid AclMode from Stash {stash:?}"))
    }
}

impl StashType for MzAclItem {
    type Stash = proto::MzAclItem;

    fn into_stash(self) -> Self::Stash {
        proto::MzAclItem {
            grantee: Some(self.grantee.into_stash()),
            grantor: Some(self.grantor.into_stash()),
            acl_mode: Some(self.acl_mode.into_stash()),
        }
    }

    fn from_stash(stash: Self::Stash) -> Result<Self, anyhow::Error> {
        let grantee = stash
            .grantee
            .map(RoleId::from_stash)
            .ok_or_else(|| anyhow::anyhow!("Invalid MzAclItem, missing grantee"))??;
        let grantor = stash
            .grantor
            .map(RoleId::from_stash)
            .ok_or_else(|| anyhow::anyhow!("Invalid MzAclItem, missing grantor"))??;
        let acl_mode = stash
            .acl_mode
            .map(AclMode::from_stash)
            .ok_or_else(|| anyhow::anyhow!("Invalid MzAclItem, missing acl_mode"))??;

        Ok(MzAclItem {
            grantee,
            grantor,
            acl_mode,
        })
    }
}

impl StashType for RoleAttributes {
    type Stash = proto::RoleAttributes;

    fn into_stash(self) -> Self::Stash {
        let RoleAttributes {
            inherit,
            create_role,
            create_db,
            create_cluster,
            ..
        } = self;

        proto::RoleAttributes {
            inherit,
            create_role,
            create_db,
            create_cluster,
        }
    }

    fn from_stash(stash: Self::Stash) -> Result<Self, anyhow::Error> {
        let proto::RoleAttributes {
            inherit,
            create_cluster,
            create_role,
            create_db,
        } = stash;

        let mut attributes = RoleAttributes::new();

        attributes.inherit = inherit;
        attributes.create_cluster = create_cluster;
        attributes.create_role = create_role;
        attributes.create_db = create_db;

        Ok(attributes)
    }
}

impl StashType for RoleId {
    type Stash = proto::RoleId;

    fn into_stash(self) -> Self::Stash {
        let value = match self {
            RoleId::User(id) => proto::role_id::Value::User(id),
            RoleId::System(id) => proto::role_id::Value::System(id),
            RoleId::Public => proto::role_id::Value::Public(Default::default()),
        };

        proto::RoleId { value: Some(value) }
    }

    fn from_stash(stash: Self::Stash) -> Result<Self, anyhow::Error> {
        match stash.value {
            Some(proto::role_id::Value::User(id)) => Ok(RoleId::User(id)),
            Some(proto::role_id::Value::System(id)) => Ok(RoleId::System(id)),
            Some(proto::role_id::Value::Public(_)) => Ok(RoleId::Public),
            None => Err(anyhow::anyhow!("Invalid RoleId, found None value")),
        }
    }
}

impl StashType for DatabaseId {
    type Stash = proto::DatabaseId;

    fn into_stash(self) -> Self::Stash {
        let value = match self {
            DatabaseId::User(id) => proto::database_id::Value::User(id),
            DatabaseId::System(id) => proto::database_id::Value::System(id),
        };

        proto::DatabaseId { value: Some(value) }
    }

    fn from_stash(stash: Self::Stash) -> Result<Self, anyhow::Error> {
        match stash.value {
            Some(proto::database_id::Value::User(id)) => Ok(DatabaseId::User(id)),
            Some(proto::database_id::Value::System(id)) => Ok(DatabaseId::System(id)),
            None => Err(anyhow::anyhow!("Invalid DatabaseId, found None value")),
        }
    }
}

impl StashType for SchemaId {
    type Stash = proto::SchemaId;

    fn into_stash(self) -> Self::Stash {
        let value = match self {
            SchemaId::User(id) => proto::schema_id::Value::User(id),
            SchemaId::System(id) => proto::schema_id::Value::System(id),
        };

        proto::SchemaId { value: Some(value) }
    }

    fn from_stash(stash: Self::Stash) -> Result<Self, anyhow::Error> {
        match stash.value {
            Some(proto::schema_id::Value::User(id)) => Ok(SchemaId::User(id)),
            Some(proto::schema_id::Value::System(id)) => Ok(SchemaId::System(id)),
            None => Err(anyhow::anyhow!("Invalid SchemaId, found None value")),
        }
    }
}

impl StashType for ClusterId {
    type Stash = proto::ClusterId;

    fn into_stash(self) -> Self::Stash {
        let value = match self {
            ClusterId::User(id) => proto::cluster_id::Value::User(id),
            ClusterId::System(id) => proto::cluster_id::Value::System(id),
        };

        proto::ClusterId { value: Some(value) }
    }

    fn from_stash(stash: Self::Stash) -> Result<Self, anyhow::Error> {
        match stash.value {
            Some(proto::cluster_id::Value::User(id)) => Ok(ClusterId::User(id)),
            Some(proto::cluster_id::Value::System(id)) => Ok(ClusterId::System(id)),
            None => Err(anyhow::anyhow!("Invalid ClusterId, found None value")),
        }
    }
}

impl StashType for EpochMillis {
    type Stash = proto::EpochMillis;

    fn into_stash(self) -> Self::Stash {
        proto::EpochMillis { millis: self }
    }

    fn from_stash(stash: Self::Stash) -> Result<Self, anyhow::Error> {
        Ok(stash.millis)
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn proptest_role_attributes_roundtrips(attrs: RoleAttributes) {
            let stash = attrs.clone().into_stash();
            let after = RoleAttributes::from_stash(stash).expect("roundtrip");

            prop_assert_eq!(attrs, after);
        }

        #[test]
        fn proptest_role_id_roundtrips(role_id: RoleId) {
            let stash = role_id.clone().into_stash();
            let after = RoleId::from_stash(stash).expect("roundtrip");

            prop_assert_eq!(role_id, after);
        }

        #[test]
        fn proptest_database_id_roundtrips(id: DatabaseId) {
            let stash = id.clone().into_stash();
            let after = DatabaseId::from_stash(stash).expect("roundtrip");

            prop_assert_eq!(id, after);
        }

        #[test]
        fn proptest_schema_id_roundtrips(id: SchemaId) {
            let stash = id.clone().into_stash();
            let after = SchemaId::from_stash(stash).expect("roundtrip");

            prop_assert_eq!(id, after);
        }
    }
}
