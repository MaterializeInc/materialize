// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The current types used to represent catalog data stored on disk. These objects generally fall
//! into two categories.
//!
//! The key-value objects are a one-to-one mapping of the protobuf objects used to save catalog
//! data durably. They can be converted to and from protobuf via the [`mz_proto::RustType`] trait.
//! These objects should not be exposed anywhere outside the [`crate::durable`] module.
//!
//! The other type of objects combine the information from keys and values into a single struct,
//! but are still a direct representation of the data stored on disk. They can be converted to and
//! from the key-value objects via the [`DurableType`] trait. These objects are used to pass
//! information to other modules in this crate and other catalog related code.
//!
//! All non-catalog code should interact with the objects in [`crate::memory::objects`] and never
//! directly interact with the objects in this module.
//!
//! As an example, [`DatabaseKey`] and [`DatabaseValue`] are key-value objects, while [`Database`]
//! is the non-key-value counterpart.

pub mod serialization;
pub(crate) mod state_update;

use std::cmp::Ordering;
use std::collections::BTreeMap;

use mz_audit_log::VersionedEvent;
use mz_controller::clusters::ReplicaLogging;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_persist_types::ShardId;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
use mz_sql::catalog::{
    CatalogItemType, DefaultPrivilegeAclItem, DefaultPrivilegeObject, ObjectType, RoleAttributes,
    RoleMembership, RoleVars,
};
use mz_sql::names::{CommentObjectId, DatabaseId, SchemaId};
use mz_sql::plan::{ClusterSchedule, NetworkPolicyRule};
use proptest_derive::Arbitrary;

use crate::builtin::RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL;
use crate::durable::Epoch;
use crate::durable::objects::serialization::proto;

// Structs used to pass information to outside modules.

/// A trait for representing `Self` as a key-value pair of type
/// `(Key, Value)` for the purpose of storing this value durably.
///
/// To encode a key-value pair, use [`DurableType::into_key_value`].
///
/// To decode a key-value pair, use [`DurableType::from_key_value`].
///
/// This trait is based on [`RustType`], however it is meant to
/// convert the types used in [`RustType`] to a more consumable and
/// condensed type.
///
/// [`RustType`]: mz_proto::RustType
pub trait DurableType: Sized {
    type Key;
    type Value;

    /// Consume and convert `Self` into a `(Key, Value)` key-value pair.
    fn into_key_value(self) -> (Self::Key, Self::Value);

    /// Consume and convert a `(Key, Value)` key-value pair back into a
    /// `Self` value.
    fn from_key_value(key: Self::Key, value: Self::Value) -> Self;

    // TODO(jkosh44) Would be great to not clone, since this is always used for lookups which only
    // needs a reference. In practice, we currently almost always use this method to clone a single
    // 64-bit integer, so it's not a huge deal.
    /// Produce a `Key` from self. This may involve cloning/copying required fields.
    fn key(&self) -> Self::Key;
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct Database {
    pub id: DatabaseId,
    pub oid: u32,
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

impl DurableType for Database {
    type Key = DatabaseKey;
    type Value = DatabaseValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            DatabaseKey { id: self.id },
            DatabaseValue {
                oid: self.oid,
                name: self.name,
                owner_id: self.owner_id,
                privileges: self.privileges,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            id: key.id,
            oid: value.oid,
            name: value.name,
            owner_id: value.owner_id,
            privileges: value.privileges,
        }
    }

    fn key(&self) -> Self::Key {
        DatabaseKey { id: self.id }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct Schema {
    pub id: SchemaId,
    pub oid: u32,
    pub name: String,
    pub database_id: Option<DatabaseId>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

impl DurableType for Schema {
    type Key = SchemaKey;
    type Value = SchemaValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            SchemaKey { id: self.id },
            SchemaValue {
                oid: self.oid,
                database_id: self.database_id,
                name: self.name,
                owner_id: self.owner_id,
                privileges: self.privileges,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            id: key.id,
            oid: value.oid,
            name: value.name,
            database_id: value.database_id,
            owner_id: value.owner_id,
            privileges: value.privileges,
        }
    }

    fn key(&self) -> Self::Key {
        SchemaKey { id: self.id }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct Role {
    pub id: RoleId,
    pub oid: u32,
    pub name: String,
    pub attributes: RoleAttributes,
    pub membership: RoleMembership,
    pub vars: RoleVars,
}

impl DurableType for Role {
    type Key = RoleKey;
    type Value = RoleValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            RoleKey { id: self.id },
            RoleValue {
                oid: self.oid,
                name: self.name,
                attributes: self.attributes,
                membership: self.membership,
                vars: self.vars,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            id: key.id,
            oid: value.oid,
            name: value.name,
            attributes: value.attributes,
            membership: value.membership,
            vars: value.vars,
        }
    }

    fn key(&self) -> Self::Key {
        RoleKey { id: self.id }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct RoleAuth {
    pub role_id: RoleId,
    pub password_hash: Option<String>,
    pub updated_at: u64,
}

impl DurableType for RoleAuth {
    type Key = RoleAuthKey;
    type Value = RoleAuthValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            RoleAuthKey {
                role_id: self.role_id,
            },
            RoleAuthValue {
                password_hash: self.password_hash,
                updated_at: self.updated_at,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            role_id: key.role_id,
            password_hash: value.password_hash,
            updated_at: value.updated_at,
        }
    }

    fn key(&self) -> Self::Key {
        RoleAuthKey {
            role_id: self.role_id,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct NetworkPolicy {
    pub name: String,
    pub id: NetworkPolicyId,
    pub oid: u32,
    pub rules: Vec<NetworkPolicyRule>,
    pub owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
}

impl DurableType for NetworkPolicy {
    type Key = NetworkPolicyKey;
    type Value = NetworkPolicyValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            NetworkPolicyKey { id: self.id },
            NetworkPolicyValue {
                oid: self.oid,
                name: self.name,
                rules: self.rules,
                owner_id: self.owner_id,
                privileges: self.privileges,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            id: key.id,
            oid: value.oid,
            name: value.name,
            rules: value.rules,
            owner_id: value.owner_id,
            privileges: value.privileges,
        }
    }

    fn key(&self) -> Self::Key {
        NetworkPolicyKey { id: self.id }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct Cluster {
    pub id: ClusterId,
    pub name: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub config: ClusterConfig,
}

impl DurableType for Cluster {
    type Key = ClusterKey;
    type Value = ClusterValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            ClusterKey { id: self.id },
            ClusterValue {
                name: self.name,
                owner_id: self.owner_id,
                privileges: self.privileges,
                config: self.config,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            id: key.id,
            name: value.name,
            owner_id: value.owner_id,
            privileges: value.privileges,
            config: value.config,
        }
    }

    fn key(&self) -> Self::Key {
        ClusterKey { id: self.id }
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterConfig {
    pub variant: ClusterVariant,
    pub workload_class: Option<String>,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub enum ClusterVariant {
    Managed(ClusterVariantManaged),
    Unmanaged,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterVariantManaged {
    pub size: String,
    pub availability_zones: Vec<String>,
    pub logging: ReplicaLogging,
    pub replication_factor: u32,
    pub optimizer_feature_overrides: BTreeMap<String, String>,
    pub schedule: ClusterSchedule,
}

#[derive(Clone, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct IntrospectionSourceIndex {
    pub cluster_id: ClusterId,
    pub name: String,
    pub item_id: CatalogItemId,
    pub index_id: GlobalId,
    pub oid: u32,
}

impl DurableType for IntrospectionSourceIndex {
    type Key = ClusterIntrospectionSourceIndexKey;
    type Value = ClusterIntrospectionSourceIndexValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            ClusterIntrospectionSourceIndexKey {
                cluster_id: self.cluster_id,
                name: self.name,
            },
            ClusterIntrospectionSourceIndexValue {
                catalog_id: self
                    .item_id
                    .try_into()
                    .expect("cluster introspection source index mapping must be an Introspection Source Index ID"),
                global_id: self
                    .index_id
                    .try_into()
                    .expect("cluster introspection source index mapping must be a Introspection Source Index ID"),
                oid: self.oid,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            cluster_id: key.cluster_id,
            name: key.name,
            item_id: value.catalog_id.into(),
            index_id: value.global_id.into(),
            oid: value.oid,
        }
    }

    fn key(&self) -> Self::Key {
        ClusterIntrospectionSourceIndexKey {
            cluster_id: self.cluster_id,
            name: self.name.clone(),
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct ClusterReplica {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub name: String,
    pub config: ReplicaConfig,
    pub owner_id: RoleId,
}

impl DurableType for ClusterReplica {
    type Key = ClusterReplicaKey;
    type Value = ClusterReplicaValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            ClusterReplicaKey {
                id: self.replica_id,
            },
            ClusterReplicaValue {
                cluster_id: self.cluster_id,
                name: self.name,
                config: self.config,
                owner_id: self.owner_id,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            cluster_id: value.cluster_id,
            replica_id: key.id,
            name: value.name,
            config: value.config,
            owner_id: value.owner_id,
        }
    }

    fn key(&self) -> Self::Key {
        ClusterReplicaKey {
            id: self.replica_id,
        }
    }
}

// The on-disk replica configuration does not match the in-memory replica configuration, so we need
// separate structs. As of writing this comment, it is mainly due to the fact that we don't persist
// the replica allocation.
#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct ReplicaConfig {
    pub location: ReplicaLocation,
    pub logging: ReplicaLogging,
}

impl From<mz_controller::clusters::ReplicaConfig> for ReplicaConfig {
    fn from(config: mz_controller::clusters::ReplicaConfig) -> Self {
        Self {
            location: config.location.into(),
            logging: config.compute.logging,
        }
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub enum ReplicaLocation {
    Unmanaged {
        storagectl_addrs: Vec<String>,
        computectl_addrs: Vec<String>,
    },
    Managed {
        size: String,
        /// `Some(az)` if the AZ was specified by the user and must be respected;
        availability_zone: Option<String>,
        internal: bool,
        billed_as: Option<String>,
        pending: bool,
    },
}

impl From<mz_controller::clusters::ReplicaLocation> for ReplicaLocation {
    fn from(loc: mz_controller::clusters::ReplicaLocation) -> Self {
        match loc {
            mz_controller::clusters::ReplicaLocation::Unmanaged(
                mz_controller::clusters::UnmanagedReplicaLocation {
                    storagectl_addrs,
                    computectl_addrs,
                },
            ) => Self::Unmanaged {
                storagectl_addrs,
                computectl_addrs,
            },
            mz_controller::clusters::ReplicaLocation::Managed(
                mz_controller::clusters::ManagedReplicaLocation {
                    allocation: _,
                    size,
                    availability_zones,
                    billed_as,
                    internal,
                    pending,
                },
            ) => ReplicaLocation::Managed {
                size,
                availability_zone:
                    if let mz_controller::clusters::ManagedReplicaAvailabilityZones::FromReplica(
                        Some(az),
                    ) = availability_zones
                    {
                        Some(az)
                    } else {
                        None
                    },
                internal,
                billed_as,
                pending,
            },
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct Item {
    pub id: CatalogItemId,
    pub oid: u32,
    pub global_id: GlobalId,
    pub schema_id: SchemaId,
    pub name: String,
    pub create_sql: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub extra_versions: BTreeMap<RelationVersion, GlobalId>,
}

impl Item {
    pub fn item_type(&self) -> CatalogItemType {
        item_type(&self.create_sql)
    }
}

impl DurableType for Item {
    type Key = ItemKey;
    type Value = ItemValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            ItemKey { id: self.id },
            ItemValue {
                oid: self.oid,
                global_id: self.global_id,
                schema_id: self.schema_id,
                name: self.name,
                create_sql: self.create_sql,
                owner_id: self.owner_id,
                privileges: self.privileges,
                extra_versions: self.extra_versions,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            id: key.id,
            oid: value.oid,
            global_id: value.global_id,
            schema_id: value.schema_id,
            name: value.name,
            create_sql: value.create_sql,
            owner_id: value.owner_id,
            privileges: value.privileges,
            extra_versions: value.extra_versions,
        }
    }

    fn key(&self) -> Self::Key {
        ItemKey { id: self.id }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct SourceReferences {
    pub source_id: CatalogItemId,
    pub updated_at: u64,
    pub references: Vec<SourceReference>,
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Arbitrary)]
pub struct SourceReference {
    pub name: String,
    pub namespace: Option<String>,
    pub columns: Vec<String>,
}

impl DurableType for SourceReferences {
    type Key = SourceReferencesKey;
    type Value = SourceReferencesValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            SourceReferencesKey {
                source_id: self.source_id,
            },
            SourceReferencesValue {
                updated_at: self.updated_at,
                references: self.references,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            source_id: key.source_id,
            updated_at: value.updated_at,
            references: value.references,
        }
    }

    fn key(&self) -> Self::Key {
        SourceReferencesKey {
            source_id: self.source_id,
        }
    }
}

/// A newtype wrapper for [`CatalogItemId`] that is only for the "system" namespace.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct SystemCatalogItemId(u64);

impl TryFrom<CatalogItemId> for SystemCatalogItemId {
    type Error = &'static str;

    fn try_from(val: CatalogItemId) -> Result<Self, Self::Error> {
        match val {
            CatalogItemId::System(x) => Ok(SystemCatalogItemId(x)),
            CatalogItemId::IntrospectionSourceIndex(_) => Err("introspection_source_index"),
            CatalogItemId::User(_) => Err("user"),
            CatalogItemId::Transient(_) => Err("transient"),
        }
    }
}

impl From<SystemCatalogItemId> for CatalogItemId {
    fn from(val: SystemCatalogItemId) -> Self {
        CatalogItemId::System(val.0)
    }
}

/// A newtype wrapper for [`CatalogItemId`] that is only for the "introspection source index" namespace.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct IntrospectionSourceIndexCatalogItemId(u64);

impl TryFrom<CatalogItemId> for IntrospectionSourceIndexCatalogItemId {
    type Error = &'static str;

    fn try_from(val: CatalogItemId) -> Result<Self, Self::Error> {
        match val {
            CatalogItemId::System(_) => Err("system"),
            CatalogItemId::IntrospectionSourceIndex(x) => {
                Ok(IntrospectionSourceIndexCatalogItemId(x))
            }
            CatalogItemId::User(_) => Err("user"),
            CatalogItemId::Transient(_) => Err("transient"),
        }
    }
}

impl From<IntrospectionSourceIndexCatalogItemId> for CatalogItemId {
    fn from(val: IntrospectionSourceIndexCatalogItemId) -> Self {
        CatalogItemId::IntrospectionSourceIndex(val.0)
    }
}

/// A newtype wrapper for [`GlobalId`] that is only for the "system" namespace.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct SystemGlobalId(u64);

impl TryFrom<GlobalId> for SystemGlobalId {
    type Error = &'static str;

    fn try_from(val: GlobalId) -> Result<Self, Self::Error> {
        match val {
            GlobalId::System(x) => Ok(SystemGlobalId(x)),
            GlobalId::IntrospectionSourceIndex(_) => Err("introspection_source_index"),
            GlobalId::User(_) => Err("user"),
            GlobalId::Transient(_) => Err("transient"),
            GlobalId::Explain => Err("explain"),
        }
    }
}

impl From<SystemGlobalId> for GlobalId {
    fn from(val: SystemGlobalId) -> Self {
        GlobalId::System(val.0)
    }
}

/// A newtype wrapper for [`GlobalId`] that is only for the "introspection source index" namespace.
#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct IntrospectionSourceIndexGlobalId(u64);

impl TryFrom<GlobalId> for IntrospectionSourceIndexGlobalId {
    type Error = &'static str;

    fn try_from(val: GlobalId) -> Result<Self, Self::Error> {
        match val {
            GlobalId::System(_) => Err("system"),
            GlobalId::IntrospectionSourceIndex(x) => Ok(IntrospectionSourceIndexGlobalId(x)),
            GlobalId::User(_) => Err("user"),
            GlobalId::Transient(_) => Err("transient"),
            GlobalId::Explain => Err("explain"),
        }
    }
}

impl From<IntrospectionSourceIndexGlobalId> for GlobalId {
    fn from(val: IntrospectionSourceIndexGlobalId) -> Self {
        GlobalId::IntrospectionSourceIndex(val.0)
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct SystemObjectDescription {
    pub schema_name: String,
    pub object_type: CatalogItemType,
    pub object_name: String,
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct SystemObjectUniqueIdentifier {
    pub catalog_id: CatalogItemId,
    pub global_id: GlobalId,
    pub fingerprint: String,
}

impl SystemObjectUniqueIdentifier {
    pub fn runtime_alterable(&self) -> bool {
        self.fingerprint == RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL
    }
}

/// Functions can share the same name as any other catalog item type
/// within a given schema.
/// For example, a function can have the same name as a type, e.g.
/// 'date'.
/// As such, system objects are keyed in the catalog storage by the
/// tuple (schema_name, object_type, object_name), which is guaranteed
/// to be unique.
#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct SystemObjectMapping {
    pub description: SystemObjectDescription,
    pub unique_identifier: SystemObjectUniqueIdentifier,
}

impl DurableType for SystemObjectMapping {
    type Key = GidMappingKey;
    type Value = GidMappingValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            GidMappingKey {
                schema_name: self.description.schema_name,
                object_type: self.description.object_type,
                object_name: self.description.object_name,
            },
            GidMappingValue {
                catalog_id: self
                    .unique_identifier
                    .catalog_id
                    .try_into()
                    .expect("catalog_id to be in the system namespace"),
                global_id: self
                    .unique_identifier
                    .global_id
                    .try_into()
                    .expect("collection_id to be in the system namespace"),
                fingerprint: self.unique_identifier.fingerprint,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            description: SystemObjectDescription {
                schema_name: key.schema_name,
                object_type: key.object_type,
                object_name: key.object_name,
            },
            unique_identifier: SystemObjectUniqueIdentifier {
                catalog_id: value.catalog_id.into(),
                global_id: value.global_id.into(),
                fingerprint: value.fingerprint,
            },
        }
    }

    fn key(&self) -> Self::Key {
        GidMappingKey {
            schema_name: self.description.schema_name.clone(),
            object_type: self.description.object_type.clone(),
            object_name: self.description.object_name.clone(),
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct DefaultPrivilege {
    pub object: DefaultPrivilegeObject,
    pub acl_item: DefaultPrivilegeAclItem,
}

impl DurableType for DefaultPrivilege {
    type Key = DefaultPrivilegesKey;
    type Value = DefaultPrivilegesValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            DefaultPrivilegesKey {
                role_id: self.object.role_id,
                database_id: self.object.database_id,
                schema_id: self.object.schema_id,
                object_type: self.object.object_type,
                grantee: self.acl_item.grantee,
            },
            DefaultPrivilegesValue {
                privileges: self.acl_item.acl_mode,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            object: DefaultPrivilegeObject {
                role_id: key.role_id,
                database_id: key.database_id,
                schema_id: key.schema_id,
                object_type: key.object_type,
            },
            acl_item: DefaultPrivilegeAclItem {
                grantee: key.grantee,
                acl_mode: value.privileges,
            },
        }
    }

    fn key(&self) -> Self::Key {
        DefaultPrivilegesKey {
            role_id: self.object.role_id,
            database_id: self.object.database_id,
            schema_id: self.object.schema_id,
            object_type: self.object.object_type,
            grantee: self.acl_item.grantee,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct Comment {
    pub object_id: CommentObjectId,
    pub sub_component: Option<usize>,
    pub comment: String,
}

impl DurableType for Comment {
    type Key = CommentKey;
    type Value = CommentValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            CommentKey {
                object_id: self.object_id,
                sub_component: self.sub_component,
            },
            CommentValue {
                comment: self.comment,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            object_id: key.object_id,
            sub_component: key.sub_component,
            comment: value.comment,
        }
    }

    fn key(&self) -> Self::Key {
        CommentKey {
            object_id: self.object_id,
            sub_component: self.sub_component,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdAlloc {
    pub name: String,
    pub next_id: u64,
}

impl DurableType for IdAlloc {
    type Key = IdAllocKey;
    type Value = IdAllocValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            IdAllocKey { name: self.name },
            IdAllocValue {
                next_id: self.next_id,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            name: key.name,
            next_id: value.next_id,
        }
    }

    fn key(&self) -> Self::Key {
        IdAllocKey {
            name: self.name.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub key: String,
    pub value: u64,
}

impl DurableType for Config {
    type Key = ConfigKey;
    type Value = ConfigValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            ConfigKey { key: self.key },
            ConfigValue { value: self.value },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            key: key.key,
            value: value.value,
        }
    }

    fn key(&self) -> Self::Key {
        ConfigKey {
            key: self.key.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Setting {
    pub name: String,
    pub value: String,
}

impl DurableType for Setting {
    type Key = SettingKey;
    type Value = SettingValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            SettingKey { name: self.name },
            SettingValue { value: self.value },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            name: key.name,
            value: value.value,
        }
    }

    fn key(&self) -> Self::Key {
        SettingKey {
            name: self.name.clone(),
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct SystemConfiguration {
    pub name: String,
    pub value: String,
}

impl DurableType for SystemConfiguration {
    type Key = ServerConfigurationKey;
    type Value = ServerConfigurationValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            ServerConfigurationKey { name: self.name },
            ServerConfigurationValue { value: self.value },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            name: key.name,
            value: value.value,
        }
    }

    fn key(&self) -> Self::Key {
        ServerConfigurationKey {
            name: self.name.clone(),
        }
    }
}

impl DurableType for MzAclItem {
    type Key = SystemPrivilegesKey;
    type Value = SystemPrivilegesValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            SystemPrivilegesKey {
                grantee: self.grantee,
                grantor: self.grantor,
            },
            SystemPrivilegesValue {
                acl_mode: self.acl_mode,
            },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            grantee: key.grantee,
            grantor: key.grantor,
            acl_mode: value.acl_mode,
        }
    }

    fn key(&self) -> Self::Key {
        SystemPrivilegesKey {
            grantee: self.grantee,
            grantor: self.grantor,
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct AuditLog {
    pub event: VersionedEvent,
}

impl DurableType for AuditLog {
    type Key = AuditLogKey;
    type Value = ();

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (AuditLogKey { event: self.event }, ())
    }

    fn from_key_value(key: Self::Key, _value: Self::Value) -> Self {
        Self { event: key.event }
    }

    fn key(&self) -> Self::Key {
        AuditLogKey {
            event: self.event.clone(),
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct StorageCollectionMetadata {
    pub id: GlobalId,
    pub shard: ShardId,
}

impl DurableType for StorageCollectionMetadata {
    type Key = StorageCollectionMetadataKey;
    type Value = StorageCollectionMetadataValue;

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (
            StorageCollectionMetadataKey { id: self.id },
            StorageCollectionMetadataValue { shard: self.shard },
        )
    }

    fn from_key_value(key: Self::Key, value: Self::Value) -> Self {
        Self {
            id: key.id,
            shard: value.shard,
        }
    }

    fn key(&self) -> Self::Key {
        StorageCollectionMetadataKey { id: self.id }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct UnfinalizedShard {
    pub shard: ShardId,
}

impl DurableType for UnfinalizedShard {
    type Key = UnfinalizedShardKey;
    type Value = ();

    fn into_key_value(self) -> (Self::Key, Self::Value) {
        (UnfinalizedShardKey { shard: self.shard }, ())
    }

    fn from_key_value(key: Self::Key, _value: Self::Value) -> Self {
        Self { shard: key.shard }
    }

    fn key(&self) -> Self::Key {
        UnfinalizedShardKey {
            shard: self.shard.clone(),
        }
    }
}

// Structs used internally to represent on-disk state.

/// A snapshot of the current on-disk state.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Snapshot {
    pub databases: BTreeMap<proto::DatabaseKey, proto::DatabaseValue>,
    pub schemas: BTreeMap<proto::SchemaKey, proto::SchemaValue>,
    pub roles: BTreeMap<proto::RoleKey, proto::RoleValue>,
    pub role_auth: BTreeMap<proto::RoleAuthKey, proto::RoleAuthValue>,
    pub items: BTreeMap<proto::ItemKey, proto::ItemValue>,
    pub comments: BTreeMap<proto::CommentKey, proto::CommentValue>,
    pub clusters: BTreeMap<proto::ClusterKey, proto::ClusterValue>,
    pub network_policies: BTreeMap<proto::NetworkPolicyKey, proto::NetworkPolicyValue>,
    pub cluster_replicas: BTreeMap<proto::ClusterReplicaKey, proto::ClusterReplicaValue>,
    pub introspection_sources: BTreeMap<
        proto::ClusterIntrospectionSourceIndexKey,
        proto::ClusterIntrospectionSourceIndexValue,
    >,
    pub id_allocator: BTreeMap<proto::IdAllocKey, proto::IdAllocValue>,
    pub configs: BTreeMap<proto::ConfigKey, proto::ConfigValue>,
    pub settings: BTreeMap<proto::SettingKey, proto::SettingValue>,
    pub system_object_mappings: BTreeMap<proto::GidMappingKey, proto::GidMappingValue>,
    pub system_configurations:
        BTreeMap<proto::ServerConfigurationKey, proto::ServerConfigurationValue>,
    pub default_privileges: BTreeMap<proto::DefaultPrivilegesKey, proto::DefaultPrivilegesValue>,
    pub source_references: BTreeMap<proto::SourceReferencesKey, proto::SourceReferencesValue>,
    pub system_privileges: BTreeMap<proto::SystemPrivilegesKey, proto::SystemPrivilegesValue>,
    pub storage_collection_metadata:
        BTreeMap<proto::StorageCollectionMetadataKey, proto::StorageCollectionMetadataValue>,
    pub unfinalized_shards: BTreeMap<proto::UnfinalizedShardKey, ()>,
    pub txn_wal_shard: BTreeMap<(), proto::TxnWalShardValue>,
}

impl Snapshot {
    pub fn empty() -> Snapshot {
        Snapshot::default()
    }
}

/// Token used to fence out other processes.
///
/// Every time a new process takes over, the `epoch` should be incremented.
/// Every time a new version is deployed, the `deploy` generation should be incremented.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Arbitrary)]
pub struct FenceToken {
    pub(crate) deploy_generation: u64,
    pub(crate) epoch: Epoch,
}

impl PartialOrd for FenceToken {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FenceToken {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deploy_generation
            .cmp(&other.deploy_generation)
            .then(self.epoch.cmp(&other.epoch))
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SettingKey {
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct SettingValue {
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct IdAllocKey {
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct IdAllocValue {
    pub(crate) next_id: u64,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct GidMappingKey {
    pub(crate) schema_name: String,
    pub(crate) object_type: CatalogItemType,
    pub(crate) object_name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct GidMappingValue {
    pub(crate) catalog_id: SystemCatalogItemId,
    pub(crate) global_id: SystemGlobalId,
    pub(crate) fingerprint: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterKey {
    pub(crate) id: ClusterId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterValue {
    pub(crate) name: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
    pub(crate) config: ClusterConfig,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterIntrospectionSourceIndexKey {
    pub(crate) cluster_id: ClusterId,
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterIntrospectionSourceIndexValue {
    pub(crate) catalog_id: IntrospectionSourceIndexCatalogItemId,
    pub(crate) global_id: IntrospectionSourceIndexGlobalId,
    pub(crate) oid: u32,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ClusterReplicaKey {
    pub(crate) id: ReplicaId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterReplicaValue {
    pub(crate) cluster_id: ClusterId,
    pub(crate) name: String,
    pub(crate) config: ReplicaConfig,
    pub(crate) owner_id: RoleId,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct DatabaseKey {
    pub(crate) id: DatabaseId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct DatabaseValue {
    pub(crate) name: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
    pub(crate) oid: u32,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct SourceReferencesKey {
    pub(crate) source_id: CatalogItemId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SourceReferencesValue {
    pub(crate) references: Vec<SourceReference>,
    pub(crate) updated_at: u64,
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Arbitrary)]
pub struct SchemaKey {
    pub(crate) id: SchemaId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct SchemaValue {
    pub(crate) database_id: Option<DatabaseId>,
    pub(crate) name: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
    pub(crate) oid: u32,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug, Arbitrary)]
pub struct ItemKey {
    pub(crate) id: CatalogItemId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct ItemValue {
    pub(crate) schema_id: SchemaId,
    pub(crate) name: String,
    pub(crate) create_sql: String,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
    pub(crate) oid: u32,
    pub(crate) global_id: GlobalId,
    pub(crate) extra_versions: BTreeMap<RelationVersion, GlobalId>,
}

impl ItemValue {
    pub fn item_type(&self) -> CatalogItemType {
        item_type(&self.create_sql)
    }
}

fn item_type(create_sql: &str) -> CatalogItemType {
    // NOTE(benesch): the implementation of this method is hideous, but is
    // there a better alternative? Storing the object type alongside the
    // `create_sql` would introduce the possibility of skew.
    let mut tokens = create_sql.split_whitespace();
    assert_eq!(tokens.next(), Some("CREATE"));
    match tokens.next() {
        Some("TABLE") => CatalogItemType::Table,
        Some("SOURCE") | Some("SUBSOURCE") => CatalogItemType::Source,
        Some("SINK") => CatalogItemType::Sink,
        Some("VIEW") => CatalogItemType::View,
        Some("MATERIALIZED") => {
            assert_eq!(tokens.next(), Some("VIEW"));
            CatalogItemType::MaterializedView
        }
        Some("CONTINUAL") => {
            assert_eq!(tokens.next(), Some("TASK"));
            CatalogItemType::ContinualTask
        }
        Some("INDEX") => CatalogItemType::Index,
        Some("TYPE") => CatalogItemType::Type,
        Some("FUNCTION") => CatalogItemType::Func,
        Some("SECRET") => CatalogItemType::Secret,
        Some("CONNECTION") => CatalogItemType::Connection,
        Some("REPLACEMENT") => {
            let _name = tokens.next();
            assert_eq!(tokens.next(), Some("FOR"));
            match tokens.next() {
                Some("MATERIALIZED") => {
                    assert_eq!(tokens.next(), Some("VIEW"));
                    CatalogItemType::ReplacementMaterializedView
                }
                other => panic!("unexpected replacement target: {:?}", other),
            }
        }
        _ => panic!("unexpected create sql: {}", create_sql),
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub struct CommentKey {
    pub(crate) object_id: CommentObjectId,
    pub(crate) sub_component: Option<usize>,
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Arbitrary)]
pub struct CommentValue {
    pub(crate) comment: String,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub struct RoleKey {
    pub(crate) id: RoleId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Debug)]
pub struct RoleValue {
    pub(crate) name: String,
    pub(crate) attributes: RoleAttributes,
    pub(crate) membership: RoleMembership,
    pub(crate) vars: RoleVars,
    pub(crate) oid: u32,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Hash, Debug)]
pub struct NetworkPolicyKey {
    pub(crate) id: NetworkPolicyId,
}

#[derive(Clone, PartialOrd, PartialEq, Eq, Ord, Debug)]
pub struct NetworkPolicyValue {
    pub(crate) name: String,
    pub(crate) rules: Vec<NetworkPolicyRule>,
    pub(crate) owner_id: RoleId,
    pub(crate) privileges: Vec<MzAclItem>,
    pub(crate) oid: u32,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ConfigKey {
    pub(crate) key: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ConfigValue {
    pub(crate) value: u64,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct AuditLogKey {
    pub(crate) event: VersionedEvent,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct StorageCollectionMetadataKey {
    pub(crate) id: GlobalId,
}

/// This value is stored transparently, however, it should only ever be
/// manipulated by the storage controller.
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct StorageCollectionMetadataValue {
    pub(crate) shard: ShardId,
}

/// This value is stored transparently, however, it should only ever be
/// manipulated by the storage controller.
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct UnfinalizedShardKey {
    pub(crate) shard: ShardId,
}

/// This value is stored transparently, however, it should only ever be
/// manipulated by the storage controller.
#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct TxnWalShardValue {
    pub(crate) shard: ShardId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct ServerConfigurationKey {
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord)]
pub struct ServerConfigurationValue {
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesKey {
    pub(crate) role_id: RoleId,
    pub(crate) database_id: Option<DatabaseId>,
    pub(crate) schema_id: Option<SchemaId>,
    pub(crate) object_type: ObjectType,
    pub(crate) grantee: RoleId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct DefaultPrivilegesValue {
    pub(crate) privileges: AclMode,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SystemPrivilegesKey {
    pub(crate) grantee: RoleId,
    pub(crate) grantor: RoleId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct SystemPrivilegesValue {
    pub(crate) acl_mode: AclMode,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct RoleAuthKey {
    // TODO(auth): Depending on what the future holds, here is where
    // we might also want to key by a `version` field.
    // That way we can store password versions or what have you.
    pub(crate) role_id: RoleId,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct RoleAuthValue {
    pub(crate) password_hash: Option<String>,
    pub(crate) updated_at: u64,
}

#[cfg(test)]
mod test {
    use mz_proto::{ProtoType, RustType};
    use proptest::prelude::*;

    use super::{
        DatabaseKey, DatabaseValue, FenceToken, ItemKey, ItemValue, SchemaKey, SchemaValue,
    };
    use crate::durable::Epoch;

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_database_key_roundtrip(key: DatabaseKey) {
            let proto = key.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(key, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_database_value_roundtrip(value: DatabaseValue) {
            let proto = value.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(value, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_schema_key_roundtrip(key: SchemaKey) {
            let proto = key.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(key, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_schema_value_roundtrip(value: SchemaValue) {
            let proto = value.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(value, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_item_key_roundtrip(key: ItemKey) {
            let proto = key.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(key, round);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // slow
        fn proptest_item_value_roundtrip(value: ItemValue) {
            let proto = value.into_proto();
            let round = proto.into_rust().expect("to roundtrip");

            prop_assert_eq!(value, round);
        }
    }

    #[mz_ore::test]
    fn test_fence_token_order() {
        let ft1 = FenceToken {
            deploy_generation: 10,
            epoch: Epoch::new(20).expect("non-zero"),
        };
        let ft2 = FenceToken {
            deploy_generation: 10,
            epoch: Epoch::new(19).expect("non-zero"),
        };

        assert!(ft1 > ft2);

        let ft3 = FenceToken {
            deploy_generation: 11,
            epoch: Epoch::new(10).expect("non-zero"),
        };

        assert!(ft3 > ft1);

        let ft4 = FenceToken {
            deploy_generation: 11,
            epoch: Epoch::new(30).expect("non-zero"),
        };

        assert!(ft4 > ft1);
    }
}
