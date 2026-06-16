// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Leaf catalog data types (object/item types, role attributes).

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::LazyLock;

use mz_auth::password::Password;
use mz_cloud_provider::{CloudProvider, InvalidCloudProviderError};
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId};
use proptest_derive::Arbitrary;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::names::{CommentObjectId, DatabaseId, SchemaId};
use crate::session::vars::OwnedVarInput;

/// Specification for objects that will be affected by a default privilege.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct DefaultPrivilegeObject {
    /// The role id that created the object.
    pub role_id: RoleId,
    /// The database that the object is created in if Some, otherwise all databases.
    pub database_id: Option<DatabaseId>,
    /// The schema that the object is created in if Some, otherwise all databases.
    pub schema_id: Option<SchemaId>,
    /// The type of object.
    pub object_type: ObjectType,
}

impl DefaultPrivilegeObject {
    /// Creates a new [`DefaultPrivilegeObject`].
    pub fn new(
        role_id: RoleId,
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
        object_type: ObjectType,
    ) -> DefaultPrivilegeObject {
        DefaultPrivilegeObject {
            role_id,
            database_id,
            schema_id,
            object_type,
        }
    }
}

impl std::fmt::Display for DefaultPrivilegeObject {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO: Don't just wrap Debug.
        write!(f, "{self:?}")
    }
}

/// Specification for the privileges that will be granted from default privileges.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct DefaultPrivilegeAclItem {
    /// The role that will receive the privileges.
    pub grantee: RoleId,
    /// The specific privileges granted.
    pub acl_mode: AclMode,
}

impl DefaultPrivilegeAclItem {
    /// Creates a new [`DefaultPrivilegeAclItem`].
    pub fn new(grantee: RoleId, acl_mode: AclMode) -> DefaultPrivilegeAclItem {
        DefaultPrivilegeAclItem { grantee, acl_mode }
    }

    /// Converts this [`DefaultPrivilegeAclItem`] into an [`MzAclItem`].
    pub fn mz_acl_item(self, grantor: RoleId) -> MzAclItem {
        MzAclItem {
            grantee: self.grantee,
            grantor,
            acl_mode: self.acl_mode,
        }
    }
}

/// The authenticator that auto-provisioned a role on first login.
#[derive(
    Debug,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Arbitrary
)]
pub enum AutoProvisionSource {
    /// Role was auto-provisioned by [`mz_auth::AuthenticatorKind::Oidc`].
    Oidc,
    /// Role was auto-provisioned by [`mz_auth::AuthenticatorKind::Frontegg`].
    Frontegg,
    /// Role was auto-provisioned by [`mz_auth::AuthenticatorKind::None`]
    None,
}

/// Attributes belonging to a `CatalogRole`.
#[derive(
    Debug,
    Clone,
    Eq,
    Serialize,
    Deserialize,
    PartialEq,
    Ord,
    PartialOrd,
    Arbitrary
)]
pub struct RoleAttributes {
    /// Indicates whether the role has inheritance of privileges.
    pub inherit: bool,
    /// Whether or not this user is a superuser.
    pub superuser: Option<bool>,
    /// Whether this role is login
    pub login: Option<bool>,
    /// The authenticator that auto-provisioned this role, if any.
    pub auto_provision_source: Option<AutoProvisionSource>,
    // Force use of constructor.
    _private: (),
}

impl RoleAttributes {
    /// Creates a new [`RoleAttributes`] with default attributes.
    pub const fn new() -> RoleAttributes {
        RoleAttributes {
            inherit: true,
            superuser: None,
            login: None,
            auto_provision_source: None,
            _private: (),
        }
    }

    /// Adds all attributes except password and auto_provision_source.
    pub const fn with_all(mut self) -> RoleAttributes {
        self.inherit = true;
        self.superuser = Some(true);
        self.login = Some(true);
        self
    }

    /// Returns whether or not the role has inheritence of privileges.
    pub const fn is_inherit(&self) -> bool {
        self.inherit
    }
}

/// Default variable values for a `CatalogRole`.
#[derive(Default, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct RoleVars {
    /// Map of variable names to their value.
    pub map: BTreeMap<String, OwnedVarInput>,
}

/// The type of a `CatalogItem`.
#[derive(
    Debug,
    Deserialize,
    Clone,
    Copy,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize
)]
pub enum CatalogItemType {
    /// A table.
    Table,
    /// A source.
    Source,
    /// A sink.
    Sink,
    /// A view.
    View,
    /// A materialized view.
    MaterializedView,
    /// An index.
    Index,
    /// A type.
    Type,
    /// A func.
    Func,
    /// A secret.
    Secret,
    /// A connection.
    Connection,
}

impl CatalogItemType {
    /// Reports whether the given type of item conflicts with items of type
    /// `CatalogItemType::Type`.
    ///
    /// In PostgreSQL, even though types live in a separate namespace from other
    /// schema objects, creating a table, view, or materialized view creates a
    /// type named after that relation. This prevents creating a type with the
    /// same name as a relational object, even though types and relational
    /// objects live in separate namespaces. (Indexes are even weirder; while
    /// they don't get a type with the same name, they get an entry in
    /// `pg_class` that prevents *record* types of the same name as the index,
    /// but not other types of types, like enums.)
    ///
    /// We don't presently construct types that mirror relational objects,
    /// though we likely will need to in the future for full PostgreSQL
    /// compatibility (see database-issues#7142). For now, we use this method to
    /// prevent creating types and relational objects that have the same name, so
    /// that it is a backwards compatible change in the future to introduce a
    /// type named after each relational object in the system.
    pub fn conflicts_with_type(&self) -> bool {
        match self {
            CatalogItemType::Table => true,
            CatalogItemType::Source => true,
            CatalogItemType::View => true,
            CatalogItemType::MaterializedView => true,
            CatalogItemType::Index => true,
            CatalogItemType::Type => true,
            CatalogItemType::Sink => false,
            CatalogItemType::Func => false,
            CatalogItemType::Secret => false,
            CatalogItemType::Connection => false,
        }
    }
}

impl fmt::Display for CatalogItemType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CatalogItemType::Table => f.write_str("table"),
            CatalogItemType::Source => f.write_str("source"),
            CatalogItemType::Sink => f.write_str("sink"),
            CatalogItemType::View => f.write_str("view"),
            CatalogItemType::MaterializedView => f.write_str("materialized view"),
            CatalogItemType::Index => f.write_str("index"),
            CatalogItemType::Type => f.write_str("type"),
            CatalogItemType::Func => f.write_str("func"),
            CatalogItemType::Secret => f.write_str("secret"),
            CatalogItemType::Connection => f.write_str("connection"),
        }
    }
}

impl From<CatalogItemType> for ObjectType {
    fn from(value: CatalogItemType) -> Self {
        match value {
            CatalogItemType::Table => ObjectType::Table,
            CatalogItemType::Source => ObjectType::Source,
            CatalogItemType::Sink => ObjectType::Sink,
            CatalogItemType::View => ObjectType::View,
            CatalogItemType::MaterializedView => ObjectType::MaterializedView,
            CatalogItemType::Index => ObjectType::Index,
            CatalogItemType::Type => ObjectType::Type,
            CatalogItemType::Func => ObjectType::Func,
            CatalogItemType::Secret => ObjectType::Secret,
            CatalogItemType::Connection => ObjectType::Connection,
        }
    }
}

// Enum variant docs would be useless here.
#[allow(missing_docs)]
#[derive(
    Debug,
    Clone,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
    Copy,
    Deserialize,
    Serialize
)]
/// The types of objects stored in the catalog.
pub enum ObjectType {
    Table,
    View,
    MaterializedView,
    Source,
    Sink,
    Index,
    Type,
    Role,
    Cluster,
    ClusterReplica,
    Secret,
    Connection,
    Database,
    Schema,
    Func,
    NetworkPolicy,
}

impl ObjectType {
    /// Reports if the object type can be treated as a relation.
    pub fn is_relation(&self) -> bool {
        match self {
            ObjectType::Table
            | ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Source => true,
            ObjectType::Sink
            | ObjectType::Index
            | ObjectType::Type
            | ObjectType::Secret
            | ObjectType::Connection
            | ObjectType::Func
            | ObjectType::Database
            | ObjectType::Schema
            | ObjectType::Cluster
            | ObjectType::ClusterReplica
            | ObjectType::Role
            | ObjectType::NetworkPolicy => false,
        }
    }
}

impl From<CommentObjectId> for ObjectType {
    fn from(value: CommentObjectId) -> ObjectType {
        match value {
            CommentObjectId::Table(_) => ObjectType::Table,
            CommentObjectId::View(_) => ObjectType::View,
            CommentObjectId::MaterializedView(_) => ObjectType::MaterializedView,
            CommentObjectId::Source(_) => ObjectType::Source,
            CommentObjectId::Sink(_) => ObjectType::Sink,
            CommentObjectId::Index(_) => ObjectType::Index,
            CommentObjectId::Func(_) => ObjectType::Func,
            CommentObjectId::Connection(_) => ObjectType::Connection,
            CommentObjectId::Type(_) => ObjectType::Type,
            CommentObjectId::Secret(_) => ObjectType::Secret,
            CommentObjectId::Role(_) => ObjectType::Role,
            CommentObjectId::Database(_) => ObjectType::Database,
            CommentObjectId::Schema(_) => ObjectType::Schema,
            CommentObjectId::Cluster(_) => ObjectType::Cluster,
            CommentObjectId::ClusterReplica(_) => ObjectType::ClusterReplica,
            CommentObjectId::NetworkPolicy(_) => ObjectType::NetworkPolicy,
        }
    }
}

impl Display for ObjectType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::MaterializedView => "MATERIALIZED VIEW",
            ObjectType::Source => "SOURCE",
            ObjectType::Sink => "SINK",
            ObjectType::Index => "INDEX",
            ObjectType::Type => "TYPE",
            ObjectType::Role => "ROLE",
            ObjectType::Cluster => "CLUSTER",
            ObjectType::ClusterReplica => "CLUSTER REPLICA",
            ObjectType::Secret => "SECRET",
            ObjectType::Connection => "CONNECTION",
            ObjectType::Database => "DATABASE",
            ObjectType::Schema => "SCHEMA",
            ObjectType::Func => "FUNCTION",
            ObjectType::NetworkPolicy => "NETWORK POLICY",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
// These attributes are needed because the key of a map must be a string. We also
// get the added benefit of flattening this struct in it's serialized form.
#[serde(into = "BTreeMap<String, RoleId>")]
#[serde(try_from = "BTreeMap<String, RoleId>")]
/// Represents the grantee and a grantor of a role membership.
pub struct RoleMembership {
    /// Key is the role that some role is a member of, value is the grantor role ID.
    // TODO(jkosh44) This structure does not allow a role to have multiple of the same membership
    // from different grantors. This isn't a problem now since we don't implement ADMIN OPTION, but
    // we should figure this out before implementing ADMIN OPTION. It will likely require a messy
    // migration.
    pub map: BTreeMap<RoleId, RoleId>,
}

impl RoleMembership {
    /// Creates a new [`RoleMembership`].
    pub fn new() -> RoleMembership {
        RoleMembership {
            map: BTreeMap::new(),
        }
    }
}

impl From<RoleMembership> for BTreeMap<String, RoleId> {
    fn from(value: RoleMembership) -> Self {
        value
            .map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }
}

impl TryFrom<BTreeMap<String, RoleId>> for RoleMembership {
    type Error = anyhow::Error;

    fn try_from(value: BTreeMap<String, RoleId>) -> Result<Self, Self::Error> {
        Ok(RoleMembership {
            map: value
                .into_iter()
                .map(|(k, v)| Ok((RoleId::from_str(&k)?, v)))
                .collect::<Result<_, anyhow::Error>>()?,
        })
    }
}

impl From<CatalogItemType> for mz_audit_log::ObjectType {
    fn from(value: CatalogItemType) -> Self {
        match value {
            CatalogItemType::Table => mz_audit_log::ObjectType::Table,
            CatalogItemType::Source => mz_audit_log::ObjectType::Source,
            CatalogItemType::View => mz_audit_log::ObjectType::View,
            CatalogItemType::MaterializedView => mz_audit_log::ObjectType::MaterializedView,
            CatalogItemType::Index => mz_audit_log::ObjectType::Index,
            CatalogItemType::Type => mz_audit_log::ObjectType::Type,
            CatalogItemType::Sink => mz_audit_log::ObjectType::Sink,
            CatalogItemType::Func => mz_audit_log::ObjectType::Func,
            CatalogItemType::Secret => mz_audit_log::ObjectType::Secret,
            CatalogItemType::Connection => mz_audit_log::ObjectType::Connection,
        }
    }
}

/// Whether a SQL object type can be interpreted as matching the type of the given catalog item.
/// For example, if `v` is a view, `DROP SOURCE v` should not work, since Source and View
/// are non-matching types.
///
/// For now tables are treated as a special kind of source in Materialize, so just
/// allow `TABLE` to refer to either.
impl PartialEq<ObjectType> for CatalogItemType {
    fn eq(&self, other: &ObjectType) -> bool {
        match (self, other) {
            (CatalogItemType::Source, ObjectType::Source)
            | (CatalogItemType::Table, ObjectType::Table)
            | (CatalogItemType::Sink, ObjectType::Sink)
            | (CatalogItemType::View, ObjectType::View)
            | (CatalogItemType::MaterializedView, ObjectType::MaterializedView)
            | (CatalogItemType::Index, ObjectType::Index)
            | (CatalogItemType::Type, ObjectType::Type)
            | (CatalogItemType::Secret, ObjectType::Secret)
            | (CatalogItemType::Connection, ObjectType::Connection) => true,
            (_, _) => false,
        }
    }
}

impl PartialEq<CatalogItemType> for ObjectType {
    fn eq(&self, other: &CatalogItemType) -> bool {
        other == self
    }
}

/// Parameters used to modify password
#[derive(Debug, Clone, Eq, PartialEq, Arbitrary)]
pub struct PasswordConfig {
    /// The Password.
    pub password: Password,
    /// a non default iteration count for hashing the password.
    pub scram_iterations: NonZeroU32,
}

/// A modification of a role password in the catalog
#[derive(Debug, Clone, Eq, PartialEq, Arbitrary)]
pub enum PasswordAction {
    /// Set a new password.
    Set(PasswordConfig),
    /// Remove the existing password.
    Clear,
    /// Leave the existing password unchanged.
    NoChange,
}

/// A raw representation of attributes belonging to a `CatalogRole` that we might
/// get as input from the user. This includes the password.
/// This struct explicitly does not implement `Serialize` or `Deserialize` to avoid
/// accidentally serializing passwords.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Arbitrary)]
pub struct RoleAttributesRaw {
    /// Indicates whether the role has inheritance of privileges.
    pub inherit: bool,
    /// The raw password of the role. This is for self managed auth, not cloud.
    pub password: Option<Password>,
    /// Hash iterations used to securely store passwords. This is for self-managed auth
    pub scram_iterations: Option<NonZeroU32>,
    /// Whether or not this user is a superuser.
    pub superuser: Option<bool>,
    /// Whether this role is login
    pub login: Option<bool>,
    /// The authenticator that auto-provisioned this role, if any.
    pub auto_provision_source: Option<AutoProvisionSource>,
    // Force use of constructor.
    _private: (),
}

impl RoleAttributesRaw {
    /// Creates a new [`RoleAttributesRaw`] with default attributes.
    pub const fn new() -> RoleAttributesRaw {
        RoleAttributesRaw {
            inherit: true,
            password: None,
            scram_iterations: None,
            superuser: None,
            login: None,
            auto_provision_source: None,
            _private: (),
        }
    }

    /// Adds all attributes excluding password.
    pub const fn with_all(mut self) -> RoleAttributesRaw {
        self.inherit = true;
        self.superuser = Some(true);
        self.login = Some(true);
        self
    }
}

/// Converts a [`RoleAttributesRaw`] into a [`RoleAttributes`], dropping the password.
///
/// This lives here as a free function rather than a `From` impl because [`RoleAttributes`]
/// has a private field and the construction goes through its public constructor.
pub fn role_attributes_from_raw(raw: RoleAttributesRaw) -> RoleAttributes {
    let mut attrs = RoleAttributes::new();
    attrs.inherit = raw.inherit;
    attrs.superuser = raw.superuser;
    attrs.login = raw.login;
    attrs.auto_provision_source = raw.auto_provision_source;
    attrs
}

impl From<RoleAttributes> for RoleAttributesRaw {
    fn from(
        RoleAttributes {
            inherit,
            superuser,
            login,
            auto_provision_source,
            ..
        }: RoleAttributes,
    ) -> RoleAttributesRaw {
        RoleAttributesRaw {
            inherit,
            password: None,
            scram_iterations: None,
            superuser,
            login,
            auto_provision_source,
            _private: (),
        }
    }
}

/// A globally unique identifier for a Materialize deployment, of the form
/// `<cloud-provider>-<region>-<organization-id>-<ordinal>`, where:
///
/// * The cloud provider is a [`CloudProvider`].
/// * The organization ID is a UUID in its canonical text format.
/// * The ordinal is a decimal number with between one and eight digits.
///
/// There is no way to construct an environment ID from parts, to ensure that
/// the `Display` representation is parseable according to the above rules.
// NOTE(benesch): ideally we'd have accepted the components of the environment
// ID using separate command-line arguments, or at least a string format that
// used a field separator that did not appear in the fields. Alas. We can't
// easily change it now, as it's used as the e.g. default sink progress topic.
#[derive(Debug, Clone, PartialEq)]
pub struct EnvironmentId {
    cloud_provider: CloudProvider,
    cloud_provider_region: String,
    organization_id: Uuid,
    ordinal: u64,
}

impl EnvironmentId {
    /// Creates a dummy `EnvironmentId` for use in tests.
    pub fn for_tests() -> EnvironmentId {
        EnvironmentId {
            cloud_provider: CloudProvider::Local,
            cloud_provider_region: "az1".into(),
            organization_id: Uuid::new_v4(),
            ordinal: 0,
        }
    }

    /// Returns the cloud provider associated with this environment ID.
    pub fn cloud_provider(&self) -> &CloudProvider {
        &self.cloud_provider
    }

    /// Returns the cloud provider region associated with this environment ID.
    pub fn cloud_provider_region(&self) -> &str {
        &self.cloud_provider_region
    }

    /// Returns the name of the region associted with this environment ID.
    ///
    /// A region is a combination of [`EnvironmentId::cloud_provider`] and
    /// [`EnvironmentId::cloud_provider_region`].
    pub fn region(&self) -> String {
        format!("{}/{}", self.cloud_provider, self.cloud_provider_region)
    }

    /// Returns the organization ID associated with this environment ID.
    pub fn organization_id(&self) -> Uuid {
        self.organization_id
    }

    /// Returns the ordinal associated with this environment ID.
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }
}

// *Warning*: once the LaunchDarkly integration is live, our contexts will be
// populated using this key. Consequently, any changes to that trait
// implementation will also have to be reflected in the existing feature
// targeting config in LaunchDarkly, otherwise environments might receive
// different configs upon restart.
impl fmt::Display for EnvironmentId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}-{}-{}-{}",
            self.cloud_provider, self.cloud_provider_region, self.organization_id, self.ordinal
        )
    }
}

impl FromStr for EnvironmentId {
    type Err = InvalidEnvironmentIdError;

    fn from_str(s: &str) -> Result<EnvironmentId, InvalidEnvironmentIdError> {
        static MATCHER: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(
                "^(?P<cloud_provider>[[:alnum:]]+)-\
                  (?P<cloud_provider_region>[[:alnum:]\\-]+)-\
                  (?P<organization_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-\
                  (?P<ordinal>\\d{1,8})$"
            ).unwrap()
        });
        let captures = MATCHER.captures(s).ok_or(InvalidEnvironmentIdError)?;
        Ok(EnvironmentId {
            cloud_provider: CloudProvider::from_str(&captures["cloud_provider"])?,
            cloud_provider_region: captures["cloud_provider_region"].into(),
            organization_id: captures["organization_id"]
                .parse()
                .map_err(|_| InvalidEnvironmentIdError)?,
            ordinal: captures["ordinal"]
                .parse()
                .map_err(|_| InvalidEnvironmentIdError)?,
        })
    }
}

/// The error type for [`EnvironmentId::from_str`].
#[derive(Debug, Clone, PartialEq)]
pub struct InvalidEnvironmentIdError;

impl fmt::Display for InvalidEnvironmentIdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("invalid environment ID")
    }
}

impl Error for InvalidEnvironmentIdError {}

impl From<InvalidCloudProviderError> for InvalidEnvironmentIdError {
    fn from(_: InvalidCloudProviderError) -> Self {
        InvalidEnvironmentIdError
    }
}

/// An error returned by the catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogError {
    /// Unknown database.
    UnknownDatabase(String),
    /// Database already exists.
    DatabaseAlreadyExists(String),
    /// Unknown schema.
    UnknownSchema(String),
    /// Schema already exists.
    SchemaAlreadyExists(String),
    /// Unknown role.
    UnknownRole(String),
    /// Role already exists.
    RoleAlreadyExists(String),
    /// Network Policy already exists.
    NetworkPolicyAlreadyExists(String),
    /// Unknown cluster.
    UnknownCluster(String),
    /// Unexpected builtin cluster.
    UnexpectedBuiltinCluster(String),
    /// Unexpected builtin cluster.
    UnexpectedBuiltinClusterType(String),
    /// Cluster already exists.
    ClusterAlreadyExists(String),
    /// Unknown cluster replica.
    UnknownClusterReplica(String),
    /// Unknown cluster replica size.
    UnknownClusterReplicaSize(String),
    /// Duplicate Replica. #[error("cannot create multiple replicas named '{0}' on cluster '{1}'")]
    DuplicateReplica(String, String),
    /// Unknown item.
    UnknownItem(String),
    /// Item already exists.
    ItemAlreadyExists(CatalogItemId, String),
    /// Unknown function.
    UnknownFunction {
        /// The identifier of the function we couldn't find
        name: String,
        /// A suggested alternative to the named function.
        alternative: Option<String>,
    },
    /// Unknown type.
    UnknownType {
        /// The identifier of the type we couldn't find.
        name: String,
    },
    /// Unknown connection.
    UnknownConnection(String),
    /// Unknown network policy.
    UnknownNetworkPolicy(String),
    /// Expected the catalog item to have the given type, but it did not.
    UnexpectedType {
        /// The item's name.
        name: String,
        /// The actual type of the item.
        actual_type: CatalogItemType,
        /// The expected type of the item.
        expected_type: CatalogItemType,
    },
    /// Ran out of unique IDs.
    IdExhaustion,
    /// Ran out of unique OIDs.
    OidExhaustion,
    /// Timeline already exists.
    TimelineAlreadyExists(String),
    /// Id Allocator already exists.
    IdAllocatorAlreadyExists(String),
    /// Config already exists.
    ConfigAlreadyExists(String),
    /// Builtin migrations failed.
    FailedBuiltinSchemaMigration(String),
    /// StorageCollectionMetadata already exists.
    StorageCollectionMetadataAlreadyExists(GlobalId),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UnknownDatabase(name) => write!(f, "unknown database '{}'", name),
            Self::DatabaseAlreadyExists(name) => write!(f, "database '{name}' already exists"),
            Self::UnknownFunction { name, .. } => write!(f, "function \"{}\" does not exist", name),
            Self::UnknownType { name, .. } => write!(f, "type \"{}\" does not exist", name),
            Self::UnknownConnection(name) => write!(f, "connection \"{}\" does not exist", name),
            Self::UnknownSchema(name) => write!(f, "unknown schema '{}'", name),
            Self::SchemaAlreadyExists(name) => write!(f, "schema '{name}' already exists"),
            Self::UnknownRole(name) => write!(f, "unknown role '{}'", name),
            Self::RoleAlreadyExists(name) => write!(f, "role '{name}' already exists"),
            Self::NetworkPolicyAlreadyExists(name) => {
                write!(f, "network policy '{name}' already exists")
            }
            Self::UnknownCluster(name) => write!(f, "unknown cluster '{}'", name),
            Self::UnknownNetworkPolicy(name) => write!(f, "unknown network policy '{}'", name),
            Self::UnexpectedBuiltinCluster(name) => {
                write!(f, "Unexpected builtin cluster '{}'", name)
            }
            Self::UnexpectedBuiltinClusterType(name) => {
                write!(f, "Unexpected builtin cluster type'{}'", name)
            }
            Self::ClusterAlreadyExists(name) => write!(f, "cluster '{name}' already exists"),
            Self::UnknownClusterReplica(name) => {
                write!(f, "unknown cluster replica '{}'", name)
            }
            Self::UnknownClusterReplicaSize(name) => {
                write!(f, "unknown cluster replica size '{}'", name)
            }
            Self::DuplicateReplica(replica_name, cluster_name) => write!(
                f,
                "cannot create multiple replicas named '{replica_name}' on cluster '{cluster_name}'"
            ),
            Self::UnknownItem(name) => write!(f, "unknown catalog item '{}'", name),
            Self::ItemAlreadyExists(_gid, name) => {
                write!(f, "catalog item '{name}' already exists")
            }
            Self::UnexpectedType {
                name,
                actual_type,
                expected_type,
            } => {
                write!(f, "\"{name}\" is a {actual_type} not a {expected_type}")
            }
            Self::IdExhaustion => write!(f, "id counter overflows i64"),
            Self::OidExhaustion => write!(f, "oid counter overflows u32"),
            Self::TimelineAlreadyExists(name) => write!(f, "timeline '{name}' already exists"),
            Self::IdAllocatorAlreadyExists(name) => {
                write!(f, "ID allocator '{name}' already exists")
            }
            Self::ConfigAlreadyExists(key) => write!(f, "config '{key}' already exists"),
            Self::FailedBuiltinSchemaMigration(objects) => {
                write!(f, "failed to migrate schema of builtin objects: {objects}")
            }
            Self::StorageCollectionMetadataAlreadyExists(key) => {
                write!(f, "storage metadata for '{key}' already exists")
            }
        }
    }
}

impl CatalogError {
    /// Returns any applicable hints for [`CatalogError`].
    pub fn hint(&self) -> Option<String> {
        match self {
            CatalogError::UnknownFunction { alternative, .. } => {
                match alternative {
                    None => Some("No function matches the given name and argument types. You might need to add explicit type casts.".into()),
                    Some(alt) => Some(format!("Try using {alt}")),
                }
            }
            _ => None,
        }
    }
}

impl Error for CatalogError {}

#[derive(
    Debug,
    Clone,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
    Copy,
    Deserialize,
    Serialize
)]
/// The types of objects in the system.
pub enum SystemObjectType {
    /// Catalog object type.
    Object(ObjectType),
    /// Entire system.
    System,
}

impl SystemObjectType {
    /// Reports if the object type can be treated as a relation.
    pub fn is_relation(&self) -> bool {
        match self {
            SystemObjectType::Object(object_type) => object_type.is_relation(),
            SystemObjectType::System => false,
        }
    }
}

impl Display for SystemObjectType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SystemObjectType::Object(object_type) => std::fmt::Display::fmt(&object_type, f),
            SystemObjectType::System => f.write_str("SYSTEM"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CloudProvider, EnvironmentId, InvalidEnvironmentIdError};

    #[mz_ore::test]
    fn test_environment_id() {
        for (input, expected) in [
            (
                "local-az1-1497a3b7-a455-4fc4-8752-b44a94b5f90a-452",
                Ok(EnvironmentId {
                    cloud_provider: CloudProvider::Local,
                    cloud_provider_region: "az1".into(),
                    organization_id: "1497a3b7-a455-4fc4-8752-b44a94b5f90a".parse().unwrap(),
                    ordinal: 452,
                }),
            ),
            (
                "aws-us-east-1-1497a3b7-a455-4fc4-8752-b44a94b5f90a-0",
                Ok(EnvironmentId {
                    cloud_provider: CloudProvider::Aws,
                    cloud_provider_region: "us-east-1".into(),
                    organization_id: "1497a3b7-a455-4fc4-8752-b44a94b5f90a".parse().unwrap(),
                    ordinal: 0,
                }),
            ),
            (
                "gcp-us-central1-1497a3b7-a455-4fc4-8752-b44a94b5f90a-0",
                Ok(EnvironmentId {
                    cloud_provider: CloudProvider::Gcp,
                    cloud_provider_region: "us-central1".into(),
                    organization_id: "1497a3b7-a455-4fc4-8752-b44a94b5f90a".parse().unwrap(),
                    ordinal: 0,
                }),
            ),
            (
                "azure-australiaeast-1497a3b7-a455-4fc4-8752-b44a94b5f90a-0",
                Ok(EnvironmentId {
                    cloud_provider: CloudProvider::Azure,
                    cloud_provider_region: "australiaeast".into(),
                    organization_id: "1497a3b7-a455-4fc4-8752-b44a94b5f90a".parse().unwrap(),
                    ordinal: 0,
                }),
            ),
            (
                "generic-moon-station-11-darkside-1497a3b7-a455-4fc4-8752-b44a94b5f90a-0",
                Ok(EnvironmentId {
                    cloud_provider: CloudProvider::Generic,
                    cloud_provider_region: "moon-station-11-darkside".into(),
                    organization_id: "1497a3b7-a455-4fc4-8752-b44a94b5f90a".parse().unwrap(),
                    ordinal: 0,
                }),
            ),
            ("", Err(InvalidEnvironmentIdError)),
            (
                "local-az1-1497a3b7-a455-4fc4-8752-b44a94b5f90a-123456789",
                Err(InvalidEnvironmentIdError),
            ),
            (
                "local-1497a3b7-a455-4fc4-8752-b44a94b5f90a-452",
                Err(InvalidEnvironmentIdError),
            ),
            (
                "local-az1-1497a3b7-a455-4fc48752-b44a94b5f90a-452",
                Err(InvalidEnvironmentIdError),
            ),
        ] {
            let actual = input.parse();
            assert_eq!(expected, actual, "input = {}", input);
            if let Ok(actual) = actual {
                assert_eq!(input, actual.to_string(), "input = {}", input);
            }
        }
    }
}
