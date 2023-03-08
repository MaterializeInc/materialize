// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! Catalog abstraction layer.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::str::FromStr;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_controller::clusters::{ClusterId, ReplicaId};
use mz_expr::MirScalarExpr;
use mz_ore::now::{EpochMillis, NowFn};
use mz_repr::explain::ExprHumanizer;
use mz_repr::{ColumnName, GlobalId, RelationDesc};
use mz_sql_parser::ast::Expr;
use mz_sql_parser::ast::UnresolvedObjectName;
use mz_storage_client::types::connections::Connection;
use mz_storage_client::types::sources::SourceDesc;

use crate::func::Func;
use crate::names::{
    Aug, DatabaseId, FullObjectName, PartialObjectName, QualifiedObjectName, QualifiedSchemaName,
    ResolvedDatabaseSpecifier, RoleId, SchemaSpecifier,
};
use crate::normalize;
use crate::plan::statement::ddl::PlannedRoleAttributes;
use crate::plan::statement::StatementDesc;
use crate::plan::PlanError;
use crate::vars::SystemVars;

/// A catalog keeps track of SQL objects and session state available to the
/// planner.
///
/// The `sql` crate is agnostic to any particular catalog implementation. This
/// trait describes the required interface.
///
/// The SQL standard mandates a catalog hierarchy of exactly three layers. A
/// catalog contains databases, databases contain schemas, and schemas contain
/// catalog items, like sources, sinks, view, and indexes.
///
/// There are two classes of operations provided by a catalog:
///
///   * Resolution operations, like [`resolve_item`]. These fill in missing name
///     components based upon connection defaults, e.g., resolving the partial
///     name `view42` to the fully-specified name `materialize.public.view42`.
///
///   * Lookup operations, like [`SessionCatalog::get_item`]. These retrieve
///     metadata about a catalog entity based on a fully-specified name that is
///     known to be valid (i.e., because the name was successfully resolved,
///     or was constructed based on the output of a prior lookup operation).
///     These functions panic if called with invalid input.
///
/// [`list_databases`]: Catalog::list_databases
/// [`get_item`]: Catalog::resolve_item
/// [`resolve_item`]: SessionCatalog::resolve_item
pub trait SessionCatalog: fmt::Debug + ExprHumanizer + Send + Sync {
    /// Returns the id of the role that is issuing the query.
    fn active_role_id(&self) -> &RoleId;

    /// Returns the database to use if one is not explicitly specified.
    fn active_database_name(&self) -> Option<&str> {
        self.active_database()
            .map(|id| self.get_database(id))
            .map(|db| db.name())
    }

    /// Returns the database to use if one is not explicitly specified.
    fn active_database(&self) -> Option<&DatabaseId>;

    /// Returns the cluster to use if one is not explicitly specified.
    fn active_cluster(&self) -> &str;

    /// Returns the resolved search paths for the current user. (Invalid search paths are skipped.)
    fn search_path(&self) -> &[(ResolvedDatabaseSpecifier, SchemaSpecifier)];

    /// Returns the descriptor of the named prepared statement on the session, or
    /// None if the prepared statement does not exist.
    fn get_prepared_statement_desc(&self, name: &str) -> Option<&StatementDesc>;

    /// Resolves the named database.
    ///
    /// If `database_name` exists in the catalog, it returns a reference to the
    /// resolved database; otherwise it returns an error.
    fn resolve_database(&self, database_name: &str) -> Result<&dyn CatalogDatabase, CatalogError>;

    /// Gets a database by its ID.
    ///
    /// Panics if `id` does not specify a valid database.
    fn get_database(&self, id: &DatabaseId) -> &dyn CatalogDatabase;

    /// Resolves a partially-specified schema name.
    ///
    /// If the schema exists in the catalog, it returns a reference to the
    /// resolved schema; otherwise it returns an error.
    fn resolve_schema(
        &self,
        database_name: Option<&str>,
        schema_name: &str,
    ) -> Result<&dyn CatalogSchema, CatalogError>;

    /// Resolves a schema name within a specified database.
    ///
    /// If the schema exists in the database, it returns a reference to the
    /// resolved schema; otherwise it returns an error.
    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
    ) -> Result<&dyn CatalogSchema, CatalogError>;

    /// Gets a schema by its ID.
    ///
    /// Panics if `id` does not specify a valid schema.
    fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
    ) -> &dyn CatalogSchema;

    /// Returns true if `schema` is an internal system schema, false otherwise
    fn is_system_schema(&self, schema: &str) -> bool;

    /// Resolves the named role.
    fn resolve_role(&self, role_name: &str) -> Result<&dyn CatalogRole, CatalogError>;

    /// Gets a role by its ID.
    fn try_get_role(&self, id: &RoleId) -> Option<&dyn CatalogRole>;

    /// Gets a role by its ID.
    ///
    /// Panics if `id` does not specify a valid role.
    fn get_role(&self, id: &RoleId) -> &dyn CatalogRole;

    /// Resolves the named cluster.
    ///
    /// If the provided name is `None`, resolves the currently active cluster.
    fn resolve_cluster<'a, 'b>(
        &'a self,
        cluster_name: Option<&'b str>,
    ) -> Result<&dyn CatalogCluster<'a>, CatalogError>;

    /// Resolves a partially-specified item name.
    ///
    /// If the partial name has a database component, it searches only the
    /// specified database; otherwise, it searches the active database. If the
    /// partial name has a schema component, it searches only the specified
    /// schema; otherwise, it searches a default set of schemas within the
    /// selected database. It returns an error if none of the searched schemas
    /// contain an item whose name matches the item component of the partial
    /// name.
    ///
    /// Note that it is not an error if the named item appears in more than one
    /// of the search schemas. The catalog implementation must choose one.
    fn resolve_item(&self, item_name: &PartialObjectName)
        -> Result<&dyn CatalogItem, CatalogError>;

    /// Performs the same operation as [`SessionCatalog::resolve_item`] but for
    /// functions within the catalog.
    fn resolve_function(
        &self,
        item_name: &PartialObjectName,
    ) -> Result<&dyn CatalogItem, CatalogError>;

    /// Gets an item by its ID.
    fn try_get_item(&self, id: &GlobalId) -> Option<&dyn CatalogItem>;

    /// Gets an item by its ID.
    ///
    /// Panics if `id` does not specify a valid item.
    fn get_item(&self, id: &GlobalId) -> &dyn CatalogItem;

    /// Reports whether the specified type exists in the catalog.
    fn item_exists(&self, name: &QualifiedObjectName) -> bool;

    /// Gets a cluster by ID.
    fn get_cluster(&self, id: ClusterId) -> &dyn CatalogCluster;

    /// Finds a name like `name` that is not already in use.
    ///
    /// If `name` itself is available, it is returned unchanged.
    fn find_available_name(&self, name: QualifiedObjectName) -> QualifiedObjectName;

    /// Returns a fully qualified human readable name from fully qualified non-human readable name
    fn resolve_full_name(&self, name: &QualifiedObjectName) -> FullObjectName;

    /// Returns the configuration of the catalog.
    fn config(&self) -> &CatalogConfig;

    /// Returns the number of milliseconds since the system epoch. For normal use
    /// this means the Unix epoch. This can safely be mocked in tests and start
    /// at 0.
    fn now(&self) -> EpochMillis;

    /// Returns the set of supported AWS PrivateLink availability zone ids.
    fn aws_privatelink_availability_zones(&self) -> Option<BTreeSet<String>>;

    /// Returns system vars
    fn system_vars(&self) -> &SystemVars;

    /// Returns mutable system vars
    ///
    /// Clients should use this this method carefully, as changes to the backing
    /// state here are not guarateed to be persisted. The motivating use case
    /// for this method was ensuring that features are temporary turned on so
    /// catalog rehydration does not break due to unsupported SQL syntax.
    fn system_vars_mut(&mut self) -> &mut SystemVars;
}

/// Configuration associated with a catalog.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// Returns the time at which the catalog booted.
    pub start_time: DateTime<Utc>,
    /// Returns the instant at which the catalog booted.
    pub start_instant: Instant,
    /// A random integer associated with this instance of the catalog.
    ///
    /// NOTE(benesch): this is only necessary for producing unique Kafka sink
    /// topics. Perhaps we can remove this when #2915 is complete.
    pub nonce: u64,
    /// A persistent ID associated with the environment.
    pub environment_id: EnvironmentId,
    /// A transient UUID associated with this process.
    pub session_id: Uuid,
    /// Whether the server is running in unsafe mode.
    pub unsafe_mode: bool,
    /// Whether persisted introspection sources are enabled.
    pub persisted_introspection: bool,
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// Default timestamp interval.
    pub timestamp_interval: Duration,
    /// Function that returns a wall clock now time; can safely be mocked to return
    /// 0.
    pub now: NowFn,
}

impl CatalogConfig {
    /// Returns the default progress topic name for a Kafka sink for a given
    /// connection.
    pub fn default_kafka_sink_progress_topic(&self, connection_id: GlobalId) -> String {
        format!(
            "_materialize-progress-{}-{connection_id}",
            self.environment_id
        )
    }
}

/// A database in a [`SessionCatalog`].
pub trait CatalogDatabase {
    /// Returns a fully-specified name of the database.
    fn name(&self) -> &str;

    /// Returns a stable ID for the database.
    fn id(&self) -> DatabaseId;

    /// Returns whether the database contains schemas.
    fn has_schemas(&self) -> bool;
}

/// A schema in a [`SessionCatalog`].
pub trait CatalogSchema {
    /// Returns a fully-specified id of the database
    fn database(&self) -> &ResolvedDatabaseSpecifier;

    /// Returns a fully-specified name of the schema.
    fn name(&self) -> &QualifiedSchemaName;

    /// Returns a stable ID for the schema.
    fn id(&self) -> &SchemaSpecifier;

    /// Lists the `CatalogItem`s for the schema.
    fn has_items(&self) -> bool;
}

/// Attributes belonging to a [`CatalogRole`].
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct RoleAttributes {
    /// Indicates whether the role has inheritance of privileges.
    pub inherit: bool,
    /// Indicates whether the role is allowed to create more roles.
    pub create_role: bool,
    /// Indicates whether the role is allowed to create databases.
    pub create_db: bool,
    /// Indicates whether the role is allowed to create clusters.
    pub create_cluster: bool,
    // Force use of constructor.
    _private: (),
}

impl RoleAttributes {
    /// Creates a new [`RoleAttributes`] with default attributes.
    pub fn new() -> RoleAttributes {
        RoleAttributes {
            inherit: true,
            create_role: false,
            create_db: false,
            create_cluster: false,
            _private: (),
        }
    }

    /// Adds the create role attribute.
    pub fn with_create_role(mut self) -> RoleAttributes {
        self.create_role = true;
        self
    }

    /// Adds the create db attribute.
    pub fn with_create_db(mut self) -> RoleAttributes {
        self.create_db = true;
        self
    }

    /// Adds the create cluster attribute.
    pub fn with_create_cluster(mut self) -> RoleAttributes {
        self.create_cluster = true;
        self
    }

    /// Adds all attributes.
    pub fn with_all(mut self) -> RoleAttributes {
        self.inherit = true;
        self.create_role = true;
        self.create_db = true;
        self.create_cluster = true;
        self
    }
}

impl From<PlannedRoleAttributes> for RoleAttributes {
    fn from(
        PlannedRoleAttributes {
            inherit,
            create_role,
            create_db,
            create_cluster,
        }: PlannedRoleAttributes,
    ) -> RoleAttributes {
        let default_attributes = RoleAttributes::new();
        RoleAttributes {
            inherit: inherit.unwrap_or(default_attributes.inherit),
            create_role: create_role.unwrap_or(default_attributes.create_role),
            create_db: create_db.unwrap_or(default_attributes.create_db),
            create_cluster: create_cluster.unwrap_or(default_attributes.create_cluster),
            _private: (),
        }
    }
}

impl From<(&dyn CatalogRole, PlannedRoleAttributes)> for RoleAttributes {
    fn from(
        (
            role,
            PlannedRoleAttributes {
                inherit,
                create_role,
                create_db,
                create_cluster,
            },
        ): (&dyn CatalogRole, PlannedRoleAttributes),
    ) -> RoleAttributes {
        RoleAttributes {
            inherit: inherit.unwrap_or_else(|| role.is_inherit()),
            create_role: create_role.unwrap_or_else(|| role.create_role()),
            create_db: create_db.unwrap_or_else(|| role.create_db()),
            create_cluster: create_cluster.unwrap_or_else(|| role.create_cluster()),
            _private: (),
        }
    }
}

/// A role in a [`SessionCatalog`].
pub trait CatalogRole {
    /// Returns a fully-specified name of the role.
    fn name(&self) -> &str;

    /// Returns a stable ID for the role.
    fn id(&self) -> RoleId;

    /// Indicates whether the role has inheritance of privileges.
    fn is_inherit(&self) -> bool;

    /// Indicates whether the role has the role creation attribute.
    fn create_role(&self) -> bool;

    /// Indicates whether the role has the database creation attribute.
    fn create_db(&self) -> bool;

    /// Indicates whether the role has the cluster creation attribute.
    fn create_cluster(&self) -> bool;
}

/// A cluster in a [`SessionCatalog`].
pub trait CatalogCluster<'a> {
    /// Returns a fully-specified name of the cluster.
    fn name(&self) -> &str;

    /// Returns a stable ID for the cluster.
    fn id(&self) -> ClusterId;

    /// Returns the ID of the object this cluster is linked to, if
    /// any.
    fn linked_object_id(&self) -> Option<GlobalId>;

    /// Returns the objects that are bound to this cluster.
    fn bound_objects(&self) -> &BTreeSet<GlobalId>;

    /// Returns the replicas of the cluster as a map from replica name to
    /// replica ID.
    fn replicas(&self) -> &BTreeMap<String, ReplicaId>;
}

/// An item in a [`SessionCatalog`].
///
/// Note that "item" has a very specific meaning in the context of a SQL
/// catalog, and refers to the various entities that belong to a schema.
pub trait CatalogItem {
    /// Returns the fully qualified name of the catalog item.
    fn name(&self) -> &QualifiedObjectName;

    /// Returns a stable ID for the catalog item.
    fn id(&self) -> GlobalId;

    /// Returns the catalog item's OID.
    fn oid(&self) -> u32;

    /// Returns a description of the result set produced by the catalog item.
    ///
    /// If the catalog item is not of a type that produces data (i.e., a sink or
    /// an index), it returns an error.
    fn desc(&self, name: &FullObjectName) -> Result<Cow<RelationDesc>, CatalogError>;

    /// Returns the resolved function.
    ///
    /// If the catalog item is not of a type that produces functions (i.e.,
    /// anything other than a function), it returns an error.
    fn func(&self) -> Result<&'static Func, CatalogError>;

    /// Returns the resolved source connection.
    ///
    /// If the catalog item is not of a type that contains a `SourceDesc`
    /// (i.e., anything other than sources), it returns an error.
    fn source_desc(&self) -> Result<Option<&SourceDesc>, CatalogError>;

    /// Returns the resolved connection.
    ///
    /// If the catalog item is not a connection, it returns an error.
    fn connection(&self) -> Result<&Connection, CatalogError>;

    /// Returns the type of the catalog item.
    fn item_type(&self) -> CatalogItemType;

    /// A normalized SQL statement that describes how to create the catalog
    /// item.
    fn create_sql(&self) -> &str;

    /// Returns the IDs of the catalog items upon which this catalog item
    /// depends.
    fn uses(&self) -> &[GlobalId];

    /// Returns the IDs of the catalog items that depend upon this catalog item.
    fn used_by(&self) -> &[GlobalId];

    /// If this catalog item is a source, it return the IDs of its subsources
    fn subsources(&self) -> Vec<GlobalId>;

    /// Returns the index details associated with the catalog item, if the
    /// catalog item is an index.
    fn index_details(&self) -> Option<(&[MirScalarExpr], GlobalId)>;

    /// Returns the column defaults associated with the catalog item, if the
    /// catalog item is a table.
    fn table_details(&self) -> Option<&[Expr<Aug>]>;

    /// Returns the type information associated with the catalog item, if the
    /// catalog item is a type.
    fn type_details(&self) -> Option<&CatalogTypeDetails<IdReference>>;
}

/// The type of a [`CatalogItem`].
#[derive(Debug, Deserialize, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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

/// Details about a type in the catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogTypeDetails<T: TypeReference> {
    /// The ID of the type with this type as the array element, if available.
    pub array_id: Option<GlobalId>,
    /// The description of this type.
    pub typ: CatalogType<T>,
}

/// Represents a reference to type in the catalog
pub trait TypeReference {
    /// The actual type used to reference a `CatalogType`
    type Reference: Clone + Debug + Eq + PartialEq;
}

/// Reference to a type by it's name
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameReference;

impl TypeReference for NameReference {
    type Reference = &'static str;
}

/// Reference to a type by it's global ID
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdReference;

impl TypeReference for IdReference {
    type Reference = GlobalId;
}

/// A type stored in the catalog.
///
/// The variants correspond one-to-one with [`mz_repr::ScalarType`], but with type
/// modifiers removed and with embedded types replaced with references to other
/// types in the catalog.
#[allow(missing_docs)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogType<T: TypeReference> {
    Array {
        element_reference: T::Reference,
    },
    Bool,
    Bytes,
    Char,
    Date,
    Float32,
    Float64,
    Int16,
    Int32,
    Int64,
    UInt16,
    UInt32,
    UInt64,
    MzTimestamp,
    Interval,
    Jsonb,
    List {
        element_reference: T::Reference,
    },
    Map {
        key_reference: T::Reference,
        value_reference: T::Reference,
    },
    Numeric,
    Oid,
    PgLegacyChar,
    Pseudo,
    Range {
        element_reference: T::Reference,
    },
    Record {
        fields: Vec<(ColumnName, T::Reference)>,
    },
    RegClass,
    RegProc,
    RegType,
    String,
    Time,
    Timestamp,
    TimestampTz,
    Uuid,
    VarChar,
    Int2Vector,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Mirrored from [PostgreSQL's `typcategory`][typcategory].
///
/// Note that Materialize also uses a number of pseudotypes when planning, but
/// we have yet to need to integrate them with `TypeCategory`.
///
/// [typcategory]:
/// https://www.postgresql.org/docs/9.6/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
pub enum TypeCategory {
    /// Array type.
    Array,
    /// Bit string type.
    BitString,
    /// Boolean type.
    Boolean,
    /// Composite type.
    Composite,
    /// Date/time type.
    DateTime,
    /// Enum type.
    Enum,
    /// Geometric type.
    Geometric,
    /// List type. Materialize specific.
    List,
    /// Network address type.
    NetworkAddress,
    /// Numeric type.
    Numeric,
    /// Pseudo type.
    Pseudo,
    /// Range type.
    Range,
    /// String type.
    String,
    /// Timestamp type.
    Timespan,
    /// User-defined type.
    UserDefined,
    /// Unknown type.
    Unknown,
}

impl fmt::Display for TypeCategory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            TypeCategory::Array => "array",
            TypeCategory::BitString => "bit-string",
            TypeCategory::Boolean => "boolean",
            TypeCategory::Composite => "composite",
            TypeCategory::DateTime => "date-time",
            TypeCategory::Enum => "enum",
            TypeCategory::Geometric => "geometric",
            TypeCategory::List => "list",
            TypeCategory::NetworkAddress => "network-address",
            TypeCategory::Numeric => "numeric",
            TypeCategory::Pseudo => "pseudo",
            TypeCategory::Range => "range",
            TypeCategory::String => "string",
            TypeCategory::Timespan => "timespan",
            TypeCategory::UserDefined => "user-defined",
            TypeCategory::Unknown => "unknown",
        })
    }
}

/// Identifies an environment.
///
/// Outside of tests, an environment ID can be constructed only from a string of
/// the following form:
///
/// ```text
/// <CLOUD PROVIDER>-<CLOUD PROVIDER REGION>-<ORGANIZATION ID>-<ORDINAL>
/// ```
///
/// The fields have the following formats:
///
/// * The cloud provider consists of one or more alphanumeric characters.
/// * The cloud provider region consists of one or more alphanumeric or hyphen
///   characters.
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
        static MATCHER: Lazy<Regex> = Lazy::new(|| {
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

/// Identifies a supported cloud provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudProvider {
    /// A pseudo-provider value used by local development environments.
    Local,
    /// A pseudo-provider value used by Docker.
    Docker,
    /// A deprecated psuedo-provider value used by mzcompose.
    // TODO(benesch): remove once v0.39 ships.
    MzCompose,
    /// A pseudo-provider value used by cloudtest.
    Cloudtest,
    /// Amazon Web Services.
    Aws,
}

impl fmt::Display for CloudProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CloudProvider::Local => f.write_str("local"),
            CloudProvider::Docker => f.write_str("docker"),
            CloudProvider::MzCompose => f.write_str("mzcompose"),
            CloudProvider::Cloudtest => f.write_str("cloudtest"),
            CloudProvider::Aws => f.write_str("aws"),
        }
    }
}

impl FromStr for CloudProvider {
    type Err = InvalidCloudProviderError;

    fn from_str(s: &str) -> Result<CloudProvider, InvalidCloudProviderError> {
        match s {
            "local" => Ok(CloudProvider::Local),
            "docker" => Ok(CloudProvider::Docker),
            "mzcompose" => Ok(CloudProvider::MzCompose),
            "cloudtest" => Ok(CloudProvider::Cloudtest),
            "aws" => Ok(CloudProvider::Aws),
            _ => Err(InvalidCloudProviderError),
        }
    }
}

/// The error type for [`CloudProvider::from_str`].
#[derive(Debug, Clone, PartialEq)]
pub struct InvalidCloudProviderError;

impl fmt::Display for InvalidCloudProviderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("invalid cloud provider")
    }
}

impl Error for InvalidCloudProviderError {}

/// An error returned by the catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogError {
    /// Unknown database.
    UnknownDatabase(String),
    /// Unknown schema.
    UnknownSchema(String),
    /// Unknown role.
    UnknownRole(String),
    /// Unknown cluster.
    UnknownCluster(String),
    /// Unknown cluster replica.
    UnknownClusterReplica(String),
    /// Unknown item.
    UnknownItem(String),
    /// Unknown function.
    UnknownFunction(String),
    /// Unknown connection.
    UnknownConnection(String),
    /// Expected the catalog item to have the given type, but it did not.
    UnexpectedType {
        /// The item's name.
        name: String,
        /// The actual type of the item.
        actual_type: CatalogItemType,
        /// The expected type of the item.
        expected_type: CatalogItemType,
    },
    /// Invalid attempt to depend on a non-dependable item.
    InvalidDependency {
        /// The invalid item's name.
        name: String,
        /// The invalid item's type.
        typ: CatalogItemType,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UnknownDatabase(name) => write!(f, "unknown database '{}'", name),
            Self::UnknownFunction(name) => write!(f, "function \"{}\" does not exist", name),
            Self::UnknownConnection(name) => write!(f, "connection \"{}\" does not exist", name),
            Self::UnknownSchema(name) => write!(f, "unknown schema '{}'", name),
            Self::UnknownRole(name) => write!(f, "unknown role '{}'", name),
            Self::UnknownCluster(name) => write!(f, "unknown cluster '{}'", name),
            Self::UnknownClusterReplica(name) => {
                write!(f, "unknown cluster replica '{}'", name)
            }
            Self::UnknownItem(name) => write!(f, "unknown catalog item '{}'", name),
            Self::UnexpectedType {
                name,
                actual_type,
                expected_type,
            } => {
                write!(f, "\"{name}\" is a {actual_type} not a {expected_type}")
            }
            Self::InvalidDependency { name, typ } => write!(
                f,
                "catalog item '{}' is {} {} and so cannot be depended upon",
                name,
                if matches!(typ, CatalogItemType::Index) {
                    "an"
                } else {
                    "a"
                },
                typ,
            ),
        }
    }
}

impl Error for CatalogError {}

/// Provides a method of generating a 3-layer catalog on the fly, and then
/// resolving objects within it.
pub(crate) struct ErsatzCatalog<'a, T>(
    pub BTreeMap<String, BTreeMap<String, BTreeMap<String, &'a T>>>,
);

impl<'a, T> ErsatzCatalog<'a, T> {
    /// Returns the fully qualified name for `item`, as well as the `T` that it
    /// describes.
    ///
    /// # Errors
    /// - If `item` cannot be normalized to a [`PartialObjectName`]
    /// - If the normalized `PartialObjectName` does not resolve to an item in
    ///   `self.0`.
    pub fn resolve(
        &self,
        item: UnresolvedObjectName,
    ) -> Result<(UnresolvedObjectName, &'a T), PlanError> {
        let name = normalize::unresolved_object_name(item)?;

        let schemas = match self.0.get(&name.item) {
            Some(schemas) => schemas,
            None => sql_bail!("table {name} not found in source"),
        };

        let schema = match &name.schema {
            Some(schema) => schema,
            None => match schemas.keys().exactly_one() {
                Ok(schema) => schema,
                Err(_) => {
                    sql_bail!("table {name} is ambiguous, consider specifying the schema")
                }
            },
        };

        let databases = match schemas.get(schema) {
            Some(databases) => databases,
            None => sql_bail!("schema {schema} not found in source"),
        };

        let database = match &name.database {
            Some(database) => database,
            None => match databases.keys().exactly_one() {
                Ok(database) => database,
                Err(_) => {
                    sql_bail!("table {name} is ambiguous, consider specifying the database")
                }
            },
        };

        let desc = match databases.get(database) {
            Some(desc) => *desc,
            None => sql_bail!("database {database} not found source"),
        };

        Ok((
            UnresolvedObjectName::qualified(&[database, schema, &name.item]),
            desc,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{EnvironmentId, InvalidEnvironmentIdError};

    use super::CloudProvider;

    #[test]
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
