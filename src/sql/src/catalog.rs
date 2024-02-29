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
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use itertools::Itertools;
use mz_build_info::BuildInfo;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::MirScalarExpr;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::str::StrExt;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem, PrivilegeMap};
use mz_repr::explain::ExprHumanizer;
use mz_repr::role_id::RoleId;
use mz_repr::{ColumnName, GlobalId, RelationDesc};
use mz_sql_parser::ast::{Expr, Ident, QualifiedReplica, UnresolvedItemName};
use mz_storage_types::connections::inline::{ConnectionResolver, ReferencedConnection};
use mz_storage_types::connections::{Connection, ConnectionContext};
use mz_storage_types::sources::SourceDesc;
use once_cell::sync::Lazy;
use proptest_derive::Arbitrary;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::func::Func;
use crate::names::{
    Aug, CommentObjectId, DatabaseId, FullItemName, FullSchemaName, ObjectId, PartialItemName,
    QualifiedItemName, QualifiedSchemaName, ResolvedDatabaseSpecifier, ResolvedIds, SchemaId,
    SchemaSpecifier, SystemObjectId,
};
use crate::normalize;
use crate::plan::statement::ddl::PlannedRoleAttributes;
use crate::plan::statement::StatementDesc;
use crate::plan::{query, PlanError, PlanNotice};
use crate::session::vars::{OwnedVarInput, SystemVars};

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
///     known to be valid (i.e., because the name was successfully resolved, or
///     was constructed based on the output of a prior lookup operation). These
///     functions panic if called with invalid input.
///
///   * Session management, such as managing variables' states and adding
///     notices to the session.
///
/// [`list_databases`]: Catalog::list_databases
/// [`get_item`]: Catalog::resolve_item
/// [`resolve_item`]: SessionCatalog::resolve_item
pub trait SessionCatalog: fmt::Debug + ExprHumanizer + Send + Sync + ConnectionResolver {
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

    /// Gets all databases.
    fn get_databases(&self) -> Vec<&dyn CatalogDatabase>;

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

    /// Gets all schemas.
    fn get_schemas(&self) -> Vec<&dyn CatalogSchema>;

    /// Gets the mz_internal schema id.
    fn get_mz_internal_schema_id(&self) -> &SchemaId;

    /// Gets the mz_unsafe schema id.
    fn get_mz_unsafe_schema_id(&self) -> &SchemaId;

    /// Returns true if `schema` is an internal system schema, false otherwise
    fn is_system_schema(&self, schema: &str) -> bool;

    /// Returns true if `schema` is an internal system schema, false otherwise
    fn is_system_schema_specifier(&self, schema: &SchemaSpecifier) -> bool;

    /// Resolves the named role.
    fn resolve_role(&self, role_name: &str) -> Result<&dyn CatalogRole, CatalogError>;

    /// Gets a role by its ID.
    fn try_get_role(&self, id: &RoleId) -> Option<&dyn CatalogRole>;

    /// Gets a role by its ID.
    ///
    /// Panics if `id` does not specify a valid role.
    fn get_role(&self, id: &RoleId) -> &dyn CatalogRole;

    /// Gets all roles.
    fn get_roles(&self) -> Vec<&dyn CatalogRole>;

    /// Gets the id of the `mz_system` role.
    fn mz_system_role_id(&self) -> RoleId;

    /// Collects all role IDs that `id` is transitively a member of.
    fn collect_role_membership(&self, id: &RoleId) -> BTreeSet<RoleId>;

    /// Resolves the named cluster.
    ///
    /// If the provided name is `None`, resolves the currently active cluster.
    fn resolve_cluster<'a, 'b>(
        &'a self,
        cluster_name: Option<&'b str>,
    ) -> Result<&dyn CatalogCluster<'a>, CatalogError>;

    /// Resolves the named cluster replica.
    fn resolve_cluster_replica<'a, 'b>(
        &'a self,
        cluster_replica_name: &'b QualifiedReplica,
    ) -> Result<&dyn CatalogClusterReplica<'a>, CatalogError>;

    /// Resolves a partially-specified item name, that is NOT a function or
    /// type. (For resolving functions or types, please use
    /// [SessionCatalog::resolve_function] or [SessionCatalog::resolve_type].)
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
    fn resolve_item(&self, item_name: &PartialItemName) -> Result<&dyn CatalogItem, CatalogError>;

    /// Performs the same operation as [`SessionCatalog::resolve_item`] but for
    /// functions within the catalog.
    fn resolve_function(
        &self,
        item_name: &PartialItemName,
    ) -> Result<&dyn CatalogItem, CatalogError>;

    /// Performs the same operation as [`SessionCatalog::resolve_item`] but for
    /// types within the catalog.
    fn resolve_type(&self, item_name: &PartialItemName) -> Result<&dyn CatalogItem, CatalogError>;

    /// Resolves `name` to a type or item, preferring the type if both exist.
    fn resolve_item_or_type(
        &self,
        name: &PartialItemName,
    ) -> Result<&dyn CatalogItem, CatalogError> {
        if let Ok(ty) = self.resolve_type(name) {
            return Ok(ty);
        }
        self.resolve_item(name)
    }

    /// Gets a type named `name` from exactly one of the system schemas.
    ///
    /// # Panics
    /// - If `name` is not an entry in any system schema
    /// - If more than one system schema has an entry named `name`.
    fn get_system_type(&self, name: &str) -> &dyn CatalogItem;

    /// Gets an item by its ID.
    fn try_get_item(&self, id: &GlobalId) -> Option<&dyn CatalogItem>;

    /// Gets an item by its ID.
    ///
    /// Panics if `id` does not specify a valid item.
    fn get_item(&self, id: &GlobalId) -> &dyn CatalogItem;

    /// Gets all items.
    fn get_items(&self) -> Vec<&dyn CatalogItem>;

    /// Looks up an item by its name.
    fn get_item_by_name(&self, name: &QualifiedItemName) -> Option<&dyn CatalogItem>;

    /// Looks up a type by its name.
    fn get_type_by_name(&self, name: &QualifiedItemName) -> Option<&dyn CatalogItem>;

    /// Gets a cluster by ID.
    fn get_cluster(&self, id: ClusterId) -> &dyn CatalogCluster;

    /// Gets all clusters.
    fn get_clusters(&self) -> Vec<&dyn CatalogCluster>;

    /// Gets a cluster replica by ID.
    fn get_cluster_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &dyn CatalogClusterReplica;

    /// Gets all cluster replicas.
    fn get_cluster_replicas(&self) -> Vec<&dyn CatalogClusterReplica>;

    /// Gets all system privileges.
    fn get_system_privileges(&self) -> &PrivilegeMap;

    /// Gets all default privileges.
    fn get_default_privileges(
        &self,
    ) -> Vec<(&DefaultPrivilegeObject, Vec<&DefaultPrivilegeAclItem>)>;

    /// Finds a name like `name` that is not already in use.
    ///
    /// If `name` itself is available, it is returned unchanged.
    fn find_available_name(&self, name: QualifiedItemName) -> QualifiedItemName;

    /// Returns a fully qualified human readable name from fully qualified non-human readable name
    fn resolve_full_name(&self, name: &QualifiedItemName) -> FullItemName;

    /// Returns a fully qualified human readable schema name from fully qualified non-human
    /// readable schema name
    fn resolve_full_schema_name(&self, name: &QualifiedSchemaName) -> FullSchemaName;

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

    /// Returns the [`RoleId`] of the owner of an object by its ID.
    fn get_owner_id(&self, id: &ObjectId) -> Option<RoleId>;

    /// Returns the [`PrivilegeMap`] of the object.
    fn get_privileges(&self, id: &SystemObjectId) -> Option<&PrivilegeMap>;

    /// Returns all the IDs of all objects that depend on `ids`, including `ids` themselves.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    fn object_dependents(&self, ids: &Vec<ObjectId>) -> Vec<ObjectId>;

    /// Returns all the IDs of all objects that depend on `id`, including `id` themselves.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than `id`. This is particularly userful for the order to drop
    /// objects.
    fn item_dependents(&self, id: GlobalId) -> Vec<ObjectId>;

    /// Returns all possible privileges associated with an object type.
    fn all_object_privileges(&self, object_type: SystemObjectType) -> AclMode;

    /// Returns the object type of `object_id`.
    fn get_object_type(&self, object_id: &ObjectId) -> ObjectType;

    /// Returns the system object type of `id`.
    fn get_system_object_type(&self, id: &SystemObjectId) -> SystemObjectType;

    /// Returns the minimal qualification required to unambiguously specify
    /// `qualified_name`.
    fn minimal_qualification(&self, qualified_name: &QualifiedItemName) -> PartialItemName;

    /// Adds a [`PlanNotice`] that will be displayed to the user if the plan
    /// successfully executes.
    fn add_notice(&self, notice: PlanNotice);

    /// Returns the associated comments for the given `id`
    fn get_item_comments(&self, id: &GlobalId) -> Option<&BTreeMap<Option<usize>, String>>;
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
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// Default timestamp interval.
    pub timestamp_interval: Duration,
    /// Function that returns a wall clock now time; can safely be mocked to return
    /// 0.
    pub now: NowFn,
    /// Context for source and sink connections.
    pub connection_context: ConnectionContext,
}

/// A database in a [`SessionCatalog`].
pub trait CatalogDatabase {
    /// Returns a fully-specified name of the database.
    fn name(&self) -> &str;

    /// Returns a stable ID for the database.
    fn id(&self) -> DatabaseId;

    /// Returns whether the database contains schemas.
    fn has_schemas(&self) -> bool;

    /// Returns the schemas of the database as a map from schema name to
    /// schema ID.
    fn schema_ids(&self) -> &BTreeMap<String, SchemaId>;

    /// Returns the schemas of the database.
    fn schemas(&self) -> Vec<&dyn CatalogSchema>;

    /// Returns the ID of the owning role.
    fn owner_id(&self) -> RoleId;

    /// Returns the privileges associated with the database.
    fn privileges(&self) -> &PrivilegeMap;
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

    /// Returns the IDs of the items in the schema.
    fn item_ids(&self) -> Box<dyn Iterator<Item = GlobalId> + '_>;

    /// Returns the ID of the owning role.
    fn owner_id(&self) -> RoleId;

    /// Returns the privileges associated with the schema.
    fn privileges(&self) -> &PrivilegeMap;
}

/// Attributes belonging to a [`CatalogRole`].
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Arbitrary)]
pub struct RoleAttributes {
    /// Indicates whether the role has inheritance of privileges.
    pub inherit: bool,
    // Force use of constructor.
    _private: (),
}

impl RoleAttributes {
    /// Creates a new [`RoleAttributes`] with default attributes.
    pub const fn new() -> RoleAttributes {
        RoleAttributes {
            inherit: true,
            _private: (),
        }
    }

    /// Adds all attributes.
    pub const fn with_all(mut self) -> RoleAttributes {
        self.inherit = true;
        self
    }

    /// Returns whether or not the role has inheritence of privileges.
    pub const fn is_inherit(&self) -> bool {
        self.inherit
    }
}

impl From<PlannedRoleAttributes> for RoleAttributes {
    fn from(PlannedRoleAttributes { inherit }: PlannedRoleAttributes) -> RoleAttributes {
        let default_attributes = RoleAttributes::new();
        RoleAttributes {
            inherit: inherit.unwrap_or(default_attributes.inherit),
            _private: (),
        }
    }
}

/// Default variable values for a [`CatalogRole`].
#[derive(Default, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct RoleVars {
    /// Map of variable names to their value.
    pub map: BTreeMap<String, OwnedVarInput>,
}

/// A role in a [`SessionCatalog`].
pub trait CatalogRole {
    /// Returns a fully-specified name of the role.
    fn name(&self) -> &str;

    /// Returns a stable ID for the role.
    fn id(&self) -> RoleId;

    /// Returns all role IDs that this role is an immediate a member of, and the grantor of that
    /// membership.
    ///
    /// Key is the role that some role is a member of, value is the grantor role ID.
    fn membership(&self) -> &BTreeMap<RoleId, RoleId>;

    /// Returns the attributes associated with this role.
    fn attributes(&self) -> &RoleAttributes;

    /// Returns all variables that this role has a default value stored for.
    fn vars(&self) -> &BTreeMap<String, OwnedVarInput>;
}

/// A cluster in a [`SessionCatalog`].
pub trait CatalogCluster<'a> {
    /// Returns a fully-specified name of the cluster.
    fn name(&self) -> &str;

    /// Returns a stable ID for the cluster.
    fn id(&self) -> ClusterId;

    /// Returns the objects that are bound to this cluster.
    fn bound_objects(&self) -> &BTreeSet<GlobalId>;

    /// Returns the replicas of the cluster as a map from replica name to
    /// replica ID.
    fn replica_ids(&self) -> &BTreeMap<String, ReplicaId>;

    /// Returns the replicas of the cluster.
    fn replicas(&self) -> Vec<&dyn CatalogClusterReplica>;

    /// Returns the replica belonging to the cluster with replica ID `id`.
    fn replica(&self, id: ReplicaId) -> &dyn CatalogClusterReplica;

    /// Returns the ID of the owning role.
    fn owner_id(&self) -> RoleId;

    /// Returns the privileges associated with the cluster.
    fn privileges(&self) -> &PrivilegeMap;

    /// Returns true if this cluster is a managed cluster.
    fn is_managed(&self) -> bool;

    /// Returns the size of the cluster, if the cluster is a managed cluster.
    fn managed_size(&self) -> Option<&str>;
}

/// A cluster replica in a [`SessionCatalog`]
pub trait CatalogClusterReplica<'a>: Debug {
    /// Returns the name of the cluster replica.
    fn name(&self) -> &str;

    /// Returns a stable ID for the cluster that the replica belongs to.
    fn cluster_id(&self) -> ClusterId;

    /// Returns a stable ID for the replica.
    fn replica_id(&self) -> ReplicaId;

    /// Returns the ID of the owning role.
    fn owner_id(&self) -> RoleId;

    /// Returns whether or not the replica is internal
    fn internal(&self) -> bool;
}

/// An item in a [`SessionCatalog`].
///
/// Note that "item" has a very specific meaning in the context of a SQL
/// catalog, and refers to the various entities that belong to a schema.
pub trait CatalogItem {
    /// Returns the fully qualified name of the catalog item.
    fn name(&self) -> &QualifiedItemName;

    /// Returns a stable ID for the catalog item.
    fn id(&self) -> GlobalId;

    /// Returns the catalog item's OID.
    fn oid(&self) -> u32;

    /// Returns a description of the result set produced by the catalog item.
    ///
    /// If the catalog item is not of a type that produces data (i.e., a sink or
    /// an index), it returns an error.
    fn desc(&self, name: &FullItemName) -> Result<Cow<RelationDesc>, CatalogError>;

    /// Returns the resolved function.
    ///
    /// If the catalog item is not of a type that produces functions (i.e.,
    /// anything other than a function), it returns an error.
    fn func(&self) -> Result<&'static Func, CatalogError>;

    /// Returns the resolved source connection.
    ///
    /// If the catalog item is not of a type that contains a `SourceDesc`
    /// (i.e., anything other than sources), it returns an error.
    fn source_desc(&self) -> Result<Option<&SourceDesc<ReferencedConnection>>, CatalogError>;

    /// Returns the resolved connection.
    ///
    /// If the catalog item is not a connection, it returns an error.
    fn connection(&self) -> Result<&Connection<ReferencedConnection>, CatalogError>;

    /// Returns the type of the catalog item.
    fn item_type(&self) -> CatalogItemType;

    /// A normalized SQL statement that describes how to create the catalog
    /// item.
    fn create_sql(&self) -> &str;

    /// Returns the IDs of the catalog items upon which this catalog item
    /// directly references.
    fn references(&self) -> &ResolvedIds;

    /// Returns the IDs of the catalog items upon which this catalog item
    /// depends.
    fn uses(&self) -> BTreeSet<GlobalId>;

    /// Returns the IDs of the catalog items that directly reference this catalog item.
    fn referenced_by(&self) -> &[GlobalId];

    /// Returns the IDs of the catalog items that depend upon this catalog item.
    fn used_by(&self) -> &[GlobalId];

    /// Reports whether this catalog item is a subsource.
    fn is_subsource(&self) -> bool;

    /// Reports whether this catalog item is a progress source.
    fn is_progress_source(&self) -> bool;

    /// If this catalog item is a source, it return the IDs of its subsources.
    fn subsources(&self) -> BTreeSet<GlobalId>;

    /// If this catalog item is a source, it return the IDs of its progress collection.
    fn progress_id(&self) -> Option<GlobalId>;

    /// Returns the index details associated with the catalog item, if the
    /// catalog item is an index.
    fn index_details(&self) -> Option<(&[MirScalarExpr], GlobalId)>;

    /// Returns the column defaults associated with the catalog item, if the
    /// catalog item is a table.
    fn table_details(&self) -> Option<&[Expr<Aug>]>;

    /// Returns the type information associated with the catalog item, if the
    /// catalog item is a type.
    fn type_details(&self) -> Option<&CatalogTypeDetails<IdReference>>;

    /// Returns the ID of the owning role.
    fn owner_id(&self) -> RoleId;

    /// Returns the privileges associated with the item.
    fn privileges(&self) -> &PrivilegeMap;

    /// Returns the cluster the item belongs to.
    fn cluster_id(&self) -> Option<ClusterId>;
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
    /// compatibility (see #23789). For now, we use this method to prevent
    /// creating types and relational objects that have the same name, so that
    /// it is a backwards compatible change in the future to introduce a type
    /// named after each relational object in the system.
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

/// Details about a type in the catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogTypeDetails<T: TypeReference> {
    /// The ID of the type with this type as the array element, if available.
    pub array_id: Option<GlobalId>,
    /// The description of this type.
    pub typ: CatalogType<T>,
    /// Additional metadata about the type in PostgreSQL, if relevant.
    pub pg_metadata: Option<CatalogTypePgMetadata>,
}

/// Additional PostgreSQL metadata about a type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogTypePgMetadata {
    /// The OID of the `typinput` function in PostgreSQL.
    pub typinput_oid: u32,
    /// The OID of the `typreceive` function in PostgreSQL.
    pub typreceive_oid: u32,
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
    AclItem,
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
        element_modifiers: Vec<i64>,
    },
    Map {
        key_reference: T::Reference,
        key_modifiers: Vec<i64>,
        value_reference: T::Reference,
        value_modifiers: Vec<i64>,
    },
    Numeric,
    Oid,
    PgLegacyChar,
    PgLegacyName,
    Pseudo,
    Range {
        element_reference: T::Reference,
    },
    Record {
        fields: Vec<CatalogRecordField<T>>,
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
    MzAclItem,
}

impl CatalogType<IdReference> {
    /// Returns the relation description for the type, if the type is a record
    /// type.
    pub fn desc(&self, catalog: &dyn SessionCatalog) -> Result<Option<RelationDesc>, PlanError> {
        match &self {
            CatalogType::Record { fields } => {
                let mut desc = RelationDesc::empty();
                for f in fields {
                    let name = f.name.clone();
                    let ty = query::scalar_type_from_catalog(
                        catalog,
                        f.type_reference,
                        &f.type_modifiers,
                    )?;
                    // TODO: support plumbing `NOT NULL` constraints through
                    // `CREATE TYPE`.
                    let ty = ty.nullable(true);
                    desc = desc.with_column(name, ty);
                }
                Ok(Some(desc))
            }
            _ => Ok(None),
        }
    }
}

/// A description of a field in a [`CatalogType::Record`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogRecordField<T: TypeReference> {
    /// The name of the field.
    pub name: ColumnName,
    /// The ID of the type of the field.
    pub type_reference: T::Reference,
    /// Modifiers to apply to the type.
    pub type_modifiers: Vec<i64>,
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
    /// Unknown cluster.
    UnknownCluster(String),
    /// Cluster already exists.
    ClusterAlreadyExists(String),
    /// Unknown cluster replica.
    UnknownClusterReplica(String),
    /// Duplicate Replica. #[error("cannot create multiple replicas named '{0}' on cluster '{1}'")]
    DuplicateReplica(String, String),
    /// Unknown item.
    UnknownItem(String),
    /// Item already exists.
    ItemAlreadyExists(GlobalId, String),
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
    /// StorageMetadata already exists.
    StorageMetadataAlreadyExists(GlobalId),
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
            Self::UnknownCluster(name) => write!(f, "unknown cluster '{}'", name),
            Self::ClusterAlreadyExists(name) => write!(f, "cluster '{name}' already exists"),
            Self::UnknownClusterReplica(name) => {
                write!(f, "unknown cluster replica '{}'", name)
            }
            Self::DuplicateReplica(replica_name, cluster_name) => write!(f, "cannot create multiple replicas named '{replica_name}' on cluster '{cluster_name}'"),
            Self::UnknownItem(name) => write!(f, "unknown catalog item '{}'", name),
            Self::ItemAlreadyExists(_gid, name) => write!(f, "catalog item '{name}' already exists"),
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
            Self::IdExhaustion => write!(f, "id counter overflows i64"),
            Self::OidExhaustion => write!(f, "oid counter overflows u32"),
            Self::TimelineAlreadyExists(name) => write!(f, "timeline '{name}' already exists"),
            Self::IdAllocatorAlreadyExists(name) => write!(f, "ID allocator '{name}' already exists"),
            Self::ConfigAlreadyExists(key) => write!(f, "config '{key}' already exists"),
            Self::FailedBuiltinSchemaMigration(objects) => write!(f, "failed to migrate schema of builtin objects: {objects}"),
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

/// Provides a method of generating a 3-layer catalog on the fly, and then
/// resolving objects within it.
pub(crate) struct SubsourceCatalog<T>(pub BTreeMap<String, BTreeMap<String, BTreeMap<String, T>>>);

impl<T> SubsourceCatalog<T> {
    /// Returns the fully qualified name for `item`, as well as the `T` that it
    /// describes.
    ///
    /// # Errors
    /// - If `item` cannot be normalized to a [`PartialItemName`]
    /// - If the normalized `PartialItemName` does not resolve to an item in
    ///   `self.0`.
    pub(crate) fn resolve(
        &self,
        item: UnresolvedItemName,
    ) -> Result<(UnresolvedItemName, &T), PlanError> {
        let name = normalize::unresolved_item_name(item)?;

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
            Some(desc) => desc,
            None => sql_bail!("database {database} not found source"),
        };

        // Note: Using unchecked here is okay because all of these values were originally Idents.
        Ok((
            UnresolvedItemName::qualified(&[
                Ident::new_unchecked(database),
                Ident::new_unchecked(schema),
                Ident::new_unchecked(&name.item),
            ]),
            desc,
        ))
    }
}

// Enum variant docs would be useless here.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash, Copy, Deserialize, Serialize)]
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
            | ObjectType::Role => false,
        }
    }
}

impl From<mz_sql_parser::ast::ObjectType> for ObjectType {
    fn from(value: mz_sql_parser::ast::ObjectType) -> Self {
        match value {
            mz_sql_parser::ast::ObjectType::Table => ObjectType::Table,
            mz_sql_parser::ast::ObjectType::View => ObjectType::View,
            mz_sql_parser::ast::ObjectType::MaterializedView => ObjectType::MaterializedView,
            mz_sql_parser::ast::ObjectType::Source => ObjectType::Source,
            mz_sql_parser::ast::ObjectType::Subsource => ObjectType::Source,
            mz_sql_parser::ast::ObjectType::Sink => ObjectType::Sink,
            mz_sql_parser::ast::ObjectType::Index => ObjectType::Index,
            mz_sql_parser::ast::ObjectType::Type => ObjectType::Type,
            mz_sql_parser::ast::ObjectType::Role => ObjectType::Role,
            mz_sql_parser::ast::ObjectType::Cluster => ObjectType::Cluster,
            mz_sql_parser::ast::ObjectType::ClusterReplica => ObjectType::ClusterReplica,
            mz_sql_parser::ast::ObjectType::Secret => ObjectType::Secret,
            mz_sql_parser::ast::ObjectType::Connection => ObjectType::Connection,
            mz_sql_parser::ast::ObjectType::Database => ObjectType::Database,
            mz_sql_parser::ast::ObjectType::Schema => ObjectType::Schema,
            mz_sql_parser::ast::ObjectType::Func => ObjectType::Func,
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
        })
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash, Copy, Deserialize, Serialize)]
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

/// Enum used to format object names in error messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorMessageObjectDescription {
    /// The name of a specific object.
    Object {
        /// Type of object.
        object_type: ObjectType,
        /// Name of object.
        object_name: Option<String>,
    },
    /// The name of the entire system.
    System,
}

impl ErrorMessageObjectDescription {
    /// Generate a new [`ErrorMessageObjectDescription`] from an [`ObjectId`].
    pub fn from_id(
        object_id: &ObjectId,
        catalog: &dyn SessionCatalog,
    ) -> ErrorMessageObjectDescription {
        let object_name = match object_id {
            ObjectId::Cluster(cluster_id) => catalog.get_cluster(*cluster_id).name().to_string(),
            ObjectId::ClusterReplica((cluster_id, replica_id)) => catalog
                .get_cluster_replica(*cluster_id, *replica_id)
                .name()
                .to_string(),
            ObjectId::Database(database_id) => catalog.get_database(database_id).name().to_string(),
            ObjectId::Schema((database_spec, schema_spec)) => {
                let name = catalog.get_schema(database_spec, schema_spec).name();
                catalog.resolve_full_schema_name(name).to_string()
            }
            ObjectId::Role(role_id) => catalog.get_role(role_id).name().to_string(),
            ObjectId::Item(id) => {
                let name = catalog.get_item(id).name();
                catalog.resolve_full_name(name).to_string()
            }
        };
        ErrorMessageObjectDescription::Object {
            object_type: catalog.get_object_type(object_id),
            object_name: Some(object_name),
        }
    }

    /// Generate a new [`ErrorMessageObjectDescription`] from a [`SystemObjectId`].
    pub fn from_sys_id(
        object_id: &SystemObjectId,
        catalog: &dyn SessionCatalog,
    ) -> ErrorMessageObjectDescription {
        match object_id {
            SystemObjectId::Object(object_id) => {
                ErrorMessageObjectDescription::from_id(object_id, catalog)
            }
            SystemObjectId::System => ErrorMessageObjectDescription::System,
        }
    }

    /// Generate a new [`ErrorMessageObjectDescription`] from a [`SystemObjectType`].
    pub fn from_object_type(object_type: SystemObjectType) -> ErrorMessageObjectDescription {
        match object_type {
            SystemObjectType::Object(object_type) => ErrorMessageObjectDescription::Object {
                object_type,
                object_name: None,
            },
            SystemObjectType::System => ErrorMessageObjectDescription::System,
        }
    }
}

impl Display for ErrorMessageObjectDescription {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorMessageObjectDescription::Object {
                object_type,
                object_name,
            } => {
                let object_name = object_name
                    .as_ref()
                    .map(|object_name| format!(" {}", object_name.quoted()))
                    .unwrap_or_else(|| "".to_string());
                write!(f, "{object_type}{object_name}")
            }
            ErrorMessageObjectDescription::System => f.write_str("SYSTEM"),
        }
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
