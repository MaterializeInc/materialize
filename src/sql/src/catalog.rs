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
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::time::Instant;

use chrono::{DateTime, Utc};
use mz_build_info::BuildInfo;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::MirScalarExpr;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::str::StrExt;
use mz_repr::adt::mz_acl_item::{AclMode, PrivilegeMap};
use mz_repr::explain::ExprHumanizer;
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{
    CatalogItemId, ColumnName, GlobalId, RelationDesc, RelationVersion, RelationVersionSelector,
};
use mz_sql_parser::ast::{Expr, QualifiedReplica, UnresolvedItemName};
use mz_storage_types::connections::inline::{ConnectionResolver, ReferencedConnection};
use mz_storage_types::connections::{Connection, ConnectionContext};
use mz_storage_types::sources::{SourceDesc, SourceExportDataConfig, SourceExportDetails};
use uuid::Uuid;

use crate::func::Func;
use crate::names::{
    Aug, DatabaseId, FullItemName, FullSchemaName, ObjectId, PartialItemName, QualifiedItemName,
    QualifiedSchemaName, ResolvedDatabaseSpecifier, ResolvedIds, SchemaId, SchemaSpecifier,
    SystemObjectId,
};
use crate::plan::statement::StatementDesc;
use crate::plan::statement::ddl::PlannedRoleAttributes;
use crate::plan::{ClusterSchedule, CreateClusterPlan, PlanError, PlanNotice, query};
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
/// [`get_databases`]: SessionCatalog::get_databases
/// [`get_item`]: SessionCatalog::get_item
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

    /// Retrieves a reference to the specified portal's descriptor.
    ///
    /// If there is no such portal, returns `None`.
    fn get_portal_desc_unverified(&self, portal_name: &str) -> Option<&StatementDesc>;

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
    fn get_mz_internal_schema_id(&self) -> SchemaId;

    /// Gets the mz_unsafe schema id.
    fn get_mz_unsafe_schema_id(&self) -> SchemaId;

    /// Returns true if `schema` is an internal system schema, false otherwise
    fn is_system_schema_specifier(&self, schema: SchemaSpecifier) -> bool;

    /// Resolves the named role.
    fn resolve_role(&self, role_name: &str) -> Result<&dyn CatalogRole, CatalogError>;

    /// Resolves the named network policy.
    fn resolve_network_policy(
        &self,
        network_policy_name: &str,
    ) -> Result<&dyn CatalogNetworkPolicy, CatalogError>;

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
    /// Gets a network_policy by its ID.
    ///
    /// Panics if `id` does not specify a valid role.
    fn get_network_policy(&self, id: &NetworkPolicyId) -> &dyn CatalogNetworkPolicy;

    /// Gets all roles.
    fn get_network_policies(&self) -> Vec<&dyn CatalogNetworkPolicy>;

    ///
    /// If the provided name is `None`, resolves the currently active cluster.
    fn resolve_cluster<'a, 'b>(
        &'a self,
        cluster_name: Option<&'b str>,
    ) -> Result<&'a dyn CatalogCluster<'a>, CatalogError>;

    /// Resolves the named cluster replica.
    fn resolve_cluster_replica<'a, 'b>(
        &'a self,
        cluster_replica_name: &'b QualifiedReplica,
    ) -> Result<&'a dyn CatalogClusterReplica<'a>, CatalogError>;

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
    fn try_get_item(&self, id: &CatalogItemId) -> Option<&dyn CatalogItem>;

    /// Tries to get an item by a [`GlobalId`], returning `None` if the [`GlobalId`] does not
    /// exist.
    ///
    /// Note: A single Catalog Item can have multiple [`GlobalId`]s associated with it.
    fn try_get_item_by_global_id<'a>(
        &'a self,
        id: &GlobalId,
    ) -> Option<Box<dyn CatalogCollectionItem + 'a>>;

    /// Gets an item by its ID.
    ///
    /// Panics if `id` does not specify a valid item.
    fn get_item(&self, id: &CatalogItemId) -> &dyn CatalogItem;

    /// Gets an item by a [`GlobalId`].
    ///
    /// Panics if `id` does not specify a valid item.
    ///
    /// Note: A single Catalog Item can have multiple [`GlobalId`]s associated with it.
    fn get_item_by_global_id<'a>(&'a self, id: &GlobalId) -> Box<dyn CatalogCollectionItem + 'a>;

    /// Gets all items.
    fn get_items(&self) -> Vec<&dyn CatalogItem>;

    /// Looks up an item by its name.
    fn get_item_by_name(&self, name: &QualifiedItemName) -> Option<&dyn CatalogItem>;

    /// Looks up a type by its name.
    fn get_type_by_name(&self, name: &QualifiedItemName) -> Option<&dyn CatalogItem>;

    /// Gets a cluster by ID.
    fn get_cluster(&self, id: ClusterId) -> &dyn CatalogCluster<'_>;

    /// Gets all clusters.
    fn get_clusters(&self) -> Vec<&dyn CatalogCluster<'_>>;

    /// Gets a cluster replica by ID.
    fn get_cluster_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &dyn CatalogClusterReplica<'_>;

    /// Gets all cluster replicas.
    fn get_cluster_replicas(&self) -> Vec<&dyn CatalogClusterReplica<'_>>;

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

    /// Returns the [`CatalogItemId`] for from a [`GlobalId`].
    fn resolve_item_id(&self, global_id: &GlobalId) -> CatalogItemId;

    /// Returns the [`GlobalId`] for the specificed Catalog Item, at the specified version.
    fn resolve_global_id(
        &self,
        item_id: &CatalogItemId,
        version: RelationVersionSelector,
    ) -> GlobalId;

    /// Returns the configuration of the catalog.
    fn config(&self) -> &CatalogConfig;

    /// Returns the number of milliseconds since the system epoch. For normal use
    /// this means the Unix epoch. This can safely be mocked in tests and start
    /// at 0.
    fn now(&self) -> EpochMillis;

    /// Returns the set of supported AWS PrivateLink availability zone ids.
    fn aws_privatelink_availability_zones(&self) -> Option<BTreeSet<String>>;

    /// Returns true if the session has `restrict_to_user_objects` active.
    ///
    /// Defaults to false so that non-session catalog implementations (e.g. those used during
    /// catalog rehydration) are unaffected.
    fn restrict_to_user_objects(&self) -> bool {
        false
    }

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
    fn item_dependents(&self, id: CatalogItemId) -> Vec<ObjectId>;

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
    fn get_item_comments(&self, id: &CatalogItemId) -> Option<&BTreeMap<Option<usize>, String>>;

    /// Reports whether the specified cluster size is a modern "cc" size rather
    /// than a legacy T-shirt size.
    fn is_cluster_size_cc(&self, size: &str) -> bool;
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
    /// topics. Perhaps we can remove this when database-issues#977 is complete.
    pub nonce: u64,
    /// A persistent ID associated with the environment.
    pub environment_id: EnvironmentId,
    /// A transient UUID associated with this process.
    pub session_id: Uuid,
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// Function that returns a wall clock now time; can safely be mocked to return
    /// 0.
    pub now: NowFn,
    /// Context for source and sink connections.
    pub connection_context: ConnectionContext,
    /// Helm chart version
    pub helm_chart_version: Option<String>,
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
    fn item_ids(&self) -> Box<dyn Iterator<Item = CatalogItemId> + '_>;

    /// Returns the ID of the owning role.
    fn owner_id(&self) -> RoleId;

    /// Returns the privileges associated with the schema.
    fn privileges(&self) -> &PrivilegeMap;
}

// The following data types live in `mz-sql-types` so that lower-level crates can
// depend on them without pulling in all of `mz-sql`. Re-exported here at their
// original paths.
pub use mz_sql_types::catalog::{
    AutoProvisionSource, CatalogError, CatalogItemType, DefaultPrivilegeAclItem,
    DefaultPrivilegeObject, EnvironmentId, InvalidEnvironmentIdError, ObjectType, PasswordAction,
    PasswordConfig, RoleAttributes, RoleAttributesRaw, RoleMembership, RoleVars, SystemObjectType,
    role_attributes_from_raw,
};

/// Builds a [`RoleAttributesRaw`] from [`PlannedRoleAttributes`].
///
/// This lives here as a free function rather than a `From` impl because
/// [`RoleAttributesRaw`] now lives in `mz-sql-types` (which cannot depend on the
/// planner) and has a private field that must be constructed via its constructor.
pub fn role_attributes_raw_from_planned(planned: PlannedRoleAttributes) -> RoleAttributesRaw {
    let PlannedRoleAttributes {
        inherit,
        password,
        scram_iterations,
        superuser,
        login,
        ..
    } = planned;
    let mut raw = RoleAttributesRaw::new();
    raw.inherit = inherit.unwrap_or(raw.inherit);
    raw.password = password;
    raw.scram_iterations = scram_iterations;
    raw.superuser = superuser;
    raw.login = login;
    raw
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

/// A network policy in a [`SessionCatalog`].
pub trait CatalogNetworkPolicy {
    /// Returns a fully-specified name of the NetworkPolicy.
    fn name(&self) -> &str;

    /// Returns a stable ID for the NetworkPolicy.
    fn id(&self) -> NetworkPolicyId;

    /// Returns the ID of the owning NetworkPolicy.
    fn owner_id(&self) -> RoleId;

    /// Returns the privileges associated with the NetworkPolicy.
    fn privileges(&self) -> &PrivilegeMap;
}

/// A cluster in a [`SessionCatalog`].
pub trait CatalogCluster<'a> {
    /// Returns a fully-specified name of the cluster.
    fn name(&self) -> &str;

    /// Returns a stable ID for the cluster.
    fn id(&self) -> ClusterId;

    /// Returns the objects that are bound to this cluster.
    fn bound_objects(&self) -> &BTreeSet<CatalogItemId>;

    /// Returns the replicas of the cluster as a map from replica name to
    /// replica ID.
    fn replica_ids(&self) -> &BTreeMap<String, ReplicaId>;

    /// Returns the replicas of the cluster.
    fn replicas(&self) -> Vec<&dyn CatalogClusterReplica<'_>>;

    /// Returns the replica belonging to the cluster with replica ID `id`.
    fn replica(&self, id: ReplicaId) -> &dyn CatalogClusterReplica<'_>;

    /// Returns the ID of the owning role.
    fn owner_id(&self) -> RoleId;

    /// Returns the privileges associated with the cluster.
    fn privileges(&self) -> &PrivilegeMap;

    /// Returns true if this cluster is a managed cluster.
    fn is_managed(&self) -> bool;

    /// Returns the size of the cluster, if the cluster is a managed cluster.
    fn managed_size(&self) -> Option<&str>;

    /// Returns the schedule of the cluster, if the cluster is a managed cluster.
    fn schedule(&self) -> Option<&ClusterSchedule>;

    /// Returns the replication factor of the cluster, if the cluster is a managed cluster.
    fn replication_factor(&self) -> Option<u32>;

    /// Try to convert this cluster into a [`CreateClusterPlan`].
    // TODO(jkosh44) Make this infallible and convert to `to_plan`.
    fn try_to_plan(&self) -> Result<CreateClusterPlan, PlanError>;
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

    /// Returns the [`CatalogItemId`] for the item.
    fn id(&self) -> CatalogItemId;

    /// Returns the [`GlobalId`]s associated with this item.
    fn global_ids(&self) -> Box<dyn Iterator<Item = GlobalId> + '_>;

    /// Returns the catalog item's OID.
    fn oid(&self) -> u32;

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
    fn connection(&self) -> Result<Connection<ReferencedConnection>, CatalogError>;

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
    fn uses(&self) -> BTreeSet<CatalogItemId>;

    /// Returns the IDs of the catalog items that directly reference this catalog item.
    fn referenced_by(&self) -> &[CatalogItemId];

    /// Returns the IDs of the catalog items that depend upon this catalog item.
    fn used_by(&self) -> &[CatalogItemId];

    /// Reports whether this catalog entry is a subsource and, if it is, the
    /// ingestion it is an export of, as well as the item it exports.
    fn subsource_details(
        &self,
    ) -> Option<(CatalogItemId, &UnresolvedItemName, &SourceExportDetails)>;

    /// Reports whether this catalog entry is a source export and, if it is, the
    /// ingestion it is an export of, as well as the item it exports.
    fn source_export_details(
        &self,
    ) -> Option<(
        CatalogItemId,
        &UnresolvedItemName,
        &SourceExportDetails,
        &SourceExportDataConfig<ReferencedConnection>,
    )>;

    /// Reports whether this catalog item is a progress source.
    fn is_progress_source(&self) -> bool;

    /// If this catalog item is a source, it return the IDs of its progress collection.
    fn progress_id(&self) -> Option<CatalogItemId>;

    /// Returns the index details associated with the catalog item, if the
    /// catalog item is an index.
    fn index_details(&self) -> Option<(&[MirScalarExpr], GlobalId)>;

    /// Returns the column defaults associated with the catalog item, if the
    /// catalog item is a table that accepts writes.
    fn writable_table_details(&self) -> Option<&[Expr<Aug>]>;

    /// The item this catalog item replaces, if any.
    fn replacement_target(&self) -> Option<CatalogItemId>;

    /// Returns the type information associated with the catalog item, if the
    /// catalog item is a type.
    fn type_details(&self) -> Option<&CatalogTypeDetails<IdReference>>;

    /// Returns the ID of the owning role.
    fn owner_id(&self) -> RoleId;

    /// Returns the privileges associated with the item.
    fn privileges(&self) -> &PrivilegeMap;

    /// Returns the cluster the item belongs to.
    fn cluster_id(&self) -> Option<ClusterId>;

    /// Returns the [`CatalogCollectionItem`] for a specific version of this
    /// [`CatalogItem`].
    fn at_version(&self, version: RelationVersionSelector) -> Box<dyn CatalogCollectionItem>;

    /// The latest version of this item, if it's version-able.
    fn latest_version(&self) -> Option<RelationVersion>;
}

/// An item in a [`SessionCatalog`] and the specific "collection"/pTVC that it
/// refers to.
pub trait CatalogCollectionItem: CatalogItem + Send + Sync {
    /// Returns a description of the result set produced by the catalog item.
    ///
    /// If the catalog item is not of a type that produces data (e.g., a sink or
    /// an index), it returns `None`.
    fn relation_desc(&self) -> Option<Cow<'_, RelationDesc>>;

    /// The [`GlobalId`] for this item.
    fn global_id(&self) -> GlobalId;
}

/// Details about a type in the catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogTypeDetails<T: TypeReference> {
    /// The ID of the type with this type as the array element, if available.
    pub array_id: Option<CatalogItemId>,
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
    type Reference = CatalogItemId;
}

/// A type stored in the catalog.
///
/// The variants correspond one-to-one with [`mz_repr::SqlScalarType`], but with type
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
                let mut desc = RelationDesc::builder();
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
                Ok(Some(desc.finish()))
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

/// Converts a parser AST object type into a catalog [`ObjectType`].
///
/// This lives here rather than as a `From` impl on [`ObjectType`] because that type now
/// lives in `mz-sql-types`, which must not depend on `mz-sql-parser`.
pub fn object_type_from_ast(value: mz_sql_parser::ast::ObjectType) -> ObjectType {
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
        mz_sql_parser::ast::ObjectType::NetworkPolicy => ObjectType::NetworkPolicy,
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
            ObjectId::NetworkPolicy(network_policy_id) => catalog
                .get_network_policy(network_policy_id)
                .name()
                .to_string(),
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
