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
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc, MIN_DATETIME};
use once_cell::sync::Lazy;

use mz_build_info::{BuildInfo, DUMMY_BUILD_INFO};
use mz_dataflow_types::client::ComputeInstanceId;
use mz_dataflow_types::sources::SourceConnector;
use mz_expr::{DummyHumanizer, ExprHumanizer, MirScalarExpr};
use mz_ore::now::{EpochMillis, NowFn, NOW_ZERO};
use mz_repr::{ColumnName, GlobalId, RelationDesc, ScalarType};
use mz_sql_parser::ast::Expr;
use uuid::Uuid;

use crate::func::Func;
use crate::names::{
    Aug, DatabaseId, FullObjectName, PartialObjectName, QualifiedObjectName, QualifiedSchemaName,
    ResolvedDatabaseSpecifier, SchemaSpecifier,
};
use crate::plan::statement::StatementDesc;

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
pub trait SessionCatalog: fmt::Debug + ExprHumanizer {
    /// Returns the name of the user who is issuing the query.
    fn active_user(&self) -> &str;

    /// Returns the database to use if one is not explicitly specified.
    fn active_database_name(&self) -> Option<&str> {
        self.active_database()
            .map(|id| self.get_database(id))
            .map(|db| db.name())
    }

    /// Returns the database to use if one is not explicitly specified.
    fn active_database(&self) -> Option<&DatabaseId>;

    /// Returns the compute instance to use if one is not explicitly specified.
    fn active_compute_instance(&self) -> &str;

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

    /// Resolves the named compute instance.
    ///
    /// If the provided name is `None`, resolves the currently-active compute
    /// instance.
    fn resolve_compute_instance<'a, 'b>(
        &'a self,
        compute_instance_name: Option<&'b str>,
    ) -> Result<&dyn CatalogComputeInstance<'a>, CatalogError>;

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
    /// A persistent UUID associated with the catalog.
    pub cluster_id: Uuid,
    /// A transient UUID associated with this process.
    pub session_id: Uuid,
    /// Whether the server is running in unsafe mode.
    pub unsafe_mode: bool,
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// Default timestamp frequency for CREATE SOURCE
    pub timestamp_frequency: Duration,
    /// Function that returns a wall clock now time; can safely be mocked to return
    /// 0.
    pub now: NowFn,
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

/// A role in a [`SessionCatalog`].
pub trait CatalogRole {
    /// Returns a fully-specified name of the role.
    fn name(&self) -> &str;

    /// Returns a stable ID for the role.
    fn id(&self) -> i64;
}

/// A compute instance in a [`SessionCatalog`].
pub trait CatalogComputeInstance<'a> {
    /// Returns a fully-specified name of the compute instance.
    fn name(&self) -> &str;

    /// Returns a stable ID for the compute instance.
    fn id(&self) -> ComputeInstanceId;

    /// Returns the set of non-transient indexes on this cluster.
    fn indexes(&self) -> &std::collections::HashSet<GlobalId>;

    /// Returns the set of non-transient indexes on this cluster.
    fn replica_names(&self) -> HashSet<&String>;
}

/// A connector in a [`SessionCatalog`]
pub trait CatalogConnector {
    /// Returns the connection URI for this connector regardless of type
    fn uri(&self) -> String;

    /// Returns the options associated with this connector as a Vec, if the type does not support options or none were specified this will be empty
    fn options(&self) -> std::collections::BTreeMap<String, String>;
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

    /// Returns the resolved source connector.
    ///
    /// If the catalog item is not of a type that contains a `SourceConnector`
    /// (i.e., anything other than sources), it returns an error.
    fn source_connector(&self) -> Result<&SourceConnector, CatalogError>;

    /// Returns the resolved connector, called a catalog connector to disambiguate
    ///
    /// If the catalog item is not of a type that implements `CatalogConnector`
    /// it returns an error
    fn catalog_connector(&self) -> Result<&dyn CatalogConnector, CatalogError>;

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
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CatalogItemType {
    /// A table.
    Table,
    /// A source.
    Source,
    /// A sink.
    Sink,
    /// A view.
    View,
    /// An index.
    Index,
    /// A type.
    Type,
    /// A func.
    Func,
    /// A Secret.
    Secret,
    /// A Connector.
    Connector,
}

impl fmt::Display for CatalogItemType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CatalogItemType::Table => f.write_str("table"),
            CatalogItemType::Source => f.write_str("source"),
            CatalogItemType::Sink => f.write_str("sink"),
            CatalogItemType::View => f.write_str("view"),
            CatalogItemType::Index => f.write_str("index"),
            CatalogItemType::Type => f.write_str("type"),
            CatalogItemType::Func => f.write_str("func"),
            CatalogItemType::Secret => f.write_str("secret"),
            CatalogItemType::Connector => f.write_str("connector"),
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
/// The variants correspond one-to-one with [`ScalarType`], but with type
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

/// An error returned by the catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogError {
    /// Unknown database.
    UnknownDatabase(String),
    /// Unknown schema.
    UnknownSchema(String),
    /// Unknown role.
    UnknownRole(String),
    /// Unknown compute instance.
    UnknownComputeInstance(String),
    /// Unknown compute instance replica.
    UnknownComputeInstanceReplica(String),
    /// Unknown item.
    UnknownItem(String),
    /// Unknown function.
    UnknownFunction(String),
    /// Unknown source.
    UnknownSource(String),
    /// Unknown connector.
    UnknownConnector(String),
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
            Self::UnknownSource(name) => write!(f, "source \"{}\" does not exist", name),
            Self::UnknownConnector(name) => write!(f, "connector \"{}\" does not exist", name),
            Self::UnknownSchema(name) => write!(f, "unknown schema '{}'", name),
            Self::UnknownRole(name) => write!(f, "unknown role '{}'", name),
            Self::UnknownComputeInstance(name) => write!(f, "unknown cluster '{}'", name),
            Self::UnknownComputeInstanceReplica(name) => {
                write!(f, "unknown cluster replica '{}'", name)
            }
            Self::UnknownItem(name) => write!(f, "unknown catalog item '{}'", name),
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

/// A dummy [`SessionCatalog`] implementation.
///
/// This implementation is suitable for use in tests that plan queries which are
/// not demanding of the catalog, as many methods are unimplemented.
#[derive(Debug)]
pub struct DummyCatalog;

static DUMMY_CONFIG: Lazy<CatalogConfig> = Lazy::new(|| CatalogConfig {
    start_time: MIN_DATETIME,
    start_instant: Instant::now(),
    nonce: 0,
    cluster_id: Uuid::from_u128(0),
    session_id: Uuid::from_u128(0),
    unsafe_mode: true,
    build_info: &DUMMY_BUILD_INFO,
    timestamp_frequency: Duration::from_secs(1),
    now: NOW_ZERO.clone(),
});

impl SessionCatalog for DummyCatalog {
    fn active_user(&self) -> &str {
        "dummy"
    }

    fn active_database(&self) -> Option<&DatabaseId> {
        Some(&DatabaseId(0))
    }

    fn active_compute_instance(&self) -> &str {
        "dummy"
    }

    fn get_prepared_statement_desc(&self, _: &str) -> Option<&StatementDesc> {
        None
    }

    fn resolve_database(&self, _: &str) -> Result<&dyn CatalogDatabase, CatalogError> {
        unimplemented!()
    }

    fn get_database(&self, _: &DatabaseId) -> &dyn CatalogDatabase {
        &DummyDatabase
    }

    fn resolve_schema(&self, _: Option<&str>, _: &str) -> Result<&dyn CatalogSchema, CatalogError> {
        unimplemented!()
    }

    fn resolve_schema_in_database(
        &self,
        _: &ResolvedDatabaseSpecifier,
        _: &str,
    ) -> Result<&dyn CatalogSchema, CatalogError> {
        unimplemented!()
    }

    fn get_schema(&self, _: &ResolvedDatabaseSpecifier, _: &SchemaSpecifier) -> &dyn CatalogSchema {
        unimplemented!()
    }

    fn is_system_schema(&self, _: &str) -> bool {
        false
    }

    fn resolve_role(&self, _: &str) -> Result<&dyn CatalogRole, CatalogError> {
        unimplemented!();
    }

    fn resolve_item(&self, _: &PartialObjectName) -> Result<&dyn CatalogItem, CatalogError> {
        unimplemented!();
    }

    fn resolve_function(&self, _: &PartialObjectName) -> Result<&dyn CatalogItem, CatalogError> {
        unimplemented!();
    }

    fn resolve_compute_instance<'a, 'b>(
        &'a self,
        _: Option<&'b str>,
    ) -> Result<&'a dyn CatalogComputeInstance, CatalogError> {
        unimplemented!();
    }

    fn get_item(&self, _: &GlobalId) -> &dyn CatalogItem {
        unimplemented!();
    }

    fn try_get_item(&self, _: &GlobalId) -> Option<&dyn CatalogItem> {
        unimplemented!();
    }

    fn item_exists(&self, _: &QualifiedObjectName) -> bool {
        false
    }

    fn resolve_full_name(&self, _: &QualifiedObjectName) -> FullObjectName {
        unimplemented!()
    }

    fn config(&self) -> &CatalogConfig {
        &DUMMY_CONFIG
    }

    fn now(&self) -> EpochMillis {
        (self.config().now)()
    }

    fn find_available_name(&self, name: QualifiedObjectName) -> QualifiedObjectName {
        name
    }
}

impl ExprHumanizer for DummyCatalog {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        DummyHumanizer.humanize_id(id)
    }

    fn humanize_scalar_type(&self, ty: &ScalarType) -> String {
        DummyHumanizer.humanize_scalar_type(ty)
    }
}

/// A dummy [`CatalogDatabase`] implementation.
///
/// This implementation is suitable for use in tests that plan queries which are
/// not demanding of the catalog, as many methods are unimplemented.
#[derive(Debug)]
pub struct DummyDatabase;

impl CatalogDatabase for DummyDatabase {
    fn name(&self) -> &str {
        "dummy"
    }

    fn id(&self) -> DatabaseId {
        DatabaseId(0)
    }

    fn has_schemas(&self) -> bool {
        true
    }
}
