// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![forbid(missing_docs)]

//! Catalog abstraction layer.

use std::error::Error;
use std::fmt;
use std::time::SystemTime;

use expr::{GlobalId, ScalarExpr};
use repr::RelationDesc;

use crate::names::{DatabaseSpecifier, FullName, PartialName};
use crate::plan::PlanContext;

/// A catalog keeps track of SQL objects available to the planner.
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
///   * Lookup operations, like [`list_items`] or [`get_item`]. These retrieve
///     metadata about a catalog entity based on a fully-specified name that is
///     known to be valid (i.e., because the name was successfully resolved,
///     or was constructed based on the output of a prior lookup operation).
///     These functions panic if called with invalid input.
///
/// [`list_databases`]: Catalog::list_databases
/// [`get_item`]: Catalog::resolve_item
/// [`resolve_item`]: Catalog::resolve_item
pub trait Catalog: fmt::Debug {
    /// Returns the time at which the catalog booted.
    ///
    /// NOTE(benesch): this is only necessary for producing unique Kafka sink
    /// topics. Perhaps we can remove this when #2915 is complete.
    fn startup_time(&self) -> SystemTime;

    /// Returns a random integer associated with this instance of the catalog.
    ///
    /// NOTE(benesch): this is only necessary for producing unique Kafka sink
    /// topics. Perhaps we can remove this when #2915 is complete.
    fn nonce(&self) -> u64;

    /// Returns the database to use if one is not explicitly specified.
    fn default_database(&self) -> &str;

    /// Resolves the named database.
    ///
    /// If `database_name` exists in the catalog, it returns `Ok`; otherwise it
    /// returns an error.
    ///
    /// This function is named as such for symmetry with [`resolve_schema`] and
    /// [`resolve_item`], but there is no such thing as a "partial" database
    /// name. All database names are full names. This function amounts to an
    /// existence check for the named database.
    fn resolve_database(&self, database_name: &str) -> Result<(), CatalogError>;

    /// Resolves a partially-specified schema name.
    ///
    /// If `database_name` is provided, it searches the named database for a
    /// schema named `schema_name`. If `database_name` is not provided, it
    /// searches the default database instead. It returns an error if the
    /// database does not exist, or if the database exists but the schema does
    /// not.
    fn resolve_schema(
        &self,
        database_name: Option<String>,
        schema_name: &str,
    ) -> Result<DatabaseSpecifier, CatalogError>;

    /// Resolves a partially-specified item name.
    ///
    /// If the partial name has a database component, it searches only the
    /// specified database is searched; otherwise, it searches the default
    /// database. If the partial name has a schema component, it searches only
    /// the specified schema; otherwise, it searches a default set of schemas
    /// within the selected database. It returns an error if none of the
    /// searched schemas contain an item whose name matches the item component
    /// of the partial name.
    ///
    /// Note that it is not an error if the named item appears in more than one
    /// of the search schemas. The catalog implementation must choose one.
    fn resolve_item(&self, item_name: &PartialName) -> Result<FullName, CatalogError>;

    /// Lists the schemas in the specified database.
    ///
    /// Panics if `database_spec` does not specify a valid database.
    fn list_schemas<'a>(
        &'a self,
        database_spec: &DatabaseSpecifier,
    ) -> Box<dyn Iterator<Item = &'a str> + 'a>;

    /// Lists the items in the specified schema in the specified database.
    ///
    /// Panics if `database_spec` and `schema_name` do not specify a valid
    /// schema.
    fn list_items<'a>(
        &'a self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
    ) -> Box<dyn Iterator<Item = &'a dyn CatalogItem> + 'a>;

    /// Gets an item by its fully-specified name.
    ///
    /// Panics if `name` does not specify a valid item.
    fn get_item(&self, name: &FullName) -> &dyn CatalogItem;

    /// Gets an item by its ID.
    ///
    /// Panics if `id` does not specify a valid item.
    fn get_item_by_id(&self, id: &GlobalId) -> &dyn CatalogItem;

    /// Reports whether the specified catalog item is queryable.
    ///
    /// A queryable catalog item is one for which a timestamp can be determined.
    /// In practice, this means a catalog item whose transitive dependency set
    /// does not include any unmaterialized sources.
    ///
    /// Panics if `id` does not specify an object on which indexes can be built.
    fn is_queryable(&self, id: GlobalId) -> bool;

    /// Reports whether the specified catalog item is materialized.
    ///
    /// A materialized catalog item has at least one index.
    ///
    /// Panics if `id` does not specify an object on which indexes can be built.
    fn is_materialized(&self, id: GlobalId) -> bool;

    /// Expresses whether or not the catalog allows experimental mode features.
    fn experimental_mode(&self) -> bool;
}

/// An item in a [`Catalog`].
///
/// Note that "item" has a very specific meaning in the context of a SQL
/// catalog, and refers to the various entities that belong to a schema.
pub trait CatalogItem {
    /// Returns the fully-specified name of the catalog item.
    fn name(&self) -> &FullName;

    /// Returns a stable ID for the catalog item.
    fn id(&self) -> GlobalId;

    /// Returns a description of the result set produced by the catalog item.
    ///
    /// If the catalog item is not of a type that produces data (i.e., a sink or
    /// an index), it returns an error.
    fn desc(&self) -> Result<&RelationDesc, CatalogError>;

    /// Returns the type of the catalog item.
    fn item_type(&self) -> CatalogItemType;

    /// A normalized SQL statement that describes how to create the catalog
    /// item.
    fn create_sql(&self) -> &str;

    /// The [`PlanContext`] associated with the catalog item.
    fn plan_cx(&self) -> &PlanContext;

    /// Returns the IDs of the catalog items upon which this catalog item
    /// depends.
    fn uses(&self) -> Vec<GlobalId>;

    /// Returns the IDs of the catalog items that depend upon this catalog item.
    fn used_by(&self) -> &[GlobalId];

    /// Returns the index details associated with the catalog item, if the
    /// catalog item is an index.
    fn index_details(&self) -> Option<(&[ScalarExpr], GlobalId)>;
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
}

impl fmt::Display for CatalogItemType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CatalogItemType::Table => f.write_str("table"),
            CatalogItemType::Source => f.write_str("source"),
            CatalogItemType::Sink => f.write_str("sink"),
            CatalogItemType::View => f.write_str("view"),
            CatalogItemType::Index => f.write_str("index"),
        }
    }
}

/// An error returned by the catalog.
#[derive(Debug)]
pub enum CatalogError {
    /// Unknown database.
    UnknownDatabase(String),
    /// Unknown schema.
    UnknownSchema(String),
    /// Unknown item.
    UnknownItem(String),
    /// Invalid attempt to depend on a sink.
    InvalidSinkDependency(String),
    /// Invalid attempt to depend on an index.
    InvalidIndexDependency(String),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UnknownDatabase(name) => write!(f, "unknown database '{}'", name),
            Self::UnknownSchema(name) => write!(f, "unknown schema '{}'", name),
            Self::UnknownItem(name) => write!(f, "unknown catalog item '{}'", name),
            Self::InvalidSinkDependency(name) => write!(
                f,
                "catalog item '{}' is a sink and so cannot be depended upon",
                name
            ),
            Self::InvalidIndexDependency(name) => write!(
                f,
                "catalog item '{}' is an index and so cannot be depended upon",
                name
            ),
        }
    }
}

impl Error for CatalogError {}

/// A dummy [`Catalog`] implementation.
///
/// This implementation is suitable for use in tests that plan queries which are
/// not demanding of the catalog, as many methods are unimplemented.
#[derive(Debug)]
pub struct DummyCatalog;

impl Catalog for DummyCatalog {
    fn startup_time(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
    }

    fn nonce(&self) -> u64 {
        0
    }

    fn default_database(&self) -> &str {
        "dummy"
    }

    fn resolve_database(&self, _: &str) -> Result<(), CatalogError> {
        unimplemented!();
    }

    fn resolve_schema(
        &self,
        _: Option<String>,
        _: &str,
    ) -> Result<DatabaseSpecifier, CatalogError> {
        unimplemented!();
    }

    fn resolve_item(&self, _: &PartialName) -> Result<FullName, CatalogError> {
        unimplemented!();
    }

    fn list_schemas<'a>(&'a self, _: &DatabaseSpecifier) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        unimplemented!();
    }

    fn list_items<'a>(
        &'a self,
        _: &DatabaseSpecifier,
        _: &str,
    ) -> Box<dyn Iterator<Item = &'a dyn CatalogItem> + 'a> {
        unimplemented!();
    }

    fn get_item(&self, _: &FullName) -> &dyn CatalogItem {
        unimplemented!();
    }

    fn get_item_by_id(&self, _: &GlobalId) -> &dyn CatalogItem {
        unimplemented!();
    }

    fn is_queryable(&self, _: GlobalId) -> bool {
        false
    }

    fn is_materialized(&self, _: GlobalId) -> bool {
        false
    }

    fn experimental_mode(&self) -> bool {
        false
    }
}
