// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structured name types for SQL objects.

use anyhow::anyhow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use once_cell::sync::Lazy;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use uncased::UncasedStr;

use mz_controller::clusters::{ClusterId, ReplicaId};
use mz_expr::LocalId;
use mz_ore::cast::CastFrom;
use mz_ore::str::StrExt;
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql_parser::ast::MutRecBlock;
use mz_sql_parser::ast::UnresolvedObjectName;

use crate::ast::display::{AstDisplay, AstFormatter};
use crate::ast::fold::{Fold, FoldNode};
use crate::ast::visit::{Visit, VisitNode};
use crate::ast::visit_mut::VisitMut;
use crate::ast::{
    self, AstInfo, Cte, CteBlock, CteMutRec, Ident, Query, Raw, RawClusterName, RawDataType,
    RawItemName, Statement, UnresolvedItemName,
};
use crate::catalog::{CatalogError, CatalogItemType, CatalogTypeDetails, SessionCatalog};
use crate::normalize;
use crate::plan::PlanError;

/// A fully-qualified human readable name of an item in the catalog.
///
/// Catalog names compare case sensitively. Use
/// [`normalize::unresolved_item_name`] to
/// perform proper case folding if converting an [`UnresolvedItemName`] to a
/// `FullItemName`.
///
/// [`normalize::unresolved_item_name`]: crate::normalize::unresolved_item_name
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct FullItemName {
    /// The database name.
    pub database: RawDatabaseSpecifier,
    /// The schema name.
    pub schema: String,
    /// The item name.
    pub item: String,
}

impl fmt::Display for FullItemName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let RawDatabaseSpecifier::Name(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        write!(f, "{}.{}", self.schema, self.item)
    }
}

impl From<FullItemName> for UnresolvedItemName {
    fn from(full_name: FullItemName) -> UnresolvedItemName {
        let mut name_parts = Vec::new();
        if let RawDatabaseSpecifier::Name(database) = full_name.database {
            name_parts.push(Ident::new(database));
        }
        name_parts.push(Ident::new(full_name.schema));
        name_parts.push(Ident::new(full_name.item));
        UnresolvedItemName(name_parts)
    }
}

/// A fully-qualified non-human readable name of an item in the catalog using IDs for the database
/// and schema.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct QualifiedItemName {
    pub qualifiers: ItemQualifiers,
    pub item: String,
}

impl fmt::Display for QualifiedItemName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let ResolvedDatabaseSpecifier::Id(id) = &self.qualifiers.database_spec {
            write!(f, "{}.", id)?;
        }
        write!(f, "{}.{}", self.qualifiers.schema_spec, self.item)
    }
}

/// An optionally-qualified human-readable name of an item in the catalog.
///
/// This is like a [`FullItemName`], but either the database or schema name may be
/// omitted.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartialItemName {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub item: String,
}

impl PartialItemName {
    // Whether either self or other might be a (possibly differently qualified)
    // version of the other.
    pub fn matches(&self, other: &Self) -> bool {
        match (&self.database, &other.database) {
            (Some(d1), Some(d2)) if d1 != d2 => return false,
            _ => (),
        }
        match (&self.schema, &other.schema) {
            (Some(s1), Some(s2)) if s1 != s2 => return false,
            _ => (),
        }
        self.item == other.item
    }
}

impl fmt::Display for PartialItemName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        if let Some(schema) = &self.schema {
            write!(f, "{}.", schema)?;
        }
        write!(f, "{}", self.item)
    }
}

impl From<FullItemName> for PartialItemName {
    fn from(n: FullItemName) -> PartialItemName {
        let database = match n.database {
            RawDatabaseSpecifier::Ambient => None,
            RawDatabaseSpecifier::Name(name) => Some(name),
        };
        PartialItemName {
            database,
            schema: Some(n.schema),
            item: n.item,
        }
    }
}

impl From<String> for PartialItemName {
    fn from(item: String) -> Self {
        PartialItemName {
            database: None,
            schema: None,
            item,
        }
    }
}

impl From<PartialItemName> for UnresolvedItemName {
    fn from(partial_name: PartialItemName) -> UnresolvedItemName {
        let mut name_parts = Vec::new();
        if let Some(database) = partial_name.database {
            name_parts.push(Ident::new(database));
        }
        if let Some(schema) = partial_name.schema {
            name_parts.push(Ident::new(schema));
        }
        name_parts.push(Ident::new(partial_name.item));
        UnresolvedItemName(name_parts)
    }
}

/// A fully-qualified human readable name of a schema in the catalog.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FullSchemaName {
    /// The database name
    pub database: RawDatabaseSpecifier,
    /// The schema name
    pub schema: String,
}

impl fmt::Display for FullSchemaName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let RawDatabaseSpecifier::Name(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        write!(f, "{}", self.schema)
    }
}

/// The fully-qualified non-human readable name of a schema in the catalog.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct QualifiedSchemaName {
    pub database: ResolvedDatabaseSpecifier,
    pub schema: String,
}

impl fmt::Display for QualifiedSchemaName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.database {
            ResolvedDatabaseSpecifier::Ambient => f.write_str(&self.schema),
            ResolvedDatabaseSpecifier::Id(id) => write!(f, "{}.{}", id, self.schema),
        }
    }
}

/// An optionally-qualified name of an schema in the catalog.
///
/// This is like a [`FullSchemaName`], but either the database or schema name may be
/// omitted.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartialSchemaName {
    pub database: Option<String>,
    pub schema: String,
}

impl fmt::Display for PartialSchemaName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        write!(f, "{}", self.schema)
    }
}

/// A human readable name of a database.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum RawDatabaseSpecifier {
    /// The "ambient" database, which is always present and is not named
    /// explicitly, but by omission.
    Ambient,
    /// A normal database with a name.
    Name(String),
}

impl fmt::Display for RawDatabaseSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Ambient => f.write_str("<none>"),
            Self::Name(name) => f.write_str(name),
        }
    }
}

impl From<Option<String>> for RawDatabaseSpecifier {
    fn from(s: Option<String>) -> RawDatabaseSpecifier {
        match s {
            None => Self::Ambient,
            Some(name) => Self::Name(name),
        }
    }
}

/// An id of a database.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ResolvedDatabaseSpecifier {
    /// The "ambient" database, which is always present and is not named
    /// explicitly, but by omission.
    Ambient,
    /// A normal database with a name.
    Id(DatabaseId),
}

impl fmt::Display for ResolvedDatabaseSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Ambient => f.write_str("<none>"),
            Self::Id(id) => write!(f, "{}", id),
        }
    }
}

impl AstDisplay for ResolvedDatabaseSpecifier {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(format!("{}", self));
    }
}

impl From<DatabaseId> for ResolvedDatabaseSpecifier {
    fn from(id: DatabaseId) -> Self {
        Self::Id(id)
    }
}

/*
 * TODO(jkosh44) It's possible that in order to fix
 * https://github.com/MaterializeInc/materialize/issues/8805 we will need to assign temporary
 * schemas unique Ids. If/when that happens we can remove this enum and refer to all schemas by
 * their Id.
 */
/// An id of a schema.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SchemaSpecifier {
    /// A temporary schema
    Temporary,
    /// A normal database with a name.
    Id(SchemaId),
}

impl SchemaSpecifier {
    const TEMPORARY_SCHEMA_ID: u64 = 0;

    pub fn is_system(&self) -> bool {
        match self {
            SchemaSpecifier::Temporary => false,
            SchemaSpecifier::Id(id) => id.is_system(),
        }
    }

    pub fn is_temporary(&self) -> bool {
        matches!(self, SchemaSpecifier::Temporary)
    }
}

impl fmt::Display for SchemaSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Temporary => f.write_str(format!("{}", Self::TEMPORARY_SCHEMA_ID).as_str()),
            Self::Id(id) => write!(f, "{}", id),
        }
    }
}

impl AstDisplay for SchemaSpecifier {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(format!("{}", self));
    }
}

impl From<SchemaId> for SchemaSpecifier {
    fn from(id: SchemaId) -> SchemaSpecifier {
        match id {
            SchemaId::User(id) if id == SchemaSpecifier::TEMPORARY_SCHEMA_ID => {
                SchemaSpecifier::Temporary
            }
            schema_id => SchemaSpecifier::Id(schema_id),
        }
    }
}

impl From<&SchemaSpecifier> for SchemaId {
    fn from(schema_spec: &SchemaSpecifier) -> Self {
        match schema_spec {
            SchemaSpecifier::Temporary => SchemaId::User(SchemaSpecifier::TEMPORARY_SCHEMA_ID),
            SchemaSpecifier::Id(id) => id.clone(),
        }
    }
}

impl From<SchemaSpecifier> for SchemaId {
    fn from(schema_spec: SchemaSpecifier) -> Self {
        match schema_spec {
            SchemaSpecifier::Temporary => SchemaId::User(SchemaSpecifier::TEMPORARY_SCHEMA_ID),
            SchemaSpecifier::Id(id) => id,
        }
    }
}

// Aug is the type variable assigned to an AST that has already been
// name-resolved. An AST in this state has global IDs populated next to table
// names, and local IDs assigned to CTE definitions and references.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Copy, Clone, Default)]
pub struct Aug;

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ItemQualifiers {
    pub database_spec: ResolvedDatabaseSpecifier,
    pub schema_spec: SchemaSpecifier,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResolvedItemName {
    Item {
        id: GlobalId,
        qualifiers: ItemQualifiers,
        full_name: FullItemName,
        // Whether this object, when printed out, should use [id AS name] syntax. We
        // want this for things like tables and sources, but not for things like
        // types.
        print_id: bool,
    },
    Cte {
        id: LocalId,
        name: String,
    },
    Error,
}

impl ResolvedItemName {
    pub fn full_name_str(&self) -> String {
        match self {
            ResolvedItemName::Item { full_name, .. } => full_name.to_string(),
            ResolvedItemName::Cte { name, .. } => name.clone(),
            ResolvedItemName::Error => "error in name resolution".to_string(),
        }
    }

    pub fn full_item_name(&self) -> &FullItemName {
        match self {
            ResolvedItemName::Item { full_name, .. } => full_name,
            _ => panic!("cannot call object_full_name on non-object"),
        }
    }
}

impl AstDisplay for ResolvedItemName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ResolvedItemName::Item {
                id,
                qualifiers: _,
                full_name,
                print_id,
            } => {
                if *print_id {
                    f.write_str(format!("[{} AS ", id));
                }
                if let RawDatabaseSpecifier::Name(database) = &full_name.database {
                    f.write_node(&Ident::new(database));
                    f.write_str(".");
                }
                f.write_node(&Ident::new(&full_name.schema));
                f.write_str(".");
                f.write_node(&Ident::new(&full_name.item));
                if *print_id {
                    f.write_str("]");
                }
            }
            ResolvedItemName::Cte { name, .. } => f.write_node(&Ident::new(name)),
            ResolvedItemName::Error => {}
        }
    }
}

impl std::fmt::Display for ResolvedItemName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_ast_string().as_str())
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResolvedSchemaName {
    Schema {
        database_spec: ResolvedDatabaseSpecifier,
        schema_spec: SchemaSpecifier,
        full_name: FullSchemaName,
    },
    Error,
}

impl ResolvedSchemaName {
    /// Panics if this is `Self::Error`.
    pub fn schema_spec(&self) -> &SchemaSpecifier {
        match self {
            ResolvedSchemaName::Schema { schema_spec, .. } => schema_spec,
            ResolvedSchemaName::Error => {
                unreachable!("should have been handled by name resolution")
            }
        }
    }
}

impl AstDisplay for ResolvedSchemaName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ResolvedSchemaName::Schema { full_name, .. } => {
                if let RawDatabaseSpecifier::Name(database) = &full_name.database {
                    f.write_node(&Ident::new(database));
                    f.write_str(".");
                }
                f.write_node(&Ident::new(&full_name.schema));
            }
            ResolvedSchemaName::Error => {}
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResolvedDatabaseName {
    Database { id: DatabaseId, name: String },
    Error,
}

impl ResolvedDatabaseName {
    /// Panics if this is `Self::Error`.
    pub fn database_id(&self) -> &DatabaseId {
        match self {
            ResolvedDatabaseName::Database { id, .. } => id,
            ResolvedDatabaseName::Error => {
                unreachable!("should have been handled by name resolution")
            }
        }
    }
}

impl AstDisplay for ResolvedDatabaseName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ResolvedDatabaseName::Database { name, .. } => f.write_node(&Ident::new(name)),
            ResolvedDatabaseName::Error => {}
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResolvedClusterName {
    pub id: ClusterId,
    /// If set, a name to print in the `AstDisplay` implementation instead of
    /// `None`. This is only meant to be used by the `NameSimplifier`.
    ///
    /// NOTE(benesch): it would be much clearer if the `NameSimplifier` folded
    /// the AST into a different metadata type, to avoid polluting the resolved
    /// AST with this field.
    pub print_name: Option<String>,
}

impl AstDisplay for ResolvedClusterName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        if let Some(print_name) = &self.print_name {
            f.write_node(&Ident::new(print_name))
        } else {
            f.write_str(format!("[{}]", self.id))
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResolvedClusterReplicaName {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
}

impl AstDisplay for ResolvedClusterReplicaName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(format!("[{}.{}]", self.cluster_id, self.replica_id))
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResolvedDataType {
    AnonymousList(Box<ResolvedDataType>),
    AnonymousMap {
        key_type: Box<ResolvedDataType>,
        value_type: Box<ResolvedDataType>,
    },
    Named {
        id: GlobalId,
        qualifiers: ItemQualifiers,
        full_name: FullItemName,
        modifiers: Vec<i64>,
        print_id: bool,
    },
    Error,
}

impl AstDisplay for ResolvedDataType {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ResolvedDataType::AnonymousList(element_type) => {
                element_type.fmt(f);
                f.write_str(" list");
            }
            ResolvedDataType::AnonymousMap {
                key_type,
                value_type,
            } => {
                f.write_str("map[");
                key_type.fmt(f);
                f.write_str("=>");
                value_type.fmt(f);
                f.write_str("]");
            }
            ResolvedDataType::Named {
                id,
                full_name,
                modifiers,
                print_id,
                ..
            } => {
                if *print_id {
                    f.write_str(format!("[{} AS ", id));
                }
                if let RawDatabaseSpecifier::Name(database) = &full_name.database {
                    f.write_node(&Ident::new(database));
                    f.write_str(".");
                }

                f.write_node(&Ident::new(&full_name.schema));
                f.write_str(".");

                f.write_node(&Ident::new(&full_name.item));
                if *print_id {
                    f.write_str("]");
                }
                if modifiers.len() > 0 {
                    f.write_str("(");
                    f.write_node(&ast::display::comma_separated(modifiers));
                    f.write_str(")");
                }
            }
            ResolvedDataType::Error => {}
        }
    }
}

impl ResolvedDataType {
    /// Return the name of `self`'s item without qualification or IDs.
    ///
    /// This is used to generate to generate default column names for cast operations.
    pub fn unqualified_item_name(&self) -> String {
        let mut res = String::new();
        match self {
            ResolvedDataType::AnonymousList(element_type) => {
                res += &element_type.unqualified_item_name();
                res += " list";
            }
            ResolvedDataType::AnonymousMap {
                key_type,
                value_type,
            } => {
                res += "map[";
                res += &key_type.unqualified_item_name();
                res += "=>";
                res += &value_type.unqualified_item_name();
                res += "]";
            }
            ResolvedDataType::Named { full_name, .. } => {
                res += &full_name.item;
            }
            ResolvedDataType::Error => {}
        }
        res
    }
}

impl fmt::Display for ResolvedDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.to_ast_string().as_str())
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResolvedRoleName {
    pub id: RoleId,
    pub name: String,
}

impl AstDisplay for ResolvedRoleName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(format!("[{} AS {}]", self.id, self.name));
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResolvedObjectName {
    Cluster(ResolvedClusterName),
    ClusterReplica(ResolvedClusterReplicaName),
    Database(ResolvedDatabaseName),
    Schema(ResolvedSchemaName),
    Role(ResolvedRoleName),
    Item(ResolvedItemName),
}

impl AstDisplay for ResolvedObjectName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ResolvedObjectName::Cluster(n) => f.write_node(n),
            ResolvedObjectName::ClusterReplica(n) => f.write_node(n),
            ResolvedObjectName::Database(n) => f.write_node(n),
            ResolvedObjectName::Schema(n) => f.write_node(n),
            ResolvedObjectName::Role(n) => f.write_node(n),
            ResolvedObjectName::Item(n) => f.write_node(n),
        }
    }
}

impl AstInfo for Aug {
    type NestedStatement = Statement<Raw>;
    type ItemName = ResolvedItemName;
    type SchemaName = ResolvedSchemaName;
    type DatabaseName = ResolvedDatabaseName;
    type ClusterName = ResolvedClusterName;
    type DataType = ResolvedDataType;
    type CteId = LocalId;
    type RoleName = ResolvedRoleName;
    type ObjectName = ResolvedObjectName;
}

/// The identifier for a schema.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Arbitrary,
)]
pub enum SchemaId {
    User(u64),
    System(u64),
}

impl SchemaId {
    pub fn is_user(&self) -> bool {
        matches!(self, SchemaId::User(_))
    }

    pub fn is_system(&self) -> bool {
        matches!(self, SchemaId::System(_))
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SchemaId::System(id) => write!(f, "s{}", id),
            SchemaId::User(id) => write!(f, "u{}", id),
        }
    }
}

impl FromStr for SchemaId {
    type Err = PlanError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(PlanError::Unstructured(format!(
                "couldn't parse SchemaId {}",
                s
            )));
        }
        let val: u64 = s[1..].parse()?;
        match s.chars().next() {
            Some('s') => Ok(SchemaId::System(val)),
            Some('u') => Ok(SchemaId::User(val)),
            _ => Err(PlanError::Unstructured(format!(
                "couldn't parse SchemaId {}",
                s
            ))),
        }
    }
}

/// The identifier for a database.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Arbitrary,
)]
pub enum DatabaseId {
    User(u64),
    System(u64),
}

impl DatabaseId {
    pub fn is_user(&self) -> bool {
        matches!(self, DatabaseId::User(_))
    }

    pub fn is_system(&self) -> bool {
        matches!(self, DatabaseId::System(_))
    }
}

impl fmt::Display for DatabaseId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DatabaseId::System(id) => write!(f, "s{}", id),
            DatabaseId::User(id) => write!(f, "u{}", id),
        }
    }
}

impl FromStr for DatabaseId {
    type Err = PlanError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(PlanError::Unstructured(format!(
                "couldn't parse DatabaseId {}",
                s
            )));
        }
        let val: u64 = s[1..].parse()?;
        match s.chars().next() {
            Some('s') => Ok(DatabaseId::System(val)),
            Some('u') => Ok(DatabaseId::User(val)),
            _ => Err(PlanError::Unstructured(format!(
                "couldn't parse DatabaseId {}",
                s
            ))),
        }
    }
}

pub static PUBLIC_ROLE_NAME: Lazy<&UncasedStr> = Lazy::new(|| UncasedStr::new("PUBLIC"));

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ObjectId {
    Cluster(ClusterId),
    ClusterReplica((ClusterId, ReplicaId)),
    Database(DatabaseId),
    Schema((ResolvedDatabaseSpecifier, SchemaSpecifier)),
    Role(RoleId),
    Item(GlobalId),
}

impl ObjectId {
    pub fn unwrap_cluster_id(self) -> ClusterId {
        match self {
            ObjectId::Cluster(id) => id,
            _ => panic!("ObjectId::unwrap_cluster_id called on {self:?}"),
        }
    }
    pub fn unwrap_cluster_replica_id(self) -> (ClusterId, ReplicaId) {
        match self {
            ObjectId::ClusterReplica(id) => id,
            _ => panic!("ObjectId::unwrap_cluster_replica_id called on {self:?}"),
        }
    }
    pub fn unwrap_database_id(self) -> DatabaseId {
        match self {
            ObjectId::Database(id) => id,
            _ => panic!("ObjectId::unwrap_database_id called on {self:?}"),
        }
    }
    pub fn unwrap_schema_id(self) -> (ResolvedDatabaseSpecifier, SchemaSpecifier) {
        match self {
            ObjectId::Schema(id) => id,
            _ => panic!("ObjectId::unwrap_schema_id called on {self:?}"),
        }
    }
    pub fn unwrap_role_id(self) -> RoleId {
        match self {
            ObjectId::Role(id) => id,
            _ => panic!("ObjectId::unwrap_role_id called on {self:?}"),
        }
    }
    pub fn unwrap_item_id(self) -> GlobalId {
        match self {
            ObjectId::Item(id) => id,
            _ => panic!("ObjectId::unwrap_item_id called on {self:?}"),
        }
    }

    pub fn is_system(&self) -> bool {
        match self {
            ObjectId::Cluster(cluster_id) => cluster_id.is_system(),
            // replica IDs aren't namespaced so we rely on the cluster ID.
            ObjectId::ClusterReplica((cluster_id, _replica_id)) => cluster_id.is_system(),
            ObjectId::Database(database_id) => database_id.is_system(),
            ObjectId::Schema((_database_id, schema_id)) => schema_id.is_system(),
            ObjectId::Role(role_id) => role_id.is_system(),
            ObjectId::Item(global_id) => global_id.is_system(),
        }
    }
}

impl TryFrom<ResolvedObjectName> for ObjectId {
    type Error = anyhow::Error;

    fn try_from(name: ResolvedObjectName) -> Result<ObjectId, Self::Error> {
        match name {
            ResolvedObjectName::Cluster(name) => Ok(ObjectId::Cluster(name.id)),
            ResolvedObjectName::ClusterReplica(name) => {
                Ok(ObjectId::ClusterReplica((name.cluster_id, name.replica_id)))
            }
            ResolvedObjectName::Database(name) => Ok(ObjectId::Database(*name.database_id())),
            ResolvedObjectName::Schema(name) => match name {
                ResolvedSchemaName::Schema {
                    database_spec,
                    schema_spec,
                    ..
                } => Ok(ObjectId::Schema((database_spec, schema_spec))),
                ResolvedSchemaName::Error => Err(anyhow!("error in name resolution")),
            },
            ResolvedObjectName::Role(name) => Ok(ObjectId::Role(name.id)),
            ResolvedObjectName::Item(name) => match name {
                ResolvedItemName::Item { id, .. } => Ok(ObjectId::Item(id)),
                ResolvedItemName::Cte { .. } => Err(anyhow!("CTE does not correspond to object")),
                ResolvedItemName::Error => Err(anyhow!("error in name resolution")),
            },
        }
    }
}

impl From<ClusterId> for ObjectId {
    fn from(id: ClusterId) -> Self {
        ObjectId::Cluster(id)
    }
}

impl From<&ClusterId> for ObjectId {
    fn from(id: &ClusterId) -> Self {
        ObjectId::Cluster(*id)
    }
}

impl From<(ClusterId, ReplicaId)> for ObjectId {
    fn from(id: (ClusterId, ReplicaId)) -> Self {
        ObjectId::ClusterReplica(id)
    }
}

impl From<&(ClusterId, ReplicaId)> for ObjectId {
    fn from(id: &(ClusterId, ReplicaId)) -> Self {
        ObjectId::ClusterReplica(*id)
    }
}

impl From<DatabaseId> for ObjectId {
    fn from(id: DatabaseId) -> Self {
        ObjectId::Database(id)
    }
}

impl From<&DatabaseId> for ObjectId {
    fn from(id: &DatabaseId) -> Self {
        ObjectId::Database(*id)
    }
}

impl From<ItemQualifiers> for ObjectId {
    fn from(qualifiers: ItemQualifiers) -> Self {
        ObjectId::Schema((qualifiers.database_spec, qualifiers.schema_spec))
    }
}

impl From<&ItemQualifiers> for ObjectId {
    fn from(qualifiers: &ItemQualifiers) -> Self {
        ObjectId::Schema((qualifiers.database_spec, qualifiers.schema_spec))
    }
}

impl From<(ResolvedDatabaseSpecifier, SchemaSpecifier)> for ObjectId {
    fn from(id: (ResolvedDatabaseSpecifier, SchemaSpecifier)) -> Self {
        ObjectId::Schema(id)
    }
}

impl From<&(ResolvedDatabaseSpecifier, SchemaSpecifier)> for ObjectId {
    fn from(id: &(ResolvedDatabaseSpecifier, SchemaSpecifier)) -> Self {
        ObjectId::Schema(*id)
    }
}

impl From<RoleId> for ObjectId {
    fn from(id: RoleId) -> Self {
        ObjectId::Role(id)
    }
}

impl From<&RoleId> for ObjectId {
    fn from(id: &RoleId) -> Self {
        ObjectId::Role(*id)
    }
}

impl From<GlobalId> for ObjectId {
    fn from(id: GlobalId) -> Self {
        ObjectId::Item(id)
    }
}

impl From<&GlobalId> for ObjectId {
    fn from(id: &GlobalId) -> Self {
        ObjectId::Item(*id)
    }
}

#[derive(Debug)]
pub struct NameResolver<'a> {
    catalog: &'a dyn SessionCatalog,
    ctes: BTreeMap<String, LocalId>,
    status: Result<(), PlanError>,
    ids: BTreeSet<GlobalId>,
}

impl<'a> NameResolver<'a> {
    fn new(catalog: &'a dyn SessionCatalog) -> NameResolver {
        NameResolver {
            catalog,
            ctes: BTreeMap::new(),
            status: Ok(()),
            ids: BTreeSet::new(),
        }
    }

    fn fold_data_type_internal(
        &mut self,
        data_type: <Raw as AstInfo>::DataType,
    ) -> Result<<Aug as AstInfo>::DataType, PlanError> {
        match data_type {
            RawDataType::Array(elem_type) => {
                let name = elem_type.to_string();
                match self.fold_data_type_internal(*elem_type)? {
                    ResolvedDataType::AnonymousList(_) | ResolvedDataType::AnonymousMap { .. } => {
                        sql_bail!("type \"{}[]\" does not exist", name)
                    }
                    ResolvedDataType::Named { id, modifiers, .. } => {
                        let element_item = self.catalog.get_item(&id);
                        let array_item = match element_item.type_details() {
                            Some(CatalogTypeDetails {
                                array_id: Some(array_id),
                                ..
                            }) => self.catalog.get_item(array_id),
                            Some(_) => sql_bail!("type \"{}[]\" does not exist", name),
                            None => sql_bail!(
                                "{} does not refer to a type",
                                self.catalog
                                    .resolve_full_name(element_item.name())
                                    .to_string()
                                    .quoted()
                            ),
                        };
                        Ok(ResolvedDataType::Named {
                            id: array_item.id(),
                            qualifiers: array_item.name().qualifiers.clone(),
                            full_name: self.catalog.resolve_full_name(array_item.name()),
                            modifiers,
                            print_id: true,
                        })
                    }
                    ResolvedDataType::Error => sql_bail!("type \"{}[]\" does not exist", name),
                }
            }
            RawDataType::List(elem_type) => {
                let elem_type = self.fold_data_type_internal(*elem_type)?;
                Ok(ResolvedDataType::AnonymousList(Box::new(elem_type)))
            }
            RawDataType::Map {
                key_type,
                value_type,
            } => {
                let key_type = self.fold_data_type_internal(*key_type)?;
                let value_type = self.fold_data_type_internal(*value_type)?;
                Ok(ResolvedDataType::AnonymousMap {
                    key_type: Box::new(key_type),
                    value_type: Box::new(value_type),
                })
            }
            RawDataType::Other { name, typ_mod } => {
                let (full_name, item) = match name {
                    RawItemName::Name(name) => {
                        let name = normalize::unresolved_item_name(name)?;
                        let item = self.catalog.resolve_item(&name)?;
                        let full_name = self.catalog.resolve_full_name(item.name());
                        (full_name, item)
                    }
                    RawItemName::Id(id, name) => {
                        let gid: GlobalId = id.parse()?;
                        let item = self.catalog.get_item(&gid);
                        let full_name = normalize::full_name(name)?;
                        (full_name, item)
                    }
                };
                self.ids.insert(item.id());
                Ok(ResolvedDataType::Named {
                    id: item.id(),
                    qualifiers: item.name().qualifiers.clone(),
                    full_name,
                    modifiers: typ_mod,
                    print_id: true,
                })
            }
        }
    }

    fn fold_raw_object_name_name_internal(
        &mut self,
        name: RawItemName,
        consider_function: bool,
    ) -> ResolvedItemName {
        match name {
            RawItemName::Name(raw_name) => {
                let raw_name = match normalize::unresolved_item_name(raw_name) {
                    Ok(raw_name) => raw_name,
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e);
                        }
                        return ResolvedItemName::Error;
                    }
                };

                // Check if unqualified name refers to a CTE.
                if raw_name.database.is_none() && raw_name.schema.is_none() {
                    let norm_name = normalize::ident(Ident::new(&raw_name.item));
                    if let Some(id) = self.ctes.get(&norm_name) {
                        return ResolvedItemName::Cte {
                            id: *id,
                            name: norm_name,
                        };
                    }
                }

                let r = if consider_function {
                    self.catalog.resolve_function(&raw_name)
                } else {
                    self.catalog.resolve_item(&raw_name)
                };

                match r {
                    Ok(item) => {
                        self.ids.insert(item.id());
                        let print_id = !matches!(
                            item.item_type(),
                            CatalogItemType::Func | CatalogItemType::Type
                        );
                        ResolvedItemName::Item {
                            id: item.id(),
                            qualifiers: item.name().qualifiers.clone(),
                            full_name: self.catalog.resolve_full_name(item.name()),
                            print_id,
                        }
                    }
                    Err(mut e) => {
                        if self.status.is_ok() {
                            match &mut e {
                                CatalogError::UnknownFunction { name, alternative } => {
                                    // Suggest using the `jsonb_` version of
                                    // `json_` functions that do not exist.
                                    let name: Vec<&str> = name.split('.').collect();
                                    match name.split_last() {
                                        Some((i, q)) if i.starts_with("json_") && q.len() < 2 => {
                                            let mut jsonb_version = q
                                                .iter()
                                                .map(|q| Ident::new(*q))
                                                .collect::<Vec<_>>();
                                            jsonb_version
                                                .push(Ident::new(i.replace("json_", "jsonb_")));
                                            let jsonb_version = RawItemName::Name(
                                                UnresolvedItemName(jsonb_version),
                                            );
                                            let _ = self.fold_raw_object_name_name_internal(
                                                jsonb_version.clone(),
                                                true,
                                            );

                                            if self.status.is_ok() {
                                                *alternative = Some(jsonb_version.to_string());
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                _ => {}
                            }

                            self.status = Err(e.into());
                        }
                        ResolvedItemName::Error
                    }
                }
            }
            // We don't want to hit this code path, but immaterial if we do
            name @ RawItemName::Id(..) => self.fold_item_name(name),
        }
    }
}

impl<'a> Fold<Raw, Aug> for NameResolver<'a> {
    fn fold_nested_statement(
        &mut self,
        stmt: <Raw as AstInfo>::NestedStatement,
    ) -> <Aug as AstInfo>::NestedStatement {
        stmt
    }

    fn fold_query(&mut self, q: Query<Raw>) -> Query<Aug> {
        // Retain the old values of various CTE names so that we can restore them after we're done
        // planning this SELECT.
        let mut shadowed_cte_ids = Vec::new();

        // A reused identifier indicates a reused name.
        use itertools::Itertools;
        if let Some(ident) = q.ctes.bound_identifiers().duplicates().next() {
            self.status = Err(sql_err!(
                "WITH query name \"{}\" specified more than once",
                normalize::ident_ref(ident),
            ));
        }

        let ctes: CteBlock<Aug> = match q.ctes {
            CteBlock::Simple(ctes) => {
                let mut result_ctes = Vec::<Cte<Aug>>::new();

                let initial_id = self.ctes.len();

                for (offset, cte) in ctes.into_iter().enumerate() {
                    let cte_name = normalize::ident(cte.alias.name.clone());
                    let local_id = LocalId::new(u64::cast_from(initial_id + offset));

                    result_ctes.push(Cte {
                        alias: cte.alias,
                        id: local_id,
                        query: self.fold_query(cte.query),
                    });

                    let shadowed_id = self.ctes.insert(cte_name.clone(), local_id);
                    shadowed_cte_ids.push((cte_name, shadowed_id));
                }
                CteBlock::Simple(result_ctes)
            }
            CteBlock::MutuallyRecursive(MutRecBlock { options, ctes }) => {
                let mut result_ctes = Vec::<CteMutRec<Aug>>::new();

                let initial_id = self.ctes.len();

                // The identifiers for each CTE will be `initial_id` plus their offset in `q.ctes`.
                for (offset, cte) in ctes.iter().enumerate() {
                    let cte_name = normalize::ident(cte.name.clone());
                    let local_id = LocalId::new(u64::cast_from(initial_id + offset));
                    let shadowed_id = self.ctes.insert(cte_name.clone(), local_id);
                    shadowed_cte_ids.push((cte_name, shadowed_id));
                }

                for (offset, cte) in ctes.into_iter().enumerate() {
                    let local_id = LocalId::new(u64::cast_from(initial_id + offset));

                    let columns = cte
                        .columns
                        .into_iter()
                        .map(|column| self.fold_cte_mut_rec_column_def(column))
                        .collect();
                    let query = self.fold_query(cte.query);
                    result_ctes.push(CteMutRec {
                        name: cte.name,
                        columns,
                        id: local_id,
                        query,
                    });
                }
                CteBlock::MutuallyRecursive(MutRecBlock {
                    options: options
                        .into_iter()
                        .map(|option| self.fold_mut_rec_block_option(option))
                        .collect(),
                    ctes: result_ctes,
                })
            }
        };

        let result = Query {
            ctes,
            body: self.fold_set_expr(q.body),
            limit: q.limit.map(|l| self.fold_limit(l)),
            offset: q.offset.map(|l| self.fold_expr(l)),
            order_by: q
                .order_by
                .into_iter()
                .map(|c| self.fold_order_by_expr(c))
                .collect(),
        };

        // Restore the old values of the CTEs.
        for (name, value) in shadowed_cte_ids.iter() {
            match value {
                Some(value) => {
                    self.ctes.insert(name.to_string(), value.clone());
                }
                None => {
                    self.ctes.remove(name);
                }
            };
        }

        result
    }

    fn fold_cte_id(&mut self, _id: <Raw as AstInfo>::CteId) -> <Aug as AstInfo>::CteId {
        panic!("this should have been handled when walking the CTE");
    }

    fn fold_item_name(
        &mut self,
        item_name: <Raw as AstInfo>::ItemName,
    ) -> <Aug as AstInfo>::ItemName {
        match item_name {
            name @ RawItemName::Name(..) => self.fold_raw_object_name_name_internal(name, false),
            RawItemName::Id(id, raw_name) => {
                let gid: GlobalId = match id.parse() {
                    Ok(id) => id,
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e.into());
                        }
                        return ResolvedItemName::Error;
                    }
                };
                let item = match self.catalog.try_get_item(&gid) {
                    Some(item) => item,
                    None => {
                        if self.status.is_ok() {
                            self.status = Err(PlanError::InvalidId(gid));
                        }
                        return ResolvedItemName::Error;
                    }
                };

                self.ids.insert(gid.clone());
                let full_name = match normalize::full_name(raw_name) {
                    Ok(full_name) => full_name,
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e);
                        }
                        return ResolvedItemName::Error;
                    }
                };
                ResolvedItemName::Item {
                    id: gid,
                    qualifiers: item.name().qualifiers.clone(),
                    full_name,
                    print_id: true,
                }
            }
        }
    }

    fn fold_data_type(
        &mut self,
        data_type: <Raw as AstInfo>::DataType,
    ) -> <Aug as AstInfo>::DataType {
        match self.fold_data_type_internal(data_type) {
            Ok(data_type) => data_type,
            Err(e) => {
                if self.status.is_ok() {
                    self.status = Err(e);
                }
                ResolvedDataType::Error
            }
        }
    }

    fn fold_schema_name(
        &mut self,
        name: <Raw as AstInfo>::SchemaName,
    ) -> <Aug as AstInfo>::SchemaName {
        let norm_name = match normalize::unresolved_schema_name(name) {
            Ok(norm_name) => norm_name,
            Err(e) => {
                if self.status.is_ok() {
                    self.status = Err(e);
                }
                return ResolvedSchemaName::Error;
            }
        };
        match self
            .catalog
            .resolve_schema(norm_name.database.as_deref(), norm_name.schema.as_str())
        {
            Ok(schema) => {
                let raw_database_spec = match schema.database() {
                    ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
                    ResolvedDatabaseSpecifier::Id(id) => {
                        RawDatabaseSpecifier::Name(self.catalog.get_database(id).name().to_string())
                    }
                };
                ResolvedSchemaName::Schema {
                    database_spec: schema.database().clone(),
                    schema_spec: schema.id().clone(),
                    full_name: FullSchemaName {
                        database: raw_database_spec,
                        schema: schema.name().schema.clone(),
                    },
                }
            }
            Err(e) => {
                if self.status.is_ok() {
                    self.status = Err(e.into());
                }
                ResolvedSchemaName::Error
            }
        }
    }

    fn fold_database_name(
        &mut self,
        database_name: <Raw as AstInfo>::DatabaseName,
    ) -> <Aug as AstInfo>::DatabaseName {
        match self.catalog.resolve_database(database_name.0.as_str()) {
            Ok(database) => ResolvedDatabaseName::Database {
                id: database.id(),
                name: database_name.0.into_string(),
            },
            Err(e) => {
                if self.status.is_ok() {
                    self.status = Err(e.into());
                }
                ResolvedDatabaseName::Error
            }
        }
    }

    fn fold_cluster_name(
        &mut self,
        cluster_name: <Raw as AstInfo>::ClusterName,
    ) -> <Aug as AstInfo>::ClusterName {
        match cluster_name {
            RawClusterName::Unresolved(ident) => {
                match self.catalog.resolve_cluster(Some(ident.as_str())) {
                    Ok(cluster) => ResolvedClusterName {
                        id: cluster.id(),
                        print_name: None,
                    },
                    Err(e) => {
                        self.status = Err(e.into());
                        ResolvedClusterName {
                            // The ID is arbitrary here; we just need some dummy
                            // value to return.
                            id: ClusterId::System(0),
                            print_name: None,
                        }
                    }
                }
            }
            RawClusterName::Resolved(ident) => match ident.parse() {
                Ok(id) => ResolvedClusterName {
                    id,
                    print_name: None,
                },
                Err(e) => {
                    self.status = Err(e.into());
                    ResolvedClusterName {
                        // The ID is arbitrary here; we just need some dummy
                        // value to return.
                        id: ClusterId::System(0),
                        print_name: None,
                    }
                }
            },
        }
    }

    fn fold_with_option_value(
        &mut self,
        node: mz_sql_parser::ast::WithOptionValue<Raw>,
    ) -> mz_sql_parser::ast::WithOptionValue<Aug> {
        use mz_sql_parser::ast::WithOptionValue::*;
        match node {
            Sequence(vs) => Sequence(
                vs.into_iter()
                    .map(|v| self.fold_with_option_value(v))
                    .collect(),
            ),
            Value(v) => Value(self.fold_value(v)),
            Ident(i) => Ident(self.fold_ident(i)),
            DataType(dt) => DataType(self.fold_data_type(dt)),
            Secret(secret) => {
                let item_name = self.fold_item_name(secret);
                match &item_name {
                    ResolvedItemName::Item { id, .. } => {
                        let item = self.catalog.get_item(id);
                        if item.item_type() != CatalogItemType::Secret {
                            self.status =
                                Err(PlanError::InvalidSecret(Box::new(item_name.clone())));
                        }
                    }
                    ResolvedItemName::Cte { .. } => {
                        self.status = Err(PlanError::InvalidSecret(Box::new(item_name.clone())));
                    }
                    ResolvedItemName::Error => {}
                }
                Secret(item_name)
            }
            Item(obj) => {
                let item_name = self.fold_item_name(obj);
                match &item_name {
                    ResolvedItemName::Item { .. } => {}
                    ResolvedItemName::Cte { .. } => {
                        self.status = Err(PlanError::InvalidObject(Box::new(item_name.clone())));
                    }
                    ResolvedItemName::Error => {}
                }
                Item(item_name)
            }
            UnresolvedItemName(name) => UnresolvedItemName(self.fold_unresolved_item_name(name)),
            ClusterReplicas(replicas) => ClusterReplicas(
                replicas
                    .into_iter()
                    .map(|r| self.fold_replica_definition(r))
                    .collect(),
            ),
            ConnectionKafkaBroker(broker) => ConnectionKafkaBroker(self.fold_kafka_broker(broker)),
        }
    }

    fn fold_role_name(&mut self, name: <Raw as AstInfo>::RoleName) -> <Aug as AstInfo>::RoleName {
        match self.catalog.resolve_role(name.as_str()) {
            Ok(role) => ResolvedRoleName {
                id: role.id(),
                name: role.name().to_string(),
            },
            Err(e) => {
                if self.status.is_ok() {
                    self.status = Err(e.into());
                }
                // garbage value that will be ignored since there's an error.
                ResolvedRoleName {
                    id: RoleId::User(0),
                    name: "".to_string(),
                }
            }
        }
    }
    fn fold_object_name(
        &mut self,
        name: <Raw as AstInfo>::ObjectName,
    ) -> <Aug as AstInfo>::ObjectName {
        match name {
            UnresolvedObjectName::Cluster(name) => ResolvedObjectName::Cluster(
                self.fold_cluster_name(RawClusterName::Unresolved(name)),
            ),
            UnresolvedObjectName::ClusterReplica(name) => {
                match self.catalog.resolve_cluster_replica(&name) {
                    Ok(cluster_replica) => {
                        ResolvedObjectName::ClusterReplica(ResolvedClusterReplicaName {
                            cluster_id: cluster_replica.cluster_id(),
                            replica_id: cluster_replica.replica_id(),
                        })
                    }
                    Err(e) => {
                        self.status = Err(e.into());
                        ResolvedObjectName::ClusterReplica(ResolvedClusterReplicaName {
                            // The ID is arbitrary here; we just need some dummy
                            // value to return.
                            cluster_id: ClusterId::System(0),
                            replica_id: 0,
                        })
                    }
                }
            }
            UnresolvedObjectName::Database(name) => {
                ResolvedObjectName::Database(self.fold_database_name(name))
            }
            UnresolvedObjectName::Schema(name) => {
                ResolvedObjectName::Schema(self.fold_schema_name(name))
            }
            UnresolvedObjectName::Role(name) => ResolvedObjectName::Role(self.fold_role_name(name)),
            UnresolvedObjectName::Item(name) => {
                ResolvedObjectName::Item(self.fold_item_name(RawItemName::Name(name)))
            }
        }
    }

    fn fold_expr(&mut self, node: mz_sql_parser::ast::Expr<Raw>) -> mz_sql_parser::ast::Expr<Aug> {
        use mz_sql_parser::ast::Expr::*;
        match node {
            mz_sql_parser::ast::Expr::Identifier(i) => {
                Identifier(i.into_iter().map(|i| self.fold_ident(i)).collect())
            }
            mz_sql_parser::ast::Expr::QualifiedWildcard(i) => {
                QualifiedWildcard(i.into_iter().map(|i| self.fold_ident(i)).collect())
            }
            mz_sql_parser::ast::Expr::FieldAccess { expr, field } => FieldAccess {
                expr: Box::new(self.fold_expr(*expr)),
                field: self.fold_ident(field),
            },
            mz_sql_parser::ast::Expr::WildcardAccess(expr) => {
                WildcardAccess(Box::new(self.fold_expr(*expr)))
            }
            mz_sql_parser::ast::Expr::Parameter(p) => Parameter(p),
            mz_sql_parser::ast::Expr::Not { expr } => Not {
                expr: Box::new(self.fold_expr(*expr)),
            },
            mz_sql_parser::ast::Expr::And { left, right } => And {
                left: Box::new(self.fold_expr(*left)),
                right: Box::new(self.fold_expr(*right)),
            },
            mz_sql_parser::ast::Expr::Or { left, right } => Or {
                left: Box::new(self.fold_expr(*left)),
                right: Box::new(self.fold_expr(*right)),
            },
            mz_sql_parser::ast::Expr::IsExpr {
                expr,
                construct,
                negated,
            } => IsExpr {
                expr: Box::new(self.fold_expr(*expr)),
                construct: self.fold_is_expr_construct(construct),
                negated,
            },
            mz_sql_parser::ast::Expr::InList {
                expr,
                list,
                negated,
            } => InList {
                expr: Box::new(self.fold_expr(*expr)),
                list: list.into_iter().map(|l| self.fold_expr(l)).collect(),
                negated,
            },
            mz_sql_parser::ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => InSubquery {
                expr: Box::new(self.fold_expr(*expr)),
                subquery: Box::new(self.fold_query(*subquery)),
                negated,
            },
            mz_sql_parser::ast::Expr::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => Like {
                expr: Box::new(self.fold_expr(*expr)),
                pattern: Box::new(self.fold_expr(*pattern)),
                escape: escape.map(|expr| Box::new(self.fold_expr(*expr))),
                case_insensitive,
                negated,
            },
            mz_sql_parser::ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Between {
                expr: Box::new(self.fold_expr(*expr)),
                negated,
                low: Box::new(self.fold_expr(*low)),
                high: Box::new(self.fold_expr(*high)),
            },
            mz_sql_parser::ast::Expr::Op { op, expr1, expr2 } => Op {
                op: self.fold_op(op),
                expr1: Box::new(self.fold_expr(*expr1)),
                expr2: expr2.map(|expr2| Box::new(self.fold_expr(*expr2))),
            },
            mz_sql_parser::ast::Expr::Cast { expr, data_type } => Cast {
                expr: Box::new(self.fold_expr(*expr)),
                data_type: self.fold_data_type(data_type),
            },
            mz_sql_parser::ast::Expr::Collate { expr, collation } => Collate {
                expr: Box::new(self.fold_expr(*expr)),
                collation: self.fold_unresolved_item_name(collation),
            },
            mz_sql_parser::ast::Expr::HomogenizingFunction { function, exprs } => {
                HomogenizingFunction {
                    function: self.fold_homogenizing_function(function),
                    exprs: exprs.into_iter().map(|expr| self.fold_expr(expr)).collect(),
                }
            }
            mz_sql_parser::ast::Expr::NullIf { l_expr, r_expr } => NullIf {
                l_expr: Box::new(self.fold_expr(*l_expr)),
                r_expr: Box::new(self.fold_expr(*r_expr)),
            },
            mz_sql_parser::ast::Expr::Nested(expr) => Nested(Box::new(self.fold_expr(*expr))),
            mz_sql_parser::ast::Expr::Row { exprs } => Row {
                exprs: exprs.into_iter().map(|expr| self.fold_expr(expr)).collect(),
            },
            mz_sql_parser::ast::Expr::Value(value) => Value(self.fold_value(value)),
            mz_sql_parser::ast::Expr::Function(mz_sql_parser::ast::Function {
                name,
                args,
                filter,
                over,
                distinct,
            }) => Function(mz_sql_parser::ast::Function {
                name: match name {
                    name @ RawItemName::Name(..) => {
                        self.fold_raw_object_name_name_internal(name, true)
                    }
                    _ => self.fold_item_name(name),
                },
                args: self.fold_function_args(args),
                filter: filter.map(|expr| Box::new(self.fold_expr(*expr))),
                over: over.map(|over| self.fold_window_spec(over)),
                distinct,
            }),
            mz_sql_parser::ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => Case {
                operand: operand.map(|expr| Box::new(self.fold_expr(*expr))),
                conditions: conditions
                    .into_iter()
                    .map(|expr| self.fold_expr(expr))
                    .collect(),
                results: results
                    .into_iter()
                    .map(|expr| self.fold_expr(expr))
                    .collect(),
                else_result: else_result.map(|expr| Box::new(self.fold_expr(*expr))),
            },
            mz_sql_parser::ast::Expr::Exists(query) => Exists(Box::new(self.fold_query(*query))),
            mz_sql_parser::ast::Expr::Subquery(subquery) => {
                Subquery(Box::new(self.fold_query(*subquery)))
            }
            mz_sql_parser::ast::Expr::AnySubquery { left, op, right } => AnySubquery {
                left: Box::new(self.fold_expr(*left)),
                op: self.fold_op(op),
                right: Box::new(self.fold_query(*right)),
            },
            mz_sql_parser::ast::Expr::AnyExpr { left, op, right } => AnyExpr {
                left: Box::new(self.fold_expr(*left)),
                op: self.fold_op(op),
                right: Box::new(self.fold_expr(*right)),
            },
            mz_sql_parser::ast::Expr::AllSubquery { left, op, right } => AllSubquery {
                left: Box::new(self.fold_expr(*left)),
                op: self.fold_op(op),
                right: Box::new(self.fold_query(*right)),
            },
            mz_sql_parser::ast::Expr::AllExpr { left, op, right } => AllExpr {
                left: Box::new(self.fold_expr(*left)),
                op: self.fold_op(op),
                right: Box::new(self.fold_expr(*right)),
            },
            mz_sql_parser::ast::Expr::Array(exprs) => {
                Array(exprs.into_iter().map(|expr| self.fold_expr(expr)).collect())
            }
            mz_sql_parser::ast::Expr::ArraySubquery(subquery) => {
                ArraySubquery(Box::new(self.fold_query(*subquery)))
            }
            mz_sql_parser::ast::Expr::List(exprs) => {
                List(exprs.into_iter().map(|expr| self.fold_expr(expr)).collect())
            }
            mz_sql_parser::ast::Expr::ListSubquery(subquery) => {
                ListSubquery(Box::new(self.fold_query(*subquery)))
            }
            mz_sql_parser::ast::Expr::Subscript { expr, positions } => Subscript {
                expr: Box::new(self.fold_expr(*expr)),
                positions: positions
                    .into_iter()
                    .map(|p| self.fold_subscript_position(p))
                    .collect(),
            },
        }
    }

    fn fold_table_function(
        &mut self,
        node: mz_sql_parser::ast::TableFunction<Raw>,
    ) -> mz_sql_parser::ast::TableFunction<Aug> {
        mz_sql_parser::ast::TableFunction {
            name: match &node.name {
                RawItemName::Name(name) => {
                    if *name == UnresolvedItemName::unqualified("values") && self.status.is_ok() {
                        self.status = Err(PlanError::FromValueRequiresParen);
                    }

                    self.fold_raw_object_name_name_internal(node.name, true)
                }
                RawItemName::Id(..) => self.fold_item_name(node.name),
            },
            args: self.fold_function_args(node.args),
        }
    }
}

/// Resolves names in an AST node using the provided catalog.
#[tracing::instrument(
    target = "compiler",
    level = "trace",
    name = "ast_resolve_names",
    skip_all
)]
pub fn resolve<N>(
    catalog: &dyn SessionCatalog,
    node: N,
) -> Result<(N::Folded, BTreeSet<GlobalId>), PlanError>
where
    N: FoldNode<Raw, Aug>,
{
    let mut resolver = NameResolver::new(catalog);
    let result = node.fold(&mut resolver);
    resolver.status?;
    Ok((result, resolver.ids))
}

#[derive(Debug)]
/// An AST visitor that transforms an AST that contains temporary GlobalId references to one where
/// every temporary GlobalId has been replaced by its final allocated id, as dictated by the
/// provided `allocation`
///
/// This is useful when trying to create multiple objects in a single DDL transaction and the
/// objects that are about to be don't have allocated GlobalIds yet. What we can do in that case is
/// for the planner to assign temporary `GlobalId::Transient` identifiers to all the objects that
/// it wants to create and use those for any interelationships.
///
/// Then, when the coordinator receives the list of plans to be executed it can batch allocate
/// the final `GlobalIds` and use this TransientResolver to walk through all the ASTs and make them
/// refer to the final GlobalIds of the objects.
pub struct TransientResolver<'a> {
    /// A map from transient `GlobalId`s to their final non-transient `GlobalId`s.
    allocation: &'a BTreeMap<GlobalId, GlobalId>,
    status: Result<(), PlanError>,
}

impl<'a> TransientResolver<'a> {
    fn new(allocation: &'a BTreeMap<GlobalId, GlobalId>) -> Self {
        TransientResolver {
            allocation,
            status: Ok(()),
        }
    }
}

impl Fold<Aug, Aug> for TransientResolver<'_> {
    fn fold_item_name(&mut self, item_name: ResolvedItemName) -> ResolvedItemName {
        match item_name {
            ResolvedItemName::Item {
                id: transient_id @ GlobalId::Transient(_),
                qualifiers,
                full_name,
                print_id,
            } => {
                let id = match self.allocation.get(&transient_id) {
                    Some(id) => *id,
                    None => {
                        let obj = ResolvedItemName::Item {
                            id: transient_id,
                            qualifiers: qualifiers.clone(),
                            full_name: full_name.clone(),
                            print_id,
                        };
                        self.status = Err(PlanError::InvalidObject(Box::new(obj)));
                        transient_id
                    }
                };
                ResolvedItemName::Item {
                    id,
                    qualifiers,
                    full_name,
                    print_id,
                }
            }
            other => other,
        }
    }
    fn fold_cluster_name(
        &mut self,
        node: <Aug as AstInfo>::ClusterName,
    ) -> <Aug as AstInfo>::ClusterName {
        node
    }
    fn fold_cte_id(&mut self, node: <Aug as AstInfo>::CteId) -> <Aug as AstInfo>::CteId {
        node
    }
    fn fold_data_type(&mut self, node: <Aug as AstInfo>::DataType) -> <Aug as AstInfo>::DataType {
        node
    }
    fn fold_database_name(
        &mut self,
        node: <Aug as AstInfo>::DatabaseName,
    ) -> <Aug as AstInfo>::DatabaseName {
        node
    }
    fn fold_nested_statement(
        &mut self,
        node: <Aug as AstInfo>::NestedStatement,
    ) -> <Aug as AstInfo>::NestedStatement {
        node
    }
    fn fold_schema_name(
        &mut self,
        node: <Aug as AstInfo>::SchemaName,
    ) -> <Aug as AstInfo>::SchemaName {
        node
    }
    fn fold_role_name(&mut self, node: <Aug as AstInfo>::RoleName) -> <Aug as AstInfo>::RoleName {
        node
    }
    fn fold_object_name(
        &mut self,
        node: <Aug as AstInfo>::ObjectName,
    ) -> <Aug as AstInfo>::ObjectName {
        node
    }
}

pub fn resolve_transient_ids<N>(
    allocation: &BTreeMap<GlobalId, GlobalId>,
    node: N,
) -> Result<N::Folded, PlanError>
where
    N: FoldNode<Aug, Aug>,
{
    let mut resolver = TransientResolver::new(allocation);
    let result = node.fold(&mut resolver);
    resolver.status?;
    Ok(result)
}

#[derive(Debug, Default)]
pub struct DependencyVisitor {
    ids: BTreeSet<GlobalId>,
}

impl<'ast> Visit<'ast, Aug> for DependencyVisitor {
    fn visit_item_name(&mut self, item_name: &'ast <Aug as AstInfo>::ItemName) {
        if let ResolvedItemName::Item { id, .. } = item_name {
            self.ids.insert(*id);
        }
    }

    fn visit_data_type(&mut self, data_type: &'ast <Aug as AstInfo>::DataType) {
        match data_type {
            ResolvedDataType::AnonymousList(data_type) => self.visit_data_type(data_type),
            ResolvedDataType::AnonymousMap {
                key_type,
                value_type,
            } => {
                self.visit_data_type(key_type);
                self.visit_data_type(value_type);
            }
            ResolvedDataType::Named { id, .. } => {
                self.ids.insert(*id);
            }
            ResolvedDataType::Error => {}
        }
    }
}

pub fn visit_dependencies<'ast, N>(node: &'ast N) -> BTreeSet<GlobalId>
where
    N: VisitNode<'ast, Aug> + 'ast,
{
    let mut visitor = DependencyVisitor::default();
    node.visit(&mut visitor);
    visitor.ids
}

// Used when displaying a view's source for human creation. If the name
// specified is the same as the name in the catalog, we don't use the ID format.
#[derive(Debug)]
pub struct NameSimplifier<'a> {
    pub catalog: &'a dyn SessionCatalog,
}

impl<'ast, 'a> VisitMut<'ast, Aug> for NameSimplifier<'a> {
    fn visit_cluster_name_mut(&mut self, node: &mut ResolvedClusterName) {
        node.print_name = Some(self.catalog.get_cluster(node.id).name().into());
    }

    fn visit_item_name_mut(&mut self, name: &mut ResolvedItemName) {
        if let ResolvedItemName::Item {
            id,
            full_name,
            print_id,
            ..
        } = name
        {
            let item = self.catalog.get_item(id);
            let catalog_full_name = self.catalog.resolve_full_name(item.name());
            if catalog_full_name == *full_name {
                *print_id = false;
            }
        }
    }

    fn visit_data_type_mut(&mut self, name: &mut ResolvedDataType) {
        if let ResolvedDataType::Named {
            id,
            full_name,
            print_id,
            ..
        } = name
        {
            let item = self.catalog.get_item(id);
            let catalog_full_name = self.catalog.resolve_full_name(item.name());
            if catalog_full_name == *full_name {
                *print_id = false;
            }
        }
    }
}
