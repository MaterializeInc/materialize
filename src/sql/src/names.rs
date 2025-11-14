// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structured name types for SQL objects.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::anyhow;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::LocalId;
use mz_ore::assert_none;
use mz_ore::cast::CastFrom;
use mz_ore::str::StrExt;
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, RelationVersion};
use mz_repr::{ColumnName, RelationVersionSelector};
use mz_sql_parser::ast::visit_mut::VisitMutNode;
use mz_sql_parser::ast::{CreateContinualTaskStatement, Expr, RawNetworkPolicyName, Version};
use mz_sql_parser::ident;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use uncased::UncasedStr;

use crate::ast::display::{AstDisplay, AstFormatter};
use crate::ast::fold::{Fold, FoldNode};
use crate::ast::visit::{Visit, VisitNode};
use crate::ast::visit_mut::VisitMut;
use crate::ast::{
    self, AstInfo, Cte, CteBlock, CteMutRec, DocOnIdentifier, GrantTargetSpecification,
    GrantTargetSpecificationInner, Ident, MutRecBlock, ObjectType, Query, Raw, RawClusterName,
    RawDataType, RawItemName, Statement, UnresolvedItemName, UnresolvedObjectName,
};
use crate::catalog::{
    CatalogError, CatalogItem, CatalogItemType, CatalogType, CatalogTypeDetails, SessionCatalog,
};
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

impl FullItemName {
    /// Converts the name into a string vector of its constituent parts:
    /// database (if present), schema, and item.
    pub fn into_parts(self) -> Vec<String> {
        let mut parts = vec![];
        if let RawDatabaseSpecifier::Name(name) = self.database {
            parts.push(name);
        }
        parts.push(self.schema);
        parts.push(self.item);
        parts
    }
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
        // TODO(parkmycar): Change UnresolvedItemName to use `Ident` internally.
        let mut name_parts = Vec::new();
        if let RawDatabaseSpecifier::Name(database) = full_name.database {
            name_parts.push(Ident::new_unchecked(database));
        }
        name_parts.push(Ident::new_unchecked(full_name.schema));
        name_parts.push(Ident::new_unchecked(full_name.item));
        UnresolvedItemName(name_parts)
    }
}

/// A fully-qualified non-human readable name of an item in the catalog using IDs for the database
/// and schema.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct QualifiedItemName {
    pub qualifiers: ItemQualifiers,
    pub item: String,
}

// Do not implement [`Display`] for [`QualifiedItemName`]. [`FullItemName`] should always be
// displayed instead.
static_assertions::assert_not_impl_any!(QualifiedItemName: fmt::Display);

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
        // TODO(parkmycar): Change UnresolvedItemName to use `Ident` internally.
        let mut name_parts = Vec::new();
        if let Some(database) = partial_name.database {
            name_parts.push(Ident::new_unchecked(database));
        }
        if let Some(schema) = partial_name.schema {
            name_parts.push(Ident::new_unchecked(schema));
        }
        name_parts.push(Ident::new_unchecked(partial_name.item));
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
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub enum ResolvedDatabaseSpecifier {
    /// The "ambient" database, which is always present and is not named
    /// explicitly, but by omission.
    Ambient,
    /// A normal database with a name.
    Id(DatabaseId),
}

impl ResolvedDatabaseSpecifier {
    pub fn id(&self) -> Option<DatabaseId> {
        match self {
            ResolvedDatabaseSpecifier::Ambient => None,
            ResolvedDatabaseSpecifier::Id(id) => Some(*id),
        }
    }
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

impl From<Option<DatabaseId>> for ResolvedDatabaseSpecifier {
    fn from(id: Option<DatabaseId>) -> Self {
        match id {
            Some(id) => Self::Id(id),
            None => Self::Ambient,
        }
    }
}

/*
 * TODO(jkosh44) It's possible that in order to fix
 * https://github.com/MaterializeInc/database-issues/issues/2689 we will need to assign temporary
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

    pub fn is_user(&self) -> bool {
        match self {
            SchemaSpecifier::Temporary => true,
            SchemaSpecifier::Id(id) => id.is_user(),
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ItemQualifiers {
    pub database_spec: ResolvedDatabaseSpecifier,
    pub schema_spec: SchemaSpecifier,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResolvedItemName {
    Item {
        id: CatalogItemId,
        qualifiers: ItemQualifiers,
        full_name: FullItemName,
        // Whether this object, when printed out, should use [id AS name] syntax. We
        // want this for things like tables and sources, but not for things like
        // types.
        print_id: bool,
        version: RelationVersionSelector,
    },
    Cte {
        id: LocalId,
        name: String,
    },
    ContinualTask {
        id: LocalId,
        name: PartialItemName,
    },
    Error,
}

impl ResolvedItemName {
    pub fn full_name_str(&self) -> String {
        match self {
            ResolvedItemName::Item { full_name, .. } => full_name.to_string(),
            ResolvedItemName::Cte { name, .. } => name.clone(),
            ResolvedItemName::ContinualTask { name, .. } => name.to_string(),
            ResolvedItemName::Error => "error in name resolution".to_string(),
        }
    }

    pub fn full_item_name(&self) -> &FullItemName {
        match self {
            ResolvedItemName::Item { full_name, .. } => full_name,
            _ => panic!("cannot call object_full_name on non-object"),
        }
    }

    pub fn item_id(&self) -> &CatalogItemId {
        match self {
            ResolvedItemName::Item { id, .. } => id,
            _ => panic!("cannot call item_id on non-object"),
        }
    }

    pub fn version(&self) -> &RelationVersionSelector {
        match self {
            ResolvedItemName::Item { version, .. } => version,
            _ => panic!("cannot call version on non-object"),
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
                version,
            } => {
                if *print_id {
                    f.write_str(format!("[{} AS ", id));
                }
                if let RawDatabaseSpecifier::Name(database) = &full_name.database {
                    f.write_node(&Ident::new_unchecked(database));
                    f.write_str(".");
                }
                f.write_node(&Ident::new_unchecked(&full_name.schema));
                f.write_str(".");
                f.write_node(&Ident::new_unchecked(&full_name.item));

                if *print_id {
                    if let RelationVersionSelector::Specific(version) = version {
                        let version: Version = (*version).into();
                        f.write_str(" VERSION ");
                        f.write_node(&version);
                    }
                }

                if *print_id {
                    f.write_str("]");
                }
            }
            ResolvedItemName::Cte { name, .. } => f.write_node(&Ident::new_unchecked(name)),
            ResolvedItemName::ContinualTask { name, .. } => {
                // TODO: Remove this once PartialItemName uses Ident instead of
                // String.
                if let Some(database) = name.database.as_ref() {
                    f.write_node(&Ident::new_unchecked(database));
                    f.write_str(".");
                }
                if let Some(schema) = name.schema.as_ref() {
                    f.write_node(&Ident::new_unchecked(schema));
                    f.write_str(".");
                }
                f.write_node(&Ident::new_unchecked(&name.item));
            }
            ResolvedItemName::Error => {}
        }
    }
}

impl std::fmt::Display for ResolvedItemName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_ast_string_simple().as_str())
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResolvedColumnReference {
    Column { name: ColumnName, index: usize },
    Error,
}

impl AstDisplay for ResolvedColumnReference {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ResolvedColumnReference::Column { name, .. } => {
                f.write_node(&Ident::new_unchecked(name.as_str()));
            }
            ResolvedColumnReference::Error => {}
        }
    }
}

impl std::fmt::Display for ResolvedColumnReference {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_ast_string_simple().as_str())
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
    pub fn database_spec(&self) -> &ResolvedDatabaseSpecifier {
        match self {
            ResolvedSchemaName::Schema { database_spec, .. } => database_spec,
            ResolvedSchemaName::Error => {
                unreachable!("should have been handled by name resolution")
            }
        }
    }

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
                    f.write_node(&Ident::new_unchecked(database));
                    f.write_str(".");
                }
                f.write_node(&Ident::new_unchecked(&full_name.schema));
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
            ResolvedDatabaseName::Database { name, .. } => {
                f.write_node(&Ident::new_unchecked(name))
            }
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
            f.write_node(&Ident::new_unchecked(print_name))
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
        id: CatalogItemId,
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
                    f.write_node(&Ident::new_unchecked(database));
                    f.write_str(".");
                }

                f.write_node(&Ident::new_unchecked(&full_name.schema));
                f.write_str(".");

                f.write_node(&Ident::new_unchecked(&full_name.item));
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
    /// This is used to generate default column names for cast operations.
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

    /// Return the name of `self`'s without IDs or modifiers.
    ///
    /// This is used for error messages.
    pub fn human_readable_name(&self) -> String {
        let mut res = String::new();
        match self {
            ResolvedDataType::AnonymousList(element_type) => {
                res += &element_type.human_readable_name();
                res += " list";
            }
            ResolvedDataType::AnonymousMap {
                key_type,
                value_type,
            } => {
                res += "map[";
                res += &key_type.human_readable_name();
                res += "=>";
                res += &value_type.human_readable_name();
                res += "]";
            }
            ResolvedDataType::Named { full_name, .. } => {
                if let RawDatabaseSpecifier::Name(database) = &full_name.database {
                    res += database;
                    res += ".";
                }
                res += &full_name.schema;
                res += ".";
                res += &full_name.item;
            }
            ResolvedDataType::Error => {}
        }
        res
    }
}

impl fmt::Display for ResolvedDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.to_ast_string_simple().as_str())
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
pub struct ResolvedNetworkPolicyName {
    pub id: NetworkPolicyId,
    pub name: String,
}

impl AstDisplay for ResolvedNetworkPolicyName {
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
    NetworkPolicy(ResolvedNetworkPolicyName),
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
            ResolvedObjectName::NetworkPolicy(n) => f.write_node(n),
        }
    }
}

impl AstInfo for Aug {
    type NestedStatement = Statement<Raw>;
    type ItemName = ResolvedItemName;
    type ColumnReference = ResolvedColumnReference;
    type SchemaName = ResolvedSchemaName;
    type DatabaseName = ResolvedDatabaseName;
    type ClusterName = ResolvedClusterName;
    type DataType = ResolvedDataType;
    type CteId = LocalId;
    type RoleName = ResolvedRoleName;
    type ObjectName = ResolvedObjectName;
    type NetworkPolicyName = ResolvedNetworkPolicyName;
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

pub static PUBLIC_ROLE_NAME: LazyLock<&UncasedStr> = LazyLock::new(|| UncasedStr::new("PUBLIC"));

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ObjectId {
    Cluster(ClusterId),
    ClusterReplica((ClusterId, ReplicaId)),
    Database(DatabaseId),
    Schema((ResolvedDatabaseSpecifier, SchemaSpecifier)),
    Role(RoleId),
    Item(CatalogItemId),
    NetworkPolicy(NetworkPolicyId),
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
    pub fn unwrap_item_id(self) -> CatalogItemId {
        match self {
            ObjectId::Item(id) => id,
            _ => panic!("ObjectId::unwrap_item_id called on {self:?}"),
        }
    }

    pub fn is_system(&self) -> bool {
        match self {
            ObjectId::Cluster(cluster_id) => cluster_id.is_system(),
            ObjectId::ClusterReplica((_cluster_id, replica_id)) => replica_id.is_system(),
            ObjectId::Database(database_id) => database_id.is_system(),
            ObjectId::Schema((_database_id, schema_id)) => schema_id.is_system(),
            ObjectId::Role(role_id) => role_id.is_system(),
            ObjectId::Item(global_id) => global_id.is_system(),
            ObjectId::NetworkPolicy(network_policy_id) => network_policy_id.is_system(),
        }
    }

    pub fn is_user(&self) -> bool {
        match self {
            ObjectId::Cluster(cluster_id) => cluster_id.is_user(),
            ObjectId::ClusterReplica((_cluster_id, replica_id)) => replica_id.is_user(),
            ObjectId::Database(database_id) => database_id.is_user(),
            ObjectId::Schema((_database_id, schema_id)) => schema_id.is_user(),
            ObjectId::Role(role_id) => role_id.is_user(),
            ObjectId::Item(global_id) => global_id.is_user(),
            ObjectId::NetworkPolicy(network_policy_id) => network_policy_id.is_user(),
        }
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectId::Cluster(cluster_id) => write!(f, "C{cluster_id}"),
            ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                write!(f, "CR{cluster_id}.{replica_id}")
            }
            ObjectId::Database(database_id) => write!(f, "D{database_id}"),
            ObjectId::Schema((database_spec, schema_spec)) => {
                let database_id = match database_spec {
                    ResolvedDatabaseSpecifier::Ambient => "".to_string(),
                    ResolvedDatabaseSpecifier::Id(database_id) => format!("{database_id}."),
                };
                write!(f, "S{database_id}{schema_spec}")
            }
            ObjectId::Role(role_id) => write!(f, "R{role_id}"),
            ObjectId::Item(item_id) => write!(f, "I{item_id}"),
            ObjectId::NetworkPolicy(network_policy_id) => write!(f, "NP{network_policy_id}"),
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
                ResolvedItemName::ContinualTask { .. } => {
                    Err(anyhow!("ContinualTask does not correspond to object"))
                }
                ResolvedItemName::Error => Err(anyhow!("error in name resolution")),
            },
            ResolvedObjectName::NetworkPolicy(name) => Ok(ObjectId::NetworkPolicy(name.id)),
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

impl From<CatalogItemId> for ObjectId {
    fn from(id: CatalogItemId) -> Self {
        ObjectId::Item(id)
    }
}

impl From<&CatalogItemId> for ObjectId {
    fn from(id: &CatalogItemId) -> Self {
        ObjectId::Item(*id)
    }
}

impl From<CommentObjectId> for ObjectId {
    fn from(id: CommentObjectId) -> Self {
        match id {
            CommentObjectId::Table(item_id)
            | CommentObjectId::View(item_id)
            | CommentObjectId::MaterializedView(item_id)
            | CommentObjectId::Source(item_id)
            | CommentObjectId::Sink(item_id)
            | CommentObjectId::Index(item_id)
            | CommentObjectId::Func(item_id)
            | CommentObjectId::Connection(item_id)
            | CommentObjectId::Type(item_id)
            | CommentObjectId::Secret(item_id)
            | CommentObjectId::ContinualTask(item_id)
            | CommentObjectId::ReplacementMaterializedView(item_id) => ObjectId::Item(item_id),
            CommentObjectId::Role(id) => ObjectId::Role(id),
            CommentObjectId::Database(id) => ObjectId::Database(id),
            CommentObjectId::Schema(id) => ObjectId::Schema(id),
            CommentObjectId::Cluster(id) => ObjectId::Cluster(id),
            CommentObjectId::ClusterReplica(id) => ObjectId::ClusterReplica(id),
            CommentObjectId::NetworkPolicy(id) => ObjectId::NetworkPolicy(id),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SystemObjectId {
    /// The ID of a specific object.
    Object(ObjectId),
    /// Identifier for the entire system.
    System,
}

impl SystemObjectId {
    pub fn object_id(&self) -> Option<&ObjectId> {
        match self {
            SystemObjectId::Object(object_id) => Some(object_id),
            SystemObjectId::System => None,
        }
    }

    pub fn is_system(&self) -> bool {
        matches!(self, SystemObjectId::System)
    }
}

impl From<ObjectId> for SystemObjectId {
    fn from(id: ObjectId) -> Self {
        SystemObjectId::Object(id)
    }
}

/// Comments can be applied to multiple kinds of objects (e.g. Tables and Role), so we need a way
/// to represent these different types and their IDs (e.g. [`CatalogItemId`] and [`RoleId`]), as
/// well as the inner kind of object that is represented, e.g. [`CatalogItemId`] is used to
/// identify both Tables and Views. No other kind of ID encapsulates all of this, hence this new
/// "*Id" type.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum CommentObjectId {
    Table(CatalogItemId),
    View(CatalogItemId),
    MaterializedView(CatalogItemId),
    Source(CatalogItemId),
    Sink(CatalogItemId),
    Index(CatalogItemId),
    Func(CatalogItemId),
    Connection(CatalogItemId),
    Type(CatalogItemId),
    Secret(CatalogItemId),
    ContinualTask(CatalogItemId),
    Role(RoleId),
    Database(DatabaseId),
    Schema((ResolvedDatabaseSpecifier, SchemaSpecifier)),
    Cluster(ClusterId),
    ClusterReplica((ClusterId, ReplicaId)),
    NetworkPolicy(NetworkPolicyId),
    ReplacementMaterializedView(CatalogItemId),
}

/// Whether to resolve an name in the types namespace, the functions namespace,
/// or the relations namespace. It is possible to resolve name in multiple
/// namespaces, in which case types are preferred to functions are preferred to
/// relations.
// NOTE(benesch,sploiselle): The fact that some names are looked up in multiple
// namespaces is a bit dubious, and stems from the fact that we don't
// automatically create types for relations (see database-issues#7142). It's possible that we
// don't allow names to be looked up in multiple namespaces (i.e., this becomes
// `enum ItemResolutionNamespace`), but it's also possible that the design of
// the `DOC ON TYPE` option means we're forever stuck with this complexity.
#[derive(Debug, Clone, Copy)]
struct ItemResolutionConfig {
    types: bool,
    functions: bool,
    relations: bool,
}

#[derive(Debug)]
pub struct NameResolver<'a> {
    catalog: &'a dyn SessionCatalog,
    ctes: BTreeMap<String, LocalId>,
    continual_task: Option<(PartialItemName, LocalId)>,
    status: Result<(), PlanError>,
    ids: BTreeMap<CatalogItemId, BTreeSet<GlobalId>>,
}

impl<'a> NameResolver<'a> {
    fn new(catalog: &'a dyn SessionCatalog) -> NameResolver<'a> {
        NameResolver {
            catalog,
            ctes: BTreeMap::new(),
            continual_task: None,
            status: Ok(()),
            ids: BTreeMap::new(),
        }
    }

    fn resolve_data_type(&mut self, data_type: RawDataType) -> Result<ResolvedDataType, PlanError> {
        match data_type {
            RawDataType::Array(elem_type) => {
                let name = elem_type.to_string();
                match self.resolve_data_type(*elem_type)? {
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
                            None => {
                                // Resolution should never produce a
                                // `ResolvedDataType::Named` with an ID of a
                                // non-type, but we error gracefully just in
                                // case.
                                sql_bail!(
                                    "internal error: {} does not refer to a type",
                                    self.catalog
                                        .resolve_full_name(element_item.name())
                                        .to_string()
                                        .quoted()
                                );
                            }
                        };
                        self.ids.insert(array_item.id(), BTreeSet::new());
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
                let elem_type = self.resolve_data_type(*elem_type)?;
                Ok(ResolvedDataType::AnonymousList(Box::new(elem_type)))
            }
            RawDataType::Map {
                key_type,
                value_type,
            } => {
                let key_type = self.resolve_data_type(*key_type)?;
                let value_type = self.resolve_data_type(*value_type)?;
                Ok(ResolvedDataType::AnonymousMap {
                    key_type: Box::new(key_type),
                    value_type: Box::new(value_type),
                })
            }
            RawDataType::Other { name, typ_mod } => {
                let (full_name, item) = match name {
                    RawItemName::Name(name) => {
                        let name = normalize::unresolved_item_name(name)?;
                        let item = self.catalog.resolve_type(&name)?;
                        let full_name = self.catalog.resolve_full_name(item.name());
                        (full_name, item)
                    }
                    RawItemName::Id(id, name, version) => {
                        let id: CatalogItemId = id.parse()?;
                        let item = self.catalog.get_item(&id);
                        let full_name = normalize::full_name(name)?;
                        assert_none!(version, "no support for versioning data types");

                        (full_name, item)
                    }
                };
                self.ids.insert(item.id(), BTreeSet::new());
                // If this is a named array type, then make sure to include the element reference
                // in the resolved IDs. This helps ensure that named array types are resolved the
                // same as an array type with the same element type. For example, `int4[]` and
                // `_int4` should have the same set of resolved IDs.
                if let Some(CatalogTypeDetails {
                    typ: CatalogType::Array { element_reference },
                    ..
                }) = item.type_details()
                {
                    self.ids.insert(*element_reference, BTreeSet::new());
                }
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

    fn resolve_item_name(
        &mut self,
        item_name: RawItemName,
        config: ItemResolutionConfig,
    ) -> ResolvedItemName {
        match item_name {
            RawItemName::Name(name) => self.resolve_item_name_name(name, config),
            RawItemName::Id(id, raw_name, version) => {
                self.resolve_item_name_id(id, raw_name, version)
            }
        }
    }

    fn resolve_item_name_name(
        &mut self,
        raw_name: UnresolvedItemName,
        config: ItemResolutionConfig,
    ) -> ResolvedItemName {
        let raw_name = match normalize::unresolved_item_name(raw_name) {
            Ok(raw_name) => raw_name,
            Err(e) => {
                if self.status.is_ok() {
                    self.status = Err(e);
                }
                return ResolvedItemName::Error;
            }
        };

        let mut r: Result<&dyn CatalogItem, CatalogError> =
            Err(CatalogError::UnknownItem(raw_name.to_string()));

        if r.is_err() && config.types {
            r = self.catalog.resolve_type(&raw_name);
        }

        if r.is_err() && config.functions {
            r = self.catalog.resolve_function(&raw_name);
        }

        if r.is_err() && config.relations {
            // Check if unqualified name refers to a CTE.
            //
            // Note that this is done in non-function contexts as CTEs
            // are treated as relations.
            if raw_name.database.is_none() && raw_name.schema.is_none() {
                let norm_name = normalize::ident(Ident::new_unchecked(&raw_name.item));
                if let Some(id) = self.ctes.get(&norm_name) {
                    return ResolvedItemName::Cte {
                        id: *id,
                        name: norm_name,
                    };
                }
            }
            if let Some((ct_name, ct_id)) = self.continual_task.as_ref() {
                if *ct_name == raw_name {
                    return ResolvedItemName::ContinualTask {
                        id: *ct_id,
                        name: raw_name,
                    };
                }
            }
            r = self.catalog.resolve_item(&raw_name);
        };

        match r {
            Ok(item) => {
                // Record the item at its current version.
                let item = item.at_version(RelationVersionSelector::Latest);
                self.ids
                    .entry(item.id())
                    .or_default()
                    .insert(item.global_id());
                let print_id = !matches!(
                    item.item_type(),
                    CatalogItemType::Func | CatalogItemType::Type
                );
                let alter_table_enabled =
                    self.catalog.system_vars().enable_alter_table_add_column();
                let version = match item.latest_version() {
                    // Only track the version of referenced object if the feature is enabled.
                    Some(v) if item.id().is_user() && alter_table_enabled => {
                        RelationVersionSelector::Specific(v)
                    }
                    _ => RelationVersionSelector::Latest,
                };

                ResolvedItemName::Item {
                    id: item.id(),
                    qualifiers: item.name().qualifiers.clone(),
                    full_name: self.catalog.resolve_full_name(item.name()),
                    print_id,
                    version,
                }
            }
            Err(mut e) => {
                if self.status.is_ok() {
                    match &mut e {
                        CatalogError::UnknownFunction {
                            name: _,
                            alternative,
                        } => {
                            // Suggest using the `jsonb_` version of `json_`
                            // functions that do not exist.
                            if raw_name.database.is_none()
                                && (raw_name.schema.is_none()
                                    || raw_name.schema.as_deref() == Some("pg_catalog")
                                        && raw_name.item.starts_with("json_"))
                            {
                                let jsonb_name = PartialItemName {
                                    item: raw_name.item.replace("json_", "jsonb_"),
                                    ..raw_name
                                };
                                if self.catalog.resolve_function(&jsonb_name).is_ok() {
                                    *alternative = Some(jsonb_name.to_string());
                                }
                            }
                        }
                        _ => (),
                    }

                    self.status = Err(e.into());
                }
                ResolvedItemName::Error
            }
        }
    }

    fn resolve_item_name_id(
        &mut self,
        id: String,
        raw_name: UnresolvedItemName,
        version: Option<Version>,
    ) -> ResolvedItemName {
        let id: CatalogItemId = match id.parse() {
            Ok(id) => id,
            Err(e) => {
                if self.status.is_ok() {
                    self.status = Err(e.into());
                }
                return ResolvedItemName::Error;
            }
        };
        let item = match self.catalog.try_get_item(&id) {
            Some(item) => item,
            None => {
                if self.status.is_ok() {
                    self.status = Err(PlanError::InvalidId(id));
                }
                return ResolvedItemName::Error;
            }
        };
        let alter_table_enabled = self.catalog.system_vars().enable_alter_table_add_column();
        let version = match version {
            // If there isn't a version specified, and this item supports versioning, track the
            // latest.
            None => match item.latest_version() {
                // Only track the version of the referenced object, if the feature is enabled.
                Some(v) if alter_table_enabled => RelationVersionSelector::Specific(v),
                _ => RelationVersionSelector::Latest,
            },
            // Note: Return the specific version if one is specified, even if the feature is off.
            Some(v) => {
                let specified_version = RelationVersion::from(v);
                match item.latest_version() {
                    Some(latest) if latest >= specified_version => {
                        RelationVersionSelector::Specific(specified_version)
                    }
                    _ => {
                        if self.status.is_ok() {
                            self.status = Err(PlanError::InvalidVersion {
                                name: item.name().item.clone(),
                                version: v.to_string(),
                            })
                        }
                        return ResolvedItemName::Error;
                    }
                }
            }
        };
        let item = item.at_version(version);
        self.ids
            .entry(item.id())
            .or_default()
            .insert(item.global_id());

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
            id,
            qualifiers: item.name().qualifiers.clone(),
            full_name,
            print_id: true,
            version,
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
            // Queries can be recursive, so need the ability to grow the stack.
            body: mz_ore::stack::maybe_grow(|| self.fold_set_expr(q.body)),
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

    fn fold_create_continual_task_statement(
        &mut self,
        stmt: CreateContinualTaskStatement<Raw>,
    ) -> CreateContinualTaskStatement<Aug> {
        // Insert a LocalId so that using the name of the continual task in the
        // inserts and deletes resolves.
        match normalize::unresolved_item_name(stmt.name.name().clone()) {
            Ok(local_name) => {
                assert!(self.continual_task.is_none());
                // TODO: Assign LocalIds more robustly (e.g. something like a
                // `self.next_local_id` field).
                self.continual_task = Some((local_name, LocalId::new(0)));
            }
            Err(err) => {
                if self.status.is_ok() {
                    self.status = Err(err);
                }
            }
        };
        mz_sql_parser::ast::fold::fold_create_continual_task_statement(self, stmt)
    }

    fn fold_cte_id(&mut self, _id: <Raw as AstInfo>::CteId) -> <Aug as AstInfo>::CteId {
        panic!("this should have been handled when walking the CTE");
    }

    fn fold_item_name(
        &mut self,
        item_name: <Raw as AstInfo>::ItemName,
    ) -> <Aug as AstInfo>::ItemName {
        self.resolve_item_name(
            item_name,
            // By default, when resolving an item name, we assume only relations
            // should be in scope.
            ItemResolutionConfig {
                functions: false,
                types: false,
                relations: true,
            },
        )
    }

    fn fold_column_name(&mut self, column_name: ast::ColumnName<Raw>) -> ast::ColumnName<Aug> {
        let item_name = self.resolve_item_name(
            column_name.relation,
            ItemResolutionConfig {
                functions: false,
                types: true,
                relations: true,
            },
        );

        match &item_name {
            ResolvedItemName::Item {
                id,
                full_name,
                version,
                qualifiers: _,
                print_id: _,
            } => {
                let item = self.catalog.get_item(id).at_version(*version);
                let desc = match item.desc(full_name) {
                    Ok(desc) => desc,
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e.into());
                        }
                        return ast::ColumnName {
                            relation: ResolvedItemName::Error,
                            column: ResolvedColumnReference::Error,
                        };
                    }
                };

                let name = normalize::column_name(column_name.column.clone());
                let Some((index, _typ)) = desc.get_by_name(&name) else {
                    if self.status.is_ok() {
                        let similar = desc.iter_similar_names(&name).cloned().collect();
                        self.status = Err(PlanError::UnknownColumn {
                            table: Some(full_name.clone().into()),
                            column: name,
                            similar,
                        })
                    }
                    return ast::ColumnName {
                        relation: ResolvedItemName::Error,
                        column: ResolvedColumnReference::Error,
                    };
                };

                ast::ColumnName {
                    relation: item_name,
                    column: ResolvedColumnReference::Column { name, index },
                }
            }
            ResolvedItemName::Cte { .. }
            | ResolvedItemName::ContinualTask { .. }
            | ResolvedItemName::Error => ast::ColumnName {
                relation: ResolvedItemName::Error,
                column: ResolvedColumnReference::Error,
            },
        }
    }

    fn fold_column_reference(
        &mut self,
        _node: <Raw as AstInfo>::ColumnReference,
    ) -> <Aug as AstInfo>::ColumnReference {
        // Do not call this function directly; instead resolve through `fold_column_name`
        ResolvedColumnReference::Error
    }

    fn fold_data_type(
        &mut self,
        data_type: <Raw as AstInfo>::DataType,
    ) -> <Aug as AstInfo>::DataType {
        match self.resolve_data_type(data_type) {
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
                            id: ClusterId::system(0).expect("0 is a valid ID"),
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
                        id: ClusterId::system(0).expect("0 is a valid ID"),
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
            Map(map) => Map(map
                .into_iter()
                .map(|(k, v)| (k, self.fold_with_option_value(v)))
                .collect()),
            Value(v) => Value(self.fold_value(v)),
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
                    ResolvedItemName::Cte { .. } | ResolvedItemName::ContinualTask { .. } => {
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
                    ResolvedItemName::Cte { .. } | ResolvedItemName::ContinualTask { .. } => {
                        self.status = Err(PlanError::InvalidObject(Box::new(item_name.clone())));
                    }
                    ResolvedItemName::Error => {}
                }
                Item(item_name)
            }
            UnresolvedItemName(name) => UnresolvedItemName(self.fold_unresolved_item_name(name)),
            Ident(name) => Ident(self.fold_ident(name)),
            Expr(e) => Expr(self.fold_expr(e)),
            ClusterReplicas(replicas) => ClusterReplicas(
                replicas
                    .into_iter()
                    .map(|r| self.fold_replica_definition(r))
                    .collect(),
            ),
            ConnectionKafkaBroker(broker) => ConnectionKafkaBroker(self.fold_kafka_broker(broker)),
            ConnectionAwsPrivatelink(privatelink) => {
                ConnectionAwsPrivatelink(self.fold_connection_default_aws_privatelink(privatelink))
            }
            RetainHistoryFor(value) => RetainHistoryFor(self.fold_value(value)),
            Refresh(refresh) => Refresh(self.fold_refresh_option_value(refresh)),
            ClusterScheduleOptionValue(value) => ClusterScheduleOptionValue(value),
            ClusterAlterStrategy(value) => {
                ClusterAlterStrategy(self.fold_cluster_alter_option_value(value))
            }
            NetworkPolicyRules(rules) => NetworkPolicyRules(
                rules
                    .into_iter()
                    .map(|r| self.fold_network_policy_rule_definition(r))
                    .collect(),
            ),
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

    fn fold_network_policy_name(
        &mut self,
        name: <Raw as AstInfo>::NetworkPolicyName,
    ) -> <Aug as AstInfo>::NetworkPolicyName {
        match self.catalog.resolve_network_policy(&name.to_string()) {
            Ok(policy) => ResolvedNetworkPolicyName {
                id: policy.id(),
                name: policy.name().to_string(),
            },
            Err(e) => {
                if self.status.is_ok() {
                    self.status = Err(e.into());
                }
                // garbage value that will be ignored since there's an error.
                ResolvedNetworkPolicyName {
                    id: NetworkPolicyId::User(0),
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
                            cluster_id: ClusterId::system(0).expect("0 is a valid ID"),
                            replica_id: ReplicaId::System(0),
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
            UnresolvedObjectName::NetworkPolicy(name) => ResolvedObjectName::NetworkPolicy(
                self.fold_network_policy_name(RawNetworkPolicyName::Unresolved(name)),
            ),
        }
    }

    fn fold_function(
        &mut self,
        node: mz_sql_parser::ast::Function<Raw>,
    ) -> mz_sql_parser::ast::Function<Aug> {
        // Functions implemented as SQL statements can have very deeply nested
        // and recursive structures, so need the ability to grow the stack.
        mz_ore::stack::maybe_grow(|| {
            mz_sql_parser::ast::Function {
                name: self.resolve_item_name(
                    node.name,
                    // When resolving a function name, only function items should be
                    // considered.
                    ItemResolutionConfig {
                        functions: true,
                        types: false,
                        relations: false,
                    },
                ),
                args: self.fold_function_args(node.args),
                filter: node.filter.map(|expr| Box::new(self.fold_expr(*expr))),
                over: node.over.map(|over| self.fold_window_spec(over)),
                distinct: node.distinct,
            }
        })
    }

    fn fold_table_factor(
        &mut self,
        node: mz_sql_parser::ast::TableFactor<Raw>,
    ) -> mz_sql_parser::ast::TableFactor<Aug> {
        use mz_sql_parser::ast::TableFactor::*;
        match node {
            Table { name, alias } => Table {
                name: self.fold_item_name(name),
                alias: alias.map(|alias| self.fold_table_alias(alias)),
            },
            Function {
                function,
                alias,
                with_ordinality,
            } => {
                match &function.name {
                    RawItemName::Name(name) => {
                        if *name == UnresolvedItemName::unqualified(ident!("values"))
                            && self.status.is_ok()
                        {
                            self.status = Err(PlanError::FromValueRequiresParen);
                        }
                    }
                    _ => {}
                }

                Function {
                    function: self.fold_function(function),
                    alias: alias.map(|alias| self.fold_table_alias(alias)),
                    with_ordinality,
                }
            }
            RowsFrom {
                functions,
                alias,
                with_ordinality,
            } => RowsFrom {
                functions: functions
                    .into_iter()
                    .map(|f| self.fold_function(f))
                    .collect(),
                alias: alias.map(|alias| self.fold_table_alias(alias)),
                with_ordinality,
            },
            Derived {
                lateral,
                subquery,
                alias,
            } => Derived {
                lateral,
                subquery: Box::new(self.fold_query(*subquery)),
                alias: alias.map(|alias| self.fold_table_alias(alias)),
            },
            NestedJoin { join, alias } => NestedJoin {
                join: Box::new(self.fold_table_with_joins(*join)),
                alias: alias.map(|alias| self.fold_table_alias(alias)),
            },
        }
    }

    fn fold_grant_target_specification(
        &mut self,
        node: GrantTargetSpecification<Raw>,
    ) -> GrantTargetSpecification<Aug> {
        match node {
            GrantTargetSpecification::Object {
                object_type: ObjectType::Type,
                object_spec_inner: GrantTargetSpecificationInner::Objects { names },
            } => GrantTargetSpecification::Object {
                object_type: ObjectType::Type,
                object_spec_inner: GrantTargetSpecificationInner::Objects {
                    names: names
                        .into_iter()
                        .map(|name| match name {
                            UnresolvedObjectName::Item(name) => {
                                ResolvedObjectName::Item(self.resolve_item_name_name(
                                    name,
                                    // `{GRANT|REVOKE} ... ON TYPE ...` can only
                                    // refer to type names.
                                    ItemResolutionConfig {
                                        functions: false,
                                        types: true,
                                        relations: false,
                                    },
                                ))
                            }
                            _ => self.fold_object_name(name),
                        })
                        .collect(),
                },
            },
            _ => mz_sql_parser::ast::fold::fold_grant_target_specification(self, node),
        }
    }

    fn fold_doc_on_identifier(&mut self, node: DocOnIdentifier<Raw>) -> DocOnIdentifier<Aug> {
        match node {
            DocOnIdentifier::Column(name) => DocOnIdentifier::Column(self.fold_column_name(name)),
            DocOnIdentifier::Type(name) => DocOnIdentifier::Type(self.resolve_item_name(
                name,
                // In `DOC ON TYPE ...`, the type can refer to either a type or
                // a relation.
                //
                // It's possible this will get simpler once database-issues#7142 is fixed. See
                // the comment on `ItemResolutionConfig` for details.
                ItemResolutionConfig {
                    functions: false,
                    types: true,
                    relations: true,
                },
            )),
        }
    }

    fn fold_expr(&mut self, node: Expr<Raw>) -> Expr<Aug> {
        // Exprs can be recursive, so need the ability to grow the stack.
        mz_ore::stack::maybe_grow(|| mz_sql_parser::ast::fold::fold_expr(self, node))
    }
}

/// Resolves names in an AST node using the provided catalog.
#[mz_ore::instrument(target = "compiler", level = "trace", name = "ast_resolve_names")]
pub fn resolve<N>(
    catalog: &dyn SessionCatalog,
    node: N,
) -> Result<(N::Folded, ResolvedIds), PlanError>
where
    N: FoldNode<Raw, Aug>,
{
    let mut resolver = NameResolver::new(catalog);
    let result = node.fold(&mut resolver);
    resolver.status?;
    Ok((result, ResolvedIds::new(resolver.ids)))
}

/// A set of items and their corresponding collections resolved by name resolution.
///
/// This is a newtype of a [`BTreeMap`] that is provided to make it harder to confuse a set of
/// resolved IDs with other collections of [`CatalogItemId`].
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolvedIds {
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    entries: BTreeMap<CatalogItemId, BTreeSet<GlobalId>>,
}

impl ResolvedIds {
    fn new(entries: BTreeMap<CatalogItemId, BTreeSet<GlobalId>>) -> Self {
        ResolvedIds { entries }
    }

    /// Returns an emptry [`ResolvedIds`].
    pub fn empty() -> Self {
        ResolvedIds {
            entries: BTreeMap::new(),
        }
    }

    /// Returns if the set of IDs is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns all of the [`GlobalId`]s in this set.
    pub fn collections(&self) -> impl Iterator<Item = &GlobalId> {
        self.entries.values().flat_map(|gids| gids.into_iter())
    }

    /// Returns all of the [`CatalogItemId`]s in this set.
    pub fn items(&self) -> impl Iterator<Item = &CatalogItemId> {
        self.entries.keys()
    }

    /// Returns if this set of IDs contains the provided [`CatalogItemId`].
    pub fn contains_item(&self, item: &CatalogItemId) -> bool {
        self.entries.contains_key(item)
    }

    pub fn add_item(&mut self, item: CatalogItemId) {
        self.entries.insert(item, BTreeSet::new());
    }

    pub fn remove_item(&mut self, item: &CatalogItemId) {
        self.entries.remove(item);
    }

    /// Create a new [`ResolvedIds`] that contains the elements from `self`
    /// where `predicate` returns `true`.
    pub fn retain_items<F>(&self, predicate: F) -> Self
    where
        F: Fn(&CatalogItemId) -> bool,
    {
        let mut new_ids = self.clone();
        new_ids
            .entries
            .retain(|item_id, _global_ids| predicate(item_id));
        new_ids
    }
}

impl FromIterator<(CatalogItemId, GlobalId)> for ResolvedIds {
    fn from_iter<T: IntoIterator<Item = (CatalogItemId, GlobalId)>>(iter: T) -> Self {
        let mut ids = ResolvedIds::empty();
        ids.extend(iter);
        ids
    }
}

impl Extend<(CatalogItemId, GlobalId)> for ResolvedIds {
    fn extend<T: IntoIterator<Item = (CatalogItemId, GlobalId)>>(&mut self, iter: T) {
        for (item_id, global_id) in iter {
            self.entries.entry(item_id).or_default().insert(global_id);
        }
    }
}

/// A set of IDs references by the `HirRelationExpr` of an object.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DependencyIds(pub BTreeSet<CatalogItemId>);

impl FromIterator<CatalogItemId> for DependencyIds {
    fn from_iter<T: IntoIterator<Item = CatalogItemId>>(iter: T) -> Self {
        DependencyIds(iter.into_iter().collect())
    }
}

#[derive(Debug)]
pub struct DependencyVisitor<'a> {
    catalog: &'a dyn SessionCatalog,
    ids: BTreeMap<CatalogItemId, BTreeSet<GlobalId>>,
}

impl<'a> DependencyVisitor<'a> {
    pub fn new(catalog: &'a dyn SessionCatalog) -> Self {
        DependencyVisitor {
            catalog,
            ids: Default::default(),
        }
    }
}

impl<'a, 'ast> Visit<'ast, Aug> for DependencyVisitor<'a> {
    fn visit_item_name(&mut self, item_name: &'ast <Aug as AstInfo>::ItemName) {
        if let ResolvedItemName::Item { id, version, .. } = item_name {
            let global_ids = self.ids.entry(*id).or_default();
            if let Some(item) = self.catalog.try_get_item(id) {
                global_ids.insert(item.at_version(*version).global_id());
            }
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
                self.ids.entry(*id).or_default();
            }
            ResolvedDataType::Error => {}
        }
    }
}

pub fn visit_dependencies<'ast, N>(catalog: &dyn SessionCatalog, node: &'ast N) -> ResolvedIds
where
    N: VisitNode<'ast, Aug> + 'ast,
{
    let mut visitor = DependencyVisitor::new(catalog);
    node.visit(&mut visitor);
    ResolvedIds::new(visitor.ids)
}

#[derive(Debug)]
pub struct ItemDependencyModifier<'a> {
    pub modified: bool,
    pub id_map: &'a BTreeMap<CatalogItemId, CatalogItemId>,
}

impl<'ast, 'a> VisitMut<'ast, Raw> for ItemDependencyModifier<'a> {
    fn visit_item_name_mut(&mut self, item_name: &mut RawItemName) {
        if let RawItemName::Id(id, _, _) = item_name {
            let parsed_id = id.parse::<CatalogItemId>().unwrap();
            if let Some(new_id) = self.id_map.get(&parsed_id) {
                *id = new_id.to_string();
                self.modified = true;
            }
        }
    }
}

/// Updates any references in the provided AST node that are keys in `id_map`.
/// If an id is found it will be updated to the value of the key in `id_map`.
/// This assumes the names of the reference(s) are unmodified (e.g. each pair of
/// ids refer to an item of the same name, whose id has changed).
pub fn modify_dependency_item_ids<'ast, N>(
    node: &'ast mut N,
    id_map: &BTreeMap<CatalogItemId, CatalogItemId>,
) -> bool
where
    N: VisitMutNode<'ast, Raw>,
{
    let mut modifier = ItemDependencyModifier {
        id_map,
        modified: false,
    };
    node.visit_mut(&mut modifier);

    modifier.modified
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

/// Returns the [`CatalogItemId`] dependencies the provided `node` has.
///
/// _DOES NOT_ resolve names, simply does a recursive walk through an object to
/// find all of the IDs.
pub fn dependencies<'ast, N>(node: &'ast N) -> Result<BTreeSet<CatalogItemId>, anyhow::Error>
where
    N: VisitNode<'ast, Raw>,
{
    let mut visitor = IdDependencVisitor::default();
    node.visit(&mut visitor);
    match visitor.error {
        Some(error) => Err(error),
        None => Ok(visitor.ids),
    }
}

#[derive(Debug, Default)]
struct IdDependencVisitor {
    ids: BTreeSet<CatalogItemId>,
    error: Option<anyhow::Error>,
}

impl<'ast> Visit<'ast, Raw> for IdDependencVisitor {
    fn visit_item_name(&mut self, node: &'ast <Raw as AstInfo>::ItemName) {
        // Bail early if we're already in an error state.
        if self.error.is_some() {
            return;
        }

        match node {
            // Nothing to do! We don't lookup names.
            RawItemName::Name(_) => (),
            RawItemName::Id(id, _name, _version) => match id.parse::<CatalogItemId>() {
                Ok(id) => {
                    self.ids.insert(id);
                }
                Err(e) => {
                    self.error = Some(e);
                }
            },
        }
    }
}
