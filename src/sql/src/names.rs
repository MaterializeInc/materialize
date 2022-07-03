// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structured name types for SQL objects.

use anyhow::Error;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use mz_compute_client::controller::ComputeInstanceId;
use mz_expr::LocalId;
use mz_ore::str::StrExt;
use mz_repr::GlobalId;

use crate::ast::display::{AstDisplay, AstFormatter};
use crate::ast::fold::{Fold, FoldNode};
use crate::ast::visit_mut::VisitMut;
use crate::ast::{
    self, AstInfo, Cte, Ident, Query, Raw, RawClusterName, RawDataType, RawObjectName, Statement,
    UnresolvedObjectName,
};
use crate::catalog::{CatalogItemType, CatalogTypeDetails, SessionCatalog};
use crate::normalize;
use crate::plan::PlanError;

/// A fully-qualified human readable name of an item in the catalog.
///
/// Catalog names compare case sensitively. Use
/// [`normalize::unresolved_object_name`] to
/// perform proper case folding if converting an [`UnresolvedObjectName`] to a
/// `FullObjectName`.
///
/// [`normalize::unresolved_object_name`]: crate::normalize::unresolved_object_name
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FullObjectName {
    /// The database name.
    pub database: RawDatabaseSpecifier,
    /// The schema name.
    pub schema: String,
    /// The item name.
    pub item: String,
}

impl fmt::Display for FullObjectName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let RawDatabaseSpecifier::Name(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        write!(f, "{}.{}", self.schema, self.item)
    }
}

impl From<FullObjectName> for UnresolvedObjectName {
    fn from(full_name: FullObjectName) -> UnresolvedObjectName {
        let mut name_parts = Vec::new();
        if let RawDatabaseSpecifier::Name(database) = full_name.database {
            name_parts.push(Ident::new(database));
        }
        name_parts.push(Ident::new(full_name.schema));
        name_parts.push(Ident::new(full_name.item));
        UnresolvedObjectName(name_parts)
    }
}

/// A fully-qualified non-human readable name of an item in the catalog using IDs for the database
/// and schema.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct QualifiedObjectName {
    pub qualifiers: ObjectQualifiers,
    pub item: String,
}

impl fmt::Display for QualifiedObjectName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let ResolvedDatabaseSpecifier::Id(id) = &self.qualifiers.database_spec {
            write!(f, "{}.", id)?;
        }
        write!(f, "{}.{}", self.qualifiers.schema_spec, self.item)
    }
}

/// An optionally-qualified human-readable name of an item in the catalog.
///
/// This is like a [`FullObjectName`], but either the database or schema name may be
/// omitted.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartialObjectName {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub item: String,
}

impl PartialObjectName {
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

impl fmt::Display for PartialObjectName {
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

impl From<FullObjectName> for PartialObjectName {
    fn from(n: FullObjectName) -> PartialObjectName {
        let database = match n.database {
            RawDatabaseSpecifier::Ambient => None,
            RawDatabaseSpecifier::Name(name) => Some(name),
        };
        PartialObjectName {
            database,
            schema: Some(n.schema),
            item: n.item,
        }
    }
}

impl From<String> for PartialObjectName {
    fn from(item: String) -> Self {
        PartialObjectName {
            database: None,
            schema: None,
            item,
        }
    }
}

/// A fully-qualified human readable name of a schema in the catalog.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
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

impl From<u64> for ResolvedDatabaseSpecifier {
    fn from(id: u64) -> Self {
        Self::Id(DatabaseId(id))
    }
}

/*
 * TODO(jkosh44) It's possible that in order to fix
 * https://github.com/MaterializeInc/materialize/issues/8805 we will need to assign temporary
 * schemas unique Ids. If/when that happens we can remove this enum and refer to all schemas by
 * their Id.
 */
/// An id of a schema.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum SchemaSpecifier {
    /// A temporary schema
    Temporary,
    /// A normal database with a name.
    Id(SchemaId),
}

impl SchemaSpecifier {
    const TEMPORARY_SCHEMA_ID: u64 = 0;
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

impl From<u64> for SchemaSpecifier {
    fn from(id: u64) -> Self {
        if id == Self::TEMPORARY_SCHEMA_ID {
            Self::Temporary
        } else {
            Self::Id(SchemaId(id))
        }
    }
}

impl From<&SchemaSpecifier> for SchemaId {
    fn from(schema_spec: &SchemaSpecifier) -> Self {
        match schema_spec {
            SchemaSpecifier::Temporary => SchemaId(SchemaSpecifier::TEMPORARY_SCHEMA_ID),
            SchemaSpecifier::Id(id) => id.clone(),
        }
    }
}

impl From<SchemaSpecifier> for SchemaId {
    fn from(schema_spec: SchemaSpecifier) -> Self {
        match schema_spec {
            SchemaSpecifier::Temporary => SchemaId(SchemaSpecifier::TEMPORARY_SCHEMA_ID),
            SchemaSpecifier::Id(id) => id,
        }
    }
}

impl From<&SchemaSpecifier> for u64 {
    fn from(schema_spec: &SchemaSpecifier) -> Self {
        SchemaId::from(schema_spec).0
    }
}

impl From<SchemaSpecifier> for u64 {
    fn from(schema_spec: SchemaSpecifier) -> Self {
        SchemaId::from(schema_spec).0
    }
}

// Aug is the type variable assigned to an AST that has already been
// name-resolved. An AST in this state has global IDs populated next to table
// names, and local IDs assigned to CTE definitions and references.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Default)]
pub struct Aug;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ObjectQualifiers {
    pub database_spec: ResolvedDatabaseSpecifier,
    pub schema_spec: SchemaSpecifier,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ResolvedObjectName {
    Object {
        id: GlobalId,
        qualifiers: ObjectQualifiers,
        full_name: FullObjectName,
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

impl ResolvedObjectName {
    pub fn full_name_str(&self) -> String {
        match self {
            ResolvedObjectName::Object { full_name, .. } => full_name.to_string(),
            ResolvedObjectName::Cte { name, .. } => name.clone(),
            ResolvedObjectName::Error => "error in name resolution".to_string(),
        }
    }
}

impl AstDisplay for ResolvedObjectName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ResolvedObjectName::Object {
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
            ResolvedObjectName::Cte { name, .. } => f.write_node(&Ident::new(name)),
            ResolvedObjectName::Error => {}
        }
    }
}

impl std::fmt::Display for ResolvedObjectName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_ast_string().as_str())
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ResolvedSchemaName {
    Schema {
        database_spec: ResolvedDatabaseSpecifier,
        schema_spec: SchemaSpecifier,
        full_name: FullSchemaName,
    },
    Error,
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

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ResolvedDatabaseName {
    Database { id: DatabaseId, name: String },
    Error,
}

impl AstDisplay for ResolvedDatabaseName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        match self {
            ResolvedDatabaseName::Database { name, .. } => f.write_node(&Ident::new(name)),
            ResolvedDatabaseName::Error => {}
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ResolvedClusterName(pub ComputeInstanceId);

impl AstDisplay for ResolvedClusterName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        f.write_str(format!("[{}]", self.0))
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ResolvedDataType {
    AnonymousList(Box<ResolvedDataType>),
    AnonymousMap {
        key_type: Box<ResolvedDataType>,
        value_type: Box<ResolvedDataType>,
    },
    Named {
        id: GlobalId,
        qualifiers: ObjectQualifiers,
        full_name: FullObjectName,
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

impl ResolvedDataType {
    pub(crate) fn get_ids(&self) -> Vec<GlobalId> {
        let mut ids = Vec::new();
        match self {
            ResolvedDataType::AnonymousList(typ) => {
                ids.extend(typ.get_ids());
            }
            ResolvedDataType::AnonymousMap {
                key_type,
                value_type,
            } => {
                ids.extend(key_type.get_ids());
                ids.extend(value_type.get_ids());
            }
            ResolvedDataType::Named { id, .. } => {
                ids.push(id.clone());
            }
            ResolvedDataType::Error => {}
        };
        ids
    }
}

impl AstInfo for Aug {
    type NestedStatement = Statement<Raw>;
    type ObjectName = ResolvedObjectName;
    type SchemaName = ResolvedSchemaName;
    type DatabaseName = ResolvedDatabaseName;
    type ClusterName = ResolvedClusterName;
    type DataType = ResolvedDataType;
    type CteId = LocalId;
}

/// The identifier for a schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SchemaId(pub u64);

impl SchemaId {
    /// Constructs a new schema identifier. It is the caller's responsibility
    /// to provide a unique `id`.
    pub fn new(id: u64) -> Self {
        SchemaId(id)
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for SchemaId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let val: u64 = s.parse()?;
        Ok(SchemaId(val))
    }
}

/// The identifier for a database.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct DatabaseId(pub u64);

impl DatabaseId {
    /// Constructs a new database identifier. It is the caller's responsibility
    /// to provide a unique `id`.
    pub fn new(id: u64) -> Self {
        DatabaseId(id)
    }
}

impl fmt::Display for DatabaseId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for DatabaseId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let val: u64 = s.parse()?;
        Ok(DatabaseId(val))
    }
}

#[derive(Debug)]
pub struct NameResolver<'a> {
    catalog: &'a dyn SessionCatalog,
    ctes: HashMap<String, LocalId>,
    status: Result<(), PlanError>,
    ids: HashSet<GlobalId>,
}

impl<'a> NameResolver<'a> {
    fn new(catalog: &'a dyn SessionCatalog) -> NameResolver {
        NameResolver {
            catalog,
            ctes: HashMap::new(),
            status: Ok(()),
            ids: HashSet::new(),
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
                            }) => self.catalog.get_item(&array_id),
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
                    RawObjectName::Name(name) => {
                        let name = normalize::unresolved_object_name(name)?;
                        let item = self.catalog.resolve_item(&name)?;
                        let full_name = self.catalog.resolve_full_name(item.name());
                        (full_name, item)
                    }
                    RawObjectName::Id(id, name) => {
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
        let mut old_cte_values = Vec::new();
        // A single WITH block cannot use the same name multiple times.
        let mut used_names = HashSet::new();
        let mut ctes = Vec::new();
        for cte in q.ctes {
            let cte_name = normalize::ident(cte.alias.name.clone());

            if used_names.contains(&cte_name) {
                self.status = Err(PlanError::Unstructured(format!(
                    "WITH query name \"{}\" specified more than once",
                    cte_name
                )));
            }
            used_names.insert(cte_name.clone());

            let id = LocalId::new(self.ctes.len() as u64);
            ctes.push(Cte {
                alias: cte.alias,
                id,
                query: self.fold_query(cte.query),
            });
            let old_val = self.ctes.insert(cte_name.clone(), id);
            old_cte_values.push((cte_name, old_val));
        }
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
        for (name, value) in old_cte_values.iter() {
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

    fn fold_object_name(
        &mut self,
        object_name: <Raw as AstInfo>::ObjectName,
    ) -> <Aug as AstInfo>::ObjectName {
        match object_name {
            RawObjectName::Name(raw_name) => {
                let raw_name = match normalize::unresolved_object_name(raw_name) {
                    Ok(raw_name) => raw_name,
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e);
                        }
                        return ResolvedObjectName::Error;
                    }
                };

                // Check if unqualified name refers to a CTE.
                if raw_name.database.is_none() && raw_name.schema.is_none() {
                    let norm_name = normalize::ident(Ident::new(&raw_name.item));
                    if let Some(id) = self.ctes.get(&norm_name) {
                        return ResolvedObjectName::Cte {
                            id: *id,
                            name: norm_name,
                        };
                    }
                }

                match self.catalog.resolve_item(&raw_name) {
                    Ok(item) => {
                        self.ids.insert(item.id());
                        let print_id = !matches!(
                            item.item_type(),
                            CatalogItemType::Func | CatalogItemType::Type
                        );
                        ResolvedObjectName::Object {
                            id: item.id(),
                            qualifiers: item.name().qualifiers.clone(),
                            full_name: self.catalog.resolve_full_name(item.name()),
                            print_id,
                        }
                    }
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e.into());
                        }
                        ResolvedObjectName::Error
                    }
                }
            }
            RawObjectName::Id(id, raw_name) => {
                let gid: GlobalId = match id.parse() {
                    Ok(id) => id,
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e.into());
                        }
                        return ResolvedObjectName::Error;
                    }
                };
                let item = match self.catalog.try_get_item(&gid) {
                    Some(item) => item,
                    None => {
                        if self.status.is_ok() {
                            self.status =
                                Err(PlanError::Unstructured(format!("invalid id {}", &gid)));
                        }
                        return ResolvedObjectName::Error;
                    }
                };

                self.ids.insert(gid.clone());
                let full_name = match normalize::full_name(raw_name) {
                    Ok(full_name) => full_name,
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e.into());
                        }
                        return ResolvedObjectName::Error;
                    }
                };
                ResolvedObjectName::Object {
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
                match self.catalog.resolve_compute_instance(Some(ident.as_str())) {
                    Ok(cluster) => ResolvedClusterName(cluster.id()),
                    Err(e) => {
                        self.status = Err(e.into());
                        ResolvedClusterName(0)
                    }
                }
            }
            RawClusterName::Resolved(ident) => match ident.parse() {
                Ok(id) => ResolvedClusterName(id),
                Err(e) => {
                    self.status = Err(e.into());
                    ResolvedClusterName(0)
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
            Value(v) => Value(self.fold_value(v)),
            Ident(i) => Ident(self.fold_ident(i)),
            DataType(dt) => DataType(self.fold_data_type(dt)),
            Secret(secret) => {
                let object_name = self.fold_object_name(secret);
                match &object_name {
                    ResolvedObjectName::Object { id, .. } => {
                        let item = self.catalog.get_item(&id);
                        if item.item_type() != CatalogItemType::Secret {
                            self.status = Err(PlanError::InvalidSecret(object_name.clone()));
                        }
                    }
                    ResolvedObjectName::Cte { .. } => {
                        self.status = Err(PlanError::InvalidSecret(object_name.clone()));
                    }
                    ResolvedObjectName::Error => {}
                }
                Secret(object_name)
            }
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
) -> Result<(N::Folded, HashSet<GlobalId>), PlanError>
where
    N: FoldNode<Raw, Aug>,
{
    let mut resolver = NameResolver::new(catalog);
    let result = node.fold(&mut resolver);
    resolver.status?;
    Ok((result, resolver.ids))
}

// Used when displaying a view's source for human creation. If the name
// specified is the same as the name in the catalog, we don't use the ID format.
#[derive(Debug)]
pub struct NameSimplifier<'a> {
    pub catalog: &'a dyn SessionCatalog,
}

impl<'ast, 'a> VisitMut<'ast, Aug> for NameSimplifier<'a> {
    fn visit_object_name_mut(&mut self, name: &mut ResolvedObjectName) {
        if let ResolvedObjectName::Object {
            id,
            full_name,
            print_id,
            ..
        } = name
        {
            let item = self.catalog.get_item(&id);
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
