// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structured name types for SQL objects.

use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use mz_dataflow_types::client::ComputeInstanceId;
use mz_expr::{GlobalId, Id, LocalId};
use mz_ore::str::StrExt;

use crate::ast::display::{AstDisplay, AstFormatter};
use crate::ast::fold::Fold;
use crate::ast::visit_mut::VisitMut;
use crate::ast::{
    self, AstInfo, Cte, Expr, Ident, Query, Raw, RawIdent, RawName, Statement, UnresolvedDataType,
    UnresolvedObjectName,
};
use crate::catalog::{CatalogItemType, CatalogTypeDetails, SessionCatalog};
use crate::normalize;
use crate::plan::{PlanError, QueryContext, StatementContext};

/// A fully-qualified name of an item in the catalog.
///
/// Catalog names compare case sensitively. Use
/// [`normalize::unresolved_object_name`] to
/// perform proper case folding if converting an [`UnresolvedObjectName`] to a
/// `FullName`.
///
/// [`normalize::unresolved_object_name`]: crate::normalize::unresolved_object_name
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FullName {
    /// The database name.
    pub database: DatabaseSpecifier,
    /// The schema name.
    pub schema: String,
    /// The item name.
    pub item: String,
}

impl fmt::Display for FullName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let DatabaseSpecifier::Name(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        write!(f, "{}.{}", self.schema, self.item)
    }
}

/// A name of a database in a [`FullName`].
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DatabaseSpecifier {
    /// The "ambient" database, which is always present and is not named
    /// explicitly, but by omission.
    Ambient,
    /// A normal database with a name.
    Name(String),
}

impl fmt::Display for DatabaseSpecifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DatabaseSpecifier::Ambient => f.write_str("<none>"),
            DatabaseSpecifier::Name(name) => f.write_str(name),
        }
    }
}

impl From<Option<String>> for DatabaseSpecifier {
    fn from(s: Option<String>) -> DatabaseSpecifier {
        match s {
            None => DatabaseSpecifier::Ambient,
            Some(name) => DatabaseSpecifier::Name(name),
        }
    }
}

/// The fully-qualified name of a schema in the catalog.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SchemaName {
    pub database: DatabaseSpecifier,
    pub schema: String,
}

impl fmt::Display for SchemaName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.database {
            DatabaseSpecifier::Ambient => f.write_str(&self.schema),
            DatabaseSpecifier::Name(name) => write!(f, "{}.{}", name, self.schema),
        }
    }
}

impl From<FullName> for UnresolvedObjectName {
    fn from(full_name: FullName) -> UnresolvedObjectName {
        let mut name_parts = Vec::new();
        if let DatabaseSpecifier::Name(database) = full_name.database {
            name_parts.push(Ident::new(database));
        }
        name_parts.push(Ident::new(full_name.schema));
        name_parts.push(Ident::new(full_name.item));
        UnresolvedObjectName(name_parts)
    }
}

/// An optionally-qualified name of an item in the catalog.
///
/// This is like a [`FullName`], but either the database or schema name may be
/// omitted.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartialName {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub item: String,
}

impl PartialName {
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

impl fmt::Display for PartialName {
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

impl From<FullName> for PartialName {
    fn from(n: FullName) -> PartialName {
        PartialName {
            database: match n.database {
                DatabaseSpecifier::Ambient => None,
                DatabaseSpecifier::Name(name) => Some(name),
            },
            schema: Some(n.schema),
            item: n.item,
        }
    }
}

// Aug is the type variable assigned to an AST that has already been
// name-resolved. An AST in this state has global IDs populated next to table
// names, and local IDs assigned to CTE definitions and references.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Default)]
pub struct Aug;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ResolvedObjectName {
    pub id: Id,
    pub raw_name: PartialName,
    // Whether this object, when printed out, should use [id AS name] syntax. We
    // want this for things like tables and sources, but not for things like
    // types.
    pub print_id: bool,
}

impl AstDisplay for ResolvedObjectName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        if self.print_id {
            f.write_str(format!("[{} AS ", self.id));
        }
        let n = self.raw_name();
        if let Some(database) = n.database {
            f.write_node(&Ident::new(database));
            f.write_str(".");
        }
        if let Some(schema) = n.schema {
            f.write_node(&Ident::new(schema));
            f.write_str(".");
        }
        f.write_node(&Ident::new(n.item));
        if self.print_id {
            f.write_str("]");
        }
    }
}

impl std::fmt::Display for ResolvedObjectName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_ast_string().as_str())
    }
}

impl ResolvedObjectName {
    pub fn raw_name(&self) -> PartialName {
        self.raw_name.clone()
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
        name: PartialName,
        modifiers: Vec<i64>,
        print_id: bool,
    },
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
                name,
                modifiers,
                print_id,
            } => {
                if *print_id {
                    f.write_str(format!("[{} AS ", id));
                }
                if let Some(database) = &name.database {
                    f.write_node(&Ident::new(database));
                    f.write_str(".");
                }
                if let Some(schema) = &name.schema {
                    f.write_node(&Ident::new(schema));
                    f.write_str(".");
                }
                f.write_node(&Ident::new(&name.item));
                if *print_id {
                    f.write_str("]");
                }
                if modifiers.len() > 0 {
                    f.write_str("(");
                    f.write_node(&ast::display::comma_separated(modifiers));
                    f.write_str(")");
                }
            }
        }
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
        };
        ids
    }
}

impl AstInfo for Aug {
    type ObjectName = ResolvedObjectName;
    type ClusterName = ResolvedClusterName;
    type DataType = ResolvedDataType;
    type Id = Id;
}

#[derive(Debug)]
pub struct NameResolver<'a> {
    catalog: &'a dyn SessionCatalog,
    ctes: HashMap<String, LocalId>,
    status: Result<(), PlanError>,
    ids: HashSet<GlobalId>,
}

impl<'a> NameResolver<'a> {
    pub fn new(catalog: &'a dyn SessionCatalog) -> NameResolver {
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
            UnresolvedDataType::Array(elem_type) => {
                let name = elem_type.to_string();
                match self.fold_data_type_internal(*elem_type)? {
                    ResolvedDataType::AnonymousList(_) | ResolvedDataType::AnonymousMap { .. } => {
                        sql_bail!("type \"{}[]\" does not exist", name)
                    }
                    ResolvedDataType::Named { id, modifiers, .. } => {
                        let element_item = self.catalog.get_item_by_id(&id);
                        let array_item = match element_item.type_details() {
                            Some(CatalogTypeDetails {
                                array_id: Some(array_id),
                                ..
                            }) => self.catalog.get_item_by_id(&array_id),
                            Some(_) => sql_bail!("type \"{}[]\" does not exist", name),
                            None => sql_bail!(
                                "{} does not refer to a type",
                                element_item.name().to_string().quoted()
                            ),
                        };
                        Ok(ResolvedDataType::Named {
                            id: array_item.id(),
                            name: array_item.name().clone().into(),
                            modifiers,
                            print_id: true,
                        })
                    }
                }
            }
            UnresolvedDataType::List(elem_type) => {
                let elem_type = self.fold_data_type_internal(*elem_type)?;
                Ok(ResolvedDataType::AnonymousList(Box::new(elem_type)))
            }
            UnresolvedDataType::Map {
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
            UnresolvedDataType::Other { name, typ_mod } => {
                let (name, item) = match name {
                    RawName::Name(name) => {
                        let name = normalize::unresolved_object_name(name).unwrap();
                        let item = self.catalog.resolve_item(&name)?;
                        (item.name().clone().into(), item)
                    }
                    RawName::Id(id, name) => {
                        let name = normalize::unresolved_object_name(name).unwrap();
                        let gid: GlobalId = id.parse()?;
                        let item = self.catalog.get_item_by_id(&gid);
                        (name, item)
                    }
                };
                self.ids.insert(item.id());
                Ok(ResolvedDataType::Named {
                    id: item.id(),
                    name,
                    modifiers: typ_mod,
                    print_id: true,
                })
            }
        }
    }
}

impl<'a> Fold<Raw, Aug> for NameResolver<'a> {
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
                id: Id::Local(id),
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

    fn fold_id(&mut self, _id: <Raw as AstInfo>::Id) -> <Aug as AstInfo>::Id {
        panic!("this should have been handled when walking the CTE");
    }

    fn fold_object_name(
        &mut self,
        object_name: <Raw as AstInfo>::ObjectName,
    ) -> <Aug as AstInfo>::ObjectName {
        match object_name {
            RawName::Name(raw_name) => {
                // Check if unqualified name refers to a CTE.
                if raw_name.0.len() == 1 {
                    let norm_name = normalize::ident(raw_name.0[0].clone());
                    if let Some(id) = self.ctes.get(&norm_name) {
                        return ResolvedObjectName {
                            id: Id::Local(*id),
                            raw_name: normalize::unresolved_object_name(raw_name).unwrap(),
                            print_id: false,
                        };
                    }
                }

                let name = normalize::unresolved_object_name(raw_name).unwrap();
                match self.catalog.resolve_item(&name) {
                    Ok(item) => {
                        self.ids.insert(item.id());
                        let print_id = !matches!(
                            item.item_type(),
                            CatalogItemType::Func | CatalogItemType::Type
                        );
                        ResolvedObjectName {
                            id: Id::Global(item.id()),
                            raw_name: item.name().clone().into(),
                            print_id,
                        }
                    }
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e.into());
                        }
                        ResolvedObjectName {
                            id: Id::Local(LocalId::new(0)),
                            raw_name: name,
                            print_id: false,
                        }
                    }
                }
            }
            RawName::Id(id, raw_name) => {
                let gid: GlobalId = match id.parse() {
                    Ok(id) => id,
                    Err(e) => {
                        self.status = Err(e.into());
                        GlobalId::User(0)
                    }
                };
                if self.status.is_ok() && self.catalog.try_get_item_by_id(&gid).is_none() {
                    self.status = Err(PlanError::Unstructured(format!("invalid id {}", &gid)));
                }
                self.ids.insert(gid.clone());
                ResolvedObjectName {
                    id: Id::Global(gid),
                    raw_name: normalize::unresolved_object_name(raw_name).unwrap(),
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
                ResolvedDataType::Named {
                    id: GlobalId::User(0),
                    name: PartialName {
                        database: None,
                        schema: None,
                        item: "ignored".into(),
                    },
                    modifiers: vec![],
                    print_id: false,
                }
            }
        }
    }

    fn fold_cluster_name(
        &mut self,
        cluster_name: <Raw as AstInfo>::ClusterName,
    ) -> <Aug as AstInfo>::ClusterName {
        match cluster_name {
            RawIdent::Unresolved(ident) => {
                match self.catalog.resolve_compute_instance(Some(ident.as_str())) {
                    Ok(cluster) => ResolvedClusterName(cluster.id()),
                    Err(e) => {
                        self.status = Err(e.into());
                        ResolvedClusterName(0)
                    }
                }
            }
            RawIdent::Resolved(ident) => match ident.parse() {
                Ok(id) => ResolvedClusterName(id),
                Err(e) => {
                    self.status = Err(e.into());
                    ResolvedClusterName(0)
                }
            },
        }
    }
}

pub fn resolve_names_stmt(
    scx: &mut StatementContext,
    stmt: Statement<Raw>,
) -> Result<(Statement<Aug>, HashSet<GlobalId>), PlanError> {
    let mut n = NameResolver::new(scx.catalog);
    let result = n.fold_statement(stmt);
    n.status?;
    Ok((result, n.ids))
}

pub fn resolve_names_stmt_show(
    scx: &StatementContext,
    stmt: Statement<Raw>,
) -> Result<Statement<Aug>, PlanError> {
    let mut n = NameResolver::new(scx.catalog);
    let result = n.fold_statement(stmt);
    n.status?;
    Ok(result)
}

// Attaches additional information to a `Raw` AST, resulting in an `Aug` AST, by
// resolving names and (aspirationally) performing semantic analysis such as
// type-checking.
pub fn resolve_names(qcx: &mut QueryContext, query: Query<Raw>) -> Result<Query<Aug>, PlanError> {
    let mut n = NameResolver::new(qcx.scx.catalog);
    let result = n.fold_query(query);
    n.status?;
    Ok(result)
}

pub fn resolve_names_expr(qcx: &mut QueryContext, expr: Expr<Raw>) -> Result<Expr<Aug>, PlanError> {
    let mut n = NameResolver::new(qcx.scx.catalog);
    let result = n.fold_expr(expr);
    n.status?;
    Ok(result)
}

pub fn resolve_names_data_type(
    scx: &StatementContext,
    data_type: UnresolvedDataType,
) -> Result<(ResolvedDataType, HashSet<GlobalId>), PlanError> {
    let mut n = NameResolver::new(scx.catalog);
    let result = n.fold_data_type(data_type);
    n.status?;
    Ok((result, n.ids))
}

pub fn resolve_names_cluster(
    scx: &StatementContext,
    cluster_name: RawIdent,
) -> Result<ResolvedClusterName, PlanError> {
    let mut n = NameResolver::new(scx.catalog);
    let result = n.fold_cluster_name(cluster_name);
    n.status?;
    Ok(result)
}

pub fn resolve_object_name(
    scx: &StatementContext,
    object_name: <Raw as AstInfo>::ObjectName,
) -> Result<<Aug as AstInfo>::ObjectName, PlanError> {
    let mut n = NameResolver::new(scx.catalog);
    let result = n.fold_object_name(object_name);
    n.status?;
    Ok(result)
}

/// A general implementation for name resolution on AST elements.
///
/// This implementation is appropriate Whenever:
/// - You don't need to export the name resolution outside the `sql` crate and
///   the extra typing isn't too onerous.
/// - Discovered dependencies should extend `qcx.ids`.
pub fn resolve_names_extend_qcx_ids<F, T>(qcx: &mut QueryContext, f: F) -> Result<T, PlanError>
where
    F: FnOnce(&mut NameResolver) -> T,
{
    let mut n = NameResolver::new(qcx.scx.catalog);
    let result = f(&mut n);
    n.status?;
    Ok(result)
}

// Used when displaying a view's source for human creation. If the name
// specified is the same as the name in the catalog, we don't use the ID format.
#[derive(Debug)]
pub struct NameSimplifier<'a> {
    pub catalog: &'a dyn SessionCatalog,
}

impl<'ast, 'a> VisitMut<'ast, Aug> for NameSimplifier<'a> {
    fn visit_object_name_mut(&mut self, name: &mut ResolvedObjectName) {
        if let Id::Global(id) = name.id {
            let item = self.catalog.get_item_by_id(&id);
            if PartialName::from(item.name().clone()) == name.raw_name() {
                name.print_id = false;
            }
        }
    }

    fn visit_data_type_mut(&mut self, name: &mut ResolvedDataType) {
        if let ResolvedDataType::Named {
            id, name, print_id, ..
        } = name
        {
            let item = self.catalog.get_item_by_id(id);
            if PartialName::from(item.name().clone()) == *name {
                *print_id = false;
            }
        }
    }
}
