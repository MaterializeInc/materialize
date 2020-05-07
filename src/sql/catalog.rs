// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;
use std::time::SystemTime;

use catalog::{
    Catalog, CatalogEntry, CatalogItem, DatabaseResolver, Index, Schema, Sink, Source, View,
};
use expr::{GlobalId, ScalarExpr};
use repr::RelationDesc;

pub use catalog::names::{DatabaseSpecifier, FullName, PartialName};
pub use catalog::{PlanContext, SchemaType};

pub trait PlanCatalog: fmt::Debug {
    fn creation_time(&self) -> SystemTime;

    fn nonce(&self) -> u64;

    fn databases<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a>;

    fn get(&self, name: &FullName) -> Result<&dyn PlanCatalogEntry, failure::Error>;

    fn get_by_id(&self, id: &GlobalId) -> &dyn PlanCatalogEntry;

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn PlanCatalogEntry> + 'a>;

    fn get_schemas(&self, database_spec: &DatabaseSpecifier) -> Option<&dyn SchemaMap>;

    fn database_resolver<'a>(
        &'a self,
        database_spec: DatabaseSpecifier,
    ) -> Result<Box<dyn PlanDatabaseResolver<'a> + 'a>, failure::Error>;

    fn resolve(
        &self,
        current_database: DatabaseSpecifier,
        search_path: &[&str],
        name: &PartialName,
    ) -> Result<FullName, failure::Error>;

    fn empty_item_map(&self) -> Box<dyn ItemMap>;
}

pub trait PlanCatalogEntry {
    fn name(&self) -> &FullName;

    fn id(&self) -> GlobalId;

    fn desc(&self) -> Result<&RelationDesc, failure::Error>;

    fn create_sql(&self) -> &str;

    fn plan_cx(&self) -> &PlanContext;

    fn index_details(&self) -> Option<(&[ScalarExpr], GlobalId)>;

    fn item_type(&self) -> CatalogItemType;

    fn uses(&self) -> Vec<GlobalId>;

    fn used_by(&self) -> &[GlobalId];
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CatalogItemType {
    Source,
    Sink,
    View,
    Index,
}

impl fmt::Display for CatalogItemType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CatalogItemType::Source => f.write_str("source"),
            CatalogItemType::Sink => f.write_str("sink"),
            CatalogItemType::View => f.write_str("view"),
            CatalogItemType::Index => f.write_str("index"),
        }
    }
}

pub trait PlanDatabaseResolver<'a> {
    fn resolve_schema(&self, schema_name: &str) -> Option<(&'a dyn PlanSchema, SchemaType)>;
}

pub trait PlanSchema {
    fn items(&self) -> &dyn ItemMap;
}

pub trait SchemaMap {
    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a>;
}

pub trait ItemMap {
    fn is_empty(&self) -> bool;
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a String, &'a GlobalId)> + 'a>;
}

impl PlanCatalog for Catalog {
    fn creation_time(&self) -> SystemTime {
        self.creation_time()
    }

    fn nonce(&self) -> u64 {
        self.nonce()
    }

    fn databases<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        Box::new(self.databases())
    }

    fn get(&self, name: &FullName) -> Result<&dyn PlanCatalogEntry, failure::Error> {
        Ok(self.get(name)?)
    }

    fn get_by_id(&self, id: &GlobalId) -> &dyn PlanCatalogEntry {
        self.get_by_id(id)
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn PlanCatalogEntry> + 'a> {
        Box::new(self.iter().map(|e| e as &dyn PlanCatalogEntry))
    }

    fn get_schemas(&self, database_spec: &DatabaseSpecifier) -> Option<&dyn SchemaMap> {
        self.get_schemas(database_spec).map(|m| m as &dyn SchemaMap)
    }

    fn database_resolver<'a>(
        &'a self,
        database_spec: DatabaseSpecifier,
    ) -> Result<Box<dyn PlanDatabaseResolver<'a> + 'a>, failure::Error> {
        Ok(Box::new(self.database_resolver(database_spec)?))
    }

    fn resolve(
        &self,
        current_database: DatabaseSpecifier,
        search_path: &[&str],
        name: &PartialName,
    ) -> Result<FullName, failure::Error> {
        Ok(self.resolve(current_database, search_path, name)?)
    }

    fn empty_item_map(&self) -> Box<dyn ItemMap> {
        Box::new(BTreeMap::new())
    }
}

impl PlanCatalogEntry for CatalogEntry {
    fn name(&self) -> &FullName {
        self.name()
    }

    fn id(&self) -> GlobalId {
        self.id()
    }

    fn desc(&self) -> Result<&RelationDesc, failure::Error> {
        Ok(self.desc()?)
    }

    fn create_sql(&self) -> &str {
        match self.item() {
            CatalogItem::Source(Source { create_sql, .. }) => create_sql,
            CatalogItem::Sink(Sink { create_sql, .. }) => create_sql,
            CatalogItem::View(View { create_sql, .. }) => create_sql,
            CatalogItem::Index(Index { create_sql, .. }) => create_sql,
        }
    }

    fn plan_cx(&self) -> &PlanContext {
        match self.item() {
            CatalogItem::Source(Source { plan_cx, .. }) => plan_cx,
            CatalogItem::Sink(Sink { plan_cx, .. }) => plan_cx,
            CatalogItem::View(View { plan_cx, .. }) => plan_cx,
            CatalogItem::Index(Index { plan_cx, .. }) => plan_cx,
        }
    }

    fn item_type(&self) -> CatalogItemType {
        match self.item() {
            CatalogItem::Source(_) => CatalogItemType::Source,
            CatalogItem::Sink(_) => CatalogItemType::Sink,
            CatalogItem::View(_) => CatalogItemType::View,
            CatalogItem::Index(_) => CatalogItemType::Index,
        }
    }

    fn index_details(&self) -> Option<(&[ScalarExpr], GlobalId)> {
        if let CatalogItem::Index(Index { keys, on, .. }) = self.item() {
            Some((keys, *on))
        } else {
            None
        }
    }

    fn uses(&self) -> Vec<GlobalId> {
        self.uses()
    }

    fn used_by(&self) -> &[GlobalId] {
        self.used_by()
    }
}

impl<'a> PlanDatabaseResolver<'a> for DatabaseResolver<'a> {
    fn resolve_schema(&self, schema_name: &str) -> Option<(&'a dyn PlanSchema, SchemaType)> {
        self.resolve_schema(schema_name)
            .map(|(a, b)| (a as &'a dyn PlanSchema, b))
    }
}

impl PlanSchema for Schema {
    fn items(&self) -> &dyn ItemMap {
        &self.items
    }
}

impl SchemaMap for BTreeMap<String, Schema> {
    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        Box::new(self.keys().map(|k| k.as_str()))
    }
}

impl ItemMap for BTreeMap<String, GlobalId> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&String, &GlobalId)> + 'a> {
        Box::new(self.iter())
    }
}
