// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::time::SystemTime;

use expr::{GlobalId, ScalarExpr};
use repr::RelationDesc;

use crate::names::{DatabaseSpecifier, FullName, PartialName};
use crate::PlanContext;

pub trait PlanCatalog: fmt::Debug {
    fn creation_time(&self) -> SystemTime;

    fn nonce(&self) -> u64;

    fn databases<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a>;

    fn get(&self, name: &FullName) -> Result<&dyn PlanCatalogEntry, failure::Error>;

    fn get_by_id(&self, id: &GlobalId) -> &dyn PlanCatalogEntry;

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
    fn resolve_schema(&self, schema_name: &str) -> Option<&'a dyn PlanSchema>;
}

pub trait PlanSchema {
    fn items(&self) -> &dyn ItemMap;
    fn is_system(&self) -> bool;
}

pub trait SchemaMap {
    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a>;
}

pub trait ItemMap {
    fn is_empty(&self) -> bool;
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a String, &'a GlobalId)> + 'a>;
}

#[derive(Debug)]
pub struct DummyCatalog;

impl PlanCatalog for DummyCatalog {
    fn creation_time(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
    }

    fn nonce(&self) -> u64 {
        0
    }

    fn databases<'a>(&'a self) -> Box<dyn Iterator<Item = &'a str> + 'a> {
        unimplemented!();
    }

    fn get(&self, _: &FullName) -> Result<&dyn PlanCatalogEntry, failure::Error> {
        unimplemented!();
    }

    fn get_by_id(&self, _: &GlobalId) -> &dyn PlanCatalogEntry {
        unimplemented!();
    }

    fn get_schemas(&self, _: &DatabaseSpecifier) -> Option<&dyn SchemaMap> {
        unimplemented!();
    }

    fn database_resolver<'a>(
        &'a self,
        _: DatabaseSpecifier,
    ) -> Result<Box<dyn PlanDatabaseResolver<'a> + 'a>, failure::Error> {
        unimplemented!();
    }

    fn resolve(
        &self,
        _: DatabaseSpecifier,
        _: &[&str],
        _: &PartialName,
    ) -> Result<FullName, failure::Error> {
        unimplemented!();
    }

    fn empty_item_map(&self) -> Box<dyn ItemMap> {
        unimplemented!();
    }
}
