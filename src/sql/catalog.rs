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

    fn get_schemas<'a>(
        &'a self,
        _: &DatabaseSpecifier,
    ) -> Result<Box<dyn Iterator<Item = &'a str> + 'a>, failure::Error>;

    fn get_items<'a>(
        &'a self,
        _: &DatabaseSpecifier,
        schema_name: &str,
    ) -> Result<Box<dyn Iterator<Item = &'a dyn PlanCatalogEntry> + 'a>, failure::Error>;

    fn resolve(
        &self,
        current_database: DatabaseSpecifier,
        search_path: &[&str],
        name: &PartialName,
    ) -> Result<FullName, failure::Error>;

    fn resolve_schema(
        &self,
        _: &DatabaseSpecifier,
        _: Option<&DatabaseSpecifier>,
        schema_name: &str,
    ) -> Result<DatabaseSpecifier, failure::Error>;
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

    fn get_schemas<'a>(
        &'a self,
        _: &DatabaseSpecifier,
    ) -> Result<Box<dyn Iterator<Item = &'a str> + 'a>, failure::Error> {
        unimplemented!();
    }

    fn get_items<'a>(
        &'a self,
        _: &DatabaseSpecifier,
        _: &str,
    ) -> Result<Box<dyn Iterator<Item = &'a dyn PlanCatalogEntry> + 'a>, failure::Error> {
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

    fn resolve_schema(
        &self,
        _: &DatabaseSpecifier,
        _: Option<&DatabaseSpecifier>,
        _: &str,
    ) -> Result<DatabaseSpecifier, failure::Error> {
        unimplemented!();
    }
}
