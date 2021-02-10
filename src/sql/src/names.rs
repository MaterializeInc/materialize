// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use serde::{Deserialize, Serialize};

use sql_parser::ast::{Ident, ObjectName};

/// The fully-qualified name of an item in the catalog.
///
/// Catalog names compare case sensitively. Normalization is the responsibility
/// of the consumer (e.g., the SQL layer).
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FullName {
    pub database: DatabaseSpecifier,
    pub schema: String,
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

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DatabaseSpecifier {
    Ambient,
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

impl From<FullName> for ObjectName {
    fn from(full_name: FullName) -> ObjectName {
        let mut name_parts = Vec::new();
        if let DatabaseSpecifier::Name(database) = full_name.database {
            name_parts.push(Ident::new(database));
        }
        name_parts.push(Ident::new(full_name.schema));
        name_parts.push(Ident::new(full_name.item));
        ObjectName(name_parts)
    }
}

/// A partial name of an item in the catalog.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
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
