// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structured name types for SQL objects.

use std::fmt;

use serde::{Deserialize, Serialize};

use mz_sql_parser::ast::{Ident, UnresolvedObjectName};

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
