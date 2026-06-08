// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fully qualified database-object identifier type.
//!
//! [`ObjectId`] is the canonical way to refer to a database object throughout
//! project compilation and graph analysis. It is used as a map key,
//! dependency graph node, and display type in error messages.
//!
//! ## Invariant
//!
//! User objects are fully qualified `database.schema.object`. Objects in
//! Materialize's system schemas (`pg_catalog`, `mz_catalog`, `mz_internal`,
//! `mz_introspection`, `information_schema`, `mz_unsafe`,
//! `mz_catalog_unstable`) live at the cluster level and have no database;
//! their `ObjectId` carries `database = None` and renders as
//! `schema.object`. Partially qualified references are resolved into
//! `ObjectId`s by [`ObjectId::from_item_name`] and
//! [`ObjectId::from_raw_item_name`], which fill in missing components from
//! the current file's database/schema context.
//!
//! ## Resolution Examples
//!
//! ```text
//! from_item_name("sales", default_db="materialize", default_schema="public")
//!   → ObjectId::new("materialize", "public", "sales" )
//!
//! from_item_name("analytics.summary", default_db="materialize", default_schema="public")
//!   → ObjectId::new("materialize", "analytics", "summary" )
//!
//! from_item_name("mz_catalog.mz_objects", ...)
//!   → ObjectId { database: None, schema: "mz_catalog", object: "mz_objects" }
//!
//! from_item_name("other_db.staging.events", ...)
//!   → ObjectId::new("other_db", "staging", "events" )
//! ```

use mz_repr::namespaces::is_system_schema;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::path::Path;
use std::str::FromStr;
use tower_lsp::lsp_types::Url;

use mz_sql_parser::ast::{Ident, RawItemName, UnresolvedItemName};

/// A canonical object identifier.
///
/// User objects are fully qualified (`database.schema.object`). System-schema
/// objects (e.g. `mz_catalog.mz_objects`) carry `database = None` because
/// system catalogs are outside any database.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId {
    database: Option<String>,
    schema: String,
    object: String,
}

impl ObjectId {
    /// Create a user-object ObjectId with the given database, schema, and object names.
    pub fn new(database: String, schema: String, object: String) -> Self {
        Self {
            database: Some(database),
            schema,
            object,
        }
    }

    /// Create a system-schema ObjectId (no database). The caller is
    /// responsible for ensuring `schema` is a system schema.
    pub fn new_system(schema: String, object: String) -> Self {
        Self {
            database: None,
            schema,
            object,
        }
    }

    /// Get the database name, or `None` for system-schema objects.
    #[inline]
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    /// Get the database name, panicking if this is a system-schema oid.
    ///
    /// Use only in code paths that exclusively handle user objects (apply,
    /// promote, stage, etc.). System-schema oids never reach those paths
    /// because they are not project objects.
    #[inline]
    pub fn expect_database(&self) -> &str {
        self.database.as_deref().unwrap_or_else(|| {
            panic!(
                "system-schema ObjectId '{}' used in user-object context",
                self
            )
        })
    }

    /// Get the schema name.
    #[inline]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Get the object name.
    #[inline]
    pub fn object(&self) -> &str {
        &self.object
    }

    /// Resolve an [`UnresolvedItemName`] into an [`ObjectId`].
    ///
    /// Name parts are resolved based on how many components are present:
    /// - 1-part (`object`) — uses both `default_database` and `default_schema`.
    /// - 2-part (`schema.object`) — `database = None` if `schema` is a system
    ///   schema; otherwise `default_database`.
    /// - 3-part (`database.schema.object`) — used as-is.
    pub fn from_item_name(
        name: &UnresolvedItemName,
        default_database: &str,
        default_schema: &str,
    ) -> Self {
        match name.0.as_slice() {
            [object] => Self {
                database: Some(default_database.to_string()),
                schema: default_schema.to_string(),
                object: object.to_string(),
            },
            [schema, object] => {
                let schema_str = schema.to_string();
                let database = if is_system_schema(&schema_str) {
                    None
                } else {
                    Some(default_database.to_string())
                };
                Self {
                    database,
                    schema: schema_str,
                    object: object.to_string(),
                }
            }
            [database, schema, object] => Self {
                database: Some(database.to_string()),
                schema: schema.to_string(),
                object: object.to_string(),
            },
            _ => Self {
                database: Some(default_database.to_string()),
                schema: default_schema.to_string(),
                object: "unknown".to_string(),
            },
        }
    }

    /// Resolve a [`RawItemName`] into a fully qualified [`ObjectId`].
    ///
    /// Unwraps the inner [`UnresolvedItemName`] and delegates to
    /// [`from_item_name`](Self::from_item_name).
    pub fn from_raw_item_name(
        name: &RawItemName,
        default_database: &str,
        default_schema: &str,
    ) -> Self {
        // RawItemName wraps UnresolvedItemName
        Self::from_item_name(name.name(), default_database, default_schema)
    }

    /// Convert to an [`UnresolvedItemName`] (the reverse of [`from_item_name`](Self::from_item_name)).
    ///
    /// Produces a 3-part name for user objects and a 2-part name for
    /// system-schema objects (`database = None`).
    pub fn to_unresolved_item_name(&self) -> UnresolvedItemName {
        let mut parts = Vec::with_capacity(3);
        if let Some(db) = &self.database {
            parts.push(Ident::new(db).expect("valid database"));
        }
        parts.push(Ident::new(&self.schema).expect("valid schema"));
        parts.push(Ident::new(&self.object).expect("valid object"));
        UnresolvedItemName(parts)
    }

    /// Parse an ObjectId from a fully qualified name string.
    ///
    /// # Arguments
    /// * `fqn` - Fully qualified name in the format "database.schema.object"
    ///
    /// # Returns
    /// ObjectId if the FQN is valid (has exactly 3 dot-separated parts)
    ///
    /// # Errors
    /// Returns error if the FQN format is invalid
    /// Derive the default database and schema from a file's URI.
    ///
    /// Expects the file to be under `<root>/models/<database>/<schema>/`.
    /// Returns `None` if the path doesn't match the expected layout.
    pub fn default_db_schema_from_uri(file_uri: &Url, root: &Path) -> Option<(String, String)> {
        let file_path = file_uri.to_file_path().ok()?;
        let models_dir = root.join("models");
        let relative = file_path.strip_prefix(&models_dir).ok()?;

        let components: Vec<_> = relative
            .components()
            .map(|c| c.as_os_str().to_string_lossy().to_string())
            .collect();

        // Expected: [database, schema, file.sql] or deeper
        if components.len() >= 3 {
            Some((components[0].clone(), components[1].clone()))
        } else {
            None
        }
    }
}

impl FromStr for ObjectId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        match parts.as_slice() {
            [database, schema, object]
                if !database.is_empty() && !schema.is_empty() && !object.is_empty() =>
            {
                Ok(ObjectId {
                    database: Some((*database).to_string()),
                    schema: (*schema).to_string(),
                    object: (*object).to_string(),
                })
            }
            [schema, object]
                if !schema.is_empty() && !object.is_empty() && is_system_schema(schema) =>
            {
                Ok(ObjectId {
                    database: None,
                    schema: (*schema).to_string(),
                    object: (*object).to_string(),
                })
            }
            _ => Err(format!(
                "invalid object id '{}': expected format \
                 'database.schema.object' (or 'schema.object' for system catalogs)",
                s
            )),
        }
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.database {
            Some(db) => write!(f, "{}.{}.{}", db, self.schema, self.object),
            None => write!(f, "{}.{}", self.schema, self.object),
        }
    }
}

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ObjectIdVisitor;

        impl<'de> de::Visitor<'de> for ObjectIdVisitor {
            type Value = ObjectId;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an object ID")
            }

            fn visit_str<E>(self, value: &str) -> Result<ObjectId, E>
            where
                E: de::Error,
            {
                ObjectId::from_str(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(ObjectIdVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_user_object_three_parts() {
        let id: ObjectId = "materialize.public.foo".parse().unwrap();
        assert_eq!(id.database(), Some("materialize"));
        assert_eq!(id.schema(), "public");
        assert_eq!(id.object(), "foo");
    }

    #[test]
    fn parse_system_schema_two_parts() {
        for input in [
            "mz_catalog.mz_objects",
            "pg_catalog.pg_class",
            "mz_internal.mz_comments",
            "mz_introspection.mz_compute_dependencies",
            "information_schema.tables",
        ] {
            let id: ObjectId = input.parse().unwrap_or_else(|e| panic!("{}: {}", input, e));
            assert_eq!(id.database(), None, "{}", input);
        }
    }

    #[test]
    fn parse_user_two_parts_rejected() {
        let err = "public.foo".parse::<ObjectId>().unwrap_err();
        assert!(err.contains("invalid object id"), "got: {}", err);
    }

    #[test]
    fn parse_one_part_rejected() {
        assert!("foo".parse::<ObjectId>().is_err());
    }

    #[test]
    fn display_round_trip() {
        for input in [
            "materialize.public.foo",
            "mz_catalog.mz_objects",
            "pg_catalog.pg_class",
        ] {
            let id: ObjectId = input.parse().unwrap();
            assert_eq!(id.to_string(), input);
        }
    }

    #[test]
    fn from_item_name_two_part_system_strips_default_db() {
        let name = UnresolvedItemName(vec![
            Ident::new("mz_catalog").unwrap(),
            Ident::new("mz_objects").unwrap(),
        ]);
        let id = ObjectId::from_item_name(&name, "materialize", "public");
        assert_eq!(id.database(), None);
        assert_eq!(id.schema(), "mz_catalog");
        assert_eq!(id.object(), "mz_objects");
    }

    #[test]
    fn from_item_name_two_part_user_uses_default_db() {
        let name = UnresolvedItemName(vec![
            Ident::new("public").unwrap(),
            Ident::new("foo").unwrap(),
        ]);
        let id = ObjectId::from_item_name(&name, "materialize", "default");
        assert_eq!(id.database(), Some("materialize"));
        assert_eq!(id.schema(), "public");
        assert_eq!(id.object(), "foo");
    }

    #[test]
    fn from_item_name_three_part_used_as_is() {
        let name = UnresolvedItemName(vec![
            Ident::new("other_db").unwrap(),
            Ident::new("staging").unwrap(),
            Ident::new("events").unwrap(),
        ]);
        let id = ObjectId::from_item_name(&name, "materialize", "public");
        assert_eq!(id.database(), Some("other_db"));
        assert_eq!(id.schema(), "staging");
        assert_eq!(id.object(), "events");
    }
}
