//! Fully qualified `database.schema.object` identifier type.
//!
//! [`ObjectId`] is the canonical way to refer to a database object throughout
//! the project pipeline. It is used as a map key, dependency graph node, and
//! display type in error messages.
//!
//! ## Invariant
//!
//! An `ObjectId` is always fully qualified: all three components (`database`,
//! `schema`, `object`) are non-empty strings. Partially qualified references
//! are resolved into `ObjectId`s by [`ObjectId::from_item_name`] and
//! [`ObjectId::from_raw_item_name`], which fill in missing components from
//! the current file's database/schema context.
//!
//! ## Resolution Examples
//!
//! ```text
//! from_item_name("sales", default_db="materialize", default_schema="public")
//!   → ObjectId { database: "materialize", schema: "public", object: "sales" }
//!
//! from_item_name("analytics.summary", default_db="materialize", default_schema="public")
//!   → ObjectId { database: "materialize", schema: "analytics", object: "summary" }
//!
//! from_item_name("other_db.staging.events", ...)
//!   → ObjectId { database: "other_db", schema: "staging", object: "events" }
//! ```

use mz_sql_parser::ast::{Ident, RawItemName, UnresolvedItemName};

/// A fully qualified object identifier.
///
/// Used to uniquely identify database objects across the project.
/// Format: `database.schema.object`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
pub struct ObjectId {
    pub database: String,
    pub schema: String,
    pub object: String,
}

impl ObjectId {
    /// Create a new ObjectId with the given database, schema, and object names.
    pub fn new(database: String, schema: String, object: String) -> Self {
        Self {
            database,
            schema,
            object,
        }
    }

    /// Get the database name.
    #[inline]
    pub fn database(&self) -> &str {
        &self.database
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

    /// Resolve an [`UnresolvedItemName`] into a fully qualified [`ObjectId`].
    ///
    /// Name parts are resolved based on how many components are present:
    /// - 1-part (`object`) — uses both `default_database` and `default_schema`.
    /// - 2-part (`schema.object`) — uses `default_database`.
    /// - 3-part (`database.schema.object`) — used as-is.
    pub fn from_item_name(
        name: &UnresolvedItemName,
        default_database: &str,
        default_schema: &str,
    ) -> Self {
        match name.0.as_slice() {
            [object] => Self::new(
                default_database.to_string(),
                default_schema.to_string(),
                object.to_string(),
            ),
            [schema, object] => Self::new(
                default_database.to_string(),
                schema.to_string(),
                object.to_string(),
            ),
            [database, schema, object] => {
                Self::new(database.to_string(), schema.to_string(), object.to_string())
            }
            _ => Self::new(
                default_database.to_string(),
                default_schema.to_string(),
                "unknown".to_string(),
            ),
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
    /// Always produces a fully qualified 3-part name (`database.schema.object`).
    pub fn to_unresolved_item_name(&self) -> UnresolvedItemName {
        UnresolvedItemName(vec![
            Ident::new(&self.database).expect("valid database"),
            Ident::new(&self.schema).expect("valid schema"),
            Ident::new(&self.object).expect("valid object"),
        ])
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
    #[must_use = "this returns the parsed ObjectId, which should be used"]
    pub fn from_fqn(fqn: &str) -> Result<Self, String> {
        let parts: Vec<&str> = fqn.split('.').collect();
        if parts.len() != 3 {
            return Err(format!(
                "invalid FQN '{}': expected format 'database.schema.object'",
                fqn
            ));
        }
        Ok(ObjectId {
            database: parts[0].to_string(),
            schema: parts[1].to_string(),
            object: parts[2].to_string(),
        })
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.database, self.schema, self.object)
    }
}
