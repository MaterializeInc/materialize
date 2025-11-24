use mz_sql_parser::ast::{RawItemName, UnresolvedItemName};

/// A fully qualified object identifier.
///
/// Used to uniquely identify database objects across the project.
/// Format: `database.schema.object`
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId {
    pub database: String,
    pub schema: String,
    pub object: String,
}

impl ObjectId {
    pub fn new(database: String, schema: String, object: String) -> Self {
        Self {
            database,
            schema,
            object,
        }
    }

    /// Create from an UnresolvedItemName with default database and schema
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

    /// Create from a RawItemName with default database and schema
    pub fn from_raw_item_name(
        name: &RawItemName,
        default_database: &str,
        default_schema: &str,
    ) -> Self {
        // RawItemName wraps UnresolvedItemName
        Self::from_item_name(name.name(), default_database, default_schema)
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