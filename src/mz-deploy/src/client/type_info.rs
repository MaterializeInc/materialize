//! Column-schema introspection for the data-contract and type-checking systems.
//!
//! Methods on [`TypeInfoClient`] run `SHOW COLUMNS` against external
//! dependencies and `CREATE TABLE FROM SOURCE` tables, returning their column
//! names, types, and nullability as a [`Types`](crate::types::Types) snapshot.
//!
//! Plain `CREATE TABLE` objects are excluded — their schemas are derived from
//! the SQL AST during type checking and do not need server queries.
//!
//! - **`gen-data-contracts`** uses [`query_external_types`](TypeInfoClient::query_external_types)
//!   to generate `types.lock` for external dependencies and source tables.
//! - **Incremental type checking** uses [`query_object_columns`](TypeInfoClient::query_object_columns)
//!   to query a single view's output columns inline after validation, enabling
//!   type-hash comparison for dirty propagation.

use crate::client::connection::TypeInfoClient;
use crate::client::errors::ConnectionError;
use crate::client::quote_identifier;
use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::types::{ColumnType, ObjectKind, Types};
use std::collections::BTreeMap;

impl TypeInfoClient<'_> {
    /// Query SHOW COLUMNS for external dependencies and `CREATE TABLE FROM SOURCE` tables.
    ///
    /// Plain `CREATE TABLE` objects are excluded — their schemas are derived from
    /// the SQL AST during type checking. Only `CreateTableFromSource` tables need
    /// their columns queried from the live server.
    ///
    /// Also queries `mz_catalog.mz_objects` to determine each object's kind.
    /// Source tables are always recorded as `ObjectKind::Table`.
    pub async fn query_external_types(
        &self,
        project: &planned::Project,
    ) -> Result<Types, ConnectionError> {
        let mut objects = BTreeMap::new();
        let mut kinds = BTreeMap::new();

        // Source tables are always Table kind
        let source_table_oids: Vec<ObjectId> = project.get_tables_from_source().collect();
        for oid in &source_table_oids {
            kinds.insert(oid.to_string(), ObjectKind::Table);
        }

        let oids: Vec<ObjectId> = project
            .external_dependencies
            .iter()
            .cloned()
            .chain(source_table_oids.into_iter())
            .collect();

        for oid in &oids {
            let quoted_db = quote_identifier(&oid.database);
            let quoted_schema = quote_identifier(&oid.schema);
            let quoted_object = quote_identifier(&oid.object);

            let rows = self
                .client
                .query(
                    &format!(
                        "SHOW COLUMNS FROM {}.{}.{}",
                        quoted_db, quoted_schema, quoted_object
                    ),
                    &[],
                )
                .await?;

            let mut columns = BTreeMap::new();
            for (position, row) in rows.iter().enumerate() {
                let name: String = row.get("name");
                let type_str: String = row.get("type");
                let nullable: bool = row.get("nullable");

                columns.insert(
                    name,
                    ColumnType {
                        r#type: type_str,
                        nullable,
                        position,
                    },
                );
            }

            objects.insert(oid.to_string(), columns);
        }

        // Query object kinds from mz_catalog for external dependencies
        for oid in &project.external_dependencies {
            let quoted_db = quote_identifier(&oid.database);
            let quoted_schema = quote_identifier(&oid.schema);
            let quoted_object = quote_identifier(&oid.object);

            let rows = self
                .client
                .query(
                    &format!(
                        "SELECT o.type FROM mz_catalog.mz_objects o \
                         JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id \
                         JOIN mz_catalog.mz_databases d ON s.database_id = d.id \
                         WHERE d.name = {} AND s.name = {} AND o.name = {}",
                        quoted_db, quoted_schema, quoted_object
                    ),
                    &[],
                )
                .await?;

            if let Some(row) = rows.first() {
                let type_str: String = row.get("type");
                let kind = match type_str.as_str() {
                    "table" => ObjectKind::Table,
                    "view" => ObjectKind::View,
                    "materialized-view" => ObjectKind::MaterializedView,
                    "source" => ObjectKind::Source,
                    "sink" => ObjectKind::Sink,
                    "secret" => ObjectKind::Secret,
                    "connection" => ObjectKind::Connection,
                    _ => ObjectKind::Table,
                };
                kinds.insert(oid.to_string(), kind);
            }
        }

        Ok(Types {
            version: 1,
            tables: objects,
            kinds,
        })
    }

    /// Query column types for a single object via `SHOW COLUMNS`.
    ///
    /// When `flatten` is true, the object is referenced using the flattened
    /// `"db.schema.object"` form (for temporary views). Otherwise it uses
    /// the standard `db.schema.object` quoting.
    pub async fn query_object_columns(
        &self,
        oid: &ObjectId,
        flatten: bool,
    ) -> Result<BTreeMap<String, ColumnType>, ConnectionError> {
        let object_ref = if flatten {
            format!("\"{}.{}.{}\"", oid.database, oid.schema, oid.object)
        } else {
            let quoted_db = quote_identifier(&oid.database);
            let quoted_schema = quote_identifier(&oid.schema);
            let quoted_object = quote_identifier(&oid.object);
            format!("{}.{}.{}", quoted_db, quoted_schema, quoted_object)
        };

        let rows = self
            .client
            .query(&format!("SHOW COLUMNS FROM {}", object_ref), &[])
            .await?;

        let mut columns = BTreeMap::new();
        for (position, row) in rows.iter().enumerate() {
            let name: String = row.get("name");
            let type_str: String = row.get("type");
            let nullable: bool = row.get("nullable");

            columns.insert(
                name,
                ColumnType {
                    r#type: type_str,
                    nullable,
                    position,
                },
            );
        }

        Ok(columns)
    }

    /// Query types for internal project views from the database.
    pub async fn query_internal_types(
        &self,
        object_ids: &[&ObjectId],
        flatten: bool,
    ) -> Result<Types, ConnectionError> {
        let mut objects = BTreeMap::new();

        for oid in object_ids {
            let columns = self.query_object_columns(oid, flatten).await?;
            objects.insert(oid.to_string(), columns);
        }

        Ok(Types {
            version: 1,
            tables: objects,
            kinds: BTreeMap::new(),
        })
    }
}
