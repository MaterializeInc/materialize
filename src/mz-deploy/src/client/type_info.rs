//! Column-schema introspection for the `types.lock` system.
//!
//! Methods on [`TypeInfoClient`] run `SHOW COLUMNS` against external
//! dependencies and project tables on the live region, returning their
//! column names, types, and nullability as a [`Types`](crate::types::Types)
//! snapshot. This is the data source for `gen-data-contracts`.

use crate::client::connection::TypeInfoClient;
use crate::client::errors::ConnectionError;
use crate::client::quote_identifier;
use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::types::{ColumnType, Types};
use std::collections::BTreeMap;

impl TypeInfoClient<'_> {
    /// Query SHOW COLUMNS for all external dependencies and return their schemas as a Types object.
    pub async fn query_external_types(
        &self,
        project: &planned::Project,
    ) -> Result<Types, ConnectionError> {
        let mut objects = BTreeMap::new();
        let oids = project
            .external_dependencies
            .iter()
            .cloned()
            .chain(project.get_tables());

        for oid in oids {
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
            for row in rows {
                let name: String = row.get("name");
                let type_str: String = row.get("type");
                let nullable: bool = row.get("nullable");

                columns.insert(
                    name,
                    ColumnType {
                        r#type: type_str,
                        nullable,
                    },
                );
            }

            objects.insert(oid.to_string(), columns);
        }

        Ok(Types {
            version: 1,
            tables: objects,
        })
    }

    /// Query types for internal project views from the database.
    pub async fn query_internal_types(
        &self,
        object_ids: &[&ObjectId],
        flatten: bool,
    ) -> Result<Types, ConnectionError> {
        let mut objects = BTreeMap::new();

        for oid in object_ids {
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
            for row in rows {
                let name: String = row.get("name");
                let type_str: String = row.get("type");
                let nullable: bool = row.get("nullable");

                columns.insert(
                    name,
                    ColumnType {
                        r#type: type_str,
                        nullable,
                    },
                );
            }

            objects.insert(oid.to_string(), columns);
        }

        Ok(Types {
            version: 1,
            tables: objects,
        })
    }
}
