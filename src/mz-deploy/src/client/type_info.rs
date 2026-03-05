//! Type/column introspection operations for mz-deploy.

use crate::client::connection::TypeInfoClient;
use crate::client::errors::ConnectionError;
use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::types::{ColumnType, Types};
use crate::utils::sql_utils::quote_identifier;
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
                .postgres_client()
                .query(
                    &format!(
                        "SHOW COLUMNS FROM {}.{}.{}",
                        quoted_db, quoted_schema, quoted_object
                    ),
                    &[],
                )
                .await
                .map_err(ConnectionError::Query)?;

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
            objects,
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
                .postgres_client()
                .query(&format!("SHOW COLUMNS FROM {}", object_ref), &[])
                .await
                .map_err(ConnectionError::Query)?;

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
            objects,
        })
    }
}
