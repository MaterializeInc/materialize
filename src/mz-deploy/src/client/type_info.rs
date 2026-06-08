// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Column-schema introspection for the data-contract and type-checking systems.
//!
//! Methods on [`TypeInfoClient`] query the Materialize system catalog for
//! external dependencies and `CREATE TABLE FROM SOURCE` tables, returning their
//! column names, types, nullability, object kinds, and comments as a
//! [`Types`](crate::types::Types) snapshot.
//!
//! Plain `CREATE TABLE` objects are excluded — their schemas are derived from
//! the SQL AST during type checking and do not need server queries.
//!
//! - **`lock`** uses [`query_types_for_objects`](TypeInfoClient::query_types_for_objects)
//!   to generate `types.lock` for declared dependencies and source tables,
//!   retrieving column types, object kind, and comments from the catalog in a
//!   single query per object.
//! - **`query_external_types`** delegates to `query_types_for_objects`, extracting
//!   object lists from the compiled project graph.

use crate::client::connection::TypeInfoClient;
use crate::client::errors::ConnectionError;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, ObjectKind, Types};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

/// Per-object payload returned by the catalog query in `query_types_for_objects`.
#[derive(Deserialize)]
struct CatalogObjectInfo {
    object_type: ObjectKind,
    object_comment: Option<String>,
    columns: Vec<CatalogColumnInfo>,
}

#[derive(Deserialize)]
struct CatalogColumnInfo {
    name: String,
    r#type: String,
    nullable: bool,
    position: i64,
    comment: Option<String>,
}

impl TypeInfoClient<'_> {
    /// Resolve the column schema, kind, and comments for `objects` plus
    /// `source_tables` in a single catalog query.
    ///
    /// Joins `mz_catalog.mz_columns`, `mz_catalog.mz_objects`,
    /// `mz_catalog.mz_schemas`, `mz_catalog.mz_databases`, and
    /// `mz_internal.mz_comments` to retrieve columns, types, nullability,
    /// object kind, and both object-level and column-level comments. Each input
    /// triple `(database, schema, object)` is expanded from a single `jsonb`
    /// parameter via `jsonb_array_elements`, and the per-object metadata is
    /// returned as a `jsonb` blob deserialized by serde on the client.
    ///
    /// Returns `(types, missing)` where `missing` lists any input objects that
    /// did not exist in the target catalog. The `lock` command surfaces those
    /// as `DeclaredDependenciesMissing`.
    ///
    /// Source tables are always recorded as `ObjectKind::Table` regardless of
    /// the catalog's `o.type`. Objects without columns (e.g. secrets,
    /// connections) appear in the result with an empty column map.
    pub async fn query_types_for_objects(
        &self,
        objects: &[ObjectId],
        source_tables: &[ObjectId],
    ) -> Result<(Types, Vec<ObjectId>), ConnectionError> {
        let source_table_set: BTreeSet<&ObjectId> = source_tables.iter().collect();
        let all_oids: Vec<&ObjectId> = objects.iter().chain(source_tables.iter()).collect();

        if all_oids.is_empty() {
            return Ok((
                Types {
                    version: 1,
                    tables: BTreeMap::new(),
                    kinds: BTreeMap::new(),
                    comments: BTreeMap::new(),
                },
                Vec::new(),
            ));
        }

        // Pass the (db, schema, obj) triples as a single jsonb array. Materialize's
        // unnest takes one array, so jsonb_array_elements is the cleanest way to
        // expand the input into a row per object.
        let input_json = serde_json::Value::Array(
            all_oids
                .iter()
                .map(|o| {
                    serde_json::json!({
                        "db": o.database(),
                        "sch": o.schema(),
                        "obj": o.object(),
                    })
                })
                .collect(),
        );

        let rows = self
            .client
            .query(
                "WITH input AS ( \
                    SELECT \
                        elem->>'db' AS db, \
                        elem->>'sch' AS sch, \
                        elem->>'obj' AS obj \
                    FROM jsonb_array_elements($1) AS elem \
                 ) \
                 SELECT \
                    i.db AS db, \
                    i.sch AS sch, \
                    i.obj AS obj, \
                    jsonb_build_object( \
                        'object_type', o.type, \
                        'object_comment', obj_comment.comment, \
                        'columns', COALESCE( \
                            jsonb_agg(jsonb_build_object( \
                                'name', c.name, \
                                'type', c.type, \
                                'nullable', c.nullable, \
                                'position', c.position::int8, \
                                'comment', col_comment.comment \
                            )) FILTER (WHERE c.id IS NOT NULL), \
                            '[]'::jsonb \
                        ) \
                    )::text AS data \
                 FROM input i \
                 JOIN mz_catalog.mz_schemas s ON s.name = i.sch \
                 LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id \
                 JOIN mz_catalog.mz_objects o \
                    ON o.schema_id = s.id AND o.name = i.obj \
                 LEFT JOIN mz_catalog.mz_columns c ON c.id = o.id \
                 LEFT JOIN mz_internal.mz_comments obj_comment \
                    ON o.id = obj_comment.id AND obj_comment.object_sub_id IS NULL \
                 LEFT JOIN mz_internal.mz_comments col_comment \
                    ON c.id = col_comment.id AND col_comment.object_sub_id = c.position \
                 WHERE (i.db IS NULL AND s.database_id IS NULL) \
                    OR (i.db IS NOT NULL AND d.name = i.db) \
                 GROUP BY i.db, i.sch, i.obj, o.type, obj_comment.comment",
                &[&input_json],
            )
            .await?;

        let mut tables = BTreeMap::new();
        let mut kinds = BTreeMap::new();
        let mut comments = BTreeMap::new();
        let mut found = BTreeSet::new();

        for row in &rows {
            let db: Option<String> = row.get("db");
            let sch: String = row.get("sch");
            let obj: String = row.get("obj");
            let oid = match db {
                Some(db) => ObjectId::new(db, sch, obj),
                None => ObjectId::new_system(sch, obj),
            };
            let data: String = row.get("data");
            let info: CatalogObjectInfo = serde_json::from_str(&data).map_err(|e| {
                ConnectionError::Message(format!(
                    "failed to decode catalog metadata for {}: {}",
                    oid, e
                ))
            })?;

            let kind = if source_table_set.contains(&oid) {
                ObjectKind::Table
            } else {
                info.object_type
            };
            kinds.insert(oid.clone(), kind);

            if let Some(comment) = info.object_comment {
                comments.insert(oid.clone(), comment);
            }

            let mut columns = BTreeMap::new();
            for col in info.columns {
                columns.insert(
                    col.name,
                    ColumnType {
                        r#type: col.r#type,
                        nullable: col.nullable,
                        position: usize::try_from(col.position).unwrap_or(0),
                        comment: col.comment,
                    },
                );
            }
            tables.insert(oid.clone(), columns);
            found.insert(oid);
        }

        let missing: Vec<ObjectId> = all_oids
            .iter()
            .filter(|o| !found.contains(**o))
            .map(|o| (*o).clone())
            .collect();

        Ok((
            Types {
                version: 1,
                tables,
                kinds,
                comments,
            },
            missing,
        ))
    }
}
