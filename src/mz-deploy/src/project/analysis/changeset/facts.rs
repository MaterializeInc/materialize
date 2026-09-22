// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fact extraction and loading for the dirty propagation query.
//!
//! Translates the compiled project and the two deployment snapshots into the
//! temporary tables that `dirty_propagation.sql` reads, and bulk-loads them
//! over `COPY`. The fact contract, including every column's meaning, is
//! documented in that file's header.
//!
//! Object keys travel as three raw name components. `ObjectId`'s `Display`
//! quotes identifiers that need it, so a rendered key would not match the raw
//! `database`/`schema`/`object` columns that `_mz_deploy.tables.objects`
//! stores, and every object in a schema named after a keyword would read as
//! deleted and re-added on each deploy.

use std::collections::BTreeSet;

use crate::client::{Client, ConnectionError, copy_escape};
use crate::project::SchemaQualifier;
use crate::project::analysis::deployment_snapshot::DeploymentSnapshot;
use crate::project::ast::Statement;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use mz_sql_parser::ast::RawClusterName;

/// The fact tables, in creation order. One statement per file, so nothing
/// here needs parsing before it reaches the server.
const TEMP_TABLES: &[&str] = &[
    include_str!("temp_tables/project_object.sql"),
    include_str!("temp_tables/project_depends_on.sql"),
    include_str!("temp_tables/project_stmt_cluster.sql"),
    include_str!("temp_tables/project_index_cluster.sql"),
    include_str!("temp_tables/replacement_schema.sql"),
    include_str!("temp_tables/new_object.sql"),
    include_str!("temp_tables/old_object.sql"),
    include_str!("temp_tables/old_schema_kind.sql"),
    include_str!("temp_tables/forced_schema.sql"),
];

/// Statement kind tag written to `project_object.kind`.
///
/// The apply-managed kinds are the ones `partition_objects` drops and the
/// snapshot builder skips, so the query recognizes them by this tag rather
/// than by a separate fact.
fn object_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::CreateView(_) => "view",
        Statement::CreateMaterializedView(_) => "materialized_view",
        Statement::CreateSink(_) => "sink",
        Statement::CreateTable(_) => "table",
        Statement::CreateTableFromSource(_) => "table_from_source",
        Statement::CreateSource(_) => "source",
        Statement::CreateSecret(_) => "secret",
        Statement::CreateConnection(_) => "connection",
    }
}

/// The three raw name components of `id`, tab-delimited and COPY-escaped.
///
/// A system-schema object has no database and takes `''`, which keeps every
/// join in the query on plain equality instead of three-valued logic. Such an
/// object reaches the query only as the parent side of `project_depends_on`.
fn key(id: &ObjectId) -> String {
    format!(
        "{}\t{}\t{}",
        copy_escape(id.database().unwrap_or("")),
        copy_escape(id.schema()),
        copy_escape(id.object()),
    )
}

/// The two raw name components of a schema qualifier, tab-delimited.
fn schema_key(sq: &SchemaQualifier) -> String {
    format!("{}\t{}", copy_escape(&sq.database), copy_escape(&sq.schema))
}

/// Create the fact tables on the session.
///
/// Executes one statement at a time: Materialize rejects DDL inside the
/// implicit transaction that a multi-statement simple query creates.
///
/// The tables are session-scoped and created once per command, so a plain
/// `CREATE` is used rather than `IF NOT EXISTS`. A second creation in one
/// session means the analysis ran twice, which is worth an error rather than
/// stale rows surviving into the second run.
async fn create_temp_tables(client: &Client) -> Result<(), ConnectionError> {
    for statement in TEMP_TABLES {
        client.execute(statement, &[]).await?;
    }
    Ok(())
}

/// Create the fact tables and load every relation the query reads.
///
/// The loads are writes, so the caller must run the query itself outside this
/// call's transactions rather than wrapping both in one.
pub(super) async fn load(
    client: &Client,
    project: &Project,
    old_snapshot: &DeploymentSnapshot,
    new_snapshot: &DeploymentSnapshot,
    forced_dirty_schemas: &BTreeSet<SchemaQualifier>,
) -> Result<(), ConnectionError> {
    create_temp_tables(client).await?;

    let mut objects = Vec::new();
    let mut depends_on = Vec::new();
    let mut stmt_clusters = Vec::new();
    let mut index_clusters = Vec::new();

    for db in &project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                let id = &obj.id;
                objects.push(format!(
                    "{}\t{}",
                    key(id),
                    copy_escape(object_kind(&obj.typed_object.stmt))
                ));

                if let Some(parents) = project.dependency_graph.get(id) {
                    for parent in parents {
                        depends_on.push(format!("{}\t{}", key(id), key(parent)));
                    }
                }

                if let Some(cluster) = obj.typed_object.stmt_cluster() {
                    stmt_clusters.push(format!(
                        "{}\t{}",
                        key(id),
                        copy_escape(&cluster.to_string())
                    ));
                }

                for index in &obj.typed_object.indexes {
                    let Some(RawClusterName::Unresolved(cluster)) = &index.in_cluster else {
                        continue;
                    };
                    let name = index
                        .name
                        .as_ref()
                        .map(|n| n.to_string())
                        .unwrap_or_default();
                    index_clusters.push(format!(
                        "{}\t{}\t{}",
                        key(id),
                        copy_escape(&name),
                        copy_escape(&cluster.to_string())
                    ));
                }
            }
        }
    }

    client
        .copy_into("project_object", objects.into_iter())
        .await?;
    client
        .copy_into("project_depends_on", depends_on.into_iter())
        .await?;
    client
        .copy_into("project_stmt_cluster", stmt_clusters.into_iter())
        .await?;
    client
        .copy_into("project_index_cluster", index_clusters.into_iter())
        .await?;
    client
        .copy_into(
            "replacement_schema",
            project.replacement_schemas.iter().map(schema_key),
        )
        .await?;
    client
        .copy_into(
            "new_object",
            new_snapshot
                .objects
                .iter()
                .map(|(id, hash)| format!("{}\t{}", key(id), copy_escape(hash))),
        )
        .await?;
    client
        .copy_into(
            "old_object",
            old_snapshot
                .objects
                .iter()
                .map(|(id, hash)| format!("{}\t{}", key(id), copy_escape(hash))),
        )
        .await?;
    client
        .copy_into(
            "old_schema_kind",
            old_snapshot.schemas.iter().map(|(sq, kind)| {
                format!("{}\t{}", schema_key(sq), copy_escape(&kind.to_string()))
            }),
        )
        .await?;
    client
        .copy_into("forced_schema", forced_dirty_schemas.iter().map(schema_key))
        .await?;

    Ok(())
}

/// Reads the kind tag back, so a change to [`DeploymentKind`]'s spelling
/// cannot silently desync the `'replacement'` literal the query matches on.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::DeploymentKind;

    #[mz_ore::test]
    fn replacement_kind_matches_query_literal() {
        assert_eq!(DeploymentKind::Replacement.to_string(), "replacement");
    }

    #[mz_ore::test]
    fn copy_escape_protects_delimiters() {
        assert_eq!(copy_escape("a\tb"), "a\\tb");
        assert_eq!(copy_escape("a\nb"), "a\\nb");
        assert_eq!(copy_escape("a\\b"), "a\\\\b");
    }
}
