// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `mz-deploy dev` — developer inner-loop overlay command.
//!
//! Creates per-developer overlay databases (`<base_db>__<profile>`) from
//! the dirty subset of the project's views, materialized views, and indexes.
//! Every overlay materialized view and index is rewritten to run on a single
//! user-supplied target cluster. The overlay is drop-and-rebuilt on every
//! invocation.
//!
//! Requires the `materialize_developer` role plus `CREATEDB` at run time.

use std::collections::BTreeSet;

use crate::cli::commands::ObjectRef;
use crate::cli::error::CliError;
use crate::client::{Client, quote_identifier};
use crate::config::Settings;
use crate::project::SchemaQualifier;
use crate::project::analysis::changeset::ChangeSet;
use crate::project::analysis::deployment_snapshot;
use crate::project::ast::Statement;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::resolve::normalize::NormalizingVisitor;
use crate::{info, verbose};

/// Overlay database name convention: `<base_db>__<profile>`.
fn overlay_db_name(base_db: &str, profile: &str) -> String {
    format!("{}__{}", base_db, profile)
}

/// Refuse to proceed if the user-supplied target cluster hosts a promoted
/// deployment.
async fn refuse_if_targets_production_cluster(
    client: &Client,
    cluster: &str,
) -> Result<(), CliError> {
    let production = client.deployments().list_production_clusters().await?;
    if let Some(rec) = production.into_iter().find(|r| r.cluster_name == cluster) {
        return Err(CliError::DevTargetsProductionCluster { cluster: rec });
    }
    Ok(())
}

/// Top-level entry point for `mz-deploy dev`.
///
/// Orchestrates role/privilege validation, dirty-set computation, plan
/// printing, and the drop+create DDL phases.
///
/// * `cluster` — target cluster for every overlay MV and index. Required
///   unless `down` is set (clap enforces this).
/// * `down` — when `true`, only run the drop phase and exit immediately.
/// * `dry_run` — when `true`, print the plan but issue no DDL.
pub async fn run(
    settings: &Settings,
    cluster: Option<String>,
    down: bool,
    dry_run: bool,
) -> Result<(), CliError> {
    let profile = settings.connection();
    // `dev` always loads with `needs_connection: true`, so a profile must be set.
    let profile_name = settings
        .profile_name
        .clone()
        .expect("dev requires an active profile");
    let project_name = settings
        .directory
        .file_name()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CliError::InvalidProjectDirectory {
            path: settings.directory.display().to_string(),
        })?
        .to_string();

    let planned_project = super::compile::run(settings, true).await?;

    let in_project_databases: BTreeSet<String> = planned_project
        .databases
        .iter()
        .map(|db| db.name.clone())
        .collect();

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    crate::cli::commands::setup::verify(&client, settings.emulator()).await?;
    let role =
        crate::cli::commands::setup::validate_connection(&client, settings.emulator()).await?;
    crate::cli::commands::setup::require_developer(role)?;

    if in_project_databases.is_empty() {
        info!("Project has no databases — nothing to overlay.");
        return Ok(());
    }

    let sample_overlay_db = overlay_db_name(
        in_project_databases.iter().next().expect("non-empty"),
        &profile_name,
    );
    crate::cli::commands::setup::require_createdb(&client, &profile.username, &sample_overlay_db)
        .await?;

    if down {
        drop_phase(&client, &profile_name, &project_name, &in_project_databases).await?;
        info!("Overlay removed.");
        return Ok(());
    }

    // clap guarantees `cluster` is `Some` whenever `down` is false.
    let target_cluster = cluster.expect("cluster required unless --down");
    refuse_if_targets_production_cluster(&client, &target_cluster).await?;

    let new_snapshot = deployment_snapshot::build_snapshot_from_planned(&planned_project)?;
    let production_snapshot = deployment_snapshot::load_from_database(&client, None).await?;

    // Empty production → full overlay (first-run semantics matching stage).
    let change_set = if production_snapshot.objects.is_empty() {
        verbose!("Full deployment: no production deployment found");
        None
    } else {
        Some(ChangeSet::from_deployment_snapshot_comparison(
            &production_snapshot,
            &new_snapshot,
            &planned_project,
            &BTreeSet::new(),
        ))
    };

    let all_objects = match change_set.as_ref() {
        Some(cs) if cs.is_empty() => Vec::new(),
        Some(cs) => {
            verbose!("{}", cs);
            planned_project.get_sorted_objects_filtered(&cs.objects_to_deploy)?
        }
        None => planned_project.get_sorted_objects()?,
    };

    let mut skipped = 0usize;
    let overlay_objects: Vec<ObjectRef<'_>> = all_objects
        .into_iter()
        .filter(|(_, typed_obj)| match &typed_obj.stmt {
            Statement::CreateView(_) | Statement::CreateMaterializedView(_) => true,
            _ => {
                skipped += 1;
                false
            }
        })
        .collect();
    if skipped > 0 {
        verbose!(
            "skipped {} object(s) of unsupported type (tables/sources/sinks)",
            skipped
        );
    }

    let dirty_schemas: BTreeSet<SchemaQualifier> = overlay_objects
        .iter()
        .map(|(id, _)| {
            SchemaQualifier::new(id.expect_database().to_string(), id.schema().to_string())
        })
        .collect();

    print_plan(&dirty_schemas, &profile_name);

    if dry_run {
        return Ok(());
    }

    drop_phase(&client, &profile_name, &project_name, &in_project_databases).await?;

    if dirty_schemas.is_empty() {
        info!("Dev overlay ready (nothing to overlay).");
        return Ok(());
    }

    create_phase(
        &client,
        &profile_name,
        &project_name,
        &in_project_databases,
        &dirty_schemas,
        &overlay_objects,
        &target_cluster,
    )
    .await?;

    info!("Dev overlay ready.");
    Ok(())
}

fn print_plan(dirty_schemas: &BTreeSet<SchemaQualifier>, profile_name: &str) {
    if dirty_schemas.is_empty() {
        info!("Dirty set is empty — nothing to overlay.");
        return;
    }
    info!("→ Dirty schemas:");
    for qual in dirty_schemas {
        info!("    {}.{}", qual.database, qual.schema);
    }

    let overlay_dbs: BTreeSet<String> = dirty_schemas
        .iter()
        .map(|q| overlay_db_name(&q.database, profile_name))
        .collect();
    info!("→ Overlay databases:");
    for db in &overlay_dbs {
        info!("    {}", db);
    }
}

/// Phase 1 of the dev rebuild: drop every overlay database recorded for
/// this `(profile, project)` pair, then purge the manifest rows. Finally
/// sweep any in-project `<base_db>__<profile>` names not in the manifest
/// (catalog restore, interrupted prior run).
pub(crate) async fn drop_phase(
    client: &Client,
    profile_name: &str,
    project_name: &str,
    in_project_databases: &BTreeSet<String>,
) -> Result<(), CliError> {
    let overlays = client.dev_overlays();

    let existing: BTreeSet<String> = overlays
        .list_overlays(profile_name, project_name)
        .await?
        .into_iter()
        .collect();
    for db in &existing {
        drop_database(client, db).await?;
    }
    overlays.delete_overlays(profile_name, project_name).await?;

    for base_db in in_project_databases {
        let overlay_db = overlay_db_name(base_db, profile_name);
        if !existing.contains(&overlay_db) {
            drop_database(client, &overlay_db).await?;
        }
    }

    Ok(())
}

async fn drop_database(client: &Client, database: &str) -> Result<(), CliError> {
    let sql = format!(
        "DROP DATABASE IF EXISTS {} CASCADE",
        quote_identifier(database),
    );
    client.execute(&sql, &[]).await?;
    Ok(())
}

/// Phase 2 of the dev rebuild: create overlay databases, schemas, and objects.
///
/// Per dirty schema we issue `CREATE DATABASE IF NOT EXISTS <overlay_db>`,
/// insert a manifest row (so `drop_phase` can always reach it even if we crash
/// mid-run), then `CREATE SCHEMA IF NOT EXISTS`. Objects are emitted in
/// dependency order with references rewritten through `OverlayTransformer`
/// and every `IN CLUSTER` clause rewritten to `target_cluster`.
pub(crate) async fn create_phase(
    client: &Client,
    profile_name: &str,
    project_name: &str,
    in_project_databases: &BTreeSet<String>,
    dirty_schemas: &BTreeSet<SchemaQualifier>,
    overlay_objects: &[ObjectRef<'_>],
    target_cluster: &str,
) -> Result<(), CliError> {
    let provisioning = client.provisioning();
    let overlays = client.dev_overlays();

    let mut created_overlay_dbs: BTreeSet<String> = BTreeSet::new();
    for qualifier in dirty_schemas {
        let overlay_db = overlay_db_name(&qualifier.database, profile_name);
        if created_overlay_dbs.insert(overlay_db.clone()) {
            provisioning.create_database(&overlay_db).await?;
            overlays
                .insert_overlay(profile_name, project_name, &overlay_db)
                .await?;
        }
    }

    for qualifier in dirty_schemas {
        let overlay_db = overlay_db_name(&qualifier.database, profile_name);
        provisioning
            .create_schema(&overlay_db, &qualifier.schema)
            .await?;
    }

    for (object_id, typed_object) in overlay_objects {
        let original_fqn: FullyQualifiedName = object_id.clone().into();
        let mut visitor = NormalizingVisitor::overlay(
            &original_fqn,
            profile_name,
            in_project_databases,
            dirty_schemas,
            target_cluster,
        );

        let stmt = typed_object
            .stmt
            .clone()
            .normalize_name_with(&visitor, &original_fqn.to_item_name())
            .normalize_dependencies_with(&mut visitor)
            .normalize_cluster_with(&visitor);

        client.execute(&stmt.to_string(), &[]).await?;

        let mut indexes = typed_object.indexes.clone();
        visitor.normalize_index_references(&mut indexes);
        visitor.normalize_index_clusters(&mut indexes);
        for index in &indexes {
            client.execute(&index.to_string(), &[]).await?;
        }
    }

    Ok(())
}
