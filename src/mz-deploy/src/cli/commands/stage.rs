//! Stage command - deploy to staging environment with renamed schemas and clusters.

use crate::cli::{CliError, helpers};
use crate::client::{ClusterOptions, Profile};
use crate::project::changeset::ChangeSet;
use crate::project::planned::extract_external_indexes;
use crate::project::{self, normalize::NormalizingVisitor, typed::FullyQualifiedName};
use crate::utils::git;
use crate::utils::git::get_git_commit;
use crate::verbose;
use mz_sql_parser::ast::Ident;
use owo_colors::OwoColorize;
use std::collections::HashSet;
use std::path::Path;

/// Deploy project to staging environment with renamed schemas and clusters.
///
/// This command implements blue/green deployment by creating staging versions of all
/// schemas and clusters with a suffix (e.g., `public_dev` for staging env "dev").
/// Objects are deployed to these staging resources, allowing testing without
/// affecting production. Later, `apply --staging-env` can atomically swap staging
/// and production using ALTER SWAP.
///
/// This command:
/// - Compiles the project (using `compile::run`)
/// - Determines staging environment name (from --name or git SHA)
/// - Creates staging schemas (schema_<env>) for all schemas in the project
/// - Creates staging clusters (cluster_<env>) by cloning production cluster configs
/// - Deploys all objects to staging environment with transformed names
/// - Records deployment metadata for conflict detection
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `stage_name` - Optional staging environment name (defaults to first 5 chars of git SHA)
/// * `directory` - Project root directory
/// * `allow_dirty` - Allow deploying with uncommitted changes
///
/// # Returns
/// Ok(()) if staging deployment succeeds
///
/// # Errors
/// Returns `CliError::GitShaFailed` if no git SHA and no --name provided
/// Returns `CliError::GitDirty` if repository has uncommitted changes and allow_dirty is false
/// Returns `CliError::Connection` for database errors
/// Returns `CliError::Project` for project compilation errors
pub async fn run(
    profile: &Profile,
    stage_name: Option<&str>,
    directory: &Path,
    allow_dirty: bool,
) -> Result<(), CliError> {
    // Check for uncommitted changes before proceeding
    if !allow_dirty && git::is_dirty(directory) {
        return Err(CliError::GitDirty);
    }

    let stage_name = match stage_name {
        Some(name) => name.to_string(),
        None => get_git_commit(directory)
            .map(|commit| commit.chars().take(7).collect())
            .ok_or(CliError::GitShaFailed)?,
    };

    // Run compile to validate and get the project (skip type checking for staging deployment)
    let compile_args = super::compile::CompileArgs {
        typecheck: false, // Skip type checking for staging deployment
        docker_image: None,
    };
    let planned_project = super::compile::run(directory, compile_args).await?;

    let staging_suffix = format!("_{}", stage_name);

    println!("Deploying to staging environment: {}", stage_name);

    // Connect to the database
    let client = helpers::connect_to_database(profile).await?;

    // Initialize deployment tracking infrastructure
    helpers::initialize_deployment_tracking(&client).await?;

    // Validate deployment doesn't already exist
    let existing_metadata = client.get_deployment_metadata(&stage_name).await?;

    if existing_metadata.is_some() {
        return Err(CliError::InvalidEnvironmentName {
            name: format!("deployment '{}' already exists", stage_name),
        });
    }

    // Build new snapshot from current planned project
    let new_snapshot = project::deployment_snapshot::build_snapshot_from_planned(&planned_project)?;

    // Load PRODUCTION deployment state for comparison (environment=None)
    // Stage always compares against production, not against previous staging deployments
    let production_snapshot =
        project::deployment_snapshot::load_from_database(&client, None).await?;

    let change_set = if !production_snapshot.objects.is_empty() {
        Some(ChangeSet::from_deployment_snapshot_comparison(
            &production_snapshot,
            &new_snapshot,
            &planned_project,
        ))
    } else {
        None
    };

    let objects = if let Some(ref cs) = change_set {
        if cs.is_empty() {
            println!("No changes detected compared to production, skipping deployment");
            return Ok(());
        }

        verbose!("{}", cs);
        planned_project.get_sorted_objects_filtered(&cs.objects_to_deploy)?
    } else {
        verbose!("Full deployment: no production deployment found");
        planned_project.get_sorted_objects()?
    };

    // Collect schemas and clusters from objects that are actually being deployed
    let mut schema_set = HashSet::new();
    let mut cluster_set = HashSet::new();

    for (object_id, typed_obj) in &objects {
        schema_set.insert((object_id.database.clone(), object_id.schema.clone()));
        cluster_set.extend(typed_obj.clusters());
    }

    // Also include clusters from the changeset if available
    if let Some(ref cs) = change_set {
        for cluster in &cs.dirty_clusters {
            cluster_set.insert(cluster.name.clone());
        }
    } else {
        // For full deployment, include all project clusters
        for cluster in &planned_project.cluster_dependencies {
            cluster_set.insert(cluster.name.clone());
        }
    }

    // Create staging schemas
    println!("Creating staging schemas...");
    for (database, schema) in &schema_set {
        let staging_schema = format!("{}{}", schema, staging_suffix);
        client.create_schema(database, &staging_schema).await?;
        verbose!("  Created schema {}.{}", database, staging_schema);
    }
    verbose!();

    // Create staging clusters (by cloning production cluster configs)
    println!("Creating staging clusters ...");
    for prod_cluster in &cluster_set {
        let staging_cluster = format!("{}{}", prod_cluster, staging_suffix);

        // Check if staging cluster already exists
        let cluster_exists = client.cluster_exists(&staging_cluster).await?;

        if cluster_exists {
            verbose!("  Cluster '{}' already exists, skipping", staging_cluster);
            continue;
        }

        // Get production cluster configuration
        let prod_config = client.get_cluster(prod_cluster).await?;

        let prod_config = match prod_config {
            Some(config) => config,
            None => {
                return Err(CliError::ClusterNotFound {
                    name: prod_cluster.clone(),
                });
            }
        };

        // Create cluster options from production config
        let options = ClusterOptions::from_cluster(&prod_config)?;

        // Create staging cluster
        client
            .create_cluster(&staging_cluster, options.clone())
            .await?;
        verbose!(
            "  Created cluster '{}' (size: {}, replication_factor: {}, cloned from '{}')",
            staging_cluster,
            options.size,
            options.replication_factor,
            prod_cluster
        );
    }
    verbose!();

    // Write cluster mappings to deploy.clusters table
    let cluster_names: Vec<String> = cluster_set.iter().cloned().collect();
    client
        .insert_deployment_clusters(&stage_name, &cluster_names)
        .await?;

    // Deploy objects using staging transformer
    println!("Deploying objects to staging environment...\n");

    // Collect ObjectIds from objects being deployed for the staging transformer
    let objects_to_deploy_set: HashSet<_> = objects.iter().map(|(oid, _)| oid.clone()).collect();

    // Deploy external indexes
    let mut external_indexes: Vec<_> = planned_project
        .iter_objects()
        .filter(|object| !objects_to_deploy_set.contains(&object.id))
        .flat_map(extract_external_indexes)
        .filter_map(|(cluster, index)| cluster_set.contains(&cluster.name).then_some(index))
        .collect();

    let fqn = FullyQualifiedName::null();
    let empty = HashSet::new();
    let external_index_visitor =
        NormalizingVisitor::staging(&fqn, staging_suffix.clone(), &empty, None);
    external_index_visitor.normalize_index_clusters(external_indexes.as_mut_slice());
    for index in external_indexes {
        verbose!("Creating external index {}", index);
        client.execute(&index.to_string(), &[]).await?;
    }

    let mut success_count = 0;
    for (idx, (object_id, typed_obj)) in objects.iter().enumerate() {
        verbose!(
            "Applying {}/{}: {}{} (to schema {}{})",
            idx + 1,
            objects.len(),
            &object_id.object,
            staging_suffix,
            &object_id.schema,
            staging_suffix
        );

        // Create original FQN (without staging suffix)
        let original_item_name = mz_sql_parser::ast::UnresolvedItemName(vec![
            Ident::new(&object_id.database).expect("valid database"),
            Ident::new(&object_id.schema).expect("valid schema"),
            Ident::new(&object_id.object).expect("valid object"),
        ]);
        let original_fqn = FullyQualifiedName::from(original_item_name);

        // Create staging visitor (it will apply the suffix during normalization)
        // External dependencies and objects not being deployed are NOT transformed
        let visitor = NormalizingVisitor::staging(
            &original_fqn,
            staging_suffix.clone(),
            &planned_project.external_dependencies,
            Some(&objects_to_deploy_set),
        );

        // Normalize and deploy main statement
        // The visitor will transform all names and clusters to include the staging suffix
        let stmt = typed_obj
            .stmt
            .clone()
            .normalize_name_with(&visitor, &original_fqn.to_item_name())
            .normalize_dependencies_with(&visitor)
            .normalize_cluster_with(&visitor);

        client
            .execute(&stmt.to_string(), &[])
            .await
            .map_err(|source| CliError::SqlExecutionFailed {
                statement: stmt.to_string(),
                source,
            })?;

        // Deploy indexes, grants, and comments (normalize them with staging transformer)
        let mut indexes = typed_obj.indexes.clone();
        let mut grants = typed_obj.grants.clone();
        let mut comments = typed_obj.comments.clone();

        // Normalize references to use staging suffix
        visitor.normalize_index_references(&mut indexes);
        visitor.normalize_index_clusters(&mut indexes);
        visitor.normalize_grant_references(&mut grants);
        visitor.normalize_comment_references(&mut comments);

        for index in &indexes {
            client
                .execute(&index.to_string(), &[])
                .await
                .map_err(|source| CliError::SqlExecutionFailed {
                    statement: index.to_string(),
                    source,
                })?;
        }

        for grant in &grants {
            client
                .execute(&grant.to_string(), &[])
                .await
                .map_err(|source| CliError::SqlExecutionFailed {
                    statement: grant.to_string(),
                    source,
                })?;
        }

        for comment in &comments {
            client
                .execute(&comment.to_string(), &[])
                .await
                .map_err(|source| CliError::SqlExecutionFailed {
                    statement: comment.to_string(),
                    source,
                })?;
        }

        success_count += 1;
    }

    // Collect deployment metadata
    let metadata = helpers::collect_deployment_metadata(&client, directory).await;

    // Build a filtered snapshot containing only the objects that were actually deployed
    // This ensures the staging deployment table only tracks staged objects
    let mut staging_snapshot = project::deployment_snapshot::DeploymentSnapshot::default();
    for (object_id, typed_obj) in &objects {
        let hash = project::deployment_snapshot::compute_typed_hash(typed_obj);
        staging_snapshot.objects.insert(object_id.clone(), hash);

        // Track which schema this object belongs to
        staging_snapshot
            .schemas
            .insert((object_id.database.clone(), object_id.schema.clone()));
    }

    // Write deployment state to database (environment=stage_name for staging, promoted_at=None)
    project::deployment_snapshot::write_to_database(
        &client,
        &staging_snapshot,
        &stage_name,
        &metadata,
        None,
    )
    .await?;

    println!(
        "\n{} Successfully deployed {} objects to staging environment '{}'",
        "SUCCESS:".green().bold(),
        success_count,
        stage_name.cyan()
    );

    Ok(())
}
