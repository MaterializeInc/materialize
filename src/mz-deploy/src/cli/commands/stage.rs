//! Stage command - deploy to staging environment with renamed schemas and clusters.

use crate::cli::{CliError, helpers};
use crate::client::{ClusterOptions, Profile};
use crate::project::changeset::ChangeSet;
use crate::project::object_id::ObjectId;
use crate::project::planned::extract_external_indexes;
use crate::project::typed::FullyQualifiedName;
use crate::project::{self, normalize::NormalizingVisitor};
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
/// * `no_rollback` - Skip automatic rollback on failure (for debugging)
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
    no_rollback: bool,
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
    project::deployment_snapshot::initialize_deployment_table(&client).await?;

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

    // Collect deployment metadata and write to database BEFORE creating resources
    // This allows abort logic to clean up even if resource creation fails
    println!("Recording deployment metadata...");
    let metadata = helpers::collect_deployment_metadata(&client, directory).await;

    // Build a snapshot containing all objects that will be deployed
    let mut staging_snapshot = project::deployment_snapshot::DeploymentSnapshot::default();
    for (object_id, typed_obj) in &objects {
        let hash = project::deployment_snapshot::compute_typed_hash(typed_obj);
        staging_snapshot.objects.insert(object_id.clone(), hash);

        // Track which schema this object belongs to
        staging_snapshot
            .schemas
            .insert((object_id.database.clone(), object_id.schema.clone()));
    }

    // Write deployment state to database BEFORE creating resources
    // (environment=stage_name for staging, promoted_at=None)
    project::deployment_snapshot::write_to_database(
        &client,
        &staging_snapshot,
        &stage_name,
        &metadata,
        None,
    )
    .await?;

    verbose!("Deployment metadata recorded");

    // Perform resource creation with automatic rollback on failure
    let result = create_resources_with_rollback(
        &client,
        &stage_name,
        &staging_suffix,
        &schema_set,
        &cluster_set,
        &planned_project,
        &objects,
        no_rollback,
    )
    .await;

    match result {
        Ok(success_count) => {
            println!(
                "\n{} Successfully deployed {} objects to staging environment '{}'",
                "SUCCESS:".green().bold(),
                success_count,
                stage_name.cyan()
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Create staging resources (schemas, clusters, objects) with automatic rollback on failure.
///
/// This function performs all resource creation and automatically triggers rollback
/// on failure unless the no_rollback flag is set.
#[allow(clippy::too_many_arguments)]
async fn create_resources_with_rollback<'a>(
    client: &crate::client::Client,
    stage_name: &str,
    staging_suffix: &str,
    schema_set: &HashSet<(String, String)>,
    cluster_set: &HashSet<String>,
    planned_project: &'a project::planned::Project,
    objects: &'a [(ObjectId, &'a project::typed::DatabaseObject)],
    no_rollback: bool,
) -> Result<usize, CliError> {
    // Wrap resource creation in a closure that we can call and handle errors from
    let create_result = async {
        // Create staging schemas
        println!("Creating staging schemas...");
        for (database, schema) in schema_set {
            let staging_schema = format!("{}{}", schema, staging_suffix);
            client.create_schema(database, &staging_schema).await?;
            verbose!("  Created schema {}.{}", database, staging_schema);
        }
        verbose!();

        // Write cluster mappings to deploy.clusters table BEFORE creating clusters
        // This allows abort logic to clean up even if cluster creation fails
        let cluster_names: Vec<String> = cluster_set.iter().cloned().collect();
        client
            .insert_deployment_clusters(stage_name, &cluster_names)
            .await?;
        verbose!("Cluster mappings recorded");

        // Create staging clusters (by cloning production cluster configs)
        println!("Creating staging clusters ...");
        for prod_cluster in cluster_set {
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
            NormalizingVisitor::staging(&fqn, staging_suffix.to_string(), &empty, None);
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
                staging_suffix.to_string(),
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

        // Return success count
        Ok::<usize, CliError>(success_count)
    }
    .await;

    // Handle result with rollback on failure
    match create_result {
        Ok(count) => Ok(count),
        Err(e) => {
            if no_rollback {
                eprintln!("\n{} Deployment failed (skipping rollback due to --no-rollback flag)", "ERROR:".red().bold());
                return Err(e);
            }

            eprintln!("\n{} Deployment failed, rolling back...", "ERROR:".red().bold());
            let (schemas, clusters) = rollback_staging_resources(client, stage_name).await;

            if schemas > 0 || clusters > 0 {
                eprintln!("Rolled back: {} schema(s), {} cluster(s)", schemas, clusters);
            }

            Err(e)
        }
    }
}

/// Rollback staging resources on deployment failure.
///
/// This function performs best-effort cleanup of staging resources created during
/// a failed deployment. It mirrors the abort command logic but uses a best-effort
/// approach where cleanup failures are logged rather than returning errors.
///
/// # Arguments
/// * `client` - Database client
/// * `environment` - Staging environment name
///
/// # Returns
/// Number of schemas and clusters that were cleaned up (for summary message)
async fn rollback_staging_resources(
    client: &crate::client::Client,
    environment: &str,
) -> (usize, usize) {
    // Get staging resources using pattern matching (same as abort command)
    let staging_schemas = match client.get_staging_schemas(environment).await {
        Ok(schemas) => schemas,
        Err(e) => {
            verbose!("Warning: Failed to query staging schemas: {}", e);
            vec![]
        }
    };

    let staging_clusters = match client.get_staging_clusters(environment).await {
        Ok(clusters) => clusters,
        Err(e) => {
            verbose!("Warning: Failed to query staging clusters: {}", e);
            vec![]
        }
    };

    let schema_count = staging_schemas.len();
    let cluster_count = staging_clusters.len();

    // Drop staging schemas (best-effort)
    if !staging_schemas.is_empty() {
        verbose!("Dropping staging schemas...");
        match client.drop_staging_schemas(&staging_schemas).await {
            Ok(()) => {
                for (database, schema) in &staging_schemas {
                    verbose!("  Dropped {}.{}", database, schema);
                }
            }
            Err(e) => {
                verbose!("Warning: Failed to drop some schemas: {}", e);
            }
        }
    }

    // Drop staging clusters (best-effort)
    if !staging_clusters.is_empty() {
        verbose!("Dropping staging clusters...");
        match client.drop_staging_clusters(&staging_clusters).await {
            Ok(()) => {
                for cluster in &staging_clusters {
                    verbose!("  Dropped {}", cluster);
                }
            }
            Err(e) => {
                verbose!("Warning: Failed to drop some clusters: {}", e);
            }
        }
    }

    // Delete deployment records (best-effort)
    verbose!("Deleting deployment records...");
    if let Err(e) = client.delete_deployment_clusters(environment).await {
        verbose!("Warning: Failed to delete cluster records: {}", e);
    }

    if let Err(e) = client.delete_deployment(environment).await {
        verbose!("Warning: Failed to delete deployment records: {}", e);
    }

    (schema_count, cluster_count)
}
