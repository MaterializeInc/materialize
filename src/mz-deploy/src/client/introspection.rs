//! Database introspection operations.
//!
//! This module contains methods for querying database metadata,
//! such as checking for existence of schemas, clusters, and objects.

use crate::client::errors::ConnectionError;
use crate::client::models::{Cluster, ClusterConfig, ClusterGrant, ClusterOptions, ClusterReplica};
use crate::project::object_id::ObjectId;
use crate::utils::sql_utils::quote_identifier;
use std::collections::BTreeSet;
use tokio_postgres::Client as PgClient;
use tokio_postgres::types::ToSql;

/// Check if a schema exists in the specified database.
pub async fn schema_exists(
    client: &PgClient,
    database: &str,
    schema: &str,
) -> Result<bool, ConnectionError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1
            FROM mz_catalog.mz_schemas s
            JOIN mz_catalog.mz_databases d ON s.database_id = d.id
            WHERE s.name = $1 AND d.name = $2
        ) AS exists
    "#;

    let row = client
        .query_one(query, &[&schema, &database])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(row.get("exists"))
}

/// Check if a cluster exists.
pub async fn cluster_exists(client: &PgClient, name: &str) -> Result<bool, ConnectionError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1 FROM mz_catalog.mz_clusters WHERE name = $1
        ) AS exists
    "#;

    let row = client
        .query_one(query, &[&name])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(row.get("exists"))
}

/// Get a cluster by name.
pub async fn get_cluster(
    client: &PgClient,
    name: &str,
) -> Result<Option<Cluster>, ConnectionError> {
    let query = r#"
        SELECT
            id,
            name,
            size,
            replication_factor
        FROM mz_catalog.mz_clusters
        WHERE name = $1
    "#;

    let rows = client
        .query(query, &[&name])
        .await
        .map_err(ConnectionError::Query)?;

    if rows.is_empty() {
        return Ok(None);
    }

    let row = &rows[0];
    let replication_factor: Option<i64> = row
        .try_get("replication_factor")
        .or_else(|_| {
            row.try_get::<_, Option<i32>>("replication_factor")
                .map(|v| v.map(i64::from))
        })
        .or_else(|_| {
            row.try_get::<_, Option<i16>>("replication_factor")
                .map(|v| v.map(i64::from))
        })
        .unwrap_or(None);

    Ok(Some(Cluster {
        id: row.get("id"),
        name: row.get("name"),
        size: row.get("size"),
        replication_factor,
    }))
}

/// List all clusters.
pub async fn list_clusters(client: &PgClient) -> Result<Vec<Cluster>, ConnectionError> {
    let query = r#"
        SELECT
            id,
            name,
            size,
            replication_factor
        FROM mz_catalog.mz_clusters
        ORDER BY name
    "#;

    let rows = client
        .query(query, &[])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(rows
        .iter()
        .map(|row| Cluster {
            id: row.get("id"),
            name: row.get("name"),
            size: row.get("size"),
            replication_factor: row.get("replication_factor"),
        })
        .collect())
}

/// Get cluster configuration including replicas and grants.
///
/// This fetches all information needed to clone a cluster's configuration:
/// - For managed clusters: size and replication factor
/// - For unmanaged clusters: replica configurations
/// - For both: privilege grants
pub async fn get_cluster_config(
    client: &PgClient,
    name: &str,
) -> Result<Option<ClusterConfig>, ConnectionError> {
    // Query 1: Get cluster info and replicas with LEFT JOIN
    let cluster_query = r#"
        SELECT
            c.id,
            c.name,
            c.managed,
            c.size,
            c.replication_factor,
            r.name AS replica_name,
            r.size AS replica_size,
            r.availability_zone
        FROM mz_catalog.mz_clusters c
        LEFT JOIN mz_catalog.mz_cluster_replicas r ON c.id = r.cluster_id
        WHERE c.name = $1
        ORDER BY r.name
    "#;

    let cluster_rows = client
        .query(cluster_query, &[&name])
        .await
        .map_err(ConnectionError::Query)?;

    if cluster_rows.is_empty() {
        return Ok(None);
    }

    // Extract cluster-level info from first row
    let first_row = &cluster_rows[0];
    let managed: bool = first_row.get("managed");
    let size: Option<String> = first_row.get("size");
    let replication_factor: Option<i64> = first_row
        .try_get("replication_factor")
        .or_else(|_| {
            first_row
                .try_get::<_, Option<i32>>("replication_factor")
                .map(|v| v.map(i64::from))
        })
        .or_else(|_| {
            first_row
                .try_get::<_, Option<i16>>("replication_factor")
                .map(|v| v.map(i64::from))
        })
        .unwrap_or(None);

    // Query 2: Get grants
    let grants_query = r#"
        WITH cluster_privilege AS (
            SELECT mz_internal.mz_aclexplode(privileges).*
            FROM mz_clusters
            WHERE name = $1
        )
        SELECT
            grantee.name AS grantee,
            c.privilege_type
        FROM cluster_privilege AS c
        JOIN mz_roles AS grantee ON c.grantee = grantee.id
        WHERE grantee.name NOT IN ('none', 'mz_system', 'mz_support')
    "#;

    let grant_rows = client
        .query(grants_query, &[&name])
        .await
        .map_err(ConnectionError::Query)?;

    let grants: Vec<ClusterGrant> = grant_rows
        .iter()
        .map(|row| ClusterGrant {
            grantee: row.get("grantee"),
            privilege_type: row.get("privilege_type"),
        })
        .collect();

    if managed {
        // Managed cluster
        let size = size.ok_or_else(|| {
            ConnectionError::Message(format!(
                "Managed cluster '{}' has no size (unexpected)",
                name
            ))
        })?;

        let replication_factor = replication_factor.unwrap_or(1).try_into().map_err(|_| {
            ConnectionError::Message(format!("Invalid replication_factor for cluster '{}'", name))
        })?;

        Ok(Some(ClusterConfig::Managed {
            options: ClusterOptions {
                size,
                replication_factor,
            },
            grants,
        }))
    } else {
        // Unmanaged cluster - collect replicas
        let mut replicas = Vec::new();
        for row in &cluster_rows {
            let replica_name: Option<String> = row.get("replica_name");
            if let Some(replica_name) = replica_name {
                replicas.push(ClusterReplica {
                    name: replica_name,
                    size: row.get("replica_size"),
                    availability_zone: row.get("availability_zone"),
                });
            }
        }

        Ok(Some(ClusterConfig::Unmanaged { replicas, grants }))
    }
}

/// Get the current Materialize user/role.
pub async fn get_current_user(client: &PgClient) -> Result<String, ConnectionError> {
    let row = client
        .query_one("SELECT current_user()", &[])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(row.get(0))
}

/// Check which objects from a set exist in the production database.
///
/// Returns fully-qualified names of objects that exist.
pub async fn check_objects_exist(
    client: &PgClient,
    objects: &BTreeSet<ObjectId>,
) -> Result<Vec<String>, ConnectionError> {
    let fqns: Vec<String> = objects.iter().map(|o| o.to_string()).collect();
    if fqns.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders: Vec<String> = (1..=fqns.len()).map(|i| format!("${}", i)).collect();
    let placeholders_str = placeholders.join(", ");

    let query = format!(
        r#"
        SELECT d.name || '.' || s.name || '.' || mo.name as fqn
        FROM mz_objects mo
        JOIN mz_schemas s ON mo.schema_id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        WHERE d.name || '.' || s.name || '.' || mo.name IN ({})
        AND mo.type IN ('table', 'view', 'materialized-view', 'source', 'sink')
        ORDER BY fqn
    "#,
        placeholders_str
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for fqn in &fqns {
        params.push(fqn);
    }

    let rows = client
        .query(&query, &params)
        .await
        .map_err(ConnectionError::Query)?;

    Ok(rows.iter().map(|row| row.get("fqn")).collect())
}

/// Check which tables from the given set exist in the database.
///
/// Returns a HashSet of ObjectIds for tables that already exist.
pub async fn check_tables_exist(
    client: &PgClient,
    tables: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    let fqns: Vec<String> = tables.iter().map(|o| o.to_string()).collect();
    if fqns.is_empty() {
        return Ok(BTreeSet::new());
    }

    let placeholders: Vec<String> = (1..=fqns.len()).map(|i| format!("${}", i)).collect();
    let placeholders_str = placeholders.join(", ");

    let query = format!(
        r#"
        SELECT d.name || '.' || s.name || '.' || t.name as fqn
        FROM mz_tables t
        JOIN mz_schemas s ON t.schema_id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        WHERE d.name || '.' || s.name || '.' || t.name IN ({})
        ORDER BY fqn
    "#,
        placeholders_str
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for fqn in &fqns {
        params.push(fqn);
    }

    let rows = client
        .query(&query, &params)
        .await
        .map_err(ConnectionError::Query)?;

    // Convert FQN strings back to ObjectIds
    let mut existing = BTreeSet::new();
    for row in rows {
        let fqn: String = row.get("fqn");
        // Find the matching ObjectId from the input set
        if let Some(obj_id) = tables.iter().find(|o| o.to_string() == fqn) {
            existing.insert(obj_id.clone());
        }
    }

    Ok(existing)
}

/// Check which sinks from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for sinks that already exist.
/// Used during apply to skip creating sinks that already exist (like tables).
pub async fn check_sinks_exist(
    client: &PgClient,
    sinks: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    let fqns: Vec<String> = sinks.iter().map(|o| o.to_string()).collect();
    if fqns.is_empty() {
        return Ok(BTreeSet::new());
    }

    let placeholders: Vec<String> = (1..=fqns.len()).map(|i| format!("${}", i)).collect();
    let placeholders_str = placeholders.join(", ");

    let query = format!(
        r#"
        SELECT d.name || '.' || s.name || '.' || k.name as fqn
        FROM mz_sinks k
        JOIN mz_schemas s ON k.schema_id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        WHERE d.name || '.' || s.name || '.' || k.name IN ({})
        ORDER BY fqn
    "#,
        placeholders_str
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for fqn in &fqns {
        params.push(fqn);
    }

    let rows = client
        .query(&query, &params)
        .await
        .map_err(ConnectionError::Query)?;

    // Convert FQN strings back to ObjectIds
    let mut existing = BTreeSet::new();
    for row in rows {
        let fqn: String = row.get("fqn");
        // Find the matching ObjectId from the input set
        if let Some(obj_id) = sinks.iter().find(|o| o.to_string() == fqn) {
            existing.insert(obj_id.clone());
        }
    }

    Ok(existing)
}

/// Get staging schema names for a specific deployment.
pub async fn get_staging_schemas(
    client: &PgClient,
    deploy_id: &str,
) -> Result<Vec<(String, String)>, ConnectionError> {
    let suffix = format!("_{}", deploy_id);
    let pattern = format!("%{}", suffix);

    let query = r#"
        SELECT d.name as database, s.name as schema
        FROM mz_schemas s
        JOIN mz_databases d ON s.database_id = d.id
        WHERE s.name LIKE $1
    "#;

    let rows = client
        .query(query, &[&pattern])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(rows
        .iter()
        .map(|row| {
            let database: String = row.get("database");
            let schema: String = row.get("schema");
            (database, schema)
        })
        .collect())
}

/// Get staging cluster names for a specific deployment.
pub async fn get_staging_clusters(
    client: &PgClient,
    deploy_id: &str,
) -> Result<Vec<String>, ConnectionError> {
    let suffix = format!("_{}", deploy_id);
    let pattern = format!("%{}", suffix);

    let query = r#"
        SELECT name
        FROM mz_clusters
        WHERE name LIKE $1
    "#;

    let rows = client
        .query(query, &[&pattern])
        .await
        .map_err(ConnectionError::Query)?;

    Ok(rows.iter().map(|row| row.get("name")).collect())
}

/// Drop all objects in a schema.
///
/// Returns the fully-qualified names of dropped objects.
pub async fn drop_schema_objects(
    client: &PgClient,
    database: &str,
    schema: &str,
) -> Result<Vec<String>, ConnectionError> {
    let query = r#"
        SELECT mo.name, mo.type
        FROM mz_objects mo
        JOIN mz_schemas s ON mo.schema_id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        WHERE d.name = $1 AND s.name = $2
        AND mo.type IN ('table', 'view', 'materialized-view', 'source', 'sink')
        ORDER BY mo.id DESC
    "#;

    let rows = client
        .query(query, &[&database, &schema])
        .await
        .map_err(ConnectionError::Query)?;

    let mut dropped = Vec::new();
    for row in rows {
        let name: String = row.get("name");
        let obj_type: String = row.get("type");

        let fqn = format!(
            "{}.{}.{}",
            quote_identifier(database),
            quote_identifier(schema),
            quote_identifier(&name)
        );
        let drop_type = match obj_type.as_str() {
            "table" => "TABLE",
            "view" => "VIEW",
            "materialized-view" => "MATERIALIZED VIEW",
            "source" => "SOURCE",
            "sink" => "SINK",
            _ => continue,
        };

        let drop_sql = format!("DROP {} IF EXISTS {} CASCADE", drop_type, fqn);
        client
            .execute(&drop_sql, &[])
            .await
            .map_err(ConnectionError::Query)?;

        dropped.push(fqn);
    }

    Ok(dropped)
}

/// Drop specific objects by their ObjectIds.
///
/// Returns the fully-qualified names of dropped objects.
pub async fn drop_objects(
    client: &PgClient,
    objects: &BTreeSet<ObjectId>,
) -> Result<Vec<String>, ConnectionError> {
    let mut dropped = Vec::new();

    if objects.is_empty() {
        return Ok(dropped);
    }

    let placeholders: Vec<String> = (1..=objects.len()).map(|i| format!("${}", i)).collect();
    let placeholders_str = placeholders.join(", ");

    let query = format!(
        r#"
        SELECT mo.name, s.name as schema_name, d.name as database_name, mo.type
        FROM mz_objects mo
        JOIN mz_schemas s ON mo.schema_id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        WHERE d.name || '.' || s.name || '.' || mo.name IN ({})
        AND mo.type IN ('table', 'view', 'materialized-view', 'source', 'sink')
        ORDER BY mo.id DESC
    "#,
        placeholders_str
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    let fqns: Vec<_> = objects.iter().map(|object| object.to_string()).collect();
    for fqn in &fqns {
        params.push(fqn);
    }

    let rows = client
        .query(&query, &params)
        .await
        .map_err(ConnectionError::Query)?;

    for row in rows {
        let name: String = row.get("name");
        let schema: String = row.get("schema_name");
        let database: String = row.get("database_name");
        let obj_type: String = row.get("type");

        let fqn = format!(
            "{}.{}.{}",
            quote_identifier(&database),
            quote_identifier(&schema),
            quote_identifier(&name)
        );
        let drop_type = match obj_type.as_str() {
            "table" => "TABLE",
            "view" => "VIEW",
            "materialized-view" => "MATERIALIZED VIEW",
            "source" => "SOURCE",
            "sink" => "SINK",
            _ => continue,
        };

        let drop_sql = format!("DROP {} IF EXISTS {} CASCADE", drop_type, fqn);
        client
            .execute(&drop_sql, &[])
            .await
            .map_err(ConnectionError::Query)?;

        dropped.push(fqn);
    }

    Ok(dropped)
}

/// Drop staging schemas by name.
pub async fn drop_staging_schemas(
    client: &PgClient,
    schemas: &[(String, String)],
) -> Result<(), ConnectionError> {
    for (database, schema) in schemas {
        let drop_sql = format!(
            "DROP SCHEMA IF EXISTS {}.{} CASCADE",
            quote_identifier(database),
            quote_identifier(schema)
        );
        client
            .execute(&drop_sql, &[])
            .await
            .map_err(ConnectionError::Query)?;
    }

    Ok(())
}

/// Drop staging clusters by name.
pub async fn drop_staging_clusters(
    client: &PgClient,
    clusters: &[String],
) -> Result<(), ConnectionError> {
    for cluster in clusters {
        let drop_sql = format!(
            "DROP CLUSTER IF EXISTS {} CASCADE",
            quote_identifier(cluster)
        );
        client
            .execute(&drop_sql, &[])
            .await
            .map_err(ConnectionError::Query)?;
    }

    Ok(())
}
