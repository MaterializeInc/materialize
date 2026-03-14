//! Read-only catalog introspection queries.
//!
//! Methods on [`IntrospectionClient`] query the `mz_catalog` and
//! `information_schema` to inspect the live environment without modifying it.
//! Provides batch existence checks for schemas, clusters, and objects, as well
//! as dependency lookups used during deployment planning and sink repointing.

use crate::client::connection::{Client, IntrospectionClient};
use crate::client::errors::ConnectionError;
use crate::client::models::{Cluster, ClusterConfig, ClusterOptions, ClusterReplica, ObjectGrant};
use crate::client::quote_identifier;
use crate::client::sql_placeholders;
use crate::project::SchemaQualifier;
use crate::project::object_id::ObjectId;
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::types::ToSql;

/// A sink that depends on an object in a schema being dropped.
///
/// Used during apply to identify sinks that need to be repointed to new
/// upstream objects before the old schemas are dropped with CASCADE.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DependentSink {
    pub sink_database: String,
    pub sink_schema: String,
    pub sink_name: String,
    pub dependency_database: String,
    pub dependency_schema: String,
    pub dependency_name: String,
    pub dependency_type: String,
}

/// Check if a schema exists in the specified database.
pub async fn schema_exists(
    client: &Client,
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

    let row = client.query_one(query, &[&schema, &database]).await?;

    Ok(row.get("exists"))
}

/// Check if a cluster exists.
pub async fn cluster_exists(client: &Client, name: &str) -> Result<bool, ConnectionError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1 FROM mz_catalog.mz_clusters WHERE name = $1
        ) AS exists
    "#;

    let row = client.query_one(query, &[&name]).await?;

    Ok(row.get("exists"))
}

/// Get a cluster by name.
pub async fn get_cluster(client: &Client, name: &str) -> Result<Option<Cluster>, ConnectionError> {
    let query = r#"
        SELECT
            id,
            name,
            size,
            replication_factor::bigint AS replication_factor
        FROM mz_catalog.mz_clusters
        WHERE name = $1
    "#;

    let rows = client.query(query, &[&name]).await?;

    if rows.is_empty() {
        return Ok(None);
    }

    let row = &rows[0];
    Ok(Some(Cluster {
        id: row.get("id"),
        name: row.get("name"),
        size: row.get("size"),
        replication_factor: row.get("replication_factor"),
    }))
}

/// List all clusters.
pub async fn list_clusters(client: &Client) -> Result<Vec<Cluster>, ConnectionError> {
    let query = r#"
        SELECT
            id,
            name,
            size,
            replication_factor
        FROM mz_catalog.mz_clusters
        ORDER BY name
    "#;

    let rows = client.query(query, &[]).await?;

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
    client: &Client,
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

    let cluster_rows = client.query(cluster_query, &[&name]).await?;

    if cluster_rows.is_empty() {
        return Ok(None);
    }

    // Extract cluster-level info from first row
    let first_row = &cluster_rows[0];
    let managed: bool = first_row.get("managed");
    let size: Option<String> = first_row.get("size");
    let replication_factor: Option<i64> = first_row.get("replication_factor");

    // Query 2: Get grants (excluding owner's implicit privileges)
    let grants_query = r#"
        WITH cluster_privilege AS (
            SELECT mz_internal.mz_aclexplode(privileges).*, owner_id
            FROM mz_clusters
            WHERE name = $1
        )
        SELECT
            grantee.name AS grantee,
            c.privilege_type
        FROM cluster_privilege AS c
        JOIN mz_roles AS grantee ON c.grantee = grantee.id
        WHERE grantee.name NOT IN ('none', 'mz_system', 'mz_support')
          AND c.grantee != c.owner_id
    "#;

    let grant_rows = client.query(grants_query, &[&name]).await?;

    let grants: Vec<ObjectGrant> = grant_rows
        .iter()
        .map(|row| ObjectGrant {
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

/// Check if a network policy exists.
pub async fn network_policy_exists(client: &Client, name: &str) -> Result<bool, ConnectionError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1 FROM mz_catalog.mz_network_policies WHERE name = $1
        ) AS exists
    "#;

    let row = client.query_one(query, &[&name]).await?;

    Ok(row.get("exists"))
}

/// Check if a role exists.
pub async fn role_exists(client: &Client, name: &str) -> Result<bool, ConnectionError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1 FROM mz_catalog.mz_roles WHERE name = $1
        ) AS exists
    "#;

    let row = client.query_one(query, &[&name]).await?;

    Ok(row.get("exists"))
}

/// Get the members granted to a role.
pub async fn get_role_members(
    client: &Client,
    role_name: &str,
) -> Result<Vec<String>, ConnectionError> {
    let query = r#"
        SELECT m.name AS member
        FROM mz_catalog.mz_role_members rm
        JOIN mz_catalog.mz_roles r ON r.id = rm.role_id
        JOIN mz_catalog.mz_roles m ON m.id = rm.member
        WHERE r.name = $1
        ORDER BY m.name
    "#;

    let rows = client.query(query, &[&role_name]).await?;

    Ok(rows.iter().map(|row| row.get("member")).collect())
}

/// Get session default parameter names for a role.
pub async fn get_role_parameters(
    client: &Client,
    role_name: &str,
) -> Result<Vec<String>, ConnectionError> {
    let query = r#"
        SELECT rp.parameter_name
        FROM mz_catalog.mz_role_parameters rp
        JOIN mz_catalog.mz_roles r ON r.id = rp.role_id
        WHERE r.name = $1
        ORDER BY rp.parameter_name
    "#;

    let rows = client.query(query, &[&role_name]).await?;

    Ok(rows.iter().map(|row| row.get("parameter_name")).collect())
}

/// Get the current Materialize user/role.
pub async fn get_current_user(client: &Client) -> Result<String, ConnectionError> {
    let row = client.query_one("SELECT current_user()", &[]).await?;

    Ok(row.get(0))
}

/// Check which schemas from a set of (database, schema) pairs exist.
///
/// Returns a BTreeSet of (database, schema) tuples that exist.
pub async fn check_schemas_exist(
    client: &Client,
    schemas: &[(String, String)],
) -> Result<BTreeSet<(String, String)>, ConnectionError> {
    if schemas.is_empty() {
        return Ok(BTreeSet::new());
    }

    // Build FQN strings and a lookup map from FQN -> original tuple (reusing the same strings)
    let fqns: Vec<String> = schemas
        .iter()
        .map(|(db, schema)| format!("{}.{}", db, schema))
        .collect();

    let fqn_map: BTreeMap<&str, &(String, String)> = fqns
        .iter()
        .zip_eq(schemas.iter())
        .map(|(fqn, pair)| (fqn.as_str(), pair))
        .collect();

    let placeholders_str = sql_placeholders(fqns.len());

    let query = format!(
        r#"
        SELECT d.name || '.' || s.name as fqn
        FROM mz_catalog.mz_schemas s
        JOIN mz_catalog.mz_databases d ON s.database_id = d.id
        WHERE d.name || '.' || s.name IN ({})
        ORDER BY fqn
    "#,
        placeholders_str
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for fqn in &fqns {
        params.push(fqn);
    }

    let rows = client.query(&query, &params).await?;

    let mut existing = BTreeSet::new();
    for row in rows {
        let fqn: String = row.get("fqn");
        if let Some(pair) = fqn_map.get(fqn.as_str()) {
            existing.insert((*pair).clone());
        }
    }

    Ok(existing)
}

/// Check which clusters from a set of names exist.
///
/// Returns a BTreeSet of cluster names that exist.
pub async fn check_clusters_exist(
    client: &Client,
    clusters: &[String],
) -> Result<BTreeSet<String>, ConnectionError> {
    if clusters.is_empty() {
        return Ok(BTreeSet::new());
    }

    let placeholders_str = sql_placeholders(clusters.len());

    let query = format!(
        r#"
        SELECT name FROM mz_catalog.mz_clusters
        WHERE name IN ({})
        ORDER BY name
    "#,
        placeholders_str
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for name in clusters {
        params.push(name);
    }

    let rows = client.query(&query, &params).await?;

    Ok(rows.iter().map(|row| row.get("name")).collect())
}

/// Check which objects from a set exist in the production database.
///
/// Returns fully-qualified names of objects that exist.
pub async fn check_objects_exist(
    client: &Client,
    objects: &BTreeSet<ObjectId>,
) -> Result<Vec<String>, ConnectionError> {
    let fqns: Vec<String> = objects.iter().map(|o| o.to_string()).collect();
    if fqns.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders_str = sql_placeholders(fqns.len());

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

    let rows = client.query(&query, &params).await?;

    Ok(rows.iter().map(|row| row.get("fqn")).collect())
}

/// Check which objects from the given set exist in a specific catalog table.
///
/// Returns a BTreeSet of ObjectIds for objects that already exist.
async fn check_catalog_objects_exist(
    client: &Client,
    objects: &BTreeSet<ObjectId>,
    catalog_table: &str,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    if objects.is_empty() {
        return Ok(BTreeSet::new());
    }

    // Build a lookup map from FQN string -> ObjectId for O(1) matching
    let fqn_map: BTreeMap<String, &ObjectId> = objects.iter().map(|o| (o.to_string(), o)).collect();
    let fqns: Vec<&String> = fqn_map.keys().collect();

    let placeholders_str = sql_placeholders(fqns.len());

    let query = format!(
        r#"
        SELECT d.name || '.' || s.name || '.' || t.name as fqn
        FROM {} t
        JOIN mz_schemas s ON t.schema_id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        WHERE d.name || '.' || s.name || '.' || t.name IN ({})
        ORDER BY fqn
    "#,
        catalog_table, placeholders_str
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for fqn in &fqns {
        params.push(*fqn);
    }

    let rows = client.query(&query, &params).await?;

    let mut existing = BTreeSet::new();
    for row in rows {
        let fqn: String = row.get("fqn");
        if let Some(obj_id) = fqn_map.get(&fqn) {
            existing.insert((*obj_id).clone());
        }
    }

    Ok(existing)
}

/// Check which tables from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for tables that already exist.
pub async fn check_tables_exist(
    client: &Client,
    tables: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, tables, "mz_tables").await
}

/// Check which sources from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for sources that already exist.
pub async fn check_sources_exist(
    client: &Client,
    sources: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, sources, "mz_sources").await
}

/// Check which secrets from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for secrets that already exist.
pub async fn check_secrets_exist(
    client: &Client,
    secrets: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, secrets, "mz_secrets").await
}

/// Check which connections from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for connections that already exist.
pub async fn check_connections_exist(
    client: &Client,
    connections: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, connections, "mz_connections").await
}

/// Check which sinks from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for sinks that already exist.
/// Used during apply to skip creating sinks that already exist (like tables).
pub async fn check_sinks_exist(
    client: &Client,
    sinks: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, sinks, "mz_sinks").await
}

/// Find sinks that depend on objects in the specified schemas.
///
/// This is used during apply to identify sinks that need to be repointed
/// before old schemas are dropped with CASCADE. Only returns sinks whose
/// upstream object (FROM clause) is in one of the specified schemas.
pub async fn find_sinks_depending_on_schemas(
    client: &Client,
    schemas: &[SchemaQualifier],
) -> Result<Vec<DependentSink>, ConnectionError> {
    if schemas.is_empty() {
        return Ok(Vec::new());
    }

    // Build WHERE clause for (database, schema) pairs
    let mut conditions = Vec::new();
    let mut param_idx = 1;

    for _ in schemas {
        conditions.push(format!(
            "(dep_db.name = ${} AND dep_schema.name = ${})",
            param_idx,
            param_idx + 1
        ));
        param_idx += 2;
    }

    let where_clause = conditions.join(" OR ");

    let query = format!(
        r#"
        SELECT
            sink_db.name as sink_database,
            sink_schema.name as sink_schema,
            sinks.name as sink_name,
            dep_db.name as dependency_database,
            dep_schema.name as dependency_schema,
            dep_obj.name as dependency_name,
            dep_obj.type as dependency_type
        FROM mz_sinks sinks
        JOIN mz_schemas sink_schema ON sinks.schema_id = sink_schema.id
        JOIN mz_databases sink_db ON sink_schema.database_id = sink_db.id
        JOIN mz_internal.mz_object_dependencies deps ON sinks.id = deps.object_id
        JOIN mz_objects dep_obj ON deps.referenced_object_id = dep_obj.id
        JOIN mz_schemas dep_schema ON dep_obj.schema_id = dep_schema.id
        JOIN mz_databases dep_db ON dep_schema.database_id = dep_db.id
        WHERE ({})
          AND dep_obj.type IN ('materialized-view', 'table', 'source')
        ORDER BY sink_db.name, sink_schema.name, sinks.name
        "#,
        where_clause
    );

    // Build params vector with references to the schema tuples
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for sq in schemas {
        params.push(&sq.database);
        params.push(&sq.schema);
    }

    let rows = client.query(&query, &params).await?;

    Ok(rows
        .iter()
        .map(|row| DependentSink {
            sink_database: row.get("sink_database"),
            sink_schema: row.get("sink_schema"),
            sink_name: row.get("sink_name"),
            dependency_database: row.get("dependency_database"),
            dependency_schema: row.get("dependency_schema"),
            dependency_name: row.get("dependency_name"),
            dependency_type: row.get("dependency_type"),
        })
        .collect())
}

/// Check if a connection exists in the specified database and schema.
pub async fn check_connection_exists(
    client: &Client,
    database: &str,
    schema: &str,
    name: &str,
) -> Result<bool, ConnectionError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1
            FROM mz_catalog.mz_connections c
            JOIN mz_catalog.mz_schemas s ON c.schema_id = s.id
            JOIN mz_catalog.mz_databases d ON s.database_id = d.id
            WHERE d.name = $1 AND s.name = $2 AND c.name = $3
        ) AS exists
    "#;
    let row = client
        .query_one(query, &[&database, &schema, &name])
        .await?;
    Ok(row.get("exists"))
}

/// Check if an object (MV, table, source) exists in the specified schema.
///
/// Used to verify that a replacement object exists before repointing a sink.
pub async fn object_exists(
    client: &Client,
    database: &str,
    schema: &str,
    object: &str,
) -> Result<bool, ConnectionError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1 FROM mz_objects o
            JOIN mz_schemas s ON o.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name = $1 AND s.name = $2 AND o.name = $3
              AND o.type IN ('materialized-view', 'table', 'source')
        ) AS exists
    "#;

    let row = client
        .query_one(query, &[&database, &schema, &object])
        .await?;

    Ok(row.get("exists"))
}

/// Get staging schema names for a specific deployment.
pub async fn get_staging_schemas(
    client: &Client,
    deploy_id: &str,
) -> Result<Vec<SchemaQualifier>, ConnectionError> {
    let suffix = format!("_{}", deploy_id);
    let pattern = format!("%{}", suffix);

    let query = r#"
        SELECT d.name as database, s.name as schema
        FROM mz_schemas s
        JOIN mz_databases d ON s.database_id = d.id
        WHERE s.name LIKE $1
    "#;

    let rows = client.query(query, &[&pattern]).await?;

    Ok(rows
        .iter()
        .map(|row| {
            let database: String = row.get("database");
            let schema: String = row.get("schema");
            SchemaQualifier::new(database, schema)
        })
        .collect())
}

/// Get staging cluster names for a specific deployment.
pub async fn get_staging_clusters(
    client: &Client,
    deploy_id: &str,
) -> Result<Vec<String>, ConnectionError> {
    let suffix = format!("_{}", deploy_id);
    let pattern = format!("%{}", suffix);

    let query = r#"
        SELECT name
        FROM mz_clusters
        WHERE name LIKE $1
    "#;

    let rows = client.query(query, &[&pattern]).await?;

    Ok(rows.iter().map(|row| row.get("name")).collect())
}

/// Map a Materialize object type string to its DROP keyword.
fn mz_type_to_drop_keyword(obj_type: &str) -> Option<&'static str> {
    match obj_type {
        "table" => Some("TABLE"),
        "view" => Some("VIEW"),
        "materialized-view" => Some("MATERIALIZED VIEW"),
        "source" => Some("SOURCE"),
        "sink" => Some("SINK"),
        _ => None,
    }
}

/// Drop all objects in a schema.
///
/// Returns the fully-qualified names of dropped objects.
pub async fn drop_schema_objects(
    client: &Client,
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

    let rows = client.query(query, &[&database, &schema]).await?;

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
        let Some(drop_type) = mz_type_to_drop_keyword(obj_type.as_str()) else {
            continue;
        };

        let drop_sql = format!("DROP {} IF EXISTS {} CASCADE", drop_type, fqn);
        client.execute(&drop_sql, &[]).await?;

        dropped.push(fqn);
    }

    Ok(dropped)
}

/// Drop specific objects by their ObjectIds.
///
/// Returns the fully-qualified names of dropped objects.
pub async fn drop_objects(
    client: &Client,
    objects: &BTreeSet<ObjectId>,
) -> Result<Vec<String>, ConnectionError> {
    let mut dropped = Vec::new();

    if objects.is_empty() {
        return Ok(dropped);
    }

    let placeholders_str = sql_placeholders(objects.len());

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

    let rows = client.query(&query, &params).await?;

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
        let Some(drop_type) = mz_type_to_drop_keyword(obj_type.as_str()) else {
            continue;
        };

        let drop_sql = format!("DROP {} IF EXISTS {} CASCADE", drop_type, fqn);
        client.execute(&drop_sql, &[]).await?;

        dropped.push(fqn);
    }

    Ok(dropped)
}

/// Drop staging schemas by name.
pub async fn drop_staging_schemas(
    client: &Client,
    schemas: &[SchemaQualifier],
) -> Result<(), ConnectionError> {
    for sq in schemas {
        let drop_sql = format!(
            "DROP SCHEMA IF EXISTS {}.{} CASCADE",
            quote_identifier(&sq.database),
            quote_identifier(&sq.schema)
        );
        client.execute(&drop_sql, &[]).await?;
    }

    Ok(())
}

/// Drop staging clusters by name.
pub async fn drop_staging_clusters(
    client: &Client,
    clusters: &[String],
) -> Result<(), ConnectionError> {
    for cluster in clusters {
        let drop_sql = format!(
            "DROP CLUSTER IF EXISTS {} CASCADE",
            quote_identifier(cluster)
        );
        client.execute(&drop_sql, &[]).await?;
    }

    Ok(())
}

/// Get privilege grants on a named infrastructure object (cluster, network policy).
///
/// `catalog_table` is the system catalog table (e.g., `"mz_clusters"`,
/// `"mz_network_policies"`). Returns `(grantee, privilege_type)` pairs from
/// `mz_aclexplode`, filtering out system roles.
async fn get_named_object_grants(
    client: &Client,
    catalog_table: &str,
    name: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    let query = format!(
        r#"
        -- Explode the ACL bitmap into individual (grantee, privilege_type) rows.
        -- Each object stores privileges as a compact bitmap; mz_aclexplode unpacks it.
        WITH privilege AS (
            SELECT mz_internal.mz_aclexplode(privileges).*, owner_id
            FROM {}
            WHERE name = $1
        )
        SELECT
            grantee.name AS grantee,
            p.privilege_type
        FROM privilege AS p
        -- Resolve grantee role IDs to human-readable names.
        JOIN mz_roles AS grantee ON p.grantee = grantee.id
        -- Exclude system roles that are not user-manageable.
        WHERE grantee.name NOT IN ('none', 'mz_system', 'mz_support')
          -- Owners implicitly have all privileges; don't surface those as explicit grants.
          AND p.grantee != p.owner_id
        "#,
        catalog_table
    );

    let rows = client.query(&query, &[&name]).await?;

    Ok(rows
        .iter()
        .map(|row| ObjectGrant {
            grantee: row.get("grantee"),
            privilege_type: row.get("privilege_type"),
        })
        .collect())
}

/// Get privilege grants on a cluster by name.
pub async fn get_cluster_grants(
    client: &Client,
    name: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    get_named_object_grants(client, "mz_clusters", name).await
}

/// Get privilege grants on a network policy by name.
pub async fn get_network_policy_grants(
    client: &Client,
    name: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    get_named_object_grants(client, "mz_network_policies", name).await
}

/// Get privilege grants on a database object (table, source, secret, connection).
///
/// `catalog_table` is the system catalog table name (e.g., `"mz_tables"`, `"mz_secrets"`).
pub async fn get_database_object_grants(
    client: &Client,
    catalog_table: &str,
    database: &str,
    schema: &str,
    name: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    let query = format!(
        r#"
        -- Locate the object by its fully-qualified name (database.schema.object)
        -- using a 3-table join chain: catalog_table -> mz_schemas -> mz_databases.
        -- Then explode the ACL bitmap into individual privilege rows.
        WITH privilege AS (
            SELECT mz_internal.mz_aclexplode(t.privileges).*, t.owner_id
            FROM {} t
            JOIN mz_schemas s ON t.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name = $1 AND s.name = $2 AND t.name = $3
        )
        SELECT
            grantee.name AS grantee,
            p.privilege_type
        FROM privilege AS p
        JOIN mz_roles AS grantee ON p.grantee = grantee.id
        WHERE grantee.name NOT IN ('none', 'mz_system', 'mz_support')
          AND p.grantee != p.owner_id
        "#,
        catalog_table
    );

    let rows = client.query(&query, &[&database, &schema, &name]).await?;

    Ok(rows
        .iter()
        .map(|row| ObjectGrant {
            grantee: row.get("grantee"),
            privilege_type: row.get("privilege_type"),
        })
        .collect())
}

/// Get default privilege grants for a named infrastructure object (cluster, network policy).
///
/// Queries `mz_default_privileges` to find grants that would be auto-applied
/// to the given object based on its owner and any PUBLIC default privileges.
/// These grants should be protected from revocation during reconciliation.
async fn get_default_privilege_grants_for_named_object(
    client: &Client,
    catalog_table: &str,
    name: &str,
    object_type: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    let query = format!(
        r#"
        -- Query default privileges from ALTER DEFAULT PRIVILEGES rules.
        -- These are auto-applied grants that should be protected from revocation.
        SELECT
            grantee_role.name AS grantee,
            dp_priv.privilege_type
        FROM mz_default_privileges dp
        -- Expand the privilege bitmap into individual privilege type strings.
        CROSS JOIN LATERAL unnest(
            mz_internal.mz_format_privileges(dp.privileges)
        ) AS dp_priv(privilege_type)
        JOIN {} obj ON obj.name = $1
        JOIN mz_roles AS grantee_role ON dp.grantee = grantee_role.id
        WHERE dp.object_type = $2
          -- Match rules targeting the object's owner, or PUBLIC ('p') rules
          -- that apply to all owners.
          AND (dp.role_id = obj.owner_id OR dp.role_id = 'p')
          -- Named objects (clusters, network policies) are not schema-scoped,
          -- so only global default privileges (both NULL) apply.
          AND dp.database_id IS NULL
          AND dp.schema_id IS NULL
          AND grantee_role.name NOT IN ('none', 'mz_system', 'mz_support')
        "#,
        catalog_table
    );

    let rows = client.query(&query, &[&name, &object_type]).await?;

    Ok(rows
        .iter()
        .map(|row| ObjectGrant {
            grantee: row.get("grantee"),
            privilege_type: row.get("privilege_type"),
        })
        .collect())
}

/// Get default privilege grants for a cluster by name.
pub async fn get_default_privilege_grants_for_cluster(
    client: &Client,
    name: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    get_default_privilege_grants_for_named_object(client, "mz_clusters", name, "cluster").await
}

/// Get default privilege grants for a network policy by name.
pub async fn get_default_privilege_grants_for_network_policy(
    client: &Client,
    name: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    get_default_privilege_grants_for_named_object(client, "mz_network_policies", name, "type").await
}

/// Get default privilege grants for a database object (table, source, secret, connection).
///
/// Queries `mz_default_privileges` to find grants that would be auto-applied
/// to the given object based on its owner, database, schema, and any PUBLIC
/// default privileges. These grants should be protected from revocation.
pub async fn get_default_privilege_grants_for_database_object(
    client: &Client,
    catalog_table: &str,
    database: &str,
    schema: &str,
    name: &str,
    object_type: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    let query = format!(
        r#"
        -- Query default privileges from ALTER DEFAULT PRIVILEGES rules
        -- for a schema-qualified database object.
        SELECT
            grantee_role.name AS grantee,
            dp_priv.privilege_type
        FROM mz_default_privileges dp
        -- Expand the privilege bitmap into individual privilege type strings.
        CROSS JOIN LATERAL unnest(
            mz_internal.mz_format_privileges(dp.privileges)
        ) AS dp_priv(privilege_type)
        -- Locate the object by FQN to determine its owner, database, and schema.
        JOIN {} obj ON obj.name = $3
        JOIN mz_schemas s ON obj.schema_id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        JOIN mz_roles AS grantee_role ON dp.grantee = grantee_role.id
        WHERE d.name = $1 AND s.name = $2
          AND dp.object_type = $4
          -- Match rules targeting the object's owner, or PUBLIC ('p') rules.
          AND (dp.role_id = obj.owner_id OR dp.role_id = 'p')
          -- Match both global rules (database_id IS NULL) and rules scoped to
          -- this specific database. Global rules apply to all databases.
          AND (dp.database_id IS NULL OR dp.database_id = d.id)
          -- Same for schema: global or scoped to this specific schema.
          AND (dp.schema_id IS NULL OR dp.schema_id = s.id)
          AND grantee_role.name NOT IN ('none', 'mz_system', 'mz_support')
        "#,
        catalog_table
    );

    let rows = client
        .query(&query, &[&database, &schema, &name, &object_type])
        .await?;

    Ok(rows
        .iter()
        .map(|row| ObjectGrant {
            grantee: row.get("grantee"),
            privilege_type: row.get("privilege_type"),
        })
        .collect())
}

/// Get the `CREATE CONNECTION` SQL for an existing connection.
///
/// Uses `SHOW CREATE CONNECTION` which returns the canonical, non-redacted SQL
/// including fully-qualified secret references. Returns `None` if the
/// connection does not exist.
pub async fn get_connection_create_sql(
    client: &Client,
    database: &str,
    schema: &str,
    name: &str,
) -> Result<Option<String>, ConnectionError> {
    let fqn = format!(
        "{}.{}.{}",
        quote_identifier(database),
        quote_identifier(schema),
        quote_identifier(name)
    );
    let query = format!("SHOW CREATE CONNECTION {}", fqn);
    let rows = client.query(&query, &[]).await?;
    Ok(rows.first().map(|row| row.get("create_sql")))
}

impl IntrospectionClient<'_> {
    /// Get the current Materialize user/role.
    pub async fn get_current_user(&self) -> Result<String, ConnectionError> {
        get_current_user(self.client).await
    }

    /// Check which objects from a set exist in the production database.
    pub async fn check_objects_exist(
        &self,
        objects: &BTreeSet<ObjectId>,
    ) -> Result<Vec<String>, ConnectionError> {
        check_objects_exist(self.client, objects).await
    }

    /// Check which objects from a set exist in a specific catalog table.
    pub async fn check_catalog_objects_exist(
        &self,
        objects: &BTreeSet<ObjectId>,
        catalog_table: &str,
    ) -> Result<BTreeSet<ObjectId>, ConnectionError> {
        check_catalog_objects_exist(self.client, objects, catalog_table).await
    }

    /// Check which tables from the given set exist in the database.
    pub async fn check_tables_exist(
        &self,
        tables: &BTreeSet<ObjectId>,
    ) -> Result<BTreeSet<ObjectId>, ConnectionError> {
        check_tables_exist(self.client, tables).await
    }

    /// Check which sources from the given set exist in the database.
    pub async fn check_sources_exist(
        &self,
        sources: &BTreeSet<ObjectId>,
    ) -> Result<BTreeSet<ObjectId>, ConnectionError> {
        check_sources_exist(self.client, sources).await
    }

    /// Check which secrets from the given set exist in the database.
    pub async fn check_secrets_exist(
        &self,
        secrets: &BTreeSet<ObjectId>,
    ) -> Result<BTreeSet<ObjectId>, ConnectionError> {
        check_secrets_exist(self.client, secrets).await
    }

    /// Check which connections from the given set exist in the database.
    pub async fn check_connections_exist(
        &self,
        connections: &BTreeSet<ObjectId>,
    ) -> Result<BTreeSet<ObjectId>, ConnectionError> {
        check_connections_exist(self.client, connections).await
    }

    /// Check which sinks from the given set exist in the database.
    pub async fn check_sinks_exist(
        &self,
        sinks: &BTreeSet<ObjectId>,
    ) -> Result<BTreeSet<ObjectId>, ConnectionError> {
        check_sinks_exist(self.client, sinks).await
    }

    /// Check which schemas from a set of (database, schema) pairs exist.
    pub async fn check_schemas_exist(
        &self,
        schemas: &[(String, String)],
    ) -> Result<BTreeSet<(String, String)>, ConnectionError> {
        check_schemas_exist(self.client, schemas).await
    }

    /// Check which clusters from a set of names exist.
    pub async fn check_clusters_exist(
        &self,
        clusters: &[String],
    ) -> Result<BTreeSet<String>, ConnectionError> {
        check_clusters_exist(self.client, clusters).await
    }

    /// Find sinks that depend on objects in the specified schemas.
    pub async fn find_sinks_depending_on_schemas(
        &self,
        schemas: &[SchemaQualifier],
    ) -> Result<Vec<DependentSink>, ConnectionError> {
        find_sinks_depending_on_schemas(self.client, schemas).await
    }

    /// Check if a connection exists in the specified database and schema.
    pub async fn check_connection_exists(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<bool, ConnectionError> {
        check_connection_exists(self.client, database, schema, name).await
    }

    /// Check if an object (MV, table, source) exists in the specified schema.
    pub async fn object_exists(
        &self,
        database: &str,
        schema: &str,
        object: &str,
    ) -> Result<bool, ConnectionError> {
        object_exists(self.client, database, schema, object).await
    }

    /// Get staging schema names for a specific deployment.
    pub async fn get_staging_schemas(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<SchemaQualifier>, ConnectionError> {
        get_staging_schemas(self.client, deploy_id).await
    }

    /// Get staging cluster names for a specific deployment.
    pub async fn get_staging_clusters(
        &self,
        deploy_id: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        get_staging_clusters(self.client, deploy_id).await
    }

    /// Drop all objects in a schema.
    pub async fn drop_schema_objects(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        drop_schema_objects(self.client, database, schema).await
    }

    /// Drop specific objects by their ObjectIds.
    pub async fn drop_objects(
        &self,
        objects: &BTreeSet<ObjectId>,
    ) -> Result<Vec<String>, ConnectionError> {
        drop_objects(self.client, objects).await
    }

    /// Drop staging schemas by name.
    pub async fn drop_staging_schemas(
        &self,
        schemas: &[SchemaQualifier],
    ) -> Result<(), ConnectionError> {
        drop_staging_schemas(self.client, schemas).await
    }

    /// Drop staging clusters by name.
    pub async fn drop_staging_clusters(&self, clusters: &[String]) -> Result<(), ConnectionError> {
        drop_staging_clusters(self.client, clusters).await
    }

    /// Check if a schema exists in the specified database.
    pub async fn schema_exists(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<bool, ConnectionError> {
        schema_exists(self.client, database, schema).await
    }

    /// Check if a network policy exists.
    pub async fn network_policy_exists(&self, name: &str) -> Result<bool, ConnectionError> {
        network_policy_exists(self.client, name).await
    }

    /// Check if a role exists.
    pub async fn role_exists(&self, name: &str) -> Result<bool, ConnectionError> {
        role_exists(self.client, name).await
    }

    /// Get the members granted to a role.
    pub async fn get_role_members(&self, name: &str) -> Result<Vec<String>, ConnectionError> {
        get_role_members(self.client, name).await
    }

    /// Get session default parameter names for a role.
    pub async fn get_role_parameters(&self, name: &str) -> Result<Vec<String>, ConnectionError> {
        get_role_parameters(self.client, name).await
    }

    /// Check if a cluster exists.
    pub async fn cluster_exists(&self, name: &str) -> Result<bool, ConnectionError> {
        cluster_exists(self.client, name).await
    }

    /// Get a cluster by name.
    pub async fn get_cluster(&self, name: &str) -> Result<Option<Cluster>, ConnectionError> {
        get_cluster(self.client, name).await
    }

    /// List all clusters.
    pub async fn list_clusters(&self) -> Result<Vec<Cluster>, ConnectionError> {
        list_clusters(self.client).await
    }

    /// Get cluster configuration including replicas and grants.
    pub async fn get_cluster_config(
        &self,
        name: &str,
    ) -> Result<Option<ClusterConfig>, ConnectionError> {
        get_cluster_config(self.client, name).await
    }

    /// Get privilege grants on a cluster by name.
    pub async fn get_cluster_grants(
        &self,
        name: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_cluster_grants(self.client, name).await
    }

    /// Get privilege grants on a network policy by name.
    pub async fn get_network_policy_grants(
        &self,
        name: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_network_policy_grants(self.client, name).await
    }

    /// Get privilege grants on a database object.
    pub async fn get_database_object_grants(
        &self,
        catalog_table: &str,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_database_object_grants(self.client, catalog_table, database, schema, name).await
    }

    /// Get the `CREATE CONNECTION` SQL for an existing connection.
    pub async fn get_connection_create_sql(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<String>, ConnectionError> {
        get_connection_create_sql(self.client, database, schema, name).await
    }

    /// Get default privilege grants for a cluster by name.
    pub async fn get_default_privilege_grants_for_cluster(
        &self,
        name: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_default_privilege_grants_for_cluster(self.client, name).await
    }

    /// Get default privilege grants for a network policy by name.
    pub async fn get_default_privilege_grants_for_network_policy(
        &self,
        name: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_default_privilege_grants_for_network_policy(self.client, name).await
    }

    /// Get default privilege grants for a database object.
    pub async fn get_default_privilege_grants_for_database_object(
        &self,
        catalog_table: &str,
        database: &str,
        schema: &str,
        name: &str,
        object_type: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_default_privilege_grants_for_database_object(
            self.client,
            catalog_table,
            database,
            schema,
            name,
            object_type,
        )
        .await
    }
}
