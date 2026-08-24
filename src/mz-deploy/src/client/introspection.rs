// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Read-only catalog introspection queries.
//!
//! Methods on [`IntrospectionClient`] query the `mz_catalog` and
//! `information_schema` to inspect the live environment without modifying it.
//! Provides batch existence checks for schemas, clusters, and objects, as well
//! as dependency lookups used during deployment planning and sink repointing.

use crate::client::connection::{Client, IntrospectionClient};
use crate::client::errors::ConnectionError;
use crate::client::models::{
    Cluster, ClusterConfig, ClusterReplica, DefaultPrivilege, ObjectComment, ObjectGrant,
};
use crate::client::sql_placeholders;
use crate::client::staging_suffix_like_pattern;
use crate::client::{parse_create_cluster, quote_identifier};
use crate::project::SchemaQualifier;
use crate::project::ir::object_id::ObjectId;
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::Row;
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

/// A `target` CTE holding the `(database, schema, object)` name triples a bulk
/// catalog read is restricted to.
///
/// The triples arrive as three parallel arrays bound to `$1`, `$2`, and `$3` by
/// [`array_literal`], and are zipped back into rows server-side. The statement
/// text is therefore identical however many objects a project manages, and a
/// query that needs parameters of its own numbers them from `$4` on.
///
/// The three name components are matched separately rather than against a
/// concatenated fully-qualified name. A concatenation would have to agree with
/// the catalog on when an identifier needs quoting, and a name containing a `.`
/// would make it ambiguous.
const TARGET_OBJECTS_CTE: &str = "\
    target AS (
        SELECT
            ($1::text::text[])[i] AS database,
            ($2::text::text[])[i] AS schema,
            ($3::text::text[])[i] AS object
        FROM generate_series(1, array_length($1::text::text[], 1)) AS g(i)
    )";

/// A `target` CTE holding the names a bulk read over a named-object catalog
/// table is restricted to.
///
/// Names arrive as one array bound to `$1` by [`array_literal`], so a query that
/// needs parameters of its own numbers them from `$2` on.
const TARGET_NAMES_CTE: &str = "\
    target AS (SELECT name FROM unnest($1::text::text[]) AS u(name))";

/// Encode names as a PostgreSQL array literal, to bind as the single `text`
/// parameter that a bulk read's `$n::text::text[]` casts to an array.
///
/// Materialize's pgwire cannot decode an array-typed bind parameter, so an array
/// has to travel as text and be cast server-side. Every element is double-quoted
/// with its `"` and `\` escaped, which makes the encoding total: a name holding
/// a comma, a brace, a quote, or nothing at all survives it unchanged.
fn array_literal<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut literal = String::from("{");
    for (i, value) in values.into_iter().enumerate() {
        if i > 0 {
            literal.push(',');
        }
        literal.push('"');
        for ch in value.chars() {
            if matches!(ch, '"' | '\\') {
                literal.push('\\');
            }
            literal.push(ch);
        }
        literal.push('"');
    }
    literal.push('}');
    literal
}

/// The three parallel name arrays [`TARGET_OBJECTS_CTE`] zips back together,
/// each encoded by [`array_literal`].
fn target_object_arrays(objects: &BTreeSet<ObjectId>) -> [String; 3] {
    [
        array_literal(objects.iter().map(ObjectId::expect_database)),
        array_literal(objects.iter().map(ObjectId::schema)),
        array_literal(objects.iter().map(ObjectId::object)),
    ]
}

/// Group rows carrying the `database`, `schema`, and `object` columns of
/// [`TARGET_OBJECTS_CTE`] by the object they describe.
///
/// An object the catalog holds no rows for is absent from the map rather than
/// present with an empty vector, so callers must treat a miss as "nothing
/// recorded".
fn group_by_object<T>(
    rows: &[Row],
    mut value: impl FnMut(&Row) -> T,
) -> BTreeMap<ObjectId, Vec<T>> {
    let mut grouped: BTreeMap<ObjectId, Vec<T>> = BTreeMap::new();
    for row in rows {
        let id = ObjectId::new(row.get("database"), row.get("schema"), row.get("object"));
        grouped.entry(id).or_default().push(value(row));
    }
    grouped
}

/// Group rows carrying the `name` column of [`TARGET_NAMES_CTE`] by the object
/// they describe. Absent means "nothing recorded", as in [`group_by_object`].
fn group_by_name<T>(rows: &[Row], mut value: impl FnMut(&Row) -> T) -> BTreeMap<String, Vec<T>> {
    let mut grouped: BTreeMap<String, Vec<T>> = BTreeMap::new();
    for row in rows {
        grouped.entry(row.get("name")).or_default().push(value(row));
    }
    grouped
}

/// Read an [`ObjectGrant`] out of a row of one of the privilege queries.
fn object_grant(row: &Row) -> ObjectGrant {
    ObjectGrant {
        grantee: row.get("grantee"),
        privilege_type: row.get("privilege_type"),
    }
}

/// Check if a schema exists in the specified database.
pub(super) async fn schema_exists(
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

/// Get a cluster by name.
pub(super) async fn get_cluster(
    client: &Client,
    name: &str,
) -> Result<Option<Cluster>, ConnectionError> {
    let query = r#"
        SELECT
            c.id,
            c.name,
            c.managed,
            c.size,
            c.replication_factor::bigint AS replication_factor
        FROM mz_catalog.mz_clusters c
        WHERE c.name = $1
    "#;

    let rows = client.query(query, &[&name]).await?;

    if rows.is_empty() {
        return Ok(None);
    }

    let row = &rows[0];
    Ok(Some(Cluster {
        id: row.get("id"),
        name: row.get("name"),
        managed: row.get("managed"),
        size: row.get("size"),
        replication_factor: row.get("replication_factor"),
    }))
}

/// Get the clusters named in `names`, keyed by cluster name.
///
/// A name with no cluster is absent from the map.
pub(super) async fn get_clusters(
    client: &Client,
    names: &[&str],
) -> Result<BTreeMap<String, Cluster>, ConnectionError> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }

    let query = r#"
        SELECT
            c.id,
            c.name,
            c.managed,
            c.size,
            c.replication_factor::bigint AS replication_factor
        FROM mz_catalog.mz_clusters c
        WHERE c.name = ANY($1::text::text[])
    "#;

    let rows = client
        .query(query, &[&array_literal(names.iter().copied())])
        .await?;

    Ok(rows
        .iter()
        .map(|row| {
            let cluster = Cluster {
                id: row.get("id"),
                name: row.get("name"),
                managed: row.get("managed"),
                size: row.get("size"),
                replication_factor: row.get("replication_factor"),
            };
            (cluster.name.clone(), cluster)
        })
        .collect())
}

/// The canonical `CREATE CLUSTER` statement for a cluster, as the server renders
/// it from the catalog. Returns `None` if the cluster does not exist.
///
/// Errors on unmanaged clusters.
pub(super) async fn get_cluster_create_sql(
    client: &Client,
    name: &str,
) -> Result<Option<String>, ConnectionError> {
    let query = format!("SHOW CREATE CLUSTER {}", quote_identifier(name));
    let rows = client.query(&query, &[]).await?;
    Ok(rows.first().map(|row| row.get("create_sql")))
}

/// The canonical `CREATE CLUSTER` statement for each of `names`, keyed by
/// cluster name.
///
/// `SHOW CREATE CLUSTER` names one cluster at a time, and no catalog relation
/// carries the statement it renders, so there is one statement per cluster no
/// matter what. Issuing them together lets the connection pipeline them, so the
/// whole set costs one round trip instead of one per cluster.
///
/// A name whose cluster does not exist is absent from the map. Errors on an
/// unmanaged cluster, which has no `SHOW CREATE CLUSTER` form.
pub(super) async fn get_cluster_create_sqls(
    client: &Client,
    names: &[&str],
) -> Result<BTreeMap<String, String>, ConnectionError> {
    let reads = names.iter().map(|name| async move {
        let sql = get_cluster_create_sql(client, name).await?;
        Ok::<_, ConnectionError>(sql.map(|sql| (name.to_string(), sql)))
    });

    Ok(futures::future::try_join_all(reads)
        .await?
        .into_iter()
        .flatten()
        .collect())
}

/// List all clusters.
pub(super) async fn list_clusters(client: &Client) -> Result<Vec<Cluster>, ConnectionError> {
    let query = r#"
        SELECT
            c.id,
            c.name,
            c.managed,
            c.size,
            c.replication_factor::bigint AS replication_factor
        FROM mz_catalog.mz_clusters c
        ORDER BY c.name
    "#;

    let rows = client.query(query, &[]).await?;

    Ok(rows
        .iter()
        .map(|row| Cluster {
            id: row.get("id"),
            name: row.get("name"),
            managed: row.get("managed"),
            size: row.get("size"),
            replication_factor: row.get("replication_factor"),
        })
        .collect())
}

/// Get cluster configuration including replicas and grants.
///
/// This fetches all information needed to clone a cluster's configuration:
/// - For managed clusters: the canonical `CREATE CLUSTER` statement
/// - For unmanaged clusters: replica configurations
/// - For both: privilege grants
pub(super) async fn get_cluster_config(
    client: &Client,
    name: &str,
) -> Result<Option<ClusterConfig>, ConnectionError> {
    // Query 1: Get cluster info and replicas with LEFT JOIN
    let cluster_query = r#"
        SELECT
            c.managed,
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

    let managed: bool = cluster_rows[0].get("managed");

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
        let create_sql = get_cluster_create_sql(client, name).await?.ok_or_else(|| {
            ConnectionError::Message(format!("Cluster '{}' has no CREATE statement", name))
        })?;
        let create_stmt = parse_create_cluster(&create_sql)
            .map_err(|e| ConnectionError::Message(format!("cluster '{}': {}", name, e)))?;

        Ok(Some(ClusterConfig::Managed {
            create_stmt,
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

/// Which of `names` name an existing object in `catalog_table`.
///
/// Used in place of a per-name `EXISTS` probe when a phase needs to know the
/// answer for a whole set of names.
async fn existing_names(
    client: &Client,
    catalog_table: &str,
    names: &[&str],
) -> Result<BTreeSet<String>, ConnectionError> {
    if names.is_empty() {
        return Ok(BTreeSet::new());
    }

    let query = format!(
        "SELECT o.name FROM {} o WHERE o.name = ANY($1::text::text[])",
        catalog_table
    );

    let rows = client
        .query(&query, &[&array_literal(names.iter().copied())])
        .await?;

    Ok(rows.iter().map(|row| row.get("name")).collect())
}

/// Which of `names` name an existing role.
pub(super) async fn existing_roles(
    client: &Client,
    names: &[&str],
) -> Result<BTreeSet<String>, ConnectionError> {
    existing_names(client, "mz_catalog.mz_roles", names).await
}

/// Which of `names` name an existing network policy.
pub(super) async fn existing_network_policies(
    client: &Client,
    names: &[&str],
) -> Result<BTreeSet<String>, ConnectionError> {
    existing_names(client, "mz_catalog.mz_network_policies", names).await
}

/// Check if a role exists.
pub(super) async fn role_exists(client: &Client, name: &str) -> Result<bool, ConnectionError> {
    let query = r#"
        SELECT EXISTS(
            SELECT 1 FROM mz_catalog.mz_roles WHERE name = $1
        ) AS exists
    "#;

    let row = client.query_one(query, &[&name]).await?;

    Ok(row.get("exists"))
}

/// The members granted to each of `names`, keyed by role name.
///
/// A role with no members is absent from the map.
pub(super) async fn get_role_members_bulk(
    client: &Client,
    names: &[&str],
) -> Result<BTreeMap<String, Vec<String>>, ConnectionError> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }

    let query = r#"
        SELECT r.name, m.name AS member
        FROM mz_catalog.mz_role_members rm
        JOIN mz_catalog.mz_roles r ON r.id = rm.role_id
        JOIN mz_catalog.mz_roles m ON m.id = rm.member
        WHERE r.name = ANY($1::text::text[])
        ORDER BY r.name, m.name
    "#;

    let rows = client
        .query(query, &[&array_literal(names.iter().copied())])
        .await?;

    Ok(group_by_name(&rows, |row| row.get("member")))
}

/// The session defaults set on each of `names`, keyed by role name.
///
/// A role with no session defaults is absent from the map.
pub(super) async fn get_role_parameters_bulk(
    client: &Client,
    names: &[&str],
) -> Result<BTreeMap<String, Vec<String>>, ConnectionError> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }

    let query = r#"
        SELECT r.name, rp.parameter_name
        FROM mz_catalog.mz_role_parameters rp
        JOIN mz_catalog.mz_roles r ON r.id = rp.role_id
        WHERE r.name = ANY($1::text::text[])
        ORDER BY r.name, rp.parameter_name
    "#;

    let rows = client
        .query(query, &[&array_literal(names.iter().copied())])
        .await?;

    Ok(group_by_name(&rows, |row| row.get("parameter_name")))
}

/// Get the current Materialize user/role.
pub(super) async fn get_current_user(client: &Client) -> Result<String, ConnectionError> {
    let row = client.query_one("SELECT current_user()", &[]).await?;

    Ok(row.get(0))
}

/// Check which databases from a set of names exist.
///
/// Returns a BTreeSet of database names that exist.
pub(super) async fn check_databases_exist(
    client: &Client,
    databases: &[String],
) -> Result<BTreeSet<String>, ConnectionError> {
    if databases.is_empty() {
        return Ok(BTreeSet::new());
    }

    let query = format!(
        "SELECT name FROM mz_catalog.mz_databases WHERE name IN ({})",
        sql_placeholders(databases.len())
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for database in databases {
        params.push(database);
    }

    let rows = client.query(&query, &params).await?;

    Ok(rows.iter().map(|row| row.get("name")).collect())
}

/// Check which schemas from a set of (database, schema) pairs exist.
///
/// Returns a BTreeSet of (database, schema) tuples that exist.
pub(super) async fn check_schemas_exist(
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
pub(super) async fn check_clusters_exist(
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
pub(super) async fn check_objects_exist(
    client: &Client,
    objects: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    if objects.is_empty() {
        return Ok(BTreeSet::new());
    }

    let fqn_map: BTreeMap<String, &ObjectId> = objects.iter().map(|o| (o.to_string(), o)).collect();
    let fqns: Vec<&String> = fqn_map.keys().collect();

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

    Ok(rows
        .iter()
        .filter_map(|row| {
            let fqn: String = row.get("fqn");
            fqn_map.get(&fqn).map(|id| (*id).clone())
        })
        .collect())
}

/// Check which objects from the given set exist with a specific catalog type.
///
/// Returns a BTreeSet of ObjectIds for objects that already exist.
async fn check_catalog_objects_exist(
    client: &Client,
    objects: &BTreeSet<ObjectId>,
    object_type: &str,
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
        SELECT d.name || '.' || s.name || '.' || o.name as fqn
        FROM mz_objects o
        JOIN mz_schemas s ON o.schema_id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        WHERE d.name || '.' || s.name || '.' || o.name IN ({})
          AND o.type = ${}
        ORDER BY fqn
    "#,
        placeholders_str,
        fqns.len() + 1
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    for fqn in &fqns {
        params.push(*fqn);
    }
    params.push(&object_type);

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
pub(super) async fn check_tables_exist(
    client: &Client,
    tables: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, tables, "table").await
}

/// Check which sources from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for sources that already exist.
pub(super) async fn check_sources_exist(
    client: &Client,
    sources: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, sources, "source").await
}

/// Check which secrets from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for secrets that already exist.
pub(super) async fn check_secrets_exist(
    client: &Client,
    secrets: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, secrets, "secret").await
}

/// Check which connections from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for connections that already exist.
pub(super) async fn check_connections_exist(
    client: &Client,
    connections: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, connections, "connection").await
}

/// Check which sinks from the given set exist in the database.
///
/// Returns a BTreeSet of ObjectIds for sinks that already exist.
/// Used during apply to skip creating sinks that already exist (like tables).
pub(super) async fn check_sinks_exist(
    client: &Client,
    sinks: &BTreeSet<ObjectId>,
) -> Result<BTreeSet<ObjectId>, ConnectionError> {
    check_catalog_objects_exist(client, sinks, "sink").await
}

/// Find sinks that depend on objects in the specified schemas.
///
/// This is used during apply to identify sinks that need to be repointed
/// before old schemas are dropped with CASCADE. Only returns sinks whose
/// upstream object (FROM clause) is in one of the specified schemas.
pub(super) async fn find_sinks_depending_on_schemas(
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
pub(super) async fn check_connection_exists(
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
pub(super) async fn object_exists(
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
pub(super) async fn get_staging_schemas(
    client: &Client,
    deploy_id: &str,
) -> Result<Vec<SchemaQualifier>, ConnectionError> {
    let pattern = staging_suffix_like_pattern(deploy_id);

    let query = r#"
        SELECT d.name as database, s.name as schema
        FROM mz_schemas s
        JOIN mz_databases d ON s.database_id = d.id
        WHERE s.name LIKE $1 ESCAPE '\'
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
pub(super) async fn get_staging_clusters(
    client: &Client,
    deploy_id: &str,
) -> Result<Vec<String>, ConnectionError> {
    let pattern = staging_suffix_like_pattern(deploy_id);

    let query = r#"
        SELECT name
        FROM mz_clusters
        WHERE name LIKE $1 ESCAPE '\'
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
pub(super) async fn drop_schema_objects(
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
pub(super) async fn drop_objects(
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
pub(super) async fn drop_staging_schemas(
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
pub(super) async fn drop_staging_clusters(
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

/// Get privilege grants on each of a set of named infrastructure objects
/// (clusters, network policies).
///
/// `catalog_table` is the system catalog table (e.g., `"mz_clusters"`,
/// `"mz_network_policies"`). Returns `(grantee, privilege_type)` pairs from
/// `mz_aclexplode`, filtering out system roles. A name the catalog records no
/// grants for is absent from the map.
async fn get_named_object_grants(
    client: &Client,
    catalog_table: &str,
    names: &[&str],
) -> Result<BTreeMap<String, Vec<ObjectGrant>>, ConnectionError> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }

    let query = format!(
        r#"
        WITH {},
        -- Explode each object's ACL bitmap into individual
        -- (grantee, privilege_type) rows. Each object stores its privileges as a
        -- compact bitmap; mz_aclexplode unpacks it.
        privilege AS (
            SELECT
                target.name,
                mz_internal.mz_aclexplode(o.privileges).*,
                o.owner_id
            FROM {} o
            JOIN target ON target.name = o.name
        )
        SELECT
            p.name,
            -- `p` is the PUBLIC pseudo-role. It has no `mz_roles` row, so it is
            -- resolved here rather than by the join, which would drop it.
            CASE WHEN p.grantee = 'p' THEN 'public' ELSE grantee.name END AS grantee,
            p.privilege_type
        FROM privilege AS p
        -- Resolve grantee role IDs to human-readable names.
        LEFT JOIN mz_roles AS grantee ON p.grantee = grantee.id
        -- Exclude system roles that are not user-manageable.
        WHERE (
            p.grantee = 'p'
            OR grantee.name NOT IN ('none', 'mz_system', 'mz_support')
        )
          -- Owners implicitly have all privileges; don't surface those as explicit grants.
          AND p.grantee != p.owner_id
        -- Ordered so the revocations a plan emits are deterministic.
        ORDER BY p.name, grantee, p.privilege_type
        "#,
        TARGET_NAMES_CTE, catalog_table
    );

    let rows = client
        .query(&query, &[&array_literal(names.iter().copied())])
        .await?;

    Ok(group_by_name(&rows, object_grant))
}

/// Get privilege grants on one named infrastructure object.
async fn get_one_named_object_grants(
    client: &Client,
    catalog_table: &str,
    name: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    Ok(get_named_object_grants(client, catalog_table, &[name])
        .await?
        .remove(name)
        .unwrap_or_default())
}

/// Get privilege grants on each of a set of clusters, keyed by cluster name.
pub(super) async fn get_cluster_grants(
    client: &Client,
    names: &[&str],
) -> Result<BTreeMap<String, Vec<ObjectGrant>>, ConnectionError> {
    get_named_object_grants(client, "mz_clusters", names).await
}

/// Get privilege grants on each of a set of network policies, keyed by name.
pub(super) async fn get_network_policy_grants(
    client: &Client,
    names: &[&str],
) -> Result<BTreeMap<String, Vec<ObjectGrant>>, ConnectionError> {
    get_named_object_grants(client, "mz_network_policies", names).await
}

/// Get privilege grants on each of a set of database objects (tables, sources,
/// secrets, connections).
///
/// An object the catalog records no grants for is absent from the map.
pub(super) async fn get_database_object_grants(
    client: &Client,
    objects: &BTreeSet<ObjectId>,
    object_type: &str,
) -> Result<BTreeMap<ObjectId, Vec<ObjectGrant>>, ConnectionError> {
    if objects.is_empty() {
        return Ok(BTreeMap::new());
    }

    let query = format!(
        r#"
        WITH {},
        -- Locate the target objects by name, then explode each one's ACL bitmap
        -- into individual privilege rows.
        privilege AS (
            SELECT
                target.database,
                target.schema,
                target.object,
                mz_internal.mz_aclexplode(o.privileges).*,
                o.owner_id
            FROM mz_objects o
            JOIN mz_schemas s ON o.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            JOIN target
                ON target.database = d.name
               AND target.schema = s.name
               AND target.object = o.name
            WHERE o.type = $4
        )
        SELECT
            p.database,
            p.schema,
            p.object,
            -- `p` is the PUBLIC pseudo-role, which has no `mz_roles` row.
            CASE WHEN p.grantee = 'p' THEN 'public' ELSE grantee.name END AS grantee,
            p.privilege_type
        FROM privilege AS p
        LEFT JOIN mz_roles AS grantee ON p.grantee = grantee.id
        WHERE (
            p.grantee = 'p'
            OR grantee.name NOT IN ('none', 'mz_system', 'mz_support')
        )
          AND p.grantee != p.owner_id
        -- Ordered so the revocations a plan emits are deterministic.
        ORDER BY p.database, p.schema, p.object, grantee, p.privilege_type
        "#,
        TARGET_OBJECTS_CTE
    );

    let [databases, schemas, names] = target_object_arrays(objects);
    let rows = client
        .query(&query, &[&databases, &schemas, &names, &object_type])
        .await?;

    Ok(group_by_object(&rows, object_grant))
}

/// Get privilege grants on a schema.
pub(super) async fn get_schema_grants(
    client: &Client,
    database: &str,
    schema: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    let query = r#"
        WITH privilege AS (
            SELECT mz_internal.mz_aclexplode(s.privileges).*, s.owner_id
            FROM mz_schemas s
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name = $1 AND s.name = $2
        )
        SELECT
            -- `p` is the PUBLIC pseudo-role, which has no `mz_roles` row.
            CASE WHEN p.grantee = 'p' THEN 'public' ELSE grantee.name END AS grantee,
            p.privilege_type
        FROM privilege AS p
        LEFT JOIN mz_roles AS grantee ON p.grantee = grantee.id
        WHERE (
            p.grantee = 'p'
            OR grantee.name NOT IN ('none', 'mz_system', 'mz_support')
        )
          AND p.grantee != p.owner_id
        ORDER BY grantee, p.privilege_type
        "#;

    let rows = client.query(query, &[&database, &schema]).await?;

    Ok(rows
        .iter()
        .map(|row| ObjectGrant {
            grantee: row.get("grantee"),
            privilege_type: row.get("privilege_type"),
        })
        .collect())
}

/// Get default privilege grants for a schema.
///
/// `ALTER DEFAULT PRIVILEGES ... ON SCHEMAS` can be scoped `IN DATABASE`, so
/// unlike the named-object query this matches both global rules and rules
/// scoped to the schema's own database.
pub(super) async fn get_default_privilege_grants_for_schema(
    client: &Client,
    database: &str,
    schema: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    let query = r#"
        SELECT
            -- `p` is the PUBLIC pseudo-role, which has no `mz_roles` row.
            CASE WHEN dp.grantee = 'p' THEN 'public' ELSE grantee_role.name END AS grantee,
            dp_priv.privilege_type
        FROM mz_default_privileges dp
        CROSS JOIN LATERAL unnest(
            mz_internal.mz_format_privileges(dp.privileges)
        ) AS dp_priv(privilege_type)
        JOIN mz_schemas s ON s.name = $2
        JOIN mz_databases d ON s.database_id = d.id
        LEFT JOIN mz_roles AS grantee_role ON dp.grantee = grantee_role.id
        WHERE d.name = $1
          AND dp.object_type = 'schema'
          -- Match rules targeting the schema's owner, or PUBLIC ('p') rules.
          AND (dp.role_id = s.owner_id OR dp.role_id = 'p')
          -- A schema lives in a database, so a rule scoped to that database
          -- applies to it, as does a global rule.
          AND (dp.database_id IS NULL OR dp.database_id = d.id)
          -- Schemas are not themselves schema-scoped.
          AND dp.schema_id IS NULL
          AND (
              dp.grantee = 'p'
              OR grantee_role.name NOT IN ('none', 'mz_system', 'mz_support')
          )
        "#;

    let rows = client.query(query, &[&database, &schema]).await?;

    Ok(rows
        .iter()
        .map(|row| ObjectGrant {
            grantee: row.get("grantee"),
            privilege_type: row.get("privilege_type"),
        })
        .collect())
}

/// Get the default privilege grants for each of a set of named infrastructure
/// objects (clusters, network policies).
///
/// Queries `mz_default_privileges` for the grants that would be auto-applied to
/// each object based on its owner and any PUBLIC default privileges. These
/// grants are protected from revocation during reconciliation. A name with no
/// such rules is absent from the map.
async fn get_default_privilege_grants_for_named_objects(
    client: &Client,
    catalog_table: &str,
    names: &[&str],
    object_type: &str,
) -> Result<BTreeMap<String, Vec<ObjectGrant>>, ConnectionError> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }

    let query = format!(
        r#"
        -- The grants ALTER DEFAULT PRIVILEGES rules auto-apply to each target
        -- object. Reconciliation protects them from revocation.
        WITH {},
        -- Resolve each target to the owner that decides which rules reach it.
        resolved AS (
            SELECT target.name, obj.owner_id
            FROM {} obj
            JOIN target ON target.name = obj.name
        )
        SELECT
            r.name,
            -- `p` is the PUBLIC pseudo-role, which has no `mz_roles` row.
            CASE WHEN dp.grantee = 'p' THEN 'public' ELSE grantee_role.name END AS grantee,
            dp_priv.privilege_type
        FROM resolved AS r
        JOIN mz_default_privileges dp
            -- Match rules targeting the object's owner, or PUBLIC ('p') rules
            -- that apply to all owners.
            ON (dp.role_id = r.owner_id OR dp.role_id = 'p')
           AND dp.object_type = $2
            -- Named objects (clusters, network policies) are not schema-scoped,
            -- so only global default privileges (both NULL) apply.
           AND dp.database_id IS NULL
           AND dp.schema_id IS NULL
        -- Expand the privilege bitmap into individual privilege type strings.
        CROSS JOIN LATERAL unnest(
            mz_internal.mz_format_privileges(dp.privileges)
        ) AS dp_priv(privilege_type)
        LEFT JOIN mz_roles AS grantee_role ON dp.grantee = grantee_role.id
        WHERE (
            dp.grantee = 'p'
            OR grantee_role.name NOT IN ('none', 'mz_system', 'mz_support')
        )
        ORDER BY r.name, grantee, dp_priv.privilege_type
        "#,
        TARGET_NAMES_CTE, catalog_table
    );

    let names = array_literal(names.iter().copied());
    let rows = client.query(&query, &[&names, &object_type]).await?;

    Ok(group_by_name(&rows, object_grant))
}

/// Get the default privilege grants for one named infrastructure object.
async fn get_default_privilege_grants_for_named_object(
    client: &Client,
    catalog_table: &str,
    name: &str,
    object_type: &str,
) -> Result<Vec<ObjectGrant>, ConnectionError> {
    Ok(
        get_default_privilege_grants_for_named_objects(client, catalog_table, &[name], object_type)
            .await?
            .remove(name)
            .unwrap_or_default(),
    )
}

/// Get the default privilege grants for each of a set of database objects
/// (tables, sources, secrets, connections).
///
/// Queries `mz_default_privileges` for the grants that would be auto-applied to
/// each object based on its owner, database, schema, and any PUBLIC default
/// privileges. These grants are protected from revocation. An object with no
/// such rules is absent from the map.
pub(super) async fn get_default_privilege_grants_for_database_objects(
    client: &Client,
    objects: &BTreeSet<ObjectId>,
    catalog_object_type: &str,
    default_privilege_type: &str,
) -> Result<BTreeMap<ObjectId, Vec<ObjectGrant>>, ConnectionError> {
    if objects.is_empty() {
        return Ok(BTreeMap::new());
    }

    let query = format!(
        r#"
        -- The grants ALTER DEFAULT PRIVILEGES rules auto-apply to each target
        -- object. Reconciliation protects them from revocation.
        WITH {},
        -- Resolve each target to the owner, database, and schema that decide
        -- which rules reach it.
        resolved AS (
            SELECT
                target.database,
                target.schema,
                target.object,
                obj.owner_id,
                d.id AS database_id,
                s.id AS schema_id
            FROM mz_objects obj
            JOIN mz_schemas s ON obj.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            JOIN target
                ON target.database = d.name
               AND target.schema = s.name
               AND target.object = obj.name
            WHERE obj.type = $4
        )
        SELECT
            r.database,
            r.schema,
            r.object,
            -- `p` is the PUBLIC pseudo-role, which has no `mz_roles` row.
            CASE WHEN dp.grantee = 'p' THEN 'public' ELSE grantee_role.name END AS grantee,
            dp_priv.privilege_type
        FROM resolved AS r
        JOIN mz_default_privileges dp
            -- Match rules targeting the object's owner, or PUBLIC ('p') rules.
            ON (dp.role_id = r.owner_id OR dp.role_id = 'p')
           AND dp.object_type = $5
            -- Match both global rules (database_id IS NULL) and rules scoped to
            -- the object's own database. Global rules apply to all databases.
           AND (dp.database_id IS NULL OR dp.database_id = r.database_id)
            -- Same for schema: global or scoped to the object's own schema.
           AND (dp.schema_id IS NULL OR dp.schema_id = r.schema_id)
        -- Expand the privilege bitmap into individual privilege type strings.
        CROSS JOIN LATERAL unnest(
            mz_internal.mz_format_privileges(dp.privileges)
        ) AS dp_priv(privilege_type)
        LEFT JOIN mz_roles AS grantee_role ON dp.grantee = grantee_role.id
        WHERE (
            dp.grantee = 'p'
            OR grantee_role.name NOT IN ('none', 'mz_system', 'mz_support')
        )
        ORDER BY r.database, r.schema, r.object, grantee, dp_priv.privilege_type
        "#,
        TARGET_OBJECTS_CTE
    );

    let [databases, schemas, names] = target_object_arrays(objects);
    let rows = client
        .query(
            &query,
            &[
                &databases,
                &schemas,
                &names,
                &catalog_object_type,
                &default_privilege_type,
            ],
        )
        .await?;

    Ok(group_by_object(&rows, object_grant))
}

/// The canonical `CREATE CONNECTION` SQL for each of `connections`, keyed by
/// object.
///
/// `mz_connections.create_sql` renders secret references in the catalog's
/// internal `SECRET [<id> AS <name>]` form, which is not parseable SQL, so the
/// statement has to come from `SHOW CREATE CONNECTION` one connection at a time.
/// Issuing them together lets the connection pipeline them, so the whole set
/// costs one round trip instead of one per connection.
///
/// A connection that does not exist is absent from the map.
pub(super) async fn get_connection_create_sqls(
    client: &Client,
    connections: &BTreeSet<ObjectId>,
) -> Result<BTreeMap<ObjectId, String>, ConnectionError> {
    let reads = connections.iter().map(|id| async move {
        let sql = get_connection_create_sql(client, id.expect_database(), id.schema(), id.object())
            .await?;
        Ok::<_, ConnectionError>(sql.map(|sql| (id.clone(), sql)))
    });

    Ok(futures::future::try_join_all(reads)
        .await?
        .into_iter()
        .flatten()
        .collect())
}

/// Read the `ALTER DEFAULT PRIVILEGES` rules recorded against a database or
/// schema scope.
///
/// `scope_predicate` narrows `mz_default_privileges` to the rules the scope owns
/// by identity, not the rules that merely apply to objects inside it. Both the
/// target role and the grantee can be the `p` pseudo-role, which stands for
/// `PUBLIC` and has no `mz_roles` row, so both are resolved with an outer join
/// and a `CASE` rather than an inner join that would drop those rules.
///
/// Role names come back exactly as the catalog stores them: they are identifiers
/// and identifiers are case-sensitive, so the caller compares them verbatim.
async fn get_default_privileges(
    client: &Client,
    scope_predicate: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<DefaultPrivilege>, ConnectionError> {
    let query = format!(
        r#"
        SELECT
            CASE WHEN dp.role_id = 'p' THEN 'public' ELSE target.name END AS target_role,
            dp.object_type,
            CASE WHEN dp.grantee = 'p' THEN 'public' ELSE grantee.name END AS grantee,
            upper(dp_priv.privilege_type) AS privilege
        FROM mz_default_privileges dp
        -- Expand the privilege bitmap into individual privilege type strings.
        CROSS JOIN LATERAL unnest(
            mz_internal.mz_format_privileges(dp.privileges)
        ) AS dp_priv(privilege_type)
        LEFT JOIN mz_roles AS target ON dp.role_id = target.id
        LEFT JOIN mz_roles AS grantee ON dp.grantee = grantee.id
        WHERE {}
        "#,
        scope_predicate
    );

    let rows = client.query(&query, params).await?;

    Ok(rows
        .iter()
        .map(|row| DefaultPrivilege {
            target_role: row.get("target_role"),
            object_type: row.get("object_type"),
            grantee: row.get("grantee"),
            privilege: row.get("privilege"),
        })
        .collect())
}

/// Get the default-privilege rules scoped to a database.
pub(super) async fn get_database_default_privileges(
    client: &Client,
    database: &str,
) -> Result<Vec<DefaultPrivilege>, ConnectionError> {
    get_default_privileges(
        client,
        "dp.database_id = (SELECT id FROM mz_databases WHERE name = $1)
           AND dp.schema_id IS NULL",
        &[&database],
    )
    .await
}

/// Get the default-privilege rules scoped to a schema.
///
/// Schema ids are unique across databases, so matching on `schema_id` alone
/// identifies the scope.
pub(super) async fn get_schema_default_privileges(
    client: &Client,
    database: &str,
    schema: &str,
) -> Result<Vec<DefaultPrivilege>, ConnectionError> {
    get_default_privileges(
        client,
        "dp.schema_id = (
             SELECT s.id
             FROM mz_schemas s
             JOIN mz_databases d ON s.database_id = d.id
             WHERE d.name = $1 AND s.name = $2
         )",
        &[&database, &schema],
    )
    .await
}

/// Get the comments on each of a set of database objects (tables, sources,
/// secrets, connections), including comments on their columns.
///
/// An object carrying no comments is absent from the map.
pub(super) async fn get_database_object_comments(
    client: &Client,
    objects: &BTreeSet<ObjectId>,
    object_type: &str,
) -> Result<BTreeMap<ObjectId, Vec<ObjectComment>>, ConnectionError> {
    if objects.is_empty() {
        return Ok(BTreeMap::new());
    }

    let query = format!(
        r#"
        -- Every comment recorded against a target object. `object_sub_id` is
        -- NULL for a comment on the object itself and the 1-based column
        -- position otherwise, so the LEFT JOIN resolves it to a column name and
        -- leaves object-level comments with a NULL column.
        WITH {}
        SELECT
            target.database,
            target.schema,
            target.object,
            col.name AS column_name,
            c.comment
        FROM mz_internal.mz_comments c
        JOIN mz_objects o ON c.id = o.id
        JOIN mz_schemas s ON o.schema_id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        JOIN target
            ON target.database = d.name
           AND target.schema = s.name
           AND target.object = o.name
        LEFT JOIN mz_columns col
            ON col.id = c.id AND col.position::int4 = c.object_sub_id
        WHERE o.type = $4
        ORDER BY target.database, target.schema, target.object, c.object_sub_id
        "#,
        TARGET_OBJECTS_CTE
    );

    let [databases, schemas, names] = target_object_arrays(objects);
    let rows = client
        .query(&query, &[&databases, &schemas, &names, &object_type])
        .await?;

    Ok(group_by_object(&rows, |row| ObjectComment {
        column: row.get("column_name"),
        comment: row.get("comment"),
    }))
}

/// Get the comment on each of a set of named objects (clusters, roles, network
/// policies, databases).
///
/// `catalog_table` is the system catalog table (e.g., `"mz_clusters"`). These
/// objects have no columns, so every result has a `None` column. A name carrying
/// no comment is absent from the map.
pub(super) async fn get_named_object_comments(
    client: &Client,
    catalog_table: &str,
    names: &[&str],
) -> Result<BTreeMap<String, Vec<ObjectComment>>, ConnectionError> {
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }

    let query = format!(
        r#"
        WITH {}
        SELECT target.name, c.comment
        FROM mz_internal.mz_comments c
        JOIN {} o ON c.id = o.id
        JOIN target ON target.name = o.name
        WHERE c.object_sub_id IS NULL
        ORDER BY target.name
        "#,
        TARGET_NAMES_CTE, catalog_table
    );

    let rows = client
        .query(&query, &[&array_literal(names.iter().copied())])
        .await?;

    Ok(group_by_name(&rows, |row| ObjectComment {
        column: None,
        comment: row.get("comment"),
    }))
}

/// Get the comment on one named object.
pub(super) async fn get_one_named_object_comments(
    client: &Client,
    catalog_table: &str,
    name: &str,
) -> Result<Vec<ObjectComment>, ConnectionError> {
    Ok(get_named_object_comments(client, catalog_table, &[name])
        .await?
        .remove(name)
        .unwrap_or_default())
}

/// Get the comment on a schema.
pub(super) async fn get_schema_comments(
    client: &Client,
    database: &str,
    schema: &str,
) -> Result<Vec<ObjectComment>, ConnectionError> {
    let query = r#"
        SELECT c.comment
        FROM mz_internal.mz_comments c
        JOIN mz_schemas s ON c.id = s.id
        JOIN mz_databases d ON s.database_id = d.id
        WHERE d.name = $1 AND s.name = $2 AND c.object_sub_id IS NULL
        "#;

    let rows = client.query(query, &[&database, &schema]).await?;

    Ok(rows
        .iter()
        .map(|row| ObjectComment {
            column: None,
            comment: row.get("comment"),
        })
        .collect())
}

/// Get the `CREATE CONNECTION` SQL for an existing connection.
///
/// Uses `SHOW CREATE CONNECTION` which returns the canonical, non-redacted SQL
/// including fully-qualified secret references. Returns `None` if the
/// connection does not exist.
pub(super) async fn get_connection_create_sql(
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
    ) -> Result<BTreeSet<ObjectId>, ConnectionError> {
        check_objects_exist(self.client, objects).await
    }

    /// Check which objects from a set exist with a specific catalog type.
    pub async fn check_catalog_objects_exist(
        &self,
        objects: &BTreeSet<ObjectId>,
        object_type: &str,
    ) -> Result<BTreeSet<ObjectId>, ConnectionError> {
        check_catalog_objects_exist(self.client, objects, object_type).await
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

    /// Check if a role exists.
    pub async fn role_exists(&self, name: &str) -> Result<bool, ConnectionError> {
        role_exists(self.client, name).await
    }

    /// Which of `names` name an existing role.
    pub async fn existing_roles(
        &self,
        names: &[&str],
    ) -> Result<BTreeSet<String>, ConnectionError> {
        existing_roles(self.client, names).await
    }

    /// Which of `names` name an existing network policy.
    pub async fn existing_network_policies(
        &self,
        names: &[&str],
    ) -> Result<BTreeSet<String>, ConnectionError> {
        existing_network_policies(self.client, names).await
    }

    /// Get the members granted to each of `names`, keyed by role name.
    pub async fn get_role_members_bulk(
        &self,
        names: &[&str],
    ) -> Result<BTreeMap<String, Vec<String>>, ConnectionError> {
        get_role_members_bulk(self.client, names).await
    }

    /// Get the session defaults set on each of `names`, keyed by role name.
    pub async fn get_role_parameters_bulk(
        &self,
        names: &[&str],
    ) -> Result<BTreeMap<String, Vec<String>>, ConnectionError> {
        get_role_parameters_bulk(self.client, names).await
    }

    /// Get a cluster by name.
    pub async fn get_cluster(&self, name: &str) -> Result<Option<Cluster>, ConnectionError> {
        get_cluster(self.client, name).await
    }

    /// Get the clusters named in `names`, keyed by cluster name.
    pub async fn get_clusters(
        &self,
        names: &[&str],
    ) -> Result<BTreeMap<String, Cluster>, ConnectionError> {
        get_clusters(self.client, names).await
    }

    /// Get the canonical `CREATE CLUSTER` SQL for each of `names`.
    pub async fn get_cluster_create_sqls(
        &self,
        names: &[&str],
    ) -> Result<BTreeMap<String, String>, ConnectionError> {
        get_cluster_create_sqls(self.client, names).await
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

    /// Get privilege grants on each of a set of clusters, keyed by name.
    pub async fn get_cluster_grants(
        &self,
        names: &[&str],
    ) -> Result<BTreeMap<String, Vec<ObjectGrant>>, ConnectionError> {
        get_cluster_grants(self.client, names).await
    }

    /// Get privilege grants on each of a set of network policies, keyed by name.
    pub async fn get_network_policy_grants(
        &self,
        names: &[&str],
    ) -> Result<BTreeMap<String, Vec<ObjectGrant>>, ConnectionError> {
        get_network_policy_grants(self.client, names).await
    }

    /// Get privilege grants on each of a set of database objects.
    pub async fn get_database_object_grants(
        &self,
        objects: &BTreeSet<ObjectId>,
        object_type: &str,
    ) -> Result<BTreeMap<ObjectId, Vec<ObjectGrant>>, ConnectionError> {
        get_database_object_grants(self.client, objects, object_type).await
    }

    /// Get the `CREATE CONNECTION` SQL for each of `connections`.
    pub async fn get_connection_create_sqls(
        &self,
        connections: &BTreeSet<ObjectId>,
    ) -> Result<BTreeMap<ObjectId, String>, ConnectionError> {
        get_connection_create_sqls(self.client, connections).await
    }

    /// Get default privilege grants for globally named objects.
    pub async fn get_default_privilege_grants_for_named_objects(
        &self,
        catalog_table: &str,
        names: &[&str],
        object_type: &str,
    ) -> Result<BTreeMap<String, Vec<ObjectGrant>>, ConnectionError> {
        get_default_privilege_grants_for_named_objects(
            self.client,
            catalog_table,
            names,
            object_type,
        )
        .await
    }

    /// Get privilege grants on a database by name.
    pub async fn get_database_grants(
        &self,
        database: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_one_named_object_grants(self.client, "mz_databases", database).await
    }

    /// Get privilege grants on a schema.
    pub async fn get_schema_grants(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_schema_grants(self.client, database, schema).await
    }

    /// Get default privilege grants for a database by name.
    pub async fn get_default_privilege_grants_for_database(
        &self,
        database: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_default_privilege_grants_for_named_object(
            self.client,
            "mz_databases",
            database,
            "database",
        )
        .await
    }

    /// Get default privilege grants for a schema.
    pub async fn get_default_privilege_grants_for_schema(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<ObjectGrant>, ConnectionError> {
        get_default_privilege_grants_for_schema(self.client, database, schema).await
    }

    /// Check which databases from a set of names exist.
    pub async fn check_databases_exist(
        &self,
        databases: &[String],
    ) -> Result<BTreeSet<String>, ConnectionError> {
        check_databases_exist(self.client, databases).await
    }

    /// Get the default-privilege rules scoped to a database.
    pub async fn get_database_default_privileges(
        &self,
        database: &str,
    ) -> Result<Vec<DefaultPrivilege>, ConnectionError> {
        get_database_default_privileges(self.client, database).await
    }

    /// Get the default-privilege rules scoped to a schema.
    pub async fn get_schema_default_privileges(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<DefaultPrivilege>, ConnectionError> {
        get_schema_default_privileges(self.client, database, schema).await
    }

    /// Get the comments on each of a set of database objects, including their
    /// column comments.
    pub async fn get_database_object_comments(
        &self,
        objects: &BTreeSet<ObjectId>,
        object_type: &str,
    ) -> Result<BTreeMap<ObjectId, Vec<ObjectComment>>, ConnectionError> {
        get_database_object_comments(self.client, objects, object_type).await
    }

    /// Get the comment on each of a set of named objects (clusters, roles,
    /// network policies, databases).
    pub async fn get_named_object_comments(
        &self,
        catalog_table: &str,
        names: &[&str],
    ) -> Result<BTreeMap<String, Vec<ObjectComment>>, ConnectionError> {
        get_named_object_comments(self.client, catalog_table, names).await
    }

    /// Get the comment on one named object.
    pub async fn get_one_named_object_comments(
        &self,
        catalog_table: &str,
        name: &str,
    ) -> Result<Vec<ObjectComment>, ConnectionError> {
        get_one_named_object_comments(self.client, catalog_table, name).await
    }

    /// Get the comment on a schema by name.
    pub async fn get_schema_comments(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<ObjectComment>, ConnectionError> {
        get_schema_comments(self.client, database, schema).await
    }

    /// Get the default privilege grants for each of a set of database objects.
    pub async fn get_default_privilege_grants_for_database_objects(
        &self,
        objects: &BTreeSet<ObjectId>,
        catalog_object_type: &str,
        default_privilege_type: &str,
    ) -> Result<BTreeMap<ObjectId, Vec<ObjectGrant>>, ConnectionError> {
        get_default_privilege_grants_for_database_objects(
            self.client,
            objects,
            catalog_object_type,
            default_privilege_type,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{array_literal, target_object_arrays};
    use crate::project::ir::object_id::ObjectId;
    use std::collections::BTreeSet;

    #[mz_ore::test]
    fn test_array_literal_quotes_every_element() {
        assert_eq!(array_literal(["a", "b"]), r#"{"a","b"}"#);
        let empty: [&str; 0] = [];
        assert_eq!(array_literal(empty), "{}");
    }

    /// A name that would otherwise be read as array syntax has to survive the
    /// encoding: an unquoted `,` or `}` would split or truncate the array, and
    /// `NULL` would decode as a null element rather than the literal name.
    #[mz_ore::test]
    fn test_array_literal_neutralizes_array_syntax() {
        assert_eq!(
            array_literal(["a,b", "{c}", "", "NULL", " d "]),
            r#"{"a,b","{c}","","NULL"," d "}"#
        );
    }

    /// `"` and `\` are the two characters an array literal escapes with `\`, so
    /// each has to be escaped to survive as itself.
    #[mz_ore::test]
    fn test_array_literal_escapes_quote_and_backslash() {
        assert_eq!(array_literal([r#"say "hi""#]), r#"{"say \"hi\""}"#);
        assert_eq!(array_literal([r"back\slash"]), r#"{"back\\slash"}"#);
        assert_eq!(array_literal([r#"\""#]), r#"{"\\\""}"#);
    }

    /// The three arrays are parallel: index `i` of each names one component of
    /// the same object, in the set's iteration order.
    #[mz_ore::test]
    fn test_target_object_arrays_stay_parallel() {
        let objects = BTreeSet::from([
            ObjectId::new("db1".into(), "s1".into(), "t1".into()),
            ObjectId::new("db2".into(), "s2".into(), "t2".into()),
        ]);

        assert_eq!(
            target_object_arrays(&objects),
            [
                r#"{"db1","db2"}"#.to_string(),
                r#"{"s1","s2"}"#.to_string(),
                r#"{"t1","t2"}"#.to_string(),
            ]
        );
    }
}
