// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! DDL statements that materialize the `_mz_deploy` tracking database.
//!
//! Each entry is executed as its own statement by [`super::setup::setup`].
//! Executing them individually (rather than as one multi-statement batch via
//! `batch_execute`) avoids Materialize's rejection of DDL inside the implicit
//! transaction block that a simple multi-statement query creates.
//!
//! Every statement is idempotent — `setup` is the only command that writes
//! to `_mz_deploy`, and it can be re-run any number of times to bring an
//! existing installation up to the current set of objects. The initial
//! `tables.version` row is seeded separately by [`super::setup::setup`] with
//! a pre-check, since there is no `INSERT IF NOT EXISTS` form in Materialize.
//!
//! Order matters: tables must exist before the indexes and views that
//! reference them.
//!
//! [`EXPECTED_OBJECTS`] MUST stay in sync with the `CREATE` statements here;
//! a unit test in this module guards the invariant.

/// All DDL statements required to initialize `_mz_deploy` from a clean
/// database, and safe to re-run against an existing one. The `_mz_deploy`
/// database itself is created separately by [`super::setup::setup`]
/// immediately before iterating these.
pub(super) const SETUP_STATEMENTS: &[&str] = &[
    "CREATE SCHEMA IF NOT EXISTS _mz_deploy.tables",
    r#"CREATE TABLE IF NOT EXISTS _mz_deploy.tables.deployments (
        deploy_id   TEXT NOT NULL,
        deployed_at TIMESTAMPTZ NOT NULL,
        promoted_at TIMESTAMPTZ,
        database    TEXT NOT NULL,
        schema      TEXT NOT NULL,
        deployed_by TEXT NOT NULL,
        commit      TEXT,
        kind        TEXT NOT NULL,
        mode        TEXT NOT NULL
    )"#,
    r#"CREATE INDEX IF NOT EXISTS deployments_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.deployments (deploy_id)"#,
    r#"CREATE TABLE IF NOT EXISTS _mz_deploy.tables.objects (
        deploy_id TEXT NOT NULL,
        database  TEXT NOT NULL,
        schema    TEXT NOT NULL,
        object    TEXT NOT NULL,
        hash      TEXT NOT NULL
    )"#,
    r#"CREATE INDEX IF NOT EXISTS objects_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.objects (deploy_id)"#,
    r#"CREATE TABLE IF NOT EXISTS _mz_deploy.tables.clusters (
        deploy_id  TEXT NOT NULL,
        cluster_id TEXT NOT NULL
    )"#,
    r#"CREATE TABLE IF NOT EXISTS _mz_deploy.tables.pending_statements (
        deploy_id      TEXT NOT NULL,
        sequence_num   INT NOT NULL,
        database       TEXT NOT NULL,
        schema         TEXT NOT NULL,
        object         TEXT NOT NULL,
        object_hash    TEXT NOT NULL,
        statement_sql  TEXT NOT NULL,
        statement_kind TEXT NOT NULL,
        executed_at    TIMESTAMPTZ
    )"#,
    r#"CREATE INDEX IF NOT EXISTS pending_statements_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.pending_statements (deploy_id)"#,
    r#"CREATE TABLE IF NOT EXISTS _mz_deploy.tables.replacement_mvs (
        deploy_id          TEXT NOT NULL,
        target_database    TEXT NOT NULL,
        target_schema      TEXT NOT NULL,
        target_name        TEXT NOT NULL,
        replacement_schema TEXT NOT NULL
    )"#,
    r#"CREATE TABLE IF NOT EXISTS _mz_deploy.tables.version (
        version BIGINT NOT NULL
    )"#,
    r#"CREATE INDEX IF NOT EXISTS version_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.version (version)"#,
    // Per-developer overlay database manifest.
    r#"CREATE TABLE IF NOT EXISTS _mz_deploy.tables.dev_overlays (
        profile       TEXT NOT NULL,
        project       TEXT NOT NULL,
        overlay_db    TEXT NOT NULL,
        created_at    TIMESTAMPTZ NOT NULL
    )"#,
    r#"CREATE INDEX IF NOT EXISTS dev_overlays_profile_project_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.tables.dev_overlays (profile, project)"#,
    r#"CREATE VIEW IF NOT EXISTS _mz_deploy.public.production AS
    WITH candidates AS (
        SELECT DISTINCT ON (database, schema)
            database, schema, deploy_id, promoted_at, commit, kind
        FROM _mz_deploy.tables.deployments
        WHERE promoted_at IS NOT NULL
        ORDER BY database, schema, promoted_at DESC
    )
    SELECT c.database, c.schema, c.deploy_id, c.promoted_at, c.commit, c.kind
    FROM candidates c
    JOIN mz_schemas s ON c.schema = s.name
    JOIN mz_databases d ON c.database = d.name"#,
    r#"CREATE INDEX IF NOT EXISTS production_database_schema_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.public.production (database, schema)"#,
    r#"CREATE VIEW IF NOT EXISTS _mz_deploy.public.staging_deployments AS
    SELECT deploy_id, deployed_at, database, schema, deployed_by, commit, kind, mode
    FROM _mz_deploy.tables.deployments
    WHERE promoted_at IS NULL"#,
    r#"CREATE INDEX IF NOT EXISTS staging_deployments_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.public.staging_deployments (deploy_id)"#,
    r#"CREATE VIEW IF NOT EXISTS _mz_deploy.public.deployment_clusters AS
    SELECT dc.deploy_id, c.name
    FROM _mz_deploy.tables.clusters dc
    JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id"#,
    r#"CREATE INDEX IF NOT EXISTS deployment_clusters_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.public.deployment_clusters (deploy_id)"#,
    r#"CREATE VIEW IF NOT EXISTS _mz_deploy.public.missing_clusters AS
    SELECT d.deploy_id, dc.cluster_id
    FROM _mz_deploy.tables.deployments d
    JOIN _mz_deploy.tables.clusters dc USING (deploy_id)
    LEFT JOIN mz_catalog.mz_clusters c ON dc.cluster_id = c.id
    WHERE d.promoted_at IS NULL AND c.id IS NULL"#,
    r#"CREATE INDEX IF NOT EXISTS missing_clusters_deploy_id_idx
        IN CLUSTER _mz_deploy_server
        ON _mz_deploy.public.missing_clusters (deploy_id)"#,
    r#"CREATE VIEW IF NOT EXISTS _mz_deploy.public.deployments AS
    SELECT deploy_id, deployed_at, promoted_at, database, schema, deployed_by,
           commit, kind, mode
    FROM _mz_deploy.tables.deployments"#,
    r#"CREATE VIEW IF NOT EXISTS _mz_deploy.public.objects AS
    SELECT deploy_id, database, schema, object, hash
    FROM _mz_deploy.tables.objects"#,
    r#"CREATE VIEW IF NOT EXISTS _mz_deploy.public.pending_statements AS
    SELECT deploy_id, sequence_num, database, schema, object, object_hash,
           statement_sql, statement_kind, executed_at
    FROM _mz_deploy.tables.pending_statements"#,
    r#"CREATE VIEW IF NOT EXISTS _mz_deploy.public.replacement_mvs AS
    SELECT deploy_id, target_database, target_schema, target_name,
           replacement_schema
    FROM _mz_deploy.tables.replacement_mvs"#,
    r#"CREATE VIEW IF NOT EXISTS _mz_deploy.public.version AS
    SELECT version
    FROM _mz_deploy.tables.version"#,
];

/// The known set of objects that `_mz_deploy` contains after a successful
/// `setup`. Used by `verify()` to check whether setup has been run.
///
/// Each entry is `(schema, object_name, kind)` where `kind` is the value
/// `mz_objects.type` uses: `"table"`, `"view"`, `"index"`.
///
/// This list MUST stay in sync with `SETUP_STATEMENTS` above. Any object
/// created by a statement must appear here, and vice versa.
pub(super) const EXPECTED_OBJECTS: &[(&str, &str, &str)] = &[
    ("tables", "deployments", "table"),
    ("tables", "deployments_deploy_id_idx", "index"),
    ("tables", "objects", "table"),
    ("tables", "objects_deploy_id_idx", "index"),
    ("tables", "clusters", "table"),
    ("tables", "pending_statements", "table"),
    ("tables", "pending_statements_deploy_id_idx", "index"),
    ("tables", "replacement_mvs", "table"),
    ("tables", "version", "table"),
    ("tables", "version_idx", "index"),
    ("tables", "dev_overlays", "table"),
    ("tables", "dev_overlays_profile_project_idx", "index"),
    ("public", "production", "view"),
    ("public", "production_database_schema_idx", "index"),
    ("public", "staging_deployments", "view"),
    ("public", "staging_deployments_deploy_id_idx", "index"),
    ("public", "deployment_clusters", "view"),
    ("public", "deployment_clusters_deploy_id_idx", "index"),
    ("public", "missing_clusters", "view"),
    ("public", "missing_clusters_deploy_id_idx", "index"),
    ("public", "deployments", "view"),
    ("public", "objects", "view"),
    ("public", "pending_statements", "view"),
    ("public", "replacement_mvs", "view"),
    ("public", "version", "view"),
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Extract `(schema, name, kind)` triples from a single CREATE DDL
    /// statement in `SETUP_STATEMENTS`. Returns `None` for statements we
    /// don't track in `EXPECTED_OBJECTS` (currently: `CREATE SCHEMA`).
    fn parsed_object(stmt: &str) -> Option<(String, String, &'static str)> {
        // Normalize whitespace so we can work with tokens.
        let collapsed: String = stmt.split_whitespace().collect::<Vec<_>>().join(" ");
        let tokens: Vec<&str> = collapsed.split(' ').collect();

        // Pattern prefix check — every tracked stmt starts with
        // "CREATE {TABLE|INDEX|VIEW} IF NOT EXISTS".
        if tokens.len() < 6 || tokens[0] != "CREATE" || tokens[2..5] != ["IF", "NOT", "EXISTS"] {
            return None;
        }

        match tokens[1] {
            "TABLE" | "VIEW" => {
                // token 5: `_mz_deploy.<schema>.<name>` (may have trailing `(` for TABLE).
                let fqn = tokens[5].trim_end_matches('(');
                let (schema, name) = split_db_schema_name(fqn)?;
                let kind = if tokens[1] == "TABLE" {
                    "table"
                } else {
                    "view"
                };
                Some((schema, name, kind))
            }
            "INDEX" => {
                // token 5: index name. Schema is the target's schema, found
                // after the `ON` keyword.
                let name = tokens[5].to_string();
                let on_pos = tokens.iter().position(|t| *t == "ON")?;
                let target = tokens.get(on_pos + 1)?.trim_end_matches('(');
                let (target_schema, _target) = split_db_schema_name(target)?;
                Some((target_schema, name, "index"))
            }
            _ => None,
        }
    }

    /// Split `_mz_deploy.<schema>.<name>` into `(schema, name)`.
    fn split_db_schema_name(fqn: &str) -> Option<(String, String)> {
        let rest = fqn.strip_prefix("_mz_deploy.")?;
        let (schema, name) = rest.split_once('.')?;
        Some((schema.to_string(), name.to_string()))
    }

    /// Guards that every object created by `SETUP_STATEMENTS` has a
    /// corresponding `EXPECTED_OBJECTS` entry and vice versa — so `verify`
    /// catches exactly the set of objects `setup` installs. If this fails,
    /// you added a CREATE without a matching EXPECTED_OBJECTS row (or the
    /// other way around).
    #[test]
    fn expected_objects_match_setup_statements() {
        let parsed: BTreeSet<(String, String, &'static str)> = SETUP_STATEMENTS
            .iter()
            .filter_map(|stmt| parsed_object(stmt))
            .collect();

        let expected: BTreeSet<(String, String, &'static str)> = EXPECTED_OBJECTS
            .iter()
            .map(|(s, n, k)| (s.to_string(), n.to_string(), *k))
            .collect();

        let missing_from_expected: Vec<_> = parsed.difference(&expected).collect();
        let missing_from_statements: Vec<_> = expected.difference(&parsed).collect();

        assert!(
            missing_from_expected.is_empty() && missing_from_statements.is_empty(),
            "SETUP_STATEMENTS and EXPECTED_OBJECTS are out of sync.\n\
             In SETUP_STATEMENTS but not in EXPECTED_OBJECTS: {:?}\n\
             In EXPECTED_OBJECTS but not in SETUP_STATEMENTS: {:?}",
            missing_from_expected,
            missing_from_statements,
        );
    }
}
