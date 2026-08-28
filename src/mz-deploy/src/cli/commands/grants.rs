// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared helpers for grant reconciliation across apply commands.

use crate::cli::CliError;
use crate::cli::commands::reconcile::{ObjectKind, ReconcileTarget};
use crate::cli::executor::DeploymentExecutor;
use crate::client::{Client, ObjectGrant};
use crate::info;
use mz_sql_parser::ast::{
    GrantPrivilegesStatement, GrantTargetSpecification, Ident, Privilege, PrivilegeSpecification,
    Raw, RevokePrivilegesStatement,
};
use owo_colors::{OwoColorize, Stream, Style};
use std::collections::BTreeSet;
use std::fmt;

/// Reconcile grants for one catalog object.
pub async fn reconcile(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    target: &ReconcileTarget<'_>,
    grants: &[GrantPrivilegesStatement<Raw>],
) -> Result<(), CliError> {
    for grant in grants {
        executor.execute_sql(grant).await?;
    }
    let introspection = client.introspection();
    let kind = target.kind();
    let (current, default_privs) = match target {
        ReconcileTarget::Item { id, .. } => (
            introspection
                .get_database_object_grants(
                    kind.catalog_table(),
                    id.expect_database(),
                    id.schema(),
                    id.object(),
                )
                .await
                .map_err(CliError::Connection)?,
            introspection
                .get_default_privilege_grants_for_database_object(
                    kind.catalog_table(),
                    id.expect_database(),
                    id.schema(),
                    id.object(),
                    kind.default_privilege_type(),
                )
                .await
                .map_err(CliError::Connection)?,
        ),
        ReconcileTarget::Named {
            kind: ObjectKind::Cluster,
            name,
        } => (
            introspection
                .get_cluster_grants(name)
                .await
                .map_err(CliError::Connection)?,
            introspection
                .get_default_privilege_grants_for_cluster(name)
                .await
                .map_err(CliError::Connection)?,
        ),
        ReconcileTarget::Named {
            kind: ObjectKind::NetworkPolicy,
            name,
        } => (
            introspection
                .get_network_policy_grants(name)
                .await
                .map_err(CliError::Connection)?,
            introspection
                .get_default_privilege_grants_for_network_policy(name)
                .await
                .map_err(CliError::Connection)?,
        ),
        ReconcileTarget::Named { kind, .. } => {
            unreachable!("invalid named {} target", kind.label())
        }
    };
    let protected: BTreeSet<_> = default_privs
        .iter()
        .map(|g| (g.grantee.to_lowercase(), g.privilege_type.to_uppercase()))
        .collect();
    let desired = desired_grants(grants, kind.all_privileges());
    let grant_target = target.grant_target();
    let revocations = stale_grant_revocations(&current, &desired, &protected, &grant_target);
    execute_revocations(executor, &revocations, kind.label(), &target.display_name()).await
}

/// Extract `(grantee, privilege_type)` pairs from parsed GRANT statements.
///
/// Expands `ALL` privileges based on `all_privileges` (the set of privileges
/// that `ALL` maps to for the object type).
pub fn desired_grants(
    grants: &[GrantPrivilegesStatement<Raw>],
    all_privileges: &[&str],
) -> BTreeSet<(String, String)> {
    let mut result = BTreeSet::new();
    for grant in grants {
        let privs: Vec<String> = match &grant.privileges {
            PrivilegeSpecification::All => all_privileges.iter().map(|p| p.to_string()).collect(),
            PrivilegeSpecification::Privileges(privs) => {
                privs.iter().map(|p| p.to_string()).collect()
            }
        };
        for role in &grant.roles {
            let role_name = role.as_str().to_lowercase();
            for priv_name in &privs {
                result.insert((role_name.clone(), priv_name.clone()));
            }
        }
    }
    result
}

/// Parse a privilege type string (e.g. `"SELECT"`) into a [`Privilege`] enum value.
///
/// Returns `None` for privilege names mz-deploy doesn't recognize, which can
/// happen if a future Materialize release introduces a new privilege type.
/// Callers should skip unknown privileges rather than fail outright so the
/// CLI keeps working against newer servers.
fn parse_privilege(s: &str) -> Option<Privilege> {
    let p = if s.eq_ignore_ascii_case("SELECT") {
        Privilege::SELECT
    } else if s.eq_ignore_ascii_case("INSERT") {
        Privilege::INSERT
    } else if s.eq_ignore_ascii_case("UPDATE") {
        Privilege::UPDATE
    } else if s.eq_ignore_ascii_case("DELETE") {
        Privilege::DELETE
    } else if s.eq_ignore_ascii_case("USAGE") {
        Privilege::USAGE
    } else if s.eq_ignore_ascii_case("CREATE") {
        Privilege::CREATE
    } else if s.eq_ignore_ascii_case("CREATEROLE") {
        Privilege::CREATEROLE
    } else if s.eq_ignore_ascii_case("CREATEDB") {
        Privilege::CREATEDB
    } else if s.eq_ignore_ascii_case("CREATECLUSTER") {
        Privilege::CREATECLUSTER
    } else if s.eq_ignore_ascii_case("CREATENETWORKPOLICY") {
        Privilege::CREATENETWORKPOLICY
    } else {
        return None;
    };
    Some(p)
}

/// Compute REVOKE statements for grants that exist in `current` but not in
/// `desired` and not in `protected` (3-way set difference).
///
/// Grantee names are lowercased and privilege types uppercased before comparison
/// so that catalog casing differences don't cause spurious revocations.
///
/// `protected` contains grants that should never be revoked (e.g., grants
/// originating from `ALTER DEFAULT PRIVILEGES`).
pub fn stale_grant_revocations(
    current: &[ObjectGrant],
    desired: &BTreeSet<(String, String)>,
    protected: &BTreeSet<(String, String)>,
    target: &GrantTargetSpecification<Raw>,
) -> Vec<RevokePrivilegesStatement<Raw>> {
    let mut revocations = Vec::new();
    for grant in current {
        let key = (
            grant.grantee.to_lowercase(),
            grant.privilege_type.to_uppercase(),
        );
        if desired.contains(&key) || protected.contains(&key) {
            continue;
        }
        let Some(privilege) = parse_privilege(&grant.privilege_type) else {
            crate::verbose!(
                "skipping revocation of unknown privilege '{}' on grantee '{}' (target: {:?})",
                grant.privilege_type,
                grant.grantee,
                target,
            );
            continue;
        };
        revocations.push(RevokePrivilegesStatement {
            privileges: PrivilegeSpecification::Privileges(vec![privilege]),
            target: target.clone(),
            roles: vec![Ident::new_unchecked(grant.grantee.clone())],
        });
    }
    revocations
}

/// Execute REVOKE statements for stale grants, printing status for each.
pub async fn execute_revocations(
    executor: &DeploymentExecutor<'_>,
    revocations: &[RevokePrivilegesStatement<Raw>],
    object_type_label: &str,
    display_name: &impl fmt::Display,
) -> Result<(), CliError> {
    let dash_style = Style::new().red().bold();
    for stmt in revocations {
        if !executor.is_dry_run() {
            info!(
                "  {} Revoking stale grant on {} '{}'",
                "-".if_supports_color(Stream::Stderr, |t| dash_style.style(t)),
                object_type_label,
                display_name,
            );
        }
        executor.execute_sql(stmt).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::ir::object_id::ObjectId;
    use mz_sql_parser::ast::Statement;
    use mz_sql_parser::parser::parse_statements;

    fn make_object_grant(grantee: &str, privilege_type: &str) -> ObjectGrant {
        ObjectGrant {
            grantee: grantee.to_string(),
            privilege_type: privilege_type.to_string(),
        }
    }

    /// Parse a GRANT SQL string into a GrantPrivilegesStatement.
    fn parse_grant(sql: &str) -> GrantPrivilegesStatement<Raw> {
        let stmts = parse_statements(sql).unwrap();
        match stmts.into_iter().next().unwrap().ast {
            Statement::GrantPrivileges(g) => g,
            other => panic!("expected GRANT, got: {}", other),
        }
    }

    fn cluster_target(name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::named(ObjectKind::Cluster, name).grant_target()
    }

    fn network_policy_target(name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::named(ObjectKind::NetworkPolicy, name).grant_target()
    }

    fn obj_id(db: &str, schema: &str, name: &str) -> ObjectId {
        ObjectId::new(db.to_string(), schema.to_string(), name.to_string())
    }

    fn table_target(db: &str, schema: &str, name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::item(ObjectKind::Table, &obj_id(db, schema, name)).grant_target()
    }

    fn secret_target(db: &str, schema: &str, name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::item(ObjectKind::Secret, &obj_id(db, schema, name)).grant_target()
    }

    fn connection_target(db: &str, schema: &str, name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::item(ObjectKind::Connection, &obj_id(db, schema, name)).grant_target()
    }

    fn source_target(db: &str, schema: &str, name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::item(ObjectKind::Source, &obj_id(db, schema, name)).grant_target()
    }

    /// Convert revocations to strings for easier assertion.
    fn to_strings(revocations: &[RevokePrivilegesStatement<Raw>]) -> Vec<String> {
        revocations.iter().map(|r| r.to_string()).collect()
    }

    #[mz_ore::test]
    fn test_desired_grants_single_privilege_single_role() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let result = desired_grants(&[grant], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&("reader".to_string(), "USAGE".to_string())));
    }

    #[mz_ore::test]
    fn test_desired_grants_multiple_privileges() {
        let grant = parse_grant("GRANT USAGE, CREATE ON CLUSTER my_cluster TO writer");
        let result = desired_grants(&[grant], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("writer".to_string(), "USAGE".to_string())));
        assert!(result.contains(&("writer".to_string(), "CREATE".to_string())));
    }

    #[mz_ore::test]
    fn test_desired_grants_all_expands_to_object_type_privileges() {
        let grant = parse_grant("GRANT ALL ON CLUSTER my_cluster TO admin");
        // For clusters, ALL = USAGE + CREATE
        let result = desired_grants(&[grant], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("admin".to_string(), "USAGE".to_string())));
        assert!(result.contains(&("admin".to_string(), "CREATE".to_string())));
    }

    #[mz_ore::test]
    fn test_desired_grants_all_with_single_privilege_object_type() {
        let grant = parse_grant("GRANT ALL ON SECRET \"db\".\"public\".\"my_secret\" TO reader");
        // For secrets, ALL = USAGE only
        let result = desired_grants(&[grant], &["USAGE"]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&("reader".to_string(), "USAGE".to_string())));
    }

    #[mz_ore::test]
    fn test_desired_grants_multiple_roles() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader, writer");
        let result = desired_grants(&[grant], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("reader".to_string(), "USAGE".to_string())));
        assert!(result.contains(&("writer".to_string(), "USAGE".to_string())));
    }

    #[mz_ore::test]
    fn test_desired_grants_multiple_grant_statements() {
        let g1 = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let g2 = parse_grant("GRANT CREATE ON CLUSTER my_cluster TO writer");
        let result = desired_grants(&[g1, g2], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("reader".to_string(), "USAGE".to_string())));
        assert!(result.contains(&("writer".to_string(), "CREATE".to_string())));
    }

    #[mz_ore::test]
    fn test_desired_grants_deduplicates() {
        // Two grant statements granting the same privilege to the same role
        let g1 = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let g2 = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let result = desired_grants(&[g1, g2], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 1);
    }

    #[mz_ore::test]
    fn test_desired_grants_empty_input() {
        let result = desired_grants(&[], &["USAGE", "CREATE"]);
        assert!(result.is_empty());
    }

    #[mz_ore::test]
    fn test_desired_grants_role_name_lowercased() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO \"MyRole\"");
        let result = desired_grants(&[grant], &["USAGE"]);
        assert!(result.contains(&("myrole".to_string(), "USAGE".to_string())));
    }

    #[mz_ore::test]
    fn test_desired_grants_table_all_privileges() {
        let grant = parse_grant("GRANT ALL ON TABLE \"db\".\"public\".\"my_table\" TO admin");
        // For tables, ALL = SELECT + INSERT + UPDATE + DELETE
        let result = desired_grants(&[grant], &["SELECT", "INSERT", "UPDATE", "DELETE"]);
        assert_eq!(result.len(), 4);
        assert!(result.contains(&("admin".to_string(), "SELECT".to_string())));
        assert!(result.contains(&("admin".to_string(), "INSERT".to_string())));
        assert!(result.contains(&("admin".to_string(), "UPDATE".to_string())));
        assert!(result.contains(&("admin".to_string(), "DELETE".to_string())));
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_no_stale() {
        let current = vec![make_object_grant("reader", "USAGE")];
        let mut desired = BTreeSet::new();
        desired.insert(("reader".to_string(), "USAGE".to_string()));

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        assert!(revocations.is_empty());
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_has_stale() {
        let current = vec![
            make_object_grant("reader", "USAGE"),
            make_object_grant("writer", "CREATE"),
        ];
        let mut desired = BTreeSet::new();
        desired.insert(("reader".to_string(), "USAGE".to_string()));

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert_eq!(
            strings[0],
            "REVOKE CREATE ON CLUSTER my_cluster FROM writer"
        );
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_empty_desired() {
        let current = vec![make_object_grant("reader", "USAGE")];
        let desired = BTreeSet::new();

        let target = table_target("db", "public", "t");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert_eq!(strings[0], "REVOKE USAGE ON TABLE db.public.t FROM reader");
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_empty_current() {
        let mut desired = BTreeSet::new();
        desired.insert(("reader".to_string(), "USAGE".to_string()));

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&[], &desired, &BTreeSet::new(), &target);
        assert!(revocations.is_empty());
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_both_empty() {
        let target = secret_target("db", "public", "s");
        let revocations = stale_grant_revocations(&[], &BTreeSet::new(), &BTreeSet::new(), &target);
        assert!(revocations.is_empty());
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_case_insensitive_match() {
        // Current has mixed case, desired has lowercase — should still match
        let current = vec![make_object_grant("Reader", "usage")];
        let mut desired = BTreeSet::new();
        desired.insert(("reader".to_string(), "USAGE".to_string()));

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        assert!(revocations.is_empty());
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_multiple_stale() {
        let current = vec![
            make_object_grant("reader", "USAGE"),
            make_object_grant("writer", "CREATE"),
            make_object_grant("admin", "USAGE"),
        ];
        let desired = BTreeSet::new(); // All grants removed

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        assert_eq!(revocations.len(), 3);
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_network_policy_keyword() {
        let current = vec![make_object_grant("reader", "USAGE")];
        let desired = BTreeSet::new();

        let target = network_policy_target("my_policy");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert_eq!(
            strings[0],
            "REVOKE USAGE ON NETWORK POLICY my_policy FROM reader"
        );
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_connection_keyword() {
        let current = vec![make_object_grant("app", "USAGE")];
        let desired = BTreeSet::new();

        let target = connection_target("db", "public", "my_conn");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert_eq!(
            strings[0],
            "REVOKE USAGE ON CONNECTION db.public.my_conn FROM app"
        );
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_secret_keyword() {
        let current = vec![make_object_grant("app", "USAGE")];
        let desired = BTreeSet::new();

        let target = secret_target("db", "public", "my_secret");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert_eq!(
            strings[0],
            "REVOKE USAGE ON SECRET db.public.my_secret FROM app"
        );
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_source_keyword() {
        let current = vec![make_object_grant("reader", "SELECT")];
        let desired = BTreeSet::new();

        let target = source_target("db", "public", "my_source");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert_eq!(
            strings[0],
            "REVOKE SELECT ON TABLE db.public.my_source FROM reader"
        );
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_protected_grants_not_revoked() {
        // Current has grants for reader (from default privileges) and writer (explicit).
        // Neither is in desired, but reader's grant is protected.
        let current = vec![
            make_object_grant("reader", "SELECT"),
            make_object_grant("writer", "SELECT"),
        ];
        let desired = BTreeSet::new();
        let mut protected = BTreeSet::new();
        protected.insert(("reader".to_string(), "SELECT".to_string()));

        let target = table_target("db", "public", "t");
        let revocations = stale_grant_revocations(&current, &desired, &protected, &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert!(strings[0].contains("writer"));
        assert!(!strings[0].contains("reader"));
    }

    #[mz_ore::test]
    fn test_end_to_end_no_revocations_when_grants_match() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let desired = desired_grants(&[grant], &["USAGE", "CREATE"]);
        let current = vec![make_object_grant("reader", "USAGE")];

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        assert!(revocations.is_empty());
    }

    #[mz_ore::test]
    fn test_end_to_end_revoke_removed_grant() {
        // Project only declares USAGE for reader, but cluster also has CREATE for writer
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let desired = desired_grants(&[grant], &["USAGE", "CREATE"]);
        let current = vec![
            make_object_grant("reader", "USAGE"),
            make_object_grant("writer", "CREATE"),
        ];

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert!(strings[0].contains("writer"));
        assert!(strings[0].contains("CREATE"));
    }

    #[mz_ore::test]
    fn test_end_to_end_revoke_all_when_grants_removed() {
        // No grants declared in project, but cluster has grants
        let desired = desired_grants(&[], &["USAGE", "CREATE"]);
        let current = vec![
            make_object_grant("reader", "USAGE"),
            make_object_grant("writer", "CREATE"),
        ];

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        assert_eq!(revocations.len(), 2);
    }

    #[mz_ore::test]
    fn test_end_to_end_grant_all_covers_all_current() {
        let grant = parse_grant("GRANT ALL ON CLUSTER my_cluster TO admin");
        let desired = desired_grants(&[grant], &["USAGE", "CREATE"]);
        // admin has both USAGE and CREATE — both covered by ALL
        let current = vec![
            make_object_grant("admin", "USAGE"),
            make_object_grant("admin", "CREATE"),
        ];

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        assert!(revocations.is_empty());
    }

    #[mz_ore::test]
    fn test_end_to_end_grant_all_still_revokes_other_roles() {
        let grant = parse_grant("GRANT ALL ON CLUSTER my_cluster TO admin");
        let desired = desired_grants(&[grant], &["USAGE", "CREATE"]);
        // admin is covered, but reader is not in the project file
        let current = vec![
            make_object_grant("admin", "USAGE"),
            make_object_grant("admin", "CREATE"),
            make_object_grant("reader", "USAGE"),
        ];

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert!(strings[0].contains("reader"));
    }

    #[mz_ore::test]
    fn test_end_to_end_multiple_roles_multiple_privileges() {
        let g1 = parse_grant("GRANT USAGE ON CLUSTER c TO reader");
        let g2 = parse_grant("GRANT USAGE, CREATE ON CLUSTER c TO writer");
        let desired = desired_grants(&[g1, g2], &["USAGE", "CREATE"]);

        // Current has an extra admin grant
        let current = vec![
            make_object_grant("reader", "USAGE"),
            make_object_grant("writer", "USAGE"),
            make_object_grant("writer", "CREATE"),
            make_object_grant("admin", "USAGE"),
        ];

        let target = cluster_target("c");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        let strings = to_strings(&revocations);
        assert_eq!(strings.len(), 1);
        assert!(strings[0].contains("admin"));
    }
}
