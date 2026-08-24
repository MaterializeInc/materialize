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
use crate::cli::commands::reconcile::ReconcileTarget;
use crate::cli::executor::DeploymentExecutor;
use crate::client::ObjectGrant;
use crate::info;
use mz_sql_parser::ast::{
    GrantPrivilegesStatement, GrantTargetSpecification, Ident, Privilege, PrivilegeSpecification,
    Raw, RevokePrivilegesStatement,
};
use owo_colors::{OwoColorize, Stream, Style};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Reconcile grants on one object from a catalog state already read by the
/// orchestration layer.
pub async fn reconcile(
    executor: &DeploymentExecutor<'_>,
    target: &ReconcileTarget<'_>,
    grants: &[GrantPrivilegesStatement<Raw>],
    current: &[ObjectGrant],
    default_privileges: &[ObjectGrant],
) -> Result<(), CliError> {
    let protected: BTreeSet<_> = default_privileges
        .iter()
        .filter_map(GrantKey::from_catalog)
        .collect();
    let desired = desired_grants(grants, target.kind().all_privileges());
    let grant_target = target
        .grant_target()
        .expect("grant reconciliation requires a grant-bearing target");
    execute_grants(
        executor,
        &missing_grant_statements(&desired, current, &grant_target),
    )
    .await?;
    let revocations = stale_grant_revocations(current, &desired, &protected, &grant_target);
    execute_revocations(
        executor,
        &revocations,
        target.kind().label(),
        &target.display_name(),
    )
    .await
}

/// One concrete privilege granted to one role.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GrantKey {
    pub grantee: String,
    pub privilege: Privilege,
}

impl GrantKey {
    fn from_catalog(grant: &ObjectGrant) -> Option<Self> {
        Some(Self {
            grantee: grant.grantee.clone(),
            privilege: parse_privilege(&grant.privilege_type)?,
        })
    }
}

/// Extract typed grant keys from parsed GRANT statements.
///
/// Expands `ALL` privileges based on `all_privileges` (the set of privileges
/// that `ALL` maps to for the object type).
///
/// Grantee names are kept exactly as authored. Role names are identifiers and
/// identifiers are case-sensitive in Materialize: a role created as `"Reader"`
/// cannot be reached as `reader`. The parser has already folded unquoted names
/// to the casing the catalog stores, so the authored name and the catalog name
/// agree without normalization, and folding here would instead conflate two
/// roles that differ only by case. Privilege types are uppercased because they
/// are keywords, not identifiers.
pub fn desired_grants(
    grants: &[GrantPrivilegesStatement<Raw>],
    all_privileges: &[Privilege],
) -> BTreeSet<GrantKey> {
    let mut result = BTreeSet::new();
    for grant in grants {
        let privileges: &[Privilege] = match &grant.privileges {
            PrivilegeSpecification::All => all_privileges,
            PrivilegeSpecification::Privileges(privileges) => privileges,
        };
        for role in &grant.roles {
            for privilege in privileges {
                result.insert(GrantKey {
                    grantee: role.as_str().to_string(),
                    privilege: *privilege,
                });
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
pub(crate) fn parse_privilege(s: &str) -> Option<Privilege> {
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

/// Compute GRANT statements for desired grants that `current` does not already
/// hold (2-way set difference).
///
/// Missing privileges are grouped by grantee so one statement carries every
/// privilege that role is owed. `desired` has `ALL` already expanded to concrete
/// privileges, so the emitted SQL names them explicitly rather than echoing an
/// authored `GRANT ALL`.
pub fn missing_grant_statements(
    desired: &BTreeSet<GrantKey>,
    current: &[ObjectGrant],
    target: &GrantTargetSpecification<Raw>,
) -> Vec<GrantPrivilegesStatement<Raw>> {
    let present: BTreeSet<_> = current.iter().filter_map(GrantKey::from_catalog).collect();

    let mut by_grantee: BTreeMap<&str, Vec<Privilege>> = BTreeMap::new();
    for key in desired {
        if present.contains(key) {
            continue;
        }
        by_grantee
            .entry(key.grantee.as_str())
            .or_default()
            .push(key.privilege);
    }

    for privileges in by_grantee.values_mut() {
        privileges.sort_by_key(ToString::to_string);
    }

    by_grantee
        .into_iter()
        .map(|(grantee, privileges)| GrantPrivilegesStatement {
            privileges: PrivilegeSpecification::Privileges(privileges),
            target: target.clone(),
            roles: vec![Ident::new_unchecked(grantee)],
        })
        .collect()
}

/// Compute REVOKE statements for grants that exist in `current` but not in
/// `desired` and not in `protected` (3-way set difference).
///
/// Grantee names are compared exactly and privilege types uppercased, matching
/// [`desired_grants`].
///
/// `protected` contains grants that should never be revoked (e.g., grants
/// originating from `ALTER DEFAULT PRIVILEGES`).
pub fn stale_grant_revocations(
    current: &[ObjectGrant],
    desired: &BTreeSet<GrantKey>,
    protected: &BTreeSet<GrantKey>,
    target: &GrantTargetSpecification<Raw>,
) -> Vec<RevokePrivilegesStatement<Raw>> {
    let mut revocations = Vec::new();
    for grant in current {
        let Some(privilege) = parse_privilege(&grant.privilege_type) else {
            crate::verbose!(
                "skipping revocation of unknown privilege '{}' on grantee '{}' (target: {:?})",
                grant.privilege_type,
                grant.grantee,
                target,
            );
            continue;
        };
        let key = GrantKey {
            grantee: grant.grantee.clone(),
            privilege,
        };
        if desired.contains(&key) || protected.contains(&key) {
            continue;
        }
        revocations.push(RevokePrivilegesStatement {
            privileges: PrivilegeSpecification::Privileges(vec![privilege]),
            target: target.clone(),
            roles: vec![Ident::new_unchecked(grant.grantee.clone())],
        });
    }
    revocations
}

/// Execute GRANT statements for missing grants.
pub async fn execute_grants(
    executor: &DeploymentExecutor<'_>,
    grants: &[GrantPrivilegesStatement<Raw>],
) -> Result<(), CliError> {
    for stmt in grants {
        executor.execute_sql(stmt).await?;
    }
    Ok(())
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
    use crate::cli::commands::reconcile::ObjectKind;
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
        ReconcileTarget::named(ObjectKind::Cluster, name)
            .grant_target()
            .unwrap()
    }

    fn network_policy_target(name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::named(ObjectKind::NetworkPolicy, name)
            .grant_target()
            .unwrap()
    }

    fn obj_id(db: &str, schema: &str, name: &str) -> ObjectId {
        ObjectId::new(db.to_string(), schema.to_string(), name.to_string())
    }

    fn table_target(db: &str, schema: &str, name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::item(ObjectKind::Table, &obj_id(db, schema, name))
            .grant_target()
            .unwrap()
    }

    fn secret_target(db: &str, schema: &str, name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::item(ObjectKind::Secret, &obj_id(db, schema, name))
            .grant_target()
            .unwrap()
    }

    fn connection_target(db: &str, schema: &str, name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::item(ObjectKind::Connection, &obj_id(db, schema, name))
            .grant_target()
            .unwrap()
    }

    fn source_target(db: &str, schema: &str, name: &str) -> GrantTargetSpecification<Raw> {
        ReconcileTarget::item(ObjectKind::Source, &obj_id(db, schema, name))
            .grant_target()
            .unwrap()
    }

    fn key(grantee: &str, privilege: Privilege) -> GrantKey {
        GrantKey {
            grantee: grantee.to_string(),
            privilege,
        }
    }

    /// Convert revocations to strings for easier assertion.
    fn to_strings(revocations: &[RevokePrivilegesStatement<Raw>]) -> Vec<String> {
        revocations.iter().map(|r| r.to_string()).collect()
    }

    #[mz_ore::test]
    fn test_desired_grants() {
        let cases = [
            (
                "GRANT USAGE ON CLUSTER c TO reader",
                ObjectKind::Cluster,
                vec![key("reader", Privilege::USAGE)],
            ),
            (
                "GRANT USAGE, CREATE ON CLUSTER c TO writer",
                ObjectKind::Cluster,
                vec![
                    key("writer", Privilege::USAGE),
                    key("writer", Privilege::CREATE),
                ],
            ),
            (
                "GRANT ALL ON CLUSTER c TO admin",
                ObjectKind::Cluster,
                vec![
                    key("admin", Privilege::USAGE),
                    key("admin", Privilege::CREATE),
                ],
            ),
            (
                "GRANT ALL ON SECRET db.public.s TO reader",
                ObjectKind::Secret,
                vec![key("reader", Privilege::USAGE)],
            ),
            (
                "GRANT USAGE ON CLUSTER c TO reader, writer",
                ObjectKind::Cluster,
                vec![
                    key("reader", Privilege::USAGE),
                    key("writer", Privilege::USAGE),
                ],
            ),
            (
                "GRANT USAGE ON CLUSTER c TO \"MyRole\"",
                ObjectKind::Cluster,
                vec![key("MyRole", Privilege::USAGE)],
            ),
            (
                "GRANT USAGE ON CLUSTER c TO MyRole",
                ObjectKind::Cluster,
                vec![key("myrole", Privilege::USAGE)],
            ),
            (
                "GRANT ALL ON TABLE db.public.t TO admin",
                ObjectKind::Table,
                vec![
                    key("admin", Privilege::SELECT),
                    key("admin", Privilege::INSERT),
                    key("admin", Privilege::UPDATE),
                    key("admin", Privilege::DELETE),
                ],
            ),
        ];

        for (sql, kind, expected) in cases {
            assert_eq!(
                desired_grants(&[parse_grant(sql)], kind.all_privileges()),
                expected.into_iter().collect(),
                "{sql}"
            );
        }

        let repeated = parse_grant("GRANT USAGE ON CLUSTER c TO reader");
        assert_eq!(
            desired_grants(
                &[repeated.clone(), repeated],
                ObjectKind::Cluster.all_privileges()
            ),
            BTreeSet::from([key("reader", Privilege::USAGE)])
        );
        assert!(desired_grants(&[], ObjectKind::Cluster.all_privileges()).is_empty());
    }

    #[mz_ore::test]
    fn test_stale_grant_revocations_no_stale() {
        let current = vec![make_object_grant("reader", "USAGE")];
        let mut desired = BTreeSet::new();
        desired.insert(key("reader", Privilege::USAGE));

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
        desired.insert(key("reader", Privilege::USAGE));

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
        desired.insert(key("reader", Privilege::USAGE));

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

    /// Privilege types are keywords, so casing in the catalog does not matter.
    #[mz_ore::test]
    fn test_stale_grant_revocations_privilege_case_insensitive() {
        let current = vec![make_object_grant("reader", "usage")];
        let mut desired = BTreeSet::new();
        desired.insert(key("reader", Privilege::USAGE));

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        assert!(revocations.is_empty());
    }

    /// Grantees are identifiers, so a grant held by `Reader` does not satisfy one
    /// declared for `reader`; the stale one is revoked.
    #[mz_ore::test]
    fn test_stale_grant_revocations_distinguish_roles_by_case() {
        let current = vec![make_object_grant("Reader", "usage")];
        let mut desired = BTreeSet::new();
        desired.insert(key("reader", Privilege::USAGE));

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        assert_eq!(
            to_strings(&revocations),
            vec!["REVOKE USAGE ON CLUSTER my_cluster FROM \"Reader\""]
        );
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
    fn test_stale_grant_revocations_render_target_kind() {
        let cases = [
            (
                make_object_grant("reader", "USAGE"),
                network_policy_target("my_policy"),
                "REVOKE USAGE ON NETWORK POLICY my_policy FROM reader",
            ),
            (
                make_object_grant("app", "USAGE"),
                connection_target("db", "public", "my_conn"),
                "REVOKE USAGE ON CONNECTION db.public.my_conn FROM app",
            ),
            (
                make_object_grant("app", "USAGE"),
                secret_target("db", "public", "my_secret"),
                "REVOKE USAGE ON SECRET db.public.my_secret FROM app",
            ),
            (
                make_object_grant("reader", "SELECT"),
                source_target("db", "public", "my_source"),
                "REVOKE SELECT ON TABLE db.public.my_source FROM reader",
            ),
        ];

        for (current, target, expected) in cases {
            let revocations =
                stale_grant_revocations(&[current], &BTreeSet::new(), &BTreeSet::new(), &target);
            assert_eq!(to_strings(&revocations), vec![expected]);
        }
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
        protected.insert(key("reader", Privilege::SELECT));

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
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
        let current = vec![make_object_grant("reader", "USAGE")];

        let target = cluster_target("my_cluster");
        let revocations = stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target);
        assert!(revocations.is_empty());
    }

    #[mz_ore::test]
    fn test_end_to_end_revoke_removed_grant() {
        // Project only declares USAGE for reader, but cluster also has CREATE for writer
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
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
        let desired = desired_grants(&[], ObjectKind::Cluster.all_privileges());
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
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
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
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
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
        let desired = desired_grants(&[g1, g2], ObjectKind::Cluster.all_privileges());

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

    /// Convert missing grants to strings for easier assertion.
    fn missing_to_strings(
        desired: &BTreeSet<GrantKey>,
        current: &[ObjectGrant],
        target: &GrantTargetSpecification<Raw>,
    ) -> Vec<String> {
        missing_grant_statements(desired, current, target)
            .iter()
            .map(|g| g.to_string())
            .collect()
    }

    #[mz_ore::test]
    fn test_missing_grants_nothing_missing() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER c TO reader");
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
        let current = vec![make_object_grant("reader", "USAGE")];
        let strings = missing_to_strings(&desired, &current, &cluster_target("c"));
        assert!(strings.is_empty(), "unexpected grants: {:?}", strings);
    }

    #[mz_ore::test]
    fn test_missing_grants_all_missing() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER c TO reader");
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
        let strings = missing_to_strings(&desired, &[], &cluster_target("c"));
        assert_eq!(strings.len(), 1);
        assert_eq!(strings[0], "GRANT USAGE ON CLUSTER c TO reader");
    }

    /// An authored `GRANT ALL` whose privileges are only partly held emits just
    /// the gap, named explicitly rather than as `ALL`.
    #[mz_ore::test]
    fn test_missing_grants_partial_all() {
        let grant = parse_grant("GRANT ALL ON TABLE \"db\".\"public\".\"t\" TO analyst");
        let desired = desired_grants(&[grant], ObjectKind::Table.all_privileges());
        let current = vec![
            make_object_grant("analyst", "SELECT"),
            make_object_grant("analyst", "INSERT"),
        ];
        let strings = missing_to_strings(&desired, &current, &table_target("db", "public", "t"));
        assert_eq!(strings.len(), 1);
        assert_eq!(
            strings[0],
            "GRANT DELETE, UPDATE ON TABLE db.public.t TO analyst"
        );
    }

    #[mz_ore::test]
    fn test_missing_grants_grouped_per_grantee() {
        let g1 = parse_grant("GRANT USAGE, CREATE ON CLUSTER c TO writer");
        let g2 = parse_grant("GRANT USAGE ON CLUSTER c TO reader");
        let desired = desired_grants(&[g1, g2], ObjectKind::Cluster.all_privileges());
        let current = vec![make_object_grant("writer", "USAGE")];
        let strings = missing_to_strings(&desired, &current, &cluster_target("c"));
        assert_eq!(
            strings,
            vec![
                "GRANT USAGE ON CLUSTER c TO reader",
                "GRANT CREATE ON CLUSTER c TO writer",
            ]
        );
    }

    /// Privilege casing in the catalog must not resurface a grant that is
    /// already held.
    #[mz_ore::test]
    fn test_missing_grants_privilege_case_insensitive() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER c TO reader");
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
        let current = vec![make_object_grant("reader", "usage")];
        let strings = missing_to_strings(&desired, &current, &cluster_target("c"));
        assert!(strings.is_empty(), "unexpected grants: {:?}", strings);
    }

    /// A privilege already supplied by ALTER DEFAULT PRIVILEGES shows up in
    /// `current`, so it is neither re-granted nor revoked.
    #[mz_ore::test]
    fn test_missing_grants_satisfied_by_default_privilege() {
        let grant = parse_grant("GRANT SELECT ON TABLE \"db\".\"public\".\"t\" TO analyst");
        let desired = desired_grants(&[grant], ObjectKind::Table.all_privileges());
        let current = vec![make_object_grant("analyst", "SELECT")];
        let target = table_target("db", "public", "t");
        assert!(missing_to_strings(&desired, &current, &target).is_empty());

        let protected = BTreeSet::from([key("analyst", Privilege::SELECT)]);
        let revocations = stale_grant_revocations(&current, &desired, &protected, &target);
        assert!(revocations.is_empty());
    }

    /// Role names are identifiers, so a grant authored for `"Reader"` must not be
    /// emitted for `reader`: they are different roles, and the second may not
    /// exist at all.
    #[mz_ore::test]
    fn test_missing_grants_preserve_exact_role_casing() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER c TO \"Reader\"");
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
        let strings = missing_to_strings(&desired, &[], &cluster_target("c"));
        assert_eq!(strings, vec!["GRANT USAGE ON CLUSTER c TO \"Reader\""]);
    }

    /// Two roles differing only by case are distinct, so a grant held by one does
    /// not satisfy a grant declared for the other.
    #[mz_ore::test]
    fn test_missing_grants_do_not_conflate_roles_by_case() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER c TO \"Reader\"");
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
        let current = vec![make_object_grant("reader", "USAGE")];
        let target = cluster_target("c");

        assert_eq!(
            missing_to_strings(&desired, &current, &target),
            vec!["GRANT USAGE ON CLUSTER c TO \"Reader\""]
        );
        assert_eq!(
            to_strings(&stale_grant_revocations(
                &current,
                &desired,
                &BTreeSet::new(),
                &target
            )),
            vec!["REVOKE USAGE ON CLUSTER c FROM reader"]
        );
    }

    /// `PUBLIC` reaches the diff as the `public` pseudo-role name, so a grant the
    /// catalog already holds for it is not re-emitted.
    #[mz_ore::test]
    fn test_public_grantee_round_trips() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER c TO PUBLIC");
        let desired = desired_grants(&[grant], ObjectKind::Cluster.all_privileges());
        let target = cluster_target("c");

        assert_eq!(
            missing_to_strings(&desired, &[], &target),
            vec!["GRANT USAGE ON CLUSTER c TO public"]
        );

        let current = vec![make_object_grant("public", "USAGE")];
        assert!(missing_to_strings(&desired, &current, &target).is_empty());
        assert!(stale_grant_revocations(&current, &desired, &BTreeSet::new(), &target).is_empty());
    }

    /// A stale grant to `PUBLIC` is revocable, which it is not when the grantee
    /// is dropped by an inner join to `mz_roles`.
    #[mz_ore::test]
    fn test_stale_public_grant_is_revoked() {
        let current = vec![make_object_grant("public", "USAGE")];
        let revocations = stale_grant_revocations(
            &current,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &cluster_target("c"),
        );
        assert_eq!(
            to_strings(&revocations),
            vec!["REVOKE USAGE ON CLUSTER c FROM public"]
        );
    }
}
