//! Shared helpers for grant reconciliation across apply commands.

use crate::cli::CliError;
use crate::cli::executor::DeploymentExecutor;
use crate::client::{Client, ObjectGrant, quote_identifier};
use mz_sql_parser::ast::{GrantPrivilegesStatement, PrivilegeSpecification, Raw};
use owo_colors::OwoColorize;
use std::collections::BTreeSet;
use std::fmt;

/// The kind of database object for grant reconciliation.
///
/// Groups the catalog table name, SQL keyword, privilege set, and display label
/// that vary per object type so callers don't have to pass four loose strings.
pub enum GrantObjectKind {
    Table,
    Source,
    Secret,
    Connection,
}

impl GrantObjectKind {
    pub fn catalog_table(&self) -> &'static str {
        match self {
            Self::Table => "mz_tables",
            Self::Source => "mz_sources",
            Self::Secret => "mz_secrets",
            Self::Connection => "mz_connections",
        }
    }

    pub fn sql_keyword(&self) -> &'static str {
        match self {
            Self::Table => "TABLE",
            Self::Source => "SOURCE",
            Self::Secret => "SECRET",
            Self::Connection => "CONNECTION",
        }
    }

    pub fn all_privileges(&self) -> &'static [&'static str] {
        match self {
            Self::Table => &["SELECT", "INSERT", "UPDATE", "DELETE"],
            Self::Source => &["SELECT"],
            Self::Secret | Self::Connection => &["USAGE"],
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Source => "source",
            Self::Secret => "secret",
            Self::Connection => "connection",
        }
    }
}

/// Reconcile grants for a single object: apply desired grants, revoke stale ones.
pub async fn reconcile(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    obj_id: &crate::project::object_id::ObjectId,
    grants: &[GrantPrivilegesStatement<Raw>],
    kind: &GrantObjectKind,
) -> Result<(), CliError> {
    for grant in grants {
        executor.execute_sql(grant).await?;
    }
    let fqn = quoted_fqn(&obj_id.database, &obj_id.schema, &obj_id.object);
    let current = client
        .introspection()
        .get_database_object_grants(
            kind.catalog_table(),
            &obj_id.database,
            &obj_id.schema,
            &obj_id.object,
        )
        .await
        .map_err(CliError::Connection)?;
    let desired = desired_grants(grants, kind.all_privileges());
    let revocations = stale_grant_revocations(&current, &desired, kind.sql_keyword(), &fqn);
    execute_revocations(executor, &revocations, kind.label(), obj_id).await
}

/// Build a quoted fully-qualified name from components.
pub fn quoted_fqn(database: &str, schema: &str, name: &str) -> String {
    format!(
        "{}.{}.{}",
        quote_identifier(database),
        quote_identifier(schema),
        quote_identifier(name),
    )
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

/// Compute REVOKE SQL strings for grants that exist in `current` but not in `desired`.
pub fn stale_grant_revocations(
    current: &[ObjectGrant],
    desired: &BTreeSet<(String, String)>,
    object_type_keyword: &str,
    object_name_sql: &str,
) -> Vec<String> {
    let mut revocations = Vec::new();
    for grant in current {
        let key = (
            grant.grantee.to_lowercase(),
            grant.privilege_type.to_uppercase(),
        );
        if !desired.contains(&key) {
            revocations.push(format!(
                "REVOKE {} ON {} {} FROM \"{}\"",
                grant.privilege_type, object_type_keyword, object_name_sql, grant.grantee
            ));
        }
    }
    revocations
}

/// Execute REVOKE statements for stale grants, printing status for each.
pub async fn execute_revocations(
    executor: &DeploymentExecutor<'_>,
    revocations: &[String],
    object_type_label: &str,
    display_name: &impl fmt::Display,
) -> Result<(), CliError> {
    for sql in revocations {
        if !executor.is_dry_run() {
            println!(
                "  {} Revoking stale grant on {} '{}'",
                "-".red().bold(),
                object_type_label,
                display_name,
            );
        }
        executor.execute_sql(sql).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    // =========================================================================
    // quoted_fqn tests
    // =========================================================================

    #[test]
    fn test_quoted_fqn_simple() {
        assert_eq!(
            quoted_fqn("db", "public", "my_table"),
            "\"db\".\"public\".\"my_table\""
        );
    }

    #[test]
    fn test_quoted_fqn_with_special_chars() {
        assert_eq!(
            quoted_fqn("my db", "my schema", "my\"table"),
            "\"my db\".\"my schema\".\"my\"\"table\""
        );
    }

    // =========================================================================
    // desired_grants tests
    // =========================================================================

    #[test]
    fn test_desired_grants_single_privilege_single_role() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let result = desired_grants(&[grant], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&("reader".to_string(), "USAGE".to_string())));
    }

    #[test]
    fn test_desired_grants_multiple_privileges() {
        let grant = parse_grant("GRANT USAGE, CREATE ON CLUSTER my_cluster TO writer");
        let result = desired_grants(&[grant], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("writer".to_string(), "USAGE".to_string())));
        assert!(result.contains(&("writer".to_string(), "CREATE".to_string())));
    }

    #[test]
    fn test_desired_grants_all_expands_to_object_type_privileges() {
        let grant = parse_grant("GRANT ALL ON CLUSTER my_cluster TO admin");
        // For clusters, ALL = USAGE + CREATE
        let result = desired_grants(&[grant], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("admin".to_string(), "USAGE".to_string())));
        assert!(result.contains(&("admin".to_string(), "CREATE".to_string())));
    }

    #[test]
    fn test_desired_grants_all_with_single_privilege_object_type() {
        let grant = parse_grant("GRANT ALL ON SECRET \"db\".\"public\".\"my_secret\" TO reader");
        // For secrets, ALL = USAGE only
        let result = desired_grants(&[grant], &["USAGE"]);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&("reader".to_string(), "USAGE".to_string())));
    }

    #[test]
    fn test_desired_grants_multiple_roles() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader, writer");
        let result = desired_grants(&[grant], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("reader".to_string(), "USAGE".to_string())));
        assert!(result.contains(&("writer".to_string(), "USAGE".to_string())));
    }

    #[test]
    fn test_desired_grants_multiple_grant_statements() {
        let g1 = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let g2 = parse_grant("GRANT CREATE ON CLUSTER my_cluster TO writer");
        let result = desired_grants(&[g1, g2], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("reader".to_string(), "USAGE".to_string())));
        assert!(result.contains(&("writer".to_string(), "CREATE".to_string())));
    }

    #[test]
    fn test_desired_grants_deduplicates() {
        // Two grant statements granting the same privilege to the same role
        let g1 = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let g2 = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let result = desired_grants(&[g1, g2], &["USAGE", "CREATE"]);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_desired_grants_empty_input() {
        let result = desired_grants(&[], &["USAGE", "CREATE"]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_desired_grants_role_name_lowercased() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO \"MyRole\"");
        let result = desired_grants(&[grant], &["USAGE"]);
        assert!(result.contains(&("myrole".to_string(), "USAGE".to_string())));
    }

    #[test]
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

    // =========================================================================
    // stale_grant_revocations tests
    // =========================================================================

    #[test]
    fn test_stale_grant_revocations_no_stale() {
        let current = vec![make_object_grant("reader", "USAGE")];
        let mut desired = BTreeSet::new();
        desired.insert(("reader".to_string(), "USAGE".to_string()));

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"my_cluster\"");
        assert!(revocations.is_empty());
    }

    #[test]
    fn test_stale_grant_revocations_has_stale() {
        let current = vec![
            make_object_grant("reader", "USAGE"),
            make_object_grant("writer", "CREATE"),
        ];
        let mut desired = BTreeSet::new();
        desired.insert(("reader".to_string(), "USAGE".to_string()));

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"my_cluster\"");
        assert_eq!(revocations.len(), 1);
        assert_eq!(
            revocations[0],
            "REVOKE CREATE ON CLUSTER \"my_cluster\" FROM \"writer\""
        );
    }

    #[test]
    fn test_stale_grant_revocations_empty_desired() {
        let current = vec![make_object_grant("reader", "USAGE")];
        let desired = BTreeSet::new();

        let revocations =
            stale_grant_revocations(&current, &desired, "TABLE", "\"db\".\"public\".\"t\"");
        assert_eq!(revocations.len(), 1);
        assert_eq!(
            revocations[0],
            "REVOKE USAGE ON TABLE \"db\".\"public\".\"t\" FROM \"reader\""
        );
    }

    #[test]
    fn test_stale_grant_revocations_empty_current() {
        let mut desired = BTreeSet::new();
        desired.insert(("reader".to_string(), "USAGE".to_string()));

        let revocations = stale_grant_revocations(&[], &desired, "CLUSTER", "\"my_cluster\"");
        assert!(revocations.is_empty());
    }

    #[test]
    fn test_stale_grant_revocations_both_empty() {
        let revocations = stale_grant_revocations(&[], &BTreeSet::new(), "SECRET", "\"s\"");
        assert!(revocations.is_empty());
    }

    #[test]
    fn test_stale_grant_revocations_case_insensitive_match() {
        // Current has mixed case, desired has lowercase — should still match
        let current = vec![make_object_grant("Reader", "usage")];
        let mut desired = BTreeSet::new();
        desired.insert(("reader".to_string(), "USAGE".to_string()));

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"my_cluster\"");
        assert!(revocations.is_empty());
    }

    #[test]
    fn test_stale_grant_revocations_multiple_stale() {
        let current = vec![
            make_object_grant("reader", "USAGE"),
            make_object_grant("writer", "CREATE"),
            make_object_grant("admin", "USAGE"),
        ];
        let desired = BTreeSet::new(); // All grants removed

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"my_cluster\"");
        assert_eq!(revocations.len(), 3);
    }

    #[test]
    fn test_stale_grant_revocations_network_policy_keyword() {
        let current = vec![make_object_grant("reader", "USAGE")];
        let desired = BTreeSet::new();

        let revocations =
            stale_grant_revocations(&current, &desired, "NETWORK POLICY", "\"my_policy\"");
        assert_eq!(revocations.len(), 1);
        assert_eq!(
            revocations[0],
            "REVOKE USAGE ON NETWORK POLICY \"my_policy\" FROM \"reader\""
        );
    }

    #[test]
    fn test_stale_grant_revocations_connection_keyword() {
        let current = vec![make_object_grant("app", "USAGE")];
        let desired = BTreeSet::new();

        let revocations = stale_grant_revocations(
            &current,
            &desired,
            "CONNECTION",
            "\"db\".\"public\".\"my_conn\"",
        );
        assert_eq!(revocations.len(), 1);
        assert_eq!(
            revocations[0],
            "REVOKE USAGE ON CONNECTION \"db\".\"public\".\"my_conn\" FROM \"app\""
        );
    }

    #[test]
    fn test_stale_grant_revocations_secret_keyword() {
        let current = vec![make_object_grant("app", "USAGE")];
        let desired = BTreeSet::new();

        let revocations = stale_grant_revocations(
            &current,
            &desired,
            "SECRET",
            "\"db\".\"public\".\"my_secret\"",
        );
        assert_eq!(revocations.len(), 1);
        assert_eq!(
            revocations[0],
            "REVOKE USAGE ON SECRET \"db\".\"public\".\"my_secret\" FROM \"app\""
        );
    }

    #[test]
    fn test_stale_grant_revocations_source_keyword() {
        let current = vec![make_object_grant("reader", "SELECT")];
        let desired = BTreeSet::new();

        let revocations = stale_grant_revocations(
            &current,
            &desired,
            "SOURCE",
            "\"db\".\"public\".\"my_source\"",
        );
        assert_eq!(revocations.len(), 1);
        assert_eq!(
            revocations[0],
            "REVOKE SELECT ON SOURCE \"db\".\"public\".\"my_source\" FROM \"reader\""
        );
    }

    // =========================================================================
    // Integration: desired_grants + stale_grant_revocations together
    // =========================================================================

    #[test]
    fn test_end_to_end_no_revocations_when_grants_match() {
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let desired = desired_grants(&[grant], &["USAGE", "CREATE"]);
        let current = vec![make_object_grant("reader", "USAGE")];

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"my_cluster\"");
        assert!(revocations.is_empty());
    }

    #[test]
    fn test_end_to_end_revoke_removed_grant() {
        // Project only declares USAGE for reader, but cluster also has CREATE for writer
        let grant = parse_grant("GRANT USAGE ON CLUSTER my_cluster TO reader");
        let desired = desired_grants(&[grant], &["USAGE", "CREATE"]);
        let current = vec![
            make_object_grant("reader", "USAGE"),
            make_object_grant("writer", "CREATE"),
        ];

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"my_cluster\"");
        assert_eq!(revocations.len(), 1);
        assert!(revocations[0].contains("writer"));
        assert!(revocations[0].contains("CREATE"));
    }

    #[test]
    fn test_end_to_end_revoke_all_when_grants_removed() {
        // No grants declared in project, but cluster has grants
        let desired = desired_grants(&[], &["USAGE", "CREATE"]);
        let current = vec![
            make_object_grant("reader", "USAGE"),
            make_object_grant("writer", "CREATE"),
        ];

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"my_cluster\"");
        assert_eq!(revocations.len(), 2);
    }

    #[test]
    fn test_end_to_end_grant_all_covers_all_current() {
        let grant = parse_grant("GRANT ALL ON CLUSTER my_cluster TO admin");
        let desired = desired_grants(&[grant], &["USAGE", "CREATE"]);
        // admin has both USAGE and CREATE — both covered by ALL
        let current = vec![
            make_object_grant("admin", "USAGE"),
            make_object_grant("admin", "CREATE"),
        ];

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"my_cluster\"");
        assert!(revocations.is_empty());
    }

    #[test]
    fn test_end_to_end_grant_all_still_revokes_other_roles() {
        let grant = parse_grant("GRANT ALL ON CLUSTER my_cluster TO admin");
        let desired = desired_grants(&[grant], &["USAGE", "CREATE"]);
        // admin is covered, but reader is not in the project file
        let current = vec![
            make_object_grant("admin", "USAGE"),
            make_object_grant("admin", "CREATE"),
            make_object_grant("reader", "USAGE"),
        ];

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"my_cluster\"");
        assert_eq!(revocations.len(), 1);
        assert!(revocations[0].contains("reader"));
    }

    #[test]
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

        let revocations = stale_grant_revocations(&current, &desired, "CLUSTER", "\"c\"");
        assert_eq!(revocations.len(), 1);
        assert!(revocations[0].contains("admin"));
    }
}
