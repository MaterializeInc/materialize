// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reconcile the `ALTER DEFAULT PRIVILEGES` rules a database or schema mod file
//! declares.
//!
//! Unlike object grants, which are reconciled one privilege at a time, a rule is
//! replayed exactly as authored whenever the catalog does not already satisfy
//! it. A rule maps onto `mz_default_privileges` rows keyed by
//! (target role, grantee, object type), so the statement is already the unit the
//! server stores, and replaying the authored SQL beats reconstructing it from
//! catalog rows. When a rule's privileges cannot be enumerated, it is replayed
//! rather than assumed satisfied.

use crate::cli::CliError;
use crate::cli::executor::DeploymentExecutor;
use crate::client::Client;
use mz_sql_parser::ast::{
    AbbreviatedGrantOrRevokeStatement, AlterDefaultPrivilegesStatement, ObjectType,
    PrivilegeSpecification, Raw, TargetRoleSpecification,
};
use std::collections::BTreeSet;

/// The name the catalog uses for `PUBLIC`, which it stores as the `p`
/// pseudo-role in both the target-role and grantee positions.
const PUBLIC: &str = "public";

/// One `mz_default_privileges` row, keyed by everything that identifies it
/// within a single database or schema scope.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DefaultPrivilege {
    /// The role whose newly created objects receive the privilege.
    pub target_role: String,
    /// The object type, spelled as `mz_default_privileges` spells it: the
    /// object-type keyword lowercased, for example `table` or `network policy`.
    pub object_type: String,
    /// The role receiving the privilege.
    pub grantee: String,
    /// The privilege, uppercased.
    pub privilege: String,
}

/// The scope a mod file's default-privilege rules are attached to.
pub enum DefaultPrivilegeScope<'a> {
    Database(&'a str),
    Schema { database: &'a str, schema: &'a str },
}

impl DefaultPrivilegeScope<'_> {
    /// The default-privilege rules the catalog currently records for this scope.
    async fn current_privileges(
        &self,
        client: &Client,
    ) -> Result<BTreeSet<DefaultPrivilege>, CliError> {
        let introspection = client.introspection();
        let rows = match self {
            Self::Database(database) => {
                introspection
                    .get_database_default_privileges(database)
                    .await
            }
            Self::Schema { database, schema } => {
                introspection
                    .get_schema_default_privileges(database, schema)
                    .await
            }
        };
        Ok(rows.map_err(CliError::Connection)?.into_iter().collect())
    }
}

/// Reconcile the default-privilege rules for one scope, replaying only the rules
/// the catalog does not already satisfy.
pub async fn reconcile(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    scope: &DefaultPrivilegeScope<'_>,
    statements: &[AlterDefaultPrivilegesStatement<Raw>],
) -> Result<(), CliError> {
    if statements.is_empty() {
        return Ok(());
    }
    let current = scope.current_privileges(client).await?;
    for stmt in statements {
        if !is_satisfied(stmt, &current) {
            executor.execute_sql(stmt).await?;
        }
    }
    Ok(())
}

/// Whether the catalog already reflects a rule.
///
/// A `GRANT` rule is satisfied when every row it implies is present, a `REVOKE`
/// rule when none of them is. A rule whose privileges cannot be enumerated is
/// reported unsatisfied so it gets replayed.
pub fn is_satisfied(
    stmt: &AlterDefaultPrivilegesStatement<Raw>,
    current: &BTreeSet<DefaultPrivilege>,
) -> bool {
    let (privileges, object_type, grantees, is_grant) = match &stmt.grant_or_revoke {
        AbbreviatedGrantOrRevokeStatement::Grant(grant) => {
            (&grant.privileges, grant.object_type, &grant.grantees, true)
        }
        AbbreviatedGrantOrRevokeStatement::Revoke(revoke) => (
            &revoke.privileges,
            revoke.object_type,
            &revoke.revokees,
            false,
        ),
    };

    let Some(privilege_names) = privilege_names(privileges, object_type) else {
        return false;
    };

    let target_roles: Vec<String> = match &stmt.target_roles {
        TargetRoleSpecification::Roles(roles) => {
            roles.iter().map(|r| r.as_str().to_lowercase()).collect()
        }
        TargetRoleSpecification::AllRoles => vec![PUBLIC.to_string()],
    };
    let object_type = object_type.to_string().to_lowercase();

    let mut all_present = true;
    let mut any_present = false;
    for target_role in &target_roles {
        for grantee in grantees {
            for privilege in &privilege_names {
                let row = DefaultPrivilege {
                    target_role: target_role.clone(),
                    object_type: object_type.clone(),
                    grantee: grantee.as_str().to_lowercase(),
                    privilege: privilege.clone(),
                };
                if current.contains(&row) {
                    any_present = true;
                } else {
                    all_present = false;
                }
            }
        }
    }

    if is_grant { all_present } else { !any_present }
}

/// The privileges a rule names, expanding `ALL` for the object type.
///
/// Returns `None` for an object type whose `ALL` expansion is unknown, which the
/// caller treats as "cannot prove satisfied".
fn privilege_names(
    privileges: &PrivilegeSpecification,
    object_type: ObjectType,
) -> Option<Vec<String>> {
    match privileges {
        PrivilegeSpecification::Privileges(privileges) => Some(
            privileges
                .iter()
                .map(|p| p.to_string().to_uppercase())
                .collect(),
        ),
        // Mirrors the server's `mz_sql::rbac::all_object_privileges`, restricted
        // to the object types the `ALTER DEFAULT PRIVILEGES` grammar accepts:
        // TABLES, TYPES, SECRETS, CONNECTIONS, SCHEMAS, DATABASES, CLUSTERS.
        PrivilegeSpecification::All => match object_type {
            ObjectType::Table => Some(vec![
                "SELECT".into(),
                "INSERT".into(),
                "UPDATE".into(),
                "DELETE".into(),
            ]),
            ObjectType::Type | ObjectType::Secret | ObjectType::Connection => {
                Some(vec!["USAGE".into()])
            }
            ObjectType::Database | ObjectType::Schema | ObjectType::Cluster => {
                Some(vec!["USAGE".into(), "CREATE".into()])
            }
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_sql_parser::ast::Statement;
    use mz_sql_parser::parser::parse_statements;

    fn parse_adp(sql: &str) -> AlterDefaultPrivilegesStatement<Raw> {
        let stmts = parse_statements(sql).unwrap();
        match stmts.into_iter().next().unwrap().ast {
            Statement::AlterDefaultPrivileges(a) => a,
            other => panic!("expected ALTER DEFAULT PRIVILEGES, got: {}", other),
        }
    }

    fn row(
        target_role: &str,
        object_type: &str,
        grantee: &str,
        privilege: &str,
    ) -> DefaultPrivilege {
        DefaultPrivilege {
            target_role: target_role.to_string(),
            object_type: object_type.to_string(),
            grantee: grantee.to_string(),
            privilege: privilege.to_string(),
        }
    }

    fn current(rows: Vec<DefaultPrivilege>) -> BTreeSet<DefaultPrivilege> {
        rows.into_iter().collect()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_grant_rule_satisfied_when_row_present() {
        let stmt = parse_adp(
            "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
             GRANT SELECT ON TABLES TO analyst",
        );
        let rows = current(vec![row("owner", "table", "analyst", "SELECT")]);
        assert!(is_satisfied(&stmt, &rows));
        assert!(!is_satisfied(&stmt, &BTreeSet::new()));
    }

    /// Every row a rule implies has to be present, not just one of them.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_grant_rule_needs_every_row() {
        let stmt = parse_adp(
            "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
             GRANT SELECT ON TABLES TO analyst, auditor",
        );
        let partial = current(vec![row("owner", "table", "analyst", "SELECT")]);
        assert!(!is_satisfied(&stmt, &partial));

        let complete = current(vec![
            row("owner", "table", "analyst", "SELECT"),
            row("owner", "table", "auditor", "SELECT"),
        ]);
        assert!(is_satisfied(&stmt, &complete));
    }

    /// `ALL ON TABLES` expands to the four table privileges, matching the
    /// server's own mapping.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_grant_all_expands_for_tables() {
        let stmt = parse_adp(
            "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
             GRANT ALL ON TABLES TO analyst",
        );
        let partial = current(vec![row("owner", "table", "analyst", "SELECT")]);
        assert!(!is_satisfied(&stmt, &partial));

        let complete = current(
            ["SELECT", "INSERT", "UPDATE", "DELETE"]
                .into_iter()
                .map(|p| row("owner", "table", "analyst", p))
                .collect(),
        );
        assert!(is_satisfied(&stmt, &complete));
    }

    /// `FOR ALL ROLES` and a `PUBLIC` grantee are both the `p` pseudo-role,
    /// which the catalog query reports as `public`.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_all_roles_and_public_grantee() {
        let stmt = parse_adp(
            "ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA db.app \
             GRANT USAGE ON SECRETS TO PUBLIC",
        );
        let rows = current(vec![row("public", "secret", "public", "USAGE")]);
        assert!(is_satisfied(&stmt, &rows));
    }

    /// A revoke rule is satisfied only while none of its rows exists.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_revoke_rule_inverts_the_test() {
        let stmt = parse_adp(
            "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
             REVOKE SELECT ON TABLES FROM analyst",
        );
        assert!(is_satisfied(&stmt, &BTreeSet::new()));

        let rows = current(vec![row("owner", "table", "analyst", "SELECT")]);
        assert!(!is_satisfied(&stmt, &rows));
    }

    /// `ALL ON CLUSTERS` is `USAGE` plus `CREATE`, and the object type is keyed
    /// the way `mz_default_privileges` spells it.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_grant_all_expands_for_clusters() {
        let stmt = parse_adp(
            "ALTER DEFAULT PRIVILEGES FOR ROLE owner \
             GRANT ALL ON CLUSTERS TO analyst",
        );
        let partial = current(vec![row("owner", "cluster", "analyst", "USAGE")]);
        assert!(!is_satisfied(&stmt, &partial));

        let complete = current(vec![
            row("owner", "cluster", "analyst", "USAGE"),
            row("owner", "cluster", "analyst", "CREATE"),
        ]);
        assert!(is_satisfied(&stmt, &complete));
    }

    /// Role names are compared case-insensitively, matching how the catalog
    /// query lowercases them.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_role_names_case_insensitive() {
        let stmt = parse_adp(
            "ALTER DEFAULT PRIVILEGES FOR ROLE \"Owner\" IN SCHEMA db.app \
             GRANT SELECT ON TABLES TO \"Analyst\"",
        );
        let rows = current(vec![row("owner", "table", "analyst", "SELECT")]);
        assert!(is_satisfied(&stmt, &rows));
    }
}
