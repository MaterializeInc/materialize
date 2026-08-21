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
//! A rule maps onto `mz_default_privileges` rows keyed by
//! (target role, object type, grantee, privilege). The declared rules are folded
//! into the row set the project wants — an abbreviated `GRANT` adds rows, an
//! abbreviated `REVOKE` takes them away — and that set is diffed against the
//! catalog. So a privilege the project stops declaring is revoked rather than
//! left behind, the same way object grants and comments are reconciled.
//!
//! Only rows the scope itself owns take part. The identity queries match on the
//! scope's own database or schema id, so a global rule, or one belonging to a
//! different scope, is never revoked from here.

use crate::cli::CliError;
use crate::cli::commands::grants::parse_privilege;
use crate::cli::executor::DeploymentExecutor;
use crate::client::{Client, DefaultPrivilege};
use crate::verbose;
use mz_sql_parser::ast::{
    AbbreviatedGrantOrRevokeStatement, AbbreviatedGrantStatement, AbbreviatedRevokeStatement,
    AlterDefaultPrivilegesStatement, GrantTargetAllSpecification, Ident, ObjectType, Privilege,
    PrivilegeSpecification, Raw, TargetRoleSpecification, UnresolvedDatabaseName,
    UnresolvedSchemaName,
};
use std::collections::{BTreeMap, BTreeSet};

/// The name the catalog reports for `PUBLIC`, which it stores as the `p`
/// pseudo-role in both the target-role and grantee positions.
///
/// `PUBLIC` folds to this when parsed as a role name, so the same spelling round
/// trips through both the catalog and the AST.
const PUBLIC: &str = "public";

/// The scope a mod file's default-privilege rules are attached to.
pub enum DefaultPrivilegeScope<'a> {
    Database(&'a str),
    Schema { database: &'a str, schema: &'a str },
}

impl DefaultPrivilegeScope<'_> {
    /// The rules the catalog currently records against this scope.
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

    /// The `IN DATABASE` or `IN SCHEMA` clause a synthesized rule carries.
    fn target_objects(&self) -> GrantTargetAllSpecification<Raw> {
        match self {
            Self::Database(database) => GrantTargetAllSpecification::AllDatabases {
                databases: vec![UnresolvedDatabaseName(Ident::new_unchecked(*database))],
            },
            Self::Schema { database, schema } => GrantTargetAllSpecification::AllSchemas {
                schemas: vec![UnresolvedSchemaName(vec![
                    Ident::new_unchecked(*database),
                    Ident::new_unchecked(*schema),
                ])],
            },
        }
    }
}

/// Reconcile the default-privilege rules for one scope.
pub async fn reconcile(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    scope: &DefaultPrivilegeScope<'_>,
    statements: &[AlterDefaultPrivilegesStatement<Raw>],
) -> Result<(), CliError> {
    let current = scope.current_privileges(client).await?;
    let Some(desired) = declared_privileges(statements) else {
        // The declared state cannot be enumerated, so revoking anything risks
        // removing a rule the project still wants. Replay instead.
        verbose!("replaying default privileges verbatim: declared state is not enumerable");
        for stmt in statements {
            executor.execute_sql(stmt).await?;
        }
        return Ok(());
    };
    for stmt in privilege_changes(&desired, &current, scope) {
        executor.execute_sql(&stmt).await?;
    }
    Ok(())
}

/// Fold the declared rules into the row set the project wants.
///
/// Rules apply in file order: an abbreviated `GRANT` adds its rows and an
/// abbreviated `REVOKE` takes them away, so a project can grant broadly and then
/// carve out an exception, matching the order the server would have applied the
/// statements in.
///
/// Returns `None` when a rule names privileges that cannot be enumerated, which
/// leaves the declared state unknowable.
pub fn declared_privileges(
    statements: &[AlterDefaultPrivilegesStatement<Raw>],
) -> Option<BTreeSet<DefaultPrivilege>> {
    let mut declared = BTreeSet::new();
    for stmt in statements {
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
        let privilege_names = privilege_names(privileges, object_type)?;
        let object_type = object_type.to_string().to_lowercase();

        for target_role in target_role_names(&stmt.target_roles) {
            for grantee in grantees {
                for privilege in &privilege_names {
                    let row = DefaultPrivilege {
                        target_role: target_role.clone(),
                        object_type: object_type.clone(),
                        grantee: grantee.as_str().to_string(),
                        privilege: privilege.clone(),
                    };
                    if is_grant {
                        declared.insert(row);
                    } else {
                        declared.remove(&row);
                    }
                }
            }
        }
    }
    Some(declared)
}

/// Compute the rules that close the gap between `desired` and `current`.
///
/// Rows are grouped by (target role, object type, grantee) so one statement
/// carries every privilege that triple gains or loses. Grants come before
/// revocations.
pub fn privilege_changes(
    desired: &BTreeSet<DefaultPrivilege>,
    current: &BTreeSet<DefaultPrivilege>,
    scope: &DefaultPrivilegeScope<'_>,
) -> Vec<AlterDefaultPrivilegesStatement<Raw>> {
    let mut changes = rules_for(desired.difference(current), scope, true);
    changes.extend(rules_for(current.difference(desired), scope, false));
    changes
}

/// Build one rule per (target role, object type, grantee) triple in `rows`.
fn rules_for<'a>(
    rows: impl Iterator<Item = &'a DefaultPrivilege>,
    scope: &DefaultPrivilegeScope<'_>,
    grant: bool,
) -> Vec<AlterDefaultPrivilegesStatement<Raw>> {
    let mut grouped: BTreeMap<(&str, &str, &str), Vec<Privilege>> = BTreeMap::new();
    for row in rows {
        let Some(privilege) = parse_privilege(&row.privilege) else {
            verbose!(
                "skipping unknown default privilege '{}' for grantee '{}' on {}",
                row.privilege,
                row.grantee,
                row.object_type,
            );
            continue;
        };
        grouped
            .entry((
                row.target_role.as_str(),
                row.object_type.as_str(),
                row.grantee.as_str(),
            ))
            .or_default()
            .push(privilege);
    }

    grouped
        .into_iter()
        .filter_map(|((target_role, object_type, grantee), privileges)| {
            let Some(object_type) = parse_object_type(object_type) else {
                verbose!(
                    "skipping default privileges on unknown object type '{}'",
                    object_type
                );
                return None;
            };
            let privileges = PrivilegeSpecification::Privileges(privileges);
            let grantees = vec![Ident::new_unchecked(grantee)];
            let grant_or_revoke = if grant {
                AbbreviatedGrantOrRevokeStatement::Grant(AbbreviatedGrantStatement {
                    privileges,
                    object_type,
                    grantees,
                })
            } else {
                AbbreviatedGrantOrRevokeStatement::Revoke(AbbreviatedRevokeStatement {
                    privileges,
                    object_type,
                    revokees: grantees,
                })
            };
            Some(AlterDefaultPrivilegesStatement {
                target_roles: target_role_spec(target_role),
                target_objects: scope.target_objects(),
                grant_or_revoke,
            })
        })
        .collect()
}

/// The target roles a rule names, as the catalog spells them.
fn target_role_names(target_roles: &TargetRoleSpecification<Raw>) -> Vec<String> {
    match target_roles {
        TargetRoleSpecification::Roles(roles) => {
            roles.iter().map(|r| r.as_str().to_string()).collect()
        }
        TargetRoleSpecification::AllRoles => vec![PUBLIC.to_string()],
    }
}

/// The inverse of [`target_role_names`] for a single role.
fn target_role_spec(target_role: &str) -> TargetRoleSpecification<Raw> {
    if target_role == PUBLIC {
        TargetRoleSpecification::AllRoles
    } else {
        TargetRoleSpecification::Roles(vec![Ident::new_unchecked(target_role)])
    }
}

/// Parse the object type back out of the spelling `mz_default_privileges` uses.
///
/// Covers the object types the `ALTER DEFAULT PRIVILEGES` grammar accepts, which
/// are the only ones that can produce a row.
fn parse_object_type(object_type: &str) -> Option<ObjectType> {
    match object_type {
        "table" => Some(ObjectType::Table),
        "type" => Some(ObjectType::Type),
        "secret" => Some(ObjectType::Secret),
        "connection" => Some(ObjectType::Connection),
        "database" => Some(ObjectType::Database),
        "schema" => Some(ObjectType::Schema),
        "cluster" => Some(ObjectType::Cluster),
        _ => None,
    }
}

/// The privileges a rule names, expanding `ALL` for the object type.
///
/// Returns `None` for an object type whose `ALL` expansion is unknown, which the
/// caller treats as an unknowable declared state.
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

    const SCOPE: DefaultPrivilegeScope<'static> = DefaultPrivilegeScope::Schema {
        database: "db",
        schema: "app",
    };

    fn parse_adp(sql: &str) -> AlterDefaultPrivilegesStatement<Raw> {
        let stmts = parse_statements(sql).unwrap();
        match stmts.into_iter().next().unwrap().ast {
            Statement::AlterDefaultPrivileges(a) => a,
            other => panic!("expected ALTER DEFAULT PRIVILEGES, got: {}", other),
        }
    }

    fn declared(sql: &[&str]) -> BTreeSet<DefaultPrivilege> {
        let stmts: Vec<_> = sql.iter().map(|s| parse_adp(s)).collect();
        declared_privileges(&stmts).expect("declared state is enumerable")
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

    fn changes(desired: &[&str], current_rows: Vec<DefaultPrivilege>) -> Vec<String> {
        privilege_changes(&declared(desired), &current(current_rows), &SCOPE)
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_declared_grant_produces_a_row() {
        let rows = declared(
            &["ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
             GRANT SELECT ON TABLES TO analyst"],
        );
        assert_eq!(
            rows,
            current(vec![row("owner", "table", "analyst", "SELECT")])
        );
    }

    /// A later `REVOKE` carves a row back out, the way the server would have
    /// applied the statements in order.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_declared_revoke_carves_out_a_grant() {
        let rows = declared(&[
            "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
             GRANT SELECT, INSERT ON TABLES TO analyst",
            "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
             REVOKE INSERT ON TABLES FROM analyst",
        ]);
        assert_eq!(
            rows,
            current(vec![row("owner", "table", "analyst", "SELECT")])
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_declared_all_expands_per_object_type() {
        let tables = declared(
            &["ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
             GRANT ALL ON TABLES TO analyst"],
        );
        assert_eq!(
            tables,
            current(
                ["SELECT", "INSERT", "UPDATE", "DELETE"]
                    .into_iter()
                    .map(|p| row("owner", "table", "analyst", p))
                    .collect()
            )
        );

        let clusters =
            declared(&["ALTER DEFAULT PRIVILEGES FOR ROLE owner GRANT ALL ON CLUSTERS TO analyst"]);
        assert_eq!(
            clusters,
            current(vec![
                row("owner", "cluster", "analyst", "USAGE"),
                row("owner", "cluster", "analyst", "CREATE"),
            ])
        );
    }

    /// `FOR ALL ROLES` and a `PUBLIC` grantee are both the `p` pseudo-role, which
    /// the catalog reports as `public`.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_declared_all_roles_and_public_grantee() {
        let rows = declared(&["ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA db.app \
             GRANT USAGE ON SECRETS TO PUBLIC"]);
        assert_eq!(
            rows,
            current(vec![row("public", "secret", "public", "USAGE")])
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_converged_scope_emits_nothing() {
        let declared_sql = ["ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
             GRANT SELECT ON TABLES TO analyst"];
        let rows = vec![row("owner", "table", "analyst", "SELECT")];
        assert!(changes(&declared_sql, rows).is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_missing_row_is_granted() {
        assert_eq!(
            changes(
                &["ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
                   GRANT SELECT ON TABLES TO analyst"],
                vec![],
            ),
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
                 GRANT SELECT ON TABLES TO analyst"
            ]
        );
    }

    /// Dropping the last rule revokes the rows it used to declare, rather than
    /// leaving them behind.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_dropping_every_rule_revokes_what_is_left() {
        assert_eq!(
            changes(&[], vec![row("owner", "table", "analyst", "SELECT")]),
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
                 REVOKE SELECT ON TABLES FROM analyst"
            ]
        );
    }

    /// Changing the privilege grants the new one and revokes the old one.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_changed_privilege_grants_and_revokes() {
        assert_eq!(
            changes(
                &["ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
                   GRANT INSERT ON TABLES TO analyst"],
                vec![row("owner", "table", "analyst", "SELECT")],
            ),
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
                 GRANT INSERT ON TABLES TO analyst",
                "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
                 REVOKE SELECT ON TABLES FROM analyst",
            ]
        );
    }

    /// Privileges for one triple collapse into a single statement.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_privileges_group_per_triple() {
        assert_eq!(
            changes(
                &["ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
                   GRANT SELECT, INSERT ON TABLES TO analyst"],
                vec![],
            ),
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA db.app \
                 GRANT INSERT, SELECT ON TABLES TO analyst"
            ]
        );
    }

    /// A synthesized rule for the `p` pseudo-role renders as `FOR ALL ROLES` and
    /// `TO public`, which is how it round trips.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_public_round_trips_through_a_synthesized_rule() {
        assert_eq!(
            changes(&[], vec![row("public", "secret", "public", "USAGE")]),
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA db.app \
                 REVOKE USAGE ON SECRETS FROM public"
            ]
        );
    }

    /// Role names are identifiers, so their exact casing survives into the
    /// emitted SQL rather than being folded.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_exact_role_casing_is_preserved() {
        assert_eq!(
            changes(&[], vec![row("Owner", "table", "Analyst", "SELECT")]),
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"Owner\" IN SCHEMA db.app \
                 REVOKE SELECT ON TABLES FROM \"Analyst\""
            ]
        );
    }

    /// A database scope renders `IN DATABASE`.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function 'rust_psm_stack_pointer'
    fn test_database_scope_renders_in_database() {
        let rendered: Vec<String> = privilege_changes(
            &BTreeSet::new(),
            &current(vec![row("owner", "schema", "analyst", "USAGE")]),
            &DefaultPrivilegeScope::Database("db"),
        )
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            rendered,
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN DATABASE db \
                 REVOKE USAGE ON SCHEMAS FROM analyst"
            ]
        );
    }
}
