//! Apply connections command - create missing connections and reconcile drifted ones.

use crate::cli::CliError;
use crate::cli::commands::apply_objects::{self, HandleResult};
use crate::cli::commands::grants;
use crate::cli::executor::ObjectAction;
use crate::cli::executor::{ApplyPlan, ApplyResult, DeploymentExecutor};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;
use crate::project::object_id::ObjectId;
use crate::project::typed;
use crate::secret_resolver::SecretResolver;
use mz_sql_parser::ast::{
    AlterConnectionAction, AlterConnectionStatement, ConnectionOption, ConnectionOptionName, Raw,
    Statement as ParserStatement,
};
use mz_sql_parser::parser::parse_statements;
use std::collections::BTreeMap;

const PHASE_NAME: &str = "connections";
const GRANT_KIND: grants::GrantObjectKind = grants::GrantObjectKind::Connection;

fn matches(stmt: &Statement) -> bool {
    matches!(stmt, Statement::CreateConnection(_))
}

struct Connections {
    resolver: SecretResolver,
}

impl Connections {
    fn new(settings: &Settings) -> Result<Self, CliError> {
        Ok(Connections {
            resolver: SecretResolver::new(&settings.profile_config.security),
        })
    }

    async fn handle_existing(
        &self,
        client: &Client,
        executor: &DeploymentExecutor<'_>,
        obj_id: &ObjectId,
        typed_obj: &typed::DatabaseObject,
    ) -> Result<HandleResult, CliError> {
        let Statement::CreateConnection(ref create_stmt) = typed_obj.stmt else {
            unreachable!("filtered for CreateConnection");
        };

        let resolved_stmt = match self
            .resolver
            .resolve_statement_for_cli(&typed_obj.stmt)
            .await?
        {
            Statement::CreateConnection(s) => s,
            _ => unreachable!(),
        };

        let live_sql = client
            .introspection()
            .get_connection_create_sql(&obj_id.database, &obj_id.schema, &obj_id.object)
            .await
            .map_err(CliError::Connection)?;

        let action = match live_sql {
            None => {
                // Object was in catalog batch check but SHOW CREATE returned nothing —
                // treat as needing creation.
                executor.execute_sql(&resolved_stmt).await?;
                ObjectAction::Created
            }
            Some(sql) => {
                let live_create = parse_create_connection_sql(&sql)?;
                let (to_set, to_drop) =
                    diff_connection_options(&resolved_stmt.values, &live_create.values);

                if to_set.is_empty() && to_drop.is_empty() {
                    ObjectAction::UpToDate
                } else {
                    let actions: Vec<AlterConnectionAction<Raw>> = to_set
                        .into_iter()
                        .map(AlterConnectionAction::SetOption)
                        .chain(to_drop.into_iter().map(AlterConnectionAction::DropOption))
                        .collect();

                    let alter_stmt = AlterConnectionStatement::<Raw> {
                        name: create_stmt.name.clone(),
                        if_exists: false,
                        actions,
                        with_options: vec![],
                    };
                    executor.execute_sql(&alter_stmt).await?;
                    ObjectAction::Altered
                }
            }
        };

        apply_objects::reconcile_grants_and_comments(
            client,
            executor,
            obj_id,
            typed_obj,
            &GRANT_KIND,
        )
        .await?;

        Ok(HandleResult {
            action,
            redacted_statements: vec![],
        })
    }

    async fn handle_new(
        &self,
        client: &Client,
        executor: &DeploymentExecutor<'_>,
        obj_id: &ObjectId,
        typed_obj: &typed::DatabaseObject,
    ) -> Result<HandleResult, CliError> {
        let resolved_stmt = match self
            .resolver
            .resolve_statement_for_cli(&typed_obj.stmt)
            .await?
        {
            Statement::CreateConnection(s) => s,
            _ => unreachable!(),
        };

        executor.execute_sql(&resolved_stmt).await?;

        apply_objects::reconcile_grants_and_comments(
            client,
            executor,
            obj_id,
            typed_obj,
            &GRANT_KIND,
        )
        .await?;

        Ok(HandleResult {
            action: ObjectAction::Created,
            redacted_statements: vec![],
        })
    }
}

/// Plan connection changes without executing or printing.
pub async fn plan(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    planned_project: &project::planned::Project,
    apply_plan: &mut ApplyPlan,
) -> Result<ApplyResult, CliError> {
    let connections = Connections::new(settings)?;
    let input = apply_objects::prepare_phase(
        &GRANT_KIND,
        matches,
        client,
        executor,
        planned_project,
        apply_plan,
    )
    .await?;

    let mut results = Vec::new();

    for (obj_id, typed_obj) in &input.existing_objects {
        executor.take_statements();
        let hr = connections
            .handle_existing(client, executor, obj_id, typed_obj)
            .await?;
        results.push(apply_objects::to_object_result(obj_id, executor, hr));
    }

    for (obj_id, typed_obj) in &input.to_create {
        executor.take_statements();
        let hr = connections
            .handle_new(client, executor, obj_id, typed_obj)
            .await?;
        results.push(apply_objects::to_object_result(obj_id, executor, hr));
    }

    Ok(ApplyResult {
        phase: PHASE_NAME.to_string(),
        results,
    })
}

/// Run the `apply connections` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    apply_objects::run_compiled_phase(settings, dry_run, |s, c, e, pp, ap| {
        Box::pin(self::plan(s, c, e, pp, ap))
    })
    .await
}

/// Parse a `CREATE CONNECTION` SQL string back into its AST statement.
fn parse_create_connection_sql(
    sql: &str,
) -> Result<mz_sql_parser::ast::CreateConnectionStatement<Raw>, CliError> {
    let stmts = parse_statements(sql).map_err(|e| {
        CliError::Message(format!(
            "failed to parse SHOW CREATE CONNECTION output: {}",
            e.error
        ))
    })?;

    let stmt = stmts
        .into_iter()
        .next()
        .ok_or_else(|| CliError::Message("SHOW CREATE CONNECTION returned empty SQL".into()))?;

    match stmt.ast {
        ParserStatement::CreateConnection(c) => Ok(c),
        other => Err(CliError::Message(format!(
            "expected CREATE CONNECTION, got: {}",
            other
        ))),
    }
}

/// Diff two sets of connection options.
///
/// Returns `(to_set, to_drop)`:
/// - `to_set`: options that need `ALTER CONNECTION ... SET`
/// - `to_drop`: option names that need `ALTER CONNECTION ... DROP`
fn diff_connection_options(
    project_opts: &[ConnectionOption<Raw>],
    live_opts: &[ConnectionOption<Raw>],
) -> (Vec<ConnectionOption<Raw>>, Vec<ConnectionOptionName>) {
    let project_map: BTreeMap<ConnectionOptionName, &ConnectionOption<Raw>> =
        project_opts.iter().map(|o| (o.name, o)).collect();
    let live_map: BTreeMap<ConnectionOptionName, &ConnectionOption<Raw>> =
        live_opts.iter().map(|o| (o.name, o)).collect();

    let mut to_set = Vec::new();
    let mut to_drop = Vec::new();

    // Options in project but not in live, or with different values → SET
    for (name, project_opt) in &project_map {
        match live_map.get(name) {
            None => to_set.push((*project_opt).clone()),
            Some(live_opt) => {
                if *project_opt != *live_opt {
                    to_set.push((*project_opt).clone());
                }
            }
        }
    }

    // Options in live but not in project → DROP
    for name in live_map.keys() {
        if !project_map.contains_key(name) {
            to_drop.push(*name);
        }
    }

    (to_set, to_drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_sql_parser::ast::WithOptionValue;

    fn make_option(name: ConnectionOptionName, value: &str) -> ConnectionOption<Raw> {
        ConnectionOption {
            name,
            value: Some(WithOptionValue::Value(mz_sql_parser::ast::Value::String(
                value.to_string(),
            ))),
        }
    }

    #[test]
    fn test_diff_no_changes() {
        let opts = vec![
            make_option(ConnectionOptionName::Host, "localhost"),
            make_option(ConnectionOptionName::Port, "5432"),
        ];
        let (to_set, to_drop) = diff_connection_options(&opts, &opts);
        assert!(to_set.is_empty());
        assert!(to_drop.is_empty());
    }

    #[test]
    fn test_diff_option_changed() {
        let project = vec![make_option(
            ConnectionOptionName::Host,
            "new-host.example.com",
        )];
        let live = vec![make_option(
            ConnectionOptionName::Host,
            "old-host.example.com",
        )];
        let (to_set, to_drop) = diff_connection_options(&project, &live);
        assert_eq!(to_set.len(), 1);
        assert_eq!(to_set[0].name, ConnectionOptionName::Host);
        assert!(to_drop.is_empty());
    }

    #[test]
    fn test_diff_option_added() {
        let project = vec![
            make_option(ConnectionOptionName::Host, "localhost"),
            make_option(ConnectionOptionName::Database, "mydb"),
        ];
        let live = vec![make_option(ConnectionOptionName::Host, "localhost")];
        let (to_set, to_drop) = diff_connection_options(&project, &live);
        assert_eq!(to_set.len(), 1);
        assert_eq!(to_set[0].name, ConnectionOptionName::Database);
        assert!(to_drop.is_empty());
    }

    #[test]
    fn test_diff_option_dropped() {
        let project = vec![make_option(ConnectionOptionName::Host, "localhost")];
        let live = vec![
            make_option(ConnectionOptionName::Host, "localhost"),
            make_option(ConnectionOptionName::Database, "mydb"),
        ];
        let (to_set, to_drop) = diff_connection_options(&project, &live);
        assert!(to_set.is_empty());
        assert_eq!(to_drop.len(), 1);
        assert_eq!(to_drop[0], ConnectionOptionName::Database);
    }

    #[test]
    fn test_diff_multiple_changes() {
        let project = vec![
            make_option(ConnectionOptionName::Host, "new-host"),
            make_option(ConnectionOptionName::Port, "5433"),
        ];
        let live = vec![
            make_option(ConnectionOptionName::Host, "old-host"),
            make_option(ConnectionOptionName::Database, "mydb"),
        ];
        let (to_set, to_drop) = diff_connection_options(&project, &live);
        assert_eq!(to_set.len(), 2);
        assert!(to_set.iter().any(|o| o.name == ConnectionOptionName::Host));
        assert!(to_set.iter().any(|o| o.name == ConnectionOptionName::Port));
        assert_eq!(to_drop.len(), 1);
        assert_eq!(to_drop[0], ConnectionOptionName::Database);
    }

    #[test]
    fn test_diff_secret_option_compared_structurally() {
        use mz_sql_parser::ast::{Ident, RawItemName, UnresolvedItemName};

        let secret_ref =
            WithOptionValue::Secret(RawItemName::Name(UnresolvedItemName::qualified(&[
                Ident::new_unchecked("db"),
                Ident::new_unchecked("public"),
                Ident::new_unchecked("my_pass"),
            ])));
        let project = vec![ConnectionOption {
            name: ConnectionOptionName::SaslPassword,
            value: Some(secret_ref.clone()),
        }];
        let live = vec![ConnectionOption {
            name: ConnectionOptionName::SaslPassword,
            value: Some(secret_ref),
        }];
        let (to_set, to_drop) = diff_connection_options(&project, &live);
        assert!(to_set.is_empty());
        assert!(to_drop.is_empty());
    }
}
