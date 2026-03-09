//! Apply connections command - create missing connections and reconcile drifted ones.

use crate::cli::commands::grants;
use crate::cli::progress;
use crate::cli::{CliError, executor};
use crate::client::Client;
use crate::config::Settings;
use crate::project;
use crate::project::ast::Statement;
use crate::secret_resolver::SecretResolver;
use mz_sql_parser::ast::{
    AlterConnectionAction, AlterConnectionStatement, ConnectionOption, ConnectionOptionName, Raw,
    Statement as ParserStatement,
};
use mz_sql_parser::parser::parse_statements;
use std::collections::BTreeMap;
use std::time::Instant;

/// Run the `apply connections` command.
///
/// Compiles the project, collects all connection objects, and for each connection:
/// 1. Fetches `SHOW CREATE CONNECTION` for the existing connection (if any)
/// 2. If missing, executes `CREATE CONNECTION IF NOT EXISTS`
/// 3. If exists, diffs options and emits `ALTER CONNECTION ... SET/DROP`
/// 4. Applies any associated grants and comments
pub async fn run(settings: &Settings, dry_run: bool) -> Result<(), CliError> {
    let profile = settings.connection();
    let start_time = Instant::now();

    let planned_project = super::compile::run(settings, true).await?;

    let connections: Vec<_> = planned_project
        .iter_objects()
        .filter(|obj| matches!(obj.typed_object.stmt, Statement::CreateConnection(_)))
        .collect();

    if connections.is_empty() {
        progress::info("No connections found in project — nothing to do.");
        return Ok(());
    }

    progress::info(&format!(
        "Found {} connection(s) in project",
        connections.len()
    ));

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let executor = executor::DeploymentExecutor::with_dry_run(&client, dry_run);

    // Prepare schemas
    let connection_schemas = project::SchemaQualifier::collect_from(&connections);
    executor
        .prepare_databases_and_schemas(&planned_project, &connection_schemas, None)
        .await?;

    let resolver = SecretResolver::new(&settings.profile_config.security);

    let mut created = 0u32;
    let mut altered = 0u32;
    let mut up_to_date = 0u32;

    for obj in &connections {
        let typed_obj = &obj.typed_object;
        let Statement::CreateConnection(ref create_stmt) = typed_obj.stmt else {
            unreachable!("filtered for CreateConnection above");
        };

        let resolved_stmt = match resolver.resolve_statement_for_cli(&typed_obj.stmt).await? {
            Statement::CreateConnection(s) => s,
            _ => unreachable!(),
        };

        let database = &obj.id.database;
        let schema = &obj.id.schema;
        let name = &obj.id.object;

        // Fetch existing connection's CREATE SQL
        let live_sql = client
            .introspection()
            .get_connection_create_sql(database, schema, name)
            .await
            .map_err(CliError::Connection)?;

        match live_sql {
            None => {
                // Connection doesn't exist — create it
                let mut create_if_not_exists = resolved_stmt.clone();
                create_if_not_exists.if_not_exists = true;
                executor.execute_sql(&create_if_not_exists).await?;
                created += 1;
                progress::success(&format!("{} (created)", obj.id));
            }
            Some(sql) => {
                // Parse the live CREATE CONNECTION SQL
                let live_create = parse_create_connection_sql(&sql)?;
                let (to_set, to_drop) =
                    diff_connection_options(&resolved_stmt.values, &live_create.values);

                if to_set.is_empty() && to_drop.is_empty() {
                    up_to_date += 1;
                    progress::info(&format!("{} (up to date)", obj.id));
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
                    altered += 1;
                    progress::success(&format!("{} (altered)", obj.id));
                }
            }
        }

        if !dry_run {
            grants::reconcile(
                &client,
                &executor,
                &obj.id,
                &typed_obj.grants,
                &grants::GrantObjectKind::Connection,
            )
            .await?;
        }

        // Apply comments
        for comment in &typed_obj.comments {
            executor.execute_sql(comment).await?;
        }
    }

    let duration = start_time.elapsed();
    progress::success(&format!(
        "Applied {} connection(s) in {:.1}s ({} created, {} altered, {} up to date)",
        connections.len(),
        duration.as_secs_f64(),
        created,
        altered,
        up_to_date,
    ));

    Ok(())
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
