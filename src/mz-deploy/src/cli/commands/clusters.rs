// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Clusters apply command - converge live cluster state to match definitions.

use std::collections::BTreeMap;

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult, connect_apply_client,
};
use crate::client::{Client, quote_identifier};
use crate::config::Settings;
use crate::project::clusters::{self, ClusterDefinition};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit_mut::VisitMut;
use mz_sql_parser::ast::{
    ClusterOption, ClusterOptionName, CreateClusterStatement, Raw, Statement, Value,
    WithOptionValue,
};
use mz_sql_parser::parser::parse_statements;

/// Plan cluster changes without executing or printing.
pub async fn plan(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
) -> Result<ApplyResult, CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;

    let definitions = clusters::load_clusters(
        directory,
        &profile.name,
        settings.profile_suffix(),
        settings.variables(),
    )?;

    if definitions.is_empty() {
        return Ok(ApplyResult {
            phase: "clusters".to_string(),
            results: vec![],
        });
    }

    let mut object_results = Vec::new();
    for def in &definitions {
        let obj_result = plan_cluster(client, executor, def).await?;
        object_results.push(obj_result);
    }

    Ok(ApplyResult {
        phase: "clusters".to_string(),
        results: object_results,
    })
}

/// Run the `clusters apply` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    let client = connect_apply_client(settings).await?;
    let executor = DeploymentExecutor::new_dry_run(&client);
    let mut plan_result = ApplyPlan::new();
    let phase = plan(settings, &client, &executor).await?;
    plan_result.add_phase(phase);

    if !dry_run {
        plan_result.execute(&client).await?;
    }

    Ok(plan_result)
}

/// Plan a single cluster definition: create if missing, alter if drifted,
/// then plan grants, revocations, and comments.
async fn plan_cluster(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &ClusterDefinition,
) -> Result<ObjectResult, CliError> {
    let cluster_name = &def.name;

    // Drain any prior statements
    executor.take_statements();

    let live = live_cluster(client, cluster_name).await?;

    let action = match live {
        None => {
            executor.execute_sql(&def.create_stmt).await?;
            ObjectAction::Created
        }
        Some(live) => {
            let defaults = default_options(
                client
                    .default_cluster_replication_factor()
                    .await
                    .map_err(CliError::Connection)?,
            );
            let (to_set, to_reset) = diff_cluster_options(&def.create_stmt, &live, &defaults);

            if to_set.is_empty() && to_reset.is_empty() {
                ObjectAction::UpToDate
            } else {
                // RESET must run before SET. When an edit both raises SIZE and
                // drops AUTO SCALING STRATEGY, running the SET first would
                // validate the new size against the still-live policy, and the
                // server rejects a hydration size equal to the cluster size with
                // `HYDRATION SIZE must differ from the cluster SIZE`. Clearing
                // the policy first lets the size change land.
                //
                // SET and RESET cannot be combined in one statement.
                if !to_reset.is_empty() {
                    let reset_sql = format!(
                        "ALTER CLUSTER {} RESET ({})",
                        quote_identifier(cluster_name),
                        render_option_list(&to_reset)
                    );
                    executor.execute_sql(&reset_sql).await?;
                }
                if !to_set.is_empty() {
                    let alter_sql = format!(
                        "ALTER CLUSTER {} SET ({})",
                        quote_identifier(cluster_name),
                        render_option_list(&to_set)
                    );
                    executor.execute_sql(&alter_sql).await?;
                }
                ObjectAction::Altered
            }
        }
    };

    // Reconcile grants
    grants::reconcile_named_object(
        client,
        executor,
        cluster_name,
        &def.grants,
        &grants::GrantNamedObjectKind::Cluster,
    )
    .await?;

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    Ok(ObjectResult {
        object: cluster_name.clone(),
        action,
        statements: executor.take_statements(),
        redacted_statements: vec![],
        transaction_group: None,
        post_statements: vec![],
    })
}

/// The live cluster's configuration, as the canonical `CREATE CLUSTER` statement
/// the server renders from the catalog. `None` when the cluster does not exist.
///
/// Errors on an unmanaged cluster, which has no `SHOW CREATE CLUSTER` form.
async fn live_cluster(
    client: &Client,
    name: &str,
) -> Result<Option<CreateClusterStatement<Raw>>, CliError> {
    let Some(cluster) = client
        .introspection()
        .get_cluster(name)
        .await
        .map_err(CliError::Connection)?
    else {
        return Ok(None);
    };
    if !cluster.managed {
        return Err(CliError::Message(format!(
            "cluster '{}' is unmanaged; mz-deploy reconciles managed clusters only",
            name
        )));
    }
    client
        .introspection()
        .get_cluster_create_sql(name)
        .await
        .map_err(CliError::Connection)?
        .map(|sql| parse_create_cluster(&sql))
        .transpose()
}

/// Parse the `create_sql` column of `SHOW CREATE CLUSTER`.
fn parse_create_cluster(sql: &str) -> Result<CreateClusterStatement<Raw>, CliError> {
    let statements = parse_statements(sql).map_err(|e| {
        CliError::Message(format!(
            "failed to parse SHOW CREATE CLUSTER output: {}",
            e.error
        ))
    })?;
    match statements.into_iter().next().map(|statement| statement.ast) {
        Some(Statement::CreateCluster(create)) => Ok(create),
        Some(other) => Err(CliError::Message(format!(
            "expected CREATE CLUSTER, got: {}",
            other
        ))),
        None => Err(CliError::Message(
            "SHOW CREATE CLUSTER returned empty SQL".to_string(),
        )),
    }
}

/// The options `SHOW CREATE CLUSTER` renders for every managed cluster, paired
/// with the value the server assigns when the definition omits them.
///
/// A live option equal to its default is indistinguishable from an unset one, so
/// a definition that omits it has not drifted. Options the server omits from
/// `SHOW CREATE CLUSTER` when unset need no entry here.
///
/// The interval mirrors `mz_controller_types::DEFAULT_REPLICA_LOGGING_INTERVAL`.
fn default_options(
    replication_factor: u32,
) -> BTreeMap<ClusterOptionName, Option<WithOptionValue<Raw>>> {
    let sql = format!(
        "CREATE CLUSTER defaults (\
         EXPERIMENTAL ARRANGEMENT COMPRESSION = false, \
         INTROSPECTION DEBUGGING = false, \
         INTROSPECTION INTERVAL = INTERVAL '00:00:01', \
         MANAGED = true, \
         REPLICATION FACTOR = {replication_factor}, \
         SCHEDULE = MANUAL)"
    );
    let create = parse_create_cluster(&sql).expect("cluster defaults are valid SQL");
    create
        .options
        .iter()
        .map(|option| (option.name.clone(), comparable(option)))
        .collect()
}

/// An option's value reduced to a form the two sides of the diff can be compared
/// by.
///
/// The server re-renders durations in canonical interval form, so a definition's
/// `LINGER DURATION = '60s'` comes back from `SHOW CREATE CLUSTER` as
/// `'00:01:00'`. Every value that parses as an interval is reduced to that
/// interval, whatever syntax it was written in.
///
/// Comparison only. An option that needs setting is emitted as the definition
/// wrote it.
fn comparable(option: &ClusterOption<Raw>) -> Option<WithOptionValue<Raw>> {
    let mut value = option.value.clone();
    if let Some(value) = &mut value {
        CanonicalIntervals.visit_with_option_value_mut(value);
    }
    value
}

/// Rewrites every interval-valued literal in an option to its canonical form,
/// reaching nested values such as `LINGER DURATION` inside `AUTO SCALING
/// STRATEGY`.
struct CanonicalIntervals;

impl<'ast> VisitMut<'ast, Raw> for CanonicalIntervals {
    fn visit_value_mut(&mut self, node: &'ast mut Value) {
        let text = match node {
            Value::String(text) => text.as_str(),
            Value::Interval(interval) => interval.value.as_str(),
            _ => return,
        };
        if let Ok(interval) = mz_repr::strconv::parse_interval(text) {
            *node = Value::String(interval.to_string());
        }
    }
}

/// Compare a cluster definition against the live cluster and return the options
/// to `SET` and the option names to `RESET` so the caller can converge live state
/// onto the definition.
///
/// An option the definition declares is `SET` when its value differs from the
/// live one. An option the definition omits is `RESET` unless the live value is
/// already the server default, which `defaults` supplies. The comparison is a
/// difference over option names, so a cluster option Materialize adds later
/// reconciles without a change here.
fn diff_cluster_options(
    local: &CreateClusterStatement<Raw>,
    live: &CreateClusterStatement<Raw>,
    defaults: &BTreeMap<ClusterOptionName, Option<WithOptionValue<Raw>>>,
) -> (Vec<ClusterOption<Raw>>, Vec<ClusterOptionName>) {
    let local_options = index_options(local);
    let live_options = index_options(live);

    let to_set = local_options
        .values()
        .filter(|option| {
            live_options.get(&option.name).map(|live| comparable(live)) != Some(comparable(option))
        })
        .map(|option| (*option).clone())
        .collect();

    let to_reset = live_options
        .values()
        .filter(|option| !local_options.contains_key(&option.name))
        .filter(|option| defaults.get(&option.name) != Some(&comparable(option)))
        .map(|option| option.name.clone())
        .collect();

    (to_set, to_reset)
}

/// Index a statement's options by name, dropping the ones that spell "unset".
///
/// A duplicate name is rejected by the server, so the last one wins here.
fn index_options(
    create: &CreateClusterStatement<Raw>,
) -> BTreeMap<ClusterOptionName, &ClusterOption<Raw>> {
    create
        .options
        .iter()
        .filter(|option| !is_unset(option))
        .map(|option| (option.name.clone(), option))
        .collect()
}

/// Whether an option is set to an empty block, as in `AUTO SCALING STRATEGY = ()`.
///
/// That is how a definition spells "unset". The server normalizes it away and
/// omits the option from `SHOW CREATE CLUSTER`.
fn is_unset(option: &ClusterOption<Raw>) -> bool {
    option.to_ast_string_simple().ends_with("= ()")
}

/// Render cluster options (or option names) as a comma-separated `ALTER CLUSTER`
/// argument list.
fn render_option_list<T: AstDisplay>(items: &[T]) -> String {
    items
        .iter()
        .map(AstDisplay::to_ast_string_simple)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical statement `SHOW CREATE CLUSTER` renders: every default
    /// spelled out, whatever the definition said.
    fn live(size: &str, replication_factor: u32, extra: &str) -> CreateClusterStatement<Raw> {
        let sql = format!(
            "CREATE CLUSTER \"scaled\" (\
             EXPERIMENTAL ARRANGEMENT COMPRESSION = false, \
             INTROSPECTION DEBUGGING = false, \
             INTROSPECTION INTERVAL = INTERVAL '00:00:01', \
             MANAGED = true, \
             REPLICATION FACTOR = {replication_factor}, \
             SIZE = '{size}', \
             SCHEDULE = MANUAL{extra})"
        );
        parse_create_cluster(&sql).unwrap()
    }

    /// Render a diff as `(SET statement parts, RESET names)` for concise asserts.
    fn diff(local: &str, live: &CreateClusterStatement<Raw>) -> (Vec<String>, Vec<String>) {
        let local = parse_create_cluster(local).unwrap();
        let (to_set, to_reset) = diff_cluster_options(&local, live, &default_options(1));
        (
            to_set
                .iter()
                .map(AstDisplay::to_ast_string_simple)
                .collect(),
            to_reset
                .iter()
                .map(AstDisplay::to_ast_string_simple)
                .collect(),
        )
    }

    #[mz_ore::test]
    fn test_diff_defaults_are_not_drift() {
        // The definition names only SIZE, so every other option the server
        // renders holds its default, REPLICATION FACTOR = 1 among them.
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '25cc')",
                &live("25cc", 1, "")
            ),
            (vec![], Vec::<String>::new())
        );
    }

    #[mz_ore::test]
    fn test_diff_up_to_date() {
        let strategy = ", AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '100cc'))";
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '25cc', REPLICATION FACTOR = 2, \
                 AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '100cc')))",
                &live("25cc", 2, strategy)
            ),
            (vec![], Vec::<String>::new())
        );
    }

    #[mz_ore::test]
    fn test_diff_size_only() {
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '50cc', REPLICATION FACTOR = 2)",
                &live("25cc", 2, "")
            ),
            (vec!["SIZE = '50cc'".to_string()], Vec::<String>::new())
        );
    }

    #[mz_ore::test]
    fn test_diff_replication_factor_reset_when_omitted() {
        // The live value is not the default, so omitting it resets.
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '25cc')",
                &live("25cc", 3, "")
            ),
            (vec![], vec!["REPLICATION FACTOR".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_diff_strategy_set() {
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '25cc', REPLICATION FACTOR = 2, \
                 AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '100cc')))",
                &live("25cc", 2, "")
            ),
            (
                vec![
                    "AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '100cc'))".to_string()
                ],
                Vec::<String>::new()
            )
        );
    }

    #[mz_ore::test]
    fn test_diff_strategy_reset() {
        let strategy = ", AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '100cc'))";
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '25cc', REPLICATION FACTOR = 2)",
                &live("25cc", 2, strategy)
            ),
            (vec![], vec!["AUTO SCALING STRATEGY".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_diff_option_the_reconciler_never_names() {
        // Nothing in the diff mentions either option. They reconcile because the
        // comparison is over option names.
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '25cc', REPLICATION FACTOR = 2, \
                 AVAILABILITY ZONES = ('use1-az1'))",
                &live("25cc", 2, ", WORKLOAD CLASS = 'batch'")
            ),
            (
                vec!["AVAILABILITY ZONES = ('use1-az1')".to_string()],
                vec!["WORKLOAD CLASS".to_string()]
            )
        );
    }

    #[mz_ore::test]
    fn test_diff_duration_spelling_is_not_drift() {
        // The definition writes '60s'; the server renders '00:01:00'.
        let strategy = ", AUTO SCALING STRATEGY = (ON HYDRATION \
                        (HYDRATION SIZE = '100cc', LINGER DURATION = '00:01:00'))";
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '25cc', AUTO SCALING STRATEGY = \
                 (ON HYDRATION (HYDRATION SIZE = '100cc', LINGER DURATION = '60s')))",
                &live("25cc", 1, strategy)
            ),
            (vec![], Vec::<String>::new())
        );
    }

    #[mz_ore::test]
    fn test_diff_sizes_are_not_read_as_durations() {
        // Canonicalizing durations must not touch a value that is not one.
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '50cc')",
                &live("25cc", 1, "")
            ),
            (vec!["SIZE = '50cc'".to_string()], Vec::<String>::new())
        );
    }

    #[mz_ore::test]
    fn test_diff_empty_block_is_not_drift() {
        // An empty block writes "no policy", which the server omits from SHOW
        // CREATE entirely.
        assert_eq!(
            diff(
                "CREATE CLUSTER scaled (SIZE = '25cc', AUTO SCALING STRATEGY = ())",
                &live("25cc", 1, "")
            ),
            (vec![], Vec::<String>::new())
        );
    }

    #[mz_ore::test]
    fn test_default_replication_factor_is_read_from_the_server() {
        // Where the server default is 2, a live factor of 2 is the unset value.
        let local = parse_create_cluster("CREATE CLUSTER scaled (SIZE = '25cc')").unwrap();
        let (to_set, to_reset) =
            diff_cluster_options(&local, &live("25cc", 2, ""), &default_options(2));
        assert!(to_set.is_empty() && to_reset.is_empty());
    }

    #[mz_ore::test]
    fn test_default_introspection_interval_matches_the_server() {
        // `default_options` spells the interval out as SQL. Guard the constant
        // it mirrors.
        assert_eq!(
            mz_controller_types::DEFAULT_REPLICA_LOGGING_INTERVAL,
            std::time::Duration::from_secs(1)
        );
    }
}
