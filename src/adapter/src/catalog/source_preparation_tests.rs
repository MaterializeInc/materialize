// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use itertools::Itertools;
use mz_ore::now::NowFn;
use mz_sql::plan::PlanError;
use mz_sql::pure::{
    PurifiedStatement, SourcePreparation, SourcePreparationResponse, SourcePreparationStatus,
};
use mz_sql::session::vars::VarInput;
use mz_storage_types::configuration::StorageConfiguration;

use crate::catalog::{Catalog, CatalogState};

fn source_statement(
    state: &CatalogState,
    sql: &str,
) -> Result<mz_sql::ast::Statement<mz_sql::names::Aug>, PlanError> {
    let statement = mz_sql_parser::parser::parse_statements(sql)
        .expect("parse source statement")
        .remove(0)
        .ast;
    let (statement, _) = mz_sql::names::resolve(&state.for_system_session(), statement)?;
    Ok(statement)
}

const CLOCK_SOURCE: &str = "CREATE SOURCE clock_src FROM LOAD GENERATOR CLOCK";

fn resolve(
    preparation: &SourcePreparation,
    state: &CatalogState,
) -> Result<SourcePreparationStatus, PlanError> {
    preparation.resolve(
        &state.for_system_session(),
        source_statement(state, CLOCK_SOURCE)?,
    )
}

fn finish(
    preparation: &mut SourcePreparation,
    state: &CatalogState,
    response: SourcePreparationResponse,
) -> Result<SourcePreparationStatus, PlanError> {
    preparation.finish(
        &state.for_system_session(),
        source_statement(state, CLOCK_SOURCE)?,
        response,
    )
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn source_preparation_checks_current_inputs() {
    Catalog::with_debug(|catalog| async move {
        let mut state = catalog.state().clone();
        for (name, value) in [
            ("enable_load_generator_clock", "true"),
            ("enable_create_table_from_source", "true"),
            ("force_source_table_syntax", "false"),
        ] {
            state
                .system_config_mut()
                .set(name, VarInput::Flat(value))
                .expect("set catalog parameter");
        }
        let clock = Arc::new(AtomicU64::new(1000));
        state.config.now = NowFn::from({
            let clock = Arc::clone(&clock);
            move || clock.load(Ordering::SeqCst)
        });
        let storage = StorageConfiguration::new(
            state.config.connection_context.clone(),
            mz_storage_types::dyncfgs::all_dyncfgs(Default::default()),
        );
        let mut preparation = SourcePreparation::new(1000);
        let SourcePreparationStatus::Pending(request) =
            resolve(&preparation, &state).expect("resolve source preparation")
        else {
            panic!("metadata preparation must be requested");
        };
        let original_input = request.execute(&storage).await;
        let late_input = request.execute(&storage).await;
        let SourcePreparationStatus::Ready(original) =
            finish(&mut preparation, &state, original_input).expect("accept source preparation")
        else {
            panic!("matching metadata must be reused");
        };

        clock.store(2000, Ordering::SeqCst);
        state
            .system_config_mut()
            .set("max_tables", VarInput::Flat("999"))
            .expect("set catalog parameter");
        let SourcePreparationStatus::Ready(reused) =
            resolve(&preparation, &state).expect("resolve source preparation")
        else {
            panic!("unrelated catalog changes must not require metadata preparation");
        };
        assert_eq!(
            reused, original,
            "implicit CLOCK time must remain statement-local"
        );

        state
            .system_config_mut()
            .set("enable_load_generator_clock", VarInput::Flat("false"))
            .expect("set catalog parameter");
        assert!(
            resolve(&preparation, &state).is_err(),
            "cached metadata must not bypass current feature checks"
        );
        state
            .system_config_mut()
            .set("enable_load_generator_clock", VarInput::Flat("true"))
            .expect("set catalog parameter");

        state
            .system_config_mut()
            .set("force_source_table_syntax", VarInput::Flat("true"))
            .expect("set catalog parameter");
        let SourcePreparationStatus::Pending(changed) =
            resolve(&preparation, &state).expect("resolve source preparation")
        else {
            panic!("changed preparation inputs must not reuse old metadata");
        };
        assert_ne!(changed, request);
        let changed_input = changed.execute(&storage).await;
        let SourcePreparationStatus::Ready(current) =
            finish(&mut preparation, &state, changed_input).expect("accept current metadata")
        else {
            panic!("matching current metadata must be reused");
        };

        let SourcePreparationStatus::Ready(after_obsolete) =
            finish(&mut preparation, &state, late_input).expect("ignore obsolete metadata")
        else {
            panic!("obsolete completion must not evict valid current metadata");
        };
        assert_eq!(after_obsolete, current);
        let PurifiedStatement::PurifiedCreateSource {
            create_source_stmt: original,
            ..
        } = *original
        else {
            panic!("expected the original source definition");
        };
        let PurifiedStatement::PurifiedCreateSource {
            create_source_stmt: current,
            ..
        } = *current
        else {
            panic!("expected the current source definition");
        };
        // Fresh metadata has its own discovery timestamp, but CLOCK AS OF is statement-local.
        assert_eq!(current, original);
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn source_preparation_checks_failure_inputs() {
    Catalog::with_debug(|catalog| async move {
        let mut state = catalog.state().clone();
        for name in [
            "enable_create_table_from_source",
            "force_source_table_syntax",
        ] {
            state
                .system_config_mut()
                .set(name, VarInput::Flat("false"))
                .expect("require explicit multi-output exports");
        }
        let storage = StorageConfiguration::new(
            state.config.connection_context.clone(),
            mz_storage_types::dyncfgs::all_dyncfgs(Default::default()),
        );
        let sql = "CREATE SOURCE auction_src FROM LOAD GENERATOR AUCTION";
        let mut preparation = SourcePreparation::new(1000);
        let SourcePreparationStatus::Pending(request) = preparation
            .resolve(
                &state.for_system_session(),
                source_statement(&state, sql).expect("resolve auction"),
            )
            .expect("request auction metadata")
        else {
            panic!("auction metadata must be requested");
        };
        let response = request.execute(&storage).await;
        assert!(
            preparation
                .finish(
                    &state.for_system_session(),
                    source_statement(&state, sql).expect("resolve auction"),
                    response,
                )
                .is_err(),
            "the current requirement for explicit exports must fail"
        );

        let obsolete_failure = request.execute(&storage).await;
        state
            .system_config_mut()
            .set("enable_create_table_from_source", VarInput::Flat("true"))
            .expect("allow exports to be created separately");
        let SourcePreparationStatus::Pending(current) = preparation
            .finish(
                &state.for_system_session(),
                source_statement(&state, sql).expect("resolve auction"),
                obsolete_failure,
            )
            .expect("an obsolete failure must not abort current preparation")
        else {
            panic!("metadata must be requested with the current export policy");
        };
        assert_ne!(current, request);
        let response = current.execute(&storage).await;
        assert!(matches!(
            preparation
                .finish(
                    &state.for_system_session(),
                    source_statement(&state, sql).expect("resolve auction"),
                    response,
                )
                .expect("accept current auction metadata"),
            SourcePreparationStatus::Ready(_)
        ));
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn source_planning_retains_ids_when_exports_change() {
    use crate::catalog::command::source::{SourcePlanning, plan_create_source};
    use mz_sql::plan::{Params, PlanContext};
    use mz_sql::pure::PurifiedStatement;
    use std::collections::BTreeMap;

    Catalog::with_debug(|catalog| async move {
        let mut state = catalog.state().clone();
        state
            .system_config_mut()
            .set("force_source_table_syntax", VarInput::Flat("false"))
            .expect("allow generated exports");
        let storage = StorageConfiguration::new(
            state.config.connection_context.clone(),
            mz_storage_types::dyncfgs::all_dyncfgs(Default::default()),
        );
        let sql = "CREATE SOURCE materialize.public.auction_src FROM LOAD GENERATOR AUCTION FOR ALL TABLES";
        let mut preparation = SourcePreparation::new(1000);
        let mut status = preparation
            .resolve(
                &state.for_system_session(),
                source_statement(&state, sql).expect("resolve auction"),
            )
            .expect("resolve preparation");
        let mut purified = loop {
            match status {
                SourcePreparationStatus::Ready(statement) => break *statement,
                SourcePreparationStatus::Pending(request) => {
                    let response = request.execute(&storage).await;
                    status = preparation
                        .finish(
                            &state.for_system_session(),
                            source_statement(&state, sql).expect("resolve auction"),
                            response,
                        )
                        .expect("prepare auction exports");
                }
            }
        };
        let mut ids = BTreeMap::new();
        let pcx = PlanContext::zero();
        let params = Params::empty();
        let original = loop {
            match plan_create_source(
                &state.for_system_session(),
                &pcx,
                &params,
                purified.clone(),
                &ids,
            )
            .expect("plan auction")
            {
                SourcePlanning::Ready(plans) => break plans,
                SourcePlanning::NeedsIds(keys) => {
                    let count = u64::try_from(keys.len()).expect("ID count fits");
                    let ts = catalog.current_upper().await;
                    let allocated = catalog
                        .allocate_user_ids(count, ts)
                        .await
                        .expect("allocate IDs");
                    ids.extend(keys.into_iter().zip_eq(allocated));
                }
            }
        };
        let PurifiedStatement::PurifiedCreateSource { subsources, .. } = &mut purified else {
            panic!("expected source metadata");
        };
        let removed = subsources.pop_first();
        assert!(removed.is_some(), "auction has generated exports");
        let SourcePlanning::Ready(replanned) =
            plan_create_source(&state.for_system_session(), &pcx, &params, purified, &ids)
                .expect("replan remaining exports")
        else {
            panic!("remaining objects already have allocated IDs");
        };
        assert_eq!(replanned.len() + 1, original.len());
        for plan in replanned {
            let previous = original
                .iter()
                .find(|previous| previous.plan.name == plan.plan.name)
                .expect("remaining name was present");
            assert_eq!(
                (plan.item_id, plan.global_id),
                (previous.item_id, previous.global_id)
            );
        }
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn source_command_preserves_only_observed_outcomes() {
    use std::collections::BTreeMap;

    use crate::AdapterError;
    use crate::catalog::command::CatalogCommand;
    use crate::catalog::command::source_statement::{
        SourceCommand, SourceStatement, SourceStatementPreparation,
    };
    use crate::catalog::{CommandExecutionError, CommandWork};
    use crate::session::Session;

    fn storage(catalog: &Catalog) -> StorageConfiguration {
        StorageConfiguration::new(
            catalog.state().config.connection_context.clone(),
            mz_storage_types::dyncfgs::all_dyncfgs(Default::default()),
        )
    }

    async fn stage(catalog: &Catalog, session: &Session, sql: &str) -> SourceCommand {
        let raw = mz_sql_parser::parser::parse_statements(sql)
            .expect("parse source")
            .remove(0)
            .ast;
        let mut statement = SourceStatement::new(
            Arc::new(raw),
            mz_sql::plan::Params::empty(),
            session.meta(),
            1000,
        );
        let (state, snapshot) = catalog.ddl_transaction_snapshot().await;
        let mut response = None;
        loop {
            let ts = catalog.current_upper().await;
            match catalog
                .prepare_source_statement(
                    &mut statement,
                    (&state, &snapshot),
                    None,
                    ts,
                    response.take(),
                )
                .await
                .expect("stage source")
            {
                SourceStatementPreparation::Ready(prepared) => return prepared.command,
                SourceStatementPreparation::Prepare(request) => {
                    response = Some(request.execute(&storage(catalog)).await);
                }
                SourceStatementPreparation::Allocate(keys) => {
                    let ids = catalog
                        .allocate_user_ids(u64::try_from(keys.len()).expect("ID count"), ts)
                        .await
                        .expect("allocate source IDs");
                    statement.record_ids(keys, ids);
                }
            }
        }
    }

    async fn apply(
        catalog: &mut Catalog,
        command: SourceCommand,
    ) -> Result<SourceCommand, AdapterError> {
        let mut commands = [CatalogCommand::Source(Arc::new(command))];
        let mut responses = BTreeMap::new();
        loop {
            let ts = catalog.current_upper().await;
            match catalog
                .transact_commands(None, ts, None, &mut commands, &mut responses)
                .await
            {
                Ok(_) => {
                    let [CatalogCommand::Source(command)] = commands else {
                        unreachable!()
                    };
                    return Ok(Arc::unwrap_or_clone(command));
                }
                Err(CommandExecutionError::Error(error)) => return Err(error),
                Err(CommandExecutionError::Pending { index, work }) => match work {
                    CommandWork::Connection(_) | CommandWork::ConnectionSecret(_) => {
                        unreachable!("source command")
                    }
                    CommandWork::Prepare(request) => {
                        responses.insert(
                            index,
                            crate::catalog::CommandResponse::Source(
                                request.execute(&storage(catalog)).await,
                            ),
                        );
                    }
                    CommandWork::Allocate(keys) => {
                        let ids = catalog
                            .allocate_user_ids(u64::try_from(keys.len()).expect("ID count"), ts)
                            .await
                            .expect("allocate source IDs");
                        let CatalogCommand::Source(command) = &mut commands[index] else {
                            unreachable!()
                        };
                        Arc::make_mut(command).record_ids(keys, ids);
                    }
                },
            }
        }
    }

    Catalog::with_debug(|mut catalog| async move {
        catalog.state.system_config_mut()
            .set("force_source_table_syntax", VarInput::Flat("false"))
            .expect("allow source syntax");
        catalog.state.system_config_mut()
            .set("enable_load_generator_counter", VarInput::Flat("true"))
            .expect("allow counter source fixture");
        let ts = catalog.current_upper().await;
        let cluster_id = catalog.allocate_user_cluster_id(ts).await.expect("allocate source cluster");
        let ts = catalog.current_upper().await;
        catalog.transact(None, ts, None, vec![crate::catalog::Op::CreateCluster {
            id: cluster_id,
            name: "source_cluster".into(),
            introspection_sources: Vec::new(),
            owner_id: mz_sql::session::user::MZ_SYSTEM_ROLE_ID,
            config: mz_catalog::memory::objects::ClusterConfig {
                variant: mz_catalog::memory::objects::ClusterVariant::Unmanaged,
                workload_class: None,
            },
        }]).await.expect("create source cluster");
        let mut session = Session::dummy();
        session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
        session
            .start_transaction(mz_ore::now::to_datetime(1000), None, None)
            .expect("start invoking transaction");
        let sql = "CREATE SOURCE IF NOT EXISTS materialize.public.ddl_outcome IN CLUSTER source_cluster FROM LOAD GENERATOR COUNTER";
        let staged = stage(&catalog, &session, sql).await;
        let competing = stage(&catalog, &session, sql).await;
        apply(&mut catalog, competing).await.expect("commit competing source");
        let (_, before) = catalog.ddl_transaction_snapshot().await;

        let error = apply(&mut catalog, staged.clone()).await.expect_err("observed creation cannot become a skip");
        assert!(matches!(error, AdapterError::DDLTransactionOutcomeChanged));
        assert_eq!(catalog.ddl_transaction_snapshot().await.1, before);

        let applied = apply(&mut catalog, staged.unobserved()).await.expect("unreported outcome may become a skip");
        assert_eq!(applied.skipped_name(), Some("ddl_outcome"));
        assert_eq!(catalog.ddl_transaction_snapshot().await.1, before);
        assert!(applied.item_ids().all(|id| catalog.try_get_entry(&id).is_none()),
            "a skipped compound source must not leak its progress source");
        catalog.expire().await;
    }).await;
}
