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
use mz_sql::pure::{SourcePreparation, SourcePreparationResponse, SourcePreparationStatus};
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
        assert!(matches!(
            finish(&mut preparation, &state, changed_input).expect("accept current metadata"),
            SourcePreparationStatus::Ready(_)
        ));

        // A completion from superseded work cannot override current-input validation.
        let SourcePreparationStatus::Pending(retry) =
            finish(&mut preparation, &state, late_input).expect("reject obsolete metadata")
        else {
            panic!("late metadata for different inputs must not be accepted");
        };
        assert_eq!(retry, changed);
        let changed_input = changed.execute(&storage).await;
        let SourcePreparationStatus::Ready(reprepared) =
            finish(&mut preparation, &state, changed_input).expect("accept current metadata")
        else {
            panic!("metadata for the current inputs must be accepted");
        };
        assert_eq!(reprepared, original);
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
        let sql = "CREATE SOURCE auction_src FROM LOAD GENERATOR AUCTION FOR ALL TABLES";
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
