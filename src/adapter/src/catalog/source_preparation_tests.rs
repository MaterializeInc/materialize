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

use mz_ore::now::NowFn;
use mz_sql::plan::PlanError;
use mz_sql::pure::{SourcePreparation, SourcePreparationStatus};
use mz_sql::session::vars::VarInput;
use mz_storage_types::configuration::StorageConfiguration;

use crate::catalog::{Catalog, CatalogState};

fn resolve(
    preparation: &SourcePreparation,
    state: &CatalogState,
) -> Result<SourcePreparationStatus, PlanError> {
    let catalog = state.for_system_session();
    let statement = mz_sql_parser::parser::parse_statements(
        "CREATE SOURCE clock_src FROM LOAD GENERATOR CLOCK",
    )
    .expect("parse source statement")
    .remove(0)
    .ast;
    let (statement, _) = mz_sql::names::resolve(&catalog, statement)?;
    preparation.resolve(&catalog, statement)
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
        let original_input = request
            .execute(&storage)
            .await
            .expect("prepare load-generator metadata");
        preparation.record(original_input.clone());
        let SourcePreparationStatus::Ready(original) =
            resolve(&preparation, &state).expect("resolve source preparation")
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
        let changed_input = changed
            .execute(&storage)
            .await
            .expect("prepare load-generator metadata");
        preparation.record(changed_input.clone());

        // A completion from superseded work cannot override current-input validation.
        preparation.record(original_input);
        let SourcePreparationStatus::Pending(retry) =
            resolve(&preparation, &state).expect("resolve source preparation")
        else {
            panic!("late metadata for different inputs must not be accepted");
        };
        assert_eq!(retry, changed);
        preparation.record(changed_input);
        let SourcePreparationStatus::Ready(reprepared) =
            resolve(&preparation, &state).expect("resolve source preparation")
        else {
            panic!("metadata for the current inputs must be accepted");
        };
        assert_eq!(reprepared, original);
        catalog.expire().await;
    })
    .await;
}
