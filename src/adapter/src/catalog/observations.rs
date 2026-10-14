// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::sync::{Arc, Mutex};

use mz_sql::catalog::{CatalogItem, CatalogItemRef};

use crate::AdapterError;
use crate::catalog::{Catalog, CatalogState};

/// Catalog observations whose answers must remain valid when their operations execute.
#[derive(Clone, Debug, Default)]
pub struct CatalogReadSet {
    observations: Vec<Observation>,
}

/// A shared recorder for catalog observations at one operation prefix.
#[derive(Clone, Debug, Default)]
pub struct CatalogReadRecorder {
    reads: Arc<Mutex<CatalogReadSet>>,
}

impl Catalog {
    /// Starts observations with a conservative configuration fence.
    ///
    /// Global and scoped configuration changes invalidate preparation, including
    /// scoped rows written when clusters or replicas are created.
    pub(crate) fn new_read_recorder(&self) -> CatalogReadRecorder {
        let recorder = CatalogReadRecorder::default();
        recorder.record(
            "configuration generation",
            self.state.configuration_generation,
            |state| state.configuration_generation,
        );
        recorder
    }
}

impl CatalogReadRecorder {
    /// Captures the observations recorded so far without changing the recorder.
    /// Call after all preparation work using clones of this recorder has finished
    /// to capture a complete statement.
    pub fn snapshot(&self) -> CatalogReadSet {
        self.reads
            .lock()
            .expect("catalog read recorder poisoned")
            .clone()
    }

    /// Returns an item view that records the properties its callers access.
    pub fn item<'a>(&self, item: &'a dyn CatalogItem) -> CatalogItemRef<'a> {
        super::observed_item::item(self.clone(), item)
    }

    pub(super) fn record<T>(
        &self,
        label: &'static str,
        expected: T,
        read: impl Fn(&CatalogState) -> T + Send + Sync + 'static,
    ) where
        T: PartialEq + Send + Sync + 'static,
    {
        self.reads
            .lock()
            .expect("catalog read recorder poisoned")
            .record(label, expected, read);
    }
}

#[derive(Clone)]
struct Observation {
    label: &'static str,
    matches: Arc<dyn Fn(&CatalogState) -> bool + Send + Sync>,
}

impl fmt::Debug for Observation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Catalog values can include secret definitions. Only the accessor's
        // static label belongs in transaction diagnostics.
        f.debug_tuple("Observation").field(&self.label).finish()
    }
}

impl CatalogReadSet {
    /// Reads a catalog value and records the answer for subsequent validation.
    ///
    /// `read` must be pure and tolerate objects disappearing, for example by returning `None`.
    /// It may run repeatedly against different snapshots, including after a failed commit attempt.
    pub fn observe<T>(
        &mut self,
        state: &CatalogState,
        label: &'static str,
        read: impl Fn(&CatalogState) -> T + Send + Sync + 'static,
    ) -> T
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let answer = read(state);
        self.record(label, answer.clone(), read);
        answer
    }

    pub(super) fn record<T>(
        &mut self,
        label: &'static str,
        expected: T,
        read: impl Fn(&CatalogState) -> T + Send + Sync + 'static,
    ) where
        T: PartialEq + Send + Sync + 'static,
    {
        self.observations.push(Observation {
            label,
            matches: Arc::new(move |state| read(state) == expected),
        });
    }

    /// Checks observations against the state at their position in a catalog transaction.
    pub fn check(&self, state: &CatalogState) -> Result<(), AdapterError> {
        for observation in &self.observations {
            if !(observation.matches)(state) {
                tracing::debug!(accessor = observation.label, "catalog observation changed");
                return Err(AdapterError::DDLTransactionRace);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mz_catalog::SYSTEM_CONN_ID;
    use mz_sql::catalog::CatalogDatabase;
    use mz_sql::names::ResolvedDatabaseSpecifier;
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

    use super::CatalogReadSet;
    use crate::AdapterError;
    use crate::catalog::{Catalog, Op};
    use crate::session::DEFAULT_DATABASE_NAME;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
    async fn observations_validate_the_operation_prefix_atomically() {
        Catalog::with_debug(|mut catalog| async move {
            let database_id = ResolvedDatabaseSpecifier::Id(
                catalog
                    .resolve_database(DEFAULT_DATABASE_NAME)
                    .expect("default database exists")
                    .id(),
            );
            let create_schema = |name: &str| Op::CreateSchema {
                database_id,
                schema_name: name.to_owned(),
                owner_id: MZ_SYSTEM_ROLE_ID,
            };
            let mut reads = CatalogReadSet::default();
            let exists = reads.observe(catalog.state(), "schema exists", move |state| {
                state
                    .resolve_schema_in_database(&database_id, "observed", &SYSTEM_CONN_ID)
                    .is_ok()
            });
            assert!(!exists);

            let timestamp = catalog.current_upper().await;
            let result = catalog
                .transact(
                    None,
                    timestamp,
                    None,
                    vec![create_schema("observed"), Op::ValidateReads(reads.clone())],
                )
                .await;
            assert!(matches!(result, Err(AdapterError::DDLTransactionRace)));
            assert!(
                catalog
                    .resolve_schema_in_database(&database_id, "observed", &SYSTEM_CONN_ID)
                    .is_err()
            );
            {
                let mut storage = catalog.storage().await;
                let tx = storage
                    .transaction()
                    .await
                    .expect("open catalog transaction");
                assert!(tx.get_schemas().all(|schema| schema.name != "observed"));
            }

            let timestamp = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    timestamp,
                    None,
                    vec![
                        create_schema("unrelated"),
                        Op::ValidateReads(reads.clone()),
                        create_schema("observed"),
                    ],
                )
                .await
                .expect("unrelated writes preserve the observed absence");
            assert!(matches!(
                reads.check(catalog.state()),
                Err(AdapterError::DDLTransactionRace)
            ));
            catalog.expire().await;
        })
        .await;
    }
}
