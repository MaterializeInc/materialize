// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::catalog::{CatalogError, CatalogItemRef};
use mz_sql::names::{DatabaseId, PartialItemName, ResolvedDatabaseSpecifier, SchemaSpecifier};

use super::{CatalogState, ConnCatalog, ConnectionId};

#[derive(Clone, Copy)]
pub(super) enum ItemNamespace {
    Item,
    Function,
    Type,
}

impl ItemNamespace {
    fn resolve(
        self,
        state: &CatalogState,
        database: Option<&DatabaseId>,
        search_path: &[(ResolvedDatabaseSpecifier, SchemaSpecifier)],
        name: &PartialItemName,
        conn_id: &ConnectionId,
        unresolvable_ids: &BTreeSet<CatalogItemId>,
    ) -> Result<CatalogItemId, CatalogError> {
        let search_path = state.effective_search_path(search_path, matches!(self, Self::Item));
        let entry = match self {
            Self::Item => state.resolve_entry(database, &search_path, name, conn_id),
            Self::Function => state.resolve_function(database, &search_path, name, conn_id),
            Self::Type => state.resolve_type(database, &search_path, name, conn_id),
        }?;
        if unresolvable_ids.contains(&entry.id()) {
            return Err(match self {
                Self::Item => CatalogError::UnknownItem(name.to_string()),
                Self::Function => CatalogError::UnknownFunction {
                    name: name.to_string(),
                    alternative: None,
                },
                Self::Type => CatalogError::UnknownType {
                    name: name.to_string(),
                },
            });
        }
        Ok(entry.id())
    }
}

impl ConnCatalog<'_> {
    pub(crate) fn is_system_item_without_introspection(&self, id: CatalogItemId) -> bool {
        self.observe("system item without introspection", move |state| {
            state.try_get_entry(&id).map(|entry| {
                state.is_system_schema_specifier(entry.name.qualifiers.schema_spec)
                    && state.introspection_dependencies(id).is_empty()
            })
        })
        .expect("catalog item exists")
    }

    pub(super) fn observed_collection(
        &self,
        id: &GlobalId,
    ) -> Option<Box<dyn mz_sql::catalog::CatalogCollectionItem>> {
        let global_id = *id;
        let item_id = self.observe("item by global ID", move |state| {
            state
                .try_get_entry_by_global_id(&global_id)
                .map(|entry| entry.id())
        })?;
        Some(self.state.collection(item_id, id))
    }

    pub(super) fn observe<T>(
        &self,
        label: &'static str,
        read: impl Fn(&CatalogState) -> T + Send + Sync + 'static,
    ) -> T
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.state.observe(label, read)
    }

    pub(super) fn resolve_observed_item(
        &self,
        name: &PartialItemName,
        namespace: ItemNamespace,
    ) -> Result<CatalogItemRef<'_>, CatalogError> {
        let answer = self.state.read_namespace(
            |state, database, search_path| {
                namespace.resolve(
                    state,
                    database,
                    search_path,
                    name,
                    &self.conn_id,
                    &self.unresolvable_ids,
                )
            },
            |answer| {
                let name = name.clone();
                let database_name = self.database_name.clone().into_owned();
                let raw_search_path = self.raw_search_path.clone().into_owned();
                let conn_id = self.conn_id.clone();
                let unresolvable_ids = self.unresolvable_ids.clone();
                (
                    "item resolution",
                    answer.clone(),
                    move |state: &CatalogState| {
                        let database = state.database_by_name.get(&database_name);
                        // Re-resolve the raw path so newly created schemas can shadow a
                        // binding. A changed path alone is not a changed binding.
                        let search_path = state.resolve_search_path_for(
                            database,
                            raw_search_path.iter().map(|name| name.as_str()),
                            &conn_id,
                        );
                        namespace.resolve(
                            state,
                            database,
                            &search_path,
                            &name,
                            &conn_id,
                            &unresolvable_ids,
                        )
                    },
                )
            },
        );
        answer.map(|id| self.state.item(&id))
    }
}

#[cfg(test)]
mod tests {
    use mz_catalog::SYSTEM_CONN_ID;
    use mz_catalog::memory::objects::{CatalogItem, Secret};
    use mz_sql::catalog::{CatalogDatabase, CatalogSchema, SessionCatalog};
    use mz_sql::names::{ItemQualifiers, QualifiedItemName};
    use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
    use mz_sql::session::vars::{SystemVars, VarInput};

    use super::*;
    use crate::AdapterError;
    use crate::catalog::{Catalog, CatalogReadRecorder, DropObjectInfo, Op};
    use crate::session::{DEFAULT_DATABASE_NAME, Session};

    async fn create_item(catalog: &mut Catalog, schema: &str, name: &str) {
        let database_spec = ResolvedDatabaseSpecifier::Id(
            catalog
                .resolve_database(DEFAULT_DATABASE_NAME)
                .expect("default database")
                .id(),
        );
        let schema_spec = catalog
            .resolve_schema_in_database(&database_spec, schema, &SYSTEM_CONN_ID)
            .expect("fixture schema")
            .id()
            .clone();
        let (id, global_id) = catalog
            .allocate_user_id_for_test()
            .await
            .expect("allocate item ID");
        let timestamp = catalog.current_upper().await;
        catalog
            .transact(
                None,
                timestamp,
                None,
                vec![Op::CreateItem {
                    id,
                    name: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec,
                            schema_spec,
                        },
                        item: name.to_owned(),
                    },
                    item: CatalogItem::Secret(Secret {
                        create_sql: format!("CREATE SECRET materialize.{schema}.{name} AS 'value'"),
                        global_id,
                    }),
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("create fixture item");
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
    async fn name_reads_detect_shadowing_and_phantoms_without_conflicting_on_empty_schemas() {
        Catalog::with_debug(|mut catalog| async move {
            let database_id = ResolvedDatabaseSpecifier::Id(
                catalog
                    .resolve_database(DEFAULT_DATABASE_NAME)
                    .expect("default database")
                    .id(),
            );
            let schema_op = |name: &str| Op::CreateSchema {
                database_id,
                schema_name: name.to_owned(),
                owner_id: MZ_SYSTEM_ROLE_ID,
            };
            let timestamp = catalog.current_upper().await;
            catalog
                .transact(None, timestamp, None, vec![schema_op("later")])
                .await
                .expect("create later schema");
            create_item(&mut catalog, "later", "target").await;

            let mut session = Session::dummy();
            session
                .vars_mut()
                .set(
                    &SystemVars::new(),
                    "search_path",
                    VarInput::Flat("earlier, later"),
                    false,
                )
                .expect("set search path");
            let positive = CatalogReadRecorder::default();
            let negative = CatalogReadRecorder::default();
            let name = |item: &str| PartialItemName {
                database: None,
                schema: None,
                item: item.to_owned(),
            };
            assert!(
                catalog
                    .for_preparation(
                        &session,
                        &crate::catalog::CatalogPreparation::Observed(positive.clone())
                    )
                    .resolve_item(&name("target"))
                    .is_ok()
            );
            assert!(
                catalog
                    .for_preparation(
                        &session,
                        &crate::catalog::CatalogPreparation::Observed(negative.clone())
                    )
                    .resolve_item(&name("missing"))
                    .is_err()
            );
            let positive = positive.snapshot();
            let negative = negative.snapshot();

            let path_reads = CatalogReadRecorder::default();
            let effective_path_reads = CatalogReadRecorder::default();
            let schema_reads = CatalogReadRecorder::default();
            let _ = catalog
                .for_preparation(
                    &session,
                    &crate::catalog::CatalogPreparation::Observed(path_reads.clone()),
                )
                .search_path();
            let _ = catalog
                .for_preparation(
                    &session,
                    &crate::catalog::CatalogPreparation::Observed(effective_path_reads.clone()),
                )
                .effective_search_path(true);
            let _ = catalog
                .for_preparation(
                    &session,
                    &crate::catalog::CatalogPreparation::Observed(schema_reads.clone()),
                )
                .get_schemas();
            let absent_schemas: Vec<_> = [false, true]
                .into_iter()
                .map(|qualified| {
                    let reads = CatalogReadRecorder::default();
                    let catalog = catalog.for_preparation(
                        &session,
                        &crate::catalog::CatalogPreparation::Observed(reads.clone()),
                    );
                    let result = if qualified {
                        catalog.resolve_schema_in_database(&database_id, "earlier")
                    } else {
                        catalog.resolve_schema(None, "earlier")
                    };
                    assert!(result.is_err());
                    reads.snapshot()
                })
                .collect();

            let timestamp = catalog.current_upper().await;
            catalog
                .transact(None, timestamp, None, vec![schema_op("earlier")])
                .await
                .expect("create earlier schema");
            assert!(positive.check(catalog.state()).is_ok());
            assert!(negative.check(catalog.state()).is_ok());
            for reads in absent_schemas.into_iter().chain([
                path_reads.snapshot(),
                effective_path_reads.snapshot(),
                schema_reads.snapshot(),
            ]) {
                assert!(matches!(
                    reads.check(catalog.state()),
                    Err(AdapterError::DDLTransactionRace)
                ));
            }

            create_item(&mut catalog, "earlier", "target").await;
            assert!(matches!(
                positive.check(catalog.state()),
                Err(AdapterError::DDLTransactionRace)
            ));
            assert!(negative.check(catalog.state()).is_ok());

            create_item(&mut catalog, "earlier", "missing").await;
            assert!(matches!(
                negative.check(catalog.state()),
                Err(AdapterError::DDLTransactionRace)
            ));
            catalog.expire().await;
        })
        .await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
    async fn qualified_absence_validation_tolerates_a_dropped_schema() {
        Catalog::with_debug(|mut catalog| async move {
            let database_spec = ResolvedDatabaseSpecifier::Id(
                catalog
                    .resolve_database(DEFAULT_DATABASE_NAME)
                    .expect("default database")
                    .id(),
            );
            let timestamp = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    timestamp,
                    None,
                    vec![Op::CreateSchema {
                        database_id: database_spec,
                        schema_name: "lookup_schema".to_owned(),
                        owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                )
                .await
                .expect("create schema");
            let schema_spec = catalog
                .resolve_schema_in_database(&database_spec, "lookup_schema", &SYSTEM_CONN_ID)
                .expect("fixture schema")
                .id()
                .clone();
            let name = QualifiedItemName {
                qualifiers: ItemQualifiers {
                    database_spec,
                    schema_spec,
                },
                item: "absent".to_owned(),
            };
            let recorder = CatalogReadRecorder::default();
            assert!(
                catalog
                    .for_system_session()
                    .with_read_recorder(recorder.clone())
                    .get_item_by_name(&name)
                    .is_none()
            );
            let timestamp = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    timestamp,
                    None,
                    vec![Op::DropObjects(vec![DropObjectInfo::Schema((
                        database_spec,
                        schema_spec,
                    ))])],
                )
                .await
                .expect("drop empty schema");
            assert!(matches!(
                recorder.snapshot().check(catalog.state()),
                Err(AdapterError::DDLTransactionRace)
            ));
            catalog.expire().await;
        })
        .await;
    }
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
    async fn routing_reads_ignore_names_but_detect_eligibility_changes() {
        Catalog::with_debug(|catalog| async move {
            let mut state = catalog.state().clone();
            let id = state.get_system_type("text").id();
            let reads = CatalogReadRecorder::default();
            assert!(
                state
                    .for_system_session()
                    .with_read_recorder(reads.clone())
                    .is_system_item_without_introspection(id)
            );
            state
                .entry_by_id
                .get_mut(&id)
                .expect("builtin type exists")
                .name
                .item = "renamed_type".to_owned();
            assert!(reads.snapshot().check(&state).is_ok());
            state
                .entry_by_id
                .get_mut(&id)
                .expect("builtin type exists")
                .name
                .qualifiers
                .schema_spec = SchemaSpecifier::Temporary;
            assert!(reads.snapshot().check(&state).is_err());
            catalog.expire().await;
        })
        .await;
    }
}
