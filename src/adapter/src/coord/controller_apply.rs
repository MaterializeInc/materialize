// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to applying [CatalogSideEffects](CatalogSideEffect) to
//! controller(s).

use maplit::btreeset;
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{DataSourceDesc, Table, TableDataSource};
use mz_ore::instrument;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};
use mz_storage_types::connections::inline::IntoInlineConnection;
use tracing::{info_span, Instrument};

use crate::catalog::side_effects::CatalogSideEffect;
use crate::coord::Coordinator;
use crate::{AdapterError, ResultExt};

impl Coordinator {
    #[instrument(level = "debug")]
    pub async fn controller_apply_side_effects(
        &mut self,
        updates: Vec<CatalogSideEffect>,
    ) -> Result<(), AdapterError> {
        let mut tables_to_drop = Vec::new();

        for update in updates {
            tracing::info!(?update, "have to apply!");
            match update {
                CatalogSideEffect::CreateTable(id, table) => {
                    self.controller_create_table(id, table).await?
                }
                CatalogSideEffect::DropTable(id) => {
                    tables_to_drop.push(id);
                }
            }
        }

        // No error returns are allowed after this point. Enforce this at compile time
        // by using this odd structure so we don't accidentally add a stray `?`.
        //
        // WIP: Do we need to cargo cult this structure?
        let _: () = async {
            if !tables_to_drop.is_empty() {
                let ts = self.get_local_write_ts().await;
                self.drop_tables(tables_to_drop, ts.timestamp);
            }
        }
        .instrument(info_span!("coord::controller_apply_updates::finalize"))
        .await;

        Ok(())
    }

    #[instrument(level = "debug")]
    async fn controller_create_table(
        &mut self,
        table_id: GlobalId,
        table: Table,
    ) -> Result<(), AdapterError> {
        // The table data_source determines whether this table will be written to
        // by environmentd (e.g. with INSERT INTO statements) or by the storage layer
        // (e.g. a source-fed table).
        match table.data_source {
            TableDataSource::TableWrites { defaults: _ } => {
                // Determine the initial validity for the table.
                let register_ts = self.get_local_write_ts().await.timestamp;

                // After acquiring `register_ts` but before using it, we need to
                // be sure we're still the leader. Otherwise a new generation
                // may also be trying to use `register_ts` for a different
                // purpose.
                //
                // See #28216.
                self.catalog
                    .confirm_leadership()
                    .await
                    .unwrap_or_terminate("unable to confirm leadership");

                //if let Some(id) = ctx.as_ref().and_then(|ctx| ctx.extra().contents()) {
                //    self.set_statement_execution_timestamp(id, register_ts);
                //}

                let collection_desc = CollectionDescription::from_desc(
                    table.desc.clone(),
                    DataSourceOther::TableWrites,
                );
                let storage_metadata = self.catalog.state().storage_metadata();
                self.controller
                    .storage
                    .create_collections(
                        storage_metadata,
                        Some(register_ts),
                        vec![(table_id, collection_desc)],
                    )
                    .await
                    .unwrap_or_terminate("cannot fail to create collections");
                self.apply_local_write(register_ts).await;

                self.initialize_storage_read_policies(
                    btreeset![table_id],
                    table
                        .custom_logical_compaction_window
                        .unwrap_or(CompactionWindow::Default),
                )
                .await;
            }
            TableDataSource::DataSource(data_source) => {
                match data_source {
                    DataSourceDesc::IngestionExport {
                        ingestion_id,
                        external_reference: _,
                        details,
                        data_config,
                    } => {
                        // TODO: It's a little weird that a table will be present in this
                        // source status collection, we might want to split out into a separate
                        // status collection.
                        let status_collection_id =
                            Some(self.catalog().resolve_builtin_storage_collection(
                                &mz_catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
                            ));
                        let collection_desc = CollectionDescription::<Timestamp> {
                            desc: table.desc.clone(),
                            data_source: DataSource::IngestionExport {
                                ingestion_id,
                                details,
                                data_config: data_config
                                    .into_inline_connection(self.catalog.state()),
                            },
                            since: None,
                            status_collection_id,
                        };
                        let storage_metadata = self.catalog.state().storage_metadata();
                        self.controller
                            .storage
                            .create_collections(
                                storage_metadata,
                                None,
                                vec![(table_id, collection_desc)],
                            )
                            .await
                            .unwrap_or_terminate("cannot fail to create collections");

                        let read_policies = self
                            .catalog()
                            .state()
                            .source_compaction_windows(vec![table_id]);
                        for (compaction_window, storage_policies) in read_policies {
                            self.initialize_storage_read_policies(
                                storage_policies,
                                compaction_window,
                            )
                            .await;
                        }
                    }
                    _ => unreachable!("CREATE TABLE data source got {:?}", data_source),
                }
            }
        }
        Ok(())
    }

    fn drop_tables(&mut self, tables: Vec<GlobalId>, ts: Timestamp) {
        let storage_metadata = self.catalog.state().storage_metadata();
        self.controller
            .storage
            .drop_tables(storage_metadata, tables, ts)
            .unwrap_or_terminate("cannot fail to drop tables");
    }
}
