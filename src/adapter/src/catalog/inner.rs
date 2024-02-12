// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functionality belonging to the catalog but extracted to control file size.

use mz_audit_log::{EventDetails, EventType, VersionedEvent};
use mz_catalog::durable::Transaction;
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_controller_types::ClusterId;
use mz_ore::collections::CollectionExt;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::CatalogItem as SqlCatalogItem;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{Ident, RawClusterName, Statement};
use mz_storage_types::sources::IngestionDescription;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::{
    catalog_type_to_audit_object_type, BuiltinTableUpdate, Catalog, CatalogEntry, CatalogItem,
    CatalogState, DataSourceDesc, Index, MaterializedView, Sink, Source,
};
use crate::coord::ConnMeta;
use crate::AdapterError;

impl Catalog {
    /// Update catalog in response to an alter set cluster operation.
    pub(super) fn transact_alter_set_cluster(
        state: &mut CatalogState,
        tx: &mut Transaction,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        oracle_write_ts: Timestamp,
        audit_events: &mut Vec<VersionedEvent>,
        session: Option<&ConnMeta>,
        id: GlobalId,
        cluster_id: ClusterId,
    ) -> Result<(), AdapterError> {
        let entry = state.get_entry(&id);
        let name = entry.name().clone();

        let cluster = state.get_cluster(cluster_id);

        // Prevent changing system objects.
        if entry.id().is_system() {
            let schema_name = state
                .resolve_full_name(&name, session.map(|session| session.conn_id()))
                .schema;
            return Err(AdapterError::Catalog(Error::new(
                ErrorKind::ReadOnlySystemSchema(schema_name),
            )));
        }

        // Only internal users can move objects to system clusters
        if cluster.id.is_system()
            && !session
                .map(|session| session.user().is_internal())
                .unwrap_or(false)
        {
            return Err(AdapterError::Catalog(Error::new(
                ErrorKind::ReadOnlyCluster(cluster.name.clone()),
            )));
        }

        // Since the catalog serializes the items using only their creation statement
        // and context, we need to parse and rewrite the with options in that statement.
        // (And then make any other changes to the object definition to match.)
        let mut stmt = mz_sql::parse::parse(entry.create_sql())
            // TODO: Should never happen
            .expect("invalid create sql persisted to catalog")
            .into_element()
            .ast;

        // Patch AST.
        let stmt_in_cluster = match &mut stmt {
            Statement::CreateIndex(s) => &mut s.in_cluster,
            Statement::CreateMaterializedView(s) => &mut s.in_cluster,
            Statement::CreateSink(s) => &mut s.in_cluster,
            Statement::CreateSource(s) => &mut s.in_cluster,
            // Planner produced wrong plan.
            _ => coord_bail!("object {id} does not have an associated cluster"),
        };
        let old_cluster = entry.cluster_id();
        let cluster_name = Ident::new(cluster.name.clone())?;
        *stmt_in_cluster = Some(RawClusterName::Unresolved(cluster_name));

        // Update catalog item with new cluster.
        let create_sql = stmt.to_ast_string_stable();
        let item = match (&stmt, entry.item().clone()) {
            (Statement::CreateIndex(_), CatalogItem::Index(old_index)) => {
                CatalogItem::Index(Index {
                    create_sql,
                    cluster_id: cluster.id,
                    ..old_index
                })
            }
            (Statement::CreateMaterializedView(_), CatalogItem::MaterializedView(old_mv)) => {
                CatalogItem::MaterializedView(MaterializedView {
                    create_sql,
                    cluster_id: cluster.id,
                    ..old_mv
                })
            }
            (Statement::CreateSink(_), CatalogItem::Sink(old_sink)) => CatalogItem::Sink(Sink {
                create_sql,
                cluster_id: cluster.id,
                ..old_sink
            }),
            (Statement::CreateSource(_), CatalogItem::Source(old_source)) => {
                match old_source.data_source {
                    DataSourceDesc::Ingestion(ingestion) => CatalogItem::Source(Source {
                        create_sql: Some(create_sql),
                        data_source: DataSourceDesc::Ingestion(IngestionDescription {
                            instance_id: cluster.id,
                            ..ingestion
                        }),
                        ..old_source
                    }),
                    DataSourceDesc::Source
                    | DataSourceDesc::Introspection(_)
                    | DataSourceDesc::Progress
                    | DataSourceDesc::Webhook { .. } => {
                        // Planner produced wrong plan.
                        coord_bail!("source {id} does not have an associated cluster");
                    }
                }
            }
            // Planner produced wrong plan.
            _ => coord_bail!("object {id} does not have an associated cluster"),
        };

        let new_entry = CatalogEntry {
            item: item.clone(),
            ..entry.clone()
        };

        tx.update_item(id, new_entry.into())?;

        state.add_to_audit_log(
            oracle_write_ts,
            session,
            tx,
            builtin_table_updates,
            audit_events,
            EventType::Alter,
            catalog_type_to_audit_object_type(entry.item().typ()),
            EventDetails::AlterSetClusterV1(mz_audit_log::AlterSetClusterV1 {
                id: id.to_string(),
                name: Self::full_name_detail(
                    &state.resolve_full_name(&name, session.map(|session| session.conn_id())),
                ),
                old_cluster: old_cluster.map(|old_cluster| old_cluster.to_string()),
                new_cluster: Some(cluster_id.to_string()),
            }),
        )?;

        let to_name = entry.name().clone();
        builtin_table_updates.extend(state.pack_item_update(id, -1));
        state.move_item(id, cluster_id);
        Self::update_item(state, builtin_table_updates, id, to_name, item)
    }
}
