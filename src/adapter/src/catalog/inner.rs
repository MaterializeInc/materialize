// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functionality belonging to the catalog but extracted to control file size.

use std::collections::BTreeSet;

use mz_audit_log::{EventDetails, EventType, VersionedEvent};
use mz_catalog::Transaction;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::CatalogItem as SqlCatalogItem;

use crate::catalog::{
    catalog_type_to_audit_object_type, BuiltinTableUpdate, Catalog, CatalogEntry, CatalogItem,
    CatalogState, Error, ErrorKind,
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
        drop_ids: &BTreeSet<GlobalId>,
        audit_events: &mut Vec<VersionedEvent>,
        session: Option<&ConnMeta>,
        id: GlobalId,
        item: CatalogItem,
    ) -> Result<(), AdapterError> {
        let entry = state.get_entry(&id);
        let name = entry.name().clone();

        let cluster_id = item
            .cluster_id()
            .expect("SET CLUSTER called on incorrect item type");

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
                old_cluster: entry
                    .cluster_id()
                    .map(|old_cluster| old_cluster.to_string()),
                new_cluster: Some(cluster_id.to_string()),
            }),
        )?;

        let to_name = entry.name().clone();
        builtin_table_updates.extend(state.pack_item_update(id, -1));
        state.move_item(id, cluster_id);
        Self::update_item(state, builtin_table_updates, id, to_name, item, drop_ids)
    }
}
