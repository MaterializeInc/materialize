// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transactional publication of the active deployment's common configuration.

use std::collections::{BTreeMap, BTreeSet};

use mz_catalog::builtin::{
    BuiltinTable, MZ_CLUSTER_REPLICA_SIZE_INTERNAL, MZ_CLUSTER_REPLICA_SIZES, MZ_EGRESS_IPS,
    MZ_LICENSE_KEYS,
};
use mz_repr::{CatalogItemId, Diff, Row, Timestamp};
use mz_storage_client::client::TableData;
use mz_storage_client::collection_reader::CollectionReader;
use mz_storage_types::controller::StorageError;

use crate::catalog::{BuiltinTableUpdate, Catalog};
use crate::coord::Coordinator;
use crate::util::ResultExt;

fn tables() -> [&'static BuiltinTable; 4] {
    [
        &MZ_CLUSTER_REPLICA_SIZES,
        &MZ_CLUSTER_REPLICA_SIZE_INTERNAL,
        &MZ_EGRESS_IPS,
        &MZ_LICENSE_KEYS,
    ]
}

pub(super) fn table_ids(catalog: &Catalog) -> BTreeSet<CatalogItemId> {
    tables()
        .into_iter()
        .map(|table| catalog.resolve_builtin_table(table))
        .collect()
}

pub(super) struct Reconciliation {
    desired: BTreeMap<CatalogItemId, Vec<Row>>,
}

impl Reconciliation {
    /// Captures local configuration, including tables whose desired contents are empty.
    pub(super) fn new(catalog: &Catalog, snapshot: &[BuiltinTableUpdate]) -> Self {
        let mut desired: BTreeMap<_, Vec<Row>> = table_ids(catalog)
            .into_iter()
            .map(|id| (id, Vec::new()))
            .collect();
        for update in snapshot {
            if let Some(rows) = desired.get_mut(&update.id) {
                let TableData::Rows(updates) = &update.data else {
                    panic!("configuration snapshots contain rows, not staged batches");
                };
                for (row, diff) in updates {
                    assert_eq!(*diff, Diff::ONE, "configuration snapshots are positive");
                    rows.push(row.clone());
                }
            }
        }
        Self { desired }
    }
}

impl Coordinator {
    /// Reconciles configuration before readiness using the bootstrap WAL snapshot.
    pub(super) async fn reconcile_builtin_configuration(
        &mut self,
        state: Option<Reconciliation>,
        reader: &CollectionReader,
        txns_shard: mz_persist_client::ShardId,
        mut read_ts: Timestamp,
    ) {
        let Some(state) = state else {
            return;
        };
        // A prewarming catalog handle can publish protection, so its writability
        // alone does not authorize configuration changes. This is the writable
        // bootstrap path, never the read-only replacement-shard backfill path.
        assert!(!self.controller.read_only());
        let max_attempts = mz_adapter_types::dyncfgs::GROUP_COMMIT_MAX_ATTEMPTS
            .get(self.catalog().system_config().dyncfgs())
            .max(1);
        for attempt in 0..max_attempts {
            if attempt > 0 {
                let timestamp = self.get_local_write_ts().await;
                self.catalog()
                    .upper_handle()
                    .advance_upper(timestamp.advance_to)
                    .await
                    .unwrap_or_terminate(
                        "unable to advance catalog upper for configuration snapshot",
                    );
                match self
                    .table_write_handle
                    .append(timestamp.timestamp, timestamp.advance_to, Vec::new())
                    .await
                    .expect("bootstrap owns the table write outcome")
                {
                    Ok(()) => {
                        read_ts = timestamp.timestamp;
                        self.apply_local_write(read_ts).await;
                    }
                    Err(StorageError::InvalidUppers(_)) => continue,
                    Err(error) => Err::<(), _>(error)
                        .unwrap_or_terminate("cannot fence configuration snapshot"),
                }
            }
            let mut appends = Vec::new();
            for (id, desired) in &state.desired {
                let table = self
                    .catalog()
                    .get_entry(id)
                    .table()
                    .expect("configuration is a table");
                let global_id = table.global_id_writes();
                let metadata = mz_storage_types::controller::CollectionMetadata {
                    persist_location: self.query_persist_location.clone(),
                    data_shard: self
                        .catalog()
                        .state()
                        .storage_metadata()
                        .get_collection_shard(global_id)
                        .expect("configuration has committed shard metadata"),
                    relation_desc: table.desc.latest(),
                    txns_shard: Some(txns_shard),
                };
                let mut changes: Vec<_> = desired
                    .iter()
                    .cloned()
                    .map(|row| (row, Diff::ONE))
                    .collect();
                let mut snapshot = reader
                    .snapshot_cursor(global_id, metadata, read_ts)
                    .await
                    .unwrap_or_terminate("cannot snapshot builtin configuration");
                while let Some(values) = snapshot.next().await {
                    for (data, _, diff) in values {
                        changes.push((
                            data.0.expect("configuration contains valid rows"),
                            Diff::from(-diff),
                        ));
                    }
                }
                differential_dataflow::consolidation::consolidate(&mut changes);
                if !changes.is_empty() {
                    appends.push((global_id, vec![TableData::Rows(changes)]));
                }
            }
            if appends.is_empty() {
                return;
            }
            let write_ts = read_ts.step_forward();
            let advance_to = write_ts.step_forward();
            // A snapshot at B requires WAL upper > B. Commit at B+1 can succeed
            // only with upper == B+1, so no intervening write can invalidate any
            // of the four snapshots. Advance the catalog first to validate runtime
            // authority and order this write against deployment promotion.
            self.catalog()
                .upper_handle()
                .advance_upper(advance_to)
                .await
                .unwrap_or_terminate("unable to authorize builtin configuration publication");
            match self
                .table_write_handle
                .append(write_ts, advance_to, appends)
                .await
                .expect("bootstrap owns the table write outcome")
            {
                Ok(()) => {
                    self.apply_local_write(write_ts).await;
                    return;
                }
                Err(StorageError::InvalidUppers(_)) => {
                    // Discard all proposed diffs and resnapshot after a fresh fence.
                }
                Err(error) => {
                    Err::<(), _>(error).unwrap_or_terminate("cannot publish builtin configuration")
                }
            }
        }
        mz_ore::halt!(
            "builtin configuration reconciliation reached attempt limit {max_attempts}, rebuilding"
        );
    }
}
