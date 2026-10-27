// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Finite publication of active deployment configuration before child launch.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use mz_persist_client::Diagnostics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, Row, Timestamp};
use mz_storage_client::client::TableData;
use mz_storage_types::sources::SourceData;
use mz_timestamp_oracle::TimestampOracle;

use crate::builtin::{
    MZ_CLUSTER_REPLICA_SIZE_INTERNAL, MZ_CLUSTER_REPLICA_SIZES, MZ_EGRESS_IPS, MZ_LICENSE_KEYS,
};
use crate::catalog::{BuiltinTableUpdate, Catalog, CatalogError, TableInfrastructure};

impl Catalog {
    /// Reconciles shared configuration from this active initializer's inputs.
    ///
    /// Call after table infrastructure completion and before launching children.
    /// `snapshot` is the builtin snapshot returned by catalog open. Replica-size
    /// rows are derived here. Joiners, savepoints and read-only deployments cannot
    /// publish configuration. This performs no ongoing maintenance or compaction.
    pub async fn reconcile_deployment_configuration(
        &self,
        persist: &mz_persist_client::PersistClient,
        infrastructure: &mut TableInfrastructure,
        snapshot: &[BuiltinTableUpdate],
        oracle: &(dyn TimestampOracle<Timestamp> + Send + Sync),
    ) -> Result<(), CatalogError> {
        {
            let storage = self.storage().await;
            if self.diagnostic_config.read_only
                || storage.is_read_only()
                || storage.is_savepoint()
                || storage.is_join()
            {
                return Err(CatalogError::ReadOnly);
            }
        }
        let mut desired: BTreeMap<_, Vec<Row>> = [
            &*MZ_CLUSTER_REPLICA_SIZES,
            &*MZ_CLUSTER_REPLICA_SIZE_INTERNAL,
            &*MZ_EGRESS_IPS,
            &*MZ_LICENSE_KEYS,
        ]
        .into_iter()
        .map(|table| (self.resolve_builtin_table(table), Vec::new()))
        .collect();
        let sizes = self
            .state()
            .resolve_builtin_table_updates(self.state().pack_all_replica_size_updates());
        for update in snapshot.iter().chain(&sizes) {
            if let Some(rows) = desired.get_mut(&update.id) {
                let TableData::Rows(updates) = &update.data else {
                    return Err(anyhow::anyhow!("configuration snapshot must contain rows").into());
                };
                for (row, diff) in updates {
                    if *diff != Diff::ONE {
                        return Err(
                            anyhow::anyhow!("configuration snapshot must be positive").into()
                        );
                    }
                    rows.push(row.clone());
                }
            }
        }
        let mut readers = Vec::new();
        // Leases precede the fence. The earlier infrastructure timestamp alone
        // provides no read protection while peers advance permitted bounds.
        for (id, rows) in desired {
            let table = self
                .get_entry(&id)
                .table()
                .context("configuration is a table")?;
            let global_id = table.global_id_writes();
            let shard = *infrastructure
                .routes
                .get(&global_id)
                .context("configuration must have completed WAL registration")?;
            let reader = persist
                .open_leased_reader(
                    shard,
                    Arc::new(table.desc.latest()),
                    Arc::new(UnitSchema),
                    Diagnostics {
                        shard_name: global_id.to_string(),
                        handle_purpose: "deployment configuration".into(),
                    },
                    false,
                )
                .await
                .expect("valid configuration schema");
            readers.push((shard, rows, reader));
        }
        let result: Result<(), CatalogError> = async {
            let mut lower_bound = infrastructure.completed_at.step_forward();
            for (_, _, reader) in &readers {
                let since = reader
                    .since()
                    .as_option()
                    .context("configuration shard must not be finalized")?;
                lower_bound = lower_bound.max(*since);
            }
            let attempts = mz_adapter_types::dyncfgs::GROUP_COMMIT_MAX_ATTEMPTS
                .get(self.system_config().dyncfgs())
                .max(1);
            for _ in 0..attempts {
                let write = loop {
                    let write = oracle.write_ts().await;
                    if write.timestamp >= lower_bound {
                        break write;
                    }
                    tokio::task::yield_now().await;
                };
                let fence = write.timestamp;
                self.advance_upper(fence.step_forward()).await?;
                let mut txn = infrastructure.txns.begin();
                txn.tidy(std::mem::take(&mut infrastructure.tidy));
                match txn.commit_at(&mut infrastructure.txns, fence).await {
                    Ok(apply) => infrastructure
                        .tidy
                        .merge(apply.apply(&mut infrastructure.txns).await),
                    Err(upper) => {
                        infrastructure.tidy.merge(txn.take_tidy());
                        lower_bound = upper;
                        continue;
                    }
                }
                oracle.apply_write(fence).await;
                infrastructure.completed_at = fence;
                let tidy = infrastructure.txns.apply_le(&fence).await;
                infrastructure.tidy.merge(tidy);
                let mut txn = infrastructure.txns.begin();
                let mut changed = false;
                for (shard, desired, reader) in &mut readers {
                    let actual = infrastructure
                        .txns
                        .read_cache()
                        .data_snapshot(*shard, fence)
                        .snapshot_and_fetch(reader)
                        .await
                        .map_err(|_| {
                            anyhow::anyhow!("configuration snapshot lost its leased since")
                        })?;
                    let mut diffs: Vec<_> =
                        desired.iter().cloned().map(|row| (row, 1i64)).collect();
                    for ((data, ()), _, diff) in actual {
                        let row = data.0.map_err(|error| {
                            anyhow::anyhow!("invalid configuration row: {error}")
                        })?;
                        diffs.push((row, -diff));
                    }
                    differential_dataflow::consolidation::consolidate(&mut diffs);
                    changed |= !diffs.is_empty();
                    for (row, diff) in diffs {
                        txn.write(shard, SourceData(Ok(row)), (), diff).await;
                    }
                }
                if !changed {
                    return Ok(());
                }
                let timestamp = fence.step_forward();
                self.advance_upper(timestamp.step_forward()).await?;
                txn.tidy(std::mem::take(&mut infrastructure.tidy));
                // Snapshot at B requires WAL upper > B. Success at B+1 proves
                // there was no intervening write to invalidate any of the diffs.
                match txn.commit_at(&mut infrastructure.txns, timestamp).await {
                    Ok(apply) => {
                        infrastructure
                            .tidy
                            .merge(apply.apply(&mut infrastructure.txns).await);
                        oracle.apply_write(timestamp).await;
                        let tidy = infrastructure.txns.apply_le(&timestamp).await;
                        infrastructure.tidy.merge(tidy);
                        infrastructure.completed_at = timestamp;
                        return Ok(());
                    }
                    Err(upper) => {
                        infrastructure.tidy.merge(txn.take_tidy());
                        lower_bound = upper;
                    }
                }
                // Discard this transaction. Diffs must never be replayed at a
                // later timestamp without a new fence and fresh snapshots.
            }
            Err(
                anyhow::anyhow!("deployment configuration reconciliation exhausted its attempts")
                    .into(),
            )
        }
        .await;
        for (_, _, reader) in readers {
            reader.expire().await;
        }
        result
    }
}
