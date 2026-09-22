// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Commit-boundary tests use real Iceberg metadata and manifests, but do not
//! write Parquet payloads. File record counts represent one append per timestamp.

#![allow(
    clippy::disallowed_types,
    reason = "Iceberg's Catalog API requires std HashMap properties"
)]

use std::collections::HashMap;
use std::sync::Mutex;

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Operation, Snapshot, SnapshotReference,
    SnapshotRetention, Struct,
};
use iceberg::{CatalogBuilder, MemoryCatalog, Namespace, TableCommit, TableUpdate};
use mz_ore::metrics::MetricsRegistry;
use mz_repr::SqlScalarType;

use super::*;
use crate::metrics::sink::iceberg::IcebergSinkMetricDefs;

fn properties(upper: u64) -> Vec<(String, String)> {
    vec![
        ("mz-sink-id".into(), GlobalId::User(1).to_string()),
        (
            "mz-frontier".into(),
            serde_json::to_string(&[Timestamp::new(upper)]).unwrap(),
        ),
        ("mz-sink-version".into(), "1".into()),
    ]
}

fn data_file(table: &Table, name: &str, records: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!(
            "{}/data/{name}.parquet",
            table.metadata().location()
        ))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(records)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .partition(Struct::empty())
        .build()
        .unwrap()
}

/// Publishes the competing batch after the loser's transaction refresh, but
/// before its catalog CAS. The inner catalog checks the real requirements and
/// produces the conflict, rather than the wrapper fabricating an error.
/// Conflicts may be retried within Iceberg or by the sink's outer retry loop.
#[derive(Debug)]
struct CompetingCatalog {
    inner: MemoryCatalog,
    winner: Mutex<Option<(Table, DataFile, Vec<(String, String)>)>>,
    lose_response: Mutex<bool>,
    fail_reload: Mutex<bool>,
}

#[async_trait::async_trait]
impl Catalog for CompetingCatalog {
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        if std::mem::take(&mut *self.fail_reload.lock().unwrap()) {
            return Err(iceberg::Error::new(
                ErrorKind::Unexpected,
                "reload unavailable",
            ));
        }
        self.inner.load_table(table).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        let winner = self.winner.lock().unwrap().take();
        if let Some((table, file, properties)) = winner {
            let tx = Transaction::new(&table);
            tx.row_delta()
                .set_snapshot_properties(properties.into_iter().collect())
                .add_data_files(vec![file])
                .apply(tx)?
                .commit(&self.inner)
                .await?;
        }
        let table = self.inner.update_table(commit).await?;
        if std::mem::take(&mut *self.lose_response.lock().unwrap()) {
            *self.fail_reload.lock().unwrap() = true;
            return Err(iceberg::Error::new(ErrorKind::Unexpected, "response lost"));
        }
        Ok(table)
    }

    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }
}

async fn setup() -> (MemoryCatalog, Table, IcebergSinkMetrics) {
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "commit-test",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), "/warehouse".into())]),
        )
        .await
        .unwrap();
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let desc = mz_repr::RelationDesc::builder()
        .with_column("id", SqlScalarType::Int32.nullable(false))
        .finish();
    let (_, schema) = relation_desc_to_iceberg_schema(&desc).unwrap();
    let table = load_or_create_table(&catalog, "test".into(), "sink".into(), &schema)
        .await
        .unwrap();
    let metrics = IcebergSinkMetrics::new(
        &IcebergSinkMetricDefs::register_with(&MetricsRegistry::new()),
        GlobalId::User(1),
        0,
    );

    (catalog, table, metrics)
}

#[mz_ore::test(tokio::test)]
async fn overlapping_batch_retry_does_not_duplicate_records() {
    let (catalog, table, metrics) = setup().await;
    // No input before 10. Both writers start from this same durable upper.
    let ten = Antichain::from_elem(Timestamp::new(10));
    let (table, seeded) = try_commit_batch(
        table,
        properties(10),
        vec![],
        vec![],
        &catalog,
        "test",
        "sink",
        1,
        &Antichain::from_elem(Timestamp::new(0)),
        &metrics,
    )
    .await;
    assert!(matches!(seeded, RetryResult::Ok(())), "{seeded:?}");

    let larger = data_file(&table, "10-20", 10);
    let smaller = data_file(&table, "10-15", 5);
    let ident = table.identifier().clone();
    let catalog = CompetingCatalog {
        inner: catalog,
        winner: Mutex::new(Some((table.clone(), smaller, properties(15)))),
        lose_response: Mutex::new(false),
        fail_reload: Mutex::new(false),
    };

    // Match the caller's retry contract: only Table is refreshed, while file
    // descriptors and batch bounds are retained across attempts.
    let (_, result) = Retry::default()
        .max_tries(5)
        .retry_async_with_state(table, |_, table| {
            try_commit_batch(
                table,
                properties(20),
                vec![larger.clone()],
                vec![],
                &catalog,
                "test",
                "sink",
                1,
                &ten,
                &metrics,
            )
        })
        .await;
    assert!(
        catalog.winner.lock().unwrap().is_none(),
        "competing commit must reach the boundary"
    );

    let table = catalog.load_table(&ident).await.unwrap();
    let snapshot = table.metadata().current_snapshot().unwrap();
    let records: u64 = snapshot.summary().additional_properties["total-records"]
        .parse()
        .unwrap();
    let (upper, version) = retrieve_upper_from_snapshots(table.metadata())
        .unwrap()
        .unwrap();
    assert_eq!(version, 1);
    assert!(result.is_err(), "overlap must stop prepared files");
    assert_eq!(upper, Antichain::from_elem(Timestamp::new(15)));
    assert_eq!(records, 5);

    // Model re-rendering: read the same durable progress as mint_batch_descriptions,
    // then write NEW files from replayed input at/after that upper. No suffix of
    // the old file is republished. This harness does not run the storage restart.
    let replay = 10..20;
    let remaining = replay
        .filter(|t| upper.less_equal(&Timestamp::new(*t)))
        .count();
    let rebuilt = data_file(&table, "reconstructed-15-20", remaining.try_into().unwrap());
    let (table, result) = try_commit_batch(
        table,
        properties(20),
        vec![rebuilt],
        vec![],
        &catalog,
        "test",
        "sink",
        1,
        &upper,
        &metrics,
    )
    .await;
    assert!(matches!(result, RetryResult::Ok(())), "{result:?}");
    assert_progress(&table, 20, 1, 10);
}

fn assert_progress(table: &Table, upper: u64, version: u64, records: u64) {
    assert_eq!(
        retrieve_upper_from_snapshots(table.metadata()).unwrap(),
        Some((Antichain::from_elem(Timestamp::new(upper)), version))
    );
    assert_eq!(
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties["total-records"]
            .parse::<u64>()
            .unwrap(),
        records
    );
}

#[mz_ore::test(tokio::test)]
async fn initial_snapshot_nonzero_as_of_is_cas_protected() {
    let (catalog, table, metrics) = setup().await;
    let initial = data_file(&table, "initial", 5);
    let catalog = CompetingCatalog {
        winner: Mutex::new(Some((table.clone(), initial.clone(), properties(15)))),
        inner: catalog,
        lose_response: Mutex::new(false),
        fail_reload: Mutex::new(false),
    };
    let (table, result) = try_commit_batch(
        table,
        properties(15),
        vec![initial],
        vec![],
        &catalog,
        "test",
        "sink",
        1,
        &Antichain::from_elem(Timestamp::new(14)),
        &metrics,
    )
    .await;
    assert!(matches!(result, RetryResult::FatalErr(_)), "{result:?}");
    assert_progress(&table, 15, 1, 5);

    // Metadata-only changes do not prevent initialization at a nonzero as_of.
    let (catalog, table, metrics) = setup().await;
    assert_eq!(
        retrieve_upper_from_snapshots(table.metadata()).unwrap(),
        None
    );
    let table = catalog
        .update_table(
            TableCommit::builder()
                .ident(table.identifier().clone())
                .requirements(vec![])
                .updates(vec![TableUpdate::SetProperties {
                    updates: HashMap::from([("test-property".into(), "value".into())]),
                }])
                .build(),
        )
        .await
        .unwrap();
    assert!(!table.metadata().metadata_log().is_empty());
    assert_eq!(
        retrieve_upper_from_snapshots(table.metadata()).unwrap(),
        None
    );
    let initial = data_file(&table, "initial", 5);
    let (table, result) = try_commit_batch(
        table,
        properties(15),
        vec![initial],
        vec![],
        &catalog,
        "test",
        "sink",
        1,
        &Antichain::from_elem(Timestamp::new(14)),
        &metrics,
    )
    .await;
    assert!(matches!(result, RetryResult::Ok(())), "{result:?}");
    assert_progress(&table, 15, 1, 5);
}

#[mz_ore::test(tokio::test)]
async fn expired_materialize_progress_behind_replace_fails_closed() {
    let (catalog, table, metrics) = setup().await;
    let initial = data_file(&table, "initial", 5);
    let lower = Antichain::from_elem(Timestamp::new(14));
    let (table, result) = try_commit_batch(
        table,
        properties(15),
        vec![initial],
        vec![],
        &catalog,
        "test",
        "sink",
        1,
        &lower,
        &metrics,
    )
    .await;
    assert!(matches!(result, RetryResult::Ok(())), "{result:?}");

    // Publish a no-op replacement of the same real manifests without MZ
    // properties, then expire its progress-bearing parent through the catalog.
    let parent = table.metadata().current_snapshot().unwrap();
    let parent_id = parent.snapshot_id();
    let replacement_id = parent_id ^ 1;
    let mut summary = parent.summary().clone();
    summary.operation = Operation::Replace;
    summary
        .additional_properties
        .retain(|k, _| !k.starts_with("mz-"));
    let replacement = Snapshot::builder()
        .with_snapshot_id(replacement_id)
        .with_parent_snapshot_id(Some(parent_id))
        .with_sequence_number(table.metadata().next_sequence_number())
        .with_timestamp_ms(parent.timestamp_ms())
        .with_manifest_list(parent.manifest_list())
        .with_summary(summary)
        .with_schema_id(table.metadata().current_schema_id())
        .build();
    let table = catalog
        .update_table(
            TableCommit::builder()
                .ident(table.identifier().clone())
                .requirements(vec![])
                .updates(vec![
                    TableUpdate::AddSnapshot {
                        snapshot: replacement,
                    },
                    TableUpdate::SetSnapshotRef {
                        ref_name: "main".into(),
                        reference: SnapshotReference::new(
                            replacement_id,
                            SnapshotRetention::branch(None, None, None),
                        ),
                    },
                ])
                .build(),
        )
        .await
        .unwrap();
    // A replacement alone must not hide retained Materialize progress.
    assert_progress(&table, 15, 1, 5);
    let stale = table.clone();
    let table = catalog
        .update_table(
            TableCommit::builder()
                .ident(table.identifier().clone())
                .requirements(vec![])
                .updates(vec![TableUpdate::RemoveSnapshots {
                    snapshot_ids: vec![parent_id],
                }])
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(table.metadata().snapshots().len(), 1);
    assert_eq!(table.metadata().current_snapshot_id(), Some(replacement_id));
    assert!(!table.metadata().history().is_empty());
    assert!(table.metadata().snapshot_by_id(parent_id).is_none());

    // Recovery cannot reinterpret the retained replacement as initialization.
    let err = retrieve_upper_from_snapshots(table.metadata()).unwrap_err();
    assert!(
        err.to_string().contains("no retained Materialize progress"),
        "{err:#}"
    );

    // A prepared batch that matched the durable upper before expiration must
    // also stop after its transaction refresh, without publishing its files.
    let file = data_file(&stale, "15-20", 5);
    let (_, result) = try_commit_batch(
        stale,
        properties(20),
        vec![file],
        vec![],
        &catalog,
        "test",
        "sink",
        1,
        &Antichain::from_elem(Timestamp::new(15)),
        &metrics,
    )
    .await;
    let RetryResult::FatalErr(err) = result else {
        panic!("missing progress must stop prepared files: {result:?}");
    };
    assert!(
        err.to_string().contains("no retained Materialize progress"),
        "{err:#}"
    );
    let reloaded = catalog.load_table(table.identifier()).await.unwrap();
    assert_eq!(reloaded.metadata(), table.metadata());

    // Even removing all snapshots and their log cannot make a v2 table fresh:
    // its last sequence number still records prior writes.
    let table = catalog
        .update_table(
            TableCommit::builder()
                .ident(table.identifier().clone())
                .requirements(vec![])
                .updates(vec![
                    TableUpdate::RemoveSnapshotRef {
                        ref_name: "main".into(),
                    },
                    TableUpdate::RemoveSnapshots {
                        snapshot_ids: vec![replacement_id],
                    },
                ])
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(table.metadata().snapshots().len(), 0);
    assert!(table.metadata().history().is_empty());
    assert!(table.metadata().last_sequence_number() > 0);
    let err = retrieve_upper_from_snapshots(table.metadata()).unwrap_err();
    assert!(
        err.to_string().contains("no retained Materialize progress"),
        "{err:#}"
    );
}

#[mz_ore::test(tokio::test)]
async fn newer_version_fences_even_at_matching_lower() {
    let (catalog, table, metrics) = setup().await;
    let mut newer = properties(10);
    newer
        .iter_mut()
        .find(|(k, _)| k == "mz-sink-version")
        .unwrap()
        .1 = "2".into();
    let winner = data_file(&table, "newer", 0);
    let catalog = CompetingCatalog {
        winner: Mutex::new(Some((table.clone(), winner, newer))),
        inner: catalog,
        lose_response: Mutex::new(false),
        fail_reload: Mutex::new(false),
    };
    let file = data_file(&table, "fenced", 10);
    let (table, result) = try_commit_batch(
        table,
        properties(20),
        vec![file],
        vec![],
        &catalog,
        "test",
        "sink",
        1,
        &Antichain::from_elem(Timestamp::new(10)),
        &metrics,
    )
    .await;
    assert!(matches!(result, RetryResult::FatalErr(_)), "{result:?}");
    assert_progress(&table, 10, 2, 0);
}

#[mz_ore::test(tokio::test)]
async fn uncertain_commit_and_failed_reload_do_not_republish() {
    let (catalog, table, metrics) = setup().await;
    let file = data_file(&table, "uncertain", 10);
    let catalog = CompetingCatalog {
        winner: Mutex::new(None),
        inner: catalog,
        lose_response: Mutex::new(true),
        fail_reload: Mutex::new(false),
    };
    let (stale, result) = try_commit_batch(
        table,
        properties(20),
        vec![file.clone()],
        vec![],
        &catalog,
        "test",
        "sink",
        1,
        &Antichain::from_elem(Timestamp::new(10)),
        &metrics,
    )
    .await;
    assert!(matches!(result, RetryResult::RetryableErr(_)), "{result:?}");
    assert!(stale.metadata().current_snapshot().is_none());
    let (table, result) = try_commit_batch(
        stale,
        properties(20),
        vec![file],
        vec![],
        &catalog,
        "test",
        "sink",
        1,
        &Antichain::from_elem(Timestamp::new(10)),
        &metrics,
    )
    .await;
    assert!(matches!(result, RetryResult::FatalErr(_)), "{result:?}");
    assert_progress(&table, 20, 1, 10);
}
