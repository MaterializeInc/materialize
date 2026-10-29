// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Row, Timestamp};
use mz_storage_types::sources::SourceData;
use mz_timestamp_oracle::{TimestampOracle, WriteTimestamp};

use super::{Catalog, OpenCatalogResult, TableInfrastructure};
use crate::config::{Config, StateConfig};
use crate::durable::{
    BootstrapArgs, CONTROLLER_INITIALIZATION_COMPLETE_KEY, CatalogTimestampOracle,
    DurableCatalogState, TestCatalogStateBuilder, test_bootstrap_args,
};

// The same oracle serves durable admission, catalog commits and the table WAL.
#[derive(Debug)]
struct TableBootstrapOracle(Mutex<(Timestamp, Timestamp)>);

#[async_trait::async_trait]
impl TimestampOracle<Timestamp> for TableBootstrapOracle {
    async fn write_ts(&self) -> WriteTimestamp {
        let mut state = self.0.lock().expect("oracle lock");
        state.1 = state.0.max(state.1).step_forward();
        WriteTimestamp {
            timestamp: state.1,
            advance_to: state.1.step_forward(),
        }
    }

    async fn peek_write_ts(&self) -> Timestamp {
        self.0.lock().expect("oracle lock").1
    }

    async fn read_ts(&self) -> Timestamp {
        self.0.lock().expect("oracle lock").0
    }

    async fn apply_write(&self, timestamp: Timestamp) {
        let mut state = self.0.lock().expect("oracle lock");
        state.0 = state.0.max(timestamp);
        state.1 = state.1.max(timestamp);
    }
}

#[derive(Debug)]
struct PausedBootstrapOracle<'a> {
    inner: &'a TableBootstrapOracle,
    pause: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

#[async_trait::async_trait]
impl TimestampOracle<Timestamp> for PausedBootstrapOracle<'_> {
    async fn write_ts(&self) -> WriteTimestamp {
        let pause = self.pause.lock().expect("pause lock").take();
        if let Some((started, resume)) = pause {
            started.send(()).expect("initializer reached WAL setup");
            resume.await.expect("resume initializer");
        }
        self.inner.write_ts().await
    }

    async fn peek_write_ts(&self) -> Timestamp {
        self.inner.peek_write_ts().await
    }

    async fn read_ts(&self) -> Timestamp {
        self.inner.read_ts().await
    }

    async fn apply_write(&self, timestamp: Timestamp) {
        self.inner.apply_write(timestamp).await;
    }
}

struct Fixture {
    persist: PersistClient,
    organization: uuid::Uuid,
    oracle: Arc<TableBootstrapOracle>,
    state: StateConfig,
}

impl Fixture {
    async fn new() -> Self {
        let metrics = MetricsRegistry::new();
        let mut config = mz_persist_client::cfg::PersistConfig::new_for_tests();
        config.configs = Arc::new(mz_txn_wal::all_dyncfgs((*config.configs).clone()));
        let persist =
            mz_persist_client::cache::PersistClientCache::new(config, &metrics, |_, _| {
                mz_persist_client::rpc::PubSubClientConnection::noop()
            })
            .open(mz_persist_types::PersistLocation::new_in_mem())
            .await
            .expect("in-memory Persist");
        let bootstrap = test_bootstrap_args();
        // Only borrow debug configuration. The tested catalog has its own durable
        // identity and runs normal migrations rather than debug initialization.
        let template_persist = PersistClient::new_for_tests().await;
        let template_organization = uuid::Uuid::new_v4();
        let template_storage = TestCatalogStateBuilder::new(template_persist.clone())
            .with_organization_id(template_organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
            .await
            .expect("template storage");
        let template = Catalog::open_debug_catalog_inner(
            template_persist,
            template_storage,
            mz_ore::now::SYSTEM_TIME.clone(),
            Some(
                format!("local-az1-{template_organization}-0")
                    .parse()
                    .expect("template environment ID"),
            ),
            &mz_build_info::DUMMY_BUILD_INFO,
            [("enable_catalog_read_protection".into(), "true".into())].into(),
            &bootstrap,
            None,
            None,
        )
        .await
        .expect("configuration template");
        let mut state = Catalog::diagnostic_state_config(&template.diagnostic_config);
        template.expire().await;
        let organization = uuid::Uuid::new_v4();
        state.environment_id = format!("local-az1-{organization}-0")
            .parse()
            .expect("environment ID");
        state.persist_client = persist.clone();
        state.builtin_item_migration_config.persist_client = persist.clone();
        state.skip_migrations = false;
        let now = mz_ore::now::SYSTEM_TIME().into();
        Self {
            persist,
            organization,
            oracle: Arc::new(TableBootstrapOracle(Mutex::new((now, now)))),
            state,
        }
    }

    fn builder(&self) -> TestCatalogStateBuilder {
        TestCatalogStateBuilder::new(self.persist.clone())
            .with_organization_id(self.organization)
            .with_default_deploy_generation()
            .with_timestamp_oracle(CatalogTimestampOracle::new(
                Arc::<TableBootstrapOracle>::clone(&self.oracle),
                mz_ore::now::SYSTEM_TIME.clone(),
            ))
    }

    fn bootstrap(controller_initialization: bool) -> BootstrapArgs {
        BootstrapArgs {
            controller_initialization,
            ..test_bootstrap_args()
        }
    }

    async fn durable(&self, controller_initialization: bool) -> Box<dyn DurableCatalogState> {
        self.builder()
            .unwrap_build()
            .await
            .open(
                self.oracle.write_ts().await.timestamp,
                &Self::bootstrap(controller_initialization),
            )
            .await
            .expect("durable initialization admission")
    }

    // Each simulated process and inspection handle owns a fresh metrics registry.
    async fn open(&self, storage: Box<dyn DurableCatalogState>) -> OpenCatalogResult {
        let mut state = Catalog::diagnostic_state_config(&self.state);
        state.skip_migrations = false;
        state.boot_ts = self.oracle.write_ts().await.timestamp;
        Catalog::open(Config {
            storage,
            metrics_registry: &MetricsRegistry::new(),
            state,
        })
        .await
        .expect("normal Catalog::open")
    }

    async fn initialize(&self, opened: OpenCatalogResult) -> Catalog {
        opened
            .complete_fresh_deployment_initialization(
                self.persist.clone(),
                &MetricsRegistry::new(),
                self.oracle.as_ref(),
            )
            .await
            .expect("complete pending initialization")
    }

    async fn infrastructure(&self, catalog: &mut Catalog) -> TableInfrastructure {
        catalog
            .complete_table_infrastructure(
                self.persist.clone(),
                Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new())),
                self.oracle.as_ref(),
            )
            .await
            .expect("register table infrastructure")
    }

    async fn append_type_row(&self, catalog: &mut Catalog, row: Row) {
        let mut infrastructure = self.infrastructure(catalog).await;
        let id = catalog.resolve_builtin_table(&crate::builtin::MZ_TYPES);
        let table = catalog.get_entry(&id).table().expect("mz_types table");
        let shard = infrastructure.routes[&table.global_id_writes()];
        let write = self.oracle.write_ts().await;
        catalog
            .advance_upper(write.timestamp.step_forward())
            .await
            .expect("catalog fence");
        let mut txn = infrastructure.txns.begin();
        txn.write(&shard, SourceData(Ok(row)), (), 1).await;
        let apply = txn
            .commit_at(&mut infrastructure.txns, write.timestamp)
            .await
            .expect("inject drift");
        let _ = apply.apply(&mut infrastructure.txns).await;
        self.oracle.apply_write(write.timestamp).await;
        drop(infrastructure);
    }

    async fn assert_provenance(&self, catalog: &Catalog, marker: Option<u64>) {
        let snapshot = catalog.storage().await.snapshot().await.expect("snapshot");
        let config = |key: &str| {
            snapshot
                .configs
                .get(&crate::durable::objects::ConfigKey { key: key.into() })
                .map(|value| value.value)
        };
        assert_eq!(config("catalog_read_protection_enabled"), Some(1));
        assert_eq!(config(CONTROLLER_INITIALIZATION_COMPLETE_KEY), marker);
    }

    async fn types(&self, catalog: &mut Catalog) -> Vec<(Row, i64)> {
        let infrastructure = self.infrastructure(catalog).await;
        let id = catalog.resolve_builtin_table(&crate::builtin::MZ_TYPES);
        let table = catalog.get_entry(&id).table().expect("mz_types table");
        let shard = infrastructure.routes[&table.global_id_writes()];
        let mut reader = self
            .persist
            .open_leased_reader::<SourceData, (), Timestamp, i64>(
                shard,
                Arc::new(table.desc.latest()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
                false,
            )
            .await
            .expect("mz_types reader");
        let snapshot = infrastructure
            .txns
            .read_cache()
            .data_snapshot(shard, infrastructure.completed_at)
            .snapshot_and_fetch(&mut reader)
            .await
            .expect("mz_types snapshot");
        reader.expire().await;
        let mut rows: Vec<_> = snapshot
            .into_iter()
            .map(|((data, ()), _, diff)| (data.0.expect("valid mz_types row"), diff))
            .collect();
        differential_dataflow::consolidation::consolidate(&mut rows);
        rows
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Interruption {
    DurableCreation,
    CatalogOpen,
    StorageMetadata,
    WalRegistration,
    SeedCommitted,
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn pending_initialization_recovers_every_durable_boundary() {
    let clean = Fixture::new().await;
    let mut catalog = clean
        .initialize(clean.open(clean.durable(true).await).await)
        .await;
    clean.assert_provenance(&catalog, Some(1)).await;
    let expected = clean.types(&mut catalog).await;
    assert!(
        !expected.is_empty(),
        "clean initialization must seed mz_types"
    );
    assert!(expected.iter().all(|(_, diff)| *diff == 1));
    catalog.expire().await;

    for interruption in [
        Interruption::DurableCreation,
        Interruption::CatalogOpen,
        Interruption::StorageMetadata,
        Interruption::WalRegistration,
        Interruption::SeedCommitted,
    ] {
        let fixture = Fixture::new().await;
        let storage = fixture.durable(true).await;
        if interruption == Interruption::DurableCreation {
            storage.expire().await;
        } else {
            let mut opened = fixture.open(storage).await;
            if interruption >= Interruption::StorageMetadata {
                opened
                    .catalog
                    .initialize_storage_metadata()
                    .await
                    .expect("storage metadata");
            }
            if interruption >= Interruption::WalRegistration {
                let mut infrastructure = fixture.infrastructure(&mut opened.catalog).await;
                if interruption == Interruption::SeedCommitted {
                    opened
                        .catalog
                        .reconcile_initial_builtin_snapshot(
                            &fixture.persist,
                            &mut infrastructure,
                            &opened.builtin_table_updates,
                            fixture.oracle.as_ref(),
                        )
                        .await
                        .expect("commit seed without completion marker");
                }
            }
            fixture.assert_provenance(&opened.catalog, Some(0)).await;
            if interruption == Interruption::SeedCommitted {
                assert_eq!(fixture.types(&mut opened.catalog).await, expected);
            }
            opened.catalog.expire().await;
        }
        let reopened = fixture.open(fixture.durable(true).await).await;
        fixture.assert_provenance(&reopened.catalog, Some(0)).await;
        let mut recovered = fixture.initialize(reopened).await;
        fixture.assert_provenance(&recovered, Some(1)).await;
        let actual = fixture.types(&mut recovered).await;
        assert!(
            actual.iter().all(|(_, diff)| *diff == 1),
            "{interruption:?}"
        );
        assert_eq!(actual, expected, "{interruption:?}");
        recovered.expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn completed_restart_preserves_seed_drift() {
    let fixture = Fixture::new().await;
    let mut catalog = fixture
        .initialize(fixture.open(fixture.durable(true).await).await)
        .await;
    let mut expected = fixture.types(&mut catalog).await;
    let extra = expected.first_mut().expect("seeded mz_types");
    extra.1 += 1;
    let row = extra.0.clone();
    fixture.append_type_row(&mut catalog, row).await;
    assert_eq!(fixture.types(&mut catalog).await, expected);
    catalog.expire().await;

    let storage = fixture
        .builder()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("completed join");
    let opened = fixture.open(storage).await;
    let mut restarted = opened
        .complete_deployment_restart(
            fixture.persist.clone(),
            &MetricsRegistry::new(),
            fixture.oracle.as_ref(),
        )
        .await
        .expect("ordinary controller restart");
    fixture.assert_provenance(&restarted, Some(1)).await;
    assert_eq!(
        fixture.types(&mut restarted).await,
        expected,
        "restart must not repair seed projections"
    );
    restarted.expire().await;

    // A non-join handle must also be rejected by the initializer itself, not
    // merely by join admission or the initializer's join-handle guard.
    let opened = fixture.open(fixture.durable(false).await).await;
    assert!(
        opened
            .complete_fresh_deployment_initialization(
                fixture.persist.clone(),
                &MetricsRegistry::new(),
                fixture.oracle.as_ref(),
            )
            .await
            .is_err()
    );
    let storage = fixture
        .builder()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join after refusal");
    let mut observed = fixture.open(storage).await.catalog;
    assert_eq!(
        fixture.types(&mut observed).await,
        expected,
        "rejected initialization must not rewrite completed seed tables"
    );
    observed.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn delayed_initializer_preserves_completed_seed_changes() {
    let fixture = Fixture::new().await;
    let first = fixture.open(fixture.durable(true).await).await;
    let delayed = fixture.open(fixture.durable(true).await).await;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let oracle = PausedBootstrapOracle {
        inner: fixture.oracle.as_ref(),
        pause: Mutex::new(Some((started_tx, resume_rx))),
    };
    let metrics = MetricsRegistry::new();
    let delayed = delayed.complete_fresh_deployment_initialization(
        fixture.persist.clone(),
        &metrics,
        &oracle,
    );
    let peer = async {
        // The delayed initializer passed pending admission, but has not fenced
        // or observed the table WAL. A peer completes and starts publishing.
        started_rx
            .await
            .expect("delayed initializer passed admission");
        let mut active = fixture.initialize(first).await;
        fixture.assert_provenance(&active, Some(1)).await;
        let mut expected = fixture.types(&mut active).await;
        let extra = expected.first_mut().expect("seeded mz_types");
        extra.1 += 1;
        fixture.append_type_row(&mut active, extra.0.clone()).await;
        resume_tx.send(()).expect("resume delayed initializer");
        (active, expected)
    };
    let (result, (mut active, expected)) =
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            tokio::join!(delayed, peer)
        })
        .await
        .expect("overlapping initialization makes progress");
    assert!(
        result.is_err(),
        "completed initialization cannot be reclaimed"
    );
    assert_eq!(
        fixture.types(&mut active).await,
        expected,
        "a delayed initializer must not reconcile away completed-deployment writes"
    );
    active.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn unmarked_catalog_cannot_initialize() {
    let fixture = Fixture::new().await;
    let opened = fixture.open(fixture.durable(false).await).await;
    fixture.assert_provenance(&opened.catalog, None).await;
    let before = opened
        .catalog
        .storage()
        .await
        .snapshot()
        .await
        .expect("before refusal");
    assert!(
        opened
            .complete_fresh_deployment_initialization(
                fixture.persist.clone(),
                &MetricsRegistry::new(),
                fixture.oracle.as_ref(),
            )
            .await
            .is_err()
    );
    let mut storage = fixture
        .builder()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join after refusal");
    let after = storage.snapshot().await.expect("after refusal");
    assert_eq!(
        after.storage_collection_metadata, before.storage_collection_metadata,
        "rejected initialization must not mint storage mappings"
    );
    storage.expire().await;
}
