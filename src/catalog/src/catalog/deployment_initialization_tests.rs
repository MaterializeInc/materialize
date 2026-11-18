// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

struct OvertakingBootstrapOracle {
    inner: Arc<TableBootstrapOracle>,
    peer: tokio::sync::Mutex<(Catalog, TableInfrastructure)>,
    enabled: AtomicBool,
    allocations: AtomicUsize,
    completions: AtomicUsize,
}

impl std::fmt::Debug for OvertakingBootstrapOracle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OvertakingBootstrapOracle")
            .finish_non_exhaustive()
    }
}

impl OvertakingBootstrapOracle {
    async fn advance_peer(&self) {
        let peer_write = self.inner.write_ts().await;
        let mut peer = self.peer.lock().await;
        let (catalog, infrastructure) = &mut *peer;
        catalog
            .advance_upper(peer_write.timestamp.step_forward())
            .await
            .expect("peer catalog fence");
        let mut txn = infrastructure.txns.begin();
        let apply = txn
            .commit_at(&mut infrastructure.txns, peer_write.timestamp)
            .await
            .expect("peer WAL advancement");
        let tidy = apply.apply(&mut infrastructure.txns).await;
        infrastructure.tidy.merge(tidy);
        self.inner.apply_write(peer_write.timestamp).await;
    }
}

#[async_trait::async_trait]
impl TimestampOracle<Timestamp> for OvertakingBootstrapOracle {
    async fn write_ts(&self) -> WriteTimestamp {
        let write = self.inner.write_ts().await;
        if self.enabled.load(Ordering::SeqCst) {
            self.advance_peer().await;
        }
        // Observers may stop interference only after this allocation's peer
        // advancement completed, so a requested rejected round is deterministic.
        self.allocations.fetch_add(1, Ordering::SeqCst);
        write
    }

    async fn peek_write_ts(&self) -> Timestamp {
        self.inner.peek_write_ts().await
    }

    async fn read_ts(&self) -> Timestamp {
        self.inner.read_ts().await
    }

    async fn apply_write(&self, timestamp: Timestamp) {
        self.completions.fetch_add(1, Ordering::SeqCst);
        self.inner.apply_write(timestamp).await;
    }
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
                Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new())),
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

    async fn create_table(
        &self,
        catalog: &mut Catalog,
    ) -> (mz_repr::CatalogItemId, mz_repr::GlobalId) {
        use mz_sql::names::{ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier};

        let database = catalog.resolve_database("materialize").expect("database");
        let database_spec = ResolvedDatabaseSpecifier::Id(database.id);
        let schema = catalog
            .resolve_schema_in_database(&database_spec, "public", &crate::SYSTEM_CONN_ID)
            .expect("schema");
        let name = QualifiedItemName {
            qualifiers: ItemQualifiers {
                database_spec,
                schema_spec: schema.id.clone(),
            },
            item: "registration_race".into(),
        };
        let (id, gid) = catalog
            .allocate_user_id_for_test()
            .await
            .expect("table IDs");
        let item = super::test_support::parse_item(
            &mut catalog.state,
            gid,
            "CREATE TABLE materialize.public.registration_race (a int)",
            &Default::default(),
        )
        .expect("table definition");
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                None,
                ts,
                None,
                vec![super::Op::CreateItem {
                    id,
                    name,
                    item,
                    owner_id: mz_sql::session::user::MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("commit table");
        catalog
            .initialize_storage_metadata()
            .await
            .expect("table binding");
        (id, gid)
    }

    async fn append_type_row(&self, catalog: &mut Catalog, row: Row) {
        self.append_row(catalog, &crate::builtin::MZ_TYPES, row)
            .await;
    }

    async fn append_row(
        &self,
        catalog: &mut Catalog,
        builtin: &'static crate::builtin::BuiltinTable,
        row: Row,
    ) {
        let mut infrastructure = self.infrastructure(catalog).await;
        let id = catalog.resolve_builtin_table(builtin);
        let table = catalog.get_entry(&id).table().expect("builtin table");
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
        self.rows(catalog, &crate::builtin::MZ_TYPES).await
    }

    async fn rows(
        &self,
        catalog: &mut Catalog,
        builtin: &'static crate::builtin::BuiltinTable,
    ) -> Vec<(Row, i64)> {
        let infrastructure = self.infrastructure(catalog).await;
        let id = catalog.resolve_builtin_table(builtin);
        let table = catalog.get_entry(&id).table().expect("builtin table table");
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
            .expect("builtin table reader");
        let snapshot = infrastructure
            .txns
            .read_cache()
            .data_snapshot(shard, infrastructure.completed_at)
            .snapshot_and_fetch(&mut reader)
            .await
            .expect("builtin table snapshot");
        reader.expire().await;
        let mut rows: Vec<_> = snapshot
            .into_iter()
            .map(|((data, ()), _, diff)| (data.0.expect("valid builtin table row"), diff))
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
        .admit_deployment_restart()
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
                Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new())),
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
        Arc::new(mz_txn_wal::metrics::Metrics::new(&metrics)),
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
                Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new())),
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

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn delayed_configuration_reconciliation_respects_promotion() {
    let fixture = Fixture::new().await;
    let initial = fixture
        .initialize(fixture.open(fixture.durable(true).await).await)
        .await;
    initial.expire().await;
    let storage = fixture
        .builder()
        .unwrap_build()
        .await
        .admit()
        .await
        .expect("restart admission");
    let mut opened = fixture.open(storage).await;
    let mut infrastructure = fixture.infrastructure(&mut opened.catalog).await;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let oracle = PausedBootstrapOracle {
        inner: fixture.oracle.as_ref(),
        pause: Mutex::new(Some((started_tx, resume_rx))),
    };
    let reconcile = opened.catalog.reconcile_deployment_configuration(
        &fixture.persist,
        &mut infrastructure,
        &opened.builtin_table_updates,
        &oracle,
    );
    let peer = async {
        started_rx
            .await
            .expect("reconciliation passed its entry check");
        let storage = fixture
            .builder()
            .with_deploy_generation(1)
            .unwrap_build()
            .await
            .open_for_promotion(
                fixture.oracle.write_ts().await.timestamp,
                &Fixture::bootstrap(false),
            )
            .await
            .expect("promote the peer deployment");
        let mut active = fixture.open(storage).await.catalog;
        let table = &crate::builtin::MZ_CLUSTER_REPLICA_SIZES;
        let mut expected = fixture.rows(&mut active, table).await;
        let extra = expected.first_mut().expect("seeded sizes");
        extra.1 += 1;
        fixture
            .append_row(&mut active, table, extra.0.clone())
            .await;
        resume_tx.send(()).expect("resume old controller");
        (active, expected)
    };
    let (result, (mut active, expected)) =
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            tokio::join!(reconcile, peer)
        })
        .await
        .expect("promotion and reconciliation make progress");
    result.expect("old controller remains available after losing configuration authority");
    assert_eq!(
        fixture
            .rows(&mut active, &crate::builtin::MZ_CLUSTER_REPLICA_SIZES)
            .await,
        expected,
        "old controller must preserve configuration published after promotion"
    );
    drop(infrastructure);
    opened.catalog.expire().await;
    active.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn table_registration_retains_preparation_across_peer_progress() {
    let fixture = Fixture::new().await;
    let mut catalog = fixture
        .initialize(fixture.open(fixture.durable(true).await).await)
        .await;
    let storage = fixture
        .builder()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("peer join");
    let mut peer = fixture.open(storage).await.catalog;
    let infrastructure = fixture.infrastructure(&mut peer).await;
    let (_, gid) = fixture.create_table(&mut catalog).await;
    let shard = catalog
        .state()
        .storage_metadata()
        .get_collection_shard(gid)
        .expect("table shard");
    let oracle = OvertakingBootstrapOracle {
        inner: Arc::clone(&fixture.oracle),
        peer: tokio::sync::Mutex::new((peer, infrastructure)),
        enabled: AtomicBool::new(true),
        allocations: AtomicUsize::new(0),
        completions: AtomicUsize::new(0),
    };
    let metrics = Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new()));
    let mut prepared = catalog
        .prepare_table_infrastructure(fixture.persist.clone(), metrics, None)
        .await
        .expect("retained preparation");
    for after_validation in [false, true] {
        oracle.enabled.store(!after_validation, Ordering::SeqCst);
        oracle.allocations.store(0, Ordering::SeqCst);
        oracle.completions.store(0, Ordering::SeqCst);
        let (hook, mut attempts) = tokio::sync::mpsc::unbounded_channel();
        if after_validation {
            catalog.before_table_registration = Some(hook);
        }
        let completion = catalog.try_complete_table_infrastructure(&mut prepared, &oracle);
        let interference = async {
            if after_validation {
                let resume = attempts.recv().await.expect("validated registration");
                oracle.advance_peer().await;
                resume.send(()).expect("resume registration CAS");
            }
        };
        let (completed, ()) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            tokio::join!(completion, interference)
        })
        .await
        .expect("bounded round");
        assert!(completed.expect("ordinary contention").is_none());
        assert_eq!(
            oracle.allocations.load(Ordering::SeqCst),
            if after_validation { 1 } else { 3 }
        );
        assert_eq!(oracle.completions.load(Ordering::SeqCst), 0);
        let (_, registered) = oracle
            .peer
            .lock()
            .await
            .1
            .txns
            .registration_status([shard])
            .await;
        assert!(
            registered.is_empty(),
            "rejected registration cannot admit the table"
        );
    }
    oracle.enabled.store(false, Ordering::SeqCst);
    let completed = catalog
        .try_complete_table_infrastructure(&mut prepared, &oracle)
        .await
        .expect("retry")
        .expect("registration completes after contention");
    let mut infrastructure = prepared.finish(completed);
    assert_eq!(infrastructure.routes[&gid], shard);
    let (_, registered) = infrastructure.txns.registration_status([shard]).await;
    assert!(registered.contains(&shard));
    drop(infrastructure);
    let (peer, infrastructure) = oracle.peer.into_inner();
    drop(infrastructure);
    peer.expire().await;
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn startup_table_registration_recovers_after_peer_contention() {
    let fixture = Fixture::new().await;
    let mut catalog = fixture
        .initialize(fixture.open(fixture.durable(true).await).await)
        .await;
    let storage = fixture
        .builder()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("peer join");
    let mut peer = fixture.open(storage).await.catalog;
    let infrastructure = fixture.infrastructure(&mut peer).await;
    let (_, gid) = fixture.create_table(&mut catalog).await;
    let shard = catalog
        .state()
        .storage_metadata()
        .get_collection_shard(gid)
        .expect("table shard");
    let oracle = OvertakingBootstrapOracle {
        inner: Arc::clone(&fixture.oracle),
        peer: tokio::sync::Mutex::new((peer, infrastructure)),
        enabled: AtomicBool::new(true),
        allocations: AtomicUsize::new(0),
        completions: AtomicUsize::new(0),
    };
    let registration = catalog.complete_table_infrastructure(
        fixture.persist.clone(),
        Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new())),
        &oracle,
    );
    let peer_progress = async {
        // Exhaust a complete bounded round through real catalog and WAL writes.
        // Startup must retain the operation instead of surfacing contention.
        while oracle.allocations.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert_eq!(oracle.completions.load(Ordering::SeqCst), 0);
        oracle.enabled.store(false, Ordering::SeqCst);
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        tokio::join!(registration, peer_progress)
    })
    .await
    .expect("startup resumes after peer contention");
    let mut infrastructure = result.expect("ordinary contention does not fail startup");
    assert!(oracle.allocations.load(Ordering::SeqCst) >= 4);
    assert_eq!(oracle.completions.load(Ordering::SeqCst), 1);
    assert_eq!(infrastructure.routes[&gid], shard);
    let (_, registered) = infrastructure.txns.registration_status([shard]).await;
    assert!(registered.contains(&shard));
    drop(infrastructure);
    let (peer, infrastructure) = oracle.peer.into_inner();
    drop(infrastructure);
    peer.expire().await;
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn prepared_table_registration_does_not_resurrect_forgotten_shard() {
    for already_registered in [false, true] {
        let fixture = Fixture::new().await;
        let mut peer = fixture
            .initialize(fixture.open(fixture.durable(true).await).await)
            .await;
        let mut infrastructure = fixture.infrastructure(&mut peer).await;
        let (id, gid) = fixture.create_table(&mut peer).await;
        if already_registered {
            infrastructure = fixture.infrastructure(&mut peer).await;
        }
        let shard = peer
            .state()
            .storage_metadata()
            .get_collection_shard(gid)
            .expect("table shard");
        let storage = fixture
            .builder()
            .unwrap_build()
            .await
            .join()
            .await
            .expect("join");
        let mut joining = fixture.open(storage).await.catalog;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        let oracle = PausedBootstrapOracle {
            inner: fixture.oracle.as_ref(),
            pause: Mutex::new(Some((started_tx, resume_rx))),
        };
        let mut prepared = joining
            .prepare_table_infrastructure(
                fixture.persist.clone(),
                Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new())),
                None,
            )
            .await
            .expect("retained preparation");
        let (hook, mut attempts) = tokio::sync::mpsc::unbounded_channel();
        if already_registered {
            joining.before_table_registration = Some(hook);
        }
        let registration = joining.try_complete_table_infrastructure(&mut prepared, &oracle);
        let retirement = async {
            let resume = if already_registered {
                attempts
                    .recv()
                    .await
                    .expect("WAL membership observed before catalog validation")
            } else {
                started_rx
                    .await
                    .expect("handles prepared before allocation");
                resume_tx
            };
            let ts = peer.current_upper().await;
            peer.transact(
                None,
                None,
                ts,
                None,
                vec![super::Op::DropObjects(vec![super::DropObjectInfo::Item(
                    id,
                )])],
            )
            .await
            .expect("retire table in catalog");
            assert!(
                peer.storage()
                    .await
                    .snapshot()
                    .await
                    .expect("retirement snapshot")
                    .unfinalized_shards
                    .keys()
                    .any(|key| key.shard == shard)
            );
            let write = fixture.oracle.write_ts().await;
            peer.advance_upper(write.timestamp.step_forward())
                .await
                .expect("Forget fence");
            infrastructure
                .txns
                .forget(write.timestamp, [shard])
                .await
                .expect("Forget retired table");
            fixture.oracle.apply_write(write.timestamp).await;
            resume.send(()).expect("resume registration");
        };
        let (registration, ()) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            tokio::join!(registration, retirement)
        })
        .await
        .expect("registration observes retirement");
        assert!(registration.expect("defer stale preparation").is_none());
        let cut = joining
            .try_complete_table_infrastructure(&mut prepared, fixture.oracle.as_ref())
            .await
            .expect("refresh retired bindings")
            .expect("remaining tables are registered");
        let completed = prepared.finish(cut);
        assert!(!completed.routes.contains_key(&gid));
        assert!(
            !completed
                .txns
                .read_cache()
                .registered_at_progress(&shard, &completed.completed_at.step_forward())
        );
        drop(completed);
        drop(infrastructure);
        joining.expire().await;
        peer.expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn table_schema_preparation_precedes_registration_timestamp() {
    let fixture = Fixture::new().await;
    let mut catalog = fixture
        .initialize(fixture.open(fixture.durable(true).await).await)
        .await;
    let (id, gid) = fixture.create_table(&mut catalog).await;
    let shard = catalog
        .state()
        .storage_metadata()
        .get_collection_shard(gid)
        .expect("unregistered table shard");
    let (_, new_global_id) = catalog
        .allocate_user_id_for_test()
        .await
        .expect("version ID");
    let ts = catalog.current_upper().await;
    catalog
        .transact(
            None,
            None,
            ts,
            None,
            vec![super::Op::AlterAddColumn {
                id,
                new_global_id,
                name: "b".into(),
                typ: mz_repr::SqlScalarType::Int64.nullable(true),
                sql: mz_sql_parser::parser::parse_data_type("bigint").expect("column type"),
            }],
        )
        .await
        .expect("commit schema evolution without applying it");
    catalog
        .initialize_storage_metadata()
        .await
        .expect("version binding");
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let oracle = PausedBootstrapOracle {
        inner: fixture.oracle.as_ref(),
        pause: Mutex::new(Some((started_tx, resume_rx))),
    };
    let registration = catalog.complete_table_infrastructure(
        fixture.persist.clone(),
        Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new())),
        &oracle,
    );
    let inspect = async {
        started_rx.await.expect("registration timestamp requested");
        let (_, schema, _) = fixture
            .persist
            .latest_schema::<SourceData, (), Timestamp, i64>(shard, Diagnostics::for_tests())
            .await
            .expect("read schema")
            .expect("registered schema");
        assert_eq!(
            schema.arity(),
            2,
            "schema preparation must precede timestamp allocation"
        );
        resume_tx.send(()).expect("resume registration");
    };
    let (registration, ()) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        tokio::join!(registration, inspect)
    })
    .await
    .expect("prepared schema registers");
    let infrastructure = registration.expect("registration");
    assert_eq!(infrastructure.routes[&new_global_id], shard);
    drop(infrastructure);
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn restart_admission_defers_configuration_to_bounded_activation() {
    let fixture = Fixture::new().await;
    let mut catalog = fixture
        .initialize(fixture.open(fixture.durable(true).await).await)
        .await;
    let builtin = &crate::builtin::MZ_CLUSTER_REPLICA_SIZES;
    let expected = fixture.rows(&mut catalog, builtin).await;
    fixture
        .append_row(
            &mut catalog,
            builtin,
            expected.first().expect("sizes").0.clone(),
        )
        .await;
    let drifted = fixture.rows(&mut catalog, builtin).await;
    assert_ne!(drifted, expected);
    catalog.expire().await;
    let restarted = fixture
        .builder()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("controller restart join");
    let mut catalog = fixture
        .open(restarted)
        .await
        .admit_deployment_restart()
        .await
        .expect("restart admission does not publish configuration");
    assert_eq!(fixture.rows(&mut catalog, builtin).await, drifted);
    let storage = fixture
        .builder()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("peer join");
    let mut peer = fixture.open(storage).await.catalog;
    let infrastructure = fixture.infrastructure(&mut peer).await;
    let oracle = OvertakingBootstrapOracle {
        inner: Arc::clone(&fixture.oracle),
        peer: tokio::sync::Mutex::new((peer, infrastructure)),
        enabled: AtomicBool::new(true),
        allocations: AtomicUsize::new(0),
        completions: AtomicUsize::new(0),
    };
    let mut updates = mz_dyncfg::ConfigUpdates::default();
    updates.add(&mz_adapter_types::dyncfgs::GROUP_COMMIT_MAX_ATTEMPTS, 1);
    updates.apply(catalog.system_config().dyncfgs());
    let metrics = Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new()));
    let mut prepared = None;
    for _ in 0..2 {
        assert!(
            !catalog
                .complete_deployment_activation(
                    fixture.persist.clone(),
                    Arc::clone(&metrics),
                    &oracle,
                    &mut prepared,
                )
                .await
                .expect("peer contention must not terminate the controller")
        );
    }
    assert_eq!(oracle.allocations.load(Ordering::SeqCst), 2);
    assert_eq!(oracle.completions.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.rows(&mut catalog, builtin).await, drifted);
    oracle.enabled.store(false, Ordering::SeqCst);
    assert!(
        catalog
            .complete_deployment_activation(
                fixture.persist.clone(),
                metrics,
                &oracle,
                &mut prepared,
            )
            .await
            .expect("later activation round")
    );
    assert_eq!(fixture.rows(&mut catalog, builtin).await, expected);
    let (peer, infrastructure) = oracle.peer.into_inner();
    drop(infrastructure);
    peer.expire().await;
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn activation_registers_no_unrelated_tables_and_join_adopts_existing_membership() {
    let fixture = Fixture::new().await;
    let mut catalog = fixture
        .initialize(fixture.open(fixture.durable(true).await).await)
        .await;
    let builtin = &crate::builtin::MZ_CLUSTER_REPLICA_SIZES;
    let expected = fixture.rows(&mut catalog, builtin).await;
    fixture
        .append_row(
            &mut catalog,
            builtin,
            expected.first().expect("sizes").0.clone(),
        )
        .await;
    let mut observer = fixture.infrastructure(&mut catalog).await;
    let (before, _) = observer.txns.registration_status([]).await;
    let before_allocation = fixture.oracle.peek_write_ts().await;
    let adopted = fixture.infrastructure(&mut catalog).await;
    let (after, _) = observer.txns.registration_status([]).await;
    assert_eq!(before, after, "adoption must not advance the WAL");
    assert_eq!(
        before_allocation,
        fixture.oracle.peek_write_ts().await,
        "adoption needs no timestamp allocation"
    );
    drop(adopted);
    let (_, gid) = fixture.create_table(&mut catalog).await;
    let shard = catalog
        .state()
        .storage_metadata()
        .get_collection_shard(gid)
        .expect("table shard");
    let mut prepared = None;
    let metrics = Arc::new(mz_txn_wal::metrics::Metrics::new(&MetricsRegistry::new()));
    assert!(
        catalog
            .complete_deployment_activation(
                fixture.persist.clone(),
                metrics,
                fixture.oracle.as_ref(),
                &mut prepared
            )
            .await
            .expect("configuration activation")
    );
    let (_, registered) = observer.txns.registration_status([shard]).await;
    assert!(
        registered.is_empty(),
        "activation must not register an unrelated user table"
    );
    assert_eq!(fixture.rows(&mut catalog, builtin).await, expected);
    drop(observer);
    catalog.expire().await;
}
