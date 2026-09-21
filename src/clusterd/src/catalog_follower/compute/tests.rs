// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Bounded native follower coverage, not full adapter-loss acceptance.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use mz_catalog::catalog::{Catalog, CatalogError, Op, test_support};
use mz_catalog::durable::{DurableCatalogError, TestCatalogStateBuilder};
use mz_catalog::expr_cache::{ExpressionCacheHandle, GlobalExpressions, expression_build_version};
use mz_catalog::memory::error::ErrorKind;
use mz_cluster_client::client::TimelyConfig;
use mz_compute::server::{ComputeInstanceContext, ComputeRuntimeRole};
use mz_compute_client::protocol::command::{ComputeCommand, Peek, PeekTarget};
use mz_compute_client::protocol::response::{ComputeResponse, PeekResponse};
use mz_compute_client::service::ComputeClient;
use mz_compute_types::dataflows::{DataflowDescription, IndexDesc};
use mz_compute_types::plan::LirRelationExpr;
use mz_dyncfg::{ConfigUpdates, ConfigVal};
use mz_expr::{MapFilterProject, MirScalarExpr, RowSetFinishing, SafeMfpPlan};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_proto::RustType;
use mz_repr::{Datum, GlobalId, RelationDesc, RelationVersion, Row, Timestamp};
use mz_service::client::GenericClient;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::load_generator::LoadGeneratorOutput;
use mz_storage_types::sources::{SourceData, SourceExportStatementDetails};
use mz_txn_wal::operator::TxnsContext;
use prost::Message;
use timely::progress::Antichain;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;
use uuid::Uuid;

use crate::catalog_follower as follower;
use follower::tests::{debug_catalog, name, transact};

#[mz_ore::test(tokio::test)]
async fn written_index_survives_writer_and_query_disconnect() {
    const CHILD: &str = "MZ_CLUSTERD_NATIVE_INDEX_TEST_CHILD";
    const TEST: &str =
        "catalog_follower::compute::tests::written_index_survives_writer_and_query_disconnect";

    if !in_child(CHILD, TEST, Duration::from_secs(120)).await {
        return;
    }

    let Fixture {
        clients,
        persist,
        mut writer,
        mut observer,
        store,
        config,
        source,
        index,
        desc,
        shard,
        mut input,
    } = Fixture::new((1, 15_000), 20_000, false).await;
    let ts = writer.current_upper().await;
    let reader = writer
        .transact(None, ts, None, vec![Op::CreateClientIncarnation])
        .await
        .unwrap()
        .created_client_incarnations[0];

    let factory = start_runtime(config, clients).await;

    let replica = wait_window(&mut observer, reader, source, index, 20_000).await;
    publish(
        &mut observer,
        reader,
        BTreeMap::from([(index, Timestamp::new(15_000))]),
    )
    .await
    .unwrap();
    assert_eq!(
        observer.state().client_read_requirements()[&(reader, source)],
        Timestamp::new(15_000)
    );
    let mut query = factory();
    query
        .send(ComputeCommand::HelloQuery {
            nonce: Uuid::new_v4(),
        })
        .await
        .unwrap();
    assert!(matches!(
        query.recv().await.unwrap(),
        Some(ComputeResponse::QueryReady)
    ));
    assert_rows(&mut *query, index, &desc, 15_000, &[1]).await;
    publish(&mut observer, reader, BTreeMap::new())
        .await
        .unwrap();
    drop(query);
    drop(writer);
    drop(store);

    // Nothing in this phase issues maintained compute commands or runs a source.
    // Real Persist input progress must move the replica's durable publication.
    input
        .compare_and_append(
            vec![(
                (SourceData(Ok(Row::pack_slice(&[Datum::Int64(2)]))), ()),
                Timestamp::new(35_000),
                1,
            )],
            Antichain::from_elem(Timestamp::new(20_000)),
            Antichain::from_elem(Timestamp::new(40_000)),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        wait_window(&mut observer, reader, source, index, 40_000).await,
        replica
    );
    let permission = observer.state().collection_compaction_bounds()[&source].clone();
    assert!(
        permission
            .as_option()
            .is_some_and(|time| *time > Timestamp::new(15_000))
    );
    // Listen leases deliberately retain the preceding batch frontier. A
    // progress-only batch lets that physical lease pass the committed window.
    input
        .compare_and_append(
            Vec::<((SourceData, ()), Timestamp, StorageDiff)>::new(),
            Antichain::from_elem(Timestamp::new(40_000)),
            Antichain::from_elem(Timestamp::new(40_001)),
        )
        .await
        .unwrap()
        .unwrap();
    let applied = timeout(Duration::from_secs(30), async {
        loop {
            let since = persist
                .recent_since::<SourceData, (), Timestamp, StorageDiff>(
                    shard,
                    Diagnostics::for_tests(),
                )
                .await
                .unwrap();
            if timely::PartialOrder::less_equal(&permission, &since) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if applied.is_err() {
        let state = persist.inspect_shard::<Timestamp>(&shard).await.unwrap();
        panic!(
            "replica must apply {permission:?} without the originating writer: {}",
            serde_json::to_string_pretty(&state).unwrap()
        );
    }
    let rejected = publish(
        &mut observer,
        reader,
        BTreeMap::from([(index, Timestamp::new(15_000))]),
    )
    .await;
    assert!(
        matches!(rejected, Err(CatalogError::Catalog(ref error))
        if matches!(error.kind, ErrorKind::Durable(DurableCatalogError::InvalidReadProtection(_)))),
        "a new historical grant below the published bound must be rejected: {rejected:?}"
    );
    assert!(
        !observer
            .state()
            .client_read_requirements()
            .contains_key(&(reader, index))
    );
    publish(
        &mut observer,
        reader,
        BTreeMap::from([(index, Timestamp::new(35_000))]),
    )
    .await
    .unwrap();
    let mut query = factory();
    query
        .send(ComputeCommand::HelloQuery {
            nonce: Uuid::new_v4(),
        })
        .await
        .unwrap();
    assert!(matches!(
        query.recv().await.unwrap(),
        Some(ComputeResponse::QueryReady)
    ));
    assert_rows(&mut *query, index, &desc, 35_000, &[1, 2]).await;
    std::process::exit(0);
}

/// Runs process-lifetime Timely workers in an isolated child. Returns true only
/// in that child, with a panic hook that exits instead of joining the workers.
pub(super) async fn in_child(child_env: &str, test_name: &str, child_timeout: Duration) -> bool {
    if std::env::var_os(child_env).is_none() {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--nocapture"])
            .env(child_env, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let (status, read_out, read_err) = tokio::join!(
            async {
                match timeout(child_timeout, child.wait()).await {
                    Ok(status) => status.map_err(|error| error.to_string()),
                    Err(_) => {
                        child.kill().await.unwrap();
                        Err(format!(
                            "native follower child timed out after {child_timeout:?}"
                        ))
                    }
                }
            },
            stdout.read_to_end(&mut out),
            stderr.read_to_end(&mut err),
        );
        assert!(
            matches!(&status, Ok(status) if status.success()),
            "{status:?}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out),
            String::from_utf8_lossy(&err),
        );
        read_out.unwrap();
        read_err.unwrap();
        return false;
    }

    // Timely workers live for the process lifetime. A panic must not unwind into
    // runtime destruction, which would join workers that intentionally never exit.
    let panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        panic_hook(info);
        std::process::exit(1);
    }));

    true
}

/// Catalog and written source-index plan, with no runtime or reader incarnation.
/// Handles are opened before callers can install a runtime-only blob gate.
pub(super) struct Fixture {
    pub clients: Arc<PersistClientCache>,
    pub persist: PersistClient,
    pub writer: Catalog,
    pub observer: Catalog,
    pub store: ExpressionCacheHandle,
    pub config: follower::Config,
    pub source: GlobalId,
    pub index: GlobalId,
    pub desc: RelationDesc,
    pub shard: ShardId,
    pub input: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
}

impl Fixture {
    /// Seeds one value at its timestamp and advances the input to `initial_upper`.
    /// `non_inline` forces source batches into blob storage before seeding.
    pub async fn new(
        (initial_value, initial_timestamp): (i64, u64),
        initial_upper: u64,
        non_inline: bool,
    ) -> Self {
        let mut persist_config = mz_persist_client::cfg::PersistConfig::new_for_tests();
        persist_config.configs = Arc::new(mz_dyncfgs::all_dyncfgs());
        persist_config.critical_downgrade_interval = Duration::from_millis(10);
        let clients = Arc::new(PersistClientCache::new(
            persist_config,
            &MetricsRegistry::new(),
            |_, _| mz_persist_client::rpc::PubSubClientConnection::noop(),
        ));
        let location = PersistLocation::new_in_mem();
        let persist = clients.open(location.clone()).await.unwrap();
        let mut writer = debug_catalog(&persist, Some(ShardId::new())).await;
        assert!(writer.state().catalog_read_protection_enabled());
        // Keep normal asynchronous lease downgrades, with a fixture-sized clock
        // interval rather than waiting for the production 15-minute lease.
        transact(
            &mut writer,
            vec![Op::UpdateSystemConfiguration {
                name: "persist_reader_lease_duration".into(),
                value: mz_sql::session::vars::OwnedVarInput::Flat("1s".into()),
            }],
        )
        .await;
        let cluster = writer.user_clusters().next().unwrap();
        let cluster_id = cluster.id;
        let replica_id = cluster
            .replicas()
            .next()
            .expect("bootstrap replica")
            .replica_id;
        let config = follower::Config {
            environment_id: writer.config().environment_id.clone(),
            reconstruction: writer.replica_config(),
            connection_context: writer.config().connection_context.clone(),
            cluster_id,
            replica_id,
            deploy_generation: 0,
            persist_location: location,
            build_info: writer.config().build_info,
        };

        create(
            &mut writer,
            "ingestion",
            "CREATE SOURCE materialize.public.ingestion IN CLUSTER quickstart \
             FROM LOAD GENERATOR COUNTER"
                .into(),
        )
        .await;
        let details = hex::encode(
            SourceExportStatementDetails::LoadGenerator {
                output: LoadGeneratorOutput::Default,
            }
            .into_proto()
            .encode_to_vec(),
        );
        let source = create(
            &mut writer,
            "input",
            format!(
                "CREATE TABLE materialize.public.input FROM SOURCE materialize.public.ingestion \
                 (REFERENCE counter) WITH (DETAILS '{details}')"
            ),
        )
        .await;
        let index = create(
            &mut writer,
            "retaining_index",
            "CREATE INDEX retaining_index IN CLUSTER quickstart \
             ON materialize.public.input (counter) WITH (RETAIN HISTORY FOR '10 seconds')"
                .into(),
        )
        .await;
        let desc = writer
            .state()
            .try_get_desc_by_global_id(&source)
            .unwrap()
            .into_owned();
        let shard = writer.state().storage_metadata().collection_metadata[&source];
        let mut input = persist
            .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Arc::new(desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();
        if non_inline {
            disable_inline_parts(&clients);
        }
        input
            .compare_and_append(
                vec![(
                    (
                        SourceData(Ok(Row::pack_slice(&[Datum::Int64(initial_value)]))),
                        (),
                    ),
                    Timestamp::new(initial_timestamp),
                    1,
                )],
                Antichain::from_elem(Timestamp::MIN),
                Antichain::from_elem(Timestamp::new(initial_upper)),
            )
            .await
            .unwrap()
            .unwrap();

        // Discover the exact named shard opened by follower::run. An unrelated
        // expression cache can contain valid bytes that the follower will never see.
        let storage = TestCatalogStateBuilder::new(persist.clone())
            .with_organization_id(config.environment_id.organization_id())
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .join()
            .await
            .unwrap();
        let opened = Box::pin(Catalog::open_committed(
            writer.replica_config().into_state(
                config.build_info,
                config.environment_id.clone(),
                config.connection_context.clone(),
                persist.clone(),
            ),
            storage,
        ))
        .await
        .unwrap();
        let build = expression_build_version(config.build_info);
        let store = ExpressionCacheHandle::open_plan_store(
            build.clone(),
            &persist,
            opened
                .expression_cache_shard
                .expect("named expression shard"),
        )
        .await;
        let observer = opened.catalog;
        let mut mir = DataflowDescription::new("native source index".into());
        mir.import_source(source, desc.typ().clone(), true);
        mir.export_index(
            index,
            IndexDesc {
                on_id: source,
                key: vec![MirScalarExpr::Column(0, Default::default())],
            },
            desc.typ().into(),
        );
        let features = Default::default();
        let physical_plan =
            LirRelationExpr::finalize_dataflow(mir.clone(), &features, None).unwrap();
        let revision = Uuid::new_v4();
        store
            .write_plans(vec![(
                index,
                revision,
                GlobalExpressions {
                    global_mir: mir,
                    physical_plan,
                    dataflow_metainfos: Default::default(),
                    optimizer_features: features,
                    item_version: RelationVersion::root(),
                },
            )])
            .await
            .unwrap();
        transact(
            &mut writer,
            vec![Op::SetWrittenPlan {
                id: index,
                build_version: build.to_string(),
                expected_revision: None,
                revision: Some(revision),
                imports: BTreeSet::from([source]),
            }],
        )
        .await;
        Self {
            clients,
            persist,
            writer,
            observer,
            store,
            config,
            source,
            index,
            desc,
            shard,
            input,
        }
    }
}

pub(super) async fn start_runtime(
    config: follower::Config,
    clients: Arc<PersistClientCache>,
) -> impl Fn() -> Box<dyn ComputeClient> + use<> {
    let registry = MetricsRegistry::new();
    let mut server = mz_compute::server::serve(
        TimelyConfig {
            workers: 2,
            addresses: vec!["127.0.0.1:0".into()],
            ..Default::default()
        },
        ComputeRuntimeRole::Solo,
        true,
        &registry,
        Arc::clone(&clients),
        TxnsContext::default(),
        Arc::new(TracingHandle::disabled()),
        ComputeInstanceContext {
            scratch_directory: None,
            worker_core_affinity: false,
            connection_context: config.connection_context.clone(),
        },
        Vec::new(),
    )
    .await
    .unwrap();
    let endpoint = server.take_replica().unwrap();
    let factory = server.client_builder();
    drop(server);
    // A returned follower, including an error, is always a failure in this test.
    mz_ore::task::spawn(|| "native follower test", async move {
        let result = follower::run(config, clients, registry, Some(endpoint)).await;
        panic!("native follower stopped: {result:?}");
    });

    factory
}

pub(super) fn disable_inline_parts(clients: &PersistClientCache) {
    let mut updates = ConfigUpdates::default();
    updates.add_dynamic(
        "persist_inline_writes_single_max_bytes",
        ConfigVal::Usize(0),
    );
    updates.add_dynamic("persist_inline_writes_total_max_bytes", ConfigVal::Usize(0));
    clients.cfg.apply_from(&updates);
}

async fn create(catalog: &mut Catalog, item_name: &str, sql: String) -> GlobalId {
    let (id, global_id) = catalog.allocate_user_id_for_test().await.unwrap();
    let item = test_support::parse_item(
        &mut catalog.state().clone(),
        global_id,
        &sql,
        &BTreeMap::new(),
    )
    .unwrap_or_else(|error| panic!("parse {sql}: {error}"));
    let op = Op::CreateItem {
        id,
        name: name(catalog, item_name),
        item,
        owner_id: MZ_SYSTEM_ROLE_ID,
    };
    transact(catalog, vec![op]).await;
    global_id
}

async fn wait_window(
    catalog: &mut Catalog,
    reader: u64,
    source: GlobalId,
    index: GlobalId,
    upper: u64,
) -> u64 {
    timeout(Duration::from_secs(30), async {
        loop {
            catalog.sync_to_current_updates().await.unwrap();
            let state = catalog.state();
            let expected = state
                .index_read_policy(index)
                .unwrap()
                .frontier(Antichain::from_elem(Timestamp::new(upper)).borrow());
            let floor = *expected.as_option().unwrap();
            if state.collection_compaction_bounds().get(&index) == Some(&expected)
                && let Some((&incarnation, _)) = state
                    .client_incarnations()
                    .iter()
                    .find(|(id, _)| **id != reader)
                && state.client_read_requirements().get(&(incarnation, index)) == Some(&floor)
                && state.client_read_requirements().get(&(incarnation, source)) == Some(&floor)
            {
                return incarnation;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replica must publish its live window and logical input grant")
}

pub(super) async fn publish(
    catalog: &mut Catalog,
    incarnation: u64,
    requirements: BTreeMap<GlobalId, Timestamp>,
) -> Result<(), CatalogError> {
    timeout(Duration::from_secs(10), async {
        loop {
            catalog.sync_to_current_updates().await.unwrap();
            let ts = catalog.current_upper().await;
            match catalog
                .transact(
                    None,
                    ts,
                    None,
                    vec![Op::PublishClientReadRequirements {
                        incarnation,
                        requirements: requirements.clone(),
                    }],
                )
                .await
            {
                Err(CatalogError::Catalog(error))
                    if matches!(
                        error.kind,
                        ErrorKind::Durable(DurableCatalogError::CatalogOutOfSync { .. })
                    ) =>
                {
                    continue;
                }
                result => return result.map(|_| ()),
            }
        }
    })
    .await
    .expect("publish historical read grant")
}

pub(super) async fn assert_rows(
    query: &mut dyn ComputeClient,
    index: GlobalId,
    desc: &RelationDesc,
    timestamp: u64,
    expected: &[i64],
) {
    let uuid = Uuid::new_v4();
    query
        .send(ComputeCommand::SetQueryMaxResultSize {
            max_result_size: u64::MAX,
        })
        .await
        .unwrap();
    query
        .send(ComputeCommand::Peek(Box::new(Peek {
            target: PeekTarget::Index { id: index },
            result_desc: desc.clone(),
            literal_constraints: None,
            uuid,
            timestamp: Timestamp::new(timestamp),
            finishing: RowSetFinishing::trivial(desc.arity()),
            map_filter_project: SafeMfpPlan::from_mfp(MapFilterProject::new(desc.arity())),
            otel_ctx: mz_ore::tracing::OpenTelemetryContext::empty(),
        })))
        .await
        .unwrap();
    let response = timeout(Duration::from_secs(15), async {
        loop {
            let response = query.recv().await.unwrap().expect("query connection");
            match response {
                ComputeResponse::Frontiers(..) => continue,
                response => break response,
            }
        }
    })
    .await
    .unwrap();
    let ComputeResponse::PeekResponse(id, PeekResponse::Rows(batches), _) = response else {
        panic!("expected index rows: {response:?}");
    };
    assert_eq!(id, uuid);
    let mut rows = Vec::new();
    for batch in batches {
        for i in 0..batch.entries() {
            let (row, count) = batch.get(i).unwrap();
            rows.extend(std::iter::repeat_n(
                row.iter().next().unwrap().unwrap_int64(),
                count.get(),
            ));
        }
    }
    rows.sort();
    assert_eq!(rows, expected);
}
