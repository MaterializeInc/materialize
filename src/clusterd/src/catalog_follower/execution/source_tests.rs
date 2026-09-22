// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Production source installation and live ingestion without a catalog writer.
//! This does not claim process-restart coverage: native workers are process-lived
//! and the fixture's Persist backend is memory-local to the child process.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mz_catalog::catalog::{Catalog, Op, test_support};
use mz_cluster_client::client::TimelyConfig;
use mz_compute::server::{ComputeInstanceContext, ComputeRuntimeRole};
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::ComputeClient;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_proto::RustType;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_storage::storage_state::StorageInstanceContext;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::load_generator::LoadGeneratorOutput;
use mz_storage_types::sources::{SourceData, SourceExportStatementDetails};
use mz_txn_wal::operator::TxnsContext;
use prost::Message;
use timely::progress::Antichain;
use tokio::time::timeout;
use uuid::Uuid;

use super::tests::in_child;
use crate::catalog_follower as follower;
use follower::tests::{debug_catalog, name, transact};

#[mz_ore::test(tokio::test)]
async fn catalog_counter_ingests_after_writer_and_query_drop() {
    const CHILD: &str = "MZ_CLUSTERD_NATIVE_SOURCE_TEST_CHILD";
    const TEST: &str = "catalog_follower::execution::source_tests::catalog_counter_ingests_after_writer_and_query_drop";
    if !in_child(CHILD, TEST, Duration::from_secs(120)).await {
        return;
    }

    let mut persist_config = mz_persist_client::cfg::PersistConfig::new_for_tests();
    persist_config.configs = Arc::new(mz_dyncfgs::all_dyncfgs());
    let registry = MetricsRegistry::new();
    let clients = Arc::new(PersistClientCache::new(
        persist_config,
        &registry,
        |_, _| mz_persist_client::rpc::PubSubClientConnection::noop(),
    ));
    let location = PersistLocation::new_in_mem();
    let persist = clients.open(location.clone()).await.unwrap();
    let mut writer = debug_catalog(&persist, Some(ShardId::new())).await;
    assert!(writer.state().catalog_read_protection_enabled());
    let cluster = writer.user_clusters().next().unwrap();
    let config = follower::Config {
        environment_id: writer.config().environment_id.clone(),
        reconstruction: writer.replica_config(),
        connection_context: writer.config().connection_context.clone(),
        cluster_id: cluster.id,
        replica_id: cluster.replicas().next().unwrap().replica_id,
        deploy_generation: 0,
        persist_location: location,
        build_info: writer.config().build_info,
    };

    create(
        &mut writer,
        "counter_source",
        "CREATE SOURCE materialize.public.counter_source IN CLUSTER quickstart \
         FROM LOAD GENERATOR COUNTER (TICK INTERVAL '100ms')"
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
        "counter_rows",
        format!(
            "CREATE TABLE materialize.public.counter_rows \
             FROM SOURCE materialize.public.counter_source \
             (REFERENCE counter) WITH (DETAILS '{details}')"
        ),
    )
    .await;
    // Both the shard identity and schema come from committed catalog definitions.
    // No StorageCollections registration or source writer seeds this input.
    let shard = writer.state().storage_metadata().collection_metadata[&source];
    let desc = writer
        .state()
        .try_get_desc_by_global_id(&source)
        .unwrap()
        .into_owned();

    // The snapshots below are reads too. Establish their durable logical grant
    // before the follower can publish compaction, and retain the physical lease.
    let ts = writer.current_upper().await;
    let reader = writer
        .transact(
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation { replica_id: None }],
        )
        .await
        .unwrap()
        .created_client_incarnations[0];
    transact(
        &mut writer,
        vec![Op::PublishClientReadRequirements {
            incarnation: reader,
            requirements: BTreeMap::from([(source, Timestamp::MIN)]),
        }],
    )
    .await;
    assert!(
        persist
            .latest_schema::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Diagnostics::for_tests()
            )
            .await
            .unwrap()
            .is_none(),
        "only the native source may initialize its schema"
    );
    let factory = start_runtime(config, clients, registry).await;
    let mut query = factory();
    query
        .send(ComputeCommand::HelloQuery {
            nonce: Uuid::new_v4(),
        })
        .await
        .unwrap();
    assert!(matches!(
        // Readiness follows full catalog reconstruction. The subprocess deadline
        // bounds bootstrap as well as the subsequent ingestion assertions.
        query.recv().await.unwrap(),
        Some(ComputeResponse::QueryReady)
    ));
    timeout(Duration::from_secs(30), async {
        while persist
            .recent_upper::<SourceData, (), Timestamp, StorageDiff>(shard, Diagnostics::for_tests())
            .await
            .unwrap()
            == Antichain::from_elem(Timestamp::MIN)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("native source initializes and writes the cold shard");
    let mut input = persist
        .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            Arc::new(desc),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
            true,
        )
        .await
        .unwrap();
    let (first_upper, first_counter) =
        wait_for_rows(&persist, shard, &mut input, Timestamp::MIN, 0).await;
    drop(query);
    drop(factory);
    drop(writer);

    // Only the native follower and its two runtime endpoints own maintained
    // execution now. Require new persisted data, not just frontier movement.
    // Establish a post-disconnect observation first so rows committed between
    // the first snapshot and disconnection cannot alone satisfy the assertion.
    let (disconnected_upper, disconnected_counter) =
        wait_for_rows(&persist, shard, &mut input, first_upper, first_counter).await;
    let (next_upper, next_counter) = wait_for_rows(
        &persist,
        shard,
        &mut input,
        disconnected_upper,
        disconnected_counter,
    )
    .await;
    assert!(next_upper > disconnected_upper);
    assert!(next_counter > disconnected_counter);
    std::process::exit(0);
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

async fn wait_for_rows(
    persist: &PersistClient,
    shard: ShardId,
    input: &mut ReadHandle<SourceData, (), Timestamp, StorageDiff>,
    previous_upper: Timestamp,
    previous_counter: i64,
) -> (Timestamp, i64) {
    timeout(Duration::from_secs(30), async {
        loop {
            let upper = persist
                .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                    shard,
                    Diagnostics::for_tests(),
                )
                .await
                .unwrap();
            let upper = *upper.as_option().expect("unbounded counter stays open");
            if upper > previous_upper {
                let rows = input
                    .snapshot_and_fetch(Antichain::from_elem(upper.step_back().unwrap()))
                    .await
                    .expect("snapshot remains protected by the reader grant");
                let mut maximum = 0;
                for ((data, ()), _, diff) in rows {
                    let row = data.0.expect("counter must not produce source errors");
                    assert_eq!(diff, 1, "counter rows are append-only");
                    let counter = row.iter().next().unwrap().unwrap_int64();
                    maximum = maximum.max(counter);
                }
                if maximum > previous_counter {
                    return (upper, maximum);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("native source must advance its Persist upper and ingest new counter rows")
}

async fn start_runtime(
    config: follower::Config,
    clients: Arc<PersistClientCache>,
    registry: MetricsRegistry,
) -> impl Fn() -> Box<dyn ComputeClient> + use<> {
    let timely = TimelyConfig {
        workers: 2,
        addresses: vec!["127.0.0.1:0".into()],
        ..Default::default()
    };
    let mut compute = mz_compute::server::serve(
        timely.clone(),
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
    let mut storage = mz_storage::server::serve_with_replica(
        timely,
        true,
        &registry,
        Arc::clone(&clients),
        TxnsContext::default(),
        Arc::new(TracingHandle::disabled()),
        SYSTEM_TIME.clone(),
        config.connection_context.clone(),
        StorageInstanceContext::new(None, None),
        Vec::new(),
    )
    .await
    .unwrap();
    let compute_endpoint = compute.take_replica().unwrap();
    let storage_endpoint = storage.take_replica().unwrap();
    let factory = compute.client_builder();
    drop(compute);
    drop(storage);
    mz_ore::task::spawn(|| "native source follower test", async move {
        let result = follower::run(
            config,
            clients,
            registry,
            Some(compute_endpoint),
            Some(storage_endpoint),
        )
        .await;
        panic!("native source follower stopped: {result:?}");
    });
    factory
}
