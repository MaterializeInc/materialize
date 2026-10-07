// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_expr::CollectionPlan;
use mz_persist_client::critical::Opaque;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_proto::RustType;
use mz_repr::{CatalogItemId, Datum, GlobalId, Row, Timestamp};
use mz_sql::DEFAULT_SCHEMA;
use mz_sql::catalog::CatalogDatabase;
use mz_sql::names::{ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::DEFAULT_DATABASE_NAME;
use mz_storage_client::storage_collections::CollectionFrontiers;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::load_generator::LoadGeneratorOutput;
use mz_storage_types::sources::{SourceData, SourceExportStatementDetails};
use prost::Message;
use timely::progress::Antichain;
use uuid::Uuid;

use crate::SYSTEM_CONN_ID;
use crate::catalog::{Catalog, Op, test_support};
use crate::durable::objects::CollectionCompactionBound;
use crate::memory::objects::CatalogItem;

/// A native durable catalog, with no controller or query client. Source data
/// advances only through explicit Persist writes, never a running generator.
struct Fixture {
    catalog: Catalog,
    persist: PersistClient,
    qualifiers: ItemQualifiers,
    prefix: String,
    observed_uppers: BTreeMap<GlobalId, Antichain<Timestamp>>,
}

impl Fixture {
    async fn new() -> Self {
        let persist = PersistClient::new_for_tests().await;
        let organization = Uuid::new_v4();
        let mut bootstrap = crate::durable::test_bootstrap_args();
        bootstrap.default_cluster_replication_factor = 0;
        let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .open(Timestamp::MIN, &bootstrap)
            .await
            .expect("open durable catalog");
        let catalog = Catalog::open_debug_catalog_inner(
            persist.clone(),
            storage,
            (|| 0u64).into(),
            Some(
                format!("local-az1-{organization}-0")
                    .parse()
                    .expect("environment ID"),
            ),
            &mz_build_info::DUMMY_BUILD_INFO,
            BTreeMap::from([("enable_catalog_read_protection".into(), "true".into())]),
            &bootstrap,
            None,
            None,
        )
        .await
        .expect("open protected catalog");
        let database = catalog
            .resolve_database(DEFAULT_DATABASE_NAME)
            .expect("default database");
        let database_spec = ResolvedDatabaseSpecifier::Id(database.id());
        let schema = catalog
            .resolve_schema_in_database(&database_spec, DEFAULT_SCHEMA, &SYSTEM_CONN_ID)
            .expect("default schema");
        let qualifiers = ItemQualifiers {
            database_spec,
            schema_spec: schema.id.clone(),
        };
        let prefix = format!("{}.{}", database.name, schema.name.schema);
        assert_eq!(
            catalog
                .state()
                .resolve_cluster("quickstart")
                .expect("default cluster")
                .replicas()
                .count(),
            0
        );
        assert!(catalog.state().client_incarnations().is_empty());
        Self {
            catalog,
            persist,
            qualifiers,
            prefix,
            observed_uppers: BTreeMap::new(),
        }
    }

    async fn transact(&mut self, ops: Vec<Op>) {
        let ts = self.catalog.current_upper().await;
        self.catalog
            .transact(None, ts, None, ops)
            .await
            .expect("catalog transaction");
    }

    async fn reopen(self) -> Self {
        let config = self.catalog.config();
        let organization = config.environment_id.organization_id();
        let state_config = self.catalog.replica_config().into_state(
            config.build_info,
            config.environment_id.clone(),
            config.connection_context.clone(),
            self.persist.clone(),
        );
        self.catalog.expire().await;
        let storage = crate::durable::TestCatalogStateBuilder::new(self.persist.clone())
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .join()
            .await
            .expect("join after shutdown");
        let opened = Box::pin(Catalog::open_committed(state_config, storage))
            .await
            .expect("reconstruct native catalog");
        Self {
            catalog: opened.catalog,
            ..self
        }
    }

    async fn create(&mut self, name: &str, sql: String) -> (CatalogItemId, GlobalId) {
        self.create_with_plan(name, sql, None).await
    }

    async fn create_with_plan(
        &mut self,
        name: &str,
        sql: String,
        imports: Option<BTreeSet<GlobalId>>,
    ) -> (CatalogItemId, GlobalId) {
        let (id, global_id) = self
            .catalog
            .allocate_user_id_for_test()
            .await
            .expect("allocate fixture IDs");
        let item =
            test_support::parse_item(&mut self.catalog.state, global_id, &sql, &BTreeMap::new())
                .unwrap_or_else(|err| panic!("parse {sql}: {err}"));
        let mut ops = vec![Op::CreateItem {
            id,
            name: QualifiedItemName {
                qualifiers: self.qualifiers.clone(),
                item: name.into(),
            },
            item,
            owner_id: MZ_SYSTEM_ROLE_ID,
        }];
        if let Some(imports) = imports {
            ops.push(self.select_plan(global_id, "test-build", Some(imports)));
        }
        self.transact(ops).await;
        (id, global_id)
    }

    fn select_plan(&self, id: GlobalId, build: &str, imports: Option<BTreeSet<GlobalId>>) -> Op {
        // Catalog admission checks the selection metadata, not immutable bytes.
        Op::SetWrittenPlan {
            id,
            build_version: build.into(),
            expected_revision: self.catalog.state().written_plan(id, build),
            revision: imports.as_ref().map(|_| Uuid::new_v4()),
            imports: imports.unwrap_or_default(),
            replica_owner: None,
        }
    }

    async fn source(&mut self, name: &str) -> GlobalId {
        self.create(
            &format!("{name}_ingestion"),
            format!(
                "CREATE SOURCE {}.{name}_ingestion IN CLUSTER quickstart \
                 FROM LOAD GENERATOR COUNTER",
                self.prefix,
            ),
        )
        .await;
        let details = hex::encode(
            SourceExportStatementDetails::LoadGenerator {
                output: LoadGeneratorOutput::Default,
            }
            .into_proto()
            .encode_to_vec(),
        );
        // This is an ingestion export, not TableWrites. Its durable upper is
        // the data shard's upper and does not require a transaction WAL.
        self.create(
            name,
            format!(
                "CREATE TABLE {}.{name} FROM SOURCE {}.{name}_ingestion \
                 (REFERENCE counter) WITH (DETAILS '{details}')",
                self.prefix, self.prefix,
            ),
        )
        .await
        .1
    }

    async fn index(&mut self, name: &str, on: &str, imports: &[GlobalId]) -> GlobalId {
        let (_, index) = self
            .create_with_plan(
                name,
                format!(
                    "CREATE INDEX {name} IN CLUSTER quickstart \
                     ON {}.{on} (counter) WITH (RETAIN HISTORY FOR '10 seconds')",
                    self.prefix,
                ),
                Some(imports.iter().copied().collect()),
            )
            .await;
        assert!(
            !self
                .catalog
                .state()
                .maintained_read_requirements()
                .contains_key(&index),
            "index object retention is derived, not a maintained-read record"
        );
        index
    }

    async fn advance_upper(&mut self, id: GlobalId, upper: u64) {
        self.append(id, upper, Vec::new()).await;
    }

    async fn append(
        &mut self,
        id: GlobalId,
        upper: u64,
        updates: Vec<((SourceData, ()), Timestamp, StorageDiff)>,
    ) {
        let shard = self.catalog.state().storage_metadata().collection_metadata[&id];
        let desc = self
            .catalog
            .state()
            .try_get_desc_by_global_id(&id)
            .expect("persisted relation")
            .into_owned();
        let mut writer = self
            .persist
            .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Arc::new(desc),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("open source writer");
        let expected = writer.upper().clone();
        writer
            .compare_and_append(
                updates,
                expected,
                Antichain::from_elem(Timestamp::new(upper)),
            )
            .await
            .expect("valid upper advance")
            .expect("uncontended upper advance");
        writer.expire().await;
        self.observed_uppers
            .insert(id, Antichain::from_elem(Timestamp::new(upper)));
        // Advance the source's independent recovery requirement from its durable
        // output, so its birth protection cannot mask missing index retention.
        if let Some(mut requirement) = self
            .catalog
            .state()
            .maintained_read_requirements()
            .get(&id)
            .cloned()
        {
            requirement.frontier = Some(Timestamp::new(upper).saturating_sub(1));
            self.transact(vec![Op::SetReadProtection {
                requirements: vec![requirement],
                bounds: vec![],
            }])
            .await;
        }
    }

    async fn propose_bounds(&mut self, ids: &[GlobalId], proposed: u64, expected: u64) {
        let proposed = Timestamp::new(proposed);
        let frontiers: Vec<_> = self
            .observed_uppers
            .iter()
            .map(|(id, upper)| CollectionFrontiers {
                id: *id,
                write_frontier: upper.clone(),
                implied_capability: Antichain::from_elem(Timestamp::MIN),
                read_capabilities: Antichain::from_elem(Timestamp::MIN),
            })
            .collect();
        let mut bounds: BTreeMap<_, _> = ids
            .iter()
            .map(|id| {
                let frontier = self
                    .catalog
                    .state()
                    .maintained_read_frontier(*id, &BTreeSet::new())
                    .map_or(proposed, |required| proposed.min(required));
                (*id, Antichain::from_elem(frontier))
            })
            .collect();
        bounds.extend(self.catalog.state().index_retention_proposals(&frontiers));
        self.transact(vec![Op::SetReadProtection {
            requirements: vec![],
            bounds: bounds
                .into_iter()
                .map(|(id, frontier)| CollectionCompactionBound {
                    id,
                    frontier: frontier.as_option().copied(),
                })
                .collect(),
        }])
        .await;
        for id in ids {
            self.assert_bound(*id, expected);
        }
    }

    async fn set_bounds(&mut self, bounds: &[(GlobalId, u64)]) {
        self.transact(vec![Op::SetReadProtection {
            requirements: vec![],
            bounds: bounds
                .iter()
                .map(|(id, frontier)| CollectionCompactionBound {
                    id: *id,
                    frontier: Some(Timestamp::new(*frontier)),
                })
                .collect(),
        }])
        .await;
    }

    fn assert_bound(&self, id: GlobalId, expected: u64) {
        assert_eq!(
            self.catalog.state().collection_compaction_bounds()[&id],
            Antichain::from_elem(Timestamp::new(expected)),
            "committed compaction bound for {id}",
        );
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_retention_tracks_durable_input_upper_without_replicas() {
    let mut f = Fixture::new().await;
    let input = f.source("input").await;
    f.advance_upper(input, 100_000).await;
    let index = f.index("retaining_index", "input", &[input]).await;

    f.propose_bounds(&[input, index], 100_000, 90_000).await;
    // No catalog object or client grant changes. A creation-time pin cannot
    // satisfy this second proposal, only the new durable input upper can.
    f.advance_upper(input, 120_000).await;
    f.propose_bounds(&[input, index], 120_000, 110_000).await;
    assert!(f.catalog.state().client_incarnations().is_empty());
    assert!(f.catalog.state().client_read_requirements().is_empty());
    f.catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_plan_selection_atomically_protects_actual_imports() {
    let mut f = Fixture::new().await;
    let input = f.source("input").await;
    f.advance_upper(input, 100_000).await;
    f.set_bounds(&[(input, 20_000)]).await;
    let old = f.index("old_import", "input", &[input]).await;
    let replacement = f.index("replacement_import", "input", &[input]).await;
    let past = f.index("past_import", "input", &[input]).await;
    // All progress proposals are below the observed upper's retention frontier.
    // Leave the logical input behind the actual import to distinguish admission.
    f.set_bounds(&[(old, 40_000), (past, 80_000)]).await;
    f.assert_bound(input, 20_000);
    f.assert_bound(old, 40_000);
    f.assert_bound(past, 80_000);

    let index = f.index("retaining_index", "input", &[old]).await;
    f.assert_bound(index, 40_000);
    let initial_revision = f.catalog.state().written_plan(index, "test-build");
    assert!(initial_revision.is_some());
    f.set_bounds(&[(old, 80_000)]).await;
    f.assert_bound(old, 40_000);

    let invalid = f.select_plan(index, "test-build", Some(BTreeSet::from([past])));
    let ts = f.catalog.current_upper().await;
    assert!(
        f.catalog
            .transact(None, ts, None, vec![invalid])
            .await
            .is_err(),
        "a selected import must still be readable at the owner's bound",
    );
    assert_eq!(
        f.catalog.state().written_plan(index, "test-build"),
        initial_revision,
    );
    f.assert_bound(index, 40_000);

    f.transact(vec![f.select_plan(
        index,
        "other-build",
        Some(BTreeSet::from([old])),
    )])
    .await;
    // Only selection metadata changes. The SQL definition remains unchanged.
    f.transact(vec![f.select_plan(
        index,
        "test-build",
        Some(BTreeSet::from([replacement])),
    )])
    .await;
    assert_ne!(
        f.catalog.state().written_plan(index, "test-build"),
        initial_revision,
    );
    f.set_bounds(&[(old, 80_000), (replacement, 80_000)]).await;
    f.assert_bound(old, 40_000);
    f.assert_bound(replacement, 40_000);

    // Retiring the other build releases only its import. The replacement is
    // protected by the surviving selection without any replica or client hold.
    f.transact(vec![f.select_plan(index, "other-build", None)])
        .await;
    f.set_bounds(&[(old, 80_000), (replacement, 80_000)]).await;
    f.assert_bound(old, 80_000);
    f.assert_bound(replacement, 40_000);
    f.assert_bound(index, 40_000);
    assert!(f.catalog.state().client_incarnations().is_empty());
    f.catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn closing_client_incarnation_preserves_index_object_retention() {
    let mut f = Fixture::new().await;
    let input = f.source("input").await;
    let row = SourceData(Ok(Row::pack_slice(&[Datum::Int64(42)])));
    f.append(
        input,
        100_000,
        vec![((row.clone(), ()), Timestamp::new(50_000), 1)],
    )
    .await;
    f.index("retaining_index", "input", &[input]).await;

    let ts = f.catalog.current_upper().await;
    let incarnation = f
        .catalog
        .transact(
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation { replica_id: None }],
        )
        .await
        .expect("create client")
        .created_client_incarnations[0];
    f.transact(vec![Op::PublishClientReadRequirements {
        incarnation,
        requirements: BTreeMap::from([(input, Timestamp::new(80_000))]),
    }])
    .await;
    f.propose_bounds(&[input], 80_000, 80_000).await;

    let heartbeat = f.catalog.state().client_incarnations()[&incarnation].heartbeat;
    f.transact(vec![Op::ReclaimClientIncarnation {
        incarnation,
        expected_heartbeat: heartbeat,
    }])
    .await;
    assert!(f.catalog.state().client_incarnations().is_empty());
    f.propose_bounds(&[input], 100_000, 90_000).await;

    // No leased reader or query token exists while committed permission is
    // applied to Persist. The critical handle is the compaction enforcer.
    let shard = f.catalog.state().storage_metadata().collection_metadata[&input];
    let mut since = f
        .persist
        .open_critical_since::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            PersistClient::CONTROLLER_CRITICAL_SINCE,
            Opaque::encode(&0u64),
            Diagnostics::for_tests(),
        )
        .await
        .expect("open compaction enforcer");
    let opaque = since.opaque().clone();
    since
        .compare_and_downgrade_since(
            &opaque,
            (
                &opaque,
                &f.catalog.state().collection_compaction_bounds()[&input],
            ),
        )
        .await
        .expect("apply committed permission");
    drop(since);
    let mut f = f.reopen().await;
    assert!(f.catalog.state().client_read_requirements().is_empty());
    assert_eq!(
        f.persist.recent_since::<SourceData, (), Timestamp, StorageDiff>(
            shard, Diagnostics::for_tests(),
        ).await.expect("observe applied since after restart"),
        Antichain::from_elem(Timestamp::new(90_000)),
    );
    let mut reader = f
        .persist
        .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            Arc::new(
                f.catalog
                    .state()
                    .try_get_desc_by_global_id(&input)
                    .expect("reconstructed source descriptor")
                    .into_owned(),
            ),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
            false,
        )
        .await
        .expect("open historical reader after compaction");
    assert!(
        reader
            .snapshot_and_fetch(Antichain::from_elem(Timestamp::new(85_000)))
            .await
            .is_err()
    );
    let rows = reader
        .snapshot_and_fetch(Antichain::from_elem(Timestamp::new(90_000)))
        .await
        .expect("policy-retained snapshot remains readable");
    assert_eq!(
        rows.into_iter()
            .map(|((row, ()), _, diff)| (row, diff))
            .collect::<Vec<_>>(),
        vec![(row, 1)]
    );
    reader.expire().await;
    f.advance_upper(input, 120_000).await;
    f.propose_bounds(&[input], 120_000, 110_000).await;
    f.catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_retention_protects_logical_input_eliminated_by_optimizer() {
    let mut f = Fixture::new().await;
    let live = f.source("live_input").await;
    let eliminated = f.source("eliminated_input").await;
    f.advance_upper(live, 100_000).await;
    f.advance_upper(eliminated, 100_000).await;
    let (view_id, _) = f
        .create(
            "logical_view",
            format!(
                "CREATE VIEW {}.logical_view AS \
                 SELECT counter FROM {}.live_input UNION ALL \
                 SELECT counter FROM {}.eliminated_input WHERE false",
                f.prefix, f.prefix, f.prefix,
            ),
        )
        .await;
    let CatalogItem::View(view) = f.catalog.state().get_entry(&view_id).item() else {
        panic!("expected view");
    };
    assert_eq!(
        view.raw_expr.depends_on(),
        BTreeSet::from([live, eliminated])
    );
    assert_eq!(
        view.locally_optimized_expr.depends_on(),
        BTreeSet::from([live]),
        "fixture must actually eliminate the second input"
    );
    f.index("retaining_index", "logical_view", &[live]).await;
    f.propose_bounds(&[live, eliminated], 100_000, 90_000).await;
    f.catalog.expire().await;
}
