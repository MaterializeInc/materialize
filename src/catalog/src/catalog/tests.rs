// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::{env, iter};

use crate::memory::objects::CatalogItem;
use itertools::Itertools;
use mz_postgres_util::{query, sql};
use tokio_postgres::NoTls;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::SYSTEM_CONN_ID;
use crate::builtin::{BUILTINS, Builtin, BuiltinType};
use crate::durable::{CatalogError, DurableCatalogError, FenceError, test_bootstrap_args};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::{assert_err, assert_ok, task};
use mz_persist_client::PersistClient;
use mz_pgrepr::oid::{FIRST_MATERIALIZE_OID, FIRST_UNPINNED_OID, FIRST_USER_OID};
use mz_repr::namespaces::{INFORMATION_SCHEMA, PG_CATALOG_SCHEMA};
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, RelationVersionSelector, SqlScalarType, Timestamp};
use mz_sql::catalog::{CatalogSchema, CatalogType, SessionCatalog};
use mz_sql::func::OP_IMPLS;
use mz_sql::names::{
    self, DatabaseId, ItemQualifiers, ObjectId, PartialItemName, QualifiedItemName,
    ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier, SystemObjectId,
};
use mz_sql::plan::StatementContext;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

use crate::catalog::state::LocalExpressionCache;
use crate::catalog::{Catalog, DebugAwsContext, Op};

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn replica_reconstruction_context_round_trips() {
    Catalog::with_debug(|catalog| async move {
        let original = catalog.replica_config();
        assert!(!original.cluster_replica_sizes.0.is_empty());
        let encoded = serde_json::to_string(&original).expect("serialize replica context");
        let decoded: crate::config::ReplicaCatalogConfig =
            serde_json::from_str(&encoded).expect("deserialize replica context");
        assert_eq!(
            serde_json::to_value(&original.cluster_replica_sizes)
                .expect("serialize original sizes"),
            serde_json::to_value(&decoded.cluster_replica_sizes).expect("serialize decoded sizes"),
        );
        assert_eq!(
            encoded,
            serde_json::to_string(&decoded).expect("reserialize replica context")
        );
        assert_eq!(
            decoded.system_parameter_defaults,
            catalog.system_config().defaults()
        );
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_catalog_revision() {
    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let bootstrap_args = test_bootstrap_args();
    {
        let mut catalog = Catalog::open_debug_catalog(
            persist_client.clone(),
            organization_id.clone(),
            &bootstrap_args,
        )
        .await
        .expect("unable to open debug catalog");
        assert_eq!(catalog.transient_revision(), 1);
        assert!(catalog.transient_revision_is_current());
        let snapshot = catalog.clone();
        let commit_ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                commit_ts,
                None,
                vec![Op::CreateDatabase {
                    name: "test".to_string(),
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("failed to transact");
        assert_eq!(catalog.transient_revision(), 2);
        assert!(catalog.transient_revision_is_current());
        // The pre-transaction snapshot detects its own staleness through
        // the shared latest revision.
        assert!(!snapshot.transient_revision_is_current());
        assert_eq!(snapshot.transient_revision(), 1);
        catalog.expire().await;
    }
    {
        let catalog = Catalog::open_debug_catalog(persist_client, organization_id, &bootstrap_args)
            .await
            .expect("unable to open debug catalog");
        assert_eq!(catalog.transient_revision(), 1);
        catalog.expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn owned_catalog_reconstruction_preserves_pending_replica() {
    Catalog::with_debug(|mut catalog| async move {
        let replica = catalog
            .user_cluster_replicas()
            .next()
            .expect("bootstrap user replica")
            .clone();
        let mut config = replica.config;
        let mz_controller_types::clusters::ReplicaLocation::Managed(location) =
            &mut config.location
        else {
            panic!("bootstrap replica must be managed");
        };
        location.pending = true;
        let ts = catalog.current_upper().await;
        let replica_id = catalog
            .allocate_user_replica_ids(1, ts)
            .await
            .expect("can allocate pending replica ID")[0];
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![Op::CreateClusterReplica {
                    cluster_id: replica.cluster_id,
                    replica_id,
                    name: "pending_replica".into(),
                    config,
                    owner_id: replica.owner_id,
                    reason: super::ReplicaCreateDropReason::GracefulReconfiguration,
                }],
            )
            .await
            .expect("can create pending replica");

        let expected = catalog.state().dump(None).expect("can dump catalog state");
        let reader = catalog
            .open_diagnostic_reader()
            .await
            .expect("can open diagnostic catalog reader");
        let upper = catalog.current_upper().await;
        let input = reader
            .into_snapshot_at(upper)
            .await
            .expect("can extract catalog snapshot");
        let reconstructed = catalog
            .reconstruct_state(input)
            .await
            .expect("can reconstruct catalog state");
        assert_eq!(
            expected,
            reconstructed
                .dump(None)
                .expect("can dump reconstructed catalog state")
        );
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn durable_temporary_membership_preserves_storage_lifetime() {
    use crate::SYSTEM_CONN_ID;
    use crate::durable::TestCatalogStateBuilder;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::ShardId;
    use mz_repr::RelationVersion;
    use mz_sql::names::CommentObjectId;
    use mz_storage_client::controller::StorageTxn;

    async fn check(catalog: &Catalog) {
        let reader = catalog
            .open_diagnostic_reader()
            .await
            .expect("can open diagnostic catalog reader");
        let upper = catalog.current_upper().await;
        let snapshot = reader
            .into_snapshot_at(upper)
            .await
            .expect("can extract catalog snapshot");
        catalog
            .check_durable_consistency(snapshot)
            .await
            .expect("durable catalog state is consistent");
    }

    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let bootstrap = test_bootstrap_args();
    let storage = TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("can open durable catalog storage");
    let mut catalog = Catalog::open_debug_catalog_inner(
        persist,
        storage,
        SYSTEM_TIME.clone(),
        Some(
            format!("local-az1-{organization}-0")
                .parse()
                .expect("valid test environment ID"),
        ),
        &mz_build_info::DUMMY_BUILD_INFO,
        BTreeMap::from([("enable_catalog_read_protection".into(), "true".into())]),
        &bootstrap,
        None,
        None,
    )
    .await
    .expect("can open debug catalog");
    assert!(catalog.state().catalog_read_protection_enabled());

    let local_owner = Uuid::new_v4();
    let foreign_owner = Uuid::new_v4();
    catalog
        .state
        .temporary_namespaces
        .register(SYSTEM_CONN_ID.clone(), local_owner);
    let local = GlobalId::User(100_000);
    let foreign = GlobalId::User(100_001);
    let version = GlobalId::User(100_002);
    let local_item = CatalogItemId::User(100_000);
    let foreign_item = CatalogItemId::User(100_001);
    let alias_item = CatalogItemId::User(100_002);
    let ordinary_comment = catalog
        .entries()
        .find(|entry| entry.conn_id().is_none())
        .expect("bootstrap contains ordinary catalog items")
        .comment_object_id();
    let ids = [local, foreign, version];
    let (updates, incarnation) = {
        let mut storage = catalog.storage().await;
        let mut tx = storage
            .transaction()
            .await
            .expect("can start temporary table creation transaction");
        // Same SQL name in distinct sessions must not collide or become visible
        // locally. The foreign table's extra version also has a live alias.
        for (offset, (item, id, name, owner, versions)) in [
            (local_item, local, "t", local_owner, BTreeMap::new()),
            (
                foreign_item,
                foreign,
                "t",
                foreign_owner,
                BTreeMap::from([(RelationVersion::root().bump(), version)]),
            ),
            (alias_item, version, "alias", foreign_owner, BTreeMap::new()),
        ]
        .into_iter()
        .enumerate()
        {
            tx.insert_item(
                item,
                FIRST_USER_OID + u32::try_from(offset).expect("test item offset fits in u32"),
                id,
                SchemaSpecifier::Temporary.into(),
                name,
                format!("CREATE TEMPORARY TABLE mz_temp.{name} (a pg_catalog.int4)"),
                MZ_SYSTEM_ROLE_ID,
                vec![],
                versions,
                Some(owner),
            )
            .expect("can insert temporary catalog item");
            for column in [None, Some(0)] {
                tx.update_comment(
                    CommentObjectId::Table(item),
                    column,
                    Some("temporary".into()),
                )
                .expect("can comment on temporary item");
            }
        }
        tx.update_comment(ordinary_comment, None, Some("ordinary".into()))
            .expect("can comment on ordinary item");
        // The native catalog harness has no storage controller. Allocate the
        // metadata and initial permission together, as storage preparation does.
        let foreign_shard = ShardId::new();
        tx.insert_collection_metadata(BTreeMap::from([
            (local, ShardId::new()),
            (foreign, foreign_shard),
            (version, foreign_shard),
        ]))
        .expect("can insert collection metadata");
        for id in ids {
            tx.set_collection_compaction_bound(id, Some(Timestamp::MIN))
                .expect("can set initial collection compaction bound");
        }
        let incarnation = tx
            .create_client_incarnation(None)
            .expect("can create client incarnation");
        tx.publish_client_read_requirements(
            incarnation,
            ids.into_iter().map(|id| (id, Timestamp::MIN)).collect(),
        )
        .expect("can publish initial client read requirements");
        let updates = tx.get_and_commit_op_updates();
        let ts = tx.upper();
        tx.commit(ts)
            .await
            .expect("can commit temporary table creation");
        (updates, incarnation)
    };
    let _ = catalog
        .state
        .apply_updates(updates, &mut LocalExpressionCache::Closed)
        .await;
    assert!(catalog.state.try_get_entry(&local_item).is_some());
    assert!(catalog.state.try_get_entry(&foreign_item).is_none());
    assert!(catalog.state.try_get_entry(&alias_item).is_none());
    catalog
        .check_consistency()
        .expect("comments follow local SQL visibility");
    assert_eq!(
        catalog
            .state
            .comments
            .get_object_comments(CommentObjectId::Table(local_item))
            .expect("local comments remain visible")
            .len(),
        2
    );
    assert!(
        catalog
            .state
            .comments
            .get_object_comments(CommentObjectId::Table(foreign_item))
            .is_none()
    );
    check(&catalog).await;
    assert!(
        catalog
            .state
            .storage_metadata()
            .retained_collections
            .is_empty()
    );

    // Comment-only transactions have no Item update to classify their targets.
    let updates = {
        let mut storage = catalog.storage().await;
        let mut tx = storage
            .transaction()
            .await
            .expect("can start comment transaction");
        tx.update_comment(
            CommentObjectId::Table(local_item),
            Some(0),
            Some("edited".into()),
        )
        .expect("can replace local column comment");
        tx.update_comment(
            CommentObjectId::Table(foreign_item),
            None,
            Some("edited foreign".into()),
        )
        .expect("can replace foreign table comment");
        tx.update_comment(CommentObjectId::Table(foreign_item), Some(0), None)
            .expect("can remove foreign column comment");
        let updates = tx.get_and_commit_op_updates();
        let ts = tx.upper();
        tx.commit(ts).await.expect("can commit comment changes");
        updates
    };
    let _ = catalog
        .state
        .apply_updates(updates, &mut LocalExpressionCache::Closed)
        .await;
    assert_eq!(
        catalog
            .state
            .comments
            .get_object_comments(CommentObjectId::Table(local_item))
            .expect("local comments remain visible after replacement")
            .get(&Some(0))
            .map(String::as_str),
        Some("edited")
    );
    assert!(
        catalog
            .state
            .comments
            .get_object_comments(CommentObjectId::Table(foreign_item))
            .is_none()
    );
    assert_eq!(
        catalog
            .state
            .comments
            .get_object_comments(ordinary_comment)
            .expect("ordinary item comments remain visible")
            .get(&None)
            .map(String::as_str),
        Some("ordinary")
    );
    check(&catalog).await;

    // Dry runs leave durable client protection in place for the drop checks.
    let release = Op::PublishClientReadRequirements {
        incarnation,
        requirements: BTreeMap::new(),
    };
    let ts = catalog.current_upper().await;
    let (released, _) = catalog
        .transact_incremental_dry_run(catalog.state(), vec![release.clone()], None, None, ts)
        .await
        .expect("can dry-run client read requirement release");
    for id in ids {
        assert!(
            released
                .storage_metadata()
                .collection_metadata
                .contains_key(&id),
            "final client release must preserve live SQL collection {id}"
        );
        assert!(released.collection_compaction_bounds().contains_key(&id));
    }

    // Dropping one alias must not retire the extra version. Only the durable
    // removal of its final SQL owner makes final release eligible for cleanup.
    for item in [alias_item, foreign_item] {
        let updates = {
            let mut storage = catalog.storage().await;
            let mut tx = storage
                .transaction()
                .await
                .expect("can start temporary item removal transaction");
            tx.remove_item(item)
                .expect("can remove temporary catalog item");
            tx.drop_comments(&BTreeSet::from([CommentObjectId::Table(item)]))
                .expect("can remove temporary item comments");
            let updates = tx.get_and_commit_op_updates();
            let ts = tx.upper();
            tx.commit(ts)
                .await
                .expect("can commit temporary item removal");
            updates
        };
        let _ = catalog
            .state
            .apply_updates(updates, &mut LocalExpressionCache::Closed)
            .await;
        let dropped = item == foreign_item;
        for id in [foreign, version] {
            assert!(
                catalog
                    .state
                    .storage_metadata()
                    .collection_metadata
                    .contains_key(&id)
            );
            assert_eq!(
                catalog
                    .state
                    .storage_metadata()
                    .retained_collections
                    .contains(&id),
                dropped
            );
        }
        check(&catalog).await;
        let ts = catalog.current_upper().await;
        let (released, _) = catalog
            .transact_incremental_dry_run(catalog.state(), vec![release.clone()], None, None, ts)
            .await
            .expect("can dry-run client read requirement release after item removal");
        assert!(
            released
                .storage_metadata()
                .collection_metadata
                .contains_key(&local)
        );
        for id in [foreign, version] {
            assert_eq!(
                released
                    .storage_metadata()
                    .collection_metadata
                    .contains_key(&id),
                !dropped
            );
            assert_eq!(
                released.collection_compaction_bounds().contains_key(&id),
                !dropped
            );
        }
    }
    // A dropped foreign item is no longer a valid reason to hide a comment.
    // Apply a malformed update without persisting it and retain the diagnostic.
    use crate::memory::objects::{StateDiff, StateUpdate, StateUpdateKind};
    let orphan = StateUpdate {
        kind: StateUpdateKind::Comment(crate::durable::objects::Comment {
            object_id: CommentObjectId::Table(foreign_item),
            sub_component: None,
            comment: "orphan".into(),
        }),
        ts: catalog.current_upper().await,
        diff: StateDiff::Addition,
    };
    let _ = catalog
        .state
        .apply_updates(vec![orphan], &mut LocalExpressionCache::Closed)
        .await;
    let errors = catalog
        .check_consistency()
        .expect_err("orphaned comments must remain errors");
    assert_eq!(
        errors["comments"],
        serde_json::json!([
            {"Dangling": {"Table": foreign_item}}
        ])
    );
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn owned_catalog_reconstruction_preserves_protection_snapshot() {
    use crate::durable::TestCatalogStateBuilder;
    use crate::durable::objects::{CollectionCompactionBound, MaintainedReadRequirement};
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::ShardId;
    use mz_storage_client::controller::StorageTxn;

    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let bootstrap = test_bootstrap_args();
    let input = GlobalId::User(100_000);
    let output = GlobalId::User(100_001);
    let mut seed = TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .build()
        .await
        .expect("failed to build seed catalog")
        .open(SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("failed to open seed catalog");
    let _ = seed
        .sync_to_current_updates()
        .await
        .expect("failed to sync seed catalog");
    let mut tx = seed
        .transaction()
        .await
        .expect("failed to start seed transaction");
    tx.insert_collection_metadata(BTreeMap::from([
        (input, ShardId::new()),
        (output, ShardId::new()),
    ]))
    .expect("failed to insert seed collection metadata");
    tx.set_collection_compaction_bound(input, Some(Timestamp::from(10)))
        .expect("failed to set seed compaction bound");
    tx.set_maintained_read_requirement(output, BTreeSet::from([input]), Some(Timestamp::from(10)))
        .expect("failed to set seed read requirement");
    let _ = tx.get_and_commit_op_updates();
    let ts = tx.upper();
    tx.commit(ts)
        .await
        .expect("failed to commit seed transaction");
    seed.expire().await;

    let mut writer = Catalog::open_debug_catalog(persist.clone(), organization, &bootstrap)
        .await
        .expect("failed to open writer catalog");
    let expected = writer
        .state()
        .dump(None)
        .expect("failed to dump initial writer catalog");
    let reader = writer
        .open_diagnostic_reader()
        .await
        .expect("failed to open readonly catalog");
    let upper = writer.current_upper().await;
    let memory = writer.clone();

    let ts = writer.current_upper().await;
    writer
        .transact(
            None,
            ts,
            None,
            vec![Op::SetReadProtection {
                requirements: vec![MaintainedReadRequirement {
                    id: output,
                    inputs: BTreeSet::from([input]),
                    frontier: Some(Timestamp::from(20)),
                }],
                bounds: vec![CollectionCompactionBound {
                    id: input,
                    frontier: Some(Timestamp::from(20)),
                }],
            }],
        )
        .await
        .expect("failed to update writer read protection");
    assert_ne!(
        expected,
        writer
            .state()
            .dump(None)
            .expect("failed to dump updated writer catalog")
    );

    let input = reader
        .into_snapshot_at(upper)
        .await
        .expect("failed to extract catalog prefix");
    let reconstructed = memory
        .reconstruct_state(input)
        .await
        .expect("failed to reconstruct catalog");
    assert_eq!(
        expected,
        reconstructed
            .dump(None)
            .expect("can dump reconstructed catalog state")
    );
    drop(memory);
    writer.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_normalized_create() {
    use mz_ore::collections::CollectionExt;
    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();
        let scx = &mut StatementContext::new(None, &conn_catalog);

        let parsed =
            mz_sql_parser::parser::parse_statements("create view public.foo as select 1 as bar")
                .expect("")
                .into_element()
                .ast;

        let (stmt, _) = names::resolve(scx.catalog, parsed).expect("");

        // Ensure that all identifiers are quoted.
        assert_eq!(
            r#"CREATE VIEW "materialize"."public"."foo" AS SELECT 1 AS "bar""#,
            mz_sql::normalize::create_statement(scx, stmt).expect(""),
        );
        catalog.expire().await;
    })
    .await;
}

/// Resolving a statement and resolving its normalized `create_sql` must
/// yield the same `ResolvedIds`.
///
/// The in-memory catalog records the ids from the first resolution, while
/// a catalog reload re-derives them from the stored `create_sql`. Any
/// disagreement makes the reloaded catalog differ from the in-memory one.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // slow
async fn test_resolved_ids_survive_create_sql_round_trip() {
    use mz_ore::collections::CollectionExt;
    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();
        let scx = &mut StatementContext::new(None, &conn_catalog);

        let resolve_and_normalize = |scx: &mut StatementContext, sql: &str| {
            let parsed = mz_sql_parser::parser::parse_statements(sql)
                .expect("parses")
                .into_element()
                .ast;
            let (stmt, ids) = names::resolve(scx.catalog, parsed).expect("resolves");
            let normalized = mz_sql::normalize::create_statement(scx, stmt).expect("normalizes");
            (ids, normalized)
        };

        const ARRAY_SUFFIX: &str = "create table public.t (a pg_catalog.int4[])";
        const ARRAY_TYPE: &str = "create table public.t (a pg_catalog._int4)";
        const ELEMENT_TYPE: &str = "create table public.t (a pg_catalog.int4)";

        let mut ids_by_spelling: BTreeMap<&str, Vec<CatalogItemId>> = BTreeMap::new();
        for sql in [
            ARRAY_SUFFIX,
            ARRAY_TYPE,
            ELEMENT_TYPE,
            "create table public.t (a pg_catalog.int4 list)",
            "create view public.v as select null::pg_catalog.text[]",
        ] {
            let (ids, normalized) = resolve_and_normalize(scx, sql);
            let (round_tripped_ids, _) = resolve_and_normalize(scx, &normalized);
            assert_eq!(
                ids.items().collect::<Vec<_>>(),
                round_tripped_ids.items().collect::<Vec<_>>(),
                "resolving {normalized:?} produced different ids than {sql:?}",
            );
            ids_by_spelling.insert(sql, ids.items().copied().collect());
        }

        // `int4[]` and `_int4` name the same type, so both spellings must
        // record the same ids. `int4[]` is only ever stored as `_int4`,
        // so this is what keeps the reloaded catalog identical to the
        // in-memory one.
        assert_eq!(
            ids_by_spelling[ARRAY_SUFFIX], ids_by_spelling[ARRAY_TYPE],
            "{ARRAY_SUFFIX:?} and {ARRAY_TYPE:?} must resolve to the same ids",
        );

        // Neither spelling records the element type: an array reference
        // names the array type alone.
        let element_ids = &ids_by_spelling[ELEMENT_TYPE];
        assert!(
            ids_by_spelling[ARRAY_SUFFIX]
                .iter()
                .all(|id| !element_ids.contains(id)),
            "{ARRAY_SUFFIX:?} recorded an element type id from {ELEMENT_TYPE:?}",
        );

        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // slow
async fn test_large_catalog_item() {
    let view_def = "CREATE VIEW \"materialize\".\"public\".\"v\" AS SELECT 1 FROM (SELECT 1";
    let column = ", 1";
    let view_def_size = view_def.bytes().count();
    let column_size = column.bytes().count();
    let column_count =
        (mz_sql_parser::parser::MAX_STATEMENT_BATCH_SIZE - view_def_size) / column_size + 1;
    let columns = iter::repeat(column).take(column_count).join("");
    let create_sql = format!("{view_def}{columns})");
    let create_sql_check = create_sql.clone();
    assert_ok!(mz_sql_parser::parser::parse_statements(&create_sql));
    assert_err!(mz_sql_parser::parser::parse_statements_with_limit(
        &create_sql
    ));

    let persist_client = PersistClient::new_for_tests().await;
    let organization_id = Uuid::new_v4();
    let id = CatalogItemId::User(1);
    let gid = GlobalId::User(1);
    let bootstrap_args = test_bootstrap_args();
    {
        let mut catalog = Catalog::open_debug_catalog(
            persist_client.clone(),
            organization_id.clone(),
            &bootstrap_args,
        )
        .await
        .expect("unable to open debug catalog");
        let item = catalog
            .state()
            .deserialize_item(
                gid,
                &create_sql,
                &BTreeMap::new(),
                &mut LocalExpressionCache::Closed,
                None,
            )
            .expect("unable to parse view");
        let commit_ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                commit_ts,
                None,
                vec![Op::CreateItem {
                    item,
                    name: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Id(DatabaseId::User(1)),
                            schema_spec: SchemaSpecifier::Id(SchemaId::User(3)),
                        },
                        item: "v".to_string(),
                    },
                    id,
                    owner_id: MZ_SYSTEM_ROLE_ID,
                }],
            )
            .await
            .expect("failed to transact");
        catalog.expire().await;
    }
    {
        let catalog = Catalog::open_debug_catalog(persist_client, organization_id, &bootstrap_args)
            .await
            .expect("unable to open debug catalog");
        let view = catalog.get_entry(&id);
        assert_eq!("v", view.name.item);
        match &view.item {
            CatalogItem::View(view) => assert_eq!(create_sql_check, view.create_sql),
            item => panic!("expected view, got {}", item.typ()),
        }
        catalog.expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_object_type() {
    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();

        assert_eq!(
            mz_sql::catalog::ObjectType::ClusterReplica,
            conn_catalog.get_object_type(&ObjectId::ClusterReplica((
                ClusterId::user(1).expect("1 is a valid ID"),
                ReplicaId::User(1)
            )))
        );
        assert_eq!(
            mz_sql::catalog::ObjectType::Role,
            conn_catalog.get_object_type(&ObjectId::Role(RoleId::User(1)))
        );
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_get_privileges() {
    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();

        assert_eq!(
            None,
            conn_catalog.get_privileges(&SystemObjectId::Object(ObjectId::ClusterReplica((
                ClusterId::user(1).expect("1 is a valid ID"),
                ReplicaId::User(1),
            ))))
        );
        assert_eq!(
            None,
            conn_catalog.get_privileges(&SystemObjectId::Object(ObjectId::Role(RoleId::User(1))))
        );
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn verify_builtin_descs() {
    // Provide a full AWS context so the builtin AWS connection views resolve
    // as they do in a cloud environment. Without an account id,
    // `mz_aws_privatelink_connections.principal` folds to NULL, its
    // `WHERE principal IS NOT NULL` filter is statically unsatisfiable, and
    // the optimizer infers an extra empty key that the declared desc, which
    // describes the cloud shape, does not carry.
    let aws_context = DebugAwsContext {
        aws_account_id: Some("123456789000".to_string()),
        aws_external_id_prefix: Some("eb5cb59b-e2fe-41f3-87ca-d2176a495345".to_string()),
        aws_connection_role_arn: Some(
            "arn:aws:iam::123456789000:role/MaterializeConnection".to_string(),
        ),
    };
    Catalog::with_debug_aws_context(aws_context, |catalog| async move {
        let conn_catalog = catalog.for_system_session();

        for builtin in BUILTINS::iter() {
            let (schema, name, expected_desc) = match builtin {
                Builtin::Table(t) => (&t.schema, &t.name, &t.desc),
                Builtin::View(v) => (&v.schema, &v.name, &v.desc),
                Builtin::MaterializedView(mv) => (&mv.schema, &mv.name, &mv.desc),
                Builtin::Source(s) => (&s.schema, &s.name, &s.desc),
                Builtin::Log(_)
                | Builtin::Type(_)
                | Builtin::Func(_)
                | Builtin::Index(_)
                | Builtin::Connection(_) => continue,
            };
            let item = conn_catalog
                .resolve_item(&PartialItemName {
                    database: None,
                    schema: Some(schema.to_string()),
                    item: name.to_string(),
                })
                .expect("unable to resolve item")
                .at_version(RelationVersionSelector::Latest);

            let actual_desc = item.relation_desc().expect("invalid item type");
            for (index, ((actual_name, actual_typ), (expected_name, expected_typ))) in
                actual_desc.iter().zip_eq(expected_desc.iter()).enumerate()
            {
                assert_eq!(
                    actual_name, expected_name,
                    "item {schema}.{name} column {index} name did not match its expected name"
                );
                assert_eq!(
                    actual_typ, expected_typ,
                    "item {schema}.{name} column {index} ('{actual_name}') type did not match its expected type"
                );
            }
            assert_eq!(
                &*actual_desc, expected_desc,
                "item {schema}.{name} did not match its expected RelationDesc"
            );
        }
        catalog.expire().await;
    })
    .await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_compare_builtins_postgres() {
    async fn inner(catalog: Catalog) {
        // Verify that all builtin functions:
        // - have a unique OID
        // - if they have a postgres counterpart (same oid) then they have matching name
        let (client, connection) = tokio_postgres::connect(
            &env::var("POSTGRES_URL").unwrap_or_else(|_| "host=localhost user=postgres".into()),
            NoTls,
        )
        .await
        .expect("failed to connect to Postgres");

        task::spawn(|| "compare_builtin_postgres", async move {
            if let Err(e) = connection.await {
                panic!("connection error: {}", e);
            }
        });

        struct PgProc {
            name: String,
            arg_oids: Vec<u32>,
            ret_oid: Option<u32>,
            ret_set: bool,
        }

        struct PgType {
            name: String,
            ty: String,
            elem: u32,
            array: u32,
            input: u32,
            receive: u32,
            send: u32,
        }

        struct PgOper {
            oprresult: u32,
            name: String,
        }

        let pg_proc: BTreeMap<_, _> = query(
            &client,
            sql!(
                "SELECT
                p.oid,
                proname,
                proargtypes,
                prorettype,
                proretset
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid"
            ),
            &[],
        )
        .await
        .expect("pg query failed")
        .into_iter()
        .map(|row| {
            let oid: u32 = row.get("oid");
            let pg_proc = PgProc {
                name: row.get("proname"),
                arg_oids: row.get("proargtypes"),
                ret_oid: row.get("prorettype"),
                ret_set: row.get("proretset"),
            };
            (oid, pg_proc)
        })
        .collect();

        let pg_type: BTreeMap<_, _> = query(
            &client,
            sql!(
                "SELECT oid, typname, typtype::text, typelem, typarray, typinput::oid, typreceive::oid as typreceive, typsend::oid as typsend FROM pg_type"
            ),
            &[],
        )
        .await
            .expect("pg query failed")
            .into_iter()
            .map(|row| {
                let oid: u32 = row.get("oid");
                let pg_type = PgType {
                    name: row.get("typname"),
                    ty: row.get("typtype"),
                    elem: row.get("typelem"),
                    array: row.get("typarray"),
                    input: row.get("typinput"),
                    receive: row.get("typreceive"),
                    send: row.get("typsend"),
                };
                (oid, pg_type)
            })
            .collect();

        let pg_oper: BTreeMap<_, _> = query(
            &client,
            sql!("SELECT oid, oprname, oprresult FROM pg_operator"),
            &[],
        )
        .await
        .expect("pg query failed")
        .into_iter()
        .map(|row| {
            let oid: u32 = row.get("oid");
            let pg_oper = PgOper {
                name: row.get("oprname"),
                oprresult: row.get("oprresult"),
            };
            (oid, pg_oper)
        })
        .collect();

        let conn_catalog = catalog.for_system_session();
        let resolve_type_oid = |item: &str| {
            conn_catalog
                .resolve_type(&PartialItemName {
                    database: None,
                    // All functions we check exist in PG, so the types must, as
                    // well
                    schema: Some(PG_CATALOG_SCHEMA.into()),
                    item: item.to_string(),
                })
                .expect("unable to resolve type")
                .oid()
        };

        let func_oids: BTreeSet<_> = BUILTINS::funcs()
            .flat_map(|f| f.inner.func_impls().into_iter().map(|f| f.oid))
            .collect();

        let mut all_oids = BTreeSet::new();

        // A function to determine if two oids are equivalent enough for these tests. We don't
        // support some types, so map exceptions here.
        let equivalent_types: BTreeSet<(Option<u32>, Option<u32>)> = BTreeSet::from_iter(
            [
                // We don't support NAME.
                (Type::NAME, Type::TEXT),
                (Type::NAME_ARRAY, Type::TEXT_ARRAY),
                // We don't support time with time zone.
                (Type::TIME, Type::TIMETZ),
                (Type::TIME_ARRAY, Type::TIMETZ_ARRAY),
            ]
            .map(|(a, b)| (Some(a.oid()), Some(b.oid()))),
        );
        let ignore_return_types: BTreeSet<u32> = BTreeSet::from([
            1619, // pg_typeof: TODO: We now have regtype and can correctly implement this.
        ]);
        let is_same_type = |fn_oid: u32, a: Option<u32>, b: Option<u32>| -> bool {
            if ignore_return_types.contains(&fn_oid) {
                return true;
            }
            if equivalent_types.contains(&(a, b)) || equivalent_types.contains(&(b, a)) {
                return true;
            }
            a == b
        };

        for builtin in BUILTINS::iter() {
            match builtin {
                Builtin::Type(ty) => {
                    assert!(all_oids.insert(ty.oid), "{} reused oid {}", ty.name, ty.oid);

                    if ty.oid >= FIRST_MATERIALIZE_OID {
                        // High OIDs are reserved in Materialize and don't have
                        // PostgreSQL counterparts.
                        continue;
                    }

                    // For types that have a PostgreSQL counterpart, verify that
                    // the name and oid match.
                    let pg_ty = pg_type.get(&ty.oid).unwrap_or_else(|| {
                        panic!("pg_proc missing type {}: oid {}", ty.name, ty.oid)
                    });
                    assert_eq!(
                        ty.name, pg_ty.name,
                        "oid {} has name {} in postgres; expected {}",
                        ty.oid, pg_ty.name, ty.name,
                    );

                    let (typinput_oid, typreceive_oid, typsend_oid) = match &ty.details.pg_metadata
                    {
                        None => (0, 0, 0),
                        Some(pgmeta) => (
                            pgmeta.typinput_oid,
                            pgmeta.typreceive_oid,
                            pgmeta.typsend_oid,
                        ),
                    };
                    assert_eq!(
                        typinput_oid, pg_ty.input,
                        "type {} has typinput OID {:?} in mz but {:?} in pg",
                        ty.name, typinput_oid, pg_ty.input,
                    );
                    assert_eq!(
                        typreceive_oid, pg_ty.receive,
                        "type {} has typreceive OID {:?} in mz but {:?} in pg",
                        ty.name, typreceive_oid, pg_ty.receive,
                    );
                    // Unlike typinput and typreceive below, typsend is not also
                    // checked against `func_oids`. Nothing resolves a typsend OID
                    // to a name, so the corresponding `*send` functions are
                    // deliberately not registered as builtins.
                    assert_eq!(
                        typsend_oid, pg_ty.send,
                        "type {} has typsend OID {:?} in mz but {:?} in pg",
                        ty.name, typsend_oid, pg_ty.send,
                    );
                    if typinput_oid != 0 {
                        assert!(
                            func_oids.contains(&typinput_oid),
                            "type {} has typinput OID {} that does not exist in pg_proc",
                            ty.name,
                            typinput_oid,
                        );
                    }
                    if typreceive_oid != 0 {
                        assert!(
                            func_oids.contains(&typreceive_oid),
                            "type {} has typreceive OID {} that does not exist in pg_proc",
                            ty.name,
                            typreceive_oid,
                        );
                    }

                    // Ensure the type matches.
                    match &ty.details.typ {
                        CatalogType::Array { element_reference } => {
                            let elem_ty = BUILTINS::iter()
                                .filter_map(|builtin| match builtin {
                                    Builtin::Type(ty @ BuiltinType { name, .. })
                                        if element_reference == name =>
                                    {
                                        Some(ty)
                                    }
                                    _ => None,
                                })
                                .next();
                            let elem_ty = match elem_ty {
                                Some(ty) => ty,
                                None => {
                                    panic!("{} is unexpectedly not a type", element_reference)
                                }
                            };
                            assert_eq!(
                                pg_ty.elem, elem_ty.oid,
                                "type {} has mismatched element OIDs",
                                ty.name
                            )
                        }
                        CatalogType::Pseudo => {
                            assert_eq!(
                                pg_ty.ty, "p",
                                "type {} is not a pseudo type as expected",
                                ty.name
                            )
                        }
                        CatalogType::Range { .. } => {
                            assert_eq!(
                                pg_ty.ty, "r",
                                "type {} is not a range type as expected",
                                ty.name
                            );
                        }
                        _ => {
                            assert_eq!(
                                pg_ty.ty, "b",
                                "type {} is not a base type as expected",
                                ty.name
                            )
                        }
                    }

                    // Ensure the array type reference is correct.
                    let schema = catalog
                        .resolve_schema_in_database(
                            &ResolvedDatabaseSpecifier::Ambient,
                            ty.schema,
                            &SYSTEM_CONN_ID,
                        )
                        .expect("unable to resolve schema");
                    let allocated_type = catalog
                        .resolve_type(
                            None,
                            &vec![(ResolvedDatabaseSpecifier::Ambient, schema.id().clone())],
                            &PartialItemName {
                                database: None,
                                schema: Some(schema.name().schema.clone()),
                                item: ty.name.to_string(),
                            },
                            &SYSTEM_CONN_ID,
                        )
                        .expect("unable to resolve type");
                    let ty = if let CatalogItem::Type(ty) = &allocated_type.item {
                        ty
                    } else {
                        panic!("unexpectedly not a type")
                    };
                    match ty.details.array_id {
                        Some(array_id) => {
                            let array_ty = catalog.get_entry(&array_id);
                            assert_eq!(
                                pg_ty.array, array_ty.oid,
                                "type {} has mismatched array OIDs",
                                allocated_type.name.item,
                            );
                        }
                        None => assert_eq!(
                            pg_ty.array, 0,
                            "type {} does not have an array type in mz but does in pg",
                            allocated_type.name.item,
                        ),
                    }
                }
                Builtin::Func(func) => {
                    for imp in func.inner.func_impls() {
                        assert!(
                            all_oids.insert(imp.oid),
                            "{} reused oid {}",
                            func.name,
                            imp.oid
                        );

                        assert!(
                            imp.oid < FIRST_USER_OID,
                            "built-in function {} erroneously has OID in user space ({})",
                            func.name,
                            imp.oid,
                        );

                        // For functions that have a postgres counterpart, verify that the name and
                        // oid match.
                        let pg_fn = if imp.oid >= FIRST_UNPINNED_OID {
                            continue;
                        } else {
                            pg_proc.get(&imp.oid).unwrap_or_else(|| {
                                panic!("pg_proc missing function {}: oid {}", func.name, imp.oid)
                            })
                        };
                        assert_eq!(
                            func.name, pg_fn.name,
                            "funcs with oid {} don't match names: {} in mz, {} in pg",
                            imp.oid, func.name, pg_fn.name
                        );

                        // Complain, but don't fail, if argument oids don't match.
                        // TODO: make these match.
                        let imp_arg_oids = imp
                            .arg_typs
                            .iter()
                            .map(|item| resolve_type_oid(item))
                            .collect::<Vec<_>>();

                        if imp_arg_oids != pg_fn.arg_oids {
                            println!(
                                "funcs with oid {} ({}) don't match arguments: {:?} in mz, {:?} in pg",
                                imp.oid, func.name, imp_arg_oids, pg_fn.arg_oids
                            );
                        }

                        let imp_return_oid = imp.return_typ.map(resolve_type_oid);

                        assert!(
                            is_same_type(imp.oid, imp_return_oid, pg_fn.ret_oid),
                            "funcs with oid {} ({}) don't match return types: {:?} in mz, {:?} in pg",
                            imp.oid,
                            func.name,
                            imp_return_oid,
                            pg_fn.ret_oid
                        );

                        assert_eq!(
                            imp.return_is_set, pg_fn.ret_set,
                            "funcs with oid {} ({}) don't match set-returning value: {:?} in mz, {:?} in pg",
                            imp.oid, func.name, imp.return_is_set, pg_fn.ret_set
                        );
                    }
                }
                _ => (),
            }
        }

        for (op, func) in OP_IMPLS.iter() {
            for imp in func.func_impls() {
                assert!(all_oids.insert(imp.oid), "{} reused oid {}", op, imp.oid);

                // For operators that have a postgres counterpart, verify that the name and oid match.
                let pg_op = if imp.oid >= FIRST_UNPINNED_OID {
                    continue;
                } else {
                    pg_oper.get(&imp.oid).unwrap_or_else(|| {
                        panic!("pg_operator missing operator {}: oid {}", op, imp.oid)
                    })
                };

                assert_eq!(*op, pg_op.name);

                let imp_return_oid = imp.return_typ.map(resolve_type_oid).expect("must have oid");
                if imp_return_oid != pg_op.oprresult {
                    panic!(
                        "operators with oid {} ({}) don't match return typs: {} in mz, {} in pg",
                        imp.oid, op, imp_return_oid, pg_op.oprresult
                    );
                }
            }
        }
        catalog.expire().await;
    }

    Catalog::with_debug(inner).await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_pg_views_forbidden_types() {
    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();

        for view in BUILTINS::views()
            .filter(|view| view.schema == PG_CATALOG_SCHEMA || view.schema == INFORMATION_SCHEMA)
        {
            let item = conn_catalog
                .resolve_item(&PartialItemName {
                    database: None,
                    schema: Some(view.schema.to_string()),
                    item: view.name.to_string(),
                })
                .expect("unable to resolve view")
                // TODO(alter_table)
                .at_version(RelationVersionSelector::Latest);
            let full_name = conn_catalog.resolve_full_name(item.name());
            let desc = item.relation_desc().expect("invalid item type");
            for col_type in desc.iter_types() {
                match &col_type.scalar_type {
                    typ @ SqlScalarType::UInt16
                    | typ @ SqlScalarType::UInt32
                    | typ @ SqlScalarType::UInt64
                    | typ @ SqlScalarType::MzTimestamp
                    | typ @ SqlScalarType::List { .. }
                    | typ @ SqlScalarType::Map { .. }
                    | typ @ SqlScalarType::MzAclItem => {
                        panic!("{typ:?} type found in {full_name}");
                    }
                    SqlScalarType::AclItem
                    | SqlScalarType::Bool
                    | SqlScalarType::Int16
                    | SqlScalarType::Int32
                    | SqlScalarType::Int64
                    | SqlScalarType::Float32
                    | SqlScalarType::Float64
                    | SqlScalarType::Numeric { .. }
                    | SqlScalarType::Date
                    | SqlScalarType::Time
                    | SqlScalarType::Timestamp { .. }
                    | SqlScalarType::TimestampTz { .. }
                    | SqlScalarType::Interval
                    | SqlScalarType::PgLegacyChar
                    | SqlScalarType::Bytes
                    | SqlScalarType::String
                    | SqlScalarType::Char { .. }
                    | SqlScalarType::VarChar { .. }
                    | SqlScalarType::Jsonb
                    | SqlScalarType::Uuid
                    | SqlScalarType::Array(_)
                    | SqlScalarType::Record { .. }
                    | SqlScalarType::Oid
                    | SqlScalarType::RegProc
                    | SqlScalarType::RegType
                    | SqlScalarType::RegClass
                    | SqlScalarType::Int2Vector
                    | SqlScalarType::Range { .. }
                    | SqlScalarType::PgLegacyName => {}
                }
            }
        }
        catalog.expire().await;
    })
    .await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
async fn test_mz_introspection_builtins() {
    Catalog::with_debug(|catalog| async move {
        let conn_catalog = catalog.for_system_session();

        let introspection_schema_id = catalog.get_mz_introspection_schema_id();
        let introspection_schema_spec = SchemaSpecifier::Id(introspection_schema_id);

        for entry in catalog.entries() {
            let schema_spec = entry.name().qualifiers.schema_spec;
            let introspection_deps = catalog.introspection_dependencies(entry.id);
            if introspection_deps.is_empty() {
                assert!(
                    schema_spec != introspection_schema_spec,
                    "entry does not depend on introspection sources but is in \
                     `mz_introspection`: {}",
                    conn_catalog.resolve_full_name(entry.name()),
                );
            } else {
                assert!(
                    schema_spec == introspection_schema_spec,
                    "entry depends on introspection sources but is not in \
                     `mz_introspection`: {}",
                    conn_catalog.resolve_full_name(entry.name()),
                );
            }
        }
    })
    .await
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // Requires a subprocess and persist's native dependencies.
async fn test_unapplicable_foreign_catalog_changes_halt_writer() {
    use crate::durable::TestCatalogStateBuilder;
    use crate::durable::debug::{ConfigCollection, ItemCollection};
    use crate::durable::objects::serialization::proto;
    use mz_ore::now::SYSTEM_TIME;

    const CHILD: &str = "MZ_TEST_FOREIGN_CATALOG_RECOVERY_CHILD";
    if env::var_os(CHILD).is_none() {
        for case in ["invalid-sql", "protection-mode"] {
            let output = std::process::Command::new(env::current_exe().expect("test executable"))
                .args([
                    "--exact",
                    "catalog::tests::test_unapplicable_foreign_catalog_changes_halt_writer",
                    "--nocapture",
                ])
                .env(CHILD, case)
                .env("MZ_TEST_LOG_FILTER", "warn")
                .output()
                .expect("run child test");
            let output_text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            // halt! uses _exit(166), unlike libtest assertion/panic failures.
            assert_eq!(output.status.code(), Some(166), "{output_text}");
            if case == "invalid-sql" {
                assert!(
            output_text.contains(
                "invalid persisted SQL: CREATE VIEW materialize.public.invalid_view AS SELECT missing_column"
            ),
            "{output_text}"
        );
                assert!(
                    output_text.contains(
                        "halting process: cannot apply committed catalog changes, restart required"
                    ),
                    "{output_text}"
                );
            } else {
                assert!(
                    output_text.contains("catalog_read_protection_enabled"),
                    "{output_text}"
                );
                assert!(output_text.contains("restart required"), "{output_text}");
            }
        }
        return;
    }

    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let bootstrap = test_bootstrap_args();
    let builder = TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation();
    let storage = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("open fresh durable catalog");
    let mut writer = Catalog::open_debug_catalog_inner(
        persist,
        storage,
        SYSTEM_TIME.clone(),
        Some(
            format!("local-az1-{organization}-0")
                .parse()
                .expect("parse test environment ID"),
        ),
        &mz_build_info::DUMMY_BUILD_INFO,
        BTreeMap::from([("enable_catalog_read_protection".into(), "true".into())]),
        &bootstrap,
        None,
        None,
    )
    .await
    .expect("bootstrap protected writer");
    assert!(writer.state().catalog_read_protection_enabled());
    assert!(!writer.storage().await.is_read_only());

    let mut observer = builder.clone().unwrap_build().await;
    let epoch = observer.epoch().await.expect("read initial catalog epoch");
    let generation = observer
        .get_deployment_generation()
        .await
        .expect("read initial deployment generation");
    let key = proto::ItemKey {
        gid: proto::CatalogItemId::User(1),
    };
    let value = proto::ItemValue {
        schema_id: proto::SchemaId::User(3),
        name: "invalid_view".into(),
        definition: proto::CatalogItem::V1(proto::CatalogItemV1 {
            // Parseable SQL with an unresolvable column reaches item planning.
            create_sql: "CREATE VIEW materialize.public.invalid_view AS SELECT missing_column"
                .into(),
        }),
        owner_id: proto::RoleId::System(1),
        privileges: vec![],
        oid: FIRST_USER_OID,
        global_id: proto::GlobalId::User(1),
        extra_versions: vec![],
        ephemeral_owner_session: None,
    };
    // A debug handle's pending generation must not promote the live writer.
    let mut admin = builder
        .with_deploy_generation(99)
        .unwrap_build()
        .await
        .open_debug()
        .await
        .expect("open debug catalog for foreign edits");
    if env::var(CHILD).expect("read child recovery test case") == "protection-mode" {
        admin
            .edit::<ConfigCollection>(
                proto::ConfigKey {
                    key: "catalog_read_protection_enabled".into(),
                },
                proto::ConfigValue { value: 0 },
                true,
            )
            .await
            .expect("disable catalog read protection through debug handle");
        assert!(
            observer
                .trace_consolidated()
                .await
                .expect("read catalog trace after protection-mode edit")
                .configs
                .values
                .iter()
                .any(
                    |((key, value), _, diff)| key.key == "catalog_read_protection_enabled"
                        && value.value == 0
                        && *diff == mz_repr::Diff::ONE
                )
        );
    } else {
        assert_eq!(
            admin
                .edit::<ItemCollection>(key.clone(), value.clone(), true)
                .await
                .expect("persist unplannable view through debug handle"),
            None
        );
        let trace = observer
            .trace_consolidated()
            .await
            .expect("read catalog trace after invalid-SQL edit");
        assert!(
            trace.items.values.iter().any(|((k, v), _, diff)| {
                k == &key && v == &value && *diff == mz_repr::Diff::ONE
            })
        );
    }
    assert_eq!(
        observer
            .epoch()
            .await
            .expect("read catalog epoch after foreign edit"),
        epoch
    );
    assert_eq!(
        observer
            .get_deployment_generation()
            .await
            .expect("read deployment generation after foreign edit"),
        generation
    );

    writer
        .sync_to_current_updates()
        .await
        .expect("writer was not fenced");
    panic!("writer continued after observing an unapplicable committed change");
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] //  unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
async fn test_multi_subscriber_catalog() {
    let persist_client = PersistClient::new_for_tests().await;
    let bootstrap_args = test_bootstrap_args();
    let organization_id = Uuid::new_v4();
    let db_name = "DB";

    let storage = crate::durable::TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_default_deploy_generation()
        .build()
        .await
        .expect("build fresh protected writer")
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap_args)
        .await
        .expect("open fresh protected writer");
    let mut writer_catalog = Catalog::open_debug_catalog_inner(
        persist_client.clone(),
        storage,
        mz_ore::now::SYSTEM_TIME.clone(),
        Some(
            format!("local-az1-{organization_id}-0")
                .parse()
                .expect("environment id"),
        ),
        &mz_build_info::DUMMY_BUILD_INFO,
        std::collections::BTreeMap::from([(
            "enable_catalog_read_protection".into(),
            "true".into(),
        )]),
        &bootstrap_args,
        None,
        None,
    )
    .await
    .expect("open_debug_catalog");
    let mut read_only_catalog = Catalog::open_debug_read_only_catalog(
        persist_client.clone(),
        organization_id.clone(),
        &bootstrap_args,
    )
    .await
    .expect("open_debug_read_only_catalog");
    assert_err!(writer_catalog.resolve_database(db_name));
    assert_err!(read_only_catalog.resolve_database(db_name));
    let before_ddl = read_only_catalog.clone();

    let commit_ts = writer_catalog.current_upper().await;
    writer_catalog
        .transact(
            None,
            commit_ts,
            None,
            vec![Op::CreateDatabase {
                name: db_name.to_string(),
                owner_id: MZ_SYSTEM_ROLE_ID,
            }],
        )
        .await
        .expect("failed to transact");

    let write_db = writer_catalog
        .resolve_database(db_name)
        .expect("resolve_database");
    read_only_catalog
        .sync_to_current_updates()
        .await
        .expect("sync_to_current_updates");
    let read_db = read_only_catalog
        .resolve_database(db_name)
        .expect("resolve_database")
        .clone();

    assert_eq!(write_db, &read_db);
    assert!(!before_ddl.transient_revision_is_current());

    let before_metadata = read_only_catalog.clone();
    let commit_ts = writer_catalog.current_upper().await;
    writer_catalog
        .transact(
            None,
            commit_ts,
            None,
            vec![Op::CreateClientIncarnation { replica_id: None }],
        )
        .await
        .expect("publish peer client metadata");
    read_only_catalog
        .sync_to_current_updates()
        .await
        .expect("follow client metadata");
    assert!(before_metadata.transient_revision_is_current());

    // Restart reconciliation would remove this committed pending replica.
    let replica = writer_catalog
        .user_cluster_replicas()
        .next()
        .expect("bootstrap user replica")
        .clone();
    let mut replica_config = replica.config;
    let mz_controller_types::clusters::ReplicaLocation::Managed(location) =
        &mut replica_config.location
    else {
        panic!("bootstrap replica must be managed");
    };
    location.pending = true;
    let ts = writer_catalog.current_upper().await;
    let replica_id = writer_catalog
        .allocate_user_replica_ids(1, ts)
        .await
        .expect("allocate pending replica ID")[0];
    let ts = writer_catalog.current_upper().await;
    writer_catalog
        .transact(
            None,
            ts,
            None,
            vec![Op::CreateClusterReplica {
                cluster_id: replica.cluster_id,
                replica_id,
                name: "pending_replica".into(),
                config: replica_config,
                owner_id: replica.owner_id,
                reason: super::ReplicaCreateDropReason::GracefulReconfiguration,
            }],
        )
        .await
        .expect("create pending replica");

    let joined = crate::durable::TestCatalogStateBuilder::new(persist_client.clone())
        .with_organization_id(organization_id)
        .with_default_deploy_generation()
        .build()
        .await
        .expect("build joined writer")
        .join()
        .await
        .expect("join active writer generation");
    let before_open = writer_catalog
        .storage()
        .await
        .transaction()
        .await
        .expect("snapshot before committed open")
        .current_snapshot();
    let upper_before_open = writer_catalog.current_upper().await;
    let mut config = Catalog::diagnostic_state_config(&writer_catalog.diagnostic_config);
    // Startup-only inputs must not override committed desired state.
    config.skip_migrations = false;
    config.builtin_system_cluster_config.replication_factor = 0;
    config.remote_system_parameters = Some(BTreeMap::from([("max_tables".into(), "999".into())]));
    config.external_login_password_mz_system = Some("not-a-committed-password".into());
    config.enable_expression_cache_override = Some(true);
    let opened = Catalog::open_committed(config, joined)
        .await
        .expect("open independent committed catalog");
    let mut writer_catalog_peer = opened.catalog;
    let initial_updates = opened.initial_updates;
    let initial = crate::memory::implications::CatalogImplications::from_updates(
        initial_updates,
        &Catalog::expression_build_version(writer_catalog.config().build_info).to_string(),
    );
    assert_eq!(
        initial.clusters.keys().copied().collect::<BTreeSet<_>>(),
        writer_catalog
            .clusters()
            .map(|cluster| cluster.id)
            .collect(),
    );
    assert!(writer_catalog_peer.expr_cache_handle.is_none());
    assert_eq!(
        writer_catalog_peer.state().dump(None).expect("dump peer"),
        writer_catalog.state().dump(None).expect("dump writer")
    );
    assert_eq!(writer_catalog_peer.current_upper().await, upper_before_open);
    assert_eq!(
        writer_catalog_peer
            .storage()
            .await
            .transaction()
            .await
            .expect("snapshot after committed open")
            .current_snapshot(),
        before_open
    );
    let peer_db = writer_catalog_peer
        .resolve_database(db_name)
        .expect("resolve_database for peer");
    assert_eq!(peer_db, &read_db);
    let peer_before_ddl = writer_catalog_peer.clone();
    let ts = writer_catalog.current_upper().await;
    writer_catalog
        .transact(
            None,
            ts,
            None,
            vec![Op::CreateDatabase {
                name: "after_committed_open".into(),
                owner_id: MZ_SYSTEM_ROLE_ID,
            }],
        )
        .await
        .expect("commit subsequent DDL");
    assert!(peer_before_ddl.transient_revision_is_current());
    writer_catalog_peer
        .sync_to_current_updates()
        .await
        .expect("committed loader follows subsequent DDL");
    assert!(!peer_before_ddl.transient_revision_is_current());
    assert_eq!(
        writer_catalog_peer
            .resolve_database("after_committed_open")
            .expect("peer followed database"),
        writer_catalog
            .resolve_database("after_committed_open")
            .expect("writer database")
    );
    drop(peer_before_ddl);
    writer_catalog
        .sync_to_current_updates()
        .await
        .expect("same-generation peer does not fence");
    read_only_catalog
        .sync_to_current_updates()
        .await
        .expect("same-generation reader survives");

    let promoted = crate::durable::TestCatalogStateBuilder::new(persist_client)
        .with_organization_id(organization_id)
        .with_deploy_generation(1)
        .build()
        .await
        .expect("build promotion")
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap_args)
        .await
        .expect("promote generation");

    let write_fence_err = writer_catalog
        .sync_to_current_updates()
        .await
        .expect_err("sync_to_current_updates for fencer");
    assert!(matches!(
        write_fence_err,
        CatalogError::Durable(DurableCatalogError::Fence(
            FenceError::DeployGeneration { .. }
        ))
    ));
    let read_fence_err = read_only_catalog
        .sync_to_current_updates()
        .await
        .expect_err("sync_to_current_updates after fencer");
    assert!(matches!(
        read_fence_err,
        CatalogError::Durable(DurableCatalogError::Fence(
            FenceError::DeployGeneration { .. }
        ))
    ));

    writer_catalog.expire().await;
    read_only_catalog.expire().await;
    writer_catalog_peer.expire().await;
    promoted.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn catalog_invalid_cast_remains_typed() {
    use crate::catalog::{Catalog, CatalogState};
    use crate::memory::error::ItemError;
    use crate::optimize::OptimizerError;
    use mz_sql::plan::PlanError;

    Catalog::with_debug(|catalog| async move {
        let error = CatalogState::parse_plan(
            "CREATE VIEW materialize.public.v AS SELECT CAST(ARRAY[1] AS INTEGER)",
            None,
            &catalog.for_system_session(),
        )
        .expect_err("array cannot be cast to integer");
        let ItemError::PlanError(error @ PlanError::InvalidCast { .. }) = error else {
            panic!("native catalog parsing lost the invalid-cast cause");
        };
        assert!(matches!(
            ItemError::from(OptimizerError::PlanError(error)),
            ItemError::PlanError(PlanError::InvalidCast { .. })
        ));
    })
    .await;
}
