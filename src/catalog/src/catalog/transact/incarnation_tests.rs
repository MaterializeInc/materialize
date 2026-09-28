// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_controller_types::ReplicaId;
use mz_persist_client::PersistClient;
use uuid::Uuid;

use crate::catalog::{Catalog, Op};

#[mz_ore::test(tokio::test)]
async fn replica_identity_survives_renewal_and_catalog_reopen() {
    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let bootstrap = crate::catalog::test_bootstrap_args();
    let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("valid incarnation fixture operation");
    let mut catalog = Catalog::open_debug_catalog_inner(
        persist.clone(),
        storage,
        mz_ore::now::SYSTEM_TIME.clone(),
        Some(
            format!("local-az1-{organization}-0")
                .parse()
                .expect("valid incarnation fixture operation"),
        ),
        &mz_build_info::DUMMY_BUILD_INFO,
        BTreeMap::from([("enable_catalog_read_protection".into(), "true".into())]),
        &bootstrap,
        None,
        None,
    )
    .await
    .expect("valid incarnation fixture operation");
    let replica_id = catalog
        .clusters()
        .flat_map(|cluster| cluster.replicas())
        .next()
        .expect("bootstrap replica")
        .replica_id;
    let ts = catalog.current_upper().await;
    let created = catalog
        .transact(
            None,
            ts,
            None,
            vec![
                Op::CreateClientIncarnation { replica_id: None },
                Op::CreateClientIncarnation {
                    replica_id: Some(replica_id),
                },
                Op::CreateClientIncarnation {
                    replica_id: Some(replica_id),
                },
            ],
        )
        .await
        .expect("valid incarnation fixture operation")
        .created_client_incarnations;
    let [query, first, restarted]: [u64; 3] = created
        .try_into()
        .expect("valid incarnation fixture operation");
    assert!(query < first && first < restarted);
    let ts = catalog.current_upper().await;
    catalog
        .transact(
            None,
            ts,
            None,
            vec![
                Op::PublishClientReadRequirements {
                    incarnation: first,
                    requirements: BTreeMap::new(),
                },
                Op::PublishClientReadRequirements {
                    incarnation: restarted,
                    requirements: BTreeMap::new(),
                },
            ],
        )
        .await
        .expect("valid incarnation fixture operation");

    let follower = Catalog::open_debug_read_only_catalog(persist, organization, &bootstrap)
        .await
        .expect("valid incarnation fixture operation");
    let incarnations = follower.state().client_incarnations();
    assert_eq!(incarnations[&query].replica_id, None);
    assert_eq!(incarnations[&first].replica_id, Some(replica_id));
    assert_eq!(incarnations[&restarted].replica_id, Some(replica_id));
    assert_eq!(incarnations[&first].heartbeat, 1);
    assert_eq!(incarnations[&restarted].heartbeat, 1);
    assert_eq!(incarnations, catalog.state().client_incarnations());

    // A tagged participant does not fence an earlier incarnation. Only the
    // existing heartbeat compare-and-reclaim operation closes one.
    let ts = catalog.current_upper().await;
    catalog
        .transact(
            None,
            ts,
            None,
            vec![Op::ReclaimClientIncarnation {
                incarnation: first,
                expected_heartbeat: 0,
            }],
        )
        .await
        .expect("valid incarnation fixture operation");
    assert!(catalog.state().client_incarnations().contains_key(&first));
    let ts = catalog.current_upper().await;
    catalog
        .transact(
            None,
            ts,
            None,
            vec![Op::ReclaimClientIncarnation {
                incarnation: first,
                expected_heartbeat: 1,
            }],
        )
        .await
        .expect("valid incarnation fixture operation");
    assert!(!catalog.state().client_incarnations().contains_key(&first));
    assert_eq!(
        catalog.state().client_incarnations()[&restarted].replica_id,
        Some(replica_id)
    );

    let ts = catalog.current_upper().await;
    assert!(
        catalog
            .transact(
                None,
                ts,
                None,
                vec![Op::CreateClientIncarnation {
                    replica_id: Some(ReplicaId::User(u64::MAX)),
                }]
            )
            .await
            .is_err()
    );
}
