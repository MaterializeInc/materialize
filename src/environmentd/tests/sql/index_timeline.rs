// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use mz_catalog::durable::objects::{
    ClientIncarnation, ClientReadRequirement, CollectionCompactionBound, DurableType, Item,
};
use mz_catalog::durable::persist_backed_catalog_join_active;
use mz_environmentd::test_util::TestHarness;
use mz_postgres_util::{batch_execute, sql};

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
async fn index_creation_protects_timeline_before_installation() {
    let test_case = async {
        let server = TestHarness::default().start().await;
        let client = server.connect().await.unwrap();
        for statement in [
            sql!("CREATE TABLE timeline_input (a int)"),
            sql!("CREATE CLUSTER timeline_cluster REPLICAS (), MANAGED = false"),
            sql!("CREATE INDEX timeline_idx IN CLUSTER timeline_cluster ON timeline_input (a)"),
        ] {
            batch_execute(&client, statement).await.unwrap();
        }

        // No replica can install this index. Do not SELECT or EXPLAIN the input:
        // read acquisition could supply the very protection creation must own.
        let persist = server
            .persist_clients
            .open(server.persist_location.clone())
            .await
            .unwrap();
        let mut peer = persist_backed_catalog_join_active(
            persist,
            server.environment_id.organization_id(),
            mz_environmentd::BUILD_INFO.semver_version(),
            Arc::new(mz_catalog::durable::Metrics::new(
                &mz_ore::metrics::MetricsRegistry::new(),
            )),
            Some(server.catalog_timestamp_oracle().await),
        )
        .await
        .unwrap();
        peer.sync_to_current_updates().await.unwrap();
        let snapshot = peer.snapshot().await.unwrap();
        let item_id = |name| {
            snapshot
                .items
                .iter()
                .map(|(key, value)| Item::from_key_value(key.clone(), value.clone()))
                .find(|item| item.name == name)
                .unwrap()
                .global_id
        };
        let index = item_id("timeline_idx");
        let input = item_id("timeline_input");
        // This observer creates no incarnation or grants. With no reads and no
        // replicas in the index's cluster, its non-replica grant is the creator's.
        let grant = snapshot
            .client_read_requirements
            .iter()
            .map(|(key, value)| ClientReadRequirement::from_key_value(key.clone(), value.clone()))
            .find(|grant| {
                grant.id == index
                    && snapshot
                        .client_incarnations
                        .iter()
                        .map(|(key, value)| {
                            ClientIncarnation::from_key_value(key.clone(), value.clone())
                        })
                        .any(|client| client.id == grant.incarnation && client.replica_id.is_none())
            })
            .expect("SQL creation must durably protect the index before installation");
        let creator = snapshot
            .client_incarnations
            .iter()
            .map(|(key, value)| ClientIncarnation::from_key_value(key.clone(), value.clone()))
            .find(|client| client.id == grant.incarnation)
            .unwrap();
        let creator_id = creator.id;
        let initial_heartbeat = creator.heartbeat;

        // Drive timeline advancement with a blind write rather than waiting for
        // an unchanged client's periodic heartbeat.
        batch_execute(&client, sql!("INSERT INTO timeline_input VALUES (1)"))
            .await
            .unwrap();
        loop {
            peer.sync_to_current_updates().await.unwrap();
            let current = peer.snapshot().await.unwrap();
            let requirements: Vec<_> = current
                .client_read_requirements
                .into_iter()
                .map(|(key, value)| ClientReadRequirement::from_key_value(key, value))
                .collect();
            let grant = requirements
                .iter()
                .find(|grant| grant.incarnation == creator_id && grant.id == index)
                .expect("metadata publication must retain the creator's index grant");
            let bound = current
                .collection_compaction_bounds
                .into_iter()
                .map(|(key, value)| CollectionCompactionBound::from_key_value(key, value))
                .find(|bound| bound.id == index)
                .and_then(|bound| bound.frontier)
                .expect("the live index must retain its creation bound");
            assert!(bound <= grant.frontier, "grant must remain readable");
            assert!(
                requirements.iter().any(|requirement| {
                    requirement.incarnation == creator_id
                        && requirement.id == input
                        && requirement.frontier <= grant.frontier
                }),
                "the creator must also protect the index's logical input"
            );
            let creator = current
                .client_incarnations
                .into_iter()
                .map(|(key, value)| ClientIncarnation::from_key_value(key, value))
                .find(|client| client.id == creator_id)
                .expect("the creator must remain live");
            // Heartbeats advance only with complete requirement publications.
            // Observe that event rather than assuming a sleep caused publication.
            if creator.heartbeat > initial_heartbeat {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(180), test_case)
        .await
        .expect("index creation protection test timed out");
}
