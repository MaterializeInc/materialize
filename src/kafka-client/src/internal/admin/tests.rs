// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the admin client.
//!
//! Tests that need a broker run only when `MZ_KAFKA_CLIENT_TEST_BROKERS` is
//! set to one or more `;`-separated bootstrap server lists, e.g.
//! `127.0.0.1:19092;127.0.0.1:29092`, and are skipped otherwise.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};

use crate::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigSource, NewPartitions, NewTopic,
    OwnedResourceSpecifier, ResourceSpecifier, TopicReplication,
};
use crate::client::DefaultClientContext;
use crate::config::{ClientConfig, FromClientConfigAndContext};
use crate::error::KafkaError;
use crate::internal::cluster::Cluster;
use crate::internal::compression::Compression;
use crate::internal::net::RequestOptions;
use crate::internal::records::{BatchBuilder, ProducerIdentity};
use crate::internal::runtime;
use crate::topic_partition_list::{Offset, TopicPartitionList};
use crate::types::RDKafkaErrorCode;
use crate::util::{Deadline, Timeout};

use super::topic_name;

/// The bootstrap server lists to test against, if configured.
fn test_brokers() -> Vec<String> {
    std::env::var("MZ_KAFKA_CLIENT_TEST_BROKERS")
        .ok()
        .map(|v| {
            v.split(';')
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// Awaits `fut` on a current-thread runtime, i.e. not on `kafka-io`.
fn block_on<F: Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(fut)
}

#[allow(clippy::disallowed_methods)]
fn admin(pairs: &[(&str, &str)]) -> AdminClient<DefaultClientContext> {
    let mut config = ClientConfig::new();
    for (k, v) in pairs {
        config.set(*k, *v);
    }
    AdminClient::from_config_and_context(&config, DefaultClientContext).unwrap()
}

/// Whether the cluster is Redpanda, whose cluster ids start with `redpanda.`.
fn is_redpanda(admin: &AdminClient<DefaultClientContext>) -> bool {
    admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(10))
        .unwrap();
    admin
        .inner()
        .cluster()
        .metadata()
        .cluster_id
        .as_deref()
        .is_some_and(|id| id.starts_with("redpanda."))
}

/// Produces `n` records to a partition with acks=1.
fn produce(cluster: &Arc<Cluster>, topic: &str, partition: i32, n: usize) {
    let cluster = Arc::clone(cluster);
    let topic = topic.to_string();
    let mut batch = BatchBuilder::new(Compression::None, None, false);
    for _ in 0..n {
        batch.append(1, None, Some(b"v"), &[]);
    }
    let records = batch.build(ProducerIdentity::NONE, -1).unwrap().bytes();
    runtime::block_on(async move {
        let deadline = Deadline::new(Duration::from_secs(30));
        loop {
            let leader = cluster.leader(&topic, partition, deadline).await.unwrap();
            let response = leader
                .request::<ProduceRequest, _>(
                    |_| {
                        ProduceRequest::default()
                            .with_acks(1)
                            .with_timeout_ms(10_000)
                            .with_topic_data(vec![
                                TopicProduceData::default()
                                    .with_name(topic_name(&topic))
                                    .with_partition_data(vec![
                                        PartitionProduceData::default()
                                            .with_index(partition)
                                            .with_records(Some(records.clone())),
                                    ]),
                            ])
                    },
                    RequestOptions::default(),
                )
                .await
                .unwrap();
            let p = &response.responses[0].partition_responses[0];
            // A new topic's partitions may not be ready on their leader yet.
            if p.error_code != 0 && !deadline.is_elapsed() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            assert_eq!(p.error_code, 0, "{response:?}");
            return;
        }
    })
}

/// Polls until the topic's partition count is `n`.
fn wait_partitions(admin: &AdminClient<DefaultClientContext>, topic: &str, n: usize) {
    let start = Instant::now();
    loop {
        let md = admin
            .inner()
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        if md
            .topics()
            .iter()
            .any(|t| t.name() == topic && t.partitions().len() == n)
        {
            return;
        }
        assert!(start.elapsed() < Duration::from_secs(30), "{md:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn opts() -> AdminOptions {
    AdminOptions::new().request_timeout(Some(Duration::from_secs(10)))
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn topics() {
    for brokers in test_brokers() {
        let admin = admin(&[("bootstrap.servers", brokers.as_str())]);
        let redpanda = is_redpanda(&admin);
        let opts = opts();
        let t1 = unique("mzkc-admin-t1");
        let t2 = unique("mzkc-admin-t2");

        // Configs, and results in request order.
        let new_t1 = NewTopic::new(&t1, 3, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact")
            .set("retention.ms", "-1");
        let new_t2 = NewTopic::new(&t2, -1, TopicReplication::Fixed(-1));
        let res = block_on(admin.create_topics([&new_t2, &new_t1], &opts)).unwrap();
        assert_eq!(res, [Ok(t2.clone()), Ok(t1.clone())]);
        let res = block_on(admin.create_topics([&new_t1], &opts)).unwrap();
        assert_eq!(
            res,
            [Err((t1.clone(), RDKafkaErrorCode::TopicAlreadyExists))]
        );
        wait_partitions(&admin, &t1, 3);

        // The broker defaults applied to `t2` are what Materialize reads from
        // the broker's config.
        let md = admin
            .inner()
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        let broker = md.brokers()[0].id();
        let config = block_on(admin.describe_configs([&ResourceSpecifier::Broker(broker)], &opts))
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(config.specifier, OwnedResourceSpecifier::Broker(broker));
        let num_partitions: usize = config
            .get("num.partitions")
            .and_then(|e| e.value.as_deref())
            .unwrap()
            .parse()
            .unwrap();
        let replication_factor: usize = config
            .get("default.replication.factor")
            .and_then(|e| e.value.as_deref())
            .unwrap()
            .parse()
            .unwrap();
        wait_partitions(&admin, &t2, num_partitions);
        let md = admin
            .inner()
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        let topic = md.topics().iter().find(|t| t.name() == t2).unwrap();
        assert!(
            topic
                .partitions()
                .iter()
                .all(|p| p.replicas().len() == replication_factor)
        );

        // Topic configs: set ones are dynamic, the rest are not.
        let config = block_on(admin.describe_configs([&ResourceSpecifier::Topic(&t1)], &opts))
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(config.specifier, OwnedResourceSpecifier::Topic(t1.clone()));
        let entry = config.get("cleanup.policy").unwrap();
        assert_eq!(entry.value.as_deref(), Some("compact"));
        assert_eq!(entry.source, ConfigSource::DynamicTopic);
        assert!(!entry.is_default);
        let dynamic: Vec<_> = config
            .entries
            .iter()
            .filter(|e| e.source == ConfigSource::DynamicTopic)
            .map(|e| (e.name.as_str(), e.value.as_deref()))
            .collect();
        assert_eq!(
            dynamic,
            [
                ("cleanup.policy", Some("compact")),
                ("retention.ms", Some("-1"))
            ]
        );

        // Non-incremental alter: unset dynamic configs revert to defaults.
        let alter = AlterConfig::new(ResourceSpecifier::Topic(&t1)).set("retention.ms", "12345");
        let res = block_on(admin.alter_configs([&alter], &opts)).unwrap();
        assert_eq!(res, [Ok(OwnedResourceSpecifier::Topic(t1.clone()))]);
        // The change reaches the broker that answers DescribeConfigs
        // asynchronously.
        let start = Instant::now();
        loop {
            let config = block_on(admin.describe_configs([&ResourceSpecifier::Topic(&t1)], &opts))
                .unwrap()
                .into_iter()
                .next()
                .unwrap()
                .unwrap();
            let dynamic: Vec<_> = config
                .entries
                .iter()
                .filter(|e| e.source == ConfigSource::DynamicTopic)
                .map(|e| (e.name.as_str(), e.value.as_deref()))
                .collect();
            if dynamic == [("retention.ms", Some("12345"))] {
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(10), "{dynamic:?}");
            std::thread::sleep(Duration::from_millis(50));
        }

        // Unknown resources and configs.
        let missing = unique("mzkc-admin-missing");
        let res =
            block_on(admin.describe_configs([&ResourceSpecifier::Topic(&missing)], &opts)).unwrap();
        assert_eq!(
            res.into_iter().next().unwrap().unwrap_err(),
            RDKafkaErrorCode::UnknownTopicOrPartition
        );
        let alter = AlterConfig::new(ResourceSpecifier::Topic(&t1)).set("mzkc.bogus", "1");
        let res = block_on(admin.alter_configs([&alter], &opts)).unwrap();
        assert_eq!(
            res,
            [Err((
                OwnedResourceSpecifier::Topic(t1.clone()),
                RDKafkaErrorCode::InvalidConfig
            ))]
        );
        let bogus = unique("mzkc-admin-bogus");
        let new_bogus = NewTopic::new(&bogus, 1, TopicReplication::Fixed(1)).set("abc", "def");
        let res = block_on(admin.create_topics([&new_bogus], &opts)).unwrap();
        if redpanda {
            // Redpanda ignores unknown topic configs.
            assert_eq!(res, [Ok(bogus.clone())]);
        } else {
            let code = res.into_iter().next().unwrap().unwrap_err().1;
            assert_eq!(code, RDKafkaErrorCode::InvalidConfig);
            assert_eq!(
                KafkaError::AdminOp(code).to_string(),
                "Admin operation error: InvalidConfig (Broker: Configuration is invalid)"
            );
        }

        // Partitions: the new total, and no shrinking.
        let res = block_on(admin.create_partitions([&NewPartitions::new(&t1, 5)], &opts)).unwrap();
        assert_eq!(res, [Ok(t1.clone())]);
        wait_partitions(&admin, &t1, 5);
        let res = block_on(admin.create_partitions([&NewPartitions::new(&t1, 2)], &opts)).unwrap();
        let expected = if redpanda {
            RDKafkaErrorCode::InvalidRequest
        } else {
            RDKafkaErrorCode::InvalidPartitions
        };
        assert_eq!(res, [Err((t1.clone(), expected))]);

        // Delete, including a missing topic.
        let names = [t1.as_str(), t2.as_str(), missing.as_str()];
        let res = block_on(admin.delete_topics(&names, &opts)).unwrap();
        assert_eq!(
            res,
            [
                Ok(t1.clone()),
                Ok(t2.clone()),
                Err((missing.clone(), RDKafkaErrorCode::UnknownTopicOrPartition))
            ]
        );
        let _ = block_on(admin.delete_topics(&[bogus.as_str()], &opts));
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn delete_records() {
    for brokers in test_brokers() {
        let admin = admin(&[("bootstrap.servers", brokers.as_str())]);
        let opts = opts();
        let topic = unique("mzkc-admin-dr");
        let new_topic = NewTopic::new(&topic, 2, TopicReplication::Fixed(1));
        let res = block_on(admin.create_topics([&new_topic], &opts)).unwrap();
        assert_eq!(res, [Ok(topic.clone())]);
        wait_partitions(&admin, &topic, 2);
        produce(admin.inner().cluster(), &topic, 0, 5);
        produce(admin.inner().cluster(), &topic, 1, 2);

        // The result is sorted by topic and partition, as in librdkafka.
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic, 7, Offset::Offset(1))
            .unwrap();
        tpl.add_partition_offset(&topic, 1, Offset::End).unwrap();
        tpl.add_partition_offset(&topic, 0, Offset::Offset(3))
            .unwrap();
        let res = block_on(admin.delete_records(&tpl, &opts)).unwrap();
        let elements = res.elements();
        let partitions: Vec<_> = elements.iter().map(|e| e.partition()).collect();
        assert_eq!(partitions, [0, 1, 7]);
        assert_eq!(elements[0].offset(), Offset::Offset(3));
        assert_eq!(elements[0].error(), Ok(()));
        assert_eq!(elements[1].offset(), Offset::Offset(2));
        assert_eq!(elements[1].error(), Ok(()));
        assert_eq!(elements[2].offset(), Offset::Offset(1));
        assert_eq!(
            elements[2].error(),
            Err(KafkaError::OffsetFetch(RDKafkaErrorCode::UnknownPartition))
        );
        assert_eq!(
            admin
                .inner()
                .fetch_watermarks(&topic, 0, Duration::from_secs(10)),
            Ok((3, 5))
        );

        // Beyond the high watermark.
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic, 0, Offset::Offset(100))
            .unwrap();
        let res = block_on(admin.delete_records(&tpl, &opts)).unwrap();
        assert_eq!(
            res.elements()[0].error(),
            Err(KafkaError::OffsetFetch(RDKafkaErrorCode::OffsetOutOfRange))
        );

        // No leader for any partition.
        let missing = unique("mzkc-admin-missing");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&missing, 0, Offset::Offset(1))
            .unwrap();
        let res = block_on(admin.delete_records(&tpl, &opts));
        assert_eq!(
            res.err(),
            Some(KafkaError::AdminOp(RDKafkaErrorCode::NoEnt))
        );
        // Not auto-created by the leader lookup.
        let md = admin
            .inner()
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        assert!(md.topics().iter().all(|t| t.name() != missing));

        let _ = block_on(admin.delete_topics(&[topic.as_str()], &opts));
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn request_errors() {
    for brokers in test_brokers() {
        let admin = admin(&[("bootstrap.servers", brokers.as_str())]);
        let opts = opts();
        assert_eq!(
            block_on(admin.create_topics(std::iter::empty::<&NewTopic>(), &opts)).err(),
            Some(KafkaError::AdminOp(RDKafkaErrorCode::InvalidArgument))
        );
        assert_eq!(
            block_on(admin.delete_records(&TopicPartitionList::new(), &opts)).err(),
            Some(KafkaError::AdminOp(RDKafkaErrorCode::InvalidArgument))
        );
        let resources = [ResourceSpecifier::Broker(1), ResourceSpecifier::Broker(2)];
        assert_eq!(
            block_on(admin.describe_configs(&resources, &opts)).err(),
            Some(KafkaError::AdminOp(RDKafkaErrorCode::Conflict))
        );
        // A broker that is not in the cluster: waits for it until the
        // request timeout.
        let short = AdminOptions::new().request_timeout(Some(Duration::from_millis(500)));
        let start = Instant::now();
        assert_eq!(
            block_on(admin.describe_configs([&ResourceSpecifier::Broker(4711)], &short)).err(),
            Some(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut))
        );
        assert!(start.elapsed() >= Duration::from_millis(500));
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn unreachable_cluster() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let bootstrap = format!("127.0.0.1:{port}");
    let admin = admin(&[("bootstrap.servers", bootstrap.as_str())]);
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_millis(300)));
    let topic = NewTopic::new("t", 1, TopicReplication::Fixed(1));

    let start = Instant::now();
    let err = block_on(admin.create_topics([&topic], &opts)).unwrap_err();
    assert!(start.elapsed() >= Duration::from_millis(300));
    assert_eq!(
        err,
        KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut)
    );
    assert_eq!(
        err.to_string(),
        "Admin operation error: OperationTimedOut (Local: Timed out)"
    );

    // Dropping the client cancels pending operations. The future needs no
    // tokio runtime to be awaited on.
    let long = AdminOptions::new().request_timeout(Some(Duration::from_secs(60)));
    let fut = admin.delete_topics(&["t"], &long);
    drop(admin);
    let start = Instant::now();
    #[allow(clippy::disallowed_methods)]
    let result = futures::executor::block_on(fut);
    assert_eq!(result.err(), Some(KafkaError::Canceled));
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[mz_ore::test]
fn option_validation() {
    let admin = admin(&[("bootstrap.servers", "127.0.0.1:1")]);
    let topic = NewTopic::new("t", 1, TopicReplication::Fixed(1));
    let err = block_on(admin.create_topics(
        [&topic],
        &AdminOptions::new().request_timeout(Some(Timeout::Never)),
    ))
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Admin operation creation error: Invalid value type for \"request_timeout\": \
         expecting integer in range 0..3600000"
    );
    let err = block_on(admin.create_topics(
        [&topic],
        &AdminOptions::new().operation_timeout(Some(Duration::from_secs(3601))),
    ))
    .unwrap_err();
    assert_eq!(
        err,
        KafkaError::AdminOpCreation(
            "Invalid value type for \"operation_timeout\": expecting integer in range \
             -1..3600000"
                .into()
        )
    );
    let err =
        block_on(admin.delete_topics(&["t"], &AdminOptions::new().broker_id(-1))).unwrap_err();
    assert_eq!(
        err,
        KafkaError::AdminOpCreation(
            "Invalid value type for \"broker\": expecting integer in range 0..2147483647".into()
        )
    );

    let bad = NewTopic::new("t", -2, TopicReplication::Fixed(1));
    assert_eq!(
        block_on(admin.create_topics([&bad], &AdminOptions::new())).unwrap_err(),
        KafkaError::AdminOpCreation(
            "num_partitions out of expected range 1..100000 or -1 for broker default".into()
        )
    );
    let bad = NewTopic::new("t", 1, TopicReplication::Fixed(-2));
    assert_eq!(
        block_on(admin.create_topics([&bad], &AdminOptions::new())).unwrap_err(),
        KafkaError::AdminOpCreation("replication_factor out of expected range -1..10000".into())
    );
    let bad = NewTopic::new("t", 2, TopicReplication::Variable(&[&[1]]));
    assert_eq!(
        block_on(admin.create_topics([&bad], &AdminOptions::new())).unwrap_err(),
        KafkaError::AdminOpCreation(
            "replication configuration for topic 't' assigns 1 partition(s), which does not \
             match the specified number of partitions (2)"
                .into()
        )
    );
    let bad = NewPartitions::new("t", 1).assign(&[&[1], &[2]]);
    assert_eq!(
        block_on(admin.create_partitions([&bad], &AdminOptions::new())).unwrap_err(),
        KafkaError::AdminOpCreation(
            "partition assignment for topic 't' assigns 2 partition(s), which is more than the \
             requested total number of partitions (1)"
                .into()
        )
    );
    let bad = NewPartitions::new("t", 0);
    assert_eq!(
        block_on(admin.create_partitions([&bad], &AdminOptions::new())).unwrap_err(),
        KafkaError::AdminOpCreation("new_total_cnt out of expected range 1..100000".into())
    );
    let bad = NewTopic::new("t\0", 1, TopicReplication::Fixed(1));
    assert!(matches!(
        block_on(admin.create_topics([&bad], &AdminOptions::new())),
        Err(KafkaError::Nul(_))
    ));
}
