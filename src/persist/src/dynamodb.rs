// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A DynamoDB implementation of [Consensus].

use std::cmp;
use std::collections::HashMap;
use std::ops::Range;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_config::default_provider::{credentials, region};
use aws_config::meta::region::ProvideRegion;
use aws_config::sts::AssumeRoleProvider;
use aws_sdk_dynamodb::model::{AttributeValue, Put, TransactWriteItem, Update};
use aws_sdk_dynamodb::types::Blob as DynamoBlob;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_types::credentials::SharedCredentialsProvider;
use bytes::{Buf, Bytes};
use futures_executor::block_on;
use futures_util::FutureExt;
use mz_ore::task::RuntimeExt;
use tokio::runtime::Handle as AsyncHandle;
use tracing::{debug, trace};
use uuid::Uuid;

use mz_aws_util::config::AwsConfig;
use mz_ore::cast::CastFrom;

use crate::error::Error;
use crate::storage::{Atomicity, Consensus, LockInfo, SeqNo};

/// Configuration for opening an [DynamoDbConsensus].
#[derive(Clone, Debug)]
pub struct DynamoDbConsensusConfig {
    client: DynamoDbClient,
    table: String,
    shard: String,
}

impl DynamoDbConsensusConfig {
    const EXTERNAL_TESTS_DYNAMO: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_DYNAMO_TABLE";
    /// wip
    pub async fn new(
        table: String,
        shard: String,
        role_arn: Option<String>,
    ) -> Result<Self, Error> {
        let mut loader = aws_config::from_env();
        if let Some(role_arn) = role_arn {
            let mut role_provider = AssumeRoleProvider::builder(role_arn).session_name("persist");
            if let Some(region) = region::default_provider().region().await {
                role_provider = role_provider.region(region);
            }
            let default_provider =
                SharedCredentialsProvider::new(credentials::default_provider().await);
            loader = loader.credentials_provider(role_provider.build(default_provider));
        }
        let config = AwsConfig::from_loader(loader).await;
        let client = mz_aws_util::dynamodb::client(&config);
        Ok(DynamoDbConsensusConfig {
            client,
            table,
            shard,
        })
    }

    /// wip
    pub async fn new_for_test() -> Result<Option<Self>, Error> {
        let table = match std::env::var(Self::EXTERNAL_TESTS_DYNAMO) {
            Ok(table) => table,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        // Give each test a unique prefix so they don't conflict. We don't have
        // to worry about deleting any data that we create because the bucket is
        // set to auto-delete after 1 day.
        let shard = Uuid::new_v4().to_string();
        let role_arn = None;
        let config = Self::new(table, shard, role_arn).await?;
        Ok(Some(config))
    }
}

/// wip
#[derive(Debug)]
pub struct DynamoDbConsensusCore {
    client: DynamoDbClient,
    table: String,
    shard: String,
}

impl DynamoDbConsensusCore {
    const HEAD: &'static str = "HEAD";

    /// wip
    pub fn open(config: DynamoDbConsensusConfig) -> Self {
        Self {
            client: config.client,
            table: config.table,
            shard: config.shard,
        }
    }

    fn get_key(&self, seqno_or_head: &str) -> HashMap<String, AttributeValue> {
        let mut key = HashMap::new();

        key.insert(
            "shard-id".to_string(),
            AttributeValue::S(self.shard.clone()),
        );
        key.insert(
            "seqno-or-head".to_string(),
            AttributeValue::S(seqno_or_head.into()),
        );

        key
    }

    async fn head(&self) -> Result<Option<(SeqNo, Vec<u8>)>, Error> {
        println!("starting head");
        let head_key = self.get_key(Self::HEAD);
        let seqno = match self
            .client
            .get_item()
            .table_name(&self.table)
            .set_key(Some(head_key))
            .send()
            .await
            .expect("wip")
            .item
        {
            None => return Ok(None),
            Some(attributes) => {
                if let Some(AttributeValue::S(seqno_str)) = attributes.get("pointer") {
                    seqno_str.clone()
                } else {
                    panic!("wip expected to get a pointer back")
                }
            }
        };
        println!("got seqno");

        let seqno_key = self.get_key(&seqno);
        let value = if let Some(AttributeValue::B(value)) = self
            .client
            .get_item()
            .table_name(&self.table)
            .set_key(Some(seqno_key))
            .send()
            .await
            .expect("wip")
            .item
            .expect("wip2")
            .get("pointer")
        {
            value.clone().into_inner()
        } else {
            panic!("wip expected to get a pointer back")
        };

        let seqno = SeqNo(seqno.parse::<u64>().expect("wip"));
        println!("ending head");

        Ok(Some((seqno, value)))
    }

    async fn cas(&mut self, seqno: SeqNo, data: Vec<u8>) -> Result<bool, Error> {
        // We want to write `data` to seqno, assuming that seqno does not exist
        // we want to make sure that head points to seqno - 1 (or does not exist)
        // and we want to repoint head to seqno

        let head_key = self.get_key(Self::HEAD);
        let curr_str = seqno.0.to_string();
        let mut seqno_put = self.get_key(&curr_str);
        seqno_put.insert("value".into(), AttributeValue::B(DynamoBlob::new(data)));
        let mut transact_write_items = vec![];
        let put = Put::builder()
            .table_name(&self.table)
            .set_item(Some(seqno_put))
            .condition_expression("attribute_not_exists(#value)")
            .expression_attribute_names("#value", "value")
            .build();

        transact_write_items.push(TransactWriteItem::builder().put(put).build());
        let update = if seqno.0 > 0 {
            let prev_str = (seqno.0 - 1).to_string();
            Update::builder()
                .table_name(&self.table)
                .set_key(Some(head_key))
                .condition_expression("#pointer = :prev")
                .update_expression("SET #pointer = :curr")
                .expression_attribute_names("#pointer", "pointer")
                .expression_attribute_values(":prev", AttributeValue::S(prev_str))
                .expression_attribute_values(":curr", AttributeValue::S(curr_str))
                .build()
        } else {
            Update::builder()
                .table_name(&self.table)
                .set_key(Some(head_key))
                .condition_expression("attribute_not_exists(#pointer)")
                .update_expression("SET #pointer = :curr")
                .expression_attribute_names("#pointer", "pointer")
                .expression_attribute_values(":curr", AttributeValue::S(curr_str))
                .build()
        };
        transact_write_items.push(TransactWriteItem::builder().update(update).build());

        if self
            .client
            .transact_write_items()
            .set_transact_items(Some(transact_write_items))
            .send()
            .await
            .is_ok()
        {
            Ok(true)
        } else {
            // wip need better error handling
            Ok(false)
        }
    }

    async fn scan(&self, seqno: SeqNo) -> Result<Vec<(SeqNo, Vec<u8>)>, Error> {
        match self
            .client
            .scan()
            .table_name(&self.table)
            .consistent_read(true)
            .send()
            .await
        {
            Ok(resp) => {
                println!("responses {:?}", resp.items);
                Ok(vec![])
            }
            Err(_) => Err(Error::from("error occurred while scanning")),
        }
    }

    async fn truncate(&mut self, seqno: SeqNo) -> Result<(), Error> {
        unimplemented!()
    }
}

impl Consensus for DynamoDbConsensusCore {
    fn head(&self) -> Result<Option<(SeqNo, Vec<u8>)>, Error> {
        futures_executor::block_on(self.head())
    }

    fn cas(&mut self, seqno: SeqNo, data: Vec<u8>) -> Result<bool, Error> {
        futures_executor::block_on(self.cas(seqno, data))
    }

    fn scan(&self, seqno: SeqNo) -> Result<Vec<(SeqNo, Vec<u8>)>, Error> {
        futures_executor::block_on(self.scan(seqno))
    }

    fn truncate(&mut self, seqno: SeqNo) -> Result<(), Error> {
        futures_executor::block_on(self.truncate(seqno))
    }
}

#[cfg(test)]
mod tests {
    use tracing::info;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn dynamodb_consensus() -> Result<(), Error> {
        mz_ore::test::init_logging();
        let config = match futures_executor::block_on(DynamoDbConsensusConfig::new_for_test())? {
            Some(client) => client,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    DynamoDbConsensusConfig::EXTERNAL_TESTS_DYNAMO
                );
                println!("going to skip this test");
                return Ok(());
            }
        };
        let mut consensus = DynamoDbConsensusCore::open(config);

        consensus_impl_test(consensus)
    }

    fn consensus_impl_test<C: Consensus>(mut consensus: C) -> Result<(), Error> {
        // empty head works
        let head = consensus.head();
        assert_eq!(head, Ok(None));

        let result = consensus.cas(SeqNo(0), "abc".as_bytes().to_vec());
        assert_eq!(result, Ok(true));
        let scan = consensus.scan(SeqNo(0));
        assert_eq!(scan, Ok(vec![]));

        Ok(())
    }
}
