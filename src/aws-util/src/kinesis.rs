// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS Kinesis client and utilities.

use anyhow::Context;

use aws_sdk_kinesis::model::{Shard, ShardIteratorType};
use aws_sdk_kinesis::Client;

use crate::config::AwsConfig;
use crate::util;

/// Constructs a new AWS Kinesis client that respects the
/// [system proxy configuration](mz_http_proxy#system-proxy-configuration).
pub fn client(config: &AwsConfig) -> Result<Client, anyhow::Error> {
    let mut builder = aws_sdk_kinesis::config::Config::builder().region(config.region().cloned());
    builder.set_credentials_provider(Some(config.credentials_provider().clone()));
    if let Some(endpoint) = config.endpoint() {
        builder = builder.endpoint_resolver(endpoint.clone());
    }
    let conn = util::connector()?;
    Ok(Client::from_conf_conn(builder.build(), conn))
}

/// Wrapper around AWS Kinesis ListShards API.
///
/// Returns all shards in a given Kinesis stream, potentially paginating through
/// a list of shards (greater than 100) using the next_token request parameter.
///
/// Does not currently handle any ListShards errors, will return all errors
/// directly to the caller.
pub async fn list_shards(client: &Client, stream_name: &str) -> Result<Vec<Shard>, anyhow::Error> {
    let mut next_token = None;
    let mut all_shards = Vec::new();
    loop {
        let output = client
            .list_shards()
            .set_next_token(next_token)
            .stream_name(stream_name)
            .send()
            .await
            .context("fetching shard list")?;

        match output.shards {
            Some(shards) => {
                all_shards.extend(shards);
            }
            None => {
                return Err(anyhow::Error::msg(format!(
                    "kinesis stream {} does not contain any shards",
                    stream_name
                )));
            }
        }

        if output.next_token.is_some() {
            // Use the `next_token` field to paginate through Shards.
            next_token = output.next_token;
        } else {
            // When `next_token` is None, we've paginated through all of the Shards.
            break Ok(all_shards);
        }
    }
}

/// Instead of returning Shard objects, only return their ids.
pub async fn get_shard_ids(
    client: &Client,
    stream_name: &str,
) -> Result<impl Iterator<Item = String>, anyhow::Error> {
    let shards = list_shards(client, stream_name).await?;
    Ok(shards
        .into_iter()
        .map(|s| s.shard_id.unwrap_or_else(|| "".into())))
}

/// Wrapper around AWS Kinesis GetShardIterator API (and Rusoto).
///
/// This function returns the TRIM_HORIZON shard iterator of a given stream and shard, meaning
/// it will return the location in the shard with the oldest data record. We use this to connect
/// to newly discovered shards.
///
/// Does not currently handle any GetShardIterator errors, will return all errors
/// directly to the caller.
pub async fn get_shard_iterator(
    client: &Client,
    stream_name: &str,
    shard_id: &str,
) -> Result<Option<String>, anyhow::Error> {
    Ok(client
        .get_shard_iterator()
        .stream_name(stream_name)
        .shard_id(shard_id)
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .context("fetching shard iterator")?
        .shard_iterator)
}
