// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Connection types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use mz_kafka_util::KafkaAddrs;
use mz_repr::GlobalId;
use mz_secrets::SecretsReader;

use crate::types::connections::aws::AwsExternalIdPrefix;

pub mod aws;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum StringOrSecret {
    String(String),
    Secret(GlobalId),
}

impl StringOrSecret {
    pub fn get_string(&self, secrets_reader: &SecretsReader) -> anyhow::Result<String> {
        match self {
            StringOrSecret::String(s) => Ok(s.clone()),
            StringOrSecret::Secret(id) => secrets_reader.read_string(*id),
        }
    }
}

/// Extra context to pass through when instantiating a connection for a source
/// or sink.
///
/// Should be kept cheaply cloneable.
#[derive(Debug, Clone)]
pub struct ConnectionContext {
    /// The level for librdkafka's logs.
    pub librdkafka_log_level: tracing::Level,
    /// A prefix for an external ID to use for all AWS AssumeRole operations.
    pub aws_external_id_prefix: Option<AwsExternalIdPrefix>,
}

impl ConnectionContext {
    /// Constructs a new connection context from command line arguments.
    ///
    /// **WARNING:** it is critical for security that the `aws_external_id` be
    /// provided by the operator of the Materialize service (i.e., via a CLI
    /// argument or environment variable) and not the end user of Materialize
    /// (e.g., via a configuration option in a SQL statement). See
    /// [`AwsExternalIdPrefix`] for details.
    pub fn from_cli_args(
        filter: &tracing_subscriber::filter::Targets,
        aws_external_id_prefix: Option<String>,
    ) -> ConnectionContext {
        ConnectionContext {
            librdkafka_log_level: mz_ore::tracing::target_level(filter, "librdkafka"),
            aws_external_id_prefix: aws_external_id_prefix.map(AwsExternalIdPrefix),
        }
    }
}

impl Default for ConnectionContext {
    fn default() -> ConnectionContext {
        ConnectionContext {
            librdkafka_log_level: tracing::Level::INFO,
            aws_external_id_prefix: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Connection {
    Kafka(KafkaConnection),
    Csr(mz_ccsr::ClientConfig),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaConnection {
    pub broker: KafkaAddrs,
    pub options: BTreeMap<String, StringOrSecret>,
}
