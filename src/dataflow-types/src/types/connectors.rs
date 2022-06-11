// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Connector types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use mz_kafka_util::KafkaAddrs;
use mz_repr::GlobalId;
use mz_secrets::SecretsReader;

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

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Connector {
    Kafka(KafkaConnector),
    Csr(mz_ccsr::ClientConfig),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaConnector {
    pub broker: KafkaAddrs,
    pub options: BTreeMap<String, StringOrSecret>,
}
