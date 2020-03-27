// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use structopt::StructOpt;

#[derive(Clone, Debug, StructOpt)]
pub struct Args {
    /// The materialized host
    #[structopt(long, default_value = "localhost")]
    pub materialized_host: String,

    #[structopt(long, default_value = "6875")]
    pub materialized_port: u16,

    #[structopt(long, default_value = "foo")]
    pub kinesis_source_name: String,

    /// The kinesis port
    #[structopt(long, default_value = "5468")]
    pub kinesis_port: u16,

    #[structopt(long, default_value = "kinesis-test")]
    pub kinesis_stream_name: String,

    #[structopt(long, default_value = "us-east-2")]
    pub kinesis_region: String,

    /// From the IAM account with Kinesis Data Stream privileges
    #[structopt(long)]
    pub kinesis_access_key: String,

    /// From the IAM account with Kinesis Data Stream privileges
    #[structopt(long)]
    pub kinesis_secret_access_key: String,

    /// A random seed for generating the records and prices
    #[structopt(long)]
    pub seed: Option<u64>,
}

impl Args {
    pub(crate) fn kinesis_config(&self) -> KinesisConfig {
        KinesisConfig {
            port: self.kinesis_port,
            stream_name: self.kinesis_stream_name.clone(),
            region: self.kinesis_region.clone(),
            access_key: self.kinesis_access_key.clone(),
            secret_access_key: self.kinesis_secret_access_key.clone(),
            seed: self.seed,
        }
    }

    pub(crate) fn mz_config(&self) -> MzConfig {
        MzConfig {
            host: self.materialized_host.clone(),
            port: self.materialized_port,
            kinesis_source_name: self.kinesis_source_name.clone(),
            seed: self.seed,
        }
    }

    pub(crate) fn kinesis(&self) -> String {
        if let Some(seed) = self.seed {
            format!("{}-{}", self.kinesis_stream_name, seed)
        } else {
            String::from("")
        }
    }
}

#[derive(Debug)]
pub struct KinesisConfig {
    pub port: u16,
    pub stream_name: String,
    pub region: String,
    pub access_key: String,
    pub secret_access_key: String,
    pub seed: Option<u64>,
}

#[derive(Debug)]
pub struct MzConfig {
    pub host: String,
    pub port: u16,
    pub kinesis_source_name: String,
    pub seed: Option<u64>,
}
