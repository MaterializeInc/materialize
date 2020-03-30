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
